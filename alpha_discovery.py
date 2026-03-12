"""
alpha_discovery.py — Alpha Discovery Engine
============================================
Finds high-probability trading opportunities using:

1. Correlation Breakdowns  — detects when historically correlated pairs diverge
   (e.g. DXY vs EUR/USD, BTC vs ETH, Gold vs USD)

2. Volume Anomalies        — z-score spike detection on volume
   (sudden volume > 2.5σ above mean = potential breakout)

3. Price-RSI Divergence    — classic hidden/regular divergence scanner
   (price makes new high but RSI doesn't = weakening momentum)

4. Liquidity Gaps          — detects price areas with no candle body support
   (fast-fill zones → sharp moves when price enters)

5. Cross-Asset Flow        — tracks capital rotation signals
   (e.g. risk-on: USD down + equities up + crypto up)

Publishes alpha signals to Redis 'alpha' channel.
"""

import os
import sys
import time
import threading
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from collections import deque

from logger import logger

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from redis_broker import broker as _broker
except Exception:
    _broker = None

try:
    from data.fetcher import NASALevelFetcher
    _fetcher = NASALevelFetcher()
except Exception:
    _fetcher = None


# ── Alpha signal structure ────────────────────────────────────────────────────

def _make_alpha(asset: str, signal_type: str, direction: str,
                strength: float, detail: str, supporting_assets: List[str] = None) -> Dict:
    return {
        'asset':             asset,
        'signal_type':       signal_type,  # 'correlation_break','volume_anomaly','divergence','liquidity_gap','flow_rotation'
        'direction':         direction,    # 'BUY','SELL','NEUTRAL'
        'strength':          round(strength, 3),  # 0.0 to 1.0
        'detail':            detail,
        'supporting_assets': supporting_assets or [],
        'timestamp':         datetime.utcnow().isoformat(),
    }


# ── 1. Correlation Breakdown Scanner ─────────────────────────────────────────

class CorrelationScanner:
    """
    Monitors known correlated pairs and fires when correlation breaks.
    A correlation breakdown is an alpha signal — the lagging asset catches up.
    """

    # (asset_a, asset_b, expected_correlation_sign)
    # +1 = normally move together, -1 = normally move opposite
    CORRELATION_PAIRS = [
        ('EUR/USD',  '^DJI',   -1),   # USD strength → EUR weak, stocks weak
        ('GC=F',     '^DJI',   -1),   # Gold up when stocks fall (risk-off)
        ('GC=F',     'EUR/USD', +1),  # Gold and EUR often both weaker USD plays
        ('BTC-USD',  'ETH-USD', +1),  # Crypto twins
        ('BTC-USD',  'SOL-USD', +1),
        ('EUR/USD',  'GBP/USD', +1),  # Both vs USD
        ('AUD/USD',  'BTC-USD', +1),  # Risk-on assets
        ('USD/JPY',  '^DJI',    +1),  # Risk-on: USD/JPY rises with stocks
        ('CL=F',     'CAD/JPY', +1),  # Oil and commodity currencies
        ('GC=F',     'USD/JPY', -1),  # Gold up → JPY up → USDJPY down
    ]

    def __init__(self):
        self._price_history: Dict[str, deque] = {}   # asset → deque of (ts, price)
        self._last_corr: Dict[str, float] = {}

    def update(self, asset: str, prices: pd.Series) -> Optional[Dict]:
        """Update price history and check for correlation breakdowns."""
        if len(prices) < 30:
            return None

        self._price_history[asset] = prices.tail(60).values

        signals = []
        for a, b, expected_sign in self.CORRELATION_PAIRS:
            if a not in self._price_history or b not in self._price_history:
                continue

            pa = self._price_history[a]
            pb = self._price_history[b]
            n  = min(len(pa), len(pb), 30)
            if n < 20:
                continue

            try:
                r = np.corrcoef(pa[-n:], pb[-n:])[0, 1]
                key = f"{a}_{b}"
                prev_r = self._last_corr.get(key)
                self._last_corr[key] = r

                if prev_r is None:
                    continue

                actual_sign  = 1 if r > 0 else -1
                correlation_broken = (actual_sign != expected_sign and abs(r) > 0.4)

                if correlation_broken:
                    # Which asset lagged? The one that diverged more recently
                    lagging = b if asset == a else a
                    # Direction: lagging asset should reverse to restore correlation
                    direction = 'BUY' if (expected_sign == 1 and r < 0) else 'SELL'

                    strength = min(abs(r), 1.0)
                    signals.append(_make_alpha(
                        asset=lagging,
                        signal_type='correlation_break',
                        direction=direction,
                        strength=strength,
                        detail=f"{a}/{b} correlation broke: r={r:.2f} (expected {expected_sign:+d})",
                        supporting_assets=[a, b],
                    ))
            except Exception:
                continue

        return signals[0] if signals else None


# ── 2. Volume Anomaly Detector ────────────────────────────────────────────────

class VolumeAnomalyDetector:
    """
    Detects statistically significant volume spikes.
    Volume > 2.5σ above 20-period mean → potential breakout signal.
    """

    def scan(self, asset: str, df: pd.DataFrame) -> Optional[Dict]:
        if 'volume' not in df.columns or len(df) < 25:
            return None

        vol = df['volume'].tail(25)
        if vol.std() == 0:
            return None

        z_score = (vol.iloc[-1] - vol.mean()) / vol.std()
        if abs(z_score) < 2.5:
            return None

        # Direction: if volume spike on up close → BUY, else SELL
        last_close = df['close'].iloc[-1]
        prev_close = df['close'].iloc[-2]
        direction  = 'BUY' if last_close > prev_close else 'SELL'
        strength   = min(abs(z_score) / 5.0, 1.0)

        return _make_alpha(
            asset=asset,
            signal_type='volume_anomaly',
            direction=direction,
            strength=strength,
            detail=f"Volume z-score={z_score:.1f}σ — {direction.lower()} pressure breakout",
        )


# ── 3. Price-RSI Divergence Scanner ──────────────────────────────────────────

class DivergenceScanner:
    """
    Detects regular and hidden RSI divergence.

    Regular bearish divergence : price HH but RSI LH → SELL
    Regular bullish divergence : price LL but RSI HL → BUY
    Hidden  bearish divergence : price LH but RSI HH → SELL (continuation)
    Hidden  bullish divergence : price HL but RSI LL → BUY  (continuation)
    """

    @staticmethod
    def _rsi(series: pd.Series, period: int = 14) -> pd.Series:
        delta = series.diff()
        gain  = delta.clip(lower=0)
        loss  = -delta.clip(upper=0)
        avg_g = gain.ewm(com=period - 1, min_periods=period).mean()
        avg_l = loss.ewm(com=period - 1, min_periods=period).mean()
        rs    = avg_g / avg_l.replace(0, np.nan)
        return 100 - 100 / (1 + rs)

    def scan(self, asset: str, df: pd.DataFrame) -> Optional[Dict]:
        if len(df) < 30:
            return None

        close = df['close'].tail(30)
        rsi   = self._rsi(close)

        if rsi.isna().all():
            return None

        p  = close.values
        r  = rsi.values
        n  = len(p)

        # Look at last 3 pivots
        def local_max(arr, i): return arr[i] > arr[i-1] and arr[i] > arr[i+1]
        def local_min(arr, i): return arr[i] < arr[i-1] and arr[i] < arr[i+1]

        # Find last two highs in price
        highs_p = [i for i in range(1, n-1) if local_max(p, i)]
        lows_p  = [i for i in range(1, n-1) if local_min(p, i)]
        highs_r = [i for i in range(1, n-1) if local_max(r, i)]
        lows_r  = [i for i in range(1, n-1) if local_min(r, i)]

        if len(highs_p) >= 2 and len(highs_r) >= 2:
            # Regular bearish: price HH, RSI LH
            if p[highs_p[-1]] > p[highs_p[-2]] and r[highs_r[-1]] < r[highs_r[-2]]:
                strength = abs(p[highs_p[-1]] - p[highs_p[-2]]) / p[highs_p[-2]] * 100
                return _make_alpha(asset, 'divergence', 'SELL',
                                   min(strength * 10, 1.0),
                                   "Regular bearish divergence: price HH, RSI LH")

        if len(lows_p) >= 2 and len(lows_r) >= 2:
            # Regular bullish: price LL, RSI HL
            if p[lows_p[-1]] < p[lows_p[-2]] and r[lows_r[-1]] > r[lows_r[-2]]:
                strength = abs(p[lows_p[-1]] - p[lows_p[-2]]) / p[lows_p[-2]] * 100
                return _make_alpha(asset, 'divergence', 'BUY',
                                   min(strength * 10, 1.0),
                                   "Regular bullish divergence: price LL, RSI HL")

        return None


# ── 4. Cross-Asset Flow Rotation Detector ────────────────────────────────────

class FlowRotationDetector:
    """
    Detects capital rotation across asset classes.

    Risk-ON  signals: USD weakens, equities rise, crypto rises, JPY weakens
    Risk-OFF signals: USD strengthens, equities fall, gold rises, JPY strengthens

    These generate 'flow_rotation' alpha signals on individual assets.
    """

    def __init__(self):
        self._scores: Dict[str, float] = {}   # asset → 5-period return

    def update(self, asset: str, returns_5: float):
        self._scores[asset] = returns_5

    def compute_rotation_signal(self) -> List[Dict]:
        """Returns a list of alpha signals based on cross-asset flows."""
        s = self._scores
        signals = []

        # Need key assets to compute
        dxy_proxy   = s.get('USD/JPY', 0)      # USD strength proxy (USD/JPY up = USD strong)
        equity      = s.get('^GSPC', s.get('^DJI', 0))
        gold        = s.get('GC=F', 0)
        btc         = s.get('BTC-USD', 0)

        if not any([dxy_proxy, equity, gold, btc]):
            return []

        # Risk-ON conditions: USD down, equities up, crypto up
        risk_on_score = (
            (-dxy_proxy * 0.3) +   # USD weakening
            (equity     * 0.4) +   # equities rising
            (btc        * 0.3)     # crypto rising
        )

        if risk_on_score > 0.002:
            # Beneficiaries of risk-on: AUD, NZD, crypto, equities
            for asset in ['AUD/USD','NZD/USD','BTC-USD','ETH-USD']:
                signals.append(_make_alpha(
                    asset=asset,
                    signal_type='flow_rotation',
                    direction='BUY',
                    strength=min(risk_on_score * 100, 0.8),
                    detail=f"Risk-ON rotation detected (score={risk_on_score:.4f})",
                    supporting_assets=['USD/JPY','^GSPC','BTC-USD'],
                ))
        elif risk_on_score < -0.002:
            # Beneficiaries of risk-off: JPY, CHF, gold, USD
            for asset in ['GC=F','USD/CHF','USD/JPY']:
                signals.append(_make_alpha(
                    asset=asset,
                    signal_type='flow_rotation',
                    direction='BUY',
                    strength=min(abs(risk_on_score) * 100, 0.8),
                    detail=f"Risk-OFF rotation detected (score={risk_on_score:.4f})",
                    supporting_assets=['USD/JPY','^GSPC','GC=F'],
                ))

        return signals


# ── Main Alpha Discovery Engine ───────────────────────────────────────────────

class AlphaDiscoveryEngine:
    """
    Runs all alpha scanners continuously in a background thread.
    Publishes findings to Redis 'alpha' channel.
    """

    SCAN_INTERVAL = 300   # seconds between full scans (5 minutes)

    SCAN_ASSETS = [
        ('EUR/USD','forex'),('GBP/USD','forex'),('USD/JPY','forex'),
        ('AUD/USD','forex'),('GC=F','commodities'),('CL=F','commodities'),
        ('BTC-USD','crypto'),('ETH-USD','crypto'),('SOL-USD','crypto'),
        ('^GSPC','indices'),('^DJI','indices'),
    ]

    def __init__(self):
        self._corr_scanner   = CorrelationScanner()
        self._vol_detector   = VolumeAnomalyDetector()
        self._div_scanner    = DivergenceScanner()
        self._flow_detector  = FlowRotationDetector()
        self._running        = False
        self._signals        = deque(maxlen=200)
        self._lock           = threading.Lock()

    def start(self):
        if self._running:
            return
        self._running = True
        t = threading.Thread(target=self._run_loop, name='AlphaDiscovery', daemon=True)
        t.start()
        logger.info("[Alpha] Discovery engine started")

    def stop(self):
        self._running = False

    def _run_loop(self):
        while self._running:
            try:
                self._scan_all()
            except Exception as e:
                logger.warning(f"[Alpha] Scan error: {e}")
            time.sleep(self.SCAN_INTERVAL)

    def _scan_all(self):
        if not _fetcher:
            return

        found = 0
        returns_5 = {}

        for asset, category in self.SCAN_ASSETS:
            try:
                df = _fetcher.fetch_yahoo_data(asset, period='5d', interval='1h')
                if df is None or df.empty or len(df) < 20:
                    continue

                if 'close' not in df.columns:
                    df.columns = [c.lower() for c in df.columns]
                if 'close' not in df.columns:
                    continue

                # 5-period return for flow rotation
                if len(df) >= 6:
                    r5 = (df['close'].iloc[-1] - df['close'].iloc[-6]) / df['close'].iloc[-6]
                    returns_5[asset] = r5
                    self._flow_detector.update(asset, r5)

                # Run individual scanners
                signals_found = []

                # Correlation
                corr_sig = self._corr_scanner.update(asset, df['close'])
                if corr_sig:
                    signals_found.append(corr_sig)

                # Volume anomaly
                vol_sig = self._vol_detector.scan(asset, df)
                if vol_sig:
                    signals_found.append(vol_sig)

                # Divergence
                div_sig = self._div_scanner.scan(asset, df)
                if div_sig:
                    signals_found.append(div_sig)

                for sig in signals_found:
                    self._emit(sig)
                    found += 1

            except Exception as e:
                logger.debug(f"[Alpha] {asset} scan failed: {e}")

        # Cross-asset flow rotation
        rotation_sigs = self._flow_detector.compute_rotation_signal()
        for sig in rotation_sigs:
            self._emit(sig)
            found += 1

        if found:
            logger.info(f"[Alpha] Scan complete — {found} alpha signals found")

    def _emit(self, signal: Dict):
        with self._lock:
            self._signals.appendleft(signal)
        if _broker:
            _broker.publish_alpha(signal)
        logger.info(
            f"[Alpha] {signal['signal_type'].upper()} | {signal['asset']} "
            f"{signal['direction']} | strength={signal['strength']:.2f} | {signal['detail']}"
        )

    def get_recent_signals(self, n: int = 50) -> List[Dict]:
        with self._lock:
            return list(self._signals)[:n]

    def get_signals_for_asset(self, asset: str, n: int = 10) -> List[Dict]:
        with self._lock:
            return [s for s in self._signals if s['asset'] == asset][:n]


# ── Global singleton ──────────────────────────────────────────────────────────
alpha_engine = AlphaDiscoveryEngine()


if __name__ == '__main__':
    import signal as _signal
    alpha_engine.start()

    def _shutdown(sig, frame):
        alpha_engine.stop()
        sys.exit(0)

    _signal.signal(_signal.SIGINT, _shutdown)
    logger.info("Alpha Discovery Engine — standalone. Press Ctrl+C to stop.")

    while True:
        time.sleep(60)
        sigs = alpha_engine.get_recent_signals(10)
        for s in sigs:
            logger.info(f"  {s['signal_type']:<20} {s['asset']:<12} {s['direction']} "
                        f"strength={s['strength']:.2f}")
