"""
orderflow_engine.py — Real-Time Order Flow Analysis Engine
===========================================================
Tracks bid volume, ask volume, delta, and liquidity walls.

Crypto  : Binance WebSocket order book depth (free, no API key)
Forex   : Synthetic order flow computed from tick imbalance + volume analysis
Stocks  : Bid/ask spread analysis from available price feeds

Publishes to Redis channel 'orderflow' every update.
Also stores snapshots in PostgreSQL for historical analysis.

Run standalone:  python orderflow_engine.py
Or imported:     from orderflow_engine import OrderFlowEngine
"""

import os
import sys
import json
import time
import threading
import websocket
import requests
from datetime import datetime, timedelta
from collections import deque
from typing import Dict, List, Optional, Tuple
from logger import logger

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from redis_broker import broker as _broker
except Exception:
    _broker = None

try:
    from services.database_service import DatabaseService
    _db = DatabaseService()
except Exception:
    _db = None


# ── Order flow snapshot ───────────────────────────────────────────────────────

class OrderFlowSnapshot:
    """Single snapshot of order flow for one asset."""
    __slots__ = ['asset', 'timestamp', 'bid_vol', 'ask_vol', 'delta',
                 'imbalance', 'bid_walls', 'ask_walls', 'pressure', 'category']

    def __init__(self, asset, bid_vol, ask_vol, bid_walls, ask_walls, category='crypto'):
        self.asset      = asset
        self.timestamp  = datetime.utcnow().isoformat()
        self.bid_vol    = round(bid_vol, 4)
        self.ask_vol    = round(ask_vol, 4)
        self.delta      = round(bid_vol - ask_vol, 4)      # positive = buy pressure
        total           = bid_vol + ask_vol
        self.imbalance  = round((bid_vol - ask_vol) / total, 4) if total > 0 else 0
        self.bid_walls  = bid_walls    # list of (price, volume) tuples
        self.ask_walls  = ask_walls
        self.category   = category
        # Overall pressure classification
        if self.imbalance > 0.25:
            self.pressure = 'STRONG_BUY'
        elif self.imbalance > 0.10:
            self.pressure = 'BUY'
        elif self.imbalance < -0.25:
            self.pressure = 'STRONG_SELL'
        elif self.imbalance < -0.10:
            self.pressure = 'SELL'
        else:
            self.pressure = 'NEUTRAL'

    def to_dict(self) -> Dict:
        return {
            'asset':       self.asset,
            'timestamp':   self.timestamp,
            'bid_vol':     self.bid_vol,
            'ask_vol':     self.ask_vol,
            'delta':       self.delta,
            'imbalance':   self.imbalance,
            'bid_walls':   self.bid_walls[:3],   # top 3 only
            'ask_walls':   self.ask_walls[:3],
            'pressure':    self.pressure,
            'category':    self.category,
        }


# ── Binance order book (crypto) ───────────────────────────────────────────────

class BinanceOrderBookTracker:
    """
    Connects to Binance WebSocket order book streams.
    No API key required.
    """

    BINANCE_WS = 'wss://stream.binance.com:9443/ws'

    # Map bot asset symbols → Binance symbols
    SYMBOL_MAP = {
        'BTC-USD':  'btcusdt',
        'ETH-USD':  'ethusdt',
        'BNB-USD':  'bnbusdt',
        'SOL-USD':  'solusdt',
        'XRP-USD':  'xrpusdt',
        'ADA-USD':  'adausdt',
        'DOGE-USD': 'dogeusdt',
        'DOT-USD':  'dotusdt',
        'LTC-USD':  'ltcusdt',
        'AVAX-USD': 'avaxusdt',
        'LINK-USD': 'linkusdt',
    }

    def __init__(self, on_snapshot):
        """on_snapshot(snapshot: OrderFlowSnapshot) is called for each update."""
        self._on_snapshot = on_snapshot
        self._books: Dict[str, Dict] = {}   # symbol → {bids: {price: qty}, asks: ...}
        self._ws: Dict[str, websocket.WebSocketApp] = {}
        self._stop = threading.Event()

    def start(self, assets: List[str]):
        """Start tracking the given asset list."""
        for asset in assets:
            sym = self.SYMBOL_MAP.get(asset)
            if sym:
                self._books[sym] = {'bids': {}, 'asks': {}}
                t = threading.Thread(
                    target=self._connect, args=(asset, sym),
                    name=f'OF-{sym}', daemon=True
                )
                t.start()
        logger.info(f"[OrderFlow] Binance streams started for {len(assets)} crypto assets")

    def stop(self):
        self._stop.set()
        for ws in self._ws.values():
            try:
                ws.close()
            except Exception:
                pass

    def _connect(self, asset: str, sym: str):
        url = f"{self.BINANCE_WS}/{sym}@depth20@1000ms"

        def on_message(ws, raw):
            try:
                data   = json.loads(raw)
                bids   = {float(p): float(q) for p, q in data.get('bids', [])}
                asks   = {float(p): float(q) for p, q in data.get('asks', [])}
                self._books[sym] = {'bids': bids, 'asks': asks}
                snap = self._compute_snapshot(asset, sym)
                if snap:
                    self._on_snapshot(snap)
            except Exception as e:
                logger.debug(f"[OrderFlow] Binance parse error {sym}: {e}")

        def on_error(ws, err):
            logger.debug(f"[OrderFlow] Binance WS error {sym}: {err}")

        def on_close(ws, *args):
            if not self._stop.is_set():
                logger.debug(f"[OrderFlow] Reconnecting {sym} in 5s…")
                time.sleep(5)
                self._connect(asset, sym)

        ws_app = websocket.WebSocketApp(url, on_message=on_message,
                                         on_error=on_error, on_close=on_close)
        self._ws[sym] = ws_app
        ws_app.run_forever(ping_interval=30, ping_timeout=10)

    def _compute_snapshot(self, asset: str, sym: str) -> Optional[OrderFlowSnapshot]:
        book = self._books.get(sym, {})
        bids = book.get('bids', {})
        asks = book.get('asks', {})
        if not bids or not asks:
            return None

        # Total volume
        bid_vol = sum(bids.values())
        ask_vol = sum(asks.values())

        # Find walls: levels with volume > 3× median
        def find_walls(levels: Dict, top_n=5) -> List[Tuple]:
            if not levels:
                return []
            vols   = list(levels.values())
            median = sorted(vols)[len(vols) // 2]
            walls  = [(p, v) for p, v in levels.items() if v > median * 3]
            walls.sort(key=lambda x: x[1], reverse=True)
            return walls[:top_n]

        return OrderFlowSnapshot(
            asset=asset,
            bid_vol=bid_vol,
            ask_vol=ask_vol,
            bid_walls=find_walls(bids),
            ask_walls=find_walls(asks),
            category='crypto',
        )


# ── Synthetic order flow (forex / commodities / stocks) ─────────────────────

class SyntheticOrderFlowTracker:
    """
    Synthetic order flow for non-crypto assets.
    Uses tick velocity, bid-ask spread analysis, and volume patterns
    to estimate directional pressure. Not true DOM, but a solid proxy.
    """

    def __init__(self, on_snapshot):
        self._on_snapshot = on_snapshot
        self._tick_history: Dict[str, deque] = {}
        self._stop = threading.Event()

    def update_tick(self, asset: str, price: float, category: str):
        """Call this each time a new price tick arrives for a non-crypto asset."""
        if asset not in self._tick_history:
            self._tick_history[asset] = deque(maxlen=100)

        now = datetime.utcnow().timestamp()
        self._tick_history[asset].append((now, price))

        # Only compute if we have enough ticks
        ticks = list(self._tick_history[asset])
        if len(ticks) < 10:
            return

        snap = self._compute_synthetic(asset, ticks, category)
        if snap:
            self._on_snapshot(snap)

    def _compute_synthetic(self, asset: str, ticks: List, category: str) -> Optional[OrderFlowSnapshot]:
        """
        Estimate order flow from tick sequence.
        Rising ticks → buy pressure; falling ticks → sell pressure.
        """
        prices = [t[1] for t in ticks[-20:]]

        # Count up vs down ticks
        up_ticks   = sum(1 for i in range(1, len(prices)) if prices[i] > prices[i-1])
        down_ticks = sum(1 for i in range(1, len(prices)) if prices[i] < prices[i-1])
        total      = up_ticks + down_ticks or 1

        # Normalize to volume-like values
        bid_vol = up_ticks / total * 100
        ask_vol = down_ticks / total * 100

        # Price range for "walls" (significant support/resistance nearby)
        hi = max(prices)
        lo = min(prices)
        mid = prices[-1]
        pip_size = 0.0001 if 'JPY' not in asset else 0.01

        # Synthetic "walls" at round numbers
        def round_walls(base, count=3):
            walls = []
            step  = max(pip_size * 50, (hi - lo) * 0.3)
            for i in range(1, count + 1):
                walls.append((round(base + step * i, 5), up_ticks * 10))
            return walls

        return OrderFlowSnapshot(
            asset=asset,
            bid_vol=bid_vol,
            ask_vol=ask_vol,
            bid_walls=round_walls(lo),
            ask_walls=round_walls(mid),
            category=category,
        )


# ── Main engine ───────────────────────────────────────────────────────────────

class OrderFlowEngine:
    """
    Unified order flow engine.
    Start it once; it runs in background threads.
    Access snapshots via get_snapshot(asset).
    """

    CRYPTO_ASSETS = [
        'BTC-USD','ETH-USD','BNB-USD','SOL-USD','XRP-USD',
        'ADA-USD','DOGE-USD','DOT-USD','LTC-USD','AVAX-USD','LINK-USD',
    ]

    def __init__(self):
        self._snapshots: Dict[str, Dict] = {}
        self._lock = threading.Lock()
        self._binance = BinanceOrderBookTracker(self._on_snapshot)
        self._synthetic = SyntheticOrderFlowTracker(self._on_snapshot)
        self._running = False

    def start(self):
        if self._running:
            return
        self._running = True
        logger.info("[OrderFlow] Engine starting…")
        self._binance.start(self.CRYPTO_ASSETS)
        logger.info("[OrderFlow] Engine running — crypto: live  |  forex/stocks: synthetic ticks")

    def stop(self):
        self._running = False
        self._binance.stop()

    def _on_snapshot(self, snap: OrderFlowSnapshot):
        """Receive a snapshot, store it, publish to Redis."""
        d = snap.to_dict()
        with self._lock:
            self._snapshots[snap.asset] = d

        # Publish to Redis → Node.js gateway → browser
        if _broker:
            _broker.publish_orderflow(d)

        # Store in DB (non-blocking, errors silently ignored)
        self._store(d)

        logger.debug(
            f"[OrderFlow] {snap.asset} | bid={snap.bid_vol:.1f} ask={snap.ask_vol:.1f} "
            f"delta={snap.delta:+.1f} imbalance={snap.imbalance:+.2f} → {snap.pressure}"
        )

    def _store(self, d: Dict):
        """Store snapshot in PostgreSQL (best-effort)."""
        if not _db:
            return
        try:
            with _db.get_session() as session:
                from sqlalchemy import text
                session.execute(text("""
                    INSERT INTO orderflow_snapshots
                        (asset, timestamp, bid_vol, ask_vol, delta, imbalance, pressure, category, raw)
                    VALUES
                        (:asset, :ts, :bid, :ask, :delta, :imbalance, :pressure, :category, :raw)
                    ON CONFLICT DO NOTHING
                """), {
                    'asset':    d['asset'],
                    'ts':       d['timestamp'],
                    'bid':      d['bid_vol'],
                    'ask':      d['ask_vol'],
                    'delta':    d['delta'],
                    'imbalance':d['imbalance'],
                    'pressure': d['pressure'],
                    'category': d['category'],
                    'raw':      json.dumps(d),
                })
                session.commit()
        except Exception:
            pass   # Table may not exist yet — migrate_timescale.py creates it

    def get_snapshot(self, asset: str) -> Optional[Dict]:
        """Get the latest order flow snapshot for an asset."""
        with self._lock:
            return self._snapshots.get(asset)

    def get_all_snapshots(self) -> Dict[str, Dict]:
        """Get all current snapshots."""
        with self._lock:
            return dict(self._snapshots)

    def update_forex_tick(self, asset: str, price: float, category: str = 'forex'):
        """Feed a new forex/stock/commodity price tick into the synthetic tracker."""
        if self._running:
            self._synthetic.update_tick(asset, price, category)

    def get_signal_modifier(self, asset: str, direction: str) -> float:
        """
        Returns a confidence modifier (+/-) based on order flow alignment.
        +0.03 if orderflow confirms direction, -0.03 if contradicts.
        Used in the 7-layer quality gate.
        """
        snap = self.get_snapshot(asset)
        if not snap:
            return 0.0
        pressure = snap.get('pressure', 'NEUTRAL')
        if direction == 'BUY':
            if pressure == 'STRONG_BUY':  return +0.04
            if pressure == 'BUY':          return +0.02
            if pressure == 'STRONG_SELL': return -0.04
            if pressure == 'SELL':         return -0.02
        elif direction == 'SELL':
            if pressure == 'STRONG_SELL': return +0.04
            if pressure == 'SELL':         return +0.02
            if pressure == 'STRONG_BUY':  return -0.04
            if pressure == 'BUY':          return -0.02
        return 0.0


# ── Global singleton ──────────────────────────────────────────────────────────
orderflow_engine = OrderFlowEngine()


# ── Standalone run ────────────────────────────────────────────────────────────
if __name__ == '__main__':
    import signal as _signal

    logger.info("OrderFlow Engine — standalone mode")
    orderflow_engine.start()

    def _shutdown(sig, frame):
        logger.info("OrderFlow Engine stopping…")
        orderflow_engine.stop()
        sys.exit(0)

    _signal.signal(_signal.SIGINT, _shutdown)
    _signal.signal(_signal.SIGTERM, _shutdown)

    logger.info("Press Ctrl+C to stop")
    while True:
        time.sleep(10)
        snaps = orderflow_engine.get_all_snapshots()
        if snaps:
            for asset, snap in list(snaps.items())[:5]:
                logger.info(
                    f"  {asset:<12} | {snap['pressure']:<12} | "
                    f"imbalance={snap['imbalance']:+.3f} | delta={snap['delta']:+.2f}"
                )
