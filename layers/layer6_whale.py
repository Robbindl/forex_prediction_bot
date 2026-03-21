from __future__ import annotations
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
import threading
from core.signal import Signal
from core.signal_journal import PASS, KILLED
from core.asset_profiles import get_profile
from utils.logger import get_logger

logger = get_logger()
LAYER = 6

# ── Whale alert cache (fed by bot.py WhaleAlertManager) ──────────────────────
_WHALE_CACHE: List[Dict] = []
_CACHE_LOCK  = threading.Lock()
_CACHE_TTL   = timedelta(minutes=30)

# ── Phase 2 on-chain wallet cache ─────────────────────────────────────────────
_ONCHAIN_CACHE: List[Dict] = []
_ONCHAIN_LOCK  = threading.Lock()


def _get_recent_whales(asset: str) -> List[Dict]:
    cutoff      = datetime.utcnow() - _CACHE_TTL
    asset_upper = asset.upper()
    with _CACHE_LOCK:
        return [
            w for w in _WHALE_CACHE
            if w.get("ts", datetime.min) > cutoff
            and str(w.get("asset", w.get("symbol", ""))).upper() in asset_upper
            and str(w.get("asset", w.get("symbol", "")))
        ]


def _get_onchain_data(asset: str) -> Dict:
    """Pull Phase 2 on-chain whale intelligence. Crypto only."""
    try:
        cutoff = (datetime.utcnow() - timedelta(hours=2)).timestamp() * 1000
        with _ONCHAIN_LOCK:
            relevant = [
                e for e in _ONCHAIN_CACHE
                if e.get("ts", 0) > cutoff
                and asset.upper().replace("-USD", "").replace("-USDT", "")
                   in str(e.get("asset", "")).upper()
            ]
        if not relevant:
            return {"phase2": "no_recent_activity"}

        buys     = [e for e in relevant if e.get("type") == "WHALE_ACCUMULATION"]
        sells    = [e for e in relevant if e.get("type") == "WHALE_DISTRIBUTION"]
        clusters = [e for e in relevant if e.get("type") == "WHALE_CLUSTER_ALERT"]

        return {
            "onchain_buys":   len(buys),
            "onchain_sells":  len(sells),
            "cluster_alerts": len(clusters),
            "phase2":         "whale_intelligence",
        }
    except Exception:
        return {"phase2": "unavailable"}


def ingest_whale_alert(asset: str, direction: str, size_usd: float,
                       source: str = "", sentiment: float = 0.1) -> None:
    """Called by WhaleAlertManager to register an alert event."""
    with _CACHE_LOCK:
        _WHALE_CACHE.append({
            "asset":     asset,
            "direction": direction,
            "size_usd":  size_usd,
            "source":    source,
            "sentiment": sentiment,
            "ts":        datetime.utcnow(),
        })
        cutoff = datetime.utcnow() - _CACHE_TTL * 2
        _WHALE_CACHE[:] = [w for w in _WHALE_CACHE if w.get("ts", datetime.min) > cutoff]


def ingest_onchain_event(event: Dict) -> None:
    """Called when Phase 2 publishes whale events to Redis."""
    with _ONCHAIN_LOCK:
        _ONCHAIN_CACHE.append(event)
        cutoff = (datetime.utcnow() - timedelta(hours=2)).timestamp() * 1000
        _ONCHAIN_CACHE[:] = [e for e in _ONCHAIN_CACHE if e.get("ts", 0) > cutoff]


class WhaleLayer:
    name = "whale"

    _MIN_WHALE_USD = 1_000_000

    def process(self, signal: Signal, context: Dict[str, Any]) -> Optional[Signal]:
        conf_before = signal.confidence
        profile     = get_profile(signal.asset)

        # ── Skip entirely for non-crypto assets ───────────────────────────
        if not profile.use_whale_data:
            # Mark as intentionally skipped so the data integrity gate
            # does not penalise this signal for missing whale data
            signal.metadata["whale_skipped"] = True
            signal.journal.record(
                layer=LAYER, name=self.name, decision=PASS,
                reason="whale data not applicable for this asset class — skipping",
                conf_before=conf_before, conf_after=signal.confidence,
                data={"category": signal.category},
            )
            logger.log_pipeline(signal.asset, LAYER, "PASS", "n/a (non-crypto)")
            return signal

        # ── Whale alert data ──────────────────────────────────────────────
        whales   = _get_recent_whales(signal.asset)
        buy_vol  = sum(w["size_usd"] for w in whales
                       if w["direction"] == "BUY"  and w["size_usd"] >= self._MIN_WHALE_USD)
        sell_vol = sum(w["size_usd"] for w in whales
                       if w["direction"] == "SELL" and w["size_usd"] >= self._MIN_WHALE_USD)
        total    = buy_vol + sell_vol

        # ── Phase 2: On-chain wallet intelligence ─────────────────────────
        onchain = _get_onchain_data(signal.asset)

        signal.metadata["whale_buy_vol"]  = buy_vol
        signal.metadata["whale_sell_vol"] = sell_vol

        if total == 0 and onchain.get("phase2") == "no_recent_activity":
            # No data but crypto — mark as skipped not missing
            signal.metadata["whale_skipped"] = True
            signal.journal.record(
                layer=LAYER, name=self.name, decision=PASS,
                reason="no whale data — passing neutral",
                conf_before=conf_before, conf_after=signal.confidence,
                data={"phase2": "no_data"},
            )
            logger.log_pipeline(signal.asset, LAYER, "PASS", "no whale data")
            return signal

        # Real whale data exists — mark for data integrity gate
        signal.metadata["whale_data"] = "real"

        # ── Direction analysis — weighted by sentiment and transaction size ─
        # Source credibility weights: on-chain API > Telegram > Twitter > Reddit
        _SOURCE_WEIGHT = {
            "whale-alert.io":  1.0,
            "Telegram":        0.85,
            "Twitter":         0.70,
            "Reddit":          0.50,
        }

        def _src_weight(alert: Dict) -> float:
            src = str(alert.get("source", "")).split("/")[0]
            for key, w in _SOURCE_WEIGHT.items():
                if key.lower() in src.lower():
                    return w
            return 0.60

        def _size_weight(size_usd: float) -> float:
            """Larger transactions carry more weight."""
            if size_usd >= 100_000_000:  return 1.0   # $100M+
            if size_usd >= 10_000_000:   return 0.75  # $10M+
            if size_usd >= 5_000_000:    return 0.55  # $5M+
            return 0.35                                # $1M+

        # Compute weighted directional score across all alerts
        weighted_bull = 0.0
        weighted_bear = 0.0
        for w in whales:
            sent  = float(w.get("sentiment", 0.1))
            size  = float(w.get("size_usd", 0))
            src_w = _src_weight(w)
            sz_w  = _size_weight(size)
            combined = src_w * sz_w
            if w.get("direction") == "BUY":
                weighted_bull += combined * max(0, sent)
            else:
                weighted_bear += combined * abs(min(0, sent))

        # Also add raw volume contribution
        vol_total = buy_vol + sell_vol
        if vol_total > 0:
            vol_bull_ratio = buy_vol / vol_total
            weighted_bull += vol_bull_ratio * 0.5
            weighted_bear += (1 - vol_bull_ratio) * 0.5

        wtotal = weighted_bull + weighted_bear
        dominant = "BUY" if weighted_bull >= weighted_bear else "SELL"
        ratio    = max(weighted_bull, weighted_bear) / wtotal if wtotal > 0 else 0.5

        signal.metadata["whale_dominant"]    = dominant
        signal.metadata["whale_bull_weight"] = round(weighted_bull, 3)
        signal.metadata["whale_bear_weight"] = round(weighted_bear, 3)

        # Incorporate Phase 2 on-chain signal — highest credibility source
        onchain_buys  = onchain.get("onchain_buys",  0)
        onchain_sells = onchain.get("onchain_sells", 0)
        clusters      = onchain.get("cluster_alerts", 0)
        if onchain_buys > onchain_sells:
            dominant = "BUY"
            ratio    = min(1.0, ratio + 0.1)
        elif onchain_sells > onchain_buys:
            dominant = "SELL"
            ratio    = min(1.0, ratio + 0.1)

        # ── Kill if whale strongly opposes signal ─────────────────────────
        if dominant != signal.direction and ratio > 0.65:
            # Kill strength scales with ratio and total volume
            kill_strength = 0.10 + min(0.10, ratio * 0.15)
            reason = (
                f"whale flow {dominant} opposes {signal.direction} "
                f"(w_bull={weighted_bull:.2f} w_bear={weighted_bear:.2f} "
                f"buy=${buy_vol/1e6:.1f}M sell=${sell_vol/1e6:.1f}M)"
            )
            signal.reduce(kill_strength)
            signal.journal.record(
                layer=LAYER, name=self.name, decision=PASS,
                reason=reason,
                conf_before=conf_before, conf_after=signal.confidence,
                data={
                    "whale_dominant":    dominant,
                    "ratio":             round(ratio, 3),
                    "buy_vol_m":         round(buy_vol / 1e6, 2),
                    "sell_vol_m":        round(sell_vol / 1e6, 2),
                    "weighted_bull":     round(weighted_bull, 3),
                    "weighted_bear":     round(weighted_bear, 3),
                    **onchain,
                },
            )
            logger.log_pipeline(signal.asset, LAYER, "WHALE_OPPOSE", reason)

        # ── Boost if whale confirms direction ─────────────────────────────
        boost = 0.0
        if dominant == signal.direction:
            # Boost scales with ratio strength and total volume
            vol_m = (buy_vol + sell_vol) / 1_000_000
            vol_factor = min(1.0, vol_m / 50)   # caps at $50M total volume
            boost = min(0.12, ratio * 0.1 + vol_factor * 0.04)
            if clusters > 0:
                boost = min(0.15, boost + 0.03)
            signal.boost(boost)

        reason = (
            f"whale={dominant}  ratio={ratio:.2f}  "
            f"buy=${buy_vol/1e6:.1f}M  sell=${sell_vol/1e6:.1f}M  "
            f"sources={len(whales)}"
        )
        if clusters > 0:
            reason += f"  clusters={clusters}"

        signal.journal.record(
            layer=LAYER, name=self.name, decision=PASS,
            reason=reason,
            conf_before=conf_before, conf_after=signal.confidence,
            data={
                "whale_dominant": dominant,
                "ratio":          round(ratio, 3),
                "buy_vol_m":      round(buy_vol / 1e6, 2),
                "sell_vol_m":     round(sell_vol / 1e6, 2),
                "boost":          round(boost, 3),
                **onchain,
            },
        )
        logger.log_pipeline(signal.asset, LAYER, "PASS",
                            f"whale={dominant} boost={boost:.3f}")
        return signal