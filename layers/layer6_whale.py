"""
Layer 6 — Whale / institutional flow filter.
Merges: whale_alert_manager.py, telegram_whale_watcher.py, engines/whale_monitor.py
Synthetic orderflow REMOVED. Uses only real whale alerts + spread proxy.
"""
from __future__ import annotations
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
import threading
from core.signal import Signal
from utils.logger import get_logger

logger = get_logger()
LAYER = 6

_WHALE_CACHE: List[Dict] = []
_CACHE_LOCK  = threading.Lock()
_CACHE_TTL   = timedelta(minutes=30)


def _get_recent_whales(asset: str) -> List[Dict]:
    """Return whale events for asset in last 30 minutes."""
    cutoff = datetime.utcnow() - _CACHE_TTL
    with _CACHE_LOCK:
        return [
            w for w in _WHALE_CACHE
            if w.get("asset", "").upper() in asset.upper()
            and w.get("ts", datetime.min) > cutoff
        ]


def ingest_whale_alert(asset: str, direction: str, size_usd: float, source: str = "") -> None:
    """Called by whale watcher threads to register a whale event."""
    with _CACHE_LOCK:
        _WHALE_CACHE.append({
            "asset":     asset,
            "direction": direction,
            "size_usd":  size_usd,
            "source":    source,
            "ts":        datetime.utcnow(),
        })
        # Prune old entries
        cutoff = datetime.utcnow() - _CACHE_TTL * 2
        _WHALE_CACHE[:] = [w for w in _WHALE_CACHE if w.get("ts", datetime.min) > cutoff]


class WhaleLayer:
    name = "whale"

    _MIN_WHALE_USD = 1_000_000   # $1M minimum to count as whale

    def process(self, signal: Signal, context: Dict[str, Any]) -> Optional[Signal]:
        whales = _get_recent_whales(signal.asset)
        if not whales:
            logger.log_pipeline(signal.asset, LAYER, "PASS", "no whale data")
            return signal

        buy_vol  = sum(w["size_usd"] for w in whales if w["direction"] == "BUY"  and w["size_usd"] >= self._MIN_WHALE_USD)
        sell_vol = sum(w["size_usd"] for w in whales if w["direction"] == "SELL" and w["size_usd"] >= self._MIN_WHALE_USD)

        total = buy_vol + sell_vol
        if total == 0:
            return signal

        dominant = "BUY" if buy_vol >= sell_vol else "SELL"
        ratio    = max(buy_vol, sell_vol) / total   # 0.5 – 1.0

        signal.metadata["whale_buy_vol"]  = buy_vol
        signal.metadata["whale_sell_vol"] = sell_vol
        signal.metadata["whale_dominant"] = dominant

        if dominant != signal.direction and ratio > 0.7:
            signal.kill(
                f"Whale flow {dominant} strongly opposes {signal.direction} "
                f"(ratio={ratio:.2f})",
                LAYER,
            )
            return None

        if dominant == signal.direction:
            boost = min(0.08, ratio * 0.1)
            signal.boost(boost)
            logger.log_pipeline(signal.asset, LAYER, "WHALE_BOOST", f"+{boost:.3f}")

        logger.log_pipeline(signal.asset, LAYER, "PASS", f"whale_dominant={dominant}")
        return signal