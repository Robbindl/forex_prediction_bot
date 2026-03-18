"""
layers/layer6_whale.py — Whale intelligence layer.

Changes vs original:
  - Whale data is ONLY applied to crypto assets.
    For all other asset types the layer passes immediately (SKIPPED).
  - Zero fake/dummy data returned under any condition.
  - ingest_whale_alert() validates that the asset is crypto before storing.
  - Sources tracking written to signal.metadata for pipeline data-integrity gate.
"""
from __future__ import annotations

import threading
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from core.signal import Signal
from core.signal_journal import PASS, KILLED
from core.asset_profiles import is_crypto, get_profile
from utils.logger import get_logger

logger = get_logger()
LAYER = 6

# ── In-memory caches ──────────────────────────────────────────────────────────

_WHALE_CACHE: List[Dict] = []
_CACHE_LOCK  = threading.Lock()
_CACHE_TTL   = timedelta(minutes=30)

_ONCHAIN_CACHE: List[Dict] = []
_ONCHAIN_LOCK  = threading.Lock()


# ── Canonical crypto symbol helper ───────────────────────────────────────────

_CRYPTO_SYMBOL_MAP: Dict[str, str] = {
    "BTC":   "BTC-USD", "BITCOIN":   "BTC-USD",
    "ETH":   "ETH-USD", "ETHEREUM":  "ETH-USD",
    "BNB":   "BNB-USD",
    "SOL":   "SOL-USD", "SOLANA":    "SOL-USD",
    "XRP":   "XRP-USD", "RIPPLE":    "XRP-USD",
}


def _symbol_to_canonical(symbol: str) -> Optional[str]:
    """Convert a raw symbol (e.g. 'BTC') to canonical form ('BTC-USD')."""
    return _CRYPTO_SYMBOL_MAP.get(symbol.upper())


def _get_recent_whales(asset: str) -> List[Dict]:
    cutoff      = datetime.utcnow() - _CACHE_TTL
    asset_upper = asset.upper()
    with _CACHE_LOCK:
        return [
            w for w in _WHALE_CACHE
            if w.get("ts", datetime.min) > cutoff
            and str(w.get("asset", w.get("symbol", ""))).upper() in asset_upper
        ]


def _get_onchain_data(asset: str) -> Dict:
    """Pull Phase 2 on-chain whale intelligence for this asset."""
    try:
        from whale_intelligence import tracker  # noqa: F401
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
    except Exception as e:
        logger.error(f"[WhaleLayer] On-chain data fetch failed for {asset}: {e}")
        return {"phase2": "unavailable"}


def ingest_whale_alert(
    asset: str,
    direction: str,
    size_usd: float,
    source: str = "",
) -> None:
    """
    Called by WhaleAlertManager to register an alert event.
    Silently rejects non-crypto assets — whale flow is only meaningful for crypto.
    """
    if not is_crypto(asset):
        logger.debug(f"[WhaleLayer] Ignoring whale alert for non-crypto asset: {asset}")
        return
    if size_usd < 500_000:
        return
    with _CACHE_LOCK:
        _WHALE_CACHE.append({
            "asset":     asset,
            "direction": direction,
            "size_usd":  size_usd,
            "source":    source,
            "ts":        datetime.utcnow(),
        })
        cutoff = datetime.utcnow() - _CACHE_TTL * 2
        _WHALE_CACHE[:] = [w for w in _WHALE_CACHE if w.get("ts", datetime.min) > cutoff]


def ingest_onchain_event(event: Dict) -> None:
    """
    Called when Phase 2 publishes WHALE_ACCUMULATION / WHALE_DISTRIBUTION
    / WHALE_CLUSTER_ALERT to Redis.
    """
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

        # ── Hard skip for non-crypto assets ──────────────────────────────
        if not profile.use_whale_data:
            reason = f"whale layer N/A for {signal.category} ({signal.asset})"
            signal.metadata["whale_skipped"] = reason
            signal.journal.record(
                layer=LAYER, name=self.name, decision=PASS,
                reason=reason,
                conf_before=conf_before, conf_after=signal.confidence,
                data={"skipped": True, "category": signal.category},
            )
            logger.log_pipeline(signal.asset, LAYER, "SKIPPED", reason)
            return signal

        # ── Fetch whale data ──────────────────────────────────────────────
        whales   = _get_recent_whales(signal.asset)
        buy_vol  = sum(w["size_usd"] for w in whales
                       if w["direction"] == "BUY"  and w["size_usd"] >= self._MIN_WHALE_USD)
        sell_vol = sum(w["size_usd"] for w in whales
                       if w["direction"] == "SELL" and w["size_usd"] >= self._MIN_WHALE_USD)
        total    = buy_vol + sell_vol

        onchain = _get_onchain_data(signal.asset)

        signal.metadata["whale_buy_vol"]  = buy_vol
        signal.metadata["whale_sell_vol"] = sell_vol

        # No data at all — pass neutral (NOT fake data, just no signal)
        if total == 0 and onchain.get("phase2") in ("no_recent_activity", "unavailable"):
            signal.metadata["whale_data"] = "no_recent_data"
            signal.journal.record(
                layer=LAYER, name=self.name, decision=PASS,
                reason="no whale data in window — passing neutral",
                conf_before=conf_before, conf_after=signal.confidence,
                data={"phase2": onchain.get("phase2"), "whale_data": "none"},
            )
            logger.log_pipeline(signal.asset, LAYER, "PASS", "no whale data")
            return signal

        # ── Direction analysis ────────────────────────────────────────────
        dominant = "BUY" if buy_vol >= sell_vol else "SELL"
        ratio    = max(buy_vol, sell_vol) / total if total > 0 else 0.5

        signal.metadata["whale_dominant"] = dominant
        signal.metadata["whale_data"]     = "real"

        # Incorporate Phase 2 on-chain signal
        onchain_buys  = onchain.get("onchain_buys",  0)
        onchain_sells = onchain.get("onchain_sells", 0)
        clusters      = onchain.get("cluster_alerts", 0)
        if onchain_buys > onchain_sells:
            dominant = "BUY"
        elif onchain_sells > onchain_buys:
            dominant = "SELL"

        # ── Kill confidence if whale strongly opposes signal ──────────────
        if dominant != signal.direction and ratio > 0.7:
            reason = (
                f"whale flow {dominant} strongly opposes {signal.direction} "
                f"(ratio={ratio:.2f}  buy=${buy_vol/1e6:.1f}M  sell=${sell_vol/1e6:.1f}M)"
            )
            signal.reduce(0.15)
            signal.journal.record(
                layer=LAYER, name=self.name, decision=PASS,
                reason=reason,
                conf_before=conf_before, conf_after=signal.confidence,
                data={
                    "whale_dominant": dominant,
                    "ratio":          round(ratio, 3),
                    "buy_vol_m":      round(buy_vol / 1e6, 2),
                    "sell_vol_m":     round(sell_vol / 1e6, 2),
                    **onchain,
                },
            )
            logger.log_pipeline(signal.asset, LAYER, "WHALE_OPPOSE", reason)

        # ── Boost if whale confirms direction ─────────────────────────────
        boost = 0.0
        if dominant == signal.direction:
            boost = min(0.08, ratio * 0.1)
            if clusters > 0:
                boost = min(0.12, boost + 0.04)
            signal.boost(boost)

        reason = (
            f"whale={dominant}  ratio={ratio:.2f}  "
            f"buy=${buy_vol/1e6:.1f}M  sell=${sell_vol/1e6:.1f}M"
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
