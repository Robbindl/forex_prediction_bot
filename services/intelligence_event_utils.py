from __future__ import annotations

"""Helpers for normalizing social and whale intelligence events."""

from typing import Any, Dict, Optional


_WHALE_BEARISH = {
    "dump", "dumped", "dumping", "sell", "selling", "sold", "distribution",
    "distributing", "outflow", "withdrawal", "withdrew", "exit", "exiting",
    "crash", "crashing", "fear", "panic", "warning", "alert", "suspect",
    "hack", "hacked", "stolen", "fraud", "scam", "liquidation", "liquidated",
    "exchange", "moved to exchange", "sent to exchange", "bearish",
}
_WHALE_BULLISH = {
    "buy", "buying", "bought", "accumulation", "accumulating", "inflow",
    "deposit", "deposited", "holding", "hodl", "transfer from exchange",
    "from exchange", "cold wallet", "cold storage", "bullish", "long",
    "institutional", "treasury", "reserve", "staking", "locked",
}

_CRYPTO_SYMBOL_MAP = {
    "BTC": "BTC-USD",
    "BITCOIN": "BTC-USD",
    "ETH": "ETH-USD",
    "ETHEREUM": "ETH-USD",
    "BNB": "BNB-USD",
    "SOL": "SOL-USD",
    "XRP": "XRP-USD",
    "RIPPLE": "XRP-USD",
    "ADA": "ADA-USD",
    "DOGE": "DOGE-USD",
    "LINK": "LINK-USD",
    "DOT": "DOT-USD",
    "MATIC": "MATIC-USD",
}


def canonical_crypto_asset(symbol: str) -> str:
    normalized = str(symbol or "").upper().strip()
    if not normalized:
        return ""
    if normalized in _CRYPTO_SYMBOL_MAP:
        return _CRYPTO_SYMBOL_MAP[normalized]
    if normalized.endswith("-USD"):
        return normalized
    if normalized.isalnum():
        return f"{normalized}-USD"
    return ""


def score_whale_text(text: str) -> float:
    """Score a whale alert message using simple financial keyword polarity."""
    if not text:
        return 0.1
    words = set(text.lower().split())
    words = {w.strip(".,!?;:") for w in words}
    bearish = len(words & _WHALE_BEARISH)
    bullish = len(words & _WHALE_BULLISH)
    total = bearish + bullish
    if total == 0:
        return 0.1
    raw = (bullish - bearish) / total
    return round(max(-1.0, min(1.0, raw)), 3)


def record_whale_alert_event(
    *,
    asset: str = "",
    symbol: str = "",
    source: str,
    value_usd: float,
    raw_text: str = "",
    sentiment: Optional[float] = None,
    timestamp: Any = None,
    metadata: Optional[Dict[str, Any]] = None,
    external_id: str = "",
) -> Optional[Dict[str, Any]]:
    canonical_asset = asset or canonical_crypto_asset(symbol)
    if not canonical_asset or float(value_usd or 0.0) < 500_000:
        return None

    resolved_sentiment = float(sentiment if sentiment is not None else score_whale_text(raw_text))
    direction = "BUY" if resolved_sentiment >= 0.0 else "SELL"

    try:
        from services.market_intelligence_service import get_service

        return get_service().record_whale_alert(
            asset=canonical_asset,
            direction=direction,
            size_usd=float(value_usd or 0.0),
            source=source,
            sentiment=resolved_sentiment,
            timestamp=timestamp,
            raw_text=raw_text,
            metadata=metadata,
            external_id=external_id,
        )
    except Exception:
        return None


def record_onchain_intelligence_event(
    event: Dict[str, Any],
    *,
    external_id: str = "",
) -> Optional[Dict[str, Any]]:
    payload = dict(event or {})
    asset = canonical_crypto_asset(payload.get("asset", ""))
    if asset:
        payload["asset"] = asset
    try:
        from services.market_intelligence_service import get_service

        return get_service().record_onchain_event(payload, external_id=external_id)
    except Exception:
        return None


__all__ = [
    "canonical_crypto_asset",
    "record_onchain_intelligence_event",
    "record_whale_alert_event",
    "score_whale_text",
]
