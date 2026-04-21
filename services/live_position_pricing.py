from __future__ import annotations

from typing import Any, Callable, Dict, Optional, Tuple


PriceFallback = Callable[[str, str], Tuple[Optional[float], str]]


def _coerce_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value or 0.0)
    except Exception:
        return float(default)


def _compute_position_pnl(
    asset: str,
    category: str,
    entry_price: float,
    current_price: float,
    position_size: float,
    direction: str,
) -> float:
    if not current_price or not entry_price or not position_size:
        return 0.0
    try:
        from risk.position_sizer import PositionSizer as _PS

        return float(_PS.pnl(asset, category, entry_price, current_price, position_size, direction))
    except Exception:
        if str(direction or "").upper() == "SELL":
            return float(entry_price - current_price) * float(position_size)
        return float(current_price - entry_price) * float(position_size)


def resolve_live_position_snapshot(
    position: Dict[str, Any],
    *,
    live_snapshot: Optional[Dict[str, Any]] = None,
    live_snapshot_max_age_seconds: float = 3.0,
    provider_fallback: Optional[PriceFallback] = None,
) -> Dict[str, Any]:
    asset = str(position.get("asset", "") or "")
    category = str(position.get("category", "forex") or "forex")
    direction = str(position.get("direction") or position.get("signal") or "BUY").upper()
    entry_price = _coerce_float(position.get("entry_price", 0.0))
    position_size = _coerce_float(position.get("position_size", 0.0))

    snapshot = dict(live_snapshot or {})
    if not snapshot and asset:
        try:
            from websocket_dashboard import get_live_price_snapshot

            snapshot = dict(get_live_price_snapshot(asset) or {})
        except Exception:
            snapshot = {}

    current_price: Optional[float] = None
    price_source = str(snapshot.get("source") or position.get("current_price_source") or "")
    price_age_seconds: Optional[float] = None
    price_live = False

    if snapshot:
        snapshot_price = snapshot.get("price")
        snapshot_age = _coerce_float(snapshot.get("age_seconds"), default=9999.0)
        if snapshot_price not in (None, 0, 0.0):
            current_price = _coerce_float(snapshot_price)
            price_age_seconds = max(0.0, snapshot_age)
            price_live = price_age_seconds <= float(live_snapshot_max_age_seconds or 0.0)

    if not price_live and provider_fallback is not None and asset:
        try:
            fallback_price, fallback_source = provider_fallback(asset, category)
        except Exception:
            fallback_price, fallback_source = None, ""
        if fallback_price not in (None, 0, 0.0):
            current_price = _coerce_float(fallback_price)
            price_source = str(fallback_source or price_source or "")
            price_age_seconds = 0.0
            price_live = True

    if current_price in (None, 0, 0.0):
        current_price = _coerce_float(position.get("current_price", 0.0))
        if not price_source:
            price_source = str(position.get("current_price_source") or "")

    current_price = _coerce_float(current_price)
    live_pnl = _compute_position_pnl(
        asset,
        category,
        entry_price,
        current_price,
        position_size,
        direction,
    )

    return {
        "asset": asset,
        "category": category,
        "direction": direction,
        "entry_price": entry_price,
        "position_size": position_size,
        "current_price": current_price,
        "pnl": round(float(live_pnl), 2),
        "price_source": price_source,
        "price_age_seconds": round(float(price_age_seconds), 3) if price_live and price_age_seconds is not None else None,
        "price_live": bool(price_live),
    }
