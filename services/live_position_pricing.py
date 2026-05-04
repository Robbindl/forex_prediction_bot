from __future__ import annotations

from typing import Any, Callable, Dict, Optional, Tuple


PriceFallback = Callable[[str, str], Tuple[Optional[float], str]]


def _coerce_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value or 0.0)
    except Exception:
        return float(default)


def _is_ig_position(position: Dict[str, Any]) -> bool:
    metadata = position.get("metadata") if isinstance(position.get("metadata"), dict) else {}
    broker_execution = metadata.get("broker_execution") if isinstance(metadata.get("broker_execution"), dict) else {}
    broker = str(position.get("broker") or broker_execution.get("broker") or "").strip().lower()
    execution_mode = str(position.get("execution_mode") or "").strip().lower()
    return broker == "ig" or execution_mode.startswith("ig")


def _normalize_position_price(position: Dict[str, Any], asset: str, value: Any) -> float:
    numeric = _coerce_float(value)
    if not numeric or not _is_ig_position(position):
        return numeric
    try:
        from services.ig_market_bridge import normalize_ig_market_price

        normalized = normalize_ig_market_price(asset, numeric)
        return _coerce_float(normalized, numeric)
    except Exception:
        return numeric


def normalize_position_price(position: Dict[str, Any], value: Any) -> float:
    """Return a position price in the bot's display/strategy scale."""
    asset = str(position.get("asset") or position.get("canonical_asset") or "")
    return _normalize_position_price(position, asset, value)


def normalize_position_prices(position: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize every price-bearing field on a broker position snapshot.

    IG returns some instruments, especially silver and WTI CFDs, in broker
    dealing scale while the strategy, risk, and dashboard use chart scale.
    This copy keeps local state and UI math on one scale without mutating the
    caller's object.
    """
    snapshot = dict(position or {})
    if not _is_ig_position(snapshot):
        return snapshot

    asset = str(snapshot.get("asset") or snapshot.get("canonical_asset") or "")
    price_fields = (
        "entry_price",
        "current_price",
        "stop_loss",
        "original_sl",
        "take_profit",
        "original_take_profit",
        "requested_entry_price",
        "highest_price",
        "lowest_price",
        "broker_entry_price",
        "broker_stop_loss",
        "broker_take_profit",
    )
    raw_prices: Dict[str, float] = {}
    for field in price_fields:
        if field not in snapshot:
            continue
        before = _coerce_float(snapshot.get(field))
        after = _normalize_position_price(snapshot, asset, before)
        if before and after and abs(before - after) > 1e-9:
            raw_prices[field] = before
            snapshot[field] = after
        elif before:
            snapshot[field] = after

    levels = []
    for raw_level in list(snapshot.get("take_profit_levels", []) or []):
        before = _coerce_float(raw_level)
        after = _normalize_position_price(snapshot, asset, before)
        if after > 0:
            levels.append(round(after, 10))
        if before and after and abs(before - after) > 1e-9:
            raw_prices.setdefault("take_profit_levels", []).append(before)  # type: ignore[union-attr]
    if levels:
        snapshot["take_profit_levels"] = levels

    if raw_prices:
        metadata = dict(snapshot.get("metadata") or {})
        metadata["broker_price_normalization"] = {
            "source": "ig_dealing_scale",
            "raw_prices": raw_prices,
        }
        snapshot["metadata"] = metadata
    return snapshot


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


def _nested_dict(payload: Dict[str, Any], key: str) -> Dict[str, Any]:
    value = payload.get(key) if isinstance(payload, dict) else {}
    return value if isinstance(value, dict) else {}


def _ig_actual_cash_per_price_unit_per_size(position: Dict[str, Any], asset: str) -> float:
    if not _is_ig_position(position):
        return 0.0
    canonical = str(asset or position.get("asset") or position.get("canonical_asset") or "").upper()
    # IG $1 commodity CFDs pay by IG display point.  XAG and WTI are displayed
    # in cents by IG, so one chart-scale unit equals 100 broker points.
    if canonical in {"XAU/USD", "GC=F"}:
        return 1.0
    if canonical in {"XAG/USD", "SI=F", "WTI", "WTI/USD", "CL=F"}:
        return 100.0
    if canonical in {"BTC-USD", "BTC/USD", "BTCUSD"}:
        return 0.1
    metadata = _nested_dict(position, "metadata")
    broker_execution = _nested_dict(metadata, "broker_execution")
    broker_sizing = _nested_dict(broker_execution, "broker_sizing")
    value = _coerce_float(broker_sizing.get("broker_cash_per_price_unit_per_size"))
    if value > 0:
        return value
    try:
        from risk.position_sizer import PositionSizer as _PS

        category = str(position.get("category") or "forex")
        spec = _PS.get_spec(asset or canonical, category)
        contract = _coerce_float(spec.get("contract"))
        if contract <= 0:
            return 0.0
        return float(_PS.cash_per_price_unit(asset or canonical, category, contract))
    except Exception:
        return 0.0


def _compute_ig_broker_pnl(
    position: Dict[str, Any],
    *,
    asset: str,
    entry_price: float,
    current_price: float,
    direction: str,
) -> Optional[float]:
    broker_size = _coerce_float(position.get("broker_position_size"))
    cash_per_unit = _ig_actual_cash_per_price_unit_per_size(position, asset)
    if broker_size <= 0 or cash_per_unit <= 0 or entry_price <= 0 or current_price <= 0:
        return None
    diff = float(current_price) - float(entry_price)
    if str(direction or "").upper() == "SELL":
        diff = -diff
    return float(diff) * broker_size * cash_per_unit


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
    normalized_position = normalize_position_prices(position)
    entry_price = _normalize_position_price(normalized_position, asset, normalized_position.get("entry_price", 0.0))
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
    snapshot_price_available = False

    if snapshot:
        snapshot_price = snapshot.get("price")
        snapshot_age = _coerce_float(snapshot.get("age_seconds"), default=9999.0)
        if snapshot_price not in (None, 0, 0.0):
            current_price = _normalize_position_price(normalized_position, asset, snapshot_price)
            price_age_seconds = max(0.0, snapshot_age)
            price_live = price_age_seconds <= float(live_snapshot_max_age_seconds or 0.0)
            snapshot_price_available = True

    # Keep the most recent shared live snapshot authoritative for UI pricing.
    # Falling back to a secondary quote path after a small age threshold can
    # reintroduce older cached provider prices and make dashboards jump backward.
    if not snapshot_price_available and provider_fallback is not None and asset:
        try:
            fallback_price, fallback_source = provider_fallback(asset, category)
        except Exception:
            fallback_price, fallback_source = None, ""
        if fallback_price not in (None, 0, 0.0):
            current_price = _normalize_position_price(normalized_position, asset, fallback_price)
            price_source = str(fallback_source or price_source or "")
            price_age_seconds = 0.0
            price_live = True

    if current_price in (None, 0, 0.0):
        current_price = _normalize_position_price(normalized_position, asset, normalized_position.get("current_price", 0.0))
        if not price_source:
            price_source = str(position.get("current_price_source") or "")

    current_price = _normalize_position_price(normalized_position, asset, current_price)
    broker_pnl = _compute_ig_broker_pnl(
        normalized_position,
        asset=asset,
        entry_price=entry_price,
        current_price=current_price,
        direction=direction,
    )
    live_pnl = broker_pnl
    if live_pnl is None:
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
        "price_age_seconds": round(float(price_age_seconds), 3) if price_age_seconds is not None else None,
        "price_live": bool(price_live),
    }
