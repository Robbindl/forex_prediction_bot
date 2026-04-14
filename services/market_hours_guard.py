from __future__ import annotations

from datetime import datetime, time as dtime, timezone
from typing import Any, Dict, Optional, Tuple

from core.asset_profiles import (
    get_profile,
    is_commodity,
    is_crypto,
    is_forex,
    is_uk_index,
    is_us_index,
)

try:
    from zoneinfo import ZoneInfo

    _HAS_ZONEINFO = True
except ImportError:  # pragma: no cover - Python < 3.9 fallback
    import pytz as _pytz

    _HAS_ZONEINFO = False


_CLOSE_BUFFER_MINUTES = 60


def _utc_now(now_utc: Optional[datetime] = None) -> datetime:
    if now_utc is None:
        return datetime.now(timezone.utc)
    if now_utc.tzinfo is None:
        return now_utc.replace(tzinfo=timezone.utc)
    return now_utc.astimezone(timezone.utc)


def _now_in_tz(tz_name: str, now_utc: datetime) -> datetime:
    if _HAS_ZONEINFO:
        return now_utc.astimezone(ZoneInfo(tz_name))
    return now_utc.astimezone(_pytz.timezone(tz_name))


def _resolved_category(asset: str, category: str = "") -> str:
    resolved = str(category or get_profile(asset).category or "").strip().lower()
    return resolved


def close_buffer_status(
    asset: str,
    category: str = "",
    *,
    now_utc: Optional[datetime] = None,
) -> Tuple[bool, str]:
    """
    Return whether the asset is inside the one-hour pre-close flatten window.
    """

    resolved_category = _resolved_category(asset, category)
    now = _utc_now(now_utc)

    if resolved_category == "crypto" or is_crypto(asset):
        return False, ""

    if resolved_category == "forex" or is_forex(asset):
        if now.weekday() == 4 and dtime(21, 0) <= now.time() < dtime(22, 0):
            return True, "Close buffer: last hour before Friday 22:00 UTC close"
        return False, ""

    if resolved_category == "indices":
        if is_us_index(asset):
            et = _now_in_tz("America/New_York", now)
            if et.weekday() < 5 and dtime(15, 0) <= et.time() < dtime(16, 0):
                return True, "Close buffer: last hour before US close (16:00 ET)"
            return False, ""
        if is_uk_index(asset):
            lon = _now_in_tz("Europe/London", now)
            if lon.weekday() < 5 and dtime(15, 30) <= lon.time() < dtime(16, 30):
                return True, "Close buffer: last hour before UK close (16:30 London)"
            return False, ""
        return False, ""

    if resolved_category == "commodities" or is_commodity(asset):
        et = _now_in_tz("America/New_York", now)
        if et.weekday() in {0, 1, 2, 3} and dtime(16, 0) <= et.time() < dtime(17, 0):
            return True, "Close buffer: last hour before futures close (17:00 ET)"
        if et.weekday() == 4 and dtime(16, 0) <= et.time() < dtime(17, 0):
            return True, "Close buffer: last hour before Friday 17:00 ET close"
        return False, ""

    return False, ""


def _forex_status(now_utc: datetime) -> Tuple[bool, str]:
    buffer_active, buffer_reason = close_buffer_status("", "forex", now_utc=now_utc)
    if buffer_active:
        return False, buffer_reason
    wd = now_utc.weekday()
    t = now_utc.time()

    if wd == 5:
        return False, "Weekend closed"
    if wd == 6 and t < dtime(22, 0):
        return False, "Closed until Sunday 22:00 UTC"
    if wd == 4 and t >= dtime(22, 0):
        return False, "Weekend closed"
    return True, "Forex open 24/5"


def _us_index_status(asset: str, now_utc: datetime) -> Tuple[bool, str]:
    buffer_active, buffer_reason = close_buffer_status(asset, "indices", now_utc=now_utc)
    if buffer_active:
        return False, buffer_reason
    et = _now_in_tz("America/New_York", now_utc)
    wd = et.weekday()
    t = et.time()

    if wd >= 5:
        return False, "Weekend closed"
    if t < dtime(9, 30):
        return False, "Pre-market"
    if t >= dtime(16, 0):
        return False, "After hours"
    return True, "US market open"


def _uk_index_status(asset: str, now_utc: datetime) -> Tuple[bool, str]:
    buffer_active, buffer_reason = close_buffer_status(asset, "indices", now_utc=now_utc)
    if buffer_active:
        return False, buffer_reason
    lon = _now_in_tz("Europe/London", now_utc)
    wd = lon.weekday()
    t = lon.time()

    if wd >= 5:
        return False, "Weekend closed"
    if t < dtime(8, 0):
        return False, "Pre-market"
    if t >= dtime(16, 30):
        return False, "After hours"
    return True, "UK market open"


def _commodity_status(asset: str, now_utc: datetime) -> Tuple[bool, str]:
    buffer_active, buffer_reason = close_buffer_status(asset, "commodities", now_utc=now_utc)
    if buffer_active:
        return False, buffer_reason
    et = _now_in_tz("America/New_York", now_utc)
    wd = et.weekday()
    t = et.time()

    if wd == 5:
        return False, "Weekend closed"
    if wd == 6 and t < dtime(18, 0):
        return False, "Closed until Sunday 18:00 ET"
    if wd in {0, 1, 2, 3}:
        if dtime(17, 0) <= t < dtime(18, 0):
            return False, "Daily break"
        return True, "Futures open"
    if wd == 4:
        if t < dtime(17, 0):
            return True, "Futures open"
        return False, "Weekend closed"
    if wd == 6 and t >= dtime(18, 0):
        return True, "Futures open"
    return False, "Weekend closed"


def session_market_status(
    asset: str,
    category: str = "",
    *,
    now_utc: Optional[datetime] = None,
) -> Tuple[bool, str]:
    """
    Return the rule-based market gate for an asset.

    The gate is conservative: it blocks the last hour before known cash/futures
    closes so the bot does not open new trades into weekend or session gaps.
    """

    resolved_category = _resolved_category(asset, category)
    now = _utc_now(now_utc)

    if resolved_category == "crypto" or is_crypto(asset):
        return True, "crypto_24x7"
    if resolved_category == "forex" or is_forex(asset):
        return _forex_status(now)
    if resolved_category == "indices":
        if is_us_index(asset):
            return _us_index_status(asset, now)
        if is_uk_index(asset):
            return _uk_index_status(asset, now)
        return False, "Unknown index asset"
    if resolved_category == "commodities" or is_commodity(asset):
        return _commodity_status(asset, now)
    return False, f"Unknown asset type ({asset})"


def build_market_status(
    asset: str,
    category: str = "",
    *,
    provider_status: Optional[Dict[str, Any]] = None,
    now_utc: Optional[datetime] = None,
    source: str = "",
) -> Dict[str, Any]:
    """
    Normalize a provider market-status payload and enforce the session gate.

    Provider status is always authoritative when it reports the market as
    closed. When the provider says open, the conservative close-buffer rule
    can still force the market closed.
    """

    resolved_category = _resolved_category(asset, category)
    now = _utc_now(now_utc)
    buffer_active, buffer_reason = close_buffer_status(asset, resolved_category, now_utc=now)
    gate_open, gate_reason = session_market_status(asset, resolved_category, now_utc=now)

    provider_open: Optional[bool] = None
    provider_reason = ""
    provider_source = source
    payload: Dict[str, Any] = {}

    if isinstance(provider_status, dict):
        payload = {
            key: value
            for key, value in provider_status.items()
            if key not in {"asset", "category", "market_open", "reason", "source", "utc_now"}
        }
        if "market_open" in provider_status:
            provider_open = bool(provider_status.get("market_open"))
            provider_reason = str(provider_status.get("reason") or "").strip()
        provider_source = str(provider_status.get("source") or provider_source or "").strip()

    if provider_open is None:
        market_open = gate_open
        reason = gate_reason
    elif provider_open is False:
        market_open = False
        reason = provider_reason or gate_reason or "market closed"
    elif not gate_open:
        market_open = False
        reason = gate_reason or provider_reason or "market closed"
    else:
        market_open = True
        reason = provider_reason or gate_reason or "open"

    result = {
        "asset": asset,
        "category": resolved_category,
        "market_open": market_open,
        "reason": reason,
        "source": provider_source or "session_rules",
        "utc_now": now.strftime("%Y-%m-%d %H:%M UTC"),
        "close_buffer_minutes": _CLOSE_BUFFER_MINUTES,
        "close_buffer_active": buffer_active,
    }
    if buffer_active:
        result["close_buffer_reason"] = buffer_reason
    result.update(payload)
    return result
