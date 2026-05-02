from __future__ import annotations

from datetime import datetime, time as dtime, timedelta, timezone
from typing import Any, Dict, Optional, Tuple

from core.asset_profiles import (
    is_australia_index,
    get_profile,
    is_commodity,
    is_crypto,
    is_europe_index,
    is_forex,
    is_japan_index,
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
_OPEN_SPIKE_WINDOW_MINUTES = 15
_COMMODITY_REOPEN_WINDOW_MINUTES = 10


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


def _open_window_result(
    *,
    asset: str,
    category: str,
    market: str,
    timezone_name: str,
    window_minutes: int,
    minutes_since_open: int,
    label: str,
) -> Dict[str, Any]:
    active = 0 <= int(minutes_since_open) < int(window_minutes)
    reason = ""
    if active:
        reason = (
            f"{label}: first {int(window_minutes)}m after the open "
            f"({int(minutes_since_open)}m elapsed)"
        )
    return {
        "asset": asset,
        "category": category,
        "market": market,
        "timezone": timezone_name,
        "label": label,
        "window_minutes": int(window_minutes),
        "minutes_since_open": int(minutes_since_open),
        "active": active,
        "reason": reason,
    }


def _parse_utc_datetime(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except Exception:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _provider_close_buffer_gate(
    provider_status: Optional[Dict[str, Any]],
    now_utc: datetime,
) -> Optional[Tuple[bool, str, bool, str]]:
    if not isinstance(provider_status, dict):
        return None
    if provider_status.get("market_open") is not True:
        return None

    close_dt = _parse_utc_datetime(
        provider_status.get("session_close_utc")
        or provider_status.get("current_session_close_utc")
        or provider_status.get("close_time_utc")
        or provider_status.get("next_close_utc")
    )
    if close_dt is None:
        return None

    remaining = close_dt - now_utc
    if remaining.total_seconds() <= 0:
        return False, "Provider session ended", False, ""

    if remaining <= timedelta(minutes=_CLOSE_BUFFER_MINUTES):
        reason = (
            "Close buffer: last hour before provider close "
            f"({close_dt.strftime('%Y-%m-%d %H:%M UTC')})"
        )
        return False, reason, True, reason

    return True, "", False, ""


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
        if is_europe_index(asset):
            ber = _now_in_tz("Europe/Berlin", now)
            if ber.weekday() < 5 and dtime(16, 30) <= ber.time() < dtime(17, 30):
                return True, "Close buffer: last hour before Europe close (17:30 Berlin)"
            return False, ""
        if is_australia_index(asset):
            syd = _now_in_tz("Australia/Sydney", now)
            if syd.weekday() < 5 and dtime(15, 0) <= syd.time() < dtime(16, 0):
                return True, "Close buffer: last hour before Australia close (16:00 Sydney)"
            return False, ""
        if is_japan_index(asset):
            tok = _now_in_tz("Asia/Tokyo", now)
            if tok.weekday() < 5 and dtime(14, 0) <= tok.time() < dtime(15, 0):
                return True, "Close buffer: last hour before Japan close (15:00 Tokyo)"
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


def _europe_index_status(asset: str, now_utc: datetime) -> Tuple[bool, str]:
    buffer_active, buffer_reason = close_buffer_status(asset, "indices", now_utc=now_utc)
    if buffer_active:
        return False, buffer_reason
    ber = _now_in_tz("Europe/Berlin", now_utc)
    wd = ber.weekday()
    t = ber.time()

    if wd >= 5:
        return False, "Weekend closed"
    if t < dtime(9, 0):
        return False, "Pre-market"
    if t >= dtime(17, 30):
        return False, "After hours"
    return True, "Europe market open"


def _australia_index_status(asset: str, now_utc: datetime) -> Tuple[bool, str]:
    buffer_active, buffer_reason = close_buffer_status(asset, "indices", now_utc=now_utc)
    if buffer_active:
        return False, buffer_reason
    syd = _now_in_tz("Australia/Sydney", now_utc)
    wd = syd.weekday()
    t = syd.time()

    if wd >= 5:
        return False, "Weekend closed"
    if t < dtime(10, 0):
        return False, "Pre-market"
    if t >= dtime(16, 0):
        return False, "After hours"
    return True, "Australia market open"


def _japan_index_status(asset: str, now_utc: datetime) -> Tuple[bool, str]:
    buffer_active, buffer_reason = close_buffer_status(asset, "indices", now_utc=now_utc)
    if buffer_active:
        return False, buffer_reason
    tok = _now_in_tz("Asia/Tokyo", now_utc)
    wd = tok.weekday()
    t = tok.time()

    if wd >= 5:
        return False, "Weekend closed"
    if t < dtime(9, 0):
        return False, "Pre-market"
    if dtime(11, 30) <= t < dtime(12, 30):
        return False, "Lunch break"
    if t >= dtime(15, 0):
        return False, "After hours"
    return True, "Japan market open"


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
        if is_europe_index(asset):
            return _europe_index_status(asset, now)
        if is_australia_index(asset):
            return _australia_index_status(asset, now)
        if is_japan_index(asset):
            return _japan_index_status(asset, now)
        return False, "Unknown index asset"
    if resolved_category == "commodities" or is_commodity(asset):
        return _commodity_status(asset, now)
    return False, f"Unknown asset type ({asset})"


def open_spike_status(
    asset: str,
    category: str = "",
    *,
    now_utc: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    Return whether the asset is inside its first-minutes open-chaos window.

    This is intentionally narrow. It focuses on exchange-driven opens and the
    daily/sunday futures reopen where generic entries are more vulnerable to
    spread spikes and whipsaw than during the rest of the session.
    """

    resolved_category = _resolved_category(asset, category)
    now = _utc_now(now_utc)
    inactive = {
        "asset": asset,
        "category": resolved_category,
        "market": "",
        "timezone": "UTC",
        "label": "",
        "window_minutes": 0,
        "minutes_since_open": None,
        "active": False,
        "reason": "",
    }

    if resolved_category == "crypto" or is_crypto(asset):
        return dict(inactive)

    if resolved_category == "indices":
        if is_us_index(asset):
            local_now = _now_in_tz("America/New_York", now)
            if local_now.weekday() >= 5 or local_now.time() < dtime(9, 30):
                return dict(inactive)
            minutes = int((local_now - local_now.replace(hour=9, minute=30, second=0, microsecond=0)).total_seconds() // 60)
            return _open_window_result(
                asset=asset,
                category=resolved_category,
                market="us_index_open",
                timezone_name="America/New_York",
                window_minutes=_OPEN_SPIKE_WINDOW_MINUTES,
                minutes_since_open=minutes,
                label="US cash open",
            )
        if is_uk_index(asset):
            local_now = _now_in_tz("Europe/London", now)
            if local_now.weekday() >= 5 or local_now.time() < dtime(8, 0):
                return dict(inactive)
            minutes = int((local_now - local_now.replace(hour=8, minute=0, second=0, microsecond=0)).total_seconds() // 60)
            return _open_window_result(
                asset=asset,
                category=resolved_category,
                market="uk_index_open",
                timezone_name="Europe/London",
                window_minutes=_OPEN_SPIKE_WINDOW_MINUTES,
                minutes_since_open=minutes,
                label="UK cash open",
            )
        if is_europe_index(asset):
            local_now = _now_in_tz("Europe/Berlin", now)
            if local_now.weekday() >= 5 or local_now.time() < dtime(9, 0):
                return dict(inactive)
            minutes = int((local_now - local_now.replace(hour=9, minute=0, second=0, microsecond=0)).total_seconds() // 60)
            return _open_window_result(
                asset=asset,
                category=resolved_category,
                market="europe_index_open",
                timezone_name="Europe/Berlin",
                window_minutes=_OPEN_SPIKE_WINDOW_MINUTES,
                minutes_since_open=minutes,
                label="Europe cash open",
            )
        if is_australia_index(asset):
            local_now = _now_in_tz("Australia/Sydney", now)
            if local_now.weekday() >= 5 or local_now.time() < dtime(10, 0):
                return dict(inactive)
            minutes = int((local_now - local_now.replace(hour=10, minute=0, second=0, microsecond=0)).total_seconds() // 60)
            return _open_window_result(
                asset=asset,
                category=resolved_category,
                market="australia_index_open",
                timezone_name="Australia/Sydney",
                window_minutes=_OPEN_SPIKE_WINDOW_MINUTES,
                minutes_since_open=minutes,
                label="Australia cash open",
            )
        if is_japan_index(asset):
            local_now = _now_in_tz("Asia/Tokyo", now)
            if local_now.weekday() >= 5 or local_now.time() < dtime(9, 0):
                return dict(inactive)
            minutes = int((local_now - local_now.replace(hour=9, minute=0, second=0, microsecond=0)).total_seconds() // 60)
            return _open_window_result(
                asset=asset,
                category=resolved_category,
                market="japan_index_open",
                timezone_name="Asia/Tokyo",
                window_minutes=_OPEN_SPIKE_WINDOW_MINUTES,
                minutes_since_open=minutes,
                label="Japan cash open",
            )
        return dict(inactive)

    if resolved_category == "commodities" or is_commodity(asset):
        local_now = _now_in_tz("America/New_York", now)
        wd = local_now.weekday()
        if wd == 5:
            return dict(inactive)
        if local_now.time() < dtime(18, 0):
            return dict(inactive)
        if wd == 4:
            return dict(inactive)
        if wd not in {6, 0, 1, 2, 3}:
            return dict(inactive)
        minutes = int((local_now - local_now.replace(hour=18, minute=0, second=0, microsecond=0)).total_seconds() // 60)
        return _open_window_result(
            asset=asset,
            category=resolved_category,
            market="commodity_reopen",
            timezone_name="America/New_York",
            window_minutes=_COMMODITY_REOPEN_WINDOW_MINUTES,
            minutes_since_open=minutes,
            label="futures reopen",
        )

    return dict(inactive)


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
    crypto_24x7 = resolved_category == "crypto" or is_crypto(asset)
    provider_gate = None if crypto_24x7 else _provider_close_buffer_gate(provider_status, now)
    if provider_gate is not None:
        gate_open, gate_reason, buffer_active, buffer_reason = provider_gate
    else:
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
