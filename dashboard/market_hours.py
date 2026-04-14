from __future__ import annotations

from datetime import datetime, time, timezone, timedelta
from typing import Dict, Tuple, Optional
import pytz  # or stdlib zoneinfo if Python 3.9+

try:
    from zoneinfo import ZoneInfo
    _USE_ZONEINFO = True
except ImportError:
    import pytz as _pytz
    _USE_ZONEINFO = False

from core.asset_profiles import (
    is_crypto, is_forex, is_index, is_commodity,
    US_INDEX_ASSETS, UK_INDEX_ASSETS,
    get_profile,
)
from services.market_hours_guard import build_market_status
from utils.display_time import display_timezone_label, now_in_display_timezone, to_display_datetime


def _now_in(tz_name: str) -> datetime:
    if _USE_ZONEINFO:
        return datetime.now(ZoneInfo(tz_name))
    return datetime.now(_pytz.timezone(tz_name))


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _is_weekday(dt: Optional[datetime] = None) -> bool:
    d = dt or _utc_now()
    return d.weekday() < 5   # 0=Mon … 4=Fri


# ── Per-asset-type rules ──────────────────────────────────────────────────────

def _crypto_open() -> Tuple[bool, str]:
    return True, "24/7"


def _forex_open() -> Tuple[bool, str]:
    """Forex is 24/5 — open from Sunday 22:00 UTC to Friday 22:00 UTC."""
    now = _utc_now()
    wd  = now.weekday()   # 0=Mon 4=Fri 5=Sat 6=Sun
    h   = now.hour

    # Closed Saturday and most of Sunday
    if wd == 5:  # Saturday
        return False, "Weekend (closed)"
    if wd == 6 and h < 22:  # Sunday before 22:00
        return False, f"Weekend (opens Monday 01:00 {display_timezone_label()})"
    if wd == 4 and h >= 22:  # Friday after 22:00
        return False, f"Weekend (closed Saturday 01:00 {display_timezone_label()})"
    return True, "Forex 24/5 open"


def _us_index_open(asset: str) -> Tuple[bool, str]:
    """NYSE / Nasdaq: Mon-Fri 09:30–16:00 Eastern."""
    if not _is_weekday():
        return False, "Weekend (closed)"
    try:
        et = _now_in("America/New_York")
    except Exception:
        # Fallback: UTC-5 in winter / UTC-4 in summer (approximate)
        now = _utc_now()
        # Simple approximation: subtract 5 hours
        et = (now - timedelta(hours=5)).replace(tzinfo=timezone.utc)

    market_open  = time(9, 30)
    market_close = time(16, 0)
    t = et.time()
    local = to_display_datetime(et) or et

    if market_open <= t < market_close:
        return True, f"US market open ({local.strftime('%H:%M')} {display_timezone_label()})"
    if t < market_open:
        return False, f"Pre-market (now {local.strftime('%H:%M')} {display_timezone_label()})"
    return False, f"After hours (closed, now {local.strftime('%H:%M')} {display_timezone_label()})"


def _uk_index_open(asset: str) -> Tuple[bool, str]:
    """LSE (FTSE): Mon-Fri 08:00–16:30 London."""
    if not _is_weekday():
        return False, "Weekend (closed)"
    try:
        lon = _now_in("Europe/London")
    except Exception:
        now = _utc_now()
        lon = (now - timedelta(hours=0)).replace(tzinfo=timezone.utc)  # GMT winter

    market_open  = time(8, 0)
    market_close = time(16, 30)
    t = lon.time()
    local = to_display_datetime(lon) or lon

    if market_open <= t < market_close:
        return True, f"LSE open ({local.strftime('%H:%M')} {display_timezone_label()})"
    if t < market_open:
        return False, f"Pre-market (now {local.strftime('%H:%M')} {display_timezone_label()})"
    return False, f"After hours (now {local.strftime('%H:%M')} {display_timezone_label()})"


def _commodity_open(asset: str) -> Tuple[bool, str]:
    """
    Commodity trading hours approximation:
      Gold (XAU/USD), Silver (XAG/USD): Sun-Fri 18:00-17:00 ET, 1h break 17-18 ET
      WTI Oil (WTI):                    Sun-Fri 18:00-17:00 ET same session
    Closed Saturday and the 1h break 17:00-18:00 ET daily.
    """
    if not _is_weekday():
        # Allow Sunday after 18:00 ET
        try:
            et = _now_in("America/New_York")
        except Exception:
            et = (_utc_now() - timedelta(hours=5)).replace(tzinfo=timezone.utc)
        if et.weekday() == 6 and et.time() >= time(18, 0):
            return True, "Commodity market open (Sunday evening session)"
        return False, "Weekend (closed)"

    try:
        et = _now_in("America/New_York")
    except Exception:
        et = (_utc_now() - timedelta(hours=5)).replace(tzinfo=timezone.utc)
    local = to_display_datetime(et) or et

    t = et.time()
    # Daily break 17:00–18:00 ET
    if time(17, 0) <= t < time(18, 0):
        return False, f"Daily break (now {local.strftime('%H:%M')} {display_timezone_label()})"
    return True, f"Futures open ({local.strftime('%H:%M')} {display_timezone_label()})"


# ── Public API ────────────────────────────────────────────────────────────────

def _provider_market_status(asset: str) -> Optional[Tuple[bool, str, str]]:
    try:
        from services.market_data_router import get_market_status

        profile = get_profile(asset)
        status = get_market_status(asset, category=profile.category)
        if status and "market_open" in status:
            return (
                bool(status["market_open"]),
                str(status.get("reason", "market status unavailable")),
                str(status.get("source", "")),
            )
    except Exception:
        pass
    return None


def is_market_open_for_asset(asset: str) -> Tuple[bool, str]:
    """
    Return (is_open: bool, reason: str) for any canonical asset ID.
    Never returns hardcoded True — always computes from current UTC time.
    """
    status = market_status(asset)
    return bool(status.get("market_open")), str(status.get("reason", "market status unavailable"))


def market_status(asset: str) -> Dict:
    """
    Return a full status dict suitable for the dashboard API response.
    """
    provider_status = _provider_market_status(asset)
    normalized = build_market_status(
        asset,
        category=get_profile(asset).category,
        provider_status=None if provider_status is None else {
            "asset": asset,
            "market_open": provider_status[0],
            "reason": provider_status[1],
            "source": provider_status[2],
        },
    )
    payload = {
        "asset":       asset,
        "market_open": bool(normalized.get("market_open")),
        "reason":      str(normalized.get("reason", "")),
        "utc_now":     _utc_now().strftime("%Y-%m-%d %H:%M UTC"),
        "display_now": now_in_display_timezone().strftime(f"%Y-%m-%d %H:%M {display_timezone_label()}"),
        "display_timezone": display_timezone_label(),
    }
    if normalized.get("source"):
        payload["source"] = str(normalized.get("source") or "market_data_router")
    return payload


def all_market_statuses() -> Dict[str, Dict]:
    """Return market status for all tracked assets."""
    from core.asset_profiles import ALL_ASSETS
    return {asset: market_status(asset) for asset in sorted(ALL_ASSETS)}
