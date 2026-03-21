"""
dashboard/market_hours.py — Real per-asset-type market hours logic.

Replaces the hardcoded "market_open": True in web_app_live.py.

Usage (in web_app_live.py):
    from dashboard.market_hours import is_market_open_for_asset, market_status
"""
from __future__ import annotations

from datetime import datetime, time, timezone, timedelta
from typing import Dict, Tuple, Optional

try:
    from zoneinfo import ZoneInfo
    _USE_ZONEINFO = True
except ImportError:
    import pytz as _pytz
    _USE_ZONEINFO = False

from core.asset_profiles import (
    is_crypto, is_forex, is_commodity,
    US_INDEX_ASSETS, UK_INDEX_ASSETS,
)

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
        return False, "Weekend (opens Sunday 22:00 UTC)"
    if wd == 4 and h >= 22:  # Friday after 22:00
        return False, "Weekend (closed Friday 22:00)"
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

    if market_open <= t < market_close:
        return True, f"US market open ({et.strftime('%H:%M ET')})"
    if t < market_open:
        return False, f"Pre-market (opens 09:30 ET, now {et.strftime('%H:%M ET')})"
    return False, f"After hours (closed at 16:00 ET, now {et.strftime('%H:%M ET')})"


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

    if market_open <= t < market_close:
        return True, f"LSE open ({lon.strftime('%H:%M GMT')})"
    if t < market_open:
        return False, f"Pre-market (opens 08:00 GMT, now {lon.strftime('%H:%M')})"
    return False, f"After hours (closed at 16:30 GMT, now {lon.strftime('%H:%M')})"


def _commodity_open(asset: str) -> Tuple[bool, str]:
    """
    CME COMEX/NYMEX futures:
      Gold (GC=F), Silver (SI=F): Sun-Fri  18:00-17:00 ET (23h/day), 1h break 17-18 ET
      Crude Oil (CL=F):           Sun-Fri  18:00-17:00 ET same session
    Closed Saturday and the 1h break 17:00-18:00 ET daily.
    """
    if not _is_weekday():
        # Allow Sunday after 18:00 ET
        try:
            et = _now_in("America/New_York")
        except Exception:
            et = (_utc_now() - timedelta(hours=5)).replace(tzinfo=timezone.utc)
        if et.weekday() == 6 and et.time() >= time(18, 0):
            return True, "Futures open (Sunday evening session)"
        return False, "Weekend (closed)"

    try:
        et = _now_in("America/New_York")
    except Exception:
        et = (_utc_now() - timedelta(hours=5)).replace(tzinfo=timezone.utc)

    t = et.time()
    # Daily break 17:00–18:00 ET
    if time(17, 0) <= t < time(18, 0):
        return False, "Daily break (17:00-18:00 ET)"
    return True, f"Futures open ({et.strftime('%H:%M ET')})"


# ── Public API ────────────────────────────────────────────────────────────────

def is_market_open_for_asset(asset: str) -> Tuple[bool, str]:
    """
    Return (is_open: bool, reason: str) for any canonical asset ID.
    Never returns hardcoded True — always computes from current UTC time.
    """
    if is_crypto(asset):
        return _crypto_open()
    if is_forex(asset):
        return _forex_open()
    if asset in US_INDEX_ASSETS:
        return _us_index_open(asset)
    if asset in UK_INDEX_ASSETS:
        return _uk_index_open(asset)
    if is_commodity(asset):
        return _commodity_open(asset)
    # Unknown asset — conservative default
    return False, f"Unknown asset type ({asset})"


def market_status(asset: str) -> Dict:
    """
    Return a full status dict suitable for the dashboard API response.
    """
    open_, reason = is_market_open_for_asset(asset)
    return {
        "asset":       asset,
        "market_open": open_,
        "reason":      reason,
        "utc_now":     _utc_now().strftime("%Y-%m-%d %H:%M UTC"),
    }


def all_market_statuses() -> Dict[str, Dict]:
    """Return market status for all 18 assets."""
    from core.asset_profiles import ALL_ASSETS
    return {asset: market_status(asset) for asset in sorted(ALL_ASSETS)}
