from __future__ import annotations

from datetime import datetime, timedelta, timezone, tzinfo
from typing import Any, Optional

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None  # type: ignore[assignment]

from config.config import TZ_NAME

_TZ_ALIASES = {
    "EAT": ("Africa/Nairobi", "EAT"),
    "AFRICA/NAIROBI": ("Africa/Nairobi", "EAT"),
    "UTC+3": ("Africa/Nairobi", "EAT"),
    "UTC+03:00": ("Africa/Nairobi", "EAT"),
}


def _parse_fixed_offset(raw: str) -> Optional[timezone]:
    text = str(raw or "").strip().upper()
    if not text.startswith("UTC"):
        return None
    sign_index = max(text.find("+"), text.find("-"))
    if sign_index <= 0:
        return timezone.utc
    sign = 1 if text[sign_index] == "+" else -1
    offset_text = text[sign_index + 1 :]
    if ":" in offset_text:
        hour_text, minute_text = offset_text.split(":", 1)
    else:
        hour_text, minute_text = offset_text, "0"
    try:
        hours = int(hour_text or 0)
        minutes = int(minute_text or 0)
    except Exception:
        return None
    return timezone(sign * timedelta(hours=hours, minutes=minutes))


def _resolve_display_timezone() -> tuple[tzinfo, str]:
    configured = str(TZ_NAME or "").strip() or "EAT"
    alias = _TZ_ALIASES.get(configured.upper())
    if alias:
        zone_name, label = alias
        if ZoneInfo is not None:
            try:
                return ZoneInfo(zone_name), label
            except Exception:
                pass
        return timezone(timedelta(hours=3)), label

    fixed = _parse_fixed_offset(configured)
    if fixed is not None:
        return fixed, configured

    if ZoneInfo is not None:
        try:
            return ZoneInfo(configured), configured
        except Exception:
            pass
    return timezone.utc, "UTC"


DISPLAY_TIMEZONE, DISPLAY_TIMEZONE_LABEL = _resolve_display_timezone()


def display_timezone_label() -> str:
    return DISPLAY_TIMEZONE_LABEL


def coerce_datetime(value: Any) -> Optional[datetime]:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        raw = str(value).strip()
        if not raw:
            return None
        raw = raw.replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(raw)
        except Exception:
            return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def to_display_datetime(value: Any) -> Optional[datetime]:
    dt = coerce_datetime(value)
    if dt is None:
        return None
    return dt.astimezone(DISPLAY_TIMEZONE)


def now_in_display_timezone() -> datetime:
    return datetime.now(DISPLAY_TIMEZONE)


def format_display_datetime(
    value: Any,
    fmt: str = "%d %b %Y %H:%M:%S",
    *,
    include_tz: bool = True,
    default: str = "—",
) -> str:
    dt = to_display_datetime(value)
    if dt is None:
        return default
    rendered = dt.strftime(fmt)
    if include_tz:
        rendered = f"{rendered} {DISPLAY_TIMEZONE_LABEL}"
    return rendered
