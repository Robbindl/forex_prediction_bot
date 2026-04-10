from __future__ import annotations

import json
import threading
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

from config.config import (
    ECON_CALENDAR_ALLOW_TRADING_ECONOMICS_GUEST,
    ECON_CALENDAR_FOREX_FACTORY_ENABLED,
    ECON_CALENDAR_HTTP_TIMEOUT,
    TRADING_ECONOMICS_CREDENTIALS,
)
from services.deriv_bridge import deriv_bridge
from utils.display_time import format_display_datetime
from utils.logger import get_logger

logger = get_logger()

_CACHE_TTL_SEC = 20 * 60
_FOREX_FACTORY_WEEK_URL = "https://nfs.faireconomy.media/ff_calendar_thisweek.xml"
_TRADING_ECONOMICS_URL = (
    "https://api.tradingeconomics.com/calendar/country/All/{start}/{end}"
)

_TE_COUNTRY_TO_CURRENCY = {
    "australia": "AUD",
    "austria": "EUR",
    "belgium": "EUR",
    "canada": "CAD",
    "china": "CNY",
    "euro area": "EUR",
    "european union": "EUR",
    "european monetary union": "EUR",
    "finland": "EUR",
    "france": "EUR",
    "germany": "EUR",
    "greece": "EUR",
    "ireland": "EUR",
    "italy": "EUR",
    "japan": "JPY",
    "netherlands": "EUR",
    "new zealand": "NZD",
    "portugal": "EUR",
    "spain": "EUR",
    "switzerland": "CHF",
    "united kingdom": "GBP",
    "uk": "GBP",
    "united states": "USD",
    "us": "USD",
}


def _to_utc_datetime(value: Any) -> Optional[datetime]:
    if value in (None, ""):
        return None
    try:
        if isinstance(value, datetime):
            return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
        if isinstance(value, (int, float)):
            magnitude = abs(float(value))
            if magnitude >= 1_000_000_000_000:
                return datetime.fromtimestamp(float(value) / 1000.0, tz=timezone.utc)
            return datetime.fromtimestamp(float(value), tz=timezone.utc)

        text = str(value).strip()
        if not text:
            return None
        if text.isdigit():
            numeric = int(text)
            if len(text) >= 13:
                return datetime.fromtimestamp(numeric / 1000.0, tz=timezone.utc)
            return datetime.fromtimestamp(numeric, tz=timezone.utc)

        text = text.replace("Z", "+00:00")
        parsed = datetime.fromisoformat(text)
        return parsed.astimezone(timezone.utc) if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _impact_label(value: Any) -> str:
    if value is None:
        return "MEDIUM"
    if isinstance(value, (int, float)):
        numeric = float(value)
        if numeric >= 3:
            return "HIGH"
        if numeric >= 2:
            return "MEDIUM"
        return "LOW"

    text = str(value).strip().upper()
    if not text:
        return "MEDIUM"
    if text in {"HIGH", "MEDIUM", "LOW"}:
        return text
    if "HIGH" in text or text in {"3", "STRONG"}:
        return "HIGH"
    if "MEDIUM" in text or text == "2":
        return "MEDIUM"
    if "LOW" in text or text == "1":
        return "LOW"
    return "MEDIUM"


def _parse_forexfactory_datetime(date_text: str, time_text: str) -> Optional[datetime]:
    date_value = (date_text or "").strip()
    time_value = (time_text or "").strip().lower()
    if not date_value:
        return None

    try:
        base_date = datetime.strptime(date_value, "%m-%d-%Y").date()
    except ValueError:
        return None

    if not time_value or time_value in {"all day", "tentative"}:
        return None

    time_value = time_value.replace(" ", "")
    for fmt in ("%I:%M%p", "%I%p"):
        try:
            parsed_time = datetime.strptime(time_value, fmt).time()
            return datetime.combine(base_date, parsed_time, tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


class EconomicCalendarService:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._cache: Dict[str, Tuple[float, List[Dict[str, Any]]]] = {}
        self._forexfactory_cache: Tuple[float, List[Dict[str, Any]]] = (0.0, [])
        self._provider_notice_key = ""
        self._provider_notice_at = 0.0
        self._no_provider_notice_at = 0.0

    def get_high_impact_events(
        self,
        days: int = 3,
        currencies: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        start = datetime.now(timezone.utc)
        end = start + timedelta(days=max(1, days))
        return self.get_economic_events(
            start_time=start,
            end_time=end,
            currencies=currencies,
            impacts=["HIGH", "MEDIUM"],
        )

    def get_economic_events(
        self,
        start_time: Any = None,
        end_time: Any = None,
        currencies: Optional[List[str]] = None,
        impacts: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        start = _to_utc_datetime(start_time) or datetime.now(timezone.utc)
        end = _to_utc_datetime(end_time) or (start + timedelta(days=3))
        if end < start:
            start, end = end, start

        currency_filter = sorted({str(item).upper() for item in currencies or [] if item})
        impact_filter = sorted({_impact_label(item) for item in impacts or ["HIGH", "MEDIUM"]})
        cache_key = json.dumps(
            {
                "start": start.isoformat(),
                "end": end.isoformat(),
                "currencies": currency_filter,
                "impacts": impact_filter,
            },
            sort_keys=True,
        )

        with self._lock:
            cached = self._cache.get(cache_key)
            if cached and (time.monotonic() - cached[0]) < _CACHE_TTL_SEC:
                return list(cached[1])

        providers_used: List[str] = []
        combined: List[Dict[str, Any]] = []

        deriv_events = self._get_deriv_events(start, end, currency_filter, impact_filter)
        if deriv_events:
            providers_used.append("Deriv")
            combined.extend(deriv_events)

        te_events = self._get_trading_economics_events(start, end, currency_filter, impact_filter)
        if te_events:
            providers_used.append("TradingEconomics")
            combined.extend(te_events)

        ff_events = self._get_forex_factory_events(start, end, currency_filter, impact_filter)
        if ff_events:
            providers_used.append("ForexFactory")
            combined.extend(ff_events)

        deduped = self._dedupe_events(combined)
        with self._lock:
            self._cache[cache_key] = (time.monotonic(), list(deduped))

        if deduped and providers_used:
            self._log_provider_selection(providers_used)
        elif not deduped:
            self._log_no_provider_available()

        return deduped

    def _get_deriv_events(
        self,
        start: datetime,
        end: datetime,
        currencies: List[str],
        impacts: List[str],
    ) -> List[Dict[str, Any]]:
        try:
            return deriv_bridge.get_economic_events(
                start_time=start,
                end_time=end,
                currencies=currencies,
                impacts=impacts,
            )
        except Exception as exc:
            logger.debug(f"[EconomicCalendar] Deriv provider failed: {exc}")
            return []

    def _get_trading_economics_events(
        self,
        start: datetime,
        end: datetime,
        currencies: List[str],
        impacts: List[str],
    ) -> List[Dict[str, Any]]:
        credentials = TRADING_ECONOMICS_CREDENTIALS
        if not credentials and not ECON_CALENDAR_ALLOW_TRADING_ECONOMICS_GUEST:
            return []

        auth = credentials or "guest:guest"
        params = {
            "c": auth,
            "f": "json",
        }
        importance_codes = []
        if "HIGH" in impacts:
            importance_codes.append("3")
        if "MEDIUM" in impacts:
            importance_codes.append("2")
        if "LOW" in impacts:
            importance_codes.append("1")
        if importance_codes:
            params["importance"] = ",".join(sorted(set(importance_codes)))

        url = _TRADING_ECONOMICS_URL.format(
            start=start.strftime("%Y-%m-%d"),
            end=end.strftime("%Y-%m-%d"),
        )
        try:
            response = requests.get(
                url,
                params=params,
                timeout=ECON_CALENDAR_HTTP_TIMEOUT,
                headers={"User-Agent": "forex_prediction_bot/1.0"},
            )
            if response.status_code != 200:
                logger.debug(
                    f"[EconomicCalendar] TradingEconomics HTTP {response.status_code}"
                )
                return []
            payload = response.json()
        except Exception as exc:
            logger.debug(f"[EconomicCalendar] TradingEconomics fetch failed: {exc}")
            return []

        if not isinstance(payload, list):
            return []

        currency_filter = set(currencies)
        impact_filter = set(impacts)
        rows: List[Dict[str, Any]] = []
        for item in payload:
            if not isinstance(item, dict):
                continue

            event_dt = (
                _to_utc_datetime(item.get("Date"))
                or _to_utc_datetime(item.get("ReferenceDate"))
            )
            if event_dt is None or event_dt < start or event_dt > end:
                continue

            currency = self._map_te_currency(item)
            if currency_filter and currency not in currency_filter:
                continue

            impact = _impact_label(item.get("Importance"))
            if impact_filter and impact not in impact_filter:
                continue

            rows.append(
                {
                    "date": format_display_datetime(event_dt, "%Y-%m-%d %H:%M"),
                    "event": str(item.get("Event") or item.get("Category") or ""),
                    "impact": impact,
                    "actual": item.get("Actual"),
                    "estimate": item.get("Forecast"),
                    "forecast": item.get("Forecast"),
                    "previous": item.get("Previous"),
                    "currency": currency,
                    "surprise_direction": "",
                    "source": "TradingEconomics",
                }
            )

        return rows

    def _get_forex_factory_events(
        self,
        start: datetime,
        end: datetime,
        currencies: List[str],
        impacts: List[str],
    ) -> List[Dict[str, Any]]:
        if not ECON_CALENDAR_FOREX_FACTORY_ENABLED:
            return []

        try:
            rows = self._load_forex_factory_week()
        except Exception as exc:
            logger.debug(f"[EconomicCalendar] ForexFactory fetch failed: {exc}")
            return []

        currency_filter = set(currencies)
        impact_filter = set(impacts)
        result: List[Dict[str, Any]] = []
        for item in rows:
            event_dt = _to_utc_datetime(item.get("date"))
            if event_dt is None or event_dt < start or event_dt > end:
                continue
            currency = str(item.get("currency") or "").upper()
            impact = _impact_label(item.get("impact"))
            if currency_filter and currency not in currency_filter:
                continue
            if impact_filter and impact not in impact_filter:
                continue
            result.append(
                {
                    "date": format_display_datetime(event_dt, "%Y-%m-%d %H:%M"),
                    "event": str(item.get("event") or ""),
                    "impact": impact,
                    "actual": item.get("actual"),
                    "estimate": item.get("forecast"),
                    "forecast": item.get("forecast"),
                    "previous": item.get("previous"),
                    "currency": currency,
                    "surprise_direction": "",
                    "source": "ForexFactory",
                }
            )
        return result

    def _load_forex_factory_week(self) -> List[Dict[str, Any]]:
        with self._lock:
            loaded_at, cached = self._forexfactory_cache
            if cached and (time.monotonic() - loaded_at) < _CACHE_TTL_SEC:
                return list(cached)

        response = requests.get(
            _FOREX_FACTORY_WEEK_URL,
            timeout=ECON_CALENDAR_HTTP_TIMEOUT,
            headers={"User-Agent": "forex_prediction_bot/1.0"},
        )
        response.raise_for_status()
        encoding = response.encoding or "windows-1252"
        root = ET.fromstring(response.content.decode(encoding, errors="replace"))

        items: List[Dict[str, Any]] = []
        for event in root.findall(".//event"):
            event_dt = _parse_forexfactory_datetime(
                event.findtext("date", default=""),
                event.findtext("time", default=""),
            )
            if event_dt is None:
                continue
            impact = _impact_label(event.findtext("impact", default=""))
            currency = str(event.findtext("country", default="") or "").upper()
            items.append(
                {
                    "date": event_dt.isoformat(),
                    "event": str(event.findtext("title", default="") or ""),
                    "impact": impact,
                    "actual": event.findtext("actual", default=""),
                    "forecast": event.findtext("forecast", default=""),
                    "previous": event.findtext("previous", default=""),
                    "currency": currency,
                    "source": "ForexFactory",
                }
            )

        with self._lock:
            self._forexfactory_cache = (time.monotonic(), list(items))
        return items

    @staticmethod
    def _map_te_currency(item: Dict[str, Any]) -> str:
        explicit = str(item.get("Currency") or item.get("currency") or "").upper()
        if explicit:
            return explicit
        country = str(item.get("Country") or "").strip().lower()
        return _TE_COUNTRY_TO_CURRENCY.get(country, "")

    @staticmethod
    def _dedupe_events(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        deduped: Dict[str, Dict[str, Any]] = {}
        for item in items:
            key = "|".join(
                [
                    str(item.get("date") or ""),
                    str(item.get("currency") or ""),
                    str(item.get("event") or ""),
                ]
            )
            if not key.strip("|"):
                continue
            existing = deduped.get(key)
            if existing is None:
                deduped[key] = item
                continue
            if str(existing.get("source") or "") == "Deriv":
                continue
            if str(item.get("source") or "") == "Deriv":
                deduped[key] = item
        return sorted(deduped.values(), key=lambda item: item.get("date", ""))

    def _log_provider_selection(self, providers_used: List[str]) -> None:
        providers = ",".join(sorted(set(providers_used)))
        deriv_supported = getattr(deriv_bridge, "_economic_calendar_supported", None)
        key = f"{providers}|{deriv_supported}"
        now = time.monotonic()
        if key == self._provider_notice_key and (now - self._provider_notice_at) < 3600:
            return
        self._provider_notice_key = key
        self._provider_notice_at = now
        if deriv_supported is False and providers and providers != "Deriv":
            logger.info(
                f"[EconomicCalendar] Deriv calendar unsupported on this endpoint — "
                f"using fallback provider(s): {providers}"
            )
        else:
            logger.debug(f"[EconomicCalendar] Active provider(s): {providers}")

    def _log_no_provider_available(self) -> None:
        now = time.monotonic()
        if (now - self._no_provider_notice_at) < 900:
            return
        self._no_provider_notice_at = now
        logger.debug("[EconomicCalendar] No economic calendar events available from active providers")


economic_calendar_service = EconomicCalendarService()
