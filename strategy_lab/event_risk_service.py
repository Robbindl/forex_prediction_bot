from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pandas as pd

from services.economic_calendar_service import economic_calendar_service as deriv_bridge
from utils.logger import get_logger

logger = get_logger()

_MACRO_CURRENCIES = {
    "USD",
    "EUR",
    "GBP",
    "JPY",
    "AUD",
    "CAD",
    "CHF",
    "NZD",
}

_POSITIVE_HIGHER_KEYWORDS = (
    "gdp",
    "payroll",
    "employment",
    "retail sales",
    "pmi",
    "manufacturing",
    "services",
    "consumer confidence",
    "durable goods",
    "housing starts",
    "industrial production",
)

_HAWKISH_HIGHER_KEYWORDS = (
    "cpi",
    "inflation",
    "ppi",
    "core pce",
    "pce",
    "interest rate",
    "rate decision",
)

_NEGATIVE_HIGHER_KEYWORDS = (
    "unemployment",
    "jobless",
    "claimant",
)


def _to_utc_timestamp(value: Any) -> Optional[pd.Timestamp]:
    if value in (None, ""):
        return None
    try:
        ts = pd.Timestamp(value)
        if ts.tzinfo is None:
            return ts.tz_localize(timezone.utc)
        return ts.tz_convert(timezone.utc)
    except Exception:
        return None


def _as_list(value: Any) -> List[Any]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        return list(value)
    return [value]


def _parse_numeric_value(value: Any) -> Optional[float]:
    if value in (None, "", "N/A", "n/a", "-"):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().lower().replace(",", "")
    if not text:
        return None

    multiplier = 1.0
    if text.endswith("%"):
        text = text[:-1]
    elif text.endswith("k"):
        multiplier = 1_000.0
        text = text[:-1]
    elif text.endswith("m"):
        multiplier = 1_000_000.0
        text = text[:-1]
    elif text.endswith("b"):
        multiplier = 1_000_000_000.0
        text = text[:-1]

    try:
        return float(text) * multiplier
    except Exception:
        return None


class EventRiskService:
    _blackout_cache: Dict[Tuple[Any, ...], List[Dict[str, Any]]] = {}
    _macro_bias_cache: Dict[Tuple[Any, ...], List[Dict[str, Any]]] = {}

    @classmethod
    def clear_cache(cls) -> None:
        cls._blackout_cache.clear()
        cls._macro_bias_cache.clear()

    @classmethod
    def get_blackout_windows(
        cls,
        asset: str,
        category: str,
        start_time: Any,
        end_time: Any,
        config: Dict[str, Any] | None = None,
    ) -> List[Dict[str, Any]]:
        cfg = dict(config or {})
        if cfg.get("enabled") is False:
            return []

        start_ts = _to_utc_timestamp(start_time)
        end_ts = _to_utc_timestamp(end_time)
        if start_ts is None or end_ts is None:
            return []
        if end_ts < start_ts:
            start_ts, end_ts = end_ts, start_ts

        category_key = str(category or "").lower()
        lookback_minutes = max(0, int(cls._resolve_value(cfg.get("lookback_minutes"), category_key, 30) or 30))
        lookahead_minutes = max(0, int(cls._resolve_value(cfg.get("lookahead_minutes"), category_key, 30) or 30))
        impacts = cls._normalize_impacts(cls._resolve_value(cfg.get("impacts"), category_key, ["HIGH"]))
        currencies = cls._resolve_currencies(
            asset,
            category_key,
            cls._resolve_value(cfg.get("currencies", "auto"), category_key, "auto"),
        )

        fetch_start = (start_ts - pd.Timedelta(minutes=lookback_minutes)).to_pydatetime()
        fetch_end = (end_ts + pd.Timedelta(minutes=lookahead_minutes)).to_pydatetime()
        cache_key = (
            str(asset or "").upper(),
            category_key,
            start_ts.isoformat(),
            end_ts.isoformat(),
            tuple(currencies),
            tuple(impacts),
            lookback_minutes,
            lookahead_minutes,
        )
        cached = cls._blackout_cache.get(cache_key)
        if cached is not None:
            return list(cached)

        windows: List[Dict[str, Any]] = []
        try:
            events = deriv_bridge.get_economic_events(
                start_time=fetch_start,
                end_time=fetch_end,
                currencies=currencies,
                impacts=impacts,
            )
        except Exception as exc:
            logger.debug(f"[EventRiskService] economic event fetch failed for {asset}: {exc}")
            events = []

        for event in events or []:
            event_ts = _to_utc_timestamp(
                event.get("date")
                or event.get("event_date")
                or event.get("release_date")
                or event.get("datetime")
                or event.get("timestamp")
            )
            if event_ts is None:
                continue

            blackout_start = event_ts - pd.Timedelta(minutes=lookback_minutes)
            blackout_end = event_ts + pd.Timedelta(minutes=lookahead_minutes)
            if blackout_end < start_ts or blackout_start > end_ts:
                continue

            windows.append(
                {
                    "start": blackout_start,
                    "end": blackout_end,
                    "event_time": event_ts,
                    "event": str(event.get("event") or event.get("title") or event.get("description") or ""),
                    "impact": str(event.get("impact") or "").upper(),
                    "currency": str(event.get("currency") or "").upper(),
                    "source": str(event.get("source") or "Deriv"),
                }
            )

        windows.sort(key=lambda item: item["start"])
        cls._blackout_cache[cache_key] = list(windows)
        return windows

    @classmethod
    def get_macro_bias_windows(
        cls,
        asset: str,
        category: str,
        start_time: Any,
        end_time: Any,
        config: Dict[str, Any] | None = None,
    ) -> List[Dict[str, Any]]:
        cfg = dict(config or {})
        if cfg.get("enabled") is False:
            return []

        start_ts = _to_utc_timestamp(start_time)
        end_ts = _to_utc_timestamp(end_time)
        if start_ts is None or end_ts is None:
            return []
        if end_ts < start_ts:
            start_ts, end_ts = end_ts, start_ts

        category_key = str(category or "").lower()
        lookahead_minutes = max(1, int(cls._resolve_value(cfg.get("window_minutes"), category_key, 90) or 90))
        min_strength = float(cls._resolve_value(cfg.get("min_strength"), category_key, 0.35) or 0.35)
        impacts = cls._normalize_impacts(cls._resolve_value(cfg.get("impacts"), category_key, ["HIGH"]))
        currencies = cls._resolve_currencies(
            asset,
            category_key,
            cls._resolve_value(cfg.get("currencies", "auto"), category_key, "auto"),
        )

        cache_key = (
            str(asset or "").upper(),
            category_key,
            start_ts.isoformat(),
            end_ts.isoformat(),
            tuple(currencies),
            tuple(impacts),
            lookahead_minutes,
            round(min_strength, 4),
        )
        cached = cls._macro_bias_cache.get(cache_key)
        if cached is not None:
            return list(cached)

        try:
            events = deriv_bridge.get_economic_events(
                start_time=start_ts.to_pydatetime(),
                end_time=(end_ts + pd.Timedelta(minutes=lookahead_minutes)).to_pydatetime(),
                currencies=currencies,
                impacts=impacts,
            )
        except Exception as exc:
            logger.debug(f"[EventRiskService] macro bias fetch failed for {asset}: {exc}")
            events = []

        windows: List[Dict[str, Any]] = []
        for event in events or []:
            window = cls._build_macro_bias_window(
                event=event,
                asset=asset,
                category=category_key,
                lookahead_minutes=lookahead_minutes,
                min_strength=min_strength,
            )
            if window is None:
                continue
            if _to_utc_timestamp(window["end"]) < start_ts or _to_utc_timestamp(window["start"]) > end_ts:
                continue
            windows.append(window)

        windows.sort(key=lambda item: (item["start"], -float(item.get("strength", 0.0) or 0.0)))
        cls._macro_bias_cache[cache_key] = list(windows)
        return windows

    @classmethod
    def active_blackout(
        cls,
        timestamp: Any,
        asset: str,
        category: str,
        config: Dict[str, Any] | None = None,
        preload_windows: Sequence[Dict[str, Any]] | None = None,
    ) -> Optional[Dict[str, Any]]:
        ts = _to_utc_timestamp(timestamp)
        if ts is None:
            return None

        windows = list(preload_windows or [])
        if not windows:
            windows = cls.get_blackout_windows(asset, category, ts, ts, config)

        for window in windows:
            start = _to_utc_timestamp(window.get("start"))
            end = _to_utc_timestamp(window.get("end"))
            if start is None or end is None:
                continue
            if start <= ts <= end:
                return dict(window)
        return None

    @classmethod
    def active_macro_bias(
        cls,
        timestamp: Any,
        asset: str,
        category: str,
        config: Dict[str, Any] | None = None,
        preload_windows: Sequence[Dict[str, Any]] | None = None,
    ) -> Optional[Dict[str, Any]]:
        ts = _to_utc_timestamp(timestamp)
        if ts is None:
            return None

        windows = list(preload_windows or [])
        if not windows:
            windows = cls.get_macro_bias_windows(asset, category, ts, ts, config)

        active: List[Dict[str, Any]] = []
        for window in windows:
            start = _to_utc_timestamp(window.get("start"))
            end = _to_utc_timestamp(window.get("end"))
            if start is None or end is None:
                continue
            if start <= ts <= end:
                active.append(dict(window))
        if not active:
            return None
        active.sort(key=lambda item: float(item.get("strength", 0.0) or 0.0), reverse=True)
        return active[0]

    @staticmethod
    def _resolve_value(value: Any, category: str, default: Any = None) -> Any:
        if isinstance(value, dict):
            return value.get(category) or value.get("default", default)
        return default if value is None else value

    @staticmethod
    def _normalize_impacts(impacts: Any) -> List[str]:
        normalized = []
        for impact in _as_list(impacts or ["HIGH"]):
            text = str(impact or "").strip().upper()
            if text in {"HIGH", "MEDIUM", "LOW"} and text not in normalized:
                normalized.append(text)
        return normalized or ["HIGH"]

    @classmethod
    def _resolve_currencies(cls, asset: str, category: str, configured: Any) -> List[str]:
        explicit = [
            str(code).strip().upper()
            for code in _as_list(configured)
            if str(code).strip() and str(code).strip().upper() in _MACRO_CURRENCIES
        ]
        if explicit and "AUTO" not in explicit:
            return sorted(set(explicit))

        asset_key = str(asset or "").upper().replace("-", "/")
        inferred = {
            token
            for token in re.findall(r"[A-Z]{3}", asset_key)
            if token in _MACRO_CURRENCIES
        }

        if str(category or "").lower() == "crypto":
            inferred.add("USD")
        elif str(category or "").lower() == "commodities":
            inferred.add("USD")
        elif str(category or "").lower() == "indices":
            if "UK" in asset_key or "FTSE" in asset_key:
                inferred.update({"GBP", "USD"})
            else:
                inferred.add("USD")

        return sorted(inferred) or ["USD"]

    @classmethod
    def _build_macro_bias_window(
        cls,
        event: Dict[str, Any],
        asset: str,
        category: str,
        lookahead_minutes: int,
        min_strength: float,
    ) -> Optional[Dict[str, Any]]:
        event_ts = _to_utc_timestamp(
            event.get("date")
            or event.get("event_date")
            or event.get("release_date")
            or event.get("datetime")
            or event.get("timestamp")
        )
        if event_ts is None:
            return None

        currency = str(event.get("currency") or "").upper()
        event_name = str(event.get("event") or event.get("title") or event.get("description") or "").strip()
        currency_effect = cls._infer_currency_effect(event_name, event)
        if currency_effect == 0:
            return None

        asset_direction = cls._map_currency_effect_to_asset_direction(asset, category, currency, currency_effect)
        if asset_direction is None:
            return None

        strength = cls._estimate_surprise_strength(event_name, event)
        if strength < min_strength:
            return None

        cross_market = cls._cross_market_confirmation(
            asset=asset,
            category=category,
            event_time=event_ts,
            expected_direction=asset_direction,
        )
        if cross_market:
            alignment = str(cross_market.get("alignment") or "")
            confirmation_strength = max(0.0, min(1.0, float(cross_market.get("strength", 0.0) or 0.0)))
            if alignment == "confirmed":
                strength = min(1.0, strength + 0.15 * confirmation_strength)
            elif alignment == "opposed":
                strength = max(0.0, strength - 0.20 * confirmation_strength)

        return {
            "start": event_ts,
            "end": event_ts + pd.Timedelta(minutes=lookahead_minutes),
            "event_time": event_ts,
            "event": event_name,
            "impact": str(event.get("impact") or "").upper(),
            "currency": currency,
            "direction": asset_direction,
            "strength": round(float(strength), 4),
            "source": str(event.get("source") or "Deriv"),
            "surprise_direction": str(event.get("surprise_direction") or ""),
            "reason": cls._macro_reason(event_name, currency, asset_direction, event),
            "cross_market": cross_market,
        }

    @classmethod
    def _infer_currency_effect(cls, event_name: str, event: Dict[str, Any]) -> int:
        surprise_direction = str(event.get("surprise_direction") or "").strip().lower()
        if surprise_direction:
            explicit = {
                "positive": 1,
                "bullish": 1,
                "better": 1,
                "stronger": 1,
                "hawkish": 1,
                "negative": -1,
                "bearish": -1,
                "worse": -1,
                "weaker": -1,
                "dovish": -1,
            }.get(surprise_direction)
            if explicit is not None:
                return explicit

        actual = _parse_numeric_value(event.get("actual"))
        forecast = _parse_numeric_value(event.get("forecast") or event.get("estimate"))
        if actual is None or forecast is None or abs(actual - forecast) < 1e-12:
            if surprise_direction in {"higher", "up"}:
                return 1 if cls._higher_is_positive(event_name) else -1
            if surprise_direction in {"lower", "down"}:
                return -1 if cls._higher_is_positive(event_name) else 1
            return 0

        surprise = actual - forecast
        if cls._higher_is_positive(event_name):
            return 1 if surprise > 0 else -1
        return -1 if surprise > 0 else 1

    @classmethod
    def _estimate_surprise_strength(cls, event_name: str, event: Dict[str, Any]) -> float:
        impact_weight = {
            "HIGH": 1.0,
            "MEDIUM": 0.7,
            "LOW": 0.4,
        }.get(str(event.get("impact") or "").upper(), 0.7)

        actual = _parse_numeric_value(event.get("actual"))
        forecast = _parse_numeric_value(event.get("forecast") or event.get("estimate"))
        magnitude = 0.45
        if actual is not None and forecast is not None and abs(actual - forecast) > 1e-12:
            baseline = max(abs(forecast), abs(actual), 1.0)
            surprise_ratio = min(1.0, abs(actual - forecast) / baseline * 8.0)
            magnitude = 0.4 + surprise_ratio * 0.6
        elif str(event.get("surprise_direction") or "").strip():
            magnitude = 0.6

        if "rate decision" in str(event_name or "").lower():
            magnitude = max(magnitude, 0.75)

        return round(impact_weight * magnitude, 4)

    @staticmethod
    def _higher_is_positive(event_name: str) -> bool:
        name = str(event_name or "").lower()
        if any(keyword in name for keyword in _HAWKISH_HIGHER_KEYWORDS):
            return True
        if any(keyword in name for keyword in _NEGATIVE_HIGHER_KEYWORDS):
            return False
        if any(keyword in name for keyword in _POSITIVE_HIGHER_KEYWORDS):
            return True
        return True

    @staticmethod
    def _map_currency_effect_to_asset_direction(asset: str, category: str, currency: str, effect: int) -> Optional[str]:
        asset_key = str(asset or "").upper().replace("-", "/")
        category_key = str(category or "").lower()
        if effect == 0:
            return None

        if category_key == "forex" and "/" in asset_key:
            base, quote = asset_key.split("/", 1)
            if currency == base:
                return "BUY" if effect > 0 else "SELL"
            if currency == quote:
                return "SELL" if effect > 0 else "BUY"
            return None

        if category_key in {"commodities", "crypto"} and currency == "USD":
            return "SELL" if effect > 0 else "BUY"

        if category_key == "indices":
            if currency in {"USD", "GBP", "EUR", "JPY"}:
                return "SELL" if effect > 0 else "BUY"
        return None

    @staticmethod
    def _macro_reason(event_name: str, currency: str, direction: str, event: Dict[str, Any]) -> str:
        actual = event.get("actual")
        forecast = event.get("forecast") or event.get("estimate")
        return f"{currency} macro surprise on {event_name}: actual={actual} forecast={forecast} -> {direction}"

    @staticmethod
    def _cross_market_confirmation(
        asset: str,
        category: str,
        event_time: pd.Timestamp,
        expected_direction: str,
    ) -> Optional[Dict[str, Any]]:
        try:
            from services.free_market_intelligence import free_market_intelligence

            context = free_market_intelligence.get_asset_context(
                asset,
                category,
                as_of=event_time.to_pydatetime(),
            )
        except Exception as exc:
            logger.debug(f"[EventRiskService] cross-market confirmation unavailable for {asset}: {exc}")
            return None

        if not isinstance(context, dict):
            return None
        score = float(context.get("score", 0.0) or 0.0)
        sources = list(context.get("sources") or [])
        if not sources:
            return None

        threshold = 0.12
        if score >= threshold:
            direction = "BUY"
        elif score <= -threshold:
            direction = "SELL"
        else:
            direction = ""

        if not direction:
            alignment = "neutral"
        elif direction == str(expected_direction or "").upper():
            alignment = "confirmed"
        else:
            alignment = "opposed"

        return {
            "score": round(score, 4),
            "direction": direction,
            "alignment": alignment,
            "strength": round(min(1.0, abs(score)), 4),
            "sources": sources,
            "details": context.get("details", {}),
            "as_of": context.get("as_of"),
        }
