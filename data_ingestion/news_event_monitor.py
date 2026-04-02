from __future__ import annotations

import json
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from services.economic_calendar_service import economic_calendar_service as deriv_bridge
from utils.logger import get_logger

logger = get_logger()

POLL_INTERVAL_SECS = 900
PRE_EVENT_MINS = 10
ACTIVE_MINS = 10
POST_EVENT_MINS = 45

HIGH_IMPACT_KEYWORDS = {
    "fed",
    "fomc",
    "federal reserve",
    "interest rate",
    "cpi",
    "inflation",
    "nfp",
    "non-farm",
    "payroll",
    "gdp",
    "unemployment",
    "jobless",
    "pce",
    "ppi",
    "retail sales",
    "trade balance",
    "housing",
}

MEDIUM_IMPACT_KEYWORDS = {
    "ism",
    "pmi",
    "durable",
    "factory",
    "consumer confidence",
    "existing home",
    "new home",
    "building permit",
}

EVENT_ASSET_MAP = {
    "HIGH": {"forex", "commodities", "indices", "crypto"},
    "MEDIUM": {"forex", "indices"},
    "LOW": set(),
}


class NewsEventMonitor:
    """
    Singleton that tracks upcoming and recent economic events.
    Exposes get_event_state(category) for Layer 4 to query.
    """

    _instance: Optional["NewsEventMonitor"] = None
    _lock = threading.Lock()

    def __new__(cls) -> "NewsEventMonitor":
        with cls._lock:
            if cls._instance is None:
                inst = object.__new__(cls)
                inst._initialised = False
                cls._instance = inst
        return cls._instance

    def __init__(self) -> None:
        if self._initialised:
            return
        self._initialised = True
        self._running = False
        self._events: List[Dict[str, Any]] = []
        self._recent: List[Dict[str, Any]] = []
        self._data_lock = threading.RLock()
        self._pub = None
        self._thread: Optional[threading.Thread] = None
        self._init_redis()

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._loop, name="NewsEventMonitor", daemon=True)
        self._thread.start()
        logger.info("[NewsMonitor] Started - polling economic calendar every 15min")

    def stop(self) -> None:
        self._running = False

    def get_event_state(self, category: str) -> Dict[str, Any]:
        now = datetime.now(timezone.utc)

        with self._data_lock:
            for ev in self._recent:
                if category not in ev.get("affects", set()):
                    continue
                ev_time = ev["time"]
                mins_ago = (now - ev_time).total_seconds() / 60
                if ACTIVE_MINS <= mins_ago <= POST_EVENT_MINS:
                    return {
                        "state": "post",
                        "event": ev["name"],
                        "impact": ev["impact"],
                        "direction": ev.get("surprise_direction", ""),
                        "mins_to": int(mins_ago),
                    }
                if 0 <= mins_ago < ACTIVE_MINS:
                    return {
                        "state": "active",
                        "event": ev["name"],
                        "impact": ev["impact"],
                        "direction": "",
                        "mins_to": int(mins_ago),
                    }

            for ev in self._events:
                if category not in ev.get("affects", set()):
                    continue
                mins_until = (ev["time"] - now).total_seconds() / 60
                if 0 <= mins_until <= PRE_EVENT_MINS:
                    return {
                        "state": "pre",
                        "event": ev["name"],
                        "impact": ev["impact"],
                        "direction": "",
                        "mins_to": int(mins_until),
                    }

        return {"state": "clear", "event": "", "impact": "", "direction": "", "mins_to": 0}

    def upcoming_events(self, hours: int = 24) -> List[Dict[str, Any]]:
        with self._data_lock:
            return list(self._events)

    def _loop(self) -> None:
        self._fetch_and_update()
        while self._running:
            time.sleep(POLL_INTERVAL_SECS)
            try:
                self._fetch_and_update()
            except Exception as exc:
                logger.debug(f"[NewsMonitor] loop error: {exc}")

    def _fetch_and_update(self) -> None:
        events = self._fetch_deriv()
        now = datetime.now(timezone.utc)
        if not events:
            self._prune_cached_events(now)
            logger.debug("[NewsMonitor] No economic calendar data available")
            return

        cutoff = now + timedelta(hours=24)
        recent_cutoff = now - timedelta(minutes=POST_EVENT_MINS)

        upcoming = []
        recent = []
        for ev in events:
            ev_time = ev.get("time")
            if not ev_time:
                continue
            if now < ev_time <= cutoff:
                upcoming.append(ev)
            elif recent_cutoff <= ev_time <= now:
                recent.append(ev)

        with self._data_lock:
            self._events = sorted(upcoming, key=lambda item: item["time"])
            self._recent = sorted(recent, key=lambda item: item["time"], reverse=True)

        for ev in upcoming:
            if ev["impact"] == "HIGH":
                mins_until = (ev["time"] - now).total_seconds() / 60
                if mins_until <= PRE_EVENT_MINS:
                    self._publish_alert("NEWS_EVENT_UPCOMING", ev, int(mins_until))

        logger.info(f"[NewsMonitor] Updated: {len(upcoming)} upcoming, {len(recent)} recent events")

    def _prune_cached_events(self, now: datetime) -> None:
        cutoff = now + timedelta(hours=24)
        recent_cutoff = now - timedelta(minutes=POST_EVENT_MINS)
        with self._data_lock:
            self._events = [
                ev for ev in self._events
                if ev.get("time") and now < ev["time"] <= cutoff
            ]
            self._recent = [
                ev for ev in self._recent
                if ev.get("time") and recent_cutoff <= ev["time"] <= now
            ]

    def _fetch_calendar(self) -> Optional[List[Dict[str, Any]]]:
        try:
            raw_events = deriv_bridge.get_high_impact_events(
                days=3,
                currencies=["USD", "EUR", "GBP", "JPY", "CAD", "AUD"],
            )
            if not raw_events:
                return None

            result = []
            for item in raw_events:
                name = str(item.get("event", "") or "")
                impact = self._classify_impact(name, str(item.get("impact", "") or ""))
                if impact == "LOW":
                    continue

                ev_time = self._parse_time(item.get("date"))
                if not ev_time:
                    continue

                actual = item.get("actual")
                estimate = item.get("estimate", item.get("forecast"))
                surprise_direction = (
                    str(item.get("surprise_direction", "") or "")
                    or self._surprise_direction(name, actual, estimate)
                )

                result.append({
                    "name": name,
                    "impact": impact,
                    "time": ev_time,
                    "actual": actual,
                    "estimate": estimate,
                    "surprise_direction": surprise_direction,
                    "affects": EVENT_ASSET_MAP.get(impact, set()),
                    "source": str(item.get("source", "Deriv") or "Deriv"),
                })

            return result
        except Exception as exc:
            logger.debug(f"[NewsMonitor] Economic calendar fetch: {exc}")
            return None

    def _fetch_deriv(self) -> Optional[List[Dict[str, Any]]]:
        # Backward-compatible hook name used by older tests/callers.
        return self._fetch_calendar()

    @staticmethod
    def _classify_impact(name: str, raw_impact: str) -> str:
        lowered = name.lower()
        if any(keyword in lowered for keyword in HIGH_IMPACT_KEYWORDS):
            return "HIGH"
        if str(raw_impact).strip().lower() in {"high", "3"}:
            return "HIGH"
        if any(keyword in lowered for keyword in MEDIUM_IMPACT_KEYWORDS):
            return "MEDIUM"
        if str(raw_impact).strip().lower() in {"medium", "2"}:
            return "MEDIUM"
        return "LOW"

    @staticmethod
    def _surprise_direction(event_name: str, actual, estimate) -> str:
        if actual is None or estimate is None:
            return ""
        try:
            actual_value = float(str(actual).replace("%", "").strip())
            estimate_value = float(str(estimate).replace("%", "").strip())
        except (TypeError, ValueError):
            return ""

        name_lower = event_name.lower()
        beat = actual_value > estimate_value

        if any(keyword in name_lower for keyword in ("fed", "fomc", "interest rate", "funds rate")):
            return "SELL" if beat else "BUY"
        if any(keyword in name_lower for keyword in ("cpi", "inflation", "pce", "ppi")):
            return "SELL" if beat else "BUY"
        if any(keyword in name_lower for keyword in ("payroll", "nfp", "employment", "jobs")):
            return "SELL" if beat else "BUY"
        if "gdp" in name_lower:
            return "SELL" if beat else "BUY"
        if "unemployment" in name_lower or "jobless" in name_lower:
            return "BUY" if beat else "SELL"
        return ""

    @staticmethod
    def _parse_time(raw_value: Any) -> Optional[datetime]:
        if raw_value in (None, ""):
            return None
        if isinstance(raw_value, datetime):
            return raw_value.astimezone(timezone.utc) if raw_value.tzinfo else raw_value.replace(tzinfo=timezone.utc)
        text = str(raw_value).strip().replace("Z", "+00:00")
        for fmt in ("%Y-%m-%d %H:%M UTC", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
            try:
                parsed = datetime.strptime(text, fmt)
                return parsed.replace(tzinfo=timezone.utc)
            except ValueError:
                continue
        try:
            parsed = datetime.fromisoformat(text)
            return parsed.astimezone(timezone.utc) if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
        except ValueError:
            return None

    def _publish_alert(self, channel: str, ev: Dict[str, Any], mins: int) -> None:
        if not self._pub:
            return
        try:
            payload = {
                "type": channel,
                "event": ev["name"],
                "impact": ev["impact"],
                "mins": mins,
                "time": ev["time"].isoformat(),
                "ts": int(time.time() * 1000),
            }
            self._pub.publish(channel, json.dumps(payload, default=str))
        except Exception:
            pass

    def _init_redis(self) -> None:
        try:
            from services.redis_pool import get_client

            self._pub = get_client()
        except Exception as exc:
            logger.debug(f"[NewsMonitor] Redis: {exc}")


news_monitor = NewsEventMonitor()


def start_news_monitor() -> None:
    news_monitor.start()
