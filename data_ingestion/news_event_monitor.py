from __future__ import annotations

import json
import threading
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any

from utils.logger import get_logger

logger = get_logger()

# ── Constants ─────────────────────────────────────────────────────────────────
POLL_INTERVAL_SECS  = 900    # poll Finnhub every 15 minutes
PRE_EVENT_MINS      = 10     # REDUCED: block signals 10 min before (vs 60) for 15m trading
ACTIVE_MINS         = 10     # REDUCED: block signals 10 min after release (vs 15, markets stabilize faster)
POST_EVENT_MINS     = 45     # REDUCED: boost window 45 min (vs 90) after for 15m timeframe

# Which Finnhub event names are HIGH impact
HIGH_IMPACT_KEYWORDS = {
    "fed", "fomc", "federal reserve", "interest rate", "cpi", "inflation",
    "nfp", "non-farm", "payroll", "gdp", "unemployment", "jobless",
    "pce", "ppi", "retail sales", "trade balance", "housing",
}

MEDIUM_IMPACT_KEYWORDS = {
    "ism", "pmi", "durable", "factory", "consumer confidence",
    "existing home", "new home", "building permit",
}

# Which asset categories each event type affects
EVENT_ASSET_MAP = {
    "HIGH":   {"forex", "commodities", "indices", "crypto"},
    "MEDIUM": {"forex", "indices"},
    "LOW":    set(),
}


class NewsEventMonitor:
    """
    Singleton that tracks upcoming and recent economic events.
    Exposes get_event_state(asset, category) for Layer 4 to query.
    """

    _instance: Optional["NewsEventMonitor"] = None
    _lock      = threading.Lock()

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
        self._initialised  = True
        self._running      = False
        self._events:      List[Dict] = []      # upcoming events (next 24h)
        self._recent:      List[Dict] = []      # events in last 90 min
        self._data_lock    = threading.RLock()
        self._pub          = None
        self._thread:      Optional[threading.Thread] = None
        self._init_redis()

    # ── Public API ────────────────────────────────────────────────────────────

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._thread  = threading.Thread(
            target=self._loop, name="NewsEventMonitor", daemon=True
        )
        self._thread.start()
        logger.info("[NewsMonitor] Started — polling Finnhub calendar every 15min")

    def stop(self) -> None:
        self._running = False

    def get_event_state(self, category: str) -> Dict[str, Any]:
        """
        Called by Layer 4 for every signal.
        Returns the current news event status for an asset category.

        Return format:
        {
            "state":     "clear" | "pre" | "active" | "post",
            "event":     "FOMC Meeting" | "",
            "impact":    "HIGH" | "MEDIUM" | "",
            "direction": "BUY" | "SELL" | "",   # only set in "post" state
            "mins_to":   int,                   # minutes until/since event
        }
        """
        now = datetime.now(timezone.utc)

        with self._data_lock:
            # Check recent events first (post-event boost window)
            for ev in self._recent:
                if category not in ev.get("affects", set()):
                    continue
                ev_time  = ev["time"]
                mins_ago = (now - ev_time).total_seconds() / 60
                if ACTIVE_MINS <= mins_ago <= POST_EVENT_MINS:
                    return {
                        "state":     "post",
                        "event":     ev["name"],
                        "impact":    ev["impact"],
                        "direction": ev.get("surprise_direction", ""),
                        "mins_to":   int(mins_ago),
                    }
                if 0 <= mins_ago < ACTIVE_MINS:
                    return {
                        "state":   "active",
                        "event":   ev["name"],
                        "impact":  ev["impact"],
                        "direction": "",
                        "mins_to": int(mins_ago),
                    }

            # Check upcoming events (pre-event block window)
            for ev in self._events:
                if category not in ev.get("affects", set()):
                    continue
                ev_time    = ev["time"]
                mins_until = (ev_time - now).total_seconds() / 60
                if 0 <= mins_until <= PRE_EVENT_MINS:
                    return {
                        "state":     "pre",
                        "event":     ev["name"],
                        "impact":    ev["impact"],
                        "direction": "",
                        "mins_to":   int(mins_until),
                    }

        return {"state": "clear", "event": "", "impact": "", "direction": "", "mins_to": 0}

    def upcoming_events(self, hours: int = 24) -> List[Dict]:
        """Return all upcoming events in the next N hours."""
        with self._data_lock:
            return list(self._events)

    # ── Internal ──────────────────────────────────────────────────────────────

    def _loop(self) -> None:
        # First fetch immediately
        self._fetch_and_update()
        while self._running:
            time.sleep(POLL_INTERVAL_SECS)
            try:
                self._fetch_and_update()
            except Exception as e:
                logger.debug(f"[NewsMonitor] loop error: {e}")

    def _fetch_and_update(self) -> None:
        events = self._fetch_finnhub() or self._fetch_alphavantage()
        if not events:
            logger.debug("[NewsMonitor] No calendar data available")
            return

        now     = datetime.now(timezone.utc)
        cutoff  = now + timedelta(hours=24)
        recent_cutoff = now - timedelta(minutes=POST_EVENT_MINS)

        upcoming = []
        recent   = []

        for ev in events:
            ev_time = ev.get("time")
            if not ev_time:
                continue
            if ev_time > now and ev_time <= cutoff:
                upcoming.append(ev)
            elif recent_cutoff <= ev_time <= now:
                recent.append(ev)

        with self._data_lock:
            self._events = sorted(upcoming, key=lambda x: x["time"])
            self._recent = sorted(recent,   key=lambda x: x["time"], reverse=True)

        # Publish upcoming HIGH impact alerts to Redis
        for ev in upcoming:
            if ev["impact"] == "HIGH":
                mins_until = (ev["time"] - now).total_seconds() / 60
                if mins_until <= PRE_EVENT_MINS:
                    self._publish_alert("NEWS_EVENT_UPCOMING", ev, int(mins_until))

        logger.info(
            f"[NewsMonitor] Updated: {len(upcoming)} upcoming, "
            f"{len(recent)} recent events"
        )

    def _fetch_finnhub(self) -> Optional[List[Dict]]:
        """Fetch from Finnhub economic calendar."""
        try:
            from config.config import FINNHUB_API_KEY
            if not FINNHUB_API_KEY:
                return None

            import requests
            from datetime import date, timedelta as td

            today    = date.today()
            end_date = today + td(days=2)

            resp = requests.get(
                "https://finnhub.io/api/v1/calendar/economic",
                params={
                    "token": FINNHUB_API_KEY,
                    "from":  today.strftime("%Y-%m-%d"),
                    "to":    end_date.strftime("%Y-%m-%d"),
                },
                timeout=10,
            )
            if not resp.ok:
                return None

            raw = resp.json().get("economicCalendar", [])
            result = []
            for item in raw:
                name   = item.get("event", "")
                impact = self._classify_impact(name, item.get("impact", ""))
                if impact == "LOW":
                    continue
                # Parse time
                time_str = item.get("time", "")
                ev_time  = self._parse_time(time_str)
                if not ev_time:
                    continue

                # Determine surprise direction if actual and estimate available
                actual   = item.get("actual")
                estimate = item.get("estimate")
                surprise_dir = self._surprise_direction(name, actual, estimate)

                result.append({
                    "name":               name,
                    "impact":             impact,
                    "time":               ev_time,
                    "actual":             actual,
                    "estimate":           estimate,
                    "surprise_direction": surprise_dir,
                    "affects":            EVENT_ASSET_MAP.get(impact, set()),
                    "source":             "finnhub",
                })
            return result

        except Exception as e:
            logger.debug(f"[NewsMonitor] Finnhub fetch: {e}")
            return None

    def _fetch_alphavantage(self) -> Optional[List[Dict]]:
        """Fallback: DISABLED.
        
        Alpha Vantage news headlines don't have reliable publish timestamps.
        Previously this function returned headlines with time=now, causing them
        to be classified as "active high-impact events" regardless of when they
        were published. This blocked all trading signals for affected categories.
        
        Until a proper timestamp source is available, return None to disable
        the broken fallback.
        """
        return None

    @staticmethod
    def _classify_impact(name: str, raw_impact: str) -> str:
        name_lower = name.lower()
        if any(k in name_lower for k in HIGH_IMPACT_KEYWORDS):
            return "HIGH"
        if raw_impact.lower() in ("high", "3"):
            return "HIGH"
        if any(k in name_lower for k in MEDIUM_IMPACT_KEYWORDS):
            return "MEDIUM"
        if raw_impact.lower() in ("medium", "2"):
            return "MEDIUM"
        return "LOW"

    @staticmethod
    def _surprise_direction(event_name: str, actual, estimate) -> str:
        """
        Determine if actual beat or missed estimate and what that means
        for price direction.

        Hawkish surprises (higher rates, higher inflation) → SELL forex, SELL bonds
        Dovish surprises (lower rates, lower inflation) → BUY gold, BUY bonds
        Strong employment → BUY USD (SELL EUR/USD), SELL gold
        """
        if actual is None or estimate is None:
            return ""
        try:
            a = float(str(actual).replace("%", "").strip())
            e = float(str(estimate).replace("%", "").strip())
        except (ValueError, TypeError):
            return ""

        name_lower = event_name.lower()
        beat = a > e   # actual beat estimate

        # Fed / interest rate — higher = hawkish = USD up = gold down
        if any(k in name_lower for k in ("fed", "fomc", "interest rate", "funds rate")):
            return "SELL" if beat else "BUY"   # higher rate = gold SELL

        # CPI / inflation — higher = hawkish
        if any(k in name_lower for k in ("cpi", "inflation", "pce", "ppi")):
            return "SELL" if beat else "BUY"

        # NFP / employment — stronger = USD up = gold down
        if any(k in name_lower for k in ("payroll", "nfp", "employment", "jobs")):
            return "SELL" if beat else "BUY"

        # GDP — stronger = risk-on = gold down
        if "gdp" in name_lower:
            return "SELL" if beat else "BUY"

        # Unemployment — higher unemployment = dovish = gold up
        if "unemployment" in name_lower or "jobless" in name_lower:
            return "BUY" if beat else "SELL"

        return ""

    @staticmethod
    def _parse_time(time_str: str) -> Optional[datetime]:
        """Parse various time formats from Finnhub."""
        if not time_str:
            return None
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(time_str, fmt)
                return dt.replace(tzinfo=timezone.utc)
            except ValueError:
                continue
        return None

    def _publish_alert(self, channel: str, ev: Dict, mins: int) -> None:
        if not self._pub:
            return
        try:
            payload = {
                "type":    channel,
                "event":   ev["name"],
                "impact":  ev["impact"],
                "mins":    mins,
                "time":    ev["time"].isoformat(),
                "ts":      int(time.time() * 1000),
            }
            self._pub.publish(channel, json.dumps(payload, default=str))
        except Exception:
            pass

    def _init_redis(self) -> None:
        try:
            from services.redis_pool import get_client
            self._pub = get_client()
        except Exception as e:
            logger.debug(f"[NewsMonitor] Redis: {e}")


# ── Global singleton ──────────────────────────────────────────────────────────
news_monitor = NewsEventMonitor()


def start_news_monitor() -> None:
    """Call from bot.py to start the monitor."""
    news_monitor.start()
