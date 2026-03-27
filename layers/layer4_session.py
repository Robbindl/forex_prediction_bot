from __future__ import annotations
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from core.signal import Signal
from core.signal_journal import PASS, KILLED
from utils.logger import get_logger

logger   = get_logger()
LAYER    = 4

_PRECIOUS_METALS = {"XAU", "XAG", "GC=F", "SI=F", "GOLD", "SILVER", "XAUUSD", "XAGUSD"}


def _utc_hour() -> int:
    return datetime.now(tz=timezone.utc).hour


def _is_weekday() -> bool:
    return datetime.now(tz=timezone.utc).weekday() < 5


# FIX HIGH: Hardcoded NYSE/LSE public holidays (month, day) that fall on
# weekdays.  Previously the market-open check only verified Mon–Fri and
# UTC hour — signals for stocks and indices were generated on Thanksgiving,
# Christmas, Good Friday, etc. when exchanges are closed.
# This covers the most common US + UK market holidays.
# Format: (month, day)  — year-agnostic; floating holidays (Thanksgiving,
# Good Friday, Easter) are included as approximate fixed anchors.
_NYSE_FIXED_HOLIDAYS = frozenset({
    (1,  1),   # New Year's Day
    (7,  4),   # Independence Day
    (12, 25),  # Christmas Day
    (12, 26),  # Boxing Day (LSE)
    (5,  1),   # May Day (LSE)
})

# Floating holidays — approximate fixed weekday anchors
# (Thanksgiving = 4th Thursday of November ≈ Nov 22–28; Good Friday ≈ Mar/Apr)
# We use a narrow fixed window for Thanksgiving and skip Good Friday
# as it moves too much year-to-year for a simple (month, day) set.
_NYSE_THANKSGIVING_RANGE = frozenset({
    (11, 22), (11, 23), (11, 24), (11, 25), (11, 26), (11, 27), (11, 28),
})


def _is_exchange_holiday() -> bool:
    """Return True if today is a known NYSE/LSE public holiday."""
    now = datetime.now(tz=timezone.utc)
    md  = (now.month, now.day)
    if md in _NYSE_FIXED_HOLIDAYS:
        return True
    # Thanksgiving: 4th Thursday of November
    if now.month == 11 and now.weekday() == 3 and 22 <= now.day <= 28:
        return True
    return False


def _is_market_open(category: str) -> bool:
    h  = _utc_hour()
    wd = _is_weekday()
    if not wd:
        return category == "crypto"
    if category == "crypto":
        return True
    # FIX: Block non-crypto markets on public exchange holidays
    if category != "crypto" and _is_exchange_holiday():
        return False
    if category == "forex":
        return _active_session() != "off"
    if category in ("stocks", "indices"):
        return 13 <= h < 21
    if category == "commodities":
        return h != 21
    return True


def _active_session() -> str:
    """Return the current active trading session for FX/commodities."""
    now = datetime.now(tz=timezone.utc)
    hour = now.hour
    weekday = now.weekday()  # 0=Mon, 6=Sun

    # 24/5 FX sessions, with weekend closure from Fri 22:00 to Sun 22:00 UTC
    if weekday == 5 or weekday == 6:
        if weekday == 6 and hour >= 22:
            return "asia"  # forex opens sunday 22:00 UTC
        return "off"
    if weekday == 4 and hour >= 22:
        return "off"

    # Session blocks: Asia (Tokyo) 00:00-06:00, Europe 06:00-14:00, US 14:00-22:00
    if 0 <= hour < 6:
        return "asia"
    if 6 <= hour < 14:
        return "europe"
    if 14 <= hour < 22:
        return "us"

    return "off"

def _get_news_state(category: str) -> Dict:
    """Get current news event state for this category."""
    try:
        from data_ingestion.news_event_monitor import news_monitor
        return news_monitor.get_event_state(category)
    except Exception:
        return {"state": "clear", "event": "", "impact": "", "direction": "", "mins_to": 0}


_SESSION_BOOST: Dict[str, float] = {
    "europe": 0.04,   # London session  — _active_session() returns "europe"
    "us":     0.03,   # New York session — _active_session() returns "us"
    "asia":   0.02,   # Tokyo session   — _active_session() returns "asia"
}


class SessionLayer:
    name = "session"

    def process(self, signal: Signal, context: Dict[str, Any]) -> Optional[Signal]:
        conf_before = signal.confidence
        session     = _active_session()
        utc_hour    = _utc_hour()

        # ── 1. Market hours gate ──────────────────────────────────────────────
        if not _is_market_open(signal.category):
            reason = (
                f"market closed for {signal.category} "
                f"at UTC {utc_hour:02d}:xx"
            )
            signal.kill(reason, LAYER)
            signal.journal.record(
                layer=LAYER, name=self.name, decision=KILLED,
                reason=reason,
                conf_before=conf_before, conf_after=signal.confidence,
                data={"session": session, "utc_hour": utc_hour},
            )
            logger.log_pipeline(signal.asset, LAYER, "KILLED", reason)
            return None

        # ── 2. News event gate ────────────────────────────────────────────────
        news = _get_news_state(signal.category)
        news_state = news.get("state", "clear")
        event_name = news.get("event", "")
        impact     = news.get("impact", "")
        direction  = news.get("direction", "")
        mins       = news.get("mins_to", 0)

        if news_state == "pre" and impact == "HIGH":
            reason = (
                f"HIGH impact event in {mins}min: {event_name} — "
                f"blocking pre-event trading"
            )
            signal.kill(reason, LAYER)
            signal.journal.record(
                layer=LAYER, name=self.name, decision=KILLED,
                reason=reason,
                conf_before=conf_before, conf_after=signal.confidence,
                data={"news_state": "pre", "event": event_name,
                      "impact": impact, "mins_to": mins},
            )
            logger.log_pipeline(signal.asset, LAYER, "KILLED_PRE_EVENT", reason)
            return None

        if news_state == "active" and impact == "HIGH":
            reason = (
                f"HIGH impact event active: {event_name} "
                f"({mins}min ago) — blocking volatile window"
            )
            signal.kill(reason, LAYER)
            signal.journal.record(
                layer=LAYER, name=self.name, decision=KILLED,
                reason=reason,
                conf_before=conf_before, conf_after=signal.confidence,
                data={"news_state": "active", "event": event_name,
                      "impact": impact, "mins_ago": mins},
            )
            logger.log_pipeline(signal.asset, LAYER, "KILLED_EVENT_ACTIVE", reason)
            return None

        if news_state == "pre" and impact == "MEDIUM":
            # Medium impact — reduce confidence instead of kill
            signal.reduce(0.05)
            signal.journal.record(
                layer=LAYER, name=self.name, decision=PASS,
                reason=f"MEDIUM impact event in {mins}min: {event_name} -0.05",
                conf_before=conf_before, conf_after=signal.confidence,
                data={"news_state": "pre", "event": event_name, "impact": "MEDIUM"},
            )

        if news_state == "post" and direction:
            # Post-event: boost if signal aligns with surprise, reduce if opposed
            if direction == signal.direction:
                boost = 0.08 if impact == "HIGH" else 0.04
                signal.boost(boost)
                signal.metadata["news_boost"] = f"+{boost} post-{event_name}"
                signal.journal.record(
                    layer=LAYER, name=self.name, decision=PASS,
                    reason=f"Post-event surprise confirms {signal.direction}: {event_name} +{boost}",
                    conf_before=conf_before, conf_after=signal.confidence,
                    data={"news_state": "post", "event": event_name,
                          "surprise_dir": direction, "boost": boost},
                )
                logger.log_pipeline(signal.asset, LAYER, "NEWS_BOOST",
                                    f"{event_name} confirms {signal.direction}")
            else:
                signal.reduce(0.06)
                signal.journal.record(
                    layer=LAYER, name=self.name, decision=PASS,
                    reason=f"Post-event surprise opposes {signal.direction}: {event_name} -0.06",
                    conf_before=conf_before, conf_after=signal.confidence,
                    data={"news_state": "post", "event": event_name,
                          "surprise_dir": direction, "reduce": 0.06},
                )

        # ── 3. Session boost ──────────────────────────────────────────────────
        signal.metadata["session"] = session
        boost = _SESSION_BOOST.get(session, 0.0)
        if boost:
            signal.boost(boost)

        reason = f"session={session}  UTC {utc_hour:02d}:xx"
        if news_state != "clear":
            reason += f"  news={news_state}({event_name[:20]})"

        signal.journal.record(
            layer=LAYER, name=self.name, decision=PASS,
            reason=reason,
            conf_before=conf_before, conf_after=signal.confidence,
            data={"session": session, "utc_hour": utc_hour,
                  "boost": boost, "news_state": news_state},
        )
        logger.log_pipeline(signal.asset, LAYER, "PASS", f"session={session}")
        return signal