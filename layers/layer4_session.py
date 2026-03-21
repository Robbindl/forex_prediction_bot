"""
layers/layer4_session.py — Trading session + news event filter.

Two gates in one layer:
    1. Market hours  — kills signals when market is genuinely closed
    2. News events   — blocks signals before/during high-impact releases,
                       boosts signals after a surprise

Market hours (UTC, weekdays only):
    Crypto       — 24/7
    Forex        — 22:00 Sun – 22:00 Fri (any active session)
    Stocks       — 13:00–21:00 (NYSE)
    Indices      — 13:00–21:00 (NYSE)
    Commodities  — 22:00 Sun – 21:00 Fri (CME/NYMEX, 23h/day)
                   Closed daily 21:00–22:00 UTC

News event behaviour:
    PRE  (-60 to 0 min before):  KILL — unpredictable pre-event drift
    ACTIVE (0 to +15 min):       KILL — spreads widen, stops get hunted
    POST (+15 to +90 min):       BOOST if surprise direction matches signal
                                 REDUCE if surprise direction opposes signal
"""
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


def _active_session() -> str:
    h = _utc_hour()
    if 0  <= h < 9:  return "tokyo"
    if 22 <= h:      return "sydney"
    if 7  <= h < 16: return "london"
    if 12 <= h < 21: return "new_york"
    return "off"


def _is_market_open(category: str) -> bool:
    h  = _utc_hour()
    wd = _is_weekday()
    if not wd:
        return category == "crypto"
    if category == "crypto":
        return True
    if category == "forex":
        return _active_session() != "off"
    if category in ("stocks", "indices"):
        return 13 <= h < 21
    if category == "commodities":
        return h != 21
    return True


def _get_news_state(category: str) -> Dict:
    """Get current news event state for this category."""
    try:
        from data_ingestion.news_event_monitor import news_monitor
        return news_monitor.get_event_state(category)
    except Exception:
        return {"state": "clear", "event": "", "impact": "", "direction": "", "mins_to": 0}


_SESSION_BOOST: Dict[str, float] = {
    "london":   0.04,
    "new_york": 0.03,
    "tokyo":    0.02,
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