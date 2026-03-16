"""
layers/layer4_session.py — Trading session / market hours filter.

Writes full decision to signal.journal.
"""
from __future__ import annotations
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from core.signal import Signal
from core.signal_journal import PASS, KILLED
from utils.logger import get_logger

logger = get_logger()
LAYER = 4


def _utc_hour() -> int:
    return datetime.now(tz=timezone.utc).hour


def _active_session() -> str:
    h = _utc_hour()
    if 0  <= h < 9:  return "tokyo"
    if 22 <= h:      return "sydney"
    if 7  <= h < 16: return "london"
    if 12 <= h < 21: return "new_york"
    return "off"


def _is_market_open(category: str) -> bool:
    session = _active_session()
    h       = _utc_hour()
    if category == "crypto":
        return True
    if category == "forex":
        return session != "off"
    if category in ("stocks", "indices"):
        return 13 <= h < 21
    if category == "commodities":
        return session in ("london", "new_york")
    return True


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

        if not _is_market_open(signal.category):
            reason = f"market closed for {signal.category} at UTC {utc_hour:02d}:xx"
            signal.reduce(0.05)
            signal.journal.record(
                layer=LAYER, name=self.name, decision=PASS,
                reason=reason,
                conf_before=conf_before, conf_after=signal.confidence,
                data={"session": session, "utc_hour": utc_hour},
            )
            logger.log_pipeline(signal.asset, LAYER, "MARKET_CLOSED", reason)

        signal.metadata["session"] = session
        boost = _SESSION_BOOST.get(session, 0.0)
        if boost:
            signal.boost(boost)

        reason = f"session={session}  UTC {utc_hour:02d}:xx"
        signal.journal.record(
            layer=LAYER, name=self.name, decision=PASS,
            reason=reason,
            conf_before=conf_before, conf_after=signal.confidence,
            data={"session": session, "utc_hour": utc_hour, "boost": boost},
        )
        logger.log_pipeline(signal.asset, LAYER, "PASS", f"session={session}")
        return signal