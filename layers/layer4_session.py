"""Layer 4 — Trading session / market hours filter. Merges market_calendar + session_tracker."""
from __future__ import annotations
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from core.signal import Signal
from utils.logger import get_logger

logger = get_logger()
LAYER = 4


def _utc_hour() -> int:
    return datetime.now(tz=timezone.utc).hour


def _active_session() -> str:
    h = _utc_hour()
    if 22 <= h or h < 7:
        return "sydney"
    if 0 <= h < 9:
        return "tokyo"
    if 7 <= h < 16:
        return "london"
    if 12 <= h < 21:
        return "new_york"
    return "off"


def _is_market_open(category: str) -> bool:
    session = _active_session()
    h = _utc_hour()
    if category == "crypto":
        return True
    if category == "forex":
        return session != "off"
    if category in ("stocks", "indices"):
        return 13 <= h < 21        # NYSE hours approx UTC
    if category == "commodities":
        return session in ("london", "new_york")
    return True


_SESSION_BOOST: Dict[str, float] = {
    "london":   0.04,
    "new_york": 0.03,
}


class SessionLayer:
    name = "session"

    def process(self, signal: Signal, context: Dict[str, Any]) -> Optional[Signal]:
        if not _is_market_open(signal.category):
            signal.kill(
                f"Market closed for {signal.category} at UTC {_utc_hour():02d}:xx", LAYER
            )
            return None

        session = _active_session()
        signal.metadata["session"] = session

        boost = _SESSION_BOOST.get(session, 0.0)
        if boost:
            signal.boost(boost)

        logger.log_pipeline(signal.asset, LAYER, "PASS", f"session={session}")
        return signal