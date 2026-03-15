"""Layer 4 — Trading session / market hours filter."""
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
    """
    Real session windows (UTC):
      Tokyo:    00:00 – 09:00   (Asian session)
      Sydney:   22:00 – 24:00   (late Pacific — overlaps Tokyo open)
      London:   07:00 – 16:00
      New York: 12:00 – 21:00

    Bug fixed: original code checked (22 ≤ h OR h < 7) → "sydney" first,
    which swallowed 00:00–06:59 UTC and left Tokyo with only 07:00–08:59.
    Now Tokyo is checked first for 00:00–08:59, Sydney only for 22:00+.
    """
    h = _utc_hour()
    if 0 <= h < 9:    return "tokyo"     # full Asian session 00:00–09:00
    if 22 <= h:       return "sydney"    # late Pacific  22:00–23:59
    if 7 <= h < 16:   return "london"
    if 12 <= h < 21:  return "new_york"
    return "off"                          # 21:00–22:00 gap between NY close and Sydney open


def _is_market_open(category: str) -> bool:
    session = _active_session()
    h       = _utc_hour()
    if category == "crypto":
        return True
    if category == "forex":
        return session != "off"
    if category in ("stocks", "indices"):
        return 13 <= h < 21               # NYSE hours approx UTC
    if category == "commodities":
        return session in ("london", "new_york")
    return True


_SESSION_BOOST: Dict[str, float] = {
    "london":   0.04,
    "new_york": 0.03,
    "tokyo":    0.02,   # small boost — Asian session is valid but lower liquidity
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