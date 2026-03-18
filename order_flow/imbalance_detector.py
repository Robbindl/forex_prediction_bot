from __future__ import annotations

import json
import time
from collections import deque
from typing import Deque, Dict, Optional

from utils.logger import get_logger

logger = get_logger()

# ── Thresholds ────────────────────────────────────────────────────────────────
STRONG_THRESHOLD  = 0.40
MILD_THRESHOLD    = 0.20
ROLLING_WINDOW    = 10      # snapshots to average before alerting
ALERT_COOLDOWN_SECS = 60    # minimum seconds between imbalance alerts


class ImbalanceDetector:
    """
    Per-asset rolling imbalance tracker.
    analyse() is called by __init__.py on every ORDERBOOK_SNAPSHOT.
    """

    def __init__(self, asset: str) -> None:
        self.asset       = asset
        self._scores:    Deque[float] = deque(maxlen=ROLLING_WINDOW)
        self._last_alert: float       = 0.0
        self._pub                     = None
        self._init_redis()

    # ── Public API ────────────────────────────────────────────────────────────

    def analyse(self, snapshot: dict) -> Optional[dict]:
        """
        Process one ORDERBOOK_SNAPSHOT.
        Returns the alert dict if one was published, else None.
        """
        score = snapshot.get("imbalance", 0.0)
        self._scores.append(score)

        if len(self._scores) < ROLLING_WINDOW:
            return None     # not enough data yet

        rolling = sum(self._scores) / len(self._scores)
        bias    = self._classify(rolling)

        if bias == "NEUTRAL":
            return None

        if time.time() - self._last_alert < ALERT_COOLDOWN_SECS:
            return None

        self._last_alert = time.time()

        event = {
            "type":          "BID_ASK_IMBALANCE_ALERT",
            "asset":         self.asset,
            "score":         round(score, 4),
            "rolling_score": round(rolling, 4),
            "bias":          bias,
            "bid_vol":       round(snapshot.get("bid_vol", 0), 4),
            "ask_vol":       round(snapshot.get("ask_vol", 0), 4),
            "implication":   self._implication(bias),
            "ts":            int(time.time() * 1000),
        }

        if self._pub:
            try:
                self._pub.publish("BID_ASK_IMBALANCE_ALERT", json.dumps(event))
            except Exception as e:
                logger.debug(f"[ImbalanceDet] Redis publish {self.asset}: {e}")
                self._pub = None

        logger.info(
            f"[ImbalanceDet] {self.asset} imbalance={rolling:+.3f} [{bias}] "
            f"bids={snapshot.get('bid_vol', 0):.2f} "
            f"asks={snapshot.get('ask_vol', 0):.2f}"
        )
        return event

    def current_score(self) -> float:
        """
        Returns rolling imbalance score -1.0 … +1.0.
        Used by meta-model ensemble as the 'orderflow' input signal.
        Positive = bullish pressure, negative = bearish pressure.
        """
        if not self._scores:
            return 0.0
        return round(sum(self._scores) / len(self._scores), 4)

    def current_bias(self) -> str:
        return self._classify(self.current_score())

    # ── Internal ──────────────────────────────────────────────────────────────

    def _init_redis(self) -> None:
        try:
            import redis
            from config.config import REDIS_URL
            from services.redis_pool import get_client as _get_redis_client

            self._pub = _get_redis_client()
            self._pub.ping()
        except Exception as e:
            logger.debug(f"[ImbalanceDet] Redis unavailable for {self.asset}: {e}")

    @staticmethod
    def _classify(score: float) -> str:
        if   score >= STRONG_THRESHOLD:   return "STRONG_BUY"
        elif score >= MILD_THRESHOLD:     return "MILD_BUY"
        elif score <= -STRONG_THRESHOLD:  return "STRONG_SELL"
        elif score <= -MILD_THRESHOLD:    return "MILD_SELL"
        return "NEUTRAL"

    @staticmethod
    def _implication(bias: str) -> str:
        return {
            "STRONG_BUY":  "Heavy buying pressure — sellers being absorbed, expect upward move",
            "MILD_BUY":    "Moderate buying pressure — bulls in control short-term",
            "STRONG_SELL": "Heavy selling pressure — buyers being absorbed, expect downward move",
            "MILD_SELL":   "Moderate selling pressure — bears in control short-term",
        }.get(bias, "")