from __future__ import annotations

import json
import time
from collections import deque
from typing import Deque, Dict, List, Optional

from utils.logger import get_logger

logger = get_logger()

# ── Configuration ─────────────────────────────────────────────────────────────
WICK_THRESHOLD_PCT   = 0.15     # wick must pierce level by at least 0.15 %
REVERT_WINDOW_MS     = 30_000   # must revert within 30 seconds
MIN_PRICE_HISTORY    = 15       # need at least this many ticks before scanning
ALERT_COOLDOWN_SECS  = 120      # one alert per level per 2 minutes
MAX_PRICE_HISTORY    = 300      # rolling tick buffer size


class StopHuntDetector:
    """
    Per-asset stop-hunt detector.
    Receives price ticks from OrderbookProcessor and wall list from
    LiquidityWallDetector via __init__.py.
    """

    def __init__(self, asset: str) -> None:
        self.asset       = asset
        self._prices:    Deque[Dict] = deque(maxlen=MAX_PRICE_HISTORY)
        self._walls:     List[dict]  = []
        self._cooldown:  Dict[str, float] = {}   # "SIDE:price" → last alert ts
        self._pub                         = None
        self._init_redis()

    # ── Public API ────────────────────────────────────────────────────────────

    def ingest_price(self, price: float, ts: int) -> None:
        """
        Feed a new mid-price tick. Called by __init__.py on every
        ORDERBOOK_SNAPSHOT after OrderbookProcessor.update().
        """
        self._prices.append({"price": price, "ts": ts})
        if len(self._prices) >= MIN_PRICE_HISTORY:
            self._scan()

    def update_walls(self, walls: List[dict]) -> None:
        """
        Receive fresh wall list from LiquidityWallDetector.scan().
        Replaces stale wall list — walls change as orders fill/cancel.
        """
        self._walls = [w for w in walls if w.get("price", 0) > 0]

    # ── Internal ──────────────────────────────────────────────────────────────

    def _init_redis(self) -> None:
        try:
            import redis
            from config.config import REDIS_URL
            from services.redis_pool import get_client as _get_redis_client

            self._pub = _get_redis_client()
            self._pub.ping()
        except Exception as e:
            logger.debug(f"[StopHunt] Redis unavailable for {self.asset}: {e}")

    def _scan(self) -> None:
        """Check each known wall for a wick-and-revert pattern."""
        ticks = list(self._prices)
        for wall in self._walls:
            level     = wall.get("price", 0.0)
            side      = wall.get("side", "")
            if not level or not side:
                continue

            rate_key  = f"{side}:{level:.8f}"
            last_seen = self._cooldown.get(rate_key, 0)
            if time.time() - last_seen < ALERT_COOLDOWN_SECS:
                continue

            hunt = self._detect_hunt(ticks, level, side)
            if hunt:
                self._cooldown[rate_key] = time.time()
                self._publish_hunt(wall, hunt)

    def _detect_hunt(
        self,
        ticks: List[Dict],
        level: float,
        side: str,
    ) -> Optional[Dict]:
        """
        Scan recent price ticks for the wick-and-revert pattern near level.

        Iterates over ALL ticks (not ticks[:-3]) so that a spike+revert
        sequence that was just appended is always included in the scan.
        The inner loop naturally stops when it runs out of subsequent ticks.

        Returns a result dict if pattern found, else None.
        """
        threshold = level * (WICK_THRESHOLD_PCT / 100)

        for i, tick in enumerate(ticks):
            spike_price = tick["price"]
            spike_ts    = tick["ts"]

            if side == "BID":
                # Wick: price dips below bid wall by at least threshold
                if spike_price >= level - threshold:
                    continue
                # Revert: look for price climbing back above the wall level
                for j in range(i + 1, len(ticks)):
                    revert_price = ticks[j]["price"]
                    revert_ts    = ticks[j]["ts"]
                    dt_ms        = revert_ts - spike_ts
                    if dt_ms > REVERT_WINDOW_MS:
                        break
                    if revert_price > level:
                        return {
                            "spike":     spike_price,
                            "revert":    revert_price,
                            "wick_pct":  round((level - spike_price) / level * 100, 4),
                            "revert_ms": dt_ms,
                        }

            elif side == "ASK":
                # Wick: price spikes above ask wall by at least threshold
                if spike_price <= level + threshold:
                    continue
                # Revert: look for price falling back below the wall level
                for j in range(i + 1, len(ticks)):
                    revert_price = ticks[j]["price"]
                    revert_ts    = ticks[j]["ts"]
                    dt_ms        = revert_ts - spike_ts
                    if dt_ms > REVERT_WINDOW_MS:
                        break
                    if revert_price < level:
                        return {
                            "spike":     spike_price,
                            "revert":    revert_price,
                            "wick_pct":  round((spike_price - level) / level * 100, 4),
                            "revert_ms": dt_ms,
                        }
        return None

    def _publish_hunt(self, wall: dict, hunt: dict) -> None:
        level       = wall["price"]
        side        = wall["side"]
        implication = "BUY" if side == "BID" else "SELL"

        # Confidence: larger wick + faster revert = higher confidence
        wick_score  = min(1.0, hunt["wick_pct"] / (WICK_THRESHOLD_PCT * 10))
        speed_score = max(0.0, 1.0 - hunt["revert_ms"] / REVERT_WINDOW_MS)
        confidence  = round(wick_score * 0.6 + speed_score * 0.4, 3)

        event = {
            "type":          "STOP_HUNT_DETECTED",
            "asset":         self.asset,
            "wall_price":    level,
            "wall_side":     side,
            "wall_strength": wall.get("strength", "UNKNOWN"),
            "spike_price":   hunt["spike"],
            "revert_price":  hunt["revert"],
            "wick_pct":      hunt["wick_pct"],
            "revert_ms":     hunt["revert_ms"],
            "implication":   implication,
            "confidence":    confidence,
            "ts":            int(time.time() * 1000),
        }

        if self._pub:
            try:
                self._pub.publish("STOP_HUNT_DETECTED", json.dumps(event))
            except Exception as e:
                logger.debug(f"[StopHunt] Redis publish {self.asset}: {e}")
                self._pub = None

        logger.warning(
            f"[StopHunt] {self.asset} {side} hunt @ {level:.6f} "
            f"wick={hunt['wick_pct']:.3f}% revert={hunt['revert_ms']}ms "
            f"→ {implication} (conf={confidence:.2f})"
        )