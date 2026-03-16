"""
order_flow/liquidity_wall_detector.py — Large order cluster detector.

Scans the order book for price levels whose size is significantly larger
than the average level. These "walls" act as:
  • Support  (large bid wall below price) — absorbs selling pressure
  • Resistance (large ask wall above price) — absorbs buying pressure
  • Price magnets — price tends to gravitate toward and test large walls

A wall also reveals institutional intent: a large bid wall signals a
buyer willing to defend that level; a large ask wall signals a seller.

Detection thresholds
--------------------
    MODERATE  — level size >= 5× average level size
    STRONG    — level size >= 10× average level size
    EXTREME   — level size >= 20× average level size

Redis events published
----------------------
    LIQUIDITY_WALL_DETECTED  {asset, side, price, size, size_ratio,
                               strength, distance_pct, ts}

Run tests
---------
    pytest tests/test_orderflow.py::TestLiquidityWallDetector -v
"""
from __future__ import annotations

import json
import time
from typing import Dict, List, Optional, Tuple

from utils.logger import get_logger

logger = get_logger()

# ── Thresholds ────────────────────────────────────────────────────────────────
WALL_MULTIPLIERS: Dict[str, float] = {
    "MODERATE": 5.0,
    "STRONG":   10.0,
    "EXTREME":  20.0,
}

# Only alert once per price level per N seconds (avoid spam)
WALL_ALERT_COOLDOWN_SECS = 120


class LiquidityWallDetector:
    """
    Stateless wall scanner with a rate-limiting cache.
    Call scan() on every order book snapshot.
    """

    def __init__(self, asset: str) -> None:
        self.asset     = asset
        self._cooldown: Dict[str, float] = {}   # "BID:price" → last alert ts
        self._pub                        = None
        self._init_redis()

    # ── Public API ────────────────────────────────────────────────────────────

    def scan(
        self,
        top_bids: List,
        top_asks: List,
        mid_price: float = 0.0,
    ) -> List[dict]:
        """
        Scan bid and ask levels for walls.
        Returns list of wall event dicts (may be empty).
        Each detected wall is also published to Redis.
        """
        walls: List[dict] = []
        walls += self._find_walls(top_bids, "BID",  mid_price)
        walls += self._find_walls(top_asks, "ASK",  mid_price)
        return walls

    # ── Internal ──────────────────────────────────────────────────────────────

    def _init_redis(self) -> None:
        try:
            import redis
            from config.config import REDIS_URL
            self._pub = redis.from_url(REDIS_URL)
            self._pub.ping()
        except Exception as e:
            logger.debug(f"[WallDetector] Redis unavailable for {self.asset}: {e}")

    def _find_walls(
        self,
        levels: List,
        side: str,
        mid_price: float,
    ) -> List[dict]:
        if len(levels) < 3:
            return []

        # Extract sizes; handle both [[price, qty], …] and [[price, qty, …], …]
        sizes = []
        for level in levels:
            try:
                sizes.append(float(level[1]))
            except (IndexError, ValueError, TypeError):
                continue

        if not sizes:
            return []

        avg = sum(sizes) / len(sizes)
        if avg == 0:
            return []

        walls = []
        for level in levels:
            try:
                price = float(level[0])
                qty   = float(level[1])
            except (IndexError, ValueError, TypeError):
                continue

            ratio = qty / avg
            strength = self._classify_strength(ratio)
            if strength is None:
                continue

            # Rate-limit: don't re-alert the same wall too frequently
            cache_key = f"{side}:{price:.8f}"
            last_alert = self._cooldown.get(cache_key, 0)
            if time.time() - last_alert < WALL_ALERT_COOLDOWN_SECS:
                continue
            self._cooldown[cache_key] = time.time()

            distance_pct = 0.0
            if mid_price and mid_price > 0:
                distance_pct = round(abs(price - mid_price) / mid_price * 100, 4)

            event = {
                "type":         "LIQUIDITY_WALL_DETECTED",
                "asset":        self.asset,
                "side":         side,
                "price":        price,
                "size":         round(qty, 4),
                "avg_size":     round(avg, 4),
                "size_ratio":   round(ratio, 2),
                "strength":     strength,
                "distance_pct": distance_pct,
                "implication":  self._implication(side, strength),
                "ts":           int(time.time() * 1000),
            }
            walls.append(event)

            if self._pub:
                try:
                    self._pub.publish("LIQUIDITY_WALL_DETECTED", json.dumps(event))
                except Exception as e:
                    logger.debug(f"[WallDetector] Redis publish: {e}")
                    self._pub = None

            logger.info(
                f"[WallDetector] {self.asset} {side} wall @ {price:.6f} "
                f"size={qty:.2f} ({ratio:.1f}× avg) [{strength}]"
            )

        # Prune stale cooldown entries (keep memory bounded)
        if len(self._cooldown) > 500:
            cutoff = time.time() - WALL_ALERT_COOLDOWN_SECS * 2
            self._cooldown = {
                k: v for k, v in self._cooldown.items() if v > cutoff
            }

        return walls

    @staticmethod
    def _classify_strength(ratio: float) -> Optional[str]:
        if   ratio >= WALL_MULTIPLIERS["EXTREME"]:  return "EXTREME"
        elif ratio >= WALL_MULTIPLIERS["STRONG"]:   return "STRONG"
        elif ratio >= WALL_MULTIPLIERS["MODERATE"]: return "MODERATE"
        return None

    @staticmethod
    def _implication(side: str, strength: str) -> str:
        if side == "BID":
            return {
                "EXTREME":  "Very strong support — institutional buyer defending level",
                "STRONG":   "Strong support — significant buying interest",
                "MODERATE": "Moderate support — watch for test of this level",
            }.get(strength, "")
        else:
            return {
                "EXTREME":  "Very strong resistance — institutional seller defending level",
                "STRONG":   "Strong resistance — significant selling interest",
                "MODERATE": "Moderate resistance — watch for rejection at this level",
            }.get(strength, "")
