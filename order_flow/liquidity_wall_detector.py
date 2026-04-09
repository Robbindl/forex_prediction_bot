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
            from services.redis_pool import get_client as _get_redis_client

            self._pub = _get_redis_client()
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

        sizes = self._extract_level_sizes(levels)
        if not sizes:
            return []

        avg = sum(sizes) / len(sizes)
        if avg == 0:
            return []

        walls = []
        for level in levels:
            event = self._build_wall_event(level, side=side, avg=avg, mid_price=mid_price)
            if event is None:
                continue
            walls.append(event)
            self._publish_wall(event)

        # Prune stale cooldown entries (keep memory bounded)
        self._prune_cooldown()

        return walls

    @staticmethod
    def _extract_level_sizes(levels: List) -> List[float]:
        sizes: List[float] = []
        for level in levels:
            try:
                sizes.append(float(level[1]))
            except (IndexError, ValueError, TypeError):
                continue
        return sizes

    def _build_wall_event(
        self,
        level,
        side: str,
        avg: float,
        mid_price: float,
    ) -> Optional[dict]:
        try:
            price = float(level[0])
            qty = float(level[1])
        except (IndexError, ValueError, TypeError):
            return None

        ratio = qty / avg
        strength = self._classify_strength(ratio)
        if strength is None:
            return None

        now = time.time()
        cache_key = f"{side}:{price:.8f}"
        last_alert = self._cooldown.get(cache_key, 0)
        if now - last_alert < WALL_ALERT_COOLDOWN_SECS:
            return None
        self._cooldown[cache_key] = now

        distance_pct = 0.0
        if mid_price and mid_price > 0:
            distance_pct = round(abs(price - mid_price) / mid_price * 100, 4)

        return {
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
            "ts":           int(now * 1000),
        }

    def _publish_wall(self, event: dict) -> None:
        # FIX HIGH: Reconnect Redis if previous publish failed.
        # Previously self._pub = None on error, never recovered.
        if self._pub is None:
            self._init_redis()

        if self._pub:
            try:
                self._pub.publish("LIQUIDITY_WALL_DETECTED", json.dumps(event))
            except Exception as e:
                logger.debug(f"[WallDetector] Redis publish: {e}")
                self._pub = None   # will reconnect on next wall detection

        logger.info(
            f"[WallDetector] {self.asset} {event['side']} wall @ {event['price']:.6f} "
            f"size={event['size']:.2f} ({event['size_ratio']:.1f}× avg) [{event['strength']}]"
        )

    def _prune_cooldown(self) -> None:
        if len(self._cooldown) > 500:
            cutoff = time.time() - WALL_ALERT_COOLDOWN_SECS * 2
            self._cooldown = {
                k: v for k, v in self._cooldown.items() if v > cutoff
            }

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
