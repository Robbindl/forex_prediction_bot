from __future__ import annotations

import time
import threading
import json
from collections import deque
from typing import Dict, List, Optional, Tuple
from utils.logger import get_logger

logger = get_logger()

# ── Configuration ─────────────────────────────────────────────────────────────
WALL_MEMORY_SECS    = 300      # remember walls for 5 minutes
HUNT_MEMORY_SECS    = 120      # remember hunts for 2 minutes
HUNT_REJECTION_RATE = 3        # reject entry if 3+ hunts in last 2 mins @ entry level
MAX_ENTERED_ENTRIES = 5        # do not reject more than N entries per period (prevent over-rejection)


class OrderFlowSignalValidator:
    """
    Validates trading signals against order flow intelligence (walls & hunts).
    
    Thread-safe. Maintains rolling windows of recent walls and hunts.
    """

    def __init__(self) -> None:
        self._lock           = threading.RLock()
        self._running        = False
        self._sub_thread     = None
        self._redis_subscription_enabled = True
        
        # Rolling windows: deque([{"price": X, "strength": Y, "ts": Z}, ...])
        self._walls:    deque = deque(maxlen=500)
        self._hunts:    deque = deque(maxlen=500)
        
        # Cache: "BTCUSDT:12345.0" → ts of rejection (rate-limit over-rejections)
        self._rejections: Dict[str, float] = {}
        self._rejection_window_secs = 300
        
        # Redis
        self._pub = None
        self._init_redis()

    # ── Public API ────────────────────────────────────────────────────────────

    def start(self, use_redis_subscriber: bool = True) -> None:
        """Start the validator. Optionally subscribe to Redis for wall/hunt alerts."""
        if self._running:
            return
        self._running = True
        self._redis_subscription_enabled = bool(use_redis_subscriber)
        if self._redis_subscription_enabled:
            self._sub_thread = threading.Thread(
                target=self._subscribe_loop, name="OrderFlowValidator", daemon=True
            )
            self._sub_thread.start()
            logger.info("[OrderFlowValidator] Started with Redis subscriber")
        else:
            self._sub_thread = None
            logger.info("[OrderFlowValidator] Started in direct-feed mode")

    def stop(self) -> None:
        """Stop subscribing."""
        self._running = False
        self._sub_thread = None

    def validate_signal(self, signal: Dict) -> Tuple[bool, Optional[str]]:
        """
        Validate a trading signal against order flow conditions.
        
        Returns:
          (allowed: bool, reason: str or None)
          
        Rejection reasons:
          - "stop_hunt_at_entry" → entry price matches recent hunt location
          - "strong_wall_at_entry" → EXTREME wall blocks entry price
          - "stop_hunt_congestion" → too many hunts at this level → risky
        """
        asset = signal.get("asset", "")
        entry = float(signal.get("entry_price", 0))
        direction = signal.get("direction", "BUY").upper()
        
        if not asset or not entry:
            return True, None
        
        with self._lock:
            # Clean stale data
            self._cleanup()
            
            # Check 1: Stop hunt congestion at entry level
            hunt_reason = self._check_hunt_congestion(asset, entry, direction)
            if hunt_reason:
                # Rate-limit rejections for this asset:price pair
                cache_key = f"{asset}:{entry:.6f}"
                last_rejection = self._rejections.get(cache_key, 0)
                time_since = time.time() - last_rejection
                
                if time_since > self._rejection_window_secs:
                    # Allow rejection after cooldown
                    self._rejections[cache_key] = time.time()
                    logger.warning(
                        f"[OrderFlowValidator] {asset} {direction} @ {entry:.6f} rejected: {hunt_reason}"
                    )
                    return False, hunt_reason
            
            # Check 2: EXTREME wall at entry price
            wall_reason = self._check_extreme_wall(asset, entry, direction)
            if wall_reason:
                logger.warning(
                    f"[OrderFlowValidator] {asset} {direction} @ {entry:.6f} blocked by wall"
                )
                return False, wall_reason
        
        return True, None

    def adjust_signal(self, signal: Dict) -> Dict:
        """
        Adjust signal parameters based on order flow conditions.
        Returns modified signal dict.
        
        Adjustments:
          - Tighten stop loss if strong wall nearby
          - Reduce position size if high hunt activity
        """
        asset = signal.get("asset", "")
        entry = float(signal.get("entry_price", 0))
        sl = float(signal.get("stop_loss", 0))
        direction = signal.get("direction", "BUY").upper()
        pos_size = float(signal.get("position_size", 1.0))
        
        if not asset or not entry or not sl:
            return signal
        
        with self._lock:
            self._cleanup()
            
            # Adjustment 1: Tighten SL if strong wall between entry and current SL
            adjusted_sl = self._tighten_stop_loss(asset, entry, sl, direction)
            if adjusted_sl != sl:
                logger.info(
                    f"[OrderFlowValidator] {asset} SL tightened: {sl:.6f} → {adjusted_sl:.6f} "
                    f"(wall detected)"
                )
                signal["stop_loss"] = adjusted_sl
            
            # Adjustment 2: Reduce position size if hunt activity is high
            hunt_count = self._count_hunts_near(asset, entry, window_pct=0.5)
            if hunt_count >= 2:
                reduction_factor = 1.0 - (hunt_count * 0.15)  # -15% per hunt
                reduction_factor = max(0.6, reduction_factor)  # min 60% of original size
                adjusted_size = pos_size * reduction_factor
                if adjusted_size != pos_size:
                    logger.info(
                        f"[OrderFlowValidator] {asset} position size reduced: {pos_size:.4f} → {adjusted_size:.4f} "
                        f"({hunt_count} hunts detected)"
                    )
                    signal["position_size"] = adjusted_size
        
        return signal

    def get_intelligence(self, asset: str) -> Dict:
        """
        Return current order flow intelligence for an asset.
        Used by dashboard and logging.
        """
        with self._lock:
            self._cleanup()
            
            walls = [w for w in self._walls if w.get("asset") == asset]
            hunts = [h for h in self._hunts if h.get("asset") == asset]
            
            return {
                "asset": asset,
                "wall_count": len(walls),
                "hunt_count": len(hunts),
                "walls": walls[-10:] if walls else [],  # last 10
                "hunts": hunts[-10:] if hunts else [],  # last 10
            }

    def ingest_wall(self, event: dict) -> None:
        """Direct in-process ingest path for wall events."""
        self._ingest_wall(event)

    def ingest_hunt(self, event: dict) -> None:
        """Direct in-process ingest path for stop-hunt events."""
        self._ingest_hunt(event)

    # ── Internal ──────────────────────────────────────────────────────────────

    def _init_redis(self) -> None:
        try:
            from services.redis_pool import get_client as _get_redis_client
            self._pub = _get_redis_client()
            self._pub.ping()
        except Exception as e:
            logger.debug(f"[OrderFlowValidator] Redis unavailable: {e}")

    def _subscribe_loop(self) -> None:
        """Background thread — subscribes to wall and hunt alerts."""
        ps = None
        redis_unavailable_logged = False
        while self._running:
            try:
                from services.redis_pool import get_pubsub as _get_pubsub
                ps = _get_pubsub(old_pubsub=ps)
                if ps is None:
                    if not redis_unavailable_logged:
                        logger.info("[OrderFlowValidator] No Redis — running in validation-only mode")
                        redis_unavailable_logged = True
                    time.sleep(10)
                    continue
                redis_unavailable_logged = False
                ps.subscribe("LIQUIDITY_WALL_DETECTED", "STOP_HUNT_DETECTED")
                logger.info("[OrderFlowValidator] Subscribed to wall and hunt alerts")
                
                for msg in ps.listen():
                    if not self._running:
                        break
                    if msg.get("type") == "message":
                        ch = msg.get("channel", b"").decode() if isinstance(msg.get("channel"), bytes) else msg.get("channel", "")
                        try:
                            event = json.loads(msg["data"])
                            if ch == "LIQUIDITY_WALL_DETECTED":
                                self._ingest_wall(event)
                            elif ch == "STOP_HUNT_DETECTED":
                                self._ingest_hunt(event)
                        except Exception as e:
                            logger.debug(f"[OrderFlowValidator] Ingest error: {e}")
            except Exception as e:
                logger.warning(f"[OrderFlowValidator] Subscriber dropped ({e}) — retrying in 10s")
                if self._running:
                    time.sleep(10)

    def _ingest_wall(self, event: dict) -> None:
        """Ingest a LIQUIDITY_WALL_DETECTED event."""
        with self._lock:
            self._walls.append({
                "asset": event.get("asset"),
                "side": event.get("side"),
                "price": float(event.get("price", 0)),
                "size": float(event.get("size", 0)),
                "strength": event.get("wall_strength", "UNKNOWN"),
                "ts": time.time(),
            })

    def _ingest_hunt(self, event: dict) -> None:
        """Ingest a STOP_HUNT_DETECTED event."""
        with self._lock:
            self._hunts.append({
                "asset": event.get("asset"),
                "side": event.get("wall_side"),
                "price": float(event.get("wall_price", 0)),
                "spike": float(event.get("spike_price", 0)),
                "wick_pct": float(event.get("wick_pct", 0)),
                "confidence": float(event.get("confidence", 0)),
                "ts": time.time(),
            })

    def _cleanup(self) -> None:
        """Remove stale data from rolling windows."""
        now = time.time()
        
        # Keep only recent walls
        while self._walls and (now - self._walls[0].get("ts", 0)) > WALL_MEMORY_SECS:
            self._walls.popleft()
        
        # Keep only recent hunts
        while self._hunts and (now - self._hunts[0].get("ts", 0)) > HUNT_MEMORY_SECS:
            self._hunts.popleft()
        
        # Clean rejection cache
        stale_keys = [
            k for k, v in self._rejections.items()
            if (now - v) > self._rejection_window_secs
        ]
        for k in stale_keys:
            del self._rejections[k]

    def _check_hunt_congestion(self, asset: str, entry: float, direction: str) -> Optional[str]:
        """
        Check if entry price has too many recent hunts nearby.
        Returns rejection reason or None.
        """
        hunts_near = self._count_hunts_near(asset, entry, window_pct=0.3)
        if hunts_near >= HUNT_REJECTION_RATE:
            return f"stop_hunt_congestion ({hunts_near} hunts @ {entry:.6f})"
        return None

    def _check_extreme_wall(self, asset: str, entry: float, direction: str) -> Optional[str]:
        """
        Check if an EXTREME wall blocks the entry price.
        Returns rejection reason or None.
        """
        for wall in self._walls:
            if wall.get("asset") != asset:
                continue
            if wall.get("strength") != "EXTREME":
                continue
            
            wall_price = wall.get("price", 0)
            wall_side = wall.get("side", "")
            
            # Check if entry is on the wrong side of the wall
            if direction == "BUY" and wall_side == "ASK" and entry >= wall_price:
                # Trying to buy above an ASK wall = blocked
                return f"extreme_wall_blocks_entry ({wall_side} @ {wall_price:.6f})"
            
            if direction == "SELL" and wall_side == "BID" and entry <= wall_price:
                # Trying to sell below a BID wall = blocked
                return f"extreme_wall_blocks_entry ({wall_side} @ {wall_price:.6f})"
        
        return None

    def _tighten_stop_loss(self, asset: str, entry: float, sl: float, direction: str) -> float:
        """
        If a strong wall is between entry and SL, tighten SL above the wall.
        Returns new SL or original if no adjustment needed.
        """
        # Only look for walls between entry and SL
        if direction == "BUY":
            # SL is below entry; look for walls in (SL, entry)
            walls_in_zone = [
                w for w in self._walls
                if (w.get("asset") == asset and 
                    w.get("strength") == "STRONG" and
                    sl < w.get("price", 0) < entry)
            ]
            if walls_in_zone:
                # Tighten to just above the wall
                wall_price = max(w.get("price", 0) for w in walls_in_zone)
                new_sl = wall_price * 1.001  # 0.1% buffer above wall
                return max(new_sl, sl)  # never loosen
        else:
            # SELL: SL is above entry; look for walls in (entry, SL)
            walls_in_zone = [
                w for w in self._walls
                if (w.get("asset") == asset and
                    w.get("strength") == "STRONG" and
                    entry < w.get("price", 0) < sl)
            ]
            if walls_in_zone:
                # Tighten to just below the wall
                wall_price = min(w.get("price", 0) for w in walls_in_zone)
                new_sl = wall_price * 0.999  # 0.1% buffer below wall
                return min(new_sl, sl)  # never loosen
        
        return sl

    def _count_hunts_near(self, asset: str, price: float, window_pct: float = 0.3) -> int:
        """
        Count hunts near a price level (within ±window_pct).
        window_pct = 0.3 means ±0.3% of price.
        """
        margin = price * (window_pct / 100)
        lower = price - margin
        upper = price + margin
        
        return sum(
            1 for h in self._hunts
            if (h.get("asset") == asset and 
                lower <= h.get("price", 0) <= upper)
        )


# ── Singleton ─────────────────────────────────────────────────────────────────
_validator: Optional[OrderFlowSignalValidator] = None

def get_validator() -> OrderFlowSignalValidator:
    """Get or create the signal validator singleton."""
    global _validator
    if _validator is None:
        _validator = OrderFlowSignalValidator()
    return _validator


def start_validator() -> None:
    """Start the validator (subscribe to Redis)."""
    get_validator().start()


def stop_validator() -> None:
    """Stop the validator."""
    get_validator().stop()


__all__ = [
    "OrderFlowSignalValidator",
    "get_validator",
    "start_validator",
    "stop_validator",
]
