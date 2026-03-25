from __future__ import annotations

import json
import threading
import time
from collections import deque
from typing import Deque, Dict, Optional

from utils.logger import get_logger

logger = get_logger()

# ── Configuration ─────────────────────────────────────────────────────────────
CASCADE_WINDOW_SECS   = 60          # rolling window to detect a cascade
CASCADE_USD_THRESHOLD = 10_000_000  # $10M in window → cascade alert
CRITICAL_USD          = 50_000_000  # $50M → severity = CRITICAL


class LiquidationStream:
    """
    Subscribes to LIQUIDATION_EVENT from Redis.
    Tracks USD value of liquidations per asset in a rolling window.
    Fires a LIQUIDATION_CASCADE_ALERT when the threshold is crossed.

    Falls back gracefully if Redis is unavailable — no crash.
    """

    def __init__(self) -> None:
        self._window:  Deque[dict]     = deque(maxlen=2000)
        self._totals:  Dict[str, float] = {}    # asset → lifetime USD liquidated
        self._lock     = threading.Lock()
        self._running  = False
        self._pub                       = None  # Redis publisher (lazy)
        self._last_cascade: Dict[str, float] = {}   # asset → ts of last alert

    # ── Public API ────────────────────────────────────────────────────────────

    def start(self) -> None:
        self._running = True
        self._init_redis()
        t = threading.Thread(
            target=self._subscribe, name="LiqStream", daemon=True
        )
        t.start()
        logger.info("[LiqStream] Started — watching for liquidation cascades")

    def stop(self) -> None:
        self._running = False

    def get_totals(self) -> Dict[str, float]:
        with self._lock:
            return dict(self._totals)

    def ingest(self, event: dict) -> None:
        """
        Public method so exchange_stream_manager can call us directly
        (without going through Redis) if desired.
        """
        self._process(event)

    # ── Internal ──────────────────────────────────────────────────────────────

    def _init_redis(self) -> None:
        try:
            import redis
            from config.config import REDIS_URL
            from services.redis_pool import get_client as _get_redis_client

            self._pub = _get_redis_client()
            self._pub.ping()
        except Exception as e:
            logger.warning(f"[LiqStream] Redis unavailable: {e}")

    def _subscribe(self) -> None:
        """Subscribe to Redis LIQUIDATION_EVENT channel."""
        if not self._pub:
            logger.info("[LiqStream] No Redis — running in direct-ingest mode only")
            return
        ps = None
        while self._running:
            try:
                from services.redis_pool import get_pubsub as _get_pubsub
                ps = _get_pubsub(old_pubsub=ps)  # close old before new
                ps.subscribe("LIQUIDATION_EVENT")
                logger.info("[LiqStream] Subscribed to LIQUIDATION_EVENT")
                for msg in ps.listen():
                    if not self._running:
                        break
                    if msg.get("type") == "message":
                        try:
                            self._process(json.loads(msg["data"]))
                        except Exception as e:
                            logger.debug(f"[LiqStream] Process error: {e}")
            except Exception as e:
                logger.error(f"[LiqStream] Subscribe error: {e}")
                if self._running:
                    import time; time.sleep(10)

    def _process(self, event: dict) -> None:
        asset    = event.get("asset", "UNKNOWN")
        # FIX S4: Bybit v5 API sends "qty" not "size" for liquidation quantity.
        # Previously size=event.get("size",0) always returned 0 for Bybit events
        # → size_usd = 0*price = 0 → cascade_usd never exceeded threshold →
        # LIQUIDATION_CASCADE_ALERT never fired.  Now we check both field names.
        size     = float(event.get("qty", event.get("size", 0)) or 0)
        price    = float(event.get("price", 0) or 0)
        size_usd = size * price
        ts       = event.get("ts", int(time.time() * 1000))

        with self._lock:
            event["size_usd"] = size_usd
            self._window.append({**event, "size_usd": size_usd})
            self._totals[asset] = self._totals.get(asset, 0.0) + size_usd

        cascade_usd = self._window_total(asset)
        if cascade_usd >= CASCADE_USD_THRESHOLD:
            # Rate-limit: only one cascade alert per asset per minute
            last = self._last_cascade.get(asset, 0)
            if time.time() - last >= 60:
                self._last_cascade[asset] = time.time()
                self._publish_cascade(asset, cascade_usd)

    def _window_total(self, asset: str) -> float:
        cutoff = (time.time() - CASCADE_WINDOW_SECS) * 1000
        with self._lock:
            return sum(
                e.get("size_usd", 0)
                for e in self._window
                if e.get("asset") == asset and e.get("ts", 0) >= cutoff
            )

    def _publish_cascade(self, asset: str, usd_total: float) -> None:
        severity = "CRITICAL" if usd_total >= CRITICAL_USD else "HIGH"
        event = {
            "type":      "LIQUIDATION_CASCADE_ALERT",
            "asset":     asset,
            "usd_total": round(usd_total, 2),
            "window_s":  CASCADE_WINDOW_SECS,
            "severity":  severity,
            "ts":        int(time.time() * 1000),
        }
        if self._pub:
            try:
                self._pub.publish("LIQUIDATION_CASCADE_ALERT", json.dumps(event))
            except Exception as e:
                logger.debug(f"[LiqStream] Redis publish: {e}")
        logger.warning(
            f"[LiqStream] CASCADE [{severity}] {asset} — "
            f"${usd_total:,.0f} in {CASCADE_WINDOW_SECS}s"
        )