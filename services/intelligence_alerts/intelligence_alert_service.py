from __future__ import annotations

import json
import threading
import time
from typing import Callable, Dict, List, Optional

from utils.logger import get_logger
from services.intelligence_alerts.alert_formatter import AlertFormatter
from services.intelligence_alerts.alert_router    import AlertRouter

logger = get_logger()

# ── All channels to subscribe to ─────────────────────────────────────────────
SUBSCRIBED_CHANNELS: List[str] = [
    # Data ingestion
    "LIQUIDATION_CASCADE_ALERT",
    "FUNDING_RATE_ALERT",
    "OI_CHANGE_ALERT",
    "MACRO_NEWS_EVENT",
    # Whale intelligence
    "WHALE_ACCUMULATION",
    "WHALE_DISTRIBUTION",
    "WHALE_CLUSTER_ALERT",
    "EXCHANGE_INFLOW_ALERT",
    "EXCHANGE_OUTFLOW_ALERT",
    # Order flow
    "LIQUIDITY_WALL_DETECTED",
    "BID_ASK_IMBALANCE_ALERT",
    "STOP_HUNT_DETECTED",
    # Narrative AI
    "NARRATIVE_TREND_DETECTED",
    "REDDIT_TOPIC_SPIKE",
    "TWITTER_TOPIC_SPIKE",
]

# ── Priority classification ───────────────────────────────────────────────────
CHANNEL_PRIORITY: Dict[str, str] = {
    "LIQUIDATION_CASCADE_ALERT": "CRITICAL",
    "WHALE_CLUSTER_ALERT":       "CRITICAL",
    "STOP_HUNT_DETECTED":        "HIGH",
    "WHALE_ACCUMULATION":        "HIGH",
    "WHALE_DISTRIBUTION":        "HIGH",
    "EXCHANGE_INFLOW_ALERT":     "HIGH",
    "EXCHANGE_OUTFLOW_ALERT":    "HIGH",
    "FUNDING_RATE_ALERT":        "HIGH",
    "BID_ASK_IMBALANCE_ALERT":   "MEDIUM",
    "MACRO_NEWS_EVENT":          "MEDIUM",
    "NARRATIVE_TREND_DETECTED":  "MEDIUM",
    "OI_CHANGE_ALERT":           "MEDIUM",
    "LIQUIDITY_WALL_DETECTED":   "LOW",
    "REDDIT_TOPIC_SPIKE":        "LOW",
    "TWITTER_TOPIC_SPIKE":       "LOW",
}

# ── Rate limits (seconds between alerts per channel) ─────────────────────────
PRIORITY_RATE_LIMITS: Dict[str, int] = {
    "CRITICAL": 60,
    "HIGH":     300,
    "MEDIUM":   600,
    "LOW":      900,
}


class IntelligenceAlertService:
    """
    Background service that subscribes to all market-intelligence Redis channels
    and dispatches formatted alerts to Telegram and dashboard.
    """

    def __init__(self) -> None:
        self._running      = False
        self._sub_thread:  Optional[threading.Thread] = None
        self._formatter    = AlertFormatter()
        self._router       = AlertRouter()
        self._rate_cache:  Dict[str, float] = {}   # channel → last sent ts
        self._lock         = threading.Lock()
        self._handlers:    List[Callable] = []      # custom handlers

    # ── Public API ────────────────────────────────────────────────────────────

    def start(self) -> None:
        self._running   = True
        self._sub_thread = threading.Thread(
            target=self._subscribe_loop,
            name="IntelAlerts",
            daemon=True,
        )
        self._sub_thread.start()
        logger.info(
            f"[IntelAlerts] Started — monitoring {len(SUBSCRIBED_CHANNELS)} channels"
        )

    def stop(self) -> None:
        self._running = False
        logger.info("[IntelAlerts] Stopped")

    def set_telegram(self, telegram_bot) -> None:
        self._router.set_telegram(telegram_bot)
        logger.info("[IntelAlerts] Telegram wired")

    def add_handler(self, fn: Callable[[dict, str], None]) -> None:
        """
        Register a custom handler called for every alert.
        fn(event_dict, priority) — use for custom integrations.
        """
        self._handlers.append(fn)

    # ── Internal ──────────────────────────────────────────────────────────────

    def _subscribe_loop(self, _old_ps=None) -> None:
        """Single background thread — subscribes to all channels."""
        ps = _old_ps
        redis_unavailable_logged = False
        while self._running:
            try:
                from services.redis_pool import get_pubsub as _get_pubsub
                ps = _get_pubsub(old_pubsub=ps)  # close old connection first
                if ps is None:
                    if not redis_unavailable_logged:
                        logger.warning("[IntelAlerts] Redis unavailable — subscriber paused")
                        redis_unavailable_logged = True
                    time.sleep(30)
                    continue

                redis_unavailable_logged = False
                ps.subscribe(*SUBSCRIBED_CHANNELS)
                logger.info(f"[IntelAlerts] Subscribed to {len(SUBSCRIBED_CHANNELS)} channels")

                for msg in ps.listen():
                    if not self._running:
                        break
                    if msg.get("type") != "message":
                        continue
                    try:
                        channel = msg.get("channel", b"").decode() if isinstance(
                            msg.get("channel"), bytes
                        ) else msg.get("channel", "")
                        data    = msg.get("data", b"")
                        if isinstance(data, bytes):
                            data = data.decode()
                        event = json.loads(data)
                        self._handle_event(channel, event)
                    except Exception as e:
                        logger.debug(f"[IntelAlerts] Parse error: {e}")
            except Exception as e:
                logger.warning(f"[IntelAlerts] Subscriber dropped ({e}) — retrying in 30s")
                time.sleep(30)

    def _handle_event(self, channel: str, event: dict) -> None:
        """Process one event — rate check, format, route."""
        priority = CHANNEL_PRIORITY.get(channel, "LOW")

        # Rate limiting (CRITICAL events always pass)
        if priority != "CRITICAL":
            if not self._rate_check(channel, priority):
                return

        # Format and route in a separate thread so we never block
        threading.Thread(
            target=self._dispatch,
            args=(channel, event, priority),
            daemon=True,
        ).start()

    def _rate_check(self, channel: str, priority: str) -> bool:
        """Return True if enough time has passed since last alert for this channel."""
        min_interval = PRIORITY_RATE_LIMITS.get(priority, 900)
        now = time.time()
        with self._lock:
            last = self._rate_cache.get(channel, 0)
            if now - last < min_interval:
                return False
            self._rate_cache[channel] = now
            return True

    def _dispatch(self, channel: str, event: dict, priority: str) -> None:
        """Format and send the alert to all configured destinations."""
        try:
            message = self._formatter.format(channel, event, priority)
            if not message:
                return

            # Route to Telegram and dashboard
            self._router.route(
                channel  = channel,
                message  = message,
                event    = event,
                priority = priority,
            )

            # Call custom handlers
            for fn in self._handlers:
                try:
                    fn(event, priority)
                except Exception as e:
                    logger.debug(f"[IntelAlerts] Custom handler error: {e}")

        except Exception as e:
            logger.error(f"[IntelAlerts] Dispatch error [{channel}]: {e}")
