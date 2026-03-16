"""
services/intelligence_alerts/alert_router.py — Alert delivery router.

Routes formatted alerts to:
  1. Telegram — rich Markdown messages via existing TelegramCommander
  2. Redis     — INTELLIGENCE_ALERT channel for the dashboard live feed
  3. Logger    — always logged regardless of other destinations

Designed to fail gracefully — if Telegram is down, Redis still gets
the alert. If both are down, the logger records it.

Run tests
---------
    pytest tests/test_intelligence_alerts.py::TestAlertRouter -v
"""
from __future__ import annotations

import json
import time
from typing import Any, Dict, Optional

from utils.logger import get_logger

logger = get_logger()


class AlertRouter:
    """
    Routes formatted alert messages to all configured destinations.
    Add new destinations by adding a method and calling it in route().
    """

    def __init__(self) -> None:
        self._telegram  = None
        self._pub       = None
        self._init_redis()

    # ── Public API ────────────────────────────────────────────────────────────

    def set_telegram(self, telegram_bot) -> None:
        self._telegram = telegram_bot

    def route(
        self,
        channel:  str,
        message:  str,
        event:    Dict[str, Any],
        priority: str,
    ) -> None:
        """Send alert to all configured destinations."""
        # 1. Always log
        log_level = {
            "CRITICAL": "warning",
            "HIGH":     "info",
            "MEDIUM":   "info",
            "LOW":      "debug",
        }.get(priority, "info")
        getattr(logger, log_level)(
            f"[IntelAlert] [{priority}] {channel}: "
            f"{event.get('asset', event.get('narrative', ''))}"
        )

        # 2. Telegram
        self._send_telegram(message, priority)

        # 3. Redis dashboard channel
        self._publish_redis(channel, message, event, priority)

    # ── Destinations ──────────────────────────────────────────────────────────

    def _send_telegram(self, message: str, priority: str) -> None:
        if not self._telegram:
            return
        try:
            # Use send_message directly — TelegramCommander handles rate limiting
            self._telegram.send_message(message)
        except Exception as e:
            logger.debug(f"[AlertRouter] Telegram send error: {e}")

    def _publish_redis(
        self,
        channel:  str,
        message:  str,
        event:    Dict[str, Any],
        priority: str,
    ) -> None:
        if not self._pub:
            return
        try:
            payload = {
                "type":     "INTELLIGENCE_ALERT",
                "channel":  channel,
                "priority": priority,
                "message":  message,
                "event":    event,
                "ts":       int(time.time() * 1000),
            }
            self._pub.publish(
                "INTELLIGENCE_ALERT",
                json.dumps(payload, default=str),
            )
        except Exception as e:
            logger.debug(f"[AlertRouter] Redis publish error: {e}")
            self._pub = None   # stop hammering dead Redis

    # ── Setup ─────────────────────────────────────────────────────────────────

    def _init_redis(self) -> None:
        try:
            import redis
            from config.config import REDIS_URL
            self._pub = redis.from_url(REDIS_URL)
            self._pub.ping()
        except Exception as e:
            logger.debug(f"[AlertRouter] Redis unavailable: {e}")
