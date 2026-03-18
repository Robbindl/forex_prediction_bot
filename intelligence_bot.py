from __future__ import annotations

import threading
from typing import Optional

from utils.logger import get_logger

logger = get_logger()


class IntelligenceBot:
    """
    Minimal send-only Telegram bot.
    No polling. No conflict. Just sends messages.
    """

    def __init__(self) -> None:
        self._token:   str = ""
        self._chat_id: str = ""
        self._lock     = threading.Lock()
        self._ready    = False
        self._load_credentials()

    def _load_credentials(self) -> None:
        try:
            from config.config import WHALE_TELEGRAM_TOKEN, INTELLIGENCE_CHAT_ID
            self._token   = WHALE_TELEGRAM_TOKEN   or ""
            self._chat_id = INTELLIGENCE_CHAT_ID   or ""
            if self._token and self._chat_id:
                self._ready = True
                logger.info(
                    f"[IntelBot] Ready — send-only via WHALE_TELEGRAM_TOKEN "
                    f"→ chat {self._chat_id}"
                )
            else:
                missing = []
                if not self._token:   missing.append("WHALE_TELEGRAM_TOKEN")
                if not self._chat_id: missing.append("INTELLIGENCE_CHAT_ID")
                logger.warning(
                    f"[IntelBot] Disabled — missing .env vars: {missing}"
                )
        except Exception as e:
            logger.warning(f"[IntelBot] Credential load error: {e}")

    def send_message(self, text: str) -> bool:
        """
        Send a message via Bot 2. Thread-safe.
        Returns True on success, False on failure.
        Never raises — always degrades gracefully.
        """
        if not self._ready:
            return False
        try:
            import requests
            url  = f"https://api.telegram.org/bot{self._token}/sendMessage"
            data = {
                "chat_id":    self._chat_id,
                "text":       text,
                "parse_mode": "Markdown",
            }
            with self._lock:
                resp = requests.post(url, json=data, timeout=10)
            if not resp.ok:
                logger.debug(f"[IntelBot] Send failed: {resp.status_code} {resp.text[:100]}")
                return False
            return True
        except Exception as e:
            logger.debug(f"[IntelBot] Send error: {e}")
            return False

    @property
    def is_ready(self) -> bool:
        return self._ready


# ── Global singleton ──────────────────────────────────────────────────────────
intelligence_bot = IntelligenceBot()
