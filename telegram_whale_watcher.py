"""
telegram_whale_watcher.py — Real Telethon-based whale channel watcher.

Reads whale alert channels using your Telegram user account (Telethon)
and your whale bot token. Credentials come from .env:
  WHALE_TELEGRAM_TOKEN  — bot token for the whale alert bot
  TELEGRAM_API_ID       — from my.telegram.org
  TELEGRAM_API_HASH     — from my.telegram.org
  TELEGRAM_PHONE        — your phone number e.g. +254746204130
  TELEGRAM_SESSION      — session file name (default: whale_session)
"""
from __future__ import annotations

import os
import re
import threading
import time
from datetime import datetime
from typing import Dict, List, Optional
import asyncio

from utils.logger import get_logger

logger = get_logger()

# ── Credentials from .env ─────────────────────────────────────────────────────
_BOT_TOKEN   = os.getenv("WHALE_TELEGRAM_TOKEN", "")
_API_ID      = os.getenv("TELEGRAM_API_ID", "")
_API_HASH    = os.getenv("TELEGRAM_API_HASH", "")
_PHONE       = os.getenv("TELEGRAM_PHONE", "")
_SESSION     = os.getenv("TELEGRAM_SESSION", "whale_session")

# ── Whale alert channels to monitor ──────────────────────────────────────────
# Add or remove channel usernames as needed
WHALE_CHANNELS = [
    "WhaleSniper",
    "whalecointalk",
    "whalebotalerts",
    "whale_alert_io",
    "lookonchain"
]

# Minimum USD value to consider a whale alert
MIN_VALUE_USD = 1_000_000

# Regex patterns to extract transaction data from messages
_VALUE_PATTERNS = [
    re.compile(r'\$\s*([\d,]+(?:\.\d+)?)\s*([MBK])', re.IGNORECASE),
    re.compile(r'([\d,]+(?:\.\d+)?)\s*([MBK])\s*USD', re.IGNORECASE),
    re.compile(r'USD\s*([\d,]+(?:\.\d+)?)', re.IGNORECASE),
    re.compile(r'([\d,]+(?:\.\d+)?)\s*(?:million|billion)', re.IGNORECASE),
]

_SYMBOL_PATTERN = re.compile(
    r'\b(BTC|ETH|BNB|XRP|ADA|SOL|DOGE|DOT|AVAX|LINK|LTC|'
    r'MATIC|UNI|ATOM|XLM|TRX|ALGO|VET|FIL|THETA)\b',
    re.IGNORECASE,
)


def _parse_value_usd(text: str) -> float:
    """Extract USD value from a whale alert message."""
    for pattern in _VALUE_PATTERNS:
        match = pattern.search(text)
        if match:
            try:
                groups = match.groups()
                amount = float(groups[0].replace(',', ''))
                if len(groups) > 1 and groups[1]:
                    suffix = groups[1].upper()
                    if suffix == 'B':
                        amount *= 1_000_000_000
                    elif suffix == 'M':
                        amount *= 1_000_000
                    elif suffix == 'K':
                        amount *= 1_000
                return amount
            except (ValueError, IndexError):
                continue
    return 0.0


def _parse_symbol(text: str) -> str:
    """Extract crypto symbol from message text."""
    match = _SYMBOL_PATTERN.search(text)
    return match.group(0).upper() if match else "UNKNOWN"


def _parse_alert(text: str, source: str, date: datetime) -> Optional[Dict]:
    """
    Parse a raw Telegram message into a whale alert dict.
    Returns None if the message is not a qualifying whale alert.
    """
    lower = text.lower()
    if not any(kw in lower for kw in ['whale', 'transfer', 'moved', 'million', 'billion', '$']):
        return None

    value_usd = _parse_value_usd(text)
    if value_usd < MIN_VALUE_USD:
        return None

    symbol  = _parse_symbol(text)
    value_m = value_usd / 1_000_000
    title   = f"🐋 {symbol} ${value_m:.1f}M — {source}"

    return {
        "title":     title,
        "value_usd": value_usd,
        "symbol":    symbol,
        "date":      date.isoformat(),
        "source":    f"Telegram/{source}",
        "sentiment": 0.15 if value_usd > 10_000_000 else 0.1,
        "raw_text":  text[:200],
    }


class TelegramWhaleWatcher:
    """
    Monitors Telegram whale-alert channels using Telethon.
    Interface matches what whale_alert_manager.py expects:
      - .bot_token      — truthy when configured
      - .start_monitoring()
      - .stop_monitoring()
      - .get_recent_alerts()
    """

    def __init__(self):
        self.bot_token   = _BOT_TOKEN          # checked by WhaleAlertManager
        self._api_id     = int(_API_ID) if _API_ID.isdigit() else 0
        self._api_hash   = _API_HASH
        self._phone      = _PHONE
        self._session    = _SESSION

        self._client           = None
        self._is_running       = False
        self._monitor_thread:  Optional[threading.Thread] = None
        self._recent_alerts:   List[Dict] = []
        self._max_alerts       = 200
        self._lock             = threading.Lock()

        self.on_alert = None   # optional callback set by bot.py

        if self.bot_token and self._api_id and self._api_hash:
            logger.info(
                f"[TelegramWhaleWatcher] Configured — "
                f"api_id={self._api_id} phone={self._phone} "
                f"channels={len(WHALE_CHANNELS)}"
            )
        else:
            logger.warning(
                "[TelegramWhaleWatcher] Missing credentials — "
                "set WHALE_TELEGRAM_TOKEN, TELEGRAM_API_ID, TELEGRAM_API_HASH in .env"
            )

    # ── Public interface (called by WhaleAlertManager) ────────────────────

    def start_monitoring(self, interval_seconds: int = 120) -> None:
        """Start background Telethon listener thread."""
        if not self.bot_token or not self._api_id or not self._api_hash:
            logger.warning("[TelegramWhaleWatcher] Cannot start — credentials missing")
            return

        if self._is_running:
            return

        self._is_running = True
        self._monitor_thread = threading.Thread(
            target=self._run_listener,
            name="TelegramWhaleListener",
            daemon=True,
        )
        self._monitor_thread.start()
        logger.info("[TelegramWhaleWatcher] Listener thread started")

    def stop_monitoring(self) -> None:
        """Stop the listener and disconnect Telethon."""
        self._is_running = False
        if self._client:
            try:
                import asyncio
                loop = asyncio.new_event_loop()
                loop.run_until_complete(self._client.disconnect())
                loop.close()
            except Exception:
                pass
        logger.info("[TelegramWhaleWatcher] Stopped")

    def get_recent_alerts(self, min_value_usd: float = MIN_VALUE_USD) -> List[Dict]:
        """Return recent whale alerts above threshold."""
        with self._lock:
            return [
                a for a in self._recent_alerts
                if a.get("value_usd", 0) >= min_value_usd
            ]

    # ── Internal Telethon listener ────────────────────────────────────────

    def _run_listener(self) -> None:
        """Run the Telethon event loop in a background thread."""
        import asyncio
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self._listen_async())
        except Exception as e:
            logger.error(f"[TelegramWhaleWatcher] Listener crashed: {e}")
        finally:
            loop.close()

    async def _listen_async(self) -> None:
        """Async Telethon client — connects, fetches history, then listens live."""
        try:
            from telethon import TelegramClient, events
            from telethon.errors import SessionPasswordNeededError
        except ImportError:
            logger.error(
                "[TelegramWhaleWatcher] telethon not installed — "
                "run: pip install telethon"
            )
            return

        client = TelegramClient(self._session, self._api_id, self._api_hash)
        self._client = client

        try:
            await client.start(phone=self._phone)
            logger.info("[TelegramWhaleWatcher] Telethon connected")
        except SessionPasswordNeededError:
            logger.error(
                "[TelegramWhaleWatcher] 2FA enabled — "
                "run telethon interactively once to create the session file"
            )
            return
        except Exception as e:
            logger.error(f"[TelegramWhaleWatcher] Connect failed: {e}")
            return

        # ── Fetch recent history from each channel ──────────────────────
        for channel in WHALE_CHANNELS:
            if not self._is_running:
                break
            try:
                entity = await client.get_entity(channel)
                async for msg in client.iter_messages(entity, limit=50):
                    if msg.text:
                        alert = _parse_alert(msg.text, channel, msg.date or datetime.utcnow())
                        if alert:
                            self._add_alert(alert)
                logger.debug(f"[TelegramWhaleWatcher] Fetched history from {channel}")
            except Exception as e:
                logger.debug(f"[TelegramWhaleWatcher] Could not read {channel}: {e}")

        # ── Real-time listener for new messages ─────────────────────────
        @client.on(events.NewMessage(chats=WHALE_CHANNELS))
        async def _on_message(event):
            if not event.text:
                return
            channel_name = getattr(event.chat, "username", str(event.chat_id))
            alert = _parse_alert(event.text, channel_name, datetime.utcnow())
            if alert:
                self._add_alert(alert)
                logger.info(
                    f"[TelegramWhaleWatcher] NEW alert: "
                    f"{alert['symbol']} ${alert['value_usd']/1_000_000:.1f}M"
                )
                # Fire callback to bot.py → ingest_whale_alert()
                if self.on_alert:
                    try:
                        self.on_alert(alert)
                    except Exception:
                        pass

        logger.info(
            f"[TelegramWhaleWatcher] Listening to {len(WHALE_CHANNELS)} channels"
        )

        # Keep running until stopped
        while self._is_running:
            await asyncio.sleep(5)

        await client.disconnect()

    def _add_alert(self, alert: Dict) -> None:
        """Thread-safe insert into recent alerts list."""
        with self._lock:
            # Deduplicate by title
            existing_titles = {a["title"] for a in self._recent_alerts}
            if alert["title"] not in existing_titles:
                self._recent_alerts.insert(0, alert)
                self._recent_alerts = self._recent_alerts[: self._max_alerts]