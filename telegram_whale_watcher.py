from __future__ import annotations

import re
import threading
import time
from datetime import datetime
from typing import Dict, List, Optional
import asyncio

from config.config import (
    TELEGRAM_API_HASH,
    TELEGRAM_API_ID,
    TELEGRAM_PHONE,
    TELEGRAM_SESSION,
    WHALE_ALLOWED_ASSETS,
    WHALE_TELEGRAM_CHANNELS,
    WHALE_TELEGRAM_MIN_VALUE_USD,
    WHALE_TELEGRAM_TOKEN,
)
from services.intelligence_event_utils import record_whale_alert_event
from utils.logger import get_logger

logger = get_logger()

# ── Credentials from config ───────────────────────────────────────────────────
_BOT_TOKEN   = WHALE_TELEGRAM_TOKEN
_API_ID      = TELEGRAM_API_ID
_API_HASH    = TELEGRAM_API_HASH
_PHONE       = TELEGRAM_PHONE
# Telethon persists auth in <session>.session. For this bot that file is
# typically whale_session.session and should be preserved across runs.
_SESSION     = TELEGRAM_SESSION

# ── Whale alert channels to monitor ──────────────────────────────────────────
# One primary Telegram fallback channel is preferred when the paid API is absent.
WHALE_CHANNELS = list(WHALE_TELEGRAM_CHANNELS or ["Whale Liquidations"])
_ALLOWED_SYMBOLS = {
    asset.split("-")[0].replace("/", "").upper()
    for asset in (WHALE_ALLOWED_ASSETS or [])
    if str(asset or "").strip()
}
if not _ALLOWED_SYMBOLS:
    _ALLOWED_SYMBOLS = {"BTC", "ETH", "BNB", "SOL", "XRP"}

# Minimum USD value to consider a whale alert
MIN_VALUE_USD = float(WHALE_TELEGRAM_MIN_VALUE_USD or 1_000_000)

# Regex patterns to extract transaction data from messages
_VALUE_PATTERNS = [
    re.compile(r'\$\s*([\d,]+(?:\.\d+)?)\s*([MBK])', re.IGNORECASE),
    re.compile(r'([\d,]+(?:\.\d+)?)\s*([MBK])\s*USD', re.IGNORECASE),
    re.compile(r'USD\s*([\d,]+(?:\.\d+)?)', re.IGNORECASE),
    re.compile(r'([\d,]+(?:\.\d+)?)\s*(?:million|billion)', re.IGNORECASE),
    re.compile(r'\(\s*\$\s*([\d,]+(?:\.\d+)?)\s*\)', re.IGNORECASE),
    re.compile(r'\$\s*([\d,]+(?:\.\d+)?)(?!\s*[MBK])\b', re.IGNORECASE),
]

_SYMBOL_PATTERN = re.compile(
    r'\b(BTC|ETH|BNB|XRP|ADA|SOL|DOGE|DOT|AVAX|LINK|LTC|'
    r'MATIC|UNI|ATOM|XLM|TRX|ALGO|VET|FIL|THETA)\b',
    re.IGNORECASE,
)
_LIQUIDATION_EVENT_PATTERN = re.compile(
    r'#?(?P<symbol>[A-Z0-9]{2,10})\s+Liquidated\s+\$?(?P<value>[\d,]+(?:\.\d+)?)\s*(?P<suffix>[KMB])?\s+in\s+(?P<side>Long|Short)\b',
    re.IGNORECASE,
)
_LIQUIDATION_TOTAL_PATTERN = re.compile(
    r'24h\s+Liquidation\s+for\s+\$?#?(?P<symbol>[A-Z0-9]{2,10})\s*:\s*\$?(?P<value>[\d,]+(?:\.\d+)?)\s*(?P<suffix>[KMB])?',
    re.IGNORECASE,
)


# ── Whale text scorer — financial keyword matching, no external deps ─────────
_WHALE_BEARISH = {
    "dump", "dumped", "dumping", "sell", "selling", "sold", "distribution",
    "distributing", "outflow", "withdrawal", "withdrew", "exit", "exiting",
    "crash", "crashing", "fear", "panic", "warning", "alert", "suspect",
    "hack", "hacked", "stolen", "fraud", "scam", "liquidation", "liquidated",
    "exchange", "moved to exchange", "sent to exchange", "bearish",
}
_WHALE_BULLISH = {
    "buy", "buying", "bought", "accumulation", "accumulating", "inflow",
    "deposit", "deposited", "holding", "hodl", "transfer from exchange",
    "from exchange", "cold wallet", "cold storage", "bullish", "long",
    "institutional", "treasury", "reserve", "staking", "locked",
}

def _score_whale_text(text: str) -> float:
    """
    Score a whale alert message using financial keywords.
    Returns -1.0 (sell pressure) to +1.0 (buy pressure).
    Neutral large transfers default to slightly positive (accumulation bias).
    """
    if not text:
        return 0.1
    words   = set(text.lower().split())
    words   = {w.strip(".,!?;:") for w in words}
    bearish = len(words & _WHALE_BEARISH)
    bullish = len(words & _WHALE_BULLISH)
    total   = bearish + bullish
    if total == 0:
        return 0.1   # unknown transfer — slight accumulation bias
    raw = (bullish - bearish) / total
    return round(max(-1.0, min(1.0, raw)), 3)


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


def _ping_health(source: str = "whale") -> None:
    try:
        from monitoring.system_health_service import monitor

        monitor.ping_source(source)
    except Exception:
        pass


def _scale_value(amount_text: str, suffix: str = "") -> float:
    try:
        amount = float(str(amount_text or "0").replace(",", ""))
    except (TypeError, ValueError):
        return 0.0
    factor = {"K": 1_000, "M": 1_000_000, "B": 1_000_000_000}.get(str(suffix or "").upper(), 1.0)
    return amount * factor


def _parse_symbol(text: str) -> str:
    """Extract crypto symbol from message text."""
    match = _SYMBOL_PATTERN.search(text)
    return match.group(0).upper() if match else "UNKNOWN"


def _parse_liquidation_alert(text: str) -> Optional[Dict]:
    event_match = _LIQUIDATION_EVENT_PATTERN.search(text or "")
    total_match = _LIQUIDATION_TOTAL_PATTERN.search(text or "")
    if not event_match and not total_match:
        return None

    symbol = (
        str((event_match.group("symbol") if event_match else total_match.group("symbol")) or "")
        .upper()
        .strip("#$")
    )
    if not symbol or symbol not in _ALLOWED_SYMBOLS:
        return None

    event_value = _scale_value(
        event_match.group("value") if event_match else "0",
        event_match.group("suffix") if event_match else "",
    )
    total_24h_value = _scale_value(
        total_match.group("value") if total_match else "0",
        total_match.group("suffix") if total_match else "",
    )
    resolved_value = max(event_value, total_24h_value)
    if resolved_value < MIN_VALUE_USD:
        return None

    side = str(event_match.group("side") if event_match else "").upper()
    if side == "SHORT":
        direction = "BUY"
        sentiment = 0.45
        pressure = "short-squeeze pressure"
    elif side == "LONG":
        direction = "SELL"
        sentiment = -0.45
        pressure = "long-liquidation pressure"
    else:
        direction = "BUY"
        sentiment = 0.1
        pressure = "liquidation pressure"

    if total_24h_value >= 10_000_000:
        sentiment = 0.60 if direction == "BUY" else -0.60
    elif total_24h_value >= 3_000_000:
        sentiment = 0.52 if direction == "BUY" else -0.52

    return {
        "symbol": symbol,
        "value_usd": resolved_value,
        "direction": direction,
        "sentiment": round(sentiment, 3),
        "title": f"🐋 {symbol} {pressure} ${resolved_value / 1_000_000:.1f}M",
        "event_kind": "liquidation",
        "liquidation_side": side or "",
        "event_value_usd": round(event_value, 2),
        "total_24h_value_usd": round(total_24h_value, 2),
    }


def _parse_alert(text: str, source: str, date: datetime) -> Optional[Dict]:
    """
    Parse a raw Telegram message into a whale alert dict.
    Returns None if the message is not a qualifying whale alert.
    """
    liquidation = _parse_liquidation_alert(text)
    if liquidation:
        symbol = liquidation["symbol"]
        value_usd = float(liquidation["value_usd"] or 0.0)
        return {
            "title": f"{liquidation['title']} — {source}",
            "value_usd": value_usd,
            "symbol": symbol,
            "date": date.isoformat(),
            "source": f"Telegram/{source}",
            "sentiment": float(liquidation["sentiment"] or 0.0),
            "direction": liquidation["direction"],
            "event_kind": liquidation["event_kind"],
            "liquidation_side": liquidation["liquidation_side"],
            "event_value_usd": liquidation["event_value_usd"],
            "total_24h_value_usd": liquidation["total_24h_value_usd"],
            "raw_text": text[:200],
            "external_id": f"telegram:{source}:{date.isoformat()}:{symbol}:{int(value_usd)}",
        }

    lower = text.lower()
    if not any(kw in lower for kw in ['whale', 'transfer', 'moved', 'million', 'billion', '$']):
        return None

    value_usd = _parse_value_usd(text)
    if value_usd < MIN_VALUE_USD:
        return None

    symbol  = _parse_symbol(text)
    if symbol not in _ALLOWED_SYMBOLS:
        return None
    value_m = value_usd / 1_000_000
    title   = f"🐋 {symbol} ${value_m:.1f}M — {source}"

    # Score the raw message text through the financial keyword scorer
    sentiment = _score_whale_text(text)

    return {
        "title":     title,
        "value_usd": value_usd,
        "symbol":    symbol,
        "date":      date.isoformat(),
        "source":    f"Telegram/{source}",
        "sentiment": sentiment,
        "direction": "BUY" if sentiment >= 0 else "SELL",
        "event_kind": "whale",
        "raw_text":  text[:200],
        "external_id": f"telegram:{source}:{date.isoformat()}:{symbol}:{int(value_usd)}",
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

    def _mark_healthy(self) -> None:
        _ping_health("whale")

    async def _resolve_watch_entities(self, client) -> List:
        resolved = []
        dialog_lookup: Dict[str, object] = {}
        try:
            async for dialog in client.iter_dialogs():
                entity = getattr(dialog, "entity", None)
                if entity is None:
                    continue
                title = str(getattr(dialog, "name", "") or "").strip()
                if title:
                    dialog_lookup[title.casefold()] = entity
                username = str(getattr(entity, "username", "") or "").strip().lstrip("@")
                if username:
                    dialog_lookup[username.casefold()] = entity
        except Exception as e:
            logger.debug(f"[TelegramWhaleWatcher] Dialog discovery failed: {e}")

        for channel in WHALE_CHANNELS:
            spec = str(channel or "").strip()
            if not spec:
                continue
            entity = None
            try:
                entity = await client.get_entity(spec)
            except Exception:
                entity = dialog_lookup.get(spec.casefold()) or dialog_lookup.get(spec.lstrip("@").casefold())
            if entity is not None:
                resolved.append(entity)
            else:
                logger.warning(f"[TelegramWhaleWatcher] Could not resolve configured channel '{spec}'")
        return resolved

    async def _connect_client(self):
        try:
            from telethon import TelegramClient
            from telethon.errors import SessionPasswordNeededError
        except ImportError:
            logger.error(
                "[TelegramWhaleWatcher] telethon not installed — "
                "run: pip install telethon"
            )
            return None

        client = TelegramClient(self._session, self._api_id, self._api_hash)
        self._client = client

        try:
            await client.start(phone=self._phone)
            logger.info("[TelegramWhaleWatcher] Telethon connected")
            self._mark_healthy()
        except SessionPasswordNeededError:
            logger.error(
                "[TelegramWhaleWatcher] 2FA enabled — "
                "run telethon interactively once to create the session file"
            )
            return None
        except Exception as e:
            logger.error(f"[TelegramWhaleWatcher] Connect failed: {e}")
            return None

        return client

    async def _fetch_recent_history(self, client, resolved_channels: List) -> None:
        for entity in resolved_channels:
            if not self._is_running:
                break
            try:
                async for msg in client.iter_messages(entity, limit=50):
                    if msg.text:
                        channel_name = str(getattr(entity, "title", "") or getattr(entity, "username", "") or "telegram")
                        alert = _parse_alert(msg.text, channel_name, msg.date or datetime.utcnow())
                        if alert:
                            self._add_alert(alert)
                logger.debug(
                    f"[TelegramWhaleWatcher] Fetched history from "
                    f"{getattr(entity, 'title', getattr(entity, 'username', entity))}"
                )
            except Exception as e:
                logger.debug(
                    f"[TelegramWhaleWatcher] Could not read "
                    f"{getattr(entity, 'title', getattr(entity, 'username', entity))}: {e}"
                )

    async def _handle_new_message(self, event) -> None:
        if not event.text:
            return
        channel_name = str(
            getattr(event.chat, "title", "")
            or getattr(event.chat, "username", "")
            or event.chat_id
        )
        alert = _parse_alert(event.text, channel_name, datetime.utcnow())
        if alert:
            self._add_alert(alert)
            logger.info(
                f"[TelegramWhaleWatcher] NEW alert: "
                f"{alert['symbol']} ${alert['value_usd']/1_000_000:.1f}M"
            )
            if self.on_alert:
                try:
                    self.on_alert(alert)
                except Exception:
                    pass

    async def _register_live_handler(self, client, resolved_channels: List) -> None:
        from telethon import events

        client.add_event_handler(self._handle_new_message, events.NewMessage(chats=resolved_channels))
        logger.info(
            f"[TelegramWhaleWatcher] Listening to {len(resolved_channels)} channels"
        )

    async def _keepalive_loop(self) -> None:
        while self._is_running:
            self._mark_healthy()
            await asyncio.sleep(5)

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
        client = await self._connect_client()
        if client is None:
            return

        resolved_channels = await self._resolve_watch_entities(client)
        if not resolved_channels:
            logger.error("[TelegramWhaleWatcher] No Telegram whale channels could be resolved")
            return
        self._mark_healthy()

        await self._fetch_recent_history(client, resolved_channels)
        await self._register_live_handler(client, resolved_channels)
        await self._keepalive_loop()
        await client.disconnect()

    def _add_alert(self, alert: Dict) -> None:
        """Thread-safe insert into recent alerts list."""
        inserted = False
        with self._lock:
            # Deduplicate by title
            existing_titles = {a["title"] for a in self._recent_alerts}
            if alert["title"] not in existing_titles:
                self._recent_alerts.insert(0, alert)
                self._recent_alerts = self._recent_alerts[: self._max_alerts]
                inserted = True
        if inserted:
            record_whale_alert_event(
                symbol=alert.get("symbol", ""),
                source=alert.get("source", "Telegram"),
                value_usd=float(alert.get("value_usd", 0.0) or 0.0),
                raw_text=alert.get("raw_text", alert.get("title", "")),
                sentiment=float(alert.get("sentiment", 0.1) or 0.1),
                timestamp=alert.get("date"),
                metadata={
                    "title": alert.get("title", ""),
                    "event_kind": alert.get("event_kind", "whale"),
                    "liquidation_side": alert.get("liquidation_side", ""),
                    "event_value_usd": alert.get("event_value_usd", 0.0),
                    "total_24h_value_usd": alert.get("total_24h_value_usd", 0.0),
                },
                external_id=str(alert.get("external_id", "")),
            )
