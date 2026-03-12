"""
WhaleMonitor — whale signal tracking extracted from UltimateTradingSystem.
All methods are properly inside the class. No loose module-level functions.
"""

import asyncio
import os
import re
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from logger import logger
try:
    from telethon_whale_store import whale_store
except Exception:
    whale_store = None


class WhaleMonitor:
    """Monitors on-chain whale activity and Telegram channels for large-move signals."""

    WHALE_CHANNELS = [
        'whale_alert', 'whalebotalerts', 'WhaleSniper', 'lookonchain',
        'cryptoquant_alert', 'WhaleBotRektd', 'WhaleWire', 'whalecointalk'
    ]

    def __init__(self, telegram=None):
        self.telegram = telegram
        self.whale_signals: List[Dict] = []
        self.whale_weights: Dict[str, float] = {
            'BTC': 1.0, 'ETH': 1.0, 'BNB': 1.0, 'SOL': 1.0, 'XRP': 1.0
        }
        self._thread: Optional[threading.Thread] = None
        logger.info("WhaleMonitor initialised")

    def setup_whale_integration(self):
        """Initialize whale monitoring and start background listener."""
        self.start_whale_monitor()
        logger.info("Whale Intelligence: ACTIVE")
        logger.info("  Monitoring %d channels", len(self.WHALE_CHANNELS))
        logger.info("  Whale activity influences position sizing")

    def start_whale_monitor(self):
        """Start Telethon listener in a daemon thread (uses saved session)."""
        if self._thread and self._thread.is_alive():
            logger.debug("Whale monitor already running")
            return

        def run():
            asyncio.run(self._whale_loop())

        self._thread = threading.Thread(target=run, name="whale-monitor", daemon=True)
        self._thread.start()
        logger.info("Whale monitor thread started")

    async def _whale_loop(self):
        """Async Telethon event loop."""
        try:
            from telethon import TelegramClient, events
            from dotenv import load_dotenv
            load_dotenv()
        except ImportError as e:
            logger.warning(f"Whale monitor disabled — missing dependency: {e}")
            return

        # .env is the ONLY source — never hardcode Telegram API credentials as fallbacks
        api_id_str = os.getenv('TELEGRAM_API_ID', '')
        api_hash   = os.getenv('TELEGRAM_API_HASH', '')
        session    = os.getenv('TELEGRAM_SESSION', 'whale_session')
        phone      = os.getenv('TELEGRAM_PHONE', None)

        if not api_id_str or not api_hash:
            logger.error("WhaleMonitor: TELEGRAM_API_ID and TELEGRAM_API_HASH must be set in .env")
            return
        api_id = int(api_id_str)

        client = TelegramClient(session, api_id, api_hash)
        try:
            await client.start()
            logger.info("Whale Monitor: connected via saved session")
        except Exception as e:
            logger.error(f"Whale Monitor: session connect failed: {e}")
            if phone:
                try:
                    await client.start(phone=phone)
                except Exception as e2:
                    logger.error(f"Whale Monitor: phone fallback failed: {e2}")
                    return
            else:
                return

        @client.on(events.NewMessage(chats=self.WHALE_CHANNELS))
        async def handler(event):
            if not event.message or not event.message.text:
                return
            parsed = self._extract_whale_info(event.message.text)
            if parsed:
                amount, symbol, value = parsed
                await self._handle_alert(amount, symbol, value,
                                         event.chat.username or "unknown")

        logger.info("Whale Monitor: listening on %d channels", len(self.WHALE_CHANNELS))
        await client.run_until_disconnected()

    @staticmethod
    def _extract_whale_info(text: str):
        """Return (amount, symbol, value_usd) tuple or None."""
        patterns = [
            r'(\d+[,]?\d*\.?\d*)\s*(BTC|ETH|BNB|SOL|XRP).*?\$(\d+[.,]?\d*)[mM]',
            r'(\d+[kKmM]?)\s*(BTC|ETH).*?(\d+[mM]?)',
            r'(\d+[,]?\d*)\s*(BTC|ETH).*?\$(\d+[.,]?\d*)[mM]',
        ]
        for pattern in patterns:
            m = re.search(pattern, text, re.IGNORECASE)
            if m:
                try:
                    amount    = float(re.sub(r'[^\d.]', '', m.group(1)))
                    symbol    = m.group(2).upper()
                    value_str = m.group(3).lower()
                    value     = float(re.sub(r'[^\d.]', '', value_str))
                    if 'm' in value_str:
                        value *= 1_000_000
                    if value >= 1_000_000:
                        return amount, symbol, value
                except Exception:
                    pass
        return None

    async def _handle_alert(self, amount: float, symbol: str,
                             value: float, channel: str):
        """Async-safe handler — stores alert then notifies Telegram."""
        self.process_whale_alert({
            'amount':  amount,
            'symbol':  symbol,
            'value':   value,
            'channel': channel,
            'bullish': self._is_bullish_whale(channel, symbol),
            'time':    datetime.now(),
        })
        if self.telegram:
            value_m = value / 1_000_000
            msg = f"Whale Alert\n{amount:.2f} {symbol} (${value_m:.1f}M)\nChannel: @{channel}"
            try:
                if hasattr(self.telegram, 'send_whale_alert'):
                    self.telegram.send_whale_alert(amount, symbol, value_m, channel)
                elif hasattr(self.telegram, 'send_message'):
                    self.telegram.send_message(msg)
            except Exception as e:
                logger.debug(f"Telegram whale notify failed: {e}")

    def process_whale_alert(self, alert: dict):
        """
        Store a parsed whale alert dict.
        Keys: symbol, amount, value, channel, bullish, time.
        """
        alert.setdefault('time', datetime.now())
        alert.setdefault('bullish', True)
        self.whale_signals.append(alert)
        if len(self.whale_signals) > 500:
            self.whale_signals = self.whale_signals[-500:]
        # ── Push to global store so dashboard + all signal paths see it ──
        if whale_store is not None:
            whale_store.add(dict(alert))
        logger.info(
            "Whale alert: %.2f %s ($%.1fM) @%s | %s",
            alert.get('amount', 0),
            alert.get('symbol', '?'),
            alert.get('value', 0) / 1_000_000,
            alert.get('channel', '?'),
            'BULLISH' if alert.get('bullish') else 'NEUTRAL',
        )

    def _is_bullish_whale(self, channel: str, symbol: str) -> bool:
        ch = channel.lower()
        if any(k in ch for k in ('binance', 'exchange', 'inflow', 'cex')):
            return False
        if any(k in ch for k in ('withdrawal', 'outflow', 'treasury', 'cold')):
            return True
        return True

    def get_whale_sentiment(self, asset: str, hours: int = 24) -> float:
        """Return sentiment score -1 (bearish) to +1 (bullish) for an asset."""
        cutoff = datetime.now() - timedelta(hours=hours)
        recent = [
            s for s in self.whale_signals
            if s.get('symbol') == asset and s.get('time', datetime.min) > cutoff
        ]
        if not recent:
            return 0.0
        total   = sum(s.get('value', 0) for s in recent)
        bullish = sum(s.get('value', 0) for s in recent if s.get('bullish'))
        if total == 0:
            return 0.0
        return round((bullish / total) * 2 - 1, 2)

    def enhance_signal_with_whale(self, signal: Dict, asset: str) -> Dict:
        """Boost/cut signal confidence based on whale sentiment (±20% max)."""
        sentiment = self.get_whale_sentiment(asset)
        if abs(sentiment) > 0.3:
            boost = 1.0 + sentiment * 0.2
            signal['confidence'] = min(signal.get('confidence', 0.5) * boost, 0.95)
            signal['reason'] = signal.get('reason', '') + f" | Whale: {sentiment:+.2f}"
            if 'position_size' in signal:
                signal['position_size'] *= boost
        return signal