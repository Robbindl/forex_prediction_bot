"""
WhaleMonitor — whale signal tracking extracted from UltimateTradingSystem.
Receives telegram reference at init; owns whale_signals and whale_weights state.
"""

import time
import threading
import requests
from typing import Dict, List, Optional
from datetime import datetime, timedelta
from logger import logger


class WhaleMonitor:
    """Monitors on-chain whale activity and telegram channels for large move signals."""

    def __init__(self, telegram=None):
        self.telegram = telegram
        self.whale_signals: List[Dict] = []
        self.whale_weights: Dict[str, float] = {}
        logger.info("WhaleMonitor initialised")

    def process_whale_alert(self, alert: dict):
        """Store a whale alert — called externally when a new alert arrives."""
        alert.setdefault('timestamp', datetime.now())
        self.whale_signals.append(alert)
        # Keep last 500 only
        if len(self.whale_signals) > 500:
            self.whale_signals = self.whale_signals[-500:]

def setup_whale_integration(self):
    """Initialize whale monitoring with trading influence"""
    self.whale_signals = []
    self.whale_weights = {
        'BTC': 1.0,
        'ETH': 1.0,
        'BNB': 1.0,
        'SOL': 1.0,
        'XRP': 1.0
    }
    self.start_whale_monitor()
    logger.info("🐋 Whale Intelligence: ACTIVE")
    logger.info("   • Monitoring 8 whale channels")
    logger.info("   • Whale activity influences position sizing")
    logger.info("   • Large inflows = +20% confidence boost")

def start_whale_monitor(self):
    """Start whale monitor in background thread using saved session"""
    import threading
    import asyncio
    from telethon import TelegramClient, events
    import re
    import os
    from dotenv import load_dotenv
    
    load_dotenv()
    
    # Your credentials from .env
    api_id = int(os.getenv('TELEGRAM_API_ID', '32486436'))
    api_hash = os.getenv('TELEGRAM_API_HASH', '3e264a0c1e28644378a9c5236bf251cb')
    session_name = os.getenv('TELEGRAM_SESSION', 'whale_session')
    phone = os.getenv('TELEGRAM_PHONE')  # Optional, but good to have
    
    # Channels to monitor
    WHALE_CHANNELS = [
        'whale_alert',
        'whalebotalerts',
        'WhaleSniper',
        'lookonchain',
        'cryptoquant_alert',
        'WhaleBotRektd',
        'WhaleWire',
        'whalecointalk'
    ]
    
    def extract_whale_info(text):
        """Extract whale transaction details"""
        if not text:
            return None
        
        patterns = [
            r'(\d+[,]?\d*\.?\d*)\s*(BTC|ETH|BNB|SOL|XRP).*?\$(\d+[.,]?\d*)[mM]',
            r'(\d+[kKmM]?)\s*(BTC|ETH).*?(\d+[mM]?)',
            r'(\d+[,]?\d*)\s*(BTC|ETH).*?\$(\d+[.,]?\d*)[mM]',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                try:
                    amount = float(re.sub(r'[^\d.]', '', match.group(1)))
                    symbol = match.group(2).upper()
                    value_str = match.group(3).lower()
                    
                    if 'm' in value_str:
                        value = float(re.sub(r'[^\d.]', '', value_str)) * 1_000_000
                    else:
                        value = float(re.sub(r'[^\d.]', '', value_str))
                    
                    if value >= 1_000_000:
                        return amount, symbol, value
                except:
                    pass
        return None
    
    async def whale_loop():
        # This will use the saved session file - NO PHONE PROMPT!
        client = TelegramClient(session_name, api_id, api_hash)
        
        try:
            # Try to start with saved session first
            await client.start()
            logger.info("🐋 Whale Monitor: Connected using saved session")
        except Exception as e:
            logger.error(f"Failed to connect with saved session: {e}")
            # Fall back to phone if needed (but shouldn't happen if session exists)
            if phone:
                await client.start(phone=phone)
            else:
                raise
        
        @client.on(events.NewMessage(chats=WHALE_CHANNELS))
        async def handler(event):
            if not event.message.text:
                return
            
            whale = extract_whale_info(event.message.text)
            if whale:
                amount, symbol, value = whale
                await self.process_whale_alert(amount, symbol, value, event.chat.username)
        
        logger.info("🐋 Whale Monitor: Connected and listening")
        await client.run_until_disconnected()
    
    def run_whale():
        asyncio.run(whale_loop())
    
    thread = threading.Thread(target=run_whale, daemon=True)
    thread.start()

async def process_whale_alert(self, amount: float, symbol: str, value: float, channel: str):
    """Process whale alert and influence trading decisions"""
    
    value_millions = value / 1_000_000
    
    # Store whale signal
    signal = {
        'time': datetime.now(),
        'symbol': symbol,
        'amount': amount,
        'value': value,
        'channel': channel,
        'bullish': self._is_bullish_whale(channel, symbol)
    }
    
    if not hasattr(self, 'whale_signals'):
        self.whale_signals = []
    
    self.whale_signals.append(signal)
    
    # Keep last 100 signals
    if len(self.whale_signals) > 100:
        self.whale_signals = self.whale_signals[-100:]
    
    # Calculate whale sentiment
    sentiment = self.get_whale_sentiment(symbol)
    
    # Log the alert
    alert_msg = (
        f"🐋 Whale Alert: {amount:.2f} {symbol} (${value_millions:.1f}M)\n"
        f"   • Channel: @{channel}\n"
        f"   • Sentiment: {'BULLISH' if signal['bullish'] else 'NEUTRAL'}\n"
        f"   • Impact: {self.whale_weights.get(symbol, 1.0):.1f}x weight"
    )
    logger.info(alert_msg)
    
    # Send to Telegram if alert bot exists
    if hasattr(self, 'telegram') and self.telegram:
        try:
            if hasattr(self.telegram, 'send_whale_alert'):
                self.telegram.send_whale_alert(amount, symbol, value_millions, channel)
            else:
                # Fallback for commander
                self.telegram.send_message(
                    f"🐋 *Whale Alert*\n"
                    f"{amount:.2f} {symbol} (${value_millions:.1f}M)\n"
                    f"Channel: @{channel}"
                )
        except Exception as e:
            logger.debug(f"Telegram send failed: {e}")

def _is_bullish_whale(self, channel: str, symbol: str) -> bool:
    """Determine if whale movement is bullish"""
    # Exchange inflows = bearish (selling)
    bearish_channels = ['binance', 'exchange', 'inflow', 'cex']
    # Exchange outflows = bullish (buying)
    bullish_channels = ['withdrawal', 'outflow', 'treasury', 'cold']
    
    channel_lower = channel.lower()
    
    if any(b in channel_lower for b in bearish_channels):
        return False
    if any(b in channel_lower for b in bullish_channels):
        return True
    
    # Default: large transfers are neutral
    return True

def get_whale_sentiment(self, asset: str, hours: int = 24) -> float:
    """Get whale sentiment score (-1 to 1) for an asset"""
    if not hasattr(self, 'whale_signals'):
        return 0.0
        
    recent = [s for s in self.whale_signals 
             if s['symbol'] == asset and 
             s['time'] > datetime.now() - timedelta(hours=hours)]
    
    if not recent:
        return 0.0
    
    # Calculate weighted sentiment
    total_value = sum(s['value'] for s in recent)
    bullish_value = sum(s['value'] for s in recent if s['bullish'])
    
    if total_value == 0:
        return 0.0
    
    # Sentiment from -1 (bearish) to 1 (bullish)
    sentiment = (bullish_value / total_value) * 2 - 1
    return round(sentiment, 2)

def enhance_signal_with_whale(self, signal: Dict, asset: str) -> Dict:
    """Enhance trading signal with whale data"""
    sentiment = self.get_whale_sentiment(asset)
    
    # Adjust confidence based on whale sentiment
    if abs(sentiment) > 0.3:
        boost = 1.0 + (sentiment * 0.2)  # Up to 20% boost/cut
        signal['confidence'] = min(signal['confidence'] * boost, 0.95)
        signal['reason'] += f" | Whale sentiment: {sentiment:.2f}"
        
        # Adjust position size
        if 'position_size' in signal:
            signal['position_size'] *= boost
    
    return signal
# ==========================================

# ============= PROFESSIONAL TRADING STRATEGIES =============