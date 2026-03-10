"""
Whale Monitor - Reads Telegram channels and sends alerts to your bot
"""

from telethon import TelegramClient, events
import asyncio
import re
import requests
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# ===== YOUR CREDENTIALS (from .env) =====
api_id = int(os.getenv('TELEGRAM_API_ID'))
api_hash = os.getenv('TELEGRAM_API_HASH')

# Your bot token for sending alerts (from .env)
BOT_TOKEN = os.getenv('TELEGRAM_TOKEN')
YOUR_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

# Whale channels to monitor
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

def send_telegram_alert(message):
    """Send alert to your Telegram bot"""
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    data = {
        'chat_id': YOUR_CHAT_ID,
        'text': message,
        'parse_mode': 'Markdown'
    }
    try:
        requests.post(url, data=data)
    except Exception as e:
        print(f"⚠️ Alert send error: {e}")

def extract_whale_info(text):
    """Extract whale transaction details from message"""
    # Pattern for: 1000 BTC ($65,000,000)
    patterns = [
        r'(\d+[,]?\d*\.?\d*)\s*(BTC|ETH|BNB|SOL|XRP).*?\$(\d+[.,]?\d*)[mM]',
        r'(\d+[kKmM]?)\s*(BTC|ETH).*?(\d+[mM]?)',
        r'(\d+[,]?\d*)\s*(BTC|ETH).*?\$(\d+[.,]?\d*)[mM]',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            try:
                # Extract amount
                amount_str = match.group(1).replace(',', '')
                amount = float(amount_str)
                
                # Extract symbol
                symbol = match.group(2).upper()
                
                # Extract value
                value_str = match.group(3).lower()
                if 'm' in value_str:
                    value = float(value_str.replace('m', '').replace(',', '')) * 1_000_000
                else:
                    value = float(value_str.replace(',', ''))
                
                # Only return if over $1M
                if value >= 1_000_000:
                    return amount, symbol, value
            except:
                pass
    return None

async def main():
    print("="*50)
    print("🐋 WHALE MONITOR STARTING")
    print("="*50)
    print(f"📡 Monitoring {len(WHALE_CHANNELS)} channels:")
    for channel in WHALE_CHANNELS:
        print(f"   • @{channel}")
    print("\n⏳ Connecting to Telegram...")
    
    # Create client
    client = TelegramClient('whale_session', api_id, api_hash)
    
    @client.on(events.NewMessage(chats=WHALE_CHANNELS))
    async def handler(event):
        if not event.message.text:
            return
        
        text = event.message.text
        whale = extract_whale_info(text)
        
        if whale:
            amount, symbol, value = whale
            value_millions = value / 1_000_000
            
            # Create alert message
            alert = (
                f"🐋 *Whale Alert Detected*\n"
                f"━━━━━━━━━━━━━━━\n"
                f"💰 {amount:.2f} {symbol}\n"
                f"💵 ${value_millions:.1f} Million\n"
                f"📢 Channel: @{event.chat.username}\n"
                f"━━━━━━━━━━━━━━━"
            )
            
            print(f"\n✅ ALERT: {amount} {symbol} (${value_millions:.1f}M) from @{event.chat.username}")
            send_telegram_alert(alert)
    
    await client.start()
    print("✅ Connected! Waiting for whale alerts...")
    await client.run_until_disconnected()

if __name__ == "__main__":
    asyncio.run(main())