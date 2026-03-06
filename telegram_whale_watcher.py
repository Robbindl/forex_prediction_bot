"""
Telegram Whale Alert Watcher
Monitors Telegram channels for whale alerts
"""

import requests
import re
import time
import threading
from datetime import datetime
from typing import List, Dict, Optional
import os
from dotenv import load_dotenv

load_dotenv()

class TelegramWhaleWatcher:
    """
    Tracks Telegram channels that post whale alerts
    """
    
    def __init__(self):
        """
        Initialize with Telegram bot token from .env
        """
        self.bot_token = os.getenv('TELEGRAM_TOKEN')
        self.chat_id = os.getenv('TELEGRAM_CHAT_ID')
        
        self.base_url = f"https://api.telegram.org/bot{self.bot_token}"
        self.recent_messages = []
        self.max_messages = 100
        self.is_running = False
        self.last_update_id = 0  # Track last update to avoid duplicates
        
        # Public channels to monitor (you need to join these)
        self.public_channels = [
            'whale_alert',           # Official Whale Alert
            'Whalebotalerts',         # Whalebotalerts
            'WhaleSniper',            # WhaleSniper
            'lookonchain',            # lookonchain
            'CryptoQuantAlerts',      # CryptoQuantAlerts
            'WhaleBotRektd',          # WhaleBotRektd
            'WhaleWire',              # WhaleWire Telegram
        ]
        
        # Your personal chat (for testing)
        self.your_chat_id = self.chat_id
        
        if self.bot_token:
            print(f"✅ Telegram bot initialized")
        else:
            print(f"⚠️ Telegram bot token not found in .env")
    
    def get_updates(self, limit: int = 100) -> List[Dict]:
        """
        Get recent updates from Telegram
        """
        if not self.bot_token:
            return []
        
        try:
            url = f"{self.base_url}/getUpdates"
            params = {
                'limit': limit,
                'timeout': 5,
                'offset': self.last_update_id + 1,  # Only get new updates
                'allowed_updates': ['message', 'channel_post']
            }
            
            response = requests.get(url, params=params, timeout=10)
            data = response.json()
            
            if data.get('ok'):
                updates = data.get('result', [])
                if updates:
                    # Update the last update ID
                    self.last_update_id = updates[-1]['update_id']
                return updates
            else:
                print(f"⚠️ Telegram API error: {data.get('description')}")
                
        except Exception as e:
            print(f"⚠️ Telegram getUpdates error: {e}")
        
        return []
    
    def extract_whale_info(self, text: str) -> Optional[Dict]:
        """
        Extract whale transaction info from message text
        """
        patterns = [
            r'(\d+[,]?\d*\.?\d*)\s*(BTC|ETH|BNB|SOL|XRP|ADA|DOGE)\s*\(?\$?(\d+[,]?\d*\.?\d*)[\s\)]*(million|M|k|K)?',
            r'(\d+\.?\d*)\s*(Bitcoin|Ethereum).*?(\d+\.?\d*)\s*(million|M)',
            r'(\d+[,]?\d*\.?\d*)\s*(#BTC|#ETH).*?\$(\d+[,]?\d*\.?\d*)',
            r'(\d+[kKmM]?)\s*(BTC|ETH).*?(\d+[kKmM]?)',
            r'Whale Alert.*?(\d+[,]?\d*)\s*(BTC|ETH).*?\$(\d+[.,]?\d*)[mM]',  # Whale Alert format
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                try:
                    # Handle different formats
                    amount = float(re.sub(r'[^\d.]', '', match.group(1)))
                    symbol = match.group(2).replace('#', '').upper()
                    
                    # Determine value
                    if len(match.groups()) >= 4 and match.group(4):
                        unit = match.group(4).lower() if match.group(4) else ''
                        if 'million' in unit or 'm' in unit:
                            value_usd = amount * 1_000_000
                        elif 'k' in unit:
                            value_usd = amount * 1_000
                        else:
                            value_usd = float(re.sub(r'[^\d.]', '', match.group(3))) if match.group(3) else amount * 50_000
                    else:
                        # Try to extract dollar value from third group
                        value_str = match.group(3) if len(match.groups()) >= 3 else ''
                        if 'm' in value_str.lower():
                            value_usd = float(re.sub(r'[^\d.]', '', value_str)) * 1_000_000
                        elif value_str:
                            value_usd = float(re.sub(r'[^\d.]', '', value_str))
                        else:
                            value_usd = amount * 50_000  # Rough estimate
                    
                    # Clean up symbol
                    if '-' in symbol:
                        symbol = symbol.split('-')[0]
                    
                    # Filter out small values
                    if value_usd < 100_000:
                        continue
                    
                    return {
                        'amount': amount,
                        'symbol': symbol,
                        'value_usd': value_usd,
                        'text': text[:100]
                    }
                except Exception as e:
                    continue
        return None
    
    def fetch_channel_messages(self) -> List[Dict]:
        """
        Fetch recent messages from all channels
        """
        updates = self.get_updates(limit=100)
        messages = []
        
        for update in updates:
            # Check for channel posts
            message = update.get('channel_post') or update.get('message')
            if not message:
                continue
            
            msg_text = message.get('text') or message.get('caption', '')
            msg_date = datetime.fromtimestamp(message['date'])
            chat = message.get('chat', {})
            chat_title = chat.get('title', 'Unknown')
            chat_username = chat.get('username', '')
            
            # Only process if it's from a channel we're interested in
            if chat_username and chat_username.lstrip('@') in self.public_channels:
                whale_info = self.extract_whale_info(msg_text)
                if whale_info:
                    messages.append({
                        'id': message['message_id'],
                        'text': msg_text,
                        'date': msg_date,
                        'channel': chat_username,
                        'channel_title': chat_title,
                        'whale_info': whale_info,
                        'source': f'Telegram @{chat_username}'
                    })
        
        return messages
    
    def start_monitoring(self, interval_seconds: int = 300):
        """
        Start background thread to monitor Telegram
        """
        self.is_running = True
        
        def monitor_loop():
            while self.is_running:
                try:
                    messages = self.fetch_channel_messages()
                    if messages:
                        # Add to existing messages, avoiding duplicates
                        existing_ids = {m['id'] for m in self.recent_messages}
                        new_messages = [m for m in messages if m['id'] not in existing_ids]
                        
                        if new_messages:
                            self.recent_messages = (new_messages + self.recent_messages)[:self.max_messages]
                            print(f"📱 Telegram: {len(new_messages)} new whale messages")
                    
                    time.sleep(interval_seconds)
                    
                except Exception as e:
                    print(f"⚠️ Telegram monitor error: {e}")
                    time.sleep(60)
        
        thread = threading.Thread(target=monitor_loop, daemon=True)
        thread.start()
        print(f"📱 Telegram whale monitor started (checking every {interval_seconds}s)")
    
    def stop_monitoring(self):
        """Stop monitoring"""
        self.is_running = False
    
    def get_recent_alerts(self, min_value_usd: float = 1000000) -> List[Dict]:
        """
        Get recent whale alerts, filtered by minimum value
        """
        alerts = []
        for msg in self.recent_messages[:30]:
            whale_info = msg.get('whale_info', {})
            if whale_info.get('value_usd', 0) >= min_value_usd:
                value_millions = whale_info['value_usd'] / 1_000_000
                
                alerts.append({
                    'title': f"📱 {whale_info['amount']} {whale_info['symbol']} (${value_millions:.1f}M)",
                    'value_usd': whale_info['value_usd'],
                    'symbol': whale_info['symbol'],
                    'date': msg['date'].isoformat(),
                    'source': msg['source'],
                    'channel': msg['channel'],
                    'sentiment': 0.1
                })
        return alerts
    
    def get_top_alerts(self, limit: int = 5) -> List[Dict]:
        """Get the largest whale alerts by value"""
        alerts = self.get_recent_alerts(min_value_usd=0)
        sorted_alerts = sorted(alerts, key=lambda x: x['value_usd'], reverse=True)
        return sorted_alerts[:limit]