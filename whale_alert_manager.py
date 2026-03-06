"""
Unified Whale Alert Manager
Combines Twitter and Telegram whale alerts
"""

from twitter_whale_watcher import TwitterWhaleWatcher
from telegram_whale_watcher import TelegramWhaleWatcher
import threading
import time
from typing import List, Dict, Optional

class WhaleAlertManager:
    """
    Manages whale alerts from multiple sources
    """
    
    def __init__(self):
        """Initialize both Twitter and Telegram watchers"""
        self.twitter_watcher = TwitterWhaleWatcher()
        self.telegram_watcher = TelegramWhaleWatcher()
        self.all_alerts = []
        self.max_alerts = 50
        self.collecting = False
        
        print("🐋 Whale Alert Manager initialized")
        print(f"   • Twitter: {'✅' if self.twitter_watcher.client else '❌'}")
        print(f"   • Telegram: {'✅' if self.telegram_watcher.bot_token else '❌'}")
    
    def start_monitoring(self):
        """Start all watchers"""
        if self.twitter_watcher.client:
            self.twitter_watcher.start_monitoring()
        
        if self.telegram_watcher.bot_token:
            self.telegram_watcher.start_monitoring()
        
        # Start collector thread
        self.collecting = True
        collector = threading.Thread(target=self._collect_alerts, daemon=True)
        collector.start()
        print("🐋 Whale alert collection started")
    
    def _collect_alerts(self):
        """Collect alerts from all sources"""
        while self.collecting:
            try:
                all_new = []
                
                if self.twitter_watcher.client:
                    all_new.extend(self.twitter_watcher.get_recent_alerts())
                
                if self.telegram_watcher.bot_token:
                    all_new.extend(self.telegram_watcher.get_recent_alerts())
                
                if all_new:
                    # Sort by date (newest first)
                    all_new.sort(key=lambda x: x.get('date', ''), reverse=True)
                    
                    # Remove duplicates (by title)
                    seen = set()
                    unique = []
                    for alert in all_new:
                        if alert['title'] not in seen:
                            seen.add(alert['title'])
                            unique.append(alert)
                    
                    # Merge with existing alerts, keeping newest
                    self.all_alerts = (unique + self.all_alerts)[:self.max_alerts]
                    print(f"🐋 Total alerts: {len(self.all_alerts)} (Twitter: {len([a for a in self.all_alerts if 'Twitter' in a['source']])}, Telegram: {len([a for a in self.all_alerts if 'Telegram' in a['source']])})")
                
            except Exception as e:
                print(f"⚠️ Alert collector error: {e}")
            
            time.sleep(30)
    
    def get_alerts(self, min_value_usd: float = 1000000) -> List[Dict]:
        """Get all alerts above minimum value"""
        return [
            a for a in self.all_alerts 
            if a.get('value_usd', 0) >= min_value_usd
        ]
    
    def get_top_alerts(self, limit: int = 10) -> List[Dict]:
        """Get top alerts by value"""
        sorted_alerts = sorted(
            self.all_alerts,
            key=lambda x: x.get('value_usd', 0),
            reverse=True
        )
        return sorted_alerts[:limit]
    
    def get_summary(self) -> Dict:
        """Get summary of whale activity"""
        alerts = self.get_alerts()
        twitter_count = len([a for a in alerts if 'Twitter' in a['source']])
        telegram_count = len([a for a in alerts if 'Telegram' in a['source']])
        
        total_value = sum(a.get('value_usd', 0) for a in alerts[:10]) / 1_000_000
        
        return {
            'total_alerts': len(alerts),
            'twitter_alerts': twitter_count,
            'telegram_alerts': telegram_count,
            'top_value_millions': total_value,
            'largest_alert': alerts[0] if alerts else None
        }
    
    def stop(self):
        """Stop all monitoring"""
        self.collecting = False
        if self.twitter_watcher:
            self.twitter_watcher.stop_monitoring()
        if self.telegram_watcher:
            self.telegram_watcher.stop_monitoring()
        print("🐋 Whale alert monitoring stopped")