"""
Unified Whale Alert Manager
Combines Twitter, Telegram, Free API, and REDDIT whale alerts
NOW WITH: Proper logging and Twitter fixes
"""

from twitter_whale_watcher import TwitterWhaleWatcher
from telegram_whale_watcher import TelegramWhaleWatcher
from reddit_watcher import RedditWatcher
import threading
import time
import requests
import re
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from models.trade_models import WhaleAlert
from config.database import SessionLocal
from logger import logger
try:
    from telethon_whale_store import whale_store as _whale_store
except Exception:
    _whale_store = None

class FreeWhaleAPI:
    """Free whale-alert.io API - NO KEY NEEDED"""
    
    def __init__(self):
        self.base_url = "https://api.whale-alert.io/v1"
        self.session = requests.Session()
        self.cache = []
        self.last_fetch = 0
        self.cache_ttl = 300  # 5 minutes cache
    
    def fetch_transactions(self, min_value: int = 1000000) -> List[Dict]:
        """Fetch whale transactions from free API"""
        try:
            # Check cache first
            if time.time() - self.last_fetch < self.cache_ttl:
                logger.debug("Using cached Free API data")
                return self.cache
            
            url = f"{self.base_url}/transactions"
            params = {
                "api_key": "free-tier",
                "min_value": min_value,
                "limit": 25
            }
            
            response = self.session.get(url, params=params, timeout=8)
            # Guard: empty or HTML response would crash .json()
            if not response.content or not response.text.strip().startswith('{'):
                logger.debug(f"Free API returned non-JSON (status {response.status_code}) — skipping")
                return self.cache
            data = response.json()
            
            alerts = []
            if 'transactions' in data:
                for tx in data['transactions']:
                    value_m = tx['amount_usd'] / 1_000_000
                    if value_m >= 1.0:
                        alerts.append({
                            'title': f"🐋 {tx['amount']:.2f} {tx['symbol']} (${value_m:.1f}M)",
                            'value_usd': tx['amount_usd'],
                            'symbol': tx['symbol'],
                            'amount': tx['amount'],
                            'alert_time': datetime.fromtimestamp(tx['timestamp']),
                            'source': 'Whale-Alert.io (Free)',
                            'url': tx.get('url', ''),
                            'sentiment': 0.15 if value_m > 10 else 0.1
                        })
            
            self.cache = alerts
            self.last_fetch = time.time()
            logger.info(f"🌐 Free API: Fetched {len(alerts)} alerts")
            return alerts
            
        except Exception as e:
            logger.error(f"Free API error: {e}")
            return self.cache
    
    def fetch_by_symbol(self, symbol: str, min_value: int = 1000000) -> List[Dict]:
        all_alerts = self.fetch_transactions(min_value)
        return [a for a in all_alerts if a['symbol'] == symbol]


class WhaleAlertDB:
    """Database handler for whale alerts"""
    
    def __init__(self):
        self.session = SessionLocal() if SessionLocal else None
        self.enabled = self.session is not None
        if self.enabled:
            logger.info("💾 Database: Connected")
        else:
            logger.warning("💾 Database: Not connected")
    
    def save_alert(self, alert_data: Dict) -> bool:
        if not self.enabled:
            return False
        
        try:
            exists = self.session.query(WhaleAlert).filter(
                WhaleAlert.title == alert_data['title'],
                WhaleAlert.alert_time == alert_data['alert_time']
            ).first()
            
            if not exists:
                alert = WhaleAlert(
                    title=alert_data['title'],
                    symbol=alert_data['symbol'],
                    value_usd=alert_data['value_usd'],
                    source=alert_data['source'],
                    alert_time=alert_data['alert_time']
                )
                self.session.add(alert)
                self.session.commit()
                logger.debug(f"💾 Saved: {alert_data['symbol']} ${alert_data['value_usd']/1_000_000:.1f}M")
                return True
        except Exception as e:
            logger.error(f"DB save error: {e}")
            self.session.rollback()
        
        return False
    
    def save_alerts(self, alerts: List[Dict]) -> int:
        saved = 0
        for alert in alerts:
            if self.save_alert(alert):
                saved += 1
        if saved > 0:
            logger.info(f"💾 Saved {saved} new alerts to database")
        return saved
    
    def get_alerts(self, hours: int = 24, min_value: int = 1000000) -> List[Dict]:
        if not self.enabled:
            return []
        
        try:
            cutoff = datetime.now() - timedelta(hours=hours)
            alerts = self.session.query(WhaleAlert).filter(
                WhaleAlert.alert_time >= cutoff,
                WhaleAlert.value_usd >= min_value
            ).order_by(WhaleAlert.value_usd.desc()).limit(100).all()
            
            logger.debug(f"📊 Retrieved {len(alerts)} alerts from DB")
            return [{
                'title': a.title,
                'value_usd': float(a.value_usd),
                'symbol': a.symbol,
                'source': a.source,
                'alert_time': a.alert_time.isoformat(),
                'value_millions': float(a.value_usd) / 1_000_000
            } for a in alerts]
        except Exception as e:
            logger.error(f"DB get error: {e}")
            return []
    
    def close(self):
        if self.session:
            self.session.close()
            logger.info("💾 Database closed")


class WhaleAlertManager:
    """
    Manages whale alerts from multiple sources
    INCLUDES: Twitter + Telegram + Free API + REDDIT + Database
    """
    
    def __init__(self):
        """Initialize all watchers"""
        logger.info("🐋 Initializing WhaleAlertManager...")
        
        self.twitter_watcher = TwitterWhaleWatcher()
        self.telegram_watcher = TelegramWhaleWatcher()
        self.free_api = FreeWhaleAPI()
        self.reddit = RedditWatcher()
        self.db = WhaleAlertDB()
        self.all_alerts = []
        self.max_alerts = 100
        self.collecting = False
        
        # Determine Twitter status
        twitter_status = '❌ DISABLED'
        if hasattr(self.twitter_watcher, 'active_method') and self.twitter_watcher.active_method:
            twitter_status = f'✅ ACTIVE ({self.twitter_watcher.active_method})'
        elif hasattr(self.twitter_watcher, 'client') and self.twitter_watcher.client:
            twitter_status = '✅ ACTIVE (legacy)'
        
        logger.info("="*60)
        logger.info("🐋 WHALE ALERT MANAGER - QUAD SOURCE + DATABASE")
        logger.info("="*60)
        logger.info(f"📱 Telegram: {'✅ ACTIVE' if self.telegram_watcher.bot_token else '❌ DISABLED'}")
        logger.info(f"🐦 Twitter:   {twitter_status}")
        logger.info(f"🌐 Free API:  ✅ ALWAYS ACTIVE")
        logger.info(f"📱 Reddit:    {'✅ ACTIVE' if self.reddit.enabled else '❌ DISABLED'}")
        logger.info(f"💾 Database:  {'✅ CONNECTED' if self.db.enabled else '❌ NOT CONNECTED'}")
        logger.info("="*60)
    
    def start_monitoring(self):
        """Start all watchers"""
        logger.info("▶️ Starting all watchers...")
        
        # Start Twitter if available
        twitter_active = False
        if hasattr(self.twitter_watcher, 'active_method') and self.twitter_watcher.active_method:
            self.twitter_watcher.start_monitoring()
            twitter_active = True
        elif hasattr(self.twitter_watcher, 'client') and self.twitter_watcher.client:
            self.twitter_watcher.start_monitoring()
            twitter_active = True
        
        if twitter_active:
            logger.info("🐦 Twitter monitor started")
        
        # Start Telegram if available
        if self.telegram_watcher.bot_token:
            self.telegram_watcher.start_monitoring()
            logger.info("📱 Telegram monitor started")
        
        # Start Reddit if available
        if self.reddit.enabled:
            self.reddit.start_monitoring()
            logger.info("📱 Reddit monitor started")
        
        # Start collector thread
        self.collecting = True
        collector = threading.Thread(target=self._collect_alerts, daemon=True)
        collector.start()
        logger.info("🐋 Collector thread started")
    
    def _collect_alerts(self):
        """Collect alerts from ALL sources and save to database"""
        logger.info("🐋 Collector running (checking every 60s)")
        
        while self.collecting:
            try:
                all_new = []
                
                # 1. Get from Twitter
                twitter_active = False
                if hasattr(self.twitter_watcher, 'active_method') and self.twitter_watcher.active_method:
                    twitter_active = True
                elif hasattr(self.twitter_watcher, 'client') and self.twitter_watcher.client:
                    twitter_active = True
                
                if twitter_active:
                    try:
                        twitter_alerts = self.twitter_watcher.get_recent_alerts()
                        for a in twitter_alerts:
                            if 'whale_info' in a:
                                info = a['whale_info']
                                all_new.append({
                                    'title': f"🐋 {info['amount']} {info['symbol']} (${info['value_usd']/1_000_000:.1f}M)",
                                    'value_usd': info['value_usd'],
                                    'symbol': info['symbol'],
                                    'alert_time': a.get('created_at', datetime.now()),
                                    'source': f"Twitter @{a['account']}",
                                    'sentiment': 0.15 if info['value_usd'] > 10_000_000 else 0.1
                                })
                        if twitter_alerts:
                            logger.info(f"🐦 Twitter: {len(twitter_alerts)} alerts")
                    except Exception as e:
                        logger.error(f"Twitter collect error: {e}")
                
                # 2. Get from Telegram
                if self.telegram_watcher.bot_token:
                    try:
                        telegram_alerts = self.telegram_watcher.get_recent_alerts()
                        for a in telegram_alerts:
                            all_new.append({
                                'title': a['title'],
                                'value_usd': a['value_usd'],
                                'symbol': a['symbol'],
                                'alert_time': datetime.fromisoformat(a['date']) if isinstance(a['date'], str) else a['date'],
                                'source': a['source'],
                                'sentiment': a.get('sentiment', 0.1)
                            })
                        if telegram_alerts:
                            logger.info(f"📱 Telegram: {len(telegram_alerts)} alerts")
                    except Exception as e:
                        logger.error(f"Telegram collect error: {e}")
                
                # 3. Get from Free API
                try:
                    free_alerts = self.free_api.fetch_transactions()
                    for a in free_alerts:
                        all_new.append({
                            'title': a['title'],
                            'value_usd': a['value_usd'],
                            'symbol': a['symbol'],
                            'alert_time': a['alert_time'],
                            'source': a['source'],
                            'sentiment': a['sentiment']
                        })
                    if free_alerts:
                        logger.info(f"🌐 Free API: {len(free_alerts)} alerts")
                except Exception as e:
                    logger.error(f"Free API collect error: {e}")
                
                # 4. Get from Reddit
                if self.reddit.enabled:
                    try:
                        reddit_alerts = self.reddit.get_whale_alerts()
                        for a in reddit_alerts:
                            all_new.append({
                                'title': a['title'],
                                'value_usd': a['value_usd'],
                                'symbol': a['symbol'],
                                'alert_time': a['created'],
                                'source': a['source'],
                                'sentiment': 0.1
                            })
                        if reddit_alerts:
                            logger.info(f"📱 Reddit: {len(reddit_alerts)} whale mentions")
                    except Exception as e:
                        logger.error(f"Reddit collect error: {e}")
                
                if all_new:
                    # Remove duplicates
                    seen = set()
                    unique = []
                    for alert in all_new:
                        if alert['title'] not in seen:
                            seen.add(alert['title'])
                            unique.append(alert)
                    
                    # Sort by value
                    unique.sort(key=lambda x: x['value_usd'], reverse=True)
                    
                    # Save to database
                    if self.db.enabled:
                        saved = self.db.save_alerts(unique)
                        if saved > 0:
                            logger.info(f"💾 Saved {saved} new alerts")
                    
                    # Update memory cache
                    self.all_alerts = (unique + self.all_alerts)[:self.max_alerts]
                    logger.debug(f"🐋 Total in memory: {len(self.all_alerts)}")
                
            except Exception as e:
                logger.error(f"Collector error: {e}")
            
            time.sleep(60)
    
    def get_alerts(self, min_value_usd: float = 1000000, hours: int = 24) -> List[Dict]:
        """Get alerts — merges Telethon live feed + DB + in-memory sources."""
        results = []

        # 1. Telethon whale_store (primary — real-time, no token needed)
        try:
            if _whale_store is not None and len(_whale_store) > 0:
                tele_alerts = _whale_store.format_for_dashboard(hours=hours)
                tele_filtered = [a for a in tele_alerts if a.get('value_usd', 0) >= min_value_usd]
                results.extend(tele_filtered)
                if tele_filtered:
                    logger.debug(f"Telethon whale store: {len(tele_filtered)} alerts")
        except Exception as _e:
            logger.debug(f"whale_store merge: {_e}")

        # 2. DB / legacy sources (fallback / supplement)
        try:
            if self.db.enabled:
                db_alerts = self.db.get_alerts(hours=hours, min_value=min_value_usd)
                results.extend(db_alerts)
        except Exception:
            pass

        # 3. In-memory (twitter, reddit etc.)
        try:
            cutoff = datetime.now() - timedelta(hours=hours)
            filtered = [
                a for a in self.all_alerts
                if a.get('value_usd', 0) >= min_value_usd
                and a.get('alert_time', datetime.now()) > cutoff
            ]
            results.extend(filtered)
        except Exception:
            pass

        # Deduplicate by title+time, sort by value
        seen = set()
        deduped = []
        for a in results:
            key = (a.get('title',''), str(a.get('alert_time','')))
            if key not in seen:
                seen.add(key)
                deduped.append(a)

        deduped.sort(key=lambda x: x.get('value_usd', x.get('value', 0)), reverse=True)
        return deduped[:self.max_alerts]
    
    def get_alerts_for_symbol(self, symbol: str, min_value_usd: float = 1000000, days: int = 7) -> List[Dict]:
        all_alerts = self.get_alerts(min_value_usd, hours=days*24)
        return [a for a in all_alerts if a.get('symbol') == symbol]
    
    def get_top_alerts(self, limit: int = 10, days: int = 7) -> List[Dict]:
        alerts = self.get_alerts(hours=days*24)
        return alerts[:limit]
    
    def get_summary(self) -> Dict:
        alerts = self.get_alerts(hours=24)
        
        if not alerts:
            logger.debug("No alerts in last 24h")
            return {'total_alerts': 0}
        
        sources = {}
        symbols = {}
        total_value = 0
        
        for alert in alerts:
            source = alert.get('source', 'Unknown')
            sources[source] = sources.get(source, 0) + 1
            symbol = alert.get('symbol', 'Unknown')
            symbols[symbol] = symbols.get(symbol, 0) + 1
            total_value += alert.get('value_usd', 0)
        
        summary = {
            'total_alerts': len(alerts),
            'total_value_millions': round(total_value / 1_000_000, 1),
            'by_source': sources,
            'by_symbol': dict(list(symbols.items())[:5]),
            'largest_alert': alerts[0] if alerts else None,
            'database_active': self.db.enabled
        }
        
        logger.info(f"📊 Summary: {len(alerts)} alerts, ${summary['total_value_millions']}M total")
        return summary
    
    def stop(self):
        """Stop all monitoring"""
        logger.info("🛑 Stopping all watchers...")
        self.collecting = False
        
        if hasattr(self.twitter_watcher, 'stop_monitoring'):
            self.twitter_watcher.stop_monitoring()
        if self.telegram_watcher:
            self.telegram_watcher.stop_monitoring()
        if self.reddit.enabled:
            self.reddit.stop_monitoring()
        if hasattr(self, 'db'):
            self.db.close()
        
        logger.info("🐋 Whale alert monitoring stopped")


# ===== SIMPLE TEST =====
if __name__ == "__main__":
    logger.info("\n🐋 TESTING WHALE ALERT MANAGER")
    logger.info("="*60)
    
    manager = WhaleAlertManager()
    
    logger.info("\n📡 Fetching alerts from database...")
    alerts = manager.get_alerts(min_value_usd=1000000, hours=168)
    
    if alerts:
        logger.info(f"✅ Found {len(alerts)} alerts")
        logger.info("\n🐋 Top 5:")
        for i, alert in enumerate(alerts[:5], 1):
            value_m = alert['value_usd'] / 1_000_000
            logger.info(f"{i}. {alert['title']} from {alert.get('source', 'Unknown')}")
    else:
        logger.info("📭 No alerts yet - collector will add them")
    
    logger.info("\n📊 Summary:")
    summary = manager.get_summary()
    for key, value in summary.items():
        if key != 'largest_alert':
            logger.info(f"   • {key}: {value}")
    
    logger.info("="*60)