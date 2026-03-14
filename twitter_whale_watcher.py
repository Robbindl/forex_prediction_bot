"""
Twitter Whale Alert Watcher
Tracks whale accounts for large crypto transactions
NOW WITH: Official API + Chrome Extension + Open Source Scraper (ALL FREE OPTIONS)
FIXED: Async warnings + Increased rate limits
"""

import tweepy
import re
import requests
import json
import time
import threading
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import os
import getpass
import asyncio  # ADDED for async support
from dotenv import load_dotenv
from utils.logger import logger

# Load environment variables
load_dotenv()


# ============================================================
# METHOD 1: OFFICIAL API (if you have billing)
# ============================================================

class OfficialAPI:
    """Uses official Twitter API (requires billing)"""
    
    def __init__(self):
        self.api_key = os.getenv('TWITTER_API_KEY')
        self.api_secret = os.getenv('TWITTER_API_SECRET')
        self.access_token = os.getenv('TWITTER_ACCESS_TOKEN')
        self.access_secret = os.getenv('TWITTER_ACCESS_SECRET')
        self.client = None
        self.enabled = False
        self._setup()
    
    def _setup(self):
        """Setup official API client"""
        try:
            if all([self.api_key, self.api_secret, self.access_token, self.access_secret]):
                self.client = tweepy.Client(
                    consumer_key=self.api_key,
                    consumer_secret=self.api_secret,
                    access_token=self.access_token,
                    access_token_secret=self.access_secret,
                    wait_on_rate_limit=True
                )
                
                # Test connection
                me = self.client.get_me()
                if me and me.data:
                    logger.info(f"✅ Official API: Logged in as @{me.data.username}")
                    self.enabled = True
                else:
                    logger.warning("⚠️ Official API: Could not verify account")
        except tweepy.errors.Forbidden as e:
            if "453" in str(e):
                logger.warning("⚠️ Official API needs billing ($100/month)")
            else:
                logger.warning(f"⚠️ Official API: {e}")
        except Exception as e:
            logger.debug(f"Official API setup failed: {e}")
    
    def get_user_tweets(self, username: str, count: int = 15) -> List[Dict]:  # INCREASED from 10 to 15
        """Get tweets from a user"""
        if not self.enabled:
            return []
        
        try:
            user = self.client.get_user(username=username, user_auth=True)
            if not user.data:
                return []
            
            tweets = self.client.get_users_tweets(
                id=user.data.id,
                max_results=count,
                tweet_fields=['created_at'],
                exclude=['retweets', 'replies'],
                user_auth=True
            )
            
            results = []
            if tweets and tweets.data:
                for tweet in tweets.data:
                    results.append({
                        'id': tweet.id,
                        'text': tweet.text,
                        'created_at': tweet.created_at,
                        'account': username,
                        'source': f'Official @{username}'
                    })
            return results
        except Exception as e:
            logger.debug(f"Official API error for @{username}: {e}")
            return []


# ============================================================
# METHOD 2: CHROME EXTENSION (100% Free)
# ============================================================
# Install "Twitter Web API" Chrome Extension:
# https://chromewebstore.google.com/detail/twitter-web-api/pnbhkojogdglhidcgnfljnomjdckkfjh
# ============================================================

class ChromeExtensionAPI:
    """Uses the free Chrome Extension as a local API server"""
    
    def __init__(self):
        self.base_url = "http://localhost:3000"
        self.session = None
        self.enabled = False
        self._check_extension()
    
    def _check_extension(self):
        """Check if the Chrome extension is running"""
        try:
            response = requests.get(f"{self.base_url}/health", timeout=2)
            if response.status_code == 200:
                self.enabled = True
                self.session = requests.Session()
                logger.info("✅ Chrome Extension: Connected (http://localhost:3000)")
            else:
                logger.warning("⚠️ Chrome Extension: Not responding")
        except:
            logger.warning("⚠️ Chrome Extension: Not running")
            logger.info("   📥 Install: https://chromewebstore.google.com/detail/twitter-web-api/pnbhkojogdglhidcgnfljnomjdckkfjh")
    
    def get_user_tweets(self, username: str, count: int = 15) -> List[Dict]:  # INCREASED from 10 to 15
        """Get tweets from a user using the extension"""
        if not self.enabled:
            return []
        
        try:
            response = self.session.get(
                f"{self.base_url}/api/user/{username}/tweets",
                params={"count": count},
                timeout=10
            )
            if response.status_code == 200:
                data = response.json()
                tweets = data.get('tweets', [])
                for tweet in tweets:
                    tweet['source'] = f'Chrome @{username}'
                    tweet['account'] = username
                return tweets
        except Exception as e:
            logger.debug(f"Chrome extension error: {e}")
        return []


# ============================================================
# METHOD 3: OPEN SOURCE SCRAPER (Twikit - 100% Free)
# ============================================================
# Install: pip install twikit
# FIXED: Async warnings + Rate limit increases
# ============================================================

class OpenSourceScraper:
    """Uses open-source Twikit library - completely free"""
    
    def __init__(self):
        self.client = None
        self.logged_in = False
        self.cookies_file = "twitter_cookies.json"
        self.enabled = False
        self._init_scraper()
        
        # RATE LIMIT SETTINGS - INCREASED
        self.requests_per_minute = 30  # NEW: Increased from ~1 to 30
        self.request_interval = 60.0 / self.requests_per_minute  # ~2 seconds between requests
        self.request_count = 0
        self.last_reset = time.time()
    
    def _init_scraper(self):
        """Initialize the Twikit client"""
        try:
            global twikit
            import twikit
            from twikit import Client
            self.twikit = twikit
            self.Client = Client
            self.enabled = True
            logger.info("✅ Open Source Scraper: Twikit library loaded")
            self._load_cookies()
        except ImportError:
            logger.warning("⚠️ Twikit not installed. Run: pip install twikit")
    
    def _load_cookies(self):
        """Load saved cookies"""
        try:
            if os.path.exists(self.cookies_file):
                self.client = self.Client(language='en-US')
                self.client.load_cookies(self.cookies_file)
                self.logged_in = True
                logger.info("✅ Loaded saved Twitter cookies")
        except Exception as e:
            logger.debug(f"Could not load cookies: {e}")
    
    def _save_cookies(self):
        """Save cookies after login"""
        try:
            if self.client:
                self.client.save_cookies(self.cookies_file)
                logger.info("✅ Saved Twitter cookies")
        except Exception as e:
            logger.debug(f"Could not save cookies: {e}")
    
    def _wait_for_rate_limit(self):
        """Ensure we don't exceed rate limits - NEW METHOD"""
        now = time.time()
        
        # Reset counter every minute
        if now - self.last_reset >= 60:
            self.request_count = 0
            self.last_reset = now
            logger.debug("Twitter rate limit reset")
        
        # If we've hit the limit, wait until next minute
        if self.request_count >= self.requests_per_minute:
            sleep_time = 60 - (now - self.last_reset)
            if sleep_time > 0:
                logger.debug(f"Rate limit reached, waiting {sleep_time:.1f}s")
                time.sleep(sleep_time)
            self.request_count = 0
            self.last_reset = time.time()
        
        # Increment counter
        self.request_count += 1
        
        # Small delay between requests
        if self.request_interval > 0:
            time.sleep(self.request_interval)
    
    # ===== FIXED: Async login method =====
    def login(self, username: str, email: str, password: str) -> bool:
        """Login to Twitter (sync wrapper for async method)"""
        if not self.enabled:
            return False
        
        try:
            async def do_login():
                self.client = self.Client(language='en-US')
                await self.client.login(
                    auth_info_1=username,
                    auth_info_2=email,
                    password=password
                )
                return True
            
            # Run the async login
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            result = loop.run_until_complete(do_login())
            loop.close()
            
            self.logged_in = True
            self._save_cookies()
            logger.info(f"✅ Logged into Twitter as @{username}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Login failed: {e}")
            return False
    
    # ===== FIXED: Async get_user_tweets method with rate limiting =====
    def get_user_tweets(self, username: str, count: int = 20) -> List[Dict]:  # INCREASED from 10 to 20
        """Get tweets from a user (sync wrapper for async method)"""
        if not self.enabled or not self.logged_in:
            return []
        
        self._wait_for_rate_limit()  # NEW: Apply rate limiting
        
        try:
            async def do_fetch():
                user = await self.client.get_user_by_screen_name(username)
                tweets = await user.get_tweets(count)
                return tweets
            
            # Run the async fetch
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            tweets = loop.run_until_complete(do_fetch())
            loop.close()
            
            results = []
            for tweet in tweets:
                results.append({
                    'id': tweet.id,
                    'text': tweet.text,
                    'created_at': tweet.created_at_datetime,
                    'account': username,
                    'source': f'OpenSource @{username}'
                })
            return results
        except Exception as e:
            logger.debug(f"Twikit error for @{username}: {e}")
            return []


# ============================================================
# MAIN TWITTER WHALE WATCHER (Uses best available method)
# ============================================================

class TwitterWhaleWatcher:
    """
    Tracks Twitter accounts that post whale alerts
    AUTOMATICALLY uses best available method:
    1. Official API (if billing enabled)
    2. Chrome Extension (if running)
    3. Open Source Scraper (if installed and logged in)
    """
    
    def __init__(self, method: str = "auto"):
        """
        Initialize with preferred method:
        - method="auto": Try official, then chrome, then opensource
        - method="official": Use official API only
        - method="chrome": Use Chrome Extension only
        - method="opensource": Use Open Source Scraper only
        """
        self.method = method
        self.active_method = None
        self.active_client = None
        
        # Initialize all methods
        self.official = OfficialAPI()
        self.chrome = ChromeExtensionAPI()
        self.opensource = OpenSourceScraper()
        
        # Common properties
        self.last_request_time = 0
        self.recent_tweets = []
        self.max_tweets = 200  # INCREASED from 100
        self.is_running = False
        self.monitor_thread = None
        
        # Whale accounts to monitor
        self.whale_accounts = [
            'whale_alert',
            'lookonchain',
            'spotonchain',
            'OnchainDataNerd',
            'nansen_ai',
            'ArkhamIntel',
            'CryptoWhale',
            'WhaleWire',
            'santimentfeed',
            'glassnode',
            'intotheblock',
            'KobeissiLetter',
            'WatcherGuru',
            'tier10k',
        ]
        
        # Select best available method
        self._select_method()
    
    def _select_method(self):
        """Select the best available method based on preference"""
        
        if self.method in ['auto', 'official'] and self.official.enabled:
            self.active_method = 'official'
            self.active_client = self.official
            logger.info("✅ Using Official API method")
            return
        
        if self.method in ['auto', 'chrome'] and self.chrome.enabled:
            self.active_method = 'chrome'
            self.active_client = self.chrome
            logger.info("✅ Using Chrome Extension method (FREE)")
            return
        
        if self.method in ['auto', 'opensource'] and self.opensource.enabled:
            self.active_method = 'opensource'
            self.active_client = self.opensource
            logger.info("✅ Using Open Source Scraper method (FREE)")
            if not self.opensource.logged_in:
                logger.warning("⚠️ Open source scraper needs login")
                logger.info("   Run: from twitter_whale_watcher import login_twitter; login_twitter()")
            return
        
        logger.warning("⚠️ No Twitter method available")
        if self.method == 'auto':
            logger.info("   • Install Chrome Extension for instant free access")
            logger.info("   • Or run: pip install twikit for open source option")
    
    def _check_rate_limit(self):
        """Simple rate limiting"""
        now = time.time()
        if now - self.last_request_time < 1:  # REDUCED from 2 seconds to 1 second
            time.sleep(1 - (now - self.last_request_time))
        self.last_request_time = time.time()
    
    def extract_whale_info(self, text: str) -> Optional[Dict]:
        """Extract whale transaction info from tweet text"""
        if not text:
            return None
        
        text = text.replace(',', '').replace('$', '')
        
        patterns = [
            r'(\d+\.?\d*)\s*(BTC|ETH|BNB|SOL|XRP|ADA|DOGE).*?(\d+\.?\d*)\s*(million|M|m)',
            r'(\d+\.?\d*)\s*(BTC|ETH|BNB|SOL|XRP).*?\$?(\d+\.?\d*)\s*(M|Million)',
            r'(\d+\.?\d*)\s*#?(BTC|ETH|BNB|SOL|XRP).*?(\d+\.?\d*)\s*M',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                try:
                    amount = float(match.group(1))
                    symbol = match.group(2).upper()
                    
                    if len(match.groups()) >= 4:
                        value_num = float(match.group(3))
                        unit = match.group(4).lower() if match.group(4) else ''
                        value_usd = value_num * 1_000_000 if 'm' in unit else value_num
                    else:
                        # Estimate based on current prices
                        price_map = {'BTC': 65000, 'ETH': 3500, 'BNB': 600, 'SOL': 150, 'XRP': 0.6}
                        value_usd = amount * price_map.get(symbol, 50000)
                    
                    if value_usd >= 1_000_000:
                        return {
                            'amount': round(amount, 2),
                            'symbol': symbol,
                            'value_usd': round(value_usd)
                        }
                except:
                    pass
        return None
    
    def fetch_whale_tweets(self) -> List[Dict]:
        """Fetch recent tweets using active method"""
        if not self.active_client:
            return []
        
        all_tweets = []
        
        # INCREASED from 5 to 8 accounts per check
        for account in self.whale_accounts[:8]:  
            try:
                self._check_rate_limit()
                # INCREASED from 5 to 10 tweets per account
                tweets = self.active_client.get_user_tweets(account, count=10)
                
                for tweet in tweets:
                    whale_info = self.extract_whale_info(tweet.get('text', ''))
                    if whale_info:
                        all_tweets.append({
                            'id': tweet.get('id'),
                            'text': tweet.get('text'),
                            'created_at': tweet.get('created_at', datetime.now()),
                            'account': account,
                            'whale_info': whale_info,
                            'source': tweet.get('source', f'Twitter @{account}')
                        })
                
                time.sleep(0.5)  # REDUCED from 1 second to 0.5 seconds
                
            except Exception as e:
                logger.debug(f"Error fetching @{account}: {e}")
                continue
        
        return all_tweets
    
    def start_monitoring(self, interval_seconds: int = 120):  # REDUCED from 300 to 120 seconds
        """Start background monitoring"""
        if not self.active_client:
            logger.warning("⚠️ No Twitter method available")
            return
        
        if self.is_running:
            return
        
        self.is_running = True
        
        def monitor_loop():
            logger.info(f"🐦 Twitter monitor started ({self.active_method} method)")
            
            while self.is_running:
                try:
                    tweets = self.fetch_whale_tweets()
                    if tweets:
                        self.recent_tweets = (tweets + self.recent_tweets)[:self.max_tweets]
                        whale_count = sum(1 for t in tweets if t.get('whale_info'))
                        if whale_count > 0:
                            logger.info(f"🐦 Twitter: {whale_count} whale tweets found")
                    
                    time.sleep(interval_seconds)
                    
                except Exception as e:
                    logger.error(f"Twitter monitor error: {e}")
                    time.sleep(60)
        
        self.monitor_thread = threading.Thread(target=monitor_loop, daemon=True)
        self.monitor_thread.start()
    
    def stop_monitoring(self):
        """Stop monitoring"""
        self.is_running = False
        logger.info("🐦 Twitter monitor stopped")
    
    def get_recent_alerts(self, min_value_usd: float = 1000000) -> List[Dict]:
        """Get recent whale alerts"""
        alerts = []
        # INCREASED from 30 to 50 tweets to check
        for tweet in self.recent_tweets[:50]:  
            info = tweet.get('whale_info', {})
            if info.get('value_usd', 0) >= min_value_usd:
                value_m = info['value_usd'] / 1_000_000
                alerts.append({
                    'title': f"🐋 {info['amount']} {info['symbol']} (${value_m:.1f}M)",
                    'value_usd': info['value_usd'],
                    'symbol': info['symbol'],
                    'date': tweet.get('created_at').isoformat() if hasattr(tweet.get('created_at'), 'isoformat') else str(tweet.get('created_at')),
                    'source': tweet.get('source'),
                    'account': tweet.get('account')
                })
        return alerts


# ===== LOGIN HELPER FOR OPEN SOURCE METHOD =====
def login_twitter(username: str = None, email: str = None, password: str = None):
    """Helper function to login to Twitter for open source scraper"""
    logger.info("\n🔐 TWITTER LOGIN FOR OPEN SOURCE SCRAPER")

    logger.info("="*50)

    if not username:
        username = input("Enter your Twitter username: ").strip()
    if not email:
        email = input("Enter your Twitter email: ").strip()
    if not password:
        password = getpass.getpass("Enter your Twitter password: ")
    
    scraper = OpenSourceScraper()
    success = scraper.login(username, email, password)
    
    if success:
        logger.info("\n✅ Login successful! Cookies saved for future use.")

        logger.info("   You can now use the open source scraper method.")

    else:
        logger.info("\n❌ Login failed. Check your credentials and try again.")

    return success


# ===== SIMPLE TEST =====
if __name__ == "__main__":
    logger.info("\n🐦 TESTING TWITTER WHALE WATCHER")

    logger.info("="*60)

    # Try auto mode
    watcher = TwitterWhaleWatcher(method="auto")
    
    logger.info(f"\n✅ Active method: {watcher.active_method or 'None'}")

    if watcher.active_method:
        logger.info("\n📡 Testing fetch...")

        tweets = watcher.fetch_whale_tweets()
        logger.info(f"Found {len(tweets)} tweets")

        if tweets:
            logger.info("\n🐋 Sample whale alerts:")

            for tweet in tweets[:3]:
                info = tweet.get('whale_info')
                if info:
                    value_m = info['value_usd'] / 1_000_000
                    logger.info(f"  • {info['amount']} {info['symbol']} (${value_m:.1f}M) from {tweet.get('source')}")

    else:
        logger.info("\n❌ No method available. Options:")

        logger.info("   1. Install Chrome Extension: https://chromewebstore.google.com/detail/twitter-web-api/pnbhkojogdglhidcgnfljnomjdckkfjh")

        logger.info("   2. Install Twikit: pip install twikit")

        logger.info("   3. Run login: from twitter_whale_watcher import login_twitter; login_twitter()")

    logger.info("="*60)