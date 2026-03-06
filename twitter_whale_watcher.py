"""
Twitter Whale Alert Watcher
Tracks whale accounts for large crypto transactions
"""

import tweepy
import re
from datetime import datetime
from typing import List, Dict, Optional
import time
import threading
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class TwitterWhaleWatcher:
    """
    Tracks Twitter accounts that post whale alerts
    """
    
    def __init__(self):
        """Initialize with Twitter API credentials from .env"""
        self.bearer_token = os.getenv('TWITTER_BEARER_TOKEN')
        self.api_key = os.getenv('TWITTER_API_KEY')
        self.api_secret = os.getenv('TWITTER_API_SECRET')
        self.access_token = os.getenv('TWITTER_ACCESS_TOKEN')
        self.access_secret = os.getenv('TWITTER_ACCESS_SECRET')
        
        self.client = None
        # 🐋 WHALE-FOCUSED ACCOUNTS ONLY
        self.whale_accounts = [
            # Primary whale trackers
            'whale_alert',            # #1 whale transaction tracker
            'lookonchain',            # Best wallet-level whale tracking
            'spotonchain',            # Great whale alerts
            'OnchainDataNerd',        # Whale movements
            'nansen_ai',              # Smart money/whale wallets
            'ArkhamIntel',            # Whale wallet intelligence
            'CryptoWhale',            # Dedicated whale alerts
            'WhaleWire',              # Whale transaction alerts
            
            # On-chain analytics (whale behavior)
            'santimentfeed',          # Whale accumulation metrics
            'glassnode',              # On-chain whale metrics
            'intotheblock',           # Large holder insights
            
            # Keep a few news/macro for context
            'KobeissiLetter',         # Market-moving context
            'WatcherGuru',            # Crypto news
            'tier10k',                # Fast macro alerts
        ]

        # Crypto-specific whale accounts (subset of above)
        self.crypto_whales = [
            'whale_alert',
            'lookonchain',
            'spotonchain',
            'OnchainDataNerd',
            'nansen_ai',
            'ArkhamIntel',
            'CryptoWhale',
            'WhaleWire',
            'santimentfeed',
        ]
        
        self.recent_tweets = []
        self.max_tweets = 100
        self.is_running = False
        
        self.setup_client()
    
    def setup_client(self):
        """Setup Twitter API client with bearer token"""
        try:
            if self.bearer_token:
                self.client = tweepy.Client(bearer_token=self.bearer_token)
                print(f"✅ Twitter API client initialized with Bearer Token")
            elif self.api_key and self.api_secret:
                # Fallback to OAuth 1.0a if bearer token not available
                auth = tweepy.OAuth1UserHandler(
                    self.api_key, self.api_secret,
                    self.access_token, self.access_secret
                )
                api = tweepy.API(auth)
                self.client = tweepy.Client(
                    consumer_key=self.api_key,
                    consumer_secret=self.api_secret,
                    access_token=self.access_token,
                    access_token_secret=self.access_secret
                )
                print(f"✅ Twitter API client initialized with OAuth")
            else:
                print(f"⚠️ Twitter API credentials not found in .env")
        except Exception as e:
            print(f"⚠️ Twitter API setup failed: {e}")
    
    def extract_whale_info(self, text: str) -> Optional[Dict]:
        """
        Extract whale transaction info from tweet text
        """
        # Pattern for: XXXX BTC ($XX,XXX,XXX) moved
        patterns = [
            r'(\d+[,]?\d*\.?\d*)\s*(BTC|ETH|BNB|SOL|XRP|ADA|DOGE|MATIC)\s*\(?\$?(\d+[,]?\d*\.?\d*)[\s\)]*(million|M|k|K)?',
            r'(\d+[,]?\d*\.?\d*)\s*(BTC|ETH).*?\$(\d+[,]?\d*\.?\d*)',
            r'(\d+\.?\d*)\s*(Bitcoin|Ethereum).*?(\d+\.?\d*)\s*(million|M)',
            r'(\d+[,]?\d*\.?\d*)\s*(#BTC|#ETH).*?\$(\d+[,]?\d*\.?\d*)',
            r'(\d+[kKmM]?)\s*(BTC|ETH|SOL|XRP).*?(\d+[mM]?)',  # Simpler format
            r'Whale Alert.*?(\d+[,]?\d*)\s*(BTC|ETH).*?\$(\d+[.,]?\d*)[mM]',  # Whale Alert format
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                try:
                    # Handle different group lengths
                    if len(match.groups()) >= 3:
                        amount = float(re.sub(r'[^\d.]', '', match.group(1)))
                        symbol = match.group(2).replace('#', '').upper()
                        
                        # Handle million/thousand indicators
                        value_usd = 0
                        if len(match.groups()) >= 4 and match.group(4):
                            unit = match.group(4).lower() if match.group(4) else ''
                            if 'm' in unit:
                                value_usd = amount * 1_000_000
                            elif 'k' in unit:
                                value_usd = amount * 1_000
                            else:
                                value_usd = float(re.sub(r'[^\d.]', '', match.group(3))) * 1_000_000
                        else:
                            # Try to extract dollar value
                            value_str = match.group(3) if len(match.groups()) >= 3 else ''
                            if 'm' in value_str.lower():
                                value_usd = float(re.sub(r'[^\d.]', '', value_str)) * 1_000_000
                            else:
                                value_usd = float(re.sub(r'[^\d.]', '', value_str)) if value_str else amount * 50_000
                        
                        # Clean up symbol (BTC-USD -> BTC)
                        if '-' in symbol:
                            symbol = symbol.split('-')[0]
                        
                        # Ensure minimum value is reasonable
                        if value_usd < 100_000:  # Too small, probably not a whale
                            continue
                        
                        return {
                            'amount': amount,
                            'symbol': symbol,
                            'value_usd': value_usd,
                            'text': text[:100] + '...' if len(text) > 100 else text
                        }
                except Exception as e:
                    # Silent fail for regex errors
                    continue
        return None
    
    def fetch_whale_tweets(self) -> List[Dict]:
        """
        Fetch recent tweets from whale alert accounts
        """
        if not self.client:
            return []
        
        all_tweets = []
        
        for account in self.whale_accounts:
            try:
                # Add small delay to avoid rate limits
                time.sleep(0.5)
                
                # Get user ID first
                user = self.client.get_user(username=account)
                if not user or not user.data:
                    print(f"  ⚠️ Could not find user @{account}")
                    continue
                
                user_id = user.data.id
                
                # Get recent tweets
                tweets = self.client.get_users_tweets(
                    id=user_id,
                    max_results=10,  # Reduced to 10 to save API calls
                    tweet_fields=['created_at', 'public_metrics'],
                    exclude=['retweets', 'replies']
                )
                
                if tweets and tweets.data:
                    tweet_count = 0
                    for tweet in tweets.data:
                        whale_info = self.extract_whale_info(tweet.text)
                        if whale_info:
                            all_tweets.append({
                                'id': tweet.id,
                                'text': tweet.text,
                                'created_at': tweet.created_at,
                                'account': account,
                                'whale_info': whale_info,
                                'likes': tweet.public_metrics.get('like_count', 0),
                                'retweets': tweet.public_metrics.get('retweet_count', 0),
                                'source': f'Twitter @{account}'
                            })
                            tweet_count += 1
                    
                    if tweet_count > 0:
                        print(f"  ✅ Found {tweet_count} whale tweets from @{account}")
                            
            except tweepy.TooManyRequests:
                print(f"  ⚠️ Rate limited for @{account}, waiting...")
                time.sleep(60)
            except tweepy.Unauthorized:
                print(f"  ⚠️ Unauthorized for @{account} - need to follow this account")
            except Exception as e:
                print(f"  ⚠️ Error fetching @{account}: {str(e)[:50]}")
                continue
        
        # Sort by date, newest first
        all_tweets.sort(key=lambda x: x['created_at'], reverse=True)
        
        return all_tweets[:self.max_tweets]
    
    def start_monitoring(self, interval_seconds: int = 300):
        """
        Start background thread to monitor Twitter
        """
        if not self.client:
            print("⚠️ Twitter client not initialized - cannot start monitoring")
            return
            
        self.is_running = True
        
        def monitor_loop():
            while self.is_running:
                try:
                    tweets = self.fetch_whale_tweets()
                    if tweets:
                        self.recent_tweets = tweets
                        print(f"🐦 Twitter: {len(tweets)} whale tweets cached")
                    else:
                        print(f"🐦 Twitter: No new whale tweets found")
                    
                    time.sleep(interval_seconds)
                    
                except Exception as e:
                    print(f"⚠️ Twitter monitor error: {e}")
                    time.sleep(60)
        
        thread = threading.Thread(target=monitor_loop, daemon=True)
        thread.start()
        print(f"🐦 Twitter whale monitor started (checking every {interval_seconds}s)")
    
    def stop_monitoring(self):
        """Stop monitoring"""
        self.is_running = False
    
    def get_recent_alerts(self, min_value_usd: float = 1000000) -> List[Dict]:
        """
        Get recent whale alerts, filtered by minimum value
        """
        alerts = []
        for tweet in self.recent_tweets[:30]:
            whale_info = tweet.get('whale_info', {})
            if whale_info.get('value_usd', 0) >= min_value_usd:
                value_millions = whale_info['value_usd'] / 1_000_000
                
                alerts.append({
                    'title': f"🐋 {whale_info['amount']} {whale_info['symbol']} (${value_millions:.1f}M)",
                    'value_usd': whale_info['value_usd'],
                    'symbol': whale_info['symbol'],
                    'date': tweet['created_at'].isoformat(),
                    'source': tweet['source'],
                    'url': f"https://twitter.com/{tweet['account']}/status/{tweet['id']}",
                    'likes': tweet.get('likes', 0),
                    'retweets': tweet.get('retweets', 0),
                    'sentiment': 0.15 if whale_info['value_usd'] > 10_000_000 else 0.1
                })
        return alerts
    
    def get_top_alerts(self, limit: int = 5) -> List[Dict]:
        """Get the largest whale alerts by value"""
        alerts = self.get_recent_alerts(min_value_usd=0)
        sorted_alerts = sorted(alerts, key=lambda x: x['value_usd'], reverse=True)
        return sorted_alerts[:limit]