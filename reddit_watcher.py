
import os
"""
Reddit Watcher - Free source for whale alerts and news sentiment
UPDATED: Faster rate limits (50 requests per minute)
"""

import praw
import re
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import time
import threading
from textblob import TextBlob
from utils.logger import logger

class RedditWatcher:
    """
    Fetches whale alerts and news sentiment from Reddit
    NOW WITH: 50 requests per minute (optimized)
    """
    
    def __init__(self):
        self.reddit = None
        self.enabled = False
        self.recent_posts = []
        self.max_posts = 100
        self.is_running = False
        self.request_count = 0
        self.last_reset = time.time()
        self.setup_reddit()
        
        # Rate limiting - 50 requests per minute
        self.requests_per_minute = 50
        self.request_interval = 60.0 / self.requests_per_minute  # ~1.2 seconds between requests
        
        # Subreddits to monitor
        self.whale_subs = [
            'whalealert',           # Dedicated whale alerts
            'CryptoMarkets',         # Crypto market discussions
            'CryptoCurrency',        # General crypto
            'Bitcoin',               # BTC-specific
            'ethereum',              # ETH-specific
            'solana',                # SOL-specific
            'Crypto_General',        # Crypto discussions
        ]
        
        self.news_subs = [
            'news',                  # General news
            'worldnews',             # World news
            'economy',               # Economic news
            'investing',             # Investing discussions
            'stocks',                # Stock market
            'wallstreetbets',        # Market sentiment
            'CryptoMarkets',         # Crypto sentiment
            'forex',                 # Forex discussions
            'commodities',           # Commodities
        ]
        
        logger.info("\n" + "="*50)

        logger.info("📱 REDDIT WATCHER (OPTIMIZED)")

        logger.info("="*50)

        logger.info(f"✅ Reddit API: {'ACTIVE' if self.enabled else 'DISABLED'}")

        logger.info(f"📊 Whale subs: {len(self.whale_subs)}")

        logger.info(f"📰 News subs: {len(self.news_subs)}")

        logger.info(f"⚡ Rate limit: {self.requests_per_minute} requests/min")

        logger.info("="*50)

    def setup_reddit(self):
        """Setup Reddit API with your free credentials"""
        try:
            # You'll get these from reddit.com/prefs/apps
            self.reddit = praw.Reddit(
                client_id=os.getenv("REDDIT_CLIENT_ID", ""),
                client_secret=os.getenv("REDDIT_CLIENT_SECRET", ""),
                user_agent="trading-bot/1.0 by RGriffons"
            )
            # Test connection — user.me() returns None for anonymous/fake credentials
            # so we must also verify credentials aren't placeholders
            cid = self.reddit.config.client_id
            if not cid or cid in ('YOUR_CLIENT_ID', 'your_client_id', ''):
                raise ValueError("Reddit client_id is still a placeholder — set real credentials in reddit_watcher.py")
            me = self.reddit.user.me()
            # me is None for anonymous read-only — that means auth failed silently
            if me is None:
                raise ValueError("Reddit auth returned None — credentials invalid or not set")
            self.enabled = True
            logger.info(f"Reddit API connected as u/{me.name}")
        except Exception as e:
            logger.warning(f"Reddit API disabled: {e}")
            logger.info("   Get free credentials at: https://www.reddit.com/prefs/apps")
            self.enabled = False
    
    def _wait_for_rate_limit(self):
        """Ensure we don't exceed 50 requests per minute"""
        now = time.time()
        
        # Reset counter every minute
        if now - self.last_reset >= 60:
            self.request_count = 0
            self.last_reset = now
        
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
        
        # Small delay between requests to be smooth
        time.sleep(self.request_interval)
    
    def extract_whale_info(self, text: str) -> Optional[Dict]:
        """Extract whale transaction info (reuses your existing regex)"""
        if not text:
            return None
        
        text = text.replace(',', '').replace('$', '')
        
        patterns = [
            r'(\d+\.?\d*)\s*(BTC|ETH|BNB|SOL|XRP).*?(\d+\.?\d*)\s*(M|Million)',
            r'(\d+\.?\d*)\s*(BTC|ETH).*?\$?(\d+\.?\d*)\s*M',
            r'(\d+\.?\d*)\s*#?(BTC|ETH).*?(\d+\.?\d*)\s*M',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                try:
                    amount = float(match.group(1))
                    symbol = match.group(2).upper()
                    
                    if len(match.groups()) >= 4:
                        value_num = float(match.group(3))
                        value_usd = value_num * 1_000_000
                    else:
                        # Estimate based on current prices
                        price_map = {'BTC': 65000, 'ETH': 3500, 'BNB': 600, 'SOL': 150}
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
    
    def analyze_sentiment(self, text: str) -> float:
        """Analyze sentiment of text (-1 to 1)"""
        try:
            blob = TextBlob(text)
            return blob.sentiment.polarity
        except:
            return 0.0
    
    def get_whale_alerts(self, limit: int = 50) -> List[Dict]:
        """Fetch potential whale alerts from Reddit - OPTIMIZED"""
        if not self.enabled:
            return []
        
        alerts = []
        
        # Process more subreddits now (up to 5) since we have higher rate limit
        for subreddit_name in self.whale_subs[:5]:  
            try:
                self._wait_for_rate_limit()
                subreddit = self.reddit.subreddit(subreddit_name)
                
                # Get more posts (25 instead of 20)
                for post in subreddit.new(limit=25):
                    # Check title for whale keywords
                    title_lower = post.title.lower()
                    if any(k in title_lower for k in ['whale', 'million', 'billion', 'large transfer', 'moved']):
                        whale_info = self.extract_whale_info(post.title)
                        if whale_info:
                            alerts.append({
                                'title': post.title,
                                'value_usd': whale_info['value_usd'],
                                'symbol': whale_info['symbol'],
                                'amount': whale_info['amount'],
                                'url': f"https://reddit.com{post.permalink}",
                                'score': post.score,
                                'comments': post.num_comments,
                                'created': datetime.fromtimestamp(post.created_utc),
                                'source': f'Reddit r/{subreddit_name}',
                                'type': 'whale_alert'
                            })
                    
                    # Also check comments for whale info
                    if post.num_comments > 0:
                        post.comment_sort = 'top'
                        post.comments.replace_more(limit=0)
                        for comment in list(post.comments)[:8]:  # More comments (8 instead of 5)
                            if any(k in comment.body.lower() for k in ['whale', 'million', 'billion']):
                                whale_info = self.extract_whale_info(comment.body)
                                if whale_info:
                                    alerts.append({
                                        'title': f"Comment: {whale_info['amount']} {whale_info['symbol']}",
                                        'value_usd': whale_info['value_usd'],
                                        'symbol': whale_info['symbol'],
                                        'amount': whale_info['amount'],
                                        'url': f"https://reddit.com{post.permalink}",
                                        'score': comment.score,
                                        'created': datetime.fromtimestamp(comment.created_utc),
                                        'source': f'Reddit r/{subreddit_name} comment',
                                        'type': 'whale_alert'
                                    })
                
                logger.debug(f"✅ Processed r/{subreddit_name}")
                
            except Exception as e:
                logger.debug(f"Reddit error r/{subreddit_name}: {e}")
                continue
        
        logger.info(f"🐋 Found {len(alerts)} whale alerts")
        return alerts
    
    def get_news_sentiment(self, limit: int = 200) -> Dict:
        """Get comprehensive news sentiment from Reddit - OPTIMIZED"""
        if not self.enabled:
            return {'score': 0, 'posts': []}
        
        all_posts = []
        sentiments = []
        
        # Process more news subs (up to 8)
        for subreddit_name in self.news_subs[:8]:  
            try:
                self._wait_for_rate_limit()
                subreddit = self.reddit.subreddit(subreddit_name)
                
                # Get more hot posts (30 instead of 20)
                for post in subreddit.hot(limit=30):
                    # Analyze title sentiment
                    title_sentiment = self.analyze_sentiment(post.title)
                    
                    # Get top comments for deeper sentiment
                    post.comment_sort = 'top'
                    post.comments.replace_more(limit=0)
                    comment_sentiments = []
                    for comment in list(post.comments)[:8]:  # More comments (8 instead of 5)
                        comment_sentiments.append(self.analyze_sentiment(comment.body))
                    
                    avg_comment_sentiment = sum(comment_sentiments) / len(comment_sentiments) if comment_sentiments else 0
                    
                    # Combined sentiment (70% post, 30% comments)
                    combined_sentiment = (title_sentiment * 0.7) + (avg_comment_sentiment * 0.3)
                    
                    post_data = {
                        'title': post.title,
                        'subreddit': subreddit_name,
                        'sentiment': combined_sentiment,
                        'score': post.score,
                        'upvote_ratio': post.upvote_ratio,
                        'comments': post.num_comments,
                        'url': f"https://reddit.com{post.permalink}",
                        'created': datetime.fromtimestamp(post.created_utc)
                    }
                    all_posts.append(post_data)
                    sentiments.append(combined_sentiment)
                
                logger.debug(f"✅ Processed r/{subreddit_name}")
                
            except Exception as e:
                logger.debug(f"Reddit news error r/{subreddit_name}: {e}")
                continue
        
        # Calculate overall sentiment
        if sentiments:
            avg_sentiment = sum(sentiments) / len(sentiments)
            
            # Weight by post engagement
            weighted_sentiment = 0
            total_weight = 0
            for post in all_posts:
                weight = post['score'] + post['comments'] + 1
                weighted_sentiment += post['sentiment'] * weight
                total_weight += weight
            
            final_sentiment = weighted_sentiment / total_weight if total_weight > 0 else avg_sentiment
        else:
            final_sentiment = 0
        
        result = {
            'score': final_sentiment,
            'posts': sorted(all_posts, key=lambda x: x['score'], reverse=True)[:30],  # More posts (30 instead of 20)
            'total_posts': len(all_posts),
            'timestamp': datetime.now().isoformat()
        }
        
        logger.info(f"📊 Reddit sentiment: {final_sentiment:.2f} from {len(all_posts)} posts")
        return result
    
    def get_market_sentiment_by_asset(self, asset: str) -> Dict:
        """Get sentiment for specific asset (BTC, ETH, AAPL, etc) - OPTIMIZED"""
        if not self.enabled:
            return {'score': 0, 'posts': []}
        
        # Map asset to relevant subreddits
        asset_subs = {
            'BTC': ['Bitcoin', 'CryptoMarkets', 'CryptoCurrency'],
            'ETH': ['ethereum', 'CryptoMarkets', 'CryptoCurrency'],
            'SOL': ['solana', 'CryptoMarkets', 'CryptoCurrency'],
            'AAPL': ['stocks', 'investing', 'wallstreetbets'],
            'MSFT': ['stocks', 'investing', 'wallstreetbets'],
            'TSLA': ['stocks', 'investing', 'wallstreetbets', 'teslamotors'],
            'GOLD': ['commodities', 'investing', 'wallstreetbets'],
        }
        
        search_terms = [asset, asset.replace('-USD', '')]
        subs = asset_subs.get(asset.split('-')[0], ['investing', 'stocks', 'CryptoMarkets'])
        
        relevant_posts = []
        sentiments = []
        
        for sub in subs[:3]:  # Check up to 3 subreddits
            try:
                self._wait_for_rate_limit()
                subreddit = self.reddit.subreddit(sub)
                
                # Get more posts (40 instead of 30)
                for post in subreddit.hot(limit=40):
                    title_lower = post.title.lower()
                    if any(term.lower() in title_lower for term in search_terms):
                        sentiment = self.analyze_sentiment(post.title)
                        relevant_posts.append({
                            'title': post.title,
                            'sentiment': sentiment,
                            'score': post.score,
                            'url': f"https://reddit.com{post.permalink}",
                            'subreddit': sub
                        })
                        sentiments.append(sentiment)
                        
            except Exception as e:
                logger.debug(f"Asset error r/{sub}: {e}")
                continue
        
        avg_sentiment = sum(sentiments) / len(sentiments) if sentiments else 0
        
        result = {
            'asset': asset,
            'score': avg_sentiment,
            'posts': relevant_posts[:15],  # More posts (15 instead of 10)
            'total_mentions': len(relevant_posts)
        }
        
        logger.info(f"💰 {asset} sentiment: {avg_sentiment:.2f} from {len(relevant_posts)} mentions")
        return result
    
    def start_monitoring(self):
        """Start background monitoring. Safe to call multiple times."""
        if not self.enabled:
            return
        if self.is_running:
            logger.debug("📱 Reddit monitor already running — skipping duplicate start")
            return
        self.is_running = True
        thread = threading.Thread(target=self._monitor_loop, daemon=True)
        thread.start()
        logger.info("📱 Reddit monitor started (optimized mode)")
    
    def _monitor_loop(self):
        """Background monitoring loop - OPTIMIZED"""
        while self.is_running:
            try:
                # Get whale alerts
                whales = self.get_whale_alerts()
                if whales:
                    self.recent_posts.extend(whales)
                    self.recent_posts = self.recent_posts[-self.max_posts:]
                
                time.sleep(120)  # Check every 2 minutes instead of 5
                
            except Exception as e:
                logger.error(f"Reddit monitor error: {e}")
                time.sleep(60)
    
    def stop_monitoring(self):
        """Stop monitoring"""
        self.is_running = False
        logger.info("📱 Reddit monitor stopped")