"""
📰 COMPREHENSIVE NEWS SOURCES INTEGRATION
All your requested sources: Binance, Watcher Guru, Whale Alert, Kobeissi Letter,
Walter Bloomberg, Bloomberg, Forbes, Forbes Crypto, Bloomberg Crypto,
Gold Telegraph, Wall Street Journal, Bloomberg Markets
"""

import requests
import feedparser
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from textblob import TextBlob
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

# Import all config values
from config.config import (
    NEWSAPI_KEY, GNEWS_KEY, RAPIDAPI_KEY,
    WHALE_ALERT_KEY, TWITTER_BEARER_TOKEN,
    BLOOMBERG_RSS, FORBES_RSS, GOLD_TELEGRAPH_RSS,
    BINANCE_ANNOUNCEMENTS_URL
)

class NewsSourceIntegrator:
    """
    Integrates all news sources into one unified interface
    """
    
    def __init__(self):
        self.sources = {}
        self._setup_sources()
        
    def _setup_sources(self):
        """Initialize all news sources"""
        
        # ===== BINANCE ANNOUNCEMENTS =====
        self.sources['binance'] = {
            'name': 'Binance Announcements',
            'type': 'api',
            'enabled': True,
            'function': self.fetch_binance_announcements
        }
        
        # ===== WHALE ALERT =====
        self.sources['whale_alert'] = {
            'name': 'Whale Alert',
            'type': 'api',
            'enabled': bool(WHALE_ALERT_KEY),
            'function': self.fetch_whale_alerts,
            'api_key': WHALE_ALERT_KEY
        }
        
        # ===== TWITTER SOURCES =====
        if TWITTER_BEARER_TOKEN:
            self.sources['kobeissi'] = {
                'name': 'The Kobeissi Letter',
                'type': 'twitter',
                'enabled': True,
                'username': 'KobeissiLetter',
                'function': self.fetch_twitter_user
            }
            
            self.sources['walter'] = {
                'name': 'Walter Bloomberg',
                'type': 'twitter',
                'enabled': True,
                'username': 'WalterBloomberg',
                'function': self.fetch_twitter_user
            }
        
        # ===== BLOOMBERG RSS FEEDS =====
        for key, url in BLOOMBERG_RSS.items():
            self.sources[f'bloomberg_{key}'] = {
                'name': f'Bloomberg {key.title()}',
                'type': 'rss',
                'enabled': True,
                'url': url,
                'function': self.fetch_rss_feed
            }
        
        # ===== FORBES RSS FEEDS =====
        for key, url in FORBES_RSS.items():
            self.sources[f'forbes_{key}'] = {
                'name': f'Forbes {key.title()}',
                'type': 'rss',
                'enabled': True,
                'url': url,
                'function': self.fetch_rss_feed
            }
        
        # ===== GOLD TELEGRAPH =====
        self.sources['gold_telegraph'] = {
            'name': 'Gold Telegraph',
            'type': 'rss',
            'enabled': True,
            'url': GOLD_TELEGRAPH_RSS,
            'function': self.fetch_rss_feed
        }
        
        # ===== WALL STREET JOURNAL (via NewsAPI) =====
        self.sources['wsj'] = {
            'name': 'Wall Street Journal',
            'type': 'newsapi',
            'enabled': bool(NEWSAPI_KEY),
            'function': self.fetch_wsj_newsapi,
            'api_key': NEWSAPI_KEY
        }
        
        # ===== WATCHER GURU (via RSS) =====
        self.sources['watcher_guru'] = {
            'name': 'Watcher Guru',
            'type': 'rss',
            'enabled': True,
            'url': 'https://watcher.guru/feed/',  # Check actual RSS URL
            'function': self.fetch_rss_feed
        }
    
    # ===== FETCH METHODS =====
    
    def fetch_binance_announcements(self, limit=10):
        """Fetch Binance official announcements"""
        try:
            params = {
                "type": "1",
                "pageSize": limit,
                "pageNo": 1
            }
            
            response = requests.get(BINANCE_ANNOUNCEMENTS_URL, params=params, timeout=5)
            data = response.json()
            
            articles = []
            for article in data.get('data', {}).get('articles', []):
                title = article.get('title', '')
                
                # Analyze sentiment
                blob = TextBlob(title)
                sentiment = blob.sentiment.polarity
                
                # Adjust sentiment based on catalog
                catalog = article.get('catalogId', 0)
                if catalog == 161:  # Delisting
                    sentiment = -0.5
                elif catalog == 160:  # New listings
                    sentiment = 0.5
                
                articles.append({
                    'title': title,
                    'sentiment': sentiment,
                    'url': f"https://www.binance.com/en/support/announcement/{article['code']}",
                    'date': article.get('releaseDate', ''),
                    'source': 'Binance Announcements',
                    'category': article.get('catalogName', '')
                })
            
            return articles
        except Exception as e:
            print(f"Binance error: {e}")
            return []
    
    def fetch_whale_alerts(self, min_value_usd=1000000):
        """Fetch large crypto transactions from Whale Alert"""
        if not WHALE_ALERT_KEY:
            return []
        
        try:
            url = "https://api.whale-alert.io/v1/transactions"
            params = {
                "api_key": WHALE_ALERT_KEY,
                "min_value": min_value_usd,
                "limit": 20
            }
            
            response = requests.get(url, params=params, timeout=5)
            data = response.json()
            
            alerts = []
            for tx in data.get('transactions', []):
                value = tx.get('amount_usd', 0)
                sentiment = 0.1 if tx.get('symbol') in ['BTC', 'ETH'] else 0.05
                
                alerts.append({
                    'title': f"Whale Alert: {tx.get('amount', 0):.2f} {tx.get('symbol')} (${value:,.0f}) moved from {tx.get('from', {}).get('owner_type')} to {tx.get('to', {}).get('owner_type')}",
                    'sentiment': sentiment,
                    'value_usd': value,
                    'symbol': tx.get('symbol'),
                    'source': 'Whale Alert',
                    'url': tx.get('url', ''),
                    'date': datetime.fromtimestamp(tx.get('timestamp', 0)).isoformat()
                })
            
            return alerts
        except Exception as e:
            print(f"Whale Alert error: {e}")
            return []
    
    def fetch_twitter_user(self, username, limit=10):
        """Fetch tweets from a Twitter user"""
        if not TWITTER_BEARER_TOKEN:
            return []
        
        try:
            headers = {"Authorization": f"Bearer {TWITTER_BEARER_TOKEN}"}
            
            # Get user ID
            url = f"https://api.twitter.com/2/users/by/username/{username}"
            user_response = requests.get(url, headers=headers, timeout=5)
            
            if user_response.status_code != 200:
                return []
            
            user_id = user_response.json()['data']['id']
            
            # Get tweets
            tweets_url = f"https://api.twitter.com/2/users/{user_id}/tweets"
            params = {
                "max_results": limit,
                "tweet.fields": "created_at,public_metrics"
            }
            
            tweets_response = requests.get(tweets_url, headers=headers, params=params, timeout=5)
            
            if tweets_response.status_code != 200:
                return []
            
            tweets = tweets_response.json().get('data', [])
            
            articles = []
            for tweet in tweets:
                blob = TextBlob(tweet['text'])
                sentiment = blob.sentiment.polarity
                
                articles.append({
                    'title': tweet['text'][:100] + ('...' if len(tweet['text']) > 100 else ''),
                    'sentiment': sentiment,
                    'date': tweet.get('created_at', ''),
                    'url': f"https://twitter.com/{username}/status/{tweet['id']}",
                    'source': f'@{username}',
                    'likes': tweet.get('public_metrics', {}).get('like_count', 0),
                    'retweets': tweet.get('public_metrics', {}).get('retweet_count', 0)
                })
            
            return articles
        except Exception as e:
            print(f"Twitter error for @{username}: {e}")
            return []
    
    def fetch_rss_feed(self, url, limit=10):
        """Fetch and parse RSS feed"""
        try:
            feed = feedparser.parse(url)
            
            articles = []
            for entry in feed.entries[:limit]:
                title = entry.get('title', '')
                blob = TextBlob(title)
                sentiment = blob.sentiment.polarity
                
                articles.append({
                    'title': title,
                    'sentiment': sentiment,
                    'url': entry.get('link', ''),
                    'date': entry.get('published', ''),
                    'source': feed.feed.get('title', 'RSS Feed'),
                    'summary': entry.get('summary', '')[:200]
                })
            
            return articles
        except Exception as e:
            print(f"RSS error for {url}: {e}")
            return []
    
    def fetch_wsj_newsapi(self, days=1):
        """Fetch WSJ articles using NewsAPI"""
        # ===== FIXED: Only check if key exists, no hardcoded string! =====
        if not NEWSAPI_KEY:
            print("  ⚠️ NewsAPI key not configured in .env")
            return []
        # ================================================================
        
        try:
            url = "https://newsapi.org/v2/everything"
            params = {
                "domains": "wsj.com",  # Wall Street Journal domain
                "from": (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d'),
                "sortBy": "relevancy",
                "language": "en",
                "apiKey": NEWSAPI_KEY,
                "pageSize": 10
            }
            
            response = requests.get(url, params=params, timeout=5)
            data = response.json()
            
            if data.get('status') == 'ok':
                articles = []
                for article in data.get('articles', []):
                    title = article.get('title', '')
                    blob = TextBlob(title)
                    sentiment = blob.sentiment.polarity
                    
                    articles.append({
                        'title': title,
                        'sentiment': sentiment,
                        'url': article.get('url'),
                        'date': article.get('publishedAt'),
                        'source': 'Wall Street Journal',
                        'author': article.get('author')
                    })
                return articles
            else:
                print(f"  ⚠️ NewsAPI error: {data.get('message', 'Unknown error')}")
                return []
                
        except Exception as e:
            print(f"  ⚠️ WSJ NewsAPI error: {e}")
            return []
    
    # ===== MAIN METHODS =====
    
    def fetch_all_sources(self, asset_type='general'):
        """
        Fetch from all enabled sources with better timeout handling
        """
        all_articles = []
        enabled_sources = {name: src for name, src in self.sources.items() if src['enabled']}
        
        print(f"\n📰 Fetching from {len(enabled_sources)} news sources...")
        
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_source = {}
            
            for name, source in enabled_sources.items():
                # Different limits per source type
                if source['type'] == 'twitter':
                    future = executor.submit(source['function'], source['username'], 5)
                elif source['type'] == 'rss':
                    future = executor.submit(source['function'], source['url'], 8)
                elif source['type'] == 'newsapi':
                    future = executor.submit(source['function'], 5)
                elif source['type'] == 'api':
                    if name == 'binance':
                        future = executor.submit(source['function'], 10)
                    elif name == 'whale_alert':
                        future = executor.submit(source['function'], 10)
                    else:
                        future = executor.submit(source['function'], 5)
                else:
                    continue
                
                future_to_source[future] = name
            
            # Collect results with longer timeout
            for future in as_completed(future_to_source):
                name = future_to_source[future]
                try:
                    articles = future.result(timeout=8)  # Increased timeout
                    if articles:
                        all_articles.extend(articles)
                        print(f"  ✅ {self.sources[name]['name']}: {len(articles)} articles")
                    else:
                        print(f"  ⏭️ {self.sources[name]['name']}: No articles")
                except Exception as e:
                    print(f"  ⚠️ {self.sources[name]['name']}: {str(e)[:50]}")
        
        # Show summary
        if all_articles:
            from collections import Counter
            sources = [a['source'] for a in all_articles]
            counts = Counter(sources)
            print(f"\n📊 Total: {len(all_articles)} articles from {len(counts)} sources")
            for source, count in counts.most_common(5):
                print(f"  • {source}: {count} articles")
        
        return all_articles
    
    def fetch_with_retry(self, func, *args, retries=2, timeout=10):
        """Fetch with retry logic"""
        for attempt in range(retries + 1):
            try:
                return func(*args, timeout=timeout)
            except Exception as e:
                if attempt == retries:
                    raise
                print(f"  🔄 Retry {attempt + 1}/{retries}...")
                time.sleep(2)
        return []
    
    def get_sentiment_summary(self, asset=None):
        """
        Get overall sentiment from all sources
        """
        articles = self.fetch_all_sources()
        
        if not articles:
            return {
                'overall_sentiment': 'Neutral',
                'score': 0,
                'article_count': 0,
                'sources': []
            }
        
        total_sentiment = sum(a['sentiment'] for a in articles)
        avg_sentiment = total_sentiment / len(articles)
        
        # Count by source
        source_counts = {}
        for a in articles:
            source = a.get('source', 'Unknown')
            source_counts[source] = source_counts.get(source, 0) + 1
        
        return {
            'overall_sentiment': self._interpret_sentiment(avg_sentiment),
            'score': avg_sentiment,
            'article_count': len(articles),
            'sources': source_counts,
            'recent_articles': articles[:5]
        }
    
    def _interpret_sentiment(self, score):
        """Convert score to text"""
        if score > 0.3:
            return "Very Bullish"
        elif score > 0.1:
            return "Bullish"
        elif score > -0.1:
            return "Neutral"
        elif score > -0.3:
            return "Bearish"
        else:
            return "Very Bearish"