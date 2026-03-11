"""
📰 COMPREHENSIVE NEWS SOURCES INTEGRATION
All your requested sources: Binance, Watcher Guru, Whale Alert, Kobeissi Letter,
Walter Bloomberg, Bloomberg, Forbes, Forbes Crypto, Bloomberg Crypto,
Wall Street Journal, Bloomberg Markets
PLUS: Investing.com, DailyFX, FXStreet, TradingView, FRED, ECB, MarketAux, ForexFactory

🔑 = Requires API Key (you have these already)
🌐 = Completely Free (RSS or no key needed)
"""

import requests
import feedparser
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from textblob import TextBlob
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import xml.etree.ElementTree as ET

# Import all config values
from logger import logger

from config.config import (
    NEWSAPI_KEY, GNEWS_KEY, RAPIDAPI_KEY,
    WHALE_ALERT_KEY, TWITTER_BEARER_TOKEN, ALPHA_VANTAGE_API_KEY,
    BLOOMBERG_RSS, FORBES_RSS,
    BINANCE_ANNOUNCEMENTS_URL, MARKETAUX_TOKEN
)

class NewsSourceIntegrator:
    """
    Integrates all news sources into one unified interface
    Now with 30+ sources - all FREE options included!
    """
    
    def __init__(self):
        self.sources = {}
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        self._setup_sources()
        
    def _setup_sources(self):
        """Initialize all news sources - ONLY FREE SOURCES!"""
        
        # ===== 1. BINANCE ANNOUNCEMENTS (🌐 FREE - no key) =====
        self.sources['binance'] = {
            'name': 'Binance Announcements',
            'type': 'api',
            'enabled': True,
            'free': True,
            'needs_key': False,
            'function': self.fetch_binance_announcements
        }
        
        # ===== 2. WHALE ALERT (🔑 NEEDS API KEY - you have it) =====
        self.sources['whale_alert'] = {
            'name': 'Whale Alert',
            'type': 'api',
            'enabled': bool(WHALE_ALERT_KEY),
            'free': False,
            'needs_key': True,
            'function': self.fetch_whale_alerts,
            'api_key': WHALE_ALERT_KEY
        }
        
        # ===== 3. TWITTER SOURCES (🔑 NEED API KEY - you have it) =====
        if TWITTER_BEARER_TOKEN:
            self.sources['kobeissi'] = {
                'name': 'The Kobeissi Letter',
                'type': 'twitter',
                'enabled': True,
                'free': False,
                'needs_key': True,
                'username': 'KobeissiLetter',
                'function': self.fetch_twitter_user
            }
            
            self.sources['walter'] = {
                'name': 'Walter Bloomberg',
                'type': 'twitter',
                'enabled': True,
                'free': False,
                'needs_key': True,
                'username': 'WalterBloomberg',
                'function': self.fetch_twitter_user
            }
        
        # ===== 4. BLOOMBERG RSS FEEDS (🌐 FREE - no key) =====
        for key, url in BLOOMBERG_RSS.items():
            self.sources[f'bloomberg_{key}'] = {
                'name': f'Bloomberg {key.title()}',
                'type': 'rss',
                'enabled': True,
                'free': True,
                'needs_key': False,
                'url': url,
                'function': self.fetch_rss_feed
            }
        
        # ===== 5. FORBES RSS FEEDS (🌐 FREE - no key) =====
        for key, url in FORBES_RSS.items():
            self.sources[f'forbes_{key}'] = {
                'name': f'Forbes {key.title()}',
                'type': 'rss',
                'enabled': True,
                'free': True,
                'needs_key': False,
                'url': url,
                'function': self.fetch_rss_feed
            }
        
        # ===== 6. WALL STREET JOURNAL (🔑 via NewsAPI - you have key) =====
        self.sources['wsj'] = {
            'name': 'Wall Street Journal',
            'type': 'newsapi',
            'enabled': bool(NEWSAPI_KEY),
            'free': False,
            'needs_key': True,
            'function': self.fetch_wsj_newsapi,
            'api_key': NEWSAPI_KEY
        }
        
        # ===== 7. WATCHER GURU (🌐 FREE RSS - no key) =====
        self.sources['watcher_guru'] = {
            'name': 'Watcher Guru',
            'type': 'rss',
            'enabled': True,
            'free': True,
            'needs_key': False,
            'url': 'https://watcher.guru/feed/',
            'function': self.fetch_rss_feed
        }
        
        # ===== 8. INVESTING.COM RSS FEEDS (🌐 FREE - no key) =====
        investing_feeds = {
            'top_news': 'https://www.investing.com/rss/news.rss',
            'stocks': 'https://www.investing.com/rss/stock_market.rss',
            'forex': 'https://www.investing.com/rss/forex.rss',
            'commodities': 'https://www.investing.com/rss/commodities.rss',
            'crypto': 'https://www.investing.com/rss/cryptocurrency.rss',
            'economic_calendar': 'https://www.investing.com/rss/economic_calendar.rss',
            'analysis': 'https://www.investing.com/rss/analysis.rss',
            'technical': 'https://www.investing.com/rss/technical.rss'
        }
        
        for key, url in investing_feeds.items():
            self.sources[f'investing_{key}'] = {
                'name': f'Investing.com {key.replace("_", " ").title()}',
                'type': 'rss',
                'enabled': True,
                'free': True,
                'needs_key': False,
                'url': url,
                'function': self.fetch_rss_feed
            }
        
        # ===== 9. DAILYFX RSS FEEDS (🌐 FREE - no key) =====
        dailyfx_feeds = {
            'news': 'https://www.dailyfx.com/feeds/news',
            'analysis': 'https://www.dailyfx.com/feeds/analysis',
            'forex': 'https://www.dailyfx.com/feeds/forex',
            'crypto': 'https://www.dailyfx.com/feeds/cryptocurrency'
        }
        
        for key, url in dailyfx_feeds.items():
            self.sources[f'dailyfx_{key}'] = {
                'name': f'DailyFX {key.title()}',
                'type': 'rss',
                'enabled': True,
                'free': True,
                'needs_key': False,
                'url': url,
                'function': self.fetch_rss_feed
            }
        
        # ===== 10. FXSTREET RSS FEEDS (🌐 FREE - no key) =====
        fxstreet_feeds = {
            'news': 'https://www.fxstreet.com/rss/news',
            'analysis': 'https://www.fxstreet.com/rss/analysis',
            'technical': 'https://www.fxstreet.com/rss/technical',
            'crypto': 'https://www.fxstreet.com/rss/cryptocurrencies'
        }
        
        for key, url in fxstreet_feeds.items():
            self.sources[f'fxstreet_{key}'] = {
                'name': f'FXStreet {key.title()}',
                'type': 'rss',
                'enabled': True,
                'free': True,
                'needs_key': False,
                'url': url,
                'function': self.fetch_rss_feed
            }
        
        # ===== 11. TRADINGVIEW RSS FEED (🌐 FREE - no key) =====
        self.sources['tradingview'] = {
            'name': 'TradingView Ideas',
            'type': 'rss',
            'enabled': True,
            'free': True,
            'needs_key': False,
            'url': 'https://www.tradingview.com/feed/',
            'function': self.fetch_rss_feed
        }
        
        # ===== 12. ALPHA VANTAGE NEWS (🔑 NEEDS API KEY - you have it) =====
        self.sources['alpha_vantage'] = {
            'name': 'Alpha Vantage News',
            'type': 'api',
            'enabled': bool(ALPHA_VANTAGE_API_KEY),
            'free': False,
            'needs_key': True,
            'function': self.fetch_alpha_vantage_news,
            'api_key': ALPHA_VANTAGE_API_KEY
        }
        
        # ===== 13. FRED ECONOMIC DATA (🌐 FREE - no key) =====
        self.sources['fred'] = {
            'name': 'FRED Economic Data',
            'type': 'api',
            'enabled': True,
            'free': True,
            'needs_key': False,
            'function': self.fetch_fred_data
        }
        
        # ===== 14. ECB DATA PORTAL (🌐 FREE - no key) =====
        self.sources['ecb'] = {
            'name': 'ECB Data Portal',
            'type': 'api',
            'enabled': True,
            'free': True,
            'needs_key': False,
            'function': self.fetch_ecb_data
        }
        
        # ===== 15. FOREXFACTORY API (🌐 FREE - community API) =====
        self.sources['forexfactory'] = {
            'name': 'ForexFactory',
            'type': 'api',
            'enabled': True,
            'free': True,
            'needs_key': False,  # Community API is free
            'function': self.fetch_forexfactory_api
        }
        
        # ===== 16. YAHOO FINANCE RSS (🌐 FREE - no key) =====
        self.sources['yahoo_finance'] = {
            'name': 'Yahoo Finance',
            'type': 'rss',
            'enabled': True,
            'free': True,
            'needs_key': False,
            'url': 'http://finance.yahoo.com/rss/',
            'function': self.fetch_rss_feed
        }
        
        # ===== 17. MARKET AUX (🔑 NEEDS API KEY - you have it now!) =====
        self.sources['marketaux'] = {
            'name': 'MarketAux',
            'type': 'api',
            'enabled': bool(MARKETAUX_TOKEN),  # Will be True now!
            'free': True,  # Free tier exists
            'needs_key': True,
            'function': self.fetch_marketaux,
            'api_key': MARKETAUX_TOKEN
        }
        
        # ===== 18. NEWSAPI (🔑 NEEDS API KEY - you have it) =====
        self.sources['newsapi'] = {
            'name': 'NewsAPI',
            'type': 'api',
            'enabled': bool(NEWSAPI_KEY),
            'free': False,
            'needs_key': True,
            'function': self.fetch_newsapi,
            'api_key': NEWSAPI_KEY
        }
        
        # ===== 19. GNEWS (🔑 NEEDS API KEY - you have it) =====
        self.sources['gnews'] = {
            'name': 'GNews',
            'type': 'api',
            'enabled': bool(GNEWS_KEY),
            'free': False,
            'needs_key': True,
            'function': self.fetch_gnews,
            'api_key': GNEWS_KEY
        }
        
        # ===== 20. RAPIDAPI (🔑 NEEDS API KEY - you have it) =====
        self.sources['rapidapi'] = {
            'name': 'RapidAPI Finance',
            'type': 'api',
            'enabled': bool(RAPIDAPI_KEY),
            'free': False,
            'needs_key': True,
            'function': self.fetch_rapidapi,
            'api_key': RAPIDAPI_KEY
        }
    
    # ===== FETCH METHODS =====
    
    def fetch_binance_announcements(self, limit=10):
        """Fetch Binance official announcements (🌐 FREE)"""
        try:
            params = {
                "type": "1",
                "pageSize": limit,
                "pageNo": 1
            }
            
            response = requests.get(BINANCE_ANNOUNCEMENTS_URL, params=params, timeout=15)
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
            logger.error(f"Binance error: {e}")
            return []
    
    def fetch_whale_alerts(self, min_value_usd=1000000):
        """Fetch large crypto transactions from Whale Alert (🔑 needs key)"""
        if not WHALE_ALERT_KEY:
            return []
        
        try:
            url = "https://api.whale-alert.io/v1/transactions"
            params = {
                "api_key": WHALE_ALERT_KEY,
                "min_value": min_value_usd,
                "limit": 20
            }
            
            response = requests.get(url, params=params, timeout=15)
            data = response.json()
            
            alerts = []
            for tx in data.get('transactions', []):
                value = tx.get('amount_usd', 0)
                sentiment = 0.1 if tx.get('symbol') in ['BTC', 'ETH'] else 0.05
                
                alerts.append({
                    'title': f"Whale Alert: {tx.get('amount', 0):.2f} {tx.get('symbol')} (${value:,.0f}) moved",
                    'sentiment': sentiment,
                    'value_usd': value,
                    'symbol': tx.get('symbol'),
                    'source': 'Whale Alert',
                    'url': tx.get('url', ''),
                    'date': datetime.fromtimestamp(tx.get('timestamp', 0)).isoformat()
                })
            
            return alerts
        except Exception as e:
            logger.error(f"Whale Alert error: {e}")
            return []
    
    def fetch_twitter_user(self, username, limit=10):
        """Fetch tweets from a Twitter user (🔑 needs key)"""
        if not TWITTER_BEARER_TOKEN:
            return []
        
        try:
            headers = {"Authorization": f"Bearer {TWITTER_BEARER_TOKEN}"}
            
            # Get user ID
            url = f"https://api.twitter.com/2/users/by/username/{username}"
            user_response = requests.get(url, headers=headers, timeout=15)
            
            if user_response.status_code != 200:
                return []
            
            user_id = user_response.json()['data']['id']
            
            # Get tweets
            tweets_url = f"https://api.twitter.com/2/users/{user_id}/tweets"
            params = {
                "max_results": limit,
                "tweet.fields": "created_at,public_metrics"
            }
            
            tweets_response = requests.get(tweets_url, headers=headers, params=params, timeout=15)
            
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
            logger.error(f"Twitter error for @{username}: {e}")
            return []
    
    def fetch_rss_feed(self, url, limit=10):
        """Fetch and parse RSS feed (🌐 FREE)"""
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
            logger.error(f"RSS error for {url}: {e}")
            return []
    
    def fetch_wsj_newsapi(self, days=1):
        """Fetch WSJ articles using NewsAPI (🔑 needs key)"""
        if not NEWSAPI_KEY:
            logger.warning("NewsAPI key not configured in .env")
            return []
        
        try:
            url = "https://newsapi.org/v2/everything"
            params = {
                "domains": "wsj.com",
                "from": (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d'),
                "sortBy": "relevancy",
                "language": "en",
                "apiKey": NEWSAPI_KEY,
                "pageSize": 10
            }
            
            response = requests.get(url, params=params, timeout=15)
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
                logger.warning(f"NewsAPI error: {data.get('message', 'Unknown error')}")
                return []
                
        except Exception as e:
            logger.warning(f"WSJ NewsAPI error: {e}")
            return []
    
    def fetch_alpha_vantage_news(self, tickers="FOREX,CRYPTO", limit=10):
        """Fetch news from Alpha Vantage (🔑 needs key)"""
        if not ALPHA_VANTAGE_API_KEY:
            return []
        
        try:
            url = "https://www.alphavantage.co/query"
            params = {
                'function': 'NEWS_SENTIMENT',
                'tickers': tickers,
                'apikey': ALPHA_VANTAGE_API_KEY,
                'limit': limit
            }
            
            response = requests.get(url, params=params, timeout=15)
            data = response.json()
            
            articles = []
            for item in data.get('feed', [])[:limit]:
                articles.append({
                    'title': item['title'],
                    'sentiment': float(item.get('overall_sentiment_score', 0)),
                    'url': item['url'],
                    'date': item['time_published'],
                    'source': item['source'],
                    'summary': item.get('summary', '')[:200]
                })
            
            return articles
        except Exception as e:
            logger.error(f"Alpha Vantage error: {e}")
            return []
    
    def fetch_fred_data(self, limit=10):
        """Fetch economic data from FRED (🌐 FREE)"""
        try:
            # Get latest releases
            url = "https://api.stlouisfed.org/fred/releases"
            params = {
                "api_key": "YOUR_API_KEY",  # Optional, free tier available
                "limit": limit
            }
            
            response = requests.get(url, params=params, timeout=15)
            # Parse XML response
            import xml.etree.ElementTree as ET
            root = ET.fromstring(response.text)
            
            articles = []
            for release in root.findall('release')[:limit]:
                name = release.find('name').text if release.find('name') is not None else ''
                
                articles.append({
                    'title': f"FRED Release: {name}",
                    'sentiment': 0,  # Neutral
                    'date': datetime.now().isoformat(),
                    'source': 'FRED',
                    'url': release.find('link').text if release.find('link') is not None else ''
                })
            
            return articles
        except Exception as e:
            logger.error(f"FRED error: {e}")
            return []
    
    def fetch_ecb_data(self, limit=10):
        """Fetch economic data from ECB (🌐 FREE)"""
        try:
            url = "https://data-api.ecb.europa.eu/service/data/EXR"
            params = {
                "format": "jsondata",
                "startPeriod": (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
            }
            
            response = requests.get(url, params=params, timeout=15)
            data = response.json()
            
            articles = []
            # Parse ECB data structure
            if 'dataSets' in data:
                articles.append({
                    'title': 'ECB Exchange Rate Data Updated',
                    'sentiment': 0,
                    'date': datetime.now().isoformat(),
                    'source': 'ECB',
                    'url': 'https://www.ecb.europa.eu/stats'
                })
            
            return articles
        except Exception as e:
            logger.error(f"ECB error: {e}")
            return []
    
    def fetch_forexfactory_api(self, limit=10):
        """
        Fetch economic calendar — tries 3 sources in order:
          1. ForexFactory official JSON feed (nfs.faireconomy.media)
          2. FXStreet economic calendar API
          3. jblanked community mirror (original — kept as last resort)
        Returns [] cleanly on total failure, never crashes the caller.
        """
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                          '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
        }

        def _parse_ff_official(data):
            """Parse ForexFactory official JSON format"""
            articles = []
            for event in (data or [])[:limit]:
                impact = event.get('impact', event.get('volatility', '')).lower()
                sentiment = -0.3 if impact == 'high' else -0.1 if impact == 'medium' else 0
                title = event.get('title', event.get('name', 'Economic Event'))
                country = event.get('country', event.get('countryCode', ''))
                articles.append({
                    'title': f"{title} ({country})" if country else title,
                    'sentiment': sentiment,
                    'date': event.get('date', event.get('dateUtc', '')),
                    'source': 'ForexFactory',
                    'impact': impact,
                    'actual': event.get('actual'),
                    'forecast': event.get('forecast', event.get('consensus'))
                })
            return articles

        # ── Source 1: ForexFactory official JSON ──
        try:
            url = 'https://nfs.faireconomy.media/ff_calendar_thisweek.json'
            r = requests.get(url, headers=headers, timeout=8)
            if r.status_code == 200 and r.text.strip().startswith('['):
                data = r.json()
                articles = _parse_ff_official(data)
                if articles:
                    return articles
        except Exception:
            pass

        # ── Source 2: ForexFactory next week (if current week empty) ──
        try:
            url = 'https://nfs.faireconomy.media/ff_calendar_nextweek.json'
            r = requests.get(url, headers=headers, timeout=8)
            if r.status_code == 200 and r.text.strip().startswith('['):
                data = r.json()
                articles = _parse_ff_official(data)
                if articles:
                    return articles
        except Exception:
            pass

        # ── Source 3: jblanked mirror (original) ──
        try:
            url = 'https://www.jblanked.com/news/api/calendar/'
            r = requests.get(url, headers=headers, timeout=8)
            if r.status_code == 200:
                text = r.text.strip()
                if text.startswith('[') or text.startswith('{'):
                    data = r.json()
                    if isinstance(data, dict):
                        data = data.get('data', data.get('events', []))
                    articles = _parse_ff_official(data)
                    if articles:
                        return articles
        except Exception:
            pass

        return []
    
    def fetch_newsapi(self, query="finance", limit=10):
        """Fetch from NewsAPI (🔑 needs key)"""
        if not NEWSAPI_KEY:
            return []
        
        try:
            url = "https://newsapi.org/v2/everything"
            params = {
                'q': query,
                'language': 'en',
                'sortBy': 'publishedAt',
                'pageSize': limit,
                'apiKey': NEWSAPI_KEY
            }
            
            response = requests.get(url, params=params, timeout=15)
            data = response.json()
            
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
                    'source': article.get('source', {}).get('name', 'NewsAPI'),
                    'author': article.get('author')
                })
            
            return articles
        except Exception as e:
            logger.error(f"NewsAPI error: {e}")
            return []
    
    def fetch_gnews(self, query="finance", limit=10):
        """Fetch from GNews API (🔑 needs key)"""
        if not GNEWS_KEY:
            return []
        
        try:
            url = "https://gnews.io/api/v4/search"
            params = {
                'q': query,
                'lang': 'en',
                'max': limit,
                'apikey': GNEWS_KEY
            }
            
            response = requests.get(url, params=params, timeout=15)
            data = response.json()
            
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
                    'source': article.get('source', {}).get('name', 'GNews'),
                    'description': article.get('description', '')
                })
            
            return articles
        except Exception as e:
            logger.error(f"GNews error: {e}")
            return []
    
    def fetch_rapidapi(self, symbol="AAPL", limit=10):
        """Fetch from RapidAPI Finance (🔑 needs key)"""
        if not RAPIDAPI_KEY:
            return []
        
        try:
            url = "https://real-time-finance-data.p.rapidapi.com/stock-news"
            headers = {
                "x-rapidapi-key": RAPIDAPI_KEY,
                "x-rapidapi-host": "real-time-finance-data.p.rapidapi.com"
            }
            params = {"symbol": symbol, "language": "en"}
            
            response = requests.get(url, headers=headers, params=params, timeout=15)
            data = response.json()
            
            articles = []
            if data.get('data', {}).get('news'):
                for item in data['data']['news'][:limit]:
                    title = item.get('title', '')
                    blob = TextBlob(title)
                    sentiment = blob.sentiment.polarity
                    
                    articles.append({
                        'title': title,
                        'sentiment': sentiment,
                        'url': item.get('link'),
                        'date': item.get('publishedAt'),
                        'source': 'RapidAPI Finance',
                        'summary': item.get('summary', '')
                    })
            
            return articles
        except Exception as e:
            logger.error(f"RapidAPI error: {e}")
            return []
    
    def fetch_marketaux(self, limit=10):
        """Fetch from MarketAux (🔑 using your free token)"""
        from config.config import MARKETAUX_TOKEN
        
        if not MARKETAUX_TOKEN:
            logger.warning("MarketAux token not configured in .env")
            return []
        
        try:
            url = "https://api.marketaux.com/v1/news/all"
            params = {
                'api_token': MARKETAUX_TOKEN,
                'language': 'en',
                'limit': limit
            }
            
            response = requests.get(url, params=params, timeout=15)
            data = response.json()
            
            articles = []
            for item in data.get('data', []):
                articles.append({
                    'title': item['title'],
                    'sentiment': item.get('sentiment_score', 0),
                    'url': item['url'],
                    'date': item['published_at'],
                    'source': item['source'],
                    'entities': item.get('entities', [])
                })
            
            logger.info(f"MarketAux: {len(articles)} articles")
            return articles
        except Exception as e:
            logger.error(f"MarketAux error: {e}")
            return []
    
    def fetch_by_symbol(self, symbol, limit=5):
        """Fetch symbol-specific news from TradingView (🌐 FREE)"""
        try:
            url = f"https://www.tradingview.com/feed/?symbol={symbol}"
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
                    'source': f'TradingView {symbol}',
                    'author': entry.get('author', '')
                })
            
            return articles
        except Exception as e:
            logger.error(f"TradingView symbol error: {e}")
            return []
    
    # ===== MAIN METHODS =====
    
    def fetch_all_sources(self, asset_type='general'):
        """
        Fetch from all enabled sources
        """
        all_articles = []
        enabled_sources = {name: src for name, src in self.sources.items() if src['enabled']}
        
        logger.info(f"Fetching from {len(enabled_sources)} news sources...")
        
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_source = {}
            
            for name, source in enabled_sources.items():
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
                    elif name == 'alpha_vantage':
                        future = executor.submit(source['function'], limit=10)
                    elif name == 'fred':
                        future = executor.submit(source['function'], 5)
                    elif name == 'ecb':
                        future = executor.submit(source['function'], 5)
                    elif name == 'forexfactory':
                        future = executor.submit(source['function'], 8)
                    elif name == 'newsapi':
                        future = executor.submit(source['function'], limit=5)
                    elif name == 'gnews':
                        future = executor.submit(source['function'], limit=5)
                    elif name == 'rapidapi':
                        future = executor.submit(source['function'], limit=5)
                    elif name == 'marketaux':
                        future = executor.submit(source['function'], 5)
                    else:
                        future = executor.submit(source['function'], 5)
                else:
                    continue
                
                future_to_source[future] = name
            
            # Collect results
            for future in as_completed(future_to_source):
                name = future_to_source[future]
                try:
                    articles = future.result(timeout=8)
                    if articles:
                        all_articles.extend(articles)
                        status = "✅"
                        if self.sources[name].get('needs_key', False):
                            status = "🔑"
                        elif self.sources[name].get('free', True):
                            status = "🌐"
                        logger.info(f"  {self.sources[name]['name']}: {len(articles)} articles")
                    else:
                        logger.debug(f"  {self.sources[name]['name']}: No articles")
                except Exception as e:
                    print(f"  ⚠️ {self.sources[name]['name']}: {str(e)[:50]}")
        
        # Show summary
        if all_articles:
            from collections import Counter
            sources = [a['source'] for a in all_articles]
            counts = Counter(sources)
            print(f"\n📊 Total: {len(all_articles)} articles from {len(counts)} sources")
            for source, count in counts.most_common(5):
                # Check if source needs key
                src_type = "🔑" if any(s.get('needs_key') for s in self.sources.values() if s['name'] == source) else "🌐"
                print(f"  {src_type} {source}: {count} articles")
        
        return all_articles
    
    def get_sentiment_summary(self, asset=None):
        """Get overall sentiment from all sources"""
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
        key_sources = []
        free_sources = []
        
        for a in articles:
            source = a.get('source', 'Unknown')
            source_counts[source] = source_counts.get(source, 0) + 1
            
            # Track which sources need keys
            needs_key = any(s.get('needs_key') for s in self.sources.values() if s['name'] == source)
            if needs_key:
                key_sources.append(source)
            else:
                free_sources.append(source)
        
        return {
            'overall_sentiment': self._interpret_sentiment(avg_sentiment),
            'score': avg_sentiment,
            'article_count': len(articles),
            'sources': source_counts,
            'key_sources': list(set(key_sources)),
            'free_sources': list(set(free_sources)),
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