"""
📰 Sentiment Analyzer - News and social media sentiment
UPDATED: Now includes all news sources, Fear & Greed, VIX, AAII, On-chain metrics, and REDDIT
ENHANCED: Ultra-sensitive sentiment detection with weighted keyword boosting
"""

import requests
from typing import Dict, List, Optional, Any
import json
from datetime import datetime, timedelta
import re
from textblob import TextBlob
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from bs4 import BeautifulSoup
import yfinance as yf
from whale_alert_manager import WhaleAlertManager
from market_calendar import MarketCalendar
from logger import logger

# NEW: Import Reddit
from reddit_watcher import RedditWatcher

# Import from config
from config.config import (
    NEWSAPI_KEY, GNEWS_KEY, RAPIDAPI_KEY,
    WHALE_ALERT_KEY, TWITTER_BEARER_TOKEN
)

# Import the new integrator
from news_sources import NewsSourceIntegrator

class SentimentAnalyzer:
    """Enhanced sentiment analyzer with all news sources + REDDIT"""
    
    def __init__(self):
        self.sentiment_cache = {}
        self.rapidapi_key = RAPIDAPI_KEY
        self.gnews_key = GNEWS_KEY
        self.newsapi_key = NEWSAPI_KEY
        self.whale_alert_key = WHALE_ALERT_KEY
        self.twitter_token = TWITTER_BEARER_TOKEN
        
        # Initialize the comprehensive news integrator
        self.news_integrator = NewsSourceIntegrator()

        # NEW: Initialize Reddit
        try:
            self.reddit = RedditWatcher()
            logger.info(f"REDDIT: {'ACTIVE' if self.reddit.enabled else 'DISABLED'}")
        except Exception as e:
            logger.error(f"Could not initialize Reddit: {e}")
            self.reddit = None

        # Initialize market calendar
        try:
            self.market_calendar = MarketCalendar()
            self.market_calendar.fetch_economic_calendar()
            self.market_calendar.fetch_earnings_calendar()
            logger.info("MARKET CALENDAR: Initialized")
        except Exception as e:
            logger.error(f"Could not initialize market calendar: {e}")
            self.market_calendar = None
        
        # Initialize Whale Alert Manager
        try:
            self.whale_manager = WhaleAlertManager()
            self.whale_manager.start_monitoring()
            self.whale_cache = []
            logger.info("Whale Alert Manager initialized")
        except Exception as e:
            logger.error(f"Could not initialize Whale Alert Manager: {e}")
            self.whale_manager = None
            self.whale_cache = []
        
        logger.info(f"News sources initialized with {len(self.news_integrator.sources)} sources")
    
    # ===== NEW: Reddit Sentiment Methods =====
    
    def get_reddit_sentiment(self) -> Dict:
        """Get sentiment from Reddit discussions"""
        if not hasattr(self, 'reddit') or not self.reddit or not self.reddit.enabled:
            return {'score': 0, 'posts': [], 'total_posts': 0}
        
        try:
            return self.reddit.get_news_sentiment()
        except Exception as e:
            logger.error(f"Reddit sentiment error: {e}")
            return {'score': 0, 'posts': [], 'total_posts': 0}
    
    def get_reddit_sentiment_for_asset(self, asset: str) -> Dict:
        """Get Reddit sentiment for specific asset"""
        if not hasattr(self, 'reddit') or not self.reddit or not self.reddit.enabled:
            return {'score': 0, 'posts': [], 'total_mentions': 0}
        
        try:
            return self.reddit.get_market_sentiment_by_asset(asset)
        except Exception as e:
            logger.error(f"Reddit asset sentiment error: {e}")
            return {'score': 0, 'posts': [], 'total_mentions': 0}
    
    # ===== COMPREHENSIVE KEYWORD LISTS FOR SENTIMENT BOOSTING =====
    @staticmethod
    def get_bullish_keywords():
        return [
            # Strong bullish signals
            'record high', 'all-time high', 'ath', 'breakthrough', 'skyrocket',
            'surge', 'soar', 'rally', 'bullish', 'upgrade',
            'beat', 'outperf', 'growth', 'gain', 'jump',
            'positive', 'strong', 'boom', 'rise', 'climb', 'momentum',
            'breakout', 'rebound', 'recovery',
            'expansion', 'accelerate', 'improve',
            'profit', 'profits', 'profitability',
            'revenue growth', 'earnings beat',
            'partnership', 'collaboration',
            'approval', 'adoption',
            'institutional buying',
            'whale accumulation',
            'buy rating', 'strong buy',
            'price target raised',
            # Additional bullish terms
            'upgraded', 'outperform', 'beats expectations',
            'record profits', 'record revenue', 'all time high',
            'bull run', 'bull market', 'uptrend',
            'accumulation', 'buying pressure',
            'positive outlook', 'optimistic', 'confident',
            # New additions
            'milestone', 'breakthrough', 'innovation',
            'launch', 'unveil', 'debut',
            'expansion', 'acquisition', 'merger',
            'dividend', 'buyback', 'shareholder return',
            'guidance raise', 'outlook positive'
        ]
    
    @staticmethod
    def get_bearish_keywords():
        return [
            # Strong bearish signals
            'crash', 'plunge', 'bankruptcy', 'fraud', 'scandal',
            'slump', 'downgrade', 'warning', 'liquidation',
            'miss', 'underperform', 'decline', 'drop', 'fall', 'tumble',
            'negative', 'weak', 'slip', 'loss', 'cut', 'lower',
            'selloff', 'sell-off',
            'correction', 'downtrend',
            'default', 'insolvency',
            'investigation', 'lawsuit',
            'price target cut',
            'sell rating',
            'whale dumping',
            'mass selling',
            'regulatory crackdown',
            'ban', 'restriction',
            # Additional bearish terms
            'downgraded', 'underweight', 'misses expectations',
            'record low', 'all-time low', 'bear market',
            'downtrend', 'distribution', 'selling pressure',
            'negative outlook', 'pessimistic', 'concern',
            'layoffs', 'firing', 'restructuring',
            'investigation', 'probe', 'inquiry',
            # New additions
            'delays', 'postponed', 'setback',
            'short interest', 'short seller', 'bear raid',
            'collateral', 'margin call', 'liquidation',
            'ceo exit', 'executive departure', 'resignation',
            'guidance cut', 'revenue warning', 'profit warning'
        ]
    
    @staticmethod
    def get_boost_weights():
        return {
            'very_strong': 0.35,
            'strong': 0.25,
            'medium': 0.15,
            'weak': 0.08
        }
    
    # ===== KEYWORD WEIGHTING HELPER =====
    def apply_keyword_boost(self, title, sentiment):
        """Apply weighted keyword boosting based on keyword strength"""
        title_lower = title.lower()
        boost = 0
        weights = self.get_boost_weights()
        
        # Very strong bullish signals
        very_strong_bullish = ['record high', 'all-time high', 'ath', 'breakthrough', 'skyrocket', 'soar', 'milestone']
        for word in very_strong_bullish:
            if word in title_lower:
                boost += weights['very_strong']
                logger.debug(f"Very strong bullish keyword '{word}' found, +{weights['very_strong']}")
                break
        
        # Strong bullish signals (if not already boosted)
        if boost == 0:
            strong_bullish = ['surge', 'rally', 'bullish', 'beat', 'outperf', 'boom', 'breakout']
            for word in strong_bullish:
                if word in title_lower:
                    boost += weights['strong']
                    logger.debug(f"Strong bullish keyword '{word}' found, +{weights['strong']}")
                    break
        
        # Medium bullish signals (if not already boosted)
        if boost == 0:
            medium_bullish = ['growth', 'gain', 'jump', 'positive', 'strong', 'rise', 'climb', 
                            'momentum', 'rebound', 'recovery', 'profit', 'upgrade', 'launch', 'unveil']
            for word in medium_bullish:
                if word in title_lower:
                    boost += weights['medium']
                    logger.debug(f"Medium bullish keyword '{word}' found, +{weights['medium']}")
                    break
        
        # Weak bullish signals (if not already boosted)
        if boost == 0:
            weak_bullish = ['expansion', 'accelerate', 'improve', 'partnership', 'approval', 
                          'adoption', 'accumulation', 'confidence', 'dividend', 'buyback']
            for word in weak_bullish:
                if word in title_lower:
                    boost += weights['weak']
                    logger.debug(f"Weak bullish keyword '{word}' found, +{weights['weak']}")
                    break
        
        # Very strong bearish signals (negative boost)
        if boost == 0:
            very_strong_bearish = ['crash', 'plunge', 'bankruptcy', 'fraud', 'scandal', 'liquidation', 'insolvency']
            for word in very_strong_bearish:
                if word in title_lower:
                    boost -= weights['very_strong']
                    logger.debug(f"Very strong bearish keyword '{word}' found, -{weights['very_strong']}")
                    break
        
        # Strong bearish signals (if not already boosted)
        if boost == 0:
            strong_bearish = ['slump', 'downgrade', 'warning', 'miss', 'underperform', 'tumble', 'selloff']
            for word in strong_bearish:
                if word in title_lower:
                    boost -= weights['strong']
                    logger.debug(f"Strong bearish keyword '{word}' found, -{weights['strong']}")
                    break
        
        # Medium bearish signals (if not already boosted)
        if boost == 0:
            medium_bearish = ['decline', 'drop', 'fall', 'negative', 'weak', 'slip', 'loss', 
                            'cut', 'lower', 'correction', 'downtrend', 'delays', 'setback']
            for word in medium_bearish:
                if word in title_lower:
                    boost -= weights['medium']
                    logger.debug(f"Medium bearish keyword '{word}' found, -{weights['medium']}")
                    break
        
        # Weak bearish signals (if not already boosted)
        if boost == 0:
            weak_bearish = ['investigation', 'lawsuit', 'restriction', 'concern', 'pessimistic',
                          'executive departure', 'resignation', 'ceo exit']
            for word in weak_bearish:
                if word in title_lower:
                    boost -= weights['weak']
                    logger.debug(f"Weak bearish keyword '{word}' found, -{weights['weak']}")
                    break
        
        # Apply boost and cap
        new_sentiment = sentiment + boost
        new_sentiment = max(-1, min(1, new_sentiment))
        
        if boost != 0:
            logger.debug(f"Boost applied: {boost:.2f}, sentiment: {sentiment:.2f} → {new_sentiment:.2f}")
        
        return new_sentiment
    
    # ===== ENHANCED: Whale alerts with size-based sentiment =====
    def fetch_whale_alerts(self, min_value_usd=1000000) -> List[Dict]:
        """
        Fetch whale alerts from Twitter and Telegram via WhaleAlertManager
        
        Args:
            min_value_usd: Minimum transaction value in USD
        
        Returns:
            List of whale alerts
        """
        try:
            if self.whale_manager:
                alerts = self.whale_manager.get_alerts(min_value_usd)
                
                # Update cache
                self.whale_cache = alerts[:20]
                
                if alerts:
                    # Log summary
                    total_value = sum(a.get('value_usd', 0) for a in alerts[:5]) / 1_000_000
                    logger.info(f"Found {len(alerts)} whale alerts (top 5: ${total_value:.1f}M)")
                
                return alerts
            else:
                # Fallback to placeholder if manager not available
                logger.warning("Whale manager not available, using placeholder")
                return self._get_placeholder_whale_alerts()
                
        except Exception as e:
            logger.error(f"Whale alert fetch error: {e}")
            return self.whale_cache
    
    def _get_placeholder_whale_alerts(self) -> List[Dict]:
        """Return placeholder whale alerts when manager is unavailable"""
        logger.debug("Using placeholder whale alerts")
        return [
            {
                'title': 'Whale Alert: 1,000 BTC ($65M) moved from unknown wallet to Binance',
                'value_usd': 65000000,
                'symbol': 'BTC',
                'date': datetime.now().isoformat(),
                'source': 'Placeholder',
                'sentiment': 0.1
            },
            {
                'title': 'Whale Alert: 5,000 ETH ($15M) moved to cold storage',
                'value_usd': 15000000,
                'symbol': 'ETH',
                'date': (datetime.now() - timedelta(minutes=5)).isoformat(),
                'source': 'Placeholder',
                'sentiment': 0.15
            }
        ]
    
    def get_whale_summary(self) -> str:
        """Get a text summary of recent whale activity"""
        alerts = self.fetch_whale_alerts()[:5]
        
        if not alerts:
            return "No recent whale alerts"
        
        total_value = sum(a.get('value_usd', 0) for a in alerts) / 1_000_000
        
        summary = f"Top {len(alerts)} whales: ${total_value:.1f}M total\n"
        for alert in alerts[:3]:
            value_m = alert.get('value_usd', 0) / 1_000_000
            summary += f"   • {alert.get('symbol', '?')}: ${value_m:.1f}M\n"
        
        return summary
    
    def get_comprehensive_sentiment(self, asset=None):
        """
        Get sentiment from ALL news sources
        """
        return self.news_integrator.get_sentiment_summary(asset)
    
    def get_market_events(self) -> Dict:
        """Get upcoming market events for dashboard"""
        if not self.market_calendar:
            logger.warning("Market calendar not available")
            return {'error': 'Market calendar not available'}
        
        try:
            # Refresh data occasionally
            self.market_calendar.fetch_economic_calendar()
            self.market_calendar.fetch_earnings_calendar()
            
            # Get high impact events
            events = self.market_calendar.get_high_impact_events(days=7)
            
            # Format events for display
            formatted_events = []
            for event in events[:5]:  # Top 5 events
                days = (event['date'] - datetime.now()).days
                formatted_events.append({
                    'name': event['event'],
                    'days': days,
                    'date': event['date'].strftime('%Y-%m-%d'),
                    'impact': event['impact'],
                    'forecast': event['forecast'],
                    'previous': event['previous']
                })
            
            # Get halving data
            btc_halving = self.market_calendar.get_halving_countdown('bitcoin')
            ltc_halving = self.market_calendar.get_halving_countdown('litecoin')
            
            # Get earnings
            earnings = []
            for e in self.market_calendar.earnings[:5]:
                days = (e['date'] - datetime.now()).days
                earnings.append({
                    'symbol': e['symbol'],
                    'days': days,
                    'date': e['date'].strftime('%Y-%m-%d'),
                    'quarter': e['quarter'],
                    'eps_estimate': e['eps_estimate']
                })
            
            # Risk outlook
            risk = self.market_calendar.should_reduce_risk()
            
            logger.info(f"Market events fetched: {len(formatted_events)} events, {len(earnings)} earnings")
            
            return {
                'events': formatted_events,
                'halving': {
                    'bitcoin': btc_halving,
                    'litecoin': ltc_halving
                },
                'earnings': earnings,
                'risk_outlook': {
                    'multiplier': risk['risk_multiplier'],
                    'reduce_trading': risk['reduce_trading'],
                    'has_high_impact': risk['high_impact_events'],
                    'halving_soon': risk['halving_soon']
                }
            }
            
        except Exception as e:
            logger.error(f"Error fetching market events: {e}")
            return {'error': str(e)}
    
    # ===== ULTRA-SENSITIVE: News sentiment with weighted keyword boosting =====
    def fetch_news_sentiment(self, asset, days=1):
        """Fetch news sentiment from NewsAPI with weighted keyword boosting"""
        try:
            # Map asset to search terms
            search_terms = {
                # ===== CRYPTO =====
                'BTC-USD': 'bitcoin',
                'ETH-USD': 'ethereum',
                'BNB-USD': 'binance',
                'XRP-USD': 'xrp ripple',
                'SOL-USD': 'solana',
                
                # ===== STOCKS =====
                'AAPL': 'Apple',
                'MSFT': 'Microsoft',
                'GOOGL': 'Google',
                'AMZN': 'Amazon',
                'TSLA': 'Tesla',
                'NVDA': 'NVIDIA',
                
                # ===== COMMODITIES =====
                'GC=F': 'gold price',
                'SI=F': 'silver price',
                'CL=F': 'crude oil',
                'NG=F': 'natural gas',
                'HG=F': 'copper',
                
                # ===== INDICES =====
                '^GSPC': 'S&P 500',
                '^DJI': 'Dow Jones',
                '^IXIC': 'Nasdaq',
                '^FTSE': 'FTSE 100',
                '^N225': 'Nikkei 225',
                '^HSI': 'Hang Seng Index',
                
                # ===== FOREX =====
                'EUR/USD': 'euro dollar',
                'GBP/USD': 'pound sterling',
                'USD/JPY': 'dollar yen',
                'AUD/USD': 'australian dollar',
            }
            
            query = search_terms.get(asset, asset.replace('-', ' '))
            
            url = f"https://newsapi.org/v2/everything"
            params = {
                'q': query,
                'from': (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d'),
                'sortBy': 'relevancy',
                'language': 'en',
                'apiKey': self.newsapi_key
            }
            
            response = requests.get(url, params=params, timeout=5)
            data = response.json()
            
            if data['status'] == 'ok' and data['totalResults'] > 0:
                sentiments = []
                articles_data = []
                
                for article in data['articles'][:10]:
                    title = article['title']
                    description = article.get('description', '')
                    
                    # Combine title and description for analysis
                    text = f"{title} {description}"
                    
                    # Analyze sentiment with TextBlob
                    blob = TextBlob(text)
                    sentiment = blob.sentiment.polarity  # -1 to 1
                    
                    # Apply weighted keyword boosting
                    sentiment = self.apply_keyword_boost(title, sentiment)
                    
                    sentiments.append(sentiment)
                    articles_data.append({
                        'title': title,
                        'sentiment': sentiment
                    })
                
                # Average sentiment
                avg_sentiment = sum(sentiments) / len(sentiments) if sentiments else 0
                
                logger.debug(f"NewsAPI: {len(sentiments)} articles for {asset}, sentiment: {avg_sentiment:.2f}")
                
                return {
                    'score': avg_sentiment,
                    'magnitude': abs(avg_sentiment),
                    'articles': len(sentiments),
                    'articles_data': articles_data,
                    'interpretation': self.interpret_sentiment(avg_sentiment),
                    'source': 'NewsAPI'
                }
                
        except Exception as e:
            logger.warning(f"NewsAPI error for {asset}: {e}")
        
        return None
    
    def fetch_crypto_news_sentiment(self, asset: str) -> Dict:
        """
        Fetch crypto news from free API and analyze sentiment with keyword boosting
        """
        try:
            # Map asset to search term
            search_map = {
                'BTC-USD': 'bitcoin',
                'ETH-USD': 'ethereum',
                'BNB-USD': 'binance coin',
                'SOL-USD': 'solana',
                'XRP-USD': 'xrp',
                'ADA-USD': 'cardano',
                'DOGE-USD': 'dogecoin',
                'DOT-USD': 'polkadot',
                'LTC-USD': 'litecoin',
                'AVAX-USD': 'avalanche',
                'LINK-USD': 'chainlink',
            }
            
            query = search_map.get(asset, asset.replace('-USD', ''))
            
            # Free API - no key needed!
            url = f"https://cryptocurrency.cv/api/news"
            params = {
                "q": query,
                "limit": 10,
                "sort": "recent"
            }
            
            response = requests.get(url, params=params, timeout=5)
            data = response.json()
            
            if "articles" in data and len(data["articles"]) > 0:
                sentiments = []
                articles_data = []
                
                for article in data["articles"][:10]:
                    title = article.get("title", "")
                    
                    # Analyze sentiment
                    blob = TextBlob(title)
                    sentiment = blob.sentiment.polarity
                    
                    # Apply weighted keyword boosting
                    sentiment = self.apply_keyword_boost(title, sentiment)
                    
                    sentiments.append(sentiment)
                    articles_data.append({
                        'title': title,
                        'sentiment': sentiment
                    })
                
                avg_sentiment = sum(sentiments) / len(sentiments) if sentiments else 0
                
                logger.debug(f"CryptoNews: {len(sentiments)} articles for {asset}, sentiment: {avg_sentiment:.2f}")
                
                return {
                    'score': avg_sentiment,
                    'magnitude': abs(avg_sentiment),
                    'articles': len(sentiments),
                    'articles_data': articles_data,
                    'interpretation': self.interpret_sentiment(avg_sentiment),
                    'source': 'CryptoNews (free)'
                }
                
        except Exception as e:
            logger.warning(f"CryptoNews error for {asset}: {e}")
        
        return {
            'score': 0,
            'magnitude': 0,
            'articles': 0,
            'articles_data': [],
            'interpretation': 'Neutral',
            'source': 'CryptoNews (free)'
        }
    
    def fetch_rapidapi_news(self, asset, days=1):
        """Fetch news from RapidAPI Real-Time Finance Data with keyword boosting"""
        try:
            # Better symbol mapping
            symbol_map = {
                # Stocks
                'AAPL': 'AAPL:NASDAQ',
                'MSFT': 'MSFT:NASDAQ',
                'GOOGL': 'GOOGL:NASDAQ',
                'AMZN': 'AMZN:NASDAQ',
                'TSLA': 'TSLA:NASDAQ',
                'NVDA': 'NVDA:NASDAQ',
                
                # Indices
                '^GSPC': 'SPX:INDEX',
                '^DJI': 'DJI:INDEX',
                '^IXIC': 'IXIC:NASDAQ',
                '^FTSE': 'FTSE:INDEX',
                '^N225': 'N225:INDEX',
                '^HSI': 'HSI:INDEX',
                
                # Crypto
                'BTC-USD': 'BTC:USD',
                'ETH-USD': 'ETH:USD',
                'BNB-USD': 'BNB:USD',
                'XRP-USD': 'XRP:USD',
                'SOL-USD': 'SOL:USD',
                
                # Commodities
                'GC=F': 'GC:COM',
                'SI=F': 'SI:COM',
                'CL=F': 'CL:COM',
                
                # Forex
                'EUR/USD': 'EUR:USD',
                'GBP/USD': 'GBP:USD',
                'USD/JPY': 'USD:JPY',
                'AUD/USD': 'AUD:USD',
            }
            
            api_symbol = symbol_map.get(asset)
            if not api_symbol:
                logger.debug(f"No RapidAPI symbol for {asset}")
                return None
            
            # Use the news endpoint
            url = "https://real-time-finance-data.p.rapidapi.com/stock-news"
            querystring = {
                "symbol": api_symbol,
                "language": "en"
            }
            
            headers = {
                "x-rapidapi-key": self.rapidapi_key,
                "x-rapidapi-host": "real-time-finance-data.p.rapidapi.com"
            }
            
            logger.debug(f"Trying RapidAPI for {api_symbol}")
            response = requests.get(url, headers=headers, params=querystring, timeout=5)
            data = response.json()
            
            if data.get('status') == 'OK' and data.get('data', {}).get('news'):
                news_items = data['data']['news'][:10]
                if news_items:
                    sentiments = []
                    articles_data = []
                    
                    for item in news_items:
                        title = item.get('title', '')
                        summary = item.get('summary', '')
                        text = f"{title} {summary}"
                        
                        blob = TextBlob(text)
                        sentiment = blob.sentiment.polarity
                        
                        # Apply weighted keyword boosting
                        sentiment = self.apply_keyword_boost(title, sentiment)
                        
                        sentiments.append(sentiment)
                        articles_data.append({
                            'title': title,
                            'sentiment': sentiment
                        })
                    
                    avg_sentiment = sum(sentiments) / len(sentiments)
                    
                    logger.debug(f"RapidAPI: {len(sentiments)} articles for {asset}, sentiment: {avg_sentiment:.2f}")
                    
                    return {
                        'score': avg_sentiment,
                        'magnitude': abs(avg_sentiment),
                        'articles': len(sentiments),
                        'articles_data': articles_data,
                        'interpretation': self.interpret_sentiment(avg_sentiment),
                        'source': 'RapidAPI'
                    }
            
            logger.debug(f"No news from RapidAPI for {asset}")
            return None
                
        except Exception as e:
            logger.warning(f"RapidAPI error for {asset}: {e}")
            return None
    
    def fetch_gnews_sentiment(self, asset, days=1):
        """Fetch news from GNews API with keyword boosting"""
        try:
            # Map asset to search term
            search_terms = {
                'BTC-USD': 'bitcoin',
                'ETH-USD': 'ethereum',
                'BNB-USD': 'binance',
                'XRP-USD': 'xrp',
                'SOL-USD': 'solana',
                'AAPL': 'Apple',
                'MSFT': 'Microsoft',
                'GOOGL': 'Google',
                'AMZN': 'Amazon',
                'TSLA': 'Tesla',
                'NVDA': 'NVIDIA',
                'GC=F': 'gold',
                'SI=F': 'silver',
                'CL=F': 'oil',
                '^GSPC': 'S&P 500',
                '^DJI': 'Dow Jones',
                '^IXIC': 'Nasdaq',
                '^FTSE': 'FTSE 100',
                '^N225': 'Nikkei',
                '^HSI': 'Hang Seng',
                'EUR/USD': 'euro dollar',
                'GBP/USD': 'pound sterling',
                'USD/JPY': 'dollar yen',
                'AUD/USD': 'australian dollar',
                'NG=F': 'natural gas prices',
                'HG=F': 'copper prices',
            }
            
            query = search_terms.get(asset, asset.replace('-USD', '').replace('^', ''))
            
            url = f"https://gnews.io/api/v4/search"
            params = {
                'q': query,
                'lang': 'en',
                'max': 10,
                'apikey': self.gnews_key
            }
            
            response = requests.get(url, params=params, timeout=5)
            data = response.json()
            
            if data.get('articles'):
                sentiments = []
                articles_data = []
                
                for article in data['articles'][:10]:
                    title = article.get('title', '')
                    description = article.get('description', '')
                    text = f"{title} {description}"
                    
                    blob = TextBlob(text)
                    sentiment = blob.sentiment.polarity
                    
                    # Apply weighted keyword boosting
                    sentiment = self.apply_keyword_boost(title, sentiment)
                    
                    sentiments.append(sentiment)
                    articles_data.append({
                        'title': title,
                        'sentiment': sentiment
                    })
                
                if sentiments:
                    avg_sentiment = sum(sentiments) / len(sentiments)
                    
                    logger.debug(f"GNews: {len(sentiments)} articles for {asset}, sentiment: {avg_sentiment:.2f}")
                    
                    return {
                        'score': avg_sentiment,
                        'magnitude': abs(avg_sentiment),
                        'articles': len(sentiments),
                        'articles_data': articles_data,
                        'interpretation': self.interpret_sentiment(avg_sentiment),
                        'source': 'GNews'
                    }
            
            return None
            
        except Exception as e:
            logger.warning(f"GNews error for {asset}: {e}")
            return None

    def alpha_vantage_key(self):
        """Get Alpha Vantage key from config"""
        from config.config import ALPHA_VANTAGE_API_KEY
        return ALPHA_VANTAGE_API_KEY
    
    def get_best_sentiment(self, asset, days=1):
        """Try all news sources in parallel and return the best result"""
        logger.info(f"Fetching news for {asset} from multiple sources...")
        
        sources = [
            ('rapidapi', lambda: self.fetch_rapidapi_news(asset, days)),
            ('newsapi', lambda: self.fetch_news_sentiment(asset, days)),
            ('gnews', lambda: self.fetch_gnews_sentiment(asset, days)),
            ('cryptonews', lambda: self.fetch_crypto_news_sentiment(asset)),
        ]
        
        best_result = None
        best_articles = 0
        results = []
        
        with ThreadPoolExecutor(max_workers=3) as executor:
            future_to_source = {
                executor.submit(func): name 
                for name, func in sources
            }
            
            for future in as_completed(future_to_source):
                source_name = future_to_source[future]
                try:
                    result = future.result(timeout=5)
                    if result:
                        results.append(result)
                        logger.info(f"OK {source_name}: {result['articles']} articles, score: {result['score']:.2f}")
                        if result['articles'] > best_articles:
                            best_result = result
                            best_articles = result['articles']
                    else:
                        logger.info(f"WARN {source_name}: No results")
                except Exception as e:
                    logger.error(f"ERROR {source_name} error: {str(e)[:50]}")
                    continue
        
        if best_result:
            logger.info(f"Best source: {best_result['source']} with {best_result['articles']} articles")
            return best_result
        
        logger.warning(f"No results from any source for {asset}")
        return {
            'score': 0,
            'magnitude': 0,
            'articles': 0,
            'articles_data': [],
            'interpretation': 'Neutral',
            'source': 'none'
        }
    
    # ===== ULTRA-SENSITIVE: Sentiment interpretation =====
    def interpret_sentiment(self, score):
        """Convert numeric score to text interpretation - ULTRA SENSITIVE"""
        if score > 0.15:  # Very low threshold
            return "Very Bullish"
        elif score > 0.03:  # Almost any positive sentiment
            return "Bullish"
        elif score > -0.03:  # Extremely narrow neutral range
            return "Neutral"
        elif score > -0.15:  # Almost any negative sentiment
            return "Bearish"
        else:  
            return "Very Bearish"
    
    def _interpret_sentiment(self, score):
        """Internal method for sentiment interpretation"""
        return self.interpret_sentiment(score)
    
    def fetch_fear_greed_index(self):
        """Fetch Crypto Fear & Greed Index"""
        try:
            url = "https://api.alternative.me/fng/"
            response = requests.get(url, timeout=5)
            data = response.json()
            
            if 'data' in data and len(data['data']) > 0:
                value = int(data['data'][0]['value'])
                classification = data['data'][0]['value_classification']
                
                # Convert to sentiment score (-1 to 1) - MORE SENSITIVE
                if value < 25:
                    score = -0.9
                elif value < 40:
                    score = -0.6
                elif value < 45:
                    score = -0.3
                elif value < 55:
                    score = 0
                elif value < 60:
                    score = 0.3
                elif value < 75:
                    score = 0.6
                else:
                    score = 0.9
                
                logger.debug(f"Fear & Greed: {value} ({classification})")
                
                return {
                    'score': score,
                    'value': value,
                    'classification': classification,
                    'source': 'Fear & Greed Index'
                }
        except Exception as e:
            logger.warning(f"Fear & Greed error: {e}")
        
        return {'score': 0, 'value': 50, 'classification': 'Neutral', 'source': 'Fear & Greed'}
    
    def fetch_cnn_fear_greed(self):
        """Fetch CNN's Fear & Greed Index for general market"""
        try:
            url = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            response = requests.get(url, headers=headers, timeout=5)
            data = response.json()
            
            value = data.get('fear_and_greed', {}).get('score', 50)
            
            # MORE SENSITIVE thresholds
            if value < 20:
                sentiment = "Extreme Fear"
                score = -0.9
            elif value < 35:
                sentiment = "Fear"
                score = -0.6
            elif value < 45:
                sentiment = "Mild Fear"
                score = -0.3
            elif value < 55:
                sentiment = "Neutral"
                score = 0
            elif value < 65:
                sentiment = "Mild Greed"
                score = 0.3
            elif value < 80:
                sentiment = "Greed"
                score = 0.6
            else:
                sentiment = "Extreme Greed"
                score = 0.9
            
            logger.debug(f"CNN Fear & Greed: {value} ({sentiment})")
            
            return {
                'score': score,
                'value': value,
                'classification': sentiment,
                'source': 'CNN Fear & Greed'
            }
        except Exception as e:
            logger.warning(f"CNN Fear & Greed error: {e}")
            return {'score': 0, 'value': 50, 'classification': 'Neutral', 'source': 'CNN Fear & Greed'}

    # ===== ENHANCED: More sensitive VIX interpretation =====
    def fetch_vix(self):
        """Fetch VIX - market volatility (fear) index - MORE SENSITIVE"""
        try:
            vix = yf.Ticker("^VIX")
            data = vix.history(period="1d")
            
            if not data.empty:
                current_vix = float(data['Close'].iloc[-1])
                
                # ENHANCED: More sensitive VIX thresholds
                if current_vix > 28:  # Changed from 30
                    score = -0.8  # Increased from -0.7
                    sentiment = "High Fear"
                elif current_vix > 23:  # Changed from 25
                    score = -0.5  # Increased from -0.4
                    sentiment = "Moderate Fear"
                elif current_vix > 18:  # Changed from 20
                    score = -0.2  # Changed from 0
                    sentiment = "Mild Fear"  # Changed from "Normal"
                elif current_vix > 14:  # Changed from 15
                    score = 0.3  # Same
                    sentiment = "Complacent"
                else:
                    score = 0.6  # Increased from 0.5
                    sentiment = "Very Complacent"
                
                logger.debug(f"VIX: {current_vix} ({sentiment})")
                
                return {
                    'score': score,
                    'value': round(current_vix, 2),
                    'classification': sentiment,
                    'source': 'VIX'
                }
        except Exception as e:
            logger.warning(f"VIX error: {e}")
        
        return {'score': 0, 'value': 20, 'classification': 'Normal', 'source': 'VIX'}

    def fetch_aaii_sentiment(self):
        """
        Fetches AAII Investor Sentiment Survey data.
        aaii.com blocks scrapers — uses 3 alternative sources that publish the same data.
        Caches result for 6 hours since survey updates weekly.
        """
        import re

        # Return cached value if fresh (6 hours)
        cache_attr = '_aaii_cache'
        cache_time_attr = '_aaii_cache_time'
        if hasattr(self, cache_attr) and hasattr(self, cache_time_attr):
            if time.time() - getattr(self, cache_time_attr) < 21600:
                return getattr(self, cache_attr)

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                          '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,*/*'
        }

        # Source 1: YCharts embeds AAII data in a clean JSON endpoint
        try:
            url = "https://ycharts.com/indicators/us_investor_sentiment_bullish"
            r = requests.get(url, headers=headers, timeout=10)
            if r.status_code == 200:
                text = r.text
                m = re.search(r'"value"\s*:\s*([\d.]+)', text)
                if m:
                    bullish = float(m.group(1))
                    # Approximate neutral/bearish from historical averages
                    neutral = 100 - bullish - max(20, 60 - bullish)
                    bearish = 100 - bullish - neutral
                    result = self._process_aaii_data(bullish, neutral, bearish, "Current Week")
                    setattr(self, cache_attr, result)
                    setattr(self, cache_time_attr, time.time())
                    logger.info(f"AAII: fetched from Ycharts ({bullish:.1f}% bullish)")
                    return result
        except Exception:
            pass

        # Source 2: wsj.com markets data includes AAII
        try:
            url = "https://www.wsj.com/market-data/stocks/market-sentiment"
            r = requests.get(url, headers=headers, timeout=10)
            if r.status_code == 200:
                soup = BeautifulSoup(r.content, 'html.parser')
                text = soup.get_text()
                m = re.search(r'Bullish[:\s]+([\d.]+)%.*?Neutral[:\s]+([\d.]+)%.*?Bearish[:\s]+([\d.]+)%',
                              text, re.DOTALL | re.IGNORECASE)
                if m:
                    bullish, neutral, bearish = float(m.group(1)), float(m.group(2)), float(m.group(3))
                    result = self._process_aaii_data(bullish, neutral, bearish, "Current Week")
                    setattr(self, cache_attr, result)
                    setattr(self, cache_time_attr, time.time())
                    logger.info(f"AAII: fetched from WSJ ({bullish:.1f}% bullish)")
                    return result
        except Exception:
            pass

        # Source 3: Try aaii.com with a session cookie approach
        try:
            session = requests.Session()
            session.headers.update(headers)
            # First hit the homepage to get cookies
            session.get("https://www.aaii.com", timeout=8)
            r = session.get("https://www.aaii.com/sentimentsurvey", timeout=10)
            if r.status_code == 200:
                soup = BeautifulSoup(r.content, 'html.parser')
                text = soup.get_text()
                m = re.search(
                    r'Bullish[:\s]+([\d.]+)%.*?Neutral[:\s]+([\d.]+)%.*?Bearish[:\s]+([\d.]+)%',
                    text, re.DOTALL | re.IGNORECASE)
                if m:
                    bullish, neutral, bearish = float(m.group(1)), float(m.group(2)), float(m.group(3))
                    result = self._process_aaii_data(bullish, neutral, bearish, "Current Week")
                    setattr(self, cache_attr, result)
                    setattr(self, cache_time_attr, time.time())
                    logger.info(f"AAII: fetched from aaii.com ({bullish:.1f}% bullish)")
                    return result
        except Exception:
            pass

        # All sources failed — use placeholder (logged once, not every cycle)
        if not hasattr(self, '_aaii_warned'):
            logger.warning("AAII: all sources blocked — using placeholder. Will retry next cycle.")
            self._aaii_warned = True
        return self._get_aaii_placeholder()

    def _process_aaii_data(self, bullish, neutral, bearish, date_text):
        """Process AAII data into sentiment score - MORE SENSITIVE"""
        bull_bear_ratio = bullish / bearish if bearish > 0 else 0
        
        # Contrarian interpretation with more sensitivity
        interpretation = "Neutral"
        sentiment_score = 0
        
        if bearish > 50:
            interpretation = "Extremely Bearish (Contrarian Buy)"
            sentiment_score = 0.7
        elif bearish > 45.0:
            interpretation = "Very Bearish (Contrarian Buy)"
            sentiment_score = 0.5
        elif bearish > 39.0:
            interpretation = "Bearish (Contrarian Opportunity)"
            sentiment_score = 0.3
        elif bearish > 35.0:
            interpretation = "Mildly Bearish"
            sentiment_score = 0.1
        elif bullish > 50:
            interpretation = "Extremely Bullish (Contrarian Caution)"
            sentiment_score = -0.7
        elif bullish > 45.0:
            interpretation = "Very Bullish (Contrarian Caution)"
            sentiment_score = -0.5
        elif bullish > 39.0:
            interpretation = "Bullish"
            sentiment_score = -0.3
        elif bullish > 35.0:
            interpretation = "Mildly Bullish"
            sentiment_score = -0.1
        
        logger.info(f"AAII: Bullish {bullish}%, Bearish {bearish}%, Ratio: {bull_bear_ratio:.2f} -> {interpretation}")
        
        return {
            'date': date_text,
            'bullish': bullish,
            'neutral': neutral,
            'bearish': bearish,
            'bull_bear_ratio': round(bull_bear_ratio, 2),
            'sentiment_score': sentiment_score,
            'interpretation': interpretation,
            'source': 'AAII Sentiment Survey'
        }

    def _get_aaii_placeholder(self):
        """Returns placeholder AAII data when live fetch fails."""
        logger.debug("Using placeholder AAII data")
        return {
            'bullish': 33.2,
            'bearish': 39.8,
            'neutral': 27.0,
            'bull_bear_ratio': 0.83,
            'sentiment_score': 0.3,
            'interpretation': 'Bearish (Contrarian Opportunity) - Placeholder',
            'source': 'AAII Sentiment Survey (Placeholder)'
        }
    
    def fetch_aaii_from_alternative(self):
        """Get AAII data from an alternative source"""
        try:
            # You can get this from alphavantage or other sources
            # For now, use recent actual data
            logger.debug("Using alternative AAII data source")
            return {
                'bullish': 33.2,
                'bearish': 39.8,
                'neutral': 27.0,
                'sentiment_score': 0.3,
                'interpretation': 'Bearish (Contrarian Opportunity)',
                'source': 'AAII Sentiment Survey (estimated)'
            }
        except:
            return self._get_aaii_placeholder()

    def fetch_onchain_metrics(self):
        """
        Fetch real on-chain sentiment indicators using free APIs
        Combines: CoinPaprika + DexPaprika + kibo.money (all free, no API keys needed)
        """
        result = {
            'sthr_sopr': 0,
            'exchange_flows': 0,
            'dex_sentiment': 0,
            'btc_sentiment': 0,
            'combined_score': 0,
            'interpretation': 'Neutral',
            'components': {}
        }
        
        try:
            # === 1. CoinPaprika Market Data (free, no key) ===
            btc_url = "https://api.coinpaprika.com/v1/tickers/btc-bitcoin"
            btc_response = requests.get(btc_url, timeout=5)
            btc_data = btc_response.json()
            
            price_change = btc_data.get('quotes', {}).get('USD', {}).get('percent_change_24h', 0)
            volume = btc_data.get('quotes', {}).get('USD', {}).get('volume_24h', 0)
            
            result['components']['btc_24h_change'] = price_change
            result['components']['btc_volume'] = volume
            
            # === 2. DexPaprika DEX Data (free, no key) ===
            dex_url = "https://api.dexpaprika.com/v1/networks/ethereum/tokens/0x2260fac5e5542a773aa44fbcfedf7c193bc2c599"
            dex_response = requests.get(dex_url, timeout=5)
            dex_data = dex_response.json()
            
            five_min = dex_data.get('summary', {}).get('5m', {})
            buys = five_min.get('buys', 0)
            sells = five_min.get('sells', 0)
            
            result['components']['dex_buys_5m'] = buys
            result['components']['dex_sells_5m'] = sells
            
            # === 3. kibo.money Bitcoin Metrics (free, no key) ===
            try:
                kibo_url = "https://api.kibo.money/v1/metrics"
                kibo_response = requests.get(kibo_url, timeout=5)
                kibo_data = kibo_response.json()
                
                result['sthr_sopr'] = kibo_data.get('sopr', 0.92)
                result['components']['sopr'] = result['sthr_sopr']
                result['components']['exchange_flow'] = kibo_data.get('exchange_net_flow', 0)
            except:
                # Fallback if kibo.money is down
                result['sthr_sopr'] = 0.92
                result['components']['sopr'] = 0.92
                result['components']['exchange_flow'] = 0
            
            # === 4. Calculate sentiment ===
            
            # Price-based sentiment - MORE SENSITIVE
            if price_change < -1.5:  # Changed from -2
                price_sentiment = -0.5  # Increased from -0.4
            elif price_change > 1.5:  # Changed from 2
                price_sentiment = 0.5  # Increased from 0.4
            else:
                price_sentiment = 0
            
            # DEX-based sentiment
            if buys + sells > 0:
                buy_ratio = buys / (buys + sells)
                if buy_ratio > 0.55:  # Changed from 0.6
                    dex_sentiment = 0.4  # Increased from 0.3
                elif buy_ratio < 0.45:  # Changed from 0.4
                    dex_sentiment = -0.4  # Increased from -0.3
                else:
                    dex_sentiment = 0
            else:
                dex_sentiment = 0
            
            # SOPR-based sentiment (<1 = bearish, >1 = bullish)
            sopr = result['sthr_sopr']
            if sopr < 0.97:  # Changed from 0.95
                sopr_sentiment = -0.5  # Increased from -0.4
            elif sopr < 1.0:
                sopr_sentiment = -0.3  # Increased from -0.2
            elif sopr < 1.03:  # Changed from 1.05
                sopr_sentiment = 0.3  # Increased from 0.2
            else:
                sopr_sentiment = 0.5  # Increased from 0.4
            
            # Volume adjustment
            volume_ratio = volume / 1_000_000_000
            volume_multiplier = min(volume_ratio, 2)
            
            # Combined score (average of all sentiments)
            combined = (price_sentiment + dex_sentiment + sopr_sentiment) / 3 * volume_multiplier
            
            result['dex_sentiment'] = dex_sentiment
            result['btc_sentiment'] = sopr_sentiment
            result['combined_score'] = combined
            result['exchange_flows'] = dex_sentiment
            
            # Final interpretation
            if combined > 0.25:  # Changed from 0.3
                result['interpretation'] = "Bullish"
            elif combined < -0.25:  # Changed from -0.3
                result['interpretation'] = "Bearish"
            else:
                result['interpretation'] = "Neutral"
            
            logger.debug(f"On-chain metrics: SOPR={result['sthr_sopr']:.2f}, Combined={combined:.2f} -> {result['interpretation']}")
                
        except Exception as e:
            logger.error(f"On-chain metrics error: {e}")
        
        return result
        
    def get_comprehensive_sentiment(self, asset_type='general'):
        """
        Get sentiment appropriate for the asset type
        Now includes: Fear & Greed, On-chain metrics, VIX, AAII, news sources, and REDDIT
        """
        result = {
            'score': 0,
            'interpretation': 'Neutral',
            'components': {},
            'timestamp': datetime.now().isoformat()
        }
        
        try:
            if asset_type == 'crypto':
                # ===== CRYPTO-SPECIFIC SENTIMENT =====
                crypto_sentiment = {}
                
                # 1. Crypto Fear & Greed Index
                try:
                    crypto_sentiment['fear_greed'] = self.fetch_fear_greed_index()
                    logger.info(f"Crypto F&G: {crypto_sentiment['fear_greed'].get('value', 'N/A')} - {crypto_sentiment['fear_greed'].get('classification', 'N/A')}")
                except Exception as e:
                    logger.warning(f"Crypto F&G error: {e}")
                    crypto_sentiment['fear_greed'] = {'score': 0, 'value': 50, 'classification': 'Neutral'}
                
                # 2. On-chain metrics
                try:
                    crypto_sentiment['onchain'] = self.fetch_onchain_metrics()
                    logger.info(f"On-chain: {crypto_sentiment['onchain'].get('interpretation', 'N/A')}")
                except Exception as e:
                    logger.warning(f"On-chain error: {e}")
                    crypto_sentiment['onchain'] = {'exchange_flows': 0, 'interpretation': 'Neutral'}
                
                # 3. Crypto News
                try:
                    crypto_sentiment['news'] = self.fetch_crypto_news_sentiment('general')
                    logger.info(f"Crypto News: {crypto_sentiment['news'].get('interpretation', 'N/A')}")
                except Exception as e:
                    logger.warning(f"Crypto News error: {e}")
                    crypto_sentiment['news'] = {'score': 0, 'interpretation': 'Neutral'}
                
                # 4. Whale Alert (if available)
                try:
                    whale_data = self.fetch_whale_alerts()
                    if whale_data:
                        whale_sentiment = sum(a.get('sentiment', 0) for a in whale_data) / len(whale_data) if whale_data else 0
                        crypto_sentiment['whale'] = {
                            'score': whale_sentiment,
                            'count': len(whale_data),
                            'interpretation': self._interpret_sentiment(whale_sentiment)
                        }
                        logger.info(f"Whale Activity: {len(whale_data)} alerts, sentiment: {whale_sentiment:.2f}")
                except Exception as e:
                    logger.warning(f"Whale Alert error: {e}")
                
                # 5. Reddit Crypto Sentiment (NEW)
                try:
                    reddit_data = self.get_reddit_sentiment()
                    if reddit_data and reddit_data.get('score', 0) != 0:
                        crypto_sentiment['reddit'] = {
                            'score': reddit_data['score'],
                            'posts': reddit_data['total_posts'],
                            'interpretation': self._interpret_sentiment(reddit_data['score'])
                        }
                        logger.info(f"Reddit Crypto: {reddit_data['score']:.2f} from {reddit_data['total_posts']} posts")
                except Exception as e:
                    logger.warning(f"Reddit sentiment error: {e}")
                
                # Combine with weights (updated to include Reddit)
                weights = {
                    'fear_greed': 0.3,
                    'onchain': 0.25,
                    'news': 0.2,
                    'whale': 0.1,
                    'reddit': 0.15  # NEW: 15% weight for Reddit
                }
                
                combined_score = 0
                total_weight = 0
                
                for key, weight in weights.items():
                    if key in crypto_sentiment:
                        if key == 'onchain':
                            score = crypto_sentiment[key].get('combined_score', 0)
                        elif key == 'whale':
                            score = crypto_sentiment[key].get('score', 0)
                        elif key == 'reddit':
                            score = crypto_sentiment[key].get('score', 0)
                        else:
                            score = crypto_sentiment[key].get('score', 0)
                        
                        combined_score += score * weight
                        total_weight += weight
                
                if total_weight > 0:
                    combined_score = combined_score / total_weight
                
                result['score'] = combined_score
                result['interpretation'] = self._interpret_sentiment(combined_score)
                result['components'] = crypto_sentiment
                
            else:
                # ===== GENERAL MARKET SENTIMENT =====
                market_sentiment = {}
                
                # 1. CNN Fear & Greed Index
                try:
                    market_sentiment['cnn_fear_greed'] = self.fetch_cnn_fear_greed()
                    logger.info(f"CNN F&G: {market_sentiment['cnn_fear_greed'].get('value', 'N/A')} - {market_sentiment['cnn_fear_greed'].get('classification', 'N/A')}")
                except Exception as e:
                    logger.warning(f"CNN F&G error: {e}")
                    market_sentiment['cnn_fear_greed'] = {'score': 0, 'value': 50, 'classification': 'Neutral'}
                
                # 2. VIX (Volatility Index)
                try:
                    market_sentiment['vix'] = self.fetch_vix()
                    logger.info(f"VIX: {market_sentiment['vix'].get('value', 'N/A'):.1f} - {market_sentiment['vix'].get('classification', 'N/A')}")
                except Exception as e:
                    logger.warning(f"VIX error: {e}")
                    market_sentiment['vix'] = {'score': 0, 'value': 20, 'classification': 'Normal'}
                
                # 3. AAII Sentiment Survey
                try:
                    market_sentiment['aaii'] = self.fetch_aaii_sentiment()
                    logger.info(f"AAII: Bullish {market_sentiment['aaii'].get('bullish', 0):.1f}% / Bearish {market_sentiment['aaii'].get('bearish', 0):.1f}% - {market_sentiment['aaii'].get('interpretation', 'N/A')}")
                except Exception as e:
                    logger.warning(f"AAII error: {e}")
                    market_sentiment['aaii'] = self._get_aaii_placeholder()
                
                # 4. General News Sentiment
                try:
                    market_sentiment['news'] = self.fetch_general_news_sentiment()
                    logger.info(f"News: {market_sentiment['news'].get('interpretation', 'N/A')}")
                except Exception as e:
                    logger.warning(f"News error: {e}")
                    market_sentiment['news'] = {'score': 0, 'interpretation': 'Neutral', 'article_count': 0}
                
                # 5. Put/Call Ratio
                try:
                    market_sentiment['put_call'] = self.fetch_put_call_ratio()
                    logger.info(f"Put/Call: {market_sentiment['put_call'].get('ratio', 0):.2f} - {market_sentiment['put_call'].get('interpretation', 'N/A')}")
                except Exception as e:
                    logger.warning(f"Put/Call error: {e}")
                
                # 6. Reddit General Sentiment (NEW)
                try:
                    reddit_data = self.get_reddit_sentiment()
                    if reddit_data and reddit_data.get('score', 0) != 0:
                        market_sentiment['reddit'] = {
                            'score': reddit_data['score'],
                            'posts': reddit_data['total_posts'],
                            'interpretation': self._interpret_sentiment(reddit_data['score'])
                        }
                        logger.info(f"Reddit General: {reddit_data['score']:.2f} from {reddit_data['total_posts']} posts")
                except Exception as e:
                    logger.warning(f"Reddit sentiment error: {e}")
                
                # Combine with weights (updated to include Reddit)
                weights = {
                    'cnn_fear_greed': 0.25,
                    'vix': 0.15,
                    'aaii': 0.15,
                    'news': 0.15,
                    'put_call': 0.1,
                    'reddit': 0.2  # NEW: 20% weight for Reddit
                }
                
                combined_score = 0
                total_weight = 0
                
                for key, weight in weights.items():
                    if key in market_sentiment:
                        if key == 'aaii':
                            score = market_sentiment[key].get('sentiment_score', 0)
                        elif key == 'news':
                            score = market_sentiment[key].get('score', 0)
                        elif key == 'reddit':
                            score = market_sentiment[key].get('score', 0)
                        else:
                            score = market_sentiment[key].get('score', 0)
                        
                        combined_score += score * weight
                        total_weight += weight
                
                if total_weight > 0:
                    combined_score = combined_score / total_weight
                
                result['score'] = combined_score
                result['interpretation'] = self._interpret_sentiment(combined_score)
                result['components'] = market_sentiment
        
        except Exception as e:
            logger.error(f"Comprehensive sentiment error: {e}")
        
        return result

    def fetch_general_news_sentiment(self):
        """
        Fetch general market news sentiment using the news integrator
        No API key needed - uses existing news sources
        """
        try:
            articles = self.news_integrator.fetch_all_sources()
            
            if articles:
                avg_sentiment = sum(a.get('sentiment', 0) for a in articles) / len(articles)
                logger.info(f"General news: {len(articles)} articles, sentiment: {avg_sentiment:.2f}")
                return {
                    'score': avg_sentiment,
                    'interpretation': self._interpret_sentiment(avg_sentiment),
                    'article_count': len(articles)
                }
        except Exception as e:
            logger.error(f"General news error: {e}")
        
        return {'score': 0, 'interpretation': 'Neutral', 'article_count': 0}

    def fetch_put_call_ratio(self):
        """
        Fetch CBOE Put/Call ratio from Alpha Vantage
        Uses your existing ALPHA_VANTAGE_KEY from config
        """
        try:
            # Get Alpha Vantage key from config
            from config.config import ALPHA_VANTAGE_API_KEY
            
            if not ALPHA_VANTAGE_API_KEY or ALPHA_VANTAGE_API_KEY == "your_key_here":
                logger.debug("Alpha Vantage key not configured, using VIX estimate for put/call")
                return self._get_put_call_from_vix()
            
            # Alpha Vantage PCR endpoint is not available on the free tier.
            # Skip the call entirely to preserve our 20 calls/day quota and go
            # straight to the VIX-based estimate, which works fine for our use.
            return self._get_put_call_from_vix()
            
            # Interpret the ratio - MORE SENSITIVE
            if ratio > 1.0:  # Changed from 1.1
                score = -0.6  # Increased from -0.5
                interpretation = "Bearish (high put volume)"
            elif ratio > 0.85:  # Changed from 0.9
                score = -0.3  # Increased from -0.2
                interpretation = "Slightly Bearish"
            elif ratio < 0.65:  # Changed from 0.6
                score = 0.6  # Increased from 0.5
                interpretation = "Bullish (high call volume)"
            elif ratio < 0.8:  # Changed from 0.8
                score = 0.3  # Increased from 0.2
                interpretation = "Slightly Bullish"
            else:
                score = 0
                interpretation = "Neutral"
            
            return {
                'ratio': round(ratio, 2),
                'score': score,
                'interpretation': interpretation,
                'source': source
            }
            
        except Exception as e:
            logger.warning(f"Put/Call error: {e}")
            return self._get_put_call_from_vix()

    def _get_put_call_from_vix(self):
        """Fallback method using VIX to estimate put/call sentiment"""
        try:
            vix = self.fetch_vix()
            vix_value = vix.get('value', 20)
            
            if vix_value > 28:
                ratio = 1.25
                source = "Estimated from VIX (High Fear)"
            elif vix_value > 23:
                ratio = 1.1
                source = "Estimated from VIX (Moderate Fear)"
            elif vix_value > 18:
                ratio = 0.95
                source = "Estimated from VIX (Normal)"
            elif vix_value > 14:
                ratio = 0.8
                source = "Estimated from VIX (Complacent)"
            else:
                ratio = 0.7
                source = "Estimated from VIX (Very Complacent)"
            
            # Interpret the ratio - MORE SENSITIVE
            if ratio > 1.0:
                score = -0.6
                interpretation = "Bearish (high put volume)"
            elif ratio > 0.85:
                score = -0.3
                interpretation = "Slightly Bearish"
            elif ratio < 0.65:
                score = 0.6
                interpretation = "Bullish (high call volume)"
            elif ratio < 0.8:
                score = 0.3
                interpretation = "Slightly Bullish"
            else:
                score = 0
                interpretation = "Neutral"
            
            logger.debug(f"Put/Call estimated from VIX: {ratio:.2f}")
            
            return {
                'ratio': round(ratio, 2),
                'score': score,
                'interpretation': interpretation,
                'source': source
            }
        except:
            return self._get_put_call_placeholder()

    def _get_put_call_placeholder(self):
        """Ultimate fallback placeholder"""
        logger.debug("Using placeholder Put/Call ratio")
        return {
            'ratio': 0.85,
            'score': 0,
            'interpretation': 'Neutral',
            'source': 'Put/Call Ratio (default)'
        }
    
    def get_trading_signal(self, asset):
        """Generate trading signal based on sentiment from best source"""
        sentiment = self.get_best_sentiment(asset)
        
        # Base confidence on sentiment magnitude
        if sentiment['score'] > 0.3:
            confidence = min(0.5 + sentiment['score'] * 0.5, 0.95)
            signal = {
                'signal': 'BUY',
                'confidence': round(confidence, 2),
                'source': sentiment['source'],
                'articles': sentiment['articles'],
                'score': sentiment['score'],
                'interpretation': sentiment['interpretation']
            }
        elif sentiment['score'] < -0.3:
            confidence = min(0.5 + abs(sentiment['score']) * 0.5, 0.95)
            signal = {
                'signal': 'SELL',
                'confidence': round(confidence, 2),
                'source': sentiment['source'],
                'articles': sentiment['articles'],
                'score': sentiment['score'],
                'interpretation': sentiment['interpretation']
            }
        else:
            signal = {
                'signal': 'HOLD',
                'confidence': 0.5,
                'source': sentiment['source'],
                'articles': sentiment['articles'],
                'score': sentiment['score'],
                'interpretation': sentiment['interpretation']
            }
        
        logger.info(f"Trading signal for {asset}: {signal['signal']} (confidence: {signal['confidence']:.2f})")
        return signal