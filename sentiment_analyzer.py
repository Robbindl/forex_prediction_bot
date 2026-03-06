"""
📰 Sentiment Analyzer - News and social media sentiment
UPDATED: Now includes all news sources, Fear & Greed, VIX, AAII, On-chain metrics
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

# Import from config
from config.config import (
    NEWSAPI_KEY, GNEWS_KEY, RAPIDAPI_KEY,
    WHALE_ALERT_KEY, TWITTER_BEARER_TOKEN
)

# Import the new integrator
from news_sources import NewsSourceIntegrator

class SentimentAnalyzer:
    """Enhanced sentiment analyzer with all news sources"""
    
    def __init__(self):
        self.sentiment_cache = {}
        self.rapidapi_key = RAPIDAPI_KEY
        self.gnews_key = GNEWS_KEY
        self.newsapi_key = NEWSAPI_KEY
        self.whale_alert_key = WHALE_ALERT_KEY
        self.twitter_token = TWITTER_BEARER_TOKEN
        
        # Initialize the comprehensive news integrator
        self.news_integrator = NewsSourceIntegrator()
        
        # ===== NEW: Initialize Whale Alert Manager =====
        try:
            self.whale_manager = WhaleAlertManager()
            self.whale_manager.start_monitoring()
            self.whale_cache = []
            print(f"🐋 Whale Alert Manager initialized")
        except Exception as e:
            print(f"⚠️ Could not initialize Whale Alert Manager: {e}")
            self.whale_manager = None
            self.whale_cache = []
        # ==============================================
        
        print(f"📰 News sources initialized with {len(self.news_integrator.sources)} sources")
    
    # ===== NEW: Fetch whale alerts from Twitter/Telegram =====
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
                    print(f"  🐋 Found {len(alerts)} whale alerts (top 5: ${total_value:.1f}M)")
                
                return alerts
            else:
                # Fallback to placeholder if manager not available
                return self._get_placeholder_whale_alerts()
                
        except Exception as e:
            print(f"  ⚠️ Whale alert fetch error: {e}")
            return self.whale_cache
    
    def _get_placeholder_whale_alerts(self) -> List[Dict]:
        """Return placeholder whale alerts when manager is unavailable"""
        return [
            {
                'title': '🐋 1,000 BTC ($65M) moved from unknown wallet to Binance',
                'value_usd': 65000000,
                'symbol': 'BTC',
                'date': datetime.now().isoformat(),
                'source': 'Placeholder',
                'sentiment': 0.1
            },
            {
                'title': '🐋 5,000 ETH ($15M) moved to cold storage',
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
        
        summary = f"🐋 Top {len(alerts)} whales: ${total_value:.1f}M total\n"
        for alert in alerts[:3]:
            value_m = alert.get('value_usd', 0) / 1_000_000
            summary += f"   • {alert.get('symbol', '?')}: ${value_m:.1f}M\n"
        
        return summary
    
    def get_comprehensive_sentiment(self, asset=None):
        """
        Get sentiment from ALL news sources
        """
        return self.news_integrator.get_sentiment_summary(asset)
    
    def fetch_news_sentiment(self, asset, days=1):
        """Fetch news sentiment from NewsAPI (primary source)"""
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
                    
                    # Analyze sentiment
                    blob = TextBlob(text)
                    sentiment = blob.sentiment.polarity  # -1 to 1
                    sentiments.append(sentiment)
                    articles_data.append({
                        'title': title,
                        'sentiment': sentiment
                    })
                
                # Average sentiment
                avg_sentiment = sum(sentiments) / len(sentiments) if sentiments else 0
                
                return {
                    'score': avg_sentiment,
                    'magnitude': abs(avg_sentiment),
                    'articles': len(sentiments),
                    'articles_data': articles_data,
                    'interpretation': self.interpret_sentiment(avg_sentiment),
                    'source': 'NewsAPI'
                }
                
        except Exception as e:
            print(f"  ⚠️ NewsAPI error: {e}")
        
        return None
    
    def fetch_crypto_news_sentiment(self, asset: str) -> Dict:
        """
        Fetch crypto news from free API and analyze sentiment
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
                    sentiments.append(sentiment)
                    articles_data.append({
                        'title': title,
                        'sentiment': sentiment
                    })
                
                avg_sentiment = sum(sentiments) / len(sentiments) if sentiments else 0
                
                return {
                    'score': avg_sentiment,
                    'magnitude': abs(avg_sentiment),
                    'articles': len(sentiments),
                    'articles_data': articles_data,
                    'interpretation': self.interpret_sentiment(avg_sentiment),
                    'source': 'CryptoNews (free)'
                }
                
        except Exception as e:
            print(f"  ⚠️ CryptoNews error: {e}")
        
        return {
            'score': 0,
            'magnitude': 0,
            'articles': 0,
            'articles_data': [],
            'interpretation': 'Neutral',
            'source': 'CryptoNews (free)'
        }
    
    def fetch_rapidapi_news(self, asset, days=1):
        """Fetch news from RapidAPI Real-Time Finance Data"""
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
                print(f"    ⚠️ No RapidAPI symbol for {asset}")
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
            
            print(f"    🔄 Trying RapidAPI for {api_symbol}")
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
                        sentiments.append(sentiment)
                        articles_data.append({
                            'title': title,
                            'sentiment': sentiment
                        })
                    
                    avg_sentiment = sum(sentiments) / len(sentiments)
                    return {
                        'score': avg_sentiment,
                        'magnitude': abs(avg_sentiment),
                        'articles': len(sentiments),
                        'articles_data': articles_data,
                        'interpretation': self.interpret_sentiment(avg_sentiment),
                        'source': 'RapidAPI'
                    }
            
            print(f"    ⚠️ No news from RapidAPI for {asset}")
            return None
                
        except Exception as e:
            print(f"  ⚠️ RapidAPI error: {e}")
            return None
    
    def fetch_gnews_sentiment(self, asset, days=1):
        """Fetch news from GNews API"""
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
                '^GSPC': 'S&P 500 index',
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
                    sentiments.append(sentiment)
                    articles_data.append({
                        'title': title,
                        'sentiment': sentiment
                    })
                
                if sentiments:
                    avg_sentiment = sum(sentiments) / len(sentiments)
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
            print(f"  ⚠️ GNews error: {e}")
            return None

    def alpha_vantage_key(self):
        """Get Alpha Vantage key from config"""
        from config.config import ALPHA_VANTAGE_API_KEY
        return ALPHA_VANTAGE_API_KEY
    
    def get_best_sentiment(self, asset, days=1):
        """Try all news sources in parallel and return the best result"""
        print(f"\n  🔍 Fetching news for {asset} from multiple sources...")
        
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
                        print(f"    ✅ {source_name}: {result['articles']} articles, score: {result['score']:.2f}")
                        if result['articles'] > best_articles:
                            best_result = result
                            best_articles = result['articles']
                    else:
                        print(f"    ⚠️ {source_name}: No results")
                except Exception as e:
                    print(f"    ❌ {source_name} error: {str(e)[:50]}")
                    continue
        
        if best_result:
            print(f"    🏆 Best source: {best_result['source']} with {best_result['articles']} articles")
            return best_result
        
        print(f"    ❌ No results from any source")
        return {
            'score': 0,
            'magnitude': 0,
            'articles': 0,
            'articles_data': [],
            'interpretation': 'Neutral',
            'source': 'none'
        }
    
    def interpret_sentiment(self, score):
        """Convert numeric score to text interpretation"""
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
                
                # Convert to sentiment score (-1 to 1)
                if value < 25:
                    score = -0.8
                elif value < 45:
                    score = -0.4
                elif value < 55:
                    score = 0
                elif value < 75:
                    score = 0.4
                else:
                    score = 0.8
                
                return {
                    'score': score,
                    'value': value,
                    'classification': classification,
                    'source': 'Fear & Greed Index'
                }
        except Exception as e:
            print(f"  ⚠️ Fear & Greed error: {e}")
        
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
            
            if value < 25:
                sentiment = "Extreme Fear"
                score = -0.8
            elif value < 45:
                sentiment = "Fear"
                score = -0.4
            elif value < 55:
                sentiment = "Neutral"
                score = 0
            elif value < 75:
                sentiment = "Greed"
                score = 0.4
            else:
                sentiment = "Extreme Greed"
                score = 0.8
                
            return {
                'score': score,
                'value': value,
                'classification': sentiment,
                'source': 'CNN Fear & Greed'
            }
        except Exception as e:
            print(f"  ⚠️ CNN Fear & Greed error: {e}")
            return {'score': 0, 'value': 50, 'classification': 'Neutral', 'source': 'CNN Fear & Greed'}

    def fetch_vix(self):
        """Fetch VIX - market volatility (fear) index"""
        try:
            vix = yf.Ticker("^VIX")
            data = vix.history(period="1d")
            
            if not data.empty:
                current_vix = float(data['Close'].iloc[-1])
                
                # VIX > 30 = high fear, VIX < 20 = complacency
                if current_vix > 30:
                    score = -0.7
                    sentiment = "High Fear"
                elif current_vix > 25:
                    score = -0.4
                    sentiment = "Moderate Fear"
                elif current_vix > 20:
                    score = 0
                    sentiment = "Normal"
                elif current_vix > 15:
                    score = 0.3
                    sentiment = "Complacent"
                else:
                    score = 0.5
                    sentiment = "Very Complacent"
                    
                return {
                    'score': score,
                    'value': round(current_vix, 2),
                    'classification': sentiment,
                    'source': 'VIX'
                }
        except Exception as e:
            print(f"  ⚠️ VIX error: {e}")
        
        return {'score': 0, 'value': 20, 'classification': 'Normal', 'source': 'VIX'}

    def fetch_aaii_sentiment(self):
        """
        Fetches the latest AAII Investor Sentiment Survey data.
        Uses multiple strategies to find the data.
        """
        # Try different User-Agents to avoid 403
        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Safari/605.1.15',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36'
        ]
        
        for user_agent in user_agents:
            try:
                headers = {'User-Agent': user_agent}
                url = "https://www.aaii.com/sentimentsurvey"
                
                response = requests.get(url, headers=headers, timeout=10)
                if response.status_code == 200:
                    soup = BeautifulSoup(response.content, 'html.parser')
                    
                    # Look for the survey data in various ways
                    # Method 1: Find by text pattern
                    text = soup.get_text()
                    import re
                    
                    # Pattern for "Bullish: XX.X%" type format
                    pattern = r'Bullish:?\s*(\d+\.?\d*)%.*?Neutral:?\s*(\d+\.?\d*)%.*?Bearish:?\s*(\d+\.?\d*)%'
                    match = re.search(pattern, text, re.DOTALL | re.IGNORECASE)
                    
                    if match:
                        bullish = float(match.group(1))
                        neutral = float(match.group(2))
                        bearish = float(match.group(3))
                        return self._process_aaii_data(bullish, neutral, bearish, "Current Week")
                    
                    # Method 2: Look for tables
                    tables = soup.find_all('table')
                    for table in tables:
                        rows = table.find_all('tr')
                        if len(rows) >= 2:
                            cells = rows[1].find_all('td')
                            if len(cells) >= 4:
                                try:
                                    bullish = float(cells[1].text.strip().replace('%', ''))
                                    neutral = float(cells[2].text.strip().replace('%', ''))
                                    bearish = float(cells[3].text.strip().replace('%', ''))
                                    return self._process_aaii_data(bullish, neutral, bearish, "Current Week")
                                except:
                                    continue
                    
                    # If we got here with 200 but no data, try next user-agent
                    continue
                    
            except Exception as e:
                print(f"Error with user-agent {user_agent[:20]}...: {e}")
                continue
        
        # If all attempts fail, return placeholder
        print("Could not fetch AAII data after multiple attempts.")
        return self._get_aaii_placeholder()

    def _process_aaii_data(self, bullish, neutral, bearish, date_text):
        """Process AAII data into sentiment score"""
        bull_bear_ratio = bullish / bearish if bearish > 0 else 0
        
        # Contrarian interpretation
        interpretation = "Neutral"
        sentiment_score = 0
        
        if bearish > 50:
            interpretation = "Extremely Bearish (Contrarian Buy)"
            sentiment_score = 0.6
        elif bearish > 39.0:
            interpretation = "Bearish (Contrarian Opportunity)"
            sentiment_score = 0.3
        elif bullish > 50:
            interpretation = "Extremely Bullish (Contrarian Caution)"
            sentiment_score = -0.6
        elif bullish > 45.0:
            interpretation = "Bullish (Contrarian Caution)"
            sentiment_score = -0.3
        
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

    def fetch_whale_alerts(self, min_value_usd=1000000):
        """
        Fetch large crypto transactions from Whale Alert
        🔑 API KEY REQUIRED: Get from https://whale-alert.io/
        """
        if not self.whale_alert_key or self.whale_alert_key == "your_whale_alert_key_here":
            print("  ⚠️ Whale Alert key not configured - skipping")
            return []
        
        try:
            url = "https://api.whale-alert.io/v1/transactions"
            params = {
                "api_key": self.whale_alert_key,
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
                    'title': f"Whale Alert: {tx.get('amount', 0):.2f} {tx.get('symbol')} (${value:,.0f}) moved",
                    'sentiment': sentiment,
                    'value_usd': value,
                    'symbol': tx.get('symbol'),
                    'source': 'Whale Alert',
                    'date': datetime.fromtimestamp(tx.get('timestamp', 0)).isoformat()
                })
            
            return alerts
        except Exception as e:
            print(f"Whale Alert error: {e}")
            return []

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
            
            # Price-based sentiment
            if price_change < -2:
                price_sentiment = -0.4
            elif price_change > 2:
                price_sentiment = 0.4
            else:
                price_sentiment = 0
            
            # DEX-based sentiment
            if buys + sells > 0:
                buy_ratio = buys / (buys + sells)
                if buy_ratio > 0.6:
                    dex_sentiment = 0.3
                elif buy_ratio < 0.4:
                    dex_sentiment = -0.3
                else:
                    dex_sentiment = 0
            else:
                dex_sentiment = 0
            
            # SOPR-based sentiment (<1 = bearish, >1 = bullish)
            sopr = result['sthr_sopr']
            if sopr < 0.95:
                sopr_sentiment = -0.4
            elif sopr < 1.0:
                sopr_sentiment = -0.2
            elif sopr < 1.05:
                sopr_sentiment = 0.2
            else:
                sopr_sentiment = 0.4
            
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
            if combined > 0.3:
                result['interpretation'] = "Bullish"
            elif combined < -0.3:
                result['interpretation'] = "Bearish"
            else:
                result['interpretation'] = "Neutral"
                
        except Exception as e:
            print(f"⚠️ On-chain metrics error: {e}")
        
        return result
        
    def get_comprehensive_sentiment(self, asset_type='general'):
        """
        Get sentiment appropriate for the asset type
        Now includes: Fear & Greed, On-chain metrics, VIX, AAII, and multiple news sources
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
                    print(f"  📊 Crypto F&G: {crypto_sentiment['fear_greed'].get('value', 'N/A')} - {crypto_sentiment['fear_greed'].get('classification', 'N/A')}")
                except Exception as e:
                    print(f"  ⚠️ Crypto F&G error: {e}")
                    crypto_sentiment['fear_greed'] = {'score': 0, 'value': 50, 'classification': 'Neutral'}
                
                # 2. On-chain metrics
                try:
                    crypto_sentiment['onchain'] = self.fetch_onchain_metrics()
                    print(f"  ⛓️ On-chain: {crypto_sentiment['onchain'].get('interpretation', 'N/A')}")
                except Exception as e:
                    print(f"  ⚠️ On-chain error: {e}")
                    crypto_sentiment['onchain'] = {'exchange_flows': 0, 'interpretation': 'Neutral'}
                
                # 3. Crypto News
                try:
                    crypto_sentiment['news'] = self.fetch_crypto_news_sentiment('general')
                    print(f"  📰 Crypto News: {crypto_sentiment['news'].get('interpretation', 'N/A')}")
                except Exception as e:
                    print(f"  ⚠️ Crypto News error: {e}")
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
                        print(f"  🐋 Whale Activity: {len(whale_data)} alerts, sentiment: {whale_sentiment:.2f}")
                except Exception as e:
                    print(f"  ⚠️ Whale Alert error: {e}")
                
                # Combine with weights
                weights = {
                    'fear_greed': 0.4,
                    'onchain': 0.3,
                    'news': 0.2,
                    'whale': 0.1
                }
                
                combined_score = 0
                total_weight = 0
                
                for key, weight in weights.items():
                    if key in crypto_sentiment:
                        if key == 'onchain':
                            score = crypto_sentiment[key].get('combined_score', 0)
                        elif key == 'whale':
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
                    print(f"  📊 CNN F&G: {market_sentiment['cnn_fear_greed'].get('value', 'N/A')} - {market_sentiment['cnn_fear_greed'].get('classification', 'N/A')}")
                except Exception as e:
                    print(f"  ⚠️ CNN F&G error: {e}")
                    market_sentiment['cnn_fear_greed'] = {'score': 0, 'value': 50, 'classification': 'Neutral'}
                
                # 2. VIX (Volatility Index)
                try:
                    market_sentiment['vix'] = self.fetch_vix()
                    print(f"  📉 VIX: {market_sentiment['vix'].get('value', 'N/A'):.1f} - {market_sentiment['vix'].get('classification', 'N/A')}")
                except Exception as e:
                    print(f"  ⚠️ VIX error: {e}")
                    market_sentiment['vix'] = {'score': 0, 'value': 20, 'classification': 'Normal'}
                
                # 3. AAII Sentiment Survey
                try:
                    market_sentiment['aaii'] = self.fetch_aaii_sentiment()
                    print(f"  📝 AAII: Bullish {market_sentiment['aaii'].get('bullish', 0):.1f}% / Bearish {market_sentiment['aaii'].get('bearish', 0):.1f}% - {market_sentiment['aaii'].get('interpretation', 'N/A')}")
                except Exception as e:
                    print(f"  ⚠️ AAII error: {e}")
                    market_sentiment['aaii'] = self._get_aaii_placeholder()
                
                # 4. General News Sentiment
                try:
                    market_sentiment['news'] = self.fetch_general_news_sentiment()
                    print(f"  📰 News: {market_sentiment['news'].get('interpretation', 'N/A')}")
                except Exception as e:
                    print(f"  ⚠️ News error: {e}")
                    market_sentiment['news'] = {'score': 0, 'interpretation': 'Neutral', 'article_count': 0}
                
                # 5. Put/Call Ratio (from Yahoo Finance - free, no key)
                try:
                    market_sentiment['put_call'] = self.fetch_put_call_ratio()
                    print(f"  📊 Put/Call: {market_sentiment['put_call'].get('ratio', 0):.2f} - {market_sentiment['put_call'].get('interpretation', 'N/A')}")
                except Exception as e:
                    print(f"  ⚠️ Put/Call error: {e}")
                
                # Combine with weights
                weights = {
                    'cnn_fear_greed': 0.3,
                    'vix': 0.2,
                    'aaii': 0.2,
                    'news': 0.2,
                    'put_call': 0.1
                }
                
                combined_score = 0
                total_weight = 0
                
                for key, weight in weights.items():
                    if key in market_sentiment:
                        if key == 'aaii':
                            score = market_sentiment[key].get('sentiment_score', 0)
                        elif key == 'news':
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
            print(f"⚠️ Comprehensive sentiment error: {e}")
        
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
                return {
                    'score': avg_sentiment,
                    'interpretation': self._interpret_sentiment(avg_sentiment),
                    'article_count': len(articles)
                }
        except Exception as e:
            print(f"General news error: {e}")
        
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
                print("  ⚠️ Alpha Vantage key not configured, using VIX estimate")
                return self._get_put_call_from_vix()
            
            # Alpha Vantage API for Put/Call ratio
            url = "https://www.alphavantage.co/query"
            params = {
                'function': 'PCR',  # Put/Call Ratio
                'symbol': 'SPX',
                'apikey': ALPHA_VANTAGE_API_KEY
            }
            
            response = requests.get(url, params=params, timeout=5)
            data = response.json()
            
            # Check if we got valid data
            if 'data' in data and len(data['data']) > 0:
                ratio = float(data['data'][0]['value'])
                source = "Alpha Vantage"
                print(f"  ✅ Got real Put/Call from Alpha Vantage: {ratio}")
            else:
                # Fallback to VIX estimate
                print(f"  ⚠️ Alpha Vantage returned no data, using VIX estimate")
                return self._get_put_call_from_vix()
            
            # Interpret the ratio
            if ratio > 1.1:
                score = -0.5
                interpretation = "Bearish (high put volume)"
            elif ratio > 0.9:
                score = -0.2
                interpretation = "Slightly Bearish"
            elif ratio < 0.6:
                score = 0.5
                interpretation = "Bullish (high call volume)"
            elif ratio < 0.8:
                score = 0.2
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
            print(f"  ⚠️ Put/Call error: {e}")
            return self._get_put_call_from_vix()

    def _get_put_call_from_vix(self):
        """Fallback method using VIX to estimate put/call sentiment"""
        try:
            vix = self.fetch_vix()
            vix_value = vix.get('value', 20)
            
            if vix_value > 30:
                ratio = 1.2
                source = "Estimated from VIX (High Fear)"
            elif vix_value > 25:
                ratio = 1.0
                source = "Estimated from VIX (Moderate Fear)"
            elif vix_value > 20:
                ratio = 0.85
                source = "Estimated from VIX (Normal)"
            elif vix_value > 15:
                ratio = 0.7
                source = "Estimated from VIX (Complacent)"
            else:
                ratio = 0.6
                source = "Estimated from VIX (Very Complacent)"
            
            # Interpret the ratio
            if ratio > 1.1:
                score = -0.5
                interpretation = "Bearish (high put volume)"
            elif ratio > 0.9:
                score = -0.2
                interpretation = "Slightly Bearish"
            elif ratio < 0.6:
                score = 0.5
                interpretation = "Bullish (high call volume)"
            elif ratio < 0.8:
                score = 0.2
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
        except:
            return self._get_put_call_placeholder()

    def _get_put_call_placeholder(self):
        """Ultimate fallback placeholder"""
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
            return {
                'signal': 'BUY',
                'confidence': round(confidence, 2),
                'source': sentiment['source'],
                'articles': sentiment['articles'],
                'score': sentiment['score'],
                'interpretation': sentiment['interpretation']
            }
        elif sentiment['score'] < -0.3:
            confidence = min(0.5 + abs(sentiment['score']) * 0.5, 0.95)
            return {
                'signal': 'SELL',
                'confidence': round(confidence, 2),
                'source': sentiment['source'],
                'articles': sentiment['articles'],
                'score': sentiment['score'],
                'interpretation': sentiment['interpretation']
            }
        else:
            return {
                'signal': 'HOLD',
                'confidence': 0.5,
                'source': sentiment['source'],
                'articles': sentiment['articles'],
                'score': sentiment['score'],
                'interpretation': sentiment['interpretation']
            }