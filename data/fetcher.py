"""
⚡ ULTIMATE MULTI-API FETCHER - Real-time data from ALL sources
With ACCURATE market hours for EAT timezone (UTC+3)
FIXED: Added CoinGecko, better error handling, improved Yahoo mappings
"""

import requests
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
import time
from typing import Dict, List, Optional, Tuple, Callable, Any
import sys
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import warnings
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from logger import logger

# ===== IMPORT FROM CONFIG =====
from config.config import ITICK_TOKEN, OILPRICE_API_KEY
# ==============================

# API Clients (install these)
try:
    import finnhub
    FINNHUB_AVAILABLE = True
except ImportError:
    finnhub = None
    FINNHUB_AVAILABLE = False

try:
    from alpha_vantage.timeseries import TimeSeries
    from alpha_vantage.foreignexchange import ForeignExchange
    ALPHA_VANTAGE_AVAILABLE = True
except ImportError:
    ALPHA_VANTAGE_AVAILABLE = False

try:
    from twelvedata import TDClient
    TWELVEDATA_AVAILABLE = True
except ImportError:
    TWELVEDATA_AVAILABLE = False

warnings.filterwarnings('ignore')


class MarketHours:
    """
    ACCURATE market hours checker for East African Time (EAT = UTC+3)
    All times converted to your local timezone
    """
    
    @staticmethod
    def get_ny_time():
        """Convert current EAT to New York time (UTC-4/UTC-5)"""
        now = datetime.now()
        # EAT is UTC+3, NY is UTC-4 (summer) or UTC-5 (winter)
        # Using UTC-4 for simplicity (April-October)
        return now - timedelta(hours=7)  # UTC+3 to UTC-4 = -7 hours
    
    @staticmethod
    def is_crypto_open() -> bool:
        """Crypto: 24/7 - ALWAYS OPEN"""
        return True
    
    @staticmethod
    def is_forex_open() -> bool:
        """Forex: Opens Sunday 5pm NY time (Monday 1am EAT) - Closes Friday 5pm NY (Friday 1am EAT)"""
        ny_time = MarketHours.get_ny_time()
        weekday = ny_time.weekday()  # 0=Monday, 4=Friday, 5=Saturday, 6=Sunday
        
        # Forex closed on weekends (Saturday and Sunday until 5pm NY)
        if weekday == 5:  # Saturday
            return False
        if weekday == 6:  # Sunday
            # Sunday after 5pm NY (Monday 1am EAT) is OPEN
            return ny_time.hour >= 17
        if weekday == 4:  # Friday
            # Friday before 5pm NY is OPEN, after 5pm is CLOSED
            return ny_time.hour < 17
        
        # Monday through Thursday - always open
        return True
    
    @staticmethod
    def is_stock_open() -> bool:
        """
        US Stocks: Mon-Fri 9:30am - 4:00pm NY time
        In EAT: 5:30pm - 12:00am (midnight) next day
        """
        ny_time = MarketHours.get_ny_time()
        weekday = ny_time.weekday()
        
        # Closed on weekends
        if weekday >= 5:  # Saturday or Sunday
            return False
        
        # Check if within trading hours (9:30 AM - 4:00 PM NY)
        current_time = ny_time.hour + ny_time.minute / 60
        market_open = 9.5  # 9:30 AM
        market_close = 16.0  # 4:00 PM
        
        return market_open <= current_time <= market_close
    
    @staticmethod
    def is_commodity_open() -> bool:
        """
        Commodities (CME): Sunday 6pm - Friday 5pm NY time
        With daily break 5pm-6pm NY time
        
        In EAT:
        - Opens: Monday 1am (Sunday 6pm NY)
        - Closes: Saturday 1am (Friday 5pm NY)
        - Daily break: 1am-2am EAT (5pm-6pm NY)
        """
        ny_time = MarketHours.get_ny_time()
        weekday = ny_time.weekday()
        
        # Check weekend closure
        if weekday == 5:  # Saturday
            return False
        if weekday == 6:  # Sunday
            # Sunday after 6pm NY (Monday 1am EAT) is OPEN
            return ny_time.hour >= 18
        
        if weekday == 4:  # Friday
            # Friday before 5pm NY is OPEN, after 5pm is CLOSED
            return ny_time.hour < 17
        
        # Monday - Thursday: Check daily maintenance break (5pm-6pm NY / 1am-2am EAT)
        if 17 <= ny_time.hour < 18:  # 5pm-6pm NY = daily maintenance
            return False
        
        return True
    
    @staticmethod
    def is_index_open() -> bool:
        """Indices: Follow stock market hours"""
        return MarketHours.is_stock_open()
    
    @staticmethod
    def get_status() -> Dict[str, bool]:
        """Get current market status for all categories"""
        return {
            'crypto': MarketHours.is_crypto_open(),
            'forex': MarketHours.is_forex_open(),
            'stocks': MarketHours.is_stock_open(),
            'commodities': MarketHours.is_commodity_open(),
            'indices': MarketHours.is_index_open(),
            'is_weekend': datetime.now().weekday() >= 5,
            'current_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'ny_time': MarketHours.get_ny_time().strftime('%Y-%m-%d %H:%M:%S')
        }
    
    @staticmethod
    def get_status_message(category: str) -> str:
        """Get human-readable status message"""
        status = MarketHours.get_status()
        ny_time = MarketHours.get_ny_time()
        
        if status.get(category, False):
            return f"Open (NY: {ny_time.strftime('%H:%M')})"
        
        if category == 'crypto':
            return "Open 24/7"
        elif category == 'forex':
            if ny_time.weekday() >= 5:
                return "Closed for weekend"
            return "Closed (Daily cycle)"
        elif category in ['stocks', 'indices']:
            if ny_time.weekday() >= 5:
                return "Closed for weekend"
            return f"Closed (Opens at {(ny_time.replace(hour=9, minute=30) + timedelta(hours=7)).strftime('%H:%M')} EAT)"
        elif category == 'commodities':
            if ny_time.weekday() >= 5:
                return "Closed for weekend"
            if 17 <= ny_time.hour < 18:
                return "Daily maintenance break (1am-2am EAT)"
            return f"Closed (Opens at {(ny_time.replace(hour=18, minute=0) + timedelta(hours=7)).strftime('%H:%M')} EAT)"
        return "Market Closed"


class NASALevelFetcher:
    """
    🚀 ULTIMATE MULTI-API FETCHER
    - Finnhub: Real-time forex, stocks, crypto
    - Alpha Vantage: Stocks, forex, commodities (via REST API)
    - Twelve Data: Commodities, ETFs, indices (SUPPORTS 4H!)
    - Yahoo Finance: Universal fallback
    - CoinGecko: Free crypto data (no API key needed!)
    - SUPPORTS: 1m, 5m, 15m, 1h, 4h, 1d for day trading
    """
    
    def __init__(self):
        logger.info("="*60)
        logger.info(" INITIALIZING ULTIMATE MULTI-API FETCHER")
        logger.info("="*60)
        
        # API Keys
        self.alpha_vantage_key = 'PACP0NRM3SIFWZBL'
        self.finnhub_key = 'd6bc2ohr01qnr27kdcb0d6bc2ohr01qnr27kdcbg'
        self.twelve_data_key = '6c8e5137892642fe96cbfbf9d782c7d0'
        
        # ===== NEW API TOKENS FROM CONFIG =====
        self.itick_token = ITICK_TOKEN
        self.oilprice_token = OILPRICE_API_KEY
        # ======================================
        
        # Initialize API clients
        self._init_api_clients()
        
        # Connection pooling
        self.session = requests.Session()
        retry_strategy = Retry(total=3, backoff_factor=1.0, status_forcelist=[429, 500, 502, 503, 504])
        adapter = HTTPAdapter(pool_connections=50, pool_maxsize=100, max_retries=retry_strategy)
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)
        
        # Thread pool for parallel requests
        self.executor = ThreadPoolExecutor(max_workers=20)
        
        # Cache with market hours awareness
        self.cache = {}
        self.cache_lock = threading.RLock()
        self.cache_ttl = 30  # 30 seconds cache for real-time data
        
        # Interval mapping for different timeframes
        self.interval_map = {
            '1m': '1m',
            '5m': '5m',
            '15m': '15m',
            '1h': '1h',
            '4h': '1h',  # Yahoo doesn't have 4h, use 1h and aggregate
            '1d': '1d'
        }
        
        # Period mapping for historical data
        self.period_map = {
            '1m': '1d',      # 1 day of 1m data = 390 candles
            '5m': '5d',       # 5 days of 5m data
            '15m': '1mo',     # 1 month of 15m data
            '1h': '3mo',      # 3 months of 1h data
            '4h': '6mo',      # 6 months of 4h data
            '1d': '1y'        # 1 year of daily data
        }
        
        # Initialize symbol mappings
        self._init_symbol_maps()
        
        logger.info(f"Finnhub: {'Connected' if FINNHUB_AVAILABLE else 'Not Installed'}")
        logger.info(f"Alpha Vantage: {'Connected' if ALPHA_VANTAGE_AVAILABLE else 'Not Installed'}")
        logger.info(f"Twelve Data: {'Connected' if TWELVEDATA_AVAILABLE else 'Not Installed'}")
        logger.info(f"Yahoo Finance: Connected")
        logger.info(f"CoinGecko: Available (free crypto data)")
        logger.info(f"Timeframes: 1m, 5m, 15m, 1h, 4h, 1d")
        logger.info("="*60)
    
    def _init_symbol_maps(self):
        """Initialize symbol mappings for different APIs"""
        
        # Yahoo Finance mappings (FIXED for commodities)
        self.yahoo_map = {
            # Crypto
            'BTC-USD': 'BTC-USD',
            'ETH-USD': 'ETH-USD',
            'BNB-USD': 'BNB-USD',
            'SOL-USD': 'SOL-USD',
            'XRP-USD': 'XRP-USD',
            'ADA-USD': 'ADA-USD',
            'DOGE-USD': 'DOGE-USD',
            'DOT-USD': 'DOT-USD',
            'LTC-USD': 'LTC-USD',
            'AVAX-USD': 'AVAX-USD',
            'LINK-USD': 'LINK-USD',
            
            # Forex - FIXED mappings
            'EUR/USD': 'EURUSD=X',
            'GBP/USD': 'GBPUSD=X',
            'USD/JPY': 'JPY=X',           # Yahoo uses JPY=X for USD/JPY
            'AUD/USD': 'AUDUSD=X',
            'USD/CAD': 'CAD=X',            # Yahoo uses CAD=X for USD/CAD
            'NZD/USD': 'NZDUSD=X',
            'USD/CHF': 'CHF=X',            # Yahoo uses CHF=X for USD/CHF
            'EUR/GBP': 'EURGBP=X',
            'EUR/JPY': 'EURJPY=X',
            'GBP/JPY': 'GBPJPY=X',
            'AUD/JPY': 'AUDJPY=X',
            'EUR/AUD': 'EURAUD=X',
            'GBP/AUD': 'GBPAUD=X',
            'AUD/CAD': 'AUDCAD=X',
            'CAD/JPY': 'CADJPY=X',
            'CHF/JPY': 'CHFJPY=X',
            'EUR/CAD': 'EURCAD=X',
            'EUR/CHF': 'EURCHF=X',
            'GBP/CAD': 'GBPCAD=X',
            'GBP/CHF': 'GBPCHF=X',
            
            # Stocks
            'AAPL': 'AAPL',
            'MSFT': 'MSFT',
            'GOOGL': 'GOOGL',
            'AMZN': 'AMZN',
            'TSLA': 'TSLA',
            'NVDA': 'NVDA',
            'META': 'META',
            'JPM': 'JPM',
            'V': 'V',
            'WMT': 'WMT',
            'JNJ': 'JNJ',
            'PG': 'PG',
            'KO': 'KO',
            'PEP': 'PEP',
            
            # Indices
            '^GSPC': '^GSPC',
            '^DJI': '^DJI',
            '^IXIC': '^IXIC',
            '^FTSE': '^FTSE',
            '^N225': '^N225',
            '^HSI': '^HSI',
            '^GDAXI': '^GDAXI',
            
            # Commodities (futures as fallback for spot)
            'XAU/USD': 'GC=F',      # Gold futures
            'XAG/USD': 'SI=F',      # Silver futures
            'XPT/USD': 'PL=F',      # Platinum futures
            'XPD/USD': 'PA=F',      # Palladium futures
            'WTI/USD': 'CL=F',      # WTI Crude futures
            'NG/USD': 'NG=F',       # Natural gas futures
            'XCU/USD': 'HG=F',      # Copper futures
            'GC=F': 'GC=F',          # Gold futures
            'SI=F': 'SI=F',          # Silver futures
            'CL=F': 'CL=F',          # Crude futures
            'NG=F': 'NG=F',          # Gas futures
            'HG=F': 'HG=F',          # Copper futures
        }
        
        # Twelve Data mappings
        self.twelve_map = {
            # Crypto
            'BTC-USD': 'BTC/USD',
            'ETH-USD': 'ETH/USD',
            'BNB-USD': 'BNB/USD',
            'SOL-USD': 'SOL/USD',
            'XRP-USD': 'XRP/USD',
            
            # Forex
            'EUR/USD': 'EUR/USD',
            'GBP/USD': 'GBP/USD',
            'USD/JPY': 'USD/JPY',
            'AUD/USD': 'AUD/USD',
            'USD/CAD': 'USD/CAD',
            'NZD/USD': 'NZD/USD',
            'USD/CHF': 'USD/CHF',
            
            # Stocks
            'AAPL': 'AAPL',
            'MSFT': 'MSFT',
            'GOOGL': 'GOOGL',
            'AMZN': 'AMZN',
            'TSLA': 'TSLA',
            
            # Indices
            '^GSPC': 'SPX',
            '^DJI': 'DJI',
            '^IXIC': 'IXIC',
            '^FTSE': 'FTSE',
            '^N225': 'NIKKEI',
            
            # Spot metals
            'XAU/USD': 'XAU/USD',
            'XAG/USD': 'XAG/USD',
            'XPT/USD': 'XPT/USD',
            'XPD/USD': 'XPD/USD',
            'WTI/USD': 'WTI/USD',
            'NG/USD': 'NG/USD',
            'XCU/USD': 'XCU/USD',
        }
        
        # CoinGecko mappings
        self.coingecko_map = {
            'BTC-USD': 'bitcoin',
            'ETH-USD': 'ethereum',
            'BNB-USD': 'binancecoin',
            'SOL-USD': 'solana',
            'XRP-USD': 'ripple',
            'ADA-USD': 'cardano',
            'DOGE-USD': 'dogecoin',
            'DOT-USD': 'polkadot',
            'LTC-USD': 'litecoin',
            'AVAX-USD': 'avalanche-2',
            'LINK-USD': 'chainlink',
            'MATIC-USD': 'matic-network',
            'UNI-USD': 'uniswap',
            'ATOM-USD': 'cosmos',
            'XLM-USD': 'stellar',
            'ALGO-USD': 'algorand',
        }
    
    def _init_api_clients(self):
        """Initialize API clients"""
        self.finnhub_client = finnhub.Client(self.finnhub_key) if FINNHUB_AVAILABLE else None
        
        if ALPHA_VANTAGE_AVAILABLE:
            self.av_ts = TimeSeries(key=self.alpha_vantage_key, output_format='pandas')
            self.av_fx = ForeignExchange(key=self.alpha_vantage_key, output_format='pandas')
        else:
            self.av_ts = self.av_fx = None
        
        if TWELVEDATA_AVAILABLE:
            self.td_client = TDClient(apikey=self.twelve_data_key)
        else:
            self.td_client = None
    
    def _get_cache_key(self, api: str, symbol: str, interval: str = '1d') -> str:
        """Generate cache key with interval"""
        return f"{api}:{symbol}:{interval}"
    
    def _get_from_cache(self, key: str) -> Optional[float]:
        with self.cache_lock:
            if key in self.cache:
                price, timestamp = self.cache[key]
                if (datetime.now() - timestamp).seconds < self.cache_ttl:
                    return price
                else:
                    del self.cache[key]
        return None
    
    def _save_to_cache(self, key: str, price: float):
        with self.cache_lock:
            self.cache[key] = (price, datetime.now())
    
    # ===== iTick API METHODS =====
    
    def fetch_itick_price(self, asset: str, category: str) -> Optional[float]:
        """iTick price fetch with global rate limit — max 1 call per 2 seconds."""
        import time as _t
        # Global throttle across all threads: never call iTick more than once per 2s
        with getattr(self, '_itick_lock', __import__('threading').Lock()):
            if not hasattr(self, '_itick_lock'):
                self._itick_lock = __import__('threading').Lock()
            _last = getattr(self, '_itick_last_call', 0)
            _gap = _t.time() - _last
            if _gap < 2.0:
                _t.sleep(2.0 - _gap)
            self._itick_last_call = _t.time()
        """
        Fetch price from iTick API - Best for forex and stocks
        """
        try:
            # Map asset to iTick format
            if category == 'forex':
                # Convert EUR/USD to EURUSD
                symbol = asset.replace('/', '')
                url = f"https://api.itick.org/forex/quote"
                params = {
                    "region": "GB",
                    "code": symbol
                }
            elif category == 'stocks':
                # Stocks like AAPL, MSFT
                url = f"https://api.itick.org/stock/quote"
                params = {
                    "region": "US",
                    "code": asset
                }
            elif category == 'crypto':
                # Crypto like BTC-USD
                symbol = asset.replace('-', '')
                url = f"https://api.itick.org/crypto/quote"
                params = {
                    "region": "US",
                    "code": symbol
                }
            else:
                return None
            
            headers = {
                "accept": "application/json",
                "token": self.itick_token
            }
            
            response = self.session.get(url, params=params, headers=headers, timeout=3)
            data = response.json()
            
            # Check data structure properly
            if data.get("code") == 0:
                if "data" in data and data["data"] is not None:
                    # Try different price fields that might exist
                    if "ld" in data["data"]:
                        return float(data["data"]["ld"])  # Latest price
                    elif "c" in data["data"]:
                        return float(data["data"]["c"])   # Close price
                    elif "p" in data["data"]:
                        return float(data["data"]["p"])   # Price
                
        except Exception as e:
            logger.warning(f"iTick {category} error for {asset}: {e}")
        
        return None

    # ===== OilPriceAPI METHODS =====
    
    def fetch_oilprice_commodity(self, symbol: str) -> Optional[float]:
        """
        Fetch commodity prices from OilPriceAPI
        Best for: XAU/USD (Gold), XAG/USD (Silver), WTI/USD (Oil)
        """
        try:
            # Map symbol to OilPriceAPI format
            commodity_map = {
                'XAU/USD': 'GOLD',      # Gold
                'XAG/USD': 'SILVER',    # Silver
                'XPT/USD': 'PLATINUM',  # Platinum
                'XPD/USD': 'PALLADIUM', # Palladium
                'WTI/USD': 'WTI',        # WTI Crude Oil
                'BRENT/USD': 'BRENT',    # Brent Crude Oil
                'NG/USD': 'NATURAL_GAS', # Natural Gas
                'XCU/USD': 'COPPER',     # Copper
            }
            
            oil_symbol = commodity_map.get(symbol)
            if not oil_symbol:
                return None
            
            url = "https://api.oilpriceapi.com/v1/prices/latest"
            headers = {
                "Authorization": f"Bearer {self.oilprice_token}",
                "Content-Type": "application/json"
            }
            
            response = self.session.get(url, headers=headers, timeout=3)
            data = response.json()
            
            if data.get("status") == "success" and "data" in data:
                # Get the price for specific commodity
                price_key = f"price_{oil_symbol.lower()}"
                if price_key in data["data"]:
                    return float(data["data"][price_key])
            
        except Exception as e:
            logger.warning(f"OilPriceAPI error for {symbol}: {e}")
        
        return None

    # ===== COINGECKO API (FREE, NO KEY NEEDED) =====
    
    def fetch_coingecko_price(self, symbol: str) -> Optional[float]:
        """
        Fetch crypto price from CoinGecko - FREE, no API key required!
        """
        try:
            coin_id = self.coingecko_map.get(symbol)
            if not coin_id:
                return None
            
            url = f"https://api.coingecko.com/api/v3/simple/price?ids={coin_id}&vs_currencies=usd"
            response = self.session.get(url, timeout=5)
            data = response.json()
            
            if coin_id in data and 'usd' in data[coin_id]:
                return float(data[coin_id]['usd'])
                
        except Exception as e:
            logger.warning(f"CoinGecko error for {symbol}: {e}")
        
        return None
    
    def fetch_coingecko_historical(self, symbol: str, days: int = 30) -> Optional[pd.DataFrame]:
        """
        Fetch historical crypto data from CoinGecko
        """
        try:
            coin_id = self.coingecko_map.get(symbol)
            if not coin_id:
                return None
            
            url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart"
            params = {
                'vs_currency': 'usd',
                'days': days,
                'interval': 'daily'
            }
            
            response = self.session.get(url, params=params, timeout=5)
            data = response.json()
            
            if 'prices' in data and len(data['prices']) > 0:
                df = pd.DataFrame(data['prices'], columns=['timestamp', 'close'])
                df['date'] = pd.to_datetime(df['timestamp'], unit='ms')
                df.set_index('date', inplace=True)
                
                # Add OHLC approximations (CoinGecko only gives close)
                df['open'] = df['close']
                df['high'] = df['close']
                df['low'] = df['close']
                df['volume'] = 0
                
                return df[['open', 'high', 'low', 'close', 'volume']]
                
        except Exception as e:
            logger.warning(f"CoinGecko historical error for {symbol}: {e}")
        
        return None

    # ===== Free Crypto News API =====
    
    def fetch_crypto_news_sentiment(self, asset: str) -> Dict[str, Any]:
        """
        Fetch crypto news and calculate sentiment
        No API key required!
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
            }
            
            query = search_map.get(asset, asset.replace('-USD', ''))
            
            # Get news articles - no API key needed!
            url = f"https://cryptocurrency.cv/api/news"
            params = {
                "q": query,
                "limit": 10,
                "sort": "recent"
            }
            
            response = self.session.get(url, params=params, timeout=3)
            data = response.json()
            
            if "articles" in data and len(data["articles"]) > 0:
                # Analyze sentiment of articles
                sentiments = []
                for article in data["articles"][:5]:
                    title = article.get("title", "")
                    # Simple sentiment analysis (you can use TextBlob here)
                    from textblob import TextBlob
                    blob = TextBlob(title)
                    sentiment = blob.sentiment.polarity
                    sentiments.append(sentiment)
                
                avg_sentiment = sum(sentiments) / len(sentiments) if sentiments else 0
                
                return {
                    'score': avg_sentiment,
                    'articles': len(sentiments),
                    'source': 'CryptoNews',
                    'interpretation': self._interpret_sentiment(avg_sentiment)
                }
            
        except Exception as e:
            logger.warning(f"CryptoNews error for {asset}: {e}")
        
        return {
            'score': 0,
            'articles': 0,
            'source': 'CryptoNews',
            'interpretation': 'Neutral'
        }
    
    def _interpret_sentiment(self, score: float) -> str:
        """Helper method to interpret sentiment score"""
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
    
    # ===== FINNHUB API METHODS =====
    
    def fetch_finnhub_stock(self, symbol: str) -> Optional[float]:
        """Real-time stock from Finnhub using quote()"""
        if not self.finnhub_client:
            return None
        
        try:
            quote = self.finnhub_client.quote(symbol)
            if quote and 'c' in quote:  # 'c' is current price
                return float(quote['c'])
        except Exception as e:
            logger.warning(f"Finnhub stock error for {symbol}: {e}")
        return None
    
    def fetch_finnhub_crypto(self, symbol: str) -> Optional[float]:
        """Real-time crypto from Finnhub using quote() with BINANCE prefix"""
        if not self.finnhub_client:
            return None
        
        try:
            # Convert BTC-USD to BINANCE:BTCUSDT format
            base = symbol.split('-')[0]
            # Handle different crypto pairs
            finnhub_symbol = f"BINANCE:{base}USDT"
            
            quote = self.finnhub_client.quote(finnhub_symbol)
            
            if quote and 'c' in quote:
                return float(quote['c'])
                
        except Exception as e:
            logger.warning(f"Finnhub crypto error for {symbol}: {e}")
        return None
    
    def fetch_finnhub_forex(self, pair: str) -> Optional[float]:
        """Finnhub forex requires a paid plan (403 on free tier) — skip it."""
        return None
        # ↓ original code kept for reference but never reached
        _FINNHUB_FOREX_DISABLED = True
        if _FINNHUB_FOREX_DISABLED:
            return None
        """Real-time forex from Finnhub using forex_rates()"""
        if not self.finnhub_client:
            return None
        
        try:
            # Parse forex pair (e.g., "EUR/USD" -> base="EUR", quote="USD")
            base = pair[:3]
            quote_currency = pair[4:]
            
            # Get forex rates
            rates = self.finnhub_client.forex_rates(base=base)
            
            if rates and 'quote' in rates and quote_currency in rates['quote']:
                return float(rates['quote'][quote_currency])
                
        except Exception as e:
            logger.warning(f"Finnhub forex error for {pair}: {e}")
        return None
    
    # ===== ALPHA VANTAGE METHODS - FIXED FOR ALL ASSETS =====
    
    def fetch_alphavantage_stock(self, symbol: str) -> Optional[float]:
        """Stock price from Alpha Vantage using GLOBAL_QUOTE"""
        try:
            url = "https://www.alphavantage.co/query"
            params = {
                'function': 'GLOBAL_QUOTE',
                'symbol': symbol,
                'apikey': self.alpha_vantage_key
            }
            response = self.session.get(url, params=params, timeout=3)
            data = response.json()
            
            if 'Global Quote' in data and '05. price' in data['Global Quote']:
                return float(data['Global Quote']['05. price'])
                
        except Exception as e:
            logger.warning(f"Alpha Vantage stock error for {symbol}: {e}")
        return None
    
    def fetch_alphavantage_forex(self, pair: str) -> Optional[float]:
        """Forex from Alpha Vantage using CURRENCY_EXCHANGE_RATE"""
        try:
            from_currency = pair[:3]
            to_currency = pair[4:]
            
            url = "https://www.alphavantage.co/query"
            params = {
                'function': 'CURRENCY_EXCHANGE_RATE',
                'from_currency': from_currency,
                'to_currency': to_currency,
                'apikey': self.alpha_vantage_key
            }
            response = self.session.get(url, params=params, timeout=3)
            data = response.json()
            
            if 'Realtime Currency Exchange Rate' in data:
                rate_data = data['Realtime Currency Exchange Rate']
                if '5. Exchange Rate' in rate_data:
                    return float(rate_data['5. Exchange Rate'])
                    
        except Exception as e:
            logger.warning(f"Alpha Vantage forex error for {pair}: {e}")
        return None
    
    def fetch_alphavantage_crypto(self, symbol: str) -> Optional[float]:
        """Crypto from Alpha Vantage using CURRENCY_EXCHANGE_RATE (works for all cryptos)"""
        try:
            base = symbol.split('-')[0]
            
            url = "https://www.alphavantage.co/query"
            params = {
                'function': 'CURRENCY_EXCHANGE_RATE',
                'from_currency': base,
                'to_currency': 'USD',
                'apikey': self.alpha_vantage_key
            }
            response = self.session.get(url, params=params, timeout=3)
            data = response.json()
            
            if 'Realtime Currency Exchange Rate' in data:
                rate_data = data['Realtime Currency Exchange Rate']
                if '5. Exchange Rate' in rate_data:
                    return float(rate_data['5. Exchange Rate'])
                    
        except Exception as e:
            logger.warning(f"Alpha Vantage crypto error for {symbol}: {e}")
        return None
    
    def fetch_alphavantage_commodity(self, symbol: str) -> Optional[float]:
        """Commodity from Alpha Vantage using dedicated endpoints"""
        commodity_map = {
            # Spot mappings
            'XAU/USD': 'GOLD',      # Gold Spot
            'XAG/USD': 'SILVER',    # Silver Spot
            'WTI/USD': 'WTI',       # WTI Crude Oil Spot
            'NG/USD': 'NATURAL_GAS', # Natural Gas Spot
            
            # Futures (keep for backward compatibility)
            'GC=F': 'GOLD',          # Gold Futures
            'SI=F': 'SILVER',        # Silver Futures
            'CL=F': 'WTI',           # WTI Futures
            'NG=F': 'NATURAL_GAS',   # Natural Gas Futures
        }
        
        if symbol not in commodity_map:
            return None
        
        try:
            url = "https://www.alphavantage.co/query"
            params = {
                'function': commodity_map[symbol],
                'interval': 'daily',
                'apikey': self.alpha_vantage_key
            }
            response = self.session.get(url, params=params, timeout=3)
            data = response.json()
            
            # Different commodities have different response structures
            if 'data' in data and len(data['data']) > 0:
                return float(data['data'][0]['value'])
            elif 'values' in data and len(data['values']) > 0:
                return float(data['values'][0]['value'])
                
        except Exception as e:
            logger.warning(f"Alpha Vantage commodity error for {symbol}: {e}")
        return None
    
    # ===== TWELVE DATA METHODS (SUPPORTS 4H!) =====
    
    def fetch_twelvedata_price(self, symbol: str) -> Optional[float]:
        """Price from Twelve Data - works for stocks, ETFs, commodities, and spot metals"""
        if not self.td_client:
            return None
        
        # Use mapped symbol if available, otherwise use original
        twelve_symbol = self.twelve_map.get(symbol, symbol)
        
        try:
            ts = self.td_client.time_series(
                symbol=twelve_symbol,
                interval='1min',
                outputsize=1
            )
            data = ts.as_json()
            if data and len(data) > 0:
                price = float(data[0]['close'])
                return price
        except Exception as e:
            # Silently fail - Yahoo will be fallback
            pass
        
        return None
    
    def _fetch_twelve_historical(self, asset: str, interval: str) -> pd.DataFrame:
        """Fetch from Twelve Data"""
        try:
            twelve_symbol = self.twelve_map.get(asset, asset.replace('-USD', '/USD'))
            
            # Map interval
            interval_map = {
                '1m': '1min', '5m': '5min', '15m': '15min',
                '1h': '1h', '4h': '4h', '1d': '1day'
            }
            twelve_interval = interval_map.get(interval, '1day')
            
            # Get data from Twelve Data
            ts = self.td_client.time_series(
                symbol=twelve_symbol,
                interval=twelve_interval,
                outputsize=100
            )
            
            data = ts.as_json()
            if data and len(data) > 0:
                df = pd.DataFrame(data)
                df['datetime'] = pd.to_datetime(df['datetime'])
                df.set_index('datetime', inplace=True)
                df = df.astype(float)
                df.index.name = 'date'
                
                # Use the safe dataframe helper
                return self._safe_dataframe(df, asset)
            else:
                logger.warning(f"Twelve Data: No data for {asset}")
                
        except Exception as e:
            logger.warning(f"Twelve Data error for {asset}: {e}")
        
        return pd.DataFrame()
    
    # ===== YAHOO FINANCE =====
    
    def fetch_yahoo_price(self, symbol: str, interval: str = '1m') -> Optional[float]:
        """Universal fallback - Yahoo Finance with interval support"""
        try:
            # Map interval to Yahoo format
            yahoo_interval = self.interval_map.get(interval, '1m')
            
            # For real-time price, use 1m data
            if interval in ['1m', '5m', '15m']:
                # Get appropriate period based on interval
                period = '1d' if interval == '1m' else '5d'
                ticker = yf.Ticker(symbol)
                data = ticker.history(period=period, interval=yahoo_interval)
                if not data.empty:
                    return float(data['Close'].iloc[-1])
            
            # Fallback to 1m data
            ticker = yf.Ticker(symbol)
            data = ticker.history(period='1d', interval='1m')
            if not data.empty:
                return float(data['Close'].iloc[-1])
            
            # Try 5m data as last resort
            data = ticker.history(period='5d', interval='5m')
            if not data.empty:
                return float(data['Close'].iloc[-1])
            
            # Try to get quote from info (yfinance >= 0.2.x returns Response, not dict)
            # Use fast_info first (always a proper object), then safe-cast .info
            try:
                fast = ticker.fast_info
                price = getattr(fast, 'last_price', None) or getattr(fast, 'regularMarketPrice', None)
                if price:
                    return float(price)
            except Exception:
                pass
            try:
                info = ticker.info
                # Safely convert to dict — older yfinance returns dict, newer returns Response
                if not isinstance(info, dict):
                    info = dict(info) if hasattr(info, 'items') else {}
                if info.get('regularMarketPrice'):
                    return float(info['regularMarketPrice'])
                if info.get('currentPrice'):
                    return float(info['currentPrice'])
                if info.get('ask'):
                    return float(info['ask'])
                if info.get('bid'):
                    return float(info['bid'])
            except Exception:
                pass
                
        except Exception as e:
            logger.warning(f"Yahoo error for {symbol}: {str(e)[:50]}")
        return None
    
    # ===== HISTORICAL DATA METHODS (for backtesting) =====
    
    def fetch_yahoo_historical(self, symbol: str, interval: str = '1d', period: str = '1mo') -> pd.DataFrame:
        """
        Fetch historical OHLCV data from Yahoo Finance
        Supports: 1m, 5m, 15m, 1h, 1d
        """
        try:
            # Map interval to Yahoo format
            yahoo_interval = self.interval_map.get(interval, '1d')
            
            # Map period if not provided
            if period == '1mo' and interval in self.period_map:
                period = self.period_map.get(interval, '1mo')
            
            ticker = yf.Ticker(symbol)
            df = ticker.history(period=period, interval=yahoo_interval)
            
            if not df.empty:
                df.columns = df.columns.str.lower()
                df.index.name = 'date'
                
                # For 4h requests, resample from 1h
                if interval == '4h' and yahoo_interval == '1h':
                    df = df.resample('4H').agg({
                        'open': 'first',
                        'high': 'max',
                        'low': 'min',
                        'close': 'last',
                        'volume': 'sum'
                    }).dropna()
                    logger.info(f"Resampled to 4h: {len(df)} candles")
                
                return df
                
        except Exception as e:
            logger.warning(f"Yahoo historical error for {symbol}: {e}")
        
        return pd.DataFrame()
    
    def get_historical_data(self, asset: str, interval: str = '1d', days: int = 100) -> pd.DataFrame:
        """
        Get historical data with multiple API fallbacks
        AUTO-FIX: Switches to daily data if intraday period exceeds Yahoo's 60-day limit
        """
        
        # ===== AUTO-DETECT AND FIX YAHOO 60-DAY LIMIT =====
        original_interval = interval
        original_days = days
        
        # Check if we're requesting intraday data beyond Yahoo's limit
        if interval in ['1m', '5m', '15m'] and days > 60:
            logger.warning(f"⚠️ Yahoo {interval} data limited to 60 days. Requested: {days} days")
            logger.warning(f"   Automatically switching to daily data (1d) for {asset}")
            interval = '1d'
            days = min(days, 365)  # Still get up to a year of daily data
        # ===================================================
        
        # Determine category
        category = self._get_asset_category(asset)
        
        # Try category-specific sources first
        if category == 'crypto':
            # Try CoinGecko first (free, reliable)
            df = self.fetch_coingecko_historical(asset, days)
            if df is not None and not df.empty:
                # Log if we switched intervals
                if original_interval != interval:
                    logger.info(f"✅ Got {len(df)} rows of {interval} data for {asset} (auto-switched from {original_interval})")
                return df
        
        # Try Yahoo (most reliable for historical)
        yahoo_symbol = self.yahoo_map.get(asset, asset)
        df = self.fetch_yahoo_historical(yahoo_symbol, interval, f"{days}d")
        
        if not df.empty:
            # Log if we switched intervals
            if original_interval != interval:
                logger.info(f"✅ Got {len(df)} rows of {interval} data for {asset} (auto-switched from {original_interval})")
            return df
        
        # Try Twelve Data
        if self.td_client:
            try:
                twelve_symbol = self.twelve_map.get(asset, asset)
                interval_map = {
                    '1d': '1day',
                    '1h': '1h',
                    '15m': '15min',
                    '5m': '5min',
                }
                twelve_interval = interval_map.get(interval, '1day')
                
                ts = self.td_client.time_series(
                    symbol=twelve_symbol,
                    interval=twelve_interval,
                    outputsize=days
                )
                data = ts.as_json()
                
                if data and len(data) > 0:
                    df = pd.DataFrame(data)
                    df['datetime'] = pd.to_datetime(df['datetime'])
                    df.set_index('datetime', inplace=True)
                    df = df.astype(float)
                    df.index.name = 'date'
                    
                    # Log if we switched intervals
                    if original_interval != interval:
                        logger.info(f"✅ Got {len(df)} rows of {interval} data for {asset} from Twelve Data (auto-switched)")
                    return df[['open', 'high', 'low', 'close', 'volume']]
            except:
                pass
        
        # If we switched intervals and still got no data, try the original interval one last time
        if original_interval != interval and original_days <= 60:
            logger.info(f"Trying original {original_interval} data for {asset} (within 60-day limit)")
            return self.get_historical_data(asset, original_interval, min(original_days, 60))

        # 4h fallback: Yahoo doesn't serve 4h for stocks/indices natively.
        # If 4h returned nothing, silently fall back to 1d which Yahoo always has.
        if interval == '4h' and df.empty:
            logger.debug(f"4h unavailable for {asset} — falling back to 1d")
            yahoo_symbol = self.yahoo_map.get(asset, asset)
            df = self.fetch_yahoo_historical(yahoo_symbol, '1d', self._days_to_yahoo_period(original_days))
            if not df.empty:
                return df
        
        return pd.DataFrame()
    
    def _get_asset_category(self, asset: str) -> str:
        """Determine asset category from symbol"""
        # Spot commodity symbols that contain '/' must be checked BEFORE the forex catch-all
        _COMMODITY_SPOTS = {'XAU/USD','XAG/USD','XPT/USD','XPD/USD',
                            'WTI/USD','BRENT/USD','NG/USD','XCU/USD'}
        if asset in self.coingecko_map or '-USD' in asset:
            return 'crypto'
        elif asset in _COMMODITY_SPOTS:
            return 'commodities'
        elif '/' in asset:
            return 'forex'
        elif asset.startswith('^'):
            return 'indices'
        elif asset in ['GC=F', 'SI=F', 'CL=F', 'NG=F', 'HG=F']:
            return 'commodities'
        elif '=F' in asset:
            return 'commodities'
        else:
            return 'stocks'
        
    def enable_websocket(self, trading_system):
        """
        Enable WebSocket for real-time data with full integration
        """
        try:
            from websocket_manager import WebSocketManager
            from websocket_handlers import WebSocketHandlers
            
            self.ws_manager = WebSocketManager()
            self.ws_handlers = WebSocketHandlers(trading_system)
            
            # Start WebSocket
            self.ws_manager.start()
            
            # Subscribe to all crypto
            crypto_symbols = [
                'BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'SOLUSDT', 'XRPUSDT',
                'ADAUSDT', 'DOGEUSDT', 'DOTUSDT', 'LTCUSDT', 'AVAXUSDT',
                'LINKUSDT', 'MATICUSDT'
            ]
            self.ws_manager.subscribe_bybit(crypto_symbols, self.ws_handlers.on_price_update)
            
            logger.info(f"✅ WebSocket enabled: {len(crypto_symbols)} crypto, {len(stock_symbols)} stocks, {len(forex_symbols)} forex")
            return True
            
        except Exception as e:
            logger.error(f"❌ WebSocket enable failed: {e}")
            return False

    def stop_websocket(self):
        """Stop WebSocket connections"""
        if hasattr(self, 'ws_manager'):
            self.ws_manager.stop()
            logger.info("📡 WebSocket stopped")
        
    # ===== MULTI-API FETCH WITH PARALLEL EXECUTION =====

    def _safe_dataframe(self, df, asset_name):
        """Ensure DataFrame has all required columns"""
        required_cols = ['open', 'high', 'low', 'close']
        
        # Check required columns
        missing = [col for col in required_cols if col not in df.columns]
        if missing:
            logger.warning(f"Missing required columns for {asset_name}: {missing}")
            return pd.DataFrame()
        
        # Add volume if missing
        if 'volume' not in df.columns:
            df['volume'] = 0
        
        # Return only the columns we want, in the right order
        return df[['open', 'high', 'low', 'close', 'volume']]
    
    def get_real_time_price(self, asset: str, category: str, interval: str = '1m') -> Tuple[Optional[float], str]:
        """
        Try ALL APIs in parallel, return fastest valid price with source
        Now with caching to reduce API calls
        """
        # Check market hours first
        market_status = MarketHours.get_status().get(category, False)
        if not market_status:
            return None, f"Market Closed ({MarketHours.get_status_message(category)})"
        
        # ===== CHECK CACHE FIRST =====
        cache_key = f"price:{asset}:{category}"
        cached = self._get_from_cache(cache_key)
        if cached:
            return cached, "Cache"
        
        # Define API sources based on category
        sources = []
        
        if category == 'forex':
            sources = [
                ('iTick', lambda: self.fetch_itick_price(asset, 'forex')),
                ('Finnhub', lambda: self.fetch_finnhub_forex(asset)),
                ('AlphaVantage', lambda: self.fetch_alphavantage_forex(asset)),
                ('TwelveData', lambda: self.fetch_twelvedata_price(asset)),
                ('Yahoo', lambda: self.fetch_yahoo_price(self._to_yahoo_forex(asset), interval))
            ]
        elif category == 'crypto':
            sources = [
                ('CoinGecko', lambda: self.fetch_coingecko_price(asset)),  # FREE, no key needed
                ('iTick', lambda: self.fetch_itick_price(asset, 'crypto')),
                ('Finnhub', lambda: self.fetch_finnhub_crypto(asset)),
                ('AlphaVantage', lambda: self.fetch_alphavantage_crypto(asset)),
                ('TwelveData', lambda: self.fetch_twelvedata_price(asset)),
                ('Yahoo', lambda: self.fetch_yahoo_price(asset, interval))
            ]
        elif category == 'stocks':
            sources = [
                ('iTick', lambda: self.fetch_itick_price(asset, 'stocks')),
                ('Finnhub', lambda: self.fetch_finnhub_stock(asset)),
                ('AlphaVantage', lambda: self.fetch_alphavantage_stock(asset)),
                ('TwelveData', lambda: self.fetch_twelvedata_price(asset)),
                ('Yahoo', lambda: self.fetch_yahoo_price(asset, interval))
            ]
        elif category == 'commodities':
            sources = [
                ('OilPriceAPI', lambda: self.fetch_oilprice_commodity(asset)),
                ('TwelveData', lambda: self.fetch_twelvedata_price(self._to_twelvedata_commodity(asset))),
                ('AlphaVantage', lambda: self.fetch_alphavantage_commodity(asset)),
                ('Yahoo', lambda: self.fetch_yahoo_price(self._to_yahoo_commodity(asset), interval))
            ]
        elif category == 'indices':
            sources = [
                ('iTick', lambda: self.fetch_itick_price(asset, 'stocks')),
                ('Yahoo', lambda: self.fetch_yahoo_price(asset, interval)),
                ('TwelveData', lambda: self.fetch_twelvedata_price(self._to_twelvedata_index(asset))),
                ('Finnhub', lambda: self.fetch_finnhub_stock(asset.replace('^', ''))),
            ]
        else:
            sources = [('Yahoo', lambda: self.fetch_yahoo_price(asset, interval))]
        
        # Try ALL sources in parallel with timeout
        results = []
        with ThreadPoolExecutor(max_workers=len(sources)) as executor:
            future_to_source = {
                executor.submit(func): source_name 
                for source_name, func in sources
            }
            
            for future in as_completed(future_to_source):
                source_name = future_to_source[future]
                try:
                    price = future.result(timeout=3)
                    if price and price > 0:
                        # Cache successful result
                        self._save_to_cache(cache_key, price)
                        return price, source_name
                except Exception as e:
                    results.append(f"⚠️ {source_name}: {str(e)[:50]}")
                    continue
        
        # Log failures at debug level only
        if results:
            logger.debug(f"{asset} - " + " | ".join(results[:2]))
        
        return None, "All APIs failed"
    
    def _to_yahoo_forex(self, pair: str) -> str:
        """Convert forex pair to Yahoo Finance symbol - FIXED VERSION"""
        yahoo_map = {
            'EUR/USD': 'EURUSD=X',
            'GBP/USD': 'GBPUSD=X',
            'USD/JPY': 'JPY=X',           # Yahoo uses JPY=X for USD/JPY
            'AUD/USD': 'AUDUSD=X',
            'USD/CAD': 'CAD=X',            # Yahoo uses CAD=X for USD/CAD
            'NZD/USD': 'NZDUSD=X',
            'USD/CHF': 'CHF=X',            # Yahoo uses CHF=X for USD/CHF
            'EUR/GBP': 'EURGBP=X',
            'EUR/JPY': 'EURJPY=X',
            'GBP/JPY': 'GBPJPY=X',
            'AUD/JPY': 'AUDJPY=X',
            'EUR/AUD': 'EURAUD=X',
            'GBP/AUD': 'GBPAUD=X',
        }
        
        # Try direct mapping first
        if pair in yahoo_map:
            return yahoo_map[pair]
        
        # Fallback: replace / with nothing and add =X
        return pair.replace('/', '') + '=X'

    def _to_yahoo_commodity(self, symbol: str) -> str:
        """Convert spot symbols to Yahoo format if needed"""
        yahoo_map = {
            'XAU/USD': 'GC=F',      # Gold Spot → Gold Futures
            'XAG/USD': 'SI=F',      # Silver Spot → Silver Futures
            'XPT/USD': 'PL=F',      # Platinum Spot → Platinum Futures
            'XPD/USD': 'PA=F',      # Palladium Spot → Palladium Futures
            'WTI/USD': 'CL=F',      # WTI Spot → WTI Futures
            'BRENT/USD': 'BZ=F',    # Brent Spot → Brent Futures
            'NG/USD': 'NG=F',       # Natural Gas Spot → Natural Gas Futures
            'XCU/USD': 'HG=F',      # Copper Spot → Copper Futures
            # Keep futures as-is
            'GC=F': 'GC=F',
            'SI=F': 'SI=F',
            'CL=F': 'CL=F',
            'NG=F': 'NG=F',
            'HG=F': 'HG=F',
        }
        return yahoo_map.get(symbol, symbol)
    
    def _to_twelvedata_commodity(self, asset: str) -> str:
        """Convert commodity to Twelve Data format"""
        mapping = {
            'GC=F': 'GC',          # Gold Futures
            'SI=F': 'SI',          # Silver Futures
            'CL=F': 'CL',          # Crude Oil
            'NG=F': 'NG',          # Natural Gas
            'HG=F': 'HG',          # Copper
            'XAU/USD': 'XAU/USD',  # Gold Spot
            'XAG/USD': 'XAG/USD',  # Silver Spot
            'WTI/USD': 'WTI/USD',  # WTI Spot
            'NG/USD': 'NG/USD',    # Natural Gas Spot
            'XCU/USD': 'XCU/USD',  # Copper Spot
        }
        return mapping.get(asset, asset)
    
    def _to_twelvedata_index(self, asset: str) -> str:
        """Convert index to Twelve Data format"""
        mapping = {
            '^GSPC': 'SPX',
            '^DJI': 'DJI',
            '^IXIC': 'IXIC',
            '^FTSE': 'FTSE',
            '^N225': 'NIKKEI',
        }
        return mapping.get(asset, asset.replace('^', ''))
    
    def get_market_status(self) -> Dict:
        """Get current market status"""
        return MarketHours.get_status()


# Backward compatibility
class DataFetcher(NASALevelFetcher):
    pass