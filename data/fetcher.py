"""
⚡ ULTIMATE MULTI-API FETCHER - Real-time data from ALL sources
With ACCURATE market hours for EAT timezone (UTC+3)
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
    - SUPPORTS: 1m, 5m, 15m, 1h, 4h, 1d for day trading
    """
    
    def __init__(self):
        print("\n" + "="*60)
        print(" INITIALIZING ULTIMATE MULTI-API FETCHER")
        print("="*60 + "\n")
        
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
        retry_strategy = Retry(total=2, backoff_factor=0.5)
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
        
        print(f"[OK] Finnhub: {'Connected' if FINNHUB_AVAILABLE else 'Not Installed'}")
        print(f"[OK] Alpha Vantage: {'Connected' if ALPHA_VANTAGE_AVAILABLE else 'Not Installed'}")
        print(f"[OK] Twelve Data: {'Connected' if TWELVEDATA_AVAILABLE else 'Not Installed'}")
        print(f"[OK] Yahoo Finance: Connected")
        print(f"[OK] Timeframes: 1m, 5m, 15m, 1h, 4h, 1d")
        print("="*60 + "\n")
    
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
            print(f"[WARN] iTick {category} error for {asset}: {e}")
        
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
            print(f"[WARN] OilPriceAPI error for {symbol}: {e}")
        
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
            print(f"[WARN] CryptoNews error for {asset}: {e}")
        
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
            print(f"[WARN] Finnhub stock error: {e}")
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
            print(f"[WARN] Finnhub crypto error: {e}")
        return None
    
    def fetch_finnhub_forex(self, pair: str) -> Optional[float]:
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
            print(f"[WARN] Finnhub forex error: {e}")
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
            print(f"[WARN] Alpha Vantage stock error: {e}")
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
            print(f"[WARN] Alpha Vantage forex error: {e}")
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
            print(f"[WARN] Alpha Vantage crypto error: {e}")
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
            print(f"[WARN] Alpha Vantage commodity error: {e}")
        return None
    
    # ===== TWELVE DATA METHODS (SUPPORTS 4H!) =====
    
    def fetch_twelvedata_price(self, symbol: str) -> Optional[float]:
        """Price from Twelve Data - works for stocks, ETFs, commodities, and spot metals"""
        if not self.td_client:
            return None
        
        # Add symbol mapping for spot metals and futures
        commodity_map = {
            # ===== SPOT METALS (Twelve Data format) =====
            'XAU/USD': 'XAU/USD',      # Gold Spot
            'XAG/USD': 'XAG/USD',      # Silver Spot
            'XPT/USD': 'XPT/USD',      # Platinum Spot
            'XPD/USD': 'XPD/USD',      # Palladium Spot
            'WTI/USD': 'WTI/USD',      # WTI Crude Oil Spot
            'NG/USD': 'NG/USD',        # Natural Gas Spot
            'XCU/USD': 'XCU/USD',      # Copper Spot
            
            # ===== FUTURES (keep for backward compatibility) =====
            'GC=F': 'GC',               # Gold Futures
            'SI=F': 'SI',               # Silver Futures
            'CL=F': 'CL',               # Crude Oil Futures
            'NG=F': 'NG',               # Natural Gas Futures
            'HG=F': 'HG',               # Copper Futures
            'PL=F': 'PL',               # Platinum Futures
            'PA=F': 'PA',               # Palladium Futures
        }
        
        # Use mapped symbol if available, otherwise use original
        twelve_symbol = commodity_map.get(symbol, symbol)
        
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
            # Map Yahoo symbols to Twelve Data format
            symbol_map = {
                # Crypto
                'BTC-USD': 'BTC/USD', 
                'ETH-USD': 'ETH/USD',
                'BNB-USD': 'BNB/USD',
                'SOL-USD': 'SOL/USD',
                'XRP-USD': 'XRP/USD',
                
                # Stocks
                'AAPL': 'AAPL', 
                'MSFT': 'MSFT',
                'GOOGL': 'GOOGL',
                'AMZN': 'AMZN',
                'TSLA': 'TSLA',
                'NVDA': 'NVDA',
                
                # Forex
                'EUR/USD': 'EUR/USD', 
                'GBP/USD': 'GBP/USD',
                'USD/JPY': 'USD/JPY',
                'AUD/USD': 'AUD/USD',
                'USD/CAD': 'USD/CAD',
                
                # Spot Metals
                'XAU/USD': 'XAU/USD',
                'XAG/USD': 'XAG/USD',
                'XPT/USD': 'XPT/USD',
                'XPD/USD': 'XPD/USD',

                # Indices - FIX THESE
                '^GSPC': 'SPX',      # S&P 500
                '^DJI': 'DJI',       # Dow Jones
                '^IXIC': 'IXIC',     # Nasdaq
                '^FTSE': 'FTSE',     # FTSE 100
                '^N225': 'NIKKEI',   # Nikkei 225
            }
            
            twelve_symbol = symbol_map.get(asset, asset.replace('-USD', '/USD'))
            
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
                print(f"   ⚠️ Twelve Data: No data for {asset}")
                
        except Exception as e:
            print(f"   ⚠️ Twelve Data error: {e}")
        
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
                
        except Exception as e:
            print(f"[WARN] Yahoo error: {e}")
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
                    print(f"   📊 Resampled to 4h: {len(df)} candles")
                
                return df
                
        except Exception as e:
            print(f"[WARN] Yahoo historical error: {e}")
        
        return pd.DataFrame()
    
    # ===== MULTI-API FETCH WITH PARALLEL EXECUTION =====

    def _safe_dataframe(self, df, asset_name):
        """Ensure DataFrame has all required columns"""
        required_cols = ['open', 'high', 'low', 'close']
        
        # Check required columns
        missing = [col for col in required_cols if col not in df.columns]
        if missing:
            print(f"   ⚠️ Missing required columns for {asset_name}: {missing}")
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
        if hasattr(self, 'cache_manager') and self.cache_manager and self.cache_manager.enabled:
            # Try to get from cache first
            cached_price = self.cache_manager.get_price(asset, "any")
            if cached_price:
                return cached_price, "Cache"
        # =============================
        
        # Define API sources based on category
        sources = []
        
        if category == 'forex':
            sources = [
                ('iTick', lambda: self.fetch_itick_price(asset, 'forex')),
                ('Finnhub', lambda: self.fetch_finnhub_forex(asset)),
                ('AlphaVantage', lambda: self.fetch_alphavantage_forex(asset)),
                ('Yahoo', lambda: self.fetch_yahoo_price(self._to_yahoo_forex(asset), interval))
            ]
        elif category == 'crypto':
            sources = [
                ('iTick', lambda: self.fetch_itick_price(asset, 'crypto')),
                ('Finnhub', lambda: self.fetch_finnhub_crypto(asset)),
                ('AlphaVantage', lambda: self.fetch_alphavantage_crypto(asset)),
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
                ('TwelveData', lambda: self.fetch_twelvedata_price(asset)),
                ('AlphaVantage', lambda: self.fetch_alphavantage_commodity(asset)),
                ('Yahoo', lambda: self.fetch_yahoo_price(self._to_yahoo_commodity(asset), interval))
            ]
        elif category == 'indices':
            sources = [
                ('iTick', lambda: self.fetch_itick_price(asset, 'stocks')),
                ('Yahoo', lambda: self.fetch_yahoo_price(asset, interval)),
                ('TwelveData', lambda: self.fetch_twelvedata_price(asset)),
            ]
        else:
            sources = [('Yahoo', lambda: self.fetch_yahoo_price(asset, interval))]
        
        # Check cache for each source
        for source_name, _ in sources:
            cache_key = self._get_cache_key(source_name, asset, interval)
            cached = self._get_from_cache(cache_key)
            if cached:
                return cached, f"{source_name} (cached)"
        
        # Try ALL sources in parallel with timeout
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
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
                        # Cache successful result in both caches
                        cache_key = self._get_cache_key(source_name, asset, interval)
                        self._save_to_cache(cache_key, price)
                        
                        # Also save to Redis cache if available
                        if hasattr(self, 'cache_manager') and self.cache_manager:
                            self.cache_manager.set_price(asset, source_name, price, ttl=30)
                        
                        return price, source_name
                except Exception as e:
                    results.append(f"⚠️ {source_name}: {str(e)[:50]}")
                    continue
        
        # Log failures at debug level only
        if results:
            print(f"  [LOG] {asset} - " + " | ".join(results[:2]))
        
        return None, "All APIs failed"
    
    def _to_yahoo_forex(self, pair: str) -> str:
        """Convert forex pair to Yahoo Finance symbol - FIXED VERSION"""
        yahoo_map = {
            # Majors
            'EUR/USD': 'EURUSD=X',
            'GBP/USD': 'GBPUSD=X',
            'USD/JPY': 'USDJPY=X',      # Fixed
            'AUD/USD': 'AUDUSD=X',
            'USD/CAD': 'USDCAD=X',       # Fixed
            'NZD/USD': 'NZDUSD=X',
            'USD/CHF': 'USDCHF=X',       # Fixed
            
            # Crosses
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
        }
        
        # Try direct mapping first, then fallback to conversion
        if pair in yahoo_map:
            return yahoo_map[pair]
        
        # Fallback: replace / with nothing and add =X
        return pair.replace('/', '') + '=X'

    def _to_yahoo_commodity(self, symbol: str) -> str:
        """Convert spot symbols to Yahoo format if needed"""
        yahoo_map = {
            # Spot to futures mapping (Yahoo doesn't have spot)
            'XAU/USD': 'GC=F',      # Gold Spot → Gold Futures
            'XAG/USD': 'SI=F',      # Silver Spot → Silver Futures
            'XPT/USD': 'PL=F',      # Platinum Spot → Platinum Futures
            'XPD/USD': 'PA=F',      # Palladium Spot → Palladium Futures
            'WTI/USD': 'CL=F',      # WTI Spot → WTI Futures
            'NG/USD': 'NG=F',       # Natural Gas Spot → Natural Gas Futures
            'XCU/USD': 'HG=F',      # Copper Spot → Copper Futures
        }
        return yahoo_map.get(symbol, symbol)
    
    def get_market_status(self) -> Dict:
        return MarketHours.get_status()

    # ===== ENHANCED METHODS ADDED FOR BETTER ERROR HANDLING =====
    
    def _get_sources_for_category(self, asset: str, category: str, interval: str) -> List[Tuple[str, callable]]:
        """Get list of API sources for a given category with proper error handling"""
        
        sources = []
        
        # Add sources with proper error handling wrappers
        if category == 'forex':
            sources = [
                ('iTick', lambda: self._safe_api_call(self.fetch_itick_price, asset, 'forex')),
                ('Finnhub', lambda: self._safe_api_call(self.fetch_finnhub_forex, asset)),
                ('AlphaVantage', lambda: self._safe_api_call(self.fetch_alphavantage_forex, asset)),
                ('TwelveData', lambda: self._safe_api_call(self.fetch_twelvedata_price, self._to_twelvedata_forex(asset))),
                ('Yahoo', lambda: self._safe_api_call(self.fetch_yahoo_price, self._to_yahoo_forex(asset), interval)),
            ]
        elif category == 'crypto':
            sources = [
                ('iTick', lambda: self._safe_api_call(self.fetch_itick_price, asset, 'crypto')),
                ('Finnhub', lambda: self._safe_api_call(self.fetch_finnhub_crypto, asset)),
                ('AlphaVantage', lambda: self._safe_api_call(self.fetch_alphavantage_crypto, asset)),
                ('TwelveData', lambda: self._safe_api_call(self.fetch_twelvedata_price, self._to_twelvedata_crypto(asset))),
                ('Yahoo', lambda: self._safe_api_call(self.fetch_yahoo_price, asset, interval)),
            ]
        elif category == 'stocks':
            sources = [
                ('iTick', lambda: self._safe_api_call(self.fetch_itick_price, asset, 'stocks')),
                ('Finnhub', lambda: self._safe_api_call(self.fetch_finnhub_stock, asset)),
                ('AlphaVantage', lambda: self._safe_api_call(self.fetch_alphavantage_stock, asset)),
                ('TwelveData', lambda: self._safe_api_call(self.fetch_twelvedata_price, asset)),
                ('Yahoo', lambda: self._safe_api_call(self.fetch_yahoo_price, asset, interval)),
            ]
        elif category == 'commodities':
            sources = [
                ('OilPriceAPI', lambda: self._safe_api_call(self.fetch_oilprice_commodity, asset)),
                ('TwelveData', lambda: self._safe_api_call(self.fetch_twelvedata_price, self._to_twelvedata_commodity(asset))),
                ('AlphaVantage', lambda: self._safe_api_call(self.fetch_alphavantage_commodity, asset)),
                ('Yahoo', lambda: self._safe_api_call(self.fetch_yahoo_price, self._to_yahoo_commodity(asset), interval)),
            ]
        elif category == 'indices':
            sources = [
                ('iTick', lambda: self._safe_api_call(self.fetch_itick_price, asset, 'stocks')),
                ('Yahoo', lambda: self._safe_api_call(self.fetch_yahoo_price, asset, interval)),
                ('TwelveData', lambda: self._safe_api_call(self.fetch_twelvedata_price, self._to_twelvedata_index(asset))),
            ]
        else:
            sources = [('Yahoo', lambda: self._safe_api_call(self.fetch_yahoo_price, asset, interval))]
        
        return sources

    def _safe_api_call(self, func, *args, **kwargs):
        """Wrapper to safely call API functions and return None on any error"""
        try:
            result = func(*args, **kwargs)
            # Validate result
            if result is None:
                return None
            if isinstance(result, (int, float)) and result > 0:
                return result
            return None
        except Exception:
            return None

    def _try_parallel_sources(self, sources: List[Tuple[str, callable]], asset: str) -> Tuple[Optional[float], Optional[str]]:
        """Try all sources in parallel and return the fastest successful result"""
        
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
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
                        return price, source_name
                except Exception:
                    continue
        
        return None, None

    def _get_yahoo_symbol(self, asset: str, category: str) -> str:
        """Get Yahoo Finance symbol for any asset"""
        
        if category == 'forex':
            return self._to_yahoo_forex(asset)
        elif category == 'commodities':
            return self._to_yahoo_commodity(asset)
        elif category == 'indices':
            # Handle indices like ^GSPC
            return asset
        else:
            # Crypto, stocks use same symbol
            return asset

    def _to_twelvedata_forex(self, pair: str) -> str:
        """Convert forex pair to Twelve Data format"""
        return pair.replace('/', '/')  # Twelve Data uses same format

    def _to_twelvedata_crypto(self, asset: str) -> str:
        """Convert crypto to Twelve Data format"""
        return asset.replace('-', '/')

    def _to_twelvedata_commodity(self, asset: str) -> str:
        """Convert commodity to Twelve Data format"""
        mapping = {
            'GC=F': 'GC',  # Gold Futures
            'SI=F': 'SI',  # Silver Futures
            'CL=F': 'CL',  # Crude Oil
            'NG=F': 'NG',  # Natural Gas
            'HG=F': 'HG',  # Copper
            'XAU/USD': 'XAU/USD',  # Gold Spot
            'XAG/USD': 'XAG/USD',  # Silver Spot
            'WTI/USD': 'WTI/USD',  # WTI Spot
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

    def fetch_yahoo_price_enhanced(self, symbol: str, interval: str = '1m') -> Optional[float]:
        """Enhanced Yahoo Finance price fetch with better error handling and multiple fallbacks"""
        try:
            import yfinance as yf
            
            # Map interval to Yahoo period
            period_map = {
                '1m': '1d',
                '5m': '5d',
                '15m': '1mo',
                '1h': '3mo',
                '1d': '1y'
            }
            
            period = period_map.get(interval, '1d')
            yf_interval = interval if interval != '1d' else '1d'
            
            ticker = yf.Ticker(symbol)
            
            # Try with specified interval first
            data = ticker.history(period=period, interval=yf_interval)
            
            if not data.empty:
                return float(data['Close'].iloc[-1])
            
            # Fallback to different intervals
            for fallback_interval in ['5m', '15m', '1h', '1d']:
                if fallback_interval == interval:
                    continue
                data = ticker.history(period=period_map.get(fallback_interval, '1d'), 
                                     interval=fallback_interval)
                if not data.empty:
                    return float(data['Close'].iloc[-1])
            
            # Last resort - try to get quote
            try:
                info = ticker.info
                if 'regularMarketPrice' in info:
                    return float(info['regularMarketPrice'])
                if 'currentPrice' in info:
                    return float(info['currentPrice'])
            except:
                pass
                
        except Exception as e:
            print(f"    ⚠️ Yahoo error for {symbol}: {str(e)[:50]}")
        
        return None

    def get_real_time_price_enhanced(self, asset: str, category: str, interval: str = '1m', max_retries: int = 2) -> Tuple[Optional[float], str]:
        """
        Enhanced version of get_real_time_price with better error handling, retries, and fallbacks
        
        Args:
            asset: Asset symbol (e.g., 'EUR/USD', 'BTC-USD')
            category: Asset category ('forex', 'crypto', 'stocks', 'commodities', 'indices')
            interval: Timeframe for price data
            max_retries: Number of retry attempts for failed APIs
        
        Returns:
            Tuple of (price, source) or (None, error_message)
        """
        # Check market hours first
        market_status = MarketHours.get_status().get(category, False)
        if not market_status:
            return None, f"Market Closed ({MarketHours.get_status_message(category)})"
        
        # Define API sources with priority and retry counts
        sources = self._get_sources_for_category(asset, category, interval)
        
        # Check cache first
        for source_name, _ in sources:
            cache_key = self._get_cache_key(source_name, asset, interval)
            cached = self._get_from_cache(cache_key)
            if cached:
                return cached, f"{source_name} (cached)"
        
        # Track attempts for retry logic
        failed_sources = []
        successful_price = None
        successful_source = None
        
        # Try with parallel execution first (fastest wins)
        successful_price, successful_source = self._try_parallel_sources(sources, asset)
        
        if successful_price:
            return successful_price, successful_source
        
        # If all parallel attempts failed, try sequential with retries
        print(f"  ⚠️ All parallel attempts failed for {asset}, trying sequential with retries...")
        
        for attempt in range(max_retries):
            for source_name, func in sources:
                if source_name in failed_sources:
                    continue  # Skip sources that already failed
                
                try:
                    # Add delay between retries
                    if attempt > 0:
                        time.sleep(1 * attempt)
                    
                    price = func()
                    if price and price > 0:
                        # Cache successful result
                        cache_key = self._get_cache_key(source_name, asset, interval)
                        self._save_to_cache(cache_key, price)
                        return price, f"{source_name} (retry {attempt+1})"
                    else:
                        failed_sources.append(source_name)
                except Exception as e:
                    print(f"    ⚠️ {source_name} attempt {attempt+1} failed: {str(e)[:50]}")
                    failed_sources.append(source_name)
        
        # Ultimate fallback - try Yahoo with different intervals
        print(f"  ⚠️ All APIs failed for {asset}, trying Yahoo fallback with multiple intervals...")
        
        yahoo_intervals = ['1m', '5m', '15m', '1h', '1d']
        for yf_interval in yahoo_intervals:
            try:
                yf_symbol = self._get_yahoo_symbol(asset, category)
                price = self.fetch_yahoo_price_enhanced(yf_symbol, yf_interval)
                if price and price > 0:
                    return price, f"Yahoo Fallback ({yf_interval})"
            except Exception:
                continue
        
        # If everything fails, try to get from historical data as last resort
        try:
            df = self.fetch_yahoo_historical(self._get_yahoo_symbol(asset, category), '1d', '5d')
            if not df.empty and 'close' in df.columns:
                return float(df['close'].iloc[-1]), "Yahoo Historical (last resort)"
        except Exception:
            pass
        
        return None, "All APIs failed after multiple attempts"


# Backward compatibility
class DataFetcher(NASALevelFetcher):
    pass