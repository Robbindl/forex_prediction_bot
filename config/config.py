import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

"""
Configuration file for Forex Prediction Bot
ALL SENSITIVE DATA LOADED FROM ENVIRONMENT VARIABLES
"""

# ==================================================
# MARKET DATA APIS
# ==================================================
ALPHA_VANTAGE_API_KEY = os.getenv("ALPHA_VANTAGE_KEY", "")  # ✅ FIXED
FINNHUB_API_KEY = os.getenv("FINNHUB_KEY", "")  # ✅ FIXED
TWELVE_DATA_API_KEY = os.getenv("TWELVEDATA_KEY", "")  # ✅ FIXED

# ===== NEW APIS =====
ITICK_TOKEN = os.getenv("ITICK_TOKEN", "")  # ✅ FIXED
OILPRICE_API_KEY = os.getenv("OILPRICE_API_KEY", "")  # ✅ FIXED

# ==================================================
# NEWS APIS
# ==================================================
NEWSAPI_KEY = os.getenv("NEWSAPI_KEY", "")  # ✅ FIXED
GNEWS_KEY = os.getenv("GNEWS_KEY", "")  # ✅ FIXED
RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY", "")  # ✅ FIXED

# ===== WHALE ALERT =====
WHALE_ALERT_KEY = os.getenv("WHALE_ALERT_KEY", "")  # ✅ FIXED

# ==================================================
# TWITTER API
# ==================================================
TWITTER_BEARER_TOKEN = os.getenv("TWITTER_BEARER_TOKEN", "")  # ✅ FIXED
TWITTER_API_KEY = os.getenv("TWITTER_API_KEY", "")  # ✅ FIXED
TWITTER_API_SECRET = os.getenv("TWITTER_API_SECRET", "")  # ✅ FIXED
TWITTER_ACCESS_TOKEN = os.getenv("TWITTER_ACCESS_TOKEN", "")  # ✅ FIXED
TWITTER_ACCESS_SECRET = os.getenv("TWITTER_ACCESS_SECRET", "")  # ✅ FIXED

# ==================================================
# RSS FEED URLs (Not secrets - can stay here)
# ==================================================

# Bloomberg RSS Feeds
BLOOMBERG_RSS = {
    'markets': 'https://feeds.bloomberg.com/markets/news.rss',
    'technology': 'https://feeds.bloomberg.com/technology/news.rss',
    'politics': 'https://feeds.bloomberg.com/politics/news.rss',
    'economy': 'https://feeds.bloomberg.com/economics/news.rss',
    'crypto': 'https://www.bloomberg.com/crypto/feed.xml',
}

# Forbes RSS Feeds
FORBES_RSS = {
    'home': 'https://www.forbes.com/forbesapi/thought/2025/feed.xml',
    'business': 'https://www.forbes.com/business/feed/',
    'investing': 'https://www.forbes.com/investing/feed/',
    'markets': 'https://www.forbes.com/markets/feed/',
    'crypto': 'https://www.forbes.com/digital-assets/feed/',
}

# GOLD_TELEGRAPH_RSS = 'https://goldtelegraph.com/feed/'

# Binance Announcements API
BINANCE_ANNOUNCEMENTS_URL = 'https://www.binance.com/bapi/composite/v1/public/cms/article/list/query'

# ==================================================
# APIFY (for Forbes Scraper - optional)
# ==================================================
APIFY_TOKEN = os.getenv("APIFY_TOKEN", "")  # ✅ FIXED

MARKETAUX_TOKEN = os.getenv("MARKETAUX_TOKEN", "")

# ==================================================
# TELEGRAM ALERTS
# ==================================================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")  # ✅ FIXED
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")  # ✅ FIXED

# ==================================================
# EMAIL ALERTS
# ==================================================
EMAIL_USERNAME = os.getenv("EMAIL_USERNAME", "")  # ✅ FIXED
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD", "")  # ✅ FIXED

# ==================================================
# TRADING DEFAULTS
# ==================================================
DEFAULT_BALANCE = float(os.getenv("DEFAULT_BALANCE", "30"))
DEFAULT_RISK = float(os.getenv("DEFAULT_RISK", "1.0"))
MAX_POSITIONS = int(os.getenv("MAX_POSITIONS", "5"))

# ==================================================
# ASSET LISTS (These are not secrets, can stay here)
# ==================================================

FOREX_PAIRS = [
    "EUR/USD", "GBP/USD", "USD/JPY", "USD/CHF", 
    "AUD/USD", "USD/CAD", "NZD/USD", "EUR/GBP",
    "GBP/JPY", "AUD/JPY"
]  # ← Make sure XPT/XPD aren't here (they shouldn't be)

CRYPTOCURRENCIES = [
    "BTC-USD", "ETH-USD", "BNB-USD", "XRP-USD",
    "ADA-USD", "DOGE-USD", "SOL-USD", "DOT-USD",
    "LTC-USD", "AVAX-USD", "LINK-USD"
]  # ← UPDATE this to your 11 cryptos

COMMODITIES = [
    "XAU/USD", "XAG/USD",  # ← REMOVE XPT/USD, XPD/USD
    "WTI/USD", "NG/USD", "XCU/USD",
    "GC=F", "CL=F", "SI=F",
]  # ← UPDATE this list

INDICES = [
    "^GSPC", "^DJI", "^IXIC", "^FTSE", "^N225",
]  # ← Fine as is

STOCKS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "META", "NVDA",
    "JPM", "V", "MA", "PYPL",  # ← REMOVE extra stocks
    # ... etc
]

# Analysis settings
TIMEFRAMES = ["1d", "1h", "15m"]
LOOKBACK_PERIOD = 100
PREDICTION_HORIZON = 5

# Machine Learning settings
ML_MODEL_TYPE = "ensemble"
TRAIN_TEST_SPLIT = 0.8
USE_FEATURE_ENGINEERING = True

# Risk Management
MAX_CORRELATION_THRESHOLD = 0.7
VOLATILITY_FILTER = True
MIN_CONFIDENCE_SCORE = 0.65

# Crypto-specific Risk Settings
CRYPTO_HIGH_RISK = True
CRYPTO_MIN_VOLUME = 1000000
CRYPTO_VOLATILITY_MULTIPLIER = 1.5
CRYPTO_MAX_POSITION_SIZE = 0.5

# Alerts and notifications
ENABLE_ALERTS = True
ALERT_THRESHOLD = 0.75
CRYPTO_ALERT_THRESHOLD = 0.80

# Position Sizing Defaults
DEFAULT_ACCOUNT_BALANCE = 10000
DEFAULT_RISK_PER_TRADE = 1.0
CRYPTO_RISK_PER_TRADE = 0.5
MAX_RISK_PER_TRADE = 3.0
MAX_OPEN_POSITIONS = 5

# Asset Categories for Web Dashboard
ASSET_CATEGORIES = {
    'forex': FOREX_PAIRS,
    'crypto': CRYPTOCURRENCIES,
    'stocks': STOCKS,
    'commodities': COMMODITIES,
    'indices': INDICES
}