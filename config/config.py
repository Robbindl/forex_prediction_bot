"""
config/config.py — Single source of truth for all configuration.
All secrets are loaded from environment variables via .env file.
No other config file exists in this system.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────────────────────────────────────────
# MARKET DATA APIs
# ─────────────────────────────────────────────────────────────────────────────

ALPHA_VANTAGE_API_KEY   = os.getenv("ALPHA_VANTAGE_KEY", "")
FINNHUB_API_KEY         = os.getenv("FINNHUB_KEY", "")
TWELVE_DATA_API_KEY     = os.getenv("TWELVEDATA_KEY", "")
ITICK_TOKEN             = os.getenv("ITICK_TOKEN", "")
OILPRICE_API_KEY        = os.getenv("OILPRICE_API_KEY", "")

# ─────────────────────────────────────────────────────────────────────────────
# NEWS APIs
# ─────────────────────────────────────────────────────────────────────────────

NEWSAPI_KEY     = os.getenv("NEWSAPI_KEY", "")
GNEWS_KEY       = os.getenv("GNEWS_KEY", "")
RAPIDAPI_KEY    = os.getenv("RAPIDAPI_KEY", "")
MARKETAUX_TOKEN = os.getenv("MARKETAUX_TOKEN", "")
APIFY_TOKEN     = os.getenv("APIFY_TOKEN", "")
WHALE_ALERT_KEY = os.getenv("WHALE_ALERT_KEY", "")

# ─────────────────────────────────────────────────────────────────────────────
# SOCIAL / TWITTER
# ─────────────────────────────────────────────────────────────────────────────

TWITTER_BEARER_TOKEN  = os.getenv("TWITTER_BEARER_TOKEN", "")
TWITTER_API_KEY       = os.getenv("TWITTER_API_KEY", "")
TWITTER_API_SECRET    = os.getenv("TWITTER_API_SECRET", "")
TWITTER_ACCESS_TOKEN  = os.getenv("TWITTER_ACCESS_TOKEN", "")
TWITTER_ACCESS_SECRET = os.getenv("TWITTER_ACCESS_SECRET", "")

# ─────────────────────────────────────────────────────────────────────────────
# TELEGRAM
# ─────────────────────────────────────────────────────────────────────────────

TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

# ─────────────────────────────────────────────────────────────────────────────
# EMAIL
# ─────────────────────────────────────────────────────────────────────────────

EMAIL_USERNAME = os.getenv("EMAIL_USERNAME", "")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD", "")

# ─────────────────────────────────────────────────────────────────────────────
# DATABASE (optional — system continues if unavailable)
# ─────────────────────────────────────────────────────────────────────────────

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://user:password@localhost:5432/trading_bot"
)
DB_HOST     = os.getenv("DB_HOST", "localhost")
DB_PORT     = int(os.getenv("DB_PORT", "5432"))
DB_NAME     = os.getenv("DB_NAME", "trading_bot")
DB_USER     = os.getenv("DB_USER", "user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "password")

# ─────────────────────────────────────────────────────────────────────────────
# TRADING — ACCOUNT DEFAULTS
# ─────────────────────────────────────────────────────────────────────────────

DEFAULT_BALANCE       = float(os.getenv("DEFAULT_BALANCE", "30"))
DEFAULT_RISK          = float(os.getenv("DEFAULT_RISK", "1.0"))
MAX_POSITIONS         = int(os.getenv("MAX_POSITIONS", "5"))
DEFAULT_ACCOUNT_BALANCE = float(os.getenv("DEFAULT_ACCOUNT_BALANCE", "10000"))
DEFAULT_RISK_PER_TRADE  = float(os.getenv("DEFAULT_RISK_PER_TRADE", "1.0"))
CRYPTO_RISK_PER_TRADE   = float(os.getenv("CRYPTO_RISK_PER_TRADE", "0.5"))
MAX_RISK_PER_TRADE      = float(os.getenv("MAX_RISK_PER_TRADE", "3.0"))

# ─────────────────────────────────────────────────────────────────────────────
# TRADING — RISK FILTERS
# ─────────────────────────────────────────────────────────────────────────────

MIN_CONFIDENCE_SCORE       = float(os.getenv("MIN_CONFIDENCE_SCORE", "0.65"))
ALERT_THRESHOLD            = float(os.getenv("ALERT_THRESHOLD", "0.75"))
CRYPTO_ALERT_THRESHOLD     = float(os.getenv("CRYPTO_ALERT_THRESHOLD", "0.80"))
MAX_CORRELATION_THRESHOLD  = float(os.getenv("MAX_CORRELATION_THRESHOLD", "0.7"))
VOLATILITY_FILTER          = os.getenv("VOLATILITY_FILTER", "true").lower() == "true"
CRYPTO_HIGH_RISK           = os.getenv("CRYPTO_HIGH_RISK", "true").lower() == "true"
CRYPTO_MIN_VOLUME          = int(os.getenv("CRYPTO_MIN_VOLUME", "1000000"))
CRYPTO_VOLATILITY_MULT     = float(os.getenv("CRYPTO_VOLATILITY_MULTIPLIER", "1.5"))
CRYPTO_MAX_POSITION_SIZE   = float(os.getenv("CRYPTO_MAX_POSITION_SIZE", "0.5"))
ENABLE_ALERTS              = os.getenv("ENABLE_ALERTS", "true").lower() == "true"

# ─────────────────────────────────────────────────────────────────────────────
# TRADING — CATEGORY POSITION CAPS
# ─────────────────────────────────────────────────────────────────────────────

CATEGORY_CAPS: dict = {
    "forex":       int(os.getenv("CAP_FOREX",       "3")),
    "crypto":      int(os.getenv("CAP_CRYPTO",      "2")),
    "stocks":      int(os.getenv("CAP_STOCKS",      "2")),
    "commodities": int(os.getenv("CAP_COMMODITIES", "2")),
    "indices":     int(os.getenv("CAP_INDICES",     "1")),
}

# ─────────────────────────────────────────────────────────────────────────────
# ML SETTINGS
# ─────────────────────────────────────────────────────────────────────────────

ML_MODEL_TYPE          = os.getenv("ML_MODEL_TYPE", "ensemble")
TRAIN_TEST_SPLIT       = float(os.getenv("TRAIN_TEST_SPLIT", "0.8"))
USE_FEATURE_ENGINEERING = os.getenv("USE_FEATURE_ENGINEERING", "true").lower() == "true"
MODEL_MAX_AGE_HOURS    = int(os.getenv("MODEL_MAX_AGE_HOURS", "24"))
MODEL_DIR              = Path(os.getenv("MODEL_DIR", "models"))
MODEL_DIR.mkdir(exist_ok=True)

# ─────────────────────────────────────────────────────────────────────────────
# DATA / ANALYSIS
# ─────────────────────────────────────────────────────────────────────────────

TIMEFRAMES         = os.getenv("TIMEFRAMES", "1d,1h,15m").split(",")
LOOKBACK_PERIOD    = int(os.getenv("LOOKBACK_PERIOD", "100"))
PREDICTION_HORIZON = int(os.getenv("PREDICTION_HORIZON", "5"))
CACHE_TTL          = int(os.getenv("CACHE_TTL", "30"))

# ─────────────────────────────────────────────────────────────────────────────
# SYSTEM
# ─────────────────────────────────────────────────────────────────────────────

LOG_LEVEL               = os.getenv("LOG_LEVEL", "INFO")
LOG_DIR                 = Path(os.getenv("LOG_DIR", "logs"))
WS_RECONNECT_DELAY      = int(os.getenv("WS_RECONNECT_DELAY", "30"))
WS_MAX_RECONNECT_DELAY  = int(os.getenv("WS_MAX_RECONNECT_DELAY", "120"))
SCAN_INTERVAL_SECONDS   = int(os.getenv("SCAN_INTERVAL_SECONDS", "60"))
MAX_SCAN_WORKERS        = int(os.getenv("MAX_SCAN_WORKERS", "8"))

# ─────────────────────────────────────────────────────────────────────────────
# ASSET UNIVERSE
# ─────────────────────────────────────────────────────────────────────────────

FOREX_PAIRS: list = [
    "EUR/USD", "GBP/USD", "USD/JPY", "USD/CHF",
    "AUD/USD", "USD/CAD", "NZD/USD", "EUR/GBP",
    "GBP/JPY", "AUD/JPY",
]

CRYPTOCURRENCIES: list = [
    "BTC-USD", "ETH-USD", "BNB-USD", "XRP-USD",
    "ADA-USD", "DOGE-USD", "SOL-USD", "DOT-USD",
    "LTC-USD", "AVAX-USD", "LINK-USD",
]

COMMODITIES: list = [
    "XAU/USD", "XAG/USD",
    "WTI/USD", "NG/USD", "XCU/USD",
    "GC=F", "CL=F", "SI=F",
]

INDICES: list = [
    "^GSPC", "^DJI", "^IXIC", "^FTSE", "^N225",
]

STOCKS: list = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA",
    "META", "NVDA", "JPM", "V", "MA",
]

ASSET_CATEGORIES: dict = {
    "forex":       FOREX_PAIRS,
    "crypto":      CRYPTOCURRENCIES,
    "stocks":      STOCKS,
    "commodities": COMMODITIES,
    "indices":     INDICES,
}

# ─────────────────────────────────────────────────────────────────────────────
# RSS FEEDS (not secrets — hardcoded is fine)
# ─────────────────────────────────────────────────────────────────────────────

BLOOMBERG_RSS: dict = {
    "markets":    "https://feeds.bloomberg.com/markets/news.rss",
    "technology": "https://feeds.bloomberg.com/technology/news.rss",
    "politics":   "https://feeds.bloomberg.com/politics/news.rss",
    "economy":    "https://feeds.bloomberg.com/economics/news.rss",
    "crypto":     "https://www.bloomberg.com/crypto/feed.xml",
}

FORBES_RSS: dict = {
    "business":  "https://www.forbes.com/business/feed/",
    "investing": "https://www.forbes.com/investing/feed/",
    "markets":   "https://www.forbes.com/markets/feed/",
    "crypto":    "https://www.forbes.com/digital-assets/feed/",
}

BINANCE_ANNOUNCEMENTS_URL = (
    "https://www.binance.com/bapi/composite/v1/public/cms/article/list/query"
)