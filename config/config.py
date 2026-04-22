"""
config/config.py — Single source of truth for all configuration.
All secrets are loaded from environment variables via .env file.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

_ROOT_DIR = Path(__file__).resolve().parents[1]
_BASE_ENV = _ROOT_DIR / ".env"

# Load the repo .env file only.
load_dotenv(_BASE_ENV)


def _parse_int(value: str, default=None):
    try:
        text = str(value or "").strip()
        return int(text) if text else default
    except Exception:
        return default


def _parse_float(value: str, default=None):
    try:
        text = str(value or "").strip()
        return float(text) if text else default
    except Exception:
        return default

# ─────────────────────────────────────────────────────────────────────────────
# MARKET DATA
# ─────────────────────────────────────────────────────────────────────────────

DERIV_ENABLED         = os.getenv("DERIV_ENABLED", "true").lower() == "true"
DERIV_APP_ID          = os.getenv("DERIV_APP_ID", "").strip()
DERIV_TOKEN           = os.getenv("DERIV_TOKEN", "").strip()
DERIV_SYMBOL_MAP      = os.getenv("DERIV_SYMBOL_MAP", "")
IG_ENABLED            = os.getenv("IG_ENABLED", "false").lower() == "true"
IG_ENVIRONMENT        = os.getenv("IG_ENVIRONMENT", "demo").strip().lower() or "demo"
IG_API_KEY            = os.getenv("IG_API_KEY", "").strip()
IG_IDENTIFIER         = os.getenv("IG_IDENTIFIER", "").strip()
IG_PASSWORD           = os.getenv("IG_PASSWORD", "").strip()
IG_ACCOUNT_ID         = os.getenv("IG_ACCOUNT_ID", "").strip()
IG_EPIC_MAP           = os.getenv("IG_EPIC_MAP", "").strip()
IG_ROUTED_CATEGORIES  = [
    item.strip().lower()
    for item in os.getenv("IG_ROUTED_CATEGORIES", "commodities").split(",")
    if item.strip()
]
IG_ROUTED_ASSETS      = [
    item.strip()
    for item in os.getenv("IG_ROUTED_ASSETS", "").split(",")
    if item.strip()
]
IG_ROUTE_TO_DERIV_BY_DEFAULT = os.getenv("IG_ROUTE_TO_DERIV_BY_DEFAULT", "false").lower() == "true"
IG_MAX_ROUTED_ASSETS = _parse_int(os.getenv("IG_MAX_ROUTED_ASSETS", "6"), 6)
IG_STREAMING_HOLDOFF_SEC = _parse_int(os.getenv("IG_STREAMING_HOLDOFF_SEC", "300"), 300)
BINANCE_PUBLIC_DATA_ENABLED = os.getenv("BINANCE_PUBLIC_DATA_ENABLED", "true").lower() == "true"
DUKASCOPY_HISTORY_ENABLED = os.getenv("DUKASCOPY_HISTORY_ENABLED", "true").lower() == "true"
DUKASCOPY_SYMBOL_MAP  = os.getenv("DUKASCOPY_SYMBOL_MAP", "").strip()
FMP_HISTORY_ENABLED   = os.getenv("FMP_HISTORY_ENABLED", "true").lower() == "true"
FMP_API_KEY           = os.getenv("FMP_API_KEY", "").strip()
FMP_SYMBOL_MAP        = os.getenv("FMP_SYMBOL_MAP", "").strip()
LOCAL_CANDLE_STORE_ENABLED = os.getenv("LOCAL_CANDLE_STORE_ENABLED", "true").lower() == "true"
LOCAL_CANDLE_STORE_PATH = Path(os.getenv("LOCAL_CANDLE_STORE_PATH", "data/local_candles.sqlite3"))
LOCAL_CANDLE_STORE_REQUIRED_COVERAGE = max(
    0.5,
    min(1.0, float(os.getenv("LOCAL_CANDLE_STORE_REQUIRED_COVERAGE", "1.0"))),
)
PLAYBOOK_ONLY_RUNTIME = os.getenv("PLAYBOOK_ONLY_RUNTIME", "false").lower() == "true"
EIA_API_KEY           = os.getenv("EIA_API_KEY", "")

# ─────────────────────────────────────────────────────────────────────────────
# NEWS APIs
# ─────────────────────────────────────────────────────────────────────────────

ALPHA_VANTAGE_API_KEY = os.getenv("ALPHA_VANTAGE_KEY", "")
FINNHUB_API_KEY       = os.getenv("FINNHUB_KEY", "")
NEWSAPI_KEY     = os.getenv("NEWSAPI_KEY", "")
GNEWS_KEY       = os.getenv("GNEWS_KEY", "")
NEWS_SENTIMENT_ENABLED = os.getenv("NEWS_SENTIMENT_ENABLED", "true").lower() == "true"
NEWS_RSS_ENABLED = os.getenv("NEWS_RSS_ENABLED", "false").lower() == "true"
NEWS_REDDIT_ENABLED = os.getenv("NEWS_REDDIT_ENABLED", "false").lower() == "true"
WHALE_ALERT_KEY = os.getenv("WHALE_ALERT_KEY", "")
FRED_API_KEY    = os.getenv("FRED_API_KEY", "")
TRADING_ECONOMICS_CREDENTIALS = (
    os.getenv("TRADING_ECONOMICS_CREDENTIALS", "").strip()
    or os.getenv("TRADING_ECONOMICS_API_KEY", "").strip()
)
ECON_CALENDAR_ALLOW_TRADING_ECONOMICS_GUEST = (
    os.getenv("ECON_CALENDAR_ALLOW_TRADING_ECONOMICS_GUEST", "false").lower() == "true"
)
ECON_CALENDAR_FOREX_FACTORY_ENABLED = (
    os.getenv("ECON_CALENDAR_FOREX_FACTORY_ENABLED", "true").lower() == "true"
)
ECON_CALENDAR_HTTP_TIMEOUT = int(os.getenv("ECON_CALENDAR_HTTP_TIMEOUT", "15"))

# ─────────────────────────────────────────────────────────────────────────────
# OPENAI
# ─────────────────────────────────────────────────────────────────────────────

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")

# ─────────────────────────────────────────────────────────────────────────────
# ROBBIE CHAT / DEEPSEEK
# ─────────────────────────────────────────────────────────────────────────────

DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY", "").strip()
ROBBIE_CHAT_PROVIDER = os.getenv("ROBBIE_CHAT_PROVIDER", "auto").strip().lower() or "auto"
ROBBIE_CHAT_MODEL = os.getenv("ROBBIE_CHAT_MODEL", "deepseek-chat").strip() or "deepseek-chat"
ROBBIE_CHAT_BASE_URL = os.getenv("ROBBIE_CHAT_BASE_URL", "https://api.deepseek.com").strip() or "https://api.deepseek.com"
ROBBIE_CHAT_MODE = os.getenv("ROBBIE_CHAT_MODE", "hybrid").strip().lower() or "hybrid"
if ROBBIE_CHAT_MODE not in {"strict", "hybrid", "llm"}:
    ROBBIE_CHAT_MODE = "hybrid"
ROBBIE_CHAT_ALLOW_WORLD_KNOWLEDGE = os.getenv("ROBBIE_CHAT_ALLOW_WORLD_KNOWLEDGE", "true").lower() == "true"
ROBBIE_CHAT_INCLUDE_LOCAL_DRAFT = os.getenv("ROBBIE_CHAT_INCLUDE_LOCAL_DRAFT", "auto").strip().lower() or "auto"
if ROBBIE_CHAT_INCLUDE_LOCAL_DRAFT not in {"auto", "always", "never"}:
    ROBBIE_CHAT_INCLUDE_LOCAL_DRAFT = "auto"
ROBBIE_CHAT_TIMEOUT_SECONDS = int(os.getenv("ROBBIE_CHAT_TIMEOUT_SECONDS", "30"))
ROBBIE_CHAT_HISTORY_LIMIT = int(os.getenv("ROBBIE_CHAT_HISTORY_LIMIT", "10"))
ROBBIE_CHAT_CONTEXT_CHAR_LIMIT = int(os.getenv("ROBBIE_CHAT_CONTEXT_CHAR_LIMIT", "12000"))
ROBBIE_CHAT_MAX_TOKENS = _parse_int(os.getenv("ROBBIE_CHAT_MAX_TOKENS", "1100"), 1100)
ROBBIE_CHAT_TEMPERATURE = _parse_float(os.getenv("ROBBIE_CHAT_TEMPERATURE", "0.35"), 0.35)
ROBBIE_CHAT_NEWS_ENABLED = os.getenv("ROBBIE_CHAT_NEWS_ENABLED", "true").lower() == "true"
ROBBIE_CHAT_NEWS_LIMIT = int(os.getenv("ROBBIE_CHAT_NEWS_LIMIT", "8"))
ROBBIE_CHAT_CLOSED_TRADES_LIMIT = _parse_int(os.getenv("ROBBIE_CHAT_CLOSED_TRADES_LIMIT", "100"), 100)
ROBBIE_CHAT_OPEN_POSITIONS_LIMIT = _parse_int(os.getenv("ROBBIE_CHAT_OPEN_POSITIONS_LIMIT", "8"), 8)
ROBBIE_CHAT_MARKET_EVENT_LIMIT = _parse_int(os.getenv("ROBBIE_CHAT_MARKET_EVENT_LIMIT", "10"), 10)
ROBBIE_CHAT_MARKET_LOOKAHEAD_DAYS = _parse_int(os.getenv("ROBBIE_CHAT_MARKET_LOOKAHEAD_DAYS", "5"), 5)

# ─────────────────────────────────────────────────────────────────────────────
# CAPABILITY / INTELLIGENCE LIMITS
# ─────────────────────────────────────────────────────────────────────────────

TOP_OPPORTUNITIES_LIMIT = _parse_int(os.getenv("TOP_OPPORTUNITIES_LIMIT", "10"), 10)
LEARNING_HISTORY_LIMIT = _parse_int(os.getenv("LEARNING_HISTORY_LIMIT", "40"), 40)
DASHBOARD_WEAK_POSITIONS_LIMIT = _parse_int(os.getenv("DASHBOARD_WEAK_POSITIONS_LIMIT", "8"), 8)

# ─────────────────────────────────────────────────────────────────────────────
# SOCIAL / TWITTER
# ─────────────────────────────────────────────────────────────────────────────

TWITTER_BEARER_TOKEN  = os.getenv("TWITTER_BEARER_TOKEN", "")
TWITTER_API_KEY       = os.getenv("TWITTER_API_KEY", "")
TWITTER_API_SECRET    = os.getenv("TWITTER_API_SECRET", "")
TWITTER_ACCESS_TOKEN  = os.getenv("TWITTER_ACCESS_TOKEN", "")
TWITTER_ACCESS_SECRET = os.getenv("TWITTER_ACCESS_SECRET", "")

# ─────────────────────────────────────────────────────────────────────────────
# TELEGRAM — COMMAND BOT
# ─────────────────────────────────────────────────────────────────────────────

COMMAND_BOT_TOKEN   = os.getenv("COMMAND_BOT_TOKEN", "").strip()
COMMAND_BOT_CHAT_ID = os.getenv("COMMAND_BOT_CHAT_ID", "").strip()
DEBUG_FORCE_TELEGRAM = os.getenv("DEBUG_FORCE_TELEGRAM", "false").lower() == "true"
TELEGRAM_PID_FILE    = Path(os.getenv("TELEGRAM_PID_FILE", "telegram_bot.pid"))
PAPER_TRADES_FILE    = Path(os.getenv("PAPER_TRADES_FILE", "data/paper_trades.json"))
# Export compatibility aliases for runtime code, but keep COMMAND_BOT_* as
# the single source of truth in .env.
TELEGRAM_TOKEN   = COMMAND_BOT_TOKEN
TELEGRAM_CHAT_ID = COMMAND_BOT_CHAT_ID

# ─────────────────────────────────────────────────────────────────────────────
# TELEGRAM — DEEPSEEK CHAT BOT
# ─────────────────────────────────────────────────────────────────────────────

DEEPSEEK_TELEGRAM_TOKEN = os.getenv("DEEPSEEK_TELEGRAM_TOKEN", "").strip()
DEEPSEEK_TELEGRAM_CHAT_ID = os.getenv("DEEPSEEK_TELEGRAM_CHAT_ID", "").strip()

# ─────────────────────────────────────────────────────────────────────────────
# TELEGRAM — WHALE BOT
# ─────────────────────────────────────────────────────────────────────────────

WHALE_TELEGRAM_TOKEN = os.getenv("WHALE_TELEGRAM_TOKEN", "")
# Chat ID for intelligence alerts and runtime monitoring.
# Set INTELLIGENCE_CHAT_ID in .env to send to a different chat.
# Defaults to the command-bot chat if not set.
INTELLIGENCE_CHAT_ID = (
    os.getenv("INTELLIGENCE_CHAT_ID", "")
    or COMMAND_BOT_CHAT_ID
).strip()
TELEGRAM_API_ID      = os.getenv("TELEGRAM_API_ID", "")
TELEGRAM_API_HASH    = os.getenv("TELEGRAM_API_HASH", "")
TELEGRAM_PHONE       = os.getenv("TELEGRAM_PHONE", "")
TELEGRAM_SESSION     = os.getenv("TELEGRAM_SESSION", "whale_session")
WHALE_TELEGRAM_CHANNELS = [
    item.strip()
    for item in os.getenv("WHALE_TELEGRAM_CHANNELS", "whalebotalerts").split(",")
    if item.strip()
]
WHALE_ALLOWED_ASSETS = [
    item.strip().upper()
    for item in os.getenv(
        "WHALE_ALLOWED_ASSETS",
        "BTC-USD,ETH-USD,BNB-USD,SOL-USD,XRP-USD",
    ).split(",")
    if item.strip()
]
WHALE_TELEGRAM_MIN_VALUE_USD = float(os.getenv("WHALE_TELEGRAM_MIN_VALUE_USD", "1000000"))
WHALE_TWITTER_WHALE_ENABLED = os.getenv("WHALE_TWITTER_WHALE_ENABLED", "false").lower() == "true"
WHALE_REDDIT_WHALE_ENABLED = os.getenv("WHALE_REDDIT_WHALE_ENABLED", "false").lower() == "true"
BNB_RPC_URL          = os.getenv("BNB_RPC_URL", "https://bsc-dataseed1.binance.org").strip()
SOLANA_RPC_URL       = os.getenv("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com").strip()
XRPL_RPC_URL         = os.getenv("XRPL_RPC_URL", "https://s1.ripple.com:51234").strip()

# ─────────────────────────────────────────────────────────────────────────────
# EMAIL
# ─────────────────────────────────────────────────────────────────────────────

EMAIL_USERNAME = os.getenv("EMAIL_USERNAME", "")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD", "")

# ─────────────────────────────────────────────────────────────────────────────
# DASHBOARD / WEB INTERFACE
# ─────────────────────────────────────────────────────────────────────────────

DEVELOPMENT_MODE = os.getenv("DEVELOPMENT_MODE", "false").lower() == "true"
DASHBOARD_API_KEY = os.getenv("DASHBOARD_API_KEY", "")
SESSION_TOKEN_TTL = int(os.getenv("SESSION_TOKEN_TTL", "3600"))  # 1 hour default
TRUST_PROXY_COUNT = int(os.getenv("TRUST_PROXY_COUNT", "1"))
DASHBOARD_BG_REFRESH_WORKERS = _parse_int(os.getenv("DASHBOARD_BG_REFRESH_WORKERS", "6"), 6)
DASHBOARD_COMMAND_CENTER_WORKERS = _parse_int(os.getenv("DASHBOARD_COMMAND_CENTER_WORKERS", "4"), 4)
DASHBOARD_CORRELATION_WORKERS = _parse_int(os.getenv("DASHBOARD_CORRELATION_WORKERS", "8"), 8)
DASHBOARD_HEATMAP_WORKERS = _parse_int(os.getenv("DASHBOARD_HEATMAP_WORKERS", "10"), 10)
DASHBOARD_SENTIMENT_ASSET_WORKERS = _parse_int(os.getenv("DASHBOARD_SENTIMENT_ASSET_WORKERS", "10"), 10)
DASHBOARD_SENTIMENT_FETCH_WORKERS = _parse_int(os.getenv("DASHBOARD_SENTIMENT_FETCH_WORKERS", "6"), 6)
DASHBOARD_CORS_ORIGINS = [
    origin.strip()
    for origin in os.getenv("DASHBOARD_CORS_ORIGINS", "http://localhost:5000").split(",")
    if origin.strip()
]
TZ_OFFSET_HOURS   = int(os.getenv("TZ_OFFSET_HOURS", "3"))
TZ_NAME           = os.getenv("TZ_NAME", "UTC+3").strip() or "UTC+3"

# ─────────────────────────────────────────────────────────────────────────────
# DATABASE
# ─────────────────────────────────────────────────────────────────────────────

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    ""
).strip()
DATABASE_SSLMODE = os.getenv("DATABASE_SSLMODE", "").strip()
DB_CONNECT_RETRIES = int(os.getenv("DB_CONNECT_RETRIES", "5"))
DB_RETRY_DELAY_SECONDS = int(os.getenv("DB_RETRY_DELAY_SECONDS", "3"))
DB_POOL_SIZE = int(os.getenv("DB_POOL_SIZE", "5"))
DB_MAX_OVERFLOW = int(os.getenv("DB_MAX_OVERFLOW", "5"))
DB_POOL_RECYCLE_SECONDS = int(os.getenv("DB_POOL_RECYCLE_SECONDS", "3600"))

# ─────────────────────────────────────────────────────────────────────────────
# REDIS
# ─────────────────────────────────────────────────────────────────────────────

REDIS_URL      = os.getenv("REDIS_URL", "redis://localhost:6379/0")
REDIS_CACHE_PREFIX = os.getenv("REDIS_CACHE_PREFIX", "trading_bot:cache:")

# ─────────────────────────────────────────────────────────────────────────────
# ML SERVICE
# ─────────────────────────────────────────────────────────────────────────────

ML_SERVICE_PORT = int(os.getenv("ML_SERVICE_PORT", "9100"))

# ─────────────────────────────────────────────────────────────────────────────
# TRADING — ACCOUNT DEFAULTS
# ─────────────────────────────────────────────────────────────────────────────

DEFAULT_BALANCE         = float(os.getenv("DEFAULT_BALANCE", "10000"))
MAX_POSITIONS           = int(os.getenv("MAX_POSITIONS", "8"))
DEFAULT_RISK_PER_TRADE      = float(os.getenv("DEFAULT_RISK_PER_TRADE",      "1.5"))
CRYPTO_RISK_PER_TRADE       = float(os.getenv("CRYPTO_RISK_PER_TRADE",       "2.0"))
COMMODITIES_RISK_PER_TRADE  = float(os.getenv("COMMODITIES_RISK_PER_TRADE",  "2.0"))
INDICES_RISK_PER_TRADE      = float(os.getenv("INDICES_RISK_PER_TRADE",      "1.5"))
MAX_RISK_PER_TRADE          = float(os.getenv("MAX_RISK_PER_TRADE",          "3.0"))
DAILY_LOSS_LIMIT_PERCENT    = float(os.getenv("DAILY_LOSS_LIMIT_PERCENT",    "5.0"))
DRAWDOWN_HALT_PERCENT       = float(os.getenv("DRAWDOWN_HALT_PERCENT",       "25.0"))
DRAWDOWN_REDUCE_PERCENT     = float(
    os.getenv(
        "DRAWDOWN_REDUCE_PERCENT",
        str(max(5.0, min(15.0, DRAWDOWN_HALT_PERCENT * 0.5))),
    )
)
PORTFOLIO_MAX_SINGLE_ASSET_PCT = float(os.getenv("PORTFOLIO_MAX_SINGLE_ASSET_PCT", "35.0"))
PORTFOLIO_MAX_CATEGORY_PCT = float(os.getenv("PORTFOLIO_MAX_CATEGORY_PCT", "40.0"))
PORTFOLIO_MAX_SAME_DIRECTION_POSITIONS = int(os.getenv("PORTFOLIO_MAX_SAME_DIRECTION_POSITIONS", "4"))
PORTFOLIO_CORRELATION_CATEGORY_TRIGGER_PCT = float(
    os.getenv("PORTFOLIO_CORRELATION_CATEGORY_TRIGGER_PCT", "85.0")
)

# ─────────────────────────────────────────────────────────────────────────────
# TRADING — SPREAD THRESHOLDS (Asset-Specific)
# ─────────────────────────────────────────────────────────────────────────────

SPREAD_THRESHOLDS: dict = {
    "forex":       float(os.getenv("SPREAD_THRESHOLD_FOREX",       "0.002")),
    "crypto":      float(os.getenv("SPREAD_THRESHOLD_CRYPTO",      "0.005")),
    "commodities": float(os.getenv("SPREAD_THRESHOLD_COMMODITIES", "0.01")),
    "indices":     float(os.getenv("SPREAD_THRESHOLD_INDICES",     "0.01")),
}

# ─────────────────────────────────────────────────────────────────────────────
# STRATEGY LAB — BACKTEST EXECUTION ASSUMPTIONS
# Flat costs were making lower-friction assets look worse than they are.
# These defaults remain conservative, but are now category-aware.
# ─────────────────────────────────────────────────────────────────────────────

BACKTEST_COMMISSION_DEFAULT = float(os.getenv("BACKTEST_COMMISSION_DEFAULT", "0.001"))
BACKTEST_SLIPPAGE_DEFAULT = float(os.getenv("BACKTEST_SLIPPAGE_DEFAULT", "0.0005"))
BACKTEST_RISK_PER_TRADE = float(os.getenv("BACKTEST_RISK_PER_TRADE", "0.01"))

BACKTEST_EXECUTION_PROFILES: dict = {
    "default": {
        "commission": BACKTEST_COMMISSION_DEFAULT,
        "slippage": BACKTEST_SLIPPAGE_DEFAULT,
        "risk_per_trade": BACKTEST_RISK_PER_TRADE,
    },
    "forex": {
        "commission": float(os.getenv("BACKTEST_COMMISSION_FOREX", "0.00004")),
        "slippage": float(os.getenv("BACKTEST_SLIPPAGE_FOREX", "0.00008")),
        "risk_per_trade": BACKTEST_RISK_PER_TRADE,
    },
    "crypto": {
        "commission": float(os.getenv("BACKTEST_COMMISSION_CRYPTO", "0.00060")),
        "slippage": float(os.getenv("BACKTEST_SLIPPAGE_CRYPTO", "0.00035")),
        "risk_per_trade": BACKTEST_RISK_PER_TRADE,
    },
    "commodities": {
        "commission": float(os.getenv("BACKTEST_COMMISSION_COMMODITIES", "0.00012")),
        "slippage": float(os.getenv("BACKTEST_SLIPPAGE_COMMODITIES", "0.00018")),
        "risk_per_trade": BACKTEST_RISK_PER_TRADE,
    },
    "indices": {
        "commission": float(os.getenv("BACKTEST_COMMISSION_INDICES", "0.00008")),
        "slippage": float(os.getenv("BACKTEST_SLIPPAGE_INDICES", "0.00012")),
        "risk_per_trade": BACKTEST_RISK_PER_TRADE,
    },
}


def get_backtest_execution_profile(category: str = "") -> dict:
    key = (category or "").lower()
    base = BACKTEST_EXECUTION_PROFILES.get("default", {})
    resolved = BACKTEST_EXECUTION_PROFILES.get(key, base)
    return {
        "commission": float(resolved.get("commission", base.get("commission", BACKTEST_COMMISSION_DEFAULT))),
        "slippage": float(resolved.get("slippage", base.get("slippage", BACKTEST_SLIPPAGE_DEFAULT))),
        "risk_per_trade": float(resolved.get("risk_per_trade", base.get("risk_per_trade", BACKTEST_RISK_PER_TRADE))),
    }

# ─────────────────────────────────────────────────────────────────────────────
# TRADING — RISK FILTERS
# ─────────────────────────────────────────────────────────────────────────────

MIN_CONFIDENCE_SCORE      = float(os.getenv("MIN_CONFIDENCE_SCORE", "0.55"))
MIN_FINAL_CONFIDENCE      = float(os.getenv("MIN_FINAL_CONFIDENCE", "0.55"))
MAX_SIGNAL_CONFIDENCE     = float(os.getenv("MAX_SIGNAL_CONFIDENCE", "0.95"))
SIGNAL_CONFIDENCE_CURVE_POWER = float(os.getenv("SIGNAL_CONFIDENCE_CURVE_POWER", "2.4"))
LIVE_APPROVED_REGISTRY_ONLY = os.getenv("LIVE_APPROVED_REGISTRY_ONLY", "false").lower() == "true"
LIVE_REQUIRE_ASSET_APPROVAL = os.getenv("LIVE_REQUIRE_ASSET_APPROVAL", "true").lower() == "true"
ALERT_THRESHOLD           = float(os.getenv("ALERT_THRESHOLD", "0.75"))
CRYPTO_ALERT_THRESHOLD    = float(os.getenv("CRYPTO_ALERT_THRESHOLD", "0.80"))
MAX_CORRELATION_THRESHOLD = float(os.getenv("MAX_CORRELATION_THRESHOLD", "0.7"))
TRADE_CLOSE_COOLDOWN_MINUTES = int(os.getenv("TRADE_CLOSE_COOLDOWN_MINUTES", "60"))
INACTIVITY_RELIEF_START_HOURS = float(os.getenv("INACTIVITY_RELIEF_START_HOURS", "4"))
INACTIVITY_RELIEF_FULL_HOURS = float(os.getenv("INACTIVITY_RELIEF_FULL_HOURS", "12"))
VOLATILITY_FILTER         = os.getenv("VOLATILITY_FILTER", "true").lower() == "true"
CRYPTO_HIGH_RISK          = os.getenv("CRYPTO_HIGH_RISK", "true").lower() == "true"
CRYPTO_MIN_VOLUME         = int(os.getenv("CRYPTO_MIN_VOLUME", "1000000"))
CRYPTO_VOLATILITY_MULT    = float(os.getenv("CRYPTO_VOLATILITY_MULTIPLIER", "1.5"))
# Position sizing uses the bot's internal contract-spec model.
ENABLE_ALERTS             = os.getenv("ENABLE_ALERTS", "true").lower() == "true"

# ─────────────────────────────────────────────────────────────────────────────
# TRADING — CATEGORY POSITION CAPS
# Must stay in sync with core/assets.py _CATEGORY_CAPS
# ─────────────────────────────────────────────────────────────────────────────

CATEGORY_CAPS: dict = {
    "forex":       int(os.getenv("CAP_FOREX",       "4")),
    "crypto":      int(os.getenv("CAP_CRYPTO",      "4")),
    "commodities": int(os.getenv("CAP_COMMODITIES", "4")),
    "indices":     int(os.getenv("CAP_INDICES",     "4")),
}
CATEGORY_CAP_SOFT_BUFFER = int(os.getenv("CATEGORY_CAP_SOFT_BUFFER", "2"))

# ─────────────────────────────────────────────────────────────────────────────
# ML SETTINGS
# ─────────────────────────────────────────────────────────────────────────────

ML_MODEL_TYPE           = os.getenv("ML_MODEL_TYPE", "ensemble")
TRAIN_TEST_SPLIT        = float(os.getenv("TRAIN_TEST_SPLIT", "0.8"))
USE_FEATURE_ENGINEERING = os.getenv("USE_FEATURE_ENGINEERING", "true").lower() == "true"
MODEL_MAX_AGE_HOURS     = int(os.getenv("MODEL_MAX_AGE_HOURS", "24"))
MODEL_DIR               = Path(os.getenv("MODEL_DIR", "models"))
MODEL_DIR.mkdir(exist_ok=True)
MAX_TRAINING_WORKERS    = int(os.getenv("MAX_TRAINING_WORKERS", "2"))
MIN_HOLDOUT_ACCURACY    = float(os.getenv("MIN_HOLDOUT_ACCURACY", "0.52"))
MIN_WALK_FORWARD_ACCURACY = float(os.getenv("MIN_WALK_FORWARD_ACCURACY", "0.52"))
MIN_WALK_FORWARD_SAMPLES  = int(os.getenv("MIN_WALK_FORWARD_SAMPLES", "60"))

# ─────────────────────────────────────────────────────────────────────────────
# DATA / ANALYSIS
# ─────────────────────────────────────────────────────────────────────────────

TIMEFRAMES         = os.getenv("TIMEFRAMES", "1m,5m,15m,30m,1h,4h,1d").split(",")
LOOKBACK_PERIOD    = int(os.getenv("LOOKBACK_PERIOD", "100"))
PREDICTION_HORIZON = int(os.getenv("PREDICTION_HORIZON", "5"))
CACHE_TTL          = int(os.getenv("CACHE_TTL", "300"))
MARKET_DATA_QUOTE_CACHE_TTL = int(os.getenv("MARKET_DATA_QUOTE_CACHE_TTL", "5"))
MARKET_DATA_OHLCV_CACHE_TTL = int(os.getenv("MARKET_DATA_OHLCV_CACHE_TTL", "60"))
MARKET_DATA_OHLCV_SLOW_CACHE_TTL = int(os.getenv("MARKET_DATA_OHLCV_SLOW_CACHE_TTL", "300"))
FREE_INTEL_ENABLED = os.getenv("FREE_INTEL_ENABLED", "true").lower() == "true"
FREE_INTEL_CACHE_SECONDS = int(os.getenv("FREE_INTEL_CACHE_SECONDS", "1800"))

FRED_US_2Y_SERIES       = os.getenv("FRED_US_2Y_SERIES", "DGS2")
FRED_US_10Y_SERIES      = os.getenv("FRED_US_10Y_SERIES", "DGS10")
FRED_US_REAL_10Y_SERIES = os.getenv("FRED_US_REAL_10Y_SERIES", "DFII10")
FRED_USD_BROAD_SERIES   = os.getenv("FRED_USD_BROAD_SERIES", "DTWEXBGS")
FRED_VIX_SERIES         = os.getenv("FRED_VIX_SERIES", "VIXCLS")

EIA_CRUDE_STOCKS_SERIES = os.getenv("EIA_CRUDE_STOCKS_SERIES", "PET.WCESTUS1.W")
CFTC_ENABLED            = os.getenv("CFTC_ENABLED", "true").lower() == "true"

# News and sentiment freshness window (default 12 hours, adjustable 6-12h)
SENTIMENT_MAX_AGE_HOURS = int(os.getenv("SENTIMENT_MAX_AGE_HOURS", "12"))

# ─────────────────────────────────────────────────────────────────────────────
# TRADING TIMEFRAME
# TRADING_TIMEFRAME — the candle interval used for signal generation and ML.
#   "15m" = ATR-based SL/TP distances are small → trades hit TP or SL
#           naturally within minutes to a few hours depending on market.
#   "1d"  = daily ATR distances → trades last days or weeks (old behaviour).
# ─────────────────────────────────────────────────────────────────────────────

TRADING_TIMEFRAME = os.getenv("TRADING_TIMEFRAME", "15m")
FOREX_TRADING_TIMEFRAME = os.getenv("FOREX_TRADING_TIMEFRAME", TRADING_TIMEFRAME)
CRYPTO_TRADING_TIMEFRAME = os.getenv("CRYPTO_TRADING_TIMEFRAME", TRADING_TIMEFRAME)
COMMODITIES_TRADING_TIMEFRAME = os.getenv(
    "COMMODITIES_TRADING_TIMEFRAME",
    TRADING_TIMEFRAME,
)
INDICES_TRADING_TIMEFRAME = os.getenv(
    "INDICES_TRADING_TIMEFRAME",
    TRADING_TIMEFRAME,
)

CATEGORY_TRADING_TIMEFRAMES: dict = {
    "forex": FOREX_TRADING_TIMEFRAME,
    "crypto": CRYPTO_TRADING_TIMEFRAME,
    "commodities": COMMODITIES_TRADING_TIMEFRAME,
    "indices": INDICES_TRADING_TIMEFRAME,
}


def get_trading_timeframe(category: str = "") -> str:
    return CATEGORY_TRADING_TIMEFRAMES.get((category or "").lower(), TRADING_TIMEFRAME)


def get_timeframe_periods(interval: str) -> int:
    return {
        "1m": 600,
        "5m": 500,
        "15m": 500,
        "30m": 400,
        "1h": 300,
        "4h": 200,
        "1d": LOOKBACK_PERIOD,
    }.get(interval, LOOKBACK_PERIOD)


def get_research_timeframe_periods(interval: str = "", category: str = "") -> int:
    resolved = (interval or get_trading_timeframe(category)).lower()
    return {
        "1m": int(os.getenv("RESEARCH_PERIODS_1M", "6000")),
        "5m": int(os.getenv("RESEARCH_PERIODS_5M", "10000")),
        "15m": int(os.getenv("RESEARCH_PERIODS_15M", "12000")),
        "30m": int(os.getenv("RESEARCH_PERIODS_30M", "10000")),
        "1h": int(os.getenv("RESEARCH_PERIODS_1H", "6000")),
        "4h": int(os.getenv("RESEARCH_PERIODS_4H", "3000")),
        "1d": int(os.getenv("RESEARCH_PERIODS_1D", "1500")),
    }.get(resolved, get_timeframe_periods(resolved))


def get_chart_timeframe_periods(interval: str) -> int:
    return {
        "1m": 1000,
        "5m": 1000,
        "15m": 1000,
        "30m": 1000,
        "1h": 1000,
        "4h": 1000,
        "1d": 3000,
    }.get(interval, get_timeframe_periods(interval))

# ─────────────────────────────────────────────────────────────────────────────
# SYSTEM
# ─────────────────────────────────────────────────────────────────────────────

LOG_LEVEL              = os.getenv("LOG_LEVEL", "INFO")
LOG_DIR                = Path(os.getenv("LOG_DIR", "logs"))
LOG_MAX_BYTES          = int(os.getenv("LOG_MAX_BYTES", str(5 * 1024 * 1024)))
LOG_BACKUP_COUNT       = int(os.getenv("LOG_BACKUP_COUNT", "2"))
ERROR_LOG_MAX_BYTES    = int(os.getenv("ERROR_LOG_MAX_BYTES", str(2 * 1024 * 1024)))
TRADES_LOG_MAX_BYTES   = int(os.getenv("TRADES_LOG_MAX_BYTES", str(2 * 1024 * 1024)))
ML_SERVICE_LOG_MAX_BYTES = int(os.getenv("ML_SERVICE_LOG_MAX_BYTES", str(2 * 1024 * 1024)))
LOG_RETENTION_DAYS     = int(os.getenv("LOG_RETENTION_DAYS", "14"))
WS_RECONNECT_DELAY     = int(os.getenv("WS_RECONNECT_DELAY", "30"))
WS_MAX_RECONNECT_DELAY = int(os.getenv("WS_MAX_RECONNECT_DELAY", "120"))
SCAN_INTERVAL_SECONDS  = int(os.getenv("SCAN_INTERVAL_SECONDS", "45"))
MAX_SCAN_WORKERS       = int(os.getenv("MAX_SCAN_WORKERS", "4"))
AUTO_RESEARCH_ALLOW_IN_BOT_RUNTIME = (
    os.getenv("AUTO_RESEARCH_ALLOW_IN_BOT_RUNTIME", "false").lower() == "true"
)
AUTO_RESEARCH_ALLOW_SEPARATE_WORKER = (
    os.getenv("AUTO_RESEARCH_ALLOW_SEPARATE_WORKER", "true").lower() == "true"
)
AUTO_RESEARCH_MAX_PARALLEL_ASSETS = int(
    os.getenv("AUTO_RESEARCH_MAX_PARALLEL_ASSETS", os.getenv("MAX_PARALLEL_ASSETS", "1"))
)
AUTO_RESEARCH_DEFER_ON_RESOURCE_PRESSURE = (
    os.getenv("AUTO_RESEARCH_DEFER_ON_RESOURCE_PRESSURE", "true").lower() == "true"
)
AUTO_RESEARCH_MAX_CPU_PERCENT = float(os.getenv("AUTO_RESEARCH_MAX_CPU_PERCENT", "75"))
AUTO_RESEARCH_MAX_RAM_PERCENT = float(os.getenv("AUTO_RESEARCH_MAX_RAM_PERCENT", "82"))
AUTO_RESEARCH_PRESSURE_RETRY_SECONDS = int(os.getenv("AUTO_RESEARCH_PRESSURE_RETRY_SECONDS", "300"))

# ─────────────────────────────────────────────────────────────────────────────
# SIGNAL GOVERNANCE
# ─────────────────────────────────────────────────────────────────────────────

GOVERNANCE_VALIDATION_DAYS = int(os.getenv("GOVERNANCE_VALIDATION_DAYS", "30"))
GOVERNANCE_VALIDATION_HORIZON = os.getenv("GOVERNANCE_VALIDATION_HORIZON", "1H").upper()
GOVERNANCE_MIN_LIVE_SAMPLES = int(os.getenv("GOVERNANCE_MIN_LIVE_SAMPLES", "25"))
GOVERNANCE_MIN_LIVE_ACCURACY = float(os.getenv("GOVERNANCE_MIN_LIVE_ACCURACY", "54.0"))
GOVERNANCE_BOOTSTRAP_MIN_LIVE_SAMPLES = int(os.getenv("GOVERNANCE_BOOTSTRAP_MIN_LIVE_SAMPLES", "10"))
GOVERNANCE_BOOTSTRAP_MIN_LIVE_ACCURACY = float(os.getenv("GOVERNANCE_BOOTSTRAP_MIN_LIVE_ACCURACY", "50.0"))
GOVERNANCE_PORTFOLIO_MIN_LIVE_SAMPLES = int(os.getenv("GOVERNANCE_PORTFOLIO_MIN_LIVE_SAMPLES", "100"))
GOVERNANCE_PORTFOLIO_MIN_LIVE_ACCURACY = float(os.getenv("GOVERNANCE_PORTFOLIO_MIN_LIVE_ACCURACY", "40.0"))
GOVERNANCE_MIN_SEED_CONFIDENCE = float(
    os.getenv("GOVERNANCE_MIN_SEED_CONFIDENCE", os.getenv("GOVERNANCE_MIN_ML_CONFIDENCE", "0.15"))
)
GOVERNANCE_MIN_ML_CONFIDENCE = GOVERNANCE_MIN_SEED_CONFIDENCE
GOVERNANCE_MIN_RISK_REWARD = float(os.getenv("GOVERNANCE_MIN_RISK_REWARD", "1.5"))
GOVERNANCE_MIN_REAL_SOURCES = int(os.getenv("GOVERNANCE_MIN_REAL_SOURCES", "3"))
GOVERNANCE_REQUIRE_MODEL_RESEARCH = os.getenv("GOVERNANCE_REQUIRE_MODEL_RESEARCH", "true").lower() == "true"
GOVERNANCE_ALLOW_PROVISIONAL_MODEL_RESEARCH_IN_PAPER = (
    os.getenv("GOVERNANCE_ALLOW_PROVISIONAL_MODEL_RESEARCH_IN_PAPER", "true").lower() == "true"
)
GOVERNANCE_REQUIRE_NON_DELAYED_PRICE = os.getenv("GOVERNANCE_REQUIRE_NON_DELAYED_PRICE", "true").lower() == "true"
GOVERNANCE_REQUIRE_NON_DELAYED_OHLCV = os.getenv("GOVERNANCE_REQUIRE_NON_DELAYED_OHLCV", "true").lower() == "true"
GOVERNANCE_ENABLE_FOREX_FILTER = os.getenv("GOVERNANCE_ENABLE_FOREX_FILTER", "true").lower() == "true"
GOVERNANCE_EXPECTANCY_MIN_SAMPLES = int(os.getenv("GOVERNANCE_EXPECTANCY_MIN_SAMPLES", "8"))
GOVERNANCE_EXPECTANCY_MIN_AVG_R = float(os.getenv("GOVERNANCE_EXPECTANCY_MIN_AVG_R", "0.0"))
GOVERNANCE_EXPECTANCY_MIN_TARGET_HIT_RATE = float(
    os.getenv("GOVERNANCE_EXPECTANCY_MIN_TARGET_HIT_RATE", "0.24")
)
GOVERNANCE_EXPECTANCY_MAX_PREMATURE_STOP_RATE = float(
    os.getenv("GOVERNANCE_EXPECTANCY_MAX_PREMATURE_STOP_RATE", "0.45")
)
GOVERNANCE_EXPECTANCY_MIN_QUALITY_SCORE = float(
    os.getenv("GOVERNANCE_EXPECTANCY_MIN_QUALITY_SCORE", "42.0")
)

POLICY_TRAINING_PERIODS_15M = int(os.getenv("POLICY_TRAINING_PERIODS_15M", "800"))
POLICY_TRAINING_PERIODS_1H = int(os.getenv("POLICY_TRAINING_PERIODS_1H", "500"))
POLICY_TRAINING_PERIODS_4H = int(os.getenv("POLICY_TRAINING_PERIODS_4H", "300"))
POLICY_TRAINING_PERIODS_1D = int(os.getenv("POLICY_TRAINING_PERIODS_1D", str(LOOKBACK_PERIOD)))

FOREX_FILTER_MIN_CONFIDENCE = float(os.getenv("FOREX_FILTER_MIN_CONFIDENCE", "0.65"))
FOREX_FILTER_BOOTSTRAP_MIN_CONFIDENCE = float(os.getenv("FOREX_FILTER_BOOTSTRAP_MIN_CONFIDENCE", "0.50"))
FOREX_FILTER_MAX_SPREAD_BPS = float(os.getenv("FOREX_FILTER_MAX_SPREAD_BPS", "1.8"))
FOREX_FILTER_BOOTSTRAP_MAX_SPREAD_BPS = float(os.getenv("FOREX_FILTER_BOOTSTRAP_MAX_SPREAD_BPS", "2.0"))

# ─────────────────────────────────────────────────────────────────────────────
# ASSET UNIVERSE — your 25 assets
# These lists are used by sentiment, intelligence, and other
# services that need to know which assets to track.
# The canonical trading registry lives in core/assets.py
# ─────────────────────────────────────────────────────────────────────────────

FOREX_PAIRS: list = [
    "EUR/USD", "EUR/JPY", "EUR/GBP", "GBP/JPY", "GBP/USD",
    "AUD/USD", "NZD/USD", "USD/JPY", "USD/CAD", "USD/CHF",
]

CRYPTOCURRENCIES: list = [
    "BTC-USD", "ETH-USD", "BNB-USD",
    "SOL-USD", "XRP-USD",
]

COMMODITIES: list = [
    "XAU/USD",  # Gold
    "XAG/USD",  # Silver
    "WTI",      # WTI Crude Oil
]

INDICES: list = [
    "US30",   # US30
    "US100",  # US100
    "US500",  # US500
    "UK100",  # FTSE
    "GER40",  # Germany 40 / DAX
    "AUS200", # Australia 200 / ASX
    "JPN225", # Japan 225 / Nikkei
]

STOCKS: list = []   # not trading stocks

ASSET_CATEGORIES: dict = {
    "forex":       FOREX_PAIRS,
    "crypto":      CRYPTOCURRENCIES,
    "commodities": COMMODITIES,
    "indices":     INDICES,
}

# ─────────────────────────────────────────────────────────────────────────────
# RSS FEEDS
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

# ─────────────────────────────────────────────────────────────────────────────
# API KEY EXPIRY DATES (optional: for alerts)
# ─────────────────────────────────────────────────────────────────────────────
# Configure API key expiration dates here for automated alerts
# Format: {name: date}
# Example: {"MyAPIKey": date(2026, 12, 31)}

from datetime import date as _date

API_KEY_EXPIRY_DATES: dict = {
    # Add your API key expiry dates here
    # "QOS API": _date(2026, 3, 29),  # Uncomment and update as needed
}
