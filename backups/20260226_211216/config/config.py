import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

"""
Configuration file for Forex Prediction Bot
Add your API keys here
NOW WITH CRYPTOCURRENCY SUPPORT! 🪙
"""

# API Keys (Get free keys from respective websites)
ALPHA_VANTAGE_API_KEY = os.getenv("ALPHA_VANTAGE_KEY", "")  # alphavantage.co
FINNHUB_API_KEY = os.getenv("FINNHUB_KEY", "")  # finnhub.io
TWELVE_DATA_API_KEY = os.getenv("TWELVEDATA_KEY", "")  # twelvedata.com

# Trading pairs and assets to monitor
FOREX_PAIRS = [
    "EUR/USD", "GBP/USD", "USD/JPY", "USD/CHF", 
    "AUD/USD", "USD/CAD", "NZD/USD", "EUR/GBP",
    "GBP/JPY", "AUD/JPY"
]

# 🪙 CRYPTOCURRENCIES (Yahoo Finance format: SYMBOL-USD)
CRYPTOCURRENCIES = [
    # Top 10 by Market Cap
    "BTC-USD",   # Bitcoin
    "ETH-USD",   # Ethereum
    "BNB-USD",   # Binance Coin
    "XRP-USD",   # Ripple
    "ADA-USD",   # Cardano
    "DOGE-USD",  # Dogecoin
    "SOL-USD",   # Solana
    "DOT-USD",   # Polkadot
    "MATIC-USD", # Polygon
    "LTC-USD",   # Litecoin
    
    # Additional Popular Cryptos
    "AVAX-USD",  # Avalanche
    "LINK-USD",  # Chainlink
    "UNI-USD",   # Uniswap
    "ATOM-USD",  # Cosmos
    "XLM-USD",   # Stellar
    "ALGO-USD",  # Algorand
    "VET-USD",   # VeChain
    "ICP-USD",   # Internet Computer
    "FIL-USD",   # Filecoin
    "TRX-USD",   # Tron
]

COMMODITIES = [
    "GC=F",  # Gold
    "SI=F",  # Silver
    "CL=F",  # Crude Oil
    "NG=F",  # Natural Gas
    "HG=F",  # Copper
]

INDICES = [
    "^GSPC",  # S&P 500
    "^DJI",   # Dow Jones
    "^IXIC",  # NASDAQ
    "^FTSE",  # FTSE 100
    "^N225",  # Nikkei 225
]

STOCKS = [
    # Tech Giants
    "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "META", "NVDA",
    # Finance
    "JPM", "BAC", "GS", "WFC", "C",
    # Energy
    "XOM", "CVX", "COP",
    # Other Popular
    "NFLX", "DIS", "V", "MA", "PYPL"
]

# Analysis settings
TIMEFRAMES = ["1d", "1h", "15m"]
LOOKBACK_PERIOD = 100  # Number of candles to analyze
PREDICTION_HORIZON = 5  # Predict next N periods

# Machine Learning settings
ML_MODEL_TYPE = "ensemble"  # Options: "rf", "xgboost", "lstm", "ensemble"
TRAIN_TEST_SPLIT = 0.8
USE_FEATURE_ENGINEERING = True

# Risk Management
MAX_CORRELATION_THRESHOLD = 0.7
VOLATILITY_FILTER = True
MIN_CONFIDENCE_SCORE = 0.65

# 🪙 Crypto-specific Risk Settings
CRYPTO_HIGH_RISK = True  # Flag crypto as high volatility
CRYPTO_MIN_VOLUME = 1000000  # Minimum 24h volume (USD)
CRYPTO_VOLATILITY_MULTIPLIER = 1.5  # Adjust SL/TP for higher volatility
CRYPTO_MAX_POSITION_SIZE = 0.5  # Max 0.5% position for crypto (vs 1% for forex)

# Alerts and notifications
ENABLE_ALERTS = True
ALERT_THRESHOLD = 0.75  # Confidence level for alerts
CRYPTO_ALERT_THRESHOLD = 0.80  # Higher threshold for crypto (more volatile)

# Position Sizing Defaults
DEFAULT_ACCOUNT_BALANCE = 10000
DEFAULT_RISK_PER_TRADE = 1.0  # 1% per trade (forex/stocks)
CRYPTO_RISK_PER_TRADE = 0.5  # 0.5% per trade (crypto - more volatile)
MAX_RISK_PER_TRADE = 3.0  # Never risk more than 3%
MAX_OPEN_POSITIONS = 5  # Maximum concurrent positions

# Asset Categories for Web Dashboard
ASSET_CATEGORIES = {
    'forex': FOREX_PAIRS,
    'crypto': CRYPTOCURRENCIES,  # NEW!
    'stocks': STOCKS,
    'commodities': COMMODITIES,
    'indices': INDICES
}


# Default trading balance
DEFAULT_BALANCE = float(os.getenv("DEFAULT_BALANCE", "20"))
