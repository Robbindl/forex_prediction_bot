# ─────────────────────────────────────────────────────────────────────────────
# NEWS EVENT BLOCKING (15m-friendly)
# ─────────────────────────────────────────────────────────────────────────────
# Original: PRE_EVENT_MINS=60 blocks signals for 1 hour. KILLS 15m scalping.
# Optimized: PRE_EVENT_MINS=10 for 15m (still safe, allows trading 45 min before)
# ACTIVE_MINS=10 (markets settle faster on data releases than we think)

NEWS_EVENT = {
    "pre_event_mins": 10,
    "active_mins": 10,
    "post_event_mins": 45,
}

# ─────────────────────────────────────────────────────────────────────────────
# POSITION SIZING (ATR-based, 15m specific)
# ─────────────────────────────────────────────────────────────────────────────
# On 15m, ATR is naturally smaller (bar-level noise). Adjust multipliers:

RISK_MULTIPLIER = {
    "stop_loss_atr": 1.2,
    "take_profit_atr": 1.5,
    "trailing_stop_atr": 0.5,
}

# ─────────────────────────────────────────────────────────────────────────────
# ASSET-SPECIFIC TUNING (Crypto vs Forex vs Commodities)
# ─────────────────────────────────────────────────────────────────────────────

ASSET_CLASS_TUNING = {
    "crypto": {
        # BTC/ETH/SOL: High volatility, 24/7, less news-driven
        "min_confidence": 0.58,
        "news_pre_event_mins": 10,
        "stop_loss_atr": 1.2,
        "take_profit_atr": 1.8,  # Crypto moves bigger
        "target_rr": 2.0,
    },
    "forex": {
        # EUR/USD/GBP: Lower volatility, news-sensitive, tight spreads
        "min_confidence": 0.65,    # ← STRICTER for forex (fewer false signals)
        "news_pre_event_mins": 15,  # ← LONGER for forex (respects data releases)
        "stop_loss_atr": 1.5,       # ← WIDER SL (forex less predictable)
        "take_profit_atr": 1.5,
        "target_rr": 1.8,
    },
    "commodities": {
        # Oil/Gold/Silver: Trend-driven, geopolitical surprises
        "min_confidence": 0.62,
        "news_pre_event_mins": 12,
        "stop_loss_atr": 1.4,
        "take_profit_atr": 1.6,
        "target_rr": 1.95,
    },
    "indices": {
        # SPX/DJIA/NDX: Correlated, earnings-driven, large moves
        "min_confidence": 0.62,
        "news_pre_event_mins": 15,
        "stop_loss_atr": 1.3,
        "take_profit_atr": 1.7,
        "target_rr": 1.9,
    },
}

# ─────────────────────────────────────────────────────────────────────────────
# SIGNAL QUALITY GATES (15m-specific)
# ─────────────────────────────────────────────────────────────────────────────

SIGNAL_QUALITY = {
    # Minimum bars of data needed
    "min_bars": 50,  # 50 × 15m = ~12.5 hours. Enough for MA/BB.
    
    # Reject signals if price crossed MA recently (vs late-riding trends)
    "ma_crossover_bars": 3,  # Only take signals 3+ bars from MA cross
    
    # Reject if rolling volatility too low (no signal in tight range)
    "min_atr_ticks": 5,  # ATR must be > 5 ticks (asset-specific conversion needed)
    
    # Reject if spread too wide (bad execution expected)
    "max_spread_bps": 2.0,  # 2 basis points. Spreads wider → kill it
}

# ─────────────────────────────────────────────────────────────────────────────
# 15M DATA VALIDATION
# ─────────────────────────────────────────────────────────────────────────────
# On 15m: data gaps are CRITICAL (missing candle = missing signal).
# Forex gaps are common (Fri 22:00 close, Sun 22:00 reopen UTC).

DATA_VALIDATION = {
    "allow_weekend_gaps": True,   # Allow Fri→Sun gap (forex markets)
    "allow_gap_tolerance_candles": 2,  # Up to 2 candles of gap acceptable
    "reject_if_latest_candle_not_closed": True,  # Don't trade on partial candles
}

# ─────────────────────────────────────────────────────────────────────────────
# RECOMMENDED TRADING DEFAULTS (DEPLOY IMMEDIATELY)
# ─────────────────────────────────────────────────────────────────────────────

RECOMMENDED_ENV_OVERRIDES = {
    "TRADING_TIMEFRAME": "15m",
    "DEFAULT_BALANCE": "10000",
    "MAX_POSITIONS": "6",
    
    # Current live bot risk posture
    "DAILY_LOSS_LIMIT_PERCENT": "35.0",
    "DRAWDOWN_HALT_PERCENT": "40.0",
    "DRAWDOWN_REDUCE_PERCENT": "20.0",
    
    # Risk per trade
    "DEFAULT_RISK_PER_TRADE": "1.2",    # 1.2% vs default 1.5%
    "CRYPTO_RISK_PER_TRADE": "1.5",     # 1.5% vs default 2.0%
    
}

# ─────────────────────────────────────────────────────────────────────────────
# DEPLOYMENT CHECKLIST
# ─────────────────────────────────────────────────────────────────────────────
"""
Before going live with 15m all-assets:

[ ] Set TRADING_TIMEFRAME=15m in .env
[ ] Confirm the live runtime playbook/policy stack matches the assets you want to trade
[ ] Keep the Strategy Lab active bench trimmed to the current 9 research presets
[ ] Leave archived presets out of auto-research until they earn re-entry
[ ] Update data_ingestion.news_event_monitor PRE_EVENT_MINS=10 (CRITICAL!)
[ ] Verify .env MAX_POSITIONS=6 (distribute across asset classes)
[ ] Backtest last 6 months on 15m with new parameters
[ ] Run bot on paper account for 1 week
[ ] Monitor P&L by asset class (crypto, forex, commodities, indices)
[ ] If any class underperforms, apply ASSET_CLASS_TUNING overrides
"""

__all__ = [
    "NEWS_EVENT",
    "RISK_MULTIPLIER",
    "ASSET_CLASS_TUNING",
    "SIGNAL_QUALITY",
    "DATA_VALIDATION",
    "RECOMMENDED_ENV_OVERRIDES",
]
