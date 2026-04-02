from __future__ import annotations
from typing import Optional
import pandas as pd
from config.config import (
    FOREX_FILTER_BOOTSTRAP_MIN_CONFIDENCE,
    FOREX_FILTER_BOOTSTRAP_MAX_SPREAD_BPS,
    FOREX_FILTER_MAX_SPREAD_BPS,
    FOREX_FILTER_MIN_CONFIDENCE,
)
from utils.logger import get_logger

logger = get_logger()

# Forex-specific thresholds (optimized for EUR/USD, GBP/USD, etc on 15m)
FOREX_MIN_ATR = 0.0008           # ~8 pips minimum (EUR/USD scale)
FOREX_MAX_SPREAD_BPS = 1.5       # 1.5 basis points (tight, but achievable)
FOREX_MIN_BARS_SINCE_MA_CROSS = 3  # Don't enter right at MA — wait for confirmation
FOREX_SESSION_SENSITIVITY = {
    "asian": 0.8,     # Lower volatility — reduce position size
    "london": 1.2,    # High volatility surge — okay to trade
    "newyork": 1.1,   # Good volatility, trending
    "overlap": 1.3,   # London/NY overlap = highest vol, best trades
}

class ForexFilter:
    """
    Gate 6.5: Forex-specific quality checks.
    Pre-filters signals for forex pairs before risk manager.
    """

    @staticmethod
    def should_trade_forex_signal(
        asset: str,
        signal_confidence: float,
        df: pd.DataFrame,
        atr: float,
        current_spread_bps: Optional[float] = None,
        live_validation_scope: str = "asset",
    ) -> tuple[bool, str]:
        """
        Validate a forex signal against forex-specific filters.
        
        Returns: (should_trade: bool, rejection_reason: str)
        If should_trade=True, rejection_reason="PASSED" for logging.
        """
        
        # Filter 1: Keep forex selective, but do not require the full asset-grade floor
        # while the pair is still relying on portfolio/bootstrap live validation.
        min_confidence = (
            FOREX_FILTER_MIN_CONFIDENCE
            if live_validation_scope == "asset"
            else FOREX_FILTER_BOOTSTRAP_MIN_CONFIDENCE
        )
        if signal_confidence < min_confidence:
            return False, f"confidence {signal_confidence:.2f} < {min_confidence:.2f}"
        
        # Filter 2: Adequate ATR (price needs to move)
        if atr < FOREX_MIN_ATR:
            return False, f"ATR {atr:.5f} < min {FOREX_MIN_ATR}"
        
        # Filter 3: Reject if spread too wide
        max_spread_bps = (
            FOREX_FILTER_MAX_SPREAD_BPS
            if live_validation_scope == "asset"
            else FOREX_FILTER_BOOTSTRAP_MAX_SPREAD_BPS
        )
        if current_spread_bps and current_spread_bps > max_spread_bps:
            return False, f"spread {current_spread_bps:.1f}bps > max {max_spread_bps:.1f}"
        
        # Filter 4: Avoid entering right at MA crossovers (wait for confirmation)
        try:
            close = df["close"].astype(float)
            sma_20 = close.rolling(20).mean()
            
            if len(close) >= FOREX_MIN_BARS_SINCE_MA_CROSS:
                latest_price = close.iloc[-1]
                ma = sma_20.iloc[-1]
                
                # Check last N bars for recent MA cross
                recent_prices = close.iloc[-FOREX_MIN_BARS_SINCE_MA_CROSS:]
                recent_mas = sma_20.iloc[-FOREX_MIN_BARS_SINCE_MA_CROSS:]
                
                # If price crossed MA in last 3 bars, reject (too early)
                crossed_ma_recently = any(
                    (recent_prices.iloc[i] > recent_mas.iloc[i] and 
                     recent_prices.iloc[i-1] <= recent_mas.iloc[i-1]) or
                    (recent_prices.iloc[i] < recent_mas.iloc[i] and 
                     recent_prices.iloc[i-1] >= recent_mas.iloc[i-1])
                    for i in range(1, len(recent_prices))
                )
                
                if crossed_ma_recently:
                    return False, "MA cross too recent (wait for confirmation)"
        except Exception as e:
            logger.debug(f"[ForexFilter] MA cross check error: {e}")
        
        # Filter 5: Reject if volatility collapsed (no edge if price frozen)
        try:
            close = df["close"].astype(float)
            vol_20 = close.pct_change().rolling(20).std()
            
            if len(vol_20) > 0:
                current_vol = vol_20.iloc[-1]
                vol_30d_avg = vol_20.mean()
                
                # Volatility lower than 50% of normal = reject (no edge)
                if current_vol < vol_30d_avg * 0.5:
                    return False, f"volatility collapsed {current_vol:.4f} vs avg {vol_30d_avg:.4f}"
        except Exception as e:
            logger.debug(f"[ForexFilter] volatility check error: {e}")
        
        # All filters passed
        return True, "PASSED"

    @staticmethod
    def get_session_multiplier(asset: str) -> float:
        """
        Return position size multiplier based on current forex session.
        
        UTC times:
          Asian:  21:00 (prev) - 08:00
          London: 08:00 - 17:00
          NY:     13:00 - 22:00
          Overlap (London+NY): 13:00 - 17:00
        """
        try:
            from datetime import datetime
            now_utc = datetime.utcnow()
            hour = now_utc.hour
            
            # London + NY overlap (13:00-17:00 UTC)
            if 13 <= hour < 17:
                return FOREX_SESSION_SENSITIVITY["overlap"]
            # London session (08:00-17:00 UTC)
            elif 8 <= hour < 17:
                return FOREX_SESSION_SENSITIVITY["london"]
            # NY session (13:00-22:00 UTC)
            elif 13 <= hour < 22:
                return FOREX_SESSION_SENSITIVITY["newyork"]
            # Asian session (21:00-08:00 UTC)
            else:
                return FOREX_SESSION_SENSITIVITY["asian"]
        except Exception:
            return 1.0  # Default: no multiplier on error

__all__ = ["ForexFilter"]
