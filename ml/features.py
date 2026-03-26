"""
ml/features.py — Single source of truth for ML feature engineering.

Both trainer and predictor use this to ensure feature count consistency.
Previously trainer produced 6 features, predictor expected 10, causing
ValueError when calling model.predict_proba() on trained models.
"""

from __future__ import annotations
import numpy as np
import pandas as pd
from utils.logger import get_logger

logger = get_logger()


def build_features(df: pd.DataFrame) -> np.ndarray | None:
    """
    Build canonical 6-feature set for both training and prediction.
    
    Features:
    - ret1: 1-period return
    - ret5: 5-period return  
    - vol5: 5-period volatility (std of returns)
    - sma5: 5-period simple moving average
    - sma20: 20-period simple moving average
    - hl_pct: high-low percentage range
    
    Returns None if insufficient data or invalid values.
    """
    if df is None or len(df) < 20:
        return None
    
    try:
        # Calculate returns
        ret1 = df['close'].pct_change(1).iloc[-1]
        ret5 = df['close'].pct_change(5).iloc[-1]
        
        # Calculate volatility (std of last 5 returns)
        returns = df['close'].pct_change().tail(5)
        vol5 = returns.std()
        
        # Calculate moving averages
        sma5 = df['close'].tail(5).mean()
        sma20 = df['close'].tail(20).mean()
        
        # Calculate high-low percentage range
        recent_high = df['high'].tail(5).max()
        recent_low = df['low'].tail(5).min()
        hl_pct = (recent_high - recent_low) / recent_low if recent_low > 0 else 0
        
        # Validate all features are finite
        features = [ret1, ret5, vol5, sma5, sma20, hl_pct]
        if not all(np.isfinite(f) for f in features):
            return None
            
        return np.array(features, dtype=np.float32)
        
    except Exception as e:
        logger.debug(f"[Features] Build failed: {e}")
        return None