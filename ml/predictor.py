"""
ml/predictor.py — ML prediction engine.
Merges: advanced_predictor.py, engines/ml_engine.py, alpha_discovery.py
"""
from __future__ import annotations
import threading
from typing import Any, Dict, List, Optional, Tuple
import numpy as np
import pandas as pd
from utils.logger import get_logger
from ml.registry import ModelRegistry

logger   = get_logger()
registry = ModelRegistry()

_FEATURE_COLS = ["open", "high", "low", "close", "volume"]


def _build_features(df: pd.DataFrame) -> Optional[np.ndarray]:
    """Build feature matrix from OHLCV dataframe."""
    if df is None or len(df) < 30:
        return None
    try:
        d = df[_FEATURE_COLS].astype(float).copy()
        close = d["close"]

        d["ret1"]   = close.pct_change(1)
        d["ret5"]   = close.pct_change(5)
        d["ret10"]  = close.pct_change(10)
        d["vol5"]   = d["ret1"].rolling(5).std()
        d["vol20"]  = d["ret1"].rolling(20).std()
        d["sma5"]   = close.rolling(5).mean()  / close
        d["sma20"]  = close.rolling(20).mean() / close
        d["sma50"]  = close.rolling(50).mean() / close if len(d) >= 50 else 1.0
        d["hl_pct"] = (d["high"] - d["low"]) / close
        d["oc_pct"] = (d["close"] - d["open"]) / d["open"].replace(0, np.nan)

        d = d.replace([np.inf, -np.inf], np.nan).fillna(0)
        feature_cols = [c for c in d.columns if c not in _FEATURE_COLS]
        return d[feature_cols].values[-1].reshape(1, -1)
    except Exception as e:
        logger.debug(f"[Predictor] Feature build error: {e}")
        return None


class MLPredictor:
    """Loads models from registry and generates directional probability predictions."""

    def __init__(self):
        self._lock = threading.Lock()

    def predict(self, asset: str, category: str, df: pd.DataFrame) -> Tuple[float, float]:
        """
        Returns (probability_of_up, confidence).
        probability_of_up: 0.0 (down) → 1.0 (up).
        confidence: model certainty 0.0 → 1.0.
        """
        features = _build_features(df)
        if features is None:
            return 0.5, 0.0

        model_key = f"{category}_classifier"
        model     = registry.get(model_key)

        if model is None:
            # No trained model — use simple momentum signal
            return self._momentum_fallback(df)

        try:
            with self._lock:
                proba = model.predict_proba(features)
            up_prob    = float(proba[0][1]) if proba.shape[1] > 1 else float(proba[0][0])
            confidence = abs(up_prob - 0.5) * 2   # 0 at 0.5, 1 at 0 or 1
            logger.log_ml(model_key, asset, up_prob, confidence)
            return up_prob, confidence
        except Exception as e:
            logger.debug(f"[Predictor] Model predict error {asset}: {e}")
            return self._momentum_fallback(df)

    @staticmethod
    def _momentum_fallback(df: pd.DataFrame) -> Tuple[float, float]:
        """Simple 5-day momentum as fallback when no model is available."""
        try:
            close   = df["close"].astype(float)
            ret5    = (close.iloc[-1] - close.iloc[-6]) / close.iloc[-6]
            prob_up = 0.5 + min(0.3, max(-0.3, ret5 * 5))
            conf    = abs(ret5) * 10
            return float(prob_up), float(min(0.5, conf))
        except Exception:
            return 0.5, 0.0


# ── Global singleton ──────────────────────────────────────────────────────────
predictor = MLPredictor()