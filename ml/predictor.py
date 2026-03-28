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

# Phase 11 — prediction latency tracking
try:
    from monitoring.metrics import metrics, PREDICTION, MetricsTimer
    _METRICS_OK = True
except ImportError:
    _METRICS_OK = False

logger   = get_logger()
# FIX: Import the shared module-level singleton from ml.registry instead of
# creating a NEW ModelRegistry() instance here.  Previously predictor.py
# instantiated its own registry (empty in-memory dict) while ml/trainer.py
# saved models to the registry.py singleton — the two never shared data, so
# MLPredictor.predict() fell through to momentum fallback on every call.
from ml.registry import registry
from ml.features import build_features


class MLPredictor:
    """Loads models from registry and generates directional probability predictions."""

    def __init__(self):
        self._lock = threading.Lock()

    def predict(self, asset: str, category: str, df: pd.DataFrame) -> Tuple[float, float]:
        """
        Returns (probability_of_up, confidence).
        Phase 11: prediction latency is tracked automatically.
        """
        if _METRICS_OK:
            import time as _t
            _t0 = _t.perf_counter()
            result = self._predict_inner(asset, category, df)
            metrics.record(PREDICTION, (_t.perf_counter() - _t0) * 1000)
            return result
        return self._predict_inner(asset, category, df)

    def _predict_inner(self, asset: str, category: str, df: pd.DataFrame) -> Tuple[float, float]:
        features = build_features(df)
        if features is None:
            return 0.5, 0.0

        model_key = f"{category}_classifier"
        model     = registry.get(model_key)

        if model is None:
            return 0.5, 0.0

        try:
            with self._lock:
                proba = model.predict_proba(features.reshape(1, -1))
            up_prob    = float(proba[0][1]) if proba.shape[1] > 1 else float(proba[0][0])
            confidence = abs(up_prob - 0.5) * 2   # 0 at 0.5, 1 at 0 or 1
            logger.log_ml(model_key, asset, up_prob, confidence)
            return up_prob, confidence
        except Exception as e:
            logger.debug(f"[Predictor] Model predict error {asset}: {e}")
            return 0.5, 0.0

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