"""
ml/meta_model/__init__.py — Meta AI Model.

Combines predictions from all signal engines into one ensemble score.
Runs inside the policy review stage of the decision engine,
before the signal reporter.

Every decision is written to signal.journal and appears automatically
in the Telegram signal report. No extra wiring needed for future additions.

Components
----------
    MarketConditionClassifier  — detects current market regime
    ModelWeightingEngine       — assigns per-regime weights to each engine
    EnsemblePredictor          — combines all signals into one score

Engines combined
----------------
    technical   — ML predictor (ml/predictor.py)
    sentiment   — sentiment service + narrative scores
    whale       — whale cache + on-chain data (Phase 2)
    orderflow   — bid/ask imbalance (Phase 3)
    macro       — funding rates + OI signals (Phase 1)

Market regimes
--------------
    trending_bull    — strong uptrend (ADX > 25, price above EMAs)
    trending_bear    — strong downtrend
    ranging          — sideways, low ADX
    high_volatility  — elevated volatility, unclear direction
    crisis           — extreme volatility or macro shock

Run tests
---------
    pytest tests/test_meta_model.py -v -m "not integration"
"""
from __future__ import annotations

from ml.meta_model.market_condition_classifier import MarketConditionClassifier
from ml.meta_model.model_weighting_engine      import ModelWeightingEngine
from ml.meta_model.ensemble_predictor          import EnsemblePredictor

# ── Module-level singletons ───────────────────────────────────────────────────
classifier = MarketConditionClassifier()
weighter   = ModelWeightingEngine()
predictor  = EnsemblePredictor(classifier=classifier, weighter=weighter)

__all__ = [
    "classifier", "weighter", "predictor",
    "MarketConditionClassifier", "ModelWeightingEngine", "EnsemblePredictor",
]
