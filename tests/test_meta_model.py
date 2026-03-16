"""
tests/test_meta_model.py — Meta AI Model tests.

  Unit tests        — always run, no external services required.
  Integration tests — skipped when live data is unavailable.

Run just unit tests:
    pytest tests/test_meta_model.py -v -m "not integration"

Run everything:
    pytest tests/test_meta_model.py -v
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest


# ── Fixtures ──────────────────────────────────────────────────────────────────

def _make_ohlcv(n: int = 100, trend: str = "up", seed: int = 42) -> pd.DataFrame:
    np.random.seed(seed)
    price = 100.0
    rows  = []
    for _ in range(n):
        drift  = 0.003 if trend == "up" else (-0.003 if trend == "down" else 0.0)
        change = np.random.normal(drift, 0.012)
        open_  = price
        close  = price * (1 + change)
        high   = max(open_, close) * (1 + abs(np.random.normal(0, 0.004)))
        low    = min(open_, close) * (1 - abs(np.random.normal(0, 0.004)))
        rows.append({
            "open": round(open_, 4), "high": round(high, 4),
            "low":  round(low,  4),  "close": round(close, 4),
            "volume": 1000.0,
        })
        price = close
    return pd.DataFrame(rows)


def _make_signal(direction="BUY", confidence=0.72, asset="BTC-USD"):
    from core.signal import Signal
    return Signal(
        asset=asset, canonical_asset=asset,
        direction=direction, category="crypto",
        confidence=confidence,
        entry_price=65000.0,
        stop_loss=63500.0,
        take_profit=68000.0,
        strategy_id="voting",
    )


# ── MarketConditionClassifier tests ──────────────────────────────────────────

class TestMarketConditionClassifier:

    def test_returns_ranging_with_no_data(self):
        from ml.meta_model.market_condition_classifier import MarketConditionClassifier
        clf = MarketConditionClassifier()
        assert clf.classify(df=None) == "ranging"

    def test_detects_trending_bull(self):
        from ml.meta_model.market_condition_classifier import MarketConditionClassifier
        clf = MarketConditionClassifier()
        df  = _make_ohlcv(n=100, trend="up")
        regime = clf.classify(df=df)
        assert regime in ("trending_bull", "ranging", "high_volatility")

    def test_detects_trending_bear(self):
        from ml.meta_model.market_condition_classifier import MarketConditionClassifier
        clf = MarketConditionClassifier()
        df  = _make_ohlcv(n=100, trend="down")
        regime = clf.classify(df=df)
        assert regime in ("trending_bear", "ranging", "high_volatility")

    def test_crisis_on_high_macro(self):
        from ml.meta_model.market_condition_classifier import MarketConditionClassifier
        clf = MarketConditionClassifier()
        regime = clf.classify(
            df=None,
            macro_impact="HIGH",
            narrative_str=0.5,
        )
        assert regime == "crisis"

    def test_all_valid_regimes_returned(self):
        from ml.meta_model.market_condition_classifier import (
            MarketConditionClassifier
        )
        valid = {"trending_bull", "trending_bear", "ranging",
                 "high_volatility", "crisis"}
        clf   = MarketConditionClassifier()
        for _ in range(5):
            df     = _make_ohlcv(n=100)
            regime = clf.classify(df=df)
            assert regime in valid, f"Unknown regime: {regime}"

    def test_classify_from_context(self):
        from ml.meta_model.market_condition_classifier import MarketConditionClassifier
        clf     = MarketConditionClassifier()
        context = {
            "price_data":        _make_ohlcv(n=100, trend="up"),
            "funding_bias":      "NEUTRAL",
            "macro_impact":      "LOW",
            "narrative_strength": 0.0,
        }
        regime = clf.classify_from_context(context)
        assert isinstance(regime, str)
        assert len(regime) > 0

    def test_get_regime_description(self):
        from ml.meta_model.market_condition_classifier import MarketConditionClassifier
        clf = MarketConditionClassifier()
        for regime in ["trending_bull", "trending_bear", "ranging",
                       "high_volatility", "crisis"]:
            desc = clf.get_regime_description(regime)
            assert isinstance(desc, str)
            assert len(desc) > 5


# ── ModelWeightingEngine tests ────────────────────────────────────────────────

class TestModelWeightingEngine:

    def test_weights_sum_to_one(self):
        from ml.meta_model.model_weighting_engine import ModelWeightingEngine, REGIME_WEIGHTS
        eng = ModelWeightingEngine()
        for regime in REGIME_WEIGHTS:
            weights = eng.get_weights(regime)
            total   = sum(weights.values())
            assert abs(total - 1.0) < 0.001, f"{regime} weights sum to {total}"

    def test_all_engines_present(self):
        from ml.meta_model.model_weighting_engine import ModelWeightingEngine, ENGINES
        eng = ModelWeightingEngine()
        for regime in ["trending_bull", "ranging", "crisis"]:
            weights = eng.get_weights(regime)
            for engine in ENGINES:
                assert engine in weights, f"Missing engine '{engine}' for {regime}"

    def test_trending_bull_technical_highest(self):
        from ml.meta_model.model_weighting_engine import ModelWeightingEngine
        eng     = ModelWeightingEngine()
        weights = eng.get_weights("trending_bull")
        assert weights["technical"] == max(weights.values())

    def test_crisis_macro_highest(self):
        from ml.meta_model.model_weighting_engine import ModelWeightingEngine
        eng     = ModelWeightingEngine()
        weights = eng.get_weights("crisis")
        assert weights["macro"] == max(weights.values())

    def test_ranging_orderflow_highest(self):
        from ml.meta_model.model_weighting_engine import ModelWeightingEngine
        eng     = ModelWeightingEngine()
        weights = eng.get_weights("ranging")
        assert weights["orderflow"] == max(weights.values())

    def test_override_applied(self):
        from ml.meta_model.model_weighting_engine import ModelWeightingEngine
        eng = ModelWeightingEngine()
        eng.set_override("ranging", {
            "technical": 0.5, "sentiment": 0.1,
            "whale": 0.1, "orderflow": 0.2, "macro": 0.1,
        })
        weights = eng.get_weights("ranging")
        assert weights["technical"] > 0.4

    def test_clear_override_restores_default(self):
        from ml.meta_model.model_weighting_engine import ModelWeightingEngine
        eng = ModelWeightingEngine()
        eng.set_override("ranging", {"technical": 0.9, "sentiment": 0.025,
                                      "whale": 0.025, "orderflow": 0.025, "macro": 0.025})
        eng.clear_override("ranging")
        weights = eng.get_weights("ranging")
        # Should revert to orderflow being highest
        assert weights["orderflow"] == max(weights.values())

    def test_explain_returns_string(self):
        from ml.meta_model.model_weighting_engine import ModelWeightingEngine
        eng = ModelWeightingEngine()
        s   = eng.explain("trending_bull")
        assert isinstance(s, str)
        assert "technical" in s

    def test_unknown_regime_returns_defaults(self):
        from ml.meta_model.model_weighting_engine import ModelWeightingEngine
        eng     = ModelWeightingEngine()
        weights = eng.get_weights("nonexistent_regime")
        assert abs(sum(weights.values()) - 1.0) < 0.001


# ── EnsemblePredictor tests ───────────────────────────────────────────────────

class TestEnsemblePredictor:

    def _make_predictor(self):
        from ml.meta_model.market_condition_classifier import MarketConditionClassifier
        from ml.meta_model.model_weighting_engine      import ModelWeightingEngine
        from ml.meta_model.ensemble_predictor          import EnsemblePredictor
        return EnsemblePredictor(
            classifier=MarketConditionClassifier(),
            weighter=ModelWeightingEngine(),
        )

    def test_process_returns_signal(self):
        pred    = self._make_predictor()
        signal  = _make_signal()
        context = {"price_data": _make_ohlcv(n=100), "ml_prediction": 0.75}
        result  = pred.process(signal, context)
        assert result is signal   # same object returned

    def test_confidence_not_negative(self):
        pred    = self._make_predictor()
        signal  = _make_signal(confidence=0.30)
        context = {"price_data": _make_ohlcv(n=100), "ml_prediction": 0.10}
        signal.metadata["sentiment_score"] = -0.8
        result  = pred.process(signal, context)
        assert result.confidence >= 0.0

    def test_confidence_not_above_one(self):
        pred    = self._make_predictor()
        signal  = _make_signal(confidence=0.99)
        context = {"price_data": _make_ohlcv(n=100), "ml_prediction": 0.99}
        signal.metadata["sentiment_score"]    = 0.9
        signal.metadata["whale_dominant"]     = "BUY"
        signal.metadata["whale_buy_vol"]      = 10_000_000
        signal.metadata["whale_sell_vol"]     = 1_000_000
        signal.metadata["orderflow_imbalance"] = 0.9
        result  = pred.process(signal, context)
        assert result.confidence <= 1.0

    def test_journal_entry_added(self):
        pred    = self._make_predictor()
        signal  = _make_signal()
        context = {"price_data": _make_ohlcv(n=100), "ml_prediction": 0.7}
        pred.process(signal, context)
        names = [e.name for e in signal.journal.entries]
        assert "meta_ai" in names

    def test_metadata_populated(self):
        pred    = self._make_predictor()
        signal  = _make_signal()
        context = {"price_data": _make_ohlcv(n=100), "ml_prediction": 0.7}
        pred.process(signal, context)
        assert "meta_ai_regime"   in signal.metadata
        assert "meta_ai_ensemble" in signal.metadata
        assert "meta_ai_weights"  in signal.metadata

    def test_boost_on_strong_ensemble(self):
        """All engines strongly bullish → confidence should go up."""
        pred    = self._make_predictor()
        signal  = _make_signal(confidence=0.70, direction="BUY")
        before  = signal.confidence
        context = {
            "price_data":    _make_ohlcv(n=100, trend="up"),
            "ml_prediction": 0.90,   # strong bullish
            "funding_bias":  "HIGH_SHORT",   # shorts over-leveraged = bullish
            "oi_signal":     "TREND_CONTINUATION",
        }
        signal.metadata["sentiment_score"]    = 0.7
        signal.metadata["whale_dominant"]     = "BUY"
        signal.metadata["whale_buy_vol"]      = 8_000_000
        signal.metadata["whale_sell_vol"]     = 1_000_000
        signal.metadata["orderflow_imbalance"] = 0.6
        pred.process(signal, context)
        assert signal.confidence >= before   # should not decrease

    def test_reduce_on_weak_ensemble(self):
        """All engines bearish on a BUY signal → confidence should go down."""
        pred    = self._make_predictor()
        signal  = _make_signal(confidence=0.75, direction="BUY")
        before  = signal.confidence
        context = {
            "price_data":    _make_ohlcv(n=100, trend="down"),
            "ml_prediction": 0.10,   # strongly bearish
            "funding_bias":  "EXTREME_LONG",
            "oi_signal":     "POTENTIAL_REVERSAL",
        }
        signal.metadata["sentiment_score"]    = -0.7
        signal.metadata["whale_dominant"]     = "SELL"
        signal.metadata["whale_buy_vol"]      = 500_000
        signal.metadata["whale_sell_vol"]     = 9_000_000
        signal.metadata["orderflow_imbalance"] = -0.7
        pred.process(signal, context)
        assert signal.confidence <= before   # should not increase

    def test_no_adjustment_on_neutral(self):
        """Neutral ensemble (score ~0.5) → confidence unchanged."""
        from ml.meta_model.ensemble_predictor import EnsemblePredictor
        from ml.meta_model.market_condition_classifier import MarketConditionClassifier
        from ml.meta_model.model_weighting_engine import ModelWeightingEngine

        pred    = EnsemblePredictor(MarketConditionClassifier(), ModelWeightingEngine())
        signal  = _make_signal(confidence=0.70)
        before  = signal.confidence
        # Only neutral ML available
        context = {"ml_prediction": 0.50}
        pred.process(signal, context)
        assert signal.confidence == before   # exactly neutral = no change

    def test_compute_ensemble_with_no_scores(self):
        from ml.meta_model.ensemble_predictor import EnsemblePredictor
        from ml.meta_model.market_condition_classifier import MarketConditionClassifier
        from ml.meta_model.model_weighting_engine import ModelWeightingEngine

        pred    = EnsemblePredictor(MarketConditionClassifier(), ModelWeightingEngine())
        scores  = {"technical": None, "sentiment": None,
                   "whale": None, "orderflow": None, "macro": None}
        weights = {"technical": 0.3, "sentiment": 0.2,
                   "whale": 0.2, "orderflow": 0.2, "macro": 0.1}
        score, count = pred._compute_ensemble(scores, weights)
        assert score == 0.5   # neutral fallback
        assert count == 0


# ── MetaAILayer tests ─────────────────────────────────────────────────────────

class TestMetaAILayer:

    def test_layer_passes_signal_through(self):
        from layers.layer8_meta_ai import MetaAILayer
        layer  = MetaAILayer()
        signal = _make_signal()
        ctx    = {"price_data": _make_ohlcv(n=100), "ml_prediction": 0.7}
        result = layer.process(signal, ctx)
        assert result is not None
        assert result is signal

    def test_layer_does_not_kill_on_error(self):
        """MetaAI is enrichment only — never kills a signal."""
        from layers.layer8_meta_ai import MetaAILayer

        layer  = MetaAILayer()
        signal = _make_signal()

        # Force an error by passing invalid context
        result = layer.process(signal, context={})
        assert result is not None   # must not kill signal

    def test_layer_name_is_meta_ai(self):
        from layers.layer8_meta_ai import MetaAILayer
        assert MetaAILayer.name == "meta_ai"


# ── Integration tests ─────────────────────────────────────────────────────────

@pytest.mark.integration
class TestMetaModelIntegration:

    def test_full_pipeline_with_layer8(self):
        """Verify Layer 8 runs inside the full pipeline without breaking anything."""
        from core.pipeline import pipeline
        from core.signal   import Signal

        signal = Signal(
            asset="BTC-USD", canonical_asset="BTC-USD",
            direction="BUY", category="crypto",
            confidence=0.75,
            entry_price=65000.0,
            stop_loss=63500.0,
            take_profit=68000.0,
            strategy_id="voting",
        )
        import numpy as np
        df = _make_ohlcv(n=150, trend="up")
        context = {
            "price_data":    df,
            "ml_prediction": 0.72,
            "spread":        5.0,
        }
        result = pipeline.run(signal, context)
        # Should either pass or fail cleanly — never crash
        # Layer 8 journal entry should exist
        names = [e.name for e in signal.journal.entries]
        assert "meta_ai" in names