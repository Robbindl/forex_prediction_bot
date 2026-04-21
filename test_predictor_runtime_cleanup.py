from __future__ import annotations

from datetime import datetime, timezone

from core.engine import TradingCore
from core.signal import Signal
from core.decision_engine import count_valid_sources
from services.signal_governance import SignalGovernance


def test_playbook_runtime_seed_clears_fake_ml_placeholders() -> None:
    context = {
        "ml_prediction": 0.5,
        "ml_confidence": 0.0,
    }

    TradingCore._initialize_playbook_runtime_seed(context)

    assert context["predictor_prediction"] is None
    assert context["predictor_confidence"] == 0.0
    assert "ml_prediction" not in context
    assert "ml_confidence" not in context
    assert context["seed_decision"]["reason"] == "playbook runtime active; no external predictor attached"


def test_count_valid_sources_hides_model_lane_without_real_predictor() -> None:
    signal = Signal(
        asset="EUR/USD",
        canonical_asset="EUR/USD",
        category="forex",
        direction="BUY",
        confidence=0.8,
    )
    signal.metadata.update(
        {
            "regime": "trending_up",
            "sentiment_sources": ["comprehensive_sentiment"],
            "sentiment_timestamp": datetime.now(timezone.utc).isoformat(),
        }
    )

    total = count_valid_sources(signal)

    assert total == 2
    assert signal.metadata["eligible_source_families"] == [
        "regime",
        "sentiment",
        "macro",
        "positioning",
        "flow",
        "cross_asset",
    ]
    assert signal.metadata["valid_source_families"] == ["regime", "sentiment"]


def test_count_valid_sources_restores_model_lane_when_predictor_is_real() -> None:
    signal = Signal(
        asset="EUR/USD",
        canonical_asset="EUR/USD",
        category="forex",
        direction="BUY",
        confidence=0.8,
    )
    signal.metadata.update(
        {
            "predictor_prediction": 0.81,
            "predictor_confidence": 0.77,
            "predictor_real": True,
            "predictor_model": "future_model_v1",
            "regime": "trending_up",
            "sentiment_sources": ["comprehensive_sentiment"],
            "sentiment_timestamp": datetime.now(timezone.utc).isoformat(),
        }
    )

    total = count_valid_sources(signal)

    assert total == 3
    assert "model" in signal.metadata["eligible_source_families"]
    assert "model" in signal.metadata["valid_source_families"]
    assert signal.metadata["source_family_evidence"]["model"] == [
        "model:future_model_v1",
        "predictor_real",
    ]


def test_signal_governance_uses_playbook_confidence_without_predictor(monkeypatch) -> None:
    service = SignalGovernance()
    signal = Signal(
        asset="EUR/USD",
        canonical_asset="EUR/USD",
        category="forex",
        direction="BUY",
        confidence=0.74,
        metadata={
            "seed_source": "playbook",
            "playbook_action": "seed",
            "playbook_confidence": 0.72,
            "valid_sources_count": 4,
            "market_data": {"price": {}, "ohlcv": {}},
            "seed_model": "playbook_runtime",
        },
    )

    monkeypatch.setattr(service, "_resolve_research_model", lambda signal, model_key: (model_key, {"research_status": "playbook_runtime"}, ""))
    monkeypatch.setattr(service, "_get_live_validation", lambda asset: {"total": 0, "accuracy_pct": 0.0})
    monkeypatch.setattr(service, "_get_registry_validation", lambda asset, category: {})
    monkeypatch.setattr(service, "_get_expectancy_validation", lambda asset, category: {"sample_count": 0})
    monkeypatch.setattr(service, "_assess_model_research", lambda model_key, model_meta, category="": (True, ""))

    state = service._build_evaluation_state(signal, {})

    assert state["predictor_conf"] == 0.0
    assert state["effective_seed_confidence"] == 0.72
    assert state["min_seed_confidence"] > 0.0
