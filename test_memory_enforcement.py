from __future__ import annotations

import importlib
import sys
from types import SimpleNamespace

from core.signal import Signal


def test_memory_review_applies_confidence_adjustment(monkeypatch) -> None:
    decision_mod = importlib.import_module("core.decision_engine")

    fake_service = SimpleNamespace(
        score_setup=lambda signal, context: {
            "fingerprint": {"asset": signal.asset},
            "sample_count": 10,
            "same_asset_matches": 8,
            "avg_similarity": 0.78,
            "win_rate": 0.64,
            "memory_edge": 0.24,
            "memory_score": 71.0,
            "adjustment": 0.05,
            "notes": ["memory_positive_edge"],
        }
    )
    monkeypatch.setitem(
        sys.modules,
        "services.setup_memory_service",
        SimpleNamespace(get_service=lambda: fake_service),
    )

    signal = Signal(asset="EUR/USD", direction="BUY", category="forex", confidence=0.64)
    before = signal.confidence

    allowed = decision_mod.SignalDecisionEngine._apply_memory_review(signal, {})

    assert allowed is True
    assert signal.alive is True
    assert signal.confidence > before
    assert signal.metadata["memory_adjustment_applied"] == 0.05


def test_memory_review_blocks_strong_negative_memory(monkeypatch) -> None:
    decision_mod = importlib.import_module("core.decision_engine")

    fake_service = SimpleNamespace(
        score_setup=lambda signal, context: {
            "fingerprint": {"asset": signal.asset},
            "sample_count": 12,
            "same_asset_matches": 10,
            "avg_similarity": 0.81,
            "win_rate": 0.34,
            "memory_edge": -0.26,
            "memory_score": 31.0,
            "adjustment": -0.09,
            "notes": ["memory_negative_edge"],
        }
    )
    monkeypatch.setitem(
        sys.modules,
        "services.setup_memory_service",
        SimpleNamespace(get_service=lambda: fake_service),
    )

    signal = Signal(asset="BTC-USD", direction="BUY", category="crypto", confidence=0.69)

    allowed = decision_mod.SignalDecisionEngine._apply_memory_review(signal, {})

    assert allowed is False
    assert signal.alive is False
    assert "negative setup memory" in signal.kill_reason
    assert signal.metadata["memory_adjustment_applied"] == -0.09


def test_setup_memory_fetch_rows_passes_asset_to_db(monkeypatch) -> None:
    memory_mod = importlib.import_module("services.setup_memory_service")
    calls = {}

    fake_db = SimpleNamespace(
        get_setup_memory_records=lambda **kwargs: calls.update(kwargs) or []
    )
    monkeypatch.setitem(
        sys.modules,
        "services.db_pool",
        SimpleNamespace(get_db=lambda: fake_db),
    )

    service = memory_mod.SetupMemoryService()
    rows = service._fetch_rows(asset="BTC-USD", category="crypto", days_back=30, limit=100)

    assert rows == []
    assert calls["asset"] == "BTC-USD"
    assert calls["category"] == "crypto"
    assert calls["limit"] == 100
