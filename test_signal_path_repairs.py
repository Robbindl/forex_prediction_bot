from __future__ import annotations

import math

import pandas as pd

from core.engine import TradingCore
from core.signal import Signal
from risk.manager import RiskManager
from services.signal_scorecard import SignalScorecard


def _frame(base: float = 100.0, step: float = 0.05, rows: int = 32) -> pd.DataFrame:
    data = []
    price = base
    for _ in range(rows):
        open_price = price
        high_price = price + 0.30
        low_price = price - 0.25
        close_price = price + 0.08
        data.append(
            {
                "open": round(open_price, 6),
                "high": round(high_price, 6),
                "low": round(low_price, 6),
                "close": round(close_price, 6),
            }
        )
        price += step
    return pd.DataFrame(data)


def _core_stub() -> TradingCore:
    core = TradingCore.__new__(TradingCore)
    core._log_seed_decision = lambda *args, **kwargs: None
    return core


def test_structure_stop_uses_nearest_level_and_keeps_tighter_invalidation():
    manager = RiskManager(account_balance=10_000.0)

    stop = manager.get_stop_loss_scaled(
        entry=100.0,
        direction="BUY",
        category="forex",
        atr=0.40,
        structure={"support_levels": [99.10, 99.80]},
    )

    assert math.isclose(stop, 99.76, rel_tol=0.0, abs_tol=1e-6)


def test_entry_plan_uses_live_price_and_anchor_for_retest():
    core = _core_stub()
    frame = _frame(base=100.40, step=0.0)
    context = {"current_price": 100.52, "seed_decision": {}}

    plan = core._extract_seed_entry_price(
        "EUR/USD",
        direction="BUY",
        playbook_name="breakout_retest",
        playbook_entry_style="retest_hold",
        price_data=frame,
        playbook_price_data=frame,
        structure={"resistance": 100.50},
        context=context,
    )

    assert plan is not None
    assert plan["entry_price"] == 100.52
    assert plan["entry_source"] == "live_price"
    assert plan["anchor_role"] == "retest_level"
    assert plan["anchor_price"] == 100.5


def test_entry_plan_rejects_stale_market_entry():
    core = _core_stub()
    frame = _frame()
    context = {"current_price": 103.20, "seed_decision": {}}

    plan = core._extract_seed_entry_price(
        "BTC-USD",
        direction="BUY",
        playbook_name="breakout_continuation",
        playbook_entry_style="breakout_close",
        price_data=frame,
        playbook_price_data=frame,
        structure={},
        context=context,
    )

    assert plan is None
    assert context["seed_decision"]["reason"] == "stale_market_entry"


def test_execution_survivor_selection_keeps_top_ranked_order():
    signals = [
        Signal(asset="BTC-USD", direction="BUY", category="crypto", confidence=0.90),
        Signal(asset="ETH-USD", direction="BUY", category="crypto", confidence=0.88),
        Signal(asset="SOL-USD", direction="BUY", category="crypto", confidence=0.86),
        Signal(asset="EUR/USD", direction="BUY", category="forex", confidence=0.84),
    ]

    selected = TradingCore._select_execution_survivors(signals, limit=3)

    assert [sig.asset for sig in selected] == ["BTC-USD", "ETH-USD", "SOL-USD"]


def test_signal_scorecard_caps_confidence_when_execution_expectancy_is_bad(monkeypatch):
    scorecard = SignalScorecard()
    signal = Signal(
        asset="BTC-USD",
        direction="BUY",
        category="crypto",
        confidence=0.90,
        entry_price=100.0,
        stop_loss=99.0,
        take_profit=103.0,
        risk_reward=3.0,
        metadata={
            "playbook_action": "seed",
            "playbook_name": "breakout_continuation",
            "playbook_score": 0.90,
            "playbook_confidence": 0.90,
            "alignment_score": 0.88,
            "setup_quality": 0.86,
            "breakout_score": 0.82,
            "regime": "trending_up",
            "market_structure": {
                "alignment_score": 0.88,
                "setup_quality": 0.86,
                "breakout_score": 0.82,
                "structure_bias": "buy",
                "regime": "trending_up",
            },
        },
    )

    monkeypatch.setattr(
        SignalScorecard,
        "_live_validation",
        lambda self, asset: (0.80, {"scope": "asset", "samples": 24, "accuracy_pct": 80.0}),
    )
    monkeypatch.setattr(
        SignalScorecard,
        "_execution_expectancy",
        lambda self, sig: (
            0.25,
            {
                "scope": "asset",
                "sample_count": 18,
                "avg_rr_realized": -0.30,
                "target_hit_rate": 0.20,
                "late_entry_rate": 0.40,
                "premature_stop_rate": 0.18,
                "target_miss_rate": 0.32,
                "avg_quality_score": 38.0,
            },
        ),
    )

    result = scorecard.score(signal, {})

    assert result["final_score"] <= 0.66
    assert result["execution_expectancy"]["scope"] == "asset"


def test_signal_scorecard_bootstrap_caps_unproven_setups(monkeypatch):
    scorecard = SignalScorecard()
    signal = Signal(
        asset="EUR/USD",
        direction="BUY",
        category="forex",
        confidence=0.90,
        entry_price=1.1000,
        stop_loss=1.0950,
        take_profit=1.1120,
        risk_reward=2.4,
        metadata={
            "playbook_action": "seed",
            "playbook_name": "trend_pullback",
            "playbook_score": 0.88,
            "playbook_confidence": 0.89,
            "alignment_score": 0.84,
            "setup_quality": 0.83,
            "pullback_score": 0.78,
            "regime": "trending_up",
            "market_structure": {
                "alignment_score": 0.84,
                "setup_quality": 0.83,
                "pullback_score": 0.78,
                "structure_bias": "buy",
                "regime": "trending_up",
            },
        },
    )

    monkeypatch.setattr(
        SignalScorecard,
        "_live_validation",
        lambda self, asset: (0.50, {"scope": "bootstrap", "samples": 0, "accuracy_pct": 0.0}),
    )
    monkeypatch.setattr(
        SignalScorecard,
        "_execution_expectancy",
        lambda self, sig: (None, {"scope": "bootstrap", "sample_count": 0}),
    )

    result = scorecard.score(signal, {})

    assert result["final_score"] <= 0.72
