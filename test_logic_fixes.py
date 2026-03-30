from __future__ import annotations

import importlib
import json
import os
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import requests

import core.state as state_mod
from core.asset_profiles import get_profile
from core.assets import registry
from core.engine import TradingCore
from core.signal import Signal
from execution.paper_trader import PaperTrader
from ml.validation import evaluate_classifier_research
from risk.position_sizer import PositionSizer


def _patch_db(monkeypatch, fake_db) -> None:
    monkeypatch.setattr(sys.modules["services.db_pool"], "get_db", lambda: fake_db, raising=False)


def test_execute_signal_sizes_before_portfolio_risk() -> None:
    engine = TradingCore(balance=10_000.0)
    engine.state = SimpleNamespace(
        open_position_count=lambda: 0,
        get_open_positions=lambda: [],
        daily_pnl=0.0,
        balance=10_000.0,
        initial_balance=10_000.0,
        add_position=MagicMock(),
    )

    risk_manager = MagicMock()
    risk_manager.validate_signal.return_value = (True, "OK")
    risk_manager.calculate_position_size.return_value = 123.45
    engine._risk_manager = risk_manager
    engine._paper_trader = MagicMock()
    engine._paper_trader.execute_signal.return_value = None

    seen: dict = {}

    def _evaluate(**kwargs):
        seen.update(kwargs)
        return False, "blocked"

    engine._portfolio_risk = SimpleNamespace(evaluate=_evaluate)

    signal = Signal(
        asset="EUR/USD",
        canonical_asset="EUR/USD",
        category="forex",
        direction="BUY",
        confidence=0.82,
        entry_price=1.1000,
        stop_loss=1.0900,
        take_profit=1.1200,
    )

    assert engine._execute_signal(signal) is False
    assert seen["signal"]["position_size"] == 123.45
    engine._paper_trader.execute_signal.assert_not_called()


def test_reprice_open_positions_tightens_existing_sell_exit_levels() -> None:
    engine = TradingCore(balance=10_000.0)
    positions = [
        {
            "trade_id": "t1",
            "asset": "XAU/USD",
            "category": "commodities",
            "direction": "SELL",
            "entry_price": 4425.56,
            "stop_loss": 4491.9434,
            "original_sl": 4491.9434,
            "take_profit": 4292.7932,
        }
    ]
    synced = []

    engine.state = SimpleNamespace(
        get_open_positions=lambda: [dict(p) for p in positions],
        sync_open_position=lambda pos: synced.append(dict(pos)),
    )
    engine.fetcher = MagicMock()
    engine._risk_manager = MagicMock()
    engine._paper_trader = SimpleNamespace(_lock=threading.RLock(), open_positions={"t1": dict(positions[0])})

    price_data = pd.DataFrame(
        {
            "high": [4400, 4406, 4410, 4414, 4418, 4422, 4426, 4430, 4432, 4434, 4431, 4428, 4427, 4429, 4430, 4428],
            "low": [4392, 4398, 4402, 4406, 4410, 4414, 4418, 4422, 4424, 4425, 4422, 4419, 4418, 4420, 4421, 4419],
            "close": [4398, 4404, 4408, 4412, 4416, 4420, 4424, 4428, 4430, 4428, 4425, 4422, 4424, 4426, 4427, 4425],
        }
    )

    engine._fetch_price_data = MagicMock(return_value=price_data)
    engine._risk_manager.get_stop_loss.return_value = 4459.16
    engine._risk_manager.get_take_profit.return_value = 4375.16

    updates = engine.reprice_open_positions()

    assert len(updates) == 1
    assert updates[0]["old_stop_loss"] == 4491.9434
    assert updates[0]["new_stop_loss"] == 4459.16
    assert updates[0]["old_take_profit"] == 4292.7932
    assert updates[0]["new_take_profit"] == 4375.16
    assert synced[0]["stop_loss"] == 4459.16
    assert synced[0]["take_profit"] == 4375.16
    assert synced[0]["take_profit_levels"] == [4400.36, 4375.16, 4349.96]


def test_paper_trader_partial_tp_emits_partial_close_and_keeps_remainder() -> None:
    trader = PaperTrader(account_balance=10_000.0)
    partials = []
    updates = []
    trader.on_trade_closed = partials.append
    trader.on_position_updated = updates.append

    position = {
        "trade_id": "abc123",
        "asset": "BTC-USD",
        "category": "crypto",
        "direction": "BUY",
        "entry_price": 100.0,
        "stop_loss": 90.0,
        "original_sl": 90.0,
        "take_profit": 130.0,
        "take_profit_levels": [110.0, 120.0, 130.0],
        "position_size": 3.0,
        "open_time": datetime.utcnow().isoformat(),
        "highest_price": 100.0,
        "lowest_price": 100.0,
        "tp_hit": 0,
    }

    result = trader._check_exit(position, 111.0)

    assert result is None
    assert position["position_size"] < 3.0
    assert position["tp_hit"] == 1
    assert updates
    assert len(partials) == 1
    assert partials[0]["is_partial_close"] is True
    assert partials[0]["parent_trade_id"] == "abc123"
    assert partials[0]["trade_id"] == "abc123-PT1"


def test_state_record_partial_close_keeps_parent_open_and_zero_trade_count(monkeypatch, tmp_path: Path) -> None:
    class FakeDB:
        def __init__(self) -> None:
            self.open_positions = []
            self.trades = []
            self.daily_updates = []

        def save_open_position(self, position):
            self.open_positions.append(dict(position))

        def save_trade(self, trade):
            self.trades.append(dict(trade))
            return trade["trade_id"]

        def delete_open_position(self, trade_id):
            pass

        def upsert_daily_stats(self, date_str, pnl_delta, balance, trade_count_delta=1):
            self.daily_updates.append({
                "date": date_str,
                "pnl_delta": pnl_delta,
                "balance": balance,
                "trade_count_delta": trade_count_delta,
            })

        def get_recent_trades(self, limit):
            return [dict(t) for t in self.trades[-limit:]]

        def get_performance_summary(self, days=365):
            return {}

        def load_open_positions(self):
            return []

    fake_db = FakeDB()
    _patch_db(monkeypatch, fake_db)

    temp_state_file = tmp_path / "system_state.json"
    monkeypatch.setattr(state_mod, "_STATE_FILE", temp_state_file)
    temp_state_file.parent.mkdir(parents=True, exist_ok=True)

    state = state_mod.SystemState()
    state.set_balance(1_000.0, "test")
    state.add_position({
        "trade_id": "abc123",
        "asset": "BTC-USD",
        "canonical_asset": "BTC-USD",
        "category": "crypto",
        "direction": "BUY",
        "entry_price": 100.0,
        "stop_loss": 90.0,
        "take_profit": 130.0,
        "position_size": 3.0,
        "strategy_id": "policy_agent",
        "open_time": datetime.utcnow().isoformat(),
        "tp_hit": 0,
    })

    state.update_position_field("abc123", position_size=2.0, stop_loss=100.0, tp_hit=1)

    partial = state.record_partial_close("abc123", {
        "trade_id": "abc123-PT1",
        "parent_trade_id": "abc123",
        "asset": "BTC-USD",
        "canonical_asset": "BTC-USD",
        "category": "crypto",
        "direction": "BUY",
        "entry_price": 100.0,
        "exit_price": 110.0,
        "exit_reason": "Partial TP 1/3",
        "position_size": 1.0,
        "pnl": 50.0,
        "strategy_id": "policy_agent",
        "is_partial_close": True,
        "open_time": datetime.utcnow().isoformat(),
        "exit_time": datetime.utcnow().isoformat(),
    })

    assert partial is not None
    assert partial["trade_id"] == "abc123-PT1"
    assert state.open_position_count() == 1
    assert state.get_open_position("abc123") is not None
    assert state.get_open_position("abc123")["position_size"] == 2.0
    assert state.balance == 1_050.0
    assert fake_db.daily_updates[-1]["trade_count_delta"] == 0


def test_get_closed_positions_keeps_cached_recent_entries(monkeypatch, tmp_path: Path) -> None:
    class FakeDB:
        def get_recent_trades(self, limit):
            return [
                {"trade_id": "db-old", "exit_time": "2026-03-28T10:00:00"},
                {"trade_id": "local-old", "exit_time": "2026-03-29T09:00:00"},
            ]

    _patch_db(monkeypatch, FakeDB())

    temp_state_file = tmp_path / "system_state.json"
    monkeypatch.setattr(state_mod, "_STATE_FILE", temp_state_file)
    temp_state_file.parent.mkdir(parents=True, exist_ok=True)

    state = state_mod.SystemState()
    with state._lock:
        state._closed_positions = [
            {"trade_id": "local-old", "exit_time": "2026-03-29T09:00:00"},
            {"trade_id": "local-new", "exit_time": "2026-03-29T10:00:00"},
        ]

    closed = state.get_closed_positions(limit=3)

    assert [trade["trade_id"] for trade in closed] == ["local-new", "local-old", "db-old"]


def test_duplicate_full_close_callback_stops_side_effects(monkeypatch) -> None:
    class FakeState:
        def __init__(self) -> None:
            self.balance = 10_000.0
            self.close_position = MagicMock(return_value=None)

        def init_db(self):
            return None

        def open_position_count(self):
            return 0

        def set_balance(self, balance, reason="init"):
            self.balance = balance

        def get_open_positions(self):
            return []

    class DummyFetcher:
        def get_ohlcv(self, *args, **kwargs):
            return None

    fake_state = FakeState()

    core_state_module = importlib.import_module("core.state")
    data_fetcher_module = importlib.import_module("data.fetcher")
    ml_registry_module = importlib.import_module("ml.registry")

    monkeypatch.setattr(core_state_module, "state", fake_state, raising=False)
    monkeypatch.setattr(data_fetcher_module, "DataFetcher", DummyFetcher, raising=False)
    monkeypatch.setattr(ml_registry_module.registry, "load_all", lambda: None, raising=False)
    monkeypatch.setattr(TradingCore, "_check_offline_sl_tp", lambda self: None)

    engine = TradingCore(balance=10_000.0)
    engine.telegram = SimpleNamespace(bot=SimpleNamespace(alert_trade_closed=MagicMock()))

    assert engine._init_subsystems() is True
    engine._risk_manager.update_balance = MagicMock()

    engine._paper_trader.on_trade_closed({
        "trade_id": "dup1",
        "asset": "EUR/USD",
        "exit_price": 1.1,
        "exit_reason": "Stop Loss",
        "pnl": -10.0,
    })

    engine._risk_manager.update_balance.assert_not_called()
    engine.telegram.bot.alert_trade_closed.assert_not_called()


def test_check_exit_uses_asset_and_category_for_mt5_pnl(monkeypatch) -> None:
    called = {}

    def _fake_pnl(asset, category, entry, current, size, direction):
        called["args"] = (asset, category, entry, current, size, direction)
        return 12.34

    monkeypatch.setattr(PositionSizer, "pnl", staticmethod(_fake_pnl))

    trader = PaperTrader(account_balance=10_000.0)
    position = {
        "trade_id": "fx1",
        "asset": "EUR/USD",
        "category": "forex",
        "direction": "BUY",
        "entry_price": 1.1000,
        "stop_loss": 1.0900,
        "original_sl": 1.0900,
        "take_profit": 0.0,
        "position_size": 1_000.0,
        "open_time": datetime.utcnow().isoformat(),
    }

    assert trader._check_exit(position, 1.1050) is None
    assert called["args"][:2] == ("EUR/USD", "forex")
    assert position["pnl"] == 12.34


def test_research_validation_produces_walk_forward_metrics() -> None:
    class ThresholdModel:
        def fit(self, X, y):
            positives = X[y == 1][:, 0]
            negatives = X[y == 0][:, 0]
            self.threshold = float((positives.mean() + negatives.mean()) / 2)
            return self

        def predict(self, X):
            return (X[:, 0] >= self.threshold).astype(int)

    X_rows = []
    y_rows = []
    for i in range(120):
        X_rows.append([-2.0 + i * 0.01, 0.0])
        y_rows.append(0)
        X_rows.append([0.2 + i * 0.01, 1.0])
        y_rows.append(1)
    X = np.array(X_rows, dtype=np.float32)
    y = np.array(y_rows, dtype=np.int32)

    report = evaluate_classifier_research(
        X,
        y,
        model_factory=ThresholdModel,
        train_test_split=0.8,
        min_walk_forward_train=80,
        walk_forward_window=20,
        walk_forward_step=10,
    )

    assert report["holdout_accuracy"] >= 0.99
    assert report["walk_forward_accuracy"] >= 0.99
    assert report["walk_forward_samples"] > 0
    assert report["research_approved"] is True


def test_signal_governance_rejects_delayed_fallback_data(monkeypatch) -> None:
    governance_mod = importlib.import_module("services.signal_governance")
    governance = governance_mod.SignalGovernance()

    monkeypatch.setattr(
        governance_mod.SignalGovernance,
        "_get_live_validation",
        staticmethod(lambda asset: {"scope": "asset", "total": 40, "accuracy_pct": 61.0}),
    )
    monkeypatch.setattr(
        governance_mod.registry,
        "get_metadata",
        lambda name: {"research_approved": True, "walk_forward_accuracy": 0.58},
        raising=False,
    )

    signal = Signal(
        asset="EUR/USD",
        canonical_asset="EUR/USD",
        category="forex",
        direction="BUY",
        confidence=0.81,
        entry_price=1.10,
        stop_loss=1.09,
        take_profit=1.13,
        risk_reward=2.0,
    )
    signal.metadata.update({
        "valid_sources_count": 4,
        "ml_confidence": 0.31,
        "policy_model": "forex_policy",
    })

    verdict = governance.evaluate(signal, {
        "market_data": {
            "price": {"source": "DelayedFeed", "source_class": "fallback", "delayed": True},
            "ohlcv": {"source": "Deriv", "source_class": "primary_api", "delayed": False},
        }
    })

    assert verdict["mode"] == "deriv"
    assert verdict["approved"] is False
    assert any("price source DelayedFeed is delayed" in item for item in verdict["violations"])


def test_signal_governance_accepts_researched_primary_signal(monkeypatch) -> None:
    governance_mod = importlib.import_module("services.signal_governance")
    governance = governance_mod.SignalGovernance()

    monkeypatch.setattr(
        governance_mod.SignalGovernance,
        "_get_live_validation",
        staticmethod(lambda asset: {"scope": "asset", "total": 55, "accuracy_pct": 58.4}),
    )
    monkeypatch.setattr(
        governance_mod.registry,
        "get_metadata",
        lambda name: {
            "research_approved": True,
            "research_grade": "approved",
            "holdout_accuracy": 0.56,
            "walk_forward_accuracy": 0.55,
        },
        raising=False,
    )

    signal = Signal(
        asset="BTC-USD",
        canonical_asset="BTC-USD",
        category="crypto",
        direction="BUY",
        confidence=0.84,
        entry_price=100.0,
        stop_loss=95.0,
        take_profit=110.0,
        risk_reward=2.0,
    )
    signal.metadata.update({
        "valid_sources_count": 5,
        "ml_confidence": 0.28,
        "policy_model": "crypto_policy",
    })

    verdict = governance.evaluate(signal, {
        "market_data": {
            "price": {"source": "DerivStream", "source_class": "stream", "delayed": False},
            "ohlcv": {"source": "Deriv", "source_class": "primary_api", "delayed": False},
        }
    })

    assert verdict["mode"] == "deriv"
    assert verdict["approved"] is True
    assert verdict["grade"] in {"A", "B"}


def test_signal_governance_bootstraps_provisional_crypto_with_realtime_secondary_price(monkeypatch) -> None:
    governance_mod = importlib.import_module("services.signal_governance")
    governance = governance_mod.SignalGovernance()

    monkeypatch.setattr(
        governance_mod.SignalGovernance,
        "_get_live_validation",
        staticmethod(lambda asset: {"scope": "asset", "total": 0, "accuracy_pct": 0.0}),
    )
    monkeypatch.setattr(
        governance_mod.registry,
        "get_metadata",
        lambda name: {
            "research_approved": False,
            "research_grade": "provisional",
            "research_status": "provisional",
            "holdout_accuracy": 0.64,
            "holdout_threshold": 0.52,
            "walk_forward_accuracy": 0.53,
            "walk_forward_threshold": 0.52,
            "walk_forward_samples": 550,
            "walk_forward_required_samples": 60,
        },
        raising=False,
    )

    signal = Signal(
        asset="SOL-USD",
        canonical_asset="SOL-USD",
        category="crypto",
        direction="BUY",
        confidence=0.84,
        entry_price=100.0,
        stop_loss=95.0,
        take_profit=110.0,
        risk_reward=2.0,
    )
    signal.metadata.update({
        "valid_sources_count": 4,
        "ml_confidence": 0.28,
        "policy_model": "crypto_policy",
    })

    verdict = governance.evaluate(signal, {
        "market_data": {
            "price": {"source": "Binance", "source_class": "secondary_api", "delayed": False, "realtime": True},
            "ohlcv": {"source": "Binance", "source_class": "secondary_api", "delayed": False, "realtime": False},
        }
    })

    assert verdict["approved"] is True
    assert any("bootstrap research allowance" in item for item in verdict["warnings"])
    assert any("live validation bootstrap" in item for item in verdict["warnings"])


def test_trading_agent_decide_stamps_policy_model(monkeypatch) -> None:
    agent_mod = importlib.import_module("ml.agent")
    trading_agent = agent_mod.TradingAgent()

    monkeypatch.setattr(trading_agent, "score", lambda asset, category, df, context: (0.8, 0.6), raising=False)

    signal = Signal(
        asset="BTC-USD",
        canonical_asset="BTC-USD",
        category="crypto",
        direction="BUY",
        confidence=0.70,
    )

    result = trading_agent.decide(signal, {"price_data": pd.DataFrame({"close": [1, 2, 3]})})

    assert result is signal
    assert signal.metadata["policy_model"] == "crypto_policy"


def test_signal_governance_rejects_delayed_ohlcv_for_indices(monkeypatch) -> None:
    governance_mod = importlib.import_module("services.signal_governance")
    governance = governance_mod.SignalGovernance()

    monkeypatch.setattr(
        governance_mod.SignalGovernance,
        "_get_live_validation",
        staticmethod(lambda asset: {"scope": "asset", "total": 25, "accuracy_pct": 55.0}),
    )
    monkeypatch.setattr(
        governance_mod.registry,
        "get_metadata",
        lambda name: {"research_approved": True, "walk_forward_accuracy": 0.54},
        raising=False,
    )

    signal = Signal(
        asset="US500",
        canonical_asset="US500",
        category="indices",
        direction="BUY",
        confidence=0.78,
        entry_price=5000.0,
        stop_loss=4950.0,
        take_profit=5100.0,
        risk_reward=2.0,
    )
    signal.metadata.update({
        "valid_sources_count": 3,
        "ml_confidence": 0.24,
        "policy_model": "index_policy",
    })

    verdict = governance.evaluate(signal, {
        "timeframe": "15m",
        "market_data": {
            "price": {"source": "Deriv", "source_class": "primary_api", "delayed": False},
            "ohlcv": {"source": "DelayedFeed", "source_class": "fallback", "delayed": True},
        }
    })

    assert verdict["mode"] == "deriv"
    assert verdict["approved"] is False
    assert any("ohlcv source DelayedFeed is delayed" in item for item in verdict["violations"])


def test_signal_governance_rejects_delayed_ohlcv_even_on_slow_timeframes(monkeypatch) -> None:
    governance_mod = importlib.import_module("services.signal_governance")
    governance = governance_mod.SignalGovernance()

    monkeypatch.setattr(
        governance_mod.SignalGovernance,
        "_get_live_validation",
        staticmethod(lambda asset: {"scope": "asset", "total": 30, "accuracy_pct": 55.0}),
    )
    monkeypatch.setattr(
        governance_mod.registry,
        "get_metadata",
        lambda name: {"research_approved": True, "walk_forward_accuracy": 0.55},
        raising=False,
    )

    signal = Signal(
        asset="US500",
        canonical_asset="US500",
        category="indices",
        direction="BUY",
        confidence=0.82,
        entry_price=5000.0,
        stop_loss=4950.0,
        take_profit=5100.0,
        risk_reward=2.0,
    )
    signal.metadata.update({
        "valid_sources_count": 3,
        "ml_confidence": 0.24,
        "policy_model": "index_policy",
        "regime": "trending_bull",
    })

    verdict = governance.evaluate(signal, {
        "timeframe": "1h",
        "market_data": {
            "price": {"source": "Deriv", "source_class": "primary_api", "delayed": False},
            "ohlcv": {"source": "DelayedFeed", "source_class": "fallback", "delayed": True},
        }
    })

    assert verdict["mode"] == "deriv"
    assert verdict["approved"] is False
    assert any("ohlcv source DelayedFeed is delayed" in item for item in verdict["violations"])


def test_signal_governance_applies_forex_filter(monkeypatch) -> None:
    governance_mod = importlib.import_module("services.signal_governance")
    governance = governance_mod.SignalGovernance()

    monkeypatch.setattr(
        governance_mod.SignalGovernance,
        "_get_live_validation",
        staticmethod(lambda asset: {"scope": "asset", "total": 18, "accuracy_pct": 56.0}),
    )
    monkeypatch.setattr(
        governance_mod.SignalGovernance,
        "_run_forex_filter",
        staticmethod(lambda signal, context: (False, "spread too wide")),
    )
    monkeypatch.setattr(
        governance_mod.registry,
        "get_metadata",
        lambda name: {"research_approved": True, "walk_forward_accuracy": 0.55},
        raising=False,
    )

    signal = Signal(
        asset="EUR/USD",
        canonical_asset="EUR/USD",
        category="forex",
        direction="BUY",
        confidence=0.82,
        entry_price=1.10,
        stop_loss=1.09,
        take_profit=1.13,
        risk_reward=2.0,
    )
    signal.metadata.update({
        "valid_sources_count": 3,
        "ml_confidence": 0.22,
        "policy_model": "forex_policy",
    })

    verdict = governance.evaluate(signal, {
        "market_data": {
            "price": {"source": "Deriv", "source_class": "primary_api", "delayed": False},
            "ohlcv": {"source": "Deriv", "source_class": "primary_api", "delayed": False},
        }
    })

    assert verdict["mode"] == "deriv"
    assert verdict["approved"] is False
    assert any("forex quality: spread too wide" in item for item in verdict["violations"])


def test_asset_profiles_disable_reddit_for_non_crypto() -> None:
    assert get_profile("EUR/USD").use_reddit is False
    assert get_profile("US500").use_reddit is False
    assert get_profile("XAU/USD").use_reddit is False
    assert get_profile("BTC-USD").use_reddit is True


def test_free_market_intelligence_aggregates_oil_sources(monkeypatch) -> None:
    intel_mod = importlib.import_module("services.free_market_intelligence")
    service = intel_mod.FreeMarketIntelligence()

    monkeypatch.setattr(
        service,
        "_macro_context",
        lambda asset, category: {
            "components": {"usd_macro": 0.2},
            "details": {"macro": {"usd_broad": {"latest": 120.0}}},
            "sources": ["fred"],
        },
        raising=False,
    )
    monkeypatch.setattr(
        service,
        "_cftc_context",
        lambda asset: {"score": 0.3, "market": "WTI"},
        raising=False,
    )
    monkeypatch.setattr(
        service,
        "_eia_context",
        lambda: {"score": 0.4, "latest": 123.0},
        raising=False,
    )

    payload = service.get_asset_context("WTI", "commodities")

    assert payload["score"] == 0.3
    assert set(payload["sources"]) == {"fred", "cftc", "eia"}
    assert payload["components"]["usd_macro"] == 0.2
    assert payload["components"]["cftc_positioning"] == 0.3
    assert payload["components"]["eia_inventory"] == 0.4


def test_sentiment_review_records_market_intelligence_sources(monkeypatch) -> None:
    intel_mod = importlib.import_module("services.signal_intelligence")

    monkeypatch.setattr(
        intel_mod,
        "fetch_sentiment_details",
        lambda asset, category: {
            "score": 0.35,
            "composite_score": 0.35,
            "components": {"news": 0.2},
            "weights": {"news": 1.0},
        },
        raising=False,
    )

    signal = Signal(
        asset="EUR/USD",
        canonical_asset="EUR/USD",
        category="forex",
        direction="BUY",
        confidence=0.75,
        entry_price=1.10,
        stop_loss=1.09,
        take_profit=1.13,
    )

    payload = intel_mod.apply_sentiment_review(
        signal,
        {
            "market_intelligence": {
                "market_intelligence_sources": ["fred", "cftc"],
                "market_intelligence_score": 0.4,
                "market_intelligence_details": {"macro": {"usd_broad": {"latest": 120.0}}},
            }
        },
    )

    assert payload["score"] == 0.35
    assert signal.metadata["market_intelligence_sources"] == ["fred", "cftc"]
    assert signal.metadata["market_intelligence_score"] == 0.4


def test_decision_engine_counts_market_intelligence_as_extra_source() -> None:
    from core.decision_engine import count_valid_sources

    signal = Signal(
        asset="EUR/USD",
        canonical_asset="EUR/USD",
        category="forex",
        direction="BUY",
        confidence=0.8,
    )
    signal.metadata.update({
        "ml_prediction_real": True,
        "regime": "trending_bull",
        "sentiment_sources": ["comprehensive_sentiment"],
        "market_intelligence_sources": ["fred", "cftc"],
        "meta_ai_active_engines": 2,
    })

    assert count_valid_sources(signal) == 5


def test_market_intelligence_service_builds_asset_snapshot(monkeypatch) -> None:
    intel_mod = importlib.import_module("services.market_intelligence_service")

    service = intel_mod.MarketIntelligenceService()
    monkeypatch.setattr(
        service,
        "get_sentiment_details",
        lambda asset, category="": {"score": 0.25, "composite_score": 0.25, "components": {"news": 0.2}, "weights": {"news": 1.0}},
        raising=False,
    )
    monkeypatch.setattr(
        service,
        "get_free_market_intelligence",
        lambda asset, category="": {"score": 0.4, "sources": ["fred"]},
        raising=False,
    )
    monkeypatch.setattr(
        service,
        "get_derivatives_snapshot",
        lambda asset: {"funding_bias": "BULLISH", "oi_signal": "RISING"},
        raising=False,
    )
    monkeypatch.setattr(
        service,
        "get_narrative_snapshot",
        lambda asset: {"dominant_narrative": "ETF_NEWS", "narrative_strength": 0.62},
        raising=False,
    )
    monkeypatch.setattr(
        service,
        "get_whale_snapshot",
        lambda asset: {"has_data": True, "dominant": "BUY", "ratio": 0.81},
        raising=False,
    )

    snapshot = service.get_asset_snapshot("BTC-USD", "crypto")

    assert snapshot["asset"] == "BTC-USD"
    assert snapshot["category"] == "crypto"
    assert snapshot["sentiment_score"] == 0.25
    assert snapshot["free_market_intelligence"]["score"] == 0.4
    assert snapshot["market_intelligence_score"] == 0.4
    assert snapshot["market_intelligence_sources"] == ["fred"]
    assert snapshot["market_intelligence_details"] == {}
    assert snapshot["funding_bias"] == "BULLISH"
    assert snapshot["oi_signal"] == "RISING"
    assert snapshot["dominant_narrative"] == "ETF_NEWS"
    assert snapshot["whale_snapshot"]["dominant"] == "BUY"


def test_market_intelligence_service_formats_dashboard_whale_summary() -> None:
    intel_mod = importlib.import_module("services.market_intelligence_service")

    service = intel_mod.MarketIntelligenceService()
    now = datetime.utcnow().isoformat()
    service.record_whale_alert(
        asset="BTC-USD",
        direction="BUY",
        size_usd=2_500_000,
        source="Twitter @whale",
        sentiment=0.35,
        timestamp=now,
        raw_text="Large BTC accumulation",
        metadata={"title": "BTC whale accumulation"},
        external_id="twitter:test:1",
    )
    service.record_onchain_event(
        {
            "type": "WHALE_DISTRIBUTION",
            "asset": "BTC-USD",
            "label": "Exchange wallet",
            "delta": -4.2,
            "value_usd": 1_400_000,
            "source": "on-chain",
            "timestamp": now,
        },
        external_id="onchain:test:1",
    )

    summary = service.get_whale_dashboard_summary(min_value_usd=1_000_000, hours=24)

    assert summary["success"] is True
    assert summary["alert_count_24h"] == 2
    assert summary["top_assets"][0]["asset"] == "BTC-USD"
    assert summary["total_volume_usd"] == round(2_500_000 + 1_400_000, 0)
    assert summary["alerts"][0]["asset"] == "BTC-USD"
    assert summary["alerts"][0]["event_type"] in {"whale_alert", "onchain_event"}


def test_market_intelligence_service_treats_naive_iso_timestamp_as_utc() -> None:
    intel_mod = importlib.import_module("services.market_intelligence_service")

    naive_timestamp = datetime.utcnow().isoformat()
    normalized = intel_mod._normalize_timestamp(naive_timestamp)

    assert normalized.tzinfo is not None
    expected = datetime.fromisoformat(naive_timestamp).replace(tzinfo=timezone.utc)
    assert normalized == expected


def test_sentiment_dashboard_service_uses_market_intelligence(monkeypatch) -> None:
    dashboard_mod = importlib.import_module("services.sentiment_dashboard_service")

    fake_intelligence = SimpleNamespace(
        get_whale_events=lambda **kwargs: [{"asset": "BTC-USD", "value_usd": 2_000_000, "source": "Twitter"}],
        get_market_events=lambda days=7, limit=8: {
            "events": [{"title": "US CPI", "impact": "HIGH", "date": "2026-03-29"}],
            "earnings": [],
            "halving": {},
            "risk_outlook": {"reduce_trading": True},
        },
    )

    service = dashboard_mod.SentimentDashboardService(service=SimpleNamespace())
    monkeypatch.setattr(
        dashboard_mod.SentimentDashboardService,
        "_get_market_intelligence",
        lambda self: fake_intelligence,
        raising=False,
    )

    alerts = service.fetch_whale_alerts(min_value_usd=1_000_000)
    events = service.get_market_events()

    assert alerts[0]["asset"] == "BTC-USD"
    assert events["events"][0]["title"] == "US CPI"
    assert events["risk_outlook"]["reduce_trading"] is True


def test_engine_context_uses_market_intelligence_snapshot(monkeypatch) -> None:
    intel_mod = importlib.import_module("services.market_intelligence_service")

    fake_service = SimpleNamespace(
        get_asset_snapshot=lambda asset, category="": {
            "asset": asset,
            "category": category,
            "sentiment_score": 0.33,
            "sentiment_details": {"score": 0.33, "composite_score": 0.33, "components": {}, "weights": {}},
            "free_market_intelligence": {"score": 0.4, "sources": ["fred"]},
            "market_intelligence_score": 0.4,
            "market_intelligence_sources": ["fred"],
            "market_intelligence_details": {"macro": {"usd_broad": {"latest": 120.0}}},
            "funding_bias": "BULLISH",
            "oi_signal": "RISING",
            "narrative_strength": 0.55,
            "dominant_narrative": "ETF_NEWS",
            "whale_snapshot": {"has_data": True, "dominant": "BUY", "ratio": 0.8},
        }
    )
    monkeypatch.setattr(intel_mod, "get_service", lambda: fake_service, raising=False)

    engine = TradingCore(balance=10_000.0)
    engine.state = SimpleNamespace(
        balance=10_000.0,
        daily_pnl=0.0,
        open_position_count=lambda: 0,
    )
    engine.fetcher = MagicMock()
    monkeypatch.setattr(engine, "_get_macro_impact_static", lambda: "LOW", raising=False)

    context = engine._build_context("BTC-USD", "crypto")

    assert context["market_intelligence"]["sentiment_score"] == 0.33
    assert context["sentiment_score"] == 0.33
    assert context["funding_bias"] == "BULLISH"
    assert context["oi_signal"] == "RISING"
    assert context["narrative_strength"] == 0.55
    assert context["dominant_narrative"] == "ETF_NEWS"


def test_whale_review_uses_market_intelligence_snapshot() -> None:
    intel_mod = importlib.import_module("services.signal_intelligence")

    signal = Signal(
        asset="BTC-USD",
        canonical_asset="BTC-USD",
        category="crypto",
        direction="BUY",
        confidence=0.70,
        entry_price=100_000.0,
        stop_loss=99_000.0,
        take_profit=103_000.0,
    )

    payload = intel_mod.apply_whale_review(
        signal,
        {
            "market_intelligence": {
                "whale_snapshot": {
                    "has_data": True,
                    "dominant": "BUY",
                    "ratio": 0.82,
                    "buy_vol_m": 24.0,
                    "sell_vol_m": 4.0,
                    "clusters": 1,
                    "weighted_bull": 1.4,
                    "weighted_bear": 0.2,
                    "phase2": "whale_intelligence",
                    "source_breakdown": {"telegram": 2, "twitter": 1},
                }
            }
        },
    )

    assert payload["dominant"] == "BUY"
    assert payload["ratio"] == 0.82
    assert signal.metadata["whale_dominant"] == "BUY"
    assert signal.metadata["whale_sources"] == {"telegram": 2, "twitter": 1}
    assert signal.confidence > 0.70


def test_fetcher_prefers_deriv_stream_quote(monkeypatch) -> None:
    fetcher_mod = importlib.import_module("data.fetcher")
    ws_mod = importlib.import_module("websocket_dashboard")

    monkeypatch.setattr(fetcher_mod.DataFetcher, "_init_clients", lambda self: None, raising=False)
    monkeypatch.setattr(fetcher_mod.cache, "get", lambda key: None, raising=False)
    monkeypatch.setattr(fetcher_mod.cache, "set", lambda key, value, ttl=None: None, raising=False)
    monkeypatch.setattr(ws_mod, "get_live_price", lambda asset, max_age_seconds=10.0: (1.2345, "Deriv"), raising=False)

    fetcher = fetcher_mod.DataFetcher()
    fetcher._deriv_bridge = SimpleNamespace(
        resolve_symbol_info=lambda asset, category="": {"symbol": "frxeurusd", "display_name": "EUR/USD"},
    )

    price, spread = fetcher.get_real_time_price("EUR/USD", "forex")
    meta = fetcher.get_last_price_metadata("EUR/USD")

    assert price == 1.2345
    assert spread == 0.0
    assert meta["source"] == "Deriv"
    assert meta["source_class"] == "stream"
    assert meta["deriv_symbol"] == "frxeurusd"


def test_fetcher_market_cache_is_local_only() -> None:
    fetcher_mod = importlib.import_module("data.fetcher")
    shared_cache_mod = importlib.import_module("data.cache")

    assert fetcher_mod.cache is not shared_cache_mod.cache


def test_shared_fetcher_returns_singleton(monkeypatch) -> None:
    fetcher_mod = importlib.import_module("data.fetcher")

    monkeypatch.setattr(fetcher_mod.DataFetcher, "_init_clients", lambda self: None, raising=False)
    monkeypatch.setattr(fetcher_mod, "_shared_fetcher", None, raising=False)

    first = fetcher_mod.get_shared_fetcher()
    second = fetcher_mod.get_shared_fetcher()

    assert first is second


def test_fetcher_prefers_deriv_quote_when_stream_missing(monkeypatch) -> None:
    fetcher_mod = importlib.import_module("data.fetcher")
    ws_mod = importlib.import_module("websocket_dashboard")

    monkeypatch.setattr(fetcher_mod.DataFetcher, "_init_clients", lambda self: None, raising=False)
    monkeypatch.setattr(fetcher_mod.cache, "get", lambda key: None, raising=False)
    monkeypatch.setattr(fetcher_mod.cache, "set", lambda key, value, ttl=None: None, raising=False)
    monkeypatch.setattr(ws_mod, "get_live_price", lambda asset, max_age_seconds=10.0: (None, None), raising=False)

    fetcher = fetcher_mod.DataFetcher()
    fetcher._deriv_bridge = SimpleNamespace(
        get_quote=lambda asset, category="": (
            1.2501,
            0.0001,
            {"source": "Deriv", "source_class": "primary_api", "delayed": False, "realtime": True},
        ),
        resolve_symbol_info=lambda asset, category="": {"symbol": "frxeurusd"},
    )

    price, spread = fetcher.get_real_time_price("EUR/USD", "forex")
    meta = fetcher.get_last_price_metadata("EUR/USD")

    assert price == 1.2501
    assert spread == 0.0001
    assert meta["source"] == "Deriv"
    assert meta["source_class"] == "primary_api"


def test_deriv_bridge_logs_initial_connect_once_and_throttles_reconnects(monkeypatch) -> None:
    deriv_mod = importlib.import_module("services.deriv_bridge")
    websocket_mod = importlib.import_module("websocket")

    class _FakeWs:
        def settimeout(self, value):
            self.timeout = value

        def close(self):
            return None

    infos: list[str] = []
    debugs: list[str] = []
    warnings: list[str] = []

    monkeypatch.setattr(websocket_mod, "create_connection", lambda *args, **kwargs: _FakeWs(), raising=False)
    monkeypatch.setattr(
        deriv_mod,
        "logger",
        SimpleNamespace(
            info=lambda message: infos.append(message),
            debug=lambda message: debugs.append(message),
            warning=lambda message: warnings.append(message),
        ),
        raising=False,
    )

    bridge = deriv_mod.DerivBridge()
    bridge._enabled = True
    bridge._app_id = "test-app-id"
    bridge._url = "wss://example.invalid"
    bridge._has_connected_once = False
    bridge._reconnect_count = 0
    bridge._last_reconnect_log = 0.0

    assert bridge._connect_locked() is True
    bridge._close_locked()
    assert bridge._connect_locked() is True

    assert sum("Connected to Deriv public market data" in message for message in infos) == 1
    assert bridge._reconnect_count == 1
    assert any("Reconnected to Deriv public market data" in message for message in debugs)
    assert not warnings


def test_deriv_bridge_falls_back_to_history_when_market_closed(monkeypatch) -> None:
    deriv_mod = importlib.import_module("services.deriv_bridge")
    bridge = deriv_mod.DerivBridge()
    resolved = {
        "symbol": "frxEURUSD",
        "display_name": "EUR/USD",
        "market": "forex",
        "submarket": "major_pairs",
        "pip": 0.00001,
        "exchange_is_open": 0,
    }

    monkeypatch.setattr(bridge, "_ensure_session_locked", lambda: True, raising=False)
    monkeypatch.setattr(bridge, "_resolve_symbol_locked", lambda asset, category="": resolved, raising=False)
    monkeypatch.setattr(
        bridge,
        "_request_locked",
        lambda payload: {"candles": [{"close": 1.15123}]} if payload.get("style") == "candles" else {"history": {"prices": [], "times": []}},
        raising=False,
    )

    price, spread, meta = bridge.get_quote("EUR/USD", category="forex")

    assert price == 1.15123
    assert spread == resolved["pip"]
    assert meta["source"] == "Deriv"
    assert meta["source_class"] == "primary_api"
    assert meta["delayed"] is True
    assert meta["realtime"] is False
    assert meta["market_open"] is False


def test_deriv_bridge_does_not_cross_map_unsupported_assets(monkeypatch) -> None:
    deriv_mod = importlib.import_module("services.deriv_bridge")
    bridge = deriv_mod.DerivBridge()
    fake_symbols = [
        {"symbol": "cryBTCUSD", "display_name": "BTC/USD", "market": "cryptocurrency", "submarket": "non_stable_coin"},
        {"symbol": "cryETHUSD", "display_name": "ETH/USD", "market": "cryptocurrency", "submarket": "non_stable_coin"},
    ]

    monkeypatch.setattr(bridge, "_load_active_symbols_locked", lambda: fake_symbols, raising=False)

    assert bridge._resolve_symbol_locked("BNB-USD", category="crypto") is None
    assert bridge._resolve_symbol_locked("SOL-USD", category="crypto") is None
    assert bridge._resolve_symbol_locked("XRP-USD", category="crypto") is None


def test_deriv_bridge_normalises_decimal_pip_size() -> None:
    deriv_mod = importlib.import_module("services.deriv_bridge")

    normalized = deriv_mod.DerivBridge._normalise_active_symbol({
        "underlying_symbol": "frxEURUSD",
        "underlying_symbol_name": "EUR/USD",
        "market": "forex",
        "submarket": "major_pairs",
        "pip_size": 0.00001,
    })

    assert normalized["symbol"] == "frxEURUSD"
    assert normalized["display_name"] == "EUR/USD"
    assert normalized["pip"] == 0.00001
    assert normalized["pip_size"] == 5


def test_asset_registry_normalises_wti_aliases() -> None:
    from core.assets import registry

    assert registry.canonical("WTI") == "WTI"
    assert registry.canonical("WTI/USD") == "WTI"
    assert registry.canonical("CL=F") == "WTI"
    assert registry.category("WTI") == "commodities"


def test_asset_registry_includes_eurjpy_and_excludes_wti_from_active_universe() -> None:
    from core.assets import registry

    assets = dict(registry.all_assets())

    assert assets["EUR/JPY"] == "forex"
    assert "WTI" not in assets


def test_fetcher_marks_quote_unavailable_when_deriv_missing(monkeypatch) -> None:
    fetcher_mod = importlib.import_module("data.fetcher")
    ws_mod = importlib.import_module("websocket_dashboard")

    monkeypatch.setattr(fetcher_mod.DataFetcher, "_init_clients", lambda self: None, raising=False)
    monkeypatch.setattr(fetcher_mod.cache, "get", lambda key: None, raising=False)
    monkeypatch.setattr(fetcher_mod.cache, "set", lambda key, value, ttl=None: None, raising=False)
    monkeypatch.setattr(ws_mod, "get_live_price", lambda asset, max_age_seconds=10.0: (None, None), raising=False)

    fetcher = fetcher_mod.DataFetcher()

    price, spread = fetcher.get_real_time_price("BTC-USD", "crypto")
    meta = fetcher.get_last_price_metadata("BTC-USD")

    assert price is None
    assert spread is None
    assert meta["source"] == "unavailable"
    assert meta["source_class"] == "unavailable"


def test_fetcher_falls_back_to_binance_quote_for_unsupported_crypto(monkeypatch) -> None:
    fetcher_mod = importlib.import_module("data.fetcher")
    ws_mod = importlib.import_module("websocket_dashboard")

    monkeypatch.setattr(fetcher_mod.DataFetcher, "_init_clients", lambda self: None, raising=False)
    monkeypatch.setattr(fetcher_mod.cache, "get", lambda key: None, raising=False)
    monkeypatch.setattr(fetcher_mod.cache, "set", lambda key, value, ttl=None: None, raising=False)
    monkeypatch.setattr(ws_mod, "get_live_price", lambda asset, max_age_seconds=10.0: (None, None), raising=False)

    fetcher = fetcher_mod.DataFetcher()
    fetcher._binance_bridge = SimpleNamespace(
        get_quote=lambda asset, category="": (
            610.5,
            0.2,
            {"source": "Binance", "source_class": "secondary_api", "delayed": False, "realtime": True},
        ),
        resolve_symbol_info=lambda asset, category="": {"symbol": "BNBUSDT", "exchange": "binance"},
    )

    price, spread = fetcher.get_real_time_price("BNB-USD", "crypto")
    meta = fetcher.get_last_price_metadata("BNB-USD")

    assert price == 610.5
    assert spread == 0.2
    assert meta["source"] == "Binance"
    assert meta["source_class"] == "secondary_api"
    assert meta["exchange_symbol"] == "BNBUSDT"


def test_fetcher_records_deriv_ohlcv_metadata(monkeypatch) -> None:
    fetcher_mod = importlib.import_module("data.fetcher")
    monkeypatch.setattr(fetcher_mod.DataFetcher, "_init_clients", lambda self: None, raising=False)
    monkeypatch.setattr(fetcher_mod.cache, "get", lambda key: None, raising=False)
    monkeypatch.setattr(fetcher_mod.cache, "set", lambda key, value, ttl=None: None, raising=False)

    fetcher = fetcher_mod.DataFetcher()
    fetcher._deriv_bridge = SimpleNamespace(
        get_ohlcv=lambda asset, interval, periods, category="": (
            pd.DataFrame({
                "open": [1.0, 1.1],
                "high": [1.2, 1.2],
                "low": [0.9, 1.0],
                "close": [1.1, 1.15],
                "volume": [0.0, 0.0],
            }),
            {"source": "Deriv", "source_class": "primary_api", "delayed": False, "realtime": False},
        )
    )

    df = fetcher.get_ohlcv("EUR/USD", "forex", "15m", 2)
    meta = fetcher.get_last_ohlcv_metadata("EUR/USD", "15m")

    assert df is not None and not df.empty
    assert meta["source"] == "Deriv"
    assert meta["source_class"] == "primary_api"
    assert meta["delayed"] is False


def test_fetcher_falls_back_to_binance_ohlcv_for_unsupported_crypto(monkeypatch) -> None:
    fetcher_mod = importlib.import_module("data.fetcher")
    monkeypatch.setattr(fetcher_mod.DataFetcher, "_init_clients", lambda self: None, raising=False)
    monkeypatch.setattr(fetcher_mod.cache, "get", lambda key: None, raising=False)
    monkeypatch.setattr(fetcher_mod.cache, "set", lambda key, value, ttl=None: None, raising=False)

    fetcher = fetcher_mod.DataFetcher()
    fetcher._binance_bridge = SimpleNamespace(
        get_ohlcv=lambda asset, interval, periods, category="": (
            pd.DataFrame({
                "open": [100.0, 101.0],
                "high": [102.0, 103.0],
                "low": [99.0, 100.0],
                "close": [101.0, 102.5],
                "volume": [12_000.0, 15_000.0],
            }),
            {"source": "Binance", "source_class": "secondary_api", "delayed": False, "realtime": False},
        ),
    )

    df = fetcher.get_ohlcv("SOL-USD", "crypto", "15m", 2)
    meta = fetcher.get_last_ohlcv_metadata("SOL-USD", "15m")

    assert df is not None and not df.empty
    assert meta["source"] == "Binance"
    assert meta["source_class"] == "secondary_api"
    assert meta["delayed"] is False


def test_fetcher_marks_ohlcv_unavailable_when_deriv_missing(monkeypatch) -> None:
    fetcher_mod = importlib.import_module("data.fetcher")
    monkeypatch.setattr(fetcher_mod.DataFetcher, "_init_clients", lambda self: None, raising=False)
    monkeypatch.setattr(fetcher_mod.cache, "get", lambda key: None, raising=False)
    monkeypatch.setattr(fetcher_mod.cache, "set", lambda key, value, ttl=None: None, raising=False)

    fetcher = fetcher_mod.DataFetcher()

    df = fetcher.get_ohlcv("EUR/USD", "forex", "15m", 2)
    meta = fetcher.get_last_ohlcv_metadata("EUR/USD", "15m")

    assert df is None
    assert meta["source"] == "unavailable"
    assert meta["source_class"] == "unavailable"


def test_redis_cache_clear_only_touches_namespaced_keys(monkeypatch) -> None:
    redis_cache_mod = importlib.import_module("services.redis_cache")
    redis_pool_mod = importlib.import_module("services.redis_pool")

    class _FakeRedis:
        def __init__(self) -> None:
            self.store = {}

        def ping(self):
            return True

        def get(self, key):
            return self.store.get(key)

        def set(self, key, value, ex=None):
            self.store[key] = value
            return True

        def delete(self, *keys):
            for key in keys:
                self.store.pop(key, None)
            return True

        def exists(self, key):
            return 1 if key in self.store else 0

        def scan(self, cursor, match=None, count=100):
            import fnmatch

            keys = list(self.store.keys())
            if match:
                keys = [key for key in keys if fnmatch.fnmatch(key, match)]
            return 0, keys

    fake = _FakeRedis()
    fake.store["signals"] = '"keep"'
    fake.store["trading_bot:cache:legacy"] = '"drop"'
    monkeypatch.setattr(redis_pool_mod, "get_client", lambda: fake, raising=False)

    cache = redis_cache_mod.RedisCache(default_ttl=30, prefix="trading_bot:cache:")
    cache.set("alpha", {"ok": True})

    assert cache.get("alpha") == {"ok": True}
    assert "trading_bot:cache:alpha" in fake.store

    cache.clear()

    assert "signals" in fake.store
    assert "trading_bot:cache:alpha" not in fake.store
    assert "trading_bot:cache:legacy" not in fake.store


def test_market_hours_prefers_deriv_status(monkeypatch) -> None:
    market_hours_mod = importlib.import_module("dashboard.market_hours")

    monkeypatch.setattr(
        market_hours_mod,
        "_deriv_market_status",
        lambda asset: (False, "Closed on Deriv"),
        raising=False,
    )

    is_open, reason = market_hours_mod.is_market_open_for_asset("EUR/USD")
    status = market_hours_mod.market_status("EUR/USD")

    assert is_open is False
    assert reason == "Closed on Deriv"
    assert status["reason"] == "Closed on Deriv"
    assert status["source"] == "Deriv"


def test_trading_core_market_hours_prefers_deriv_status(monkeypatch) -> None:
    deriv_mod = importlib.import_module("services.deriv_bridge")

    monkeypatch.setattr(
        deriv_mod.deriv_bridge,
        "get_market_status",
        lambda asset, category="": {
            "asset": asset,
            "market_open": True,
            "reason": "Metals open on Deriv",
            "source": "Deriv",
        },
        raising=False,
    )

    is_open, reason = TradingCore._market_hours_status("XAU/USD", "commodities")

    assert is_open is True
    assert reason == "Metals open on Deriv"


def test_decision_engine_uses_context_market_status_over_legacy_utc_gate(monkeypatch) -> None:
    decision_mod = importlib.import_module("core.decision_engine")
    engine = decision_mod.SignalDecisionEngine()

    monkeypatch.setattr(
        decision_mod,
        "_utc_now",
        lambda: datetime(2026, 3, 29, 20, 0, tzinfo=timezone.utc),
        raising=False,
    )
    monkeypatch.setattr(
        decision_mod,
        "_get_news_state",
        lambda category: {"state": "clear", "event": "", "impact": "", "direction": "", "mins_to": 0},
        raising=False,
    )

    signal = Signal(
        asset="XAU/USD",
        canonical_asset="XAU/USD",
        category="commodities",
        direction="SELL",
        confidence=0.84,
        entry_price=4500.0,
        stop_loss=4520.0,
        take_profit=4440.0,
    )

    passed = engine._apply_market_review(
        signal,
        {
            "market_status": {"market_open": True, "reason": "Metals open on Deriv"},
            "regime": "trending_down",
        },
    )

    assert passed is True
    assert signal.alive is True
    assert signal.step_reached == decision_mod.STEP_MARKET
    assert signal.kill_reason == ""


def test_auto_trainer_start_is_non_blocking(monkeypatch) -> None:
    trainer_mod = importlib.import_module("ml.trainer")
    sync_started = threading.Event()

    def _slow_sync() -> None:
        sync_started.set()
        time.sleep(0.25)

    monkeypatch.setattr(trainer_mod, "_sync_prediction_outcomes", _slow_sync, raising=False)

    trainer = trainer_mod.AutoTrainer(fetcher=None)
    started_at = time.perf_counter()
    trainer.start()
    elapsed = time.perf_counter() - started_at

    assert elapsed < 0.5
    assert sync_started.wait(timeout=1.0) is True

    trainer.stop()
    assert trainer._thread is not None
    trainer._thread.join(timeout=1.0)


def test_prediction_tracker_uses_database_service_prediction_api(monkeypatch) -> None:
    pred_mod = importlib.import_module("prediction_tracker")

    class _FakeDB:
        def __init__(self):
            self.calls = {"ensure": 0, "save": 0, "mark": 0}

        def ensure_prediction_outcomes_table(self):
            self.calls["ensure"] += 1

        def save_prediction_outcomes(self, records):
            self.calls["save"] += len(records)

        def mark_prediction_outcome_evaluated(self, record):
            self.calls["mark"] += 1

        def get_prediction_accuracy_rollups(self, since, asset_limit=50, recent_limit=20):
            return {
                "by_horizon": [(60, 3, 2, 1, 0.5, 0.7)],
                "by_asset": [("EUR/USD", 60, 3, 2)],
                "recent": [("EUR/USD", "BUY", 1.1, 1.2, True, 1.5, 0.8, 60, datetime(2026, 3, 29, 12, 0, 0))],
            }

        def get_pending_prediction_outcomes(self, lookback_days):
            return []

    fake_db = _FakeDB()
    monkeypatch.setattr(pred_mod.PredictionTracker, "start", lambda self: None, raising=False)
    monkeypatch.setattr(pred_mod, "_db", fake_db, raising=False)
    monkeypatch.setattr(pred_mod, "_DB_AVAILABLE", True, raising=False)

    tracker = pred_mod.PredictionTracker()
    tracker._store_pending([{
        "asset": "EUR/USD",
        "direction": "BUY",
        "entry_price": 1.1,
        "confidence": 0.7,
        "signal_time": "2026-03-29T10:00:00",
        "horizon_minutes": 60,
        "eval_time": "2026-03-29T11:00:00",
    }])
    tracker._store_outcome({
        "asset": "EUR/USD",
        "actual_price": 1.2,
        "direction_correct": True,
        "target_hit": True,
        "pct_move": 1.5,
        "signal_time": "2026-03-29T10:00:00",
        "horizon_minutes": 60,
    })
    stats = tracker.get_accuracy_stats(days_back=7)

    assert fake_db.calls["ensure"] == 1
    assert fake_db.calls["save"] == 1
    assert fake_db.calls["mark"] == 1
    assert stats["by_horizon"]["1H"]["total"] == 3
    assert stats["by_asset"]["EUR/USD"]["1H"]["accuracy_pct"] == 66.7


def test_trainer_builds_live_training_data_from_database_service(monkeypatch) -> None:
    trainer_mod = importlib.import_module("ml.trainer")
    captured = {}

    class _FakeDB:
        def get_live_prediction_training_rows(self, category, since, limit=2000):
            captured["category"] = category
            captured["since"] = since
            captured["limit"] = limit
            return [
                (100.0, 101.0, json.dumps([1, 2, 3, 4, 5, 6]), json.dumps({})),
                (100.0, 99.0, json.dumps([2, 3, 4, 5, 6, 7]), json.dumps({})),
            ] * 25

    monkeypatch.setitem(
        sys.modules,
        "services.db_pool",
        SimpleNamespace(get_db=lambda: _FakeDB()),
    )

    X, y = trainer_mod._build_live_training_data("crypto")

    assert captured["category"] == "crypto"
    assert captured["limit"] == 2000
    assert isinstance(captured["since"], datetime)
    assert X is not None and X.shape == (50, 6)
    assert y is not None and len(y) == 50


def test_intelligence_alert_service_pauses_cleanly_without_redis(monkeypatch) -> None:
    service_mod = importlib.import_module("services.intelligence_alerts.intelligence_alert_service")
    service = service_mod.IntelligenceAlertService()

    monkeypatch.setitem(
        sys.modules,
        "services.redis_pool",
        SimpleNamespace(get_pubsub=lambda old_pubsub=None: None),
    )
    monkeypatch.setattr(
        service_mod.time,
        "sleep",
        lambda secs: setattr(service, "_running", False),
        raising=False,
    )

    service._running = True
    service._subscribe_loop()

    assert service._running is False


def test_order_flow_subscriber_pauses_cleanly_without_redis(monkeypatch) -> None:
    order_flow_mod = importlib.import_module("order_flow")

    monkeypatch.setitem(
        sys.modules,
        "services.redis_pool",
        SimpleNamespace(get_pubsub=lambda old_pubsub=None: None),
    )
    monkeypatch.setattr(
        time,
        "sleep",
        lambda secs: setattr(order_flow_mod, "_running", False),
    )

    order_flow_mod._running = True
    order_flow_mod._subscribe_loop()

    assert order_flow_mod._running is False


def test_liquidation_stream_subscriber_pauses_cleanly_without_redis(monkeypatch) -> None:
    liq_mod = importlib.import_module("data_ingestion.liquidation_stream")
    stream = liq_mod.LiquidationStream()
    stream._pub = object()
    stream._running = True

    monkeypatch.setattr(
        sys.modules["services.redis_pool"],
        "get_pubsub",
        lambda old_pubsub=None: None,
        raising=False,
    )
    monkeypatch.setattr(
        liq_mod.time,
        "sleep",
        lambda secs: setattr(stream, "_running", False),
    )

    stream._subscribe()

    assert stream._running is False


def test_reddit_watcher_network_backoff_skips_repeated_calls(monkeypatch) -> None:
    reddit_mod = importlib.import_module("reddit_watcher")
    reddit_mod._shared_cache.clear()
    reddit_mod._subreddit_backoff_until.clear()
    reddit_mod._subreddit_backoff_notified.clear()

    calls = {"count": 0}

    def _boom(url, headers, timeout=10):
        calls["count"] += 1
        raise requests.RequestException("WinError 10013 forbidden by its access permissions")

    monkeypatch.setattr(reddit_mod, "_rate_limited_request", _boom, raising=False)
    watcher = reddit_mod.RedditWatcher()

    assert watcher._fetch_subreddit("Forex") is None
    assert watcher._fetch_subreddit("Forex") is None
    assert calls["count"] == 1


def test_reddit_watcher_asset_map_matches_active_universe() -> None:
    reddit_mod = importlib.import_module("reddit_watcher")

    assert "EUR/JPY" in reddit_mod.RedditWatcher.ASSET_SUBREDDITS
    assert "EUR/JPY" in reddit_mod.RedditWatcher.ASSET_TERMS
    assert "WTI" not in reddit_mod.RedditWatcher.ASSET_SUBREDDITS
    assert "WTI/USD" not in reddit_mod.RedditWatcher.ASSET_SUBREDDITS


def test_reddit_watcher_unknown_asset_fallback_is_not_equity_only() -> None:
    reddit_mod = importlib.import_module("reddit_watcher")

    assert reddit_mod.RedditWatcher._default_subreddits_for_asset("EUR/CHF") == [
        "Forex",
        "Forexstrategy",
        "trading",
    ]
    assert reddit_mod.RedditWatcher._default_subreddits_for_asset("ADA-USD") == [
        "CryptoCurrency",
        "CryptoMarkets",
        "trading",
    ]


def test_system_health_service_collect_loop_degrades_cleanly_on_redis_publish_failure(
    monkeypatch,
) -> None:
    monitor_mod = importlib.import_module("monitoring.system_health_service")
    original_instance = monitor_mod.SystemHealthService._instance

    monkeypatch.setattr(monitor_mod.SystemHealthService, "_init_redis", lambda self: None)
    monitor_mod.SystemHealthService._instance = None
    service = monitor_mod.SystemHealthService()

    class _BrokenPublisher:
        def __init__(self):
            self.calls = 0

        def set(self, *args, **kwargs):
            self.calls += 1
            raise TimeoutError("redis socket timeout")

    broken_pub = _BrokenPublisher()
    current_time = {"value": 1000.0}

    monkeypatch.setattr(monitor_mod.time, "time", lambda: current_time["value"])
    monkeypatch.setattr(service, "get_snapshot", lambda: {"healthy": True})

    def _stop_after_one_sleep(_seconds: float) -> None:
        service._running = False

    monkeypatch.setattr(monitor_mod.time, "sleep", _stop_after_one_sleep)

    try:
        service._pub = broken_pub
        service._running = True
        service._redis_degraded_logged = False
        service._collect_loop()
    finally:
        service._running = False
        monitor_mod.SystemHealthService._instance = original_instance

    assert broken_pub.calls == 1
    assert service._pub is None
    assert service._redis_retry_at == current_time["value"] + 60
    assert service._redis_degraded_logged is True


def test_bnb_tracker_backoff_skips_repeated_rpc_calls(monkeypatch) -> None:
    bnb_mod = importlib.import_module("whale_intelligence.bnb_tracker")
    monkeypatch.setattr(bnb_mod, "_RPC_BACKOFF_UNTIL", 0.0)
    monkeypatch.setattr(bnb_mod, "_RPC_BACKOFF_NOTIFIED", False)

    calls = {"count": 0}

    def _fail(*args, **kwargs):
        calls["count"] += 1
        raise requests.Timeout("rpc timeout")

    monkeypatch.setattr(bnb_mod.requests, "post", _fail)
    tracker = bnb_mod.BNBTracker()
    address = "0x" + ("1" * 40)

    assert tracker.fetch_balance(address) is None
    assert tracker.fetch_balance(address) is None
    assert calls["count"] == 1
    assert bnb_mod._RPC_BACKOFF_UNTIL > 0


def test_solana_tracker_token_balance_backoff_skips_repeated_rpc_calls(monkeypatch) -> None:
    sol_mod = importlib.import_module("whale_intelligence.solana_tracker")
    monkeypatch.setattr(sol_mod, "_RPC_BACKOFF_UNTIL", 0.0)
    monkeypatch.setattr(sol_mod, "_RPC_BACKOFF_NOTIFIED", False)

    calls = {"count": 0}

    def _fail(*args, **kwargs):
        calls["count"] += 1
        raise requests.Timeout("rpc timeout")

    monkeypatch.setattr(sol_mod.requests, "post", _fail)
    tracker = sol_mod.SolanaTracker()

    assert tracker.get_token_balance("wallet", "mint") is None
    assert tracker.get_token_balance("wallet", "mint") is None
    assert calls["count"] == 1
    assert sol_mod._RPC_BACKOFF_UNTIL > 0


def test_xrp_tracker_history_backoff_skips_repeated_rpc_calls(monkeypatch) -> None:
    xrp_mod = importlib.import_module("whale_intelligence.xrp_tracker")
    monkeypatch.setattr(xrp_mod, "_RPC_BACKOFF_UNTIL", 0.0)
    monkeypatch.setattr(xrp_mod, "_RPC_BACKOFF_NOTIFIED", False)

    calls = {"count": 0}

    def _fail(*args, **kwargs):
        calls["count"] += 1
        raise requests.Timeout("rpc timeout")

    monkeypatch.setattr(xrp_mod.requests, "post", _fail)
    tracker = xrp_mod.XRPTracker()

    assert tracker.get_transaction_history("r" + ("1" * 24), limit=5) is None
    assert tracker.get_transaction_history("r" + ("1" * 24), limit=5) is None
    assert calls["count"] == 1
    assert xrp_mod._RPC_BACKOFF_UNTIL > 0


def test_telegram_send_message_timeout_is_handled_cleanly(monkeypatch) -> None:
    import threading
    from concurrent.futures import TimeoutError as FutureTimeoutError

    tg_mod = importlib.import_module("telegram_commander")

    class _FakeFuture:
        def __init__(self):
            self.cancelled = False

        def result(self, timeout=None):
            raise FutureTimeoutError()

        def cancel(self):
            self.cancelled = True

    fake_future = _FakeFuture()
    warnings = []

    def _fake_run_coroutine_threadsafe(coro, loop):
        coro.close()
        return fake_future

    commander = object.__new__(tg_mod.TelegramCommander)
    commander.chat_id = "123"
    commander.application = SimpleNamespace(bot=SimpleNamespace(send_message=lambda **kwargs: None))
    commander._loop = SimpleNamespace(is_closed=lambda: False)
    commander._rl_lock = threading.Lock()
    commander._rl_times = []

    monkeypatch.setattr(tg_mod.asyncio, "run_coroutine_threadsafe", _fake_run_coroutine_threadsafe)
    monkeypatch.setattr(tg_mod.logger, "warning", warnings.append)

    assert commander.send_message("timeout test") is False
    assert fake_future.cancelled is True
    assert warnings == ["[Telegram] send timed out"]


def test_websocket_manager_repeated_deriv_disconnects_are_downgraded(monkeypatch) -> None:
    import asyncio
    from types import ModuleType

    ws_mod = importlib.import_module("websocket_manager")
    dashboard_mod = ModuleType("websocket_dashboard")
    dashboard_mod.set_connected = lambda *args, **kwargs: None
    monkeypatch.setitem(sys.modules, "websocket_dashboard", dashboard_mod)

    manager = ws_mod.WebSocketManager()
    manager.running = True
    warnings = []
    debugs = []
    attempts = {"count": 0}

    class _FakeLoop:
        @staticmethod
        def time():
            return 0.0

    async def _fail_connect():
        attempts["count"] += 1
        if attempts["count"] >= 2:
            manager.running = False
        raise RuntimeError("socket down")

    async def _fake_sleep(_seconds: float):
        return None

    monkeypatch.setattr(manager, "_connect_deriv", _fail_connect)
    monkeypatch.setattr(ws_mod.asyncio, "get_event_loop", lambda: _FakeLoop())
    monkeypatch.setattr(ws_mod.asyncio, "sleep", _fake_sleep)
    monkeypatch.setattr(ws_mod.logger, "warning", warnings.append)
    monkeypatch.setattr(ws_mod.logger, "debug", debugs.append)

    asyncio.run(manager._connect_deriv_with_reconnect())

    assert len(warnings) == 1
    assert len(debugs) == 1
    assert "Deriv stream lost" in warnings[0]
    assert "Deriv stream still unavailable" in debugs[0]


def test_exchange_stream_repeated_disconnects_are_downgraded(monkeypatch) -> None:
    import threading

    ex_mod = importlib.import_module("data_ingestion.exchange_stream_manager")
    running = threading.Event()
    running.set()
    connection = ex_mod._ExchangeConnection("binance", lambda event: None, running)
    warnings = []
    debugs = []
    attempts = {"count": 0}

    def _fail_connect():
        attempts["count"] += 1
        if attempts["count"] >= 2:
            running.clear()
        raise RuntimeError("socket down")

    monkeypatch.setattr(connection, "_connect_and_read", _fail_connect)
    monkeypatch.setattr(ex_mod.time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(ex_mod.logger, "warning", warnings.append)
    monkeypatch.setattr(ex_mod.logger, "debug", debugs.append)

    connection._loop()

    assert len(warnings) == 1
    assert len(debugs) == 1
    assert "reconnecting in 5s" in warnings[0]
    assert "still unavailable" in debugs[0]


def test_chart_api_supports_30m_interval(monkeypatch) -> None:
    dashboard_mod = importlib.import_module("dashboard.web_app_live")

    frame = pd.DataFrame(
        {
            "open": [1.10, 1.11, 1.12],
            "high": [1.12, 1.13, 1.14],
            "low": [1.09, 1.10, 1.11],
            "close": [1.11, 1.12, 1.13],
            "volume": [100.0, 120.0, 90.0],
        },
        index=pd.date_range("2026-03-29 00:00:00", periods=3, freq="30min", tz="UTC"),
    )

    class _FakeFetcher:
        def get_ohlcv(self, asset, category, interval="15m", periods=100):
            assert asset == "EUR/USD"
            assert category == "forex"
            assert interval == "30m"
            assert periods == 1000
            return frame

    monkeypatch.setattr(dashboard_mod, "_DEVELOPMENT_MODE", True, raising=False)
    monkeypatch.setattr(dashboard_mod, "_get_fetcher", lambda: _FakeFetcher(), raising=False)

    client = dashboard_mod.app.test_client()
    response = client.get("/api/chart/candles?asset=EUR/USD&interval=30m")
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["success"] is True
    assert payload["interval_used"] == "30m"
    assert payload["bars_requested"] == 1000
    assert len(payload["candles"]) == 3


def test_correlation_matrix_api_uses_pairwise_overlap(monkeypatch) -> None:
    dashboard_mod = importlib.import_module("dashboard.web_app_live")

    idx_a = pd.date_range("2026-03-30 00:00:00", periods=20, freq="15min", tz="UTC")
    idx_b = pd.date_range("2026-03-30 00:30:00", periods=20, freq="15min", tz="UTC")
    frame_a = pd.DataFrame({"close": np.linspace(100, 119, 20)}, index=idx_a)
    frame_b = pd.DataFrame({"close": np.linspace(200, 238, 20)}, index=idx_b)

    class _FakeFetcher:
        def get_ohlcv(self, asset, category, interval="15m", periods=50):
            if asset == "EUR/USD":
                return frame_a
            if asset == "GBP/USD":
                return frame_b
            return None

    monkeypatch.setattr(dashboard_mod, "ALL_ASSETS", [("EUR/USD", "forex"), ("GBP/USD", "forex")], raising=False)
    monkeypatch.setattr(dashboard_mod, "_fetcher", _FakeFetcher(), raising=False)
    monkeypatch.setattr(dashboard_mod, "_cache_get", lambda key: None, raising=False)
    monkeypatch.setattr(dashboard_mod, "_cache_set", lambda key, value, ttl=30: None, raising=False)

    client = dashboard_mod.app.test_client()
    response = client.get("/api/correlation-matrix")
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["success"] is True
    assert set(payload["labels"]) == {"EUR/USD", "GBP/USD"}
    index = {label: idx for idx, label in enumerate(payload["labels"])}
    assert payload["matrix"][index["EUR/USD"]][index["EUR/USD"]] == 1.0
    assert payload["matrix"][index["GBP/USD"]][index["GBP/USD"]] == 1.0
    assert all(np.isfinite(value) for row in payload["matrix"] for value in row)


def test_heatmap_api_reports_partial_payload(monkeypatch) -> None:
    dashboard_mod = importlib.import_module("dashboard.web_app_live")

    monkeypatch.setattr(
        dashboard_mod,
        "ALL_ASSETS",
        [("EUR/USD", "forex"), ("BTC-USD", "crypto"), ("ETH-USD", "crypto")],
        raising=False,
    )
    monkeypatch.setattr(dashboard_mod, "_cache_get", lambda key: None, raising=False)
    monkeypatch.setattr(dashboard_mod, "_cache_set", lambda key, value, ttl=0: None, raising=False)
    monkeypatch.setattr(dashboard_mod, "_is_market_weekend", lambda category: False, raising=False)

    class _FakeFetcher:
        def get_ohlcv(self, asset, category, interval="1d", periods=5):
            if asset == "ETH-USD":
                return None
            return pd.DataFrame(
                {
                    "open": [100.0, 110.0],
                    "close": [110.0, 120.0],
                },
                index=pd.date_range("2026-03-29", periods=2, freq="1D", tz="UTC"),
            )

    monkeypatch.setattr(dashboard_mod, "_fetcher", _FakeFetcher(), raising=False)

    client = dashboard_mod.app.test_client()
    response = client.get("/api/market/heatmap")
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["success"] is True
    assert payload["expected_assets"] == 3
    assert payload["partial"] is True
    assert len(payload["items"]) == 2


def test_market_intelligence_page_overview_stays_lightweight(monkeypatch) -> None:
    dashboard_mod = importlib.import_module("dashboard.web_app_live")

    monkeypatch.setattr(dashboard_mod, "_cache_get", lambda key: None, raising=False)
    monkeypatch.setattr(dashboard_mod, "_cache_set", lambda key, value, ttl=0: None, raising=False)

    class _FakeResponse:
        def __init__(self, payload):
            self._payload = payload

        def get_json(self):
            return self._payload

    def _fake_call_view(fn):
        name = getattr(fn, "__name__", "")
        if name == "api_chart_assets":
            return _FakeResponse({"success": True, "assets": [{"symbol": "EUR/USD", "category": "forex"}]})
        raise AssertionError(f"Unexpected view {name}")

    monkeypatch.setattr(dashboard_mod, "_call_view", _fake_call_view, raising=False)

    with dashboard_mod.app.test_request_context("/api/page-overview?page=market_intelligence"):
        response = dashboard_mod.api_page_overview()

    payload = response.get_json()
    assert payload["success"] is True
    assert "assets" in payload
    assert "heatmap" not in payload


def test_page_overview_normalizes_cached_response_objects(monkeypatch) -> None:
    dashboard_mod = importlib.import_module("dashboard.web_app_live")

    class _FakeResponse:
        def __init__(self, payload):
            self._payload = payload

        def get_json(self):
            return self._payload

    monkeypatch.setattr(
        dashboard_mod,
        "_cache_get",
        lambda key: {
            "success": True,
            "command_center": _FakeResponse({"success": True, "balance": 123.0}),
            "whale": (_FakeResponse({"success": True, "alert_count_24h": 4}), 200),
        },
        raising=False,
    )

    with dashboard_mod.app.test_request_context("/api/page-overview?page=command_center"):
        response = dashboard_mod.api_page_overview()

    payload = response.get_json()
    assert response.status_code == 200
    assert payload["success"] is True
    assert payload["command_center"]["balance"] == 123.0
    assert payload["whale"]["alert_count_24h"] == 4


def test_command_center_survives_live_price_wait_timeout(monkeypatch) -> None:
    dashboard_mod = importlib.import_module("dashboard.web_app_live")
    futures_mod = importlib.import_module("concurrent.futures")

    class _FakeCore:
        is_running = True
        is_ready = True

        def get_performance(self):
            return {"balance": 1000.0, "total_pnl": 10.0, "win_rate": 55.0, "total_trades": 4}

        def get_daily_stats(self):
            return {"daily_pnl": 2.0, "daily_trades": 1}

        def get_positions(self):
            return [
                {
                    "trade_id": "t1",
                    "asset": "BTC-USD",
                    "category": "crypto",
                    "direction": "BUY",
                    "confidence": 0.8,
                    "entry_price": 100.0,
                    "stop_loss": 95.0,
                    "take_profit": 110.0,
                    "position_size": 1.0,
                    "strategy_id": "policy_agent",
                    "open_time": "2026-03-30T00:00:00",
                    "pnl": 0.0,
                }
            ]

        def health_report(self):
            return {"is_running": True, "engine_ready": True}

    class _FakeFuture:
        def cancel(self):
            return True

        def result(self):
            return (101.0, 0.0)

    class _FakePool:
        def __init__(self, max_workers=1):
            self.futures = []

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def submit(self, fn, *args, **kwargs):
            fut = _FakeFuture()
            self.futures.append(fut)
            return fut

    monkeypatch.setattr(dashboard_mod, "_CORE", _FakeCore(), raising=False)
    monkeypatch.setattr(dashboard_mod, "_cache_get", lambda key: None, raising=False)
    monkeypatch.setattr(dashboard_mod, "_cache_set", lambda key, value, ttl=0: None, raising=False)
    monkeypatch.setattr(dashboard_mod, "_get_sent", lambda: None, raising=False)
    monkeypatch.setattr(dashboard_mod, "_get_market_intelligence", lambda: None, raising=False)
    monkeypatch.setattr(futures_mod, "ThreadPoolExecutor", _FakePool, raising=False)
    monkeypatch.setattr(futures_mod, "wait", lambda fs, timeout=None: (set(), set(fs)), raising=False)

    with dashboard_mod.app.test_request_context("/api/command-center"):
        response = dashboard_mod.api_command_center()

    payload = response.get_json()
    assert response.status_code == 200
    assert payload["success"] is True
    assert payload["open_positions"] == 1
    assert payload["positions"][0]["current_price"] == 0.0


def test_market_events_preserves_risk_outlook(monkeypatch) -> None:
    dashboard_mod = importlib.import_module("dashboard.web_app_live")
    fake_intelligence = SimpleNamespace(
        get_market_events=lambda days=7, limit=20: {
            "events": [{"title": "US CPI", "impact": "HIGH"}],
            "earnings": [],
            "halving": {},
            "risk_outlook": {"reduce_trading": True, "summary": "Reduce risk into CPI"},
        }
    )

    monkeypatch.setattr(dashboard_mod, "_DEVELOPMENT_MODE", True, raising=False)
    monkeypatch.setattr(dashboard_mod, "_get_market_intelligence", lambda: fake_intelligence, raising=False)

    client = dashboard_mod.app.test_client()
    response = client.get("/api/market/events")
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["success"] is True
    assert payload["risk_outlook"]["reduce_trading"] is True
    assert payload["risk_outlook"]["summary"] == "Reduce risk into CPI"
    assert payload["events"][0]["title"] == "US CPI"


def test_system_monitor_overview_includes_snapshot_payload(monkeypatch) -> None:
    dashboard_mod = importlib.import_module("dashboard.web_app_live")

    class _FakeResponse:
        def __init__(self, payload):
            self._payload = payload

        def get_json(self):
            return self._payload

    def _fake_call_view(fn):
        name = getattr(fn, "__name__", "")
        if name == "api_system_health":
            return _FakeResponse({"success": True, "cpu_pct": 10.0})
        if name == "api_monitoring_metrics":
            return _FakeResponse({"success": True, "metrics": {}})
        if name == "api_monitoring_errors":
            return _FakeResponse({"success": True, "errors": {}})
        if name == "api_monitoring_snapshot":
            return _FakeResponse({
                "success": True,
                "source_health": {"technicals": {"fresh": True, "status": "fresh"}},
                "total_signals": 12,
            })
        raise AssertionError(f"Unexpected view {name}")

    monkeypatch.setattr(dashboard_mod, "_cache_get", lambda key: None, raising=False)
    monkeypatch.setattr(dashboard_mod, "_cache_set", lambda key, value, ttl=0: None, raising=False)
    monkeypatch.setattr(dashboard_mod, "_call_view", _fake_call_view, raising=False)

    with dashboard_mod.app.test_request_context("/api/system-monitor/overview"):
        response = dashboard_mod.api_system_monitor_overview()

    payload = response.get_json()
    assert payload["success"] is True
    assert payload["snapshot"]["source_health"]["technicals"]["fresh"] is True
    assert payload["snapshot"]["total_signals"] == 12


def test_monitoring_snapshot_uses_get_snapshot(monkeypatch) -> None:
    dashboard_mod = importlib.import_module("dashboard.web_app_live")
    monitor_mod = importlib.import_module("monitoring.system_health_service")

    monkeypatch.setattr(dashboard_mod, "_cache_get", lambda key: None, raising=False)
    monkeypatch.setattr(dashboard_mod, "_cache_set", lambda key, value, ttl=0: None, raising=False)
    monkeypatch.setattr(
        monitor_mod,
        "monitor",
        SimpleNamespace(get_snapshot=lambda: {"errors": {"redis": "timeout"}, "source_health": {"technicals": {"fresh": True}}}),
        raising=False,
    )

    client = dashboard_mod.app.test_client()
    response = client.get("/api/monitoring/snapshot")
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["success"] is True
    assert payload["errors"]["redis"] == "timeout"
    assert payload["source_health"]["technicals"]["fresh"] is True


def test_monitoring_errors_uses_get_snapshot(monkeypatch) -> None:
    dashboard_mod = importlib.import_module("dashboard.web_app_live")
    monitor_mod = importlib.import_module("monitoring.system_health_service")

    monkeypatch.setattr(dashboard_mod, "_cache_get", lambda key: None, raising=False)
    monkeypatch.setattr(dashboard_mod, "_cache_set", lambda key, value, ttl=0: None, raising=False)
    monkeypatch.setattr(
        monitor_mod,
        "monitor",
        SimpleNamespace(get_snapshot=lambda: {"errors": {"rss": "high memory"}}),
        raising=False,
    )

    client = dashboard_mod.app.test_client()
    response = client.get("/api/monitoring/errors")
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["success"] is True
    assert payload["errors"]["rss"] == "high memory"


def test_chart_stream_emits_connected_event_immediately(monkeypatch) -> None:
    dashboard_mod = importlib.import_module("dashboard.web_app_live")

    class _FakeFetcher:
        def get_real_time_price(self, asset, category):
            return None, None

    monkeypatch.setattr(dashboard_mod, "_DEVELOPMENT_MODE", True, raising=False)
    monkeypatch.setattr(dashboard_mod, "_fetcher", _FakeFetcher(), raising=False)

    client = dashboard_mod.app.test_client()
    response = client.get("/api/chart/stream?asset=BTC-USD", buffered=False)
    first_chunk = next(response.response).decode("utf-8")
    response.close()

    assert response.status_code == 200
    assert '"type": "connected"' in first_chunk
    assert '"asset": "BTC-USD"' in first_chunk


def test_chart_stream_is_not_gzipped(monkeypatch) -> None:
    dashboard_mod = importlib.import_module("dashboard.web_app_live")

    class _FakeFetcher:
        def get_real_time_price(self, asset, category):
            return 123.45, 0.0

    monkeypatch.setattr(dashboard_mod, "_DEVELOPMENT_MODE", True, raising=False)
    monkeypatch.setattr(dashboard_mod, "_fetcher", _FakeFetcher(), raising=False)

    client = dashboard_mod.app.test_client()
    response = client.get(
        "/api/chart/stream?asset=BTC-USD",
        buffered=False,
        headers={"Accept-Encoding": "gzip"},
    )
    first_chunk = next(response.response).decode("utf-8")
    response.close()

    assert response.status_code == 200
    assert response.headers.get("Content-Encoding") is None
    assert '"type": "connected"' in first_chunk


def test_config_ignores_legacy_deriv_aliases(monkeypatch) -> None:
    config_mod = importlib.import_module("config.config")
    original_app_id = os.environ.get("DERIV_APP_ID")
    original_token = os.environ.get("DERIV_TOKEN")
    original_legacy_app_id = os.environ.get("DERIV_API_APP_ID")
    original_legacy_token = os.environ.get("DERIV_API_TOKEN")

    try:
        monkeypatch.setenv("DERIV_APP_ID", "")
        monkeypatch.setenv("DERIV_TOKEN", "")
        monkeypatch.setenv("DERIV_API_APP_ID", "legacy-app-id")
        monkeypatch.setenv("DERIV_API_TOKEN", "legacy-token")

        reloaded = importlib.reload(config_mod)
        assert reloaded.DERIV_APP_ID == ""
        assert reloaded.DERIV_TOKEN == ""
    finally:
        for key, value in {
            "DERIV_APP_ID": original_app_id,
            "DERIV_TOKEN": original_token,
            "DERIV_API_APP_ID": original_legacy_app_id,
            "DERIV_API_TOKEN": original_legacy_token,
        }.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        importlib.reload(config_mod)


def test_config_intelligence_chat_falls_back_to_command_bot_chat(monkeypatch) -> None:
    config_mod = importlib.import_module("config.config")
    original_intelligence = os.environ.get("INTELLIGENCE_CHAT_ID")
    original_command_chat = os.environ.get("COMMAND_BOT_CHAT_ID")

    try:
        monkeypatch.setenv("INTELLIGENCE_CHAT_ID", "")
        monkeypatch.setenv("COMMAND_BOT_CHAT_ID", "command-chat")

        reloaded = importlib.reload(config_mod)
        assert reloaded.INTELLIGENCE_CHAT_ID == "command-chat"
    finally:
        for key, value in {
            "INTELLIGENCE_CHAT_ID": original_intelligence,
            "COMMAND_BOT_CHAT_ID": original_command_chat,
        }.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        importlib.reload(config_mod)


def test_config_exports_telegram_aliases_from_command_bot_vars(monkeypatch) -> None:
    config_mod = importlib.import_module("config.config")
    original_command_token = os.environ.get("COMMAND_BOT_TOKEN")
    original_command_chat = os.environ.get("COMMAND_BOT_CHAT_ID")
    original_telegram_token = os.environ.get("TELEGRAM_TOKEN")
    original_telegram_chat = os.environ.get("TELEGRAM_CHAT_ID")

    try:
        monkeypatch.setenv("COMMAND_BOT_TOKEN", "command-token")
        monkeypatch.setenv("COMMAND_BOT_CHAT_ID", "command-chat")
        monkeypatch.setenv("TELEGRAM_TOKEN", "legacy-token")
        monkeypatch.setenv("TELEGRAM_CHAT_ID", "legacy-chat")

        reloaded = importlib.reload(config_mod)
        assert reloaded.COMMAND_BOT_TOKEN == "command-token"
        assert reloaded.COMMAND_BOT_CHAT_ID == "command-chat"
        assert reloaded.TELEGRAM_TOKEN == "command-token"
        assert reloaded.TELEGRAM_CHAT_ID == "command-chat"
    finally:
        for key, value in {
            "COMMAND_BOT_TOKEN": original_command_token,
            "COMMAND_BOT_CHAT_ID": original_command_chat,
            "TELEGRAM_TOKEN": original_telegram_token,
            "TELEGRAM_CHAT_ID": original_telegram_chat,
        }.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        importlib.reload(config_mod)


def test_risk_manager_uses_configured_daily_loss_limit(monkeypatch) -> None:
    config_mod = importlib.import_module("config.config")
    risk_mod = importlib.import_module("risk.manager")
    original_daily_loss = os.environ.get("DAILY_LOSS_LIMIT_PERCENT")

    try:
        monkeypatch.setenv("DAILY_LOSS_LIMIT_PERCENT", "4.0")
        importlib.reload(config_mod)
        reloaded_risk = importlib.reload(risk_mod)

        manager = reloaded_risk.RiskManager(account_balance=10_000.0)
        allowed, reason = manager.validate_signal(confidence=0.90, daily_pnl=-400.0, category="forex")

        assert allowed is False
        assert "4.0%" in reason
    finally:
        if original_daily_loss is None:
            os.environ.pop("DAILY_LOSS_LIMIT_PERCENT", None)
        else:
            os.environ["DAILY_LOSS_LIMIT_PERCENT"] = original_daily_loss
        importlib.reload(config_mod)
        importlib.reload(risk_mod)


def test_risk_manager_uses_atr_based_levels_for_commodities() -> None:
    risk_mod = importlib.import_module("risk.manager")
    manager = risk_mod.RiskManager(account_balance=10_000.0)

    entry = 4425.56
    stop_loss = manager.get_stop_loss(entry, "SELL", "commodities", atr=24.0)
    take_profit = manager.get_take_profit(entry, stop_loss, "SELL", category="commodities")

    risk = round(stop_loss - entry, 2)
    reward = round(entry - take_profit, 2)

    assert risk == 33.60
    assert reward == 50.40


def test_generate_seed_signal_passes_estimated_atr_to_risk_manager() -> None:
    engine = TradingCore(balance=10_000.0)
    engine._predictor = SimpleNamespace(predict=lambda canonical, category, df: (0.20, 0.85))

    seen: dict = {}

    class _RiskStub:
        def get_stop_loss(self, entry, direction, category, atr=0.0):
            seen["atr"] = atr
            return entry + 10.0 if direction == "SELL" else entry - 10.0

        def get_take_profit(self, entry, stop_loss, direction, category="", rr=None):
            return entry - 15.0 if direction == "SELL" else entry + 15.0

    engine._risk_manager = _RiskStub()

    price_data = pd.DataFrame(
        {
            "high": [4400, 4406, 4410, 4414, 4418, 4422, 4426, 4430, 4432, 4434, 4431, 4428, 4427, 4429, 4430, 4428],
            "low": [4392, 4398, 4402, 4406, 4410, 4414, 4418, 4422, 4424, 4425, 4422, 4419, 4418, 4420, 4421, 4419],
            "close": [4398, 4404, 4408, 4412, 4416, 4420, 4424, 4428, 4430, 4428, 4425, 4422, 4424, 4426, 4427, 4425],
        }
    )
    context = {"market_data": {}}

    signal = engine._generate_seed_signal("XAU/USD", "XAU/USD", "commodities", price_data, context)

    assert signal is not None
    assert seen["atr"] > 0.0
    assert signal.metadata["exit_model"] == "atr"
    assert signal.metadata["atr"] > 0.0


def test_portfolio_risk_uses_configured_drawdown_halt(monkeypatch) -> None:
    config_mod = importlib.import_module("config.config")
    portfolio_mod = importlib.import_module("risk.portfolio_risk")
    original_halt = os.environ.get("DRAWDOWN_HALT_PERCENT")
    original_reduce = os.environ.get("DRAWDOWN_REDUCE_PERCENT")

    try:
        monkeypatch.setenv("DRAWDOWN_HALT_PERCENT", "7.0")
        monkeypatch.setenv("DRAWDOWN_REDUCE_PERCENT", "5.0")
        importlib.reload(config_mod)
        reloaded_portfolio = importlib.reload(portfolio_mod)

        engine = reloaded_portfolio.PortfolioRiskEngine()
        approved, reason = engine.evaluate(
            signal={"asset": "EUR/USD", "category": "forex", "position_size": 1.0, "entry_price": 1.10},
            open_positions=[],
            balance=10_000.0,
        )
        assert approved is True

        approved, reason = engine.evaluate(
            signal={"asset": "EUR/USD", "category": "forex", "position_size": 1.0, "entry_price": 1.10},
            open_positions=[],
            balance=9_300.0,
        )
        assert approved is False
        assert "halt threshold 7.0%" in reason
    finally:
        for key, value in {
            "DRAWDOWN_HALT_PERCENT": original_halt,
            "DRAWDOWN_REDUCE_PERCENT": original_reduce,
        }.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        importlib.reload(config_mod)
        importlib.reload(portfolio_mod)


def test_chain_trackers_use_configured_rpc_urls(monkeypatch) -> None:
    config_mod = importlib.import_module("config.config")
    bnb_mod = importlib.import_module("whale_intelligence.bnb_tracker")
    sol_mod = importlib.import_module("whale_intelligence.solana_tracker")
    xrp_mod = importlib.import_module("whale_intelligence.xrp_tracker")
    originals = {
        "BNB_RPC_URL": os.environ.get("BNB_RPC_URL"),
        "SOLANA_RPC_URL": os.environ.get("SOLANA_RPC_URL"),
        "XRPL_RPC_URL": os.environ.get("XRPL_RPC_URL"),
    }

    try:
        monkeypatch.setenv("BNB_RPC_URL", "https://bnb.example")
        monkeypatch.setenv("SOLANA_RPC_URL", "https://solana.example")
        monkeypatch.setenv("XRPL_RPC_URL", "https://xrpl.example")

        importlib.reload(config_mod)
        bnb_reloaded = importlib.reload(bnb_mod)
        sol_reloaded = importlib.reload(sol_mod)
        xrp_reloaded = importlib.reload(xrp_mod)

        assert bnb_reloaded.BNBTracker()._rpc_url == "https://bnb.example"
        assert sol_reloaded.SolanaTracker()._rpc_url == "https://solana.example"
        assert xrp_reloaded.XRPTracker()._rpc_url == "https://xrpl.example"
    finally:
        for key, value in originals.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        importlib.reload(config_mod)
        importlib.reload(bnb_mod)
        importlib.reload(sol_mod)
        importlib.reload(xrp_mod)


def test_prediction_service_uses_configured_ml_service_port(monkeypatch) -> None:
    config_mod = importlib.import_module("config.config")
    service_mod = importlib.import_module("ml.prediction_service")
    original_port = os.environ.get("ML_SERVICE_PORT")

    try:
        monkeypatch.setenv("ML_SERVICE_PORT", "9205")
        importlib.reload(config_mod)
        reloaded = importlib.reload(service_mod)
        assert reloaded._PORT == 9205
    finally:
        if original_port is None:
            os.environ.pop("ML_SERVICE_PORT", None)
        else:
            os.environ["ML_SERVICE_PORT"] = original_port
        importlib.reload(config_mod)
        importlib.reload(service_mod)


def test_config_database_url_requires_explicit_env(monkeypatch) -> None:
    config_mod = importlib.import_module("config.config")
    original_db_url = os.environ.get("DATABASE_URL")

    try:
        monkeypatch.setenv("DATABASE_URL", "")
        reloaded = importlib.reload(config_mod)
        assert reloaded.DATABASE_URL == ""
    finally:
        if original_db_url is None:
            os.environ.pop("DATABASE_URL", None)
        else:
            os.environ["DATABASE_URL"] = original_db_url
        importlib.reload(config_mod)


def test_telegram_manager_uses_configured_debug_flag_and_pid_file(monkeypatch) -> None:
    config_mod = importlib.import_module("config.config")
    manager_mod = importlib.import_module("telegram_manager")
    original_debug = os.environ.get("DEBUG_FORCE_TELEGRAM")
    original_pid = os.environ.get("TELEGRAM_PID_FILE")

    try:
        monkeypatch.setenv("DEBUG_FORCE_TELEGRAM", "true")
        monkeypatch.setenv("TELEGRAM_PID_FILE", "custom_telegram.pid")
        importlib.reload(config_mod)
        reloaded_manager = importlib.reload(manager_mod)

        assert config_mod.DEBUG_FORCE_TELEGRAM is True
        assert config_mod.TELEGRAM_PID_FILE.name == "custom_telegram.pid"
        assert reloaded_manager.TelegramManager._pid_file.name == "custom_telegram.pid"
    finally:
        for key, value in {
            "DEBUG_FORCE_TELEGRAM": original_debug,
            "TELEGRAM_PID_FILE": original_pid,
        }.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        importlib.reload(config_mod)
        importlib.reload(manager_mod)


def test_telegram_category_keyboard_matches_active_universe() -> None:
    tg_mod = importlib.import_module("telegram_commander")
    keyboard = tg_mod._category_keyboard().inline_keyboard
    callback_data = {
        button.callback_data
        for row in keyboard
        for button in row
        if getattr(button, "callback_data", None)
    }

    assert "cat:stocks" not in callback_data
    assert tg_mod._CATEGORY_ASSETS["crypto"] == registry.assets_by_category("crypto")
    assert tg_mod._CATEGORY_ASSETS["forex"] == registry.assets_by_category("forex")
    assert tg_mod._CATEGORY_ASSETS["commodities"] == registry.assets_by_category("commodities")
    assert tg_mod._CATEGORY_ASSETS["indices"] == registry.assets_by_category("indices")


def test_trade_history_api_uses_shared_db_service(monkeypatch) -> None:
    dashboard_mod = importlib.import_module("dashboard.web_app_live")

    class _FakeDB:
        def get_recent_trades(self, limit=50):
            assert limit == 2
            return [{
                "trade_id": "t1",
                "asset": "EUR/USD",
                "category": "forex",
                "direction": "BUY",
                "entry_time": "2026-03-29T10:00:00+00:00",
                "exit_time": "2026-03-29T11:30:00+00:00",
                "exit_reason": "Take Profit",
                "pnl": 12.5,
            }]

    monkeypatch.setattr(sys.modules["services.db_pool"], "get_db", lambda: _FakeDB(), raising=False)
    client = dashboard_mod.app.test_client()
    response = client.get("/api/trade-history?limit=2")
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["success"] is True
    assert payload["count"] == 1
    assert payload["trades"][0]["display_timezone"] == "EAT"
    assert payload["trades"][0]["duration_str"] == "1h 30m"


def test_trade_history_api_disables_caching(monkeypatch) -> None:
    dashboard_mod = importlib.import_module("dashboard.web_app_live")

    class _FakeDB:
        def get_recent_trades(self, limit=50):
            return [{
                "trade_id": "t1",
                "asset": "BTC-USD",
                "category": "crypto",
                "direction": "SELL",
                "entry_time": "2026-03-29T10:00:00+00:00",
                "exit_time": "2026-03-29T11:30:00+00:00",
                "exit_reason": "Take Profit",
                "pnl": 12.5,
            }]

    monkeypatch.setattr(sys.modules["services.db_pool"], "get_db", lambda: _FakeDB(), raising=False)
    client = dashboard_mod.app.test_client()
    response = client.get("/api/trade-history?limit=1")
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["success"] is True
    assert payload["count"] == 1
    assert response.headers["Cache-Control"] == "no-cache, no-store, must-revalidate"
    assert response.headers["Pragma"] == "no-cache"
    assert response.headers["Expires"] == "0"


def test_whale_alert_db_uses_shared_database_service(monkeypatch) -> None:
    whale_mod = importlib.import_module("whale_alert_manager")

    class _FakeDB:
        def __init__(self):
            self.saved = []

        def ping(self):
            return True

        def save_whale_alert(self, alert):
            self.saved.append(alert)
            return True

        def get_recent_whale_alerts(self, hours=24):
            return [
                {"title": "small", "value_usd": 500_000, "symbol": "BTC", "source": "x"},
                {"title": "big", "value_usd": 2_000_000, "symbol": "ETH", "source": "x"},
            ]

    fake_db = _FakeDB()
    monkeypatch.setattr(whale_mod, "get_db", lambda: fake_db, raising=False)
    adapter = whale_mod.WhaleAlertDB()

    assert adapter.enabled is True
    assert adapter.save_alert({"title": "a", "value_usd": 2_000_000}) is True
    assert fake_db.saved[0]["title"] == "a"
    alerts = adapter.get_alerts(hours=24, min_value=1_000_000)
    assert len(alerts) == 1
    assert alerts[0]["title"] == "big"


def test_telegram_history_uses_database_side_filters(monkeypatch) -> None:
    import asyncio

    tg_mod = importlib.import_module("telegram_commander")
    captured: Dict[str, Any] = {}

    class _FakeDB:
        def get_recent_trades(self, limit=50, category="", pnl_filter="all"):
            captured.update({
                "limit": limit,
                "category": category,
                "pnl_filter": pnl_filter,
            })
            return []

    async def _fake_send(*args, **kwargs):
        return None

    _patch_db(monkeypatch, _FakeDB())
    asyncio.run(
        tg_mod.TelegramCommander._show_history(
            SimpleNamespace(),
            _fake_send,
            filter_cat="won",
        )
    )

    assert captured == {
        "limit": 30,
        "category": "",
        "pnl_filter": "won",
    }


def test_state_rebuild_stats_uses_shared_db_rollups(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(state_mod, "_STATE_FILE", tmp_path / "system_state.json")

    class _FakeDB:
        def get_closed_trade_rollups(self):
            return {
                "rows": [("trend", "EUR/USD", 12.0), ("trend", "EUR/USD", -4.0)],
                "strategy": {
                    "trend": {"wins": 1, "losses": 1, "pnl": 8.0},
                },
                "asset": {
                    "EUR/USD": {"wins": 1, "losses": 1, "pnl": 8.0},
                },
            }

    _patch_db(monkeypatch, _FakeDB())
    state = state_mod.SystemState()
    state._rebuild_stats_from_db()

    assert state.get_all_strategy_stats()["trend"]["total"] == 2
    assert state.get_all_strategy_stats()["trend"]["pnl"] == 8.0
    assert state.get_asset_win_rate("EUR/USD") == 0.5


def test_signal_reporter_stores_research_validation_summary(monkeypatch) -> None:
    reporter_mod = importlib.import_module("core.signal_reporter")

    captured: Dict[str, Any] = {}

    class _FakeDB:
        def save_strategy_performance_snapshot(self, **kwargs):
            captured.update(kwargs)

    _patch_db(monkeypatch, _FakeDB())
    reporter = reporter_mod.reporter
    original_db_ok = reporter._db_ok
    reporter._db_ok = True

    try:
        signal = Signal(
            asset="EUR/USD",
            direction="BUY",
            category="forex",
            confidence=0.81,
            strategy_id="trend",
            canonical_asset="EUR/USD",
        )
        signal.journal.record(
            layer=0,
            name="research_validation",
            decision=reporter_mod.INFO,
            reason="research approved",
            conf_before=signal.confidence,
            conf_after=signal.confidence,
            data={
                "walk_forward_accuracy": 0.61,
                "holdout_accuracy": 0.58,
                "live_validation_total": 12,
                "live_validation_accuracy_pct": 66.7,
            },
        )

        reporter._store_performance(signal)
    finally:
        reporter._db_ok = original_db_ok

    assert captured["asset"] == "EUR/USD"
    assert captured["category"] == "forex"
    assert captured["strategy_id"] == "trend"
    assert round(captured["win_rate"], 3) == 0.667
    assert captured["sharpe_ratio"] == 0.0
    assert captured["total_trades"] == 12


def test_init_db_uses_database_service_for_strategy_tables(monkeypatch) -> None:
    source = Path("config/database.py").read_text(encoding="utf-8")

    assert "from services.db_pool import get_db as get_database_service" in source
    assert "get_database_service().ensure_strategy_reporting_tables()" in source
    assert "from core.signal_reporter import _CREATE_STRATEGY_PERFORMANCE, _CREATE_STRATEGY_OPTIMISATION" not in source


def test_trade_and_personality_models_define_runtime_indexes() -> None:
    source = Path("models/trade_models.py").read_text(encoding="utf-8")

    assert 'Index("idx_trades_category_exit_time", "category", "exit_time")' in source
    assert 'Index("idx_trades_canonical_asset_exit_time", "canonical_asset", "exit_time")' in source
    assert 'Index("idx_trading_diary_asset_setup_date", "asset", "setup_type", "created_at")' in source
    assert 'Index("idx_trading_diary_asset_date", "asset", "created_at")' in source
    assert 'Index("idx_memorable_moments_date", "moment_date")' in source
