from __future__ import annotations

import asyncio
import copy
import hashlib
import importlib
import json
import lzma
import os
import socket
import struct
import sys
import tempfile
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import ModuleType, SimpleNamespace
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


def test_trading_core_fmt_ml_pair_hides_inactive_ml() -> None:
    assert TradingCore._fmt_ml_pair(0.5, 0.0) == "n/a"
    assert TradingCore._fmt_ml_pair(0.5, 0.1) == "n/a"
    assert TradingCore._fmt_ml_pair(None, 0.9) == "n/a"


def test_trading_core_fmt_ml_pair_formats_real_ml() -> None:
    assert TradingCore._fmt_ml_pair(0.7342, 0.421) == "0.734/0.421"

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

def test_reduce_weak_positions_partially_closes_and_keeps_parent_open() -> None:
    engine = TradingCore(balance=10_000.0)
    current = {
        "trade_id": "t1",
        "asset": "BTC-USD",
        "category": "crypto",
        "direction": "SELL",
        "entry_price": 100.0,
        "stop_loss": 104.0,
        "take_profit": 94.0,
        "position_size": 10.0,
        "confidence": 0.61,
        "pnl": -25.0,
        "metadata": {
            "opportunity_score": 0.31,
            "memory_score": 42.0,
            "memory_sample_count": 8,
            "execution_quality_score": 40.0,
            "execution_feedback_sample_count": 9,
        },
    }
    synced: list[dict] = []
    partials: list[dict] = []

    def _get_open_positions():
        return [dict(current)]

    def _get_open_position(trade_id):
        return dict(current) if trade_id == "t1" else None

    def _sync_open_position(snapshot):
        current.update(snapshot)
        synced.append(dict(snapshot))

    def _record_partial_close(parent_trade_id, partial_trade):
        partials.append(dict(partial_trade))
        return dict(partial_trade)

    engine.state = SimpleNamespace(
        get_open_positions=_get_open_positions,
        get_open_position=_get_open_position,
        sync_open_position=_sync_open_position,
        record_partial_close=_record_partial_close,
        balance=10_000.0,
    )
    engine.fetcher = SimpleNamespace(get_real_time_price=lambda asset, category: (95.0, 0.0))
    engine._paper_trader = SimpleNamespace(
        _lock=threading.RLock(),
        open_positions={"t1": dict(current)},
        on_trade_closed=None,
        _notify_position_updated=lambda pos: None,
    )
    engine._risk_manager = SimpleNamespace(update_balance=lambda balance: None)
    engine._notify_telegram_close = lambda trade: None

    actions = engine.reduce_weak_positions(limit=1, reduction_fraction=0.35)

    assert len(actions) == 1
    assert actions[0]["success"] is True
    assert round(actions[0]["remaining_size"], 4) == 6.5
    assert round(actions[0]["reduced_size"], 4) == 3.5
    assert round(current["position_size"], 4) == 6.5
    assert synced[-1]["position_size"] == 6.5
    assert partials[0]["is_partial_close"] is True
    assert partials[0]["parent_trade_id"] == "t1"
    assert round(partials[0]["position_size"], 4) == 3.5

def test_reprice_open_positions_uses_execution_feedback_policy(monkeypatch) -> None:
    engine = TradingCore(balance=10_000.0)
    position = {
        "trade_id": "t2",
        "asset": "BTC-USD",
        "category": "crypto",
        "direction": "SELL",
        "entry_price": 66000.0,
        "stop_loss": 66990.0,
        "original_sl": 66990.0,
        "take_profit": 64020.0,
        "metadata": {},
    }
    synced = []
    seen = {}

    engine.state = SimpleNamespace(
        get_open_positions=lambda: [dict(position)],
        sync_open_position=lambda pos: synced.append(dict(pos)),
    )
    engine.fetcher = MagicMock()
    engine._paper_trader = SimpleNamespace(_lock=threading.RLock(), open_positions={"t2": dict(position)})

    class _Risk:
        def get_stop_loss_scaled(self, entry, direction, category, atr=0.0, distance_multiplier=1.0):
            seen["stop_buffer_multiplier"] = distance_multiplier
            return 66750.0

        def get_take_profit(self, entry, stop_loss, direction, category="", rr_multiplier=1.0):
            seen["target_rr_multiplier"] = rr_multiplier
            return 64800.0

    class _Feedback:
        def get_exit_adjustment(self, asset, category, context=None):
            return {
                "stop_buffer_multiplier": 1.12,
                "target_rr_multiplier": 0.88,
                "avg_quality_score": 63.4,
                "sample_count": 21,
            }

    engine._risk_manager = _Risk()
    engine._fetch_price_data = MagicMock(
        return_value=pd.DataFrame(
            {
                "high": [66120, 66140, 66170, 66190, 66210, 66230, 66220, 66180],
                "low": [65940, 65920, 65910, 65890, 65880, 65870, 65890, 65910],
                "close": [66080, 66060, 66020, 65980, 65960, 65920, 65940, 65970],
            }
        )
    )

    feedback_mod = importlib.import_module("services.execution_feedback_service")
    monkeypatch.setattr(feedback_mod, "get_service", lambda: _Feedback(), raising=False)

    updates = engine.reprice_open_positions()

    assert len(updates) == 1
    assert round(seen["stop_buffer_multiplier"], 2) == 1.12
    assert round(seen["target_rr_multiplier"], 2) == 0.88
    assert synced[0]["metadata"]["execution_feedback_policy"]["sample_count"] == 21
    assert synced[0]["metadata"]["execution_quality_score"] == 63.4
    assert synced[0]["metadata"]["target_rr_multiplier"] == 0.88
    assert updates[0]["execution_quality_score"] == 63.4

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

def test_paper_trader_applies_realistic_entry_and_exit_costs() -> None:
    trader = PaperTrader(account_balance=10_000.0)

    trade = trader.execute_signal(
        {
            "asset": "BTC-USD",
            "category": "crypto",
            "direction": "BUY",
            "confidence": 0.72,
            "entry_price": 100.0,
            "stop_loss": 90.0,
            "take_profit": 120.0,
            "position_size": 2.0,
            "metadata": {
                "paper_execution_profile": {
                    "commission": 0.001,
                    "slippage": 0.002,
                    "spread_pct": 0.01,
                }
            },
        }
    )

    assert trade is not None
    assert round(trade["entry_price"], 4) == 100.7
    assert trade["metadata"]["paper_execution"]["fill_mode"] == "paper_realistic"

def test_paper_trader_execute_signal_records_utc_open_time() -> None:
    trader = PaperTrader(account_balance=10_000.0)

    trade = trader.execute_signal(
        {
            "asset": "BTC-USD",
            "category": "crypto",
            "direction": "BUY",
            "confidence": 0.72,
            "entry_price": 100.0,
            "stop_loss": 90.0,
            "take_profit": 120.0,
            "position_size": 1.0,
            "metadata": {},
        }
    )

    assert trade is not None
    assert str(trade["open_time"]).endswith("+00:00")

def test_paper_trader_stop_loss_uses_harsher_adverse_fill() -> None:
    trader = PaperTrader(account_balance=10_000.0)

    trade = trader.execute_signal(
        {
            "asset": "BTC-USD",
            "category": "crypto",
            "direction": "BUY",
            "confidence": 0.70,
            "entry_price": 100.0,
            "stop_loss": 90.0,
            "take_profit": 120.0,
            "position_size": 1.0,
            "metadata": {
                "paper_execution_profile": {
                    "commission": 0.001,
                    "slippage": 0.002,
                    "spread_pct": 0.01,
                }
            },
        }
    )

    assert trade is not None

    closed = trader._check_exit(trade, 89.0)

    assert closed is not None
    assert closed["exit_reason"] == "Stop Loss"
    assert closed["exit_price"] < 89.0
    assert (
        closed["metadata"]["paper_execution"]["exit_slippage_pct"]
        > closed["metadata"]["paper_execution"]["entry_slippage_pct"]
    )

def test_paper_trader_applies_playbook_trailing_after_one_r() -> None:
    trader = PaperTrader(account_balance=10_000.0)

    position = {
        "trade_id": "trail123",
        "asset": "EUR/USD",
        "category": "forex",
        "direction": "BUY",
        "entry_price": 100.0,
        "stop_loss": 90.0,
        "original_sl": 90.0,
        "take_profit": 125.0,
        "take_profit_levels": [],
        "position_size": 1.0,
        "open_time": datetime.utcnow().isoformat(),
        "highest_price": 100.0,
        "lowest_price": 100.0,
        "tp_hit": 0,
        "metadata": {
            "atr": 2.0,
            "trade_management_plan": {
                "trail_activation_rr": 1.0,
                "trail_atr_multiple": 0.75,
            }
        },
    }

    result = trader._check_exit(position, 111.0)

    assert result is None
    assert round(float(position["stop_loss"]), 2) == 102.50

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
    import services.db_pool as db_pool
    monkeypatch.setattr(db_pool, "get_db", lambda: fake_db)

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
        "strategy_id": "playbook_breakout_continuation",
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
        "strategy_id": "playbook_breakout_continuation",
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


def test_state_json_roundtrip_persists_closed_positions(monkeypatch, tmp_path: Path) -> None:
    class FakeDB:
        def get_recent_trades(self, limit):
            return []

        def load_open_positions(self):
            return []

        def get_closed_trade_rollups(self):
            return {"rows": [], "strategy": {}, "asset": {}}

    _patch_db(monkeypatch, FakeDB())

    temp_state_file = tmp_path / "system_state.json"
    monkeypatch.setattr(state_mod, "_STATE_FILE", temp_state_file)
    temp_state_file.parent.mkdir(parents=True, exist_ok=True)

    state = state_mod.SystemState()
    with state._lock:
        state._closed_positions = [
            {
                "trade_id": "closed-1",
                "asset": "XAU/USD",
                "exit_time": "2026-04-06T14:36:30+00:00",
                "exit_reason": "Stop Loss (offline)",
            }
        ]
    state.force_save()

    reloaded = state_mod.SystemState()
    closed = reloaded.get_closed_positions(limit=5)

    assert closed
    assert closed[0]["trade_id"] == "closed-1"


def test_load_positions_from_db_skips_rows_already_closed_in_json(monkeypatch, tmp_path: Path) -> None:
    class FakeDB:
        def __init__(self) -> None:
            self.deleted: list[str] = []

        def get_recent_trades(self, limit):
            return []

        def load_open_positions(self):
            return [
                {
                    "trade_id": "abc123",
                    "asset": "XAU/USD",
                    "canonical_asset": "XAU/USD",
                    "category": "commodities",
                }
            ]

        def delete_open_position(self, trade_id):
            self.deleted.append(trade_id)

        def get_closed_trade_rollups(self):
            return {"rows": [], "strategy": {}, "asset": {}}

    fake_db = FakeDB()
    _patch_db(monkeypatch, fake_db)
    import services.db_pool as db_pool
    monkeypatch.setattr(db_pool, "get_db", lambda: fake_db)

    temp_state_file = tmp_path / "system_state.json"
    monkeypatch.setattr(state_mod, "_STATE_FILE", temp_state_file)
    temp_state_file.parent.mkdir(parents=True, exist_ok=True)
    temp_state_file.write_text(
        json.dumps(
            {
                "schema_version": 4,
                "saved_at": "2026-04-06T17:36:30",
                "balance": 1000.0,
                "initial_balance": 1000.0,
                "daily_trades": 0,
                "daily_pnl": 0.0,
                "last_save_date": datetime.utcnow().date().isoformat(),
                "cooldowns": {},
                "strategy_stats": {},
                "session_stats": {},
                "asset_stats": {},
                "open_positions": [],
                "closed_positions": [
                    {
                        "trade_id": "abc123",
                        "asset": "XAU/USD",
                        "exit_time": "2026-04-06T14:36:30+00:00",
                        "exit_reason": "Stop Loss (offline)",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    state = state_mod.SystemState()
    state.init_db()

    assert state.get_open_position("abc123") is None
    assert fake_db.deleted == ["abc123"]

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

    monkeypatch.setattr(core_state_module, "state", fake_state, raising=False)
    monkeypatch.setattr(data_fetcher_module, "DataFetcher", DummyFetcher, raising=False)
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

def test_signal_governance_rejects_delayed_fallback_data(monkeypatch) -> None:
    governance_mod = importlib.import_module("services.signal_governance")
    governance = governance_mod.SignalGovernance()

    monkeypatch.setattr(
        governance_mod.SignalGovernance,
        "_get_live_validation",
        staticmethod(lambda asset: {"scope": "asset", "total": 40, "accuracy_pct": 61.0}),
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

def test_signal_governance_accepts_local_store_ohlcv_provenance() -> None:
    governance_mod = importlib.import_module("services.signal_governance")

    ok, reason = governance_mod.SignalGovernance._check_market_source(
        {"source": "LocalStore", "source_class": "local_store", "delayed": False},
        True,
        "ohlcv",
    )

    assert ok is True
    assert reason == ""
    assert governance_mod.SignalGovernance._source_score(
        {"source": "LocalStore", "source_class": "local_store", "delayed": False}
    ) == 84

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

def test_signal_governance_uses_aligned_policy_research_when_commodities_seed_metadata_missing(monkeypatch) -> None:
    governance_mod = importlib.import_module("services.signal_governance")
    governance = governance_mod.SignalGovernance()

    monkeypatch.setattr(
        governance_mod.SignalGovernance,
        "_get_live_validation",
        staticmethod(lambda asset: {"scope": "asset", "total": 32, "accuracy_pct": 60.0}),
    )
    monkeypatch.setattr(
        governance_mod.SignalGovernance,
        "_get_expectancy_validation",
        staticmethod(
            lambda asset, category: {
                "scope": "asset",
                "sample_count": 18,
                "avg_rr_realized": 0.24,
                "target_hit_rate": 0.44,
                "premature_stop_rate": 0.10,
                "avg_quality_score": 63.0,
            }
        ),
    )
    monkeypatch.setattr(
        governance_mod.registry,
        "get_metadata",
        lambda name: (
            {}
            if name == "commodities_classifier"
            else {
                "research_approved": False,
                "research_grade": "provisional",
                "research_status": "provisional",
                "holdout_accuracy": 0.5385,
                "holdout_threshold": 0.52,
                "walk_forward_accuracy": 0.5281,
                "walk_forward_threshold": 0.52,
                "walk_forward_samples": 604,
                "walk_forward_required_samples": 60,
            }
        ),
        raising=False,
    )

    signal = Signal(
        asset="XAG/USD",
        canonical_asset="XAG/USD",
        category="commodities",
        direction="BUY",
        confidence=0.84,
        entry_price=30.0,
        stop_loss=29.4,
        take_profit=31.2,
        risk_reward=2.0,
    )
    signal.metadata.update({
        "valid_sources_count": 4,
        "ml_confidence": 0.29,
        "seed_model": "commodities_classifier",
        "policy_model": "commodities_policy",
        "agent_policy_status": "research_unapproved",
        "agent_score": 0.582,
    })

    verdict = governance.evaluate(signal, {
        "market_data": {
            "price": {"source": "Deriv", "source_class": "primary_api", "delayed": False},
            "ohlcv": {"source": "Deriv", "source_class": "primary_api", "delayed": False},
        }
    })

    assert verdict["approved"] is True
    assert verdict["research_model_key"] == "commodities_policy"
    assert any("using aligned commodities_policy research metadata" in item for item in verdict["warnings"])

def test_signal_governance_keeps_block_when_policy_research_is_misaligned(monkeypatch) -> None:
    governance_mod = importlib.import_module("services.signal_governance")
    governance = governance_mod.SignalGovernance()

    monkeypatch.setattr(
        governance_mod.SignalGovernance,
        "_get_live_validation",
        staticmethod(lambda asset: {"scope": "asset", "total": 32, "accuracy_pct": 60.0}),
    )
    monkeypatch.setattr(
        governance_mod.SignalGovernance,
        "_get_expectancy_validation",
        staticmethod(
            lambda asset, category: {
                "scope": "asset",
                "sample_count": 18,
                "avg_rr_realized": 0.24,
                "target_hit_rate": 0.44,
                "premature_stop_rate": 0.10,
                "avg_quality_score": 63.0,
            }
        ),
    )
    monkeypatch.setattr(
        governance_mod.registry,
        "get_metadata",
        lambda name: (
            {}
            if name == "commodities_classifier"
            else {
                "research_approved": False,
                "research_grade": "provisional",
                "research_status": "provisional",
                "holdout_accuracy": 0.5385,
                "holdout_threshold": 0.52,
                "walk_forward_accuracy": 0.5281,
                "walk_forward_threshold": 0.52,
                "walk_forward_samples": 604,
                "walk_forward_required_samples": 60,
            }
        ),
        raising=False,
    )

    signal = Signal(
        asset="XAU/USD",
        canonical_asset="XAU/USD",
        category="commodities",
        direction="SELL",
        confidence=0.84,
        entry_price=2300.0,
        stop_loss=2320.0,
        take_profit=2260.0,
        risk_reward=2.0,
    )
    signal.metadata.update({
        "valid_sources_count": 4,
        "ml_confidence": 0.29,
        "seed_model": "commodities_classifier",
        "policy_model": "commodities_policy",
        "agent_policy_status": "research_unapproved",
        "agent_score": 0.757,
    })

    verdict = governance.evaluate(signal, {
        "market_data": {
            "price": {"source": "Deriv", "source_class": "primary_api", "delayed": False},
            "ohlcv": {"source": "Deriv", "source_class": "primary_api", "delayed": False},
        }
    })

    assert verdict["approved"] is False
    assert verdict["research_model_key"] == "commodities_classifier"
    assert any("commodities_classifier lacks approved walk-forward research" in item for item in verdict["violations"])

def test_signal_governance_allows_non_commodities_provisional_research_in_paper(monkeypatch) -> None:
    governance_mod = importlib.import_module("services.signal_governance")
    governance = governance_mod.SignalGovernance()

    monkeypatch.delenv("BOT_LIVE_RUNTIME", raising=False)
    monkeypatch.setattr(
        governance_mod.SignalGovernance,
        "_get_live_validation",
        staticmethod(lambda asset: {"scope": "asset", "total": 32, "accuracy_pct": 60.0}),
    )
    monkeypatch.setattr(
        governance_mod.SignalGovernance,
        "_get_expectancy_validation",
        staticmethod(
            lambda asset, category: {
                "scope": "asset",
                "sample_count": 18,
                "avg_rr_realized": 0.24,
                "target_hit_rate": 0.44,
                "premature_stop_rate": 0.10,
                "avg_quality_score": 63.0,
            }
        ),
    )
    monkeypatch.setattr(
        governance_mod.registry,
        "get_metadata",
        lambda name: {
            "research_approved": False,
            "research_grade": "provisional",
            "research_status": "provisional",
            "holdout_accuracy": 0.49,
            "holdout_threshold": 0.52,
            "walk_forward_accuracy": 0.48,
            "walk_forward_threshold": 0.52,
            "walk_forward_samples": 20,
            "walk_forward_required_samples": 60,
        },
        raising=False,
    )

    signal = Signal(
        asset="USD/JPY",
        canonical_asset="USD/JPY",
        category="forex",
        direction="BUY",
        confidence=0.84,
        entry_price=150.0,
        stop_loss=149.2,
        take_profit=151.4,
        risk_reward=1.75,
    )
    signal.metadata.update({
        "valid_sources_count": 4,
        "ml_confidence": 0.29,
        "policy_model": "forex_policy",
    })

    monkeypatch.setattr(
        governance_mod.SignalGovernance,
        "_run_forex_filter",
        staticmethod(lambda signal, context: (True, "ok")),
    )

    verdict = governance.evaluate(signal, {
        "market_data": {
            "price": {"source": "Deriv", "source_class": "primary_api", "delayed": False},
            "ohlcv": {"source": "Deriv", "source_class": "primary_api", "delayed": False},
        }
    })

    assert verdict["approved"] is True
    assert any("allowed in paper runtime with provisional research" in item for item in verdict["warnings"])

def test_signal_governance_rejects_same_provisional_model_in_live_runtime(monkeypatch) -> None:
    governance_mod = importlib.import_module("services.signal_governance")
    governance = governance_mod.SignalGovernance()

    monkeypatch.setenv("BOT_LIVE_RUNTIME", "1")
    monkeypatch.setattr(
        governance_mod.SignalGovernance,
        "_get_live_validation",
        staticmethod(lambda asset: {"scope": "asset", "total": 32, "accuracy_pct": 60.0}),
    )
    monkeypatch.setattr(
        governance_mod.SignalGovernance,
        "_get_expectancy_validation",
        staticmethod(
            lambda asset, category: {
                "scope": "asset",
                "sample_count": 18,
                "avg_rr_realized": 0.24,
                "target_hit_rate": 0.44,
                "premature_stop_rate": 0.10,
                "avg_quality_score": 63.0,
            }
        ),
    )
    monkeypatch.setattr(
        governance_mod.registry,
        "get_metadata",
        lambda name: {
            "research_approved": False,
            "research_grade": "provisional",
            "research_status": "provisional",
            "holdout_accuracy": 0.49,
            "holdout_threshold": 0.52,
            "walk_forward_accuracy": 0.48,
            "walk_forward_threshold": 0.52,
            "walk_forward_samples": 20,
            "walk_forward_required_samples": 60,
        },
        raising=False,
    )

    signal = Signal(
        asset="USD/JPY",
        canonical_asset="USD/JPY",
        category="forex",
        direction="BUY",
        confidence=0.84,
        entry_price=150.0,
        stop_loss=149.2,
        take_profit=151.4,
        risk_reward=1.75,
    )
    signal.metadata.update({
        "valid_sources_count": 4,
        "ml_confidence": 0.29,
        "policy_model": "forex_policy",
    })

    monkeypatch.setattr(
        governance_mod.SignalGovernance,
        "_run_forex_filter",
        staticmethod(lambda signal, context: (True, "ok")),
    )

    verdict = governance.evaluate(signal, {
        "market_data": {
            "price": {"source": "Deriv", "source_class": "primary_api", "delayed": False},
            "ohlcv": {"source": "Deriv", "source_class": "primary_api", "delayed": False},
        }
    })

    assert verdict["approved"] is False
    assert any("forex_policy lacks approved walk-forward research" in item for item in verdict["violations"])

def test_signal_governance_rejects_bootstrap_asset_with_poor_live_accuracy(monkeypatch) -> None:
    governance_mod = importlib.import_module("services.signal_governance")
    governance = governance_mod.SignalGovernance()

    monkeypatch.setattr(
        governance_mod.SignalGovernance,
        "_get_live_validation",
        staticmethod(lambda asset: {"scope": "asset", "total": 12, "accuracy_pct": 21.7}),
    )
    monkeypatch.setattr(
        governance_mod.registry,
        "get_metadata",
        lambda name: {
            "research_approved": True,
            "research_grade": "approved",
            "holdout_accuracy": 0.64,
            "walk_forward_accuracy": 0.58,
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

    assert verdict["approved"] is False
    assert any("bootstrap accuracy" in item for item in verdict["violations"])

def test_signal_governance_rejects_live_runtime_asset_without_registry_approval(monkeypatch) -> None:
    governance_mod = importlib.import_module("services.signal_governance")
    governance = governance_mod.SignalGovernance()

    monkeypatch.setenv("BOT_LIVE_RUNTIME", "1")
    monkeypatch.setattr(governance_mod, "PLAYBOOK_ONLY_RUNTIME", True, raising=False)
    monkeypatch.setattr(
        governance_mod.SignalGovernance,
        "_get_live_validation",
        staticmethod(lambda asset: {"scope": "asset", "total": 42, "accuracy_pct": 61.9}),
    )
    monkeypatch.setattr(
        governance_mod.SignalGovernance,
        "_get_expectancy_validation",
        staticmethod(
            lambda asset, category: {
                "scope": "asset",
                "sample_count": 14,
                "avg_rr_realized": 0.22,
                "target_hit_rate": 0.41,
                "premature_stop_rate": 0.11,
                "avg_quality_score": 58.0,
            }
        ),
    )
    signal = Signal(
        asset="ETH-USD",
        canonical_asset="ETH-USD",
        category="crypto",
        direction="BUY",
        confidence=0.82,
        entry_price=100.0,
        stop_loss=95.0,
        take_profit=110.0,
        risk_reward=2.0,
    )
    signal.metadata.update({
        "valid_sources_count": 4,
        "ml_confidence": 0.29,
        "policy_model": "crypto_policy",
    })

    verdict = governance.evaluate(signal, {
        "market_data": {
            "price": {"source": "DerivStream", "source_class": "stream", "delayed": False},
            "ohlcv": {"source": "Deriv", "source_class": "primary_api", "delayed": False},
        }
    })

    assert verdict["registry_validation"]["required"] is False
    assert verdict["registry_validation"]["match_scope"] == "playbook_only"

def test_signal_governance_bootstraps_empty_live_registry_in_live_runtime(monkeypatch) -> None:
    governance_mod = importlib.import_module("services.signal_governance")
    governance = governance_mod.SignalGovernance()

    monkeypatch.setenv("BOT_LIVE_RUNTIME", "1")
    monkeypatch.setattr(governance_mod, "PLAYBOOK_ONLY_RUNTIME", True, raising=False)
    monkeypatch.setattr(
        governance_mod.SignalGovernance,
        "_get_live_validation",
        staticmethod(lambda asset: {"scope": "asset", "total": 42, "accuracy_pct": 61.9}),
    )
    monkeypatch.setattr(
        governance_mod.SignalGovernance,
        "_get_expectancy_validation",
        staticmethod(
            lambda asset, category: {
                "scope": "asset",
                "sample_count": 14,
                "avg_rr_realized": 0.22,
                "target_hit_rate": 0.41,
                "premature_stop_rate": 0.11,
                "avg_quality_score": 58.0,
            }
        ),
    )
    monkeypatch.setattr(
        governance_mod.SignalGovernance,
        "_run_forex_filter",
        staticmethod(lambda signal, context: (True, "ok")),
    )

    signal = Signal(
        asset="USD/JPY",
        canonical_asset="USD/JPY",
        category="forex",
        direction="BUY",
        confidence=0.82,
        entry_price=100.0,
        stop_loss=95.0,
        take_profit=110.0,
        risk_reward=2.0,
    )
    signal.metadata.update({
        "valid_sources_count": 4,
        "ml_confidence": 0.29,
        "policy_model": "forex_policy",
    })

    verdict = governance.evaluate(signal, {
        "market_data": {
            "price": {"source": "DerivStream", "source_class": "stream", "delayed": False},
            "ohlcv": {"source": "Deriv", "source_class": "primary_api", "delayed": False},
        }
    })

    assert verdict["approved"] is True
    assert verdict["registry_validation"]["bootstrap_mode"] is False
    assert verdict["registry_validation"]["match_scope"] == "playbook_only"

def test_signal_governance_rejects_negative_expectancy_asset(monkeypatch) -> None:
    governance_mod = importlib.import_module("services.signal_governance")
    governance = governance_mod.SignalGovernance()

    monkeypatch.setattr(
        governance_mod.SignalGovernance,
        "_get_registry_validation",
        staticmethod(
            lambda asset, category: {
                "required": False,
                "asset_required": False,
                "asset": asset,
                "category": category,
                "matched": True,
                "exact_match": True,
                "match_scope": "asset",
                "names": ["approved_alpha"],
            }
        ),
    )
    monkeypatch.setattr(
        governance_mod.SignalGovernance,
        "_get_live_validation",
        staticmethod(lambda asset: {"scope": "asset", "total": 33, "accuracy_pct": 57.6}),
    )
    monkeypatch.setattr(
        governance_mod.SignalGovernance,
        "_get_expectancy_validation",
        staticmethod(
            lambda asset, category: {
                "scope": "asset",
                "sample_count": 12,
                "avg_rr_realized": -0.18,
                "target_hit_rate": 0.19,
                "premature_stop_rate": 0.53,
                "avg_quality_score": 37.0,
            }
        ),
    )
    monkeypatch.setattr(
        governance_mod.registry,
        "get_metadata",
        lambda name: {"research_approved": True, "walk_forward_accuracy": 0.58},
        raising=False,
    )

    signal = Signal(
        asset="SOL-USD",
        canonical_asset="SOL-USD",
        category="crypto",
        direction="BUY",
        confidence=0.81,
        entry_price=100.0,
        stop_loss=94.0,
        take_profit=112.0,
        risk_reward=2.0,
    )
    signal.metadata.update({
        "valid_sources_count": 4,
        "ml_confidence": 0.30,
        "policy_model": "crypto_policy",
    })

    verdict = governance.evaluate(signal, {
        "market_data": {
            "price": {"source": "DerivStream", "source_class": "stream", "delayed": False},
            "ohlcv": {"source": "Deriv", "source_class": "primary_api", "delayed": False},
        }
    })

    assert verdict["approved"] is False
    assert any("live asset expectancy -0.18R below minimum -0.05R" in item for item in verdict["violations"])

def test_signal_governance_relaxes_crypto_live_validation_and_expectancy(monkeypatch) -> None:
    governance_mod = importlib.import_module("services.signal_governance")
    governance = governance_mod.SignalGovernance()

    monkeypatch.setattr(
        governance_mod.SignalGovernance,
        "_get_registry_validation",
        staticmethod(
            lambda asset, category: {
                "required": False,
                "asset_required": False,
                "asset": asset,
                "category": category,
                "matched": True,
                "exact_match": True,
                "match_scope": "asset",
                "names": ["approved_alpha"],
            }
        ),
    )
    monkeypatch.setattr(
        governance_mod.SignalGovernance,
        "_get_live_validation",
        staticmethod(lambda asset: {"scope": "asset", "total": 33, "accuracy_pct": 45.2}),
    )
    monkeypatch.setattr(
        governance_mod.SignalGovernance,
        "_get_expectancy_validation",
        staticmethod(
            lambda asset, category: {
                "scope": "asset",
                "sample_count": 12,
                "avg_rr_realized": -0.01,
                "target_hit_rate": 0.28,
                "premature_stop_rate": 0.31,
                "avg_quality_score": 48.0,
            }
        ),
    )
    monkeypatch.setattr(
        governance_mod.registry,
        "get_metadata",
        lambda name: {"research_approved": True, "walk_forward_accuracy": 0.58},
        raising=False,
    )

    signal = Signal(
        asset="XRP-USD",
        canonical_asset="XRP-USD",
        category="crypto",
        direction="BUY",
        confidence=0.81,
        entry_price=100.0,
        stop_loss=95.0,
        take_profit=110.0,
        risk_reward=1.4,
    )
    signal.metadata.update({
        "valid_sources_count": 4,
        "ml_confidence": 0.18,
        "policy_model": "crypto_policy",
    })

    verdict = governance.evaluate(signal, {
        "market_data": {
            "price": {"source": "Binance", "source_class": "secondary_api", "delayed": False, "realtime": True},
            "ohlcv": {"source": "Binance", "source_class": "secondary_api", "delayed": False, "realtime": False},
        }
    })

    assert verdict["approved"] is True
    assert verdict["min_risk_reward"] == 1.2

def test_signal_governance_allows_aligned_provisional_commodities_policy_in_paper(monkeypatch) -> None:
    governance_mod = importlib.import_module("services.signal_governance")
    governance = governance_mod.SignalGovernance()

    monkeypatch.delenv("BOT_LIVE_RUNTIME", raising=False)
    monkeypatch.setattr(
        governance_mod.SignalGovernance,
        "_get_live_validation",
        staticmethod(lambda asset: {"scope": "asset", "total": 32, "accuracy_pct": 52.0}),
    )
    monkeypatch.setattr(
        governance_mod.SignalGovernance,
        "_get_expectancy_validation",
        staticmethod(
            lambda asset, category: {
                "scope": "asset",
                "sample_count": 18,
                "avg_rr_realized": 0.06,
                "target_hit_rate": 0.34,
                "premature_stop_rate": 0.21,
                "avg_quality_score": 58.0,
            }
        ),
    )
    monkeypatch.setattr(
        governance_mod.registry,
        "get_metadata",
        lambda name: (
            {}
            if name == "commodities_classifier"
            else {
                "research_approved": False,
                "research_grade": "provisional",
                "research_status": "provisional",
                "holdout_accuracy": 0.5385,
                "holdout_threshold": 0.52,
                "walk_forward_accuracy": 0.5281,
                "walk_forward_threshold": 0.52,
                "walk_forward_samples": 604,
                "walk_forward_required_samples": 60,
            }
        ),
        raising=False,
    )

    signal = Signal(
        asset="XAG/USD",
        canonical_asset="XAG/USD",
        category="commodities",
        direction="BUY",
        confidence=0.82,
        entry_price=30.0,
        stop_loss=29.2,
        take_profit=31.2,
        risk_reward=1.25,
    )
    signal.metadata.update({
        "valid_sources_count": 4,
        "ml_confidence": 0.18,
        "seed_model": "commodities_classifier",
        "policy_model": "commodities_policy",
        "agent_policy_status": "research_unapproved",
        "agent_score": 0.62,
    })

    verdict = governance.evaluate(signal, {
        "market_data": {
            "price": {"source": "IG", "source_class": "primary_api", "delayed": False},
            "ohlcv": {"source": "IG", "source_class": "primary_api", "delayed": False},
        }
    })

    assert verdict["approved"] is True
    assert verdict["research_model_key"] == "commodities_policy"
    assert any("provisional commodity research" in item for item in verdict["warnings"])

def test_signal_governance_relaxes_forex_risk_reward_floor(monkeypatch) -> None:
    governance_mod = importlib.import_module("services.signal_governance")
    governance = governance_mod.SignalGovernance()

    monkeypatch.setattr(
        governance_mod.SignalGovernance,
        "_get_live_validation",
        staticmethod(lambda asset: {"scope": "asset", "total": 32, "accuracy_pct": 60.0}),
    )
    monkeypatch.setattr(
        governance_mod.SignalGovernance,
        "_get_expectancy_validation",
        staticmethod(
            lambda asset, category: {
                "scope": "asset",
                "sample_count": 18,
                "avg_rr_realized": 0.18,
                "target_hit_rate": 0.41,
                "premature_stop_rate": 0.18,
                "avg_quality_score": 58.0,
            }
        ),
    )
    monkeypatch.setattr(
        governance_mod.SignalGovernance,
        "_run_forex_filter",
        staticmethod(lambda signal, context: (True, "ok")),
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
        confidence=0.82,
        entry_price=1.10,
        stop_loss=1.09,
        take_profit=1.112,
        risk_reward=1.2,
    )
    signal.metadata.update({
        "valid_sources_count": 4,
        "ml_confidence": 0.18,
        "policy_model": "forex_policy",
    })

    verdict = governance.evaluate(signal, {
        "market_data": {
            "price": {"source": "Deriv", "source_class": "primary_api", "delayed": False},
            "ohlcv": {"source": "Deriv", "source_class": "primary_api", "delayed": False},
        }
    })

    assert verdict["approved"] is True
    assert verdict["min_risk_reward"] == 0.75

def test_signal_governance_accepts_strong_playbook_seed_when_ml_is_weak(monkeypatch) -> None:
    governance_mod = importlib.import_module("services.signal_governance")
    governance = governance_mod.SignalGovernance()

    monkeypatch.delenv("BOT_LIVE_RUNTIME", raising=False)
    monkeypatch.setattr(
        governance_mod.SignalGovernance,
        "_get_live_validation",
        staticmethod(lambda asset: {"scope": "asset", "total": 31, "accuracy_pct": 58.0}),
    )
    monkeypatch.setattr(
        governance_mod.SignalGovernance,
        "_get_expectancy_validation",
        staticmethod(
            lambda asset, category: {
                "scope": "asset",
                "sample_count": 15,
                "avg_rr_realized": 0.17,
                "target_hit_rate": 0.39,
                "premature_stop_rate": 0.16,
                "avg_quality_score": 57.0,
            }
        ),
    )
    monkeypatch.setattr(
        governance_mod.SignalGovernance,
        "_run_forex_filter",
        staticmethod(lambda signal, context: (True, "ok")),
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
        confidence=0.76,
        entry_price=1.1520,
        stop_loss=1.1508,
        take_profit=1.1540,
        risk_reward=1.67,
        strategy_id="playbook_breakout_continuation",
    )
    signal.metadata.update({
        "valid_sources_count": 4,
        "ml_confidence": 0.05,
        "seed_source": "playbook",
        "seed_model": "breakout_continuation",
        "playbook_action": "seed",
        "playbook_name": "breakout_continuation",
        "playbook_confidence": 0.72,
        "policy_model": "forex_policy",
    })

    verdict = governance.evaluate(signal, {
        "market_data": {
            "price": {"source": "Deriv", "source_class": "primary_api", "delayed": False},
            "ohlcv": {"source": "Deriv", "source_class": "primary_api", "delayed": False},
        }
    })

    assert verdict["approved"] is True
    assert verdict["effective_seed_confidence"] == 0.72

def test_signal_governance_relaxes_indices_risk_reward_floor(monkeypatch) -> None:
    governance_mod = importlib.import_module("services.signal_governance")
    governance = governance_mod.SignalGovernance()

    monkeypatch.setattr(
        governance_mod.SignalGovernance,
        "_get_live_validation",
        staticmethod(lambda asset: {"scope": "bootstrap", "total": 0, "accuracy_pct": 0.0}),
    )
    monkeypatch.setattr(
        governance_mod.SignalGovernance,
        "_get_expectancy_validation",
        staticmethod(lambda asset, category: {"scope": "bootstrap", "sample_count": 0}),
    )
    monkeypatch.setattr(
        governance_mod.registry,
        "get_metadata",
        lambda name: {
            "research_approved": False,
            "research_grade": "provisional",
            "research_status": "provisional",
            "holdout_accuracy": 0.6463,
            "holdout_threshold": 0.52,
            "walk_forward_accuracy": 0.5234,
            "walk_forward_threshold": 0.52,
            "walk_forward_samples": 470,
            "walk_forward_required_samples": 60,
        },
        raising=False,
    )

    signal = Signal(
        asset="US500",
        canonical_asset="US500",
        category="indices",
        direction="BUY",
        confidence=0.80,
        entry_price=5000.0,
        stop_loss=4970.0,
        take_profit=5039.0,
        risk_reward=1.3,
    )
    signal.metadata.update({
        "valid_sources_count": 4,
        "ml_confidence": 0.16,
        "policy_model": "indices_policy",
    })

    verdict = governance.evaluate(signal, {
        "market_data": {
            "price": {"source": "Deriv", "source_class": "primary_api", "delayed": False},
            "ohlcv": {"source": "Deriv", "source_class": "primary_api", "delayed": False},
        }
    })

    assert verdict["approved"] is True
    assert verdict["min_risk_reward"] == 1.0

def test_signal_governance_relaxes_crypto_live_accuracy_in_paper(monkeypatch) -> None:
    governance_mod = importlib.import_module("services.signal_governance")
    governance = governance_mod.SignalGovernance()

    monkeypatch.delenv("BOT_LIVE_RUNTIME", raising=False)
    monkeypatch.setattr(
        governance_mod.SignalGovernance,
        "_get_live_validation",
        staticmethod(lambda asset: {"scope": "asset", "total": 35, "accuracy_pct": 28.6}),
    )
    monkeypatch.setattr(
        governance_mod.SignalGovernance,
        "_get_expectancy_validation",
        staticmethod(lambda asset, category: {"scope": "bootstrap", "sample_count": 0}),
    )
    monkeypatch.setattr(
        governance_mod.SignalGovernance,
        "_get_registry_validation",
        staticmethod(
            lambda asset, category: {
                "required": False,
                "asset_required": False,
                "bootstrap_mode": False,
                "asset": asset,
                "category": category,
                "matched": True,
                "exact_match": True,
                "match_scope": "asset",
                "strategies": [{"name": "approved_crypto"}],
                "names": ["approved_crypto"],
            }
        ),
    )
    monkeypatch.setattr(
        governance_mod.registry,
        "get_metadata",
        lambda name: {"research_approved": True, "walk_forward_accuracy": 0.58},
        raising=False,
    )

    signal = Signal(
        asset="ETH-USD",
        canonical_asset="ETH-USD",
        category="crypto",
        direction="BUY",
        confidence=0.82,
        entry_price=100.0,
        stop_loss=95.0,
        take_profit=112.0,
        risk_reward=1.2,
    )
    signal.metadata.update({
        "valid_sources_count": 4,
        "ml_confidence": 0.18,
        "policy_model": "crypto_policy",
    })

    verdict = governance.evaluate(signal, {
        "market_data": {
            "price": {"source": "Binance", "source_class": "secondary_api", "delayed": False, "realtime": True},
            "ohlcv": {"source": "Binance", "source_class": "secondary_api", "delayed": False},
        }
    })

    assert verdict["approved"] is True
    assert verdict["min_risk_reward"] == 1.2

def test_signal_governance_portfolio_context_is_warning_only_without_asset_samples(monkeypatch) -> None:
    governance_mod = importlib.import_module("services.signal_governance")
    governance = governance_mod.SignalGovernance()

    monkeypatch.setattr(
        governance_mod.SignalGovernance,
        "_get_registry_validation",
        staticmethod(
            lambda asset, category: {
                "required": False,
                "asset_required": False,
                "asset": asset,
                "category": category,
                "matched": True,
                "exact_match": True,
                "match_scope": "asset",
                "names": ["approved_alpha"],
            }
        ),
    )
    monkeypatch.setattr(
        governance_mod.SignalGovernance,
        "_get_live_validation",
        staticmethod(
            lambda asset: {
                "scope": "portfolio_context",
                "total": 0,
                "accuracy_pct": 0.0,
                "portfolio_total": 139,
                "portfolio_accuracy_pct": 43.2,
            }
        ),
    )
    monkeypatch.setattr(
        governance_mod.SignalGovernance,
        "_get_expectancy_validation",
        staticmethod(lambda asset, category: {"scope": "bootstrap", "sample_count": 0}),
    )
    monkeypatch.setattr(
        governance_mod.registry,
        "get_metadata",
        lambda name: {"research_approved": True, "walk_forward_accuracy": 0.58},
        raising=False,
    )

    signal = Signal(
        asset="XAU/USD",
        canonical_asset="XAU/USD",
        category="commodities",
        direction="SELL",
        confidence=0.80,
        entry_price=2000.0,
        stop_loss=2015.0,
        take_profit=1970.0,
        risk_reward=2.0,
    )
    signal.metadata.update({
        "valid_sources_count": 4,
        "ml_confidence": 0.28,
        "policy_model": "commodities_policy",
    })

    verdict = governance.evaluate(signal, {
        "market_data": {
            "price": {"source": "Deriv", "source_class": "primary_api", "delayed": False},
            "ohlcv": {"source": "Deriv", "source_class": "primary_api", "delayed": False},
        }
    })

    assert verdict["approved"] is True
    assert any("no asset samples yet" in item for item in verdict["warnings"])

def test_signal_governance_warns_on_soft_portfolio_drawdown_without_veto(monkeypatch) -> None:
    governance_mod = importlib.import_module("services.signal_governance")
    governance = governance_mod.SignalGovernance()

    monkeypatch.setattr(
        governance_mod.SignalGovernance,
        "_get_live_validation",
        staticmethod(lambda asset: {"scope": "portfolio", "total": 139, "accuracy_pct": 43.2}),
    )
    monkeypatch.setattr(
        governance_mod.SignalGovernance,
        "_run_forex_filter",
        staticmethod(lambda signal, context: (True, "ok")),
    )
    monkeypatch.setattr(
        governance_mod.registry,
        "get_metadata",
        lambda name: {"research_approved": True, "research_grade": "approved", "walk_forward_accuracy": 0.58},
        raising=False,
    )

    signal = Signal(
        asset="EUR/USD",
        canonical_asset="EUR/USD",
        category="forex",
        direction="BUY",
        confidence=0.84,
        entry_price=1.10,
        stop_loss=1.09,
        take_profit=1.13,
        risk_reward=2.0,
    )
    signal.metadata.update({
        "valid_sources_count": 4,
        "ml_confidence": 0.28,
        "policy_model": "forex_policy",
    })

    verdict = governance.evaluate(signal, {
        "market_data": {
            "price": {"source": "Deriv", "source_class": "primary_api", "delayed": False},
            "ohlcv": {"source": "Deriv", "source_class": "primary_api", "delayed": False},
        }
    })

    assert verdict["approved"] is True
    assert any("live portfolio accuracy 43.2% below preferred 54.0%" in item for item in verdict["warnings"])

def test_signal_governance_rejects_severely_weak_portfolio_accuracy(monkeypatch) -> None:
    governance_mod = importlib.import_module("services.signal_governance")
    governance = governance_mod.SignalGovernance()

    monkeypatch.setattr(
        governance_mod.SignalGovernance,
        "_get_live_validation",
        staticmethod(lambda asset: {"scope": "portfolio", "total": 139, "accuracy_pct": 36.4}),
    )
    monkeypatch.setattr(
        governance_mod.SignalGovernance,
        "_run_forex_filter",
        staticmethod(lambda signal, context: (True, "ok")),
    )
    monkeypatch.setattr(
        governance_mod.registry,
        "get_metadata",
        lambda name: {"research_approved": True, "research_grade": "approved", "walk_forward_accuracy": 0.58},
        raising=False,
    )

    signal = Signal(
        asset="XAU/USD",
        canonical_asset="XAU/USD",
        category="commodities",
        direction="SELL",
        confidence=0.80,
        entry_price=2000.0,
        stop_loss=2015.0,
        take_profit=1970.0,
        risk_reward=2.0,
    )
    signal.metadata.update({
        "valid_sources_count": 4,
        "ml_confidence": 0.28,
        "policy_model": "commodities_policy",
    })

    verdict = governance.evaluate(signal, {
        "market_data": {
            "price": {"source": "Deriv", "source_class": "primary_api", "delayed": False},
            "ohlcv": {"source": "Deriv", "source_class": "primary_api", "delayed": False},
        }
    })

    assert verdict["approved"] is False
    assert any("live portfolio accuracy 36.4% below minimum 40.0%" in item for item in verdict["violations"])

def test_forex_filter_uses_bootstrap_confidence_floor() -> None:
    forex_mod = importlib.import_module("risk.forex_filter")

    df = pd.DataFrame({"close": np.linspace(1.0, 1.1, 40)})
    passed, reason = forex_mod.ForexFilter.should_trade_forex_signal(
        asset="EUR/USD",
        signal_confidence=0.52,
        df=df,
        atr=0.0012,
        current_spread_bps=0.4,
        live_validation_scope="portfolio",
    )

    assert passed is True
    assert reason == "PASSED"

def test_forex_filter_keeps_strict_asset_confidence_floor() -> None:
    forex_mod = importlib.import_module("risk.forex_filter")

    df = pd.DataFrame({"close": np.linspace(1.0, 1.1, 40)})
    passed, reason = forex_mod.ForexFilter.should_trade_forex_signal(
        asset="EUR/USD",
        signal_confidence=0.52,
        df=df,
        atr=0.0012,
        current_spread_bps=0.4,
        live_validation_scope="asset",
    )

    assert passed is False
    assert "confidence 0.52 < 0.65" in reason

def test_forex_filter_allows_slightly_wider_bootstrap_spread() -> None:
    forex_mod = importlib.import_module("risk.forex_filter")

    df = pd.DataFrame({"close": np.linspace(1.0, 1.1, 40)})
    passed, reason = forex_mod.ForexFilter.should_trade_forex_signal(
        asset="EUR/USD",
        signal_confidence=0.58,
        df=df,
        atr=0.0012,
        current_spread_bps=1.7,
        live_validation_scope="portfolio",
    )

    assert passed is True
    assert reason == "PASSED"

def test_forex_filter_uses_price_relative_atr_floor_for_major_pairs() -> None:
    forex_mod = importlib.import_module("risk.forex_filter")

    df = pd.DataFrame({"close": np.linspace(1.20, 1.35, 40)})
    passed, reason = forex_mod.ForexFilter.should_trade_forex_signal(
        asset="USD/CAD",
        signal_confidence=0.66,
        df=df,
        atr=0.00021,
        current_spread_bps=0.6,
        live_validation_scope="asset",
    )

    assert passed is True
    assert reason == "PASSED"

def test_forex_filter_still_rejects_truly_dead_atr() -> None:
    forex_mod = importlib.import_module("risk.forex_filter")

    df = pd.DataFrame({"close": np.linspace(1.05, 1.10, 40)})
    passed, reason = forex_mod.ForexFilter.should_trade_forex_signal(
        asset="EUR/USD",
        signal_confidence=0.70,
        df=df,
        atr=0.00005,
        current_spread_bps=0.4,
        live_validation_scope="asset",
    )

    assert passed is False
    assert "ATR 0.00005 < min" in reason

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

def test_signal_governance_uses_adaptive_min_rr_preview(monkeypatch) -> None:
    governance_mod = importlib.import_module("services.signal_governance")
    governance = governance_mod.SignalGovernance()

    monkeypatch.setattr(
        governance_mod.SignalGovernance,
        "_get_live_validation",
        staticmethod(lambda asset: {"scope": "asset", "total": 30, "accuracy_pct": 55.0}),
    )
    monkeypatch.setattr(
        governance_mod.SignalGovernance,
        "_get_registry_validation",
        staticmethod(
            lambda asset, category: {
                "required": False,
                "asset_required": False,
                "bootstrap_mode": False,
                "asset": asset,
                "category": category,
                "matched": True,
                "exact_match": True,
                "match_scope": "asset",
                "strategies": [{"name": "approved_index"}],
                "names": ["approved_index"],
            }
        ),
    )
    monkeypatch.setattr(
        governance_mod.SignalGovernance,
        "_get_expectancy_validation",
        staticmethod(lambda asset, category: {"scope": "bootstrap", "sample_count": 0}),
    )
    monkeypatch.setattr(
        governance_mod.registry,
        "get_metadata",
        lambda name: {"research_approved": True, "walk_forward_accuracy": 0.58},
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
        take_profit=5072.5,
        risk_reward=1.45,
    )
    signal.metadata.update({
        "valid_sources_count": 3,
        "ml_confidence": 0.82,
        "policy_model": "indices_policy",
    })

    verdict = governance.evaluate(
        signal,
        {
            "adaptive_policy": {"min_rr": 1.42},
            "market_data": {
                "price": {"source": "Deriv", "source_class": "primary_api", "delayed": False},
                "ohlcv": {"source": "Deriv", "source_class": "primary_api", "delayed": False},
            },
        },
    )

    assert verdict["approved"] is True
    assert verdict["min_risk_reward"] == 1.42

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

def test_signal_governance_uses_seed_model_when_policy_is_bypassed(monkeypatch) -> None:
    governance_mod = importlib.import_module("services.signal_governance")
    governance = governance_mod.SignalGovernance()

    monkeypatch.setattr(
        governance_mod.SignalGovernance,
        "_get_registry_validation",
        staticmethod(
            lambda asset, category: {
                "required": False,
                "asset_required": False,
                "bootstrap_mode": False,
                "asset": asset,
                "category": category,
                "matched": True,
                "exact_match": True,
                "match_scope": "asset",
                "strategies": [{"name": "approved_fx"}],
                "names": ["approved_fx"],
            }
        ),
    )
    monkeypatch.setattr(
        governance_mod.SignalGovernance,
        "_get_live_validation",
        staticmethod(lambda asset: {"scope": "asset", "total": 42, "accuracy_pct": 61.9}),
    )
    monkeypatch.setattr(
        governance_mod.SignalGovernance,
        "_get_expectancy_validation",
        staticmethod(
            lambda asset, category: {
                "scope": "asset",
                "sample_count": 14,
                "avg_rr_realized": 0.22,
                "target_hit_rate": 0.41,
                "premature_stop_rate": 0.11,
                "avg_quality_score": 58.0,
            }
        ),
    )
    monkeypatch.setattr(
        governance_mod.SignalGovernance,
        "_run_forex_filter",
        staticmethod(lambda signal, context: (True, "ok")),
    )
    monkeypatch.setattr(
        governance_mod.registry,
        "get_metadata",
        lambda name: (
            {
                "research_approved": False,
                "research_status": "unapproved",
                "walk_forward_accuracy": 0.40,
                "walk_forward_samples": 20,
                "holdout_accuracy": 0.49,
            }
            if name == "forex_policy"
            else {
                "research_approved": True,
                "research_status": "approved",
                "walk_forward_accuracy": 0.58,
                "walk_forward_samples": 480,
                "holdout_accuracy": 0.56,
            }
        ),
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
        "valid_sources_count": 4,
        "ml_confidence": 0.29,
        "policy_model": "forex_policy",
        "seed_model": "forex_classifier",
        "agent_policy_status": "research_unapproved",
    })

    verdict = governance.evaluate(signal, {
        "market_data": {
            "price": {"source": "Deriv", "source_class": "primary_api", "delayed": False},
            "ohlcv": {"source": "Deriv", "source_class": "primary_api", "delayed": False},
        }
    })

    assert verdict["approved"] is True
    assert verdict["model_key"] == "forex_classifier"

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
    assert signal.confidence == 0.70
    assert any("whale_support" in item for item in payload["adjustments"])

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

def test_deriv_bridge_disables_unsupported_economic_calendar(monkeypatch) -> None:
    deriv_mod = importlib.import_module("services.deriv_bridge")
    bridge = deriv_mod.DerivBridge()
    bridge._enabled = True

    calls = {"count": 0}

    def _fake_request(_payload):
        calls["count"] += 1
        raise deriv_mod.DerivUnsupportedRequestError("UnrecognisedRequest: Unrecognised request")

    monkeypatch.setattr(bridge, "_ensure_session_locked", lambda: True, raising=False)
    monkeypatch.setattr(bridge, "_request_locked", _fake_request, raising=False)

    first = bridge.get_economic_events(
        start_time="2026-03-31T00:00:00+00:00",
        end_time="2026-04-01T00:00:00+00:00",
        currencies=["USD"],
        impacts=["HIGH"],
    )
    second = bridge.get_economic_events(
        start_time="2026-03-31T00:00:00+00:00",
        end_time="2026-04-01T00:00:00+00:00",
        currencies=["USD"],
        impacts=["HIGH"],
    )

    assert first == []
    assert second == []
    assert calls["count"] == 1
    assert bridge._economic_calendar_supported is False

def test_deriv_bridge_economic_calendar_uses_currency_payload_and_epoch(monkeypatch) -> None:
    deriv_mod = importlib.import_module("services.deriv_bridge")
    bridge = deriv_mod.DerivBridge()
    bridge._enabled = True

    payloads = []

    def _fake_request(payload):
        payloads.append(dict(payload))
        if payload["economic_calendar"] == "USD":
            return {
                "economic_calendar": [
                    {
                        "epoch": "2026-03-31T12:00:00+00:00",
                        "currency": "USD",
                        "impact": "high",
                        "description": "US CPI",
                        "forecast": "3.2",
                        "actual": "3.4",
                    }
                ]
            }
        return {
            "economic_calendar": [
                {
                    "epoch": "2026-03-31T13:00:00+00:00",
                    "currency": "EUR",
                    "impact": "medium",
                    "description": "ECB Remarks",
                    "forecast": "",
                    "actual": "",
                }
            ]
        }

    monkeypatch.setattr(bridge, "_ensure_session_locked", lambda: True, raising=False)
    monkeypatch.setattr(bridge, "_request_locked", _fake_request, raising=False)

    rows = bridge.get_economic_events(
        start_time="2026-03-31T00:00:00+00:00",
        end_time="2026-04-01T00:00:00+00:00",
        currencies=["USD", "EUR"],
        impacts=["HIGH", "MEDIUM"],
    )

    assert payloads == [{"economic_calendar": "EUR"}, {"economic_calendar": "USD"}]
    assert len(rows) == 2
    assert rows[0]["currency"] == "USD" or rows[0]["currency"] == "EUR"
    assert {row["currency"] for row in rows} == {"USD", "EUR"}
    assert any(row["event"] == "US CPI" for row in rows)
    assert bridge._economic_calendar_supported is True

def test_deriv_bridge_default_symbol_map_includes_index_overrides() -> None:
    deriv_mod = importlib.import_module("services.deriv_bridge")
    bridge = deriv_mod.DerivBridge()

    overrides = bridge._parse_symbol_map("")

    assert overrides["US30"] == "OTC_DJI"
    assert overrides["US100"] == "OTC_NDX"
    assert overrides["US500"] == "OTC_SPC"
    assert overrides["UK100"] == "OTC_FTSE"

def test_economic_calendar_service_uses_forexfactory_fallback_when_deriv_is_unsupported(monkeypatch) -> None:
    calendar_mod = importlib.import_module("services.economic_calendar_service")
    service = calendar_mod.EconomicCalendarService()

    monkeypatch.setattr(calendar_mod.deriv_bridge, "get_economic_events", lambda *args, **kwargs: [], raising=False)
    monkeypatch.setattr(calendar_mod.deriv_bridge, "_economic_calendar_supported", False, raising=False)

    xml_payload = """<?xml version="1.0" encoding="windows-1252"?>
<weeklyevents>
  <event>
    <title>US CPI</title>
    <country>USD</country>
    <date><![CDATA[03-31-2026]]></date>
    <time><![CDATA[8:30am]]></time>
    <impact><![CDATA[High]]></impact>
    <forecast>3.2%</forecast>
    <previous>3.1%</previous>
  </event>
  <event>
    <title>BOJ Minutes</title>
    <country>JPY</country>
    <date><![CDATA[03-31-2026]]></date>
    <time><![CDATA[11:50pm]]></time>
    <impact><![CDATA[Medium]]></impact>
    <forecast></forecast>
    <previous></previous>
  </event>
</weeklyevents>
"""

    class _Response:
        status_code = 200
        encoding = "windows-1252"
        content = xml_payload.encode("windows-1252")

        def raise_for_status(self) -> None:
            return None

    monkeypatch.setattr(calendar_mod.requests, "get", lambda *args, **kwargs: _Response(), raising=False)

    rows = service.get_economic_events(
        start_time="2026-03-31T00:00:00+00:00",
        end_time="2026-04-01T00:00:00+00:00",
        currencies=["USD", "JPY"],
        impacts=["HIGH", "MEDIUM"],
    )

    assert {row["source"] for row in rows} == {"ForexFactory"}
    assert {row["currency"] for row in rows} == {"USD", "JPY"}
    assert any(row["event"] == "US CPI" for row in rows)

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

def test_deriv_bridge_default_symbol_map_does_not_force_wti() -> None:
    deriv_mod = importlib.import_module("services.deriv_bridge")

    assert "WTI" not in deriv_mod._DEFAULT_SYMBOL_OVERRIDES
    assert "WTI/USD" not in deriv_mod._DEFAULT_SYMBOL_OVERRIDES
    assert "CL=F" not in deriv_mod._DEFAULT_SYMBOL_OVERRIDES

def test_asset_registry_normalises_wti_aliases() -> None:
    from core.assets import registry

    assert registry.canonical("WTI") == "WTI"
    assert registry.canonical("WTI/USD") == "WTI"
    assert registry.canonical("CL=F") == "WTI"
    assert registry.category("WTI") == "commodities"

def test_asset_registry_includes_eurjpy_and_wti_in_active_universe() -> None:
    from core.assets import registry

    assets = dict(registry.all_assets())

    assert assets["EUR/JPY"] == "forex"
    assert assets["WTI"] == "commodities"
    assert len(assets) == 19

def test_api_backtest_strategies_reports_active_and_archived_strategy_sets(monkeypatch) -> None:
    dashboard_mod = importlib.import_module("dashboard.web_app_live")

    monkeypatch.setattr(dashboard_mod, "_DEVELOPMENT_MODE", True, raising=False)
    monkeypatch.setattr(dashboard_mod, "_AUTH_CONFIG_ERROR", "", raising=False)

    client = dashboard_mod.app.test_client()
    response = client.get("/api/backtest/strategies")
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["success"] is True
    assert payload["enabled"] is False
    assert payload["presets"] == []
    assert payload["archived_presets"] == []
    assert payload["playbooks"] == [
        "breakout_continuation",
        "breakout_retest",
        "trend_pullback",
        "reversal_exhaustion",
        "failed_break_reclaim",
        "aggressive_expansion",
    ]
    assert payload["live_runtime"] == ["playbook_only"]

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

def test_fetcher_records_deriv_wti_quote_metadata(monkeypatch) -> None:
    fetcher_mod = importlib.import_module("data.fetcher")
    ws_mod = importlib.import_module("websocket_dashboard")

    monkeypatch.setattr(fetcher_mod.DataFetcher, "_init_clients", lambda self: None, raising=False)
    monkeypatch.setattr(fetcher_mod.cache, "get", lambda key: None, raising=False)
    monkeypatch.setattr(fetcher_mod.cache, "set", lambda key, value, ttl=None: None, raising=False)
    monkeypatch.setattr(ws_mod, "get_live_price", lambda asset, max_age_seconds=10.0: (None, None), raising=False)

    fetcher = fetcher_mod.DataFetcher()
    fetcher._deriv_bridge = SimpleNamespace(
        get_quote=lambda asset, category="": (
            82.45,
            0.03,
            {"source": "Deriv", "source_class": "primary_api", "delayed": False, "realtime": True},
        ),
        resolve_symbol_info=lambda asset, category="": {"symbol": "frxUSOIL", "display_name": "WTI Oil"},
    )

    price, spread = fetcher.get_real_time_price("WTI", "commodities")
    meta = fetcher.get_last_price_metadata("WTI")

    assert price == 82.45
    assert spread == 0.03
    assert meta["source"] == "Deriv"
    assert meta["source_class"] == "primary_api"
    assert meta["deriv_symbol"] == "frxUSOIL"

def test_fetcher_prefers_ig_quote_for_routed_commodities(monkeypatch) -> None:
    fetcher_mod = importlib.import_module("data.fetcher")
    ws_mod = importlib.import_module("websocket_dashboard")

    monkeypatch.setattr(fetcher_mod.DataFetcher, "_init_clients", lambda self: None, raising=False)
    monkeypatch.setattr(fetcher_mod.cache, "get", lambda key: None, raising=False)
    monkeypatch.setattr(fetcher_mod.cache, "set", lambda key, value, ttl=None: None, raising=False)
    monkeypatch.setattr(ws_mod, "get_live_price", lambda asset, max_age_seconds=10.0: (None, None), raising=False)

    fetcher = fetcher_mod.DataFetcher()
    fetcher._ig_bridge = SimpleNamespace(
        get_quote=lambda asset, category="": (
            2345.6,
            0.4,
            {"source": "IG", "source_class": "primary_api", "delayed": False, "realtime": True},
        ),
        resolve_symbol_info=lambda asset, category="": {"symbol": "CS.D.GOLD.CFD.IP", "exchange": "ig"},
    )
    fetcher._deriv_bridge = SimpleNamespace(
        get_quote=lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("Deriv fallback should not run when IG succeeds")),
    )

    price, spread = fetcher.get_real_time_price("XAU/USD", "commodities")
    meta = fetcher.get_last_price_metadata("XAU/USD")

    assert price == 2345.6
    assert spread == 0.4
    assert meta["source"] == "IG"
    assert meta["source_class"] == "primary_api"
    assert meta["ig_epic"] == "CS.D.GOLD.CFD.IP"

def test_fetcher_prefers_shared_ig_live_price_cache_for_routed_commodities(monkeypatch) -> None:
    fetcher_mod = importlib.import_module("data.fetcher")
    ws_mod = importlib.import_module("websocket_dashboard")

    monkeypatch.setattr(fetcher_mod.DataFetcher, "_init_clients", lambda self: None, raising=False)
    monkeypatch.setattr(fetcher_mod.cache, "get", lambda key: None, raising=False)
    monkeypatch.setattr(fetcher_mod.cache, "set", lambda key, value, ttl=None: None, raising=False)
    monkeypatch.setattr(ws_mod, "get_live_price", lambda asset, max_age_seconds=15.0: (2500.75, "IG"), raising=False)

    fetcher = fetcher_mod.DataFetcher()
    fetcher._ig_bridge = SimpleNamespace(
        get_quote=lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("IG REST quote should not run when shared live cache is fresh")),
        resolve_symbol_info=lambda asset, category="": {"symbol": "CS.D.IN_GOLD.MFI.IP", "exchange": "ig", "instrument_name": "Spot Gold"},
    )
    fetcher._deriv_bridge = SimpleNamespace(
        get_quote=lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("Deriv fallback should not run when IG shared live cache is fresh")),
    )

    price, spread = fetcher.get_real_time_price("XAU/USD", "commodities")
    meta = fetcher.get_last_price_metadata("XAU/USD")

    assert price == 2500.75
    assert spread == 0.0
    assert meta["source"] == "IG"
    assert meta["source_class"] == "stream"
    assert meta["ig_epic"] == "CS.D.IN_GOLD.MFI.IP"

def test_fetcher_market_microstructure_prefers_true_orderflow_depth_for_crypto(monkeypatch) -> None:
    fetcher_mod = importlib.import_module("data.fetcher")

    monkeypatch.setattr(fetcher_mod.DataFetcher, "_init_clients", lambda self: None, raising=False)

    fetcher = fetcher_mod.DataFetcher()
    fetcher._deriv_bridge = SimpleNamespace(
        get_microstructure=lambda asset, category="": {
            "source": "Deriv",
            "score": 0.18,
            "tick_imbalance": 0.22,
            "book_imbalance": 0.0,
            "stop_hunt_risk": 0.12,
            "synthetic_depth_available": True,
            "depth_available": False,
            "microstructure_source": "live_store_synthetic_depth",
        }
    )
    fetcher._binance_bridge = None
    fetcher._ig_bridge = None

    monkeypatch.setitem(
        sys.modules,
        "order_flow",
        SimpleNamespace(
            get_snapshot=lambda symbol: {
                "imbalance": 0.64,
                "spread_pct": 0.01,
                "bid_vol": 120.0,
                "ask_vol": 40.0,
                "top_bids": [[100.0, 50.0], [99.9, 40.0]],
                "top_asks": [[100.1, 20.0], [100.2, 20.0]],
            }
        ),
    )

    micro = fetcher.get_market_microstructure("BTC-USD", "crypto")

    assert micro["depth_available"] is True
    assert micro["synthetic_depth_available"] is False
    assert micro["microstructure_source"] == "order_flow_true_depth"
    assert micro["book_imbalance"] == 0.64
    assert micro["depth_levels"] == 2
    assert micro["bid_vol"] == 120.0
    assert micro["ask_vol"] == 40.0

def test_fetcher_falls_back_to_deriv_quote_for_routed_commodities_when_ig_unavailable(monkeypatch) -> None:
    fetcher_mod = importlib.import_module("data.fetcher")
    ws_mod = importlib.import_module("websocket_dashboard")

    monkeypatch.setattr(fetcher_mod.DataFetcher, "_init_clients", lambda self: None, raising=False)
    monkeypatch.setattr(fetcher_mod.cache, "get", lambda key: None, raising=False)
    monkeypatch.setattr(fetcher_mod.cache, "set", lambda key, value, ttl=None: None, raising=False)
    monkeypatch.setattr(ws_mod, "get_live_price", lambda asset, max_age_seconds=10.0: (None, None), raising=False)

    fetcher = fetcher_mod.DataFetcher()
    fetcher._ig_bridge = SimpleNamespace(
        get_quote=lambda asset, category="": (
            None,
            None,
            {
                "source": "IG",
                "source_class": "primary_api",
                "provider_error_code": "missing_credentials",
                "provider_error_message": "IG_IDENTIFIER and IG_PASSWORD are required for IG commodity data.",
            },
        ),
        resolve_symbol_info=lambda asset, category="": {"symbol": "IX.D.XAU.CFD.IP", "exchange": "ig"},
    )
    fetcher._deriv_bridge = SimpleNamespace(
        get_quote=lambda asset, category="": (
            2498.2,
            0.3,
            {"source": "Deriv", "source_class": "primary_api", "delayed": False, "realtime": True},
        ),
        resolve_symbol_info=lambda asset, category="": {"symbol": "frxXAUUSD", "display_name": "Gold"},
    )

    price, spread = fetcher.get_real_time_price("XAU/USD", "commodities")
    meta = fetcher.get_last_price_metadata("XAU/USD")

    assert price == 2498.2
    assert spread == 0.3
    assert meta["source"] == "Deriv"
    assert meta["deriv_symbol"] == "frxXAUUSD"

def test_fetcher_uses_ig_ohlcv_for_routed_commodities(monkeypatch) -> None:
    fetcher_mod = importlib.import_module("data.fetcher")
    monkeypatch.setattr(fetcher_mod.DataFetcher, "_init_clients", lambda self: None, raising=False)
    monkeypatch.setattr(fetcher_mod.cache, "get", lambda key: None, raising=False)
    monkeypatch.setattr(fetcher_mod.cache, "set", lambda key, value, ttl=None: None, raising=False)

    fetcher = fetcher_mod.DataFetcher()
    fetcher._ig_bridge = SimpleNamespace(
        get_ohlcv=lambda asset, interval, periods, category="", end_time=None, closed_only=False: (
            pd.DataFrame({
                "open": [2320.0, 2324.5],
                "high": [2328.0, 2330.2],
                "low": [2318.8, 2323.3],
                "close": [2326.7, 2329.1],
                "volume": [0.0, 0.0],
            }),
            {"source": "IG", "source_class": "primary_api", "delayed": False, "realtime": False},
        ),
        resolve_symbol_info=lambda asset, category="": {"symbol": "CS.D.GOLD.CFD.IP", "exchange": "ig"},
    )

    df = fetcher.get_ohlcv("XAU/USD", "commodities", "15m", 2)
    meta = fetcher.get_last_ohlcv_metadata("XAU/USD", "15m")

    assert df is not None and not df.empty
    assert meta["source"] == "IG"
    assert meta["source_class"] == "primary_api"
    assert meta["ig_epic"] == "CS.D.GOLD.CFD.IP"

def test_fetcher_uses_fmp_ohlcv_before_deriv_for_forex(monkeypatch) -> None:
    fetcher_mod = importlib.import_module("data.fetcher")
    monkeypatch.setattr(fetcher_mod.DataFetcher, "_init_clients", lambda self: None, raising=False)
    monkeypatch.setattr(fetcher_mod.cache, "get", lambda key: None, raising=False)
    monkeypatch.setattr(fetcher_mod.cache, "set", lambda key, value, ttl=None: None, raising=False)

    fetcher = fetcher_mod.DataFetcher()
    fetcher._fmp_bridge = SimpleNamespace(
        get_ohlcv=lambda asset, interval, periods, category="", end_time=None, closed_only=False: (
            pd.DataFrame(
                {
                    "open": [1.1010, 1.1020],
                    "high": [1.1030, 1.1040],
                    "low": [1.1000, 1.1010],
                    "close": [1.1025, 1.1035],
                    "volume": [1000.0, 1200.0],
                }
            ),
            {"source": "FMP", "source_class": "secondary_api", "delayed": False, "realtime": False},
        ),
        resolve_symbol_info=lambda asset, category="": {"symbol": "EURUSD", "exchange": "fmp"},
    )
    fetcher._deriv_bridge = SimpleNamespace(
        get_ohlcv=lambda asset, interval, periods, category="", end_time=None, closed_only=False: (
            pd.DataFrame(
                {
                    "open": [9.0, 9.0],
                    "high": [9.0, 9.0],
                    "low": [9.0, 9.0],
                    "close": [9.0, 9.0],
                    "volume": [0.0, 0.0],
                }
            ),
            {"source": "Deriv", "source_class": "primary_api", "delayed": False, "realtime": False},
        ),
        resolve_symbol_info=lambda asset, category="": {"symbol": "frxEURUSD", "exchange": "deriv"},
    )

    df = fetcher.get_ohlcv("EUR/USD", "forex", "15m", 2)
    meta = fetcher.get_last_ohlcv_metadata("EUR/USD", "15m")

    assert df is not None and not df.empty
    assert float(df.iloc[-1]["close"]) == 1.1035
    assert meta["source"] == "FMP"
    assert meta["source_class"] == "secondary_api"
    assert meta["fmp_symbol"] == "EURUSD"

def test_fetcher_uses_dukascopy_ohlcv_before_fmp_for_forex(monkeypatch) -> None:
    fetcher_mod = importlib.import_module("data.fetcher")
    monkeypatch.setattr(fetcher_mod.DataFetcher, "_init_clients", lambda self: None, raising=False)
    monkeypatch.setattr(fetcher_mod.cache, "get", lambda key: None, raising=False)
    monkeypatch.setattr(fetcher_mod.cache, "set", lambda key, value, ttl=None: None, raising=False)

    fetcher = fetcher_mod.DataFetcher()
    fetcher._dukascopy_bridge = SimpleNamespace(
        get_ohlcv=lambda asset, interval, periods, category="", end_time=None, closed_only=False: (
            pd.DataFrame(
                {
                    "open": [1.0810, 1.0820],
                    "high": [1.0830, 1.0840],
                    "low": [1.0800, 1.0810],
                    "close": [1.0825, 1.0835],
                    "volume": [100.0, 120.0],
                }
            ),
            {"source": "Dukascopy", "source_class": "secondary_api", "delayed": False, "realtime": False},
        ),
        resolve_symbol_info=lambda asset, category="": {"symbol": "EURUSD", "exchange": "dukascopy"},
    )
    fetcher._fmp_bridge = SimpleNamespace(
        get_ohlcv=lambda asset, interval, periods, category="", end_time=None, closed_only=False: (
            pd.DataFrame(
                {
                    "open": [9.0, 9.0],
                    "high": [9.0, 9.0],
                    "low": [9.0, 9.0],
                    "close": [9.0, 9.0],
                    "volume": [0.0, 0.0],
                }
            ),
            {"source": "FMP", "source_class": "secondary_api", "delayed": False, "realtime": False},
        ),
        resolve_symbol_info=lambda asset, category="": {"symbol": "EURUSD", "exchange": "fmp"},
    )

    df = fetcher.get_ohlcv("EUR/USD", "forex", "15m", 2)
    meta = fetcher.get_last_ohlcv_metadata("EUR/USD", "15m")

    assert df is not None and not df.empty
    assert float(df.iloc[-1]["close"]) == 1.0835
    assert meta["source"] == "Dukascopy"
    assert meta["dukascopy_symbol"] == "EURUSD"

def test_local_candle_store_records_and_resamples_live_prices(tmp_path) -> None:
    store_mod = importlib.import_module("services.local_candle_store")
    store = store_mod.LocalCandleStore(enabled=True, path=tmp_path / "candles.sqlite3")

    base = pd.Timestamp("2026-04-06T00:00:15Z").timestamp()
    store.record_live_price("XAU/USD", 100.0, source="IG", timestamp=base)
    store.record_live_price("XAU/USD", 101.0, source="IG", timestamp=base + 20)
    store.record_live_price("XAU/USD", 99.0, source="IG", timestamp=base + 40)
    store.record_live_price("XAU/USD", 102.0, source="IG", timestamp=base + 65)

    exact_df, exact_meta = store.get_ohlcv(
        "XAU/USD",
        "commodities",
        "1m",
        2,
        end_time="2026-04-06T00:02:00Z",
    )
    assert exact_df is not None and len(exact_df) == 2
    assert float(exact_df.iloc[0]["open"]) == 100.0
    assert float(exact_df.iloc[0]["high"]) == 101.0
    assert float(exact_df.iloc[0]["low"]) == 99.0
    assert float(exact_df.iloc[0]["close"]) == 99.0
    assert exact_meta["provider_family"] == "IG"

    resampled_df, resampled_meta = store.get_ohlcv(
        "XAU/USD",
        "commodities",
        "5m",
        1,
        end_time="2026-04-06T00:05:00Z",
    )
    assert resampled_df is not None and len(resampled_df) == 1
    assert float(resampled_df.iloc[0]["open"]) == 100.0
    assert float(resampled_df.iloc[0]["high"]) == 102.0
    assert float(resampled_df.iloc[0]["low"]) == 99.0
    assert float(resampled_df.iloc[0]["close"]) == 102.0
    assert resampled_meta["provider_family"] == "IG"
    assert resampled_meta["local_mode"] == "resampled_1m"

def test_local_candle_store_merges_exact_history_with_live_tail() -> None:
    local_mod = importlib.import_module("services.local_candle_store")
    store = local_mod.LocalCandleStore(enabled=True, path=Path(tempfile.mkdtemp()) / "candles.sqlite3")

    exact_frame = pd.DataFrame(
        {
            "open": [100.0, 101.0],
            "high": [101.0, 102.0],
            "low": [99.5, 100.5],
            "close": [100.8, 101.6],
            "volume": [10.0, 11.0],
        },
        index=pd.to_datetime(["2026-04-06T00:00:00Z", "2026-04-06T00:05:00Z"], utc=True),
    )
    store.store_ohlcv(
        "XAG/USD",
        "commodities",
        "5m",
        exact_frame,
        {"source": "Dukascopy", "provider_family": "DUKASCOPY", "source_class": "secondary_api"},
    )
    base = pd.Timestamp("2026-04-06T00:10:05Z").timestamp()
    store.record_live_price("XAG/USD", 101.7, source="IG", timestamp=base)
    store.record_live_price("XAG/USD", 101.9, source="IG", timestamp=base + 45)
    store.record_live_price("XAG/USD", 102.2, source="IG", timestamp=base + 115)

    merged_df, merged_meta = store.get_ohlcv(
        "XAG/USD",
        "commodities",
        "5m",
        3,
        end_time="2026-04-06T00:15:00Z",
    )

    assert merged_df is not None and len(merged_df) == 3
    assert float(merged_df.iloc[-1]["close"]) == 102.2
    assert merged_meta["local_mode"] == "merged_live_tail"
    assert merged_meta["provider_family"] == "MIXED"
    assert merged_meta["latest_provider_family"] == "IG"
    assert merged_meta["latest_source_class"] == "stream_cache"

def test_fetcher_prefers_local_candle_store_when_complete(monkeypatch) -> None:
    fetcher_mod = importlib.import_module("data.fetcher")
    monkeypatch.setattr(fetcher_mod.DataFetcher, "_init_clients", lambda self: None, raising=False)
    monkeypatch.setattr(fetcher_mod.cache, "get", lambda key: None, raising=False)
    monkeypatch.setattr(fetcher_mod.cache, "set", lambda key, value, ttl=None: None, raising=False)

    frame = pd.DataFrame(
        {
            "open": [1.1000, 1.1010],
            "high": [1.1020, 1.1030],
            "low": [1.0990, 1.1000],
            "close": [1.1015, 1.1025],
            "volume": [10.0, 12.0],
        },
        index=pd.date_range("2026-04-01 00:00:00", periods=2, freq="15min", tz="UTC"),
    )

    fetcher = fetcher_mod.DataFetcher()
    fetcher._local_candle_store = SimpleNamespace(
        get_ohlcv=lambda asset, category, interval, periods, end_time=None, closed_only=False: (
            frame,
            {"provider_family": "DUKASCOPY", "local_mode": "exact"},
        )
    )
    fetcher._dukascopy_bridge = SimpleNamespace(
        get_ohlcv=lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("provider should not be called"))
    )

    df = fetcher.get_ohlcv("EUR/USD", "forex", "15m", 2)
    meta = fetcher.get_last_ohlcv_metadata("EUR/USD", "15m")

    assert df is not None and len(df) == 2
    assert meta["source"] == "LocalStore"
    assert meta["source_class"] == "local_store"
    assert meta["provider_family"] == "DUKASCOPY"

def test_fetcher_writes_provider_history_through_local_candle_store(monkeypatch) -> None:
    fetcher_mod = importlib.import_module("data.fetcher")
    monkeypatch.setattr(fetcher_mod.DataFetcher, "_init_clients", lambda self: None, raising=False)
    monkeypatch.setattr(fetcher_mod.cache, "get", lambda key: None, raising=False)
    monkeypatch.setattr(fetcher_mod.cache, "set", lambda key, value, ttl=None: None, raising=False)

    stored = []
    frame = pd.DataFrame(
        {
            "open": [70.0, 70.2],
            "high": [70.3, 70.6],
            "low": [69.9, 70.0],
            "close": [70.2, 70.5],
            "volume": [0.0, 0.0],
        },
        index=pd.date_range("2026-04-01 00:00:00", periods=2, freq="15min", tz="UTC"),
    )

    fetcher = fetcher_mod.DataFetcher()
    fetcher._local_candle_store = SimpleNamespace(
        get_ohlcv=lambda asset, category, interval, periods, end_time=None, closed_only=False: (None, {}),
        store_ohlcv=lambda asset, category, interval, df, meta: stored.append((asset, category, interval, len(df), meta["source"])),
    )
    fetcher._dukascopy_bridge = SimpleNamespace(
        get_ohlcv=lambda asset, interval, periods, category="", end_time=None, closed_only=False: (
            frame,
            {"source": "Dukascopy", "source_class": "secondary_api"},
        ),
        resolve_symbol_info=lambda asset, category="": {"symbol": "LIGHTCMDUSD", "exchange": "dukascopy"},
    )

    df = fetcher.get_ohlcv("WTI", "commodities", "15m", 2)
    meta = fetcher.get_last_ohlcv_metadata("WTI", "15m")

    assert df is not None and len(df) == 2
    assert meta["source"] == "Dukascopy"
    assert stored == [("WTI", "commodities", "15m", 2, "Dukascopy")]

def test_websocket_dashboard_set_live_price_records_local_store(monkeypatch) -> None:
    ws_mod = importlib.import_module("websocket_dashboard")
    calls = []

    monkeypatch.setattr(
        ws_mod,
        "local_candle_store",
        SimpleNamespace(record_live_price=lambda *args, **kwargs: calls.append((args, kwargs))),
        raising=False,
    )

    ws_mod.set_live_price("XAU/USD", 2345.5, "IG")

    assert calls
    assert calls[0][0][0] == "XAU/USD"
    assert calls[0][0][1] == 2345.5
    assert calls[0][1]["source"] == "IG"

def test_fetcher_falls_back_to_ig_when_fmp_history_missing_for_routed_commodity(monkeypatch) -> None:
    fetcher_mod = importlib.import_module("data.fetcher")
    monkeypatch.setattr(fetcher_mod.DataFetcher, "_init_clients", lambda self: None, raising=False)
    monkeypatch.setattr(fetcher_mod.cache, "get", lambda key: None, raising=False)
    monkeypatch.setattr(fetcher_mod.cache, "set", lambda key, value, ttl=None: None, raising=False)

    fetcher = fetcher_mod.DataFetcher()
    fetcher._fmp_bridge = SimpleNamespace(
        get_ohlcv=lambda asset, interval, periods, category="", end_time=None, closed_only=False: (None, {}),
        resolve_symbol_info=lambda asset, category="": {"symbol": "CLUSD", "exchange": "fmp"},
    )
    fetcher._ig_bridge = SimpleNamespace(
        get_ohlcv=lambda asset, interval, periods, category="", end_time=None, closed_only=False: (
            pd.DataFrame(
                {
                    "open": [70.1, 70.4],
                    "high": [70.6, 70.8],
                    "low": [69.9, 70.2],
                    "close": [70.5, 70.7],
                    "volume": [0.0, 0.0],
                }
            ),
            {"source": "IG", "source_class": "primary_api", "delayed": False, "realtime": False},
        ),
        resolve_symbol_info=lambda asset, category="": {"symbol": "CC.D.CL.BMU.IP", "exchange": "ig"},
    )

    df = fetcher.get_ohlcv("WTI", "commodities", "15m", 2)
    meta = fetcher.get_last_ohlcv_metadata("WTI", "15m")

    assert df is not None and not df.empty
    assert float(df.iloc[-1]["close"]) == 70.7
    assert meta["source"] == "IG"
    assert meta["ig_epic"] == "CC.D.CL.BMU.IP"

def test_fetcher_falls_back_to_ig_when_dukascopy_history_missing_for_routed_commodity(monkeypatch) -> None:
    fetcher_mod = importlib.import_module("data.fetcher")
    monkeypatch.setattr(fetcher_mod.DataFetcher, "_init_clients", lambda self: None, raising=False)
    monkeypatch.setattr(fetcher_mod.cache, "get", lambda key: None, raising=False)
    monkeypatch.setattr(fetcher_mod.cache, "set", lambda key, value, ttl=None: None, raising=False)

    fetcher = fetcher_mod.DataFetcher()
    fetcher._dukascopy_bridge = SimpleNamespace(
        get_ohlcv=lambda asset, interval, periods, category="", end_time=None, closed_only=False: (None, {}),
        resolve_symbol_info=lambda asset, category="": {"symbol": "LIGHTCMDUSD", "exchange": "dukascopy"},
    )
    fetcher._ig_bridge = SimpleNamespace(
        get_ohlcv=lambda asset, interval, periods, category="", end_time=None, closed_only=False: (
            pd.DataFrame(
                {
                    "open": [80.1, 80.4],
                    "high": [80.6, 80.8],
                    "low": [79.9, 80.2],
                    "close": [80.5, 80.7],
                    "volume": [0.0, 0.0],
                }
            ),
            {"source": "IG", "source_class": "primary_api", "delayed": False, "realtime": False},
        ),
        resolve_symbol_info=lambda asset, category="": {"symbol": "CC.D.CL.BMU.IP", "exchange": "ig"},
    )

    df = fetcher.get_ohlcv("WTI", "commodities", "15m", 2)
    meta = fetcher.get_last_ohlcv_metadata("WTI", "15m")

    assert df is not None and not df.empty
    assert float(df.iloc[-1]["close"]) == 80.7
    assert meta["source"] == "IG"
    assert meta["ig_epic"] == "CC.D.CL.BMU.IP"

def test_fmp_bridge_caches_intraday_restriction(monkeypatch) -> None:
    fmp_mod = importlib.import_module("services.fmp_history_bridge")

    monkeypatch.setattr(fmp_mod, "FMP_HISTORY_ENABLED", True, raising=False)
    monkeypatch.setattr(fmp_mod, "FMP_API_KEY", "demo-key", raising=False)
    monkeypatch.setattr(fmp_mod, "FMP_SYMBOL_MAP", "", raising=False)

    calls = {"count": 0}

    class _FakeResponse:
        status_code = 402
        text = "Restricted Endpoint: This endpoint is not available under your current subscription"

        def json(self):
            return {"error": self.text}

    class _FakeSession:
        headers = {}

        def get(self, *args, **kwargs):
            calls["count"] += 1
            return _FakeResponse()

    bridge = fmp_mod.FMPHistoryBridge()
    bridge._session = _FakeSession()

    df1, meta1 = bridge.get_ohlcv("EUR/USD", "15m", 20, category="forex")
    df2, meta2 = bridge.get_ohlcv("EUR/USD", "15m", 20, category="forex")

    assert df1 is None and df2 is None
    assert meta1["provider_error_code"] == "restricted_intraday"
    assert meta2["provider_error_code"] == "restricted_intraday"
    assert calls["count"] == 1

def test_dukascopy_bridge_decodes_and_resamples_minute_candles(monkeypatch) -> None:
    duk_mod = importlib.import_module("services.dukascopy_history_bridge")
    monkeypatch.setattr(duk_mod, "DUKASCOPY_HISTORY_ENABLED", True, raising=False)
    monkeypatch.setattr(duk_mod, "DUKASCOPY_SYMBOL_MAP", "", raising=False)

    class _FakeResponse:
        status_code = 200

        def __init__(self, content: bytes) -> None:
            self.content = content

    class _FakeSession:
        headers = {}

        def __init__(self) -> None:
            self.calls = []

        def get(self, url, timeout=None):
            self.calls.append(url)
            rows = b"".join(
                struct.pack(">IIIII f", offset, open_, close_, low, high, volume)
                for offset, open_, close_, low, high, volume in [
                    (0, 2245000, 2245200, 2244900, 2245300, 1.0),
                    (60, 2245200, 2245400, 2245100, 2245500, 2.0),
                    (120, 2245400, 2245100, 2245000, 2245600, 3.0),
                    (180, 2245100, 2245800, 2245000, 2245900, 4.0),
                    (240, 2245800, 2246000, 2245700, 2246100, 5.0),
                ]
            )
            return _FakeResponse(lzma.compress(rows))

    bridge = duk_mod.DukascopyHistoryBridge()
    fake_session = _FakeSession()
    bridge._session = fake_session

    df, meta = bridge.get_ohlcv("XAU/USD", "5m", 1, category="commodities", end_time="2024-04-01T00:05:00Z", closed_only=True)

    assert df is not None and not df.empty
    assert len(df) >= 1
    assert float(df.iloc[-1]["open"]) == 2245.0
    assert float(df.iloc[-1]["high"]) == 2246.1
    assert float(df.iloc[-1]["low"]) == 2244.9
    assert float(df.iloc[-1]["close"]) == 2246.0
    assert float(df.iloc[-1]["volume"]) == 15.0
    assert meta["dukascopy_symbol"] == "XAUUSD"

def test_fetcher_preserves_ig_ohlcv_error_metadata_for_routed_commodities(monkeypatch) -> None:
    fetcher_mod = importlib.import_module("data.fetcher")
    monkeypatch.setattr(fetcher_mod.DataFetcher, "_init_clients", lambda self: None, raising=False)
    monkeypatch.setattr(fetcher_mod.cache, "get", lambda key: None, raising=False)
    monkeypatch.setattr(fetcher_mod.cache, "set", lambda key, value, ttl=None: None, raising=False)

    fetcher = fetcher_mod.DataFetcher()
    fetcher._ig_bridge = SimpleNamespace(
        get_ohlcv=lambda asset, interval, periods, category="", end_time=None, closed_only=False: (
            None,
            {
                "source": "IG",
                "source_class": "primary_api",
                "delayed": False,
                "realtime": False,
                "provider_error_code": "missing_credentials",
                "provider_error_message": "IG_IDENTIFIER and IG_PASSWORD are required for IG commodity data.",
            },
        ),
        resolve_symbol_info=lambda asset, category="": {"symbol": "IX.D.USCRUDE.CFD.IP", "exchange": "ig"},
    )

    df = fetcher.get_ohlcv("WTI", "commodities", "15m", 2)
    meta = fetcher.get_last_ohlcv_metadata("WTI", "15m")

    assert df is None
    assert meta["source"] == "IG"
    assert meta["provider_error_code"] == "missing_credentials"
    assert "IG_IDENTIFIER and IG_PASSWORD" in meta["provider_error_message"]
    assert meta["ig_epic"] == "IX.D.USCRUDE.CFD.IP"

def test_ig_bridge_uses_oauth_bearer_token_for_requests(monkeypatch) -> None:
    ig_mod = importlib.import_module("services.ig_market_bridge")

    monkeypatch.setattr(ig_mod, "IG_ENABLED", True, raising=False)
    monkeypatch.setattr(ig_mod, "IG_API_KEY", "api-key", raising=False)
    monkeypatch.setattr(ig_mod, "IG_IDENTIFIER", "user1", raising=False)
    monkeypatch.setattr(ig_mod, "IG_PASSWORD", "pass1", raising=False)
    monkeypatch.setattr(ig_mod, "IG_ACCOUNT_ID", "", raising=False)
    monkeypatch.setattr(ig_mod, "IG_ENVIRONMENT", "demo", raising=False)
    monkeypatch.setattr(ig_mod, "IG_EPIC_MAP", "", raising=False)

    calls = {"post": [], "request": []}

    class _FakeResponse:
        def __init__(self, status_code, payload=None, headers=None, text="") -> None:
            self.status_code = status_code
            self._payload = payload or {}
            self.headers = headers or {}
            self.text = text

        @property
        def ok(self):
            return 200 <= self.status_code < 300

        def json(self):
            return self._payload

    class _FakeSession:
        def __init__(self) -> None:
            self.headers = {}

        def post(self, url, json=None, headers=None, timeout=None):
            calls["post"].append({"url": url, "json": json, "headers": dict(headers or {})})
            return _FakeResponse(
                200,
                {
                    "accountId": "ACC123",
                    "oauthToken": {
                        "access_token": "access-1",
                        "refresh_token": "refresh-1",
                        "expires_in": "60",
                        "token_type": "Bearer",
                        "scope": "profile",
                    },
                },
            )

        def request(self, method, url, params=None, headers=None, timeout=None):
            calls["request"].append(
                {"method": method, "url": url, "params": params, "headers": dict(headers or {})}
            )
            return _FakeResponse(200, {"markets": []})

    monkeypatch.setattr(ig_mod.requests, "Session", _FakeSession, raising=False)

    bridge = ig_mod.IGMarketBridge()
    payload = bridge._request("GET", "/markets", params={"searchTerm": "gold"})

    assert payload == {"markets": []}
    assert len(calls["post"]) == 1
    assert calls["post"][0]["headers"]["Version"] == "3"
    assert len(calls["request"]) == 1
    assert calls["request"][0]["headers"]["Authorization"] == "Bearer access-1"
    assert calls["request"][0]["headers"]["IG-ACCOUNT-ID"] == "ACC123"
    assert "CST" not in calls["request"][0]["headers"]
    assert "X-SECURITY-TOKEN" not in calls["request"][0]["headers"]

def test_ig_bridge_refreshes_oauth_token_when_expired(monkeypatch) -> None:
    ig_mod = importlib.import_module("services.ig_market_bridge")

    monkeypatch.setattr(ig_mod, "IG_ENABLED", True, raising=False)
    monkeypatch.setattr(ig_mod, "IG_API_KEY", "api-key", raising=False)
    monkeypatch.setattr(ig_mod, "IG_IDENTIFIER", "user1", raising=False)
    monkeypatch.setattr(ig_mod, "IG_PASSWORD", "pass1", raising=False)
    monkeypatch.setattr(ig_mod, "IG_ACCOUNT_ID", "ACC999", raising=False)
    monkeypatch.setattr(ig_mod, "IG_ENVIRONMENT", "demo", raising=False)
    monkeypatch.setattr(ig_mod, "IG_EPIC_MAP", "", raising=False)

    refresh_calls = []

    class _FakeResponse:
        def __init__(self, status_code, payload=None, headers=None, text="") -> None:
            self.status_code = status_code
            self._payload = payload or {}
            self.headers = headers or {}
            self.text = text

        @property
        def ok(self):
            return 200 <= self.status_code < 300

        def json(self):
            return self._payload

    class _FakeSession:
        def __init__(self) -> None:
            self.headers = {}

        def post(self, url, json=None, headers=None, timeout=None):
            refresh_calls.append({"url": url, "json": json, "headers": dict(headers or {})})
            assert url.endswith("/session/refresh-token")
            return _FakeResponse(
                200,
                {
                    "access_token": "access-2",
                    "refresh_token": "refresh-2",
                    "expires_in": "120",
                    "token_type": "Bearer",
                    "scope": "profile",
                },
            )

    monkeypatch.setattr(ig_mod.requests, "Session", _FakeSession, raising=False)

    bridge = ig_mod.IGMarketBridge()
    bridge._access_token = "stale-access"
    bridge._refresh_token = "refresh-1"
    bridge._session_expires_at = 0.0

    bridge._ensure_session()

    assert len(refresh_calls) == 1
    assert bridge._access_token == "access-2"
    assert bridge._refresh_token == "refresh-2"
    assert bridge._account_id == "ACC999"
    assert bridge._session_expires_at > time.monotonic()

def test_ig_bridge_retries_once_after_401_with_oauth_refresh(monkeypatch) -> None:
    ig_mod = importlib.import_module("services.ig_market_bridge")

    monkeypatch.setattr(ig_mod, "IG_ENABLED", True, raising=False)
    monkeypatch.setattr(ig_mod, "IG_API_KEY", "api-key", raising=False)
    monkeypatch.setattr(ig_mod, "IG_IDENTIFIER", "user1", raising=False)
    monkeypatch.setattr(ig_mod, "IG_PASSWORD", "pass1", raising=False)
    monkeypatch.setattr(ig_mod, "IG_ACCOUNT_ID", "ACC101", raising=False)
    monkeypatch.setattr(ig_mod, "IG_ENVIRONMENT", "demo", raising=False)
    monkeypatch.setattr(ig_mod, "IG_EPIC_MAP", "", raising=False)

    request_calls = []
    refresh_calls = []

    class _FakeResponse:
        def __init__(self, status_code, payload=None, headers=None, text="") -> None:
            self.status_code = status_code
            self._payload = payload or {}
            self.headers = headers or {}
            self.text = text

        @property
        def ok(self):
            return 200 <= self.status_code < 300

        def json(self):
            return self._payload

    class _FakeSession:
        def __init__(self) -> None:
            self.headers = {}

        def post(self, url, json=None, headers=None, timeout=None):
            refresh_calls.append({"url": url, "json": json, "headers": dict(headers or {})})
            return _FakeResponse(
                200,
                {
                    "access_token": "access-fresh",
                    "refresh_token": "refresh-fresh",
                    "expires_in": "90",
                    "token_type": "Bearer",
                    "scope": "profile",
                },
            )

        def request(self, method, url, params=None, headers=None, timeout=None):
            request_calls.append({"method": method, "url": url, "headers": dict(headers or {})})
            if len(request_calls) == 1:
                return _FakeResponse(401, {"errorCode": "error.security.oauth-token-invalid"})
            return _FakeResponse(200, {"markets": [{"epic": "IX.D.XAU.CFD.IP"}]})

    monkeypatch.setattr(ig_mod.requests, "Session", _FakeSession, raising=False)

    bridge = ig_mod.IGMarketBridge()
    bridge._access_token = "access-stale"
    bridge._refresh_token = "refresh-stale"
    bridge._session_expires_at = time.monotonic() + 300.0

    payload = bridge._request("GET", "/markets", params={"searchTerm": "gold"})

    assert payload == {"markets": [{"epic": "IX.D.XAU.CFD.IP"}]}
    assert len(refresh_calls) == 1
    assert len(request_calls) == 2
    assert request_calls[0]["headers"]["Authorization"] == "Bearer access-stale"
    assert request_calls[1]["headers"]["Authorization"] == "Bearer access-fresh"

def test_ig_bridge_account_summary_includes_watchlists_and_activity(monkeypatch) -> None:
    ig_mod = importlib.import_module("services.ig_market_bridge")

    monkeypatch.setattr(ig_mod, "IG_ENABLED", True, raising=False)
    monkeypatch.setattr(ig_mod, "IG_API_KEY", "api-key", raising=False)
    monkeypatch.setattr(ig_mod, "IG_IDENTIFIER", "user1", raising=False)
    monkeypatch.setattr(ig_mod, "IG_PASSWORD", "pass1", raising=False)
    monkeypatch.setattr(ig_mod, "IG_ACCOUNT_ID", "ACC101", raising=False)
    monkeypatch.setattr(ig_mod, "IG_ENVIRONMENT", "demo", raising=False)
    monkeypatch.setattr(ig_mod, "IG_EPIC_MAP", "", raising=False)

    class _FakeResponse:
        def __init__(self, status_code, payload=None, headers=None, text="") -> None:
            self.status_code = status_code
            self._payload = payload or {}
            self.headers = headers or {}
            self.text = text

        @property
        def ok(self):
            return 200 <= self.status_code < 300

        def json(self):
            return self._payload

    class _FakeSession:
        def __init__(self) -> None:
            self.headers = {}

        def post(self, url, json=None, headers=None, timeout=None):
            return _FakeResponse(
                200,
                {
                    "accountId": "ACC101",
                    "oauthToken": {
                        "access_token": "access-1",
                        "refresh_token": "refresh-1",
                        "expires_in": "60",
                        "token_type": "Bearer",
                        "scope": "profile",
                    },
                },
            )

        def request(self, method, url, params=None, headers=None, timeout=None):
            if url.endswith("/accounts"):
                return _FakeResponse(
                    200,
                    {
                        "accounts": [
                            {
                                "accountId": "ACC101",
                                "accountName": "CFD",
                                "accountType": "CFD",
                                "status": "ENABLED",
                                "preferred": True,
                                "currency": "USD",
                                "balance": {
                                    "balance": 20000.0,
                                    "available": 19000.0,
                                    "profitLoss": 125.5,
                                },
                            }
                        ]
                    },
                )
            if url.endswith("/watchlists"):
                return _FakeResponse(
                    200,
                    {"watchlists": [{"id": "1", "name": "My Watchlist", "editable": True, "deleteable": False}]},
                )
            if "/history/activity" in url:
                return _FakeResponse(
                    200,
                    {
                        "activities": [
                            {
                                "date": "2026-04-05T00:00:00",
                                "actionType": "POSITION",
                                "description": "Viewed account",
                                "dealId": "D1",
                                "details": [{"marketName": "Spot Gold", "epic": "CS.D.IN_GOLD.MFI.IP"}],
                            }
                        ]
                    },
                )
            raise AssertionError(f"Unexpected IG URL: {url}")

    monkeypatch.setattr(ig_mod.requests, "Session", _FakeSession, raising=False)

    bridge = ig_mod.IGMarketBridge()
    summary = bridge.get_account_summary()

    assert summary["authenticated"] is True
    assert summary["account_id"] == "ACC101"
    assert summary["balance"] == 20000.0
    assert summary["watchlist_count"] == 1
    assert summary["recent_activity_count"] == 1
    assert summary["recent_activities"][0]["market_name"] == "Spot Gold"

def test_ig_bridge_client_sentiment_uses_market_id(monkeypatch) -> None:
    ig_mod = importlib.import_module("services.ig_market_bridge")

    monkeypatch.setattr(ig_mod, "IG_ENABLED", True, raising=False)
    monkeypatch.setattr(ig_mod, "IG_API_KEY", "api-key", raising=False)
    monkeypatch.setattr(ig_mod, "IG_IDENTIFIER", "user1", raising=False)
    monkeypatch.setattr(ig_mod, "IG_PASSWORD", "pass1", raising=False)
    monkeypatch.setattr(ig_mod, "IG_ACCOUNT_ID", "ACC101", raising=False)
    monkeypatch.setattr(ig_mod, "IG_ENVIRONMENT", "demo", raising=False)
    monkeypatch.setattr(ig_mod, "IG_EPIC_MAP", "", raising=False)

    class _FakeSession:
        def __init__(self) -> None:
            self.headers = {}

    monkeypatch.setattr(ig_mod.requests, "Session", _FakeSession, raising=False)

    bridge = ig_mod.IGMarketBridge()
    monkeypatch.setattr(
        bridge,
        "resolve_symbol_info",
        lambda asset, category="": {"symbol": "CC.D.CL.BMU.IP", "exchange": "ig"},
        raising=False,
    )
    monkeypatch.setattr(
        bridge,
        "_get_market_details",
        lambda epic: {"instrument": {"marketId": "CL"}},
        raising=False,
    )
    monkeypatch.setattr(
        bridge,
        "_request",
        lambda method, path, params=None, version="1": {
            "marketId": "CL",
            "longPositionPercentage": 70.0,
            "shortPositionPercentage": 30.0,
        },
        raising=False,
    )

    payload = bridge.get_client_sentiment("WTI", category="commodities")

    assert payload["market_id"] == "CL"
    assert payload["bias"] == "BUY"
    assert payload["score"] == 0.4
    assert payload["long_pct"] == 70.0

def test_ig_bridge_get_streaming_session_fetches_cst_and_endpoint(monkeypatch) -> None:
    ig_mod = importlib.import_module("services.ig_market_bridge")

    monkeypatch.setattr(ig_mod, "IG_ENABLED", True, raising=False)
    monkeypatch.setattr(ig_mod, "IG_API_KEY", "api-key", raising=False)
    monkeypatch.setattr(ig_mod, "IG_IDENTIFIER", "user1", raising=False)
    monkeypatch.setattr(ig_mod, "IG_PASSWORD", "pass1", raising=False)
    monkeypatch.setattr(ig_mod, "IG_ACCOUNT_ID", "ACC101", raising=False)
    monkeypatch.setattr(ig_mod, "IG_ENVIRONMENT", "demo", raising=False)
    monkeypatch.setattr(ig_mod, "IG_EPIC_MAP", "", raising=False)

    class _FakeResponse:
        def __init__(self, status_code, payload=None, headers=None, text="") -> None:
            self.status_code = status_code
            self._payload = payload or {}
            self.headers = headers or {}
            self.text = text

        @property
        def ok(self):
            return 200 <= self.status_code < 300

        def json(self):
            return self._payload

    class _FakeSession:
        def __init__(self) -> None:
            self.headers = {}
            self.calls = []

        def get(self, url, params=None, headers=None, timeout=None):
            self.calls.append({"url": url, "params": dict(params or {}), "headers": dict(headers or {})})
            return _FakeResponse(
                200,
                {
                    "accountId": "ACC101",
                    "lightstreamerEndpoint": "https://demo-apd.marketdatasystems.com",
                },
                headers={"CST": "cst-123", "X-SECURITY-TOKEN": "xst-456"},
            )

    monkeypatch.setattr(ig_mod.requests, "Session", _FakeSession, raising=False)

    bridge = ig_mod.IGMarketBridge()
    bridge._access_token = "access-1"
    bridge._token_type = "Bearer"
    bridge._session_expires_at = time.monotonic() + 300.0
    monkeypatch.setattr(bridge, "_ensure_session", lambda: None, raising=False)

    payload = bridge.get_streaming_session()

    assert payload["account_id"] == "ACC101"
    assert payload["lightstreamer_endpoint"] == "https://demo-apd.marketdatasystems.com"
    assert payload["password"] == "CST-cst-123|XST-xst-456"
    assert bridge._cst_token == "cst-123"
    assert bridge._security_token == "xst-456"

def test_ig_bridge_override_epic_uses_market_details_for_streaming_flag(monkeypatch) -> None:
    ig_mod = importlib.import_module("services.ig_market_bridge")

    monkeypatch.setattr(ig_mod, "IG_ENABLED", True, raising=False)
    monkeypatch.setattr(ig_mod, "IG_API_KEY", "api-key", raising=False)
    monkeypatch.setattr(ig_mod, "IG_IDENTIFIER", "user1", raising=False)
    monkeypatch.setattr(ig_mod, "IG_PASSWORD", "pass1", raising=False)
    monkeypatch.setattr(ig_mod, "IG_ACCOUNT_ID", "ACC101", raising=False)
    monkeypatch.setattr(ig_mod, "IG_ENVIRONMENT", "demo", raising=False)
    monkeypatch.setattr(
        ig_mod,
        "IG_EPIC_MAP",
        json.dumps({"XAU/USD": "CS.D.IN_GOLD.MFI.IP"}),
        raising=False,
    )

    class _FakeSession:
        def __init__(self) -> None:
            self.headers = {}

    monkeypatch.setattr(ig_mod.requests, "Session", _FakeSession, raising=False)

    bridge = ig_mod.IGMarketBridge()
    monkeypatch.setattr(
        bridge,
        "_get_market_details",
        lambda epic: {
            "instrument": {
                "name": "Spot Gold",
                "type": "COMMODITIES",
                "streamingPricesAvailable": True,
            },
            "snapshot": {
                "marketStatus": "TRADEABLE",
                "delayTime": 0,
            },
        },
        raising=False,
    )

    payload = bridge.resolve_symbol_info("XAU/USD", category="commodities")

    assert payload["symbol"] == "CS.D.IN_GOLD.MFI.IP"
    assert payload["streaming_prices_available"] is True
    assert payload["market_status"] == "TRADEABLE"

def test_ig_streaming_manager_filters_streamable_assets(monkeypatch) -> None:
    stream_mod = importlib.import_module("services.ig_streaming_manager")

    manager = stream_mod.IGStreamingManager()
    monkeypatch.setattr(manager, "is_available", lambda: True, raising=False)
    monkeypatch.setattr(
        stream_mod,
        "filter_ig_primary_assets",
        lambda assets: {
            "XAU/USD": "commodities",
            "WTI": "commodities",
            "EUR/USD": "forex",
        },
        raising=False,
    )
    monkeypatch.setattr(
        stream_mod,
        "ig_market_bridge",
        SimpleNamespace(
            list_profiles=lambda: ["ig"],
            resolve_symbol_info=lambda asset, category="": {
                "symbol": f"EPIC:{asset}",
                "streaming_prices_available": asset == "XAU/USD",
            },
        ),
        raising=False,
    )

    payload = manager.filter_streamable_assets({"XAU/USD": "commodities", "WTI": "commodities"})

    assert payload == {"XAU/USD": "commodities"}

def test_ig_streaming_manager_item_update_emits_midprice_callback(monkeypatch) -> None:
    stream_mod = importlib.import_module("services.ig_streaming_manager")
    ws_mod = importlib.import_module("websocket_dashboard")

    manager = stream_mod.IGStreamingManager()
    seen = {"activity": [], "callbacks": []}

    monkeypatch.setattr(
        ws_mod,
        "mark_feed_activity",
        lambda source, symbol_count=None: seen["activity"].append((source, symbol_count)),
        raising=False,
    )

    manager._callbacks = [
        lambda source, asset, price, volume, side, ts=None: seen["callbacks"].append(
            (source, asset, round(float(price), 5), ts)
        )
    ]
    manager._asset_to_epic = {"XAU/USD": "CS.D.IN_GOLD.MFI.IP"}
    manager._epic_to_asset = {"CS.D.IN_GOLD.MFI.IP": "XAU/USD"}

    class _FakeUpdate:
        @staticmethod
        def getItemName():
            return "PRICE:ACC101:CS.D.IN_GOLD.MFI.IP"

        @staticmethod
        def getFields():
            return {
                "BIDPRICE1": "4676.99",
                "ASKPRICE1": "4677.19",
                "TIMESTAMP": "1775260992210",
            }

    manager._handle_item_update(_FakeUpdate())

    assert seen["activity"] == [("ig", 1)]
    assert seen["callbacks"][0][0] == "IG"
    assert seen["callbacks"][0][1] == "XAU/USD"
    assert seen["callbacks"][0][2] == 4677.09

def test_ig_streaming_manager_subscribe_prices_builds_epic_map(monkeypatch) -> None:
    stream_mod = importlib.import_module("services.ig_streaming_manager")

    manager = stream_mod.IGStreamingManager()
    monkeypatch.setattr(manager, "filter_streamable_assets", lambda assets: {"XAU/USD": "commodities"}, raising=False)
    monkeypatch.setattr(
        stream_mod,
        "ig_market_bridge",
        SimpleNamespace(
            resolve_symbol_info=lambda asset, category="": {
                "symbol": "CS.D.IN_GOLD.MFI.IP",
                "streaming_prices_available": True,
            }
        ),
        raising=False,
    )
    connect_calls = []
    monkeypatch.setattr(manager, "_connect_locked", lambda: connect_calls.append("connect"), raising=False)

    payload = manager.subscribe_prices({"XAU/USD": "commodities"}, lambda *_args: None)

    assert payload == {"XAU/USD": "commodities"}
    assert manager._asset_to_epic == {"XAU/USD": "CS.D.IN_GOLD.MFI.IP"}
    assert manager._epic_to_asset == {"CS.D.IN_GOLD.MFI.IP": "XAU/USD"}
    assert connect_calls == ["connect"]

def test_live_microstructure_service_scores_pressure_and_stop_hunt() -> None:
    micro_mod = importlib.import_module("services.live_microstructure_service")
    service = micro_mod.LiveMicrostructureService(maxlen=32)

    for step in range(6):
        service.record_quote(
            "ig",
            "XAU/USD",
            bid=100.00 + step * 0.02,
            ask=100.03 + step * 0.02,
            bid_size=6.0 + step,
            ask_size=2.0,
            timestamp=1000 + step,
        )

    bullish = service.get_snapshot("ig", "XAU/USD", price=100.13, spread=0.03)

    service.record_quote("ig", "WTI", bid=80.00, ask=80.04, timestamp=2000)
    service.record_quote("ig", "WTI", bid=80.06, ask=80.14, timestamp=2001)
    service.record_quote("ig", "WTI", bid=80.18, ask=80.34, timestamp=2002)
    service.record_quote("ig", "WTI", bid=79.96, ask=80.20, timestamp=2003, flags="EDIT")

    stressed = service.get_snapshot("ig", "WTI", price=80.08, spread=0.24)

    assert bullish["tick_imbalance"] > 0.5
    assert bullish["book_imbalance"] > 0.4
    assert bullish["pressure_direction"] == "BUY"
    assert bullish["score"] > 0.2
    assert stressed["stop_hunt_risk"] >= 0.35
    assert stressed["exhaustion_risk"] >= 0.2

def test_live_microstructure_service_synthesizes_depth_when_sizes_missing() -> None:
    micro_mod = importlib.import_module("services.live_microstructure_service")
    service = micro_mod.LiveMicrostructureService(maxlen=32)

    service.record_quote("ig", "BTC-USD", bid=100.00, ask=100.08, timestamp=1)
    service.record_quote("ig", "BTC-USD", bid=100.04, ask=100.10, timestamp=2)
    service.record_quote("ig", "BTC-USD", bid=100.08, ask=100.12, timestamp=3)
    service.record_quote("ig", "BTC-USD", bid=100.12, ask=100.15, timestamp=4)

    snapshot = service.get_snapshot("ig", "BTC-USD", price=100.135, spread=0.03)

    assert snapshot["depth_available"] is False
    assert snapshot["synthetic_depth_available"] is True
    assert snapshot["microstructure_source"] == "live_store_synthetic_depth"

def test_cross_asset_spillover_service_links_wti_to_usdcad() -> None:
    spill_mod = importlib.import_module("services.cross_asset_spillover_service")
    service = spill_mod.CrossAssetSpilloverService()

    def _frame(start: float, end: float) -> pd.DataFrame:
        values = np.linspace(start, end, 24)
        return pd.DataFrame(
            {
                "open": values,
                "high": values * 1.001,
                "low": values * 0.999,
                "close": values,
                "volume": np.full(len(values), 1000.0),
            }
        )

    class _Fetcher:
        @staticmethod
        def get_ohlcv(asset: str, category: str, interval: str = "15m", periods: int = 24):
            if asset == "WTI":
                return _frame(80.0, 83.0)
            raise AssertionError(f"unexpected peer fetch for {asset}")

    snapshot = service.build_snapshot(
        asset="USD/CAD",
        category="forex",
        fetcher=_Fetcher(),
        timeframe="15m",
    )

    assert snapshot["state"] == "sell_support"
    assert snapshot["supportive_direction"] == "SELL"
    assert snapshot["dominant_peer"] == "WTI"
    assert snapshot["score"] < 0.0
    assert snapshot["confidence"] > 0.05

def test_ig_streaming_manager_item_update_records_microstructure(monkeypatch) -> None:
    stream_mod = importlib.import_module("services.ig_streaming_manager")
    ws_mod = importlib.import_module("websocket_dashboard")

    manager = stream_mod.IGStreamingManager()
    seen = {"records": []}

    monkeypatch.setattr(
        ws_mod,
        "mark_feed_activity",
        lambda source, symbol_count=None: None,
        raising=False,
    )
    monkeypatch.setitem(
        sys.modules,
        "services.live_microstructure_service",
        SimpleNamespace(
            get_service=lambda: SimpleNamespace(
                record_quote=lambda *args, **kwargs: seen["records"].append((args, kwargs))
            )
        ),
    )

    manager._callbacks = []
    manager._asset_to_epic = {"XAU/USD": "CS.D.IN_GOLD.MFI.IP"}
    manager._epic_to_asset = {"CS.D.IN_GOLD.MFI.IP": "XAU/USD"}

    class _FakeUpdate:
        @staticmethod
        def getItemName():
            return "PRICE:ACC101:CS.D.IN_GOLD.MFI.IP"

        @staticmethod
        def getFields():
            return {
                "BIDPRICE1": "4676.99",
                "ASKPRICE1": "4677.19",
                "BIDSIZE1": "7",
                "ASKSIZE1": "3",
                "BIDPRICE2": "4676.79",
                "ASKPRICE2": "4677.39",
                "BIDSIZE2": "5",
                "ASKSIZE2": "4",
                "TIMESTAMP": "1775260992210",
                "DLG_FLAG": "TRADEABLE",
            }

    manager._handle_item_update(_FakeUpdate())

    assert len(seen["records"]) == 1
    args, kwargs = seen["records"][0]
    assert args[:2] == ("ig", "XAU/USD")
    assert kwargs["bid"] == 4676.99
    assert kwargs["ask"] == 4677.19
    assert kwargs["bid_size"] == 7.0
    assert kwargs["ask_size"] == 3.0
    assert len(kwargs["levels"]) >= 2

def test_sentiment_service_commodity_sentiment_includes_ig_client_sentiment(monkeypatch) -> None:
    sentiment_mod = importlib.import_module("services.sentiment_service")

    monkeypatch.setattr(sentiment_mod._PriceMomentum, "get", staticmethod(lambda asset: 0.2), raising=False)
    monkeypatch.setattr(sentiment_mod._NewsSentiment, "get", staticmethod(lambda asset: 0.1), raising=False)
    monkeypatch.setattr(sentiment_mod._NewsSentiment, "macro_impact", staticmethod(lambda asset: None), raising=False)
    monkeypatch.setattr(sentiment_mod._MarketInstruments, "vix", staticmethod(lambda: None), raising=False)
    monkeypatch.setattr(sentiment_mod, "_reddit_score", lambda asset: None, raising=False)
    monkeypatch.setattr(
        sentiment_mod.SentimentService,
        "_ig_client_sentiment",
        staticmethod(lambda asset: {"asset": asset, "bias": "BUY", "long_pct": 70.0, "short_pct": 30.0, "score": 0.4}),
        raising=False,
    )

    service = sentiment_mod.SentimentService()
    payload = service.get_comprehensive_sentiment("WTI")

    assert payload["components"]["ig_client_sentiment"] == 0.4
    assert payload["weights"]["ig_client_sentiment"] == 0.1
    assert payload["ig_client_sentiment"]["bias"] == "BUY"

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

def test_market_hours_prefers_provider_status(monkeypatch) -> None:
    market_hours_mod = importlib.import_module("dashboard.market_hours")

    monkeypatch.setattr(
        market_hours_mod,
        "_provider_market_status",
        lambda asset: (False, "Closed on IG", "IG"),
        raising=False,
    )

    is_open, reason = market_hours_mod.is_market_open_for_asset("EUR/USD")
    status = market_hours_mod.market_status("EUR/USD")

    assert is_open is False
    assert reason == "Closed on IG"
    assert status["reason"] == "Closed on IG"
    assert status["source"] == "IG"

def test_trading_core_market_hours_prefers_routed_provider_status(monkeypatch) -> None:
    router_mod = importlib.import_module("services.market_data_router")

    monkeypatch.setattr(
        router_mod,
        "get_market_status",
        lambda asset, category="": {
            "asset": asset,
            "market_open": True,
            "reason": "Metals open on IG",
            "source": "IG",
        },
        raising=False,
    )

    is_open, reason = TradingCore._market_hours_status("XAU/USD", "commodities")

    assert is_open is True
    assert reason == "Metals open on IG"

def test_market_data_router_filters_ig_routed_commodities_from_deriv_streams() -> None:
    router_mod = importlib.import_module("services.market_data_router")
    setattr(router_mod, "is_ig_primary_category", lambda category: str(category or "").lower() == "commodities")

    filtered = router_mod.filter_deriv_stream_assets(
        {
            "EUR/USD": "forex",
            "XAU/USD": "commodities",
            "WTI": "commodities",
            "BTC-USD": "crypto",
        }
    )

    assert filtered == {
        "EUR/USD": "forex",
        "BTC-USD": "crypto",
    }

def test_market_data_router_get_client_sentiment_uses_ig_for_routed_assets(monkeypatch) -> None:
    router_mod = importlib.import_module("services.market_data_router")
    ig_mod = importlib.import_module("services.ig_market_bridge")

    monkeypatch.setattr(
        router_mod,
        "is_ig_primary_category",
        lambda category: str(category or "").lower() == "commodities",
        raising=False,
    )
    monkeypatch.setattr(
        ig_mod,
        "ig_market_bridge",
        SimpleNamespace(
            get_client_sentiment=lambda asset, category="": {
                "asset": asset,
                "category": category,
                "bias": "BUY",
                "score": 0.4,
            }
        ),
        raising=False,
    )

    payload = router_mod.get_client_sentiment("WTI", category="commodities")

    assert payload["asset"] == "WTI"
    assert payload["category"] == "commodities"
    assert payload["score"] == 0.4
    assert router_mod.get_client_sentiment("EUR/USD", category="forex") is None

def test_market_data_router_get_broker_account_summary_uses_ig_bridge(monkeypatch) -> None:
    router_mod = importlib.import_module("services.market_data_router")
    ig_mod = importlib.import_module("services.ig_market_bridge")

    monkeypatch.setattr(
        ig_mod,
        "ig_market_bridge",
        SimpleNamespace(
            get_account_summary=lambda: {
                "enabled": True,
                "authenticated": True,
                "environment": "demo",
                "account_id": "Z6A62A",
            }
        ),
        raising=False,
    )

    payload = router_mod.get_broker_account_summary()

    assert payload["authenticated"] is True
    assert payload["account_id"] == "Z6A62A"

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

def test_reddit_watcher_429_uses_stale_cache_and_sets_longer_backoff(monkeypatch) -> None:
    reddit_mod = importlib.import_module("reddit_watcher")
    reddit_mod._shared_cache.clear()
    reddit_mod._subreddit_backoff_until.clear()
    reddit_mod._subreddit_backoff_notified.clear()
    reddit_mod._rate_limit_until = 0.0
    reddit_mod._global_backoff_notified_until = 0.0

    cache_key = "stocks_hot_20"
    cached_posts = [{"title": "cached"}]
    reddit_mod._shared_cache[cache_key] = (cached_posts, time.time() - 3600)

    class _Response:
        status_code = 429
        headers = {"Retry-After": "120"}

    monkeypatch.setattr(reddit_mod, "_rate_limited_request", lambda *args, **kwargs: _Response(), raising=False)
    watcher = reddit_mod.RedditWatcher()

    rows = watcher._fetch_subreddit("stocks", "hot", 20)

    assert rows == cached_posts
    assert reddit_mod._subreddit_backoff_until["stocks"] > time.time() + 100
    assert reddit_mod._rate_limit_until > time.time() + 100

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

def test_telegram_whale_watcher_uses_liquidation_24h_total_for_allowed_asset(monkeypatch) -> None:
    telegram_mod = importlib.import_module("telegram_whale_watcher")

    monkeypatch.setattr(telegram_mod, "_ALLOWED_SYMBOLS", {"BTC", "ETH", "BNB", "SOL", "XRP"}, raising=False)
    monkeypatch.setattr(telegram_mod, "MIN_VALUE_USD", 1_000_000, raising=False)

    alert = telegram_mod._parse_alert(
        "🟢 #BTC Liquidated $59.0K in Short - at $67068\n☠️ 24h Liquidation for $BTC: $15.28M",
        "Whale Liquidations",
        datetime(2026, 4, 3, 0, 11, 0),
    )

    assert alert is not None
    assert alert["symbol"] == "BTC"
    assert alert["event_kind"] == "liquidation"
    assert alert["direction"] == "BUY"
    assert alert["liquidation_side"] == "SHORT"
    assert alert["value_usd"] == 15_280_000

def test_telegram_whale_watcher_filters_out_mixed_channel_assets(monkeypatch) -> None:
    telegram_mod = importlib.import_module("telegram_whale_watcher")

    monkeypatch.setattr(telegram_mod, "_ALLOWED_SYMBOLS", {"BTC", "ETH", "BNB", "SOL", "XRP"}, raising=False)
    monkeypatch.setattr(telegram_mod, "MIN_VALUE_USD", 1_000_000, raising=False)

    alert = telegram_mod._parse_alert(
        "🔴 #DOGE Liquidated $3.4M in Long - at $0.15\n☠️ 24h Liquidation for $DOGE: $11.20M",
        "Whale Liquidations",
        datetime(2026, 4, 3, 0, 12, 0),
    )

    assert alert is None

def test_telegram_whale_watcher_mark_healthy_pings_whale_source(monkeypatch) -> None:
    telegram_mod = importlib.import_module("telegram_whale_watcher")
    calls = []

    monkeypatch.setattr(telegram_mod, "_ping_health", lambda source="whale": calls.append(source), raising=False)

    watcher = telegram_mod.TelegramWhaleWatcher()
    watcher._mark_healthy()

    assert calls == ["whale"]

def test_telegram_status_formatter_includes_ig_broker_summary() -> None:
    telegram_mod = importlib.import_module("telegram_commander")

    text = telegram_mod.TelegramCommander._format_ig_broker_status(
        {
            "enabled": True,
            "authenticated": True,
            "environment": "demo",
            "account_type": "CFD",
            "account_id": "Z6A62A",
            "balance": 20000.0,
            "available": 19000.0,
            "watchlist_count": 2,
            "recent_activity_count": 1,
        }
    )

    assert "IG Broker Data" in text
    assert "DEMO CFD Z6A62A" in text
    assert "$20,000.00" in text
    assert "2 watchlists, 1 recent activities" in text

def test_telegram_status_formatter_includes_signal_diagnostics_summary() -> None:
    telegram_mod = importlib.import_module("telegram_commander")

    text = telegram_mod.TelegramCommander._format_signal_diagnostics_status(
        {
            "count": 4,
            "broker_supportive_count": 2,
            "broker_fragile_count": 1,
            "true_depth_count": 1,
            "synthetic_depth_count": 2,
            "cross_support_count": 3,
            "cross_conflict_count": 1,
            "recent_pattern_block_count": 1,
        }
    )

    assert "Signal Diagnostics" in text
    assert "2 supportive / 1 fragile" in text
    assert "1 true / 2 synthetic" in text
    assert "3 supportive / 1 conflicted" in text
    assert "1 recent-pattern block" in text

def test_telegram_status_formatter_includes_provider_routing_summary(monkeypatch) -> None:
    telegram_mod = importlib.import_module("telegram_commander")
    config_mod = importlib.import_module("config.config")

    monkeypatch.setattr(config_mod, "IG_ROUTED_CATEGORIES", ["commodities"], raising=False)

    text = telegram_mod.TelegramCommander._format_provider_routing_status()

    assert "Provider Routing" in text
    assert "Deriv:    16 assets" in text
    assert "IG:       3 assets" in text

def test_telegram_whale_watcher_parses_whalebotalerts_parenthesized_usd_value(monkeypatch) -> None:
    telegram_mod = importlib.import_module("telegram_whale_watcher")

    monkeypatch.setattr(telegram_mod, "_ALLOWED_SYMBOLS", {"BTC", "ETH", "BNB", "SOL", "XRP"}, raising=False)
    monkeypatch.setattr(telegram_mod, "MIN_VALUE_USD", 1_000_000, raising=False)

    alert = telegram_mod._parse_alert(
        "🚨🚨🚨 501 BTC ($33,618,476) transferred from Gemini to Unknown",
        "whalebotalerts",
        datetime(2026, 4, 4, 1, 9, 0),
    )

    assert alert is not None
    assert alert["symbol"] == "BTC"
    assert alert["event_kind"] == "whale"
    assert alert["value_usd"] == 33_618_476

def test_whale_alert_manager_uses_telegram_only_when_social_whale_sources_disabled(monkeypatch) -> None:
    manager_mod = importlib.import_module("whale_alert_manager")
    original_instance = manager_mod.WhaleAlertManager._instance
    manager_mod.WhaleAlertManager._instance = None

    class _FakeTelegramWatcher:
        def __init__(self):
            self.bot_token = "configured"

    class _UnexpectedTwitterWatcher:
        def __init__(self):
            raise AssertionError("Twitter whale watcher should stay disabled")

    class _UnexpectedRedditWatcher:
        def __init__(self):
            raise AssertionError("Reddit whale watcher should stay disabled")

    monkeypatch.setattr(manager_mod, "WHALE_TWITTER_WHALE_ENABLED", False, raising=False)
    monkeypatch.setattr(manager_mod, "WHALE_REDDIT_WHALE_ENABLED", False, raising=False)
    monkeypatch.setattr(manager_mod, "TelegramWhaleWatcher", _FakeTelegramWatcher, raising=False)
    monkeypatch.setattr(manager_mod, "TwitterWhaleWatcher", _UnexpectedTwitterWatcher, raising=False)
    monkeypatch.setattr(manager_mod, "RedditWatcher", _UnexpectedRedditWatcher, raising=False)
    monkeypatch.setattr(manager_mod, "WhaleAlertDB", lambda: SimpleNamespace(enabled=False), raising=False)
    monkeypatch.setattr(
        manager_mod.os,
        "getenv",
        lambda key, default="": "" if key == "WHALE_ALERT_KEY" else default,
        raising=False,
    )

    try:
        manager = manager_mod.WhaleAlertManager()
        assert manager.telegram_watcher is not None
        assert manager.twitter_watcher is None
        assert manager.reddit is None
    finally:
        manager_mod.WhaleAlertManager._instance = original_instance

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

def test_system_health_service_suppresses_startup_cpu_alerts_until_sustained(monkeypatch) -> None:
    monitor_mod = importlib.import_module("monitoring.system_health_service")
    original_instance = monitor_mod.SystemHealthService._instance

    monkeypatch.setattr(monitor_mod.SystemHealthService, "_init_redis", lambda self: None)
    monitor_mod.SystemHealthService._instance = None
    service = monitor_mod.SystemHealthService()

    current_time = {"value": 1000.0}
    alerts = []

    monkeypatch.setattr(monitor_mod.time, "time", lambda: current_time["value"])
    monkeypatch.setitem(
        sys.modules,
        "psutil",
        SimpleNamespace(
            cpu_percent=lambda interval=1: 98.0,
            virtual_memory=lambda: SimpleNamespace(percent=91.0),
        ),
    )
    monkeypatch.setattr(service, "_send_alert", lambda alert_type, message: alerts.append(alert_type))

    try:
        service._start_time = current_time["value"] - (monitor_mod.PHASE_SILENT_SECS - 5)
        service._check_alerts()
        assert alerts == []
        assert service._cpu_high_since is None
        assert service._ram_high_since is None

        current_time["value"] += 10
        service._start_time = current_time["value"] - (monitor_mod.PHASE_SILENT_SECS + 1)
        service._check_alerts()
        assert alerts == []
        assert service._cpu_high_since == current_time["value"]
        assert service._ram_high_since == current_time["value"]

        current_time["value"] += monitor_mod.ALERT_CHECK_INTERVAL + 1
        service._check_alerts()
    finally:
        monitor_mod.SystemHealthService._instance = original_instance

    assert alerts == ["cpu_high", "ram_high"]

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

def test_xrp_tracker_accepts_result_scoped_success_status(monkeypatch) -> None:
    xrp_mod = importlib.import_module("whale_intelligence.xrp_tracker")

    class _Resp:
        @staticmethod
        def raise_for_status():
            return None

        @staticmethod
        def json():
            return {
                "result": {
                    "status": "success",
                    "transactions": [{"tx": {"hash": "abc"}}],
                }
            }

    monkeypatch.setattr(xrp_mod.requests, "post", lambda *args, **kwargs: _Resp())
    tracker = xrp_mod.XRPTracker()

    rows = tracker.get_transaction_history("r" + ("1" * 24), limit=5)

    assert rows == [{"tx": {"hash": "abc"}}]

def test_news_event_monitor_prunes_stale_cache_when_fetch_returns_none(monkeypatch) -> None:
    monitor_mod = importlib.import_module("data_ingestion.news_event_monitor")
    monitor = monitor_mod.news_monitor
    now = datetime.now(timezone.utc)

    with monitor._data_lock:
        monitor._events = [
            {"name": "old upcoming", "impact": "HIGH", "time": now - timedelta(hours=2), "affects": {"forex"}},
            {"name": "valid upcoming", "impact": "HIGH", "time": now + timedelta(minutes=5), "affects": {"forex"}},
        ]
        monitor._recent = [
            {"name": "expired recent", "impact": "HIGH", "time": now - timedelta(minutes=120), "affects": {"forex"}},
            {"name": "valid recent", "impact": "HIGH", "time": now - timedelta(minutes=5), "affects": {"forex"}},
        ]

    monkeypatch.setattr(monitor, "_fetch_deriv", lambda: None, raising=False)
    monitor._fetch_and_update()

    with monitor._data_lock:
        assert [ev["name"] for ev in monitor._events] == ["valid upcoming"]
        assert [ev["name"] for ev in monitor._recent] == ["valid recent"]

def test_data_fetcher_ping_health_forwards_to_monitor(monkeypatch) -> None:
    fetcher_mod = importlib.import_module("data.fetcher")
    monitor_mod = importlib.import_module("monitoring.system_health_service")
    seen = []

    monkeypatch.setattr(
        monitor_mod,
        "monitor",
        SimpleNamespace(ping_source=lambda source: seen.append(source)),
        raising=False,
    )

    fetcher_mod.DataFetcher._ping_health("technicals")
    fetcher_mod.DataFetcher._ping_health("trades")

    assert seen == ["technicals", "trades"]

def test_funding_rate_monitor_analyse_pings_health(monkeypatch) -> None:
    funding_mod = importlib.import_module("data_ingestion.funding_rate_monitor")
    monitor_mod = importlib.import_module("monitoring.system_health_service")
    seen = []

    monkeypatch.setattr(
        monitor_mod,
        "monitor",
        SimpleNamespace(ping_source=lambda source: seen.append(source)),
        raising=False,
    )

    service = funding_mod.FundingRateMonitor()
    service._analyse("BTCUSDT", 0.006)

    assert seen == ["funding_rate"]

def test_open_interest_monitor_analyse_pings_health(monkeypatch) -> None:
    oi_mod = importlib.import_module("data_ingestion.open_interest_monitor")
    monitor_mod = importlib.import_module("monitoring.system_health_service")
    seen = []

    monkeypatch.setattr(
        monitor_mod,
        "monitor",
        SimpleNamespace(ping_source=lambda source: seen.append(source)),
        raising=False,
    )

    service = oi_mod.OpenInterestMonitor()
    service._analyse("BTCUSDT", 12345.0)

    assert seen == ["open_interest"]

def test_liquidation_stream_process_pings_health(monkeypatch) -> None:
    liq_mod = importlib.import_module("data_ingestion.liquidation_stream")
    monitor_mod = importlib.import_module("monitoring.system_health_service")
    seen = []

    monkeypatch.setattr(
        monitor_mod,
        "monitor",
        SimpleNamespace(ping_source=lambda source: seen.append(source)),
        raising=False,
    )

    service = liq_mod.LiquidationStream()
    service._process({"asset": "BTCUSDT", "qty": 1.0, "price": 50000.0, "ts": int(time.time() * 1000)})

    assert seen == ["liquidations"]

def test_news_event_monitor_update_pings_health(monkeypatch) -> None:
    news_mod = importlib.import_module("data_ingestion.news_event_monitor")
    monitor_mod = importlib.import_module("monitoring.system_health_service")
    seen = []
    now = datetime.now(timezone.utc)

    monkeypatch.setattr(
        monitor_mod,
        "monitor",
        SimpleNamespace(ping_source=lambda source: seen.append(source)),
        raising=False,
    )
    monkeypatch.setattr(
        news_mod.news_monitor,
        "_fetch_deriv",
        lambda: [
            {
                "name": "US CPI",
                "impact": "HIGH",
                "time": now + timedelta(minutes=5),
                "affects": {"forex", "indices"},
            }
        ],
        raising=False,
    )

    news_mod.news_monitor._fetch_and_update()

    assert seen == ["news"]

def test_order_flow_update_pings_health(monkeypatch) -> None:
    order_flow_mod = importlib.import_module("order_flow")
    monitor_mod = importlib.import_module("monitoring.system_health_service")
    seen = []

    monkeypatch.setattr(
        monitor_mod,
        "monitor",
        SimpleNamespace(ping_source=lambda source: seen.append(source)),
        raising=False,
    )

    class _Proc:
        def update(self, bids, asks):
            return {"top_bids": bids, "top_asks": asks, "mid": 100.0, "ts": int(time.time() * 1000)}

    class _Walls:
        def scan(self, bids, asks):
            return []

    class _Imbalance:
        def analyse(self, snapshot):
            return None

    class _StopHunt:
        def update_walls(self, walls):
            return None

        def ingest_price(self, mid, ts):
            return None

    monkeypatch.setattr(order_flow_mod, "_get_or_create", lambda asset: (_Proc(), _Walls(), _Imbalance(), _StopHunt()), raising=False)

    order_flow_mod._on_orderbook_update({"asset": "BTCUSDT", "bids": [[100, 1]], "asks": [[101, 1]]})

    assert seen == ["order_book"]

def test_market_intelligence_record_whale_alert_pings_health_once_per_new_event(monkeypatch) -> None:
    market_mod = importlib.import_module("services.market_intelligence_service")
    monitor_mod = importlib.import_module("monitoring.system_health_service")
    seen = []

    monkeypatch.setattr(
        monitor_mod,
        "monitor",
        SimpleNamespace(ping_source=lambda source: seen.append(source)),
        raising=False,
    )

    service = market_mod.MarketIntelligenceService()
    timestamp = "2026-04-04T12:00:00+00:00"

    service.record_whale_alert(
        asset="BTC-USD",
        direction="BUY",
        size_usd=1_500_000,
        source="Telegram/whalebotalerts",
        sentiment=0.25,
        timestamp=timestamp,
        raw_text="Large BTC transfer spotted",
        external_id="whale-1",
    )
    service.record_whale_alert(
        asset="BTC-USD",
        direction="BUY",
        size_usd=1_500_000,
        source="Telegram/whalebotalerts",
        sentiment=0.25,
        timestamp=timestamp,
        raw_text="Large BTC transfer spotted",
        external_id="whale-1",
    )

    assert seen == ["whale"]

def test_sentiment_price_momentum_get_pings_health_only_on_recompute(monkeypatch) -> None:
    sentiment_mod = importlib.import_module("services.sentiment_sources")
    monitor_mod = importlib.import_module("monitoring.system_health_service")
    seen = []

    monkeypatch.setattr(
        monitor_mod,
        "monitor",
        SimpleNamespace(ping_source=lambda source: seen.append(source)),
        raising=False,
    )
    monkeypatch.setattr(sentiment_mod._PriceMomentum, "_compute", lambda asset: 0.21, raising=False)
    sentiment_mod._PriceMomentum._cache.clear()

    assert sentiment_mod._PriceMomentum.get("BTC-USD") == 0.21
    assert sentiment_mod._PriceMomentum.get("BTC-USD") == 0.21
    assert seen == ["sentiment"]

def test_sentiment_news_get_pings_health_only_on_recompute(monkeypatch) -> None:
    sentiment_mod = importlib.import_module("services.sentiment_sources")
    monitor_mod = importlib.import_module("monitoring.system_health_service")
    seen = []

    monkeypatch.setattr(
        monitor_mod,
        "monitor",
        SimpleNamespace(ping_source=lambda source: seen.append(source)),
        raising=False,
    )
    monkeypatch.setattr(sentiment_mod._NewsSentiment, "_compute", lambda asset: -0.18, raising=False)
    sentiment_mod._NewsSentiment._cache.clear()

    assert sentiment_mod._NewsSentiment.get("EUR/USD") == -0.18
    assert sentiment_mod._NewsSentiment.get("EUR/USD") == -0.18
    assert seen == ["sentiment"]

def test_macro_data_collector_process_pings_health_even_without_threshold_break(monkeypatch) -> None:
    macro_mod = importlib.import_module("data_ingestion.macro_data_collector")
    monitor_mod = importlib.import_module("monitoring.system_health_service")
    seen = []

    monkeypatch.setattr(
        monitor_mod,
        "monitor",
        SimpleNamespace(ping_source=lambda source: seen.append(source)),
        raising=False,
    )

    collector = macro_mod.MacroDataCollector()
    collector._process("FEDFUNDS", "Fed Funds Rate", 5.0)
    collector._process("FEDFUNDS", "Fed Funds Rate", 5.0)

    assert seen == ["macro", "macro"]

def test_exchange_stream_manager_market_data_event_pings_trades_health(monkeypatch) -> None:
    stream_mod = importlib.import_module("data_ingestion.exchange_stream_manager")
    monitor_mod = importlib.import_module("monitoring.system_health_service")
    seen = []

    monkeypatch.setattr(
        monitor_mod,
        "monitor",
        SimpleNamespace(ping_source=lambda source: seen.append(source)),
        raising=False,
    )

    manager = stream_mod.ExchangeStreamManager()
    manager._pub = None
    manager._handlers = []

    manager._on_event(
        {
            "type": "MARKET_DATA_UPDATE",
            "exchange": "binance",
            "asset": "BTCUSDT",
            "price": 68000.0,
            "ts": int(time.time() * 1000),
        }
    )

    assert seen == ["trades"]

def test_exchange_stream_manager_bybit_market_data_event_keeps_liquidations_fresh(monkeypatch) -> None:
    stream_mod = importlib.import_module("data_ingestion.exchange_stream_manager")
    monitor_mod = importlib.import_module("monitoring.system_health_service")
    seen = []

    monkeypatch.setattr(
        monitor_mod,
        "monitor",
        SimpleNamespace(ping_source=lambda source: seen.append(source)),
        raising=False,
    )

    manager = stream_mod.ExchangeStreamManager()
    manager._pub = None
    manager._handlers = []

    manager._on_event(
        {
            "type": "MARKET_DATA_UPDATE",
            "exchange": "bybit",
            "asset": "BTCUSDT",
            "price": 68000.0,
            "ts": int(time.time() * 1000),
        }
    )

    assert seen == ["trades", "liquidations"]

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

def test_telegram_configure_bot_menu_registers_commands_and_menu_button() -> None:
    tg_mod = importlib.import_module("telegram_commander")

    seen = {}

    class _FakeBot:
        async def set_my_commands(self, commands):
            seen["commands"] = commands

        async def set_chat_menu_button(self, menu_button=None):
            seen["menu_button"] = menu_button

    commander = object.__new__(tg_mod.TelegramCommander)
    commander.application = SimpleNamespace(bot=_FakeBot())

    asyncio.run(commander._configure_bot_menu())

    commands = seen["commands"]
    assert [item.command for item in commands] == [
        "menu",
        "status",
        "positions",
        "balance",
        "signal",
        "why",
        "history",
        "ask",
        "pause",
        "resume",
    ]
    assert seen["menu_button"].__class__.__name__ == "MenuButtonCommands"

def test_telegram_build_main_menu_surfaces_counts_and_guidance(monkeypatch) -> None:
    tg_mod = importlib.import_module("telegram_commander")
    personality_mod = importlib.import_module("services.personality_service")

    commander = object.__new__(tg_mod.TelegramCommander)
    commander.trading_system = SimpleNamespace(
        is_running=False,
        get_balance=lambda: 1134.25,
        get_positions=lambda: [],
        get_strategy_stats=lambda: {},
    )

    monkeypatch.setattr(
        personality_mod.personality,
        "get_report",
        lambda: {"stats": {"total_trades_remembered": 0}},
        raising=False,
    )

    text, kb = commander._build_main_menu()
    labels = [button.text for row in kb.inline_keyboard for button in row]

    assert "Open positions: 0" in text
    assert "Diary and strategies fill in after the bot has closed trades to learn from." in text
    assert "📈 Positions (0)" in labels
    assert "▶️ Resume" in labels

def test_telegram_build_top_setups_includes_broker_and_depth_context() -> None:
    tg_mod = importlib.import_module("telegram_commander")

    commander = object.__new__(tg_mod.TelegramCommander)
    commander.trading_system = SimpleNamespace(
        get_top_ranked_opportunities=lambda limit=5, refresh=False: [
            {
                "asset": "XAU/USD",
                "direction": "BUY",
                "opportunity_score": 0.88,
                "confidence": 0.76,
                "playbook_name": "opening_drive",
                "playbook_entry_style": "opening_drive_break",
                "session_label": "us open",
                "playbook_timeframe": "5m",
                "trade_management_plan": {
                    "partial_take_profit_rr": [1.0],
                    "runner_target_rr": 1.9,
                    "trail_activation_rr": 0.85,
                    "trail_atr_multiple": 0.75,
                },
                "memory_score": 67.0,
                "execution_quality_score": 64.0,
                "broker_quality_score": 0.91,
                "broker_primary_provider": "IG",
                "broker_agreement_state": "strong",
                "broker_quote_quality_state": "fresh",
                "broker_spread_regime": "tight",
                "microstructure_score": 0.73,
                "depth_available": True,
                "synthetic_depth_available": False,
                "cross_asset_state": "buy support",
                "cross_asset_primary_peer": "XAG/USD",
                "source": "signal",
            }
        ]
    )

    text, _kb = commander._build_top_setups(refresh=True)

    assert "Top Playbook Opportunities" in text
    assert "Broker `0.91`" in text
    assert "Micro `0.73`" in text
    assert "Depth `True depth`" in text
    assert "Cross-asset `buy support` via `XAG/USD`" in text
    assert "Provider `IG` | strong / fresh / tight" in text
    assert "Playbook `opening drive` | `opening drive break`" in text
    assert "Session `us open` | TF `5m`" in text

def test_telegram_build_signal_includes_runtime_diagnostics() -> None:
    tg_mod = importlib.import_module("telegram_commander")

    commander = object.__new__(tg_mod.TelegramCommander)
    commander.trading_system = SimpleNamespace(
        get_signal_for_asset=lambda asset: {
            "asset": asset,
            "direction": "BUY",
            "entry_price": 2312.5,
            "stop_loss": 2298.0,
            "take_profit": 2345.0,
            "confidence": 0.74,
            "risk_reward": 2.24,
            "strategy_id": "playbook_breakout_continuation",
            "metadata": {
                "playbook_name": "breakout_continuation",
                "playbook_entry_style": "breakout_close",
                "playbook_timeframe": "5m",
                "session_label": "europe open",
                "trade_management_plan": {
                    "partial_take_profit_rr": [1.0],
                    "runner_target_rr": 2.4,
                    "trail_activation_rr": 1.0,
                    "trail_atr_multiple": 0.85,
                },
                "regime": "trend_following",
                "session": "london",
                "broker_quality": {
                    "score": 0.86,
                    "primary_provider": "IG",
                    "quote_agreement_state": "strong",
                    "quote_quality_state": "fresh",
                    "spread_regime": "tight",
                },
                "market_microstructure": {
                    "score": 0.42,
                    "depth_available": True,
                    "synthetic_depth_available": False,
                },
                "cross_asset_context": {
                    "state": "buy_support",
                    "dominant_peer": "XAG/USD",
                },
                "adaptive_policy": {
                    "recent_review_profile": {
                        "notes": ["recent_pattern_true_depth_winners"],
                    }
                },
            },
        }
    )

    text, _ = asyncio.run(commander._build_signal("XAU/USD"))

    assert "Diagnostics" in text
    assert "Playbook `breakout continuation` | `breakout close`" in text
    assert "Session `europe open` | TF `5m`" in text
    assert "Manage `TP1 1.0R | Runner 2.4R | Trail 1.0R · ATRx0.85`" in text
    assert "Broker `0.86` | `IG` | strong / fresh / tight" in text
    assert "Micro `0.42` | Depth `True depth`" in text
    assert "Spillover `buy support` via `XAG/USD`" in text
    assert "Pattern `true depth winners`" in text

def test_telegram_build_positions_includes_runtime_diagnostics() -> None:
    tg_mod = importlib.import_module("telegram_commander")

    commander = object.__new__(tg_mod.TelegramCommander)
    commander.trading_system = SimpleNamespace(
        fetcher=None,
        get_positions=lambda: [
            {
                "asset": "BNB-USD",
                "category": "crypto",
                "direction": "BUY",
                "entry_price": 591.07,
                "current_price": 589.53,
                "stop_loss": 588.42,
                "take_profit": 594.72,
                "position_size": 0.1375,
                "confidence": 0.61,
                "trade_id": "trade-1",
                "open_time": "2026-04-05T15:40:05+00:00",
                "metadata": {
                    "playbook_name": "crypto_orderflow_continuation",
                    "playbook_entry_style": "orderflow_break",
                    "playbook_timeframe": "15m",
                    "session_label": "us core",
                    "trade_management_plan": {
                        "partial_take_profit_rr": [1.0],
                        "runner_target_rr": 3.0,
                        "trail_activation_rr": 1.15,
                        "trail_atr_multiple": 1.18,
                    },
                    "broker_quality": {
                        "score": 0.76,
                        "primary_provider": "Binance",
                        "quote_agreement_state": "unconfirmed",
                        "quote_quality_state": "aging",
                    },
                    "market_microstructure": {
                        "score": 0.19,
                        "depth_available": True,
                        "synthetic_depth_available": False,
                    },
                    "cross_asset_context": {
                        "state": "conflicted",
                        "dominant_peer": "ETH-USD",
                    },
                    "adaptive_policy": {
                        "recent_review_profile": {
                            "notes": ["recent_pattern_premature_stop"],
                        }
                    },
                },
            }
        ],
    )

    text, _ = asyncio.run(commander._build_positions())

    assert "Playbook `crypto orderflow continuation` | `orderflow break`" in text
    assert "Session `us core` | TF `15m`" in text
    assert "Broker `0.76` | `Binance` | unconfirmed / aging" in text
    assert "Micro `0.19` | Depth `True depth`" in text
    assert "Spillover `conflicted` via `ETH-USD`" in text
    assert "Pattern `premature stop`" in text

def test_telegram_trade_history_context_uses_review_diagnostics() -> None:
    tg_mod = importlib.import_module("telegram_commander")

    text = tg_mod.TelegramCommander._format_trade_history_context(
        {
            "metadata": {
                "playbook_name": "failed_break_reclaim",
                "playbook_entry_style": "reclaim_failure",
                "playbook_timeframe": "5m",
                "session_label": "us overlap",
                "trade_management_plan": {
                    "partial_take_profit_rr": [1.0],
                    "runner_target_rr": 2.2,
                    "trail_activation_rr": 0.9,
                    "trail_atr_multiple": 0.7,
                },
                "post_trade_review": {
                    "entry_diagnostics": {
                        "broker_context": "fragile",
                        "depth_mode": "synthetic_depth",
                        "cross_asset_context": "conflicted",
                        "cross_asset_primary_peer": "WTI",
                    }
                }
            }
        }
    )

    assert "Playbook `failed break reclaim` | `reclaim failure`" in text
    assert "Session `us overlap` | TF `5m`" in text
    assert "broker fragile" in text
    assert "synthetic depth" in text
    assert "conflicted via WTI" in text

def test_robbie_explainer_confidence_question_uses_signal_not_mood() -> None:
    personality_mod = importlib.import_module("services.personality_service")

    explainer = object.__new__(personality_mod.RobbieExplainer)
    explainer.db = SimpleNamespace(
        get_personality_report=lambda: {
            "current_mood": "cautious",
            "mood_emoji": "🤔",
            "stats": {"consecutive_wins": 0, "consecutive_losses": 2, "weekly_win_rate": 45, "last_10_wins": 4, "last_10_pnl": -12.0},
        },
        get_asset_memory=lambda asset: {
            "has_memory": True,
            "total_trades": 6,
            "win_rate": 67,
            "wins": 4,
            "losses": 2,
            "total_pnl": 88.4,
            "avg_confidence": 71,
            "last_seen": "2026-04-05",
            "last_pnl": 24.1,
        },
    )

    text = explainer.answer(
        "BTC-USD",
        "How confident are you about BTC-USD?",
        signal={
            "direction": "BUY",
            "confidence": 0.71,
            "risk_reward": 1.8,
            "metadata": {
                "governance_grade": "A",
                "execution_quality_score": 64.0,
            },
        },
        df=None,
    )

    assert "Current confidence on *BTC-USD* is *71%*" in text
    assert "reward to risk is `1.80:1`" in text
    assert "Right now I'm feeling" not in text

def test_robbie_explainer_sentiment_question_uses_live_signal_metadata() -> None:
    personality_mod = importlib.import_module("services.personality_service")

    explainer = object.__new__(personality_mod.RobbieExplainer)
    explainer.db = SimpleNamespace(
        get_personality_report=lambda: {
            "current_mood": "neutral",
            "mood_emoji": "😐",
            "stats": {"consecutive_wins": 0, "consecutive_losses": 0, "weekly_win_rate": 50, "last_10_wins": 5, "last_10_pnl": 0.0},
        },
        get_asset_memory=lambda asset: {"has_memory": False},
    )

    text = explainer.answer(
        "BTC-USD",
        "What is the sentiment for BTC-USD?",
        signal={
            "direction": "SELL",
            "confidence": 0.63,
            "metadata": {
                "sentiment_score": -0.137,
                "sentiment_sources": ["comprehensive_sentiment", "reddit", "narrative_ai"],
                "narrative": "AI_TOKENS",
                "whale_dominant": "SELL",
            },
        },
        df=None,
    )

    assert "*Sentiment read on BTC-USD:*" in text
    assert "Sentiment leans bearish at -0.14." in text
    assert "Whale flow currently leans bearish." in text
    assert "AI-related crypto narrative" in text
    assert "3 sources" in text

def test_telegram_alert_trade_closed_includes_post_trade_review() -> None:
    tg_mod = importlib.import_module("telegram_commander")

    commander = object.__new__(tg_mod.TelegramCommander)
    captured = {}
    commander.send_message = lambda text, parse_mode=tg_mod.ParseMode.MARKDOWN, reply_markup=None: captured.setdefault("text", text) or True

    commander.alert_trade_closed(
        {
            "asset": "BTC-USD",
            "entry_price": 66345.677,
            "exit_price": 66611.0597,
            "pnl": -48.23,
            "exit_reason": "Stop Loss",
            "open_time": "2026-04-04T10:00:00+00:00",
            "exit_time": "2026-04-04T10:37:00+00:00",
            "duration_minutes": 37,
            "metadata": {
                "post_trade_review": {
                    "outcome": "loss",
                    "summary": "The entry arrived late and the stop was too tight for the volatility.",
                    "lesson": "Do not chase extended entries; wait for fresher structure or better price location.",
                    "next_focus": "Avoid chasing entries after the move is already mature.",
                    "what_went_wrong": [
                        "The entry arrived late, so the trade took heat before it had enough room to work.",
                        "The stop appears to have been too tight for the amount of normal market noise.",
                    ],
                    "avoid": [
                        "Avoid chasing entries after the move is already mature.",
                        "Avoid cramped stops when volatility is still noisy around entry.",
                    ],
                }
            },
        }
    )

    message = captured["text"]
    assert "Trade Review" in message
    assert "What went wrong" in message
    assert "What I'll avoid" in message
    assert "Do not chase extended entries" in message
    assert "04 Apr 2026 10:00:00 UTC" in message
    assert "04 Apr 2026 10:37:00 UTC" in message
    assert "37m" in message

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

def test_websocket_manager_filters_ig_routed_assets_before_tracking(monkeypatch) -> None:
    ws_mod = importlib.import_module("websocket_manager")

    manager = ws_mod.WebSocketManager()
    scheduled = []
    infos = []

    monkeypatch.setattr(ws_mod, "filter_deriv_stream_assets", lambda assets: {"EUR/USD": "forex"}, raising=False)
    monkeypatch.setattr(ws_mod, "filter_ig_primary_assets", lambda assets: {"XAU/USD": "commodities"}, raising=False)
    monkeypatch.setattr(
        manager,
        "_schedule",
        lambda coro: (scheduled.append(type(coro).__name__), getattr(coro, "close", lambda: None)()),
        raising=False,
    )
    monkeypatch.setattr(ws_mod.logger, "info", infos.append)

    manager.subscribe_deriv({"EUR/USD": "forex", "XAU/USD": "commodities"}, lambda *_args: None)

    assert manager._asset_categories == {"EUR/USD": "forex"}
    assert manager._stream_started is True
    assert scheduled
    assert any("Skipping IG-routed assets" in str(message) for message in infos)

def test_websocket_manager_does_not_start_for_ig_only_assets(monkeypatch) -> None:
    ws_mod = importlib.import_module("websocket_manager")

    manager = ws_mod.WebSocketManager()
    scheduled = []
    infos = []

    monkeypatch.setattr(ws_mod, "filter_deriv_stream_assets", lambda assets: {}, raising=False)
    monkeypatch.setattr(ws_mod, "filter_ig_primary_assets", lambda assets: {"XAU/USD": "commodities"}, raising=False)
    monkeypatch.setattr(
        manager,
        "_schedule",
        lambda coro: (scheduled.append(type(coro).__name__), getattr(coro, "close", lambda: None)()),
        raising=False,
    )
    monkeypatch.setattr(ws_mod.logger, "info", infos.append)

    manager.subscribe_deriv({"XAU/USD": "commodities"}, lambda *_args: None)

    assert manager._asset_categories == {}
    assert manager._stream_started is False
    assert scheduled == []
    assert any("No Deriv/Binance stream assets" in str(message) for message in infos)

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

def test_exchange_stream_bybit_uses_linear_endpoint_and_app_heartbeat() -> None:
    ex_mod = importlib.import_module("data_ingestion.exchange_stream_manager")

    assert ex_mod.EXCHANGE_WS_URLS["bybit"].endswith("/public/linear")
    assert "allLiquidation.BTCUSDT" in ex_mod.SUBSCRIPTIONS["bybit"]["args"]
    assert ex_mod._APP_HEARTBEAT_PAYLOADS["bybit"] == {"op": "ping"}
    assert ex_mod._RUN_FOREVER_KWARGS["bybit"]["ping_interval"] == 0

def test_exchange_stream_binance_disables_client_ping_loop() -> None:
    ex_mod = importlib.import_module("data_ingestion.exchange_stream_manager")

    assert ex_mod._RUN_FOREVER_KWARGS["binance"]["ping_interval"] == 0
    assert ex_mod._RUN_FOREVER_KWARGS["binance"]["ping_timeout"] is None

def test_exchange_stream_normalises_bybit_all_liquidations() -> None:
    ex_mod = importlib.import_module("data_ingestion.exchange_stream_manager")

    events = ex_mod._normalise_many(
        "bybit",
        {
            "topic": "allLiquidation.BTCUSDT",
            "data": [
                {"s": "BTCUSDT", "S": "Sell", "v": "1.25", "p": "70000", "T": 1234567890},
                {"s": "BTCUSDT", "S": "Buy", "v": "0.50", "p": "69950", "T": 1234567891},
            ],
        },
    )

    assert [event["type"] for event in events] == ["LIQUIDATION_EVENT", "LIQUIDATION_EVENT"]
    assert events[0]["asset"] == "BTCUSDT"
    assert events[0]["qty"] == 1.25
    assert events[0]["price"] == 70000.0
    assert events[1]["side"] == "Buy"

def test_exchange_stream_short_session_is_treated_as_failure() -> None:
    ex_mod = importlib.import_module("data_ingestion.exchange_stream_manager")

    failure = ex_mod._post_run_failure_message(
        running=True,
        last_error=None,
        session_age=12.5,
        close_code=1006,
        close_msg="abnormal closure",
    )

    assert "connection closed after 12.5s" in failure
    assert "1006" in failure

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

def test_chart_api_surfaces_provider_error_when_wti_data_is_denied(monkeypatch) -> None:
    dashboard_mod = importlib.import_module("dashboard.web_app_live")

    class _FakeFetcher:
        def get_ohlcv(self, asset, category, interval="15m", periods=100):
            assert asset == "WTI"
            assert category == "commodities"
            return None

        def get_last_ohlcv_metadata(self, asset, interval):
            return {
                "source": "IG",
                "provider_error_code": "missing_credentials",
                "provider_error_message": "IG_IDENTIFIER and IG_PASSWORD are required for IG commodity data.",
            }

    monkeypatch.setattr(dashboard_mod, "_DEVELOPMENT_MODE", True, raising=False)
    monkeypatch.setattr(dashboard_mod, "_get_fetcher", lambda: _FakeFetcher(), raising=False)

    client = dashboard_mod.app.test_client()
    response = client.get("/api/chart/candles?asset=WTI&interval=15m")
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["success"] is True
    assert payload["candles"] == []
    assert payload["data_source"] == "IG"
    assert payload["provider_error_code"] == "missing_credentials"
    assert "IG_IDENTIFIER and IG_PASSWORD" in payload["message"]

def test_correlation_matrix_api_uses_pairwise_overlap(monkeypatch) -> None:
    dashboard_mod = importlib.import_module("dashboard.web_app_live")

    idx_a = pd.date_range("2026-03-01 00:00:00", periods=15, freq="1D", tz="UTC")
    idx_b = pd.date_range("2026-03-01 12:00:00", periods=15, freq="1D", tz="UTC")
    frame_a = pd.DataFrame({"close": np.linspace(100, 119, 15)}, index=idx_a)
    frame_b = pd.DataFrame({"close": np.linspace(200, 238, 15)}, index=idx_b)

    class _FakeFetcher:
        def get_ohlcv(self, asset, category, interval="15m", periods=50):
            if asset == "EUR/USD":
                return frame_a
            if asset == "GBP/USD":
                return frame_b
            return None

        def get_last_ohlcv_metadata(self, asset, interval):
            return {"source": "test"}

    monkeypatch.setattr(dashboard_mod, "ALL_ASSETS", [("EUR/USD", "forex"), ("GBP/USD", "forex")], raising=False)
    monkeypatch.setattr(dashboard_mod, "_CAT", {"EUR/USD": "forex", "GBP/USD": "forex"}, raising=False)
    monkeypatch.setattr(dashboard_mod, "_get_fetcher", lambda: _FakeFetcher(), raising=False)
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

def test_correlation_matrix_api_uses_shared_daily_interval_and_preserves_unknown_pairs(monkeypatch) -> None:
    dashboard_mod = importlib.import_module("dashboard.web_app_live")

    idx_fx = pd.date_range("2026-03-01 00:00:00", periods=15, freq="1D", tz="UTC")
    idx_wti = pd.date_range("2026-03-01 22:00:00", periods=15, freq="1D", tz="UTC")
    idx_flat = pd.date_range("2026-03-01 00:00:00", periods=15, freq="1D", tz="UTC")

    frame_fx = pd.DataFrame({"close": np.linspace(100.0, 115.0, 15)}, index=idx_fx)
    frame_wti = pd.DataFrame({"close": np.linspace(50.0, 65.0, 15)}, index=idx_wti)
    frame_flat = pd.DataFrame({"close": np.full(15, 200.0)}, index=idx_flat)
    requests: list[tuple[str, str, int]] = []

    class _FakeFetcher:
        def get_ohlcv(self, asset, category, interval="15m", periods=50, closed_only=False):
            requests.append((asset, interval, periods))
            if asset == "EUR/USD":
                return frame_fx
            if asset == "WTI":
                return frame_wti
            if asset == "US500":
                return frame_flat
            return None

        def get_last_ohlcv_metadata(self, asset, interval):
            return {"source": "test"}

    monkeypatch.setattr(
        dashboard_mod,
        "ALL_ASSETS",
        [("EUR/USD", "forex"), ("WTI", "commodities"), ("US500", "indices")],
        raising=False,
    )
    monkeypatch.setattr(dashboard_mod, "_CAT", {"EUR/USD": "forex", "WTI": "commodities", "US500": "indices"}, raising=False)
    monkeypatch.setattr(dashboard_mod, "_get_fetcher", lambda: _FakeFetcher(), raising=False)
    monkeypatch.setattr(dashboard_mod, "_cache_get", lambda key: None, raising=False)
    monkeypatch.setattr(dashboard_mod, "_cache_set", lambda key, value, ttl=30: None, raising=False)

    client = dashboard_mod.app.test_client()
    response = client.get("/api/correlation-matrix")
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["success"] is True
    assert payload["interval"] == "1d"
    assert set(payload["labels"]) == {"EUR/USD", "WTI", "US500"}
    assert all(interval == "1d" for _, interval, _ in requests)

    index = {label: idx for idx, label in enumerate(payload["labels"])}
    assert payload["matrix"][index["EUR/USD"]][index["WTI"]] is not None
    assert payload["matrix"][index["WTI"]][index["EUR/USD"]] is not None
    assert payload["matrix"][index["US500"]][index["EUR/USD"]] is None
    assert payload["matrix"][index["EUR/USD"]][index["US500"]] is None

def test_correlation_matrix_api_falls_back_to_hourly_when_daily_omits_routed_wti(monkeypatch) -> None:
    dashboard_mod = importlib.import_module("dashboard.web_app_live")

    idx_fx_daily = pd.date_range("2026-03-01 00:00:00", periods=15, freq="1D", tz="UTC")
    idx_fx_hourly = pd.date_range("2026-04-05 00:00:00", periods=48, freq="1h", tz="UTC")
    idx_wti_hourly = pd.date_range("2026-04-05 00:00:00", periods=48, freq="1h", tz="UTC")

    frame_fx_daily = pd.DataFrame({"close": np.linspace(100.0, 115.0, 15)}, index=idx_fx_daily)
    frame_fx_hourly = pd.DataFrame({"close": np.linspace(100.0, 108.0, 48)}, index=idx_fx_hourly)
    frame_wti_hourly = pd.DataFrame({"close": np.linspace(60.0, 68.0, 48)}, index=idx_wti_hourly)

    class _FakeFetcher:
        def get_ohlcv(self, asset, category, interval="15m", periods=50, closed_only=False):
            if interval == "1d":
                if asset == "EUR/USD":
                    return frame_fx_daily
                return None
            if interval == "1h":
                if asset == "EUR/USD":
                    return frame_fx_hourly
                return None
            return None

        def get_last_ohlcv_metadata(self, asset, interval):
            if asset == "WTI":
                return {
                    "source": "IG",
                    "provider_error_code": "error.public-api.exceeded-account-historical-data-allowance",
                    "provider_error_message": "Exceeded account historical data allowance",
                }
            return {"source": "Deriv"}

    monkeypatch.setattr(
        dashboard_mod,
        "ALL_ASSETS",
        [("EUR/USD", "forex"), ("WTI", "commodities")],
        raising=False,
    )
    monkeypatch.setattr(dashboard_mod, "_CAT", {"EUR/USD": "forex", "WTI": "commodities"}, raising=False)
    monkeypatch.setattr(dashboard_mod, "_get_fetcher", lambda: _FakeFetcher(), raising=False)
    monkeypatch.setattr(dashboard_mod, "_cache_get", lambda key: None, raising=False)
    monkeypatch.setattr(dashboard_mod, "_cache_set", lambda key, value, ttl=30: None, raising=False)
    monkeypatch.setattr(
        dashboard_mod,
        "_stream_candles_from_live_feed",
        lambda asset, interval, periods, source_hint="IG": frame_wti_hourly if asset == "WTI" and interval == "1h" else None,
        raising=False,
    )

    client = dashboard_mod.app.test_client()
    response = client.get("/api/correlation-matrix")
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["success"] is True
    assert payload["interval"] == "1h"
    assert payload["available_assets"] == 2
    assert payload["labels"] == ["EUR/USD", "WTI"]
    assert payload["matrix"][0][1] is not None
    assert payload["matrix"][1][0] is not None

def test_correlation_matrix_api_preserves_dukascopy_history_for_routed_commodity(monkeypatch) -> None:
    dashboard_mod = importlib.import_module("dashboard.web_app_live")
    config_mod = importlib.import_module("config.config")

    idx = pd.date_range("2026-03-01 00:00:00", periods=20, freq="1D", tz="UTC")
    frame_fx = pd.DataFrame({"close": np.linspace(100.0, 119.0, 20)}, index=idx)
    frame_xau = pd.DataFrame({"close": np.linspace(2200.0, 2238.0, 20)}, index=idx)

    class _FakeFetcher:
        def get_ohlcv(self, asset, category, interval="15m", periods=50, closed_only=False):
            if interval == "1d":
                if asset == "EUR/USD":
                    return frame_fx
                if asset == "XAU/USD":
                    return frame_xau
            return None

        def get_last_ohlcv_metadata(self, asset, interval):
            if asset == "XAU/USD":
                return {"source": "Dukascopy", "source_class": "secondary_api"}
            return {"source": "Deriv", "source_class": "primary_api"}

    monkeypatch.setattr(config_mod, "IG_ENABLED", True, raising=False)
    monkeypatch.setattr(config_mod, "IG_ROUTED_CATEGORIES", ["commodities"], raising=False)
    monkeypatch.setattr(
        dashboard_mod,
        "ALL_ASSETS",
        [("EUR/USD", "forex"), ("XAU/USD", "commodities")],
        raising=False,
    )
    monkeypatch.setattr(dashboard_mod, "_CAT", {"EUR/USD": "forex", "XAU/USD": "commodities"}, raising=False)
    monkeypatch.setattr(dashboard_mod, "_get_fetcher", lambda: _FakeFetcher(), raising=False)
    monkeypatch.setattr(dashboard_mod, "_cache_get", lambda key: None, raising=False)
    monkeypatch.setattr(dashboard_mod, "_cache_set", lambda key, value, ttl=30: None, raising=False)
    monkeypatch.setattr(
        dashboard_mod,
        "_stream_candles_from_live_feed",
        lambda asset, interval, periods, source_hint="IG": None,
        raising=False,
    )

    client = dashboard_mod.app.test_client()
    response = client.get("/api/correlation-matrix")
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["success"] is True
    assert payload["interval"] == "1d"
    index_lookup = {label: idx for idx, label in enumerate(payload["labels"])}
    assert payload["matrix"][index_lookup["EUR/USD"]][index_lookup["XAU/USD"]] is not None
    assert payload["matrix"][index_lookup["XAU/USD"]][index_lookup["EUR/USD"]] is not None

def test_backtest_multi_asset_api_uses_registry_universe(monkeypatch) -> None:
    dashboard_mod = importlib.import_module("dashboard.web_app_live")
    assets_mod = importlib.import_module("core.assets")

    monkeypatch.setattr(dashboard_mod, "_DEVELOPMENT_MODE", True, raising=False)
    monkeypatch.setattr(dashboard_mod, "_AUTH_CONFIG_ERROR", "", raising=False)
    monkeypatch.setattr(
        assets_mod.registry,
        "all_assets",
        lambda: [("EUR/USD", "forex"), ("WTI", "commodities"), ("US500", "indices")],
        raising=False,
    )

    client = dashboard_mod.app.test_client()
    response = client.get("/api/backtest/multi-asset?strategy=alpha")
    payload = response.get_json()

    assert response.status_code == 409
    assert payload["success"] is False
    assert payload["disabled"] is True
    assert payload["mode"] == "playbook_only"

def test_dashboard_api_fails_closed_when_prod_auth_is_unconfigured(monkeypatch) -> None:
    dashboard_mod = importlib.import_module("dashboard.web_app_live")

    monkeypatch.setattr(dashboard_mod, "_DEVELOPMENT_MODE", False, raising=False)
    monkeypatch.setattr(
        dashboard_mod,
        "_AUTH_CONFIG_ERROR",
        "DASHBOARD_API_KEY is required when DEVELOPMENT_MODE=false",
        raising=False,
    )
    monkeypatch.setattr(dashboard_mod, "_API_KEY_HASH", None, raising=False)

    client = dashboard_mod.app.test_client()
    response = client.get("/api/status")
    payload = response.get_json()

    assert response.status_code == 503
    assert payload["success"] is False
    assert "DASHBOARD_API_KEY" in payload["error"]

def test_dashboard_login_requires_api_key_in_prod_mode(monkeypatch) -> None:
    dashboard_mod = importlib.import_module("dashboard.web_app_live")

    monkeypatch.setattr(dashboard_mod, "_DEVELOPMENT_MODE", False, raising=False)
    monkeypatch.setattr(dashboard_mod, "_AUTH_CONFIG_ERROR", "", raising=False)
    monkeypatch.setattr(dashboard_mod, "_API_KEY_HASH", hashlib.sha256(b"secret-key").hexdigest(), raising=False)

    client = dashboard_mod.app.test_client()
    response = client.post("/api/login", json={})
    payload = response.get_json()

    assert response.status_code == 400
    assert payload["success"] is False
    assert payload["error"] == "Dashboard API key is required"

def test_dashboard_login_issues_token_with_valid_api_key(monkeypatch) -> None:
    dashboard_mod = importlib.import_module("dashboard.web_app_live")

    monkeypatch.setattr(dashboard_mod, "_DEVELOPMENT_MODE", False, raising=False)
    monkeypatch.setattr(dashboard_mod, "_AUTH_CONFIG_ERROR", "", raising=False)
    monkeypatch.setattr(dashboard_mod, "_API_KEY_HASH", hashlib.sha256(b"secret-key").hexdigest(), raising=False)
    monkeypatch.setattr(dashboard_mod, "_SESSION_TOKEN_TTL", 60, raising=False)
    with dashboard_mod._SESSION_TOKEN_LOCK:
        dashboard_mod._SESSION_TOKENS.clear()

    client = dashboard_mod.app.test_client()
    response = client.post("/api/login", json={"api_key": "secret-key"})
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["success"] is True
    assert payload["mode"] == "prod"
    assert payload["token"]

def test_run_hypercorn_server_wraps_flask_with_asyncio_wsgi_middleware(monkeypatch) -> None:
    dashboard_mod = importlib.import_module("dashboard.web_app_live")

    served: dict[str, object] = {}

    class _FakeConfig:
        def __init__(self):
            self.bind = []
            self.worker_class = None
            self.loglevel = None
            self.keep_alive_timeout = None
            self.alpn_protocols = None
            self.certfile = None
            self.keyfile = None

    class _FakeMiddleware:
        def __init__(self, app):
            self.app = app

    class _FakeWrapper:
        def __init__(self, app, max_body_size=None):
            self.app = app
            self.max_body_size = max_body_size

        def run_app(self, environ, send):
            return None

    async def _fake_serve(app, config):
        served["app"] = app
        served["config"] = config

    fake_hypercorn = ModuleType("hypercorn")
    fake_app_wrappers = ModuleType("hypercorn.app_wrappers")
    fake_app_wrappers.WSGIWrapper = _FakeWrapper
    fake_hypercorn.app_wrappers = fake_app_wrappers

    monkeypatch.setitem(sys.modules, "hypercorn", fake_hypercorn)
    monkeypatch.setitem(sys.modules, "hypercorn.app_wrappers", fake_app_wrappers)
    monkeypatch.setitem(sys.modules, "hypercorn.config", SimpleNamespace(Config=_FakeConfig, Sockets=object))
    monkeypatch.setitem(sys.modules, "hypercorn.asyncio", SimpleNamespace(serve=_fake_serve))
    monkeypatch.setitem(
        sys.modules,
        "hypercorn.middleware.wsgi",
        SimpleNamespace(AsyncioWSGIMiddleware=_FakeMiddleware),
    )

    ok = dashboard_mod._run_hypercorn_server("127.0.0.1", 5000)

    assert ok is True
    assert isinstance(served["app"], _FakeMiddleware)
    assert served["app"].app is dashboard_mod.app
    assert served["config"].bind == ["127.0.0.1:5000"]

def test_run_hypercorn_server_patches_empty_wsgi_responses(monkeypatch) -> None:
    dashboard_mod = importlib.import_module("dashboard.web_app_live")

    served: dict[str, object] = {}

    class _FakeConfig:
        def __init__(self):
            self.bind = []
            self.worker_class = None
            self.loglevel = None
            self.keep_alive_timeout = None
            self.alpn_protocols = None
            self.certfile = None
            self.keyfile = None

    class _FakeMiddleware:
        def __init__(self, app):
            self.app = app

    class _FakeWrapper:
        def __init__(self, app, max_body_size=None):
            self.app = app
            self.max_body_size = max_body_size

        def run_app(self, environ, send):
            return None

    async def _fake_serve(app, config):
        served["app"] = app
        served["config"] = config

    fake_hypercorn = ModuleType("hypercorn")
    fake_app_wrappers = ModuleType("hypercorn.app_wrappers")
    fake_app_wrappers.WSGIWrapper = _FakeWrapper
    fake_hypercorn.app_wrappers = fake_app_wrappers

    monkeypatch.setitem(sys.modules, "hypercorn", fake_hypercorn)
    monkeypatch.setitem(sys.modules, "hypercorn.app_wrappers", fake_app_wrappers)
    monkeypatch.setitem(sys.modules, "hypercorn.config", SimpleNamespace(Config=_FakeConfig, Sockets=object))
    monkeypatch.setitem(sys.modules, "hypercorn.asyncio", SimpleNamespace(serve=_fake_serve))
    monkeypatch.setitem(
        sys.modules,
        "hypercorn.middleware.wsgi",
        SimpleNamespace(AsyncioWSGIMiddleware=_FakeMiddleware),
    )

    ok = dashboard_mod._run_hypercorn_server("127.0.0.1", 5000)

    assert ok is True
    assert getattr(fake_app_wrappers.WSGIWrapper.run_app, "_robbie_empty_response_fix", False) is True

    wrapper = fake_app_wrappers.WSGIWrapper(
        lambda environ, start_response: (start_response("304 Not Modified", [("etag", "x")]) or []),
        max_body_size=0,
    )
    messages: list[dict[str, object]] = []
    wrapper.run_app({}, messages.append)

    assert messages == [
        {
            "type": "http.response.start",
            "status": 304,
            "headers": [(b"etag", b"x")],
        }
    ]

def test_heatmap_api_reports_partial_payload(monkeypatch) -> None:
    dashboard_mod = importlib.import_module("dashboard.web_app_live")

    monkeypatch.setattr(dashboard_mod, "_DEVELOPMENT_MODE", True, raising=False)
    monkeypatch.setattr(dashboard_mod, "_AUTH_CONFIG_ERROR", "", raising=False)

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

        def get_last_ohlcv_metadata(self, asset, interval):
            return {"source": "test"}

        def get_real_time_price(self, asset, category):
            return None, None

        def get_last_price_metadata(self, asset):
            return {"source": "test"}

    monkeypatch.setattr(dashboard_mod, "_get_fetcher", lambda: _FakeFetcher(), raising=False)

    client = dashboard_mod.app.test_client()
    response = client.get("/api/market/heatmap")
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["success"] is True
    assert payload["expected_assets"] == 3
    assert payload["partial"] is True
    assert len(payload["items"]) == 2

def test_heatmap_api_keeps_wti_when_daily_history_needs_fallback(monkeypatch) -> None:
    dashboard_mod = importlib.import_module("dashboard.web_app_live")

    monkeypatch.setattr(dashboard_mod, "_DEVELOPMENT_MODE", True, raising=False)
    monkeypatch.setattr(dashboard_mod, "_AUTH_CONFIG_ERROR", "", raising=False)
    monkeypatch.setattr(dashboard_mod, "ALL_ASSETS", [("WTI", "commodities")], raising=False)
    monkeypatch.setattr(dashboard_mod, "_CAT", {"WTI": "commodities"}, raising=False)
    monkeypatch.setattr(dashboard_mod, "_cache_get", lambda key: None, raising=False)
    monkeypatch.setattr(dashboard_mod, "_cache_set", lambda key, value, ttl=0: None, raising=False)
    monkeypatch.setattr(dashboard_mod, "_is_market_weekend", lambda category: False, raising=False)

    intraday = pd.DataFrame(
        {"close": [80.0, 81.0, 82.0]},
        index=pd.date_range("2026-04-05 00:00:00", periods=3, freq="12h", tz="UTC"),
    )
    requests: list[tuple[str, str, int]] = []

    class _FakeFetcher:
        def get_ohlcv(self, asset, category, interval="1d", periods=5, closed_only=False):
            requests.append((asset, interval, periods))
            if interval == "1d":
                return None
            if interval == "1h":
                return intraday
            return None

        def get_last_ohlcv_metadata(self, asset, interval):
            if interval == "1d":
                return {
                    "source": "IG",
                    "provider_error_code": "error.public-api.exceeded-account-historical-data-allowance",
                    "provider_error_message": "Exceeded account historical data allowance",
                }
            return {"source": "IG"}

        def get_real_time_price(self, asset, category):
            return 83.0, 0.0

        def get_last_price_metadata(self, asset):
            return {"source": "IG"}

    monkeypatch.setattr(dashboard_mod, "_get_fetcher", lambda: _FakeFetcher(), raising=False)

    client = dashboard_mod.app.test_client()
    response = client.get("/api/market/heatmap")
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["success"] is True
    assert payload["expected_assets"] == 1
    assert payload["partial"] is False
    assert [item["asset"] for item in payload["items"]] == ["WTI"]
    assert ("WTI", "1d", 2) in requests
    assert ("WTI", "1h", 30) in requests

def test_heatmap_api_keeps_wti_with_ig_stream_only(monkeypatch) -> None:
    dashboard_mod = importlib.import_module("dashboard.web_app_live")

    monkeypatch.setattr(dashboard_mod, "_DEVELOPMENT_MODE", True, raising=False)
    monkeypatch.setattr(dashboard_mod, "_AUTH_CONFIG_ERROR", "", raising=False)
    monkeypatch.setattr(dashboard_mod, "ALL_ASSETS", [("WTI", "commodities")], raising=False)
    monkeypatch.setattr(dashboard_mod, "_CAT", {"WTI": "commodities"}, raising=False)
    monkeypatch.setattr(dashboard_mod, "_cache_get", lambda key: None, raising=False)
    monkeypatch.setattr(dashboard_mod, "_cache_set", lambda key, value, ttl=0: None, raising=False)
    monkeypatch.setattr(dashboard_mod, "_is_market_weekend", lambda category: False, raising=False)

    stream_frame = pd.DataFrame(
        {"close": [100.0, 101.0, 102.5]},
        index=pd.to_datetime(
            ["2026-04-05T00:00:00Z", "2026-04-05T12:00:00Z", "2026-04-06T00:00:00Z"],
            utc=True,
        ),
    )

    class _FakeFetcher:
        def get_ohlcv(self, asset, category, interval="1d", periods=5, closed_only=False):
            return None

        def get_last_ohlcv_metadata(self, asset, interval):
            return {
                "source": "IG",
                "provider_error_code": "error.public-api.exceeded-account-historical-data-allowance",
                "provider_error_message": "Exceeded account historical data allowance",
            }

        def get_real_time_price(self, asset, category):
            return 103.0, 0.0

        def get_last_price_metadata(self, asset):
            return {"source": "IG"}

    monkeypatch.setattr(dashboard_mod, "_get_fetcher", lambda: _FakeFetcher(), raising=False)
    monkeypatch.setattr(
        dashboard_mod,
        "_stream_candles_from_live_feed",
        lambda asset, interval, periods, source_hint="IG": stream_frame if asset == "WTI" and interval == "5m" else None,
        raising=False,
    )

    client = dashboard_mod.app.test_client()
    response = client.get("/api/market/heatmap")
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["success"] is True
    assert [item["asset"] for item in payload["items"]] == ["WTI"]
    assert payload["items"][0]["source"] == "IG"
    assert payload["items"][0]["change_pct"] is not None

def test_heatmap_api_preserves_fallback_history_for_routed_commodity(monkeypatch) -> None:
    dashboard_mod = importlib.import_module("dashboard.web_app_live")

    monkeypatch.setattr(dashboard_mod, "_DEVELOPMENT_MODE", True, raising=False)
    monkeypatch.setattr(dashboard_mod, "_AUTH_CONFIG_ERROR", "", raising=False)
    monkeypatch.setattr(dashboard_mod, "ALL_ASSETS", [("XAG/USD", "commodities")], raising=False)
    monkeypatch.setattr(dashboard_mod, "_CAT", {"XAG/USD": "commodities"}, raising=False)
    monkeypatch.setattr(dashboard_mod, "_cache_get", lambda key: None, raising=False)
    monkeypatch.setattr(dashboard_mod, "_cache_set", lambda key, value, ttl=0: None, raising=False)
    monkeypatch.setattr(dashboard_mod, "_is_market_weekend", lambda category: False, raising=False)

    daily = pd.DataFrame(
        {"open": [23.9, 24.1], "close": [24.1, 24.4]},
        index=pd.date_range("2026-04-05 00:00:00", periods=2, freq="1D", tz="UTC"),
    )
    stream_frame = pd.DataFrame(
        {"close": [99.0, 100.0, 101.0]},
        index=pd.to_datetime(
            ["2026-04-05T00:00:00Z", "2026-04-05T12:00:00Z", "2026-04-06T00:00:00Z"],
            utc=True,
        ),
    )

    class _FakeFetcher:
        def get_ohlcv(self, asset, category, interval="1d", periods=5, closed_only=False):
            if interval == "1d":
                return daily
            return None

        def get_last_ohlcv_metadata(self, asset, interval):
            return {"source": "Deriv"}

        def get_real_time_price(self, asset, category):
            return 24.5, 0.0

        def get_last_price_metadata(self, asset):
            return {"source": "IG"}

    monkeypatch.setattr(dashboard_mod, "_get_fetcher", lambda: _FakeFetcher(), raising=False)
    monkeypatch.setattr(
        dashboard_mod,
        "_stream_candles_from_live_feed",
        lambda asset, interval, periods, source_hint="IG": stream_frame,
        raising=False,
    )

    client = dashboard_mod.app.test_client()
    response = client.get("/api/market/heatmap")
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["success"] is True
    assert [item["asset"] for item in payload["items"]] == ["XAG/USD"]
    assert payload["items"][0]["source"] == "Deriv"
    assert payload["items"][0]["change_pct"] == round((24.5 - 24.1) / 24.1 * 100.0, 3)

def test_market_intelligence_page_overview_stays_lightweight(monkeypatch) -> None:
    dashboard_mod = importlib.import_module("dashboard.web_app_live")

    monkeypatch.setattr(dashboard_mod, "_DEVELOPMENT_MODE", True, raising=False)
    monkeypatch.setattr(dashboard_mod, "_AUTH_CONFIG_ERROR", "", raising=False)

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
        if name == "api_status":
            return _FakeResponse({"success": True, "provider_routing": {"summary_label": "Deriv 16, IG 3"}})
        if name == "api_market_events":
            return _FakeResponse({"success": True, "events": [], "risk_outlook": {}})
        raise AssertionError(f"Unexpected view {name}")

    monkeypatch.setattr(dashboard_mod, "_call_view", _fake_call_view, raising=False)

    with dashboard_mod.app.test_request_context("/api/page-overview?page=market_intelligence"):
        response = dashboard_mod.api_page_overview()

    payload = response.get_json()
    assert payload["success"] is True
    assert "assets" in payload
    assert "status" in payload
    assert "events" in payload
    assert "heatmap" not in payload

def test_chart_asset_descriptor_reflects_provider_routing(monkeypatch) -> None:
    dashboard_mod = importlib.import_module("dashboard.web_app_live")
    config_mod = importlib.import_module("config.config")

    monkeypatch.setattr(config_mod, "IG_ENABLED", True, raising=False)
    monkeypatch.setattr(config_mod, "IG_ROUTED_CATEGORIES", ["commodities"], raising=False)

    commodity = dashboard_mod._chart_asset_descriptor("WTI", "commodities")
    assert commodity["primary_provider"] == "IG"
    assert commodity["secondary_provider"] == "Deriv"
    assert commodity["quote_mode"] == "stream"

    crypto = dashboard_mod._chart_asset_descriptor("BNB-USD", "crypto")
    assert crypto["primary_provider"] == "Deriv"
    assert crypto["secondary_provider"] == "Binance"
    assert crypto["quote_mode"] == "stream"

    forex = dashboard_mod._chart_asset_descriptor("EUR/USD", "forex")
    assert forex["primary_provider"] == "Deriv"
    assert forex["secondary_provider"] == ""
    assert forex["quote_mode"] == "stream"

def test_api_chart_candles_includes_data_source_on_success(monkeypatch) -> None:
    import pandas as pd

    dashboard_mod = importlib.import_module("dashboard.web_app_live")

    monkeypatch.setattr(dashboard_mod, "_DEVELOPMENT_MODE", True, raising=False)
    monkeypatch.setattr(dashboard_mod, "_AUTH_CONFIG_ERROR", "", raising=False)

    class _FakeFetcher:
        def get_ohlcv(self, asset, category, interval="15m", periods=0):
            index = pd.to_datetime(["2026-04-05T00:00:00Z", "2026-04-05T00:15:00Z"], utc=True)
            return pd.DataFrame(
                {
                    "open": [100.0, 101.0],
                    "high": [101.0, 102.0],
                    "low": [99.0, 100.5],
                    "close": [100.5, 101.5],
                    "volume": [10.0, 12.0],
                },
                index=index,
            )

        def get_last_ohlcv_metadata(self, asset, interval):
            return {"source": "IG"}

    monkeypatch.setattr(dashboard_mod, "_fetcher", _FakeFetcher(), raising=False)

    client = dashboard_mod.app.test_client()
    response = client.get("/api/chart/candles?asset=WTI&interval=15m")
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["success"] is True
    assert len(payload["candles"]) == 2
    assert payload["data_source"] == "IG"

def test_api_chart_history_honours_requested_bars_and_end_time(monkeypatch) -> None:
    dashboard_mod = importlib.import_module("dashboard.web_app_live")

    monkeypatch.setattr(dashboard_mod, "_DEVELOPMENT_MODE", True, raising=False)
    monkeypatch.setattr(dashboard_mod, "_AUTH_CONFIG_ERROR", "", raising=False)
    monkeypatch.setattr(dashboard_mod, "_cache_get", lambda key: None, raising=False)
    monkeypatch.setattr(dashboard_mod, "_cache_set", lambda key, value, ttl=0: None, raising=False)

    calls = {}
    frame = pd.DataFrame(
        {
            "open": [1.2000, 1.2010, 1.2020],
            "high": [1.2010, 1.2020, 1.2030],
            "low": [1.1990, 1.2000, 1.2010],
            "close": [1.2005, 1.2015, 1.2025],
            "volume": [10.0, 11.0, 12.0],
        },
        index=pd.to_datetime(
            ["2026-04-01T00:00:00Z", "2026-04-01T01:00:00Z", "2026-04-01T02:00:00Z"],
            utc=True,
        ),
    )

    class _FakeFetcher:
        def get_ohlcv(self, asset, category, interval="1h", periods=0, end_time=None, closed_only=False):
            calls["asset"] = asset
            calls["category"] = category
            calls["interval"] = interval
            calls["periods"] = periods
            calls["end_time"] = end_time
            calls["closed_only"] = closed_only
            return frame

        def get_last_ohlcv_metadata(self, asset, interval):
            return {"source": "Dukascopy", "source_class": "secondary_api", "provider_family": "DUKASCOPY"}

    monkeypatch.setattr(dashboard_mod, "_fetcher", _FakeFetcher(), raising=False)

    client = dashboard_mod.app.test_client()
    response = client.get(
        "/api/chart/history?asset=EUR/USD&interval=1h&bars=500&end_time=2026-04-02T00:00:00Z"
    )
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["success"] is True
    assert payload["history_mode"] == "deep"
    assert payload["bars_requested"] == 500
    assert payload["bars_returned"] == 3
    assert payload["data_source"] == "Dukascopy"
    assert payload["oldest_time"] == payload["candles"][0]["time"]
    assert payload["next_end_time"] == payload["candles"][0]["time"] - 1
    assert calls["asset"] == "EUR/USD"
    assert calls["interval"] == "1h"
    assert calls["periods"] == 500
    assert calls["closed_only"] is True
    assert str(calls["end_time"]).startswith("2026-04-02 00:00:00")

def test_api_chart_candles_preserves_fallback_history_for_routed_commodity_when_available(monkeypatch) -> None:
    dashboard_mod = importlib.import_module("dashboard.web_app_live")
    config_mod = importlib.import_module("config.config")

    monkeypatch.setattr(dashboard_mod, "_DEVELOPMENT_MODE", True, raising=False)
    monkeypatch.setattr(dashboard_mod, "_AUTH_CONFIG_ERROR", "", raising=False)
    monkeypatch.setattr(config_mod, "IG_ENABLED", True, raising=False)
    monkeypatch.setattr(config_mod, "IG_ROUTED_CATEGORIES", ["commodities"], raising=False)
    monkeypatch.setattr(dashboard_mod, "_cache_get", lambda key: None, raising=False)
    monkeypatch.setattr(dashboard_mod, "_cache_set", lambda key, value, ttl=0: None, raising=False)

    deriv_frame = pd.DataFrame(
        {
            "open": [72.0, 72.1],
            "high": [72.2, 72.3],
            "low": [71.9, 72.0],
            "close": [72.1, 72.2],
            "volume": [10.0, 11.0],
        },
        index=pd.to_datetime(["2026-04-06T00:00:00Z", "2026-04-06T00:05:00Z"], utc=True),
    )
    stream_frame = pd.DataFrame(
        {
            "open": [24.0, 24.1],
            "high": [24.2, 24.3],
            "low": [23.9, 24.0],
            "close": [24.1, 24.2],
            "volume": [20.0, 22.0],
        },
        index=pd.to_datetime(["2026-04-06T00:00:00Z", "2026-04-06T00:05:00Z"], utc=True),
    )

    class _FakeFetcher:
        def get_ohlcv(self, asset, category, interval="15m", periods=0):
            return deriv_frame

        def get_last_ohlcv_metadata(self, asset, interval):
            return {"source": "Deriv"}

    monkeypatch.setattr(dashboard_mod, "_fetcher", _FakeFetcher(), raising=False)
    monkeypatch.setattr(
        dashboard_mod,
        "_stream_candles_from_live_feed",
        lambda asset, interval, periods, source_hint="IG": stream_frame if asset == "XAG/USD" else None,
        raising=False,
    )

    client = dashboard_mod.app.test_client()
    response = client.get("/api/chart/candles?asset=XAG/USD&interval=5m")
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["success"] is True
    assert payload["data_source"] == "Deriv"
    assert payload["candles"][0]["close"] == 72.1

def test_history_allows_live_overlay_for_local_store_when_provider_matches() -> None:
    dashboard_mod = importlib.import_module("dashboard.web_app_live")

    assert dashboard_mod._history_allows_live_overlay(
        {"primary_provider": "IG"},
        {"source": "LocalStore", "source_class": "local_store", "provider_family": "IG"},
    ) is True
    assert dashboard_mod._history_allows_live_overlay(
        {"primary_provider": "IG"},
        {
            "source": "LocalStore",
            "source_class": "local_store",
            "provider_family": "MIXED",
            "latest_provider_family": "IG",
            "latest_source_class": "stream_cache",
        },
    ) is True
    assert dashboard_mod._history_allows_live_overlay(
        {"primary_provider": "IG"},
        {"source": "LocalStore", "source_class": "local_store", "provider_family": "DUKASCOPY"},
    ) is False

def test_api_chart_candles_disables_live_overlay_for_cross_provider_history(monkeypatch) -> None:
    dashboard_mod = importlib.import_module("dashboard.web_app_live")
    config_mod = importlib.import_module("config.config")

    monkeypatch.setattr(dashboard_mod, "_DEVELOPMENT_MODE", True, raising=False)
    monkeypatch.setattr(dashboard_mod, "_AUTH_CONFIG_ERROR", "", raising=False)
    monkeypatch.setattr(config_mod, "IG_ENABLED", True, raising=False)
    monkeypatch.setattr(config_mod, "IG_ROUTED_CATEGORIES", ["commodities"], raising=False)
    monkeypatch.setattr(dashboard_mod, "_cache_get", lambda key: None, raising=False)
    monkeypatch.setattr(dashboard_mod, "_cache_set", lambda key, value, ttl=0: None, raising=False)

    duk_frame = pd.DataFrame(
        {
            "open": [2245.0, 2246.0],
            "high": [2247.0, 2248.0],
            "low": [2244.0, 2245.0],
            "close": [2246.5, 2247.5],
            "volume": [12.0, 15.0],
        },
        index=pd.to_datetime(["2026-04-06T00:00:00Z", "2026-04-06T00:30:00Z"], utc=True),
    )

    class _FakeFetcher:
        def get_ohlcv(self, asset, category, interval="30m", periods=0):
            return duk_frame

        def get_last_ohlcv_metadata(self, asset, interval):
            return {"source": "Dukascopy", "source_class": "secondary_api"}

    monkeypatch.setattr(dashboard_mod, "_fetcher", _FakeFetcher(), raising=False)

    client = dashboard_mod.app.test_client()
    response = client.get("/api/chart/candles?asset=XAU/USD&interval=30m")
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["success"] is True
    assert payload["data_source"] == "Dukascopy"
    assert payload["live_overlay_allowed"] is False
    assert payload["live_price_source"] == "IG"

def test_api_chart_candles_caps_ig_history_requests(monkeypatch) -> None:
    import pandas as pd

    dashboard_mod = importlib.import_module("dashboard.web_app_live")
    config_mod = importlib.import_module("config.config")

    monkeypatch.setattr(dashboard_mod, "_DEVELOPMENT_MODE", True, raising=False)
    monkeypatch.setattr(dashboard_mod, "_AUTH_CONFIG_ERROR", "", raising=False)
    monkeypatch.setattr(config_mod, "IG_ENABLED", True, raising=False)
    monkeypatch.setattr(config_mod, "IG_ROUTED_CATEGORIES", ["commodities"], raising=False)
    monkeypatch.setattr(dashboard_mod, "_cache_get", lambda key: None, raising=False)
    monkeypatch.setattr(dashboard_mod, "_cache_set", lambda key, value, ttl=0: None, raising=False)

    captured: dict[str, int] = {}

    class _FakeFetcher:
        def get_ohlcv(self, asset, category, interval="15m", periods=0):
            captured["periods"] = int(periods or 0)
            index = pd.to_datetime(["2026-04-05T00:00:00Z", "2026-04-05T00:05:00Z"], utc=True)
            return pd.DataFrame(
                {
                    "open": [100.0, 101.0],
                    "high": [101.0, 102.0],
                    "low": [99.0, 100.5],
                    "close": [100.5, 101.5],
                    "volume": [10.0, 12.0],
                },
                index=index,
            )

        def get_last_ohlcv_metadata(self, asset, interval):
            return {"source": "IG"}

    monkeypatch.setattr(dashboard_mod, "_fetcher", _FakeFetcher(), raising=False)

    client = dashboard_mod.app.test_client()
    response = client.get("/api/chart/candles?asset=WTI&interval=5m")
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["success"] is True
    assert payload["bars_requested"] == 240
    assert captured["periods"] == 240

def test_api_chart_candles_uses_last_good_cache_on_ig_allowance_error(monkeypatch) -> None:
    dashboard_mod = importlib.import_module("dashboard.web_app_live")
    config_mod = importlib.import_module("config.config")

    monkeypatch.setattr(dashboard_mod, "_DEVELOPMENT_MODE", True, raising=False)
    monkeypatch.setattr(dashboard_mod, "_AUTH_CONFIG_ERROR", "", raising=False)
    monkeypatch.setattr(config_mod, "IG_ENABLED", True, raising=False)
    monkeypatch.setattr(config_mod, "IG_ROUTED_CATEGORIES", ["commodities"], raising=False)

    cached_payload = {
        "success": True,
        "candles": [{"time": 1712275200, "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.5, "volume": 10.0}],
        "interval_used": "5m",
        "bars_requested": 240,
        "data_source": "IG",
        "cached": False,
    }

    def _fake_cache_get(key):
        if key.startswith("chart_candles_last:WTI:commodities:5m"):
            return dict(cached_payload)
        return None

    monkeypatch.setattr(dashboard_mod, "_cache_get", _fake_cache_get, raising=False)
    monkeypatch.setattr(dashboard_mod, "_cache_set", lambda key, value, ttl=0: None, raising=False)

    class _FakeFetcher:
        def get_ohlcv(self, asset, category, interval="15m", periods=0):
            return None

        def get_last_ohlcv_metadata(self, asset, interval):
            return {
                "source": "IG",
                "provider_error_code": "error.public-api.exceeded-account-historical-data-allowance",
                "provider_error_message": "error.public-api.exceeded-account-historical-data-allowance",
            }

    monkeypatch.setattr(dashboard_mod, "_fetcher", _FakeFetcher(), raising=False)

    client = dashboard_mod.app.test_client()
    response = client.get("/api/chart/candles?asset=WTI&interval=5m")
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["success"] is True
    assert payload["cached"] is True
    assert payload["candles"]
    assert payload["provider_warning_code"] == "error.public-api.exceeded-account-historical-data-allowance"

def test_api_chart_candles_retries_with_smaller_ig_window_on_allowance_error(monkeypatch) -> None:
    import pandas as pd

    dashboard_mod = importlib.import_module("dashboard.web_app_live")
    config_mod = importlib.import_module("config.config")

    monkeypatch.setattr(dashboard_mod, "_DEVELOPMENT_MODE", True, raising=False)
    monkeypatch.setattr(dashboard_mod, "_AUTH_CONFIG_ERROR", "", raising=False)
    monkeypatch.setattr(config_mod, "IG_ENABLED", True, raising=False)
    monkeypatch.setattr(config_mod, "IG_ROUTED_CATEGORIES", ["commodities"], raising=False)
    monkeypatch.setattr(dashboard_mod, "_cache_get", lambda key: None, raising=False)
    monkeypatch.setattr(dashboard_mod, "_cache_set", lambda key, value, ttl=0: None, raising=False)

    attempts: list[int] = []

    class _FakeFetcher:
        def __init__(self):
            self._meta = {}

        def get_ohlcv(self, asset, category, interval="15m", periods=0):
            attempts.append(int(periods or 0))
            if len(attempts) == 1:
                self._meta = {
                    "source": "IG",
                    "provider_error_code": "error.public-api.exceeded-account-historical-data-allowance",
                    "provider_error_message": "error.public-api.exceeded-account-historical-data-allowance",
                }
                return None
            self._meta = {"source": "IG"}
            index = pd.to_datetime(["2026-04-05T00:00:00Z", "2026-04-05T00:30:00Z"], utc=True)
            return pd.DataFrame(
                {
                    "open": [70.0, 70.5],
                    "high": [71.0, 71.2],
                    "low": [69.5, 70.2],
                    "close": [70.7, 70.9],
                    "volume": [10.0, 12.0],
                },
                index=index,
            )

        def get_last_ohlcv_metadata(self, asset, interval):
            return dict(self._meta)

    monkeypatch.setattr(dashboard_mod, "_fetcher", _FakeFetcher(), raising=False)

    client = dashboard_mod.app.test_client()
    response = client.get("/api/chart/candles?asset=WTI&interval=30m")
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["success"] is True
    assert payload["candles"]
    assert attempts == [240, 60]
    assert payload["bars_requested"] == 60

def test_api_status_includes_provider_routing(monkeypatch) -> None:
    dashboard_mod = importlib.import_module("dashboard.web_app_live")

    monkeypatch.setattr(dashboard_mod, "_DEVELOPMENT_MODE", True, raising=False)
    monkeypatch.setattr(dashboard_mod, "_AUTH_CONFIG_ERROR", "", raising=False)
    monkeypatch.setattr(dashboard_mod, "_cache_get", lambda key: None, raising=False)
    monkeypatch.setattr(dashboard_mod, "_cache_set", lambda key, value, ttl=0: None, raising=False)
    monkeypatch.setattr(dashboard_mod, "_CORE", None, raising=False)

    client = dashboard_mod.app.test_client()
    response = client.get("/api/status")
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["success"] is True
    assert "provider_routing" in payload
    assert payload["provider_routing"]["asset_count"] >= 1
    assert "summary_label" in payload["provider_routing"]

def test_api_status_includes_signal_diagnostics_summary(monkeypatch) -> None:
    dashboard_mod = importlib.import_module("dashboard.web_app_live")

    class _Core:
        is_ready = True
        is_running = True

        def get_balance(self):
            return 1500.0

        def get_positions(self):
            return [
                {
                    "metadata": {
                        "broker_quality": {
                            "score": 0.18,
                            "quote_agreement_state": "severe_divergence",
                            "spread_regime": "stressed",
                            "quote_quality_state": "stale",
                        },
                        "market_microstructure": {
                            "depth_available": False,
                            "synthetic_depth_available": True,
                        },
                        "cross_asset_context": {
                            "alignment": -0.44,
                            "confidence": 0.82,
                        },
                        "adaptive_policy": {
                            "recent_review_profile": {
                                "block_new_entries": True,
                            }
                        },
                    }
                }
            ]

    monkeypatch.setattr(dashboard_mod, "_DEVELOPMENT_MODE", True, raising=False)
    monkeypatch.setattr(dashboard_mod, "_AUTH_CONFIG_ERROR", "", raising=False)
    monkeypatch.setattr(dashboard_mod, "_cache_get", lambda key: None, raising=False)
    monkeypatch.setattr(dashboard_mod, "_cache_set", lambda key, value, ttl=0: None, raising=False)
    monkeypatch.setattr(dashboard_mod, "_CORE", _Core(), raising=False)

    client = dashboard_mod.app.test_client()
    response = client.get("/api/status")
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["success"] is True
    assert payload["signal_diagnostics"]["count"] == 1
    assert payload["signal_diagnostics"]["broker_fragile_count"] == 1
    assert payload["signal_diagnostics"]["synthetic_depth_count"] == 1
    assert payload["signal_diagnostics"]["cross_conflict_count"] == 1
    assert payload["signal_diagnostics"]["recent_pattern_block_count"] == 1

def test_market_data_router_filters_ig_primary_assets(monkeypatch) -> None:
    router_mod = importlib.import_module("services.market_data_router")

    monkeypatch.setattr(
        router_mod,
        "is_ig_primary_category",
        lambda category: str(category).lower() == "commodities",
        raising=False,
    )

    result = router_mod.filter_ig_primary_assets(
        {
            "XAU/USD": "commodities",
            "EUR/USD": "forex",
            "BTC-USD": "crypto",
        }
    )

    assert result == {"XAU/USD": "commodities"}

def test_page_overview_normalizes_cached_response_objects(monkeypatch) -> None:
    dashboard_mod = importlib.import_module("dashboard.web_app_live")

    monkeypatch.setattr(dashboard_mod, "_DEVELOPMENT_MODE", True, raising=False)
    monkeypatch.setattr(dashboard_mod, "_AUTH_CONFIG_ERROR", "", raising=False)

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

def test_page_overview_command_center_reuses_embedded_whale_summary(monkeypatch) -> None:
    dashboard_mod = importlib.import_module("dashboard.web_app_live")

    monkeypatch.setattr(dashboard_mod, "_DEVELOPMENT_MODE", True, raising=False)
    monkeypatch.setattr(dashboard_mod, "_AUTH_CONFIG_ERROR", "", raising=False)
    monkeypatch.setattr(dashboard_mod, "_cache_get", lambda key: None, raising=False)
    monkeypatch.setattr(dashboard_mod, "_cache_set", lambda key, value, ttl=0: None, raising=False)

    class _FakeResponse:
        def __init__(self, payload):
            self._payload = payload

        def get_json(self):
            return self._payload

    def _fake_call_view(fn):
        name = getattr(fn, "__name__", "")
        if name == "api_command_center":
            return _FakeResponse({
                "success": True,
                "balance": 123.0,
                "recent": [{"asset": "BTC-USD", "value_usd": 1_250_000}],
                "alert_count_24h": 3,
                "whale_alerts_24h": 3,
            })
        if name == "api_whale_summary":
            raise AssertionError("api_whale_summary should not be called for command_center overview")
        raise AssertionError(f"Unexpected view {name}")

    monkeypatch.setattr(dashboard_mod, "_call_view", _fake_call_view, raising=False)

    with dashboard_mod.app.test_request_context("/api/page-overview?page=command_center"):
        response = dashboard_mod.api_page_overview()

    payload = response.get_json()
    assert response.status_code == 200
    assert payload["success"] is True
    assert payload["command_center"]["balance"] == 123.0
    assert payload["whale"]["success"] is True
    assert payload["whale"]["alert_count_24h"] == 3
    assert payload["whale"]["recent"][0]["asset"] == "BTC-USD"

def test_sentiment_dashboard_exposes_macro_news_context(monkeypatch) -> None:
    dashboard_mod = importlib.import_module("dashboard.web_app_live")

    monkeypatch.setattr(dashboard_mod, "_DEVELOPMENT_MODE", True, raising=False)
    monkeypatch.setattr(dashboard_mod, "_AUTH_CONFIG_ERROR", "", raising=False)
    monkeypatch.setattr(dashboard_mod, "_cache_get", lambda key: None, raising=False)
    monkeypatch.setattr(dashboard_mod, "_cache_set", lambda key, value, ttl=0: None, raising=False)

    class _FakeSentimentService:
        class _NewsIntegrator:
            @staticmethod
            def fetch_all_sources():
                return [
                    {"title": "Risk assets tumble on fresh macro fears", "date": "2026-04-02", "sentiment": -0.8, "source": "FXStreet"},
                    {"title": "Investors stay cautious after selloff", "date": "2026-04-02", "sentiment": -0.4, "source": "Reuters"},
                    {"title": "Market steadies but sentiment remains fragile", "date": "2026-04-02", "sentiment": 0.0, "source": "CNBC"},
                ]

        news_integrator = _NewsIntegrator()

        @staticmethod
        def get_comprehensive_sentiment():
            return {
                "score": 0.42,
                "interpretation": "Strongly Bullish",
                "components": {
                    "fear_greed": 0.54,
                    "vix": 0.18,
                },
            }

        @staticmethod
        def fetch_fear_greed_index():
            return {"value": 12, "classification": "Extreme Fear", "score": 0.54}

        @staticmethod
        def fetch_vix():
            return {"value": 25.1, "classification": "Elevated", "score": 0.18}

        @staticmethod
        def fetch_whale_alerts(min_value_usd: float = 1_000_000):
            return []

    monkeypatch.setattr(dashboard_mod, "_get_sent", lambda: _FakeSentimentService(), raising=False)

    with dashboard_mod.app.test_request_context("/api/sentiment/dashboard"):
        response = dashboard_mod.api_sentiment_dashboard()

    payload = response.get_json()
    assert response.status_code == 200
    assert payload["success"] is True
    assert payload["market_composite"]["score"] == 0.42
    assert payload["news_sentiment"]["score"] < 0
    assert payload["sentiment_context"]["mode"] == "contrarian_rebound"
    assert payload["sentiment_context"]["display_label"] == "Bullish Rebound Bias"

def test_command_center_survives_live_price_wait_timeout(monkeypatch) -> None:
    dashboard_mod = importlib.import_module("dashboard.web_app_live")
    futures_mod = importlib.import_module("concurrent.futures")

    monkeypatch.setattr(dashboard_mod, "_DEVELOPMENT_MODE", True, raising=False)
    monkeypatch.setattr(dashboard_mod, "_AUTH_CONFIG_ERROR", "", raising=False)

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
                    "strategy_id": "playbook_breakout_continuation",
                    "open_time": "2026-03-30T00:00:00",
                    "pnl": 0.0,
                    "metadata": {
                        "setup_memory": {"memory_score": 71.0, "sample_count": 12},
                        "execution_feedback": {"quality_score": 64.0},
                        "opportunity_score": 0.83,
                        "opportunity_rank": 1,
                    },
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
    assert payload["positions"][0]["memory_score"] == 71.0
    assert payload["positions"][0]["execution_quality_score"] == 64.0
    assert payload["positions"][0]["opportunity_score"] == 0.83
    assert payload["positions"][0]["open_time"] == "2026-03-30T00:00:00"
    assert payload["signal_quality"]["avg_memory_score"] == 71.0
    assert payload["signal_quality"]["avg_execution_quality"] == 64.0
    assert payload["signal_quality"]["top_signal_asset"] == "BTC-USD"

def test_command_center_includes_top_opportunities_and_weak_positions(monkeypatch) -> None:
    dashboard_mod = importlib.import_module("dashboard.web_app_live")

    monkeypatch.setattr(dashboard_mod, "_DEVELOPMENT_MODE", True, raising=False)
    monkeypatch.setattr(dashboard_mod, "_AUTH_CONFIG_ERROR", "", raising=False)

    class _FakeCore:
        is_running = True
        is_ready = True

        def get_performance(self):
            return {"balance": 1000.0, "total_pnl": 10.0, "win_rate": 55.0, "total_trades": 4}

        def get_daily_stats(self):
            return {"daily_pnl": 2.0, "daily_trades": 1}

        def get_positions(self):
            return []

        def health_report(self):
            return {"is_running": True, "engine_ready": True}

        def get_top_ranked_opportunities(self, limit=5):
            return [{"asset": "BTC-USD", "direction": "SELL", "opportunity_score": 0.91, "source": "signal"}]

        def get_weak_positions(self, limit=5):
            return [{"asset": "ETH-USD", "quality_score": 48.0, "weak_reasons": ["execution weak"]}]

    monkeypatch.setattr(dashboard_mod, "_CORE", _FakeCore(), raising=False)
    monkeypatch.setattr(dashboard_mod, "_cache_get", lambda key: None, raising=False)
    monkeypatch.setattr(dashboard_mod, "_cache_set", lambda key, value, ttl=0: None, raising=False)
    monkeypatch.setattr(dashboard_mod, "_get_sent", lambda: None, raising=False)
    monkeypatch.setattr(dashboard_mod, "_get_market_intelligence", lambda: None, raising=False)

    with dashboard_mod.app.test_request_context("/api/command-center"):
        response = dashboard_mod.api_command_center()

    payload = response.get_json()
    assert response.status_code == 200
    assert payload["success"] is True
    assert payload["top_opportunities"][0]["asset"] == "BTC-USD"
    assert payload["weak_positions"][0]["asset"] == "ETH-USD"
    assert "provider_routing" in payload
    assert "summary_label" in payload["provider_routing"]

def test_command_center_includes_signal_diagnostics_fields(monkeypatch) -> None:
    dashboard_mod = importlib.import_module("dashboard.web_app_live")

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
                    "asset": "XAU/USD",
                    "category": "commodities",
                    "direction": "BUY",
                    "confidence": 0.82,
                    "entry_price": 100.0,
                    "stop_loss": 98.0,
                    "take_profit": 104.0,
                    "position_size": 1.0,
                    "strategy_id": "playbook_breakout_continuation",
                    "open_time": "2026-04-05T00:00:00",
                    "pnl": 12.0,
                    "metadata": {
                        "broker_quality": {
                            "score": 0.79,
                            "primary_provider": "IG",
                            "comparison_provider": "Deriv",
                            "quote_agreement_state": "strong",
                            "spread_regime": "tight",
                            "quote_quality_state": "fresh",
                        },
                        "market_microstructure": {
                            "score": 0.31,
                            "depth_available": False,
                            "synthetic_depth_available": True,
                            "microstructure_source": "live_store_synthetic_depth",
                        },
                        "cross_asset_context": {
                            "score": 0.37,
                            "alignment": 0.37,
                            "confidence": 0.76,
                            "state": "buy_support",
                            "dominant_peer": "XAG/USD",
                            "dominant_relation": "gold_silver_relative",
                        },
                        "adaptive_policy": {
                            "recent_review_profile": {
                                "sample_count": 5,
                                "notes": ["recent_pattern_true_depth_winners"],
                                "block_new_entries": False,
                            }
                        },
                    },
                }
            ]

        def health_report(self):
            return {"is_running": True, "engine_ready": True}

    class _FakeFetcher:
        @staticmethod
        def get_real_time_price(asset, category):
            return 101.25, {"source": "IG"}

    monkeypatch.setattr(dashboard_mod, "_DEVELOPMENT_MODE", True, raising=False)
    monkeypatch.setattr(dashboard_mod, "_AUTH_CONFIG_ERROR", "", raising=False)
    monkeypatch.setattr(dashboard_mod, "_CORE", _FakeCore(), raising=False)
    monkeypatch.setattr(dashboard_mod, "_fetcher", _FakeFetcher(), raising=False)
    monkeypatch.setattr(dashboard_mod, "_cache_get", lambda key: None, raising=False)
    monkeypatch.setattr(dashboard_mod, "_cache_set", lambda key, value, ttl=0: None, raising=False)
    monkeypatch.setattr(dashboard_mod, "_get_sent", lambda: None, raising=False)
    monkeypatch.setattr(dashboard_mod, "_get_market_intelligence", lambda: None, raising=False)

    with dashboard_mod.app.test_request_context("/api/command-center"):
        response = dashboard_mod.api_command_center()

    payload = response.get_json()
    assert response.status_code == 200
    assert payload["success"] is True
    assert payload["positions"][0]["broker_quality_score"] == 0.79
    assert payload["positions"][0]["depth_mode"] == "synthetic_depth"
    assert payload["positions"][0]["cross_asset_primary_peer"] == "XAG/USD"
    assert payload["positions"][0]["recent_pattern_notes"] == ["recent_pattern_true_depth_winners"]
    assert payload["signal_diagnostics"]["broker_supportive_count"] == 1
    assert payload["signal_diagnostics"]["synthetic_depth_count"] == 1
    assert payload["signal_diagnostics"]["cross_support_count"] == 1

def test_command_center_uses_fast_ranking_snapshots(monkeypatch) -> None:
    dashboard_mod = importlib.import_module("dashboard.web_app_live")

    monkeypatch.setattr(dashboard_mod, "_DEVELOPMENT_MODE", True, raising=False)
    monkeypatch.setattr(dashboard_mod, "_AUTH_CONFIG_ERROR", "", raising=False)
    calls: dict[str, tuple] = {}

    class _FakeCore:
        is_running = True
        is_ready = True

        def get_performance(self):
            return {"balance": 1000.0, "total_pnl": 10.0, "win_rate": 55.0, "total_trades": 4}

        def get_daily_stats(self):
            return {"daily_pnl": 2.0, "daily_trades": 1}

        def get_positions(self):
            return []

        def health_report(self):
            return {"is_running": True, "engine_ready": True}

        def get_top_ranked_opportunities(self, limit=5, refresh=False, allow_refresh_when_empty=True):
            calls["top"] = (limit, refresh, allow_refresh_when_empty)
            return [{"asset": "BTC-USD"}]

        def get_weak_positions(self, limit=5, include_market_status=True):
            calls["weak"] = (limit, include_market_status)
            return [{"asset": "ETH-USD"}]

    monkeypatch.setattr(dashboard_mod, "_CORE", _FakeCore(), raising=False)
    monkeypatch.setattr(dashboard_mod, "_cache_get", lambda key: None, raising=False)
    monkeypatch.setattr(dashboard_mod, "_cache_set", lambda key, value, ttl=0: None, raising=False)
    monkeypatch.setattr(dashboard_mod, "_get_sent", lambda: None, raising=False)
    monkeypatch.setattr(dashboard_mod, "_get_market_intelligence", lambda: None, raising=False)

    with dashboard_mod.app.test_request_context("/api/command-center"):
        response = dashboard_mod.api_command_center()

    payload = response.get_json()
    assert response.status_code == 200
    assert payload["success"] is True
    assert payload["top_opportunities"] == [{"asset": "BTC-USD"}]
    assert payload["weak_positions"] == [{"asset": "ETH-USD"}]
    assert calls["top"] == (5, False, False)
    assert calls["weak"] == (5, False)

def test_operator_action_endpoints_call_core_methods(monkeypatch) -> None:
    dashboard_mod = importlib.import_module("dashboard.web_app_live")
    calls: dict[str, tuple] = {}

    class _FakeCore:
        def reprice_weak_exits(self, tighten_only=True, limit=3, score_threshold=0.62):
            calls["reprice"] = (tighten_only, limit, score_threshold)
            return [{"trade_id": "t1", "asset": "BTC-USD"}]

        def reduce_weak_positions(self, reduction_fraction=0.35, limit=3, score_threshold=0.58):
            calls["reduce"] = (reduction_fraction, limit, score_threshold)
            return [{"trade_id": "t1", "asset": "BTC-USD", "success": True}]

        def get_top_ranked_opportunities(self, limit=5, refresh=False):
            calls["top"] = (limit, refresh)
            return [{"asset": "BTC-USD", "opportunity_score": 0.88}]

    monkeypatch.setattr(dashboard_mod, "_DEVELOPMENT_MODE", True, raising=False)
    monkeypatch.setattr(dashboard_mod, "_CORE", _FakeCore(), raising=False)
    monkeypatch.setattr(dashboard_mod, "_invalidate_cache_prefixes", lambda *prefixes: None, raising=False)

    client = dashboard_mod.app.test_client()

    resp1 = client.post("/api/positions/reprice-weak", json={"limit": 2, "tighten_only": True})
    payload1 = resp1.get_json()
    assert resp1.status_code == 200
    assert payload1["success"] is True
    assert payload1["repriced"] == 1
    assert calls["reprice"] == (True, 2, 0.62)

    resp2 = client.post("/api/positions/reduce-weak", json={"limit": 2, "reduction_fraction": 0.4})
    payload2 = resp2.get_json()
    assert resp2.status_code == 200
    assert payload2["success"] is True
    assert payload2["reduced"] == 1
    assert calls["reduce"] == (0.4, 2, 0.58)

    resp3 = client.get("/api/opportunities/top?limit=4&refresh=1")
    payload3 = resp3.get_json()
    assert resp3.status_code == 200
    assert payload3["success"] is True
    assert payload3["count"] == 1
    assert payload3["opportunities"][0]["asset"] == "BTC-USD"
    assert calls["top"] == (4, True)

def test_deriv_bridge_request_retries_after_socket_failure(monkeypatch) -> None:
    import json
    import time

    from services.deriv_bridge import DerivBridge

    class _FailWs:
        def settimeout(self, timeout):
            return None

        def send(self, message):
            return None

        def recv(self):
            raise RuntimeError("socket closed")

        def close(self):
            return None

    class _SuccessWs:
        def __init__(self) -> None:
            self._last_req_id = None

        def settimeout(self, timeout):
            return None

        def send(self, message):
            self._last_req_id = json.loads(message)["req_id"]

        def recv(self):
            return json.dumps({"req_id": self._last_req_id, "active_symbols": []})

        def close(self):
            return None

    bridge = DerivBridge()
    bridge._enabled = True
    bridge._app_id = "test-app"
    bridge._ws = _FailWs()

    reconnects = {"count": 0}

    def _fake_connect_locked():
        reconnects["count"] += 1
        bridge._ws = _SuccessWs()
        bridge._last_io = time.monotonic()
        return True

    monkeypatch.setattr(bridge, "_connect_locked", _fake_connect_locked)

    response = bridge._request_locked({"active_symbols": "full"}, max_retries=1)

    assert reconnects["count"] == 1
    assert response["active_symbols"] == []

def test_api_backtest_robustness_returns_report(monkeypatch) -> None:
    dashboard_mod = importlib.import_module("dashboard.web_app_live")

    monkeypatch.setattr(dashboard_mod, "_DEVELOPMENT_MODE", True, raising=False)

    client = dashboard_mod.app.test_client()
    response = client.get("/api/backtest/robustness?asset=BTC-USD&strategy=demo_strategy&periods=200&rsi_period=21&stop_mult=2.0&tp_mult=4.0")
    payload = response.get_json()

    assert response.status_code == 409
    assert payload["success"] is False
    assert payload["disabled"] is True
    assert payload["mode"] == "playbook_only"

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

def test_health_report_includes_monitor_source_health(monkeypatch) -> None:
    core_mod = importlib.import_module("core.engine")
    monitor_mod = importlib.import_module("monitoring.system_health_service")

    core = object.__new__(core_mod.TradingCore)
    ready = threading.Event()
    ready.set()
    core._engine_ready = ready
    core._is_running = True
    core.strategy_mode = "playbook"
    core.state = SimpleNamespace(
        balance=1250.0,
        open_position_count=lambda: 2,
        daily_trades=3,
        daily_pnl=-12.5,
        get_all_cooldowns=lambda: {"BTC-USD": 7},
    )

    monkeypatch.setattr(
        monitor_mod,
        "monitor",
        SimpleNamespace(
            get_snapshot=lambda: {
                "source_health": {
                    "technicals": {"fresh": True, "status": "fresh"},
                    "order_book": {"fresh": False, "status": "stale", "age_secs": 18.0, "threshold": 10},
                },
                "recent_error_count": 2,
                "recent_errors": [{"module": "redis", "message": "timeout"}],
            }
        ),
        raising=False,
    )

    report = core.health_report()

    assert report["status"] == "degraded"
    assert report["source_health"]["technicals"]["fresh"] is True
    assert report["stale_sources"] == ["order_book"]
    assert report["stale_source_count"] == 1
    assert report["recent_error_count"] == 2
    assert any("order_book" in issue for issue in report["issues"])

def test_health_report_includes_ig_broker_snapshot(monkeypatch) -> None:
    core_mod = importlib.import_module("core.engine")
    monitor_mod = importlib.import_module("monitoring.system_health_service")
    ig_mod = importlib.import_module("services.ig_market_bridge")

    core = object.__new__(core_mod.TradingCore)
    ready = threading.Event()
    ready.set()
    core._engine_ready = ready
    core._is_running = True
    core.strategy_mode = "playbook"
    core.state = SimpleNamespace(
        balance=1250.0,
        open_position_count=lambda: 1,
        daily_trades=1,
        daily_pnl=5.0,
        get_all_cooldowns=lambda: {},
    )

    monkeypatch.setattr(
        monitor_mod,
        "monitor",
        SimpleNamespace(get_snapshot=lambda: {"source_health": {}, "recent_error_count": 0, "recent_errors": []}),
        raising=False,
    )
    monkeypatch.setattr(
        ig_mod,
        "ig_market_bridge",
        SimpleNamespace(
            get_account_summary=lambda: {
                "enabled": True,
                "authenticated": True,
                "environment": "demo",
                "account_id": "Z6A62A",
                "watchlist_count": 2,
            }
        ),
        raising=False,
    )

    report = core.health_report()

    assert report["ig_broker"]["authenticated"] is True
    assert report["ig_broker"]["account_id"] == "Z6A62A"

def test_api_system_health_includes_ig_broker(monkeypatch) -> None:
    dashboard_mod = importlib.import_module("dashboard.web_app_live")
    ws_mod = importlib.import_module("websocket_dashboard")

    monkeypatch.setattr(dashboard_mod, "_DEVELOPMENT_MODE", True, raising=False)
    monkeypatch.setattr(dashboard_mod, "_AUTH_CONFIG_ERROR", "", raising=False)
    monkeypatch.setattr(dashboard_mod, "_cache_get", lambda key: None, raising=False)
    monkeypatch.setattr(dashboard_mod, "_cache_set", lambda key, value, ttl=0: None, raising=False)

    class _FakeCore:
        is_ready = True
        is_running = True

        def get_balance(self):
            return 1500.0

        def health_report(self):
            return {
                "ram_pct": 12.0,
                "cpu_pct": 5.0,
                "open_positions": 0,
                "active_cooldowns": 0,
                "source_health": {},
                "stale_sources": [],
                "stale_source_count": 0,
                "never_seen_sources": [],
                "never_seen_source_count": 0,
                "recent_error_count": 0,
                "recent_errors": [],
                "issues": [],
                "strategy_mode": "playbook",
                "balance": 1500.0,
                "ig_broker": {
                    "enabled": True,
                    "authenticated": True,
                    "environment": "demo",
                    "account_id": "Z6A62A",
                }
            }

    monkeypatch.setattr(dashboard_mod, "_CORE", _FakeCore(), raising=False)
    monkeypatch.setattr(
        dashboard_mod,
        "_get_disk_usage_pct",
        lambda path: 20.0,
        raising=False,
    )
    monkeypatch.setattr(
        ws_mod,
        "connection_status",
        {
            "deriv": {"connected": True, "symbol_count": 14, "assets": "Forex, Crypto, Indices"},
            "binance": {"connected": False, "symbol_count": 0, "assets": "BNB, SOL, XRP"},
            "ig": {"connected": True, "symbol_count": 3, "assets": "Gold, Silver, WTI"},
        },
        raising=False,
    )

    with dashboard_mod.app.test_request_context("/api/system-health"):
        response = dashboard_mod.api_system_health()

    payload = response.get_json()
    assert payload["success"] is True
    assert payload["ig_broker"]["authenticated"] is True
    assert payload["ig_broker"]["account_id"] == "Z6A62A"
    assert payload["feed_connections"]["deriv"]["connected"] is True
    assert payload["feed_connections"]["ig"]["symbol_count"] == 3

def test_health_report_does_not_degrade_for_never_seen_sources_only(monkeypatch) -> None:
    core_mod = importlib.import_module("core.engine")
    monitor_mod = importlib.import_module("monitoring.system_health_service")

    core = object.__new__(core_mod.TradingCore)
    ready = threading.Event()
    ready.set()
    core._engine_ready = ready
    core._is_running = True
    core.strategy_mode = "playbook"
    core.state = SimpleNamespace(
        balance=1250.0,
        open_position_count=lambda: 0,
        daily_trades=0,
        daily_pnl=0.0,
        get_all_cooldowns=lambda: {},
    )

    monkeypatch.setattr(
        monitor_mod,
        "monitor",
        SimpleNamespace(
            get_snapshot=lambda: {
                "source_health": {
                    "whale": {"fresh": False, "status": "never_seen"},
                    "macro": {"fresh": False, "status": "never_seen"},
                },
                "recent_error_count": 0,
                "recent_errors": [],
            }
        ),
        raising=False,
    )

    report = core.health_report()

    assert report["status"] == "healthy"
    assert report["stale_sources"] == []
    assert report["never_seen_sources"] == ["macro", "whale"]
    assert report["issues"] == []

def test_api_system_health_includes_source_health(monkeypatch) -> None:
    dashboard_mod = importlib.import_module("dashboard.web_app_live")

    monkeypatch.setattr(dashboard_mod, "_cache_get", lambda key: None, raising=False)
    monkeypatch.setattr(dashboard_mod, "_cache_set", lambda key, value, ttl=0: None, raising=False)
    monkeypatch.setattr(dashboard_mod, "_redis_broker", SimpleNamespace(is_connected=lambda: True), raising=False)
    monkeypatch.setattr(dashboard_mod, "telegram_manager", SimpleNamespace(is_running=True), raising=False)
    monkeypatch.setattr(
        dashboard_mod,
        "_CORE",
        SimpleNamespace(
            is_running=True,
            is_ready=True,
            health_report=lambda: {
                "is_running": True,
                "engine_ready": True,
                "strategy_mode": "playbook",
                "balance": 1111.0,
                "open_positions": 1,
                "active_cooldowns": 2,
                "issues": ["Stale data sources: order_book"],
                "source_health": {"order_book": {"fresh": False, "status": "stale"}},
                "stale_sources": ["order_book"],
                "stale_source_count": 1,
                "recent_error_count": 3,
                "recent_errors": [{"module": "redis", "message": "timeout"}],
            },
        ),
        raising=False,
    )

    class _FakeDB:
        def ping(self):
            return True

    monkeypatch.setitem(sys.modules, "services.db_pool", SimpleNamespace(get_db=lambda: _FakeDB()))

    client = dashboard_mod.app.test_client()
    response = client.get("/api/system/health")
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["success"] is True
    assert payload["source_health"]["order_book"]["fresh"] is False
    assert payload["stale_sources"] == ["order_book"]
    assert payload["stale_source_count"] == 1
    assert payload["recent_error_count"] == 3

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

def test_chart_stream_prefers_shared_live_price_cache(monkeypatch) -> None:
    dashboard_mod = importlib.import_module("dashboard.web_app_live")
    ws_mod = importlib.import_module("websocket_dashboard")

    class _FakeFetcher:
        def get_real_time_price(self, asset, category):
            raise AssertionError("direct provider fetch should not be used when shared live cache is fresh")

    monkeypatch.setattr(dashboard_mod, "_DEVELOPMENT_MODE", True, raising=False)
    monkeypatch.setattr(dashboard_mod, "_fetcher", _FakeFetcher(), raising=False)
    monkeypatch.setattr(ws_mod, "get_live_price", lambda asset, max_age_seconds=15.0: (2501.25, "IG"), raising=False)

    client = dashboard_mod.app.test_client()
    response = client.get("/api/chart/stream?asset=WTI", buffered=False)
    first_chunk = next(response.response).decode("utf-8")
    second_chunk = next(response.response).decode("utf-8")
    response.close()

    assert response.status_code == 200
    assert '"type": "connected"' in first_chunk
    assert '"type": "tick"' in second_chunk
    assert '"asset": "WTI"' in second_chunk
    assert '"source": "IG"' in second_chunk

def test_record_live_quote_emits_transaction_and_cache(monkeypatch) -> None:
    dashboard_mod = importlib.import_module("dashboard.web_app_live")

    seen = {"transactions": [], "prices": []}
    monkeypatch.setattr(
        dashboard_mod,
        "add_transaction",
        lambda source, symbol, price, volume=None, side=None: seen["transactions"].append(
            (source, symbol, price, volume, side)
        ),
        raising=False,
    )
    monkeypatch.setattr(
        dashboard_mod,
        "set_live_price",
        lambda symbol, price, source="WebSocket": seen["prices"].append((symbol, price, source)),
        raising=False,
    )

    dashboard_mod._record_live_quote("IG", "WTI", 82.45)

    assert seen["transactions"] == [("IG", "WTI", 82.45, None, None)]
    assert seen["prices"] == [("WTI", 82.45, "IG")]

def test_record_live_quote_can_refresh_cache_without_transaction(monkeypatch) -> None:
    dashboard_mod = importlib.import_module("dashboard.web_app_live")

    seen = {"transactions": [], "prices": []}
    monkeypatch.setattr(
        dashboard_mod,
        "add_transaction",
        lambda source, symbol, price, volume=None, side=None: seen["transactions"].append(
            (source, symbol, price, volume, side)
        ),
        raising=False,
    )
    monkeypatch.setattr(
        dashboard_mod,
        "set_live_price",
        lambda symbol, price, source="WebSocket": seen["prices"].append((symbol, price, source)),
        raising=False,
    )

    dashboard_mod._record_live_quote("IG", "WTI", 82.45, emit_transaction=False)

    assert seen["transactions"] == []
    assert seen["prices"] == [("WTI", 82.45, "IG")]

def test_websocket_dashboard_mark_feed_activity_updates_last_tick_without_transaction() -> None:
    ws_mod = importlib.import_module("websocket_dashboard")

    original = copy.deepcopy(ws_mod.connection_status.get("ig"))
    try:
        ws_mod.connection_status["ig"] = {
            "connected": False,
            "last_tick": None,
            "symbol_count": 0,
            "assets": "Gold, Silver, WTI",
        }
        ws_mod.mark_feed_activity("ig", 3)
        status = ws_mod.connection_status["ig"]

        assert status["connected"] is True
        assert status["symbol_count"] == 3
        assert status["last_tick"] is not None
    finally:
        ws_mod.connection_status["ig"] = original

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
    assert reward == 53.76

def test_risk_manager_aligns_take_profit_to_structure() -> None:
    risk_mod = importlib.import_module("risk.manager")
    manager = risk_mod.RiskManager(account_balance=10_000.0)

    entry = 100.0
    stop_loss = 95.0
    base_take_profit = manager.get_take_profit(entry, stop_loss, "BUY", category="crypto")

    neutral_take_profit = manager.align_take_profit_to_structure(
        entry,
        base_take_profit,
        "BUY",
        category="crypto",
        structure={
            "resistance": 104.0,
            "regime": "ranging",
            "structure_bias": "buy",
            "alignment_score": 0.60,
            "setup_quality": 0.55,
            "breakout_score": 0.10,
            "volatility_state": "normal",
        },
        atr=2.0,
        confidence=0.65,
    )
    strong_breakout_take_profit = manager.align_take_profit_to_structure(
        entry,
        base_take_profit,
        "BUY",
        category="crypto",
        structure={
            "resistance": 104.0,
            "regime": "trending_up",
            "structure_bias": "buy",
            "alignment_score": 0.82,
            "setup_quality": 0.80,
            "breakout_score": 0.92,
            "volatility_state": "expansion",
        },
        atr=2.0,
        confidence=0.82,
    )

    assert neutral_take_profit < base_take_profit
    assert neutral_take_profit < 104.0
    assert strong_breakout_take_profit > neutral_take_profit
    assert strong_breakout_take_profit <= base_take_profit

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

def test_portfolio_risk_scales_asset_exposure_to_cap() -> None:
    portfolio_mod = importlib.import_module("risk.portfolio_risk")
    engine = portfolio_mod.PortfolioRiskEngine(max_single_asset_pct=35.0, max_category_pct=40.0)
    signal = {
        "asset": "EUR/USD",
        "category": "forex",
        "position_size": 40_000.0,
        "entry_price": 1.10,
        "direction": "BUY",
    }

    approved, reason = engine.evaluate(
        signal=signal,
        open_positions=[],
        balance=1_000.0,
    )

    assert approved is True
    assert "asset EUR/USD scaled" in reason
    assert round(signal["position_size"], 2) == 35_000.0

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

def test_trade_history_api_uses_runtime_state_and_normalizes_open_time(monkeypatch) -> None:
    dashboard_mod = importlib.import_module("dashboard.web_app_live")

    def _get_closed_positions(limit=100):
        assert limit == 12
        return [{
            "trade_id": "t1",
            "asset": "EUR/USD",
            "category": "forex",
            "direction": "BUY",
            "open_time": "2026-03-29T10:00:00+00:00",
            "exit_time": "2026-03-29T11:30:00+00:00",
            "duration_minutes": 90,
            "exit_reason": "Take Profit",
            "pnl": 12.5,
        }]

    monkeypatch.setattr(state_mod.state, "get_closed_positions", _get_closed_positions)
    client = dashboard_mod.app.test_client()
    response = client.get("/api/trade-history?limit=2")
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["success"] is True
    assert payload["count"] == 1
    assert payload["trades"][0]["display_timezone"] == "EAT"
    assert payload["trades"][0]["duration_str"] == "1h 30m"
    assert payload["trades"][0]["open_time"] == "2026-03-29T10:00:00+00:00"
    assert payload["trades"][0]["entry_time"] == "2026-03-29T10:00:00+00:00"
    assert payload["trades"][0]["exit_time"] == "2026-03-29T11:30:00+00:00"

def test_trade_history_api_enriches_execution_feedback_fields(monkeypatch) -> None:
    dashboard_mod = importlib.import_module("dashboard.web_app_live")

    monkeypatch.setattr(
        state_mod.state,
        "get_closed_positions",
        lambda limit=100: [{
            "trade_id": "t2",
            "asset": "BTC-USD",
            "category": "crypto",
            "direction": "SELL",
            "entry_time": "2026-03-29T10:00:00+00:00",
            "exit_time": "2026-03-29T10:45:00+00:00",
            "exit_reason": "Stop Loss",
            "pnl": -40.5,
            "metadata": {
                "execution_feedback": {
                    "quality_score": 58.2,
                    "rr_realized": -0.91,
                    "premature_stop": True,
                    "target_miss": False,
                },
                "execution_feedback_policy": {
                    "sample_count": 19,
                    "target_rr_multiplier": 0.87,
                    "stop_buffer_multiplier": 1.08,
                    "notes": ["tighten targets", "widen stops slightly"],
                },
                "setup_memory": {
                    "memory_score": 63.5,
                    "memory_edge": 0.28,
                    "sample_count": 14,
                    "win_rate": 0.62,
                    "avg_similarity": 0.71,
                    "notes": ["memory_positive_edge"],
                },
                "setup_memory_fingerprint": {
                    "regime": "trending_down",
                    "setup_style": "pullback",
                },
                "opportunity_score": 0.812,
                "opportunity_rank": 2,
                "opportunity_breakdown": {"structure": 0.24, "memory": 0.18},
            },
        }],
    )
    client = dashboard_mod.app.test_client()
    response = client.get("/api/trade-history?limit=1")
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["success"] is True
    trade = payload["trades"][0]
    assert trade["execution_quality_score"] == 58.2
    assert trade["rr_realized"] == -0.91
    assert trade["premature_stop"] is True
    assert trade["execution_feedback_sample_count"] == 19
    assert trade["target_rr_multiplier"] == 0.87
    assert trade["stop_buffer_multiplier"] == 1.08
    assert trade["execution_notes"] == ["tighten targets", "widen stops slightly"]
    assert trade["memory_score"] == 63.5
    assert trade["memory_edge"] == 0.28
    assert trade["memory_sample_count"] == 14
    assert trade["memory_notes"] == ["memory_positive_edge"]
    assert trade["memory_regime"] == "trending_down"
    assert trade["memory_setup_style"] == "pullback"
    assert trade["opportunity_score"] == 0.812
    assert trade["opportunity_rank"] == 2

def test_trade_history_api_hides_orphan_partial_close_rows(monkeypatch) -> None:
    dashboard_mod = importlib.import_module("dashboard.web_app_live")

    monkeypatch.setattr(
        state_mod.state,
        "get_closed_positions",
        lambda limit=100: [{
            "trade_id": "abc123-PT2",
            "asset": "BNB-USD",
            "category": "crypto",
            "direction": "BUY",
            "entry_time": "2026-03-29T10:00:00+00:00",
            "exit_time": "2026-03-29T10:20:00+00:00",
            "exit_reason": "Partial TP 2/3",
            "pnl": 5.25,
            "metadata": {},
        }],
    )
    client = dashboard_mod.app.test_client()
    response = client.get("/api/trade-history?limit=1")
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["success"] is True
    assert payload["count"] == 0
    assert payload["trades"] == []

def test_trade_history_api_rolls_partial_pnl_into_parent_trade(monkeypatch) -> None:
    dashboard_mod = importlib.import_module("dashboard.web_app_live")

    monkeypatch.setattr(
        state_mod.state,
        "get_closed_positions",
        lambda limit=100: [
            {
                "trade_id": "runner-1-PT1",
                "asset": "BTC-USD",
                "category": "crypto",
                "direction": "SELL",
                "open_time": "2026-03-29T10:00:00+00:00",
                "exit_time": "2026-03-29T10:30:00+00:00",
                "duration_minutes": 30,
                "exit_reason": "Partial TP 1/2",
                "pnl": 7.5,
                "metadata": {},
            },
            {
                "trade_id": "runner-1",
                "asset": "BTC-USD",
                "category": "crypto",
                "signal": "SELL",
                "open_time": "2026-03-29T10:00:00+00:00",
                "exit_time": "2026-03-29T11:00:00+00:00",
                "duration_minutes": 60,
                "exit_reason": "Stop Loss (offline)",
                "pnl": 0.0,
                "metadata": {},
            },
        ],
    )
    client = dashboard_mod.app.test_client()
    response = client.get("/api/trade-history?limit=5")
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["success"] is True
    assert payload["count"] == 1
    trade = payload["trades"][0]
    assert trade["trade_id"] == "runner-1"
    assert trade["direction"] == "SELL"
    assert trade["pnl"] == 7.5
    assert trade["has_partial_closes"] is True
    assert trade["partial_close_count"] == 1
    assert trade["display_exit_reason"] == "Partial TP x1 -> Stop Loss (offline)"
    assert trade["continuation_summary"] == "Partial TP x1 | Runner +0.00 | Total +7.50"

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

def test_risk_portfolio_api_includes_execution_feedback_summary(monkeypatch) -> None:
    dashboard_mod = importlib.import_module("dashboard.web_app_live")

    class _PortfolioRisk:
        def get_portfolio_stats(self, positions, balance):
            return {
                "total_exposure": 3200.0,
                "exposure_pct": 32.0,
                "drawdown_pct": 4.8,
                "peak_balance": 10350.0,
            }

    class _Core:
        portfolio_risk = _PortfolioRisk()

        def get_positions(self):
            return [{
                "asset": "BTC-USD",
                "category": "crypto",
                "position_size": 0.05,
                "entry_price": 65000.0,
                "pnl": 120.0,
                "metadata": {
                    "setup_memory": {"memory_score": 68.0, "sample_count": 11},
                    "execution_feedback": {"quality_score": 62.0},
                    "opportunity_score": 0.79,
                },
            }]

        def get_balance(self):
            return 10_000.0

        def get_performance(self):
            return {"win_rate": 0.58, "total_trades": 17, "total_pnl": 845.0}

        def get_closed_trades(self, limit=100):
            return [
                {"pnl": 120.0},
                {"pnl": -60.0},
                {"pnl": 90.0},
            ]

    class _Feedback:
        def summarize_history(self, asset="", category="", days_back=120, limit=500):
            base = {
                "sample_count": 18,
                "avg_quality_score": 61.5,
                "avg_rr_realized": 0.44,
                "target_hit_rate": 0.39,
                "premature_stop_rate": 0.17,
            }
            if category:
                return {**base, "category": category, "sample_count": 6}
            return base

    feedback_mod = importlib.import_module("services.execution_feedback_service")
    monkeypatch.setattr(feedback_mod, "get_service", lambda: _Feedback(), raising=False)
    monkeypatch.setattr(dashboard_mod, "_DEVELOPMENT_MODE", True, raising=False)
    monkeypatch.setattr(dashboard_mod, "_core", lambda: _Core(), raising=False)
    monkeypatch.setattr(dashboard_mod, "_cache_get", lambda key: None, raising=False)
    monkeypatch.setattr(dashboard_mod, "_cache_set", lambda key, value, ttl=0: None, raising=False)

    client = dashboard_mod.app.test_client()
    response = client.get("/api/risk/portfolio")
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["success"] is True
    assert payload["execution_feedback"]["sample_count"] == 18
    assert payload["execution_feedback"]["avg_quality_score"] == 61.5
    assert payload["execution_by_category"]["crypto"]["sample_count"] == 6
    assert payload["by_category"]["crypto"]["avg_memory_score"] == 68.0
    assert payload["by_category"]["crypto"]["avg_execution_quality"] == 62.0
    assert payload["by_category"]["crypto"]["avg_opportunity_score"] == 0.79
    assert payload["quality_snapshot"]["avg_memory_score"] == 68.0
    assert payload["quality_snapshot"]["avg_execution_quality"] == 62.0
    assert payload["quality_snapshot"]["top_category"] == "crypto"

def test_risk_portfolio_api_includes_signal_diagnostics_snapshot(monkeypatch) -> None:
    dashboard_mod = importlib.import_module("dashboard.web_app_live")

    class _PortfolioRisk:
        def get_portfolio_stats(self, positions, balance):
            return {
                "total_exposure": 3200.0,
                "exposure_pct": 32.0,
                "drawdown_pct": 4.8,
                "peak_balance": 10350.0,
            }

    class _Core:
        portfolio_risk = _PortfolioRisk()

        def get_positions(self):
            return [{
                "asset": "WTI",
                "category": "commodities",
                "position_size": 1.0,
                "entry_price": 80.0,
                "pnl": 55.0,
                "metadata": {
                    "broker_quality": {
                        "score": 0.74,
                        "quote_agreement_state": "strong",
                        "spread_regime": "normal",
                        "quote_quality_state": "fresh",
                    },
                    "market_microstructure": {
                        "score": 0.29,
                        "depth_available": True,
                        "synthetic_depth_available": False,
                    },
                    "cross_asset_context": {
                        "alignment": -0.31,
                        "confidence": 0.71,
                    },
                    "adaptive_policy": {
                        "recent_review_profile": {
                            "block_new_entries": True,
                        }
                    },
                },
            }]

        def get_balance(self):
            return 10_000.0

        def get_performance(self):
            return {"win_rate": 0.58, "total_trades": 17, "total_pnl": 845.0}

        def get_closed_trades(self, limit=100):
            return [{"pnl": 50.0}, {"pnl": -25.0}]

    class _Feedback:
        def summarize_history(self, asset="", category="", days_back=120, limit=500):
            return {
                "sample_count": 4 if category else 12,
                "avg_quality_score": 61.5,
                "avg_rr_realized": 0.44,
                "target_hit_rate": 0.39,
                "premature_stop_rate": 0.17,
            }

    feedback_mod = importlib.import_module("services.execution_feedback_service")
    monkeypatch.setattr(feedback_mod, "get_service", lambda: _Feedback(), raising=False)
    monkeypatch.setattr(dashboard_mod, "_DEVELOPMENT_MODE", True, raising=False)
    monkeypatch.setattr(dashboard_mod, "_core", lambda: _Core(), raising=False)
    monkeypatch.setattr(dashboard_mod, "_cache_get", lambda key: None, raising=False)
    monkeypatch.setattr(dashboard_mod, "_cache_set", lambda key, value, ttl=0: None, raising=False)

    client = dashboard_mod.app.test_client()
    response = client.get("/api/risk/portfolio")
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["success"] is True
    assert payload["by_category"]["commodities"]["avg_broker_quality"] == 0.74
    assert payload["by_category"]["commodities"]["avg_microstructure_score"] == 0.29
    assert payload["by_category"]["commodities"]["avg_cross_asset_alignment"] == -0.31
    assert payload["by_category"]["commodities"]["true_depth_count"] == 1
    assert payload["by_category"]["commodities"]["recent_block_count"] == 1
    assert payload["quality_snapshot"]["avg_broker_quality"] == 0.74
    assert payload["signal_diagnostics"]["true_depth_count"] == 1
    assert payload["signal_diagnostics"]["cross_conflict_count"] == 1
    assert payload["signal_diagnostics"]["recent_pattern_block_count"] == 1

def test_ai_predictions_overview_includes_live_quality_and_leaders(monkeypatch) -> None:
    dashboard_mod = importlib.import_module("dashboard.web_app_live")
    recent_exit = datetime.now(timezone.utc).isoformat()

    class _FakeResponse:
        def __init__(self, payload):
            self._payload = payload

        def get_json(self):
            return self._payload

    def _fake_call_view(fn):
        name = getattr(fn, "__name__", "")
        if name == "api_accuracy":
            return _FakeResponse({
                "success": True,
                "data": {
                    "days_back": 30,
                    "by_horizon": {
                        "1H": {"total": 5, "correct": 3, "accuracy_pct": 60.0},
                        "4H": {"total": 4, "correct": 3, "accuracy_pct": 75.0},
                    },
                    "by_asset": {},
                    "recent": [],
                },
            })
        if name == "api_signals_live":
            return _FakeResponse({
                "success": True,
                "signals": [
                    {
                        "asset": "BTC-USD",
                        "direction": "SELL",
                        "category": "crypto",
                        "confidence": 0.81,
                        "memory_score": 72.0,
                        "memory_sample_count": 12,
                        "memory_setup_style": "pullback",
                        "execution_quality_score": 68.0,
                        "execution_feedback_sample_count": 9,
                        "opportunity_score": 0.84,
                    },
                    {
                        "asset": "ETH-USD",
                        "direction": "BUY",
                        "category": "crypto",
                        "confidence": 0.74,
                        "memory_score": 64.0,
                        "memory_sample_count": 8,
                        "memory_regime": "ranging",
                        "execution_quality_score": 59.0,
                        "execution_feedback_sample_count": 7,
                        "opportunity_score": 0.71,
                    },
                ],
            })
        raise AssertionError(f"Unexpected view {name}")

    class _Core:
        def get_closed_trades(self, limit=300):
            return [
                {
                    "trade_id": "t-win",
                    "asset": "BTC-USD",
                    "canonical_asset": "BTC-USD",
                    "strategy_id": "playbook_crypto_orderflow_continuation",
                    "pnl": 125.0,
                    "exit_time": recent_exit,
                    "metadata": {
                        "playbook_name": "crypto_orderflow_continuation",
                        "execution_feedback": {"rr_realized": 1.8},
                    },
                },
                {
                    "trade_id": "t-loss",
                    "asset": "ETH-USD",
                    "canonical_asset": "ETH-USD",
                    "strategy_id": "playbook_breakout_retest",
                    "pnl": -50.0,
                    "exit_time": recent_exit,
                    "metadata": {
                        "playbook_name": "breakout_retest",
                        "execution_feedback": {"rr_realized": -0.6},
                    },
                },
                {
                    "trade_id": "t-partial-PT1",
                    "asset": "BTC-USD",
                    "canonical_asset": "BTC-USD",
                    "strategy_id": "playbook_crypto_orderflow_continuation",
                    "pnl": 25.0,
                    "exit_time": recent_exit,
                    "exit_reason": "Partial TP 1",
                    "metadata": {
                        "playbook_name": "crypto_orderflow_continuation",
                        "is_partial_close": True,
                        "parent_trade_id": "t-win",
                    },
                },
            ]

    monkeypatch.setattr(dashboard_mod, "_DEVELOPMENT_MODE", True, raising=False)
    monkeypatch.setattr(dashboard_mod, "_cache_get", lambda key: None, raising=False)
    monkeypatch.setattr(dashboard_mod, "_cache_set", lambda key, value, ttl=0: None, raising=False)
    monkeypatch.setattr(dashboard_mod, "_call_view", _fake_call_view, raising=False)
    monkeypatch.setattr(dashboard_mod, "_core", lambda: _Core(), raising=False)

    client = dashboard_mod.app.test_client()
    response = client.get("/api/ai-predictions/overview?days=30")
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["success"] is True
    assert payload["live_quality"]["signal_count"] == 2
    assert payload["live_quality"]["avg_memory_score"] == 68.0
    assert payload["live_quality"]["avg_execution_quality"] == 63.5
    assert payload["live_quality"]["avg_opportunity_score"] == 0.775
    assert payload["live_leaders"]["memory"][0]["asset"] == "BTC-USD"
    assert payload["live_leaders"]["execution"][0]["score"] == 68.0
    assert payload["playbook_performance"]["summary"]["trade_count"] == 2
    assert payload["playbook_performance"]["summary"]["win_rate"] == 50.0
    assert payload["playbook_performance"]["playbooks"][0]["label"] == "crypto_orderflow_continuation"

def test_strategy_performance_includes_memory_and_execution_metrics(monkeypatch) -> None:
    dashboard_mod = importlib.import_module("dashboard.web_app_live")

    class _Core:
        def get_strategy_stats(self):
            return {
                "playbook_breakout_continuation": {"wins": 2, "losses": 1, "pnl": 135.0},
                "mean_revert": {"wins": 1, "losses": 1, "pnl": -20.0},
            }

        def get_closed_trades(self, limit=200):
            return [
                {
                    "asset": "BTC-USD",
                    "direction": "SELL",
                    "strategy_id": "playbook_breakout_continuation",
                    "pnl": 80.0,
                    "confidence": 0.82,
                    "exit_time": "2026-03-30T02:15:00",
                    "metadata": {
                        "execution_feedback": {"quality_score": 70.0, "rr_realized": 1.24, "target_capture": 1.0},
                        "setup_memory": {"memory_score": 66.0, "memory_edge": 0.21, "sample_count": 12},
                    },
                },
                {
                    "asset": "ETH-USD",
                    "direction": "BUY",
                    "strategy_id": "playbook_breakout_continuation",
                    "pnl": -25.0,
                    "confidence": 0.71,
                    "exit_time": "2026-03-30T03:45:00",
                    "metadata": {
                        "execution_feedback": {"quality_score": 58.0, "rr_realized": -0.42, "premature_stop": True},
                        "setup_memory": {"memory_score": 54.0, "memory_edge": -0.08, "sample_count": 7},
                    },
                },
            ]

    monkeypatch.setattr(dashboard_mod, "_DEVELOPMENT_MODE", True, raising=False)
    monkeypatch.setattr(dashboard_mod, "_core", lambda: _Core(), raising=False)

    client = dashboard_mod.app.test_client()
    response = client.get("/api/strategy/performance")
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["success"] is True
    strat = payload["strategies"]["playbook_breakout_continuation"]
    assert strat["avg_memory_score"] == 60.0
    assert strat["avg_execution_quality"] == 64.0
    assert strat["avg_rr_realized"] == 0.41
    assert strat["premature_stop_rate"] == 0.5
    assert payload["summary"]["trade_count"] == 2
    assert payload["timeline"][0]["execution_quality_score"] == 70.0
    assert payload["timeline"][1]["memory_score"] == 54.0

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

def test_telegram_history_uses_runtime_state_filters(monkeypatch) -> None:
    import asyncio

    tg_mod = importlib.import_module("telegram_commander")
    captured: Dict[str, Any] = {"limit": None}

    def _get_closed_positions(limit=100):
        captured["limit"] = limit
        return []

    async def _fake_send(*args, **kwargs):
        return None

    monkeypatch.setattr(state_mod.state, "get_closed_positions", _get_closed_positions)
    asyncio.run(
        tg_mod.TelegramCommander._show_history(
            SimpleNamespace(),
            _fake_send,
            filter_cat="won",
        )
    )

    assert captured == {"limit": 120}

def test_telegram_history_renders_dict_trade_rows(monkeypatch) -> None:
    import asyncio

    tg_mod = importlib.import_module("telegram_commander")
    captured: Dict[str, Any] = {}

    async def _fake_send(text, **kwargs):
        captured["text"] = text
        captured["kwargs"] = kwargs
        return None

    monkeypatch.setattr(
        state_mod.state,
        "get_closed_positions",
        lambda limit=100: [
            {
                "asset": "BTC-USD",
                "category": "crypto",
                "direction": "SELL",
                "open_time": "2026-04-02T10:00:00+00:00",
                "exit_time": "2026-04-02T10:45:00+00:00",
                "duration_minutes": 45,
                "exit_reason": "Take Profit 1",
                "pnl": 12.5,
            }
        ],
    )
    asyncio.run(
        tg_mod.TelegramCommander._show_history(
            SimpleNamespace(),
            _fake_send,
            filter_cat="all",
        )
    )

    assert "TRADE HISTORY" in captured["text"]
    assert "BTC-USD" in captured["text"]
    assert "🟢 1 won" in captured["text"]
    assert "Net: $+12.50" in captured["text"]
    assert "$+12.50" in captured["text"]
    assert "02 Apr 10:00" in captured["text"]
    assert "(45m)" in captured["text"]

def test_telegram_history_hides_partial_close_rows_for_consistency(monkeypatch) -> None:
    import asyncio

    tg_mod = importlib.import_module("telegram_commander")
    captured: Dict[str, Any] = {}

    async def _fake_send(text, **kwargs):
        captured["text"] = text
        return None

    monkeypatch.setattr(
        state_mod.state,
        "get_closed_positions",
        lambda limit=100: [
            {
                "trade_id": "abc123-PT1",
                "asset": "BNB-USD",
                "category": "crypto",
                "direction": "BUY",
                "entry_time": "2026-04-02T10:00:00+00:00",
                "exit_time": "2026-04-02T10:15:00+00:00",
                "exit_reason": "Partial TP 1/2",
                "pnl": 4.0,
                "metadata": {},
            },
            {
                "trade_id": "abc123",
                "asset": "BNB-USD",
                "category": "crypto",
                "direction": "BUY",
                "entry_time": "2026-04-02T10:00:00+00:00",
                "exit_time": "2026-04-02T11:00:00+00:00",
                "exit_reason": "Trailing Exit",
                "pnl": 12.0,
                "metadata": {},
            },
        ],
    )

    asyncio.run(
        tg_mod.TelegramCommander._show_history(
            SimpleNamespace(),
            _fake_send,
            filter_cat="all",
        )
    )

    assert "Partial TP x1 -> Trailing Exit" in captured["text"]
    assert "Total +16.00" in captured["text"]
    assert "Partial TP 1/2" not in captured["text"]

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

def test_engine_uses_configured_trade_close_cooldown() -> None:
    source = Path("core/engine.py").read_text(encoding="utf-8")

    assert "TRADE_CLOSE_COOLDOWN_MINUTES = CONFIG_TRADE_CLOSE_COOLDOWN_MINUTES" in source

def test_execute_signal_treats_category_caps_as_soft(monkeypatch) -> None:
    engine_mod = importlib.import_module("core.engine")
    config_mod = importlib.import_module("config.config")

    monkeypatch.setattr(config_mod, "CATEGORY_CAPS", {"forex": 2}, raising=False)
    monkeypatch.setattr(config_mod, "CATEGORY_CAP_SOFT_BUFFER", 2, raising=False)

    risk_called = {"value": False}

    class _Risk:
        def validate_signal(self, **kwargs):
            risk_called["value"] = True
            return False, "risk-stop"

    class _State:
        daily_pnl = 0.0

        @staticmethod
        def open_position_count():
            return 0

        @staticmethod
        def get_open_positions():
            return [{"category": "forex"}, {"category": "forex"}]

    core = engine_mod.TradingCore.__new__(engine_mod.TradingCore)
    core.state = _State()
    core._risk_manager = _Risk()

    signal = SimpleNamespace(category="forex", asset="EUR/USD", confidence=0.82)
    approved = engine_mod.TradingCore._execute_signal(core, signal)

    assert approved is False
    assert risk_called["value"] is True

def test_portfolio_risk_allows_same_direction_cluster_when_category_exposure_is_small() -> None:
    portfolio_mod = importlib.import_module("risk.portfolio_risk")
    engine = portfolio_mod.PortfolioRiskEngine(
        max_single_asset_pct=80.0,
        max_category_pct=60.0,
        max_same_direction_positions=4,
        correlation_category_trigger_pct=85.0,
        target_allocation={"forex": 60.0},
    )

    open_positions = [
        {"asset": "EUR/USD", "category": "forex", "direction": "BUY", "position_size": 20_000.0, "entry_price": 1.10},
        {"asset": "GBP/USD", "category": "forex", "direction": "BUY", "position_size": 20_000.0, "entry_price": 1.28},
        {"asset": "AUD/USD", "category": "forex", "direction": "BUY", "position_size": 20_000.0, "entry_price": 0.67},
        {"asset": "USD/CAD", "category": "forex", "direction": "BUY", "position_size": 20_000.0, "entry_price": 1.35},
    ]
    signal = {
        "asset": "EUR/JPY",
        "category": "forex",
        "direction": "BUY",
        "position_size": 20_000.0,
        "entry_price": 162.0,
    }

    approved, reason = engine.evaluate(signal, open_positions=open_positions, balance=10_000.0)

    assert approved is True
    assert "Correlation risk" not in reason

def test_portfolio_risk_blocks_same_direction_cluster_only_when_category_exposure_is_high() -> None:
    portfolio_mod = importlib.import_module("risk.portfolio_risk")
    engine = portfolio_mod.PortfolioRiskEngine(
        max_single_asset_pct=80.0,
        max_category_pct=60.0,
        max_same_direction_positions=4,
        correlation_category_trigger_pct=85.0,
        target_allocation={"forex": 60.0},
    )

    open_positions = [
        {"asset": "EUR/USD", "category": "forex", "direction": "BUY", "position_size": 120_000.0, "entry_price": 1.10},
        {"asset": "GBP/USD", "category": "forex", "direction": "BUY", "position_size": 120_000.0, "entry_price": 1.28},
        {"asset": "AUD/USD", "category": "forex", "direction": "BUY", "position_size": 120_000.0, "entry_price": 0.67},
        {"asset": "USD/CAD", "category": "forex", "direction": "BUY", "position_size": 120_000.0, "entry_price": 1.35},
    ]
    signal = {
        "asset": "EUR/JPY",
        "category": "forex",
        "direction": "BUY",
        "position_size": 120_000.0,
        "entry_price": 162.0,
    }

    approved, reason = engine.evaluate(signal, open_positions=open_positions, balance=10_000.0)

    assert approved is False
    assert "Correlation risk" in reason

def _build_trend_frame(start: float, step: float, rows: int = 90) -> pd.DataFrame:
    closes = [start + step * i for i in range(rows)]
    highs = [value + abs(step) * 0.8 + 0.4 for value in closes]
    lows = [value - abs(step) * 0.8 - 0.4 for value in closes]
    opens = [closes[0]] + closes[:-1]
    volume = [1_000 + i * 5 for i in range(rows)]
    return pd.DataFrame(
        {
            "open": opens,
            "high": highs,
            "low": lows,
            "close": closes,
            "volume": volume,
        }
    )

def _build_breakout_frame(start: float = 1.1500, rows: int = 70) -> pd.DataFrame:
    closes = []
    value = start
    for i in range(rows - 6):
        value += 0.00003 if i % 2 == 0 else -0.00002
        closes.append(round(value, 6))
    base = max(closes[-12:])
    closes.extend(
        [
            round(base - 0.00004, 6),
            round(base - 0.00001, 6),
            round(base + 0.00002, 6),
            round(base + 0.00018, 6),
            round(base + 0.00042, 6),
            round(base + 0.00065, 6),
        ]
    )
    opens = [closes[0]] + closes[:-1]
    highs = [max(o, c) + 0.00008 for o, c in zip(opens, closes)]
    lows = [min(o, c) - 0.00007 for o, c in zip(opens, closes)]
    volume = [900 + i * 4 for i in range(rows - 6)] + [1300, 1350, 1500, 1800, 2100, 2400]
    return pd.DataFrame(
        {
            "open": opens,
            "high": highs,
            "low": lows,
            "close": closes,
            "volume": volume,
        }
    )

def _build_breakout_retest_frame(start: float = 1.1500, rows: int = 70) -> pd.DataFrame:
    closes = []
    value = start
    for i in range(rows - 8):
        value += 0.00002 if i % 2 == 0 else -0.000015
        closes.append(round(value, 6))

    opens = [closes[0]] + closes[:-1]
    highs = [max(o, c) + 0.00005 for o, c in zip(opens, closes)]
    lows = [min(o, c) - 0.00005 for o, c in zip(opens, closes)]
    volume = [850 + i * 3 for i in range(rows - 8)]

    range_high = max(highs[-18:])
    retest_closes = [
        round(range_high + 0.00018, 6),
        round(range_high + 0.00022, 6),
        round(range_high + 0.00010, 6),
        round(range_high + 0.00001, 6),
    ]

    for close_value in retest_closes:
        open_value = closes[-1] if closes else start
        high_value = max(open_value, close_value) + 0.00005
        low_value = min(open_value, close_value) - 0.00004
        closes.append(close_value)
        opens.append(round(open_value, 6))
        highs.append(round(high_value, 6))
        lows.append(round(low_value, 6))
        volume.append(volume[-1] + 120 if volume else 1100)

    lows[-1] = round(range_high - 0.00002, 6)
    highs[-1] = round(max(opens[-1], closes[-1]) + 0.00004, 6)

    return pd.DataFrame(
        {
            "open": opens,
            "high": highs,
            "low": lows,
            "close": closes,
            "volume": volume,
        }
    )

def _build_reversal_sweep_frame(start: float = 159.20, rows: int = 70) -> pd.DataFrame:
    closes = []
    value = start
    for i in range(rows - 1):
        value += 0.01 if i % 3 != 0 else -0.004
        closes.append(round(value, 3))

    opens = [closes[0]] + closes[:-1]
    highs = [round(max(o, c) + 0.03, 3) for o, c in zip(opens, closes)]
    lows = [round(min(o, c) - 0.03, 3) for o, c in zip(opens, closes)]
    volume = [900 + i * 6 for i in range(rows - 1)]

    range_high = max(highs[-18:])
    reversal_close = round(range_high - 0.05, 3)
    reversal_open = round(range_high + 0.03, 3)
    opens.append(reversal_open)
    closes.append(reversal_close)
    highs.append(round(range_high + 0.08, 3))
    lows.append(round(reversal_close - 0.03, 3))
    volume.append(volume[-1] + 250)

    return pd.DataFrame(
        {
            "open": opens,
            "high": highs,
            "low": lows,
            "close": closes,
            "volume": volume,
        }
    )

def _build_aggressive_breakdown_frame(start: float = 159.60, rows: int = 70) -> pd.DataFrame:
    closes = []
    value = start
    for i in range(rows - 1):
        value += 0.004 if i % 4 != 0 else -0.003
        closes.append(round(value, 3))

    opens = [closes[0]] + closes[:-1]
    highs = [round(max(o, c) + 0.02, 3) for o, c in zip(opens, closes)]
    lows = [round(min(o, c) - 0.02, 3) for o, c in zip(opens, closes)]
    volume = [850 + i * 5 for i in range(rows - 1)]

    prior_low = min(lows[-12:])
    latest_open = round(closes[-1] + 0.02, 3)
    latest_close = round(prior_low - 0.09, 3)
    opens.append(latest_open)
    closes.append(latest_close)
    highs.append(round(latest_open + 0.01, 3))
    lows.append(round(latest_close - 0.02, 3))
    volume.append(volume[-1] + 320)

    return pd.DataFrame(
        {
            "open": opens,
            "high": highs,
            "low": lows,
            "close": closes,
            "volume": volume,
        }
    )

def _build_exhausted_uptrend_frame(start: float = 100.0, rows: int = 90) -> pd.DataFrame:
    closes = []
    value = start
    for i in range(rows - 5):
        value += 0.28 if i % 3 != 0 else 0.12
        closes.append(round(value, 4))

    for bump in (1.8, 2.6, 3.4, 4.2, 5.0):
        value += bump
        closes.append(round(value, 4))

    opens = [closes[0]] + closes[:-1]
    highs = [round(max(o, c) + 0.45, 4) for o, c in zip(opens, closes)]
    lows = [round(min(o, c) - 0.35, 4) for o, c in zip(opens, closes)]
    volume = [1000 + i * 7 for i in range(rows - 5)] + [2100, 2400, 2700, 3000, 3300]

    return pd.DataFrame(
        {
            "open": opens,
            "high": highs,
            "low": lows,
            "close": closes,
            "volume": volume,
        }
    )

def _build_early_inflection_frame(start: float = 1.1500, rows: int = 70) -> pd.DataFrame:
    closes = []
    value = start
    for i in range(rows - 1):
        value += 0.00007 if i % 3 != 0 else 0.00003
        closes.append(round(value, 6))

    opens = [closes[0]] + closes[:-1]
    highs = [round(max(o, c) + 0.00006, 6) for o, c in zip(opens, closes)]
    lows = [round(min(o, c) - 0.00005, 6) for o, c in zip(opens, closes)]
    volume = [950 + i * 4 for i in range(rows - 1)]

    range_high = max(highs[-18:])
    latest_open = round(range_high - 0.00001, 6)
    latest_close = round(range_high - 0.00018, 6)
    opens.append(latest_open)
    closes.append(latest_close)
    highs.append(round(range_high - 0.000005, 6))
    lows.append(round(latest_close - 0.00005, 6))
    volume.append(volume[-1] + 180)

    return pd.DataFrame(
        {
            "open": opens,
            "high": highs,
            "low": lows,
            "close": closes,
            "volume": volume,
        }
    )

def test_market_structure_service_detects_aligned_buy_setup() -> None:
    svc_mod = importlib.import_module("services.market_structure_service")
    service = svc_mod.get_service()

    structure = service.analyze(
        "BTC-USD",
        "crypto",
        {
            "15m": _build_trend_frame(100.0, 0.5),
            "1h": _build_trend_frame(100.0, 1.2),
            "4h": _build_trend_frame(100.0, 2.8),
        },
    )

    assert structure["structure_bias"] == "buy"
    assert structure["alignment_score"] > 0.5
    assert structure["setup_quality"] > 0.3
    assert structure["trend_15m"] == "trending_up"

def test_market_structure_service_flags_upside_exhaustion_on_extended_buy_move() -> None:
    svc_mod = importlib.import_module("services.market_structure_service")
    service = svc_mod.get_service()

    structure = service.analyze(
        "XAU/USD",
        "commodities",
        {
            "15m": _build_exhausted_uptrend_frame(),
            "1h": _build_exhausted_uptrend_frame(start=102.0),
            "4h": _build_exhausted_uptrend_frame(start=105.0),
        },
    )

    assert structure["structure_bias"] == "buy"
    assert structure["upside_exhaustion_score"] > 0.55
    assert structure["bias_exhausted"] is True

def test_generate_seed_signal_uses_market_structure_alignment() -> None:
    core = TradingCore.__new__(TradingCore)
    core._predictor = SimpleNamespace(predict=lambda *args, **kwargs: (0.72, 0.60))
    core._risk_manager = SimpleNamespace(
        get_stop_loss=lambda entry, direction, category, atr=0.0: entry - 10.0,
        get_take_profit=lambda entry, stop_loss, direction, category="": entry + 15.0,
    )

    svc_mod = importlib.import_module("services.market_structure_service")
    structure = svc_mod.get_service().analyze(
        "BTC-USD",
        "crypto",
        {
            "15m": _build_trend_frame(100.0, 0.5),
            "1h": _build_trend_frame(100.0, 1.2),
            "4h": _build_trend_frame(100.0, 2.8),
        },
    )
    price_data = _build_trend_frame(100.0, 0.5)
    ctx = {
        "sentiment_score": 0.1,
        "regime": "unknown",
        "market_data": {},
        "market_structure": structure,
    }

    signal = TradingCore._generate_seed_signal(core, "BTC-USD", "BTC-USD", "crypto", price_data, ctx)

    assert signal is not None
    assert signal.direction == "BUY"
    assert signal.confidence > 0.60
    assert signal.metadata["structure_bias"] == "buy"
    assert signal.metadata["setup_quality"] == structure["setup_quality"]

def test_playbook_service_seeds_breakout_when_classifier_is_neutral() -> None:
    svc_mod = importlib.import_module("services.playbook_service")
    service = svc_mod.get_service()
    price_data = _build_breakout_frame()
    context = {
        "market_structure": {
            "structure_bias": "buy",
            "alignment_score": 0.74,
            "setup_quality": 0.71,
            "pullback_score": 0.12,
            "breakout_score": 0.79,
            "volatility_state": "expansion",
            "regime": "trending_up",
            "trend_15m": "trending_up",
            "trend_1h": "trending_up",
            "distance_to_support": 0.0018,
            "distance_to_resistance": 0.0009,
        }
    }

    pick = service.pick_seed(
        "EUR/USD",
        "forex",
        price_data,
        context,
        ml_direction="",
        ml_confidence=0.05,
    )

    assert pick["action"] == "seed"
    assert pick["primary"]["playbook"] == "breakout_continuation"
    assert pick["primary"]["direction"] == "BUY"
    assert float(pick["primary"]["confidence"]) >= 0.58

def test_generate_seed_signal_uses_playbook_when_classifier_is_neutral() -> None:
    core = TradingCore.__new__(TradingCore)
    core._predictor = SimpleNamespace(predict=lambda *args, **kwargs: (0.5, 0.05))
    core._risk_manager = SimpleNamespace(
        get_stop_loss=lambda entry, direction, category, atr=0.0: entry - 0.0009,
        get_take_profit=lambda entry, stop_loss, direction, category="", rr_multiplier=1.0: entry + 0.0016,
    )

    price_data = _build_breakout_frame()
    ctx = {
        "sentiment_score": 0.08,
        "regime": "unknown",
        "market_data": {},
        "market_structure": {
            "structure_bias": "buy",
            "alignment_score": 0.74,
            "setup_quality": 0.71,
            "pullback_score": 0.12,
            "breakout_score": 0.79,
            "volatility_state": "expansion",
            "regime": "trending_up",
            "trend_15m": "trending_up",
            "trend_1h": "trending_up",
            "distance_to_support": 0.0018,
            "distance_to_resistance": 0.0009,
        },
    }

    signal = TradingCore._generate_seed_signal(core, "EUR/USD", "EUR/USD", "forex", price_data, ctx)

    assert signal is not None
    assert signal.direction == "BUY"
    assert signal.metadata["seed_source"] == "playbook"
    assert signal.metadata["playbook_name"] == "breakout_continuation"
    assert signal.strategy_id == "playbook_breakout_continuation"

def test_playbook_service_supports_all_asset_categories(monkeypatch) -> None:
    svc_mod = importlib.import_module("services.playbook_service")
    monkeypatch.setattr(
        svc_mod,
        "_utc_now",
        lambda: datetime(2026, 4, 6, 15, 0, tzinfo=timezone.utc),
    )
    service = svc_mod.get_service()
    context = {
        "market_structure": {
            "structure_bias": "buy",
            "alignment_score": 0.76,
            "setup_quality": 0.73,
            "pullback_score": 0.16,
            "breakout_score": 0.81,
            "volatility_state": "expansion",
            "regime": "trending_up",
            "trend_15m": "trending_up",
            "trend_1h": "trending_up",
            "distance_to_support": 0.0016,
            "distance_to_resistance": 0.0008,
        }
    }

    for asset, category, start in (
        ("BTC-USD", "crypto", 100.0),
        ("XAU/USD", "commodities", 4600.0),
        ("US500", "indices", 5800.0),
    ):
        pick = service.pick_seed(
            asset,
            category,
            _build_breakout_frame(start=start),
            context,
            ml_direction="",
            ml_confidence=0.04,
        )
        assert pick["action"] == "seed"
        assert pick["primary"]["direction"] == "BUY"
        assert pick["primary"]["preferred_interval"] == "5m"

def test_playbook_service_applies_asset_specific_index_sessions(monkeypatch) -> None:
    svc_mod = importlib.import_module("services.playbook_service")
    monkeypatch.setattr(
        svc_mod,
        "_utc_now",
        lambda: datetime(2026, 4, 6, 8, 0, tzinfo=timezone.utc),
    )
    service = svc_mod.get_service()
    context = {
        "market_structure": {
            "structure_bias": "buy",
            "alignment_score": 0.74,
            "setup_quality": 0.71,
            "pullback_score": 0.14,
            "breakout_score": 0.80,
            "volatility_state": "expansion",
            "regime": "trending_up",
            "trend_15m": "trending_up",
            "trend_1h": "trending_up",
            "distance_to_support": 0.0018,
            "distance_to_resistance": 0.0009,
        }
    }

    blocked = service.pick_seed(
        "US500",
        "indices",
        _build_breakout_frame(start=5800.0),
        context,
        ml_direction="",
        ml_confidence=0.03,
    )
    assert blocked["action"] == ""
    assert blocked["blocked_reason"].startswith("session_block:")

    allowed = service.pick_seed(
        "UK100",
        "indices",
        _build_breakout_frame(start=8600.0),
        context,
        ml_direction="",
        ml_confidence=0.03,
    )
    assert allowed["action"] == "seed"
    assert allowed["primary"]["direction"] == "BUY"

def test_playbook_service_detects_breakout_retest_entry(monkeypatch) -> None:
    svc_mod = importlib.import_module("services.playbook_service")
    monkeypatch.setattr(
        svc_mod,
        "_utc_now",
        lambda: datetime(2026, 4, 6, 9, 0, tzinfo=timezone.utc),
    )
    service = svc_mod.get_service()
    analysis = service.analyze(
        "EUR/USD",
        "forex",
        _build_breakout_retest_frame(),
        context={
            "market_structure": {
                "structure_bias": "buy",
                "alignment_score": 0.68,
                "setup_quality": 0.69,
                "pullback_score": 0.10,
                "breakout_score": 0.72,
                "volatility_state": "normal",
                "regime": "trending_up",
                "trend_15m": "trending_up",
                "trend_1h": "trending_up",
                "distance_to_support": 0.0017,
                "distance_to_resistance": 0.0007,
            }
        },
    )

    assert any(candidate["playbook"] == "breakout_retest" for candidate in analysis["candidates"])
    retest = next(candidate for candidate in analysis["candidates"] if candidate["playbook"] == "breakout_retest")
    assert retest["direction"] == "BUY"
    assert retest["entry_style"] == "retest_hold"

def test_playbook_service_blocks_long_trend_pullback_when_upside_exhausted(monkeypatch) -> None:
    svc_mod = importlib.import_module("services.playbook_service")
    monkeypatch.setattr(
        svc_mod,
        "_utc_now",
        lambda: datetime(2026, 4, 7, 9, 0, tzinfo=timezone.utc),
    )
    service = svc_mod.get_service()

    candidate = service._trend_pullback(
        _build_trend_frame(100.0, 0.35),
        asset="XAU/USD",
        category="commodities",
        session="europe_core",
        structure={
            "structure_bias": "buy",
            "alignment_score": 0.74,
            "setup_quality": 0.72,
            "pullback_score": 0.78,
            "breakout_score": 0.12,
            "volatility_state": "normal",
            "regime": "trending_up",
            "trend_15m": "trending_up",
            "trend_1h": "trending_up",
            "distance_to_support": 0.0016,
            "distance_to_resistance": 0.0004,
            "upside_exhaustion_score": 0.71,
            "downside_exhaustion_score": 0.0,
        },
    )

    assert candidate is None

def test_playbook_service_detects_early_inflection_for_selected_asset(monkeypatch) -> None:
    svc_mod = importlib.import_module("services.playbook_service")
    monkeypatch.setattr(
        svc_mod,
        "_utc_now",
        lambda: datetime(2026, 4, 7, 9, 30, tzinfo=timezone.utc),
    )
    service = svc_mod.get_service()
    analysis = service.analyze(
        "EUR/USD",
        "forex",
        _build_early_inflection_frame(),
        context={
            "market_structure": {
                "structure_bias": "buy",
                "alignment_score": 0.71,
                "setup_quality": 0.69,
                "pullback_score": 0.22,
                "breakout_score": 0.41,
                "volatility_state": "normal",
                "regime": "trending_up",
                "trend_15m": "trending_up",
                "trend_1h": "trending_up",
                "distance_to_support": 0.0019,
                "distance_to_resistance": 0.0005,
                "upside_exhaustion_score": 0.63,
                "downside_exhaustion_score": 0.0,
            }
        },
    )

    assert any(candidate["playbook"] == "early_inflection" for candidate in analysis["candidates"])
    inflection = next(candidate for candidate in analysis["candidates"] if candidate["playbook"] == "early_inflection")
    assert inflection["direction"] == "SELL"
    assert inflection["entry_style"] == "early_inflection_turn"

def test_playbook_service_does_not_enable_early_inflection_for_non_selected_asset(monkeypatch) -> None:
    svc_mod = importlib.import_module("services.playbook_service")
    monkeypatch.setattr(
        svc_mod,
        "_utc_now",
        lambda: datetime(2026, 4, 7, 9, 30, tzinfo=timezone.utc),
    )
    service = svc_mod.get_service()
    analysis = service.analyze(
        "XAG/USD",
        "commodities",
        _build_early_inflection_frame(start=72.0),
        context={
            "market_structure": {
                "structure_bias": "buy",
                "alignment_score": 0.71,
                "setup_quality": 0.69,
                "pullback_score": 0.22,
                "breakout_score": 0.41,
                "volatility_state": "normal",
                "regime": "trending_up",
                "trend_15m": "trending_up",
                "trend_1h": "trending_up",
                "distance_to_support": 0.0019,
                "distance_to_resistance": 0.0005,
                "upside_exhaustion_score": 0.63,
                "downside_exhaustion_score": 0.0,
            }
        },
    )

    assert not any(candidate["playbook"] == "early_inflection" for candidate in analysis["candidates"])

def test_playbook_service_requires_dual_trend_confirmation_for_long_pullback(monkeypatch) -> None:
    svc_mod = importlib.import_module("services.playbook_service")
    monkeypatch.setattr(
        svc_mod,
        "_utc_now",
        lambda: datetime(2026, 4, 7, 9, 0, tzinfo=timezone.utc),
    )
    service = svc_mod.get_service()

    candidate = service._trend_pullback(
        _build_trend_frame(1.1500, 0.0002),
        asset="EUR/USD",
        category="forex",
        session="europe_core",
        structure={
            "structure_bias": "buy",
            "alignment_score": 0.72,
            "setup_quality": 0.70,
            "pullback_score": 0.74,
            "breakout_score": 0.10,
            "volatility_state": "normal",
            "regime": "trending_up",
            "trend_15m": "trending_up",
            "trend_1h": "ranging",
            "distance_to_support": 0.0014,
            "distance_to_resistance": 0.0010,
            "upside_exhaustion_score": 0.18,
            "downside_exhaustion_score": 0.0,
        },
    )

    assert candidate is None

def test_playbook_service_detects_reversal_exhaustion(monkeypatch) -> None:
    svc_mod = importlib.import_module("services.playbook_service")
    monkeypatch.setattr(
        svc_mod,
        "_utc_now",
        lambda: datetime(2026, 4, 6, 13, 0, tzinfo=timezone.utc),
    )
    service = svc_mod.get_service()
    analysis = service.analyze(
        "USD/JPY",
        "forex",
        _build_reversal_sweep_frame(),
        context={
            "market_structure": {
                "structure_bias": "buy",
                "alignment_score": 0.66,
                "setup_quality": 0.67,
                "pullback_score": 0.08,
                "breakout_score": 0.70,
                "volatility_state": "expansion",
                "regime": "trending_up",
                "trend_15m": "trending_up",
                "trend_1h": "trending_up",
                "distance_to_support": 0.0016,
                "distance_to_resistance": 0.0006,
            }
        },
    )

    assert any(candidate["playbook"] == "reversal_exhaustion" for candidate in analysis["candidates"])
    reversal = next(candidate for candidate in analysis["candidates"] if candidate["playbook"] == "reversal_exhaustion")
    assert reversal["direction"] == "SELL"
    assert reversal["entry_style"] == "reclaim_reversal"

def test_playbook_service_detects_reversal_exhaustion_for_index(monkeypatch) -> None:
    svc_mod = importlib.import_module("services.playbook_service")
    monkeypatch.setattr(
        svc_mod,
        "_utc_now",
        lambda: datetime(2026, 4, 6, 15, 30, tzinfo=timezone.utc),
    )
    service = svc_mod.get_service()
    analysis = service.analyze(
        "US100",
        "indices",
        _build_reversal_sweep_frame(start=24100.0),
        context={
            "market_structure": {
                "structure_bias": "buy",
                "alignment_score": 0.69,
                "setup_quality": 0.68,
                "pullback_score": 0.07,
                "breakout_score": 0.73,
                "volatility_state": "expansion",
                "regime": "trending_up",
                "trend_15m": "trending_up",
                "trend_1h": "trending_up",
                "distance_to_support": 0.0019,
                "distance_to_resistance": 0.0006,
            }
        },
    )

    assert any(candidate["playbook"] == "reversal_exhaustion" for candidate in analysis["candidates"])
    reversal = next(candidate for candidate in analysis["candidates"] if candidate["playbook"] == "reversal_exhaustion")
    assert reversal["direction"] == "SELL"

def test_playbook_service_detects_reversal_exhaustion_for_alt_crypto(monkeypatch) -> None:
    svc_mod = importlib.import_module("services.playbook_service")
    monkeypatch.setattr(
        svc_mod,
        "_utc_now",
        lambda: datetime(2026, 4, 6, 16, 0, tzinfo=timezone.utc),
    )
    service = svc_mod.get_service()
    analysis = service.analyze(
        "BNB-USD",
        "crypto",
        _build_reversal_sweep_frame(start=600.0),
        context={
            "market_structure": {
                "structure_bias": "buy",
                "alignment_score": 0.70,
                "setup_quality": 0.69,
                "pullback_score": 0.06,
                "breakout_score": 0.74,
                "volatility_state": "expansion",
                "regime": "trending_up",
                "trend_15m": "trending_up",
                "trend_1h": "trending_up",
                "distance_to_support": 0.0017,
                "distance_to_resistance": 0.0005,
            }
        },
    )

    assert any(candidate["playbook"] == "reversal_exhaustion" for candidate in analysis["candidates"])
    reversal = next(candidate for candidate in analysis["candidates"] if candidate["playbook"] == "reversal_exhaustion")
    assert reversal["direction"] == "SELL"

def test_playbook_service_detects_aggressive_expansion_trigger(monkeypatch) -> None:
    svc_mod = importlib.import_module("services.playbook_service")
    monkeypatch.setattr(
        svc_mod,
        "_utc_now",
        lambda: datetime(2026, 4, 6, 13, 0, tzinfo=timezone.utc),
    )
    service = svc_mod.get_service()
    analysis = service.analyze(
        "USD/JPY",
        "forex",
        _build_aggressive_breakdown_frame(),
        context={
            "market_structure": {
                "structure_bias": "sell",
                "alignment_score": 0.71,
                "setup_quality": 0.70,
                "pullback_score": -0.12,
                "breakout_score": -0.74,
                "volatility_state": "expansion",
                "regime": "trending_down",
                "trend_15m": "trending_down",
                "trend_1h": "trending_down",
                "distance_to_support": 0.0004,
                "distance_to_resistance": 0.0018,
            }
        },
    )

    assert any(candidate["playbook"] == "aggressive_expansion" for candidate in analysis["candidates"])
    expansion = next(candidate for candidate in analysis["candidates"] if candidate["playbook"] == "aggressive_expansion")
    assert expansion["direction"] == "SELL"
    assert expansion["entry_style"] == "expansion_break"

def test_playbook_service_detects_opening_drive_for_us_index(monkeypatch) -> None:
    svc_mod = importlib.import_module("services.playbook_service")
    monkeypatch.setattr(
        svc_mod,
        "_utc_now",
        lambda: datetime(2026, 4, 6, 15, 0, tzinfo=timezone.utc),
    )
    service = svc_mod.get_service()
    analysis = service.analyze(
        "US500",
        "indices",
        _build_breakout_frame(start=5800.0),
        context={
            "market_structure": {
                "structure_bias": "buy",
                "alignment_score": 0.74,
                "setup_quality": 0.72,
                "pullback_score": 0.11,
                "breakout_score": 0.82,
                "volatility_state": "expansion",
                "regime": "trending_up",
                "trend_15m": "trending_up",
                "trend_1h": "trending_up",
                "distance_to_support": 1.8,
                "distance_to_resistance": 0.8,
            }
        },
    )

    assert any(candidate["playbook"] == "opening_drive" for candidate in analysis["candidates"])
    opening_drive = next(candidate for candidate in analysis["candidates"] if candidate["playbook"] == "opening_drive")
    assert opening_drive["direction"] == "BUY"
    assert opening_drive["entry_style"] == "opening_drive_break"

def test_playbook_service_blocks_us_index_after_open_window(monkeypatch) -> None:
    svc_mod = importlib.import_module("services.playbook_service")
    monkeypatch.setattr(
        svc_mod,
        "_utc_now",
        lambda: datetime(2026, 4, 6, 18, 0, tzinfo=timezone.utc),
    )
    service = svc_mod.get_service()
    pick = service.pick_seed(
        "US500",
        "indices",
        _build_breakout_frame(start=5800.0),
        context={
            "market_structure": {
                "structure_bias": "buy",
                "alignment_score": 0.74,
                "setup_quality": 0.72,
                "pullback_score": 0.11,
                "breakout_score": 0.82,
                "volatility_state": "expansion",
                "regime": "trending_up",
                "trend_15m": "trending_up",
                "trend_1h": "trending_up",
                "distance_to_support": 1.8,
                "distance_to_resistance": 0.8,
            }
        },
        ml_direction="",
        ml_confidence=0.03,
    )

    assert pick["action"] == ""
    assert pick["blocked_reason"].startswith("session_block:us_core")

def test_playbook_service_detects_news_impulse_for_major_forex(monkeypatch) -> None:
    svc_mod = importlib.import_module("services.playbook_service")
    monkeypatch.setattr(
        svc_mod,
        "_utc_now",
        lambda: datetime(2026, 4, 6, 13, 30, tzinfo=timezone.utc),
    )
    service = svc_mod.get_service()
    analysis = service.analyze(
        "EUR/USD",
        "forex",
        _build_breakout_frame(),
        context={
            "news_event": {
                "state": "active",
                "event": "ECB surprise",
                "impact": "HIGH",
                "direction": "BUY",
                "mins_to": 0,
            },
            "market_structure": {
                "structure_bias": "buy",
                "alignment_score": 0.75,
                "setup_quality": 0.73,
                "pullback_score": 0.12,
                "breakout_score": 0.82,
                "volatility_state": "expansion",
                "regime": "trending_up",
                "trend_15m": "trending_up",
                "trend_1h": "trending_up",
                "distance_to_support": 0.0017,
                "distance_to_resistance": 0.0007,
            }
        },
    )

    assert any(candidate["playbook"] == "news_impulse" for candidate in analysis["candidates"])
    impulse = next(candidate for candidate in analysis["candidates"] if candidate["playbook"] == "news_impulse")
    assert impulse["direction"] == "BUY"
    assert impulse["entry_style"] == "news_followthrough"

def test_playbook_service_detects_crypto_orderflow_continuation(monkeypatch) -> None:
    svc_mod = importlib.import_module("services.playbook_service")
    monkeypatch.setattr(
        svc_mod,
        "_utc_now",
        lambda: datetime(2026, 4, 6, 16, 0, tzinfo=timezone.utc),
    )
    service = svc_mod.get_service()
    analysis = service.analyze(
        "BTC-USD",
        "crypto",
        _build_breakout_frame(start=68000.0),
        context={
            "market_microstructure": {
                "depth_available": True,
                "synthetic_depth_available": False,
                "book_imbalance": 0.44,
                "score": 0.41,
                "spread_bps": 8.0,
            },
            "market_structure": {
                "structure_bias": "buy",
                "alignment_score": 0.75,
                "setup_quality": 0.72,
                "pullback_score": 0.10,
                "breakout_score": 0.80,
                "volatility_state": "expansion",
                "regime": "trending_up",
                "trend_15m": "trending_up",
                "trend_1h": "trending_up",
                "distance_to_support": 8.0,
                "distance_to_resistance": 4.0,
            }
        },
    )

    assert any(candidate["playbook"] == "crypto_orderflow_continuation" for candidate in analysis["candidates"])
    orderflow = next(candidate for candidate in analysis["candidates"] if candidate["playbook"] == "crypto_orderflow_continuation")
    assert orderflow["direction"] == "BUY"
    assert orderflow["entry_style"] == "orderflow_break"

def test_playbook_service_blocks_major_forex_without_dual_trend_alignment(monkeypatch) -> None:
    svc_mod = importlib.import_module("services.playbook_service")
    monkeypatch.setattr(
        svc_mod,
        "_utc_now",
        lambda: datetime(2026, 4, 6, 9, 0, tzinfo=timezone.utc),
    )
    service = svc_mod.get_service()
    pick = service.pick_seed(
        "EUR/USD",
        "forex",
        _build_breakout_frame(),
        context={
            "market_structure": {
                "structure_bias": "buy",
                "alignment_score": 0.72,
                "setup_quality": 0.70,
                "pullback_score": 0.10,
                "breakout_score": 0.78,
                "volatility_state": "expansion",
                "regime": "trending_up",
                "trend_15m": "trending_up",
                "trend_1h": "ranging",
                "distance_to_support": 0.0016,
                "distance_to_resistance": 0.0008,
            }
        },
        ml_direction="",
        ml_confidence=0.03,
    )

    assert pick["action"] == ""
    assert pick["blocked_reason"].startswith("trend_misaligned:")

def test_playbook_service_limits_alt_crypto_to_liquid_sessions(monkeypatch) -> None:
    svc_mod = importlib.import_module("services.playbook_service")
    monkeypatch.setattr(
        svc_mod,
        "_utc_now",
        lambda: datetime(2026, 4, 6, 2, 0, tzinfo=timezone.utc),
    )
    service = svc_mod.get_service()
    pick = service.pick_seed(
        "BNB-USD",
        "crypto",
        _build_breakout_frame(start=600.0),
        context={
            "market_structure": {
                "structure_bias": "buy",
                "alignment_score": 0.76,
                "setup_quality": 0.74,
                "pullback_score": 0.12,
                "breakout_score": 0.81,
                "volatility_state": "expansion",
                "regime": "trending_up",
                "trend_15m": "trending_up",
                "trend_1h": "trending_up",
                "distance_to_support": 0.0030,
                "distance_to_resistance": 0.0012,
            }
        },
        ml_direction="",
        ml_confidence=0.02,
    )

    assert pick["action"] == ""
    assert pick["blocked_reason"].startswith("session_block:")

def test_playbook_service_keeps_wti_on_trend_playbooks_only(monkeypatch) -> None:
    svc_mod = importlib.import_module("services.playbook_service")
    monkeypatch.setattr(
        svc_mod,
        "_utc_now",
        lambda: datetime(2026, 4, 6, 15, 0, tzinfo=timezone.utc),
    )
    service = svc_mod.get_service()
    analysis = service.analyze(
        "WTI",
        "commodities",
        _build_reversal_sweep_frame(start=70.0),
        context={
            "market_structure": {
                "structure_bias": "buy",
                "alignment_score": 0.68,
                "setup_quality": 0.70,
                "pullback_score": 0.08,
                "breakout_score": 0.73,
                "volatility_state": "expansion",
                "regime": "trending_up",
                "trend_15m": "trending_up",
                "trend_1h": "trending_up",
                "distance_to_support": 0.0100,
                "distance_to_resistance": 0.0040,
            }
        },
    )

    assert all(candidate["playbook"] in {"breakout_continuation", "breakout_retest", "aggressive_expansion"} for candidate in analysis["candidates"])
    assert not any(candidate["playbook"] == "reversal_exhaustion" for candidate in analysis["candidates"])

def test_generate_seed_signal_builds_playbook_management_plan(monkeypatch) -> None:
    svc_mod = importlib.import_module("services.playbook_service")
    monkeypatch.setattr(
        svc_mod,
        "_utc_now",
        lambda: datetime(2026, 4, 6, 15, 0, tzinfo=timezone.utc),
    )

    core = TradingCore.__new__(TradingCore)
    core._predictor = SimpleNamespace(predict=lambda *args, **kwargs: (0.5, 0.05))
    core._risk_manager = SimpleNamespace(
        get_stop_loss=lambda entry, direction, category, atr=0.0: entry - 0.0009,
        get_take_profit=lambda entry, stop_loss, direction, category="", rr_multiplier=1.0: entry + 0.0016,
    )

    price_data = _build_breakout_frame()
    ctx = {
        "sentiment_score": 0.08,
        "regime": "unknown",
        "market_data": {},
        "timeframe": "15m",
        "market_structure": {
            "structure_bias": "buy",
            "alignment_score": 0.74,
            "setup_quality": 0.71,
            "pullback_score": 0.12,
            "breakout_score": 0.79,
            "volatility_state": "expansion",
            "regime": "trending_up",
            "trend_15m": "trending_up",
            "trend_1h": "trending_up",
            "distance_to_support": 0.0018,
            "distance_to_resistance": 0.0009,
        },
    }

    signal = TradingCore._generate_seed_signal(core, "XAU/USD", "XAU/USD", "commodities", price_data, ctx)

    assert signal is not None
    assert signal.metadata["playbook_name"] == "breakout_continuation"
    assert signal.metadata["playbook_timeframe"] == "5m"
    assert signal.metadata["playbook_entry_style"] == "breakout_close"
    assert signal.metadata["trade_management_plan"]["partial_take_profit_rr"] == [1.0]
    assert signal.metadata["trade_management_plan"]["runner_target_rr"] >= 2.2
    assert len(signal.take_profit_levels) >= 2
    risk_distance = abs(float(signal.entry_price) - float(signal.stop_loss))
    expected_first_tp = round(float(signal.entry_price) + risk_distance, 6)
    assert abs(float(signal.take_profit_levels[0]) - expected_first_tp) <= 1e-6

def test_playbook_service_uses_asset_specific_management_overrides(monkeypatch) -> None:
    svc_mod = importlib.import_module("services.playbook_service")
    monkeypatch.setattr(
        svc_mod,
        "_utc_now",
        lambda: datetime(2026, 4, 6, 16, 0, tzinfo=timezone.utc),
    )

    service = svc_mod.get_service()
    analysis = service.analyze(
        "WTI",
        "commodities",
        _build_breakout_frame(start=70.0),
        context={
            "news_event": {
                "state": "post",
                "event": "EIA crude surprise",
                "impact": "HIGH",
                "direction": "BUY",
                "mins_to": 5,
            },
            "market_structure": {
                "structure_bias": "buy",
                "alignment_score": 0.76,
                "setup_quality": 0.74,
                "pullback_score": 0.10,
                "breakout_score": 0.83,
                "volatility_state": "expansion",
                "regime": "trending_up",
                "trend_15m": "trending_up",
                "trend_1h": "trending_up",
                "distance_to_support": 0.40,
                "distance_to_resistance": 0.15,
            }
        },
    )

    primary = analysis["primary"]
    assert primary is not None
    management = primary["management"]
    assert service.preferred_interval("commodities", "WTI") == "15m"
    assert primary["preferred_interval"] == "15m"
    assert management["preferred_interval"] == "15m"
    assert management["runner_target_rr"] >= 2.5
    assert management["trail_activation_rr"] >= 1.0
    assert management["trail_atr_multiple"] >= 1.0

def test_generate_seed_signal_playbook_only_rejects_without_playbook_seed() -> None:
    core = TradingCore.__new__(TradingCore)
    core._predictor = SimpleNamespace(predict=lambda *args, **kwargs: (0.82, 0.76))
    core._risk_manager = SimpleNamespace(
        get_stop_loss=lambda entry, direction, category, atr=0.0: entry - 0.0009,
        get_take_profit=lambda entry, stop_loss, direction, category="", rr_multiplier=1.0: entry + 0.0016,
    )

    price_data = pd.DataFrame(
        {
            "open": np.full(60, 1.1500),
            "high": np.full(60, 1.1503),
            "low": np.full(60, 1.1497),
            "close": np.full(60, 1.1500),
            "volume": np.linspace(800.0, 900.0, 60),
        }
    )
    ctx = {
        "sentiment_score": 0.02,
        "regime": "ranging",
        "market_data": {},
        "timeframe": "15m",
        "market_structure": {
            "structure_bias": "neutral",
            "alignment_score": 0.08,
            "setup_quality": 0.10,
            "pullback_score": 0.0,
            "breakout_score": 0.0,
            "volatility_state": "normal",
            "regime": "ranging",
            "distance_to_support": 0.0040,
            "distance_to_resistance": 0.0040,
        },
    }

    signal = TradingCore._generate_seed_signal(core, "EUR/USD", "EUR/USD", "forex", price_data, ctx)

    assert signal is None
    assert ctx["seed_decision"]["reason"] == "no_playbook_seed"
    assert ctx["seed_decision"]["candidate_count"] == 0
    assert ctx["seed_decision"]["structure_bias"] == "neutral"
    assert ctx["seed_decision"]["alignment_score"] == 0.08
    assert ctx["seed_decision"]["setup_quality"] == 0.10
    assert ctx["playbook_decision"]["preferred_interval"] == "5m"

def test_generate_seed_signal_carries_playbook_rejection_details(monkeypatch) -> None:
    core = TradingCore.__new__(TradingCore)
    core._predictor = SimpleNamespace(predict=lambda *args, **kwargs: (0.50, 0.00))
    core._risk_manager = SimpleNamespace(
        get_stop_loss=lambda entry, direction, category, atr=0.0: entry - 0.0009,
        get_take_profit=lambda entry, stop_loss, direction, category="", rr_multiplier=1.0: entry + 0.0016,
    )

    class _FakePlaybookService:
        @staticmethod
        def preferred_interval(category: str, asset: str) -> str:
            return "5m"

        @staticmethod
        def pick_seed(*args, **kwargs):
            return {
                "action": "",
                "asset": "XAU/USD",
                "category": "commodities",
                "primary": None,
                "blocked_reason": "alignment_too_weak:breakout_continuation",
                "session": "us_open",
                "session_label": "us_open",
                "rejected_reasons": [
                    "alignment_too_weak:breakout_continuation",
                    "trend_misaligned:breakout_retest",
                ],
                "allowed_sessions": ["europe_open", "europe_core", "us_overlap", "us_open", "us_core"],
                "asset_plan": {"min_alignment_score": 0.58, "min_setup_quality": 0.57},
                "candidates": [],
            }

    playbook_mod = importlib.import_module("services.playbook_service")
    monkeypatch.setattr(playbook_mod, "get_service", lambda: _FakePlaybookService(), raising=False)

    price_data = pd.DataFrame(
        {
            "open": np.full(60, 3300.0),
            "high": np.full(60, 3301.0),
            "low": np.full(60, 3299.0),
            "close": np.full(60, 3300.0),
            "volume": np.linspace(800.0, 900.0, 60),
        }
    )
    ctx = {
        "sentiment_score": 0.06,
        "regime": "ranging",
        "market_data": {},
        "timeframe": "30m",
        "market_structure": {
            "structure_bias": "neutral",
            "alignment_score": 0.22,
            "setup_quality": 0.24,
            "pullback_score": 0.0,
            "breakout_score": 0.0,
            "volatility_state": "normal",
            "regime": "ranging",
        },
    }

    signal = TradingCore._generate_seed_signal(core, "XAU/USD", "XAU/USD", "commodities", price_data, ctx)

    assert signal is None
    assert ctx["seed_decision"]["reason"] == "alignment_too_weak:breakout_continuation"
    assert ctx["seed_decision"]["session"] == "us_open"
    assert ctx["seed_decision"]["playbook_timeframe"] == "5m"
    assert ctx["seed_decision"]["rejected_reasons"] == [
        "alignment_too_weak:breakout_continuation",
        "trend_misaligned:breakout_retest",
    ]
    assert ctx["playbook_decision"]["blocked_reason"] == "alignment_too_weak:breakout_continuation"
    assert ctx["playbook_decision"]["allowed_sessions"][-1] == "us_core"

def test_market_review_records_structure_context() -> None:
    decision_mod = importlib.import_module("core.decision_engine")
    svc_mod = importlib.import_module("services.market_structure_service")
    engine = decision_mod.SignalDecisionEngine()

    frames = {
        "15m": _build_trend_frame(100.0, 0.6),
        "1h": _build_trend_frame(100.0, 1.0),
        "4h": _build_trend_frame(100.0, 2.5),
    }
    structure = svc_mod.get_service().analyze("ETH-USD", "crypto", frames)
    signal = Signal(
        asset="ETH-USD",
        canonical_asset="ETH-USD",
        category="crypto",
        direction="BUY",
        confidence=0.70,
        entry_price=float(frames["15m"]["close"].iloc[-1]),
        stop_loss=float(frames["15m"]["close"].iloc[-1]) - 10.0,
        take_profit=float(frames["15m"]["close"].iloc[-1]) + 15.0,
    )
    context = {
        "ml_prediction": 0.7,
        "ml_confidence": 0.7,
        "spread": 0.05,
        "market_microstructure": {},
        "timeframe": "15m",
        "price_data": frames["15m"],
        "market_status": {"market_open": True, "reason": "open"},
        "market_structure": structure,
    }

    approved = engine._apply_market_review(signal, context)

    assert approved is True
    assert signal.metadata["structure_bias"] == "buy"
    assert signal.metadata["market_structure"]["structure_bias"] == "buy"
    assert signal.journal.entries[-1].data["market_structure"]["structure_bias"] == "buy"

def test_decision_engine_skips_policy_when_playbook_only_runtime(monkeypatch) -> None:
    decision_mod = importlib.import_module("core.decision_engine")
    monkeypatch.setattr(decision_mod, "PLAYBOOK_ONLY_RUNTIME", True, raising=False)
    engine = decision_mod.SignalDecisionEngine()

    signal = Signal(
        asset="EUR/USD",
        canonical_asset="EUR/USD",
        category="forex",
        direction="BUY",
        confidence=0.72,
        entry_price=1.1520,
        stop_loss=1.1508,
        take_profit=1.1548,
        strategy_id="playbook_breakout_continuation",
    )

    approved = engine._apply_policy_review(signal, {"price_data": _build_breakout_frame()})

    assert approved is True
    assert signal.metadata["agent_policy_status"] == "playbook_only"

def test_opportunity_ranker_prefers_higher_quality_signal() -> None:
    ranker_mod = importlib.import_module("services.opportunity_ranker")
    ranker = ranker_mod.get_service()

    strong = Signal(
        asset="BTC-USD",
        canonical_asset="BTC-USD",
        category="crypto",
        direction="BUY",
        confidence=0.86,
        entry_price=100.0,
        stop_loss=95.0,
        take_profit=112.0,
        risk_reward=2.4,
    )
    strong.metadata.update(
        {
            "setup_quality": 0.84,
            "alignment_score": 0.81,
            "pullback_score": 0.40,
            "breakout_score": 0.52,
            "sentiment_score": 0.32,
            "whale_dominant": "BUY",
            "whale_bull_weight": 0.78,
            "whale_bear_weight": 0.18,
            "orderflow_applicable": True,
            "orderflow_imbalance": 0.41,
            "broker_quality": {
                "score": 0.91,
                "quote_agreement_state": "strong",
                "quote_quality_state": "fresh",
                "spread_regime": "tight",
            },
            "microstructure_score": 0.54,
            "microstructure_alignment": 0.54,
            "tick_imbalance": 0.48,
            "book_imbalance": 0.36,
            "depth_available": True,
            "microstructure_source": "order_flow_true_depth",
            "cross_asset_score": 0.36,
            "cross_asset_alignment": 0.36,
            "cross_asset_confidence": 0.82,
            "cross_asset_state": "buy_support",
            "cross_asset_primary_peer": "ETH-USD",
        }
    )

    weak = Signal(
        asset="EUR/USD",
        canonical_asset="EUR/USD",
        category="forex",
        direction="BUY",
        confidence=0.71,
        entry_price=1.10,
        stop_loss=1.095,
        take_profit=1.107,
        risk_reward=1.4,
    )
    weak.metadata.update(
        {
            "setup_quality": 0.31,
            "alignment_score": 0.28,
            "pullback_score": 0.09,
            "breakout_score": 0.10,
            "sentiment_score": 0.05,
            "orderflow_applicable": False,
            "broker_quality": {
                "score": 0.18,
                "quote_agreement_state": "severe_divergence",
                "quote_quality_state": "stale",
                "spread_regime": "extreme",
            },
            "microstructure_score": -0.21,
            "microstructure_alignment": -0.21,
            "tick_imbalance": -0.35,
            "book_imbalance": -0.28,
            "synthetic_depth_available": True,
            "microstructure_source": "live_store_synthetic_depth",
            "cross_asset_score": -0.34,
            "cross_asset_alignment": -0.34,
            "cross_asset_confidence": 0.74,
            "cross_asset_state": "sell_support",
            "cross_asset_primary_peer": "WTI",
        }
    )

    state = SimpleNamespace(
        get_open_positions=lambda: [{"asset": "GBP/USD", "category": "forex", "direction": "BUY"}]
    )
    ranked = ranker.rank(
        [
            (weak, {"spread": 0.0025}),
            (strong, {"spread": 0.12}),
        ],
        state,
    )

    assert ranked[0][0].asset == "BTC-USD"
    assert ranked[0][0].metadata["opportunity_rank"] == 1
    assert ranked[0][0].metadata["opportunity_score"] >= ranked[1][0].metadata["opportunity_score"]
    assert "broker_quality" in ranked[0][0].metadata["opportunity_breakdown"]
    assert "microstructure" in ranked[0][0].metadata["opportunity_breakdown"]
    assert "cross_asset" in ranked[0][0].metadata["opportunity_breakdown"]

def test_trading_cycle_executes_ranked_survivors_first() -> None:
    core = TradingCore.__new__(TradingCore)
    core.state = SimpleNamespace(
        check_day_rollover=lambda: False,
        get_open_positions=lambda: [],
        daily_pnl=0.0,
    )
    core._risk_manager = None
    core._paper_trader = None
    core._stop_event = SimpleNamespace(is_set=lambda: False)

    sig_a = Signal(
        asset="EUR/USD",
        canonical_asset="EUR/USD",
        category="forex",
        direction="BUY",
        confidence=0.90,
    )
    sig_b = Signal(
        asset="BTC-USD",
        canonical_asset="BTC-USD",
        category="crypto",
        direction="SELL",
        confidence=0.91,
    )

    core._generate_signals = lambda: [(sig_a, {"x": 1}), (sig_b, {"x": 2})]
    core.decision_engine = SimpleNamespace(evaluate=lambda sig, ctx: sig)
    core._rank_survivors = lambda accepted_pairs: [accepted_pairs[1], accepted_pairs[0]]

    executed: List[str] = []
    core._execute_signal = lambda sig: executed.append(sig.asset) or True

    core._trading_cycle()

    assert executed == ["BTC-USD", "EUR/USD"]

def test_trading_core_top_ranked_opportunities_expose_broker_and_depth_fields() -> None:
    core = TradingCore.__new__(TradingCore)
    core.state = SimpleNamespace(get_open_positions=lambda: [])
    core._last_ranked_at_utc = "2026-04-05T08:00:00"
    core._last_ranked_opportunities = []

    signal = Signal(
        asset="XAU/USD",
        canonical_asset="XAU/USD",
        category="commodities",
        direction="BUY",
        confidence=0.74,
        entry_price=100.0,
        stop_loss=98.0,
        take_profit=104.0,
    )
    signal.metadata.update(
        {
            "opportunity_score": 0.86,
            "opportunity_rank": 1,
            "memory_score": 66.0,
            "execution_quality_score": 62.0,
            "regime": "trending_up",
            "setup_quality": 0.72,
            "broker_quality": {
                "score": 0.89,
                "primary_provider": "IG",
                "comparison_provider": "Deriv",
                "quote_agreement_state": "aligned",
                "quote_quality_state": "fresh",
                "spread_regime": "tight",
            },
            "broker_quality_score": 0.89,
            "market_microstructure": {
                "score": 0.31,
                "pressure_direction": "BUY",
                "depth_available": True,
                "synthetic_depth_available": False,
                "microstructure_source": "live_store_depth",
            },
            "microstructure_score": 0.31,
            "depth_available": True,
            "synthetic_depth_available": False,
            "microstructure_source": "live_store_depth",
            "cross_asset_score": 0.28,
            "cross_asset_alignment": 0.28,
            "cross_asset_confidence": 0.66,
            "cross_asset_state": "buy_support",
            "cross_asset_primary_peer": "XAG/USD",
            "cross_asset_primary_relation": "silver_confirmation",
            "opportunity_breakdown": {
                "broker_quality": 0.89,
                "microstructure": 0.66,
                "cross_asset": 0.63,
            },
        }
    )

    core._remember_ranked_opportunities([(signal, {"timeframe": "15m"})])
    ranked = core.get_top_ranked_opportunities(limit=1, refresh=False, include_positions=False)

    assert ranked[0]["broker_quality_score"] == 0.89
    assert ranked[0]["broker_primary_provider"] == "IG"
    assert ranked[0]["broker_agreement_state"] == "aligned"
    assert ranked[0]["microstructure_score"] == 0.31
    assert ranked[0]["depth_available"] is True
    assert ranked[0]["microstructure_source"] == "live_store_depth"
    assert ranked[0]["cross_asset_score"] == 0.28
    assert ranked[0]["cross_asset_primary_peer"] == "XAG/USD"

def test_signal_journal_to_dict_includes_factor_attribution_and_fingerprint() -> None:
    signal = Signal(
        asset="BTC-USD",
        canonical_asset="BTC-USD",
        category="crypto",
        direction="SELL",
        confidence=0.83,
        entry_price=65000.0,
        stop_loss=65650.0,
        take_profit=64050.0,
        risk_reward=1.8,
    )
    signal.metadata.update({
        "ml_prediction": 0.22,
        "ml_confidence": 0.81,
        "market_structure": {
            "structure_bias": "sell",
            "alignment_score": 0.77,
            "setup_quality": 0.73,
            "pullback_score": -0.54,
            "breakout_score": -0.38,
            "volatility_state": "expansion",
        },
        "structure_bias": "sell",
        "alignment_score": 0.77,
        "setup_quality": 0.73,
        "pullback_score": -0.54,
        "breakout_score": -0.38,
        "volatility_state": "expansion",
        "sentiment_score": -0.32,
        "whale_dominant": "SELL",
        "whale_bear_weight": 0.69,
        "orderflow_applicable": True,
        "orderflow_imbalance": -0.48,
        "agent_score": 0.88,
        "broker_quality": {
            "score": 0.87,
            "primary_provider": "IG",
            "comparison_provider": "Deriv",
            "quote_agreement_state": "strong",
            "quote_quality_state": "fresh",
            "spread_regime": "tight",
        },
        "broker_quality_score": 0.87,
        "market_microstructure": {
            "score": -0.44,
            "pressure_direction": "SELL",
            "stop_hunt_risk": 0.09,
            "exhaustion_risk": 0.12,
            "depth_available": True,
            "synthetic_depth_available": False,
            "microstructure_source": "order_flow_true_depth",
        },
        "microstructure_score": -0.44,
        "microstructure_alignment": 0.44,
        "depth_available": True,
        "synthetic_depth_available": False,
        "microstructure_source": "order_flow_true_depth",
        "cross_asset_score": -0.33,
        "cross_asset_alignment": 0.33,
        "cross_asset_confidence": 0.74,
        "cross_asset_state": "sell_support",
        "cross_asset_primary_peer": "US100",
        "cross_asset_primary_relation": "risk_on_beta",
        "opportunity_score": 0.8123,
        "opportunity_rank": 1,
        "opportunity_breakdown": {
            "confidence": 0.83,
            "structure": 0.78,
            "setup": 0.74,
            "sentiment": 0.66,
            "whales": 0.84,
            "order_flow": 0.74,
            "broker_quality": 0.87,
            "microstructure": 0.79,
            "cross_asset": 0.72,
            "risk_reward": 0.62,
            "spread": 0.81,
            "portfolio_fit": 0.73,
        },
        "governance_score": 92,
        "governance_grade": "A",
        "regime": "trending_down",
        "session": "asia",
    })
    signal.journal.record(
        layer=1,
        name="market",
        decision="PASS",
        reason="market review passed",
        conf_before=0.74,
        conf_after=0.79,
        data={
            "rr": 1.8,
            "spread_pct": 0.0007,
            "orderflow_imbalance": -0.48,
            "market_structure": {
                "structure_bias": "sell",
                "alignment_score": 0.77,
                "setup_quality": 0.73,
                "pullback_score": -0.54,
                "breakout_score": -0.38,
                "volatility_state": "expansion",
            },
            "regime": "trending_down",
            "session": "asia",
        },
    )
    signal.journal.record(
        layer=2,
        name="intelligence",
        decision="PASS",
        reason="intel aligned",
        conf_before=0.79,
        conf_after=0.82,
        data={
            "sentiment_score": -0.32,
            "whale_dominant": "SELL",
            "whale_ratio": 0.69,
        },
    )
    signal.journal.record(
        layer=4,
        name="governance",
        decision="PASS",
        reason="grade=A score=92",
        conf_before=0.82,
        conf_after=0.82,
        data={"valid_sources": 4, "min_required": 2, "score": 92, "grade": "A"},
    )
    signal.journal.record(
        layer=5,
        name="policy",
        decision="PASS",
        reason="policy accepted SELL",
        conf_before=0.82,
        conf_after=0.83,
        data={"agent_score": 0.88, "final_confidence": 0.83},
    )

    payload = signal.journal.to_dict(signal)

    assert payload["final_policy_decision"] == "PASS"
    assert payload["real_sources_valid"] == 4
    assert payload["opportunity_score"] == 0.8123
    assert payload["opportunity_rank"] == 1
    assert payload["setup_fingerprint"]["structure_bias"] == "sell"
    assert payload["setup_fingerprint"]["setup_style"] == "pullback"
    assert payload["factor_attribution"]["market_structure"] > 0
    assert payload["factor_attribution"]["ml"] > 0
    assert payload["factor_attribution"]["order_flow"] > 0
    assert payload["factor_attribution"]["broker_quality"] > 0
    assert payload["factor_attribution"]["microstructure"] > 0
    assert payload["factor_attribution"]["cross_asset"] > 0
    assert payload["broker_primary_provider"] == "IG"
    assert payload["depth_mode"] == "true_depth"
    assert payload["top_positive_factor"] != ""

def test_signal_journal_summary_supports_current_policy_and_governance_entry_names() -> None:
    signal = Signal(
        asset="EUR/USD",
        canonical_asset="EUR/USD",
        category="forex",
        direction="BUY",
        confidence=0.71,
    )
    signal.journal.record(
        layer=4,
        name="governance",
        decision="PASS",
        reason="governance passed",
        conf_before=0.68,
        conf_after=0.68,
        data={"valid_sources": 3, "min_required": 2, "score": 84, "grade": "B"},
    )
    signal.journal.record(
        layer=5,
        name="policy",
        decision="PASS",
        reason="policy accepted BUY",
        conf_before=0.68,
        conf_after=0.71,
        data={"agent_score": 0.74, "final_confidence": 0.71},
    )

    summary = signal.journal.summary(signal)

    assert summary["final_policy_decision"] == "PASS"
    assert summary["final_policy_reason"] == "policy accepted BUY"
    assert summary["final_policy_score"] == 0.74
    assert summary["final_confidence"] == 0.71
    assert summary["real_sources_valid"] == 3
    assert summary["real_sources_required"] == 2
    assert summary["governance_grade"] == "B"

def test_signal_journal_telegram_plain_is_human_readable_for_live_signal() -> None:
    signal = Signal(
        asset="BTC-USD",
        canonical_asset="BTC-USD",
        category="crypto",
        direction="SELL",
        confidence=0.626,
        entry_price=66345.677,
        stop_loss=66611.05971,
        take_profit=65947.60294,
        position_size=0.175,
        risk_reward=1.5,
    )
    signal.take_profit_levels = [66146.63997, 65947.60294, 65748.56591]
    signal.metadata.update({
        "regime": "trending_down",
        "sentiment_score": -0.137,
        "whale_dominant": "SELL",
        "memory_score": 63.1,
        "memory_edge": 0.311,
        "memory_sample_count": 37,
        "memory_win_rate": 0.707,
        "broker_quality": {
            "score": 0.84,
            "primary_provider": "Deriv",
            "comparison_provider": "Binance",
            "quote_agreement_state": "aligned",
            "quote_quality_state": "fresh",
            "spread_regime": "tight",
        },
        "broker_quality_score": 0.84,
        "market_microstructure": {
            "score": -0.39,
            "pressure_direction": "SELL",
            "stop_hunt_risk": 0.18,
            "exhaustion_risk": 0.09,
            "depth_available": True,
            "synthetic_depth_available": False,
            "microstructure_source": "order_flow_true_depth",
        },
        "microstructure_score": -0.39,
        "microstructure_alignment": 0.39,
        "depth_available": True,
        "synthetic_depth_available": False,
        "microstructure_source": "order_flow_true_depth",
        "cross_asset_score": -0.27,
        "cross_asset_alignment": 0.27,
        "cross_asset_confidence": 0.71,
        "cross_asset_state": "sell_support",
        "cross_asset_primary_peer": "ETH-USD",
        "cross_asset_primary_relation": "crypto_breadth",
        "governance_validation": {
            "model_key": "crypto_policy",
            "model_research": {
                "research_grade": "institutional",
                "research_approved": True,
                "walk_forward_accuracy": 0.659,
                "holdout_accuracy": 0.725,
            },
            "live_validation": {
                "accuracy_pct": 66.7,
            },
        },
    })
    signal.journal.record(
        layer=1,
        name="market",
        decision="PASS",
        reason="ml=0.539 rr=1.50 regime=trending_down session=europe news=clear",
        conf_before=0.61,
        conf_after=0.61,
        data={
            "ml_direction": "SELL",
            "rr": 1.5,
            "regime": "trending_down",
            "session": "europe",
            "news_state": "clear",
        },
    )
    signal.journal.record(
        layer=2,
        name="intelligence",
        decision="PASS",
        reason="sentiment=-0.137 whale=SELL sources=3",
        conf_before=0.61,
        conf_after=0.61,
        data={
            "sentiment_score": -0.137,
            "sentiment_sources": ["comprehensive_sentiment", "reddit", "narrative_ai"],
            "narrative": "AI_TOKENS",
            "whale_dominant": "SELL",
            "whale_ratio": 1.0,
            "cross_asset_alignment": 0.27,
            "cross_asset_primary_peer": "ETH-USD",
            "cross_asset_primary_relation": "crypto_breadth",
        },
    )
    signal.journal.record(
        layer=0,
        name="memory",
        decision="INFO",
        reason="memory score=63.1 edge=+0.311 samples=37",
        conf_before=0.61,
        conf_after=0.61,
        data={
            "memory_score": 63.1,
            "memory_edge": 0.311,
            "memory_win_rate": 0.707,
            "memory_sample_count": 37,
        },
    )
    signal.journal.record(
        layer=3,
        name="meta_ai",
        decision="PASS",
        reason="ensemble neutral - no adjustment",
        conf_before=0.61,
        conf_after=0.61,
        data={
            "regime": "trending_bear",
            "ensemble": 0.598,
        },
    )
    signal.journal.record(
        layer=4,
        name="policy",
        decision="PASS",
        reason="policy accepted SELL (score=0.205)",
        conf_before=0.61,
        conf_after=0.61,
        data={
            "agent_score": 0.205,
            "agent_confidence": 0.267,
            "agent_directional_edge": 0.795,
            "agent_policy_status": "ok",
            "final_confidence": 0.626,
        },
    )
    signal.journal.record(
        layer=5,
        name="governance",
        decision="PASS",
        reason="grade=A score=100",
        conf_before=0.61,
        conf_after=0.626,
        data={
            "valid_sources": 6,
            "min_required": 2,
            "score": 100,
            "grade": "A",
        },
    )
    signal.journal.record(
        layer=6,
        name="execution",
        decision="PASS",
        reason="final_score=0.626 size=0.1750 tp_levels=3",
        conf_before=0.626,
        conf_after=0.626,
        data={
            "position_size": 0.175,
            "notes": ["compressed_volatility", "balance_drawdown"],
        },
    )
    signal.journal.record(
        layer=0,
        name="research_validation",
        decision="INFO",
        reason="wf=0.659 holdout=0.725 live=66.7%",
        conf_before=0.626,
        conf_after=0.626,
        data={
            "model_key": "crypto_policy",
            "research_grade": "institutional",
            "research_approved": True,
            "walk_forward_accuracy": 0.659,
            "holdout_accuracy": 0.725,
            "live_validation_accuracy_pct": 66.7,
        },
    )

    message = signal.journal.to_telegram_plain(signal)

    assert "BTC-USD SELL setup" in message
    assert "The bot is preparing a sell trade on BTC-USD near 66,345.677" in message
    assert "What the bot is seeing right now:" in message
    assert "- Market view: Trend is trending down" in message
    assert "reward to risk is 1.50:1" in message
    assert "Deriv and Binance are aligned" in message
    assert "spread regime is tight" in message
    assert "Why the bot trusts this setup:" in message
    assert "- Flow and sentiment: Sentiment is slightly bearish" in message
    assert "whale flow leans sell" in message
    assert "ETH-USD is confirming the trade through crypto breadth" in message
    assert "the main narrative is AI-related crypto narrative" in message
    assert "Similar setups won 70.7% of the time across 37 examples" in message
    assert "The policy model approved the sell setup" in message
    assert "How the trade will be managed:" in message
    assert "- Execution posture: The setup stayed above the live execution floor" in message
    assert "true order-book depth is available" in message
    assert "microstructure pressure still leans sell" in message
    assert "Review time:" in message
    assert "sentiment_score=" not in message
    assert "adaptive_policy={" not in message
    assert "scorecard_preview={" not in message

def test_alert_formatter_humanizes_internal_narrative_labels() -> None:
    formatter_mod = importlib.import_module("services.intelligence_alerts.alert_formatter")
    formatter = formatter_mod.AlertFormatter()

    message = formatter.format(
        "NARRATIVE_TREND_DETECTED",
        {
            "narrative": "AI_TOKENS",
            "velocity": 2.4,
            "strength": "STRONG",
            "count": 7,
            "keywords_matched": ["ai", "gpt", "agent"],
        },
        "MEDIUM",
    )

    assert message is not None
    assert "AI-related crypto narrative" in message
    assert "AI_TOKENS" not in message

def test_adaptive_policy_service_adjusts_thresholds_by_setup_quality() -> None:
    adaptive_mod = importlib.import_module("services.adaptive_policy_service")
    service = adaptive_mod.get_service()

    strong_signal = Signal(
        asset="BTC-USD",
        canonical_asset="BTC-USD",
        category="crypto",
        direction="BUY",
        confidence=0.76,
    )
    strong_signal.metadata.update({
        "market_structure": {
            "structure_bias": "buy",
            "alignment_score": 0.82,
            "setup_quality": 0.79,
            "pullback_score": 0.44,
            "breakout_score": 0.56,
            "regime": "trending_up",
            "volatility_state": "expansion",
        },
        "structure_bias": "buy",
        "alignment_score": 0.82,
        "setup_quality": 0.79,
        "pullback_score": 0.44,
        "breakout_score": 0.56,
        "opportunity_score": 0.88,
        "sentiment_score": 0.31,
        "orderflow_imbalance": 0.35,
    })

    weak_signal = Signal(
        asset="BTC-USD",
        canonical_asset="BTC-USD",
        category="crypto",
        direction="BUY",
        confidence=0.76,
    )
    weak_signal.metadata.update({
        "market_structure": {
            "structure_bias": "sell",
            "alignment_score": 0.58,
            "setup_quality": 0.29,
            "pullback_score": -0.51,
            "breakout_score": -0.42,
            "regime": "volatile",
            "volatility_state": "extreme",
        },
        "structure_bias": "sell",
        "alignment_score": 0.58,
        "setup_quality": 0.29,
        "pullback_score": -0.51,
        "breakout_score": -0.42,
        "opportunity_score": 0.49,
        "sentiment_score": -0.26,
        "orderflow_imbalance": -0.33,
    })

    strong = service.get_thresholds("BTC-USD", "crypto", {"market_structure": strong_signal.metadata["market_structure"]}, strong_signal)
    weak = service.get_thresholds("BTC-USD", "crypto", {"market_structure": weak_signal.metadata["market_structure"]}, weak_signal)

    assert strong["min_final_confidence"] < weak["min_final_confidence"]
    assert strong["max_spread"] > weak["max_spread"]
    assert strong["risk_multiplier"] > weak["risk_multiplier"]
    assert strong["cooldown_minutes"] < weak["cooldown_minutes"]

def test_adaptive_policy_service_lowers_floor_for_cleared_bootstrap_signal() -> None:
    adaptive_mod = importlib.import_module("services.adaptive_policy_service")
    service = adaptive_mod.get_service()

    base_signal = Signal(
        asset="EUR/USD",
        canonical_asset="EUR/USD",
        category="forex",
        direction="BUY",
        confidence=0.70,
    )
    base_signal.metadata.update({
        "market_structure": {
            "structure_bias": "buy",
            "alignment_score": 0.72,
            "setup_quality": 0.68,
            "pullback_score": 0.35,
            "breakout_score": 0.42,
            "regime": "trending_up",
            "volatility_state": "expansion",
        },
        "structure_bias": "buy",
        "alignment_score": 0.72,
        "setup_quality": 0.68,
        "pullback_score": 0.35,
        "breakout_score": 0.42,
        "opportunity_score": 0.74,
    })

    boosted_signal = Signal(
        asset="EUR/USD",
        canonical_asset="EUR/USD",
        category="forex",
        direction="BUY",
        confidence=0.70,
    )
    boosted_signal.metadata.update({
        **base_signal.metadata,
        "agent_score": 0.82,
        "agent_policy_status": "ok",
        "governance_validation": {
            "approved": True,
            "violations": [],
            "live_validation": {"scope": "portfolio", "total": 139, "accuracy_pct": 43.2},
        },
    })

    base = service.get_thresholds("EUR/USD", "forex", {"market_structure": base_signal.metadata["market_structure"]}, base_signal)
    boosted = service.get_thresholds("EUR/USD", "forex", {"market_structure": boosted_signal.metadata["market_structure"]}, boosted_signal)

    assert boosted["min_final_confidence"] < base["min_final_confidence"]
    assert boosted["risk_multiplier"] >= base["risk_multiplier"]
    assert boosted["min_rr"] < 1.0
    assert "policy_aligned" in boosted["notes"]
    assert "governance_cleared" in boosted["notes"]

def test_adaptive_policy_service_uses_category_base_rr_in_paper() -> None:
    adaptive_mod = importlib.import_module("services.adaptive_policy_service")
    service = adaptive_mod.get_service()

    forex_signal = Signal(
        asset="EUR/USD",
        canonical_asset="EUR/USD",
        category="forex",
        direction="BUY",
        confidence=0.70,
    )
    forex_signal.metadata.update({
        "market_structure": {
            "structure_bias": "buy",
            "alignment_score": 0.72,
            "setup_quality": 0.68,
            "pullback_score": 0.35,
            "breakout_score": 0.42,
            "regime": "trending_up",
            "volatility_state": "expansion",
        },
        "structure_bias": "buy",
        "alignment_score": 0.72,
        "setup_quality": 0.68,
        "pullback_score": 0.35,
        "breakout_score": 0.42,
        "opportunity_score": 0.74,
    })

    crypto_signal = Signal(
        asset="ETH-USD",
        canonical_asset="ETH-USD",
        category="crypto",
        direction="BUY",
        confidence=0.74,
    )
    crypto_signal.metadata.update({
        "market_structure": {
            "structure_bias": "buy",
            "alignment_score": 0.76,
            "setup_quality": 0.73,
            "pullback_score": 0.43,
            "breakout_score": 0.52,
            "regime": "trending_up",
            "volatility_state": "expansion",
        },
        "structure_bias": "buy",
        "alignment_score": 0.76,
        "setup_quality": 0.73,
        "pullback_score": 0.43,
        "breakout_score": 0.52,
        "opportunity_score": 0.84,
    })

    forex_policy = service.get_thresholds("EUR/USD", "forex", {"market_structure": forex_signal.metadata["market_structure"]}, forex_signal)
    crypto_policy = service.get_thresholds("ETH-USD", "crypto", {"market_structure": crypto_signal.metadata["market_structure"]}, crypto_signal)

    assert forex_policy["min_rr"] < 1.0
    assert crypto_policy["min_rr"] >= 1.0

def test_recent_pattern_learning_service_blocks_repeated_late_entry_failures() -> None:
    learning_mod = importlib.import_module("services.recent_pattern_learning_service")
    memory_mod = importlib.import_module("services.setup_memory_service")

    service = learning_mod.RecentPatternLearningService()
    signal = Signal(
        asset="BTC-USD",
        canonical_asset="BTC-USD",
        category="crypto",
        direction="SELL",
        confidence=0.71,
        risk_reward=1.6,
    )
    signal.metadata.update(
        {
            "regime": "trending_down",
            "session": "europe",
            "structure_bias": "sell",
            "alignment_score": 0.73,
            "setup_quality": 0.69,
            "pullback_score": -0.44,
            "breakout_score": -0.51,
            "volatility_state": "expansion",
            "sentiment_score": -0.24,
            "whale_dominant": "SELL",
            "whale_bear_weight": 0.71,
            "orderflow_imbalance": -0.36,
            "opportunity_score": 0.79,
        }
    )

    fp = memory_mod.get_service().build_fingerprint(signal, {"timeframe": "15m"})
    now = datetime.now(timezone.utc).isoformat()
    rows = []
    for _ in range(6):
        rows.append(
            {
                "asset": "BTC-USD",
                "canonical_asset": "BTC-USD",
                "category": "crypto",
                "direction": "SELL",
                "entry_time": now,
                "exit_time": now,
                "metadata": {
                    "setup_memory_fingerprint": fp,
                    "execution_feedback": {
                        "exit_family": "stop_loss",
                        "late_entry": True,
                        "premature_stop": False,
                        "target_miss": False,
                        "stop_too_tight": False,
                        "stop_too_wide": False,
                        "rr_realized": -1.0,
                        "quality_score": 30.0,
                    },
                },
            }
        )

    service._fetch_rows = lambda asset, category, days_back, limit: rows
    profile = service.get_profile("BTC-USD", "crypto", signal, {"timeframe": "15m"})

    assert profile["sample_count"] == 6
    assert profile["late_entry_rate"] >= 0.9
    assert profile["block_new_entries"] is True
    assert "late entries" in profile["block_reason"]

def test_recent_pattern_learning_service_boosts_clean_winner_clusters() -> None:
    learning_mod = importlib.import_module("services.recent_pattern_learning_service")
    memory_mod = importlib.import_module("services.setup_memory_service")

    service = learning_mod.RecentPatternLearningService()
    signal = Signal(
        asset="BTC-USD",
        canonical_asset="BTC-USD",
        category="crypto",
        direction="BUY",
        confidence=0.74,
        risk_reward=1.7,
    )
    signal.metadata.update(
        {
            "regime": "trending_up",
            "session": "us",
            "structure_bias": "buy",
            "alignment_score": 0.81,
            "setup_quality": 0.78,
            "pullback_score": 0.42,
            "breakout_score": 0.55,
            "volatility_state": "expansion",
            "sentiment_score": 0.26,
            "whale_dominant": "BUY",
            "whale_bull_weight": 0.74,
            "orderflow_imbalance": 0.39,
            "opportunity_score": 0.83,
        }
    )

    fp = memory_mod.get_service().build_fingerprint(signal, {"timeframe": "15m"})
    now = datetime.now(timezone.utc).isoformat()
    rows = []
    for _ in range(6):
        rows.append(
            {
                "asset": "BTC-USD",
                "canonical_asset": "BTC-USD",
                "category": "crypto",
                "direction": "BUY",
                "entry_time": now,
                "exit_time": now,
                "metadata": {
                    "setup_memory_fingerprint": fp,
                    "execution_feedback": {
                        "exit_family": "take_profit",
                        "late_entry": False,
                        "premature_stop": False,
                        "target_miss": False,
                        "stop_too_tight": False,
                        "stop_too_wide": False,
                        "full_target": True,
                        "target_capture": 1.0,
                        "giveback_ratio": 0.14,
                        "rr_realized": 1.42,
                        "quality_score": 74.0,
                    },
                },
            }
        )

    service._fetch_rows = lambda asset, category, days_back, limit: rows
    profile = service.get_profile("BTC-USD", "crypto", signal, {"timeframe": "15m"})

    assert profile["sample_count"] == 6
    assert profile["win_rate"] >= 0.9
    assert profile["bonus_confidence"] > 0
    assert profile["bonus_risk"] > 0
    assert profile["target_rr_multiplier"] > 1.0
    assert "recent_pattern_targets_extend" in profile["notes"]

def test_recent_pattern_learning_service_penalizes_broker_and_microstructure_failures() -> None:
    learning_mod = importlib.import_module("services.recent_pattern_learning_service")
    memory_mod = importlib.import_module("services.setup_memory_service")

    service = learning_mod.RecentPatternLearningService()
    signal = Signal(
        asset="XAU/USD",
        canonical_asset="XAU/USD",
        category="commodities",
        direction="BUY",
        confidence=0.69,
        risk_reward=1.6,
    )
    signal.metadata.update(
        {
            "regime": "ranging",
            "session": "us",
            "structure_bias": "buy",
            "alignment_score": 0.62,
            "setup_quality": 0.58,
            "pullback_score": 0.21,
            "breakout_score": 0.33,
            "volatility_state": "expansion",
            "opportunity_score": 0.67,
            "broker_quality": {
                "score": 0.22,
                "primary_provider": "IG",
                "comparison_provider": "Deriv",
                "quote_agreement_state": "severe_divergence",
                "spread_regime": "stressed",
                "quote_quality_state": "stale",
            },
            "market_microstructure": {
                "score": -0.28,
                "microstructure_alignment": -0.28,
                "stop_hunt_risk": 0.58,
                "exhaustion_risk": 0.47,
                "depth_available": False,
                "synthetic_depth_available": True,
                "microstructure_source": "live_store_synthetic_depth",
            },
        }
    )

    fp = memory_mod.get_service().build_fingerprint(signal, {"timeframe": "15m"})
    now = datetime.now(timezone.utc).isoformat()
    rows = []
    for _ in range(6):
        rows.append(
            {
                "asset": "XAU/USD",
                "canonical_asset": "XAU/USD",
                "category": "commodities",
                "direction": "BUY",
                "entry_time": now,
                "exit_time": now,
                "metadata": {
                    "setup_memory_fingerprint": fp,
                    "broker_quality": {
                        "score": 0.18,
                        "primary_provider": "IG",
                        "comparison_provider": "Deriv",
                        "quote_agreement_state": "severe_divergence",
                        "spread_regime": "stressed",
                        "quote_quality_state": "stale",
                        "market_transition_risk": 0.79,
                        "fallback_active": True,
                    },
                    "market_microstructure": {
                        "score": -0.24,
                        "microstructure_alignment": -0.24,
                        "stop_hunt_risk": 0.61,
                        "exhaustion_risk": 0.48,
                        "depth_available": False,
                        "synthetic_depth_available": True,
                        "microstructure_source": "live_store_synthetic_depth",
                    },
                    "execution_feedback": {
                        "exit_family": "stop_loss",
                        "late_entry": False,
                        "premature_stop": False,
                        "target_miss": False,
                        "stop_too_tight": False,
                        "stop_too_wide": False,
                        "rr_realized": -1.02,
                        "quality_score": 29.0,
                    },
                },
            }
        )

    service._fetch_rows = lambda asset, category, days_back, limit: rows
    profile = service.get_profile("XAU/USD", "commodities", signal, {"timeframe": "15m"})

    assert profile["sample_count"] == 6
    assert profile["broker_divergence_rate"] >= 0.9
    assert profile["stop_hunt_rate"] >= 0.9
    assert profile["synthetic_depth_loss_rate"] >= 0.9
    assert profile["penalty_confidence"] > 0
    assert "recent_pattern_broker_divergence" in profile["notes"]
    assert "recent_pattern_stop_hunt" in profile["notes"]
    assert profile["block_new_entries"] is True
    assert "brokers disagree" in profile["block_reason"]

def test_recent_pattern_learning_service_rewards_true_depth_and_broker_confirmed_winners() -> None:
    learning_mod = importlib.import_module("services.recent_pattern_learning_service")
    memory_mod = importlib.import_module("services.setup_memory_service")

    service = learning_mod.RecentPatternLearningService()
    signal = Signal(
        asset="BTC-USD",
        canonical_asset="BTC-USD",
        category="crypto",
        direction="BUY",
        confidence=0.76,
        risk_reward=1.8,
    )
    signal.metadata.update(
        {
            "regime": "trending_up",
            "session": "us",
            "structure_bias": "buy",
            "alignment_score": 0.79,
            "setup_quality": 0.75,
            "pullback_score": 0.33,
            "breakout_score": 0.57,
            "volatility_state": "expansion",
            "opportunity_score": 0.85,
            "broker_quality": {
                "score": 0.84,
                "primary_provider": "Binance",
                "comparison_provider": "Deriv",
                "quote_agreement_state": "strong",
                "spread_regime": "tight",
                "quote_quality_state": "fresh",
            },
            "market_microstructure": {
                "score": 0.44,
                "microstructure_alignment": 0.44,
                "stop_hunt_risk": 0.08,
                "exhaustion_risk": 0.07,
                "depth_available": True,
                "synthetic_depth_available": False,
                "microstructure_source": "order_flow_true_depth",
            },
        }
    )

    fp = memory_mod.get_service().build_fingerprint(signal, {"timeframe": "15m"})
    now = datetime.now(timezone.utc).isoformat()
    rows = []
    for _ in range(6):
        rows.append(
            {
                "asset": "BTC-USD",
                "canonical_asset": "BTC-USD",
                "category": "crypto",
                "direction": "BUY",
                "entry_time": now,
                "exit_time": now,
                "metadata": {
                    "setup_memory_fingerprint": fp,
                    "broker_quality": {
                        "score": 0.82,
                        "primary_provider": "Binance",
                        "comparison_provider": "Deriv",
                        "quote_agreement_state": "strong",
                        "spread_regime": "tight",
                        "quote_quality_state": "fresh",
                    },
                    "market_microstructure": {
                        "score": 0.41,
                        "microstructure_alignment": 0.41,
                        "stop_hunt_risk": 0.09,
                        "exhaustion_risk": 0.06,
                        "depth_available": True,
                        "synthetic_depth_available": False,
                        "microstructure_source": "order_flow_true_depth",
                    },
                    "execution_feedback": {
                        "exit_family": "take_profit",
                        "late_entry": False,
                        "premature_stop": False,
                        "target_miss": False,
                        "stop_too_tight": False,
                        "stop_too_wide": False,
                        "full_target": True,
                        "target_capture": 1.0,
                        "giveback_ratio": 0.12,
                        "rr_realized": 1.38,
                        "quality_score": 76.0,
                    },
                },
            }
        )

    service._fetch_rows = lambda asset, category, days_back, limit: rows
    profile = service.get_profile("BTC-USD", "crypto", signal, {"timeframe": "15m"})

    assert profile["sample_count"] == 6
    assert profile["true_depth_win_rate"] >= 0.9
    assert profile["broker_confirmed_win_rate"] >= 0.9
    assert profile["bonus_confidence"] > 0
    assert profile["bonus_risk"] > 0
    assert profile["target_rr_multiplier"] > 1.0
    assert "recent_pattern_true_depth_winners" in profile["notes"]
    assert "recent_pattern_broker_confirmed_winners" in profile["notes"]

def test_recent_pattern_learning_service_penalizes_cross_asset_conflict_patterns() -> None:
    learning_mod = importlib.import_module("services.recent_pattern_learning_service")
    memory_mod = importlib.import_module("services.setup_memory_service")

    service = learning_mod.RecentPatternLearningService()
    signal = Signal(
        asset="USD/CAD",
        canonical_asset="USD/CAD",
        category="forex",
        direction="BUY",
        confidence=0.71,
        risk_reward=1.6,
    )
    signal.metadata.update(
        {
            "regime": "trending_up",
            "session": "us",
            "structure_bias": "buy",
            "alignment_score": 0.68,
            "setup_quality": 0.61,
            "pullback_score": 0.24,
            "breakout_score": 0.41,
            "volatility_state": "expansion",
            "opportunity_score": 0.73,
            "cross_asset_primary_peer": "WTI",
            "cross_asset_primary_relation": "oil_cad_link",
            "cross_asset_alignment": -0.46,
            "cross_asset_confidence": 0.82,
        }
    )

    fp = memory_mod.get_service().build_fingerprint(signal, {"timeframe": "15m"})
    now = datetime.now(timezone.utc).isoformat()
    rows = []
    for _ in range(6):
        rows.append(
            {
                "asset": "USD/CAD",
                "canonical_asset": "USD/CAD",
                "category": "forex",
                "direction": "BUY",
                "entry_time": now,
                "exit_time": now,
                "metadata": {
                    "setup_memory_fingerprint": fp,
                    "cross_asset_context": {
                        "score": -0.44,
                        "alignment": -0.44,
                        "confidence": 0.84,
                        "state": "sell_support",
                        "supportive_direction": "SELL",
                        "dominant_peer": "WTI",
                        "dominant_relation": "oil_cad_link",
                    },
                    "execution_feedback": {
                        "exit_family": "stop_loss",
                        "late_entry": False,
                        "premature_stop": False,
                        "target_miss": False,
                        "stop_too_tight": False,
                        "stop_too_wide": False,
                        "rr_realized": -0.92,
                        "quality_score": 35.0,
                    },
                },
            }
        )

    service._fetch_rows = lambda asset, category, days_back, limit: rows
    profile = service.get_profile("USD/CAD", "forex", signal, {"timeframe": "15m"})

    assert profile["sample_count"] == 6
    assert profile["cross_asset_conflicted_loss_rate"] >= 0.9
    assert profile["cross_asset_relation_match_count"] == 6
    assert profile["penalty_confidence"] > 0
    assert "recent_pattern_cross_asset_conflict" in profile["notes"]
    assert "recent_pattern_cross_asset_relation_failures" in profile["notes"]
    assert profile["block_new_entries"] is True
    assert "spillover conflicts" in profile["block_reason"]

def test_recent_pattern_learning_service_rewards_cross_asset_confirmed_winners() -> None:
    learning_mod = importlib.import_module("services.recent_pattern_learning_service")
    memory_mod = importlib.import_module("services.setup_memory_service")

    service = learning_mod.RecentPatternLearningService()
    signal = Signal(
        asset="WTI",
        canonical_asset="WTI",
        category="commodities",
        direction="BUY",
        confidence=0.74,
        risk_reward=1.8,
    )
    signal.metadata.update(
        {
            "regime": "trending_up",
            "session": "us",
            "structure_bias": "buy",
            "alignment_score": 0.77,
            "setup_quality": 0.73,
            "pullback_score": 0.26,
            "breakout_score": 0.49,
            "volatility_state": "expansion",
            "opportunity_score": 0.84,
            "cross_asset_primary_peer": "US500",
            "cross_asset_primary_relation": "growth_cycle_confirmation",
            "cross_asset_alignment": 0.38,
            "cross_asset_confidence": 0.76,
        }
    )

    fp = memory_mod.get_service().build_fingerprint(signal, {"timeframe": "15m"})
    now = datetime.now(timezone.utc).isoformat()
    rows = []
    for _ in range(6):
        rows.append(
            {
                "asset": "WTI",
                "canonical_asset": "WTI",
                "category": "commodities",
                "direction": "BUY",
                "entry_time": now,
                "exit_time": now,
                "metadata": {
                    "setup_memory_fingerprint": fp,
                    "cross_asset_context": {
                        "score": 0.36,
                        "alignment": 0.36,
                        "confidence": 0.79,
                        "state": "buy_support",
                        "supportive_direction": "BUY",
                        "dominant_peer": "US500",
                        "dominant_relation": "growth_cycle_confirmation",
                    },
                    "execution_feedback": {
                        "exit_family": "take_profit",
                        "late_entry": False,
                        "premature_stop": False,
                        "target_miss": False,
                        "stop_too_tight": False,
                        "stop_too_wide": False,
                        "full_target": True,
                        "target_capture": 1.0,
                        "giveback_ratio": 0.11,
                        "rr_realized": 1.21,
                        "quality_score": 74.0,
                    },
                },
            }
        )

    service._fetch_rows = lambda asset, category, days_back, limit: rows
    profile = service.get_profile("WTI", "commodities", signal, {"timeframe": "15m"})

    assert profile["sample_count"] == 6
    assert profile["cross_asset_confirmed_win_rate"] >= 0.9
    assert profile["cross_asset_relation_support_win_rate"] >= 0.9
    assert profile["bonus_confidence"] > 0
    assert profile["bonus_risk"] > 0
    assert profile["target_rr_multiplier"] > 1.0
    assert "recent_pattern_cross_asset_confirmed_winners" in profile["notes"]
    assert "recent_pattern_cross_asset_relation_edge" in profile["notes"]

def test_adaptive_policy_service_applies_recent_pattern_penalties(monkeypatch) -> None:
    adaptive_mod = importlib.import_module("services.adaptive_policy_service")
    learning_mod = importlib.import_module("services.recent_pattern_learning_service")
    service = adaptive_mod.get_service()

    signal = Signal(
        asset="BTC-USD",
        canonical_asset="BTC-USD",
        category="crypto",
        direction="BUY",
        confidence=0.74,
    )
    signal.metadata.update(
        {
            "market_structure": {
                "structure_bias": "buy",
                "alignment_score": 0.76,
                "setup_quality": 0.71,
                "pullback_score": 0.43,
                "breakout_score": 0.52,
                "regime": "trending_up",
                "volatility_state": "expansion",
            },
            "structure_bias": "buy",
            "alignment_score": 0.76,
            "setup_quality": 0.71,
            "pullback_score": 0.43,
            "breakout_score": 0.52,
            "opportunity_score": 0.81,
        }
    )

    base = service.get_thresholds("BTC-USD", "crypto", {"market_structure": signal.metadata["market_structure"]}, signal)

    monkeypatch.setattr(
        learning_mod.get_service(),
        "get_profile",
        lambda asset, category, signal, context=None, days_back=45, limit=240: {
            "sample_count": 7,
            "penalty_confidence": 0.03,
            "penalty_risk": 0.10,
            "penalty_rr": 0.12,
            "cooldown_delta": 6,
            "block_new_entries": False,
            "block_reason": "",
            "notes": ["recent_pattern_late_entry", "recent_pattern_hard_losses"],
            "late_entry_rate": 0.67,
            "hard_loss_rate": 0.51,
            "avg_quality_score": 33.0,
            "avg_rr_realized": -0.72,
        },
        raising=False,
    )

    policy = service.get_thresholds("BTC-USD", "crypto", {"market_structure": signal.metadata["market_structure"]}, signal)

    assert policy["recent_review_profile"]["sample_count"] == 7
    assert "recent_pattern_late_entry" in policy["notes"]
    assert policy["risk_multiplier"] < base["risk_multiplier"]
    assert policy["min_rr"] > base["min_rr"]
    assert policy["cooldown_minutes"] > base["cooldown_minutes"]
    assert policy["min_final_confidence"] > base["min_final_confidence"]

def test_adaptive_policy_service_applies_recent_winner_boosts(monkeypatch) -> None:
    adaptive_mod = importlib.import_module("services.adaptive_policy_service")
    learning_mod = importlib.import_module("services.recent_pattern_learning_service")
    service = adaptive_mod.get_service()

    signal = Signal(
        asset="BTC-USD",
        canonical_asset="BTC-USD",
        category="crypto",
        direction="BUY",
        confidence=0.74,
    )
    signal.metadata.update(
        {
            "market_structure": {
                "structure_bias": "buy",
                "alignment_score": 0.76,
                "setup_quality": 0.73,
                "pullback_score": 0.43,
                "breakout_score": 0.52,
                "regime": "trending_up",
                "volatility_state": "expansion",
            },
            "structure_bias": "buy",
            "alignment_score": 0.76,
            "setup_quality": 0.73,
            "pullback_score": 0.43,
            "breakout_score": 0.52,
            "opportunity_score": 0.84,
        }
    )

    base = service.get_thresholds("BTC-USD", "crypto", {"market_structure": signal.metadata["market_structure"]}, signal)

    monkeypatch.setattr(
        learning_mod.get_service(),
        "get_profile",
        lambda asset, category, signal, context=None, days_back=45, limit=240: {
            "sample_count": 8,
            "penalty_confidence": 0.0,
            "penalty_risk": 0.0,
            "penalty_rr": 0.0,
            "bonus_confidence": 0.02,
            "bonus_risk": 0.08,
            "bonus_rr_relief": 0.06,
            "cooldown_delta": -2,
            "target_rr_multiplier": 1.12,
            "block_new_entries": False,
            "block_reason": "",
            "notes": ["recent_pattern_winners", "recent_pattern_targets_extend"],
            "win_rate": 0.72,
            "full_target_rate": 0.56,
            "avg_quality_score": 69.0,
            "avg_rr_realized": 1.18,
        },
        raising=False,
    )

    boosted = service.get_thresholds("BTC-USD", "crypto", {"market_structure": signal.metadata["market_structure"]}, signal)

    assert boosted["recent_review_profile"]["sample_count"] == 8
    assert boosted["min_final_confidence"] < base["min_final_confidence"]
    assert boosted["risk_multiplier"] > base["risk_multiplier"]
    assert boosted["min_rr"] < base["min_rr"]
    assert boosted["target_rr_multiplier"] > 1.0
    assert "recent_pattern_winners" in boosted["notes"]

def test_execution_review_uses_adaptive_policy_thresholds() -> None:
    decision_mod = importlib.import_module("core.decision_engine")
    engine = decision_mod.SignalDecisionEngine()

    price_data = _build_trend_frame(100.0, 0.35)
    structure = {
        "structure_bias": "buy",
        "alignment_score": 0.84,
        "setup_quality": 0.78,
        "pullback_score": 0.41,
        "breakout_score": 0.57,
        "regime": "trending_up",
        "volatility_state": "expansion",
    }
    signal = Signal(
        asset="EUR/USD",
        canonical_asset="EUR/USD",
        category="forex",
        direction="BUY",
        confidence=0.66,
        entry_price=100.0,
        stop_loss=99.2,
        take_profit=101.2,
        risk_reward=1.5,
    )
    signal.metadata.update({
        "market_structure": structure,
        "structure_bias": "buy",
        "alignment_score": 0.84,
        "setup_quality": 0.78,
        "pullback_score": 0.41,
        "breakout_score": 0.57,
        "ml_confidence": 0.91,
        "seed_candidate_score": 0.82,
        "agent_directional_edge": 0.84,
        "meta_ai_ensemble": 0.79,
        "memory_edge": 0.22,
        "memory_sample_count": 28,
        "opportunity_score": 0.87,
        "sentiment_score": 0.28,
        "orderflow_applicable": True,
        "orderflow_imbalance": 0.34,
    })

    approved = engine._apply_execution_review(
        signal,
        {
            "category": "forex",
            "spread": 0.23,  # 0.23 / 100 = 0.0023, above base forex threshold but within adaptive allowance
            "price_data": price_data,
            "market_structure": structure,
        },
    )

    assert approved is True
    assert signal.alive is True
    assert signal.metadata["adaptive_policy"]["max_spread"] > 0.002
    assert signal.journal.entries[-1].data["adaptive_policy"]["max_spread"] > 0.002

def test_execution_review_extends_take_profit_for_recent_winner_pattern(monkeypatch) -> None:
    decision_mod = importlib.import_module("core.decision_engine")
    adaptive_mod = importlib.import_module("services.adaptive_policy_service")
    scorecard_mod = importlib.import_module("services.signal_scorecard")
    engine = decision_mod.SignalDecisionEngine()

    monkeypatch.setattr(
        adaptive_mod.get_service(),
        "get_thresholds",
        lambda asset, category, context=None, signal=None, state=None: {
            "min_final_confidence": 0.55,
            "max_spread": 0.003,
            "risk_multiplier": 1.08,
            "cooldown_minutes": 12,
            "min_rr": 1.45,
            "target_rr_multiplier": 1.12,
            "block_new_entries": False,
            "block_reason": "",
            "recent_review_profile": {
                "sample_count": 7,
                "win_rate": 0.71,
                "full_target_rate": 0.55,
            },
            "notes": ["recent_pattern_winners", "recent_pattern_targets_extend"],
        },
        raising=False,
    )
    monkeypatch.setattr(
        scorecard_mod.get_service(),
        "score",
        lambda signal, context=None: {
            "final_score": 0.71,
            "raw_score": 0.71,
            "reliability": 0.82,
            "breakdown": {},
            "notes": [],
            "live_validation": {},
        },
        raising=False,
    )

    signal = Signal(
        asset="BTC-USD",
        canonical_asset="BTC-USD",
        category="crypto",
        direction="BUY",
        confidence=0.69,
        entry_price=100.0,
        stop_loss=99.0,
        take_profit=101.5,
        risk_reward=1.5,
    )

    approved = engine._apply_execution_review(signal, {"category": "crypto", "spread": 0.1, "price_data": _build_trend_frame(100.0, 0.2)})

    assert approved is True
    assert signal.alive is True
    assert signal.take_profit > 101.5
    assert signal.risk_reward > 1.5
    assert signal.metadata["adaptive_target_rr_multiplier"] == 1.12
    assert signal.journal.entries[-1].data["adaptive_policy"]["target_rr_multiplier"] == 1.12

def test_execution_review_blocks_signal_when_recent_pattern_learning_flags_setup(monkeypatch) -> None:
    decision_mod = importlib.import_module("core.decision_engine")
    adaptive_mod = importlib.import_module("services.adaptive_policy_service")
    engine = decision_mod.SignalDecisionEngine()

    monkeypatch.setattr(
        adaptive_mod.get_service(),
        "get_thresholds",
        lambda asset, category, context=None, signal=None, state=None: {
            "min_final_confidence": 0.55,
            "max_spread": 0.003,
            "risk_multiplier": 1.0,
            "cooldown_minutes": 15,
            "min_rr": 1.5,
            "block_new_entries": True,
            "block_reason": "recent similar setups keep failing from late entries",
            "recent_review_profile": {
                "sample_count": 6,
                "late_entry_rate": 0.66,
                "hard_loss_rate": 0.51,
            },
            "notes": ["recent_pattern_late_entry", "recent_pattern_hard_losses"],
        },
        raising=False,
    )

    signal = Signal(
        asset="BTC-USD",
        canonical_asset="BTC-USD",
        category="crypto",
        direction="SELL",
        confidence=0.67,
        entry_price=100.0,
        stop_loss=101.0,
        take_profit=98.0,
        risk_reward=2.0,
    )

    approved = engine._apply_execution_review(signal, {"category": "crypto", "spread": 0.1, "price_data": _build_trend_frame(100.0, -0.2)})

    assert approved is False
    assert signal.alive is False
    assert "late entries" in signal.kill_reason.lower()
    assert signal.journal.entries[-1].decision == "KILLED"
    assert signal.journal.entries[-1].data["adaptive_policy"]["block_new_entries"] is True

def test_risk_manager_uses_asset_class_target_rr_overrides() -> None:
    risk_mod = importlib.import_module("risk.manager")
    manager = risk_mod.RiskManager(account_balance=10_000.0)

    assert manager.get_target_rr("crypto") == 1.7
    assert manager.get_target_rr("forex") == 1.5
    assert manager.get_target_rr("commodities") == 1.6
    assert manager.get_target_rr("indices") == 1.65

def test_setup_memory_service_scores_similar_historical_winners() -> None:
    memory_mod = importlib.import_module("services.setup_memory_service")
    service = memory_mod.SetupMemoryService()

    signal = Signal(
        asset="BTC-USD",
        canonical_asset="BTC-USD",
        category="crypto",
        direction="SELL",
        confidence=0.78,
        risk_reward=1.7,
    )
    signal.metadata.update({
        "regime": "trending_down",
        "session": "asia",
        "structure_bias": "sell",
        "alignment_score": 0.79,
        "setup_quality": 0.75,
        "pullback_score": -0.52,
        "breakout_score": -0.36,
        "volatility_state": "expansion",
        "sentiment_score": -0.28,
        "whale_dominant": "SELL",
        "whale_bear_weight": 0.72,
        "orderflow_imbalance": -0.41,
        "opportunity_score": 0.84,
    })
    fp = service.build_fingerprint(signal, {"timeframe": "15m"})

    now = datetime.now(timezone.utc)
    rows = []
    for i in range(8):
        rows.append({
            "asset": "BTC-USD",
            "category": "crypto",
            "direction": "SELL",
            "confidence": 0.8,
            "signal_time": (now - timedelta(days=i + 1)).isoformat(),
            "horizon_minutes": 240,
            "actual_price": 0.0,
            "direction_correct": True,
            "target_hit": i < 6,
            "pct_move": -1.8,
            "signal_metadata": json.dumps({"setup_memory_fingerprint": fp}),
        })

    service._fetch_rows = lambda asset, category, days_back, limit: rows
    result = service.score_setup(signal, {"timeframe": "15m"})

    assert result["sample_count"] == 8
    assert result["memory_edge"] > 0
    assert result["adjustment"] > 0
    assert result["same_asset_matches"] == 8

def test_memory_review_reduces_signal_on_negative_memory_edge(monkeypatch) -> None:
    decision_mod = importlib.import_module("core.decision_engine")
    memory_mod = importlib.import_module("services.setup_memory_service")
    engine = decision_mod.SignalDecisionEngine()

    signal = Signal(
        asset="ETH-USD",
        canonical_asset="ETH-USD",
        category="crypto",
        direction="BUY",
        confidence=0.80,
    )

    fake_memory = {
        "fingerprint": {"asset": "ETH-USD", "category": "crypto", "direction": "BUY"},
        "sample_count": 12,
        "same_asset_matches": 7,
        "avg_similarity": 0.74,
        "win_rate": 0.31,
        "target_hit_rate": 0.18,
        "avg_move_pct": -1.9,
        "memory_edge": -0.44,
        "memory_score": 31.5,
        "adjustment": -0.055,
        "notes": ["memory_negative_edge"],
    }
    monkeypatch.setattr(memory_mod.get_service(), "score_setup", lambda signal, context=None: fake_memory, raising=False)

    approved = engine._apply_memory_review(signal, {})

    assert approved is True
    assert signal.confidence == 0.80
    assert signal.metadata["memory_edge"] == fake_memory["memory_edge"]
    assert signal.journal.entries[-1].name == "memory"
    assert signal.journal.entries[-1].decision == "INFO"

def test_execution_feedback_service_detects_premature_stop_and_target_miss() -> None:
    feedback_mod = importlib.import_module("services.execution_feedback_service")
    service = feedback_mod.ExecutionFeedbackService()

    feedback = service.analyze_trade({
        "asset": "XAU/USD",
        "canonical_asset": "XAU/USD",
        "category": "commodities",
        "direction": "BUY",
        "entry_price": 100.0,
        "exit_price": 95.0,
        "stop_loss": 95.0,
        "original_sl": 95.0,
        "take_profit": 110.0,
        "highest_price": 109.5,
        "lowest_price": 94.8,
        "duration_minutes": 40,
        "pnl": -50.0,
        "exit_reason": "Stop Loss",
        "metadata": {
            "timeframe": "15m",
            "setup_quality": 0.72,
            "opportunity_score": 0.81,
        },
    })

    assert feedback["exit_family"] == "stop_loss"
    assert feedback["premature_stop"] is True
    assert feedback["target_miss"] is True
    assert feedback["giveback_ratio"] > 0.45
    assert feedback["quality_score"] < 45.0

def test_execution_feedback_service_reduces_target_for_poor_capture() -> None:
    feedback_mod = importlib.import_module("services.execution_feedback_service")
    service = feedback_mod.ExecutionFeedbackService()
    now = datetime.now(timezone.utc).isoformat()

    poor_feedback = {
        "exit_family": "stop_loss",
        "partial_close": False,
        "full_target": False,
        "premature_stop": True,
        "late_entry": False,
        "target_miss": True,
        "rr_realized": -1.0,
        "mfe_rr": 1.15,
        "mae_rr": 1.05,
        "target_capture": 0.12,
        "giveback_ratio": 0.78,
        "quality_score": 24.0,
        "duration_minutes": 55,
    }
    rows = [
        {
            "asset": "BTC-USD",
            "canonical_asset": "BTC-USD",
            "category": "crypto",
            "entry_time": now,
            "exit_time": now,
            "metadata": {"execution_feedback": poor_feedback},
        }
        for _ in range(12)
    ]

    service._fetch_rows = lambda asset, category, days_back, limit: rows
    policy = service.get_exit_adjustment("BTC-USD", "crypto", {})

    assert policy["sample_count"] >= 12
    assert policy["target_rr_multiplier"] < 1.0
    assert "targets_too_ambitious" in policy["notes"]

def test_state_close_position_attaches_execution_feedback(monkeypatch) -> None:
    state_mod = importlib.import_module("core.state")

    class _FakeDB:
        def __init__(self):
            self.saved = None

        def save_trade(self, trade_data):
            self.saved = trade_data

        def delete_open_position(self, trade_id):
            return None

        def upsert_daily_stats(self, *args, **kwargs):
            return None

    fake_db = _FakeDB()
    monkeypatch.setitem(sys.modules, "services.db_pool", SimpleNamespace(get_db=lambda: fake_db))

    system = state_mod.SystemState()
    monkeypatch.setattr(system, "_persist_json", lambda: None)
    system._open_positions["trade-1"] = {
        "trade_id": "trade-1",
        "asset": "BTC-USD",
        "canonical_asset": "BTC-USD",
        "category": "crypto",
        "direction": "SELL",
        "entry_price": 100.0,
        "stop_loss": 105.0,
        "original_sl": 105.0,
        "take_profit": 90.0,
        "position_size": 1.0,
        "highest_price": 106.0,
        "lowest_price": 91.0,
        "open_time": datetime.utcnow().isoformat(),
        "metadata": {"timeframe": "15m", "setup_quality": 0.66},
    }

    closed = system.close_position("trade-1", 105.0, "Stop Loss", -50.0)

    assert closed is not None
    assert "execution_feedback" in closed["metadata"]
    assert closed["metadata"]["execution_feedback"]["exit_family"] == "stop_loss"
    assert fake_db.saved["metadata"]["execution_feedback"]["exit_family"] == "stop_loss"

def test_state_close_position_attaches_post_trade_review(monkeypatch) -> None:
    state_mod = importlib.import_module("core.state")

    class _FakeDB:
        def __init__(self):
            self.saved = None

        def save_trade(self, trade_data):
            self.saved = trade_data

        def delete_open_position(self, trade_id):
            return None

        def upsert_daily_stats(self, *args, **kwargs):
            return None

    fake_db = _FakeDB()
    monkeypatch.setitem(sys.modules, "services.db_pool", SimpleNamespace(get_db=lambda: fake_db))

    system = state_mod.SystemState()
    monkeypatch.setattr(system, "_persist_json", lambda: None)
    system._open_positions["trade-2"] = {
        "trade_id": "trade-2",
        "asset": "BTC-USD",
        "canonical_asset": "BTC-USD",
        "category": "crypto",
        "direction": "SELL",
        "entry_price": 100.0,
        "stop_loss": 105.0,
        "original_sl": 105.0,
        "take_profit": 90.0,
        "position_size": 1.0,
        "highest_price": 106.0,
        "lowest_price": 91.0,
        "open_time": datetime.utcnow().isoformat(),
        "metadata": {"timeframe": "15m", "setup_quality": 0.66, "memory_score": 39.0, "memory_edge": -0.16, "memory_sample_count": 11},
    }

    closed = system.close_position("trade-2", 105.0, "Stop Loss", -50.0)

    assert closed is not None
    review = closed["metadata"]["post_trade_review"]
    assert review["outcome"] == "loss"
    assert review["what_went_wrong"]
    assert review["lesson"]
    assert review["avoid"]
    assert fake_db.saved["metadata"]["post_trade_review"]["outcome"] == "loss"

def test_state_close_position_keeps_utc_offset_and_duration(monkeypatch) -> None:
    state_mod = importlib.import_module("core.state")

    class _FakeDB:
        def __init__(self):
            self.saved = None

        def save_trade(self, trade_data):
            self.saved = trade_data

        def delete_open_position(self, trade_id):
            return None

        def upsert_daily_stats(self, *args, **kwargs):
            return None

    fake_db = _FakeDB()
    monkeypatch.setitem(sys.modules, "services.db_pool", SimpleNamespace(get_db=lambda: fake_db))

    system = state_mod.SystemState()
    monkeypatch.setattr(system, "_persist_json", lambda: None)
    system._open_positions["trade-utc"] = {
        "trade_id": "trade-utc",
        "asset": "BTC-USD",
        "canonical_asset": "BTC-USD",
        "category": "crypto",
        "direction": "BUY",
        "entry_price": 100.0,
        "stop_loss": 95.0,
        "take_profit": 110.0,
        "position_size": 1.0,
        "open_time": (datetime.now(timezone.utc) - timedelta(minutes=3, seconds=30)).isoformat(),
        "metadata": {},
    }

    closed = system.close_position("trade-utc", 95.0, "Stop Loss", -5.0)

    assert closed is not None
    assert str(closed["exit_time"]).endswith("+00:00")
    assert int(closed["duration_minutes"]) >= 3
    assert str(fake_db.saved["exit_time"]).endswith("+00:00")

def test_personality_record_trade_persists_post_trade_review_in_diary_notes(monkeypatch) -> None:
    personality_mod = importlib.import_module("services.personality_service")

    class _DiaryEntry:
        def __init__(self, **kwargs):
            for key, value in kwargs.items():
                setattr(self, key, value)
            self.id = None

    monkeypatch.setattr(personality_mod, "TradingDiary", _DiaryEntry, raising=False)

    db = object.__new__(personality_mod.PersonalityDatabase)
    db._lock = threading.Lock()
    db._update_mood_from_trade = lambda trade: None
    db._check_memorable = lambda trade, setup: None

    class _FakeSession:
        def __init__(self):
            self.added = None

        def add(self, entry):
            self.added = entry

        def flush(self):
            if self.added is not None:
                self.added.id = 17

    fake_session = _FakeSession()

    class _Ctx:
        def __enter__(self_inner):
            return fake_session

        def __exit__(self_inner, exc_type, exc, tb):
            return False

    db._get_session = lambda: _Ctx()

    entry_id = db.record_trade(
        {
            "asset": "BTC-USD",
            "trade_id": "trade-77",
            "category": "crypto",
            "direction": "SELL",
            "entry_price": 100.0,
            "exit_price": 105.0,
            "confidence": 0.63,
            "exit_reason": "Stop Loss",
            "pnl": -50.0,
            "metadata": {
                "regime": "trending_down",
                "post_trade_review": {
                    "outcome": "loss",
                    "lesson": "Do not chase extended entries; wait for fresher structure or better price location.",
                    "avoid": ["Avoid chasing entries after the move is already mature."],
                    "entry_diagnostics": {
                        "broker_context": "fragile",
                        "micro_context": "hostile",
                        "depth_mode": "synthetic_depth",
                        "cross_asset_context": "conflicted",
                        "cross_asset_primary_peer": "WTI",
                    },
                },
                "execution_feedback": {"exit_family": "stop_loss", "late_entry": True},
                "setup_memory": {"memory_score": 41.0, "memory_edge": -0.14, "sample_count": 9},
                "memory_score": 41.0,
                "memory_edge": -0.14,
                "memory_sample_count": 9,
                "broker_quality": {
                    "score": 0.18,
                    "primary_provider": "IG",
                    "comparison_provider": "Deriv",
                    "quote_agreement_state": "severe_divergence",
                    "spread_regime": "stressed",
                    "quote_quality_state": "stale",
                    "market_transition_risk": 0.81,
                    "fallback_active": True,
                },
                "market_microstructure": {
                    "score": -0.26,
                    "tick_imbalance": -0.31,
                    "book_imbalance": -0.22,
                    "velocity_bps": -1.8,
                    "spread_bps": 8.4,
                    "spread_stress": 1.62,
                    "stop_hunt_risk": 0.58,
                    "exhaustion_risk": 0.49,
                    "depth_available": False,
                    "synthetic_depth_available": True,
                    "microstructure_source": "live_store_synthetic_depth",
                },
                "cross_asset_context": {
                    "score": -0.42,
                    "confidence": 0.8,
                    "state": "sell_support",
                    "supportive_direction": "SELL",
                    "dominant_peer": "WTI",
                    "dominant_relation": "oil_cad_link",
                },
            },
        }
    )

    assert entry_id == 17
    assert fake_session.added.notes["post_trade_review"]["outcome"] == "loss"
    assert fake_session.added.notes["execution_feedback"]["exit_family"] == "stop_loss"
    assert fake_session.added.notes["setup_memory"]["memory_score"] == 41.0
    assert fake_session.added.notes["broker_quality"]["quote_agreement_state"] == "severe_divergence"
    assert fake_session.added.notes["market_microstructure"]["synthetic_depth_available"] is True
    assert fake_session.added.notes["cross_asset_context"]["dominant_peer"] == "WTI"
    assert fake_session.added.notes["entry_diagnostics"]["micro_context"] == "hostile"
    assert fake_session.added.notes["entry_diagnostics"]["cross_asset_context"] == "conflicted"

def test_post_trade_review_service_explains_stop_loss_and_take_profit() -> None:
    review_mod = importlib.import_module("services.post_trade_review_service")
    service = review_mod.get_service()

    loss_review = service.build_review(
        {
            "asset": "BTC-USD",
            "direction": "SELL",
            "pnl": -55.0,
            "metadata": {
                "memory_score": 39.0,
                "memory_edge": -0.18,
                "memory_sample_count": 12,
                "broker_quality": {
                    "score": 0.21,
                    "primary_provider": "IG",
                    "comparison_provider": "Deriv",
                    "quote_agreement_state": "severe_divergence",
                    "spread_regime": "stressed",
                    "quote_quality_state": "stale",
                    "market_transition_risk": 0.78,
                    "fallback_active": True,
                },
                "market_microstructure": {
                    "score": -0.24,
                    "microstructure_alignment": -0.24,
                    "tick_imbalance": -0.28,
                    "book_imbalance": -0.19,
                    "stop_hunt_risk": 0.57,
                    "exhaustion_risk": 0.46,
                    "depth_available": False,
                    "synthetic_depth_available": True,
                    "microstructure_source": "live_store_synthetic_depth",
                },
                "cross_asset_context": {
                    "score": -0.39,
                    "alignment": -0.39,
                    "confidence": 0.81,
                    "state": "sell_support",
                    "supportive_direction": "SELL",
                    "dominant_peer": "ETH-USD",
                    "dominant_relation": "crypto_breadth",
                },
                "execution_feedback": {
                    "exit_family": "stop_loss",
                    "rr_realized": -1.0,
                    "target_capture": 0.0,
                    "giveback_ratio": 0.9,
                    "late_entry": True,
                    "premature_stop": True,
                    "target_miss": True,
                    "stop_too_tight": True,
                    "stop_too_wide": False,
                    "quality_score": 31.0,
                    "regime": "trending_down",
                    "structure_bias": "sell",
                },
            },
        }
    )
    win_review = service.build_review(
        {
            "asset": "BTC-USD",
            "direction": "SELL",
            "pnl": 82.0,
            "metadata": {
                "memory_score": 71.0,
                "memory_edge": 0.24,
                "memory_sample_count": 16,
                "setup_quality": 0.71,
                "alignment_score": 0.74,
                "opportunity_score": 0.81,
                "broker_quality": {
                    "score": 0.83,
                    "primary_provider": "IG",
                    "comparison_provider": "Deriv",
                    "quote_agreement_state": "strong",
                    "spread_regime": "tight",
                    "quote_quality_state": "fresh",
                    "market_transition_risk": 0.08,
                },
                "market_microstructure": {
                    "score": 0.41,
                    "microstructure_alignment": 0.41,
                    "tick_imbalance": 0.52,
                    "book_imbalance": 0.37,
                    "stop_hunt_risk": 0.09,
                    "exhaustion_risk": 0.08,
                    "depth_available": True,
                    "synthetic_depth_available": False,
                    "microstructure_source": "order_flow_true_depth",
                },
                "cross_asset_context": {
                    "score": 0.34,
                    "alignment": 0.34,
                    "confidence": 0.77,
                    "state": "sell_support",
                    "supportive_direction": "SELL",
                    "dominant_peer": "ETH-USD",
                    "dominant_relation": "crypto_breadth",
                },
                "execution_feedback": {
                    "exit_family": "take_profit",
                    "rr_realized": 1.5,
                    "target_capture": 1.0,
                    "giveback_ratio": 0.11,
                    "quality_score": 77.0,
                    "regime": "trending_down",
                    "structure_bias": "sell",
                },
            },
        }
    )

    assert loss_review["outcome"] == "loss"
    assert loss_review["what_went_wrong"]
    assert loss_review["avoid"]
    assert "brokers disagree" in loss_review["lesson"]
    assert loss_review["entry_diagnostics"]["broker_context"] == "fragile"
    assert loss_review["entry_diagnostics"]["micro_context"] == "hostile"
    assert loss_review["entry_diagnostics"]["depth_mode"] == "synthetic_depth"
    assert loss_review["entry_diagnostics"]["cross_asset_context"] == "conflicted"
    assert loss_review["entry_diagnostics"]["cross_asset_primary_peer"] == "ETH-USD"
    assert any("ETH-USD" in item for item in loss_review["what_went_wrong"])
    assert win_review["outcome"] == "win"
    assert win_review["what_went_right"]
    assert win_review["keep"]
    assert "Keep" in win_review["next_focus"]
    assert win_review["entry_diagnostics"]["broker_context"] == "supportive"
    assert win_review["entry_diagnostics"]["depth_mode"] == "true_depth"
    assert win_review["entry_diagnostics"]["cross_asset_context"] == "supportive"
    assert any("ETH-USD" in item for item in win_review["what_went_right"])

def test_generate_seed_signal_uses_execution_feedback_policy(monkeypatch) -> None:
    engine = TradingCore(balance=10_000.0)
    engine._predictor = SimpleNamespace(predict=lambda canonical, category, df: (0.20, 0.85))

    feedback_mod = importlib.import_module("services.execution_feedback_service")
    monkeypatch.setattr(
        feedback_mod.get_service(),
        "get_exit_adjustment",
        lambda asset, category, context=None: {
            "sample_count": 14,
            "target_rr_multiplier": 0.86,
            "stop_buffer_multiplier": 1.09,
            "avg_quality_score": 36.5,
            "notes": ["targets_too_ambitious"],
        },
        raising=False,
    )

    seen: dict = {}

    class _RiskStub:
        def get_stop_loss_scaled(self, entry, direction, category, atr=0.0, distance_multiplier=1.0):
            seen["stop_buffer_multiplier"] = distance_multiplier
            return entry + 10.0 if direction == "SELL" else entry - 10.0

        def get_take_profit(self, entry, stop_loss, direction, category="", rr=None, rr_multiplier=1.0):
            seen["target_rr_multiplier"] = rr_multiplier
            return entry - 12.0 if direction == "SELL" else entry + 12.0

    engine._risk_manager = _RiskStub()

    price_data = pd.DataFrame(
        {
            "high": [66400, 66440, 66490, 66510, 66530, 66520, 66510, 66480, 66450, 66410, 66390, 66370, 66350, 66320, 66300, 66280],
            "low": [66320, 66340, 66380, 66410, 66440, 66420, 66400, 66370, 66340, 66310, 66290, 66270, 66240, 66210, 66190, 66170],
            "close": [66360, 66390, 66440, 66480, 66500, 66470, 66440, 66410, 66380, 66340, 66310, 66290, 66270, 66240, 66220, 66200],
        }
    )

    signal = engine._generate_seed_signal(
        "BTC-USD",
        "BTC-USD",
        "crypto",
        price_data,
        {"market_data": {}, "timeframe": "15m"},
    )

    assert signal is not None
    assert round(seen["stop_buffer_multiplier"], 2) == 1.09
    assert round(seen["target_rr_multiplier"], 2) == 0.86
    assert signal.metadata["execution_feedback_policy"]["sample_count"] == 14
    assert signal.metadata["target_rr_multiplier"] == 0.86

def test_top_ranked_opportunities_can_skip_refresh_when_snapshot_empty(monkeypatch) -> None:
    engine = TradingCore(balance=10_000.0)
    engine._engine_ready.set()
    calls: list[int] = []

    def _fake_scan(limit: int = 5):
        calls.append(limit)
        engine._last_ranked_opportunities = []
        return []

    monkeypatch.setattr(engine, "scan_top_ranked_opportunities", _fake_scan, raising=False)

    ranked = engine.get_top_ranked_opportunities(
        limit=3,
        refresh=False,
        include_positions=False,
        allow_refresh_when_empty=False,
    )

    assert ranked == []
    assert calls == []

def test_get_weak_positions_can_skip_provider_market_status() -> None:
    engine = TradingCore(balance=10_000.0)
    engine.state._open_positions = {
        "t1": {
            "trade_id": "t1",
            "asset": "BNB-USD",
            "category": "crypto",
            "direction": "BUY",
            "confidence": 0.61,
            "pnl": -4.0,
            "risk_reward": 1.25,
            "metadata": {
                "opportunity_score": 0.44,
                "memory_score": 58.0,
                "memory_sample_count": 7,
                "execution_feedback": {"quality_score": 53.0, "sample_count": 6},
            },
        }
    }

    def _boom(*args, **kwargs):
        raise AssertionError("provider market status should be skipped")

    engine._market_hours_status = _boom  # type: ignore[method-assign]

    weak = engine.get_weak_positions(limit=3, include_market_status=False)

    assert weak
    assert weak[0]["asset"] == "BNB-USD"
    assert weak[0]["market_reason"] == "crypto_24x7"

def test_generate_seed_signal_aligns_take_profit_to_structure() -> None:
    engine = TradingCore(balance=10_000.0)
    engine._predictor = SimpleNamespace(predict=lambda canonical, category, df: (0.80, 0.88))
    engine._risk_manager = importlib.import_module("risk.manager").RiskManager(account_balance=10_000.0)

    price_data = pd.DataFrame(
        {
            "high": [100.2, 100.5, 100.8, 101.0, 101.3, 101.5, 101.8, 102.0, 102.2, 102.3, 102.5, 102.7, 102.8, 103.0, 103.1, 103.2],
            "low": [99.7, 99.9, 100.1, 100.3, 100.5, 100.8, 101.0, 101.2, 101.4, 101.5, 101.7, 101.9, 102.1, 102.2, 102.4, 102.5],
            "close": [100.0, 100.2, 100.5, 100.7, 101.0, 101.2, 101.5, 101.7, 101.9, 102.0, 102.2, 102.4, 102.6, 102.7, 102.9, 103.0],
        }
    )
    context = {
        "market_data": {},
        "timeframe": "15m",
        "market_structure": {
            "resistance": 104.0,
            "support": 101.6,
            "regime": "ranging",
            "structure_bias": "buy",
            "alignment_score": 0.62,
            "setup_quality": 0.58,
            "breakout_score": 0.10,
            "volatility_state": "normal",
        },
    }

    signal = engine._generate_seed_signal("BTC-USD", "BTC-USD", "crypto", price_data, context)

    assert signal is not None
    base_take_profit = engine._risk_manager.get_take_profit(
        signal.entry_price,
        signal.stop_loss,
        "BUY",
        category="crypto",
        rr_multiplier=signal.metadata["target_rr_multiplier"],
    )
    assert signal.take_profit < base_take_profit
    assert signal.metadata["structure_target_alignment"]["applied"] is True

def test_prune_stale_log_artifacts_removes_only_old_one_off_logs(tmp_path) -> None:
    logger_mod = importlib.import_module("utils.logger")

    old_probe = tmp_path / "sample.out.log"
    old_probe.write_text("old", encoding="utf-8")
    old_age = (datetime.now() - timedelta(days=20)).timestamp()
    os.utime(old_probe, (old_age, old_age))

    recent_probe = tmp_path / "recent.out.log"
    recent_probe.write_text("recent", encoding="utf-8")

    core_log = tmp_path / "trading_bot.log"
    core_log.write_text("keep", encoding="utf-8")
    os.utime(core_log, (old_age, old_age))

    removed = logger_mod.prune_stale_log_artifacts(tmp_path, retention_days=14)

    assert removed == 1
    assert old_probe.exists() is False
    assert recent_probe.exists() is True
    assert core_log.exists() is True

def test_get_rotating_file_logger_reuses_named_logger_and_writes_file(tmp_path) -> None:
    logger_mod = importlib.import_module("utils.logger")
    log_path = tmp_path / "playbook_runtime.log"

    logger_a = logger_mod.get_rotating_file_logger(
        "unit_playbook_runtime",
        log_path,
        max_bytes=512,
        backup_count=1,
    )
    logger_b = logger_mod.get_rotating_file_logger(
        "unit_playbook_runtime",
        log_path,
        max_bytes=512,
        backup_count=1,
    )

    assert logger_a is logger_b

    logger_a.info("hello from rotating logger")
    for handler in logger_a.handlers:
        handler.flush()

    assert log_path.exists() is True
    assert "hello from rotating logger" in log_path.read_text(encoding="utf-8")

def test_signal_confidence_caps_at_max_signal_confidence() -> None:
    config_mod = importlib.import_module("config.config")
    max_conf = float(config_mod.MAX_SIGNAL_CONFIDENCE)

    signal = Signal(
        asset="EUR/USD",
        canonical_asset="EUR/USD",
        category="forex",
        direction="BUY",
        confidence=max_conf + 0.10,
        entry_price=1.1,
        stop_loss=1.09,
        take_profit=1.12,
    )

    assert signal.confidence == max_conf

    signal.boost(0.20)

    assert signal.confidence == max_conf

def test_signal_scorecard_penalizes_poor_live_accuracy(monkeypatch) -> None:
    scorecard_mod = importlib.import_module("services.signal_scorecard")

    class _Tracker:
        @staticmethod
        def get_accuracy_stats(days_back: int = 30):
            return {
                "by_asset": {
                    "ETH-USD": {
                        "1H": {"total": 35, "accuracy_pct": 28.6},
                    }
                }
            }

    monkeypatch.setitem(sys.modules, "prediction_tracker", SimpleNamespace(prediction_tracker=_Tracker()))
    scorecard = scorecard_mod.get_service()
    signal = Signal(
        asset="ETH-USD",
        canonical_asset="ETH-USD",
        category="crypto",
        direction="BUY",
        confidence=0.90,
        entry_price=100.0,
        stop_loss=98.0,
        take_profit=104.0,
        risk_reward=2.0,
        metadata={
            "ml_confidence": 0.95,
            "alignment_score": 0.9,
            "setup_quality": 0.9,
            "structure_bias": "buy",
            "regime": "trending_up",
        },
    )

    payload = scorecard.score(signal, {"spread": 0.05})

    assert payload["live_validation"]["samples"] == 35
    assert payload["live_validation"]["accuracy_pct"] == 28.6
    assert payload["final_score"] < 0.35

def test_signal_scorecard_uses_playbook_seed_strength(monkeypatch) -> None:
    scorecard_mod = importlib.import_module("services.signal_scorecard")

    class _Tracker:
        @staticmethod
        def get_accuracy_stats(days_back: int = 30):
            return {}

    monkeypatch.setitem(sys.modules, "prediction_tracker", SimpleNamespace(prediction_tracker=_Tracker()))
    scorecard = scorecard_mod.get_service()

    base_signal = Signal(
        asset="EUR/USD",
        canonical_asset="EUR/USD",
        category="forex",
        direction="BUY",
        confidence=0.76,
        entry_price=1.1520,
        stop_loss=1.1508,
        take_profit=1.1540,
        risk_reward=1.67,
    )
    base_signal.metadata.update({
        "ml_confidence": 0.05,
        "seed_candidate_score": 0.76,
        "market_structure": {
            "alignment_score": 0.72,
            "setup_quality": 0.70,
            "pullback_score": 0.12,
            "breakout_score": 0.76,
            "structure_bias": "buy",
        },
        "regime": "trending_up",
    })

    playbook_signal = Signal(
        asset="EUR/USD",
        canonical_asset="EUR/USD",
        category="forex",
        direction="BUY",
        confidence=0.76,
        entry_price=1.1520,
        stop_loss=1.1508,
        take_profit=1.1540,
        risk_reward=1.67,
    )
    playbook_signal.metadata.update({
        **dict(base_signal.metadata),
        "seed_source": "playbook",
        "seed_model": "breakout_continuation",
        "playbook_action": "seed",
        "playbook_name": "breakout_continuation",
        "playbook_confidence": 0.74,
        "playbook_score": 0.71,
    })

    base_score = scorecard.score(base_signal, {})
    playbook_score = scorecard.score(playbook_signal, {})

    assert playbook_score["final_score"] > base_score["final_score"]
    assert "playbook breakout_continuation seeded the trade" in playbook_score["notes"]

def test_market_review_records_evidence_without_mutating_score(monkeypatch) -> None:
    decision_mod = importlib.import_module("core.decision_engine")
    engine = decision_mod.SignalDecisionEngine()

    monkeypatch.setattr(
        decision_mod,
        "_get_news_state",
        lambda category: {"state": "clear", "event": "", "impact": "", "direction": "", "mins_to": 0},
        raising=False,
    )
    monkeypatch.setattr(decision_mod, "_get_orderflow_imbalance", lambda asset: 0.41, raising=False)

    signal = Signal(
        asset="BTC-USD",
        canonical_asset="BTC-USD",
        category="crypto",
        direction="BUY",
        confidence=0.73,
        entry_price=100.0,
        stop_loss=98.0,
        take_profit=104.0,
    )
    context = {
        "ml_prediction": 0.81,
        "ml_confidence": 0.88,
        "spread": 0.04,
        "market_microstructure": {"score": 0.44, "stop_hunt_risk": 0.12},
        "market_structure": {
            "structure_bias": "buy",
            "alignment_score": 0.83,
            "setup_quality": 0.77,
            "pullback_score": 0.34,
            "breakout_score": 0.59,
            "volatility_state": "expansion",
            "regime": "trending_up",
        },
        "market_status": {"market_open": True, "reason": "open"},
        "regime": "trending_up",
    }

    approved = engine._apply_market_review(signal, context)

    assert approved is True
    assert signal.confidence == 0.73
    assert signal.metadata["ml_direction"] == "BUY"
    assert signal.metadata["ml_direction_agrees"] is True
    assert signal.metadata["news_state"] == "clear"
    assert "ml_agrees" in signal.metadata["market_review_notes"]

def test_broker_quality_service_scores_agreement_and_transition() -> None:
    broker_mod = importlib.import_module("services.broker_quality_service")
    service = broker_mod.BrokerQualityService()

    class _Fetcher:
        @staticmethod
        def get_provider_quote(asset: str, category: str, provider: str):
            if provider == "deriv":
                return 100.01, 0.03, {
                    "source": "Deriv",
                    "source_class": "stream",
                    "realtime": True,
                    "live_age_seconds": 1.4,
                }
            return None, None, {}

        @staticmethod
        def get_provider_market_status(asset: str, category: str, provider: str):
            if provider == "deriv":
                return {"market_open": True}
            return None

    first = service.build_snapshot(
        asset="XAU/USD",
        category="commodities",
        fetcher=_Fetcher(),
        primary_price=100.0,
        primary_spread=0.02,
        primary_meta={
            "source": "IG",
            "source_class": "stream",
            "realtime": True,
            "live_age_seconds": 0.8,
        },
        market_status={"market_open": True},
    )
    second = service.build_snapshot(
        asset="XAU/USD",
        category="commodities",
        fetcher=_Fetcher(),
        primary_price=100.0,
        primary_spread=0.12,
        primary_meta={
            "source": "IG",
            "source_class": "stream",
            "realtime": True,
            "live_age_seconds": 11.0,
        },
        market_status={"market_open": False},
    )

    assert first["primary_provider"] == "IG"
    assert first["comparison_provider"] == "Deriv"
    assert first["quote_agreement_state"] in {"strong", "aligned"}
    assert first["quote_quality_state"] == "fresh"
    assert first["spread_regime"] == "tight"
    assert second["market_state_changed"] is True
    assert second["market_transition_risk"] >= 0.65
    assert "market_state_changed" in second["notes"]

def test_market_review_records_broker_quality_notes(monkeypatch) -> None:
    decision_mod = importlib.import_module("core.decision_engine")
    engine = decision_mod.SignalDecisionEngine()

    monkeypatch.setattr(
        decision_mod,
        "_get_news_state",
        lambda category: {"state": "clear", "event": "", "impact": "", "direction": "", "mins_to": 0},
        raising=False,
    )
    monkeypatch.setattr(decision_mod, "_get_orderflow_imbalance", lambda asset: 0.0, raising=False)

    signal = Signal(
        asset="WTI",
        canonical_asset="WTI",
        category="commodities",
        direction="BUY",
        confidence=0.71,
        entry_price=100.0,
        stop_loss=98.0,
        take_profit=104.0,
    )
    context = {
        "ml_prediction": 0.73,
        "ml_confidence": 0.79,
        "spread": 0.15,
        "market_microstructure": {},
        "market_structure": {
            "structure_bias": "buy",
            "alignment_score": 0.61,
            "setup_quality": 0.58,
            "pullback_score": 0.15,
            "breakout_score": 0.48,
            "volatility_state": "normal",
            "regime": "trending_up",
        },
        "broker_quality": {
            "score": 0.24,
            "primary_provider": "IG",
            "comparison_provider": "Deriv",
            "quote_agreement_state": "severe_divergence",
            "spread_regime": "stressed",
            "quote_quality_state": "stale",
            "market_state": "EDITS_ONLY",
            "market_state_changed": True,
            "market_transition_risk": 0.84,
            "fallback_active": True,
        },
        "market_status": {"market_open": True, "reason": "open"},
        "regime": "trending_up",
    }

    approved = engine._apply_market_review(signal, context)

    assert approved is True
    assert signal.metadata["broker_quality_score"] == 0.24
    assert signal.metadata["broker_agreement_state"] == "severe_divergence"
    assert "broker_severe_divergence" in signal.metadata["market_review_notes"]
    assert "spread_stressed" in signal.metadata["market_review_notes"]
    assert "quote_stale" in signal.metadata["market_review_notes"]
    assert "market_transition_risk" in signal.metadata["market_review_notes"]
    assert "provider_fallback_active" in signal.metadata["market_review_notes"]

def test_market_review_records_enriched_microstructure_notes(monkeypatch) -> None:
    decision_mod = importlib.import_module("core.decision_engine")
    engine = decision_mod.SignalDecisionEngine()

    monkeypatch.setattr(
        decision_mod,
        "_get_news_state",
        lambda category: {"state": "clear", "event": "", "impact": "", "direction": "", "mins_to": 0},
        raising=False,
    )
    monkeypatch.setattr(decision_mod, "_get_orderflow_imbalance", lambda asset: 0.0, raising=False)

    signal = Signal(
        asset="XAU/USD",
        canonical_asset="XAU/USD",
        category="commodities",
        direction="BUY",
        confidence=0.70,
        entry_price=100.0,
        stop_loss=98.0,
        take_profit=104.0,
    )
    context = {
        "ml_prediction": 0.76,
        "ml_confidence": 0.81,
        "spread": 0.03,
        "market_microstructure": {
            "score": 0.42,
            "stop_hunt_risk": 0.14,
            "exhaustion_risk": 0.47,
            "tick_imbalance": 0.61,
            "book_imbalance": 0.33,
            "velocity_bps": 2.1,
            "depth_available": True,
        },
        "market_structure": {
            "structure_bias": "buy",
            "alignment_score": 0.72,
            "setup_quality": 0.69,
            "pullback_score": 0.18,
            "breakout_score": 0.52,
            "volatility_state": "expansion",
            "regime": "trending_up",
        },
        "market_status": {"market_open": True, "reason": "open"},
        "regime": "trending_up",
    }

    approved = engine._apply_market_review(signal, context)

    assert approved is True
    assert signal.metadata["book_imbalance"] == 0.33
    assert signal.metadata["exhaustion_risk"] == 0.47
    assert "book_pressure_support" in signal.metadata["market_review_notes"]
    assert "micro_momentum_support" in signal.metadata["market_review_notes"]
    assert "micro_exhaustion" in signal.metadata["market_review_notes"]

def test_signal_intelligence_cross_asset_review_aligns_with_trade_direction() -> None:
    intel_mod = importlib.import_module("services.signal_intelligence")

    signal = Signal(
        asset="USD/CAD",
        canonical_asset="USD/CAD",
        category="forex",
        direction="SELL",
        confidence=0.69,
    )
    payload = intel_mod.apply_cross_asset_review(
        signal,
        {
            "cross_asset_context": {
                "score": -0.44,
                "confidence": 0.76,
                "state": "sell_support",
                "supportive_direction": "SELL",
                "dominant_peer": "WTI",
                "dominant_relation": "oil_cad_link",
                "peers": [
                    {
                        "peer_asset": "WTI",
                        "relation_label": "oil_cad_link",
                        "supportive_direction": "SELL",
                        "peer_direction": "BUY",
                        "buy_bias": -0.44,
                        "weight": 1.0,
                        "state": "supportive",
                    }
                ],
            }
        },
    )

    assert payload["alignment"] == 0.44
    assert signal.metadata["cross_asset_alignment"] == 0.44
    assert signal.metadata["cross_asset_primary_peer"] == "WTI"
    assert "cross_asset_support" in payload["adjustments"]

def test_signal_scorecard_penalizes_ml_conflict_and_event_risk(monkeypatch) -> None:
    scorecard_mod = importlib.import_module("services.signal_scorecard")

    class _Tracker:
        @staticmethod
        def get_accuracy_stats(days_back: int = 30):
            return {"by_asset": {}}

    monkeypatch.setitem(sys.modules, "prediction_tracker", SimpleNamespace(prediction_tracker=_Tracker()))
    scorecard = scorecard_mod.get_service()

    base_metadata = {
        "ml_confidence": 0.92,
        "ml_prediction_real": True,
        "alignment_score": 0.84,
        "setup_quality": 0.81,
        "structure_bias": "buy",
        "market_structure": {"structure_bias": "buy", "alignment_score": 0.84, "setup_quality": 0.81},
        "regime": "trending_up",
        "session": "us",
    }
    aligned = Signal(
        asset="ETH-USD",
        canonical_asset="ETH-USD",
        category="crypto",
        direction="BUY",
        confidence=0.82,
        entry_price=100.0,
        stop_loss=98.0,
        take_profit=104.0,
        risk_reward=2.0,
        metadata={
            **base_metadata,
            "ml_direction": "BUY",
            "ml_direction_agrees": True,
            "news_state": "clear",
        },
    )
    conflicted = Signal(
        asset="ETH-USD",
        canonical_asset="ETH-USD",
        category="crypto",
        direction="BUY",
        confidence=0.82,
        entry_price=100.0,
        stop_loss=98.0,
        take_profit=104.0,
        risk_reward=2.0,
        metadata={
            **base_metadata,
            "ml_direction": "SELL",
            "ml_direction_agrees": False,
            "news_state": "pre",
            "news_impact": "MEDIUM",
        },
    )

    aligned_payload = scorecard.score(aligned, {"spread": 0.04})
    conflicted_payload = scorecard.score(conflicted, {"spread": 0.04})

    assert conflicted_payload["breakdown"]["ml_alignment"] < aligned_payload["breakdown"]["ml_alignment"]
    assert conflicted_payload["breakdown"]["news"] < aligned_payload["breakdown"]["news"]
    assert conflicted_payload["final_score"] < aligned_payload["final_score"]

def test_signal_scorecard_penalizes_poor_broker_quality(monkeypatch) -> None:
    scorecard_mod = importlib.import_module("services.signal_scorecard")

    class _Tracker:
        @staticmethod
        def get_accuracy_stats(days_back: int = 30):
            return {"by_asset": {}}

    monkeypatch.setitem(sys.modules, "prediction_tracker", SimpleNamespace(prediction_tracker=_Tracker()))
    scorecard = scorecard_mod.get_service()

    base_metadata = {
        "ml_confidence": 0.88,
        "ml_prediction_real": True,
        "ml_direction": "BUY",
        "ml_direction_agrees": True,
        "alignment_score": 0.82,
        "setup_quality": 0.78,
        "structure_bias": "buy",
        "market_structure": {"structure_bias": "buy", "alignment_score": 0.82, "setup_quality": 0.78},
        "regime": "trending_up",
        "session": "us",
        "news_state": "clear",
    }
    strong = Signal(
        asset="XAU/USD",
        canonical_asset="XAU/USD",
        category="commodities",
        direction="BUY",
        confidence=0.80,
        entry_price=100.0,
        stop_loss=98.0,
        take_profit=104.0,
        risk_reward=2.0,
        metadata={
            **base_metadata,
            "broker_quality": {
                "score": 0.91,
                "quote_agreement_state": "strong",
                "quote_quality_state": "fresh",
                "spread_regime": "tight",
            },
        },
    )
    weak = Signal(
        asset="XAU/USD",
        canonical_asset="XAU/USD",
        category="commodities",
        direction="BUY",
        confidence=0.80,
        entry_price=100.0,
        stop_loss=98.0,
        take_profit=104.0,
        risk_reward=2.0,
        metadata={
            **base_metadata,
            "broker_quality": {
                "score": 0.18,
                "quote_agreement_state": "severe_divergence",
                "quote_quality_state": "stale",
                "spread_regime": "extreme",
            },
        },
    )

    strong_payload = scorecard.score(strong, {"spread": 0.04})
    weak_payload = scorecard.score(weak, {"spread": 0.04})

    assert weak_payload["breakdown"]["broker_quality"] < strong_payload["breakdown"]["broker_quality"]
    assert weak_payload["final_score"] < strong_payload["final_score"]
    assert any("brokers materially disagree" in note for note in weak_payload["notes"])

def test_signal_scorecard_penalizes_enriched_microstructure_risk(monkeypatch) -> None:
    scorecard_mod = importlib.import_module("services.signal_scorecard")

    class _Tracker:
        @staticmethod
        def get_accuracy_stats(days_back: int = 30):
            return {"by_asset": {}}

    monkeypatch.setitem(sys.modules, "prediction_tracker", SimpleNamespace(prediction_tracker=_Tracker()))
    scorecard = scorecard_mod.get_service()

    base_metadata = {
        "ml_confidence": 0.86,
        "ml_prediction_real": True,
        "ml_direction": "BUY",
        "ml_direction_agrees": True,
        "alignment_score": 0.79,
        "setup_quality": 0.75,
        "structure_bias": "buy",
        "market_structure": {"structure_bias": "buy", "alignment_score": 0.79, "setup_quality": 0.75},
        "regime": "trending_up",
        "session": "us",
        "news_state": "clear",
    }
    supportive = Signal(
        asset="WTI",
        canonical_asset="WTI",
        category="commodities",
        direction="BUY",
        confidence=0.79,
        entry_price=100.0,
        stop_loss=98.0,
        take_profit=104.0,
        risk_reward=2.0,
        metadata={
            **base_metadata,
            "microstructure_score": 0.35,
            "tick_imbalance": 0.55,
            "book_imbalance": 0.28,
            "stop_hunt_risk": 0.08,
            "exhaustion_risk": 0.06,
        },
    )
    risky = Signal(
        asset="WTI",
        canonical_asset="WTI",
        category="commodities",
        direction="BUY",
        confidence=0.79,
        entry_price=100.0,
        stop_loss=98.0,
        take_profit=104.0,
        risk_reward=2.0,
        metadata={
            **base_metadata,
            "microstructure_score": 0.12,
            "tick_imbalance": -0.18,
            "book_imbalance": -0.24,
            "stop_hunt_risk": 0.52,
            "exhaustion_risk": 0.48,
        },
    )

    supportive_payload = scorecard.score(supportive, {"spread": 0.03})
    risky_payload = scorecard.score(risky, {"spread": 0.03})

    assert risky_payload["breakdown"]["microstructure"] < supportive_payload["breakdown"]["microstructure"]
    assert risky_payload["final_score"] < supportive_payload["final_score"]

def test_signal_scorecard_uses_cross_asset_confirmation(monkeypatch) -> None:
    scorecard_mod = importlib.import_module("services.signal_scorecard")

    class _Tracker:
        @staticmethod
        def get_accuracy_stats(days_back: int = 30):
            return {"by_asset": {}}

    monkeypatch.setitem(sys.modules, "prediction_tracker", SimpleNamespace(prediction_tracker=_Tracker()))
    scorecard = scorecard_mod.get_service()

    base_metadata = {
        "ml_confidence": 0.88,
        "ml_prediction_real": True,
        "ml_direction": "SELL",
        "ml_direction_agrees": True,
        "alignment_score": 0.77,
        "setup_quality": 0.74,
        "structure_bias": "sell",
        "market_structure": {"structure_bias": "sell", "alignment_score": 0.77, "setup_quality": 0.74},
        "regime": "trending_down",
        "session": "us",
        "news_state": "clear",
    }
    confirmed = Signal(
        asset="USD/CAD",
        canonical_asset="USD/CAD",
        category="forex",
        direction="SELL",
        confidence=0.78,
        entry_price=1.35,
        stop_loss=1.36,
        take_profit=1.33,
        risk_reward=2.0,
        metadata={
            **base_metadata,
            "cross_asset_alignment": 0.42,
            "cross_asset_confidence": 0.81,
            "cross_asset_primary_peer": "WTI",
        },
    )
    conflicted = Signal(
        asset="USD/CAD",
        canonical_asset="USD/CAD",
        category="forex",
        direction="SELL",
        confidence=0.78,
        entry_price=1.35,
        stop_loss=1.36,
        take_profit=1.33,
        risk_reward=2.0,
        metadata={
            **base_metadata,
            "cross_asset_alignment": -0.38,
            "cross_asset_confidence": 0.81,
            "cross_asset_primary_peer": "WTI",
        },
    )

    confirmed_payload = scorecard.score(confirmed, {"spread": 0.0005})
    conflicted_payload = scorecard.score(conflicted, {"spread": 0.0005})

    assert confirmed_payload["breakdown"]["cross_asset"] > conflicted_payload["breakdown"]["cross_asset"]
    assert confirmed_payload["final_score"] > conflicted_payload["final_score"]
    assert any("cross-asset spillover conflicts" in note for note in conflicted_payload["notes"])

def test_squash_confidence_makes_top_end_hard_to_reach() -> None:
    confidence_mod = importlib.import_module("core.confidence")

    assert round(confidence_mod.squash_confidence(1.0), 3) == 0.95
    assert confidence_mod.squash_confidence(0.90) < 0.80
    assert confidence_mod.squash_confidence(0.80) < 0.60

def test_sentiment_review_no_longer_mutates_signal_score() -> None:
    intelligence_mod = importlib.import_module("services.signal_intelligence")
    signal = Signal(
        asset="BTC-USD",
        canonical_asset="BTC-USD",
        category="crypto",
        direction="BUY",
        confidence=0.71,
        entry_price=100.0,
        stop_loss=98.0,
        take_profit=104.0,
        metadata={},
    )

    result = intelligence_mod.apply_sentiment_review(
        signal,
        {
            "sentiment_details": {
                "score": 0.7,
                "composite_score": 0.7,
                "components": {"reddit": 0.8},
                "weights": {"reddit": 1.0},
            },
            "market_intelligence": {
                "dominant_narrative": "RISK_ON",
                "narrative_strength": 0.4,
            },
        },
    )

    assert signal.confidence == 0.71
    assert "sentiment_support" in result["adjustments"]

