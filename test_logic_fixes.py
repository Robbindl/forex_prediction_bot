from __future__ import annotations

import importlib
import sys
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import core.state as state_mod
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
