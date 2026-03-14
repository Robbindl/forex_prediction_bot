"""Tests for SystemState — cooldowns, positions, balance, day rollover."""
import pytest
from unittest.mock import patch
from datetime import datetime, timedelta


@pytest.fixture
def state():
    """Fresh SystemState for each test — DB calls are mocked by conftest."""
    from core.state import SystemState
    s = SystemState()
    s._open_positions   = {}
    s._closed_positions = []
    s._balance          = 1000.0
    s._initial_balance  = 1000.0
    s._daily_trades     = 0
    s._daily_pnl        = 0.0
    s._cooldowns        = {}
    return s


def _make_position(trade_id="abc123", asset="BTC-USD",
                   category="crypto", direction="BUY"):
    return {
        "trade_id":        trade_id,
        "asset":           asset,
        "canonical_asset": asset,
        "category":        category,
        "direction":       direction,
        "signal":          direction,
        "entry_price":     50000.0,
        "stop_loss":       49000.0,
        "take_profit":     52000.0,
        "position_size":   0.001,
        "confidence":      0.8,
        "strategy_id":     "RSI",
        "open_time":       datetime.utcnow().isoformat(),
        "session":         "london",
    }


# ── Positions ─────────────────────────────────────────────────────────────────

def test_add_position(state):
    pos = _make_position()
    state.add_position(pos)
    assert state.open_position_count() == 1
    assert state.has_open_position_for("BTC-USD")


def test_add_multiple_positions(state):
    state.add_position(_make_position("t1", "BTC-USD"))
    state.add_position(_make_position("t2", "ETH-USD"))
    assert state.open_position_count() == 2


def test_close_position_returns_closed(state):
    pos = _make_position()
    state.add_position(pos)
    closed = state.close_position("abc123", 51000.0, "Take Profit", 100.0)
    assert closed is not None
    assert closed["exit_price"]  == 51000.0
    assert closed["exit_reason"] == "Take Profit"
    assert closed["pnl"]         == 100.0


def test_close_position_removes_from_open(state):
    state.add_position(_make_position())
    state.close_position("abc123", 51000.0, "Take Profit", 100.0)
    assert state.open_position_count() == 0
    assert not state.has_open_position_for("BTC-USD")


def test_close_nonexistent_returns_none(state):
    result = state.close_position("doesnotexist", 50000, "SL", -50)
    assert result is None


def test_balance_increases_on_win(state):
    state.add_position(_make_position())
    state.close_position("abc123", 51000.0, "Take Profit", 200.0)
    assert state.balance == pytest.approx(1200.0)


def test_balance_decreases_on_loss(state):
    state.add_position(_make_position())
    state.close_position("abc123", 49000.0, "Stop Loss", -100.0)
    assert state.balance == pytest.approx(900.0)


def test_daily_pnl_accumulates(state):
    state.add_position(_make_position("t1", "BTC-USD"))
    state.add_position(_make_position("t2", "ETH-USD"))
    state.close_position("t1", 51000, "TP", 100.0)
    state.close_position("t2", 49000, "SL", -50.0)
    assert state.daily_pnl == pytest.approx(50.0)


def test_daily_trades_increments(state):
    state.add_position(_make_position("t1", "BTC-USD"))
    state.add_position(_make_position("t2", "ETH-USD"))
    assert state.daily_trades == 2


def test_get_open_positions_returns_list(state):
    state.add_position(_make_position())
    positions = state.get_open_positions()
    assert isinstance(positions, list)
    assert len(positions) == 1
    assert positions[0]["trade_id"] == "abc123"


def test_has_open_position_false_when_empty(state):
    assert not state.has_open_position_for("BTC-USD")


# ── Balance ───────────────────────────────────────────────────────────────────

def test_set_balance(state):
    state.set_balance(5000.0)
    assert state.balance == 5000.0


def test_adjust_balance_positive(state):
    new_bal = state.adjust_balance(500.0)
    assert new_bal == 1500.0
    assert state.balance == 1500.0


def test_adjust_balance_negative(state):
    new_bal = state.adjust_balance(-200.0)
    assert new_bal == 800.0


# ── Cooldowns ─────────────────────────────────────────────────────────────────

def test_set_and_check_cooldown(state):
    state.set_cooldown("BTC-USD", minutes=60)
    assert state.is_cooling_down("BTC-USD")


def test_cooldown_not_active_for_other_asset(state):
    state.set_cooldown("BTC-USD", minutes=60)
    assert not state.is_cooling_down("ETH-USD")


def test_cooldown_remaining_returns_positive(state):
    state.set_cooldown("BTC-USD", minutes=60)
    remaining = state.cooldown_remaining("BTC-USD")
    assert 58 <= remaining <= 60


def test_expired_cooldown_returns_false(state):
    # Manually set an already-expired cooldown
    state._cooldowns["BTC-USD"] = datetime.now() - timedelta(minutes=1)
    assert not state.is_cooling_down("BTC-USD")
    assert "BTC-USD" not in state._cooldowns


def test_get_all_cooldowns_excludes_expired(state):
    state._cooldowns["BTC-USD"] = datetime.now() + timedelta(minutes=30)
    state._cooldowns["ETH-USD"] = datetime.now() - timedelta(minutes=1)
    cooldowns = state.get_all_cooldowns()
    assert "BTC-USD" in cooldowns
    assert "ETH-USD" not in cooldowns


def test_no_cooldown_returns_zero_remaining(state):
    assert state.cooldown_remaining("BTC-USD") == 0


# ── Day rollover ──────────────────────────────────────────────────────────────

def test_day_rollover_resets_counters(state):
    state._daily_trades   = 5
    state._daily_pnl      = 250.0
    state._last_save_date = "2020-01-01"   # old date — forces rollover
    rolled = state.check_day_rollover()
    assert rolled is True
    assert state.daily_trades == 0
    assert state.daily_pnl    == 0.0


def test_no_rollover_same_day(state):
    from datetime import date
    state._last_save_date = date.today().isoformat()
    state._daily_trades   = 3
    rolled = state.check_day_rollover()
    assert rolled is False
    assert state.daily_trades == 3


def test_day_rollover_purges_expired_cooldowns(state):
    state._cooldowns["BTC-USD"] = datetime.now() - timedelta(hours=2)
    state._last_save_date = "2020-01-01"
    state.check_day_rollover()
    assert "BTC-USD" not in state._cooldowns


# ── Performance ───────────────────────────────────────────────────────────────

def test_get_performance_empty(state):
    perf = state.get_performance()
    assert isinstance(perf, dict)
    assert "balance" in perf


def test_get_performance_after_trades(state):
    state.add_position(_make_position("t1", "BTC-USD"))
    state.close_position("t1", 51000, "TP", 100.0)
    perf = state.get_performance()
    assert perf["balance"] == pytest.approx(1100.0)