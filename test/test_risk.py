"""Tests for risk manager and position sizer."""
import pytest
from risk.manager import RiskManager, DailyLossGuard
from risk.position_sizer import PositionSizer


def test_position_size_zero_on_no_stop():
    sizer = PositionSizer(account_balance=10000)
    assert sizer.calculate(entry_price=100, stop_loss=100) == 0.0


def test_position_size_zero_on_zero_entry():
    sizer = PositionSizer(account_balance=10000)
    assert sizer.calculate(entry_price=0, stop_loss=95) == 0.0


def test_position_size_scales_with_balance():
    s1 = PositionSizer(1000).calculate(100, 95, "forex")
    s2 = PositionSizer(2000).calculate(100, 95, "forex")
    assert s2 == pytest.approx(s1 * 2, rel=0.01)


def test_position_size_crypto_cap():
    sizer = PositionSizer(account_balance=1000)
    size = sizer.calculate(50000, 49000, "crypto", confidence=0.9)
    assert size > 0
    assert size * 50000 <= 1000 * 0.5


def test_daily_loss_guard_blocks_trading():
    guard = DailyLossGuard(balance=1000, limit_pct=5.0)
    can_trade, msg = guard.check(daily_pnl=-60.0)
    assert can_trade is False
    assert "limit" in msg.lower()


def test_daily_loss_guard_allows_under_limit():
    guard = DailyLossGuard(balance=1000, limit_pct=5.0)
    can_trade, _ = guard.check(daily_pnl=-30.0)
    assert can_trade is True


def test_risk_manager_rejects_low_confidence():
    rm = RiskManager(account_balance=1000)
    allowed, reason = rm.validate_signal(confidence=0.3, daily_pnl=0)
    assert allowed is False


def test_risk_manager_passes_good_signal():
    rm = RiskManager(account_balance=1000)
    allowed, _ = rm.validate_signal(confidence=0.75, daily_pnl=0)
    assert allowed is True


def test_stop_loss_buy_is_below_entry():
    rm = RiskManager(1000)
    sl = rm.get_stop_loss(entry=100, direction="BUY", category="forex", atr=1.0)
    assert sl < 100


def test_stop_loss_sell_is_above_entry():
    rm = RiskManager(1000)
    sl = rm.get_stop_loss(entry=100, direction="SELL", category="forex", atr=1.0)
    assert sl > 100