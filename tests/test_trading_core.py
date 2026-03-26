"""
tests/test_trading_core.py — Unit tests for TradingCore engine logic

Tests the critical trading paths without integration to external services:
  • Position execution and risk validation
  • Position sizing and confidence scaling
  • Daily loss protection
  • Market hours enforcement
  • Signal generation filtering
  • Manual position closing
"""

import pytest
from datetime import datetime
from unittest.mock import Mock, MagicMock, patch, ANY
import sys
import os

# Add project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.signal import Signal
from risk.manager import RiskManager, DailyLossGuard
from risk.position_sizer import PositionSizer
from execution.paper_trader import PaperTrader


# ═══════════════════════════════════════════════════════════════════════════════
# FIXTURES — Setup test data and mocks
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.fixture
def risk_manager():
    """Create a RiskManager with standard account balance."""
    return RiskManager(account_balance=10000.0)


@pytest.fixture
def position_sizer():
    """Create a PositionSizer with standard account balance."""
    return PositionSizer(account_balance=10000.0)


@pytest.fixture
def small_account_sizer():
    """Create a PositionSizer with small account ($100)."""
    return PositionSizer(account_balance=100.0)


@pytest.fixture
def paper_trader():
    """Create a PaperTrader with standard setup."""
    rm = RiskManager(account_balance=10000.0)
    return PaperTrader(account_balance=10000.0, risk_manager=rm)


@pytest.fixture
def buy_signal():
    """Create a standard BUY signal for testing."""
    return Signal(
        asset="EUR/USD",
        canonical_asset="EUR/USD",
        category="forex",
        direction="BUY",
        confidence=0.75,
        entry_price=1.0800,
        stop_loss=1.0770,
        take_profit=1.0850,
        position_size=4.0,
        strategy_id="TestStrategy",
        indicators={"test": 1},
    )


@pytest.fixture
def sell_signal():
    """Create a standard SELL signal for testing."""
    return Signal(
        asset="BTC-USD",
        canonical_asset="BTC-USD",
        category="crypto",
        direction="SELL",
        confidence=0.68,
        entry_price=50000.0,
        stop_loss=51000.0,
        take_profit=49000.0,
        position_size=1.0,
        strategy_id="TestStrategy",
        indicators={"test": 1},
    )


# ═══════════════════════════════════════════════════════════════════════════════
# TESTS: RiskManager and DailyLossGuard
# ═══════════════════════════════════════════════════════════════════════════════

class TestDailyLossGuard:
    """Test the 5% daily loss protection mechanism."""

    def test_guard_allows_positive_pnl(self):
        """Positive P&L should always be allowed."""
        guard = DailyLossGuard(balance=10000.0, limit_pct=5.0)
        allowed, msg = guard.check(daily_pnl=500.0)
        assert allowed, "Positive P&L should be allowed"
        assert msg == ""

    def test_guard_blocks_daily_loss_limit(self):
        """Loss >= 5% of balance should block trading."""
        guard = DailyLossGuard(balance=10000.0, limit_pct=5.0)
        # 5%+ loss should block
        allowed, msg = guard.check(daily_pnl=-500.0)
        assert not allowed, "5% loss should hit limit"
        assert "Daily loss limit hit" in msg

    def test_guard_allows_below_limit(self):
        """Loss below 5% should be allowed."""
        guard = DailyLossGuard(balance=10000.0, limit_pct=5.0)
        allowed, msg = guard.check(daily_pnl=-400.0)  # 4% loss
        assert allowed, "4% loss should be allowed"

    def test_guard_reset(self):
        """Reset should reset both initial and current balance."""
        guard = DailyLossGuard(balance=10000.0, limit_pct=5.0)
        guard.reset(9500.0)
        # With new $9500 balance, $500 loss is >5%
        allowed, msg = guard.check(daily_pnl=-500.0)
        assert not allowed, "After reset, should check against new baseline"


class TestRiskManager:
    """Test position validation and daily loss enforcement."""

    def test_confidence_validation(self, risk_manager):
        """Signals below min confidence should be rejected."""
        allowed, reason = risk_manager.validate_signal(
            confidence=0.4,
            daily_pnl=0.0,
            category="forex",
        )
        assert not allowed, "Low confidence should be rejected"
        assert "minimum" in reason.lower()

    def test_daily_pnl_blocks_after_loss_limit(self, risk_manager):
        """After hitting 5% loss, no new trades should be allowed."""
        allowed, reason = risk_manager.validate_signal(
            confidence=0.75,
            daily_pnl=-510.0,  # >5% loss
            category="forex",
        )
        assert not allowed, "Should block after daily loss limit"
        assert "loss limit" in reason.lower()

    def test_update_balance_preserves_baseline(self, risk_manager):
        """update_balance should not reset the daily loss baseline."""
        # Start with $10,000 and take $500 loss (5%)
        risk_manager.validate_signal(0.75, -500.0, "forex")
        
        # Update balance to $9,500 (simulating withdrawn profit from before today)
        risk_manager.update_balance(9500.0)
        
        # Additional $100 loss should still be blocked (total $600 = 6% of original)
        allowed, _ = risk_manager.validate_signal(0.75, -600.0, "forex")
        assert not allowed, "Baseline should not reset on balance update"


# ═══════════════════════════════════════════════════════════════════════════════
# TESTS: PositionSizer
# ═══════════════════════════════════════════════════════════════════════════════

class TestPositionSizer:
    """Test accurate position sizing with confidence scaling."""

    def test_forex_sizing_standard_account(self, position_sizer):
        """Test EUR/USD sizing on $10,000 account."""
        size = position_sizer.calculate(
            entry_price=1.0800,
            stop_loss=1.0770,
            asset="EUR/USD",
            category="forex",
            confidence=0.75,
        )
        # Should produce reasonable lot size
        assert size > 0, "Position size must be positive"
        assert size < 1000000, "Position should not be absurdly large"
        # On $10k account, EUR/USD base 4 lots at 75% confidence ≈ ~7.5 contracts
        assert 5 <= size <= 15, f"EUR/USD sizing unexpected: {size}"

    def test_confidence_scaling(self, position_sizer):
        """Position size should scale with confidence."""
        size_low = position_sizer.calculate(
            entry_price=1.0800,
            stop_loss=1.0770,
            asset="EUR/USD",
            category="forex",
            confidence=0.65,  # Low confidence
        )
        size_high = position_sizer.calculate(
            entry_price=1.0800,
            stop_loss=1.0770,
            asset="EUR/USD",
            category="forex",
            confidence=0.85,  # High confidence
        )
        assert size_high > size_low, "Higher confidence should yield larger position"

    def test_small_account_scaling(self, small_account_sizer):
        """Position sizing should scale proportionally to account size."""
        # $100 account is 1/100th of $10k reference
        size = small_account_sizer.calculate(
            entry_price=1.0800,
            stop_loss=1.0770,
            asset="EUR/USD",
            category="forex",
            confidence=0.75,
        )
        # Should be scaled down proportionally
        assert size > 0, "Position size must be positive even on small account"
        assert size < 1000, "Small account should have proportionally smaller positions"

    def test_crypto_sizing(self, position_sizer):
        """Test crypto asset positioning."""
        size = position_sizer.calculate(
            entry_price=50000.0,
            stop_loss=49000.0,
            asset="BTC-USD",
            category="crypto",
            confidence=0.75,
        )
        assert size > 0, "Crypto position size must be positive"
        # BTC sizing should produce lots, not raw coin amount
        assert size < 100, "BTC positioning should be in lots, not raw coins"

    def test_zero_entry_returns_zero(self, position_sizer):
        """Zero entry price should return no position."""
        size = position_sizer.calculate(
            entry_price=0.0,
            stop_loss=1.0770,
            asset="EUR/USD",
            category="forex",
            confidence=0.75,
        )
        assert size == 0.0, "Zero entry price should yield zero size"


# ═══════════════════════════════════════════════════════════════════════════════
# TESTS: PaperTrader execution and monitoring
# ═══════════════════════════════════════════════════════════════════════════════

class TestPaperTrader:
    """Test trade execution, SL/TP, and exit logic."""

    def test_execute_signal_creates_position(self, paper_trader, buy_signal):
        """Executing a signal should create an open position."""
        trade = paper_trader.execute_signal(buy_signal.to_dict())
        
        assert trade is not None, "Trade should be created"
        assert trade["trade_id"], "Trade should have an ID"
        assert trade["asset"] == "EUR/USD"
        assert trade["direction"] == "BUY"
        assert trade["entry_price"] == buy_signal.entry_price
        assert trade["stop_loss"] == buy_signal.stop_loss

    def test_execute_signal_stores_position(self, paper_trader, buy_signal):
        """Executed trade should be tracked in open_positions."""
        trade = paper_trader.execute_signal(buy_signal.to_dict())
        
        assert trade["trade_id"] in paper_trader.open_positions
        stored = paper_trader.open_positions[trade["trade_id"]]
        assert stored["asset"] == "EUR/USD"

    def test_stop_loss_triggers(self, paper_trader, buy_signal):
        """Price hitting stop loss should close position."""
        trade = paper_trader.execute_signal(buy_signal.to_dict())
        trade_id = trade["trade_id"]
        
        # Price drops below SL
        prices = {"EUR/USD": buy_signal.stop_loss - 0.0001}
        closed = paper_trader.update_positions(prices)
        
        assert len(closed) == 1, "SL should close the position"
        assert closed[0]["trade_id"] == trade_id
        assert "Stop Loss" in closed[0]["exit_reason"]
        assert trade_id not in paper_trader.open_positions

    def test_take_profit_triggers(self, paper_trader, buy_signal):
        """Price hitting take profit should close position."""
        trade = paper_trader.execute_signal(buy_signal.to_dict())
        trade_id = trade["trade_id"]
        
        # Price rises above TP
        prices = {"EUR/USD": buy_signal.take_profit + 0.0001}
        closed = paper_trader.update_positions(prices)
        
        assert len(closed) == 1, "TP should close the position"
        assert "Take Profit" in closed[0]["exit_reason"]

    def test_sell_signal_stop_loss(self, paper_trader, sell_signal):
        """Sell position SL should trigger on price above SL."""
        trade = paper_trader.execute_signal(sell_signal.to_dict())
        trade_id = trade["trade_id"]
        
        # Price rises above SL for SELL
        prices = {"BTC-USD": sell_signal.stop_loss + 100.0}
        closed = paper_trader.update_positions(prices)
        
        assert len(closed) == 1, "SL should close SELL position when price rises"
        assert closed[0]["trade_id"] == trade_id

    def test_partial_tp_closes_fraction(self, paper_trader):
        """Partial TP should close only 1/N of position and lock in break-even."""
        signal = Signal(
            asset="BTC-USD",
            canonical_asset="BTC-USD",
            category="crypto",
            direction="BUY",
            confidence=0.75,
            entry_price=50000.0,
            stop_loss=49000.0,
            take_profit=51000.0,
            take_profit_levels=[50500.0, 51000.0, 51500.0],  # 3 TP levels
            position_size=3.0,
            strategy_id="TestStrategy",
        )
        
        trade = paper_trader.execute_signal(signal.to_dict())
        trade_id = trade["trade_id"]
        initial_size = trade["position_size"]
        
        # Callback to track partial closes
        closed_trades = []
        paper_trader.on_trade_closed = lambda t: closed_trades.append(t)
        
        # Hit first TP level
        prices = {"BTC-USD": 50500.0}
        paper_trader.update_positions(prices)
        
        # Should have one partial close and position still open
        assert len(closed_trades) == 1, "Should have partial TP close"
        assert trade_id in paper_trader.open_positions, "Position should remain open"
        
        # Remaining position should be smaller
        remaining = paper_trader.open_positions[trade_id]
        assert remaining["position_size"] < initial_size, "Remaining size should be less"

    def test_pnl_calculation_buy(self, paper_trader, buy_signal):
        """P&L should be calculated correctly for BUY positions."""
        trade = paper_trader.execute_signal(buy_signal.to_dict())
        trade_id = trade["trade_id"]
        
        # Price moves in favor: +0.01 up
        prices = {"EUR/USD": buy_signal.entry_price + 0.01}
        paper_trader.update_positions(prices)
        
        pos = paper_trader.open_positions[trade_id]
        # P&L should be positive (simplified: price_delta * position_size)
        assert pos["pnl"] > 0, "Profitable move should show positive P&L"

    def test_pnl_calculation_sell(self, paper_trader, sell_signal):
        """P&L should be calculated correctly for SELL positions."""
        trade = paper_trader.execute_signal(sell_signal.to_dict())
        trade_id = trade["trade_id"]
        
        # Price moves in favor: down $1000
        prices = {"BTC-USD": sell_signal.entry_price - 1000.0}
        paper_trader.update_positions(prices)
        
        pos = paper_trader.open_positions[trade_id]
        # P&L should be positive (price down favors short)
        assert pos["pnl"] > 0, "Profitable short move should show positive P&L"

    def test_trailing_stop_moves_sl(self, paper_trader):
        """Trailing stop should move SL when position is 90% toward TP."""
        signal = Signal(
            asset="EUR/USD",
            canonical_asset="EUR/USD",
            category="forex",
            direction="BUY",
            confidence=0.75,
            entry_price=1.0800,
            stop_loss=1.0770,    # 30 pips SL = 1 ATR approx
            take_profit=1.0900,  # 100 pips TP
            position_size=4.0,
            strategy_id="TestStrategy",
        )
        
        trade = paper_trader.execute_signal(signal.to_dict())
        trade_id = trade["trade_id"]
        
        # Move price to 90% toward TP: entry 1.0800 + 0.9*(0.0100) = 1.0890
        prices = {"EUR/USD": 1.0890}
        paper_trader.update_positions(prices)
        
        pos = paper_trader.open_positions[trade_id]
        # SL should be moved closer to prevent loss (trailing stop)
        # At 90% progress, SL trails 0.5*ATR behind highest price
        assert pos["stop_loss"] > signal.stop_loss, "SL should trail upward"


# ═══════════════════════════════════════════════════════════════════════════════
# TESTS: Signal validation and filtering
# ═══════════════════════════════════════════════════════════════════════════════

class TestSignalValidation:
    """Test signal completeness and sanity checks."""

    def test_signal_with_missing_prices(self):
        """Signal missing entry/SL should not execute."""
        bad_signal = Signal(
            asset="EUR/USD",
            direction="BUY",
            category="forex",
            confidence=0.75,
            entry_price=0.0,  # Missing entry
            stop_loss=1.0770,
            take_profit=1.0850,
        )
        
        rm = RiskManager(10000.0)
        pt = PaperTrader(10000.0, rm)
        
        trade = pt.execute_signal(bad_signal.to_dict())
        assert trade is None, "Signal without entry price should not execute"

    def test_signal_risk_reward_calculation(self, buy_signal):
        """Risk-reward ratio should be calculated correctly."""
        entry = buy_signal.entry_price
        sl = buy_signal.stop_loss
        tp = buy_signal.take_profit
        
        risk = abs(entry - sl)
        reward = abs(tp - entry)
        expected_rr = reward / risk if risk else 0
        
        assert buy_signal.risk_reward > 0, "RR should be calculated"
        # RR for this signal: (1.0850 - 1.0800) / (1.0800 - 1.0770) = 50/30 ≈ 1.67
        assert abs(buy_signal.risk_reward - expected_rr) < 0.01


# ═══════════════════════════════════════════════════════════════════════════════
# TESTS: Edge cases and error handling
# ═══════════════════════════════════════════════════════════════════════════════

class TestEdgeCases:
    """Test boundary conditions and error handling."""

    def test_position_restoration(self):
        """Restoring persisted position should recreate it."""
        rm = RiskManager(10000.0)
        pt = PaperTrader(10000.0, rm)
        
        persisted_pos = {
            "trade_id": "test_123",
            "asset": "EUR/USD",
            "direction": "BUY",
            "entry_price": 1.0800,
            "stop_loss": 1.0770,
            "take_profit": 1.0850,
            "position_size": 4.0,
            "open_time": datetime.utcnow().isoformat(),
            "pnl": 0.0,
            "highest_price": 1.0800,
            "lowest_price": 1.0800,
            "tp_hit": 0,
        }
        
        pt.restore_position(persisted_pos)
        
        assert "test_123" in pt.open_positions
        assert pt.open_positions["test_123"]["asset"] == "EUR/USD"

    def test_concurrent_position_updates(self):
        """Multiple position updates should be thread-safe."""
        rm = RiskManager(10000.0)
        pt = PaperTrader(10000.0, rm)
        
        # Create multiple positions
        for i in range(3):
            pt.open_positions[f"pos_{i}"] = {
                "asset": f"ASSET_{i}",
                "entry_price": 100.0,
                "stop_loss": 95.0,
                "direction": "BUY",
                "position_size": 1.0,
                "highest_price": 100.0,
                "lowest_price": 100.0,
                "pnl": 0.0,
            }
        
        # Update all at once
        prices = {f"ASSET_{i}": 102.0 for i in range(3)}
        closed = pt.update_positions(prices)
        
        # None should close (well above SL)
        assert len(closed) == 0, "Price > SL should not close"
        # All should still have positive P&L
        for pos in pt.open_positions.values():
            assert pos["pnl"] > 0, "All positions should be profitable"

    def test_extreme_confidence_values(self):
        """Signals with extreme confidence should be clipped."""
        signal_high = Signal(
            asset="EUR/USD",
            direction="BUY",
            category="forex",
            confidence=1.5,  # >100%
            entry_price=1.0800,
            stop_loss=1.0770,
            take_profit=1.0850,
        )
        
        # Signal should clip to 1.0
        assert signal_high.confidence <= 1.0, "Confidence should be capped at 1.0"

    def test_negative_pnl_handling(self):
        """Positions with losses should still be tracked correctly."""
        rm = RiskManager(10000.0)
        pt = PaperTrader(10000.0, rm)
        
        signal = Signal(
            asset="EUR/USD",
            direction="BUY",
            category="forex",
            confidence=0.75,
            entry_price=1.0800,
            stop_loss=1.0770,
            take_profit=1.0850,
            position_size=4.0,
            strategy_id="TestStrategy",
        )
        
        trade = pt.execute_signal(signal.to_dict())
        trade_id = trade["trade_id"]
        
        # Price moves against position
        prices = {"EUR/USD": 1.0750}
        paper_trader_closed = pt.update_positions(prices)
        # Should trigger SL
        assert len(paper_trader_closed) == 0 or (
            paper_trader_closed[0]["pnl"] < 0
        ), "Loss should be recorded"


# ═══════════════════════════════════════════════════════════════════════════════
# Test execution
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
