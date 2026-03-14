"""Tests for paper trader execution and SL/TP logic."""
import pytest
from execution.paper_trader import PaperTrader
from risk.manager import RiskManager


@pytest.fixture
def trader():
    return PaperTrader(
        account_balance=1000,
        risk_manager=RiskManager(1000),
    )


def test_execute_signal_returns_trade(trader):
    signal = {
        "asset": "BTC-USD", "canonical_asset": "BTC-USD",
        "category": "crypto", "direction": "BUY",
        "confidence": 0.8, "entry_price": 50000,
        "stop_loss": 49000, "take_profit": 52000,
        "position_size": 0.001, "strategy_id": "RSI",
    }
    trade = trader.execute_signal(signal)
    assert trade is not None
    assert trade["asset"] == "BTC-USD"
    assert trade["direction"] == "BUY"
    assert "trade_id" in trade


def test_execute_signal_rejects_zero_price(trader):
    signal = {
        "asset": "BTC-USD", "canonical_asset": "BTC-USD",
        "category": "crypto", "direction": "BUY",
        "confidence": 0.8, "entry_price": 0,
        "stop_loss": 0, "take_profit": 0,
        "position_size": 0.001, "strategy_id": "RSI",
    }
    trade = trader.execute_signal(signal)
    assert trade is None


def test_stop_loss_triggers_on_buy(trader):
    signal = {
        "asset": "ETH-USD", "canonical_asset": "ETH-USD",
        "category": "crypto", "direction": "BUY",
        "confidence": 0.8, "entry_price": 2000,
        "stop_loss": 1950, "take_profit": 2100,
        "position_size": 0.1, "strategy_id": "MACD",
    }
    trader.execute_signal(signal)
    closed = trader.update_positions({"ETH-USD": 1940})
    assert len(closed) == 1
    assert closed[0]["exit_reason"] == "Stop Loss"
    assert closed[0]["pnl"] < 0


def test_take_profit_triggers_on_buy(trader):
    signal = {
        "asset": "ETH-USD", "canonical_asset": "ETH-USD",
        "category": "crypto", "direction": "BUY",
        "confidence": 0.8, "entry_price": 2000,
        "stop_loss": 1950, "take_profit": 2100,
        "position_size": 0.1, "strategy_id": "MACD",
    }
    trader.execute_signal(signal)
    closed = trader.update_positions({"ETH-USD": 2110})
    assert len(closed) == 1
    assert "Take Profit" in closed[0]["exit_reason"]
    assert closed[0]["pnl"] > 0


def test_stop_loss_triggers_on_sell(trader):
    signal = {
        "asset": "SOL-USD", "canonical_asset": "SOL-USD",
        "category": "crypto", "direction": "SELL",
        "confidence": 0.75, "entry_price": 100,
        "stop_loss": 106, "take_profit": 90,
        "position_size": 1.0, "strategy_id": "Bollinger",
    }
    trader.execute_signal(signal)
    closed = trader.update_positions({"SOL-USD": 107})
    assert len(closed) == 1
    assert closed[0]["exit_reason"] == "Stop Loss"
    assert closed[0]["pnl"] < 0


def test_take_profit_triggers_on_sell(trader):
    signal = {
        "asset": "SOL-USD", "canonical_asset": "SOL-USD",
        "category": "crypto", "direction": "SELL",
        "confidence": 0.75, "entry_price": 100,
        "stop_loss": 106, "take_profit": 90,
        "position_size": 1.0, "strategy_id": "Bollinger",
    }
    trader.execute_signal(signal)
    closed = trader.update_positions({"SOL-USD": 89})
    assert len(closed) == 1
    assert "Take Profit" in closed[0]["exit_reason"]
    assert closed[0]["pnl"] > 0


def test_no_exit_while_price_in_range(trader):
    signal = {
        "asset": "BTC-USD", "canonical_asset": "BTC-USD",
        "category": "crypto", "direction": "BUY",
        "confidence": 0.8, "entry_price": 50000,
        "stop_loss": 49000, "take_profit": 52000,
        "position_size": 0.001, "strategy_id": "RSI",
    }
    trader.execute_signal(signal)
    closed = trader.update_positions({"BTC-USD": 50500})
    assert len(closed) == 0
    assert len(trader.open_positions) == 1


def test_on_trade_closed_callback_fires(trader):
    fired = []
    trader.on_trade_closed = lambda t: fired.append(t)
    signal = {
        "asset": "SOL-USD", "canonical_asset": "SOL-USD",
        "category": "crypto", "direction": "SELL",
        "confidence": 0.75, "entry_price": 100,
        "stop_loss": 106, "take_profit": 90,
        "position_size": 1.0, "strategy_id": "Bollinger",
    }
    trader.execute_signal(signal)
    trader.update_positions({"SOL-USD": 89})
    assert len(fired) == 1


def test_restore_position(trader):
    pos = {
        "trade_id": "test123", "asset": "BTC-USD",
        "canonical_asset": "BTC-USD", "category": "crypto",
        "direction": "BUY", "entry_price": 50000,
        "stop_loss": 49000, "take_profit": 52000,
        "position_size": 0.001, "strategy_id": "RSI",
        "confidence": 0.8, "open_time": "2025-01-01T00:00:00",
    }
    trader.restore_position(pos)
    assert "test123" in trader.open_positions