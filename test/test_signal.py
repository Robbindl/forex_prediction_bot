"""Tests for Signal dataclass."""
import pytest
from core.signal import Signal


def test_signal_default_alive():
    s = Signal(asset="EUR/USD", direction="BUY", category="forex", confidence=0.7)
    assert s.alive is True
    assert s.kill_reason == ""


def test_signal_kill():
    s = Signal(asset="BTC-USD", direction="BUY", category="crypto", confidence=0.8)
    s.kill("test reason", layer=3)
    assert s.alive is False
    assert s.kill_reason == "test reason"
    assert s.layer_reached == 3


def test_signal_boost_capped_at_one():
    s = Signal(asset="BTC-USD", direction="BUY", category="crypto", confidence=0.95)
    s.boost(0.5)
    assert s.confidence == 1.0


def test_signal_reduce_floored_at_zero():
    s = Signal(asset="BTC-USD", direction="BUY", category="crypto", confidence=0.05)
    s.reduce(0.5)
    assert s.confidence == 0.0


def test_signal_to_dict_roundtrip():
    s = Signal(
        asset="EUR/USD", direction="SELL", category="forex",
        confidence=0.72, entry_price=1.1050, stop_loss=1.1100,
        take_profit=1.0950, strategy_id="RSI",
    )
    d  = s.to_dict()
    s2 = Signal.from_dict(d)
    assert s2.asset       == s.asset
    assert s2.direction   == s.direction
    assert s2.confidence  == pytest.approx(s.confidence, rel=0.001)
    assert s2.strategy_id == s.strategy_id


def test_signal_risk_reward_zero_on_init():
    s = Signal(asset="EUR/USD", direction="BUY", category="forex", confidence=0.7)
    assert s.risk_reward == 0.0


def test_signal_metadata_empty_on_init():
    s = Signal(asset="EUR/USD", direction="BUY", category="forex", confidence=0.7)
    assert s.metadata == {}


def test_signal_kill_is_permanent():
    s = Signal(asset="BTC-USD", direction="BUY", category="crypto", confidence=0.8)
    s.kill("killed", layer=2)
    s.boost(0.5)           # boost should not revive it
    assert s.alive is False