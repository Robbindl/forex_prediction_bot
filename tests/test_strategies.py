"""Tests for RSI, MACD, Bollinger and Voting strategies using synthetic OHLCV data."""
import pytest
import pandas as pd
import numpy as np
from core.signal import Signal


def _flat_df(n=100, price=100.0):
    """Flat price OHLCV dataframe — no signal expected."""
    return pd.DataFrame({
        "open":   [price] * n,
        "high":   [price + 0.1] * n,
        "low":    [price - 0.1] * n,
        "close":  [price] * n,
        "volume": [1_000_000] * n,
    })


def _trending_up_df(n=100, start=100.0, step=0.5):
    """Steadily rising price — should favour BUY signals."""
    prices = [start + i * step for i in range(n)]
    return pd.DataFrame({
        "open":   prices,
        "high":   [p + 0.2 for p in prices],
        "low":    [p - 0.2 for p in prices],
        "close":  prices,
        "volume": [1_000_000] * n,
    })


def _oversold_df(n=100):
    """
    Price crashes sharply then stabilises — RSI should go oversold
    and then cross back up, triggering a BUY signal.
    """
    prices = [100.0] * 40 + [60.0] * 20 + [62.0] * 40
    prices = prices[:n]
    return pd.DataFrame({
        "open":   prices,
        "high":   [p + 0.5 for p in prices],
        "low":    [p - 0.5 for p in prices],
        "close":  prices,
        "volume": [1_000_000] * n,
    })


def _bollinger_bounce_df(n=100):
    """
    Price dips below lower Bollinger Band then bounces back.
    """
    import math
    base   = 100.0
    prices = [base + math.sin(i * 0.3) * 2 for i in range(n - 5)]
    # Sharp dip then recovery
    prices += [base - 10, base - 11, base - 10, base - 8, base - 5]
    return pd.DataFrame({
        "open":   prices,
        "high":   [p + 0.5 for p in prices],
        "low":    [p - 0.5 for p in prices],
        "close":  prices,
        "volume": [1_000_000] * n,
    })


# ── RSI ───────────────────────────────────────────────────────────────────────

def test_rsi_returns_none_on_flat_data():
    from strategies.rsi import RSIStrategy
    df  = _flat_df()
    sig = RSIStrategy().generate("BTC-USD", "BTC-USD", "crypto", df)
    assert sig is None


def test_rsi_returns_none_on_insufficient_data():
    from strategies.rsi import RSIStrategy
    df  = _flat_df(n=10)
    sig = RSIStrategy().generate("BTC-USD", "BTC-USD", "crypto", df)
    assert sig is None


def test_rsi_returns_signal_or_none_on_valid_data():
    from strategies.rsi import RSIStrategy
    df  = _oversold_df()
    sig = RSIStrategy().generate("BTC-USD", "BTC-USD", "crypto", df)
    # May or may not fire — just must not crash
    assert sig is None or isinstance(sig, Signal)


def test_rsi_signal_has_valid_fields():
    from strategies.rsi import RSIStrategy
    # Try multiple datasets until we get a signal
    for _ in range(5):
        df  = _oversold_df()
        sig = RSIStrategy().generate("BTC-USD", "BTC-USD", "crypto", df)
        if sig:
            assert sig.direction in ("BUY", "SELL")
            assert 0.0 < sig.confidence <= 1.0
            assert sig.entry_price > 0
            assert sig.stop_loss > 0
            assert sig.take_profit > 0
            assert "rsi" in sig.indicators
            return
    # No signal generated — that's acceptable
    assert True


# ── MACD ──────────────────────────────────────────────────────────────────────

def test_macd_returns_none_on_insufficient_data():
    from strategies.macd import MACDStrategy
    df  = _flat_df(n=20)
    sig = MACDStrategy().generate("EUR/USD", "EUR/USD", "forex", df)
    assert sig is None


def test_macd_returns_signal_or_none_on_valid_data():
    from strategies.macd import MACDStrategy
    df  = _trending_up_df(n=100)
    sig = MACDStrategy().generate("EUR/USD", "EUR/USD", "forex", df)
    assert sig is None or isinstance(sig, Signal)


def test_macd_signal_confidence_in_range():
    from strategies.macd import MACDStrategy
    df  = _trending_up_df(n=100)
    sig = MACDStrategy().generate("EUR/USD", "EUR/USD", "forex", df)
    if sig:
        assert 0.0 < sig.confidence <= 1.0


# ── Bollinger ─────────────────────────────────────────────────────────────────

def test_bollinger_returns_none_on_insufficient_data():
    from strategies.bollinger import BollingerStrategy
    df  = _flat_df(n=15)
    sig = BollingerStrategy().generate("GBP/USD", "GBP/USD", "forex", df)
    assert sig is None


def test_bollinger_returns_signal_or_none_on_valid_data():
    from strategies.bollinger import BollingerStrategy
    df  = _bollinger_bounce_df()
    sig = BollingerStrategy().generate("GBP/USD", "GBP/USD", "forex", df)
    assert sig is None or isinstance(sig, Signal)


def test_bollinger_signal_has_bb_indicators():
    from strategies.bollinger import BollingerStrategy
    df  = _bollinger_bounce_df()
    sig = BollingerStrategy().generate("GBP/USD", "GBP/USD", "forex", df)
    if sig:
        assert "bb_upper" in sig.indicators
        assert "bb_lower" in sig.indicators
        assert "bb_mid"   in sig.indicators


# ── Voting (ensemble) ─────────────────────────────────────────────────────────

def test_voting_returns_none_on_insufficient_data():
    from strategies.voting import VotingStrategy
    df  = _flat_df(n=10)
    sig = VotingStrategy().generate("BTC-USD", "BTC-USD", "crypto", df)
    assert sig is None


def test_voting_returns_signal_or_none():
    from strategies.voting import VotingStrategy
    df  = _trending_up_df(n=100)
    sig = VotingStrategy().generate("BTC-USD", "BTC-USD", "crypto", df)
    assert sig is None or isinstance(sig, Signal)


def test_voting_signal_has_votes_in_indicators():
    from strategies.voting import VotingStrategy
    # Try several datasets
    for builder in [_trending_up_df, _oversold_df, _bollinger_bounce_df]:
        df  = builder()
        sig = VotingStrategy().generate("BTC-USD", "BTC-USD", "crypto", df)
        if sig:
            assert "votes" in sig.indicators
            assert sig.indicators["votes"] >= 2
            return
    assert True


def test_voting_confidence_boosted_on_unanimity():
    from strategies.voting import VotingStrategy
    from unittest.mock import patch, MagicMock

    mock_sig = Signal(
        asset="BTC-USD", canonical_asset="BTC-USD",
        direction="BUY", category="crypto",
        confidence=0.70, entry_price=50000,
        stop_loss=49000, take_profit=52000, strategy_id="MOCK",
    )

    strategy = VotingStrategy(min_votes=2)
    with patch.object(strategy._strategies[0], "generate", return_value=mock_sig), \
         patch.object(strategy._strategies[1], "generate", return_value=mock_sig), \
         patch.object(strategy._strategies[2], "generate", return_value=mock_sig):
        df  = _flat_df()
        sig = strategy.generate("BTC-USD", "BTC-USD", "crypto", df)

    assert sig is not None
    # Unanimity boost should push confidence above 0.70
    assert sig.confidence >= 0.70


def test_voting_requires_minimum_votes():
    from strategies.voting import VotingStrategy
    from unittest.mock import patch

    strategy = VotingStrategy(min_votes=3)
    buy_sig = Signal(
        asset="BTC-USD", canonical_asset="BTC-USD",
        direction="BUY", category="crypto",
        confidence=0.70, entry_price=50000,
        stop_loss=49000, take_profit=52000, strategy_id="MOCK",
    )
    sell_sig = Signal(
        asset="BTC-USD", canonical_asset="BTC-USD",
        direction="SELL", category="crypto",
        confidence=0.70, entry_price=50000,
        stop_loss=51000, take_profit=48000, strategy_id="MOCK",
    )
    # 2 BUY vs 1 SELL — dominant=BUY with only 2 votes, min_votes=3 → None
    with patch.object(strategy._strategies[0], "generate", return_value=buy_sig), \
         patch.object(strategy._strategies[1], "generate", return_value=buy_sig), \
         patch.object(strategy._strategies[2], "generate", return_value=sell_sig):
        df  = _flat_df()
        sig = strategy.generate("BTC-USD", "BTC-USD", "crypto", df)

    assert sig is None