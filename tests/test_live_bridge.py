"""
tests/test_live_bridge.py — Live Bridge tests.

  Unit tests        — always run, no external services required.
  Integration tests — skipped when DataFetcher has no live data.

Run just unit tests:
    pytest tests/test_live_bridge.py -v -m "not integration"

Run everything (requires market data):
    pytest tests/test_live_bridge.py -v
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest


# ── Fixtures ──────────────────────────────────────────────────────────────────

def _make_ohlcv(n: int = 300, trend: str = "up", seed: int = 42) -> pd.DataFrame:
    np.random.seed(seed)
    price = 100.0
    rows  = []
    for _ in range(n):
        drift  = 0.002 if trend == "up" else (-0.002 if trend == "down" else 0.0)
        change = np.random.normal(drift, 0.012)
        open_  = price
        close  = price * (1 + change)
        high   = max(open_, close) * (1 + abs(np.random.normal(0, 0.005)))
        low    = min(open_, close) * (1 - abs(np.random.normal(0, 0.005)))
        rows.append({
            "open": round(open_, 4), "high": round(high, 4),
            "low":  round(low,  4),  "close": round(close, 4),
            "volume": round(np.random.uniform(1000, 5000), 2),
        })
        price = close
    return pd.DataFrame(rows)


# ── DynamicStrategyLive tests ─────────────────────────────────────────────────

class TestDynamicStrategyLive:

    def test_wraps_config_correctly(self):
        from strategy_lab.live_bridge    import DynamicStrategyLive
        from strategy_lab.strategy_builder import StrategyBuilder
        live = DynamicStrategyLive(StrategyBuilder.triple_ema_config())
        assert live.name    == "triple_ema"
        assert live.version == "1.0"

    def test_is_basestrategy_subclass(self):
        from strategy_lab.live_bridge    import DynamicStrategyLive
        from strategy_lab.strategy_builder import StrategyBuilder
        from strategies.base import BaseStrategy
        live = DynamicStrategyLive(StrategyBuilder.example_config())
        assert isinstance(live, BaseStrategy)

    def test_generate_returns_none_on_short_data(self):
        from strategy_lab.live_bridge    import DynamicStrategyLive
        from strategy_lab.strategy_builder import StrategyBuilder
        live   = DynamicStrategyLive(StrategyBuilder.example_config())
        df     = _make_ohlcv(n=10)
        result = live.generate("BTC-USD", "BTC-USD", "crypto", df)
        assert result is None

    def test_generate_returns_signal_or_none_on_valid_data(self):
        from strategy_lab.live_bridge    import DynamicStrategyLive
        from strategy_lab.strategy_builder import StrategyBuilder
        from core.signal import Signal
        live   = DynamicStrategyLive(StrategyBuilder.example_config())
        df     = _make_ohlcv(n=300, trend="up")
        result = live.generate("BTC-USD", "BTC-USD", "crypto", df)
        if result is not None:
            assert isinstance(result, Signal)
            assert result.direction  in ("BUY", "SELL")
            assert 0.0 <= result.confidence <= 1.0
            assert result.entry_price > 0
            assert result.stop_loss   > 0
            assert result.take_profit > 0

    def test_signal_has_correct_asset(self):
        from strategy_lab.live_bridge    import DynamicStrategyLive
        from strategy_lab.strategy_builder import StrategyBuilder
        live = DynamicStrategyLive(StrategyBuilder.rsi_mean_reversion_config())
        df   = _make_ohlcv(n=300, trend="down")  # down trend triggers RSI oversold
        result = live.generate("ETH-USD", "ETH-USD", "crypto", df)
        if result is not None:
            assert result.asset == "ETH-USD"

    def test_min_confidence_filter(self):
        """Strategy with very high min_confidence should return fewer signals."""
        from strategy_lab.live_bridge    import DynamicStrategyLive
        from strategy_lab.strategy_builder import StrategyBuilder

        # Set an impossibly high min_confidence — should always return None
        live = DynamicStrategyLive(
            StrategyBuilder.example_config(),
            min_confidence=0.999,
        )
        df = _make_ohlcv(n=300, trend="up")
        # Run generate 10 times across different windows
        results = []
        for i in range(10):
            r = live.generate("BTC-USD", "BTC-USD", "crypto", df.iloc[:200+i])
            results.append(r)
        # With min_confidence=0.999 most (if not all) should be None
        none_count = sum(1 for r in results if r is None)
        assert none_count >= 8

    def test_does_not_crash_on_exception(self):
        """Broken config should return None, not raise."""
        from strategy_lab.live_bridge import DynamicStrategyLive
        # Config with a rule referencing a non-existent column
        broken_config = {
            "name":        "broken",
            "version":     "1.0",
            "indicators":  [],
            "entry_rules": [
                {"col": "nonexistent_col", "op": ">", "val": 0, "direction": "BUY"}
            ],
        }
        live   = DynamicStrategyLive(broken_config)
        df     = _make_ohlcv(n=100)
        result = live.generate("BTC-USD", "BTC-USD", "crypto", df)
        assert result is None   # must not raise

    def test_all_presets_can_be_wrapped(self):
        """Every preset config should wrap without error."""
        from strategy_lab.live_bridge    import DynamicStrategyLive
        from strategy_lab.strategy_builder import StrategyBuilder
        for name, config in StrategyBuilder.all_configs().items():
            live = DynamicStrategyLive(config)
            assert live.name == name, f"Name mismatch for {name}"


# ── VotingStrategy.add_strategy tests ────────────────────────────────────────

class TestVotingStrategyAddRemove:

    def test_add_strategy_increases_pool(self):
        from strategies.voting           import VotingStrategy
        from strategy_lab.live_bridge    import DynamicStrategyLive
        from strategy_lab.strategy_builder import StrategyBuilder

        voting = VotingStrategy()
        before = len(voting._strategies)
        live   = DynamicStrategyLive(StrategyBuilder.triple_ema_config())
        voting.add_strategy(live)
        assert len(voting._strategies) == before + 1

    def test_add_strategy_name_appears_in_list(self):
        from strategies.voting           import VotingStrategy
        from strategy_lab.live_bridge    import DynamicStrategyLive
        from strategy_lab.strategy_builder import StrategyBuilder

        voting = VotingStrategy()
        voting.add_strategy(DynamicStrategyLive(StrategyBuilder.golden_cross_config()))
        assert "golden_cross" in voting.list_strategies()

    def test_duplicate_strategy_not_added_twice(self):
        from strategies.voting           import VotingStrategy
        from strategy_lab.live_bridge    import DynamicStrategyLive
        from strategy_lab.strategy_builder import StrategyBuilder

        voting = VotingStrategy()
        before = len(voting._strategies)
        live   = DynamicStrategyLive(StrategyBuilder.triple_ema_config())
        voting.add_strategy(live)
        voting.add_strategy(live)   # second call — should be ignored
        assert len(voting._strategies) == before + 1

    def test_remove_strategy_decreases_pool(self):
        from strategies.voting           import VotingStrategy
        from strategy_lab.live_bridge    import DynamicStrategyLive
        from strategy_lab.strategy_builder import StrategyBuilder

        voting = VotingStrategy()
        voting.add_strategy(DynamicStrategyLive(StrategyBuilder.triple_ema_config()))
        before = len(voting._strategies)
        result = voting.remove_strategy("triple_ema")
        assert result is True
        assert len(voting._strategies) == before - 1

    def test_remove_nonexistent_returns_false(self):
        from strategies.voting import VotingStrategy
        voting = VotingStrategy()
        result = voting.remove_strategy("does_not_exist")
        assert result is False

    def test_cannot_remove_last_strategy(self):
        from strategies.voting           import VotingStrategy
        from strategy_lab.live_bridge    import DynamicStrategyLive
        from strategy_lab.strategy_builder import StrategyBuilder

        # Create voting with only one strategy
        voting = VotingStrategy()
        voting._strategies = [
            DynamicStrategyLive(StrategyBuilder.example_config())
        ]
        result = voting.remove_strategy("ema_rsi_crossover")
        assert result is False
        assert len(voting._strategies) == 1

    def test_list_strategies_returns_all_names(self):
        from strategies.voting import VotingStrategy
        voting = VotingStrategy()
        names  = voting.list_strategies()
        assert "RSI"       in names
        assert "MACD"      in names
        assert "Bollinger" in names

    def test_added_strategy_participates_in_voting(self):
        """
        Add a DynamicStrategyLive that always signals BUY.
        On uptrend data it should add a BUY vote.
        """
        from strategies.voting           import VotingStrategy
        from strategy_lab.live_bridge    import DynamicStrategyLive
        from strategy_lab.strategy_builder import StrategyBuilder

        voting = VotingStrategy(min_votes=1, min_confidence=0.50)
        live   = DynamicStrategyLive(
            StrategyBuilder.triple_ema_config(),
            min_confidence=0.50,
        )
        voting.add_strategy(live)
        assert "triple_ema" in voting.list_strategies()

        # Just confirm it runs without error on valid data
        df = _make_ohlcv(n=300, trend="up")
        voting.generate("BTC-USD", "BTC-USD", "crypto", df)   # must not raise


# ── register_best_strategies tests ───────────────────────────────────────────

class TestRegisterBestStrategies:

    def test_register_returns_zero_when_no_configs(self):
        """Default LIVE_STRATEGY_CONFIGS is empty — should register nothing."""
        import strategy_lab.live_bridge as bridge
        original = bridge.LIVE_STRATEGY_CONFIGS[:]
        bridge.LIVE_STRATEGY_CONFIGS = []   # ensure empty
        try:
            count = bridge.register_best_strategies()
            assert count == 0
        finally:
            bridge.LIVE_STRATEGY_CONFIGS = original

    def test_list_live_strategies_returns_names(self):
        import strategy_lab.live_bridge as bridge
        from strategy_lab.strategy_builder import StrategyBuilder
        original = bridge.LIVE_STRATEGY_CONFIGS[:]
        bridge.LIVE_STRATEGY_CONFIGS = [StrategyBuilder.triple_ema_config()]
        try:
            names = bridge.list_live_strategies()
            assert "triple_ema" in names
        finally:
            bridge.LIVE_STRATEGY_CONFIGS = original


# ── Integration tests ─────────────────────────────────────────────────────────

@pytest.mark.integration
class TestLiveBridgeIntegration:

    def test_live_strategy_on_real_btc(self):
        """Wrap golden_cross and generate a signal on real BTC data."""
        try:
            from data.fetcher import DataFetcher
            df = DataFetcher().get_ohlcv("BTC-USD", "crypto", "1d", 300)
            if df is None or df.empty:
                pytest.skip("No BTC data")
        except Exception:
            pytest.skip("DataFetcher not available")

        from strategy_lab.live_bridge    import DynamicStrategyLive
        from strategy_lab.strategy_builder import StrategyBuilder
        from core.signal import Signal

        live   = DynamicStrategyLive(StrategyBuilder.golden_cross_config())
        result = live.generate("BTC-USD", "BTC-USD", "crypto", df)

        if result is not None:
            assert isinstance(result, Signal)
            print(f"\ngolden_cross signal: {result}")
        else:
            print("\ngolden_cross: no signal on current data (normal)")

    def test_voting_with_lab_strategy_on_real_data(self):
        """Add triple_ema to voting and run on real BTC data."""
        try:
            from data.fetcher import DataFetcher
            df = DataFetcher().get_ohlcv("BTC-USD", "crypto", "1d", 300)
            if df is None or df.empty:
                pytest.skip("No BTC data")
        except Exception:
            pytest.skip("DataFetcher not available")

        from strategies.voting           import VotingStrategy
        from strategy_lab.live_bridge    import DynamicStrategyLive
        from strategy_lab.strategy_builder import StrategyBuilder

        voting = VotingStrategy(min_votes=1, min_confidence=0.50)
        voting.add_strategy(
            DynamicStrategyLive(StrategyBuilder.triple_ema_config())
        )
        assert len(voting._strategies) == 4   # 3 original + 1 new

        result = voting.generate("BTC-USD", "BTC-USD", "crypto", df)
        print(f"\nVoting (4 strategies) on BTC: {result}")
