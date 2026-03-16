"""
tests/test_strategy_lab.py — Strategy Laboratory tests.

  Unit tests        — always run, no external services required.
  Integration tests — skipped automatically when DataFetcher has no data.

Run just unit tests:
    pytest tests/test_strategy_lab.py -v -m "not integration"

Run everything (requires live market data APIs):
    pytest tests/test_strategy_lab.py -v
"""
from __future__ import annotations

import math
import pytest
import numpy as np
import pandas as pd


# ── Fixtures ──────────────────────────────────────────────────────────────────

def _make_ohlcv(n: int = 200, trend: str = "up",
                seed: int = 42) -> pd.DataFrame:
    """
    Generate synthetic OHLCV data.
    trend: "up" | "down" | "flat" | "volatile"
    """
    np.random.seed(seed)
    price = 100.0
    rows  = []
    for i in range(n):
        if trend == "up":
            drift = 0.002
        elif trend == "down":
            drift = -0.002
        elif trend == "volatile":
            drift = np.random.choice([-0.01, 0.01])
        else:
            drift = 0.0

        change = np.random.normal(drift, 0.012)
        open_  = price
        close  = price * (1 + change)
        high   = max(open_, close) * (1 + abs(np.random.normal(0, 0.005)))
        low    = min(open_, close) * (1 - abs(np.random.normal(0, 0.005)))
        vol    = np.random.uniform(1000, 5000)
        rows.append({
            "open": round(open_, 4), "high": round(high, 4),
            "low":  round(low,  4),  "close": round(close, 4),
            "volume": round(vol, 2),
        })
        price = close

    return pd.DataFrame(rows)


# ── StrategyBuilder tests ─────────────────────────────────────────────────────

class TestStrategyBuilder:

    def test_from_dict_creates_strategy(self):
        from strategy_lab.strategy_builder import StrategyBuilder
        config   = StrategyBuilder.example_config()
        strategy = StrategyBuilder.from_dict(config)
        assert strategy.name    == "ema_rsi_crossover"
        assert strategy.version == "1.0"

    def test_missing_entry_rules_raises(self):
        from strategy_lab.strategy_builder import StrategyBuilder
        with pytest.raises(ValueError, match="entry_rules"):
            StrategyBuilder.from_dict({"name": "bad_config"})

    def test_generate_returns_none_on_short_data(self):
        from strategy_lab.strategy_builder import StrategyBuilder
        strategy = StrategyBuilder.from_dict(StrategyBuilder.example_config())
        df       = _make_ohlcv(n=10)
        result   = strategy.generate(df)
        assert result is None

    def test_generate_returns_signal_or_none(self):
        from strategy_lab.strategy_builder import StrategyBuilder
        strategy = StrategyBuilder.from_dict(StrategyBuilder.example_config())
        df       = _make_ohlcv(n=200, trend="up")
        result   = strategy.generate(df)
        # May or may not trigger on this data — just check structure if it does
        if result is not None:
            assert "direction"   in result
            assert "confidence"  in result
            assert "entry_price" in result
            assert "stop_loss"   in result
            assert "take_profit" in result
            assert result["direction"] in ("BUY", "SELL")
            assert 0.0 <= result["confidence"] <= 1.0

    def test_rsi_indicator_added_to_df(self):
        from strategy_lab.strategy_builder import DynamicStrategy
        strategy = DynamicStrategy({
            "name": "test", "version": "1.0",
            "indicators": [{"name": "rsi", "params": {"period": 14}}],
            "entry_rules": [{"col": "rsi", "op": "<", "val": 30, "direction": "BUY"}],
        })
        df = _make_ohlcv(n=100)
        df = strategy._add_indicators(df)
        assert "rsi" in df.columns
        assert not df["rsi"].iloc[-1] != df["rsi"].iloc[-1]  # not NaN

    def test_atr_indicator_added_to_df(self):
        from strategy_lab.strategy_builder import DynamicStrategy
        strategy = DynamicStrategy({
            "name": "test", "version": "1.0",
            "indicators": [{"name": "atr", "params": {"period": 14}}],
            "entry_rules": [{"col": "close", "op": ">", "val": 0}],
        })
        df = _make_ohlcv(n=100)
        df = strategy._add_indicators(df)
        assert "atr" in df.columns

    def test_confidence_boost_applied(self):
        from strategy_lab.strategy_builder import DynamicStrategy
        strategy = DynamicStrategy({
            "name": "test", "version": "1.0",
            "indicators": [{"name": "rsi", "params": {"period": 14}}],
            "entry_rules": [{"col": "rsi", "op": "<", "val": 80, "direction": "BUY"}],
            "base_confidence": 0.65,
            "confidence_boosts": [{"col": "rsi", "above": 30, "boost": 0.10}],
        })
        df    = _make_ohlcv(n=100)
        df    = strategy._add_indicators(df)
        # RSI on uptrending data is usually > 30
        conf  = strategy._calc_confidence(df)
        assert conf >= 0.65  # at minimum base

    def test_cross_above_op_detected(self):
        """Manually construct a crossover and verify the rule fires."""
        from strategy_lab.strategy_builder import DynamicStrategy
        strategy = DynamicStrategy({
            "name": "cross_test", "version": "1.0",
            "indicators": [],
            "entry_rules": [
                {"col": "fast", "op": "cross_above",
                 "col2": "slow", "direction": "BUY"}
            ],
        })
        # Construct df where fast crosses above slow on last bar
        df = pd.DataFrame({
            "open":   [100, 100, 100],
            "high":   [101, 101, 102],
            "low":    [99, 99, 99],
            "close":  [100, 100, 101],
            "volume": [1000, 1000, 1000],
            "fast":   [9.0, 9.5, 10.5],   # crosses above slow on last bar
            "slow":   [10.0, 10.0, 10.0],
        })
        entry = strategy._evaluate_rules(df)
        assert entry is not None
        assert entry["direction"] == "BUY"

    def test_all_preset_configs_are_valid(self):
        from strategy_lab.strategy_builder import StrategyBuilder
        configs = [
            StrategyBuilder.example_config(),
            StrategyBuilder.rsi_mean_reversion_config(),
            StrategyBuilder.macd_trend_config(),
        ]
        for config in configs:
            s = StrategyBuilder.from_dict(config)
            assert s.name
            assert s._config.get("entry_rules")


# ── BacktestEngineV2 tests ────────────────────────────────────────────────────

class TestBacktestEngineV2:

    def test_returns_backtest_result(self):
        from strategy_lab.strategy_builder  import StrategyBuilder
        from strategy_lab.backtest_engine_v2 import BacktestEngineV2
        strategy = StrategyBuilder.from_dict(StrategyBuilder.example_config())
        engine   = BacktestEngineV2(strategy=strategy, initial_balance=10_000)
        result   = engine.run(_make_ohlcv(n=200, trend="up"))
        assert result is not None
        assert result.initial_balance == 10_000

    def test_empty_result_on_insufficient_data(self):
        from strategy_lab.strategy_builder  import StrategyBuilder
        from strategy_lab.backtest_engine_v2 import BacktestEngineV2
        strategy = StrategyBuilder.from_dict(StrategyBuilder.example_config())
        engine   = BacktestEngineV2(strategy=strategy)
        result   = engine.run(_make_ohlcv(n=10))
        assert result.total_trades == 0

    def test_balance_never_goes_negative(self):
        from strategy_lab.strategy_builder  import StrategyBuilder
        from strategy_lab.backtest_engine_v2 import BacktestEngineV2
        strategy = StrategyBuilder.from_dict(
            StrategyBuilder.rsi_mean_reversion_config()
        )
        engine = BacktestEngineV2(strategy=strategy, initial_balance=1_000)
        result = engine.run(_make_ohlcv(n=300, trend="volatile"))
        assert result.final_balance >= 0

    def test_equity_curve_starts_at_initial_balance(self):
        from strategy_lab.strategy_builder  import StrategyBuilder
        from strategy_lab.backtest_engine_v2 import BacktestEngineV2
        strategy = StrategyBuilder.from_dict(StrategyBuilder.example_config())
        engine   = BacktestEngineV2(strategy=strategy, initial_balance=5_000)
        result   = engine.run(_make_ohlcv(n=200))
        assert result.equity_curve[0] == 5_000

    def test_win_rate_between_zero_and_one(self):
        from strategy_lab.strategy_builder  import StrategyBuilder
        from strategy_lab.backtest_engine_v2 import BacktestEngineV2
        strategy = StrategyBuilder.from_dict(StrategyBuilder.example_config())
        engine   = BacktestEngineV2(strategy=strategy)
        result   = engine.run(_make_ohlcv(n=300, trend="up"))
        assert 0.0 <= result.win_rate <= 1.0

    def test_to_dict_has_required_keys(self):
        from strategy_lab.strategy_builder  import StrategyBuilder
        from strategy_lab.backtest_engine_v2 import BacktestEngineV2
        strategy = StrategyBuilder.from_dict(StrategyBuilder.example_config())
        engine   = BacktestEngineV2(strategy=strategy)
        result   = engine.run(_make_ohlcv(n=200))
        d        = result.to_dict()
        for key in ["initial_balance", "final_balance", "total_trades",
                    "win_rate", "total_pnl", "sharpe_ratio", "max_drawdown"]:
            assert key in d, f"Missing key: {key}"

    def test_summary_returns_string(self):
        from strategy_lab.strategy_builder  import StrategyBuilder
        from strategy_lab.backtest_engine_v2 import BacktestEngineV2
        strategy = StrategyBuilder.from_dict(StrategyBuilder.example_config())
        engine   = BacktestEngineV2(strategy=strategy)
        result   = engine.run(_make_ohlcv(n=200))
        s        = result.summary()
        assert isinstance(s, str)
        assert "Trades=" in s


# ── ParameterOptimizer tests ──────────────────────────────────────────────────

class TestParameterOptimizer:

    def test_grid_search_returns_sorted_results(self):
        from strategy_lab.strategy_builder    import StrategyBuilder
        from strategy_lab.parameter_optimizer import ParameterOptimizer
        df        = _make_ohlcv(n=300, trend="up")
        optimizer = ParameterOptimizer(
            base_config=StrategyBuilder.example_config(),
            df=df,
        )
        results = optimizer.grid_search({
            "rsi_period": [10, 14],
            "stop_mult":  [1.0, 2.0],
        })
        assert len(results) == 4   # 2 × 2 combinations
        # Results should be sorted by Sharpe (best first)
        sharpes = [r["sharpe"] for r in results]
        assert sharpes == sorted(sharpes, reverse=True)

    def test_random_search_returns_at_most_n_samples(self):
        from strategy_lab.strategy_builder    import StrategyBuilder
        from strategy_lab.parameter_optimizer import ParameterOptimizer
        df        = _make_ohlcv(n=300, trend="up")
        optimizer = ParameterOptimizer(
            base_config=StrategyBuilder.example_config(),
            df=df,
        )
        results = optimizer.random_search(
            {"rsi_period": [10, 14, 21], "stop_mult": [1.0, 1.5, 2.0]},
            n_samples=5,
        )
        assert len(results) <= 5

    def test_top_n_returns_correct_count(self):
        from strategy_lab.strategy_builder    import StrategyBuilder
        from strategy_lab.parameter_optimizer import ParameterOptimizer
        df        = _make_ohlcv(n=300)
        optimizer = ParameterOptimizer(
            base_config=StrategyBuilder.example_config(),
            df=df,
        )
        results = optimizer.grid_search({
            "rsi_period": [10, 14, 21],
        })
        top3 = optimizer.top_n(results, n=3)
        assert len(top3) <= 3

    def test_result_contains_param_values(self):
        from strategy_lab.strategy_builder    import StrategyBuilder
        from strategy_lab.parameter_optimizer import ParameterOptimizer
        df        = _make_ohlcv(n=200)
        optimizer = ParameterOptimizer(
            base_config=StrategyBuilder.example_config(),
            df=df,
        )
        results = optimizer.grid_search({"stop_mult": [1.5, 2.0]})
        for r in results:
            assert "stop_mult" in r
            assert r["stop_mult"] in (1.5, 2.0)


# ── PerformanceAnalyzer tests ─────────────────────────────────────────────────

class TestPerformanceAnalyzer:

    def _make_trades(self, pnls):
        return [{"pnl": p, "duration": 5,
                 "direction": "BUY", "entry_price": 100, "exit_price": 100,
                 "entry_bar": i, "exit_bar": i + 5,
                 "outcome": "take_profit" if p > 0 else "stop_loss"}
                for i, p in enumerate(pnls)]

    def test_win_rate_correct(self):
        from strategy_lab.performance_analyzer import PerformanceAnalyzer
        analyzer = PerformanceAnalyzer()
        trades   = self._make_trades([10, -5, 20, -3, 15])  # 3 wins, 2 losses
        curve    = [10000, 10010, 10005, 10025, 10022, 10037]
        result   = analyzer.compute(trades, curve, 10000, 10037)
        assert result.win_rate == pytest.approx(0.6, abs=0.01)

    def test_max_drawdown_correct(self):
        from strategy_lab.performance_analyzer import PerformanceAnalyzer
        analyzer = PerformanceAnalyzer()
        # Peak 1000 → trough 800 = 20% drawdown
        curve  = [1000, 1050, 1100, 900, 800, 850, 950]
        trades = self._make_trades([50, 50, -200, -100, 50, 100])
        result = analyzer.compute(trades, curve, 1000, 950)
        assert result.max_drawdown == pytest.approx(0.2727, abs=0.01)

    def test_profit_factor_correct(self):
        from strategy_lab.performance_analyzer import PerformanceAnalyzer
        analyzer = PerformanceAnalyzer()
        # Gross profit = 30, gross loss = 10 → PF = 3.0
        trades = self._make_trades([10, 20, -5, -5])
        curve  = [1000, 1010, 1030, 1025, 1020]
        result = analyzer.compute(trades, curve, 1000, 1020)
        assert result.profit_factor == pytest.approx(3.0, abs=0.01)

    def test_zero_trades_returns_safe_result(self):
        from strategy_lab.performance_analyzer import PerformanceAnalyzer
        analyzer = PerformanceAnalyzer()
        result   = analyzer.compute([], [10000], 10000, 10000)
        assert result.total_trades    == 0
        assert result.win_rate        == 0.0
        assert result.sharpe_ratio    == 0.0
        assert result.max_drawdown    == 0.0

    def test_compare_ranks_by_sharpe(self):
        from strategy_lab.performance_analyzer import PerformanceAnalyzer
        from strategy_lab.strategy_builder     import StrategyBuilder
        from strategy_lab.backtest_engine_v2   import BacktestEngineV2
        analyzer = PerformanceAnalyzer()

        s1 = StrategyBuilder.from_dict(StrategyBuilder.example_config())
        s2 = StrategyBuilder.from_dict(StrategyBuilder.rsi_mean_reversion_config())

        e1 = BacktestEngineV2(s1)
        e2 = BacktestEngineV2(s2)

        r1 = e1.run(_make_ohlcv(n=300, trend="up"))
        r2 = e2.run(_make_ohlcv(n=300, trend="up"))

        ranked = analyzer.compare([r1, r2], labels=["EMA_RSI", "RSI_MR"])
        assert len(ranked) == 2
        assert ranked[0]["sharpe"] >= ranked[1]["sharpe"]

    def test_extended_stats_returns_all_keys(self):
        from strategy_lab.performance_analyzer import PerformanceAnalyzer
        from strategy_lab.strategy_builder     import StrategyBuilder
        from strategy_lab.backtest_engine_v2   import BacktestEngineV2

        analyzer = PerformanceAnalyzer()
        strategy = StrategyBuilder.from_dict(StrategyBuilder.example_config())
        engine   = BacktestEngineV2(strategy)
        result   = engine.run(_make_ohlcv(n=300, trend="up"))
        ext      = analyzer.extended_stats(result)

        for key in ["sortino_ratio", "avg_duration",
                    "consecutive_wins", "consecutive_losses"]:
            assert key in ext


# ── Integration tests (require live DataFetcher) ──────────────────────────────

@pytest.mark.integration
class TestStrategyLabIntegration:

    def test_run_backtest_on_real_data(self):
        """Fetch real BTC data and run a full backtest end-to-end."""
        try:
            from data.fetcher import DataFetcher
            fetcher = DataFetcher()
            df      = fetcher.get_ohlcv("BTC-USD", "crypto", "1d", 300)
            if df is None or df.empty:
                pytest.skip("No market data available")
        except Exception:
            pytest.skip("DataFetcher not available")

        from strategy_lab import run_backtest, StrategyBuilder
        result = run_backtest(
            strategy_config=StrategyBuilder.example_config(),
            asset="BTC-USD",
            category="crypto",
            initial_balance=10_000,
        )
        assert result is not None
        assert result.total_pnl_pct > -1.0   # didn't lose everything
        print(f"\nBTC Backtest: {result.summary()}")
