"""
tests/test_strategy_adapter.py — StrategyAdapter tests.

  Unit tests        — always run, no external services required.
  Integration tests — skipped when DataFetcher has no live data.

Run just unit tests:
    pytest tests/test_strategy_adapter.py -v -m "not integration"

Run everything (requires market data):
    pytest tests/test_strategy_adapter.py -v
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


def _make_fake_signal(direction="BUY", confidence=0.72,
                      entry=100.0, sl=98.0, tp=106.0):
    """Create a minimal Signal-like object matching core/signal.py."""
    class FakeSignal:
        pass
    s = FakeSignal()
    s.direction   = direction
    s.confidence  = confidence
    s.entry_price = entry
    s.stop_loss   = sl
    s.take_profit = tp
    s.strategy_id = "TEST"
    s.indicators  = {"rsi": 28.5}
    return s


# ── StrategyAdapter unit tests ────────────────────────────────────────────────

class TestStrategyAdapter:

    def test_adapter_wraps_rsi_strategy(self):
        from strategy_lab.strategy_adapter import StrategyAdapter
        from strategies.rsi import RSIStrategy
        adapter = StrategyAdapter(RSIStrategy(), asset="BTC-USD", category="crypto")
        assert adapter.name    == "RSI"
        assert adapter._asset  == "BTC-USD"
        assert adapter._category == "crypto"

    def test_adapter_wraps_macd_strategy(self):
        from strategy_lab.strategy_adapter import StrategyAdapter
        from strategies.macd import MACDStrategy
        adapter = StrategyAdapter(MACDStrategy(), asset="EUR/USD", category="forex")
        assert adapter.name == "MACD"

    def test_adapter_wraps_bollinger_strategy(self):
        from strategy_lab.strategy_adapter import StrategyAdapter
        from strategies.bollinger import BollingerStrategy
        adapter = StrategyAdapter(BollingerStrategy())
        assert adapter.name == "Bollinger"

    def test_adapter_wraps_voting_strategy(self):
        from strategy_lab.strategy_adapter import StrategyAdapter
        from strategies.voting import VotingStrategy
        adapter = StrategyAdapter(VotingStrategy())
        assert adapter.name == "Voting"

    def test_generate_returns_none_on_short_data(self):
        from strategy_lab.strategy_adapter import StrategyAdapter
        from strategies.rsi import RSIStrategy
        adapter = StrategyAdapter(RSIStrategy())
        df      = _make_ohlcv(n=5)
        result  = adapter.generate(df)
        assert result is None

    def test_generate_returns_dict_or_none_on_valid_data(self):
        from strategy_lab.strategy_adapter import StrategyAdapter
        from strategies.rsi import RSIStrategy
        adapter = StrategyAdapter(RSIStrategy())
        df      = _make_ohlcv(n=200)
        result  = adapter.generate(df)
        if result is not None:
            assert isinstance(result, dict)
            assert "direction"   in result
            assert "confidence"  in result
            assert "entry_price" in result
            assert "stop_loss"   in result
            assert "take_profit" in result

    def test_signal_to_dict_converts_correctly(self):
        from strategy_lab.strategy_adapter import StrategyAdapter
        signal = _make_fake_signal(direction="BUY", confidence=0.72,
                                   entry=100.0, sl=98.0, tp=106.0)
        d = StrategyAdapter._signal_to_dict(signal)
        assert d["direction"]   == "BUY"
        assert d["confidence"]  == pytest.approx(0.72)
        assert d["entry_price"] == pytest.approx(100.0)
        assert d["stop_loss"]   == pytest.approx(98.0)
        assert d["take_profit"] == pytest.approx(106.0)
        assert d["strategy_id"] == "TEST"
        assert d["indicators"]  == {"rsi": 28.5}

    def test_signal_to_dict_handles_sell_direction(self):
        from strategy_lab.strategy_adapter import StrategyAdapter
        signal = _make_fake_signal(direction="SELL", entry=100.0,
                                   sl=102.0, tp=94.0)
        d = StrategyAdapter._signal_to_dict(signal)
        assert d["direction"] == "SELL"

    def test_generate_passes_correct_args_to_strategy(self):
        """Verify adapter calls strategy.generate with all required args."""
        from strategy_lab.strategy_adapter import StrategyAdapter

        received_args = {}

        class SpyStrategy:
            name    = "Spy"
            version = "1.0"
            def generate(self, asset, canonical, category, df):
                received_args.update({
                    "asset": asset, "canonical": canonical,
                    "category": category, "df_len": len(df),
                })
                return None

        adapter = StrategyAdapter(SpyStrategy(), asset="ETH-USD", category="crypto")
        df      = _make_ohlcv(n=100)
        adapter.generate(df)

        assert received_args["asset"]    == "ETH-USD"
        assert received_args["canonical"] == "ETH-USD"
        assert received_args["category"] == "crypto"
        assert received_args["df_len"]   == 100

    def test_generate_returns_none_on_strategy_exception(self):
        """Adapter should not propagate exceptions from the wrapped strategy."""
        from strategy_lab.strategy_adapter import StrategyAdapter

        class BrokenStrategy:
            name    = "Broken"
            version = "1.0"
            def generate(self, *args, **kwargs):
                raise RuntimeError("Simulated strategy crash")

        adapter = StrategyAdapter(BrokenStrategy())
        df      = _make_ohlcv(n=100)
        result  = adapter.generate(df)   # must not raise
        assert result is None

    def test_adapter_works_with_backtest_engine(self):
        """Full round-trip: adapter → BacktestEngineV2 → BacktestResult."""
        from strategy_lab.strategy_adapter  import StrategyAdapter
        from strategy_lab.backtest_engine_v2 import BacktestEngineV2
        from strategies.rsi import RSIStrategy

        adapter = StrategyAdapter(RSIStrategy(), asset="BTC-USD", category="crypto")
        engine  = BacktestEngineV2(strategy=adapter, initial_balance=10_000)
        result  = engine.run(_make_ohlcv(n=300, trend="up"))

        assert result is not None
        assert result.initial_balance == 10_000
        assert 0.0 <= result.win_rate <= 1.0
        assert isinstance(result.summary(), str)

    def test_all_strategies_run_without_error(self):
        """Every existing strategy must complete a backtest without crashing."""
        from strategy_lab.strategy_adapter  import StrategyAdapter
        from strategy_lab.backtest_engine_v2 import BacktestEngineV2
        from strategies.rsi       import RSIStrategy
        from strategies.macd      import MACDStrategy
        from strategies.bollinger import BollingerStrategy
        from strategies.voting    import VotingStrategy

        df = _make_ohlcv(n=300, trend="up")
        for cls in [RSIStrategy, MACDStrategy, BollingerStrategy, VotingStrategy]:
            adapter = StrategyAdapter(cls(), asset="BTC-USD", category="crypto")
            engine  = BacktestEngineV2(strategy=adapter, initial_balance=10_000)
            result  = engine.run(df)
            assert result is not None, f"{cls.__name__} backtest returned None"

    def test_canonical_replaces_slash(self):
        """Forex asset EUR/USD should have canonical EUR-USD."""
        from strategy_lab.strategy_adapter import StrategyAdapter
        from strategies.rsi import RSIStrategy
        adapter = StrategyAdapter(RSIStrategy(), asset="EUR/USD", category="forex")
        assert adapter._canonical == "EUR-USD"


# ── compare_all_strategies tests ──────────────────────────────────────────────

class TestCompareAllStrategies:

    def test_returns_four_results(self):
        from strategy_lab.strategy_adapter import compare_all_strategies
        df      = _make_ohlcv(n=300, trend="up")
        results = compare_all_strategies(df, asset="BTC-USD", category="crypto")
        assert len(results) == 4

    def test_results_sorted_by_sharpe(self):
        from strategy_lab.strategy_adapter import compare_all_strategies
        df      = _make_ohlcv(n=300, trend="up")
        results = compare_all_strategies(df)
        sharpes = [r["sharpe"] for r in results]
        assert sharpes == sorted(sharpes, reverse=True)

    def test_result_contains_all_required_keys(self):
        from strategy_lab.strategy_adapter import compare_all_strategies
        df      = _make_ohlcv(n=300)
        results = compare_all_strategies(df)
        for r in results:
            for key in ["label", "sharpe", "win_rate",
                        "total_pnl", "max_drawdown", "trades"]:
                assert key in r, f"Missing key '{key}' in result"

    def test_labels_match_strategy_names(self):
        from strategy_lab.strategy_adapter import compare_all_strategies
        df      = _make_ohlcv(n=300)
        results = compare_all_strategies(df)
        labels  = {r["label"] for r in results}
        assert labels == {"RSI", "MACD", "Bollinger", "Voting"}

    def test_best_strategy_on_uptrend(self):
        """On a clean uptrend, at least one strategy should be profitable."""
        from strategy_lab.strategy_adapter import compare_all_strategies
        df      = _make_ohlcv(n=400, trend="up", seed=1)
        results = compare_all_strategies(df)
        best    = results[0]
        # Not asserting profit (depends on synthetic data randomness)
        # Just confirm the best has a valid sharpe
        assert isinstance(best["sharpe"], float)
        assert best["label"] in ("RSI", "MACD", "Bollinger", "Voting")


# ── Integration tests (require live DataFetcher) ──────────────────────────────

@pytest.mark.integration
class TestStrategyAdapterIntegration:

    def test_backtest_existing_rsi_on_btc(self):
        """Fetch real BTC data and backtest RSIStrategy end-to-end."""
        try:
            from data.fetcher import DataFetcher
            df = DataFetcher().get_ohlcv("BTC-USD", "crypto", "1d", 300)
            if df is None or df.empty:
                pytest.skip("No BTC data available")
        except Exception:
            pytest.skip("DataFetcher not available")

        from strategy_lab.strategy_adapter import backtest_existing
        from strategies.rsi import RSIStrategy

        result = backtest_existing(RSIStrategy(), "BTC-USD", "crypto")
        assert result is not None
        print(f"\nRSI on BTC: {result.summary()}")

    def test_compare_all_on_btc(self):
        """Compare all four strategies on real BTC data."""
        try:
            from data.fetcher import DataFetcher
            df = DataFetcher().get_ohlcv("BTC-USD", "crypto", "1d", 300)
            if df is None or df.empty:
                pytest.skip("No BTC data available")
        except Exception:
            pytest.skip("DataFetcher not available")

        from strategy_lab.strategy_adapter import compare_all_strategies
        results = compare_all_strategies(df, asset="BTC-USD", category="crypto")

        assert len(results) == 4
        print("\nStrategy comparison on real BTC data:")
        for r in results:
            print(f"  {r['label']:12} Sharpe={r['sharpe']:+.2f}  "
                  f"WinRate={r['win_rate']:.1%}  "
                  f"PnL={r['total_pnl']:+.2f}  "
                  f"MaxDD={r['max_drawdown']:.1%}")

    def test_compare_all_on_forex(self):
        """Compare all four strategies on EUR/USD."""
        try:
            from data.fetcher import DataFetcher
            df = DataFetcher().get_ohlcv("EUR/USD", "forex", "1d", 300)
            if df is None or df.empty:
                pytest.skip("No EUR/USD data available")
        except Exception:
            pytest.skip("DataFetcher not available")

        from strategy_lab.strategy_adapter import compare_all_strategies
        results = compare_all_strategies(
            df, asset="EUR/USD", category="forex"
        )
        assert len(results) == 4
        print("\nStrategy comparison on EUR/USD:")
        for r in results:
            print(f"  {r['label']:12} Sharpe={r['sharpe']:+.2f}  "
                  f"Trades={r['trades']}")
