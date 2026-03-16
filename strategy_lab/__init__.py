"""
strategy_lab/__init__.py — Strategy Laboratory.

A professional strategy experimentation environment that lets you
build, backtest, optimise, and compare trading strategies without
touching the live trading engine.

Components
----------
    StrategyBuilder      — define strategies as config dicts, no new classes needed
    BacktestEngineV2     — run backtests on historical OHLCV data
    ParameterOptimizer   — grid-search optimal parameters
    PerformanceAnalyzer  — compute Sharpe, drawdown, win rate, expectancy

Quick start
-----------
    from strategy_lab import run_backtest, optimize_strategy

    # Run a single backtest
    result = run_backtest(
        strategy_config=StrategyBuilder.example_config(),
        asset="BTC-USD",
        category="crypto",
    )
    print(result.summary())

    # Optimise parameters
    best = optimize_strategy(
        base_config=StrategyBuilder.example_config(),
        param_grid={"rsi_period": [10, 14, 21], "ema_fast": [8, 12, 20]},
        asset="BTC-USD",
        category="crypto",
    )

Run tests
---------
    pytest tests/test_strategy_lab.py -v -m "not integration"
    pytest tests/test_strategy_lab.py -v                        # needs DataFetcher
"""
from __future__ import annotations

from strategy_lab.strategy_builder    import StrategyBuilder, DynamicStrategy
from strategy_lab.backtest_engine_v2  import BacktestEngineV2, BacktestResult
from strategy_lab.parameter_optimizer import ParameterOptimizer
from strategy_lab.performance_analyzer import PerformanceAnalyzer

from utils.logger import get_logger

logger = get_logger()


def run_backtest(
    strategy_config: dict,
    asset: str,
    category: str,
    initial_balance: float = 10_000.0,
    periods: int = 500,
) -> "BacktestResult":
    """
    Convenience wrapper — fetch data and run a full backtest in one call.
    Uses the existing DataFetcher so all caching and API fallbacks apply.
    """
    try:
        from data.fetcher import DataFetcher
        fetcher  = DataFetcher()
        df       = fetcher.get_ohlcv(asset, category, "1d", periods)
        if df is None or df.empty:
            raise ValueError(f"No OHLCV data available for {asset}")
        strategy = StrategyBuilder.from_dict(strategy_config)
        engine   = BacktestEngineV2(strategy=strategy,
                                    initial_balance=initial_balance)
        return engine.run(df)
    except Exception as e:
        logger.error(f"[StrategyLab] run_backtest failed: {e}", exc_info=True)
        raise


def optimize_strategy(
    base_config: dict,
    param_grid: dict,
    asset: str,
    category: str,
    initial_balance: float = 10_000.0,
    periods: int = 500,
) -> list:
    """
    Convenience wrapper — fetch data and run a full parameter grid search.
    Returns results sorted by Sharpe ratio (best first).
    """
    try:
        from data.fetcher import DataFetcher
        fetcher  = DataFetcher()
        df       = fetcher.get_ohlcv(asset, category, "1d", periods)
        if df is None or df.empty:
            raise ValueError(f"No OHLCV data available for {asset}")
        optimizer = ParameterOptimizer(
            base_config=base_config,
            df=df,
            initial_balance=initial_balance,
        )
        return optimizer.grid_search(param_grid)
    except Exception as e:
        logger.error(f"[StrategyLab] optimize_strategy failed: {e}", exc_info=True)
        raise


__all__ = [
    "StrategyBuilder", "DynamicStrategy",
    "BacktestEngineV2", "BacktestResult",
    "ParameterOptimizer", "PerformanceAnalyzer",
    "run_backtest", "optimize_strategy",
]
