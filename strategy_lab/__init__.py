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
        try:
            from config.config import TRADING_TIMEFRAME as _TF
        except Exception:
            _TF = "15m"
            df       = fetcher.get_ohlcv(asset, category, _TF, periods)
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
        try:
            from config.config import TRADING_TIMEFRAME as _TF
        except Exception:
            _TF = "15m"
            df       = fetcher.get_ohlcv(asset, category, _TF, periods)
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