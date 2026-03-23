from __future__ import annotations

from typing import Dict, List, Optional, TYPE_CHECKING

import pandas as pd

from utils.logger import get_logger

if TYPE_CHECKING:
    from strategies.base import BaseStrategy
    from strategy_lab.backtest_engine_v2 import BacktestResult

logger = get_logger()


class StrategyAdapter:
    """
    Wraps a BaseStrategy instance so BacktestEngineV2 can call it.

    Translates:
        BacktestEngineV2 calls  →  strategy.generate(df)
        Adapter calls           →  wrapped.generate(asset, canonical, category, df)
        Signal returned         →  converted to dict for BacktestEngineV2
    """

    def __init__(
        self,
        strategy: "BaseStrategy",
        asset:    str = "BTC-USD",
        category: str = "crypto",
    ) -> None:
        self._strategy  = strategy
        self._asset     = asset
        self._category  = category
        # canonical is just asset with "/" replaced by "-" for consistency
        self._canonical = asset.replace("/", "-")
        self.name       = getattr(strategy, "name", strategy.__class__.__name__)
        self.version    = getattr(strategy, "version", "1.0")

    # ── BacktestEngineV2 interface ────────────────────────────────────────────

    def generate(self, df: pd.DataFrame) -> Optional[Dict]:
        """
        Called by BacktestEngineV2 on every bar.
        Delegates to the wrapped strategy and converts Signal → dict.
        """
        if df is None or len(df) < 2:
            return None
        try:
            signal = self._strategy.generate(
                self._asset,
                self._canonical,
                self._category,
                df,
            )
            if signal is None:
                return None
            return self._signal_to_dict(signal)
        except Exception as e:
            logger.debug(f"[StrategyAdapter] {self.name} generate error: {e}")
            return None

    # ── Internal ──────────────────────────────────────────────────────────────

    @staticmethod
    def _signal_to_dict(signal) -> Dict:
        """
        Convert a core.signal.Signal object to the dict schema that
        BacktestEngineV2 reads.

        BacktestEngineV2 reads: direction, confidence, entry_price,
                                stop_loss, take_profit, strategy_id
        """
        return {
            "direction":   signal.direction,
            "confidence":  float(signal.confidence),
            "entry_price": float(signal.entry_price),
            "stop_loss":   float(signal.stop_loss),
            "take_profit": float(signal.take_profit),
            "strategy_id": getattr(signal, "strategy_id", "unknown"),
            "indicators":  getattr(signal, "indicators",  {}),
        }


# ── Convenience functions ─────────────────────────────────────────────────────

def backtest_existing(
    strategy:        "BaseStrategy",
    asset:           str   = "BTC-USD",
    category:        str   = "crypto",
    initial_balance: float = 10_000.0,
    periods:         int   = 500,
    interval:        str   = "15m",
) -> "BacktestResult":
    """
    Fetch historical data and run a full backtest on any existing strategy.

    Parameters
    ----------
    strategy        : any BaseStrategy subclass instance
    asset           : asset identifier e.g. "BTC-USD", "EUR/USD"
    category        : "crypto" | "forex" | "commodities" | "indices"
    initial_balance : starting account balance
    periods         : number of historical bars to fetch
    interval        : "1d" | "1h" | "15m"

    Returns
    -------
    BacktestResult with full metrics and trade log
    """
    from data.fetcher import DataFetcher
    from strategy_lab.backtest_engine_v2 import BacktestEngineV2

    fetcher = DataFetcher()
    df      = fetcher.get_ohlcv(asset, category, interval, periods)
    if df is None or df.empty:
        raise ValueError(f"No OHLCV data for {asset} ({category})")

    adapter = StrategyAdapter(strategy, asset=asset, category=category)
    engine  = BacktestEngineV2(strategy=adapter, initial_balance=initial_balance)
    result  = engine.run(df)

    logger.info(
        f"[StrategyAdapter] {strategy.__class__.__name__} on {asset}: "
        f"{result.summary()}"
    )
    return result


def compare_all_strategies(
    df:              pd.DataFrame,
    asset:           str   = "BTC-USD",
    category:        str   = "crypto",
    initial_balance: float = 10_000.0,
) -> List[Dict]:
    """
    Run BacktestEngineV2 on all four of your existing strategies and
    return results ranked by Sharpe ratio.

    Parameters
    ----------
    df              : OHLCV DataFrame (fetch once, reuse for all strategies)
    asset           : asset identifier
    category        : asset category
    initial_balance : starting balance for each backtest

    Returns
    -------
    List of dicts sorted best-to-worst by Sharpe ratio.
    Each dict has: label, sharpe, win_rate, total_pnl,
                   max_drawdown, trades, profit_factor
    """
    from strategies.rsi       import RSIStrategy
    from strategies.macd      import MACDStrategy
    from strategies.bollinger import BollingerStrategy
    from strategies.voting    import VotingStrategy
    from strategy_lab.backtest_engine_v2  import BacktestEngineV2
    from strategy_lab.performance_analyzer import PerformanceAnalyzer

    strategy_classes = [
        ("RSI",       RSIStrategy()),
        ("MACD",      MACDStrategy()),
        ("Bollinger", BollingerStrategy()),
        ("Voting",    VotingStrategy()),
    ]

    results = []
    analyzer = PerformanceAnalyzer()

    for label, strategy in strategy_classes:
        try:
            adapter = StrategyAdapter(strategy, asset=asset, category=category)
            engine  = BacktestEngineV2(strategy=adapter,
                                       initial_balance=initial_balance)
            result  = engine.run(df)
            results.append((label, result))
            logger.info(f"[StrategyAdapter] {label}: {result.summary()}")
        except Exception as e:
            logger.warning(f"[StrategyAdapter] {label} failed: {e}")

    if not results:
        return []

    backtest_results = [r for _, r in results]
    labels           = [l for l, _ in results]
    return analyzer.compare(backtest_results, labels=labels)


def compare_all_strategies_from_asset(
    asset:           str   = "BTC-USD",
    category:        str   = "crypto",
    initial_balance: float = 10_000.0,
    periods:         int   = 500,
) -> List[Dict]:
    """
    Same as compare_all_strategies but fetches data automatically.
    Convenience wrapper for one-liner usage.

    Example
    -------
        from strategy_lab.strategy_adapter import compare_all_strategies_from_asset
        results = compare_all_strategies_from_asset("EUR/USD", "forex")
        for r in results:
            print(r)
    """
    try:
        import core.engine as _eng_mod
        fetcher = getattr(getattr(_eng_mod, "_CORE_INSTANCE", None), "fetcher", None)
    except Exception:
        fetcher = None
    if fetcher is None:
        from data.fetcher import DataFetcher
        fetcher = DataFetcher()
    from config.config import TRADING_TIMEFRAME
    _TF = TRADING_TIMEFRAME
    df = fetcher.get_ohlcv(asset, category, _TF, periods)
    if df is None or df.empty:
        raise ValueError(f"No OHLCV data for {asset} ({category})")
    return compare_all_strategies(df, asset=asset, category=category,
                                  initial_balance=initial_balance)