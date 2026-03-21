from __future__ import annotations

from typing import Dict, List, Optional, TYPE_CHECKING

import pandas as pd

from strategies.base import BaseStrategy
from core.signal     import Signal
from utils.logger    import get_logger

if TYPE_CHECKING:
    pass

logger = get_logger()


class DynamicStrategyLive(BaseStrategy):
    """
    Wraps a strategy_lab DynamicStrategy config as a BaseStrategy.
    Drop-in replacement for RSIStrategy, MACDStrategy, etc.

    Parameters
    ----------
    config  : dict from StrategyBuilder (any preset or custom config)
    min_confidence : minimum confidence to return a signal (default 0.65)
    """

    def __init__(
        self,
        config: Dict,
        min_confidence: float = 0.65,
    ) -> None:
        from strategy_lab.strategy_builder import StrategyBuilder
        self._dynamic      = StrategyBuilder.from_dict(config)
        self._min_conf     = min_confidence
        self.name          = config.get("name", "dynamic")
        self.version       = config.get("version", "1.0")

    # ── BaseStrategy interface ────────────────────────────────────────────────

    def generate(
        self,
        asset:     str,
        canonical: str,
        category:  str,
        df:        pd.DataFrame,
    ) -> Optional[Signal]:
        """
        Called by VotingStrategy and TradingCore exactly like any other
        BaseStrategy. Delegates to the underlying DynamicStrategy and
        converts the result into a proper Signal object.
        """
        if df is None or len(df) < 50:
            return None
        try:
            result = self._dynamic.generate(df)
            if result is None:
                return None
            if float(result.get("confidence", 0)) < self._min_conf:
                return None
            return self._make_signal(
                asset       = asset,
                canonical   = canonical,
                category    = category,
                direction   = result["direction"],
                confidence  = float(result["confidence"]),
                entry       = float(result["entry_price"]),
                stop_loss   = float(result["stop_loss"]),
                take_profit = float(result["take_profit"]),
                indicators  = result.get("indicators", {}),
            )
        except Exception as e:
            logger.debug(f"[LiveBridge] {self.name} on {asset}: {e}")
            return None


# ── VotingStrategy runtime injection ─────────────────────────────────────────

def add_to_voting(
    strategy:       "DynamicStrategyLive",
    voting_instance = None,
) -> bool:
    """
    Add a DynamicStrategyLive to a VotingStrategy instance at runtime.

    Parameters
    ----------
    strategy        : DynamicStrategyLive instance to add
    voting_instance : existing VotingStrategy instance, or None to find it
                      automatically from TradingCore

    Returns True if successfully added, False otherwise.
    """
    try:
        if voting_instance is None:
            # Try to find the active VotingStrategy from TradingCore state
            from core.engine import TradingCore
            # TradingCore is a singleton — access via its module-level reference
            import core.engine as _eng
            # Walk through active strategy references
            for attr in dir(_eng):
                obj = getattr(_eng, attr, None)
                if hasattr(obj, "_strategies"):
                    voting_instance = obj
                    break

        if voting_instance is None:
            logger.warning("[LiveBridge] Could not find VotingStrategy instance")
            return False

        if not hasattr(voting_instance, "_strategies"):
            logger.warning("[LiveBridge] Target is not a VotingStrategy")
            return False

        # Check not already added
        existing_names = [s.name for s in voting_instance._strategies]
        if strategy.name in existing_names:
            logger.info(f"[LiveBridge] {strategy.name} already in voting")
            return True

        voting_instance._strategies.append(strategy)
        logger.info(
            f"[LiveBridge] Added '{strategy.name}' to VotingStrategy "
            f"(now {len(voting_instance._strategies)} strategies)"
        )
        return True

    except Exception as e:
        logger.warning(f"[LiveBridge] add_to_voting failed: {e}")
        return False


# ── Registry: configs selected for live trading ───────────────────────────────

# Edit this list to choose which lab strategies run live.
# Only add configs that performed well in your backtests.
# Keep it small — 1-2 additions to VotingStrategy is enough.
LIVE_STRATEGY_CONFIGS: List[Dict] = [
    # Uncomment the strategies you want to add after backtesting:
    # StrategyBuilder.triple_ema_config(),
    # StrategyBuilder.golden_cross_config(),
    # StrategyBuilder.macd_rsi_confluence_config(),
    # StrategyBuilder.adx_ema_momentum_config(),
    # StrategyBuilder.bollinger_rsi_reversion_config(),
]


def register_best_strategies(voting_instance=None) -> int:
    """
    Registers all configs in LIVE_STRATEGY_CONFIGS into the VotingStrategy.
    Called from bot.py during startup.

    Returns number of strategies successfully added.
    """
    from strategy_lab.strategy_builder import StrategyBuilder  # noqa: F401

    if not LIVE_STRATEGY_CONFIGS:
        logger.info(
            "[LiveBridge] No live strategies configured. "
            "Edit LIVE_STRATEGY_CONFIGS in strategy_lab/live_bridge.py "
            "to add strategies after backtesting."
        )
        return 0

    added = 0
    for config in LIVE_STRATEGY_CONFIGS:
        try:
            live_strat = DynamicStrategyLive(config)
            if add_to_voting(live_strat, voting_instance):
                added += 1
        except Exception as e:
            name = config.get("name", "unknown")
            logger.warning(f"[LiveBridge] Failed to register '{name}': {e}")

    logger.info(f"[LiveBridge] Registered {added} lab strategies for live trading")
    return added


def list_live_strategies() -> List[str]:
    """Returns names of all configs currently in LIVE_STRATEGY_CONFIGS."""
    return [c.get("name", "unknown") for c in LIVE_STRATEGY_CONFIGS]
