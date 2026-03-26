"""
strategies/voting.py — Ensemble voting strategy.
Merges strategies/voting_engine.py. All strategies vote; majority wins.
"""
from __future__ import annotations
from typing import List, Optional
import pandas as pd
from strategies.base import BaseStrategy
from strategies.rsi      import RSIStrategy
from strategies.macd     import MACDStrategy
from strategies.bollinger import BollingerStrategy
from core.signal import Signal
from utils.logger import get_logger

logger = get_logger()


class VotingStrategy(BaseStrategy):
    name = "Voting"

    def __init__(self, min_votes: int = 1, min_confidence: float = 0.58):
        # 15m optimization: min_confidence lowered from 0.55 to 0.58
        # Generates actionable signals on single strong strategy votes
        # Confidence floor of 0.58 ensures quality signals without being too strict
        self.min_votes      = min_votes
        self.min_confidence = min_confidence
        self._strategies: List[BaseStrategy] = [
            RSIStrategy(),
            MACDStrategy(),
            BollingerStrategy(),
        ]
        # Auto-load any lab strategies registered in live_bridge.py
        self._load_live_bridge_strategies()

    def _load_live_bridge_strategies(self) -> None:
        """
        Automatically loads strategies from strategy_lab/live_bridge.py
        LIVE_STRATEGY_CONFIGS on every instantiation.
        This means adding a strategy to LIVE_STRATEGY_CONFIGS and
        restarting bot.py is all that is needed — no other changes.
        Silently skips if strategy_lab is not installed.
        """
        try:
            from strategy_lab.live_bridge    import LIVE_STRATEGY_CONFIGS
            from strategy_lab.live_bridge    import DynamicStrategyLive
            for config in LIVE_STRATEGY_CONFIGS:
                live = DynamicStrategyLive(config)
                existing = [s.name for s in self._strategies]
                if live.name not in existing:
                    self._strategies.append(live)
                    logger.info(f"[Voting] Live bridge: added '{live.name}'")
        except ImportError:
            pass   # strategy_lab not installed — skip silently
        except Exception as e:
            logger.debug(f"[Voting] Live bridge load error: {e}")

    def generate(self, asset, canonical, category, df) -> Optional[Signal]:
        signals: List[Signal] = []
        for strat in self._strategies:
            try:
                sig = strat.generate(asset, canonical, category, df)
                if sig and sig.confidence >= self.min_confidence:
                    signals.append(sig)
            except Exception as e:
                logger.debug(f"[Voting] {strat.name} error on {asset}: {e}")

        if not signals:
            logger.debug(f"[Voting] {asset} no base signals at confidence>={self.min_confidence}")
            return None

        buy_signals  = [s for s in signals if s.direction == "BUY"]
        sell_signals = [s for s in signals if s.direction == "SELL"]

        dominant = buy_signals if len(buy_signals) >= len(sell_signals) else sell_signals
        if len(dominant) < self.min_votes:
            return None

        # Aggregate: average confidence, use median prices
        direction  = dominant[0].direction
        confidence = sum(s.confidence for s in dominant) / len(dominant)

        # Boost for unanimity
        if len(signals) == len(dominant):
            confidence = min(1.0, confidence + 0.05)

        entry = dominant[0].entry_price
        sl    = (sum(s.stop_loss   for s in dominant) / len(dominant))
        tp    = (sum(s.take_profit for s in dominant) / len(dominant))

        combined_indicators = {}
        for s in dominant:
            combined_indicators.update(s.indicators)
        combined_indicators["votes"]        = len(dominant)
        combined_indicators["total_signals"] = len(signals)
        combined_indicators["strategies"]   = [s.strategy_id for s in dominant]

        return self._make_signal(
            asset, canonical, category, direction, confidence,
            entry, sl, tp, combined_indicators,
        )

    def add_strategy(self, strategy: BaseStrategy) -> None:
        """
        Add a new strategy to the voting pool at runtime.
        Used by strategy_lab/live_bridge.py to inject lab-tested strategies.
        Safe to call after bot startup — takes effect on the next scan cycle.
        """
        existing = [s.name for s in self._strategies]
        if strategy.name in existing:
            logger.info(f"[Voting] '{strategy.name}' already in pool — skipped")
            return
        self._strategies.append(strategy)
        logger.info(
            f"[Voting] Added '{strategy.name}' — "
            f"pool now has {len(self._strategies)} strategies: "
            f"{[s.name for s in self._strategies]}"
        )

    def remove_strategy(self, name: str) -> bool:
        """
        Remove a strategy from the voting pool by name.
        Returns True if found and removed, False if not found.
        Protects against removing all strategies — minimum pool size is 1.
        """
        if len(self._strategies) <= 1:
            logger.warning("[Voting] Cannot remove — pool must have at least 1 strategy")
            return False
        before = len(self._strategies)
        self._strategies = [s for s in self._strategies if s.name != name]
        if len(self._strategies) < before:
            logger.info(f"[Voting] Removed '{name}' — pool now has {len(self._strategies)} strategies")
            return True
        logger.warning(f"[Voting] '{name}' not found in pool")
        return False

    def list_strategies(self) -> List[str]:
        """Return names of all strategies currently in the voting pool."""
        return [s.name for s in self._strategies]