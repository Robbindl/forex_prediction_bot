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

    def __init__(self, min_votes: int = 2, min_confidence: float = 0.55):
        self.min_votes      = min_votes
        self.min_confidence = min_confidence
        self._strategies: List[BaseStrategy] = [
            RSIStrategy(),
            MACDStrategy(),
            BollingerStrategy(),
        ]

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