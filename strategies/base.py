"""strategies/base.py — BaseStrategy interface."""
from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Optional
import pandas as pd
from core.signal import Signal


class BaseStrategy(ABC):
    name: str = "base"
    version: str = "1.0"

    @abstractmethod
    def generate(
        self,
        asset: str,
        canonical: str,
        category: str,
        df: pd.DataFrame,
    ) -> Optional[Signal]:
        """
        Analyse df (OHLCV) and return a Signal or None.
        df must have columns: open, high, low, close, volume (lowercase).
        """
        ...

    def _make_signal(
        self,
        asset: str,
        canonical: str,
        category: str,
        direction: str,
        confidence: float,
        entry: float,
        stop_loss: float,
        take_profit: float,
        indicators: dict,
    ) -> Signal:
        rr = 0.0
        if stop_loss and entry and stop_loss != entry:
            risk   = abs(entry - stop_loss)
            reward = abs(take_profit - entry)
            rr     = reward / risk if risk else 0.0

        return Signal(
            asset=asset,
            canonical_asset=canonical,
            direction=direction,
            category=category,
            confidence=round(min(1.0, max(0.0, confidence)), 4),
            entry_price=entry,
            stop_loss=stop_loss,
            take_profit=take_profit,
            risk_reward=round(rr, 2),
            strategy_id=self.name,
            indicators=indicators,
        )

    @staticmethod
    def _has_enough_data(df: pd.DataFrame, min_rows: int = 50) -> bool:
        return df is not None and len(df) >= min_rows