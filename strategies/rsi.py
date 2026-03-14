"""strategies/rsi.py — RSI mean-reversion strategy."""
from __future__ import annotations
from typing import Optional
import pandas as pd
import numpy as np
from strategies.base import BaseStrategy
from core.signal import Signal
from utils.logger import get_logger

logger = get_logger()


class RSIStrategy(BaseStrategy):
    name = "RSI"

    def __init__(self, period: int = 14, oversold: float = 30, overbought: float = 70):
        self.period     = period
        self.oversold   = oversold
        self.overbought = overbought

    def generate(self, asset, canonical, category, df) -> Optional[Signal]:
        if not self._has_enough_data(df, self.period + 5):
            return None
        try:
            close  = df["close"].astype(float)
            rsi    = self._rsi(close, self.period)
            latest = rsi.iloc[-1]
            prev   = rsi.iloc[-2]
            price  = float(close.iloc[-1])
            atr    = self._atr(df, 14).iloc[-1]

            direction = None
            if prev <= self.oversold and latest > self.oversold:
                direction  = "BUY"
                confidence = 0.55 + min(0.2, (self.oversold - prev) / 30)
            elif prev >= self.overbought and latest < self.overbought:
                direction  = "SELL"
                confidence = 0.55 + min(0.2, (prev - self.overbought) / 30)

            if direction is None:
                return None

            sl = price - 1.5 * atr if direction == "BUY" else price + 1.5 * atr
            tp = price + 2.5 * atr if direction == "BUY" else price - 2.5 * atr

            return self._make_signal(
                asset, canonical, category, direction, confidence,
                price, sl, tp,
                indicators={"rsi": round(latest, 2), "rsi_prev": round(prev, 2)},
            )
        except Exception as e:
            logger.debug(f"[RSI] {asset}: {e}")
            return None

    @staticmethod
    def _rsi(close: pd.Series, period: int) -> pd.Series:
        delta = close.diff()
        gain  = delta.clip(lower=0).ewm(com=period - 1, min_periods=period).mean()
        loss  = (-delta.clip(upper=0)).ewm(com=period - 1, min_periods=period).mean()
        rs    = gain / loss.replace(0, np.nan)
        return 100 - (100 / (1 + rs))

    @staticmethod
    def _atr(df: pd.DataFrame, period: int) -> pd.Series:
        h, l, c = df["high"].astype(float), df["low"].astype(float), df["close"].astype(float)
        tr = pd.concat([h - l, (h - c.shift()).abs(), (l - c.shift()).abs()], axis=1).max(axis=1)
        return tr.ewm(span=period, min_periods=period).mean()