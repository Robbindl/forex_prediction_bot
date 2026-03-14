"""strategies/bollinger.py — Bollinger Band breakout/reversion strategy."""
from __future__ import annotations
from typing import Optional
import pandas as pd
import numpy as np
from strategies.base import BaseStrategy
from core.signal import Signal
from utils.logger import get_logger

logger = get_logger()


class BollingerStrategy(BaseStrategy):
    name = "Bollinger"

    def __init__(self, period: int = 20, std_dev: float = 2.0):
        self.period  = period
        self.std_dev = std_dev

    def generate(self, asset, canonical, category, df) -> Optional[Signal]:
        if not self._has_enough_data(df, self.period + 5):
            return None
        try:
            close  = df["close"].astype(float)
            price  = float(close.iloc[-1])
            sma    = close.rolling(self.period).mean()
            std    = close.rolling(self.period).std()
            upper  = sma + self.std_dev * std
            lower  = sma - self.std_dev * std
            bw     = (upper - lower) / sma        # bandwidth

            cur_close = close.iloc[-1]
            prev_close = close.iloc[-2]
            cur_upper  = upper.iloc[-1]
            cur_lower  = lower.iloc[-1]
            cur_mid    = sma.iloc[-1]
            cur_bw     = bw.iloc[-1]

            # Width filter — ignore squeeze
            if cur_bw < 0.005:
                return None

            direction  = None
            confidence = 0.0

            # Price crosses below lower band then bounces back above
            if prev_close <= cur_lower and cur_close > cur_lower:
                direction  = "BUY"
                confidence = 0.55 + min(0.2, (cur_lower - prev_close) / (cur_mid * 0.01 + 1e-9))

            # Price crosses above upper band then drops back below
            elif prev_close >= cur_upper and cur_close < cur_upper:
                direction  = "SELL"
                confidence = 0.55 + min(0.2, (prev_close - cur_upper) / (cur_mid * 0.01 + 1e-9))

            if direction is None:
                return None

            atr = self._atr(df, 14).iloc[-1]
            sl  = price - 1.5 * atr if direction == "BUY" else price + 1.5 * atr
            tp  = cur_mid if direction == "BUY" else cur_mid   # target the middle band

            return self._make_signal(
                asset, canonical, category, direction, min(0.85, confidence),
                price, sl, tp,
                indicators={
                    "bb_upper":  round(cur_upper, 5),
                    "bb_lower":  round(cur_lower, 5),
                    "bb_mid":    round(cur_mid, 5),
                    "bb_width":  round(float(cur_bw), 5),
                },
            )
        except Exception as e:
            logger.debug(f"[Bollinger] {asset}: {e}")
            return None

    @staticmethod
    def _atr(df, period):
        h, l, c = df["high"].astype(float), df["low"].astype(float), df["close"].astype(float)
        tr = pd.concat([h - l, (h - c.shift()).abs(), (l - c.shift()).abs()], axis=1).max(axis=1)
        return tr.ewm(span=period, min_periods=period).mean()