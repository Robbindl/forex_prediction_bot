"""strategies/macd.py — MACD crossover strategy."""
from __future__ import annotations
from typing import Optional
import pandas as pd
from strategies.base import BaseStrategy
from core.signal import Signal
from utils.logger import get_logger

logger = get_logger()


class MACDStrategy(BaseStrategy):
    name = "MACD"

    def __init__(self, fast: int = 12, slow: int = 26, signal: int = 9):
        self.fast   = fast
        self.slow   = slow
        self.signal = signal

    def generate(self, asset, canonical, category, df) -> Optional[Signal]:
        if not self._has_enough_data(df, self.slow + self.signal + 5):
            return None
        try:
            close = df["close"].astype(float)
            price = float(close.iloc[-1])

            ema_fast   = close.ewm(span=self.fast,   min_periods=self.fast).mean()
            ema_slow   = close.ewm(span=self.slow,   min_periods=self.slow).mean()
            macd_line  = ema_fast - ema_slow
            signal_line = macd_line.ewm(span=self.signal, min_periods=self.signal).mean()
            histogram  = macd_line - signal_line

            curr_hist = histogram.iloc[-1]
            prev_hist = histogram.iloc[-2]
            curr_macd = macd_line.iloc[-1]
            curr_sig  = signal_line.iloc[-1]

            direction = None
            if prev_hist < 0 and curr_hist > 0:
                direction  = "BUY"
                confidence = 0.55 + min(0.2, abs(curr_hist) / (abs(price) * 0.001 + 1e-9))
            elif prev_hist > 0 and curr_hist < 0:
                direction  = "SELL"
                confidence = 0.55 + min(0.2, abs(curr_hist) / (abs(price) * 0.001 + 1e-9))

            if direction is None:
                return None

            atr = self._atr(df, 14).iloc[-1]
            sl  = price - 1.8 * atr if direction == "BUY" else price + 1.8 * atr
            tp  = price + 3.0 * atr if direction == "BUY" else price - 3.0 * atr

            return self._make_signal(
                asset, canonical, category, direction, min(0.85, confidence),
                price, sl, tp,
                indicators={
                    "macd":      round(curr_macd, 6),
                    "signal":    round(curr_sig, 6),
                    "histogram": round(curr_hist, 6),
                },
            )
        except Exception as e:
            logger.debug(f"[MACD] {asset}: {e}")
            return None

    @staticmethod
    def _atr(df, period):
        h, l, c = df["high"].astype(float), df["low"].astype(float), df["close"].astype(float)
        import pandas as pd
        tr = pd.concat([h - l, (h - c.shift()).abs(), (l - c.shift()).abs()], axis=1).max(axis=1)
        return tr.ewm(span=period, min_periods=period).mean()