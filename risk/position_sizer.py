"""risk/position_sizer.py — Dynamic position sizer. Extracted from advanced_risk_manager.py."""
from __future__ import annotations
from utils.logger import get_logger
from config.config import (
    DEFAULT_RISK_PER_TRADE, CRYPTO_RISK_PER_TRADE, MAX_RISK_PER_TRADE,
    CRYPTO_MAX_POSITION_SIZE,
)

logger = get_logger()


class PositionSizer:
    """Calculates position size using fixed fractional risk per trade."""

    def __init__(self, account_balance: float):
        self.account_balance = account_balance

    def calculate(
        self,
        entry_price: float,
        stop_loss: float,
        category: str = "forex",
        confidence: float = 0.7,
    ) -> float:
        """
        Returns position size (units) based on risk per trade.
        Uses stop distance to scale size so total risk = risk_pct * balance.
        """
        if not entry_price or not stop_loss or entry_price == stop_loss:
            return 0.0

        risk_pct = CRYPTO_RISK_PER_TRADE if category == "crypto" else DEFAULT_RISK_PER_TRADE
        risk_pct = min(risk_pct * (0.7 + confidence * 0.6), MAX_RISK_PER_TRADE)

        risk_amount   = self.account_balance * risk_pct / 100
        stop_distance = abs(entry_price - stop_loss)

        if stop_distance == 0:
            return 0.0

        size = risk_amount / stop_distance

        # Crypto cap
        if category == "crypto":
            max_size = self.account_balance * CRYPTO_MAX_POSITION_SIZE / entry_price
            size     = min(size, max_size)

        return round(max(0.001, size), 6)