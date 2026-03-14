"""risk/manager.py — Risk manager. Clean rewrite of advanced_risk_manager.py."""
from __future__ import annotations
import threading
from typing import Dict, Optional, Tuple
from risk.position_sizer import PositionSizer
from utils.logger import get_logger
from config.config import MAX_RISK_PER_TRADE, MIN_CONFIDENCE_SCORE

logger = get_logger()

_DAILY_LOSS_LIMIT_PCT = 5.0   # halt trading if daily loss > 5% of balance


class DailyLossGuard:
    def __init__(self, balance: float, limit_pct: float = _DAILY_LOSS_LIMIT_PCT):
        self._initial  = balance
        self._limit    = limit_pct
        self._lock     = threading.Lock()

    def reset(self, balance: float) -> None:
        with self._lock:
            self._initial = balance

    def check(self, daily_pnl: float) -> Tuple[bool, str]:
        """Returns (can_trade, message)."""
        with self._lock:
            loss_pct = abs(daily_pnl) / self._initial * 100 if self._initial else 0
            if daily_pnl < 0 and loss_pct >= self._limit:
                msg = f"Daily loss limit hit: {loss_pct:.2f}% >= {self._limit}%"
                return False, msg
            return True, ""


class RiskManager:
    """Central risk gatekeeper."""

    def __init__(self, account_balance: float = 10000.0):
        self.account_balance   = account_balance
        self._sizer            = PositionSizer(account_balance)
        self._daily_loss_guard = DailyLossGuard(account_balance)
        self._lock             = threading.Lock()

    def update_balance(self, balance: float) -> None:
        with self._lock:
            self.account_balance      = balance
            self._sizer.account_balance = balance

    def reset_daily(self, balance: float) -> None:
        self._daily_loss_guard.reset(balance)

    def update_balance(self, new_balance: float) -> None:
        """Sync account balance after a trade closes."""
        self.account_balance = new_balance
        self._daily_loss_guard = DailyLossGuard(
            balance=new_balance,
            limit_pct=_DAILY_LOSS_LIMIT_PCT,
        )

    def calculate_position_size(
        self,
        entry_price: float,
        stop_loss: float,
        category: str = "forex",
        confidence: float = 0.7,
    ) -> float:
        with self._lock:
            return self._sizer.calculate(entry_price, stop_loss, category, confidence)

    def validate_signal(
        self,
        confidence: float,
        daily_pnl: float,
        category: str = "forex",
    ) -> Tuple[bool, str]:
        """Gate a signal through all risk checks. Returns (allowed, reason)."""
        if confidence < MIN_CONFIDENCE_SCORE:
            return False, f"Confidence {confidence:.3f} below minimum {MIN_CONFIDENCE_SCORE}"

        can_trade, msg = self._daily_loss_guard.check(daily_pnl)
        if not can_trade:
            return False, msg

        return True, "OK"

    def get_stop_loss(self, entry: float, direction: str, category: str, atr: float = 0.0) -> float:
        """Calculate SL based on ATR or fixed percentage."""
        mult = {"crypto": 2.0, "forex": 1.5, "stocks": 1.8}.get(category, 1.5)
        if atr:
            dist = atr * mult
        else:
            dist = entry * 0.015   # 1.5% default
        return entry - dist if direction == "BUY" else entry + dist

    def get_take_profit(self, entry: float, stop_loss: float, direction: str, rr: float = 2.0) -> float:
        dist = abs(entry - stop_loss)
        return entry + dist * rr if direction == "BUY" else entry - dist * rr