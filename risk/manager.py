"""risk/manager.py — Risk manager. Clean rewrite of advanced_risk_manager.py."""
from __future__ import annotations
import threading
from typing import Dict, Optional, Tuple
from risk.position_sizer import PositionSizer
from utils.logger import get_logger
from config.config import DAILY_LOSS_LIMIT_PERCENT, MAX_RISK_PER_TRADE, MIN_CONFIDENCE_SCORE
from config.optimization import ASSET_CLASS_TUNING

logger = get_logger()

_STOP_FALLBACK_PCT = {
    "forex": 0.0035,
    "crypto": 0.0090,
    "commodities": 0.0075,
    "indices": 0.0060,
}

_STOP_MIN_PCT = {
    "forex": 0.0015,
    "crypto": 0.0040,
    "commodities": 0.0030,
    "indices": 0.0025,
}

_STOP_MAX_PCT = {
    "forex": 0.0060,
    "crypto": 0.0120,
    "commodities": 0.0100,
    "indices": 0.0090,
}

_DEFAULT_RISK_REWARD = 1.5


def _stop_atr_multiplier(category: str) -> float:
    tuning = ASSET_CLASS_TUNING.get((category or "").lower(), {})
    return float(tuning.get("stop_loss_atr", 1.4))


def _default_risk_reward(category: str) -> float:
    tuning = ASSET_CLASS_TUNING.get((category or "").lower(), {})
    stop_mult = float(tuning.get("stop_loss_atr", 1.0) or 1.0)
    take_mult = float(tuning.get("take_profit_atr", _DEFAULT_RISK_REWARD) or _DEFAULT_RISK_REWARD)
    if stop_mult > 0:
        return max(_DEFAULT_RISK_REWARD, take_mult / stop_mult)
    return _DEFAULT_RISK_REWARD


def _clamp_stop_distance(entry: float, category: str, dist: float) -> float:
    if entry <= 0 or dist <= 0:
        return max(float(dist or 0.0), 0.0)
    cat = (category or "").lower()
    min_dist = entry * _STOP_MIN_PCT.get(cat, 0.0025)
    max_dist = entry * _STOP_MAX_PCT.get(cat, 0.0090)
    return max(min_dist, min(max_dist, dist))


class DailyLossGuard:
    def __init__(self, balance: float, limit_pct: float = DAILY_LOSS_LIMIT_PERCENT):
        self._initial  = balance
        self._current  = balance   # FIX: track current balance separately
        self._limit    = limit_pct
        self._lock     = threading.Lock()

    def reset(self, balance: float) -> None:
        """Called at UTC midnight rollover — resets both initial and current."""
        with self._lock:
            self._initial = balance
            self._current = balance

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

    def update_balance(self, new_balance: float) -> None:
        """
        Sync account balance after a trade closes.
        Updates self.account_balance and PositionSizer.

        FIX: Previously this created a NEW DailyLossGuard on every trade close,
        which reset the baseline (_initial) after each trade.  That meant the
        daily loss protection degraded throughout the day — each consecutive
        losing trade reset the guard to the new (lower) balance, effectively
        allowing unlimited compounding losses.  Now we update the guard's
        running balance without re-seeding the initial baseline.
        """
        with self._lock:
            self.account_balance        = new_balance
            self._sizer.account_balance = new_balance
            # Update the guard's current balance for loss-pct calculation
            # but do NOT reset the day-start baseline (_initial stays fixed
            # until reset_daily() is called at UTC midnight rollover).
            self._daily_loss_guard._current = new_balance

    def reset_daily(self, balance: float) -> None:
        self._daily_loss_guard.reset(balance)

    def calculate_position_size(
        self,
        entry_price: float,
        stop_loss:   float,
        category:    str   = "forex",
        confidence:  float = 0.7,
        asset:       str   = "",  # Added for pip value calculation
    ) -> float:
        """Calculate position size with asset-aware pip values."""
        with self._lock:
            return self._sizer.calculate(
                entry_price, stop_loss, category, confidence, asset
            )

    def validate_signal(
        self,
        confidence: float,
        daily_pnl:  float,
        category:   str = "forex",
    ) -> Tuple[bool, str]:
        """Gate a signal through all risk checks. Returns (allowed, reason)."""
        if confidence < MIN_CONFIDENCE_SCORE:
            return False, f"Confidence {confidence:.3f} below minimum {MIN_CONFIDENCE_SCORE}"

        can_trade, msg = self._daily_loss_guard.check(daily_pnl)
        if not can_trade:
            return False, msg

        return True, "OK"

    def get_stop_loss(self, entry: float, direction: str, category: str, atr: float = 0.0) -> float:
        """Calculate SL using ATR when available, otherwise category fallback."""
        distance_multiplier = 1.0
        return self.get_stop_loss_scaled(
            entry,
            direction,
            category,
            atr=atr,
            distance_multiplier=distance_multiplier,
        )

    def get_stop_loss_scaled(
        self,
        entry: float,
        direction: str,
        category: str,
        atr: float = 0.0,
        distance_multiplier: float = 1.0,
    ) -> float:
        scale = max(0.75, min(1.25, float(distance_multiplier or 1.0)))
        if atr and atr > 0:
            dist = _clamp_stop_distance(entry, category, atr * _stop_atr_multiplier(category) * scale)
        else:
            dist = entry * _STOP_FALLBACK_PCT.get((category or "").lower(), 0.0060) * scale
        return entry - dist if direction == "BUY" else entry + dist

    def get_target_rr(self, category: str = "", rr_multiplier: float = 1.0) -> float:
        multiplier = max(0.70, min(1.30, float(rr_multiplier or 1.0)))
        return max(1.0, _default_risk_reward(category) * multiplier)

    def get_take_profit(
        self,
        entry: float,
        stop_loss: float,
        direction: str,
        category: str = "",
        rr: Optional[float] = None,
        rr_multiplier: float = 1.0,
    ) -> float:
        dist = abs(entry - stop_loss)
        ratio = float(rr) if rr and rr > 0 else self.get_target_rr(category, rr_multiplier=rr_multiplier)
        if dist <= 0:
            return entry
        return entry + dist * ratio if direction == "BUY" else entry - dist * ratio
