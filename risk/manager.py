"""risk/manager.py — Risk manager. Clean rewrite of advanced_risk_manager.py."""
from __future__ import annotations
import threading
from typing import Any, Dict, List, Optional, Tuple
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


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _stop_atr_multiplier(category: str) -> float:
    tuning = ASSET_CLASS_TUNING.get((category or "").lower(), {})
    return float(tuning.get("stop_loss_atr", 1.4))


def _default_risk_reward(category: str) -> float:
    tuning = ASSET_CLASS_TUNING.get((category or "").lower(), {})
    explicit_target_rr = float(tuning.get("target_rr", 0.0) or 0.0)
    if explicit_target_rr > 0:
        return max(1.0, explicit_target_rr)
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


def _structure_price_candidates(value: Any) -> List[float]:
    candidates: List[float] = []
    if isinstance(value, (int, float)):
        level = float(value)
        if level > 0:
            candidates.append(level)
        return candidates
    if isinstance(value, list):
        for item in value:
            try:
                level = float(item)
            except Exception:
                continue
            if level > 0:
                candidates.append(level)
    return candidates


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
        structure: Optional[Dict[str, Any]] = None,
    ) -> float:
        scale = max(0.75, min(1.25, float(distance_multiplier or 1.0)))
        if atr and atr > 0:
            dist = _clamp_stop_distance(entry, category, atr * _stop_atr_multiplier(category) * scale)
        else:
            dist = entry * _STOP_FALLBACK_PCT.get((category or "").lower(), 0.0060) * scale

        base_stop = entry - dist if direction == "BUY" else entry + dist
        structure_stop = self._structure_invalidation_stop(
            entry=entry,
            direction=direction,
            category=category,
            structure=structure,
            fallback_stop=base_stop,
            atr=atr,
        )
        return float(structure_stop if structure_stop > 0 else base_stop)

    def _structure_invalidation_stop(
        self,
        entry: float,
        direction: str,
        category: str,
        structure: Optional[Dict[str, Any]],
        fallback_stop: float,
        atr: float = 0.0,
    ) -> float:
        if entry <= 0:
            return float(fallback_stop)
        structure = structure if isinstance(structure, dict) else {}
        if not structure:
            return float(fallback_stop)

        cat = (category or "").lower()
        direction = str(direction or "").upper()
        support_keys = ["support", "support_levels", "swing_low", "recent_low", "invalid_below"]
        resistance_keys = ["resistance", "resistance_levels", "swing_high", "recent_high", "invalid_above"]

        raw_levels: List[float] = []
        for key in support_keys if direction == "BUY" else resistance_keys:
            raw_levels.extend(_structure_price_candidates(structure.get(key)))

        if not raw_levels:
            return float(fallback_stop)

        if direction == "BUY":
            protective_levels = sorted(level for level in raw_levels if level < entry)
        elif direction == "SELL":
            protective_levels = sorted(level for level in raw_levels if level > entry)
        else:
            return float(fallback_stop)

        if not protective_levels:
            return float(fallback_stop)

        # Use the nearest invalidation level, not the farthest one. A far anchor
        # widens the stop and destroys the trade's reward profile.
        anchor_level = protective_levels[-1] if direction == "BUY" else protective_levels[0]
        buffer_dist = 0.0
        if atr and atr > 0:
            buffer_dist = max(buffer_dist, atr * 0.10)
        buffer_dist = max(
            buffer_dist,
            entry * (_STOP_MIN_PCT.get(cat, 0.0025) * 0.20),
        )

        candidate_stop = anchor_level - buffer_dist if direction == "BUY" else anchor_level + buffer_dist
        candidate_dist = abs(entry - candidate_stop)
        if candidate_dist <= 0:
            return float(fallback_stop)

        clamped_dist = _clamp_stop_distance(entry, category, candidate_dist)
        structure_stop = entry - clamped_dist if direction == "BUY" else entry + clamped_dist

        fallback_dist = abs(entry - fallback_stop)
        if fallback_dist > 0:
            # Fall back only when the structural stop would be wider than the
            # generic ATR stop. A valid, tighter structural invalidation should
            # be preserved.
            if direction == "BUY" and structure_stop < fallback_stop:
                structure_stop = fallback_stop
            elif direction == "SELL" and structure_stop > fallback_stop:
                structure_stop = fallback_stop

        return float(structure_stop)

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

    @staticmethod
    def _structure_level(structure: Dict[str, Any], direction: str) -> float:
        target_key = "resistance" if direction == "BUY" else "support"
        levels_key = "resistance_levels" if direction == "BUY" else "support_levels"
        structure_level = _safe_float(structure.get(target_key), 0.0)
        if structure_level <= 0:
            levels = structure.get(levels_key)
            if isinstance(levels, list) and levels:
                structure_level = _safe_float(levels[0], 0.0)
        return structure_level

    @staticmethod
    def _structure_reward_cap(
        structure_reward: float,
        breakout_alignment: float,
        regime: str,
        volatility_state: str,
        alignment_score: float,
        setup_quality: float,
        confidence: float,
        atr: float,
    ) -> float:
        structure_cap = structure_reward * 0.94

        if regime in {"trending_up", "trending_down"}:
            structure_cap = structure_reward * 0.98

        if breakout_alignment >= 0.55 and regime in {"trending_up", "trending_down"}:
            extension = 0.04
            extension += max(0.0, alignment_score - 0.55) * 0.14
            extension += max(0.0, setup_quality - 0.55) * 0.18
            extension += max(0.0, min(0.35, confidence - 0.60)) * 0.20
            if volatility_state == "expansion":
                extension += 0.04
            elif volatility_state == "extreme":
                extension -= 0.08
            structure_cap = structure_reward * (1.0 + max(0.0, min(0.22, extension)))
            if atr > 0:
                structure_cap += atr * (0.18 + breakout_alignment * 0.28)
        elif volatility_state == "extreme":
            structure_cap = structure_reward * 0.88

        return structure_cap

    def align_take_profit_to_structure(
        self,
        entry: float,
        proposed_take_profit: float,
        direction: str,
        category: str = "",
        structure: Optional[Dict[str, Any]] = None,
        atr: float = 0.0,
        confidence: float = 0.0,
    ) -> float:
        if entry <= 0 or proposed_take_profit <= 0:
            return proposed_take_profit

        structure = structure if isinstance(structure, dict) else {}
        if not structure:
            return proposed_take_profit

        direction = str(direction or "").upper()
        if direction not in {"BUY", "SELL"}:
            return proposed_take_profit

        structure_level = self._structure_level(structure, direction)
        if structure_level <= 0:
            return proposed_take_profit

        proposed_reward = abs(float(proposed_take_profit) - float(entry))
        structure_reward = abs(structure_level - float(entry))
        if proposed_reward <= 0 or structure_reward <= 0:
            return proposed_take_profit

        alignment_score = max(0.0, min(1.0, _safe_float(structure.get("alignment_score"), 0.0)))
        setup_quality = max(0.0, min(1.0, _safe_float(structure.get("setup_quality"), 0.0)))
        breakout_score = _safe_float(structure.get("breakout_score"), 0.0)
        regime = str(structure.get("regime") or "").lower()
        volatility_state = str(structure.get("volatility_state") or "").lower()

        sign = 1.0 if direction == "BUY" else -1.0
        breakout_alignment = max(0.0, breakout_score * sign)
        structure_cap = self._structure_reward_cap(
            structure_reward,
            breakout_alignment,
            regime,
            volatility_state,
            alignment_score,
            setup_quality,
            confidence,
            atr,
        )

        # Do not force a far lower target if the current plan is already safely inside structure.
        if proposed_reward <= structure_reward * 0.85:
            return proposed_take_profit

        adjusted_reward = max(structure_reward * 0.72, min(proposed_reward, structure_cap))
        if adjusted_reward <= 0:
            return proposed_take_profit
        if abs(adjusted_reward - proposed_reward) <= 1e-9:
            return proposed_take_profit
        return entry + adjusted_reward if direction == "BUY" else entry - adjusted_reward
