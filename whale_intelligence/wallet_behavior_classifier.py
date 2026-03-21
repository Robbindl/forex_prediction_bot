from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import List, Optional

from utils.logger import get_logger

logger = get_logger()

# ── Data model ────────────────────────────────────────────────────────────────

@dataclass
class WalletProfile:
    """All we know about a single wallet."""
    address:        str
    label:          str             = ""
    chain:          str             = "btc"     # btc | eth
    wallet_type:    str             = "unknown" # exchange | unknown | institutional
    history:        List[dict]      = field(default_factory=list)   # [{delta, ts}]
    behavior:       str             = "unknown"
    confidence:     float           = 0.0
    total_received: float           = 0.0
    total_sent:     float           = 0.0
    last_active_ts: int             = 0


# ── Classifier ────────────────────────────────────────────────────────────────

class WalletBehaviorClassifier:
    """
    Stateless classifier — takes a WalletProfile, returns an updated copy
    with behaviour and confidence fields set.
    """

    MIN_HISTORY       = 5       # minimum movement count before classifying
    DORMANT_DAYS      = 180     # inactive this long → dormant label
    FLIPPER_MAX_HOLD  = 7 * 86400   # average hold < 7 days → flipper

    # ── Public API ────────────────────────────────────────────────────────────

    def classify(self, profile: WalletProfile) -> WalletProfile:
        """
        Assigns profile.behavior and profile.confidence in-place.
        Returns the same profile for chaining.
        """
        if len(profile.history) < self.MIN_HISTORY:
            profile.behavior   = "insufficient_data"
            profile.confidence = 0.0
            return profile

        # Hard-override: known exchange wallets
        if profile.wallet_type == "exchange":
            profile.behavior   = "exchange"
            profile.confidence = 0.95
            return profile

        buys  = [h for h in profile.history if h.get("delta", 0) > 0]
        sells = [h for h in profile.history if h.get("delta", 0) < 0]
        n     = len(profile.history)

        buy_ratio  = len(buys)  / n
        sell_ratio = len(sells) / n
        avg_hold   = self._avg_hold_time(profile.history)

        if self._is_dormant(profile):
            profile.behavior   = "dormant"
            profile.confidence = 0.90
        elif buy_ratio >= 0.80:
            profile.behavior   = "accumulator"
            profile.confidence = round(buy_ratio, 3)
        elif sell_ratio >= 0.80:
            profile.behavior   = "distributor"
            profile.confidence = round(sell_ratio, 3)
        elif (buy_ratio >= 0.35 and sell_ratio >= 0.35
              and avg_hold < self.FLIPPER_MAX_HOLD):
            profile.behavior   = "flipper"
            profile.confidence = round(min(buy_ratio, sell_ratio) * 2, 3)
        else:
            profile.behavior   = "mixed"
            profile.confidence = 0.40

        logger.debug(
            f"[Classifier] {profile.address[:12]}... "
            f"→ {profile.behavior} (conf={profile.confidence:.2f})"
        )
        return profile

    @staticmethod
    def signal_weight(behavior: str) -> float:
        """
        Returns how much weight to apply to this wallet's signal.
        Used by Layer 6 (WhaleLayer) when adjusting confidence scores.
        """
        return {
            "accumulator":       0.90,
            "distributor":       0.90,
            "dormant":           0.75,
            "mixed":             0.45,
            "flipper":           0.30,
            "unknown":           0.25,
            "insufficient_data": 0.10,
            "exchange":          0.15,
        }.get(behavior, 0.25)

    def get_signal_direction(self, profile: WalletProfile) -> Optional[str]:
        """
        Returns 'BUY', 'SELL', or None based on the last movement + behaviour.
        """
        if not profile.history:
            return None
        last_delta = profile.history[-1].get("delta", 0)
        if profile.behavior == "accumulator" and last_delta > 0:
            return "BUY"
        if profile.behavior == "distributor" and last_delta < 0:
            return "SELL"
        if profile.behavior == "dormant":
            return "BUY" if last_delta > 0 else "SELL"
        return None

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _avg_hold_time(self, history: List[dict]) -> float:
        """Average time in seconds between consecutive movements."""
        if len(history) < 2:
            return float("inf")
        timestamps = sorted(h.get("ts", 0) for h in history)
        diffs = [
            (timestamps[i + 1] - timestamps[i]) / 1000   # ms → s
            for i in range(len(timestamps) - 1)
        ]
        return sum(diffs) / len(diffs) if diffs else float("inf")

    def _is_dormant(self, profile: WalletProfile) -> bool:
        if not profile.last_active_ts:
            return False
        days_inactive = (
            (time.time() * 1000 - profile.last_active_ts)
            / (86_400 * 1000)
        )
        return days_inactive >= self.DORMANT_DAYS
