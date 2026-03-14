"""
risk/portfolio_risk.py — Portfolio-level risk controls.

Enforces constraints no single trade-level check can see:
  • maximum exposure per asset / category / total
  • correlation between open positions
  • portfolio drawdown halt
  • asset allocation targets

Sits between the signal aggregator and the execution router.
RiskManager handles per-trade checks. This handles the portfolio view.
"""
from __future__ import annotations
import threading
from typing import Dict, List, Optional, Tuple
from utils.logger import get_logger

logger = get_logger()

# ── Defaults (all overridable via constructor) ─────────────────────────────────
_MAX_SINGLE_ASSET_PCT   = 20.0   # max % of portfolio in any one asset
_MAX_CATEGORY_PCT       = 40.0   # max % in any one category (crypto, forex…)
_MAX_CORRELATION        = 0.85   # block new position if corr with open pos > this
_DRAWDOWN_HALT_PCT      = 8.0    # block all new positions if drawdown > 8%
_DRAWDOWN_REDUCE_PCT    = 5.0    # start scaling down above 5%

_TARGET_ALLOCATION = {
    "crypto":      40.0,
    "forex":       30.0,
    "commodities": 20.0,
    "indices":      5.0,
    "stocks":       5.0,
}
_ALLOCATION_TOLERANCE = 10.0   # ±10% from target is acceptable


class PortfolioRiskEngine:
    """
    Evaluates a proposed signal against the current portfolio state
    and returns (approved: bool, reason: str).
    """

    def __init__(
        self,
        max_single_asset_pct: float  = _MAX_SINGLE_ASSET_PCT,
        max_category_pct: float      = _MAX_CATEGORY_PCT,
        drawdown_halt_pct: float     = _DRAWDOWN_HALT_PCT,
        drawdown_reduce_pct: float   = _DRAWDOWN_REDUCE_PCT,
        target_allocation: Optional[Dict[str, float]] = None,
    ):
        self._max_asset    = max_single_asset_pct
        self._max_cat      = max_category_pct
        self._dd_halt      = drawdown_halt_pct
        self._dd_reduce    = drawdown_reduce_pct
        self._targets      = target_allocation or dict(_TARGET_ALLOCATION)
        self._lock         = threading.RLock()
        self._peak_balance = 0.0

    def evaluate(
        self,
        signal: dict,
        open_positions: List[dict],
        balance: float,
        initial_balance: float,
        daily_pnl: float,
    ) -> Tuple[bool, str]:
        """
        Returns (approved, reason).
        reason is empty string if approved.
        """
        with self._lock:
            # Update peak for drawdown calculation
            if balance > self._peak_balance:
                self._peak_balance = balance

            asset    = signal.get("asset", "")
            category = signal.get("category", "unknown")
            size     = float(signal.get("position_size", 0))
            entry    = float(signal.get("entry_price", 0))
            exposure = size * entry if entry else 0

            # ── 1. Drawdown halt ──────────────────────────────────────────
            if self._peak_balance > 0:
                drawdown_pct = (self._peak_balance - balance) / self._peak_balance * 100
                if drawdown_pct >= self._dd_halt:
                    return False, (
                        f"Portfolio drawdown {drawdown_pct:.1f}% >= "
                        f"halt threshold {self._dd_halt}%"
                    )
                if drawdown_pct >= self._dd_reduce:
                    # Allow but scale down
                    scale = 1.0 - (drawdown_pct - self._dd_reduce) / (self._dd_halt - self._dd_reduce)
                    signal["position_size"] = size * max(0.25, scale)
                    logger.info(
                        f"[PortfolioRisk] Scaling position to {scale:.0%} "
                        f"due to drawdown {drawdown_pct:.1f}%"
                    )

            # ── 2. Single-asset exposure ──────────────────────────────────
            asset_exposure = sum(
                float(p.get("position_size", 0)) * float(p.get("entry_price", 0))
                for p in open_positions
                if p.get("asset") == asset
            )
            if balance > 0:
                asset_pct = (asset_exposure + exposure) / balance * 100
                if asset_pct > self._max_asset:
                    return False, (
                        f"Asset exposure {asset_pct:.1f}% > max {self._max_asset}% "
                        f"for {asset}"
                    )

            # ── 3. Category exposure ──────────────────────────────────────
            cat_exposure = sum(
                float(p.get("position_size", 0)) * float(p.get("entry_price", 0))
                for p in open_positions
                if p.get("category") == category
            )
            if balance > 0:
                cat_pct = (cat_exposure + exposure) / balance * 100
                if cat_pct > self._max_cat:
                    return False, (
                        f"Category {category} exposure {cat_pct:.1f}% > max {self._max_cat}%"
                    )

            # ── 4. Allocation drift ───────────────────────────────────────
            if category in self._targets and balance > 0:
                new_cat_pct = (cat_exposure + exposure) / balance * 100
                target      = self._targets[category]
                if new_cat_pct > target + _ALLOCATION_TOLERANCE:
                    return False, (
                        f"Category {category} would be {new_cat_pct:.1f}% "
                        f"(target {target}% ±{_ALLOCATION_TOLERANCE}%)"
                    )

            # ── 5. Correlation block ──────────────────────────────────────
            # Simple proxy: block same-category same-direction if already
            # have a position (true correlation requires historical returns)
            direction = (signal.get("direction") or signal.get("signal", "BUY")).upper()
            same_dir_cat = [
                p for p in open_positions
                if p.get("category") == category
                and (p.get("direction") or p.get("signal", "")).upper() == direction
            ]
            # More than 3 same-direction positions in same category is
            # effectively highly correlated
            if len(same_dir_cat) >= 3:
                return False, (
                    f"Correlation risk: already {len(same_dir_cat)} {direction} "
                    f"positions in {category}"
                )

        return True, ""

    def get_portfolio_stats(
        self,
        open_positions: List[dict],
        balance: float,
    ) -> dict:
        """Returns current exposure breakdown for the dashboard."""
        total_exposure = sum(
            float(p.get("position_size", 0)) * float(p.get("entry_price", 0))
            for p in open_positions
        )
        by_category: Dict[str, float] = {}
        for p in open_positions:
            cat = p.get("category", "unknown")
            exp = float(p.get("position_size", 0)) * float(p.get("entry_price", 0))
            by_category[cat] = by_category.get(cat, 0) + exp

        drawdown = 0.0
        if self._peak_balance > 0:
            drawdown = (self._peak_balance - balance) / self._peak_balance * 100

        return {
            "total_exposure":  round(total_exposure, 2),
            "exposure_pct":    round(total_exposure / balance * 100, 1) if balance else 0,
            "drawdown_pct":    round(max(0, drawdown), 2),
            "peak_balance":    round(self._peak_balance, 2),
            "by_category":     {k: round(v, 2) for k, v in by_category.items()},
            "position_count":  len(open_positions),
        }