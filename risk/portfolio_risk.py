from __future__ import annotations
import threading
from typing import Dict, List, Optional, Tuple
from config.config import (
    DRAWDOWN_HALT_PERCENT,
    DRAWDOWN_REDUCE_PERCENT,
    PORTFOLIO_CORRELATION_CATEGORY_TRIGGER_PCT,
    PORTFOLIO_MAX_CATEGORY_PCT,
    PORTFOLIO_MAX_SAME_DIRECTION_POSITIONS,
    PORTFOLIO_MAX_SINGLE_ASSET_PCT,
)
from utils.logger import get_logger

logger = get_logger()


def _lot_exposure(asset: str, category: str, units: float, entry: float) -> float:
    """Convert position units to USD exposure using lot-based calculation."""
    try:
        from risk.position_sizer import CONTRACT_SPECS, _DEFAULTS
        spec     = CONTRACT_SPECS.get(asset) or _DEFAULTS.get(category, {})
        contract = spec.get("contract", 1)
        pip_val  = spec.get("pip_val", 10.0)
        lots     = units / contract if contract > 0 else units
        return lots * pip_val * 100  # 100 pip range as notional proxy
    except Exception:
        return units * entry if entry else 0

# ── Defaults (all overridable via constructor) ─────────────────────────────────
_MAX_SINGLE_ASSET_PCT   = PORTFOLIO_MAX_SINGLE_ASSET_PCT
_MAX_CATEGORY_PCT       = PORTFOLIO_MAX_CATEGORY_PCT
_MAX_CORRELATION        = 0.85   # block new position if corr with open pos > this
_MAX_SAME_DIRECTION_POSITIONS = PORTFOLIO_MAX_SAME_DIRECTION_POSITIONS
_CORRELATION_CATEGORY_TRIGGER_PCT = PORTFOLIO_CORRELATION_CATEGORY_TRIGGER_PCT
_DRAWDOWN_HALT_PCT      = DRAWDOWN_HALT_PERCENT
_DRAWDOWN_REDUCE_PCT    = DRAWDOWN_REDUCE_PERCENT

_TARGET_ALLOCATION = {
    "crypto":      40.0,
    "forex":       30.0,
    "commodities": 20.0,
    "indices":      5.0,
    "stocks":       5.0,
}
_ALLOCATION_TOLERANCE = 10.0   # ±10% from target is acceptable


def _signal_exposure(signal: dict, asset: str, category: str) -> float:
    return _lot_exposure(
        asset,
        category,
        float(signal.get("position_size", 0) or 0),
        float(signal.get("entry_price", 0) or 0),
    )


class PortfolioRiskEngine:
    """
    Evaluates a proposed signal against the current portfolio state
    and returns (approved: bool, reason: str).
    """

    def __init__(
        self,
        max_single_asset_pct: float  = _MAX_SINGLE_ASSET_PCT,
        max_category_pct: float      = _MAX_CATEGORY_PCT,
        max_same_direction_positions: int = _MAX_SAME_DIRECTION_POSITIONS,
        correlation_category_trigger_pct: float = _CORRELATION_CATEGORY_TRIGGER_PCT,
        drawdown_halt_pct: float     = _DRAWDOWN_HALT_PCT,
        drawdown_reduce_pct: float   = _DRAWDOWN_REDUCE_PCT,
        target_allocation: Optional[Dict[str, float]] = None,
    ):
        self._max_asset    = max_single_asset_pct
        self._max_cat      = max_category_pct
        self._max_same_dir = max(1, int(max_same_direction_positions))
        self._corr_cat_trigger_pct = max(0.0, float(correlation_category_trigger_pct))
        self._dd_halt      = drawdown_halt_pct
        self._dd_reduce    = min(drawdown_reduce_pct, max(0.0, drawdown_halt_pct - 0.1))
        self._targets      = target_allocation or dict(_TARGET_ALLOCATION)
        self._lock         = threading.RLock()
        self._peak_balance = 0.0

    def evaluate(
        self,
        signal: dict,
        open_positions: List[dict],
        balance: float,
        initial_balance: float = 0.0,
        daily_pnl: float = 0.0,
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
            entry    = float(signal.get("entry_price", 0))
            exposure = _signal_exposure(signal, asset, category)
            adjustments: List[str] = []

            def _scale_to_limit(current_exposure: float, limit_pct: float, label: str) -> Tuple[bool, str]:
                nonlocal exposure
                if balance <= 0:
                    return False, "balance unavailable"
                allowed_total = balance * limit_pct / 100.0
                allowed_new = allowed_total - current_exposure
                if allowed_new <= 0:
                    return False, f"{label} already fully allocated"
                if exposure <= allowed_new:
                    return True, ""
                current_size = float(signal.get("position_size", 0) or 0)
                if current_size <= 0 or exposure <= 0:
                    return False, f"{label} position size invalid"
                scale = max(0.0, min(1.0, allowed_new / exposure))
                new_size = current_size * scale
                if new_size <= 0:
                    return False, f"{label} position size reduced below tradable minimum"
                signal["position_size"] = new_size
                exposure = _signal_exposure(signal, asset, category)
                adjustments.append(f"{label} scaled to {scale:.0%} of initial size")
                logger.info(
                    f"[PortfolioRisk] Resized {asset} to fit {label}: "
                    f"size {current_size:.6f} -> {new_size:.6f}, exposure={exposure / balance * 100:.1f}%"
                )
                return True, ""

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
                    gap = max(0.1, self._dd_halt - self._dd_reduce)
                    scale = 1.0 - (drawdown_pct - self._dd_reduce) / gap
                    current_size = float(signal.get("position_size", 0) or 0)
                    signal["position_size"] = current_size * max(0.25, scale)
                    exposure = _signal_exposure(signal, asset, category)
                    logger.info(
                        f"[PortfolioRisk] Scaling position to {scale:.0%} "
                        f"due to drawdown {drawdown_pct:.1f}%"
                    )
                    adjustments.append(f"drawdown scaled to {max(0.25, scale):.0%}")

            # ── 2. Single-asset exposure ──────────────────────────────────
            asset_exposure = sum(
                _lot_exposure(p.get("asset",""), p.get("category","forex"),
                              float(p.get("position_size", 0)), float(p.get("entry_price", 0)))
                for p in open_positions
                if p.get("asset") == asset
            )
            if balance > 0:
                asset_pct = (asset_exposure + exposure) / balance * 100
                if asset_pct > self._max_asset:
                    ok, reason = _scale_to_limit(asset_exposure, self._max_asset, f"asset {asset}")
                    if not ok:
                        return False, (
                            f"Asset exposure {asset_pct:.1f}% > max {self._max_asset}% "
                            f"for {asset}: {reason}"
                        )

            # ── 3. Category exposure ──────────────────────────────────────
            cat_exposure = sum(
                _lot_exposure(p.get("asset",""), p.get("category","forex"),
                              float(p.get("position_size", 0)), float(p.get("entry_price", 0)))
                for p in open_positions
                if p.get("category") == category
            )
            if balance > 0:
                cat_pct = (cat_exposure + exposure) / balance * 100
                if cat_pct > self._max_cat:
                    ok, reason = _scale_to_limit(cat_exposure, self._max_cat, f"category {category}")
                    if not ok:
                        return False, (
                            f"Category {category} exposure {cat_pct:.1f}% > max {self._max_cat}%: {reason}"
                        )

            # ── 4. Allocation drift ───────────────────────────────────────
            if category in self._targets and balance > 0:
                new_cat_pct = (cat_exposure + exposure) / balance * 100
                target      = self._targets[category]
                if new_cat_pct > target + _ALLOCATION_TOLERANCE:
                    limit_pct = target + _ALLOCATION_TOLERANCE
                    ok, reason = _scale_to_limit(cat_exposure, limit_pct, f"allocation {category}")
                    if not ok:
                        return False, (
                            f"Category {category} would be {new_cat_pct:.1f}% "
                            f"(target {target}% ±{_ALLOCATION_TOLERANCE}%): {reason}"
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
            projected_cat_pct = (
                (cat_exposure + exposure) / balance * 100
                if balance > 0 else 100.0
            )
            correlation_trigger_pct = self._max_cat * (self._corr_cat_trigger_pct / 100.0)
            if (
                len(same_dir_cat) >= self._max_same_dir
                and projected_cat_pct >= correlation_trigger_pct
            ):
                return False, (
                    f"Correlation risk: already {len(same_dir_cat)} {direction} "
                    f"positions in {category} with category exposure {projected_cat_pct:.1f}% "
                    f">= trigger {correlation_trigger_pct:.1f}%"
                )
            if len(same_dir_cat) >= self._max_same_dir:
                adjustments.append(
                    f"correlation watch: {len(same_dir_cat)} existing {direction} {category} positions"
                )

        return True, "; ".join(adjustments)

    def get_portfolio_stats(
        self,
        open_positions: List[dict],
        balance: float,
    ) -> dict:
        """Returns current exposure breakdown for the dashboard."""
        total_exposure = sum(
            _lot_exposure(p.get("asset",""), p.get("category","forex"),
                          float(p.get("position_size", 0)), float(p.get("entry_price", 0)))
            for p in open_positions
        )
        by_category: Dict[str, float] = {}
        for p in open_positions:
            cat = p.get("category", "unknown")
            exp = _lot_exposure(p.get("asset",""), cat,
                                float(p.get("position_size", 0)), float(p.get("entry_price", 0)))
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
