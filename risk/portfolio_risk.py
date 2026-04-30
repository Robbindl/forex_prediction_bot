from __future__ import annotations

import math
import threading
from typing import Dict, List, Optional, Tuple
from config.config import (
    DRAWDOWN_HALT_PERCENT,
    MAX_CORRELATION_THRESHOLD,
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
_MAX_CORRELATION        = max(0.0, min(1.0, float(MAX_CORRELATION_THRESHOLD)))
_MAX_SAME_DIRECTION_POSITIONS = PORTFOLIO_MAX_SAME_DIRECTION_POSITIONS
_CORRELATION_CATEGORY_TRIGGER_PCT = PORTFOLIO_CORRELATION_CATEGORY_TRIGGER_PCT
_DRAWDOWN_HALT_PCT      = DRAWDOWN_HALT_PERCENT
_DRAWDOWN_REDUCE_PCT    = DRAWDOWN_REDUCE_PERCENT
_CORRELATION_INTERVAL   = "1d"
_CORRELATION_LOOKBACK_PERIODS = 60
_MIN_CORRELATION_OBSERVATIONS = 20

_TARGET_ALLOCATION = {
    "crypto":      40.0,
    "forex":       30.0,
    "commodities": 20.0,
    "indices":      5.0,
    "stocks":       5.0,
}
_ALLOCATION_TOLERANCE = 10.0   # ±10% from target is acceptable
_MACRO_THEME_TRIGGER_FLOOR = 30.0


def _direction_sign(direction: str) -> int:
    token = str(direction or "BUY").strip().upper()
    return -1 if token == "SELL" else 1


def _load_local_correlation_frame(asset: str, category: str):
    try:
        from services.local_candle_store import local_candle_store
    except Exception:
        return None
    if not local_candle_store.enabled():
        return None
    try:
        frame, _ = local_candle_store.get_ohlcv(
            asset,
            category,
            _CORRELATION_INTERVAL,
            _CORRELATION_LOOKBACK_PERIODS,
            closed_only=True,
        )
    except Exception:
        return None
    return frame


def _asset_pair_correlation_from_local_store(
    asset: str,
    category: str,
    other_asset: str,
    other_category: str,
) -> Optional[Tuple[float, int]]:
    left = _load_local_correlation_frame(asset, category)
    right = _load_local_correlation_frame(other_asset, other_category)
    if left is None or right is None:
        return None
    try:
        import pandas as pd

        left_close = pd.to_numeric(left.get("close"), errors="coerce").dropna()
        right_close = pd.to_numeric(right.get("close"), errors="coerce").dropna()
        if left_close.empty or right_close.empty:
            return None
        left_close.index = pd.to_datetime(left_close.index, utc=True, errors="coerce")
        right_close.index = pd.to_datetime(right_close.index, utc=True, errors="coerce")
        merged = pd.concat(
            [left_close.rename("left"), right_close.rename("right")],
            axis=1,
            join="inner",
        ).dropna()
        if len(merged) < _MIN_CORRELATION_OBSERVATIONS:
            return None
        returns = merged.pct_change().replace([math.inf, -math.inf], pd.NA).dropna()
        if len(returns) < max(12, _MIN_CORRELATION_OBSERVATIONS // 2):
            return None
        correlation = returns["left"].corr(returns["right"])
    except Exception:
        return None
    if correlation is None or not math.isfinite(float(correlation)):
        return None
    return float(correlation), int(len(returns))


def _signal_exposure(signal: dict, asset: str, category: str) -> float:
    return _lot_exposure(
        asset,
        category,
        float(signal.get("position_size", 0) or 0),
        float(signal.get("entry_price", 0) or 0),
    )


def _portfolio_asset_exposure(open_positions: List[dict], asset: str) -> float:
    return sum(
        _lot_exposure(
            p.get("asset", ""),
            p.get("category", "forex"),
            float(p.get("position_size", 0)),
            float(p.get("entry_price", 0)),
        )
        for p in open_positions
        if p.get("asset") == asset
    )


def _portfolio_category_exposure(open_positions: List[dict], category: str) -> float:
    return sum(
        _lot_exposure(
            p.get("asset", ""),
            p.get("category", "forex"),
            float(p.get("position_size", 0)),
            float(p.get("entry_price", 0)),
        )
        for p in open_positions
        if p.get("category") == category
    )


def _macro_theme(asset: str, category: str, direction: str) -> str:
    asset_token = str(asset or "").strip().upper()
    category_token = str(category or "").strip().lower()
    direction_token = str(direction or "BUY").strip().upper()
    if direction_token not in {"BUY", "SELL"}:
        direction_token = "BUY"

    if category_token in {"crypto", "indices"}:
        return "risk_on" if direction_token == "BUY" else "risk_off"

    if asset_token in {"WTI", "BRENT", "USOIL", "UKOIL"}:
        return "risk_on" if direction_token == "BUY" else "risk_off"

    if asset_token in {"XAU/USD", "XAG/USD"}:
        return "risk_off" if direction_token == "BUY" else "risk_on"

    if category_token == "forex":
        if asset_token.startswith("USD/"):
            return "usd_strength" if direction_token == "BUY" else "usd_weakness"
        if asset_token.endswith("/USD"):
            return "usd_weakness" if direction_token == "BUY" else "usd_strength"
        if asset_token.endswith("/JPY") or asset_token.endswith("/CHF"):
            base = asset_token.split("/", 1)[0]
            if base and base not in {"USD"}:
                return "risk_on" if direction_token == "BUY" else "risk_off"

    return ""


def _portfolio_macro_theme_exposure(open_positions: List[dict], macro_theme: str) -> float:
    if not macro_theme:
        return 0.0
    return sum(
        _lot_exposure(
            p.get("asset", ""),
            p.get("category", "forex"),
            float(p.get("position_size", 0)),
            float(p.get("entry_price", 0)),
        )
        for p in open_positions
        if _macro_theme(
            p.get("asset", ""),
            p.get("category", "unknown"),
            p.get("direction") or p.get("signal", "BUY"),
        ) == macro_theme
    )


def _portfolio_scale_to_limit(
    signal: dict,
    *,
    asset: str,
    category: str,
    balance: float,
    current_exposure: float,
    limit_pct: float,
    label: str,
    exposure: float,
    adjustments: List[str],
) -> tuple[bool, str, float]:
    if balance <= 0:
        return False, "balance unavailable", exposure
    allowed_total = balance * limit_pct / 100.0
    allowed_new = allowed_total - current_exposure
    if allowed_new <= 0:
        return False, f"{label} already fully allocated", exposure
    if exposure <= allowed_new:
        return True, "", exposure
    current_size = float(signal.get("position_size", 0) or 0)
    if current_size <= 0 or exposure <= 0:
        return False, f"{label} position size invalid", exposure
    scale = max(0.0, min(1.0, allowed_new / exposure))
    new_size = current_size * scale
    if new_size <= 0:
        return False, f"{label} position size reduced below tradable minimum", exposure
    signal["position_size"] = new_size
    new_exposure = _signal_exposure(signal, asset, category)
    adjustments.append(f"{label} scaled to {scale:.0%} of initial size")
    logger.info(
        f"[PortfolioRisk] Resized {asset} to fit {label}: "
        f"size {current_size:.6f} -> {new_size:.6f}, exposure={new_exposure / balance * 100:.1f}%"
    )
    return True, "", new_exposure


def _portfolio_apply_drawdown_limits(
    engine: "PortfolioRiskEngine",
    signal: dict,
    *,
    asset: str,
    category: str,
    balance: float,
    exposure: float,
    adjustments: List[str],
) -> tuple[bool, str, float]:
    if engine._peak_balance <= 0:
        return True, "", exposure
    drawdown_pct = (engine._peak_balance - balance) / engine._peak_balance * 100
    if drawdown_pct >= engine._dd_halt:
        return False, f"Portfolio drawdown {drawdown_pct:.1f}% >= halt threshold {engine._dd_halt}%", exposure
    if drawdown_pct >= engine._dd_reduce:
        gap = max(0.1, engine._dd_halt - engine._dd_reduce)
        scale = 1.0 - (drawdown_pct - engine._dd_reduce) / gap
        current_size = float(signal.get("position_size", 0) or 0)
        signal["position_size"] = current_size * max(0.25, scale)
        new_exposure = _signal_exposure(signal, asset, category)
        logger.info(
            f"[PortfolioRisk] Scaling position to {scale:.0%} due to drawdown {drawdown_pct:.1f}%"
        )
        adjustments.append(f"drawdown scaled to {max(0.25, scale):.0%}")
        return True, "", new_exposure
    return True, "", exposure


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
        correlation_threshold: float = _MAX_CORRELATION,
        drawdown_halt_pct: float     = _DRAWDOWN_HALT_PCT,
        drawdown_reduce_pct: float   = _DRAWDOWN_REDUCE_PCT,
        target_allocation: Optional[Dict[str, float]] = None,
    ):
        self._max_asset    = max_single_asset_pct
        self._max_cat      = max_category_pct
        self._max_same_dir = max(1, int(max_same_direction_positions))
        self._corr_cat_trigger_pct = max(0.0, float(correlation_category_trigger_pct))
        self._corr_threshold = max(0.0, min(1.0, float(correlation_threshold)))
        self._dd_halt      = drawdown_halt_pct
        self._dd_reduce    = min(drawdown_reduce_pct, max(0.0, drawdown_halt_pct - 0.1))
        self._targets      = target_allocation or dict(_TARGET_ALLOCATION)
        self._lock         = threading.RLock()
        self._correlation_cache: Dict[Tuple[Tuple[str, str], Tuple[str, str]], Optional[Tuple[float, int]]] = {}
        self._peak_balance = 0.0

    def _pair_correlation(
        self,
        asset: str,
        category: str,
        other_asset: str,
        other_category: str,
    ) -> Optional[Tuple[float, int]]:
        left = (str(asset or "").strip().upper(), str(category or "").strip().lower())
        right = (str(other_asset or "").strip().upper(), str(other_category or "").strip().lower())
        cache_key = tuple(sorted((left, right)))
        if cache_key not in self._correlation_cache:
            self._correlation_cache[cache_key] = _asset_pair_correlation_from_local_store(
                left[0],
                left[1],
                right[0],
                right[1],
            )
        return self._correlation_cache.get(cache_key)

    def _correlated_reinforcing_positions(
        self,
        *,
        asset: str,
        category: str,
        direction: str,
        open_positions: List[dict],
    ) -> Tuple[List[Dict[str, float]], int]:
        candidate_sign = _direction_sign(direction)
        matches: List[Dict[str, float]] = []
        reviewed_pairs = 0
        for position in open_positions:
            other_asset = str(position.get("asset", "") or "")
            other_category = str(position.get("category", "unknown") or "unknown")
            if not other_asset or other_asset == asset:
                continue
            correlation_info = self._pair_correlation(asset, category, other_asset, other_category)
            if correlation_info is None:
                continue
            reviewed_pairs += 1
            correlation, sample_count = correlation_info
            if abs(correlation) < self._corr_threshold:
                continue
            other_direction = str(position.get("direction") or position.get("signal", "BUY") or "BUY")
            reinforcing = correlation * candidate_sign * _direction_sign(other_direction) > 0
            if not reinforcing:
                continue
            matches.append(
                {
                    "asset": other_asset,
                    "category": other_category,
                    "direction": str(other_direction).upper(),
                    "correlation": float(correlation),
                    "samples": int(sample_count),
                    "exposure": _lot_exposure(
                        other_asset,
                        other_category,
                        float(position.get("position_size", 0) or 0),
                        float(position.get("entry_price", 0) or 0),
                    ),
                }
            )
        matches.sort(key=lambda item: abs(float(item.get("correlation", 0.0))), reverse=True)
        return matches, reviewed_pairs

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

            ok, reason, exposure = _portfolio_apply_drawdown_limits(
                self,
                signal,
                asset=asset,
                category=category,
                balance=balance,
                exposure=exposure,
                adjustments=adjustments,
            )
            if not ok:
                return False, reason

            # ── 2. Single-asset exposure ──────────────────────────────────
            asset_exposure = _portfolio_asset_exposure(open_positions, asset)
            if balance > 0:
                asset_pct = (asset_exposure + exposure) / balance * 100
                if asset_pct > self._max_asset:
                    ok, reason, exposure = _portfolio_scale_to_limit(
                        signal,
                        asset=asset,
                        category=category,
                        balance=balance,
                        current_exposure=asset_exposure,
                        limit_pct=self._max_asset,
                        label=f"asset {asset}",
                        exposure=exposure,
                        adjustments=adjustments,
                    )
                    if not ok:
                        return False, (
                            f"Asset exposure {asset_pct:.1f}% > max {self._max_asset}% "
                            f"for {asset}: {reason}"
                        )

            # ── 3. Category exposure ──────────────────────────────────────
            cat_exposure = _portfolio_category_exposure(open_positions, category)
            if balance > 0:
                cat_pct = (cat_exposure + exposure) / balance * 100
                if cat_pct > self._max_cat:
                    ok, reason, exposure = _portfolio_scale_to_limit(
                        signal,
                        asset=asset,
                        category=category,
                        balance=balance,
                        current_exposure=cat_exposure,
                        limit_pct=self._max_cat,
                        label=f"category {category}",
                        exposure=exposure,
                        adjustments=adjustments,
                    )
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
                    ok, reason, exposure = _portfolio_scale_to_limit(
                        signal,
                        asset=asset,
                        category=category,
                        balance=balance,
                        current_exposure=cat_exposure,
                        limit_pct=limit_pct,
                        label=f"allocation {category}",
                        exposure=exposure,
                        adjustments=adjustments,
                    )
                    if not ok:
                        return False, (
                            f"Category {category} would be {new_cat_pct:.1f}% "
                            f"(target {target}% ±{_ALLOCATION_TOLERANCE}%): {reason}"
                        )

            # ── 5. Correlation block ──────────────────────────────────────
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
            correlated_positions, reviewed_pairs = self._correlated_reinforcing_positions(
                asset=asset,
                category=category,
                direction=direction,
                open_positions=open_positions,
            )
            if correlated_positions:
                correlated_exposure = sum(float(item.get("exposure", 0.0) or 0.0) for item in correlated_positions)
                projected_corr_pct = (
                    (correlated_exposure + exposure) / balance * 100
                    if balance > 0 else 100.0
                )
                lead = correlated_positions[0]
                if projected_corr_pct >= correlation_trigger_pct:
                    related_assets = ", ".join(str(item.get("asset", "")) for item in correlated_positions[:3])
                    if len(correlated_positions) > 3:
                        related_assets = f"{related_assets}, +{len(correlated_positions) - 3} more"
                    return False, (
                        f"Correlation risk: {asset} {direction} reinforces {related_assets} "
                        f"(lead corr {float(lead.get('correlation', 0.0)):+.2f} over {int(lead.get('samples', 0))} bars) "
                        f"with correlated exposure {projected_corr_pct:.1f}% >= trigger {correlation_trigger_pct:.1f}%"
                    )
                adjustments.append(
                    f"correlation watch: {len(correlated_positions)} reinforcing pair(s), lead "
                    f"{lead.get('asset', '')} corr {float(lead.get('correlation', 0.0)):+.2f}"
                )
            if (
                len(same_dir_cat) >= self._max_same_dir
                and projected_cat_pct >= correlation_trigger_pct
            ):
                return False, (
                    f"Correlation risk: already {len(same_dir_cat)} {direction} "
                    f"positions in {category} with category exposure {projected_cat_pct:.1f}% "
                    f">= trigger {correlation_trigger_pct:.1f}%"
                )
            if len(same_dir_cat) >= self._max_same_dir and not correlated_positions and reviewed_pairs == 0:
                adjustments.append(
                    f"correlation watch: {len(same_dir_cat)} existing {direction} {category} positions"
                )

            macro_theme = _macro_theme(asset, category, direction)
            if macro_theme:
                same_macro = [
                    p for p in open_positions
                    if _macro_theme(
                        p.get("asset", ""),
                        p.get("category", "unknown"),
                        p.get("direction") or p.get("signal", "BUY"),
                    ) == macro_theme
                ]
                macro_exposure = _portfolio_macro_theme_exposure(open_positions, macro_theme)
                projected_macro_pct = (
                    (macro_exposure + exposure) / balance * 100
                    if balance > 0 else 100.0
                )
                macro_trigger_pct = max(
                    _MACRO_THEME_TRIGGER_FLOOR,
                    self._max_cat * (self._corr_cat_trigger_pct / 100.0),
                )
                macro_limit = max(3, self._max_same_dir)
                if (
                    len(same_macro) >= macro_limit
                    and projected_macro_pct >= macro_trigger_pct
                ):
                    categories = sorted({str(p.get("category", "unknown")) for p in same_macro} | {category})
                    return False, (
                        f"Macro correlation risk: already {len(same_macro)} {macro_theme} positions across "
                        f"{', '.join(categories)} with theme exposure {projected_macro_pct:.1f}% "
                        f">= trigger {macro_trigger_pct:.1f}%"
                    )
                if len(same_macro) >= max(2, macro_limit - 1):
                    adjustments.append(
                        f"macro watch: {len(same_macro)} open {macro_theme} positions across categories"
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
