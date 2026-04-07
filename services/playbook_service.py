from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import pandas as pd


def _clip(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, float(value)))


def _utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)


def _active_session(*, category: str = "") -> str:
    now = _utc_now()
    hour = now.hour
    weekday = now.weekday()
    category_key = str(category or "").strip().lower()
    if category_key == "crypto":
        if 0 <= hour < 6:
            return "asia_core"
        if 6 <= hour < 14:
            return "europe_open" if hour < 8 else "europe_core"
        if 14 <= hour < 16:
            return "us_overlap"
        if 16 <= hour < 19:
            return "us_open"
        return "us_core"
    if weekday == 5 or weekday == 6:
        if weekday == 6 and hour >= 22:
            return "asia_core"
        return "off"
    if weekday == 4 and hour >= 22:
        return "off"
    if 0 <= hour < 6:
        return "asia_core"
    if 6 <= hour < 8:
        return "europe_open"
    if 8 <= hour < 13:
        return "europe_core"
    if 13 <= hour < 15:
        return "us_overlap"
    if 15 <= hour < 17:
        return "us_open"
    if 17 <= hour < 22:
        return "us_core"
    return "off"


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _session_matches(current: str, allowed: str) -> bool:
    current_label = str(current or "").strip().lower()
    allowed_label = str(allowed or "").strip().lower()
    if not current_label or not allowed_label:
        return False
    if current_label == allowed_label:
        return True

    broad_windows = {
        "asia": {"asia_core"},
        "europe": {"europe_open", "europe_core", "us_overlap"},
        "us": {"us_overlap", "us_open", "us_core"},
    }
    if allowed_label in broad_windows:
        return current_label in broad_windows[allowed_label]
    return False


def _news_direction_sign(raw_direction: Any) -> int:
    label = str(raw_direction or "").strip().lower()
    if label in {"buy", "bullish", "up", "long", "risk_on"}:
        return 1
    if label in {"sell", "bearish", "down", "short", "risk_off"}:
        return -1
    return 0


@dataclass(frozen=True)
class _PlaybookProfile:
    breakout_min_score: float
    pullback_min_score: float
    retest_min_score: float
    reversal_min_score: float
    expansion_min_score: float
    seed_min_confidence: float
    support_min_confidence: float
    override_min_confidence: float
    override_gap: float
    weak_ml_confidence: float
    breakout_lookback: int
    preferred_interval: str
    allowed_sessions: tuple[str, ...]
    retest_window: int
    retest_tolerance_atr: float
    runner_target_rr: float
    trail_activation_rr: float
    trail_atr_multiple: float


@dataclass(frozen=True)
class _AssetPlaybookPlan:
    allowed_playbooks: tuple[str, ...]
    allowed_sessions: tuple[str, ...]
    min_alignment_score: float
    min_setup_quality: float
    min_trend_agreement: int
    reversal_min_opposing_trend_agreement: int


_CATEGORY_PROFILES: Dict[str, _PlaybookProfile] = {
    "forex": _PlaybookProfile(0.56, 0.58, 0.57, 0.57, 0.58, 0.58, 0.52, 0.66, 0.12, 0.32, 18, "5m", ("europe", "us"), 3, 0.18, 2.1, 1.0, 0.75),
    "crypto": _PlaybookProfile(0.58, 0.60, 0.58, 0.59, 0.60, 0.60, 0.54, 0.68, 0.10, 0.36, 20, "5m", ("asia", "europe", "us"), 4, 0.25, 2.6, 1.0, 1.15),
    "commodities": _PlaybookProfile(0.57, 0.58, 0.57, 0.58, 0.58, 0.59, 0.53, 0.67, 0.11, 0.34, 18, "5m", ("europe", "us"), 3, 0.22, 2.2, 1.0, 0.95),
    "indices": _PlaybookProfile(0.57, 0.59, 0.57, 0.58, 0.58, 0.59, 0.53, 0.67, 0.11, 0.34, 18, "5m", ("us",), 3, 0.20, 2.0, 1.0, 0.90),
}

_TREND_PLAYBOOKS = {
    "breakout_continuation",
    "breakout_retest",
    "trend_pullback",
    "aggressive_expansion",
    "opening_drive",
    "news_impulse",
    "crypto_orderflow_continuation",
}

_REVERSAL_PLAYBOOKS = {
    "reversal_exhaustion",
    "failed_break_reclaim",
}

_CATEGORY_PLANS: Dict[str, _AssetPlaybookPlan] = {
    "forex": _AssetPlaybookPlan(
        ("breakout_continuation", "breakout_retest", "trend_pullback", "failed_break_reclaim", "reversal_exhaustion", "aggressive_expansion", "news_impulse"),
        ("europe", "us"),
        0.56,
        0.54,
        1,
        1,
    ),
    "crypto": _AssetPlaybookPlan(
        ("breakout_continuation", "breakout_retest", "trend_pullback", "aggressive_expansion", "crypto_orderflow_continuation"),
        ("europe", "us"),
        0.58,
        0.56,
        1,
        1,
    ),
    "commodities": _AssetPlaybookPlan(
        ("breakout_continuation", "breakout_retest", "trend_pullback", "failed_break_reclaim", "reversal_exhaustion", "aggressive_expansion", "news_impulse"),
        ("europe", "us"),
        0.57,
        0.55,
        1,
        1,
    ),
    "indices": _AssetPlaybookPlan(
        ("breakout_continuation", "breakout_retest", "trend_pullback", "failed_break_reclaim", "aggressive_expansion", "opening_drive"),
        ("us",),
        0.58,
        0.56,
        1,
        1,
    ),
}

_ASSET_PLANS: Dict[str, _AssetPlaybookPlan] = {
    "EUR/USD": _AssetPlaybookPlan(
        ("breakout_continuation", "breakout_retest", "trend_pullback", "failed_break_reclaim", "reversal_exhaustion", "aggressive_expansion", "news_impulse"),
        ("europe_open", "europe_core", "us_overlap", "us_open"),
        0.60,
        0.58,
        2,
        1,
    ),
    "GBP/USD": _AssetPlaybookPlan(
        ("breakout_continuation", "breakout_retest", "trend_pullback", "failed_break_reclaim", "reversal_exhaustion", "aggressive_expansion", "news_impulse"),
        ("europe_open", "europe_core", "us_overlap", "us_open"),
        0.60,
        0.58,
        2,
        1,
    ),
    "USD/JPY": _AssetPlaybookPlan(
        ("breakout_continuation", "breakout_retest", "trend_pullback", "failed_break_reclaim", "reversal_exhaustion", "aggressive_expansion", "news_impulse"),
        ("asia_core", "europe_open", "europe_core", "us_overlap", "us_open"),
        0.59,
        0.57,
        2,
        1,
    ),
    "EUR/JPY": _AssetPlaybookPlan(
        ("breakout_continuation", "breakout_retest", "trend_pullback", "failed_break_reclaim", "aggressive_expansion"),
        ("europe_open", "europe_core", "us_overlap", "us_open"),
        0.61,
        0.59,
        2,
        1,
    ),
    "GBP/JPY": _AssetPlaybookPlan(
        ("breakout_continuation", "breakout_retest", "trend_pullback", "failed_break_reclaim", "aggressive_expansion"),
        ("europe_open", "europe_core", "us_overlap", "us_open"),
        0.62,
        0.60,
        2,
        1,
    ),
    "AUD/USD": _AssetPlaybookPlan(
        ("breakout_continuation", "breakout_retest", "trend_pullback", "aggressive_expansion"),
        ("asia_core", "europe_open", "europe_core", "us_overlap"),
        0.60,
        0.58,
        2,
        1,
    ),
    "USD/CAD": _AssetPlaybookPlan(
        ("breakout_continuation", "breakout_retest", "trend_pullback", "aggressive_expansion", "news_impulse"),
        ("europe_core", "us_overlap", "us_open", "us_core"),
        0.60,
        0.58,
        2,
        1,
    ),
    "XAU/USD": _AssetPlaybookPlan(
        ("breakout_continuation", "breakout_retest", "trend_pullback", "failed_break_reclaim", "reversal_exhaustion", "aggressive_expansion", "news_impulse"),
        ("europe_open", "europe_core", "us_overlap", "us_open", "us_core"),
        0.58,
        0.57,
        1,
        1,
    ),
    "XAG/USD": _AssetPlaybookPlan(
        ("breakout_continuation", "breakout_retest", "trend_pullback", "failed_break_reclaim", "aggressive_expansion", "news_impulse"),
        ("europe_open", "europe_core", "us_overlap", "us_open", "us_core"),
        0.59,
        0.58,
        1,
        1,
    ),
    "WTI": _AssetPlaybookPlan(
        ("breakout_continuation", "breakout_retest", "aggressive_expansion", "opening_drive", "news_impulse"),
        ("us_open", "us_core"),
        0.60,
        0.58,
        2,
        1,
    ),
    "US30": _AssetPlaybookPlan(
        ("breakout_continuation", "breakout_retest", "trend_pullback", "failed_break_reclaim", "aggressive_expansion", "opening_drive"),
        ("us_open", "us_core"),
        0.59,
        0.57,
        2,
        1,
    ),
    "US100": _AssetPlaybookPlan(
        ("breakout_continuation", "breakout_retest", "trend_pullback", "failed_break_reclaim", "aggressive_expansion", "opening_drive"),
        ("us_open", "us_core"),
        0.59,
        0.57,
        2,
        1,
    ),
    "US500": _AssetPlaybookPlan(
        ("breakout_continuation", "breakout_retest", "trend_pullback", "failed_break_reclaim", "aggressive_expansion", "opening_drive"),
        ("us_open", "us_core"),
        0.59,
        0.57,
        2,
        1,
    ),
    "UK100": _AssetPlaybookPlan(
        ("breakout_continuation", "breakout_retest", "trend_pullback", "failed_break_reclaim", "aggressive_expansion", "opening_drive"),
        ("europe_open", "europe_core"),
        0.58,
        0.56,
        2,
        1,
    ),
    "BTC-USD": _AssetPlaybookPlan(
        ("breakout_continuation", "breakout_retest", "trend_pullback", "aggressive_expansion", "crypto_orderflow_continuation"),
        ("asia_core", "europe_open", "europe_core", "us_overlap", "us_open", "us_core"),
        0.60,
        0.58,
        1,
        1,
    ),
    "ETH-USD": _AssetPlaybookPlan(
        ("breakout_continuation", "breakout_retest", "trend_pullback", "aggressive_expansion", "crypto_orderflow_continuation"),
        ("asia_core", "europe_open", "europe_core", "us_overlap", "us_open", "us_core"),
        0.60,
        0.58,
        1,
        1,
    ),
    "BNB-USD": _AssetPlaybookPlan(
        ("breakout_continuation", "breakout_retest", "aggressive_expansion", "crypto_orderflow_continuation"),
        ("europe_core", "us_overlap", "us_open", "us_core"),
        0.62,
        0.60,
        2,
        1,
    ),
    "SOL-USD": _AssetPlaybookPlan(
        ("breakout_continuation", "breakout_retest", "aggressive_expansion", "crypto_orderflow_continuation"),
        ("europe_core", "us_overlap", "us_open", "us_core"),
        0.62,
        0.60,
        2,
        1,
    ),
    "XRP-USD": _AssetPlaybookPlan(
        ("breakout_continuation", "breakout_retest", "aggressive_expansion", "crypto_orderflow_continuation"),
        ("europe_core", "us_overlap", "us_open", "us_core"),
        0.63,
        0.61,
        2,
        1,
    ),
}

_ASSET_MANAGEMENT_OVERRIDES: Dict[str, Dict[str, Any]] = {
    "EUR/USD": {"preferred_interval": "5m", "runner_target_rr": 2.0, "trail_activation_rr": 0.9, "trail_atr_multiple": 0.65},
    "GBP/USD": {"preferred_interval": "5m", "runner_target_rr": 2.1, "trail_activation_rr": 0.9, "trail_atr_multiple": 0.68},
    "USD/JPY": {"preferred_interval": "5m", "runner_target_rr": 2.2, "trail_activation_rr": 0.95, "trail_atr_multiple": 0.70},
    "EUR/JPY": {"preferred_interval": "15m", "runner_target_rr": 2.2, "trail_activation_rr": 1.0, "trail_atr_multiple": 0.78},
    "GBP/JPY": {"preferred_interval": "15m", "runner_target_rr": 2.3, "trail_activation_rr": 1.0, "trail_atr_multiple": 0.80},
    "AUD/USD": {"preferred_interval": "15m", "runner_target_rr": 2.0, "trail_activation_rr": 0.95, "trail_atr_multiple": 0.70},
    "USD/CAD": {"preferred_interval": "5m", "runner_target_rr": 2.0, "trail_activation_rr": 0.95, "trail_atr_multiple": 0.72},
    "XAU/USD": {"preferred_interval": "5m", "runner_target_rr": 2.4, "trail_activation_rr": 1.0, "trail_atr_multiple": 0.85},
    "XAG/USD": {"preferred_interval": "5m", "runner_target_rr": 2.6, "trail_activation_rr": 1.0, "trail_atr_multiple": 0.95},
    "WTI": {"preferred_interval": "15m", "runner_target_rr": 2.7, "trail_activation_rr": 1.1, "trail_atr_multiple": 1.05},
    "US30": {"preferred_interval": "5m", "runner_target_rr": 2.0, "trail_activation_rr": 0.85, "trail_atr_multiple": 0.80},
    "US100": {"preferred_interval": "5m", "runner_target_rr": 2.2, "trail_activation_rr": 0.85, "trail_atr_multiple": 0.82},
    "US500": {"preferred_interval": "5m", "runner_target_rr": 1.9, "trail_activation_rr": 0.85, "trail_atr_multiple": 0.75},
    "UK100": {"preferred_interval": "5m", "runner_target_rr": 1.9, "trail_activation_rr": 0.85, "trail_atr_multiple": 0.75},
    "BTC-USD": {"preferred_interval": "5m", "runner_target_rr": 2.8, "trail_activation_rr": 1.1, "trail_atr_multiple": 1.15},
    "ETH-USD": {"preferred_interval": "5m", "runner_target_rr": 2.7, "trail_activation_rr": 1.05, "trail_atr_multiple": 1.12},
    "BNB-USD": {"preferred_interval": "15m", "runner_target_rr": 3.0, "trail_activation_rr": 1.15, "trail_atr_multiple": 1.18},
    "SOL-USD": {"preferred_interval": "15m", "runner_target_rr": 3.1, "trail_activation_rr": 1.15, "trail_atr_multiple": 1.20},
    "XRP-USD": {"preferred_interval": "15m", "runner_target_rr": 3.2, "trail_activation_rr": 1.2, "trail_atr_multiple": 1.25},
}


class PlaybookService:
    def _profile(self, category: str) -> _PlaybookProfile:
        return _CATEGORY_PROFILES.get(str(category or "").strip().lower(), _CATEGORY_PROFILES["forex"])

    def _asset_plan(self, asset: str, category: str) -> _AssetPlaybookPlan:
        canonical = str(asset or "").strip().upper()
        return _ASSET_PLANS.get(canonical, _CATEGORY_PLANS.get(str(category or "").strip().lower(), _CATEGORY_PLANS["forex"]))

    @staticmethod
    def _frame(price_data) -> Optional[pd.DataFrame]:
        if price_data is None or getattr(price_data, "empty", True):
            return None
        frame = price_data.copy()
        frame.columns = [str(c).lower() for c in frame.columns]
        required = {"open", "high", "low", "close"}
        if not required.issubset(set(frame.columns)) or len(frame) < 25:
            return None
        try:
            for col in required:
                frame[col] = frame[col].astype(float)
        except Exception:
            return None
        return frame

    @staticmethod
    def _atr(frame: pd.DataFrame, period: int = 14) -> float:
        if frame is None or len(frame) < period + 1:
            return 0.0
        high = frame["high"]
        low = frame["low"]
        close = frame["close"]
        prev_close = close.shift(1)
        tr = pd.concat(
            [
                high - low,
                (high - prev_close).abs(),
                (low - prev_close).abs(),
            ],
            axis=1,
        ).max(axis=1)
        try:
            return float(tr.tail(period).mean())
        except Exception:
            return 0.0

    def _management_template(
        self,
        profile: _PlaybookProfile,
        playbook: str,
        *,
        asset: str,
        category: str,
    ) -> Dict[str, Any]:
        canonical = str(asset or "").strip().upper()
        overrides = dict(_ASSET_MANAGEMENT_OVERRIDES.get(canonical, {}))
        preferred_interval = str(overrides.get("preferred_interval") or profile.preferred_interval or "").strip().lower()
        runner_target_rr = _safe_float(overrides.get("runner_target_rr", profile.runner_target_rr), profile.runner_target_rr)
        trail_activation_rr = _safe_float(overrides.get("trail_activation_rr", profile.trail_activation_rr), profile.trail_activation_rr)
        trail_atr_multiple = _safe_float(overrides.get("trail_atr_multiple", profile.trail_atr_multiple), profile.trail_atr_multiple)
        partial_take_profit_rr = [1.0]

        if playbook in _REVERSAL_PLAYBOOKS:
            runner_target_rr = max(1.6, runner_target_rr * 0.88)
            trail_activation_rr = min(trail_activation_rr, 0.9)
        elif playbook == "opening_drive":
            runner_target_rr = max(1.7, runner_target_rr * 0.92)
            trail_activation_rr = min(trail_activation_rr, 0.85)
        elif playbook == "news_impulse":
            runner_target_rr = max(1.8, runner_target_rr * 0.95)
            trail_activation_rr = min(trail_activation_rr, 0.9)
        elif playbook == "crypto_orderflow_continuation":
            runner_target_rr = max(runner_target_rr, 2.6)
            trail_atr_multiple = max(trail_atr_multiple, 1.05)

        return {
            "style": "intraday_playbook",
            "playbook": playbook,
            "asset": canonical,
            "category": str(category or "").strip().lower(),
            "partial_take_profit_rr": partial_take_profit_rr,
            "runner_target_rr": round(float(runner_target_rr), 4),
            "trail_activation_rr": round(float(trail_activation_rr), 4),
            "trail_atr_multiple": round(float(trail_atr_multiple), 4),
            "trail_mode": "extreme_atr",
            "break_even_after_partial": True,
            "preferred_interval": preferred_interval,
        }

    def preferred_interval(self, category: str, asset: str = "") -> str:
        canonical = str(asset or "").strip().upper()
        override = _ASSET_MANAGEMENT_OVERRIDES.get(canonical, {})
        interval = str(override.get("preferred_interval", "") or "").strip().lower()
        if interval:
            return interval
        return self._profile(category).preferred_interval

    @staticmethod
    def _trend_sign(state: str) -> int:
        label = str(state or "").strip().lower()
        if label == "trending_up":
            return 1
        if label == "trending_down":
            return -1
        return 0

    @staticmethod
    def _default_allowed_sessions(
        asset: str,
        category: str,
        profile: _PlaybookProfile,
        plan: _AssetPlaybookPlan,
    ) -> tuple[str, ...]:
        if plan.allowed_sessions:
            return plan.allowed_sessions
        canonical = str(asset or "").strip().upper()
        if str(category or "").strip().lower() == "indices":
            if canonical == "UK100":
                return ("europe",)
            return ("us",)
        return profile.allowed_sessions

    def _session_allowed(self, asset: str, category: str) -> tuple[bool, str, tuple[str, ...]]:
        profile = self._profile(category)
        plan = self._asset_plan(asset, category)
        current = _active_session(category=category)
        allowed = self._default_allowed_sessions(asset, category, profile, plan)
        if not allowed:
            return True, current, allowed
        return any(_session_matches(current, item) for item in allowed), current, allowed

    def _qualify_candidate(
        self,
        candidate: Dict[str, Any],
        *,
        asset: str,
        category: str,
        structure: Dict[str, Any],
        plan: _AssetPlaybookPlan,
    ) -> tuple[bool, str]:
        playbook = str(candidate.get("playbook") or "").strip()
        if playbook not in plan.allowed_playbooks:
            return False, f"playbook_not_allowed:{playbook}"

        direction = str(candidate.get("direction") or "").upper()
        direction_sign = 1 if direction == "BUY" else -1 if direction == "SELL" else 0
        if direction_sign == 0:
            return False, f"invalid_direction:{playbook}"

        structure_bias = str(structure.get("structure_bias", "neutral") or "neutral").lower()
        alignment_score = float(structure.get("alignment_score", 0.0) or 0.0)
        setup_quality = float(structure.get("setup_quality", 0.0) or 0.0)
        trend_15m = str(structure.get("trend_15m", "unknown") or "unknown").lower()
        trend_1h = str(structure.get("trend_1h", "unknown") or "unknown").lower()

        trend_states = (trend_15m, trend_1h)
        aligned_trends = sum(1 for state in trend_states if self._trend_sign(state) == direction_sign)
        opposing_trends = sum(1 for state in trend_states if self._trend_sign(state) == -direction_sign)
        bias_alignment = (
            (structure_bias == "buy" and direction == "BUY")
            or (structure_bias == "sell" and direction == "SELL")
        )

        if alignment_score < plan.min_alignment_score:
            return False, f"alignment_too_weak:{playbook}"
        if setup_quality < plan.min_setup_quality:
            return False, f"setup_quality_too_weak:{playbook}"

        if playbook in _TREND_PLAYBOOKS:
            if structure_bias in {"buy", "sell"} and not bias_alignment:
                return False, f"bias_conflict:{playbook}"
            if aligned_trends < max(0, int(plan.min_trend_agreement or 0)):
                return False, f"trend_misaligned:{playbook}"
        elif playbook in _REVERSAL_PLAYBOOKS:
            if structure_bias in {"buy", "sell"} and bias_alignment:
                return False, f"reversal_not_countertrend:{playbook}"
            if opposing_trends < max(0, int(plan.reversal_min_opposing_trend_agreement or 0)):
                return False, f"reversal_unconfirmed:{playbook}"

        candidate["asset_plan"] = {
            "allowed_playbooks": list(plan.allowed_playbooks),
            "allowed_sessions": list(plan.allowed_sessions),
            "min_alignment_score": round(float(plan.min_alignment_score), 4),
            "min_setup_quality": round(float(plan.min_setup_quality), 4),
            "min_trend_agreement": int(plan.min_trend_agreement),
        }
        candidate["htf_alignment"] = {
            "trend_15m": trend_15m,
            "trend_1h": trend_1h,
            "structure_bias": structure_bias,
            "aligned_trends": aligned_trends,
            "opposing_trends": opposing_trends,
        }
        return True, ""

    def _breakout_continuation(
        self,
        frame: pd.DataFrame,
        *,
        asset: str,
        structure: Dict[str, Any],
        category: str,
        session: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        profile = self._profile(category)
        preferred_interval = self.preferred_interval(category, asset)
        lookback = min(profile.breakout_lookback, max(8, len(frame) - 1))
        recent = frame.tail(lookback + 1)
        prior = recent.iloc[:-1]
        latest = recent.iloc[-1]

        range_high = float(prior["high"].max())
        range_low = float(prior["low"].min())
        latest_close = float(latest["close"])
        latest_open = float(latest["open"])
        latest_high = float(latest["high"])
        latest_low = float(latest["low"])
        current_body = abs(latest_close - latest_open)
        avg_body = float((prior["close"] - prior["open"]).abs().tail(lookback).mean() or 0.0)
        atr = self._atr(frame.tail(max(20, lookback + 2)))
        range_span = max(range_high - range_low, atr, 1e-9)

        breakout_up = max(0.0, latest_close - range_high)
        breakout_down = max(0.0, range_low - latest_close)
        wick_up = max(0.0, latest_high - range_high)
        wick_down = max(0.0, range_low - latest_low)

        if breakout_up <= 0.0 and breakout_down <= 0.0:
            return None

        direction = "BUY" if breakout_up >= breakout_down else "SELL"
        breakout_dist = breakout_up if direction == "BUY" else breakout_down
        breakout_wick = wick_up if direction == "BUY" else wick_down
        breakout_score = float(structure.get("breakout_score", 0.0) or 0.0)
        alignment_score = float(structure.get("alignment_score", 0.0) or 0.0)
        setup_quality = float(structure.get("setup_quality", 0.0) or 0.0)
        volatility_state = str(structure.get("volatility_state", "unknown") or "unknown").lower()
        regime = str(structure.get("regime", "unknown") or "unknown").lower()

        direction_breakout = breakout_score if direction == "BUY" else -breakout_score
        breakout_norm = _clip(breakout_dist / max(atr * 0.75, range_span * 0.18, 1e-9))
        body_norm = _clip(current_body / max(avg_body * 2.0, 1e-9))
        wick_confirm = _clip((breakout_dist + breakout_wick) / max(atr, 1e-9))
        structure_component = _clip(direction_breakout, 0.0, 1.0)
        regime_component = 0.72 if (
            (direction == "BUY" and regime == "trending_up")
            or (direction == "SELL" and regime == "trending_down")
        ) else 0.55 if volatility_state in {"expansion", "normal"} else 0.40

        score = (
            breakout_norm * 0.34
            + body_norm * 0.20
            + wick_confirm * 0.10
            + _clip(setup_quality) * 0.16
            + _clip(alignment_score) * 0.10
            + structure_component * 0.10
        )
        confidence = _clip(0.42 + score * 0.40 + regime_component * 0.18, 0.0, 0.95)

        if score < profile.breakout_min_score:
            return None

        notes = [
            "range_break",
            f"session={session}",
            f"body_x={current_body / max(avg_body, 1e-9):.2f}",
            f"breakout_atr={breakout_dist / max(atr, 1e-9):.2f}",
        ]
        return {
            "playbook": "breakout_continuation",
            "direction": direction,
            "score": round(score, 4),
            "confidence": round(confidence, 4),
            "entry_style": "breakout_close",
            "session": session,
            "preferred_interval": preferred_interval,
            "management": self._management_template(profile, "breakout_continuation", asset=asset, category=category),
            "notes": notes,
        }

    def _breakout_retest(
        self,
        frame: pd.DataFrame,
        *,
        asset: str,
        structure: Dict[str, Any],
        category: str,
        session: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        profile = self._profile(category)
        preferred_interval = self.preferred_interval(category, asset)
        if len(frame) < profile.breakout_lookback + profile.retest_window + 2:
            return None

        recent = frame.tail(profile.breakout_lookback + profile.retest_window + 2)
        base = recent.iloc[: -(profile.retest_window + 1)]
        if base.empty:
            return None
        prior_recent = recent.iloc[-(profile.retest_window + 1) : -1]
        latest = recent.iloc[-1]

        range_high = float(base["high"].max())
        range_low = float(base["low"].min())
        atr = self._atr(recent.tail(24))
        tolerance = max(atr * profile.retest_tolerance_atr, abs(range_high - range_low) * 0.08, 1e-9)

        buy_break_seen = any(float(value) > range_high for value in prior_recent["close"])
        sell_break_seen = any(float(value) < range_low for value in prior_recent["close"])

        latest_open = float(latest["open"])
        latest_close = float(latest["close"])
        latest_low = float(latest["low"])
        latest_high = float(latest["high"])

        candidates: List[Dict[str, Any]] = []
        alignment_score = float(structure.get("alignment_score", 0.0) or 0.0)
        setup_quality = float(structure.get("setup_quality", 0.0) or 0.0)
        breakout_score = float(structure.get("breakout_score", 0.0) or 0.0)
        regime = str(structure.get("regime", "unknown") or "unknown").lower()

        if buy_break_seen and latest_low <= range_high + tolerance and latest_close >= range_high:
            hold_strength = _clip((latest_close - range_high + tolerance) / max(tolerance * 2.0, 1e-9))
            body_bias = _clip((latest_close - latest_open + tolerance) / max(tolerance * 2.5, 1e-9))
            score = (
                hold_strength * 0.34
                + body_bias * 0.16
                + _clip(alignment_score) * 0.15
                + _clip(setup_quality) * 0.15
                + _clip(breakout_score, 0.0, 1.0) * 0.10
                + (0.10 if regime == "trending_up" else 0.04)
            )
            confidence = _clip(0.43 + score * 0.42, 0.0, 0.94)
            if score >= profile.retest_min_score:
                candidates.append(
                    {
                        "playbook": "breakout_retest",
                        "direction": "BUY",
                        "score": round(score, 4),
                        "confidence": round(confidence, 4),
                        "entry_style": "retest_hold",
                        "session": session,
                        "preferred_interval": preferred_interval,
                        "management": self._management_template(profile, "breakout_retest", asset=asset, category=category),
                        "notes": [
                            "retest_hold",
                            f"session={session}",
                            f"level={range_high:.6f}",
                            f"atr_tol={tolerance / max(atr, 1e-9):.2f}",
                        ],
                    }
                )

        if sell_break_seen and latest_high >= range_low - tolerance and latest_close <= range_low:
            hold_strength = _clip((range_low - latest_close + tolerance) / max(tolerance * 2.0, 1e-9))
            body_bias = _clip((latest_open - latest_close + tolerance) / max(tolerance * 2.5, 1e-9))
            score = (
                hold_strength * 0.34
                + body_bias * 0.16
                + _clip(alignment_score) * 0.15
                + _clip(setup_quality) * 0.15
                + _clip(-breakout_score, 0.0, 1.0) * 0.10
                + (0.10 if regime == "trending_down" else 0.04)
            )
            confidence = _clip(0.43 + score * 0.42, 0.0, 0.94)
            if score >= profile.retest_min_score:
                candidates.append(
                    {
                        "playbook": "breakout_retest",
                        "direction": "SELL",
                        "score": round(score, 4),
                        "confidence": round(confidence, 4),
                        "entry_style": "retest_hold",
                        "session": session,
                        "preferred_interval": preferred_interval,
                        "management": self._management_template(profile, "breakout_retest", asset=asset, category=category),
                        "notes": [
                            "retest_hold",
                            f"session={session}",
                            f"level={range_low:.6f}",
                            f"atr_tol={tolerance / max(atr, 1e-9):.2f}",
                        ],
                    }
                )

        if not candidates:
            return None
        candidates.sort(key=lambda item: (float(item.get("confidence", 0.0)), float(item.get("score", 0.0))), reverse=True)
        return candidates[0]

    def _trend_pullback(
        self,
        frame: pd.DataFrame,
        *,
        asset: str,
        structure: Dict[str, Any],
        category: str,
        session: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        profile = self._profile(category)
        preferred_interval = self.preferred_interval(category, asset)
        structure_bias = str(structure.get("structure_bias", "neutral") or "neutral").lower()
        if structure_bias not in {"buy", "sell"}:
            return None

        direction = "BUY" if structure_bias == "buy" else "SELL"
        pullback_score = float(structure.get("pullback_score", 0.0) or 0.0)
        alignment_score = float(structure.get("alignment_score", 0.0) or 0.0)
        setup_quality = float(structure.get("setup_quality", 0.0) or 0.0)
        regime = str(structure.get("regime", "unknown") or "unknown").lower()
        distance_key = "distance_to_support" if direction == "BUY" else "distance_to_resistance"
        distance = float(structure.get(distance_key, 0.02) or 0.02)
        directional_pullback = pullback_score if direction == "BUY" else -pullback_score

        if directional_pullback <= 0.12:
            return None

        close = frame["close"].astype(float)
        fast = float(close.tail(8).mean())
        slow = float(close.tail(21).mean())
        trend_confirm = 1.0 if ((direction == "BUY" and fast >= slow) or (direction == "SELL" and fast <= slow)) else 0.0
        level_proximity = _clip(1.0 - distance / 0.01)
        regime_component = 0.74 if (
            (direction == "BUY" and regime == "trending_up")
            or (direction == "SELL" and regime == "trending_down")
        ) else 0.52

        score = (
            _clip(directional_pullback) * 0.30
            + _clip(setup_quality) * 0.20
            + _clip(alignment_score) * 0.18
            + level_proximity * 0.18
            + trend_confirm * 0.14
        )
        confidence = _clip(0.40 + score * 0.40 + regime_component * 0.18, 0.0, 0.92)

        if score < profile.pullback_min_score:
            return None

        notes = [
            "trend_pullback",
            f"session={session}",
            f"pullback={directional_pullback:.2f}",
            f"level_dist={distance:.4f}",
        ]
        return {
            "playbook": "trend_pullback",
            "direction": direction,
            "score": round(score, 4),
            "confidence": round(confidence, 4),
            "entry_style": "pullback_hold",
            "session": session,
            "preferred_interval": preferred_interval,
            "management": self._management_template(profile, "trend_pullback", asset=asset, category=category),
            "notes": notes,
        }

    def _reversal_exhaustion(
        self,
        frame: pd.DataFrame,
        *,
        asset: str,
        structure: Dict[str, Any],
        category: str,
        session: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        profile = self._profile(category)
        preferred_interval = self.preferred_interval(category, asset)
        lookback = min(max(profile.breakout_lookback, 14), max(10, len(frame) - 1))
        recent = frame.tail(lookback + 1)
        if len(recent) < 12:
            return None
        prior = recent.iloc[:-1]
        latest = recent.iloc[-1]

        range_high = float(prior["high"].max())
        range_low = float(prior["low"].min())
        latest_open = float(latest["open"])
        latest_close = float(latest["close"])
        latest_high = float(latest["high"])
        latest_low = float(latest["low"])
        atr = self._atr(recent.tail(24))
        avg_body = float((prior["close"] - prior["open"]).abs().tail(lookback).mean() or 0.0)
        tolerance = max(atr * 0.12, abs(range_high - range_low) * 0.06, 1e-9)
        setup_quality = float(structure.get("setup_quality", 0.0) or 0.0)
        alignment_score = float(structure.get("alignment_score", 0.0) or 0.0)
        breakout_score = float(structure.get("breakout_score", 0.0) or 0.0)
        regime = str(structure.get("regime", "unknown") or "unknown").lower()
        structure_bias = str(structure.get("structure_bias", "neutral") or "neutral").lower()

        def _candidate(direction: str, sweep_size: float, reclaim_dist: float, body_strength: float) -> Dict[str, Any]:
            stretch_component = 0.10 if (
                (direction == "SELL" and structure_bias == "buy")
                or (direction == "BUY" and structure_bias == "sell")
            ) else 0.05
            regime_component = 0.10 if (
                (direction == "SELL" and regime in {"trending_up", "volatile"})
                or (direction == "BUY" and regime in {"trending_down", "volatile"})
            ) else 0.04
            score = (
                _clip(sweep_size / max(atr, 1e-9)) * 0.26
                + _clip(reclaim_dist / max(atr, 1e-9)) * 0.24
                + _clip(body_strength / max(avg_body * 2.0, 1e-9)) * 0.18
                + _clip(setup_quality) * 0.12
                + _clip(abs(breakout_score)) * 0.10
                + _clip(alignment_score) * 0.10
                + stretch_component
                + regime_component
            )
            confidence = _clip(0.42 + score * 0.42, 0.0, 0.95)
            return {
                "playbook": "reversal_exhaustion",
                "direction": direction,
                "score": round(score, 4),
                "confidence": round(confidence, 4),
                "entry_style": "reclaim_reversal",
                "session": session,
                "preferred_interval": preferred_interval,
                "management": self._management_template(profile, "reversal_exhaustion", asset=asset, category=category),
                "notes": [
                    "liquidity_sweep",
                    "reversal_exhaustion",
                    "bearish_reclaim_failure" if direction == "SELL" else "bullish_reclaim_failure",
                    f"session={session}",
                ],
            }

        candidates: List[Dict[str, Any]] = []
        if latest_high >= range_high + tolerance and latest_close <= range_high and latest_close < latest_open:
            sweep_size = latest_high - range_high
            reclaim_dist = range_high - latest_close
            body_strength = latest_open - latest_close
            candidate = _candidate("SELL", sweep_size, reclaim_dist, body_strength)
            if float(candidate["score"]) >= profile.reversal_min_score:
                candidates.append(candidate)

        if latest_low <= range_low - tolerance and latest_close >= range_low and latest_close > latest_open:
            sweep_size = range_low - latest_low
            reclaim_dist = latest_close - range_low
            body_strength = latest_close - latest_open
            candidate = _candidate("BUY", sweep_size, reclaim_dist, body_strength)
            if float(candidate["score"]) >= profile.reversal_min_score:
                candidates.append(candidate)

        if not candidates:
            return None
        candidates.sort(key=lambda item: (float(item.get("confidence", 0.0)), float(item.get("score", 0.0))), reverse=True)
        return candidates[0]

    def _failed_break_reclaim(
        self,
        frame: pd.DataFrame,
        *,
        asset: str,
        structure: Dict[str, Any],
        category: str,
        session: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        profile = self._profile(category)
        preferred_interval = self.preferred_interval(category, asset)
        lookback = min(max(profile.breakout_lookback, 14), max(10, len(frame) - 2))
        recent = frame.tail(lookback + 2)
        if len(recent) < 12:
            return None
        base = recent.iloc[:-2]
        prior_bar = recent.iloc[-2]
        latest = recent.iloc[-1]
        if base.empty:
            return None

        range_high = float(base["high"].max())
        range_low = float(base["low"].min())
        atr = self._atr(recent.tail(24))
        avg_body = float((base["close"] - base["open"]).abs().tail(lookback).mean() or 0.0)
        tolerance = max(atr * 0.10, abs(range_high - range_low) * 0.05, 1e-9)
        setup_quality = float(structure.get("setup_quality", 0.0) or 0.0)
        alignment_score = float(structure.get("alignment_score", 0.0) or 0.0)
        regime = str(structure.get("regime", "unknown") or "unknown").lower()

        prior_close = float(prior_bar["close"])
        prior_high = float(prior_bar["high"])
        prior_low = float(prior_bar["low"])
        latest_open = float(latest["open"])
        latest_close = float(latest["close"])
        latest_high = float(latest["high"])
        latest_low = float(latest["low"])

        candidates: List[Dict[str, Any]] = []
        if prior_close >= range_high + tolerance and latest_close <= range_high and latest_close < latest_open:
            reclaim = range_high - latest_close
            body_strength = latest_open - latest_close
            lower_high = max(0.0, prior_high - latest_high)
            score = (
                _clip(reclaim / max(atr, 1e-9)) * 0.30
                + _clip(body_strength / max(avg_body * 2.0, 1e-9)) * 0.20
                + _clip(lower_high / max(atr, 1e-9)) * 0.14
                + _clip(setup_quality) * 0.16
                + _clip(alignment_score) * 0.10
                + (0.10 if regime in {"trending_up", "volatile"} else 0.04)
            )
            confidence = _clip(0.42 + score * 0.42, 0.0, 0.94)
            if score >= profile.reversal_min_score:
                candidates.append(
                    {
                        "playbook": "failed_break_reclaim",
                        "direction": "SELL",
                        "score": round(score, 4),
                        "confidence": round(confidence, 4),
                        "entry_style": "reclaim_failure",
                        "session": session,
                        "preferred_interval": preferred_interval,
                        "management": self._management_template(profile, "failed_break_reclaim", asset=asset, category=category),
                        "notes": [
                            "failed_breakout",
                            "lower_high",
                            "bearish_reclaim_failure",
                            f"session={session}",
                        ],
                    }
                )

        if prior_close <= range_low - tolerance and latest_close >= range_low and latest_close > latest_open:
            reclaim = latest_close - range_low
            body_strength = latest_close - latest_open
            higher_low = max(0.0, latest_low - prior_low)
            score = (
                _clip(reclaim / max(atr, 1e-9)) * 0.30
                + _clip(body_strength / max(avg_body * 2.0, 1e-9)) * 0.20
                + _clip(higher_low / max(atr, 1e-9)) * 0.14
                + _clip(setup_quality) * 0.16
                + _clip(alignment_score) * 0.10
                + (0.10 if regime in {"trending_down", "volatile"} else 0.04)
            )
            confidence = _clip(0.42 + score * 0.42, 0.0, 0.94)
            if score >= profile.reversal_min_score:
                candidates.append(
                    {
                        "playbook": "failed_break_reclaim",
                        "direction": "BUY",
                        "score": round(score, 4),
                        "confidence": round(confidence, 4),
                        "entry_style": "reclaim_failure",
                        "session": session,
                        "preferred_interval": preferred_interval,
                        "management": self._management_template(profile, "failed_break_reclaim", asset=asset, category=category),
                        "notes": [
                            "failed_breakout",
                            "higher_low",
                            "bullish_reclaim_failure",
                            f"session={session}",
                        ],
                    }
                )

        if not candidates:
            return None
        candidates.sort(key=lambda item: (float(item.get("confidence", 0.0)), float(item.get("score", 0.0))), reverse=True)
        return candidates[0]

    def _aggressive_expansion_trigger(
        self,
        frame: pd.DataFrame,
        *,
        asset: str,
        structure: Dict[str, Any],
        category: str,
        session: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        profile = self._profile(category)
        preferred_interval = self.preferred_interval(category, asset)
        lookback = min(max(profile.breakout_lookback, 16), max(10, len(frame) - 1))
        recent = frame.tail(lookback + 1)
        if len(recent) < 12:
            return None
        prior = recent.iloc[:-1]
        latest = recent.iloc[-1]

        latest_open = float(latest["open"])
        latest_close = float(latest["close"])
        latest_high = float(latest["high"])
        latest_low = float(latest["low"])
        atr = self._atr(recent.tail(24))
        avg_body = float((prior["close"] - prior["open"]).abs().tail(lookback).mean() or 0.0)
        previous_high = float(prior["high"].tail(min(12, len(prior))).max())
        previous_low = float(prior["low"].tail(min(12, len(prior))).min())
        setup_quality = float(structure.get("setup_quality", 0.0) or 0.0)
        alignment_score = float(structure.get("alignment_score", 0.0) or 0.0)
        regime = str(structure.get("regime", "unknown") or "unknown").lower()

        body = abs(latest_close - latest_open)
        close_near_low = _clip((latest_high - latest_close) / max(atr * 0.8, 1e-9), 0.0, 1.0)
        close_near_high = _clip((latest_close - latest_low) / max(atr * 0.8, 1e-9), 0.0, 1.0)

        if latest_close < latest_open and latest_close <= previous_low:
            expansion_dist = previous_low - latest_close
            score = (
                _clip(body / max(avg_body * 2.5, 1e-9)) * 0.34
                + _clip(expansion_dist / max(atr, 1e-9)) * 0.26
                + close_near_low * 0.10
                + _clip(setup_quality) * 0.16
                + _clip(alignment_score) * 0.08
                + (0.10 if regime in {"trending_down", "volatile"} else 0.04)
            )
            confidence = _clip(0.40 + score * 0.44, 0.0, 0.94)
            if score >= profile.expansion_min_score:
                return {
                    "playbook": "aggressive_expansion",
                    "direction": "SELL",
                    "score": round(score, 4),
                    "confidence": round(confidence, 4),
                    "entry_style": "expansion_break",
                    "session": session,
                    "preferred_interval": preferred_interval,
                    "management": self._management_template(profile, "aggressive_expansion", asset=asset, category=category),
                    "notes": [
                        "aggressive_downside_expansion",
                        f"session={session}",
                        f"body_x={body / max(avg_body, 1e-9):.2f}",
                    ],
                }

        if latest_close > latest_open and latest_close >= previous_high:
            expansion_dist = latest_close - previous_high
            score = (
                _clip(body / max(avg_body * 2.5, 1e-9)) * 0.34
                + _clip(expansion_dist / max(atr, 1e-9)) * 0.26
                + close_near_high * 0.10
                + _clip(setup_quality) * 0.16
                + _clip(alignment_score) * 0.08
                + (0.10 if regime in {"trending_up", "volatile"} else 0.04)
            )
            confidence = _clip(0.40 + score * 0.44, 0.0, 0.94)
            if score >= profile.expansion_min_score:
                return {
                    "playbook": "aggressive_expansion",
                    "direction": "BUY",
                    "score": round(score, 4),
                    "confidence": round(confidence, 4),
                    "entry_style": "expansion_break",
                    "session": session,
                    "preferred_interval": preferred_interval,
                    "management": self._management_template(profile, "aggressive_expansion", asset=asset, category=category),
                    "notes": [
                        "aggressive_upside_expansion",
                        f"session={session}",
                        f"body_x={body / max(avg_body, 1e-9):.2f}",
                    ],
                }
        return None

    def _opening_drive(
        self,
        frame: pd.DataFrame,
        *,
        asset: str,
        structure: Dict[str, Any],
        category: str,
        session: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        if category not in {"indices", "commodities"}:
            return None
        if not str(session or "").endswith("_open"):
            return None

        profile = self._profile(category)
        preferred_interval = self.preferred_interval(category, asset)
        lookback = min(max(profile.breakout_lookback, 12), max(10, len(frame) - 1))
        recent = frame.tail(lookback + 1)
        if len(recent) < 12:
            return None
        prior = recent.iloc[:-1]
        latest = recent.iloc[-1]

        range_high = float(prior["high"].max())
        range_low = float(prior["low"].min())
        latest_open = float(latest["open"])
        latest_close = float(latest["close"])
        latest_high = float(latest["high"])
        latest_low = float(latest["low"])
        atr = self._atr(recent.tail(24))
        avg_body = float((prior["close"] - prior["open"]).abs().tail(lookback).mean() or 0.0)
        setup_quality = float(structure.get("setup_quality", 0.0) or 0.0)
        alignment_score = float(structure.get("alignment_score", 0.0) or 0.0)
        regime = str(structure.get("regime", "unknown") or "unknown").lower()

        body = abs(latest_close - latest_open)
        if body <= 0.0:
            return None

        candidates: List[Dict[str, Any]] = []
        if latest_close > range_high and latest_close > latest_open:
            impulse = latest_close - range_high
            close_strength = _clip((latest_close - latest_low) / max(atr * 0.8, 1e-9))
            score = (
                _clip(body / max(avg_body * 2.1, 1e-9)) * 0.30
                + _clip(impulse / max(atr, 1e-9)) * 0.24
                + close_strength * 0.12
                + _clip(setup_quality) * 0.14
                + _clip(alignment_score) * 0.10
                + (0.10 if regime in {"trending_up", "volatile"} else 0.04)
            )
            confidence = _clip(0.43 + score * 0.43, 0.0, 0.95)
            if score >= profile.breakout_min_score:
                candidates.append(
                    {
                        "playbook": "opening_drive",
                        "direction": "BUY",
                        "score": round(score, 4),
                        "confidence": round(confidence, 4),
                        "entry_style": "opening_drive_break",
                        "session": session,
                        "preferred_interval": preferred_interval,
                        "management": self._management_template(profile, "opening_drive", asset=asset, category=category),
                        "notes": [
                            "opening_drive",
                            "cash_open_break",
                            f"session={session}",
                        ],
                    }
                )

        if latest_close < range_low and latest_close < latest_open:
            impulse = range_low - latest_close
            close_strength = _clip((latest_high - latest_close) / max(atr * 0.8, 1e-9))
            score = (
                _clip(body / max(avg_body * 2.1, 1e-9)) * 0.30
                + _clip(impulse / max(atr, 1e-9)) * 0.24
                + close_strength * 0.12
                + _clip(setup_quality) * 0.14
                + _clip(alignment_score) * 0.10
                + (0.10 if regime in {"trending_down", "volatile"} else 0.04)
            )
            confidence = _clip(0.43 + score * 0.43, 0.0, 0.95)
            if score >= profile.breakout_min_score:
                candidates.append(
                    {
                        "playbook": "opening_drive",
                        "direction": "SELL",
                        "score": round(score, 4),
                        "confidence": round(confidence, 4),
                        "entry_style": "opening_drive_break",
                        "session": session,
                        "preferred_interval": preferred_interval,
                        "management": self._management_template(profile, "opening_drive", asset=asset, category=category),
                        "notes": [
                            "opening_drive",
                            "cash_open_break",
                            f"session={session}",
                        ],
                    }
                )

        if not candidates:
            return None
        candidates.sort(key=lambda item: (float(item.get("confidence", 0.0)), float(item.get("score", 0.0))), reverse=True)
        return candidates[0]

    def _news_impulse(
        self,
        frame: pd.DataFrame,
        *,
        asset: str,
        structure: Dict[str, Any],
        category: str,
        session: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        if category not in {"forex", "commodities"}:
            return None

        news = dict((context or {}).get("news_event") or {})
        news_state = str(news.get("state") or "").strip().lower()
        impact = str(news.get("impact") or "").strip().upper()
        direction_sign = _news_direction_sign(news.get("direction"))
        if news_state not in {"active", "post"} or impact not in {"HIGH", "MEDIUM"} or direction_sign == 0:
            return None

        profile = self._profile(category)
        preferred_interval = self.preferred_interval(category, asset)
        lookback = min(max(profile.breakout_lookback, 10), max(8, len(frame) - 1))
        recent = frame.tail(lookback + 1)
        if len(recent) < 10:
            return None
        prior = recent.iloc[:-1]
        latest = recent.iloc[-1]
        atr = self._atr(recent.tail(24))
        avg_body = float((prior["close"] - prior["open"]).abs().tail(lookback).mean() or 0.0)
        body = abs(float(latest["close"]) - float(latest["open"]))
        prior_high = float(prior["high"].max())
        prior_low = float(prior["low"].min())
        setup_quality = float(structure.get("setup_quality", 0.0) or 0.0)
        alignment_score = float(structure.get("alignment_score", 0.0) or 0.0)
        regime = str(structure.get("regime", "unknown") or "unknown").lower()

        if direction_sign > 0:
            if float(latest["close"]) <= max(prior_high, float(latest["open"])):
                return None
            impulse = float(latest["close"]) - prior_high
            close_strength = _clip((float(latest["close"]) - float(latest["low"])) / max(atr * 0.8, 1e-9))
            direction = "BUY"
        else:
            if float(latest["close"]) >= min(prior_low, float(latest["open"])):
                return None
            impulse = prior_low - float(latest["close"])
            close_strength = _clip((float(latest["high"]) - float(latest["close"])) / max(atr * 0.8, 1e-9))
            direction = "SELL"

        impact_bonus = 0.10 if impact == "HIGH" else 0.05
        state_bonus = 0.08 if news_state == "active" else 0.05
        regime_bonus = 0.08 if (
            (direction == "BUY" and regime in {"trending_up", "volatile"})
            or (direction == "SELL" and regime in {"trending_down", "volatile"})
        ) else 0.03
        score = (
            _clip(body / max(avg_body * 2.4, 1e-9)) * 0.28
            + _clip(impulse / max(atr, 1e-9)) * 0.24
            + close_strength * 0.10
            + _clip(setup_quality) * 0.14
            + _clip(alignment_score) * 0.09
            + impact_bonus
            + state_bonus
            + regime_bonus
        )
        confidence = _clip(0.44 + score * 0.42, 0.0, 0.95)
        if score < max(profile.breakout_min_score, 0.60):
            return None
        return {
            "playbook": "news_impulse",
            "direction": direction,
            "score": round(score, 4),
            "confidence": round(confidence, 4),
            "entry_style": "news_followthrough",
            "session": session,
            "preferred_interval": preferred_interval,
            "management": self._management_template(profile, "news_impulse", asset=asset, category=category),
            "notes": [
                "news_impulse",
                f"impact={impact}",
                f"event={str(news.get('event') or 'macro')[:36]}",
                f"session={session}",
            ],
        }

    def _crypto_orderflow_continuation(
        self,
        frame: pd.DataFrame,
        *,
        asset: str,
        structure: Dict[str, Any],
        category: str,
        session: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        if category != "crypto":
            return None

        micro = dict((context or {}).get("market_microstructure") or {})
        true_depth = bool(micro.get("depth_available"))
        synthetic_depth = bool(micro.get("synthetic_depth_available"))
        if not true_depth and not synthetic_depth:
            return None

        imbalance = _safe_float(micro.get("book_imbalance", micro.get("score", 0.0)), 0.0)
        micro_score = _safe_float(micro.get("score", 0.0), 0.0)
        spread_bps = _safe_float(micro.get("spread_bps", 0.0), 0.0)
        threshold = 0.18 if true_depth else 0.26
        if abs(imbalance) < threshold or spread_bps > 35.0:
            return None

        profile = self._profile(category)
        preferred_interval = self.preferred_interval(category, asset)
        lookback = min(max(profile.breakout_lookback, 12), max(10, len(frame) - 1))
        recent = frame.tail(lookback + 1)
        if len(recent) < 10:
            return None
        prior = recent.iloc[:-1]
        latest = recent.iloc[-1]
        atr = self._atr(recent.tail(24))
        avg_body = float((prior["close"] - prior["open"]).abs().tail(lookback).mean() or 0.0)
        prev_high = float(prior["high"].tail(min(12, len(prior))).max())
        prev_low = float(prior["low"].tail(min(12, len(prior))).min())
        latest_open = float(latest["open"])
        latest_close = float(latest["close"])
        body = abs(latest_close - latest_open)
        setup_quality = float(structure.get("setup_quality", 0.0) or 0.0)
        alignment_score = float(structure.get("alignment_score", 0.0) or 0.0)

        if imbalance > 0 and latest_close > prev_high and latest_close > latest_open:
            impulse = latest_close - prev_high
            direction = "BUY"
        elif imbalance < 0 and latest_close < prev_low and latest_close < latest_open:
            impulse = prev_low - latest_close
            direction = "SELL"
        else:
            return None

        score = (
            _clip(abs(imbalance)) * 0.22
            + _clip(abs(micro_score)) * 0.16
            + _clip(body / max(avg_body * 2.1, 1e-9)) * 0.18
            + _clip(impulse / max(atr, 1e-9)) * 0.18
            + _clip(setup_quality) * 0.12
            + _clip(alignment_score) * 0.08
            + (0.06 if true_depth else 0.02)
        )
        confidence = _clip(0.43 + score * 0.43, 0.0, 0.96)
        if score < max(profile.breakout_min_score, 0.60):
            return None

        return {
            "playbook": "crypto_orderflow_continuation",
            "direction": direction,
            "score": round(score, 4),
            "confidence": round(confidence, 4),
            "entry_style": "orderflow_break",
            "session": session,
            "preferred_interval": preferred_interval,
            "management": self._management_template(profile, "crypto_orderflow_continuation", asset=asset, category=category),
            "notes": [
                "crypto_orderflow_continuation",
                "true_depth" if true_depth else "synthetic_depth",
                f"imbalance={imbalance:.2f}",
                f"spread_bps={spread_bps:.1f}",
                f"session={session}",
            ],
        }

    def analyze(
        self,
        asset: str,
        category: str,
        price_data,
        context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        context = context or {}
        frame = self._frame(price_data)
        if frame is None:
            return {"asset": asset, "category": category, "candidates": [], "primary": None}

        plan = self._asset_plan(asset, category)
        session_allowed, session, allowed_sessions = self._session_allowed(asset, category)
        if not session_allowed:
            return {
                "asset": asset,
                "category": category,
                "candidates": [],
                "primary": None,
                "blocked_reason": f"session_block:{session}",
                "session": session,
                "allowed_sessions": list(allowed_sessions),
                "asset_plan": {
                    "allowed_playbooks": list(plan.allowed_playbooks),
                    "allowed_sessions": list(allowed_sessions),
                },
            }

        structure = dict(context.get("market_structure") or {})
        candidates: List[Dict[str, Any]] = []
        rejected_reasons: List[str] = []
        for builder in (
            self._news_impulse,
            self._opening_drive,
            self._crypto_orderflow_continuation,
            self._reversal_exhaustion,
            self._failed_break_reclaim,
            self._aggressive_expansion_trigger,
            self._breakout_continuation,
            self._breakout_retest,
            self._trend_pullback,
        ):
            candidate = builder(
                frame,
                asset=asset,
                structure=structure,
                category=category,
                session=session,
                context=context,
            )
            if candidate:
                approved, reason = self._qualify_candidate(
                    candidate,
                    asset=asset,
                    category=category,
                    structure=structure,
                    plan=plan,
                )
                if approved:
                    candidates.append(candidate)
                elif reason:
                    rejected_reasons.append(reason)

        candidates.sort(key=lambda item: (float(item.get("confidence", 0.0)), float(item.get("score", 0.0))), reverse=True)
        primary = dict(candidates[0]) if candidates else None
        return {
            "asset": asset,
            "category": category,
            "session": session,
            "allowed_sessions": list(allowed_sessions),
            "candidates": candidates,
            "primary": primary,
            "blocked_reason": "" if primary else (rejected_reasons[0] if rejected_reasons else ""),
            "rejected_reasons": rejected_reasons[:5],
            "asset_plan": {
                "allowed_playbooks": list(plan.allowed_playbooks),
                "allowed_sessions": list(allowed_sessions),
                "min_alignment_score": round(float(plan.min_alignment_score), 4),
                "min_setup_quality": round(float(plan.min_setup_quality), 4),
                "min_trend_agreement": int(plan.min_trend_agreement),
            },
        }

    def pick_seed(
        self,
        asset: str,
        category: str,
        price_data,
        context: Optional[Dict[str, Any]] = None,
        *,
        ml_direction: str = "",
        ml_confidence: float = 0.0,
    ) -> Dict[str, Any]:
        analysis = self.analyze(asset, category, price_data, context=context)
        best = analysis.get("primary")
        if not best:
            return {
                "action": "",
                "asset": asset,
                "category": category,
                "primary": None,
                "blocked_reason": analysis.get("blocked_reason", ""),
                "session": analysis.get("session", ""),
                "session_label": analysis.get("session", ""),
                "rejected_reasons": list(analysis.get("rejected_reasons") or []),
                "allowed_sessions": list(analysis.get("allowed_sessions") or []),
                "asset_plan": dict(analysis.get("asset_plan") or {}),
            }

        profile = self._profile(category)
        direction = str(best.get("direction") or "").upper()
        confidence = float(best.get("confidence", 0.0) or 0.0)
        ml_direction = str(ml_direction or "").upper()
        ml_confidence = float(ml_confidence or 0.0)
        action = ""

        if not ml_direction or ml_confidence < 0.10:
            if confidence >= profile.seed_min_confidence:
                action = "seed"
        elif direction == ml_direction:
            if confidence >= profile.support_min_confidence:
                action = "support"
        elif confidence >= max(profile.override_min_confidence, ml_confidence + profile.override_gap) and ml_confidence <= profile.weak_ml_confidence:
            action = "override"

        return {
            "action": action,
            "asset": asset,
            "category": category,
            "session": analysis.get("session", ""),
            "session_label": analysis.get("session", ""),
            "primary": best,
            "candidates": analysis.get("candidates", []),
            "blocked_reason": analysis.get("blocked_reason", ""),
            "rejected_reasons": list(analysis.get("rejected_reasons") or []),
            "allowed_sessions": list(analysis.get("allowed_sessions") or []),
            "asset_plan": dict(analysis.get("asset_plan") or {}),
        }


_service = PlaybookService()


def get_service() -> PlaybookService:
    return _service
