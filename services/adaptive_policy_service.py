from __future__ import annotations

import os
from typing import Any, Dict

from config.config import (
    GOVERNANCE_MIN_RISK_REWARD,
    MIN_FINAL_CONFIDENCE,
    SPREAD_THRESHOLDS,
    TRADE_CLOSE_COOLDOWN_MINUTES,
)


def _clip(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, float(value)))


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _direction_sign(direction: str) -> int:
    return 1 if str(direction or "").upper() == "BUY" else -1


_LIVE_CATEGORY_BASE_MIN_RR = {
    "crypto": 1.35,
    "commodities": 1.15,
    "forex": 1.15,
    "indices": 1.25,
}

_PAPER_CATEGORY_BASE_MIN_RR = {
    "crypto": 1.20,
    "commodities": 1.00,
    "forex": 0.75,
    "indices": 1.00,
}


def _runtime_live() -> bool:
    return os.getenv("BOT_LIVE_RUNTIME", "0") == "1"


def _category_base_min_rr(category: str) -> float:
    category_key = str(category or "").lower()
    profile = _LIVE_CATEGORY_BASE_MIN_RR if _runtime_live() else _PAPER_CATEGORY_BASE_MIN_RR
    return float(profile.get(category_key, GOVERNANCE_MIN_RISK_REWARD))


def _category_min_rr_floor(category: str, base_rr: float) -> float:
    category_key = str(category or "").lower()
    if _runtime_live():
        if category_key == "forex":
            return 0.95
        if category_key == "commodities":
            return 1.00
        if category_key == "indices":
            return 1.05
        if category_key == "crypto":
            return 1.20
        return max(1.0, base_rr - 0.10)
    if category_key == "forex":
        return 0.65
    if category_key == "commodities":
        return 0.85
    if category_key == "indices":
        return 0.85
    if category_key == "crypto":
        return 1.00
    return max(0.75, base_rr - 0.20)


def _apply_structure_thresholds(
    thresholds: Dict[str, Any],
    *,
    aligned_structure: bool,
    structure_bias: str,
    structure_strength: float,
    setup_alignment: float,
    alignment_score: float,
) -> None:
    if aligned_structure and structure_strength >= 0.55:
        boost = min(1.0, 0.6 * structure_strength + 0.4 * max(0.0, setup_alignment))
        thresholds["min_final_confidence"] -= 0.01 + boost * 0.03
        thresholds["max_spread"] *= 1.0 + boost * 0.20
        thresholds["risk_multiplier"] += 0.05 + boost * 0.15
        thresholds["cooldown_minutes"] -= 4
        thresholds["min_rr"] -= 0.08
        thresholds["notes"].append("structure_advantage")
    elif structure_bias in {"buy", "sell"} and not aligned_structure and alignment_score >= 0.45:
        penalty = min(1.0, 0.6 * alignment_score + 0.4 * max(0.0, abs(setup_alignment)))
        thresholds["min_final_confidence"] += 0.02 + penalty * 0.03
        thresholds["max_spread"] *= 1.0 - penalty * 0.18
        thresholds["risk_multiplier"] -= 0.10 + penalty * 0.15
        thresholds["cooldown_minutes"] += 6
        thresholds["min_rr"] += 0.10
        thresholds["notes"].append("structure_conflict")

    if setup_alignment >= 0.45:
        thresholds["min_final_confidence"] -= 0.01
        thresholds["risk_multiplier"] += 0.05
        thresholds["notes"].append("setup_aligned")
    elif setup_alignment <= -0.45:
        thresholds["min_final_confidence"] += 0.015
        thresholds["risk_multiplier"] -= 0.08
        thresholds["min_rr"] += 0.05
        thresholds["notes"].append("setup_conflict")


def _apply_market_context_thresholds(
    thresholds: Dict[str, Any],
    *,
    regime: str,
    volatility_state: str,
    aligned_structure: bool,
    opportunity_score: float,
    sentiment_score: float,
    orderflow_score: float,
) -> None:
    if opportunity_score >= 0.80:
        thresholds["min_final_confidence"] -= 0.015
        thresholds["max_spread"] *= 1.05
        thresholds["risk_multiplier"] += 0.08
        thresholds["cooldown_minutes"] -= 2
        thresholds["notes"].append("high_opportunity")
    elif 0.0 < opportunity_score <= 0.55:
        thresholds["min_final_confidence"] += 0.01
        thresholds["risk_multiplier"] -= 0.05
        thresholds["notes"].append("thin_opportunity")

    if regime in {"trending_up", "trending_down"} and aligned_structure:
        thresholds["min_final_confidence"] -= 0.01
        thresholds["risk_multiplier"] += 0.04
        thresholds["notes"].append("trend_support")
    elif regime == "volatile":
        thresholds["min_final_confidence"] += 0.025
        thresholds["risk_multiplier"] -= 0.10
        thresholds["cooldown_minutes"] += 8
        thresholds["notes"].append("volatile_regime")

    if volatility_state == "expansion" and aligned_structure:
        thresholds["max_spread"] *= 1.08
        thresholds["risk_multiplier"] += 0.04
        thresholds["notes"].append("volatility_expansion")
    elif volatility_state == "extreme":
        thresholds["min_final_confidence"] += 0.04
        thresholds["max_spread"] *= 0.82
        thresholds["risk_multiplier"] -= 0.18
        thresholds["cooldown_minutes"] += 10
        thresholds["min_rr"] += 0.12
        thresholds["notes"].append("extreme_volatility")

    if abs(sentiment_score) >= 0.35:
        if sentiment_score > 0:
            thresholds["min_final_confidence"] -= 0.005
            thresholds["risk_multiplier"] += 0.03
            thresholds["notes"].append("sentiment_confirmed")
        else:
            thresholds["min_final_confidence"] += 0.01
            thresholds["risk_multiplier"] -= 0.04
            thresholds["notes"].append("sentiment_conflict")

    if abs(orderflow_score) >= 0.30:
        if orderflow_score > 0:
            thresholds["max_spread"] *= 1.03
            thresholds["risk_multiplier"] += 0.03
            thresholds["notes"].append("orderflow_confirmed")
        else:
            thresholds["max_spread"] *= 0.95
            thresholds["risk_multiplier"] -= 0.04
            thresholds["notes"].append("orderflow_conflict")


def _apply_memory_thresholds(
    thresholds: Dict[str, Any],
    *,
    memory_sample_count: float,
    memory_edge: float,
    memory_score: float,
) -> None:
    if memory_sample_count >= 6:
        if memory_edge >= 0.18 or memory_score >= 62.0:
            thresholds["min_final_confidence"] -= 0.012
            thresholds["max_spread"] *= 1.05
            thresholds["risk_multiplier"] += 0.06
            thresholds["notes"].append("memory_positive_edge")
        elif memory_edge <= -0.12 or memory_score <= 42.0:
            thresholds["min_final_confidence"] += 0.018
            thresholds["max_spread"] *= 0.93
            thresholds["risk_multiplier"] -= 0.08
            thresholds["cooldown_minutes"] += 4
            thresholds["notes"].append("memory_negative_edge")


def _apply_recent_review_thresholds(
    thresholds: Dict[str, Any],
    *,
    asset: str,
    category_key: str,
    signal: Any | None,
    context: Dict[str, Any],
) -> None:
    if signal is None:
        return

    try:
        from services.recent_pattern_learning_service import get_service as get_recent_pattern_learning_service

        recent_review_profile = get_recent_pattern_learning_service().get_profile(
            asset=asset,
            category=category_key,
            signal=signal,
            context=context,
        )
        if int(recent_review_profile.get("sample_count", 0) or 0) >= 4:
            thresholds["min_final_confidence"] += _safe_float(recent_review_profile.get("penalty_confidence"), 0.0)
            thresholds["min_final_confidence"] -= _safe_float(recent_review_profile.get("bonus_confidence"), 0.0)
            thresholds["risk_multiplier"] -= _safe_float(recent_review_profile.get("penalty_risk"), 0.0)
            thresholds["risk_multiplier"] += _safe_float(recent_review_profile.get("bonus_risk"), 0.0)
            thresholds["min_rr"] += _safe_float(recent_review_profile.get("penalty_rr"), 0.0)
            thresholds["min_rr"] -= _safe_float(recent_review_profile.get("bonus_rr_relief"), 0.0)
            thresholds["cooldown_minutes"] += int(recent_review_profile.get("cooldown_delta", 0) or 0)
            thresholds["target_rr_multiplier"] *= _safe_float(recent_review_profile.get("target_rr_multiplier"), 1.0)
            thresholds["notes"].extend(list(recent_review_profile.get("notes") or []))
            thresholds["block_new_entries"] = bool(recent_review_profile.get("block_new_entries"))
            thresholds["block_reason"] = str(recent_review_profile.get("block_reason") or "")
        thresholds["recent_review_profile"] = recent_review_profile
    except Exception:
        thresholds["recent_review_profile"] = {}


def _apply_controls_thresholds(
    thresholds: Dict[str, Any],
    *,
    live_scope: str,
    policy_status: str,
    policy_support: float,
    governance: Dict[str, Any],
    portfolio_state: Any | None,
) -> None:
    if live_scope in {"portfolio", "bootstrap", "unavailable"}:
        thresholds["min_final_confidence"] -= 0.012
        thresholds["max_spread"] *= 1.04
        thresholds["notes"].append(f"{live_scope}_live_validation")

    if policy_status == "ok":
        if policy_support >= 0.70:
            policy_boost = min(0.04, 0.015 + max(0.0, policy_support - 0.70) * 0.10)
            thresholds["min_final_confidence"] -= policy_boost
            thresholds["risk_multiplier"] += min(0.08, 0.03 + max(0.0, policy_support - 0.70) * 0.10)
            thresholds["notes"].append("policy_aligned")
        elif policy_support <= 0.58:
            thresholds["min_final_confidence"] += 0.01
            thresholds["risk_multiplier"] -= 0.03
            thresholds["notes"].append("policy_marginal")

    if governance.get("approved") is True and not governance.get("violations"):
        thresholds["min_final_confidence"] -= 0.008
        thresholds["notes"].append("governance_cleared")

    if portfolio_state is None:
        return

    try:
        balance = _safe_float(getattr(portfolio_state, "balance", 0.0), 0.0)
        initial_balance = _safe_float(getattr(portfolio_state, "initial_balance", 0.0), 0.0)
        if balance > 0 and initial_balance > 0 and balance < initial_balance:
            drawdown_pct = (initial_balance - balance) / initial_balance * 100.0
            if drawdown_pct >= 10.0:
                thresholds["risk_multiplier"] -= 0.08
                thresholds["cooldown_minutes"] += 4
                thresholds["notes"].append("balance_drawdown")
    except Exception:
        pass


class AdaptivePolicyService:
    """
    Produces context-aware trading thresholds from the current signal setup.

    This is intentionally lightweight: it adapts execution and sizing gates
    without replacing the decision engine or governance logic.
    """

    def get_thresholds(
        self,
        asset: str,
        category: str,
        context: Dict[str, Any] | None = None,
        signal: Any | None = None,
        state: Any | None = None,
    ) -> Dict[str, Any]:
        context = context or {}
        metadata = dict(getattr(signal, "metadata", {}) or {})
        category_key = str(category or "").lower()
        direction_sign = _direction_sign(getattr(signal, "direction", "BUY"))

        structure = context.get("market_structure") or metadata.get("market_structure") or {}
        if not isinstance(structure, dict):
            structure = {}

        alignment_score = _safe_float(metadata.get("alignment_score", structure.get("alignment_score")), 0.0)
        setup_quality = _safe_float(metadata.get("setup_quality", structure.get("setup_quality")), 0.0)
        pullback_score = _safe_float(metadata.get("pullback_score", structure.get("pullback_score")), 0.0)
        breakout_score = _safe_float(metadata.get("breakout_score", structure.get("breakout_score")), 0.0)
        structure_bias = str(metadata.get("structure_bias") or structure.get("structure_bias") or "neutral").lower()
        regime = str(structure.get("regime") or metadata.get("regime") or context.get("regime") or "unknown").lower()
        volatility_state = str(metadata.get("volatility_state") or structure.get("volatility_state") or "unknown").lower()
        opportunity_score = _safe_float(metadata.get("opportunity_score"), 0.0)
        sentiment_score = _safe_float(metadata.get("sentiment_score"), 0.0) * direction_sign
        orderflow_score = _safe_float(metadata.get("orderflow_imbalance"), 0.0) * direction_sign
        memory_edge = _safe_float(metadata.get("memory_edge"), 0.0)
        memory_score = _safe_float(metadata.get("memory_score"), 50.0)
        memory_sample_count = _safe_float(metadata.get("memory_sample_count"), 0.0)
        governance = metadata.get("governance_validation") or {}
        if not isinstance(governance, dict):
            governance = {}
        live_validation = governance.get("live_validation") or metadata.get("live_validation_profile") or {}
        if not isinstance(live_validation, dict):
            live_validation = {}
        live_scope = str(live_validation.get("scope", "bootstrap") or "bootstrap").lower()
        policy_status = str(metadata.get("agent_policy_status", "ok") or "ok").lower()
        agent_score = _safe_float(metadata.get("agent_score"), 0.5)
        policy_support = agent_score if direction_sign > 0 else 1.0 - agent_score

        base_confidence = float(MIN_FINAL_CONFIDENCE)
        base_spread = float(SPREAD_THRESHOLDS.get(category_key, 0.002) or 0.002)
        base_cooldown = int(TRADE_CLOSE_COOLDOWN_MINUTES)
        base_rr = _category_base_min_rr(category_key)

        thresholds: Dict[str, Any] = {
            "min_final_confidence": base_confidence,
            "max_spread": base_spread,
            "risk_multiplier": 1.0,
            "cooldown_minutes": base_cooldown,
            "min_rr": base_rr,
            "target_rr_multiplier": 1.0,
            "notes": [],
            "recent_review_profile": {},
            "block_new_entries": False,
            "block_reason": "",
        }

        aligned_structure = False
        if structure_bias in {"buy", "sell"}:
            aligned_structure = (
                (structure_bias == "buy" and direction_sign > 0)
                or (structure_bias == "sell" and direction_sign < 0)
            )

        dominant_setup = breakout_score if abs(breakout_score) >= abs(pullback_score) else pullback_score
        setup_alignment = dominant_setup * direction_sign
        structure_strength = _clip((alignment_score + setup_quality) / 2.0, 0.0, 1.0)

        _apply_structure_thresholds(
            thresholds,
            aligned_structure=aligned_structure,
            structure_bias=structure_bias,
            structure_strength=structure_strength,
            setup_alignment=setup_alignment,
            alignment_score=alignment_score,
        )
        _apply_market_context_thresholds(
            thresholds,
            regime=regime,
            volatility_state=volatility_state,
            aligned_structure=aligned_structure,
            opportunity_score=opportunity_score,
            sentiment_score=sentiment_score,
            orderflow_score=orderflow_score,
        )
        _apply_memory_thresholds(
            thresholds,
            memory_sample_count=memory_sample_count,
            memory_edge=memory_edge,
            memory_score=memory_score,
        )
        _apply_recent_review_thresholds(
            thresholds,
            asset=asset,
            category_key=category_key,
            signal=signal,
            context=context,
        )
        _apply_controls_thresholds(
            thresholds,
            live_scope=live_scope,
            policy_status=policy_status,
            policy_support=policy_support,
            governance=governance,
            portfolio_state=state,
        )

        thresholds["min_final_confidence"] = round(_clip(thresholds["min_final_confidence"], 0.48, 0.74), 4)
        thresholds["max_spread"] = round(_clip(thresholds["max_spread"], base_spread * 0.65, base_spread * 1.45), 6)
        thresholds["risk_multiplier"] = round(_clip(thresholds["risk_multiplier"], 0.60, 1.35), 4)
        thresholds["cooldown_minutes"] = int(round(_clip(thresholds["cooldown_minutes"], 5, max(base_cooldown + 20, 20))))
        min_rr_floor = _category_min_rr_floor(category_key, base_rr)
        thresholds["min_rr"] = round(_clip(thresholds["min_rr"], min_rr_floor, 2.2), 2)
        thresholds["target_rr_multiplier"] = round(_clip(thresholds["target_rr_multiplier"], 0.88, 1.18), 4)

        return {
            "asset": asset,
            "category": category_key,
            "regime": regime,
            "volatility_state": volatility_state,
            "min_final_confidence": thresholds["min_final_confidence"],
            "max_spread": thresholds["max_spread"],
            "risk_multiplier": thresholds["risk_multiplier"],
            "cooldown_minutes": thresholds["cooldown_minutes"],
            "min_rr": thresholds["min_rr"],
            "target_rr_multiplier": thresholds["target_rr_multiplier"],
            "recent_review_profile": thresholds["recent_review_profile"],
            "block_new_entries": thresholds["block_new_entries"],
            "block_reason": thresholds["block_reason"],
            "notes": thresholds["notes"],
        }


_service = AdaptivePolicyService()


def get_service() -> AdaptivePolicyService:
    return _service
