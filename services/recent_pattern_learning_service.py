from __future__ import annotations

import json
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _clip(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, float(value)))


def _is_true_depth_source(source: str) -> bool:
    token = str(source or "").strip().lower()
    return token in {"order_flow_true_depth", "dukascopy_live_depth", "ctrader_live_depth"}


def _parse_metadata(raw: Any) -> Dict[str, Any]:
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            data = json.loads(raw)
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}
    return {}


def _coerce_datetime(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    text = str(value).strip()
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _parse_broker_quality(metadata: Dict[str, Any]) -> Dict[str, Any]:
    raw = metadata.get("broker_quality")
    if isinstance(raw, dict):
        return raw
    fallback = {
        "score": metadata.get("broker_quality_score"),
        "quote_agreement_state": metadata.get("broker_agreement_state"),
        "spread_regime": metadata.get("broker_spread_regime"),
        "quote_quality_state": metadata.get("broker_quote_quality_state"),
        "market_state": metadata.get("broker_market_state"),
    }
    return {k: v for k, v in fallback.items() if v not in (None, "", {})}


def _parse_microstructure(metadata: Dict[str, Any]) -> Dict[str, Any]:
    raw = metadata.get("market_microstructure")
    if isinstance(raw, dict):
        return raw
    fallback = {
        "score": metadata.get("microstructure_score"),
        "microstructure_alignment": metadata.get("microstructure_alignment"),
        "tick_imbalance": metadata.get("tick_imbalance"),
        "book_imbalance": metadata.get("book_imbalance"),
        "velocity_bps": metadata.get("velocity_bps"),
        "stop_hunt_risk": metadata.get("stop_hunt_risk"),
        "exhaustion_risk": metadata.get("exhaustion_risk"),
        "depth_available": metadata.get("depth_available"),
        "synthetic_depth_available": metadata.get("synthetic_depth_available"),
        "microstructure_source": metadata.get("microstructure_source"),
    }
    return {k: v for k, v in fallback.items() if v not in (None, "", {})}


def _parse_cross_asset(metadata: Dict[str, Any]) -> Dict[str, Any]:
    raw = metadata.get("cross_asset_context")
    snapshot = dict(raw) if isinstance(raw, dict) else {}
    fallback = {
        "score": metadata.get("cross_asset_score"),
        "alignment": metadata.get("cross_asset_alignment"),
        "confidence": metadata.get("cross_asset_confidence"),
        "state": metadata.get("cross_asset_state"),
        "supportive_direction": metadata.get("cross_asset_supportive_direction"),
        "dominant_peer": metadata.get("cross_asset_primary_peer"),
        "dominant_relation": metadata.get("cross_asset_primary_relation"),
    }
    for key, value in fallback.items():
        if key not in snapshot and value not in (None, "", {}):
            snapshot[key] = value
    return snapshot


def _safe_ratio(numerator: float, denominator: float) -> float:
    return float(numerator) / float(denominator) if denominator else 0.0


def _pattern_learning_cache_key(
    asset: str,
    category: str,
    signal: Any,
    fingerprint: Dict[str, Any],
    metadata: Dict[str, Any],
    context_cross_asset: Dict[str, Any],
) -> Tuple[str, str, str, str, str, str, str]:
    return (
        str(asset or ""),
        str(category or ""),
        str(getattr(signal, "direction", "") or ""),
        str((fingerprint or {}).get("regime", "")),
        str((fingerprint or {}).get("session", "")),
        str((fingerprint or {}).get("setup_style", "")),
        str(metadata.get("cross_asset_primary_relation") or context_cross_asset.get("dominant_relation") or ""),
    )


def _pattern_learning_feedback_for_row(row: Dict[str, Any], metadata_row: Dict[str, Any]) -> Dict[str, Any]:
    feedback = metadata_row.get("execution_feedback")
    if isinstance(feedback, dict):
        return feedback
    try:
        from services.execution_feedback_service import get_service as get_execution_feedback_service

        return get_execution_feedback_service().analyze_trade(
            {
                **row,
                "metadata": metadata_row,
            }
        )
    except Exception:
        return {}


def _pattern_learning_row_weight(
    similarity: float,
    row_direction: str,
    signal_direction: str,
    exit_time: datetime | None,
    now_utc: datetime,
    days_back: int,
) -> float:
    age_days = float(days_back)
    if exit_time is not None:
        age_days = max(0.0, (now_utc - exit_time).total_seconds() / 86400.0)
    recency_weight = max(0.35, 1.0 - min(age_days, float(days_back)) / max(float(days_back) * 1.2, 1.0))
    direction_weight = 1.0 if row_direction == signal_direction else 0.78
    return similarity * recency_weight * direction_weight


def _pattern_learning_totals() -> Dict[str, Any]:
    return {
        "weighted_total": 0.0,
        "weighted_similarity": 0.0,
        "weighted_late": 0.0,
        "weighted_premature": 0.0,
        "weighted_target_miss": 0.0,
        "weighted_stop_tight": 0.0,
        "weighted_stop_wide": 0.0,
        "weighted_stop_like": 0.0,
        "weighted_hard_loss": 0.0,
        "weighted_wins": 0.0,
        "weighted_full_target": 0.0,
        "weighted_capture": 0.0,
        "weighted_giveback": 0.0,
        "weighted_quality": 0.0,
        "weighted_rr": 0.0,
        "weighted_broker_score": 0.0,
        "weighted_micro_alignment": 0.0,
        "weighted_broker_divergence": 0.0,
        "weighted_spread_stress": 0.0,
        "weighted_quote_stale": 0.0,
        "weighted_market_transition": 0.0,
        "weighted_stop_hunt": 0.0,
        "weighted_exhaustion": 0.0,
        "weighted_synthetic_depth_loss": 0.0,
        "weighted_true_depth_win": 0.0,
        "weighted_broker_confirmed_win": 0.0,
        "weighted_cross_alignment": 0.0,
        "weighted_cross_confidence": 0.0,
        "weighted_cross_support": 0.0,
        "weighted_cross_conflict": 0.0,
        "weighted_cross_conflicted_loss": 0.0,
        "weighted_cross_confirmed_win": 0.0,
        "weighted_cross_relation_total": 0.0,
        "weighted_cross_relation_support_win": 0.0,
        "weighted_cross_relation_conflict_loss": 0.0,
        "sample_count": 0,
        "cross_relation_match_count": 0,
        "matched_examples": [],
    }


def _pattern_learning_accumulate_match(
    totals: Dict[str, Any],
    *,
    weight: float,
    similarity: float,
    feedback: Dict[str, Any],
    broker_quality: Dict[str, Any],
    micro: Dict[str, Any],
    cross_asset: Dict[str, Any],
    current_cross_relation: str,
    current_cross_peer: str,
) -> None:
    rr_realized = _safe_float(feedback.get("rr_realized"), 0.0)
    broker_score = _safe_float(broker_quality.get("score"), 0.0)
    agreement_state = str(broker_quality.get("quote_agreement_state") or "").lower()
    spread_regime = str(broker_quality.get("spread_regime") or "").lower()
    quote_quality_state = str(broker_quality.get("quote_quality_state") or "").lower()
    market_transition_risk = _safe_float(broker_quality.get("market_transition_risk"), 0.0)
    micro_alignment = _safe_float(
        cross_asset.get("microstructure_alignment", micro.get("microstructure_alignment", micro.get("score"))),
        0.0,
    )
    stop_hunt_risk = _safe_float(micro.get("stop_hunt_risk"), 0.0)
    exhaustion_risk = _safe_float(micro.get("exhaustion_risk"), 0.0)
    depth_available = bool(micro.get("depth_available"))
    synthetic_depth_available = bool(micro.get("synthetic_depth_available"))
    micro_source = str(micro.get("microstructure_source") or "")
    cross_alignment = _safe_float(cross_asset.get("alignment", cross_asset.get("score")), 0.0)
    cross_confidence = _safe_float(cross_asset.get("confidence"), 0.0)
    cross_peer = str(cross_asset.get("dominant_peer") or "").strip().upper()
    cross_relation = str(cross_asset.get("dominant_relation") or "").strip().lower()
    relation_match = bool(
        current_cross_relation
        and cross_relation == current_cross_relation
        and (not current_cross_peer or not cross_peer or cross_peer == current_cross_peer)
    )

    totals["weighted_total"] += weight
    totals["sample_count"] += 1
    totals["weighted_similarity"] += weight * similarity
    totals["weighted_late"] += weight * float(bool(feedback.get("late_entry")))
    totals["weighted_premature"] += weight * float(bool(feedback.get("premature_stop")))
    totals["weighted_target_miss"] += weight * float(bool(feedback.get("target_miss")))
    totals["weighted_stop_tight"] += weight * float(bool(feedback.get("stop_too_tight")))
    totals["weighted_stop_wide"] += weight * float(bool(feedback.get("stop_too_wide")))
    totals["weighted_stop_like"] += weight * float(str(feedback.get("exit_family") or "") in {"stop_loss", "stop_loss_offline", "trailing_stop"})
    totals["weighted_wins"] += weight * float(rr_realized > 0.15)
    totals["weighted_hard_loss"] += weight * float(rr_realized <= -0.75)
    totals["weighted_full_target"] += weight * float(bool(feedback.get("full_target")))
    totals["weighted_capture"] += weight * _safe_float(feedback.get("target_capture"), 0.0)
    totals["weighted_giveback"] += weight * _safe_float(feedback.get("giveback_ratio"), 0.0)
    totals["weighted_quality"] += weight * _safe_float(feedback.get("quality_score"), 50.0)
    totals["weighted_rr"] += weight * rr_realized
    totals["weighted_broker_score"] += weight * broker_score
    totals["weighted_micro_alignment"] += weight * micro_alignment
    totals["weighted_broker_divergence"] += weight * float(agreement_state in {"divergent", "severe_divergence"})
    totals["weighted_spread_stress"] += weight * float(spread_regime in {"wide", "stressed", "extreme"})
    totals["weighted_quote_stale"] += weight * float(quote_quality_state in {"stale", "delayed"})
    totals["weighted_market_transition"] += weight * float(
        market_transition_risk >= 0.60 or bool(broker_quality.get("market_state_changed"))
    )
    totals["weighted_stop_hunt"] += weight * float(stop_hunt_risk >= 0.45)
    totals["weighted_exhaustion"] += weight * float(exhaustion_risk >= 0.42)
    totals["weighted_synthetic_depth_loss"] += weight * float(synthetic_depth_available and rr_realized <= -0.15)
    totals["weighted_true_depth_win"] += weight * float(
        depth_available and _is_true_depth_source(micro_source) and rr_realized > 0.15
    )
    totals["weighted_broker_confirmed_win"] += weight * float(
        broker_score >= 0.65 and agreement_state in {"strong", "aligned"} and rr_realized > 0.15
    )
    totals["weighted_cross_alignment"] += weight * cross_alignment
    totals["weighted_cross_confidence"] += weight * cross_confidence
    totals["weighted_cross_support"] += weight * float(cross_alignment >= 0.20)
    totals["weighted_cross_conflict"] += weight * float(cross_alignment <= -0.20)
    totals["weighted_cross_conflicted_loss"] += weight * float(cross_alignment <= -0.20 and rr_realized <= -0.15)
    totals["weighted_cross_confirmed_win"] += weight * float(cross_alignment >= 0.20 and rr_realized > 0.15)

    relation_weight = weight * float(relation_match)
    totals["cross_relation_match_count"] += int(relation_match)
    totals["weighted_cross_relation_total"] += relation_weight
    totals["weighted_cross_relation_support_win"] += relation_weight * float(cross_alignment >= 0.20 and rr_realized > 0.15)
    totals["weighted_cross_relation_conflict_loss"] += relation_weight * float(cross_alignment <= -0.20 and rr_realized <= -0.15)

    if len(totals["matched_examples"]) < 5:
        totals["matched_examples"].append(
            {
                "similarity": round(similarity, 4),
                "exit_family": feedback.get("exit_family"),
                "rr_realized": round(rr_realized, 3),
                "quality_score": round(_safe_float(feedback.get("quality_score"), 50.0), 1),
                "broker_score": round(broker_score, 4),
                "micro_alignment": round(micro_alignment, 4),
                "cross_alignment": round(cross_alignment, 4),
                "cross_relation": cross_relation,
                "cross_peer": cross_peer,
            }
        )


def _pattern_learning_rate_snapshot(totals: Dict[str, Any]) -> Dict[str, Any]:
    weighted_total = float(totals["weighted_total"] or 0.0)
    return {
        "late_entry_rate": _safe_ratio(totals["weighted_late"], weighted_total),
        "premature_stop_rate": _safe_ratio(totals["weighted_premature"], weighted_total),
        "target_miss_rate": _safe_ratio(totals["weighted_target_miss"], weighted_total),
        "stop_too_tight_rate": _safe_ratio(totals["weighted_stop_tight"], weighted_total),
        "stop_too_wide_rate": _safe_ratio(totals["weighted_stop_wide"], weighted_total),
        "stop_like_rate": _safe_ratio(totals["weighted_stop_like"], weighted_total),
        "hard_loss_rate": _safe_ratio(totals["weighted_hard_loss"], weighted_total),
        "win_rate": _safe_ratio(totals["weighted_wins"], weighted_total),
        "full_target_rate": _safe_ratio(totals["weighted_full_target"], weighted_total),
        "avg_target_capture": _safe_ratio(totals["weighted_capture"], weighted_total),
        "avg_giveback_ratio": _safe_ratio(totals["weighted_giveback"], weighted_total),
        "avg_quality": _safe_ratio(totals["weighted_quality"], weighted_total),
        "avg_rr_realized": _safe_ratio(totals["weighted_rr"], weighted_total),
        "avg_similarity": _safe_ratio(totals["weighted_similarity"], weighted_total),
        "avg_broker_score": _safe_ratio(totals["weighted_broker_score"], weighted_total),
        "avg_micro_alignment": _safe_ratio(totals["weighted_micro_alignment"], weighted_total),
        "broker_divergence_rate": _safe_ratio(totals["weighted_broker_divergence"], weighted_total),
        "spread_stress_rate": _safe_ratio(totals["weighted_spread_stress"], weighted_total),
        "quote_stale_rate": _safe_ratio(totals["weighted_quote_stale"], weighted_total),
        "market_transition_rate": _safe_ratio(totals["weighted_market_transition"], weighted_total),
        "stop_hunt_rate": _safe_ratio(totals["weighted_stop_hunt"], weighted_total),
        "exhaustion_rate": _safe_ratio(totals["weighted_exhaustion"], weighted_total),
        "synthetic_depth_loss_rate": _safe_ratio(totals["weighted_synthetic_depth_loss"], weighted_total),
        "true_depth_win_rate": _safe_ratio(totals["weighted_true_depth_win"], weighted_total),
        "broker_confirmed_win_rate": _safe_ratio(totals["weighted_broker_confirmed_win"], weighted_total),
        "avg_cross_asset_alignment": _safe_ratio(totals["weighted_cross_alignment"], weighted_total),
        "avg_cross_asset_confidence": _safe_ratio(totals["weighted_cross_confidence"], weighted_total),
        "cross_asset_support_rate": _safe_ratio(totals["weighted_cross_support"], weighted_total),
        "cross_asset_conflict_rate": _safe_ratio(totals["weighted_cross_conflict"], weighted_total),
        "cross_asset_conflicted_loss_rate": _safe_ratio(totals["weighted_cross_conflicted_loss"], weighted_total),
        "cross_asset_confirmed_win_rate": _safe_ratio(totals["weighted_cross_confirmed_win"], weighted_total),
        "cross_asset_relation_match_count": int(totals["cross_relation_match_count"] or 0),
        "cross_asset_relation_support_win_rate": _safe_ratio(
            totals["weighted_cross_relation_support_win"],
            totals["weighted_cross_relation_total"],
        ),
        "cross_asset_relation_conflict_loss_rate": _safe_ratio(
            totals["weighted_cross_relation_conflict_loss"],
            totals["weighted_cross_relation_total"],
        ),
        "weighted_total": weighted_total,
        "sample_count": int(totals["sample_count"] or 0),
        "matched_examples": list(totals["matched_examples"] or []),
    }


def _pattern_learning_adjustments() -> Dict[str, Any]:
    return {
        "penalty_confidence": 0.0,
        "penalty_risk": 0.0,
        "penalty_rr": 0.0,
        "bonus_confidence": 0.0,
        "bonus_risk": 0.0,
        "bonus_rr_relief": 0.0,
        "cooldown_delta": 0,
        "target_rr_multiplier": 1.0,
        "notes": [],
        "block_new_entries": False,
        "block_reason": "",
    }


def _pattern_learning_apply_penalty_rules(adjustments: Dict[str, Any], rates: Dict[str, Any]) -> None:
    for condition, confidence_delta, risk_delta, rr_delta, cooldown_delta, note in [
        (rates["late_entry_rate"] >= 0.24, 0.010, 0.05, 0.0, 3, "recent_pattern_late_entry_watch"),
        (rates["late_entry_rate"] >= 0.32, 0.016, 0.07, 0.0, 4, "recent_pattern_late_entry"),
        (rates["premature_stop_rate"] >= 0.24, 0.010, 0.05, 0.0, 3, "recent_pattern_premature_stop"),
        (rates["target_miss_rate"] >= 0.26, 0.010, 0.00, 0.10, 0, "recent_pattern_target_miss"),
        (rates["stop_too_tight_rate"] >= 0.24, 0.008, 0.04, 0.0, 0, "recent_pattern_stop_too_tight"),
        (rates["stop_too_wide_rate"] >= 0.24, 0.008, 0.05, 0.05, 0, "recent_pattern_stop_too_wide"),
        (rates["hard_loss_rate"] >= 0.28 and rates["avg_rr_realized"] <= -0.18, 0.012, 0.06, 0.04, 3, "recent_pattern_negative_expectancy"),
        (rates["hard_loss_rate"] >= 0.34 and rates["avg_rr_realized"] <= -0.32, 0.018, 0.09, 0.0, 5, "recent_pattern_hard_losses"),
        (rates["broker_divergence_rate"] >= 0.34, 0.010, 0.05, 0.0, 3, "recent_pattern_broker_divergence"),
        (rates["spread_stress_rate"] >= 0.30, 0.008, 0.06, 0.06, 0, "recent_pattern_spread_stress"),
        (rates["quote_stale_rate"] >= 0.28, 0.010, 0.04, 0.0, 2, "recent_pattern_quote_stale"),
        (rates["market_transition_rate"] >= 0.30, 0.008, 0.04, 0.0, 0, "recent_pattern_market_transition"),
        (rates["stop_hunt_rate"] >= 0.30, 0.010, 0.07, 0.04, 3, "recent_pattern_stop_hunt"),
        (rates["exhaustion_rate"] >= 0.30, 0.008, 0.04, 0.0, 0, "recent_pattern_micro_exhaustion"),
        (rates["synthetic_depth_loss_rate"] >= 0.36, 0.008, 0.00, 0.05, 0, "recent_pattern_synthetic_depth_losses"),
        (rates["cross_asset_conflicted_loss_rate"] >= 0.34, 0.009, 0.05, 0.0, 3, "recent_pattern_cross_asset_conflict"),
        (
            rates["cross_asset_relation_match_count"] >= 3 and rates["cross_asset_relation_conflict_loss_rate"] >= 0.50,
            0.006,
            0.00,
            0.04,
            0,
            "recent_pattern_cross_asset_relation_failures",
        ),
        (
            rates["win_rate"] <= 0.46 and rates["avg_quality"] <= 48.0 and rates["avg_rr_realized"] <= -0.08,
            0.014,
            0.07,
            0.07,
            4,
            "recent_pattern_poor_profitability",
        ),
        (
            rates["avg_giveback_ratio"] >= 0.52 and rates["full_target_rate"] <= 0.24,
            0.008,
            0.04,
            0.05,
            0,
            "recent_pattern_profit_giveback",
        ),
    ]:
        if condition:
            adjustments["penalty_confidence"] += confidence_delta
            adjustments["penalty_risk"] += risk_delta
            adjustments["penalty_rr"] += rr_delta
            adjustments["cooldown_delta"] += cooldown_delta
            adjustments["notes"].append(note)


def _pattern_learning_apply_bonus_rules(adjustments: Dict[str, Any], rates: Dict[str, Any]) -> None:
    for condition, confidence_delta, risk_delta, rr_relief_delta, rr_multiplier_delta, note in [
        (rates["win_rate"] >= 0.60 and rates["avg_quality"] >= 60.0 and rates["avg_rr_realized"] >= 0.45, 0.010, 0.06, 0.04, 0.00, "recent_pattern_winners"),
        (rates["true_depth_win_rate"] >= 0.48 and rates["avg_micro_alignment"] >= 0.18 and rates["win_rate"] >= 0.60, 0.006, 0.04, 0.00, 0.04, "recent_pattern_true_depth_winners"),
        (rates["broker_confirmed_win_rate"] >= 0.50 and rates["avg_broker_score"] >= 0.62 and rates["win_rate"] >= 0.58, 0.005, 0.03, 0.00, 0.00, "recent_pattern_broker_confirmed_winners"),
        (rates["cross_asset_confirmed_win_rate"] >= 0.48 and rates["avg_cross_asset_alignment"] >= 0.16 and rates["win_rate"] >= 0.58, 0.006, 0.04, 0.00, 0.04, "recent_pattern_cross_asset_confirmed_winners"),
        (rates["cross_asset_relation_match_count"] >= 3 and rates["cross_asset_relation_support_win_rate"] >= 0.52, 0.004, 0.03, 0.00, 0.00, "recent_pattern_cross_asset_relation_edge"),
        (rates["full_target_rate"] >= 0.48 and rates["avg_target_capture"] >= 0.75 and rates["avg_giveback_ratio"] <= 0.28, 0.006, 0.03, 0.04, 0.08, "recent_pattern_targets_extend"),
        (rates["win_rate"] >= 0.68 and rates["avg_similarity"] >= 0.70 and rates["avg_rr_realized"] >= 0.85, 0.004, 0.03, 0.00, 0.04, "recent_pattern_high_conviction_winners"),
    ]:
        if condition:
            adjustments["bonus_confidence"] += confidence_delta
            adjustments["bonus_risk"] += risk_delta
            adjustments["bonus_rr_relief"] += rr_relief_delta
            adjustments["target_rr_multiplier"] += rr_multiplier_delta
            adjustments["notes"].append(note)


def _pattern_learning_apply_block_rules(adjustments: Dict[str, Any], rates: Dict[str, Any], *, block_samples: int, current_cross_relation: str) -> None:
    if rates["sample_count"] < block_samples:
        return

    relation_label = current_cross_relation.replace("_", " ").strip() or "related-market"
    for condition, reason in [
        (
            rates["late_entry_rate"] >= 0.42 and rates["hard_loss_rate"] >= 0.28 and rates["avg_quality"] <= 50.0,
            "recent similar setups keep failing from late entries",
        ),
        (
            rates["late_entry_rate"] >= 0.56 and rates["hard_loss_rate"] >= 0.40,
            "recent similar setups keep failing from late entries",
        ),
        (
            rates["broker_divergence_rate"] >= 0.62 and rates["hard_loss_rate"] >= 0.38,
            "recent similar setups keep failing when brokers disagree",
        ),
        (
            rates["stop_hunt_rate"] >= 0.55 and rates["avg_rr_realized"] <= -0.45,
            "recent similar setups keep failing in stop-hunt conditions",
        ),
        (
            rates["cross_asset_relation_match_count"] >= 4 and rates["cross_asset_relation_conflict_loss_rate"] >= 0.70,
            f"recent similar setups keep failing when {relation_label} spillover conflicts",
        ),
        (
            rates["stop_like_rate"] >= 0.78 and rates["avg_rr_realized"] <= -0.55 and rates["avg_quality"] <= 40.0,
            "recent similar setups are stopping out too often with poor quality",
        ),
        (
            rates["premature_stop_rate"] >= 0.55 and rates["target_miss_rate"] >= 0.55 and rates["avg_quality"] <= 38.0,
            "recent similar setups keep giving back progress before securing enough profit",
        ),
        (
            rates["sample_count"] >= max(block_samples + 2, 7)
            and rates["win_rate"] <= 0.42
            and rates["avg_rr_realized"] <= -0.10
            and rates["avg_quality"] <= 46.0,
            "recent similar setups have stayed unprofitable over a meaningful sample",
        ),
        (
            rates["avg_giveback_ratio"] >= 0.60
            and rates["full_target_rate"] <= 0.22
            and rates["avg_rr_realized"] <= -0.04,
            "recent similar setups keep wasting good trade progress before exit",
        ),
    ]:
        if condition:
            adjustments["block_new_entries"] = True
            adjustments["block_reason"] = reason
            break


def _pattern_learning_build_summary(
    *,
    asset: str,
    category: str,
    rates: Dict[str, Any],
    adjustments: Dict[str, Any],
    current_cross_relation: str,
    current_cross_peer: str,
) -> Dict[str, Any]:
    target_rr_multiplier = round(_clip(float(adjustments["target_rr_multiplier"] or 1.0), 0.88, 1.18), 4)
    return {
        "asset": asset,
        "category": category,
        "sample_count": int(rates["sample_count"] or 0),
        "avg_similarity": round(float(rates["avg_similarity"] or 0.0), 4),
        "win_rate": round(float(rates["win_rate"] or 0.0), 4),
        "full_target_rate": round(float(rates["full_target_rate"] or 0.0), 4),
        "avg_target_capture": round(float(rates["avg_target_capture"] or 0.0), 4),
        "avg_giveback_ratio": round(float(rates["avg_giveback_ratio"] or 0.0), 4),
        "late_entry_rate": round(float(rates["late_entry_rate"] or 0.0), 4),
        "premature_stop_rate": round(float(rates["premature_stop_rate"] or 0.0), 4),
        "target_miss_rate": round(float(rates["target_miss_rate"] or 0.0), 4),
        "stop_too_tight_rate": round(float(rates["stop_too_tight_rate"] or 0.0), 4),
        "stop_too_wide_rate": round(float(rates["stop_too_wide_rate"] or 0.0), 4),
        "stop_like_rate": round(float(rates["stop_like_rate"] or 0.0), 4),
        "hard_loss_rate": round(float(rates["hard_loss_rate"] or 0.0), 4),
        "broker_divergence_rate": round(float(rates["broker_divergence_rate"] or 0.0), 4),
        "spread_stress_rate": round(float(rates["spread_stress_rate"] or 0.0), 4),
        "quote_stale_rate": round(float(rates["quote_stale_rate"] or 0.0), 4),
        "market_transition_rate": round(float(rates["market_transition_rate"] or 0.0), 4),
        "stop_hunt_rate": round(float(rates["stop_hunt_rate"] or 0.0), 4),
        "exhaustion_rate": round(float(rates["exhaustion_rate"] or 0.0), 4),
        "synthetic_depth_loss_rate": round(float(rates["synthetic_depth_loss_rate"] or 0.0), 4),
        "true_depth_win_rate": round(float(rates["true_depth_win_rate"] or 0.0), 4),
        "broker_confirmed_win_rate": round(float(rates["broker_confirmed_win_rate"] or 0.0), 4),
        "avg_cross_asset_alignment": round(float(rates["avg_cross_asset_alignment"] or 0.0), 4),
        "avg_cross_asset_confidence": round(float(rates["avg_cross_asset_confidence"] or 0.0), 4),
        "cross_asset_support_rate": round(float(rates["cross_asset_support_rate"] or 0.0), 4),
        "cross_asset_conflict_rate": round(float(rates["cross_asset_conflict_rate"] or 0.0), 4),
        "cross_asset_conflicted_loss_rate": round(float(rates["cross_asset_conflicted_loss_rate"] or 0.0), 4),
        "cross_asset_confirmed_win_rate": round(float(rates["cross_asset_confirmed_win_rate"] or 0.0), 4),
        "cross_asset_relation_match_count": int(rates["cross_asset_relation_match_count"] or 0),
        "cross_asset_relation_support_win_rate": round(float(rates["cross_asset_relation_support_win_rate"] or 0.0), 4),
        "cross_asset_relation_conflict_loss_rate": round(float(rates["cross_asset_relation_conflict_loss_rate"] or 0.0), 4),
        "cross_asset_primary_relation": current_cross_relation,
        "cross_asset_primary_peer": current_cross_peer,
        "avg_quality_score": round(float(rates["avg_quality"] or 0.0), 1),
        "avg_rr_realized": round(float(rates["avg_rr_realized"] or 0.0), 4),
        "avg_broker_score": round(float(rates["avg_broker_score"] or 0.0), 4),
        "avg_micro_alignment": round(float(rates["avg_micro_alignment"] or 0.0), 4),
        "penalty_confidence": round(float(adjustments["penalty_confidence"] or 0.0), 4),
        "penalty_risk": round(float(adjustments["penalty_risk"] or 0.0), 4),
        "penalty_rr": round(float(adjustments["penalty_rr"] or 0.0), 4),
        "bonus_confidence": round(float(adjustments["bonus_confidence"] or 0.0), 4),
        "bonus_risk": round(float(adjustments["bonus_risk"] or 0.0), 4),
        "bonus_rr_relief": round(float(adjustments["bonus_rr_relief"] or 0.0), 4),
        "cooldown_delta": int(adjustments["cooldown_delta"] or 0),
        "target_rr_multiplier": target_rr_multiplier,
        "block_new_entries": bool(adjustments["block_new_entries"]),
        "block_reason": str(adjustments["block_reason"] or ""),
        "notes": list(adjustments["notes"] or []),
        "examples": list(rates["matched_examples"] or []),
    }


class RecentPatternLearningService:
    _TTL_SECONDS = 180
    _MIN_SAMPLES = 4
    _BLOCK_SAMPLES = 5
    _SIMILARITY_FLOOR = 0.5

    def __init__(self) -> None:
        self._cache: Dict[Tuple[str, str, str, str, str, str, str], Tuple[float, Dict[str, Any]]] = {}
        self._lock = threading.RLock()

    def get_profile(
        self,
        asset: str,
        category: str,
        signal: Any,
        context: Dict[str, Any] | None = None,
        days_back: int = 45,
        limit: int = 240,
    ) -> Dict[str, Any]:
        context = context or {}
        metadata = dict(getattr(signal, "metadata", {}) or {})
        context_cross_asset = context.get("cross_asset_context")
        if not isinstance(context_cross_asset, dict):
            context_cross_asset = {}
        fingerprint = None
        try:
            from services.setup_memory_service import get_service as get_setup_memory_service

            memory_service = get_setup_memory_service()
            fingerprint = memory_service.build_fingerprint(signal, context)
            similarity_fn = getattr(memory_service, "_similarity")
        except Exception:
            fingerprint = {}
            similarity_fn = None

        cache_key = _pattern_learning_cache_key(asset, category, signal, fingerprint or {}, metadata, context_cross_asset)
        now = time.time()
        with self._lock:
            cached = self._cache.get(cache_key)
            if cached and (now - cached[0]) < self._TTL_SECONDS:
                return dict(cached[1])

        rows = self._fetch_rows(asset=asset, category=category, days_back=days_back, limit=limit)
        if not rows or not fingerprint or similarity_fn is None:
            summary = self._empty_profile(asset, category)
            with self._lock:
                self._cache[cache_key] = (now, dict(summary))
            return summary

        totals = _pattern_learning_totals()
        now_utc = datetime.now(timezone.utc)
        signal_direction = str(getattr(signal, "direction", "") or "").upper()
        current_cross_relation = str(
            metadata.get("cross_asset_primary_relation") or context_cross_asset.get("dominant_relation") or ""
        ).strip().lower()
        current_cross_peer = str(
            metadata.get("cross_asset_primary_peer") or context_cross_asset.get("dominant_peer") or ""
        ).strip().upper()
        for row in rows:
            metadata_row = _parse_metadata(row.get("metadata") or row.get("trade_metadata"))
            feedback = _pattern_learning_feedback_for_row(row, metadata_row)
            if not feedback:
                continue

            historical_fp = metadata_row.get("setup_memory_fingerprint")
            if not isinstance(historical_fp, dict) or not historical_fp:
                continue

            broker_quality = _parse_broker_quality(metadata_row)
            micro = _parse_microstructure(metadata_row)
            cross_asset = _parse_cross_asset(metadata_row)

            similarity = _safe_float(similarity_fn(fingerprint, historical_fp), 0.0)
            if similarity < self._SIMILARITY_FLOOR:
                continue

            row_direction = str(row.get("direction") or feedback.get("direction") or "").upper()
            exit_time = _coerce_datetime(row.get("exit_time") or row.get("entry_time"))
            weight = _pattern_learning_row_weight(similarity, row_direction, signal_direction, exit_time, now_utc, days_back)
            _pattern_learning_accumulate_match(
                totals,
                weight=weight,
                similarity=similarity,
                feedback=feedback,
                broker_quality=broker_quality,
                micro=micro,
                cross_asset=cross_asset,
                current_cross_relation=current_cross_relation,
                current_cross_peer=current_cross_peer,
            )

        if totals["weighted_total"] <= 0 or totals["sample_count"] <= 0:
            summary = self._empty_profile(asset, category)
            with self._lock:
                self._cache[cache_key] = (now, dict(summary))
            return summary
        rates = _pattern_learning_rate_snapshot(totals)
        adjustments = _pattern_learning_adjustments()
        if rates["sample_count"] >= self._MIN_SAMPLES:
            _pattern_learning_apply_penalty_rules(adjustments, rates)
            _pattern_learning_apply_bonus_rules(adjustments, rates)
        _pattern_learning_apply_block_rules(
            adjustments,
            rates,
            block_samples=self._BLOCK_SAMPLES,
            current_cross_relation=current_cross_relation,
        )
        summary = _pattern_learning_build_summary(
            asset=asset,
            category=category,
            rates=rates,
            adjustments=adjustments,
            current_cross_relation=current_cross_relation,
            current_cross_peer=current_cross_peer,
        )

        with self._lock:
            self._cache[cache_key] = (now, dict(summary))
        return summary

    def _fetch_rows(self, asset: str, category: str, days_back: int, limit: int) -> List[Dict[str, Any]]:
        try:
            from services.db_pool import get_db

            return get_db().get_execution_feedback_trades(
                since=datetime.utcnow() - timedelta(days=days_back),
                asset=asset,
                category=category,
                limit=limit,
            )
        except Exception:
            return []

    @staticmethod
    def _empty_profile(asset: str, category: str) -> Dict[str, Any]:
        return {
            "asset": asset,
            "category": category,
            "sample_count": 0,
            "avg_similarity": 0.0,
            "win_rate": 0.0,
            "full_target_rate": 0.0,
            "avg_target_capture": 0.0,
            "avg_giveback_ratio": 0.0,
            "late_entry_rate": 0.0,
            "premature_stop_rate": 0.0,
            "target_miss_rate": 0.0,
            "stop_too_tight_rate": 0.0,
            "stop_too_wide_rate": 0.0,
            "stop_like_rate": 0.0,
            "hard_loss_rate": 0.0,
            "broker_divergence_rate": 0.0,
            "spread_stress_rate": 0.0,
            "quote_stale_rate": 0.0,
            "market_transition_rate": 0.0,
            "stop_hunt_rate": 0.0,
            "exhaustion_rate": 0.0,
            "synthetic_depth_loss_rate": 0.0,
            "true_depth_win_rate": 0.0,
            "broker_confirmed_win_rate": 0.0,
            "avg_cross_asset_alignment": 0.0,
            "avg_cross_asset_confidence": 0.0,
            "cross_asset_support_rate": 0.0,
            "cross_asset_conflict_rate": 0.0,
            "cross_asset_conflicted_loss_rate": 0.0,
            "cross_asset_confirmed_win_rate": 0.0,
            "cross_asset_relation_match_count": 0,
            "cross_asset_relation_support_win_rate": 0.0,
            "cross_asset_relation_conflict_loss_rate": 0.0,
            "cross_asset_primary_relation": "",
            "cross_asset_primary_peer": "",
            "avg_quality_score": 50.0,
            "avg_rr_realized": 0.0,
            "avg_broker_score": 0.0,
            "avg_micro_alignment": 0.0,
            "penalty_confidence": 0.0,
            "penalty_risk": 0.0,
            "penalty_rr": 0.0,
            "bonus_confidence": 0.0,
            "bonus_risk": 0.0,
            "bonus_rr_relief": 0.0,
            "cooldown_delta": 0,
            "target_rr_multiplier": 1.0,
            "block_new_entries": False,
            "block_reason": "",
            "notes": [],
            "examples": [],
        }


_service = RecentPatternLearningService()


def get_service() -> RecentPatternLearningService:
    return _service
