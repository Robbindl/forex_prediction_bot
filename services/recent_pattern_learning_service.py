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


class RecentPatternLearningService:
    _TTL_SECONDS = 180
    _MIN_SAMPLES = 4
    _BLOCK_SAMPLES = 6
    _SIMILARITY_FLOOR = 0.52

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

        cache_key = (
            str(asset or ""),
            str(category or ""),
            str(getattr(signal, "direction", "") or ""),
            str((fingerprint or {}).get("regime", "")),
            str((fingerprint or {}).get("session", "")),
            str((fingerprint or {}).get("setup_style", "")),
            str(metadata.get("cross_asset_primary_relation") or context_cross_asset.get("dominant_relation") or ""),
        )
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

        weighted_total = 0.0
        weighted_similarity = 0.0
        weighted_late = 0.0
        weighted_premature = 0.0
        weighted_target_miss = 0.0
        weighted_stop_tight = 0.0
        weighted_stop_wide = 0.0
        weighted_stop_like = 0.0
        weighted_hard_loss = 0.0
        weighted_wins = 0.0
        weighted_full_target = 0.0
        weighted_capture = 0.0
        weighted_giveback = 0.0
        weighted_quality = 0.0
        weighted_rr = 0.0
        weighted_broker_score = 0.0
        weighted_micro_alignment = 0.0
        weighted_broker_divergence = 0.0
        weighted_spread_stress = 0.0
        weighted_quote_stale = 0.0
        weighted_market_transition = 0.0
        weighted_stop_hunt = 0.0
        weighted_exhaustion = 0.0
        weighted_synthetic_depth_loss = 0.0
        weighted_true_depth_win = 0.0
        weighted_broker_confirmed_win = 0.0
        weighted_cross_alignment = 0.0
        weighted_cross_confidence = 0.0
        weighted_cross_support = 0.0
        weighted_cross_conflict = 0.0
        weighted_cross_conflicted_loss = 0.0
        weighted_cross_confirmed_win = 0.0
        weighted_cross_relation_total = 0.0
        weighted_cross_relation_support_win = 0.0
        weighted_cross_relation_conflict_loss = 0.0
        sample_count = 0
        cross_relation_match_count = 0
        matched_examples: List[Dict[str, Any]] = []

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
            feedback = metadata_row.get("execution_feedback")
            if not isinstance(feedback, dict):
                try:
                    from services.execution_feedback_service import get_service as get_execution_feedback_service

                    feedback = get_execution_feedback_service().analyze_trade(
                        {
                            **row,
                            "metadata": metadata_row,
                        }
                    )
                except Exception:
                    feedback = {}
            if not isinstance(feedback, dict) or not feedback:
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
            direction_weight = 1.0 if row_direction == signal_direction else 0.78
            exit_time = _coerce_datetime(row.get("exit_time") or row.get("entry_time"))
            age_days = float(days_back)
            if exit_time is not None:
                age_days = max(0.0, (now_utc - exit_time).total_seconds() / 86400.0)
            recency_weight = max(0.35, 1.0 - min(age_days, float(days_back)) / max(float(days_back) * 1.2, 1.0))
            weight = similarity * recency_weight * direction_weight

            weighted_total += weight
            sample_count += 1
            weighted_similarity += weight * similarity
            weighted_late += weight * (1.0 if feedback.get("late_entry") else 0.0)
            weighted_premature += weight * (1.0 if feedback.get("premature_stop") else 0.0)
            weighted_target_miss += weight * (1.0 if feedback.get("target_miss") else 0.0)
            weighted_stop_tight += weight * (1.0 if feedback.get("stop_too_tight") else 0.0)
            weighted_stop_wide += weight * (1.0 if feedback.get("stop_too_wide") else 0.0)
            weighted_stop_like += weight * (
                1.0 if str(feedback.get("exit_family") or "") in {"stop_loss", "stop_loss_offline", "trailing_stop"} else 0.0
            )
            rr_realized = _safe_float(feedback.get("rr_realized"), 0.0)
            weighted_wins += weight * (1.0 if rr_realized > 0.15 else 0.0)
            weighted_hard_loss += weight * (1.0 if rr_realized <= -0.75 else 0.0)
            weighted_full_target += weight * (1.0 if feedback.get("full_target") else 0.0)
            weighted_capture += weight * _safe_float(feedback.get("target_capture"), 0.0)
            weighted_giveback += weight * _safe_float(feedback.get("giveback_ratio"), 0.0)
            weighted_quality += weight * _safe_float(feedback.get("quality_score"), 50.0)
            weighted_rr += weight * rr_realized

            broker_score = _safe_float(broker_quality.get("score"), 0.0)
            agreement_state = str(broker_quality.get("quote_agreement_state") or "").lower()
            spread_regime = str(broker_quality.get("spread_regime") or "").lower()
            quote_quality_state = str(broker_quality.get("quote_quality_state") or "").lower()
            market_transition_risk = _safe_float(broker_quality.get("market_transition_risk"), 0.0)

            micro_alignment = _safe_float(
                metadata_row.get("microstructure_alignment", micro.get("microstructure_alignment", micro.get("score"))),
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

            weighted_broker_score += weight * broker_score
            weighted_micro_alignment += weight * micro_alignment
            weighted_broker_divergence += weight * (
                1.0 if agreement_state in {"divergent", "severe_divergence"} else 0.0
            )
            weighted_spread_stress += weight * (
                1.0 if spread_regime in {"wide", "stressed", "extreme"} else 0.0
            )
            weighted_quote_stale += weight * (
                1.0 if quote_quality_state in {"stale", "delayed"} else 0.0
            )
            weighted_market_transition += weight * (
                1.0 if market_transition_risk >= 0.60 or broker_quality.get("market_state_changed") else 0.0
            )
            weighted_stop_hunt += weight * (1.0 if stop_hunt_risk >= 0.45 else 0.0)
            weighted_exhaustion += weight * (1.0 if exhaustion_risk >= 0.42 else 0.0)
            weighted_synthetic_depth_loss += weight * (
                1.0 if synthetic_depth_available and rr_realized <= -0.15 else 0.0
            )
            weighted_true_depth_win += weight * (
                1.0 if depth_available and micro_source == "order_flow_true_depth" and rr_realized > 0.15 else 0.0
            )
            weighted_broker_confirmed_win += weight * (
                1.0
                if broker_score >= 0.65
                and agreement_state in {"strong", "aligned"}
                and rr_realized > 0.15
                else 0.0
            )
            weighted_cross_alignment += weight * cross_alignment
            weighted_cross_confidence += weight * cross_confidence
            weighted_cross_support += weight * (1.0 if cross_alignment >= 0.20 else 0.0)
            weighted_cross_conflict += weight * (1.0 if cross_alignment <= -0.20 else 0.0)
            weighted_cross_conflicted_loss += weight * (
                1.0 if cross_alignment <= -0.20 and rr_realized <= -0.15 else 0.0
            )
            weighted_cross_confirmed_win += weight * (
                1.0 if cross_alignment >= 0.20 and rr_realized > 0.15 else 0.0
            )
            if current_cross_relation and cross_relation == current_cross_relation and (
                not current_cross_peer or not cross_peer or cross_peer == current_cross_peer
            ):
                cross_relation_match_count += 1
                weighted_cross_relation_total += weight
                weighted_cross_relation_support_win += weight * (
                    1.0 if cross_alignment >= 0.20 and rr_realized > 0.15 else 0.0
                )
                weighted_cross_relation_conflict_loss += weight * (
                    1.0 if cross_alignment <= -0.20 and rr_realized <= -0.15 else 0.0
                )

            if len(matched_examples) < 5:
                matched_examples.append(
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

        if weighted_total <= 0 or sample_count <= 0:
            summary = self._empty_profile(asset, category)
            with self._lock:
                self._cache[cache_key] = (now, dict(summary))
            return summary

        late_entry_rate = weighted_late / weighted_total
        premature_stop_rate = weighted_premature / weighted_total
        target_miss_rate = weighted_target_miss / weighted_total
        stop_too_tight_rate = weighted_stop_tight / weighted_total
        stop_too_wide_rate = weighted_stop_wide / weighted_total
        stop_like_rate = weighted_stop_like / weighted_total
        hard_loss_rate = weighted_hard_loss / weighted_total
        win_rate = weighted_wins / weighted_total
        full_target_rate = weighted_full_target / weighted_total
        avg_target_capture = weighted_capture / weighted_total
        avg_giveback_ratio = weighted_giveback / weighted_total
        avg_quality = weighted_quality / weighted_total
        avg_rr_realized = weighted_rr / weighted_total
        avg_similarity = weighted_similarity / weighted_total
        avg_broker_score = weighted_broker_score / weighted_total
        avg_micro_alignment = weighted_micro_alignment / weighted_total
        broker_divergence_rate = weighted_broker_divergence / weighted_total
        spread_stress_rate = weighted_spread_stress / weighted_total
        quote_stale_rate = weighted_quote_stale / weighted_total
        market_transition_rate = weighted_market_transition / weighted_total
        stop_hunt_rate = weighted_stop_hunt / weighted_total
        exhaustion_rate = weighted_exhaustion / weighted_total
        synthetic_depth_loss_rate = weighted_synthetic_depth_loss / weighted_total
        true_depth_win_rate = weighted_true_depth_win / weighted_total
        broker_confirmed_win_rate = weighted_broker_confirmed_win / weighted_total
        avg_cross_asset_alignment = weighted_cross_alignment / weighted_total
        avg_cross_asset_confidence = weighted_cross_confidence / weighted_total
        cross_asset_support_rate = weighted_cross_support / weighted_total
        cross_asset_conflict_rate = weighted_cross_conflict / weighted_total
        cross_asset_conflicted_loss_rate = weighted_cross_conflicted_loss / weighted_total
        cross_asset_confirmed_win_rate = weighted_cross_confirmed_win / weighted_total
        if weighted_cross_relation_total > 0.0:
            cross_asset_relation_support_win_rate = weighted_cross_relation_support_win / weighted_cross_relation_total
            cross_asset_relation_conflict_loss_rate = weighted_cross_relation_conflict_loss / weighted_cross_relation_total
        else:
            cross_asset_relation_support_win_rate = 0.0
            cross_asset_relation_conflict_loss_rate = 0.0

        penalty_confidence = 0.0
        penalty_risk = 0.0
        penalty_rr = 0.0
        bonus_confidence = 0.0
        bonus_risk = 0.0
        bonus_rr_relief = 0.0
        cooldown_delta = 0
        target_rr_multiplier = 1.0
        notes: List[str] = []

        if sample_count >= self._MIN_SAMPLES:
            if late_entry_rate >= 0.34:
                penalty_confidence += 0.014
                penalty_risk += 0.06
                cooldown_delta += 4
                notes.append("recent_pattern_late_entry")
            if premature_stop_rate >= 0.28:
                penalty_confidence += 0.010
                penalty_risk += 0.05
                cooldown_delta += 3
                notes.append("recent_pattern_premature_stop")
            if target_miss_rate >= 0.30:
                penalty_confidence += 0.008
                penalty_rr += 0.08
                target_rr_multiplier -= 0.06
                notes.append("recent_pattern_target_miss")
            if stop_too_tight_rate >= 0.28:
                penalty_confidence += 0.008
                penalty_risk += 0.04
                notes.append("recent_pattern_stop_too_tight")
            if stop_too_wide_rate >= 0.28:
                penalty_confidence += 0.008
                penalty_risk += 0.05
                penalty_rr += 0.05
                notes.append("recent_pattern_stop_too_wide")
            if hard_loss_rate >= 0.38 and avg_rr_realized <= -0.40:
                penalty_confidence += 0.016
                penalty_risk += 0.08
                cooldown_delta += 5
                notes.append("recent_pattern_hard_losses")
            if broker_divergence_rate >= 0.34:
                penalty_confidence += 0.010
                penalty_risk += 0.05
                cooldown_delta += 3
                notes.append("recent_pattern_broker_divergence")
            if spread_stress_rate >= 0.30:
                penalty_confidence += 0.008
                penalty_risk += 0.06
                penalty_rr += 0.06
                notes.append("recent_pattern_spread_stress")
            if quote_stale_rate >= 0.28:
                penalty_confidence += 0.010
                penalty_risk += 0.04
                cooldown_delta += 2
                notes.append("recent_pattern_quote_stale")
            if market_transition_rate >= 0.30:
                penalty_confidence += 0.008
                penalty_risk += 0.04
                notes.append("recent_pattern_market_transition")
            if stop_hunt_rate >= 0.30:
                penalty_confidence += 0.010
                penalty_risk += 0.07
                penalty_rr += 0.04
                cooldown_delta += 3
                notes.append("recent_pattern_stop_hunt")
            if exhaustion_rate >= 0.30:
                penalty_confidence += 0.008
                penalty_risk += 0.04
                notes.append("recent_pattern_micro_exhaustion")
            if synthetic_depth_loss_rate >= 0.36:
                penalty_confidence += 0.008
                penalty_rr += 0.05
                notes.append("recent_pattern_synthetic_depth_losses")
            if cross_asset_conflicted_loss_rate >= 0.34:
                penalty_confidence += 0.009
                penalty_risk += 0.05
                cooldown_delta += 3
                notes.append("recent_pattern_cross_asset_conflict")
            if cross_relation_match_count >= 3 and cross_asset_relation_conflict_loss_rate >= 0.50:
                penalty_confidence += 0.006
                penalty_rr += 0.04
                notes.append("recent_pattern_cross_asset_relation_failures")

            if win_rate >= 0.60 and avg_quality >= 60.0 and avg_rr_realized >= 0.45:
                bonus_confidence += 0.010
                bonus_risk += 0.06
                bonus_rr_relief += 0.04
                cooldown_delta -= 2
                notes.append("recent_pattern_winners")
            if true_depth_win_rate >= 0.48 and avg_micro_alignment >= 0.18 and win_rate >= 0.60:
                bonus_confidence += 0.006
                bonus_risk += 0.04
                target_rr_multiplier += 0.04
                notes.append("recent_pattern_true_depth_winners")
            if broker_confirmed_win_rate >= 0.50 and avg_broker_score >= 0.62 and win_rate >= 0.58:
                bonus_confidence += 0.005
                bonus_risk += 0.03
                notes.append("recent_pattern_broker_confirmed_winners")
            if cross_asset_confirmed_win_rate >= 0.48 and avg_cross_asset_alignment >= 0.16 and win_rate >= 0.58:
                bonus_confidence += 0.006
                bonus_risk += 0.04
                target_rr_multiplier += 0.04
                notes.append("recent_pattern_cross_asset_confirmed_winners")
            if cross_relation_match_count >= 3 and cross_asset_relation_support_win_rate >= 0.52:
                bonus_confidence += 0.004
                bonus_risk += 0.03
                notes.append("recent_pattern_cross_asset_relation_edge")

            if full_target_rate >= 0.48 and avg_target_capture >= 0.75 and avg_giveback_ratio <= 0.28:
                bonus_confidence += 0.006
                bonus_risk += 0.03
                bonus_rr_relief += 0.04
                target_rr_multiplier += 0.08
                notes.append("recent_pattern_targets_extend")

            if win_rate >= 0.68 and avg_similarity >= 0.70 and avg_rr_realized >= 0.85:
                bonus_confidence += 0.004
                bonus_risk += 0.03
                target_rr_multiplier += 0.04
                notes.append("recent_pattern_high_conviction_winners")

        block_new_entries = False
        block_reason = ""
        if sample_count >= self._BLOCK_SAMPLES:
            if late_entry_rate >= 0.62 and hard_loss_rate >= 0.45:
                block_new_entries = True
                block_reason = "recent similar setups keep failing from late entries"
            elif broker_divergence_rate >= 0.62 and hard_loss_rate >= 0.38:
                block_new_entries = True
                block_reason = "recent similar setups keep failing when brokers disagree"
            elif stop_hunt_rate >= 0.55 and avg_rr_realized <= -0.45:
                block_new_entries = True
                block_reason = "recent similar setups keep failing in stop-hunt conditions"
            elif cross_relation_match_count >= 4 and cross_asset_relation_conflict_loss_rate >= 0.70:
                relation_label = current_cross_relation.replace("_", " ").strip() or "related-market"
                block_new_entries = True
                block_reason = f"recent similar setups keep failing when {relation_label} spillover conflicts"
            elif stop_like_rate >= 0.78 and avg_rr_realized <= -0.55 and avg_quality <= 40.0:
                block_new_entries = True
                block_reason = "recent similar setups are stopping out too often with poor quality"
            elif premature_stop_rate >= 0.55 and target_miss_rate >= 0.55 and avg_quality <= 38.0:
                block_new_entries = True
                block_reason = "recent similar setups keep giving back progress before securing enough profit"

        target_rr_multiplier = round(_clip(target_rr_multiplier, 0.88, 1.18), 4)

        summary = {
            "asset": asset,
            "category": category,
            "sample_count": int(sample_count),
            "avg_similarity": round(avg_similarity, 4),
            "win_rate": round(win_rate, 4),
            "full_target_rate": round(full_target_rate, 4),
            "avg_target_capture": round(avg_target_capture, 4),
            "avg_giveback_ratio": round(avg_giveback_ratio, 4),
            "late_entry_rate": round(late_entry_rate, 4),
            "premature_stop_rate": round(premature_stop_rate, 4),
            "target_miss_rate": round(target_miss_rate, 4),
            "stop_too_tight_rate": round(stop_too_tight_rate, 4),
            "stop_too_wide_rate": round(stop_too_wide_rate, 4),
            "stop_like_rate": round(stop_like_rate, 4),
            "hard_loss_rate": round(hard_loss_rate, 4),
            "broker_divergence_rate": round(broker_divergence_rate, 4),
            "spread_stress_rate": round(spread_stress_rate, 4),
            "quote_stale_rate": round(quote_stale_rate, 4),
            "market_transition_rate": round(market_transition_rate, 4),
            "stop_hunt_rate": round(stop_hunt_rate, 4),
            "exhaustion_rate": round(exhaustion_rate, 4),
            "synthetic_depth_loss_rate": round(synthetic_depth_loss_rate, 4),
            "true_depth_win_rate": round(true_depth_win_rate, 4),
            "broker_confirmed_win_rate": round(broker_confirmed_win_rate, 4),
            "avg_cross_asset_alignment": round(avg_cross_asset_alignment, 4),
            "avg_cross_asset_confidence": round(avg_cross_asset_confidence, 4),
            "cross_asset_support_rate": round(cross_asset_support_rate, 4),
            "cross_asset_conflict_rate": round(cross_asset_conflict_rate, 4),
            "cross_asset_conflicted_loss_rate": round(cross_asset_conflicted_loss_rate, 4),
            "cross_asset_confirmed_win_rate": round(cross_asset_confirmed_win_rate, 4),
            "cross_asset_relation_match_count": int(cross_relation_match_count),
            "cross_asset_relation_support_win_rate": round(cross_asset_relation_support_win_rate, 4),
            "cross_asset_relation_conflict_loss_rate": round(cross_asset_relation_conflict_loss_rate, 4),
            "cross_asset_primary_relation": current_cross_relation,
            "cross_asset_primary_peer": current_cross_peer,
            "avg_quality_score": round(avg_quality, 1),
            "avg_rr_realized": round(avg_rr_realized, 4),
            "avg_broker_score": round(avg_broker_score, 4),
            "avg_micro_alignment": round(avg_micro_alignment, 4),
            "penalty_confidence": round(penalty_confidence, 4),
            "penalty_risk": round(penalty_risk, 4),
            "penalty_rr": round(penalty_rr, 4),
            "bonus_confidence": round(bonus_confidence, 4),
            "bonus_risk": round(bonus_risk, 4),
            "bonus_rr_relief": round(bonus_rr_relief, 4),
            "cooldown_delta": int(cooldown_delta),
            "target_rr_multiplier": target_rr_multiplier,
            "block_new_entries": block_new_entries,
            "block_reason": block_reason,
            "notes": notes,
            "examples": matched_examples,
        }

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
