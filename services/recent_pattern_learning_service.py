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


class RecentPatternLearningService:
    _TTL_SECONDS = 180
    _MIN_SAMPLES = 4
    _BLOCK_SAMPLES = 6
    _SIMILARITY_FLOOR = 0.52

    def __init__(self) -> None:
        self._cache: Dict[Tuple[str, str, str, str, str], Tuple[float, Dict[str, Any]]] = {}
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
            str((fingerprint or {}).get("setup_style", "")),
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
        sample_count = 0
        matched_examples: List[Dict[str, Any]] = []

        now_utc = datetime.now(timezone.utc)
        signal_direction = str(getattr(signal, "direction", "") or "").upper()
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

            if len(matched_examples) < 5:
                matched_examples.append(
                    {
                        "similarity": round(similarity, 4),
                        "exit_family": feedback.get("exit_family"),
                        "rr_realized": round(rr_realized, 3),
                        "quality_score": round(_safe_float(feedback.get("quality_score"), 50.0), 1),
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

            if win_rate >= 0.60 and avg_quality >= 60.0 and avg_rr_realized >= 0.45:
                bonus_confidence += 0.010
                bonus_risk += 0.06
                bonus_rr_relief += 0.04
                cooldown_delta -= 2
                notes.append("recent_pattern_winners")

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
            "avg_quality_score": round(avg_quality, 1),
            "avg_rr_realized": round(avg_rr_realized, 4),
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
            "avg_quality_score": 50.0,
            "avg_rr_realized": 0.0,
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
