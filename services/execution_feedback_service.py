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


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except Exception:
        return int(default)


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


def _weighted_mean(total: float, weight: float) -> float:
    if weight <= 0:
        return 0.0
    return total / weight


class ExecutionFeedbackService:
    _TTL_SECONDS = 180

    def __init__(self) -> None:
        self._cache: Dict[Tuple[str, str, int, int], Tuple[float, List[Dict[str, Any]]]] = {}
        self._summary_cache: Dict[Tuple[str, str, int, int], Tuple[float, Dict[str, Any]]] = {}
        self._lock = threading.RLock()

    def analyze_trade(self, trade: Dict[str, Any]) -> Dict[str, Any]:
        metadata = _parse_metadata(trade.get("metadata") or trade.get("trade_metadata"))
        feedback_policy = metadata.get("execution_feedback_policy") or {}
        if not isinstance(feedback_policy, dict):
            feedback_policy = {}
        paper_execution = metadata.get("paper_execution") or {}
        if not isinstance(paper_execution, dict):
            paper_execution = {}

        asset = str(trade.get("canonical_asset") or trade.get("asset") or "")
        category = str(trade.get("category") or "")
        direction = str(trade.get("direction") or trade.get("signal") or "BUY").upper()
        entry_price = _safe_float(trade.get("entry_price"), 0.0)
        exit_price = _safe_float(trade.get("exit_price"), entry_price)
        stop_loss = _safe_float(trade.get("stop_loss"), 0.0)
        original_sl = _safe_float(trade.get("original_sl"), stop_loss)
        take_profit = _safe_float(
            trade.get("take_profit"),
            _safe_float(metadata.get("take_profit"), 0.0),
        )
        highest_price = _safe_float(trade.get("highest_price"), entry_price)
        lowest_price = _safe_float(trade.get("lowest_price"), entry_price)
        highest_price = max(highest_price, entry_price, exit_price)
        lowest_price = min(lowest_price, entry_price, exit_price)

        pnl = _safe_float(trade.get("pnl"), 0.0)
        duration_minutes = max(0, _safe_int(trade.get("duration_minutes"), 0))
        exit_reason = str(trade.get("exit_reason") or "").strip()
        exit_family = self._classify_exit(exit_reason, bool(trade.get("is_partial_close")))

        sign = 1.0 if direction == "BUY" else -1.0
        initial_risk = abs(entry_price - (original_sl or stop_loss))
        target_distance = abs(take_profit - entry_price)
        realized_move = sign * (exit_price - entry_price)
        favorable_move = (
            max(0.0, highest_price - entry_price)
            if direction == "BUY"
            else max(0.0, entry_price - lowest_price)
        )
        adverse_move = (
            max(0.0, entry_price - lowest_price)
            if direction == "BUY"
            else max(0.0, highest_price - entry_price)
        )

        rr_realized = realized_move / initial_risk if initial_risk > 0 else 0.0
        mfe_rr = favorable_move / initial_risk if initial_risk > 0 else 0.0
        mae_rr = adverse_move / initial_risk if initial_risk > 0 else 0.0
        target_capture = (
            _clip(max(0.0, realized_move) / target_distance, 0.0, 2.5)
            if target_distance > 0
            else 0.0
        )
        giveback_ratio = (
            _clip((favorable_move - max(0.0, realized_move)) / favorable_move, 0.0, 1.0)
            if favorable_move > 0
            else 0.0
        )

        stop_like = exit_family in {"stop_loss", "stop_loss_offline", "trailing_stop"}
        target_like = exit_family in {"take_profit", "take_profit_offline", "partial_tp"}
        full_target = exit_family in {"take_profit", "take_profit_offline"} and target_capture >= 0.95
        premature_stop = stop_like and mfe_rr >= 0.75 and rr_realized < 0.25
        late_entry = (
            stop_like
            and duration_minutes <= self._late_entry_threshold_minutes(metadata)
            and mae_rr >= 0.85
            and mfe_rr <= 0.35
        )
        target_miss = (
            target_distance > 0
            and favorable_move >= target_distance * 0.90
            and max(0.0, realized_move) < target_distance * 0.55
            and not target_like
        )
        stop_too_wide = stop_like and favorable_move <= initial_risk * 0.15 and mae_rr >= 1.0
        stop_too_tight = stop_like and favorable_move >= initial_risk * 0.60 and rr_realized <= 0.10

        notes: List[str] = []
        if premature_stop:
            notes.append("premature_stop")
        if late_entry:
            notes.append("late_entry")
        if target_miss:
            notes.append("target_miss")
        if stop_too_wide:
            notes.append("stop_too_wide")
        if stop_too_tight:
            notes.append("stop_too_tight")
        if full_target:
            notes.append("full_target")
        if exit_family == "partial_tp":
            notes.append("partial_take_profit")

        requested_entry_price = _safe_float(paper_execution.get("requested_entry_price"), entry_price)
        requested_exit_price = _safe_float(paper_execution.get("requested_exit_price"), exit_price)
        entry_fill_delta_pct = (
            abs(entry_price - requested_entry_price) / requested_entry_price
            if requested_entry_price > 0
            else 0.0
        )
        exit_fill_delta_pct = (
            abs(exit_price - requested_exit_price) / requested_exit_price
            if requested_exit_price > 0
            else 0.0
        )
        entry_commission = _safe_float(paper_execution.get("entry_commission"), 0.0)
        exit_commission = _safe_float(paper_execution.get("exit_commission"), 0.0)
        total_commission = _safe_float(
            paper_execution.get("total_commission"),
            entry_commission + exit_commission,
        )
        execution_drag_rr = total_commission / initial_risk if initial_risk > 0 else 0.0
        if execution_drag_rr > 0.08:
            notes.append("execution_drag_high")

        quality_score = 50.0
        quality_score += rr_realized * 14.0
        quality_score += target_capture * 10.0
        quality_score += (1.0 - giveback_ratio) * 8.0
        quality_score += 5.0 if target_like else 0.0
        quality_score += 3.0 if bool(trade.get("is_partial_close")) else 0.0
        quality_score -= 14.0 if premature_stop else 0.0
        quality_score -= 10.0 if late_entry else 0.0
        quality_score -= 8.0 if stop_too_wide else 0.0
        quality_score -= 6.0 if stop_too_tight else 0.0
        quality_score = round(_clip(quality_score, 0.0, 100.0), 1)

        if rr_realized >= 1.5:
            outcome_class = "strong_win"
        elif rr_realized > 0.15:
            outcome_class = "win"
        elif rr_realized > -0.15:
            outcome_class = "scratch"
        elif rr_realized > -1.0:
            outcome_class = "loss"
        else:
            outcome_class = "hard_loss"

        adaptive_policy = metadata.get("adaptive_policy") or {}
        if not isinstance(adaptive_policy, dict):
            adaptive_policy = {}
        risk_multiplier = _safe_float(adaptive_policy.get("risk_multiplier"), 1.0)
        if risk_multiplier >= 1.12:
            adaptive_posture = "aggressive"
        elif risk_multiplier <= 0.88:
            adaptive_posture = "defensive"
        else:
            adaptive_posture = "neutral"

        return {
            "version": 1,
            "asset": asset,
            "category": category,
            "direction": direction,
            "exit_family": exit_family,
            "exit_reason": exit_reason,
            "partial_close": bool(trade.get("is_partial_close")),
            "duration_minutes": duration_minutes,
            "duration_bucket": self._duration_bucket(duration_minutes),
            "initial_risk": round(initial_risk, 6),
            "target_distance": round(target_distance, 6),
            "realized_move": round(realized_move, 6),
            "favorable_move": round(favorable_move, 6),
            "adverse_move": round(adverse_move, 6),
            "rr_realized": round(rr_realized, 4),
            "mfe_rr": round(mfe_rr, 4),
            "mae_rr": round(mae_rr, 4),
            "target_capture": round(target_capture, 4),
            "giveback_ratio": round(giveback_ratio, 4),
            "premature_stop": premature_stop,
            "late_entry": late_entry,
            "target_miss": target_miss,
            "stop_too_wide": stop_too_wide,
            "stop_too_tight": stop_too_tight,
            "full_target": full_target,
            "quality_score": quality_score,
            "outcome_class": outcome_class,
            "pnl": round(pnl, 6),
            "regime": str(metadata.get("regime") or metadata.get("market_structure", {}).get("regime") or "unknown"),
            "structure_bias": str(metadata.get("structure_bias") or metadata.get("market_structure", {}).get("structure_bias") or "neutral"),
            "setup_quality": round(_safe_float(metadata.get("setup_quality"), 0.0), 4),
            "opportunity_score": round(_safe_float(metadata.get("opportunity_score"), 0.0), 4),
            "memory_score": round(_safe_float(metadata.get("memory_score"), 0.0), 4),
            "adaptive_posture": adaptive_posture,
            "target_rr_multiplier": round(_safe_float(feedback_policy.get("target_rr_multiplier"), 1.0), 4),
            "stop_buffer_multiplier": round(_safe_float(feedback_policy.get("stop_buffer_multiplier"), 1.0), 4),
            "sample_count": _safe_int(feedback_policy.get("sample_count"), 0),
            "entry_fill_delta_pct": round(entry_fill_delta_pct, 6),
            "exit_fill_delta_pct": round(exit_fill_delta_pct, 6),
            "entry_commission": round(entry_commission, 6),
            "exit_commission": round(exit_commission, 6),
            "total_commission": round(total_commission, 6),
            "execution_drag_rr": round(execution_drag_rr, 6),
            "notes": notes,
        }

    def summarize_history(
        self,
        asset: str = "",
        category: str = "",
        days_back: int = 120,
        limit: int = 500,
    ) -> Dict[str, Any]:
        key = (asset, category, days_back, limit)
        now = time.time()
        with self._lock:
            cached = self._summary_cache.get(key)
            if cached and (now - cached[0]) < self._TTL_SECONDS:
                return dict(cached[1])

        rows = self._fetch_rows(asset=asset, category=category, days_back=days_back, limit=limit)
        if not rows:
            summary = {
                "sample_count": 0,
                "win_rate": 0.0,
                "target_hit_rate": 0.0,
                "stop_like_rate": 0.0,
                "premature_stop_rate": 0.0,
                "late_entry_rate": 0.0,
                "target_miss_rate": 0.0,
                "avg_rr_realized": 0.0,
                "avg_mfe_rr": 0.0,
                "avg_mae_rr": 0.0,
                "avg_target_capture": 0.0,
                "avg_giveback_ratio": 0.0,
                "avg_quality_score": 50.0,
                "avg_duration_minutes": 0.0,
                "exit_mix": {},
            }
            with self._lock:
                self._summary_cache[key] = (now, dict(summary))
            return summary

        total_weight = 0.0
        weighted_wins = 0.0
        weighted_target_hits = 0.0
        weighted_stop_like = 0.0
        weighted_premature = 0.0
        weighted_late = 0.0
        weighted_target_miss = 0.0
        weighted_rr = 0.0
        weighted_mfe = 0.0
        weighted_mae = 0.0
        weighted_capture = 0.0
        weighted_giveback = 0.0
        weighted_quality = 0.0
        weighted_duration = 0.0
        exit_mix: Dict[str, float] = {}
        samples = 0

        now_utc = datetime.now(timezone.utc)
        for row in rows:
            metadata = _parse_metadata(row.get("metadata") or row.get("trade_metadata"))
            feedback = metadata.get("execution_feedback")
            if not isinstance(feedback, dict):
                feedback = self.analyze_trade(row)

            exit_time = _coerce_datetime(row.get("exit_time") or row.get("entry_time"))
            age_days = float(days_back)
            if exit_time is not None:
                age_days = max(0.0, (now_utc - exit_time).total_seconds() / 86400.0)
            recency_weight = max(0.35, 1.0 - min(age_days, float(days_back)) / max(float(days_back) * 1.35, 1.0))
            partial_weight = 0.65 if feedback.get("partial_close") else 1.0
            weight = recency_weight * partial_weight

            total_weight += weight
            samples += 1
            weighted_wins += weight * (1.0 if _safe_float(feedback.get("rr_realized"), 0.0) > 0 else 0.0)
            weighted_target_hits += weight * (1.0 if feedback.get("full_target") else 0.0)
            weighted_stop_like += weight * (1.0 if feedback.get("exit_family") in {"stop_loss", "stop_loss_offline", "trailing_stop"} else 0.0)
            weighted_premature += weight * (1.0 if feedback.get("premature_stop") else 0.0)
            weighted_late += weight * (1.0 if feedback.get("late_entry") else 0.0)
            weighted_target_miss += weight * (1.0 if feedback.get("target_miss") else 0.0)
            weighted_rr += weight * _safe_float(feedback.get("rr_realized"), 0.0)
            weighted_mfe += weight * _safe_float(feedback.get("mfe_rr"), 0.0)
            weighted_mae += weight * _safe_float(feedback.get("mae_rr"), 0.0)
            weighted_capture += weight * _safe_float(feedback.get("target_capture"), 0.0)
            weighted_giveback += weight * _safe_float(feedback.get("giveback_ratio"), 0.0)
            weighted_quality += weight * _safe_float(feedback.get("quality_score"), 50.0)
            weighted_duration += weight * _safe_float(feedback.get("duration_minutes"), 0.0)

            family = str(feedback.get("exit_family") or "other")
            exit_mix[family] = exit_mix.get(family, 0.0) + weight

        summary = {
            "sample_count": samples,
            "win_rate": round(_weighted_mean(weighted_wins, total_weight), 4),
            "target_hit_rate": round(_weighted_mean(weighted_target_hits, total_weight), 4),
            "stop_like_rate": round(_weighted_mean(weighted_stop_like, total_weight), 4),
            "premature_stop_rate": round(_weighted_mean(weighted_premature, total_weight), 4),
            "late_entry_rate": round(_weighted_mean(weighted_late, total_weight), 4),
            "target_miss_rate": round(_weighted_mean(weighted_target_miss, total_weight), 4),
            "avg_rr_realized": round(_weighted_mean(weighted_rr, total_weight), 4),
            "avg_mfe_rr": round(_weighted_mean(weighted_mfe, total_weight), 4),
            "avg_mae_rr": round(_weighted_mean(weighted_mae, total_weight), 4),
            "avg_target_capture": round(_weighted_mean(weighted_capture, total_weight), 4),
            "avg_giveback_ratio": round(_weighted_mean(weighted_giveback, total_weight), 4),
            "avg_quality_score": round(_weighted_mean(weighted_quality, total_weight), 1),
            "avg_duration_minutes": round(_weighted_mean(weighted_duration, total_weight), 1),
            "exit_mix": {
                family: round(weight / total_weight, 4)
                for family, weight in sorted(exit_mix.items())
                if total_weight > 0
            },
        }

        with self._lock:
            self._summary_cache[key] = (now, dict(summary))
        return summary

    def get_exit_adjustment(
        self,
        asset: str,
        category: str,
        context: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        asset_summary = self.summarize_history(asset=asset, category=category, days_back=120, limit=220)
        category_summary = self.summarize_history(asset="", category=category, days_back=120, limit=600)

        asset_samples = _safe_int(asset_summary.get("sample_count"), 0)
        category_samples = _safe_int(category_summary.get("sample_count"), 0)
        asset_weight = min(0.75, asset_samples / 18.0) if asset_samples >= 4 else 0.0
        summary = self._blend_summaries(asset_summary, category_summary, asset_weight)
        sample_count = _safe_int(summary.get("sample_count"), 0)

        target_rr_multiplier = 1.0
        stop_buffer_multiplier = 1.0
        notes: List[str] = []

        if sample_count < 6:
            notes.append("execution_feedback_bootstrap")
        else:
            target_miss_rate = _safe_float(summary.get("target_miss_rate"), 0.0)
            target_capture = _safe_float(summary.get("avg_target_capture"), 0.0)
            premature_stop_rate = _safe_float(summary.get("premature_stop_rate"), 0.0)
            avg_mfe_rr = _safe_float(summary.get("avg_mfe_rr"), 0.0)
            stop_like_rate = _safe_float(summary.get("stop_like_rate"), 0.0)
            avg_mae_rr = _safe_float(summary.get("avg_mae_rr"), 0.0)
            late_entry_rate = _safe_float(summary.get("late_entry_rate"), 0.0)
            target_hit_rate = _safe_float(summary.get("target_hit_rate"), 0.0)
            avg_rr_realized = _safe_float(summary.get("avg_rr_realized"), 0.0)
            avg_giveback_ratio = _safe_float(summary.get("avg_giveback_ratio"), 0.0)
            avg_quality_score = _safe_float(summary.get("avg_quality_score"), 50.0)

            if target_miss_rate > 0.28 and target_capture < 0.58:
                delta = min(
                    0.14,
                    (target_miss_rate - 0.28) * 0.28 + max(0.0, 0.58 - target_capture) * 0.45,
                )
                target_rr_multiplier -= delta
                notes.append("targets_too_ambitious")

            if premature_stop_rate > 0.24 and avg_mfe_rr > 0.80:
                delta = min(
                    0.12,
                    (premature_stop_rate - 0.24) * 0.24 + max(0.0, avg_mfe_rr - 0.80) * 0.05,
                )
                stop_buffer_multiplier += delta
                notes.append("profits_given_back_before_stop")

            if stop_like_rate > 0.60 and avg_mae_rr > 1.05 and avg_mfe_rr < 0.45:
                delta = min(
                    0.08,
                    max(0.0, avg_mae_rr - 1.05) * 0.08 + max(0.0, stop_like_rate - 0.60) * 0.08,
                )
                stop_buffer_multiplier -= delta
                notes.append("losses_extend_without_progress")

            if late_entry_rate > 0.30 and avg_mae_rr > 0.90:
                target_rr_multiplier -= min(0.06, (late_entry_rate - 0.30) * 0.18)
                notes.append("entries_arrive_late")

            if target_hit_rate > 0.52 and avg_rr_realized > 1.10 and avg_giveback_ratio < 0.20:
                delta = min(
                    0.10,
                    (target_hit_rate - 0.52) * 0.16 + max(0.0, avg_rr_realized - 1.10) * 0.04,
                )
                target_rr_multiplier += delta
                notes.append("targets_can_extend")

            if avg_quality_score < 40.0:
                target_rr_multiplier -= 0.03
                notes.append("execution_quality_soft_reduce")

        target_rr_multiplier = round(_clip(target_rr_multiplier, 0.82, 1.18), 4)
        stop_buffer_multiplier = round(_clip(stop_buffer_multiplier, 0.88, 1.15), 4)

        return {
            "version": 1,
            "asset_sample_count": asset_samples,
            "category_sample_count": category_samples,
            "sample_count": sample_count,
            "target_rr_multiplier": target_rr_multiplier,
            "stop_buffer_multiplier": stop_buffer_multiplier,
            "avg_quality_score": round(_safe_float(summary.get("avg_quality_score"), 50.0), 1),
            "source": "asset_category_blend" if asset_weight > 0 else "category_only",
            "notes": notes,
            "summary": summary,
        }

    def _fetch_rows(self, asset: str, category: str, days_back: int, limit: int) -> List[Dict[str, Any]]:
        key = (asset, category, days_back, limit)
        now = time.time()
        with self._lock:
            cached = self._cache.get(key)
            if cached and (now - cached[0]) < self._TTL_SECONDS:
                return list(cached[1])

        try:
            from services.db_pool import get_db

            rows = get_db().get_execution_feedback_trades(
                since=datetime.utcnow() - timedelta(days=days_back),
                asset=asset,
                category=category,
                limit=limit,
            )
        except Exception:
            rows = []

        with self._lock:
            self._cache[key] = (now, list(rows))
        return rows

    @staticmethod
    def _classify_exit(exit_reason: str, is_partial_close: bool = False) -> str:
        reason = str(exit_reason or "").strip().lower()
        if is_partial_close or "partial tp" in reason:
            return "partial_tp"
        if "trailing stop" in reason:
            return "trailing_stop"
        if "stop loss" in reason and "offline" in reason:
            return "stop_loss_offline"
        if "stop loss" in reason:
            return "stop_loss"
        if "take profit" in reason and "offline" in reason:
            return "take_profit_offline"
        if "take profit" in reason:
            return "take_profit"
        if "manual" in reason:
            return "manual"
        return "other"

    @staticmethod
    def _duration_bucket(duration_minutes: int) -> str:
        if duration_minutes <= 15:
            return "scalp"
        if duration_minutes <= 120:
            return "intraday"
        if duration_minutes <= 720:
            return "session"
        if duration_minutes <= 2880:
            return "swing"
        return "extended"

    @staticmethod
    def _late_entry_threshold_minutes(metadata: Dict[str, Any]) -> int:
        timeframe = str(metadata.get("timeframe") or "").lower()
        mapping = {
            "1m": 30,
            "5m": 60,
            "15m": 120,
            "30m": 180,
            "1h": 360,
            "4h": 1440,
            "1d": 4320,
        }
        return mapping.get(timeframe, 180)

    @staticmethod
    def _blend_summaries(
        asset_summary: Dict[str, Any],
        category_summary: Dict[str, Any],
        asset_weight: float,
    ) -> Dict[str, Any]:
        weight = _clip(asset_weight, 0.0, 1.0)
        other_weight = 1.0 - weight
        numeric_keys = [
            "win_rate",
            "target_hit_rate",
            "stop_like_rate",
            "premature_stop_rate",
            "late_entry_rate",
            "target_miss_rate",
            "avg_rr_realized",
            "avg_mfe_rr",
            "avg_mae_rr",
            "avg_target_capture",
            "avg_giveback_ratio",
            "avg_quality_score",
            "avg_duration_minutes",
        ]
        merged = {
            "sample_count": max(
                _safe_int(asset_summary.get("sample_count"), 0),
                _safe_int(category_summary.get("sample_count"), 0),
            ),
        }
        for key in numeric_keys:
            merged[key] = round(
                (_safe_float(asset_summary.get(key), 0.0) * weight)
                + (_safe_float(category_summary.get(key), 0.0) * other_weight),
                4 if key != "avg_quality_score" else 1,
            )
        return merged


_service = ExecutionFeedbackService()


def get_service() -> ExecutionFeedbackService:
    return _service
