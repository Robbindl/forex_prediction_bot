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


def _safe_bool(value: Any) -> bool:
    if isinstance(value, str):
        return value.lower() in {"true", "1", "yes", "y"}
    return bool(value)


def _clip(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, float(value)))


def _bucket(value: float, scale: float = 0.2) -> int:
    normalized = _clip(value, -1.0, 1.0)
    return int(round((normalized + 1.0) / scale))


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


class SetupMemoryService:
    _TTL_SECONDS = 300

    def __init__(self) -> None:
        self._cache: Dict[Tuple[str, str, int, int], Tuple[float, List[Dict[str, Any]]]] = {}
        self._lock = threading.RLock()

    def build_fingerprint(self, signal, context: Dict[str, Any] | None = None) -> Dict[str, Any]:
        context = context or {}
        metadata = dict(getattr(signal, "metadata", {}) or {})
        structure = context.get("market_structure") or metadata.get("market_structure") or {}
        if not isinstance(structure, dict):
            structure = {}

        adaptive_policy = metadata.get("adaptive_policy") or {}
        if not isinstance(adaptive_policy, dict):
            adaptive_policy = {}

        sentiment_score = _safe_float(metadata.get("sentiment_score"), 0.0)
        whale_dominant = str(metadata.get("whale_dominant") or "").upper()
        whale_ratio = max(
            _safe_float(metadata.get("whale_ratio"), 0.0),
            _safe_float(metadata.get("whale_bull_weight"), 0.0),
            _safe_float(metadata.get("whale_bear_weight"), 0.0),
        )
        orderflow_imbalance = _safe_float(metadata.get("orderflow_imbalance"), 0.0)
        opportunity_score = _safe_float(metadata.get("opportunity_score"), 0.0)
        setup_quality = _safe_float(metadata.get("setup_quality", structure.get("setup_quality")), 0.0)
        alignment_score = _safe_float(metadata.get("alignment_score", structure.get("alignment_score")), 0.0)
        pullback_score = _safe_float(metadata.get("pullback_score", structure.get("pullback_score")), 0.0)
        breakout_score = _safe_float(metadata.get("breakout_score", structure.get("breakout_score")), 0.0)
        setup_signal = breakout_score if abs(breakout_score) >= abs(pullback_score) else pullback_score
        setup_style = "breakout" if abs(breakout_score) >= abs(pullback_score) and abs(breakout_score) >= 0.2 else (
            "pullback" if abs(pullback_score) >= 0.2 else "mixed"
        )

        if sentiment_score >= 0.2:
            sentiment_bucket = "bullish"
        elif sentiment_score <= -0.2:
            sentiment_bucket = "bearish"
        else:
            sentiment_bucket = "neutral"

        if whale_dominant in {"BUY", "SELL"} and whale_ratio >= 0.55:
            whale_bucket = whale_dominant.lower()
        else:
            whale_bucket = "neutral"

        if orderflow_imbalance >= 0.2:
            orderflow_bucket = "buy_pressure"
        elif orderflow_imbalance <= -0.2:
            orderflow_bucket = "sell_pressure"
        else:
            orderflow_bucket = "balanced"

        risk_multiplier = _safe_float(adaptive_policy.get("risk_multiplier"), 1.0)
        if risk_multiplier >= 1.12:
            adaptive_posture = "aggressive"
        elif risk_multiplier <= 0.88:
            adaptive_posture = "defensive"
        else:
            adaptive_posture = "neutral"

        return {
            "version": 1,
            "asset": str(getattr(signal, "canonical_asset", "") or getattr(signal, "asset", "") or ""),
            "category": str(getattr(signal, "category", "") or context.get("category") or ""),
            "direction": str(getattr(signal, "direction", "BUY") or "BUY").upper(),
            "timeframe": str(context.get("timeframe") or metadata.get("timeframe") or ""),
            "regime": str(structure.get("regime") or metadata.get("regime") or context.get("regime") or "unknown").lower(),
            "session": str(metadata.get("session") or ""),
            "structure_bias": str(metadata.get("structure_bias") or structure.get("structure_bias") or "neutral").lower(),
            "alignment_bucket": _bucket((alignment_score * 2.0) - 1.0, scale=0.25),
            "setup_quality_bucket": _bucket((setup_quality * 2.0) - 1.0, scale=0.25),
            "setup_style": setup_style,
            "volatility_state": str(metadata.get("volatility_state") or structure.get("volatility_state") or "unknown").lower(),
            "sentiment_bucket": sentiment_bucket,
            "sentiment_band": _bucket(sentiment_score, scale=0.25),
            "whale_bucket": whale_bucket,
            "whale_ratio_bucket": int(round(_clip(whale_ratio, 0.0, 1.0) * 4.0)),
            "orderflow_bucket": orderflow_bucket,
            "orderflow_band": _bucket(orderflow_imbalance, scale=0.25),
            "opportunity_bucket": int(round(_clip(opportunity_score, 0.0, 1.0) * 4.0)),
            "adaptive_posture": adaptive_posture,
            "risk_reward_band": int(round(_clip(_safe_float(getattr(signal, "risk_reward", 0.0), 0.0), 0.0, 4.0))),
            "confidence_band": int(round(_clip(_safe_float(getattr(signal, "confidence", 0.0), 0.0), 0.0, 1.0) * 5.0)),
            "setup_signal_band": _bucket(setup_signal, scale=0.25),
        }

    def score_setup(self, signal, context: Dict[str, Any] | None = None) -> Dict[str, Any]:
        context = context or {}
        fingerprint = self.build_fingerprint(signal, context)
        rows = self._fetch_rows(
            asset=fingerprint["asset"],
            category=fingerprint["category"],
            days_back=90,
            limit=3500,
        )

        if not rows:
            return {
                "fingerprint": fingerprint,
                "sample_count": 0,
                "similar_matches": 0,
                "same_asset_matches": 0,
                "avg_similarity": 0.0,
                "win_rate": 0.0,
                "target_hit_rate": 0.0,
                "avg_move_pct": 0.0,
                "memory_edge": 0.0,
                "memory_score": 50.0,
                "adjustment": 0.0,
                "notes": ["bootstrap_memory"],
            }

        weighted_correct = 0.0
        weighted_target_hit = 0.0
        weighted_move = 0.0
        weighted_similarity = 0.0
        weighted_total = 0.0
        same_asset_matches = 0
        similar_matches = 0
        top_examples: List[Dict[str, Any]] = []

        for row in rows:
            historical_meta = _parse_metadata(row.get("signal_metadata"))
            historical_fp = historical_meta.get("setup_memory_fingerprint")
            if not isinstance(historical_fp, dict):
                historical_fp = self._fingerprint_from_metadata(row, historical_meta)
            similarity = self._similarity(fingerprint, historical_fp)
            if similarity < 0.46:
                continue

            similar_matches += 1
            if row.get("asset") == fingerprint["asset"]:
                same_asset_matches += 1

            signal_time = _coerce_datetime(row.get("signal_time"))
            age_days = 90.0
            if signal_time is not None:
                age_days = max(0.0, (datetime.now(timezone.utc) - signal_time).total_seconds() / 86400.0)
            recency_weight = max(0.35, 1.0 - min(age_days, 90.0) / 120.0)
            asset_weight = 1.18 if row.get("asset") == fingerprint["asset"] else 1.0
            horizon_weight = 1.05 if int(row.get("horizon_minutes") or 0) in {60, 240} else 0.95
            weight = similarity * recency_weight * asset_weight * horizon_weight

            correct = 1.0 if _safe_bool(row.get("direction_correct")) else 0.0
            target_hit = 1.0 if _safe_bool(row.get("target_hit")) else 0.0
            move = _safe_float(row.get("pct_move"), 0.0)
            row_direction = str(row.get("direction") or "BUY").upper()
            directional_move = move if row_direction == "BUY" else -move

            weighted_correct += weight * correct
            weighted_target_hit += weight * target_hit
            weighted_move += weight * directional_move
            weighted_similarity += weight * similarity
            weighted_total += weight

            if len(top_examples) < 5:
                top_examples.append({
                    "asset": row.get("asset"),
                    "similarity": round(similarity, 4),
                    "move_pct": round(move, 3),
                    "correct": bool(correct),
                })

        if weighted_total <= 0:
            return {
                "fingerprint": fingerprint,
                "sample_count": 0,
                "similar_matches": 0,
                "same_asset_matches": 0,
                "avg_similarity": 0.0,
                "win_rate": 0.0,
                "target_hit_rate": 0.0,
                "avg_move_pct": 0.0,
                "memory_edge": 0.0,
                "memory_score": 50.0,
                "adjustment": 0.0,
                "notes": ["no_weighted_matches"],
            }

        sample_strength = min(1.0, similar_matches / 24.0)
        avg_similarity = weighted_similarity / weighted_total
        win_rate = weighted_correct / weighted_total
        target_hit_rate = weighted_target_hit / weighted_total
        avg_move_pct = weighted_move / weighted_total

        directional_edge = (win_rate - 0.5) * 2.0
        target_edge = (target_hit_rate - 0.45) * 1.4
        similarity_edge = max(0.0, avg_similarity - 0.5) * 0.6
        asset_bonus = min(0.12, same_asset_matches / 20.0)
        move_bias = _clip(avg_move_pct / 4.0, -0.35, 0.35)

        memory_edge = _clip(
            (directional_edge * 0.55 + target_edge * 0.20 + similarity_edge + asset_bonus + move_bias) * sample_strength,
            -1.0,
            1.0,
        )
        memory_score = round(_clip(50.0 + memory_edge * 42.0, 0.0, 100.0), 1)

        adjustment = 0.0
        notes: List[str] = []
        if similar_matches >= 6:
            if memory_edge >= 0.18:
                adjustment = min(0.07, 0.01 + memory_edge * 0.08)
                notes.append("memory_positive_edge")
            elif memory_edge <= -0.12:
                adjustment = -min(0.09, 0.015 + abs(memory_edge) * 0.09)
                notes.append("memory_negative_edge")
        else:
            notes.append("memory_low_sample")

        return {
            "fingerprint": fingerprint,
            "sample_count": int(similar_matches),
            "similar_matches": int(similar_matches),
            "same_asset_matches": int(same_asset_matches),
            "avg_similarity": round(avg_similarity, 4),
            "win_rate": round(win_rate, 4),
            "target_hit_rate": round(target_hit_rate, 4),
            "avg_move_pct": round(avg_move_pct, 4),
            "memory_edge": round(memory_edge, 4),
            "memory_score": memory_score,
            "adjustment": round(adjustment, 4),
            "top_examples": top_examples,
            "notes": notes,
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

            rows = get_db().get_setup_memory_records(
                category=category,
                asset=asset,
                since=datetime.utcnow() - timedelta(days=days_back),
                limit=limit,
            )
        except Exception:
            rows = []

        with self._lock:
            self._cache[key] = (now, list(rows))
        return rows

    def _fingerprint_from_metadata(self, row: Dict[str, Any], metadata: Dict[str, Any]) -> Dict[str, Any]:
        pseudo_signal = type("MemorySignal", (), {
            "asset": row.get("asset", ""),
            "canonical_asset": row.get("asset", ""),
            "category": row.get("category", ""),
            "direction": row.get("direction", "BUY"),
            "confidence": _safe_float(row.get("confidence"), 0.0),
            "risk_reward": _safe_float(metadata.get("risk_reward"), 0.0),
            "metadata": metadata,
        })()
        return self.build_fingerprint(pseudo_signal, {"category": row.get("category", ""), "timeframe": metadata.get("timeframe", "")})

    @staticmethod
    def _similarity(current: Dict[str, Any], historical: Dict[str, Any]) -> float:
        score = 0.0
        weight_total = 0.0

        def exact(key: str, weight: float) -> None:
            nonlocal score, weight_total
            weight_total += weight
            if current.get(key) == historical.get(key):
                score += weight

        def bucket_distance(key: str, weight: float, max_distance: float = 4.0) -> None:
            nonlocal score, weight_total
            weight_total += weight
            a = _safe_float(current.get(key), 0.0)
            b = _safe_float(historical.get(key), 0.0)
            score += weight * max(0.0, 1.0 - min(abs(a - b), max_distance) / max_distance)

        exact("asset", 0.18)
        exact("category", 0.05)
        exact("direction", 0.08)
        exact("timeframe", 0.03)
        exact("regime", 0.10)
        exact("session", 0.04)
        exact("structure_bias", 0.12)
        exact("setup_style", 0.08)
        exact("volatility_state", 0.06)
        exact("sentiment_bucket", 0.06)
        exact("whale_bucket", 0.06)
        exact("orderflow_bucket", 0.06)
        exact("adaptive_posture", 0.03)
        bucket_distance("alignment_bucket", 0.07)
        bucket_distance("setup_quality_bucket", 0.07)
        bucket_distance("sentiment_band", 0.05)
        bucket_distance("whale_ratio_bucket", 0.03)
        bucket_distance("orderflow_band", 0.05)
        bucket_distance("opportunity_bucket", 0.05)
        bucket_distance("risk_reward_band", 0.03)
        bucket_distance("confidence_band", 0.03)
        bucket_distance("setup_signal_band", 0.05)

        if weight_total <= 0:
            return 0.0
        return round(score / weight_total, 4)


_service = SetupMemoryService()


def get_service() -> SetupMemoryService:
    return _service
