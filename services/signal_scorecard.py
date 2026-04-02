from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

from config.config import (
    GOVERNANCE_BOOTSTRAP_MIN_LIVE_SAMPLES,
    GOVERNANCE_VALIDATION_DAYS,
    GOVERNANCE_VALIDATION_HORIZON,
    MAX_SIGNAL_CONFIDENCE,
    SIGNAL_CONFIDENCE_CURVE_POWER,
    SPREAD_THRESHOLDS,
)
from ml.registry import registry


def _clip(value: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, float(value or 0.0)))


def _aligned_score(raw: float, direction: str) -> float:
    sign = 1.0 if str(direction).upper() == "BUY" else -1.0
    return _clip((float(raw or 0.0) * sign + 1.0) / 2.0)


def _maybe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value or default)
    except Exception:
        return default


class SignalScorecard:
    @staticmethod
    def _curve_score(raw_score: float) -> float:
        raw = _clip(raw_score)
        knee = 0.85
        if raw <= knee:
            return raw
        span = 1.0 - knee
        normalized = (raw - knee) / span
        return knee + (normalized ** SIGNAL_CONFIDENCE_CURVE_POWER) * span

    def _live_validation(self, asset: str) -> Tuple[float, Dict[str, Any]]:
        try:
            from prediction_tracker import prediction_tracker as tracker

            stats = tracker.get_accuracy_stats(days_back=GOVERNANCE_VALIDATION_DAYS)
        except Exception:
            return 0.55, {"scope": "unavailable", "samples": 0, "accuracy_pct": 0.0}

        by_asset = (stats.get("by_asset") or {}).get(asset, {})
        asset_stats = by_asset.get(GOVERNANCE_VALIDATION_HORIZON)
        if not asset_stats:
            return 0.55, {"scope": "bootstrap", "samples": 0, "accuracy_pct": 0.0}

        live_total = int(asset_stats.get("total", 0) or 0)
        live_accuracy = _maybe_float(asset_stats.get("accuracy_pct"), 0.0)
        if live_total < GOVERNANCE_BOOTSTRAP_MIN_LIVE_SAMPLES:
            return 0.55, {
                "scope": "bootstrap",
                "samples": live_total,
                "accuracy_pct": round(live_accuracy, 2),
            }

        return _clip(live_accuracy / 100.0, 0.25, 1.0), {
            "scope": "asset",
            "samples": live_total,
            "accuracy_pct": round(live_accuracy, 2),
        }

    @staticmethod
    def _research_quality(signal, context: Dict[str, Any]) -> float:
        governance = signal.metadata.get("governance_validation") or {}
        research = governance.get("model_research") or {}
        if governance.get("approved") and research.get("research_approved"):
            return 1.0
        if research.get("research_approved"):
            return 0.95
        if float(research.get("walk_forward_accuracy", 0.0) or 0.0) >= 0.55:
            return 0.80
        if float(research.get("holdout_accuracy", 0.0) or 0.0) >= 0.52:
            return 0.68

        model_key = (
            signal.metadata.get("policy_model")
            or signal.metadata.get("seed_model")
            or f"{signal.category}_classifier"
        )
        model_meta = registry.get_metadata(model_key) if model_key else {}
        if bool(model_meta.get("research_approved")):
            return 1.0
        if float(model_meta.get("walk_forward_accuracy", 0.0) or 0.0) >= 0.55:
            return 0.80
        if float(model_meta.get("holdout_accuracy", 0.0) or 0.0) >= 0.52:
            return 0.68
        return 0.55

    @staticmethod
    def _structure_quality(signal, context: Dict[str, Any]) -> float:
        structure = signal.metadata.get("market_structure") or context.get("market_structure") or {}
        if not isinstance(structure, dict):
            return 0.5

        alignment_score = _clip(_maybe_float(structure.get("alignment_score"), signal.metadata.get("alignment_score", 0.0)))
        setup_quality = _clip(_maybe_float(structure.get("setup_quality"), signal.metadata.get("setup_quality", 0.0)))
        pullback_score = _maybe_float(structure.get("pullback_score"), signal.metadata.get("pullback_score", 0.0))
        breakout_score = _maybe_float(structure.get("breakout_score"), signal.metadata.get("breakout_score", 0.0))
        structure_bias = str(structure.get("structure_bias", signal.metadata.get("structure_bias", "neutral"))).lower()
        dominant_setup = breakout_score if abs(breakout_score) >= abs(pullback_score) else pullback_score
        sign = 1.0 if signal.direction == "BUY" else -1.0

        bias_score = 0.60
        if structure_bias in {"buy", "sell"}:
            if (structure_bias == "buy" and signal.direction == "BUY") or (
                structure_bias == "sell" and signal.direction == "SELL"
            ):
                bias_score = 0.90
            else:
                bias_score = 0.15

        setup_score = _clip((dominant_setup * sign + 1.0) / 2.0)
        return _clip(alignment_score * 0.35 + setup_quality * 0.35 + setup_score * 0.15 + bias_score * 0.15)

    @staticmethod
    def _regime_quality(signal, context: Dict[str, Any]) -> float:
        regime = str(signal.metadata.get("regime") or context.get("regime") or "unknown")
        if regime == "volatile":
            return 0.20
        if regime == "trending_up":
            return 0.90 if signal.direction == "BUY" else 0.10
        if regime == "trending_down":
            return 0.90 if signal.direction == "SELL" else 0.10
        if regime == "ranging":
            return 0.60
        return 0.50

    @staticmethod
    def _risk_reward_quality(signal) -> float:
        rr = _maybe_float(getattr(signal, "risk_reward", 0.0), 0.0)
        return _clip((rr - 1.0) / 2.0)

    @staticmethod
    def _liquidity_quality(signal, context: Dict[str, Any]) -> float:
        entry = _maybe_float(getattr(signal, "entry_price", 0.0), 0.0)
        spread = _maybe_float(context.get("spread"), 0.0)
        if spread <= 0.0 and entry > 0.0:
            spread = _maybe_float(signal.metadata.get("observed_spread_pct"), 0.0) * entry
        adaptive_policy = signal.metadata.get("adaptive_policy") or {}
        threshold = _maybe_float(
            adaptive_policy.get("max_spread", SPREAD_THRESHOLDS.get(signal.category, 0.01)),
            0.01,
        )
        if entry <= 0.0 or spread <= 0.0 or threshold <= 0.0:
            return 0.70
        spread_pct = spread / entry
        return _clip(1.0 - (spread_pct / threshold))

    @staticmethod
    def _ml_alignment_quality(signal, context: Dict[str, Any]) -> Optional[float]:
        if signal.metadata.get("ml_prediction_real") is not True:
            return None
        ml_direction = str(signal.metadata.get("ml_direction", "") or "").upper()
        if not ml_direction:
            ml_pred = context.get("ml_prediction")
            if ml_pred is None:
                return None
            try:
                ml_direction = "BUY" if float(ml_pred) > 0.5 else "SELL"
            except Exception:
                return None
        ml_conf = _clip(_maybe_float(signal.metadata.get("ml_confidence", context.get("ml_confidence")), 0.0))
        if ml_direction == signal.direction:
            return _clip(0.65 + ml_conf * 0.30)
        return _clip(0.35 - ml_conf * 0.20, 0.05, 0.45)

    @staticmethod
    def _microstructure_quality(signal) -> Optional[float]:
        micro_score = signal.metadata.get("microstructure_score")
        stop_hunt_risk = _clip(_maybe_float(signal.metadata.get("stop_hunt_risk"), 0.0))
        if micro_score is None and stop_hunt_risk <= 0.0:
            return None
        base = (
            _aligned_score(_maybe_float(micro_score, 0.0), signal.direction)
            if micro_score is not None
            else 0.55
        )
        return _clip(base - stop_hunt_risk * 0.35)

    @staticmethod
    def _news_quality(signal) -> Optional[float]:
        state = str(signal.metadata.get("news_state", "") or "").lower()
        if not state:
            return None
        impact = str(signal.metadata.get("news_impact", "") or "").upper()
        direction = str(signal.metadata.get("news_direction", "") or "").upper()
        if state == "clear":
            return 0.65
        if state == "pre":
            if impact == "HIGH":
                return 0.10
            if impact == "MEDIUM":
                return 0.42
            return 0.55
        if state == "active":
            if impact == "HIGH":
                return 0.05
            if impact == "MEDIUM":
                return 0.35
            return 0.45
        if state == "post":
            if direction == signal.direction:
                return 0.82 if impact == "HIGH" else 0.72
            if direction:
                return 0.25 if impact == "HIGH" else 0.35
            return 0.58
        return 0.55

    @staticmethod
    def _session_quality(signal) -> Optional[float]:
        session = str(signal.metadata.get("session", "") or "").lower()
        if not session:
            return None
        mapping = {
            "us": 0.74,
            "europe": 0.72,
            "asia": 0.64,
            "off": 0.50,
        }
        return mapping.get(session, 0.58)

    @staticmethod
    def _entry_quality(signal) -> Optional[float]:
        volatility_ratio = _maybe_float(signal.metadata.get("volatility_ratio"), 0.0)
        support_proximity = signal.metadata.get("support_proximity")
        resistance_proximity = signal.metadata.get("resistance_proximity")
        rr_gap = max(0.0, _maybe_float(signal.metadata.get("adaptive_rr_gap"), 0.0))
        distance_to_resistance = _maybe_float(signal.metadata.get("market_structure", {}).get("distance_to_resistance"), 1.0)
        distance_to_support = _maybe_float(signal.metadata.get("market_structure", {}).get("distance_to_support"), 1.0)

        score = 0.60
        has_signal = False
        if volatility_ratio > 0.0:
            has_signal = True
            if volatility_ratio < 0.60:
                score += 0.08
            elif volatility_ratio > 1.80:
                score -= 0.10
            elif volatility_ratio > 1.40:
                score -= 0.05
        try:
            if signal.direction == "BUY" and support_proximity is not None:
                has_signal = True
                proximity = float(support_proximity)
                if proximity < 0.15:
                    score += 0.12
                elif proximity > 0.85:
                    score -= 0.08
                if distance_to_resistance <= 0.0025:
                    score -= 0.12
            if signal.direction == "SELL" and resistance_proximity is not None:
                has_signal = True
                proximity = float(resistance_proximity)
                if proximity < 0.15:
                    score += 0.12
                elif proximity > 0.85:
                    score -= 0.08
                if distance_to_support <= 0.0025:
                    score -= 0.12
        except Exception:
            return _clip(score) if has_signal else None
        if rr_gap > 0.0:
            has_signal = True
            score -= min(0.20, rr_gap * 0.20)
        return _clip(score) if has_signal else None

    @staticmethod
    def _sentiment_quality(signal) -> Optional[float]:
        score = signal.metadata.get("sentiment_score")
        if score is None:
            return None
        return _aligned_score(_maybe_float(score, 0.0), signal.direction)

    @staticmethod
    def _whale_quality(signal) -> Optional[float]:
        dominant = str(signal.metadata.get("whale_dominant", "") or "")
        if not dominant:
            return None
        ratio = _clip(_maybe_float(signal.metadata.get("ratio", signal.metadata.get("whale_ratio", 0.5)), 0.5))
        if dominant == signal.direction:
            return _clip(0.5 + (ratio - 0.5) * 1.6)
        return _clip(0.5 - (ratio - 0.5) * 1.8)

    @staticmethod
    def _orderflow_quality(signal) -> Optional[float]:
        if signal.metadata.get("orderflow_applicable") is not True:
            return None
        imbalance = _maybe_float(signal.metadata.get("orderflow_imbalance"), 0.0)
        return _aligned_score(imbalance, signal.direction)

    @staticmethod
    def _memory_quality(signal) -> Optional[float]:
        memory_edge = signal.metadata.get("memory_edge")
        if memory_edge is None:
            return None
        sample_count = int(signal.metadata.get("memory_sample_count", 0) or 0)
        base = _clip((_maybe_float(memory_edge, 0.0) + 1.0) / 2.0)
        strength = _clip(sample_count / 25.0, 0.35, 1.0)
        return _clip(0.5 + (base - 0.5) * strength)

    @staticmethod
    def _policy_quality(signal) -> Optional[float]:
        edge = signal.metadata.get("agent_directional_edge")
        if edge is None:
            return None
        ensemble = signal.metadata.get("meta_ai_ensemble")
        policy = _clip(_maybe_float(edge, 0.5), 0.0, 1.0)
        if ensemble is None:
            return policy
        return _clip(policy * 0.70 + _clip(_maybe_float(ensemble, 0.5)) * 0.30)

    def score(self, signal, context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        context = context or {}

        seed_score = _clip(
            _maybe_float(
                signal.metadata.get(
                    "ml_confidence",
                    context.get(
                        "ml_confidence",
                        signal.metadata.get("seed_candidate_score", getattr(signal, "confidence", 0.0)),
                    ),
                ),
                getattr(signal, "confidence", 0.0),
            )
        )
        structure_score = self._structure_quality(signal, context)
        regime_score = self._regime_quality(signal, context)
        rr_score = self._risk_reward_quality(signal)
        liquidity_score = self._liquidity_quality(signal, context)
        live_score, live_payload = self._live_validation(signal.asset)
        research_score = self._research_quality(signal, context)

        components: Dict[str, Tuple[float, float]] = {
            "seed": (seed_score, 0.22),
            "structure": (structure_score, 0.18),
            "regime": (regime_score, 0.10),
            "risk_reward": (rr_score, 0.12),
            "liquidity": (liquidity_score, 0.08),
            "live_validation": (live_score, 0.09),
            "research": (research_score, 0.08),
        }

        optional_components = {
            "ml_alignment": (self._ml_alignment_quality(signal, context), 0.07),
            "microstructure": (self._microstructure_quality(signal), 0.05),
            "news": (self._news_quality(signal), 0.04),
            "session": (self._session_quality(signal), 0.03),
            "entry": (self._entry_quality(signal), 0.05),
            "sentiment": (self._sentiment_quality(signal), 0.05),
            "whales": (self._whale_quality(signal), 0.04),
            "order_flow": (self._orderflow_quality(signal), 0.04),
            "memory": (self._memory_quality(signal), 0.06),
            "policy": (self._policy_quality(signal), 0.06),
        }
        for name, (value, weight) in optional_components.items():
            if value is not None:
                components[name] = (value, weight)

        weighted_sum = 0.0
        weight_total = 0.0
        breakdown: Dict[str, float] = {}
        for name, (value, weight) in components.items():
            clipped = _clip(value)
            weighted_sum += clipped * weight
            weight_total += weight
            breakdown[name] = round(clipped, 4)

        raw_score = weighted_sum / max(weight_total, 1e-9)
        reliability = _clip(
            0.55
            + research_score * 0.20
            + live_score * 0.15
            + liquidity_score * 0.05
            + rr_score * 0.05,
            0.45,
            1.0,
        )
        curved_score = self._curve_score(raw_score)
        final_score = MAX_SIGNAL_CONFIDENCE * curved_score * (0.85 + reliability * 0.15)
        if live_payload.get("scope") == "asset":
            live_cap = 0.10 + (MAX_SIGNAL_CONFIDENCE - 0.10) * live_score
            final_score = min(final_score, live_cap)
        final_score = _clip(final_score, 0.0, MAX_SIGNAL_CONFIDENCE)

        notes = []
        if live_payload.get("scope") == "asset":
            notes.append(
                f"live {live_payload.get('accuracy_pct', 0.0):.1f}% over {int(live_payload.get('samples', 0) or 0)} samples"
            )
        elif live_payload.get("scope") == "bootstrap":
            notes.append("live validation still bootstrapping")
        if signal.metadata.get("ml_prediction_real") is True and signal.metadata.get("ml_direction_agrees") is False:
            notes.append("ml direction conflicts with trade direction")
        if signal.metadata.get("seed_below_floor"):
            notes.append("seed score started below minimum floor")
        rr_gap = max(0.0, _maybe_float(signal.metadata.get("adaptive_rr_gap"), 0.0))
        if rr_gap > 0.0:
            notes.append(f"risk/reward is {rr_gap:.2f} below adaptive minimum")

        return {
            "raw_score": round(raw_score, 4),
            "curved_score": round(curved_score, 4),
            "reliability": round(reliability, 4),
            "final_score": round(final_score, 4),
            "breakdown": breakdown,
            "live_validation": live_payload,
            "notes": notes,
        }


_service = SignalScorecard()


def get_service() -> SignalScorecard:
    return _service
