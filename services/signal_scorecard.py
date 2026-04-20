from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

from config.config import (
    GOVERNANCE_EXPECTANCY_MIN_SAMPLES,
    GOVERNANCE_BOOTSTRAP_MIN_LIVE_SAMPLES,
    GOVERNANCE_VALIDATION_DAYS,
    GOVERNANCE_VALIDATION_HORIZON,
    MAX_SIGNAL_CONFIDENCE,
    SIGNAL_CONFIDENCE_CURVE_POWER,
    SPREAD_THRESHOLDS,
)


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
    def _playbook_quality(signal) -> Optional[float]:
        action = str(signal.metadata.get("playbook_action", "") or "").strip().lower()
        if action not in {"seed", "override", "support"}:
            return None
        playbook_score = _clip(_maybe_float(signal.metadata.get("playbook_score"), 0.0))
        playbook_confidence = _clip(_maybe_float(signal.metadata.get("playbook_confidence"), 0.0))
        if playbook_score <= 0.0 and playbook_confidence <= 0.0:
            return None
        action_bias = 0.08 if action in {"seed", "override"} else 0.03
        return _clip(playbook_score * 0.45 + playbook_confidence * 0.47 + action_bias)

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
            return 0.50, {"scope": "unavailable", "samples": 0, "accuracy_pct": 0.0}

        by_asset = (stats.get("by_asset") or {}).get(asset, {})
        asset_stats = by_asset.get(GOVERNANCE_VALIDATION_HORIZON)
        if not asset_stats:
            return 0.50, {"scope": "bootstrap", "samples": 0, "accuracy_pct": 0.0}

        live_total = int(asset_stats.get("total", 0) or 0)
        live_accuracy = _maybe_float(asset_stats.get("accuracy_pct"), 0.0)
        if live_total < GOVERNANCE_BOOTSTRAP_MIN_LIVE_SAMPLES:
            return 0.50, {
                "scope": "bootstrap",
                "samples": live_total,
                "accuracy_pct": round(live_accuracy, 2),
            }

        return _clip(live_accuracy / 100.0, 0.25, 1.0), {
            "scope": "asset",
            "samples": live_total,
            "accuracy_pct": round(live_accuracy, 2),
        }

    def _execution_expectancy(self, signal) -> Tuple[Optional[float], Dict[str, Any]]:
        try:
            from services.execution_feedback_service import get_service as get_execution_feedback_service

            service = get_execution_feedback_service()
            lookback_days = max(90, GOVERNANCE_VALIDATION_DAYS * 4)
            asset_summary = service.summarize_history(
                asset=signal.asset,
                category=signal.category,
                days_back=lookback_days,
                limit=220,
            )
            category_summary = service.summarize_history(
                asset="",
                category=signal.category,
                days_back=lookback_days,
                limit=600,
            )
        except Exception:
            return None, {"scope": "unavailable", "sample_count": 0}

        asset_samples = int(asset_summary.get("sample_count", 0) or 0)
        category_samples = int(category_summary.get("sample_count", 0) or 0)
        if asset_samples >= max(4, GOVERNANCE_EXPECTANCY_MIN_SAMPLES // 2):
            scope = "asset"
            summary = asset_summary
        elif category_samples >= GOVERNANCE_EXPECTANCY_MIN_SAMPLES:
            scope = "category_context"
            summary = category_summary
        elif category_samples > 0:
            return None, {"scope": "bootstrap", "sample_count": category_samples}
        else:
            return None, {"scope": "bootstrap", "sample_count": 0}

        avg_rr_realized = _maybe_float(summary.get("avg_rr_realized"), 0.0)
        target_hit_rate = _clip(_maybe_float(summary.get("target_hit_rate"), 0.0))
        late_entry_rate = _clip(_maybe_float(summary.get("late_entry_rate"), 0.0))
        premature_stop_rate = _clip(_maybe_float(summary.get("premature_stop_rate"), 0.0))
        target_miss_rate = _clip(_maybe_float(summary.get("target_miss_rate"), 0.0))
        avg_quality_score = _clip(_maybe_float(summary.get("avg_quality_score"), 50.0) / 100.0)

        rr_score = _clip((avg_rr_realized + 0.30) / 1.80)
        timing_score = _clip(
            1.0 - (late_entry_rate * 0.55 + premature_stop_rate * 0.45 + target_miss_rate * 0.35)
        )
        expectancy_score = _clip(
            rr_score * 0.38
            + target_hit_rate * 0.18
            + avg_quality_score * 0.24
            + timing_score * 0.20
        )
        return expectancy_score, {
            "scope": scope,
            "sample_count": int(summary.get("sample_count", 0) or 0),
            "avg_rr_realized": round(avg_rr_realized, 4),
            "target_hit_rate": round(target_hit_rate, 4),
            "late_entry_rate": round(late_entry_rate, 4),
            "premature_stop_rate": round(premature_stop_rate, 4),
            "target_miss_rate": round(target_miss_rate, 4),
            "avg_quality_score": round(_maybe_float(summary.get("avg_quality_score"), 50.0), 1),
        }

    @staticmethod
    def _expectancy_confidence_cap(
        expectancy_score: Optional[float],
        expectancy_payload: Dict[str, Any],
    ) -> Optional[float]:
        if expectancy_score is None:
            return None
        scope = str(expectancy_payload.get("scope") or "bootstrap")
        cap = 0.18 + _clip(expectancy_score) * (MAX_SIGNAL_CONFIDENCE - 0.18)
        if scope == "category_context":
            cap = min(cap, 0.82)
        if _maybe_float(expectancy_payload.get("avg_rr_realized"), 0.0) < 0.0:
            cap = min(cap, 0.68)
        if (
            _maybe_float(expectancy_payload.get("target_hit_rate"), 0.0) < 0.32
            and _maybe_float(expectancy_payload.get("late_entry_rate"), 0.0) > 0.32
        ):
            cap = min(cap, 0.62)
        if _maybe_float(expectancy_payload.get("avg_quality_score"), 50.0) < 45.0:
            cap = min(cap, 0.66)
        return _clip(cap, 0.0, MAX_SIGNAL_CONFIDENCE)

    @staticmethod
    def _research_quality(signal, context: Dict[str, Any]) -> float:
        governance = signal.metadata.get("governance_validation") or {}
        research = governance.get("model_research") or {}
        research_state = str(research.get("research_status") or research.get("research_grade") or "").strip().lower()
        if governance.get("approved") and (
            research.get("research_approved") or research_state in {"playbook_runtime", "runtime"}
        ):
            return 1.0
        if research.get("research_approved") or research_state in {"playbook_runtime", "runtime"}:
            return 0.95
        if float(research.get("walk_forward_accuracy", 0.0) or 0.0) >= 0.55:
            return 0.80
        if float(research.get("holdout_accuracy", 0.0) or 0.0) >= 0.52:
            return 0.68
        if str(signal.metadata.get("seed_source") or "").strip().lower() == "playbook":
            return 0.92
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
        exhaustion_risk = _clip(_maybe_float(signal.metadata.get("exhaustion_risk"), 0.0))
        tick_imbalance = signal.metadata.get("tick_imbalance")
        book_imbalance = signal.metadata.get("book_imbalance")
        if micro_score is None and stop_hunt_risk <= 0.0 and exhaustion_risk <= 0.0 and tick_imbalance is None and book_imbalance is None:
            return None
        components = []
        if micro_score is not None:
            components.append(_aligned_score(_maybe_float(micro_score, 0.0), signal.direction))
        if tick_imbalance is not None:
            components.append(_aligned_score(_maybe_float(tick_imbalance, 0.0), signal.direction))
        if book_imbalance is not None:
            components.append(_aligned_score(_maybe_float(book_imbalance, 0.0), signal.direction))
        base = sum(components) / len(components) if components else 0.55
        penalty = stop_hunt_risk * 0.28 + exhaustion_risk * 0.22
        return _clip(base - penalty)

    @staticmethod
    def _cross_asset_quality(signal) -> Optional[float]:
        alignment = signal.metadata.get("cross_asset_alignment")
        if alignment is None:
            return None
        confidence = _clip(_maybe_float(signal.metadata.get("cross_asset_confidence"), 0.0))
        base = _clip((_maybe_float(alignment, 0.0) + 1.0) / 2.0)
        strength = _clip(0.45 + confidence * 0.55, 0.45, 1.0)
        return _clip(0.5 + (base - 0.5) * strength)

    @staticmethod
    def _broker_quality(signal) -> Optional[float]:
        broker = signal.metadata.get("broker_quality") or {}
        if not isinstance(broker, dict) or not broker:
            return None
        score = broker.get("score")
        if score is None:
            return None
        return _clip(_maybe_float(score, 0.0))

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
        score, has_signal = SignalScorecard._entry_quality_directional_adjustment(
            signal,
            score=score,
            has_signal=has_signal,
            support_proximity=support_proximity,
            resistance_proximity=resistance_proximity,
            distance_to_resistance=distance_to_resistance,
            distance_to_support=distance_to_support,
        )
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

    @staticmethod
    def _entry_quality_directional_adjustment(
        signal,
        *,
        score: float,
        has_signal: bool,
        support_proximity,
        resistance_proximity,
        distance_to_resistance: float,
        distance_to_support: float,
    ) -> tuple[float, bool]:
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
            return score, has_signal
        return score, has_signal

    @staticmethod
    def _scorecard_notes(
        signal,
        live_payload: Dict[str, Any],
        expectancy_payload: Dict[str, Any],
        seed_source: str,
        rr_gap: float,
    ) -> list[str]:
        notes: list[str] = []
        if live_payload.get("scope") == "asset":
            notes.append(
                f"live {live_payload.get('accuracy_pct', 0.0):.1f}% over {int(live_payload.get('samples', 0) or 0)} samples"
            )
        elif live_payload.get("scope") == "bootstrap":
            notes.append("live validation still bootstrapping")
        if expectancy_payload.get("scope") == "asset":
            notes.append(
                f"execution expectancy {float(expectancy_payload.get('avg_rr_realized', 0.0)):+.2f}R"
                f" over {int(expectancy_payload.get('sample_count', 0) or 0)} trades"
            )
        elif expectancy_payload.get("scope") == "category_context":
            notes.append(
                f"execution expectancy using {signal.category} context"
                f" ({int(expectancy_payload.get('sample_count', 0) or 0)} trades)"
            )
        elif expectancy_payload.get("scope") == "bootstrap":
            notes.append("execution expectancy still bootstrapping")
        if signal.metadata.get("ml_prediction_real") is True and signal.metadata.get("ml_direction_agrees") is False:
            notes.append("ml direction conflicts with trade direction")
        if seed_source == "playbook":
            notes.append(
                f"playbook {signal.metadata.get('playbook_name', 'setup')} seeded the trade"
            )
        if signal.metadata.get("seed_below_floor"):
            notes.append("seed score started below minimum floor")
        if rr_gap > 0.0:
            notes.append(f"risk/reward is {rr_gap:.2f} below adaptive minimum")
        broker = signal.metadata.get("broker_quality") or {}
        if isinstance(broker, dict) and broker:
            agreement_state = str(broker.get("quote_agreement_state", "") or "")
            quote_quality_state = str(broker.get("quote_quality_state", "") or "")
            spread_regime = str(broker.get("spread_regime", "") or "")
            if agreement_state == "severe_divergence":
                notes.append("brokers materially disagree on price")
            elif agreement_state == "divergent":
                notes.append("brokers are showing mild price divergence")
            if quote_quality_state in {"stale", "delayed"}:
                notes.append(f"quote quality is {quote_quality_state}")
            if spread_regime in {"stressed", "extreme"}:
                notes.append(f"spread regime is {spread_regime}")
        cross_alignment = _maybe_float(signal.metadata.get("cross_asset_alignment"), None)
        cross_peer = str(signal.metadata.get("cross_asset_primary_peer", "") or "")
        cross_relation = str(signal.metadata.get("cross_asset_primary_relation", "") or "")
        if cross_alignment is not None:
            if cross_alignment >= 0.25:
                notes.append(
                    f"cross-asset spillover supports the trade"
                    f"{f' via {cross_peer}' if cross_peer else ''}"
                )
            elif cross_alignment <= -0.25:
                detail = cross_peer or cross_relation or "related markets"
                notes.append(f"cross-asset spillover conflicts with direction ({detail})")
        return notes

    def score(self, signal, context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        context = context or {}

        seed_source = str(signal.metadata.get("seed_source", "") or "").strip().lower()
        playbook_confidence = _clip(_maybe_float(signal.metadata.get("playbook_confidence"), 0.0))
        ml_seed_score = _clip(
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
        if seed_source == "playbook":
            seed_score = max(
                ml_seed_score,
                playbook_confidence,
                _clip(_maybe_float(signal.metadata.get("seed_candidate_score"), getattr(signal, "confidence", 0.0))),
            )
        else:
            seed_score = ml_seed_score
        structure_score = self._structure_quality(signal, context)
        regime_score = self._regime_quality(signal, context)
        rr_score = self._risk_reward_quality(signal)
        liquidity_score = self._liquidity_quality(signal, context)
        live_score, live_payload = self._live_validation(signal.asset)
        expectancy_score, expectancy_payload = self._execution_expectancy(signal)
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
            "playbook": (self._playbook_quality(signal), 0.08),
            "ml_alignment": (self._ml_alignment_quality(signal, context), 0.07),
            "broker_quality": (self._broker_quality(signal), 0.07),
            "microstructure": (self._microstructure_quality(signal), 0.05),
            "cross_asset": (self._cross_asset_quality(signal), 0.04),
            "news": (self._news_quality(signal), 0.04),
            "session": (self._session_quality(signal), 0.03),
            "entry": (self._entry_quality(signal), 0.05),
            "sentiment": (self._sentiment_quality(signal), 0.05),
            "whales": (self._whale_quality(signal), 0.04),
            "order_flow": (self._orderflow_quality(signal), 0.04),
            "memory": (self._memory_quality(signal), 0.06),
            "policy": (self._policy_quality(signal), 0.06),
            "execution_expectancy": (expectancy_score, 0.10),
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
        expectancy_cap = self._expectancy_confidence_cap(expectancy_score, expectancy_payload)
        if expectancy_cap is not None:
            final_score = min(final_score, expectancy_cap)
        if live_payload.get("scope") in {"bootstrap", "unavailable"} and expectancy_payload.get("scope") in {"bootstrap", "unavailable"}:
            final_score = min(final_score, 0.72)
        final_score = _clip(final_score, 0.0, MAX_SIGNAL_CONFIDENCE)

        rr_gap = max(0.0, _maybe_float(signal.metadata.get("adaptive_rr_gap"), 0.0))
        notes = self._scorecard_notes(signal, live_payload, expectancy_payload, seed_source, rr_gap)

        return {
            "raw_score": round(raw_score, 4),
            "curved_score": round(curved_score, 4),
            "reliability": round(reliability, 4),
            "final_score": round(final_score, 4),
            "breakdown": breakdown,
            "live_validation": live_payload,
            "execution_expectancy": expectancy_payload,
            "notes": notes,
        }


_service = SignalScorecard()


def get_service() -> SignalScorecard:
    return _service
