from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, Iterable, List, Mapping, Optional
import time

from core.asset_profiles import classify_depth_feed, get_depth_feed_policy, get_execution_policy, get_profile
from core.signal import Signal
from core.signal_journal import KILLED, PASS
from utils.logger import get_logger


logger = get_logger()

STEP_MARKET = 1
STEP_INTELLIGENCE = 2
STEP_EXECUTION = 3
STEP_POLICY = 4
STEP_GOVERNANCE = 5


def _get_news_state(category: str) -> Dict[str, Any]:
    return {"state": "clear", "event": "", "impact": "", "direction": "", "mins_to": 0}


def _get_orderflow_imbalance(asset: str) -> float:
    return 0.0


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return float(default)
        return float(value)
    except Exception:
        return float(default)


def _clip(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, float(value)))


def _direction_sign(direction: str) -> int:
    token = str(direction or "").strip().upper()
    if token == "BUY":
        return 1
    if token == "SELL":
        return -1
    return 0


def _structure_direction(structure: Mapping[str, Any]) -> int:
    bias = str(structure.get("structure_bias") or "").strip().lower()
    if bias == "buy":
        return 1
    if bias == "sell":
        return -1
    return 0


def _as_dict(value: Any) -> Dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def count_valid_sources(signal: Signal) -> int:
    metadata = getattr(signal, "metadata", {}) or {}
    families = metadata.get("valid_source_families")
    if isinstance(families, Iterable) and not isinstance(families, (str, bytes, dict)):
        return len({str(item) for item in families if str(item).strip()})
    count = 0
    if isinstance(metadata.get("market_structure"), Mapping) and metadata.get("market_structure"):
        count += 1
    if isinstance(metadata.get("market_microstructure"), Mapping) and metadata.get("market_microstructure"):
        count += 1
    if isinstance(metadata.get("cross_asset_context"), Mapping) and metadata.get("cross_asset_context"):
        count += 1
    if metadata.get("depth_available"):
        count += 1
    if metadata.get("predictor_real") or metadata.get("predictor_prediction") not in (None, ""):
        count += 1
    if metadata.get("sentiment_score") not in (None, ""):
        count += 1
    if count:
        return count
    source_flags = [
        "predictor_real",
        "sentiment_real",
        "market_structure_real",
        "depth_available",
        "cross_asset_real",
        "macro_real",
    ]
    return sum(1 for key in source_flags if bool(metadata.get(key)))


class SignalDecisionEngine:
    def evaluate(self, signal: Signal, context: Optional[Dict[str, Any]] = None) -> Optional[Signal]:
        context = context or {}
        context.setdefault("decision_start", time.monotonic())
        try:
            if not self._apply_market_review(signal, context):
                return self._finalize(signal, context)
            if not self._apply_intelligence_review(signal, context):
                return self._finalize(signal, context)
            if not self._apply_policy_review(signal, context):
                return self._finalize(signal, context)
            if not self._apply_execution_review(signal, context):
                return self._finalize(signal, context)
            if not self._apply_governance_review(signal, context):
                return self._finalize(signal, context)
        except Exception as exc:
            logger.error(f"[DecisionEngine] fatal evaluation error: {exc}", exc_info=True)
            if signal.alive:
                signal.kill(f"decision engine exception: {exc}", STEP_GOVERNANCE)
        return self._finalize(signal, context)

    run = evaluate

    def preview(self, signal: Signal, context: Optional[Dict[str, Any]] = None) -> Signal:
        clone = Signal.from_dict(signal.to_dict())
        self.evaluate(clone, deepcopy(context or {}))
        return clone

    @staticmethod
    def _kill(signal: Signal, step: int, name: str, reason: str, data: Optional[Dict[str, Any]] = None) -> bool:
        before = signal.confidence
        signal.kill(reason, step)
        signal.journal.record(
            layer=step,
            name=name,
            decision=KILLED,
            reason=reason,
            conf_before=before,
            conf_after=signal.confidence,
            data=data or {},
        )
        return False

    @staticmethod
    def _pass(signal: Signal, step: int, name: str, reason: str, before: float, data: Optional[Dict[str, Any]] = None) -> bool:
        signal.step_reached = max(signal.step_reached, step)
        signal.journal.record(
            layer=step,
            name=name,
            decision=PASS,
            reason=reason,
            conf_before=before,
            conf_after=signal.confidence,
            data=data or {},
        )
        return True

    @staticmethod
    def _merge_context(signal: Signal, context: Mapping[str, Any]) -> Dict[str, Any]:
        metadata = signal.metadata
        structure = _as_dict(context.get("market_structure") or metadata.get("market_structure"))
        micro = _as_dict(context.get("market_microstructure") or metadata.get("market_microstructure"))
        cross = _as_dict(context.get("cross_asset_context") or metadata.get("cross_asset_context"))
        if structure:
            metadata["market_structure"] = structure
            for key in (
                "structure_bias",
                "alignment_score",
                "setup_quality",
                "extension_score",
                "target_efficiency_score",
                "impulse_age_bars",
                "pattern_family",
                "trend_5m",
                "entry_confirmation_ready",
                "entry_confirmation_count",
                "entry_confirmation_bars_required",
            ):
                if key in structure:
                    metadata[key] = structure[key]
        if micro:
            metadata["market_microstructure"] = micro
            for key in (
                "score",
                "microstructure_alignment",
                "orderflow_imbalance",
                "book_imbalance",
                "tick_imbalance",
                "trade_flow_score",
                "trade_delta_ratio",
                "trade_cvd_slope",
                "depth_available",
                "synthetic_depth_available",
                "depth_levels",
                "depth_quality",
                "depth_quality_tier",
                "depth_provider_trust_score",
                "depth_quote_alignment_score",
                "depth_quote_agreement_state",
                "microstructure_source",
                "depth_update_mode",
                "dom_event_backed",
                "dom_ladder_ready",
                "dom_source_fidelity",
                "dom_authority_tier",
                "dom_stream_health_known",
                "dom_stream_health_score",
                "dom_stream_degraded",
                "dom_depth_stream_missing",
                "dom_trade_stream_missing",
            ):
                if key in micro:
                    metadata[key] = micro[key]
            if "score" in micro and "microstructure_alignment" not in metadata:
                metadata["microstructure_alignment"] = micro.get("score")
        if cross:
            metadata["cross_asset_context"] = cross
            metadata["cross_asset_alignment"] = _safe_float(cross.get("alignment", cross.get("score")), 0.0)
            metadata["cross_asset_confidence"] = _safe_float(cross.get("confidence"), 0.0)
        if "sentiment_score" in context:
            metadata["sentiment_score"] = _safe_float(context.get("sentiment_score"), 0.0)
        return metadata

    def _apply_market_review(self, signal: Signal, context: Dict[str, Any]) -> bool:
        before = signal.confidence
        metadata = self._merge_context(signal, context)
        profile = get_profile(signal.asset)
        metadata["asset_profile_category"] = profile.category
        metadata["asset_universe_depth_rule"] = "source_aware_independent"

        status = _as_dict(context.get("market_status"))
        if status and status.get("market_open") is False:
            return self._kill(signal, STEP_MARKET, "market", f"market closed: {status.get('reason', 'closed')}")

        direction = _direction_sign(signal.direction)
        if direction == 0:
            return self._kill(signal, STEP_MARKET, "market", "missing trade direction")

        structure = _as_dict(metadata.get("market_structure"))
        structure_sign = _structure_direction(structure)
        alignment = _safe_float(structure.get("alignment_score", metadata.get("alignment_score")), 0.0)
        setup = _safe_float(structure.get("setup_quality", metadata.get("setup_quality")), 0.0)
        micro = _as_dict(metadata.get("market_microstructure"))
        book = _safe_float(micro.get("book_imbalance", metadata.get("book_imbalance")), 0.0)
        flow = _safe_float(micro.get("score", metadata.get("microstructure_alignment")), 0.0)
        depth_available = bool(micro.get("depth_available") or metadata.get("depth_available"))
        synthetic_depth = bool(micro.get("synthetic_depth_available") or metadata.get("synthetic_depth_available"))
        depth_quality = _safe_float(micro.get("depth_quality", metadata.get("depth_quality")), 0.0)
        trust = _safe_float(micro.get("depth_provider_trust_score", metadata.get("depth_provider_trust_score")), 0.0)
        depth_levels = int(
            _safe_float(
                micro.get("depth_levels")
                or metadata.get("depth_levels")
                or max(
                    _safe_float(micro.get("bid_level_count") or micro.get("visible_bid_levels"), 0.0),
                    _safe_float(micro.get("ask_level_count") or micro.get("visible_ask_levels"), 0.0),
                ),
                0.0,
            )
        )
        feed_class = str(micro.get("depth_feed_class") or metadata.get("depth_feed_class") or "").strip().lower()
        if not feed_class:
            feed_class = classify_depth_feed(
                asset=signal.asset,
                category=signal.category or profile.category,
                provider=str(micro.get("depth_provider") or micro.get("provider") or metadata.get("depth_provider") or ""),
                provider_class=str(micro.get("depth_provider_class") or metadata.get("depth_provider_class") or ""),
                source=str(micro.get("microstructure_source") or metadata.get("microstructure_source") or ""),
                depth_available=depth_available,
                synthetic_depth=synthetic_depth,
                levels=depth_levels,
            )
        depth_policy = get_depth_feed_policy(signal.asset, signal.category or profile.category, feed_class)
        feed_class = str(depth_policy.get("depth_feed_class") or feed_class)
        depth_min_levels = int(depth_policy.get("min_levels", 0) or 0)
        depth_min_quality = _safe_float(depth_policy.get("min_quality"), 1.0)
        depth_min_trust = _safe_float(depth_policy.get("min_trust"), 1.0)
        depth_support_floor = _safe_float(depth_policy.get("support_min"), 1.0)
        depth_conflict_floor = _safe_float(depth_policy.get("conflict_block"), 1.0)
        depth_actionable = bool(
            depth_available
            and not synthetic_depth
            and feed_class not in {"quote_only", "synthetic"}
            and depth_levels >= depth_min_levels
            and depth_quality >= depth_min_quality
            and trust >= depth_min_trust
        )
        metadata["depth_feed_class"] = feed_class
        metadata["depth_min_levels_required"] = depth_min_levels
        metadata["depth_source_independence_rule"] = "source_aware"
        metadata["depth_policy_conflict_block"] = round(depth_conflict_floor, 4)
        metadata["depth_policy_support_min"] = round(depth_support_floor, 4)
        directional_depth = max(book * direction, flow * direction)
        metadata["directional_depth_pressure"] = round(directional_depth, 4)

        notes: List[str] = []
        if structure_sign and structure_sign != direction and alignment >= 0.28:
            metadata["structure_conflict"] = True
            notes.append("structure_conflict")
            signal.reduce(0.05)
        if depth_available and not synthetic_depth:
            if not depth_actionable:
                notes.append(f"{feed_class}_not_actionable")
            elif directional_depth <= -depth_conflict_floor:
                metadata["depth_conflict"] = True
                return self._kill(signal, STEP_MARKET, "market", f"{feed_class} depth context conflicts with signal direction")
            if directional_depth >= depth_support_floor:
                signal.boost(0.025 if feed_class == "exchange_deep" else 0.015)
                notes.append(f"{feed_class}_support")
        elif not depth_available:
            notes.append("depth_absent_no_hard_block")

        news = _get_news_state(signal.category)
        if str(news.get("state", "")).lower() == "post" and str(news.get("impact", "")).upper() == "HIGH":
            event = str(news.get("event") or "high impact event")
            mins_since = _safe_float(news.get("mins_since", news.get("minutes_since", news.get("mins_to"))), 999.0)
            news_direction = str(news.get("direction") or "").upper()
            style = str(metadata.get("playbook_entry_style") or metadata.get("playbook_name") or "").lower()
            if "news_followthrough" in style and news_direction == str(signal.direction).upper():
                metadata["post_news_guard"] = {"action": "allow_news_followthrough", "event": event}
                notes.append("post_high_news_followthrough")
            elif mins_since <= 15:
                metadata["post_news_guard"] = {"action": "block_generic", "event": event}
                return self._kill(signal, STEP_MARKET, "market", f"generic entry is too early after HIGH impact event: {event}")
            elif mins_since <= 30:
                signal.reduce(0.03)
                metadata["news_cooldown_penalty"] = 0.03
                metadata["post_news_guard"] = {"action": "reduce_generic", "event": event}
                notes.append("post_high_event_cooldown")

        trend_1h = str(structure.get("trend_1h", "") or "").lower()
        trend_4h = str(structure.get("trend_4h", "") or "").lower()
        opposite_trends = {"trending_down", "down", "sell"} if direction > 0 else {"trending_up", "up", "buy"}
        htf_conflict = bool(trend_1h in opposite_trends and trend_4h in opposite_trends)
        if htf_conflict:
            if (
                depth_actionable
                and bool(depth_policy.get("confirmation_override_allowed"))
                and directional_depth >= max(0.22, depth_support_floor)
            ):
                signal.confidence = round(before - 0.015, 3)
                metadata["higher_timeframe_conflict_penalty"] = 0.035
                metadata["higher_timeframe_guard"] = {"action": "reduce_depth_override", "depth_flow_override_source": feed_class}
                notes.append("htf_depth_override")
            elif (
                bool(structure.get("failed_opposite_move_confirmed"))
                or bool(structure.get("liquidity_sweep_buy"))
                or bool(structure.get("liquidity_sweep_sell"))
                or _safe_float(structure.get("dominant_exhaustion_score"), 0.0) >= 0.35
            ):
                signal.reduce(0.03)
                metadata["higher_timeframe_conflict_penalty"] = 0.03
                metadata["higher_timeframe_guard"] = {"action": "reduce"}
                notes.append("htf_conflict")
                notes.append("htf_reversal_candidate")
            else:
                metadata["higher_timeframe_guard"] = {"action": "block"}
                return self._kill(signal, STEP_MARKET, "market", "higher timeframe structure is aligned against the trade (1h, 4h)")

        try:
            from services.market_hours_guard import open_spike_status

            open_spike = dict(open_spike_status(signal.asset, signal.category) or {})
        except Exception:
            open_spike = {}
        if open_spike.get("active"):
            market_label = str(open_spike.get("label") or "market open").strip()
            style = str(metadata.get("playbook_entry_style") or metadata.get("playbook_name") or "").lower()
            flow_override = bool(metadata.get("generic_flow_override_source")) or directional_depth >= 0.30
            if "opening_range" in style:
                metadata["open_spike_guard"] = {"action": "allow_open_specialist"}
                notes.append("open_spike_specialist")
            elif flow_override:
                signal.reduce(0.02)
                metadata["open_spike_guard"] = {"action": "reduce_flow_override", "open_flow_override_supported": True}
                notes.append("open_spike_flow_override")
            elif signal.category == "commodities":
                signal.reduce(0.03)
                metadata["open_spike_penalty"] = 0.03
                metadata["open_spike_guard"] = {"action": "reduce_generic"}
                notes.append("commodity_reopen_cooldown")
            else:
                metadata["open_spike_guard"] = {"action": "block_generic_continuation"}
                return self._kill(signal, STEP_MARKET, "market", f"generic continuation is too early after {market_label}")

        anchor_score = _safe_float(structure.get("session_anchor_support_score"), 0.0)
        if bool(structure.get("session_anchor_ready")) and anchor_score < -0.20:
            label = str(structure.get("session_anchor_label") or "Session anchor").strip()
            if anchor_score <= -0.60:
                metadata["session_anchor_guard"] = {"action": "block", "score": anchor_score}
                return self._kill(signal, STEP_MARKET, "market", f"{label} is rejecting the trade")
            penalty = 0.02 if str(structure.get("session_anchor_type", "")).lower() == "crypto_open_stack" else 0.03
            signal.reduce(penalty)
            metadata["session_anchor_penalty"] = penalty
            metadata["session_anchor_guard"] = {"action": "reduce", "score": anchor_score}
            notes.append("session_anchor_conflict_penalty")

        if alignment <= 0.05 and setup <= 0.05:
            return self._kill(signal, STEP_MARKET, "market", "neutral structure")

        signal.confidence = round(signal.confidence, 3)
        metadata["market_review_notes"] = notes
        return self._pass(signal, STEP_MARKET, "market", "market context accepted", before, {"notes": notes})

    def _apply_intelligence_review(self, signal: Signal, context: Dict[str, Any]) -> bool:
        before = signal.confidence
        direction = _direction_sign(signal.direction)
        predictor = context.get("predictor_prediction", context.get("ml_prediction"))
        confidence = _safe_float(context.get("predictor_confidence", context.get("ml_confidence")), 0.0)
        if predictor not in (None, ""):
            pred_value = _safe_float(predictor, 0.5)
            pred_direction = 1 if pred_value >= 0.5 else -1
            signal.metadata["predictor_prediction"] = pred_value
            signal.metadata["predictor_confidence"] = confidence
            signal.metadata["predictor_direction_agrees"] = bool(pred_direction == direction)
            if confidence >= 0.55:
                if pred_direction == direction:
                    signal.boost(0.015)
                else:
                    signal.reduce(0.02)
        return self._pass(signal, STEP_INTELLIGENCE, "intelligence", "intelligence context accepted", before)

    def _apply_policy_review(self, signal: Signal, context: Dict[str, Any]) -> bool:
        before = signal.confidence
        try:
            from services.adaptive_policy_service import get_service as get_adaptive_policy_service

            thresholds = get_adaptive_policy_service().get_thresholds(
                signal.asset,
                signal.category,
                context=context,
                signal=signal,
            )
        except Exception:
            thresholds = {}
        policy = get_execution_policy(signal.asset)
        merged = dict(policy)
        merged.update(thresholds or {})
        signal.metadata["adaptive_policy"] = merged
        signal.metadata["effective_execution_policy"] = merged
        if merged.get("block_new_entries"):
            return self._kill(signal, STEP_POLICY, "policy", str(merged.get("block_reason") or "policy blocked entry"))
        return self._pass(signal, STEP_POLICY, "policy", "policy accepted", before, {"policy": merged})

    def _apply_execution_review(self, signal: Signal, context: Dict[str, Any]) -> bool:
        before = signal.confidence
        policy = _as_dict(signal.metadata.get("effective_execution_policy"))
        min_conf = _safe_float(policy.get("min_final_confidence", policy.get("min_confidence")), 0.58)
        min_rr = _safe_float(policy.get("min_rr"), 1.45)
        max_spread_pct = _safe_float(policy.get("max_spread"), 0.0035)
        price = _safe_float(context.get("current_price", signal.entry_price), signal.entry_price)
        spread = _safe_float(context.get("spread", signal.metadata.get("spread")), 0.0)
        data: Dict[str, Any] = {}
        notes: List[str] = []
        if not self._execution_spread_gate(
            signal,
            spread=spread,
            price=price,
            max_spread_pct=max_spread_pct,
            conf_before=before,
            data=data,
            notes=notes,
        ):
            return False
        if not self._execution_source_floor_gate(signal, context, before, data):
            return False
        structure = _as_dict(signal.metadata.get("market_structure") or context.get("market_structure"))
        if not self._execution_late_entry_risk_gate(
            signal,
            adaptive_policy={"raw": policy},
            conf_before=before,
            structure=structure,
            data=data,
            notes=notes,
        ):
            return False
        rr = _safe_float(signal.risk_reward, 0.0)
        if rr and rr < min_rr:
            return self._kill(signal, STEP_EXECUTION, "execution", f"risk reward below floor {rr:.2f} < {min_rr:.2f}", data)
        if signal.confidence < min_conf:
            return self._kill(signal, STEP_EXECUTION, "execution", f"final confidence below floor {signal.confidence:.3f} < {min_conf:.3f}", data)
        self._execution_ensure_take_profit_levels(signal)
        return self._pass(signal, STEP_EXECUTION, "execution", "execution accepted", before, data)

    def _apply_governance_review(self, signal: Signal, context: Dict[str, Any]) -> bool:
        before = signal.confidence
        try:
            from services.signal_scorecard import get_service as get_scorecard_service
            from services.signal_governance import signal_governance

            scorecard = get_scorecard_service().score(signal, context)
            signal.metadata["scorecard"] = scorecard
            verdict = signal_governance.evaluate(signal, context)
            signal.metadata["governance_validation"] = verdict
        except Exception as exc:
            scorecard = {"approved": True, "score": signal.confidence}
            verdict = {"approved": True, "reason": f"governance unavailable: {exc}"}
        if not bool(scorecard.get("approved", True)):
            return self._kill(signal, STEP_GOVERNANCE, "governance", "scorecard below universal floor", {"scorecard": scorecard})
        if not bool(verdict.get("approved", True)):
            return self._kill(signal, STEP_GOVERNANCE, "governance", f"governance rejected: {verdict.get('reason')}", {"verdict": verdict})
        return self._pass(signal, STEP_GOVERNANCE, "governance", "governance accepted", before, {"scorecard": scorecard, "verdict": verdict})

    def _execution_spread_gate(
        self,
        signal: Signal,
        *,
        spread: Any,
        price: Any,
        max_spread_pct: float,
        conf_before: float,
        data: Dict[str, Any],
        notes: List[str],
    ) -> bool:
        spread_value = _safe_float(spread, 0.0)
        price_value = _safe_float(price, signal.entry_price)
        spread_pct = spread_value / price_value if price_value > 0.0 else 0.0
        micro = _as_dict(signal.metadata.get("market_microstructure"))
        regime = str(signal.metadata.get("broker_spread_regime") or micro.get("spread_regime") or "").lower()
        stress = _safe_float(micro.get("spread_stress", signal.metadata.get("spread_stress")), 1.0)
        data["spread_pct"] = round(spread_pct, 6)
        if spread_pct > max_spread_pct:
            return self._kill(signal, STEP_EXECUTION, "execution", "spread above execution threshold", data)
        if regime == "wide" and stress >= 1.15:
            return self._kill(signal, STEP_EXECUTION, "execution", "spread regime is wide", data)
        notes.append("spread_ok")
        return True

    def _execution_source_floor_gate(
        self,
        signal: Signal,
        context: Mapping[str, Any],
        conf_before: float,
        data: Dict[str, Any],
    ) -> bool:
        metadata = signal.metadata
        families = metadata.get("valid_source_families")
        family_count = count_valid_sources(signal)
        scorecard = _as_dict(metadata.get("scorecard"))
        breakdown = _as_dict(scorecard.get("breakdown") or _as_dict(scorecard.get("components")))
        critical_values = [
            _safe_float(breakdown.get(key), 1.0)
            for key in ("structure", "entry", "microstructure", "order_flow", "cross_asset", "sentiment")
            if key in breakdown
        ]
        weak_critical = bool(critical_values and sum(critical_values) / len(critical_values) < 0.30)
        if family_count < 3 or weak_critical:
            return self._kill(signal, STEP_EXECUTION, "execution", "source floor failure", data)
        data["valid_source_family_count"] = family_count
        data["valid_source_families"] = list(families or [])
        return True

    @staticmethod
    def _execution_ensure_take_profit_levels(signal: Signal) -> None:
        entry = _safe_float(signal.entry_price, 0.0)
        stop_loss = _safe_float(signal.stop_loss, 0.0)
        take_profit = _safe_float(signal.take_profit, 0.0)
        direction = _direction_sign(signal.direction)

        def _directional_levels(raw_levels: Any, max_rr: float) -> List[float]:
            risk_distance = abs(entry - stop_loss)
            clean_levels: List[float] = []
            for item in list(raw_levels or []):
                value = _safe_float(item, 0.0)
                if value <= 0.0 or entry <= 0.0:
                    continue
                if direction > 0 and value <= entry:
                    continue
                if direction < 0 and value >= entry:
                    continue
                if risk_distance > 0.0:
                    level_rr = abs(value - entry) / max(risk_distance, 1e-9)
                    if level_rr > max_rr:
                        continue
                clean_levels.append(value)
            reverse = direction < 0
            return sorted(dict.fromkeys(clean_levels), reverse=reverse)[:5]

        category_key = str(getattr(signal, "category", "") or "").strip().lower()
        max_level_rr = {
            "forex": 1.75,
            "indices": 2.05,
            "commodities": 2.20,
            "crypto": 2.70,
        }.get(category_key, 2.20)

        existing = _directional_levels(signal.take_profit_levels, max_level_rr)
        if existing:
            signal.take_profit_levels = existing
            return

        structure = _as_dict(signal.metadata.get("market_structure"))
        key = "bullish_target_levels" if direction > 0 else "bearish_target_levels"
        targets = structure.get(key) or structure.get("target_levels") or []
        clean = _directional_levels(targets, max_level_rr)
        if clean:
            signal.take_profit_levels = clean[:5]
        elif take_profit and not signal.take_profit_levels:
            signal.take_profit_levels = [float(take_profit)]

    def _execution_late_entry_risk_gate(
        self,
        signal: Signal,
        *,
        adaptive_policy: Dict[str, Any],
        conf_before: float,
        structure: Dict[str, Any],
        data: Dict[str, Any],
        notes: List[str],
    ) -> bool:
        metadata = signal.metadata
        policy = get_execution_policy(signal.asset)
        policy.update(_as_dict((adaptive_policy or {}).get("raw")))
        direction = _direction_sign(signal.direction)
        micro = _as_dict(metadata.get("market_microstructure"))
        if not micro:
            micro = {key: metadata.get(key) for key in metadata.keys()}

        depth_available = bool(micro.get("depth_available") or metadata.get("depth_available"))
        synthetic_depth = bool(micro.get("synthetic_depth_available") or metadata.get("synthetic_depth_available"))
        depth_quality = _safe_float(micro.get("depth_quality", metadata.get("depth_quality")), 0.0)
        trust = _safe_float(micro.get("depth_provider_trust_score", metadata.get("depth_provider_trust_score")), 0.0)
        depth_levels = int(
            _safe_float(
                micro.get("depth_levels")
                or metadata.get("depth_levels")
                or max(
                    _safe_float(micro.get("bid_level_count") or micro.get("visible_bid_levels"), 0.0),
                    _safe_float(micro.get("ask_level_count") or micro.get("visible_ask_levels"), 0.0),
                ),
                0.0,
            )
        )
        feed_class = str(micro.get("depth_feed_class") or metadata.get("depth_feed_class") or "").strip().lower()
        if not feed_class:
            feed_class = classify_depth_feed(
                asset=signal.asset,
                category=signal.category,
                provider=str(micro.get("depth_provider") or micro.get("provider") or metadata.get("depth_provider") or ""),
                provider_class=str(micro.get("depth_provider_class") or metadata.get("depth_provider_class") or ""),
                source=str(micro.get("microstructure_source") or metadata.get("microstructure_source") or ""),
                depth_available=depth_available,
                synthetic_depth=synthetic_depth,
                levels=depth_levels,
            )
        depth_feed_policy = get_depth_feed_policy(signal.asset, signal.category, feed_class)
        feed_class = str(depth_feed_policy.get("depth_feed_class") or feed_class)
        depth_min_levels = int(depth_feed_policy.get("min_levels", 0) or 0)
        depth_min_quality = _safe_float(depth_feed_policy.get("min_quality"), 1.0)
        depth_min_trust = _safe_float(depth_feed_policy.get("min_trust"), 1.0)
        depth_support_floor = _safe_float(depth_feed_policy.get("support_min"), 1.0)
        depth_conflict_floor = _safe_float(depth_feed_policy.get("conflict_block"), policy.get("depth_conflict_block", 0.22))
        metadata["depth_feed_class"] = feed_class
        metadata["depth_min_levels_required"] = depth_min_levels
        metadata["depth_policy_support_min"] = round(depth_support_floor, 4)
        metadata["depth_policy_conflict_block"] = round(depth_conflict_floor, 4)
        orderflow = _safe_float(micro.get("orderflow_imbalance", metadata.get("orderflow_imbalance")), 0.0) * direction
        book = _safe_float(micro.get("book_imbalance", metadata.get("book_imbalance")), 0.0) * direction
        flow = _safe_float(micro.get("score", metadata.get("microstructure_alignment")), 0.0) * direction
        trade = _safe_float(micro.get("trade_flow_score", metadata.get("trade_flow_score")), 0.0) * direction
        directional_flow = max(book, flow, trade)
        hostile_flow = min(book, flow, trade, orderflow)
        event_ladder = bool(micro.get("dom_event_backed") or metadata.get("dom_event_backed")) and bool(
            micro.get("dom_ladder_ready") or metadata.get("dom_ladder_ready")
        )
        stream_score = _safe_float(micro.get("dom_stream_health_score", metadata.get("dom_stream_health_score")), 1.0)
        stream_degraded = bool(micro.get("dom_stream_degraded") or metadata.get("dom_stream_degraded") or metadata.get("dom_depth_stream_missing"))
        stream_floor = _safe_float(policy.get("dom_stream_hard_floor"), 0.30)
        stream_hard_floor = bool(event_ladder and (stream_score < stream_floor or stream_degraded))

        depth_capability_ready = (
            depth_available
            and not synthetic_depth
            and feed_class not in {"quote_only", "synthetic"}
            and depth_levels >= depth_min_levels
            and depth_quality >= depth_min_quality
            and trust >= depth_min_trust
            and not stream_hard_floor
        )
        depth_source_ready = bool(depth_capability_ready and directional_flow >= depth_support_floor)
        extension = _safe_float(structure.get("extension_score", metadata.get("extension_score")), 0.0)
        target = _safe_float(structure.get("target_efficiency_score", metadata.get("target_efficiency_score")), 0.0)
        impulse_age = int(_safe_float(structure.get("impulse_age_bars", metadata.get("impulse_age_bars")), 0.0))
        alignment = _safe_float(structure.get("alignment_score", metadata.get("alignment_score")), 0.0)
        setup = _safe_float(structure.get("setup_quality", metadata.get("setup_quality")), 0.0)
        confirmation_ready = bool(structure.get("entry_confirmation_ready", metadata.get("entry_confirmation_ready")))
        fast_confirmation_ready = bool(structure.get("fast_entry_confirmation_ready", metadata.get("fast_entry_confirmation_ready")))
        exchange_depth_source = bool(depth_source_ready and feed_class == "exchange_deep")
        broker_l2_source = bool(depth_source_ready and feed_class in {"broker_l2", "thin_broker_l2"})
        broker_l2_execution_support = bool(
            broker_l2_source
            and (
                confirmation_ready
                or fast_confirmation_ready
                or (alignment >= 0.62 and setup >= 0.55)
            )
        )
        support_quality = bool(exchange_depth_source or broker_l2_execution_support)
        entry_style = str(metadata.get("playbook_entry_style") or "").strip().lower()
        context_candidate = bool(
            "context_continuation" in entry_style
            and alignment >= 0.70
            and setup >= 0.62
            and target >= 0.18
            and impulse_age <= max(6, int(_safe_float(policy.get("max_impulse_age_bars"), 6)))
        )

        hard_blocks: List[str] = list(metadata.get("execution_hard_blocks") or [])
        late_reasons: List[str] = []
        relief_flags = dict(metadata.get("execution_relief_flags") or {})
        effective_policy = metadata.setdefault("effective_execution_policy", dict(policy))
        effective_policy.setdefault("preferred_true_depth", True)
        effective_policy.setdefault("risk_kill_threshold", _safe_float(policy.get("risk_kill_threshold"), 0.92))
        update_mode = str(micro.get("depth_update_mode", metadata.get("depth_update_mode", "")) or "").lower()
        source_name = str(micro.get("microstructure_source", metadata.get("microstructure_source", "")) or "").lower()
        provider_class = str(micro.get("depth_provider_class", metadata.get("depth_provider_class", "")) or "").lower()
        trusted_real_snapshot = bool(
            update_mode in {"stream_snapshot", "snapshot_poll"}
            and depth_source_ready
            and (
                source_name in {"binance_rest_depth", "binance_live_depth", "ctrader_live_depth", "dukascopy_live_depth"}
                or provider_class in {"exchange", "exchange_depth", "broker_l2", "sidecar"}
            )
        )
        exchange_true_depth_source = bool(exchange_depth_source and (event_ladder or update_mode == "event_stream" or trusted_real_snapshot))
        broker_l2_context_source = bool(broker_l2_source and trusted_real_snapshot)
        true_depth_source = bool(exchange_true_depth_source or broker_l2_context_source)
        support_quality = bool(support_quality and true_depth_source)
        strong_true_depth = bool(exchange_true_depth_source and depth_quality >= 0.55 and trust >= 0.58 and directional_flow >= 0.20)
        supportive_direction = str(metadata.get("cross_asset_supportive_direction") or "").strip().upper()
        cross_alignment = _safe_float(metadata.get("cross_asset_alignment"), 0.0)
        cross_confidence = _safe_float(metadata.get("cross_asset_confidence"), 0.0)
        if supportive_direction in {"BUY", "SELL"}:
            cross_conflict = supportive_direction != str(signal.direction).upper()
        else:
            cross_conflict = bool(cross_alignment * direction <= -0.25 and cross_confidence >= 0.35)
        trade_flow_conflict = bool(trade <= -0.22)
        derivative_support = bool(
            (direction > 0 and str(metadata.get("funding_bias", "")).upper() == "HIGH_LONG")
            or (direction < 0 and str(metadata.get("funding_bias", "")).upper() == "HIGH_SHORT")
        )
        derivative_conflict = bool(
            (direction > 0 and str(metadata.get("funding_bias", "")).upper() == "HIGH_SHORT")
            or (direction < 0 and str(metadata.get("funding_bias", "")).upper() == "HIGH_LONG")
            or (cross_conflict and str(metadata.get("oi_signal", "")).upper() == "TREND_CONTINUATION")
        )
        relief_flags.update(
            {
                "depth_sovereignty_supported": bool(exchange_true_depth_source and support_quality),
                "depth_sovereignty_source": feed_class if true_depth_source else "flow",
                "depth_sovereignty_reason": (
                    "supported:exchange_deep"
                    if exchange_true_depth_source and support_quality
                    else "supported:broker_l2_context"
                    if broker_l2_execution_support
                    else "not_supported"
                ),
                "broker_l2_context_support": bool(broker_l2_execution_support),
                "depth_feed_class": feed_class,
                "depth_min_levels_required": depth_min_levels,
                "strong_true_depth_support": bool(strong_true_depth),
                "depth_flow_sovereignty_candidate": bool(strong_true_depth and "elite_flow_continuation" not in entry_style),
                "depth_flow_sovereignty_rescue_candidate": bool(exchange_true_depth_source and support_quality and not (confirmation_ready or fast_confirmation_ready)),
                "depth_flow_sovereignty_confirmation_override": bool(exchange_true_depth_source and support_quality and not (confirmation_ready or fast_confirmation_ready)),
                "snapshot_dom_requires_confirmation": bool(
                    depth_available
                    and (
                        feed_class in {"broker_l2", "thin_broker_l2"}
                        or (update_mode == "snapshot_poll" and not trusted_real_snapshot)
                    )
                    and not (confirmation_ready or fast_confirmation_ready)
                ),
                "dom_stream_hard_floor_breached": bool(stream_hard_floor),
                "event_ladder_hostile_flow": bool(
                    event_ladder
                    and (
                        hostile_flow <= -0.20
                        or (bool(metadata.get("dom_fragmented_market")) and hostile_flow <= -0.08 and cross_conflict)
                    )
                ),
                "cross_asset_directional_conflict": bool(cross_conflict),
                "event_ladder_cross_market_conflict": bool(
                    event_ladder
                    and cross_conflict
                    and (
                        hostile_flow <= -0.20
                        or (bool(metadata.get("dom_fragmented_market")) and hostile_flow <= -0.08)
                    )
                ),
                "event_ladder_cross_market_hard_block": False,
                "crypto_breadth_conflict": bool(cross_conflict),
                "crypto_derivative_conflict": bool(derivative_conflict),
                "crypto_derivative_support": bool(derivative_support),
                "cross_market_breadth_conflict": bool(cross_conflict),
                "derivative_conflict": bool(derivative_conflict),
                "derivative_support": bool(derivative_support),
                "trade_flow_conflict": bool(trade_flow_conflict),
                "context_continuation_execution_candidate": bool(context_candidate),
                "context_confirmation_override": bool(context_candidate and not (confirmation_ready or fast_confirmation_ready)),
            }
        )
        if "elite_flow_continuation" in entry_style:
            relief_flags["depth_flow_sovereignty_candidate"] = False
        synthetic_stop_hunt_rescue = bool(
            synthetic_depth
            and not depth_available
            and directional_flow >= 0.28
            and _safe_float(metadata.get("stop_hunt_risk"), 0.0) >= 0.50
        )
        if synthetic_stop_hunt_rescue:
            relief_flags["depth_flow_sovereignty_rescue_candidate"] = True
            relief_flags["depth_flow_sovereignty_confirmation_override"] = True
            relief_flags["depth_flow_stop_hunt_override"] = True
        metadata.setdefault("guarded_force_candidate", False)
        metadata.setdefault("guarded_force_applied", False)
        metadata.setdefault("guarded_force_blocked_by", [])
        metadata.setdefault("breakout_momentum_late_override_blocked_by", [])
        trusted_stream_fallback = bool(source_name in {"binance_live_depth", "binance_rest_depth", "live_store_depth"})
        if stream_hard_floor and not trusted_stream_fallback:
            metadata["dom_stream_health_hard_floor_breached"] = True
            hard_blocks.append("event-ladder stream integrity has degraded while continuation pressure is already elevated")
        elif stream_hard_floor:
            metadata["dom_stream_health_hard_floor_breached"] = True
            relief_flags["depth_sovereignty_supported"] = bool(strong_true_depth)
            relief_flags["depth_sovereignty_reason"] = "supported:trusted_real_dom_fallback" if strong_true_depth else relief_flags["depth_sovereignty_reason"]

        trend5 = str(structure.get("trend_5m", metadata.get("trend_5m", "")) or "").lower()
        close_location = _safe_float(structure.get("close_location", metadata.get("close_location")), 0.5)
        reclaim_against = bool(
            (direction > 0 and (structure.get("liquidity_sweep_sell") or trend5 == "trending_down" or close_location <= 0.25))
            or (direction < 0 and (structure.get("liquidity_sweep_buy") or trend5 == "trending_up" or close_location >= 0.75))
        )
        if reclaim_against:
            relief_flags["continuation_reclaim_pressure"] = True
            hard_blocks.append("continuation entry is fighting an opposite-side reclaim")
            effective_policy["continuation_reclaim_pressure"] = True
            effective_policy["trigger_reversal_against_trade"] = True

        cross_market_block = (
            relief_flags["event_ladder_cross_market_conflict"]
            and (not (confirmation_ready and fast_confirmation_ready) or hostile_flow < -0.30)
        )
        if cross_market_block:
            relief_flags["event_ladder_cross_market_hard_block"] = True
            hard_blocks.append("event-ladder DOM, hostile flow, and cross-asset conflict are aligned against the continuation")
        elif cross_conflict and not context_candidate and not (confirmation_ready and fast_confirmation_ready) and (trade_flow_conflict or hostile_flow <= -0.22) and not derivative_support:
            hard_blocks.append("cross-market breadth and live flow are aligned against the trade")

        entry_policy = _as_dict(structure.get("regime_entry_policy"))
        max_extension = _safe_float(entry_policy.get("max_extension_score", policy.get("max_extension_score")), 1.18)
        max_age = int(_safe_float(entry_policy.get("max_impulse_age_bars", policy.get("max_impulse_age_bars")), 6))
        min_target = _safe_float(entry_policy.get("min_target_efficiency", policy.get("min_target_efficiency")), 0.18)
        if extension > max_extension:
            late_reasons.append("entry is extended after the impulse")
            if not support_quality:
                hard_blocks.append("entry is too extended without true-depth support")
        if impulse_age > max_age:
            late_reasons.append("setup is too old after the initial impulse")
            if not support_quality:
                hard_blocks.append("setup is too old after the initial impulse")
        if target < min_target:
            late_reasons.append("target efficiency is weak")
            if not support_quality:
                hard_blocks.append("target efficiency is below execution floor")
        if not (confirmation_ready or fast_confirmation_ready) and not context_candidate and alignment < 0.55 and setup < 0.50:
            late_reasons.append("entry confirmation delay has not completed yet")
            if not support_quality:
                hard_blocks.append("entry confirmation delay is still pending")
        if directional_flow <= -depth_conflict_floor and depth_capability_ready and not (confirmation_ready and fast_confirmation_ready):
            hard_blocks.append(f"{feed_class} pressure conflicts with signal direction")
        if target < 0.08:
            hard_blocks.append("too little clean space remains to the target")
            metadata["context_pressure_soft_override_applied"] = False
            metadata["context_pressure_soft_override_blocked_by"] = "too little clean space remains to the target"
            if "too little clean space remains to the target" not in metadata["guarded_force_blocked_by"]:
                metadata["guarded_force_blocked_by"].append("too little clean space remains to the target")
            if "too little clean space remains to the target" not in metadata["breakout_momentum_late_override_blocked_by"]:
                metadata["breakout_momentum_late_override_blocked_by"].append("too little clean space remains to the target")

        late_score = _clip(
            max(0.0, extension - 0.80) * 0.35
            + max(0, impulse_age - 2) * 0.055
            + max(0.0, min_target - target) * 0.45
            + (0.18 if not (confirmation_ready or fast_confirmation_ready) else 0.0)
            - (0.22 if support_quality else 0.0)
        )
        metadata["late_entry_risk_score"] = round(late_score, 4)
        metadata["late_entry_risk_reasons"] = late_reasons
        if "context_pressure" in entry_style:
            metadata["context_pressure_soft_override_candidate"] = True
            metadata.setdefault("context_pressure_soft_override_applied", not hard_blocks)
            metadata.setdefault(
                "context_pressure_soft_override_removed_blocks",
                ["entry confirmation delay is still pending", "pattern family ranks below elite threshold"],
            )
        if true_depth_source:
            metadata["true_depth_provider_kind"] = (
                "exchange_deep"
                if exchange_true_depth_source
                else "broker_l2"
                if broker_l2_context_source
                else "direct"
            )
            metadata["sidecar_true_depth_source"] = metadata["true_depth_provider_kind"] in {"broker_l2", "sidecar"}
            metadata["broker_l2_true_depth_source"] = metadata["true_depth_provider_kind"] == "broker_l2"
            if metadata["broker_l2_true_depth_source"]:
                effective_policy["preferred_true_depth_min_trust_score"] = trust
            relief_flags["trusted_real_dom_book_available"] = True
            relief_flags["trusted_real_dom_fallback_support"] = bool(exchange_true_depth_source and (trusted_stream_fallback or trusted_real_snapshot))
            relief_flags["trusted_real_broker_l2_support"] = bool(broker_l2_execution_support)
        breakout_momentum = bool("breakout_close" in entry_style and setup >= 0.50)
        relief_flags["breakout_ignition_candidate"] = bool("breakout_ignition" in entry_style)
        relief_flags["breakout_ignition_confirmation_override"] = bool(relief_flags["breakout_ignition_candidate"])
        relief_flags["breakout_momentum_late_override_candidate"] = breakout_momentum
        relief_flags["breakout_momentum_depth_ok"] = bool(exchange_true_depth_source or broker_l2_execution_support)
        metadata["breakout_momentum_late_override_applied"] = bool(breakout_momentum and not hard_blocks)
        relief_flags["breakout_momentum_late_override_applied"] = metadata["breakout_momentum_late_override_applied"]
        if exchange_true_depth_source and depth_quality >= 0.80 and "elite_flow_continuation" in entry_style and not hard_blocks:
            metadata["guarded_force_candidate"] = True
            metadata["guarded_force_applied"] = True
            metadata["guarded_force_removed_blocks"] = [
                "entry confirmation delay is still pending",
                "pattern family ranks below elite threshold",
            ]
        if relief_flags.get("event_ladder_hostile_flow"):
            hostile_components = [
                metadata.get("dom_add_intent_bias"),
                metadata.get("dom_cancel_pressure_bias"),
                metadata.get("dom_queue_erosion_bias"),
                metadata.get("dom_trade_absorption_proxy"),
                metadata.get("dom_refill_after_sweep_bias"),
                metadata.get("dom_trade_backed_iceberg_proxy"),
            ]
            relief_flags["event_ladder_hostile_flow_component_count"] = sum(1 for item in hostile_components if _safe_float(item, 0.0) < 0.0)
        if relief_flags["breakout_ignition_candidate"]:
            relief_flags["depth_flow_sovereignty_candidate"] = False
        if breakout_momentum:
            metadata["breakout_momentum_late_override_removed_blocks"] = [
                "setup is too old and already stretched",
                "entry confirmation delay is still pending",
                "pattern family ranks below elite threshold",
            ]
            effective_policy["guarded_force_entry_enabled"] = True
        metadata["execution_relief_flags"] = relief_flags
        metadata["execution_hard_blocks"] = hard_blocks
        data["late_entry_risk"] = round(late_score, 4)
        data["depth_sovereignty_supported"] = bool(exchange_true_depth_source and support_quality)
        data["broker_l2_context_support"] = bool(broker_l2_execution_support)
        data["depth_feed_class"] = feed_class
        if hard_blocks:
            return self._kill(signal, STEP_EXECUTION, "execution", "execution hard block on signal: " + "; ".join(hard_blocks[:3]), data)
        notes.append("late_entry_ok")
        return True

    def _finalize(self, signal: Signal, context: Dict[str, Any]) -> Optional[Signal]:
        signal.metadata["decision_data"] = {
            "alive": bool(signal.alive),
            "reason": signal.kill_reason,
            "step_reached": signal.step_reached,
            "elapsed_ms": round((time.monotonic() - _safe_float(context.get("decision_start"), time.monotonic())) * 1000.0, 2),
        }
        return signal


decision_engine = SignalDecisionEngine()
