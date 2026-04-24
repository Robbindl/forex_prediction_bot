from __future__ import annotations

from typing import Any, Dict, Iterable, List, Sequence, Tuple

from config.config import (
    CATEGORY_CAPS,
    CATEGORY_CAP_SOFT_BUFFER,
    PORTFOLIO_MAX_SAME_DIRECTION_POSITIONS,
    SPREAD_THRESHOLDS,
)


def _clip(value: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, float(value)))


def _direction_sign(direction: str) -> int:
    return 1 if str(direction).upper() == "BUY" else -1


def _aligned_score(raw: float, direction: str) -> float:
    sign = 1.0 if str(direction).upper() == "BUY" else -1.0
    return _clip((float(raw or 0.0) * sign + 1.0) / 2.0)


def _is_true_depth_source(source: str) -> bool:
    token = str(source or "").strip().lower()
    return token in {"order_flow_true_depth", "dukascopy_live_depth"}


class OpportunityRanker:
    @staticmethod
    def _broker_quality_score(signal) -> float:
        broker = signal.metadata.get("broker_quality") or {}
        if not isinstance(broker, dict) or not broker:
            return 0.55

        score = _clip(float(broker.get("score", 0.55) or 0.55))
        agreement_state = str(broker.get("quote_agreement_state", "") or "").lower()
        quote_quality_state = str(broker.get("quote_quality_state", "") or "").lower()
        spread_regime = str(broker.get("spread_regime", "") or "").lower()
        transition_risk = _clip(float(broker.get("market_transition_risk", 0.0) or 0.0))

        if agreement_state in {"strong", "aligned"}:
            score += 0.08
        elif agreement_state == "divergent":
            score -= 0.14
        elif agreement_state == "severe_divergence":
            score -= 0.24

        if quote_quality_state == "fresh":
            score += 0.05
        elif quote_quality_state in {"stale", "delayed"}:
            score -= 0.10

        if spread_regime == "tight":
            score += 0.05
        elif spread_regime == "normal":
            score += 0.02
        elif spread_regime == "wide":
            score -= 0.05
        elif spread_regime == "stressed":
            score -= 0.12
        elif spread_regime == "extreme":
            score -= 0.18

        score -= transition_risk * 0.16
        if bool(broker.get("fallback_active")):
            score -= 0.03
        return round(_clip(score), 4)

    @staticmethod
    def _microstructure_score(signal) -> float:
        metadata = dict(getattr(signal, "metadata", {}) or {})
        aligned = metadata.get("microstructure_alignment")
        if aligned is None:
            aligned = metadata.get("microstructure_score")
        base = _aligned_score(float(aligned or 0.0), getattr(signal, "direction", "BUY"))

        tick = metadata.get("tick_imbalance")
        book = metadata.get("book_imbalance")
        stop_hunt_risk = _clip(float(metadata.get("stop_hunt_risk", 0.0) or 0.0))
        exhaustion_risk = _clip(float(metadata.get("exhaustion_risk", 0.0) or 0.0))

        components = [base]
        if tick is not None:
            components.append(_aligned_score(float(tick or 0.0), getattr(signal, "direction", "BUY")))
        if book is not None:
            components.append(_aligned_score(float(book or 0.0), getattr(signal, "direction", "BUY")))

        score = sum(components) / len(components)
        score -= stop_hunt_risk * 0.24
        score -= exhaustion_risk * 0.20

        if bool(metadata.get("depth_available")):
            source = str(metadata.get("microstructure_source", "") or "").lower()
            if _is_true_depth_source(source):
                score += 0.07
            else:
                score += 0.04
        elif bool(metadata.get("synthetic_depth_available")):
            score -= 0.04

        return round(_clip(score), 4)

    @staticmethod
    def _cross_asset_score(signal) -> float:
        metadata = dict(getattr(signal, "metadata", {}) or {})
        alignment = metadata.get("cross_asset_alignment")
        if alignment is None:
            return 0.55
        confidence = _clip(float(metadata.get("cross_asset_confidence", 0.0) or 0.0))
        base = _aligned_score(float(alignment or 0.0), "BUY")
        strength = _clip(0.45 + confidence * 0.55, 0.45, 1.0)
        return round(_clip(0.5 + (base - 0.5) * strength), 4)

    def _score_pair(
        self,
        signal,
        context: Dict[str, Any],
        open_positions: Sequence[Dict[str, Any]],
        candidate_counts: Dict[Tuple[str, str], int],
    ) -> Tuple[float, Dict[str, float]]:
        category = str(signal.category or "")
        direction = str(signal.direction or "BUY").upper()
        sign = _direction_sign(direction)

        confidence_score = _clip(float(signal.confidence or 0.0))

        structure_score = _clip(
            float(signal.metadata.get("setup_quality", 0.0) or 0.0) * 0.65
            + float(signal.metadata.get("alignment_score", 0.0) or 0.0) * 0.35
        )

        setup_alignment = max(
            abs(float(signal.metadata.get("pullback_score", 0.0) or 0.0)),
            abs(float(signal.metadata.get("breakout_score", 0.0) or 0.0)),
        )
        setup_score = _clip(setup_alignment)

        aligned_sentiment = float(signal.metadata.get("sentiment_score", 0.0) or 0.0) * sign
        sentiment_score = _clip((aligned_sentiment + 1.0) / 2.0)

        whale_score = 0.5
        whale_dominant = str(signal.metadata.get("whale_dominant", "") or "")
        whale_buy = float(signal.metadata.get("whale_bull_weight", 0.0) or 0.0)
        whale_bear = float(signal.metadata.get("whale_bear_weight", 0.0) or 0.0)
        whale_ratio = max(whale_buy, whale_bear)
        if whale_dominant:
            whale_score = 0.8 if whale_dominant == direction else 0.2
            whale_score = _clip(whale_score * 0.6 + min(1.0, whale_ratio) * 0.4)

        orderflow_score = 0.5
        if signal.metadata.get("orderflow_applicable") is True:
            aligned_of = float(signal.metadata.get("orderflow_imbalance", 0.0) or 0.0) * sign
            orderflow_score = _clip((aligned_of + 1.0) / 2.0)

        memory_score = 0.5
        if signal.metadata.get("memory_score") is not None or signal.metadata.get("memory_edge") is not None:
            raw_memory_score = signal.metadata.get("memory_score")
            if raw_memory_score is not None:
                memory_score = _clip(float(raw_memory_score or 50.0) / 100.0)
            else:
                memory_score = _clip((float(signal.metadata.get("memory_edge", 0.0) or 0.0) + 1.0) / 2.0)

        rr_score = _clip((float(signal.risk_reward or 0.0) - 1.0) / 2.0)

        spread_score = 0.7
        spread = float(context.get("spread", 0.0) or 0.0)
        entry = float(signal.entry_price or 0.0)
        spread_threshold = float(SPREAD_THRESHOLDS.get(category, 0.01) or 0.01)
        if spread > 0 and entry > 0 and spread_threshold > 0:
            spread_pct = spread / entry
            spread_score = _clip(1.0 - (spread_pct / spread_threshold))

        broker_score = self._broker_quality_score(signal)
        microstructure_score = self._microstructure_score(signal)
        cross_asset_score = self._cross_asset_score(signal)

        cat_open = sum(1 for p in open_positions if p.get("category") == category)
        same_dir_open = sum(
            1
            for p in open_positions
            if p.get("category") == category
            and str(p.get("direction") or p.get("signal") or "").upper() == direction
        )
        candidate_cluster = max(0, candidate_counts.get((category, direction), 0) - 1)
        soft_cap = int(CATEGORY_CAPS.get(category, 99))
        hard_cap = soft_cap + max(0, int(CATEGORY_CAP_SOFT_BUFFER))

        portfolio_fit = 1.0
        portfolio_fit -= min(0.30, cat_open * 0.10)
        portfolio_fit -= min(0.18, same_dir_open * 0.08)
        portfolio_fit -= min(0.18, candidate_cluster * 0.07)
        if cat_open >= soft_cap:
            portfolio_fit -= 0.10
        if cat_open >= hard_cap:
            portfolio_fit -= 0.18
        if same_dir_open >= PORTFOLIO_MAX_SAME_DIRECTION_POSITIONS:
            portfolio_fit -= 0.12
        portfolio_fit = _clip(portfolio_fit)

        breakdown = {
            "confidence": round(confidence_score, 4),
            "structure": round(structure_score, 4),
            "setup": round(setup_score, 4),
            "sentiment": round(sentiment_score, 4),
            "whales": round(whale_score, 4),
            "order_flow": round(orderflow_score, 4),
            "memory": round(memory_score, 4),
            "risk_reward": round(rr_score, 4),
            "spread": round(spread_score, 4),
            "broker_quality": round(broker_score, 4),
            "microstructure": round(microstructure_score, 4),
            "cross_asset": round(cross_asset_score, 4),
            "portfolio_fit": round(portfolio_fit, 4),
        }

        score = (
            confidence_score * 0.22
            + structure_score * 0.14
            + setup_score * 0.09
            + sentiment_score * 0.07
            + whale_score * 0.04
            + orderflow_score * 0.05
            + memory_score * 0.06
            + rr_score * 0.06
            + spread_score * 0.04
            + broker_score * 0.08
            + microstructure_score * 0.06
            + cross_asset_score * 0.05
            + portfolio_fit * 0.04
        )

        return round(_clip(score), 4), breakdown

    def rank(
        self,
        signal_ctx_pairs: Iterable[Tuple[Any, Dict[str, Any]]],
        state: Any,
    ) -> List[Tuple[Any, Dict[str, Any]]]:
        pairs = list(signal_ctx_pairs)
        if not pairs:
            return []

        open_positions = list(state.get_open_positions()) if state is not None else []
        candidate_counts: Dict[Tuple[str, str], int] = {}
        for signal, _ctx in pairs:
            key = (str(signal.category or ""), str(signal.direction or "BUY").upper())
            candidate_counts[key] = candidate_counts.get(key, 0) + 1

        scored: List[Tuple[float, Any, Dict[str, Any]]] = []
        for signal, context in pairs:
            score, breakdown = self._score_pair(signal, context, open_positions, candidate_counts)
            signal.metadata["opportunity_score"] = score
            signal.metadata["opportunity_breakdown"] = breakdown
            scored.append((score, signal, context))

        scored.sort(key=lambda item: item[0], reverse=True)

        ranked: List[Tuple[Any, Dict[str, Any]]] = []
        for index, (score, signal, context) in enumerate(scored, start=1):
            signal.metadata["opportunity_rank"] = index
            signal.metadata["opportunity_score"] = score
            ranked.append((signal, context))
        return ranked


_service = OpportunityRanker()


def get_service() -> OpportunityRanker:
    return _service
