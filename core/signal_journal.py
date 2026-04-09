from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


# ── Decision constants ────────────────────────────────────────────────────────
PASS    = "PASS"
KILLED  = "KILLED"
SKIPPED = "SKIPPED"
BOOSTED = "BOOSTED"
REDUCED = "REDUCED"
INFO    = "INFO"       # non-layer entries (backtest, phase data)

# ── Telegram emoji map ────────────────────────────────────────────────────────
_EMOJI = {
    PASS:    "✅",
    KILLED:  "❌",
    SKIPPED: "⏭",
    BOOSTED: "⬆",
    REDUCED: "⬇",
    INFO:    "📊",
}

_NARRATIVE_LABELS = {
    "AI_TOKENS": "AI-related crypto narrative",
    "ETF_NEWS": "ETF news flow",
    "MACRO_SHOCK": "macro shock theme",
    "DEFI_TREND": "DeFi trend",
    "REGULATION": "regulation theme",
    "LAYER2_TREND": "layer-2 trend",
    "BTC_DOMINANCE": "Bitcoin dominance theme",
    "EXCHANGE_NEWS": "exchange news flow",
    "STABLECOIN_NEWS": "stablecoin theme",
    "HALVING_BUZZ": "halving narrative",
}


def _safe_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        return float(value)
    except Exception:
        return default


def _safe_int(value: Any, default: Optional[int] = None) -> Optional[int]:
    try:
        return int(value)
    except Exception:
        return default


def _clip01(value: Any) -> float:
    num = _safe_float(value, 0.0) or 0.0
    return max(0.0, min(1.0, num))


def _clip11(value: Any) -> float:
    num = _safe_float(value, 0.0) or 0.0
    return max(-1.0, min(1.0, num))


def _signed_quality(value: Any) -> float:
    return round(_clip11((_clip01(value) - 0.5) * 2.0), 4)


def _direction_sign(direction: str) -> int:
    return 1 if str(direction or "").upper() == "BUY" else -1


@dataclass
class JournalEntry:
    """A single recorded decision from one layer or phase."""
    layer:       int            # 0 = pre-decision / post-decision
    name:        str            # decision step or phase name
    decision:    str            # PASS | KILLED | SKIPPED | BOOSTED | REDUCED | INFO
    reason:      str            # human-readable explanation
    conf_before: float          # confidence before this stage
    conf_after:  float          # confidence after this stage
    data:        Dict[str, Any] = field(default_factory=dict)
    elapsed_ms:  float          = 0.0
    ts:          float          = field(default_factory=time.time)

    @property
    def conf_delta(self) -> float:
        return round(self.conf_after - self.conf_before, 4)

    def emoji(self) -> str:
        return _EMOJI.get(self.decision, "•")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "layer":       self.layer,
            "name":        self.name,
            "decision":    self.decision,
            "reason":      self.reason,
            "conf_before": round(self.conf_before, 4),
            "conf_after":  round(self.conf_after,  4),
            "conf_delta":  self.conf_delta,
            "data":        self.data,
            "elapsed_ms":  round(self.elapsed_ms, 2),
            "ts":          self.ts,
        }


class SignalJournal:
    """
    Mutable log attached to a Signal. Every decision step writes one entry.
    Immutable once the signal is dead or executed.
    """

    def __init__(self, asset: str, direction: str) -> None:
        self.asset      = asset
        self.direction  = direction
        self.entries:   List[JournalEntry] = []
        self._start_ts  = time.time()

    # ── Public API ────────────────────────────────────────────────────────────

    def record(
        self,
        layer:       int,
        name:        str,
        decision:    str,
        reason:      str,
        conf_before: float,
        conf_after:  float,
        data:        Optional[Dict[str, Any]] = None,
        elapsed_ms:  float = 0.0,
    ) -> None:
        """Add one entry. Thread-safe — called from decision steps."""
        self.entries.append(JournalEntry(
            layer       = layer,
            name        = name,
            decision    = decision,
            reason      = reason,
            conf_before = conf_before,
            conf_after  = conf_after,
            data        = data or {},
            elapsed_ms  = elapsed_ms,
        ))

    def total_elapsed_ms(self) -> float:
        return round((time.time() - self._start_ts) * 1000, 1)

    def final_decision(self) -> str:
        """SURVIVED or KILLED"""
        # Allow manual debug override (e.g. DEBUG_FORCE_SURVIVE) to preserve
        # a surviving signal for announcement even if earlier stages recorded kills.
        for e in reversed(self.entries):
            if e.name == "debug_force" and e.decision == PASS:
                return "SURVIVED"
        for e in reversed(self.entries):
            if e.decision == KILLED:
                return "KILLED"
        return "SURVIVED"

    def kill_entry(self) -> Optional[JournalEntry]:
        for e in self.entries:
            if e.decision == KILLED:
                return e
        return None

    def _latest_entry(self, name: str) -> Optional[JournalEntry]:
        for e in reversed(self.entries):
            if e.name == name:
                return e
        return None

    def _latest_named(self, *names: str) -> Optional[JournalEntry]:
        wanted = {str(name) for name in names}
        for e in reversed(self.entries):
            if e.name in wanted:
                return e
        return None

    def _latest_layer_entry(self) -> Optional[JournalEntry]:
        for e in reversed(self.entries):
            if e.layer > 0:
                return e
        return None

    def _signal_context(self, signal=None) -> Dict[str, Any]:
        metadata = dict(getattr(signal, "metadata", {}) or {})
        market = self._latest_entry("market")
        intelligence = self._latest_entry("intelligence")
        execution = self._latest_entry("execution")
        policy = self._latest_named("policy", "agent")
        governance = self._latest_named("governance", "data_integrity")
        memory = self._latest_entry("memory")

        structure_data = metadata.get("market_structure")
        if not isinstance(structure_data, dict):
            structure_data = {}
        if not structure_data and market and isinstance(market.data.get("market_structure"), dict):
            structure_data = dict(market.data.get("market_structure") or {})

        broker_quality = metadata.get("broker_quality")
        if not isinstance(broker_quality, dict):
            broker_quality = {}
        market_microstructure = metadata.get("market_microstructure")
        if not isinstance(market_microstructure, dict):
            market_microstructure = {}
        breakdown = metadata.get("opportunity_breakdown")
        if not isinstance(breakdown, dict):
            breakdown = {}

        structure_bias = str(
            metadata.get("structure_bias")
            or structure_data.get("structure_bias")
            or "neutral"
        ).lower()
        alignment_score = _safe_float(
            metadata.get("alignment_score", structure_data.get("alignment_score")),
            0.0,
        ) or 0.0
        setup_quality = _safe_float(
            metadata.get("setup_quality", structure_data.get("setup_quality")),
            0.0,
        ) or 0.0
        pullback_score = _safe_float(
            metadata.get("pullback_score", structure_data.get("pullback_score")),
            0.0,
        ) or 0.0
        breakout_score = _safe_float(
            metadata.get("breakout_score", structure_data.get("breakout_score")),
            0.0,
        ) or 0.0
        setup_signal = breakout_score if abs(breakout_score) >= abs(pullback_score) else pullback_score
        ml_confidence = _safe_float(metadata.get("ml_confidence"), 0.0) or 0.0
        sentiment_score = _safe_float(
            metadata.get(
                "sentiment_score",
                (intelligence.data or {}).get("sentiment_score") if intelligence else 0.0,
            ),
            0.0,
        ) or 0.0

        whale_dominant = str(
            metadata.get("whale_dominant")
            or ((intelligence.data or {}).get("whale_dominant") if intelligence else "")
            or ""
        ).upper()
        whale_ratio = _safe_float(
            metadata.get(
                "whale_ratio",
                (intelligence.data or {}).get("whale_ratio") if intelligence else None,
            ),
            None,
        )
        whale_ratio = whale_ratio if whale_ratio is not None else max(
            _safe_float(metadata.get("whale_bull_weight"), 0.0) or 0.0,
            _safe_float(metadata.get("whale_bear_weight"), 0.0) or 0.0,
        )
        whale_ratio = max(0.0, min(1.0, whale_ratio or 0.0))
        orderflow_imbalance = _safe_float(
            metadata.get(
                "orderflow_imbalance",
                (market.data or {}).get("orderflow_imbalance") if market else 0.0,
            ),
            0.0,
        ) or 0.0
        return {
            "metadata": metadata,
            "market": market,
            "intelligence": intelligence,
            "execution": execution,
            "policy": policy,
            "governance": governance,
            "memory": memory,
            "structure_data": structure_data,
            "broker_quality": broker_quality,
            "market_microstructure": market_microstructure,
            "breakdown": breakdown,
            "sign": _direction_sign(self.direction),
            "structure_bias": structure_bias,
            "alignment_score": alignment_score,
            "setup_quality": setup_quality,
            "pullback_score": pullback_score,
            "breakout_score": breakout_score,
            "setup_signal": setup_signal,
            "ml_confidence": ml_confidence,
            "sentiment_score": sentiment_score,
            "whale_dominant": whale_dominant,
            "whale_ratio": whale_ratio,
            "orderflow_imbalance": orderflow_imbalance,
        }

    @staticmethod
    def _factor_market_structure(ctx: Dict[str, Any]) -> float:
        bias_factor = 0.0
        if ctx["structure_bias"] == "buy":
            bias_factor = 1.0 if ctx["sign"] > 0 else -1.0
        elif ctx["structure_bias"] == "sell":
            bias_factor = 1.0 if ctx["sign"] < 0 else -1.0
        setup_factor = _clip11(ctx["setup_signal"] * ctx["sign"])
        market_structure = bias_factor * ctx["alignment_score"] * 0.7 + setup_factor * 0.3
        return round(_clip11(market_structure), 4)

    @staticmethod
    def _factor_ml(ctx: Dict[str, Any]) -> float:
        ml_prediction = _safe_float(ctx["metadata"].get("ml_prediction"), None)
        ml_direction = 0.0
        if ml_prediction is not None:
            if ml_prediction > 0.5:
                ml_direction = 1.0
            elif ml_prediction < 0.5:
                ml_direction = -1.0
        return round(_clip11(ml_direction * ctx["sign"] * min(1.0, ctx["ml_confidence"])), 4)

    @staticmethod
    def _factor_sentiment(ctx: Dict[str, Any]) -> float:
        return round(_clip11(ctx["sentiment_score"] * ctx["sign"]), 4)

    @staticmethod
    def _factor_whales(ctx: Dict[str, Any]) -> float:
        if ctx["whale_dominant"] in {"BUY", "SELL"}:
            whale_sign = 1.0 if ctx["whale_dominant"] == "BUY" else -1.0
            return round(_clip11(whale_sign * ctx["sign"] * ctx["whale_ratio"]), 4)
        return 0.0

    @staticmethod
    def _factor_order_flow(ctx: Dict[str, Any]) -> float:
        if ctx["metadata"].get("orderflow_applicable") is False:
            return 0.0
        return round(_clip11(ctx["orderflow_imbalance"] * ctx["sign"]), 4)

    @staticmethod
    def _factor_risk(ctx: Dict[str, Any]) -> float:
        breakdown = ctx["breakdown"]
        market = ctx["market"]
        execution = ctx["execution"]
        risk_components: List[float] = []
        if breakdown:
            for key in ("risk_reward", "spread", "portfolio_fit"):
                if key in breakdown:
                    risk_components.append(_signed_quality(breakdown.get(key)))
        else:
            rr = _safe_float((market.data or {}).get("rr") if market else None, None)
            if rr is not None:
                risk_components.append(_clip11((rr - 1.5) / 1.5))
            spread_pct = _safe_float((market.data or {}).get("spread_pct") if market else None, None)
            if spread_pct is not None:
                risk_components.append(_clip11(1.0 - min(2.0, spread_pct / 0.005)))
            liq_penalty = _safe_float((execution.data or {}).get("liq_penalty") if execution else None, None)
            if liq_penalty is not None:
                risk_components.append(_clip11(1.0 - min(1.5, liq_penalty / 0.05)))
        return round(sum(risk_components) / len(risk_components), 4) if risk_components else 0.0

    @staticmethod
    def _factor_broker_quality(ctx: Dict[str, Any]) -> float:
        breakdown = ctx["breakdown"]
        if "broker_quality" in breakdown:
            return _signed_quality(breakdown.get("broker_quality"))
        broker_score = _safe_float(
            ctx["metadata"].get("broker_quality_score", ctx["broker_quality"].get("score")),
            None,
        )
        if broker_score is not None:
            return _signed_quality(broker_score)
        return 0.0

    @staticmethod
    def _factor_microstructure(ctx: Dict[str, Any]) -> float:
        micro_alignment = _safe_float(ctx["metadata"].get("microstructure_alignment"), None)
        if micro_alignment is not None:
            return round(_clip11(micro_alignment), 4)
        if "microstructure" in ctx["breakdown"]:
            return _signed_quality(ctx["breakdown"].get("microstructure"))
        micro_score = _safe_float(
            ctx["metadata"].get("microstructure_score", ctx["market_microstructure"].get("score")),
            None,
        )
        if micro_score is not None:
            return round(_clip11(micro_score * ctx["sign"]), 4)
        return 0.0

    @staticmethod
    def _factor_cross_asset(ctx: Dict[str, Any]) -> float:
        cross_alignment = _safe_float(ctx["metadata"].get("cross_asset_alignment"), None)
        if cross_alignment is not None:
            return round(_clip11(cross_alignment), 4)
        if "cross_asset" in ctx["breakdown"]:
            return _signed_quality(ctx["breakdown"].get("cross_asset"))
        return 0.0

    @staticmethod
    def _factor_policy(ctx: Dict[str, Any]) -> float:
        agent_score = _safe_float(
            ctx["metadata"].get("agent_score", (ctx["policy"].data or {}).get("agent_score") if ctx["policy"] else None),
            None,
        )
        return _signed_quality(agent_score) if agent_score is not None else 0.0

    @staticmethod
    def _factor_governance(ctx: Dict[str, Any]) -> float:
        governance_score = _safe_float(
            ctx["metadata"].get(
                "governance_score",
                (ctx["governance"].data or {}).get("score") if ctx["governance"] else None,
            ),
            None,
        )
        if governance_score is None:
            return 0.0
        governance_factor = _clip11((governance_score - 50.0) / 50.0)
        if ctx["governance"] and ctx["governance"].decision == KILLED:
            governance_factor = min(governance_factor, -0.3)
        return round(governance_factor, 4)

    @staticmethod
    def _factor_memory(ctx: Dict[str, Any]) -> float:
        memory_edge = _safe_float(
            ctx["metadata"].get(
                "memory_edge",
                (ctx["memory"].data or {}).get("memory_edge") if ctx["memory"] else None,
            ),
            None,
        )
        return round(_clip11(memory_edge), 4) if memory_edge is not None else 0.0

    def _extract_factor_attribution(self, signal=None) -> Dict[str, float]:
        ctx = self._signal_context(signal)
        return {
            "market_structure": self._factor_market_structure(ctx),
            "ml": self._factor_ml(ctx),
            "sentiment": self._factor_sentiment(ctx),
            "whales": self._factor_whales(ctx),
            "order_flow": self._factor_order_flow(ctx),
            "broker_quality": self._factor_broker_quality(ctx),
            "microstructure": self._factor_microstructure(ctx),
            "cross_asset": self._factor_cross_asset(ctx),
            "memory": self._factor_memory(ctx),
            "policy": self._factor_policy(ctx),
            "governance": self._factor_governance(ctx),
            "risk": self._factor_risk(ctx),
        }

    @staticmethod
    def _setup_style(ctx: Dict[str, Any]) -> str:
        pullback_score = ctx["pullback_score"]
        breakout_score = ctx["breakout_score"]
        if abs(breakout_score) >= abs(pullback_score) and abs(breakout_score) >= 0.2:
            return "breakout"
        if abs(pullback_score) >= 0.2:
            return "pullback"
        return "mixed"

    @staticmethod
    def _setup_sentiment_bucket(ctx: Dict[str, Any]) -> str:
        if ctx["sentiment_score"] >= 0.2:
            return "bullish"
        if ctx["sentiment_score"] <= -0.2:
            return "bearish"
        return "neutral"

    @staticmethod
    def _setup_whale_bucket(ctx: Dict[str, Any]) -> str:
        if ctx["whale_dominant"] in {"BUY", "SELL"} and ctx["whale_ratio"] >= 0.55:
            return ctx["whale_dominant"].lower()
        return "neutral"

    @staticmethod
    def _setup_orderflow_bucket(ctx: Dict[str, Any]) -> str:
        if ctx["orderflow_imbalance"] >= 0.2:
            return "buy_pressure"
        if ctx["orderflow_imbalance"] <= -0.2:
            return "sell_pressure"
        return "balanced"

    @staticmethod
    def _setup_regime(ctx: Dict[str, Any]) -> str:
        regime = str(ctx["metadata"].get("regime") or "")
        if not regime and ctx["market"]:
            regime = str((ctx["market"].data or {}).get("regime") or "")
        return regime

    @staticmethod
    def _setup_session(ctx: Dict[str, Any]) -> str:
        session = str(ctx["metadata"].get("session") or "")
        if not session and ctx["market"]:
            session = str((ctx["market"].data or {}).get("session") or "")
        return session

    @staticmethod
    def _setup_depth_mode(ctx: Dict[str, Any]) -> str:
        if bool(ctx["metadata"].get("depth_available", ctx["market_microstructure"].get("depth_available"))):
            return "true_depth"
        if bool(ctx["metadata"].get("synthetic_depth_available", ctx["market_microstructure"].get("synthetic_depth_available"))):
            return "synthetic_depth"
        return "top_of_book"

    def _extract_setup_fingerprint(self, signal=None) -> Dict[str, Any]:
        ctx = self._signal_context(signal)

        return {
            "regime": self._setup_regime(ctx),
            "structure_bias": str(
                ctx["metadata"].get("structure_bias")
                or ctx["structure_data"].get("structure_bias")
                or "neutral"
            ).lower(),
            "alignment_score": round(
                _safe_float(ctx["metadata"].get("alignment_score", ctx["structure_data"].get("alignment_score")), 0.0) or 0.0,
                4,
            ),
            "setup_quality": round(
                _safe_float(ctx["metadata"].get("setup_quality", ctx["structure_data"].get("setup_quality")), 0.0) or 0.0,
                4,
            ),
            "volatility_state": str(
                ctx["metadata"].get("volatility_state")
                or ctx["structure_data"].get("volatility_state")
                or "unknown"
            ),
            "setup_style": self._setup_style(ctx),
            "sentiment_bucket": self._setup_sentiment_bucket(ctx),
            "whale_bucket": self._setup_whale_bucket(ctx),
            "orderflow_bucket": self._setup_orderflow_bucket(ctx),
            "session": self._setup_session(ctx),
            "primary_provider": str(ctx["broker_quality"].get("primary_provider", "") or ""),
            "comparison_provider": str(ctx["broker_quality"].get("comparison_provider", "") or ""),
            "broker_agreement_state": str(
                ctx["metadata"].get("broker_agreement_state", ctx["broker_quality"].get("quote_agreement_state", "")) or ""
            ),
            "quote_quality_state": str(
                ctx["metadata"].get("broker_quote_quality_state", ctx["broker_quality"].get("quote_quality_state", "")) or ""
            ),
            "spread_regime": str(
                ctx["metadata"].get("broker_spread_regime", ctx["broker_quality"].get("spread_regime", "")) or ""
            ),
            "depth_mode": self._setup_depth_mode(ctx),
            "microstructure_source": str(
                ctx["metadata"].get("microstructure_source", ctx["market_microstructure"].get("microstructure_source", "")) or ""
            ),
            "microstructure_pressure": str(
                ctx["market_microstructure"].get("pressure_direction", ctx["metadata"].get("micro_pressure_direction", "")) or ""
            ).upper(),
        }

    @staticmethod
    def _factor_extremes(factors: Dict[str, float]) -> Dict[str, Any]:
        non_zero = {name: value for name, value in factors.items() if abs(float(value or 0.0)) >= 0.05}
        if not non_zero:
            return {
                "top_positive_factor": "",
                "top_positive_factor_value": None,
                "top_negative_factor": "",
                "top_negative_factor_value": None,
            }
        top_positive = max(non_zero.items(), key=lambda item: item[1])
        top_negative = min(non_zero.items(), key=lambda item: item[1])
        positive_name, positive_value = top_positive if top_positive[1] > 0 else ("", None)
        negative_name, negative_value = top_negative if top_negative[1] < 0 else ("", None)
        return {
            "top_positive_factor": positive_name,
            "top_positive_factor_value": round(float(positive_value), 4) if positive_value is not None else None,
            "top_negative_factor": negative_name,
            "top_negative_factor_value": round(float(negative_value), 4) if negative_value is not None else None,
        }

    def summary(self, signal=None) -> Dict[str, Any]:
        kill = self.kill_entry()
        governance = self._latest_named("governance", "data_integrity")
        policy = self._latest_named("policy", "agent")
        latest = self._latest_layer_entry()
        metadata = dict(getattr(signal, "metadata", {}) or {})

        final_conf = None
        if policy and policy.data.get("final_confidence") is not None:
            final_conf = round(_safe_float(policy.data.get("final_confidence"), 0.0) or 0.0, 4)
        elif signal is not None and getattr(signal, "confidence", None) is not None:
            final_conf = round(_safe_float(getattr(signal, "confidence"), 0.0) or 0.0, 4)
        elif latest is not None:
            final_conf = round(float(latest.conf_after), 4)

        final_score = None
        if policy and policy.data.get("agent_score") is not None:
            final_score = round(_safe_float(policy.data.get("agent_score"), 0.0) or 0.0, 4)
        elif metadata.get("agent_score") is not None:
            final_score = round(_safe_float(metadata.get("agent_score"), 0.0) or 0.0, 4)

        valid_sources = None
        min_required = None
        if governance:
            valid_sources = _safe_int(governance.data.get("valid_sources"), None)
            min_required = _safe_int(governance.data.get("min_required"), None)

        opportunity_score = _safe_float(metadata.get("opportunity_score"), None)
        if opportunity_score is not None:
            opportunity_score = round(opportunity_score, 4)
        opportunity_rank = _safe_int(metadata.get("opportunity_rank"), None)
        opportunity_breakdown = metadata.get("opportunity_breakdown")
        if not isinstance(opportunity_breakdown, dict):
            opportunity_breakdown = {}
        else:
            opportunity_breakdown = {
                str(k): round(_safe_float(v, 0.0) or 0.0, 4)
                for k, v in opportunity_breakdown.items()
            }

        factor_attribution = self._extract_factor_attribution(signal)
        factor_extremes = self._factor_extremes(factor_attribution)
        setup_fingerprint = self._extract_setup_fingerprint(signal)

        governance_score = _safe_int(
            metadata.get(
                "governance_score",
                governance.data.get("score") if governance else None,
            ),
            None,
        )
        governance_grade = str(
            metadata.get(
                "governance_grade",
                governance.data.get("grade") if governance else "",
            )
            or ""
        )
        memory_entry = self._latest_entry("memory")
        memory_score = _safe_float(
            metadata.get("memory_score", (memory_entry.data or {}).get("memory_score") if memory_entry else None),
            None,
        )
        memory_edge = _safe_float(
            metadata.get("memory_edge", (memory_entry.data or {}).get("memory_edge") if memory_entry else None),
            None,
        )
        memory_sample_count = _safe_int(
            metadata.get("memory_sample_count", (memory_entry.data or {}).get("memory_sample_count") if memory_entry else None),
            None,
        )
        broker_quality = metadata.get("broker_quality")
        if not isinstance(broker_quality, dict):
            broker_quality = {}
        market_microstructure = metadata.get("market_microstructure")
        if not isinstance(market_microstructure, dict):
            market_microstructure = {}
        broker_quality_score = _safe_float(
            metadata.get("broker_quality_score", broker_quality.get("score")),
            None,
        )
        microstructure_score = _safe_float(
            metadata.get("microstructure_score", market_microstructure.get("score")),
            None,
        )
        stop_hunt_risk = _safe_float(
            metadata.get("stop_hunt_risk", market_microstructure.get("stop_hunt_risk")),
            None,
        )
        exhaustion_risk = _safe_float(
            metadata.get("exhaustion_risk", market_microstructure.get("exhaustion_risk")),
            None,
        )

        return {
            "final_policy_decision": policy.decision if policy else "",
            "final_policy_reason": policy.reason if policy else "",
            "final_policy_score": final_score,
            "final_confidence": final_conf,
            "real_sources_valid": valid_sources,
            "real_sources_required": min_required,
            "killed_by": kill.name if kill else "",
            "kill_reason": kill.reason if kill else "",
            "last_layer": latest.name if latest else "",
            "opportunity_score": opportunity_score,
            "opportunity_rank": opportunity_rank,
            "opportunity_breakdown": opportunity_breakdown,
            "factor_attribution": factor_attribution,
            "setup_fingerprint": setup_fingerprint,
            "structure_bias": setup_fingerprint.get("structure_bias", ""),
            "alignment_score": setup_fingerprint.get("alignment_score"),
            "setup_quality": setup_fingerprint.get("setup_quality"),
            "regime": setup_fingerprint.get("regime", ""),
            "volatility_state": setup_fingerprint.get("volatility_state", ""),
            "broker_quality_score": round(broker_quality_score, 4) if broker_quality_score is not None else None,
            "broker_primary_provider": setup_fingerprint.get("primary_provider", ""),
            "broker_comparison_provider": setup_fingerprint.get("comparison_provider", ""),
            "broker_agreement_state": setup_fingerprint.get("broker_agreement_state", ""),
            "broker_quote_quality_state": setup_fingerprint.get("quote_quality_state", ""),
            "broker_spread_regime": setup_fingerprint.get("spread_regime", ""),
            "microstructure_score": round(microstructure_score, 4) if microstructure_score is not None else None,
            "microstructure_pressure": setup_fingerprint.get("microstructure_pressure", ""),
            "depth_mode": setup_fingerprint.get("depth_mode", "top_of_book"),
            "microstructure_source": setup_fingerprint.get("microstructure_source", ""),
            "stop_hunt_risk": round(stop_hunt_risk, 4) if stop_hunt_risk is not None else None,
            "exhaustion_risk": round(exhaustion_risk, 4) if exhaustion_risk is not None else None,
            "governance_score": governance_score,
            "governance_grade": governance_grade,
            "memory_score": round(memory_score, 1) if memory_score is not None else None,
            "memory_edge": round(memory_edge, 4) if memory_edge is not None else None,
            "memory_sample_count": memory_sample_count,
            **factor_extremes,
        }

    def to_list(self) -> List[Dict]:
        return [e.to_dict() for e in self.entries]

    # ── Telegram formatting ───────────────────────────────────────────────────

    def _escape_markdown(self, text: str) -> str:
        if not isinstance(text, str):
            return str(text)
        return (text.replace("\\", "\\\\")
                    .replace("_", "\\_")
                    .replace("*", "\\*")
                    .replace("`", "\\`")
                    .replace("[", "\\[")
                    .replace("]", "\\]"))

    @staticmethod
    def _humanize_token(value: Any) -> str:
        text = str(value or "").strip()
        if not text:
            return ""
        text = text.replace("_", " ").replace("-", " ")
        return " ".join(text.split()).lower()

    @staticmethod
    def _humanize_reason(value: Any) -> str:
        text = str(value or "").strip()
        if not text:
            return ""
        text = text.replace("_", " ").replace("—", "-")
        return " ".join(text.split())

    @staticmethod
    def _sentence(text: str) -> str:
        cleaned = str(text or "").strip()
        if not cleaned:
            return ""
        cleaned = cleaned.rstrip(".")
        return cleaned[0].upper() + cleaned[1:]

    @staticmethod
    def _join_clauses(parts: List[str]) -> str:
        cleaned = [str(part).strip() for part in parts if str(part or "").strip()]
        if not cleaned:
            return ""
        if len(cleaned) == 1:
            return cleaned[0]
        if len(cleaned) == 2:
            return f"{cleaned[0]} and {cleaned[1]}"
        return f"{', '.join(cleaned[:-1])}, and {cleaned[-1]}"

    @staticmethod
    def _describe_sentiment(score: Any) -> str:
        value = _safe_float(score, 0.0) or 0.0
        magnitude = abs(value)
        if magnitude < 0.05:
            return "neutral"
        direction = "bullish" if value > 0 else "bearish"
        if magnitude < 0.20:
            return f"slightly {direction}"
        if magnitude < 0.50:
            return direction
        return f"strongly {direction}"

    @staticmethod
    def _format_pct(value: Any, digits: int = 0) -> str:
        num = _safe_float(value, None)
        if num is None:
            return ""
        return f"{num * 100:.{digits}f}%"

    @staticmethod
    def _format_price(value: Any) -> str:
        num = _safe_float(value, None)
        if num is None:
            return ""
        return f"{num:,.5f}".rstrip("0").rstrip(".")

    @staticmethod
    def _factor_label(name: str) -> str:
        labels = {
            "market_structure": "market structure",
            "ml": "model conviction",
            "sentiment": "sentiment",
            "whales": "whale activity",
            "order_flow": "order flow",
            "broker_quality": "broker quality",
            "microstructure": "microstructure",
            "cross_asset": "cross-asset confirmation",
            "memory": "historical setup memory",
            "policy": "policy review",
            "governance": "governance checks",
            "risk": "execution quality",
        }
        key = str(name or "").strip()
        return labels.get(key, SignalJournal._humanize_token(key))

    @staticmethod
    def _narrative_label(value: Any) -> str:
        raw = str(value or "").strip()
        if not raw:
            return ""
        return _NARRATIVE_LABELS.get(raw, SignalJournal._humanize_token(raw).title())

    @staticmethod
    def _execution_snapshot(signal: Any) -> Dict[str, Any]:
        if signal is None:
            return {
                "entry_price": 0.0,
                "stop_loss": 0.0,
                "take_profit": 0.0,
                "confidence": 0.0,
                "position_size": 0.0,
                "risk_reward": 0.0,
                "first_target": 0.0,
                "runner_target": 0.0,
                "first_rr": 0.0,
                "runner_rr": 0.0,
            }

        entry_price = float(getattr(signal, "entry_price", 0) or 0)
        stop_loss = float(getattr(signal, "stop_loss", 0) or 0)
        take_profit = float(getattr(signal, "take_profit", 0) or 0)
        confidence = float(getattr(signal, "confidence", 0) or 0)
        position_size = float(getattr(signal, "position_size", 0) or 0)
        risk_reward = float(getattr(signal, "risk_reward", 0) or 0)
        tp_levels: List[float] = []
        for raw_level in list(getattr(signal, "take_profit_levels", []) or []):
            try:
                level = float(raw_level)
            except Exception:
                continue
            if level > 0:
                tp_levels.append(level)
        first_target = float(tp_levels[0]) if tp_levels else take_profit
        runner_target = float(tp_levels[-1]) if len(tp_levels) > 1 else 0.0
        risk = abs(entry_price - stop_loss)
        first_rr = abs(first_target - entry_price) / risk if risk > 0 and first_target else risk_reward
        runner_rr = abs(runner_target - entry_price) / risk if risk > 0 and runner_target else risk_reward
        return {
            "entry_price": entry_price,
            "stop_loss": stop_loss,
            "take_profit": take_profit,
            "confidence": confidence,
            "position_size": position_size,
            "risk_reward": risk_reward,
            "first_target": first_target,
            "runner_target": runner_target,
            "first_rr": first_rr,
            "runner_rr": runner_rr,
        }

    def _telegram_plain_market_line(self, entry: JournalEntry, data: Dict[str, Any], summary: Dict[str, Any]) -> str:
        clauses: List[str] = []
        regime = self._humanize_token(data.get("regime") or summary.get("regime"))
        if regime:
            clauses.append(f"trend is {regime}")
        ml_direction = str(data.get("ml_direction") or "").upper()
        if ml_direction:
            clauses.append(f"the model also points {ml_direction.lower()}")
        rr = _safe_float(data.get("rr"), None)
        if rr is not None and rr > 0:
            clauses.append(f"reward to risk is {rr:.2f}:1")
        session = self._humanize_token(data.get("session"))
        if session:
            clauses.append(f"the setup showed up during the {session.title()} session")
        news_state = self._humanize_token(data.get("news_state"))
        if news_state:
            if news_state == "clear":
                clauses.append("there is no major news pressure right now")
            else:
                clauses.append(f"news is {news_state}")
        agreement_state = str(summary.get("broker_agreement_state") or "").lower()
        primary_provider = str(summary.get("broker_primary_provider") or "").strip()
        comparison_provider = str(summary.get("broker_comparison_provider") or "").strip()
        if primary_provider and comparison_provider and agreement_state:
            if agreement_state in {"strong", "aligned"}:
                clauses.append(f"{primary_provider} and {comparison_provider} are aligned")
            elif agreement_state == "divergent":
                clauses.append(f"{primary_provider} and {comparison_provider} are showing some price divergence")
            elif agreement_state == "severe_divergence":
                clauses.append(f"{primary_provider} and {comparison_provider} are materially diverging")
        quote_quality_state = self._humanize_token(summary.get("broker_quote_quality_state"))
        if quote_quality_state:
            clauses.append(f"quote quality is {quote_quality_state}")
        spread_regime = self._humanize_token(summary.get("broker_spread_regime"))
        if spread_regime:
            clauses.append(f"spread regime is {spread_regime}")
        sentence = self._join_clauses(clauses) or self._humanize_reason(entry.reason) or "market conditions look tradable"
        return f"- Market view: {self._sentence(sentence)}."

    def _telegram_plain_intelligence_line(self, entry: JournalEntry, data: Dict[str, Any]) -> str:
        clauses: List[str] = []
        sentiment_desc = self._describe_sentiment(data.get("sentiment_score"))
        if sentiment_desc == "neutral":
            clauses.append("sentiment is broadly neutral")
        else:
            clauses.append(f"sentiment is {sentiment_desc}")
        ig_client_sentiment = data.get("ig_client_sentiment")
        if isinstance(ig_client_sentiment, dict):
            bias = str(ig_client_sentiment.get("bias") or "").upper()
            long_pct = _safe_float(ig_client_sentiment.get("long_pct"), None)
            short_pct = _safe_float(ig_client_sentiment.get("short_pct"), None)
            if bias in {"BUY", "SELL"} and long_pct is not None and short_pct is not None:
                clauses.append(
                    f"IG client positioning is {long_pct:.0f}% long versus {short_pct:.0f}% short, leaning {bias.lower()}"
                )
        whale_dominant = str(data.get("whale_dominant") or "").upper()
        if whale_dominant in {"BUY", "SELL"}:
            clauses.append(f"whale flow leans {whale_dominant.lower()}")
        source_count = len(data.get("sentiment_sources") or [])
        if source_count:
            clauses.append(f"this view is backed by {source_count} sources")
        narrative = self._narrative_label(data.get("narrative"))
        if narrative:
            clauses.append(f"the main narrative is {narrative}")
        cross_asset_alignment = _safe_float(data.get("cross_asset_alignment"), None)
        cross_asset_peer = str(data.get("cross_asset_primary_peer") or "").strip()
        cross_asset_relation = self._humanize_token(data.get("cross_asset_primary_relation"))
        if cross_asset_alignment is not None and cross_asset_peer:
            if cross_asset_alignment >= 0.22:
                clauses.append(
                    f"{cross_asset_peer} is confirming the trade"
                    f"{f' through {cross_asset_relation}' if cross_asset_relation else ''}"
                )
            elif cross_asset_alignment <= -0.22:
                clauses.append(
                    f"{cross_asset_peer} is conflicting with the trade"
                    f"{f' through {cross_asset_relation}' if cross_asset_relation else ''}"
                )
        sentence = self._join_clauses(clauses) or self._humanize_reason(entry.reason) or "intelligence checks were supportive"
        return f"- Flow and sentiment: {self._sentence(sentence)}."

    def _telegram_plain_memory_line(self, entry: JournalEntry, data: Dict[str, Any]) -> str:
        clauses: List[str] = []
        win_rate = _safe_float(data.get("memory_win_rate"), None)
        sample_count = _safe_int(data.get("memory_sample_count"), None)
        if win_rate is not None and sample_count:
            clauses.append(f"similar setups won {win_rate * 100:.1f}% of the time across {sample_count} examples")
        else:
            memory_score = _safe_float(data.get("memory_score"), None)
            if memory_score is not None:
                clauses.append(f"similar setup memory scored {memory_score:.1f} out of 100")
        memory_edge = _safe_float(data.get("memory_edge"), None)
        if memory_edge is not None:
            if memory_edge > 0.05:
                clauses.append("historical edge is positive")
            elif memory_edge < -0.05:
                clauses.append("historical edge is negative")
        sentence = self._join_clauses(clauses) or self._humanize_reason(entry.reason) or "historical memory was supportive"
        return f"- Historical context: {self._sentence(sentence)}."

    def _telegram_plain_meta_ai_line(self, entry: JournalEntry, data: Dict[str, Any], summary: Dict[str, Any]) -> str:
        clauses: List[str] = []
        regime = self._humanize_token(data.get("regime") or summary.get("regime"))
        if regime:
            clauses.append(f"the broader regime is {regime}")
        ensemble = _safe_float(data.get("ensemble"), None)
        if ensemble is not None:
            if ensemble >= 0.67:
                clauses.append("the ensemble view supports the trade")
            elif ensemble <= 0.33:
                clauses.append("the ensemble view leans against the trade")
            else:
                clauses.append("the ensemble view is neutral")
        reason = self._humanize_reason(entry.reason).lower()
        if "no adjustment" in reason:
            clauses.append("it did not change conviction")
        elif "support" in reason:
            clauses.append("it added a small supportive bias")
        elif "conflict" in reason:
            clauses.append("it flagged some conflict")
        sentence = self._join_clauses(clauses) or self._humanize_reason(entry.reason) or "meta model review was neutral"
        return f"- Broader AI view: {self._sentence(sentence)}."

    def _telegram_plain_policy_line(self, entry: JournalEntry, data: Dict[str, Any], direction_word: str) -> str:
        clauses: List[str] = []
        policy_status = self._humanize_token(data.get("agent_policy_status") or "ok")
        if entry.decision == PASS:
            if policy_status == "ok":
                clauses.append(f"the policy model approved the {direction_word} setup")
            else:
                clauses.append(f"the policy model was treated as advisory ({policy_status})")
        elif entry.decision == KILLED:
            clauses.append(f"the policy model rejected the {direction_word} setup")
        directional_edge = _safe_float(data.get("agent_directional_edge"), None)
        if directional_edge is not None:
            if directional_edge >= 0.65:
                clauses.append("directional edge was strong")
            elif directional_edge <= 0.35:
                clauses.append("directional edge was weak")
        sentence = self._join_clauses(clauses) or self._humanize_reason(entry.reason) or "policy review completed"
        return f"- Policy check: {self._sentence(sentence)}."

    def _telegram_plain_governance_line(self, entry: JournalEntry, data: Dict[str, Any], summary: Dict[str, Any]) -> str:
        clauses = ["data quality and live checks passed" if entry.decision == PASS else "governance checks blocked the setup"]
        grade = str(data.get("grade") or summary.get("governance_grade") or "").strip()
        if grade:
            clauses.append(f"grade {grade}")
        valid_sources = _safe_int(data.get("valid_sources"), None)
        min_required = _safe_int(data.get("min_required"), None)
        if valid_sources is not None and min_required is not None:
            clauses.append(f"{valid_sources} sources cleared the minimum of {min_required}")
        sentence = self._join_clauses(clauses) or self._humanize_reason(entry.reason) or "governance review completed"
        return f"- Safety checks: {self._sentence(sentence)}."

    def _telegram_plain_execution_line(self, entry: JournalEntry, data: Dict[str, Any], summary: Dict[str, Any], signal: Any) -> str:
        clauses: List[str] = []
        if entry.decision == PASS:
            clauses.append("the setup stayed above the live execution floor")
        else:
            clauses.append(self._humanize_reason(entry.reason) or "execution rules blocked the trade")
        depth_mode = str(summary.get("depth_mode") or "").lower()
        if depth_mode == "true_depth":
            clauses.append("true order-book depth is available")
        elif depth_mode == "synthetic_depth":
            clauses.append("microstructure is using a synthetic depth proxy")
        elif summary.get("microstructure_source"):
            clauses.append("microstructure is running on top-of-book quotes only")
        pressure = str(summary.get("microstructure_pressure") or "").upper()
        if pressure in {"BUY", "SELL"}:
            if pressure == str(self.direction or "").upper():
                clauses.append(f"microstructure pressure still leans {pressure.lower()}")
            else:
                clauses.append(f"microstructure pressure leans {pressure.lower()}, so the tape is not fully aligned")
        stop_hunt_risk = _safe_float(summary.get("stop_hunt_risk"), None)
        if stop_hunt_risk is not None and stop_hunt_risk >= 0.45:
            clauses.append("stop-hunt risk is elevated")
        exhaustion_risk = _safe_float(summary.get("exhaustion_risk"), None)
        if exhaustion_risk is not None and exhaustion_risk >= 0.42:
            clauses.append("exhaustion risk is elevated")
        position_size = _safe_float(data.get("position_size"), _safe_float(getattr(signal, "position_size", 0.0), None))
        if position_size is not None and position_size > 0:
            clauses.append(f"position size is {position_size:.4f}")
        tp_levels = len(getattr(signal, "take_profit_levels", []) or [])
        if tp_levels:
            clauses.append(f"{tp_levels} take profit levels are set")
        notes = [self._humanize_token(note) for note in (data.get("notes") or [])]
        if "balance drawdown" in notes:
            clauses.append("sizing was kept conservative because the account is in drawdown")
        sentence = self._join_clauses(clauses) or self._humanize_reason(entry.reason) or "execution review completed"
        return f"- Execution posture: {self._sentence(sentence)}."

    def _telegram_plain_research_line(self, entry: JournalEntry, data: Dict[str, Any]) -> str:
        clauses: List[str] = []
        research_approved = data.get("research_approved")
        if research_approved is True:
            clauses.append("the active model is approved for live use")
        elif research_approved is False:
            clauses.append("the active model is not yet approved for full live use")
        model_key = self._humanize_token(data.get("model_key"))
        if model_key:
            clauses.append(f"it is using the {model_key} model")
        metrics: List[str] = []
        walk_forward = _safe_float(data.get("walk_forward_accuracy"), None)
        if walk_forward is not None:
            metrics.append(f"walk forward {walk_forward * 100:.1f}%")
        holdout = _safe_float(data.get("holdout_accuracy"), None)
        if holdout is not None:
            metrics.append(f"holdout {holdout * 100:.1f}%")
        live_accuracy = _safe_float(data.get("live_validation_accuracy_pct"), None)
        if live_accuracy is not None and live_accuracy > 0:
            metrics.append(f"live {live_accuracy:.1f}%")
        if metrics:
            clauses.append("validation reads " + self._join_clauses(metrics))
        sentence = self._join_clauses(clauses) or self._humanize_reason(entry.reason) or "research validation is available"
        return f"- Research backing: {self._sentence(sentence)}."

    def _telegram_plain_stage_line(self, entry: JournalEntry, signal=None, summary: Optional[Dict[str, Any]] = None) -> str:
        summary = summary or {}
        data = entry.data if isinstance(entry.data, dict) else {}
        name = str(entry.name or "").lower()
        direction_word = str(self.direction or "").lower()

        if name == "market":
            return self._telegram_plain_market_line(entry, data, summary)

        if name == "intelligence":
            return self._telegram_plain_intelligence_line(entry, data)

        if name == "memory":
            return self._telegram_plain_memory_line(entry, data)

        if name == "meta_ai":
            return self._telegram_plain_meta_ai_line(entry, data, summary)

        if name == "policy":
            return self._telegram_plain_policy_line(entry, data, direction_word)

        if name == "governance":
            return self._telegram_plain_governance_line(entry, data, summary)

        if name == "execution":
            return self._telegram_plain_execution_line(entry, data, summary, signal)

        if name == "research_validation":
            return self._telegram_plain_research_line(entry, data)

        reason = self._humanize_reason(entry.reason)
        if not reason:
            return ""
        label = self._sentence(self._humanize_token(entry.name) or "review")
        return f"- {label}: {self._sentence(reason)}."

    def _telegram_markdown_summary_lines(self, summary: Dict[str, Any], survived: bool) -> List[str]:
        lines: List[str] = []

        if summary.get("real_sources_valid") is not None and summary.get("real_sources_required") is not None:
            lines.append(
                f"🧱 *Real Sources:* `{summary['real_sources_valid']}/{summary['real_sources_required']}`"
            )

        if summary.get("final_policy_decision"):
            score_txt = ""
            if summary.get("final_policy_score") is not None:
                score_txt = f"  score `{summary['final_policy_score']:.3f}`"
            lines.append(
                f"🧠 *Final Gate:* `{self._escape_markdown(summary['final_policy_decision'])}`{score_txt}"
            )

        if summary.get("opportunity_score") is not None:
            rank_txt = ""
            if summary.get("opportunity_rank") is not None:
                rank_txt = f"  rank `#{int(summary['opportunity_rank'])}`"
            lines.append(
                f"*Opportunity:* `{float(summary['opportunity_score']):.3f}`{rank_txt}"
            )

        if summary.get("setup_quality") is not None or summary.get("alignment_score") is not None:
            lines.append(
                f"*Structure:* `{self._escape_markdown(str(summary.get('structure_bias') or 'neutral'))}`"
                f"  align `{float(summary.get('alignment_score') or 0.0):.2f}`"
                f"  quality `{float(summary.get('setup_quality') or 0.0):.2f}`"
            )

        if summary.get("memory_score") is not None:
            lines.append(
                f"*Memory:* `score {float(summary.get('memory_score') or 0.0):.1f}`"
                f"  edge `{float(summary.get('memory_edge') or 0.0):+.2f}`"
                f"  samples `{int(summary.get('memory_sample_count') or 0)}`"
            )

        if not survived and summary.get("killed_by"):
            lines.append(
                f"🛑 *Killed By:* `{self._escape_markdown(str(summary['killed_by']).upper())}`"
            )

        positive_factor = summary.get("top_positive_factor") or ""
        negative_factor = summary.get("top_negative_factor") or ""
        factor_parts: List[str] = []
        if positive_factor:
            factor_parts.append(
                f"+{self._escape_markdown(str(positive_factor))} {float(summary.get('top_positive_factor_value') or 0.0):+.2f}"
            )
        if negative_factor:
            factor_parts.append(
                f"{self._escape_markdown(str(negative_factor))} {float(summary.get('top_negative_factor_value') or 0.0):+.2f}"
            )
        if factor_parts:
            lines.append(f"*Factors:* `{'  '.join(factor_parts)}`")

        return lines

    def _telegram_markdown_entry_lines(self) -> List[str]:
        lines: List[str] = []
        for entry in self.entries:
            emoji = entry.emoji()
            name = self._escape_markdown(entry.name.upper().replace("_", " "))

            if entry.conf_delta > 0:
                conf_str = f"conf {entry.conf_before:.2f} → {entry.conf_after:.2f} ⬆"
            elif entry.conf_delta < 0:
                conf_str = f"conf {entry.conf_before:.2f} → {entry.conf_after:.2f} ⬇"
            else:
                conf_str = f"conf {entry.conf_before:.2f}"

            reason_str = f"  _{self._escape_markdown(entry.reason)}_" if entry.reason else ""
            lines.append(f"{emoji} *{name}*   {conf_str}{reason_str}")

            if entry.data:
                data_parts: List[str] = []
                for k, v in entry.data.items():
                    if isinstance(v, float):
                        data_parts.append(f"{self._escape_markdown(k)}={v:.3f}")
                    elif v is not None:
                        data_parts.append(f"{self._escape_markdown(k)}={self._escape_markdown(v)}")
                if data_parts:
                    lines.append(f"   `{'  '.join(data_parts[:4])}`")
        return lines

    def _telegram_markdown_execution_lines(self, signal: Any) -> List[str]:
        if signal is None:
            return []

        snapshot = self._execution_snapshot(signal)
        entry_p = float(snapshot["entry_price"])
        sl = float(snapshot["stop_loss"])
        conf = float(snapshot["confidence"])
        size = float(snapshot["position_size"])
        rr = float(snapshot["risk_reward"])
        first_target = float(snapshot["first_target"])
        runner_target = float(snapshot["runner_target"])
        first_rr = float(snapshot["first_rr"])
        runner_rr = float(snapshot["runner_rr"])

        executing_lines = [
            "🚀 *EXECUTING*",
            f"   Entry: `{entry_p:.5f}`",
            f"   SL:    `{sl:.5f}`",
        ]
        if first_target:
            label = "TP1" if runner_target and abs(runner_target - first_target) > 1e-9 else "TP"
            executing_lines.append(f"   {label}:    `{first_target:.5f}`")
        if runner_target and abs(runner_target - first_target) > 1e-9:
            executing_lines.append(f"   Run:   `{runner_target:.5f}`")
            executing_lines.append(f"   R:R:   TP1 {first_rr:.1f}:1 | Run {runner_rr:.1f}:1")
        else:
            executing_lines.append(f"   R:R:   {rr:.1f}:1")
        executing_lines.extend([
            f"   Conf:  {conf:.0%}",
            f"   Size:  {size:.4f}",
        ])
        return ["\n".join(executing_lines)]

    def _telegram_plain_factor_notes(self, summary: Dict[str, Any]) -> str:
        factor_notes: List[str] = []
        positive_factor = str(summary.get("top_positive_factor") or "").strip()
        negative_factor = str(summary.get("top_negative_factor") or "").strip()
        if positive_factor:
            factor_notes.append(f"the strongest support came from {self._factor_label(positive_factor)}")
        if negative_factor:
            factor_notes.append(f"the main caution came from {self._factor_label(negative_factor)}")
        if factor_notes:
            return f"{self._sentence(self._join_clauses(factor_notes))}."
        return ""

    def _telegram_plain_intro_lines(
        self,
        signal: Any,
        summary: Dict[str, Any],
        survived: bool,
        side: str,
        direction_word: str,
    ) -> List[str]:
        lines = [f"{self.asset} {side} setup"]
        confidence = summary.get("final_confidence")

        if survived:
            entry_p = self._format_price(getattr(signal, "entry_price", 0.0) if signal else 0.0)
            intro = f"The bot is preparing a {direction_word} trade on {self.asset}"
            if entry_p:
                intro += f" near {entry_p}"
            intro += "."
            lines.append(intro)
            if confidence is not None:
                lines.append(
                    f"Overall confidence is {self._format_pct(confidence)}, and the setup passed all live checks."
                )
            else:
                lines.append("The setup passed all live checks and is ready to execute.")
        else:
            lines.append("The bot reviewed this setup, but it was blocked before execution.")
            if confidence is not None:
                lines.append(f"Final reviewed confidence was {self._format_pct(confidence)}.")

        factor_note = self._telegram_plain_factor_notes(summary)
        if factor_note:
            lines.append(factor_note)
        return lines

    def _telegram_plain_stage_groups(
        self,
        signal=None,
        summary: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, List[str]]:
        summary = summary or {}
        context_lines: List[str] = []
        trust_lines: List[str] = []
        execution_lines: List[str] = []
        other_lines: List[str] = []

        for entry in self.entries:
            stage_line = self._telegram_plain_stage_line(entry, signal=signal, summary=summary)
            if stage_line:
                name = str(entry.name or "").lower()
                if name in {"market", "intelligence", "memory", "meta_ai"}:
                    context_lines.append(stage_line)
                elif name in {"policy", "governance", "research_validation"}:
                    trust_lines.append(stage_line)
                elif name == "execution":
                    execution_lines.append(stage_line)
                else:
                    other_lines.append(stage_line)

        return {
            "context_lines": context_lines,
            "trust_lines": trust_lines,
            "execution_lines": execution_lines,
            "other_lines": other_lines,
        }

    def _telegram_plain_section_lines(
        self,
        survived: bool,
        summary: Dict[str, Any],
        groups: Dict[str, List[str]],
    ) -> List[str]:
        context_lines = groups["context_lines"]
        trust_lines = groups["trust_lines"]
        other_lines = groups["other_lines"]

        if survived:
            lines: List[str] = []
            if context_lines:
                lines.extend(["", "What the bot is seeing right now:"])
                lines.extend(context_lines)
            if trust_lines:
                lines.extend(["", "Why the bot trusts this setup:"])
                lines.extend(trust_lines)
            return lines

        lines = ["", "Why it was blocked:"]
        if summary.get("kill_reason"):
            lines.append(f"- Main reason: {self._sentence(self._humanize_reason(summary['kill_reason']))}.")
        if context_lines or trust_lines or other_lines:
            lines.extend(context_lines + trust_lines + other_lines)
        return lines

    def _telegram_plain_execution_block(
        self,
        signal: Any,
        groups: Dict[str, List[str]],
    ) -> List[str]:
        if signal is None:
            return []

        snapshot = self._execution_snapshot(signal)
        entry_p = float(snapshot["entry_price"])
        sl = float(snapshot["stop_loss"])
        conf = float(snapshot["confidence"])
        size = float(snapshot["position_size"])
        rr = float(snapshot["risk_reward"])
        first_target = float(snapshot["first_target"])
        runner_target = float(snapshot["runner_target"])
        first_rr = float(snapshot["first_rr"])
        runner_rr = float(snapshot["runner_rr"])

        lines = [
            "",
            "How the trade will be managed:",
        ]
        lines.extend(groups["execution_lines"])
        lines.append(f"- Planned entry: {self._format_price(entry_p)}")
        lines.append(f"- Protective stop: {self._format_price(sl)}")
        if first_target:
            lines.append(f"- First main target: {self._format_price(first_target)}")
        if runner_target and abs(runner_target - first_target) > 1e-9:
            lines.append(f"- Runner target: {self._format_price(runner_target)}")
            lines.append(f"- Reward to risk: TP1 {first_rr:.1f}:1 | Runner {runner_rr:.1f}:1")
        else:
            lines.append(f"- Reward to risk: {rr:.1f}:1")
        lines.append(f"- Position size: {size:.4f}")
        lines.append(f"- Confidence at execution: {conf:.0%}")
        return lines

    def to_telegram(self, signal=None) -> str:
        """
        Format the full journal as a Telegram Markdown message.
        Called by the signal reporter after the decision cycle completes.
        """
        survived = self.final_decision() == "SURVIVED"
        direction = self._escape_markdown(self.direction)
        asset = self._escape_markdown(self.asset)

        if survived:
            header = f"🔔 *NEW SIGNAL — {asset} {direction}*"
        else:
            kill = self.kill_entry()
            reason = self._escape_markdown(kill.reason if kill else 'unknown')
            header = (
                f"💀 *SIGNAL KILLED — {asset} {direction}*\n"
                f"_Reason: {reason}_"
            )

        summary = self.summary(signal)
        lines = [header, "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"]
        lines.extend(self._telegram_markdown_summary_lines(summary, survived))
        lines.extend(self._telegram_markdown_entry_lines())
        lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        lines.extend(self._telegram_markdown_execution_lines(signal) if survived else [])
        lines.append(f"\n_Decision engine: {self.total_elapsed_ms():.0f}ms_")
        return "\n".join(lines)

    def to_telegram_plain(self, signal=None) -> str:
        """Plain-text Telegram rendering for runtime alerts.
        This avoids Markdown entity failures in long journal messages.
        """
        survived = self.final_decision() == "SURVIVED"
        summary = self.summary(signal)
        side = "BUY" if str(self.direction or "").upper() == "BUY" else "SELL"
        direction_word = side.lower()
        lines = self._telegram_plain_intro_lines(signal, summary, survived, side, direction_word)
        groups = self._telegram_plain_stage_groups(signal=signal, summary=summary)
        lines.extend(self._telegram_plain_section_lines(survived, summary, groups))
        if survived and signal:
            lines.extend(self._telegram_plain_execution_block(signal, groups))
        lines.extend(["", f"Review time: {self.total_elapsed_ms() / 1000.0:.1f}s"])
        return "\n".join(lines)

    def to_dict(self, signal=None) -> Dict[str, Any]:
        summary = self.summary(signal)
        return {
            "asset":     self.asset,
            "direction": self.direction,
            "decision":  self.final_decision(),
            "entries":   self.to_list(),
            "elapsed_ms": self.total_elapsed_ms(),
            **summary,
        }
