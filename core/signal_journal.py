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

    def _extract_factor_attribution(self, signal=None) -> Dict[str, float]:
        metadata = dict(getattr(signal, "metadata", {}) or {})
        sign = _direction_sign(self.direction)

        market = self._latest_entry("market")
        intelligence = self._latest_entry("intelligence")
        execution = self._latest_entry("execution")
        policy = self._latest_named("policy", "agent")
        governance = self._latest_named("governance", "data_integrity")

        structure_data = metadata.get("market_structure")
        if not isinstance(structure_data, dict):
            structure_data = {}
        if not structure_data and market and isinstance(market.data.get("market_structure"), dict):
            structure_data = dict(market.data.get("market_structure") or {})

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

        bias_factor = 0.0
        if structure_bias == "buy":
            bias_factor = 1.0 if sign > 0 else -1.0
        elif structure_bias == "sell":
            bias_factor = 1.0 if sign < 0 else -1.0
        setup_factor = _clip11(setup_signal * sign)
        market_structure = _clip11(bias_factor * alignment_score * 0.7 + setup_factor * 0.3)

        ml_confidence = _safe_float(metadata.get("ml_confidence"), 0.0) or 0.0
        ml_prediction = _safe_float(metadata.get("ml_prediction"), None)
        ml_direction = 0.0
        if ml_prediction is not None:
            if ml_prediction > 0.5:
                ml_direction = 1.0
            elif ml_prediction < 0.5:
                ml_direction = -1.0
        ml = _clip11(ml_direction * sign * min(1.0, ml_confidence))

        sentiment_score = _safe_float(
            metadata.get(
                "sentiment_score",
                (intelligence.data or {}).get("sentiment_score") if intelligence else 0.0,
            ),
            0.0,
        ) or 0.0
        sentiment = _clip11(sentiment_score * sign)

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
        whale = 0.0
        if whale_dominant in {"BUY", "SELL"}:
            whale_sign = 1.0 if whale_dominant == "BUY" else -1.0
            whale = _clip11(whale_sign * sign * whale_ratio)

        orderflow_imbalance = _safe_float(
            metadata.get(
                "orderflow_imbalance",
                (market.data or {}).get("orderflow_imbalance") if market else 0.0,
            ),
            0.0,
        ) or 0.0
        order_flow = (
            _clip11(orderflow_imbalance * sign)
            if metadata.get("orderflow_applicable") is not False
            else 0.0
        )

        breakdown = metadata.get("opportunity_breakdown")
        if not isinstance(breakdown, dict):
            breakdown = {}
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
        risk = round(sum(risk_components) / len(risk_components), 4) if risk_components else 0.0

        agent_score = _safe_float(
            metadata.get("agent_score", (policy.data or {}).get("agent_score") if policy else None),
            None,
        )
        policy_factor = _signed_quality(agent_score) if agent_score is not None else 0.0

        governance_score = _safe_float(
            metadata.get(
                "governance_score",
                (governance.data or {}).get("score") if governance else None,
            ),
            None,
        )
        governance_factor = 0.0
        if governance_score is not None:
            governance_factor = _clip11((governance_score - 50.0) / 50.0)
            if governance and governance.decision == KILLED:
                governance_factor = min(governance_factor, -0.3)

        memory = self._latest_entry("memory")
        memory_edge = _safe_float(
            metadata.get(
                "memory_edge",
                (memory.data or {}).get("memory_edge") if memory else None,
            ),
            None,
        )
        memory_factor = round(_clip11(memory_edge), 4) if memory_edge is not None else 0.0

        return {
            "market_structure": round(market_structure, 4),
            "ml": round(ml, 4),
            "sentiment": round(sentiment, 4),
            "whales": round(whale, 4),
            "order_flow": round(order_flow, 4),
            "memory": round(memory_factor, 4),
            "policy": round(policy_factor, 4),
            "governance": round(governance_factor, 4),
            "risk": round(risk, 4),
        }

    def _extract_setup_fingerprint(self, signal=None) -> Dict[str, Any]:
        metadata = dict(getattr(signal, "metadata", {}) or {})
        market = self._latest_entry("market")
        intelligence = self._latest_entry("intelligence")

        structure_data = metadata.get("market_structure")
        if not isinstance(structure_data, dict):
            structure_data = {}
        if not structure_data and market and isinstance(market.data.get("market_structure"), dict):
            structure_data = dict(market.data.get("market_structure") or {})

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
            0.0,
        ) or 0.0
        orderflow_imbalance = _safe_float(
            metadata.get(
                "orderflow_imbalance",
                (market.data or {}).get("orderflow_imbalance") if market else None,
            ),
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

        if abs(breakout_score) >= abs(pullback_score) and abs(breakout_score) >= 0.2:
            setup_style = "breakout"
        elif abs(pullback_score) >= 0.2:
            setup_style = "pullback"
        else:
            setup_style = "mixed"

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

        regime = str(metadata.get("regime") or "")
        if not regime and market:
            regime = str((market.data or {}).get("regime") or "")

        session = str(metadata.get("session") or "")
        if not session and market:
            session = str((market.data or {}).get("session") or "")

        return {
            "regime": regime,
            "structure_bias": str(
                metadata.get("structure_bias")
                or structure_data.get("structure_bias")
                or "neutral"
            ).lower(),
            "alignment_score": round(
                _safe_float(metadata.get("alignment_score", structure_data.get("alignment_score")), 0.0) or 0.0,
                4,
            ),
            "setup_quality": round(
                _safe_float(metadata.get("setup_quality", structure_data.get("setup_quality")), 0.0) or 0.0,
                4,
            ),
            "volatility_state": str(
                metadata.get("volatility_state")
                or structure_data.get("volatility_state")
                or "unknown"
            ),
            "setup_style": setup_style,
            "sentiment_bucket": sentiment_bucket,
            "whale_bucket": whale_bucket,
            "orderflow_bucket": orderflow_bucket,
            "session": session,
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

    def _telegram_plain_stage_line(self, entry: JournalEntry, signal=None, summary: Optional[Dict[str, Any]] = None) -> str:
        summary = summary or {}
        data = entry.data if isinstance(entry.data, dict) else {}
        name = str(entry.name or "").lower()
        direction_word = str(self.direction or "").lower()

        if name == "market":
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
            sentence = self._join_clauses(clauses) or self._humanize_reason(entry.reason) or "market conditions look tradable"
            return f"- Market view: {self._sentence(sentence)}."

        if name == "intelligence":
            clauses = []
            sentiment_desc = self._describe_sentiment(data.get("sentiment_score"))
            if sentiment_desc == "neutral":
                clauses.append("sentiment is broadly neutral")
            else:
                clauses.append(f"sentiment is {sentiment_desc}")
            whale_dominant = str(data.get("whale_dominant") or "").upper()
            if whale_dominant in {"BUY", "SELL"}:
                clauses.append(f"whale flow leans {whale_dominant.lower()}")
            source_count = len(data.get("sentiment_sources") or [])
            if source_count:
                clauses.append(f"this view is backed by {source_count} sources")
            narrative = self._narrative_label(data.get("narrative"))
            if narrative:
                clauses.append(f"the main narrative is {narrative}")
            sentence = self._join_clauses(clauses) or self._humanize_reason(entry.reason) or "intelligence checks were supportive"
            return f"- Flow and sentiment: {self._sentence(sentence)}."

        if name == "memory":
            clauses = []
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

        if name == "meta_ai":
            clauses = []
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

        if name == "policy":
            clauses = []
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

        if name == "governance":
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

        if name == "execution":
            clauses = []
            if entry.decision == PASS:
                clauses.append("the setup stayed above the live execution floor")
            else:
                clauses.append(self._humanize_reason(entry.reason) or "execution rules blocked the trade")
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

        if name == "research_validation":
            clauses = []
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

        reason = self._humanize_reason(entry.reason)
        if not reason:
            return ""
        label = self._sentence(self._humanize_token(entry.name) or "review")
        return f"- {label}: {self._sentence(reason)}."

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
            kill   = self.kill_entry()
            reason = self._escape_markdown(kill.reason if kill else 'unknown')
            header = (
                f"💀 *SIGNAL KILLED — {asset} {direction}*\n"
                f"_Reason: {reason}_"
            )

        summary = self.summary(signal)
        lines = [header, "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"]

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

        for entry in self.entries:
            emoji = entry.emoji()
            name  = self._escape_markdown(entry.name.upper().replace("_", " "))

            # Confidence delta display
            if entry.conf_delta > 0:
                conf_str = f"conf {entry.conf_before:.2f} → {entry.conf_after:.2f} ⬆"
            elif entry.conf_delta < 0:
                conf_str = f"conf {entry.conf_before:.2f} → {entry.conf_after:.2f} ⬇"
            else:
                conf_str = f"conf {entry.conf_before:.2f}"

            reason_str = f"  _{self._escape_markdown(entry.reason)}_" if entry.reason else ""
            lines.append(f"{emoji} *{name}*   {conf_str}{reason_str}")

            # Show phase data inline if available
            if entry.data:
                data_parts = []
                for k, v in entry.data.items():
                    if isinstance(v, float):
                        data_parts.append(f"{self._escape_markdown(k)}={v:.3f}")
                    elif v is not None:
                        data_parts.append(f"{self._escape_markdown(k)}={self._escape_markdown(v)}")
                if data_parts:
                    lines.append(f"   `{'  '.join(data_parts[:4])}`")

        lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

        # Execution details for surviving signals
        if survived and signal:
            entry_p = float(getattr(signal, "entry_price", 0))
            sl      = float(getattr(signal, "stop_loss",   0))
            tp      = float(getattr(signal, "take_profit", 0))
            conf    = float(getattr(signal, "confidence",  0))
            size    = float(getattr(signal, "position_size", 0))
            rr      = float(getattr(signal, "risk_reward",  0))

            lines.append(
                f"🚀 *EXECUTING*\n"
                f"   Entry: `{entry_p:.5f}`\n"
                f"   SL:    `{sl:.5f}`\n"
                f"   TP:    `{tp:.5f}`\n"
                f"   R:R:   {rr:.1f}:1\n"
                f"   Conf:  {conf:.0%}\n"
                f"   Size:  {size:.4f}"
            )

        lines.append(f"\n_Decision engine: {self.total_elapsed_ms():.0f}ms_")
        return "\n".join(lines)

    def to_telegram_plain(self, signal=None) -> str:
        """Plain-text Telegram rendering for runtime alerts.
        This avoids Markdown entity failures in long journal messages.
        """
        survived = self.final_decision() == "SURVIVED"
        summary = self.summary(signal)
        side = "BUY" if str(self.direction or "").upper() == "BUY" else "SELL"
        header = f"{self.asset} {side} setup"
        direction_word = side.lower()
        lines = [header]

        confidence = summary.get("final_confidence")
        if survived:
            entry_p = self._format_price(getattr(signal, "entry_price", 0.0) if signal else 0.0)
            intro = f"The bot is preparing a {direction_word} trade on {self.asset}"
            if entry_p:
                intro += f" near {entry_p}"
            intro += "."
            lines.append(intro)
            if confidence is not None:
                lines.append(f"Overall confidence is {self._format_pct(confidence)}, and the setup passed all live checks.")
            else:
                lines.append("The setup passed all live checks and is ready to execute.")
        else:
            lines.append("The bot reviewed this setup, but it was blocked before execution.")
            if confidence is not None:
                lines.append(f"Final reviewed confidence was {self._format_pct(confidence)}.")

        factor_notes: List[str] = []
        positive_factor = str(summary.get("top_positive_factor") or "").strip()
        negative_factor = str(summary.get("top_negative_factor") or "").strip()
        if positive_factor:
            factor_notes.append(f"the strongest support came from {self._factor_label(positive_factor)}")
        if negative_factor:
            factor_notes.append(f"the main caution came from {self._factor_label(negative_factor)}")
        if factor_notes:
            lines.append(f"{self._sentence(self._join_clauses(factor_notes))}.")

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
                elif name in {"execution"}:
                    execution_lines.append(stage_line)
                else:
                    other_lines.append(stage_line)

        if survived:
            if context_lines:
                lines.extend(["", "What the bot is seeing right now:"])
                lines.extend(context_lines)
            if trust_lines:
                lines.extend(["", "Why the bot trusts this setup:"])
                lines.extend(trust_lines)
        else:
            lines.extend(["", "Why it was blocked:"])
            if summary.get("kill_reason"):
                lines.append(f"- Main reason: {self._sentence(self._humanize_reason(summary['kill_reason']))}.")
            if context_lines or trust_lines or other_lines:
                lines.extend(context_lines + trust_lines + other_lines)

        if survived and signal:
            entry_p = float(getattr(signal, "entry_price", 0))
            sl      = float(getattr(signal, "stop_loss",   0))
            tp      = float(getattr(signal, "take_profit", 0))
            conf    = float(getattr(signal, "confidence",  0))
            size    = float(getattr(signal, "position_size", 0))
            rr      = float(getattr(signal, "risk_reward",  0))

            lines.extend([
                "",
                "How the trade will be managed:",
            ])
            lines.extend(execution_lines)
            lines.extend([
                f"- Planned entry: {self._format_price(entry_p)}",
                f"- Protective stop: {self._format_price(sl)}",
                f"- First main target: {self._format_price(tp)}",
                f"- Reward to risk: {rr:.1f}:1",
                f"- Position size: {size:.4f}",
                f"- Confidence at execution: {conf:.0%}",
            ])

        lines.extend([
            "",
            f"Review time: {self.total_elapsed_ms() / 1000.0:.1f}s",
        ])
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
