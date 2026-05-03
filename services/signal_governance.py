from __future__ import annotations

from typing import Any, Dict, Mapping, Optional


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return float(default)
        return float(value)
    except Exception:
        return float(default)


def _dir_sign(value: str) -> int:
    token = str(value or "").strip().upper()
    if token == "BUY":
        return 1
    if token == "SELL":
        return -1
    return 0


class SignalGovernance:
    def evaluate(self, signal: Any, context: Optional[Mapping[str, Any]] = None) -> Dict[str, Any]:
        context = dict(context or {})
        metadata = dict(getattr(signal, "metadata", {}) or {})
        structure = dict(context.get("market_structure") or metadata.get("market_structure") or {})
        micro = dict(context.get("market_microstructure") or metadata.get("market_microstructure") or {})
        direction = _dir_sign(getattr(signal, "direction", ""))
        violations: list[str] = []
        warnings: list[str] = []

        if direction == 0:
            violations.append("missing_direction")
        if _safe_float(getattr(signal, "entry_price", 0.0), 0.0) <= 0.0:
            warnings.append("missing_entry_price")
        if _safe_float(getattr(signal, "risk_reward", 0.0), 0.0) < 1.20:
            warnings.append("risk_reward_below_target")

        depth_available = bool(micro.get("depth_available") or metadata.get("depth_available"))
        if depth_available:
            depth_quality = _safe_float(micro.get("depth_quality", metadata.get("depth_quality")), 0.0)
            trust = _safe_float(micro.get("depth_provider_trust_score", metadata.get("depth_provider_trust_score")), 0.0)
            book = _safe_float(micro.get("book_imbalance", metadata.get("book_imbalance")), 0.0) * direction
            flow = _safe_float(micro.get("score", metadata.get("microstructure_alignment")), 0.0) * direction
            if depth_quality < 0.20 or trust < 0.42:
                warnings.append("depth_quality_low")
            if max(book, flow) < -0.22:
                violations.append("depth_conflict")

        bias = str(structure.get("structure_bias", metadata.get("structure_bias", "neutral")) or "neutral").lower()
        if bias in {"buy", "sell"}:
            if (bias == "buy" and direction < 0) or (bias == "sell" and direction > 0):
                violations.append("structure_conflict")

        score = 1.0 - len(violations) * 0.35 - len(warnings) * 0.08
        score = max(0.0, min(1.0, score))
        approved = not violations and score >= 0.50
        return {
            "approved": bool(approved),
            "score": round(score, 4),
            "reason": "approved" if approved else ",".join(violations),
            "violations": violations,
            "warnings": warnings,
            "live_validation": {
                "scope": "universal_depth",
                "depth_available": bool(depth_available),
                "violations": violations,
            },
        }

    def validate(self, signal: Any, context: Optional[Mapping[str, Any]] = None) -> Dict[str, Any]:
        return self.evaluate(signal, context)


signal_governance = SignalGovernance()
