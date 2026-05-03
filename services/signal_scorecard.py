from __future__ import annotations

from typing import Any, Dict, Mapping, Optional


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return float(default)
        return float(value)
    except Exception:
        return float(default)


def _clip(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, float(value)))


def _dir_sign(direction: str) -> int:
    token = str(direction or "").strip().upper()
    if token == "BUY":
        return 1
    if token == "SELL":
        return -1
    return 0


class SignalScorecard:
    def _live_validation(self, asset: str) -> tuple[float | None, Dict[str, Any]]:
        return None, {"scope": "bootstrap", "samples": 0, "accuracy_pct": 0.0}

    def _execution_expectancy(self, signal: Any) -> tuple[float | None, Dict[str, Any]]:
        return None, {"scope": "bootstrap", "sample_count": 0}

    def score(self, signal: Any, context: Optional[Mapping[str, Any]] = None) -> Dict[str, Any]:
        context = dict(context or {})
        metadata = dict(getattr(signal, "metadata", {}) or {})
        structure = dict(context.get("market_structure") or metadata.get("market_structure") or {})
        micro = dict(context.get("market_microstructure") or metadata.get("market_microstructure") or {})
        direction = _dir_sign(getattr(signal, "direction", metadata.get("direction", "")))

        confidence = _clip(_safe_float(getattr(signal, "confidence", metadata.get("confidence")), 0.0))
        rr = _safe_float(getattr(signal, "risk_reward", metadata.get("risk_reward")), 0.0)
        rr_score = _clip((rr - 1.0) / 1.8)
        alignment = _clip(_safe_float(structure.get("alignment_score", metadata.get("alignment_score")), 0.0))
        setup = _clip(_safe_float(structure.get("setup_quality", metadata.get("setup_quality")), 0.0))
        target = _clip(_safe_float(structure.get("target_efficiency_score", metadata.get("target_efficiency_score")), 0.0))
        depth_quality = _clip(_safe_float(micro.get("depth_quality", metadata.get("depth_quality")), 0.0))
        book = _safe_float(micro.get("book_imbalance", metadata.get("book_imbalance")), 0.0) * direction
        flow = _safe_float(micro.get("score", metadata.get("microstructure_alignment")), 0.0) * direction
        depth_support = _clip(max(book, flow, 0.0))
        context_support = _clip(
            max(
                _safe_float(metadata.get("cross_asset_alignment"), 0.0) * direction,
                _safe_float(metadata.get("sentiment_score"), 0.0) * direction,
                0.0,
            )
        )
        penalties = 0.0
        extension = _safe_float(structure.get("extension_score", metadata.get("extension_score")), 0.0)
        if extension > 1.18:
            penalties += min(0.18, (extension - 1.18) * 0.18)
        if bool(metadata.get("depth_conflict")):
            penalties += 0.22
        if bool(metadata.get("market_closed")):
            penalties += 1.0

        components = {
            "confidence": round(confidence, 4),
            "risk_reward": round(rr_score, 4),
            "structure": round(alignment * 0.55 + setup * 0.45, 4),
            "target": round(target, 4),
            "depth": round(depth_quality * 0.45 + depth_support * 0.55, 4),
            "context": round(context_support, 4),
            "penalties": round(penalties, 4),
        }
        final = (
            confidence * 0.24
            + rr_score * 0.13
            + components["structure"] * 0.27
            + target * 0.08
            + components["depth"] * 0.20
            + context_support * 0.08
            - penalties
        )
        live_score, live_profile = self._live_validation(str(getattr(signal, "asset", "") or ""))
        expectancy_score, expectancy_profile = self._execution_expectancy(signal)
        if expectancy_score is not None:
            expectancy_score = _clip(_safe_float(expectancy_score), 0.0, 1.0)
            if expectancy_score < 0.45:
                final = min(final, 0.66)
        live_scope = str((live_profile or {}).get("scope", "") or "").lower()
        if expectancy_score is None and live_scope == "bootstrap":
            final = min(final, 0.72)
        if live_score is not None:
            final = min(1.0, final + max(-0.06, min(0.04, (_safe_float(live_score) - 0.55) * 0.08)))
        final = _clip(final)
        return {
            "final_score": round(final, 4),
            "score": round(final, 4),
            "approved": bool(final >= 0.56),
            "components": components,
            "live_validation": live_profile,
            "execution_expectancy": expectancy_profile,
            "notes": [],
        }

    def evaluate(self, signal: Any, context: Optional[Mapping[str, Any]] = None) -> Dict[str, Any]:
        return self.score(signal, context)


_service = SignalScorecard()


def get_service() -> SignalScorecard:
    return _service
