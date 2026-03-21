from __future__ import annotations

from typing import Dict

from utils.logger import get_logger

logger = get_logger()

# ── Engine names ──────────────────────────────────────────────────────────────
ENGINES = ["technical", "sentiment", "whale", "orderflow", "macro"]

# ── Weight table: regime → {engine: weight} ──────────────────────────────────
# All rows must sum to 1.0
REGIME_WEIGHTS: Dict[str, Dict[str, float]] = {
    "trending_bull": {
        "technical":  0.40,
        "sentiment":  0.25,
        "whale":      0.15,
        "orderflow":  0.12,
        "macro":      0.08,
    },
    "trending_bear": {
        "technical":  0.35,
        "sentiment":  0.15,
        "whale":      0.25,
        "orderflow":  0.12,
        "macro":      0.13,
    },
    "ranging": {
        "technical":  0.20,
        "sentiment":  0.18,
        "whale":      0.20,
        "orderflow":  0.30,
        "macro":      0.12,
    },
    "high_volatility": {
        "technical":  0.18,
        "sentiment":  0.12,
        "whale":      0.35,
        "orderflow":  0.25,
        "macro":      0.10,
    },
    "crisis": {
        "technical":  0.10,
        "sentiment":  0.08,
        "whale":      0.27,
        "orderflow":  0.15,
        "macro":      0.40,
    },
}

# Default weights when regime is unknown
_DEFAULT_WEIGHTS: Dict[str, float] = {
    "technical":  0.30,
    "sentiment":  0.20,
    "whale":      0.20,
    "orderflow":  0.18,
    "macro":      0.12,
}


class ModelWeightingEngine:
    """
    Returns weight dict for a given regime.
    Supports custom weight overrides at runtime.
    """

    def __init__(self) -> None:
        self._overrides: Dict[str, Dict[str, float]] = {}

    # ── Public API ────────────────────────────────────────────────────────────

    def get_weights(self, regime: str) -> Dict[str, float]:
        """
        Return normalised weight dict for the given regime.
        Custom overrides take precedence over the built-in table.
        """
        if regime in self._overrides:
            return self._normalise(self._overrides[regime])
        weights = REGIME_WEIGHTS.get(regime, _DEFAULT_WEIGHTS)
        return self._normalise(dict(weights))

    def set_override(self, regime: str, weights: Dict[str, float]) -> None:
        """
        Override weights for a specific regime at runtime.
        Useful if you find the defaults don't match your asset's behaviour.
        Weights are normalised automatically so they don't need to sum to 1.
        """
        self._overrides[regime] = weights
        logger.info(f"[WeightingEngine] Override set for regime '{regime}': {weights}")

    def clear_override(self, regime: str) -> None:
        self._overrides.pop(regime, None)

    def explain(self, regime: str) -> str:
        """Return a human-readable weight breakdown for logging / Telegram."""
        weights = self.get_weights(regime)
        parts   = [f"{k}={v:.0%}" for k, v in sorted(
            weights.items(), key=lambda x: x[1], reverse=True
        )]
        return "  ".join(parts)

    def all_regimes(self) -> Dict[str, Dict[str, float]]:
        return {r: self.get_weights(r) for r in REGIME_WEIGHTS}

    # ── Internal ──────────────────────────────────────────────────────────────

    @staticmethod
    def _normalise(weights: Dict[str, float]) -> Dict[str, float]:
        total = sum(weights.values())
        if total == 0:
            return {k: 1.0 / len(weights) for k in weights}
        return {k: round(v / total, 4) for k, v in weights.items()}