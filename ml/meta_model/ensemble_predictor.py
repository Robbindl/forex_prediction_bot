from __future__ import annotations

import threading
from typing import Any, Dict, Optional, Tuple, TYPE_CHECKING

from utils.logger import get_logger

if TYPE_CHECKING:
    from core.signal import Signal
    from ml.meta_model.market_condition_classifier import MarketConditionClassifier
    from ml.meta_model.model_weighting_engine      import ModelWeightingEngine

logger = get_logger()

BOOST_THRESHOLD  = 0.65
REDUCE_THRESHOLD = 0.35
BOOST_AMOUNT     = 0.04
REDUCE_AMOUNT    = 0.04
NEUTRAL_ZONE_MSG = "ensemble neutral — no adjustment"


class EnsemblePredictor:
    """
    Combines all five signal engines into one weighted prediction.
    Thread-safe — one instance per process.
    """

    def __init__(
        self,
        classifier: "MarketConditionClassifier",
        weighter:   "ModelWeightingEngine",
    ) -> None:
        self._classifier = classifier
        self._weighter   = weighter
        self._lock       = threading.Lock()

    def process(
        self,
        signal:  "Signal",
        context: Dict[str, Any],
    ) -> "Signal":
        conf_before = signal.confidence

        regime  = self._classifier.classify_from_context(context)
        weights = self._weighter.get_weights(regime)
        scores  = self._collect_scores(signal, context)

        ensemble_score, active_engines = self._compute_ensemble(scores, weights)
        adj_label = self._adjust_confidence(signal, ensemble_score)

        signal.metadata["meta_ai_regime"]   = regime
        signal.metadata["meta_ai_ensemble"] = round(ensemble_score, 4)
        signal.metadata["meta_ai_active_engines"] = active_engines
        signal.metadata["meta_ai_weights"]  = weights
        signal.metadata["meta_ai_scores"]   = {
            k: round(v, 3) for k, v in scores.items() if v is not None
        }

        reason = f"regime={regime}  ensemble={ensemble_score:.3f}  {adj_label}"
        weight_str = "  ".join(
            f"{k}={weights.get(k, 0):.0%}"
            for k in ["technical", "sentiment", "whale", "orderflow", "macro"]
        )
        signal.journal.record(
            layer       = 8,
            name        = "meta_ai",
            decision    = "PASS",
            reason      = reason,
            conf_before = conf_before,
            conf_after  = signal.confidence,
            data        = {
                "regime":   regime,
                "ensemble": round(ensemble_score, 4),
                "weights":  weight_str,
                "scores":   {k: round(v, 3) for k, v in scores.items() if v is not None},
                "engines":  active_engines,
            },
        )

        logger.info(
            f"[MetaAI] {signal.asset} regime={regime}  "
            f"ensemble={ensemble_score:.3f}  {adj_label}  "
            f"conf {conf_before:.3f} → {signal.confidence:.3f}"
        )
        return signal

    # ── Score collection ──────────────────────────────────────────────────────

    def _collect_scores(
        self,
        signal:  "Signal",
        context: Dict[str, Any],
    ) -> Dict[str, Optional[float]]:
        return {
            "technical": self._technical_score(context),
            "sentiment": self._sentiment_score(signal),
            "whale":     self._whale_score(signal),
            "orderflow": self._orderflow_score(signal),
            "macro":     self._macro_score(signal, context),
        }

    @staticmethod
    def _technical_score(context: Dict) -> Optional[float]:
        ml = context.get("ml_prediction")
        if ml is None:
            return None
        return float(ml)

    @staticmethod
    def _sentiment_score(signal: "Signal") -> Optional[float]:
        raw = signal.metadata.get("sentiment_score")
        if raw is None:
            return None
        direction_sign = 1.0 if signal.direction == "BUY" else -1.0
        aligned = float(raw) * direction_sign
        return max(0.0, min(1.0, (aligned + 1.0) / 2.0))

    @staticmethod
    def _whale_score(signal: "Signal") -> Optional[float]:
        """
        Convert whale dominant + ratio → 0.0–1.0.
        Returns None for non-crypto assets (no whale data).
        """
        from core.asset_profiles import is_crypto
        if not is_crypto(signal.asset):
            return None

        dominant = signal.metadata.get("whale_dominant")
        if not dominant:
            return None
        buy_vol  = float(signal.metadata.get("whale_buy_vol",  0))
        sell_vol = float(signal.metadata.get("whale_sell_vol", 0))
        total    = buy_vol + sell_vol
        if total == 0:
            return 0.5

        bull_prob = buy_vol / total
        if signal.direction == "BUY":
            return max(0.0, min(1.0, bull_prob))
        else:
            return max(0.0, min(1.0, 1.0 - bull_prob))

    @staticmethod
    def _orderflow_score(signal: "Signal") -> Optional[float]:
        """
        Convert order flow imbalance → 0.0–1.0.
        Only valid for crypto — returns None for all other asset types.
        """
        from core.asset_profiles import is_crypto
        if not is_crypto(signal.asset):
            return None

        imbalance = signal.metadata.get("orderflow_imbalance")
        if imbalance is None:
            try:
                from order_flow import get_imbalance
                symbol = (signal.asset.replace("-USD", "USDT")
                                      .replace("/", "")
                                      .replace("-", ""))
                imbalance = get_imbalance(symbol)
            except Exception:
                return None

        direction_sign = 1.0 if signal.direction == "BUY" else -1.0
        aligned = float(imbalance) * direction_sign
        return max(0.0, min(1.0, (aligned + 1.0) / 2.0))

    @staticmethod
    def _macro_score(signal: "Signal", context: Dict) -> Optional[float]:
        """
        Convert Phase 1 macro signals → 0.0–1.0.
        Funding rates and OI are crypto-only — returns None for non-crypto.
        """
        from core.asset_profiles import is_crypto
        if not is_crypto(signal.asset):
            return None

        funding = context.get("funding_bias", "NEUTRAL")
        oi      = context.get("oi_signal",    "NEUTRAL")

        if funding == "NEUTRAL" and oi == "NEUTRAL":
            return None

        funding_map = {
            "EXTREME_LONG":  0.25,
            "HIGH_LONG":     0.40,
            "NEUTRAL":       0.50,
            "HIGH_SHORT":    0.60,
            "EXTREME_SHORT": 0.75,
        }
        oi_map = {
            "TREND_CONTINUATION": 0.65,
            "NEUTRAL":            0.50,
            "POTENTIAL_REVERSAL": 0.35,
        }
        f_score = funding_map.get(funding, 0.5)
        o_score = oi_map.get(oi, 0.5)
        return round((f_score * 0.6) + (o_score * 0.4), 4)

    # ── Ensemble calculation ──────────────────────────────────────────────────

    @staticmethod
    def _compute_ensemble(
        scores:  Dict[str, Optional[float]],
        weights: Dict[str, float],
    ) -> Tuple[float, int]:
        active_score  = 0.0
        active_weight = 0.0
        active_count  = 0

        for engine, score in scores.items():
            if score is None:
                continue
            w = weights.get(engine, 0.0)
            active_score  += score * w
            active_weight += w
            active_count  += 1

        if active_weight == 0 or active_count == 0:
            return 0.5, 0

        return round(active_score / active_weight, 4), active_count

    def _adjust_confidence(self, signal: "Signal", ensemble_score: float) -> str:
        distance = abs(ensemble_score - 0.5) * 2

        if ensemble_score >= BOOST_THRESHOLD:
            amount = round(BOOST_AMOUNT * distance, 4)
            signal.boost(amount)
            return f"+{amount:.3f} boost (strong ensemble)"

        if ensemble_score <= REDUCE_THRESHOLD:
            amount = round(REDUCE_AMOUNT * distance, 4)
            signal.reduce(amount)
            return f"-{amount:.3f} reduce (weak ensemble)"

        return NEUTRAL_ZONE_MSG
