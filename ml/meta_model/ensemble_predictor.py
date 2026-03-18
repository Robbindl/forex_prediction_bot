from __future__ import annotations

import threading
from typing import Any, Dict, Optional, Tuple, TYPE_CHECKING

from utils.logger import get_logger

if TYPE_CHECKING:
    from core.signal import Signal
    from ml.meta_model.market_condition_classifier import MarketConditionClassifier
    from ml.meta_model.model_weighting_engine      import ModelWeightingEngine

logger = get_logger()

# ── Thresholds ────────────────────────────────────────────────────────────────
BOOST_THRESHOLD  = 0.65   # ensemble above this → boost confidence
REDUCE_THRESHOLD = 0.35   # ensemble below this → reduce confidence
BOOST_AMOUNT     = 0.04   # max boost per signal
REDUCE_AMOUNT    = 0.04   # max reduction per signal
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

    # ── Public API ────────────────────────────────────────────────────────────

    def process(
        self,
        signal:  "Signal",
        context: Dict[str, Any],
    ) -> "Signal":
        """
        Main entry point. Called as Layer 8 in the pipeline.
        Reads signal + context, adjusts confidence, writes to journal.
        Returns the (possibly adjusted) signal.
        """
        conf_before = signal.confidence

        # 1. Detect regime
        regime  = self._classifier.classify_from_context(context)
        weights = self._weighter.get_weights(regime)

        # 2. Collect scores from all engines
        scores  = self._collect_scores(signal, context)

        # 3. Compute weighted ensemble score
        ensemble_score, active_engines = self._compute_ensemble(scores, weights)

        # 4. Adjust confidence
        adj_label = self._adjust_confidence(signal, ensemble_score)

        # 5. Store in metadata for downstream use (PipelineReporter etc.)
        signal.metadata["meta_ai_regime"]   = regime
        signal.metadata["meta_ai_ensemble"] = round(ensemble_score, 4)
        signal.metadata["meta_ai_weights"]  = weights
        signal.metadata["meta_ai_scores"]   = {
            k: round(v, 3) for k, v in scores.items() if v is not None
        }

        # 6. Write to journal → appears in Telegram automatically
        reason = (
            f"regime={regime}  ensemble={ensemble_score:.3f}  {adj_label}"
        )
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
                "regime":    regime,
                "ensemble":  round(ensemble_score, 4),
                "weights":   weight_str,
                "scores":    {k: round(v, 3) for k, v in scores.items() if v is not None},
                "engines":   active_engines,
            },
        )

        logger.info(
            f"[MetaAI] {signal.asset} regime={regime}  "
            f"ensemble={ensemble_score:.3f}  {adj_label}  "
            f"conf {conf_before:.3f} → {signal.confidence:.3f}"
        )
        return signal

    # ── Score collection ─────────────────────────────────────────────────────

    def _collect_scores(
        self,
        signal:  "Signal",
        context: Dict[str, Any],
    ) -> Dict[str, Optional[float]]:
        """
        Pull score from each engine. Returns dict of engine → 0.0–1.0 score.
        None means engine had no data — excluded from ensemble.
        """
        return {
            "technical":  self._technical_score(context),
            "sentiment":  self._sentiment_score(signal),
            "whale":      self._whale_score(signal),
            "orderflow":  self._orderflow_score(signal),
            "macro":      self._macro_score(context),
        }

    @staticmethod
    def _technical_score(context: Dict) -> Optional[float]:
        """
        ML predictor probability (already 0.0–1.0).
        Already in context from core/engine.py.
        """
        ml = context.get("ml_prediction")
        if ml is None:
            return None
        return float(ml)

    @staticmethod
    def _sentiment_score(signal: "Signal") -> Optional[float]:
        """
        Convert sentiment_score (-1 to +1) → probability (0 to 1).
        Direction-aligned: positive for BUY means bullish sentiment.
        """
        raw = signal.metadata.get("sentiment_score")
        if raw is None:
            return None
        # Align to signal direction
        direction_sign = 1.0 if signal.direction == "BUY" else -1.0
        aligned = float(raw) * direction_sign
        # Convert -1…+1 to 0…1
        return max(0.0, min(1.0, (aligned + 1.0) / 2.0))

    @staticmethod
    def _whale_score(signal: "Signal") -> Optional[float]:
        """
        Convert whale dominant + ratio → 0.0–1.0.
        Aligned to signal direction.
        """
        dominant = signal.metadata.get("whale_dominant")
        if not dominant:
            return None
        buy_vol  = float(signal.metadata.get("whale_buy_vol",  0))
        sell_vol = float(signal.metadata.get("whale_sell_vol", 0))
        total    = buy_vol + sell_vol
        if total == 0:
            return 0.5   # neutral

        ratio     = max(buy_vol, sell_vol) / total   # 0.5–1.0
        bull_prob = buy_vol / total                   # 0.0–1.0

        # Direction-align: BUY signal wants high bull_prob
        if signal.direction == "BUY":
            return max(0.0, min(1.0, bull_prob))
        else:
            return max(0.0, min(1.0, 1.0 - bull_prob))

    @staticmethod
    def _orderflow_score(signal: "Signal") -> Optional[float]:
        """
        Convert order flow imbalance (-1 to +1) → 0.0–1.0.
        Reads from Phase 3 via signal.metadata.
        """
        imbalance = signal.metadata.get("orderflow_imbalance")
        if imbalance is None:
            # Try fetching live from Phase 3
            try:
                from order_flow import get_imbalance
                asset = (signal.asset.replace("-USD", "USDT")
                                     .replace("/", "")
                                     .replace("-", ""))
                imbalance = get_imbalance(asset)
            except Exception:
                return None

        direction_sign = 1.0 if signal.direction == "BUY" else -1.0
        aligned = float(imbalance) * direction_sign
        return max(0.0, min(1.0, (aligned + 1.0) / 2.0))

    @staticmethod
    def _macro_score(context: Dict) -> Optional[float]:
        """
        Convert Phase 1 macro signals → 0.0–1.0.
        Uses funding bias and OI signal from context.
        """
        funding = context.get("funding_bias", "NEUTRAL")
        oi      = context.get("oi_signal",    "NEUTRAL")

        # If no Phase 1 data in context, skip
        if funding == "NEUTRAL" and oi == "NEUTRAL":
            return None

        score = 0.5   # start neutral
        funding_map = {
            "EXTREME_LONG":  0.25,   # contrarian — squeeze risk
            "HIGH_LONG":     0.40,
            "NEUTRAL":       0.50,
            "HIGH_SHORT":    0.60,
            "EXTREME_SHORT": 0.75,   # contrarian — squeeze opportunity
        }
        oi_map = {
            "TREND_CONTINUATION": 0.65,
            "NEUTRAL":            0.50,
            "POTENTIAL_REVERSAL": 0.35,
        }
        f_score = funding_map.get(funding, 0.5)
        o_score = oi_map.get(oi, 0.5)
        score   = (f_score * 0.6) + (o_score * 0.4)
        return round(score, 4)

    # ── Ensemble calculation ──────────────────────────────────────────────────

    @staticmethod
    def _compute_ensemble(
        scores:  Dict[str, Optional[float]],
        weights: Dict[str, float],
    ) -> Tuple[float, int]:
        """
        Weighted average of active engine scores.
        Returns (ensemble_score, number_of_active_engines).
        """
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
            return 0.5, 0   # neutral when no engines active

        return round(active_score / active_weight, 4), active_count

    def _adjust_confidence(self, signal: "Signal", ensemble_score: float) -> str:
        """
        Apply confidence adjustment based on ensemble score.
        Returns label for journal/logging.
        """
        # Scale boost/reduce by how far from neutral (0.5) the score is
        distance = abs(ensemble_score - 0.5) * 2   # 0.0–1.0

        if ensemble_score >= BOOST_THRESHOLD:
            amount = round(BOOST_AMOUNT * distance, 4)
            signal.boost(amount)
            return f"+{amount:.3f} boost (strong ensemble)"

        if ensemble_score <= REDUCE_THRESHOLD:
            amount = round(REDUCE_AMOUNT * distance, 4)
            signal.reduce(amount)
            return f"-{amount:.3f} reduce (weak ensemble)"

        return NEUTRAL_ZONE_MSG