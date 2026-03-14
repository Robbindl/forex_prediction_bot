"""Layer 1 — Strategy voting gate. Kills signals below minimum confidence."""
from __future__ import annotations
from typing import Any, Dict, Optional
from core.signal import Signal
from utils.logger import get_logger
from config.config import MIN_CONFIDENCE_SCORE

logger = get_logger()
LAYER = 1


class VotingLayer:
    name = "voting"

    def process(self, signal: Signal, context: Dict[str, Any]) -> Optional[Signal]:
        if signal.confidence < MIN_CONFIDENCE_SCORE:
            signal.kill(f"Confidence {signal.confidence:.3f} < {MIN_CONFIDENCE_SCORE}", LAYER)
            return None

        # Boost if ML agrees
        ml_pred = context.get("ml_prediction")
        if ml_pred is not None:
            ml_direction = "BUY" if ml_pred > 0.5 else "SELL"
            if ml_direction == signal.direction:
                signal.boost(0.05)
                logger.log_pipeline(signal.asset, LAYER, "ML_AGREE", f"ml={ml_pred:.3f}")
            else:
                signal.reduce(0.05)

        logger.log_pipeline(signal.asset, LAYER, "PASS", f"conf={signal.confidence:.3f}")
        return signal