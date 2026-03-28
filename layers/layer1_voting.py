from __future__ import annotations
from typing import Any, Dict, Optional
from core.signal import Signal
from core.signal_journal import PASS
from utils.logger import get_logger
from config.config import MIN_CONFIDENCE_SCORE

logger = get_logger()
LAYER  = 1


class VotingLayer:
    name = "voting"

    def process(self, signal: Signal, context: Dict[str, Any]) -> Optional[Signal]:
        conf_before = signal.confidence

        # ── Minimum confidence gate (soft — reduces, does not kill) ───────
        if signal.confidence < MIN_CONFIDENCE_SCORE:
            reason = f"conf {signal.confidence:.3f} below minimum {MIN_CONFIDENCE_SCORE}"
            signal.reduce(0.08)
            signal.journal.record(
                layer=LAYER, name=self.name, decision=PASS,
                reason=reason,
                conf_before=conf_before, conf_after=signal.confidence,
            )
            logger.log_pipeline(signal.asset, LAYER, "LOW_CONF", reason)

        # ── ML agreement boost/reduce ─────────────────────────────────────
        ml_pred = context.get("ml_prediction")
        ml_conf = context.get("ml_confidence", 0.0)
        ml_note = ""

        if ml_pred is not None and ml_conf > 0.1:
            signal.metadata["ml_prediction_real"] = True

            ml_direction = "BUY" if ml_pred > 0.5 else "SELL"
            if ml_direction == signal.direction:
                signal.boost(0.05)
                ml_note = f"ML agrees (pred={ml_pred:.3f}) +0.05"
                logger.log_pipeline(signal.asset, LAYER, "ML_AGREE", f"ml={ml_pred:.3f}")
            else:
                signal.reduce(0.05)
                ml_note = f"ML disagrees (pred={ml_pred:.3f}) -0.05"
        else:
            signal.metadata["ml_prediction_real"] = False
            logger.debug(f"[VotingLayer] No real ML prediction for {signal.asset} — skipping ML vote")

        reason = f"conf {conf_before:.3f} → {signal.confidence:.3f}"
        if ml_note:
            reason += f"  {ml_note}"

        signal.journal.record(
            layer=LAYER, name=self.name, decision=PASS,
            reason=reason,
            conf_before=conf_before, conf_after=signal.confidence,
            data={
                "ml_prediction":      round(ml_pred, 3) if ml_pred is not None else None,
                "ml_prediction_real": signal.metadata["ml_prediction_real"],
                "ml_confidence":      round(ml_conf, 3),
                "strategy_id":        signal.strategy_id,
                "votes":              signal.indicators.get("votes", 1),
            },
        )
        logger.log_pipeline(signal.asset, LAYER, "PASS", f"conf={signal.confidence:.3f}")
        return signal
