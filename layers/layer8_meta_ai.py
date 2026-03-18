from __future__ import annotations

from typing import Any, Dict, Optional

from core.signal import Signal
from utils.logger import get_logger

logger = get_logger()
LAYER = 8


class MetaAILayer:
    name = "meta_ai"

    def __init__(self) -> None:
        # Lazy-load to avoid circular imports
        self._predictor = None

    def _get_predictor(self):
        if self._predictor is None:
            from ml.meta_model import predictor
            self._predictor = predictor
        return self._predictor

    def process(self, signal: Signal, context: Dict[str, Any]) -> Optional[Signal]:
        try:
            predictor = self._get_predictor()
            signal    = predictor.process(signal, context)
            return signal
        except Exception as e:
            # Never kill a signal due to Meta AI errors —
            # it's an enrichment layer, not a gate.
            logger.warning(f"[MetaAI] Layer error for {signal.asset}: {e}")
            signal.journal.record(
                layer       = LAYER,
                name        = self.name,
                decision    = "INFO",
                reason      = f"meta AI unavailable: {e}",
                conf_before = signal.confidence,
                conf_after  = signal.confidence,
            )
            return signal