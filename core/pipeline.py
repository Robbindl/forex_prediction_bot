"""
core/pipeline.py — Seven-layer signal pipeline.

Flow:
    Signal → L1(Voting) → L2(Quality) → L3(Regime) → L4(Session)
           → L5(Sentiment) → L6(Whale) → L7(Calibration) → execute | discard

Each layer implements process(signal, context) → Signal | None.
Returning None is equivalent to signal.kill(). Both are handled.
"""
from __future__ import annotations

import time
from typing import Any, Dict, List, Optional, Protocol, TYPE_CHECKING

from core.signal import Signal
from utils.logger import get_logger

if TYPE_CHECKING:
    pass

logger = get_logger()


class Layer(Protocol):
    """Interface every pipeline layer must satisfy."""
    name: str
    def process(self, signal: Signal, context: Dict[str, Any]) -> Optional[Signal]:
        ...


class Pipeline:
    """
    Ordered chain of layers. Each layer may modify or kill the signal.
    Context dict is shared across all layers in one pass.
    """

    def __init__(self) -> None:
        self._layers: List[Any] = []
        self._loaded = False

    def _lazy_load(self) -> None:
        """Import layers at first use to avoid circular imports at startup."""
        if self._loaded:
            return
        try:
            from layers.layer1_voting      import VotingLayer
            from layers.layer2_quality     import QualityLayer
            from layers.layer3_regime      import RegimeLayer
            from layers.layer4_session     import SessionLayer
            from layers.layer5_sentiment   import SentimentLayer
            from layers.layer6_whale       import WhaleLayer
            from layers.layer7_calibration import CalibrationLayer

            self._layers = [
                VotingLayer(),
                QualityLayer(),
                RegimeLayer(),
                SessionLayer(),
                SentimentLayer(),
                WhaleLayer(),
                CalibrationLayer(),
            ]
            self._loaded = True
            logger.info(f"[Pipeline] Loaded {len(self._layers)} layers")
        except Exception as e:
            logger.error(f"[Pipeline] Layer load failed: {e}")
            self._layers = []
            self._loaded = True   # prevent infinite retry — run with empty layers

    def run(self, signal: Signal, context: Optional[Dict[str, Any]] = None) -> Optional[Signal]:
        """
        Run signal through all layers.
        Returns the (possibly modified) Signal if it survives, else None.
        """
        self._lazy_load()

        if context is None:
            context = {}

        context.setdefault("pipeline_start", time.monotonic())

        for i, layer in enumerate(self._layers, start=1):
            if not signal.alive:
                logger.log_pipeline(signal.asset, i, "SKIP", "signal already dead")
                break
            try:
                result = layer.process(signal, context)

                # Layer returned None — treat as kill
                if result is None:
                    signal.kill(f"Layer {i} ({layer.name}) returned None", i)
                    logger.log_pipeline(signal.asset, i, "KILLED", f"layer={layer.name}")
                    break

                # Layer returned a signal — use it (may be same object or new)
                signal = result
                signal.layer_reached = i

                logger.log_pipeline(
                    signal.asset, i, "PASS",
                    f"conf={signal.confidence:.3f} layer={layer.name}"
                )

            except Exception as e:
                logger.error(f"[Pipeline] Layer {i} ({layer.name}) raised: {e}", exc_info=True)
                signal.kill(f"Layer {i} exception: {e}", i)
                break

        elapsed_ms = (time.monotonic() - context["pipeline_start"]) * 1000
        if signal.alive:
            logger.info(
                f"[Pipeline] {signal.asset} SURVIVED all {len(self._layers)} layers "
                f"conf={signal.confidence:.3f} ({elapsed_ms:.0f}ms)"
            )
            return signal
        else:
            logger.debug(
                f"[Pipeline] {signal.asset} KILLED at L{signal.layer_reached}: "
                f"{signal.kill_reason} ({elapsed_ms:.0f}ms)"
            )
            return None

    def run_batch(
        self,
        signals: List[Signal],
        context: Optional[Dict[str, Any]] = None,
    ) -> List[Signal]:
        """Run a list of signals. Returns only the survivors."""
        survivors = []
        for sig in signals:
            ctx = dict(context or {})
            result = self.run(sig, ctx)
            if result is not None:
                survivors.append(result)
        return survivors

    @property
    def layer_names(self) -> List[str]:
        self._lazy_load()
        return [l.name for l in self._layers]

    def __len__(self) -> int:
        self._lazy_load()
        return len(self._layers)


# ── Global singleton ──────────────────────────────────────────────────────────
pipeline = Pipeline()