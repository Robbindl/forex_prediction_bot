from __future__ import annotations

import os
import time
from typing import Any, Dict, List, Optional, Protocol, TYPE_CHECKING

from core.signal import Signal
from core.signal_journal import PASS, KILLED, SKIPPED
from utils.logger import get_logger

# Phase 11 — latency + kill tracking
try:
    from monitoring.metrics import metrics, PIPELINE
    from monitoring.system_health_service import monitor as _monitor
    _MONITOR_OK = True
except ImportError:
    _MONITOR_OK = False

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
    After the pipeline completes, PipelineReporter runs automatically.
    """

    def __init__(self) -> None:
        self._layers: List[Any] = []
        self._loaded  = False

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
            from layers.layer8_meta_ai     import MetaAILayer

            self._layers = [
                VotingLayer(),
                QualityLayer(),
                RegimeLayer(),
                SessionLayer(),
                SentimentLayer(),
                WhaleLayer(),
                CalibrationLayer(),
                MetaAILayer(),
            ]
            self._loaded = True
            logger.info(f"[Pipeline] Loaded {len(self._layers)} layers")
        except Exception as e:
            logger.error(f"[Pipeline] Layer load failed: {e}")
            self._layers = []
            self._loaded = True

    def run(self, signal: Signal, context: Optional[Dict[str, Any]] = None) -> Optional[Signal]:
        """
        Run signal through all layers.
        Records every decision in signal.journal.
        Calls PipelineReporter after completion (pass or fail).
        Returns the (possibly modified) Signal if it survives, else None.
        """
        self._lazy_load()

        if context is None:
            context = {}

        context.setdefault("pipeline_start", time.monotonic())
        killed_at_layer = None

        for i, layer in enumerate(self._layers, start=1):
            conf_before = signal.confidence
            t0          = time.monotonic()

            try:
                result = layer.process(signal, context)
                elapsed_ms = (time.monotonic() - t0) * 1000

                if result is None:
                    # Layer returned None — mark killed but continue to record all layers.
                    if signal.alive:
                        signal.kill(f"Layer {i} ({layer.name}) returned None", i)
                    signal.journal.record(
                        layer=i, name=layer.name, decision=KILLED,
                        reason=signal.kill_reason or f"Layer {i} returned None",
                        conf_before=conf_before,
                        conf_after=signal.confidence,
                        elapsed_ms=elapsed_ms,
                    )
                    logger.log_pipeline(signal.asset, i, "KILLED", f"layer={layer.name}")
                    killed_at_layer = i

                else:
                    signal = result
                    signal.layer_reached = i
                    decision = PASS

                    signal.journal.record(
                        layer=i, name=layer.name, decision=decision,
                        reason=signal.metadata.get(f"l{i}_reason", ""),
                        conf_before=conf_before,
                        conf_after=signal.confidence,
                        data=signal.metadata.get(f"l{i}_data", {}),
                        elapsed_ms=elapsed_ms,
                    )
                    logger.log_pipeline(
                        signal.asset, i, "PASS",
                        f"conf={signal.confidence:.3f} layer={layer.name}"
                    )
            except Exception as e:
                elapsed_ms = (time.monotonic() - t0) * 1000
                logger.error(f"[Pipeline] Layer {i} ({layer.name}) raised: {e}", exc_info=True)
                if signal.alive:
                    signal.kill(f"Layer {i} exception: {e}", i)
                signal.journal.record(
                    layer=i, name=layer.name, decision=KILLED,
                    reason=f"exception: {e}",
                    conf_before=conf_before,
                    conf_after=signal.confidence,
                    elapsed_ms=elapsed_ms,
                )
                killed_at_layer = i
                # Continue to record other layers while preserving kill state.


        elapsed_ms = (time.monotonic() - context["pipeline_start"]) * 1000

        # DEBUG override to force survival for verification.
        if os.getenv("DEBUG_FORCE_SURVIVE", "0") == "1":
            if not signal.alive:
                signal.alive = True
                signal.kill_reason = "Forced survive via DEBUG_FORCE_SURVIVE"
                signal.journal.record(
                    layer=len(self._layers),
                    name="debug_force",
                    decision=PASS,
                    reason="Forced survive",
                    conf_before=signal.confidence,
                    conf_after=signal.confidence,
                )

        if signal.alive:
            logger.info(
                f"[Pipeline] {signal.asset} SURVIVED all {len(self._layers)} layers "
                f"conf={signal.confidence:.3f} ({elapsed_ms:.0f}ms)"
            )
        else:
            logger.debug(
                f"[Pipeline] {signal.asset} KILLED at L{signal.layer_reached}: "
                f"{signal.kill_reason} ({elapsed_ms:.0f}ms)"
            )

        # Phase 11 — record latency + kill stats
        if _MONITOR_OK:
            try:
                metrics.record(PIPELINE, elapsed_ms, success=signal.alive)
                _monitor.record_pipeline_latency(elapsed_ms)
                _monitor.record_signal(
                    signal.asset, signal.direction, signal.alive
                )
                if not signal.alive and signal.kill_reason:
                    layer_name = self._layers[signal.layer_reached - 1].name \
                        if 0 < signal.layer_reached <= len(self._layers) else "unknown"
                    _monitor.record_kill(layer_name)
            except Exception:
                pass

        # ── Post-pipeline: backtest + Telegram + DB ───────────────────────────
        try:
            from core.pipeline_reporter import reporter
            signal = reporter.report(signal, context)
        except Exception as e:
            logger.debug(f"[Pipeline] reporter error: {e}")

        return signal if signal.alive else None

    def run_batch(
        self,
        signals: List[Signal],
        context: Optional[Dict[str, Any]] = None,
    ) -> List[Signal]:
        """Run a list of signals. Returns only the survivors."""
        survivors = []
        for sig in signals:
            ctx    = dict(context or {})
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