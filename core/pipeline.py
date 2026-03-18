"""
core/pipeline.py — 8-layer signal pipeline with data integrity gating.

Changes vs original:
  - Data integrity gate: after all layers run, counts how many sources
    provided REAL data.  If fewer than profile.min_valid_layers sources
    contributed, the signal is killed with reason "Insufficient real data".
  - All exception handlers log errors (no silent pass).
  - DEBUG_FORCE_SURVIVE env var still works for testing.
"""
from __future__ import annotations

import os
import time
from typing import Any, Dict, List, Optional, Protocol, TYPE_CHECKING

from core.signal import Signal
from core.signal_journal import PASS, KILLED, SKIPPED
from core.asset_profiles import get_profile
from utils.logger import get_logger

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
    name: str
    def process(self, signal: Signal, context: Dict[str, Any]) -> Optional[Signal]: ...


def _count_valid_sources(signal: Signal) -> int:
    """
    Count how many intelligence sources provided REAL data for this signal.
    Based on metadata flags set by each layer.
    """
    count = 0

    # Layer 1: ML prediction (always present or fallback)
    if signal.metadata.get("ml_prediction_real", True):
        count += 1

    # Layer 3: regime is always computable from price data
    if signal.metadata.get("regime") not in (None, "unknown"):
        count += 1

    # Layer 5: sentiment sources
    sources_applied = signal.metadata.get("sentiment_sources", [])
    if sources_applied:
        count += 1   # at least one sentiment source contributed

    # Layer 6: whale data (crypto only — skipped = not penalised)
    whale_data = signal.metadata.get("whale_data")
    if whale_data == "real":
        count += 1
    elif signal.metadata.get("whale_skipped"):
        count += 1   # intentionally skipped — not a missing source

    # Layer 8: meta AI
    if signal.metadata.get("meta_ai_ensemble") is not None:
        count += 1

    # Order flow (crypto only)
    if signal.metadata.get("orderflow_applicable") is True:
        if signal.metadata.get("orderflow_imbalance", 0.0) != 0.0:
            count += 1
    elif signal.metadata.get("orderflow_applicable") is False:
        count += 1   # not applicable — not penalised

    return count


class Pipeline:
    """
    Ordered chain of 8 layers.  After all layers run, a data integrity check
    ensures the signal was backed by sufficient real intelligence sources.
    """

    def __init__(self) -> None:
        self._layers: List[Any] = []
        self._loaded = False

    def _lazy_load(self) -> None:
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
        self._lazy_load()

        if context is None:
            context = {}

        context.setdefault("pipeline_start", time.monotonic())

        for i, layer in enumerate(self._layers, start=1):
            conf_before = signal.confidence
            t0 = time.monotonic()

            try:
                result     = layer.process(signal, context)
                elapsed_ms = (time.monotonic() - t0) * 1000

                if result is None:
                    if signal.alive:
                        signal.kill(f"Layer {i} ({layer.name}) returned None", i)
                    signal.journal.record(
                        layer=i, name=layer.name, decision=KILLED,
                        reason=signal.kill_reason or f"Layer {i} returned None",
                        conf_before=conf_before, conf_after=signal.confidence,
                        elapsed_ms=elapsed_ms,
                    )
                    logger.log_pipeline(signal.asset, i, "KILLED", f"layer={layer.name}")
                else:
                    signal = result
                    signal.layer_reached = i
                    signal.journal.record(
                        layer=i, name=layer.name, decision=PASS,
                        reason=signal.metadata.get(f"l{i}_reason", ""),
                        conf_before=conf_before, conf_after=signal.confidence,
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
                    conf_before=conf_before, conf_after=signal.confidence,
                    elapsed_ms=elapsed_ms,
                )

        # ── Data integrity gate ───────────────────────────────────────────────
        if signal.alive:
            profile       = get_profile(signal.asset)
            valid_sources = _count_valid_sources(signal)
            min_required  = profile.min_valid_layers

            signal.metadata["valid_sources_count"] = valid_sources
            signal.metadata["min_sources_required"] = min_required

            if valid_sources < min_required:
                reason = (
                    f"Insufficient real data: {valid_sources}/{min_required} "
                    f"sources for {signal.asset} ({signal.category})"
                )
                signal.kill(reason, len(self._layers))
                signal.journal.record(
                    layer=len(self._layers), name="data_integrity_gate",
                    decision=KILLED,
                    reason=reason,
                    conf_before=signal.confidence, conf_after=signal.confidence,
                    data={"valid_sources": valid_sources,
                          "min_required": min_required,
                          "category": signal.category},
                )
                logger.warning(f"[Pipeline] DATA INTEGRITY KILL — {signal.asset}: {reason}")

        elapsed_ms = (time.monotonic() - context["pipeline_start"]) * 1000

        # DEBUG override
        if os.getenv("DEBUG_FORCE_SURVIVE", "0") == "1" and not signal.alive:
            signal.alive      = True
            signal.kill_reason = "Forced survive via DEBUG_FORCE_SURVIVE"

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

        if _MONITOR_OK:
            try:
                metrics.record(PIPELINE, elapsed_ms, success=signal.alive)
                _monitor.record_pipeline_latency(elapsed_ms)
                _monitor.record_signal(signal.asset, signal.direction, signal.alive)
                if not signal.alive and signal.kill_reason:
                    layer_name = (
                        self._layers[signal.layer_reached - 1].name
                        if 0 < signal.layer_reached <= len(self._layers)
                        else "unknown"
                    )
                    _monitor.record_kill(layer_name)
            except Exception as e:
                logger.error(f"[Pipeline] Monitoring record failed: {e}")

        try:
            from core.pipeline_reporter import reporter
            signal = reporter.report(signal, context)
        except Exception as e:
            logger.error(f"[Pipeline] Reporter error: {e}")

        return signal if signal.alive else None

    def run_batch(
        self,
        signals: List[Signal],
        context: Optional[Dict[str, Any]] = None,
    ) -> List[Signal]:
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


pipeline = Pipeline()
