"""
monitoring/metrics.py — Performance metrics and latency tracker.

Provides lightweight decorators and context managers that instrument
any function in the codebase for latency tracking.

Every metric flows through SystemHealthService for storage and alerting.

Usage
-----
    from monitoring.metrics import track_latency, MetricsTimer, metrics

    # Decorator — wraps any function
    @track_latency("pipeline")
    def run_pipeline(signal, context):
        ...

    # Context manager — wrap a block
    with MetricsTimer("prediction") as t:
        result = predictor.predict(asset, category, df)
    # t.elapsed_ms is available after the with block

    # Manual recording
    metrics.record("phase3_imbalance", elapsed_ms=12.4)

    # Get a summary
    print(metrics.summary())
    # {
    #   "pipeline":   {"avg_ms": 320, "p95_ms": 890, "count": 142},
    #   "prediction": {"avg_ms": 12,  "p95_ms": 45,  "count": 142},
    # }

Predefined metric names
-----------------------
    pipeline          — full L1-L8 pipeline execution
    prediction        — ML predictor call
    backtest          — auto-backtest in PipelineReporter
    sentiment_fetch   — SentimentAnalyzer API call
    whale_fetch       — WhaleAlertManager fetch
    orderflow_update  — order book processing
    narrative_ingest  — narrative AI text ingestion
    telegram_send     — Telegram message delivery
"""
from __future__ import annotations

import functools
import time
from collections import defaultdict, deque
from typing import Any, Callable, Dict, Optional

# ── Predefined metric names ───────────────────────────────────────────────────
PIPELINE      = "pipeline"
PREDICTION    = "prediction"
BACKTEST      = "backtest"
SENTIMENT     = "sentiment_fetch"
WHALE         = "whale_fetch"
ORDERFLOW     = "orderflow_update"
NARRATIVE     = "narrative_ingest"
TELEGRAM      = "telegram_send"
LAYER_PREFIX  = "layer_"   # used as layer_1, layer_2, ... layer_8


class MetricsCollector:
    """
    Thread-safe metrics store. Collects latency samples per metric name.
    Keeps last 500 samples per metric — enough for P95 calculations.
    """

    def __init__(self, window: int = 500) -> None:
        self._window  = window
        self._samples: Dict[str, deque] = defaultdict(lambda: deque(maxlen=window))
        self._counts:  Dict[str, int]   = defaultdict(int)
        self._errors:  Dict[str, int]   = defaultdict(int)

    # ── Recording ─────────────────────────────────────────────────────────────

    def record(self, name: str, elapsed_ms: float, success: bool = True) -> None:
        """Record one latency sample for a metric."""
        self._samples[name].append(elapsed_ms)
        self._counts[name] += 1
        if not success:
            self._errors[name] += 1
        # Forward to SystemHealthService if available
        self._forward(name, elapsed_ms)

    def record_error(self, name: str, message: str) -> None:
        """Record an error for a metric."""
        self._errors[name] += 1
        try:
            from monitoring.system_health_service import monitor
            monitor.record_error(name, message)
        except Exception:
            pass

    # ── Retrieval ─────────────────────────────────────────────────────────────

    def get(self, name: str) -> Dict[str, float]:
        """Get stats for one metric."""
        samples = list(self._samples.get(name, []))
        if not samples:
            return {"avg_ms": 0.0, "p95_ms": 0.0, "max_ms": 0.0,
                    "min_ms": 0.0, "count": 0, "errors": self._errors.get(name, 0)}
        s = sorted(samples)
        return {
            "avg_ms":  round(sum(s) / len(s), 2),
            "p95_ms":  round(s[int(len(s) * 0.95)], 2),
            "max_ms":  round(max(s), 2),
            "min_ms":  round(min(s), 2),
            "count":   self._counts.get(name, len(s)),
            "errors":  self._errors.get(name, 0),
        }

    def summary(self) -> Dict[str, Dict]:
        """Get stats for all recorded metrics."""
        return {name: self.get(name) for name in self._samples}

    def all_names(self) -> list:
        return list(self._samples.keys())

    def reset(self, name: Optional[str] = None) -> None:
        """Clear samples — optionally for one metric only."""
        if name:
            self._samples.pop(name, None)
            self._counts.pop(name, None)
            self._errors.pop(name, None)
        else:
            self._samples.clear()
            self._counts.clear()
            self._errors.clear()

    # ── Forwarding ────────────────────────────────────────────────────────────

    def _forward(self, name: str, elapsed_ms: float) -> None:
        """Forward specific metrics to SystemHealthService."""
        try:
            from monitoring.system_health_service import monitor
            if name == PIPELINE:
                monitor.record_pipeline_latency(elapsed_ms)
            elif name == PREDICTION:
                monitor.record_prediction_latency(elapsed_ms)
        except Exception:
            pass


# ── Context manager ───────────────────────────────────────────────────────────

class MetricsTimer:
    """
    Context manager that times a block and records the latency.

    Usage:
        with MetricsTimer("prediction") as t:
            result = predictor.predict(...)
        print(f"Took {t.elapsed_ms:.1f}ms")
    """

    def __init__(self, name: str, collector: Optional[MetricsCollector] = None) -> None:
        self._name      = name
        self._collector = collector or _global_metrics
        self._start:    float = 0.0
        self.elapsed_ms: float = 0.0
        self.success:   bool   = True

    def __enter__(self) -> "MetricsTimer":
        self._start = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        self.elapsed_ms = (time.perf_counter() - self._start) * 1000
        self.success    = exc_type is None
        self._collector.record(self._name, self.elapsed_ms, self.success)
        return False   # don't suppress exceptions


# ── Decorator ─────────────────────────────────────────────────────────────────

def track_latency(
    name: str,
    collector: Optional[MetricsCollector] = None,
) -> Callable:
    """
    Decorator that times a function and records its latency.

    Usage:
        @track_latency("pipeline")
        def run_pipeline(signal, context):
            ...
    """
    def decorator(fn: Callable) -> Callable:
        @functools.wraps(fn)
        def wrapper(*args, **kwargs) -> Any:
            col = collector or _global_metrics
            t0  = time.perf_counter()
            try:
                result = fn(*args, **kwargs)
                col.record(name, (time.perf_counter() - t0) * 1000, success=True)
                return result
            except Exception as e:
                col.record(name, (time.perf_counter() - t0) * 1000, success=False)
                raise
        return wrapper
    return decorator


# ── Global singleton ──────────────────────────────────────────────────────────
_global_metrics = MetricsCollector()
metrics = _global_metrics
