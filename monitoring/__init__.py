"""
monitoring/__init__.py — Production observability package.

Provides:
    monitor   — SystemHealthService singleton (telemetry + alerts)
    metrics   — MetricsCollector singleton (latency tracking)
    start_monitoring() — call from bot.py to start everything

Usage in bot.py
---------------
    from monitoring import start_monitoring
    start_monitoring(telegram_bot=telegram_manager.bot)

Usage anywhere in the codebase
-------------------------------
    from monitoring import metrics, MetricsTimer, track_latency

    # Time a block
    with MetricsTimer("prediction"):
        result = predictor.predict(asset, category, df)

    # Decorator
    @track_latency("pipeline")
    def run():
        ...

    # Record an error
    metrics.record_error("layer3_regime", "ADX calculation failed")
"""
from __future__ import annotations

from monitoring.system_health_service import SystemHealthService, monitor, start_monitoring
from monitoring.metrics import (
    MetricsCollector, MetricsTimer, track_latency,
    metrics,
    PIPELINE, PREDICTION, BACKTEST, SENTIMENT,
    WHALE, ORDERFLOW, NARRATIVE, TELEGRAM,
)

__all__ = [
    "SystemHealthService", "monitor", "start_monitoring",
    "MetricsCollector", "MetricsTimer", "track_latency", "metrics",
    "PIPELINE", "PREDICTION", "BACKTEST", "SENTIMENT",
    "WHALE", "ORDERFLOW", "NARRATIVE", "TELEGRAM",
]
