"""
services/data_ingestion/__init__.py
Phase 1 — Institutional Data Aggregation Layer.

Public API:
    from services.data_ingestion import start_all, stop_all
    from services.data_ingestion import stream_manager, macro_collector
    from services.data_ingestion import liquidation_stream, funding_monitor, oi_monitor
"""
from data_ingestion.exchange_stream_manager import stream_manager
from data_ingestion.macro_data_collector    import MacroDataCollector
from data_ingestion.liquidation_stream      import LiquidationStream
from data_ingestion.funding_rate_monitor    import FundingRateMonitor
from data_ingestion.open_interest_monitor   import OpenInterestMonitor

macro_collector    = MacroDataCollector()
liquidation_stream = LiquidationStream()
funding_monitor    = FundingRateMonitor()
oi_monitor         = OpenInterestMonitor()


def start_all(exchanges=None) -> None:
    """Start every Phase 1 data feed. Call once from bot.py main()."""
    stream_manager.start(exchanges=exchanges)
    liquidation_stream.start()
    funding_monitor.start()
    oi_monitor.start()
    macro_collector.start()


def stop_all() -> None:
    """Graceful shutdown. Wire to your SIGTERM handler."""
    stream_manager.stop()


def is_running() -> bool:
    """Return True if any Phase 1 exchange streams are active."""
    return bool(stream_manager._running.is_set())