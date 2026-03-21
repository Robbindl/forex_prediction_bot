"""
whale_intelligence/__init__.py — Whale Wallet Intelligence Engine.

Upgrades existing whale tracking to full wallet behaviour analysis.
Tracks large wallets on-chain, classifies behaviour patterns,
detects coordinated cluster movements, and publishes intelligence
events to Redis for the 7-layer signal pipeline.

Redis events published
----------------------
WHALE_ACCUMULATION      — single wallet bought significant amount
WHALE_DISTRIBUTION      — single wallet sold significant amount
EXCHANGE_INFLOW_ALERT   — large transfer INTO an exchange wallet
EXCHANGE_OUTFLOW_ALERT  — large transfer OUT of an exchange wallet
WHALE_CLUSTER_ALERT     — 3+ wallets moved in the same direction within 5 min

Run tests
---------
    pytest tests/test_whale_intelligence.py -v -m "not integration"
    pytest tests/test_whale_intelligence.py -v                        # needs Redis
"""
from __future__ import annotations

from whale_intelligence.wallet_tracker            import WalletTracker
from whale_intelligence.wallet_behavior_classifier import WalletBehaviorClassifier
from whale_intelligence.wallet_database           import WalletDatabase
from whale_intelligence.wallet_cluster_analyzer   import WalletClusterAnalyzer

# ── Module-level singletons ───────────────────────────────────────────────────
_db          = WalletDatabase()
_classifier  = WalletBehaviorClassifier()
_cluster     = WalletClusterAnalyzer()
tracker      = WalletTracker(db=_db, classifier=_classifier, cluster=_cluster)


def start_all() -> None:
    """Start every Phase 2 component. Call once from bot.py main()."""
    _db.init()
    tracker.start()


def stop_all() -> None:
    """Graceful shutdown. Wire to your SIGTERM handler."""
    tracker.stop()


def is_running() -> bool:
    """Return True if Phase 2 whale tracker thread is active."""
    return bool(getattr(tracker, "_running", False))


__all__ = ["tracker", "start_all", "stop_all", "is_running",
           "WalletTracker", "WalletBehaviorClassifier",
           "WalletDatabase", "WalletClusterAnalyzer"]
