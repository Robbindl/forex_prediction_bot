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

    # FIX S3: Wire wallet_tracker → layer6_whale.ingest_onchain_event so that
    # on-chain movements actually reach the trading pipeline.
    # Previously wallet_tracker published WHALE_ACCUMULATION/DISTRIBUTION to
    # Redis but nobody subscribed and called ingest_onchain_event() — the
    # _ONCHAIN_CACHE in layer6 was permanently empty, meaning Phase 2
    # on-chain intelligence had zero effect on trading decisions.
    try:
        from layers.layer6_whale import ingest_onchain_event as _ingest
        original_publish = tracker._publish_movement

        def _patched_publish(wallet, delta, asset, new_balance):
            # Call original Redis publish path first
            original_publish(wallet, delta, asset, new_balance)
            # Also feed layer6 directly so we don't depend on Redis round-trip
            try:
                is_buy     = delta > 0
                wallet_type = wallet.get("type", "unknown")
                if wallet_type == "exchange":
                    event_type = "EXCHANGE_INFLOW_ALERT" if is_buy else "EXCHANGE_OUTFLOW_ALERT"
                else:
                    event_type = "WHALE_ACCUMULATION" if is_buy else "WHALE_DISTRIBUTION"
                _ingest({
                    "type":        event_type,
                    "asset":       asset,
                    "label":       wallet.get("label", "Unknown Whale"),
                    "wallet_type": wallet_type,
                    "delta":       round(delta, 4),
                    "new_balance": round(new_balance, 4),
                    "source":      "on-chain",
                })
            except Exception as _e:
                pass  # layer6 feed is best-effort

        tracker._publish_movement = _patched_publish  # type: ignore[method-assign]
    except Exception as _wire_err:
        pass  # layer6 not yet available — on-chain cache stays empty until first pipeline run

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
