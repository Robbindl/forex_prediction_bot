"""
whale_intelligence/__init__.py — Whale Wallet Intelligence Engine.

Upgrades existing whale tracking to full wallet behaviour analysis.
Tracks large wallets on-chain, classifies behaviour patterns,
detects coordinated cluster movements, and publishes intelligence
events to Redis for the signal decision engine.

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
from whale_intelligence.multi_chain_tracker       import MultiChainTracker
from whale_intelligence.multi_chain_seeds         import MULTI_CHAIN_SEED_WALLETS

# ── Module-level singletons ───────────────────────────────────────────────────
_db          = WalletDatabase()
_classifier  = WalletBehaviorClassifier()
_cluster     = WalletClusterAnalyzer()
tracker      = WalletTracker(db=_db, classifier=_classifier, cluster=_cluster)
multi_tracker = MultiChainTracker()  # Tracks BTC, ETH, BNB, SOL, XRP


def start_all() -> None:
    """Start every Phase 2 component. Call once from bot.py main()."""
    _db.init()

    # Load multi-chain seed wallets into appropriate trackers
    logger = __import__('utils.logger', fromlist=['get_logger']).get_logger()
    for wallet in MULTI_CHAIN_SEED_WALLETS:
        try:
            chain = wallet.get("chain", "").lower()
            # ETH/BTC go to main tracker, others go to multi_tracker
            if chain in ("eth", "btc"):
                tracker.add_wallet(
                    address=wallet["address"],
                    label=wallet["label"],
                    chain=chain,
                    wallet_type=wallet.get("type", "unknown"),
                )
            elif chain in ("bnb", "sol", "solana", "xrp"):
                multi_tracker.add_wallet(
                    address=wallet["address"],
                    label=wallet["label"],
                    chain=chain,
                    wallet_type=wallet.get("type", "unknown"),
                )
        except Exception as e:
            logger.warning(f"[whale_intelligence] Failed to add seed wallet {wallet.get('label')}: {e}")

    # FIX S3: Wire wallet_tracker → normalized intelligence service so that
    # on-chain movements actually reach the trading decision engine.
    # Previously wallet_tracker published WHALE_ACCUMULATION/DISTRIBUTION to
    # Redis but nobody subscribed and called the in-process ingestor — the
    # normalized in-process intelligence snapshot stayed empty, meaning
    # on-chain intelligence had zero effect on trading decisions.
    try:
        from services.intelligence_event_utils import record_onchain_intelligence_event as _ingest
        original_publish = tracker._publish_movement

        def _patched_publish(wallet, delta, asset, new_balance):
            # Call original Redis publish path first
            original_publish(wallet, delta, asset, new_balance)
            # Also feed the in-process decision path directly so we don't depend on Redis round-trip
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
                }, external_id=f"onchain:{wallet.get('label', 'unknown')}:{asset}:{event_type}:{int(abs(delta) * 10000)}")
            except Exception as _e:
                pass  # in-process feed is best-effort

        tracker._publish_movement = _patched_publish  # type: ignore[method-assign]
    except Exception as _wire_err:
        pass  # helper not yet available — on-chain cache stays empty until first decision cycle

    tracker.start()

    # Start multi-chain tracker for BNB, Solana, XRP (in addition to ETH/BTC)
    try:
        multi_tracker.start()
        logger.info("[whale_intelligence] Multi-chain tracker started (BNB, SOL, XRP)")
    except Exception as e:
        logger.warning(f"[whale_intelligence] Multi-chain tracker failed to start: {e}")


def stop_all() -> None:
    """Graceful shutdown. Wire to your SIGTERM handler."""
    tracker.stop()
    multi_tracker.stop()


def is_running() -> bool:
    """Return True if Phase 2 whale tracker thread is active."""
    return bool(getattr(tracker, "_running", False))


__all__ = ["tracker", "multi_tracker", "start_all", "stop_all", "is_running",
           "WalletTracker", "WalletBehaviorClassifier",
           "WalletDatabase", "WalletClusterAnalyzer", "MultiChainTracker"]
