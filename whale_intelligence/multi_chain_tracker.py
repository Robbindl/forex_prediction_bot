"""
multi_chain_tracker.py — Orchestrates whale tracking across all supported blockchains.
Polls BTC, ETH, BNB, Solana, and XRP simultaneously and publishes normalized events.
"""
from __future__ import annotations

import os
import threading
import time
from datetime import datetime
from typing import Dict, List, Optional

from utils.logger import get_logger

logger = get_logger()

POLL_INTERVAL = 300  # seconds (5 minutes)


class MultiChainTracker:
    """
    Coordinates whale tracking across multiple blockchains.
    Maintains balance state per wallet and publishes movement events.
    """

    def __init__(self) -> None:
        # Import trackers lazily to avoid circular deps
        from whale_intelligence.bnb_tracker import BNBTracker
        from whale_intelligence.solana_tracker import SolanaTracker
        from whale_intelligence.xrp_tracker import XRPTracker

        # Note: ETH/BTC tracker is managed separately in whale_intelligence/__init__.py
        # This tracker focuses on BNB, SOL, XRP which aren't covered by the main tracker
        self._bnb_tracker = BNBTracker()
        self._sol_tracker = SolanaTracker()
        self._xrp_tracker = XRPTracker()

        self._balances: Dict[str, Dict[str, float]] = {}  # {address: {chain: balance}}
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._pub = None
        self._movements: List[Dict] = []
        self._wallets: List[Dict] = []  # Will be populated from multi_chain_seeds

    def add_wallet(self, address: str, label: str, chain: str, wallet_type: str = "unknown") -> None:
        """Add a wallet to track on a specific chain."""
        entry = {
            "address": address,
            "label": label,
            "chain": chain,
            "type": wallet_type,
        }
        self._wallets.append(entry)
        logger.info(f"[MultiChainTracker] Added {chain.upper()} wallet: {label} ({address[:12]}...)")

    def start(self) -> None:
        """Start polling all chain trackers."""
        self._running = True
        self._init_redis()
        self._thread = threading.Thread(
            target=self._loop, name="MultiChainTracker", daemon=True
        )
        self._thread.start()
        logger.info("[MultiChainTracker] Started tracking across all chains")

    def stop(self) -> None:
        """Stop polling."""
        self._running = False

    def _init_redis(self) -> None:
        """Initialize Redis publisher."""
        try:
            from services.redis_pool import get_client as _get_redis_client
            self._pub = _get_redis_client()
            self._pub.ping()
            logger.info("[MultiChainTracker] Redis connected")
        except Exception as e:
            logger.warning(f"[MultiChainTracker] Redis unavailable: {e}")

    def _loop(self) -> None:
        """Main polling loop."""
        while self._running:
            try:
                self._poll_all_chains()
            except Exception as e:
                logger.error(f"[MultiChainTracker] Poll error: {e}")

            time.sleep(POLL_INTERVAL)

    def _poll_all_chains(self) -> None:
        """Poll each chain tracker for wallet updates."""
        for wallet in self._wallets:
            address = wallet["address"]
            chain = wallet["chain"].lower()
            label = wallet.get("label", address[:10])

            # Initialize wallet entry if needed
            if address not in self._balances:
                self._balances[address] = {}

            old_balance = self._balances[address].get(chain)
            new_balance = self._fetch_balance(address, chain)

            if new_balance is None:
                continue

            # Detect movement
            if old_balance is not None:
                delta = new_balance - old_balance
                if abs(delta) > 0.1:  # Only report significant changes
                    self._report_movement(
                        address=address,
                        chain=chain,
                        label=label,
                        old_balance=old_balance,
                        new_balance=new_balance,
                        delta=delta,
                    )

            self._balances[address][chain] = new_balance

    def _fetch_balance(self, address: str, chain: str) -> Optional[float]:
        """Route balance fetch to appropriate chain tracker."""
        try:
            if chain == "bnb":
                return self._bnb_tracker.fetch_balance(address)
            elif chain in ("sol", "solana"):
                return self._sol_tracker.fetch_balance(address)
            elif chain == "xrp":
                return self._xrp_tracker.fetch_balance(address)
            else:
                logger.warning(f"[MultiChainTracker] Unknown chain: {chain}")
        except Exception as e:
            logger.warning(f"[MultiChainTracker] Failed to fetch {chain} balance for {address[:10]}...: {e}")
        return None

    def _report_movement(
        self,
        address: str,
        chain: str,
        label: str,
        old_balance: float,
        new_balance: float,
        delta: float,
    ) -> None:
        """Report a whale movement and publish to Redis."""
        chain_upper = chain.upper()
        direction = "ACCUMULATION" if delta > 0 else "DISTRIBUTION"

        event = {
            "wallet": address,
            "chain": chain,
            "label": label,
            "type": f"{chain_upper}_{direction}",
            "old_balance": old_balance,
            "new_balance": new_balance,
            "delta": delta,
            "ts": datetime.utcnow().isoformat(),
        }

        self._movements.append(event)
        logger.info(f"[MultiChainTracker] {label} ({chain}): {direction} {abs(delta):.2f} {chain_upper}")

        # Publish to Redis if available
        if self._pub:
            try:
                self._pub.publish("whale_movements", str(event))
            except Exception as e:
                logger.error(f"[MultiChainTracker] Failed to publish event: {e}")

    def get_recent_movements(self, limit: int = 10) -> List[Dict]:
        """Get recent whale movements."""
        return self._movements[-limit:]
