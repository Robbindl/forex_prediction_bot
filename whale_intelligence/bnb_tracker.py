"""
bnb_tracker.py — BNB Chain whale tracker using public RPC endpoint.
Etherscan/BscScan free tier only support Ethereum. Using direct RPC for BNB.
"""
from __future__ import annotations

import requests
from typing import Dict, Optional
import time
from config.config import BNB_RPC_URL
from utils.logger import get_logger

logger = get_logger()
_RPC_BACKOFF_UNTIL = 0.0
_RPC_BACKOFF_NOTIFIED = False
_RPC_BACKOFF_SECS = 180.0

class BNBTracker:
    """Track whale wallets on BNB Chain via public RPC endpoint."""

    def __init__(self) -> None:
        self._rpc_url = BNB_RPC_URL
        self._enabled = True  # Always enabled - uses public RPC

    def fetch_balance(self, address: str) -> Optional[float]:
        """
        Fetch BNB balance for a wallet address via RPC eth_getBalance call.
        Returns balance in BNB (wei converted to decimal).
        """
        global _RPC_BACKOFF_UNTIL, _RPC_BACKOFF_NOTIFIED
        now = time.time()
        if now < _RPC_BACKOFF_UNTIL:
            if not _RPC_BACKOFF_NOTIFIED:
                logger.warning(
                    f"[BNBTracker] RPC backoff active — skipping balance calls for "
                    f"{int(_RPC_BACKOFF_UNTIL - now)}s"
                )
                _RPC_BACKOFF_NOTIFIED = True
            return None
        try:
            # Validate address format
            if not address.startswith("0x") or len(address) != 42:
                logger.error(f"[BNBTracker] Invalid address format: {address}")
                return None

            payload = {
                "jsonrpc": "2.0",
                "method": "eth_getBalance",
                "params": [address, "latest"],
                "id": 1,
            }

            resp = requests.post(self._rpc_url, json=payload, timeout=10)
            resp.raise_for_status()
            data = resp.json()

            if "error" in data:
                logger.warning(f"[BNBTracker] RPC error for {address[:10]}...: {data.get('error')}")
                return None

            result = data.get("result", "0x0")
            # Convert hex to int, then wei to BNB
            balance_wei = int(result, 16)
            balance_bnb = balance_wei / 1e18
            _RPC_BACKOFF_UNTIL = 0.0
            _RPC_BACKOFF_NOTIFIED = False
            return balance_bnb
        except Exception as e:
            _RPC_BACKOFF_UNTIL = time.time() + _RPC_BACKOFF_SECS
            _RPC_BACKOFF_NOTIFIED = False
            logger.warning(
                f"[BNBTracker] Failed to fetch balance for {address[:10]}...: {e} "
                f"— backing off {int(_RPC_BACKOFF_SECS)}s"
            )
            return None

    def classify_movement(self, delta_bnb: float) -> str:
        """Classify a BNB balance change as accumulation or distribution."""
        MIN_DELTA = 10.0  # Significant whale movement threshold
        if abs(delta_bnb) < MIN_DELTA:
            return "NOISE"
        return "BNB_ACCUMULATION" if delta_bnb > 0 else "BNB_DISTRIBUTION"

