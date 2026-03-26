"""
bnb_tracker.py — BNB Chain whale tracker using public RPC endpoint.
Etherscan/BscScan free tier only support Ethereum. Using direct RPC for BNB.
"""
from __future__ import annotations

import os
import requests
from typing import Dict, Optional
from utils.logger import get_logger

logger = get_logger()

# BNB Chain public RPC endpoints (free, no API key needed)
BNB_RPC_ENDPOINT = os.getenv("BNB_RPC_URL", "https://bsc-dataseed1.binance.org")


class BNBTracker:
    """Track whale wallets on BNB Chain via public RPC endpoint."""

    def __init__(self) -> None:
        self._rpc_url = BNB_RPC_ENDPOINT
        self._enabled = True  # Always enabled - uses public RPC

    def fetch_balance(self, address: str) -> Optional[float]:
        """
        Fetch BNB balance for a wallet address via RPC eth_getBalance call.
        Returns balance in BNB (wei converted to decimal).
        """
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
            return balance_bnb
        except Exception as e:
            logger.error(f"[BNBTracker] Failed to fetch balance for {address[:10]}...: {e}")
            return None

    def classify_movement(self, delta_bnb: float) -> str:
        """Classify a BNB balance change as accumulation or distribution."""
        MIN_DELTA = 10.0  # Significant whale movement threshold
        if abs(delta_bnb) < MIN_DELTA:
            return "NOISE"
        return "BNB_ACCUMULATION" if delta_bnb > 0 else "BNB_DISTRIBUTION"

