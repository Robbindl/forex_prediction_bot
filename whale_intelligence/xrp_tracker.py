"""
xrp_tracker.py — XRP Ledger whale tracker using rippled API.
"""
from __future__ import annotations

import requests
from typing import Optional
import time
from config.config import XRPL_RPC_URL
from utils.logger import get_logger

logger = get_logger()
MIN_XRP_DELTA = 100000.0  # Minimum XRP drops to track (1 XRP = 1e6 drops)
_RPC_BACKOFF_UNTIL = 0.0
_RPC_BACKOFF_NOTIFIED = False
_RPC_BACKOFF_SECS = 180.0


class XRPTracker:
    """Track whale wallets on XRP Ledger via rippled API."""

    def __init__(self, rpc_url: Optional[str] = None) -> None:
        self._rpc_url = rpc_url or XRPL_RPC_URL
        self._enabled = True

    def fetch_balance(self, address: str) -> Optional[float]:
        """
        Fetch XRP balance for a wallet address via rippled API.
        Returns balance in XRP (drops converted to decimal).
        """
        global _RPC_BACKOFF_UNTIL, _RPC_BACKOFF_NOTIFIED
        now = time.time()
        if now < _RPC_BACKOFF_UNTIL:
            if not _RPC_BACKOFF_NOTIFIED:
                logger.warning(
                    f"[XRPTracker] RPC backoff active — skipping balance calls for "
                    f"{int(_RPC_BACKOFF_UNTIL - now)}s"
                )
                _RPC_BACKOFF_NOTIFIED = True
            return None
        try:
            payload = {
                "method": "account_info",
                "params": [
                    {
                        "account": address,
                        "ledger_index": "validated",
                    }
                ],
            }
            resp = requests.post(self._rpc_url, json=payload, timeout=10)
            resp.raise_for_status()
            data = resp.json()

            # rippled returns result directly, not status: success
            if "error" in data or "result" not in data:
                logger.warning(f"[XRPTracker] RPC error for {address[:10]}...: {data.get('error', 'Unknown error')}")
                return None

            # Balance is in drops (1 XRP = 1,000,000 drops)
            balance_drops = int(data.get("result", {}).get("account_data", {}).get("Balance", "0"))
            balance_xrp = balance_drops / 1e6
            _RPC_BACKOFF_UNTIL = 0.0
            _RPC_BACKOFF_NOTIFIED = False
            return balance_xrp
        except Exception as e:
            _RPC_BACKOFF_UNTIL = time.time() + _RPC_BACKOFF_SECS
            _RPC_BACKOFF_NOTIFIED = False
            logger.warning(
                f"[XRPTracker] Failed to fetch balance for {address[:10]}...: {e} "
                f"— backing off {int(_RPC_BACKOFF_SECS)}s"
            )
            return None

    def classify_movement(self, delta_xrp: float) -> str:
        """Classify an XRP balance change as accumulation or distribution."""
        if abs(delta_xrp) < (MIN_XRP_DELTA / 1e6):
            return "NOISE"
        return "XRP_ACCUMULATION" if delta_xrp > 0 else "XRP_DISTRIBUTION"

    def get_transaction_history(self, address: str, limit: int = 10) -> Optional[list]:
        """Fetch recent transactions for a wallet to detect whale movements."""
        global _RPC_BACKOFF_UNTIL, _RPC_BACKOFF_NOTIFIED
        now = time.time()
        if now < _RPC_BACKOFF_UNTIL:
            if not _RPC_BACKOFF_NOTIFIED:
                logger.warning(
                    f"[XRPTracker] RPC backoff active — skipping transaction history calls for "
                    f"{int(_RPC_BACKOFF_UNTIL - now)}s"
                )
                _RPC_BACKOFF_NOTIFIED = True
            return None
        try:
            payload = {
                "method": "account_tx",
                "params": [
                    {
                        "account": address,
                        "limit": limit,
                        "ledger_index_min": -1,
                        "ledger_index_max": -1,
                    }
                ],
            }
            resp = requests.post(self._rpc_url, json=payload, timeout=10)
            resp.raise_for_status()
            data = resp.json()

            if data.get("status") != "success":
                return None

            _RPC_BACKOFF_UNTIL = 0.0
            _RPC_BACKOFF_NOTIFIED = False
            return data.get("result", {}).get("transactions", [])
        except Exception as e:
            _RPC_BACKOFF_UNTIL = time.time() + _RPC_BACKOFF_SECS
            _RPC_BACKOFF_NOTIFIED = False
            logger.warning(
                f"[XRPTracker] Failed to fetch transactions: {e} "
                f"— backing off {int(_RPC_BACKOFF_SECS)}s"
            )
            return None
