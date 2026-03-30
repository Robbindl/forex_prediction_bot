"""
solana_tracker.py — Solana whale tracker using Solana RPC.
"""
from __future__ import annotations

import requests
from typing import Dict, Optional
import time
from config.config import SOLANA_RPC_URL
from utils.logger import get_logger

logger = get_logger()
MIN_SOL_DELTA = 100.0  # Minimum SOL change to track (lamports: 1 SOL = 1e9 lamports)
_RPC_BACKOFF_UNTIL = 0.0
_RPC_BACKOFF_NOTIFIED = False
_RPC_BACKOFF_SECS = 180.0


class SolanaTracker:
    """Track whale wallets on Solana via Solana RPC API."""

    def __init__(self, rpc_url: Optional[str] = None) -> None:
        self._rpc_url = rpc_url or SOLANA_RPC_URL
        self._enabled = True

    def fetch_balance(self, address: str) -> Optional[float]:
        """
        Fetch SOL balance for a wallet address via Solana RPC.
        Returns balance in SOL (lamports converted to decimal).
        """
        global _RPC_BACKOFF_UNTIL, _RPC_BACKOFF_NOTIFIED
        now = time.time()
        if now < _RPC_BACKOFF_UNTIL:
            if not _RPC_BACKOFF_NOTIFIED:
                logger.warning(
                    f"[SolanaTracker] RPC backoff active — skipping balance calls for "
                    f"{int(_RPC_BACKOFF_UNTIL - now)}s"
                )
                _RPC_BACKOFF_NOTIFIED = True
            return None
        try:
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getBalance",
                "params": [address],
            }
            resp = requests.post(self._rpc_url, json=payload, timeout=10)
            resp.raise_for_status()
            data = resp.json()

            if "error" in data:
                logger.warning(f"[SolanaTracker] RPC error for {address[:10]}...: {data['error']}")
                return None

            balance_lamports = data.get("result", {}).get("value", 0)
            balance_sol = balance_lamports / 1e9  # Convert lamports to SOL
            _RPC_BACKOFF_UNTIL = 0.0
            _RPC_BACKOFF_NOTIFIED = False
            return balance_sol
        except Exception as e:
            _RPC_BACKOFF_UNTIL = time.time() + _RPC_BACKOFF_SECS
            _RPC_BACKOFF_NOTIFIED = False
            logger.warning(
                f"[SolanaTracker] Failed to fetch balance for {address[:10]}...: {e} "
                f"— backing off {int(_RPC_BACKOFF_SECS)}s"
            )
            return None

    def classify_movement(self, delta_sol: float) -> str:
        """Classify a SOL balance change as accumulation or distribution."""
        if abs(delta_sol) < MIN_SOL_DELTA:
            return "NOISE"
        return "SOL_ACCUMULATION" if delta_sol > 0 else "SOL_DISTRIBUTION"

    def get_token_balance(self, wallet: str, token_mint: str) -> Optional[float]:
        """
        Fetch SPL token balance for a wallet.
        Requires associated token account lookup (more complex).
        """
        global _RPC_BACKOFF_UNTIL, _RPC_BACKOFF_NOTIFIED
        now = time.time()
        if now < _RPC_BACKOFF_UNTIL:
            if not _RPC_BACKOFF_NOTIFIED:
                logger.warning(
                    f"[SolanaTracker] RPC backoff active — skipping token balance calls for "
                    f"{int(_RPC_BACKOFF_UNTIL - now)}s"
                )
                _RPC_BACKOFF_NOTIFIED = True
            return None
        try:
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getTokenAccountsByOwner",
                "params": [
                    wallet,
                    {"mint": token_mint},
                    {"encoding": "jsonParsed"},
                ],
            }
            resp = requests.post(self._rpc_url, json=payload, timeout=10)
            resp.raise_for_status()
            data = resp.json()

            if "error" in data or not data.get("result", {}).get("value"):
                return None

            accounts = data["result"]["value"]
            if not accounts:
                return None

            # Sum all token account balances for this wallet
            total = 0.0
            for account in accounts:
                parsed = account.get("account", {}).get("data", {}).get("parsed", {})
                balance_info = parsed.get("info", {}).get("tokenAmount", {})
                total += float(balance_info.get("uiAmount", 0))

            _RPC_BACKOFF_UNTIL = 0.0
            _RPC_BACKOFF_NOTIFIED = False
            return total
        except Exception as e:
            _RPC_BACKOFF_UNTIL = time.time() + _RPC_BACKOFF_SECS
            _RPC_BACKOFF_NOTIFIED = False
            logger.warning(
                f"[SolanaTracker] Failed to fetch token balance: {e} "
                f"— backing off {int(_RPC_BACKOFF_SECS)}s"
            )
            return None
