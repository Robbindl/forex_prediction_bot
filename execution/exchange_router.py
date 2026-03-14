"""
execution/exchange_router.py — Routes orders to the correct exchange.

Strategy code calls router.submit(signal) and never knows which
exchange handled it. The routing table is config-driven.

Default routing:
    crypto      → paper (until Binance connector added)
    forex       → paper (until forex broker connector added)
    commodities → paper
    stocks      → paper
    indices     → paper
"""
from __future__ import annotations
import time
from typing import Dict, Optional
from execution.exchange_adapter import ExchangeAdapter, OrderRequest, OrderResult
from utils.logger import get_logger

logger = get_logger()

_DEFAULT_ROUTING = {
    "crypto":      "paper",
    "forex":       "paper",
    "commodities": "paper",
    "stocks":      "paper",
    "indices":     "paper",
}

_MAX_RETRIES    = 3
_RETRY_BASE_SEC = 0.5   # exponential: 0.5, 1.0, 2.0


class ExchangeRouter:
    """
    Maintains a registry of exchange adapters and routes
    order requests based on asset category.
    """

    def __init__(self):
        self._adapters: Dict[str, ExchangeAdapter] = {}
        self._routing:  Dict[str, str]             = dict(_DEFAULT_ROUTING)

    def register(self, name: str, adapter: ExchangeAdapter) -> None:
        self._adapters[name] = adapter
        logger.info(f"[Router] Registered exchange adapter: {name}")

    def set_route(self, category: str, adapter_name: str) -> None:
        """Override routing for a category. E.g. route crypto to Binance."""
        self._routing[category] = adapter_name
        logger.info(f"[Router] Route {category} → {adapter_name}")

    def submit(self, signal: dict) -> Optional[OrderResult]:
        """
        Convert a signal dict to an OrderRequest and route it.
        Retries on transient failure with exponential backoff.
        """
        category = signal.get("category", "crypto")
        adapter  = self._get_adapter(category)
        if adapter is None:
            logger.error(f"[Router] No adapter for category: {category}")
            return None

        if not adapter.is_available:
            logger.warning(f"[Router] Adapter {adapter.name} unavailable (circuit open)")
            return None

        req = OrderRequest(
            symbol      = signal.get("asset", ""),
            side        = (signal.get("direction") or signal.get("signal", "BUY")).upper(),
            quantity    = float(signal.get("position_size", 0)),
            order_type  = signal.get("order_type", "MARKET"),
            price       = signal.get("entry_price"),
            stop_loss   = signal.get("stop_loss"),
            take_profit = signal.get("take_profit"),
        )

        for attempt in range(1, _MAX_RETRIES + 1):
            result = adapter.place_order(req)
            if result.status == "FILLED":
                logger.info(
                    f"[Router] {adapter.name} filled {req.side} {req.symbol} "
                    f"qty={result.filled_qty} @ {result.avg_price}"
                )
                return result
            if result.status == "FAILED" and self._is_permanent_error(result.error):
                logger.error(f"[Router] Permanent error for {req.symbol}: {result.error}")
                return result
            # Transient failure — retry with backoff
            wait = _RETRY_BASE_SEC * (2 ** (attempt - 1))
            logger.warning(
                f"[Router] Attempt {attempt}/{_MAX_RETRIES} failed for {req.symbol}: "
                f"{result.error} — retrying in {wait:.1f}s"
            )
            time.sleep(wait)

        logger.error(f"[Router] All {_MAX_RETRIES} attempts failed for {req.symbol}")
        return result

    def _get_adapter(self, category: str) -> Optional[ExchangeAdapter]:
        name = self._routing.get(category, "paper")
        adapter = self._adapters.get(name)
        if adapter is None:
            logger.warning(f"[Router] Adapter '{name}' not registered — trying 'paper'")
            adapter = self._adapters.get("paper")
        return adapter

    # FIX: Add this missing static method
    @staticmethod
    def _is_permanent_error(error: str) -> bool:
        """Check if an error is permanent (no point retrying)."""
        if not error:
            return False
        error_lower = error.lower()
        permanent_phrases = [
            "insufficient balance",
            "invalid symbol",
            "symbol not found",
            "min order size",
            "account suspended",
            "invalid api key",
            "permission denied",
            "market closed"
        ]
        return any(phrase in error_lower for phrase in permanent_phrases)