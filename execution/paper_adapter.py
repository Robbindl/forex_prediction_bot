"""
execution/paper_adapter.py — PaperTrader wrapped as an ExchangeAdapter.

This lets the rest of the system treat paper trading the same as a
real exchange. When a real exchange connector is added, swap the
adapter in ExchangeRouter without touching strategy code.
"""
from __future__ import annotations
import uuid
from typing import Optional
from execution.exchange_adapter import (
    ExchangeAdapter, OrderRequest, OrderResult, OrderBookSnapshot
)
from utils.logger import get_logger

logger = get_logger()


class PaperAdapter(ExchangeAdapter):
    """
    Wraps execution/paper_trader.py as an ExchangeAdapter.
    Supports all asset categories. Zero exchange latency.
    """

    def __init__(self, paper_trader):
        super().__init__(name="paper", rate_per_second=1000.0)
        self._pt = paper_trader

    def _place_order(self, req: OrderRequest) -> OrderResult:
        signal = {
            "asset":        req.symbol,
            "direction":    req.side,
            "signal":       req.side,
            "confidence":   0.8,
            "entry_price":  req.price or 0.0,
            "stop_loss":    req.stop_loss or 0.0,
            "take_profit":  req.take_profit or 0.0,
            "position_size": req.quantity,
        }
        trade = self._pt.execute_signal(signal)
        if trade:
            return OrderResult(
                order_id=trade.get("trade_id", str(uuid.uuid4())),
                status="FILLED",
                filled_qty=float(trade.get("position_size", req.quantity)),
                avg_price=float(trade.get("entry_price", req.price or 0)),
                raw=trade,
            )
        return OrderResult(order_id="", status="FAILED", error="PaperTrader rejected signal")

    def _cancel_order(self, order_id: str) -> bool:
        # Paper trades don't need cancellation — they execute immediately
        return True

    def _get_order_status(self, order_id: str) -> Optional[OrderResult]:
        with self._pt._lock:
            pos = self._pt.open_positions.get(order_id)
        if pos:
            return OrderResult(
                order_id=order_id,
                status="FILLED",
                filled_qty=float(pos.get("position_size", 0)),
                avg_price=float(pos.get("entry_price", 0)),
            )
        return OrderResult(order_id=order_id, status="CLOSED")

    def _get_balance(self, currency: str = "USD") -> float:
        return self._pt.account_balance

    # FIX: Add this required abstract method
    def _get_orderbook(self, symbol: str, depth: int = 10) -> Optional[OrderBookSnapshot]:
        """Paper trading has no real order book."""
        return None