"""
execution/exchange_adapter.py — Exchange abstraction layer.

Every real exchange connector must implement ExchangeAdapter.
PaperAdapter wraps the existing PaperTrader so paper trading
works through the same interface.

Adding a new exchange:
    1. Subclass ExchangeAdapter
    2. Implement the five abstract methods
    3. Register it in ExchangeRouter

Strategy code never imports exchange modules directly.
"""
from __future__ import annotations
import time
import threading
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from utils.logger import get_logger

logger = get_logger()


# ── Data models ────────────────────────────────────────────────────────────────

@dataclass
class OrderRequest:
    symbol:     str
    side:       str          # "BUY" or "SELL"
    quantity:   float
    order_type: str = "MARKET"   # "MARKET" or "LIMIT"
    price:      Optional[float] = None
    stop_loss:  Optional[float] = None
    take_profit:Optional[float] = None
    client_id:  str = ""


@dataclass
class OrderResult:
    order_id:   str
    status:     str          # PENDING / SUBMITTED / FILLED / FAILED
    filled_qty: float = 0.0
    avg_price:  float = 0.0
    error:      str   = ""
    raw:        dict  = field(default_factory=dict)


@dataclass
class OrderBookSnapshot:
    symbol:    str
    timestamp: float
    bids:      List[tuple]   # [(price, qty), ...]
    asks:      List[tuple]
    spread:    float = 0.0


# ── Circuit breaker ────────────────────────────────────────────────────────────

class CircuitBreaker:
    """
    Opens after max_failures consecutive failures.
    Attempts to close after reset_timeout seconds.
    """

    def __init__(self, max_failures: int = 3, reset_timeout: float = 60.0):
        self._max       = max_failures
        self._timeout   = reset_timeout
        self._failures  = 0
        self._opened_at: Optional[float] = None
        self._lock      = threading.Lock()

    @property
    def is_open(self) -> bool:
        with self._lock:
            if self._opened_at is None:
                return False
            if time.time() - self._opened_at >= self._timeout:
                # Half-open: allow one attempt
                self._failures  = 0
                self._opened_at = None
                return False
            return True

    def record_success(self) -> None:
        with self._lock:
            self._failures  = 0
            self._opened_at = None

    def record_failure(self) -> None:
        with self._lock:
            self._failures += 1
            if self._failures >= self._max:
                self._opened_at = time.time()
                logger.warning(
                    f"[CircuitBreaker] Opened after {self._failures} failures"
                )


# ── Token bucket rate limiter ──────────────────────────────────────────────────

class RateLimiter:
    """
    Token bucket. Replenishes at rate_per_second tokens/sec up to capacity.
    Call acquire() before each API call.
    """

    def __init__(self, rate_per_second: float = 10.0, capacity: int = 20):
        self._rate     = rate_per_second
        self._capacity = capacity
        self._tokens   = float(capacity)
        self._last     = time.monotonic()
        self._lock     = threading.Lock()

    def acquire(self, tokens: float = 1.0, block: bool = True) -> bool:
        while True:
            with self._lock:
                now    = time.monotonic()
                refill = (now - self._last) * self._rate
                self._tokens = min(self._capacity, self._tokens + refill)
                self._last   = now
                if self._tokens >= tokens:
                    self._tokens -= tokens
                    return True
            if not block:
                return False
            time.sleep(0.05)


# ── Abstract adapter ───────────────────────────────────────────────────────────

class ExchangeAdapter(ABC):
    """
    Abstract base for all exchange connectors.
    Subclass this for every real exchange.
    """

    def __init__(
        self,
        name: str,
        rate_per_second: float = 10.0,
        circuit_max_failures: int = 3,
    ):
        self.name            = name
        self._rate_limiter   = RateLimiter(rate_per_second)
        self._circuit        = CircuitBreaker(circuit_max_failures)
        self._lock           = threading.RLock()

    @property
    def is_available(self) -> bool:
        return not self._circuit.is_open

    def place_order(self, req: OrderRequest) -> OrderResult:
        if self._circuit.is_open:
            return OrderResult(
                order_id="", status="FAILED",
                error=f"{self.name} circuit open — exchange unavailable"
            )
        self._rate_limiter.acquire()
        try:
            result = self._place_order(req)
            self._circuit.record_success()
            return result
        except Exception as e:
            self._circuit.record_failure()
            logger.error(f"[{self.name}] place_order failed: {e}")
            return OrderResult(order_id="", status="FAILED", error=str(e))

    def cancel_order(self, order_id: str) -> bool:
        if self._circuit.is_open:
            return False
        self._rate_limiter.acquire()
        try:
            result = self._cancel_order(order_id)
            self._circuit.record_success()
            return result
        except Exception as e:
            self._circuit.record_failure()
            logger.error(f"[{self.name}] cancel_order failed: {e}")
            return False

    def get_order_status(self, order_id: str) -> Optional[OrderResult]:
        self._rate_limiter.acquire()
        try:
            return self._get_order_status(order_id)
        except Exception as e:
            logger.warning(f"[{self.name}] get_order_status failed: {e}")
            return None

    def get_balance(self, currency: str = "USD") -> float:
        self._rate_limiter.acquire()
        try:
            return self._get_balance(currency)
        except Exception as e:
            logger.warning(f"[{self.name}] get_balance failed: {e}")
            return 0.0

    def get_orderbook(self, symbol: str, depth: int = 10) -> Optional[OrderBookSnapshot]:
        try:
            return self._get_orderbook(symbol, depth)
        except Exception as e:
            logger.debug(f"[{self.name}] get_orderbook failed: {e}")
            return None

    # ── Abstract — implement per exchange ─────────────────────────────────────

    @abstractmethod
    def _place_order(self, req: OrderRequest) -> OrderResult: ...

    @abstractmethod
    def _cancel_order(self, order_id: str) -> bool: ...

    @abstractmethod
    def _get_order_status(self, order_id: str) -> Optional[OrderResult]: ...

    @abstractmethod
    def _get_balance(self, currency: str) -> float: ...

    @abstractmethod
    def _get_orderbook(self, symbol: str, depth: int) -> Optional[OrderBookSnapshot]: ...