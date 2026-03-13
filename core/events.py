"""
core/events.py — In-process typed EventBus for the trading platform.

Design principles:
  • Zero external dependencies — works without Redis, no network calls.
  • Thread-safe — all subscribers called from the publishing thread or
    optionally in their own daemon threads (async=True).
  • Typed events — every event is a dataclass, no raw dicts.
  • Weak references optional — use strong refs for long-lived subscribers.
  • Fire-and-forget — a slow subscriber never blocks the engine.

Usage:
    from core.events import bus, TradeOpenedEvent

    # Subscribe
    def on_trade(evt: TradeOpenedEvent):
        print(evt.asset, evt.pnl)

    bus.subscribe(TradeOpenedEvent, on_trade)

    # Publish (from anywhere in the process)
    bus.emit(TradeOpenedEvent(asset="BTC-USD", ...))
"""

from __future__ import annotations

import threading
import traceback
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Type, TypeVar

from logger import logger

E = TypeVar("E", bound="BaseEvent")


# ── Base event ────────────────────────────────────────────────────────────────

@dataclass
class BaseEvent:
    """All events inherit from this."""
    ts: datetime = field(default_factory=datetime.utcnow, init=False)

    @property
    def name(self) -> str:
        return self.__class__.__name__


# ── Trading events ────────────────────────────────────────────────────────────

@dataclass
class TradeOpenedEvent(BaseEvent):
    trade_id: str
    asset: str
    canonical_asset: str
    category: str
    direction: str          # 'BUY' | 'SELL'
    entry_price: float
    stop_loss: float
    take_profit_levels: List[float]
    position_size: float
    confidence: float
    strategy_id: str
    reason: str


@dataclass
class TradeClosedEvent(BaseEvent):
    trade_id: str
    asset: str
    canonical_asset: str
    category: str
    direction: str
    entry_price: float
    exit_price: float
    position_size: float
    pnl: float
    pnl_percent: float
    exit_reason: str        # 'Stop Loss' | 'Take Profit 1' | 'Manual' …
    duration_minutes: int
    strategy_id: str


@dataclass
class SignalGeneratedEvent(BaseEvent):
    asset: str
    canonical_asset: str
    category: str
    direction: str
    confidence: float
    entry_price: float
    stop_loss: float
    take_profit: float
    strategy_id: str
    layer_reached: int      # which layer passed last (1-7)
    reason: str


@dataclass
class SignalRejectedEvent(BaseEvent):
    asset: str
    canonical_asset: str
    category: str
    direction: str
    confidence: float
    reject_reason: str
    reject_layer: str       # e.g. 'risk_check', 'duplicate_guard', 'cooldown'


@dataclass
class RiskLimitHitEvent(BaseEvent):
    limit_type: str         # 'daily_loss' | 'max_positions' | 'category_cap' | 'correlation'
    value: float
    threshold: float
    message: str


@dataclass
class PositionUpdateEvent(BaseEvent):
    """Fires every monitoring tick so dashboard can refresh without polling."""
    open_positions: List[Dict]
    balance: float
    daily_pnl: float
    daily_trades: int


@dataclass
class BalanceChangedEvent(BaseEvent):
    old_balance: float
    new_balance: float
    delta: float
    reason: str             # 'trade_closed' | 'deposit' | 'withdrawal'


@dataclass
class SessionChangedEvent(BaseEvent):
    old_session: str
    new_session: str
    active_markets: List[str]


@dataclass
class RegimeChangedEvent(BaseEvent):
    asset: str
    old_regime: str
    new_regime: str
    confidence: float


@dataclass
class CooldownActivatedEvent(BaseEvent):
    asset: str
    canonical_asset: str
    cooldown_minutes: int
    reason: str             # 'loss' | 'manual'


@dataclass
class CooldownExpiredEvent(BaseEvent):
    asset: str
    canonical_asset: str


@dataclass
class ModelRetrainedEvent(BaseEvent):
    model_name: str
    asset: Optional[str]
    new_accuracy: float
    training_samples: int


@dataclass
class SystemStartedEvent(BaseEvent):
    strategy_mode: str
    account_balance: float
    asset_count: int


@dataclass
class SystemStoppingEvent(BaseEvent):
    reason: str
    open_positions_count: int


@dataclass
class HealthCheckEvent(BaseEvent):
    status: str             # 'healthy' | 'degraded' | 'critical'
    issues: List[str]
    ram_pct: float
    cpu_pct: float


@dataclass
class PriceUpdateEvent(BaseEvent):
    asset: str
    price: float
    category: str


@dataclass
class SentimentUpdateEvent(BaseEvent):
    asset: str
    score: float
    label: str              # 'extreme_fear' | 'fear' | 'neutral' | 'greed' | 'extreme_greed'


@dataclass
class WhaleAlertEvent(BaseEvent):
    asset: str
    direction: str
    size_usd: float
    source: str


# ── EventBus ──────────────────────────────────────────────────────────────────

class EventBus:
    """
    Thread-safe in-process pub/sub bus.

    • Subscribers are called synchronously in the emitter's thread by default.
    • Pass async_dispatch=True to each subscribe() call to run in a daemon thread.
    • Exceptions in subscribers are logged but never propagate to the emitter.
    """

    def __init__(self):
        self._lock: threading.Lock = threading.Lock()
        # event_type → list of (callback, async_dispatch)
        self._subscribers: Dict[Type[BaseEvent], List[tuple]] = {}
        # History ring buffer (last 500 events for debugging)
        self._history: List[BaseEvent] = []
        self._history_limit = 500

    def subscribe(
        self,
        event_type: Type[E],
        callback: Callable[[E], None],
        async_dispatch: bool = False,
    ) -> None:
        """
        Register a callback for an event type.

        Args:
            event_type: The event class to subscribe to.
            callback:   Called with the event instance.
            async_dispatch: If True, callback runs in a daemon thread so a
                            slow handler doesn't block the trading loop.
        """
        with self._lock:
            if event_type not in self._subscribers:
                self._subscribers[event_type] = []
            self._subscribers[event_type].append((callback, async_dispatch))

    def unsubscribe(self, event_type: Type[E], callback: Callable[[E], None]) -> None:
        """Remove a previously registered callback."""
        with self._lock:
            subs = self._subscribers.get(event_type, [])
            self._subscribers[event_type] = [
                (cb, a) for cb, a in subs if cb is not callback
            ]

    def emit(self, event: BaseEvent) -> None:
        """
        Publish an event to all subscribers.
        Never raises — exceptions are caught and logged.
        """
        # Add to history
        with self._lock:
            self._history.append(event)
            if len(self._history) > self._history_limit:
                self._history = self._history[-self._history_limit:]
            subscribers = list(self._subscribers.get(type(event), []))

        for callback, async_dispatch in subscribers:
            if async_dispatch:
                t = threading.Thread(
                    target=self._safe_call,
                    args=(callback, event),
                    daemon=True,
                    name=f"evt-{event.name}",
                )
                t.start()
            else:
                self._safe_call(callback, event)

    @staticmethod
    def _safe_call(callback: Callable, event: BaseEvent) -> None:
        try:
            callback(event)
        except Exception:
            logger.error(
                f"[EventBus] Exception in subscriber {callback.__qualname__} "
                f"for {event.name}:\n{traceback.format_exc()}"
            )

    def get_history(
        self,
        event_type: Optional[Type[BaseEvent]] = None,
        limit: int = 50,
    ) -> List[BaseEvent]:
        """Return recent events, optionally filtered by type."""
        with self._lock:
            history = list(self._history)
        if event_type:
            history = [e for e in history if isinstance(e, event_type)]
        return history[-limit:]

    def subscriber_count(self, event_type: Type[BaseEvent]) -> int:
        with self._lock:
            return len(self._subscribers.get(event_type, []))

    def clear_history(self) -> None:
        with self._lock:
            self._history.clear()


# ── Global singleton ──────────────────────────────────────────────────────────
# Import from anywhere:  from core.events import bus
bus: EventBus = EventBus()