"""
core/events.py — In-process typed EventBus for the trading platform.
"""
from __future__ import annotations

import threading
import traceback
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Type, TypeVar

from utils.logger import get_logger

logger = get_logger()

E = TypeVar("E", bound="BaseEvent")


@dataclass
class BaseEvent:
    ts: datetime = field(default_factory=datetime.utcnow, init=False)

    @property
    def name(self) -> str:
        return self.__class__.__name__


@dataclass
class TradeOpenedEvent(BaseEvent):
    trade_id: str
    asset: str
    canonical_asset: str
    category: str
    direction: str
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
    exit_reason: str
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
    layer_reached: int
    reason: str


@dataclass
class SignalRejectedEvent(BaseEvent):
    asset: str
    canonical_asset: str
    category: str
    direction: str
    confidence: float
    reject_reason: str
    reject_layer: str


@dataclass
class RiskLimitHitEvent(BaseEvent):
    limit_type: str
    value: float
    threshold: float
    message: str


@dataclass
class PositionUpdateEvent(BaseEvent):
    open_positions: List[Dict]
    balance: float
    daily_pnl: float
    daily_trades: int


@dataclass
class BalanceChangedEvent(BaseEvent):
    old_balance: float
    new_balance: float
    delta: float
    reason: str


@dataclass
class CooldownActivatedEvent(BaseEvent):
    asset: str
    canonical_asset: str
    cooldown_minutes: int
    reason: str


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
    status: str
    issues: List[str]
    ram_pct: float
    cpu_pct: float


@dataclass
class WhaleAlertEvent(BaseEvent):
    asset: str
    direction: str
    size_usd: float
    source: str


class EventBus:
    """Thread-safe in-process pub/sub bus."""

    def __init__(self):
        self._lock: threading.Lock = threading.Lock()
        self._subscribers: Dict[Type[BaseEvent], List[tuple]] = {}
        self._history: List[BaseEvent] = []
        self._history_limit = 500

    def subscribe(
        self,
        event_type: Type[E],
        callback: Callable[[E], None],
        async_dispatch: bool = False,
    ) -> None:
        with self._lock:
            if event_type not in self._subscribers:
                self._subscribers[event_type] = []
            self._subscribers[event_type].append((callback, async_dispatch))

    def unsubscribe(self, event_type: Type[E], callback: Callable[[E], None]) -> None:
        with self._lock:
            subs = self._subscribers.get(event_type, [])
            self._subscribers[event_type] = [
                (cb, a) for cb, a in subs if cb is not callback
            ]

    def emit(self, event: BaseEvent) -> None:
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
                f"[EventBus] Exception in {callback.__qualname__} "
                f"for {event.name}:\n{traceback.format_exc()}"
            )

    def get_history(
        self,
        event_type: Optional[Type[BaseEvent]] = None,
        limit: int = 50,
    ) -> List[BaseEvent]:
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


bus: EventBus = EventBus()