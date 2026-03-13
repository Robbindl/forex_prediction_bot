"""
core — Phase 2 central engine package.

Public API:
    from core import TradingCore, bus, state, registry
    from core.events import TradeOpenedEvent, ...
    from core.state  import SystemState
    from core.assets import AssetRegistry
    from core.engine import TradingCore
"""

from core.events import bus, EventBus
from core.state  import state, SystemState
from core.assets import registry, AssetRegistry
from core.engine import TradingCore

__all__ = [
    "TradingCore",
    "EventBus", "bus",
    "SystemState", "state",
    "AssetRegistry", "registry",
]