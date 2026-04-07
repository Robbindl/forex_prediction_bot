from __future__ import annotations

import threading
from datetime import datetime
from typing import Callable, Dict, Optional

from services.ig_market_bridge import IGRequestError, _normalize_ig_commodity_price, ig_market_bridge
from services.market_data_router import filter_ig_primary_assets
from utils.logger import get_logger

logger = get_logger()

try:
    from lightstreamer.client import LightstreamerClient, Subscription
    from lightstreamer.client.ls_python_client_api import (
        ClientListener,
        ItemUpdate,
        SubscriptionListener,
    )

    _LIGHTSTREAMER_AVAILABLE = True
except Exception:  # pragma: no cover - optional dependency path
    LightstreamerClient = None
    Subscription = None
    ClientListener = object
    SubscriptionListener = object
    ItemUpdate = object
    _LIGHTSTREAMER_AVAILABLE = False

_PRICE_FIELDS = [
    "BIDPRICE1",
    "ASKPRICE1",
    "BIDSIZE1",
    "ASKSIZE1",
    "BIDPRICE2",
    "ASKPRICE2",
    "BIDSIZE2",
    "ASKSIZE2",
    "BIDPRICE3",
    "ASKPRICE3",
    "BIDSIZE3",
    "ASKSIZE3",
    "TIMESTAMP",
    "DLG_FLAG",
]
_LS_ADAPTER_SET = "DEFAULT"
_LS_DATA_ADAPTER = "Pricing"
_RECONNECT_DELAY_SEC = 5.0


def _safe_float(value) -> Optional[float]:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except Exception:
        return None


class _IGClientListener(ClientListener):
    def __init__(self, manager: "IGStreamingManager") -> None:
        self._manager = manager

    def onStatusChange(self, status):
        self._manager._handle_status_change(status)

    def onServerError(self, code, message):
        self._manager._handle_server_error(code, message)


class _IGSubscriptionListener(SubscriptionListener):
    def __init__(self, manager: "IGStreamingManager") -> None:
        self._manager = manager

    def onSubscription(self):
        self._manager._handle_subscription_ready()

    def onSubscriptionError(self, code, message):
        self._manager._handle_subscription_error(code, message)

    def onItemUpdate(self, update: ItemUpdate):
        self._manager._handle_item_update(update)


class IGStreamingManager:
    """Lightstreamer-backed IG price stream for routed commodities."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._callbacks: list[Callable] = []
        self._asset_categories: Dict[str, str] = {}
        self._asset_to_epic: Dict[str, str] = {}
        self._epic_to_asset: Dict[str, str] = {}
        self._client: Optional[LightstreamerClient] = None
        self._subscription: Optional[Subscription] = None
        self._running = False
        self._account_id = ""
        self._reconnect_timer: Optional[threading.Timer] = None

    def is_available(self) -> bool:
        return bool(_LIGHTSTREAMER_AVAILABLE and ig_market_bridge.list_profiles())

    def filter_streamable_assets(self, asset_map: Dict[str, str]) -> Dict[str, str]:
        if not self.is_available():
            return {}
        streamable: Dict[str, str] = {}
        for asset, category in filter_ig_primary_assets(asset_map or {}).items():
            try:
                resolved = ig_market_bridge.resolve_symbol_info(asset, category=category)
            except Exception:
                resolved = None
            if resolved and resolved.get("streaming_prices_available") and resolved.get("symbol"):
                streamable[str(asset)] = str(category or "")
        return streamable

    def subscribe_prices(self, asset_map: Dict[str, str], callback: Callable) -> Dict[str, str]:
        streamable = self.filter_streamable_assets(asset_map or {})
        with self._lock:
            if callback not in self._callbacks:
                self._callbacks.append(callback)

            self._asset_categories = dict(streamable)
            self._asset_to_epic = {}
            self._epic_to_asset = {}

            for asset, category in streamable.items():
                try:
                    resolved = ig_market_bridge.resolve_symbol_info(asset, category=category)
                except Exception:
                    resolved = None
                epic = str((resolved or {}).get("symbol") or "").strip()
                if not epic:
                    continue
                self._asset_to_epic[str(asset)] = epic
                self._epic_to_asset[epic] = str(asset)

            if not self._asset_to_epic:
                self._running = False
                self._cancel_reconnect_locked()
                self._unsubscribe_locked()
                self._disconnect_client_locked()
                try:
                    from websocket_dashboard import set_connected

                    set_connected("ig", False, 0)
                except Exception:
                    pass
                return {}

            self._running = True
            if self._client is None:
                self._connect_locked()
            else:
                self._apply_subscription_locked()
            return dict(self._asset_categories)

    def stop(self) -> None:
        with self._lock:
            self._running = False
            self._cancel_reconnect_locked()
            self._unsubscribe_locked()
            self._disconnect_client_locked()
        try:
            from websocket_dashboard import set_connected

            set_connected("ig", False, 0)
        except Exception:
            pass

    def get_subscribed_assets(self) -> Dict[str, str]:
        with self._lock:
            return dict(self._asset_categories)

    def _connect_locked(self) -> None:
        if not self.is_available() or not self._asset_to_epic:
            return
        session = ig_market_bridge.get_streaming_session()
        self._account_id = str(session.get("account_id") or "").strip()
        endpoint = str(session.get("lightstreamer_endpoint") or "").strip()
        password = str(session.get("password") or "").strip()
        if not endpoint or not self._account_id or not password:
            raise IGRequestError("invalid_streaming_session", "IG streaming session details are incomplete.")

        self._disconnect_client_locked()
        client = LightstreamerClient(endpoint, _LS_ADAPTER_SET)
        client.connectionDetails.setUser(self._account_id)
        client.connectionDetails.setPassword(password)
        try:
            client.connectionOptions.setForcedTransport("WS-STREAMING")
        except Exception:
            pass
        client.addListener(_IGClientListener(self))
        self._client = client
        client.connect()
        self._apply_subscription_locked()

    def _disconnect_client_locked(self) -> None:
        if self._client is None:
            return
        try:
            self._client.disconnect()
        except Exception:
            pass
        self._client = None

    def _unsubscribe_locked(self) -> None:
        if self._client is None or self._subscription is None:
            self._subscription = None
            return
        try:
            self._client.unsubscribe(self._subscription)
        except Exception:
            pass
        self._subscription = None

    def _apply_subscription_locked(self) -> None:
        if self._client is None or not self._asset_to_epic or not self._account_id:
            return
        self._unsubscribe_locked()
        items = [
            f"PRICE:{self._account_id}:{epic}"
            for _asset, epic in sorted(self._asset_to_epic.items())
        ]
        subscription = Subscription("MERGE", items, _PRICE_FIELDS)
        try:
            subscription.setDataAdapter(_LS_DATA_ADAPTER)
        except Exception:
            pass
        subscription.addListener(_IGSubscriptionListener(self))
        self._subscription = subscription
        self._client.subscribe(subscription)

    def _schedule_reconnect_locked(self) -> None:
        if not self._running or not self._asset_to_epic:
            return
        if self._reconnect_timer is not None:
            return

        def _reconnect():
            try:
                with self._lock:
                    self._reconnect_timer = None
                    if not self._running or not self._asset_to_epic:
                        return
                    self._connect_locked()
            except Exception as exc:
                logger.warning(f"[IGStream] reconnect failed: {exc}")
                with self._lock:
                    self._schedule_reconnect_locked()

        timer = threading.Timer(_RECONNECT_DELAY_SEC, _reconnect)
        timer.daemon = True
        self._reconnect_timer = timer
        timer.start()

    def _cancel_reconnect_locked(self) -> None:
        if self._reconnect_timer is None:
            return
        try:
            self._reconnect_timer.cancel()
        except Exception:
            pass
        self._reconnect_timer = None

    def _handle_status_change(self, status) -> None:
        normalized = str(status or "").upper()
        try:
            from websocket_dashboard import set_connected

            if normalized.startswith("CONNECTED"):
                with self._lock:
                    self._cancel_reconnect_locked()
                    symbol_count = len(self._asset_to_epic)
                set_connected("ig", True, symbol_count)
                logger.info(f"[IGStream] status {normalized}")
            elif normalized.startswith("DISCONNECTED"):
                with self._lock:
                    symbol_count = len(self._asset_to_epic)
                    running = self._running
                    self._schedule_reconnect_locked()
                set_connected("ig", False, symbol_count)
                if running:
                    logger.warning(f"[IGStream] status {normalized}")
                else:
                    logger.debug(f"[IGStream] status {normalized}")
            else:
                logger.debug(f"[IGStream] status {normalized}")
        except Exception:
            pass

    def _handle_server_error(self, code, message) -> None:
        logger.warning(f"[IGStream] server error {code}: {message}")
        with self._lock:
            self._schedule_reconnect_locked()

    def _handle_subscription_ready(self) -> None:
        logger.info(f"[IGStream] subscribed {sorted(self._asset_to_epic.keys())}")

    def _handle_subscription_error(self, code, message) -> None:
        logger.warning(f"[IGStream] subscription error {code}: {message}")

    def _handle_item_update(self, update: ItemUpdate) -> None:
        item_name = str(update.getItemName() or "")
        parts = item_name.split(":", 2)
        if len(parts) < 3:
            return
        epic = parts[2]
        with self._lock:
            asset = self._epic_to_asset.get(epic)
            callbacks = list(self._callbacks)
            symbol_count = len(self._asset_to_epic)
        if not asset:
            return

        fields = update.getFields() or {}
        bid = _safe_float(fields.get("BIDPRICE1") or fields.get("BID"))
        ask = _safe_float(fields.get("ASKPRICE1") or fields.get("OFFER"))
        bid_size = _safe_float(fields.get("BIDSIZE1"))
        ask_size = _safe_float(fields.get("ASKSIZE1"))
        bid = _normalize_ig_commodity_price(asset, bid)
        ask = _normalize_ig_commodity_price(asset, ask)
        if bid is None and ask is None:
            return
        if bid is not None and ask is not None:
            price = (bid + ask) / 2.0
        else:
            price = ask if ask is not None else bid
        if price is None or price <= 0:
            return

        timestamp = fields.get("TIMESTAMP") or fields.get("UTM")
        as_of = datetime.utcnow()
        try:
            if timestamp not in (None, ""):
                numeric = float(timestamp)
                if numeric > 10_000_000_000:
                    numeric /= 1000.0
                as_of = datetime.fromtimestamp(numeric)
        except Exception:
            pass

        levels = []
        for level in (1, 2, 3):
            level_bid = _safe_float(fields.get(f"BIDPRICE{level}"))
            level_ask = _safe_float(fields.get(f"ASKPRICE{level}"))
            level_bid_size = _safe_float(fields.get(f"BIDSIZE{level}"))
            level_ask_size = _safe_float(fields.get(f"ASKSIZE{level}"))
            level_bid = _normalize_ig_commodity_price(asset, level_bid)
            level_ask = _normalize_ig_commodity_price(asset, level_ask)
            if any(value is not None for value in (level_bid, level_ask, level_bid_size, level_ask_size)):
                levels.append(
                    {
                        "bid": level_bid,
                        "ask": level_ask,
                        "bid_size": level_bid_size,
                        "ask_size": level_ask_size,
                    }
                )

        try:
            from services.live_microstructure_service import get_service as get_live_microstructure_service

            get_live_microstructure_service().record_quote(
                "ig",
                asset,
                bid=bid,
                ask=ask,
                price=price,
                bid_size=bid_size,
                ask_size=ask_size,
                levels=levels,
                timestamp=as_of,
                flags=str(fields.get("DLG_FLAG") or ""),
            )
        except Exception:
            pass

        try:
            from websocket_dashboard import mark_feed_activity

            mark_feed_activity("ig", symbol_count)
        except Exception:
            pass

        for callback in callbacks:
            try:
                callback("IG", asset, float(price), None, None, as_of)
            except Exception as exc:
                logger.error(f"[IGStream] callback error for {asset}: {exc}")


ig_streaming_manager = IGStreamingManager()
