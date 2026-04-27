import asyncio
import json
import threading
import time
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple

import websockets

from config.config import DERIV_APP_ID
from services.binance_market_bridge import binance_market_bridge
from services.deriv_bridge import deriv_bridge
from services.market_data_router import (
    filter_deriv_stream_assets,
    filter_ig_primary_assets,
    is_binance_primary_crypto_asset,
)
from utils.logger import logger

_DERIV_PUBLIC_WS_URL = "wss://api.derivws.com/trading/v1/options/ws/public"


class WebSocketManager:
    """
    Routed market stream manager with Deriv/Binance live streams.

    Deriv remains the live-stream source for non-IG-routed assets where it has
    coverage. Binance is used for selected spot crypto assets such as BNB, SOL,
    and XRP, even if Deriv later advertises a symbol for them. IG-routed assets
    are filtered out defensively so this manager cannot silently pull routed
    commodities back onto Deriv.
    """

    def __init__(self):
        app_id = str(DERIV_APP_ID or "").strip()
        self.deriv_url = _DERIV_PUBLIC_WS_URL
        self._deriv_headers = {"Deriv-App-ID": app_id} if app_id else {}
        self.running = False
        self.loop = None
        self.thread = None
        self.loop_ready = False
        self._stream_started = False
        self._callbacks: List[Callable] = []
        self._asset_categories: Dict[str, str] = {}
        self._asset_to_symbol: Dict[str, str] = {}
        self._symbol_to_asset: Dict[str, str] = {}
        self._binance_asset_to_symbol: Dict[str, str] = {}
        self._binance_tasks: Dict[str, asyncio.Task] = {}
        self._ws = None
        self._deriv_degraded = False
        self._binance_degraded_assets: Dict[str, bool] = {}
        self._lock = threading.RLock()
        if not app_id:
            logger.warning("[WSManager] DERIV_APP_ID is not configured; Deriv streaming will not start")
        logger.info("[WSManager] Initialized (Deriv primary, Binance secondary)")

    def start(self):
        self.running = True
        self.thread = threading.Thread(target=self._run_loop, daemon=True)
        self.thread.start()
        while not self.loop_ready:
            time.sleep(0.1)
        logger.info("[WSManager] Started")

    def _run_loop(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.loop_ready = True
        self.loop.run_forever()

    def _schedule(self, coro):
        while not self.loop_ready:
            time.sleep(0.1)
        asyncio.run_coroutine_threadsafe(coro, self.loop)

    def subscribe_deriv(self, assets: Dict[str, str], callback: Callable, include_ig_assets: bool = False):
        """
        Subscribe to canonical assets via Deriv.

        `assets` is a mapping of canonical asset -> category.
        If include_ig_assets is True, do not filter out IG-routed assets before
        attempting Deriv stream subscription. This is used for fallback when IG
        streaming or IG quote polling is not available.
        """
        if include_ig_assets:
            tracked_assets = dict(assets or {})
            skipped_ig_assets: list[str] = []
        else:
            tracked_assets = filter_deriv_stream_assets(assets or {})
            skipped_ig_assets = sorted(filter_ig_primary_assets(assets or {}).keys())

        with self._lock:
            if callback not in self._callbacks:
                self._callbacks.append(callback)
            for asset, category in tracked_assets.items():
                if asset:
                    self._asset_categories[str(asset)] = str(category or "")

        if skipped_ig_assets:
            logger.info(
                f"[WSManager] Skipping IG-routed assets from Deriv stream: {skipped_ig_assets}"
            )

        if not tracked_assets and not self._asset_categories:
            logger.info("[WSManager] No Deriv/Binance stream assets to track after routing filters")
            return

        if not self._stream_started:
            self._stream_started = True
            self._schedule(self._connect_deriv_with_reconnect())
        else:
            self._schedule(self._subscribe_pending_assets())

        logger.info(f"[WSManager] Tracking {sorted(self._asset_categories.keys())}")

    async def _connect_deriv_with_reconnect(self):
        from websocket_dashboard import set_connected

        backoff = 5
        max_backoff = 60
        while self.running:
            t0 = asyncio.get_event_loop().time()
            try:
                await self._connect_deriv()
                backoff = 5
            except Exception as e:
                if asyncio.get_event_loop().time() - t0 > 30:
                    backoff = 5
                set_connected("deriv", False)
                if not self._deriv_degraded:
                    logger.warning(f"[WSManager] Deriv stream lost: {e} - retry in {backoff}s")
                    self._deriv_degraded = True
                else:
                    logger.debug(f"[WSManager] Deriv stream still unavailable: {e} - retry in {backoff}s")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)

    async def _connect_deriv(self):
        from websocket_dashboard import set_connected

        async with self._connect_socket(self.deriv_url, headers=self._deriv_headers) as ws:
            self._ws = ws
            await self._subscribe_pending_assets()
            set_connected("deriv", True, len(self._asset_to_symbol))
            self._deriv_degraded = False
            logger.info("[WSManager] Deriv stream connected")

            heartbeat = asyncio.create_task(self._heartbeat(ws))
            try:
                async for message in ws:
                    await self._handle_message(message)
            finally:
                heartbeat.cancel()
                self._ws = None
                with self._lock:
                    self._asset_to_symbol.clear()
                    self._symbol_to_asset.clear()
                set_connected("deriv", False, 0)

    async def _heartbeat(self, ws):
        while self.running:
            await asyncio.sleep(25)
            try:
                await ws.send(json.dumps({"ping": 1}))
            except Exception:
                break

    async def _subscribe_pending_assets(self):
        await self._subscribe_pending_deriv_assets()
        await self._subscribe_pending_binance_assets()

    async def _subscribe_pending_deriv_assets(self):
        if self._ws is None:
            return

        from websocket_dashboard import set_connected

        with self._lock:
            items = list(self._asset_categories.items())

        for asset, category in items:
            if asset in self._asset_to_symbol:
                continue
            if is_binance_primary_crypto_asset(asset, category):
                continue
            resolved = deriv_bridge.resolve_symbol_info(asset, category=category)
            if not resolved:
                logger.debug(f"[WSManager] Deriv has no live symbol for {asset} ({category})")
                continue

            deriv_symbol = str(resolved.get("symbol", "")).strip()
            if not deriv_symbol:
                continue

            await self._ws.send(json.dumps({"ticks": deriv_symbol, "subscribe": 1}))
            self._asset_to_symbol[asset] = deriv_symbol
            self._symbol_to_asset[deriv_symbol] = asset

        set_connected("deriv", bool(self._asset_to_symbol), len(self._asset_to_symbol))

    async def _subscribe_pending_binance_assets(self):
        from websocket_dashboard import set_connected

        with self._lock:
            items = list(self._asset_categories.items())

        for asset, category in items:
            if asset in self._binance_tasks:
                continue
            if asset in self._asset_to_symbol:
                continue
            resolved = binance_market_bridge.resolve_symbol_info(asset, category=category)
            if not resolved:
                continue

            symbol = str(resolved.get("symbol", "")).strip()
            if not symbol:
                continue

            task = asyncio.create_task(self._connect_binance_with_reconnect(asset, symbol))
            self._binance_tasks[asset] = task
            self._binance_asset_to_symbol[asset] = symbol
            self._binance_degraded_assets.setdefault(asset, False)

        set_connected("binance", bool(self._binance_asset_to_symbol), len(self._binance_asset_to_symbol))

    async def _connect_binance_with_reconnect(self, asset: str, symbol: str):
        from websocket_dashboard import set_connected

        url = f"wss://stream.binance.com:9443/ws/{symbol.lower()}@bookTicker"
        backoff = 5
        max_backoff = 60
        while self.running:
            t0 = asyncio.get_event_loop().time()
            try:
                async with self._connect_socket(url) as ws:
                    set_connected("binance", True, len(self._binance_asset_to_symbol))
                    logger.info(f"[WSManager] Binance stream connected for {asset} ({symbol})")
                    async for message in ws:
                        await self._handle_binance_message(asset, symbol, message)
                backoff = 5
            except asyncio.CancelledError:
                break
            except Exception as exc:
                if asyncio.get_event_loop().time() - t0 > 30:
                    backoff = 5
                set_connected("binance", False, len(self._binance_asset_to_symbol))
                if not self._binance_degraded_assets.get(asset):
                    logger.warning(f"[WSManager] Binance stream lost for {asset}: {exc} - retry in {backoff}s")
                    self._binance_degraded_assets[asset] = True
                else:
                    logger.debug(f"[WSManager] Binance stream still unavailable for {asset}: {exc} - retry in {backoff}s")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)

    async def _handle_binance_message(self, asset: str, symbol: str, message: str):
        from websocket_dashboard import set_connected

        try:
            data = json.loads(message)
        except Exception:
            return

        bid = data.get("b")
        ask = data.get("a")
        try:
            bid_f = float(bid or 0.0)
            ask_f = float(ask or 0.0)
        except Exception:
            return

        if bid_f <= 0 and ask_f <= 0:
            return

        price = (bid_f + ask_f) / 2.0 if bid_f > 0 and ask_f > 0 else (ask_f if ask_f > 0 else bid_f)
        ts = datetime.now()
        try:
            from services.live_microstructure_service import get_service as get_live_microstructure_service

            get_live_microstructure_service().record_quote(
                "binance",
                asset,
                bid=bid_f if bid_f > 0 else None,
                ask=ask_f if ask_f > 0 else None,
                price=price,
                timestamp=ts,
            )
        except Exception:
            pass
        self._binance_degraded_assets[asset] = False
        set_connected("binance", True, len(self._binance_asset_to_symbol))
        for callback in list(self._callbacks):
            try:
                callback("BinanceStream", asset, price, None, None, ts)
            except Exception as exc:
                logger.error(f"[WSManager] callback error for {asset}: {exc}")

    @staticmethod
    def _parse_message_payload(message: str) -> Optional[Dict[str, Any]]:
        try:
            return json.loads(message)
        except Exception:
            return None

    @staticmethod
    def _tick_price(tick: Dict[str, Any]) -> Optional[float]:
        price = tick.get("quote")
        try:
            if price is None:
                bid = float(tick.get("bid", 0) or 0)
                ask = float(tick.get("ask", 0) or 0)
                price = (bid + ask) / 2.0 if bid > 0 and ask > 0 else None
            if price is None:
                return None
            return float(price)
        except Exception:
            return None

    @staticmethod
    def _tick_timestamp(tick: Dict[str, Any]) -> datetime:
        ts = datetime.now()
        epoch = tick.get("epoch")
        try:
            if epoch:
                ts = datetime.fromtimestamp(float(epoch))
        except Exception:
            pass
        return ts

    @staticmethod
    def _tick_bid_ask(tick: Dict[str, Any]) -> Tuple[float, float]:
        try:
            return float(tick.get("bid", 0) or 0), float(tick.get("ask", 0) or 0)
        except Exception:
            return 0.0, 0.0

    def _emit_deriv_tick(self, asset: str, price: float, bid: float, ask: float, ts: datetime) -> None:
        from websocket_dashboard import set_connected

        try:
            from services.live_microstructure_service import get_service as get_live_microstructure_service

            get_live_microstructure_service().record_quote(
                "deriv",
                asset,
                bid=bid if bid > 0 else None,
                ask=ask if ask > 0 else None,
                price=price,
                timestamp=ts,
            )
        except Exception:
            pass

        set_connected("deriv", True, len(self._asset_to_symbol))
        for callback in list(self._callbacks):
            try:
                callback("deriv", asset, price, None, None, ts)
            except Exception as exc:
                logger.error(f"[WSManager] callback error for {asset}: {exc}")

    async def _handle_deriv_tick(self, tick: Dict[str, Any]) -> None:
        deriv_symbol = str(tick.get("symbol", "")).strip()
        asset = self._symbol_to_asset.get(deriv_symbol)
        if not asset:
            return

        price = self._tick_price(tick)
        if price is None:
            return

        if price <= 0:
            return

        bid, ask = self._tick_bid_ask(tick)
        ts = self._tick_timestamp(tick)
        self._emit_deriv_tick(asset, price, bid, ask, ts)

    async def _handle_message(self, message: str):
        data = self._parse_message_payload(message)
        if data is None:
            return

        if data.get("error"):
            logger.debug(f"[WSManager] Deriv stream error: {data['error']}")
            return

        msg_type = str(data.get("msg_type", "")).lower()
        if msg_type == "ping":
            return

        if msg_type == "tick":
            await self._handle_deriv_tick(data.get("tick") or {})

    @staticmethod
    def _connect_socket(url: str, headers: Optional[Dict[str, str]] = None):
        kwargs = {"ping_interval": None, "close_timeout": 2}
        if headers:
            try:
                return websockets.connect(url, additional_headers=headers, **kwargs)
            except TypeError:
                return websockets.connect(url, extra_headers=headers, **kwargs)
        return websockets.connect(url, **kwargs)

    def stop(self):
        self.running = False
        for task in list(self._binance_tasks.values()):
            try:
                task.cancel()
            except Exception:
                pass
        if self.loop:
            self.loop.call_soon_threadsafe(self.loop.stop)
        logger.info("[WSManager] Stopped")

    def get_subscribed_assets(self) -> Dict[str, List[str]]:
        return {
            "deriv": sorted(self._asset_to_symbol.keys()),
            "binance": sorted(self._binance_asset_to_symbol.keys()),
        }
