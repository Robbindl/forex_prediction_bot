import asyncio
import json
import threading
import time
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple

import websockets

from config.config import BYBIT_PUBLIC_LINEAR_WS_URL, DERIV_APP_ID, OKX_PUBLIC_WS_URL
from services.binance_market_bridge import binance_market_bridge
from services.bybit_market_bridge import bybit_market_bridge
from services.deriv_bridge import deriv_bridge
from services.okx_market_bridge import okx_market_bridge
from services.market_data_router import (
    filter_deriv_stream_assets,
    filter_ig_primary_assets,
    is_binance_primary_crypto_asset,
    is_bybit_supported_commodity_asset,
    is_okx_supported_commodity_asset,
)
from utils.logger import logger

_DERIV_PUBLIC_WS_URL = "wss://api.derivws.com/trading/v1/options/ws/public"
_BYBIT_DEPTH_TOPIC = "orderbook.1000"
_BYBIT_TRADE_TOPIC = "publicTrade"
_BYBIT_MAX_LEVELS = 1000
_OKX_DEPTH_CHANNEL = "books"
_OKX_TRADE_CHANNEL = "trades"
_OKX_MAX_LEVELS = 400


class WebSocketManager:
    """
    Routed market stream manager with Deriv/Binance/Bybit/OKX live streams.

    Deriv remains the live-stream source for non-IG-routed assets where it has
    coverage. Binance is used for selected spot crypto assets such as BNB, SOL,
    and XRP, even if Deriv later advertises a symbol for them. Bybit is used as
    the primary deep commodity book for supported metals and WTI, while OKX
    remains the fallback commodity exchange-depth source for assets Bybit does
    not expose cleanly through the public API.
    """

    def __init__(self):
        app_id = str(DERIV_APP_ID or "").strip()
        self.deriv_url = _DERIV_PUBLIC_WS_URL
        self.bybit_url = str(BYBIT_PUBLIC_LINEAR_WS_URL or "").strip()
        self.okx_url = str(OKX_PUBLIC_WS_URL or "").strip()
        self._deriv_headers = {"Deriv-App-ID": app_id} if app_id else {}
        self.running = False
        self.loop = None
        self.thread = None
        self.loop_ready = False
        self._stream_started = False
        self._bybit_stream_started = False
        self._okx_stream_started = False
        self._callbacks: List[Callable] = []
        self._asset_categories: Dict[str, str] = {}
        self._asset_to_symbol: Dict[str, str] = {}
        self._symbol_to_asset: Dict[str, str] = {}
        self._binance_asset_to_symbol: Dict[str, str] = {}
        self._bybit_asset_to_symbol: Dict[str, str] = {}
        self._bybit_symbol_to_asset: Dict[str, str] = {}
        self._okx_asset_to_symbol: Dict[str, str] = {}
        self._okx_symbol_to_asset: Dict[str, str] = {}
        self._bybit_books: Dict[str, Dict[str, Any]] = {}
        self._okx_books: Dict[str, Dict[str, Any]] = {}
        self._binance_tasks: Dict[str, asyncio.Task] = {}
        self._ws = None
        self._bybit_ws = None
        self._okx_ws = None
        self._deriv_degraded = False
        self._binance_degraded_assets: Dict[str, bool] = {}
        self._bybit_degraded = False
        self._okx_degraded = False
        self._lock = threading.RLock()
        if not app_id:
            logger.warning("[WSManager] DERIV_APP_ID is not configured; Deriv streaming will not start")
        logger.info("[WSManager] Initialized (Deriv primary, Binance secondary, Bybit metals/WTI depth, OKX commodity fallback)")

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

    @staticmethod
    def _stream_health_service():
        try:
            from services.dom_stream_health_service import get_service as get_dom_stream_health_service

            return get_dom_stream_health_service()
        except Exception:
            return None

    def _mark_stream_connected(self, provider: str, asset: str = "", *, ts: Optional[datetime] = None) -> None:
        service = self._stream_health_service()
        if service is None:
            return
        try:
            service.mark_connected(provider, asset, ts=(ts.timestamp() if isinstance(ts, datetime) else None))
        except Exception:
            pass

    def _mark_stream_disconnected(
        self,
        provider: str,
        asset: str = "",
        *,
        reason: str = "",
        reconnect: bool = False,
    ) -> None:
        service = self._stream_health_service()
        if service is None:
            return
        try:
            service.mark_disconnected(provider, asset, reason=reason, reconnect=reconnect)
        except Exception:
            pass

    def _note_stream_depth(self, provider: str, asset: str, *, ts: Optional[datetime] = None) -> None:
        service = self._stream_health_service()
        if service is None:
            return
        try:
            service.note_depth(provider, asset, ts=(ts.timestamp() if isinstance(ts, datetime) else None))
        except Exception:
            pass

    def _note_stream_trade(self, provider: str, asset: str, *, ts: Optional[datetime] = None) -> None:
        service = self._stream_health_service()
        if service is None:
            return
        try:
            service.note_trade(provider, asset, ts=(ts.timestamp() if isinstance(ts, datetime) else None))
        except Exception:
            pass

    def _note_stream_sequence_gap(self, provider: str, asset: str = "", *, reason: str = "") -> None:
        service = self._stream_health_service()
        if service is None:
            return
        try:
            service.note_sequence_gap(provider, asset, reason=reason)
        except Exception:
            pass

    @staticmethod
    def _filter_okx_stream_assets(assets: Dict[str, str]) -> Dict[str, str]:
        selected: Dict[str, str] = {}
        for asset, category in (assets or {}).items():
            asset_text = str(asset or "")
            category_text = str(category or "")
            if not asset_text:
                continue
            if (
                is_okx_supported_commodity_asset(asset_text, category_text)
                and not is_bybit_supported_commodity_asset(asset_text, category_text)
            ):
                selected[asset_text] = category_text
        return selected

    @classmethod
    def _has_okx_assets(cls, assets: Dict[str, str]) -> bool:
        return bool(cls._filter_okx_stream_assets(assets))

    @staticmethod
    def _filter_bybit_stream_assets(assets: Dict[str, str]) -> Dict[str, str]:
        selected: Dict[str, str] = {}
        for asset, category in (assets or {}).items():
            asset_text = str(asset or "")
            category_text = str(category or "")
            if not asset_text:
                continue
            if is_bybit_supported_commodity_asset(asset_text, category_text):
                selected[asset_text] = category_text
        return selected

    @classmethod
    def _has_bybit_assets(cls, assets: Dict[str, str]) -> bool:
        return bool(cls._filter_bybit_stream_assets(assets))

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
            tracked_assets.update(self._filter_bybit_stream_assets(assets or {}))
            tracked_assets.update(self._filter_okx_stream_assets(assets or {}))

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
            logger.info("[WSManager] No Deriv/Binance/Bybit/OKX stream assets to track after routing filters")
            return

        if not self._stream_started:
            self._stream_started = True
            self._schedule(self._connect_deriv_with_reconnect())
            if self.bybit_url and self._has_bybit_assets(tracked_assets):
                self._bybit_stream_started = True
                self._schedule(self._connect_bybit_with_reconnect())
            if self.okx_url and self._has_okx_assets(tracked_assets):
                self._okx_stream_started = True
                self._schedule(self._connect_okx_with_reconnect())
        else:
            self._schedule(self._subscribe_pending_assets())
            if self.bybit_url and self._has_bybit_assets(tracked_assets) and not self._bybit_stream_started:
                self._bybit_stream_started = True
                self._schedule(self._connect_bybit_with_reconnect())
            if self.okx_url and self._has_okx_assets(tracked_assets) and not self._okx_stream_started:
                self._okx_stream_started = True
                self._schedule(self._connect_okx_with_reconnect())

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
                self._mark_stream_disconnected("deriv", reason=str(e), reconnect=True)
                if not self._deriv_degraded:
                    logger.warning(f"[WSManager] Deriv stream lost: {e} - retry in {backoff}s")
                    self._deriv_degraded = True
                else:
                    logger.debug(f"[WSManager] Deriv stream still unavailable: {e} - retry in {backoff}s")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)

    async def _connect_bybit_with_reconnect(self):
        from websocket_dashboard import set_connected

        if not self.bybit_url:
            return

        backoff = 5
        max_backoff = 60
        while self.running:
            t0 = asyncio.get_event_loop().time()
            try:
                await self._connect_bybit()
                backoff = 5
            except Exception as exc:
                if asyncio.get_event_loop().time() - t0 > 30:
                    backoff = 5
                set_connected("bybit", False, len(self._bybit_asset_to_symbol))
                self._mark_stream_disconnected("bybit", reason=str(exc), reconnect=True)
                if not self._bybit_degraded:
                    logger.warning(f"[WSManager] Bybit stream lost: {exc} - retry in {backoff}s")
                    self._bybit_degraded = True
                else:
                    logger.debug(f"[WSManager] Bybit stream still unavailable: {exc} - retry in {backoff}s")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)

    async def _connect_okx_with_reconnect(self):
        from websocket_dashboard import set_connected

        if not self.okx_url:
            return

        backoff = 5
        max_backoff = 60
        while self.running:
            t0 = asyncio.get_event_loop().time()
            try:
                await self._connect_okx()
                backoff = 5
            except Exception as exc:
                if asyncio.get_event_loop().time() - t0 > 30:
                    backoff = 5
                set_connected("okx", False, len(self._okx_asset_to_symbol))
                self._mark_stream_disconnected("okx", reason=str(exc), reconnect=True)
                if not self._okx_degraded:
                    logger.warning(f"[WSManager] OKX stream lost: {exc} - retry in {backoff}s")
                    self._okx_degraded = True
                else:
                    logger.debug(f"[WSManager] OKX stream still unavailable: {exc} - retry in {backoff}s")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)

    async def _connect_deriv(self):
        from websocket_dashboard import set_connected

        async with self._connect_socket(self.deriv_url, headers=self._deriv_headers) as ws:
            self._ws = ws
            await self._subscribe_pending_assets()
            set_connected("deriv", True, len(self._asset_to_symbol))
            self._deriv_degraded = False
            self._mark_stream_connected("deriv")
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
                self._mark_stream_disconnected("deriv", reason="socket_closed")

    async def _connect_bybit(self):
        from websocket_dashboard import set_connected

        async with self._connect_socket(self.bybit_url) as ws:
            self._bybit_ws = ws
            await self._subscribe_pending_bybit_assets()
            set_connected("bybit", True, len(self._bybit_asset_to_symbol))
            self._bybit_degraded = False
            self._mark_stream_connected("bybit")
            logger.info("[WSManager] Bybit stream connected")

            heartbeat = asyncio.create_task(self._bybit_heartbeat(ws))
            try:
                async for message in ws:
                    await self._handle_bybit_message(message)
            finally:
                heartbeat.cancel()
                self._bybit_ws = None
                with self._lock:
                    self._bybit_asset_to_symbol.clear()
                    self._bybit_symbol_to_asset.clear()
                    self._bybit_books.clear()
                set_connected("bybit", False, 0)
                self._mark_stream_disconnected("bybit", reason="socket_closed")

    async def _connect_okx(self):
        from websocket_dashboard import set_connected

        async with self._connect_socket(self.okx_url) as ws:
            self._okx_ws = ws
            await self._subscribe_pending_okx_assets()
            set_connected("okx", True, len(self._okx_asset_to_symbol))
            self._okx_degraded = False
            self._mark_stream_connected("okx")
            logger.info("[WSManager] OKX stream connected")

            heartbeat = asyncio.create_task(self._okx_heartbeat(ws))
            try:
                async for message in ws:
                    await self._handle_okx_message(message)
            finally:
                heartbeat.cancel()
                self._okx_ws = None
                with self._lock:
                    self._okx_asset_to_symbol.clear()
                    self._okx_symbol_to_asset.clear()
                    self._okx_books.clear()
                set_connected("okx", False, 0)
                self._mark_stream_disconnected("okx", reason="socket_closed")

    async def _heartbeat(self, ws):
        while self.running:
            await asyncio.sleep(25)
            try:
                await ws.send(json.dumps({"ping": 1}))
            except Exception:
                break

    async def _bybit_heartbeat(self, ws):
        while self.running:
            await asyncio.sleep(20)
            try:
                await ws.send(json.dumps({"op": "ping"}))
            except Exception:
                break

    async def _okx_heartbeat(self, ws):
        while self.running:
            await asyncio.sleep(25)
            try:
                await ws.send("ping")
            except Exception:
                break

    async def _subscribe_pending_assets(self):
        await self._subscribe_pending_deriv_assets()
        await self._subscribe_pending_binance_assets()
        await self._subscribe_pending_bybit_assets()
        await self._subscribe_pending_okx_assets()

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
            if is_bybit_supported_commodity_asset(asset, category):
                continue
            if is_okx_supported_commodity_asset(asset, category):
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

    async def _subscribe_pending_bybit_assets(self):
        if self._bybit_ws is None:
            return

        from websocket_dashboard import set_connected

        args: list[str] = []
        pending: list[tuple[str, str]] = []
        with self._lock:
            items = list(self._asset_categories.items())

        for asset, category in items:
            if asset in self._bybit_asset_to_symbol:
                continue
            resolved = bybit_market_bridge.resolve_symbol_info(asset, category=category)
            if not resolved:
                continue
            symbol = str(resolved.get("symbol", "")).strip()
            if not symbol:
                continue
            pending.append((asset, symbol))
            args.append(f"{_BYBIT_DEPTH_TOPIC}.{symbol}")
            args.append(f"{_BYBIT_TRADE_TOPIC}.{symbol}")

        if args:
            await self._bybit_ws.send(json.dumps({"op": "subscribe", "args": args}))
            for asset, symbol in pending:
                self._bybit_asset_to_symbol[asset] = symbol
                self._bybit_symbol_to_asset[symbol] = asset

        set_connected("bybit", bool(self._bybit_asset_to_symbol), len(self._bybit_asset_to_symbol))

    async def _subscribe_pending_okx_assets(self):
        if self._okx_ws is None:
            return

        from websocket_dashboard import set_connected

        args: list[dict[str, str]] = []
        pending: list[tuple[str, str]] = []
        with self._lock:
            items = list(self._asset_categories.items())

        for asset, category in items:
            if asset in self._okx_asset_to_symbol:
                continue
            resolved = okx_market_bridge.resolve_symbol_info(asset, category=category)
            if not resolved:
                continue
            symbol = str(resolved.get("symbol", "")).strip()
            if not symbol:
                continue
            pending.append((asset, symbol))
            args.append({"channel": _OKX_DEPTH_CHANNEL, "instId": symbol})
            args.append({"channel": _OKX_TRADE_CHANNEL, "instId": symbol})

        if args:
            await self._okx_ws.send(json.dumps({"op": "subscribe", "args": args}))
            for asset, symbol in pending:
                self._okx_asset_to_symbol[asset] = symbol
                self._okx_symbol_to_asset[symbol] = asset

        set_connected("okx", bool(self._okx_asset_to_symbol), len(self._okx_asset_to_symbol))

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
                self._mark_stream_disconnected("binance", asset, reason=str(exc), reconnect=True)
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
        self._note_stream_depth("binance", asset, ts=ts)
        for callback in list(self._callbacks):
            try:
                callback("BinanceStream", asset, price, None, None, ts)
            except Exception as exc:
                logger.error(f"[WSManager] callback error for {asset}: {exc}")

    async def _handle_bybit_message(self, message: str):
        from websocket_dashboard import set_connected

        data = self._parse_message_payload(message)
        if data is None:
            return

        if data.get("op") == "ping":
            return
        if data.get("op") == "pong" or data.get("ret_msg") == "pong":
            return
        if data.get("success") is True and data.get("op") == "subscribe":
            set_connected("bybit", True, len(self._bybit_asset_to_symbol))
            return

        topic = str(data.get("topic", "") or "").strip()
        if topic.startswith(f"{_BYBIT_TRADE_TOPIC}."):
            trades = list(data.get("data") or [])
            if not trades:
                return
            try:
                from services.live_microstructure_service import get_service as get_live_microstructure_service
            except Exception:
                return

            for trade in trades:
                if not isinstance(trade, dict):
                    continue
                symbol = str(trade.get("s", "") or "").strip()
                asset = self._bybit_symbol_to_asset.get(symbol)
                if not asset:
                    continue
                trade_price = self._safe_float(trade.get("p"))
                if trade_price is None or trade_price <= 0.0:
                    continue
                trade_size = self._safe_float(trade.get("v"))
                trade_side = str(trade.get("S", "") or "").strip().lower()
                book_state = self._bybit_books.get(symbol) or {}
                levels = self._bybit_levels_from_state(book_state) if book_state else []
                top_bid = levels[0].get("bid") if levels else None
                top_ask = levels[0].get("ask") if levels else None
                ts = datetime.now()
                try:
                    raw_ts = trade.get("T") or data.get("ts")
                    if raw_ts not in (None, ""):
                        ts = datetime.fromtimestamp(float(raw_ts) / 1000.0)
                except Exception:
                    pass
                try:
                    get_live_microstructure_service().record_trade(
                        "bybit",
                        asset,
                        price=float(trade_price),
                        size=float(trade_size) if trade_size not in (None, "") else None,
                        side=trade_side,
                        bid=float(top_bid) if top_bid else None,
                        ask=float(top_ask) if top_ask else None,
                        timestamp=ts,
                        flags="trade_print,trade_stream,bybit_ws",
                    )
                except Exception:
                    pass
                self._note_stream_trade("bybit", asset, ts=ts)

            self._bybit_degraded = False
            set_connected("bybit", True, len(self._bybit_asset_to_symbol))
            return

        if not topic.startswith(f"{_BYBIT_DEPTH_TOPIC}."):
            return

        payload = data.get("data") or {}
        symbol = str(payload.get("s", "") or "").strip()
        asset = self._bybit_symbol_to_asset.get(symbol)
        if not asset:
            return

        msg_type = str(data.get("type", "") or "").strip().lower()
        bids = list(payload.get("b") or [])
        asks = list(payload.get("a") or [])

        if msg_type == "snapshot" or symbol not in self._bybit_books:
            book_state = {"bids": {}, "asks": {}, "u": self._safe_int(payload.get("u"))}
            self._bybit_apply_book_side(book_state["bids"], bids)
            self._bybit_apply_book_side(book_state["asks"], asks)
            self._bybit_books[symbol] = book_state
        else:
            book_state = self._bybit_books[symbol]
            self._bybit_apply_book_side(book_state["bids"], bids)
            self._bybit_apply_book_side(book_state["asks"], asks)
            book_state["u"] = self._safe_int(payload.get("u")) or book_state.get("u")

        levels = self._bybit_levels_from_state(self._bybit_books[symbol])
        if not levels:
            return

        top_bid = levels[0].get("bid")
        top_ask = levels[0].get("ask")
        if not top_bid and not top_ask:
            return

        price = (
            (float(top_bid) + float(top_ask)) / 2.0
            if top_bid and top_ask
            else (float(top_ask) if top_ask else float(top_bid))
        )
        ts = datetime.now()
        try:
            raw_ts = payload.get("cts") or data.get("ts")
            if raw_ts not in (None, ""):
                ts = datetime.fromtimestamp(float(raw_ts) / 1000.0)
        except Exception:
            pass

        try:
            from services.live_microstructure_service import get_service as get_live_microstructure_service

            if msg_type == "snapshot":
                get_live_microstructure_service().record_quote(
                    "bybit",
                    asset,
                    bid=float(top_bid) if top_bid else None,
                    ask=float(top_ask) if top_ask else None,
                    price=price,
                    levels=levels,
                    timestamp=ts,
                    flags="depth_snapshot,stream_snapshot,bybit_ws",
                )
            else:
                get_live_microstructure_service().record_depth_delta(
                    "bybit",
                    asset,
                    bid=float(top_bid) if top_bid else None,
                    ask=float(top_ask) if top_ask else None,
                    price=price,
                    levels=levels,
                    timestamp=ts,
                    flags="depth_delta,ladder_delta,bybit_ws",
                )
        except Exception:
            pass

        self._bybit_degraded = False
        set_connected("bybit", True, len(self._bybit_asset_to_symbol))
        self._note_stream_depth("bybit", asset, ts=ts)

    async def _handle_okx_message(self, message: str):
        from websocket_dashboard import set_connected

        if str(message).strip().lower() == "pong":
            return

        data = self._parse_message_payload(message)
        if data is None:
            return

        event = str(data.get("event", "") or "").lower()
        if event == "subscribe":
            set_connected("okx", True, len(self._okx_asset_to_symbol))
            return
        if event == "error":
            logger.debug(f"[WSManager] OKX stream error: {data}")
            return

        arg = data.get("arg") or {}
        action = str(data.get("action", "") or "").strip().lower()
        channel = str(arg.get("channel", "") or "").strip().lower()
        if channel == _OKX_TRADE_CHANNEL:
            symbol = str(arg.get("instId", "") or "").strip()
            asset = self._okx_symbol_to_asset.get(symbol)
            if not asset:
                return
            rows = list(data.get("data") or [])
            if not rows:
                return
            try:
                from services.live_microstructure_service import get_service as get_live_microstructure_service
            except Exception:
                return

            book_state = self._okx_books.get(symbol) or {}
            levels = self._okx_levels_from_state(book_state) if book_state else []
            top_bid = levels[0].get("bid") if levels else None
            top_ask = levels[0].get("ask") if levels else None
            for row in rows:
                if not isinstance(row, dict):
                    continue
                trade_price = self._safe_float(row.get("px"))
                if trade_price is None or trade_price <= 0.0:
                    continue
                trade_size = self._safe_float(row.get("sz"))
                trade_side = str(row.get("side", "") or "").strip().lower()
                ts = datetime.now()
                try:
                    raw_ts = row.get("ts")
                    if raw_ts not in (None, ""):
                        ts = datetime.fromtimestamp(float(raw_ts) / 1000.0)
                except Exception:
                    pass
                try:
                    get_live_microstructure_service().record_trade(
                        "okx",
                        asset,
                        price=float(trade_price),
                        size=float(trade_size) if trade_size not in (None, "") else None,
                        side=trade_side,
                        bid=float(top_bid) if top_bid else None,
                        ask=float(top_ask) if top_ask else None,
                        timestamp=ts,
                        flags="trade_print,trade_stream,okx_ws",
                    )
                except Exception:
                    pass
                self._note_stream_trade("okx", asset, ts=ts)

            self._okx_degraded = False
            set_connected("okx", True, len(self._okx_asset_to_symbol))
            return

        if channel != _OKX_DEPTH_CHANNEL:
            return

        symbol = str(arg.get("instId", "") or "").strip()
        asset = self._okx_symbol_to_asset.get(symbol)
        if not asset:
            return

        rows = list(data.get("data") or [])
        if not rows:
            return
        row = rows[0] or {}
        bids = list(row.get("bids") or [])
        asks = list(row.get("asks") or [])
        if not bids and not asks:
            return

        okx_state = self._okx_books.get(symbol)
        seq_id = self._safe_int(row.get("seqId"))
        prev_seq_id = self._safe_int(row.get("prevSeqId"))

        if action == "snapshot" or okx_state is None:
            okx_state = {"bids": {}, "asks": {}, "seq_id": seq_id}
            self._okx_apply_book_side(okx_state["bids"], bids)
            self._okx_apply_book_side(okx_state["asks"], asks)
            self._okx_books[symbol] = okx_state
        else:
            expected_prev = okx_state.get("seq_id")
            if (
                prev_seq_id is not None
                and expected_prev is not None
                and prev_seq_id != expected_prev
            ):
                logger.warning(
                    f"[WSManager] OKX book sequence gap for {symbol}: "
                    f"expected prevSeqId={expected_prev}, got {prev_seq_id}; reconnecting"
                )
                self._note_stream_sequence_gap("okx", asset, reason="sequence_gap")
                self._okx_books.pop(symbol, None)
                try:
                    if self._okx_ws is not None:
                        await self._okx_ws.close()
                except Exception:
                    pass
                return
            self._okx_apply_book_side(okx_state["bids"], bids)
            self._okx_apply_book_side(okx_state["asks"], asks)
            okx_state["seq_id"] = seq_id if seq_id is not None else expected_prev

        levels = self._okx_levels_from_state(okx_state)

        top_bid = levels[0].get("bid") if levels else None
        top_ask = levels[0].get("ask") if levels else None
        if not top_bid and not top_ask:
            return

        price = (
            (float(top_bid) + float(top_ask)) / 2.0
            if top_bid and top_ask
            else (float(top_ask) if top_ask else float(top_bid))
        )
        ts = datetime.now()
        try:
            raw_ts = row.get("ts")
            if raw_ts not in (None, ""):
                ts = datetime.fromtimestamp(float(raw_ts) / 1000.0)
        except Exception:
            pass

        try:
            from services.live_microstructure_service import get_service as get_live_microstructure_service

            if action == "snapshot":
                get_live_microstructure_service().record_quote(
                    "okx",
                    asset,
                    bid=float(top_bid) if top_bid else None,
                    ask=float(top_ask) if top_ask else None,
                    price=price,
                    levels=levels,
                    timestamp=ts,
                    flags="depth_snapshot,stream_snapshot,okx_ws",
                )
            else:
                get_live_microstructure_service().record_depth_delta(
                    "okx",
                    asset,
                    bid=float(top_bid) if top_bid else None,
                    ask=float(top_ask) if top_ask else None,
                    price=price,
                    levels=levels,
                    timestamp=ts,
                    flags="depth_delta,ladder_delta,okx_ws",
                )
        except Exception:
            pass

        self._okx_degraded = False
        set_connected("okx", True, len(self._okx_asset_to_symbol))
        self._note_stream_depth("okx", asset, ts=ts)

    @staticmethod
    def _safe_int(value: Any) -> Optional[int]:
        try:
            if value in (None, ""):
                return None
            return int(value)
        except Exception:
            return None

    @staticmethod
    def _safe_float(value: Any) -> Optional[float]:
        try:
            if value in (None, ""):
                return None
            return float(value)
        except Exception:
            return None

    @classmethod
    def _bybit_apply_book_side(cls, side_book: Dict[float, float], rows: List[Any]) -> None:
        for entry in rows or []:
            if not entry:
                continue
            price = cls._safe_float((entry or [None])[0])
            size = cls._safe_float((entry or [None, None])[1])
            if price is None or size is None:
                continue
            if size <= 0:
                side_book.pop(price, None)
            else:
                side_book[price] = size

    @classmethod
    def _bybit_levels_from_state(cls, state: Dict[str, Any]) -> List[Dict[str, Any]]:
        bid_items = sorted(
            (state.get("bids") or {}).items(),
            key=lambda item: item[0],
            reverse=True,
        )[:_BYBIT_MAX_LEVELS]
        ask_items = sorted(
            (state.get("asks") or {}).items(),
            key=lambda item: item[0],
        )[:_BYBIT_MAX_LEVELS]

        levels: List[Dict[str, Any]] = []
        for idx in range(max(len(bid_items), len(ask_items))):
            bid_entry = bid_items[idx] if idx < len(bid_items) else None
            ask_entry = ask_items[idx] if idx < len(ask_items) else None
            levels.append(
                {
                    "bid": bid_entry[0] if bid_entry else None,
                    "bid_size": bid_entry[1] if bid_entry else None,
                    "ask": ask_entry[0] if ask_entry else None,
                    "ask_size": ask_entry[1] if ask_entry else None,
                }
            )
        return levels

    @classmethod
    def _okx_apply_book_side(cls, side_book: Dict[float, float], rows: List[Any]) -> None:
        for entry in rows or []:
            if not entry:
                continue
            price = cls._safe_float((entry or [None])[0])
            size = cls._safe_float((entry or [None, None])[1])
            if price is None or size is None:
                continue
            if size <= 0:
                side_book.pop(price, None)
            else:
                side_book[price] = size

    @classmethod
    def _okx_levels_from_state(cls, state: Dict[str, Any]) -> List[Dict[str, Any]]:
        bid_items = sorted(
            (state.get("bids") or {}).items(),
            key=lambda item: item[0],
            reverse=True,
        )[:_OKX_MAX_LEVELS]
        ask_items = sorted(
            (state.get("asks") or {}).items(),
            key=lambda item: item[0],
        )[:_OKX_MAX_LEVELS]

        levels: List[Dict[str, Any]] = []
        for idx in range(max(len(bid_items), len(ask_items))):
            bid_entry = bid_items[idx] if idx < len(bid_items) else None
            ask_entry = ask_items[idx] if idx < len(ask_items) else None
            levels.append(
                {
                    "bid": bid_entry[0] if bid_entry else None,
                    "bid_size": bid_entry[1] if bid_entry else None,
                    "ask": ask_entry[0] if ask_entry else None,
                    "ask_size": ask_entry[1] if ask_entry else None,
                }
            )
        return levels

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
        self._note_stream_depth("deriv", asset, ts=ts)
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
        self._bybit_stream_started = False
        self._okx_stream_started = False
        self._bybit_books.clear()
        self._okx_books.clear()
        self._mark_stream_disconnected("deriv", reason="manager_stopped")
        self._mark_stream_disconnected("bybit", reason="manager_stopped")
        self._mark_stream_disconnected("okx", reason="manager_stopped")
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
            "bybit": sorted(self._bybit_asset_to_symbol.keys()),
            "okx": sorted(self._okx_asset_to_symbol.keys()),
        }
