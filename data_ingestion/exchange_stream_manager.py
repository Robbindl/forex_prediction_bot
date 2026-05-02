from __future__ import annotations

import json
import socket
import threading
import time
from typing import Callable, Dict, List, Optional

from utils.logger import get_logger

logger = get_logger()


def _ping_health(source: str) -> None:
    try:
        from monitoring.system_health_service import monitor

        monitor.ping_source(str(source or ""))
    except Exception:
        return None

# ── Exchange WebSocket endpoints ───────────────────────────────────────────────
EXCHANGE_WS_URLS: Dict[str, str] = {
    "binance": "wss://stream.binance.com:9443/stream",
    "bybit":   "wss://stream.bybit.com/v5/public/linear",
    "okx":     "wss://ws.okx.com:8443/ws/v5/public",
}

# ── Subscription payloads per exchange ────────────────────────────────────────
SUBSCRIPTIONS: Dict[str, dict] = {
    "binance": {
        "method": "SUBSCRIBE",
        "params": [
            "btcusdt@ticker", "ethusdt@ticker", "solusdt@ticker",
            "btcusdt@depth@100ms", "ethusdt@depth@100ms",
            "solusdt@depth@100ms", "bnbusdt@depth@100ms",
            "xrpusdt@depth@100ms",
            "btcusdt@aggTrade",
            "ethusdt@aggTrade", "solusdt@aggTrade",
            "bnbusdt@aggTrade", "xrpusdt@aggTrade",
        ],
        "id": 1,
    },
    "bybit": {
        "op": "subscribe",
        "args": [
            "tickers.BTCUSDT", "tickers.ETHUSDT",
            "orderbook.50.BTCUSDT",
            "allLiquidation.BTCUSDT", "allLiquidation.ETHUSDT",
        ],
    },
    "okx": {
        "op": "subscribe",
        "args": [
            {"channel": "tickers",  "instId": "BTC-USDT"},
            {"channel": "tickers",  "instId": "ETH-USDT"},
            {"channel": "books5",   "instId": "BTC-USDT"},
        ],
    },
}

_RUN_FOREVER_KWARGS: Dict[str, dict] = {
    # Binance sends server-side ping frames every ~20s and expects pong replies.
    # Let the client auto-pong instead of layering on an unnecessary client ping loop.
    "binance": {
        "ping_interval": 0,
        "ping_timeout": None,
        "sockopt": ((socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1),),
    },
    # Bybit expects an application-level {"op":"ping"} heartbeat on public streams.
    "bybit": {
        "ping_interval": 0,
        "ping_timeout": None,
        "sockopt": ((socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1),),
    },
    "okx": {
        "ping_interval": 20,
        "ping_timeout": 10,
        "sockopt": ((socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1),),
    },
}

_APP_HEARTBEAT_PAYLOADS: Dict[str, dict] = {
    "bybit": {"op": "ping"},
}

_APP_HEARTBEAT_INTERVALS: Dict[str, int] = {
    "bybit": 20,
}

# How many ms to wait before reconnecting after a drop
_RECONNECT_DELAY_SECS = 5
_MIN_HEALTHY_SESSION_SECS = 60.0


def _stream_error(exchange: str, data: dict) -> Optional[str]:
    if exchange == "binance":
        if data.get("code") is not None and data.get("msg"):
            return f"{data.get('code')}: {data.get('msg')}"
        return None

    if exchange == "bybit":
        if data.get("success") is False:
            return str(data.get("ret_msg") or data.get("retMsg") or "subscription rejected")
        ret_code = data.get("retCode")
        if ret_code not in (None, 0) and data.get("retMsg"):
            return f"{ret_code}: {data.get('retMsg')}"
        return None

    return None


def _is_control_message(exchange: str, data: dict) -> bool:
    if exchange == "binance":
        return "result" in data and "id" in data

    if exchange == "bybit":
        topic = str(data.get("topic") or "")
        if topic:
            return False
        op = str(data.get("op") or "").lower()
        ret_msg = str(data.get("ret_msg") or data.get("retMsg") or "").lower()
        return op in {"ping", "pong", "subscribe"} or ret_msg in {"pong", "subscribe"}

    return False


def _post_run_failure_message(
    *,
    running: bool,
    last_error: Optional[str],
    session_age: float,
    close_code,
    close_msg,
) -> Optional[str]:
    if not running:
        return None
    if last_error:
        return str(last_error)
    if session_age and session_age < _MIN_HEALTHY_SESSION_SECS:
        return f"connection closed after {session_age:.1f}s (code={close_code}, msg={close_msg})"
    return None


def _normalise_many(exchange: str, data: dict) -> List[dict]:
    try:
        if exchange == "binance":
            return _normalise_binance_events(data, exchange)
        elif exchange == "bybit":
            return _normalise_bybit_events(data, exchange)
        elif exchange == "okx":
            return _normalise_okx_events(data, exchange)
    except Exception as exc:
        logger.debug(f"[ExStream] Normalise error ({exchange}): {exc}")
    return []


def _normalise_binance_events(data: dict, exchange: str) -> List[dict]:
    events: List[dict] = []
    # Binance wraps combined streams in {"stream":"...","data":{...}}.
    inner = data.get("data", data)
    ev = inner.get("e", "")
    if ev == "24hrTicker":
        events.append(
            {
                "type": "MARKET_DATA_UPDATE",
                "exchange": exchange,
                "asset": inner["s"],
                "price": float(inner["c"]),
                "volume": float(inner["v"]),
                "change": float(inner["P"]),
                "ts": inner["E"],
            }
        )
    elif ev == "depthUpdate" or (not ev and "lastUpdateId" in inner):
        events.append(
            {
                "type": "ORDER_BOOK_UPDATE",
                "exchange": exchange,
                "asset": inner["s"],
                "bids": inner.get("b", []),
                "asks": inner.get("a", []),
                "first_update_id": inner.get("U"),
                "final_update_id": inner.get("u"),
                "prev_final_update_id": inner.get("pu"),
                "ts": inner["E"],
            }
        )
    elif ev == "aggTrade":
        events.append(
            {
                "type": "TRADE_UPDATE",
                "exchange": exchange,
                "asset": inner["s"],
                "price": float(inner["p"]),
                "qty": float(inner["q"]),
                "side": "SELL" if inner.get("m") else "BUY",
                "ts": inner["T"],
            }
        )
    return events


def _normalise_bybit_events(data: dict, exchange: str) -> List[dict]:
    events: List[dict] = []
    topic = data.get("topic", "")
    d = data.get("data", {})

    if topic.startswith("tickers."):
        events.append(
            {
                "type": "MARKET_DATA_UPDATE",
                "exchange": exchange,
                "asset": d.get("symbol", ""),
                "price": float(d.get("lastPrice", 0) or 0),
                "volume": float(d.get("volume24h", 0) or 0),
                "change": float(d.get("price24hPcnt", 0) or 0),
                "ts": int(time.time() * 1000),
            }
        )
    elif topic.startswith("orderbook."):
        events.append(
            {
                "type": "ORDER_BOOK_UPDATE",
                "exchange": exchange,
                "asset": d.get("s", ""),
                "bids": d.get("b", []),
                "asks": d.get("a", []),
                "ts": int(time.time() * 1000),
            }
        )
    elif topic.startswith("allLiquidation."):
        rows = d if isinstance(d, list) else [d]
        for row in rows:
            if not isinstance(row, dict):
                continue
            events.append(
                {
                    "type": "LIQUIDATION_EVENT",
                    "exchange": exchange,
                    "asset": row.get("s", ""),
                    "side": row.get("S", ""),
                    "qty": float(row.get("v", 0) or 0),
                    "price": float(row.get("p", 0) or 0),
                    "ts": int(row.get("T", 0) or int(time.time() * 1000)),
                }
            )
    return events


def _normalise_okx_events(data: dict, exchange: str) -> List[dict]:
    events: List[dict] = []
    arg = data.get("arg", {})
    rows = data.get("data", [{}])
    channel = arg.get("channel", "")
    if channel == "tickers" and rows:
        row = rows[0]
        events.append(
            {
                "type": "MARKET_DATA_UPDATE",
                "exchange": exchange,
                "asset": row.get("instId", ""),
                "price": float(row.get("last", 0) or 0),
                "volume": float(row.get("vol24h", 0) or 0),
                "change": float(row.get("sodUtc8", 0) or 0),
                "ts": int(time.time() * 1000),
            }
        )
    elif channel == "books5" and rows:
        row = rows[0]
        events.append(
            {
                "type": "ORDER_BOOK_UPDATE",
                "exchange": exchange,
                "asset": arg.get("instId", ""),
                "bids": row.get("bids", []),
                "asks": row.get("asks", []),
                "ts": int(time.time() * 1000),
            }
        )
    return events


class _ExchangeConnection:
    """
    Manages a single persistent WebSocket connection to one exchange.
    Handles auto-reconnect with exponential back-off (capped at 60 s).
    """

    def __init__(
        self,
        exchange: str,
        on_event: Callable[[dict], None],
        running_flag: threading.Event,
    ) -> None:
        self.exchange     = exchange
        self._on_event    = on_event
        self._running     = running_flag
        self._delay       = _RECONNECT_DELAY_SECS
        self._degraded    = False
        self._thread: Optional[threading.Thread] = None

    # ── Public ──────────────────────────────────────────────────────────────

    def start(self) -> None:
        self._thread = threading.Thread(
            target=self._loop,
            name=f"ExStream-{self.exchange}",
            daemon=True,
        )
        self._thread.start()

    # ── Internal ────────────────────────────────────────────────────────────

    def _loop(self) -> None:
        """Connect, read, reconnect on failure."""
        while self._running.is_set():
            try:
                self._connect_and_read()
                self._delay = _RECONNECT_DELAY_SECS   # reset on clean close
            except Exception as exc:
                if not self._degraded:
                    logger.warning(
                        f"[ExStream] {self.exchange} error: {exc} "
                        f"— reconnecting in {self._delay}s"
                    )
                    self._degraded = True
                else:
                    logger.debug(
                        f"[ExStream] {self.exchange} still unavailable: {exc} "
                        f"— reconnecting in {self._delay}s"
                    )
            if self._running.is_set():
                time.sleep(self._delay)
                self._delay = min(self._delay * 2, 60)

    def _connect_and_read(self) -> None:
        import websocket  # pip install websocket-client

        url = EXCHANGE_WS_URLS[self.exchange]
        sub = SUBSCRIPTIONS[self.exchange]
        heartbeat_payload = _APP_HEARTBEAT_PAYLOADS.get(self.exchange)
        heartbeat_interval = _APP_HEARTBEAT_INTERVALS.get(self.exchange, 20)
        session_started_at = {"value": 0.0}
        last_error: Dict[str, Optional[str]] = {"message": None}
        close_state: Dict[str, object] = {"code": None, "msg": None}
        heartbeat_stop = threading.Event()
        heartbeat_thread: Dict[str, Optional[threading.Thread]] = {"value": None}

        def on_open(ws):
            self._ws_on_open(
                ws,
                self.exchange,
                sub,
                heartbeat_payload,
                heartbeat_interval,
                heartbeat_stop,
                heartbeat_thread,
                session_started_at,
                last_error,
            )

        def on_message(ws, raw):
            self._ws_on_message(ws, raw, last_error)

        def on_error(ws, err):
            self._ws_on_error(err, last_error)

        def on_close(ws, code, msg):
            self._ws_on_close(code, msg, heartbeat_stop, close_state)

        ws = websocket.WebSocketApp(
            url,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )
        ws.run_forever(**_RUN_FOREVER_KWARGS.get(self.exchange, {}))
        heartbeat_stop.set()
        thread = heartbeat_thread["value"]
        if thread is not None:
            thread.join(timeout=1.0)

        failure_message = _post_run_failure_message(
            running=self._running.is_set(),
            last_error=last_error["message"],
            session_age=(time.monotonic() - session_started_at["value"]) if session_started_at["value"] else 0.0,
            close_code=close_state["code"],
            close_msg=close_state["msg"],
        )
        if failure_message:
            raise RuntimeError(failure_message)

    def _start_heartbeat(
        self,
        ws,
        heartbeat_payload,
        heartbeat_interval: int,
        heartbeat_stop: threading.Event,
        heartbeat_thread: Dict[str, Optional[threading.Thread]],
        last_error: Dict[str, Optional[str]],
    ) -> None:
        if not heartbeat_payload or heartbeat_thread["value"] is not None:
            return

        def _loop() -> None:
            while self._running.is_set() and not heartbeat_stop.wait(heartbeat_interval):
                sock = getattr(ws, "sock", None)
                if not sock or not getattr(sock, "connected", False):
                    return
                try:
                    ws.send(json.dumps(heartbeat_payload))
                except Exception as exc:
                    last_error["message"] = f"heartbeat failed: {exc}"
                    try:
                        ws.close()
                    except Exception:
                        pass
                    return

        heartbeat_thread["value"] = threading.Thread(
            target=_loop,
            name=f"ExStreamHeartbeat-{self.exchange}",
            daemon=True,
        )
        heartbeat_thread["value"].start()

    def _ws_on_open(
        self,
        ws,
        exchange: str,
        sub: dict,
        heartbeat_payload,
        heartbeat_interval: int,
        heartbeat_stop: threading.Event,
        heartbeat_thread: Dict[str, Optional[threading.Thread]],
        session_started_at: Dict[str, float],
        last_error: Dict[str, Optional[str]],
    ) -> None:
        self._degraded = False
        self._delay = _RECONNECT_DELAY_SECS
        session_started_at["value"] = time.monotonic()
        logger.info(f"[ExStream] {exchange} connected")
        try:
            from services.dom_stream_health_service import get_service as get_dom_stream_health_service

            get_dom_stream_health_service().mark_connected(exchange)
        except Exception:
            pass
        ws.send(json.dumps(sub))
        self._start_heartbeat(ws, heartbeat_payload, heartbeat_interval, heartbeat_stop, heartbeat_thread, last_error)

    def _ws_on_message(self, ws, raw, last_error: Dict[str, Optional[str]]) -> None:
        try:
            data = json.loads(raw)
            stream_error = _stream_error(self.exchange, data)
            if stream_error:
                last_error["message"] = stream_error
                ws.close()
                return
            if _is_control_message(self.exchange, data):
                return
            for event in _normalise_many(self.exchange, data):
                self._on_event(event)
        except Exception as exc:
            logger.debug(f"[ExStream] {self.exchange} parse: {exc}")

    def _ws_on_error(self, err, last_error: Dict[str, Optional[str]]) -> None:
        message = str(err)
        last_error["message"] = message
        if "ping/pong timed out" in message:
            logger.debug(f"[ExStream] {self.exchange} WS ping timeout — reconnecting")
        else:
            logger.debug(f"[ExStream] {self.exchange} WS error: {message}")

    def _ws_on_close(
        self,
        code,
        msg,
        heartbeat_stop: threading.Event,
        close_state: Dict[str, object],
    ) -> None:
        close_state["code"] = code
        close_state["msg"] = msg
        heartbeat_stop.set()
        try:
            from services.dom_stream_health_service import get_service as get_dom_stream_health_service

            get_dom_stream_health_service().mark_disconnected(
                self.exchange,
                degraded=True,
                reason=f"closed:{code}" if code is not None else "closed",
                reconnect=True,
            )
        except Exception:
            pass
        logger.info(f"[ExStream] {self.exchange} closed (code={code})")

# ── Normalisation ──────────────────────────────────────────────────────────────

def _normalise(exchange: str, data: dict) -> Optional[dict]:
    """Convert exchange-specific payload to a unified event dict."""
    events = _normalise_many(exchange, data)
    return events[0] if events else None


# ── Manager ────────────────────────────────────────────────────────────────────

class ExchangeStreamManager:
    """
    Orchestrates connections to multiple exchanges.
    Publishes normalised events to Redis and calls any registered handlers.
    """

    def __init__(self) -> None:
        self._connections: Dict[str, _ExchangeConnection] = {}
        self._handlers:    List[Callable[[dict], None]]   = []
        self._running      = threading.Event()
        self._redis_ok     = False
        self._pub          = None          # lazy Redis publisher

    # ── Public API ────────────────────────────────────────────────────────────

    def start(self, exchanges: Optional[List[str]] = None) -> None:
        exchanges = exchanges or list(EXCHANGE_WS_URLS.keys())
        self._running.set()
        self._init_redis()

        for ex in exchanges:
            if ex not in EXCHANGE_WS_URLS:
                logger.warning(f"[ExStream] Unknown exchange '{ex}' — skipped")
                continue
            conn = _ExchangeConnection(ex, self._on_event, self._running)
            self._connections[ex] = conn
            conn.start()

        logger.info(f"[ExStream] Started feeds: {exchanges}")

    def stop(self) -> None:
        self._running.clear()
        logger.info("[ExStream] All feeds stopped")

    def add_handler(self, fn: Callable[[dict], None]) -> None:
        """Register a local callback for every event (in addition to Redis)."""
        self._handlers.append(fn)

    def active_exchanges(self) -> List[str]:
        return list(self._connections.keys())

    # ── Internal ──────────────────────────────────────────────────────────────

    def _init_redis(self) -> None:
        try:
            import redis
            from config.config import REDIS_URL
            from services.redis_pool import get_client as _get_redis_client

            self._pub = _get_redis_client()
            self._pub.ping()
            self._redis_ok = True
            logger.info("[ExStream] Redis publisher connected")
        except Exception as e:
            logger.warning(f"[ExStream] Redis unavailable ({e}) — events not published to Redis")

    def _on_event(self, event: dict) -> None:
        """Called for every normalised event from any exchange."""
        event_type = str(event.get("type", "") or "").upper()
        exchange = str(event.get("exchange", "") or "").lower()
        self._record_live_microstructure(event, event_type=event_type, exchange=exchange)

        if event_type in {"MARKET_DATA_UPDATE", "TRADE_UPDATE"}:
            _ping_health("trades")

        # Bybit carries liquidation subscriptions on the same live public socket
        # as ticker/order book updates, so socket activity is a practical
        # heartbeat even when no liquidation prints occur for a short period.
        if event_type == "LIQUIDATION_EVENT" or (
            exchange == "bybit" and event_type in {"MARKET_DATA_UPDATE", "ORDER_BOOK_UPDATE"}
        ):
            _ping_health("liquidations")

        # 1. Publish to Redis
        if self._pub:
            if not self._redis_ok:
                # FIX S15: Attempt reconnect when _redis_ok was cleared by a
                # publish error.  Previously _redis_ok was set to False on the
                # first publish failure and never recovered — the entire Redis
                # publisher was permanently silenced until the process restarted.
                try:
                    self._pub.ping()
                    self._redis_ok = True
                    logger.info("[ExStream] Redis publisher reconnected")
                except Exception:
                    pass  # still down — skip publish this cycle

            if self._redis_ok:
                try:
                    channel = event.get("type", "MARKET_DATA_UPDATE")
                    self._pub.publish(channel, json.dumps(event, default=str))
                except Exception as e:
                    logger.debug(f"[ExStream] Redis publish error: {e}")
                    self._redis_ok = False   # will retry on next event

        # 2. Call local handlers
        for fn in self._handlers:
            try:
                fn(event)
            except Exception as e:
                logger.debug(f"[ExStream] Handler error: {e}")

    @staticmethod
    def _record_live_microstructure(event: dict, *, event_type: str, exchange: str) -> None:
        if exchange != "binance":
            return
        try:
            from services.binance_market_bridge import binance_market_bridge

            symbol = str(event.get("asset") or "").strip().upper()
            if event_type == "ORDER_BOOK_UPDATE":
                binance_market_bridge.record_stream_depth_delta(
                    symbol,
                    list(event.get("bids") or []),
                    list(event.get("asks") or []),
                    timestamp=event.get("ts"),
                    first_update_id=event.get("first_update_id"),
                    final_update_id=event.get("final_update_id"),
                )
            elif event_type == "TRADE_UPDATE":
                binance_market_bridge.record_stream_trade(
                    symbol,
                    price=event.get("price"),
                    qty=event.get("qty"),
                    side=event.get("side"),
                    timestamp=event.get("ts"),
                )
            elif event_type == "MARKET_DATA_UPDATE":
                binance_market_bridge.record_stream_quote(
                    symbol,
                    price=event.get("price"),
                    timestamp=event.get("ts"),
                )
        except Exception as exc:
            logger.debug(f"[ExStream] live microstructure record error: {exc}")


# ── Module-level singleton ────────────────────────────────────────────────────
stream_manager = ExchangeStreamManager()       

