from __future__ import annotations

import json
import socket
import ssl
import threading
import time
from typing import Callable, Dict, List, Optional

from utils.logger import get_logger

logger = get_logger()

# ── Exchange WebSocket endpoints ───────────────────────────────────────────────
EXCHANGE_WS_URLS: Dict[str, str] = {
    "binance": "wss://stream.binance.com:9443/stream",
    "bybit":   "wss://stream.bybit.com/v5/public/spot",
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
        ],
        "id": 1,
    },
    "bybit": {
        "op": "subscribe",
        "args": [
            "tickers.BTCUSDT", "tickers.ETHUSDT",
            "orderbook.50.BTCUSDT",
            "liquidation.BTCUSDT", "liquidation.ETHUSDT",
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

# How many ms to wait before reconnecting after a drop
_RECONNECT_DELAY_SECS = 5


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
                logger.warning(
                    f"[ExStream] {self.exchange} error: {exc} "
                    f"— reconnecting in {self._delay}s"
                )
            if self._running.is_set():
                time.sleep(self._delay)
                self._delay = min(self._delay * 2, 60)

    def _connect_and_read(self) -> None:
        import websocket  # pip install websocket-client

        url = EXCHANGE_WS_URLS[self.exchange]
        sub = SUBSCRIPTIONS[self.exchange]

        def on_open(ws):
            logger.info(f"[ExStream] {self.exchange} connected")
            ws.send(json.dumps(sub))

        def on_message(ws, raw):
            try:
                data  = json.loads(raw)
                event = _normalise(self.exchange, data)
                if event:
                    self._on_event(event)
            except Exception as e:
                logger.debug(f"[ExStream] {self.exchange} parse: {e}")

        def on_error(ws, err):
            if "ping/pong timed out" in str(err):
                logger.debug(f"[ExStream] {self.exchange} WS ping timeout — reconnecting")
            else:
                logger.warning(f"[ExStream] {self.exchange} WS error: {err}")

        def on_close(ws, code, msg):
            logger.info(f"[ExStream] {self.exchange} closed (code={code})")

        ws = websocket.WebSocketApp(
            url,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )
        ws.run_forever(ping_interval=45, ping_timeout=30)


# ── Normalisation ──────────────────────────────────────────────────────────────

def _normalise(exchange: str, data: dict) -> Optional[dict]:
    """Convert exchange-specific payload to a unified event dict."""
    try:
        if exchange == "binance":
            # Binance wraps in {"stream":"...","data":{...}} for combined streams
            inner = data.get("data", data)
            ev    = inner.get("e", "")
            if ev == "24hrTicker":
                return {
                    "type":     "MARKET_DATA_UPDATE",
                    "exchange": exchange,
                    "asset":    inner["s"],
                    "price":    float(inner["c"]),
                    "volume":   float(inner["v"]),
                    "change":   float(inner["P"]),
                    "ts":       inner["E"],
                }
            if ev == "depthUpdate" or (not ev and "lastUpdateId" in inner):
                return {
                    "type":     "ORDER_BOOK_UPDATE",
                    "exchange": exchange,
                    "asset":    inner["s"],
                    "bids":     inner.get("b", []),
                    "asks":     inner.get("a", []),
                    "ts":       inner["E"],
                }
            if ev == "aggTrade":
                return {
                    "type":     "TRADE_UPDATE",
                    "exchange": exchange,
                    "asset":    inner["s"],
                    "price":    float(inner["p"]),
                    "qty":      float(inner["q"]),
                    "side":     "SELL" if inner.get("m") else "BUY",
                    "ts":       inner["T"],
                }

        elif exchange == "bybit":
            topic = data.get("topic", "")
            d     = data.get("data", {})

            if topic.startswith("tickers."):
                return {
                    "type":     "MARKET_DATA_UPDATE",
                    "exchange": exchange,
                    "asset":    d.get("symbol", ""),
                    "price":    float(d.get("lastPrice", 0) or 0),
                    "volume":   float(d.get("volume24h", 0) or 0),
                    "change":   float(d.get("price24hPcnt", 0) or 0),
                    "ts":       int(time.time() * 1000),
                }
            if topic.startswith("orderbook."):
                return {
                    "type":     "ORDER_BOOK_UPDATE",
                    "exchange": exchange,
                    "asset":    d.get("s", ""),
                    "bids":     d.get("b", []),
                    "asks":     d.get("a", []),
                    "ts":       int(time.time() * 1000),
                }
            if topic.startswith("liquidation."):
                return {
                    "type":      "LIQUIDATION_EVENT",
                    "exchange":  exchange,
                    "asset":     d.get("symbol", ""),
                    "side":      d.get("side", ""),
                    "size":      float(d.get("size", 0) or 0),
                    "price":     float(d.get("price", 0) or 0),
                    "ts":        int(time.time() * 1000),
                }

        elif exchange == "okx":
            arg  = data.get("arg", {})
            rows = data.get("data", [{}])
            ch   = arg.get("channel", "")
            if ch == "tickers" and rows:
                r = rows[0]
                return {
                    "type":     "MARKET_DATA_UPDATE",
                    "exchange": exchange,
                    "asset":    r.get("instId", ""),
                    "price":    float(r.get("last", 0) or 0),
                    "volume":   float(r.get("vol24h", 0) or 0),
                    "change":   float(r.get("sodUtc8", 0) or 0),
                    "ts":       int(time.time() * 1000),
                }
            if ch == "books5" and rows:
                r = rows[0]
                return {
                    "type":     "ORDER_BOOK_UPDATE",
                    "exchange": exchange,
                    "asset":    arg.get("instId", ""),
                    "bids":     r.get("bids", []),
                    "asks":     r.get("asks", []),
                    "ts":       int(time.time() * 1000),
                }
    except Exception as exc:
        logger.debug(f"[ExStream] Normalise error ({exchange}): {exc}")
    return None


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


# ── Module-level singleton ────────────────────────────────────────────────────
stream_manager = ExchangeStreamManager()       