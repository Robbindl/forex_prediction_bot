"""
WebSocket Manager - Real-time market data
Sources:
  • Bybit       — Crypto (BTC, ETH, SOL, XRP, BNB …)
  • Finnhub     — Stocks (AAPL, MSFT, GOOGL …)
  • Twelve Data — Forex + Commodities (EUR/USD, XAU/USD, XAG/USD, WTI …)
"""

import websockets
import asyncio
import json
import threading
import time
from typing import Dict, List, Callable
from datetime import datetime
from logger import logger


class WebSocketManager:
    def __init__(self):
        self.connections: Dict[str, websockets.WebSocketClientProtocol] = {}
        self.callbacks: Dict[str, List[Callable]] = {}
        self.running = False
        self.loop = None
        self.thread = None
        self.loop_ready = False
        self._finnhub_disabled = False

        # Connection URLs
        self.finnhub_url    = "wss://ws.finnhub.io"
        self.finnhub_token  = "d6bc2ohr01qnr27kdcb0d6bc2ohr01q27kdcbg"
        self.bybit_url      = "wss://stream.bybit.com/v5/public/spot"
        self.twelvedata_url = "wss://ws.twelvedata.com/v1/quotes/price"
        self.twelvedata_key = "6c8e5137892642fe96cbfbf9d782c7d0"

        logger.info("📡 WebSocket Manager initialized (Bybit + Finnhub + Twelve Data)")

    def start(self):
        self.running = True
        self.thread = threading.Thread(target=self._run_loop, daemon=True)
        self.thread.start()
        while not self.loop_ready:
            time.sleep(0.1)
        logger.info("✅ WebSocket manager started")

    def _run_loop(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.loop_ready = True
        self.loop.run_forever()

    def _schedule(self, coro):
        while not self.loop_ready:
            time.sleep(0.1)
        asyncio.run_coroutine_threadsafe(coro, self.loop)

    # ───────────────────────── BYBIT ─────────────────────────

    def subscribe_bybit(self, symbols: List[str], callback: Callable):
        """Crypto — BTCUSDT, ETHUSDT, SOLUSDT …"""
        self._schedule(self._connect_bybit_with_reconnect(symbols, callback))
        logger.info(f"📡 Bybit: subscribing to {symbols}")

    async def _connect_bybit_with_reconnect(self, symbols: List[str], callback: Callable):
        """Exponential backoff: 5s → 10s → 20s → 40s → 60s cap."""
        from websocket_dashboard import set_connected
        backoff, max_backoff = 5, 60
        while self.running:
            t0 = asyncio.get_event_loop().time()
            try:
                await self._connect_bybit(symbols, callback)
                backoff = 5
            except Exception as e:
                if asyncio.get_event_loop().time() - t0 > 30:
                    backoff = 5
                set_connected('bybit', False)
                logger.error(f"❌ Bybit lost: {e} — retry in {backoff}s")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)

    async def _connect_bybit(self, symbols: List[str], callback: Callable):
        from websocket_dashboard import set_connected
        async with websockets.connect(self.bybit_url) as ws:
            self.connections['bybit'] = ws
            set_connected('bybit', True, len(symbols))
            logger.info("✅ Bybit connected")

            args = [f"publicTrade.{s.upper()}" for s in symbols]
            await ws.send(json.dumps({"op": "subscribe", "args": args}))

            async def heartbeat():
                while self.running:
                    await asyncio.sleep(20)
                    try:
                        await ws.send(json.dumps({"op": "ping"}))
                    except Exception:
                        break
            asyncio.create_task(heartbeat())

            async for message in ws:
                try:
                    data = json.loads(message)
                    if data.get('op') in ('pong', 'ping'):
                        continue
                    if 'topic' in data and 'publicTrade' in data['topic']:
                        symbol = data['topic'].split('.')[1]
                        for trade in data.get('data', []):
                            callback('bybit', symbol,
                                     float(trade['p']), float(trade['v']),
                                     trade['S'], datetime.now())
                except Exception as e:
                    logger.error(f"Bybit msg error: {e}")

    # ───────────────────────── FINNHUB ─────────────────────────

    def subscribe_finnhub(self, symbols: List[str], callback: Callable):
        """Stocks — AAPL, MSFT, GOOGL …  (requires paid Finnhub plan for WS)"""
        if self._finnhub_disabled:
            logger.warning("Finnhub WS disabled (401 — requires paid plan)")
            return
        self._schedule(self._connect_finnhub_with_reconnect(symbols, callback))
        logger.info(f"📡 Finnhub: subscribing to {symbols}")

    async def _connect_finnhub_with_reconnect(self, symbols: List[str], callback: Callable):
        from websocket_dashboard import set_connected
        backoff, max_backoff = 5, 60
        while self.running and not self._finnhub_disabled:
            t0 = asyncio.get_event_loop().time()
            try:
                await self._connect_finnhub(symbols, callback)
                if self._finnhub_disabled:
                    break
                backoff = 5
            except Exception as e:
                err = str(e)
                if '401' in err:
                    self._finnhub_disabled = True
                    set_connected('finnhub', False)
                    logger.warning("Finnhub WS: 401 — stopped permanently")
                    break
                if asyncio.get_event_loop().time() - t0 > 30:
                    backoff = 5
                set_connected('finnhub', False)
                logger.error(f"❌ Finnhub lost: {e} — retry in {backoff}s")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)

    async def _connect_finnhub(self, symbols: List[str], callback: Callable):
        from websocket_dashboard import set_connected
        url = f"{self.finnhub_url}?token={self.finnhub_token}"
        async with websockets.connect(url) as ws:
            self.connections['finnhub'] = ws
            set_connected('finnhub', True, len(symbols))
            logger.info("✅ Finnhub connected")

            for sym in symbols:
                await ws.send(json.dumps({'type': 'subscribe', 'symbol': sym}))

            async def heartbeat():
                while self.running:
                    await asyncio.sleep(25)
                    try:
                        await ws.send(json.dumps({'type': 'ping'}))
                    except Exception:
                        break
            asyncio.create_task(heartbeat())

            async for message in ws:
                try:
                    data = json.loads(message)
                    if data.get('type') == 'trade':
                        for t in data.get('data', []):
                            callback('finnhub', t['s'], float(t['p']),
                                     float(t.get('v', 0)), None,
                                     datetime.fromtimestamp(t['t'] / 1000))
                except Exception as e:
                    if '401' in str(e):
                        self._finnhub_disabled = True
                        set_connected('finnhub', False)
                        return

    # ───────────────────────── TWELVE DATA ─────────────────────────

    def subscribe_twelvedata(self, symbols: List[str], callback: Callable):
        """Forex & Commodities — EUR/USD, GBP/USD, XAU/USD, XAG/USD, WTI/USD …"""
        self._schedule(self._connect_twelvedata_with_reconnect(symbols, callback))
        logger.info(f"📡 Twelve Data: subscribing to {symbols}")

    async def _connect_twelvedata_with_reconnect(self, symbols: List[str], callback: Callable):
        """Exponential backoff same as Bybit."""
        from websocket_dashboard import set_connected
        backoff, max_backoff = 5, 60
        while self.running:
            t0 = asyncio.get_event_loop().time()
            try:
                await self._connect_twelvedata(symbols, callback)
                backoff = 5
            except Exception as e:
                if asyncio.get_event_loop().time() - t0 > 30:
                    backoff = 5
                set_connected('twelvedata', False)
                logger.error(f"❌ Twelve Data lost: {e} — retry in {backoff}s")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)

    async def _connect_twelvedata(self, symbols: List[str], callback: Callable):
        """
        Twelve Data WebSocket — real-time forex & commodity quotes.
        Protocol:
          • Connect: wss://ws.twelvedata.com/v1/quotes/price?apikey=KEY
          • Subscribe: {"action":"subscribe","params":{"symbols":"EUR/USD,XAU/USD"}}
          • Price msg: {"event":"price","symbol":"EUR/USD","price":1.0854,...}
          • Heartbeat: {"event":"heartbeat"} — reply with same to keep alive
        """
        from websocket_dashboard import set_connected
        url = f"{self.twelvedata_url}?apikey={self.twelvedata_key}"
        async with websockets.connect(url) as ws:
            self.connections['twelvedata'] = ws
            set_connected('twelvedata', True, len(symbols))
            logger.info("✅ Twelve Data connected")

            # Subscribe
            sym_str = ",".join(symbols)
            await ws.send(json.dumps({
                "action": "subscribe",
                "params": {"symbols": sym_str}
            }))
            logger.info(f"📡 Twelve Data: subscribed to {sym_str}")

            async for message in ws:
                try:
                    data = json.loads(message)
                    event = data.get('event', '')

                    # Keep-alive heartbeat
                    if event == 'heartbeat':
                        await ws.send(json.dumps({"event": "heartbeat"}))
                        continue

                    # Subscription confirmation
                    if event == 'subscribe-status':
                        ok  = data.get('success') or []
                        bad = data.get('fails')   or []
                        ok_syms  = [s['symbol'] for s in ok  if isinstance(s, dict) and 'symbol' in s]
                        bad_syms = [s['symbol'] for s in bad if isinstance(s, dict) and 'symbol' in s]
                        if ok_syms:
                            logger.info(f"Twelve Data live: {ok_syms}")
                        if bad_syms:
                            # Free tier limit hit — log clearly, don't spam warnings
                            logger.warning(
                                f"Twelve Data: {bad_syms} rejected — free tier limit reached. "                                f"Only {ok_syms} streaming. Upgrade at twelvedata.com for more symbols."
                            )
                            # Update dashboard to show only confirmed symbols
                            from websocket_dashboard import set_connected
                            set_connected('twelvedata', True, len(ok_syms))
                        continue

                    # Live price tick
                    if event == 'price':
                        symbol    = data.get('symbol', '')
                        price     = float(data.get('price', 0))
                        bid       = float(data.get('bid', price))
                        ask       = float(data.get('ask', price))
                        # Use mid-price; volume not available for forex
                        if price > 0:
                            callback('twelvedata', symbol, price, None, None, datetime.now())

                except Exception as e:
                    logger.error(f"Twelve Data msg error: {e}")

    # ───────────────────────── CONTROL ─────────────────────────

    def stop(self):
        self.running = False
        if self.loop:
            self.loop.call_soon_threadsafe(self.loop.stop)
        logger.info("📡 WebSocket manager stopped")