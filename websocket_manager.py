"""
WebSocket Manager - Real-time market data
WORKING VERSION - Based on Bybit official API
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
    """
    Manages multiple WebSocket connections for real-time data
    Bybit public trade channel (works worldwide)
    """

    def __init__(self):
        self.connections: Dict[str, websockets.WebSocketClientProtocol] = {}
        self.callbacks: Dict[str, List[Callable]] = {}
        self.running = False
        self.loop = None
        self.thread = None
        self.loop_ready = False
        self._finnhub_disabled = False  # FIX 1: permanent stop flag

        # Connection URLs
        self.finnhub_url = "wss://ws.finnhub.io"
        self.finnhub_token = "d6bc2ohr01qnr27kdcb0d6bc2ohr01q27kdcbg"
        self.bybit_url = "wss://stream.bybit.com/v5/public/spot"

        logger.info("📡 WebSocket Manager initialized")

    def start(self):
        """Start the WebSocket manager in background thread"""
        self.running = True
        self.thread = threading.Thread(target=self._run_loop, daemon=True)
        self.thread.start()

        while not self.loop_ready:
            time.sleep(0.1)
        logger.info("✅ WebSocket manager started")

    def _run_loop(self):
        """Run asyncio event loop in background thread"""
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.loop_ready = True
        self.loop.run_forever()

    # ───────────────────────── BYBIT ─────────────────────────

    def subscribe_bybit(self, symbols: List[str], callback: Callable):
        """Subscribe to Bybit WebSocket for crypto (works worldwide)"""
        while not self.loop_ready:
            time.sleep(0.1)

        asyncio.run_coroutine_threadsafe(
            self._connect_bybit_with_reconnect(symbols, callback),
            self.loop
        )
        logger.info(f"📡 Subscribed to Bybit: {symbols}")

    async def _connect_bybit_with_reconnect(self, symbols: List[str], callback: Callable):
        """Auto-reconnect wrapper for Bybit"""
        while self.running:
            try:
                await self._connect_bybit(symbols, callback)
            except Exception as e:
                logger.error(f"❌ Bybit connection lost: {e} — reconnecting in 5s")
                await asyncio.sleep(5)

    async def _connect_bybit(self, symbols: List[str], callback: Callable):
        """Main Bybit connection loop"""
        async with websockets.connect(self.bybit_url) as ws:
            self.connections['bybit'] = ws
            logger.info("✅ Bybit WebSocket connected")

            bybit_symbols = [f"publicTrade.{s.upper()}" for s in symbols]
            await ws.send(json.dumps({"op": "subscribe", "args": bybit_symbols}))
            logger.info(f"📡 Subscribed to: {bybit_symbols}")

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

                    if data.get('op') == 'pong':
                        continue

                    if data.get('op') == 'ping':
                        await ws.send(json.dumps({"op": "pong"}))
                        continue

                    if 'topic' in data and 'publicTrade' in data['topic']:
                        symbol = data['topic'].split('.')[1]
                        for trade in data.get('data', []):
                            price = float(trade['p'])
                            volume = float(trade['v'])
                            side = trade['S']
                            timestamp = datetime.now()

                            logger.debug(f"💰 {symbol}: ${price:,.2f} | {volume:.4f} | {side}")
                            callback('bybit', symbol, price, volume, side, timestamp)

                except Exception as e:
                    logger.error(f"❌ Bybit message error: {e}")

    # ───────────────────────── FINNHUB ─────────────────────────

    def subscribe_finnhub(self, symbols: List[str], callback: Callable):
        """Subscribe to Finnhub WebSocket — disabled on 401 (free tier has no WS)"""
        if self._finnhub_disabled:
            logger.warning("Finnhub WebSocket permanently disabled (HTTP 401 — requires paid plan)")
            return

        while not self.loop_ready:
            time.sleep(0.1)

        asyncio.run_coroutine_threadsafe(
            self._connect_finnhub_with_reconnect(symbols, callback),
            self.loop
        )
        logger.info(f"📡 Subscribed to Finnhub: {symbols}")

    async def _connect_finnhub_with_reconnect(self, symbols: List[str], callback: Callable):
        """Auto-reconnect wrapper for Finnhub — stops permanently on 401"""
        while self.running and not self._finnhub_disabled:
            try:
                await self._connect_finnhub(symbols, callback)
                # FIX 1: if _connect_finnhub returned normally due to 401, stop here
                if self._finnhub_disabled:
                    break
            except Exception as e:
                err = str(e)
                # FIX 1: catch 401 at the connection level (before message loop)
                if 'HTTP 401' in err or '401' in err:
                    self._finnhub_disabled = True
                    logger.warning("Finnhub WebSocket: HTTP 401 — free tier has no WS access. Stopped permanently.")
                    break
                logger.error(f"❌ Finnhub connection lost: {e} — reconnecting in 5s")
                await asyncio.sleep(5)

    async def _connect_finnhub(self, symbols: List[str], callback: Callable):
        """Main Finnhub connection loop"""
        url = f"{self.finnhub_url}?token={self.finnhub_token}"

        async with websockets.connect(url) as ws:
            self.connections['finnhub'] = ws
            logger.info("✅ Finnhub WebSocket connected")

            for symbol in symbols:
                await ws.send(json.dumps({'type': 'subscribe', 'symbol': symbol}))

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
                        for trade in data.get('data', []):
                            symbol = trade['s']
                            price = float(trade['p'])
                            volume = float(trade.get('v', 0))
                            timestamp = datetime.fromtimestamp(trade['t'] / 1000)
                            callback('finnhub', symbol, price, volume, None, timestamp)

                except Exception as e:
                    err = str(e)
                    if 'HTTP 401' in err or '401' in err:
                        self._finnhub_disabled = True
                        logger.warning("Finnhub WebSocket: HTTP 401 — stopping permanently.")
                        return  # exit cleanly so reconnect wrapper sees _finnhub_disabled

    # ───────────────────────── CONTROL ─────────────────────────

    def stop(self):
        """Stop all connections"""
        self.running = False
        if self.loop:
            self.loop.call_soon_threadsafe(self.loop.stop)
        logger.info("📡 WebSocket manager stopped")


# ───────────────────────── STANDALONE TEST ─────────────────────────
if __name__ == "__main__":
    from websocket_dashboard import add_transaction

    def price_callback(source, symbol, price, volume, side, timestamp):
        """Standalone test callback — feeds dashboard shared store"""
        add_transaction(source, symbol, price, volume, side)
        print(f"💰 {source.upper()} | {symbol} | ${price:,.2f} | {volume:.4f} | {side}")

    print("=" * 60)
    print("🚀 TESTING WEBSOCKET MANAGER")
    print("=" * 60)

    ws = WebSocketManager()
    ws.start()
    ws.subscribe_bybit(['BTCUSDT', 'ETHUSDT', 'SOLUSDT'], price_callback)

    print("\n✅ Listening — trades will appear below")
    print("Press Ctrl+C to stop\n")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        ws.stop()
        print("\n🛑 Stopped")