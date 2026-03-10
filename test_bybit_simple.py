"""
WebSocket Manager - Real-time market data
FULL VERSION: Auto-reconnect, all symbols, message queue
"""

import websockets
import asyncio
import json
import threading
import time
from typing import Dict, List, Optional, Callable
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

class WebSocketManager:
    """
    Manages multiple WebSocket connections with auto-reconnect and message queue
    """
    
    def __init__(self):
        self.connections: Dict[str, websockets.WebSocketClientProtocol] = {}
        self.callbacks: Dict[str, List[Callable]] = {}
        self.running = False
        self.loop = None
        self.thread = None
        self.loop_ready = False
        self.message_queue = asyncio.Queue()
        self.reconnect_delay = 5
        
        # Connection URLs
        self.finnhub_token = os.getenv('FINNHUB_KEY', 'd6bc2ohr01qnr27kdcb0d6bc2ohr01q27kdcbg')
        self.finnhub_url = f"wss://ws.finnhub.io?token={self.finnhub_token}"
        self.bybit_url = "wss://stream.bybit.com/v5/public/spot"
        
        print("📡 WebSocket Manager initialized")
    
    def start(self):
        """Start the WebSocket manager"""
        self.running = True
        self.thread = threading.Thread(target=self._run_loop, daemon=True)
        self.thread.start()
        
        while not self.loop_ready:
            time.sleep(0.1)
        print("✅ WebSocket manager started")
    
    def _run_loop(self):
        """Run asyncio event loop"""
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.loop_ready = True
        self.loop.create_task(self._process_queue())
        self.loop.run_forever()
    
    async def _process_queue(self):
        """Process messages from queue"""
        while self.running:
            try:
                source, symbol, price, timestamp = await self.message_queue.get()
                for callback in self.callbacks.get(source, []):
                    try:
                        callback(source, symbol, price, timestamp)
                    except Exception as e:
                        print(f"❌ Callback error: {e}")
            except Exception as e:
                print(f"❌ Queue error: {e}")
                await asyncio.sleep(0.1)
    
    def subscribe_bybit(self, symbols: List[str], callback: Callable):
        """Subscribe to Bybit with auto-reconnect"""
        while not self.loop_ready:
            time.sleep(0.1)
        
        if 'bybit' not in self.callbacks:
            self.callbacks['bybit'] = []
        self.callbacks['bybit'].append(callback)
        
        asyncio.run_coroutine_threadsafe(
            self._connect_bybit_with_reconnect(symbols),
            self.loop
        )
        print(f"📡 Subscribed to Bybit: {symbols}")
    
    async def _connect_bybit_with_reconnect(self, symbols: List[str]):
        """Connect to Bybit with auto-reconnect"""
        while self.running:
            try:
                await self._connect_bybit(symbols)
            except Exception as e:
                print(f"❌ Bybit connection lost: {e}")
                print(f"🔄 Reconnecting in {self.reconnect_delay}s...")
                await asyncio.sleep(self.reconnect_delay)
    
    async def _connect_bybit(self, symbols: List[str]):
        """Main Bybit connection"""
        async with websockets.connect(self.bybit_url) as ws:
            self.connections['bybit'] = ws
            print("✅ Bybit WebSocket connected")
            
            # Subscribe with correct format
            bybit_symbols = [f"publicTrade.{s.upper()}" for s in symbols]
            await ws.send(json.dumps({
                "op": "subscribe",
                "args": bybit_symbols
            }))
            print(f"📡 Subscribed to: {bybit_symbols}")
            
            # Heartbeat task
            async def heartbeat():
                while self.running:
                    await asyncio.sleep(20)
                    try:
                        await ws.send(json.dumps({"op": "ping"}))
                    except:
                        break
            
            asyncio.create_task(heartbeat())
            
            # Message loop
            async for message in ws:
                try:
                    data = json.loads(message)
                    
                    # Handle pong
                    if data.get('op') == 'pong':
                        continue
                    
                    # Handle server ping
                    if data.get('op') == 'ping':
                        await ws.send(json.dumps({"op": "pong"}))
                        continue
                    
                    # Handle trade data
                    if 'topic' in data and 'publicTrade' in data['topic']:
                        symbol = data['topic'].split('.')[1]
                        if 'data' in data and isinstance(data['data'], list):
                            for trade in data['data']:
                                price = float(trade['p'])
                                timestamp = datetime.now()
                                await self.message_queue.put(('bybit', symbol, price, timestamp))
                                
                except Exception as e:
                    print(f"❌ Message error: {e}")
    
    def subscribe_finnhub(self, symbols: List[str], callback: Callable):
        """Subscribe to Finnhub with auto-reconnect"""
        while not self.loop_ready:
            time.sleep(0.1)
        
        if 'finnhub' not in self.callbacks:
            self.callbacks['finnhub'] = []
        self.callbacks['finnhub'].append(callback)
        
        asyncio.run_coroutine_threadsafe(
            self._connect_finnhub_with_reconnect(symbols),
            self.loop
        )
        print(f"📡 Subscribed to Finnhub: {symbols}")
    
    async def _connect_finnhub_with_reconnect(self, symbols: List[str]):
        """Connect to Finnhub with auto-reconnect"""
        while self.running:
            try:
                await self._connect_finnhub(symbols)
            except Exception as e:
                print(f"❌ Finnhub connection lost: {e}")
                print(f"🔄 Reconnecting in {self.reconnect_delay}s...")
                await asyncio.sleep(self.reconnect_delay)
    
    async def _connect_finnhub(self, symbols: List[str]):
        """Main Finnhub connection"""
        async with websockets.connect(self.finnhub_url) as ws:
            self.connections['finnhub'] = ws
            print("✅ Finnhub WebSocket connected")
            
            # Subscribe
            for symbol in symbols:
                await ws.send(json.dumps({'type': 'subscribe', 'symbol': symbol}))
            
            # Heartbeat
            async def heartbeat():
                while self.running:
                    await asyncio.sleep(25)
                    try:
                        await ws.send(json.dumps({'type': 'ping'}))
                    except:
                        break
            
            asyncio.create_task(heartbeat())
            
            # Message loop
            async for message in ws:
                try:
                    data = json.loads(message)
                    
                    if data.get('type') == 'trade':
                        for trade in data.get('data', []):
                            symbol = trade['s']
                            price = float(trade['p'])
                            timestamp = datetime.fromtimestamp(trade['t']/1000)
                            await self.message_queue.put(('finnhub', symbol, price, timestamp))
                            
                except Exception as e:
                    print(f"❌ Finnhub message error: {e}")
    
    def stop(self):
        """Stop all connections"""
        self.running = False
        if self.loop:
            self.loop.call_soon_threadsafe(self.loop.stop)
        print("📡 WebSocket manager stopped")