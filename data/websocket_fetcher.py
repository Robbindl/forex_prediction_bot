"""
WebSocket Real-time Data Fetcher - RATE LIMITED VERSION
"""

import json
import threading
import time
import random
from datetime import datetime
import websocket
from typing import Dict, Optional, Callable

class WebSocketFetcher:
    
    def __init__(self, on_price_callback: Optional[Callable] = None):
        self.on_price_callback = on_price_callback
        self.connections = {}
        self.running = False
        self.latest_prices = {}
        self.callbacks = []
        
        self.finnhub_token = "d6bc2ohr01qnr27kdcb0d6bc2ohr01qnr27kdcbg"
        self.binance_socket = "wss://stream.binance.com:9443/ws"
        
        # Rate limiting
        self.reconnect_delay = 30  # Start with 30 seconds
        self.max_reconnect_delay = 120  # Max 2 minutes
        self.last_request_time = 0
        
        print("📡 WebSocket Fetcher (RATE LIMITED MODE)")
    
    def add_price_callback(self, callback):
        self.callbacks.append(callback)
    
    def on_message(self, ws, message):
        try:
            data = json.loads(message)
            
            if 'data' in data and 's' in data['data'][0]:  # Finnhub
                for item in data['data']:
                    symbol = item['s']
                    price = item['p']
                    timestamp = datetime.fromtimestamp(item['t'] / 1000)
                    
                    self.latest_prices[symbol] = {
                        'price': price,
                        'timestamp': timestamp,
                        'source': 'finnhub'
                    }
                    
                    for callback in self.callbacks:
                        callback(symbol, price, timestamp)
            
            elif 'stream' in data:  # Binance
                stream = data['stream']
                if 'trade' in stream:
                    symbol = stream.split('@')[0].upper()
                    price = float(data['data']['p'])
                    timestamp = datetime.fromtimestamp(data['data']['T'] / 1000)
                    
                    self.latest_prices[symbol] = {
                        'price': price,
                        'timestamp': timestamp,
                        'source': 'binance'
                    }
                    
                    for callback in self.callbacks:
                        callback(f"{symbol}USDT", price, timestamp)
                        
        except Exception as e:
            print(f"WebSocket message error: {e}")
    
    def on_error(self, ws, error):
        print(f"WebSocket error: {error}")
    
    def on_close(self, ws, close_status_code, close_msg):
        print(f"WebSocket closed: {close_status_code} - {close_msg}")
        if self.running:
            jitter = random.uniform(0, 10)
            delay = self.reconnect_delay + jitter
            print(f"⏳ Reconnecting in {delay:.1f} seconds...")
            time.sleep(delay)
            self.reconnect_delay = min(self.reconnect_delay * 1.5, self.max_reconnect_delay)
            self.connect_all()
    
    def on_open(self, ws):
        print(f"WebSocket connected: {ws.url}")
        self.reconnect_delay = 30  # Reset on success
    
    def connect_finnhub(self, symbols):
        if not self.finnhub_token:
            return
        
        ws_url = f"wss://ws.finnhub.io?token={self.finnhub_token}"
        
        def run():
            try:
                ws = websocket.WebSocketApp(
                    ws_url,
                    on_open=self.on_open,
                    on_message=self.on_message,
                    on_error=self.on_error,
                    on_close=self.on_close
                )
                self.connections['finnhub'] = ws
                ws.run_forever()
            except Exception as e:
                print(f"Finnhub error: {e}")
        
        thread = threading.Thread(target=run, daemon=True)
        thread.start()
        
        time.sleep(5)  # Wait for connection
        
        # Subscribe with delays
        for symbol in symbols:
            try:
                time.sleep(3)  # 3 second delay between each
                subscribe_msg = json.dumps({'type': 'subscribe', 'symbol': symbol})
                if 'finnhub' in self.connections:
                    self.connections['finnhub'].send(subscribe_msg)
                    print(f"  📡 Subscribed to {symbol}")
            except Exception as e:
                print(f"  ⚠️ Could not subscribe to {symbol}")
    
    def connect_binance(self, symbols):
        def run():
            try:
                streams = [f"{s.lower()}@trade" for s in symbols]
                stream_url = f"{self.binance_socket}/{'/'.join(streams)}"
                
                ws = websocket.WebSocketApp(
                    stream_url,
                    on_open=self.on_open,
                    on_message=self.on_message,
                    on_error=self.on_error,
                    on_close=self.on_close
                )
                self.connections['binance'] = ws
                ws.run_forever()
            except Exception as e:
                print(f"Binance error: {e}")
        
        thread = threading.Thread(target=run, daemon=True)
        thread.start()
        print(f"  📡 Binance: {', '.join(symbols)}")
    
    def connect_all(self):
        """Connect with MINIMAL symbols to avoid rate limits"""
        self.running = True
        self.reconnect_delay = 30
        
        print("\n📡 Connecting with MINIMAL symbols (rate limit protection)...")
        
        # Just the essentials
        self.connect_binance(['btcusdt', 'ethusdt'])
        self.connect_finnhub(['AAPL', 'OANDA:EUR_USD', 'OANDA:XAU_USD'])
    
    def stop(self):
        self.running = False
        for name, ws in self.connections.items():
            try:
                ws.close()
            except:
                pass
        print("📡 WebSocket stopped")