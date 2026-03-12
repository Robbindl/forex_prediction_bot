import os
"""
WebSocket Real-time Data Fetcher - STABLE VERSION with auto-reconnect
"""

import json
import threading
import time
import random
from datetime import datetime, timedelta
import websocket
from typing import Dict, Optional, Callable, List
from logger import logger

class WebSocketFetcher:
    """
    Stable WebSocket fetcher with auto-reconnect and rate limiting
    """
    
    def __init__(self, on_price_callback: Optional[Callable] = None):
        self.on_price_callback = on_price_callback
        self.connections: Dict[str, websocket.WebSocketApp] = {}
        self.running = False
        self.latest_prices: Dict[str, Dict] = {}
        self.callbacks: List[Callable] = []
        self.reconnect_count: Dict[str, int] = {}
        self.last_message_time: Dict[str, datetime] = {}
        
        # API Keys
        self.finnhub_token = os.getenv("FINNHUB_KEY", "")
        self.binance_socket = "wss://stream.binance.com:9443/ws"
        
        # Connection settings
        self.reconnect_delay = 5  # Start with 5 seconds
        self.max_reconnect_delay = 300  # Max 5 minutes
        self.ping_interval = 30  # Send ping every 30 seconds
        self.connection_timeout = 10  # Connection timeout
        
        # Rate limiting
        self.message_counts: Dict[str, List[datetime]] = {}
        self.max_messages_per_minute = 60  # Conservative limit
        
        logger.info("📡 WebSocket Fetcher initialized (stable version)")
    
    def add_price_callback(self, callback: Callable):
        """Add callback for price updates"""
        self.callbacks.append(callback)
        logger.debug(f"Added callback, total: {len(self.callbacks)}")
    
    def _check_rate_limit(self, source: str) -> bool:
        """Check if we're within rate limits"""
        now = datetime.now()
        if source not in self.message_counts:
            self.message_counts[source] = []
        
        # Remove messages older than 1 minute
        self.message_counts[source] = [
            t for t in self.message_counts[source] 
            if now - t < timedelta(minutes=1)
        ]
        
        if len(self.message_counts[source]) >= self.max_messages_per_minute:
            logger.warning(f"Rate limit reached for {source}")
            return False
        
        self.message_counts[source].append(now)
        return True
    
    def _get_reconnect_delay(self, source: str) -> float:
        """Get exponential backoff delay"""
        count = self.reconnect_count.get(source, 0)
        delay = min(self.reconnect_delay * (2 ** count), self.max_reconnect_delay)
        # Add jitter
        delay += random.uniform(0, delay * 0.1)
        return delay
    
    def on_message(self, ws: websocket.WebSocketApp, message: str):
        """Handle incoming messages"""
        try:
            # Update last message time
            source = getattr(ws, 'source', 'unknown')
            self.last_message_time[source] = datetime.now()
            
            data = json.loads(message)
            
            # Finnhub format
            if 'data' in data and isinstance(data['data'], list) and len(data['data']) > 0:
                for item in data['data']:
                    if 's' in item and 'p' in item:
                        symbol = item['s']
                        price = float(item['p'])
                        timestamp = datetime.fromtimestamp(item['t'] / 1000)
                        
                        if self._check_rate_limit('finnhub'):
                            self.latest_prices[symbol] = {
                                'price': price,
                                'timestamp': timestamp,
                                'source': 'finnhub'
                            }
                            
                            for callback in self.callbacks:
                                try:
                                    callback(symbol, price, timestamp)
                                except Exception as e:
                                    logger.error(f"Callback error: {e}")
            
            # Binance format
            elif 'stream' in data:
                stream = data['stream']
                if 'trade' in stream:
                    symbol = stream.split('@')[0].upper()
                    price = float(data['data']['p'])
                    timestamp = datetime.fromtimestamp(data['data']['T'] / 1000)
                    
                    if self._check_rate_limit('binance'):
                        self.latest_prices[f"{symbol}USDT"] = {
                            'price': price,
                            'timestamp': timestamp,
                            'source': 'binance'
                        }
                        
                        for callback in self.callbacks:
                            try:
                                callback(f"{symbol}USDT", price, timestamp)
                            except Exception as e:
                                logger.error(f"Callback error: {e}")
            
            # Ping/pong response
            elif 'type' in data and data['type'] == 'pong':
                logger.debug("Received pong")
                
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error: {e}")
        except Exception as e:
            logger.error(f"Message handler error: {e}")
    
    def on_error(self, ws: websocket.WebSocketApp, error):
        """Handle errors"""
        source = getattr(ws, 'source', 'unknown')
        logger.error(f"WebSocket error for {source}: {error}")
        
        # Don't immediately reconnect - let on_close handle it
    
    def on_close(self, ws: websocket.WebSocketApp, close_status_code=None, close_msg=None):
        """Handle connection close"""
        source = getattr(ws, 'source', 'unknown')
        logger.warning(f"WebSocket closed for {source}: {close_status_code} - {close_msg}")
        
        # Update reconnect count
        self.reconnect_count[source] = self.reconnect_count.get(source, 0) + 1
        
        if self.running:
            delay = self._get_reconnect_delay(source)
            logger.info(f"Reconnecting {source} in {delay:.1f}s (attempt {self.reconnect_count[source]})")
            
            # Schedule reconnection
            def reconnect():
                time.sleep(delay)
                if self.running:
                    if source == 'finnhub':
                        self._connect_finnhub()
                    elif source == 'binance':
                        self._connect_binance(['btcusdt', 'ethusdt'])
            
            thread = threading.Thread(target=reconnect, daemon=True)
            thread.start()
    
    def on_open(self, ws: websocket.WebSocketApp):
        """Handle connection open"""
        source = getattr(ws, 'source', 'unknown')
        logger.info(f"WebSocket connected: {source}")
        
        # Reset reconnect count on success
        self.reconnect_count[source] = 0
        self.last_message_time[source] = datetime.now()
        
        # Subscribe to symbols if needed
        if source == 'finnhub' and hasattr(ws, 'symbols'):
            for symbol in ws.symbols:
                try:
                    subscribe_msg = json.dumps({'type': 'subscribe', 'symbol': symbol})
                    ws.send(subscribe_msg)
                    logger.debug(f"Subscribed to {symbol}")
                    time.sleep(0.5)  # Small delay between subscriptions
                except Exception as e:
                    logger.error(f"Subscription error for {symbol}: {e}")
    
    def _connect_finnhub(self, symbols: List[str] = None):
        """Connect to Finnhub WebSocket"""
        if not self.finnhub_token:
            logger.error("No Finnhub token")
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
                ws.source = 'finnhub'
                ws.symbols = symbols or []
                self.connections['finnhub'] = ws
                
                # Run with ping interval to keep connection alive
                ws.run_forever(ping_interval=self.ping_interval, ping_timeout=10)
                
            except Exception as e:
                logger.error(f"Finnhub connection error: {e}")
                if self.running:
                    time.sleep(self._get_reconnect_delay('finnhub'))
                    self._connect_finnhub(symbols)
        
        thread = threading.Thread(target=run, daemon=True)
        thread.start()
        
        # Wait for connection to establish
        time.sleep(2)
    
    def _connect_binance(self, symbols: List[str] = None):
        """Connect to Binance WebSocket"""
        def run():
            try:
                streams = [f"{s.lower()}@trade" for s in (symbols or [])]
                stream_url = f"{self.binance_socket}/{'/'.join(streams)}"
                
                ws = websocket.WebSocketApp(
                    stream_url,
                    on_open=self.on_open,
                    on_message=self.on_message,
                    on_error=self.on_error,
                    on_close=self.on_close
                )
                ws.source = 'binance'
                self.connections['binance'] = ws
                
                ws.run_forever(ping_interval=self.ping_interval, ping_timeout=10)
                
            except Exception as e:
                logger.error(f"Binance connection error: {e}")
                if self.running:
                    time.sleep(self._get_reconnect_delay('binance'))
                    self._connect_binance(symbols)
        
        thread = threading.Thread(target=run, daemon=True)
        thread.start()
        
        time.sleep(2)
    
    def connect_all(self):
        """Connect to all WebSocket sources with minimal symbols"""
        self.running = True
        
        logger.info("📡 Connecting WebSocket sources...")
        
        # Connect with minimal symbols to avoid rate limits
        self._connect_binance(['btcusdt', 'ethusdt'])
        self._connect_finnhub(['AAPL', 'OANDA:EUR_USD', 'OANDA:XAU_USD'])
        
        # Start health check thread
        self._start_health_check()
    
    def _start_health_check(self):
        """Start background health check thread"""
        def health_check():
            while self.running:
                time.sleep(60)  # Check every minute
                
                for source, last_time in self.last_message_time.items():
                    if last_time:
                        elapsed = (datetime.now() - last_time).seconds
                        if elapsed > 120:  # No messages for 2 minutes
                            logger.warning(f"No messages from {source} for {elapsed}s, reconnecting...")
                            if source in self.connections:
                                try:
                                    self.connections[source].close()
                                except:
                                    pass
                                del self.connections[source]
                            
                            # Reconnect
                            if source == 'finnhub':
                                self._connect_finnhub(['AAPL', 'OANDA:EUR_USD', 'OANDA:XAU_USD'])
                            elif source == 'binance':
                                self._connect_binance(['btcusdt', 'ethusdt'])
        
        thread = threading.Thread(target=health_check, daemon=True)
        thread.start()
        logger.info("Health check started")
    
    def subscribe(self, source: str, symbol: str):
        """Subscribe to additional symbol"""
        if source in self.connections:
            try:
                subscribe_msg = json.dumps({'type': 'subscribe', 'symbol': symbol})
                self.connections[source].send(subscribe_msg)
                logger.info(f"Subscribed to {symbol} on {source}")
            except Exception as e:
                logger.error(f"Subscription error: {e}")
    
    def get_latest_price(self, symbol: str) -> Optional[float]:
        """Get latest price for symbol"""
        if symbol in self.latest_prices:
            return self.latest_prices[symbol]['price']
        return None
    
    def stop(self):
        """Stop all connections"""
        logger.info("Stopping WebSocket connections...")
        self.running = False
        
        for name, ws in self.connections.items():
            try:
                ws.close()
                logger.info(f"Closed {name} connection")
            except Exception as e:
                logger.error(f"Error closing {name}: {e}")
        
        self.connections.clear()
        logger.info("WebSocket stopped")