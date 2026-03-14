"""
WebSocket Handlers - Process real-time data for your trading system
FULL VERSION: Complete symbol mapping, proper calculations
"""

from typing import Dict, Any
from datetime import datetime
import numpy as np
from utils.logger import logger
from websocket_dashboard import add_transaction  # FIXED: was dashboard_feed

class WebSocketHandlers:
    """
    Handles incoming WebSocket data with full symbol support
    """
    
    def __init__(self, trading_system):
        self.bot = trading_system
        self.latest_prices: Dict[str, Dict] = {}
        self.price_history: Dict[str, list] = {}
        self.last_signal_time: Dict[str, datetime] = {}
        self.signal_cooldown = 60  # seconds between signals for same asset
        
        # Complete symbol mapping
        self.symbol_map = {
            # Finnhub commodities
            'OANDA:XAU_USD': 'XAU/USD',
            'OANDA:XAG_USD': 'XAG/USD',
            'OANDA:WTICO_USD': 'WTI/USD',
            'OANDA:NATGAS_USD': 'NG/USD',

            # Bybit crypto
            'BTCUSDT': 'BTC-USD',
            'ETHUSDT': 'ETH-USD',
            'BNBUSDT': 'BNB-USD',
            'SOLUSDT': 'SOL-USD',
            'XRPUSDT': 'XRP-USD',

            # Finnhub forex
            'OANDA:EUR_USD': 'EUR/USD',
            'OANDA:GBP_USD': 'GBP/USD',
            'OANDA:USD_JPY': 'USD/JPY',
            'OANDA:AUD_USD': 'AUD/USD',
            'OANDA:USD_CAD': 'USD/CAD',
            'OANDA:NZD_USD': 'NZD/USD',
            'OANDA:USD_CHF': 'USD/CHF',

            'ADAUSDT': 'ADA-USD',
            'DOGEUSDT': 'DOGE-USD',
            'DOTUSDT': 'DOT-USD',
            'LTCUSDT': 'LTC-USD',
            'AVAXUSDT': 'AVAX-USD',
            'LINKUSDT': 'LINK-USD',
            
            # Finnhub stocks
            'AAPL': 'AAPL',
            'MSFT': 'MSFT',
            'GOOGL': 'GOOGL',
            'AMZN': 'AMZN',
            'TSLA': 'TSLA',
            'NVDA': 'NVDA',
            'META': 'META',
            'JPM': 'JPM',
            'V': 'V',
            'WMT': 'WMT',
            
            
        }
        
        logger.info(f"📊 WebSocket Handlers initialized with {len(self.symbol_map)} symbols")
    
    def on_price_update(self, source: str, symbol: str, price: float,
                        volume: float = None, side: str = None,      # FIXED: added volume/side
                        timestamp: datetime = None):
        """Called whenever a new price arrives"""
        try:
            if timestamp is None:
                timestamp = datetime.now()

            add_transaction(source, symbol, price, volume, side)   # FIXED: was dashboard_feed.add_transaction
  
            # Validate price
            if price <= 0:
                return
            
            # Store latest price
            self.latest_prices[symbol] = {
                'price': price,
                'source': source,
                'timestamp': timestamp
            }
            
            # Update price history
            if symbol not in self.price_history:
                self.price_history[symbol] = []
            
            self.price_history[symbol].append({
                'price': price,
                'timestamp': timestamp
            })
            
            # Keep last 200 ticks for better analysis
            if len(self.price_history[symbol]) > 200:
                self.price_history[symbol].pop(0)
            
            # Map to asset format
            asset = self.symbol_map.get(symbol)
            if not asset:
                return
            
            # Check cooldown
            if asset in self.last_signal_time:
                time_diff = (timestamp - self.last_signal_time[asset]).total_seconds()
                if time_diff < self.signal_cooldown:
                    return
            
            # Get category
            category = self._get_category(asset)
            
            # Check trading opportunity
            self._check_trading_opportunity(asset, category, price, symbol)
            
        except Exception as e:
            logger.error(f"❌ Price update error: {e}")
    
    def _get_category(self, asset: str) -> str:
        """Get asset category"""
        if asset in ['BTC-USD', 'ETH-USD', 'BNB-USD', 'SOL-USD', 'XRP-USD']:
            return 'crypto'
        elif '/' in asset:
            return 'forex'
        elif asset in ['XAU/USD', 'XAG/USD', 'WTI/USD', 'NG/USD']:
            return 'commodities'
        else:
            return 'stocks'
    
    def _check_trading_opportunity(self, asset: str, category: str, price: float, symbol: str):
        """Check for trading opportunities using EMA"""
        if asset not in self.price_history or len(self.price_history[asset]) < 20:
            return
        
        # Get recent prices
        prices = [p['price'] for p in self.price_history[asset][-20:]]
        
        # Calculate EMA
        ema_fast = self._calculate_ema(prices, 5)
        ema_slow = self._calculate_ema(prices, 15)
        
        if ema_fast is None or ema_slow is None:
            return
        
        # Calculate volatility
        returns = np.diff(prices) / prices[:-1]
        volatility = np.std(returns) * 100
        
        # Dynamic threshold based on volatility
        threshold = max(0.3, min(1.0, volatility * 2))
        
        # Check for significant move
        price_change = (price - prices[0]) / prices[0] * 100
        
        if abs(price_change) > threshold:
            logger.info(f"⚡ {asset} moved {price_change:.2f}% (vol: {volatility:.2f}%)")
            
            # Check EMA crossover
            if ema_fast > ema_slow and price_change > 0:
                self._trigger_signal(asset, category, price, "BULLISH CROSS")
            elif ema_fast < ema_slow and price_change < 0:
                self._trigger_signal(asset, category, price, "BEARISH CROSS")
    
    def _calculate_ema(self, prices: list, period: int) -> float:
        """Calculate EMA"""
        if len(prices) < period:
            return None
        
        alpha = 2 / (period + 1)
        ema = prices[0]
        
        for price in prices[1:period]:
            ema = price * alpha + ema * (1 - alpha)
        
        return ema
    
    def _trigger_signal(self, asset: str, category: str, price: float, reason: str):
        """Trigger trading signal"""
        if not hasattr(self.bot, 'scan_asset_parallel'):
            return
        
        signal = self.bot.scan_asset_parallel(asset, category)
        
        if signal and signal.get('signal') != 'HOLD':
            signal['entry_price'] = price
            signal['reason'] = f"{signal.get('reason', '')} | WebSocket: {reason}"
            
            logger.info(f"🚀 WebSocket Signal: {asset} {signal['signal']} at ${price:.2f}")
            
            if hasattr(self.bot, 'paper_trader'):
                self.bot.paper_trader.execute_signal(signal)
                self.last_signal_time[asset] = datetime.now()