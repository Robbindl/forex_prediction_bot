"""
Real-time Trading Bot with WebSocket Integration
STABLE VERSION with health monitoring and improved error handling
"""

import threading
import time
from datetime import datetime
import pandas as pd
from data.websocket_fetcher import WebSocketFetcher
from trading_system import UltimateTradingSystem
from services.database_service import DatabaseService
from logger import logger

class RealtimeTrader:
    """
    Trading bot that uses WebSocket for real-time price updates
    """
    
    def __init__(self, balance=30, strategy_mode='voting'):
        self.bot = UltimateTradingSystem(account_balance=balance)
        self.strategy_mode = strategy_mode
        self.db = DatabaseService()
        
        # Initialize WebSocket with stable version
        self.ws = WebSocketFetcher(on_price_callback=self.on_price_update)
        
        # Track positions and signals
        self.last_signal_time = {}
        self.cooldown = 60  # Seconds between signals for same asset
        self.signal_count = 0
        self.error_count = 0
        self.last_health_check = datetime.now()
        self.is_running = False
        
        logger.info(f"🚀 Real-time Trader initialized with {strategy_mode} strategy")
        logger.info(f"   • Tracking: Crypto, Stocks, Forex, Gold, Silver, Oil")
    
    def on_price_update(self, symbol: str, price: float, timestamp: datetime):
        """
        Called whenever WebSocket receives a new price
        This is where you can implement ultra-fast trading logic
        """
        try:
            # Skip if we just traded this symbol
            if symbol in self.last_signal_time:
                time_diff = (timestamp - self.last_signal_time[symbol]).total_seconds()
                if time_diff < self.cooldown:
                    return
            
            # Map symbol to your asset format
            asset = self._map_symbol(symbol)
            if not asset:
                return
            
            # Fetch historical data for this asset
            df = self._get_historical_data(asset)
            if df.empty:
                logger.debug(f"No historical data for {asset}, skipping")
                return
            
            # Generate signal using voting engine
            signal = self._generate_signal(asset, df)
            
            if signal and signal['signal'] != 'HOLD':
                self.signal_count += 1
                logger.info(f"⚡ REAL-TIME SIGNAL #{self.signal_count}: {asset} {signal['signal']} at ${price:.2f}")
                self.last_signal_time[symbol] = timestamp
                
                # Execute trade (paper trading)
                self._execute_trade(signal, price)
                
        except Exception as e:
            self.error_count += 1
            logger.error(f"Error processing price update for {symbol}: {e}")
    
    def _map_symbol(self, symbol: str) -> str:
        """Map WebSocket symbol to your asset format - FIXED with commodities"""
        mapping = {
            # Crypto (Binance)
            'BTCUSDT': 'BTC-USD',
            'ETHUSDT': 'ETH-USD',
            'BNBUSDT': 'BNB-USD',
            'XRPUSDT': 'XRP-USD',
            'SOLUSDT': 'SOL-USD',
            'ADAUSDT': 'ADA-USD',
            'DOGEUSDT': 'DOGE-USD',
            'DOTUSDT': 'DOT-USD',
            'LTCUSDT': 'LTC-USD',
            'AVAXUSDT': 'AVAX-USD',
            'LINKUSDT': 'LINK-USD',
            
            # Stocks (Finnhub)
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
            
            # Forex (Finnhub OANDA)
            'OANDA:EUR_USD': 'EUR/USD',
            'OANDA:GBP_USD': 'GBP/USD',
            'OANDA:USD_JPY': 'USD/JPY',
            'OANDA:AUD_USD': 'AUD/USD',
            'OANDA:USD_CAD': 'USD/CAD',
            'OANDA:USD_CHF': 'USD/CHF',
            'OANDA:NZD_USD': 'NZD/USD',
            
            # ===== COMMODITY SPOT (Finnhub OANDA) =====
            'OANDA:XAU_USD': 'XAU/USD',      # Gold Spot
            'OANDA:XAG_USD': 'XAG/USD',      # Silver Spot
            'OANDA:XPT_USD': 'XPT/USD',      # Platinum Spot
            'OANDA:XPD_USD': 'XPD/USD',      # Palladium Spot
            'OANDA:WTICO_USD': 'WTI/USD',    # WTI Crude Oil Spot
            'OANDA:NATGAS_USD': 'NG/USD',    # Natural Gas Spot
            'OANDA:XCU_USD': 'XCU/USD',      # Copper Spot
            
            # Futures fallbacks
            'GC=F': 'GC=F',                   # Gold Futures
            'SI=F': 'SI=F',                   # Silver Futures
            'CL=F': 'CL=F',                   # WTI Futures
        }
        return mapping.get(symbol)
    
    def _get_historical_data(self, asset: str) -> pd.DataFrame:
        """Get historical data for signal generation"""
        # Try to get from database first
        try:
            from sqlalchemy import text
            
            # Check if market_data table exists
            check_query = text("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'market_data'
            );
            """)
            
            table_exists = self.db.session.execute(check_query).scalar()
            
            if table_exists:
                query = text("""
                SELECT timestamp, open, high, low, close, volume
                FROM market_data
                WHERE asset = :asset
                ORDER BY timestamp DESC
                LIMIT 100
                """)
                
                df = pd.read_sql(query, self.db.session.bind, params={'asset': asset})
                if not df.empty:
                    df = df.sort_values('timestamp')
                    df.set_index('timestamp', inplace=True)
                    logger.debug(f"Got {len(df)} rows from database for {asset}")
                    return df
        except Exception as e:
            logger.debug(f"Database query error for {asset}: {e}")
        
        # Fallback to API fetch
        try:
            df = self.bot.fetch_historical_data(asset, days=7, interval='5m')
            if not df.empty:
                logger.debug(f"Got {len(df)} rows from API for {asset}")
            return df
        except Exception as e:
            logger.debug(f"API fetch failed for {asset}: {e}")
            return pd.DataFrame()
    
    def _generate_signal(self, asset: str, df: pd.DataFrame) -> dict:
        """Generate trading signal using your strategies"""
        try:
            # Add indicators
            from indicators.technical import TechnicalIndicators
            df = TechnicalIndicators.add_all_indicators(df)
            
            # Use voting engine (all 12 strategies)
            if hasattr(self.bot, 'voting_engine'):
                signals = self.bot.voting_engine.get_all_signals(df)
                combined_signal = self.bot.voting_engine.weighted_vote(signals)
                
                # Add the asset to the signal
                if combined_signal:
                    combined_signal['asset'] = asset
                
                return combined_signal
            
            # Fallback to single strategy
            elif self.strategy_mode == 'fast' and hasattr(self.bot, 'fast_strategy'):
                signal = self.bot.fast_strategy(df, df)
                if signal:
                    signal['asset'] = asset
                return signal
            
        except Exception as e:
            logger.error(f"Signal generation error for {asset}: {e}")
        
        return None
    
    def _execute_trade(self, signal: dict, current_price: float):
        """Execute a paper trade"""
        try:
            # Update signal with current price
            signal['entry_price'] = current_price
            
            # Asset should already be set from _generate_signal, but double-check
            if 'asset' not in signal or not signal['asset']:
                logger.warning(f"Signal missing asset, skipping")
                return
            
            # Use your paper trader
            if hasattr(self.bot, 'paper_trader'):
                result = self.bot.paper_trader.execute_signal(signal)
                if result:
                    logger.info(f"  ✅ Trade executed: {result['trade_id']} for {signal['asset']}")
                else:
                    logger.debug(f"Trade execution skipped for {signal['asset']} (filtered)")
        except Exception as e:
            logger.error(f"Trade execution error: {e}")
    
    def start(self):
        """Start real-time trading with health monitoring"""
        self.is_running = True
        
        logger.info("="*60)
        logger.info("🚀 STARTING REAL-TIME TRADING WITH WEBSOCKET")
        logger.info("="*60)
        
        # Connect to WebSocket
        self.ws.connect_all()
        
        # Add callback
        self.ws.add_price_callback(self.on_price_update)
        
        logger.info("✅ Real-time trading active!")
        logger.info("📡 Waiting for price updates...")
        logger.info("   • Crypto: BTC, ETH, BNB, SOL, XRP, etc.")
        logger.info("   • Stocks: AAPL, MSFT, GOOGL, etc.")
        logger.info("   • Forex: EUR/USD, GBP/USD, USD/JPY")
        logger.info("   • Commodities: Gold, Silver, Oil")
        
        # Start health monitoring thread
        def health_monitor():
            while self.is_running:
                try:
                    time.sleep(30)  # Check every 30 seconds
                    
                    if not self.ws.running:
                        logger.warning("WebSocket not running, reconnecting...")
                        self.ws.connect_all()
                    
                    # Periodic status update
                    if (datetime.now() - self.last_health_check).seconds > 300:  # Every 5 minutes
                        logger.info(f"Status: {self.signal_count} signals, {self.error_count} errors")
                        self.last_health_check = datetime.now()
                        
                except Exception as e:
                    logger.error(f"Health monitor error: {e}")
        
        monitor_thread = threading.Thread(target=health_monitor, daemon=True)
        monitor_thread.start()
        
        # Keep running
        try:
            while self.is_running:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("\n🛑 Stopping real-time trader...")
            self.stop()
    
    def stop(self):
        """Stop the real-time trader"""
        self.is_running = False
        if hasattr(self, 'ws'):
            self.ws.stop()
        logger.info("Real-time trader stopped")

if __name__ == "__main__":
    trader = RealtimeTrader(balance=30, strategy_mode='voting')
    trader.start()