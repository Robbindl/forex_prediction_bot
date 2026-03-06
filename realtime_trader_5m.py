#!/usr/bin/env python
"""
5-MINUTE SCALPING BOT - Ultra-fast trades with ALL assets
Features:
- Trades ALL your assets (crypto, forex, stocks, commodities)
- Asset-specific stop losses (tighter for scalping)
- 5-minute timeframe for quick trades
- Real-time WebSocket data
"""

import pandas as pd
from realtime_trader import RealtimeTrader
from sqlalchemy import text

class ScalpingTrader5m(RealtimeTrader):
    """5-minute scalping version with ALL assets and tight stops"""
    
    def __init__(self, balance=30, strategy_mode='voting'):
        super().__init__(balance, strategy_mode)
        
        # Asset-specific scalping parameters
        self.scalp_params = {
            # Crypto - most volatile, tightest stops
            'crypto': {
                'stop_pct': 0.003,      # 0.3% stop
                'tp1_pct': 0.0045,       # 0.45% TP1
                'tp2_pct': 0.0075,       # 0.75% TP2
                'tp3_pct': 0.012,        # 1.2% TP3
                'min_rsi': 35,
                'max_rsi': 65
            },
            # Forex - medium volatility
            'forex': {
                'stop_pct': 0.0015,      # 0.15% stop (15 pips)
                'tp1_pct': 0.0022,       # 0.22% TP1 (22 pips)
                'tp2_pct': 0.0035,       # 0.35% TP2 (35 pips)
                'tp3_pct': 0.005,        # 0.5% TP3 (50 pips)
                'min_rsi': 40,
                'max_rsi': 60
            },
            # Stocks - least volatile, wider stops
            'stocks': {
                'stop_pct': 0.005,       # 0.5% stop
                'tp1_pct': 0.0075,       # 0.75% TP1
                'tp2_pct': 0.012,        # 1.2% TP2
                'tp3_pct': 0.018,        # 1.8% TP3
                'min_rsi': 30,
                'max_rsi': 70
            },
            # Commodities - like forex
            'commodities': {
                'stop_pct': 0.002,       # 0.2% stop
                'tp1_pct': 0.003,        # 0.3% TP1
                'tp2_pct': 0.005,        # 0.5% TP2
                'tp3_pct': 0.008,        # 0.8% TP3
                'min_rsi': 35,
                'max_rsi': 65
            }
        }
        
        print(f"\n⚡ 5-MINUTE SCALPING MODE ACTIVE")
        print(f"   • Trading ALL assets with tight stops")
        print(f"   • Crypto: 0.3% stops (15-30 min trades)")
        print(f"   • Forex: 0.15% stops (5-15 min trades)")
        print(f"   • Stocks: 0.5% stops (30-60 min trades)")
        print(f"   • All timeframes: 5-minute candles")
    
    def _get_asset_type(self, asset: str) -> str:
        """Determine asset type for parameter selection"""
        if '-USD' in asset and asset not in ['EUR/USD', 'GBP/USD']:
            return 'crypto'
        elif '/' in asset:
            return 'forex'
        elif asset in ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'NVDA', 'META']:
            return 'stocks'
        elif asset in ['XAU/USD', 'XAG/USD', 'WTI/USD', 'NG/USD']:
            return 'commodities'
        else:
            return 'stocks'  # Default
    
    def _calculate_scalp_levels(self, entry_price: float, asset_type: str, signal: str):
        """Calculate tight stop loss and take profit levels for scalping"""
        params = self.scalp_params.get(asset_type, self.scalp_params['stocks'])
        
        if signal == 'BUY':
            stop_loss = entry_price * (1 - params['stop_pct'])
            tp1 = entry_price * (1 + params['tp1_pct'])
            tp2 = entry_price * (1 + params['tp2_pct'])
            tp3 = entry_price * (1 + params['tp3_pct'])
        else:  # SELL
            stop_loss = entry_price * (1 + params['stop_pct'])
            tp1 = entry_price * (1 - params['tp1_pct'])
            tp2 = entry_price * (1 - params['tp2_pct'])
            tp3 = entry_price * (1 - params['tp3_pct'])
        
        return stop_loss, tp1, tp2, tp3, params
    
    def _generate_signal(self, asset: str, df: pd.DataFrame) -> dict:
        """Override to add scalping-specific signal generation"""
        from indicators.technical import TechnicalIndicators
        df = TechnicalIndicators.add_all_indicators(df)
        
        # Get asset type for parameters
        asset_type = self._get_asset_type(asset)
        params = self.scalp_params.get(asset_type, self.scalp_params['stocks'])
        
        # Get latest values
        latest = df.iloc[-1]
        rsi = latest.get('rsi', 50)
        
        # Scalping signal logic (faster, tighter)
        buy_score = 0
        sell_score = 0
        
        # RSI signals (tighter ranges for scalping)
        if rsi < params['min_rsi']:
            buy_score += 2
        elif rsi < 45:
            buy_score += 1
        elif rsi > params['max_rsi']:
            sell_score += 2
        elif rsi > 55:
            sell_score += 1
        
        # Moving averages (faster for scalping)
        if 'ema_5' in df.columns and 'ema_10' in df.columns:
            if latest['ema_5'] > latest['ema_10']:
                buy_score += 1
            else:
                sell_score += 1
        
        # Price vs Bollinger Bands
        if 'bb_middle' in df.columns:
            if latest['close'] < latest['bb_middle']:
                buy_score += 1
            else:
                sell_score += 1
        
        # Volume confirmation
        if 'volume' in df.columns:
            vol_ma = df['volume'].rolling(5).mean()
            if latest['volume'] > vol_ma.iloc[-1] * 1.2:
                if buy_score > sell_score:
                    buy_score += 1
                else:
                    sell_score += 1
        
        # Determine signal
        signal = None
        confidence = 0.5
        
        if buy_score >= 3 and buy_score > sell_score:
            confidence = min(0.5 + buy_score * 0.08, 0.8)
            stop_loss, tp1, tp2, tp3, params = self._calculate_scalp_levels(
                latest['close'], asset_type, 'BUY'
            )
            signal = {
                'signal': 'BUY',
                'confidence': confidence,
                'entry_price': latest['close'],
                'stop_loss': stop_loss,
                'take_profit_levels': [
                    {'level': 1, 'price': tp1},
                    {'level': 2, 'price': tp2},
                    {'level': 3, 'price': tp3}
                ],
                'reason': f"Scalp BUY (RSI: {rsi:.1f}, score: {buy_score})",
                'strategy': 'SCALPING_5M',
                'asset': asset
            }
            
        elif sell_score >= 3 and sell_score > buy_score:
            confidence = min(0.5 + sell_score * 0.08, 0.8)
            stop_loss, tp1, tp2, tp3, params = self._calculate_scalp_levels(
                latest['close'], asset_type, 'SELL'
            )
            signal = {
                'signal': 'SELL',
                'confidence': confidence,
                'entry_price': latest['close'],
                'stop_loss': stop_loss,
                'take_profit_levels': [
                    {'level': 1, 'price': tp1},
                    {'level': 2, 'price': tp2},
                    {'level': 3, 'price': tp3}
                ],
                'reason': f"Scalp SELL (RSI: {rsi:.1f}, score: {sell_score})",
                'strategy': 'SCALPING_5M',
                'asset': asset
            }
        
        return signal
    
    def _get_historical_data(self, asset: str) -> pd.DataFrame:
        """Get 5-minute historical data"""
        # Try database first
        try:
            query = text("""
            SELECT timestamp, open, high, low, close, volume
            FROM market_data
            WHERE asset = :asset
            ORDER BY timestamp DESC
            LIMIT 200
            """)
            df = pd.read_sql(query, self.db.session.bind, params={'asset': asset})
            if not df.empty:
                return df.sort_values('timestamp').set_index('timestamp')
        except:
            pass
        
        # Fetch 5m data from Yahoo (3 days of 5m data = 864 candles)
        return self.bot.fetch_historical_data(asset, days=3, interval='5m')
    
    def _map_symbol(self, symbol: str) -> str:
        """Map WebSocket symbols to your format - ALL ASSETS INCLUDED"""
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
            'OANDA:NZD_USD': 'NZD/USD',
            'OANDA:USD_CHF': 'USD/CHF',
            
            # Commodities (Finnhub OANDA)
            'OANDA:XAU_USD': 'XAU/USD',
            'OANDA:XAG_USD': 'XAG/USD',
            'OANDA:WTICO_USD': 'WTI/USD',
            'OANDA:NATGAS_USD': 'NG/USD',
            'OANDA:XCU_USD': 'XCU/USD',
        }
        return mapping.get(symbol)
    
    def on_price_update(self, symbol: str, price: float, timestamp):
        """Override to use 5m scalping logic"""
        # Skip cooldown check
        if symbol in self.last_signal_time:
            time_diff = (timestamp - self.last_signal_time[symbol]).total_seconds()
            if time_diff < 180:  # 3 minute cooldown for 5m chart
                return
        
        asset = self._map_symbol(symbol)
        if not asset:
            return
        
        # Get 5m historical data
        df = self._get_historical_data(asset)
        if df.empty:
            return
        
        # Generate scalping signal
        signal = self._generate_signal(asset, df)
        
        if signal and signal['signal'] != 'HOLD':
            print(f"\n⚡ 5M SCALP SIGNAL: {asset} {signal['signal']} at ${price:.2f}")
            self.last_signal_time[symbol] = timestamp
            
            # Execute trade with tight scalping parameters
            self._execute_trade(signal, price)

if __name__ == "__main__":
    trader = ScalpingTrader5m(balance=30, strategy_mode='voting')
    trader.start()