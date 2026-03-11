"""
StrategyEngine — pure technical strategy methods extracted from UltimateTradingSystem.
These methods operate only on DataFrames and have no system-level side effects.
UltimateTradingSystem delegates to this class via self.strategy_engine.
"""

import numpy as np
import pandas as pd
from typing import List, Dict, Optional
from logger import logger


class StrategyEngine:
    """
    All pure technical strategy methods.
    Instantiate once, reuse across the trading system lifetime.
    """

    def __init__(self):
        logger.info("StrategyEngine initialised")

    def rsi_strategy(self, df: pd.DataFrame) -> List[Dict]:
        """RSI Mean Reversion Strategy"""
        signals = []
        for idx, row in df.iterrows():
            if 'rsi' not in df.columns:
                continue
        
            rsi = row['rsi']
            if pd.isna(rsi):
                continue
        
            # RSI oversold/overbought with confirmation
            if rsi < 25:  # Extremely oversold
                if df['close'].iloc[-3:].pct_change().mean() < -0.02:  # Strong downtrend
                    signals.append({
                        'date': idx,
                        'signal': 'BUY',
                        'confidence': 0.85,
                        'entry': row['close'],
                        'stop_loss': row['close'] * 0.95,  # 5% stop
                        'take_profit': row['close'] * 1.08,  # 8% target
                        'strategy': 'rsi_oversold'
                    })
            elif rsi > 75:  # Extremely overbought
                if df['close'].iloc[-3:].pct_change().mean() > 0.02:  # Strong uptrend
                    signals.append({
                        'date': idx,
                        'signal': 'SELL',
                        'confidence': 0.85,
                        'entry': row['close'],
                        'stop_loss': row['close'] * 1.05,
                        'take_profit': row['close'] * 0.92,
                        'strategy': 'rsi_overbought'
                    })
        return signals

    def macd_strategy(self, df: pd.DataFrame) -> List[Dict]:
        """MACD Crossover Strategy"""
        signals = []
        for i in range(1, len(df)):
            if 'macd' not in df.columns or 'macd_signal' not in df.columns:
                continue
        
            curr = df.iloc[i]
            prev = df.iloc[i-1]
        
            # MACD crosses above signal line
            if prev['macd'] <= prev['macd_signal'] and curr['macd'] > curr['macd_signal']:
                if curr['macd'] < 0:  # Bullish divergence
                    signals.append({
                        'date': df.index[i],
                        'signal': 'BUY',
                        'confidence': 0.8,
                        'entry': curr['close'],
                        'stop_loss': curr['close'] * 0.97,
                        'take_profit': curr['close'] * 1.06,
                        'strategy': 'macd_bullish'
                    })
        
            # MACD crosses below signal line
            elif prev['macd'] >= prev['macd_signal'] and curr['macd'] < curr['macd_signal']:
                if curr['macd'] > 0:  # Bearish divergence
                    signals.append({
                        'date': df.index[i],
                        'signal': 'SELL',
                        'confidence': 0.8,
                        'entry': curr['close'],
                        'stop_loss': curr['close'] * 1.03,
                        'take_profit': curr['close'] * 0.94,
                        'strategy': 'macd_bearish'
                    })
        return signals

    def bollinger_strategy(self, df: pd.DataFrame) -> List[Dict]:
        """Bollinger Band Breakout Strategy"""
        signals = []
        for idx, row in df.iterrows():
            if 'bb_upper' not in df.columns or 'bb_lower' not in df.columns:
                continue
        
            # Price touches lower band with volume confirmation
            if row['close'] <= row['bb_lower']:
                if 'volume' in df.columns and row['volume'] > df['volume'].rolling(20).mean().iloc[-1]:
                    signals.append({
                        'date': idx,
                        'signal': 'BUY',
                        'confidence': 0.75,
                        'entry': row['close'],
                        'stop_loss': row['bb_lower'] * 0.98,
                        'take_profit': row['bb_middle'] * 1.1,
                        'strategy': 'bb_oversold'
                    })
        
            # Price touches upper band
            elif row['close'] >= row['bb_upper']:
                if 'volume' in df.columns and row['volume'] > df['volume'].rolling(20).mean().iloc[-1]:
                    signals.append({
                        'date': idx,
                        'signal': 'SELL',
                        'confidence': 0.75,
                        'entry': row['close'],
                        'stop_loss': row['bb_upper'] * 1.02,
                        'take_profit': row['bb_middle'] * 0.9,
                        'strategy': 'bb_overbought'
                    })
        return signals

    def ma_cross_strategy(self, df: pd.DataFrame) -> List[Dict]:
        """Moving Average Crossover Strategy"""
        signals = []
        for i in range(1, len(df)):
            if 'sma_20' not in df.columns or 'sma_50' not in df.columns:
                continue
        
            curr = df.iloc[i]
            prev = df.iloc[i-1]
        
            # Golden Cross (20 crosses above 50)
            if prev['sma_20'] <= prev['sma_50'] and curr['sma_20'] > curr['sma_50']:
                signals.append({
                    'date': df.index[i],
                    'signal': 'BUY',
                    'confidence': 0.7,
                    'entry': curr['close'],
                    'stop_loss': curr['sma_50'] * 0.95,
                    'take_profit': curr['close'] * 1.1,
                    'strategy': 'golden_cross'
                })
        
            # Death Cross (20 crosses below 50)
            elif prev['sma_20'] >= prev['sma_50'] and curr['sma_20'] < curr['sma_50']:
                signals.append({
                    'date': df.index[i],
                    'signal': 'SELL',
                    'confidence': 0.7,
                    'entry': curr['close'],
                    'stop_loss': curr['sma_50'] * 1.05,
                    'take_profit': curr['close'] * 0.9,
                    'strategy': 'death_cross'
                })
        return signals

    def custom_rsi_strategy(self, df, oversold, overbought):
        """Custom RSI with adjustable levels"""
        signals = []
        for idx, row in df.iterrows():
            if 'rsi' not in df.columns:
                continue
            rsi = row['rsi']
            if pd.isna(rsi):
                continue
        
            if rsi < oversold:
                signals.append({
                    'date': idx,
                    'signal': 'BUY',
                    'confidence': 0.8,
                    'entry': row['close'],
                    'stop_loss': row['close'] * 0.97,
                    'take_profit': row['close'] * 1.06
                })
            elif rsi > overbought:
                signals.append({
                    'date': idx,
                    'signal': 'SELL',
                    'confidence': 0.8,
                    'entry': row['close'],
                    'stop_loss': row['close'] * 1.03,
                    'take_profit': row['close'] * 0.94
                })
        return signals

    def custom_macd_strategy(self, df, fast, slow, signal):
        """Custom MACD with adjustable parameters"""
        # Calculate custom MACD
        exp1 = df['close'].ewm(span=fast, adjust=False).mean()
        exp2 = df['close'].ewm(span=slow, adjust=False).mean()
        macd_line = exp1 - exp2
        signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    
        signals = []
        for i in range(1, len(df)):
            if macd_line.iloc[i-1] <= signal_line.iloc[i-1] and macd_line.iloc[i] > signal_line.iloc[i]:
                signals.append({
                    'date': df.index[i],
                    'signal': 'BUY',
                    'confidence': 0.8,
                    'entry': df['close'].iloc[i],
                    'stop_loss': df['close'].iloc[i] * 0.97,
                    'take_profit': df['close'].iloc[i] * 1.06
                })
            elif macd_line.iloc[i-1] >= signal_line.iloc[i-1] and macd_line.iloc[i] < signal_line.iloc[i]:
                signals.append({
                    'date': df.index[i],
                    'signal': 'SELL',
                    'confidence': 0.8,
                    'entry': df['close'].iloc[i],
                    'stop_loss': df['close'].iloc[i] * 1.03,
                    'take_profit': df['close'].iloc[i] * 0.94
                })
        return signals

# ============= ML TRAINING PIPELINE =============

    def breakout_strategy(self, df: pd.DataFrame) -> List[Dict]:
        """
        Breakout trading - enter when price breaks resistance/support
        """
        signals = []
        if len(df) < 50:
            return signals
    
        df = df.copy()
    
        # Calculate resistance and support levels
        df['resistance'] = df['high'].rolling(20).max()
        df['support'] = df['low'].rolling(20).min()
    
        # Volume confirmation
        if 'volume' in df.columns:
            df['volume_avg'] = df['volume'].rolling(20).mean()
    
        latest = df.iloc[-1]
        prev = df.iloc[-2]
    
        # Volume confirmation
        volume_ok = True
        if 'volume' in df.columns and 'volume_avg' in df.columns:
            volume_ok = latest['volume'] > df['volume_avg'].iloc[-1] * 1.2
    
        # Breakout above resistance
        if (prev['close'] < prev['resistance'] and 
            latest['close'] > latest['resistance'] and 
            volume_ok):
        
            signals.append({
                'date': df.index[-1],
                'signal': 'BUY',
                'confidence': 0.8,
                'entry': latest['close'],
                'stop_loss': latest['support'],
                'take_profit': latest['close'] * 1.05,
                'take_profit_levels': [
                    {'level': 1, 'price': latest['close'] * 1.02},
                    {'level': 2, 'price': latest['close'] * 1.05},
                    {'level': 3, 'price': latest['close'] * 1.08}
                ],
                'strategy': 'breakout',
                'reason': 'Breakout above resistance with volume'
            })
    
        # Breakdown below support
        elif (prev['close'] > prev['support'] and 
            latest['close'] < latest['support'] and 
            volume_ok):
        
            signals.append({
                'date': df.index[-1],
                'signal': 'SELL',
                'confidence': 0.8,
                'entry': latest['close'],
                'stop_loss': latest['resistance'],
                'take_profit': latest['close'] * 0.95,
                'take_profit_levels': [
                    {'level': 1, 'price': latest['close'] * 0.98},
                    {'level': 2, 'price': latest['close'] * 0.95},
                    {'level': 3, 'price': latest['close'] * 0.92}
                ],
                'strategy': 'breakout',
                'reason': 'Breakdown below support with volume'
            })
    
        return signals

    def mean_reversion_strategy(self, df: pd.DataFrame) -> List[Dict]:
        """Mean reversion - buy dips, sell rallies"""
        signals = []
        if len(df) < 50:
            return signals

        # Calculate mean and standard deviation
        df['mean'] = df['close'].rolling(20).mean()
        df['std'] = df['close'].rolling(20).std()
        df['upper'] = df['mean'] + 2 * df['std']
        df['lower'] = df['mean'] - 2 * df['std']

        latest = df.iloc[-1]

        # Price far below mean - buy signal
        if latest['close'] < latest['lower']:
            signals.append({
                'date': df.index[-1],
                'signal': 'BUY',
                'confidence': 0.75,
                'entry': latest['close'],
                'stop_loss': latest['close'] * 0.97,
                'take_profit': latest['mean'],
                'strategy': 'mean_reversion'
            })

        # Price far above mean - sell signal
        elif latest['close'] > latest['upper']:
            signals.append({
                'date': df.index[-1],
                'signal': 'SELL',
                'confidence': 0.75,
                'entry': latest['close'],
                'stop_loss': latest['close'] * 1.03,
                'take_profit': latest['mean'],
                'strategy': 'mean_reversion'
            })

        return signals

    def scalping_strategy(self, df: pd.DataFrame) -> List[Dict]:
        """
        Scalping - Fast, small profits on short timeframes
        LOOSENED for volatile markets
        """
        signals = []
        if len(df) < 20:
            return signals
    
        df = df.copy()
        latest = df.iloc[-1]
    
        # DYNAMIC STOP LOSS based on asset type
        def get_stop_pct(asset_name):
            """Get appropriate stop % for different assets"""
            if asset_name and 'USD' in asset_name and '-' in asset_name:  # Crypto
                return 0.005  # ← CHANGED from 0.003 to 0.5% (more room)
            elif asset_name and '/' in asset_name:  # Forex
                return 0.003  # ← CHANGED from 0.002 to 0.3%
            else:  # Stocks
                return 0.008  # ← CHANGED from 0.005 to 0.8%
    
        stop_pct = get_stop_pct(getattr(self, "current_asset", None))
    
        # Calculate signals
        if 'rsi' in df.columns:
            rsi = latest['rsi']
        else:
            rsi = 50
    
        # Volume spike detection
        volume_spike = True
        if 'volume' in df.columns:
            volume_ma = df['volume'].rolling(10).mean()
            volume_spike = latest['volume'] > volume_ma.iloc[-1] * 1.2
    
        # LOOSENED entry conditions
        buy_score = 0
        sell_score = 0
    
        # RSI conditions - LOOSENED
        if rsi < 55:  # ← CHANGED from 40
            buy_score += 1
        if rsi > 45:  # ← CHANGED from 60 (more sensitive)
            sell_score += 1
    
        # Moving average conditions
        if 'ema_5' in df.columns and 'ema_10' in df.columns:
            if latest['ema_5'] > latest['ema_10']:
                buy_score += 1
            else:
                sell_score += 1
    
        # Price position - LOOSENED
        if 'bb_middle' in df.columns:
            if latest['close'] < latest['bb_middle'] * 1.02:  # ← CHANGED from just below
                buy_score += 1
            if latest['close'] > latest['bb_middle'] * 0.98:  # ← CHANGED from just above
                sell_score += 1
    
        # Volume confirmation
        if volume_spike:
            buy_score += 1
            sell_score += 1
    
        # Generate signal if score is high enough - LOWERED THRESHOLD
        if buy_score >= 2.0:  # ← CHANGED from 2.5
            confidence = min(0.5 + buy_score * 0.1, 0.85)  # ← CHANGED max from 0.8 to 0.85
        
            # LOOSER stops and targets
            if latest['close'] < 10:  # Cheap assets like crypto
                stop_loss = latest['close'] * (1 - stop_pct)
                tp1 = latest['close'] * (1 + stop_pct * 2.0)  # ← CHANGED from 1.5
                tp2 = latest['close'] * (1 + stop_pct * 3.5)  # ← CHANGED from 2.5
                tp3 = latest['close'] * (1 + stop_pct * 5.0)  # ← CHANGED from 4.0
            else:  # Normal assets
                stop_loss = latest['close'] * (1 - stop_pct)
                tp1 = latest['close'] * (1 + stop_pct * 2.0)
                tp2 = latest['close'] * (1 + stop_pct * 3.5)
                tp3 = latest['close'] * (1 + stop_pct * 5.0)
        
            signals.append({
                'date': df.index[-1],
                'signal': 'BUY',
                'confidence': confidence,
                'entry': latest['close'],
                'stop_loss': stop_loss,
                'take_profit_levels': [
                    {'level': 1, 'price': tp1},
                    {'level': 2, 'price': tp2},
                    {'level': 3, 'price': tp3}
                ],
                'strategy': 'scalping',
                'reason': f'Scalp BUY (RSI: {rsi:.1f})'
            })
    
        elif sell_score >= 2.0:  # ← CHANGED from 2.5
            confidence = min(0.5 + sell_score * 0.1, 0.85)
        
            stop_loss = latest['close'] * (1 + stop_pct)
            tp1 = latest['close'] * (1 - stop_pct * 2.0)
            tp2 = latest['close'] * (1 - stop_pct * 3.5)
            tp3 = latest['close'] * (1 - stop_pct * 5.0)
        
            signals.append({
                'date': df.index[-1],
                'signal': 'SELL',
                'confidence': confidence,
                'entry': latest['close'],
                'stop_loss': stop_loss,
                'take_profit_levels': [
                    {'level': 1, 'price': tp1},
                    {'level': 2, 'price': tp2},
                    {'level': 3, 'price': tp3}
                ],
                'strategy': 'scalping',
                'reason': f'Scalp SELL (RSI: {rsi:.1f})'
            })
    
        return signals

    def trend_following_strategy(self, df: pd.DataFrame) -> List[Dict]:
        """
        Trend Following - Ride the trend using moving averages and ADX
        Best for: Strong trending markets
        """
        signals = []
        if len(df) < 50:
            return signals
    
        # Make a copy to avoid modifying original
        df = df.copy()
    
        # Calculate required indicators if they don't exist
        # EMAs
        if 'ema_9' not in df.columns:
            df['ema_9'] = df['close'].ewm(span=9, adjust=False).mean()
        if 'ema_21' not in df.columns:
            df['ema_21'] = df['close'].ewm(span=21, adjust=False).mean()
    
        # SMAs
        if 'sma_50' not in df.columns:
            df['sma_50'] = df['close'].rolling(50).mean()
        if 'sma_200' not in df.columns:
            df['sma_200'] = df['close'].rolling(200).mean()
    
        # ADX (Average Directional Index) - calculate if not present
        if 'adx' not in df.columns:
            # Simplified ADX calculation
            high = df['high']
            low = df['low']
            close = df['close']
        
            # True Range
            tr1 = high - low
            tr2 = abs(high - close.shift())
            tr3 = abs(low - close.shift())
            tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
            atr = tr.rolling(14).mean()
        
            # Directional Movement
            up_move = high - high.shift()
            down_move = low.shift() - low
        
            plus_dm = (up_move > down_move) & (up_move > 0)
            plus_dm = plus_dm.astype(float) * up_move
            minus_dm = (down_move > up_move) & (down_move > 0)
            minus_dm = minus_dm.astype(float) * down_move
        
            # Smoothed DM
            plus_di = 100 * (plus_dm.rolling(14).mean() / atr)
            minus_di = 100 * (minus_dm.rolling(14).mean() / atr)
        
            # ADX
            dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di)
            df['adx'] = dx.rolling(14).mean()
    
        latest = df.iloc[-1]
        prev = df.iloc[-2]
    
        # Get ADX value
        adx = latest['adx'] if not pd.isna(latest['adx']) else 20
    
        # Bullish trend conditions
        bullish_conditions = [
            latest['ema_9'] > latest['ema_21'],           # Fast MA above slow MA
            latest['ema_21'] > latest['sma_50'],          # Medium above long
            latest['close'] > latest['ema_9'],            # Price above fast MA
            latest['close'] > latest['sma_200'],          # Price above 200 MA (long-term uptrend)
            adx > 22,                                      # Trending market (not ranging)
            latest['close'] > df['close'].rolling(20).mean().iloc[-1],  # Above 20 MA
        ]
    
        # Bearish trend conditions
        bearish_conditions = [
            latest['ema_9'] < latest['ema_21'],
            latest['ema_21'] < latest['sma_50'],
            latest['close'] < latest['ema_9'],
            latest['close'] < latest['sma_200'],
            adx > 22,
            latest['close'] < df['close'].rolling(20).mean().iloc[-1],
        ]
    
        bullish_score = sum(bullish_conditions) / len(bullish_conditions)
        bearish_score = sum(bearish_conditions) / len(bearish_conditions)
    
        # Entry signals on pullbacks to moving averages
        if bullish_score > 0.6:
            # Check if price pulled back to EMA 21 (potential entry)
            distance_to_ema = abs(latest['close'] - latest['ema_21']) / latest['close']
            if distance_to_ema < 0.015:  # Within 1.5% of EMA 21
                confidence = min(0.7 + bullish_score * 0.2, 0.9)
            
                # Calculate dynamic take profits
                atr_value = df['atr'].iloc[-1] if 'atr' in df.columns else latest['close'] * 0.02
            
                signals.append({
                    'date': df.index[-1],
                    'signal': 'BUY',
                    'confidence': confidence,
                    'entry': latest['close'],
                    'stop_loss': latest['sma_50'] * 0.98,  # Stop below 50 MA
                    'take_profit': latest['close'] + (atr_value * 3),
                    'take_profit_levels': [
                        {'level': 1, 'price': latest['close'] + atr_value},
                        {'level': 2, 'price': latest['close'] + (atr_value * 2)},
                        {'level': 3, 'price': latest['close'] + (atr_value * 3)}
                    ],
                    'strategy': 'trend_following',
                    'reason': f'Bullish trend with pullback (ADX: {adx:.1f})',
                    'trend_strength': adx
                })
    
        elif bearish_score > 0.6:
            # Check if price rallied to EMA 21 (potential short entry)
            distance_to_ema = abs(latest['close'] - latest['ema_21']) / latest['close']
            if distance_to_ema < 0.015:
                confidence = min(0.7 + bearish_score * 0.2, 0.9)
            
                atr_value = df['atr'].iloc[-1] if 'atr' in df.columns else latest['close'] * 0.02
            
                signals.append({
                    'date': df.index[-1],
                    'signal': 'SELL',
                    'confidence': confidence,
                    'entry': latest['close'],
                    'stop_loss': latest['sma_50'] * 1.02,
                    'take_profit': latest['close'] - (atr_value * 3),
                    'take_profit_levels': [
                        {'level': 1, 'price': latest['close'] - atr_value},
                        {'level': 2, 'price': latest['close'] - (atr_value * 2)},
                        {'level': 3, 'price': latest['close'] - (atr_value * 3)}
                    ],
                    'strategy': 'trend_following',
                    'reason': f'Bearish trend with pullback (ADX: {adx:.1f})',
                    'trend_strength': adx
                })
    
        return signals

    def arbitrage_strategy(self, df: pd.DataFrame, related_asset_df: pd.DataFrame = None) -> List[Dict]:
        """
        🔄 Arbitrage - Exploit price differences between related assets
        Examples: BTC/ETH ratio, EUR/USD and DXY, Gold and Silver
        """
        signals = []
    
        # If no related asset provided, can't do arbitrage
        if related_asset_df is None or len(df) < 20 or len(related_asset_df) < 20:
            return signals
    
        # Calculate ratio between two assets
        asset1 = df['close']
        asset2 = related_asset_df['close']
    
        # Align the data
        common_dates = asset1.index.intersection(asset2.index)
        if len(common_dates) < 20:
            return signals
    
        ratio = asset1.loc[common_dates] / asset2.loc[common_dates]
    
        # Calculate mean and standard deviation of the ratio
        ratio_mean = ratio.mean()
        ratio_std = ratio.std()
        current_ratio = ratio.iloc[-1]
        z_score = (current_ratio - ratio_mean) / ratio_std
    
        # Pairs trading - mean reversion of the ratio
        if abs(z_score) > 2:  # Ratio is significantly deviated
            confidence = min(abs(z_score) * 0.3, 0.85)
        
            if z_score > 2:  # Asset1 overvalued vs Asset2
                signals.append({
                    'date': common_dates[-1],
                    'signal': 'PAIR_TRADE',
                    'confidence': confidence,
                    'entry': {
                        'asset1': df['close'].iloc[-1],
                        'asset2': related_asset_df['close'].iloc[-1],
                        'ratio': current_ratio
                    },
                    'actions': [
                        {'asset': df.name if hasattr(df, 'name') else 'ASSET1', 'action': 'SELL'},
                        {'asset': related_asset_df.name if hasattr(related_asset_df, 'name') else 'ASSET2', 'action': 'BUY'}
                    ],
                    'take_profit_ratio': ratio_mean,
                    'stop_loss_ratio': current_ratio * 1.1 if z_score > 0 else current_ratio * 0.9,
                    'strategy': 'arbitrage',
                    'reason': f'Pair trade: Ratio z-score = {z_score:.2f}',
                    'z_score': z_score
                })
        
            elif z_score < -2:  # Asset1 undervalued vs Asset2
                signals.append({
                    'date': common_dates[-1],
                    'signal': 'PAIR_TRADE',
                    'confidence': confidence,
                    'entry': {
                        'asset1': df['close'].iloc[-1],
                        'asset2': related_asset_df['close'].iloc[-1],
                        'ratio': current_ratio
                    },
                    'actions': [
                        {'asset': df.name if hasattr(df, 'name') else 'ASSET1', 'action': 'BUY'},
                        {'asset': related_asset_df.name if hasattr(related_asset_df, 'name') else 'ASSET2', 'action': 'SELL'}
                    ],
                    'take_profit_ratio': ratio_mean,
                    'stop_loss_ratio': current_ratio * 0.9,
                    'strategy': 'arbitrage',
                    'reason': f'Pair trade: Ratio z-score = {z_score:.2f}',
                    'z_score': z_score
                })
    
        # Triangular arbitrage for forex (simplified)
        if 'EUR/USD' in str(df.name) and 'GBP/USD' in str(related_asset_df.name):
            # This would check EUR/GBP cross rate for arbitrage opportunities
            pass
    
        return signals

    def triangular_arbitrage_check(self, eur_usd, gbp_usd, eur_gbp):
        """
        Check for triangular arbitrage opportunity in forex
        eur_usd * gbp_usd should equal eur_gbp approximately
        """
        implied_cross = eur_usd / gbp_usd
        actual_cross = eur_gbp

        deviation = abs(implied_cross - actual_cross) / actual_cross

        if deviation > 0.001:  # > 0.1% deviation
            return {
                'opportunity': True,
                'deviation': deviation,
                'action': 'Arbitrage possible'
            }
        return {'opportunity': False}

    def day_trading_strategy(self, df: pd.DataFrame) -> List[Dict]:
        """
        ⚡ DAY TRADING STRATEGY - Moderate levels: 0.8% - 2.5% moves
        """
        signals = []
        if len(df) < 20:
            return signals
    
        latest = df.iloc[-1]
    
        # Calculate fast indicators for day trading
        df_copy = df.copy()
        df_copy['ema_9'] = df_copy['close'].ewm(span=9).mean()
        df_copy['ema_21'] = df_copy['close'].ewm(span=21).mean()
    
        # Fast RSI (7 period instead of 14)
        delta = df_copy['close'].diff()
        gain = delta.where(delta > 0, 0).rolling(7).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(7).mean()
        rs = gain / loss
        df_copy['rsi'] = 100 - (100 / (1 + rs))
    
        latest = df_copy.iloc[-1]
    
        # Day trading signals - MODERATE STOPS, REASONABLE TARGETS
        if latest['ema_9'] > latest['ema_21'] and latest['rsi'] < 60:
            # Bullish signal
            entry = latest['close']
            stop_loss = entry * 0.992      # 0.8% stop (wider)
            tp1 = entry * 1.008            # 0.8% profit
            tp2 = entry * 1.015            # 1.5% profit
            tp3 = entry * 1.025            # 2.5% profit
        
            confidence = min(0.5 + (60 - latest['rsi']) / 100, 0.8)
        
            signals.append({
                'date': df.index[-1],
                'signal': 'BUY',
                'confidence': confidence,
                'entry': entry,
                'stop_loss': stop_loss,
                'take_profit': tp1,
                'take_profit_levels': [
                    {'level': 1, 'price': tp1},
                    {'level': 2, 'price': tp2},
                    {'level': 3, 'price': tp3}
                ],
                'strategy': 'day_trading',
                'reason': f'Day trade BUY (RSI: {latest["rsi"]:.1f})',
                'expected_duration': '30-120 minutes'
            })
    
        elif latest['ema_9'] < latest['ema_21'] and latest['rsi'] > 40:
            # Bearish signal
            entry = latest['close']
            stop_loss = entry * 1.008       # 0.8% stop above
            tp1 = entry * 0.992             # 0.8% profit below
            tp2 = entry * 0.985             # 1.5% profit below
            tp3 = entry * 0.975             # 2.5% profit below
        
            confidence = min(0.5 + (latest['rsi'] - 40) / 100, 0.8)
        
            signals.append({
                'date': df.index[-1],
                'signal': 'SELL',
                'confidence': confidence,
                'entry': entry,
                'stop_loss': stop_loss,
                'take_profit': tp1,
                'take_profit_levels': [
                    {'level': 1, 'price': tp1},
                    {'level': 2, 'price': tp2},
                    {'level': 3, 'price': tp3}
                ],
                'strategy': 'day_trading',
                'reason': f'Day trade SELL (RSI: {latest["rsi"]:.1f})',
                'expected_duration': '30-120 minutes'
            })
    
        return signals

    def news_sentiment_strategy(self, df: pd.DataFrame) -> List[Dict]:
        """
        News sentiment strategy - provides signals based on market news
        This is a placeholder - actual signals come from voting_engine
        """
        # This is just a placeholder - the actual news sentiment
        # is handled in voting_engine.py's get_all_signals method
        return []

    def add_technical_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add all technical indicators"""
        try:
            from indicators.technical import TechnicalIndicators
            df = TechnicalIndicators.add_all_indicators(df)
        except Exception as e:
            logger.debug(f"Using simple indicators: {e}")
            df = self.add_simple_indicators(df)
        return df

    def add_simple_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add simple indicators"""
        # Moving averages
        df['sma_20'] = df['close'].rolling(20).mean()
        df['sma_50'] = df['close'].rolling(50).mean()
        df['ema_12'] = df['close'].ewm(span=12).mean()
        df['ema_26'] = df['close'].ewm(span=26).mean()
    
        # RSI
        delta = df['close'].diff()
        gain = delta.where(delta > 0, 0).rolling(14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
        rs = gain / loss
        df['rsi'] = 100 - (100 / (1 + rs))
    
        # MACD
        df['macd'] = df['ema_12'] - df['ema_26']
        df['macd_signal'] = df['macd'].ewm(span=9).mean()
        df['macd_hist'] = df['macd'] - df['macd_signal']
    
        # Bollinger Bands
        df['bb_middle'] = df['close'].rolling(20).mean()
        bb_std = df['close'].rolling(20).std()
        df['bb_upper'] = df['bb_middle'] + (bb_std * 2)
        df['bb_lower'] = df['bb_middle'] - (bb_std * 2)
    
        # Volume indicators
        if 'volume' in df.columns:
            df['volume_sma'] = df['volume'].rolling(20).mean()
            df['volume_ratio'] = df['volume'] / df['volume_sma']
    
        return df