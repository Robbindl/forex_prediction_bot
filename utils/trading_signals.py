"""
Trading Signal Generator
Calculates entry points, stop loss, take profit levels, and position sizing
"""

from utils.logger import logger
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any, Tuple
from enum import Enum


class SignalType(Enum):
    """Trading signal types"""
    BUY = "BUY"
    SELL = "SELL"
    HOLD = "HOLD"


class TradingSignalGenerator:
    """Generate complete trading signals with entry, SL, and TP"""
    
    @staticmethod
    def calculate_atr_stop_loss(df: pd.DataFrame, multiplier: float = 2.0) -> Tuple[float, float]:
        """
        Calculate ATR-based stop loss levels
        
        Args:
            df: DataFrame with price data and ATR
            multiplier: ATR multiplier for stop loss distance
            
        Returns:
            (buy_stop_loss, sell_stop_loss)
        """
        if 'atr' not in df.columns:
            return 0.0, 0.0
        
        current_price = df['close'].iloc[-1]
        atr = df['atr'].iloc[-1]
        
        buy_stop_loss = current_price - (atr * multiplier)
        sell_stop_loss = current_price + (atr * multiplier)
        
        return buy_stop_loss, sell_stop_loss
    
    @staticmethod
    def calculate_support_resistance_levels(df: pd.DataFrame, 
                                           window: int = 20) -> Dict[str, List[float]]:
        """
        Calculate dynamic support and resistance levels
        
        Args:
            df: DataFrame with price data
            window: Lookback window for finding levels
            
        Returns:
            Dict with support and resistance levels
        """
        # Find swing highs and lows
        df_copy = df.copy()
        df_copy['swing_high'] = df_copy['high'] == df_copy['high'].rolling(window, center=True).max()
        df_copy['swing_low'] = df_copy['low'] == df_copy['low'].rolling(window, center=True).min()
        
        # Get recent levels
        resistance_levels = df_copy[df_copy['swing_high']]['high'].tail(5).values.tolist()
        support_levels = df_copy[df_copy['swing_low']]['low'].tail(5).values.tolist()
        
        return {
            'resistance': sorted(resistance_levels, reverse=True),
            'support': sorted(support_levels, reverse=True)
        }
    
    @staticmethod
    def calculate_take_profit_levels(entry_price: float, stop_loss: float, 
                                    risk_reward_ratios: List[float] = [1.5, 2.0, 3.0],
                                    signal_type: SignalType = SignalType.BUY) -> List[Dict[str, float]]:
        """
        Calculate multiple take profit levels based on risk/reward ratios
        
        Args:
            entry_price: Entry price for the trade
            stop_loss: Stop loss price
            risk_reward_ratios: List of R:R ratios for TP levels
            signal_type: BUY or SELL signal
            
        Returns:
            List of TP levels with their R:R ratios
        """
        risk = abs(entry_price - stop_loss)
        tp_levels = []
        
        for rr_ratio in risk_reward_ratios:
            if signal_type == SignalType.BUY:
                tp_price = entry_price + (risk * rr_ratio)
            else:  # SELL
                tp_price = entry_price - (risk * rr_ratio)
            
            tp_levels.append({
                'price': tp_price,
                'risk_reward': rr_ratio,
                'potential_gain_pct': ((tp_price - entry_price) / entry_price * 100) if signal_type == SignalType.BUY 
                                     else ((entry_price - tp_price) / entry_price * 100)
            })
        
        return tp_levels
    
    @staticmethod
    def calculate_position_size(account_balance: float, risk_percentage: float,
                               entry_price: float, stop_loss: float,
                               pip_value: float = 1.0) -> Dict[str, float]:
        """
        Calculate position size based on risk management
        
        Args:
            account_balance: Total account balance
            risk_percentage: Percentage of account to risk (e.g., 1.0 for 1%)
            entry_price: Entry price
            stop_loss: Stop loss price
            pip_value: Value per pip (for forex)
            
        Returns:
            Position sizing information
        """
        risk_amount = account_balance * (risk_percentage / 100)
        price_difference = abs(entry_price - stop_loss)
        
        if price_difference == 0:
            return {
                'position_size': 0,
                'risk_amount': risk_amount,
                'contracts': 0
            }
        
        # For forex: position size = risk amount / (stop loss pips * pip value)
        position_size = risk_amount / price_difference
        
        return {
            'position_size': position_size,
            'risk_amount': risk_amount,
            'max_loss': risk_amount,
            'pips_at_risk': price_difference / pip_value if pip_value > 0 else 0
        }
    
    @staticmethod
    def generate_entry_signal(df: pd.DataFrame, 
                             prediction: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Generate complete trading signal with entry, SL, TP
        
        Args:
            df: DataFrame with technical indicators
            prediction: ML prediction (optional)
            
        Returns:
            Complete trading signal
        """
        current_price = df['close'].iloc[-1]
        
        # Determine signal type based on multiple indicators
        signal_type = TradingSignalGenerator._determine_signal_type(df, prediction)
        
        if signal_type == SignalType.HOLD:
            return {
                'signal': SignalType.HOLD.value,
                'entry_price': current_price,
                'stop_loss': None,
                'take_profit_levels': [],
                'confidence': 0.0,
                'reason': 'No clear signal'
            }
        
        # Calculate stop loss
        buy_sl, sell_sl = TradingSignalGenerator.calculate_atr_stop_loss(df)
        
        # Get support/resistance levels for better SL placement
        sr_levels = TradingSignalGenerator.calculate_support_resistance_levels(df)
        
        # Determine stop loss based on signal type
        if signal_type == SignalType.BUY:
            # Place SL below recent support or ATR-based
            if sr_levels['support']:
                nearest_support = max([s for s in sr_levels['support'] if s < current_price], default=buy_sl)
                stop_loss = min(nearest_support, buy_sl)  # Use tighter stop
            else:
                stop_loss = buy_sl
        else:  # SELL
            # Place SL above recent resistance or ATR-based
            if sr_levels['resistance']:
                nearest_resistance = min([r for r in sr_levels['resistance'] if r > current_price], default=sell_sl)
                stop_loss = max(nearest_resistance, sell_sl)  # Use tighter stop
            else:
                stop_loss = sell_sl
        
        # Calculate take profit levels
        tp_levels = TradingSignalGenerator.calculate_take_profit_levels(
            current_price, stop_loss, [1.5, 2.0, 3.0], signal_type
        )
        
        # Calculate confidence score
        confidence = TradingSignalGenerator._calculate_signal_confidence(df, prediction)
        
        # Generate entry reason
        reason = TradingSignalGenerator._generate_signal_reason(df, signal_type, prediction)
        
        # Calculate risk metrics
        risk_pct = abs((current_price - stop_loss) / current_price) * 100
        
        return {
            'signal': signal_type.value,
            'entry_price': current_price,
            'stop_loss': stop_loss,
            'take_profit_levels': tp_levels,
            'confidence': confidence,
            'reason': reason,
            'risk_pct': risk_pct,
            'support_levels': sr_levels['support'][:3],
            'resistance_levels': sr_levels['resistance'][:3],
            'risk_reward_ratios': [tp['risk_reward'] for tp in tp_levels]
        }
    
    @staticmethod
    def _determine_signal_type(df: pd.DataFrame, 
                               prediction: Optional[Dict[str, Any]] = None) -> SignalType:
        """Determine if signal is BUY, SELL, or HOLD"""
        bullish_signals = 0
        bearish_signals = 0
        total_signals = 0
        
        # RSI signal
        if 'rsi' in df.columns:
            rsi = df['rsi'].iloc[-1]
            total_signals += 1
            if rsi < 40:
                bullish_signals += 1
            elif rsi > 60:
                bearish_signals += 1
        
        # MACD signal
        if 'macd' in df.columns and 'macd_signal' in df.columns:
            macd = df['macd'].iloc[-1]
            macd_signal = df['macd_signal'].iloc[-1]
            total_signals += 1
            if macd > macd_signal and macd > 0:
                bullish_signals += 1
            elif macd < macd_signal and macd < 0:
                bearish_signals += 1
        
        # Moving average trend
        if 'sma_20' in df.columns and 'sma_50' in df.columns:
            sma_20 = df['sma_20'].iloc[-1]
            sma_50 = df['sma_50'].iloc[-1]
            price = df['close'].iloc[-1]
            total_signals += 1
            if sma_20 > sma_50 and price > sma_20:
                bullish_signals += 1
            elif sma_20 < sma_50 and price < sma_20:
                bearish_signals += 1
        
        # ADX trend strength
        if 'adx' in df.columns:
            adx = df['adx'].iloc[-1]
            if adx > 25:  # Strong trend, weight other signals more
                total_signals += 0.5  # Add half weight for confirmation
        
        # ML prediction signal (if available)
        if prediction and prediction.get('confidence', 0) > 0.6:
            total_signals += 1
            if prediction['direction'] == 'UP':
                bullish_signals += prediction['confidence']
            else:
                bearish_signals += prediction['confidence']
        
        # Determine signal
        if total_signals == 0:
            return SignalType.HOLD
        
        bullish_ratio = bullish_signals / total_signals
        bearish_ratio = bearish_signals / total_signals
        
        # Need strong agreement (60%+) for signal
        if bullish_ratio >= 0.6:
            return SignalType.BUY
        elif bearish_ratio >= 0.6:
            return SignalType.SELL
        else:
            return SignalType.HOLD
    
    @staticmethod
    def _calculate_signal_confidence(df: pd.DataFrame, 
                                     prediction: Optional[Dict[str, Any]] = None) -> float:
        """Calculate confidence score for the signal (0-1)"""
        confidence_factors = []
        
        # ADX strength
        if 'adx' in df.columns:
            adx = df['adx'].iloc[-1]
            adx_confidence = min(adx / 40, 1.0)  # Max confidence at ADX 40+
            confidence_factors.append(adx_confidence)
        
        # Volume confirmation
        if 'volume' in df.columns and df['volume'].sum() > 0:
            current_volume = df['volume'].iloc[-1]
            avg_volume = df['volume'].rolling(20).mean().iloc[-1]
            if avg_volume > 0:
                volume_ratio = min(current_volume / avg_volume, 2.0) / 2.0
                confidence_factors.append(volume_ratio)
        
        # ML prediction confidence
        if prediction and 'confidence' in prediction:
            confidence_factors.append(prediction['confidence'])
        
        # Bollinger Band position
        if 'bb_position' in df.columns:
            bb_pos = abs(df['bb_position'].iloc[-1])
            bb_confidence = 1.0 - min(bb_pos, 1.0)  # Higher confidence near middle
            confidence_factors.append(bb_confidence)
        
        # Average all confidence factors
        if confidence_factors:
            return sum(confidence_factors) / len(confidence_factors)
        return 0.5
    
    @staticmethod
    def _generate_signal_reason(df: pd.DataFrame, signal_type: SignalType,
                                prediction: Optional[Dict[str, Any]] = None) -> str:
        """Generate human-readable reason for the signal"""
        reasons = []
        
        if signal_type == SignalType.HOLD:
            return "Mixed signals - no clear trend"
        
        # RSI reason
        if 'rsi' in df.columns:
            rsi = df['rsi'].iloc[-1]
            if signal_type == SignalType.BUY and rsi < 40:
                reasons.append(f"RSI oversold ({rsi:.1f})")
            elif signal_type == SignalType.SELL and rsi > 60:
                reasons.append(f"RSI overbought ({rsi:.1f})")
        
        # MACD reason
        if 'macd' in df.columns and 'macd_signal' in df.columns:
            macd = df['macd'].iloc[-1]
            macd_signal = df['macd_signal'].iloc[-1]
            if signal_type == SignalType.BUY and macd > macd_signal:
                reasons.append("MACD bullish crossover")
            elif signal_type == SignalType.SELL and macd < macd_signal:
                reasons.append("MACD bearish crossover")
        
        # Trend reason
        if 'sma_20' in df.columns and 'sma_50' in df.columns:
            sma_20 = df['sma_20'].iloc[-1]
            sma_50 = df['sma_50'].iloc[-1]
            if signal_type == SignalType.BUY and sma_20 > sma_50:
                reasons.append("Bullish MA alignment")
            elif signal_type == SignalType.SELL and sma_20 < sma_50:
                reasons.append("Bearish MA alignment")
        
        # ML prediction
        if prediction and prediction.get('confidence', 0) > 0.6:
            reasons.append(f"ML predicts {prediction['direction']} ({prediction['confidence']:.0%})")
        
        # ADX strength
        if 'adx' in df.columns:
            adx = df['adx'].iloc[-1]
            if adx > 25:
                reasons.append(f"Strong trend (ADX {adx:.1f})")
        
        return " + ".join(reasons) if reasons else "Technical alignment"


# Example usage and testing
if __name__ == "__main__":
    import yfinance as yf
    import sys
    sys.path.append('..')
    from indicators.technical import TechnicalIndicators
    
    # Fetch sample data
    ticker = yf.Ticker("EURUSD=X")
    df = ticker.history(period="100d")
    df.columns = df.columns.str.lower()
    df = TechnicalIndicators.add_all_indicators(df)
    
    # Generate trading signal
    signal = TradingSignalGenerator.generate_entry_signal(df)
    
    logger.info(f"\n{'='*60}")
    logger.info("TRADING SIGNAL ANALYSIS")
    logger.info(f"{'='*60}")
    logger.info(f"Signal: {signal['signal']}")
    logger.info(f"Entry Price: {signal['entry_price']:.5f}")
    logger.info(f"Stop Loss: {signal['stop_loss']:.5f}")
    logger.info(f"Risk: {signal['risk_pct']:.2f}%")
    logger.info(f"Confidence: {signal['confidence']:.0%}")
    logger.info(f"Reason: {signal['reason']}")
    
    logger.info(f"\nTake Profit Levels:")
    for i, tp in enumerate(signal['take_profit_levels'], 1):
        logger.info(f"  TP{i}: {tp['price']:.5f} (R:R {tp['risk_reward']}:1, +{tp['potential_gain_pct']:.2f}%)")
    
    logger.info(f"\nSupport Levels: {[f'{s:.5f}' for s in signal['support_levels'][:3]]}")
    logger.info(f"Resistance Levels: {[f'{r:.5f}' for r in signal['resistance_levels'][:3]]}")
    
    # Calculate position size example
    position = TradingSignalGenerator.calculate_position_size(
        account_balance=10000,
        risk_percentage=1.0,
        entry_price=signal['entry_price'],
        stop_loss=signal['stop_loss']
    )
    
    logger.info(f"\nPosition Sizing (1% risk on $10,000):")
    logger.info(f"  Position Size: {position['position_size']:.2f} units")
    logger.info(f"  Max Loss: ${position['max_loss']:.2f}")
    logger.info(f"{'='*60}\n")