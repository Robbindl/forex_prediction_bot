"""
Technical Indicators for Market Analysis
Includes: Trend, Momentum, Volatility, Volume indicators
"""

import pandas as pd
import numpy as np
from typing import Tuple
from logger import logger


class TechnicalIndicators:
    """Calculate various technical indicators"""
    
    @staticmethod
    def add_sma(df: pd.DataFrame, periods: list = [20, 50, 200]) -> pd.DataFrame:
        """Simple Moving Average"""
        for period in periods:
            df[f'sma_{period}'] = df['close'].rolling(window=period).mean()
        return df
    
    @staticmethod
    def add_ema(df: pd.DataFrame, periods: list = [12, 26, 50]) -> pd.DataFrame:
        """Exponential Moving Average"""
        for period in periods:
            df[f'ema_{period}'] = df['close'].ewm(span=period, adjust=False).mean()
        return df
    
    @staticmethod
    def add_rsi(df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
        """Relative Strength Index"""
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        
        rs = gain / loss
        df['rsi'] = 100 - (100 / (1 + rs))
        return df
    
    @staticmethod
    def add_macd(df: pd.DataFrame, fast: int = 12, slow: int = 26, 
                 signal: int = 9) -> pd.DataFrame:
        """MACD (Moving Average Convergence Divergence)"""
        ema_fast = df['close'].ewm(span=fast, adjust=False).mean()
        ema_slow = df['close'].ewm(span=slow, adjust=False).mean()
        
        df['macd'] = ema_fast - ema_slow
        df['macd_signal'] = df['macd'].ewm(span=signal, adjust=False).mean()
        df['macd_hist'] = df['macd'] - df['macd_signal']
        return df
    
    @staticmethod
    def add_bollinger_bands(df: pd.DataFrame, period: int = 20, 
                           std_dev: int = 2) -> pd.DataFrame:
        """Bollinger Bands"""
        df['bb_middle'] = df['close'].rolling(window=period).mean()
        bb_std = df['close'].rolling(window=period).std()
        
        df['bb_upper'] = df['bb_middle'] + (bb_std * std_dev)
        df['bb_lower'] = df['bb_middle'] - (bb_std * std_dev)
        df['bb_width'] = df['bb_upper'] - df['bb_lower']
        df['bb_position'] = (df['close'] - df['bb_lower']) / df['bb_width']
        return df
    
    @staticmethod
    def add_stochastic(df: pd.DataFrame, period: int = 14, 
                      smooth_k: int = 3, smooth_d: int = 3) -> pd.DataFrame:
        """Stochastic Oscillator"""
        low_min = df['low'].rolling(window=period).min()
        high_max = df['high'].rolling(window=period).max()
        
        df['stoch_k'] = 100 * (df['close'] - low_min) / (high_max - low_min)
        df['stoch_k'] = df['stoch_k'].rolling(window=smooth_k).mean()
        df['stoch_d'] = df['stoch_k'].rolling(window=smooth_d).mean()
        return df
    
    @staticmethod
    def add_atr(df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
        """Average True Range (Volatility)"""
        high_low = df['high'] - df['low']
        high_close = np.abs(df['high'] - df['close'].shift())
        low_close = np.abs(df['low'] - df['close'].shift())
        
        ranges = pd.concat([high_low, high_close, low_close], axis=1)
        true_range = np.max(ranges, axis=1)
        df['atr'] = true_range.rolling(period).mean()
        return df
    
    @staticmethod
    def add_adx(df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
        """Average Directional Index (Trend Strength)"""
        high_diff = df['high'].diff()
        low_diff = -df['low'].diff()
        
        pos_dm = high_diff.where((high_diff > low_diff) & (high_diff > 0), 0)
        neg_dm = low_diff.where((low_diff > high_diff) & (low_diff > 0), 0)
        
        # Calculate ATR
        high_low = df['high'] - df['low']
        high_close = np.abs(df['high'] - df['close'].shift())
        low_close = np.abs(df['low'] - df['close'].shift())
        ranges = pd.concat([high_low, high_close, low_close], axis=1)
        atr = np.max(ranges, axis=1).rolling(period).mean()
        
        pos_di = 100 * (pos_dm.rolling(period).mean() / atr)
        neg_di = 100 * (neg_dm.rolling(period).mean() / atr)
        
        dx = 100 * np.abs(pos_di - neg_di) / (pos_di + neg_di)
        df['adx'] = dx.rolling(period).mean()
        df['di_plus'] = pos_di
        df['di_minus'] = neg_di
        return df
    
    @staticmethod
    def add_cci(df: pd.DataFrame, period: int = 20) -> pd.DataFrame:
        """Commodity Channel Index"""
        tp = (df['high'] + df['low'] + df['close']) / 3
        sma_tp = tp.rolling(window=period).mean()
        mad = tp.rolling(window=period).apply(lambda x: np.abs(x - x.mean()).mean())
        
        df['cci'] = (tp - sma_tp) / (0.015 * mad)
        return df
    
    @staticmethod
    def add_obv(df: pd.DataFrame) -> pd.DataFrame:
        """On-Balance Volume"""
        obv = [0]
        for i in range(1, len(df)):
            if df['close'].iloc[i] > df['close'].iloc[i-1]:
                obv.append(obv[-1] + df['volume'].iloc[i])
            elif df['close'].iloc[i] < df['close'].iloc[i-1]:
                obv.append(obv[-1] - df['volume'].iloc[i])
            else:
                obv.append(obv[-1])
        
        df['obv'] = obv
        return df
    
    @staticmethod
    def add_fibonacci_levels(df: pd.DataFrame, lookback: int = 50) -> pd.DataFrame:
        """Fibonacci Retracement Levels"""
        high = df['high'].rolling(window=lookback).max()
        low = df['low'].rolling(window=lookback).min()
        diff = high - low
        
        df['fib_0'] = high
        df['fib_236'] = high - 0.236 * diff
        df['fib_382'] = high - 0.382 * diff
        df['fib_500'] = high - 0.500 * diff
        df['fib_618'] = high - 0.618 * diff
        df['fib_1'] = low
        return df
    
    @staticmethod
    def add_pivot_points(df: pd.DataFrame) -> pd.DataFrame:
        """Pivot Points (Daily)"""
        df['pivot'] = (df['high'].shift(1) + df['low'].shift(1) + df['close'].shift(1)) / 3
        df['r1'] = 2 * df['pivot'] - df['low'].shift(1)
        df['s1'] = 2 * df['pivot'] - df['high'].shift(1)
        df['r2'] = df['pivot'] + (df['high'].shift(1) - df['low'].shift(1))
        df['s2'] = df['pivot'] - (df['high'].shift(1) - df['low'].shift(1))
        return df
    
    @staticmethod
    def add_ichimoku(df: pd.DataFrame) -> pd.DataFrame:
        """Ichimoku Cloud"""
        # Tenkan-sen (Conversion Line)
        high_9 = df['high'].rolling(window=9).max()
        low_9 = df['low'].rolling(window=9).min()
        df['tenkan_sen'] = (high_9 + low_9) / 2
        
        # Kijun-sen (Base Line)
        high_26 = df['high'].rolling(window=26).max()
        low_26 = df['low'].rolling(window=26).min()
        df['kijun_sen'] = (high_26 + low_26) / 2
        
        # Senkou Span A (Leading Span A)
        df['senkou_span_a'] = ((df['tenkan_sen'] + df['kijun_sen']) / 2).shift(26)
        
        # Senkou Span B (Leading Span B)
        high_52 = df['high'].rolling(window=52).max()
        low_52 = df['low'].rolling(window=52).min()
        df['senkou_span_b'] = ((high_52 + low_52) / 2).shift(26)
        
        # Chikou Span (Lagging Span)
        df['chikou_span'] = df['close'].shift(-26)
        
        return df
    
    @staticmethod
    def add_all_indicators(df: pd.DataFrame) -> pd.DataFrame:
        """Add all technical indicators"""
        df = TechnicalIndicators.add_sma(df)
        df = TechnicalIndicators.add_ema(df)
        df = TechnicalIndicators.add_rsi(df)
        df = TechnicalIndicators.add_macd(df)
        df = TechnicalIndicators.add_bollinger_bands(df)
        df = TechnicalIndicators.add_stochastic(df)
        df = TechnicalIndicators.add_atr(df)
        df = TechnicalIndicators.add_adx(df)
        df = TechnicalIndicators.add_cci(df)
        
        if 'volume' in df.columns and df['volume'].sum() > 0:
            df = TechnicalIndicators.add_obv(df)
        
        df = TechnicalIndicators.add_fibonacci_levels(df)
        df = TechnicalIndicators.add_pivot_points(df)
        df = TechnicalIndicators.add_ichimoku(df)
        
        return df
    
    @staticmethod
    def detect_patterns(df: pd.DataFrame) -> pd.DataFrame:
        """Detect candlestick patterns"""
        # Doji
        body = abs(df['close'] - df['open'])
        range_val = df['high'] - df['low']
        df['doji'] = body < (range_val * 0.1)
        
        # Hammer
        lower_wick = df[['open', 'close']].min(axis=1) - df['low']
        upper_wick = df['high'] - df[['open', 'close']].max(axis=1)
        df['hammer'] = (lower_wick > 2 * body) & (upper_wick < body)
        
        # Engulfing
        df['bullish_engulfing'] = (
            (df['close'] > df['open']) &
            (df['close'].shift(1) < df['open'].shift(1)) &
            (df['open'] < df['close'].shift(1)) &
            (df['close'] > df['open'].shift(1))
        )
        
        df['bearish_engulfing'] = (
            (df['close'] < df['open']) &
            (df['close'].shift(1) > df['open'].shift(1)) &
            (df['open'] > df['close'].shift(1)) &
            (df['close'] < df['open'].shift(1))
        )
        
        return df


if __name__ == "__main__":
    # Test indicators
    import yfinance as yf
    
    ticker = yf.Ticker("EURUSD=X")
    df = ticker.history(period="100d")
    df.columns = df.columns.str.lower()
    
    df = TechnicalIndicators.add_all_indicators(df)
    logger.info(df[['close', 'rsi', 'macd', 'bb_upper', 'bb_lower']].tail())