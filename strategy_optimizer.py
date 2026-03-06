"""
Strategy Optimizer - Grid search optimization for trading strategies
"""

import itertools
import pandas as pd
import numpy as np
from typing import Dict, List, Any, Callable, Optional
from concurrent.futures import ProcessPoolExecutor, as_completed
import json
from datetime import datetime
import os

class StrategyOptimizer:
    """
    Grid search optimizer for trading strategies
    """
    
    def __init__(self, backtester, results_dir: str = "optimization_results"):
        self.backtester = backtester
        self.results_history = []
        self.results_dir = results_dir
        
        # Create results directory if it doesn't exist
        os.makedirs(results_dir, exist_ok=True)
    
    # ============= EXISTING OPTIMIZATION METHODS =============
    
    def optimize_rsi(self, df: pd.DataFrame, asset: str) -> Dict:
        """
        Optimize RSI strategy parameters
        """
        param_grid = {
            'oversold': [20, 25, 30, 35],
            'overbought': [65, 70, 75, 80],
            'period': [7, 14, 21]
        }
        
        return self.grid_search(
            strategy_func=self._test_rsi_params,
            param_grid=param_grid,
            df=df,
            asset=asset,
            strategy_name="RSI"
        )
    
    def optimize_macd(self, df: pd.DataFrame, asset: str) -> Dict:
        """
        Optimize MACD strategy parameters
        """
        param_grid = {
            'fast': [8, 12, 16],
            'slow': [20, 26, 32],
            'signal': [7, 9, 11]
        }
        
        return self.grid_search(
            strategy_func=self._test_macd_params,
            param_grid=param_grid,
            df=df,
            asset=asset,
            strategy_name="MACD"
        )
    
    def optimize_bollinger(self, df: pd.DataFrame, asset: str) -> Dict:
        """
        Optimize Bollinger Bands strategy parameters
        """
        param_grid = {
            'period': [10, 20, 30],
            'std_dev': [1.5, 2.0, 2.5],
            'exit_std': [2.5, 3.0, 3.5]
        }
        
        return self.grid_search(
            strategy_func=self._test_bollinger_params,
            param_grid=param_grid,
            df=df,
            asset=asset,
            strategy_name="Bollinger"
        )
    
    def optimize_ma_cross(self, df: pd.DataFrame, asset: str) -> Dict:
        """
        Optimize Moving Average Crossover parameters
        """
        param_grid = {
            'fast_period': [5, 10, 15, 20],
            'slow_period': [20, 30, 50, 100],
            'use_ema': [True, False]
        }
        
        return self.grid_search(
            strategy_func=self._test_ma_cross_params,
            param_grid=param_grid,
            df=df,
            asset=asset,
            strategy_name="MA_Cross"
        )
    
    # ============= NEW OPTIMIZATION METHODS (50+ STRATEGIES) =============
    
    def optimize_rsi_divergence(self, df: pd.DataFrame, asset: str) -> Dict:
        """Optimize RSI Divergence parameters"""
        param_grid = {
            'rsi_period': [7, 14, 21],
            'lookback': [5, 10, 20],
            'divergence_threshold': [0.01, 0.02, 0.03]
        }
        
        return self.grid_search(
            strategy_func=self._test_rsi_divergence_params,
            param_grid=param_grid,
            df=df,
            asset=asset,
            strategy_name="RSI_Divergence"
        )
    
    def optimize_stoch_rsi(self, df: pd.DataFrame, asset: str) -> Dict:
        """Optimize Stochastic RSI parameters"""
        param_grid = {
            'rsi_period': [7, 14, 21],
            'stoch_period': [5, 10, 14],
            'k_period': [3, 5, 7],
            'd_period': [3, 5, 7]
        }
        
        return self.grid_search(
            strategy_func=self._test_stoch_rsi_params,
            param_grid=param_grid,
            df=df,
            asset=asset,
            strategy_name="Stoch_RSI"
        )
    
    def optimize_stochastic(self, df: pd.DataFrame, asset: str) -> Dict:
        """Optimize Stochastic Oscillator parameters"""
        param_grid = {
            'k_period': [5, 10, 14, 20],
            'd_period': [3, 5, 7],
            'oversold': [15, 20, 25],
            'overbought': [75, 80, 85]
        }
        
        return self.grid_search(
            strategy_func=self._test_stochastic_params,
            param_grid=param_grid,
            df=df,
            asset=asset,
            strategy_name="Stochastic"
        )
    
    def optimize_stochastic_fast(self, df: pd.DataFrame, asset: str) -> Dict:
        """Optimize Fast Stochastic parameters"""
        param_grid = {
            'k_period': [5, 8, 10, 14],
            'd_period': [3, 5],
            'oversold': [15, 20, 25],
            'overbought': [75, 80, 85]
        }
        
        return self.grid_search(
            strategy_func=self._test_stochastic_fast_params,
            param_grid=param_grid,
            df=df,
            asset=asset,
            strategy_name="Stochastic_Fast"
        )
    
    def optimize_stochastic_full(self, df: pd.DataFrame, asset: str) -> Dict:
        """Optimize Full Stochastic parameters"""
        param_grid = {
            'k_period': [5, 10, 14, 20],
            'd_period': [3, 5, 7],
            'slowing': [3, 5, 7],
            'oversold': [15, 20, 25],
            'overbought': [75, 80, 85]
        }
        
        return self.grid_search(
            strategy_func=self._test_stochastic_full_params,
            param_grid=param_grid,
            df=df,
            asset=asset,
            strategy_name="Stochastic_Full"
        )
    
    def optimize_macd_histogram(self, df: pd.DataFrame, asset: str) -> Dict:
        """Optimize MACD Histogram parameters"""
        param_grid = {
            'fast': [8, 12, 16],
            'slow': [20, 26, 32],
            'signal': [7, 9, 11],
            'histogram_threshold': [0, 0.5, 1, 2]
        }
        
        return self.grid_search(
            strategy_func=self._test_macd_histogram_params,
            param_grid=param_grid,
            df=df,
            asset=asset,
            strategy_name="MACD_Histogram"
        )
    
    def optimize_macd_divergence(self, df: pd.DataFrame, asset: str) -> Dict:
        """Optimize MACD Divergence parameters"""
        param_grid = {
            'fast': [8, 12, 16],
            'slow': [20, 26, 32],
            'signal': [7, 9, 11],
            'lookback': [5, 10, 20],
            'divergence_threshold': [0.01, 0.02, 0.03]
        }
        
        return self.grid_search(
            strategy_func=self._test_macd_divergence_params,
            param_grid=param_grid,
            df=df,
            asset=asset,
            strategy_name="MACD_Divergence"
        )
    
    def optimize_cci(self, df: pd.DataFrame, asset: str) -> Dict:
        """Optimize Commodity Channel Index parameters"""
        param_grid = {
            'period': [10, 14, 20, 30],
            'oversold': [-100, -150, -200],
            'overbought': [100, 150, 200]
        }
        
        return self.grid_search(
            strategy_func=self._test_cci_params,
            param_grid=param_grid,
            df=df,
            asset=asset,
            strategy_name="CCI"
        )
    
    def optimize_williams_r(self, df: pd.DataFrame, asset: str) -> Dict:
        """Optimize Williams %R parameters"""
        param_grid = {
            'period': [7, 10, 14, 20],
            'oversold': [-70, -80, -90],
            'overbought': [-10, -20, -30]
        }
        
        return self.grid_search(
            strategy_func=self._test_williams_r_params,
            param_grid=param_grid,
            df=df,
            asset=asset,
            strategy_name="Williams_R"
        )
    
    def optimize_mfi(self, df: pd.DataFrame, asset: str) -> Dict:
        """Optimize Money Flow Index parameters"""
        param_grid = {
            'period': [7, 10, 14, 20],
            'oversold': [15, 20, 25],
            'overbought': [75, 80, 85]
        }
        
        return self.grid_search(
            strategy_func=self._test_mfi_params,
            param_grid=param_grid,
            df=df,
            asset=asset,
            strategy_name="MFI"
        )
    
    def optimize_uo(self, df: pd.DataFrame, asset: str) -> Dict:
        """Optimize Ultimate Oscillator parameters"""
        param_grid = {
            'period1': [5, 7, 10],
            'period2': [10, 14, 20],
            'period3': [20, 28, 40],
            'oversold': [25, 30, 35],
            'overbought': [65, 70, 75]
        }
        
        return self.grid_search(
            strategy_func=self._test_uo_params,
            param_grid=param_grid,
            df=df,
            asset=asset,
            strategy_name="Ultimate_Oscillator"
        )
    
    def optimize_apo(self, df: pd.DataFrame, asset: str) -> Dict:
        """Optimize Absolute Price Oscillator parameters"""
        param_grid = {
            'fast': [5, 8, 12],
            'slow': [13, 21, 26],
            'signal': [3, 5, 7]
        }
        
        return self.grid_search(
            strategy_func=self._test_apo_params,
            param_grid=param_grid,
            df=df,
            asset=asset,
            strategy_name="APO"
        )
    
    def optimize_ppo(self, df: pd.DataFrame, asset: str) -> Dict:
        """Optimize Percentage Price Oscillator parameters"""
        param_grid = {
            'fast': [8, 12, 16],
            'slow': [20, 26, 32],
            'signal': [7, 9, 11]
        }
        
        return self.grid_search(
            strategy_func=self._test_ppo_params,
            param_grid=param_grid,
            df=df,
            asset=asset,
            strategy_name="PPO"
        )
    
    def optimize_sma_cross(self, df: pd.DataFrame, asset: str) -> Dict:
        """Optimize SMA Crossover parameters"""
        param_grid = {
            'fast_period': [5, 10, 15, 20],
            'slow_period': [20, 30, 50, 100, 200]
        }
        
        return self.grid_search(
            strategy_func=self._test_sma_cross_params,
            param_grid=param_grid,
            df=df,
            asset=asset,
            strategy_name="SMA_Cross"
        )
    
    def optimize_ema_cross(self, df: pd.DataFrame, asset: str) -> Dict:
        """Optimize EMA Crossover parameters"""
        param_grid = {
            'fast_period': [5, 8, 10, 12, 15],
            'slow_period': [20, 21, 26, 30, 50]
        }
        
        return self.grid_search(
            strategy_func=self._test_ema_cross_params,
            param_grid=param_grid,
            df=df,
            asset=asset,
            strategy_name="EMA_Cross"
        )
    
    def optimize_wma_cross(self, df: pd.DataFrame, asset: str) -> Dict:
        """Optimize WMA Crossover parameters"""
        param_grid = {
            'fast_period': [5, 10, 15],
            'slow_period': [20, 30, 50]
        }
        
        return self.grid_search(
            strategy_func=self._test_wma_cross_params,
            param_grid=param_grid,
            df=df,
            asset=asset,
            strategy_name="WMA_Cross"
        )
    
    def optimize_hma_cross(self, df: pd.DataFrame, asset: str) -> Dict:
        """Optimize Hull MA Crossover parameters"""
        param_grid = {
            'fast_period': [5, 8, 12],
            'slow_period': [15, 20, 30]
        }
        
        return self.grid_search(
            strategy_func=self._test_hma_cross_params,
            param_grid=param_grid,
            df=df,
            asset=asset,
            strategy_name="HMA_Cross"
        )
    
    def optimize_vwap(self, df: pd.DataFrame, asset: str) -> Dict:
        """Optimize VWAP parameters"""
        param_grid = {
            'period': [5, 10, 20],
            'std_dev': [1, 1.5, 2, 2.5]
        }
        
        return self.grid_search(
            strategy_func=self._test_vwap_params,
            param_grid=param_grid,
            df=df,
            asset=asset,
            strategy_name="VWAP"
        )
    
    def optimize_adx(self, df: pd.DataFrame, asset: str) -> Dict:
        """Optimize ADX parameters"""
        param_grid = {
            'period': [7, 10, 14, 20],
            'threshold': [20, 25, 30, 35]
        }
        
        return self.grid_search(
            strategy_func=self._test_adx_params,
            param_grid=param_grid,
            df=df,
            asset=asset,
            strategy_name="ADX"
        )
    
    def optimize_di_plus(self, df: pd.DataFrame, asset: str) -> Dict:
        """Optimize +DI parameters"""
        param_grid = {
            'period': [7, 10, 14, 20],
            'threshold': [20, 25, 30]
        }
        
        return self.grid_search(
            strategy_func=self._test_di_plus_params,
            param_grid=param_grid,
            df=df,
            asset=asset,
            strategy_name="DI_Plus"
        )
    
    def optimize_di_minus(self, df: pd.DataFrame, asset: str) -> Dict:
        """Optimize -DI parameters"""
        param_grid = {
            'period': [7, 10, 14, 20],
            'threshold': [20, 25, 30]
        }
        
        return self.grid_search(
            strategy_func=self._test_di_minus_params,
            param_grid=param_grid,
            df=df,
            asset=asset,
            strategy_name="DI_Minus"
        )
    
    def optimize_adx_cross(self, df: pd.DataFrame, asset: str) -> Dict:
        """Optimize ADX Cross parameters"""
        param_grid = {
            'period': [7, 10, 14, 20],
            'adx_threshold': [20, 25, 30]
        }
        
        return self.grid_search(
            strategy_func=self._test_adx_cross_params,
            param_grid=param_grid,
            df=df,
            asset=asset,
            strategy_name="ADX_Cross"
        )
    
    def optimize_ichimoku(self, df: pd.DataFrame, asset: str) -> Dict:
        """Optimize Ichimoku Cloud parameters"""
        param_grid = {
            'tenkan': [5, 7, 9],
            'kijun': [20, 22, 26],
            'senkou': [45, 52, 60]
        }
        
        return self.grid_search(
            strategy_func=self._test_ichimoku_params,
            param_grid=param_grid,
            df=df,
            asset=asset,
            strategy_name="Ichimoku"
        )
    
    def optimize_ichimoku_tenkan(self, df: pd.DataFrame, asset: str) -> Dict:
        """Optimize Ichimoku Tenkan-sen parameters"""
        param_grid = {
            'tenkan_period': [5, 7, 9, 11],
            'kijun_period': [20, 26, 30]
        }
        
        return self.grid_search(
            strategy_func=self._test_ichimoku_tenkan_params,
            param_grid=param_grid,
            df=df,
            asset=asset,
            strategy_name="Ichimoku_Tenkan"
        )
    
    def optimize_ichimoku_kijun(self, df: pd.DataFrame, asset: str) -> Dict:
        """Optimize Ichimoku Kijun-sen parameters"""
        param_grid = {
            'tenkan_period': [9],
            'kijun_period': [20, 22, 24, 26, 30]
        }
        
        return self.grid_search(
            strategy_func=self._test_ichimoku_kijun_params,
            param_grid=param_grid,
            df=df,
            asset=asset,
            strategy_name="Ichimoku_Kijun"
        )
    
    def optimize_ichimoku_cross(self, df: pd.DataFrame, asset: str) -> Dict:
        """Optimize Ichimoku Cross parameters"""
        param_grid = {
            'tenkan': [5, 7, 9],
            'kijun': [20, 22, 26],
            'cloud_offset': [20, 26, 30]
        }
        
        return self.grid_search(
            strategy_func=self._test_ichimoku_cross_params,
            param_grid=param_grid,
            df=df,
            asset=asset,
            strategy_name="Ichimoku_Cross"
        )
    
    def optimize_psar(self, df: pd.DataFrame, asset: str) -> Dict:
        """Optimize Parabolic SAR parameters"""
        param_grid = {
            'acceleration': [0.01, 0.015, 0.02],
            'max_acceleration': [0.1, 0.15, 0.2]
        }
        
        return self.grid_search(
            strategy_func=self._test_psar_params,
            param_grid=param_grid,
            df=df,
            asset=asset,
            strategy_name="PSAR"
        )
    
    def optimize_bollinger_breakout(self, df: pd.DataFrame, asset: str) -> Dict:
        """Optimize Bollinger Breakout parameters"""
        param_grid = {
            'period': [10, 20, 30],
            'std_dev': [2.0, 2.5, 3.0],
            'confirmation_bars': [1, 2, 3]
        }
        
        return self.grid_search(
            strategy_func=self._test_bollinger_breakout_params,
            param_grid=param_grid,
            df=df,
            asset=asset,
            strategy_name="Bollinger_Breakout"
        )
    
    def optimize_bollinger_squeeze(self, df: pd.DataFrame, asset: str) -> Dict:
        """Optimize Bollinger Squeeze parameters"""
        param_grid = {
            'period': [20],
            'bb_std': [2.0],
            'kc_std': [1.5],
            'squeeze_period': [3, 5, 7]
        }
        
        return self.grid_search(
            strategy_func=self._test_bollinger_squeeze_params,
            param_grid=param_grid,
            df=df,
            asset=asset,
            strategy_name="Bollinger_Squeeze"
        )
    
    def optimize_bollinger_width(self, df: pd.DataFrame, asset: str) -> Dict:
        """Optimize Bollinger Width parameters"""
        param_grid = {
            'period': [20],
            'width_threshold': [0.02, 0.03, 0.04, 0.05]
        }
        
        return self.grid_search(
            strategy_func=self._test_bollinger_width_params,
            param_grid=param_grid,
            df=df,
            asset=asset,
            strategy_name="Bollinger_Width"
        )
    
    def optimize_keltner(self, df: pd.DataFrame, asset: str) -> Dict:
        """Optimize Keltner Channels parameters"""
        param_grid = {
            'ema_period': [10, 20],
            'atr_period': [10, 14, 20],
            'atr_multiplier': [1.5, 2.0, 2.5]
        }
        
        return self.grid_search(
            strategy_func=self._test_keltner_params,
            param_grid=param_grid,
            df=df,
            asset=asset,
            strategy_name="Keltner"
        )
    
    def optimize_keltner_breakout(self, df: pd.DataFrame, asset: str) -> Dict:
        """Optimize Keltner Breakout parameters"""
        param_grid = {
            'ema_period': [10, 20],
            'atr_period': [10, 14, 20],
            'atr_multiplier': [2.0, 2.5, 3.0],
            'confirmation_bars': [1, 2]
        }
        
        return self.grid_search(
            strategy_func=self._test_keltner_breakout_params,
            param_grid=param_grid,
            df=df,
            asset=asset,
            strategy_name="Keltner_Breakout"
        )
    
    def optimize_atr(self, df: pd.DataFrame, asset: str) -> Dict:
        """Optimize ATR parameters"""
        param_grid = {
            'period': [7, 10, 14, 20, 30]
        }
        
        return self.grid_search(
            strategy_func=self._test_atr_params,
            param_grid=param_grid,
            df=df,
            asset=asset,
            strategy_name="ATR"
        )
    
    def optimize_atr_trailing(self, df: pd.DataFrame, asset: str) -> Dict:
        """Optimize ATR Trailing Stop parameters"""
        param_grid = {
            'period': [7, 10, 14, 20],
            'multiplier': [1.5, 2.0, 2.5, 3.0]
        }
        
        return self.grid_search(
            strategy_func=self._test_atr_trailing_params,
            param_grid=param_grid,
            df=df,
            asset=asset,
            strategy_name="ATR_Trailing"
        )
    
    def optimize_atr_bands(self, df: pd.DataFrame, asset: str) -> Dict:
        """Optimize ATR Bands parameters"""
        param_grid = {
            'period': [10, 14, 20],
            'multiplier': [1.5, 2.0, 2.5],
            'band_type': ['upper', 'lower', 'both']
        }
        
        return self.grid_search(
            strategy_func=self._test_atr_bands_params,
            param_grid=param_grid,
            df=df,
            asset=asset,
            strategy_name="ATR_Bands"
        )
    
    def optimize_donchian(self, df: pd.DataFrame, asset: str) -> Dict:
        """Optimize Donchian Channels parameters"""
        param_grid = {
            'period': [10, 20, 30, 50]
        }
        
        return self.grid_search(
            strategy_func=self._test_donchian_params,
            param_grid=param_grid,
            df=df,
            asset=asset,
            strategy_name="Donchian"
        )
    
    def optimize_donchian_breakout(self, df: pd.DataFrame, asset: str) -> Dict:
        """Optimize Donchian Breakout parameters"""
        param_grid = {
            'period': [10, 20, 30, 50],
            'confirmation_bars': [1, 2, 3]
        }
        
        return self.grid_search(
            strategy_func=self._test_donchian_breakout_params,
            param_grid=param_grid,
            df=df,
            asset=asset,
            strategy_name="Donchian_Breakout"
        )
    
    def optimize_volatility_ratio(self, df: pd.DataFrame, asset: str) -> Dict:
        """Optimize Volatility Ratio parameters"""
        param_grid = {
            'period': [10, 20],
            'ratio_threshold': [0.5, 0.7, 0.8, 0.9]
        }
        
        return self.grid_search(
            strategy_func=self._test_volatility_ratio_params,
            param_grid=param_grid,
            df=df,
            asset=asset,
            strategy_name="Volatility_Ratio"
        )
    
    def optimize_chaikin_volatility(self, df: pd.DataFrame, asset: str) -> Dict:
        """Optimize Chaikin Volatility parameters"""
        param_grid = {
            'period': [5, 10, 14],
            'roc_period': [5, 10],
            'threshold': [5, 10, 15]
        }
        
        return self.grid_search(
            strategy_func=self._test_chaikin_volatility_params,
            param_grid=param_grid,
            df=df,
            asset=asset,
            strategy_name="Chaikin_Volatility"
        )
    
    def optimize_obv(self, df: pd.DataFrame, asset: str) -> Dict:
        """Optimize On-Balance Volume parameters"""
        param_grid = {
            'signal_period': [3, 5, 7, 10]
        }
        
        return self.grid_search(
            strategy_func=self._test_obv_params,
            param_grid=param_grid,
            df=df,
            asset=asset,
            strategy_name="OBV"
        )
    
    def optimize_obv_divergence(self, df: pd.DataFrame, asset: str) -> Dict:
        """Optimize OBV Divergence parameters"""
        param_grid = {
            'lookback': [5, 10, 20],
            'divergence_threshold': [0.01, 0.02, 0.03]
        }
        
        return self.grid_search(
            strategy_func=self._test_obv_divergence_params,
            param_grid=param_grid,
            df=df,
            asset=asset,
            strategy_name="OBV_Divergence"
        )
    
    def optimize_volume_profile(self, df: pd.DataFrame, asset: str) -> Dict:
        """Optimize Volume Profile parameters"""
        param_grid = {
            'num_bins': [10, 20, 30],
            'value_area_pct': [0.68, 0.70, 0.75]
        }
        
        return self.grid_search(
            strategy_func=self._test_volume_profile_params,
            param_grid=param_grid,
            df=df,
            asset=asset,
            strategy_name="Volume_Profile"
        )
    
    def optimize_volume_oscillator(self, df: pd.DataFrame, asset: str) -> Dict:
        """Optimize Volume Oscillator parameters"""
        param_grid = {
            'fast_period': [3, 5, 7],
            'slow_period': [10, 14, 20],
            'threshold': [0, 5, 10]
        }
        
        return self.grid_search(
            strategy_func=self._test_volume_oscillator_params,
            param_grid=param_grid,
            df=df,
            asset=asset,
            strategy_name="Volume_Oscillator"
        )
    
    def optimize_vwap_volume(self, df: pd.DataFrame, asset: str) -> Dict:
        """Optimize VWAP with Volume parameters"""
        param_grid = {
            'period': [5, 10, 20],
            'std_dev': [1, 1.5, 2],
            'volume_threshold': [1.2, 1.5, 2.0]
        }
        
        return self.grid_search(
            strategy_func=self._test_vwap_volume_params,
            param_grid=param_grid,
            df=df,
            asset=asset,
            strategy_name="VWAP_Volume"
        )
    
    def optimize_cmf(self, df: pd.DataFrame, asset: str) -> Dict:
        """Optimize Chaikin Money Flow parameters"""
        param_grid = {
            'period': [14, 20, 21],
            'threshold': [0.05, 0.1, 0.15]
        }
        
        return self.grid_search(
            strategy_func=self._test_cmf_params,
            param_grid=param_grid,
            df=df,
            asset=asset,
            strategy_name="CMF"
        )
    
    def optimize_eom(self, df: pd.DataFrame, asset: str) -> Dict:
        """Optimize Ease of Movement parameters"""
        param_grid = {
            'period': [5, 10, 14, 20],
            'signal_period': [3, 5, 7]
        }
        
        return self.grid_search(
            strategy_func=self._test_eom_params,
            param_grid=param_grid,
            df=df,
            asset=asset,
            strategy_name="EOM"
        )
    
    def optimize_vpt(self, df: pd.DataFrame, asset: str) -> Dict:
        """Optimize Volume Price Trend parameters"""
        param_grid = {
            'signal_period': [3, 5, 7, 10]
        }
        
        return self.grid_search(
            strategy_func=self._test_vpt_params,
            param_grid=param_grid,
            df=df,
            asset=asset,
            strategy_name="VPT"
        )
    
    def optimize_awesome(self, df: pd.DataFrame, asset: str) -> Dict:
        """Optimize Awesome Oscillator parameters"""
        param_grid = {
            'fast_period': [5],
            'slow_period': [34],
            'signal_period': [3, 5, 7]
        }
        
        return self.grid_search(
            strategy_func=self._test_awesome_params,
            param_grid=param_grid,
            df=df,
            asset=asset,
            strategy_name="Awesome"
        )
    
    def optimize_acceleration(self, df: pd.DataFrame, asset: str) -> Dict:
        """Optimize Acceleration Oscillator parameters"""
        param_grid = {
            'fast_period': [5],
            'slow_period': [34],
            'signal_period': [3, 5]
        }
        
        return self.grid_search(
            strategy_func=self._test_acceleration_params,
            param_grid=param_grid,
            df=df,
            asset=asset,
            strategy_name="Acceleration"
        )
    
    def optimize_rvgi(self, df: pd.DataFrame, asset: str) -> Dict:
        """Optimize Relative Vigor Index parameters"""
        param_grid = {
            'period': [8, 10, 14],
            'signal_period': [3, 4, 5]
        }
        
        return self.grid_search(
            strategy_func=self._test_rvgi_params,
            param_grid=param_grid,
            df=df,
            asset=asset,
            strategy_name="RVGI"
        )
    
    def optimize_trix(self, df: pd.DataFrame, asset: str) -> Dict:
        """Optimize TRIX parameters"""
        param_grid = {
            'period': [9, 12, 15, 18],
            'signal_period': [3, 5, 7]
        }
        
        return self.grid_search(
            strategy_func=self._test_trix_params,
            param_grid=param_grid,
            df=df,
            asset=asset,
            strategy_name="TRIX"
        )
    
    def optimize_cmo(self, df: pd.DataFrame, asset: str) -> Dict:
        """Optimize Chande Momentum Oscillator parameters"""
        param_grid = {
            'period': [7, 9, 14, 20],
            'oversold': [-40, -50, -60],
            'overbought': [40, 50, 60]
        }
        
        return self.grid_search(
            strategy_func=self._test_cmo_params,
            param_grid=param_grid,
            df=df,
            asset=asset,
            strategy_name="CMO"
        )
    
    def optimize_doji(self, df: pd.DataFrame, asset: str) -> Dict:
        """Optimize Doji detection parameters"""
        param_grid = {
            'body_threshold': [0.01, 0.02, 0.03, 0.05],
            'wick_threshold': [2, 3, 4]
        }
        
        return self.grid_search(
            strategy_func=self._test_doji_params,
            param_grid=param_grid,
            df=df,
            asset=asset,
            strategy_name="Doji"
        )
    
    def optimize_hammer(self, df: pd.DataFrame, asset: str) -> Dict:
        """Optimize Hammer detection parameters"""
        param_grid = {
            'body_threshold': [0.01, 0.02, 0.03],
            'wick_ratio': [2, 2.5, 3],
            'trend_period': [5, 10, 20]
        }
        
        return self.grid_search(
            strategy_func=self._test_hammer_params,
            param_grid=param_grid,
            df=df,
            asset=asset,
            strategy_name="Hammer"
        )
    
    def optimize_engulfing(self, df: pd.DataFrame, asset: str) -> Dict:
        """Optimize Engulfing pattern parameters"""
        param_grid = {
            'body_ratio': [0.7, 0.8, 0.9],
            'trend_period': [3, 5, 10]
        }
        
        return self.grid_search(
            strategy_func=self._test_engulfing_params,
            param_grid=param_grid,
            df=df,
            asset=asset,
            strategy_name="Engulfing"
        )
    
    def optimize_morning_star(self, df: pd.DataFrame, asset: str) -> Dict:
        """Optimize Morning Star pattern parameters"""
        param_grid = {
            'body_ratio': [0.3, 0.4, 0.5],
            'gap_threshold': [0.005, 0.01, 0.02]
        }
        
        return self.grid_search(
            strategy_func=self._test_morning_star_params,
            param_grid=param_grid,
            df=df,
            asset=asset,
            strategy_name="Morning_Star"
        )
    
    def optimize_evening_star(self, df: pd.DataFrame, asset: str) -> Dict:
        """Optimize Evening Star pattern parameters"""
        param_grid = {
            'body_ratio': [0.3, 0.4, 0.5],
            'gap_threshold': [0.005, 0.01, 0.02]
        }
        
        return self.grid_search(
            strategy_func=self._test_evening_star_params,
            param_grid=param_grid,
            df=df,
            asset=asset,
            strategy_name="Evening_Star"
        )
    
    def optimize_three_white(self, df: pd.DataFrame, asset: str) -> Dict:
        """Optimize Three White Soldiers parameters"""
        param_grid = {
            'body_min': [0.01, 0.02, 0.03],
            'wick_max': [0.3, 0.4, 0.5]
        }
        
        return self.grid_search(
            strategy_func=self._test_three_white_params,
            param_grid=param_grid,
            df=df,
            asset=asset,
            strategy_name="Three_White"
        )
    
    def optimize_three_black(self, df: pd.DataFrame, asset: str) -> Dict:
        """Optimize Three Black Crows parameters"""
        param_grid = {
            'body_min': [0.01, 0.02, 0.03],
            'wick_max': [0.3, 0.4, 0.5]
        }
        
        return self.grid_search(
            strategy_func=self._test_three_black_params,
            param_grid=param_grid,
            df=df,
            asset=asset,
            strategy_name="Three_Black"
        )
    
    def optimize_pivot_points(self, df: pd.DataFrame, asset: str) -> Dict:
        """Optimize Pivot Points parameters"""
        param_grid = {
            'pivot_type': ['classic', 'fibonacci', 'woodie'],
            'lookback': [1, 2, 3]
        }
        
        return self.grid_search(
            strategy_func=self._test_pivot_points_params,
            param_grid=param_grid,
            df=df,
            asset=asset,
            strategy_name="Pivot_Points"
        )
    
    def optimize_fibonacci(self, df: pd.DataFrame, asset: str) -> Dict:
        """Optimize Fibonacci Retracement parameters"""
        param_grid = {
            'lookback': [20, 30, 50, 100],
            'levels': [[0.236, 0.382, 0.5, 0.618, 0.786], 
                      [0.382, 0.5, 0.618]]
        }
        
        return self.grid_search(
            strategy_func=self._test_fibonacci_params,
            param_grid=param_grid,
            df=df,
            asset=asset,
            strategy_name="Fibonacci"
        )
    
    def optimize_supply_demand(self, df: pd.DataFrame, asset: str) -> Dict:
        """Optimize Supply and Demand zones parameters"""
        param_grid = {
            'zone_period': [10, 20, 30],
            'strength_bars': [1, 2, 3],
            'touch_tolerance': [0.001, 0.002, 0.005]
        }
        
        return self.grid_search(
            strategy_func=self._test_supply_demand_params,
            param_grid=param_grid,
            df=df,
            asset=asset,
            strategy_name="Supply_Demand"
        )
    
    def optimize_rsi_macd_combination(self, df: pd.DataFrame, asset: str) -> Dict:
        """Optimize RSI + MACD combination parameters"""
        param_grid = {
            'rsi_period': [7, 14],
            'rsi_oversold': [25, 30],
            'rsi_overbought': [70, 75],
            'macd_fast': [8, 12],
            'macd_slow': [20, 26],
            'macd_signal': [7, 9]
        }
        
        return self.grid_search(
            strategy_func=self._test_rsi_macd_combination_params,
            param_grid=param_grid,
            df=df,
            asset=asset,
            strategy_name="RSI_MACD_Combo"
        )
    
    def optimize_bollinger_rsi(self, df: pd.DataFrame, asset: str) -> Dict:
        """Optimize Bollinger + RSI combination parameters"""
        param_grid = {
            'bb_period': [20],
            'bb_std': [2.0],
            'rsi_period': [7, 14],
            'rsi_oversold': [25, 30],
            'rsi_overbought': [70, 75]
        }
        
        return self.grid_search(
            strategy_func=self._test_bollinger_rsi_params,
            param_grid=param_grid,
            df=df,
            asset=asset,
            strategy_name="Bollinger_RSI"
        )
    
    def optimize_adx_di(self, df: pd.DataFrame, asset: str) -> Dict:
        """Optimize ADX + DI combination parameters"""
        param_grid = {
            'period': [14],
            'adx_threshold': [20, 25, 30],
            'di_threshold': [20, 25, 30]
        }
        
        return self.grid_search(
            strategy_func=self._test_adx_di_params,
            param_grid=param_grid,
            df=df,
            asset=asset,
            strategy_name="ADX_DI"
        )
    
    def optimize_volume_breakout(self, df: pd.DataFrame, asset: str) -> Dict:
        """Optimize Volume Breakout parameters"""
        param_grid = {
            'volume_period': [10, 20],
            'volume_multiplier': [1.5, 2.0, 2.5],
            'price_period': [5, 10],
            'price_threshold': [0.01, 0.02, 0.03]
        }
        
        return self.grid_search(
            strategy_func=self._test_volume_breakout_params,
            param_grid=param_grid,
            df=df,
            asset=asset,
            strategy_name="Volume_Breakout"
        )
    
    def optimize_momentum_reversal(self, df: pd.DataFrame, asset: str) -> Dict:
        """Optimize Momentum Reversal parameters"""
        param_grid = {
            'rsi_period': [14],
            'rsi_extreme': [70, 75, 80],
            'lookback': [3, 5, 7],
            'reversal_threshold': [0.5, 1.0, 1.5]
        }
        
        return self.grid_search(
            strategy_func=self._test_momentum_reversal_params,
            param_grid=param_grid,
            df=df,
            asset=asset,
            strategy_name="Momentum_Reversal"
        )
    
    # ============= EXISTING CORE METHODS =============
    
    def grid_search(self, strategy_func: Callable, param_grid: Dict,
                   df: pd.DataFrame, asset: str, 
                   strategy_name: str = "Unknown",
                   metric: str = 'sharpe') -> Dict:
        """
        Perform grid search over parameters
        
        Args:
            strategy_func: Function that takes params and returns signals
            param_grid: Dict of parameter names to lists of values
            df: Historical data
            asset: Asset name
            strategy_name: Name of strategy for logging
            metric: Optimization metric ('sharpe', 'profit_factor', 'win_rate')
            
        Returns:
            Best parameters and results
        """
        # Generate all parameter combinations
        keys = param_grid.keys()
        values = param_grid.values()
        combinations = list(itertools.product(*values))
        
        print(f"\n🔍 Optimizing {strategy_name} for {asset} with {len(combinations)} parameter combinations...")
        
        results = []
        
        # Test each combination
        for i, combo in enumerate(combinations):
            params = dict(zip(keys, combo))
            
            # Progress indicator
            if (i + 1) % 10 == 0:
                print(f"   Progress: {i + 1}/{len(combinations)} combinations tested...")
            
            try:
                # Generate signals with these params
                signals = strategy_func(df, **params)
                
                if not signals or len(signals) == 0:
                    continue
                
                signals_df = pd.DataFrame(signals)
                
                # Run backtest
                bt_results = self.backtester.run_backtest(df, signals_df)
                
                # Store results
                results.append({
                    'params': params,
                    'trades': bt_results.total_trades,
                    'win_rate': bt_results.win_rate,
                    'profit_factor': bt_results.profit_factor,
                    'sharpe': bt_results.sharpe_ratio,
                    'sortino': bt_results.sortino_ratio,
                    'total_return': bt_results.total_return_pct,
                    'max_dd': bt_results.max_drawdown,
                    'avg_trade': bt_results.avg_win if bt_results.winning_trades > 0 else 0
                })
                
            except Exception as e:
                print(f"  ⚠️ Error with {params}: {e}")
                continue
        
        if not results:
            print(f"  ❌ No valid results for {strategy_name}")
            return {'error': 'No valid results'}
        
        # Convert to DataFrame for analysis
        results_df = pd.DataFrame(results)
        
        # Find best by selected metric
        if metric == 'sharpe':
            best_idx = results_df['sharpe'].idxmax()
            best_value = results_df.loc[best_idx, 'sharpe']
        elif metric == 'profit_factor':
            best_idx = results_df['profit_factor'].idxmax()
            best_value = results_df.loc[best_idx, 'profit_factor']
        elif metric == 'sortino':
            best_idx = results_df['sortino'].idxmax()
            best_value = results_df.loc[best_idx, 'sortino']
        else:  # win_rate
            best_idx = results_df['win_rate'].idxmax()
            best_value = results_df.loc[best_idx, 'win_rate']
        
        best = results_df.loc[best_idx].to_dict()
        
        # Save results to history
        result_entry = {
            'asset': asset,
            'strategy': strategy_name,
            'timestamp': datetime.now().isoformat(),
            'best_params': best['params'],
            'best_metric': metric,
            'best_value': best_value,
            'total_combinations': len(combinations),
            'valid_combinations': len(results),
            'results_summary': {
                'best_sharpe': float(results_df['sharpe'].max()),
                'best_profit_factor': float(results_df['profit_factor'].max()),
                'best_win_rate': float(results_df['win_rate'].max()),
                'best_return': float(results_df['total_return'].max()),
                'avg_sharpe': float(results_df['sharpe'].mean()),
            }
        }
        
        self.results_history.append(result_entry)
        
        # Save to file
        self._save_results(asset, strategy_name, results_df, result_entry)
        
        # Print summary
        print(f"\n✅ {strategy_name} Optimization Complete for {asset}")
        print(f"   Best Parameters: {best['params']}")
        print(f"   Best {metric}: {best_value:.4f}")
        print(f"   Win Rate: {best['win_rate']:.1%}")
        print(f"   Profit Factor: {best['profit_factor']:.2f}")
        print(f"   Total Return: {best['total_return']:.2f}%")
        print(f"   Max Drawdown: {best['max_dd']:.2%}")
        print(f"   Trades: {best['trades']}")
        
        return result_entry
    
    def parallel_grid_search(self, strategy_func: Callable, param_grid: Dict,
                            df: pd.DataFrame, asset: str,
                            strategy_name: str = "Unknown",
                            metric: str = 'sharpe',
                            max_workers: int = 4) -> Dict:
        """
        Perform grid search in parallel for faster optimization
        """
        # Generate all parameter combinations
        keys = param_grid.keys()
        values = param_grid.values()
        combinations = list(itertools.product(*values))
        
        print(f"\n🔍 Optimizing {strategy_name} for {asset} with {len(combinations)} combinations (parallel)...")
        
        results = []
        
        # Split combinations into chunks for parallel processing
        chunk_size = max(1, len(combinations) // max_workers)
        chunks = [combinations[i:i + chunk_size] for i in range(0, len(combinations), chunk_size)]
        
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            # Submit tasks
            future_to_chunk = {
                executor.submit(self._test_chunk, strategy_func, chunk, df): i 
                for i, chunk in enumerate(chunks)
            }
            
            # Collect results
            for future in as_completed(future_to_chunk):
                try:
                    chunk_results = future.result(timeout=60)
                    results.extend(chunk_results)
                    print(f"   ✓ Chunk {future_to_chunk[future] + 1}/{len(chunks)} complete")
                except Exception as e:
                    print(f"   ⚠️ Chunk failed: {e}")
        
        if not results:
            return {'error': 'No valid results'}
        
        # Convert to DataFrame and find best
        results_df = pd.DataFrame(results)
        
        if metric == 'sharpe':
            best_idx = results_df['sharpe'].idxmax()
        elif metric == 'profit_factor':
            best_idx = results_df['profit_factor'].idxmax()
        else:
            best_idx = results_df['win_rate'].idxmax()
        
        best = results_df.loc[best_idx].to_dict()
        
        return {
            'best_params': best['params'],
            'best_value': best[metric],
            'results': results_df.to_dict('records')
        }
    
    def _test_chunk(self, strategy_func: Callable, chunk: List[tuple], 
                   df: pd.DataFrame) -> List[Dict]:
        """Test a chunk of parameter combinations"""
        results = []
        keys = list(strategy_func.__code__.co_varnames[:len(chunk[0])]) if chunk else []
        
        for combo in chunk:
            params = dict(zip(keys, combo))
            try:
                signals = strategy_func(df, **params)
                if signals:
                    signals_df = pd.DataFrame(signals)
                    bt_results = self.backtester.run_backtest(df, signals_df)
                    
                    results.append({
                        'params': params,
                        'trades': bt_results.total_trades,
                        'win_rate': bt_results.win_rate,
                        'profit_factor': bt_results.profit_factor,
                        'sharpe': bt_results.sharpe_ratio,
                        'total_return': bt_results.total_return_pct,
                        'max_dd': bt_results.max_drawdown
                    })
            except:
                continue
        
        return results
    
    def _save_results(self, asset: str, strategy: str, 
                     results_df: pd.DataFrame, best_result: Dict):
        """Save optimization results to file"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{self.results_dir}/{asset}_{strategy}_{timestamp}.csv"
        
        # Save full results
        results_df.to_csv(filename, index=False)
        
        # Save best result as JSON
        json_filename = f"{self.results_dir}/{asset}_{strategy}_best_{timestamp}.json"
        with open(json_filename, 'w') as f:
            json.dump(best_result, f, indent=2, default=str)
        
        print(f"   💾 Results saved to {filename}")
        print(f"   💾 Best params saved to {json_filename}")
    
    def get_best_params(self, asset: str, strategy: str) -> Optional[Dict]:
        """Get best parameters from latest optimization"""
        for result in reversed(self.results_history):
            if result['asset'] == asset and result['strategy'] == strategy:
                return result['best_params']
        return None
    
    def compare_strategies(self, asset: str) -> pd.DataFrame:
        """Compare optimization results across strategies"""
        comparisons = []
        
        for result in self.results_history:
            if result['asset'] == asset:
                comparisons.append({
                    'strategy': result['strategy'],
                    'best_sharpe': result['results_summary']['best_sharpe'],
                    'best_profit_factor': result['results_summary']['best_profit_factor'],
                    'best_win_rate': result['results_summary']['best_win_rate'],
                    'best_return': result['results_summary']['best_return'],
                    'avg_sharpe': result['results_summary']['avg_sharpe'],
                    'date': result['timestamp'][:10]
                })
        
        if comparisons:
            return pd.DataFrame(comparisons).sort_values('best_sharpe', ascending=False)
        return pd.DataFrame()
    
    # ============= ALL TEST FUNCTION IMPLEMENTATIONS =============
    # (Add all the _test_* methods from the long code here)
    
    def _test_rsi_params(self, df, oversold, overbought, period=14):
        """Test RSI with specific parameters"""
        signals = []
        for i in range(period, len(df)):
            if 'rsi' not in df.columns:
                continue
            
            rsi = df['rsi'].iloc[i]
            if pd.isna(rsi):
                continue
            
            if rsi < oversold:
                signals.append({
                    'date': df.index[i],
                    'signal': 'BUY',
                    'confidence': 0.8,
                    'entry': df['close'].iloc[i],
                    'stop_loss': df['close'].iloc[i] * 0.97,
                    'take_profit': df['close'].iloc[i] * 1.06,
                    'strategy': 'rsi_optimized'
                })
            elif rsi > overbought:
                signals.append({
                    'date': df.index[i],
                    'signal': 'SELL',
                    'confidence': 0.8,
                    'entry': df['close'].iloc[i],
                    'stop_loss': df['close'].iloc[i] * 1.03,
                    'take_profit': df['close'].iloc[i] * 0.94,
                    'strategy': 'rsi_optimized'
                })
        return signals
    
    def _test_rsi_divergence_params(self, df, rsi_period, lookback, divergence_threshold):
        """Test RSI Divergence parameters"""
        signals = []
        if 'rsi' not in df.columns:
            return signals
        
        for i in range(rsi_period + lookback, len(df)):
            # Find recent swing highs/lows
            price_high = df['high'].iloc[i-lookback:i].max()
            price_low = df['low'].iloc[i-lookback:i].min()
            rsi_high = df['rsi'].iloc[i-lookback:i].max()
            rsi_low = df['rsi'].iloc[i-lookback:i].min()
            
            # Check for bearish divergence (price higher high, RSI lower high)
            if df['high'].iloc[i] > price_high and df['rsi'].iloc[i] < rsi_high - divergence_threshold:
                signals.append({
                    'date': df.index[i],
                    'signal': 'SELL',
                    'confidence': 0.7,
                    'entry': df['close'].iloc[i],
                    'stop_loss': df['close'].iloc[i] * 1.03,
                    'take_profit': df['close'].iloc[i] * 0.95
                })
            
            # Check for bullish divergence (price lower low, RSI higher low)
            if df['low'].iloc[i] < price_low and df['rsi'].iloc[i] > rsi_low + divergence_threshold:
                signals.append({
                    'date': df.index[i],
                    'signal': 'BUY',
                    'confidence': 0.7,
                    'entry': df['close'].iloc[i],
                    'stop_loss': df['close'].iloc[i] * 0.97,
                    'take_profit': df['close'].iloc[i] * 1.05
                })
        
        return signals
    
    def _test_stoch_rsi_params(self, df, rsi_period, stoch_period, k_period, d_period):
        """Test Stochastic RSI parameters"""
        signals = []
        # Calculate Stochastic RSI
        if 'rsi' not in df.columns:
            return signals
        
        rsi = df['rsi']
        stoch_rsi = (rsi - rsi.rolling(rsi_period).min()) / (rsi.rolling(rsi_period).max() - rsi.rolling(rsi_period).min())
        stoch_rsi = stoch_rsi * 100
        
        k = stoch_rsi.rolling(k_period).mean()
        d = k.rolling(d_period).mean()
        
        for i in range(max(rsi_period, stoch_period, k_period, d_period) + 1, len(df)):
            if k.iloc[i] < 20 and d.iloc[i] < 20 and k.iloc[i] > d.iloc[i]:
                signals.append({
                    'date': df.index[i],
                    'signal': 'BUY',
                    'confidence': 0.7,
                    'entry': df['close'].iloc[i],
                    'stop_loss': df['close'].iloc[i] * 0.98,
                    'take_profit': df['close'].iloc[i] * 1.04
                })
            elif k.iloc[i] > 80 and d.iloc[i] > 80 and k.iloc[i] < d.iloc[i]:
                signals.append({
                    'date': df.index[i],
                    'signal': 'SELL',
                    'confidence': 0.7,
                    'entry': df['close'].iloc[i],
                    'stop_loss': df['close'].iloc[i] * 1.02,
                    'take_profit': df['close'].iloc[i] * 0.96
                })
        
        return signals
    
    def _test_stochastic_params(self, df, k_period, d_period, oversold, overbought):
        """Test Stochastic parameters"""
        signals = []
        if 'stoch_k' in df.columns and 'stoch_d' in df.columns:
            k = df['stoch_k']
            d = df['stoch_d']
        else:
            # Calculate manually
            low_min = df['low'].rolling(k_period).min()
            high_max = df['high'].rolling(k_period).max()
            k = 100 * (df['close'] - low_min) / (high_max - low_min)
            d = k.rolling(d_period).mean()
        
        for i in range(max(k_period, d_period) + 1, len(df)):
            if k.iloc[i] < oversold and k.iloc[i] > d.iloc[i]:
                signals.append({
                    'date': df.index[i],
                    'signal': 'BUY',
                    'confidence': 0.75,
                    'entry': df['close'].iloc[i],
                    'stop_loss': df['close'].iloc[i] * 0.97,
                    'take_profit': df['close'].iloc[i] * 1.05
                })
            elif k.iloc[i] > overbought and k.iloc[i] < d.iloc[i]:
                signals.append({
                    'date': df.index[i],
                    'signal': 'SELL',
                    'confidence': 0.75,
                    'entry': df['close'].iloc[i],
                    'stop_loss': df['close'].iloc[i] * 1.03,
                    'take_profit': df['close'].iloc[i] * 0.95
                })
        
        return signals
    
    def _test_stochastic_fast_params(self, df, k_period, d_period, oversold, overbought):
        """Test Fast Stochastic parameters"""
        # Same as stochastic but with faster settings
        return self._test_stochastic_params(df, k_period, d_period, oversold, overbought)
    
    def _test_stochastic_full_params(self, df, k_period, d_period, slowing, oversold, overbought):
        """Test Full Stochastic parameters"""
        signals = []
        low_min = df['low'].rolling(k_period).min()
        high_max = df['high'].rolling(k_period).max()
        raw_k = 100 * (df['close'] - low_min) / (high_max - low_min)
        k = raw_k.rolling(slowing).mean()
        d = k.rolling(d_period).mean()
        
        for i in range(max(k_period, d_period, slowing) + 1, len(df)):
            if k.iloc[i] < oversold and k.iloc[i] > d.iloc[i]:
                signals.append({
                    'date': df.index[i],
                    'signal': 'BUY',
                    'confidence': 0.75,
                    'entry': df['close'].iloc[i],
                    'stop_loss': df['close'].iloc[i] * 0.97,
                    'take_profit': df['close'].iloc[i] * 1.05
                })
            elif k.iloc[i] > overbought and k.iloc[i] < d.iloc[i]:
                signals.append({
                    'date': df.index[i],
                    'signal': 'SELL',
                    'confidence': 0.75,
                    'entry': df['close'].iloc[i],
                    'stop_loss': df['close'].iloc[i] * 1.03,
                    'take_profit': df['close'].iloc[i] * 0.95
                })
        
        return signals
    
    def _test_macd_params(self, df, fast, slow, signal):
        """Test MACD with specific parameters"""
        signals = []
        # Calculate MACD
        ema_fast = df['close'].ewm(span=fast, adjust=False).mean()
        ema_slow = df['close'].ewm(span=slow, adjust=False).mean()
        macd_line = ema_fast - ema_slow
        signal_line = macd_line.ewm(span=signal, adjust=False).mean()
        
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
    
    def _test_macd_histogram_params(self, df, fast, slow, signal, histogram_threshold):
        """Test MACD Histogram parameters"""
        signals = []
        ema_fast = df['close'].ewm(span=fast, adjust=False).mean()
        ema_slow = df['close'].ewm(span=slow, adjust=False).mean()
        macd_line = ema_fast - ema_slow
        signal_line = macd_line.ewm(span=signal, adjust=False).mean()
        histogram = macd_line - signal_line
        
        for i in range(1, len(df)):
            if histogram.iloc[i] > histogram_threshold and histogram.iloc[i-1] <= histogram_threshold:
                signals.append({
                    'date': df.index[i],
                    'signal': 'BUY',
                    'confidence': 0.7,
                    'entry': df['close'].iloc[i],
                    'stop_loss': df['close'].iloc[i] * 0.98,
                    'take_profit': df['close'].iloc[i] * 1.05
                })
            elif histogram.iloc[i] < -histogram_threshold and histogram.iloc[i-1] >= -histogram_threshold:
                signals.append({
                    'date': df.index[i],
                    'signal': 'SELL',
                    'confidence': 0.7,
                    'entry': df['close'].iloc[i],
                    'stop_loss': df['close'].iloc[i] * 1.02,
                    'take_profit': df['close'].iloc[i] * 0.95
                })
        
        return signals
    
    def _test_macd_divergence_params(self, df, fast, slow, signal, lookback, divergence_threshold):
        """Test MACD Divergence parameters"""
        signals = []
        ema_fast = df['close'].ewm(span=fast, adjust=False).mean()
        ema_slow = df['close'].ewm(span=slow, adjust=False).mean()
        macd_line = ema_fast - ema_slow
        
        for i in range(lookback, len(df)):
            # Find recent swing highs/lows in price and MACD
            price_high = df['high'].iloc[i-lookback:i].max()
            price_low = df['low'].iloc[i-lookback:i].min()
            macd_high = macd_line.iloc[i-lookback:i].max()
            macd_low = macd_line.iloc[i-lookback:i].min()
            
            # Bearish divergence
            if df['high'].iloc[i] > price_high and macd_line.iloc[i] < macd_high - divergence_threshold:
                signals.append({
                    'date': df.index[i],
                    'signal': 'SELL',
                    'confidence': 0.7,
                    'entry': df['close'].iloc[i],
                    'stop_loss': df['close'].iloc[i] * 1.03,
                    'take_profit': df['close'].iloc[i] * 0.95
                })
            
            # Bullish divergence
            if df['low'].iloc[i] < price_low and macd_line.iloc[i] > macd_low + divergence_threshold:
                signals.append({
                    'date': df.index[i],
                    'signal': 'BUY',
                    'confidence': 0.7,
                    'entry': df['close'].iloc[i],
                    'stop_loss': df['close'].iloc[i] * 0.97,
                    'take_profit': df['close'].iloc[i] * 1.05
                })
        
        return signals
    
    def _test_cci_params(self, df, period, oversold, overbought):
        """Test CCI parameters"""
        signals = []
        if 'cci' in df.columns:
            cci = df['cci']
        else:
            # Calculate CCI manually
            tp = (df['high'] + df['low'] + df['close']) / 3
            sma_tp = tp.rolling(period).mean()
            mad = tp.rolling(period).apply(lambda x: np.abs(x - x.mean()).mean())
            cci = (tp - sma_tp) / (0.015 * mad)
        
        for i in range(period, len(df)):
            if cci.iloc[i] < oversold:
                signals.append({
                    'date': df.index[i],
                    'signal': 'BUY',
                    'confidence': 0.7,
                    'entry': df['close'].iloc[i],
                    'stop_loss': df['close'].iloc[i] * 0.97,
                    'take_profit': df['close'].iloc[i] * 1.05
                })
            elif cci.iloc[i] > overbought:
                signals.append({
                    'date': df.index[i],
                    'signal': 'SELL',
                    'confidence': 0.7,
                    'entry': df['close'].iloc[i],
                    'stop_loss': df['close'].iloc[i] * 1.03,
                    'take_profit': df['close'].iloc[i] * 0.95
                })
        
        return signals
    
    def _test_williams_r_params(self, df, period, oversold, overbought):
        """Test Williams %R parameters"""
        signals = []
        if 'williams_r' in df.columns:
            wr = df['williams_r']
        else:
            # Calculate Williams %R
            high_max = df['high'].rolling(period).max()
            low_min = df['low'].rolling(period).min()
            wr = -100 * (high_max - df['close']) / (high_max - low_min)
        
        for i in range(period, len(df)):
            if wr.iloc[i] < oversold:
                signals.append({
                    'date': df.index[i],
                    'signal': 'BUY',
                    'confidence': 0.7,
                    'entry': df['close'].iloc[i],
                    'stop_loss': df['close'].iloc[i] * 0.97,
                    'take_profit': df['close'].iloc[i] * 1.05
                })
            elif wr.iloc[i] > overbought:
                signals.append({
                    'date': df.index[i],
                    'signal': 'SELL',
                    'confidence': 0.7,
                    'entry': df['close'].iloc[i],
                    'stop_loss': df['close'].iloc[i] * 1.03,
                    'take_profit': df['close'].iloc[i] * 0.95
                })
        
        return signals
    
    def _test_mfi_params(self, df, period, oversold, overbought):
        """Test Money Flow Index parameters"""
        signals = []
        if 'mfi' in df.columns and 'volume' in df.columns:
            mfi = df['mfi']
        else:
            # Return empty if no volume data
            return signals
        
        for i in range(period, len(df)):
            if mfi.iloc[i] < oversold:
                signals.append({
                    'date': df.index[i],
                    'signal': 'BUY',
                    'confidence': 0.7,
                    'entry': df['close'].iloc[i],
                    'stop_loss': df['close'].iloc[i] * 0.97,
                    'take_profit': df['close'].iloc[i] * 1.05
                })
            elif mfi.iloc[i] > overbought:
                signals.append({
                    'date': df.index[i],
                    'signal': 'SELL',
                    'confidence': 0.7,
                    'entry': df['close'].iloc[i],
                    'stop_loss': df['close'].iloc[i] * 1.03,
                    'take_profit': df['close'].iloc[i] * 0.95
                })
        
        return signals
    
    def _test_uo_params(self, df, period1, period2, period3, oversold, overbought):
        """Test Ultimate Oscillator parameters"""
        signals = []
        
        # Make sure we're working with a DataFrame, not a list
        if not isinstance(df, pd.DataFrame):
            print(f"   ⚠️ Expected DataFrame, got {type(df)}")
            return signals
        
        try:
            # Simplified UO calculation using pandas
            close = df['close']
            low = df['low']
            high = df['high']
            
            # Calculate True Range
            tr = pd.DataFrame({
                'hl': high - low,
                'hc': abs(high - close.shift(1)),
                'lc': abs(low - close.shift(1))
            }).max(axis=1)
            
            # Calculate Buying Pressure
            bp = close - pd.concat([low, close.shift(1)], axis=1).min(axis=1)
            
            # Calculate averages
            avg7 = bp.rolling(window=period1).sum() / tr.rolling(window=period1).sum()
            avg14 = bp.rolling(window=period2).sum() / tr.rolling(window=period2).sum()
            avg28 = bp.rolling(window=period3).sum() / tr.rolling(window=period3).sum()
            
            # Ultimate Oscillator formula
            uo = 100 * (4 * avg7 + 2 * avg14 + avg28) / 7
            
            # Generate signals
            for i in range(max(period1, period2, period3) + 5, len(df)):
                if pd.isna(uo.iloc[i]):
                    continue
                    
                if uo.iloc[i] < oversold:
                    signals.append({
                        'date': df.index[i],
                        'signal': 'BUY',
                        'confidence': 0.7,
                        'entry': df['close'].iloc[i],
                        'stop_loss': df['close'].iloc[i] * 0.97,
                        'take_profit': df['close'].iloc[i] * 1.05
                    })
                elif uo.iloc[i] > overbought:
                    signals.append({
                        'date': df.index[i],
                        'signal': 'SELL',
                        'confidence': 0.7,
                        'entry': df['close'].iloc[i],
                        'stop_loss': df['close'].iloc[i] * 1.03,
                        'take_profit': df['close'].iloc[i] * 0.95
                    })
            
        except Exception as e:
            print(f"   ⚠️ UO calculation error: {e}")
        
        return signals
    
    def _test_apo_params(self, df, fast, slow, signal):
        """Test Absolute Price Oscillator parameters"""
        # APO is similar to MACD but without signal line
        signals = []
        ema_fast = df['close'].ewm(span=fast, adjust=False).mean()
        ema_slow = df['close'].ewm(span=slow, adjust=False).mean()
        apo = ema_fast - ema_slow
        signal_line = apo.ewm(span=signal, adjust=False).mean()
        
        for i in range(1, len(df)):
            if apo.iloc[i-1] <= signal_line.iloc[i-1] and apo.iloc[i] > signal_line.iloc[i]:
                signals.append({
                    'date': df.index[i],
                    'signal': 'BUY',
                    'confidence': 0.7,
                    'entry': df['close'].iloc[i],
                    'stop_loss': df['close'].iloc[i] * 0.98,
                    'take_profit': df['close'].iloc[i] * 1.04
                })
            elif apo.iloc[i-1] >= signal_line.iloc[i-1] and apo.iloc[i] < signal_line.iloc[i]:
                signals.append({
                    'date': df.index[i],
                    'signal': 'SELL',
                    'confidence': 0.7,
                    'entry': df['close'].iloc[i],
                    'stop_loss': df['close'].iloc[i] * 1.02,
                    'take_profit': df['close'].iloc[i] * 0.96
                })
        
        return signals
    
    def _test_ppo_params(self, df, fast, slow, signal):
        """Test Percentage Price Oscillator parameters"""
        signals = []
        ema_fast = df['close'].ewm(span=fast, adjust=False).mean()
        ema_slow = df['close'].ewm(span=slow, adjust=False).mean()
        ppo = 100 * (ema_fast - ema_slow) / ema_slow
        signal_line = ppo.ewm(span=signal, adjust=False).mean()
        
        for i in range(1, len(df)):
            if ppo.iloc[i-1] <= signal_line.iloc[i-1] and ppo.iloc[i] > signal_line.iloc[i]:
                signals.append({
                    'date': df.index[i],
                    'signal': 'BUY',
                    'confidence': 0.7,
                    'entry': df['close'].iloc[i],
                    'stop_loss': df['close'].iloc[i] * 0.98,
                    'take_profit': df['close'].iloc[i] * 1.04
                })
            elif ppo.iloc[i-1] >= signal_line.iloc[i-1] and ppo.iloc[i] < signal_line.iloc[i]:
                signals.append({
                    'date': df.index[i],
                    'signal': 'SELL',
                    'confidence': 0.7,
                    'entry': df['close'].iloc[i],
                    'stop_loss': df['close'].iloc[i] * 1.02,
                    'take_profit': df['close'].iloc[i] * 0.96
                })
        
        return signals
    
    def _test_sma_cross_params(self, df, fast_period, slow_period):
        """Test SMA Cross parameters"""
        signals = []
        sma_fast = df['close'].rolling(fast_period).mean()
        sma_slow = df['close'].rolling(slow_period).mean()
        
        for i in range(1, len(df)):
            if sma_fast.iloc[i-1] <= sma_slow.iloc[i-1] and sma_fast.iloc[i] > sma_slow.iloc[i]:
                signals.append({
                    'date': df.index[i],
                    'signal': 'BUY',
                    'confidence': 0.7,
                    'entry': df['close'].iloc[i],
                    'stop_loss': df['close'].iloc[i] * 0.95,
                    'take_profit': df['close'].iloc[i] * 1.1
                })
            elif sma_fast.iloc[i-1] >= sma_slow.iloc[i-1] and sma_fast.iloc[i] < sma_slow.iloc[i]:
                signals.append({
                    'date': df.index[i],
                    'signal': 'SELL',
                    'confidence': 0.7,
                    'entry': df['close'].iloc[i],
                    'stop_loss': df['close'].iloc[i] * 1.05,
                    'take_profit': df['close'].iloc[i] * 0.9
                })
        
        return signals
    
    def _test_ema_cross_params(self, df, fast_period, slow_period):
        """Test EMA Cross parameters"""
        signals = []
        ema_fast = df['close'].ewm(span=fast_period, adjust=False).mean()
        ema_slow = df['close'].ewm(span=slow_period, adjust=False).mean()
        
        for i in range(1, len(df)):
            if ema_fast.iloc[i-1] <= ema_slow.iloc[i-1] and ema_fast.iloc[i] > ema_slow.iloc[i]:
                signals.append({
                    'date': df.index[i],
                    'signal': 'BUY',
                    'confidence': 0.7,
                    'entry': df['close'].iloc[i],
                    'stop_loss': df['close'].iloc[i] * 0.96,
                    'take_profit': df['close'].iloc[i] * 1.08
                })
            elif ema_fast.iloc[i-1] >= ema_slow.iloc[i-1] and ema_fast.iloc[i] < ema_slow.iloc[i]:
                signals.append({
                    'date': df.index[i],
                    'signal': 'SELL',
                    'confidence': 0.7,
                    'entry': df['close'].iloc[i],
                    'stop_loss': df['close'].iloc[i] * 1.04,
                    'take_profit': df['close'].iloc[i] * 0.92
                })
        
        return signals
    
    def _test_wma_cross_params(self, df, fast_period, slow_period):
        """Test WMA Cross parameters"""
        signals = []
        # Simple WMA approximation
        def wma(series, period):
            weights = np.arange(1, period + 1)
            return series.rolling(period).apply(lambda x: np.sum(weights * x) / weights.sum(), raw=True)
        
        wma_fast = wma(df['close'], fast_period)
        wma_slow = wma(df['close'], slow_period)
        
        for i in range(1, len(df)):
            if wma_fast.iloc[i-1] <= wma_slow.iloc[i-1] and wma_fast.iloc[i] > wma_slow.iloc[i]:
                signals.append({
                    'date': df.index[i],
                    'signal': 'BUY',
                    'confidence': 0.7,
                    'entry': df['close'].iloc[i],
                    'stop_loss': df['close'].iloc[i] * 0.96,
                    'take_profit': df['close'].iloc[i] * 1.08
                })
            elif wma_fast.iloc[i-1] >= wma_slow.iloc[i-1] and wma_fast.iloc[i] < wma_slow.iloc[i]:
                signals.append({
                    'date': df.index[i],
                    'signal': 'SELL',
                    'confidence': 0.7,
                    'entry': df['close'].iloc[i],
                    'stop_loss': df['close'].iloc[i] * 1.04,
                    'take_profit': df['close'].iloc[i] * 0.92
                })
        
        return signals
    
    def _test_hma_cross_params(self, df, fast_period, slow_period):
        """Test HMA Cross parameters"""
        signals = []
        # Hull Moving Average approximation
        def hma(series, period):
            half_length = int(period / 2)
            sqrt_length = int(np.sqrt(period))
            wma_half = 2 * series.rolling(half_length).apply(
                lambda x: np.sum(np.arange(1, half_length + 1) * x) / np.sum(np.arange(1, half_length + 1)), raw=True
            )
            wma_full = series.rolling(period).apply(
                lambda x: np.sum(np.arange(1, period + 1) * x) / np.sum(np.arange(1, period + 1)), raw=True
            )
            raw_hma = wma_half - wma_full
            hma_val = raw_hma.rolling(sqrt_length).apply(
                lambda x: np.sum(np.arange(1, sqrt_length + 1) * x) / np.sum(np.arange(1, sqrt_length + 1)), raw=True
            )
            return hma_val
        
        hma_fast = hma(df['close'], fast_period)
        hma_slow = hma(df['close'], slow_period)
        
        for i in range(1, len(df)):
            if hma_fast.iloc[i-1] <= hma_slow.iloc[i-1] and hma_fast.iloc[i] > hma_slow.iloc[i]:
                signals.append({
                    'date': df.index[i],
                    'signal': 'BUY',
                    'confidence': 0.7,
                    'entry': df['close'].iloc[i],
                    'stop_loss': df['close'].iloc[i] * 0.96,
                    'take_profit': df['close'].iloc[i] * 1.08
                })
            elif hma_fast.iloc[i-1] >= hma_slow.iloc[i-1] and hma_fast.iloc[i] < hma_slow.iloc[i]:
                signals.append({
                    'date': df.index[i],
                    'signal': 'SELL',
                    'confidence': 0.7,
                    'entry': df['close'].iloc[i],
                    'stop_loss': df['close'].iloc[i] * 1.04,
                    'take_profit': df['close'].iloc[i] * 0.92
                })
        
        return signals
    
    def _test_vwap_params(self, df, period, std_dev):
        """Test VWAP parameters"""
        signals = []
        if 'volume' not in df.columns:
            return signals
        
        # Calculate VWAP
        vwap = (df['close'] * df['volume']).rolling(period).sum() / df['volume'].rolling(period).sum()
        std = df['close'].rolling(period).std()
        
        for i in range(period, len(df)):
            if df['close'].iloc[i] < vwap.iloc[i] - std_dev * std.iloc[i]:
                signals.append({
                    'date': df.index[i],
                    'signal': 'BUY',
                    'confidence': 0.7,
                    'entry': df['close'].iloc[i],
                    'stop_loss': df['close'].iloc[i] * 0.97,
                    'take_profit': vwap.iloc[i]
                })
            elif df['close'].iloc[i] > vwap.iloc[i] + std_dev * std.iloc[i]:
                signals.append({
                    'date': df.index[i],
                    'signal': 'SELL',
                    'confidence': 0.7,
                    'entry': df['close'].iloc[i],
                    'stop_loss': df['close'].iloc[i] * 1.03,
                    'take_profit': vwap.iloc[i]
                })
        
        return signals
    
    def _test_adx_params(self, df, period, threshold):
        """Test ADX parameters"""
        signals = []
        if 'adx' not in df.columns:
            return signals
        
        adx = df['adx']
        
        for i in range(period, len(df)):
            if adx.iloc[i] > threshold:
                # Strong trend - direction from other indicators
                if df['close'].iloc[i] > df['sma_20'].iloc[i] if 'sma_20' in df.columns else True:
                    signals.append({
                        'date': df.index[i],
                        'signal': 'BUY',
                        'confidence': 0.6,
                        'entry': df['close'].iloc[i],
                        'stop_loss': df['close'].iloc[i] * 0.97,
                        'take_profit': df['close'].iloc[i] * 1.06
                    })
                else:
                    signals.append({
                        'date': df.index[i],
                        'signal': 'SELL',
                        'confidence': 0.6,
                        'entry': df['close'].iloc[i],
                        'stop_loss': df['close'].iloc[i] * 1.03,
                        'take_profit': df['close'].iloc[i] * 0.94
                    })
        
        return signals
    
    def _test_di_plus_params(self, df, period, threshold):
        """Test +DI parameters"""
        signals = []
        if 'di_plus' not in df.columns:
            return signals
        
        di_plus = df['di_plus']
        
        for i in range(period, len(df)):
            if di_plus.iloc[i] > threshold and di_plus.iloc[i] > di_plus.iloc[i-1]:
                signals.append({
                    'date': df.index[i],
                    'signal': 'BUY',
                    'confidence': 0.65,
                    'entry': df['close'].iloc[i],
                    'stop_loss': df['close'].iloc[i] * 0.97,
                    'take_profit': df['close'].iloc[i] * 1.06
                })
        
        return signals
    
    def _test_di_minus_params(self, df, period, threshold):
        """Test -DI parameters"""
        signals = []
        if 'di_minus' not in df.columns:
            return signals
        
        di_minus = df['di_minus']
        
        for i in range(period, len(df)):
            if di_minus.iloc[i] > threshold and di_minus.iloc[i] > di_minus.iloc[i-1]:
                signals.append({
                    'date': df.index[i],
                    'signal': 'SELL',
                    'confidence': 0.65,
                    'entry': df['close'].iloc[i],
                    'stop_loss': df['close'].iloc[i] * 1.03,
                    'take_profit': df['close'].iloc[i] * 0.94
                })
        
        return signals
    
    def _test_adx_cross_params(self, df, period, adx_threshold):
        """Test ADX Cross parameters"""
        signals = []
        if 'di_plus' not in df.columns or 'di_minus' not in df.columns:
            return signals
        
        di_plus = df['di_plus']
        di_minus = df['di_minus']
        
        for i in range(1, len(df)):
            if di_plus.iloc[i-1] <= di_minus.iloc[i-1] and di_plus.iloc[i] > di_minus.iloc[i]:
                signals.append({
                    'date': df.index[i],
                    'signal': 'BUY',
                    'confidence': 0.7,
                    'entry': df['close'].iloc[i],
                    'stop_loss': df['close'].iloc[i] * 0.97,
                    'take_profit': df['close'].iloc[i] * 1.06
                })
            elif di_plus.iloc[i-1] >= di_minus.iloc[i-1] and di_plus.iloc[i] < di_minus.iloc[i]:
                signals.append({
                    'date': df.index[i],
                    'signal': 'SELL',
                    'confidence': 0.7,
                    'entry': df['close'].iloc[i],
                    'stop_loss': df['close'].iloc[i] * 1.03,
                    'take_profit': df['close'].iloc[i] * 0.94
                })
        
        return signals
    
    def _test_ichimoku_params(self, df, tenkan, kijun, senkou):
        """Test Ichimoku Cloud parameters"""
        signals = []
        # Simplified Ichimoku signal
        if 'tenkan_sen' in df.columns and 'kijun_sen' in df.columns and 'senkou_span_a' in df.columns:
            tenkan = df['tenkan_sen']
            kijun = df['kijun_sen']
            span_a = df['senkou_span_a']
            
            for i in range(1, len(df)):
                # TK Cross
                if tenkan.iloc[i-1] <= kijun.iloc[i-1] and tenkan.iloc[i] > kijun.iloc[i]:
                    if df['close'].iloc[i] > span_a.iloc[i]:
                        signals.append({
                            'date': df.index[i],
                            'signal': 'BUY',
                            'confidence': 0.75,
                            'entry': df['close'].iloc[i],
                            'stop_loss': df['close'].iloc[i] * 0.96,
                            'take_profit': df['close'].iloc[i] * 1.08
                        })
                elif tenkan.iloc[i-1] >= kijun.iloc[i-1] and tenkan.iloc[i] < kijun.iloc[i]:
                    if df['close'].iloc[i] < span_a.iloc[i]:
                        signals.append({
                            'date': df.index[i],
                            'signal': 'SELL',
                            'confidence': 0.75,
                            'entry': df['close'].iloc[i],
                            'stop_loss': df['close'].iloc[i] * 1.04,
                            'take_profit': df['close'].iloc[i] * 0.92
                        })
        
        return signals
    
    def _test_ichimoku_tenkan_params(self, df, tenkan_period, kijun_period):
        """Test Ichimoku Tenkan-sen parameters"""
        # Reuse ichimoku test with different periods
        return self._test_ichimoku_params(df, tenkan_period, kijun_period, kijun_period * 2)
    
    def _test_ichimoku_kijun_params(self, df, tenkan_period, kijun_period):
        """Test Ichimoku Kijun-sen parameters"""
        return self._test_ichimoku_params(df, tenkan_period, kijun_period, kijun_period * 2)
    
    def _test_ichimoku_cross_params(self, df, tenkan, kijun, cloud_offset):
        """Test Ichimoku Cross parameters"""
        return self._test_ichimoku_params(df, tenkan, kijun, cloud_offset)
    
    def _test_psar_params(self, df, acceleration, max_acceleration):
        """Test Parabolic SAR parameters"""
        signals = []
        if 'psar' in df.columns:
            psar = df['psar']
            
            for i in range(1, len(df)):
                if psar.iloc[i-1] > df['close'].iloc[i-1] and psar.iloc[i] < df['close'].iloc[i]:
                    signals.append({
                        'date': df.index[i],
                        'signal': 'BUY',
                        'confidence': 0.7,
                        'entry': df['close'].iloc[i],
                        'stop_loss': df['close'].iloc[i] * 0.97,
                        'take_profit': df['close'].iloc[i] * 1.06
                    })
                elif psar.iloc[i-1] < df['close'].iloc[i-1] and psar.iloc[i] > df['close'].iloc[i]:
                    signals.append({
                        'date': df.index[i],
                        'signal': 'SELL',
                        'confidence': 0.7,
                        'entry': df['close'].iloc[i],
                        'stop_loss': df['close'].iloc[i] * 1.03,
                        'take_profit': df['close'].iloc[i] * 0.94
                    })
        
        return signals
    
    def _test_bollinger_params(self, df, period, std_dev, exit_std):
        """Test Bollinger Bands with specific parameters"""
        signals = []
        if 'bb_upper' in df.columns and 'bb_lower' in df.columns and 'bb_middle' in df.columns:
            bb_upper = df['bb_upper']
            bb_lower = df['bb_lower']
            bb_middle = df['bb_middle']
        else:
            # Calculate manually
            bb_middle = df['close'].rolling(period).mean()
            bb_std = df['close'].rolling(period).std()
            bb_upper = bb_middle + (bb_std * std_dev)
            bb_lower = bb_middle - (bb_std * std_dev)
        
        for i in range(period, len(df)):
            if df['close'].iloc[i] <= bb_lower.iloc[i]:
                signals.append({
                    'date': df.index[i],
                    'signal': 'BUY',
                    'confidence': 0.75,
                    'entry': df['close'].iloc[i],
                    'stop_loss': bb_lower.iloc[i] * 0.98,
                    'take_profit': bb_middle.iloc[i]
                })
            elif df['close'].iloc[i] >= bb_upper.iloc[i]:
                signals.append({
                    'date': df.index[i],
                    'signal': 'SELL',
                    'confidence': 0.75,
                    'entry': df['close'].iloc[i],
                    'stop_loss': bb_upper.iloc[i] * 1.02,
                    'take_profit': bb_middle.iloc[i]
                })
        
        return signals
    
    def _test_bollinger_breakout_params(self, df, period, std_dev, confirmation_bars):
        """Test Bollinger Breakout parameters"""
        signals = []
        bb_middle = df['close'].rolling(period).mean()
        bb_std = df['close'].rolling(period).std()
        bb_upper = bb_middle + (bb_std * std_dev)
        bb_lower = bb_middle - (bb_std * std_dev)
        
        for i in range(period + confirmation_bars, len(df)):
            # Check for breakout
            if all(df['close'].iloc[i-j] > bb_upper.iloc[i-j] for j in range(confirmation_bars)):
                signals.append({
                    'date': df.index[i],
                    'signal': 'BUY',
                    'confidence': 0.7,
                    'entry': df['close'].iloc[i],
                    'stop_loss': bb_middle.iloc[i],
                    'take_profit': df['close'].iloc[i] * 1.05
                })
            elif all(df['close'].iloc[i-j] < bb_lower.iloc[i-j] for j in range(confirmation_bars)):
                signals.append({
                    'date': df.index[i],
                    'signal': 'SELL',
                    'confidence': 0.7,
                    'entry': df['close'].iloc[i],
                    'stop_loss': bb_middle.iloc[i],
                    'take_profit': df['close'].iloc[i] * 0.95
                })
        
        return signals
    
    def _test_bollinger_squeeze_params(self, df, period, bb_std, kc_std, squeeze_period):
        """Test Bollinger Squeeze parameters"""
        signals = []
        # Calculate Bollinger Width
        bb_middle = df['close'].rolling(period).mean()
        bb_std_val = df['close'].rolling(period).std()
        bb_width = (bb_middle + bb_std_val * bb_std) - (bb_middle - bb_std_val * bb_std)
        
        # Calculate Keltner Width (simplified)
        atr = (df['high'] - df['low']).rolling(period).mean()
        kc_width = atr * kc_std * 2
        
        # Squeeze when BB inside KC
        squeeze = bb_width < kc_width
        
        for i in range(period + squeeze_period, len(df)):
            if squeeze.iloc[i] and not squeeze.iloc[i-squeeze_period]:
                # Squeeze just started
                pass
            elif not squeeze.iloc[i] and squeeze.iloc[i-1]:
                # Squeeze release - potential breakout
                direction = 1 if df['close'].iloc[i] > df['close'].iloc[i-1] else -1
                if direction > 0:
                    signals.append({
                        'date': df.index[i],
                        'signal': 'BUY',
                        'confidence': 0.7,
                        'entry': df['close'].iloc[i],
                        'stop_loss': df['close'].iloc[i] * 0.97,
                        'take_profit': df['close'].iloc[i] * 1.06
                    })
                else:
                    signals.append({
                        'date': df.index[i],
                        'signal': 'SELL',
                        'confidence': 0.7,
                        'entry': df['close'].iloc[i],
                        'stop_loss': df['close'].iloc[i] * 1.03,
                        'take_profit': df['close'].iloc[i] * 0.94
                    })
        
        return signals
    
    def _test_bollinger_width_params(self, df, period, width_threshold):
        """Test Bollinger Width parameters"""
        signals = []
        bb_middle = df['close'].rolling(period).mean()
        bb_std = df['close'].rolling(period).std()
        bb_width = ((bb_middle + bb_std * 2) - (bb_middle - bb_std * 2)) / bb_middle
        
        for i in range(period, len(df)):
            if bb_width.iloc[i] < width_threshold:
                # Narrow width - potential breakout soon
                pass
            elif bb_width.iloc[i] > width_threshold * 2:
                # Wide width - potential reversal
                if df['close'].iloc[i] > bb_middle.iloc[i]:
                    signals.append({
                        'date': df.index[i],
                        'signal': 'SELL',
                        'confidence': 0.6,
                        'entry': df['close'].iloc[i],
                        'stop_loss': df['close'].iloc[i] * 1.03,
                        'take_profit': bb_middle.iloc[i]
                    })
                else:
                    signals.append({
                        'date': df.index[i],
                        'signal': 'BUY',
                        'confidence': 0.6,
                        'entry': df['close'].iloc[i],
                        'stop_loss': df['close'].iloc[i] * 0.97,
                        'take_profit': bb_middle.iloc[i]
                    })
        
        return signals
    
    def _test_keltner_params(self, df, ema_period, atr_period, atr_multiplier):
        """Test Keltner Channels parameters"""
        signals = []
        ema = df['close'].ewm(span=ema_period, adjust=False).mean()
        atr = (df['high'] - df['low']).rolling(atr_period).mean()
        upper = ema + atr * atr_multiplier
        lower = ema - atr * atr_multiplier
        
        for i in range(max(ema_period, atr_period), len(df)):
            if df['close'].iloc[i] <= lower.iloc[i]:
                signals.append({
                    'date': df.index[i],
                    'signal': 'BUY',
                    'confidence': 0.7,
                    'entry': df['close'].iloc[i],
                    'stop_loss': lower.iloc[i] * 0.98,
                    'take_profit': ema.iloc[i]
                })
            elif df['close'].iloc[i] >= upper.iloc[i]:
                signals.append({
                    'date': df.index[i],
                    'signal': 'SELL',
                    'confidence': 0.7,
                    'entry': df['close'].iloc[i],
                    'stop_loss': upper.iloc[i] * 1.02,
                    'take_profit': ema.iloc[i]
                })
        
        return signals
    
    def _test_keltner_breakout_params(self, df, ema_period, atr_period, atr_multiplier, confirmation_bars):
        """Test Keltner Breakout parameters"""
        signals = []
        ema = df['close'].ewm(span=ema_period, adjust=False).mean()
        atr = (df['high'] - df['low']).rolling(atr_period).mean()
        upper = ema + atr * atr_multiplier
        lower = ema - atr * atr_multiplier
        
        for i in range(max(ema_period, atr_period) + confirmation_bars, len(df)):
            if all(df['close'].iloc[i-j] > upper.iloc[i-j] for j in range(confirmation_bars)):
                signals.append({
                    'date': df.index[i],
                    'signal': 'BUY',
                    'confidence': 0.7,
                    'entry': df['close'].iloc[i],
                    'stop_loss': ema.iloc[i],
                    'take_profit': df['close'].iloc[i] * 1.05
                })
            elif all(df['close'].iloc[i-j] < lower.iloc[i-j] for j in range(confirmation_bars)):
                signals.append({
                    'date': df.index[i],
                    'signal': 'SELL',
                    'confidence': 0.7,
                    'entry': df['close'].iloc[i],
                    'stop_loss': ema.iloc[i],
                    'take_profit': df['close'].iloc[i] * 0.95
                })
        
        return signals
    
    def _test_atr_params(self, df, period):
        """Test ATR parameters"""
        signals = []
        # ATR itself doesn't generate signals, but can be used for volatility regime
        # This is a placeholder - ATR is usually combined with other indicators
        return signals
    
    def _test_atr_trailing_params(self, df, period, multiplier):
        """Test ATR Trailing Stop parameters"""
        signals = []
        atr = (df['high'] - df['low']).rolling(period).mean()
        
        long_stop = df['close'].rolling(2).min() - atr * multiplier
        short_stop = df['close'].rolling(2).max() + atr * multiplier
        
        for i in range(period, len(df)):
            if df['close'].iloc[i] > long_stop.iloc[i] and df['close'].iloc[i-1] <= long_stop.iloc[i-1]:
                signals.append({
                    'date': df.index[i],
                    'signal': 'BUY',
                    'confidence': 0.65,
                    'entry': df['close'].iloc[i],
                    'stop_loss': long_stop.iloc[i],
                    'take_profit': df['close'].iloc[i] * 1.05
                })
            elif df['close'].iloc[i] < short_stop.iloc[i] and df['close'].iloc[i-1] >= short_stop.iloc[i-1]:
                signals.append({
                    'date': df.index[i],
                    'signal': 'SELL',
                    'confidence': 0.65,
                    'entry': df['close'].iloc[i],
                    'stop_loss': short_stop.iloc[i],
                    'take_profit': df['close'].iloc[i] * 0.95
                })
        
        return signals
    
    def _test_atr_bands_params(self, df, period, multiplier, band_type):
        """Test ATR Bands parameters"""
        signals = []
        middle = df['close'].rolling(period).mean()
        atr = (df['high'] - df['low']).rolling(period).mean()
        upper = middle + atr * multiplier
        lower = middle - atr * multiplier
        
        for i in range(period, len(df)):
            if band_type in ['lower', 'both'] and df['close'].iloc[i] <= lower.iloc[i]:
                signals.append({
                    'date': df.index[i],
                    'signal': 'BUY',
                    'confidence': 0.65,
                    'entry': df['close'].iloc[i],
                    'stop_loss': lower.iloc[i] * 0.98,
                    'take_profit': middle.iloc[i]
                })
            elif band_type in ['upper', 'both'] and df['close'].iloc[i] >= upper.iloc[i]:
                signals.append({
                    'date': df.index[i],
                    'signal': 'SELL',
                    'confidence': 0.65,
                    'entry': df['close'].iloc[i],
                    'stop_loss': upper.iloc[i] * 1.02,
                    'take_profit': middle.iloc[i]
                })
        
        return signals
    
    def _test_donchian_params(self, df, period):
        """Test Donchian Channels parameters"""
        signals = []
        upper = df['high'].rolling(period).max()
        lower = df['low'].rolling(period).min()
        
        for i in range(period, len(df)):
            if df['close'].iloc[i] >= upper.iloc[i]:
                signals.append({
                    'date': df.index[i],
                    'signal': 'BUY',
                    'confidence': 0.7,
                    'entry': df['close'].iloc[i],
                    'stop_loss': lower.iloc[i],
                    'take_profit': df['close'].iloc[i] * 1.05
                })
            elif df['close'].iloc[i] <= lower.iloc[i]:
                signals.append({
                    'date': df.index[i],
                    'signal': 'SELL',
                    'confidence': 0.7,
                    'entry': df['close'].iloc[i],
                    'stop_loss': upper.iloc[i],
                    'take_profit': df['close'].iloc[i] * 0.95
                })
        
        return signals
    
    def _test_donchian_breakout_params(self, df, period, confirmation_bars):
        """Test Donchian Breakout parameters"""
        signals = []
        upper = df['high'].rolling(period).max()
        lower = df['low'].rolling(period).min()
        
        for i in range(period + confirmation_bars, len(df)):
            if all(df['close'].iloc[i-j] > upper.iloc[i-j] for j in range(confirmation_bars)):
                signals.append({
                    'date': df.index[i],
                    'signal': 'BUY',
                    'confidence': 0.75,
                    'entry': df['close'].iloc[i],
                    'stop_loss': upper.iloc[i] * 0.98,
                    'take_profit': df['close'].iloc[i] * 1.06
                })
            elif all(df['close'].iloc[i-j] < lower.iloc[i-j] for j in range(confirmation_bars)):
                signals.append({
                    'date': df.index[i],
                    'signal': 'SELL',
                    'confidence': 0.75,
                    'entry': df['close'].iloc[i],
                    'stop_loss': lower.iloc[i] * 1.02,
                    'take_profit': df['close'].iloc[i] * 0.94
                })
        
        return signals
    
    def _test_volatility_ratio_params(self, df, period, ratio_threshold):
        """Test Volatility Ratio parameters"""
        signals = []
        high_low = df['high'] - df['low']
        current_range = high_low.rolling(1).mean()
        avg_range = high_low.rolling(period).mean()
        vr = current_range / avg_range
        
        for i in range(period, len(df)):
            if vr.iloc[i] > ratio_threshold:
                # High volatility - could mean breakout
                if df['close'].iloc[i] > df['open'].iloc[i]:
                    signals.append({
                        'date': df.index[i],
                        'signal': 'BUY',
                        'confidence': 0.6,
                        'entry': df['close'].iloc[i],
                        'stop_loss': df['close'].iloc[i] * 0.97,
                        'take_profit': df['close'].iloc[i] * 1.05
                    })
                else:
                    signals.append({
                        'date': df.index[i],
                        'signal': 'SELL',
                        'confidence': 0.6,
                        'entry': df['close'].iloc[i],
                        'stop_loss': df['close'].iloc[i] * 1.03,
                        'take_profit': df['close'].iloc[i] * 0.95
                    })
        
        return signals
    
    def _test_chaikin_volatility_params(self, df, period, roc_period, threshold):
        """Test Chaikin Volatility parameters"""
        signals = []
        # Chaikin Volatility = ROC of ATR
        atr = (df['high'] - df['low']).rolling(period).mean()
        chaikin_vol = atr.pct_change(roc_period) * 100
        
        for i in range(period + roc_period, len(df)):
            if chaikin_vol.iloc[i] > threshold:
                # Volatility increasing - potential breakout
                if df['close'].iloc[i] > df['close'].iloc[i-1]:
                    signals.append({
                        'date': df.index[i],
                        'signal': 'BUY',
                        'confidence': 0.6,
                        'entry': df['close'].iloc[i],
                        'stop_loss': df['close'].iloc[i] * 0.97,
                        'take_profit': df['close'].iloc[i] * 1.05
                    })
                else:
                    signals.append({
                        'date': df.index[i],
                        'signal': 'SELL',
                        'confidence': 0.6,
                        'entry': df['close'].iloc[i],
                        'stop_loss': df['close'].iloc[i] * 1.03,
                        'take_profit': df['close'].iloc[i] * 0.95
                    })
        
        return signals
    
    def _test_obv_params(self, df, signal_period):
        """Test OBV parameters"""
        signals = []
        if 'obv' not in df.columns or 'volume' not in df.columns:
            return signals
        
        obv = df['obv']
        obv_signal = obv.rolling(signal_period).mean()
        
        for i in range(1, len(df)):
            if obv.iloc[i-1] <= obv_signal.iloc[i-1] and obv.iloc[i] > obv_signal.iloc[i]:
                signals.append({
                    'date': df.index[i],
                    'signal': 'BUY',
                    'confidence': 0.65,
                    'entry': df['close'].iloc[i],
                    'stop_loss': df['close'].iloc[i] * 0.97,
                    'take_profit': df['close'].iloc[i] * 1.05
                })
            elif obv.iloc[i-1] >= obv_signal.iloc[i-1] and obv.iloc[i] < obv_signal.iloc[i]:
                signals.append({
                    'date': df.index[i],
                    'signal': 'SELL',
                    'confidence': 0.65,
                    'entry': df['close'].iloc[i],
                    'stop_loss': df['close'].iloc[i] * 1.03,
                    'take_profit': df['close'].iloc[i] * 0.95
                })
        
        return signals
    
    def _test_obv_divergence_params(self, df, lookback, divergence_threshold):
        """Test OBV Divergence parameters"""
        signals = []
        if 'obv' not in df.columns:
            return signals
        
        obv = df['obv']
        
        for i in range(lookback, len(df)):
            price_high = df['high'].iloc[i-lookback:i].max()
            price_low = df['low'].iloc[i-lookback:i].min()
            obv_high = obv.iloc[i-lookback:i].max()
            obv_low = obv.iloc[i-lookback:i].min()
            
            # Bearish divergence (price higher high, OBV lower high)
            if df['high'].iloc[i] > price_high and obv.iloc[i] < obv_high - divergence_threshold * obv_high:
                signals.append({
                    'date': df.index[i],
                    'signal': 'SELL',
                    'confidence': 0.7,
                    'entry': df['close'].iloc[i],
                    'stop_loss': df['close'].iloc[i] * 1.03,
                    'take_profit': df['close'].iloc[i] * 0.95
                })
            
            # Bullish divergence (price lower low, OBV higher low)
            if df['low'].iloc[i] < price_low and obv.iloc[i] > obv_low + divergence_threshold * obv_low:
                signals.append({
                    'date': df.index[i],
                    'signal': 'BUY',
                    'confidence': 0.7,
                    'entry': df['close'].iloc[i],
                    'stop_loss': df['close'].iloc[i] * 0.97,
                    'take_profit': df['close'].iloc[i] * 1.05
                })
        
        return signals
    
    def _test_volume_profile_params(self, df, num_bins, value_area_pct):
        """Test Volume Profile parameters"""
        # Volume profile is more complex - this is a simplified version
        signals = []
        if 'volume' not in df.columns:
            return signals
        
        # Use last 20 periods for volume profile
        recent_df = df.tail(20)
        price_min = recent_df['low'].min()
        price_max = recent_df['high'].max()
        bin_size = (price_max - price_min) / num_bins
        
        # This is a placeholder - full volume profile implementation is complex
        return signals
    
    def _test_volume_oscillator_params(self, df, fast_period, slow_period, threshold):
        """Test Volume Oscillator parameters"""
        signals = []
        if 'volume' not in df.columns:
            return signals
        
        fast_ma = df['volume'].rolling(fast_period).mean()
        slow_ma = df['volume'].rolling(slow_period).mean()
        vo = 100 * (fast_ma - slow_ma) / slow_ma
        
        for i in range(slow_period, len(df)):
            if vo.iloc[i] > threshold and df['close'].iloc[i] > df['close'].iloc[i-1]:
                signals.append({
                    'date': df.index[i],
                    'signal': 'BUY',
                    'confidence': 0.65,
                    'entry': df['close'].iloc[i],
                    'stop_loss': df['close'].iloc[i] * 0.97,
                    'take_profit': df['close'].iloc[i] * 1.05
                })
            elif vo.iloc[i] < -threshold and df['close'].iloc[i] < df['close'].iloc[i-1]:
                signals.append({
                    'date': df.index[i],
                    'signal': 'SELL',
                    'confidence': 0.65,
                    'entry': df['close'].iloc[i],
                    'stop_loss': df['close'].iloc[i] * 1.03,
                    'take_profit': df['close'].iloc[i] * 0.95
                })
        
        return signals
    
    def _test_vwap_volume_params(self, df, period, std_dev, volume_threshold):
        """Test VWAP with Volume parameters"""
        signals = []
        if 'volume' not in df.columns:
            return signals
        
        # Calculate VWAP
        vwap = (df['close'] * df['volume']).rolling(period).sum() / df['volume'].rolling(period).sum()
        std = df['close'].rolling(period).std()
        volume_ma = df['volume'].rolling(period).mean()
        
        for i in range(period, len(df)):
            volume_surge = df['volume'].iloc[i] > volume_ma.iloc[i] * volume_threshold
            
            if df['close'].iloc[i] < vwap.iloc[i] - std_dev * std.iloc[i] and volume_surge:
                signals.append({
                    'date': df.index[i],
                    'signal': 'BUY',
                    'confidence': 0.7,
                    'entry': df['close'].iloc[i],
                    'stop_loss': df['close'].iloc[i] * 0.97,
                    'take_profit': vwap.iloc[i]
                })
            elif df['close'].iloc[i] > vwap.iloc[i] + std_dev * std.iloc[i] and volume_surge:
                signals.append({
                    'date': df.index[i],
                    'signal': 'SELL',
                    'confidence': 0.7,
                    'entry': df['close'].iloc[i],
                    'stop_loss': df['close'].iloc[i] * 1.03,
                    'take_profit': vwap.iloc[i]
                })
        
        return signals
    
    def _test_cmf_params(self, df, period, threshold):
        """Test Chaikin Money Flow parameters"""
        signals = []
        if 'volume' not in df.columns:
            return signals
        
        # Calculate CMF
        mfm = ((df['close'] - df['low']) - (df['high'] - df['close'])) / (df['high'] - df['low'])
        mfv = mfm * df['volume']
        cmf = mfv.rolling(period).sum() / df['volume'].rolling(period).sum()
        
        for i in range(period, len(df)):
            if cmf.iloc[i] > threshold and cmf.iloc[i] > cmf.iloc[i-1]:
                signals.append({
                    'date': df.index[i],
                    'signal': 'BUY',
                    'confidence': 0.7,
                    'entry': df['close'].iloc[i],
                    'stop_loss': df['close'].iloc[i] * 0.97,
                    'take_profit': df['close'].iloc[i] * 1.05
                })
            elif cmf.iloc[i] < -threshold and cmf.iloc[i] < cmf.iloc[i-1]:
                signals.append({
                    'date': df.index[i],
                    'signal': 'SELL',
                    'confidence': 0.7,
                    'entry': df['close'].iloc[i],
                    'stop_loss': df['close'].iloc[i] * 1.03,
                    'take_profit': df['close'].iloc[i] * 0.95
                })
        
        return signals
    
    def _test_eom_params(self, df, period, signal_period):
        """Test Ease of Movement parameters"""
        signals = []
        if 'volume' not in df.columns:
            return signals
        
        # Calculate EOM
        distance = ((df['high'] + df['low']) / 2) - ((df['high'].shift(1) + df['low'].shift(1)) / 2)
        box_ratio = (df['volume'] / 1000000) / (df['high'] - df['low'])
        eom = distance / box_ratio
        eom_signal = eom.rolling(signal_period).mean()
        
        for i in range(max(period, signal_period), len(df)):
            if eom_signal.iloc[i] > 0 and eom_signal.iloc[i] > eom_signal.iloc[i-1]:
                signals.append({
                    'date': df.index[i],
                    'signal': 'BUY',
                    'confidence': 0.6,
                    'entry': df['close'].iloc[i],
                    'stop_loss': df['close'].iloc[i] * 0.97,
                    'take_profit': df['close'].iloc[i] * 1.05
                })
            elif eom_signal.iloc[i] < 0 and eom_signal.iloc[i] < eom_signal.iloc[i-1]:
                signals.append({
                    'date': df.index[i],
                    'signal': 'SELL',
                    'confidence': 0.6,
                    'entry': df['close'].iloc[i],
                    'stop_loss': df['close'].iloc[i] * 1.03,
                    'take_profit': df['close'].iloc[i] * 0.95
                })
        
        return signals
    
    def _test_vpt_params(self, df, signal_period):
        """Test Volume Price Trend parameters"""
        signals = []
        if 'volume' not in df.columns:
            return signals
        
        # Calculate VPT
        vpt = df['volume'] * ((df['close'] - df['close'].shift(1)) / df['close'].shift(1))
        vpt_cum = vpt.cumsum()
        vpt_signal = vpt_cum.rolling(signal_period).mean()
        
        for i in range(1, len(df)):
            if vpt_cum.iloc[i-1] <= vpt_signal.iloc[i-1] and vpt_cum.iloc[i] > vpt_signal.iloc[i]:
                signals.append({
                    'date': df.index[i],
                    'signal': 'BUY',
                    'confidence': 0.65,
                    'entry': df['close'].iloc[i],
                    'stop_loss': df['close'].iloc[i] * 0.97,
                    'take_profit': df['close'].iloc[i] * 1.05
                })
            elif vpt_cum.iloc[i-1] >= vpt_signal.iloc[i-1] and vpt_cum.iloc[i] < vpt_signal.iloc[i]:
                signals.append({
                    'date': df.index[i],
                    'signal': 'SELL',
                    'confidence': 0.65,
                    'entry': df['close'].iloc[i],
                    'stop_loss': df['close'].iloc[i] * 1.03,
                    'take_profit': df['close'].iloc[i] * 0.95
                })
        
        return signals
    
    def _test_awesome_params(self, df, fast_period, slow_period, signal_period):
        """Test Awesome Oscillator parameters"""
        signals = []
        # Calculate Awesome Oscillator
        median = (df['high'] + df['low']) / 2
        fast_ma = median.rolling(fast_period).mean()
        slow_ma = median.rolling(slow_period).mean()
        ao = fast_ma - slow_ma
        
        for i in range(slow_period, len(df)):
            if ao.iloc[i] > 0 and ao.iloc[i-1] <= 0:
                signals.append({
                    'date': df.index[i],
                    'signal': 'BUY',
                    'confidence': 0.7,
                    'entry': df['close'].iloc[i],
                    'stop_loss': df['close'].iloc[i] * 0.97,
                    'take_profit': df['close'].iloc[i] * 1.06
                })
            elif ao.iloc[i] < 0 and ao.iloc[i-1] >= 0:
                signals.append({
                    'date': df.index[i],
                    'signal': 'SELL',
                    'confidence': 0.7,
                    'entry': df['close'].iloc[i],
                    'stop_loss': df['close'].iloc[i] * 1.03,
                    'take_profit': df['close'].iloc[i] * 0.94
                })
        
        return signals
    
    def _test_acceleration_params(self, df, fast_period, slow_period, signal_period):
        """Test Acceleration Oscillator parameters"""
        # Acceleration is AO - signal line of AO
        median = (df['high'] + df['low']) / 2
        fast_ma = median.rolling(fast_period).mean()
        slow_ma = median.rolling(slow_period).mean()
        ao = fast_ma - slow_ma
        ao_signal = ao.rolling(signal_period).mean()
        ac = ao - ao_signal
        
        for i in range(max(slow_period, signal_period), len(df)):
            if ac.iloc[i] > 0 and ac.iloc[i-1] <= 0:
                signals.append({
                    'date': df.index[i],
                    'signal': 'BUY',
                    'confidence': 0.65,
                    'entry': df['close'].iloc[i],
                    'stop_loss': df['close'].iloc[i] * 0.97,
                    'take_profit': df['close'].iloc[i] * 1.05
                })
            elif ac.iloc[i] < 0 and ac.iloc[i-1] >= 0:
                signals.append({
                    'date': df.index[i],
                    'signal': 'SELL',
                    'confidence': 0.65,
                    'entry': df['close'].iloc[i],
                    'stop_loss': df['close'].iloc[i] * 1.03,
                    'take_profit': df['close'].iloc[i] * 0.95
                })
        
        return signals
    
    def _test_rvgi_params(self, df, period, signal_period):
        """Test Relative Vigor Index parameters"""
        signals = []
        # Simplified RVGI calculation
        numerator = (df['close'] - df['open']) + 2*(df['close'].shift(1) - df['open'].shift(1)) + 2*(df['close'].shift(2) - df['open'].shift(2)) + (df['close'].shift(3) - df['open'].shift(3))
        denominator = (df['high'] - df['low']) + 2*(df['high'].shift(1) - df['low'].shift(1)) + 2*(df['high'].shift(2) - df['low'].shift(2)) + (df['high'].shift(3) - df['low'].shift(3))
        rvgi = numerator.rolling(period).sum() / denominator.rolling(period).sum()
        rvgi_signal = rvgi.rolling(signal_period).mean()
        
        for i in range(max(period, signal_period) + 3, len(df)):
            if rvgi.iloc[i] > rvgi_signal.iloc[i] and rvgi.iloc[i-1] <= rvgi_signal.iloc[i-1]:
                signals.append({
                    'date': df.index[i],
                    'signal': 'BUY',
                    'confidence': 0.65,
                    'entry': df['close'].iloc[i],
                    'stop_loss': df['close'].iloc[i] * 0.97,
                    'take_profit': df['close'].iloc[i] * 1.05
                })
            elif rvgi.iloc[i] < rvgi_signal.iloc[i] and rvgi.iloc[i-1] >= rvgi_signal.iloc[i-1]:
                signals.append({
                    'date': df.index[i],
                    'signal': 'SELL',
                    'confidence': 0.65,
                    'entry': df['close'].iloc[i],
                    'stop_loss': df['close'].iloc[i] * 1.03,
                    'take_profit': df['close'].iloc[i] * 0.95
                })
        
        return signals
    
    def _test_trix_params(self, df, period, signal_period):
        """Test TRIX parameters"""
        signals = []
        # Triple exponential smoothing
        ema1 = df['close'].ewm(span=period, adjust=False).mean()
        ema2 = ema1.ewm(span=period, adjust=False).mean()
        ema3 = ema2.ewm(span=period, adjust=False).mean()
        trix = ema3.pct_change() * 100
        trix_signal = trix.rolling(signal_period).mean()
        
        for i in range(period * 3 + signal_period, len(df)):
            if trix.iloc[i] > trix_signal.iloc[i] and trix.iloc[i-1] <= trix_signal.iloc[i-1]:
                signals.append({
                    'date': df.index[i],
                    'signal': 'BUY',
                    'confidence': 0.7,
                    'entry': df['close'].iloc[i],
                    'stop_loss': df['close'].iloc[i] * 0.97,
                    'take_profit': df['close'].iloc[i] * 1.06
                })
            elif trix.iloc[i] < trix_signal.iloc[i] and trix.iloc[i-1] >= trix_signal.iloc[i-1]:
                signals.append({
                    'date': df.index[i],
                    'signal': 'SELL',
                    'confidence': 0.7,
                    'entry': df['close'].iloc[i],
                    'stop_loss': df['close'].iloc[i] * 1.03,
                    'take_profit': df['close'].iloc[i] * 0.94
                })
        
        return signals
    
    def _test_cmo_params(self, df, period, oversold, overbought):
        """Test Chande Momentum Oscillator parameters"""
        signals = []
        # Calculate CMO
        change = df['close'].diff()
        gain = change.clip(lower=0)
        loss = -change.clip(upper=0)
        avg_gain = gain.rolling(period).mean()
        avg_loss = loss.rolling(period).mean()
        cmo = 100 * (avg_gain - avg_loss) / (avg_gain + avg_loss)
        
        for i in range(period, len(df)):
            if cmo.iloc[i] < oversold:
                signals.append({
                    'date': df.index[i],
                    'signal': 'BUY',
                    'confidence': 0.7,
                    'entry': df['close'].iloc[i],
                    'stop_loss': df['close'].iloc[i] * 0.97,
                    'take_profit': df['close'].iloc[i] * 1.05
                })
            elif cmo.iloc[i] > overbought:
                signals.append({
                    'date': df.index[i],
                    'signal': 'SELL',
                    'confidence': 0.7,
                    'entry': df['close'].iloc[i],
                    'stop_loss': df['close'].iloc[i] * 1.03,
                    'take_profit': df['close'].iloc[i] * 0.95
                })
        
        return signals
    
    def _test_doji_params(self, df, body_threshold, wick_threshold):
        """Test Doji pattern parameters"""
        signals = []
        body = abs(df['close'] - df['open'])
        range_val = df['high'] - df['low']
        doji = (body < range_val * body_threshold) & (range_val > 0)
        
        for i in range(1, len(df)):
            if doji.iloc[i]:
                # Doji detected - potential reversal
                if df['close'].iloc[i-1] > df['open'].iloc[i-1]:
                    # Previous candle was bullish - potential reversal down
                    signals.append({
                        'date': df.index[i],
                        'signal': 'SELL',
                        'confidence': 0.5,
                        'entry': df['close'].iloc[i],
                        'stop_loss': df['high'].iloc[i],
                        'take_profit': df['low'].iloc[i]
                    })
                else:
                    # Previous candle was bearish - potential reversal up
                    signals.append({
                        'date': df.index[i],
                        'signal': 'BUY',
                        'confidence': 0.5,
                        'entry': df['close'].iloc[i],
                        'stop_loss': df['low'].iloc[i],
                        'take_profit': df['high'].iloc[i]
                    })
        
        return signals
    
    def _test_hammer_params(self, df, body_threshold, wick_ratio, trend_period):
        """Test Hammer pattern parameters"""
        signals = []
        body = abs(df['close'] - df['open'])
        lower_wick = df[['open', 'close']].min(axis=1) - df['low']
        upper_wick = df['high'] - df[['open', 'close']].max(axis=1)
        
        # Hammer: small body, long lower wick, small upper wick
        hammer = (body < (df['high'] - df['low']) * body_threshold) & \
                 (lower_wick > body * wick_ratio) & \
                 (upper_wick < body)
        
        for i in range(trend_period, len(df)):
            if hammer.iloc[i]:
                # Check for downtrend
                if df['close'].iloc[i-trend_period:i].mean() < df['close'].iloc[i-trend_period]:
                    signals.append({
                        'date': df.index[i],
                        'signal': 'BUY',
                        'confidence': 0.7,
                        'entry': df['close'].iloc[i],
                        'stop_loss': df['low'].iloc[i],
                        'take_profit': df['close'].iloc[i] * 1.03
                    })
        
        return signals
    
    def _test_engulfing_params(self, df, body_ratio, trend_period):
        """Test Engulfing pattern parameters"""
        signals = []
        body1 = abs(df['close'].shift(1) - df['open'].shift(1))
        body2 = abs(df['close'] - df['open'])
        
        # Bullish engulfing
        bullish = (df['close'].shift(1) < df['open'].shift(1)) & \
                  (df['close'] > df['open']) & \
                  (df['open'] < df['close'].shift(1)) & \
                  (df['close'] > df['open'].shift(1)) & \
                  (body2 > body1 * body_ratio)
        
        # Bearish engulfing
        bearish = (df['close'].shift(1) > df['open'].shift(1)) & \
                   (df['close'] < df['open']) & \
                   (df['open'] > df['close'].shift(1)) & \
                   (df['close'] < df['open'].shift(1)) & \
                   (body2 > body1 * body_ratio)
        
        for i in range(trend_period, len(df)):
            if bullish.iloc[i]:
                signals.append({
                    'date': df.index[i],
                    'signal': 'BUY',
                    'confidence': 0.7,
                    'entry': df['close'].iloc[i],
                    'stop_loss': df['low'].iloc[i],
                    'take_profit': df['close'].iloc[i] * 1.05
                })
            elif bearish.iloc[i]:
                signals.append({
                    'date': df.index[i],
                    'signal': 'SELL',
                    'confidence': 0.7,
                    'entry': df['close'].iloc[i],
                    'stop_loss': df['high'].iloc[i],
                    'take_profit': df['close'].iloc[i] * 0.95
                })
        
        return signals
    
    def _test_morning_star_params(self, df, body_ratio, gap_threshold):
        """Test Morning Star pattern parameters"""
        signals = []
        # 3-candle pattern
        if len(df) < 3:
            return signals
        
        for i in range(2, len(df)):
            # First candle: bearish
            if df['close'].iloc[i-2] >= df['open'].iloc[i-2]:
                continue
            
            # Second candle: small body
            body2 = abs(df['close'].iloc[i-1] - df['open'].iloc[i-1])
            range2 = df['high'].iloc[i-1] - df['low'].iloc[i-1]
            if body2 > range2 * body_ratio:
                continue
            
            # Gap down
            if df['high'].iloc[i-1] >= df['close'].iloc[i-2] * (1 - gap_threshold):
                continue
            
            # Third candle: bullish
            if df['close'].iloc[i] <= df['open'].iloc[i]:
                continue
            
            if df['close'].iloc[i] > (df['close'].iloc[i-2] + df['open'].iloc[i-2]) / 2:
                signals.append({
                    'date': df.index[i],
                    'signal': 'BUY',
                    'confidence': 0.75,
                    'entry': df['close'].iloc[i],
                    'stop_loss': min(df['low'].iloc[i-1], df['low'].iloc[i]),
                    'take_profit': df['close'].iloc[i] * 1.06
                })
        
        return signals
    
    def _test_evening_star_params(self, df, body_ratio, gap_threshold):
        """Test Evening Star pattern parameters"""
        signals = []
        # 3-candle pattern
        if len(df) < 3:
            return signals
        
        for i in range(2, len(df)):
            # First candle: bullish
            if df['close'].iloc[i-2] <= df['open'].iloc[i-2]:
                continue
            
            # Second candle: small body
            body2 = abs(df['close'].iloc[i-1] - df['open'].iloc[i-1])
            range2 = df['high'].iloc[i-1] - df['low'].iloc[i-1]
            if body2 > range2 * body_ratio:
                continue
            
            # Gap up
            if df['low'].iloc[i-1] <= df['close'].iloc[i-2] * (1 + gap_threshold):
                continue
            
            # Third candle: bearish
            if df['close'].iloc[i] >= df['open'].iloc[i]:
                continue
            
            if df['close'].iloc[i] < (df['close'].iloc[i-2] + df['open'].iloc[i-2]) / 2:
                signals.append({
                    'date': df.index[i],
                    'signal': 'SELL',
                    'confidence': 0.75,
                    'entry': df['close'].iloc[i],
                    'stop_loss': max(df['high'].iloc[i-1], df['high'].iloc[i]),
                    'take_profit': df['close'].iloc[i] * 0.94
                })
        
        return signals
    
    def _test_three_white_params(self, df, body_min, wick_max):
        """Test Three White Soldiers parameters"""
        signals = []
        if len(df) < 3:
            return signals
        
        for i in range(2, len(df)):
            # Three consecutive bullish candles
            bullish = True
            for j in range(3):
                if df['close'].iloc[i-j] <= df['open'].iloc[i-j]:
                    bullish = False
                    break
                body = df['close'].iloc[i-j] - df['open'].iloc[i-j]
                if body < body_min * df['close'].iloc[i-j]:
                    bullish = False
                    break
                upper_wick = df['high'].iloc[i-j] - df['close'].iloc[i-j]
                if upper_wick > body * wick_max:
                    bullish = False
                    break
            
            if bullish:
                signals.append({
                    'date': df.index[i],
                    'signal': 'BUY',
                    'confidence': 0.8,
                    'entry': df['close'].iloc[i],
                    'stop_loss': min(df['low'].iloc[i-2:i+1]),
                    'take_profit': df['close'].iloc[i] * 1.08
                })
        
        return signals
    
    def _test_three_black_params(self, df, body_min, wick_max):
        """Test Three Black Crows parameters"""
        signals = []
        if len(df) < 3:
            return signals
        
        for i in range(2, len(df)):
            # Three consecutive bearish candles
            bearish = True
            for j in range(3):
                if df['close'].iloc[i-j] >= df['open'].iloc[i-j]:
                    bearish = False
                    break
                body = df['open'].iloc[i-j] - df['close'].iloc[i-j]
                if body < body_min * df['close'].iloc[i-j]:
                    bearish = False
                    break
                lower_wick = df['close'].iloc[i-j] - df['low'].iloc[i-j]
                if lower_wick > body * wick_max:
                    bearish = False
                    break
            
            if bearish:
                signals.append({
                    'date': df.index[i],
                    'signal': 'SELL',
                    'confidence': 0.8,
                    'entry': df['close'].iloc[i],
                    'stop_loss': max(df['high'].iloc[i-2:i+1]),
                    'take_profit': df['close'].iloc[i] * 0.92
                })
        
        return signals
    
    def _test_pivot_points_params(self, df, pivot_type, lookback):
        """Test Pivot Points parameters"""
        signals = []
        if len(df) < lookback + 1:
            return signals
        
        for i in range(lookback, len(df)):
            high = df['high'].iloc[i-lookback:i].max()
            low = df['low'].iloc[i-lookback:i].min()
            close = df['close'].iloc[i-1]
            
            if pivot_type == 'classic':
                pivot = (high + low + close) / 3
                r1 = 2 * pivot - low
                s1 = 2 * pivot - high
            elif pivot_type == 'fibonacci':
                pivot = (high + low + close) / 3
                r1 = pivot + 0.382 * (high - low)
                s1 = pivot - 0.382 * (high - low)
            else:  # woodie
                pivot = (high + low + 2*close) / 4
                r1 = 2 * pivot - low
                s1 = 2 * pivot - high
            
            if df['close'].iloc[i] > r1:
                signals.append({
                    'date': df.index[i],
                    'signal': 'BUY',
                    'confidence': 0.6,
                    'entry': df['close'].iloc[i],
                    'stop_loss': pivot,
                    'take_profit': r1 * 1.02
                })
            elif df['close'].iloc[i] < s1:
                signals.append({
                    'date': df.index[i],
                    'signal': 'SELL',
                    'confidence': 0.6,
                    'entry': df['close'].iloc[i],
                    'stop_loss': pivot,
                    'take_profit': s1 * 0.98
                })
        
        return signals
    
    def _test_fibonacci_params(self, df, lookback, levels):
        """Test Fibonacci Retracement parameters"""
        signals = []
        if len(df) < lookback:
            return signals
        
        for i in range(lookback, len(df)):
            high = df['high'].iloc[i-lookback:i].max()
            low = df['low'].iloc[i-lookback:i].min()
            diff = high - low
            
            for level in levels:
                fib_level = high - level * diff
                tolerance = diff * 0.01
                
                if abs(df['close'].iloc[i] - fib_level) < tolerance:
                    if level < 0.5:  # Support level
                        signals.append({
                            'date': df.index[i],
                            'signal': 'BUY',
                            'confidence': 0.65,
                            'entry': df['close'].iloc[i],
                            'stop_loss': fib_level * 0.98,
                            'take_profit': high
                        })
                    else:  # Resistance level
                        signals.append({
                            'date': df.index[i],
                            'signal': 'SELL',
                            'confidence': 0.65,
                            'entry': df['close'].iloc[i],
                            'stop_loss': fib_level * 1.02,
                            'take_profit': low
                        })
        
        return signals
    
    def _test_supply_demand_params(self, df, zone_period, strength_bars, touch_tolerance):
        """Test Supply and Demand zones parameters"""
        signals = []
        # Simplified supply/demand detection
        for i in range(zone_period * 2, len(df)):
            # Demand zone (support)
            demand_low = df['low'].iloc[i-zone_period:i].min()
            demand_high = demand_low * (1 + touch_tolerance)
            
            # Supply zone (resistance)
            supply_high = df['high'].iloc[i-zone_period:i].max()
            supply_low = supply_high * (1 - touch_tolerance)
            
            # Check if price is in demand zone
            if demand_low <= df['close'].iloc[i] <= demand_high:
                # Count touches
                touches = sum((demand_low <= df['low'].iloc[i-strength_bars:i]) & 
                            (df['low'].iloc[i-strength_bars:i] <= demand_high))
                if touches >= 2:
                    signals.append({
                        'date': df.index[i],
                        'signal': 'BUY',
                        'confidence': 0.65,
                        'entry': df['close'].iloc[i],
                        'stop_loss': demand_low * 0.98,
                        'take_profit': supply_high
                    })
            
            # Check if price is in supply zone
            if supply_low <= df['close'].iloc[i] <= supply_high:
                touches = sum((supply_low <= df['high'].iloc[i-strength_bars:i]) & 
                            (df['high'].iloc[i-strength_bars:i] <= supply_high))
                if touches >= 2:
                    signals.append({
                        'date': df.index[i],
                        'signal': 'SELL',
                        'confidence': 0.65,
                        'entry': df['close'].iloc[i],
                        'stop_loss': supply_high * 1.02,
                        'take_profit': demand_low
                    })
        
        return signals
    
    def _test_rsi_macd_combination_params(self, df, rsi_period, rsi_oversold, rsi_overbought,
                                         macd_fast, macd_slow, macd_signal):
        """Test RSI + MACD combination parameters"""
        signals = []
        # Get RSI and MACD signals
        rsi_signals = self._test_rsi_params(df, rsi_oversold, rsi_overbought, rsi_period)
        macd_signals = self._test_macd_params(df, macd_fast, macd_slow, macd_signal)
        
        # Combine signals - only take when both agree
        if rsi_signals and macd_signals:
            last_rsi = rsi_signals[-1]
            last_macd = macd_signals[-1]
            
            if last_rsi['signal'] == last_macd['signal']:
                confidence = (last_rsi['confidence'] + last_macd['confidence']) / 2
                signals.append({
                    'date': df.index[-1],
                    'signal': last_rsi['signal'],
                    'confidence': confidence * 1.2,  # Boost confidence when both agree
                    'entry': df['close'].iloc[-1],
                    'stop_loss': df['close'].iloc[-1] * (0.97 if last_rsi['signal'] == 'BUY' else 1.03),
                    'take_profit': df['close'].iloc[-1] * (1.07 if last_rsi['signal'] == 'BUY' else 0.93)
                })
        
        return signals
    
    def _test_bollinger_rsi_params(self, df, bb_period, bb_std, rsi_period, rsi_oversold, rsi_overbought):
        """Test Bollinger + RSI combination parameters"""
        signals = []
        bb_signals = self._test_bollinger_params(df, bb_period, bb_std, bb_std * 1.5)
        rsi_signals = self._test_rsi_params(df, rsi_oversold, rsi_overbought, rsi_period)
        
        if bb_signals and rsi_signals:
            last_bb = bb_signals[-1]
            last_rsi = rsi_signals[-1]
            
            if last_bb['signal'] == last_rsi['signal']:
                signals.append({
                    'date': df.index[-1],
                    'signal': last_bb['signal'],
                    'confidence': 0.8,
                    'entry': df['close'].iloc[-1],
                    'stop_loss': last_bb['stop_loss'],
                    'take_profit': last_bb['take_profit']
                })
        
        return signals
    
    def _test_adx_di_params(self, df, period, adx_threshold, di_threshold):
        """Test ADX + DI combination parameters"""
        signals = []
        if 'adx' not in df.columns or 'di_plus' not in df.columns or 'di_minus' not in df.columns:
            return signals
        
        adx = df['adx']
        di_plus = df['di_plus']
        di_minus = df['di_minus']
        
        for i in range(period, len(df)):
            if adx.iloc[i] > adx_threshold:
                if di_plus.iloc[i] > di_threshold and di_plus.iloc[i] > di_minus.iloc[i]:
                    signals.append({
                        'date': df.index[i],
                        'signal': 'BUY',
                        'confidence': 0.75,
                        'entry': df['close'].iloc[i],
                        'stop_loss': df['close'].iloc[i] * 0.96,
                        'take_profit': df['close'].iloc[i] * 1.08
                    })
                elif di_minus.iloc[i] > di_threshold and di_minus.iloc[i] > di_plus.iloc[i]:
                    signals.append({
                        'date': df.index[i],
                        'signal': 'SELL',
                        'confidence': 0.75,
                        'entry': df['close'].iloc[i],
                        'stop_loss': df['close'].iloc[i] * 1.04,
                        'take_profit': df['close'].iloc[i] * 0.92
                    })
        
        return signals
    
    def _test_volume_breakout_params(self, df, volume_period, volume_multiplier, price_period, price_threshold):
        """Test Volume Breakout parameters"""
        signals = []
        if 'volume' not in df.columns:
            return signals
        
        volume_ma = df['volume'].rolling(volume_period).mean()
        price_ma = df['close'].rolling(price_period).mean()
        
        for i in range(max(volume_period, price_period), len(df)):
            volume_surge = df['volume'].iloc[i] > volume_ma.iloc[i] * volume_multiplier
            price_move = abs(df['close'].iloc[i] - price_ma.iloc[i]) / price_ma.iloc[i]
            
            if volume_surge and price_move > price_threshold:
                if df['close'].iloc[i] > price_ma.iloc[i]:
                    signals.append({
                        'date': df.index[i],
                        'signal': 'BUY',
                        'confidence': 0.7,
                        'entry': df['close'].iloc[i],
                        'stop_loss': price_ma.iloc[i],
                        'take_profit': df['close'].iloc[i] * 1.05
                    })
                else:
                    signals.append({
                        'date': df.index[i],
                        'signal': 'SELL',
                        'confidence': 0.7,
                        'entry': df['close'].iloc[i],
                        'stop_loss': price_ma.iloc[i],
                        'take_profit': df['close'].iloc[i] * 0.95
                    })
        
        return signals
    
    def _test_momentum_reversal_params(self, df, rsi_period, rsi_extreme, lookback, reversal_threshold):
        """Test Momentum Reversal parameters"""
        signals = []
        if 'rsi' not in df.columns:
            return signals
        
        rsi = df['rsi']
        
        for i in range(max(rsi_period, lookback), len(df)):
            # Check for overbought/oversold
            if rsi.iloc[i] > rsi_extreme:
                # Look for reversal confirmation
                price_change = (df['close'].iloc[i] - df['close'].iloc[i-lookback]) / df['close'].iloc[i-lookback] * 100
                if price_change < reversal_threshold:
                    signals.append({
                        'date': df.index[i],
                        'signal': 'SELL',
                        'confidence': 0.7,
                        'entry': df['close'].iloc[i],
                        'stop_loss': df['high'].iloc[i],
                        'take_profit': df['close'].iloc[i] * 0.95
                    })
            elif rsi.iloc[i] < (100 - rsi_extreme):
                price_change = (df['close'].iloc[i] - df['close'].iloc[i-lookback]) / df['close'].iloc[i-lookback] * 100
                if price_change > -reversal_threshold:
                    signals.append({
                        'date': df.index[i],
                        'signal': 'BUY',
                        'confidence': 0.7,
                        'entry': df['close'].iloc[i],
                        'stop_loss': df['low'].iloc[i],
                        'take_profit': df['close'].iloc[i] * 1.05
                    })
        
        return signals