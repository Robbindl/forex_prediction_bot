"""
BacktestEngine — backtesting and strategy optimisation extracted from UltimateTradingSystem.
Receives a system reference at init so it can call fetch_historical_data, etc.
"""

import json
import os
import time
import itertools
import numpy as np
import pandas as pd
from typing import Dict, List, Optional
from datetime import datetime, timedelta
from pathlib import Path
from logger import logger


class BacktestEngine:
    """Backtesting and parameter optimisation for all strategies."""

    def __init__(self, system):
        """
        Args:
            system: UltimateTradingSystem instance — used for fetch_historical_data,
                    add_technical_indicators, strategy methods, backtester object, etc.
        """
        self.system = system
        # Convenience aliases so extracted method bodies work without change
        self.backtester = system.backtester
        self.strategy_optimizer = system.strategy_optimizer
        logger.info("BacktestEngine initialised")

    # ── property delegates so self.X works in method bodies ──────────────────
    def fetch_historical_data(self, *a, **kw):
        return self.system.fetch_historical_data(*a, **kw)

    def add_technical_indicators(self, df):
        return self.system.add_technical_indicators(df)

    def get_asset_list(self):
        return self.system.get_asset_list()

    def custom_rsi_strategy(self, *a, **kw):
        return self.system.strategy_engine.custom_rsi_strategy(*a, **kw)

    def custom_macd_strategy(self, *a, **kw):
        return self.system.strategy_engine.custom_macd_strategy(*a, **kw)

    @property
    def strategies(self):
        return self.system.strategies

    @property
    def current_strategy(self):
        return self.system.current_strategy

def backtest_asset(self, asset: str, lookback_days: int = 365):
    """Backtest a single asset with all strategies"""
    logger.info(f"Backtesting {asset}...")
    
    # Fetch data
    df = self.fetch_historical_data(asset, lookback_days)
    if df.empty:
        logger.warning(f"No data for {asset}")
        return None
    
    # Add indicators
    df = self.add_technical_indicators(df)
    
    results = []
    
    # Test each strategy
    for strategy_name, strategy_func in self.strategies.items():
        logger.debug(f"Testing {strategy_name} for {asset}")
        
        # Generate signals
        signals = strategy_func(df)
        if not signals:
            continue
        
        signals_df = pd.DataFrame(signals)
        
        # Run backtest
        results_obj = self.backtester.run_backtest(df, signals_df)
        
        # Store results
        results.append({
            'asset': asset,
            'strategy': strategy_name,
            'trades': results_obj.total_trades,
            'win_rate': results_obj.win_rate,
            'total_return': results_obj.total_return_pct,
            'profit_factor': results_obj.profit_factor,
            'sharpe': results_obj.sharpe_ratio,
            'max_dd': results_obj.max_drawdown
        })
        
        # Save detailed results
        # Create a safe filename by replacing problematic characters
        safe_asset = asset.replace('/', '_').replace('\\', '_').replace(':', '_')
        safe_filename = f"backtest_results/{safe_asset}_{strategy_name}.csv"
        try:
             self.backtester.export_trades(safe_filename)
             logger.info(f"Saved backtest results to {safe_filename}")
        except Exception as e:
             logger.warning(f"Could not save file {safe_filename}: {e}")
    
    # Display results
    if results:
        results_df = pd.DataFrame(results)
        results_df = results_df.sort_values('profit_factor', ascending=False)
        
        logger.info("="*30)
        logger.info(f"RESULTS FOR {asset}")
        logger.info("="*30)
        
        # Log results as debug to avoid console spam
        for _, row in results_df.iterrows():
            logger.debug(f"  {row['strategy']}: WR={row['win_rate']:.1%}, PF={row['profit_factor']:.2f}, Sharpe={row['sharpe']:.2f}")
        
        # Find best strategy
        best = results_df.iloc[0]
        logger.info(f"BEST STRATEGY: {best['strategy']}")
        logger.info(f"   Win Rate: {best['win_rate']:.1%}")
        logger.info(f"   Return: {best['total_return']:.1f}%")
        logger.info(f"   Profit Factor: {best['profit_factor']:.2f}")
        
        # Save summary
        safe_asset = asset.replace('/', '_').replace('\\', '_').replace(':', '_')
        summary_filename = f'backtest_results/{safe_asset}_summary.csv'
        try:
            results_df.to_csv(summary_filename, index=False)
            logger.info(f"Summary saved to {summary_filename}")
        except Exception as e:
            logger.warning(f"Could not save summary: {e}")
    
        return results_df
    return None

def backtest_all_strategies(self, assets: List[str], lookback_days: int = 365):
    """Backtest all strategies on multiple assets"""
    logger.info("="*60)
    logger.info(" COMPREHENSIVE STRATEGY BACKTEST")
    logger.info("="*60)
    
    all_results = []
    
    for asset in assets:
        result = self.backtest_asset(asset, lookback_days)
        if result is not None:
            all_results.append(result)
    
    # Combine all results
    if all_results:
        combined = pd.concat(all_results)
        combined.to_csv('backtest_results/all_strategies_comparison.csv', index=False)
        
        logger.info("="*30)
        logger.info("OVERALL BEST STRATEGIES")
        logger.info("="*30)
        
        # Group by strategy and find average
        avg_results = combined.groupby('strategy').agg({
            'win_rate': 'mean',
            'total_return': 'mean',
            'profit_factor': 'mean',
            'sharpe': 'mean'
        }).sort_values('profit_factor', ascending=False)
        
        # Log top strategies
        for strategy, row in avg_results.head(5).iterrows():
            logger.info(f"  {strategy}: PF={row['profit_factor']:.2f}, Sharpe={row['sharpe']:.2f}")
        
        # Set best strategy as default
        best_strategy = avg_results.index[0]
        self.current_strategy = best_strategy
        logger.info(f"Default strategy set to: {self.current_strategy}")

def optimize_strategy(self, asset: str, strategy: str, lookback_days: int = 365):
    """Optimize strategy parameters"""
    logger.info(f"Optimizing {strategy} for {asset}...")
    
    df = self.fetch_historical_data(asset, lookback_days)
    if df.empty:
        logger.warning(f"No data for {asset}")
        return
    
    df = self.add_technical_indicators(df)
    
    if strategy == 'rsi':
        # Optimize RSI levels
        results = []
        for oversold in [20, 25, 30, 35]:
            for overbought in [65, 70, 75, 80]:
                # Custom RSI logic with these levels
                signals = self.custom_rsi_strategy(df, oversold, overbought)
                if signals:
                    signals_df = pd.DataFrame(signals)
                    res = self.backtester.run_backtest(df, signals_df)
                    results.append({
                        'oversold': oversold,
                        'overbought': overbought,
                        'win_rate': res.win_rate,
                        'return': res.total_return_pct,
                        'profit_factor': res.profit_factor
                    })
        
        if results:
            results_df = pd.DataFrame(results)
            best = results_df.loc[results_df['profit_factor'].idxmax()]
            logger.info(f"Best RSI settings for {asset}:")
            logger.info(f"   Oversold: {best['oversold']}")
            logger.info(f"   Overbought: {best['overbought']}")
            logger.info(f"   Profit Factor: {best['profit_factor']:.2f}")
            
            results_df.to_csv(f'backtest_results/{strategy}_optimization.csv', index=False)
    
    elif strategy == 'macd':
        # Optimize MACD parameters
        results = []
        for fast in [8, 12, 16]:
            for slow in [20, 26, 30]:
                for signal in [7, 9, 11]:
                    signals = self.custom_macd_strategy(df, fast, slow, signal)
                    if signals:
                        signals_df = pd.DataFrame(signals)
                        res = self.backtester.run_backtest(df, signals_df)
                        results.append({
                            'fast': fast,
                            'slow': slow,
                            'signal': signal,
                            'win_rate': res.win_rate,
                            'return': res.total_return_pct,
                            'profit_factor': res.profit_factor
                        })
        
        if results:
            results_df = pd.DataFrame(results)
            best = results_df.loc[results_df['profit_factor'].idxmax()]
            logger.info(f"Best MACD settings for {asset}:")
            logger.info(f"   Fast: {best['fast']}")
            logger.info(f"   Slow: {best['slow']}")
            logger.info(f"   Signal: {best['signal']}")
            logger.info(f"   Profit Factor: {best['profit_factor']:.2f}")
            
            results_df.to_csv(f'backtest_results/{strategy}_optimization.csv', index=False)

def optimize_all_strategies(self, asset: str, lookback_days: int = 365):
    """
    Optimize ALL 50+ strategies for a given asset
    Returns comprehensive optimization results for every strategy
    """
    logger.info(f"="*70)
    logger.info(f"OPTIMIZING ALL 50+ STRATEGIES FOR {asset}")
    logger.info(f"="*70)
    
    # Fetch data
    df = self.fetch_historical_data(asset, lookback_days)
    if df.empty:
        logger.error(f"No data for {asset}")
        return None
    
    df = self.add_technical_indicators(df)
    
    # Store all optimization results
    all_results = {}
    
    # ===== 1. MOMENTUM INDICATORS =====
    logger.info("Optimizing Momentum Indicators...")
    
    # RSI Family
    logger.debug("  • RSI...")
    all_results['rsi'] = self.strategy_optimizer.optimize_rsi(df, asset)
    
    logger.debug("  • RSI Divergence...")
    all_results['rsi_divergence'] = self.strategy_optimizer.optimize_rsi_divergence(df, asset)
    
    logger.debug("  • Stochastic RSI...")
    all_results['stoch_rsi'] = self.strategy_optimizer.optimize_stoch_rsi(df, asset)
    
    # Stochastic Family
    logger.debug("  • Stochastic...")
    all_results['stochastic'] = self.strategy_optimizer.optimize_stochastic(df, asset)
    
    logger.debug("  • Stochastic Fast...")
    all_results['stochastic_fast'] = self.strategy_optimizer.optimize_stochastic_fast(df, asset)
    
    logger.debug("  • Stochastic Full...")
    all_results['stochastic_full'] = self.strategy_optimizer.optimize_stochastic_full(df, asset)
    
    # MACD Family
    logger.debug("  • MACD...")
    all_results['macd'] = self.strategy_optimizer.optimize_macd(df, asset)
    
    logger.debug("  • MACD Histogram...")
    all_results['macd_histogram'] = self.strategy_optimizer.optimize_macd_histogram(df, asset)
    
    logger.debug("  • MACD Divergence...")
    all_results['macd_divergence'] = self.strategy_optimizer.optimize_macd_divergence(df, asset)
    
    # Other Momentum
    logger.debug("  • CCI...")
    all_results['cci'] = self.strategy_optimizer.optimize_cci(df, asset)
    
    logger.debug("  • Williams %R...")
    all_results['williams_r'] = self.strategy_optimizer.optimize_williams_r(df, asset)
    
    logger.debug("  • MFI...")
    all_results['mfi'] = self.strategy_optimizer.optimize_mfi(df, asset)
    
    logger.debug("  • Ultimate Oscillator...")
    all_results['uo'] = self.strategy_optimizer.optimize_uo(df, asset)
    
    logger.debug("  • APO...")
    all_results['apo'] = self.strategy_optimizer.optimize_apo(df, asset)
    
    logger.debug("  • PPO...")
    all_results['ppo'] = self.strategy_optimizer.optimize_ppo(df, asset)
    
    # ===== 2. TREND INDICATORS =====
    logger.info("Optimizing Trend Indicators...")
    
    # Moving Averages
    logger.debug("  • SMA Cross...")
    all_results['sma_cross'] = self.strategy_optimizer.optimize_sma_cross(df, asset)
    
    logger.debug("  • EMA Cross...")
    all_results['ema_cross'] = self.strategy_optimizer.optimize_ema_cross(df, asset)
    
    logger.debug("  • WMA Cross...")
    all_results['wma_cross'] = self.strategy_optimizer.optimize_wma_cross(df, asset)
    
    logger.debug("  • HMA Cross...")
    all_results['hma_cross'] = self.strategy_optimizer.optimize_hma_cross(df, asset)
    
    logger.debug("  • VWAP...")
    all_results['vwap'] = self.strategy_optimizer.optimize_vwap(df, asset)
    
    # ADX Family
    logger.debug("  • ADX...")
    all_results['adx'] = self.strategy_optimizer.optimize_adx(df, asset)
    
    logger.debug("  • +DI...")
    all_results['di_plus'] = self.strategy_optimizer.optimize_di_plus(df, asset)
    
    logger.debug("  • -DI...")
    all_results['di_minus'] = self.strategy_optimizer.optimize_di_minus(df, asset)
    
    logger.debug("  • ADX Cross...")
    all_results['adx_cross'] = self.strategy_optimizer.optimize_adx_cross(df, asset)
    
    # Ichimoku
    logger.debug("  • Ichimoku...")
    all_results['ichimoku'] = self.strategy_optimizer.optimize_ichimoku(df, asset)
    
    logger.debug("  • Ichimoku Tenkan...")
    all_results['ichimoku_tenkan'] = self.strategy_optimizer.optimize_ichimoku_tenkan(df, asset)
    
    logger.debug("  • Ichimoku Kijun...")
    all_results['ichimoku_kijun'] = self.strategy_optimizer.optimize_ichimoku_kijun(df, asset)
    
    logger.debug("  • Ichimoku Cross...")
    all_results['ichimoku_cross'] = self.strategy_optimizer.optimize_ichimoku_cross(df, asset)
    
    # Parabolic SAR
    logger.debug("  • Parabolic SAR...")
    all_results['psar'] = self.strategy_optimizer.optimize_psar(df, asset)
    
    # ===== 3. VOLATILITY INDICATORS =====
    logger.info("Optimizing Volatility Indicators...")
    
    # Bollinger Bands
    logger.debug("  • Bollinger Bands...")
    all_results['bollinger'] = self.strategy_optimizer.optimize_bollinger(df, asset)
    
    logger.debug("  • Bollinger Breakout...")
    all_results['bollinger_breakout'] = self.strategy_optimizer.optimize_bollinger_breakout(df, asset)
    
    logger.debug("  • Bollinger Squeeze...")
    all_results['bollinger_squeeze'] = self.strategy_optimizer.optimize_bollinger_squeeze(df, asset)
    
    logger.debug("  • Bollinger Width...")
    all_results['bollinger_width'] = self.strategy_optimizer.optimize_bollinger_width(df, asset)
    
    # Keltner Channels
    logger.debug("  • Keltner Channels...")
    all_results['keltner'] = self.strategy_optimizer.optimize_keltner(df, asset)
    
    logger.debug("  • Keltner Breakout...")
    all_results['keltner_breakout'] = self.strategy_optimizer.optimize_keltner_breakout(df, asset)
    
    # ATR Family
    logger.debug("  • ATR...")
    all_results['atr'] = self.strategy_optimizer.optimize_atr(df, asset)
    
    logger.debug("  • ATR Trailing...")
    all_results['atr_trailing'] = self.strategy_optimizer.optimize_atr_trailing(df, asset)
    
    logger.debug("  • ATR Bands...")
    all_results['atr_bands'] = self.strategy_optimizer.optimize_atr_bands(df, asset)
    
    # Donchian Channels
    logger.debug("  • Donchian Channels...")
    all_results['donchian'] = self.strategy_optimizer.optimize_donchian(df, asset)
    
    logger.debug("  • Donchian Breakout...")
    all_results['donchian_breakout'] = self.strategy_optimizer.optimize_donchian_breakout(df, asset)
    
    # Volatility-based
    logger.debug("  • Volatility Ratio...")
    all_results['volatility_ratio'] = self.strategy_optimizer.optimize_volatility_ratio(df, asset)
    
    logger.debug("  • Chaikin Volatility...")
    all_results['chaikin_volatility'] = self.strategy_optimizer.optimize_chaikin_volatility(df, asset)
    
    # ===== 4. VOLUME INDICATORS =====
    logger.info("Optimizing Volume Indicators...")
    
    logger.debug("  • OBV...")
    all_results['obv'] = self.strategy_optimizer.optimize_obv(df, asset)
    
    logger.debug("  • OBV Divergence...")
    all_results['obv_divergence'] = self.strategy_optimizer.optimize_obv_divergence(df, asset)
    
    logger.debug("  • Volume Profile...")
    all_results['volume_profile'] = self.strategy_optimizer.optimize_volume_profile(df, asset)
    
    logger.debug("  • Volume Oscillator...")
    all_results['volume_oscillator'] = self.strategy_optimizer.optimize_volume_oscillator(df, asset)
    
    logger.debug("  • VWAP Volume...")
    all_results['vwap_volume'] = self.strategy_optimizer.optimize_vwap_volume(df, asset)
    
    logger.debug("  • CMF...")
    all_results['cmf'] = self.strategy_optimizer.optimize_cmf(df, asset)
    
    logger.debug("  • EOM...")
    all_results['eom'] = self.strategy_optimizer.optimize_eom(df, asset)
    
    logger.debug("  • VPT...")
    all_results['vpt'] = self.strategy_optimizer.optimize_vpt(df, asset)
    
    # ===== 5. OSCILLATORS =====
    logger.info("Optimizing Oscillators...")
    
    logger.debug("  • Awesome Oscillator...")
    all_results['awesome'] = self.strategy_optimizer.optimize_awesome(df, asset)
    
    logger.debug("  • Acceleration Oscillator...")
    all_results['acceleration'] = self.strategy_optimizer.optimize_acceleration(df, asset)
    
    logger.debug("  • RVGI...")
    all_results['rvgi'] = self.strategy_optimizer.optimize_rvgi(df, asset)
    
    logger.debug("  • TRIX...")
    all_results['trix'] = self.strategy_optimizer.optimize_trix(df, asset)
    
    logger.debug("  • CMO...")
    all_results['cmo'] = self.strategy_optimizer.optimize_cmo(df, asset)
    
    # ===== 6. PATTERN RECOGNITION =====
    logger.info("Optimizing Pattern Recognition...")
    
    logger.debug("  • Doji...")
    all_results['doji'] = self.strategy_optimizer.optimize_doji(df, asset)
    
    logger.debug("  • Hammer...")
    all_results['hammer'] = self.strategy_optimizer.optimize_hammer(df, asset)
    
    logger.debug("  • Engulfing...")
    all_results['engulfing'] = self.strategy_optimizer.optimize_engulfing(df, asset)
    
    logger.debug("  • Morning Star...")
    all_results['morning_star'] = self.strategy_optimizer.optimize_morning_star(df, asset)
    
    logger.debug("  • Evening Star...")
    all_results['evening_star'] = self.strategy_optimizer.optimize_evening_star(df, asset)
    
    logger.debug("  • Three White Soldiers...")
    all_results['three_white'] = self.strategy_optimizer.optimize_three_white(df, asset)
    
    logger.debug("  • Three Black Crows...")
    all_results['three_black'] = self.strategy_optimizer.optimize_three_black(df, asset)
    
    # ===== 7. SUPPORT/RESISTANCE =====
    logger.info("Optimizing Support/Resistance...")
    
    logger.debug("  • Pivot Points...")
    all_results['pivot_points'] = self.strategy_optimizer.optimize_pivot_points(df, asset)
    
    logger.debug("  • Fibonacci...")
    all_results['fibonacci'] = self.strategy_optimizer.optimize_fibonacci(df, asset)
    
    logger.debug("  • Supply/Demand...")
    all_results['supply_demand'] = self.strategy_optimizer.optimize_supply_demand(df, asset)
    
    # ===== 8. COMBINATION STRATEGIES =====
    logger.info("Optimizing Combination Strategies...")
    
    logger.debug("  • RSI + MACD...")
    all_results['rsi_macd'] = self.strategy_optimizer.optimize_rsi_macd_combination(df, asset)
    
    logger.debug("  • Bollinger + RSI...")
    all_results['bollinger_rsi'] = self.strategy_optimizer.optimize_bollinger_rsi(df, asset)
    
    logger.debug("  • ADX + DI...")
    all_results['adx_di'] = self.strategy_optimizer.optimize_adx_di(df, asset)
    
    logger.debug("  • Volume Breakout...")
    all_results['volume_breakout'] = self.strategy_optimizer.optimize_volume_breakout(df, asset)
    
    logger.debug("  • Momentum Reversal...")
    all_results['momentum_reversal'] = self.strategy_optimizer.optimize_momentum_reversal(df, asset)
    
    # ===== 9. ADVANCED ML STRATEGIES =====
    logger.info("Optimizing ML Strategies...")
    
    logger.debug("  • ML Ensemble...")
    all_results['ml_ensemble'] = self.strategy_optimizer.optimize_ml_ensemble(df, asset)
    
    logger.debug("  • XGBoost...")
    all_results['xgboost'] = self.strategy_optimizer.optimize_xgboost(df, asset)
    
    logger.debug("  • Random Forest...")
    all_results['random_forest'] = self.strategy_optimizer.optimize_random_forest(df, asset)
    
    # ===== COMPILE RESULTS =====
    logger.info(f"="*70)
    logger.info(f"OPTIMIZATION COMPLETE FOR {asset}")
    logger.info(f"="*70)
    
    # Create comparison of all strategies
    comparison = self.strategy_optimizer.compare_all_strategies(asset)
    
    # Find top 10 best performing strategies
    if not comparison.empty:
        logger.info(f"TOP 10 BEST STRATEGIES FOR {asset}")
        logger.info("-" * 70)
        for idx, row in comparison.head(10).iterrows():
            logger.info(f"  {row['strategy']}: Sharpe={row['best_sharpe']:.2f}, PF={row['best_profit_factor']:.2f}")
        
        # Save top strategies to file
        top_strategies = comparison.head(10).to_dict('records')
        with open(f"optimization_results/{asset}_top_strategies.json", 'w') as f:
            json.dump(top_strategies, f, indent=2, default=str)
    
    # Update strategy weights based on optimization
    self.update_strategy_weights_from_optimization(comparison)
    
    return {
        'asset': asset,
        'all_results': all_results,
        'comparison': comparison,
        'top_strategies': comparison.head(10) if not comparison.empty else None,
        'timestamp': datetime.now().isoformat()
    }

def update_strategy_weights_from_optimization(self, comparison_df: pd.DataFrame):
    """
    Update strategy weights in voting engine based on optimization results
    """
    if not hasattr(self, 'voting_engine') or comparison_df.empty:
        return
    
    logger.info("Updating strategy weights based on optimization...")
    
    # Normalize Sharpe ratios to weights (0.5 to 2.0 range)
    max_sharpe = comparison_df['sharpe'].max()
    min_sharpe = comparison_df['sharpe'].min()
    
    for _, row in comparison_df.iterrows():
        strategy = row['strategy']
        sharpe = row['sharpe']
        
        if max_sharpe > min_sharpe:
            # Normalize to 0.5-2.0 range
            normalized = 0.5 + 1.5 * (sharpe - min_sharpe) / (max_sharpe - min_sharpe)
        else:
            normalized = 1.0
        
        # Update weight in voting engine
        if strategy in self.voting_engine.strategy_weights:
            old_weight = self.voting_engine.strategy_weights[strategy]
            self.voting_engine.strategy_weights[strategy] = round(normalized, 2)
            logger.debug(f"  • {strategy}: {old_weight} → {self.voting_engine.strategy_weights[strategy]}")

# ============= BATCH OPTIMIZATION METHODS =============

def batch_optimize_all_assets(self, assets: List[str] = None, lookback_days: int = 365):
    """
    Optimize ALL 50+ strategies for ALL assets in one go
    This will take a while but gives you the best parameters for every strategy on every asset
    
    Args:
        assets: List of assets to optimize (None = all assets)
        lookback_days: Number of days of historical data to use
    
    Returns:
        Dictionary with optimization results for all assets
    """
    logger.info("="*80)
    logger.info("BATCH OPTIMIZING ALL 50+ STRATEGIES FOR ALL ASSETS")
    logger.info("="*80)
    logger.warning("This will take a LONG time (minutes to hours depending on number of assets)")
    logger.info("Consider running this overnight or on a weekend")
    logger.info("="*80)
    
    # Get list of assets to optimize
    if assets is None:
        # Get all tradable assets
        asset_list = self.get_asset_list()
        assets_to_optimize = [asset[0] for asset in asset_list]  # Extract asset names
    else:
        assets_to_optimize = assets
    
    logger.info(f"Will optimize {len(assets_to_optimize)} assets")
    logger.info(f"Each asset: 50+ strategies × multiple parameters = thousands of combinations")
    logger.info(f"Estimated time: {len(assets_to_optimize) * 5} minutes")
    
    all_results = {}
    
    for i, asset_name in enumerate(assets_to_optimize, 1):
        logger.info(f"="*60)
        logger.info(f"[{i}/{len(assets_to_optimize)}] OPTIMIZING {asset_name}")
        logger.info(f"="*60)
        
        try:
            # Fetch historical data
            logger.info(f"Fetching {lookback_days} days of data for {asset_name}...")
            df = self.fetch_historical_data(asset_name, lookback_days)
            
            if df.empty or len(df) < 100:
                logger.warning(f"Insufficient data for {asset_name}, skipping...")
                continue
            
            # Add all technical indicators
            logger.info(f"Adding 50+ technical indicators...")
            df = self.add_technical_indicators(df)
            logger.debug(f"Data shape: {df.shape}")
            
            # Initialize results for this asset
            asset_results = {}
            
            # ===== 1. MOMENTUM INDICATORS =====
            logger.info("Optimizing Momentum Indicators...")
            
            # RSI Family
            logger.debug("   • RSI...")
            asset_results['rsi'] = self.strategy_optimizer.optimize_rsi(df, asset_name)
            
            logger.debug("   • RSI Divergence...")
            asset_results['rsi_divergence'] = self.strategy_optimizer.optimize_rsi_divergence(df, asset_name)
            
            logger.debug("   • Stochastic RSI...")
            asset_results['stoch_rsi'] = self.strategy_optimizer.optimize_stoch_rsi(df, asset_name)
            
            # Stochastic Family
            logger.debug("   • Stochastic...")
            asset_results['stochastic'] = self.strategy_optimizer.optimize_stochastic(df, asset_name)
            
            logger.debug("   • Stochastic Fast...")
            asset_results['stochastic_fast'] = self.strategy_optimizer.optimize_stochastic_fast(df, asset_name)
            
            logger.debug("   • Stochastic Full...")
            asset_results['stochastic_full'] = self.strategy_optimizer.optimize_stochastic_full(df, asset_name)
            
            # MACD Family
            logger.debug("   • MACD...")
            asset_results['macd'] = self.strategy_optimizer.optimize_macd(df, asset_name)
            
            logger.debug("   • MACD Histogram...")
            asset_results['macd_histogram'] = self.strategy_optimizer.optimize_macd_histogram(df, asset_name)
            
            logger.debug("   • MACD Divergence...")
            asset_results['macd_divergence'] = self.strategy_optimizer.optimize_macd_divergence(df, asset_name)
            
            # Other Momentum
            logger.debug("   • CCI...")
            asset_results['cci'] = self.strategy_optimizer.optimize_cci(df, asset_name)
            
            logger.debug("   • Williams %R...")
            asset_results['williams_r'] = self.strategy_optimizer.optimize_williams_r(df, asset_name)
            
            logger.debug("   • MFI...")
            asset_results['mfi'] = self.strategy_optimizer.optimize_mfi(df, asset_name)
            
            logger.debug("   • Ultimate Oscillator...")
            asset_results['uo'] = self.strategy_optimizer.optimize_uo(df, asset_name)
            
            logger.debug("   • APO...")
            asset_results['apo'] = self.strategy_optimizer.optimize_apo(df, asset_name)
            
            logger.debug("   • PPO...")
            asset_results['ppo'] = self.strategy_optimizer.optimize_ppo(df, asset_name)
            
            # ===== 2. TREND INDICATORS =====
            logger.info("Optimizing Trend Indicators...")
            
            # Moving Averages
            logger.debug("   • SMA Cross...")
            asset_results['sma_cross'] = self.strategy_optimizer.optimize_sma_cross(df, asset_name)
            
            logger.debug("   • EMA Cross...")
            asset_results['ema_cross'] = self.strategy_optimizer.optimize_ema_cross(df, asset_name)
            
            logger.debug("   • WMA Cross...")
            asset_results['wma_cross'] = self.strategy_optimizer.optimize_wma_cross(df, asset_name)
            
            logger.debug("   • HMA Cross...")
            asset_results['hma_cross'] = self.strategy_optimizer.optimize_hma_cross(df, asset_name)
            
            logger.debug("   • VWAP...")
            asset_results['vwap'] = self.strategy_optimizer.optimize_vwap(df, asset_name)
            
            # ADX Family
            logger.debug("   • ADX...")
            asset_results['adx'] = self.strategy_optimizer.optimize_adx(df, asset_name)
            
            logger.debug("   • +DI...")
            asset_results['di_plus'] = self.strategy_optimizer.optimize_di_plus(df, asset_name)
            
            logger.debug("   • -DI...")
            asset_results['di_minus'] = self.strategy_optimizer.optimize_di_minus(df, asset_name)
            
            logger.debug("   • ADX Cross...")
            asset_results['adx_cross'] = self.strategy_optimizer.optimize_adx_cross(df, asset_name)
            
            # Ichimoku
            logger.debug("   • Ichimoku...")
            asset_results['ichimoku'] = self.strategy_optimizer.optimize_ichimoku(df, asset_name)
            
            logger.debug("   • Ichimoku Tenkan...")
            asset_results['ichimoku_tenkan'] = self.strategy_optimizer.optimize_ichimoku_tenkan(df, asset_name)
            
            logger.debug("   • Ichimoku Kijun...")
            asset_results['ichimoku_kijun'] = self.strategy_optimizer.optimize_ichimoku_kijun(df, asset_name)
            
            logger.debug("   • Ichimoku Cross...")
            asset_results['ichimoku_cross'] = self.strategy_optimizer.optimize_ichimoku_cross(df, asset_name)
            
            # Parabolic SAR
            logger.debug("   • Parabolic SAR...")
            asset_results['psar'] = self.strategy_optimizer.optimize_psar(df, asset_name)
            
            # ===== 3. VOLATILITY INDICATORS =====
            logger.info("Optimizing Volatility Indicators...")
            
            # Bollinger Bands
            logger.debug("   • Bollinger Bands...")
            asset_results['bollinger'] = self.strategy_optimizer.optimize_bollinger(df, asset_name)
            
            logger.debug("   • Bollinger Breakout...")
            asset_results['bollinger_breakout'] = self.strategy_optimizer.optimize_bollinger_breakout(df, asset_name)
            
            logger.debug("   • Bollinger Squeeze...")
            asset_results['bollinger_squeeze'] = self.strategy_optimizer.optimize_bollinger_squeeze(df, asset_name)
            
            logger.debug("   • Bollinger Width...")
            asset_results['bollinger_width'] = self.strategy_optimizer.optimize_bollinger_width(df, asset_name)
            
            # Keltner Channels
            logger.debug("   • Keltner Channels...")
            asset_results['keltner'] = self.strategy_optimizer.optimize_keltner(df, asset_name)
            
            logger.debug("   • Keltner Breakout...")
            asset_results['keltner_breakout'] = self.strategy_optimizer.optimize_keltner_breakout(df, asset_name)
            
            # ATR Family
            logger.debug("   • ATR...")
            asset_results['atr'] = self.strategy_optimizer.optimize_atr(df, asset_name)
            
            logger.debug("   • ATR Trailing...")
            asset_results['atr_trailing'] = self.strategy_optimizer.optimize_atr_trailing(df, asset_name)
            
            logger.debug("   • ATR Bands...")
            asset_results['atr_bands'] = self.strategy_optimizer.optimize_atr_bands(df, asset_name)
            
            # Donchian Channels
            logger.debug("   • Donchian Channels...")
            asset_results['donchian'] = self.strategy_optimizer.optimize_donchian(df, asset_name)
            
            logger.debug("   • Donchian Breakout...")
            asset_results['donchian_breakout'] = self.strategy_optimizer.optimize_donchian_breakout(df, asset_name)
            
            # Volatility-based
            logger.debug("   • Volatility Ratio...")
            asset_results['volatility_ratio'] = self.strategy_optimizer.optimize_volatility_ratio(df, asset_name)
            
            logger.debug("   • Chaikin Volatility...")
            asset_results['chaikin_volatility'] = self.strategy_optimizer.optimize_chaikin_volatility(df, asset_name)
            
            # ===== 4. VOLUME INDICATORS =====
            logger.info("Optimizing Volume Indicators...")
            
            logger.debug("   • OBV...")
            asset_results['obv'] = self.strategy_optimizer.optimize_obv(df, asset_name)
            
            logger.debug("   • OBV Divergence...")
            asset_results['obv_divergence'] = self.strategy_optimizer.optimize_obv_divergence(df, asset_name)
            
            logger.debug("   • Volume Profile...")
            asset_results['volume_profile'] = self.strategy_optimizer.optimize_volume_profile(df, asset_name)
            
            logger.debug("   • Volume Oscillator...")
            asset_results['volume_oscillator'] = self.strategy_optimizer.optimize_volume_oscillator(df, asset_name)
            
            logger.debug("   • VWAP Volume...")
            asset_results['vwap_volume'] = self.strategy_optimizer.optimize_vwap_volume(df, asset_name)
            
            logger.debug("   • CMF...")
            asset_results['cmf'] = self.strategy_optimizer.optimize_cmf(df, asset_name)
            
            logger.debug("   • EOM...")
            asset_results['eom'] = self.strategy_optimizer.optimize_eom(df, asset_name)
            
            logger.debug("   • VPT...")
            asset_results['vpt'] = self.strategy_optimizer.optimize_vpt(df, asset_name)
            
            # ===== 5. OSCILLATORS =====
            logger.info("Optimizing Oscillators...")
            
            logger.debug("   • Awesome Oscillator...")
            asset_results['awesome'] = self.strategy_optimizer.optimize_awesome(df, asset_name)
            
            logger.debug("   • Acceleration Oscillator...")
            asset_results['acceleration'] = self.strategy_optimizer.optimize_acceleration(df, asset_name)
            
            logger.debug("   • RVGI...")
            asset_results['rvgi'] = self.strategy_optimizer.optimize_rvgi(df, asset_name)
            
            logger.debug("   • TRIX...")
            asset_results['trix'] = self.strategy_optimizer.optimize_trix(df, asset_name)
            
            logger.debug("   • CMO...")
            asset_results['cmo'] = self.strategy_optimizer.optimize_cmo(df, asset_name)
            
            # ===== 6. PATTERN RECOGNITION =====
            logger.info("Optimizing Pattern Recognition...")
            
            logger.debug("   • Doji...")
            asset_results['doji'] = self.strategy_optimizer.optimize_doji(df, asset_name)
            
            logger.debug("   • Hammer...")
            asset_results['hammer'] = self.strategy_optimizer.optimize_hammer(df, asset_name)
            
            logger.debug("   • Engulfing...")
            asset_results['engulfing'] = self.strategy_optimizer.optimize_engulfing(df, asset_name)
            
            logger.debug("   • Morning Star...")
            asset_results['morning_star'] = self.strategy_optimizer.optimize_morning_star(df, asset_name)
            
            logger.debug("   • Evening Star...")
            asset_results['evening_star'] = self.strategy_optimizer.optimize_evening_star(df, asset_name)
            
            logger.debug("   • Three White Soldiers...")
            asset_results['three_white'] = self.strategy_optimizer.optimize_three_white(df, asset_name)
            
            logger.debug("   • Three Black Crows...")
            asset_results['three_black'] = self.strategy_optimizer.optimize_three_black(df, asset_name)
            
            # ===== 7. SUPPORT/RESISTANCE =====
            logger.info("Optimizing Support/Resistance...")
            
            logger.debug("   • Pivot Points...")
            asset_results['pivot_points'] = self.strategy_optimizer.optimize_pivot_points(df, asset_name)
            
            logger.debug("   • Fibonacci...")
            asset_results['fibonacci'] = self.strategy_optimizer.optimize_fibonacci(df, asset_name)
            
            logger.debug("   • Supply/Demand...")
            asset_results['supply_demand'] = self.strategy_optimizer.optimize_supply_demand(df, asset_name)
            
            # ===== 8. COMBINATION STRATEGIES =====
            logger.info("Optimizing Combination Strategies...")
            
            logger.debug("   • RSI + MACD...")
            asset_results['rsi_macd'] = self.strategy_optimizer.optimize_rsi_macd_combination(df, asset_name)
            
            logger.debug("   • Bollinger + RSI...")
            asset_results['bollinger_rsi'] = self.strategy_optimizer.optimize_bollinger_rsi(df, asset_name)
            
            logger.debug("   • ADX + DI...")
            asset_results['adx_di'] = self.strategy_optimizer.optimize_adx_di(df, asset_name)
            
            logger.debug("   • Volume Breakout...")
            asset_results['volume_breakout'] = self.strategy_optimizer.optimize_volume_breakout(df, asset_name)
            
            logger.debug("   • Momentum Reversal...")
            asset_results['momentum_reversal'] = self.strategy_optimizer.optimize_momentum_reversal(df, asset_name)
            
            # Store results for this asset
            all_results[asset_name] = asset_results
            
            # Show summary for this asset
            logger.info(f"COMPLETED {asset_name}")
            logger.info(f"   • Successfully optimized {len(asset_results)} strategies")
            
            # Compare strategies for this asset
            comparison = self.strategy_optimizer.compare_strategies(asset_name)
            if not comparison.empty:
                logger.info(f"TOP 3 STRATEGIES FOR {asset_name}:")
                for idx, row in comparison.head(3).iterrows():
                    logger.info(f"      {row['strategy']}: Sharpe {row['best_sharpe']:.2f}")
            
        except Exception as e:
            logger.error(f"Error optimizing {asset_name}: {e}", exc_info=True)
            continue
    
    # Create master summary
    logger.info("="*80)
    logger.info("MASTER OPTIMIZATION SUMMARY")
    logger.info("="*80)
    logger.info(f"Successfully optimized {len(all_results)} out of {len(assets_to_optimize)} assets")
    
    # Save all results to file
    self._save_optimization_results(all_results)
    
    return all_results

def create_master_optimization_report(self, all_results: Dict):
    """
    Create master report showing best strategies across all assets
    """
    logger.info(f"="*70)
    logger.info(f"MASTER OPTIMIZATION REPORT - ALL ASSETS")
    logger.info("="*70)
    
    # Aggregate results across assets
    strategy_performance = {}
    
    for asset, result in all_results.items():
        # Get comparison for this asset from the strategy optimizer
        if hasattr(self, 'strategy_optimizer'):
            comp = self.strategy_optimizer.compare_strategies(asset)
            if not comp.empty:
                for _, row in comp.iterrows():
                    strategy = row['strategy']
                    if strategy not in strategy_performance:
                        strategy_performance[strategy] = {
                            'assets': [],
                            'avg_sharpe': 0,
                            'avg_profit_factor': 0,
                            'avg_win_rate': 0,
                            'total_sharpe': 0
                        }
                    
                    strategy_performance[strategy]['assets'].append(asset)
                    strategy_performance[strategy]['total_sharpe'] += row['best_sharpe']
    
    if not strategy_performance:
        logger.warning("No strategy performance data available")
        return
    
    # Calculate averages
    for strategy, data in strategy_performance.items():
        data['avg_sharpe'] = data['total_sharpe'] / len(data['assets'])
    
    # Sort by average Sharpe
    sorted_strategies = sorted(
        strategy_performance.items(),
        key=lambda x: x[1]['avg_sharpe'],
        reverse=True
    )
    
    logger.info("TOP 10 STRATEGIES ACROSS ALL ASSETS:")
    logger.info("-" * 70)
    for i, (strategy, data) in enumerate(sorted_strategies[:10], 1):
        logger.info(f"{i}. {strategy}:")
        logger.info(f"   • Avg Sharpe: {data['avg_sharpe']:.2f}")
        logger.info(f"   • Works on: {len(data['assets'])} assets")
        logger.info(f"   • Examples: {', '.join(data['assets'][:3])}")
    
    # Save master report
    try:
        os.makedirs("optimization_results", exist_ok=True)
        
        report = {
            'timestamp': datetime.now().isoformat(),
            'total_assets': len(all_results),
            'strategy_rankings': [
                {
                    'strategy': strategy,
                    'avg_sharpe': round(data['avg_sharpe'], 3),
                    'assets_count': len(data['assets']),
                    'sample_assets': data['assets'][:5]
                }
                for strategy, data in sorted_strategies
            ]
        }
        
        filename = f"optimization_results/master_optimization_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(filename, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        logger.info(f"Master report saved to {filename}")
        
    except Exception as e:
        logger.warning(f"Could not save master report: {e}")

def _save_optimization_results(self, all_results: Dict):
    """Save all optimization results to a JSON file"""
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"optimization_results/master_optimization_{timestamp}.json"
        
        # Convert to serializable format
        serializable_results = {}
        for asset, strategies in all_results.items():
            serializable_results[asset] = {}
            for strategy_name, result in strategies.items():
                if result and 'error' not in result:
                    serializable_results[asset][strategy_name] = {
                        'best_params': result.get('best_params', {}),
                        'best_sharpe': result.get('best_value', 0),
                        'win_rate': result.get('results_summary', {}).get('best_win_rate', 0),
                        'profit_factor': result.get('results_summary', {}).get('best_profit_factor', 0),
                        'total_return': result.get('results_summary', {}).get('best_return', 0)
                    }
        
        with open(filename, 'w') as f:
            json.dump(serializable_results, f, indent=2, default=str)
        
        logger.info(f"All optimization results saved to: {filename}")
        
    except Exception as e:
        logger.warning(f"Could not save optimization results: {e}")

def load_optimized_params(self, asset: str, strategy: str) -> Optional[Dict]:
    """
    Load the best parameters for a specific asset and strategy
    """
    if not hasattr(self, 'strategy_optimizer'):
        return None
    
    return self.strategy_optimizer.get_best_params(asset, strategy)

def apply_optimized_params_to_strategies(self):
    """
    Apply all optimized parameters to your trading strategies
    Call this after running batch_optimize_all_assets
    """
    logger.info("Applying optimized parameters to strategies...")
    
    # Store optimized params for later use
    self.optimized_params = {}
    
    # Get all assets
    assets = [asset[0] for asset in self.get_asset_list()]
    
    for asset in assets:
        self.optimized_params[asset] = {}
        
        # Get best params for each strategy
        strategies = ['rsi', 'macd', 'bollinger', 'stochastic', 'adx', 'ichimoku']
        for strategy in strategies:
            params = self.load_optimized_params(asset, strategy)
            if params:
                self.optimized_params[asset][strategy] = params
                logger.debug(f"   ✓ {asset} - {strategy}: {params}")
    
    logger.info("Optimized parameters applied")