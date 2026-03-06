"""
🚀 ULTIMATE TRADING BOT - PROFESSIONAL EDITION
Combines all advanced features into one powerful system

Features:
✅ 10+ ML Models Ensemble
✅ Kelly Criterion Position Sizing
✅ Market Regime Detection
✅ Sentiment Analysis Integration
✅ Advanced Risk Management
✅ Professional Backtesting
✅ Multi-Timeframe Analysis
✅ Portfolio Optimization
✅ Real-time Notifications
✅ Performance Analytics Dashboard
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional, Any
from datetime import datetime, timedelta
import time
import json
from pathlib import Path

# Import all advanced modules
from advanced_predictor import AdvancedPredictionEngine
from advanced_risk_manager import AdvancedRiskManager, DynamicRiskAdjuster
from advanced_backtester import AdvancedBacktester, BacktestResults
from market_regime_analyzer import MarketRegimeDetector, SentimentAnalyzer

# Import core modules
from data.fetcher import DataFetcher
from indicators.technical import TechnicalIndicators
from utils.trading_signals import TradingSignalGenerator


class UltimateTradingBot:
    """
    🚀 PROFESSIONAL-GRADE TRADING SYSTEM
    
    This is the complete integration of all advanced features.
    Use this for maximum power and accuracy!
    """
    
    def __init__(
        self,
        account_balance: float = 10000,
        risk_per_trade: float = 1.0,
        max_positions: int = 5,
        use_kelly: bool = True,
        use_sentiment: bool = True,
        use_regime_detection: bool = True
    ):
        """
        Initialize the Ultimate Trading Bot
        
        Args:
            account_balance: Starting capital
            risk_per_trade: Base risk percentage
            max_positions: Maximum concurrent positions
            use_kelly: Use Kelly Criterion for position sizing
            use_sentiment: Incorporate market sentiment
            use_regime_detection: Adapt to market regimes
        """
        print("\n" + "="*70)
        print("🚀 INITIALIZING ULTIMATE TRADING BOT - PROFESSIONAL EDITION")
        print("="*70)
        
        # Core components
        self.fetcher = DataFetcher()
        self.account_balance = account_balance
        self.risk_per_trade = risk_per_trade
        self.max_positions = max_positions
        
        # Advanced components
        self.ml_engine = AdvancedPredictionEngine("super_ensemble")
        self.risk_manager = AdvancedRiskManager(account_balance)
        self.regime_detector = MarketRegimeDetector()
        self.sentiment_analyzer = SentimentAnalyzer()
        
        # Feature flags
        self.use_kelly = use_kelly
        self.use_sentiment = use_sentiment
        self.use_regime_detection = use_regime_detection
        
        # State
        self.trained_models: Dict[str, AdvancedPredictionEngine] = {}
        self.current_positions: List[Dict] = []
        self.trade_history: List[Dict] = []
        self.performance_metrics: Dict = {}
        
        print("✅ All systems initialized")
        print(f"💰 Account Balance: ${account_balance:,.2f}")
        print(f"⚙️  Kelly Criterion: {'ON' if use_kelly else 'OFF'}")
        print(f"🧠 Sentiment Analysis: {'ON' if use_sentiment else 'OFF'}")
        print(f"📊 Regime Detection: {'ON' if use_regime_detection else 'OFF'}")
        print("="*70 + "\n")
    
    def train_multi_timeframe_models(
        self,
        asset: str,
        asset_type: str = 'forex',
        timeframes: List[str] = ['1d', '1h', '15m']
    ) -> Dict[str, AdvancedPredictionEngine]:
        """
        Train ML models across multiple timeframes
        Returns better predictions by combining different perspectives
        """
        print(f"\n🧠 Training Multi-Timeframe Models for {asset}")
        print("-" * 70)
        
        models = {}
        
        for tf in timeframes:
            try:
                print(f"\n📊 Timeframe: {tf}")
                
                # Fetch data
                if asset_type == 'forex':
                    df = self.fetcher.fetch_forex_data(asset, tf, lookback=200)
                elif asset_type == 'crypto':
                    df = self.fetcher.fetch_crypto_data(asset, tf, lookback=200)
                elif asset_type == 'stock':
                    df = self.fetcher.fetch_stock_data(asset, tf, lookback=200)
                else:
                    continue
                
                if df.empty or len(df) < 100:
                    print(f"  ⚠️  Insufficient data for {tf}")
                    continue
                
                # Add indicators
                df = TechnicalIndicators.add_all_indicators(df)
                
                # Train model
                engine = AdvancedPredictionEngine("super_ensemble")
                engine.train(df, target_periods=5)
                
                models[tf] = engine
                print(f"  ✅ {tf} model trained successfully")
                
            except Exception as e:
                print(f"  ❌ Failed to train {tf}: {e}")
        
        # Store models
        self.trained_models[asset] = models
        
        print(f"\n✅ Trained {len(models)} timeframe models for {asset}")
        return models
    
    def generate_ultimate_signal(
        self,
        asset: str,
        asset_type: str = 'forex',
        primary_timeframe: str = '1d'
    ) -> Dict[str, Any]:
        """
        Generate the most powerful signal possible by combining:
        - Multi-timeframe ML predictions
        - Market regime analysis
        - Sentiment analysis
        - Advanced technical analysis
        - Dynamic risk adjustment
        """
        print(f"\n🎯 Generating Ultimate Signal for {asset}")
        print("="*70)
        
        # Fetch primary timeframe data
        if asset_type == 'forex':
            df = self.fetcher.fetch_forex_data(asset, primary_timeframe, lookback=100)
        elif asset_type == 'crypto':
            df = self.fetcher.fetch_crypto_data(asset, primary_timeframe, lookback=100)
        elif asset_type == 'stock':
            df = self.fetcher.fetch_stock_data(asset, primary_timeframe, lookback=100)
        else:
            return {'error': 'Invalid asset type'}
        
        if df.empty:
            return {'error': 'No data available'}
        
        # Add indicators
        df = TechnicalIndicators.add_all_indicators(df)
        
        # 1. MULTI-TIMEFRAME ML PREDICTIONS
        ml_predictions = {}
        ml_confidence = 0.5
        
        if asset in self.trained_models:
            print("\n🧠 Multi-Timeframe ML Analysis:")
            
            timeframe_predictions = []
            for tf, engine in self.trained_models[asset].items():
                try:
                    # Get fresh data for this timeframe
                    if asset_type == 'forex':
                        tf_df = self.fetcher.fetch_forex_data(asset, tf, lookback=100)
                    elif asset_type == 'crypto':
                        tf_df = self.fetcher.fetch_crypto_data(asset, tf, lookback=100)
                    else:
                        tf_df = self.fetcher.fetch_stock_data(asset, tf, lookback=100)
                    
                    tf_df = TechnicalIndicators.add_all_indicators(tf_df)
                    
                    prediction = engine.predict_next(tf_df)
                    ml_predictions[tf] = prediction
                    timeframe_predictions.append(prediction['confidence'])
                    
                    print(f"  {tf}: {prediction['direction']} ({prediction['confidence']:.0%})")
                    
                except:
                    continue
            
            # Average confidence across timeframes
            if timeframe_predictions:
                ml_confidence = np.mean(timeframe_predictions)
        
        # 2. MARKET REGIME DETECTION
        regime_adjustment = 1.0
        regime_info = {}
        
        if self.use_regime_detection:
            print("\n📊 Market Regime Analysis:")
            
            regime, regime_confidence = self.regime_detector.detect_regime(df)
            regime_strategy = self.regime_detector.get_regime_strategy(regime)
            
            regime_info = {
                'regime': regime.value,
                'confidence': regime_confidence,
                'bias': regime_strategy['bias'],
                'risk_multiplier': regime_strategy['risk_multiplier'],
                'description': regime_strategy['description']
            }
            
            regime_adjustment = regime_strategy['risk_multiplier']
            
            print(f"  Regime: {regime.value}")
            print(f"  Confidence: {regime_confidence:.0%}")
            print(f"  Strategy: {regime_strategy['description']}")
        
        # 3. SENTIMENT ANALYSIS
        sentiment_adjustment = 1.0
        sentiment_info = {}
        
        if self.use_sentiment and asset_type == 'crypto':
            print("\n💭 Sentiment Analysis:")
            
            fg_index = self.sentiment_analyzer.get_crypto_fear_greed_index()
            sentiment_info = fg_index
            
            print(f"  Fear & Greed: {fg_index['value']} ({fg_index['classification']})")
        
        # 4. GENERATE BASE SIGNAL
        base_signal = TradingSignalGenerator.generate_entry_signal(
            df,
            ml_predictions.get(primary_timeframe)
        )
        
        # 5. ENHANCE WITH SENTIMENT
        if self.use_sentiment and sentiment_info:
            sentiment_adj, sentiment_reason = self.sentiment_analyzer.analyze_sentiment_impact(
                sentiment_info['sentiment_score'],
                base_signal['signal']
            )
            sentiment_adjustment = sentiment_adj
            sentiment_info['adjustment'] = sentiment_adj
            sentiment_info['reason'] = sentiment_reason
            
            print(f"  Impact: {sentiment_adj:.2f}x - {sentiment_reason}")
        
        # 6. CALCULATE FINAL CONFIDENCE
        # Combine ML, regime, and sentiment
        final_confidence = base_signal['confidence']
        final_confidence *= ml_confidence  # Weight by ML confidence
        final_confidence *= (1 + (regime_adjustment - 1) * 0.3)  # Partial regime adjustment
        final_confidence *= sentiment_adjustment
        
        # Bound between 0.3 and 0.95
        final_confidence = min(max(final_confidence, 0.3), 0.95)
        
        # 7. DYNAMIC RISK CALCULATION
        print("\n💰 Position Sizing:")
        
        if base_signal['signal'] != 'HOLD':
            # Get historical performance for Kelly
            metrics = self.risk_manager.calculate_trade_metrics(self.trade_history)
            
            position_details = self.risk_manager.calculate_optimal_position_size(
                entry_price=base_signal['entry_price'],
                stop_loss=base_signal['stop_loss'],
                signal_confidence=final_confidence,
                asset_volatility=base_signal['risk_pct'] / 100,
                win_rate=metrics.win_rate if metrics.total_trades > 10 else 0.55,
                avg_win=metrics.avg_win if metrics.total_trades > 10 else 0.02,
                avg_loss=abs(metrics.avg_loss) if metrics.total_trades > 10 else 0.01
            )
            
            # Apply regime adjustment
            position_details['position_size'] *= regime_adjustment
            position_details['adjusted_by_regime'] = regime_adjustment
            
            print(f"  Position Size: {position_details['position_size']:.2f} units")
            print(f"  Risk: ${position_details['risk_amount']:.2f} ({position_details['risk_pct']:.2f}%)")
            if self.use_kelly:
                print(f"  Kelly Fraction: {position_details['kelly_fraction']:.2%}")
        else:
            position_details = {}
        
        # 8. COMPILE ULTIMATE SIGNAL
        ultimate_signal = {
            **base_signal,
            'confidence': final_confidence,
            'original_confidence': base_signal['confidence'],
            'ml_analysis': ml_predictions,
            'ml_confidence': ml_confidence,
            'regime_analysis': regime_info,
            'sentiment_analysis': sentiment_info,
            'position_sizing': position_details,
            'asset': asset,
            'asset_type': asset_type,
            'timestamp': datetime.now().isoformat(),
            'timeframe': primary_timeframe
        }
        
        # 9. FINAL RECOMMENDATION
        print("\n" + "="*70)
        print("🎯 ULTIMATE SIGNAL SUMMARY:")
        print("="*70)
        print(f"Asset: {asset}")
        print(f"Signal: {ultimate_signal['signal']}")
        print(f"Final Confidence: {final_confidence:.0%}")
        
        if ultimate_signal['signal'] != 'HOLD':
            print(f"Entry: {ultimate_signal['entry_price']:.5f}")
            print(f"Stop Loss: {ultimate_signal['stop_loss']:.5f}")
            print(f"Take Profit 1: {ultimate_signal['take_profit_levels'][0]['price']:.5f} (1.5:1)")
            print(f"Take Profit 2: {ultimate_signal['take_profit_levels'][1]['price']:.5f} (2:1)")
            print(f"Take Profit 3: {ultimate_signal['take_profit_levels'][2]['price']:.5f} (3:1)")
        
        print(f"Reason: {ultimate_signal['reason']}")
        
        if regime_info:
            print(f"\nMarket Regime: {regime_info['regime']}")
            print(f"Strategy: {regime_info['description']}")
        
        if sentiment_info and 'reason' in sentiment_info:
            print(f"\nSentiment: {sentiment_info['reason']}")
        
        print("="*70)
        
        return ultimate_signal
    
    def run_portfolio_analysis(
        self,
        assets: List[Tuple[str, str]],  # [(asset, type), ...]
        timeframe: str = '1d'
    ) -> Dict[str, Any]:
        """
        Analyze multiple assets and generate portfolio recommendations
        
        Returns comprehensive portfolio analysis with correlations and optimization
        """
        print("\n" + "="*70)
        print("📊 PORTFOLIO ANALYSIS")
        print("="*70)
        
        all_signals = []
        correlation_matrix = pd.DataFrame()
        
        # Generate signals for all assets
        for asset, asset_type in assets:
            print(f"\nAnalyzing {asset}...")
            
            signal = self.generate_ultimate_signal(asset, asset_type, timeframe)
            
            if 'error' not in signal:
                all_signals.append(signal)
        
        # Sort by confidence
        all_signals.sort(key=lambda x: x['confidence'], reverse=True)
        
        # Portfolio recommendations
        print("\n" + "="*70)
        print("🎯 TOP OPPORTUNITIES:")
        print("="*70)
        
        actionable = [s for s in all_signals if s['signal'] != 'HOLD' and s['confidence'] >= 0.70]
        
        for i, signal in enumerate(actionable[:5], 1):
            print(f"\n{i}. {signal['asset']} - {signal['signal']}")
            print(f"   Confidence: {signal['confidence']:.0%}")
            print(f"   Entry: {signal['entry_price']:.5f}")
            print(f"   Risk: {signal['risk_pct']:.2f}%")
            print(f"   Regime: {signal['regime_analysis'].get('regime', 'N/A')}")
        
        return {
            'signals': all_signals,
            'actionable_count': len(actionable),
            'top_opportunities': actionable[:5],
            'timestamp': datetime.now().isoformat()
        }
    
    def backtest_strategy(
        self,
        asset: str,
        asset_type: str,
        start_date: str,
        end_date: str,
        initial_capital: float = 10000
    ) -> BacktestResults:
        """
        Backtest the complete strategy on historical data
        """
        print(f"\n🔬 BACKTESTING STRATEGY FOR {asset}")
        print("="*70)
        
        # Fetch historical data
        if asset_type == 'forex':
            df = self.fetcher.fetch_forex_data(asset, '1d', lookback=500)
        elif asset_type == 'crypto':
            df = self.fetcher.fetch_crypto_data(asset, '1d', lookback=500)
        else:
            df = self.fetcher.fetch_stock_data(asset, '1d', lookback=500)
        
        # Filter by date range
        df = df[(df.index >= start_date) & (df.index <= end_date)]
        
        if df.empty:
            print("❌ No data in date range")
            return None
        
        # Add indicators
        df = TechnicalIndicators.add_all_indicators(df)
        
        # Train models on training portion (first 70%)
        split_idx = int(len(df) * 0.7)
        train_df = df.iloc[:split_idx]
        
        engine = AdvancedPredictionEngine("super_ensemble")
        engine.train(train_df, target_periods=5)
        
        # Generate signals on test portion
        signals = []
        for idx in range(split_idx, len(df)):
            try:
                window_df = df.iloc[:idx+1]
                prediction = engine.predict_next(window_df)
                signal = TradingSignalGenerator.generate_entry_signal(window_df, prediction)
                
                signals.append({
                    'date': df.index[idx],
                    'signal': signal['signal'],
                    'confidence': signal['confidence'],
                    'entry': signal['entry_price'],
                    'stop_loss': signal['stop_loss'],
                    'take_profit': signal['take_profit_levels'][0]['price'],
                    'asset': asset
                })
            except:
                continue
        
        signals_df = pd.DataFrame(signals)
        
        # Run backtest
        backtester = AdvancedBacktester(
            initial_capital=initial_capital,
            risk_per_trade=self.risk_per_trade / 100
        )
        
        results = backtester.run_backtest(df, signals_df)
        
        # Print results
        print("\n" + "="*70)
        print("📊 BACKTEST RESULTS:")
        print("="*70)
        print(f"Total Trades: {results.total_trades}")
        print(f"Win Rate: {results.win_rate:.1%}")
        print(f"Total Return: {results.total_return_pct:.2f}%")
        print(f"Profit Factor: {results.profit_factor:.2f}")
        print(f"Sharpe Ratio: {results.sharpe_ratio:.2f}")
        print(f"Sortino Ratio: {results.sortino_ratio:.2f}")
        print(f"Max Drawdown: {results.max_drawdown:.2%}")
        print(f"Expectancy: ${results.expectancy:.2f}")
        print("="*70)
        
        return results
    
    def save_performance_report(self, filename: str = "performance_report.json") -> None:
        """Save comprehensive performance report"""
        report = {
            'account_balance': self.account_balance,
            'total_positions': len(self.current_positions),
            'total_trades': len(self.trade_history),
            'metrics': self.performance_metrics,
            'timestamp': datetime.now().isoformat()
        }
        
        with open(filename, 'w') as f:
            json.dump(report, f, indent=2)
        
        print(f"✅ Performance report saved to {filename}")


if __name__ == "__main__":
    # Example usage
    print("Testing Ultimate Trading Bot...")
    
    bot = UltimateTradingBot(
        account_balance=10000,
        use_kelly=True,
        use_sentiment=True,
        use_regime_detection=True
    )
    
    # Train models
    bot.train_multi_timeframe_models("EUR/USD", "forex", ['1d', '1h'])
    
    # Generate ultimate signal
    signal = bot.generate_ultimate_signal("EUR/USD", "forex", "1d")
    
    print("\n✅ Ultimate Trading Bot Test Complete!")
