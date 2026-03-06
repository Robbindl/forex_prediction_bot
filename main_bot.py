#!/usr/bin/env python3
"""
Forex & Multi-Asset Prediction Bot with Trading Signals
Main orchestrator with Entry, Stop Loss, and Take Profit
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import pandas as pd
from datetime import datetime
import time
from typing import Dict, Tuple, Any
import argparse

from config.config import *
from data.fetcher import DataFetcher
from indicators.technical import TechnicalIndicators
from models.predictor import PredictionEngine
from utils.analysis import MarketAnalyzer, AlertSystem, ReportGenerator
from utils.trading_signals import TradingSignalGenerator


class ForexPredictionBot:
    """
    Main trading bot orchestrator with complete trading signals
    
    ⚠️  DISCLAIMER: This bot is for educational and informational purposes only.
    Trading financial instruments carries substantial risk of loss. Past 
    performance is not indicative of future results. Always do your own research
    and consider consulting with a licensed financial advisor before making
    investment decisions.
    """
    
    def __init__(self, model_type: str = "ensemble") -> None:
        """
        Initialize the bot
        
        Args:
            model_type: ML model type ('rf', 'xgboost', 'lstm', 'ensemble')
        """
        print("\n" + "="*70)
        print("FOREX & MULTI-ASSET PREDICTION BOT")
        print("With Entry Signals, Stop Loss & Take Profit")
        print("="*70)
        print("\n⚠️  DISCLAIMER: For educational purposes only.")
        print("This is NOT financial advice. Trade at your own risk.\n")
        
        self.fetcher = DataFetcher()
        self.model_type = model_type
        self.models: Dict[str, Any] = {}
        self.data_cache: Dict[str, pd.DataFrame] = {}
        
    def fetch_all_market_data(self, interval: str = "1d") -> Dict[str, pd.DataFrame]:
        """Fetch data for all configured assets"""
        print(f"\n{'='*70}")
        print(f"FETCHING MARKET DATA (Interval: {interval})")
        print(f"{'='*70}\n")
        
        assets = {
            'forex': FOREX_PAIRS,
            'stocks': STOCKS,
            'commodities': COMMODITIES,
            'indices': INDICES
        }
        
        all_data = self.fetcher.fetch_multiple_assets(assets, interval)
        self.data_cache = all_data
        
        print(f"\n✓ Successfully fetched data for {len(all_data)} assets")
        return all_data
    
    def add_technical_indicators(self, data_dict: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """Add technical indicators to all assets"""
        print(f"\n{'='*70}")
        print("CALCULATING TECHNICAL INDICATORS")
        print(f"{'='*70}\n")
        
        enhanced_data = {}
        for name, df in data_dict.items():
            if not df.empty:
                print(f"  Processing {name}...")
                enhanced_df = TechnicalIndicators.add_all_indicators(df)
                enhanced_df = TechnicalIndicators.detect_patterns(enhanced_df)
                enhanced_data[name] = enhanced_df
        
        print(f"\n✓ Added indicators to {len(enhanced_data)} assets")
        return enhanced_data
    
    def train_models(self, data_dict: Dict[str, pd.DataFrame], 
                    target_periods: int = PREDICTION_HORIZON) -> None:
        """Train prediction models for all assets"""
        print(f"\n{'='*70}")
        print(f"TRAINING PREDICTION MODELS ({self.model_type.upper()})")
        print(f"{'='*70}\n")
        
        for name, df in data_dict.items():
            if len(df) < 50:
                print(f"  Skipping {name}: Insufficient data")
                continue
            
            print(f"\n  Training model for {name}...")
            try:
                engine = PredictionEngine(model_type=self.model_type)
                engine.train(df, target_periods=target_periods)
                self.models[name] = engine
                print(f"  ✓ {name} model trained successfully")
            except Exception as e:
                print(f"  ✗ Failed to train {name}: {e}")
        
        print(f"\n✓ Trained {len(self.models)} models")
    
    def generate_predictions_and_signals(self, data_dict: Dict[str, pd.DataFrame]) -> Dict[str, Dict[str, Any]]:
        """Generate ML predictions AND trading signals with SL/TP"""
        print(f"\n{'='*70}")
        print("GENERATING TRADING SIGNALS (Entry, SL, TP)")
        print(f"{'='*70}\n")
        
        all_signals: Dict[str, Dict[str, Any]] = {}
        
        for name, df in data_dict.items():
            # Get ML prediction if model exists
            prediction = None
            if name in self.models:
                try:
                    prediction = self.models[name].predict_next(df)
                except Exception as e:
                    print(f"  ⚠️  Prediction failed for {name}: {e}")
            
            # Generate complete trading signal
            signal = TradingSignalGenerator.generate_entry_signal(df, prediction)
            
            # Combine prediction and signal data
            all_signals[name] = {
                **signal,
                'ml_prediction': prediction
            }
            
            # Display signal
            signal_emoji = "📈" if signal['signal'] == "BUY" else "📉" if signal['signal'] == "SELL" else "⏸️"
            print(f"\n  {name}: {signal_emoji} {signal['signal']}")
            print(f"    Entry: {signal['entry_price']:.5f}")
            
            if signal['signal'] != "HOLD":
                print(f"    Stop Loss: {signal['stop_loss']:.5f} ({signal['risk_pct']:.2f}% risk)")
                print(f"    Take Profits:")
                for i, tp in enumerate(signal['take_profit_levels'], 1):
                    print(f"      TP{i}: {tp['price']:.5f} (R:R {tp['risk_reward']}:1, +{tp['potential_gain_pct']:.2f}%)")
                print(f"    Confidence: {signal['confidence']:.0%}")
                print(f"    Reason: {signal['reason']}")
        
        return all_signals
    
    def generate_trade_recommendations(self, signals: Dict[str, Dict[str, Any]], 
                                      account_balance: float = 10000,
                                      risk_per_trade: float = 1.0) -> None:
        """Generate position sizing and trade recommendations"""
        print(f"\n{'='*70}")
        print("TRADE RECOMMENDATIONS")
        print(f"Account: ${account_balance:,.2f} | Risk per trade: {risk_per_trade}%")
        print(f"{'='*70}\n")
        
        # Filter for BUY/SELL signals only
        actionable_signals = {k: v for k, v in signals.items() 
                             if v['signal'] in ['BUY', 'SELL']}
        
        if not actionable_signals:
            print("  No actionable signals at this time.")
            return
        
        # Sort by confidence
        sorted_signals = sorted(
            actionable_signals.items(),
            key=lambda x: x[1]['confidence'],
            reverse=True
        )
        
        print("Top Trading Opportunities:\n")
        for i, (name, signal) in enumerate(sorted_signals[:5], 1):
            print(f"{i}. {name} - {signal['signal']}")
            print(f"   Entry: {signal['entry_price']:.5f}")
            print(f"   Stop Loss: {signal['stop_loss']:.5f}")
            print(f"   TP1: {signal['take_profit_levels'][0]['price']:.5f} (1.5:1)")
            print(f"   TP2: {signal['take_profit_levels'][1]['price']:.5f} (2:1)")
            print(f"   TP3: {signal['take_profit_levels'][2]['price']:.5f} (3:1)")
            print(f"   Confidence: {signal['confidence']:.0%}")
            print(f"   Risk: {signal['risk_pct']:.2f}%")
            
            # Calculate position size
            position = TradingSignalGenerator.calculate_position_size(
                account_balance,
                risk_per_trade,
                signal['entry_price'],
                signal['stop_loss']
            )
            
            print(f"   Position Size: {position['position_size']:.2f} units")
            print(f"   Max Loss: ${position['max_loss']:.2f}")
            print(f"   Potential Gains: TP1 ${position['max_loss'] * 1.5:.2f} | " 
                  f"TP2 ${position['max_loss'] * 2:.2f} | TP3 ${position['max_loss'] * 3:.2f}")
            print()
    
    def analyze_correlations(self, data_dict: Dict[str, pd.DataFrame]) -> Tuple[pd.DataFrame, Any]:
        """Analyze correlations between assets"""
        print(f"\n{'='*70}")
        print("CORRELATION ANALYSIS")
        print(f"{'='*70}\n")
        
        corr_matrix = MarketAnalyzer.calculate_correlation_matrix(data_dict)
        high_corrs = MarketAnalyzer.find_correlated_assets(
            corr_matrix, 
            threshold=MAX_CORRELATION_THRESHOLD
        )
        
        if high_corrs:
            print(f"Found {len(high_corrs)} highly correlated pairs:\n")
            for asset1, asset2, corr in high_corrs[:10]:
                print(f"  {asset1} ↔ {asset2}: {corr:.3f}")
        else:
            print("No high correlations found above threshold")
        
        return corr_matrix, high_corrs
    
    def generate_alerts(self, data_dict: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """Generate alerts for all assets"""
        print(f"\n{'='*70}")
        print("ALERT MONITORING")
        print(f"{'='*70}\n")
        
        all_alerts: Dict[str, Any] = {}
        total_alerts = 0
        
        for name, df in data_dict.items():
            alerts = AlertSystem.generate_all_alerts(df)
            if alerts:
                all_alerts[name] = alerts
                total_alerts += len(alerts)
                
                print(f"\n  {name}:")
                for alert in alerts:
                    severity_emoji = {"low": "ℹ️", "medium": "⚠️", "high": "🚨"}.get(
                        alert['severity'], "•"
                    )
                    print(f"    {severity_emoji} [{alert['type']}] {alert['message']}")
                    print(f"      → {alert['action']}")
        
        if total_alerts == 0:
            print("  No alerts at this time")
        else:
            print(f"\n✓ Generated {total_alerts} alerts across {len(all_alerts)} assets")
        
        return all_alerts
    
    def run_full_analysis(self, interval: str = "1d", train_models: bool = True,
                         account_balance: float = 10000, risk_per_trade: float = 1.0) -> None:
        """
        Run complete analysis with trading signals
        
        Args:
            interval: Time interval ('1d', '1h', '15m')
            train_models: Whether to train new models
            account_balance: Account balance for position sizing
            risk_per_trade: Risk percentage per trade
        """
        start_time = time.time()
        
        # Step 1: Fetch data
        data = self.fetch_all_market_data(interval)
        
        if not data:
            print("❌ No data fetched. Check your API keys and internet connection.")
            return
        
        # Step 2: Add indicators
        data = self.add_technical_indicators(data)
        
        # Step 3: Train models (if requested)
        if train_models:
            self.train_models(data)
        
        # Step 4: Generate predictions AND trading signals
        signals = self.generate_predictions_and_signals(data)
        
        # Step 5: Generate trade recommendations
        self.generate_trade_recommendations(signals, account_balance, risk_per_trade)
        
        # Step 6: Correlation analysis
        self.analyze_correlations(data)
        
        # Step 7: Generate alerts
        self.generate_alerts(data)
        
        # Summary
        elapsed = time.time() - start_time
        actionable_count = sum(1 for s in signals.values() if s['signal'] != 'HOLD')
        
        print(f"\n{'='*70}")
        print(f"ANALYSIS COMPLETE")
        print(f"{'='*70}")
        print(f"  Time elapsed: {elapsed:.2f}s")
        print(f"  Assets analyzed: {len(data)}")
        print(f"  Models trained: {len(self.models)}")
        print(f"  Actionable signals: {actionable_count}")
        print(f"{'='*70}\n")
    
    def watch_single_asset(self, asset_name: str, asset_type: str = "forex",
                          interval: str = "15m", duration_minutes: int = 60) -> None:
        """
        Watch a single asset with real-time trading signals
        
        Args:
            asset_name: Asset symbol
            asset_type: 'forex', 'stock', 'commodity', 'index'
            interval: Time interval
            duration_minutes: How long to watch
        """
        print(f"\n{'='*70}")
        print(f"WATCHING {asset_name} ({asset_type.upper()})")
        print(f"Interval: {interval} | Duration: {duration_minutes}m")
        print(f"{'='*70}\n")
        
        iterations = duration_minutes // 1
        
        for i in range(iterations):
            print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Update #{i+1}/{iterations}")
            
            # Fetch latest data
            if asset_type == "forex":
                df = self.fetcher.fetch_forex_data(asset_name, interval, lookback=100)
            elif asset_type == "stock":
                df = self.fetcher.fetch_stock_data(asset_name, interval, lookback=100)
            elif asset_type == "commodity":
                df = self.fetcher.fetch_commodity_data(asset_name, interval, lookback=100)
            elif asset_type == "index":
                df = self.fetcher.fetch_index_data(asset_name, interval, lookback=100)
            else:
                print(f"Unknown asset type: {asset_type}")
                return
            
            if df.empty:
                print("  ❌ Failed to fetch data")
                continue
            
            # Add indicators
            df = TechnicalIndicators.add_all_indicators(df)
            
            # Get ML prediction if model exists
            prediction = None
            key = f"{asset_type}_{asset_name}"
            if key in self.models:
                prediction = self.models[key].predict_next(df)
            
            # Generate trading signal
            signal = TradingSignalGenerator.generate_entry_signal(df, prediction)
            
            # Display current state
            latest = df.iloc[-1]
            print(f"  Price: {latest['close']:.4f}")
            print(f"  RSI: {latest['rsi']:.2f}")
            print(f"  MACD: {latest['macd']:.4f}")
            
            # Display trading signal
            signal_emoji = "📈" if signal['signal'] == "BUY" else "📉" if signal['signal'] == "SELL" else "⏸️"
            print(f"\n  {signal_emoji} SIGNAL: {signal['signal']}")
            
            if signal['signal'] != "HOLD":
                print(f"  Entry: {signal['entry_price']:.4f}")
                print(f"  Stop Loss: {signal['stop_loss']:.4f}")
                print(f"  TP1: {signal['take_profit_levels'][0]['price']:.4f} (1.5:1)")
                print(f"  TP2: {signal['take_profit_levels'][1]['price']:.4f} (2:1)")
                print(f"  Confidence: {signal['confidence']:.0%}")
                print(f"  Reason: {signal['reason']}")
            
            if i < iterations - 1:
                time.sleep(60)
        
        print(f"\n✓ Monitoring complete")


def main() -> None:
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Forex & Multi-Asset Prediction Bot with Trading Signals",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main_bot.py --full-analysis
  python main_bot.py --full-analysis --balance 5000 --risk 2.0
  python main_bot.py --watch "EUR/USD" --type forex --interval 15m
  python main_bot.py --watch "AAPL" --type stock
        """
    )
    
    parser.add_argument('--full-analysis', action='store_true',
                       help='Run full market analysis')
    parser.add_argument('--watch', type=str,
                       help='Watch a single asset')
    parser.add_argument('--type', type=str, default='forex',
                       choices=['forex', 'stock', 'commodity', 'index'],
                       help='Asset type for watch mode')
    parser.add_argument('--interval', type=str, default='1d',
                       choices=['1d', '1h', '15m'],
                       help='Time interval')
    parser.add_argument('--model', type=str, default='ensemble',
                       choices=['rf', 'xgboost', 'lstm', 'ensemble'],
                       help='ML model type')
    parser.add_argument('--no-train', action='store_true',
                       help='Skip model training')
    parser.add_argument('--duration', type=int, default=60,
                       help='Watch duration in minutes')
    parser.add_argument('--balance', type=float, default=10000,
                       help='Account balance for position sizing')
    parser.add_argument('--risk', type=float, default=1.0,
                       help='Risk percentage per trade')
    
    args = parser.parse_args()
    
    # Create bot instance
    bot = ForexPredictionBot(model_type=args.model)
    
    if args.full_analysis:
        bot.run_full_analysis(
            interval=args.interval,
            train_models=not args.no_train,
            account_balance=args.balance,
            risk_per_trade=args.risk
        )
    
    elif args.watch:
        bot.watch_single_asset(
            asset_name=args.watch,
            asset_type=args.type,
            interval=args.interval,
            duration_minutes=args.duration
        )
    
    else:
        print("No mode specified. Running full analysis...")
        print("Use --help to see all options\n")
        bot.run_full_analysis()


if __name__ == "__main__":
    main()
