"""
ULTIMATE PROFESSIONAL TRADING SYSTEM
Everything integrated: Backtesting + ML + Paper Trading + Broker Ready
"""

import sys
import os
import time
from datetime import datetime, timedelta
import threading
from typing import Dict, Optional, List
import json
import pandas as pd
import numpy as np
import yfinance as yf
import argparse
from pathlib import Path

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from data.fetcher import NASALevelFetcher, MarketHours
from advanced_predictor import AdvancedPredictionEngine
from advanced_risk_manager import AdvancedRiskManager
from advanced_backtester import AdvancedBacktester
from paper_trader import PaperTrader
from training_monitor import TrainingMonitor
from monitor import TradingMonitor
from portfolio_optimizer import EnhancedPortfolioOptimizer
from sentiment_analyzer import SentimentAnalyzer
from concurrent.futures import ThreadPoolExecutor, as_completed
from strategies.voting_engine import StrategyVotingEngine
from auto_train_intelligent import IntelligentAutoTrainer
from advanced_ai import AdvancedAIIntegration
from market_regime_analyzer import MarketRegimeDetector
from model_registry import ModelRegistry
from cache_manager import CacheManager
from strategy_optimizer import StrategyOptimizer
from session_tracker import SessionTracker

# ===== DYNAMIC POSITION SIZER =====
try:
    from advanced_risk_manager import DynamicPositionSizer
    DYNAMIC_SIZER_AVAILABLE = True
except ImportError:
    DYNAMIC_SIZER_AVAILABLE = False
    print("⚠️ Dynamic Position Sizer not available")


class UltimateTradingSystem:
    """
    ULTIMATE PROFESSIONAL TRADING SYSTEM
    """
    
    def __init__(self, account_balance: float = 10000, strategy_mode: str = 'balanced'):
        print("\n" + "="*60)
        print(" ULTIMATE PROFESSIONAL TRADING SYSTEM")
        print("="*60 + "\n")
        
        # Core Components
        from data.fetcher import NASALevelFetcher, MarketHours
        from advanced_predictor import AdvancedPredictionEngine
        from advanced_risk_manager import AdvancedRiskManager
        from advanced_backtester import AdvancedBacktester
        from paper_trader import PaperTrader
        from training_monitor import TrainingMonitor
        from monitor import TradingMonitor
        from portfolio_optimizer import EnhancedPortfolioOptimizer
        from sentiment_analyzer import SentimentAnalyzer
        from concurrent.futures import ThreadPoolExecutor, as_completed
        from strategies.voting_engine import StrategyVotingEngine
        from auto_train_intelligent import IntelligentAutoTrainer
        from advanced_ai import AdvancedAIIntegration
        from market_regime_analyzer import MarketRegimeDetector
        from session_tracker import SessionTracker  # ← ADD THIS LINE
        from model_registry import ModelRegistry
        from cache_manager import CacheManager
        from strategy_optimizer import StrategyOptimizer
        from profitability_upgrade import apply_upgrades, cooldown_tracker, category_limiter, position_age_monitor
        
         # Core Components (create these FIRST)
        self.fetcher = NASALevelFetcher()
        self.predictor = AdvancedPredictionEngine("super_ensemble")
        self.risk_manager = AdvancedRiskManager(account_balance)
        self.backtester = AdvancedBacktester(initial_capital=account_balance)
        self.monitor = TrainingMonitor()
        
        # ===== CREATE PAPER TRADER HERE (BEFORE using it) =====
        self.paper_trader = PaperTrader(self.risk_manager)
        # ======================================================
        
        self.strategy_mode = strategy_mode
        self.auto_trainer = IntelligentAutoTrainer(self)
        self.ai_system = AdvancedAIIntegration()

        # ===== PROFITABILITY UPGRADES =====
        try:
            from profitability_upgrade import apply_upgrades, cooldown_tracker, category_limiter, position_age_monitor
            apply_upgrades(self)
            self.cooldown_tracker = cooldown_tracker
            self.category_limiter = category_limiter
            self.position_age_monitor = position_age_monitor
            self.profitability_upgrades_active = True
            print("✅ PROFITABILITY UPGRADES: ACTIVE")
            print("   • 60-min cooldown after losses")
            print("   • Category position limits (1 crypto, 2 forex)")
            print("   • ATR-based stop losses")
            print("   • Entry quality filter")
            print("   • 4-hour position age limit")
        except ImportError as e:
            print("⚠️ PROFITABILITY UPGRADES: NOT INSTALLED")
            print(f"   Run: python profitability_upgrade.py")
            self.profitability_upgrades_active = False
        except Exception as e:
            print(f"⚠️ PROFITABILITY UPGRADES ERROR: {e}")
            self.profitability_upgrades_active = False
        # ====================================================
        
        # ===== STRATEGIES DEFINED FIRST (BEFORE voting engine) =====
        self.strategies = {
            'rsi': self.rsi_strategy,
            'macd': self.macd_strategy,
            'bb': self.bollinger_strategy,
            'ma_cross': self.ma_cross_strategy,
            'ml_ensemble': self.ml_ensemble_strategy,
            'ultimate': self.ultimate_indicator_strategy,
            'breakout': self.breakout_strategy,
            'mean_reversion': self.mean_reversion_strategy,
            'trend_following': self.trend_following_strategy,
            'scalping': self.scalping_strategy,
            'arbitrage': self.arbitrage_strategy,
            'day_trading': self.day_trading_strategy,
            'news_sentiment': self.news_sentiment_strategy
        }
        
        # ===== VOTING ENGINE INITIALIZED SECOND (can access strategies) =====
        self.voting_engine = StrategyVotingEngine(self)
        
        # Connect voting engine to paper trader
        self.paper_trader.voting_engine = self.voting_engine

        # ===== LOAD ALERT CONFIGURATIONS =====
        import json
        import os
        
        # Load Telegram config
        telegram_config = None
        if os.path.exists('config/telegram_config.json'):
            try:
                with open('config/telegram_config.json', 'r') as f:
                    telegram_config = json.load(f)
                print(" Telegram config loaded")
            except:
                print(" Could not load Telegram config")
        
        # Load Email config
        email_config = None
        if os.path.exists('config/email_config.json'):
            try:
                with open('config/email_config.json', 'r') as f:
                    email_config = json.load(f)
                print(" Email config loaded")
            except:
                print(" Could not load Email config")
        
        # Initialize monitor WITH alert channels
        self.live_monitor = TradingMonitor(
            risk_manager=self.risk_manager,
            paper_trader=self.paper_trader,
            email_config=email_config if email_config and email_config.get('enabled') else None,
            telegram_config=telegram_config if telegram_config and telegram_config.get('enabled') else None
        )
        
        # Connect monitor to paper trader for alerts
        self.paper_trader.monitor = self.live_monitor
        
        # ===== ENHANCED PORTFOLIO OPTIMIZER =====
        try:
            self.portfolio_optimizer = EnhancedPortfolioOptimizer(
                max_allocation=0.3, 
                max_correlation=0.7
            )
            print("✅ PORTFOLIO OPTIMIZER: ACTIVE")
            print("   • Max allocation per asset: 30%")
            print("   • Max correlation threshold: 0.7")
            print("   • VaR tracking enabled")
        except Exception as e:
            print(f"⚠️ Could not initialize portfolio optimizer: {e}")
            self.portfolio_optimizer = None
        
        # ===== SENTIMENT ANALYZER =====
        try:
            self.sentiment_analyzer = SentimentAnalyzer()
            print("✅ SENTIMENT ANALYZER: ACTIVE")
        except Exception as e:
            print(f"⚠️ Could not initialize sentiment analyzer: {e}")
            self.sentiment_analyzer = None
        
        # ===== MARKET REGIME DETECTION =====
        try:
            self.regime_detector = MarketRegimeDetector()
            self.regime_history = []
            self.current_regime = None
            self.regime_confidence = 0.0
            print("✅ MARKET REGIME DETECTION: ACTIVE")
        except Exception as e:
            print(f"⚠️ Could not initialize regime detector: {e}")
            self.regime_detector = None

        # ===== STRATEGY OPTIMIZER =====
        try:
            self.strategy_optimizer = StrategyOptimizer(self.backtester)
            print("✅ STRATEGY OPTIMIZER: ACTIVE")
            print("   • Grid search optimization")
            print("   • Finds best parameters for all strategies")
        except Exception as e:
            print(f"⚠️ Could not initialize strategy optimizer: {e}")
            self.strategy_optimizer = None
        
        # ===== DYNAMIC POSITION SIZER =====
        try:
            if DYNAMIC_SIZER_AVAILABLE:
                self.position_sizer = DynamicPositionSizer(
                    base_risk=0.01,  # 1% base risk
                    max_risk=0.03    # 3% maximum risk
                )
                print("✅ DYNAMIC POSITION SIZER: ACTIVE")
                print("   • Base risk: 1%")
                print("   • Max risk: 3%")
                print("   • Adapts to confidence, volatility, regime, win rate")
            else:
                self.position_sizer = None
                print("⚠️ DYNAMIC POSITION SIZER: Not available")
        except Exception as e:
            print(f"⚠️ Could not initialize position sizer: {e}")
            self.position_sizer = None
        
        # ===== TRACKING VARIABLES =====
        self.multi_timeframe = True
        self.current_strategy = 'scalping'  # Default strategy
        self.is_running = False
        self.last_day = datetime.now().date()
        self.health_check_counter = 0  # For periodic portfolio health checks
        self.current_asset = None  # For scalping strategy

        # ===== DAILY LOSS LIMIT =====  <-- ADD THIS HERE
        try:
            from advanced_risk_manager import DailyLossLimit
            self.daily_loss_limit = DailyLossLimit(
                max_loss_pct=3.0,  # 3% max daily loss
                alert_callback=self.send_loss_limit_alert
            )
            # Set initial balance
            self.daily_loss_limit.set_initial_balance(account_balance)
            print("✅ DAILY LOSS LIMIT: ACTIVE")
            print("   • Max daily loss: 3%")
            print("   • Auto-pause: 1 hour when limit hit")
        except Exception as e:
            print(f"⚠️ Could not initialize daily loss limit: {e}")
            self.daily_loss_limit = None
        # ============================

        # ===== ALERT CALLBACK METHODS =====
        def send_loss_limit_alert(self, message: str):
            """Send alert when daily loss limit is hit"""
            print(f"\n🔴 {message}")
            
            # Also send via monitor if available
            if hasattr(self, 'live_monitor') and self.live_monitor:
                try:
                    self.live_monitor._send_alert(
                        'CRITICAL',
                        'Daily Loss Limit Hit',
                        message
                    )
                except:
                    pass
        # ==================================

        # ===== MODEL REGISTRY =====
        try:
            self.model_registry = ModelRegistry(registry_file="model_registry.json")
            print("✅ MODEL REGISTRY: ACTIVE")
            print("   • Tracks ML model performance")
            print("   • Auto-selects best models per asset")
        except Exception as e:
            print(f"⚠️ Could not initialize model registry: {e}")
            self.model_registry = None

        # ===== CACHE MANAGER =====
        try:
            self.cache_manager = CacheManager(
                host='localhost',
                port=6379,
                db=0,
                password=None
            )
            print("✅ CACHE MANAGER: ACTIVE")
            
            # Connect cache manager to portfolio optimizer
            if hasattr(self, 'portfolio_optimizer') and self.portfolio_optimizer:
                self.portfolio_optimizer.set_cache_manager(self.cache_manager)
                
        except Exception as e:
            print(f"⚠️ Could not initialize cache manager: {e}")
            self.cache_manager = None
        # ========================

        # ===== SESSION TRACKER =====
        try:
            from session_tracker import SessionTracker
            self.session_tracker = SessionTracker()
            print("✅ SESSION TRACKER: ACTIVE")
            print("   • Tracks performance by trading session")
            print("   • Asian, London, New York sessions")
            print("   • Identifies best times to trade")
        except Exception as e:
            print(f"⚠️ Could not initialize session tracker: {e}")
            self.session_tracker = None
        # ============================
        
        # ===== CREATE RESULTS DIRECTORIES =====
        Path("backtest_results").mkdir(exist_ok=True)
        Path("ml_models").mkdir(exist_ok=True)
        Path("trade_logs").mkdir(exist_ok=True)
        Path("portfolio_reports").mkdir(exist_ok=True)  # For saving health reports
        
        print("\n" + "="*60)
        print(" ALL SYSTEMS INITIALIZED")
        print("="*60 + "\n")
    
    # ============= PROFESSIONAL TRADING STRATEGIES =============
    
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
    
    def ml_ensemble_strategy(self, df: pd.DataFrame) -> List[Dict]:
        """Machine Learning Ensemble Strategy"""
        signals = []
        try:
            # Get ML prediction
            prediction = self.predictor.predict_next(df)

            if hasattr(self, 'model_registry') and self.model_registry and 'asset' in self.__dict__:
                # Store prediction for later verification
                if not hasattr(self, '_pending_predictions'):
                    self._pending_predictions = []
                
                self._pending_predictions.append({
                    'asset': self.current_asset,
                    'timestamp': datetime.now(),
                    'prediction': prediction,
                    'price': df['close'].iloc[-1]
                })
            # ========================================
            
            if prediction['confidence'] > 0.7:
                current_price = df['close'].iloc[-1]
                
                if prediction['direction'] == 'UP':
                    signals.append({
                        'date': df.index[-1],
                        'signal': 'BUY',
                        'confidence': prediction['confidence'],
                        'entry': current_price,
                        'stop_loss': current_price * 0.97,
                        'take_profit': current_price * 1.08,
                        'strategy': 'ml_ensemble',
                        'ml_details': prediction
                    })
                elif prediction['direction'] == 'DOWN':
                    signals.append({
                        'date': df.index[-1],
                        'signal': 'SELL',
                        'confidence': prediction['confidence'],
                        'entry': current_price,
                        'stop_loss': current_price * 1.03,
                        'take_profit': current_price * 0.92,
                        'strategy': 'ml_ensemble',
                        'ml_details': prediction
                    })
        except Exception as e:
            print(f" ML prediction error: {e}")
        
        return signals
    
    def verify_pending_predictions(self):
        """Check pending predictions against actual price movements"""
        if not hasattr(self, '_pending_predictions') or not self._pending_predictions:
            return
        
        to_remove = []
        
        for pred in self._pending_predictions:
            try:
                # Get current price for the asset
                asset = pred['asset']
                price, _ = self.fetcher.get_real_time_price(asset, 'unknown')
                
                if price:
                    # Calculate actual movement
                    actual_move = (price - pred['price']) / pred['price'] * 100
                    
                    # Update registry
                    self.update_model_prediction(asset, pred['prediction'], actual_move)
                    to_remove.append(pred)
            except Exception as e:
                print(f"   ⚠️ Failed to verify prediction: {e}")
        
        # Remove verified predictions
        for pred in to_remove:
            self._pending_predictions.remove(pred)

    # ============= COMPREHENSIVE BACKTESTING =============

    def show_model_performance(self):
        """Display model performance report"""
        report = self.get_model_performance_report()
        
        if 'error' in report:
            print(f"\n❌ {report['error']}")
            return
        
        print("\n" + "="*70)
        print("📊 MODEL PERFORMANCE REPORT")
        print("="*70)
        print(f"Total Models: {report['total_models']}")
        print(f"Active Models: {report['active_models']}")
        print(f"Average Accuracy: {report['avg_accuracy']:.1%}")
        
        if report['best_models']:
            print("\n🏆 TOP PERFORMING MODELS:")
            for i, model in enumerate(report['best_models'], 1):
                print(f"\n  {i}. {model['asset']} - {model['model_name']}")
                print(f"     Accuracy: {model['accuracy']:.1%}")
                print(f"     Trade Win Rate: {model['trade_win_rate']:.1%}")
                print(f"     Predictions: {model['total_predictions']}")
    
    def backtest_asset(self, asset: str, lookback_days: int = 365):
        """Backtest a single asset with all strategies"""
        print(f"\n📊 Testing {asset}...")
        
        # Fetch data
        df = self.fetch_historical_data(asset, lookback_days)
        if df.empty:
            print(f" No data for {asset}")
            return None
        
        # Add indicators
        df = self.add_technical_indicators(df)
        
        results = []
        
        # Test each strategy
        for strategy_name, strategy_func in self.strategies.items():
            print(f"  • Testing {strategy_name}...")
            
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
                 print(f"  💾 Saved to {safe_filename}")
            except Exception as e:
                 print(f"   Could not save file: {e}")
        
        # Display results
        if results:
            results_df = pd.DataFrame(results)
            results_df = results_df.sort_values('profit_factor', ascending=False)
            
            print("\n" + "="*30)
            print(f"RESULTS FOR {asset}")
            print("="*30)
            print(results_df.to_string())
            
            # Find best strategy
            best = results_df.iloc[0]
            print(f"\n🏆 BEST STRATEGY: {best['strategy']}")
            print(f"   Win Rate: {best['win_rate']:.1%}")
            print(f"   Return: {best['total_return']:.1f}%")
            print(f"   Profit Factor: {best['profit_factor']:.2f}")
            
            # Save summary
            safe_asset = asset.replace('/', '_').replace('\\', '_').replace(':', '_')
            summary_filename = f'backtest_results/{safe_asset}_summary.csv'
            try:
                results_df.to_csv(summary_filename, index=False)
                print(f"   Summary saved to {summary_filename}")
            except Exception as e:
                print(f"   Could not save summary: {e}")
        
            return results_df
        return None
    
    def backtest_all_strategies(self, assets: List[str], lookback_days: int = 365):
        """Backtest all strategies on multiple assets"""
        print("\n" + "="*60)
        print(" COMPREHENSIVE STRATEGY BACKTEST")
        print("="*60)
        
        all_results = []
        
        for asset in assets:
            result = self.backtest_asset(asset, lookback_days)
            if result is not None:
                all_results.append(result)
        
        # Combine all results
        if all_results:
            combined = pd.concat(all_results)
            combined.to_csv('backtest_results/all_strategies_comparison.csv', index=False)
            
            print("\n" + "="*30)
            print("OVERALL BEST STRATEGIES")
            print("="*30)
            
            # Group by strategy and find average
            avg_results = combined.groupby('strategy').agg({
                'win_rate': 'mean',
                'total_return': 'mean',
                'profit_factor': 'mean',
                'sharpe': 'mean'
            }).sort_values('profit_factor', ascending=False)
            
            print(avg_results.to_string())
            
            # Set best strategy as default
            best_strategy = avg_results.index[0]
            self.current_strategy = best_strategy
            print(f"\n Default strategy set to: {self.current_strategy}")
    
    def optimize_strategy(self, asset: str, strategy: str, lookback_days: int = 365):
        """Optimize strategy parameters"""
        print(f"\n🔧 Optimizing {strategy} for {asset}...")
        
        df = self.fetch_historical_data(asset, lookback_days)
        if df.empty:
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
                print(f"\n Best RSI settings:")
                print(f"   Oversold: {best['oversold']}")
                print(f"   Overbought: {best['overbought']}")
                print(f"   Profit Factor: {best['profit_factor']:.2f}")
                
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
                print(f"\n Best MACD settings:")
                print(f"   Fast: {best['fast']}")
                print(f"   Slow: {best['slow']}")
                print(f"   Signal: {best['signal']}")
                print(f"   Profit Factor: {best['profit_factor']:.2f}")
                
                results_df.to_csv(f'backtest_results/{strategy}_optimization.csv', index=False)
    def optimize_all_strategies(self, asset: str, lookback_days: int = 365):
        """
        Optimize ALL 50+ strategies for a given asset
        Returns comprehensive optimization results for every strategy
        """
        print(f"\n{'='*70}")
        print(f"🔧 OPTIMIZING ALL 50+ STRATEGIES FOR {asset}")
        print(f"{'='*70}")
        
        # Fetch data
        df = self.fetch_historical_data(asset, lookback_days)
        if df.empty:
            print(f"❌ No data for {asset}")
            return None
        
        df = self.add_technical_indicators(df)
        
        # Store all optimization results
        all_results = {}
        
        # ===== 1. MOMENTUM INDICATORS =====
        print("\n📈 OPTIMIZING MOMENTUM INDICATORS...")
        
        # RSI Family
        all_results['rsi'] = self.strategy_optimizer.optimize_rsi(df, asset)
        all_results['rsi_divergence'] = self.strategy_optimizer.optimize_rsi_divergence(df, asset)
        all_results['stoch_rsi'] = self.strategy_optimizer.optimize_stoch_rsi(df, asset)
        
        # Stochastic
        all_results['stochastic'] = self.strategy_optimizer.optimize_stochastic(df, asset)
        all_results['stochastic_fast'] = self.strategy_optimizer.optimize_stochastic_fast(df, asset)
        all_results['stochastic_full'] = self.strategy_optimizer.optimize_stochastic_full(df, asset)
        
        # MACD Family
        all_results['macd'] = self.strategy_optimizer.optimize_macd(df, asset)
        all_results['macd_histogram'] = self.strategy_optimizer.optimize_macd_histogram(df, asset)
        all_results['macd_divergence'] = self.strategy_optimizer.optimize_macd_divergence(df, asset)
        
        # Other Momentum
        all_results['cci'] = self.strategy_optimizer.optimize_cci(df, asset)  # Commodity Channel Index
        all_results['williams_r'] = self.strategy_optimizer.optimize_williams_r(df, asset)
        all_results['mfi'] = self.strategy_optimizer.optimize_mfi(df, asset)  # Money Flow Index
        all_results['uo'] = self.strategy_optimizer.optimize_uo(df, asset)    # Ultimate Oscillator
        all_results['apo'] = self.strategy_optimizer.optimize_apo(df, asset)  # Absolute Price Oscillator
        all_results['ppo'] = self.strategy_optimizer.optimize_ppo(df, asset)  # Percentage Price Oscillator
        
        # ===== 2. TREND INDICATORS =====
        print("\n📊 OPTIMIZING TREND INDICATORS...")
        
        # Moving Averages
        all_results['sma_cross'] = self.strategy_optimizer.optimize_sma_cross(df, asset)
        all_results['ema_cross'] = self.strategy_optimizer.optimize_ema_cross(df, asset)
        all_results['wma_cross'] = self.strategy_optimizer.optimize_wma_cross(df, asset)  # Weighted MA
        all_results['hma_cross'] = self.strategy_optimizer.optimize_hma_cross(df, asset)  # Hull MA
        all_results['vwap'] = self.strategy_optimizer.optimize_vwap(df, asset)  # Volume Weighted
        
        # ADX Family
        all_results['adx'] = self.strategy_optimizer.optimize_adx(df, asset)
        all_results['di_plus'] = self.strategy_optimizer.optimize_di_plus(df, asset)
        all_results['di_minus'] = self.strategy_optimizer.optimize_di_minus(df, asset)
        all_results['adx_cross'] = self.strategy_optimizer.optimize_adx_cross(df, asset)
        
        # Ichimoku
        all_results['ichimoku'] = self.strategy_optimizer.optimize_ichimoku(df, asset)
        all_results['ichimoku_tenkan'] = self.strategy_optimizer.optimize_ichimoku_tenkan(df, asset)
        all_results['ichimoku_kijun'] = self.strategy_optimizer.optimize_ichimoku_kijun(df, asset)
        all_results['ichimoku_cross'] = self.strategy_optimizer.optimize_ichimoku_cross(df, asset)
        
        # Parabolic SAR
        all_results['psar'] = self.strategy_optimizer.optimize_psar(df, asset)
        
        # ===== 3. VOLATILITY INDICATORS =====
        print("\n📉 OPTIMIZING VOLATILITY INDICATORS...")
        
        # Bollinger Bands
        all_results['bollinger'] = self.strategy_optimizer.optimize_bollinger(df, asset)
        all_results['bollinger_breakout'] = self.strategy_optimizer.optimize_bollinger_breakout(df, asset)
        all_results['bollinger_squeeze'] = self.strategy_optimizer.optimize_bollinger_squeeze(df, asset)
        all_results['bollinger_width'] = self.strategy_optimizer.optimize_bollinger_width(df, asset)
        
        # Keltner Channels
        all_results['keltner'] = self.strategy_optimizer.optimize_keltner(df, asset)
        all_results['keltner_breakout'] = self.strategy_optimizer.optimize_keltner_breakout(df, asset)
        
        # ATR Family
        all_results['atr'] = self.strategy_optimizer.optimize_atr(df, asset)
        all_results['atr_trailing'] = self.strategy_optimizer.optimize_atr_trailing(df, asset)
        all_results['atr_bands'] = self.strategy_optimizer.optimize_atr_bands(df, asset)
        
        # Donchian Channels
        all_results['donchian'] = self.strategy_optimizer.optimize_donchian(df, asset)
        all_results['donchian_breakout'] = self.strategy_optimizer.optimize_donchian_breakout(df, asset)
        
        # Volatility-based
        all_results['volatility_ratio'] = self.strategy_optimizer.optimize_volatility_ratio(df, asset)
        all_results['chaikin_volatility'] = self.strategy_optimizer.optimize_chaikin_volatility(df, asset)
        
        # ===== 4. VOLUME INDICATORS =====
        print("\n📊 OPTIMIZING VOLUME INDICATORS...")
        
        all_results['obv'] = self.strategy_optimizer.optimize_obv(df, asset)  # On-Balance Volume
        all_results['obv_divergence'] = self.strategy_optimizer.optimize_obv_divergence(df, asset)
        all_results['volume_profile'] = self.strategy_optimizer.optimize_volume_profile(df, asset)
        all_results['volume_oscillator'] = self.strategy_optimizer.optimize_volume_oscillator(df, asset)
        all_results['vwap_volume'] = self.strategy_optimizer.optimize_vwap_volume(df, asset)
        all_results['cmf'] = self.strategy_optimizer.optimize_cmf(df, asset)  # Chaikin Money Flow
        all_results['eom'] = self.strategy_optimizer.optimize_eom(df, asset)  # Ease of Movement
        all_results['vpt'] = self.strategy_optimizer.optimize_vpt(df, asset)  # Volume Price Trend
        
        # ===== 5. OSCILLATORS =====
        print("\n📊 OPTIMIZING OSCILLATORS...")
        
        all_results['awesome'] = self.strategy_optimizer.optimize_awesome(df, asset)  # Awesome Oscillator
        all_results['acceleration'] = self.strategy_optimizer.optimize_acceleration(df, asset)
        all_results['rvgi'] = self.strategy_optimizer.optimize_rvgi(df, asset)  # Relative Vigor Index
        all_results['trix'] = self.strategy_optimizer.optimize_trix(df, asset)  # Triple Exponential Average
        all_results['cmo'] = self.strategy_optimizer.optimize_cmo(df, asset)  # Chande Momentum Oscillator
        
        # ===== 6. PATTERN RECOGNITION =====
        print("\n📊 OPTIMIZING PATTERN RECOGNITION...")
        
        all_results['doji'] = self.strategy_optimizer.optimize_doji(df, asset)
        all_results['hammer'] = self.strategy_optimizer.optimize_hammer(df, asset)
        all_results['engulfing'] = self.strategy_optimizer.optimize_engulfing(df, asset)
        all_results['morning_star'] = self.strategy_optimizer.optimize_morning_star(df, asset)
        all_results['evening_star'] = self.strategy_optimizer.optimize_evening_star(df, asset)
        all_results['three_white_soldiers'] = self.strategy_optimizer.optimize_three_white(df, asset)
        all_results['three_black_crows'] = self.strategy_optimizer.optimize_three_black(df, asset)
        
        # ===== 7. SUPPORT/RESISTANCE =====
        print("\n📊 OPTIMIZING SUPPORT/RESISTANCE...")
        
        all_results['pivot_points'] = self.strategy_optimizer.optimize_pivot_points(df, asset)
        all_results['fibonacci'] = self.strategy_optimizer.optimize_fibonacci(df, asset)
        all_results['supply_demand'] = self.strategy_optimizer.optimize_supply_demand(df, asset)
        
        # ===== 8. COMBINATION STRATEGIES =====
        print("\n🤝 OPTIMIZING COMBINATION STRATEGIES...")
        
        all_results['rsi_macd'] = self.strategy_optimizer.optimize_rsi_macd_combination(df, asset)
        all_results['bollinger_rsi'] = self.strategy_optimizer.optimize_bollinger_rsi(df, asset)
        all_results['adx_di'] = self.strategy_optimizer.optimize_adx_di(df, asset)
        all_results['volume_breakout'] = self.strategy_optimizer.optimize_volume_breakout(df, asset)
        all_results['momentum_reversal'] = self.strategy_optimizer.optimize_momentum_reversal(df, asset)
        
        # ===== 9. ADVANCED ML STRATEGIES =====
        print("\n🤖 OPTIMIZING ML STRATEGIES...")
        
        all_results['ml_ensemble'] = self.strategy_optimizer.optimize_ml_ensemble(df, asset)
        all_results['xgboost'] = self.strategy_optimizer.optimize_xgboost(df, asset)
        all_results['random_forest'] = self.strategy_optimizer.optimize_random_forest(df, asset)
        
        # ===== COMPILE RESULTS =====
        print(f"\n{'='*70}")
        print(f"📊 OPTIMIZATION COMPLETE FOR {asset}")
        print(f"{'='*70}")
        
        # Create comparison of all strategies
        comparison = self.strategy_optimizer.compare_all_strategies(asset)
        
        # Find top 10 best performing strategies
        if not comparison.empty:
            print("\n🏆 TOP 10 BEST STRATEGIES FOR", asset)
            print("-" * 70)
            print(comparison.head(10).to_string())
            
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
        
        print("\n⚖️ Updating strategy weights based on optimization...")
        
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
                print(f"  • {strategy}: {old_weight} → {self.voting_engine.strategy_weights[strategy]}")

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
        print("\n" + "="*80)
        print("🚀 BATCH OPTIMIZING ALL 50+ STRATEGIES FOR ALL ASSETS")
        print("="*80)
        print("⚠️  This will take a LONG time (minutes to hours depending on number of assets)")
        print("💡 Consider running this overnight or on a weekend")
        print("="*80 + "\n")
        
        # Get list of assets to optimize
        if assets is None:
            # Get all tradable assets
            asset_list = self.get_asset_list()
            assets_to_optimize = [asset[0] for asset in asset_list]  # Extract asset names
        else:
            assets_to_optimize = assets
        
        print(f"📊 Will optimize {len(assets_to_optimize)} assets")
        print(f"📈 Each asset: 50+ strategies × multiple parameters = thousands of combinations")
        print(f"⏱️  Estimated time: {len(assets_to_optimize) * 5} minutes\n")
        
        all_results = {}
        
        for i, asset_name in enumerate(assets_to_optimize, 1):
            print(f"\n{'='*60}")
            print(f"[{i}/{len(assets_to_optimize)}] OPTIMIZING {asset_name}")
            print(f"{'='*60}")
            
            try:
                # Fetch historical data
                print(f"\n📥 Fetching {lookback_days} days of data for {asset_name}...")
                df = self.fetch_historical_data(asset_name, lookback_days)
                
                if df.empty or len(df) < 100:
                    print(f"⚠️  Insufficient data for {asset_name}, skipping...")
                    continue
                
                # Add all technical indicators
                print(f"📊 Adding 50+ technical indicators...")
                df = self.add_technical_indicators(df)
                print(f"   ✓ Data shape: {df.shape}")
                
                # Initialize results for this asset
                asset_results = {}
                
                # ===== 1. MOMENTUM INDICATORS =====
                print("\n📈 Optimizing Momentum Indicators...")
                
                # RSI Family
                print("   • RSI...")
                asset_results['rsi'] = self.strategy_optimizer.optimize_rsi(df, asset_name)
                
                print("   • RSI Divergence...")
                asset_results['rsi_divergence'] = self.strategy_optimizer.optimize_rsi_divergence(df, asset_name)
                
                print("   • Stochastic RSI...")
                asset_results['stoch_rsi'] = self.strategy_optimizer.optimize_stoch_rsi(df, asset_name)
                
                # Stochastic Family
                print("   • Stochastic...")
                asset_results['stochastic'] = self.strategy_optimizer.optimize_stochastic(df, asset_name)
                
                print("   • Stochastic Fast...")
                asset_results['stochastic_fast'] = self.strategy_optimizer.optimize_stochastic_fast(df, asset_name)
                
                print("   • Stochastic Full...")
                asset_results['stochastic_full'] = self.strategy_optimizer.optimize_stochastic_full(df, asset_name)
                
                # MACD Family
                print("   • MACD...")
                asset_results['macd'] = self.strategy_optimizer.optimize_macd(df, asset_name)
                
                print("   • MACD Histogram...")
                asset_results['macd_histogram'] = self.strategy_optimizer.optimize_macd_histogram(df, asset_name)
                
                print("   • MACD Divergence...")
                asset_results['macd_divergence'] = self.strategy_optimizer.optimize_macd_divergence(df, asset_name)
                
                # Other Momentum
                print("   • CCI...")
                asset_results['cci'] = self.strategy_optimizer.optimize_cci(df, asset_name)
                
                print("   • Williams %R...")
                asset_results['williams_r'] = self.strategy_optimizer.optimize_williams_r(df, asset_name)
                
                print("   • MFI...")
                asset_results['mfi'] = self.strategy_optimizer.optimize_mfi(df, asset_name)
                
                print("   • Ultimate Oscillator...")
                asset_results['uo'] = self.strategy_optimizer.optimize_uo(df, asset_name)
                
                print("   • APO...")
                asset_results['apo'] = self.strategy_optimizer.optimize_apo(df, asset_name)
                
                print("   • PPO...")
                asset_results['ppo'] = self.strategy_optimizer.optimize_ppo(df, asset_name)
                
                # ===== 2. TREND INDICATORS =====
                print("\n📊 Optimizing Trend Indicators...")
                
                # Moving Averages
                print("   • SMA Cross...")
                asset_results['sma_cross'] = self.strategy_optimizer.optimize_sma_cross(df, asset_name)
                
                print("   • EMA Cross...")
                asset_results['ema_cross'] = self.strategy_optimizer.optimize_ema_cross(df, asset_name)
                
                print("   • WMA Cross...")
                asset_results['wma_cross'] = self.strategy_optimizer.optimize_wma_cross(df, asset_name)
                
                print("   • HMA Cross...")
                asset_results['hma_cross'] = self.strategy_optimizer.optimize_hma_cross(df, asset_name)
                
                print("   • VWAP...")
                asset_results['vwap'] = self.strategy_optimizer.optimize_vwap(df, asset_name)
                
                # ADX Family
                print("   • ADX...")
                asset_results['adx'] = self.strategy_optimizer.optimize_adx(df, asset_name)
                
                print("   • +DI...")
                asset_results['di_plus'] = self.strategy_optimizer.optimize_di_plus(df, asset_name)
                
                print("   • -DI...")
                asset_results['di_minus'] = self.strategy_optimizer.optimize_di_minus(df, asset_name)
                
                print("   • ADX Cross...")
                asset_results['adx_cross'] = self.strategy_optimizer.optimize_adx_cross(df, asset_name)
                
                # Ichimoku
                print("   • Ichimoku...")
                asset_results['ichimoku'] = self.strategy_optimizer.optimize_ichimoku(df, asset_name)
                
                print("   • Ichimoku Tenkan...")
                asset_results['ichimoku_tenkan'] = self.strategy_optimizer.optimize_ichimoku_tenkan(df, asset_name)
                
                print("   • Ichimoku Kijun...")
                asset_results['ichimoku_kijun'] = self.strategy_optimizer.optimize_ichimoku_kijun(df, asset_name)
                
                print("   • Ichimoku Cross...")
                asset_results['ichimoku_cross'] = self.strategy_optimizer.optimize_ichimoku_cross(df, asset_name)
                
                # Parabolic SAR
                print("   • Parabolic SAR...")
                asset_results['psar'] = self.strategy_optimizer.optimize_psar(df, asset_name)
                
                # ===== 3. VOLATILITY INDICATORS =====
                print("\n📉 Optimizing Volatility Indicators...")
                
                # Bollinger Bands
                print("   • Bollinger Bands...")
                asset_results['bollinger'] = self.strategy_optimizer.optimize_bollinger(df, asset_name)
                
                print("   • Bollinger Breakout...")
                asset_results['bollinger_breakout'] = self.strategy_optimizer.optimize_bollinger_breakout(df, asset_name)
                
                print("   • Bollinger Squeeze...")
                asset_results['bollinger_squeeze'] = self.strategy_optimizer.optimize_bollinger_squeeze(df, asset_name)
                
                print("   • Bollinger Width...")
                asset_results['bollinger_width'] = self.strategy_optimizer.optimize_bollinger_width(df, asset_name)
                
                # Keltner Channels
                print("   • Keltner Channels...")
                asset_results['keltner'] = self.strategy_optimizer.optimize_keltner(df, asset_name)
                
                print("   • Keltner Breakout...")
                asset_results['keltner_breakout'] = self.strategy_optimizer.optimize_keltner_breakout(df, asset_name)
                
                # ATR Family
                print("   • ATR...")
                asset_results['atr'] = self.strategy_optimizer.optimize_atr(df, asset_name)
                
                print("   • ATR Trailing...")
                asset_results['atr_trailing'] = self.strategy_optimizer.optimize_atr_trailing(df, asset_name)
                
                print("   • ATR Bands...")
                asset_results['atr_bands'] = self.strategy_optimizer.optimize_atr_bands(df, asset_name)
                
                # Donchian Channels
                print("   • Donchian Channels...")
                asset_results['donchian'] = self.strategy_optimizer.optimize_donchian(df, asset_name)
                
                print("   • Donchian Breakout...")
                asset_results['donchian_breakout'] = self.strategy_optimizer.optimize_donchian_breakout(df, asset_name)
                
                # Volatility-based
                print("   • Volatility Ratio...")
                asset_results['volatility_ratio'] = self.strategy_optimizer.optimize_volatility_ratio(df, asset_name)
                
                print("   • Chaikin Volatility...")
                asset_results['chaikin_volatility'] = self.strategy_optimizer.optimize_chaikin_volatility(df, asset_name)
                
                # ===== 4. VOLUME INDICATORS =====
                print("\n📊 Optimizing Volume Indicators...")
                
                print("   • OBV...")
                asset_results['obv'] = self.strategy_optimizer.optimize_obv(df, asset_name)
                
                print("   • OBV Divergence...")
                asset_results['obv_divergence'] = self.strategy_optimizer.optimize_obv_divergence(df, asset_name)
                
                print("   • Volume Profile...")
                asset_results['volume_profile'] = self.strategy_optimizer.optimize_volume_profile(df, asset_name)
                
                print("   • Volume Oscillator...")
                asset_results['volume_oscillator'] = self.strategy_optimizer.optimize_volume_oscillator(df, asset_name)
                
                print("   • VWAP Volume...")
                asset_results['vwap_volume'] = self.strategy_optimizer.optimize_vwap_volume(df, asset_name)
                
                print("   • CMF...")
                asset_results['cmf'] = self.strategy_optimizer.optimize_cmf(df, asset_name)
                
                print("   • EOM...")
                asset_results['eom'] = self.strategy_optimizer.optimize_eom(df, asset_name)
                
                print("   • VPT...")
                asset_results['vpt'] = self.strategy_optimizer.optimize_vpt(df, asset_name)
                
                # ===== 5. OSCILLATORS =====
                print("\n📊 Optimizing Oscillators...")
                
                print("   • Awesome Oscillator...")
                asset_results['awesome'] = self.strategy_optimizer.optimize_awesome(df, asset_name)
                
                print("   • Acceleration Oscillator...")
                asset_results['acceleration'] = self.strategy_optimizer.optimize_acceleration(df, asset_name)
                
                print("   • RVGI...")
                asset_results['rvgi'] = self.strategy_optimizer.optimize_rvgi(df, asset_name)
                
                print("   • TRIX...")
                asset_results['trix'] = self.strategy_optimizer.optimize_trix(df, asset_name)
                
                print("   • CMO...")
                asset_results['cmo'] = self.strategy_optimizer.optimize_cmo(df, asset_name)
                
                # ===== 6. PATTERN RECOGNITION =====
                print("\n📊 Optimizing Pattern Recognition...")
                
                print("   • Doji...")
                asset_results['doji'] = self.strategy_optimizer.optimize_doji(df, asset_name)
                
                print("   • Hammer...")
                asset_results['hammer'] = self.strategy_optimizer.optimize_hammer(df, asset_name)
                
                print("   • Engulfing...")
                asset_results['engulfing'] = self.strategy_optimizer.optimize_engulfing(df, asset_name)
                
                print("   • Morning Star...")
                asset_results['morning_star'] = self.strategy_optimizer.optimize_morning_star(df, asset_name)
                
                print("   • Evening Star...")
                asset_results['evening_star'] = self.strategy_optimizer.optimize_evening_star(df, asset_name)
                
                print("   • Three White Soldiers...")
                asset_results['three_white'] = self.strategy_optimizer.optimize_three_white(df, asset_name)
                
                print("   • Three Black Crows...")
                asset_results['three_black'] = self.strategy_optimizer.optimize_three_black(df, asset_name)
                
                # ===== 7. SUPPORT/RESISTANCE =====
                print("\n📊 Optimizing Support/Resistance...")
                
                print("   • Pivot Points...")
                asset_results['pivot_points'] = self.strategy_optimizer.optimize_pivot_points(df, asset_name)
                
                print("   • Fibonacci...")
                asset_results['fibonacci'] = self.strategy_optimizer.optimize_fibonacci(df, asset_name)
                
                print("   • Supply/Demand...")
                asset_results['supply_demand'] = self.strategy_optimizer.optimize_supply_demand(df, asset_name)
                
                # ===== 8. COMBINATION STRATEGIES =====
                print("\n🤝 Optimizing Combination Strategies...")
                
                print("   • RSI + MACD...")
                asset_results['rsi_macd'] = self.strategy_optimizer.optimize_rsi_macd_combination(df, asset_name)
                
                print("   • Bollinger + RSI...")
                asset_results['bollinger_rsi'] = self.strategy_optimizer.optimize_bollinger_rsi(df, asset_name)
                
                print("   • ADX + DI...")
                asset_results['adx_di'] = self.strategy_optimizer.optimize_adx_di(df, asset_name)
                
                print("   • Volume Breakout...")
                asset_results['volume_breakout'] = self.strategy_optimizer.optimize_volume_breakout(df, asset_name)
                
                print("   • Momentum Reversal...")
                asset_results['momentum_reversal'] = self.strategy_optimizer.optimize_momentum_reversal(df, asset_name)
                
                # Store results for this asset
                all_results[asset_name] = asset_results
                
                # Show summary for this asset
                print(f"\n✅ COMPLETED {asset_name}")
                print(f"   • Successfully optimized {len(asset_results)} strategies")
                
                # Compare strategies for this asset
                comparison = self.strategy_optimizer.compare_strategies(asset_name)
                if not comparison.empty:
                    print(f"\n   🏆 TOP 3 STRATEGIES FOR {asset_name}:")
                    for idx, row in comparison.head(3).iterrows():
                        print(f"      {row['strategy']}: Sharpe {row['best_sharpe']:.2f}")
                
            except Exception as e:
                print(f"\n❌ Error optimizing {asset_name}: {e}")
                import traceback
                traceback.print_exc()
                continue
        
        # Create master summary
        print("\n" + "="*80)
        print("📊 MASTER OPTIMIZATION SUMMARY")
        print("="*80)
        print(f"✅ Successfully optimized {len(all_results)} out of {len(assets_to_optimize)} assets")
        
        # Save all results to file
        self._save_optimization_results(all_results)
        
        return all_results
    def create_master_optimization_report(self, all_results: Dict):
        """
        Create master report showing best strategies across all assets
        """
        print(f"\n{'='*70}")
        print(f"📊 MASTER OPTIMIZATION REPORT - ALL ASSETS")
        print(f"{'='*70}")
        
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
            print("⚠️ No strategy performance data available")
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
        
        print("\n🏆 TOP 10 STRATEGIES ACROSS ALL ASSETS:")
        print("-" * 70)
        for i, (strategy, data) in enumerate(sorted_strategies[:10], 1):
            print(f"{i}. {strategy}:")
            print(f"   • Avg Sharpe: {data['avg_sharpe']:.2f}")
            print(f"   • Works on: {len(data['assets'])} assets")
            print(f"   • Examples: {', '.join(data['assets'][:3])}")
        
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
            
            print(f"\n💾 Master report saved to {filename}")
            
        except Exception as e:
            print(f"⚠️ Could not save master report: {e}")

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
            
            print(f"\n💾 All optimization results saved to: {filename}")
            
        except Exception as e:
            print(f"⚠️ Could not save optimization results: {e}")

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
        print("\n⚙️ Applying optimized parameters to strategies...")
        
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
                    print(f"   ✓ {asset} - {strategy}: {params}")
        
        print("✅ Optimized parameters applied")


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
    
    def train_ml_models(self, assets: List[str]):
        """Train ML models on multiple assets"""
        print("\n" + "="*60)
        print(" TRAINING ML ENSEMBLE MODELS")
        print("="*60 + "\n")

        for asset in assets:
            print(f"\n Training on {asset}...")
            
            # ===== TRY MULTIPLE PERIODS, TAKE THE BEST =====
            best_df = None
            best_rows = 0
            
            for days in [730, 365, 180, 90]:
                df = self.fetch_historical_data(asset, days)
                if not df.empty and len(df) > best_rows:
                    best_rows = len(df)
                    best_df = df
                    print(f"   Found {len(df)} rows with {days} days")
            
            if best_df is None:
                print(f"   No data for {asset}")
                continue
            
            df = best_df
            print(f"   Using {len(df)} rows for training")
            
            # Add indicators
            df = self.add_technical_indicators(df)
            
            try:
                # Train model
                self.predictor.train(df, target_periods=5)
                
                # Save model
                import pickle
                safe_asset = asset.replace('/', '_').replace('\\', '_').replace(':', '_')
                model_path = f"ml_models/{safe_asset}_model.pkl"
                
                os.makedirs("ml_models", exist_ok=True)
                
                with open(model_path, 'wb') as f:
                    pickle.dump(self.predictor, f)
                
                print(f" Model saved to {model_path}")

                 # ===== REGISTER MODEL IN REGISTRY =====
                if hasattr(self, 'model_registry') and self.model_registry:
                    metadata = {
                        'data_points': len(df),
                        'features': len(self.predictor.feature_names) if hasattr(self.predictor, 'feature_names') else 0,
                        'model_path': model_path
                    }
                    self.register_ml_model(asset, "ensemble", metadata)
                # ======================================
                
                # Get feature importance
                importance = self.predictor.get_feature_importance(10)
                if not importance.empty:
                    print("\n Top Features:")
                    print(importance)
                
            except Exception as e:
                print(f" Training error for {asset}: {e}")
    
    # ============= BROKER INTEGRATION (READY FOR ALPACA) =============
    
    def ultimate_indicator_strategy(self, df: pd.DataFrame) -> List[Dict]:
        """
        ULTIMATE STRATEGY - Uses ALL 50+ indicators
        Each indicator votes, weighted by reliability
        """
        print("   Running ULTIMATE indicator strategy (50+ indicators)...")

        if len(df) < 50:
            return []

        signals = []
        latest = df.iloc[-1]

        # ===== TREND INDICATORS (Weight: 30%) =====
        trend_score = 0
        trend_weight = 0

        # 1. SMA Trend (20 vs 50)
        if 'sma_20' in df.columns and 'sma_50' in df.columns:
            if latest['sma_20'] > latest['sma_50']:
                trend_score += 1
            elif latest['sma_20'] < latest['sma_50']:
                trend_score -= 1
            trend_weight += 1

        # 2. EMA Trend (12 vs 26)
        if 'ema_12' in df.columns and 'ema_26' in df.columns:
            if latest['ema_12'] > latest['ema_26']:
                trend_score += 1
            elif latest['ema_12'] < latest['ema_26']:
                trend_score -= 1
            trend_weight += 1

        # 3. ADX (Trend Strength)
        if 'adx' in df.columns:
            if latest['adx'] > 25:  # Strong trend
                if latest['close'] > df['close'].rolling(20).mean().iloc[-1]:
                    trend_score += 1  # Strong uptrend
                else:
                    trend_score -= 1  # Strong downtrend
            trend_weight += 1

        # 4. Ichimoku Cloud
        if all(x in df.columns for x in ['ichimoku_a', 'ichimoku_b']):
            if latest['close'] > max(latest['ichimoku_a'], latest['ichimoku_b']):
                trend_score += 1
            elif latest['close'] < min(latest['ichimoku_a'], latest['ichimoku_b']):
                trend_score -= 1
            trend_weight += 1

        # 5. Parabolic SAR
        if 'psar' in df.columns:
            if latest['close'] > latest['psar']:
                trend_score += 1
            else:
                trend_score -= 1
            trend_weight += 1

        # ===== MOMENTUM INDICATORS (Weight: 25%) =====
        momentum_score = 0
        momentum_weight = 0

        # 6. RSI
        if 'rsi' in df.columns:
            rsi = latest['rsi']
            if rsi < 30:
                momentum_score += 2  # Strong buy signal
            elif rsi < 40:
                momentum_score += 1  # Weak buy signal
            elif rsi > 70:
                momentum_score -= 2  # Strong sell signal
            elif rsi > 60:
                momentum_score -= 1  # Weak sell signal
            momentum_weight += 2

        # 7. Stochastic
        if all(x in df.columns for x in ['stoch_k', 'stoch_d']):
            if latest['stoch_k'] < 20 and latest['stoch_d'] < 20:
                momentum_score += 2
            elif latest['stoch_k'] > 80 and latest['stoch_d'] > 80:
                momentum_score -= 2
            momentum_weight += 2

        # 8. Williams %R
        if 'williams_r' in df.columns:
            if latest['williams_r'] < -80:
                momentum_score += 1
            elif latest['williams_r'] > -20:
                momentum_score -= 1
            momentum_weight += 1

        # 9. CCI (Commodity Channel Index)
        if 'cci' in df.columns:
            if latest['cci'] < -100:
                momentum_score += 1
            elif latest['cci'] > 100:
                momentum_score -= 1
            momentum_weight += 1

        # 10. MFI (Money Flow Index)
        if 'mfi' in df.columns:
            if latest['mfi'] < 20:
                momentum_score += 1
            elif latest['mfi'] > 80:
                momentum_score -= 1
            momentum_weight += 1

        # ===== VOLATILITY INDICATORS (Weight: 20%) =====
        volatility_score = 0
        volatility_weight = 0

        # 11. Bollinger Bands Position
        if all(x in df.columns for x in ['bb_upper', 'bb_lower', 'bb_middle']):
            bb_range = latest['bb_upper'] - latest['bb_lower']
            bb_position = (latest['close'] - latest['bb_lower']) / bb_range
        
            if bb_position < 0.2:  # Near lower band - potential bounce
                volatility_score += 1
            elif bb_position > 0.8:  # Near upper band - potential reversal
                volatility_score -= 1
            volatility_weight += 1

        # 12. Bollinger Band Width (squeeze)
        if 'bb_width' in df.columns:
            if latest['bb_width'] < df['bb_width'].rolling(20).mean().iloc[-1] * 0.7:
                # Squeeze - potential breakout
                volatility_score += 1 if trend_score > 0 else -1
            volatility_weight += 1

        # 13. Keltner Channels
        if all(x in df.columns for x in ['kc_upper', 'kc_lower']):
            if latest['close'] > latest['kc_upper']:
                volatility_score += 1
            elif latest['close'] < latest['kc_lower']:
                volatility_score -= 1
            volatility_weight += 1

        # 14. ATR (Average True Range) - for volatility regime
        if 'atr' in df.columns:
            atr_ratio = latest['atr'] / df['close'].iloc[-1]
            if atr_ratio > 0.03:  # High volatility
                # Be more cautious
                pass
            volatility_weight += 0.5

        # 15. Donchian Channels
        if all(x in df.columns for x in ['dc_upper', 'dc_lower']):
            if latest['close'] > latest['dc_upper']:
                volatility_score += 1
            elif latest['close'] < latest['dc_lower']:
                volatility_score -= 1
            volatility_weight += 1

        # ===== VOLUME INDICATORS (Weight: 15%) =====
        volume_score = 0
        volume_weight = 0

        # 16. Volume Ratio
        if 'volume_ratio' in df.columns:
            if latest['volume_ratio'] > 1.5:  # High volume
                volume_score += 1 if trend_score > 0 else -1
            volume_weight += 1

        # 17. OBV (On-Balance Volume) Trend
        if 'obv' in df.columns and len(df) > 20:
            obv_trend = df['obv'].iloc[-1] - df['obv'].iloc[-20]
            price_trend = df['close'].iloc[-1] - df['close'].iloc[-20]
        
            if obv_trend > 0 and price_trend > 0:
                volume_score += 1  # Bullish confirmation
            elif obv_trend < 0 and price_trend < 0:
                volume_score -= 1  # Bearish confirmation
            volume_weight += 1

        # 18. Volume Price Trend
        if 'vpt' in df.columns:
            if latest['vpt'] > df['vpt'].rolling(20).mean().iloc[-1]:
                volume_score += 1
            else:
                volume_score -= 1
            volume_weight += 1

        # 19. Chaikin Money Flow
        if 'cmf' in df.columns:
            if latest['cmf'] > 0.1:
                volume_score += 1
            elif latest['cmf'] < -0.1:
                volume_score -= 1
            volume_weight += 1

        # 20. Ease of Movement
        if 'eom' in df.columns:
            if latest['eom'] > 0:
                volume_score += 1
            else:
                volume_score -= 1
            volume_weight += 1

        # ===== OSCILLATORS (Weight: 10%) =====
        oscillator_score = 0
        oscillator_weight = 0

        # 21. MACD
        if all(x in df.columns for x in ['macd', 'macd_signal']):
            if latest['macd'] > latest['macd_signal']:
                oscillator_score += 1
                # MACD histogram momentum
                if 'macd_hist' in df.columns and len(df) > 1:
                    if latest['macd_hist'] > df['macd_hist'].iloc[-2]:
                        oscillator_score += 1
            else:
                oscillator_score -= 1
                if 'macd_hist' in df.columns and len(df) > 1:
                    if latest['macd_hist'] < df['macd_hist'].iloc[-2]:
                        oscillator_score -= 1
            oscillator_weight += 2

        # 22. Awesome Oscillator
        if 'ao' in df.columns:
            if latest['ao'] > 0:
                oscillator_score += 1
            else:
                oscillator_score -= 1
            oscillator_weight += 1

        # 23. Ultimate Oscillator
        if 'uo' in df.columns:
            if latest['uo'] < 30:
                oscillator_score += 1
            elif latest['uo'] > 70:
                oscillator_score -= 1
            oscillator_weight += 1

        # 24. Relative Vigor Index
        if 'rvgi' in df.columns:
            if latest['rvgi'] > 0:
                oscillator_score += 1
            else:
                oscillator_score -= 1
            oscillator_weight += 1

        # ===== CALCULATE FINAL SCORES =====

        # Normalize scores
        trend_final = (trend_score / max(trend_weight, 1)) * 30
        momentum_final = (momentum_score / max(momentum_weight, 1)) * 25
        volatility_final = (volatility_score / max(volatility_weight, 1)) * 20
        volume_final = (volume_score / max(volume_weight, 1)) * 15
        oscillator_final = (oscillator_score / max(oscillator_weight, 1)) * 10

        # Total score (-100 to +100)
        total_score = trend_final + momentum_final + volatility_final + volume_final + oscillator_final

        # Confidence (0 to 100%)
        confidence = abs(total_score) / 100
        confidence = min(max(confidence, 0), 0.95)  # Cap at 95%

        # Determine signal
        if total_score > 5:
            signal = 'BUY'
            reason = f"Strong bullish signal ({total_score:.0f} pts)"
        elif total_score < -5:
            signal = 'SELL'
            reason = f"Strong bearish signal ({total_score:.0f} pts)"
        else:
            signal = 'HOLD'
            reason = f"Neutral signal ({total_score:.0f} pts)"

        # Current price
        current_price = latest['close']

        # ===== UPDATED: DAY TRADING STOP LOSSES =====
        # Using tight 0.3% stops for day trading instead of ATR-based wide stops
        
        if signal == 'BUY':
            stop_loss = current_price * 0.997      # 0.3% stop
            tp1 = current_price * 1.003            # 0.3% profit
            tp2 = current_price * 1.006            # 0.6% profit
            tp3 = current_price * 1.01             # 1% profit
        elif signal == 'SELL':
            stop_loss = current_price * 1.003      # 0.3% stop
            tp1 = current_price * 0.997            # 0.3% profit
            tp2 = current_price * 0.994            # 0.6% profit
            tp3 = current_price * 0.99             # 1% profit
        else:
            # For HOLD signals (shouldn't be used, but just in case)
            stop_loss = current_price * 0.997
            tp1 = current_price * 1.003
            tp2 = current_price * 1.006
            tp3 = current_price * 1.01

        # Create signal
        if signal != 'HOLD':
            signals.append({
                'date': df.index[-1],
                'signal': signal,
                'confidence': confidence,
                'entry': current_price,
                'stop_loss': stop_loss,
                'take_profit': tp1,
                'take_profit_levels': [
                    {'level': 1, 'price': tp1},
                    {'level': 2, 'price': tp2},
                    {'level': 3, 'price': tp3}
                ],
                'strategy': 'ultimate',
                'reason': reason,
                'score_breakdown': {
                    'trend': round(trend_final, 1),
                    'momentum': round(momentum_final, 1),
                    'volatility': round(volatility_final, 1),
                    'volume': round(volume_final, 1),
                    'oscillator': round(oscillator_final, 1),
                    'total': round(total_score, 1)
                }
            })

        return signals
    
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
        UPDATED: Tighter stops for all assets
        Best for: 1m-5m timeframes
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
                return 0.003  # 0.3% for crypto
            elif asset_name and '/' in asset_name:  # Forex
                return 0.002  # 0.2% for forex
            else:  # Stocks
                return 0.005  # 0.5% for stocks
        
        stop_pct = get_stop_pct(self.current_asset)
        
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
        
        # TIGHTENED entry conditions
        buy_score = 0
        sell_score = 0
        
        # RSI conditions
        if rsi < 40:  # Was 60
            buy_score += 1
        if rsi > 60:  # Was 40
            sell_score += 1
        
        # Moving average conditions
        if 'ema_5' in df.columns and 'ema_10' in df.columns:
            if latest['ema_5'] > latest['ema_10']:
                buy_score += 1
            else:
                sell_score += 1
        
        # Price position
        if 'bb_middle' in df.columns:
            if latest['close'] < latest['bb_middle']:
                buy_score += 1
            else:
                sell_score += 1
        
        # Volume confirmation
        if volume_spike:
            buy_score += 0.5
            sell_score += 0.5
        
        # Generate signal if score is high enough
        if buy_score >= 2.5:
            confidence = min(0.5 + buy_score * 0.1, 0.8)
            
            # TIGHTER stops and targets
            if latest['close'] < 10:  # Cheap assets like crypto
                stop_loss = latest['close'] * (1 - stop_pct)
                tp1 = latest['close'] * (1 + stop_pct * 1.5)
                tp2 = latest['close'] * (1 + stop_pct * 2.5)
                tp3 = latest['close'] * (1 + stop_pct * 4)
            else:  # Normal assets
                stop_loss = latest['close'] * (1 - stop_pct)
                tp1 = latest['close'] * (1 + stop_pct * 1.5)
                tp2 = latest['close'] * (1 + stop_pct * 2.5)
                tp3 = latest['close'] * (1 + stop_pct * 4)
            
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
        
        elif sell_score >= 2.5:
            confidence = min(0.5 + sell_score * 0.1, 0.8)
            
            stop_loss = latest['close'] * (1 + stop_pct)
            tp1 = latest['close'] * (1 - stop_pct * 1.5)
            tp2 = latest['close'] * (1 - stop_pct * 2.5)
            tp3 = latest['close'] * (1 - stop_pct * 4)
            
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
    
    def get_combined_signal(self, df: pd.DataFrame) -> Dict:
        """
        Get combined signal from all strategies using voting
        """
        # Get signals from all strategies
        signals = self.voting_engine.get_all_signals(df)
        
        if not signals:
            return {'signal': 'HOLD', 'confidence': 0}
        
        # Let them vote
        combined = self.voting_engine.weighted_vote(signals)
        
        if combined:
            print(f"\n🗳️ VOTING RESULTS:")
            print(f"   BUY:  {combined['buy_votes']:.1%}")
            print(f"   SELL: {combined['sell_votes']:.1%}")
            print(f"   Final: {combined['signal']} ({combined['confidence']:.1%})")
            print(f"   Strategies: {', '.join(combined['contributing_strategies'][:5])}")
        
        return combined if combined else {'signal': 'HOLD', 'confidence': 0}
    
    
    def multi_timeframe_strategy(self, df_15m, df_1h, df_4h=None):
        """
        Analyze multiple timeframes for confluence
        15m: Entry timing
        1h: Trend direction
        4h: Overall market structure (optional)
        """
        
        def analyze_timeframe(df, name):
            """Analyze single timeframe"""
            try:
                latest = df.iloc[-1]
                
                # Calculate indicators if needed
                if 'ema_9' not in df.columns:
                    df['ema_9'] = df['close'].ewm(span=9).mean()
                if 'ema_21' not in df.columns:
                    df['ema_21'] = df['close'].ewm(span=21).mean()
                
                # Trend direction
                sma_20 = df['close'].rolling(20).mean().iloc[-1]
                sma_50 = df['close'].rolling(50).mean().iloc[-1]
                
                trend = 'UP' if sma_20 > sma_50 else 'DOWN'
                
                # Momentum
                if 'rsi' in df.columns:
                    rsi = latest['rsi']
                else:
                    # Calculate simple RSI
                    delta = df['close'].diff()
                    gain = delta.where(delta > 0, 0).rolling(14).mean()
                    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
                    rs = gain / loss
                    rsi = 100 - (100 / (1 + rs)).iloc[-1] if not rs.empty else 50
                
                momentum = 'BULLISH' if rsi > 50 else 'BEARISH'
                
                return {
                    'trend': trend,
                    'momentum': momentum,
                    'rsi': rsi,
                    'close': latest['close'],
                    'available': True,
                    'name': name
                }
            except Exception as e:
                print(f"   ⚠️ Error analyzing {name}: {e}")
                return {'available': False, 'name': name}
        
        signals = []
        
        # Analyze available timeframes
        tf_15m = analyze_timeframe(df_15m, '15m')
        tf_1h = analyze_timeframe(df_1h, '1h')
        
        # Check if 4h is available
        tf_4h = None
        if df_4h is not None and not df_4h.empty:
            tf_4h = analyze_timeframe(df_4h, '4h')
        
        # ===== STRATEGY WITH AVAILABLE TIMEFRAMES =====
        
        # Case 1: All three timeframes available and agree
        if tf_4h and tf_4h.get('available', False):
            if tf_15m['trend'] == tf_1h['trend'] == tf_4h['trend']:
                confidence = 0.85
                signal = 'BUY' if tf_15m['trend'] == 'UP' else 'SELL'
                
                # Calculate stop loss and take profit
                entry = tf_15m['close']
                if signal == 'BUY':
                    stop_loss = entry * 0.995
                    tp1 = entry * 1.005
                    tp2 = entry * 1.01
                    tp3 = entry * 1.02
                else:
                    stop_loss = entry * 1.005
                    tp1 = entry * 0.995
                    tp2 = entry * 0.99
                    tp3 = entry * 0.98
                
                signals.append({
                    'date': df_15m.index[-1],
                    'signal': signal,
                    'confidence': confidence,
                    'entry': entry,
                    'stop_loss': stop_loss,
                    'take_profit': tp1,
                    'take_profit_levels': [
                        {'level': 1, 'price': tp1},
                        {'level': 2, 'price': tp2},
                        {'level': 3, 'price': tp3}
                    ],
                    'reason': f"ALL TIMEFRAMES {tf_15m['trend']} - STRONG SIGNAL"
                })
                print(f"   🔥 STRONG SIGNAL: {signal} on {tf_15m['name']}")
        
        # Case 2: Only 15m and 1h available and agree
        elif tf_15m['trend'] == tf_1h['trend']:
            confidence = 0.7
            signal = 'BUY' if tf_15m['trend'] == 'UP' else 'SELL'
            
            # Calculate stop loss and take profit
            entry = tf_15m['close']
            if signal == 'BUY':
                stop_loss = entry * 0.996
                tp1 = entry * 1.004
                tp2 = entry * 1.008
                tp3 = entry * 1.015
            else:
                stop_loss = entry * 1.004
                tp1 = entry * 0.996
                tp2 = entry * 0.992
                tp3 = entry * 0.985
            
            signals.append({
                'date': df_15m.index[-1],
                'signal': signal,
                'confidence': confidence,
                'entry': entry,
                'stop_loss': stop_loss,
                'take_profit': tp1,
                'take_profit_levels': [
                    {'level': 1, 'price': tp1},
                    {'level': 2, 'price': tp2},
                    {'level': 3, 'price': tp3}
                ],
                'reason': f"15m & 1h both {tf_15m['trend']} (4h N/A) - GOOD SIGNAL"
            })
            print(f"   📈 GOOD SIGNAL: {signal} on {tf_15m['name']} (15m+1h agree)")
        
        return signals

    def two_timeframe_strategy(self, df_15m, df_1h):
        return None
    
    def strict_strategy(self, df_15m, df_1h):
        """
        STRICT MODE - Both timeframes MUST agree
        Fewer trades, higher win rate
        """
        def get_trend(df):
            df = df.copy()
            if 'ema_9' not in df.columns:
                df['ema_9'] = df['close'].ewm(span=9, adjust=False).mean()
            if 'ema_21' not in df.columns:
                df['ema_21'] = df['close'].ewm(span=21, adjust=False).mean()
            latest = df.iloc[-1]
            return 'UP' if latest['ema_9'] > latest['ema_21'] else 'DOWN'
        
        trend_15m = get_trend(df_15m)
        trend_1h = get_trend(df_1h)
        
        # ONLY trade when BOTH agree
        if trend_15m == trend_1h:
            confidence = 0.8
            signal = 'BUY' if trend_15m == 'UP' else 'SELL'
            reason = f"STRICT: Both timeframes {trend_15m}"
        else:
            return None
        
        entry = df_15m['close'].iloc[-1]
        if signal == 'BUY':
            stop_loss = entry * 0.995
            tp1 = entry * 1.005
            tp2 = entry * 1.01
            tp3 = entry * 1.02
        else:
            stop_loss = entry * 1.005
            tp1 = entry * 0.995
            tp2 = entry * 0.99
            tp3 = entry * 0.98
        
        return {
            'signal': signal,
            'confidence': confidence,
            'strategy_id': 'STRICT',  # ← ADD THIS
            'strategy_emoji': '🔒', 
            'entry': entry,
            'stop_loss': stop_loss,
            'take_profit': tp1,
            'take_profit_levels': [
                {'level': 1, 'price': tp1},
                {'level': 2, 'price': tp2},
                {'level': 3, 'price': tp3}
            ],
            'reason': reason
        }


    def fast_strategy(self, df_15m, df_1h):
        """
        FAST MODE - More opportunities, more trades
        Takes signals from multiple conditions
        """
        def get_trend(df):
            df = df.copy()
            if 'ema_9' not in df.columns:
                df['ema_9'] = df['close'].ewm(span=9, adjust=False).mean()
            if 'ema_21' not in df.columns:
                df['ema_21'] = df['close'].ewm(span=21, adjust=False).mean()
            latest = df.iloc[-1]
            return 'UP' if latest['ema_9'] > latest['ema_21'] else 'DOWN'
        
        def get_rsi_signal(df):
            if 'rsi' in df.columns:
                rsi = df['rsi'].iloc[-1]
            else:
                delta = df['close'].diff()
                gain = delta.where(delta > 0, 0).rolling(14).mean()
                loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
                rs = gain / loss
                rsi_series = 100 - (100 / (1 + rs))
                rsi = rsi_series.iloc[-1] if not rsi_series.empty else 50
            
            if rsi < 30:
                return 'OVERSOLD'
            elif rsi > 70:
                return 'OVERBOUGHT'
            else:
                return 'NEUTRAL'
        
        trend_15m = get_trend(df_15m)
        trend_1h = get_trend(df_1h)
        rsi = get_rsi_signal(df_15m)
        
        # FAST MODE - Multiple entry conditions
        if trend_15m == trend_1h:
            # Strong signal - both agree
            confidence = 0.8
            signal = 'BUY' if trend_15m == 'UP' else 'SELL'
            reason = f"FAST: Both timeframes {trend_15m}"
            
        elif trend_15m == 'UP' and trend_1h != 'DOWN':
            # 15m bullish, 1h not bearish
            confidence = 0.65
            signal = 'BUY'
            reason = "FAST: 15m bullish, 1h neutral"
            
        elif trend_15m == 'DOWN' and trend_1h != 'UP':
            # 15m bearish, 1h not bullish
            confidence = 0.65
            signal = 'SELL'
            reason = "FAST: 15m bearish, 1h neutral"
            
        elif rsi == 'OVERSOLD' and trend_1h != 'DOWN':
            # Oversold bounce opportunity
            confidence = 0.6
            signal = 'BUY'
            reason = "FAST: Oversold bounce"
            
        elif rsi == 'OVERBOUGHT' and trend_1h != 'UP':
            # Overbought reversal opportunity
            confidence = 0.6
            signal = 'SELL'
            reason = "FAST: Overbought reversal"
            
        else:
            return None
        
        entry = df_15m['close'].iloc[-1]
        if signal == 'BUY':
            stop_loss = entry * 0.995
            tp1 = entry * 1.005
            tp2 = entry * 1.01
            tp3 = entry * 1.02
        else:
            stop_loss = entry * 1.005
            tp1 = entry * 0.995
            tp2 = entry * 0.99
            tp3 = entry * 0.98
        
        return {
            'signal': signal,
            'confidence': confidence,
            'strategy_id': 'FAST',  # ← ADD THIS
            'strategy_emoji': '⚡',   # ← ADD THIS
            'entry': entry,
            'stop_loss': stop_loss,
            'take_profit': tp1,
            'take_profit_levels': [
                {'level': 1, 'price': tp1},
                {'level': 2, 'price': tp2},
                {'level': 3, 'price': tp3}
            ],
            'reason': reason
        }


    def balanced_strategy(self, df_15m, df_1h):
        """
        BALANCED MODE - Middle ground between strict and fast
        """
        def get_trend(df):
            df = df.copy()
            if 'ema_9' not in df.columns:
                df['ema_9'] = df['close'].ewm(span=9, adjust=False).mean()
            if 'ema_21' not in df.columns:
                df['ema_21'] = df['close'].ewm(span=21, adjust=False).mean()
            latest = df.iloc[-1]
            return 'UP' if latest['ema_9'] > latest['ema_21'] else 'DOWN'
        
        def get_rsi_signal(df):
            if 'rsi' in df.columns:
                rsi = df['rsi'].iloc[-1]
            else:
                delta = df['close'].diff()
                gain = delta.where(delta > 0, 0).rolling(14).mean()
                loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
                rs = gain / loss
                rsi_series = 100 - (100 / (1 + rs))
                rsi = rsi_series.iloc[-1] if not rsi_series.empty else 50
            
            if rsi < 30:
                return 'OVERSOLD'
            elif rsi > 70:
                return 'OVERBOUGHT'
            else:
                return 'NEUTRAL'
        
        trend_15m = get_trend(df_15m)
        trend_1h = get_trend(df_1h)
        rsi = get_rsi_signal(df_15m)
        
        # BALANCED MODE - Quality over quantity but still some opportunities
        if trend_15m == trend_1h:
            # Both agree - best signal
            confidence = 0.8
            signal = 'BUY' if trend_15m == 'UP' else 'SELL'
            reason = f"BALANCED: Both timeframes {trend_15m}"
            
        elif trend_15m == 'UP' and trend_1h == 'UP' and rsi != 'OVERBOUGHT':
            # 15m bullish, 1h bullish, not overbought
            confidence = 0.7
            signal = 'BUY'
            reason = "BALANCED: Bullish momentum"
            
        elif trend_15m == 'DOWN' and trend_1h == 'DOWN' and rsi != 'OVERSOLD':
            # 15m bearish, 1h bearish, not oversold
            confidence = 0.7
            signal = 'SELL'
            reason = "BALANCED: Bearish momentum"
            
        else:
            return None
        
        entry = df_15m['close'].iloc[-1]
        if signal == 'BUY':
            stop_loss = entry * 0.995
            tp1 = entry * 1.005
            tp2 = entry * 1.01
            tp3 = entry * 1.02
        else:
            stop_loss = entry * 1.005
            tp1 = entry * 0.995
            tp2 = entry * 0.99
            tp3 = entry * 0.98
        
        return {
            'signal': signal,
            'confidence': confidence,
            'strategy_id': 'BALANCED',  # ← ADD THIS
            'strategy_emoji': '⚖️',
            'entry': entry,
            'stop_loss': stop_loss,
            'take_profit': tp1,
            'take_profit_levels': [
                {'level': 1, 'price': tp1},
                {'level': 2, 'price': tp2},
                {'level': 3, 'price': tp3}
            ],
            'reason': reason
        }
        
    def enhanced_signal_generation(self, asset, category): # pyright: ignore[reportUnknownParameterType]
        """Combine multiple strategies and sentiment"""
        
        # Get sentiment
        sentiment = self.sentiment_analyzer.get_trading_signal(asset)
        
        # Get technical signals (your existing strategies)
        tech_signals = self.generate_technical_signals(asset)
        
        # Combine signals
        combined_confidence = (tech_signals['confidence'] * 0.6 + 
                            sentiment['confidence'] * 0.4)
        
        # Determine final signal
        if combined_confidence > 0.7:
            signal = sentiment['signal'] if sentiment['signal'] != 'HOLD' else tech_signals['signal']
        else:
            signal = 'HOLD'
        
        return {
            'signal': signal,
            'confidence': combined_confidence,
            'sentiment': sentiment,
            'technical': tech_signals
        }
    
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

    def connect_alpaca(self, api_key: str, api_secret: str, paper: bool = True):
        """Connect to Alpaca Brokerage"""
        try:
            from alpaca.trading.client import TradingClient
            from alpaca.trading.requests import MarketOrderRequest
            from alpaca.trading.enums import OrderSide, TimeInForce
            
            self.alpaca = TradingClient(api_key, api_secret, paper=paper)
            self.broker_connected = True
            self.broker_name = "Alpaca"
            self.broker_paper = paper
            
            # Get account info
            account = self.alpaca.get_account()
            print(f"\n Connected to Alpaca ({'PAPER' if paper else 'LIVE'})")
            print(f"   Account: {account.account_number}")
            print(f"   Balance: ${float(account.cash):,.2f}")
            
            return True
            
        except ImportError:
            print(" Alpaca SDK not installed. Run: pip install alpaca-py")
            return False
        except Exception as e:
            print(f" Alpaca connection error: {e}")
            return False
    
    def connect_interactive_brokers(self):
        """Connect to Interactive Brokers"""
        try:
            from ib_insync import IB, Stock
            
            self.ib = IB()
            self.ib.connect('127.0.0.1', 7497, clientId=1)  # TWS Paper port
            
            self.broker_connected = True
            self.broker_name = "Interactive Brokers"
            
            print(f"\n Connected to Interactive Brokers (PAPER)")
            return True
            
        except ImportError:
            print(" ib_insync not installed. Run: pip install ib_insync")
            return False
        except Exception as e:
            print(f" IB connection error: {e}")
            return False
    
    def execute_real_trade(self, signal: Dict):
        """Execute a REAL trade through connected broker"""
        if not hasattr(self, 'broker_connected') or not self.broker_connected:
            print(" No broker connected. Paper trading only.")
            return None
        
        try:
            if self.broker_name == "Alpaca":
                # Prepare order
                order_data = MarketOrderRequest(
                    symbol=signal['asset'].replace('-USD', 'USD'),
                    qty=signal['position_size'],
                    side=OrderSide.BUY if signal['signal'] == 'BUY' else OrderSide.SELL,
                    time_in_force=TimeInForce.DAY
                )
                
                # Submit order
                order = self.alpaca.submit_order(order_data)
                print(f" REAL ORDER EXECUTED: {order.id}")
                return order
                
            elif self.broker_name == "Interactive Brokers":
                # IB order logic here
                pass
                
        except Exception as e:
            print(f" Real trade error: {e}")
            return None
    
    # ============= DYNAMIC POSITION SIZING METHODS =============
    
    def calculate_dynamic_position_size(self, signal: Dict, account_volatility: float = 0.02) -> Dict:
        """
        Calculate optimal position size using dynamic position sizer
        
        Args:
            signal: Trading signal dictionary
            account_volatility: Current account volatility (default 2%)
        
        Returns:
            Position sizing details or None if sizer not available
        """
        if not hasattr(self, 'position_sizer') or self.position_sizer is None:
            return None
        
        try:
            # Get signal confidence
            confidence = signal.get('confidence', 0.5)
            
            # Get regime multiplier (from earlier detection)
            regime_multiplier = signal.get('regime_multiplier', 1.0)
            
            # Get win rate from paper trader
            win_rate = 0.5
            if hasattr(self, 'paper_trader'):
                perf = self.paper_trader.get_performance()
                win_rate = perf.get('win_rate', 50) / 100  # Convert percentage to decimal
            
            # Get Kelly fraction (can be from risk manager if available)
            kelly_fraction = 0.25  # Default conservative Kelly
            
            # Calculate size using dynamic sizer
            size_result = self.position_sizer.calculate_size(
                signal_confidence=confidence,
                account_volatility=account_volatility,
                market_regime_multiplier=regime_multiplier,
                win_rate=win_rate,
                kelly_fraction=kelly_fraction
            )
            
            # Calculate actual position units
            if 'entry_price' in signal and 'stop_loss' in signal:
                units_result = self.position_sizer.calculate_position_units(
                    account_balance=self.risk_manager.account_balance if hasattr(self, 'risk_manager') else 10000,
                    entry_price=signal['entry_price'],
                    stop_loss=signal['stop_loss'],
                    risk_percent=size_result['risk_percent'] / 100  # Convert back to decimal
                )
                
                # Combine results
                return {**size_result, **units_result}
            
            return size_result
            
        except Exception as e:
            print(f"  ⚠️ Dynamic position sizing error: {e}")
            return None

    def calculate_account_volatility(self, window: int = 20) -> float:
        """
        Calculate recent account volatility based on trade P&L
        
        Args:
            window: Number of recent trades to consider
        
        Returns:
            Volatility as decimal (e.g., 0.02 for 2%)
        """
        try:
            if not hasattr(self, 'paper_trader'):
                return 0.02  # Default 2%
            
            # Get recent trades
            trades = self.paper_trader.get_trade_history(limit=window)
            
            if len(trades) < 5:
                return 0.02  # Not enough data
            
            # Calculate P&L percentage returns
            returns = []
            for trade in trades:
                if trade.get('pnl_percent', 0) != 0:
                    returns.append(trade['pnl_percent'] / 100)  # Convert to decimal
            
            if len(returns) < 5:
                return 0.02
            
            # Calculate standard deviation of returns
            volatility = np.std(returns)
            
            # Cap between 1% and 10%
            volatility = max(0.01, min(volatility, 0.10))
            
            return volatility
            
        except Exception as e:
            print(f"  ⚠️ Account volatility calculation error: {e}")
            return 0.02

    # ===== ADD THE NEW METHODS HERE =====
    # 👇👇👇 INSERT THE NEW METHODS RIGHT HERE 👇👇👇

    def calculate_dynamic_take_profits(self, entry_price: float, stop_loss: float, 
                                    atr: float, volatility_ratio: float = 1.0) -> List[Dict]:
        """
        Calculate take profit levels that adapt to market volatility
        
        Args:
            entry_price: Entry price
            stop_loss: Stop loss price
            atr: Average True Range
            volatility_ratio: Current volatility vs average (1.0 = normal)
        
        Returns:
            List of TP levels with dynamic distances
        """
        risk = abs(entry_price - stop_loss)
        
        # Base R:R ratios
        base_ratios = [1.5, 2.5, 4.0]
        
        # Adjust based on volatility
        # Higher volatility = wider targets (but also wider stops already)
        volatility_multiplier = 1.0 + (volatility_ratio - 1.0) * 0.5
        
        # Adjust based on ATR size relative to price
        atr_pct = atr / entry_price
        if atr_pct > 0.02:  # High volatility asset
            atr_multiplier = 1.2
        elif atr_pct < 0.005:  # Low volatility asset
            atr_multiplier = 0.8
        else:
            atr_multiplier = 1.0
        
        tp_levels = []
        
        for i, ratio in enumerate(base_ratios):
            # Apply multipliers
            adjusted_ratio = ratio * volatility_multiplier * atr_multiplier
            
            # You need to set self.current_signal_direction somewhere before calling this
            # For now, let's assume we'll pass direction as a parameter
            # Let's modify the function signature to include direction
            if hasattr(self, 'current_signal_direction') and self.current_signal_direction == 'BUY':
                tp_price = entry_price + (risk * adjusted_ratio)
            else:
                tp_price = entry_price - (risk * adjusted_ratio)
            
            tp_levels.append({
                'level': i + 1,
                'price': round(tp_price, 6),
                'risk_reward': round(adjusted_ratio, 2),
                'base_ratio': ratio,
                'volatility_adjustment': round(volatility_multiplier, 2),
                'atr_adjustment': round(atr_multiplier, 2)
            })
        
        return tp_levels

    def get_volatility_ratio(self, df: pd.DataFrame) -> float:
        """
        Calculate current volatility vs historical average
        """
        if 'atr' not in df.columns or len(df) < 50:
            return 1.0
        
        current_atr = df['atr'].iloc[-1]
        avg_atr = df['atr'].rolling(50).mean().iloc[-1]
        
        if avg_atr == 0:
            return 1.0
        
        return current_atr / avg_atr

    # ============= LIVE TRADING WITH REAL STRATEGIES =============
    
    def portfolio_check(self, open_positions: List[Dict], new_signal: Dict) -> tuple:
        """
        Check if new trade fits portfolio strategy
        Returns: (should_trade, reason)
        """
        # ===== PORTFOLIO OPTIMIZER CHECKS =====
        
        # 1. Maximum positions check
        max_positions = getattr(self.risk_manager, 'max_positions', 5)
        if len(open_positions) >= max_positions:
            return False, f"Max positions ({max_positions}) reached"
        
        # 2. Category diversification
        categories = {}
        for pos in open_positions:
            cat = pos.get('category', 'unknown')
            categories[cat] = categories.get(cat, 0) + 1
        
        # Limit 3 positions per category
        new_cat = new_signal.get('category', 'unknown')
        if categories.get(new_cat, 0) >= 3:
            return False, f"Too many {new_cat} positions (max 3)"
        
        # 3. Correlation check (if we have enough positions)
        if len(open_positions) >= 2:
            # Simple correlation - same asset class?
            similar_assets = 0
            for pos in open_positions:
                if pos.get('category') == new_cat:
                    similar_assets += 1
            
            if similar_assets >= 2:
                return False, f"Already have {similar_assets} similar positions"
        
        # 4. Risk concentration
        total_risk = sum(pos.get('risk_amount', 0) for pos in open_positions)
        account_balance = getattr(self.risk_manager, 'account_balance', 10000)
        
        # Calculate risk for new trade (simplified)
        price_diff = abs(new_signal.get('entry_price', 0) - new_signal.get('stop_loss', 0))
        position_size = new_signal.get('position_size', 0.01)
        new_risk = price_diff * position_size
        
        total_risk_pct = (total_risk + new_risk) / account_balance * 100
        
        if total_risk_pct > 15:  # Max 15% portfolio risk
            return False, f"Portfolio risk too high ({total_risk_pct:.1f}%)"
        
        # All checks passed
        return True, "Approved"


    def check_portfolio_health(self):
        """Periodic portfolio health check and rebalancing suggestions"""
        try:
            open_positions = self.paper_trader.get_open_positions()
            
            if len(open_positions) < 2:
                return
            
            # Calculate diversification score
            positions_dict = {}
            for pos in open_positions:
                positions_dict[pos['asset']] = {
                    'value': pos['entry_price'] * pos['position_size'],
                    'category': pos['category'],
                    'risk_pct': pos.get('risk_pct', 1.0)
                }
            
            # Use portfolio optimizer if available
            if hasattr(self, 'portfolio_optimizer'):
                div_score = self.portfolio_optimizer.get_diversification_score(positions_dict)
                
                if div_score < 40:
                    print(f"\n⚠️ PORTFOLIO ALERT: Low diversification score ({div_score}%)")
                    print("   Consider closing some correlated positions")
                    
                    # Find most concentrated category
                    categories = {}
                    for pos in open_positions:
                        cat = pos.get('category', 'unknown')
                        categories[cat] = categories.get(cat, 0) + 1
                    
                    most_concentrated = max(categories.items(), key=lambda x: x[1])
                    if most_concentrated[1] >= 3:
                        print(f"   • Too many {most_concentrated[0]} positions ({most_concentrated[1]})")
            
            # Check risk distribution
            total_value = sum(p['entry_price'] * p['position_size'] for p in open_positions)
            account_balance = getattr(self.risk_manager, 'account_balance', 10000)
            
            if total_value > account_balance * 0.7:  # 70% of account in positions
                print(f"\n⚠️ PORTFOLIO ALERT: High exposure ({total_value/account_balance*100:.1f}%)")
                print("   Consider reducing position sizes")
                
        except Exception as e:
            print(f"⚠️ Portfolio health check error: {e}")

    def register_ml_model(self, asset: str, model_name: str = "ensemble", metadata: Dict = None):
        """Register an ML model in the registry"""
        if not hasattr(self, 'model_registry') or self.model_registry is None:
            return None
        
        try:
            key = self.model_registry.register_model(
                model_name=model_name,
                asset=asset,
                model_type="advanced_ensemble",
                metadata=metadata
            )
            print(f"   📝 Registered model for {asset}: {key}")
            return key
        except Exception as e:
            print(f"   ⚠️ Failed to register model: {e}")
            return None

    def update_model_prediction(self, asset: str, prediction: Dict, actual_move: float):
        """Update model performance with prediction result"""
        if not hasattr(self, 'model_registry') or self.model_registry is None:
            return
        
        try:
            # Get the model key for this asset
            model_key = f"{asset}_ensemble"
            self.model_registry.update_prediction(model_key, prediction, actual_move)
        except Exception as e:
            print(f"   ⚠️ Failed to update model prediction: {e}")

    def update_model_trade_result(self, asset: str, trade_result: Dict):
        """Update model performance with trade result"""
        if not hasattr(self, 'model_registry') or self.model_registry is None:
            return
        
        try:
            model_key = f"{asset}_ensemble"
            self.model_registry.update_trade_result(model_key, trade_result)
        except Exception as e:
            print(f"   ⚠️ Failed to update model trade result: {e}")

    def get_best_model_for_asset(self, asset: str) -> Optional[Dict]:
        """Get the best performing model for an asset"""
        if not hasattr(self, 'model_registry') or self.model_registry is None:
            return None
        
        try:
            return self.model_registry.get_model_for_asset(asset)
        except Exception as e:
            print(f"   ⚠️ Failed to get best model: {e}")
            return None

    def get_model_performance_report(self) -> Dict:
        """Get comprehensive model performance report"""
        if not hasattr(self, 'model_registry') or self.model_registry is None:
            return {'error': 'Model registry not available'}
        
        try:
            return self.model_registry.get_performance_report()
        except Exception as e:
            print(f"   ⚠️ Failed to get performance report: {e}")
            return {'error': str(e)}

    def scan_asset_parallel(self, asset: str, category: str) -> Optional[Dict]:
        """
        Scan single asset for signals (for parallel processing)
        """
        try:
            # Set current asset for strategies that need it
            self.current_asset = asset
            
            # Fetch data
            df_15m = self.fetch_historical_data(asset, 100, '15m')
            df_1h = self.fetch_historical_data(asset, 100, '1h')
            
            if df_15m.empty or df_1h.empty:
                return None
            
            # Add indicators
            df_15m = self.add_technical_indicators(df_15m)
            df_1h = self.add_technical_indicators(df_1h)
            
            # ===== ADD PRICE DATA TO PORTFOLIO OPTIMIZER =====
            try:
                if hasattr(self, 'portfolio_optimizer'):
                    self.portfolio_optimizer.update_price_data(asset, df_1h['close'])
            except Exception as e:
                print(f"      ⚠️ Could not update price data: {e}")
            # =================================================
            
            # Generate signal based on strategy mode
            if self.strategy_mode == 'strict':
                signal = self.strict_strategy(df_15m, df_1h)
            elif self.strategy_mode == 'fast':
                signal = self.fast_strategy(df_15m, df_1h)
            elif self.strategy_mode == 'voting':
                combined = self.get_combined_signal(df_15m)
                signal = combined if combined and combined['signal'] != 'HOLD' else None
            else:
                signal = self.balanced_strategy(df_15m, df_1h)
            
            if signal:
                # Add asset info
                signal['asset'] = asset
                signal['category'] = category
                
                # Get current price
                price, source = self.fetcher.get_real_time_price(asset, category)
                if price:
                    signal['entry_price'] = price
                    
                    # ===== ADD REGIME INFO IF AVAILABLE =====
                    if hasattr(self, 'current_regime') and self.current_regime:
                        regime_multiplier = 1.0
                        # You could calculate regime multiplier here if needed
                        signal['regime_multiplier'] = regime_multiplier
                        signal['regime'] = str(self.current_regime) if hasattr(self, 'current_regime') else 'unknown'
                    # ========================================
                    
                    return signal
            
            return None
            
        except Exception as e:
            print(f"  ⚠️ Error scanning {asset}: {e}")
            return None

    def scan_all_assets_parallel(self):
        """
        Scan all assets in parallel using ThreadPool
        """
        print(f"\n🚀 Scanning {len(self.get_asset_list())} assets in parallel...")
        
        assets = self.get_asset_list()
        signals = []
        
        with ThreadPoolExecutor(max_workers=10) as executor:
            # Submit all tasks
            future_to_asset = {
                executor.submit(self.scan_asset_parallel, asset, category): (asset, category)
                for asset, category in assets
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_asset):
                asset, category = future_to_asset[future]
                try:
                    signal = future.result(timeout=15)
                    if signal:
                        signals.append(signal)
                        print(f"  ✅ {asset}: {signal['signal']} signal found")
                    else:
                        print(f"  ⏭️ {asset}: No signal")
                except Exception as e:
                    print(f"  ❌ {asset} failed: {e}")
        
        print(f"\n✅ Found {len(signals)} signals from {len(assets)} assets")
        
        # Sort by confidence
        signals.sort(key=lambda x: x.get('confidence', 0), reverse=True)
        
        return signals
    
    def process_parallel_signals(self, signals: List[Dict]):
        """
        Process signals from parallel scan and execute trades
        """
        if not signals:
            print("  No signals to process")
            return
        
        print(f"\n📊 Processing {len(signals)} signals...")
        
        for signal in signals[:5]:  # Process top 5 signals
            try:
                asset = signal['asset']
                category = signal['category']
                
                # Get current positions
                open_positions = self.paper_trader.get_open_positions()
                
                # ===== PORTFOLIO CHECKS =====
                should_trade = True
                reason = ""
                
                # Check max positions
                if len(open_positions) >= 5:
                    should_trade = False
                    reason = "Max positions reached"
                
                # Check category diversification
                if should_trade:
                    cat_count = sum(1 for p in open_positions if p.get('category') == category)
                    if cat_count >= 3:
                        should_trade = False
                        reason = f"Too many {category} positions"
                
                # ===== CORRELATION CHECK =====
                if should_trade and open_positions and hasattr(self, 'portfolio_optimizer'):
                    allowed, corr_reason = self.portfolio_optimizer.check_position_correlation(
                        asset, category, open_positions
                    )
                    if not allowed:
                        should_trade = False
                        reason = corr_reason
                
                # ===== DAILY LOSS LIMIT CHECK =====
                if should_trade and hasattr(self, 'daily_loss_limit') and self.daily_loss_limit:
                    trading_allowed, _ = self.daily_loss_limit.update(0)
                    if not trading_allowed:
                        should_trade = False
                        reason = "Daily loss limit hit"
                
                if should_trade:
                    # Execute trade
                    trade = self.paper_trader.execute_signal(signal)
                    if trade:
                        print(f"  ✅ EXECUTED: {asset} {signal['signal']}")
                else:
                    print(f"  ⏭️ SKIPPED {asset}: {reason}")
                    
            except Exception as e:
                print(f"  ⚠️ Error processing {signal.get('asset', 'unknown')}: {e}")

    def start_professional_trading(self):
        """Start live trading with ALL features ACTIVATED (using 15m + 1h only)"""
        print("\n" + "="*70)
        print("🚀 PROFESSIONAL LIVE TRADING - ALL FEATURES ACTIVATED")
        print("="*70)
        print("📊 Portfolio Optimizer: ACTIVE - Monitoring diversification")
        print("⏰ Multi-Timeframe: ACTIVE - Checking 15m + 1h confluence")
        print("🤖 ML Predictions: ACTIVE - Ensemble models (10+ algorithms)")
        print("🧠 ADVANCED AI: ACTIVE - Reinforcement Learning + Transformers + Swarm")
        print("   • RL Agent: PPO - Learns optimal trading policies")
        print("   • Transformer: Time Series - Predicts price movements")
        print("   • Swarm Intelligence: 10+ agents collaborating")
        print("📰 Sentiment Analysis: ACTIVE - News scanning")
        print("🔄 Intelligent Auto-Trainer: ACTIVE - Event-based learning")
        print("   • Price movements (>2%)")
        print("   • Session changes (London/NY/Asia)")
        print("   • Major news events")
        print("   • Time fallback (4 hours)")
        
        # 🔥 PROFITABILITY UPGRADE: Enhanced display
        if hasattr(self, 'profitability_upgrades_active') and self.profitability_upgrades_active:
            print("💰 PROFITABILITY UPGRADES: ACTIVE")
            print("   • 60-min cooldown after losses")
            print("   • Category limits (1 crypto, 2 forex)")
            print("   • ATR-based stops")
            print("   • Entry quality filters")
            print("   • 4-hour stale position cleanup")
        
        # 📊 MARKET REGIME DETECTION: Add to banner
        print("📊 MARKET REGIME DETECTION: ACTIVE")
        print("   • Dynamic position sizing based on market conditions")
        print("   • 1.5-1.8x in strong trends")
        print("   • 0.3-0.7x in choppy/volatile markets")
        
        # 🔗 CORRELATION CHECKER: Add to banner
        print("🔗 CORRELATION CHECKER: ACTIVE")
        print("   • Prevents correlated position blowups")
        print("   • Maximum correlation threshold: 0.7")
        print("   • Portfolio VaR (Value at Risk) tracking")
        print("")
        print("📡 DATA SOURCES:")
        print("   • Finnhub      - Real-time stocks, forex, crypto")
        print("   • Twelve Data  - Commodities, indices, ETFs (supports 4H!)")
        print("   • Alpha Vantage - Stocks, forex, commodities")
        print("   • Yahoo Finance - Universal fallback")
        print("   • Binance      - Crypto WebSocket (real-time)")
        print("="*70)
        
        self.is_running = True
        
        # Initialize Advanced AI systems
        try:
            from advanced_ai import AdvancedAIIntegration
            self.ai_system = AdvancedAIIntegration()
            # We'll initialize with first asset's data when available
            self.ai_initialized = False
            print("🧠 Advanced AI systems: READY for initialization")
        except Exception as e:
            print(f"⚠️ Could not initialize Advanced AI: {e}")
            self.ai_system = None
        
        # Start auto-trainer
        if hasattr(self, 'auto_trainer'):
            self.auto_trainer.start()
        
        # Get the strategy mode from arguments
        strategy_mode = getattr(self, 'strategy_mode', 'balanced')
        
        def trading_loop():
            ai_init_counter = 0
            health_check_counter = 0
            last_day_check = datetime.now().date()
            
            while self.is_running:
                try:
                    # ===== DAILY RESET CHECK =====
                    current_date = datetime.now().date()
                    if current_date != last_day_check:
                        # New day - reset daily loss limit
                        if hasattr(self, 'daily_loss_limit') and self.daily_loss_limit:
                            self.daily_loss_limit.reset_daily()
                            current_balance = self.risk_manager.account_balance if hasattr(self, 'risk_manager') else account_balance
                            self.daily_loss_limit.set_initial_balance(current_balance)
                            print(f"\n📅 New trading day started - Daily loss limit reset")
                        last_day_check = current_date
                    
                    # Track daily trades
                    daily_trades = 0
                    
                    # Get current positions for portfolio analysis
                    open_positions = self.paper_trader.get_open_positions()
                    
                    # ===== PORTFOLIO OPTIMIZER: Check diversification =====
                    if len(open_positions) >= 2:
                        positions_dict = {}
                        for pos in open_positions:
                            positions_dict[pos['asset']] = {
                                'value': pos['entry_price'] * pos['position_size'],
                                'category': pos['category'],
                                'risk_pct': pos.get('risk_pct', 1.0)
                            }
                        
                        # Get diversification score
                        if hasattr(self, 'portfolio_optimizer'):
                            div_score = self.portfolio_optimizer.get_diversification_score(positions_dict)
                            
                            if div_score < 40:
                                print(f"\n⚠️ PORTFOLIO ALERT: Low diversification score ({div_score}%)")
                                
                                # Find most concentrated category
                                categories = {}
                                for pos in open_positions:
                                    cat = pos.get('category', 'unknown')
                                    categories[cat] = categories.get(cat, 0) + 1
                                
                                most = max(categories.items(), key=lambda x: x[1])
                                if most[1] >= 3:
                                    print(f"   • Too many {most[0]} positions ({most[1]})")
                                    print(f"   • Consider taking profit on 1 position")
                    
                    # 🔥 PROFITABILITY UPGRADE: Check for stale positions
                    if hasattr(self, 'profitability_upgrades_active') and self.profitability_upgrades_active:
                        try:
                            # Get current prices for stale position check
                            current_prices = {}
                            for pos in open_positions:
                                price, _ = self.fetcher.get_real_time_price(pos['asset'], pos.get('category', 'unknown'))
                                if price:
                                    current_prices[pos['asset']] = price
                            
                            # Check for stale positions (open >4 hours with no profit)
                            if hasattr(self, 'position_age_monitor'):
                                stale = self.position_age_monitor.get_stale_positions(open_positions, current_prices)
                                for s in stale:
                                    print(f"  ⏰ FORCE CLOSING stale position: {s['asset']} (open {s['age_hours']}h)")
                                    if hasattr(self.paper_trader, 'force_close'):
                                        self.paper_trader.force_close(s['trade_id'], current_prices[s['asset']], s['reason'])
                                    else:
                                        print(f"     ⚠️ Would close: {s['trade_id']} - {s['reason']} (force_close method not available)")
                        except Exception as e:
                            print(f"  ⚠️ Stale position check error: {e}")
                    
                    # ===== REPLACE THE OLD SCANNING LOOP WITH THIS =====
                    # ===== PARALLEL SIGNAL SCANNING =====
                    print(f"\n🚀 Scanning assets in parallel...")
                    
                    # Get all assets that are currently open for trading
                    active_assets = [
                        (asset, category) for asset, category in self.get_asset_list()
                        if MarketHours.get_status().get(category, False)
                    ]
                    
                    print(f"   Active markets: {len(active_assets)} assets")
                    
                    # Scan all active assets in parallel
                    signals = self.scan_all_assets_parallel()
                    
                    # Process the top signals
                    if signals:
                        print(f"\n📊 Processing top signals...")
                        
                        for signal in signals[:3]:  # Process top 3 signals
                            try:
                                asset = signal['asset']
                                category = signal['category']
                                
                                # ===== PORTFOLIO CHECKS =====
                                should_trade = True
                                reason = ""
                                
                                # Check max positions
                                if len(open_positions) >= 5:
                                    should_trade = False
                                    reason = "Max positions (5) reached"
                                
                                # Check category diversification
                                if should_trade:
                                    cat_count = sum(1 for p in open_positions if p.get('category') == category)
                                    if cat_count >= 3:
                                        should_trade = False
                                        reason = f"Too many {category} positions (max 3)"
                                
                                # ===== CORRELATION CHECK =====
                                if should_trade and open_positions and hasattr(self, 'portfolio_optimizer'):
                                    try:
                                        allowed, corr_reason = self.portfolio_optimizer.check_position_correlation(
                                            asset, category, open_positions
                                        )
                                        if not allowed:
                                            should_trade = False
                                            reason = corr_reason
                                            print(f"      ⚠️ Correlation check: {corr_reason}")
                                    except Exception as e:
                                        print(f"      ⚠️ Correlation check error: {e}")
                                
                                # ===== DAILY LOSS LIMIT CHECK =====
                                if should_trade and hasattr(self, 'daily_loss_limit') and self.daily_loss_limit:
                                    trading_allowed, status_message = self.daily_loss_limit.update(0)
                                    if not trading_allowed:
                                        should_trade = False
                                        reason = f"Daily loss limit: {status_message}"
                                        print(f"      ⏸️ {reason}")
                                
                                if should_trade:
                                    # Execute paper trade
                                    trade = self.paper_trader.execute_signal(signal)
                                    if trade:
                                        daily_trades += 1
                                        print(f"  ✅ EXECUTED: {asset} {signal['signal']} [{signal.get('strategy_id', 'UNKNOWN')}]")
                                        
                                        # Show TP levels if available
                                        if signal.get('take_profit_levels') and len(signal.get('take_profit_levels', [])) > 0:
                                            tp = signal['take_profit_levels'][0]
                                            print(f"     🎯 TP1: {tp['price']:.5f} ({tp.get('risk_reward', 1.5)}:1)")
                                else:
                                    print(f"  ⏭️ SKIPPED {asset}: {reason}")
                                    
                            except Exception as e:
                                print(f"  ⚠️ Error processing {signal.get('asset', 'unknown')}: {e}")
                    else:
                        print(f"  No signals found")
                    # ===================================================
                    
                    # Update positions
                    self.update_all_positions()
                    
                    # ===== PORTFOLIO HEALTH CHECK =====
                    health_check_counter += 1
                    try:
                        # Get updated open positions
                        open_positions = self.paper_trader.get_open_positions()
                        
                        # Run portfolio health check
                        if hasattr(self, 'portfolio_optimizer') and open_positions:
                            health = self.portfolio_optimizer.get_portfolio_health_report(open_positions)
                            
                            # Display warnings if any
                            if health['warnings']:
                                print(f"\n⚠️ PORTFOLIO WARNINGS:")
                                for warning in health['warnings']:
                                    print(f"  • {warning}")
                            
                            # Check if rebalancing needed
                            if health.get('needs_rebalancing', False):
                                print(f"  📊 Portfolio needs rebalancing (score: {health['diversification_score']}/100)")
                                print(f"     Current VaR (95%): ${health['var_95']} ({health['var_95_percent']}%)")
                            
                            # Show category breakdown occasionally (every 10 cycles)
                            if health_check_counter % 10 == 0 and health.get('category_breakdown'):
                                print(f"\n📊 CATEGORY BREAKDOWN:")
                                for cat, data in health['category_breakdown'].items():
                                    print(f"  • {cat}: {data['count']} positions (${data['value']:.2f})")
                            
                            # Show diversification score periodically
                            if health_check_counter % 5 == 0:
                                print(f"  📈 Diversification Score: {health['diversification_score']}/100")
                                
                    except Exception as e:
                        print(f"  ⚠️ Portfolio health check error: {e}")

                    # ===== VERIFY ML PREDICTIONS =====
                    if health_check_counter % 5 == 0:
                        try:
                            if hasattr(self, 'verify_pending_predictions'):
                                self.verify_pending_predictions()
                        except Exception as e:
                            print(f"  ⚠️ Prediction verification error: {e}")

                    # ===== CACHE MAINTENANCE =====
                    # Every 100 cycles, log cache stats (don't clear)
                    if health_check_counter % 100 == 0 and hasattr(self, 'cache_manager'):
                        # You could add cache stats here if you implement a stats method
                        # For now, just a placeholder
                        pass

                    # Clear cache once per day at midnight
                    current_date = datetime.now().date()
                    if not hasattr(self, '_last_cache_cleanup'):
                        self._last_cache_cleanup = current_date

                    if current_date != self._last_cache_cleanup:
                        if hasattr(self, 'clear_cache'):
                            self.clear_cache()
                            print("🔄 Daily cache cleanup completed")
                        self._last_cache_cleanup = current_date
                    # ==============================

                    # ===== DAILY LOSS LIMIT STATUS =====
                    daily_loss_status = ""
                    if hasattr(self, 'daily_loss_limit') and self.daily_loss_limit:
                        status = self.daily_loss_limit.get_status()
                        if status['trading_paused']:
                            daily_loss_status = " | ⏸️ LOSS LIMIT PAUSED"
                        else:
                            daily_loss_status = f" | Daily: {status['daily_loss_pct']:.1f}%"
                    # ===================================

                    # ===== SESSION TRACKER INSIGHTS =====
                    if hasattr(self, 'session_tracker') and self.session_tracker:
                        try:
                            # Show session recommendations every 10 cycles
                            if health_check_counter % 10 == 0:
                                best = self.session_tracker.get_best_session()
                                if 'message' not in best:
                                    print(f"\n📈 SESSION INSIGHT:")
                                    print(f"   Best session: {best['emoji']} {best['session']} ({best['win_rate']}% win rate)")
                                    
                                    # Show hourly breakdown
                                    hourly = self.session_tracker.analyze_by_hour()
                                    if not hourly.empty:
                                        top_hours = hourly.head(3)
                                        hours_str = []
                                        for _, h in top_hours.iterrows():
                                            hour = int(h['hour'])
                                            hours_str.append(f"{hour}:00")
                                        print(f"   Best hours: {', '.join(hours_str)}")
                        except Exception as e:
                            print(f"  ⚠️ Session tracker error: {e}")
                    # ====================================

                    # Show performance with all enhancements
                    perf = self.paper_trader.get_performance()
                    ai_status = "🧠 AI ACTIVE" if (self.ai_system and self.ai_initialized) else "🤖 AI INITIALIZING"

                    # 🔥 PROFITABILITY UPGRADE: Add to status line
                    upgrade_status = "💰 UPGRADES ON" if (hasattr(self, 'profitability_upgrades_active') and self.profitability_upgrades_active) else "💰 UPGRADES OFF"

                    # 📊 Add regime info to status line
                    regime_info = ""
                    if hasattr(self, 'current_regime') and self.current_regime:
                        regime_str = str(self.current_regime.value)[:15] if hasattr(self.current_regime, 'value') else str(self.current_regime)[:15]
                        regime_info = f" | 📊 {regime_str}"

                    # Print comprehensive status
                    print(f"\n📊 Portfolio: ${perf['current_balance']:.2f} | "
                        f"Win Rate: {perf['win_rate']}% | "
                        f"Open: {perf['open_positions']} | "
                        f"Today: {daily_trades} trades | "
                        f"Mode: {strategy_name} | "
                        f"{ai_status} | "
                        f"{upgrade_status}"
                        f"{regime_info}"
                        f"{daily_loss_status}")
                    
                except Exception as e:
                    print(f"❌ Trading error: {e}")
                    import traceback
                    traceback.print_exc()
                
                time.sleep(60)  # Scan every minute
        
        thread = threading.Thread(target=trading_loop, daemon=True)
        thread.start()
        print(f"✅ Professional trading started with {strategy_mode.upper()} strategy (15m + 1h)!")
        if hasattr(self, 'ai_system') and self.ai_system:
            print(f"🧠 Advanced AI systems will initialize on first data fetch")
        
        # 🔥 PROFITABILITY UPGRADE: Print confirmation
        if hasattr(self, 'profitability_upgrades_active') and self.profitability_upgrades_active:
            print(f"💰 Profitability upgrades protecting your account!")
        
        # 📊 Market Regime Detection confirmation
        print(f"📊 Market Regime Detection active - position sizing adapts to market conditions")
        
        # 🔗 Correlation Checker confirmation
        print(f"🔗 Correlation Checker active - preventing correlated position blowups")
    
    def get_combined_signal(self, df: pd.DataFrame) -> Dict:
        """
        Get combined signal from all strategies using voting
        """
        if not hasattr(self, 'voting_engine'):
            return {'signal': 'HOLD', 'confidence': 0}
        
        # Get signals from all strategies
        signals = self.voting_engine.get_all_signals(df)
        
        if not signals:
            return {'signal': 'HOLD', 'confidence': 0}
        
        # Let them vote
        combined = self.voting_engine.weighted_vote(signals)
        
        if combined:
            print(f"\n🗳️ VOTING RESULTS:")
            print(f"   BUY:  {combined['buy_votes']:.1%}")
            print(f"   SELL: {combined['sell_votes']:.1%}")
            print(f"   Final: {combined['signal']} ({combined['confidence']:.1%})")
            print(f"   Strategies: {', '.join(combined['contributing_strategies'][:5])}")
        
        return combined if combined else {'signal': 'HOLD', 'confidence': 0}

    def show_session_report(self):
        """Display comprehensive session performance report"""
        if not hasattr(self, 'session_tracker') or not self.session_tracker:
            print("❌ Session tracker not initialized")
            return
        
        report = self.session_tracker.get_summary_report()
        
        print("\n" + "="*70)
        print("📊 SESSION PERFORMANCE REPORT")
        print("="*70)
        print(f"Total Trades: {report['total_trades']}")
        
        print("\n📈 Performance by Session:")
        for session, stats in report['sessions'].items():
            if isinstance(stats, dict) and 'trades' in stats:
                emoji = stats.get('emoji', '')
                print(f"  {emoji} {stats['session']}:")
                print(f"     • Trades: {stats['trades']}")
                print(f"     • Win Rate: {stats['win_rate']}%")
                print(f"     • Total P&L: ${stats['total_pnl']}")
        
        if 'best_session' in report and 'message' not in report['best_session']:
            print(f"\n🏆 Best Session: {report['best_session']['emoji']} {report['best_session']['session']}")
            print(f"   Win Rate: {report['best_session']['win_rate']}%")
        
        if 'recommendation' in report:
            print(f"\n💡 Recommendation: {report['recommendation']}")

    def get_asset_list(self) -> List[tuple]:
        """Get COMPLETE list of assets to trade"""
        return [
            ('XAU/USD', 'commodities'),
            ('BTC-USD', 'crypto'),
            ('EUR/USD', 'forex'),
            ('GBP/USD', 'forex'),
            ('USD/JPY', 'forex'),
            ('AUD/USD', 'forex'),
            ('XAG/USD', 'commodities'),
            ('^GSPC', 'indices'),  # S&P 500
            ('^DJI', 'indices'),   # Dow Jones
            ('^IXIC', 'indices'),  # Nasdaq
            ('ETH-USD', 'crypto'),
            ('BNB-USD', 'crypto'),
            ('SOL-USD', 'crypto'),
            ('XRP-USD', 'crypto'),
            ('CL=F', 'commodities'),  # Crude Oil
            # ===== CRYPTO (24/7) =====
            ('ADA-USD', 'crypto'),
            ('DOGE-USD', 'crypto'),
            ('DOT-USD', 'crypto'),
            ('LTC-USD', 'crypto'),
            ('AVAX-USD', 'crypto'),
            ('LINK-USD', 'crypto'),
            
            # ===== FOREX (24/5) =====
            ('USD/CAD', 'forex'),
            ('NZD/USD', 'forex'),
            ('USD/CHF', 'forex'),
            ('EUR/GBP', 'forex'),
            ('EUR/JPY', 'forex'),
            ('GBP/JPY', 'forex'),
            ('AUD/JPY', 'forex'),
            ('EUR/AUD', 'forex'),
            ('GBP/AUD', 'forex'),
            
            # ===== STOCKS (Mon-Fri) =====
            ('AAPL', 'stocks'),
            ('MSFT', 'stocks'),
            ('GOOGL', 'stocks'),
            ('AMZN', 'stocks'),
            ('TSLA', 'stocks'),
            ('NVDA', 'stocks'),
            ('META', 'stocks'),
            ('JPM', 'stocks'),
            ('V', 'stocks'),
            ('WMT', 'stocks'),
            ('JNJ', 'stocks'),
            ('PG', 'stocks'),
            ('KO', 'stocks'),
            ('PEP', 'stocks'),
            ('HD', 'stocks'),
            ('DIS', 'stocks'),
            ('NFLX', 'stocks'),
            ('CSCO', 'stocks'),
            ('INTC', 'stocks'),
            ('AMD', 'stocks'),
            ('BA', 'stocks'),
            ('GE', 'stocks'),
            ('F', 'stocks'),
            ('GM', 'stocks'),
            ('XOM', 'stocks'),
            ('CVX', 'stocks'),
            ('COP', 'stocks'),
            ('PFE', 'stocks'),
            ('MRK', 'stocks'),
            ('ABBV', 'stocks'),
            
            # ===== COMMODITIES (Limited hours) =====
            ('XPT/USD', 'commodities'),  # Platinum Spot (replaces PL=F)
            ('XPD/USD', 'commodities'),  # Palladium Spot (replaces PA=F)
            ('WTI/USD', 'commodities'),  # WTI Crude Oil Spot (replaces CL=F)
            ('NG/USD', 'commodities'),   # Natural Gas Spot (replaces NG=F)
            ('XCU/USD', 'commodities'),  # Copper Spot (replaces HG=F)
            
            # ===== INDICES (Follow stocks) =====
            ('^FTSE', 'indices'),  # FTSE 100
            ('^N225', 'indices'),  # Nikkei 225
            ('^HSI', 'indices'),   # Hang Seng
            ('^GDAXI', 'indices'), # DAX
            ('^FCHI', 'indices'),  # CAC 40
            ('^VIX', 'indices'),   # Volatility Index
            
            # ===== ETFs =====
            ('SPY', 'stocks'),     # S&P 500 ETF
            ('QQQ', 'stocks'),     # Nasdaq ETF
            ('DIA', 'stocks'),     # Dow ETF
            ('IWM', 'stocks'),     # Russell 2000 ETF
            ('XLK', 'stocks'),     # Tech ETF
            ('XLF', 'stocks'),     # Financial ETF
            ('XLV', 'stocks'),     # Healthcare ETF
            ('XLE', 'stocks'),     # Energy ETF
        ] # type: ignore
    
    # ============= UTILITY FUNCTIONS =============
    
    def fetch_historical_data(self, asset: str, days: int = 100, interval: str = '1d') -> pd.DataFrame:
        """
        Fetch historical data using MULTIPLE APIs with interval support
        Tries all available sources in parallel for fastest response
        Supports: 1m, 5m, 15m, 1h, 4h, 1d
        """
        # ===== CHECK CACHE FIRST =====
        if hasattr(self, 'cache_manager') and self.cache_manager and self.cache_manager.enabled:
            cached_data = self.cache_manager.get_historical_data(asset, interval)
            if cached_data is not None:
                print(f"   ✅ Using cached data for {asset} ({interval})")
                return cached_data
        # =============================

        # ===== TRY MULTIPLE SOURCES IN PARALLEL =====
        sources = []
        
        # 1. Yahoo Finance (always available)
        sources.append(('yahoo', lambda: self._fetch_yahoo_historical(asset, days, interval)))
        
        # 2. Twelve Data (if available)
        if hasattr(self.fetcher, 'td_client') and self.fetcher.td_client:
            sources.append(('twelve', lambda: self._fetch_twelve_historical(asset, interval)))
        
        # 3. Alpha Vantage (if available)
        if hasattr(self.fetcher, 'av_ts') and self.fetcher.av_ts:
            sources.append(('alpha', lambda: self._fetch_alpha_historical(asset, interval)))
        
        # 4. Finnhub (if available)
        if hasattr(self.fetcher, 'finnhub_client') and self.fetcher.finnhub_client:
            sources.append(('finnhub', lambda: self._fetch_finnhub_historical(asset, interval)))
        
        # Try all sources in parallel, take the best result
        best_df = None
        best_rows = 0
        
        with ThreadPoolExecutor(max_workers=len(sources)) as executor:
            future_to_source = {
                executor.submit(func): name 
                for name, func in sources
            }
            
            for future in as_completed(future_to_source):
                source_name = future_to_source[future]
                try:
                    df = future.result(timeout=5)
                    if df is not None and not df.empty:
                        rows = len(df)
                        print(f"   ✅ {source_name}: Got {rows} rows for {asset} ({interval})")
                        
                        if rows > best_rows:
                            best_df = df
                            best_rows = rows
                except Exception as e:
                    print(f"   ⚠️ {source_name} failed: {e}")
                    continue

         # ===== SAVE TO CACHE =====
        if best_df is not None and hasattr(self, 'cache_manager') and self.cache_manager:
            # Cache for 5 minutes
            self.cache_manager.set_historical_data(asset, interval, best_df, ttl=300)
        # =========================
        
        if best_df is not None:
            return best_df
        
        print(f"   ❌ No data for {asset} ({interval}) from any source")
        return pd.DataFrame()


    def _fetch_yahoo_historical(self, asset: str, days: int, interval: str) -> pd.DataFrame:
        """Fetch from Yahoo Finance"""
        try:
            # Symbol mapping
            symbol_map = {
                'EUR/USD': 'EURUSD=X', 'GBP/USD': 'GBPUSD=X', 'USD/JPY': 'JPY=X',
                'AUD/USD': 'AUDUSD=X', 'USD/CAD': 'CAD=X', 'NZD/USD': 'NZDUSD=X',
                'USD/CHF': 'CHF=X', 'EUR/GBP': 'EURGBP=X', 'EUR/JPY': 'EURJPY=X',
                'GBP/JPY': 'GBPJPY=X', 'AUD/JPY': 'AUDJPY=X', 'EUR/AUD': 'EURAUD=X',
                'GBP/AUD': 'GBPAUD=X',
                'BTC-USD': 'BTC-USD', 'ETH-USD': 'ETH-USD', 'BNB-USD': 'BNB-USD',
                'AAPL': 'AAPL', 'MSFT': 'MSFT', 'GOOGL': 'GOOGL',
                'GC=F': 'GC=F', 'SI=F': 'SI=F', 'CL=F': 'CL=F',
                '^GSPC': '^GSPC', '^DJI': '^DJI', '^IXIC': '^IXIC',
            }
            
            yahoo_symbol = symbol_map.get(asset, asset)
            
            # Interval mapping
            interval_map = {
                '1m': '1m', '5m': '5m', '15m': '15m', '1h': '1h', '4h': '1h', '1d': '1d'
            }
            
            period_map = {
                '1m': '1d', '5m': '5d', '15m': '1mo', '1h': '3mo', '4h': '6mo', '1d': '1y'
            }
            
            yahoo_interval = interval_map.get(interval, '1d')
            yahoo_period = period_map.get(interval, f"{days}d")
            
            ticker = yf.Ticker(yahoo_symbol)
            df = ticker.history(period=yahoo_period, interval=yahoo_interval)
            
            if df.empty:
                df = ticker.history(period=f"{days}d", interval=yahoo_interval)
            
            if not df.empty:
                df.columns = df.columns.str.lower()
                df.index.name = 'date'
                
                # Resample 1h to 4h if needed
                if interval == '4h' and yahoo_interval == '1h':
                    df = df.resample('4H').agg({
                        'open': 'first', 'high': 'max', 'low': 'min',
                        'close': 'last', 'volume': 'sum'
                    }).dropna()
                
                return df
        except:
            pass
        return pd.DataFrame()


    def _fetch_twelve_historical(self, asset: str, interval: str) -> pd.DataFrame:
        """Fetch from Twelve Data"""
        try:
            # Map Yahoo symbols to Twelve Data format
            symbol_map = {

                'XAU/USD': 'XAU/USD',      # Gold Spot
                'BTC-USD': 'BTC/USD',
                'ETH-USD': 'ETH/USD',
                'EUR/USD': 'EUR/USD', 
                'GBP/USD': 'GBP/USD',
                'USD/JPY': 'USD/JPY',
                'AUD/USD': 'AUD/USD',
                'BNB-USD': 'BNB/USD',
                'SOL-USD': 'SOL/USD',
                'XRP-USD': 'XRP/USD',
                'XAG/USD': 'XAG/USD',      # Silver Spot
                'WTI/USD': 'WTI/USD',      # WTI Crude Oil Spot
                
                # Stocks
                'AAPL': 'AAPL', 
                'MSFT': 'MSFT',
                'GOOGL': 'GOOGL',
                'AMZN': 'AMZN',
                'TSLA': 'TSLA',
                'NVDA': 'NVDA',

                # Forex
                'USD/CAD': 'USD/CAD',
                
                # ===== SPOT METALS (ADDED) =====
                'XPT/USD': 'XPT/USD',      # Platinum Spot
                'XPD/USD': 'XPD/USD',      # Palladium Spot
                'NG/USD': 'NG/USD',        # Natural Gas Spot
                'XCU/USD': 'XCU/USD',      # Copper Spot
                
                # Futures (keep as fallback)
                'GC=F': 'GC',               # Gold Futures
                'SI=F': 'SI',               # Silver Futures
                'CL=F': 'CL',               # WTI Futures
                'NG=F': 'NG',               # Natural Gas Futures
                'HG=F': 'HG',               # Copper Futures
            }
            
            twelve_symbol = symbol_map.get(asset, asset.replace('-USD', '/USD'))
            
            # Map interval
            interval_map = {
                '1m': '1min', '5m': '5min', '15m': '15min',
                '1h': '1h', '4h': '4h', '1d': '1day'
            }
            twelve_interval = interval_map.get(interval, '1day')
            
            # Get data from Twelve Data
            ts = self.fetcher.td_client.time_series(
                symbol=twelve_symbol,
                interval=twelve_interval,
                outputsize=100
            )
            
            data = ts.as_json()
            if data and len(data) > 0:
                df = pd.DataFrame(data)
                df['datetime'] = pd.to_datetime(df['datetime'])
                df.set_index('datetime', inplace=True)
                df = df.astype(float)
                df.index.name = 'date'
                return df[['open', 'high', 'low', 'close', 'volume']]
        except Exception as e:
            print(f"⚠️ Twelve Data error for {asset}: {e}")
            
        return pd.DataFrame()


    def _fetch_alpha_historical(self, asset: str, interval: str) -> pd.DataFrame:
        """Fetch from Alpha Vantage"""
        try:
            # Alpha Vantage has different endpoints
            if '=F' in asset:  # Commodity
                return pd.DataFrame()  # Skip for now
            elif '/' in asset:  # Forex
                return pd.DataFrame()  # Skip for now
            else:  # Stocks
                function = 'TIME_SERIES_INTRADAY' if interval != '1d' else 'TIME_SERIES_DAILY'
                
                params = {
                    'function': function,
                    'symbol': asset,
                    'apikey': self.fetcher.alpha_vantage_key
                }
                
                if function == 'TIME_SERIES_INTRADAY':
                    params['interval'] = interval
                    params['outputsize'] = 'compact'
                
                response = self.fetcher.session.get(
                    'https://www.alphavantage.co/query',
                    params=params,
                    timeout=5
                )
                
                data = response.json()
                time_series_key = [k for k in data.keys() if 'Time Series' in k]
                
                if time_series_key:
                    df = pd.DataFrame.from_dict(data[time_series_key[0]], orient='index')
                    df.columns = ['open', 'high', 'low', 'close', 'volume']
                    df.index = pd.to_datetime(df.index)
                    df = df.sort_index().astype(float)
                    df.index.name = 'date'
                    return df
        except:
            pass
        return pd.DataFrame()


    def _fetch_finnhub_historical(self, asset: str, interval: str) -> pd.DataFrame:
        """Fetch from Finnhub"""
        try:
            # Map to Finnhub format
            if 'USD' in asset and '-' in asset:  # Crypto
                base = asset.split('-')[0]
                symbol = f"BINANCE:{base}USDT"
            elif '/' in asset:  # Forex
                symbol = f"OANDA:{asset.replace('/', '_')}"
            else:  # Stock
                symbol = asset
            
            # Map interval to Finnhub resolution
            resolution_map = {
                '1m': '1', '5m': '5', '15m': '15',
                '1h': '60', '4h': '240', '1d': 'D'
            }
            resolution = resolution_map.get(interval, 'D')
            
            # Calculate timestamps
            to_time = int(time.time())
            if interval == '1d':
                from_time = to_time - (365 * 86400)  # 1 year
            else:
                from_time = to_time - (30 * 86400)   # 30 days
            
            # Determine endpoint
            if 'BINANCE' in symbol:
                endpoint = 'https://finnhub.io/api/v1/crypto/candle'
            elif 'OANDA' in symbol:
                endpoint = 'https://finnhub.io/api/v1/forex/candle'
            else:
                endpoint = 'https://finnhub.io/api/v1/stock/candle'
            
            response = self.fetcher.session.get(
                endpoint,
                params={
                    'symbol': symbol,
                    'resolution': resolution,
                    'from': from_time,
                    'to': to_time,
                    'token': self.fetcher.finnhub_key
                },
                timeout=5
            )
            
            data = response.json()
            
            if data.get('s') == 'ok' and 'c' in data and len(data['c']) > 0:
                df = pd.DataFrame({
                    'timestamp': data['t'],
                    'open': data['o'],
                    'high': data['h'],
                    'low': data['l'],
                    'close': data['c'],
                    'volume': data.get('v', [0] * len(data['c']))
                })
                df['date'] = pd.to_datetime(df['timestamp'], unit='s')
                df.set_index('date', inplace=True)
                return df
        except:
            pass
        return pd.DataFrame()
    
    def add_technical_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add all technical indicators"""
        try:
            from indicators.technical import TechnicalIndicators
            df = TechnicalIndicators.add_all_indicators(df)
        except:
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
    
    def update_all_positions(self):
        """Update all open positions with current prices"""
        current_prices = {}
        for position in self.paper_trader.get_open_positions():
            price, _ = self.fetcher.get_real_time_price(
                position['asset'], position['category']
            )
            if price:
                current_prices[position['asset']] = price
        
        self.paper_trader.update_positions(current_prices)
    
    def evaluate_signal(self, signal: Dict) -> Dict:
        """Evaluate signal with risk management"""
        market_status = MarketHours.get_status()
        if not market_status.get(signal.get('category', ''), False):
            return {'approved': False, 'reason': 'Market closed'}
        
        return {'approved': True, 'reason': 'OK'}
    
    def generate_report(self) -> Dict:
        """Generate comprehensive report"""
        return {
            'timestamp': datetime.now().isoformat(),
            'market_status': MarketHours.get_status(),
            'paper_trading': self.paper_trader.get_performance(),
            'current_strategy': self.current_strategy,
            'models': {
                'total': self.monitor.count_trained_models(),
                'latest': self.monitor.get_latest_report()
            }
        }
    
    def stop(self):
        """Stop trading"""
        print("\n Stopping system...")
        self.is_running = False
        self.update_all_positions()
        print(" System stopped")


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Ultimate Trading System')
    parser.add_argument('--mode', type=str, 
                       choices=['backtest', 'optimize', 'train', 'live', 'compare', 'batch-optimize'],
                       default='monitor', help='Operation mode')
    parser.add_argument('--asset', type=str, default='BTC-USD',
                       help='Asset for backtesting/optimization')
    parser.add_argument('--strategy', type=str, 
                       choices=['rsi', 'macd', 'bb', 'ma_cross', 'ml_ensemble'],
                       default='rsi', help='Strategy to optimize')
    parser.add_argument('--balance', type=float, default=10000,
                       help='Account balance in USD')
    parser.add_argument('--broker', type=str, choices=['alpaca', 'ib'], 
                       help='Connect to broker')
    parser.add_argument('--strategy-mode', type=str, 
                        choices=['strict', 'fast', 'balanced', 'voting'], 
                        default='balanced', 
                        help='Trading strategy mode: strict (fewer trades), fast (more trades), balanced (default), voting (all strategies combined)')
    parser.add_argument('--reset', action='store_true', 
                       help='Reset trade history before starting')
    parser.add_argument('--lookback', type=int, default=365,
                       help='Days of historical data for optimization')
    parser.add_argument('--assets', type=str, nargs='+',
                       help='Specific assets to optimize (space-separated)')
    parser.add_argument('--sessions', action='store_true',
                        help='Show session performance report')
    
    args = parser.parse_args()
    
    if args.reset:
        import os
        from datetime import datetime
        if os.path.exists('paper_trades.json'):
            backup_name = f'paper_trades_backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
            os.rename('paper_trades.json', backup_name)
            print(f"🔄 Trade history reset - backed up to {backup_name}")
        else:
            print("📭 No trade history found to reset")

    if args.sessions:
        system.show_session_report()
        return

    # Initialize system
    system = UltimateTradingSystem(
        account_balance=args.balance,
        strategy_mode=args.strategy_mode
    )
    
    # ===== BATCH OPTIMIZATION =====
    if args.mode == 'batch-optimize':
        print("\n" + "="*70)
        print("🚀 BATCH OPTIMIZATION MODE")
        print("="*70)
        print("This will optimize ALL 50+ strategies for multiple assets")
        print("⏱️  This can take several hours depending on the number of assets")
        print("💡 Recommended to run overnight or on a weekend\n")
        
        # Confirm with user
        response = input("Continue with batch optimization? (y/n): ").strip().lower()
        if response != 'y':
            print("❌ Batch optimization cancelled")
            return
        
        # Determine which assets to optimize
        if args.assets:
            assets_to_optimize = args.assets
            print(f"\n📊 Optimizing specified {len(assets_to_optimize)} assets: {', '.join(assets_to_optimize)}")
        else:
            # Get all assets from the system
            asset_list = system.get_asset_list()
            assets_to_optimize = [asset[0] for asset in asset_list]
            print(f"\n📊 Optimizing ALL {len(assets_to_optimize)} assets in the system")
        
        print(f"📈 Using {args.lookback} days of historical data")
        print(f"⏱️  Estimated time: ~{len(assets_to_optimize) * 5} minutes\n")
        
        # Second confirmation
        response2 = input(f"Final confirmation - start optimization now? (y/n): ").strip().lower()
        if response2 != 'y':
            print("❌ Batch optimization cancelled")
            return
        
        # Run the batch optimization
        try:
            results = system.batch_optimize_all_assets(
                assets=assets_to_optimize,
                lookback_days=args.lookback
            )
            
            print("\n" + "="*70)
            print("✅ BATCH OPTIMIZATION COMPLETE")
            print("="*70)
            print(f"📊 Successfully optimized {len(results)} assets")
            print(f"📁 Results saved in: optimization_results/")
            
            # Show top strategies across all assets
            if hasattr(system, 'create_master_optimization_report'):
                system.create_master_optimization_report(results)
            
            # Ask if user wants to apply optimized params
            print("\n" + "-"*50)
            response3 = input("Apply optimized parameters to trading strategies? (y/n): ").strip().lower()
            if response3 == 'y':
                system.apply_optimized_params_to_strategies()
                print("✅ Optimized parameters applied to all strategies")
            
        except KeyboardInterrupt:
            print("\n\n⚠️ Batch optimization interrupted by user")
            print("Partial results may have been saved")
        except Exception as e:
            print(f"\n❌ Error during batch optimization: {e}")
            import traceback
            traceback.print_exc()
    
    # ===== BACKTEST =====
    elif args.mode == 'backtest':
        print(f"\n📊 Backtesting {args.asset}...")
        system.backtest_asset(args.asset)
    
    # ===== OPTIMIZE SINGLE STRATEGY =====
    elif args.mode == 'optimize':
        print(f"\n🔧 Optimizing {args.strategy} for {args.asset}...")
        system.optimize_strategy(args.asset, args.strategy)
    
    # ===== TRAIN ML MODELS =====
    elif args.mode == 'train':
        from datetime import datetime
        from data.fetcher import MarketHours
    
        market_status = MarketHours.get_status()
        is_weekend = market_status['is_weekend']
    
        if is_weekend:
            print("\n" + "="*60)
            print(" WEEKEND MODE: Training only Crypto (24/7 markets)")
            print("="*60)
            print("   • Forex: CLOSED")
            print("   • Stocks: CLOSED") 
            print("   • Commodities: CLOSED")
            print("   • Indices: CLOSED\n")
        
            assets = [
                # Crypto only (works on weekends)
                'BTC-USD', 'ETH-USD', 'BNB-USD', 'SOL-USD', 'XRP-USD',
                'ADA-USD', 'DOGE-USD', 'DOT-USD', 'LTC-USD', 'AVAX-USD',
                'LINK-USD'
            ]
        else:
            print("\n" + "="*60)
            print(" WEEKDAY MODE: Training ALL assets")
            print("="*60)
            print("   • Crypto: OPEN")
            print("   • Forex: OPEN")
            print("   • Stocks: OPEN")
            print("   • Commodities: OPEN")
            print("   • Indices: OPEN\n")
        
            assets = [
                # Crypto
                'BTC-USD', 'ETH-USD', 'BNB-USD', 'SOL-USD', 'XRP-USD',
            
                # Forex
                'EUR/USD', 'GBP/USD', 'USD/JPY', 'AUD/USD', 'USD/CAD',
            
                # Stocks
                'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'NVDA', 'META',
            
                # Commodities
                'GC=F', 'SI=F', 'CL=F', 'NG=F',
            
                # Indices
                '^GSPC', '^DJI', '^IXIC', '^FTSE'
            ]
    
        print(f" Training {len(assets)} assets...")
        system.train_ml_models(assets)
    
    # ===== COMPARE STRATEGIES =====
    elif args.mode == 'compare':
        assets = [
            'BTC-USD', 'ETH-USD', 'BNB-USD', 'SOL-USD', 'XRP-USD',  # Crypto
            'EUR/USD', 'GBP/USD', 'USD/JPY', 'AUD/USD', 'USD/CAD',  # Forex
            'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'NVDA', 'META',  # Stocks
            'GC=F', 'SI=F', 'CL=F', 'NG=F',  # Commodities
            '^GSPC', '^DJI', '^IXIC', '^FTSE'  # Indices
        ]
        system.backtest_all_strategies(assets)
    
    # ===== LIVE TRADING =====
    elif args.mode == 'live':
        # Connect to broker if requested
        if args.broker == 'alpaca':
            api_key = input("Enter Alpaca API Key: ").strip()
            api_secret = input("Enter Alpaca Secret Key: ").strip()
            system.connect_alpaca(api_key, api_secret)
        elif args.broker == 'ib':
            system.connect_interactive_brokers()
        
        # Start live trading
        system.start_professional_trading()
        
        try:
            while True:
                time.sleep(10)
                if int(time.time()) % 60 < 1:
                    report = system.generate_report()
                    print(json.dumps(report, indent=2, default=str))
        except KeyboardInterrupt:
            system.stop()
    
    # ===== DEFAULT / HELP =====
    else:
        print("\n" + "="*60)
        print(" ULTIMATE TRADING SYSTEM - HELP")
        print("="*60)
        print("\nAvailable commands:")
        print("  --mode backtest       Backtest a single asset")
        print("  --mode optimize       Optimize a single strategy")
        print("  --mode train          Train ML models")
        print("  --mode compare        Compare all strategies")
        print("  --mode live           Start live trading")
        print("  --mode batch-optimize Run batch optimization for all assets")
        print("\nExamples:")
        print("  python trading_system.py --mode backtest --asset BTC-USD")
        print("  python trading_system.py --mode optimize --asset BTC-USD --strategy rsi")
        print("  python trading_system.py --mode batch-optimize")
        print("  python trading_system.py --mode batch-optimize --assets BTC-USD ETH-USD")
        print("  python trading_system.py --mode batch-optimize --lookback 180")
        print("  python trading_system.py --mode live --balance 100 --strategy-mode voting")


if __name__ == "__main__":
    main()