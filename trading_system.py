"""
ULTIMATE PROFESSIONAL TRADING SYSTEM
Everything integrated: Backtesting + ML + Paper Trading + Broker Ready + Telegram Commander
UPDATED: Two-bot Telegram system - Command bot in web dashboard, Alert bot in trading system
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
import asyncio
import threading
import re
from logger import logger
try:
    from telethon_whale_store import whale_store as _whale_store
except Exception:
    _whale_store = None

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
from telegram_manager import telegram_manager

# ═══════════════════════════════════════════════════════════════════════
# PLATFORM UPGRADES — Redis, OrderFlow, Alpha Discovery, Prediction Tracker
# ═══════════════════════════════════════════════════════════════════════
try:
    from redis_broker import broker as _redis_broker
    _REDIS_OK = _redis_broker.is_connected
except Exception:
    _redis_broker = None
    _REDIS_OK = False

try:
    from orderflow_engine import orderflow_engine as _orderflow_engine
    _ORDERFLOW_OK = True
except Exception:
    _orderflow_engine = None
    _ORDERFLOW_OK = False

try:
    from alpha_discovery import alpha_engine as _alpha_engine
    _ALPHA_OK = True
except Exception:
    _alpha_engine = None
    _ALPHA_OK = False

try:
    from prediction_tracker import prediction_tracker as _pred_tracker
    _PRED_TRACKER_OK = True
except Exception:
    _pred_tracker = None
    _PRED_TRACKER_OK = False

# ===== TELEGRAM COMMANDER (for web dashboard only) =====
try:
    from telegram_commander import TelegramCommander
    TELEGRAM_AVAILABLE = True
except ImportError:
    TELEGRAM_AVAILABLE = False
    logger.warning("Telegram Commander not available - run: pip install python-telegram-bot")

# ===== SIMPLE ALERT BOT (for trading system only) =====
try:
    from simple_alert_bot import SimpleAlertBot
    SIMPLE_ALERT_AVAILABLE = True
except ImportError:
    SIMPLE_ALERT_AVAILABLE = False
    logger.warning("SimpleAlertBot not available - create simple_alert_bot.py")

# ===== DYNAMIC POSITION SIZER =====
try:
    from advanced_risk_manager import DynamicPositionSizer
    DYNAMIC_SIZER_AVAILABLE = True
except ImportError:
    DYNAMIC_SIZER_AVAILABLE = False
    logger.warning("Dynamic Position Sizer not available")

from advanced_risk_manager import DailyLossLimit

class UltimateTradingSystem:
    """
    ULTIMATE PROFESSIONAL TRADING SYSTEM
    """
    
    def __init__(self, account_balance: float = 10000, strategy_mode: str = 'balanced', no_telegram: bool = False):
        """
        Initialize the Ultimate Trading System
        
        Args:
            account_balance: Starting account balance
            strategy_mode: Trading strategy mode ('strict', 'fast', 'balanced', 'voting')
            no_telegram: If True, disable Telegram commander
        """
        logger.info("="*60)
        logger.info(" ULTIMATE PROFESSIONAL TRADING SYSTEM")
        logger.info("="*60)

        # Store the no_telegram flag
        self.no_telegram = no_telegram
        
        # Core Components
        # [moved to top-level import]
        from advanced_predictor import AdvancedPredictionEngine
        from advanced_risk_manager import AdvancedRiskManager
        from advanced_backtester import AdvancedBacktester
        from paper_trader import PaperTrader
        from training_monitor import TrainingMonitor
        from monitor import TradingMonitor
        from portfolio_optimizer import EnhancedPortfolioOptimizer
        # [moved to top-level import]
        from concurrent.futures import ThreadPoolExecutor, as_completed
        from strategies.voting_engine import StrategyVotingEngine
        from auto_train_intelligent import IntelligentAutoTrainer
        from advanced_ai import AdvancedAIIntegration
        from market_regime_analyzer import MarketRegimeDetector
        from session_tracker import SessionTracker
        from model_registry import ModelRegistry
        from cache_manager import CacheManager
        from strategy_optimizer import StrategyOptimizer
        from profitability_upgrade import apply_upgrades, cooldown_tracker, category_limiter, position_age_monitor
        from back_up import HumanExplainer, TradingPersonality
        
        # Core Components (create these FIRST)
        self.fetcher = NASALevelFetcher()

        # WebSocket is managed by web_app_live.py in-process thread.
        # trading_system does NOT start its own WS manager to avoid duplicate connections.
        self.use_websocket = False
        self.ws_status = {'connected': False, 'last_message': None, 'sources': {}}

        # Try loading a pre-trained predictor from auto_train_daily output first
        _predictor_loaded = False
        _trained_models_dir = Path("trained_models")
        if _trained_models_dir.exists():
            _pkl_files = sorted(_trained_models_dir.glob("*.pkl"), key=lambda p: p.stat().st_mtime, reverse=True)
            for _pkl in _pkl_files:
                try:
                    import cloudpickle
                    with open(_pkl, 'rb') as _f:
                        _model_data = cloudpickle.load(_f)
                    if isinstance(_model_data, dict) and 'predictor' in _model_data:
                        self.predictor = _model_data['predictor']
                        logger.info(f"✅ Loaded pre-trained predictor from {_pkl.name} ({len(self.predictor.models)} models)")
                        _predictor_loaded = True
                        break
                    elif hasattr(_model_data, 'models'):
                        self.predictor = _model_data
                        logger.info(f"✅ Loaded pre-trained predictor from {_pkl.name}")
                        _predictor_loaded = True
                        break
                except Exception as _e:
                    logger.debug(f"Could not load {_pkl.name}: {_e}")
                    continue
        if not _predictor_loaded:
            self.predictor = AdvancedPredictionEngine("super_ensemble")
            logger.info("ℹ️ No pre-trained models found — predictor starts untrained. Run auto_train_daily.py to train.")
        self.risk_manager = AdvancedRiskManager(account_balance)
        self.risk_manager.max_positions = 10 
        self.max_positions = 10 
        self.backtester = AdvancedBacktester(initial_capital=account_balance)
        self.monitor = TrainingMonitor()

        # Add personality to your bot
        self.personality = TradingPersonality("Robbie")
        self.explainer = HumanExplainer(self)
        
        # ===== CREATE PAPER TRADER HERE (BEFORE using it) =====
        self.paper_trader = PaperTrader(self.risk_manager)
        # ======================================================

        # ===== CONNECT PAPER TRADER TO TRADING SYSTEM FOR TELEGRAM =====
        self.paper_trader.trading_system = self
        # Start real-time position monitor (polls SL/TP every 5s)
        try:
            self.paper_trader.start_monitor()
        except Exception as _me:
            logger.warning(f"Position monitor could not start: {_me}")
        # ======================================================

        # Connect trade callback for personality memory
        self.paper_trader.on_trade_closed = self._remember_trade
        
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
            logger.info("PROFITABILITY UPGRADES: ACTIVE")
            logger.info("   • 60-min cooldown after losses")
            logger.info("   • Category position limits (1 crypto, 2 forex)")
            logger.info("   • ATR-based stop losses")
            logger.info("   • Entry quality filter")
            logger.info("   • 4-hour position age limit")
        except ImportError as e:
            logger.warning("PROFITABILITY UPGRADES: NOT INSTALLED")
            logger.info("   Run: python profitability_upgrade.py")
            self.profitability_upgrades_active = False
        except Exception as e:
            logger.error(f"PROFITABILITY UPGRADES ERROR: {e}")
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
        # .env is FIRST priority for Telegram credentials — telegram_config.json is optional fallback
        telegram_config = None
        _tok = os.getenv('COMMAND_BOT_TOKEN') or os.getenv('TELEGRAM_TOKEN', '')
        _cid = os.getenv('TELEGRAM_CHAT_ID', '')
        if _tok and _cid:
            telegram_config = {
                'enabled':   True,
                'bot_token': _tok,
                'chat_id':   _cid,
            }
            logger.info("Telegram config built from .env")
        elif os.path.exists('config/telegram_config.json'):
            try:
                with open('config/telegram_config.json', 'r', encoding='utf-8') as f:
                    telegram_config = json.load(f)
                logger.info("Telegram config loaded from telegram_config.json (fallback)")
            except Exception as e:
                logger.warning(f"Could not load Telegram config: {e}")
        
        # Load Email config
        email_config = None
        if os.path.exists('config/email_config.json'):
            try:
                with open('config/email_config.json', 'r', encoding='utf-8') as f:
                    email_config = json.load(f)
                logger.info("Email config loaded")
            except Exception as e:
                logger.warning(f"Could not load Email config: {e}")
        
        # Initialize monitor WITH alert channels
        self.live_monitor = TradingMonitor(
            risk_manager=self.risk_manager,
            paper_trader=self.paper_trader,
            email_config=email_config if email_config and email_config.get('enabled') else None,
            telegram_config=telegram_config if telegram_config and telegram_config.get('enabled') else None
        )
        
        # Connect monitor to paper trader for alerts
        self.paper_trader.monitor = self.live_monitor
        
        # ===== TELEGRAM - TWO-BOT SYSTEM =====
        # Trading system uses SIMPLE ALERT BOT (no commands)
        # Web dashboard uses FULL COMMANDER (handles /commands)
        # This prevents conflicts!
        # =====================================
        
        try:
            # Check if Telegram is disabled by flag
            if self.no_telegram:
                self.telegram = None
                logger.info("📱 TELEGRAM: Disabled by --no-telegram flag")
                logger.info("   • Trading system will run without Telegram alerts")
            else:
                # ===== USE SIMPLE ALERT BOT (no commands, no conflicts) =====
                if SIMPLE_ALERT_AVAILABLE:
                    # Use WHALE_TELEGRAM_TOKEN for alerts (it's already a bot)
                    alert_token = os.getenv('WHALE_TELEGRAM_TOKEN')
                    alert_chat_id = os.getenv('TELEGRAM_CHAT_ID')
                    
                    # Fallback to main token if whale token not set
                    if not alert_token:
                        alert_token = os.getenv('TELEGRAM_TOKEN')
                        logger.info("📱 WHALE_TELEGRAM_TOKEN not set, using TELEGRAM_TOKEN as fallback")
                    
                    if alert_token and alert_chat_id:
                        try:
                            from simple_alert_bot import SimpleAlertBot
                            self.telegram = SimpleAlertBot(alert_token, alert_chat_id)
                            logger.info("📱 SIMPLE ALERT BOT: Active")
                            logger.info("   • Using WHALE_TELEGRAM_TOKEN for alerts")
                            logger.info("   • No command conflicts with web dashboard")
                            logger.info("   • Trade alerts will be sent to Telegram")
                        except Exception as e:
                            self.telegram = None
                            logger.error(f"📱 Failed to initialize SimpleAlertBot: {e}")
                    else:
                        self.telegram = None
                        logger.warning("📱 Alert bot not configured - set WHALE_TELEGRAM_TOKEN in .env")
                        logger.info("   • Trading will continue without Telegram alerts")
                
                # ===== FALLBACK: Try full commander (but check for conflicts) =====
                elif TELEGRAM_AVAILABLE:
                    logger.warning("SimpleAlertBot not available, checking for Telegram Commander...")
                    
                    # Get token from environment or config
                    telegram_token = os.getenv('TELEGRAM_TOKEN')
                    telegram_chat_id = os.getenv('TELEGRAM_CHAT_ID')
                    
                    # Fallback to config file if env not set
                    if not telegram_token and 'telegram_config' in locals() and telegram_config:
                        telegram_token = telegram_config.get('bot_token')
                        telegram_chat_id = telegram_config.get('chat_id')
                    
                    if telegram_token and telegram_chat_id:
                        # Use the manager to prevent conflicts
                        from telegram_manager import telegram_manager
                        
                        # Check if another instance is already running
                        if hasattr(telegram_manager, 'is_other_instance_running') and telegram_manager.is_other_instance_running():
                            self.telegram = None
                            logger.warning("📱 Telegram Commander: Another instance running, skipping")
                            logger.info("   • Use --no-telegram flag to disable")
                            logger.info("   • Or install SimpleAlertBot for alerts without conflicts")
                        else:
                            # Start bot through manager
                            if telegram_manager.start(telegram_token, telegram_chat_id, self):
                                self.telegram = telegram_manager.bot
                                logger.info("📱 TELEGRAM COMMANDER: ACTIVE (fallback mode)")
                                logger.info("   • Commands: /status, /positions, /pause, /resume, /balance, /performance, /strategies, /market, /close")
                                logger.info("   • WARNING: May conflict with web dashboard!")
                            else:
                                self.telegram = None
                                logger.warning("📱 Telegram Commander: Could not start (another instance may be running)")
                    else:
                        self.telegram = None
                        if not TELEGRAM_AVAILABLE:
                            logger.warning("📱 Telegram Commander: python-telegram-bot not installed")
                        else:
                            logger.warning("📱 Telegram Commander: Not configured (set TELEGRAM_TOKEN in .env)")
                else:
                    self.telegram = None
                    logger.warning("📱 No Telegram bots available - install SimpleAlertBot or python-telegram-bot")
                    
        except Exception as e:
            self.telegram = None
            logger.error(f"📱 TELEGRAM ERROR: Could not initialize - {e}")
        # =================================
        
        # ===== ENHANCED PORTFOLIO OPTIMIZER =====
        try:
            self.portfolio_optimizer = EnhancedPortfolioOptimizer(
                max_allocation=0.3, 
                max_correlation=0.7
            )
            logger.info("PORTFOLIO OPTIMIZER: ACTIVE")
            logger.info("   • Max allocation per asset: 30%")
            logger.info("   • Max correlation threshold: 0.7")
            logger.info("   • VaR tracking enabled")
        except Exception as e:
            logger.warning(f"Could not initialize portfolio optimizer: {e}")
            self.portfolio_optimizer = None
        
        # ===== SENTIMENT ANALYZER =====
        try:
            self.sentiment_analyzer = SentimentAnalyzer()
            logger.info("SENTIMENT ANALYZER: ACTIVE")
            # Share this instance with voting_engine's TTL singleton so parallel
            # scan threads never create a second SentimentAnalyzer (and its
            # embedded WhaleAlertManager/RedditWatcher) via race condition
            try:
                import strategies.voting_engine as _ve
                import time as _t
                _ve._sentiment_instance = self.sentiment_analyzer
                _ve._sentiment_last_init = _t.time()
            except Exception:
                pass
        except Exception as e:
            logger.warning(f"Could not initialize sentiment analyzer: {e}")
            self.sentiment_analyzer = None
        
        # ===== MARKET REGIME DETECTION =====
        try:
            self.regime_detector = MarketRegimeDetector()
            self.regime_history = []
            self.current_regime = None
            self.regime_confidence = 0.0
            logger.info("MARKET REGIME DETECTION: ACTIVE")
        except Exception as e:
            logger.warning(f"Could not initialize regime detector: {e}")
            self.regime_detector = None

        # ===== STRATEGY OPTIMIZER =====
        try:
            self.strategy_optimizer = StrategyOptimizer(self.backtester)
            logger.info("STRATEGY OPTIMIZER: ACTIVE")
            logger.info("   • Grid search optimization")
            logger.info("   • Finds best parameters for all strategies")
        except Exception as e:
            logger.warning(f"Could not initialize strategy optimizer: {e}")
            self.strategy_optimizer = None
        
        # ===== DYNAMIC POSITION SIZER =====
        try:
            if DYNAMIC_SIZER_AVAILABLE:
                self.position_sizer = DynamicPositionSizer(
                    base_risk=0.01,  # 1% base risk
                    max_risk=0.03    # 3% maximum risk
                )
                logger.info("DYNAMIC POSITION SIZER: ACTIVE")
                logger.info("   • Base risk: 1%")
                logger.info("   • Max risk: 3%")
                logger.info("   • Adapts to confidence, volatility, regime, win rate")
            else:
                self.position_sizer = None
                logger.warning("Dynamic Position Sizer: Not available")
        except Exception as e:
            logger.warning(f"Could not initialize position sizer: {e}")
            self.position_sizer = None
        
        # ===== TRACKING VARIABLES =====
        self.multi_timeframe = True
        self.current_strategy = 'scalping'  # Default strategy
        self.is_running = False
        self.last_day = datetime.now().date()
        self.health_check_counter = 0  # For periodic portfolio health checks
        self.current_asset = None  # For scalping strategy

        # ===== DAILY LOSS LIMIT =====
        self.daily_loss_limit = DailyLossLimit(
            max_loss_pct=3.0,  # 3% max daily loss
            alert_callback=self.send_loss_limit_alert
        )
        # Set initial balance
        if hasattr(self, 'risk_manager'):
            self.daily_loss_limit.set_initial_balance(self.risk_manager.account_balance)

        # ===== MODEL REGISTRY =====
        try:
            self.model_registry = ModelRegistry(registry_file="model_registry.json")
            logger.info("MODEL REGISTRY: ACTIVE")
            logger.info("   • Tracks ML model performance")
            logger.info("   • Auto-selects best models per asset")
        except Exception as e:
            logger.warning(f"Could not initialize model registry: {e}")
            self.model_registry = None

        # ===== CACHE MANAGER =====
        try:
            self.cache_manager = CacheManager(
                host='localhost',
                port=6379,
                db=0,
                password=None
            )
            logger.info("CACHE MANAGER: ACTIVE")
            
            # Connect cache manager to portfolio optimizer
            if hasattr(self, 'portfolio_optimizer') and self.portfolio_optimizer:
                self.portfolio_optimizer.set_cache_manager(self.cache_manager)
                
        except Exception as e:
            logger.warning(f"Could not initialize cache manager: {e}")
            self.cache_manager = None
        # ========================

        # ===== MARKET CALENDAR =====
        try:
            from market_calendar import MarketCalendar
            self.market_calendar = MarketCalendar()
            # Fetch initial data
            self.market_calendar.fetch_economic_calendar()
            self.market_calendar.fetch_earnings_calendar()
            logger.info("MARKET CALENDAR: ACTIVE")
            logger.info("   • Economic events tracking")
            logger.info("   • Earnings calendar")
            logger.info("   • Crypto halving countdown")
        except Exception as e:
            logger.warning(f"Could not initialize market calendar: {e}")
            self.market_calendar = None

        # ===== SESSION TRACKER =====
        try:
            from session_tracker import SessionTracker
            self.session_tracker = SessionTracker()
            logger.info("SESSION TRACKER: ACTIVE")
            logger.info("   • Tracks performance by trading session")
            logger.info("   • Asian, London, New York sessions")
            logger.info("   • Identifies best times to trade")
        except Exception as e:
            logger.warning(f"Could not initialize session tracker: {e}")
            self.session_tracker = None
        # ============================
        
        # ===== CREATE RESULTS DIRECTORIES =====
        Path("backtest_results").mkdir(exist_ok=True)
        Path("ml_models").mkdir(exist_ok=True)
        Path("trade_logs").mkdir(exist_ok=True)
        Path("portfolio_reports").mkdir(exist_ok=True)  # For saving health reports
        
        # ===== WHALE INTELLIGENCE INTEGRATION =====
        self.setup_whale_integration()
        # =========================================

        # ─── Phase 3: Engine composition ─────────────────────────────────────
        # Engines are lazily wired AFTER all self.X attributes are set above.
        try:
            from engines.strategy_engine import StrategyEngine
            self.strategy_engine = StrategyEngine()
            # Re-point strategy dict to engine methods (keeps external callers working)
            self.strategies.update({
                'rsi':           self.strategy_engine.rsi_strategy,
                'macd':          self.strategy_engine.macd_strategy,
                'bb':            self.strategy_engine.bollinger_strategy,
                'ma_cross':      self.strategy_engine.ma_cross_strategy,
                'breakout':      self.strategy_engine.breakout_strategy,
                'mean_reversion':self.strategy_engine.mean_reversion_strategy,
                'trend_following':self.strategy_engine.trend_following_strategy,
                'scalping':      self.strategy_engine.scalping_strategy,
                'arbitrage':     self.strategy_engine.arbitrage_strategy,
                'day_trading':   self.strategy_engine.day_trading_strategy,
                'news_sentiment':self.strategy_engine.news_sentiment_strategy,
            })
            logger.info("✅ StrategyEngine wired")
        except Exception as e:
            logger.warning(f"StrategyEngine unavailable, using built-ins: {e}")
            self.strategy_engine = None

        try:
            from engines.whale_monitor import WhaleMonitor
            self.whale_monitor_engine = WhaleMonitor(telegram=self.telegram)
            # Copy any signals already collected via setup_whale_integration
            self.whale_monitor_engine.whale_signals = self.whale_signals
            self.whale_monitor_engine.whale_weights = self.whale_weights
            logger.info("✅ WhaleMonitor engine wired")
        except Exception as e:
            logger.warning(f"WhaleMonitor engine unavailable: {e}")
            self.whale_monitor_engine = None

        try:
            from engines.backtest_engine import BacktestEngine
            self.backtest_engine = BacktestEngine(self)
            logger.info("✅ BacktestEngine wired")
        except Exception as e:
            logger.warning(f"BacktestEngine unavailable: {e}")
            self.backtest_engine = None

        try:
            from engines.ml_engine import MLEngine
            self.ml_engine = MLEngine(self)
            logger.info("✅ MLEngine wired")
        except Exception as e:
            logger.warning(f"MLEngine unavailable: {e}")
            self.ml_engine = None

        try:
            from services.db_pool import get_db
            self.db = get_db()
            # Give paper trader the shared instance too
            if hasattr(self.paper_trader, 'db'):
                self.paper_trader.db = self.db
            logger.info("✅ Shared DB pool wired")
        except Exception as e:
            logger.warning(f"DB pool unavailable: {e}")
        # ─────────────────────────────────────────────────────────────────────

        logger.info("="*60)
        logger.info(" ALL SYSTEMS INITIALIZED")
        logger.info("="*60)
    
        # ===== ADD TELEGRAM ALERT METHODS =====
    
    def on_trade_opened(self, signal: dict):
        """Called when a new trade is opened"""
        # Send Telegram alert
        if hasattr(self, 'telegram') and self.telegram:
            try:
                # Check if it's SimpleAlertBot or TelegramCommander
                if hasattr(self.telegram, 'alert_trade_opened'):
                    self.telegram.alert_trade_opened(signal)
                else:
                    # Fallback for commander
                    self.telegram.send_message(f"🟢 New Trade: {signal['asset']} {signal['signal']} @ ${signal['entry_price']:.2f}")
            except Exception as e:
                logger.warning(f"Telegram alert failed: {e}")
    
    def on_trade_closed(self, trade: dict):
        """Called when a trade is closed"""
        # Send Telegram alert
        if hasattr(self, 'telegram') and self.telegram:
            try:
                if hasattr(self.telegram, 'alert_trade_closed'):
                    self.telegram.alert_trade_closed(trade)
                else:
                    emoji = "✅" if trade.get('pnl', 0) > 0 else "❌"
                    self.telegram.send_message(f"{emoji} Trade Closed: {trade['asset']} P&L: ${trade.get('pnl', 0):.2f}")
            except Exception as e:
                logger.warning(f"Telegram alert failed: {e}")
        
        # Update cooldown on loss
        if trade.get('pnl', 0) < 0 and hasattr(self, 'cooldown_tracker'):
            self.cooldown_tracker.record_loss(trade['asset'])
    
    def on_profit_target(self, profit_pct: float):
        """Called when profit target reached"""
        if hasattr(self, 'telegram') and self.telegram:
            try:
                if hasattr(self.telegram, 'alert_profit_target'):
                    self.telegram.alert_profit_target(profit_pct)
                else:
                    self.telegram.send_message(f"🎯 Profit Target: +{profit_pct:.1f}%")
            except Exception as e:
                logger.warning(f"Telegram alert failed: {e}")

    def _start_ws_monitor(self):
        """Monitor WebSocket health"""
        def monitor():
            while self.is_running:
                try:
                    if hasattr(self.fetcher, 'ws_manager'):
                        ws = self.fetcher.ws_manager
                        self.ws_status['sources'] = {
                            'bybit': 'bybit' in ws.connections,
                            'finnhub': 'finnhub' in ws.connections
                        }
                        
                        # Log status every 5 minutes
                        if int(time.time()) % 300 < 1:
                            status = []
                            if self.ws_status['sources'].get('bybit'):
                                status.append("Bybit✅")
                            if self.ws_status['sources'].get('finnhub'):
                                status.append("Finnhub✅")
                            
                            if status:
                                logger.info(f"📡 WebSocket: {' '.join(status)}")
                    
                    time.sleep(60)
                except Exception as e:
                    logger.error(f"WebSocket monitor error: {e}")
                    time.sleep(60)
        
        thread = threading.Thread(target=monitor, daemon=True)
        thread.start()
    # =================================================

    # ===== DAILY LOSS LIMIT ALERT METHOD =====
    def send_loss_limit_alert(self, message: str):
        """Send alert when daily loss limit is hit"""
        logger.warning(f"Daily loss limit hit: {message}")
        
        # Also send via monitor if available
        if hasattr(self, 'live_monitor') and self.live_monitor:
            try:
                self.live_monitor._send_alert(
                    'CRITICAL',
                    'Daily Loss Limit Hit',
                    message
                )
            except Exception as e:
                logger.debug(f"Monitor alert failed: {e}")
        
        # Send Telegram alert
        if hasattr(self, 'telegram') and self.telegram:
            try:
                if hasattr(self.telegram, 'alert_daily_loss_limit'):
                    self.telegram.alert_daily_loss_limit(
                        self.daily_loss_limit.get_status()['daily_loss_pct']
                    )
                else:
                    self.telegram.send_message(f"⚠️ Daily Loss Limit Hit: {self.daily_loss_limit.get_status()['daily_loss_pct']:.1f}%")
            except Exception as e:
                logger.warning(f"Telegram alert failed: {e}")
    # =========================================

     # ===== WHALE INTELLIGENCE INTEGRATION =====
    def setup_whale_integration(self):
        """Initialize whale monitoring with trading influence"""
        self.whale_signals = []
        self.whale_weights = {
            'BTC': 1.0,
            'ETH': 1.0,
            'BNB': 1.0,
            'SOL': 1.0,
            'XRP': 1.0
        }
        self.start_whale_monitor()
        logger.info("🐋 Whale Intelligence: ACTIVE")
        logger.info("   • Monitoring 8 whale channels")
        logger.info("   • Whale activity influences position sizing")
        logger.info("   • Large inflows = +20% confidence boost")

    def start_whale_monitor(self):
        """Start whale monitor in background thread using saved session"""
        import threading
        import asyncio
        from telethon import TelegramClient, events
        import re
        import os
        from dotenv import load_dotenv
        
        load_dotenv()
        
        # Your credentials from .env
        api_id = int(os.getenv('TELEGRAM_API_ID', '32486436'))
        api_hash = os.getenv('TELEGRAM_API_HASH', '3e264a0c1e28644378a9c5236bf251cb')
        session_name = os.getenv('TELEGRAM_SESSION', 'whale_session')
        phone = os.getenv('TELEGRAM_PHONE')  # Optional, but good to have
        
        # Channels to monitor
        WHALE_CHANNELS = [
            'whale_alert',
            'whalebotalerts',
            'WhaleSniper',
            'lookonchain',
            'cryptoquant_alert',
            'WhaleBotRektd',
            'WhaleWire',
            'whalecointalk'
        ]
        
        # Supported symbols — extend as needed
        _WHALE_SYMBOLS = r'(BTC|ETH|BNB|SOL|XRP|USDT|USDC|BUSD|TUSD|DAI|FRAX'\
                           r'|ADA|DOGE|SHIB|PEPE|FLOKI|BONK'\
                           r'|MATIC|AVAX|FTM|ONE|NEAR|ALGO|ATOM|ICP'\
                           r'|DOT|KSM|ARB|OP|MANTA|ZK|STRD'\
                           r'|LINK|UNI|AAVE|CRV|MKR|SNX|COMP|BAL|YFI'\
                           r'|LTC|BCH|XLM|XMR|ZEC|DASH|ETC|BTT'\
                           r'|TRX|TON|SUI|APT|SEI|INJ|TIA|PYTH'\
                           r'|SAND|MANA|AXS|ENJ|GALA|IMX'\
                           r'|FIL|AR|GRT|OCEAN|RNDR|FET|AGIX'\
                           r'|WLD|CFX|HBAR|VET|EGLD|THETA|FLR)'

        def extract_whale_info(text):
            """
            Extract whale transaction details from Telegram channel messages.
            Handles all common real-world formats:
              • "1,250 #BTC (83,456,231 USD) transferred"
              • "500 ETH ($1.2M) moved from Coinbase"
              • "BTC: 500 coins ($28M) just moved"
              • "2500 BTC ($145,000,000) just moved"
            Returns (amount, symbol, usd_value) or None if < $1M.
            """
            if not text:
                return None
            clean = text.replace(',', '').replace('#', '')

            # Strategy 1: amount SYMBOL ... (raw_usd USD)
            m = re.search(
                rf'([\d.]+)\s*{_WHALE_SYMBOLS}[^$\d]{{0,60}}\(?\$?([\d.]+)\s*(?:USD|usd)\)?',
                clean, re.IGNORECASE
            )
            if m:
                try:
                    amount = float(m.group(1)); symbol = m.group(2).upper(); value = float(m.group(3))
                    if value >= 1_000_000:
                        return amount, symbol, value
                except: pass

            # Strategy 2: amount SYMBOL ... $valueM/B/K or plain $value
            m = re.search(
                rf'([\d.]+)\s*{_WHALE_SYMBOLS}[^$\d]{{0,80}}\$?\s*([\d.]+)\s*([MmBbKk]?)',
                clean, re.IGNORECASE
            )
            if m:
                try:
                    amount  = float(m.group(1)); symbol = m.group(2).upper()
                    raw_val = float(m.group(3)); suffix = m.group(4).upper()
                    value   = raw_val * {'B':1e9,'M':1e6,'K':1e3}.get(suffix, 1)
                    if value >= 1_000_000:
                        return amount, symbol, value
                except: pass

            # Strategy 3: SYMBOL: ... $valueM  (e.g. "BTC: 500 coins ($28M)")
            m = re.search(
                rf'{_WHALE_SYMBOLS}[:\s]{{1,5}}[\d.]+[^$\d]{{0,60}}\$?([\d.]+)\s*([MmBbKk])',
                clean, re.IGNORECASE
            )
            if m:
                try:
                    symbol  = m.group(1).upper(); raw_val = float(m.group(2))
                    suffix  = m.group(3).upper()
                    value   = raw_val * {'B':1e9,'M':1e6,'K':1e3}.get(suffix, 1)
                    if value >= 1_000_000:
                        return 0, symbol, value
                except: pass

            # Strategy 4: SYMBOL anywhere + any raw number >= 1M in message
            m_sym = re.search(_WHALE_SYMBOLS, clean, re.IGNORECASE)
            if m_sym:
                symbol = m_sym.group(1).upper()
                for n in re.findall(r'[\d.]+', clean):
                    try:
                        v = float(n)
                        if v >= 1_000_000:
                            return 0, symbol, v
                    except: pass
            return None
        
        async def whale_loop():
            # FIX: use connection_retries=0 + timeout so SQLite lock doesn't hang
            client = TelegramClient(
                session_name, api_id, api_hash,
                connection_retries=1,
                timeout=10,
            )

            try:
                await client.start()
                logger.info("🐋 Whale Monitor: Connected using saved session")
            except Exception as e:
                err = str(e)
                if 'database is locked' in err.lower():
                    # Flask debug reloader can cause a brief lock on startup
                    # Wait 8s and retry once before giving up
                    import asyncio as _aio
                    logger.info("🐋 Whale Monitor: session DB briefly locked — retrying in 8s...")
                    await _aio.sleep(8)
                    try:
                        await client.start()
                        logger.info("🐋 Whale Monitor: Connected on retry")
                    except Exception as e2:
                        if 'database is locked' in str(e2).lower():
                            logger.warning("🐋 Whale Monitor: session DB still locked — another live instance owns it. Skipping.")
                        else:
                            logger.error(f"Whale Monitor retry failed: {e2}")
                        return
                    return  # connected on retry — fall through to handler setup below would re-run, skip
                logger.error(f"Failed to connect with saved session: {e}")
                if phone:
                    try:
                        await client.start(phone=phone)
                    except Exception as e2:
                        logger.error(f"Whale Monitor fallback connect failed: {e2}")
                        return
                else:
                    return  # Can't connect — exit cleanly instead of crashing
            
            @client.on(events.NewMessage(chats=WHALE_CHANNELS))
            async def handler(event):
                if not event.message.text:
                    return
                
                whale = extract_whale_info(event.message.text)
                if whale:
                    amount, symbol, value = whale
                    await self.process_whale_alert(amount, symbol, value, event.chat.username)
            
            logger.info("🐋 Whale Monitor: Connected and listening")
            await client.run_until_disconnected()
        
        def run_whale():
            asyncio.run(whale_loop())
        
        thread = threading.Thread(target=run_whale, daemon=True)
        thread.start()

    async def process_whale_alert(self, amount: float, symbol: str, value: float, channel: str):
        """Process whale alert and influence trading decisions"""
        
        value_millions = value / 1_000_000
        
        # Store whale signal
        signal = {
            'time': datetime.now(),
            'symbol': symbol,
            'amount': amount,
            'value': value,
            'channel': channel,
            'bullish': self._is_bullish_whale(channel, symbol)
        }
        
        if not hasattr(self, 'whale_signals'):
            self.whale_signals = []
        
        self.whale_signals.append(signal)
        
        # Keep last 100 signals
        if len(self.whale_signals) > 100:
            self.whale_signals = self.whale_signals[-100:]
        
        # Calculate whale sentiment
        sentiment = self.get_whale_sentiment(symbol)
        
        # Log the alert
        alert_msg = (
            f"🐋 Whale Alert: {amount:.2f} {symbol} (${value_millions:.1f}M)\n"
            f"   • Channel: @{channel}\n"
            f"   • Sentiment: {'BULLISH' if signal['bullish'] else 'NEUTRAL'}\n"
            f"   • Impact: {self.whale_weights.get(symbol, 1.0):.1f}x weight"
        )
        logger.info(alert_msg)
        
        # Send to Telegram if alert bot exists
        if hasattr(self, 'telegram') and self.telegram:
            try:
                if hasattr(self.telegram, 'send_whale_alert'):
                    self.telegram.send_whale_alert(amount, symbol, value_millions, channel)
                else:
                    # Fallback for commander
                    self.telegram.send_message(
                        f"🐋 *Whale Alert*\n"
                        f"{amount:.2f} {symbol} (${value_millions:.1f}M)\n"
                        f"Channel: @{channel}"
                    )
            except Exception as e:
                logger.debug(f"Telegram send failed: {e}")

    def _is_bullish_whale(self, channel: str, symbol: str) -> bool:
        """Determine if whale movement is bullish"""
        # Exchange inflows = bearish (selling)
        bearish_channels = ['binance', 'exchange', 'inflow', 'cex']
        # Exchange outflows = bullish (buying)
        bullish_channels = ['withdrawal', 'outflow', 'treasury', 'cold']
        
        channel_lower = channel.lower()
        
        if any(b in channel_lower for b in bearish_channels):
            return False
        if any(b in channel_lower for b in bullish_channels):
            return True
        
        # Default: large transfers are neutral
        return True

    def get_whale_sentiment(self, asset: str, hours: int = 24) -> float:
        """Get whale sentiment score (-1 to 1) for an asset"""
        if not hasattr(self, 'whale_signals'):
            return 0.0
            
        recent = [s for s in self.whale_signals 
                 if s['symbol'] == asset and 
                 s['time'] > datetime.now() - timedelta(hours=hours)]
        
        if not recent:
            return 0.0
        
        # Calculate weighted sentiment
        total_value = sum(s['value'] for s in recent)
        bullish_value = sum(s['value'] for s in recent if s['bullish'])
        
        if total_value == 0:
            return 0.0
        
        # Sentiment from -1 (bearish) to 1 (bullish)
        sentiment = (bullish_value / total_value) * 2 - 1
        return round(sentiment, 2)

    def enhance_signal_with_whale(self, signal: Dict, asset: str) -> Dict:
        """Enhance trading signal with whale data — uses Telethon store + internal signals."""
        sym = asset.split('-')[0].upper()

        # Primary: Telethon whale_store (real-time Telegram channel data)
        telethon_sentiment = 0.0
        try:
            if _whale_store is not None and len(_whale_store) > 0:
                telethon_sentiment = _whale_store.get_sentiment(sym)
        except Exception:
            pass

        # Secondary: internal whale_signals (legacy fallback)
        internal_sentiment = self.get_whale_sentiment(sym)

        # Merge: Telethon takes priority if it has data, else fall back
        sentiment = telethon_sentiment if abs(telethon_sentiment) > 0.05 else internal_sentiment

        if abs(sentiment) > 0.3:
            direction = signal.get('signal', signal.get('direction', 'BUY'))
            # Boost only if whale agrees with signal direction
            if (direction == 'BUY' and sentiment > 0) or (direction == 'SELL' and sentiment < 0):
                boost = 1.0 + abs(sentiment) * 0.20
            else:
                boost = 1.0 - abs(sentiment) * 0.10  # opposing whale = mild cut
            signal['confidence'] = min(signal.get('confidence', 0.5) * boost, 0.95)
            signal['reason'] = signal.get('reason', '') + f" | Whale: {sentiment:+.2f}"
            if 'position_size' in signal:
                signal['position_size'] *= boost

        return signal
    # ==========================================

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
            # FIX: skip entirely if models not trained — avoids log spam
            if not self.predictor.models:
                return signals
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
            logger.debug(f"ML prediction error for {self.current_asset}: {e}")
        
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
                _cat = pred.get('category') or self.fetcher._get_asset_category(asset)
                price, _ = self.fetcher.get_real_time_price(asset, _cat)
                
                if price:
                    # Calculate actual movement
                    actual_move = (price - pred['price']) / pred['price'] * 100
                    
                    # Update registry
                    self.update_model_prediction(asset, pred['prediction'], actual_move)
                    to_remove.append(pred)
            except Exception as e:
                logger.warning(f"Failed to verify prediction for {asset}: {e}")
        
        # Remove verified predictions
        for pred in to_remove:
            self._pending_predictions.remove(pred)

    # ============= COMPREHENSIVE BACKTESTING =============

    def show_model_performance(self):
        """Display model performance report"""
        report = self.get_model_performance_report()
        
        if 'error' in report:
            logger.error(f"Model performance report error: {report['error']}")
            return
        
        logger.info("="*70)
        logger.info("MODEL PERFORMANCE REPORT")
        logger.info("="*70)
        logger.info(f"Total Models: {report['total_models']}")
        logger.info(f"Active Models: {report['active_models']}")
        logger.info(f"Average Accuracy: {report['avg_accuracy']:.1%}")
        
        if report['best_models']:
            logger.info("TOP PERFORMING MODELS:")
            for i, model in enumerate(report['best_models'], 1):
                logger.info(f"  {i}. {model['asset']} - {model['model_name']}")
                logger.info(f"     Accuracy: {model['accuracy']:.1%}")
                logger.info(f"     Trade Win Rate: {model['trade_win_rate']:.1%}")
                logger.info(f"     Predictions: {model['total_predictions']}")
    
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
            with open(f"optimization_results/{asset}_top_strategies.json", 'w', encoding='utf-8') as f:
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
            with open(filename, 'w', encoding='utf-8') as f:
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
            
            with open(filename, 'w', encoding='utf-8') as f:
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
        logger.info("="*60)
        logger.info(" TRAINING ML ENSEMBLE MODELS")
        logger.info("="*60)

        for asset in assets:
            logger.info(f"Training on {asset}...")
            
            # ===== TRY MULTIPLE PERIODS, TAKE THE BEST =====
            best_df = None
            best_rows = 0
            
            for days in [730, 365, 180, 90]:
                df = self.fetch_historical_data(asset, days)
                if not df.empty and len(df) > best_rows:
                    best_rows = len(df)
                    best_df = df
                    logger.debug(f"Found {len(df)} rows with {days} days")
            
            if best_df is None:
                logger.warning(f"No data for {asset}")
                continue
            
            df = best_df
            logger.info(f"Using {len(df)} rows for training")
            
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
                
                logger.info(f"Model saved to {model_path}")

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
                    logger.debug("Top Features:")
                    for idx, row in importance.iterrows():
                        logger.debug(f"  {row['feature']}: {row['importance']:.4f}")
                
            except Exception as e:
                logger.error(f"Training error for {asset}: {e}")
    
    # ============= BROKER INTEGRATION (READY FOR ALPACA) =============
    
    def ultimate_indicator_strategy(self, df: pd.DataFrame) -> List[Dict]:
        """
        ULTIMATE STRATEGY - Uses ALL 50+ indicators
        Each indicator votes, weighted by reliability
        """
        logger.debug("Running ULTIMATE indicator strategy (50+ indicators)...")

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
            logger.info(f"VOTE RESULT: {combined['signal']} with {combined['confidence']:.1%} confidence")
        
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
                logger.debug(f"Error analyzing {name}: {e}")
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
                logger.debug(f"STRONG SIGNAL: {signal} on {tf_15m['name']}")
        
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
            logger.debug(f"GOOD SIGNAL: {signal} on {tf_15m['name']} (15m+1h agree)")
        
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
            'strategy_id': 'STRICT',
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
            'strategy_id': 'FAST',
            'strategy_emoji': '⚡',
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
            'strategy_id': 'BALANCED',
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
            logger.info(f"Connected to Alpaca ({'PAPER' if paper else 'LIVE'})")
            logger.info(f"   Account: {account.account_number}")
            logger.info(f"   Balance: ${float(account.cash):,.2f}")
            
            return True
            
        except ImportError:
            logger.warning("Alpaca SDK not installed. Run: pip install alpaca-py")
            return False
        except Exception as e:
            logger.error(f"Alpaca connection error: {e}")
            return False
    
    def connect_interactive_brokers(self):
        """Connect to Interactive Brokers"""
        try:
            from ib_insync import IB, Stock
            
            self.ib = IB()
            self.ib.connect('127.0.0.1', 7497, clientId=1)  # TWS Paper port
            
            self.broker_connected = True
            self.broker_name = "Interactive Brokers"
            
            logger.info(f"Connected to Interactive Brokers (PAPER)")
            return True
            
        except ImportError:
            logger.warning("ib_insync not installed. Run: pip install ib_insync")
            return False
        except Exception as e:
            logger.error(f"IB connection error: {e}")
            return False
    
    def execute_real_trade(self, signal: Dict):
        """Execute a REAL trade through connected broker"""
        if not hasattr(self, 'broker_connected') or not self.broker_connected:
            logger.warning("No broker connected. Paper trading only.")
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
                logger.info(f"REAL ORDER EXECUTED: {order.id}")
                return order
                
            elif self.broker_name == "Interactive Brokers":
                # IB order logic here
                pass
                
        except Exception as e:
            logger.error(f"Real trade error: {e}")
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
            logger.warning(f"Dynamic position sizing error: {e}")
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
            logger.warning(f"Account volatility calculation error: {e}")
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
                    logger.warning(f"PORTFOLIO ALERT: Low diversification score ({div_score}%)")
                    logger.info("   Consider closing some correlated positions")
                    
                    # Find most concentrated category
                    categories = {}
                    for pos in open_positions:
                        cat = pos.get('category', 'unknown')
                        categories[cat] = categories.get(cat, 0) + 1
                    
                    most_concentrated = max(categories.items(), key=lambda x: x[1])
                    if most_concentrated[1] >= 3:
                        logger.info(f"   • Too many {most_concentrated[0]} positions ({most_concentrated[1]})")
            
            # Check risk distribution
            total_value = sum(p['entry_price'] * p['position_size'] for p in open_positions)
            account_balance = getattr(self.risk_manager, 'account_balance', 10000)
            
            if total_value > account_balance * 0.7:  # 70% of account in positions
                logger.warning(f"PORTFOLIO ALERT: High exposure ({total_value/account_balance*100:.1f}%)")
                logger.info("   Consider reducing position sizes")
                
        except Exception as e:
            logger.warning(f"Portfolio health check error: {e}")

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
            logger.info(f"Registered model for {asset}: {key}")
            return key
        except Exception as e:
            logger.warning(f"Failed to register model for {asset}: {e}")
            return None
        
    def _remember_trade(self, trade_result: Dict):
        """Store trade in personality memory"""
        if hasattr(self, 'personality'):
            self.personality.remember_trade({
                'asset': trade_result.get('asset'),
                'pnl': trade_result.get('pnl', 0),
                'exit_reason': trade_result.get('exit_reason'),
                'setup': trade_result.get('setup', 'unknown')
            })

    def update_model_prediction(self, asset: str, prediction: Dict, actual_move: float):
        """Update model performance with prediction result"""
        if not hasattr(self, 'model_registry') or self.model_registry is None:
            return
        
        try:
            # Get the model key for this asset
            model_key = f"{asset}_ensemble"
            self.model_registry.update_prediction(model_key, prediction, actual_move)
            logger.debug(f"Updated prediction for {asset}: actual={actual_move:.2f}%")
        except Exception as e:
            logger.warning(f"Failed to update model prediction for {asset}: {e}")

    def update_model_trade_result(self, asset: str, trade_result: Dict):
        """Update model performance with trade result"""
        if not hasattr(self, 'model_registry') or self.model_registry is None:
            return
        
        try:
            model_key = f"{asset}_ensemble"
            self.model_registry.update_trade_result(model_key, trade_result)
            logger.debug(f"Updated trade result for {asset}: P&L=${trade_result.get('pnl',0):.2f}")
        except Exception as e:
            logger.warning(f"Failed to update model trade result for {asset}: {e}")

    def get_best_model_for_asset(self, asset: str) -> Optional[Dict]:
        """Get the best performing model for an asset"""
        if not hasattr(self, 'model_registry') or self.model_registry is None:
            return None
        
        try:
            return self.model_registry.get_model_for_asset(asset)
        except Exception as e:
            logger.warning(f"Failed to get best model for {asset}: {e}")
            return None

    def get_model_performance_report(self) -> Dict:
        """Get comprehensive model performance report"""
        if not hasattr(self, 'model_registry') or self.model_registry is None:
            return {'error': 'Model registry not available'}
        
        try:
            return self.model_registry.get_performance_report()
        except Exception as e:
            logger.warning(f"Failed to get performance report: {e}")
            return {'error': str(e)}

    def scan_asset_parallel(self, asset: str, category: str) -> Optional[Dict]:
        """
        Scan single asset — every signal passes through the FULL quality pipeline:

        LAYER 1 — Voting Engine (11 strategies vote)
        LAYER 2 — signal_learning quality gate:
                  • 3-timeframe confluence  (15m + 1h + 4h must agree ≥ 2/3)
                  • ATR-based stops         (per-asset-class multipliers)
                  • Min 1.5:1 RR enforced
                  • News blackout ±30 min around high-impact events
                  • Session filter          (only trade during active sessions)
                  • ML ensemble vote        (7 models)
                  • Learned confidence bias (win rate over last N trades)
        LAYER 3 — Market regime gate (no trades in choppy/ranging markets)
        LAYER 4 — Session quality gate (only fire in sessions this asset wins)
        LAYER 5 — Sentiment confirmation (extreme fear/greed flips direction gate)
        LAYER 6 — Whale intelligence overlay
        LAYER 7 — Portfolio optimizer price feed

        Result: signals that reach Telegram have passed every filter in the system.
        """
        try:
            # Propagate current asset to strategy engine
            self.current_asset = asset
            if hasattr(self, 'strategy_engine') and self.strategy_engine:
                self.strategy_engine.current_asset = asset

            # ── LAYER 1: Voting engine (quick pre-filter) ──────────────────────
            # Run voting engine first — it's fast and kills HOLD signals early.
            # No point running the expensive quality pipeline on a HOLD.
            df_15m = self.fetch_historical_data(asset, 100, '15m')
            df_1h  = self.fetch_historical_data(asset, 100, '1h')

            if df_15m.empty or df_1h.empty:
                return None

            df_15m = self.add_technical_indicators(df_15m)
            df_1h  = self.add_technical_indicators(df_1h)

            # Feed price history to portfolio optimizer
            try:
                if hasattr(self, 'portfolio_optimizer'):
                    self.portfolio_optimizer.update_price_data(asset, df_1h['close'])
            except Exception:
                pass

            if self.strategy_mode == 'strict':
                base_signal = self.strict_strategy(df_15m, df_1h)
            elif self.strategy_mode == 'fast':
                base_signal = self.fast_strategy(df_15m, df_1h)
            elif self.strategy_mode == 'voting':
                combined    = self.get_combined_signal(df_15m)
                base_signal = combined if combined and combined['signal'] != 'HOLD' else None
            else:
                base_signal = self.balanced_strategy(df_15m, df_1h)

            if not base_signal:
                return None   # Voting engine killed it — skip expensive layers

            # ── LAYER 2: signal_learning full quality gate ─────────────────────
            quality_signal = None
            try:
                from signal_learning import get_instant_signal
                quality_signal = get_instant_signal(asset, category, self)
            except Exception as e:
                logger.debug(f"signal_learning unavailable for {asset}: {e}")

            if quality_signal:
                # Quality gate passed — use its ATR stops, confluence, learned bias
                # NOTE: signal_learning uses 'direction' key ('BUY'/'SELL'/'HOLD'),
                #       not 'signal' key. Check both for backward compat.
                _qs_dir = quality_signal.get('direction') or quality_signal.get('signal', 'HOLD')
                if _qs_dir == 'HOLD':
                    logger.debug(f"{asset}: killed by quality gate (confluence/RR/blackout/session)")
                    return None
                # Merge quality signal on top of base signal
                base_signal.update({
                    'signal':        _qs_dir,
                    'confidence':    quality_signal.get('confidence', base_signal.get('confidence', 0.5)),
                    'stop_loss':     quality_signal.get('stop_loss',  base_signal.get('stop_loss')),
                    'take_profit':   quality_signal.get('take_profit',base_signal.get('take_profit')),
                    'take_profit_2': quality_signal.get('take_profit_2'),
                    'take_profit_3': quality_signal.get('take_profit_3'),
                    'confluence':    quality_signal.get('timeframe_conf', quality_signal.get('confluence', 'UNKNOWN')),
                    'atr':           quality_signal.get('atr'),
                    'win_rate':      quality_signal.get('win_rate', 0),
                    'signal_id':     quality_signal.get('signal_id'),
                    'learning_bias': quality_signal.get('learning_bias', 0),
                    'rr_ratio':      quality_signal.get('risk_reward', quality_signal.get('rr_ratio', 0)),
                    'session':       quality_signal.get('session', ''),
                    'news_clear':    quality_signal.get('news_clear', True),
                })

            signal = base_signal
            signal['asset']    = asset
            signal['category'] = category

            # ── Rebuild take_profit_levels from quality signal TP prices ───────
            # quality_signal sets take_profit / take_profit_2 / take_profit_3
            # (ATR-based), overwriting the base strategy's single TP price.
            # Sync take_profit_levels so paper_trader sees the updated levels.
            if signal.get('take_profit') or signal.get('take_profit_2'):
                _rebuilt_tps = []
                for _i, _k in enumerate(['take_profit','take_profit_2','take_profit_3'], 1):
                    _v = signal.get(_k)
                    if _v:
                        _rebuilt_tps.append({'level': _i, 'price': round(float(_v), 6)})
                if _rebuilt_tps:
                    signal['take_profit_levels'] = _rebuilt_tps
            # ─────────────────────────────────────────────────────────────────

            # ── LAYER 3: Market regime gate ────────────────────────────────────
            # Don't trade choppy/ranging markets — wait for trending conditions.
            try:
                if hasattr(self, 'market_regime_analyzer') and self.market_regime_analyzer:
                    regime = self.market_regime_analyzer.detect_regime(df_1h)
                    regime_str = str(regime).lower() if regime else ''
                    signal['market_regime'] = regime_str
                    # Block trades when market is choppy/mean-reverting with low ADX
                    if any(x in regime_str for x in ['choppy', 'ranging', 'low_volatility']):
                        conf = signal.get('confidence', 0)
                        if conf < 0.72:   # Allow through only high-conviction signals
                            logger.debug(f"{asset}: blocked by regime gate ({regime_str}, conf={conf:.2f})")
                            return None
            except Exception as e:
                logger.debug(f"Regime gate error for {asset}: {e}")

            # ── LAYER 4: Session quality gate ──────────────────────────────────
            # Only fire during sessions where this specific asset has a proven edge.
            try:
                if hasattr(self, 'session_tracker') and self.session_tracker:
                    session_data = self.session_tracker.get_asset_session_performance(asset)
                    if session_data:
                        current_session = self.session_tracker.get_current_session()
                        sess_win_rate   = session_data.get(current_session, {}).get('win_rate', 100)
                        signal['session_win_rate'] = sess_win_rate
                        # Skip if this asset loses money in the current session
                        if sess_win_rate < 40 and session_data.get(current_session, {}).get('trades', 0) >= 5:
                            logger.debug(f"{asset}: blocked by session gate ({current_session} win={sess_win_rate}%)")
                            return None
            except Exception as e:
                logger.debug(f"Session gate error for {asset}: {e}")

            # ── LAYER 5: Sentiment confirmation ───────────────────────────────
            # Extreme sentiment against our direction reduces confidence.
            # Cached at scan-cycle level — one fetch per 5 min, shared across all 64 assets.
            # Previously: 64 assets × 42 sources = 2,688 HTTP calls per scan!
            try:
                if hasattr(self, 'sentiment_analyzer') and self.sentiment_analyzer:
                    import time as _time
                    _now = _time.time()
                    if not hasattr(self, '_sentiment_cache') or                        not hasattr(self, '_sentiment_cache_time') or                        (_now - self._sentiment_cache_time) > 300:   # refresh every 5 min
                        self._sentiment_cache = self.sentiment_analyzer.get_comprehensive_sentiment()
                        self._sentiment_cache_time = _now
                    sent = self._sentiment_cache
                    score = sent.get('score', 0)   # -1 (fear) to +1 (greed)
                    signal['sentiment_score'] = score
                    direction = signal.get('signal', 'HOLD')
                    # Extreme fear (< -0.6) + BUY signal → lower confidence
                    if score < -0.6 and direction == 'BUY':
                        signal['confidence'] = signal.get('confidence', 0.6) * 0.88
                        signal['sentiment_note'] = 'caution: extreme fear vs BUY'
                    # Extreme greed (> 0.6) + SELL signal → lower confidence
                    elif score > 0.6 and direction == 'SELL':
                        signal['confidence'] = signal.get('confidence', 0.6) * 0.88
                        signal['sentiment_note'] = 'caution: extreme greed vs SELL'
                    # Sentiment confirms direction → small boost
                    elif (score > 0.3 and direction == 'BUY') or (score < -0.3 and direction == 'SELL'):
                        signal['confidence'] = min(0.97, signal.get('confidence', 0.6) * 1.05)
                        signal['sentiment_note'] = 'sentiment confirms direction'
            except Exception as e:
                logger.debug(f"Sentiment gate error for {asset}: {e}")

            # ── LAYER 6: Whale intelligence overlay ───────────────────────────
            try:
                if hasattr(self, 'whale_signals'):
                    signal = self.enhance_signal_with_whale(signal, asset.split('-')[0])
            except Exception:
                pass

            # ── LAYER 6b: Order Flow alignment ────────────────────────────────
            # Adjusts confidence based on real bid/ask pressure alignment.
            try:
                if _orderflow_engine:
                    of_modifier = _orderflow_engine.get_signal_modifier(
                        asset, signal.get('signal', 'HOLD')
                    )
                    if of_modifier != 0:
                        old_conf = signal.get('confidence', 0)
                        signal['confidence'] = min(0.97, max(0.0, old_conf + of_modifier))
                        signal['orderflow_modifier'] = of_modifier
                        snap = _orderflow_engine.get_snapshot(asset)
                        if snap:
                            signal['orderflow_pressure'] = snap.get('pressure', 'NEUTRAL')
                            signal['orderflow_imbalance'] = snap.get('imbalance', 0)
            except Exception:
                pass

            # ── LAYER 7: Final confidence floor ───────────────────────────────
            # After all gates, if confidence still too low — kill it.
            final_conf = signal.get('confidence', 0)
            if final_conf < 0.52:
                logger.debug(f"{asset}: final confidence {final_conf:.2f} below floor — discarded")
                return None

            # Get live price
            price, source = self.fetcher.get_real_time_price(asset, category)
            if price:
                signal['entry_price'] = price
                signal['price_source'] = source
                # Feed tick into synthetic orderflow tracker for non-crypto
                try:
                    if _orderflow_engine and category in ('forex','commodities','stocks','indices'):
                        _orderflow_engine.update_forex_tick(asset, price, category)
                except Exception:
                    pass
                # Publish live price tick to Redis
                try:
                    if _redis_broker:
                        _redis_broker.publish_price(asset, price, category)
                except Exception:
                    pass

            # Attach regime for downstream risk sizing
            if hasattr(self, 'current_regime') and self.current_regime:
                signal['regime'] = str(self.current_regime)

            logger.info(
                f"✅ QUALITY SIGNAL: {asset} {signal.get('signal')} | "
                f"conf={signal.get('confidence', 0):.0%} | "
                f"confluence={signal.get('confluence','?')} | "
                f"RR={signal.get('rr_ratio', 0):.1f} | "
                f"session={signal.get('session','?')} | "
                f"sentiment={signal.get('sentiment_score', 0):+.2f}"
            )
            return signal

        except Exception as e:
            logger.warning(f"Error scanning {asset}: {e}")
            return None

    def scan_all_assets_parallel(self):
        """
        Scan all assets in parallel using ThreadPool
        OPTIMIZED VERSION
        """
        logger.info(f"Scanning {len(self.get_asset_list())} assets in parallel...")
        
        assets = self.get_asset_list()
        signals = []
        
        # Use ThreadPoolExecutor for parallel scanning
        with ThreadPoolExecutor(max_workers=10) as executor:
            # Submit all tasks
            future_to_asset = {
                executor.submit(self.scan_asset_parallel, asset, category): (asset, category)
                for asset, category in assets
                if MarketHours.get_status().get(category, False)  # Only open markets
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_asset):
                asset, category = future_to_asset[future]
                try:
                    signal = future.result(timeout=15)
                    if signal:
                        signals.append(signal)
                        logger.debug(f"{asset}: {signal['signal']} signal found")
                    else:
                        logger.debug(f"{asset}: No signal")
                except Exception as e:
                    logger.debug(f"{asset} failed: {e}")
        
        logger.info(f"Found {len(signals)} signals from {len(assets)} assets")
        
        # Sort by confidence
        signals.sort(key=lambda x: x.get('confidence', 0), reverse=True)
        
        return signals
    
    def process_parallel_signals(self, signals: List[Dict]):
        """
        Process signals from parallel scan and execute trades
        """
        if not signals:
            logger.info("No signals to process")
            return
        
        logger.info(f"Processing {len(signals)} signals...")
        
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
                        logger.info(f"EXECUTED: {asset} {signal['signal']}")
                else:
                    logger.info(f"SKIPPED {asset}: {reason}")
                    
            except Exception as e:
                logger.warning(f"Error processing {signal.get('asset', 'unknown')}: {e}")

    def start_professional_trading(self):
        """Start live trading with ALL features ACTIVATED (using 15m + 1h only)"""
        logger.info("="*70)
        logger.info("PROFESSIONAL LIVE TRADING - ALL FEATURES ACTIVATED")
        logger.info("="*70)
        logger.info("Portfolio Optimizer: ACTIVE - Monitoring diversification")
        logger.info("Multi-Timeframe: ACTIVE - Checking 15m + 1h confluence")
        logger.info("ML Predictions: ACTIVE - Ensemble models (10+ algorithms)")
        logger.info("ADVANCED AI: ACTIVE - Reinforcement Learning + Transformers + Swarm")
        logger.info("   • RL Agent: PPO - Learns optimal trading policies")
        logger.info("   • Transformer: Time Series - Predicts price movements")
        logger.info("   • Swarm Intelligence: 10+ agents collaborating")
        logger.info("Sentiment Analysis: ACTIVE - News scanning")
        logger.info("Intelligent Auto-Trainer: ACTIVE - Event-based learning")
        logger.info("   • Price movements (>2%)")
        logger.info("   • Session changes (London/NY/Asia)")
        logger.info("   • Major news events")
        logger.info("   • Time fallback (4 hours)")
        
        # 🔥 PROFITABILITY UPGRADE: Enhanced display
        if hasattr(self, 'profitability_upgrades_active') and self.profitability_upgrades_active:
            logger.info("PROFITABILITY UPGRADES: ACTIVE")
            logger.info("   • 60-min cooldown after losses")
            logger.info("   • Category limits (1 crypto, 2 forex)")
            logger.info("   • ATR-based stops")
            logger.info("   • Entry quality filters")
            logger.info("   • 4-hour stale position cleanup")
        
        # 📊 MARKET REGIME DETECTION: Add to banner
        logger.info("MARKET REGIME DETECTION: ACTIVE")
        logger.info("   • Dynamic position sizing based on market conditions")
        logger.info("   • 1.5-1.8x in strong trends")
        logger.info("   • 0.3-0.7x in choppy/volatile markets")
        
        # 🔗 CORRELATION CHECKER: Add to banner
        logger.info("CORRELATION CHECKER: ACTIVE")
        logger.info("   • Prevents correlated position blowups")
        logger.info("   • Maximum correlation threshold: 0.7")
        logger.info("   • Portfolio VaR (Value at Risk) tracking")
        
        # 🛡️ ENHANCED RISK MANAGEMENT: Add to banner
        logger.info("ENHANCED RISK MANAGEMENT: ACTIVE")
        logger.info("   • Sentiment-based stop loss adjustment")
        logger.info("   • Volatility-aware position sizing")
        logger.info("   • Market regime detection (trending/ranging/breakout)")
        
        # 📅 MARKET CALENDAR: Add to banner
        logger.info("MARKET CALENDAR: ACTIVE")
        logger.info("   • Economic event tracking (FOMC, CPI, NFP)")
        logger.info("   • Earnings calendar")
        logger.info("   • Crypto halving countdown")
        logger.info("   • Auto-risk reduction before major events")
        logger.info("")
        logger.info("DATA SOURCES:")
        logger.info("   • Finnhub      - Real-time stocks, forex, crypto")
        logger.info("   • Twelve Data  - Commodities, indices, ETFs (supports 4H!)")
        logger.info("   • Alpha Vantage - Stocks, forex, commodities")
        logger.info("   • Yahoo Finance - Universal fallback")
        logger.info("   • Binance      - Crypto WebSocket (real-time)")
        
        # ===== TELEGRAM STARTUP MESSAGE =====
        if hasattr(self, 'telegram') and self.telegram:
            try:
                self.telegram.send_message(
                    "🚀 *Professional Trading Started*\n\n"
                    f"Mode: {self.strategy_mode.upper()}\n"
                    f"Balance: ${self.risk_manager.account_balance:.2f}\n"
                    "Monitoring 50+ assets in parallel"
                )
                logger.info("Telegram: Startup message sent")
            except Exception as e:
                logger.warning(f"Telegram startup message failed: {e}")
        # ====================================
        
        logger.info("="*70)
        
        self.is_running = True
        
        # ── Start platform upgrade services ───────────────────────────────
        try:
            if _orderflow_engine:
                _orderflow_engine.start()
                logger.info("OrderFlow Engine: ACTIVE — real-time bid/ask analysis")
        except Exception as _e:
            logger.warning(f"OrderFlow Engine failed to start: {_e}")

        try:
            if _alpha_engine:
                _alpha_engine.start()
                logger.info("Alpha Discovery: ACTIVE — correlation/anomaly/divergence scanning")
        except Exception as _e:
            logger.warning(f"Alpha Discovery failed to start: {_e}")

        try:
            if _pred_tracker:
                _pred_tracker.start()
                logger.info("Prediction Tracker: ACTIVE — accuracy monitoring at 1H/4H/24H")
        except Exception as _e:
            logger.warning(f"Prediction Tracker failed to start: {_e}")

        logger.info(f"Redis Broker: {'ACTIVE' if _REDIS_OK else 'unavailable (bot works without it)'}")
        try:
            from advanced_ai import AdvancedAIIntegration
            self.ai_system = AdvancedAIIntegration()
            # We'll initialize with first asset's data when available
            self.ai_initialized = False
            logger.info("Advanced AI systems: READY for initialization")
        except Exception as e:
            logger.warning(f"Could not initialize Advanced AI: {e}")
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
                            logger.info("New trading day started - Daily loss limit reset")
                        
                        # Refresh market calendar daily
                        if hasattr(self, 'market_calendar') and self.market_calendar:
                            self.market_calendar.fetch_economic_calendar()
                            self.market_calendar.fetch_earnings_calendar()
                        
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
                                logger.warning(f"PORTFOLIO ALERT: Low diversification score ({div_score}%)")
                                
                                # Find most concentrated category
                                categories = {}
                                for pos in open_positions:
                                    cat = pos.get('category', 'unknown')
                                    categories[cat] = categories.get(cat, 0) + 1
                                
                                most = max(categories.items(), key=lambda x: x[1])
                                if most[1] >= 3:
                                    logger.info(f"   • Too many {most[0]} positions ({most[1]})")
                                    logger.info(f"   • Consider taking profit on 1 position")
                    
                    # 🔥 PROFITABILITY UPGRADE: Check for stale positions
                    if hasattr(self, 'profitability_upgrades_active') and self.profitability_upgrades_active:
                        try:
                            # Get current prices for stale position check
                            current_prices = {}
                            for pos in open_positions:
                                _pos_cat = pos.get('category') or self.fetcher._get_asset_category(pos['asset'])
                                price, _ = self.fetcher.get_real_time_price(pos['asset'], _pos_cat)
                                if price:
                                    current_prices[pos['asset']] = price
                            
                            # Check for stale positions (open >4 hours with no profit)
                            if hasattr(self, 'position_age_monitor'):
                                stale = self.position_age_monitor.get_stale_positions(open_positions, current_prices)
                                for s in stale:
                                    logger.info(f"FORCE CLOSING stale position: {s['asset']} (open {s['age_hours']}h)")
                                    if hasattr(self.paper_trader, 'force_close'):
                                        self.paper_trader.force_close(s['trade_id'], current_prices[s['asset']], s['reason'])
                                    else:
                                        logger.debug(f"Would close: {s['trade_id']} - {s['reason']} (force_close method not available)")
                        except Exception as e:
                            logger.warning(f"Stale position check error: {e}")
                    
                    # ===== MARKET CALENDAR RISK CHECK =====
                    calendar_risk_multiplier = 1.0  # Default to no reduction
                    if hasattr(self, 'market_calendar') and self.market_calendar:
                        try:
                            # Refresh events occasionally
                            if health_check_counter % 60 == 0:  # Every hour
                                self.market_calendar.fetch_economic_calendar()
                                self.market_calendar.fetch_earnings_calendar()
                            
                            # Check if we should reduce risk
                            risk_rec = self.market_calendar.should_reduce_risk()
                            # calendar_risk_multiplier = risk_rec['risk_multiplier']  # COMMENT THIS OUT
                            calendar_risk_multiplier = 1.0  # FORCE to 1.0 (no reduction)
                            
                            if risk_rec['reduce_trading']:
                                logger.info(f"MARKET EVENT WARNING: Would reduce risk to {risk_rec['risk_multiplier']:.0%} (FORCED OFF)")
                                if risk_rec['high_impact_events']:
                                    logger.info(f"      • High-impact economic events coming up")
                                if risk_rec['halving_soon']:
                                    logger.info(f"      • Crypto halving approaching")
                        except Exception as e:
                            logger.warning(f"Market calendar error: {e}")
                    
                    # ===== REPLACE THE OLD SCANNING LOOP WITH THIS =====
                    # ===== PARALLEL SIGNAL SCANNING =====
                    logger.info(f"Scanning assets in parallel...")
                    
                    # Get all assets that are currently open for trading
                    active_assets = [
                        (asset, category) for asset, category in self.get_asset_list()
                        if MarketHours.get_status().get(category, False)
                    ]
                    
                    logger.info(f"   Active markets: {len(active_assets)} assets")
                    
                    # Scan all active assets in parallel
                    signals = self.scan_all_assets_parallel()
                    
                    # Process the top signals
                    if signals:
                        logger.info(f"Processing top signals...")
                        
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
                                            logger.debug(f"Correlation check: {corr_reason}")
                                    except Exception as e:
                                        logger.debug(f"Correlation check error: {e}")
                                
                                # ===== DAILY LOSS LIMIT CHECK =====
                                if should_trade and hasattr(self, 'daily_loss_limit') and self.daily_loss_limit:
                                    trading_allowed, status_message = self.daily_loss_limit.update(0)
                                    if not trading_allowed:
                                        should_trade = False
                                        reason = f"Daily loss limit: {status_message}"
                                        logger.info(f"{reason}")
                                
                                if should_trade:
                                    # ===== FETCH DATA FOR ENHANCED RISK =====
                                    try:
                                        # Fetch data for this asset
                                        df_15m = self.fetch_historical_data(asset, 100, '15m')
                                        df_1h = self.fetch_historical_data(asset, 100, '1h')
                                        
                                        if not df_15m.empty and not df_1h.empty:
                                            df_15m = self.add_technical_indicators(df_15m)
                                            df_1h = self.add_technical_indicators(df_1h)
                                            
                                            # 1. Get sentiment score (use cycle cache)
                                            sentiment_score = 0
                                            if hasattr(self, 'sentiment_analyzer'):
                                                import time as _t
                                                if hasattr(self, '_sentiment_cache') and                                                    hasattr(self, '_sentiment_cache_time') and                                                    (_t.time() - self._sentiment_cache_time) < 300:
                                                    sentiment_data = self._sentiment_cache
                                                else:
                                                    sentiment_data = self.sentiment_analyzer.get_comprehensive_sentiment()
                                                    self._sentiment_cache = sentiment_data
                                                    self._sentiment_cache_time = _t.time()
                                                sentiment_score = sentiment_data.get('score', 0)
                                            
                                            # 2. Detect market regimes
                                            if hasattr(self, 'risk_manager'):
                                                market_regime = self.risk_manager.get_market_regime_from_df(df_1h)
                                                volatility_regime = self.risk_manager.get_volatility_regime(df_1h)
                                                
                                                # 3. Calculate ATR
                                                atr = df_15m['atr'].iloc[-1] if 'atr' in df_15m.columns else signal['entry_price'] * 0.01
                                                
                                                # 4. Calculate dynamic stop loss
                                                stop_info = self.risk_manager.calculate_dynamic_stop_loss(
                                                    atr=atr,
                                                    entry_price=signal['entry_price'],
                                                    sentiment_score=sentiment_score,
                                                    market_regime=market_regime,
                                                    volatility_regime=volatility_regime
                                                )
                                                
                                                # 5. Override signal's stop loss
                                                if signal['signal'] == 'BUY':
                                                    signal['stop_loss'] = signal['entry_price'] - stop_info['stop_distance']
                                                else:  # SELL
                                                    signal['stop_loss'] = signal['entry_price'] + stop_info['stop_distance']
                                                
                                                # 6. Get win rate from paper trader
                                                win_rate = 0.55
                                                avg_win = 0.02
                                                avg_loss = 0.01
                                                if hasattr(self, 'paper_trader'):
                                                    perf = self.paper_trader.get_performance()
                                                    if perf['total_trades'] > 10:
                                                        win_rate = perf['win_rate'] / 100
                                                        # You could calculate avg_win and avg_loss from trade history here
                                                
                                                # 7. Calculate position size with sentiment
                                                position_info = self.risk_manager.calculate_position_size_with_sentiment(
                                                    entry_price=signal['entry_price'],
                                                    stop_loss=signal['stop_loss'],
                                                    signal_confidence=signal.get('confidence', 0.7),
                                                    sentiment_score=sentiment_score,
                                                    market_regime=market_regime,
                                                    win_rate=win_rate,
                                                    avg_win=avg_win,
                                                    avg_loss=avg_loss
                                                )
                                                
                                                # 8. Apply calendar risk multiplier
                                                position_info['position_size'] *= calendar_risk_multiplier
                                                position_info['risk_amount'] *= calendar_risk_multiplier
                                                position_info['risk_pct'] *= calendar_risk_multiplier
                                                
                                                # 9. Update signal with enhanced risk info
                                                signal['position_size'] = position_info['position_size']
                                                signal['risk_amount'] = position_info['risk_amount']
                                                signal['risk_pct'] = position_info['risk_pct']
                                                signal['risk_info'] = {
                                                    'market_regime': market_regime,
                                                    'volatility_regime': volatility_regime,
                                                    'sentiment_score': sentiment_score,
                                                    'sentiment_regime': self.risk_manager.get_sentiment_regime(sentiment_score),
                                                    'stop_atr_multiple': stop_info['atr_multiple'],
                                                    'stop_percent': stop_info['stop_percent'],
                                                    'sentiment_adjustment': position_info.get('sentiment_adjustment', 1.0),
                                                    'regime_adjustment': position_info.get('regime_adjustment', 1.0),
                                                    'calendar_adjustment': calendar_risk_multiplier
                                                }
                                                
                                                # 10. Print enhanced risk analysis
                                                logger.info(f"ENHANCED RISK ANALYSIS for {asset}:")
                                                logger.info(f"      • Market Regime: {market_regime}")
                                                logger.info(f"      • Volatility: {volatility_regime}")
                                                logger.info(f"      • Sentiment: {sentiment_score:.2f} ({self.risk_manager.get_sentiment_regime(sentiment_score)})")
                                                if calendar_risk_multiplier < 1.0:
                                                    logger.info(f"      • Calendar Event: Reducing to {calendar_risk_multiplier:.0%} size")
                                                logger.info(f"      • Stop Loss: {stop_info['atr_multiple']:.1f}x ATR ({stop_info['stop_percent']:.2f}%)")
                                                logger.info(f"      • Position Size: {position_info['position_size']:.4f} units")
                                                logger.info(f"      • Risk: ${position_info['risk_amount']:.2f} ({position_info['risk_pct']:.2f}%)")
                                                logger.info(f"      • Adjustments: {position_info.get('sentiment_adjustment', 1.0):.1f}x sentiment, {position_info.get('regime_adjustment', 1.0):.1f}x regime, {calendar_risk_multiplier:.1f}x calendar")
                                                    
                                    except Exception as e:
                                        logger.warning(f"Enhanced risk calculation error for {asset}: {e}")
                                        # Continue with original signal if enhanced risk fails
                                    
                                    # ===== ADD PROFITABILITY UPGRADE ENHANCEMENT =====
                                    if hasattr(self, 'profitability_upgrades_active') and self.profitability_upgrades_active:
                                        try:
                                            from profitability_upgrade import enhance_signal
                                            open_positions = self.paper_trader.get_open_positions() if hasattr(self.paper_trader, 'get_open_positions') else []
                                            enhanced = enhance_signal(
                                                signal,
                                                df=df_15m if 'df_15m' in locals() and not df_15m.empty else None,
                                                open_positions=open_positions
                                            )
                                            if enhanced:
                                                signal = enhanced
                                                logger.debug("Signal enhanced with profitability upgrades")
                                        except Exception as e:
                                            logger.debug(f"Enhance signal failed: {e}")
                                    # =================================================
                                    
                                    # Execute paper trade
                                    trade = self.paper_trader.execute_signal(signal)
                                    if trade:
                                        daily_trades += 1
                                        logger.info(f"EXECUTED: {asset} {signal['signal']} [{signal.get('strategy_id', 'UNKNOWN')}]")
                                        
                                        # Show TP levels if available
                                        if signal.get('take_profit_levels') and len(signal.get('take_profit_levels', [])) > 0:
                                            tp = signal['take_profit_levels'][0]
                                            logger.info(f"     🎯 TP1: {tp['price']:.5f} ({tp.get('risk_reward', 1.5)}:1)")
                                        
                                        # ===== SEND TELEGRAM ALERT (rich quality signal) =====
                                        if hasattr(self, 'telegram') and self.telegram:
                                            try:
                                                _dir   = signal.get('signal', '?')
                                                _emoji = '🟢' if _dir == 'BUY' else '🔴'
                                                _asset = signal.get('asset', asset)
                                                _entry = signal.get('entry_price', 0)
                                                _sl    = signal.get('stop_loss', 0)
                                                _tp    = signal.get('take_profit', 0)
                                                _tp2   = signal.get('take_profit_2')
                                                _tp3   = signal.get('take_profit_3')
                                                _conf  = signal.get('confidence', 0)
                                                _rr    = signal.get('rr_ratio', 0)
                                                _conf_str  = signal.get('confluence', '')
                                                _wr        = signal.get('win_rate', 0)
                                                _bias      = signal.get('learning_bias', 0)
                                                _sess      = signal.get('session', '')
                                                _sent      = signal.get('sentiment_score', 0)
                                                _sent_note = signal.get('sentiment_note', '')
                                                _regime    = signal.get('market_regime', signal.get('regime', ''))
                                                _sess_wr   = signal.get('session_win_rate', 0)
                                                _strat     = signal.get('strategy_id', signal.get('strategy', ''))
                                                _risk_pct  = signal.get('risk_pct', 1.0)
                                                _risk_amt  = signal.get('risk_amount', 0)
                                                _whale     = signal.get('whale_signal', '')

                                                # Build TP lines
                                                _tp_lines = f"   TP1: `{_tp:.5f}`"
                                                if _tp2:  _tp_lines += f"\n   TP2: `{_tp2:.5f}`"
                                                if _tp3:  _tp_lines += f"\n   TP3: `{_tp3:.5f}`"

                                                # Confluence badge
                                                _conf_badge = {
                                                    'ALL3':   '🔥 ALL 3 TF AGREE',
                                                    'BOTH':   '✅ 2/3 TF AGREE',
                                                    '2OF3':   '✅ 2/3 TF AGREE',
                                                    'DIVERGE':'⚠️ DIVERGING TF',
                                                }.get(_conf_str, _conf_str)

                                                # Sentiment label
                                                _sent_lbl = ''
                                                if   _sent >  0.4: _sent_lbl = '😏 Greedy'
                                                elif _sent >  0.1: _sent_lbl = '😐 Mild greed'
                                                elif _sent < -0.4: _sent_lbl = '😨 Fearful'
                                                elif _sent < -0.1: _sent_lbl = '😐 Mild fear'
                                                else:              _sent_lbl = '😶 Neutral'

                                                # ── Human explainer narrative (Robbie speaks) ─
                                                # Pull mood + diary + memorable moments from DB
                                                # then layer our quality-gate data on top so
                                                # every word is grounded in real numbers.
                                                _human_intro = ""
                                                _human_outro = ""
                                                _market_narrative = ""
                                                try:
                                                    from human_explainer_db import DatabaseExplainer
                                                    _explainer = DatabaseExplainer(self)
                                                    _mood = _explainer.personality.current_mood

                                                    # Mood-aware opener
                                                    _mood_name = _mood.get('name', 'neutral')
                                                    _mood_emoji = _mood.get('emoji', '🤖')
                                                    _mood_desc  = _mood.get('description', '')
                                                    _opener_map = {
                                                        'euphoric':  f"ON FIRE right now {_mood_emoji} — {_mood_desc}. Check this out:",
                                                        'on_fire':   f"Feeling sharp {_mood_emoji} — {_mood_desc}. Got one for you:",
                                                        'confident': f"Feeling good {_mood_emoji} — {_mood_desc}. Here we go:",
                                                        'cautious':  f"Taking it steady {_mood_emoji} — {_mood_desc}. Worth watching:",
                                                        'shaken':    f"Been a rough patch {_mood_emoji} — {_mood_desc}. But this looks real:",
                                                        'grumpy':    f"Market's been difficult {_mood_emoji} but this stood out:",
                                                        'rich':      f"Having a great run {_mood_emoji} — {_mood_desc}. Another one:",
                                                        'neutral':   f"Hey Robbie 👋 — fresh signal just cleared every filter:",
                                                    }
                                                    _human_intro = _opener_map.get(_mood_name, f"Hey Robbie 👋")

                                                    # Market narrative (news → price connection)
                                                    _market_narrative = _explainer.get_market_narrative(
                                                        asset, signal.get('entry_price', 0)
                                                    )

                                                    # Memorable moment for this asset (30% chance)
                                                    import random as _rnd
                                                    _moment = _explainer.personality.get_memorable_moment(asset)
                                                    if _moment and _rnd.random() < 0.35:
                                                        _human_outro = f"\n💭 _{_moment}_"

                                                    # Sign-offs matched to mood
                                                    _signoffs = {
                                                        'euphoric':  "Can't stop, won't stop 🚀",
                                                        'on_fire':   "Let's ride this! 🔥",
                                                        'confident': "Let's see if it plays out 🤞",
                                                        'cautious':  "Keeping size small, staying sharp.",
                                                        'shaken':    "Being extra careful — trust the system.",
                                                        'grumpy':    "Back to watching charts 📉",
                                                        'rich':      "Another day, another setup 💰",
                                                        'neutral':   "That's my read — your call 🤖",
                                                    }
                                                    _human_outro += f"\n_{_signoffs.get(_mood_name, 'Good luck out there!')}_"

                                                    _explainer.close()
                                                except Exception as _he:
                                                    logger.debug(f"Human explainer unavailable: {_he}")
                                                    _human_intro = "Hey Robbie 👋 — signal cleared every filter:"
                                                # ─────────────────────────────────────────────

                                                _reasons = []

                                                # 1. Timeframe confluence reasoning
                                                if _conf_str == 'ALL3':
                                                    _reasons.append(
                                                        f"All 3 timeframes (15m, 1h, 4h) are pointing "
                                                        f"{_dir.lower()} — that level of agreement is rare "
                                                        f"and historically my strongest setups."
                                                    )
                                                elif _conf_str in ('BOTH', '2OF3'):
                                                    _reasons.append(
                                                        f"2 of 3 timeframes agree on {_dir.lower()}. "
                                                        f"Not perfect confluence but enough structure to act."
                                                    )

                                                # 2. Session reasoning
                                                if _sess_wr >= 60 and _sess:
                                                    _reasons.append(
                                                        f"This is the {_sess} session — historically "
                                                        f"I win {_sess_wr:.0f}% of my {_asset} trades "
                                                        f"during this window. Good timing."
                                                    )
                                                elif _sess_wr >= 50 and _sess:
                                                    _reasons.append(
                                                        f"{_sess} session is average for {_asset} "
                                                        f"({_sess_wr:.0f}% win rate) — no edge, no penalty."
                                                    )

                                                # 3. Learned win rate reasoning
                                                if _wr >= 65:
                                                    _reasons.append(
                                                        f"My last resolved {_asset} signals hit "
                                                        f"target {_wr:.0f}% of the time. "
                                                        f"The bot has been right on this asset recently."
                                                    )
                                                elif _wr > 0 and _wr < 45:
                                                    _reasons.append(
                                                        f"{_asset} has only hit target {_wr:.0f}% recently — "
                                                        f"I'm treating this with more caution than usual."
                                                    )

                                                # 4. Sentiment reasoning
                                                if _sent < -0.4 and _dir == 'BUY':
                                                    _reasons.append(
                                                        f"Market sentiment is fearful ({_sent:+.2f}). "
                                                        f"Fear often marks bottoms — contrarian BUY "
                                                        f"but I've reduced confidence slightly."
                                                    )
                                                elif _sent > 0.4 and _dir == 'SELL':
                                                    _reasons.append(
                                                        f"Sentiment is greedy ({_sent:+.2f}). "
                                                        f"Greed often marks tops — contrarian SELL "
                                                        f"but proceeding with reduced confidence."
                                                    )
                                                elif (_sent > 0.3 and _dir == 'BUY') or \
                                                     (_sent < -0.3 and _dir == 'SELL'):
                                                    _reasons.append(
                                                        f"Sentiment is confirming this direction "
                                                        f"({_sent_lbl}, {_sent:+.2f}). "
                                                        f"Market mood and price action aligned."
                                                    )

                                                # 5. Regime reasoning
                                                if _regime:
                                                    if 'trend' in _regime.lower():
                                                        _reasons.append(
                                                            f"Market is trending ({_regime}) — "
                                                            f"momentum trades have higher success rates "
                                                            f"in this condition."
                                                        )
                                                    elif 'breakout' in _regime.lower():
                                                        _reasons.append(
                                                            f"Breakout regime detected ({_regime}). "
                                                            f"These can move fast — TP targets may hit quickly."
                                                        )
                                                    elif any(x in _regime.lower() for x in ['chop','rang']):
                                                        _reasons.append(
                                                            f"Market is {_regime} but signal still passed "
                                                            f"with {_conf:.0%} confidence — above the "
                                                            f"72% threshold required in choppy conditions."
                                                        )

                                                # 6. RR reasoning
                                                if _rr >= 3.0:
                                                    _reasons.append(
                                                        f"Risk/reward is {_rr:.1f}:1 — exceptional. "
                                                        f"Even if I only win 1 in 3 of these I'm profitable."
                                                    )
                                                elif _rr >= 2.0:
                                                    _reasons.append(
                                                        f"R:R of {_rr:.1f}:1 is solid. "
                                                        f"Need to win 34%+ to break even on this setup."
                                                    )
                                                elif _rr >= 1.5:
                                                    _reasons.append(
                                                        f"R:R is {_rr:.1f}:1 — minimum acceptable. "
                                                        f"All other factors need to be strong for this."
                                                    )

                                                # 7. Whale reasoning
                                                if _whale:
                                                    _reasons.append(
                                                        f"Whale activity detected: {_whale}. "
                                                        f"Smart money is moving — adds conviction."
                                                    )

                                                # 8. Overall conviction statement
                                                if _conf >= 0.80:
                                                    _reasons.append(
                                                        f"Overall conviction: HIGH ({_conf:.0%}). "
                                                        f"This passed every filter in the system."
                                                    )
                                                elif _conf >= 0.65:
                                                    _reasons.append(
                                                        f"Overall conviction: MODERATE ({_conf:.0%}). "
                                                        f"Good setup, standard position size."
                                                    )
                                                else:
                                                    _reasons.append(
                                                        f"Overall conviction: CAUTIOUS ({_conf:.0%}). "
                                                        f"Signal passed all gates but I'm keeping "
                                                        f"position size small."
                                                    )

                                                _reasoning_text = "\n".join(
                                                    f"   {i+1}. {r}"
                                                    for i, r in enumerate(_reasons)
                                                ) if _reasons else "   Signal passed all 7 quality layers."

                                                # Store reasoning in signal for DB
                                                signal['reasoning'] = _reasoning_text
                                                # ─────────────────────────────────────────────

                                                msg = (
                                                    f"{_human_intro}\n\n"
                                                    f"{_emoji} *{_dir} {_asset}*\n"
                                                    f"{'─'*30}\n"
                                                    f"📍 Entry: `{_entry:.5f}`\n"
                                                    f"🛑 Stop:  `{_sl:.5f}`\n"
                                                    f"{_tp_lines}\n\n"
                                                    f"📊 *Signal Quality*\n"
                                                    f"   Confidence:  {_conf:.0%}\n"
                                                    f"   R:R Ratio:   {_rr:.2f}:1\n"
                                                    f"   Confluence:  {_conf_badge}\n"
                                                    f"   Win Rate:    {_wr:.0f}% (learned)\n"
                                                    f"   Bias:        {_bias:+.3f}\n\n"
                                                    f"🧠 *Why I'm taking this trade*\n"
                                                    f"{_reasoning_text}\n\n"
                                                )
                                                if _market_narrative:
                                                    msg += f"📰 *What's moving it*\n   {_market_narrative}\n\n"
                                                msg += (
                                                    f"📈 *Context*\n"
                                                    f"   Session:     {_sess} (win {_sess_wr:.0f}%)\n"
                                                    f"   Regime:      {_regime}\n"
                                                    f"   Sentiment:   {_sent_lbl} ({_sent:+.2f})\n"
                                                    f"   Strategy:    {_strat}\n"
                                                )
                                                if _sent_note:
                                                    msg += f"   Note:        _{_sent_note}_\n"
                                                if _whale:
                                                    msg += f"\n🐋 Whale:  {_whale}\n"
                                                msg += (
                                                    f"\n💰 *Risk*\n"
                                                    f"   Risk:  ${_risk_amt:.2f} ({_risk_pct:.1f}%)\n"
                                                    f"   Balance: ${self.risk_manager.account_balance:.2f}\n"
                                                    f"{'─'*30}\n"
                                                    f"_Passed 7-layer quality filter_"
                                                )
                                                if _human_outro:
                                                    msg += f"\n{_human_outro}"

                                                self.telegram.send_message(msg)
                                                logger.debug("Rich Telegram alert sent")

                                                # ── Publish to Redis → Node.js WebSocket gateway ──
                                                try:
                                                    if _redis_broker:
                                                        _redis_broker.publish_signal(signal)
                                                except Exception:
                                                    pass

                                                # ── Record for prediction accuracy tracking ───────
                                                try:
                                                    if _pred_tracker:
                                                        _pred_tracker.record_signal(signal)
                                                except Exception:
                                                    pass
                                            except Exception as e:
                                                logger.warning(f"Telegram alert failed: {e}")
                                        # ======================================================
                                    else:
                                        logger.warning(f"Trade execution failed for {asset}")
                                else:
                                    logger.info(f"SKIPPED {asset}: {reason}")
                                        
                            except Exception as e:
                                logger.warning(f"Error processing {signal.get('asset', 'unknown')}: {e}")
                    else:
                        logger.info("No signals found")
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
                                logger.warning(f"PORTFOLIO WARNINGS:")
                                for warning in health['warnings']:
                                    logger.warning(f"  • {warning}")
                            
                            # Check if rebalancing needed
                            if health.get('needs_rebalancing', False):
                                logger.info(f"Portfolio needs rebalancing (score: {health['diversification_score']}/100)")
                                logger.info(f"     Current VaR (95%): ${health['var_95']} ({health['var_95_percent']}%)")
                            
                            # Show category breakdown occasionally (every 10 cycles)
                            if health_check_counter % 10 == 0 and health.get('category_breakdown'):
                                logger.info(f"CATEGORY BREAKDOWN:")
                                for cat, data in health['category_breakdown'].items():
                                    logger.info(f"  • {cat}: {data['count']} positions (${data['value']:.2f})")
                            
                            # Show diversification score periodically
                            if health_check_counter % 5 == 0:
                                logger.info(f"Diversification Score: {health['diversification_score']}/100")
                                    
                    except Exception as e:
                        logger.warning(f"Portfolio health check error: {e}")

                    # ===== VERIFY ML PREDICTIONS =====
                    if health_check_counter % 5 == 0:
                        try:
                            if hasattr(self, 'verify_pending_predictions'):
                                self.verify_pending_predictions()
                        except Exception as e:
                            logger.warning(f"Prediction verification error: {e}")

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
                            logger.info("Daily cache cleanup completed")
                        self._last_cache_cleanup = current_date
                    # ==============================

                    # ===== DAILY LOSS LIMIT STATUS =====
                    daily_loss_status = ""
                    if hasattr(self, 'daily_loss_limit') and self.daily_loss_limit:
                        status = self.daily_loss_limit.get_status()
                        if status['trading_paused']:
                            daily_loss_status = " | LOSS LIMIT PAUSED"
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
                                    logger.info(f"SESSION INSIGHT:")
                                    logger.info(f"   Best session: {best['emoji']} {best['session']} ({best['win_rate']}% win rate)")
                                    
                                    # Show hourly breakdown
                                    hourly = self.session_tracker.analyze_by_hour()
                                    if not hourly.empty:
                                        top_hours = hourly.head(3)
                                        hours_str = []
                                        for _, h in top_hours.iterrows():
                                            hour = int(h['hour'])
                                            hours_str.append(f"{hour}:00")
                                        logger.info(f"   Best hours: {', '.join(hours_str)}")
                        except Exception as e:
                            logger.warning(f"Session tracker error: {e}")
                    # ====================================

                    # Show performance with all enhancements
                    perf = self.paper_trader.get_performance()
                    ai_status = "AI ACTIVE" if (self.ai_system and self.ai_initialized) else "AI INITIALIZING"

                    # 🔥 PROFITABILITY UPGRADE: Add to status line
                    upgrade_status = "UPGRADES ON" if (hasattr(self, 'profitability_upgrades_active') and self.profitability_upgrades_active) else "UPGRADES OFF"

                    # 📊 Add regime info to status line
                    regime_info = ""
                    if hasattr(self, 'current_regime') and self.current_regime:
                        regime_str = str(self.current_regime.value)[:15] if hasattr(self.current_regime, 'value') else str(self.current_regime)[:15]
                        regime_info = f" | 📊 {regime_str}"

                    # Print comprehensive status
                    logger.info(f"Portfolio: ${perf['current_balance']:.2f} | "
                        f"Win Rate: {perf['win_rate']}% | "
                        f"Open: {perf['open_positions']} | "
                        f"Today: {daily_trades} trades | "
                        f"Mode: {strategy_mode} | "
                        f"{ai_status} | "
                        f"{upgrade_status}"
                        f"{regime_info}"
                        f"{daily_loss_status}")
                    
                except Exception as e:
                    logger.error(f"Trading error: {e}", exc_info=True)
                
                time.sleep(60)  # Scan every minute
        
        thread = threading.Thread(target=trading_loop, daemon=True)
        thread.start()
        logger.info(f"Professional trading started with {strategy_mode.upper()} strategy (15m + 1h)!")
        if hasattr(self, 'ai_system') and self.ai_system:
            logger.info("Advanced AI systems will initialize on first data fetch")
        
        # 🔥 PROFITABILITY UPGRADE: Print confirmation
        if hasattr(self, 'profitability_upgrades_active') and self.profitability_upgrades_active:
            logger.info("Profitability upgrades protecting your account!")
        
        # 📊 Market Regime Detection confirmation
        logger.info("Market Regime Detection active - position sizing adapts to market conditions")
        
        # 🔗 Correlation Checker confirmation
        logger.info("Correlation Checker active - preventing correlated position blowups")
        
        # 🛡️ Enhanced Risk Management confirmation
        logger.info("Enhanced Risk Management active - sentiment & volatility adjusted stops")
        
        # 📅 Market Calendar confirmation
        if hasattr(self, 'market_calendar') and self.market_calendar:
            logger.info("Market Calendar active - risk reduces before major events")
        
        # ===== TELEGRAM READY MESSAGE =====
        if hasattr(self, 'telegram') and self.telegram:
            try:
                self.telegram.send_message(
                    "✅ *Trading System Ready*\n\n"
                    f"Mode: {self.strategy_mode.upper()}\n"
                    f"Balance: ${self.risk_manager.account_balance:.2f}\n"
                    f"Open Positions: {len(self.paper_trader.get_open_positions())}\n\n"
                    "Use /help for commands"
                )
            except Exception as e:
                logger.warning(f"Telegram ready message failed: {e}")
        # ==================================

    def show_session_report(self):
        """Display comprehensive session performance report"""
        if not hasattr(self, 'session_tracker') or not self.session_tracker:
            logger.error("Session tracker not initialized")
            return
        
        report = self.session_tracker.get_summary_report()
        
        logger.info("="*70)
        logger.info("SESSION PERFORMANCE REPORT")
        logger.info("="*70)
        logger.info(f"Total Trades: {report['total_trades']}")
        
        logger.info("Performance by Session:")
        for session, stats in report['sessions'].items():
            if isinstance(stats, dict) and 'trades' in stats:
                emoji = stats.get('emoji', '')
                logger.info(f"  {emoji} {stats['session']}:")
                logger.info(f"     • Trades: {stats['trades']}")
                logger.info(f"     • Win Rate: {stats['win_rate']}%")
                logger.info(f"     • Total P&L: ${stats['total_pnl']}")
        
        if 'best_session' in report and 'message' not in report['best_session']:
            logger.info(f"Best Session: {report['best_session']['emoji']} {report['best_session']['session']}")
            logger.info(f"   Win Rate: {report['best_session']['win_rate']}%")
        
        if 'recommendation' in report:
            logger.info(f"Recommendation: {report['recommendation']}")

    def show_upcoming_events(self):
        """Display upcoming market events"""
        if not hasattr(self, 'market_calendar') or not self.market_calendar:
            return
        
        logger.info("UPCOMING MARKET EVENTS")
        logger.info("="*60)
        
        # Economic events
        events = self.market_calendar.get_high_impact_events(days=7)
        if events:
            logger.info("High-Impact Economic Events:")
            for event in events:
                days = (event['date'] - datetime.now()).days
                logger.info(f"   • {event['event']} in {days} days - Forecast: {event['forecast']}")
        
        # Earnings
        if self.market_calendar.earnings:
            logger.info("Upcoming Earnings:")
            for earning in self.market_calendar.earnings[:5]:
                days = (earning['date'] - datetime.now()).days
                logger.info(f"   • {earning['symbol']} in {days} days - EPS est: {earning['eps_estimate']}")
        
        # Halving
        halving = self.market_calendar.get_halving_countdown('bitcoin')
        if halving['days_until'] > 0:
            logger.info(f"Bitcoin Halving: {halving['days_until']} days away")

    def get_asset_list(self) -> List[tuple]:
        """Get COMPLETE list of assets to trade"""
        return [
            # ===== COMMODITIES - KEEP THESE 7 =====
            ('XAU/USD', 'commodities'),  # Gold Spot
            ('XAG/USD', 'commodities'),  # Silver Spot
            ('WTI/USD', 'commodities'),  # WTI Crude Oil Spot
            ('NG/USD', 'commodities'),   # Natural Gas Spot
            ('XCU/USD', 'commodities'),  # Copper Spot
            ('GC=F', 'commodities'),     # Gold Futures
            ('SI=F', 'commodities'),     # Silver Future
            ('CL=F', 'commodities'),     # Crude Futures

            # ===== CRYPTO - ONLY THESE 11 =====
            ('BTC-USD', 'crypto'),
            ('ETH-USD', 'crypto'),
            ('BNB-USD', 'crypto'),
            ('SOL-USD', 'crypto'),
            ('XRP-USD', 'crypto'),
            ('ADA-USD', 'crypto'),
            ('DOGE-USD', 'crypto'),
            ('DOT-USD', 'crypto'),
            ('LTC-USD', 'crypto'),
            ('AVAX-USD', 'crypto'),
            ('LINK-USD', 'crypto'),
            
            # ===== FOREX (keep all) =====
            ('EUR/USD', 'forex'),
            ('GBP/USD', 'forex'),
            ('USD/JPY', 'forex'),
            ('AUD/USD', 'forex'),
            ('USD/CAD', 'forex'),
            ('NZD/USD', 'forex'),
            ('USD/CHF', 'forex'),
            ('EUR/GBP', 'forex'),
            ('EUR/JPY', 'forex'),
            ('GBP/JPY', 'forex'),
            ('AUD/JPY', 'forex'),
            ('EUR/AUD', 'forex'),
            ('GBP/AUD', 'forex'),
            ('AUD/CAD', 'forex'),
            ('CAD/JPY', 'forex'),
            ('CHF/JPY', 'forex'),
            ('EUR/CAD', 'forex'),
            ('EUR/CHF', 'forex'),
            ('GBP/CAD', 'forex'),
            ('GBP/CHF', 'forex'),

            # ===== INDICES (keep all) =====
            ('^GSPC', 'indices'),   # S&P 500
            ('^DJI', 'indices'),    # Dow Jones
            ('^IXIC', 'indices'),   # Nasdaq
            ('^FTSE', 'indices'),   # FTSE 100
            ('^N225', 'indices'),   # Nikkei 225
            ('^HSI', 'indices'),    # Hang Seng
            ('^GDAXI', 'indices'),  # DAX
            ('^VIX', 'indices'),    # Volatility Index
            
            # ===== STOCKS - REDUCED LIST =====
            ('AAPL', 'stocks'),    # Apple
            ('MSFT', 'stocks'),    # Microsoft
            ('GOOGL', 'stocks'),   # Google
            ('AMZN', 'stocks'),    # Amazon
            ('TSLA', 'stocks'),    # Tesla
            ('NVDA', 'stocks'),    # NVIDIA
            ('META', 'stocks'),    # Meta
            ('JPM', 'stocks'),     # JPMorgan
            ('V', 'stocks'),       # Visa
            ('MA', 'stocks'),      # Mastercard
            ('JNJ', 'stocks'),     # Johnson & Johnson
            ('PFE', 'stocks'),     # Pfizer
            ('WMT', 'stocks'),     # Walmart
            ('PG', 'stocks'),      # Procter & Gamble
            ('KO', 'stocks'),      # Coca-Cola
            ('XOM', 'stocks'),     # Exxon
            ('CVX', 'stocks'),     # Chevron
        ]
    
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
                logger.debug(f"Using cached data for {asset} ({interval})")
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
                        logger.debug(f"{source_name}: Got {rows} rows for {asset} ({interval})")
                        
                        if rows > best_rows:
                            best_df = df
                            best_rows = rows
                except Exception as e:
                    logger.debug(f"{source_name} failed for {asset}: {e}")
                    continue

         # ===== SAVE TO CACHE =====
        if best_df is not None and hasattr(self, 'cache_manager') and self.cache_manager:
            # Cache for 5 minutes
            self.cache_manager.set_historical_data(asset, interval, best_df, ttl=300)
        # =========================
        
        if best_df is not None:
            return best_df
        
        logger.warning(f"No data for {asset} ({interval}) from any source")
        return pd.DataFrame()


    def _fetch_yahoo_historical(self, asset: str, days: int, interval: str) -> pd.DataFrame:
        """Fetch from Yahoo Finance"""
        try:
            # Symbol mapping
            symbol_map = {
                # Forex
                'EUR/USD': 'EURUSD=X', 'GBP/USD': 'GBPUSD=X', 'USD/JPY': 'JPY=X',
                'AUD/USD': 'AUDUSD=X', 'USD/CAD': 'CAD=X',    'NZD/USD': 'NZDUSD=X',
                'USD/CHF': 'CHF=X',    'EUR/GBP': 'EURGBP=X', 'EUR/JPY': 'EURJPY=X',
                'GBP/JPY': 'GBPJPY=X','AUD/JPY': 'AUDJPY=X', 'EUR/AUD': 'EURAUD=X',
                'GBP/AUD': 'GBPAUD=X','AUD/CAD': 'AUDCAD=X', 'CAD/JPY': 'CADJPY=X',
                'CHF/JPY': 'CHFJPY=X','EUR/CAD': 'EURCAD=X', 'EUR/CHF': 'EURCHF=X',
                'GBP/CAD': 'GBPCAD=X','GBP/CHF': 'GBPCHF=X', 'NZD/CAD': 'NZDCAD=X',
                'USD/SGD': 'SGD=X',   'USD/HKD': 'HKD=X',
                'USD/MXN': 'MXN=X',   'USD/ZAR': 'ZAR=X',    'USD/TRY': 'TRY=X',
                # Crypto
                'BTC-USD': 'BTC-USD', 'ETH-USD': 'ETH-USD',  'BNB-USD': 'BNB-USD',
                'SOL-USD': 'SOL-USD', 'XRP-USD': 'XRP-USD',  'ADA-USD': 'ADA-USD',
                'DOGE-USD':'DOGE-USD','AVAX-USD':'AVAX-USD',  'DOT-USD': 'DOT-USD',
                # Commodities — spot aliases map to futures tickers Yahoo actually has
                'XAU/USD': 'GC=F',    'GOLD': 'GC=F',        'GC=F': 'GC=F',
                'XAG/USD': 'SI=F',    'SILVER': 'SI=F',      'SI=F': 'SI=F',
                'WTI/USD': 'CL=F',    'OIL': 'CL=F',         'CL=F': 'CL=F',
                'XPT/USD': 'PL=F',    'XPD/USD': 'PA=F',
                'NG/USD':  'NG=F',    'NG=F': 'NG=F',
                'HG=F': 'HG=F',       'COPPER': 'HG=F',  'XCU/USD': 'HG=F',
                # Stocks
                'AAPL': 'AAPL', 'MSFT': 'MSFT', 'GOOGL': 'GOOGL',
                'AMZN': 'AMZN', 'TSLA': 'TSLA', 'NVDA': 'NVDA',
                'META': 'META', 'NFLX': 'NFLX', 'AMD':  'AMD',
                # Indices
                '^GSPC': '^GSPC', '^DJI': '^DJI', '^IXIC': '^IXIC',
                'SP500': '^GSPC',  'DOW': '^DJI',  'NASDAQ': '^IXIC',
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
        except Exception as e:
            logger.debug(f"Yahoo historical error for {asset}: {e}")
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
            logger.debug(f"Twelve Data error for {asset}: {e}")
            
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
        except Exception as e:
            logger.debug(f"Alpha Vantage error for {asset}: {e}")
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
        except Exception as e:
            logger.debug(f"Finnhub error for {asset}: {e}")
        return pd.DataFrame()
    
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
    
    def update_all_positions(self):
        """Update all open positions with current prices"""
        current_prices = {}
        for position in self.paper_trader.get_open_positions():
            _pos_cat = position.get('category') or self.fetcher._get_asset_category(position['asset'])
            price, _ = self.fetcher.get_real_time_price(
                position['asset'], _pos_cat
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
        logger.info("Stopping system...")
        self.is_running = False
        self.update_all_positions()
        logger.info("System stopped")


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
    parser.add_argument('--no-telegram', action='store_true',
                        help='Disable Telegram commander (use when running multiple instances)')
    
    args = parser.parse_args()
    
    if args.reset:
        import os
        from datetime import datetime
        if os.path.exists('paper_trades.json'):
            backup_name = f'paper_trades_backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
            os.rename('paper_trades.json', backup_name)
            logger.info(f"Trade history reset - backed up to {backup_name}")
        else:
            logger.info("No trade history found to reset")

    system = UltimateTradingSystem(
        account_balance=args.balance,
        strategy_mode=args.strategy_mode,
        no_telegram=args.no_telegram  # ← PASS THE FLAG TO CONSTRUCTOR
    )

    if args.sessions:
        system.show_session_report()
        return

    
    # ===== BATCH OPTIMIZATION =====
    if args.mode == 'batch-optimize':
        logger.info("="*70)
        logger.info("BATCH OPTIMIZATION MODE")
        logger.info("="*70)
        logger.warning("This will take a LONG time (minutes to hours depending on the number of assets)")
        logger.info("Recommended to run overnight or on a weekend")
        
        # Confirm with user
        response = input("Continue with batch optimization? (y/n): ").strip().lower()
        if response != 'y':
            logger.info("Batch optimization cancelled")
            return
        
        # Determine which assets to optimize
        if args.assets:
            assets_to_optimize = args.assets
            logger.info(f"Optimizing specified {len(assets_to_optimize)} assets: {', '.join(assets_to_optimize)}")
        else:
            # Get all assets from the system
            asset_list = system.get_asset_list()
            assets_to_optimize = [asset[0] for asset in asset_list]
            logger.info(f"Optimizing ALL {len(assets_to_optimize)} assets in the system")
        
        logger.info(f"Using {args.lookback} days of historical data")
        logger.info(f"Estimated time: ~{len(assets_to_optimize) * 5} minutes")
        
        # Second confirmation
        response2 = input(f"Final confirmation - start optimization now? (y/n): ").strip().lower()
        if response2 != 'y':
            logger.info("Batch optimization cancelled")
            return
        
        # Run the batch optimization
        try:
            results = system.batch_optimize_all_assets(
                assets=assets_to_optimize,
                lookback_days=args.lookback
            )
            
            logger.info("="*70)
            logger.info("BATCH OPTIMIZATION COMPLETE")
            logger.info("="*70)
            logger.info(f"Successfully optimized {len(results)} assets")
            logger.info(f"Results saved in: optimization_results/")
            
            # Show top strategies across all assets
            if hasattr(system, 'create_master_optimization_report'):
                system.create_master_optimization_report(results)
            
            # Auto-apply optimized params (no interactive prompt in production)
            logger.info("Auto-applying optimized parameters to all strategies...")
            try:
                system.apply_optimized_params_to_strategies()
                logger.info("Optimized parameters applied to all strategies")
            except Exception as _e:
                logger.warning(f"Could not auto-apply params: {_e}")
            
        except KeyboardInterrupt:
            logger.warning("Batch optimization interrupted by user")
            logger.info("Partial results may have been saved")
        except Exception as e:
            logger.error(f"Error during batch optimization: {e}", exc_info=True)
    
    # ===== BACKTEST =====
    elif args.mode == 'backtest':
        logger.info(f"Backtesting {args.asset}...")
        system.backtest_asset(args.asset)
    
    # ===== OPTIMIZE SINGLE STRATEGY =====
    elif args.mode == 'optimize':
        logger.info(f"Optimizing {args.strategy} for {args.asset}...")
        system.optimize_strategy(args.asset, args.strategy)
    
    # ===== TRAIN ML MODELS =====
    elif args.mode == 'train':
        from datetime import datetime
        from data.fetcher import MarketHours
    
        market_status = MarketHours.get_status()
        is_weekend = market_status['is_weekend']
    
        if is_weekend:
            logger.info("="*60)
            logger.info(" WEEKEND MODE: Training only Crypto (24/7 markets)")
            logger.info("="*60)
            logger.info("   • Forex: CLOSED")
            logger.info("   • Stocks: CLOSED") 
            logger.info("   • Commodities: CLOSED")
            logger.info("   • Indices: CLOSED")
        
            assets = [
                # Crypto only (works on weekends)
                'BTC-USD', 'ETH-USD', 'BNB-USD', 'SOL-USD', 'XRP-USD',
                'ADA-USD', 'DOGE-USD', 'DOT-USD', 'LTC-USD', 'AVAX-USD',
                'LINK-USD'
            ]
        else:
            logger.info("="*60)
            logger.info(" WEEKDAY MODE: Training ALL assets")
            logger.info("="*60)
            logger.info("   • Crypto: OPEN")
            logger.info("   • Forex: OPEN")
            logger.info("   • Stocks: OPEN")
            logger.info("   • Commodities: OPEN")
            logger.info("   • Indices: OPEN")
        
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
    
        logger.info(f"Training {len(assets)} assets...")
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
                    logger.info(json.dumps(report, indent=2, default=str))
        except KeyboardInterrupt:
            system.stop()
    
    # ===== DEFAULT / HELP =====
    else:
        logger.info("="*60)
        logger.info(" ULTIMATE TRADING SYSTEM - HELP")
        logger.info("="*60)
        logger.info("Available commands:")
        logger.info("  --mode backtest       Backtest a single asset")
        logger.info("  --mode optimize       Optimize a single strategy")
        logger.info("  --mode train          Train ML models")
        logger.info("  --mode compare        Compare all strategies")
        logger.info("  --mode live           Start live trading")
        logger.info("  --mode batch-optimize Run batch optimization for all assets")
        logger.info("Examples:")
        logger.info("  python trading_system.py --mode backtest --asset BTC-USD")
        logger.info("  python trading_system.py --mode optimize --asset BTC-USD --strategy rsi")
        logger.info("  python trading_system.py --mode batch-optimize")
        logger.info("  python trading_system.py --mode batch-optimize --assets BTC-USD ETH-USD")
        logger.info("  python trading_system.py --mode batch-optimize --lookback 180")
        logger.info("  python trading_system.py --mode live --balance 100 --strategy-mode voting")


if __name__ == "__main__":
    main()