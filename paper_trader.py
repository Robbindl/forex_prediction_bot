"""
PAPER TRADER - Simulated trading without real money
Tracks virtual positions, P&L, and trade history with alerts
"""

import json
import os
from datetime import datetime
from typing import Dict, List, Optional
import threading
import uuid
from services.database_service import DatabaseService
from logger import logger

class PaperTrade:
    """Individual paper trade record"""
    
    def __init__(self, asset: str, category: str, signal_type: str, 
                 entry_price: float, position_size: float, stop_loss: float,
                 take_profit_levels: List[Dict], confidence: float, reason: str,
                 strategy_id: str = "UNKNOWN", strategy_emoji: str = "🤖",
                 contributing_strategies: list = None,
                 db=None):  # FIX 3: accept shared db instance
        self.trade_id = str(uuid.uuid4())[:8]
        self.asset = asset
        self.category = category
        self.signal_type = signal_type
        self.entry_price = entry_price
        self.position_size = position_size
        self.stop_loss = stop_loss
        self.take_profit_levels = take_profit_levels
        self.confidence = confidence
        self.reason = reason
        self.entry_time = datetime.now()
        self.exit_time = None
        self.exit_price = None
        self.pnl = 0.0
        self.pnl_percent = 0.0
        self.exit_reason = None
        self.status = "OPEN"
        self.duration_minutes = 0
        self.strategy_id = strategy_id
        self.strategy_emoji = strategy_emoji
        self.contributing_strategies = contributing_strategies or []
        # FIX 3: use shared db — never create a new DatabaseService per trade
        self.db = db
        self.use_db = db is not None and getattr(db, "use_db", False)
        # Learning engine signal ID — set after recording, used to resolve outcome
        self.signal_id: str = ""
        # Trailing stop state
        self.trailing_stop_active: bool = False
        self.trailing_distance: float = 0.0
        self.highest_price: float = entry_price
        self.lowest_price: float = entry_price
    
    def to_dict(self) -> Dict:
        """Convert trade to dictionary"""
        return {
            'trade_id': self.trade_id,
            'asset': self.asset,
            'category': self.category,
            'signal': self.signal_type,
            'entry_price': self.entry_price,
            'position_size': self.position_size,
            'stop_loss': self.stop_loss,
            'take_profit_levels': self.take_profit_levels,
            'confidence': self.confidence,
            'reason': self.reason,
            'entry_time': self.entry_time.isoformat(),
            'exit_time': self.exit_time.isoformat() if self.exit_time else None,
            'exit_price': self.exit_price,
            'pnl': self.pnl,
            'pnl_percent': self.pnl_percent,
            'exit_reason': self.exit_reason,
            'status': self.status,
            'duration_minutes': self.duration_minutes,
            'strategy_id': self.strategy_id,
            'strategy_emoji': self.strategy_emoji,
            'signal_id': self.signal_id
        }


class PaperTrader:
    """
    - Paper Trading Engine
    - Simulates trades in real-time
    - Tracks open positions
    - Calculates P&L
    - Exports trade history
    - Sends alerts via monitor
    """
    
    def __init__(self, risk_manager=None, history_file: str = "paper_trades.json", save_json=False):
        self.risk_manager = risk_manager
        self.history_file = history_file
        self.save_json = save_json
        self.open_positions: Dict[str, PaperTrade] = {}
        self.closed_positions: List[PaperTrade] = []
        self.lock = threading.RLock()
        self.monitor = None  # Will be set by trading system
        self.voting_engine = None  # ← ADDED THIS (will be set by trading system)
        self.db = DatabaseService()  # Initialize database
        self.use_db = True
        self.trading_system = None  # ← ADDED THIS: Will be set by trading system for Telegram access
        
        # Load history if exists
        self._load_history()

        # Real-time position monitor — polls prices every 5s and calls update_positions
        self._monitor_running = False
        self._monitor_thread: threading.Thread | None = None

        logger.info("Paper Trader Initialized")
        logger.info(f"Open Positions: {len(self.open_positions)}")
        logger.info(f"Historical Trades: {len(self.closed_positions)}")
    
    # ── Real-time position monitor ──────────────────────────────────────────────
    def start_monitor(self) -> None:
        """Start background thread that polls prices and fires SL/TP checks every 5s."""
        if self._monitor_running:
            return
        self._monitor_running = True
        self._monitor_thread = threading.Thread(target=self._monitor_loop, name="pos-monitor", daemon=True)
        self._monitor_thread.start()
        logger.info("Position monitor started (5s interval)")

    def stop_monitor(self) -> None:
        """Stop the position monitor thread gracefully."""
        self._monitor_running = False
        logger.info("Position monitor stopped")

    def _monitor_loop(self) -> None:
        """Background loop: fetch current prices → update_positions → SL/TP resolution."""
        import time
        while self._monitor_running:
            try:
                with self.lock:
                    open_assets = {t.asset: t.category for t in self.open_positions.values()}
                if open_assets:
                    prices: dict = {}
                    # Try to get prices via the fetcher wired on trading_system
                    fetcher = None
                    if hasattr(self, 'trading_system') and self.trading_system:
                        fetcher = getattr(self.trading_system, 'fetcher', None)
                    if fetcher:
                        for asset, category in open_assets.items():
                            try:
                                price, _ = fetcher.get_real_time_price(asset, category)
                                if price and price > 0:
                                    prices[asset] = price
                            except Exception as _pe:
                                logger.debug(f"Monitor price fetch {asset}: {_pe}")
                    if prices:
                        self.update_positions(prices)
            except Exception as e:
                logger.error(f"Position monitor error: {e}", exc_info=True)
            time.sleep(5)
    # ────────────────────────────────────────────────────────────────────────────

    def _load_history(self):
        """Load trade history from file"""
        try:
            if os.path.exists(self.history_file):
                with open(self.history_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    
                    # Load closed positions (simplified - would need proper deserialization)
                    if 'closed_positions' in data:
                        logger.info(f"Loaded {len(data['closed_positions'])} trades from history")
                        
        except Exception as e:
            logger.error(f"Could not load trade history: {e}")
    
    def _save_history(self):
        """Save trade history to file"""
        if not self.save_json:  # Skip if JSON saving is disabled
            return
        
        try:
            history = {
                'open_positions': [p.to_dict() for p in self.open_positions.values()],
                'closed_positions': [p.to_dict() for p in self.closed_positions]
            }
            with open(self.history_file, 'w', encoding='utf-8') as f:
                json.dump(history, f, indent=2, default=str)
        except Exception as e:
            logger.error(f"Could not save trade history: {e}")

    def partial_close(self, trade_id: str, current_price: float, level: int) -> Optional[Dict]:
        """
        Close a percentage of position at take profit levels
        Level 1: Close 50%, move stop to breakeven
        Level 2: Close 30%, start trailing stop
        Level 3: Let rest run with trailing stop
        """
        with self.lock:
            if trade_id not in self.open_positions:
                return None
            
            trade = self.open_positions[trade_id]
            
            if level == 1:
                # Close 50% at TP1
                close_size = trade.position_size * 0.5
                trade.position_size -= close_size
                
                # Calculate P&L for closed portion
                if trade.signal_type == 'BUY':
                    pnl = (current_price - trade.entry_price) * close_size
                else:
                    pnl = (trade.entry_price - current_price) * close_size
                
                # Move stop to breakeven
                trade.stop_loss = trade.entry_price
                
                logger.info(f"TP1 HIT: Closed 50% of {trade.asset} at ${current_price:.2f}")
                logger.info(f"P&L on closed portion: ${pnl:.2f}")
                logger.info(f"Stop moved to breakeven (${trade.entry_price:.2f})")
                
                # Record this in trade metadata
                if not hasattr(trade, 'metadata'):
                    trade.metadata = {}
                trade.metadata['tp1_hit'] = True
                trade.metadata['tp1_price'] = current_price
                trade.metadata['tp1_pnl'] = pnl
                
                return {
                    'trade_id': trade_id,
                    'action': 'partial_close',
                    'level': 1,
                    'closed_percent': 50,
                    'pnl': pnl,
                    'remaining_size': trade.position_size
                }
                
            elif level == 2:
                # Close another 30% at TP2
                close_size = trade.position_size * 0.3
                trade.position_size -= close_size
                
                if trade.signal_type == 'BUY':
                    pnl = (current_price - trade.entry_price) * close_size
                else:
                    pnl = (trade.entry_price - current_price) * close_size
                
                # Activate trailing stop
                trade.trailing_stop_active = True
                trade.highest_price = current_price
                trade.trailing_distance = trade.atr * 2 if hasattr(trade, 'atr') else current_price * 0.01
                
                logger.info(f"TP2 HIT: Closed 30% of {trade.asset} at ${current_price:.2f}")
                logger.info(f"P&L on closed portion: ${pnl:.2f}")
                logger.info(f"Trailing stop activated (distance: ${trade.trailing_distance:.2f})")
                
                return {
                    'trade_id': trade_id,
                    'action': 'partial_close',
                    'level': 2,
                    'closed_percent': 30,
                    'pnl': pnl,
                    'remaining_size': trade.position_size
                }
            
            return None
    
    def update_trailing_stop(self, trade_id: str, current_price: float) -> bool:
        """
        Update trailing stop for active trades
        Returns True if stop was hit (trade should close)
        """
        with self.lock:
            if trade_id not in self.open_positions:
                return False
            
            trade = self.open_positions[trade_id]
            
            if not hasattr(trade, 'trailing_stop_active') or not trade.trailing_stop_active:
                return False
            
            # Update highest price for longs, lowest for shorts
            if trade.signal_type == 'BUY':
                if current_price > trade.highest_price:
                    trade.highest_price = current_price
                    # Move stop up
                    new_stop = current_price - trade.trailing_distance
                    trade.stop_loss = max(trade.stop_loss, new_stop)
            else:  # SELL
                if current_price < trade.lowest_price:
                    trade.lowest_price = current_price
                    # Move stop down
                    new_stop = current_price + trade.trailing_distance
                    trade.stop_loss = min(trade.stop_loss, new_stop)
            
            # Check if stop was hit
            if trade.signal_type == 'BUY' and current_price <= trade.stop_loss:
                return True
            elif trade.signal_type == 'SELL' and current_price >= trade.stop_loss:
                return True
            
            return False
    
    # 🔥 PROFITABILITY UPGRADE: New method to force close stale positions
    def force_close(self, trade_id: str, current_price: float, reason: str = "Force closed") -> Optional[Dict]:
        """
        Force close a position (used for stale trades, risk limits, etc.)
        
        Args:
            trade_id: ID of the trade to close
            current_price: Current price to close at
            reason: Reason for force closing
            
        Returns:
            Closed trade details or None if trade not found
        """
        with self.lock:
            if trade_id not in self.open_positions:
                logger.warning(f"Trade {trade_id} not found")
                return None
            
            trade = self.open_positions.pop(trade_id)
            trade.exit_time = datetime.now()
            trade.exit_price = current_price
            trade.exit_reason = reason
            trade.status = "CLOSED"
            
            # Calculate duration in minutes
            delta = trade.exit_time - trade.entry_time
            trade.duration_minutes = int(delta.total_seconds() / 60)
            
            # Calculate P&L
            if trade.signal_type == 'BUY':
                trade.pnl = (trade.exit_price - trade.entry_price) * trade.position_size
            else:  # SELL
                trade.pnl = (trade.entry_price - trade.exit_price) * trade.position_size
            
            trade.pnl_percent = (trade.pnl / (trade.entry_price * trade.position_size)) * 100 if trade.position_size > 0 else 0
            
            # ===== UPDATE VOTING ENGINE PERFORMANCE =====
            if hasattr(trade, 'contributing_strategies') and hasattr(self, 'voting_engine') and self.voting_engine:
                try:
                    trade_result = {
                        'contributing_strategies': trade.contributing_strategies,
                        'pnl': trade.pnl,
                        'asset': trade.asset,
                        'exit_reason': trade.exit_reason
                    }
                    self.voting_engine.update_strategy_performance(trade_result)
                except Exception as e:
                    logger.error(f"Failed to update voting engine: {e}")
            
            # ===== UPDATE DATABASE =====
            if hasattr(self, 'use_db') and self.use_db:
                try:
                    exit_data = {
                        'exit_price': trade.exit_price,
                        'exit_reason': trade.exit_reason,
                        'pnl': trade.pnl,
                        'pnl_percent': trade.pnl_percent
                    }
                    self.db.update_trade_exit(trade.trade_id, exit_data)
                    logger.info(f"Trade exit saved to database")
                except Exception as e:
                    logger.error(f"Failed to update database: {e}")
            
            # Update risk manager if available
            if self.risk_manager and hasattr(self.risk_manager, 'update_pnl'):
                self.risk_manager.update_pnl(trade.pnl)
            
            # Add to closed positions
            self.closed_positions.append(trade)
            
            logger.info(f"FORCE CLOSED: {trade.asset} - {reason}")
            logger.info(f"P&L: ${trade.pnl:.2f} ({trade.pnl_percent:.2f}%)")
            logger.info(f"Duration: {trade.duration_minutes} minutes")
            logger.info(f"Trade ID: {trade.trade_id}")
            
            # ===== SEND ALERT =====
            if hasattr(self, 'monitor') and self.monitor:
                try:
                    emoji = "[PROFIT]" if trade.pnl > 0 else "[LOSS]"
                    self.monitor._send_alert(
                        'WARNING',
                        f"{emoji} Trade Force Closed: {trade.asset}",
                        f"Reason: {reason}\n"
                        f"P&L: ${trade.pnl:.2f} ({trade.pnl_percent:.2f}%)\n"
                        f"Entry: ${trade.entry_price:.2f} → Exit: ${trade.exit_price:.2f}\n"
                        f"Duration: {trade.duration_minutes} minutes"
                    )
                except Exception as e:
                    logger.error(f"Could not send alert: {e}")
            
            # ===== SEND TELEGRAM ALERT =====
            if hasattr(self, 'trading_system') and hasattr(self.trading_system, 'telegram') and self.trading_system.telegram:
                try:
                    self.trading_system.telegram.alert_trade_closed(trade.to_dict())
                except Exception as e:
                    logger.error(f"Could not send Telegram alert: {e}")
            # ================================
            
            # Save history
            self._save_history()
            
            return {
                'trade_id': trade.trade_id,
                'asset': trade.asset,
                'signal': trade.signal_type,
                'pnl': trade.pnl,
                'pnl_percent': trade.pnl_percent,
                'exit_reason': reason
            }
    
    # 🔥 PROFITABILITY UPGRADE: New method to check if asset is on cooldown
    def is_asset_on_cooldown(self, asset: str) -> bool:
        """
        Check if an asset is on cooldown after a loss
        This integrates with the profitability upgrade's cooldown tracker
        
        Args:
            asset: Asset symbol to check
            
        Returns:
            True if asset is on cooldown, False otherwise
        """
        try:
            from profitability_upgrade import cooldown_tracker
            return cooldown_tracker.is_cooling_down(asset)
        except ImportError:
            return False
        except Exception:
            return False
    
    # 🔥 PROFITABILITY UPGRADE: New method to get cooldown time remaining
    def get_cooldown_remaining(self, asset: str) -> int:
        """
        Get minutes remaining on asset cooldown
        
        Args:
            asset: Asset symbol
            
        Returns:
            Minutes remaining (0 if not on cooldown)
        """
        try:
            from profitability_upgrade import cooldown_tracker
            return cooldown_tracker.get_remaining(asset)
        except ImportError:
            return 0
        except Exception:
            return 0
    
    def execute_signal(self, signal: Dict) -> Optional[Dict]:
        """
        Execute a trading signal (paper trade)
        Returns trade execution details or None if rejected
        """
        with self.lock:
            # Skip HOLD signals
            if signal.get('signal') in ['HOLD', 'CLOSED']:
                return None
            
            # 🔥 PROFITABILITY UPGRADE: Check cooldown
            if self.is_asset_on_cooldown(signal['asset']):
                remaining = self.get_cooldown_remaining(signal['asset'])
                logger.info(f"Skipping {signal['asset']}: On cooldown ({remaining}min remaining)")
                return None
            
            # Check risk manager if available
            if self.risk_manager:
                # Check max positions
                if hasattr(self.risk_manager, 'max_positions'):
                    if len(self.open_positions) >= self.risk_manager.max_positions:
                        logger.info(f"Skipping {signal['asset']}: Max positions reached")
                        return None
                
                # Calculate position size with risk management
                if hasattr(self.risk_manager, 'calculate_optimal_position_size'):
                    pos_result = self.risk_manager.calculate_optimal_position_size(
                        entry_price=signal['entry_price'],
                        stop_loss=signal['stop_loss'],
                        signal_confidence=signal.get('confidence', 0.7)
                    )
                    position_size = pos_result.get('position_size', 0)
                    risk_amount = pos_result.get('risk_amount', 0)
                else:
                    # FIXED position sizing for small accounts
                    account_balance = 20.0  # Your actual balance
                    if hasattr(self, 'risk_manager') and self.risk_manager:
                        account_balance = getattr(self.risk_manager, 'account_balance', 20.0)
                    
                    # Risk only 1% of account per trade (20 cents)
                    risk_per_trade = 0.01  # 1%
                    risk_amount = account_balance * risk_per_trade
                    
                    # Calculate price difference
                    price_diff = abs(signal['entry_price'] - signal['stop_loss'])
                    
                    if price_diff > 0:
                        # Position size = risk_amount / price_diff
                        position_size = risk_amount / price_diff
                        
                        # SAFETY CHECK: Never risk more than 2% of account
                        max_risk = account_balance * 0.02  # 2% max
                        if risk_amount > max_risk:
                            risk_amount = max_risk
                            position_size = risk_amount / price_diff
                            
                        # Position value check
                        position_value = position_size * signal['entry_price']
                        max_position_value = account_balance * 0.5  # Never put >50% in one trade
                        
                        if position_value > max_position_value:
                            position_size = max_position_value / signal['entry_price']
                            risk_amount = position_size * price_diff
                    else:
                        position_size = 0
                        risk_amount = 0
            else:
                # No risk manager - use conservative defaults
                account_balance = 10000
                risk_amount = account_balance * 0.01  # 1% risk
                price_diff = abs(signal['entry_price'] - signal['stop_loss'])
                position_size = risk_amount / price_diff if price_diff > 0 else 0
            
            if position_size <= 0:
                logger.info(f"Skipping {signal['asset']}: Invalid position size")
                return None
            
            # Create paper trade - MODIFIED to include contributing_strategies
            trade = PaperTrade(
                asset=signal['asset'],
                category=signal.get('category', 'unknown'),
                signal_type=signal['signal'],
                entry_price=signal['entry_price'],
                position_size=position_size,
                stop_loss=signal['stop_loss'],
                take_profit_levels=signal.get('take_profit_levels', []),
                confidence=signal.get('confidence', 0.5),
                reason=signal.get('reason', 'Signal generated'),
                strategy_id=signal.get('strategy_id', 'UNKNOWN'),
                strategy_emoji=signal.get('strategy_emoji', '🤖'),
                contributing_strategies=signal.get('contributing_strategies', []),
                db=self.db  # FIX 3: pass shared db instance
            )
            
            # Store position
            self.open_positions[trade.trade_id] = trade
            # Persist signal_id from signal dict so learning engine can resolve outcome later
            if signal.get('signal_id'):
                trade.signal_id = signal['signal_id']
            
            # ===== RECORD TRADE IN SESSION TRACKER =====
            if hasattr(self, 'trading_system') and hasattr(self.trading_system, 'session_tracker'):
                if self.trading_system.session_tracker:
                    trade_data = {
                        'entry_time': trade.entry_time,
                        'pnl': 0,  # Will be updated when closed
                        'asset': trade.asset,
                        'signal': trade.signal_type,
                        'strategy_id': trade.strategy_id,
                        'trade_id': trade.trade_id,
                        'category': trade.category
                    }
                    self.trading_system.session_tracker.record_trade(trade_data)
                    logger.info(f"Trade recorded in session tracker")
            # ===========================================
            
            # ===== NEW: SAVE TO DATABASE =====
            if hasattr(self, 'use_db') and self.use_db:
                try:
                    trade_data = {
                        'trade_id': trade.trade_id,
                        'asset': signal['asset'],
                        'category': signal.get('category', 'unknown'),
                        'signal': signal['signal'],
                        'entry_price': signal['entry_price'],
                        'position_size': position_size,
                        'stop_loss': signal['stop_loss'],
                        'take_profit': signal.get('take_profit'),
                        'confidence': signal.get('confidence', 0),
                        'strategy_id': signal.get('strategy_id', 'UNKNOWN'),
                        'metadata': {'reason': signal.get('reason', '')}
                    }
                    self.db.save_trade(trade_data)
                    logger.info(f"Trade saved to database")
                except Exception as e:
                    logger.error(f"Failed to save to database: {e}")
            # =================================
            
            # Update risk manager trade count if available
            if self.risk_manager and hasattr(self.risk_manager, 'increment_trades'):
                self.risk_manager.increment_trades()
            
            logger.info(f"EXECUTED: {signal['asset']} {signal['signal']}")
            logger.info(f"Size: {position_size:.4f} units")
            logger.info(f"Risk: ${risk_amount:.2f}")
            logger.info(f"Trade ID: {trade.trade_id}")
            
            # ===== SEND NEW TRADE ALERT =====
            if hasattr(self, 'monitor') and self.monitor:
                try:
                    # Pass the original signal which has ALL the strategy info
                    self.monitor.on_new_trade(signal)
                    logger.info(f"Trade alert sent! [{signal.get('strategy_id', 'UNKNOWN')} {signal.get('strategy_emoji', '🤖')}]")
                except Exception as e:
                    logger.error(f"Could not send alert: {e}")
            
            # ===== SEND TELEGRAM ALERT =====
            if hasattr(self, 'trading_system') and hasattr(self.trading_system, 'telegram') and self.trading_system.telegram:
                try:
                    self.trading_system.telegram.alert_trade_opened(signal)
                    logger.info(f"Telegram alert sent!")
                except Exception as e:
                    logger.error(f"Could not send Telegram alert: {e}")
            # ================================
            
            # Save history
            self._save_history()
            
            return {
                'trade_id': trade.trade_id,
                'asset': signal['asset'],
                'signal': signal['signal'],
                'position_size': position_size,
                'risk_amount': risk_amount,
                'entry_price': signal['entry_price']
            }
    
    def update_positions(self, current_prices: Dict[str, float]):
        """
        Update open positions with current prices.
        Check for stop loss hits and take profit targets.

        BUG FIXES applied here
        ─────────────────────
        1. A trade was being appended to to_close twice when both a
           stop-loss AND a take-profit level triggered in the same tick
           (common for volatile assets like Silver/XAG).  The second
           pop() then raised a KeyError, silently dropping the closed
           trade from accounting.
        2. `to_close` is now a set so duplicate IDs are impossible.
        3. After recording a stop-loss hit we skip the TP scan for that
           trade (the 'continue' below).
        """
        with self.lock:
            to_close = set()   # FIX: set prevents double-close

            for trade_id, trade in self.open_positions.items():
                if trade.asset not in current_prices:
                    continue

                current_price = current_prices[trade.asset]

                # ── BUY position ─────────────────────────────────────────
                if trade.signal_type == 'BUY':
                    if current_price <= trade.stop_loss:
                        trade.exit_price = trade.stop_loss
                        trade.exit_reason = "Stop Loss"
                        trade.status = "CLOSED"
                        to_close.add(trade_id)
                        continue   # FIX: skip TP check once SL is hit

                    # Check take profit levels (first level hit wins)
                    for tp in trade.take_profit_levels:
                        if current_price >= tp['price']:
                            trade.exit_price = tp['price']
                            trade.exit_reason = f"Take Profit {tp['level']}"
                            trade.status = "CLOSED"
                            to_close.add(trade_id)
                            break

                # ── SELL position ─────────────────────────────────────────
                elif trade.signal_type == 'SELL':
                    if current_price >= trade.stop_loss:
                        trade.exit_price = trade.stop_loss
                        trade.exit_reason = "Stop Loss"
                        trade.status = "CLOSED"
                        to_close.add(trade_id)
                        continue   # FIX: skip TP check once SL is hit

                    for tp in trade.take_profit_levels:
                        if current_price <= tp['price']:
                            trade.exit_price = tp['price']
                            trade.exit_reason = f"Take Profit {tp['level']}"
                            trade.status = "CLOSED"
                            to_close.add(trade_id)
                            break
            
            # Close positions and calculate P&L
            for trade_id in to_close:
                trade = self.open_positions.pop(trade_id)
                trade.exit_time = datetime.now()
                
                # Calculate duration in minutes
                delta = trade.exit_time - trade.entry_time
                trade.duration_minutes = int(delta.total_seconds() / 60)
                
                # Calculate P&L
                if trade.signal_type == 'BUY':
                    trade.pnl = (trade.exit_price - trade.entry_price) * trade.position_size
                else:  # SELL
                    trade.pnl = (trade.entry_price - trade.exit_price) * trade.position_size
                
                trade.pnl_percent = (trade.pnl / (trade.entry_price * trade.position_size)) * 100 if trade.position_size > 0 else 0
                
                # ===== UPDATE VOTING ENGINE PERFORMANCE =====
                # Check if trade has contributing strategies and voting engine exists
                if hasattr(trade, 'contributing_strategies') and hasattr(self, 'voting_engine') and self.voting_engine:
                    try:
                        trade_result = {
                            'contributing_strategies': trade.contributing_strategies,
                            'pnl': trade.pnl,
                            'asset': trade.asset,
                            'exit_reason': trade.exit_reason
                        }
                        self.voting_engine.update_strategy_performance(trade_result)
                    except Exception as e:
                        logger.error(f"Failed to update voting engine: {e}")
                # =============================================
                
                # 🔥 PROFITABILITY UPGRADE: Update cooldown tracker on losses
                if trade.pnl < 0:  # Losing trade
                    try:
                        from profitability_upgrade import on_trade_closed
                        on_trade_closed(trade.asset, trade.pnl, trade.exit_reason)
                        logger.info(f"Cooldown activated for {trade.asset} (60min)")
                    except ImportError:
                        pass  # Upgrade not installed
                    except Exception as e:
                        logger.error(f"Could not update cooldown: {e}")
                
                # ===== NEW: UPDATE DATABASE WITH EXIT DATA =====
                if hasattr(self, 'use_db') and self.use_db:
                    try:
                        exit_data = {
                            'exit_price': trade.exit_price,
                            'exit_reason': trade.exit_reason,
                            'pnl': trade.pnl,
                            'pnl_percent': trade.pnl_percent
                        }
                        self.db.update_trade_exit(trade.trade_id, exit_data)
                        logger.info(f"Trade exit saved to database")
                    except Exception as e:
                        logger.error(f"Failed to update database: {e}")
                # ==============================================
                
                # Update risk manager if available
                if self.risk_manager and hasattr(self.risk_manager, 'update_pnl'):
                    self.risk_manager.update_pnl(trade.pnl)

                # ===== UPDATE DAILY LOSS LIMIT =====
                if hasattr(self, 'trading_system') and hasattr(self.trading_system, 'daily_loss_limit'):
                    if self.trading_system.daily_loss_limit:
                        # Pass the P&L to update daily loss tracking
                        self.trading_system.daily_loss_limit.update(trade.pnl)
                # ===================================
                
                # ===== UPDATE SESSION TRACKER WITH P&L =====
                if hasattr(self, 'trading_system') and hasattr(self.trading_system, 'session_tracker'):
                    if self.trading_system.session_tracker:
                        # Find and update the trade in session tracker
                        for t in self.trading_system.session_tracker.trade_history:
                            if t.get('trade_id') == trade.trade_id:
                                t['pnl'] = trade.pnl
                                t['exit_time'] = trade.exit_time
                                t['exit_reason'] = trade.exit_reason
                                t['pnl_percent'] = trade.pnl_percent
                                logger.info(f"Session tracker updated with P&L")
                                break
                # ===========================================
                
                # Add to closed positions
                self.closed_positions.append(trade)
                
                logger.info(f"CLOSED: {trade.asset} - {trade.exit_reason}")
                logger.info(f"P&L: ${trade.pnl:.2f} ({trade.pnl_percent:.2f}%)")
                logger.info(f"Trade ID: {trade.trade_id}")
                
                # ===== SEND CLOSED TRADE ALERT =====
                if hasattr(self, 'monitor') and self.monitor:
                    try:
                        emoji = "[PROFIT]" if trade.pnl > 0 else "[LOSS]"
                        self.monitor._send_alert(
                            'SUCCESS' if trade.pnl > 0 else 'WARNING',
                            f"{emoji} Trade Closed: {trade.asset}",
                            f"Exit Reason: {trade.exit_reason}\n"
                            f"P&L: ${trade.pnl:.2f} ({trade.pnl_percent:.2f}%)\n"
                            f"Entry: ${trade.entry_price:.2f} → Exit: ${trade.exit_price:.2f}\n"
                            f"Duration: {trade.duration_minutes} minutes"
                        )
                        logger.info(f"P&L alert sent!")
                    except Exception as e:
                        logger.error(f"Could not send alert: {e}")
                
                # ===== SEND TELEGRAM ALERT =====
                if hasattr(self, 'trading_system') and hasattr(self.trading_system, 'telegram') and self.trading_system.telegram:
                    try:
                        self.trading_system.telegram.alert_trade_closed(trade.to_dict())
                        logger.info(f"Telegram alert sent!")
                    except Exception as e:
                        logger.error(f"Could not send Telegram alert: {e}")
                # ================================

                # ===== SIGNAL LEARNING: resolve outcome so bot learns =====
                sid = getattr(trade, 'signal_id', None) or (
                    trade.to_dict().get('signal_id') if hasattr(trade, 'to_dict') else None
                )
                if sid:
                    try:
                        from signal_learning import signal_engine
                        outcome = 'TP_HIT' if 'Take Profit' in (trade.exit_reason or '') else 'SL_HIT'
                        signal_engine.resolve(sid, outcome, trade.exit_price)
                        logger.debug(f"Learning engine notified: {trade.asset} {outcome}")
                    except Exception as _le:
                        logger.debug(f"Signal learning resolve skipped: {_le}")
                # =========================================================
            
            if to_close: 
                self._save_history()
    
    def get_performance(self) -> Dict:
        """Get trading performance metrics"""
        with self.lock:
            total_trades = len(self.closed_positions)
            winning_trades = len([t for t in self.closed_positions if t.pnl > 0])
            losing_trades = len([t for t in self.closed_positions if t.pnl < 0])
            
            win_rate = (winning_trades / total_trades * 100) if total_trades > 0 else 0
            
            total_pnl = sum(t.pnl for t in self.closed_positions)
            avg_win = sum(t.pnl for t in self.closed_positions if t.pnl > 0) / winning_trades if winning_trades > 0 else 0
            avg_loss = sum(abs(t.pnl) for t in self.closed_positions if t.pnl < 0) / losing_trades if losing_trades > 0 else 0
            
            profit_factor = abs(sum(t.pnl for t in self.closed_positions if t.pnl > 0) / 
                              sum(abs(t.pnl) for t in self.closed_positions if t.pnl < 0)) if losing_trades > 0 else float('inf')
            
            # Calculate current balance
            current_balance = 10000  # Default
            if self.risk_manager and hasattr(self.risk_manager, 'account_balance'):
                current_balance = self.risk_manager.account_balance
            else:
                current_balance = 10000 + total_pnl
            
            return {
                'total_trades': total_trades,
                'open_positions': len(self.open_positions),
                'winning_trades': winning_trades,
                'losing_trades': losing_trades,
                'win_rate': round(win_rate, 2),
                'total_pnl': round(total_pnl, 2),
                'avg_win': round(avg_win, 2),
                'avg_loss': round(avg_loss, 2),
                'profit_factor': round(profit_factor, 2),
                'current_balance': round(current_balance, 2)
            }
    
    def get_open_positions(self) -> List[Dict]:
        """Get list of open positions"""
        with self.lock:
            return [t.to_dict() for t in self.open_positions.values()]
    
    def get_trade_history(self, limit: int = 50) -> List[Dict]:
        """Get recent trade history"""
        with self.lock:
            recent = sorted(self.closed_positions, key=lambda x: x.exit_time or datetime.min, reverse=True)[:limit]
            return [t.to_dict() for t in recent]

    def audit_position_health(self) -> Dict:
        """
        Audit open positions for common problems:
        - Stop-loss set too tight vs entry price (main Silver bug symptom)
        - Position open too long (stale)
        - Duplicate asset positions
        - Repeated stop-loss hits on same asset (pattern detection)
        Returns a report dict with actionable warnings.
        """
        from collections import Counter
        with self.lock:
            warnings_list = []
            asset_counts: Dict[str, int] = {}

            COMMODITY_PREFIXES = ('XAG', 'XAU', 'XPT', 'XPD', 'XCU',
                                  'WTI', 'NG/', 'GC=', 'SI=', 'CL=', 'NG=', 'HG=')

            for trade_id, trade in self.open_positions.items():
                # Duplicate asset tracking
                asset_counts[trade.asset] = asset_counts.get(trade.asset, 0) + 1

                # Stop-loss tightness check
                if trade.entry_price and trade.stop_loss:
                    sl_dist_pct = abs(trade.entry_price - trade.stop_loss) / trade.entry_price * 100
                    is_commodity = any(trade.asset.startswith(p) for p in COMMODITY_PREFIXES)
                    if is_commodity and sl_dist_pct < 0.8:
                        warnings_list.append(
                            f"STOP TOO TIGHT: {trade.asset} [{trade_id}] stop-loss only "
                            f"{sl_dist_pct:.2f}% from entry (commodity needs >=1.5%). "
                            f"Likely to stop-out on normal noise."
                        )

                # Stale position check (>6 hours)
                age_h = (datetime.now() - trade.entry_time).total_seconds() / 3600
                if age_h > 6:
                    warnings_list.append(
                        f"STALE POSITION: {trade.asset} [{trade_id}] open {age_h:.1f}h "
                        f"— consider manual review."
                    )

            # Duplicate positions on same asset
            for asset, count in asset_counts.items():
                if count > 1:
                    warnings_list.append(
                        f"DUPLICATE: {asset} has {count} open positions "
                        f"(possible double-signal execution)."
                    )

            # Repeated SL hits pattern detection (last 20 closed trades)
            recent_closed = sorted(
                self.closed_positions,
                key=lambda x: x.exit_time or datetime.min,
                reverse=True
            )[:20]

            sl_hits = [t for t in recent_closed if getattr(t, 'exit_reason', '') == "Stop Loss"]
            if len(sl_hits) >= 5:
                sl_asset_counts = Counter(t.asset for t in sl_hits)
                for asset, count in sl_asset_counts.most_common(3):
                    if count >= 3:
                        warnings_list.append(
                            f"REPEATED SL: {asset} hit stop-loss {count}x in last 20 trades. "
                            f"Stop may be too tight for asset volatility. "
                            f"Check get_stop_pct() in strategy_engine.py."
                        )

            return {
                'open_positions': len(self.open_positions),
                'warnings': warnings_list,
                'warning_count': len(warnings_list),
                'healthy': len(warnings_list) == 0,
                'timestamp': datetime.now().isoformat()
            }


if __name__ == "__main__":
    # Test the paper trader
    trader = PaperTrader()
    
    # Create a test signal
    test_signal = {
        'asset': 'BTC-USD',
        'category': 'crypto',
        'signal': 'BUY',
        'confidence': 0.85,
        'entry_price': 50000,
        'stop_loss': 49000,
        'take_profit_levels': [
            {'level': 1, 'price': 50750},
            {'level': 2, 'price': 51500},
            {'level': 3, 'price': 52500}
        ],
        'reason': 'Test signal'
    }
    
    # Execute trade
    result = trader.execute_signal(test_signal)
    logger.info(f"Trade executed: {result}")
    
    # Update positions (simulate price increase)
    trader.update_positions({'BTC-USD': 51000})
    
    # Check performance
    logger.info(f"Performance: {trader.get_performance()}")