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


class PaperTrade:
    """Individual paper trade record"""
    
    def __init__(self, asset: str, category: str, signal_type: str, 
                 entry_price: float, position_size: float, stop_loss: float,
                 take_profit_levels: List[Dict], confidence: float, reason: str,
                 strategy_id: str = "UNKNOWN", strategy_emoji: str = "🤖"):
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
            'strategy_emoji': self.strategy_emoji
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
    
    def __init__(self, risk_manager=None, history_file: str = "paper_trades.json"):
        self.risk_manager = risk_manager
        self.history_file = history_file
        self.open_positions: Dict[str, PaperTrade] = {}
        self.closed_positions: List[PaperTrade] = []
        self.lock = threading.RLock()
        self.monitor = None  # Will be set by trading system
        
        # Load history if exists
        self._load_history()
        
        print(" Paper Trader Initialized")
        print(f"   • Open Positions: {len(self.open_positions)}")
        print(f"   • Historical Trades: {len(self.closed_positions)}")
    
    def _load_history(self):
        """Load trade history from file"""
        try:
            if os.path.exists(self.history_file):
                with open(self.history_file, 'r') as f:
                    data = json.load(f)
                    
                    # Load closed positions (simplified - would need proper deserialization)
                    if 'closed_positions' in data:
                        print(f"[OK] Loaded {len(data['closed_positions'])} trades from history")
                        
        except Exception as e:
            print(f" Could not load trade history: {e}")
    
    def _save_history(self):
        """Save trade history to file"""
        try:
            history = {
                'open_positions': [p.to_dict() for p in self.open_positions.values()],
                'closed_positions': [p.to_dict() for p in self.closed_positions]
            }
            with open(self.history_file, 'w') as f:
                json.dump(history, f, indent=2, default=str)
        except Exception as e:
            print(f" Could not save trade history: {e}")
    
    def execute_signal(self, signal: Dict) -> Optional[Dict]:
        """
        Execute a trading signal (paper trade)
        Returns trade execution details or None if rejected
        """
        with self.lock:
            # Skip HOLD signals
            if signal.get('signal') in ['HOLD', 'CLOSED']:
                return None
            
            # Check risk manager if available
            if self.risk_manager:
                # Check max positions
                if hasattr(self.risk_manager, 'max_positions'):
                    if len(self.open_positions) >= self.risk_manager.max_positions:
                        print(f" Skipping {signal['asset']}: Max positions reached")
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
                print(f" Skipping {signal['asset']}: Invalid position size")
                return None
            
            # Create paper trade
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
                strategy_emoji=signal.get('strategy_emoji', '🤖')
            )
            
            # Store position
            self.open_positions[trade.trade_id] = trade
            
            # Update risk manager trade count if available
            if self.risk_manager and hasattr(self.risk_manager, 'increment_trades'):
                self.risk_manager.increment_trades()
            
            print(f" EXECUTED: {signal['asset']} {signal['signal']}")
            print(f"   • Size: {position_size:.4f} units")
            print(f"   • Risk: ${risk_amount:.2f}")
            print(f"   • Trade ID: {trade.trade_id}")
            
            # ===== SEND NEW TRADE ALERT =====
            if hasattr(self, 'monitor') and self.monitor:
                try:
                    # Pass the original signal which has ALL the strategy info
                    self.monitor.on_new_trade(signal)
                    print(f"   📱 Trade alert sent! [{signal.get('strategy_id', 'UNKNOWN')} {signal.get('strategy_emoji', '🤖')}]")
                except Exception as e:
                    print(f"    Could not send alert: {e}")
            
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
        Update open positions with current prices
        Check for stop loss hits and take profit targets
        """
        with self.lock:
            to_close = []
            
            for trade_id, trade in self.open_positions.items():
                if trade.asset not in current_prices:
                    continue
                
                current_price = current_prices[trade.asset]
                
                # Check stop loss
                if trade.signal_type == 'BUY':
                    if current_price <= trade.stop_loss:
                        trade.exit_price = trade.stop_loss
                        trade.exit_reason = "Stop Loss"
                        trade.status = "CLOSED"
                        to_close.append(trade_id)
                    
                    # Check take profit levels
                    for tp in trade.take_profit_levels:
                        if current_price >= tp['price']:
                            trade.exit_price = tp['price']
                            trade.exit_reason = f"Take Profit {tp['level']}"
                            trade.status = "CLOSED"
                            to_close.append(trade_id)
                            break
                
                elif trade.signal_type == 'SELL':
                    if current_price >= trade.stop_loss:
                        trade.exit_price = trade.stop_loss
                        trade.exit_reason = "Stop Loss"
                        trade.status = "CLOSED"
                        to_close.append(trade_id)
                    
                    # Check take profit levels
                    for tp in trade.take_profit_levels:
                        if current_price <= tp['price']:
                            trade.exit_price = tp['price']
                            trade.exit_reason = f"Take Profit {tp['level']}"
                            trade.status = "CLOSED"
                            to_close.append(trade_id)
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
                
                # Update risk manager if available
                if self.risk_manager and hasattr(self.risk_manager, 'update_pnl'):
                    self.risk_manager.update_pnl(trade.pnl)
                
                # Add to closed positions
                self.closed_positions.append(trade)
                
                print(f" CLOSED: {trade.asset} - {trade.exit_reason}")
                print(f"   • P&L: ${trade.pnl:.2f} ({trade.pnl_percent:.2f}%)")
                print(f"   • Trade ID: {trade.trade_id}")
                
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
                        print("    P&L alert sent!")
                    except Exception as e:
                        print(f"    Could not send alert: {e}")
            
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
    print(f"\nTrade executed: {result}")
    
    # Update positions (simulate price increase)
    trader.update_positions({'BTC-USD': 51000})
    
    # Check performance
    print(f"\nPerformance: {trader.get_performance()}")