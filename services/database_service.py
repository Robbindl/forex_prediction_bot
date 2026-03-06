"""Database service - what your bot uses to save trades"""

from sqlalchemy.orm import Session
from sqlalchemy import func, desc
from datetime import datetime, timedelta
import uuid
import numpy as np
from sqlalchemy import text

# Fix imports
from models.trade_models import Trade
from config.database import SessionLocal

def convert_numpy(value):
    """Convert numpy types to Python native types for database storage"""
    if value is None:
        return None
    if isinstance(value, np.floating):
        return float(value)
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, np.bool_):
        return bool(value)
    return value

class DatabaseService:
    """Handles all database operations for your bot"""
    
    def __init__(self):
        """Create a new database session"""
        self.session = SessionLocal()
    
    def save_trade(self, trade_data):
        """
        Save a trade to the database
        
        Args:
            trade_data: Dictionary with trade information
        """
        # Convert any numpy values to Python native types
        trade = Trade(
            trade_id=str(convert_numpy(trade_data.get('trade_id', str(uuid.uuid4())[:8]))),
            asset=str(convert_numpy(trade_data.get('asset', 'UNKNOWN'))),
            category=str(convert_numpy(trade_data.get('category', 'unknown'))),
            direction=str(convert_numpy(trade_data.get('signal', 'HOLD'))),
            entry_price=convert_numpy(trade_data.get('entry_price', 0)),
            exit_price=convert_numpy(trade_data.get('exit_price')),
            position_size=convert_numpy(trade_data.get('position_size', 0)),
            stop_loss=convert_numpy(trade_data.get('stop_loss', 0)),
            take_profit=convert_numpy(trade_data.get('take_profit')),
            pnl=convert_numpy(trade_data.get('pnl')),
            pnl_percent=convert_numpy(trade_data.get('pnl_percent')),
            exit_time=datetime.fromisoformat(trade_data['exit_time']) if trade_data.get('exit_time') else None,
            exit_reason=str(convert_numpy(trade_data.get('exit_reason'))) if trade_data.get('exit_reason') else None,
            strategy_id=str(convert_numpy(trade_data.get('strategy_id', 'UNKNOWN'))),
            confidence=convert_numpy(trade_data.get('confidence', 0)),
            trade_metadata=trade_data.get('metadata', {})
        )
        
        # Add to database and save
        self.session.add(trade)
        self.session.commit()
        return trade.trade_id
    
    def update_trade_exit(self, trade_id, exit_data):
        """Update a trade when it closes"""
        trade = self.session.query(Trade).filter(Trade.trade_id == trade_id).first()
        if trade:
            trade.exit_price = convert_numpy(exit_data.get('exit_price'))
            trade.exit_time = datetime.now()
            trade.exit_reason = str(convert_numpy(exit_data.get('exit_reason'))) if exit_data.get('exit_reason') else None
            trade.pnl = convert_numpy(exit_data.get('pnl'))
            trade.pnl_percent = convert_numpy(exit_data.get('pnl_percent'))
            self.session.commit()
            return True
        return False
    
    def get_recent_trades(self, limit=20):
        """Get the most recent trades"""
        trades = self.session.query(Trade).order_by(desc(Trade.entry_time)).limit(limit).all()
        return [t.to_dict() for t in trades]
    
    def get_performance_summary(self, days=30):
        """Get performance for the last N days"""
        cutoff = datetime.now() - timedelta(days=days)
        
        trades = self.session.query(Trade).filter(
            Trade.entry_time >= cutoff,
            Trade.exit_time.isnot(None)  # Only closed trades
        ).all()
        
        if not trades:
            return {}
        
        total_trades = len(trades)
        winning_trades = [t for t in trades if t.pnl and t.pnl > 0]
        
        total_pnl = sum(float(t.pnl) for t in trades if t.pnl)
        
        return {
            'period_days': days,
            'total_trades': total_trades,
            'winning_trades': len(winning_trades),
            'win_rate': len(winning_trades) / total_trades if total_trades > 0 else 0,
            'total_pnl': total_pnl,
            'avg_pnl': total_pnl / total_trades if total_trades > 0 else 0
        }
    
    def close(self):
        """Close the database session (call this when done)"""
        self.session.close()