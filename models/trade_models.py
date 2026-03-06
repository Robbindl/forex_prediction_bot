"""
SQLAlchemy models for trading bot
Each class here represents a table in your database
"""

from sqlalchemy import Column, BigInteger, String, Numeric, DateTime, Boolean, JSON, Integer, Index
from sqlalchemy.sql import func
from config.database import Base
import uuid

class Trade(Base):
    """Stores all your completed trades"""
    __tablename__ = 'trades'

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    trade_id = Column(String(20), unique=True, nullable=False, index=True)
    asset = Column(String(20), nullable=False, index=True)
    category = Column(String(20), nullable=False)
    direction = Column(String(4), nullable=False)  # BUY or SELL
    entry_price = Column(Numeric(20, 8), nullable=False)
    exit_price = Column(Numeric(20, 8))
    position_size = Column(Numeric(20, 8), nullable=False)
    stop_loss = Column(Numeric(20, 8), nullable=False)
    take_profit = Column(Numeric(20, 8))
    pnl = Column(Numeric(20, 8))
    pnl_percent = Column(Numeric(10, 4))
    entry_time = Column(DateTime(timezone=True), server_default=func.now(), index=True)
    exit_time = Column(DateTime(timezone=True))
    exit_reason = Column(String(20))  # take_profit, stop_loss, manual
    strategy_id = Column(String(20), nullable=False, index=True)
    confidence = Column(Numeric(5, 4))
    trade_metadata = Column(JSON)  # ✅ FIXED: renamed from 'metadata'

    def to_dict(self):
        """Convert trade to dictionary (useful for JSON responses)"""
        return {
            'trade_id': self.trade_id,
            'asset': self.asset,
            'category': self.category,
            'direction': self.direction,
            'entry_price': float(self.entry_price) if self.entry_price else None,
            'exit_price': float(self.exit_price) if self.exit_price else None,
            'position_size': float(self.position_size),
            'pnl': float(self.pnl) if self.pnl else None,
            'pnl_percent': float(self.pnl_percent) if self.pnl_percent else None,
            'entry_time': self.entry_time.isoformat() if self.entry_time else None,
            'exit_time': self.exit_time.isoformat() if self.exit_time else None,
            'exit_reason': self.exit_reason,
            'strategy_id': self.strategy_id,
            'confidence': float(self.confidence) if self.confidence else None,
            'metadata': self.trade_metadata  # Still return as 'metadata' in JSON
        }