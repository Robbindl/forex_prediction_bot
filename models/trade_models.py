"""
SQLAlchemy models for trading bot
Each class here represents a table in your database
"""

from sqlalchemy import Column, BigInteger, String, Numeric, DateTime, Boolean, JSON, Integer, Index, ForeignKey
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
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


# ===== NEW TABLES FOR BOT PERSONALITY & TRADING DIARY =====

class TradingDiary(Base):
    """Stores bot's memory of trades for personality/historical context"""
    __tablename__ = 'trading_diary'

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    asset = Column(String(20), nullable=False, index=True)
    trade_id = Column(String(20), ForeignKey('trades.trade_id'), nullable=True)
    setup_type = Column(String(50))  # 'breakout', 'pullback', 'rsi_oversold', etc.
    pnl = Column(Numeric(20, 8))
    exit_reason = Column(String(20))
    entry_price = Column(Numeric(20, 8))
    exit_price = Column(Numeric(20, 8))
    confidence = Column(Numeric(5, 4))
    
    # Technical context
    rsi_at_entry = Column(Numeric(10, 4))
    volume_ratio = Column(Numeric(10, 4))
    market_regime = Column(String(20))  # 'trending', 'ranging', 'volatile'
    
    # Metadata
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    notes = Column(JSON)  # Store any additional context
    
    # Relationship back to trade
    trade = relationship("Trade", backref="diary_entry")


class BotPersonality(Base):
    """Stores bot's personality traits and mood history"""
    __tablename__ = 'bot_personality'

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    
    # Personality traits (change slowly over time)
    base_confidence = Column(Numeric(5, 4), default=0.7)  # 0-1
    cautiousness = Column(Numeric(5, 4), default=0.5)      # 0-1
    optimism = Column(Numeric(5, 4), default=0.6)          # 0-1
    talkativeness = Column(Numeric(5, 4), default=0.7)     # 0-1
    
    # Current state
    current_mood = Column(String(20), default='neutral')
    mood_emoji = Column(String(10), default='😐')
    consecutive_wins = Column(Integer, default=0)
    consecutive_losses = Column(Integer, default=0)
    total_trades_remembered = Column(Integer, default=0)
    
    # Performance tracking
    last_10_wins = Column(Integer, default=0)
    last_10_pnl = Column(Numeric(20, 8), default=0)
    
    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    
    # Who am I?
    bot_name = Column(String(50), default='Robbie')


class MemorableMoments(Base):
    """Store memorable trading days (big wins/losses)"""
    __tablename__ = 'memorable_moments'

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    moment_date = Column(DateTime(timezone=True), nullable=False)
    title = Column(String(200))  # e.g., "The Great Bitcoin Pump of March 3rd"
    description = Column(String(500))
    asset = Column(String(20))
    pnl = Column(Numeric(20, 8))
    is_win = Column(Boolean)
    is_memorable = Column(Boolean, default=True)
    tags = Column(JSON)  # ['breakout', 'huge_volume', 'fomo']
    
    created_at = Column(DateTime(timezone=True), server_default=func.now())

class HumanExplanations(Base):
    """Cache of explanations sent to user"""
    __tablename__ = 'human_explanations'

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    asset = Column(String(20), nullable=False, index=True)
    explanation_text = Column(String(4000))
    direction = Column(String(4))
    confidence = Column(Numeric(5, 4))
    
    # What influenced this explanation
    rsi_value = Column(Numeric(10, 4))
    volume_value = Column(Numeric(20, 2))
    news_count = Column(Integer)
    sentiment_score = Column(Numeric(5, 4))
    
    # Who received it
    sent_to_telegram = Column(Boolean, default=False)
    telegram_chat_id = Column(String(50))
    
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    
    # Index for quick lookups
    __table_args__ = (
        Index('idx_explanations_asset_date', 'asset', 'created_at'),
    )

class WhaleAlert(Base):
    """Store whale alerts permanently in database"""
    __tablename__ = 'whale_alerts'

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    title = Column(String(500), nullable=False)
    symbol = Column(String(20), nullable=False, index=True)
    value_usd = Column(Numeric(20, 2), nullable=False, index=True)
    source = Column(String(100), nullable=False)
    alert_time = Column(DateTime(timezone=True), nullable=False, index=True)
    
    def to_dict(self):
        return {
            'title': self.title,
            'symbol': self.symbol,
            'value_usd': float(self.value_usd),
            'value_millions': float(self.value_usd) / 1_000_000,
            'source': self.source,
            'alert_time': self.alert_time.isoformat() if self.alert_time else None
        }