"""
models/trade_models.py — All SQLAlchemy ORM models.
Added: OpenPosition (for restart recovery), DailyStats.
Fixed imports to use config/database.py Base.
"""
from __future__ import annotations
import uuid
from sqlalchemy import (
    BigInteger, Boolean, Column, DateTime, Index,
    Integer, JSON, Numeric, String, ForeignKey, Text,
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from config.database import Base


class Trade(Base):
    """Every closed trade — permanent record."""
    __tablename__ = "trades"

    id            = Column(BigInteger, primary_key=True, autoincrement=True)
    trade_id      = Column(String(20),  unique=True, nullable=False, index=True)
    asset         = Column(String(20),  nullable=False, index=True)
    category      = Column(String(20),  nullable=False)
    direction     = Column(String(4),   nullable=False)       # BUY | SELL
    entry_price   = Column(Numeric(20, 8), nullable=False)
    exit_price    = Column(Numeric(20, 8))
    position_size = Column(Numeric(20, 8), nullable=False)
    stop_loss     = Column(Numeric(20, 8), nullable=False)
    take_profit   = Column(Numeric(20, 8))
    pnl           = Column(Numeric(20, 8))
    pnl_percent   = Column(Numeric(10, 4))
    entry_time    = Column(DateTime(timezone=True), server_default=func.now(), index=True)
    exit_time     = Column(DateTime(timezone=True))
    exit_reason   = Column(String(50))
    strategy_id   = Column(String(30), nullable=False, index=True)
    confidence    = Column(Numeric(5, 4))
    canonical_asset = Column(String(30))
    trade_metadata  = Column(JSON)

    diary_entry = relationship("TradingDiary", back_populates="trade", uselist=False)

    def to_dict(self) -> dict:
        return {
            "trade_id":       self.trade_id,
            "asset":          self.asset,
            "canonical_asset":self.canonical_asset,
            "category":       self.category,
            "direction":      self.direction,
            "entry_price":    float(self.entry_price)   if self.entry_price   else None,
            "exit_price":     float(self.exit_price)    if self.exit_price    else None,
            "position_size":  float(self.position_size) if self.position_size else None,
            "stop_loss":      float(self.stop_loss)     if self.stop_loss     else None,
            "take_profit":    float(self.take_profit)   if self.take_profit   else None,
            "pnl":            float(self.pnl)           if self.pnl           else None,
            "pnl_percent":    float(self.pnl_percent)   if self.pnl_percent   else None,
            "entry_time":     self.entry_time.isoformat() if self.entry_time  else None,
            "exit_time":      self.exit_time.isoformat()  if self.exit_time   else None,
            "exit_reason":    self.exit_reason,
            "strategy_id":    self.strategy_id,
            "confidence":     float(self.confidence)    if self.confidence    else None,
            "metadata":       self.trade_metadata,
        }


class OpenPosition(Base):
    """
    Snapshot of currently open positions.
    Written on OPEN, deleted on CLOSE.
    Used for restart recovery so SL/TP monitoring resumes immediately.
    """
    __tablename__ = "open_positions"

    id            = Column(BigInteger, primary_key=True, autoincrement=True)
    trade_id      = Column(String(20), unique=True, nullable=False, index=True)
    asset         = Column(String(20), nullable=False)
    canonical_asset = Column(String(30))
    category      = Column(String(20), nullable=False)
    direction     = Column(String(4),  nullable=False)
    entry_price   = Column(Numeric(20, 8), nullable=False)
    stop_loss     = Column(Numeric(20, 8), nullable=False)
    take_profit   = Column(Numeric(20, 8))
    position_size = Column(Numeric(20, 8), nullable=False)
    confidence    = Column(Numeric(5, 4))
    strategy_id   = Column(String(30))
    open_time     = Column(DateTime(timezone=True), server_default=func.now())
    position_data = Column(JSON)   # full position dict for lossless restore

    def to_dict(self) -> dict:
        base = self.position_data or {}
        base.update({
            "trade_id":       self.trade_id,
            "asset":          self.asset,
            "canonical_asset":self.canonical_asset,
            "category":       self.category,
            "direction":      self.direction,
            "entry_price":    float(self.entry_price)   if self.entry_price   else 0,
            "stop_loss":      float(self.stop_loss)     if self.stop_loss     else 0,
            "take_profit":    float(self.take_profit)   if self.take_profit   else 0,
            "position_size":  float(self.position_size) if self.position_size else 0,
            "confidence":     float(self.confidence)    if self.confidence    else 0,
            "strategy_id":    self.strategy_id or "",
            "open_time":      self.open_time.isoformat() if self.open_time else "",
        })
        return base


class DailyStats(Base):
    """One row per calendar day — daily P&L and trade count."""
    __tablename__ = "daily_stats"

    id           = Column(BigInteger, primary_key=True, autoincrement=True)
    date         = Column(String(10), unique=True, nullable=False, index=True)  # YYYY-MM-DD
    trade_count  = Column(Integer, default=0)
    pnl          = Column(Numeric(20, 8), default=0)
    balance_end  = Column(Numeric(20, 8))
    updated_at   = Column(DateTime(timezone=True), onupdate=func.now())


class TradingDiary(Base):
    """Bot memory — context per trade for personality system."""
    __tablename__ = "trading_diary"

    id           = Column(BigInteger, primary_key=True, autoincrement=True)
    asset        = Column(String(20), nullable=False, index=True)
    trade_id     = Column(String(20), ForeignKey("trades.trade_id"), nullable=True)
    setup_type   = Column(String(50))
    pnl          = Column(Numeric(20, 8))
    exit_reason  = Column(String(20))
    entry_price  = Column(Numeric(20, 8))
    exit_price   = Column(Numeric(20, 8))
    confidence   = Column(Numeric(5, 4))
    rsi_at_entry = Column(Numeric(10, 4))
    volume_ratio = Column(Numeric(10, 4))
    market_regime= Column(String(20))
    created_at   = Column(DateTime(timezone=True), server_default=func.now())
    notes        = Column(JSON)

    trade = relationship("Trade", back_populates="diary_entry")


class BotPersonality(Base):
    __tablename__ = "bot_personality"

    id                      = Column(BigInteger, primary_key=True, autoincrement=True)
    base_confidence         = Column(Numeric(5, 4), default=0.7)
    cautiousness            = Column(Numeric(5, 4), default=0.5)
    optimism                = Column(Numeric(5, 4), default=0.6)
    talkativeness           = Column(Numeric(5, 4), default=0.7)
    current_mood            = Column(String(20),   default="neutral")
    mood_emoji              = Column(String(10),   default="😐")
    consecutive_wins        = Column(Integer,      default=0)
    consecutive_losses      = Column(Integer,      default=0)
    total_trades_remembered = Column(Integer,      default=0)
    last_10_wins            = Column(Integer,      default=0)
    last_10_pnl             = Column(Numeric(20, 8), default=0)
    bot_name                = Column(String(50),   default="Robbie")
    created_at              = Column(DateTime(timezone=True), server_default=func.now())
    updated_at              = Column(DateTime(timezone=True), onupdate=func.now())


class MemorableMoments(Base):
    __tablename__ = "memorable_moments"

    id           = Column(BigInteger, primary_key=True, autoincrement=True)
    moment_date  = Column(DateTime(timezone=True), nullable=False)
    title        = Column(String(200))
    description  = Column(String(500))
    asset        = Column(String(20))
    pnl          = Column(Numeric(20, 8))
    is_win       = Column(Boolean)
    is_memorable = Column(Boolean, default=True)
    tags         = Column(JSON)
    created_at   = Column(DateTime(timezone=True), server_default=func.now())


class HumanExplanations(Base):
    __tablename__ = "human_explanations"

    id               = Column(BigInteger, primary_key=True, autoincrement=True)
    asset            = Column(String(20), nullable=False, index=True)
    explanation_text = Column(Text)
    direction        = Column(String(4))
    confidence       = Column(Numeric(5, 4))
    rsi_value        = Column(Numeric(10, 4))
    volume_value     = Column(Numeric(20, 2))
    news_count       = Column(Integer)
    sentiment_score  = Column(Numeric(5, 4))
    sent_to_telegram = Column(Boolean, default=False)
    telegram_chat_id = Column(String(50))
    created_at       = Column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        Index("idx_explanations_asset_date", "asset", "created_at"),
    )


class WhaleAlert(Base):
    __tablename__ = "whale_alerts"

    id         = Column(BigInteger, primary_key=True, autoincrement=True)
    title      = Column(String(500), nullable=False)
    symbol     = Column(String(20),  nullable=False, index=True)
    value_usd  = Column(Numeric(20, 2), nullable=False, index=True)
    source     = Column(String(100), nullable=False)
    direction  = Column(String(4))
    alert_time = Column(DateTime(timezone=True), nullable=False, index=True)

    def to_dict(self) -> dict:
        return {
            "title":          self.title,
            "symbol":         self.symbol,
            "value_usd":      float(self.value_usd),
            "value_millions": float(self.value_usd) / 1_000_000,
            "source":         self.source,
            "direction":      self.direction,
            "alert_time":     self.alert_time.isoformat() if self.alert_time else None,
        }