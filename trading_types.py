"""
Type definitions and data classes for the trading bot
"""
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any, Union, Literal
from datetime import datetime
from enum import Enum


# Type aliases
SignalType = Literal['BUY', 'SELL', 'HOLD', 'CLOSED']
StrategyMode = Literal['fast', 'balanced', 'strict']
TimeFrame = Literal['15m', '1h', '4h', '1d']
AssetCategory = Literal['major', 'minor', 'exotic']


class TradeDirection(Enum):
    """Trade direction enumeration"""
    LONG = "BUY"
    SHORT = "SELL"
    
    def __str__(self):
        return self.value


class ExitReason(Enum):
    """Exit reason enumeration"""
    TAKE_PROFIT = "take_profit"
    STOP_LOSS = "stop_loss"
    TRAILING_STOP = "trailing_stop"
    SIGNAL_REVERSAL = "signal_reversal"
    MANUAL = "manual"
    TIME_EXIT = "time_exit"
    
    def __str__(self):
        return self.value


@dataclass
class TradeSignal:
    """Trading signal data class"""
    asset: str
    direction: TradeDirection
    entry_price: float
    stop_loss: float
    take_profit: Optional[float] = None
    take_profit_levels: List[float] = field(default_factory=list)
    confidence: float = 0.5
    reason: str = ""
    strategy_id: str = "UNKNOWN"
    strategy_emoji: str = "🤖"
    category: AssetCategory = "major"
    timestamp: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        """Validate after initialization"""
        if not self.take_profit and not self.take_profit_levels:
            # Default take profit (1% for long, -1% for short)
            if self.direction == TradeDirection.LONG:
                self.take_profit = self.entry_price * 1.01
            else:
                self.take_profit = self.entry_price * 0.99
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'asset': self.asset,
            'signal': self.direction.value,
            'entry_price': self.entry_price,
            'stop_loss': self.stop_loss,
            'take_profit': self.take_profit,
            'take_profit_levels': self.take_profit_levels,
            'confidence': self.confidence,
            'reason': self.reason,
            'strategy_id': self.strategy_id,
            'strategy_emoji': self.strategy_emoji,
            'category': self.category,
            'timestamp': self.timestamp.isoformat(),
            'metadata': self.metadata
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TradeSignal':
        """Create from dictionary"""
        return cls(
            asset=data['asset'],
            direction=TradeDirection(data.get('signal', 'BUY')),
            entry_price=data['entry_price'],
            stop_loss=data['stop_loss'],
            take_profit=data.get('take_profit'),
            take_profit_levels=data.get('take_profit_levels', []),
            confidence=data.get('confidence', 0.5),
            reason=data.get('reason', ''),
            strategy_id=data.get('strategy_id', 'UNKNOWN'),
            strategy_emoji=data.get('strategy_emoji', '🤖'),
            category=data.get('category', 'major'),
            timestamp=datetime.fromisoformat(data['timestamp']) if 'timestamp' in data else datetime.now(),
            metadata=data.get('metadata', {})
        )


@dataclass
class TradeResult:
    """Trade result data class"""
    trade_id: str
    asset: str
    direction: TradeDirection
    entry_price: float
    exit_price: Optional[float] = None
    position_size: float = 0.0
    stop_loss: float = 0.0
    take_profit: Optional[float] = None
    entry_time: datetime = field(default_factory=datetime.now)
    exit_time: Optional[datetime] = None
    pnl: float = 0.0
    pnl_percent: float = 0.0
    exit_reason: Optional[ExitReason] = None
    strategy_id: str = "UNKNOWN"
    strategy_emoji: str = "🤖"
    category: AssetCategory = "major"
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def duration(self) -> Optional[float]:
        """Get trade duration in minutes"""
        if self.exit_time and self.entry_time:
            delta = self.exit_time - self.entry_time
            return delta.total_seconds() / 60
        return None
    
    @property
    def is_win(self) -> bool:
        """Check if trade is winning"""
        if self.direction == TradeDirection.LONG:
            return self.pnl > 0
        else:  # SHORT
            return self.pnl > 0  # P&L already accounts for direction
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'trade_id': self.trade_id,
            'asset': self.asset,
            'direction': self.direction.value,
            'entry_price': self.entry_price,
            'exit_price': self.exit_price,
            'position_size': self.position_size,
            'stop_loss': self.stop_loss,
            'take_profit': self.take_profit,
            'entry_time': self.entry_time.isoformat(),
            'exit_time': self.exit_time.isoformat() if self.exit_time else None,
            'pnl': self.pnl,
            'pnl_percent': self.pnl_percent,
            'exit_reason': str(self.exit_reason) if self.exit_reason else None,
            'strategy_id': self.strategy_id,
            'strategy_emoji': self.strategy_emoji,
            'category': self.category,
            'metadata': self.metadata
        }


@dataclass
class PerformanceMetrics:
    """Performance metrics data class"""
    total_trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    total_pnl: float = 0.0
    total_pnl_percent: float = 0.0
    win_rate: float = 0.0
    profit_factor: float = 0.0
    avg_win: float = 0.0
    avg_loss: float = 0.0
    max_drawdown: float = 0.0
    max_drawdown_percent: float = 0.0
    sharpe_ratio: float = 0.0
    sortino_ratio: float = 0.0
    current_balance: float = 0.0
    open_positions: int = 0
    strategy_stats: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    
    def update_win_rate(self) -> None:
        """Calculate win rate"""
        if self.total_trades > 0:
            self.win_rate = (self.winning_trades / self.total_trades) * 100
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'total_trades': self.total_trades,
            'winning_trades': self.winning_trades,
            'losing_trades': self.losing_trades,
            'total_pnl': self.total_pnl,
            'total_pnl_percent': self.total_pnl_percent,
            'win_rate': self.win_rate,
            'profit_factor': self.profit_factor,
            'avg_win': self.avg_win,
            'avg_loss': self.avg_loss,
            'max_drawdown': self.max_drawdown,
            'max_drawdown_percent': self.max_drawdown_percent,
            'sharpe_ratio': self.sharpe_ratio,
            'sortino_ratio': self.sortino_ratio,
            'current_balance': self.current_balance,
            'open_positions': self.open_positions,
            'strategy_stats': self.strategy_stats
        }


@dataclass
class MarketData:
    """Market data container"""
    asset: str
    timeframe: TimeFrame
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: Optional[float] = None
    indicators: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def spread(self) -> float:
        """Calculate spread"""
        return self.high - self.low
    
    @property
    def body(self) -> float:
        """Calculate candle body"""
        return abs(self.close - self.open)
    
    @property
    def is_bullish(self) -> bool:
        """Check if candle is bullish"""
        return self.close > self.open