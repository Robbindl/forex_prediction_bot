"""
Refactored trading system with proper error handling, logging, and types
"""
from typing import Optional, List, Dict, Any
import threading
import time
from datetime import datetime

from config import config, Config
from logger import logger
from error_handling import retry, safe_execute, APIErrorHandler, CircuitBreaker
from trading_types import (
    TradeSignal, TradeResult, PerformanceMetrics,
    TradeDirection, ExitReason, StrategyMode, AssetCategory
)


class RefactoredTradingSystem:
    """Trading system with improved error handling and logging"""
    
    def __init__(self, config: Optional[Config] = None):
        self.config = config or Config()
        self.logger = logger
        self.is_running = False
        
        # Error handlers
        self.api_handler = APIErrorHandler("market_data")
        self.trade_circuit = CircuitBreaker(name="trading", failure_threshold=3)
        
        # State
        self.open_positions: Dict[str, TradeResult] = {}
        self.closed_trades: List[TradeResult] = []
        self.performance = PerformanceMetrics()
        
        self.logger.info("Trading system initialized", 
                        extra={"config": self.config.trading})
    
    @safe_execute(error_message="Failed to fetch market data", default_value=None)
    def fetch_market_data(self, asset: str, timeframe: str = '15m', bars: int = 100) -> Optional[Any]:
        """Fetch market data with error handling"""
        self.logger.debug(f"Fetching {timeframe} data for {asset}")
        
        # Use API handler with retry
        data = self.api_handler.call_api(
            self._fetch_from_source,
            asset, timeframe, bars
        )
        
        if data is None or data.empty:
            self.logger.warning(f"No data received for {asset}")
            return None
        
        self.logger.debug(f"Received {len(data)} bars for {asset}")
        return data
    
    @retry(max_retries=3, base_delay=2.0, retry_on=(ConnectionError, TimeoutError))
    def _fetch_from_source(self, asset: str, timeframe: str, bars: int) -> Any:
        """Actual data fetching with retry logic"""
        # Your actual data fetching logic here
        # This is just a placeholder
        import pandas as pd
        df = pd.DataFrame()  # Replace with actual API call
        return df
    
    @safe_execute(error_message="Failed to generate signal", default_value=None)
    def generate_signal(
        self,
        asset: str,
        strategy_mode: StrategyMode = 'balanced'
    ) -> Optional[TradeSignal]:
        """Generate trading signal with proper error handling"""
        self.logger.info(f"Generating signal for {asset} using {strategy_mode} strategy")
        
        try:
            # Fetch data
            df_15m = self.fetch_market_data(asset, '15m')
            df_1h = self.fetch_market_data(asset, '1h')
            
            if df_15m is None or df_1h is None:
                return None
            
            # Select strategy
            if strategy_mode == 'fast':
                signal_dict = self.fast_strategy(df_15m, df_1h)
                strategy_emoji = "⚡"
            elif strategy_mode == 'strict':
                signal_dict = self.strict_strategy(df_15m, df_1h)
                strategy_emoji = "🔒"
            else:
                signal_dict = self.balanced_strategy(df_15m, df_1h)
                strategy_emoji = "⚖️"
            
            if not signal_dict:
                return None
            
            # Create typed signal
            signal = TradeSignal(
                asset=asset,
                direction=TradeDirection(signal_dict.get('signal', 'HOLD')),
                entry_price=signal_dict.get('entry_price', 0.0),
                stop_loss=signal_dict.get('stop_loss', 0.0),
                take_profit=signal_dict.get('take_profit'),
                confidence=signal_dict.get('confidence', 0.5),
                reason=signal_dict.get('reason', 'Signal generated'),
                strategy_id=strategy_mode.upper(),
                strategy_emoji=strategy_emoji,
                category=self._get_asset_category(asset)
            )
            
            self.logger.trade(
                f"Signal generated for {asset}",
                direction=signal.direction.value,
                confidence=signal.confidence,
                strategy=signal.strategy_id
            )
            
            return signal
            
        except Exception as e:
            self.logger.error(f"Error generating signal for {asset}: {e}", exc_info=True)
            return None
    
    def execute_trade(self, signal: TradeSignal) -> Optional[TradeResult]:
        """Execute a trade with circuit breaker protection"""
        
        # Check circuit breaker
        if not self.trade_circuit.can_execute():
            self.logger.warning("Trading circuit is OPEN, skipping execution")
            return None
        
        try:
            # Check max positions
            if len(self.open_positions) >= self.config.trading['max_positions']:
                self.logger.info(f"Max positions reached, skipping {signal.asset}")
                return None
            
            # Calculate position size
            position_size = self._calculate_position_size(signal)
            
            if position_size <= 0:
                self.logger.warning(f"Invalid position size for {signal.asset}")
                return None
            
            # Create trade
            trade = TradeResult(
                trade_id=self._generate_trade_id(),
                asset=signal.asset,
                direction=signal.direction,
                entry_price=signal.entry_price,
                position_size=position_size,
                stop_loss=signal.stop_loss,
                take_profit=signal.take_profit,
                strategy_id=signal.strategy_id,
                strategy_emoji=signal.strategy_emoji,
                category=signal.category
            )
            
            # Store trade
            self.open_positions[trade.trade_id] = trade
            
            # Record success in circuit breaker
            self.trade_circuit.record_success()
            
            self.logger.trade(
                f"Trade executed: {signal.asset} {signal.direction}",
                trade_id=trade.trade_id,
                entry_price=trade.entry_price,
                position_size=position_size,
                strategy=signal.strategy_id
            )
            
            return trade
            
        except Exception as e:
            self.logger.error(f"Failed to execute trade: {e}", exc_info=True)
            self.trade_circuit.record_failure()
            return None
    
    def _calculate_position_size(self, signal: TradeSignal) -> float:
        """Calculate position size based on risk"""
        try:
            account_balance = self.config.trading['default_balance']
            risk_per_trade = self.config.risk['risk_per_trade']
            
            risk_amount = account_balance * risk_per_trade
            
            # Calculate price difference for risk
            if signal.direction == TradeDirection.LONG:
                price_diff = signal.entry_price - signal.stop_loss
            else:
                price_diff = signal.stop_loss - signal.entry_price
            
            if price_diff <= 0:
                return 0
            
            position_size = risk_amount / price_diff
            
            # Apply position limits
            max_position_value = account_balance * 0.5
            position_value = position_size * signal.entry_price
            
            if position_value > max_position_value:
                position_size = max_position_value / signal.entry_price
            
            return position_size
            
        except Exception as e:
            self.logger.error(f"Position size calculation failed: {e}")
            return 0
    
    def update_positions(self, current_prices: Dict[str, float]) -> None:
        """Update all open positions with current prices"""
        to_remove = []
        
        for trade_id, trade in self.open_positions.items():
            current_price = current_prices.get(trade.asset)
            
            if not current_price:
                continue
            
            # Check stop loss / take profit
            exit_signal = self._check_exit_conditions(trade, current_price)
            
            if exit_signal:
                self._close_trade(trade, current_price, exit_signal)
                to_remove.append(trade_id)
        
        # Remove closed trades
        for trade_id in to_remove:
            del self.open_positions[trade_id]
    
    def _check_exit_conditions(self, trade: TradeResult, current_price: float) -> Optional[ExitReason]:
        """Check if trade should exit"""
        
        if trade.direction == TradeDirection.LONG:
            if current_price <= trade.stop_loss:
                return ExitReason.STOP_LOSS
            if trade.take_profit and current_price >= trade.take_profit:
                return ExitReason.TAKE_PROFIT
        else:  # SHORT
            if current_price >= trade.stop_loss:
                return ExitReason.STOP_LOSS
            if trade.take_profit and current_price <= trade.take_profit:
                return ExitReason.TAKE_PROFIT
        
        return None
    
    def _close_trade(self, trade: TradeResult, exit_price: float, reason: ExitReason) -> None:
        """Close a trade and record results"""
        trade.exit_price = exit_price
        trade.exit_time = datetime.now()
        trade.exit_reason = reason
        
        # Calculate P&L
        if trade.direction == TradeDirection.LONG:
            trade.pnl = (exit_price - trade.entry_price) * trade.position_size
            trade.pnl_percent = ((exit_price - trade.entry_price) / trade.entry_price) * 100
        else:
            trade.pnl = (trade.entry_price - exit_price) * trade.position_size
            trade.pnl_percent = ((trade.entry_price - exit_price) / trade.entry_price) * 100
        
        # Add to closed trades
        self.closed_trades.append(trade)
        
        # Update performance metrics
        self._update_performance(trade)
        
        self.logger.trade(
            f"Trade closed: {trade.asset}",
            trade_id=trade.trade_id,
            exit_price=exit_price,
            pnl=f"${trade.pnl:.2f}",
            pnl_percent=f"{trade.pnl_percent:.2f}%",
            reason=str(reason)
        )
    
    def _update_performance(self, trade: TradeResult) -> None:
        """Update performance metrics"""
        self.performance.total_trades += 1
        
        if trade.is_win:
            self.performance.winning_trades += 1
        else:
            self.performance.losing_trades += 1
        
        self.performance.total_pnl += trade.pnl
        self.performance.update_win_rate()
        
        # Update strategy stats
        if trade.strategy_id not in self.performance.strategy_stats:
            self.performance.strategy_stats[trade.strategy_id] = {
                'trades': 0, 'wins': 0, 'pnl': 0.0
            }
        
        stats = self.performance.strategy_stats[trade.strategy_id]
        stats['trades'] += 1
        stats['pnl'] += trade.pnl
        if trade.is_win:
            stats['wins'] += 1
    
    def get_performance(self) -> PerformanceMetrics:
        """Get current performance metrics"""
        self.performance.open_positions = len(self.open_positions)
        return self.performance
    
    def _get_asset_category(self, asset: str) -> AssetCategory:
        """Get asset category"""
        if asset in self.config.assets['major']:
            return 'major'
        elif asset in self.config.assets['minor']:
            return 'minor'
        else:
            return 'exotic'
    
    def _generate_trade_id(self) -> str:
        """Generate unique trade ID"""
        import uuid
        return str(uuid.uuid4())[:8]
    
    # Placeholder strategy methods
    def fast_strategy(self, df_15m, df_1h) -> Optional[Dict]:
        """Fast strategy implementation"""
        # Your strategy logic here
        return None
    
    def balanced_strategy(self, df_15m, df_1h) -> Optional[Dict]:
        """Balanced strategy implementation"""
        # Your strategy logic here
        return None
    
    def strict_strategy(self, df_15m, df_1h) -> Optional[Dict]:
        """Strict strategy implementation"""
        # Your strategy logic here
        return None


# Example usage
if __name__ == "__main__":
    # Initialize with config
    system = RefactoredTradingSystem()
    
    # Generate signal
    signal = system.generate_signal("EUR/USD", "fast")
    
    if signal:
        # Execute trade
        trade = system.execute_trade(signal)
        
        if trade:
            print(f"✅ Trade executed: {trade.trade_id}")
    
    # Get performance
    perf = system.get_performance()
    print(f"📊 Performance: {perf.total_trades} trades, {perf.win_rate:.1f}% win rate")