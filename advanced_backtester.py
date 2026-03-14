"""
Advanced Backtesting Engine
Test trading strategies on historical data with realistic execution
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass, asdict
import json
from utils.logger import logger


@dataclass
class BacktestTrade:
    """Individual trade record"""
    entry_date: str
    exit_date: str
    asset: str
    direction: str
    entry_price: float
    exit_price: float
    stop_loss: float
    take_profit: float
    position_size: float
    pnl: float
    return_pct: float
    duration_days: int
    exit_reason: str
    confidence: float


@dataclass
class BacktestResults:
    """Comprehensive backtest results"""
    total_trades: int
    winning_trades: int
    losing_trades: int
    win_rate: float
    total_pnl: float
    total_return_pct: float
    avg_win: float
    avg_loss: float
    largest_win: float
    largest_loss: float
    profit_factor: float
    sharpe_ratio: float
    sortino_ratio: float
    max_drawdown: float
    max_drawdown_duration: int
    avg_trade_duration: float
    trades_per_month: float
    expectancy: float
    risk_reward_ratio: float
    
    def to_dict(self) -> Dict:
        return asdict(self)


class AdvancedBacktester:
    """
    Professional backtesting engine with realistic execution
    Features:
    - Slippage and commission modeling
    - Multiple exit conditions
    - Position sizing
    - Drawdown tracking
    - Monte Carlo simulation
    - Walk-forward optimization
    """
    
    def __init__(
        self,
        initial_capital: float = 10000,
        commission: float = 0.0001,  # 1 basis point
        slippage: float = 0.0002,    # 2 basis points
        risk_per_trade: float = 0.01  # 1%
    ):
        self.initial_capital = initial_capital
        self.commission = commission
        self.slippage = slippage
        self.risk_per_trade = risk_per_trade
        
        self.trades: List[BacktestTrade] = []
        self.equity_curve: List[float] = []
        self.current_capital = initial_capital
        
    def run_backtest(
        self,
        df: pd.DataFrame,
        signals: pd.DataFrame,
        use_trailing_stop: bool = True,
        max_positions: int = 3
    ) -> BacktestResults:
        """
        Run backtest on historical data with signals
        
        Args:
            df: OHLCV data with indicators
            signals: DataFrame with columns: date, signal, confidence, entry, sl, tp
            use_trailing_stop: Whether to use trailing stops
            max_positions: Maximum concurrent positions
            
        Returns:
            BacktestResults object
        """
        logger.info("\n🔬 Running Backtest...")

        self.trades = []
        self.equity_curve = [self.initial_capital]
        self.current_capital = self.initial_capital

        open_positions: List[Dict] = []

        # Build a sorted index of all OHLCV bars for bar-by-bar iteration
        all_bars = df.sort_index()
        bar_dates = list(all_bars.index)

        # Build a dict from signal date → signal row for O(1) lookup
        # Accept both 'entry' and 'entry_price' column names, 'stop_loss' or 'sl', 'take_profit' or 'tp'
        def _get(row, *keys):
            for k in keys:
                if k in row.index and not pd.isna(row[k]):
                    return row[k]
            return None

        # Build signal lookup: map each bar date to a signal if one fires on that bar
        signal_lookup = {}
        for _, sig in signals.iterrows():
            sig_date = sig['date'] if 'date' in sig.index else sig.name
            signal_lookup[sig_date] = sig

        # Iterate every OHLCV bar — not just signal dates.
        # This ensures SL/TP are checked on every bar, not just when the next
        # signal happens to arrive (which was the original bug: positions could
        # sit for 20 bars with no exit check while price blew through the stop).
        for bar_date in bar_dates:
            current_bar = all_bars.loc[bar_date]

            # ── 1. Check exits for all open positions on this bar ──────────
            for position in open_positions.copy():
                exit_price, exit_reason = self._check_exit(
                    position,
                    current_bar,
                    use_trailing_stop
                )
                if exit_price:
                    trade = self._close_position(
                        position,
                        exit_price,
                        bar_date,
                        exit_reason
                    )
                    self.trades.append(trade)
                    open_positions.remove(position)
                    self.current_capital += trade.pnl
                    self.equity_curve.append(self.current_capital)

            # ── 2. Open new position if a signal fires on this bar ─────────
            signal = signal_lookup.get(bar_date)
            if signal is None:
                continue

            entry    = _get(signal, 'entry_price', 'entry')
            stop_loss = _get(signal, 'stop_loss', 'sl')
            take_profit = _get(signal, 'take_profit', 'tp')
            direction = _get(signal, 'signal', 'direction')
            confidence = _get(signal, 'confidence') or 0.0

            # Guard: all required fields must be present
            if entry is None or stop_loss is None or take_profit is None:
                continue

            # FIX: use 0.52 to match live trading quality gate floor, not 0.65
            if (len(open_positions) < max_positions and
                    direction in ['BUY', 'SELL'] and
                    confidence >= 0.52):

                # Calculate position size
                position_size = self._calculate_position_size(
                    entry,
                    stop_loss,
                    self.current_capital
                )
                
                # Open new position — use local variables (entry/stop_loss/take_profit/direction)
                # not signal['entry'] which may not exist under that key name
                entry_price = self._apply_slippage(entry, direction)

                position = {
                    'entry_date': bar_date,
                    'asset': signal.get('asset', 'UNKNOWN'),
                    'direction': direction,
                    'entry_price': entry_price,
                    'stop_loss': stop_loss,
                    'take_profit': take_profit,
                    'position_size': position_size,
                    'confidence': confidence,
                    'highest_price': entry_price,
                    'lowest_price': entry_price
                }

                open_positions.append(position)
        
        # Close any remaining positions at end
        if len(df) > 0:
            final_bar = df.iloc[-1]
            for position in open_positions:
                trade = self._close_position(
                    position,
                    final_bar['close'],
                    final_bar.name,
                    'backtest_end'
                )
                self.trades.append(trade)
                self.current_capital += trade.pnl
        
        # Calculate results
        results = self._calculate_results()
        
        logger.info(f"✅ Backtest Complete: {results.total_trades} trades")

        return results
    
    def _check_exit(
        self,
        position: Dict,
        current_bar: pd.Series,
        use_trailing_stop: bool
    ) -> Tuple[Optional[float], Optional[str]]:
        """Check if position should be exited.

        Trailing stop logic (fixed):
        - Phase 1 (< 50% of profit range): SL stays at original level.
        - Phase 2 (>= 50% of profit range): SL moves to breakeven.
        - Phase 3 (> 50%): SL trails continuously — locks in 50% of the
          unrealised profit beyond breakeven so winners don't fully give back.
        The original code moved SL to breakeven once and then froze it, meaning
        a position that ran to 3× profit would still only protect breakeven.
        """
        # Update highest/lowest watermarks
        position['highest_price'] = max(position['highest_price'], current_bar['high'])
        position['lowest_price'] = min(position['lowest_price'], current_bar['low'])

        if position['direction'] == 'BUY':
            # ── Exit checks ────────────────────────────────────────────────
            if current_bar['low'] <= position['stop_loss']:
                return position['stop_loss'], 'stop_loss'
            if current_bar['high'] >= position['take_profit']:
                return position['take_profit'], 'take_profit'

            # ── Trailing stop ──────────────────────────────────────────────
            if use_trailing_stop:
                profit_range = position['take_profit'] - position['entry_price']
                if profit_range > 0:
                    halfway = position['entry_price'] + profit_range * 0.5
                    best    = position['highest_price']
                    if best >= halfway:
                        # Lock in 50% of profit above entry that price has moved
                        profit_above_entry = best - position['entry_price']
                        trail_sl = position['entry_price'] + profit_above_entry * 0.5
                        # SL only ever moves forward, never back
                        position['stop_loss'] = max(position['stop_loss'], trail_sl)

        else:  # SELL
            # ── Exit checks ────────────────────────────────────────────────
            if current_bar['high'] >= position['stop_loss']:
                return position['stop_loss'], 'stop_loss'
            if current_bar['low'] <= position['take_profit']:
                return position['take_profit'], 'take_profit'

            # ── Trailing stop ──────────────────────────────────────────────
            if use_trailing_stop:
                profit_range = position['entry_price'] - position['take_profit']
                if profit_range > 0:
                    halfway = position['entry_price'] - profit_range * 0.5
                    best    = position['lowest_price']
                    if best <= halfway:
                        profit_below_entry = position['entry_price'] - best
                        trail_sl = position['entry_price'] - profit_below_entry * 0.5
                        # SL only ever moves forward (lower) for shorts
                        position['stop_loss'] = min(position['stop_loss'], trail_sl)

        return None, None
    
    def _calculate_position_size(
        self,
        entry_price: float,
        stop_loss: float,
        capital: float
    ) -> float:
        """Calculate position size based on risk"""
        risk_amount = capital * self.risk_per_trade
        price_diff = abs(entry_price - stop_loss)
        
        if price_diff == 0:
            return 0
        
        return risk_amount / price_diff
    
    def _apply_slippage(self, price: float, direction: str) -> float:
        """Apply slippage to execution price"""
        if direction == 'BUY':
            return price * (1 + self.slippage)
        else:
            return price * (1 - self.slippage)
    
    def _close_position(
        self,
        position: Dict,
        exit_price: float,
        exit_date: str,
        exit_reason: str
    ) -> BacktestTrade:
        """Close position and calculate P&L"""
        
        # Apply slippage on exit
        direction = 'SELL' if position['direction'] == 'BUY' else 'BUY'
        exit_price = self._apply_slippage(exit_price, direction)
        
        # Calculate P&L
        if position['direction'] == 'BUY':
            price_change = exit_price - position['entry_price']
        else:
            price_change = position['entry_price'] - exit_price
        
        # Apply commission both ways
        commission_cost = (position['entry_price'] + exit_price) * self.commission * position['position_size']
        
        gross_pnl = price_change * position['position_size']
        net_pnl = gross_pnl - commission_cost
        
        return_pct = (net_pnl / (position['entry_price'] * position['position_size'])) * 100
        
        # Calculate duration
        entry_dt = pd.to_datetime(position['entry_date'])
        exit_dt = pd.to_datetime(exit_date)
        duration = (exit_dt - entry_dt).days
        
        return BacktestTrade(
            entry_date=str(position['entry_date']),
            exit_date=str(exit_date),
            asset=position['asset'],
            direction=position['direction'],
            entry_price=position['entry_price'],
            exit_price=exit_price,
            stop_loss=position['stop_loss'],
            take_profit=position['take_profit'],
            position_size=position['position_size'],
            pnl=net_pnl,
            return_pct=return_pct,
            duration_days=duration,
            exit_reason=exit_reason,
            confidence=position['confidence']
        )
    
    def _calculate_results(self) -> BacktestResults:
        """Calculate comprehensive backtest statistics"""
        
        if not self.trades:
            return BacktestResults(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
        
        total_trades = len(self.trades)
        winning_trades = [t for t in self.trades if t.pnl > 0]
        losing_trades = [t for t in self.trades if t.pnl <= 0]
        
        win_count = len(winning_trades)
        loss_count = len(losing_trades)
        win_rate = win_count / total_trades if total_trades > 0 else 0
        
        total_pnl = sum(t.pnl for t in self.trades)
        total_return_pct = (total_pnl / self.initial_capital) * 100
        
        avg_win = np.mean([t.pnl for t in winning_trades]) if winning_trades else 0
        avg_loss = np.mean([t.pnl for t in losing_trades]) if losing_trades else 0
        
        largest_win = max([t.pnl for t in self.trades])
        largest_loss = min([t.pnl for t in self.trades])
        
        total_wins = sum(t.pnl for t in winning_trades)
        total_losses = abs(sum(t.pnl for t in losing_trades))
        profit_factor = total_wins / total_losses if total_losses > 0 else 0
        
        # Sharpe Ratio
        returns = [t.return_pct for t in self.trades]
        sharpe = (np.mean(returns) - 0) / (np.std(returns) + 1e-10)
        
        # Sortino Ratio (only downside deviation)
        downside_returns = [r for r in returns if r < 0]
        downside_std = np.std(downside_returns) if downside_returns else 1e-10
        sortino = np.mean(returns) / downside_std
        
        # Max Drawdown
        equity_array = np.array(self.equity_curve)
        running_max = np.maximum.accumulate(equity_array)
        drawdown = (equity_array - running_max) / running_max
        max_dd = abs(np.min(drawdown)) if len(drawdown) > 0 else 0
        
        # Max DD Duration
        dd_duration = 0
        current_dd_duration = 0
        for i in range(len(equity_array)):
            if equity_array[i] < running_max[i]:
                current_dd_duration += 1
                dd_duration = max(dd_duration, current_dd_duration)
            else:
                current_dd_duration = 0
        
        # Trade metrics
        durations = [t.duration_days for t in self.trades]
        avg_duration = np.mean(durations) if durations else 0
        
        # Trades per month
        if self.trades:
            first_date = pd.to_datetime(self.trades[0].entry_date)
            last_date = pd.to_datetime(self.trades[-1].exit_date)
            months = max((last_date - first_date).days / 30, 1)
            trades_per_month = total_trades / months
        else:
            trades_per_month = 0
        
        # Expectancy
        expectancy = (win_rate * avg_win) + ((1 - win_rate) * avg_loss)
        
        # Risk/Reward
        risk_reward = abs(avg_win / avg_loss) if avg_loss != 0 else 0
        
        return BacktestResults(
            total_trades=total_trades,
            winning_trades=win_count,
            losing_trades=loss_count,
            win_rate=win_rate,
            total_pnl=total_pnl,
            total_return_pct=total_return_pct,
            avg_win=avg_win,
            avg_loss=avg_loss,
            largest_win=largest_win,
            largest_loss=largest_loss,
            profit_factor=profit_factor,
            sharpe_ratio=sharpe,
            sortino_ratio=sortino,
            max_drawdown=max_dd,
            max_drawdown_duration=dd_duration,
            avg_trade_duration=avg_duration,
            trades_per_month=trades_per_month,
            expectancy=expectancy,
            risk_reward_ratio=risk_reward
        )
    
    def plot_equity_curve(self) -> pd.DataFrame:
        """Return equity curve as DataFrame for plotting"""
        return pd.DataFrame({
            'equity': self.equity_curve,
            'trade_number': range(len(self.equity_curve))
        })
    
    def export_trades(self, filename: str = "backtest_trades.csv") -> None:
        """Export all trades to CSV"""
        if self.trades:
            trades_df = pd.DataFrame([asdict(t) for t in self.trades])
            trades_df.to_csv(filename, index=False)
            logger.info(f"✅ Exported {len(self.trades)} trades to {filename}")

    def monte_carlo_simulation(
        self,
        num_simulations: int = 1000,
        confidence_level: float = 0.95
    ) -> Dict[str, float]:
        """
        Run Monte Carlo simulation on trade results
        
        Returns:
            Dictionary with simulation statistics
        """
        if not self.trades:
            return {}
        
        returns = [t.return_pct / 100 for t in self.trades]
        num_trades = len(returns)
        
        simulated_returns = []
        
        for _ in range(num_simulations):
            # Random sampling with replacement
            sim_returns = np.random.choice(returns, size=num_trades, replace=True)
            final_return = (1 + sim_returns).prod() - 1
            simulated_returns.append(final_return)
        
        simulated_returns = np.array(simulated_returns)
        
        return {
            'mean_return': np.mean(simulated_returns),
            'median_return': np.median(simulated_returns),
            'std_return': np.std(simulated_returns),
            'best_case': np.percentile(simulated_returns, 95),
            'worst_case': np.percentile(simulated_returns, 5),
            'confidence_interval_lower': np.percentile(simulated_returns, (1 - confidence_level) * 100 / 2),
            'confidence_interval_upper': np.percentile(simulated_returns, 100 - (1 - confidence_level) * 100 / 2)
        }


if __name__ == "__main__":
    logger.info("Backtesting Engine Test")

    logger.info("="*60)

    # Create sample data
    dates = pd.date_range('2023-01-01', '2024-01-01', freq='D')
    df = pd.DataFrame({
        'date': dates,
        'open': np.random.randn(len(dates)).cumsum() + 100,
        'high': np.random.randn(len(dates)).cumsum() + 101,
        'low': np.random.randn(len(dates)).cumsum() + 99,
        'close': np.random.randn(len(dates)).cumsum() + 100,
        'volume': np.random.randint(1000, 10000, len(dates))
    })
    df.set_index('date', inplace=True)
    
    # Create sample signals
    signals = pd.DataFrame({
        'date': dates[::10],  # Every 10 days
        'signal': np.random.choice(['BUY', 'SELL'], len(dates[::10])),
        'confidence': np.random.uniform(0.6, 0.9, len(dates[::10])),
        'entry': df['close'].iloc[::10].values,
        'stop_loss': df['close'].iloc[::10].values * 0.98,
        'take_profit': df['close'].iloc[::10].values * 1.03,
        'asset': 'TEST'
    })
    
    # Run backtest
    backtester = AdvancedBacktester(initial_capital=10000)
    results = backtester.run_backtest(df, signals)
    
    logger.info("\nBACKTEST RESULTS:")

    logger.info(f"Total Trades: {results.total_trades}")

    logger.info(f"Win Rate: {results.win_rate:.1%}")

    logger.info(f"Total Return: {results.total_return_pct:.2f}%")

    logger.info(f"Profit Factor: {results.profit_factor:.2f}")

    logger.info(f"Sharpe Ratio: {results.sharpe_ratio:.2f}")

    logger.info(f"Max Drawdown: {results.max_drawdown:.2%}")