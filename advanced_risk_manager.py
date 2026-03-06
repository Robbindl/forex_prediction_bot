"""
Advanced Risk Management System
Features:
- Kelly Criterion position sizing
- Maximum Drawdown protection
- Sharpe Ratio optimization
- Value at Risk (VaR) calculation
- Portfolio correlation management
- Dynamic position sizing based on market conditions
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
from scipy.optimize import minimize
from dataclasses import dataclass


@dataclass
class TradeMetrics:
    """Trade performance metrics"""
    wins: int
    losses: int
    total_trades: int
    win_rate: float
    avg_win: float
    avg_loss: float
    profit_factor: float
    sharpe_ratio: float
    max_drawdown: float


class AdvancedRiskManager:
    """
    Professional-grade risk management system
    """
    
    def __init__(self, account_balance: float = 10000):
        self.account_balance = account_balance
        self.trade_history: List[Dict] = []
        self.current_positions: List[Dict] = []
        self.max_positions = 5
        self.max_correlation = 0.7
        
    def calculate_kelly_criterion(self, win_rate: float, avg_win: float, 
                                  avg_loss: float, fraction: float = 0.25) -> float:
        """
        Calculate optimal position size using Kelly Criterion
        
        Args:
            win_rate: Historical win rate (0-1)
            avg_win: Average winning trade return
            avg_loss: Average losing trade return (positive number)
            fraction: Kelly fraction (0.25 = quarter Kelly, safer)
            
        Returns:
            Optimal position size as fraction of capital
        """
        if win_rate <= 0 or win_rate >= 1:
            return 0.01  # Default 1%
        
        if avg_loss <= 0:
            return 0.01
        
        # Kelly formula: f = (p*b - q) / b
        # where p = win rate, q = loss rate, b = win/loss ratio
        b = avg_win / avg_loss
        kelly = (win_rate * b - (1 - win_rate)) / b
        
        # Apply fraction for safety
        kelly_fractional = kelly * fraction
        
        # Cap between 0.5% and 5%
        kelly_capped = max(0.005, min(kelly_fractional, 0.05))
        
        return kelly_capped
    
    def calculate_var(self, returns: pd.Series, confidence: float = 0.95) -> float:
        """
        Calculate Value at Risk (VaR)
        
        Args:
            returns: Series of historical returns
            confidence: Confidence level (0.95 = 95%)
            
        Returns:
            VaR as percentage loss
        """
        return np.percentile(returns, (1 - confidence) * 100)
    
    def calculate_optimal_position_size(
        self,
        entry_price: float,
        stop_loss: float,
        signal_confidence: float,
        asset_volatility: float = 0.02,
        win_rate: float = 0.55,
        avg_win: float = 0.02,
        avg_loss: float = 0.01
    ) -> Dict[str, float]:
        """
        Calculate optimal position size using multiple methods
        
        Returns comprehensive position sizing recommendation
        """
        # Method 1: Fixed fractional (baseline)
        risk_pct = 0.01  # 1% base risk
        risk_amount = self.account_balance * risk_pct
        price_diff = abs(entry_price - stop_loss)
        
        if price_diff == 0:
            return {'position_size': 0, 'risk_amount': 0, 'method': 'invalid'}
        
        fixed_position = risk_amount / price_diff
        
        # Method 2: Kelly Criterion
        kelly_fraction = self.calculate_kelly_criterion(win_rate, avg_win, avg_loss)
        kelly_position = (self.account_balance * kelly_fraction) / price_diff
        
        # Method 3: Volatility-adjusted
        # Higher volatility = smaller position
        vol_adjustment = 0.02 / (asset_volatility + 0.01)  # Normalize to 2% volatility
        vol_adjusted_risk = risk_pct * min(vol_adjustment, 2.0)  # Cap at 2x
        vol_position = (self.account_balance * vol_adjusted_risk) / price_diff
        
        # Method 4: Confidence-weighted
        # Higher confidence = larger position (up to 2x)
        confidence_multiplier = 0.5 + (signal_confidence * 1.5)  # 0.5x to 2x
        confidence_position = fixed_position * confidence_multiplier
        
        # Ensemble: Average of all methods
        ensemble_position = np.mean([
            fixed_position,
            kelly_position * 0.8,  # Weight Kelly lower (more conservative)
            vol_position,
            confidence_position
        ])
        
        # Final position (with safety limits)
        max_position_value = self.account_balance * 0.2  # Never more than 20% in one trade
        max_position_size = max_position_value / entry_price
        
        final_position = min(ensemble_position, max_position_size)
        final_risk = final_position * price_diff
        final_risk_pct = (final_risk / self.account_balance) * 100
        
        return {
            'position_size': final_position,
            'position_value': final_position * entry_price,
            'risk_amount': final_risk,
            'risk_pct': final_risk_pct,
            'kelly_fraction': kelly_fraction,
            'confidence_multiplier': confidence_multiplier,
            'methods': {
                'fixed': fixed_position,
                'kelly': kelly_position,
                'volatility_adjusted': vol_position,
                'confidence_weighted': confidence_position,
                'ensemble': ensemble_position
            }
        }
    
    def check_portfolio_correlation(
        self,
        new_asset: str,
        existing_positions: List[Dict],
        correlation_matrix: pd.DataFrame
    ) -> Tuple[bool, float]:
        """
        Check if adding new position would exceed correlation limits
        
        Returns:
            (allowed, max_correlation)
        """
        if not existing_positions:
            return True, 0.0
        
        max_corr = 0.0
        for position in existing_positions:
            existing_asset = position['asset']
            
            # Check correlation
            if new_asset in correlation_matrix.index and existing_asset in correlation_matrix.columns:
                corr = abs(correlation_matrix.loc[new_asset, existing_asset])
                max_corr = max(max_corr, corr)
        
        allowed = max_corr < self.max_correlation
        return allowed, max_corr
    
    def calculate_portfolio_heat(self, open_positions: List[Dict]) -> float:
        """
        Calculate total portfolio risk (heat)
        
        Returns:
            Total risk as percentage of account
        """
        total_risk = sum(pos.get('risk_amount', 0) for pos in open_positions)
        return (total_risk / self.account_balance) * 100
    
    def should_take_trade(
        self,
        signal: Dict,
        existing_positions: List[Dict],
        correlation_matrix: Optional[pd.DataFrame] = None
    ) -> Tuple[bool, str, Dict]:
        """
        Determine if trade should be taken based on risk rules
        
        Returns:
            (should_take, reason, position_details)
        """
        # Rule 1: Maximum positions
        if len(existing_positions) >= self.max_positions:
            return False, f"Max positions ({self.max_positions}) reached", {}
        
        # Rule 2: Confidence threshold
        if signal['confidence'] < 0.65:
            return False, f"Confidence too low ({signal['confidence']:.1%})", {}
        
        # Rule 3: Portfolio heat
        current_heat = self.calculate_portfolio_heat(existing_positions)
        if current_heat > 10:  # Max 10% total portfolio risk
            return False, f"Portfolio heat too high ({current_heat:.1%})", {}
        
        # Rule 4: Correlation check
        if correlation_matrix is not None:
            allowed, max_corr = self.check_portfolio_correlation(
                signal['asset'],
                existing_positions,
                correlation_matrix
            )
            if not allowed:
                return False, f"Too correlated with existing position ({max_corr:.2f})", {}
        
        # Calculate position size
        position_details = self.calculate_optimal_position_size(
            entry_price=signal['entry_price'],
            stop_loss=signal['stop_loss'],
            signal_confidence=signal['confidence'],
            asset_volatility=signal.get('risk_pct', 1.0) / 100
        )
        
        # Rule 5: Risk per trade
        if position_details['risk_pct'] > 3.0:  # Never risk more than 3%
            return False, f"Single trade risk too high ({position_details['risk_pct']:.1%})", {}
        
        return True, "Trade approved", position_details
    
    def calculate_trade_metrics(self, trades: List[Dict]) -> TradeMetrics:
        """Calculate performance metrics from trade history"""
        if not trades:
            return TradeMetrics(0, 0, 0, 0, 0, 0, 0, 0, 0)
        
        wins = [t for t in trades if t.get('pnl', 0) > 0]
        losses = [t for t in trades if t.get('pnl', 0) <= 0]
        
        win_count = len(wins)
        loss_count = len(losses)
        total = len(trades)
        
        win_rate = win_count / total if total > 0 else 0
        avg_win = np.mean([t['pnl'] for t in wins]) if wins else 0
        avg_loss = abs(np.mean([t['pnl'] for t in losses])) if losses else 0
        
        total_wins = sum(t['pnl'] for t in wins)
        total_losses = abs(sum(t['pnl'] for t in losses))
        profit_factor = total_wins / total_losses if total_losses > 0 else 0
        
        # Sharpe Ratio
        returns = [t.get('return_pct', 0) for t in trades]
        if returns:
            sharpe = (np.mean(returns) - 0.02) / (np.std(returns) + 1e-10) * np.sqrt(252)
        else:
            sharpe = 0
        
        # Max Drawdown
        cumulative = np.cumsum([t.get('pnl', 0) for t in trades])
        running_max = np.maximum.accumulate(cumulative)
        drawdown = (cumulative - running_max) / (running_max + 1)
        max_dd = abs(np.min(drawdown)) if len(drawdown) > 0 else 0
        
        return TradeMetrics(
            wins=win_count,
            losses=loss_count,
            total_trades=total,
            win_rate=win_rate,
            avg_win=avg_win,
            avg_loss=avg_loss,
            profit_factor=profit_factor,
            sharpe_ratio=sharpe,
            max_drawdown=max_dd
        )
    
    def optimize_portfolio_allocation(
        self,
        expected_returns: np.ndarray,
        cov_matrix: np.ndarray,
        risk_free_rate: float = 0.02
    ) -> np.ndarray:
        """
        Optimize portfolio allocation using Modern Portfolio Theory
        
        Returns:
            Optimal weights for each asset
        """
        n_assets = len(expected_returns)
        
        # Objective: Maximize Sharpe Ratio
        def neg_sharpe(weights):
            port_return = np.dot(weights, expected_returns)
            port_vol = np.sqrt(np.dot(weights.T, np.dot(cov_matrix, weights)))
            sharpe = (port_return - risk_free_rate) / port_vol
            return -sharpe
        
        # Constraints
        constraints = {'type': 'eq', 'fun': lambda x: np.sum(x) - 1}  # Weights sum to 1
        bounds = tuple((0, 0.3) for _ in range(n_assets))  # Max 30% per asset
        
        # Initial guess (equal weight)
        init_guess = np.array([1/n_assets] * n_assets)
        
        # Optimize
        result = minimize(
            neg_sharpe,
            init_guess,
            method='SLSQP',
            bounds=bounds,
            constraints=constraints
        )
        
        return result.x if result.success else init_guess


class DynamicRiskAdjuster:
    """
    Adjust risk parameters based on market conditions
    """
    
    @staticmethod
    def detect_market_regime(df: pd.DataFrame) -> str:
        """
        Detect current market regime
        
        Returns:
            'trending', 'ranging', 'volatile', or 'calm'
        """
        # Calculate ADX for trend
        adx = df['adx'].iloc[-1] if 'adx' in df.columns else 20
        
        # Calculate volatility
        returns = df['close'].pct_change()
        volatility = returns.rolling(20).std().iloc[-1]
        
        # Determine regime
        if adx > 25 and volatility < 0.02:
            return 'trending_calm'
        elif adx > 25 and volatility >= 0.02:
            return 'trending_volatile'
        elif adx <= 25 and volatility < 0.02:
            return 'ranging_calm'
        else:
            return 'ranging_volatile'
    
    @staticmethod
    def adjust_risk_for_regime(base_risk: float, regime: str) -> float:
        """
        Adjust risk based on market regime
        """
        multipliers = {
            'trending_calm': 1.5,      # Best conditions - increase risk
            'trending_volatile': 1.0,   # Good but risky - maintain
            'ranging_calm': 0.7,        # Harder to profit - reduce
            'ranging_volatile': 0.5     # Worst conditions - minimize risk
        }
        
        return base_risk * multipliers.get(regime, 1.0)

class DynamicPositionSizer:
    """
    Advanced position sizing that adapts to market conditions
    """
    
    def __init__(self, base_risk: float = 0.01, max_risk: float = 0.03):
        self.base_risk = base_risk  # 1% base
        self.max_risk = max_risk    # 3% maximum
        
    def calculate_size(self, 
                      signal_confidence: float,
                      account_volatility: float,
                      market_regime_multiplier: float = 1.0,
                      win_rate: float = 0.5,
                      kelly_fraction: float = 0.25) -> Dict[str, float]:
        """
        Calculate position size using multiple factors
        
        Args:
            signal_confidence: 0-1 confidence in signal
            account_volatility: Recent account volatility (drawdown)
            market_regime_multiplier: From regime detection
            win_rate: Historical win rate
            kelly_fraction: Kelly criterion fraction
            
        Returns:
            Dict with size and risk metrics
        """
        
        # 1. Base from Kelly if available
        if kelly_fraction > 0:
            kelly_size = kelly_fraction
        else:
            kelly_size = self.base_risk
        
        # 2. Confidence multiplier (0.5x to 2x)
        confidence_multiplier = 0.5 + signal_confidence
        
        # 3. Volatility reducer (higher vol = smaller size)
        # account_volatility as percentage (e.g., 0.05 for 5%)
        volatility_reducer = 1.0 / (1.0 + account_volatility * 10)
        
        # 4. Market regime adjustment
        regime_multiplier = market_regime_multiplier
        
        # 5. Win rate boost (if consistently winning)
        win_rate_boost = 1.0
        if win_rate > 0.6:
            win_rate_boost = 1.0 + (win_rate - 0.6) * 2  # Up to 1.8x
        
        # Combine all factors
        final_risk = (kelly_size * 
                     confidence_multiplier * 
                     volatility_reducer * 
                     regime_multiplier * 
                     win_rate_boost)
        
        # Cap at max risk
        final_risk = min(final_risk, self.max_risk)
        
        return {
            'risk_percent': round(final_risk * 100, 2),
            'kelly_component': kelly_size,
            'confidence_boost': confidence_multiplier,
            'volatility_reduction': volatility_reducer,
            'regime_adjustment': regime_multiplier,
            'win_rate_boost': win_rate_boost,
            'raw_risk': final_risk
        }
    
    def calculate_position_units(self, 
                                account_balance: float,
                                entry_price: float,
                                stop_loss: float,
                                risk_percent: float) -> Dict[str, float]:
        """
        Convert risk percentage to actual position units
        """
        risk_amount = account_balance * risk_percent
        price_diff = abs(entry_price - stop_loss)
        
        if price_diff == 0:
            return {'position_size': 0, 'risk_amount': 0}
        
        position_size = risk_amount / price_diff
        position_value = position_size * entry_price
        
        return {
            'position_size': position_size,
            'risk_amount': risk_amount,
            'position_value': position_value,
            'leverage': position_value / account_balance if account_balance > 0 else 0
        }
class DailyLossLimit:
    """
    Automatic trading stop when daily loss limit is hit
    """
    
    def __init__(self, max_loss_pct: float = 3.0, alert_callback=None):
        self.max_loss_pct = max_loss_pct
        self.initial_balance = None
        self.current_balance = None
        self.daily_pnl = 0.0
        self.trading_paused = False
        self.pause_time = None
        self.pause_duration = 3600  # 1 hour pause after limit hit
        self.alert_callback = alert_callback
        self.reset_daily()
    
    def reset_daily(self):
        """Reset daily counters (call at start of each day)"""
        self.daily_pnl = 0.0
        self.trading_paused = False
        self.pause_time = None
    
    def set_initial_balance(self, balance: float):
        """Set initial balance for the day"""
        self.initial_balance = balance
        self.current_balance = balance
    
    def update(self, pnl: float) -> Tuple[bool, str]:
        """
        Update with trade P&L
        Returns (trading_allowed, message)
        """
        self.daily_pnl += pnl
        self.current_balance += pnl
        
        # Calculate loss percentage
        if self.initial_balance and self.initial_balance > 0:
            loss_pct = (self.daily_pnl / self.initial_balance) * 100
        else:
            loss_pct = 0
        
        # Check if limit hit
        if loss_pct <= -self.max_loss_pct and not self.trading_paused:
            self.trading_paused = True
            self.pause_time = datetime.now()
            
            message = f"🚨 DAILY LOSS LIMIT HIT: {loss_pct:.1f}% (max {self.max_loss_pct}%)"
            print(f"\n{'='*60}")
            print(message)
            print(f"Trading paused for {self.pause_duration//60} minutes")
            print(f"{'='*60}\n")
            
            if self.alert_callback:
                self.alert_callback(message)
            
            return False, message
        
        # Check if still in pause period
        if self.trading_paused and self.pause_time:
            elapsed = (datetime.now() - self.pause_time).seconds
            if elapsed < self.pause_duration:
                remaining = (self.pause_duration - elapsed) // 60
                return False, f"Trading paused ({remaining} minutes remaining)"
            else:
                # Resume trading
                self.trading_paused = False
                self.pause_time = None
                print(f"\n✅ Trading resumed after pause period")
                return True, "Trading resumed"
        
        return True, f"OK (Daily P&L: {loss_pct:.1f}%)"
    
    def get_status(self) -> Dict:
        """Get current status"""
        if self.initial_balance and self.initial_balance > 0:
            loss_pct = (self.daily_pnl / self.initial_balance) * 100
        else:
            loss_pct = 0
        
        return {
            'initial_balance': self.initial_balance,
            'current_balance': self.current_balance,
            'daily_pnl': self.daily_pnl,
            'daily_loss_pct': round(loss_pct, 2),
            'max_loss_pct': self.max_loss_pct,
            'trading_paused': self.trading_paused,
            'limit_hit': loss_pct <= -self.max_loss_pct
        } 

if __name__ == "__main__":
    # Test the risk manager
    rm = AdvancedRiskManager(account_balance=10000)
    
    # Test Kelly Criterion
    kelly = rm.calculate_kelly_criterion(0.6, 0.02, 0.01)
    print(f"Kelly Criterion: {kelly:.2%}")
    
    # Test position sizing
    position = rm.calculate_optimal_position_size(
        entry_price=1.0850,
        stop_loss=1.0820,
        signal_confidence=0.75,
        asset_volatility=0.015,
        win_rate=0.58,
        avg_win=0.025,
        avg_loss=0.012
    )
    
    print("\nOptimal Position Sizing:")
    print(f"  Position Size: {position['position_size']:.2f} units")
    print(f"  Position Value: ${position['position_value']:.2f}")
    print(f"  Risk Amount: ${position['risk_amount']:.2f}")
    print(f"  Risk %: {position['risk_pct']:.2f}%")
    print(f"  Kelly Fraction: {position['kelly_fraction']:.2%}")
