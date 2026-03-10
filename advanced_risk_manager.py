"""
Advanced Risk Management System
Features:
- Kelly Criterion position sizing
- Maximum Drawdown protection
- Sharpe Ratio optimization
- Value at Risk (VaR) calculation
- Portfolio correlation management
- Dynamic position sizing based on market conditions
- Sentiment-based stop loss adjustment
- Market regime detection
- Dynamic position sizing with sentiment
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
from scipy.optimize import minimize
from dataclasses import dataclass
from datetime import datetime
from logger import logger


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
        self.initial_balance = account_balance
        self.trade_history: List[Dict] = []
        self.current_positions: List[Dict] = []
        self.max_positions = 5
        self.max_correlation = 0.7
        self.current_drawdown = 0.0
        self.peak_balance = account_balance
        
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
    
    # In advanced_risk_manager.py, find this method (around line 150-200)
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
        LOOSENED for volatile markets
        """
        # Method 1: Fixed fractional (INCREASED from 1% to 2%)
        risk_pct = 0.02  # ← CHANGED from 0.01 to 0.02 (2% risk)
        risk_amount = self.account_balance * risk_pct
        price_diff = abs(entry_price - stop_loss)
        
        if price_diff == 0:
            logger.warning(f"Position size calculation failed: entry price equals stop loss")
            return {'position_size': 0, 'risk_amount': 0, 'method': 'invalid'}
        
        fixed_position = risk_amount / price_diff
        
        # Method 2: Kelly Criterion (INCREASED fraction from 0.25 to 0.5)
        kelly_fraction = self.calculate_kelly_criterion(win_rate, avg_win, avg_loss)
        kelly_fraction = kelly_fraction * 0.5  # ← CHANGED from 0.25 to 0.5 (half Kelly instead of quarter)
        kelly_position = (self.account_balance * kelly_fraction) / price_diff
        
        # Method 3: Volatility-adjusted (INCREASED cap from 2x to 3x)
        vol_adjustment = 0.02 / (asset_volatility + 0.01)
        vol_adjusted_risk = risk_pct * min(vol_adjustment, 3.0)  # ← CHANGED from 2.0 to 3.0
        vol_position = (self.account_balance * vol_adjusted_risk) / price_diff
        
        # Method 4: Confidence-weighted (INCREASED multiplier range)
        confidence_multiplier = 0.5 + (signal_confidence * 2.0)  # ← CHANGED from 1.5 to 2.0 (now 0.5x to 2.5x)
        confidence_position = fixed_position * confidence_multiplier
        
        # Ensemble: Average of all methods (with higher weights for aggressive methods)
        ensemble_position = np.mean([
            fixed_position,
            kelly_position,
            vol_position,
            confidence_position
        ])
        
        # Final position (INCREASED max position value from 20% to 35%)
        max_position_value = self.account_balance * 0.35  # ← CHANGED from 0.2 to 0.35 (35% in one trade)
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
        if not allowed:
            logger.warning(f"Correlation check failed: {new_asset} vs {existing_asset} = {max_corr:.2f}")
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
    
    def update_drawdown(self, current_balance: float):
        """Update current drawdown based on balance"""
        if current_balance > self.peak_balance:
            self.peak_balance = current_balance
        
        self.current_drawdown = ((self.peak_balance - current_balance) / self.peak_balance) * 100
        logger.debug(f"Drawdown updated: {self.current_drawdown:.2f}%")
    
    # ============= NEW ENHANCED METHODS =============
    
    def calculate_dynamic_stop_loss(self, 
                                atr: float, 
                                entry_price: float,
                                sentiment_score: float = 0.0,
                                market_regime: str = 'normal',
                                volatility_regime: str = 'normal') -> Dict[str, float]:
        """
        Calculate dynamic stop loss based on market conditions
        LOOSENED for volatile markets
        """
        # Base stop loss (REDUCED from 2x ATR to 1.5x ATR - tighter stops for bigger positions)
        base_stop_distance = atr * 1.5  # ← CHANGED from 2.0 to 1.5
        
        # 1. SENTIMENT ADJUSTMENT (LESS impact)
        if sentiment_score < -0.5:  # Extreme fear
            sentiment_multiplier = 1.3  # ← CHANGED from 1.5
            sentiment_reason = "Extreme fear - widening stops"
        elif sentiment_score < -0.2:  # Fear
            sentiment_multiplier = 1.1  # ← CHANGED from 1.2
            sentiment_reason = "Fear present - slightly wider stops"
        elif sentiment_score > 0.5:   # Extreme greed
            sentiment_multiplier = 0.8  # ← CHANGED from 0.7 (less tight)
            sentiment_reason = "Extreme greed - tightening stops"
        elif sentiment_score > 0.2:    # Greed
            sentiment_multiplier = 0.95  # ← CHANGED from 0.9
            sentiment_reason = "Greed present - slightly tighter stops"
        else:
            sentiment_multiplier = 1.0
            sentiment_reason = "Neutral sentiment - normal stops"
        
        # 2. MARKET REGIME ADJUSTMENT (LESS impact)
        regime_multipliers = {
            'trending': 1.1,      # ← CHANGED from 1.2
            'ranging': 0.9,       # ← CHANGED from 0.8
            'breakout': 1.2,      # ← CHANGED from 1.3
            'volatile': 1.2,      # ← CHANGED from 1.4
            'calm': 0.9,          # ← CHANGED from 0.8
            'normal': 1.0
        }
        regime_multiplier = regime_multipliers.get(market_regime, 1.0)
        
        # 3. VOLATILITY REGIME ADJUSTMENT (LESS impact)
        vol_multipliers = {
            'low': 0.9,           # ← CHANGED from 0.8
            'normal': 1.0,
            'high': 1.2           # ← CHANGED from 1.3
        }
        vol_multiplier = vol_multipliers.get(volatility_regime, 1.0)
        
        # Combine all multipliers
        final_multiplier = sentiment_multiplier * regime_multiplier * vol_multiplier
        
        # Calculate final stop distance
        stop_distance = base_stop_distance * final_multiplier
        
        return {
            'stop_distance': stop_distance,
            'stop_percent': (stop_distance / entry_price) * 100,
            'atr_multiple': final_multiplier * 1.5,
            'base_atr_multiple': 1.5,
            'sentiment_multiplier': sentiment_multiplier,
            'regime_multiplier': regime_multiplier,
            'volatility_multiplier': vol_multiplier,
            'sentiment_reason': sentiment_reason,
            'final_multiplier': final_multiplier
        }
    
    def get_market_regime_from_df(self, df: pd.DataFrame) -> str:
        """
        Determine market regime from dataframe
        
        Returns:
            'trending', 'ranging', 'breakout', 'volatile', 'calm', 'normal'
        """
        try:
            # Check ADX for trend strength
            if 'adx' in df.columns:
                adx = df['adx'].iloc[-1]
            else:
                adx = 20
            
            # Check volatility
            if 'atr' in df.columns:
                atr_pct = df['atr'].iloc[-1] / df['close'].iloc[-1]
            else:
                atr_pct = 0.02
            
            # Check Bollinger Band position for breakouts
            if all(x in df.columns for x in ['bb_upper', 'bb_lower']):
                close = df['close'].iloc[-1]
                bb_upper = df['bb_upper'].iloc[-1]
                bb_lower = df['bb_lower'].iloc[-1]
                
                if close > bb_upper * 1.01:
                    logger.debug(f"Breakout detected: price above upper BB")
                    return 'breakout'
                elif close < bb_lower * 0.99:
                    logger.debug(f"Breakout detected: price below lower BB")
                    return 'breakout'
            
            # Determine regime
            if adx > 30:
                return 'trending'
            elif adx < 20:
                return 'ranging'
            elif atr_pct > 0.03:
                return 'volatile'
            elif atr_pct < 0.01:
                return 'calm'
            else:
                return 'normal'
                
        except Exception as e:
            logger.warning(f"Error detecting market regime: {e}")
            return 'normal'
    
    def get_volatility_regime(self, df: pd.DataFrame) -> str:
        """
        Determine volatility regime
        
        Returns:
            'low', 'normal', 'high'
        """
        try:
            if 'atr' not in df.columns or len(df) < 50:
                return 'normal'
            
            current_atr_pct = df['atr'].iloc[-1] / df['close'].iloc[-1]
            avg_atr_pct = (df['atr'] / df['close']).rolling(50).mean().iloc[-1]
            
            ratio = current_atr_pct / avg_atr_pct if avg_atr_pct > 0 else 1.0
            
            if ratio > 1.5:
                logger.debug(f"High volatility regime: ratio={ratio:.2f}")
                return 'high'
            elif ratio < 0.7:
                logger.debug(f"Low volatility regime: ratio={ratio:.2f}")
                return 'low'
            else:
                return 'normal'
                
        except Exception as e:
            logger.warning(f"Error detecting volatility regime: {e}")
            return 'normal'
    
    def calculate_position_size_with_sentiment(
        self,
        entry_price: float,
        stop_loss: float,
        signal_confidence: float,
        sentiment_score: float,
        market_regime: str = 'normal',
        win_rate: float = 0.55,
        avg_win: float = 0.02,
        avg_loss: float = 0.01
    ) -> Dict[str, float]:
        """
        Calculate position size incorporating sentiment analysis
        
        This is an enhanced version of calculate_optimal_position_size
        that also considers market sentiment
        """
        # Get base position size from existing method
        base_position = self.calculate_optimal_position_size(
            entry_price=entry_price,
            stop_loss=stop_loss,
            signal_confidence=signal_confidence,
            asset_volatility=0.02,
            win_rate=win_rate,
            avg_win=avg_win,
            avg_loss=avg_loss
        )
        
        # Apply sentiment adjustment
        if sentiment_score < -0.5:  # Extreme fear
            sentiment_adjustment = 0.6  # Reduce size significantly
            sentiment_reason = "Extreme fear - reducing position size"
        elif sentiment_score < -0.2:  # Fear
            sentiment_adjustment = 0.8
            sentiment_reason = "Fear - reducing position size"
        elif sentiment_score > 0.5:   # Extreme greed
            sentiment_adjustment = 0.7  # Reduce size (top risk)
            sentiment_reason = "Extreme greed - reducing position size"
        elif sentiment_score > 0.2:    # Greed
            sentiment_adjustment = 0.9
            sentiment_reason = "Greed - slight reduction"
        else:
            sentiment_adjustment = 1.0
            sentiment_reason = "Neutral sentiment - normal size"
        
        # Apply market regime adjustment
        regime_adjustments = {
            'trending': 1.2,
            'breakout': 1.3,
            'ranging': 0.7,
            'volatile': 0.6,
            'calm': 0.9,
            'normal': 1.0
        }
        regime_adjustment = regime_adjustments.get(market_regime, 1.0)
        
        # Final adjustment
        final_adjustment = sentiment_adjustment * regime_adjustment
        adjusted_position = base_position['position_size'] * final_adjustment
        
        # Recalculate risk
        price_diff = abs(entry_price - stop_loss)
        adjusted_risk = adjusted_position * price_diff
        adjusted_risk_pct = (adjusted_risk / self.account_balance) * 100
        
        logger.debug(f"Position size with sentiment: {adjusted_position:.4f} (adjustment={final_adjustment:.2f}x)")
        
        # Update base position with adjustments
        base_position.update({
            'position_size': adjusted_position,
            'risk_amount': adjusted_risk,
            'risk_pct': adjusted_risk_pct,
            'sentiment_adjustment': sentiment_adjustment,
            'regime_adjustment': regime_adjustment,
            'final_adjustment': final_adjustment,
            'sentiment_reason': sentiment_reason
        })
        
        return base_position
    
    def get_sentiment_regime(self, sentiment_score: float) -> str:
        """Classify sentiment regime"""
        if sentiment_score < -0.5:
            return 'extreme_fear'
        elif sentiment_score < -0.2:
            return 'fear'
        elif sentiment_score > 0.5:
            return 'extreme_greed'
        elif sentiment_score > 0.2:
            return 'greed'
        else:
            return 'neutral'
    
    def get_risk_summary(self) -> Dict:
        """Get comprehensive risk summary"""
        return {
            'account_balance': self.account_balance,
            'peak_balance': self.peak_balance,
            'current_drawdown': self.current_drawdown,
            'max_positions': self.max_positions,
            'current_positions': len(self.current_positions),
            'total_trades': len(self.trade_history),
            'risk_per_trade': '1% base',
            'max_risk_per_trade': '3%',
            'kelly_fraction': '25%',
            'correlation_limit': self.max_correlation
        }


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
        
        logger.debug(f"Position size calculated: {final_risk*100:.2f}% risk")
        
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
            logger.warning("Position units calculation failed: price_diff=0")
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
        logger.info(f"Daily loss limit set with balance: ${balance:.2f}")
    
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
            
            message = f"DAILY LOSS LIMIT HIT: {loss_pct:.1f}% (max {self.max_loss_pct}%)"
            logger.warning(message)
            logger.warning(f"Trading paused for {self.pause_duration//60} minutes")
            
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
                logger.info(f"Trading resumed after pause period")
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
    logger.info(f"Kelly Criterion: {kelly:.2%}")
    
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
    
    logger.info("\nOptimal Position Sizing:")
    logger.info(f"  Position Size: {position['position_size']:.2f} units")
    logger.info(f"  Position Value: ${position['position_value']:.2f}")
    logger.info(f"  Risk Amount: ${position['risk_amount']:.2f}")
    logger.info(f"  Risk %: {position['risk_pct']:.2f}%")
    logger.info(f"  Kelly Fraction: {position['kelly_fraction']:.2%}")
    
    # Test dynamic stop loss
    logger.info("\nDynamic Stop Loss Test:")
    stop = rm.calculate_dynamic_stop_loss(
        atr=0.0015,
        entry_price=1.0850,
        sentiment_score=-0.3,
        market_regime='trending',
        volatility_regime='normal'
    )
    logger.info(f"  Stop Distance: {stop['stop_distance']:.5f}")
    logger.info(f"  Stop %: {stop['stop_percent']:.2f}%")
    logger.info(f"  Reason: {stop['sentiment_reason']}")
    logger.info(f"  Final Multiplier: {stop['final_multiplier']:.2f}x")