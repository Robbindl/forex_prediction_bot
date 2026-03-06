"""
📊 Enhanced Portfolio Optimizer with Correlation Checking
Balances risk across multiple assets and prevents correlated losses
"""

import pandas as pd
import numpy as np
from typing import List, Dict, Tuple, Optional
from scipy.optimize import minimize
from datetime import datetime, timedelta


class CorrelationChecker:
    """
    Prevents taking correlated positions that could blow up together
    """
    
    def __init__(self, max_correlation: float = 0.7):
        self.max_correlation = max_correlation
        self.correlation_cache = {}
        self.cache_ttl = 3600  # 1 hour
        self.last_update = {}
    
    def calculate_correlation(self, asset1: str, asset2: str, 
                              price_data: Dict[str, pd.Series]) -> float:
        """
        Calculate correlation between two assets using recent price data
        """
        if asset1 not in price_data or asset2 not in price_data:
            return 0.0
        
        # Get returns
        returns1 = price_data[asset1].pct_change().dropna()
        returns2 = price_data[asset2].pct_change().dropna()
        
        # Align dates
        common_dates = returns1.index.intersection(returns2.index)
        if len(common_dates) < 10:
            return 0.0
        
        r1_aligned = returns1.loc[common_dates]
        r2_aligned = returns2.loc[common_dates]
        
        # Calculate correlation
        correlation = r1_aligned.corr(r2_aligned)
        
        return correlation
    
    def check_new_position(self, new_asset: str, category: str,
                          open_positions: List[Dict],
                          price_data: Dict[str, pd.Series]) -> Tuple[bool, str]:
        """
        Check if new position is too correlated with existing positions
        
        Returns:
            (allowed, reason)
        """
        if not open_positions:
            return True, "No existing positions"
        
        # Group by category first - correlations matter more within same category
        same_category_positions = [
            p for p in open_positions 
            if p.get('category') == category
        ]
        
        if not same_category_positions:
            return True, "No same-category positions"
        
        # Check correlation with each same-category position
        for pos in same_category_positions:
            existing_asset = pos['asset']
            
            # Get from cache or calculate
            cache_key = f"{new_asset}:{existing_asset}"
            
            if cache_key in self.correlation_cache:
                corr, timestamp = self.correlation_cache[cache_key]
                if (datetime.now() - timestamp).seconds < self.cache_ttl:
                    correlation = corr
                else:
                    correlation = self.calculate_correlation(
                        new_asset, existing_asset, price_data
                    )
                    self.correlation_cache[cache_key] = (correlation, datetime.now())
            else:
                correlation = self.calculate_correlation(
                    new_asset, existing_asset, price_data
                )
                self.correlation_cache[cache_key] = (correlation, datetime.now())
            
            if abs(correlation) > self.max_correlation:
                direction = "same direction" if correlation > 0 else "opposite direction"
                return False, f"Too correlated with {existing_asset} ({correlation:.2f}, {direction})"
        
        return True, "Correlation check passed"
    
    def get_portfolio_correlation_matrix(self, open_positions: List[Dict],
                                        price_data: Dict[str, pd.Series]) -> pd.DataFrame:
        """
        Generate correlation matrix for current portfolio
        """
        assets = [p['asset'] for p in open_positions]
        
        if len(assets) < 2:
            return pd.DataFrame()
        
        # Get returns for all assets
        returns_dict = {}
        for asset in assets:
            if asset in price_data:
                returns_dict[asset] = price_data[asset].pct_change().dropna()
        
        # Create DataFrame
        returns_df = pd.DataFrame(returns_dict)
        
        # Calculate correlation matrix
        corr_matrix = returns_df.corr()
        
        return corr_matrix
    
    def suggest_diversification(self, open_positions: List[Dict],
                               available_assets: List[str],
                               price_data: Dict[str, pd.Series]) -> List[str]:
        """
        Suggest assets that would diversify current portfolio
        """
        if not open_positions:
            return available_assets[:5]
        
        suggestions = []
        
        for asset in available_assets:
            # Skip if already in portfolio
            if any(p['asset'] == asset for p in open_positions):
                continue
            
            # Check average correlation with portfolio
            correlations = []
            for pos in open_positions:
                corr = self.calculate_correlation(asset, pos['asset'], price_data)
                correlations.append(abs(corr))
            
            avg_corr = np.mean(correlations) if correlations else 0
            
            # Lower correlation = better diversification
            if avg_corr < 0.3:
                suggestions.append((asset, avg_corr))
        
        # Sort by lowest correlation first
        suggestions.sort(key=lambda x: x[1])
        
        return [s[0] for s in suggestions[:5]]


class EnhancedPortfolioOptimizer:
    """
    Advanced portfolio optimizer with correlation checking and risk management
    """
    
    def __init__(self, max_allocation=0.3, max_correlation=0.7):
        self.max_allocation = max_allocation  # Max 30% in one asset
        self.max_correlation = max_correlation
        self.positions = {}
        self.correlation_checker = CorrelationChecker(max_correlation=max_correlation)
        self.price_cache = {}  # Store historical prices for correlation calculation
        self.cache_manager = None

    def calculate_returns(self, prices_df):
        """Calculate daily returns"""
        return prices_df.pct_change().dropna()
    
    def calculate_covariance(self, returns_df):
        """Calculate covariance matrix"""
        return returns_df.cov() * 252  # Annualized
    
    def optimize_sharpe(self, returns_df):
        """Maximize Sharpe Ratio"""
        returns = self.calculate_returns(returns_df)
        cov_matrix = self.calculate_covariance(returns)
        
        n_assets = len(returns.columns)
        init_guess = np.array([1/n_assets] * n_assets)
        
        # Constraints: weights sum to 1
        constraints = {'type': 'eq', 'fun': lambda x: np.sum(x) - 1}
        
        # Bounds: no negative weights, max per asset
        bounds = tuple((0, self.max_allocation) for _ in range(n_assets))
        
        def neg_sharpe(weights):
            portfolio_return = np.sum(returns.mean() * weights) * 252
            portfolio_vol = np.sqrt(np.dot(weights.T, np.dot(cov_matrix, weights)))
            sharpe = (portfolio_return - 0.02) / portfolio_vol  # 2% risk-free rate
            return -sharpe
        
        result = minimize(
            neg_sharpe,
            init_guess,
            method='SLSQP',
            bounds=bounds,
            constraints=constraints
        )
        
        return result.x if result.success else init_guess
    
    def calculate_portfolio_risk(self, positions):
        """Calculate total portfolio risk"""
        total_value = sum(p['value'] for p in positions.values())
        if total_value == 0:
            return 0
        
        # Calculate weighted risk
        weighted_risk = 0
        for asset, pos in positions.items():
            weight = pos['value'] / total_value
            weighted_risk += weight * pos.get('risk_pct', 0)
        
        return weighted_risk
    
    def suggest_rebalance(self, positions, target_allocation):
        """Suggest trades to reach target allocation"""
        suggestions = []
        total_value = sum(p['value'] for p in positions.values())
        
        for asset, target_weight in zip(positions.keys(), target_allocation):
            current = positions[asset]
            target_value = total_value * target_weight
            difference = target_value - current['value']
            
            if abs(difference) > total_value * 0.01:  # 1% threshold
                suggestions.append({
                    'asset': asset,
                    'action': 'BUY' if difference > 0 else 'SELL',
                    'amount': abs(difference),
                    'reason': f'Rebalance to {target_weight:.1%} allocation'
                })
        
        return suggestions
    
    def get_diversification_score(self, positions):
        """Score how diversified the portfolio is (0-100)"""
        if len(positions) <= 1:
            return 0
        
        total_value = sum(p['value'] for p in positions.values())
        if total_value == 0:
            return 0
        
        # Herfindahl index (lower is better diversified)
        hhi = sum((p['value'] / total_value) ** 2 for p in positions.values())
        
        # Convert to 0-100 score (lower HHI = higher score)
        score = (1 - hhi) * 100
        return round(score, 2)
    
    # ========== NEW ENHANCED METHODS WITH CORRELATION CHECKING ==========
    
    def update_price_data(self, asset: str, price_series: pd.Series):
        """
        Update price cache for an asset
        """
        self.price_cache[asset] = price_series
    
    def check_position_correlation(self, new_asset: str, category: str,
                                  open_positions: List[Dict]) -> Tuple[bool, str]:
        """
        Check if new position is too correlated with existing positions
        Now with caching
        """
        if not open_positions:
            return True, "No existing positions"
        
        # Group by category first - correlations matter more within same category
        same_category_positions = [
            p for p in open_positions 
            if p.get('category') == category
        ]
        
        if not same_category_positions:
            return True, "No same-category positions"
        
        # Check correlation with each same-category position
        for pos in same_category_positions:
            existing_asset = pos['asset']
            
            # ===== CHECK CACHE FIRST =====
            if hasattr(self, 'cache_manager') and self.cache_manager:
                cached_corr = self.cache_manager.get_correlation(new_asset, existing_asset)
                if cached_corr is not None:
                    correlation = cached_corr
                else:
                    # Calculate and cache
                    correlation = self.calculate_correlation(
                        new_asset, existing_asset, self.price_cache
                    )
                    self.cache_manager.set_correlation(new_asset, existing_asset, correlation)
            else:
                # No cache, just calculate
                correlation = self.calculate_correlation(
                    new_asset, existing_asset, self.price_cache
                )
            # =============================
            
            if abs(correlation) > self.max_correlation:
                direction = "same direction" if correlation > 0 else "opposite direction"
                return False, f"Too correlated with {existing_asset} ({correlation:.2f}, {direction})"
        
        return True, "Correlation check passed"
    # ============================

    def set_cache_manager(self, cache_manager):
        """Set the cache manager for correlation caching"""
        self.cache_manager = cache_manager
        if cache_manager and cache_manager.enabled:
            print("  ✅ Correlation caching enabled")
    
    def get_correlation_warnings(self, open_positions: List[Dict]) -> List[str]:
        """
        Generate warnings for highly correlated positions in portfolio
        """
        warnings = []
        
        if len(open_positions) < 2:
            return warnings
        
        # Build price data
        price_data = {}
        for pos in open_positions:
            if pos['asset'] in self.price_cache:
                price_data[pos['asset']] = self.price_cache[pos['asset']]
        
        if len(price_data) < 2:
            return warnings
        
        # Get correlation matrix
        corr_matrix = self.correlation_checker.get_portfolio_correlation_matrix(
            open_positions, price_data
        )
        
        if corr_matrix.empty:
            return warnings
        
        # Find highly correlated pairs
        for i in range(len(corr_matrix.columns)):
            for j in range(i+1, len(corr_matrix.columns)):
                asset1 = corr_matrix.columns[i]
                asset2 = corr_matrix.columns[j]
                corr = corr_matrix.iloc[i, j]
                
                if abs(corr) > self.max_correlation:
                    warnings.append(
                        f"High correlation ({corr:.2f}) between {asset1} and {asset2}"
                    )
        
        return warnings
    
    def suggest_diversification_assets(self, open_positions: List[Dict],
                                      available_assets: List[str]) -> List[str]:
        """
        Suggest new assets that would diversify the current portfolio
        """
        # Build price data
        price_data = {}
        for pos in open_positions:
            if pos['asset'] in self.price_cache:
                price_data[pos['asset']] = self.price_cache[pos['asset']]
        
        for asset in available_assets:
            if asset in self.price_cache:
                price_data[asset] = self.price_cache[asset]
        
        return self.correlation_checker.suggest_diversification(
            open_positions, available_assets, price_data
        )
    
    def calculate_portfolio_var(self, open_positions: List[Dict], 
                               confidence: float = 0.95) -> float:
        """
        Calculate Value at Risk for the portfolio
        """
        if len(open_positions) < 1:
            return 0.0
        
        # Build returns matrix
        returns_dict = {}
        for pos in open_positions:
            if pos['asset'] in self.price_cache:
                returns_dict[pos['asset']] = self.price_cache[pos['asset']].pct_change().dropna()
        
        if not returns_dict:
            return 0.0
        
        returns_df = pd.DataFrame(returns_dict)
        
        # Calculate portfolio returns (weighted)
        total_value = sum(p['value'] for p in open_positions)
        weights = [p['value'] / total_value for p in open_positions]
        
        portfolio_returns = (returns_df * weights).sum(axis=1)
        
        # Calculate VaR
        var = np.percentile(portfolio_returns, (1 - confidence) * 100)
        
        return float(var) * total_value  # Return in dollars
    
    def get_portfolio_health_report(self, open_positions: List[Dict]) -> Dict:
        """
        Generate comprehensive portfolio health report
        """
        if not open_positions:
            return {'status': 'No open positions'}
        
        total_value = sum(p['value'] for p in open_positions)
        
        # Calculate diversification score
        positions_dict = {}
        for pos in open_positions:
            positions_dict[pos['asset']] = {
                'value': pos['value'],
                'category': pos.get('category', 'unknown'),
                'risk_pct': pos.get('risk_pct', 1.0)
            }
        
        div_score = self.get_diversification_score(positions_dict)
        
        # Get correlation warnings
        warnings = self.get_correlation_warnings(open_positions)
        
        # Calculate VaR
        var_95 = self.calculate_portfolio_var(open_positions, 0.95)
        
        # Category breakdown
        categories = {}
        for pos in open_positions:
            cat = pos.get('category', 'unknown')
            if cat not in categories:
                categories[cat] = {'count': 0, 'value': 0.0}
            categories[cat]['count'] += 1
            categories[cat]['value'] += pos['value']
        
        return {
            'total_positions': len(open_positions),
            'total_value': round(total_value, 2),
            'diversification_score': div_score,
            'var_95': round(var_95, 2),
            'var_95_percent': round(var_95 / total_value * 100, 2) if total_value > 0 else 0,
            'warnings': warnings,
            'category_breakdown': categories,
            'needs_rebalancing': div_score < 50 or len(warnings) > 0
        }


# Keep original class name for backward compatibility
class PortfolioOptimizer(EnhancedPortfolioOptimizer):
    """Alias for backward compatibility"""
    pass