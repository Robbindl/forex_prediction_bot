"""
Utility functions for market analysis
Includes: Correlation analysis, Risk metrics, Alerts
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Any, Optional
from datetime import datetime
from logger import logger


class MarketAnalyzer:
    """Analyze market correlations and risk"""
    
    @staticmethod
    def calculate_correlation_matrix(data_dict: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        Calculate correlation matrix between multiple assets
        
        Args:
            data_dict: Dict of asset_name: DataFrame
            
        Returns:
            Correlation matrix
        """
        # Extract closing prices
        prices = pd.DataFrame()
        for name, df in data_dict.items():
            if not df.empty and 'close' in df.columns:
                prices[name] = df['close']
        
        # Calculate returns
        returns = prices.pct_change().dropna()
        
        # Correlation matrix
        corr_matrix = returns.corr()
        
        return corr_matrix
    
    @staticmethod
    def find_correlated_assets(corr_matrix: pd.DataFrame, 
                              threshold: float = 0.7) -> List[Tuple[str, str, float]]:
        """
        Find highly correlated asset pairs
        
        Args:
            corr_matrix: Correlation matrix
            threshold: Correlation threshold (0-1)
            
        Returns:
            List of (asset1, asset2, correlation) tuples
        """
        correlations: List[Tuple[str, str, float]] = []
        
        for i in range(len(corr_matrix.columns)):
            for j in range(i+1, len(corr_matrix.columns)):
                corr = corr_matrix.iloc[i, j]
                if abs(corr) >= threshold:
                    correlations.append((
                        corr_matrix.columns[i],
                        corr_matrix.columns[j],
                        corr
                    ))
        
        # Sort by absolute correlation
        correlations.sort(key=lambda x: abs(x[2]), reverse=True)
        
        return correlations
    
    @staticmethod
    def calculate_volatility(df: pd.DataFrame, window: int = 20) -> float:
        """Calculate rolling volatility (annualized)"""
        returns = df['close'].pct_change()
        volatility = returns.rolling(window=window).std() * np.sqrt(252)
        return volatility.iloc[-1]
    
    @staticmethod
    def calculate_sharpe_ratio(df: pd.DataFrame, risk_free_rate: float = 0.02) -> float:
        """Calculate Sharpe Ratio"""
        returns = df['close'].pct_change()
        excess_returns = returns.mean() * 252 - risk_free_rate
        volatility = returns.std() * np.sqrt(252)
        
        if volatility == 0:
            return 0
        
        return excess_returns / volatility
    
    @staticmethod
    def calculate_max_drawdown(df: pd.DataFrame) -> float:
        """Calculate maximum drawdown"""
        cumulative = (1 + df['close'].pct_change()).cumprod()
        running_max = cumulative.expanding().max()
        drawdown = (cumulative - running_max) / running_max
        
        return drawdown.min()
    
    @staticmethod
    def detect_support_resistance(df: pd.DataFrame, window: int = 20) -> Dict[str, List[float]]:
        """Detect support and resistance levels"""
        # Find local maxima and minima
        df['local_max'] = df['high'] == df['high'].rolling(window, center=True).max()
        df['local_min'] = df['low'] == df['low'].rolling(window, center=True).min()
        
        resistance_levels = df[df['local_max']]['high'].tail(3).values
        support_levels = df[df['local_min']]['low'].tail(3).values
        
        return {
            'resistance': resistance_levels.tolist(),
            'support': support_levels.tolist()
        }
    
    @staticmethod
    def calculate_risk_reward_ratio(entry: float, stop_loss: float, 
                                   take_profit: float) -> float:
        """Calculate risk/reward ratio for a trade"""
        risk = abs(entry - stop_loss)
        reward = abs(take_profit - entry)
        
        if risk == 0:
            return 0
        
        return reward / risk


class AlertSystem:
    """Generate trading alerts and signals"""
    
    @staticmethod
    def check_rsi_extremes(df: pd.DataFrame, overbought: float = 70,
                          oversold: float = 30) -> Dict[str, Any]:
        """Check for RSI extreme conditions"""
        if 'rsi' not in df.columns:
            return {}
        
        latest_rsi = df['rsi'].iloc[-1]
        
        if latest_rsi > overbought:
            return {
                'type': 'RSI_OVERBOUGHT',
                'message': f'RSI is overbought at {latest_rsi:.2f}',
                'severity': 'warning',
                'action': 'Consider selling or shorting'
            }
        elif latest_rsi < oversold:
            return {
                'type': 'RSI_OVERSOLD',
                'message': f'RSI is oversold at {latest_rsi:.2f}',
                'severity': 'warning',
                'action': 'Consider buying'
            }
        
        return {}
    
    @staticmethod
    def check_macd_crossover(df: pd.DataFrame) -> Dict[str, Any]:
        """Check for MACD crossovers"""
        if 'macd' not in df.columns or 'macd_signal' not in df.columns:
            return {}
        
        current_macd = df['macd'].iloc[-1]
        current_signal = df['macd_signal'].iloc[-1]
        prev_macd = df['macd'].iloc[-2]
        prev_signal = df['macd_signal'].iloc[-2]
        
        # Bullish crossover
        if prev_macd < prev_signal and current_macd > current_signal:
            return {
                'type': 'MACD_BULLISH_CROSS',
                'message': 'MACD bullish crossover detected',
                'severity': 'high',
                'action': 'Consider buying'
            }
        # Bearish crossover
        elif prev_macd > prev_signal and current_macd < current_signal:
            return {
                'type': 'MACD_BEARISH_CROSS',
                'message': 'MACD bearish crossover detected',
                'severity': 'high',
                'action': 'Consider selling'
            }
        
        return {}
    
    @staticmethod
    def check_bollinger_breakout(df: pd.DataFrame) -> Dict[str, Any]:
        """Check for Bollinger Band breakouts"""
        if 'bb_upper' not in df.columns or 'bb_lower' not in df.columns:
            return {}
        
        current_close = df['close'].iloc[-1]
        current_upper = df['bb_upper'].iloc[-1]
        current_lower = df['bb_lower'].iloc[-1]
        
        if current_close > current_upper:
            return {
                'type': 'BB_UPPER_BREAKOUT',
                'message': 'Price broke above upper Bollinger Band',
                'severity': 'medium',
                'action': 'Strong upward momentum or overbought'
            }
        elif current_close < current_lower:
            return {
                'type': 'BB_LOWER_BREAKOUT',
                'message': 'Price broke below lower Bollinger Band',
                'severity': 'medium',
                'action': 'Strong downward momentum or oversold'
            }
        
        return {}
    
    @staticmethod
    def check_moving_average_cross(df: pd.DataFrame) -> Dict[str, Any]:
        """Check for moving average crossovers"""
        if 'sma_20' not in df.columns or 'sma_50' not in df.columns:
            return {}
        
        current_20 = df['sma_20'].iloc[-1]
        current_50 = df['sma_50'].iloc[-1]
        prev_20 = df['sma_20'].iloc[-2]
        prev_50 = df['sma_50'].iloc[-2]
        
        # Golden cross
        if prev_20 < prev_50 and current_20 > current_50:
            return {
                'type': 'GOLDEN_CROSS',
                'message': 'Golden Cross: 20 SMA crossed above 50 SMA',
                'severity': 'high',
                'action': 'Strong bullish signal'
            }
        # Death cross
        elif prev_20 > prev_50 and current_20 < current_50:
            return {
                'type': 'DEATH_CROSS',
                'message': 'Death Cross: 20 SMA crossed below 50 SMA',
                'severity': 'high',
                'action': 'Strong bearish signal'
            }
        
        return {}
    
    @staticmethod
    def check_volume_spike(df: pd.DataFrame, threshold: float = 2.0) -> Dict[str, Any]:
        """Check for unusual volume spikes"""
        if 'volume' not in df.columns or df['volume'].sum() == 0:
            return {}
        
        current_volume = df['volume'].iloc[-1]
        avg_volume = df['volume'].rolling(20).mean().iloc[-1]
        
        if current_volume > avg_volume * threshold:
            return {
                'type': 'VOLUME_SPIKE',
                'message': f'Volume spike: {current_volume / avg_volume:.2f}x average',
                'severity': 'medium',
                'action': 'Increased market interest'
            }
        
        return {}
    
    @staticmethod
    def generate_all_alerts(df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Generate all applicable alerts"""
        alerts: List[Dict[str, Any]] = []
        
        alert = AlertSystem.check_rsi_extremes(df)
        if alert:
            alerts.append(alert)
        
        alert = AlertSystem.check_macd_crossover(df)
        if alert:
            alerts.append(alert)
        
        alert = AlertSystem.check_bollinger_breakout(df)
        if alert:
            alerts.append(alert)
        
        alert = AlertSystem.check_moving_average_cross(df)
        if alert:
            alerts.append(alert)
        
        alert = AlertSystem.check_volume_spike(df)
        if alert:
            alerts.append(alert)
        
        return alerts


class ReportGenerator:
    """Generate market analysis reports"""
    
    @staticmethod
    def generate_asset_report(symbol: str, df: pd.DataFrame, 
                             prediction: Optional[Dict[str, Any]] = None) -> str:
        """Generate detailed report for an asset"""
        report = f"\n{'='*60}\n"
        report += f"ANALYSIS REPORT: {symbol}\n"
        report += f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        report += f"{'='*60}\n\n"
        
        # Current price info
        latest = df.iloc[-1]
        report += f"CURRENT PRICE DATA:\n"
        report += f"  Close: {latest['close']:.4f}\n"
        report += f"  High:  {latest['high']:.4f}\n"
        report += f"  Low:   {latest['low']:.4f}\n"
        
        # Technical indicators
        if 'rsi' in df.columns:
            report += f"\nTECHNICAL INDICATORS:\n"
            report += f"  RSI(14): {latest['rsi']:.2f}\n"
        
        if 'macd' in df.columns:
            report += f"  MACD: {latest['macd']:.4f}\n"
            report += f"  Signal: {latest['macd_signal']:.4f}\n"
        
        if 'adx' in df.columns:
            report += f"  ADX: {latest['adx']:.2f} (Trend Strength)\n"
        
        if 'atr' in df.columns:
            report += f"  ATR: {latest['atr']:.4f} (Volatility)\n"
        
        # Prediction
        if prediction:
            report += f"\nPREDICTION:\n"
            report += f"  Direction: {prediction['direction']}\n"
            report += f"  Confidence: {prediction['confidence']:.2%}\n"
            report += f"  Predicted Price: {prediction['predicted_price']:.4f}\n"
            report += f"  Expected Change: {prediction['price_change_pct']:.2f}%\n"
        
        # Alerts
        alerts = AlertSystem.generate_all_alerts(df)
        if alerts:
            report += f"\nALERTS:\n"
            for alert in alerts:
                report += f"  [{alert['severity'].upper()}] {alert['message']}\n"
                report += f"    → {alert['action']}\n"
        
        # Risk metrics
        volatility = MarketAnalyzer.calculate_volatility(df)
        sharpe = MarketAnalyzer.calculate_sharpe_ratio(df)
        max_dd = MarketAnalyzer.calculate_max_drawdown(df)
        
        report += f"\nRISK METRICS:\n"
        report += f"  Volatility (20d): {volatility:.2%}\n"
        report += f"  Sharpe Ratio: {sharpe:.2f}\n"
        report += f"  Max Drawdown: {max_dd:.2%}\n"
        
        # Support/Resistance
        levels = MarketAnalyzer.detect_support_resistance(df)
        if levels['support']:
            report += f"\nSUPPORT LEVELS:\n"
            for level in levels['support']:
                report += f"  {level:.4f}\n"
        
        if levels['resistance']:
            report += f"\nRESISTANCE LEVELS:\n"
            for level in levels['resistance']:
                report += f"  {level:.4f}\n"
        
        report += f"\n{'='*60}\n"
        
        return report


if __name__ == "__main__":
    # Test utilities
    import yfinance as yf
    import sys
    sys.path.append('..')
    from indicators.technical import TechnicalIndicators
    
    ticker = yf.Ticker("EURUSD=X")
    df = ticker.history(period="100d")
    df.columns = df.columns.str.lower()
    df = TechnicalIndicators.add_all_indicators(df)
    
    # Test alerts
    alerts = AlertSystem.generate_all_alerts(df)
    logger.info(f"Found {len(alerts)} alerts")

    for alert in alerts:
        logger.info(f"  - {alert['message']}")

    # Test report
    report = ReportGenerator.generate_asset_report("EUR/USD", df)
    logger.info(report)