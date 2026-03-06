"""
Market Regime Detection & Sentiment Analysis
Identify market conditions and incorporate news sentiment
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple
from enum import Enum
import requests
from datetime import datetime, timedelta


class MarketRegime(Enum):
    """Market regime classifications"""
    BULL_TRENDING = "bull_trending"
    BEAR_TRENDING = "bear_trending"
    BULL_VOLATILE = "bull_volatile"
    BEAR_VOLATILE = "bear_volatile"
    RANGING_CALM = "ranging_calm"
    RANGING_VOLATILE = "ranging_volatile"
    BREAKOUT_BULLISH = "breakout_bullish"
    BREAKOUT_BEARISH = "breakout_bearish"


class MarketRegimeDetector:
    """
    Advanced market regime detection
    Identifies current market conditions for strategy adaptation
    """
    
    @staticmethod
    def detect_regime(df: pd.DataFrame) -> Tuple[MarketRegime, float]:
        """
        Detect current market regime
        
        Returns:
            (regime, confidence)
        """
        # Calculate metrics
        adx = df['adx'].iloc[-1] if 'adx' in df.columns else 20
        rsi = df['rsi'].iloc[-1] if 'rsi' in df.columns else 50
        
        # Trend direction
        sma_20 = df['sma_20'].iloc[-1] if 'sma_20' in df.columns else df['close'].iloc[-1]
        sma_50 = df['sma_50'].iloc[-1] if 'sma_50' in df.columns else df['close'].iloc[-1]
        price = df['close'].iloc[-1]
        
        # Volatility
        returns = df['close'].pct_change()
        volatility = returns.rolling(20).std().iloc[-1]
        
        # Bollinger Band width
        if 'bb_upper' in df.columns and 'bb_lower' in df.columns:
            bb_width = (df['bb_upper'].iloc[-1] - df['bb_lower'].iloc[-1]) / df['close'].iloc[-1]
        else:
            bb_width = 0.02
        
        # Volume trend
        volume_ratio = 1.0
        if 'volume' in df.columns and df['volume'].sum() > 0:
            avg_volume = df['volume'].rolling(20).mean().iloc[-1]
            current_volume = df['volume'].iloc[-1]
            volume_ratio = current_volume / (avg_volume + 1)
        
        # Decision logic
        is_trending = adx > 25
        is_bullish = price > sma_20 and sma_20 > sma_50
        is_volatile = volatility > 0.02 or bb_width > 0.04
        is_breakout = volume_ratio > 1.5 and abs(returns.iloc[-1]) > 0.015
        
        confidence = 0.5
        
        # Determine regime
        if is_breakout:
            if is_bullish:
                regime = MarketRegime.BREAKOUT_BULLISH
                confidence = min(0.9, 0.6 + (volume_ratio - 1.5) * 0.2)
            else:
                regime = MarketRegime.BREAKOUT_BEARISH
                confidence = min(0.9, 0.6 + (volume_ratio - 1.5) * 0.2)
        
        elif is_trending:
            if is_bullish:
                if is_volatile:
                    regime = MarketRegime.BULL_VOLATILE
                    confidence = 0.7
                else:
                    regime = MarketRegime.BULL_TRENDING
                    confidence = 0.85
            else:
                if is_volatile:
                    regime = MarketRegime.BEAR_VOLATILE
                    confidence = 0.7
                else:
                    regime = MarketRegime.BEAR_TRENDING
                    confidence = 0.85
        
        else:  # Ranging
            if is_volatile:
                regime = MarketRegime.RANGING_VOLATILE
                confidence = 0.6
            else:
                regime = MarketRegime.RANGING_CALM
                confidence = 0.65
        
        return regime, confidence
    
    @staticmethod
    def get_regime_strategy(regime: MarketRegime) -> Dict[str, any]:
        """
        Get recommended strategy parameters for regime
        
        Returns:
            Dict with strategy adjustments
        """
        strategies = {
            MarketRegime.BULL_TRENDING: {
                'bias': 'long',
                'risk_multiplier': 1.5,
                'take_profit_ratio': 3.0,
                'trailing_stop': True,
                'min_confidence': 0.65,
                'description': 'Strong uptrend - favor long positions'
            },
            MarketRegime.BEAR_TRENDING: {
                'bias': 'short',
                'risk_multiplier': 1.3,
                'take_profit_ratio': 2.5,
                'trailing_stop': True,
                'min_confidence': 0.70,
                'description': 'Strong downtrend - favor short positions'
            },
            MarketRegime.BULL_VOLATILE: {
                'bias': 'long',
                'risk_multiplier': 0.8,
                'take_profit_ratio': 2.0,
                'trailing_stop': False,
                'min_confidence': 0.75,
                'description': 'Volatile uptrend - reduce position size'
            },
            MarketRegime.BEAR_VOLATILE: {
                'bias': 'short',
                'risk_multiplier': 0.7,
                'take_profit_ratio': 1.8,
                'trailing_stop': False,
                'min_confidence': 0.75,
                'description': 'Volatile downtrend - reduce position size'
            },
            MarketRegime.RANGING_CALM: {
                'bias': 'neutral',
                'risk_multiplier': 0.6,
                'take_profit_ratio': 1.5,
                'trailing_stop': False,
                'min_confidence': 0.80,
                'description': 'Range-bound - use mean reversion'
            },
            MarketRegime.RANGING_VOLATILE: {
                'bias': 'neutral',
                'risk_multiplier': 0.4,
                'take_profit_ratio': 1.2,
                'trailing_stop': False,
                'min_confidence': 0.85,
                'description': 'Choppy market - minimize trading'
            },
            MarketRegime.BREAKOUT_BULLISH: {
                'bias': 'long',
                'risk_multiplier': 1.8,
                'take_profit_ratio': 3.5,
                'trailing_stop': True,
                'min_confidence': 0.70,
                'description': 'Bullish breakout - aggressive long'
            },
            MarketRegime.BREAKOUT_BEARISH: {
                'bias': 'short',
                'risk_multiplier': 1.6,
                'take_profit_ratio': 3.0,
                'trailing_stop': True,
                'min_confidence': 0.70,
                'description': 'Bearish breakout - aggressive short'
            }
        }
        
        return strategies.get(regime, strategies[MarketRegime.RANGING_CALM])


class SentimentAnalyzer:
    """
    News and social media sentiment analysis
    (Placeholder for API integrations)
    """
    
    @staticmethod
    def get_crypto_fear_greed_index() -> Dict[str, any]:
        """
        Get Fear & Greed Index for crypto
        Free API: alternative.me
        """
        try:
            url = "https://api.alternative.me/fng/"
            response = requests.get(url, timeout=5)
            data = response.json()
            
            if 'data' in data and len(data['data']) > 0:
                latest = data['data'][0]
                return {
                    'value': int(latest['value']),
                    'classification': latest['value_classification'],
                    'timestamp': latest['timestamp'],
                    'sentiment_score': int(latest['value']) / 100  # Normalize 0-1
                }
        except:
            pass
        
        return {'value': 50, 'classification': 'Neutral', 'sentiment_score': 0.5}
    
    @staticmethod
    def analyze_sentiment_impact(
        sentiment_score: float,
        signal_direction: str
    ) -> Tuple[float, str]:
        """
        Adjust signal confidence based on sentiment
        
        Returns:
            (adjusted_confidence, explanation)
        """
        # Extreme fear (0-0.25) = contrarian bullish
        # Fear (0.25-0.45) = slight bullish
        # Neutral (0.45-0.55) = no change
        # Greed (0.55-0.75) = slight bearish
        # Extreme greed (0.75-1.0) = contrarian bearish
        
        if signal_direction == 'BUY':
            if sentiment_score < 0.25:  # Extreme fear
                adjustment = 1.2  # Boost confidence
                reason = "Extreme fear - contrarian buy opportunity"
            elif sentiment_score < 0.45:
                adjustment = 1.1
                reason = "Fear in market - favorable for buying"
            elif sentiment_score > 0.75:  # Extreme greed
                adjustment = 0.8  # Reduce confidence
                reason = "Extreme greed - caution on longs"
            elif sentiment_score > 0.55:
                adjustment = 0.9
                reason = "Greed in market - be cautious"
            else:
                adjustment = 1.0
                reason = "Neutral sentiment"
        
        else:  # SELL
            if sentiment_score > 0.75:  # Extreme greed
                adjustment = 1.2  # Boost confidence
                reason = "Extreme greed - contrarian sell opportunity"
            elif sentiment_score > 0.55:
                adjustment = 1.1
                reason = "Greed in market - favorable for selling"
            elif sentiment_score < 0.25:  # Extreme fear
                adjustment = 0.8  # Reduce confidence
                reason = "Extreme fear - caution on shorts"
            elif sentiment_score < 0.45:
                adjustment = 0.9
                reason = "Fear in market - be cautious on shorts"
            else:
                adjustment = 1.0
                reason = "Neutral sentiment"
        
        return adjustment, reason
    
    @staticmethod
    def get_multi_asset_sentiment() -> Dict[str, float]:
        """
        Get sentiment scores for different asset classes
        
        Returns:
            Dict with sentiment scores (0-1) for each class
        """
        sentiment = {}
        
        # Crypto sentiment
        crypto_fg = SentimentAnalyzer.get_crypto_fear_greed_index()
        sentiment['crypto'] = crypto_fg['sentiment_score']
        
        # Stock market sentiment (VIX-based estimate)
        # Could integrate with actual VIX API
        sentiment['stocks'] = 0.5  # Placeholder
        
        # Forex sentiment (placeholder)
        sentiment['forex'] = 0.5
        
        # Commodities sentiment (placeholder)
        sentiment['commodities'] = 0.5
        
        return sentiment


class MarketCorrelationAnalyzer:
    """
    Analyze correlations between assets for portfolio construction
    """
    
    @staticmethod
    def calculate_rolling_correlation(
        df1: pd.DataFrame,
        df2: pd.DataFrame,
        window: int = 20
    ) -> pd.Series:
        """Calculate rolling correlation between two assets"""
        returns1 = df1['close'].pct_change()
        returns2 = df2['close'].pct_change()
        
        return returns1.rolling(window).corr(returns2)
    
    @staticmethod
    def find_diversification_opportunities(
        correlations: pd.DataFrame,
        current_holdings: List[str],
        threshold: float = 0.3
    ) -> List[str]:
        """
        Find assets with low correlation to current portfolio
        
        Returns:
            List of uncorrelated assets
        """
        if not current_holdings:
            return list(correlations.columns)
        
        uncorrelated = []
        
        for asset in correlations.columns:
            if asset in current_holdings:
                continue
            
            # Check correlation with all current holdings
            max_corr = max([
                abs(correlations.loc[asset, holding])
                for holding in current_holdings
                if holding in correlations.columns
            ])
            
            if max_corr < threshold:
                uncorrelated.append(asset)
        
        return uncorrelated


if __name__ == "__main__":
    # Test regime detection
    print("Market Regime Detection Test")
    print("="*60)
    
    # Create sample data
    dates = pd.date_range('2024-01-01', periods=100)
    df = pd.DataFrame({
        'close': np.random.randn(100).cumsum() + 100,
        'high': np.random.randn(100).cumsum() + 101,
        'low': np.random.randn(100).cumsum() + 99,
        'volume': np.random.randint(1000, 10000, 100),
        'adx': np.random.uniform(15, 35, 100),
        'rsi': np.random.uniform(30, 70, 100),
        'sma_20': np.random.randn(100).cumsum() + 99,
        'sma_50': np.random.randn(100).cumsum() + 98,
        'bb_upper': np.random.randn(100).cumsum() + 102,
        'bb_lower': np.random.randn(100).cumsum() + 98
    }, index=dates)
    
    detector = MarketRegimeDetector()
    regime, confidence = detector.detect_regime(df)
    
    print(f"Detected Regime: {regime.value}")
    print(f"Confidence: {confidence:.1%}")
    
    strategy = detector.get_regime_strategy(regime)
    print(f"\nRecommended Strategy:")
    print(f"  Bias: {strategy['bias']}")
    print(f"  Risk Multiplier: {strategy['risk_multiplier']}")
    print(f"  Min Confidence: {strategy['min_confidence']:.1%}")
    print(f"  Description: {strategy['description']}")
    
    # Test sentiment
    print("\n" + "="*60)
    print("Sentiment Analysis Test")
    print("="*60)
    
    analyzer = SentimentAnalyzer()
    fg_index = analyzer.get_crypto_fear_greed_index()
    
    print(f"Crypto Fear & Greed Index: {fg_index['value']}")
    print(f"Classification: {fg_index['classification']}")
    
    adjustment, reason = analyzer.analyze_sentiment_impact(
        fg_index['sentiment_score'],
        'BUY'
    )
    
    print(f"\nSentiment Impact on BUY signal:")
    print(f"  Adjustment: {adjustment:.2f}x")
    print(f"  Reason: {reason}")
