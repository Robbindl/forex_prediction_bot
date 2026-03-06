#!/usr/bin/env python3
"""
Example Script: Custom Usage of the Forex Prediction Bot

This script demonstrates various ways to use the bot programmatically
for custom analysis, backtesting, or integration into other systems.
"""

import sys
sys.path.append('.')

from data.fetcher import DataFetcher
from indicators.technical import TechnicalIndicators
from models.predictor import PredictionEngine
from utils.analysis import MarketAnalyzer, AlertSystem, ReportGenerator


def example_1_basic_analysis():
    """Example 1: Basic single-asset analysis"""
    print("\n" + "="*70)
    print("EXAMPLE 1: Basic Single-Asset Analysis")
    print("="*70 + "\n")
    
    # Fetch data
    fetcher = DataFetcher()
    df = fetcher.fetch_forex_data("EUR/USD", interval="1d", lookback=100)
    
    # Add indicators
    df = TechnicalIndicators.add_all_indicators(df)
    
    # Show latest data
    latest = df.iloc[-1]
    print(f"EUR/USD Current Data:")
    print(f"  Close: {latest['close']:.5f}")
    print(f"  RSI: {latest['rsi']:.2f}")
    print(f"  MACD: {latest['macd']:.5f}")
    
    # Check for alerts
    alerts = AlertSystem.generate_all_alerts(df)
    if alerts:
        print(f"\n⚠️  Active Alerts:")
        for alert in alerts:
            print(f"  - {alert['message']}")
    else:
        print("\n✓ No alerts")


def example_2_make_prediction():
    """Example 2: Train model and make prediction"""
    print("\n" + "="*70)
    print("EXAMPLE 2: Train Model and Make Prediction")
    print("="*70 + "\n")
    
    # Fetch and prepare data
    fetcher = DataFetcher()
    df = fetcher.fetch_stock_data("AAPL", interval="1d", lookback=200)
    df = TechnicalIndicators.add_all_indicators(df)
    
    # Train model
    print("Training XGBoost model for AAPL...")
    engine = PredictionEngine(model_type="xgboost")
    engine.train(df, target_periods=5)
    
    # Make prediction
    prediction = engine.predict_next(df)
    
    print(f"\nPrediction for AAPL:")
    print(f"  Current Price: ${prediction['current_price']:.2f}")
    print(f"  Predicted Price: ${prediction['predicted_price']:.2f}")
    print(f"  Direction: {prediction['direction']}")
    print(f"  Confidence: {prediction['confidence']:.1%}")
    print(f"  Expected Change: {prediction['price_change_pct']:+.2f}%")
    
    # Show feature importance
    importance = engine.get_feature_importance()
    print(f"\nTop 5 Most Important Features:")
    for idx, row in importance.head(5).iterrows():
        print(f"  {row['feature']}: {row['importance']:.4f}")


def example_3_correlation_analysis():
    """Example 3: Analyze correlations between multiple assets"""
    print("\n" + "="*70)
    print("EXAMPLE 3: Correlation Analysis")
    print("="*70 + "\n")
    
    # Fetch data for multiple assets
    fetcher = DataFetcher()
    
    assets = {
        "EUR/USD": fetcher.fetch_forex_data("EUR/USD", lookback=100),
        "GBP/USD": fetcher.fetch_forex_data("GBP/USD", lookback=100),
        "Gold": fetcher.fetch_commodity_data("GC=F", lookback=100),
        "Oil": fetcher.fetch_commodity_data("CL=F", lookback=100),
    }
    
    # Calculate correlation matrix
    corr_matrix = MarketAnalyzer.calculate_correlation_matrix(assets)
    
    print("Correlation Matrix:")
    print(corr_matrix.to_string())
    
    # Find highly correlated pairs
    high_corrs = MarketAnalyzer.find_correlated_assets(corr_matrix, threshold=0.6)
    
    if high_corrs:
        print(f"\nHighly Correlated Pairs (>0.6):")
        for asset1, asset2, corr in high_corrs:
            print(f"  {asset1} ↔ {asset2}: {corr:.3f}")


def example_4_risk_analysis():
    """Example 4: Calculate risk metrics"""
    print("\n" + "="*70)
    print("EXAMPLE 4: Risk Analysis")
    print("="*70 + "\n")
    
    # Fetch data
    fetcher = DataFetcher()
    df = fetcher.fetch_stock_data("TSLA", lookback=252)  # 1 year
    
    # Calculate risk metrics
    volatility = MarketAnalyzer.calculate_volatility(df, window=20)
    sharpe = MarketAnalyzer.calculate_sharpe_ratio(df)
    max_dd = MarketAnalyzer.calculate_max_drawdown(df)
    
    print("TSLA Risk Metrics (1 Year):")
    print(f"  Volatility (20-day): {volatility:.2%}")
    print(f"  Sharpe Ratio: {sharpe:.2f}")
    print(f"  Max Drawdown: {max_dd:.2%}")
    
    # Support/Resistance
    levels = MarketAnalyzer.detect_support_resistance(df)
    
    print(f"\nSupport Levels:")
    for level in levels['support']:
        print(f"  ${level:.2f}")
    
    print(f"\nResistance Levels:")
    for level in levels['resistance']:
        print(f"  ${level:.2f}")


def example_5_compare_models():
    """Example 5: Compare different ML models"""
    print("\n" + "="*70)
    print("EXAMPLE 5: Compare ML Models")
    print("="*70 + "\n")
    
    # Fetch and prepare data
    fetcher = DataFetcher()
    df = fetcher.fetch_forex_data("GBP/USD", lookback=200)
    df = TechnicalIndicators.add_all_indicators(df)
    
    models = ["rf", "xgboost", "ensemble"]
    predictions = {}
    
    for model_type in models:
        print(f"\nTraining {model_type.upper()} model...")
        engine = PredictionEngine(model_type=model_type)
        engine.train(df, target_periods=5)
        
        prediction = engine.predict_next(df)
        predictions[model_type] = prediction
        
        print(f"  Direction: {prediction['direction']}")
        print(f"  Confidence: {prediction['confidence']:.1%}")
        print(f"  Expected Change: {prediction['price_change_pct']:+.2f}%")
    
    print("\n" + "="*70)
    print("Model Comparison Summary:")
    print("="*70)
    
    for model_type, pred in predictions.items():
        print(f"{model_type.upper():10s}: {pred['direction']:5s} "
              f"({pred['confidence']:5.1%}) "
              f"{pred['price_change_pct']:+6.2f}%")


def example_6_portfolio_analysis():
    """Example 6: Analyze a portfolio of assets"""
    print("\n" + "="*70)
    print("EXAMPLE 6: Portfolio Analysis")
    print("="*70 + "\n")
    
    # Define portfolio
    portfolio = {
        "AAPL": 100,    # 100 shares
        "MSFT": 50,     # 50 shares
        "GOOGL": 25,    # 25 shares
        "TSLA": 30,     # 30 shares
    }
    
    fetcher = DataFetcher()
    total_value = 0
    portfolio_data = {}
    
    print("Portfolio Holdings:")
    print("-" * 50)
    
    for symbol, shares in portfolio.items():
        df = fetcher.fetch_stock_data(symbol, lookback=1)
        if not df.empty:
            current_price = df['close'].iloc[-1]
            position_value = shares * current_price
            total_value += position_value
            portfolio_data[symbol] = df
            
            print(f"{symbol:6s}: {shares:3d} shares @ ${current_price:8.2f} = ${position_value:10.2f}")
    
    print("-" * 50)
    print(f"{'TOTAL':6s}:                           ${total_value:10.2f}")
    
    # Calculate portfolio correlations
    if portfolio_data:
        print("\nPortfolio Correlation Matrix:")
        corr_matrix = MarketAnalyzer.calculate_correlation_matrix(portfolio_data)
        print(corr_matrix.to_string())


def example_7_generate_trading_signals():
    """Example 7: Generate trading signals for multiple assets"""
    print("\n" + "="*70)
    print("EXAMPLE 7: Generate Trading Signals")
    print("="*70 + "\n")
    
    assets = ["EUR/USD", "GBP/USD", "USD/JPY"]
    fetcher = DataFetcher()
    
    print("Scanning for trading signals...\n")
    
    for asset in assets:
        df = fetcher.fetch_forex_data(asset, lookback=100)
        df = TechnicalIndicators.add_all_indicators(df)
        
        print(f"{asset}:")
        
        # Get current indicators
        latest = df.iloc[-1]
        rsi = latest['rsi']
        macd = latest['macd']
        macd_signal = latest['macd_signal']
        
        # Generate signals
        signals = []
        
        if rsi < 30:
            signals.append("🟢 BUY: RSI oversold")
        elif rsi > 70:
            signals.append("🔴 SELL: RSI overbought")
        
        if macd > macd_signal and df['macd'].iloc[-2] < df['macd_signal'].iloc[-2]:
            signals.append("🟢 BUY: MACD bullish crossover")
        elif macd < macd_signal and df['macd'].iloc[-2] > df['macd_signal'].iloc[-2]:
            signals.append("🔴 SELL: MACD bearish crossover")
        
        if signals:
            for signal in signals:
                print(f"  {signal}")
        else:
            print("  ⚪ HOLD: No strong signals")
        
        print()


def main():
    """Run all examples"""
    print("\n" + "="*70)
    print("FOREX PREDICTION BOT - EXAMPLE USAGE SCRIPTS")
    print("="*70)
    
    try:
        # Run examples (comment out any you don't want to run)
        example_1_basic_analysis()
        example_2_make_prediction()
        example_3_correlation_analysis()
        example_4_risk_analysis()
        # example_5_compare_models()  # Takes longer
        example_6_portfolio_analysis()
        example_7_generate_trading_signals()
        
        print("\n" + "="*70)
        print("ALL EXAMPLES COMPLETED!")
        print("="*70 + "\n")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        print("Make sure all dependencies are installed: pip install -r requirements.txt")


if __name__ == "__main__":
    main()
