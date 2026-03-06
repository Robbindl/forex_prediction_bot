# debug_predictor.py
import pandas as pd
import numpy as np
import yfinance as yf
from advanced_predictor import AdvancedPredictionEngine
from indicators.technical import TechnicalIndicators

print("="*70)
print("DEBUGGING ADVANCED PREDICTOR")
print("="*70)

# Get data
print("\n📊 Fetching AAPL data...")
ticker = yf.Ticker('AAPL')
df = ticker.history(period="3mo")
df.columns = df.columns.str.lower()
print(f"Initial data shape: {df.shape}")
print(f"Initial rows: {len(df)}")
print(f"Columns: {df.columns.tolist()}")
print(f"First date: {df.index[0]}, Last date: {df.index[-1]}")

# Add indicators
print("\n📈 Adding technical indicators...")
df_indicators = TechnicalIndicators.add_all_indicators(df)
print(f"After indicators shape: {df_indicators.shape}")
print(f"NaN count after indicators: {df_indicators.isna().sum().sum()}")

# Create features
print("\n🔧 Creating advanced features...")
engine = AdvancedPredictionEngine()
df_features = engine.create_advanced_features(df_indicators)
print(f"After advanced features shape: {df_features.shape}")

# Select feature columns
exclude_cols = ['open', 'high', 'low', 'close', 'volume', 'date']
feature_cols = [col for col in df_features.columns 
                if col not in exclude_cols 
                and df_features[col].dtype in ['float64', 'int64']]
print(f"Feature columns count: {len(feature_cols)}")

# Create target
df_features['target'] = df_features['close'].pct_change(5).shift(-5)
print(f"After target creation shape: {df_features.shape}")

# Remove rows with NaN in target
df_no_nan_target = df_features.dropna(subset=['target'])
print(f"After dropping NaN target: {len(df_no_nan_target)} rows")

# Fill NaN values in features
df_filled = df_no_nan_target.copy()
df_filled[feature_cols] = df_filled[feature_cols].ffill().bfill()

# Drop any remaining NaN
df_clean = df_filled.dropna()
print(f"After all cleaning: {len(df_clean)} rows")

print("\n" + "="*70)
print("RESULTS:")
print(f"Started with: {len(df)} rows")
print(f"Ended with: {len(df_clean)} rows")
if len(df_clean) > 0:
    print("✅ SUCCESS: Data survived preprocessing!")
else:
    print("❌ FAILURE: All data was dropped")

# Show sample of what's left
if len(df_clean) > 0:
    print("\n📊 Sample of final data:")
    print(df_clean[feature_cols[:5]].head())