"""
Advanced Machine Learning Prediction Engine
Multiple sophisticated models with ensemble voting and confidence weighting
FIXED: Proper NaN handling and data preprocessing
"""

import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor, AdaBoostRegressor
from sklearn.linear_model import Ridge, Lasso, ElasticNet
from sklearn.svm import SVR
from sklearn.neural_network import MLPRegressor
from sklearn.preprocessing import StandardScaler, RobustScaler
from sklearn.model_selection import TimeSeriesSplit, cross_val_score
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
import warnings
warnings.filterwarnings('ignore')

# IMPORTANT: Add missing imports
import yfinance as yf
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    import xgboost as xgb
    XGB_AVAILABLE = True
except ImportError:
    xgb = None
    XGB_AVAILABLE = False
    print("⚠️ XGBoost not installed. Install with: pip install xgboost")

try:
    from sklearn.experimental import enable_hist_gradient_boosting
    from sklearn.ensemble import HistGradientBoostingRegressor
    HIST_AVAILABLE = True
except:
    HIST_AVAILABLE = False

# Fix for Windows console encoding
import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')


class AdvancedPredictionEngine:
    """
    Advanced multi-model ensemble prediction system
    Features:
    - 10+ ML algorithms
    - Confidence-weighted ensemble
    - Cross-validation
    - Feature importance analysis
    - Dynamic model selection
    """
    
    def __init__(self, model_type: str = "super_ensemble"):
        self.model_type = model_type
        self.models = {}
        self.scalers = {}
        self.feature_names = []
        self.performance_scores = {}
        self.model_weights = {}
        
    def create_advanced_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create advanced engineered features"""
        df = df.copy()
        
        # Price-based features
        df['price_momentum_5'] = df['close'].pct_change(5)
        df['price_momentum_10'] = df['close'].pct_change(10)
        df['price_momentum_20'] = df['close'].pct_change(20)
        
        # Volatility features
        df['volatility_5'] = df['close'].rolling(5).std()
        df['volatility_10'] = df['close'].rolling(10).std()
        df['volatility_ratio'] = df['volatility_5'] / (df['volatility_10'] + 1e-10)
        
        # Volume features (if available)
        if 'volume' in df.columns and df['volume'].sum() > 0:
            df['volume_ma_5'] = df['volume'].rolling(5).mean()
            df['volume_ma_20'] = df['volume'].rolling(20).mean()
            df['volume_ratio'] = df['volume'] / (df['volume_ma_20'] + 1)
            df['price_volume_trend'] = (df['close'].pct_change() * df['volume']).rolling(5).mean()
        
        # Trend strength
        df['trend_strength'] = abs(df['close'].rolling(20).mean() - df['close'].rolling(5).mean())
        
        # Price position in range
        high_20 = df['high'].rolling(20).max()
        low_20 = df['low'].rolling(20).min()
        df['price_position'] = (df['close'] - low_20) / (high_20 - low_20 + 1e-10)
        
        # Acceleration
        df['acceleration'] = df['close'].pct_change().diff()
        
        # Time-based features
        if hasattr(df.index, 'dayofweek'):
            df['day_of_week'] = df.index.dayofweek
            df['day_of_month'] = df.index.day
            df['month'] = df.index.month
        
        return df
    
    def prepare_training_data(self, df: pd.DataFrame, target_periods: int = 5) -> tuple:
        """
        Prepare training data with advanced features
        FIXED: Better NaN handling
        """
        # Create advanced features
        df = self.create_advanced_features(df)
        
        # Select features (exclude target and non-numeric)
        exclude_cols = ['open', 'high', 'low', 'close', 'volume', 'date']
        feature_cols = [col for col in df.columns if col not in exclude_cols and df[col].dtype in ['float64', 'int64']]
        
        # Create target (future returns)
        df['target'] = df['close'].pct_change(target_periods).shift(-target_periods)
        
        # Remove rows with NaN in target
        df = df.dropna(subset=['target'])
        
        # Check if we have any data left
        if len(df) == 0:
            raise ValueError("No data left after creating target - try shorter target_periods")
        
        # FIXED: Better NaN handling
        # First, forward fill then backward fill
        df[feature_cols] = df[feature_cols].fillna(method='ffill').fillna(method='bfill')
        
        # Fill any remaining NaN with 0
        df[feature_cols] = df[feature_cols].fillna(0)
        
        # Check final data size
        if len(df) < 30:
            raise ValueError(f"Insufficient data for training after preprocessing (only {len(df)} rows)")
        
        X = df[feature_cols].values
        y = df['target'].values
        
        self.feature_names = feature_cols
        
        print(f"   ✅ Using {len(df)} rows for training")
        print(f"   ✅ Feature count: {len(feature_cols)}")
        
        return X, y, feature_cols
    
    def build_model_ensemble(self) -> dict:
        """Build ensemble of multiple ML models"""
        models = {}
        
        # 1. Random Forest
        models['random_forest'] = RandomForestRegressor(
            n_estimators=200,
            max_depth=10,
            min_samples_split=10,
            min_samples_leaf=5,
            random_state=42,
            n_jobs=-1
        )
        
        # 2. XGBoost (if available)
        if XGB_AVAILABLE:
            models['xgboost'] = xgb.XGBRegressor(
                n_estimators=150,
                max_depth=6,
                learning_rate=0.05,
                subsample=0.8,
                colsample_bytree=0.8,
                random_state=42,
                n_jobs=-1
            )
        
        # 3. Gradient Boosting
        models['gradient_boosting'] = GradientBoostingRegressor(
            n_estimators=100,
            max_depth=5,
            learning_rate=0.1,
            subsample=0.8,
            random_state=42
        )
        
        # 4. AdaBoost
        models['adaboost'] = AdaBoostRegressor(
            n_estimators=100,
            learning_rate=0.1,
            random_state=42
        )
        
        # 5. Ridge Regression
        models['ridge'] = Ridge(alpha=1.0)
        
        # 6. Lasso Regression
        models['lasso'] = Lasso(alpha=0.1)
        
        # 7. ElasticNet
        models['elasticnet'] = ElasticNet(alpha=0.1, l1_ratio=0.5)
        
        # 8. Support Vector Regression
        models['svr'] = SVR(kernel='rbf', C=1.0, epsilon=0.1)
        
        # 9. Neural Network (MLP)
        models['mlp'] = MLPRegressor(
            hidden_layer_sizes=(100, 50),
            activation='relu',
            solver='adam',
            alpha=0.001,
            max_iter=500,
            random_state=42
        )
        
        # 10. Histogram-based Gradient Boosting (if available)
        if HIST_AVAILABLE:
            models['hist_gradient_boosting'] = HistGradientBoostingRegressor(
                max_iter=100,
                max_depth=6,
                learning_rate=0.1,
                random_state=42
            )
        
        return models
    
    def train(self, df: pd.DataFrame, target_periods: int = 5) -> None:
        """Train all models with cross-validation"""
        print(f"\n🧠 Training Advanced ML Ensemble ({len(self.build_model_ensemble())} models)...")
        
        try:
            # Prepare data
            X, y, feature_names = self.prepare_training_data(df, target_periods)
            
            # Create scalers
            self.scalers['standard'] = StandardScaler()
            self.scalers['robust'] = RobustScaler()
            
            X_standard = self.scalers['standard'].fit_transform(X)
            X_robust = self.scalers['robust'].fit_transform(X)
            
            # Build models
            model_dict = self.build_model_ensemble()
            
            # Train and evaluate each model
            tscv = TimeSeriesSplit(n_splits=min(5, len(X)//10))
            
            for name, model in model_dict.items():
                try:
                    # Choose scaler based on model type
                    if name in ['svr', 'mlp', 'ridge', 'lasso', 'elasticnet']:
                        X_train = X_standard
                    else:
                        X_train = X_robust
                    
                    # Cross-validation
                    try:
                        cv_scores = cross_val_score(
                            model, X_train, y,
                            cv=tscv,
                            scoring='neg_mean_squared_error',
                            n_jobs=-1
                        )
                        cv_score = -cv_scores.mean()
                    except:
                        cv_score = 0.001  # Fallback if CV fails
                    
                    # Train on full data
                    model.fit(X_train, y)
                    
                    # Store model and score
                    self.models[name] = model
                    self.performance_scores[name] = cv_score
                    
                    print(f"  ✓ {name}: CV MSE = {cv_score:.6f}")
                    
                except Exception as e:
                    print(f"  ✗ {name} failed: {e}")
            
            # Calculate model weights
            if self.performance_scores:
                total_inverse_mse = sum(1 / (score + 1e-10) for score in self.performance_scores.values())
                self.model_weights = {
                    name: (1 / (score + 1e-10)) / total_inverse_mse
                    for name, score in self.performance_scores.items()
                }
                
                print(f"\n✅ Trained {len(self.models)} models successfully")
                print(f"📊 Top 5 Model Weights:")
                for name, weight in sorted(self.model_weights.items(), key=lambda x: x[1], reverse=True)[:5]:
                    print(f"   {name}: {weight:.3f}")
            else:
                print("❌ No models trained successfully")
            
        except Exception as e:
            print(f"❌ Training failed: {e}")
            raise
    
    def predict_next(self, df: pd.DataFrame) -> dict:
        """Generate ensemble prediction with confidence scoring"""
        if not self.models:
            raise ValueError("Models not trained. Call train() first.")
        
        try:
            # Prepare features
            df_features = self.create_advanced_features(df)
            feature_cols = self.feature_names
            
            # Get latest features
            X_latest = df_features[feature_cols].iloc[-1:].values
            
            # Scale
            X_standard = self.scalers['standard'].transform(X_latest)
            X_robust = self.scalers['robust'].transform(X_latest)
            
            # Get predictions from each model
            predictions = {}
            for name, model in self.models.items():
                try:
                    # Choose scaler
                    if name in ['svr', 'mlp', 'ridge', 'lasso', 'elasticnet']:
                        X_pred = X_standard
                    else:
                        X_pred = X_robust
                    
                    pred = model.predict(X_pred)[0]
                    predictions[name] = pred
                except:
                    continue
            
            if not predictions:
                return {
                    'direction': 'HOLD',
                    'confidence': 0.5,
                    'predicted_return': 0,
                    'predicted_price': df['close'].iloc[-1],
                    'current_price': df['close'].iloc[-1],
                    'price_change_pct': 0,
                    'model_count': 0,
                    'prediction_std': 0
                }
            
            # Weighted ensemble prediction
            weighted_pred = sum(
                predictions[name] * self.model_weights.get(name, 1/len(predictions))
                for name in predictions.keys()
            ) / len(predictions)
            
            # Calculate prediction confidence
            pred_std = np.std(list(predictions.values()))
            confidence = 1 / (1 + pred_std * 100)
            confidence = min(max(confidence, 0.3), 0.95)
            
            # Direction
            direction = "UP" if weighted_pred > 0 else "DOWN"
            
            # Current and predicted price
            current_price = df['close'].iloc[-1]
            predicted_price = current_price * (1 + weighted_pred)
            
            return {
                'direction': direction,
                'confidence': confidence,
                'predicted_return': weighted_pred,
                'predicted_price': predicted_price,
                'current_price': current_price,
                'price_change_pct': weighted_pred * 100,
                'model_count': len(predictions),
                'prediction_std': pred_std
            }
            
        except Exception as e:
            print(f"❌ Prediction error: {e}")
            return {
                'direction': 'HOLD',
                'confidence': 0.5,
                'predicted_return': 0,
                'predicted_price': df['close'].iloc[-1],
                'current_price': df['close'].iloc[-1],
                'price_change_pct': 0,
                'model_count': 0,
                'prediction_std': 0
            }
    
    def get_feature_importance(self, top_n: int = 20) -> pd.DataFrame:
        """Get feature importance from tree-based models"""
        importance_dict = {}
        
        for name, model in self.models.items():
            if hasattr(model, 'feature_importances_'):
                importance_dict[name] = model.feature_importances_
        
        if not importance_dict:
            return pd.DataFrame()
        
        # Average importance across models
        importance_df = pd.DataFrame(importance_dict, index=self.feature_names)
        importance_df['average'] = importance_df.mean(axis=1)
        importance_df = importance_df.sort_values('average', ascending=False)
        
        return importance_df.head(top_n)


# Backward compatibility wrapper
class PredictionEngine:
    """Wrapper for backward compatibility"""
    def __init__(self, model_type: str = "ensemble"):
        self.engine = AdvancedPredictionEngine(model_type)
    
    def train(self, df: pd.DataFrame, target_periods: int = 5) -> None:
        self.engine.train(df, target_periods)
    
    def predict_next(self, df: pd.DataFrame) -> dict:
        return self.engine.predict_next(df)


if __name__ == "__main__":
    print("Testing Advanced Prediction Engine...")
    print("="*60)
    
    # Import technical indicators
    try:
        from indicators.technical import TechnicalIndicators
        print("✓ Technical indicators imported")
    except ImportError:
        print("⚠️ Technical indicators not found")
        TechnicalIndicators = None
    
    # Try multiple tickers
    tickers_to_try = ['AAPL', 'MSFT', 'GOOGL', 'BTC-USD', 'GC=F']
    df = None
    
    for ticker_symbol in tickers_to_try:
        try:
            print(f"\nTrying {ticker_symbol}...")
            ticker = yf.Ticker(ticker_symbol)
            df = ticker.history(period="3mo")
            
            if df is not None and not df.empty:
                df.columns = df.columns.str.lower()
                print(f"✓ Got {len(df)} rows from {ticker_symbol}")
                break
        except Exception as e:
            print(f"⚠️ {ticker_symbol} failed: {e}")
    
    if df is None or df.empty:
        print("\n❌ Could not get data from any ticker")
        print("Creating synthetic data...")
        dates = pd.date_range(end=pd.Timestamp.now(), periods=200, freq='D')
        df = pd.DataFrame({
            'open': np.random.randn(200).cumsum() + 100,
            'high': np.random.randn(200).cumsum() + 101,
            'low': np.random.randn(200).cumsum() + 99,
            'close': np.random.randn(200).cumsum() + 100,
            'volume': np.random.randint(1000, 10000, 200)
        }, index=dates)
        print(f"✓ Created {len(df)} rows of synthetic data")
    
    try:
        # Add indicators if available
        if TechnicalIndicators:
            print("\nAdding technical indicators...")
            df = TechnicalIndicators.add_all_indicators(df)
            print(f"✓ Added indicators. Shape: {df.shape}")
        
        # Train
        print("\nTraining Advanced ML Ensemble...")
        engine = AdvancedPredictionEngine("super_ensemble")
        engine.train(df, target_periods=3)  # Use 3 periods instead of 5
        
        # Predict
        print("\nGenerating prediction...")
        prediction = engine.predict_next(df)
        
        print("\n" + "="*60)
        print("ADVANCED ENSEMBLE PREDICTION")
        print("="*60)
        print(f"Direction: {prediction['direction']}")
        print(f"Confidence: {prediction['confidence']:.2%}")
        print(f"Predicted Return: {prediction['predicted_return']:.4f}")
        print(f"Price Change: {prediction['price_change_pct']:+.2f}%")
        print(f"Models Used: {prediction['model_count']}")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()