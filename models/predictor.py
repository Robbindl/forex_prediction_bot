"""
Machine Learning Prediction Engine
Supports: Random Forest, XGBoost, LSTM, Ensemble
"""

from utils.logger import logger
import pandas as pd
import numpy as np
from typing import Tuple, Dict, Any, List
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
import warnings
warnings.filterwarnings('ignore')

try:
    import xgboost as xgb
    XGBOOST_AVAILABLE = True
except ImportError:
    XGBOOST_AVAILABLE = False
    logger.info("XGBoost not available. Install with: pip install xgboost")

try:
    from tensorflow import keras
    from tensorflow.keras.models import Sequential
    from tensorflow.keras.layers import LSTM, Dense, Dropout
    TENSORFLOW_AVAILABLE = True
except ImportError:
    TENSORFLOW_AVAILABLE = False
    logger.info("TensorFlow not available. Install with: pip install tensorflow")


class PredictionEngine:
    """ML-based prediction engine for financial markets"""
    
    def __init__(self, model_type: str = "ensemble"):
        """
        Initialize prediction engine
        
        Args:
            model_type: 'rf', 'xgboost', 'lstm', 'ensemble'
        """
        self.model_type = model_type
        self.model = None
        self.scaler = StandardScaler()
        self.feature_names: List[str] = []
        self.trained = False
        
    def engineer_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create advanced features from technical indicators"""
        df = df.copy()
        
        # Price changes
        df['price_change'] = df['close'].pct_change()
        df['price_change_5'] = df['close'].pct_change(periods=5)
        df['price_change_10'] = df['close'].pct_change(periods=10)
        
        # Volatility features
        df['volatility_5'] = df['close'].rolling(5).std()
        df['volatility_10'] = df['close'].rolling(10).std()
        df['volatility_20'] = df['close'].rolling(20).std()
        
        # Momentum features
        df['momentum_5'] = df['close'] - df['close'].shift(5)
        df['momentum_10'] = df['close'] - df['close'].shift(10)
        
        # Volume features (if available)
        if 'volume' in df.columns and df['volume'].sum() > 0:
            df['volume_change'] = df['volume'].pct_change()
            df['volume_ma_5'] = df['volume'].rolling(5).mean()
            df['volume_ratio'] = df['volume'] / df['volume_ma_5']
        
        # RSI momentum
        if 'rsi' in df.columns:
            df['rsi_change'] = df['rsi'].diff()
            df['rsi_ma_5'] = df['rsi'].rolling(5).mean()
        
        # MACD features
        if 'macd' in df.columns:
            df['macd_change'] = df['macd'].diff()
        
        # Bollinger Band position
        if 'bb_position' in df.columns:
            df['bb_squeeze'] = df['bb_width'] / df['bb_width'].rolling(20).mean()
        
        # ADX trend strength change
        if 'adx' in df.columns:
            df['adx_change'] = df['adx'].diff()
        
        # Price position relative to moving averages
        if 'sma_20' in df.columns:
            df['price_to_sma20'] = (df['close'] - df['sma_20']) / df['sma_20']
        if 'sma_50' in df.columns:
            df['price_to_sma50'] = (df['close'] - df['sma_50']) / df['sma_50']
        
        # Trend features
        df['trend_5'] = np.where(df['close'] > df['close'].shift(5), 1, -1)
        df['trend_10'] = np.where(df['close'] > df['close'].shift(10), 1, -1)
        
        # Day of week / hour features (if intraday)
        if df.index.dtype == 'datetime64[ns]':
            df['day_of_week'] = df.index.dayofweek
            df['hour'] = df.index.hour
        
        return df
    
    def prepare_data(self, df: pd.DataFrame, target_periods: int = 5) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
        """
        Prepare data for training/prediction
        
        Args:
            df: DataFrame with features
            target_periods: How many periods ahead to predict
            
        Returns:
            Tuple of (X_train, X_test, y_train, y_test)
        """
        df = df.copy()
        
        # Create target: future price change
        df['target'] = df['close'].shift(-target_periods)
        df['target_direction'] = np.where(
            df['target'] > df['close'], 1, 0
        )
        
        # Drop NaN rows
        df = df.dropna()
        
        # Select features (exclude target and raw OHLCV)
        exclude_cols = ['open', 'high', 'low', 'close', 'volume', 
                       'target', 'target_direction']
        
        feature_cols = [col for col in df.columns 
                       if col not in exclude_cols 
                       and not col.startswith('fib_')
                       and not col.startswith('pivot')
                       and not col.startswith('senkou')
                       and not col.startswith('chikou')]
        
        X = df[feature_cols].values
        y = df['target'].values
        
        self.feature_names = feature_cols
        
        # Train-test split
        split_idx = int(len(X) * 0.8)
        X_train = X[:split_idx]
        X_test = X[split_idx:]
        y_train = y[:split_idx]
        y_test = y[split_idx:]
        
        # Scale features
        X_train = self.scaler.fit_transform(X_train)
        X_test = self.scaler.transform(X_test)
        
        return X_train, X_test, y_train, y_test
    
    def train_random_forest(self, X_train: np.ndarray, y_train: np.ndarray) -> None:
        """Train Random Forest model"""
        logger.info("Training Random Forest...")
        self.model = RandomForestRegressor(
            n_estimators=100,
            max_depth=10,
            min_samples_split=5,
            min_samples_leaf=2,
            random_state=42,
            n_jobs=-1
        )
        self.model.fit(X_train, y_train)
        self.trained = True
        
    def train_xgboost(self, X_train: np.ndarray, y_train: np.ndarray) -> None:
        """Train XGBoost model"""
        if not XGBOOST_AVAILABLE:
            logger.info("XGBoost not available, falling back to Random Forest")
            return self.train_random_forest(X_train, y_train)
            
        logger.info("Training XGBoost...")
        self.model = xgb.XGBRegressor(
            n_estimators=100,
            max_depth=7,
            learning_rate=0.1,
            subsample=0.8,
            colsample_bytree=0.8,
            random_state=42,
            n_jobs=-1
        )
        self.model.fit(X_train, y_train)
        self.trained = True
    
    def train_lstm(self, X_train: np.ndarray, y_train: np.ndarray, X_test: np.ndarray, y_test: np.ndarray) -> None:
        """Train LSTM model"""
        if not TENSORFLOW_AVAILABLE:
            logger.info("TensorFlow not available, falling back to Random Forest")
            return self.train_random_forest(X_train, y_train)
        
        logger.info("Training LSTM...")
        
        # Reshape for LSTM [samples, time steps, features]
        X_train_lstm = X_train.reshape((X_train.shape[0], 1, X_train.shape[1]))
        X_test_lstm = X_test.reshape((X_test.shape[0], 1, X_test.shape[1]))
        
        self.model = Sequential([
            LSTM(50, activation='relu', return_sequences=True, 
                 input_shape=(1, X_train.shape[1])),
            Dropout(0.2),
            LSTM(50, activation='relu'),
            Dropout(0.2),
            Dense(25, activation='relu'),
            Dense(1)
        ])
        
        self.model.compile(optimizer='adam', loss='mse', metrics=['mae'])
        
        self.model.fit(
            X_train_lstm, y_train,
            validation_data=(X_test_lstm, y_test),
            epochs=50,
            batch_size=32,
            verbose=0
        )
        self.trained = True
        
    def train_ensemble(self, X_train: np.ndarray, y_train: np.ndarray) -> None:
        """Train ensemble of models"""
        logger.info("Training Ensemble (RF + GradientBoosting)...")
        
        rf = RandomForestRegressor(
            n_estimators=100,
            max_depth=10,
            random_state=42,
            n_jobs=-1
        )
        
        gb = GradientBoostingRegressor(
            n_estimators=100,
            max_depth=5,
            learning_rate=0.1,
            random_state=42
        )
        
        rf.fit(X_train, y_train)
        gb.fit(X_train, y_train)
        
        self.model = [rf, gb]
        self.trained = True
    
    def train(self, df: pd.DataFrame, target_periods: int = 5) -> None:
        """
        Train the prediction model
        
        Args:
            df: DataFrame with technical indicators
            target_periods: Periods ahead to predict
        """
        # Engineer features
        df = self.engineer_features(df)
        
        # Prepare data
        X_train, X_test, y_train, y_test = self.prepare_data(df, target_periods)
        
        # Train based on model type
        if self.model_type == "rf":
            self.train_random_forest(X_train, y_train)
        elif self.model_type == "xgboost":
            self.train_xgboost(X_train, y_train)
        elif self.model_type == "lstm":
            self.train_lstm(X_train, y_train, X_test, y_test)
        elif self.model_type == "ensemble":
            self.train_ensemble(X_train, y_train)
        else:
            logger.info(f"Unknown model type: {self.model_type}, using Random Forest")
            self.train_random_forest(X_train, y_train)
        
        # Evaluate
        self.evaluate(X_test, y_test)
        
    def evaluate(self, X_test: np.ndarray, y_test: np.ndarray) -> None:
        """Evaluate model performance"""
        predictions = self.predict_batch(X_test)
        
        mse = mean_squared_error(y_test, predictions)
        mae = mean_absolute_error(y_test, predictions)
        r2 = r2_score(y_test, predictions)
        
        logger.info(f"\nModel Performance:")
        logger.info(f"  MSE: {mse:.6f}")
        logger.info(f"  MAE: {mae:.6f}")
        logger.info(f"  R²: {r2:.4f}")
        
    def predict_batch(self, X: np.ndarray) -> np.ndarray:
        """Make predictions on batch of data"""
        if not self.trained:
            raise Exception("Model not trained yet!")
        
        if self.model_type == "ensemble":
            # Average predictions from ensemble
            predictions = []
            for model in self.model:
                predictions.append(model.predict(X))
            return np.mean(predictions, axis=0)
        elif self.model_type == "lstm":
            X_lstm = X.reshape((X.shape[0], 1, X.shape[1]))
            return self.model.predict(X_lstm).flatten()
        else:
            return self.model.predict(X)
    
    def predict_next(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Predict next price movement
        
        Args:
            df: DataFrame with latest data and indicators
            
        Returns:
            Dict with prediction, direction, confidence
        """
        if not self.trained:
            raise Exception("Model not trained yet!")
        
        # Engineer features
        df = self.engineer_features(df)
        
        # Get latest features
        latest = df[self.feature_names].iloc[-1:].values
        latest_scaled = self.scaler.transform(latest)
        
        # Make prediction
        if self.model_type == "lstm":
            latest_scaled = latest_scaled.reshape((1, 1, latest_scaled.shape[1]))
        
        prediction = self.predict_batch(latest_scaled)[0]
        
        current_price = df['close'].iloc[-1]
        predicted_price = prediction
        price_change = predicted_price - current_price
        price_change_pct = (price_change / current_price) * 100
        
        direction = "UP" if predicted_price > current_price else "DOWN"
        
        # Confidence based on prediction magnitude
        confidence = min(abs(price_change_pct) / 2, 1.0)
        
        return {
            "current_price": current_price,
            "predicted_price": predicted_price,
            "price_change": price_change,
            "price_change_pct": price_change_pct,
            "direction": direction,
            "confidence": confidence
        }
    
    def get_feature_importance(self) -> pd.DataFrame:
        """Get feature importance (for tree-based models)"""
        if not self.trained:
            return pd.DataFrame()
        
        if self.model_type in ["rf", "xgboost"]:
            if self.model_type == "xgboost" and XGBOOST_AVAILABLE:
                importance = self.model.feature_importances_
            else:
                importance = self.model.feature_importances_
                
            df_importance = pd.DataFrame({
                'feature': self.feature_names,
                'importance': importance
            }).sort_values('importance', ascending=False)
            
            return df_importance
        elif self.model_type == "ensemble":
            # Average importance across ensemble
            importances = []
            for model in self.model:
                importances.append(model.feature_importances_)
            
            avg_importance = np.mean(importances, axis=0)
            df_importance = pd.DataFrame({
                'feature': self.feature_names,
                'importance': avg_importance
            }).sort_values('importance', ascending=False)
            
            return df_importance
        
        return pd.DataFrame()


if __name__ == "__main__":
    # Test prediction engine
    import yfinance as yf
    import sys
    sys.path.append('..')
    from indicators.technical import TechnicalIndicators
    
    logger.info("Fetching data...")
    ticker = yf.Ticker("EURUSD=X")
    df = ticker.history(period="100d")
    df.columns = df.columns.str.lower()
    
    logger.info("Adding indicators...")
    df = TechnicalIndicators.add_all_indicators(df)
    
    logger.info("\nTraining model...")
    engine = PredictionEngine(model_type="ensemble")
    engine.train(df, target_periods=5)
    
    logger.info("\nMaking prediction...")
    prediction = engine.predict_next(df)
    logger.info(f"Direction: {prediction['direction']}")
    logger.info(f"Confidence: {prediction['confidence']:.2%}")
    logger.info(f"Predicted change: {prediction['price_change_pct']:.2f}%")