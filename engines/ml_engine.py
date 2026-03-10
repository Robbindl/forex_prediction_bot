"""
MLEngine — machine learning model management extracted from UltimateTradingSystem.
Receives a system reference at init for predictor, model_registry, fetcher access.
"""

import time
import numpy as np
import pandas as pd
from typing import Dict, List, Optional
from datetime import datetime
from logger import logger


class MLEngine:
    """Manages ML model training, prediction, tracking and registry."""

    def __init__(self, system):
        self.system = system
        self.predictor = system.predictor
        self.model_registry = getattr(system, 'model_registry', None)
        self._pending_predictions: List[Dict] = []
        self._trade_history: List[Dict] = []
        logger.info("MLEngine initialised")

    # ── delegates ────────────────────────────────────────────────────────────
    def fetch_historical_data(self, *a, **kw):
        return self.system.fetch_historical_data(*a, **kw)

    @property
    def current_asset(self):
        return getattr(self.system, 'current_asset', 'UNKNOWN')

    def update_model_prediction(self, asset, prediction, actual_move):
        """Forward to self for pending prediction verification."""
        self._do_update_model_prediction(asset, prediction, actual_move)

def ml_ensemble_strategy(self, df: pd.DataFrame) -> List[Dict]:
    """Machine Learning Ensemble Strategy"""
    signals = []
    try:
        # FIX: skip if models not trained
        if not self.predictor.models:
            return signals
        prediction = self.predictor.predict_next(df)

        if hasattr(self, 'model_registry') and self.model_registry and 'asset' in self.__dict__:
            # Store prediction for later verification
            if not hasattr(self, '_pending_predictions'):
                self._pending_predictions = []
            
            self._pending_predictions.append({
                'asset': self.current_asset,
                'timestamp': datetime.now(),
                'prediction': prediction,
                'price': df['close'].iloc[-1]
            })
        # ========================================
        
        if prediction['confidence'] > 0.7:
            current_price = df['close'].iloc[-1]
            
            if prediction['direction'] == 'UP':
                signals.append({
                    'date': df.index[-1],
                    'signal': 'BUY',
                    'confidence': prediction['confidence'],
                    'entry': current_price,
                    'stop_loss': current_price * 0.97,
                    'take_profit': current_price * 1.08,
                    'strategy': 'ml_ensemble',
                    'ml_details': prediction
                })
            elif prediction['direction'] == 'DOWN':
                signals.append({
                    'date': df.index[-1],
                    'signal': 'SELL',
                    'confidence': prediction['confidence'],
                    'entry': current_price,
                    'stop_loss': current_price * 1.03,
                    'take_profit': current_price * 0.92,
                    'strategy': 'ml_ensemble',
                    'ml_details': prediction
                })
    except Exception as e:
        logger.debug(f"ML prediction error for {self.current_asset}: {e}")
    
    return signals

def verify_pending_predictions(self):
    """Check pending predictions against actual price movements"""
    if not hasattr(self, '_pending_predictions') or not self._pending_predictions:
        return
    
    to_remove = []
    
    for pred in self._pending_predictions:
        try:
            # Get current price for the asset
            asset = pred['asset']
            price, _ = self.fetcher.get_real_time_price(asset, 'unknown')
            
            if price:
                # Calculate actual movement
                actual_move = (price - pred['price']) / pred['price'] * 100
                
                # Update registry
                self.update_model_prediction(asset, pred['prediction'], actual_move)
                to_remove.append(pred)
        except Exception as e:
            logger.warning(f"Failed to verify prediction for {asset}: {e}")
    
    # Remove verified predictions
    for pred in to_remove:
        self._pending_predictions.remove(pred)

# ============= COMPREHENSIVE BACKTESTING =============

def show_model_performance(self):
    """Display model performance report"""
    report = self.get_model_performance_report()
    
    if 'error' in report:
        logger.error(f"Model performance report error: {report['error']}")
        return
    
    logger.info("="*70)
    logger.info("MODEL PERFORMANCE REPORT")
    logger.info("="*70)
    logger.info(f"Total Models: {report['total_models']}")
    logger.info(f"Active Models: {report['active_models']}")
    logger.info(f"Average Accuracy: {report['avg_accuracy']:.1%}")
    
    if report['best_models']:
        logger.info("TOP PERFORMING MODELS:")
        for i, model in enumerate(report['best_models'], 1):
            logger.info(f"  {i}. {model['asset']} - {model['model_name']}")
            logger.info(f"     Accuracy: {model['accuracy']:.1%}")
            logger.info(f"     Trade Win Rate: {model['trade_win_rate']:.1%}")
            logger.info(f"     Predictions: {model['total_predictions']}")

def train_ml_models(self, assets: List[str]):
    """Train ML models on multiple assets"""
    logger.info("="*60)
    logger.info(" TRAINING ML ENSEMBLE MODELS")
    logger.info("="*60)

    for asset in assets:
        logger.info(f"Training on {asset}...")
        
        # ===== TRY MULTIPLE PERIODS, TAKE THE BEST =====
        best_df = None
        best_rows = 0
        
        for days in [730, 365, 180, 90]:
            df = self.fetch_historical_data(asset, days)
            if not df.empty and len(df) > best_rows:
                best_rows = len(df)
                best_df = df
                logger.debug(f"Found {len(df)} rows with {days} days")
        
        if best_df is None:
            logger.warning(f"No data for {asset}")
            continue
        
        df = best_df
        logger.info(f"Using {len(df)} rows for training")
        
        # Add indicators
        df = self.add_technical_indicators(df)
        
        try:
            # Train model
            self.predictor.train(df, target_periods=5)
            
            # Save model
            import pickle
            safe_asset = asset.replace('/', '_').replace('\\', '_').replace(':', '_')
            model_path = f"ml_models/{safe_asset}_model.pkl"
            
            os.makedirs("ml_models", exist_ok=True)
            
            with open(model_path, 'wb') as f:
                pickle.dump(self.predictor, f)
            
            logger.info(f"Model saved to {model_path}")

             # ===== REGISTER MODEL IN REGISTRY =====
            if hasattr(self, 'model_registry') and self.model_registry:
                metadata = {
                    'data_points': len(df),
                    'features': len(self.predictor.feature_names) if hasattr(self.predictor, 'feature_names') else 0,
                    'model_path': model_path
                }
                self.register_ml_model(asset, "ensemble", metadata)
            # ======================================
            
            # Get feature importance
            importance = self.predictor.get_feature_importance(10)
            if not importance.empty:
                logger.debug("Top Features:")
                for idx, row in importance.iterrows():
                    logger.debug(f"  {row['feature']}: {row['importance']:.4f}")
            
        except Exception as e:
            logger.error(f"Training error for {asset}: {e}")

# ============= BROKER INTEGRATION (READY FOR ALPACA) =============

def register_ml_model(self, asset: str, model_name: str = "ensemble", metadata: Dict = None):
    """Register an ML model in the registry"""
    if not hasattr(self, 'model_registry') or self.model_registry is None:
        return None
    
    try:
        key = self.model_registry.register_model(
            model_name=model_name,
            asset=asset,
            model_type="advanced_ensemble",
            metadata=metadata
        )
        logger.info(f"Registered model for {asset}: {key}")
        return key
    except Exception as e:
        logger.warning(f"Failed to register model for {asset}: {e}")
        return None

def _remember_trade(self, trade_result: Dict):
    """Store trade in personality memory"""
    if hasattr(self, 'personality'):
        self.personality.remember_trade({
            'asset': trade_result.get('asset'),
            'pnl': trade_result.get('pnl', 0),
            'exit_reason': trade_result.get('exit_reason'),
            'setup': trade_result.get('setup', 'unknown')
        })

def _do_update_model_prediction(self, asset: str, prediction: Dict, actual_move: float):
    """Update model performance with prediction result"""
    if not hasattr(self, 'model_registry') or self.model_registry is None:
        return
    
    try:
        # Get the model key for this asset
        model_key = f"{asset}_ensemble"
        self.model_registry.update_prediction(model_key, prediction, actual_move)
        logger.debug(f"Updated prediction for {asset}: actual={actual_move:.2f}%")
    except Exception as e:
        logger.warning(f"Failed to update model prediction for {asset}: {e}")

def update_model_trade_result(self, asset: str, trade_result: Dict):
    """Update model performance with trade result"""
    if not hasattr(self, 'model_registry') or self.model_registry is None:
        return
    
    try:
        model_key = f"{asset}_ensemble"
        self.model_registry.update_trade_result(model_key, trade_result)
        logger.debug(f"Updated trade result for {asset}: P&L=${trade_result.get('pnl',0):.2f}")
    except Exception as e:
        logger.warning(f"Failed to update model trade result for {asset}: {e}")

def get_best_model_for_asset(self, asset: str) -> Optional[Dict]:
    """Get the best performing model for an asset"""
    if not hasattr(self, 'model_registry') or self.model_registry is None:
        return None
    
    try:
        return self.model_registry.get_model_for_asset(asset)
    except Exception as e:
        logger.warning(f"Failed to get best model for {asset}: {e}")
        return None

def get_model_performance_report(self) -> Dict:
    """Get comprehensive model performance report"""
    if not hasattr(self, 'model_registry') or self.model_registry is None:
        return {'error': 'Model registry not available'}
    
    try:
        return self.model_registry.get_performance_report()
    except Exception as e:
        logger.warning(f"Failed to get performance report: {e}")
        return {'error': str(e)}