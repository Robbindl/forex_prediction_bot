"""
Model Registry - Tracks ML model performance over time
"""

import json
import os
from datetime import datetime
from typing import Dict, List, Optional
import numpy as np
from logger import logger

class ModelRegistry:
    """
    Tracks performance of ML models over time
    Helps identify which models actually predict well
    """
    
    def __init__(self, registry_file: str = "model_registry.json"):
        self.registry_file = registry_file
        self.models: Dict[str, Dict] = {}
        self.load()
    
    def load(self):
        """Load registry from file"""
        if os.path.exists(self.registry_file):
            try:
                with open(self.registry_file, 'r') as f:
                    self.models = json.load(f)
                logger.info(f"📚 Loaded {len(self.models)} models from registry")

            except:
                self.models = {}
    
    def save(self):
        """Save registry to file"""
        try:
            with open(self.registry_file, 'w') as f:
                json.dump(self.models, f, indent=2)
        except Exception as e:
            logger.info(f"⚠️ Could not save model registry: {e}")

    def register_model(self, model_name: str, asset: str, 
                      model_type: str, metadata: Dict = None):
        """Register a new model"""
        key = f"{asset}_{model_name}"
        
        self.models[key] = {
            'model_name': model_name,
            'asset': asset,
            'model_type': model_type,
            'created_at': datetime.now().isoformat(),
            'last_updated': datetime.now().isoformat(),
            'total_predictions': 0,
            'correct_predictions': 0,
            'accuracy': 0.5,
            'avg_confidence': 0.0,
            'trades_followed': 0,
            'trades_won': 0,
            'trade_win_rate': 0.0,
            'metadata': metadata or {}
        }
        
        self.save()
        return key
    
    def update_prediction(self, model_key: str, prediction: Dict, 
                         actual_move: float):
        """
        Update model with prediction result
        
        Args:
            model_key: Key from register_model
            prediction: Prediction dict with 'direction' and 'confidence'
            actual_move: Actual price movement percentage
        """
        if model_key not in self.models:
            return
        
        model = self.models[model_key]
        
        # Update prediction stats
        model['total_predictions'] += 1
        
        # Check if prediction was correct
        predicted_up = prediction.get('direction') == 'UP'
        actual_up = actual_move > 0
        
        if predicted_up == actual_up:
            model['correct_predictions'] += 1
        
        # Update accuracy (exponential moving average)
        correct = 1 if predicted_up == actual_up else 0
        old_acc = model.get('accuracy', 0.5)
        model['accuracy'] = old_acc * 0.95 + correct * 0.05
        
        # Update average confidence
        confidence = prediction.get('confidence', 0.5)
        old_conf = model.get('avg_confidence', 0.5)
        model['avg_confidence'] = old_conf * 0.95 + confidence * 0.05
        
        model['last_updated'] = datetime.now().isoformat()
        self.save()
    
    def update_trade_result(self, model_key: str, trade_result: Dict):
        """
        Update model with actual trade result
        """
        if model_key not in self.models:
            return
        
        model = self.models[model_key]
        
        model['trades_followed'] += 1
        
        if trade_result.get('pnl', 0) > 0:
            model['trades_won'] += 1
        
        if model['trades_followed'] > 0:
            model['trade_win_rate'] = model['trades_won'] / model['trades_followed']
        
        model['last_updated'] = datetime.now().isoformat()
        self.save()
    
    def get_best_models(self, min_predictions: int = 10, top_n: int = 5) -> List[Dict]:
        """Get top performing models by accuracy"""
        candidates = [
            m for m in self.models.values() 
            if m.get('total_predictions', 0) >= min_predictions
        ]
        
        candidates.sort(key=lambda x: x.get('accuracy', 0), reverse=True)
        
        return candidates[:top_n]
    
    def get_model_for_asset(self, asset: str) -> Optional[Dict]:
        """Get best model for specific asset"""
        asset_models = [
            m for m in self.models.values() 
            if m.get('asset') == asset
        ]
        
        if not asset_models:
            return None
        
        # Sort by accuracy, then by trade win rate
        asset_models.sort(
            key=lambda x: (x.get('accuracy', 0), x.get('trade_win_rate', 0)), 
            reverse=True
        )
        
        return asset_models[0]
    
    def get_performance_report(self) -> Dict:
        """Generate performance report"""
        total_models = len(self.models)
        active_models = sum(1 for m in self.models.values() 
                           if m.get('total_predictions', 0) > 10)
        
        avg_accuracy = np.mean([m.get('accuracy', 0.5) 
                                for m in self.models.values()]) if total_models > 0 else 0
        
        return {
            'total_models': total_models,
            'active_models': active_models,
            'avg_accuracy': round(avg_accuracy, 3),
            'best_models': self.get_best_models(),
            'models': self.models
        }