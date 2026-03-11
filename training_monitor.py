"""
📊 TRAINING MONITOR - Check Your AI Training Status
Shows training history, model status, and performance metrics
"""

import json
import os
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List
import pickle
from logger import logger
from advanced_predictor import AdvancedPredictionEngine


class TrainingMonitor:
    """Monitor and display training status"""
    
    def __init__(self, models_dir: str = "trained_models", logs_dir: str = "training_logs"):
        self.models_dir = Path(models_dir)
        self.logs_dir = Path(logs_dir)
        logger.info(f"Training Monitor initialized - Models: {models_dir}, Logs: {logs_dir}")
    
    def get_latest_report(self) -> Dict:
        """Get the latest training report"""
        report_file = self.logs_dir / "latest_training_report.json"
        
        if report_file.exists():
            try:
                with open(report_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Failed to load latest report: {e}")
                return {}
        return {}
    
    def get_all_reports(self) -> List[Dict]:
        """Get all training reports"""
        reports = []
        
        for report_file in sorted(self.logs_dir.glob("report_*.json"), reverse=True):
            try:
                with open(report_file, 'r') as f:
                    report = json.load(f)
                    reports.append(report)
            except Exception as e:
                logger.warning(f"Failed to load report {report_file}: {e}")
                pass
        
        logger.debug(f"Loaded {len(reports)} training reports")
        return reports
    
    def count_trained_models(self) -> int:
        """Count models on disk in both possible directories"""
        count = 0
        
        # Check configured models_dir (trained_models)
        trained_count = 0
        if self.models_dir.exists():
            trained_count = len(list(self.models_dir.glob("*.pkl")))
            count += trained_count
        
        # Check ml_models directory
        ml_count = 0
        ml_models = Path("ml_models")
        if ml_models.exists() and ml_models != self.models_dir:
            ml_count = len(list(ml_models.glob("*.pkl")))
            count += ml_count
        
        logger.info(f"Found models: ml_models/{ml_count}, trained_models/{trained_count}")
        return count
    
    def get_model_ages(self) -> Dict:
        """Get age of each trained model from all directories"""
        ages = {}
        
        # Check both possible directories
        directories = [self.models_dir, Path("ml_models")]
        
        for dir_path in directories:
            if not dir_path.exists():
                continue
                
            for model_file in dir_path.glob("*.pkl"):
                try:
                    with open(model_file, 'rb') as f:
                        # Try to load just the metadata without full unpickling
                        import pickle
                        
                        # Use pickle.load with error handling
                        try:
                            model_data = pickle.load(f)
                            
                            # Try different ways to get trained_at
                            trained_at = None
                            
                            # Method 1: Direct attribute
                            if isinstance(model_data, dict):
                                trained_at = model_data.get('trained_at')
                            elif hasattr(model_data, 'get'):
                                trained_at = model_data.get('trained_at')
                            elif hasattr(model_data, 'trained_at'):
                                trained_at = getattr(model_data, 'trained_at')
                            
                            if trained_at:
                                # Convert string to datetime if needed
                                if isinstance(trained_at, str):
                                    trained_at = datetime.fromisoformat(trained_at)
                                
                                age = datetime.now() - trained_at
                                
                                # Get data points if available
                                data_points = 'N/A'
                                if isinstance(model_data, dict):
                                    data_points = model_data.get('data_points', 'N/A')
                                elif hasattr(model_data, 'data_points'):
                                    data_points = getattr(model_data, 'data_points', 'N/A')
                                
                                ages[model_file.stem] = {
                                    'trained_at': trained_at.isoformat(),
                                    'age_days': age.days,
                                    'age_hours': age.seconds // 3600,
                                    'data_points': data_points,
                                    'confidence': 'N/A',
                                    'location': dir_path.name
                                }
                                logger.debug(f"Successfully loaded {model_file.name}")
                            else:
                                # If no trained_at, use file modification time
                                mod_time = datetime.fromtimestamp(model_file.stat().st_mtime)
                                age = datetime.now() - mod_time
                                ages[model_file.stem] = {
                                    'trained_at': mod_time.isoformat(),
                                    'age_days': age.days,
                                    'age_hours': age.seconds // 3600,
                                    'data_points': 'N/A',
                                    'confidence': 'N/A',
                                    'location': dir_path.name,
                                    'note': 'Using file modification time'
                                }
                                logger.debug(f"Using file time for {model_file.name}")
                                
                        except Exception as e:
                            # If full unpickling fails, try to get just the file time
                            logger.warning(f"Could not unpickle {model_file.name}, using file time: {e}")
                            mod_time = datetime.fromtimestamp(model_file.stat().st_mtime)
                            age = datetime.now() - mod_time
                            ages[model_file.stem] = {
                                'trained_at': mod_time.isoformat(),
                                'age_days': age.days,
                                'age_hours': age.seconds // 3600,
                                'data_points': 'N/A',
                                'confidence': 'N/A',
                                'location': dir_path.name,
                                'note': 'Using file modification time (unpickle failed)'
                            }
                            
                except Exception as e:
                    logger.warning(f"Failed to process model {model_file}: {e}")
                    pass
        
        logger.debug(f"Calculated ages for {len(ages)} models")
        return ages
    
    def analyze_risk_performance(self):
        """Analyze how different risk regimes performed"""
        try:
            import json
            from collections import defaultdict
            
            with open('paper_trades.json', 'r') as f:
                data = json.load(f)
            
            closed = data.get('closed_positions', [])
            
            # Group by market regime
            regime_stats = defaultdict(lambda: {'trades': 0, 'wins': 0, 'pnl': 0})
            
            for trade in closed:
                if 'risk_info' in trade:
                    regime = trade['risk_info'].get('market_regime', 'unknown')
                    sentiment = trade['risk_info'].get('sentiment_regime', 'unknown')
                    
                    key = f"{regime}_{sentiment}"
                    regime_stats[key]['trades'] += 1
                    if trade.get('pnl', 0) > 0:
                        regime_stats[key]['wins'] += 1
                    regime_stats[key]['pnl'] += trade.get('pnl', 0)
            
            logger.info("\nRISK REGIME PERFORMANCE")
            logger.info("="*60)
            for regime, stats in regime_stats.items():
                win_rate = (stats['wins'] / stats['trades'] * 100) if stats['trades'] > 0 else 0
                logger.info(f"{regime:30} | Trades: {stats['trades']:3} | Win Rate: {win_rate:5.1f}% | P&L: ${stats['pnl']:8.2f}")
                
        except Exception as e:
            logger.error(f"Error analyzing risk performance: {e}")
    
    def print_dashboard(self):
        """Print comprehensive training dashboard"""
        
        # Log to file
        logger.info("="*80)
        logger.info("AI TRAINING MONITOR DASHBOARD")
        logger.info("="*80)
        
        # Print to console (keep for user) - using ASCII only
        logger.info("\n" + "="*80)

        logger.info("AI TRAINING MONITOR DASHBOARD")

        logger.info("="*80)

        # Get model ages first
        ages = self.get_model_ages()
        
        # MODEL TRAINING STATUS (based on actual model files)
        logger.info("\nMODEL TRAINING STATUS:")

        logger.info("-" * 80)

        logger.info("MODEL TRAINING STATUS:")
        
        if ages:
            # Find newest model
            newest_model = None
            newest_date = None
            
            for name, data in ages.items():
                trained_at = datetime.fromisoformat(data['trained_at'])
                if newest_date is None or trained_at > newest_date:
                    newest_date = trained_at
                    newest_model = name
            
            if newest_date:
                time_ago = datetime.now() - newest_date
                logger.info(f"Last Model Trained: {newest_date.strftime('%Y-%m-%d %H:%M:%S')}")

                logger.info(f"Time Ago: {time_ago.days} days, {time_ago.seconds//3600} hours ago")

                logger.info(f"Newest Model: {newest_model}")

                logger.info(f"Last Model Trained: {newest_date.strftime('%Y-%m-%d %H:%M:%S')}")
                logger.info(f"Time Ago: {time_ago.days} days, {time_ago.seconds//3600} hours ago")
        else:
            logger.info("No trained models found")

            logger.warning("No trained models found")
        
        # Model inventory
        logger.info("\nMODEL INVENTORY:")

        logger.info("-" * 80)

        logger.info("MODEL INVENTORY:")
        
        model_count = self.count_trained_models()
        logger.info(f"Total Models: {model_count}")

        logger.info(f"Total Models: {model_count}")
        
        if model_count > 0 and ages:
            # Count by location
            ml_count = sum(1 for data in ages.values() if data.get('location') == 'ml_models')
            trained_count = sum(1 for data in ages.values() if data.get('location') == 'trained_models')
            
            logger.info(f"  Location: ml_models/: {ml_count} models, trained_models/: {trained_count} models")

            # Group by age
            fresh = sum(1 for data in ages.values() if data['age_days'] == 0)
            recent = sum(1 for data in ages.values() if 0 < data['age_days'] <= 7)
            old = sum(1 for data in ages.values() if data['age_days'] > 7)
            
            logger.info(f"  Fresh (today): {fresh}")

            logger.info(f"  Recent (1-7 days): {recent}")

            logger.info(f"  Old (>7 days): {old}")

            logger.info(f"Fresh (today): {fresh}")
            logger.info(f"Recent (1-7 days): {recent}")
            logger.info(f"Old (>7 days): {old}")
            
            if old > 0:
                logger.info(f"\n  ⚠️  {old} models need retraining!")

                logger.warning(f"{old} models need retraining!")
        
        # Training history (from logs - optional, can be removed if not needed)
        logger.info("\nTRAINING HISTORY (Last 7 Days):")

        logger.info("-" * 80)

        logger.info("TRAINING HISTORY (Last 7 Days):")
        
        reports = self.get_all_reports()[:7]  # Last 7
        
        if reports:
            logger.info(f"{'Date':<20} {'Success':<10} {'Failed':<10} {'Rate':<10} {'Time':<10}")

            logger.info("-" * 80)

            for report in reports:
                date = datetime.fromisoformat(report['date']).strftime('%Y-%m-%d %H:%M')
                success = report['successfully_trained']
                failed = report['failed']
                rate = f"{report['success_rate']:.1f}%"
                time_min = f"{report['total_time_minutes']:.1f}m"
                
                logger.info(f"{date:<20} {success:<10} {failed:<10} {rate:<10} {time_min:<10}")

                # Log to file
                logger.info(f"{date} | Success: {success} | Failed: {failed} | Rate: {rate} | Time: {time_min}")
        else:
            logger.info("No training history available")

            logger.info("No training history available")
        
        # Model health check
        logger.info("\nMODEL HEALTH CHECK:")

        logger.info("-" * 80)

        logger.info("MODEL HEALTH CHECK:")
        
        if ages:
            # Find models needing attention
            needs_retrain = [name for name, data in ages.items() if data['age_days'] > 7]
            low_confidence = [name for name, data in ages.items() if isinstance(data.get('confidence'), (int, float)) and data['confidence'] < 0.5]
            
            if needs_retrain:
                logger.info(f"  ⚠️  {len(needs_retrain)} models need retraining (>7 days old)")

                logger.warning(f"{len(needs_retrain)} models need retraining (>7 days old)")
                for name in needs_retrain[:5]:
                    age = ages[name]['age_days']
                    location = ages[name].get('location', 'unknown')
                    logger.info(f"     - {name}: {age} days old ({location})")

                    logger.warning(f"     - {name}: {age} days old ({location})")
                if len(needs_retrain) > 5:
                    logger.info(f"     ... and {len(needs_retrain)-5} more")

                    logger.warning(f"     ... and {len(needs_retrain)-5} more")
            
            if low_confidence:
                logger.info(f"  ⚠️  {len(low_confidence)} models have low confidence (<50%)")

                logger.warning(f"{len(low_confidence)} models have low confidence (<50%)")
                for name in low_confidence[:3]:
                    conf = ages[name]['confidence']
                    location = ages[name].get('location', 'unknown')
                    logger.info(f"     - {name}: {conf:.0%} confidence ({location})")

                    logger.warning(f"     - {name}: {conf:.0%} confidence ({location})")
            
            if not needs_retrain and not low_confidence:
                logger.info("  ✅ All models are healthy!")

                logger.info("All models are healthy!")
        else:
            logger.info("  No model data available")

        # Recommendations
        logger.info("\nRECOMMENDATIONS:")

        logger.info("-" * 80)

        logger.info("RECOMMENDATIONS:")
        
        if ages:
            # Check if any models are old
            old_models = [name for name, data in ages.items() if data['age_days'] > 1]
            if old_models:
                logger.info("  ⚠️  Some models are more than 1 day old - consider retraining")

                logger.info("     Run: python auto_train_daily.py")

                logger.warning("Some models are more than 1 day old - consider retraining")
            else:
                logger.info("  ✅ All models are fresh!")

                logger.info("     Next training: Scheduled for tonight (if auto-training is setup)")

                logger.info("All models are fresh!")
        else:
            logger.info("  ⚠️  No models found - run initial training")

            logger.info("     Run: python auto_train_daily.py")

            logger.warning("No models found - run initial training")
        
        # Quick actions
        logger.info("\nQUICK ACTIONS:")

        logger.info("-" * 80)

        logger.info("Train now:        python auto_train_daily.py")

        logger.info("Setup auto:       Run setup_auto_training.ps1 (as Admin)")

        logger.info("View this again:  python training_monitor.py")

        logger.info("Check logs:       training_logs/")

        logger.info("\n" + "="*80 + "\n")

        logger.info("="*80)
    
    def export_status_json(self) -> str:
        """Export status as JSON for web dashboard"""
        status = {
            'latest_training': self.get_latest_report(),
            'total_models': self.count_trained_models(),
            'model_ages': self.get_model_ages(),
            'training_history': self.get_all_reports()[:30]
        }
        
        logger.info(f"Exporting status JSON with {len(status['training_history'])} history entries")
        return json.dumps(status, indent=2, default=str)


def main():
    """Main monitor function"""
    monitor = TrainingMonitor()
    monitor.print_dashboard()
    
    # Ask if user wants JSON export
    logger.info("Export status as JSON? (Y/N): ", end="")

    try:
        response = input().strip().upper()
        if response == 'Y':
            json_data = monitor.export_status_json()
            
            with open('training_status.json', 'w') as f:
                f.write(json_data)
            
            logger.info("Exported to: training_status.json")

            logger.info("Status exported to training_status.json")
    except Exception as e:
        logger.error(f"Export failed: {e}")
        pass


if __name__ == "__main__":
    main()