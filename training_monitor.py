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


class TrainingMonitor:
    """Monitor and display training status"""
    
    def __init__(self, models_dir: str = "trained_models", logs_dir: str = "training_logs"):
        self.models_dir = Path(models_dir)
        self.logs_dir = Path(logs_dir)
    
    def get_latest_report(self) -> Dict:
        """Get the latest training report"""
        report_file = self.logs_dir / "latest_training_report.json"
        
        if report_file.exists():
            with open(report_file, 'r') as f:
                return json.load(f)
        return {}
    
    def get_all_reports(self) -> List[Dict]:
        """Get all training reports"""
        reports = []
        
        for report_file in sorted(self.logs_dir.glob("report_*.json"), reverse=True):
            try:
                with open(report_file, 'r') as f:
                    report = json.load(f)
                    reports.append(report)
            except:
                pass
        
        return reports
    
    def count_trained_models(self) -> int:
        """Count models on disk"""
        return len(list(self.models_dir.glob("*.pkl")))
    
    def get_model_ages(self) -> Dict:
        """Get age of each trained model"""
        ages = {}
        
        for model_file in self.models_dir.glob("*.pkl"):
            try:
                with open(model_file, 'rb') as f:
                    model_data = pickle.load(f)
                    trained_at = model_data.get('trained_at')
                    
                    if trained_at:
                        age = datetime.now() - trained_at
                        ages[model_file.stem] = {
                            'trained_at': trained_at.isoformat(),
                            'age_days': age.days,
                            'age_hours': age.seconds // 3600,
                            'data_points': model_data.get('data_points', 'N/A'),
                            'confidence': model_data.get('test_confidence', 'N/A')
                        }
            except:
                pass
        
        return ages
    
    def print_dashboard(self):
        """Print comprehensive training dashboard"""
        
        print("\n" + "="*80)
        print("📊 AI TRAINING MONITOR DASHBOARD")
        print("="*80)
        
        # Latest training session
        latest = self.get_latest_report()
        
        if latest:
            print("\n🔄 LAST TRAINING SESSION:")
            print("-" * 80)
            
            trained_date = datetime.fromisoformat(latest['date'])
            time_ago = datetime.now() - trained_date
            
            print(f"📅 Date: {trained_date.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"⏰ Time Ago: {time_ago.days} days, {time_ago.seconds//3600} hours ago")
            print(f"✅ Successfully Trained: {latest['successfully_trained']}")
            print(f"❌ Failed: {latest['failed']}")
            print(f"📈 Success Rate: {latest['success_rate']:.1f}%")
            print(f"⏱️  Training Time: {latest['total_time_minutes']:.1f} minutes")
            print(f"🧠 Model Type: {latest['model_type'].title()}")
            print(f"📊 Timeframes: {', '.join(latest['timeframes'])}")
        else:
            print("\n⚠️  No training reports found")
            print("Run: python auto_train_daily.py")
        
        # Model inventory
        print("\n💾 MODEL INVENTORY:")
        print("-" * 80)
        
        model_count = self.count_trained_models()
        print(f"📦 Total Models: {model_count}")
        
        if model_count > 0:
            ages = self.get_model_ages()
            
            # Group by age
            fresh = sum(1 for age in ages.values() if age['age_days'] == 0)
            recent = sum(1 for age in ages.values() if 0 < age['age_days'] <= 7)
            old = sum(1 for age in ages.values() if age['age_days'] > 7)
            
            print(f"  🟢 Fresh (today): {fresh}")
            print(f"  🟡 Recent (1-7 days): {recent}")
            print(f"  🔴 Old (>7 days): {old}")
            
            if old > 0:
                print(f"\n  ⚠️  {old} models need retraining!")
        
        # Training history
        print("\n📈 TRAINING HISTORY (Last 7 Days):")
        print("-" * 80)
        
        reports = self.get_all_reports()[:7]  # Last 7
        
        if reports:
            print(f"{'Date':<20} {'Success':<10} {'Failed':<10} {'Rate':<10} {'Time':<10}")
            print("-" * 80)
            
            for report in reports:
                date = datetime.fromisoformat(report['date']).strftime('%Y-%m-%d %H:%M')
                success = report['successfully_trained']
                failed = report['failed']
                rate = f"{report['success_rate']:.1f}%"
                time_min = f"{report['total_time_minutes']:.1f}m"
                
                print(f"{date:<20} {success:<10} {failed:<10} {rate:<10} {time_min:<10}")
        else:
            print("No training history available")
        
        # Model health check
        print("\n🏥 MODEL HEALTH CHECK:")
        print("-" * 80)
        
        ages = self.get_model_ages()
        
        if ages:
            # Find models needing attention
            needs_retrain = [name for name, data in ages.items() if data['age_days'] > 7]
            low_confidence = [name for name, data in ages.items() if isinstance(data['confidence'], float) and data['confidence'] < 0.5]
            
            if needs_retrain:
                print(f"⚠️  {len(needs_retrain)} models need retraining (>7 days old)")
                for name in needs_retrain[:5]:
                    age = ages[name]['age_days']
                    print(f"   - {name}: {age} days old")
                if len(needs_retrain) > 5:
                    print(f"   ... and {len(needs_retrain)-5} more")
            
            if low_confidence:
                print(f"⚠️  {len(low_confidence)} models have low confidence (<50%)")
                for name in low_confidence[:3]:
                    conf = ages[name]['confidence']
                    print(f"   - {name}: {conf:.0%} confidence")
            
            if not needs_retrain and not low_confidence:
                print("✅ All models are healthy!")
        
        # Recommendations
        print("\n💡 RECOMMENDATIONS:")
        print("-" * 80)
        
        if latest:
            hours_since = (datetime.now() - datetime.fromisoformat(latest['date'])).seconds // 3600
            days_since = (datetime.now() - datetime.fromisoformat(latest['date'])).days
            
            if days_since > 1:
                print("⚠️  Models are more than 1 day old - consider retraining")
                print("   Run: python auto_train_daily.py")
            elif latest['success_rate'] < 80:
                print("⚠️  Last training had low success rate")
                print("   Check logs in: training_logs/")
            else:
                print("✅ Everything looks good!")
                print(f"   Next training: Scheduled for tonight (if auto-training is setup)")
        else:
            print("⚠️  No training found - run initial training")
            print("   Run: python auto_train_daily.py")
        
        # Quick actions
        print("\n🚀 QUICK ACTIONS:")
        print("-" * 80)
        print("Train now:        python auto_train_daily.py")
        print("Setup auto:       Run setup_auto_training.ps1 (as Admin)")
        print("View this again:  python training_monitor.py")
        print("Check logs:       training_logs/")
        
        print("\n" + "="*80 + "\n")
    
    def export_status_json(self) -> str:
        """Export status as JSON for web dashboard"""
        status = {
            'latest_training': self.get_latest_report(),
            'total_models': self.count_trained_models(),
            'model_ages': self.get_model_ages(),
            'training_history': self.get_all_reports()[:30]
        }
        
        return json.dumps(status, indent=2, default=str)


def main():
    """Main monitor function"""
    monitor = TrainingMonitor()
    monitor.print_dashboard()
    
    # Ask if user wants JSON export
    print("📄 Export status as JSON? (Y/N): ", end="")
    try:
        response = input().strip().upper()
        if response == 'Y':
            json_data = monitor.export_status_json()
            
            with open('training_status.json', 'w') as f:
                f.write(json_data)
            
            print("✅ Exported to: training_status.json")
    except:
        pass


if __name__ == "__main__":
    main()
