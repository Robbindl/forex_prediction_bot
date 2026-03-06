"""
🧹 WEEKLY MAINTENANCE - Runs every Sunday
"""

import subprocess
import os
import shutil
from datetime import datetime, timedelta

def cleanup_old_files(days=7):
    """Delete files older than X days"""
    print("🧹 Cleaning up old files...")
    
    folders = ['backtest_results', 'ml_models', 'logs', 'reports']
    for folder in folders:
        if os.path.exists(folder):
            for file in os.listdir(folder):
                file_path = os.path.join(folder, file)
                file_time = datetime.fromtimestamp(os.path.getmtime(file_path))
                if datetime.now() - file_time > timedelta(days=days):
                    os.remove(file_path)
                    print(f"  Deleted: {file_path}")

def backup_important_files():
    """Backup models and config"""
    print("💾 Backing up important files...")
    
    backup_dir = f"backup_{datetime.now().strftime('%Y%m%d')}"
    os.makedirs(backup_dir, exist_ok=True)
    
    # Copy models
    if os.path.exists('ml_models'):
        shutil.copytree('ml_models', f'{backup_dir}/ml_models')
    
    # Copy config
    if os.path.exists('risk_config.json'):
        shutil.copy('risk_config.json', backup_dir)
    
    # Copy trade history
    if os.path.exists('paper_trades.json'):
        shutil.copy('paper_trades.json', backup_dir)
    
    print(f"✅ Backed up to {backup_dir}")

def generate_weekly_report():
    """Create weekly performance report"""
    print("📊 Generating weekly report...")
    
    # Run strategy comparison
    subprocess.run(["python", "trading_system.py", "--mode", "compare"])
    
    # Train models with fresh data
    subprocess.run(["python", "trading_system.py", "--mode", "train"])
    
    print("✅ Weekly maintenance complete")

if __name__ == "__main__":
    print("\n" + "="*60)
    print("🧹 WEEKLY MAINTENANCE")
    print("="*60)
    
    cleanup_old_files(7)
    backup_important_files()
    generate_weekly_report()