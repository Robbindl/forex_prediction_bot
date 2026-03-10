"""
🎮 MASTER CONTROLLER - Manages all trading system components
Runs 24/7 and ensures everything stays alive
"""

import subprocess
import time
import psutil
from datetime import datetime
import logging
import sys
import os

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - MASTER - %(message)s',
    handlers=[
        logging.FileHandler('master_controller.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class MasterController:
    """Controls all trading system components"""
    
    def __init__(self):
        self.processes = {}
        self.check_interval = 60  # Check every minute
        self.last_daily = None
        self.last_weekly = None
        self.setup_alerts()
        
    def start_component(self, name, command):
        """Start a component if not running"""
        # Check if already running
        for proc in psutil.process_iter(['pid', 'cmdline']):
            try:
                cmd_line = ' '.join(proc.info['cmdline']) if proc.info['cmdline'] else ''
                if name in cmd_line:
                    logger.info(f"OK - {name} already running (PID: {proc.info['pid']})")
                    return True
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        
        # Start new process
        try:
            logger.info(f"STARTING - {name}...")
            
            # Use CREATE_NO_WINDOW flag on Windows to hide console
            if sys.platform == 'win32':
                process = subprocess.Popen(
                    command,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    creationflags=subprocess.CREATE_NO_WINDOW
                )
            else:
                process = subprocess.Popen(
                    command,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True
                )
                
            self.processes[name] = process
            logger.info(f"OK - {name} started (PID: {process.pid})")
            return True
        except Exception as e:
            logger.error(f"FAILED - Could not start {name}: {e}")
            return False
    
    def check_all(self):
        """Check all components and restart if dead - DOCKER OPTIMIZED"""
        # In Docker, we need a different approach to detect running processes
        import psutil
        
        # Check if trading_bot is running by looking for the process name
        trading_bot_running = False
        web_dash_running = False
        perf_dash_running = False
        
        for proc in psutil.process_iter(['pid', 'cmdline']):
            try:
                cmd = ' '.join(proc.info['cmdline']) if proc.info['cmdline'] else ''
                if 'trading_system.py' in cmd:
                    trading_bot_running = True
                elif 'web_app_live.py' in cmd:
                    web_dash_running = True
                elif 'performance_dashboard.py' in cmd:
                    perf_dash_running = True
            except:
                pass
        
        # Only start if not running
        if not trading_bot_running:
            self.start_component(
                "trading_bot",
                ["python", "trading_system.py", "--mode", "live", "--balance", "30", "--strategy-mode", "voting"]
            )
        
        if not web_dash_running:
            self.start_component(
                "web_dashboard",
                ["python", "web_app_live.py", "--balance", "30", "--no-telegram"]
            )
        
        if not perf_dash_running:
            self.start_component(
                "performance_dashboard",
                ["python", "performance_dashboard.py"]
            )
    
    def run_daily_tasks(self):
        """Run daily at 2 AM"""
        logger.info("DAILY TASK - Running training and maintenance...")
        try:
            # Get the full path to python in virtual environment
            python_path = r"C:\Users\ROBBIE\Downloads\forex_prediction_bot\venv_tf\Scripts\python.exe"
            
            # Train ML models using virtual environment python
            logger.info("DAILY TASK - Starting model training...")
            result = subprocess.run(
                [python_path, "trading_system.py", "--mode", "train"],
                capture_output=True,
                text=True,
                timeout=7200  # 2 hour max
            )
            
            if result.returncode == 0:
                logger.info("DAILY TASK - Training complete")
                # Log summary of training
                for line in result.stdout.split('\n')[-10:]:
                    if '✓' in line or '📊' in line:
                        logger.info(f"  {line.strip()}")
            else:
                logger.error(f"DAILY TASK - Training failed: {result.stderr}")
                
            # Run strategy comparison at 3 AM (separate process)
            time.sleep(3600)  # Wait 1 hour
            logger.info("DAILY TASK - Running strategy comparison...")
            compare_result = subprocess.run(
                [python_path, "trading_system.py", "--mode", "compare"],
                capture_output=True,
                text=True,
                timeout=3600  # 1 hour max
            )
            
            if compare_result.returncode == 0:
                logger.info("DAILY TASK - Strategy comparison complete")
                # Log best strategy
                for line in compare_result.stdout.split('\n')[-15:]:
                    if '🏆' in line or 'BEST' in line:
                        logger.info(f"  {line.strip()}")
            else:
                logger.error(f"DAILY TASK - Comparison failed: {compare_result.stderr}")
            
        except subprocess.TimeoutExpired:
            logger.error("DAILY TASK - Timed out")
        except Exception as e:
            logger.error(f"DAILY TASK - Error: {e}")
    
    def run_weekly_tasks(self):
        """Run weekly on Sunday at 8 PM"""
        logger.info("WEEKLY TASK - Running maintenance...")
        try:
            # Get Python path
            # Get Python path
            python_path = r"C:\Users\ROBBIE\Downloads\forex_prediction_bot\venv_tf\Scripts\python.exe"
            
            # Call maintenance.py if it exists
            if os.path.exists('maintenance.py'):
                logger.info("WEEKLY TASK - Running maintenance script...")
                result = subprocess.run(
                    [python_path, "maintenance.py"],
                    capture_output=True,
                    text=True,
                    timeout=7200  # 2 hour max
                )
                if result.returncode == 0:
                    logger.info("WEEKLY TASK - Maintenance complete")
                else:
                    logger.error(f"WEEKLY TASK - Maintenance failed: {result.stderr}")
            else:
                # Built-in maintenance if file doesn't exist
                self.built_in_maintenance()
                
            # Run health check
            logger.info("WEEKLY TASK - Running health check...")
            subprocess.run(
                [python_path, "health_check.py"],
                timeout=300  # 5 minutes max
            )
                
        except Exception as e:
            logger.error(f"WEEKLY TASK - Maintenance failed: {e}")
    
    def built_in_maintenance(self):
        """Built-in maintenance if maintenance.py doesn't exist"""
        logger.info("WEEKLY TASK - Running built-in maintenance...")
        
        # Clean old log files (older than 7 days)
        try:
            deleted_count = 0
            for file in os.listdir('.'):
                if file.endswith('.log') or file.endswith('.csv'):
                    file_path = os.path.join('.', file)
                    file_time = datetime.fromtimestamp(os.path.getmtime(file_path))
                    if (datetime.now() - file_time).days > 7:
                        os.remove(file_path)
                        deleted_count += 1
                        logger.info(f"  Deleted old file: {file}")
            if deleted_count > 0:
                logger.info(f"  Cleaned up {deleted_count} old files")
        except Exception as e:
            logger.error(f"  Cleanup error: {e}")
        
        # Create backup directory
        try:
            backup_dir = f"backup_{datetime.now().strftime('%Y%m%d')}"
            os.makedirs(backup_dir, exist_ok=True)
            backup_count = 0
            
            # Backup important files
            important_files = ['risk_config.json', 'paper_trades.json', '.env']
            for file in important_files:
                if os.path.exists(file):
                    import shutil
                    shutil.copy(file, backup_dir)
                    backup_count += 1
                    logger.info(f"  Backed up: {file}")
            
            # Backup ML models
            if os.path.exists('ml_models'):
                shutil.copytree('ml_models', f'{backup_dir}/ml_models', dirs_exist_ok=True)
                logger.info(f"  Backed up: ml_models/")
                
            logger.info(f"  Backed up {backup_count} files and models")
        except Exception as e:
            logger.error(f"  Backup error: {e}")
        
        logger.info("WEEKLY TASK - Built-in maintenance complete")
    
    def run(self):
        """Main loop"""
        logger.info("="*60)
        logger.info("MASTER CONTROLLER STARTED")
        logger.info("="*60)
        # REMOVED ALL EMOJIS - using plain text
        logger.info("Trading Bot: ACTIVE (VOTING mode, $30 balance)")
        logger.info("Web Dashboard: http://localhost:5000")
        logger.info("Performance Dashboard: http://localhost:8050")
        logger.info("Telegram Alerts: ACTIVE (handled by main bot only)")
        logger.info("Auto-Trainer: Background (event-based)")
        logger.info("="*60)
        logger.info("Daily tasks: 2 AM training, 3 AM comparison")
        logger.info("Weekly tasks: Sunday 8 PM maintenance")
        logger.info("="*60)
        
        while True:
            try:
                now = datetime.now()
                
                # Always keep these running
                self.check_all()
                
                # Daily tasks at 2 AM
                if now.hour == 2 and self.last_daily != now.date():
                    logger.info("SCHEDULED - Daily task time reached")
                    self.run_daily_tasks()
                    self.last_daily = now.date()
                
                # Weekly tasks Sunday 8 PM
                if now.weekday() == 6 and now.hour == 20 and self.last_weekly != now.date():
                    logger.info("SCHEDULED - Weekly task time reached")
                    self.run_weekly_tasks()
                    self.last_weekly = now.date()
                
                # Sleep for check interval
                time.sleep(self.check_interval)
                
            except KeyboardInterrupt:
                logger.info("SHUTDOWN - Stopping...")
                break
            except Exception as e:
                logger.error(f"ERROR - Main loop: {e}")
                time.sleep(5)
    
    def setup_alerts(self):
        """Setup alert configurations"""
        import json
        import os
        
        # Load Telegram config (for reference only - bot handles Telegram)
        self.telegram_config = None
        if os.path.exists('config/telegram_config.json'):
            try:
                with open('config/telegram_config.json', 'r') as f:
                    self.telegram_config = json.load(f)
                logger.info("Telegram config loaded (handled by main bot)")
            except:
                logger.warning("Could not load Telegram config")
        
        # Load Email config
        self.email_config = None
        if os.path.exists('config/email_config.json'):
            try:
                with open('config/email_config.json', 'r') as f:
                    self.email_config = json.load(f)
                logger.info("Email alerts configured")
            except:
                logger.warning("Could not load Email config")

if __name__ == "__main__":
    controller = MasterController()
    controller.run()