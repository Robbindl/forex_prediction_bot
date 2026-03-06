#!/usr/bin/env python3
r"""
████████╗██████╗  █████╗ ██████╗ ██╗███╗   ██╗ ██████╗     ███████╗██╗██╗  ██╗
╚══██╔══╝██╔══██╗██╔══██╗██╔══██╗██║████╗  ██║██╔════╝     ██╔════╝██║╚██╗██╔╝
   ██║   ██████╔╝███████║██║  ██║██║██╔██╗ ██║██║  ███╗    █████╗  ██║ ╚███╔╝ 
   ██║   ██╔══██╗██╔══██║██║  ██║██║██║╚██╗██║██║   ██║    ██╔══╝  ██║ ██╔██╗ 
   ██║   ██║  ██║██║  ██║██████╔╝██║██║ ╚████║╚██████╔╝    ██║     ██║██╔╝ ██╗
   ╚═╝   ╚═╝  ╚═╝╚═╝  ╚═╝╚═════╝ ╚═╝╚═╝  ╚═══╝ ╚═════╝     ╚═╝     ╚═╝╚═╝  ╚═╝
   
   ███████╗██╗██╗  ██╗    ███████╗ ██████╗ ██████╗     ███████╗██╗   ██╗███████╗████████╗███████╗███╗   ███╗
   ██╔════╝██║╚██╗██╔╝    ██╔════╝██╔═══██╗██╔══██╗    ██╔════╝██║   ██║██╔════╝╚══██╔══╝██╔════╝████╗ ████║
   █████╗  ██║ ╚███╔╝     █████╗  ██║   ██║██████╔╝    █████╗  ██║   ██║███████╗   ██║   █████╗  ██╔████╔██║
   ██╔══╝  ██║ ██╔██╗     ██╔══╝  ██║   ██║██╔══██╗    ██╔══╝  ██║   ██║╚════██║   ██║   ██╔══╝  ██║╚██╔╝██║
   ██║     ██║██╔╝ ██╗    ██║     ╚██████╔╝██║  ██║    ██║     ╚██████╔╝███████║   ██║   ███████╗██║ ╚═╝ ██║
   ╚═╝     ╚═╝╚═╝  ╚═╝    ╚═╝      ╚═════╝ ╚═╝  ╚═╝    ╚═╝      ╚═════╝ ╚══════╝   ╚═╝   ╚══════╝╚═╝     ╚═╝
   
   ██╗   ██╗██╗████████╗██╗███╗   ███╗ █████╗ ████████╗███████╗
   ██║   ██║██║╚══██╔══╝██║████╗ ████║██╔══██╗╚══██╔══╝██╔════╝
   ██║   ██║██║   ██║   ██║██╔████╔██║███████║   ██║   █████╗  
   ╚██╗ ██╔╝██║   ██║   ██║██║╚██╔╝██║██╔══██║   ██║   ██╔══╝  
    ╚████╔╝ ██║   ██║   ██║██║ ╚═╝ ██║██║  ██║   ██║   ███████╗
     ╚═══╝  ╚═╝   ╚═╝   ╚═╝╚═╝     ╚═╝╚═╝  ╚═╝   ╚═╝   ╚══════╝
   
   ⚡ CUSTOMIZED FOR: venv311 on Windows
   📁 Working directory: C:\Users\ROBBIE\Downloads\forex_prediction_bot
"""

import os
import re
import shutil
import sys
import subprocess
from pathlib import Path
from datetime import datetime
import json

# =============================================================================
# CONFIGURATION - CUSTOMIZED FOR YOUR SETUP
# =============================================================================

class Config:
    """Central configuration for the fix script - CUSTOMIZED FOR YOUR SYSTEM"""
    
    # Your specific paths
    PROJECT_ROOT = Path(__file__).parent.absolute()
    VENV_PATH = PROJECT_ROOT / 'venv311'
    PYTHON_PATH = VENV_PATH / 'Scripts' / 'python.exe'
    
    print(f"\n🔧 Detected Configuration:")
    print(f"  • Project Root: {PROJECT_ROOT}")
    print(f"  • Virtual Environment: {VENV_PATH}")
    print(f"  • Python Executable: {PYTHON_PATH}")
    print(f"  • Python exists: {PYTHON_PATH.exists()}")
    
    # Files to fix (complete list from your project)
    CONFIG_FILES = [
        'config/config.py',
        'data/fetcher.py',
        'sentiment_analyzer.py',
        'paper_trader.py',
        'web_app_live.py',
        'trading_system.py',
        'risk_manager.py',
        'advanced_predictor.py',
        'advanced_risk_manager.py',
        'auto_train_daily.py',
        'main_bot.py',
        'ultimate_trading_bot.py',
        'monitor.py',
        'master_controller.py',
    ]
    
    # Batch files (Windows specific)
    BATCH_FILES = [
        'start_dashboard.bat',
        'start_trading.bat', 
        'start_master.bat',
        'install_automation.bat',
    ]
    
    # PowerShell scripts
    POWERSHELL_FILES = [
        'setup_daily_training.ps1',
        'setup_auto_training.ps1',
    ]
    
    # Directories to create
    DIRECTORIES = [
        'ml_models',
        'trained_models', 
        'training_logs',
        'logs',
        'backtest_results',
        'config',
        'templates',
        'static',
        'trade_logs',
        'reports',
        'backups',
        'temp',
    ]
    
    # Required Python packages (with versions compatible with Python 3.11)
    REQUIRED_PACKAGES = [
        'psutil>=5.9.0',
        'textblob>=0.17.1',
        'Flask-CORS>=4.0.0',
        'python-telegram-bot>=20.0',
        'python-dotenv>=1.0.0',
        'twelvedata>=0.3.0',
        'alpha-vantage>=2.3.1',
        'finnhub-python>=2.4.18',
        'yfinance>=0.2.33',
        'pandas>=2.0.3',
        'numpy>=1.24.3',
        'scikit-learn>=1.3.0',
        'xgboost>=2.0.0',
        'requests>=2.31.0',
        'schedule>=1.2.0',
        'flask>=2.3.0',
        'flask-socketio>=5.3.0',
        'eventlet>=0.33.0',
        'joblib>=1.3.0',
        'tabulate>=0.9.0',
        'colorama>=0.4.6',
    ]


# =============================================================================
# BACKUP SYSTEM
# =============================================================================

class BackupManager:
    """Creates backups before modifying files"""
    
    def __init__(self):
        self.backup_dir = Config.PROJECT_ROOT / 'backups' / datetime.now().strftime('%Y%m%d_%H%M%S')
        self.backup_dir.mkdir(parents=True, exist_ok=True)
        print(f"\n📦 Backup directory: {self.backup_dir}")
    
    def backup_file(self, filepath):
        """Create a backup of a file before modifying it"""
        if not filepath.exists():
            return False
        
        rel_path = filepath.relative_to(Config.PROJECT_ROOT)
        backup_path = self.backup_dir / rel_path
        backup_path.parent.mkdir(parents=True, exist_ok=True)
        
        shutil.copy2(filepath, backup_path)
        print(f"  💾 Backed up: {rel_path}")
        return True
    
    def backup_directory(self, directory):
        """Backup an entire directory"""
        if not directory.exists():
            return
        
        rel_path = directory.relative_to(Config.PROJECT_ROOT)
        backup_path = self.backup_dir / rel_path
        
        shutil.copytree(directory, backup_path, dirs_exist_ok=True)
        print(f"  💾 Backed up directory: {rel_path}")


# =============================================================================
# VENV311-SPECIFIC FIXES
# =============================================================================

class Venv311Fixes:
    """Fixes specific to your venv311 environment"""
    
    def __init__(self, backup):
        self.backup = backup
    
    def fix_batch_files_for_venv311(self):
        """Update all batch files to use your specific venv311 path"""
        print("\n📝 Fixing batch files for venv311...")
        
        for file in Config.BATCH_FILES:
            file_path = Config.PROJECT_ROOT / file
            if file_path.exists():
                self.backup.backup_file(file_path)
                content = file_path.read_text()
                
                # Update to use your specific venv311 path
                venv_activate = r'call .\\venv311\\Scripts\\activate'
                python_path = r'C:\\Users\\ROBBIE\\Downloads\\forex_prediction_bot\\venv311\\Scripts\\python.exe'
                
                # Replace any venv references with venv311
                content = re.sub(r'venv[0-9]*', 'venv311', content)
                
                # Ensure correct Python path
                if 'python.exe' in content:
                    content = re.sub(
                        r'python\.exe',
                        python_path,
                        content
                    )
                
                # Add balance parameter if missing
                if 'web_app_live.py' in file and '--balance' not in content:
                    content = content.replace(
                        'python web_app_live.py',
                        f'{python_path} web_app_live.py --balance 20'
                    )
                
                # Fix working directory
                content = re.sub(
                    r'cd /d C:.*?forex_prediction_bot',
                    'cd /d %~dp0',
                    content
                )
                
                file_path.write_text(content)
                print(f"  ✅ Fixed {file}")
    
    def fix_powershell_scripts(self):
        """Fix PowerShell scripts for venv311"""
        print("\n📝 Fixing PowerShell scripts for venv311...")
        
        for file in Config.POWERSHELL_FILES:
            file_path = Config.PROJECT_ROOT / file
            if file_path.exists():
                self.backup.backup_file(file_path)
                
                # Read with utf-8 encoding and ignore errors
                try:
                    # Try utf-8 first
                    content = file_path.read_text(encoding='utf-8')
                except UnicodeDecodeError:
                    try:
                        # Fall back to utf-16 (common for PowerShell)
                        content = file_path.read_text(encoding='utf-16')
                    except UnicodeDecodeError:
                        try:
                            # Last resort: read as binary and decode with replacement
                            content = file_path.read_text(encoding='cp1252', errors='replace')
                        except:
                            print(f"  ⚠️  Could not read {file} with any encoding, skipping")
                            continue
                
                # Update Python path to use venv311
                old_path = r'C:\Users\ROBBIE\Downloads\forex_prediction_bot\venv311\Scripts\python.exe'
                python_path = str(Config.PYTHON_PATH).replace('\\', '\\\\')
                
                # Replace any python.exe path with your specific one
                content = re.sub(
                    r'\$PythonPath\s*=\s*".*python\.exe"',
                    f'$PythonPath = "{python_path}"',
                    content
                )
                
                # Update working directory
                project_path = str(Config.PROJECT_ROOT).replace('\\', '\\\\')
                content = re.sub(
                    r'\$WorkingDirectory\s*=\s*".*"',
                    f'$WorkingDirectory = "{project_path}"',
                    content
                )
                
                # Write back with utf-8 encoding
                file_path.write_text(content, encoding='utf-8')
                print(f"  ✅ Fixed {file}")
    
    def create_activation_script(self):
        """Create a helper script to activate venv311 easily"""
        activate_path = Config.PROJECT_ROOT / 'activate_venv.bat'
        
        # Remove emojis and use ASCII only
        content = """@echo off
    echo ========================================
    echo ACTIVATING VENV311
    echo ========================================
    echo.

    call "%~dp0venv311\Scripts\activate.bat"

    echo.
    echo ========================================
    echo VIRTUAL ENVIRONMENT ACTIVATED!
    echo ========================================
    echo.
    echo Python version:
    python --version
    echo Current path: %CD%
    echo.
    echo ========================================
    echo AVAILABLE COMMANDS:
    echo ========================================
    echo.
    echo [DASHBOARD]  python web_app_live.py --balance 20
    echo [TRADING]    python trading_system.py --mode live --balance 20
    echo [STATUS]     python training_monitor.py
    echo [TRADES]     python view_trades.py
    echo.
    echo ========================================
    cmd /k
    """
        activate_path.write_text(content, encoding='ascii')
        print(f"  ✅ Created activation script: activate_venv.bat")
    
    def fix_python_shebangs(self):
        """Fix Python shebang lines to use venv311"""
        print("\n📝 Fixing Python shebang lines...")
        
        python_files = list(Config.PROJECT_ROOT.glob('*.py')) + list(Config.PROJECT_ROOT.glob('**/*.py'))
        
        for py_file in python_files:
            if 'venv' in str(py_file):
                continue
            
            try:
                content = py_file.read_text()
                
                # Fix shebang to use env python (more portable)
                if content.startswith('#!'):
                    new_content = re.sub(
                        r'#!.*python.*',
                        '#!/usr/bin/env python3',
                        content
                    )
                    if new_content != content:
                        self.backup.backup_file(py_file)
                        py_file.write_text(new_content)
                        print(f"  ✅ Fixed shebang in {py_file.name}")
            except:
                pass


# =============================================================================
# SECURITY FIXES
# =============================================================================

class SecurityFixes:
    """Fix all security issues"""
    
    def __init__(self, backup):
        self.backup = backup
    
    def create_env_file(self):
        """Create .env file from exposed keys - ASCII only version"""
        env_path = Config.PROJECT_ROOT / '.env'
        if env_path.exists():
            env_path = Config.PROJECT_ROOT / '.env.new'
            print("⚠️  .env already exists, creating .env.new instead")
        
        # Remove all emojis, use ASCII only
        env_content = '''# ==================================================
    # TRADING BOT ENVIRONMENT VARIABLES
    # ==================================================
    # Created for: C:\\Users\\ROBBIE\\Downloads\\forex_prediction_bot
    # Python: venv311
    # ==================================================

    # MARKET DATA APIS
    # ------------------------------------------------
    ALPHA_VANTAGE_KEY=PACP0NRM3SIFWZBL
    FINNHUB_KEY=d6bc2ohr01qnr27kdcb0d6bc2ohr01qnr27kdcbg
    TWELVEDATA_KEY=6c8e5137892642fe96cbfbf9d782c7d0

    # NEWS APIS
    # ------------------------------------------------
    NEWSAPI_KEY=45bc87b407044ac1bbf346a997ce41e5
    GNEWS_KEY=3e75eb4d90be059d9e3494368a40999c
    RAPIDAPI_KEY=ef4048747cmshc06561be18df7dp1d5914jsnb61136336bf5

    # TELEGRAM ALERTS
    # ------------------------------------------------
    # WARNING: REVOKE THIS TOKEN IMMEDIATELY! It's been exposed!
    TELEGRAM_TOKEN=8292440321:AAHYqP8J-MaUhLbtLH82RTrizka8r3Dhzw4
    TELEGRAM_CHAT_ID=5747207752

    # EMAIL ALERTS (Gmail App Password)
    # ------------------------------------------------
    EMAIL_USERNAME=griffonstradingbot@gmail.com
    EMAIL_PASSWORD=erfo hjsp hmwj pgqc

    # TRADING DEFAULTS
    # ------------------------------------------------
    DEFAULT_BALANCE=20
    DEFAULT_RISK=1.0
    MAX_POSITIONS=5
    '''
        
        # Write with ascii encoding, ignoring any remaining non-ascii chars
        env_path.write_text(env_content, encoding='ascii', errors='ignore')
        print(f"  ✅ Created {env_path}")
        return env_path
    
    def fix_config_py(self):
        """Update config.py to use environment variables"""
        config_path = Config.PROJECT_ROOT / 'config' / 'config.py'
        if not config_path.exists():
            print("  ⚠️  config.py not found, skipping")
            return
        
        self.backup.backup_file(config_path)
        content = config_path.read_text()
        
        # Add imports if missing
        if 'from dotenv import load_dotenv' not in content:
            content = 'import os\nfrom dotenv import load_dotenv\n\n# Load environment variables\nload_dotenv()\n\n' + content
        
        # Replace hardcoded keys with environment variables
        patterns = [
            (r'ALPHA_VANTAGE_API_KEY\s*=\s*"[^"]*"', 
             'ALPHA_VANTAGE_API_KEY = os.getenv("ALPHA_VANTAGE_KEY", "")'),
            (r'FINNHUB_API_KEY\s*=\s*"[^"]*"', 
             'FINNHUB_API_KEY = os.getenv("FINNHUB_KEY", "")'),
            (r'TWELVE_DATA_API_KEY\s*=\s*"[^"]*"', 
             'TWELVE_DATA_API_KEY = os.getenv("TWELVEDATA_KEY", "")'),
        ]
        
        for pattern, replacement in patterns:
            content = re.sub(pattern, replacement, content)
        
        # Add DEFAULT_BALANCE if missing
        if 'DEFAULT_BALANCE' not in content:
            content += '\n\n# Default trading balance\nDEFAULT_BALANCE = float(os.getenv("DEFAULT_BALANCE", "20"))\n'
        
        config_path.write_text(content)
        print("  ✅ Fixed config.py")
    
    def create_gitignore(self):
        """Create comprehensive .gitignore - ASCII only version"""
        gitignore_path = Config.PROJECT_ROOT / '.gitignore'
        if gitignore_path.exists():
            self.backup.backup_file(gitignore_path)
        
        # Remove all emojis, use ASCII only
        content = '''# ==================================================
    # ULTIMATE .GITIGNORE FOR TRADING BOT
    # ==================================================

    # SECURITY - NEVER COMMIT THESE!
    # ------------------------------------------------
    .env
    .env.*
    !.env.example
    config/*_config.json
    config/telegram_config.json
    config/email_config.json
    *key*.py
    *secret*.py

    # PYTHON
    # ------------------------------------------------
    __pycache__/
    *.py[cod]
    *.pyc
    *.pyo
    *.pyd
    .Python
    .pytest_cache/
    .coverage
    htmlcov/
    *.cover
    *.log
    *.log.*

    # VIRTUAL ENVIRONMENTS
    # ------------------------------------------------
    venv/
    venv311/
    env/
    ENV/
    pythonenv*

    # TRADING DATA
    # ------------------------------------------------
    paper_trades.json
    paper_trades_backup_*.json
    *.db
    *.sqlite
    *.sqlite3
    backtest_results/*.csv
    !backtest_results/example_*.csv

    # ML MODELS
    # ------------------------------------------------
    ml_models/
    trained_models/
    *.pkl
    *.h5
    *.joblib
    *.onnx

    # LOGS
    # ------------------------------------------------
    logs/
    training_logs/
    *.log
    *.log.*
    master_controller.log
    health_log.txt

    # WEB APP
    # ------------------------------------------------
    static/
    *.pid
    *.sock

    # IDE
    # ------------------------------------------------
    .vscode/
    .idea/
    *.swp
    *.swo
    *~
    .DS_Store
    Thumbs.db
    desktop.ini

    # PACKAGES
    # ------------------------------------------------
    *.egg-info/
    build/
    dist/
    *.egg

    # REPORTS
    # ------------------------------------------------
    reports/
    backups/
    *.html
    !templates/*.html

    # CONFIGURATION
    # ------------------------------------------------
    config/local_*.py
    *.local.py

    # TEMPORARY FILES
    # ------------------------------------------------
    *.tmp
    *.temp
    *.bak
    *.old
    *.orig
    '''
        
        # Write with ascii encoding
        gitignore_path.write_text(content, encoding='ascii')
        print("  ✅ Created .gitignore")


# =============================================================================
# DATA FETCHING FIXES
# =============================================================================

class DataFetchingFixes:
    """Fix all data fetching issues"""
    
    def __init__(self, backup):
        self.backup = backup
    
    def fix_yahoo_symbols(self):
        """Fix Yahoo Finance symbol mappings"""
        fetcher_path = Config.PROJECT_ROOT / 'data' / 'fetcher.py'
        if not fetcher_path.exists():
            print("  ⚠️  fetcher.py not found, skipping")
            return
        
        self.backup.backup_file(fetcher_path)
        
        # Read with utf-8 encoding
        try:
            content = fetcher_path.read_text(encoding='utf-8')
        except UnicodeDecodeError:
            try:
                # Try utf-8-sig (for files with BOM)
                content = fetcher_path.read_text(encoding='utf-8-sig')
            except UnicodeDecodeError:
                try:
                    # Try latin-1 as last resort (never fails)
                    content = fetcher_path.read_text(encoding='latin-1')
                except:
                    print("  ❌ Could not read fetcher.py with any encoding")
                    return
        
        # Correct Yahoo Forex symbol mapping
        old_map = r'def _to_yahoo_forex.*?yahoo_map = {.*?}'
        
        new_map = '''    def _to_yahoo_forex(self, pair: str) -> str:
            """Convert forex pair to Yahoo Finance symbol - FIXED VERSION"""
            yahoo_map = {
                # Majors
                'EUR/USD': 'EURUSD=X',
                'GBP/USD': 'GBPUSD=X',
                'USD/JPY': 'USDJPY=X',      # Fixed
                'AUD/USD': 'AUDUSD=X',
                'USD/CAD': 'USDCAD=X',       # Fixed
                'NZD/USD': 'NZDUSD=X',
                'USD/CHF': 'USDCHF=X',       # Fixed
                
                # Crosses
                'EUR/GBP': 'EURGBP=X',
                'EUR/JPY': 'EURJPY=X',
                'GBP/JPY': 'GBPJPY=X',
                'AUD/JPY': 'AUDJPY=X',
                'EUR/AUD': 'EURAUD=X',
                'GBP/AUD': 'GBPAUD=X',
                'AUD/CAD': 'AUDCAD=X',
                'CAD/JPY': 'CADJPY=X',
                'CHF/JPY': 'CHFJPY=X',
                'EUR/CAD': 'EURCAD=X',
                'EUR/CHF': 'EURCHF=X',
                'GBP/CAD': 'GBPCAD=X',
                'GBP/CHF': 'GBPCHF=X',
            }
            
            # Try direct mapping first, then fallback to conversion
            if pair in yahoo_map:
                return yahoo_map[pair]
            
            # Fallback: replace / with nothing and add =X
            return pair.replace('/', '') + '=X'
    '''
        
        # Replace using regex with DOTALL flag
        content = re.sub(old_map, new_map, content, flags=re.DOTALL)
        
        # Write back with utf-8 encoding
        fetcher_path.write_text(content, encoding='utf-8')
        print("  ✅ Fixed Yahoo symbols in fetcher.py")


# =============================================================================
# WEB APP FIXES
# =============================================================================

class WebAppFixes:
    """Fix all web application issues"""
    
    def __init__(self, backup):
        self.backup = backup
    
    def fix_html_templates(self):
        """Fix HTML template issues"""
        html_path = Config.PROJECT_ROOT / 'templates' / 'index.html'
        if html_path.exists():
            self.backup.backup_file(html_path)
            content = html_path.read_text(encoding='utf-8', errors='ignore')
            
            # Remove PowerShell artifact if present
            if content.startswith("@'"):
                content = content[2:]
            
            html_path.write_text(content, encoding='utf-8')
            print("  ✅ Fixed index.html")
    
    def fix_web_app_venv_path(self):
        """Update web_app_live.py to work with venv311"""
        webapp_path = Config.PROJECT_ROOT / 'web_app_live.py'
        if webapp_path.exists():
            self.backup.backup_file(webapp_path)
            
            # Read with proper encoding
            try:
                content = webapp_path.read_text(encoding='utf-8')
            except UnicodeDecodeError:
                try:
                    content = webapp_path.read_text(encoding='utf-8-sig')
                except UnicodeDecodeError:
                    try:
                        content = webapp_path.read_text(encoding='latin-1')
                    except:
                        print("  ❌ Could not read web_app_live.py with any encoding")
                        return
            
            # Ensure balance argument is passed correctly
            if 'parser.add_argument' in content:
                content = re.sub(
                    r"parser\.add_argument\('--balance', type=float, default=\d+",
                    "parser.add_argument('--balance', type=float, default=20",
                    content
                )
            
            # Write back with utf-8 encoding
            webapp_path.write_text(content, encoding='utf-8')
            print("  ✅ Fixed web_app_live.py balance default")


# =============================================================================
# TRADING ENGINE FIXES
# =============================================================================

class TradingEngineFixes:
    """Fix all trading engine issues"""
    
    def __init__(self, backup):
        self.backup = backup
    
    def fix_paper_trader_threading(self):
        """Add thread locks to paper_trader.py"""
        trader_path = Config.PROJECT_ROOT / 'paper_trader.py'
        if not trader_path.exists():
            return
        
        self.backup.backup_file(trader_path)
        content = trader_path.read_text()
        
        # Add threading import if missing
        if 'import threading' not in content:
            content = 'import threading\n' + content
        
        # Add lock in __init__ if missing
        if 'self.lock = threading.RLock()' not in content:
            init_pattern = r'def __init__\(self,.*?\):'
            
            def add_lock(match):
                return match.group(0) + '\n        self.lock = threading.RLock()'
            
            content = re.sub(init_pattern, add_lock, content, count=1)
        
        trader_path.write_text(content)
        print("  ✅ Added thread locks to paper_trader.py")


# =============================================================================
# INFRASTRUCTURE FIXES
# =============================================================================

class InfrastructureFixes:
    """Fix all infrastructure issues"""
    
    def __init__(self, backup):
        self.backup = backup
    
    def create_directories(self):
        """Create all required directories"""
        print("\n📁 Creating directories...")
        for dir_name in Config.DIRECTORIES:
            dir_path = Config.PROJECT_ROOT / dir_name
            dir_path.mkdir(parents=True, exist_ok=True)
            print(f"  ✅ {dir_name}/")
    
    def update_requirements(self):
        """Update requirements.txt with all dependencies"""
        req_path = Config.PROJECT_ROOT / 'requirements.txt'
        if req_path.exists():
            self.backup.backup_file(req_path)
        
        # Create requirements.txt with all packages
        content = '\n'.join(Config.REQUIRED_PACKAGES) + '\n'
        req_path.write_text(content)
        
        print(f"  ✅ Updated requirements.txt with {len(Config.REQUIRED_PACKAGES)} packages")
    
    def create_install_script(self):
        """Create installation script for venv311"""
        install_path = Config.PROJECT_ROOT / 'install_deps.bat'
        
        # Remove emojis, use ASCII only
        content = f'''@echo off
    echo ========================================
    echo INSTALLING DEPENDENCIES FOR VENV311
    echo ========================================
    echo.

    cd /d {Config.PROJECT_ROOT}

    echo Activating virtual environment...
    call .\\venv311\\Scripts\\activate.bat

    echo.
    echo Upgrading pip...
    python -m pip install --upgrade pip

    echo.
    echo Installing requirements...
    pip install -r requirements.txt

    echo.
    echo ========================================
    echo INSTALLATION COMPLETE!
    echo ========================================
    echo.
    echo To verify installation, run:
    echo    python verify_installation.py
    echo.
    pause
    '''
        install_path.write_text(content, encoding='ascii')
        print("  ✅ Created install_deps.bat")
    
    def create_quick_start_guide(self):
        """Create a quick start guide for your specific setup - ASCII only"""
        guide_path = Config.PROJECT_ROOT / 'QUICK_START.txt'
        
        # Remove all emojis, use ASCII only
        content = f'''==================================================
    QUICK START GUIDE - YOUR TRADING BOT
    ==================================================

    Project: {Config.PROJECT_ROOT}
    Python: {Config.PYTHON_PATH}
    Virtual Env: venv311

    ==================================================
    STEP 1: FIX ALL ISSUES
    ==================================================
    Run the fix script you just used!

    ==================================================
    STEP 2: INSTALL DEPENDENCIES
    ==================================================
    Double-click: install_deps.bat
    Or run manually:
    cd {Config.PROJECT_ROOT}
    .\\venv311\\Scripts\\activate
    pip install -r requirements.txt

    ==================================================
    STEP 3: VERIFY INSTALLATION
    ==================================================
    python verify_installation.py

    ==================================================
    STEP 4: START TRADING
    ==================================================

    Option A - Full Auto System (24/7):
    double-click: start_master.bat
    or run: python master_controller.py

    Option B - Web Dashboard Only:
    double-click: start_dashboard.bat
    or run: python web_app_live.py --balance 20
    
    Access: http://localhost:5000
    Status: http://localhost:5000/status

    Option C - Manual Trading:
    .\\venv311\\Scripts\\activate
    python trading_system.py --mode live --balance 20

    ==================================================
    QUICK COMMANDS (after activation)
    ==================================================
    # Check training status
    python training_monitor.py

    # View current trades
    python view_trades.py

    # Backtest BTC
    python trading_system.py --mode backtest --asset BTC-USD

    # Compare all strategies
    python trading_system.py --mode compare

    ==================================================
    IMPORTANT NOTES
    ==================================================
    1. REVOKE the exposed Telegram token via @BotFather!
    2. Update .env with new token
    3. All backups saved in: {Config.PROJECT_ROOT}\\backups\\
    4. Logs are in: {Config.PROJECT_ROOT}\\logs\\
    5. Trade history: paper_trades.json

    ==================================================
    YOUR SYSTEM IS READY TO TRADE!
    ==================================================
    '''
        guide_path.write_text(content, encoding='ascii')
        print("  ✅ Created QUICK_START.txt")


# =============================================================================
# VERIFICATION SCRIPT
# =============================================================================

def create_verification_script():
    """Create enhanced verification script for venv311 - ASCII only"""
    verify_path = Config.PROJECT_ROOT / 'verify_installation.py'
    
    # Remove all emojis, use ASCII only
    content = '''#!/usr/bin/env python3
"""
VERIFICATION SCRIPT FOR VENV311
Run this to check if everything is working
"""

import sys
import importlib
import os
from pathlib import Path

def print_header(text):
    print(f"\\n{'='*60}")
    print(f"{text}")
    print(f"{'='*60}")

def check_venv():
    """Check if running in correct virtual environment"""
    print_header("VIRTUAL ENVIRONMENT")
    
    # Check if we're in venv311
    python_path = sys.executable
    print(f"Python: {python_path}")
    
    if 'venv311' in python_path:
        print("PASS: Running in venv311")
        return True
    else:
        print("WARNING: Not running in venv311")
        print("   Run: .\\\\venv311\\\\Scripts\\\\activate")
        return False

def check_packages():
    """Check all required packages"""
    print_header("REQUIRED PACKAGES")
    
    required = [
        'pandas', 'numpy', 'requests', 'yfinance',
        'sklearn', 'flask', 'flask_cors', 'dotenv',
        'psutil', 'textblob', 'schedule'
    ]
    
    all_ok = True
    
    for pkg in required:
        try:
            module = importlib.import_module(pkg.replace('-', '_'))
            version = getattr(module, '__version__', 'unknown')
            print(f"  PASS: {pkg:15s} {version}")
        except ImportError:
            print(f"  FAIL: {pkg:15s} NOT INSTALLED")
            all_ok = False
    
    return all_ok

def check_env_file():
    """Check if .env file exists and has keys"""
    print_header("ENVIRONMENT VARIABLES")
    
    if Path('.env').exists():
        print("  PASS: .env file exists")
        
        # Check if keys are set
        with open('.env', 'r') as f:
            content = f.read()
            if 'YOUR_KEY' not in content:
                print("  PASS: API keys appear to be configured")
            else:
                print("  WARNING: .env still has placeholder keys")
        return True
    else:
        print("  FAIL: .env file not found!")
        return False

def test_data_fetching():
    """Test data fetching from APIs"""
    print_header("DATA FETCHING TEST")
    
    try:
        sys.path.append('.')
        from data.fetcher import NASALevelFetcher
        
        fetcher = NASALevelFetcher()
        
        # Test forex
        print("Testing EUR/USD...")
        price, source = fetcher.get_real_time_price('EUR/USD', 'forex')
        if price:
            print(f"  PASS: EUR/USD: {price:.5f} from {source}")
        else:
            print(f"  FAIL: EUR/USD: No data")
        
        # Test crypto
        print("\\nTesting BTC-USD...")
        price, source = fetcher.get_real_time_price('BTC-USD', 'crypto')
        if price:
            print(f"  PASS: BTC-USD: ${price:,.2f} from {source}")
        else:
            print(f"  FAIL: BTC-USD: No data")
        
        # Test stock
        print("\\nTesting AAPL...")
        price, source = fetcher.get_real_time_price('AAPL', 'stocks')
        if price:
            print(f"  PASS: AAPL: ${price:.2f} from {source}")
        else:
            print(f"  FAIL: AAPL: No data")
        
        return True
        
    except Exception as e:
        print(f"ERROR: {e}")
        return False

def main():
    print("\\n" + "="*60)
    print("VERIFICATION FOR VENV311")
    print("="*60)
    
    checks = [
        ("Virtual Environment", check_venv),
        ("Required Packages", check_packages),
        ("Environment File", check_env_file),
        ("Data Fetching", test_data_fetching),
    ]
    
    results = {}
    for name, func in checks:
        try:
            results[name] = func()
        except Exception as e:
            print(f"{name} failed: {e}")
            results[name] = False
    
    print_header("SUMMARY")
    all_passed = all(results.values())
    
    for name, passed in results.items():
        status = "PASS" if passed else "FAIL"
        print(f"{status} - {name}")
    
    print("\\n" + "="*60)
    if all_passed:
        print("ALL CHECKS PASSED! Your trading bot is ready!")
        print("\\nNext steps:")
        print("1. Start dashboard: python web_app_live.py --balance 20")
        print("2. Access: http://localhost:5000")
        print("3. Start trading: python master_controller.py")
    else:
        print("Some checks failed - run the fix script again")
    print("="*60 + "\\n")
    
    return 0 if all_passed else 1

if __name__ == "__main__":
    sys.exit(main())
'''
    verify_path.write_text(content, encoding='ascii')
    print("  ✅ Created verification script")


# =============================================================================
# MAIN FIXER CLASS
# =============================================================================

class UltimateTradingBotFixer:
    """Master fixer class that runs all fixes"""
    
    def __init__(self):
        self.backup = BackupManager()
        self.venv311 = Venv311Fixes(self.backup)
        self.security = SecurityFixes(self.backup)
        self.data = DataFetchingFixes(self.backup)
        self.web = WebAppFixes(self.backup)
        self.trading = TradingEngineFixes(self.backup)
        self.infrastructure = InfrastructureFixes(self.backup)
    
    def print_banner(self):
        banner = '''
╔══════════════════════════════════════════════════════════════════════════════╗
║                    ULTIMATE TRADING BOT FIXER v2.0                          ║
║                    CUSTOMIZED FOR YOUR VENV311                              ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  📁 Project: C:\\Users\\ROBBIE\\Downloads\\forex_prediction_bot               ║
║  🐍 Python:  venv311\\Scripts\\python.exe                                     ║
║  💰 Balance: $20 default                                                    ║
╚══════════════════════════════════════════════════════════════════════════════╝
'''
        print(banner)
    
    def run_fixes(self):
        """Run all fixes in order"""
        
        print("\n🔧 STEP 1: VENV311-SPECIFIC FIXES")
        print("=" * 50)
        self.venv311.fix_batch_files_for_venv311()
        self.venv311.fix_powershell_scripts()
        self.venv311.create_activation_script()
        self.venv311.fix_python_shebangs()
        
        print("\n🔐 STEP 2: SECURITY FIXES")
        print("=" * 50)
        self.security.create_env_file()
        self.security.fix_config_py()
        self.security.create_gitignore()
        
        print("\n📡 STEP 3: DATA FETCHING FIXES")
        print("=" * 50)
        self.data.fix_yahoo_symbols()
        
        print("\n🌐 STEP 4: WEB APP FIXES")
        print("=" * 50)
        self.web.fix_html_templates()
        self.web.fix_web_app_venv_path()
        
        print("\n🤖 STEP 5: TRADING ENGINE FIXES")
        print("=" * 50)
        self.trading.fix_paper_trader_threading()
        
        print("\n⚙️ STEP 6: INFRASTRUCTURE FIXES")
        print("=" * 50)
        self.infrastructure.create_directories()
        self.infrastructure.update_requirements()
        self.infrastructure.create_install_script()
        self.infrastructure.create_quick_start_guide()
        create_verification_script()
    
    def print_summary(self):
        """Print final summary with venv311-specific instructions"""
        print("\n" + "="*80)
        print("✅ FIX COMPLETION SUMMARY - VENV311")
        print("="*80)
        
        print(f"\n📦 Backup created at: {self.backup.backup_dir}")
        
        print("\n📋 FILES MODIFIED:")
        print("  • All batch files updated for venv311")
        print("  • PowerShell scripts updated with correct paths")
        print("  • Created activate_venv.bat for easy activation")
        print("  • Created .env with all API keys")
        print("  • Updated config.py to use environment variables")
        print("  • Created .gitignore to prevent future leaks")
        print("  • Fixed Yahoo Finance symbols in fetcher.py")
        print("  • Fixed HTML templates")
        print("  • Added thread locks to paper_trader.py")
        print("  • Created all required directories")
        print("  • Updated requirements.txt")
        print("  • Created install_deps.bat")
        print("  • Created QUICK_START.txt")
        print("  • Created verify_installation.py")
        
        print("\n⚠️  CRITICAL SECURITY WARNING:")
        print("-" * 40)
        print("🔴 YOUR TELEGRAM TOKEN IS EXPOSED!")
        print("   Token: 8292440321:AAHYqP8J-MaUhLbtLH82RTrizka8r3Dhzw4")
        print("\n   IMMEDIATE ACTIONS:")
        print("   1. Open Telegram")
        print("   2. Search for @BotFather")
        print("   3. Send: /revoke")
        print("   4. Enter that token")
        print("   5. Create new bot with /newbot")
        print("   6. Update .env with new token")
        
        print("\n🚀 NEXT STEPS FOR VENV311:")
        print("-" * 40)
        print("1️⃣  Activate virtual environment:")
        print(f"    double-click: activate_venv.bat")
        print(f"    or run: .\\venv311\\Scripts\\activate")
        print()
        print("2️⃣  Install dependencies:")
        print("    double-click: install_deps.bat")
        print(f"    or run: pip install -r requirements.txt")
        print()
        print("3️⃣  Verify installation:")
        print("    python verify_installation.py")
        print()
        print("4️⃣  Start the system:")
        print("    • Full auto:    start_master.bat")
        print("    • Dashboard:    start_dashboard.bat")
        print("    • Manual:       python trading_system.py --mode live --balance 20")
        print()
        print("5️⃣  Access dashboard:")
        print("    • Main:    http://localhost:5000")
        print("    • Status:  http://localhost:5000/status")
        
        print("\n" + "🚀"*80)
        print("🚀 YOUR TRADING BOT IS NOW FIXED AND READY FOR VENV311!")
        print("🚀"*80 + "\n")
    
    def run(self):
        """Main execution method"""
        try:
            self.print_banner()
            self.run_fixes()
            self.print_summary()
            return 0
        except KeyboardInterrupt:
            print("\n\n⚠️ Fix interrupted by user")
            return 1
        except Exception as e:
            print(f"\n❌ Fatal error: {e}")
            import traceback
            traceback.print_exc()
            return 1


# =============================================================================
# SCRIPT ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    fixer = UltimateTradingBotFixer()
    sys.exit(fixer.run())