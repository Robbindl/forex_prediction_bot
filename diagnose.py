"""
TRADING BOT DIAGNOSTIC
======================
Run this once before starting the bot to confirm everything works.

Usage:
    python diagnose.py

Checks (in order):
  1. Python version
  2. Virtual environment
  3. Required packages
  4. Project files and folders
  5. .env file and API keys
  6. Live data fetch (yfinance)
  7. Internal module imports
  8. Paper trades file
  9. Database connection
 10. ML models
 11. Profitability upgrade integration
 12. God mode cleanup
 13. Port availability
 14. Overall verdict
"""

import sys
import os
import json
import socket
import importlib
from pathlib import Path
from datetime import datetime

# ─────────────────────────────────────────────
PASS  = "  [OK]  "
FAIL  = "  [FAIL]"
WARN  = "  [WARN]"
INFO  = "  [INFO]"
# ─────────────────────────────────────────────

results = []   # (status, message)

def ok(msg):
    results.append(("OK",   msg))
    print(f"{PASS} {msg}")

def fail(msg):
    results.append(("FAIL", msg))
    print(f"{FAIL} {msg}")

def warn(msg):
    results.append(("WARN", msg))
    print(f"{WARN} {msg}")

def info(msg):
    print(f"{INFO} {msg}")

def section(title):
    print(f"\n{'─'*55}")
    print(f"  {title}")
    print(f"{'─'*55}")


# ══════════════════════════════════════════════════════
# 1. PYTHON VERSION
# ══════════════════════════════════════════════════════
section("1. PYTHON VERSION")

ver = sys.version_info
info(f"Executable : {sys.executable}")
info(f"Version    : {sys.version.split()[0]}")

if ver.major == 3 and ver.minor >= 9:
    ok(f"Python {ver.major}.{ver.minor} — compatible")
elif ver.major == 3 and ver.minor >= 8:
    warn(f"Python {ver.major}.{ver.minor} — works but 3.11 recommended")
else:
    fail(f"Python {ver.major}.{ver.minor} — too old, need 3.8+")


# ══════════════════════════════════════════════════════
# 2. VIRTUAL ENVIRONMENT
# ══════════════════════════════════════════════════════
section("2. VIRTUAL ENVIRONMENT")

in_venv = (
    hasattr(sys, "real_prefix") or
    (hasattr(sys, "base_prefix") and sys.base_prefix != sys.prefix)
)

if in_venv:
    ok(f"Running inside venv: {sys.prefix}")
else:
    warn("Not running inside a virtual environment")
    warn("Activate with:  .\\venv311\\Scripts\\activate  (Windows)")

venv311 = Path("venv311")
if venv311.exists():
    ok("venv311 folder found")
else:
    warn("venv311 folder not found in current directory")
    info("Make sure you are running this FROM your forex_prediction_bot folder")


# ══════════════════════════════════════════════════════
# 3. REQUIRED PACKAGES
# ══════════════════════════════════════════════════════
section("3. REQUIRED PACKAGES")

PACKAGES = {
    # core
    "pandas":           "pandas",
    "numpy":            "numpy",
    "yfinance":         "yfinance",
    "sklearn":          "scikit-learn",
    "xgboost":          "xgboost",
    "scipy":            "scipy",
    "requests":         "requests",
    "joblib":           "joblib",
    # web
    "flask":            "flask",
    "flask_cors":       "flask-cors",
    "flask_socketio":   "flask-socketio",
    "dash":             "dash",
    "plotly":           "plotly",
    # system
    "psutil":           "psutil",
    "schedule":         "schedule",
    "dotenv":           "python-dotenv",
    # trading data
    "finnhub":          "finnhub-python",
    "textblob":         "textblob",
    "colorama":         "colorama",
    "tabulate":         "tabulate",
    # db
    "sqlalchemy":       "sqlalchemy",
}

missing_pkgs = []
for import_name, pip_name in PACKAGES.items():
    try:
        importlib.import_module(import_name)
        ok(f"{pip_name}")
    except ImportError:
        fail(f"{pip_name}  — run: pip install {pip_name}")
        missing_pkgs.append(pip_name)

if missing_pkgs:
    print()
    info(f"Install all missing at once:")
    info(f"  pip install {' '.join(missing_pkgs)}")


# ══════════════════════════════════════════════════════
# 4. PROJECT FILES & FOLDERS
# ══════════════════════════════════════════════════════
section("4. PROJECT FILES & FOLDERS")

REQUIRED_FILES = [
    "trading_system.py",
    "paper_trader.py",
    "run.py",
    "master_controller.py",
    "health_check.py",
    "sentiment_analyzer.py",
    "performance_dashboard.py",
    "web_app_live.py",
    "realtime_trader.py",
    "realtime_trader_5m.py",
    "advanced_predictor.py",
    "advanced_risk_manager.py",
    "advanced_backtester.py",
    "advanced_ai.py",
    "training_monitor.py",
    "monitor.py",
    "portfolio_optimizer.py",
    "market_regime_analyzer.py",
    "maintenance.py",
    "logger.py",
    "risk_manager.py",
]

REQUIRED_FOLDERS = [
    "data",
    "strategies",
    "services",
    "templates",
    "config",
    "utils",
    "models",
    "indicators",
    "logs",
]

OPTIONAL_FILES = [
    "profitability_upgrade.py",
    ".env",
    "paper_trades.json",
]

JUNK_FILES = [
    "god_trading_system.py",
    "force_patch.py",
    "windows_complete_patch.py",
    "windows_patch.py",
    "test_god_mode.py",
    "test_god_windows.py",
    "test_simple.py",
    "test_quick.py",
    "test_patches.py",
]

for f in REQUIRED_FILES:
    if Path(f).exists():
        ok(f)
    else:
        fail(f"{f}  — FILE MISSING")

print()
for folder in REQUIRED_FOLDERS:
    if Path(folder).is_dir():
        ok(f"{folder}/")
    else:
        fail(f"{folder}/  — FOLDER MISSING")

print()
info("Optional files:")
for f in OPTIONAL_FILES:
    status = "found" if Path(f).exists() else "not found"
    info(f"  {f}: {status}")

print()
info("Junk files (should be deleted):")
junk_found = []
for f in JUNK_FILES:
    if Path(f).exists():
        junk_found.append(f)
        warn(f"  DELETE: {f}")
if not junk_found:
    ok("All junk files removed")


# ══════════════════════════════════════════════════════
# 5. .ENV FILE & API KEYS - UPDATED to read your actual .env
# ══════════════════════════════════════════════════════
section("5. .ENV FILE & API KEYS")

env_path = Path(".env")
if not env_path.exists():
    warn(".env file not found — API-based features won't work")
    info("Create .env with your API keys")
else:
    ok(".env file found")
    try:
        from dotenv import dotenv_values
        env = dotenv_values(".env")
        
        # Complete list of your actual API keys from your .env
        keys_to_check = [
            ("ALPHA_VANTAGE_KEY",    "Alpha Vantage"),
            ("FINNHUB_KEY",          "Finnhub"),
            ("TWELVEDATA_KEY",       "Twelve Data"),
            ("ITICK_TOKEN",          "iTick API"),
            ("OILPRICE_API_KEY",     "OilPrice API"),
            ("NEWSAPI_KEY",          "NewsAPI"),
            ("GNEWS_KEY",            "GNews"),
            ("RAPIDAPI_KEY",         "RapidAPI"),
            ("WHALE_ALERT_KEY",      "Whale Alert"),
            ("TELEGRAM_TOKEN",       "Telegram Bot"),
            ("WHALE_TELEGRAM_TOKEN", "Whale Telegram"),
            ("TELEGRAM_CHAT_ID",     "Telegram Chat"),
            ("EMAIL_USERNAME",       "Email"),
            ("EMAIL_PASSWORD",       "Email Password"),
            ("DATABASE_URL",         "PostgreSQL database"),
            ("TWITTER_BEARER_TOKEN", "Twitter Bearer"),
            ("TWITTER_API_KEY",      "Twitter API Key"),
            ("TWITTER_API_SECRET",   "Twitter API Secret"),
            ("TWITTER_ACCESS_TOKEN", "Twitter Access Token"),
            ("TWITTER_ACCESS_SECRET","Twitter Access Secret"),
            ("APIFY_TOKEN",          "Apify"),
        ]
        
        configured = 0
        total = 0
        placeholder_count = 0
        
        for key, label in keys_to_check:
            val = env.get(key, "")
            total += 1
            if val and len(val) > 5 and "your_" not in val.lower() and "key_here" not in val.lower():
                ok(f"{label}: configured")
                configured += 1
            elif val and len(val) > 0:
                warn(f"{label}: has placeholder value")
                placeholder_count += 1
            else:
                warn(f"{label}: NOT set")
        
        print()
        info(f"API Keys configured: {configured}/{total}")
        if placeholder_count > 0:
            info(f"Placeholder keys: {placeholder_count} (need real values)")
        
        # Check database URL specifically
        db_url = env.get("DATABASE_URL", "")
        if db_url and "@" in db_url:
            # Mask password for display
            parts = db_url.split("@")
            credentials = parts[0].split("://")[1] if "://" in parts[0] else ""
            if ":" in credentials:
                user = credentials.split(":")[0]
                info(f"Database: connected as {user}")
        
    except Exception as e:
        warn(f"Could not read .env: {e}")


# ══════════════════════════════════════════════════════
# 6. LIVE DATA FETCH (YFINANCE)
# ══════════════════════════════════════════════════════
section("6. LIVE DATA FETCH (yfinance)")

try:
    import yfinance as yf
    import pandas as pd

    info("Fetching BTC-USD (5 bars)...")
    ticker = yf.Ticker("BTC-USD")
    df = ticker.history(period="5d", interval="1d")
    if not df.empty:
        price = df["Close"].iloc[-1]
        ok(f"BTC-USD: ${price:,.2f}  ({len(df)} bars fetched)")
    else:
        fail("BTC-USD: empty dataframe returned")

    info("Fetching EUR/USD (5 bars)...")
    df2 = yf.Ticker("EURUSD=X").history(period="5d", interval="1d")
    if not df2.empty:
        ok(f"EUR/USD: {df2['Close'].iloc[-1]:.5f}")
    else:
        warn("EUR/USD: no data (market may be closed or ticker changed)")

except Exception as e:
    fail(f"yfinance data fetch failed: {e}")
    info("Check your internet connection and try: pip install --upgrade yfinance")


# ══════════════════════════════════════════════════════
# 7. INTERNAL MODULE IMPORTS
# ══════════════════════════════════════════════════════
section("7. INTERNAL MODULE IMPORTS")

INTERNAL_MODULES = [
    ("data.fetcher",                "NASALevelFetcher, MarketHours"),
    ("strategies.voting_engine",    "StrategyVotingEngine"),
    ("services.database_service",   "DatabaseService"),
    ("config.config",               "config"),
    ("indicators.technical",        "TechnicalIndicators"),
    ("utils.trading_signals",       "TradingSignalGenerator"),
    ("paper_trader",                "PaperTrader"),
    ("advanced_predictor",          "AdvancedPredictionEngine"),
    ("advanced_risk_manager",       "AdvancedRiskManager"),
    ("advanced_backtester",         "AdvancedBacktester"),
    ("sentiment_analyzer",          "SentimentAnalyzer"),
    ("portfolio_optimizer",         "PortfolioOptimizer"),
    ("market_regime_analyzer",      "MarketRegimeDetector"),
    ("monitor",                     "TradingMonitor"),
    ("training_monitor",            "TrainingMonitor"),
    ("advanced_ai",                 "AdvancedAIIntegration"),
    ("auto_train_intelligent",      "IntelligentAutoTrainer"),
    ("logger",                      "TradingLogger"),
]

for module, classes in INTERNAL_MODULES:
    try:
        importlib.import_module(module)
        ok(f"{module}")
    except ImportError as e:
        err = str(e).split("No module named ")[-1].strip("'")
        if err == module:
            fail(f"{module}  — FILE MISSING")
        else:
            warn(f"{module}  — missing dependency: {err}")
    except Exception as e:
        warn(f"{module}  — error on import: {str(e)[:60]}")


# ══════════════════════════════════════════════════════
# 8. PAPER TRADES FILE
# ══════════════════════════════════════════════════════
section("8. PAPER TRADES FILE")

trades_path = Path("paper_trades.json")
if trades_path.exists():
    try:
        with open(trades_path) as f:
            data = json.load(f)
        open_pos = data.get("open_positions", [])
        closed   = data.get("closed_positions", [])
        balance  = data.get("account_balance", data.get("balance", "?"))
        total_pnl = sum(t.get("pnl", 0) for t in closed)
        wins      = sum(1 for t in closed if t.get("pnl", 0) > 0)

        ok(f"paper_trades.json readable")
        info(f"  Balance       : ${balance}")
        info(f"  Open positions: {len(open_pos)}")
        info(f"  Closed trades : {len(closed)}")
        if closed:
            wr = wins / len(closed) * 100
            info(f"  Win rate      : {wins}/{len(closed)} = {wr:.0f}%")
            info(f"  Total PnL     : ${total_pnl:.4f}")

        # Check for the stuck-open-forever problem
        now = datetime.now()
        stale = 0
        for t in open_pos:
            try:
                from datetime import datetime as dt
                entry = dt.fromisoformat(t.get("entry_time", ""))
                age_h = (now - entry).total_seconds() / 3600
                if age_h > 4:
                    stale += 1
            except Exception:
                pass
        if stale > 0:
            warn(f"{stale} position(s) open for >4 hours — profitability_upgrade.py will fix this")
        
        # Check for missing take_profit_levels
        no_tp = sum(1 for t in open_pos if not t.get("take_profit_levels"))
        if no_tp:
            warn(f"{no_tp} open position(s) have no take-profit levels — apply profitability_upgrade.py")

    except Exception as e:
        fail(f"paper_trades.json unreadable: {e}")
else:
    warn("paper_trades.json not found — will be created on first trade")


# ══════════════════════════════════════════════════════
# 9. DATABASE CONNECTION
# ══════════════════════════════════════════════════════
section("9. DATABASE CONNECTION")

try:
    from services.database_service import DatabaseService
    from sqlalchemy import text
    db = DatabaseService()
    count = db.session.execute(text("SELECT COUNT(*) FROM trades")).scalar()
    ok(f"PostgreSQL connected — {count} trades in database")
    db.close()
except ImportError:
    warn("DatabaseService not importable — check services/database_service.py")
except Exception as e:
    err_str = str(e).lower()
    if "no such table" in err_str or "does not exist" in err_str:
        warn("Database connected but tables missing — run: python init_db.py")
    elif "connection refused" in err_str or "could not connect" in err_str:
        warn("Database not reachable — PostgreSQL may not be running")
        info("Bot still works without DB — trades save to paper_trades.json")
    elif "no module named" in err_str:
        warn(f"Missing DB driver: {e}")
    else:
        warn(f"DB check skipped: {str(e)[:80]}")
        info("Bot still works without DB — trades save to paper_trades.json")


# ══════════════════════════════════════════════════════
# 10. ML MODELS - UPDATED to check both folders
# ══════════════════════════════════════════════════════
section("10. ML MODELS")

model_folders = ["trained_models", "ml_models"]
models_found = False
total_models = 0

for folder in model_folders:
    models_dir = Path(folder)
    if models_dir.exists():
        models = list(models_dir.glob("*.pkl"))
        if models:
            models_found = True
            total_models += len(models)
            ok(f"{len(models)} trained model(s) found in {folder}/")
            for m in sorted(models)[:3]:  # Show first 3
                size_kb = m.stat().st_size // 1024
                mtime = datetime.fromtimestamp(m.stat().st_mtime).strftime("%Y-%m-%d")
                info(f"  {m.name}  ({size_kb} KB, trained {mtime})")
            if len(models) > 3:
                info(f"  ... and {len(models)-3} more")
        else:
            if folder == "trained_models":
                info(f"No .pkl models in {folder}/ (checking ml_models/)")
    else:
        if folder == "trained_models":
            info(f"{folder}/ folder not found (checking ml_models/)")

if models_found:
    ok(f"Total models across all folders: {total_models}")
    
    # Check model registry
    registry_path = Path("model_registry.json")
    if registry_path.exists():
        try:
            with open(registry_path) as f:
                registry = json.load(f)
            info(f"Model registry contains {len(registry)} registered models")
        except:
            warn("Model registry exists but couldn't be read")
else:
    warn("No .pkl models found in any folder — run training to create them")
    info("Bot still works without models — uses technical indicators only")


# ══════════════════════════════════════════════════════
# 11. PROFITABILITY UPGRADE INTEGRATION
# ══════════════════════════════════════════════════════
section("11. PROFITABILITY UPGRADE INTEGRATION")

upgrade_file = Path("profitability_upgrade.py")
trading_system = Path("trading_system.py")

if not upgrade_file.exists():
    fail("profitability_upgrade.py NOT FOUND — download from Claude conversation")
else:
    try:
        import profitability_upgrade as pu
        ok("profitability_upgrade.py imports OK")

        # Check trading_system.py integration
        if trading_system.exists():
            content = trading_system.read_text(errors="ignore")
            checks = {
                "imported in trading_system.py":  "from profitability_upgrade import" in content,
                "apply_upgrades() called":         "apply_upgrades(self)" in content,
                "enhance_signal() used":           "enhance_signal(" in content,
                "on_trade_closed() wired up":      "on_trade_closed(" in content,
            }
            all_integrated = True
            for label, passed in checks.items():
                if passed:
                    ok(label)
                else:
                    fail(f"NOT DONE: {label}")
                    all_integrated = False

            if not all_integrated:
                info("Run:  python profitability_upgrade.py  for integration guide")
    except Exception as e:
        warn(f"profitability_upgrade.py found but has error: {e}")


# ══════════════════════════════════════════════════════
# 12. GOD MODE CLEANUP
# ══════════════════════════════════════════════════════
section("12. GOD MODE CLEANUP")

JUNK_CHECK = [
    "god_trading_system.py",
    "force_patch.py",
    "windows_complete_patch.py",
    "windows_patch.py",
    "test_god_mode.py",
    "test_god_windows.py",
    "test_simple.py",
    "test_quick.py",
    "test_patches.py",
]

junk_remaining = [f for f in JUNK_CHECK if Path(f).exists()]
if junk_remaining:
    for f in junk_remaining:
        fail(f"DELETE THIS FILE: {f}")
    info("These files waste space and can cause confusing import errors")
else:
    ok("All junk files removed")

if trading_system.exists():
    content = trading_system.read_text(errors="ignore")
    if "god_trading_system" in content or "OmegaOmniscient" in content:
        fail("trading_system.py still imports god_trading_system — remove that block")
        info("Delete lines ~35-46 in trading_system.py (the god mode try/except block)")
    else:
        ok("trading_system.py — god mode references removed")
    if "'god_mode'" in content or "god_mode_strategy" in content:
        fail("trading_system.py still has god_mode_strategy in strategies dict")
    else:
        ok("trading_system.py — god_mode_strategy removed from strategies")


# ══════════════════════════════════════════════════════
# 13. PORT AVAILABILITY
# ══════════════════════════════════════════════════════
section("13. PORT AVAILABILITY")

PORTS = {
    5000: "Flask dashboard (web_app_live.py)",
    8050: "Dash dashboard (performance_dashboard.py)",
}

for port, label in PORTS.items():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(1)
    result = sock.connect_ex(("127.0.0.1", port))
    sock.close()
    if result == 0:
        warn(f"Port {port} ALREADY IN USE — {label} may already be running")
    else:
        ok(f"Port {port} is free — ready for {label}")


# ══════════════════════════════════════════════════════
# 14. OVERALL VERDICT
# ══════════════════════════════════════════════════════
section("14. OVERALL VERDICT")

total   = len(results)
passes  = sum(1 for s, _ in results if s == "OK")
fails   = sum(1 for s, _ in results if s == "FAIL")
warns   = sum(1 for s, _ in results if s == "WARN")

print(f"\n  Passed : {passes}/{total}")
print(f"  Warnings: {warns}")
print(f"  Failed  : {fails}")
print()

if fails == 0 and warns <= 3:
    print("  SYSTEM READY — run:  python run.py")
elif fails == 0:
    print("  MOSTLY READY — fix warnings above, then run:  python run.py")
    print("  The bot will still work, but some features may be limited.")
else:
    print("  ACTION REQUIRED — fix the FAIL items above before running.")
    print()
    failed_items = [msg for s, msg in results if s == "FAIL"]
    print("  Critical fixes needed:")
    for item in failed_items:
        print(f"    - {item}")

print()
print(f"  Diagnostic completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("─" * 55)