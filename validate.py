#!/usr/bin/env python3
"""
validate.py — Full pre-flight validation for the forex/crypto trading bot.

Run:  python validate.py
Pass: All checks show [OK] and exits with code 0
Fail: Any [FAIL] exits with code 1 — fix before starting the bot
"""
import importlib
import os
import socket
import sys
from pathlib import Path
from urllib.parse import urlparse

# ── Terminal colours ──────────────────────────────────────────────────────────
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"
RESET  = "\033[0m"

OK   = f"{GREEN}[OK]{RESET}  "
FAIL = f"{RED}[FAIL]{RESET}"
INFO = f"{YELLOW}[INFO]{RESET}"
SKIP = f"{CYAN}[SKIP]{RESET}"

_results: list[bool] = []


def check(condition: bool, message: str, fatal: bool = False) -> bool:
    mark = OK if condition else FAIL
    print(f"  {mark} {message}")
    _results.append(condition)
    return condition


def info(message: str) -> None:
    print(f"  {INFO} {message}")


def skip(message: str) -> None:
    print(f"  {SKIP} {message}")


def section(title: str) -> None:
    print(f"\n{BOLD}{CYAN}{title}{RESET}")
    print("  " + "─" * 66)


# ─────────────────────────────────────────────────────────────────────────────
# 1. ENVIRONMENT FILE
# ─────────────────────────────────────────────────────────────────────────────
section("1. ENVIRONMENT FILE")

env_path = Path(".env")
if not check(env_path.exists(), ".env file exists"):
    print(f"\n{RED}  Cannot continue without .env — aborting.{RESET}")
    sys.exit(1)

env_text = env_path.read_text(encoding="utf-8")
env_lower = env_text.lower()

# Load env into os.environ for later checks
from dotenv import load_dotenv
load_dotenv(env_path, override=False)


# ─────────────────────────────────────────────────────────────────────────────
# 2. CORE RUNTIME SETTINGS
# ─────────────────────────────────────────────────────────────────────────────
section("2. CORE RUNTIME SETTINGS")

check("TRADING_TIMEFRAME=15m"             in env_text,  "TRADING_TIMEFRAME=15m")
check("PLAYBOOK_ONLY_RUNTIME=" in env_text and
      os.getenv("PLAYBOOK_ONLY_RUNTIME", "").strip().lower() == "true",
      "PLAYBOOK_ONLY_RUNTIME=true")
check("TZ_OFFSET_HOURS=3"                 in env_text,  "TZ_OFFSET_HOURS=3 (EAT/Nairobi)")
check("SCAN_INTERVAL_SECONDS="            in env_text,  "SCAN_INTERVAL_SECONDS defined")
check("TIMEFRAMES=1m,5m,15m,30m,1h,4h,1d" in env_text, "TIMEFRAMES include 30m and 4h")
check("LOG_LEVEL="                        in env_text,  "LOG_LEVEL defined")


# ─────────────────────────────────────────────────────────────────────────────
# 3. RISK & POSITION SIZING
# ─────────────────────────────────────────────────────────────────────────────
section("3. RISK & POSITION SIZING")

check("DAILY_LOSS_LIMIT_PERCENT=35.0"   in env_text,  "DAILY_LOSS_LIMIT_PERCENT=35.0")
check("DRAWDOWN_HALT_PERCENT=40.0"      in env_text,  "DRAWDOWN_HALT_PERCENT=40.0")
check("DEFAULT_RISK_PER_TRADE="         in env_text,  "DEFAULT_RISK_PER_TRADE defined")
check("MAX_RISK_PER_TRADE="             in env_text,  "MAX_RISK_PER_TRADE defined")
check("DEFAULT_BALANCE="                in env_text,  "DEFAULT_BALANCE defined")

# Risk values sanity
try:
    daily_loss = float(os.getenv("DAILY_LOSS_LIMIT_PERCENT", "0"))
    drawdown   = float(os.getenv("DRAWDOWN_HALT_PERCENT", "0"))
    risk_trade = float(os.getenv("DEFAULT_RISK_PER_TRADE", "0"))
    max_risk   = float(os.getenv("MAX_RISK_PER_TRADE", "0"))
    check(0 < daily_loss <= 50,    f"DAILY_LOSS_LIMIT_PERCENT in range (actual: {daily_loss}%)")
    check(drawdown > daily_loss,   f"DRAWDOWN_HALT > DAILY_LOSS ({drawdown}% > {daily_loss}%)")
    check(0 < risk_trade <= 5,     f"DEFAULT_RISK_PER_TRADE sane (actual: {risk_trade}%)")
    check(max_risk >= risk_trade,  f"MAX_RISK_PER_TRADE >= DEFAULT ({max_risk}% >= {risk_trade}%)")
except Exception as e:
    check(False, f"Risk value parsing failed: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# 4. BROKER CREDENTIALS
# ─────────────────────────────────────────────────────────────────────────────
section("4. BROKER CREDENTIALS")

# Deriv (primary)
deriv_enabled = os.getenv("DERIV_ENABLED", "false").lower() == "true"
check(deriv_enabled,                              "DERIV_ENABLED=true")
check(bool(os.getenv("DERIV_APP_ID", "").strip()), "DERIV_APP_ID present")
check(bool(os.getenv("DERIV_TOKEN", "").strip()),  "DERIV_TOKEN present")
check("DERIV_SYMBOL_MAP="  in env_text,            "DERIV_SYMBOL_MAP defined")

# IG Markets (commodity / index routing)
ig_enabled = os.getenv("IG_ENABLED", "false").lower() == "true"
if ig_enabled:
    check(bool(os.getenv("IG_API_KEY", "").strip()),      "IG_API_KEY present")
    check(bool(os.getenv("IG_IDENTIFIER", "").strip()),   "IG_IDENTIFIER present")
    check(bool(os.getenv("IG_PASSWORD", "").strip()),     "IG_PASSWORD present")
    check(bool(os.getenv("IG_ACCOUNT_ID", "").strip()),   "IG_ACCOUNT_ID present")
    ig_env = os.getenv("IG_ENVIRONMENT", "demo")
    info(f"IG_ENVIRONMENT={ig_env}")
else:
    skip("IG broker disabled (IG_ENABLED != true)")

# Binance (public data)
check("BINANCE_PUBLIC_DATA_ENABLED=true" in env_text, "BINANCE_PUBLIC_DATA_ENABLED=true")

# FMP history bridge
fmp_enabled = os.getenv("FMP_HISTORY_ENABLED", "false").lower() == "true"
if fmp_enabled:
    check(bool(os.getenv("FMP_API_KEY", "").strip()), "FMP_API_KEY present")
else:
    skip("FMP history bridge disabled")

# Dukascopy live depth (optional)
duka_live = os.getenv("DUKASCOPY_LIVE_DEPTH_ENABLED", "false").lower() == "true"
if duka_live:
    check(bool(os.getenv("DUKASCOPY_LIVE_DEPTH_USERNAME", "").strip()), "DUKASCOPY_LIVE_DEPTH_USERNAME present")
    check(bool(os.getenv("DUKASCOPY_LIVE_DEPTH_PASSWORD", "").strip()), "DUKASCOPY_LIVE_DEPTH_PASSWORD present")
else:
    skip("Dukascopy live depth disabled")

# cTrader live depth (optional)
ctrader_live = os.getenv("CTRADER_LIVE_DEPTH_ENABLED", "false").lower() == "true"
if ctrader_live:
    check(bool(os.getenv("CTRADER_LIVE_DEPTH_CLIENT_ID", "").strip()),     "CTRADER_LIVE_DEPTH_CLIENT_ID present")
    check(bool(os.getenv("CTRADER_LIVE_DEPTH_CLIENT_SECRET", "").strip()), "CTRADER_LIVE_DEPTH_CLIENT_SECRET present")
else:
    skip("cTrader live depth disabled")


# ─────────────────────────────────────────────────────────────────────────────
# 5. TELEGRAM BOTS
# ─────────────────────────────────────────────────────────────────────────────
section("5. TELEGRAM BOTS")

check(bool(os.getenv("COMMAND_BOT_TOKEN", "").strip()),   "COMMAND_BOT_TOKEN present")
check(bool(os.getenv("COMMAND_BOT_CHAT_ID", "").strip()), "COMMAND_BOT_CHAT_ID present")

deepseek_token = os.getenv("DEEPSEEK_TELEGRAM_TOKEN", "").strip()
if deepseek_token:
    check(bool(os.getenv("DEEPSEEK_TELEGRAM_CHAT_ID", "").strip()), "DEEPSEEK_TELEGRAM_CHAT_ID present")
    info("DeepSeek Telegram bot configured")
else:
    skip("DEEPSEEK_TELEGRAM_TOKEN not set — DeepSeek bot won't start")

whale_token = os.getenv("WHALE_TELEGRAM_TOKEN", "").strip()
if whale_token:
    info("Whale watcher Telegram token configured")
else:
    skip("WHALE_TELEGRAM_TOKEN not set — whale watcher Telegram silent")


# ─────────────────────────────────────────────────────────────────────────────
# 6. DATABASE & REDIS
# ─────────────────────────────────────────────────────────────────────────────
section("6. DATABASE & REDIS")

# Database URL
db_url = os.getenv("DATABASE_URL", "")
check(bool(db_url), "DATABASE_URL present")
if db_url:
    try:
        parsed = urlparse(db_url)
        check(parsed.scheme in ("postgresql", "postgres", "postgresql+psycopg2"),
              f"DATABASE_URL scheme valid (actual: {parsed.scheme})")
        check(bool(parsed.hostname), f"DATABASE_URL has host (actual: {parsed.hostname})")

        # Connectivity test
        host = parsed.hostname
        port = parsed.port or 5432
        try:
            sock = socket.create_connection((host, port), timeout=5)
            sock.close()
            check(True, f"PostgreSQL reachable at {host}:{port}")
        except Exception as conn_err:
            check(False, f"PostgreSQL unreachable at {host}:{port} — {conn_err}")
    except Exception as e:
        check(False, f"DATABASE_URL parse error: {e}")

# Redis URL
redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
check(bool(redis_url), f"REDIS_URL present ({redis_url[:40]}...)" if len(redis_url) > 40 else f"REDIS_URL present ({redis_url})")
try:
    import redis as redis_lib
    r = redis_lib.from_url(redis_url, socket_connect_timeout=5)
    r.ping()
    check(True, "Redis PING successful")
    r.close()
except ImportError:
    check(False, "redis package not installed")
except Exception as redis_err:
    check(False, f"Redis unreachable — {redis_err}")

# Local DB file (fallback state)
db_file = Path("trading_data.db")
info(f"SQLite state file: {'exists' if db_file.exists() else 'will be created on startup'}")


# ─────────────────────────────────────────────────────────────────────────────
# 7. MARKET DATA & CACHE SETTINGS
# ─────────────────────────────────────────────────────────────────────────────
section("7. MARKET DATA & CACHE SETTINGS")

check("MARKET_DATA_QUOTE_CACHE_TTL=5"  in env_text, "MARKET_DATA_QUOTE_CACHE_TTL=5s")
check("MARKET_DATA_OHLCV_CACHE_TTL=60" in env_text, "MARKET_DATA_OHLCV_CACHE_TTL=60s")
check("DUKASCOPY_HISTORY_ENABLED="      in env_text, "DUKASCOPY_HISTORY_ENABLED defined")
check("SENTIMENT_MAX_AGE_HOURS="        in env_text, "SENTIMENT_MAX_AGE_HOURS defined")


# ─────────────────────────────────────────────────────────────────────────────
# 8. GOVERNANCE & SIGNAL FILTERS
# ─────────────────────────────────────────────────────────────────────────────
section("8. GOVERNANCE & SIGNAL FILTERS")

check("GOVERNANCE_MIN_LIVE_ACCURACY="    in env_text, "GOVERNANCE_MIN_LIVE_ACCURACY defined")
check("GOVERNANCE_MIN_RISK_REWARD="      in env_text, "GOVERNANCE_MIN_RISK_REWARD defined")
check("GOVERNANCE_MIN_REAL_SOURCES="     in env_text, "GOVERNANCE_MIN_REAL_SOURCES defined")
check(os.getenv("GOVERNANCE_ENABLE_FOREX_FILTER", "true").strip().lower() == "true",
      "GOVERNANCE_ENABLE_FOREX_FILTER=true (default or explicit)")
check("FOREX_FILTER_MIN_CONFIDENCE="     in env_text, "FOREX_FILTER_MIN_CONFIDENCE defined")
check("FOREX_FILTER_MAX_SPREAD_BPS="     in env_text, "FOREX_FILTER_MAX_SPREAD_BPS defined")

try:
    rr = float(os.getenv("GOVERNANCE_MIN_RISK_REWARD", "0"))
    check(rr >= 1.5, f"GOVERNANCE_MIN_RISK_REWARD >= 1.5 (actual: {rr})")
    acc = float(os.getenv("GOVERNANCE_MIN_LIVE_ACCURACY", "0"))
    check(acc >= 50.0, f"GOVERNANCE_MIN_LIVE_ACCURACY >= 50% (actual: {acc}%)")
except Exception as e:
    check(False, f"Governance value parse error: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# 9. NEWS EVENT BLOCKING
# ─────────────────────────────────────────────────────────────────────────────
section("9. NEWS EVENT BLOCKING")

try:
    from data_ingestion.news_event_monitor import PRE_EVENT_MINS, ACTIVE_MINS, POST_EVENT_MINS
    check(PRE_EVENT_MINS  == 10, f"PRE_EVENT_MINS=10  (actual: {PRE_EVENT_MINS})")
    check(ACTIVE_MINS     == 10, f"ACTIVE_MINS=10     (actual: {ACTIVE_MINS})")
    check(POST_EVENT_MINS == 45, f"POST_EVENT_MINS=45 (actual: {POST_EVENT_MINS})")
except ImportError as e:
    check(False, f"news_event_monitor import failed: {e}")
except AttributeError as e:
    check(False, f"news_event_monitor missing constant: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# 10. ASSET CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────
section("10. ASSET CONFIGURATION")

try:
    from config.config import ASSET_CATEGORIES
    crypto      = len(ASSET_CATEGORIES.get("crypto", []))
    forex       = len(ASSET_CATEGORIES.get("forex", []))
    commodities = len(ASSET_CATEGORIES.get("commodities", []))
    indices     = len(ASSET_CATEGORIES.get("indices", []))
    total       = crypto + forex + commodities + indices

    check(crypto      >= 5, f"Crypto assets: {crypto}  (min 5)")
    check(forex       >= 7, f"Forex pairs:   {forex}  (min 7)")
    check(commodities >= 2, f"Commodities:   {commodities}  (min 2)")
    check(indices     >= 4, f"Indices:       {indices}  (min 4)")
    info(f"Total active assets: {total}")
except Exception as e:
    check(False, f"ASSET_CATEGORIES load failed: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# 11. CORE MODULE IMPORTS
# ─────────────────────────────────────────────────────────────────────────────
section("11. CORE MODULE IMPORTS")

core_modules = [
    ("config.config",                    "config.config"),
    ("config.optimization",              "config.optimization"),
    ("core.engine",                      "core.engine"),
    ("core.signal",                      "core.signal"),
    ("core.decision_engine",             "core.decision_engine"),
    ("core.confidence",                  "core.confidence"),
    ("core.state",                       "core.state"),
    ("risk.manager",                     "risk.manager"),
    ("risk.position_sizer",              "risk.position_sizer"),
    ("risk.portfolio_risk",              "risk.portfolio_risk"),
    ("risk.forex_filter",                "risk.forex_filter"),
    ("execution.exchange_router",        "execution.exchange_router"),
    ("execution.paper_trader",           "execution.paper_trader"),
    ("services.playbook_service",        "services.playbook_service"),
    ("services.market_data_router",      "services.market_data_router"),
    ("services.signal_governance",       "services.signal_governance"),
    ("services.opportunity_ranker",      "services.opportunity_ranker"),
    ("services.redis_cache",             "services.redis_cache"),
    ("indicators.technical",             "indicators.technical"),
    ("models.trade_models",              "models.trade_models"),
    ("monitoring.metrics",               "monitoring.metrics"),
]

for label, module in core_modules:
    try:
        importlib.import_module(module)
        check(True, label)
    except Exception as e:
        check(False, f"{label} — {e}")


# ─────────────────────────────────────────────────────────────────────────────
# 12. THIRD-PARTY PACKAGE AVAILABILITY
# ─────────────────────────────────────────────────────────────────────────────
section("12. THIRD-PARTY PACKAGES")

packages = [
    ("pandas",           "pandas"),
    ("numpy",            "numpy"),
    ("sklearn",          "scikit-learn"),
    ("xgboost",          "xgboost"),
    ("flask",            "flask"),
    ("flask_socketio",   "flask-socketio"),
    ("hypercorn",        "hypercorn"),
    ("redis",            "redis"),
    ("sqlalchemy",       "sqlalchemy"),
    ("telegram",         "python-telegram-bot"),
    ("telethon",         "telethon"),
    ("textblob",         "textblob"),
    ("feedparser",       "feedparser"),
    ("bs4",              "beautifulsoup4"),
    ("psutil",           "psutil"),
    ("orjson",           "orjson"),
    ("schedule",         "schedule"),
    ("tabulate",         "tabulate"),
]

for import_name, pkg_label in packages:
    try:
        importlib.import_module(import_name)
        check(True, pkg_label)
    except ImportError:
        check(False, f"{pkg_label} NOT installed — pip install {pkg_label}")


# ─────────────────────────────────────────────────────────────────────────────
# 13. PLAYBOOK RUNTIME INTEGRITY
# ─────────────────────────────────────────────────────────────────────────────
section("13. PLAYBOOK RUNTIME INTEGRITY")

try:
    engine_text = Path("core/engine.py").read_text(encoding="utf-8")
    check("PLAYBOOK_ONLY_RUNTIME"        in engine_text, "engine.py uses PLAYBOOK_ONLY_RUNTIME flag")
    check("playbook_"                    in engine_text, "engine.py seeds playbook-driven setups")
except Exception as e:
    check(False, f"engine.py read failed: {e}")

try:
    bot_text = Path("bot.py").read_text(encoding="utf-8")
    check("ML prediction service removed" in bot_text,   "bot.py: ML prediction service removed")
    check("DEEPSEEK_TELEGRAM_TOKEN"       in bot_text,   "bot.py: DeepSeek sibling bot wired")
except Exception as e:
    check(False, f"bot.py read failed: {e}")

try:
    dash_text = Path("dashboard/web_app_live.py").read_text(encoding="utf-8")
    check("strategy-lab"     not in dash_text.lower(),   "Dashboard: legacy Strategy Lab removed")
    check("/api/status"          in dash_text,           "Dashboard: /api/status route present")
    check("/api/command-center"  in dash_text,           "Dashboard: /api/command-center route present")
    check("/api/signals/live"    in dash_text,           "Dashboard: /api/signals/live route present")
    check("/api/live-book"       in dash_text,           "Dashboard: /api/live-book route present")
except Exception as e:
    check(False, f"dashboard/web_app_live.py read failed: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# 14. CRITICAL FILES & DIRECTORY STRUCTURE
# ─────────────────────────────────────────────────────────────────────────────
section("14. CRITICAL FILES & DIRECTORIES")

critical_files = [
    "bot.py",
    "deepseek_bot.py",
    "intelligence_bot.py",
    "telegram_commander.py",
    "config/config.py",
    "config/optimization.py",
    "config/database.py",
    "core/engine.py",
    "core/decision_engine.py",
    "risk/manager.py",
    "risk/forex_filter.py",
    "risk/position_sizer.py",
    "execution/exchange_router.py",
    "execution/paper_trader.py",
    "services/playbook_service.py",
    "services/market_data_router.py",
    "services/redis_pool.py",
    "dashboard/web_app_live.py",
    "indicators/technical.py",
    "models/trade_models.py",
    "monitoring/system_health_service.py",
    "static/dashboard_auth.js",
    "static/service-worker.js",
    "requirements.txt",
    ".env",
]

for f in critical_files:
    check(Path(f).exists(), f)

critical_dirs = [
    "templates", "services", "core", "risk", "execution",
    "data_ingestion", "indicators", "monitoring", "order_flow",
    "strategies", "ml", "models", "static", "logs",
]

for d in critical_dirs:
    p = Path(d)
    exists = p.is_dir()
    if d == "logs" and not exists:
        info("logs/ will be created on first run")
    else:
        check(exists, f"{d}/")


# ─────────────────────────────────────────────────────────────────────────────
# 15. DEPLOY ASSETS (Oracle Cloud)
# ─────────────────────────────────────────────────────────────────────────────
section("15. ORACLE CLOUD DEPLOY ASSETS")

deploy_files = [
    "deploy/oraclecloud/forex-bot.service",
    "deploy/oraclecloud/deepseek-bot.service",
    "deploy/oraclecloud/install.sh",
    "deploy/oraclecloud/nginx-forex-bot.conf",
    "deploy/oraclecloud/preflight.sh",
    "deploy/oraclecloud/env.production.example",
    "deploy/oraclecloud/ENV_CHECKLIST.md",
]

for f in deploy_files:
    check(Path(f).exists(), f)


# ─────────────────────────────────────────────────────────────────────────────
# SUMMARY
# ─────────────────────────────────────────────────────────────────────────────
total   = len(_results)
passed  = sum(_results)
failed  = total - passed

print("\n" + "=" * 70)
if failed == 0:
    print(f"{GREEN}{BOLD}  ALL {total} CHECKS PASSED{RESET}")
    print("""
  Ready to launch. Suggested startup order:
    1.  python validate.py                  ← you're here
    2.  python bot.py --no-telegram         ← smoke test (paper mode)
    3.  python bot.py                       ← full run with Telegram
    4.  Monitor logs/ and Redis for 3 days before going live
""")
    sys.exit(0)
else:
    print(f"{RED}{BOLD}  {failed} of {total} CHECKS FAILED{RESET}")
    print("""
  Fix every [FAIL] above before starting the bot.
  Re-run:  python validate.py
""")
    sys.exit(1)
