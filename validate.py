#!/usr/bin/env python3
"""
validate.py — Quick validation that current runtime settings are in place.

Run: python validate.py
Expected: All checks pass [OK]
"""
import sys
from pathlib import Path

# Color codes for terminal output
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
RESET = "\033[0m"
OK_MARK = "[OK]"
FAIL_MARK = "[FAIL]"
INFO_MARK = "[INFO]"

def check(condition, message):
    """Print check result."""
    if condition:
        print(f"{GREEN}{OK_MARK}{RESET} {message}")
        return True
    else:
        print(f"{RED}{FAIL_MARK}{RESET} {message}")
        return False

def main():
    print("\n" + "=" * 70)
    print("RUNTIME CONFIG VALIDATION")
    print("=" * 70 + "\n")
    
    all_ok = True
    
    # Check 1: .env configuration
    print("1. CONFIGURATION FILES")
    print("-" * 70)
    
    env_exists = Path(".env").exists()
    all_ok &= check(env_exists, ".env file exists")
    
    if env_exists:
        env_content = Path(".env").read_text()
        all_ok &= check("TRADING_TIMEFRAME=15m" in env_content, "TRADING_TIMEFRAME=15m configured")
        all_ok &= check("deriv_enabled=true" in env_content.lower(), "DERIV_ENABLED=true configured")
        all_ok &= check("DERIV_APP_ID=" in env_content, "DERIV_APP_ID present in .env")
        all_ok &= check("IG_API_KEY=" in env_content, "IG_API_KEY present in .env")
        all_ok &= check("BINANCE_PUBLIC_DATA_ENABLED=true" in env_content, "BINANCE_PUBLIC_DATA_ENABLED=true configured")
        all_ok &= check("DAILY_LOSS_LIMIT_PERCENT=35.0" in env_content, "DAILY_LOSS_LIMIT_PERCENT=35.0 configured")
        all_ok &= check("DRAWDOWN_HALT_PERCENT=40.0" in env_content, "DRAWDOWN_HALT_PERCENT=40.0 configured")
        all_ok &= check("MARKET_DATA_QUOTE_CACHE_TTL=5" in env_content, "MARKET_DATA_QUOTE_CACHE_TTL=5 configured")
        all_ok &= check("MARKET_DATA_OHLCV_CACHE_TTL=60" in env_content, "MARKET_DATA_OHLCV_CACHE_TTL=60 configured")
        all_ok &= check("TIMEFRAMES=1m,5m,15m,30m,1h,4h,1d" in env_content, "TIMEFRAMES include 30m and 4h")
        all_ok &= check("TZ_OFFSET_HOURS=3" in env_content, "TZ_OFFSET_HOURS=3 configured")
    
    # Check 2: Strategy runtime model
    print("\n2. STRATEGY RUNTIME")
    print("-" * 70)

    try:
        from strategy_lab.strategy_builder import StrategyBuilder

        active = StrategyBuilder.all_configs()
        archived = StrategyBuilder.archived_configs()
        all_ok &= check(len(active) == 9, f"Strategy Lab active bench trimmed to 9 presets [actual: {len(active)}]")
        all_ok &= check("golden_cross" not in active, "Golden Cross removed from active research bench")
        all_ok &= check("rsi_scalper" not in active, "RSI scalper removed from active research bench")
        all_ok &= check("stoch_trend" not in active, "Stochastic trend preset removed from active research bench")
        all_ok &= check(len(archived) == 6, f"Archived preset bench has 6 entries [actual: {len(archived)}]")
    except Exception as e:
        all_ok &= check(False, f"Strategy bench validation failed: {e}")

    try:
        engine_content = Path("core/engine.py").read_text(encoding="utf-8")
        all_ok &= check("strategy_id=\"policy_agent\"" in engine_content, "Live runtime remains policy_agent-based")
    except Exception as e:
        all_ok &= check(False, f"Policy agent runtime check failed: {e}")

    # Check 3: News event blocking
    print("\n3. NEWS EVENT BLOCKING (15m-friendly)")
    print("-" * 70)

    try:
        from data_ingestion.news_event_monitor import PRE_EVENT_MINS, ACTIVE_MINS, POST_EVENT_MINS
        all_ok &= check(PRE_EVENT_MINS == 10, f"PRE_EVENT_MINS=10 (vs 60) allows trading [actual: {PRE_EVENT_MINS}]")
        all_ok &= check(ACTIVE_MINS == 10, f"ACTIVE_MINS=10 (fast market stabilization) [actual: {ACTIVE_MINS}]")
        all_ok &= check(POST_EVENT_MINS == 45, f"POST_EVENT_MINS=45 (vs 90) [actual: {POST_EVENT_MINS}]")
    except Exception as e:
        all_ok &= check(False, f"News event monitor load failed: {e}")
    
    # Check 4: Database and state
    print("\n4. CORE INFRASTRUCTURE")
    print("-" * 70)
    
    db_ok = Path("trading_data.db").exists() or Path("data/system_state.json").exists()
    print(f"{YELLOW}{INFO_MARK}{RESET} Database state: {'exists' if db_ok else 'will be created on startup'}")
    
    # Check 5: Optimal assets
    print("\n5. ASSET CONFIGURATION")
    print("-" * 70)
    
    try:
        from config.config import ASSET_CATEGORIES
        assets = {
            "crypto": len(ASSET_CATEGORIES.get("crypto", [])),
            "forex": len(ASSET_CATEGORIES.get("forex", [])),
            "commodities": len(ASSET_CATEGORIES.get("commodities", [])),
            "indices": len(ASSET_CATEGORIES.get("indices", [])),
        }
        
        all_ok &= check(assets["crypto"] >= 5, f"Crypto assets: {assets['crypto']} (BTC/ETH/SOL/BNB/XRP)")
        all_ok &= check(assets["forex"] >= 7, f"Forex pairs: {assets['forex']}")
        all_ok &= check(assets["commodities"] >= 2, f"Commodities: {assets['commodities']}")
        all_ok &= check(assets["indices"] >= 4, f"Indices: {assets['indices']}")
        
        print(f"{YELLOW}{INFO_MARK}{RESET} Total assets: {sum(assets.values())} (expected: 19)")
    except Exception as e:
        all_ok &= check(False, f"Asset config load failed: {e}")
    
    # Check 6: Key files exist
    print("\n6. CRITICAL FILES")
    print("-" * 70)
    
    critical_files = [
        ("config/optimization.py", "Optimization config"),
        ("risk/forex_filter.py", "Forex-specific filters"),
        (".env", "Active env file"),
        ("DEPLOYMENT_GUIDE.md", "Deployment guide"),
    ]
    
    for filepath, description in critical_files:
        exists = Path(filepath).exists()
        all_ok &= check(exists, f"{description}: {filepath}")
    
    # Summary
    print("\n" + "=" * 70)
    if all_ok:
        print(f"{GREEN}{OK_MARK} ALL CHECKS PASSED!{RESET}")
        print("\nYou're ready to run the current market-data stack. Next steps:")
        print("  1. Verify DERIV_APP_ID / DERIV_SYMBOL_MAP in .env")
        print("  2. Verify IG_API_KEY / IG_IDENTIFIER / IG_PASSWORD for commodity routing")
        print("  3. Verify BINANCE_PUBLIC_DATA_ENABLED=true for BNB/SOL/XRP fallback")
        print("  4. Run: python bot.py --no-telegram")
        print("  5. Monitor paper trades for 3 days")
        return 0
    else:
        print(f"{RED}{FAIL_MARK} SOME CHECKS FAILED!{RESET}")
        print("\nFix any failures above, then run this script again.")
        print("See DEPLOYMENT_GUIDE.md for troubleshooting.")
        return 1

if __name__ == "__main__":
    sys.exit(main())
