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
        all_ok &= check("BINANCE_PUBLIC_DATA_ENABLED=true" in env_content, "BINANCE_PUBLIC_DATA_ENABLED=true configured")
        all_ok &= check("DAILY_LOSS_LIMIT_PERCENT=35.0" in env_content, "DAILY_LOSS_LIMIT_PERCENT=35.0 configured")
        all_ok &= check("DRAWDOWN_HALT_PERCENT=40.0" in env_content, "DRAWDOWN_HALT_PERCENT=40.0 configured")
        all_ok &= check("MARKET_DATA_QUOTE_CACHE_TTL=5" in env_content, "MARKET_DATA_QUOTE_CACHE_TTL=5 configured")
        all_ok &= check("MARKET_DATA_OHLCV_CACHE_TTL=60" in env_content, "MARKET_DATA_OHLCV_CACHE_TTL=60 configured")
        all_ok &= check("TIMEFRAMES=1m,5m,15m,30m,1h,4h,1d" in env_content, "TIMEFRAMES include 30m and 4h")
        all_ok &= check("TZ_OFFSET_HOURS=3" in env_content, "TZ_OFFSET_HOURS=3 configured")
    
    # Check 2: Strategy parameters
    print("\n2. STRATEGY PARAMETERS")
    print("-" * 70)
    
    try:
        from strategies.rsi import RSIStrategy
        rsi = RSIStrategy()
        all_ok &= check(rsi.period == 8, f"RSI period=8 (fast response) [actual: {rsi.period}]")
        all_ok &= check(rsi.oversold == 28, f"RSI oversold=28 [actual: {rsi.oversold}]")
        all_ok &= check(rsi.overbought == 72, f"RSI overbought=72 [actual: {rsi.overbought}]")
    except Exception as e:
        all_ok &= check(False, f"RSI strategy load failed: {e}")
    
    try:
        from strategies.macd import MACDStrategy
        macd = MACDStrategy()
        all_ok &= check(macd.signal == 6, f"MACD signal=6 (fast crosses) [actual: {macd.signal}]")
        all_ok &= check(macd.fast == 12, f"MACD fast=12 [actual: {macd.fast}]")
    except Exception as e:
        all_ok &= check(False, f"MACD strategy load failed: {e}")
    
    try:
        from strategies.bollinger import BollingerStrategy
        bb = BollingerStrategy()
        all_ok &= check(bb.period == 20, f"BB period=20 (perfect for 15m) [actual: {bb.period}]")
    except Exception as e:
        all_ok &= check(False, f"Bollinger strategy load failed: {e}")
    
    # Check 3: Voting confidence
    print("\n3. ENSEMBLE VOTING")
    print("-" * 70)
    
    try:
        from strategies.voting import VotingStrategy
        voting = VotingStrategy()
        all_ok &= check(voting.min_confidence == 0.58, f"Voting min_confidence=0.58 [actual: {voting.min_confidence}]")
        all_ok &= check(voting.min_votes == 1, f"Voting accepts single strong signal [min_votes: {voting.min_votes}]")
    except Exception as e:
        all_ok &= check(False, f"Voting strategy load failed: {e}")
    
    # Check 4: News event blocking
    print("\n4. NEWS EVENT BLOCKING (15m-friendly)")
    print("-" * 70)
    
    try:
        from data_ingestion.news_event_monitor import PRE_EVENT_MINS, ACTIVE_MINS, POST_EVENT_MINS
        all_ok &= check(PRE_EVENT_MINS == 10, f"PRE_EVENT_MINS=10 (vs 60) allows trading [actual: {PRE_EVENT_MINS}]")
        all_ok &= check(ACTIVE_MINS == 10, f"ACTIVE_MINS=10 (fast market stabilization) [actual: {ACTIVE_MINS}]")
        all_ok &= check(POST_EVENT_MINS == 45, f"POST_EVENT_MINS=45 (vs 90) [actual: {POST_EVENT_MINS}]")
    except Exception as e:
        all_ok &= check(False, f"News event monitor load failed: {e}")
    
    # Check 5: Database and state
    print("\n5. CORE INFRASTRUCTURE")
    print("-" * 70)
    
    db_ok = Path("trading_data.db").exists() or Path("data/system_state.json").exists()
    print(f"{YELLOW}{INFO_MARK}{RESET} Database state: {'exists' if db_ok else 'will be created on startup'}")
    
    # Check 6: Optimal assets
    print("\n6. ASSET CONFIGURATION")
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
        
        print(f"{YELLOW}{INFO_MARK}{RESET} Total assets: {sum(assets.values())} (recommended: 18)")
    except Exception as e:
        all_ok &= check(False, f"Asset config load failed: {e}")
    
    # Check 7: Key files exist
    print("\n7. CRITICAL FILES")
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
        print("  2. Verify BINANCE_PUBLIC_DATA_ENABLED=true for BNB/SOL/XRP fallback")
        print("  3. Run: python bot.py --no-telegram")
        print("  4. Monitor paper trades for 3 days")
        print("  5. Review DEPLOYMENT_GUIDE.md for full guide")
        return 0
    else:
        print(f"{RED}{FAIL_MARK} SOME CHECKS FAILED!{RESET}")
        print("\nFix any failures above, then run this script again.")
        print("See DEPLOYMENT_GUIDE.md for troubleshooting.")
        return 1

if __name__ == "__main__":
    sys.exit(main())
