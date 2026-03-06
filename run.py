#!/usr/bin/env python
"""
ULTIMATE TRADING BOT LAUNCHER v5.0
Complete interface for all bot features
Includes: Paper Trading, Dashboards, ML Training, Backtesting, Sentiment Analysis, Whale Alerts
"""

import sys
import subprocess
import time
import os
import json
from pathlib import Path
from datetime import datetime


# ─────────────────────────────────────────────
#  CONFIGURATION
# ─────────────────────────────────────────────
VERSION = "5.0"
BOT_NAME = "ULTIMATE TRADING BOT"
GITHUB_REPO = "https://github.com/yourusername/trading-bot"

# ─────────────────────────────────────────────
#  HELPERS
# ─────────────────────────────────────────────

def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')


def run(cmd: list):
    """Run a subprocess and wait for it to finish."""
    subprocess.run(cmd)


def pause():
    input("\n  Press Enter to return to menu...")


def ask(prompt: str, default: str = "") -> str:
    val = input(f"  {prompt}").strip()
    return val if val else default


def print_banner():
    """Print fancy banner"""
    print("""
╔══════════════════════════════════════════════════════════════════╗
║                                                                  ║
║     ██╗   ██╗██╗████████╗██╗███╗   ███╗ █████╗ ████████╗███████╗
║     ██║   ██║██║╚══██╔══╝██║████╗ ████║██╔══██╗╚══██╔══╝██╔════╝
║     ██║   ██║██║   ██║   ██║██╔████╔██║███████║   ██║   █████╗  
║     ██║   ██║██║   ██║   ██║██║╚██╔╝██║██╔══██║   ██║   ██╔══╝  
║     ╚██████╔╝██║   ██║   ██║██║ ╚═╝ ██║██║  ██║   ██║   ███████╗
║      ╚═════╝ ╚═╝   ╚═╝   ╚═╝╚═╝     ╚═╝╚═╝  ╚═╝   ╚═╝   ╚══════╝
║                                                                  ║
║                    ULTIMATE TRADING BOT v5.0                    ║
║              Multi-Asset AI Trading System with                  ║
║           50+ Indicators • 13 Strategies • ML Ensemble          ║
║              Sentiment Analysis • Whale Alerts                   ║
╚══════════════════════════════════════════════════════════════════╝
    """)


# ─────────────────────────────────────────────
#  SYSTEM STATUS
# ─────────────────────────────────────────────

def get_status() -> dict:
    status = {
        "balance":    "unknown",
        "open":       0,
        "closed":     0,
        "total_pnl":  0.0,
        "upgrades":   False,
        "models":     0,
        "whales":     0,
    }
    
    # Get trade data
    try:
        with open("paper_trades.json") as f:
            data = json.load(f)
        status["open"]   = len(data.get("open_positions", []))
        status["closed"] = len(data.get("closed_positions", []))
        closed = data.get("closed_positions", [])
        status["total_pnl"] = round(sum(t.get("pnl", 0) for t in closed), 4)
    except Exception:
        pass
    
    # Check profitability upgrades
    try:
        import profitability_upgrade  # noqa
        status["upgrades"] = True
    except ImportError:
        pass
    
    # Count trained models
    try:
        model_dir = Path("ml_models")
        if model_dir.exists():
            status["models"] = len(list(model_dir.glob("*.pkl")))
    except Exception:
        pass
    
    return status


def print_header(status: dict):
    pnl_sign = "+" if status["total_pnl"] >= 0 else ""
    upgrades_label = "✅ ACTIVE" if status["upgrades"] else "❌ NOT INSTALLED"
    
    print("\n" + "=" * 70)
    print(f"  {BOT_NAME} v{VERSION} - LAUNCHER")
    print("=" * 70)
    print(f"  📁 Project      : {Path.cwd()}")
    print(f"  🐍 Python       : {sys.executable.split(os.sep)[-3]}")
    print(f"  🌐 GitHub       : {GITHUB_REPO}")
    print()
    print(f"  📊 Open positions  : {status['open']}")
    print(f"  📈 Closed trades   : {status['closed']}")
    print(f"  💰 Total PnL       : {pnl_sign}${status['total_pnl']}")
    print(f"  🛡️ Profit upgrades : {upgrades_label}")
    print(f"  🤖 Trained models  : {status['models']}")
    print("=" * 70)


def print_menu():
    print("""
  📈 PAPER TRADING
  ─────────────────────────────────────────────
   1. VOTING mode     (12 strategies vote)      🗳️
   2. STRICT mode     (high-confidence entries) 🔒
   3. BALANCED mode   (middle ground)           ⚖️
   4. FAST mode       (more trades)             ⚡

  🌐 WEB DASHBOARDS
  ─────────────────────────────────────────────
   5. Live Dashboard        http://localhost:5000
   6. Sentiment Dashboard   http://localhost:5000/sentiment
   7. Backtest Visualizer   http://localhost:5000/backtest
   8. System Status         http://localhost:5000/status

  📊 REAL-TIME TRADING
  ─────────────────────────────────────────────
   9. Real-time trader     (WebSocket signals)  📡
  10. 5-Minute scalper     (fast trades)        ⚡

  🤖 MACHINE LEARNING
  ─────────────────────────────────────────────
  11. Train ML models      (all assets)         🧠
  12. Training monitor     (check status)       📊
  13. Model registry       (view performance)   📈

  📉 BACKTESTING
  ─────────────────────────────────────────────
  14. Backtest single asset
  15. Compare all strategies
  16. Optimize strategy parameters
  17. Batch optimize all    (takes hours)       ⚡

  📰 SENTIMENT & NEWS
  ─────────────────────────────────────────────
  18. Test sentiment analysis
  19. Sentiment monitor     (real-time)         📡
  20. View whale alerts     (Twitter/Telegram)  🐋

  🛠️ SYSTEM
  ─────────────────────────────────────────────
  21. Master controller    (24/7 auto-trading)  🤖
  22. System health check
  23. Verify installation
  24. Database status
  25. View open positions  (terminal)

  💰 PROFITABILITY UPGRADES
  ─────────────────────────────────────────────
   0. Check / apply profitability upgrades

  ❌ Exit
  ─────────────────────────────────────────────""")


# ─────────────────────────────────────────────
#  STRATEGY LAUNCHER HELPER
# ─────────────────────────────────────────────

def launch_strategy(mode: str, label: str):
    balance = ask(f"Balance $ (default 30): ", "30")
    reset   = ask("Reset positions? (y/n, default y): ", "y").lower()
    cmd = [
        sys.executable, "trading_system.py",
        "--mode", "live",
        "--balance", balance,
        "--strategy-mode", mode,
    ]
    if reset == "y":
        cmd.append("--reset")
    print(f"\n  Starting {label}  (Ctrl+C to stop)\n")
    run(cmd)


# ─────────────────────────────────────────────
#  UPGRADES CHECK
# ─────────────────────────────────────────────

def check_upgrades():
    print("\n  🔧 PROFITABILITY UPGRADE STATUS")
    print("  " + "-" * 50)
    if Path("profitability_upgrade.py").exists():
        print("  ✅ File found: profitability_upgrade.py")
        try:
            import profitability_upgrade
            print("  ✅ Module imports OK")
            print()
            print("  📋 Fixes included:")
            print("    • ATR-based stop losses (adapts to volatility)")
            print("    • Auto take-profit levels for VOTING strategy")
            print("    • 60-min cooldown after a losing trade")
            print("    • Max positions per asset class (1 crypto, 2 forex...)")
            print("    • Entry quality filter (ADX, RSI, BB position)")
            print("    • 4-hour position age limit (closes stale trades)")
            print()
            print("  🔍 INTEGRATION STATUS:")
            _check_integration()
        except Exception as e:
            print(f"  ❌ Import error: {e}")
    else:
        print("  ❌ profitability_upgrade.py NOT FOUND")
        print()
        print("  Download it and place it in your project folder.")
        print("  Then follow the integration guide.")


def _check_integration():
    """Quick check whether trading_system.py has been patched."""
    ts = Path("trading_system.py")
    if not ts.exists():
        print("  ❌ trading_system.py not found")
        return

    content = ts.read_text(errors="ignore")
    checks = {
        "profitability_upgrade imported":   "from profitability_upgrade import" in content,
        "apply_upgrades() called":          "apply_upgrades(self)" in content,
        "enhance_signal() used":            "enhance_signal(" in content,
        "on_trade_closed() used":           "on_trade_closed(" in content,
    }
    all_ok = True
    for label, passed in checks.items():
        icon = "✅" if passed else "❌"
        print(f"    {icon} {label}")
        if not passed:
            all_ok = False

    if all_ok:
        print()
        print("  ✅ All upgrades integrated!")
    else:
        print()
        print("  Run: python profitability_upgrade.py")
        print("  to see the full integration guide.")


# ─────────────────────────────────────────────
#  OPTION HANDLERS
# ─────────────────────────────────────────────

def view_positions():
    if not Path("paper_trades.json").exists():
        print("\n  📭 No paper_trades.json found")
        return
    with open("paper_trades.json") as f:
        data = json.load(f)
    open_pos  = data.get("open_positions", [])
    closed    = data.get("closed_positions", [])
    total_pnl = sum(t.get("pnl", 0) for t in closed)
    wins      = sum(1 for t in closed if t.get("pnl", 0) > 0)

    print(f"\n  📊 Open: {len(open_pos)}  |  Closed: {len(closed)}")
    if closed:
        print(f"  📈 Win rate: {wins}/{len(closed)} = {wins/len(closed)*100:.0f}%")
        print(f"  💰 Total PnL: ${total_pnl:.4f}")
    print()

    if open_pos:
        print("  📋 OPEN POSITIONS:")
        print(f"  {'Asset':<12} {'Signal':<6} {'Entry':>10} {'Stop':>10} {'Confidence':>11}  Strategy")
        print("  " + "-" * 68)
        for t in open_pos:
            entry  = t.get("entry_price", 0)
            stop   = t.get("stop_loss", 0)
            conf   = t.get("confidence", 0)
            strat  = t.get("strategy_id", "?")
            signal = t.get("signal", "?")
            asset  = t.get("asset", "?")
            print(f"  {asset:<12} {signal:<6} {entry:>10.4f} {stop:>10.4f} {conf*100:>10.0f}%  {strat}")
        print()
    else:
        print("  No open positions.")

    if closed:
        print("  📋 LAST 5 CLOSED TRADES:")
        print(f"  {'Asset':<12} {'Signal':<6} {'PnL':>8}  {'Exit reason':<20}  Duration")
        print("  " + "-" * 62)
        for t in list(reversed(closed))[:5]:
            pnl    = t.get("pnl", 0)
            dur    = t.get("duration_minutes", 0)
            reason = t.get("exit_reason", "?")
            asset  = t.get("asset", "?")
            signal = t.get("signal", "?")
            sign   = "+" if pnl >= 0 else ""
            print(f"  {asset:<12} {signal:<6} {sign}${pnl:>7.4f}  {reason:<20}  {dur}m")


def view_whale_alerts():
    """Show recent whale alerts"""
    code = """
from whale_alert_manager import WhaleAlertManager
manager = WhaleAlertManager()
alerts = manager.get_alerts()
print(f"\\n  🐋 Recent Whale Alerts ({len(alerts)}):")
print("  " + "-" * 50)
for alert in alerts[:10]:
    print(f"  {alert['title']}")
    print(f"     • Source: {alert['source']}")
    print(f"     • Value: ${alert['value_usd']:,.0f}")
    print()
"""
    run([sys.executable, "-c", code])


def model_registry():
    """Show model registry status"""
    code = """
from model_registry import ModelRegistry
r = ModelRegistry()
report = r.get_performance_report()
print(f"\\n  🤖 Model Registry Report")
print("  " + "-" * 50)
print(f"  Total models: {report['total_models']}")
print(f"  Active models: {report['active_models']}")
print(f"  Avg accuracy: {report['avg_accuracy']:.1%}")
if report['best_models']:
    print("\\n  🏆 Best Models:")
    for m in report['best_models'][:5]:
        print(f"    • {m['asset']}: {m['accuracy']:.1%} accuracy")
"""
    run([sys.executable, "-c", code])


def db_status():
    code = """
from services.database_service import DatabaseService
from sqlalchemy import text
db = DatabaseService()
try:
    count = db.session.execute(text('SELECT COUNT(*) FROM trades')).scalar()
    print(f"  ✅ Connected. Total trades in DB: {count}")
    rows = db.session.execute(text(
        "SELECT strategy_id, COUNT(*) FROM trades GROUP BY strategy_id"
    )).fetchall()
    if rows:
        print("  📊 By strategy:")
        for s, n in rows:
            print(f"    • {s}: {n}")
    db.close()
except Exception as e:
    print(f"  ❌ Database error: {e}")
"""
    run([sys.executable, "-c", code])


def backtest():
    asset    = ask("Asset (e.g. BTC-USD, default BTC-USD): ", "BTC-USD")
    run([sys.executable, "trading_system.py", "--mode", "backtest", "--asset", asset])


def optimise():
    asset    = ask("Asset (default BTC-USD): ", "BTC-USD")
    strategy = ask("Strategy (rsi/macd/bb/ma_cross, default rsi): ", "rsi")
    run([sys.executable, "trading_system.py",
         "--mode", "optimize", "--asset", asset, "--strategy", strategy])


def batch_optimize():
    print("\n  ⚠️ This will take several hours!")
    print("  Optimizes ALL 50+ strategies for ALL assets.")
    confirm = ask("Continue? (y/n): ", "n")
    if confirm.lower() == "y":
        run([sys.executable, "trading_system.py", "--mode", "batch-optimize"])


def test_sentiment():
    code = """
from sentiment_analyzer import SentimentAnalyzer
s = SentimentAnalyzer()
print("\\n  📰 CRYPTO SENTIMENT:")
r = s.get_comprehensive_sentiment(asset_type='crypto')
print(f"    Score: {r['score']:.2f}  |  {r['interpretation']}")
print("\\n  📰 MARKET SENTIMENT:")
r2 = s.get_comprehensive_sentiment(asset_type='general')
print(f"    Score: {r2['score']:.2f}  |  {r2['interpretation']}")
"""
    run([sys.executable, "-c", code])


def sentiment_monitor():
    code = """
from sentiment_analyzer import SentimentAnalyzer
import time
from datetime import datetime
s = SentimentAnalyzer()
print("  📰 Sentiment monitor running (Ctrl+C to stop)\\n")
try:
    while True:
        r1 = s.get_comprehensive_sentiment(asset_type='crypto')
        r2 = s.get_comprehensive_sentiment(asset_type='general')
        ts = datetime.now().strftime('%H:%M:%S')
        print(f"  [{ts}] Crypto: {r1['interpretation']} ({r1['score']:.2f})  "
              f"Market: {r2['interpretation']} ({r2['score']:.2f})")
        time.sleep(30)
except KeyboardInterrupt:
    print("\\n  Stopped.")
"""
    run([sys.executable, "-c", code])


def train_models():
    print("\n  🧠 This trains ML models on all configured assets.")
    print("  Takes approximately 20-30 minutes.")
    confirm = ask("Continue? (y/n): ", "n")
    if confirm.lower() == "y":
        run([sys.executable, "trading_system.py", "--mode", "train"])


# ─────────────────────────────────────────────
#  MAIN LOOP
# ─────────────────────────────────────────────

def main():
    while True:
        clear_screen()
        print_banner()
        status = get_status()
        print_header(status)
        print_menu()

        choice = ask("Select option: ").lower()

        # ── Paper trading (1-4) ─────────────────────
        if choice == "1":
            launch_strategy("voting",   "VOTING mode (12 strategies)")
        elif choice == "2":
            launch_strategy("strict",   "STRICT mode")
        elif choice == "3":
            launch_strategy("balanced", "BALANCED mode")
        elif choice == "4":
            launch_strategy("fast",     "FAST mode")

        # ── Web Dashboards (5-8) ────────────────────
        elif choice == "5":
            balance = ask("Balance $ (default 30): ", "30")
            run([sys.executable, "web_app_live.py", "--balance", balance])
        elif choice == "6":
            run([sys.executable, "web_app_live.py", "--balance", "30"])
        elif choice == "7":
            run([sys.executable, "web_app_live.py", "--balance", "30"])
        elif choice == "8":
            run([sys.executable, "web_app_live.py", "--balance", "30"])

        # ── Real-time Trading (9-10) ─────────────────
        elif choice == "9":
            print("\n  Starting real-time WebSocket trader (Ctrl+C to stop)")
            run([sys.executable, "realtime_trader.py"])
        elif choice == "10":
            print("\n  Starting 5-minute scalper (Ctrl+C to stop)")
            run([sys.executable, "realtime_trader_5m.py"])

        # ── Machine Learning (11-13) ─────────────────
        elif choice == "11":
            train_models()
        elif choice == "12":
            run([sys.executable, "training_monitor.py"])
        elif choice == "13":
            model_registry()

        # ── Backtesting (14-17) ──────────────────────
        elif choice == "14":
            backtest()
        elif choice == "15":
            run([sys.executable, "trading_system.py", "--mode", "compare"])
        elif choice == "16":
            optimise()
        elif choice == "17":
            batch_optimize()

        # ── Sentiment & News (18-20) ─────────────────
        elif choice == "18":
            test_sentiment()
        elif choice == "19":
            sentiment_monitor()
        elif choice == "20":
            view_whale_alerts()

        # ── System (21-25) ───────────────────────────
        elif choice == "21":
            print("\n  Starting Master Controller (Ctrl+C to stop)")
            run([sys.executable, "master_controller.py"])
        elif choice == "22":
            run([sys.executable, "health_check.py"])
        elif choice == "23":
            run([sys.executable, "verify_installation.py"])
        elif choice == "24":
            db_status()
        elif choice == "25":
            view_positions()
            pause()
            continue

        # ── Profitability upgrades (0) ───────────────
        elif choice == "0":
            check_upgrades()

        # ── Exit ─────────────────────────────────────
        elif choice in ("x", "exit", "quit"):
            print("\n  👋 Goodbye! Happy Trading!\n")
            sys.exit(0)

        else:
            print(f"\n  ❌ Unknown option: {choice}")
            time.sleep(1)
            continue

        pause()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n  👋 Goodbye! Happy Trading!\n")
        sys.exit(0)