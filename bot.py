"""
bot.py — Single-process launcher for the entire trading platform.

PHASE 2 ARCHITECTURE: Everything runs in ONE process.
No more subprocess.Popen for trading engine or dashboard.
One TradingCore instance shared by all subsystems.

Process layout (all in-process daemon threads):
  TradingCore        — trading loop, signal scanning, execution
  Flask web app      — dashboard (web_app_live.py) in a thread
  Dash perf app      — performance_dashboard.py in a thread (optional)
  TelegramCommander  — command bot, wired to TradingCore
  Health watchdog    — periodic health checks
  ML auto-trainer    — midnight training trigger

Why one process?
  TradingCore is shared by reference — no IPC, no state sync lag
  Flask reads live positions directly from TradingCore.state
  Telegram commands call TradingCore methods directly
  No state_bridge.py file polling needed

Usage:
    python bot.py                   # start everything ($30 default)
    python bot.py --balance 500
    python bot.py --strategy voting
    python bot.py --no-perf
    python bot.py status
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

from logger import logger

PYTHON   = sys.executable
BASE     = Path(__file__).parent
LOGS_DIR = BASE / "logs"
CFG_FILE = BASE / "config" / "bot_runtime.json"
TRAINING_HOUR = 0

_engine   = None
_stop_evt = threading.Event()
_args     = None


def _write_runtime_cfg(balance: float) -> None:
    CFG_FILE.parent.mkdir(exist_ok=True)
    CFG_FILE.write_text(
        json.dumps({
            "balance": balance,
            "started_at": datetime.now().isoformat(),
            "python": PYTHON,
            "arch": "single-process-v2",
        }, indent=2),
        encoding="utf-8",
    )


def read_runtime_balance(default: float = 30.0) -> float:
    """Helper any module can import: from bot import read_runtime_balance"""
    try:
        return float(json.loads(CFG_FILE.read_text(encoding="utf-8")).get("balance", default))
    except Exception:
        return default


def _tg_alert(text: str) -> None:
    try:
        from dotenv import load_dotenv
        load_dotenv(BASE / ".env", override=False)
    except Exception:
        pass
    token   = os.getenv("COMMAND_BOT_TOKEN") or os.getenv("TELEGRAM_TOKEN", "")
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
    if not token or not chat_id:
        return
    try:
        import urllib.request, urllib.parse
        url  = f"https://api.telegram.org/bot{token}/sendMessage"
        data = urllib.parse.urlencode({"chat_id": chat_id, "text": text}).encode()
        urllib.request.urlopen(url, data, timeout=8)
    except Exception:
        pass


def _start_flask(engine, balance: float) -> threading.Thread:
    def _run():
        try:
            import web_app_live
            web_app_live.inject_core(engine)
            web_app_live.app.run(
                host="0.0.0.0", port=5000,
                debug=False, use_reloader=False, threaded=True,
            )
        except Exception as e:
            logger.error(f"[Flask] Crashed: {e}", exc_info=True)
    t = threading.Thread(target=_run, name="Flask-dashboard", daemon=True)
    t.start()
    logger.info("  Flask dashboard starting on :5000")
    return t


def _start_dash(engine) -> threading.Thread:
    def _run():
        try:
            import performance_dashboard
            performance_dashboard.inject_core(engine)
            performance_dashboard.app.run(
                host="0.0.0.0", port=8050,
                debug=False, use_reloader=False,
            )
        except Exception as e:
            logger.error(f"[Dash] Crashed: {e}", exc_info=True)
    t = threading.Thread(target=_run, name="Dash-perf", daemon=True)
    t.start()
    logger.info("  Dash dashboard starting on :8050")
    return t


def _start_telegram(engine) -> None:
    try:
        from dotenv import load_dotenv
        load_dotenv(BASE / ".env", override=False)
    except Exception:
        pass
    token   = os.getenv("COMMAND_BOT_TOKEN") or os.getenv("TELEGRAM_TOKEN", "")
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
    if not token or not chat_id:
        logger.info("  Telegram: no token in .env — skipping")
        return
    try:
        from telegram_commander import TelegramCommander
        tg = TelegramCommander(
            token=token,
            chat_id=chat_id,
            trading_system=engine._engine,
        )
        engine.telegram = tg
        tg.start()
        logger.info("  TelegramCommander started")
    except Exception as e:
        logger.warning(f"  TelegramCommander failed: {e}")


def _midnight_trainer(balance: float) -> None:
    last_training_date = None
    while not _stop_evt.is_set():
        now   = datetime.now()
        today = now.date()
        if now.hour == TRAINING_HOUR and now.minute < 5 and last_training_date != today:
            last_training_date = today
            logger.info("[AutoTrainer] Midnight — starting ML training")
            _tg_alert("Brain Auto-training started")
            try:
                import subprocess
                subprocess.Popen(
                    [PYTHON, str(BASE / "auto_train_daily.py")],
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, cwd=BASE,
                )
            except Exception as e:
                logger.error(f"[AutoTrainer] Failed: {e}")
        _stop_evt.wait(60)


def _health_watchdog(engine) -> None:
    while not _stop_evt.is_set():
        _stop_evt.wait(300)
        if _stop_evt.is_set():
            break
        try:
            report = engine.health_report()
            if report["issues"]:
                msg = "Health issues:\n" + "\n".join(f"- {i}" for i in report["issues"])
                logger.warning(msg)
                _tg_alert(msg)
            else:
                logger.info(
                    f"[Health] OK RAM={report['ram_pct']:.0f}% "
                    f"CPU={report['cpu_pct']:.0f}% "
                    f"bal=${report['balance']:.2f} "
                    f"pos={report['open_positions']}"
                )
        except Exception as e:
            logger.error(f"[Health] Failed: {e}")


def cmd_status() -> None:
    try:
        cfg = json.loads(CFG_FILE.read_text(encoding="utf-8"))
        logger.info("=" * 55)
        logger.info("  BOT STATUS")
        logger.info("=" * 55)
        logger.info(f"  Started : {cfg.get('started_at', 'unknown')}")
        logger.info(f"  Balance : ${cfg.get('balance', '?')}")
        logger.info(f"  Arch    : {cfg.get('arch', 'legacy')}")
    except Exception:
        logger.info("  (bot not currently running)")


def cmd_start(balance: float, strategy_mode: str, no_perf: bool, no_telegram: bool) -> None:
    global _engine

    LOGS_DIR.mkdir(exist_ok=True)
    _write_runtime_cfg(balance)

    logger.info("=" * 55)
    logger.info("  TRADING BOT  --  Phase 2 Single-Process")
    logger.info(f"  Balance  : ${balance}")
    logger.info(f"  Strategy : {strategy_mode}")
    logger.info("=" * 55)

    from core.engine import TradingCore
    _engine = TradingCore(
        balance=balance,
        strategy_mode=strategy_mode,
        no_telegram=no_telegram,
    )

    time.sleep(0.5)
    _start_flask(_engine, balance)

    if not no_perf:
        time.sleep(0.5)
        _start_dash(_engine)

    if not no_telegram:
        time.sleep(0.5)
        _start_telegram(_engine)

    time.sleep(1.0)
    _engine.start()

    threading.Thread(
        target=_health_watchdog, args=(_engine,),
        name="health-watchdog", daemon=True,
    ).start()
    threading.Thread(
        target=_midnight_trainer, args=(balance,),
        name="midnight-trainer", daemon=True,
    ).start()

    logger.info("")
    logger.info(f"  Dashboard  -> http://localhost:5000")
    if not no_perf:
        logger.info(f"  Perf       -> http://localhost:8050")
    logger.info(f"  Ctrl+C to stop.")
    logger.info("")

    def _shutdown(sig=None, frame=None):
        logger.info("\nShutdown signal — stopping...")
        _stop_evt.set()
        if _engine:
            _engine.stop("shutdown")
        sys.exit(0)

    signal.signal(signal.SIGINT,  _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    last_hb = datetime.now()
    while True:
        time.sleep(10)
        if _stop_evt.is_set():
            break
        if (datetime.now() - last_hb).seconds >= 300:
            last_hb = datetime.now()
            if _engine:
                logger.info(
                    f"[Heartbeat] engine={'ready' if _engine.is_ready else 'init'} "
                    f"pos={_engine.state.open_position_count()} "
                    f"bal=${_engine.state.balance:.2f}"
                )


def main():
    global _args
    parser = argparse.ArgumentParser(description="Trading Bot Phase 2")
    parser.add_argument("command", nargs="?", default="start",
                        choices=["start", "stop", "status", "train"])
    parser.add_argument("--balance",     type=float, default=30.0)
    parser.add_argument("--strategy",    default="voting",
                        choices=["voting", "balanced", "strict", "fast"])
    parser.add_argument("--no-perf",     action="store_true")
    parser.add_argument("--no-telegram", action="store_true")
    _args = parser.parse_args()

    if _args.command == "status":
        cmd_status()
    elif _args.command == "train":
        import subprocess
        subprocess.Popen([PYTHON, str(BASE / "auto_train_daily.py")], cwd=BASE)
    elif _args.command == "stop":
        logger.info("Use Ctrl+C to stop the running process.")
    else:
        cmd_start(
            balance      =_args.balance,
            strategy_mode=_args.strategy,
            no_perf      =_args.no_perf,
            no_telegram  =_args.no_telegram,
        )


if __name__ == "__main__":
    main()