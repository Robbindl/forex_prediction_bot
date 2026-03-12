"""
bot.py — One command to rule them all.

Usage:
    python bot.py                  # start everything ($30 default)
    python bot.py --balance 500    # your balance, flows everywhere
    python bot.py --no-perf        # skip performance dashboard
    python bot.py train            # trigger ML training now
    python bot.py stop             # gracefully stop all services
    python bot.py status           # show what's running
"""

import argparse
import json
import os
import signal
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from logger import logger

# ── Paths ─────────────────────────────────────────────────────────────────────

PYTHON   = sys.executable
BASE     = Path(__file__).parent
LOGS_DIR = BASE / 'logs'
CFG_FILE = BASE / 'config' / 'bot_runtime.json'   # shared config for all services

SERVICES = {
    'dashboard': {'script': 'web_app_live.py',         'label': 'Dashboard        :5000'},
    'trading':   {'script': 'trading_system.py',        'label': 'Trading Engine'},
    'perf':      {'script': 'performance_dashboard.py', 'label': 'Perf Dashboard   :8050'},
    'training':  {'script': 'auto_train_daily.py',      'label': 'ML Training'},
}
# NOTE — intentionally excluded:
# 'telegram' → TelegramCommander starts INSIDE trading_system via telegram_manager.
#              Running it as a subprocess has no trading_system reference — exits
#              instantly and causes a crash loop. Already working fine (see Telegram).
# 'health'   → health_check.py runs once and exits. Not a persistent service.
#              bot.py watchdog calls it on a 5-min schedule instead.

RESTART_DELAY  = 10    # seconds before restarting a crashed service
MAX_RESTARTS   = 5     # after this, give up and alert
CHECK_INTERVAL = 30    # seconds between watchdog ticks
TRAINING_HOUR  = 0     # midnight

# ── Shared runtime state ──────────────────────────────────────────────────────

_procs:   dict = {}
_crashes: dict = {}
_lock     = threading.Lock()
_stop_evt = threading.Event()
_args     = None


# ── Runtime config (shared with master_controller / health_check) ─────────────

def _write_runtime_cfg(balance: float):
    """Write balance + start time to config/bot_runtime.json so every
    service that restarts processes can read the correct balance."""
    CFG_FILE.parent.mkdir(exist_ok=True)
    data = {
        'balance':    balance,
        'started_at': datetime.now().isoformat(),
        'python':     PYTHON,
    }
    CFG_FILE.write_text(json.dumps(data, indent=2), encoding='utf-8')


def read_runtime_balance(default: float = 30.0) -> float:
    """Helper any service can import: from bot import read_runtime_balance"""
    try:
        data = json.loads(CFG_FILE.read_text(encoding='utf-8'))
        return float(data.get('balance', default))
    except Exception:
        return default


# ── Telegram alert ────────────────────────────────────────────────────────────

def _tg_alert(text: str):
    """Watchdog alert — .env is the ONLY credential source. Never reads json config files."""
    import os
    try:
        from dotenv import load_dotenv
        load_dotenv(BASE / '.env', override=False)
    except Exception:
        pass
    token   = os.getenv('COMMAND_BOT_TOKEN') or os.getenv('TELEGRAM_TOKEN', '')
    chat_id = os.getenv('TELEGRAM_CHAT_ID', '')
    if not token or not chat_id:
        return
    try:
        import urllib.request, urllib.parse
        url  = f'https://api.telegram.org/bot{token}/sendMessage'
        data = urllib.parse.urlencode({'chat_id': chat_id, 'text': text}).encode()
        urllib.request.urlopen(url, data, timeout=8)
    except Exception:
        pass


# ── Process management ────────────────────────────────────────────────────────

def _build_cmd(key: str, balance: float) -> list:
    script = SERVICES[key]['script']
    cmd    = [PYTHON, script]
    if key == 'dashboard':
        cmd += ['--balance', str(balance)]
    elif key == 'trading':
        cmd += ['--mode', 'live', '--balance', str(balance),
                '--strategy-mode', 'voting', '--no-telegram']
    return cmd


def _start(key: str, balance: float):
    LOGS_DIR.mkdir(exist_ok=True)
    cmd   = _build_cmd(key, balance)
    label = SERVICES[key]['label']
    # STORAGE FIX: Route subprocess stdout/stderr to DEVNULL.
    # Every service (trading_system, web_app_live, etc.) uses logger.py
    # internally which already writes to logs/trading_bot.log with rotation.
    # The old approach used plain open(..., 'a') with NO size limit — that
    # single file was consuming multiple GB per hour from Flask/TF verbose output.
    try:
        kwargs = dict(stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, cwd=BASE)
        if sys.platform == 'win32':
            kwargs['creationflags'] = subprocess.CREATE_NO_WINDOW
        proc = subprocess.Popen(cmd, **kwargs)
        with _lock:
            _procs[key]   = proc
            _crashes[key] = _crashes.get(key, 0)
        _log(f'  ✅  {label:<30s}  PID {proc.pid}')
        return proc
    except Exception as e:
        _log(f'  ❌  {label} failed to start: {e}')
        return None


def _is_alive(key: str) -> bool:
    with _lock:
        proc = _procs.get(key)
    return proc is not None and proc.poll() is None


def _stop_all():
    _log('\n🛑  Stopping all services…')
    with _lock:
        procs = dict(_procs)
    for key, proc in procs.items():
        if proc and proc.poll() is None:
            label = SERVICES[key]['label']
            try:
                proc.terminate()
                proc.wait(timeout=5)
                _log(f'  stopped  {label}')
            except subprocess.TimeoutExpired:
                proc.kill()
                _log(f'  killed   {label}')
    _log('Done.')


# ── Watchdog ──────────────────────────────────────────────────────────────────

def _watchdog(balance: float, auto_services: list):
    last_training_date = None
    active = list(auto_services)

    while not _stop_evt.is_set():
        now = datetime.now()

        # Check each managed service
        for key in list(active):
            if key == 'training':
                continue
            if not _is_alive(key):
                with _lock:
                    n = _crashes.get(key, 0) + 1
                    _crashes[key] = n
                label = SERVICES[key]['label']

                if n > MAX_RESTARTS:
                    msg = (f'🚨 {label} has crashed {n}× — giving up. '
                           f'Fix the issue and run: python bot.py')
                    _log(msg)
                    _tg_alert(msg)
                    active.remove(key)
                    continue

                _log(f'⚠️  {label} crashed (#{n}) — restarting in {RESTART_DELAY}s…')
                _tg_alert(f'⚠️ [{label}] crashed (#{n}/{MAX_RESTARTS}) — restarting…')
                time.sleep(RESTART_DELAY)
                _start(key, balance)

        # Midnight auto-training
        if 'training' in active:
            today = now.date()
            if now.hour == TRAINING_HOUR and now.minute < 5 and last_training_date != today:
                last_training_date = today
                _log('🧠  Midnight — launching auto ML training…')
                _tg_alert(f'🧠 Auto-training started (balance ${balance})')
                _start('training', balance)

        # Scheduled health check every 5 min (runs once + exits — not a persistent service)
        if not hasattr(_watchdog, '_last_health') or (now - _watchdog._last_health).seconds >= 300:
            _watchdog._last_health = now
            try:
                import subprocess as _sp
                _sp.Popen(
                    [PYTHON, str(BASE / 'health_check.py')],
                    stdout=subprocess.DEVNULL,   # STORAGE FIX: was open(health.log,'a') — no size limit
                    stderr=subprocess.DEVNULL,   # health_check uses logger.py internally for real output
                    cwd=BASE,
                    creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == 'win32' else 0,
                )
            except Exception as _he:
                _log(f'Health check failed to launch: {_he}')

        _stop_evt.wait(CHECK_INTERVAL)


# ── Logging ───────────────────────────────────────────────────────────────────

def _log(msg: str):
    ts = datetime.now().strftime('%H:%M:%S')
    logger.info(f'{ts}  {msg}')

# ── Sub-commands ──────────────────────────────────────────────────────────────

def cmd_status():
    balance = read_runtime_balance()
    _log(f'\n{"="*55}')
    _log(f'  📊  BOT STATUS   —   Balance: ${balance}')
    _log(f'{"="*55}')
    try:
        cfg = json.loads(CFG_FILE.read_text(encoding='utf-8'))
        _log(f'  Started : {cfg.get("started_at", "unknown")}')
        _log(f'  Balance : ${cfg.get("balance", "?")}')
    except Exception:
        _log('  (bot not currently running)')
    _log('')
    for key, info in SERVICES.items():
        alive = _is_alive(key)
        proc  = _procs.get(key)
        pid   = proc.pid if proc else '—'
        icon  = '🟢' if alive else '🔴'
        _log(f'  {icon}  {info["label"]:<30s}  pid={pid}')
    _log('')


def cmd_train(balance: float):
    _log(f'🧠  Triggering ML training now  (balance ${balance})…')
    _tg_alert(f'🧠 Manual training triggered  (balance ${balance})')
    _start('training', balance)


def cmd_start(auto_services: list, balance: float):
    LOGS_DIR.mkdir(exist_ok=True)

    # Write balance to shared config so master_controller / health_check use it
    _write_runtime_cfg(balance)

    _log(f'\n{"="*55}')
    _log(f'  🤖  TRADING BOT   —   starting {len([s for s in auto_services if s != "training"])} services')
    _log(f'  Balance   : ${balance}')
    _log(f'  Training  : auto at midnight  +  python bot.py train')
    _log(f'  Logs      : logs/<service>.log')
    _log(f'{"="*55}\n')

    # Stagger starts so APIs don't get hammered simultaneously
    for key in auto_services:
        if key == 'training':
            continue
        time.sleep(1.5)
        _start(key, balance)

    _log(f'\n  Dashboard  → http://localhost:5000')
    _log(f'  Perf       → http://localhost:8050')
    _log(f'  Balance    → ${balance}')
    _log(f'\n  Ctrl+C to stop everything.\n')

    # Graceful shutdown
    def _shutdown(sig=None, frame=None):
        _stop_evt.set()
        _stop_all()
        sys.exit(0)

    signal.signal(signal.SIGINT,  _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    # Watchdog thread
    threading.Thread(
        target=_watchdog, args=(balance, list(auto_services)),
        daemon=True, name='watchdog'
    ).start()

    # Main thread heartbeat every 5 min
    last_hb = datetime.now()
    while True:
        time.sleep(10)
        if (datetime.now() - last_hb).seconds >= 300:
            last_hb = datetime.now()
            alive = [k for k in auto_services if k != 'training' and _is_alive(k)]
            total = len([k for k in auto_services if k != 'training'])
            _log(f'💓  {len(alive)}/{total} services running  |  Balance ${balance}')


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    global _args

    parser = argparse.ArgumentParser(
        description='Trading Bot — one command launcher',
    )
    parser.add_argument('command', nargs='?', default='start',
                        choices=['start', 'stop', 'status', 'train'])
    parser.add_argument('--balance', type=float, default=30,
                        help='Account balance in USD  (default: 30)')
    parser.add_argument('--no-perf', action='store_true',
                        help='Skip performance dashboard (:8050)')
    _args = parser.parse_args()

    balance = _args.balance

    auto_services = [
        'dashboard',
        'trading',
        'training',   # scheduler only — not immediately started
    ]
    if not _args.no_perf:
        auto_services.insert(2, 'perf')
    # 'telegram' and 'health' intentionally excluded — see SERVICES comment above

    if _args.command == 'status':
        cmd_status()
    elif _args.command == 'train':
        cmd_train(balance)
    elif _args.command == 'stop':
        _stop_all()
    else:
        cmd_start(auto_services, balance)


if __name__ == '__main__':
    main()