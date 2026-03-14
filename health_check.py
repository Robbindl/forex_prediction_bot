"""
health_check.py — Monitors only the services that bot.py actually runs.

What bot.py starts:
    ✅ web_app_live.py       — Dashboard :5000
    ✅ trading_system.py     — Trading engine (owns Telegram internally)
    ✅ performance_dashboard — Optional, skipped with --no-perf

What it does NOT start (must NOT monitor):
    ❌ master_controller.py  — replaced by bot.py watchdog
    ❌ realtime_trader.py    — not used
    ❌ telegram_commander.py — runs inside trading_system, not standalone

Run mode: called by bot.py watchdog every 5 min. Runs once, exits.
Never restarts anything — bot.py watchdog handles restarts.
"""

import json
import os
import sys
from datetime import datetime
from pathlib import Path

import psutil
import requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logger import logger

# ── Config ────────────────────────────────────────────────────────────────────

REQUIRED_SCRIPTS = {
    'Dashboard (web_app_live)': 'web_app_live.py',
    'Trading Engine':           'trading_system.py',
}
OPTIONAL_SCRIPTS = {
    'Perf Dashboard (optional)': 'performance_dashboard.py',
}

RAM_WARN_PCT  = 85
CPU_WARN_PCT  = 90
DISK_WARN_PCT = 90

# ── Helpers ───────────────────────────────────────────────────────────────────

def _get_balance(default=30.0):
    try:
        cfg = Path('config/bot_runtime.json')
        return float(json.loads(cfg.read_text(encoding='utf-8')).get('balance', default))
    except Exception:
        return default


def _tg(text: str):
    """Send alert via .env tokens — never reads telegram_config.json."""
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except Exception:
        pass
    token   = os.getenv('COMMAND_BOT_TOKEN') or os.getenv('TELEGRAM_TOKEN', '')
    chat_id = os.getenv('COMMAND_BOT_CHAT_ID') or os.getenv('TELEGRAM_CHAT_ID', '')
    if not token or not chat_id:
        logger.warning("Health: no Telegram token in .env")
        return
    try:
        requests.post(
            f'https://api.telegram.org/bot{token}/sendMessage',
            data={'chat_id': chat_id,
                  'text': f'🔍 *Health Check*\n\n{text}',
                  'parse_mode': 'Markdown'},
            timeout=10,
        )
    except Exception as e:
        logger.error(f"Health Telegram error: {e}")


def _is_running(script: str) -> bool:
    for proc in psutil.process_iter(['cmdline']):
        try:
            if script in ' '.join(proc.info['cmdline'] or []):
                return True
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    return False


def _sys_stats():
    s = {}
    try:
        m = psutil.virtual_memory()
        s['ram_pct']      = m.percent
        s['ram_used_gb']  = m.used / 1024**3
        s['ram_total_gb'] = m.total / 1024**3
    except Exception:
        s['ram_pct'] = 0
    try:
        s['cpu_pct'] = psutil.cpu_percent(interval=1)
    except Exception:
        s['cpu_pct'] = 0
    try:
        d = psutil.disk_usage('/')
        s['disk_pct']     = d.percent
        s['disk_free_gb'] = d.free / 1024**3
    except Exception:
        s['disk_pct'] = 0
    return s


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    now = datetime.now()
    logger.info(f"\n{'='*55}")
    logger.info(f"HEALTH CHECK — {now.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"{'='*55}")

    issues = []
    warnings = []
    ok_count = 0

    # ── Required processes ───────────────────────────────────────────────────
    logger.info("\nPROCESS STATUS:")
    for label, script in REQUIRED_SCRIPTS.items():
        running = _is_running(script)
        icon    = "OK  " if running else "DOWN"
        logger.info(f"  [{icon}] {label}")
        if running:
            ok_count += 1
        else:
            issues.append(f"{label} is down")

    for label, script in OPTIONAL_SCRIPTS.items():
        running = _is_running(script)
        logger.info(f"  [{'OK  ' if running else 'skip'}] {label}")

    # ── System stats ─────────────────────────────────────────────────────────
    logger.info("\nSYSTEM:")
    s = _sys_stats()

    logger.info(f"  RAM:  {s['ram_pct']:.0f}%  ({s.get('ram_used_gb',0):.1f}/{s.get('ram_total_gb',0):.1f} GB)")
    if s['ram_pct'] > RAM_WARN_PCT:
        warnings.append(f"High RAM: {s['ram_pct']:.0f}%")

    logger.info(f"  CPU:  {s['cpu_pct']:.0f}%")
    if s['cpu_pct'] > CPU_WARN_PCT:
        warnings.append(f"High CPU: {s['cpu_pct']:.0f}%")

    logger.info(f"  Disk: {s['disk_pct']:.0f}% used  ({s.get('disk_free_gb',0):.1f} GB free)")
    if s['disk_pct'] > DISK_WARN_PCT:
        warnings.append(f"Low disk: {s.get('disk_free_gb',0):.1f} GB free")

    # ── Model freshness ──────────────────────────────────────────────────────
    try:
        from training_monitor import TrainingMonitor
        ages = TrainingMonitor().get_model_ages()
        old  = [n for n, d in ages.items() if d.get('age_days', 0) > 7]
        logger.info(f"  Models: {len(ages)-len(old)} fresh, {len(old)} stale")
        if old:
            warnings.append(f"{len(old)} models >7 days old — will retrain at midnight")
    except Exception:
        pass

    # ── Summary ──────────────────────────────────────────────────────────────
    total = len(REQUIRED_SCRIPTS)
    logger.info(f"\nSUMMARY: {ok_count}/{total} required up | {len(issues)} issues | {len(warnings)} warnings")

    log_entry = {
        'timestamp': now.isoformat(),
        'ok': ok_count, 'required': total,
        'issues': issues, 'warnings': warnings,
        'status': 'OK' if not issues else 'ISSUES',
        'ram_pct': s.get('ram_pct', 0),
        'cpu_pct': s.get('cpu_pct', 0),
    }
    # STORAGE FIX: was plain open(..., 'a') which grew forever.
    # Now keep only the last 288 entries (= 1 day at 5-min intervals = ~150KB max).
    health_log_path = Path('logs/health_log.json')
    health_log_path.parent.mkdir(exist_ok=True)
    try:
        existing = []
        if health_log_path.exists():
            for line in health_log_path.read_text(encoding='utf-8').splitlines():
                line = line.strip()
                if line:
                    try:
                        existing.append(json.loads(line))
                    except Exception:
                        pass
        existing.append(log_entry)
        existing = existing[-288:]          # keep last 288 entries only
        with open(health_log_path, 'w', encoding='utf-8') as f:
            for entry in existing:
                f.write(json.dumps(entry) + '\n')
    except Exception as _e:
        logger.warning(f"Health log write failed: {_e}")

    # ── Telegram — only on real problems ────────────────────────────────────
    if issues:
        _tg(
            "🚨 *Service(s) down*\n"
            + "\n".join(f"• {i}" for i in issues)
            + "\n\n_bot.py watchdog will auto-restart._"
        )
    elif warnings:
        _tg("⚠️ *System warnings*\n" + "\n".join(f"• {w}" for w in warnings))
    else:
        # All-clear only every 6 hours — not every 5 minutes
        if now.hour % 6 == 0 and now.minute < 6:
            _tg(
                f"✅ *All healthy*\n"
                f"RAM {s.get('ram_pct',0):.0f}% · CPU {s.get('cpu_pct',0):.0f}%\n"
                f"Balance: ${_get_balance():.2f}"
            )
        else:
            logger.info("All healthy — no alert needed")


if __name__ == '__main__':
    main()