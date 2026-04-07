from __future__ import annotations

import argparse
import json
from datetime import date, datetime
from pathlib import Path
from typing import Iterable

from sqlalchemy import text

from config.config import DEFAULT_BALANCE, LOCAL_CANDLE_STORE_PATH
from config.database import SessionLocal


ROOT = Path(__file__).resolve().parent
STATE_FILE = ROOT / "data" / "system_state.json"
LOG_DIR = ROOT / "logs"
TRADE_LOG_DIR = ROOT / "trade_logs"
PORTFOLIO_REPORTS_DIR = ROOT / "portfolio_reports"
TELEGRAM_PID_FILE = ROOT / "telegram_bot.pid"
TELEGRAM_LOG_FILE = ROOT / "telegram_bot.log"
STARTUP_TEST_LOG = ROOT / "startup_test.log"
PAPER_TRADES_FILE = ROOT / "paper_trades.json"

RESET_TABLES = (
    "trading_diary",
    "trades",
    "open_positions",
    "daily_stats",
    "bot_personality",
    "memorable_moments",
    "human_explanations",
    "prediction_outcomes",
    "strategy_performance",
    "strategy_optimisation",
)


def _iter_files(paths: Iterable[Path]) -> list[Path]:
    files: list[Path] = []
    for path in paths:
        if path.is_file():
            files.append(path)
        elif path.is_dir():
            files.extend(p for p in path.rglob("*") if p.is_file())
    return files


def _write_clean_state(balance: float) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    state = {
        "schema_version": 4,
        "saved_at": datetime.now().isoformat(),
        "balance": float(balance),
        "initial_balance": float(balance),
        "daily_trades": 0,
        "daily_pnl": 0.0,
        "last_save_date": date.today().isoformat(),
        "cooldowns": {},
        "strategy_stats": {},
        "session_stats": {},
        "asset_stats": {},
        "open_positions": [],
        "closed_positions": [],
    }
    STATE_FILE.write_text(json.dumps(state, indent=2), encoding="utf-8")


def _clear_files() -> dict[str, int]:
    cleared = 0
    deleted = 0
    locked = 0

    for path in _iter_files((LOG_DIR, TRADE_LOG_DIR, PORTFOLIO_REPORTS_DIR)):
        try:
            path.unlink()
            deleted += 1
        except FileNotFoundError:
            continue
        except PermissionError:
            try:
                path.write_text("", encoding="utf-8")
                cleared += 1
            except Exception:
                locked += 1

    for path in (TELEGRAM_LOG_FILE, STARTUP_TEST_LOG):
        if not path.exists():
            continue
        try:
            path.write_text("", encoding="utf-8")
            cleared += 1
        except PermissionError:
            locked += 1

    for path in (TELEGRAM_PID_FILE, PAPER_TRADES_FILE):
        try:
            if path.exists():
                path.unlink()
                deleted += 1
        except FileNotFoundError:
            pass
        except PermissionError:
            locked += 1

    for path in (STATE_FILE.parent).glob("state_*.tmp"):
        try:
            path.unlink()
            deleted += 1
        except FileNotFoundError:
            pass
        except PermissionError:
            locked += 1

    return {"files_deleted": deleted, "files_cleared": cleared, "files_locked": locked}


def _reset_database() -> dict[str, int]:
    with SessionLocal() as session:
        for table in RESET_TABLES:
            session.execute(text(f'TRUNCATE TABLE "{table}" RESTART IDENTITY CASCADE'))
        session.commit()

        counts = {}
        for table in RESET_TABLES:
            counts[table] = int(session.execute(text(f'SELECT COUNT(*) FROM "{table}"')).scalar() or 0)
        return counts


def _candle_store_summary(wipe_candles: bool) -> dict[str, object]:
    path = Path(LOCAL_CANDLE_STORE_PATH)
    summary: dict[str, object] = {
        "path": str(path),
        "exists": path.exists(),
        "wiped": False,
    }
    if not wipe_candles:
        return summary

    related = [path, Path(f"{path}-shm"), Path(f"{path}-wal")]
    deleted = 0
    for item in related:
        try:
            if item.exists():
                item.unlink()
                deleted += 1
        except FileNotFoundError:
            pass
    summary["wiped"] = True
    summary["deleted_files"] = deleted
    return summary


def reset_bot_state(*, wipe_candles: bool = False) -> dict[str, object]:
    db_counts = _reset_database()
    _write_clean_state(DEFAULT_BALANCE)
    file_summary = _clear_files()
    candle_summary = _candle_store_summary(wipe_candles)

    return {
        "default_balance": float(DEFAULT_BALANCE),
        "database_counts_after_reset": db_counts,
        "state_file": str(STATE_FILE),
        "file_cleanup": file_summary,
        "candle_store": candle_summary,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Reset bot trading state/history/memory to a fresh start.")
    parser.add_argument(
        "--wipe-candles",
        action="store_true",
        help="Also remove the local candle store. Off by default.",
    )
    args = parser.parse_args()
    summary = reset_bot_state(wipe_candles=args.wipe_candles)
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
