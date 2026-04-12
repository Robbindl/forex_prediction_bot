from __future__ import annotations

import argparse
import json
from datetime import date, datetime
from pathlib import Path
from typing import Iterable

from sqlalchemy import text

from config.config import (
    DEFAULT_BALANCE,
    LOCAL_CANDLE_STORE_PATH,
    TELEGRAM_PID_FILE as CONFIG_TELEGRAM_PID_FILE,
    PAPER_TRADES_FILE as CONFIG_PAPER_TRADES_FILE,
)
from config.database import SessionLocal


ROOT = Path(__file__).resolve().parent
STATE_FILE = ROOT / "data" / "system_state.json"
LOG_DIR = ROOT / "logs"
TRADE_LOG_DIR = ROOT / "trade_logs"
PORTFOLIO_REPORTS_DIR = ROOT / "portfolio_reports"
TELEGRAM_PID_FILE = CONFIG_TELEGRAM_PID_FILE
TELEGRAM_LOG_FILE = LOG_DIR / "telegram_bot.log"
STARTUP_TEST_LOG = LOG_DIR / "startup_test.log"
PAPER_TRADES_FILE = CONFIG_PAPER_TRADES_FILE

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


def _new_file_summary() -> dict[str, int]:
    return {"files_deleted": 0, "files_cleared": 0, "files_locked": 0}


def _merge_file_summaries(*summaries: dict[str, int]) -> dict[str, int]:
    merged = _new_file_summary()
    for summary in summaries:
        for key in merged:
            merged[key] += int(summary.get(key, 0))
    return merged


def _delete_file_group(paths: Iterable[Path]) -> dict[str, int]:
    summary = _new_file_summary()
    for path in paths:
        try:
            path.unlink()
            summary["files_deleted"] += 1
        except FileNotFoundError:
            continue
        except PermissionError:
            summary["files_locked"] += 1
    return summary


def _clear_file_group(paths: Iterable[Path]) -> dict[str, int]:
    summary = _new_file_summary()
    for path in paths:
        if not path.exists():
            continue
        try:
            path.write_text("", encoding="utf-8")
            summary["files_cleared"] += 1
        except PermissionError:
            summary["files_locked"] += 1
    return summary


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
    directory_summary = _delete_file_group(_iter_files((LOG_DIR, TRADE_LOG_DIR, PORTFOLIO_REPORTS_DIR)))
    text_summary = _clear_file_group((TELEGRAM_LOG_FILE, STARTUP_TEST_LOG))
    tracked_summary = _delete_file_group((TELEGRAM_PID_FILE, PAPER_TRADES_FILE))
    temp_summary = _delete_file_group(STATE_FILE.parent.glob("state_*.tmp"))
    return _merge_file_summaries(directory_summary, text_summary, tracked_summary, temp_summary)


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
