"""
fix_stale_positions.py — Clean stale open positions from DB.
Uses raw SQL to avoid ORM column mismatch issues.
Run ONCE while bot is stopped:  python fix_stale_positions.py
"""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv
load_dotenv()

from config.database import init_db, SessionLocal
from sqlalchemy import text

init_db()

print("\n=== Stale Position Cleaner ===\n")

with SessionLocal() as s:
    # Get all open positions
    open_rows = s.execute(text(
        "SELECT trade_id, asset, direction, entry_price FROM open_positions"
    )).fetchall()
    print(f"Open positions in DB: {len(open_rows)}")

    stale = []
    active = []
    for row in open_rows:
        tid = row[0]
        # Check if this trade_id exists in closed trades table (using only safe columns)
        closed = s.execute(text(
            "SELECT trade_id, exit_time FROM trades WHERE trade_id = :tid LIMIT 1"
        ), {"tid": tid}).fetchone()

        if closed and closed[1] is not None:
            stale.append(row)
            print(f"  STALE (already closed): {tid} | {row[1]} | {row[2]}")
        else:
            active.append(row)
            print(f"  ACTIVE: {tid} | {row[1]} | {row[2]} | entry={row[3]}")

print(f"\nFound {len(stale)} stale, {len(active)} active\n")

if not stale:
    print("Nothing to clean. All open positions look legitimate.")
    sys.exit(0)

answer = input(f"Delete {len(stale)} stale position(s)? [y/N] ")
if answer.lower() != 'y':
    print("Aborted.")
    sys.exit(0)

with SessionLocal() as s:
    for row in stale:
        tid = row[0]
        s.execute(text("DELETE FROM open_positions WHERE trade_id = :tid"), {"tid": tid})
        print(f"  Deleted: {tid} ({row[1]})")
    s.commit()

print(f"\nDone — deleted {len(stale)} stale position(s)")
print("Restart the bot to see correct position count.\n")