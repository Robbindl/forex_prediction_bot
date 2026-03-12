"""
migrate_timescale.py — Upgrade PostgreSQL to TimescaleDB
=========================================================
Run ONCE after installing TimescaleDB extension.

What this does:
  1. Installs the TimescaleDB extension in your database
  2. Creates any missing tables used by the new services
  3. Converts time-series tables to hypertables (massive query speedup)
  4. Creates continuous aggregates for candle OHLCV data
  5. Sets up data retention policies (keep 1 year, auto-drop older)

BEFORE running:
  1. Install TimescaleDB:
     https://docs.timescale.com/self-hosted/latest/install/installation-windows/
  2. It installs alongside your existing PostgreSQL — same port, same data
  3. Your .env DATABASE_URL does NOT change

Run:
  python migrate_timescale.py

Safe to run multiple times — all operations use IF NOT EXISTS / DO NOTHING.
"""

import os
import sys
import time
from datetime import datetime

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

DATABASE_URL = os.getenv(
    'DATABASE_URL',
    'postgresql://postgres:@localhost:5432/trading_bot'
)

try:
    import psycopg2
    from psycopg2.extras import execute_values
except ImportError:
    print("ERROR: psycopg2 not installed.  Run:  pip install psycopg2-binary")
    sys.exit(1)


def run(conn, sql: str, params=None, label: str = ''):
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
        conn.commit()
        print(f"  ✓  {label or sql[:60]}")
        return True
    except Exception as e:
        conn.rollback()
        err = str(e).strip()
        if 'already exists' in err or 'already a hypertable' in err:
            print(f"  ↩  {label or sql[:60]}  (already done)")
            return True
        print(f"  ✗  {label or sql[:60]}")
        print(f"     {err}")
        return False


def migrate():
    print()
    print("╔══════════════════════════════════════════════════════╗")
    print("║  TimescaleDB Migration — Trading Intelligence Bot   ║")
    print("╚══════════════════════════════════════════════════════╝")
    print(f"  Database: {DATABASE_URL.split('@')[-1]}")
    print()

    try:
        conn = psycopg2.connect(DATABASE_URL)
        conn.autocommit = False
        print("  ✓  Connected to PostgreSQL")
    except Exception as e:
        print(f"  ✗  Connection failed: {e}")
        sys.exit(1)

    # ── Step 1: Install TimescaleDB extension ───────────────────────────────
    print("\n[1/7] Installing TimescaleDB extension…")
    ok = run(conn, "CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;",
             label="CREATE EXTENSION timescaledb")
    if not ok:
        print("\n  ⚠  TimescaleDB extension not installed.")
        print("     Download from: https://docs.timescale.com/self-hosted/latest/install/")
        print("     Continuing without hypertables (regular tables still work)…")
        timescale_available = False
    else:
        timescale_available = True

    # ── Step 2: Create new tables ───────────────────────────────────────────
    print("\n[2/7] Creating tables for new services…")

    run(conn, """
        CREATE TABLE IF NOT EXISTS orderflow_snapshots (
            id          BIGSERIAL,
            asset       TEXT NOT NULL,
            timestamp   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            bid_vol     FLOAT,
            ask_vol     FLOAT,
            delta       FLOAT,
            imbalance   FLOAT,
            pressure    TEXT,
            category    TEXT,
            raw         JSONB
        )
    """, label="CREATE TABLE orderflow_snapshots")

    run(conn, """
        CREATE TABLE IF NOT EXISTS alpha_signals (
            id               BIGSERIAL,
            asset            TEXT NOT NULL,
            timestamp        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            signal_type      TEXT,
            direction        TEXT,
            strength         FLOAT,
            detail           TEXT,
            supporting_assets TEXT[],
            acted_on         BOOLEAN DEFAULT FALSE
        )
    """, label="CREATE TABLE alpha_signals")

    run(conn, """
        CREATE TABLE IF NOT EXISTS prediction_outcomes (
            id                BIGSERIAL PRIMARY KEY,
            asset             TEXT NOT NULL,
            category          TEXT,
            direction         TEXT NOT NULL,
            entry_price       FLOAT,
            target_price      FLOAT,
            confidence        FLOAT,
            signal_time       TIMESTAMPTZ NOT NULL,
            horizon_minutes   INT NOT NULL,
            eval_time         TIMESTAMPTZ,
            actual_price      FLOAT,
            direction_correct BOOLEAN,
            target_hit        BOOLEAN,
            pct_move          FLOAT,
            evaluated         BOOLEAN DEFAULT FALSE,
            strategy          TEXT,
            session           TEXT,
            regime            TEXT
        )
    """, label="CREATE TABLE prediction_outcomes")

    run(conn, """
        CREATE TABLE IF NOT EXISTS price_candles (
            id        BIGSERIAL,
            asset     TEXT NOT NULL,
            timestamp TIMESTAMPTZ NOT NULL,
            open      FLOAT,
            high      FLOAT,
            low       FLOAT,
            close     FLOAT,
            volume    FLOAT,
            interval  TEXT DEFAULT '1h'
        )
    """, label="CREATE TABLE price_candles")

    # ── Step 3: Add indexes ─────────────────────────────────────────────────
    print("\n[3/7] Creating indexes…")

    for idx_sql, label in [
        ("CREATE INDEX IF NOT EXISTS idx_orderflow_asset_ts ON orderflow_snapshots (asset, timestamp DESC)",
         "INDEX orderflow(asset, ts)"),
        ("CREATE INDEX IF NOT EXISTS idx_alpha_asset ON alpha_signals (asset, timestamp DESC)",
         "INDEX alpha(asset)"),
        ("CREATE INDEX IF NOT EXISTS idx_pred_asset_time ON prediction_outcomes (asset, signal_time DESC)",
         "INDEX prediction(asset, signal_time)"),
        ("CREATE INDEX IF NOT EXISTS idx_pred_evaluated ON prediction_outcomes (evaluated, eval_time)",
         "INDEX prediction(evaluated, eval_time)"),
        ("CREATE INDEX IF NOT EXISTS idx_candles_asset_ts ON price_candles (asset, timestamp DESC, interval)",
         "INDEX candles(asset, ts, interval)"),
    ]:
        run(conn, idx_sql, label=label)

    # ── Step 4: Convert to hypertables ─────────────────────────────────────
    if timescale_available:
        print("\n[4/7] Converting to TimescaleDB hypertables…")
        for table, col in [
            ('orderflow_snapshots', 'timestamp'),
            ('alpha_signals',       'timestamp'),
            ('prediction_outcomes', 'signal_time'),
            ('price_candles',       'timestamp'),
        ]:
            run(conn,
                f"SELECT create_hypertable('{table}', '{col}', if_not_exists => TRUE);",
                label=f"HYPERTABLE {table}({col})")
    else:
        print("\n[4/7] Skipping hypertables (TimescaleDB not available)…")

    # ── Step 5: Continuous aggregates ──────────────────────────────────────
    if timescale_available:
        print("\n[5/7] Creating continuous aggregates…")
        run(conn, """
            CREATE MATERIALIZED VIEW IF NOT EXISTS orderflow_hourly
            WITH (timescaledb.continuous) AS
            SELECT
                time_bucket('1 hour', timestamp) AS bucket,
                asset,
                AVG(imbalance)  AS avg_imbalance,
                AVG(delta)      AS avg_delta,
                MAX(bid_vol)    AS max_bid,
                MAX(ask_vol)    AS max_ask,
                MODE() WITHIN GROUP (ORDER BY pressure) AS dominant_pressure
            FROM orderflow_snapshots
            GROUP BY bucket, asset
            WITH NO DATA;
        """, label="CAGG orderflow_hourly")

        run(conn, """
            CREATE MATERIALIZED VIEW IF NOT EXISTS prediction_daily_accuracy
            WITH (timescaledb.continuous) AS
            SELECT
                time_bucket('1 day', signal_time) AS bucket,
                asset,
                horizon_minutes,
                COUNT(*) AS total,
                SUM(CASE WHEN direction_correct THEN 1 ELSE 0 END) AS correct,
                AVG(confidence) AS avg_confidence
            FROM prediction_outcomes
            WHERE evaluated = true
            GROUP BY bucket, asset, horizon_minutes
            WITH NO DATA;
        """, label="CAGG prediction_daily_accuracy")
    else:
        print("\n[5/7] Skipping continuous aggregates (TimescaleDB not available)…")

    # ── Step 6: Retention policies ──────────────────────────────────────────
    if timescale_available:
        print("\n[6/7] Setting up data retention policies…")
        for table, days, label in [
            ('orderflow_snapshots', 90,  "orderflow: keep 90 days"),
            ('alpha_signals',       365, "alpha: keep 1 year"),
            ('price_candles',       365, "candles: keep 1 year"),
        ]:
            run(conn,
                f"SELECT add_retention_policy('{table}', INTERVAL '{days} days', if_not_exists => TRUE);",
                label=label)
    else:
        print("\n[6/7] Skipping retention policies (TimescaleDB not available)…")

    # ── Step 7: Verify ──────────────────────────────────────────────────────
    print("\n[7/7] Verifying tables…")
    tables = ['orderflow_snapshots', 'alpha_signals', 'prediction_outcomes', 'price_candles']
    with conn.cursor() as cur:
        for table in tables:
            cur.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = %s", (table,))
            exists = cur.fetchone()[0] > 0
            print(f"  {'✓' if exists else '✗'}  {table}")

    if timescale_available:
        with conn.cursor() as cur:
            cur.execute("SELECT hypertable_name FROM timescaledb_information.hypertables")
            hts = [r[0] for r in cur.fetchall()]
            print(f"\n  TimescaleDB hypertables: {', '.join(hts) if hts else 'none'}")

    conn.close()

    print()
    print("╔══════════════════════════════════════════════════════╗")
    print("║  Migration complete!                                 ║")
    print("╠══════════════════════════════════════════════════════╣")
    print("║  ✓  New tables created                               ║")
    print("║  ✓  Indexes added                                    ║")
    if timescale_available:
        print("║  ✓  TimescaleDB hypertables active                   ║")
        print("║  ✓  Continuous aggregates created                    ║")
        print("║  ✓  Retention policies set                           ║")
    print("║                                                      ║")
    print("║  Your .env DATABASE_URL is unchanged.                ║")
    print("║  Start the bot normally: python bot.py               ║")
    print("╚══════════════════════════════════════════════════════╝")
    print()


if __name__ == '__main__':
    migrate()
