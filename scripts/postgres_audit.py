#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def _mask_url(value: str) -> str:
    return re.sub(r":([^:@/]+)@", ":***@", value or "")


def _connect():
    _load_dotenv(ROOT / ".env")
    try:
        from config.config import DATABASE_SSLMODE, DATABASE_URL
    except Exception:
        DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
        DATABASE_SSLMODE = os.getenv("DATABASE_SSLMODE", "").strip()

    if not DATABASE_URL:
        raise SystemExit("DATABASE_URL is not configured")

    try:
        import psycopg2
    except Exception as exc:
        raise SystemExit(f"psycopg2 is not installed: {exc}") from exc

    kwargs: Dict[str, Any] = {
        "dsn": DATABASE_URL,
        "connect_timeout": 5,
        "application_name": "forex_bot_postgres_audit",
    }
    if DATABASE_SSLMODE:
        kwargs["sslmode"] = DATABASE_SSLMODE
    return psycopg2.connect(**kwargs), _mask_url(DATABASE_URL)


def _fetch(cur, sql: str, params: Dict[str, Any] | None = None) -> List[Dict[str, Any]]:
    cur.execute(sql, params or {})
    return [dict(row) for row in cur.fetchall()]


def _print_section(name: str, rows: Any) -> None:
    print(f"\n## {name}")
    print(json.dumps(rows, default=str, indent=2))


def _table_time_summary(cur) -> List[Dict[str, Any]]:
    tables = _fetch(
        cur,
        """
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
        ORDER BY table_name
        """,
    )
    summary: List[Dict[str, Any]] = []
    for table_row in tables:
        schema = str(table_row["table_schema"])
        table = str(table_row["table_name"])
        quoted_table = '"' + table.replace('"', '""') + '"'
        row_count = None
        try:
            cur.execute(f"SELECT count(*) AS row_count FROM public.{quoted_table}")
            row_count = int(cur.fetchone()["row_count"])
        except Exception:
            pass

        columns = _fetch(
            cur,
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = %(schema)s AND table_name = %(table)s
            ORDER BY ordinal_position
            """,
            {"schema": schema, "table": table},
        )
        time_cols = [
            str(col["column_name"])
            for col in columns
            if str(col["data_type"]) in {"timestamp without time zone", "timestamp with time zone", "date"}
        ]
        latest: Dict[str, Any] = {}
        for col in time_cols[:3]:
            quoted_col = '"' + col.replace('"', '""') + '"'
            try:
                cur.execute(f"SELECT max({quoted_col}) AS latest FROM public.{quoted_table}")
                latest[col] = cur.fetchone()["latest"]
            except Exception:
                pass
        summary.append({"table": table, "rows": row_count, "time_cols": time_cols[:6], "latest": latest})
    return summary


def _run_audit(include_tables: bool = True) -> None:
    try:
        from psycopg2.extras import RealDictCursor
    except Exception as exc:
        raise SystemExit(f"psycopg2 extras unavailable: {exc}") from exc

    conn, safe_url = _connect()
    with conn:
        conn.set_session(readonly=True, autocommit=True)
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SET statement_timeout = '10s'")
            print(f"safe_database_url={safe_url}")
            _print_section(
                "server",
                _fetch(
                    cur,
                    """
                    SELECT current_database() AS database,
                           current_user AS user_name,
                           inet_server_addr()::text AS host,
                           inet_server_port() AS port,
                           version() AS version,
                           now() AS checked_at
                    """,
                ),
            )
            _print_section(
                "database_size_and_io",
                _fetch(
                    cur,
                    """
                    SELECT datname,
                           pg_size_pretty(pg_database_size(datname)) AS size_pretty,
                           pg_database_size(datname) AS size_bytes,
                           numbackends,
                           xact_commit,
                           xact_rollback,
                           blks_read,
                           blks_hit,
                           tup_returned,
                           tup_fetched,
                           tup_inserted,
                           tup_updated,
                           tup_deleted,
                           deadlocks,
                           temp_files,
                           pg_size_pretty(temp_bytes) AS temp_bytes
                    FROM pg_stat_database
                    WHERE datname = current_database()
                    """,
                ),
            )
            _print_section(
                "connections_by_state",
                _fetch(
                    cur,
                    """
                    SELECT COALESCE(state, 'none') AS state, count(*) AS count
                    FROM pg_stat_activity
                    WHERE datname = current_database()
                    GROUP BY COALESCE(state, 'none')
                    ORDER BY count DESC
                    """,
                ),
            )
            _print_section(
                "active_queries",
                _fetch(
                    cur,
                    """
                    SELECT pid,
                           usename,
                           application_name,
                           client_addr::text AS client_addr,
                           state,
                           wait_event_type,
                           wait_event,
                           round(EXTRACT(epoch FROM now() - query_start)::numeric, 3) AS query_age_s,
                           left(regexp_replace(query, '\\s+', ' ', 'g'), 240) AS query
                    FROM pg_stat_activity
                    WHERE datname = current_database()
                      AND pid <> pg_backend_pid()
                      AND state <> 'idle'
                    ORDER BY query_start NULLS LAST
                    LIMIT 20
                    """,
                ),
            )
            _print_section(
                "waiting_locks",
                _fetch(
                    cur,
                    """
                    SELECT a.pid,
                           a.usename,
                           a.application_name,
                           a.state,
                           l.locktype,
                           l.mode,
                           l.granted,
                           COALESCE(c.relname, '') AS relation,
                           round(EXTRACT(epoch FROM now() - a.query_start)::numeric, 3) AS query_age_s,
                           left(regexp_replace(a.query, '\\s+', ' ', 'g'), 220) AS query
                    FROM pg_locks l
                    JOIN pg_stat_activity a ON a.pid = l.pid
                    LEFT JOIN pg_class c ON c.oid = l.relation
                    WHERE a.datname = current_database()
                      AND NOT l.granted
                    ORDER BY a.query_start NULLS LAST
                    LIMIT 20
                    """,
                ),
            )
            _print_section(
                "largest_tables",
                _fetch(
                    cur,
                    """
                    SELECT schemaname,
                           relname,
                           n_live_tup,
                           n_dead_tup,
                           pg_size_pretty(pg_total_relation_size(relid)) AS total_size,
                           pg_total_relation_size(relid) AS total_bytes,
                           pg_size_pretty(pg_relation_size(relid)) AS table_size,
                           pg_size_pretty(pg_indexes_size(relid)) AS index_size,
                           last_vacuum,
                           last_autovacuum,
                           last_analyze,
                           last_autoanalyze
                    FROM pg_stat_user_tables
                    ORDER BY pg_total_relation_size(relid) DESC
                    LIMIT 25
                    """,
                ),
            )
            _print_section(
                "high_churn_tables",
                _fetch(
                    cur,
                    """
                    SELECT schemaname,
                           relname,
                           seq_scan,
                           idx_scan,
                           n_tup_ins,
                           n_tup_upd,
                           n_tup_del,
                           n_live_tup,
                           n_dead_tup,
                           CASE WHEN n_live_tup > 0
                                THEN round((n_dead_tup::numeric / n_live_tup) * 100, 2)
                                ELSE 0
                           END AS dead_pct
                    FROM pg_stat_user_tables
                    ORDER BY (n_tup_ins + n_tup_upd + n_tup_del) DESC
                    LIMIT 25
                    """,
                ),
            )
            _print_section(
                "index_sizes",
                _fetch(
                    cur,
                    """
                    SELECT schemaname,
                           relname,
                           indexrelname,
                           idx_scan,
                           pg_size_pretty(pg_relation_size(indexrelid)) AS index_size,
                           pg_relation_size(indexrelid) AS index_bytes
                    FROM pg_stat_user_indexes
                    ORDER BY pg_relation_size(indexrelid) DESC
                    LIMIT 25
                    """,
                ),
            )
            try:
                _print_section(
                    "timescale_hypertables",
                    _fetch(
                        cur,
                        """
                        SELECT hypertable_schema,
                               hypertable_name,
                               owner,
                               num_dimensions,
                               num_chunks,
                               compression_enabled
                        FROM timescaledb_information.hypertables
                        ORDER BY hypertable_schema, hypertable_name
                        """,
                    ),
                )
            except Exception as exc:
                _print_section("timescale_hypertables", {"error": str(exc)})
            if include_tables:
                _print_section("public_table_row_and_freshness_summary", _table_time_summary(cur))


def main() -> None:
    parser = argparse.ArgumentParser(description="Read-only PostgreSQL health audit for the trading bot.")
    parser.add_argument("--skip-table-summary", action="store_true", help="Skip per-table row counts/latest timestamps.")
    args = parser.parse_args()
    _run_audit(include_tables=not args.skip_table_summary)


if __name__ == "__main__":
    main()
