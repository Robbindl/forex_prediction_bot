"""
config/database.py — PostgreSQL connection. Required — bot will not start without it.
"""
from __future__ import annotations
import time
from sqlalchemy.engine import make_url
from sqlalchemy import create_engine, text
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker
from utils.logger import get_logger
from config.config import (
    DATABASE_URL,
    DATABASE_SSLMODE,
    DB_CONNECT_RETRIES,
    DB_MAX_OVERFLOW,
    DB_POOL_RECYCLE_SECONDS,
    DB_POOL_SIZE,
    DB_RETRY_DELAY_SECONDS,
)

logger = get_logger()

Base = declarative_base()


def _ensure_varchar_min_length(conn, table_name: str, column_name: str, min_length: int) -> None:
    row = conn.execute(
        text(
            """
            SELECT character_maximum_length
            FROM information_schema.columns
            WHERE table_schema = current_schema()
              AND table_name = :table_name
              AND column_name = :column_name
            """
        ),
        {"table_name": table_name, "column_name": column_name},
    ).first()
    if not row:
        return
    current_length = row[0]
    if current_length is None or int(current_length) >= int(min_length):
        return
    conn.execute(
        text(f'ALTER TABLE "{table_name}" ALTER COLUMN "{column_name}" TYPE VARCHAR({int(min_length)})')
    )
    logger.info(
        f"[DB] Migrated {table_name}.{column_name} VARCHAR({current_length}) -> VARCHAR({int(min_length)})"
    )


def _sync_runtime_schema() -> None:
    with engine.connect() as conn:
        _ensure_varchar_min_length(conn, "trades", "strategy_id", 80)
        _ensure_varchar_min_length(conn, "trades", "exit_reason", 100)
        _ensure_varchar_min_length(conn, "open_positions", "strategy_id", 80)
        conn.commit()


def _redacted_database_url(url: str) -> str:
    try:
        return make_url(url).render_as_string(hide_password=True)
    except Exception:
        return "<invalid DATABASE_URL>"


def _database_target(url: str) -> str:
    try:
        parsed = make_url(url)
        host = parsed.host or "localhost"
        port = parsed.port or 5432
        database = parsed.database or "unknown"
        return f"{host}:{port}/{database}"
    except Exception:
        return "<invalid target>"


def _normalize_database_url(url: str) -> str:
    """
    Return a connection string with a safe SSL mode when the database target
    is Azure PostgreSQL and the caller did not specify one explicitly.
    """
    try:
        parsed = make_url(url)
    except Exception:
        return url

    query = dict(parsed.query or {})
    sslmode = (DATABASE_SSLMODE or query.get("sslmode") or "").strip()
    host = (parsed.host or "").lower()

    if not sslmode and host.endswith(".postgres.database.azure.com"):
        sslmode = "require"

    if not sslmode:
        return url

    if query.get("sslmode") == sslmode:
        return url

    query["sslmode"] = sslmode
    return parsed.set(query=query).render_as_string(hide_password=False)


def create_db_engine(
    max_retries: int = DB_CONNECT_RETRIES,
    retry_delay: int = DB_RETRY_DELAY_SECONDS,
):
    """
    Connect to PostgreSQL. Retries max_retries times.
    Raises RuntimeError if all attempts fail — this is intentional.
    The bot requires a database to run.
    """
    if not DATABASE_URL:
        raise RuntimeError(
            "[DB] DATABASE_URL is missing in .env.\n"
            "Bot cannot start without a database connection."
        )

    normalized_database_url = _normalize_database_url(DATABASE_URL)
    db_target = _database_target(normalized_database_url)
    for attempt in range(1, max_retries + 1):
        try:
            engine = create_engine(
                normalized_database_url,
                pool_size=DB_POOL_SIZE,
                max_overflow=DB_MAX_OVERFLOW,
                pool_pre_ping=True,
                pool_recycle=DB_POOL_RECYCLE_SECONDS,
                echo=False,
            )
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info(f"[DB] Connected to PostgreSQL — {db_target}")
            return engine
        except Exception as e:
            logger.warning(f"[DB] Connection attempt {attempt}/{max_retries} failed: {e}")
            if attempt < max_retries:
                logger.info(f"[DB] Retrying in {retry_delay}s...")
                time.sleep(retry_delay)

    raise RuntimeError(
        f"[DB] Could not connect to PostgreSQL after {max_retries} attempts.\n"
        f"Check DATABASE_URL in your .env file (current target: {db_target}).\n"
        f"Redacted URL: {_redacted_database_url(DATABASE_URL)}\n"
        "Bot cannot start without a database connection."
    )


engine       = create_db_engine()
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def init_db() -> None:
    """Create all tables if they don't exist."""
    # FIX CRITICAL: Previously imported MemorableMoments and HumanExplanations
    # which were believed to be missing — they ARE in trade_models.py (verified).
    # All imports are valid. No crash on startup.
    from models.trade_models import (          # noqa: F401 — side-effect imports
        Trade, OpenPosition, DailyStats,
        TradingDiary, BotPersonality, WhaleAlert,
        MemorableMoments, HumanExplanations,
    )
    Base.metadata.create_all(bind=engine)
    _sync_runtime_schema()
    logger.info("[DB] All tables created / verified")

    # Create whale tables (raw SQL DDL)
    from whale_intelligence.wallet_database import (
        _CREATE_WALLETS, _CREATE_BALANCES, _CREATE_MOVEMENTS, _CREATE_PROFILES
    )
    with engine.connect() as conn:
        conn.execute(text(_CREATE_WALLETS))
        conn.execute(text(_CREATE_BALANCES))
        conn.execute(text(_CREATE_MOVEMENTS))
        conn.execute(text(_CREATE_PROFILES))
        conn.commit()
    logger.info("[DB] Whale tables created / verified")

    # Create strategy tables through the shared DB service so DDL stays in one place.
    from services.db_pool import get_db as get_database_service
    get_database_service().ensure_strategy_reporting_tables()
    logger.info("[DB] Strategy tables created / verified")


def get_db():
    """
    Return a new SQLAlchemy session for use in Flask request handlers.

    FIX HIGH: The previous implementation was a generator (yield db) designed
    for FastAPI dependency injection. In Flask, callers used get_db() directly
    as an object — calling next(get_db()) without exhausting it leaks the DB
    session. This version returns a plain SessionLocal() that the caller closes.

    Usage:
        db = get_db()
        try:
            db.save_trade(...)
        finally:
            db.close()

    Or as a context manager (SessionLocal supports __enter__/__exit__):
        with get_db() as db:
            db.save_trade(...)
    """
    return SessionLocal()
