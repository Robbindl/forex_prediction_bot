"""
config/database.py — PostgreSQL connection. Required — bot will not start without it.
"""
from __future__ import annotations
import time
from sqlalchemy import create_engine, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from utils.logger import get_logger
from config.config import DATABASE_URL

logger = get_logger()

Base = declarative_base()


def create_db_engine(max_retries: int = 5, retry_delay: int = 3):
    """
    Connect to PostgreSQL. Retries max_retries times.
    Raises RuntimeError if all attempts fail — this is intentional.
    The bot requires a database to run.
    """
    for attempt in range(1, max_retries + 1):
        try:
            engine = create_engine(
                DATABASE_URL,
                pool_size=10,
                max_overflow=20,
                pool_pre_ping=True,
                pool_recycle=3600,
                echo=False,
            )
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info(f"[DB] Connected to PostgreSQL — {DATABASE_URL.split('@')[-1]}")
            return engine
        except Exception as e:
            logger.warning(f"[DB] Connection attempt {attempt}/{max_retries} failed: {e}")
            if attempt < max_retries:
                time.sleep(retry_delay)

    raise RuntimeError(
        f"[DB] Could not connect to PostgreSQL after {max_retries} attempts.\n"
        f"Check DATABASE_URL in your .env file: {DATABASE_URL}\n"
        "Bot cannot start without a database connection."
    )


engine       = create_db_engine()
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def init_db() -> None:
    """Create all tables if they don't exist."""
    from models.trade_models import (          # noqa: F401 — side-effect import
        Trade, TradingDiary, BotPersonality,
        MemorableMoments, HumanExplanations, WhaleAlert,
        OpenPosition, DailyStats,
    )
    Base.metadata.create_all(bind=engine)
    logger.info("[DB] All tables created / verified")


def get_db():
    """FastAPI / Flask dependency-style session generator."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()