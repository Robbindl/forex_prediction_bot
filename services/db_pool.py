"""
services/db_pool.py — Thread-safe DatabaseService singleton.
No NullDB fallback — database is required.
"""
from __future__ import annotations
import threading
from utils.logger import get_logger

logger   = get_logger()
_lock    = threading.Lock()
_instance = None


def get_db() -> "DatabaseService":
    """Return the shared DatabaseService. Creates on first call."""
    global _instance
    if _instance is not None:
        return _instance
    with _lock:
        if _instance is None:
            from services.database_service import DatabaseService
            _instance = DatabaseService()
            logger.info("[DB Pool] DatabaseService singleton created")
    return _instance