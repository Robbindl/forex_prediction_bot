"""
services/db_pool.py — Thread-safe DatabaseService singleton.

Usage:
    from services.db_pool import get_db

    db = get_db()   # always returns the same instance
    db.save_trade(...)

All components (PaperTrader, PaperTrade, MLEngine, etc.) should call
get_db() instead of constructing a new DatabaseService().
"""

import threading
from logger import logger

_db_instance = None
_db_lock = threading.Lock()


def get_db():
    """
    Return the shared DatabaseService instance.
    Creates it on first call (thread-safe).
    """
    global _db_instance
    if _db_instance is not None:
        return _db_instance
    with _db_lock:
        if _db_instance is None:
            try:
                from services.database_service import DatabaseService
                _db_instance = DatabaseService()
                logger.info("✅ DB pool: shared DatabaseService created")
            except Exception as e:
                logger.warning(f"⚠️ DB pool: DatabaseService unavailable ({e}) — using None")
                _db_instance = _NullDB()
    return _db_instance


class _NullDB:
    """
    Drop-in replacement when DB is unavailable.
    All methods are no-ops that return safe defaults.
    """
    use_db = False

    def save_trade(self, *a, **kw):
        return None

    def get_trade(self, *a, **kw):
        return None

    def get_all_trades(self, *a, **kw):
        return []

    def update_trade(self, *a, **kw):
        return None

    def delete_trade(self, *a, **kw):
        return None

    def get_stats(self, *a, **kw):
        return {}

    def __getattr__(self, name):
        """Swallow any unknown method call."""
        def _noop(*a, **kw):
            return None
        return _noop