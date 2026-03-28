from __future__ import annotations

import os
import threading
from typing import Optional

from utils.logger import get_logger

logger = get_logger()

# ── Pool singleton ────────────────────────────────────────────────────────────
_pool       = None
_pool_lock  = threading.Lock()
_available  = False


def _build_pool():
    """Build the connection pool once. Thread-safe."""
    global _pool, _available
    try:
        import redis
        from config.config import REDIS_URL

        # max_connections=10 — shared by all publishers + cache
        # Each publish/get/set borrows a connection from the pool
        # and releases it immediately after — so 10 connections
        # handles hundreds of concurrent publish calls efficiently.
        max_connections = int(os.environ.get("REDIS_MAX_CONNECTIONS", "50"))
        pool = redis.ConnectionPool.from_url(
            REDIS_URL,
            max_connections=max_connections,
            socket_connect_timeout=3,
            socket_timeout=3,
            retry_on_timeout=True,
            decode_responses=True,
        )
        # Test the pool
        client = redis.Redis(connection_pool=pool)
        client.ping()
        _pool      = pool
        _available = True
        logger.info(
            f"[RedisPool] Connected — max_connections={max_connections}  "
            f"url={REDIS_URL[:40]}…"
        )
    except Exception as e:
        _pool      = None
        _available = False
        logger.warning(f"[RedisPool] Unavailable ({e}) — all Redis ops will no-op")


def _ensure_pool() -> None:
    global _pool
    if _pool is not None:
        return
    with _pool_lock:
        if _pool is None:
            _build_pool()


# ── Public API ────────────────────────────────────────────────────────────────

def get_client():
    """
    Return a Redis client backed by the shared pool.
    Use for: publish, get, set, setex, delete.
    The connection is borrowed from the pool and returned automatically
    when the call completes — no need to close it.

    Returns None if Redis is unavailable.
    """
    _ensure_pool()
    if _pool is None:
        return None
    try:
        import redis
        return redis.Redis(connection_pool=_pool)
    except Exception:
        return None


def get_pubsub(old_pubsub=None):
    """
    Return a dedicated pubsub object for subscriber loops.
    Each call creates ONE new connection outside the shared pool.
    Only call this once per subscriber thread — not on every loop iteration.

    Pass old_pubsub to close the previous connection before creating a new one.
    This prevents zombie connections from accumulating when subscribers reconnect.

    Returns None if Redis is unavailable.
    """
    # Close old connection first — prevents zombie connections
    if old_pubsub is not None:
        try:
            old_pubsub.close()
            old_pubsub.connection.disconnect()
        except Exception:
            pass

    _ensure_pool()
    if not _available:
        return None
    try:
        import redis
        from config.config import REDIS_URL
        r = redis.from_url(
            REDIS_URL,
            socket_connect_timeout=3,
            socket_timeout=None,   # blocking listen() needs no timeout
            decode_responses=True,
        )
        return r.pubsub()
    except Exception as e:
        logger.debug(f"[RedisPool] pubsub connection failed: {e}")
        return None


def is_available() -> bool:
    """Return True if Redis is reachable."""
    _ensure_pool()
    return _available


def ping() -> bool:
    """Ping Redis. Returns True if alive."""
    try:
        c = get_client()
        return bool(c and c.ping())
    except Exception:
        return False


# ── Initialise on import ──────────────────────────────────────────────────────
_ensure_pool()
