"""
services/redis_pool.py — Central Redis connection pool.

Single source of truth for ALL Redis connections in the platform.
Every module imports from here instead of calling redis.from_url() directly.

Why this matters
----------------
Redis Cloud free tier allows 30 connections max.
Before this module, the bot opened 27+ individual connections — one per module.
Now the entire platform uses a shared pool of max_connections=10, leaving
plenty of headroom for VPS scaling.

Connection budget (total: ~14 connections)
------------------------------------------
    Pool (shared by all publishers + cache):  10  connections
    Subscriber connections (pub/sub):          4  connections (one per subscriber loop)
    ─────────────────────────────────────────────
    Total max:                                14  connections  (47% of 30 limit)

Usage
-----
    # Get a publish/cache connection (returns from pool, auto-releases)
    from services.redis_pool import get_client
    r = get_client()
    r.publish("MY_CHANNEL", json.dumps(data))
    r.set("key", value, ex=30)

    # Get a dedicated pubsub connection (for subscriber loops only)
    from services.redis_pool import get_pubsub
    ps = get_pubsub()
    ps.subscribe("CHANNEL_A", "CHANNEL_B")
    for msg in ps.listen():
        ...

    # Check if Redis is available
    from services.redis_pool import is_available
    if is_available():
        ...
"""
from __future__ import annotations

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
        pool = redis.ConnectionPool.from_url(
            REDIS_URL,
            max_connections=10,
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
            f"[RedisPool] Connected — max_connections=10  "
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


def get_pubsub():
    """
    Return a dedicated pubsub object for subscriber loops.
    Each call creates ONE new connection outside the shared pool.
    Only call this once per subscriber thread — not on every loop iteration.

    Returns None if Redis is unavailable.
    """
    _ensure_pool()
    if not _available:
        return None
    try:
        import redis
        from config.config import REDIS_URL
        # Dedicated connection for this subscriber — not from pool
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
