from __future__ import annotations

import threading
import time
from typing import Optional

from utils.logger import get_logger

logger = get_logger()

# ── Pool singleton ────────────────────────────────────────────────────────────
_pool       = None
_client     = None
_pool_lock  = threading.Lock()
_available  = False
_next_retry_at = 0.0
_last_unavailable_log = 0.0
_last_unavailable_message = ""


def _build_pool():
    """Build the connection pool once. Thread-safe."""
    global _pool, _client, _available, _next_retry_at, _last_unavailable_log, _last_unavailable_message
    now = time.monotonic()
    if now < _next_retry_at:
        return
    try:
        import redis
        from config.config import REDIS_MAX_CONNECTIONS, REDIS_URL

        max_connections = max(1, int(REDIS_MAX_CONNECTIONS))
        pool = redis.ConnectionPool.from_url(
            REDIS_URL,
            max_connections=max_connections,
            socket_connect_timeout=3,
            socket_timeout=3,
            retry_on_timeout=True,
            socket_keepalive=True,
            health_check_interval=30,
            decode_responses=True,
        )
        # Test the pool
        client = redis.Redis(connection_pool=pool)
        client.ping()
        _pool      = pool
        _client    = client
        _available = True
        _next_retry_at = 0.0
        _last_unavailable_message = ""
        logger.info(
            f"[RedisPool] Connected — max_connections={max_connections}  "
            f"url={REDIS_URL[:40]}…"
        )
    except Exception as e:
        _pool      = None
        _client    = None
        _available = False
        message = str(e)
        _next_retry_at = now + 10.0
        if message != _last_unavailable_message or (now - _last_unavailable_log) >= 60:
            logger.warning(f"[RedisPool] Unavailable ({message}) — all Redis ops will no-op")
            _last_unavailable_log = now
            _last_unavailable_message = message
        else:
            logger.debug(f"[RedisPool] Unavailable ({message}) — all Redis ops will no-op")


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
        return _client
    except Exception:
        return None


def get_dedicated_client(*, socket_timeout=None):
    """
    Return an independent Redis client for blocking or command-bridge work.

    The shared pool is intentionally small so dashboard refreshes cannot grow
    without bound. Long-lived BLPOP listeners should not consume that pool.
    """
    try:
        import redis
        from config.config import REDIS_URL

        client = redis.from_url(
            REDIS_URL,
            socket_connect_timeout=3,
            socket_timeout=socket_timeout,
            health_check_interval=30,
            decode_responses=True,
        )
        client.ping()
        return client
    except Exception as e:
        logger.debug(f"[RedisPool] dedicated client unavailable: {e}")
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
