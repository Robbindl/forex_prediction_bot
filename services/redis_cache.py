from __future__ import annotations
import json
import os
from typing import Any, Optional
from utils.logger import get_logger

logger = get_logger()

_REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")


class RedisCache:
    """Redis-backed cache. Uses shared connection pool."""

    def __init__(self, url: str = _REDIS_URL, default_ttl: int = 30):
        from services.redis_pool import get_client as _get_redis_client
        self._r   = _get_redis_client()
        self._ttl = default_ttl
        if self._r:
            self._r.ping()
            logger.info(f"[Cache] Redis pool connected")

    def get(self, key: str) -> Optional[Any]:
        try:
            raw = self._r.get(key)
            return json.loads(raw) if raw is not None else None
        except Exception:
            return None

    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        try:
            self._r.set(key, json.dumps(value), ex=ttl or self._ttl)
        except Exception:
            pass

    def delete(self, key: str) -> None:
        try:
            self._r.delete(key)
        except Exception:
            pass

    def clear(self) -> None:
        # FIX S14: flushdb() wipes the ENTIRE Redis database — this would
        # destroy all pub/sub channels, live price ticks, open positions cache,
        # ML predictions, and every other subsystem sharing the same Redis
        # instance.  Replace with a targeted key scan so only cache entries
        # (written by this class via set()) are removed.
        try:
            # Scan for all keys; delete in batches of 100 to avoid blocking
            cursor = 0
            while True:
                cursor, keys = self._r.scan(cursor, count=100)
                if keys:
                    self._r.delete(*keys)
                if cursor == 0:
                    break
        except Exception as e:
            logger.debug(f"[Cache] clear error: {e}")

    def purge_expired(self) -> int:
        return 0  # Redis handles expiry natively

    def __contains__(self, key: str) -> bool:
        try:
            return bool(self._r.exists(key))
        except Exception:
            return False


def get_cache(default_ttl: int = 30):
    """
    Returns a RedisCache if Redis is reachable, otherwise falls back
    to the existing in-process TTL cache. Caller code never changes.
    """
    try:
        return RedisCache(default_ttl=default_ttl)
    except Exception as e:
        logger.warning(f"[Cache] Redis unavailable ({e}) — using in-process cache")
        from data.cache import Cache
        return Cache(default_ttl=default_ttl)