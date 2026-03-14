"""
services/redis_cache.py — Redis-backed cache with in-memory fallback.

Drop-in upgrade for data/cache.py. If Redis is unavailable the system
continues using the in-process TTL cache — no crash, no data loss.

Usage:
    from services.redis_cache import get_cache
    cache = get_cache()
    cache.set("key", value, ttl=30)
    value = cache.get("key")
"""
from __future__ import annotations
import json
import os
from typing import Any, Optional
from utils.logger import get_logger

logger = get_logger()

_REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")


class RedisCache:
    """Redis-backed cache. Serialises values as JSON."""

    def __init__(self, url: str = _REDIS_URL, default_ttl: int = 30):
        import redis
        self._r = redis.from_url(url, decode_responses=True, socket_timeout=1.0)
        self._ttl = default_ttl
        self._r.ping()
        logger.info(f"[Cache] Redis connected at {url}")

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
        try:
            self._r.flushdb()
        except Exception:
            pass

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