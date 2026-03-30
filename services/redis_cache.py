from __future__ import annotations
import json
from typing import Any, Optional
from config.config import REDIS_CACHE_PREFIX
from utils.logger import get_logger

logger = get_logger()


class RedisCache:
    """Redis-backed cache. Uses shared connection pool."""

    def __init__(self, default_ttl: int = 30, prefix: str = REDIS_CACHE_PREFIX):
        from services.redis_pool import get_client as _get_redis_client
        self._r   = _get_redis_client()
        self._ttl = default_ttl
        self._prefix = self._normalise_prefix(prefix)
        if self._r:
            self._r.ping()
            logger.info(f"[Cache] Redis pool connected (prefix={self._prefix})")

    @staticmethod
    def _normalise_prefix(prefix: str) -> str:
        value = str(prefix or "trading_bot:cache:").strip()
        return value if value.endswith(":") else value + ":"

    def _full_key(self, key: str) -> str:
        return f"{self._prefix}{key}"

    def get(self, key: str) -> Optional[Any]:
        try:
            raw = self._r.get(self._full_key(key))
            return json.loads(raw) if raw is not None else None
        except Exception:
            return None

    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        try:
            self._r.set(self._full_key(key), json.dumps(value), ex=ttl or self._ttl)
        except Exception:
            pass

    def delete(self, key: str) -> None:
        try:
            self._r.delete(self._full_key(key))
        except Exception:
            pass

    def clear(self) -> None:
        # Never scan/delete the whole DB. Only clear namespaced cache keys.
        try:
            cursor = 0
            pattern = f"{self._prefix}*"
            while True:
                cursor, keys = self._r.scan(cursor, match=pattern, count=100)
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
            return bool(self._r.exists(self._full_key(key)))
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
