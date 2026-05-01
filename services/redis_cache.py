from __future__ import annotations
import json
from typing import Any, Optional
from config.config import (
    REDIS_CACHE_MAX_VALUE_BYTES,
    REDIS_CACHE_PREFIX,
    REDIS_CACHE_TTL_CAP_SECONDS,
    REDIS_OBJECT_CACHE_ENABLED,
)
from utils.logger import get_logger

logger = get_logger()


class RedisCache:
    """Redis-backed cache. Uses shared connection pool."""

    def __init__(self, default_ttl: int = 30, prefix: str = REDIS_CACHE_PREFIX):
        from services.redis_pool import get_client as _get_redis_client
        self._r   = _get_redis_client()
        self._ttl = max(1, min(int(default_ttl), int(REDIS_CACHE_TTL_CAP_SECONDS)))
        self._prefix = self._normalise_prefix(prefix)
        self._max_value_bytes = max(1024, int(REDIS_CACHE_MAX_VALUE_BYTES))
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
            raw = json.dumps(value, separators=(",", ":"))
            raw_size = len(raw.encode("utf-8"))
            if raw_size > self._max_value_bytes:
                logger.debug(
                    f"[Cache] Redis object cache skipped for {key}: "
                    f"payload_too_large ({raw_size}>{self._max_value_bytes} bytes)"
                )
                return
            bounded_ttl = max(1, min(int(ttl or self._ttl), int(REDIS_CACHE_TTL_CAP_SECONDS)))
            self._r.set(self._full_key(key), raw, ex=bounded_ttl)
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
    Returns RedisCache only when explicitly enabled. The free Redis Cloud tier is
    small, so object caching defaults to local memory while Redis remains
    available for pub/sub and small live-state coordination.
    """
    if not REDIS_OBJECT_CACHE_ENABLED:
        from data.cache import Cache
        return Cache(default_ttl=default_ttl)
    try:
        return RedisCache(default_ttl=default_ttl)
    except Exception as e:
        logger.warning(f"[Cache] Redis unavailable ({e}) — using in-process cache")
        from data.cache import Cache
        return Cache(default_ttl=default_ttl)
