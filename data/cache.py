"""data/cache.py — In-memory TTL cache. Required by data/fetcher.py."""
from __future__ import annotations
import json, threading, time
from pathlib import Path
from typing import Any, Optional
from utils.logger import get_logger

logger = get_logger()


class Cache:
    """Thread-safe TTL key-value store."""

    def __init__(self, default_ttl: int = 30, persist_path: Optional[str] = None):
        self._store: dict = {}
        self._lock        = threading.RLock()
        self.default_ttl  = default_ttl
        self._persist_path = Path(persist_path) if persist_path else None
        if self._persist_path and self._persist_path.exists():
            self._load()

    def get(self, key: str) -> Optional[Any]:
        with self._lock:
            entry = self._store.get(key)
            if entry is None:
                return None
            value, expire_at = entry
            if time.time() > expire_at:
                del self._store[key]
                return None
            return value

    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        ttl = ttl if ttl is not None else self.default_ttl
        with self._lock:
            self._store[key] = (value, time.time() + ttl)

    def delete(self, key: str) -> None:
        with self._lock:
            self._store.pop(key, None)

    def clear(self) -> None:
        with self._lock:
            self._store.clear()

    def purge_expired(self) -> int:
        now = time.time()
        with self._lock:
            before = len(self._store)
            self._store = {k: v for k, v in self._store.items() if v[1] > now}
            return before - len(self._store)

    def _load(self) -> None:
        try:
            raw = json.loads(self._persist_path.read_text(encoding="utf-8"))
            now = time.time()
            for k, (v, exp) in raw.items():
                if exp > now:
                    self._store[k] = (v, exp)
        except Exception as e:
            logger.warning(f"[Cache] Load failed: {e}")

    def save(self) -> None:
        if not self._persist_path:
            return
        try:
            self.purge_expired()
            with self._lock:
                data = {k: list(v) for k, v in self._store.items()}
            self._persist_path.write_text(json.dumps(data), encoding="utf-8")
        except Exception as e:
            logger.warning(f"[Cache] Save failed: {e}")

    def __len__(self) -> int:
        self.purge_expired()
        with self._lock:
            return len(self._store)


cache = Cache(default_ttl=30)