from __future__ import annotations

import threading
import time
from collections import defaultdict, deque
from typing import Any, Deque, Dict, Optional, Tuple


def _clip(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, float(value or 0.0)))


def _normalize_provider(value: Any) -> str:
    token = str(value or "").strip().lower()
    if token.startswith("ig"):
        return "ig"
    if token.startswith("deriv"):
        return "deriv"
    if token.startswith("binance"):
        return "binance"
    if token.startswith("bybit"):
        return "bybit"
    if token.startswith("okx"):
        return "okx"
    return token or "unknown"


def _normalize_asset(value: Any) -> str:
    return str(value or "").strip().upper()


class DomStreamHealthService:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._provider_state: Dict[str, Dict[str, Any]] = {}
        self._asset_state: Dict[Tuple[str, str], Dict[str, Any]] = {}

    @staticmethod
    def _empty_state() -> Dict[str, Any]:
        return {
            "connected": False,
            "degraded": False,
            "last_message_ts": 0.0,
            "last_depth_ts": 0.0,
            "last_trade_ts": 0.0,
            "reconnect_events": deque(maxlen=32),
            "sequence_gap_events": deque(maxlen=16),
            "last_reason": "",
        }

    def _provider_bucket(self, provider: str) -> Dict[str, Any]:
        key = _normalize_provider(provider)
        bucket = self._provider_state.get(key)
        if bucket is None:
            bucket = self._empty_state()
            self._provider_state[key] = bucket
        return bucket

    def _asset_bucket(self, provider: str, asset: str) -> Dict[str, Any]:
        key = (_normalize_provider(provider), _normalize_asset(asset))
        bucket = self._asset_state.get(key)
        if bucket is None:
            bucket = self._empty_state()
            self._asset_state[key] = bucket
        return bucket

    @staticmethod
    def _record_event(queue: Deque[float], ts: Optional[float] = None) -> None:
        queue.append(float(ts or time.time()))

    @staticmethod
    def _recent_count(queue: Deque[float], *, window_seconds: float, now_ts: float) -> int:
        return sum(1 for item in list(queue) if now_ts - float(item or 0.0) <= window_seconds)

    def mark_connected(self, provider: str, asset: str = "", *, ts: Optional[float] = None) -> None:
        event_ts = float(ts or time.time())
        with self._lock:
            provider_bucket = self._provider_bucket(provider)
            provider_bucket["connected"] = True
            provider_bucket["degraded"] = False
            provider_bucket["last_message_ts"] = event_ts
            provider_bucket["last_reason"] = ""
            if asset:
                asset_bucket = self._asset_bucket(provider, asset)
                asset_bucket["connected"] = True
                asset_bucket["degraded"] = False
                asset_bucket["last_message_ts"] = event_ts
                asset_bucket["last_reason"] = ""

    def mark_disconnected(
        self,
        provider: str,
        asset: str = "",
        *,
        ts: Optional[float] = None,
        degraded: bool = True,
        reason: str = "",
        reconnect: bool = False,
    ) -> None:
        event_ts = float(ts or time.time())
        reason_text = str(reason or "").strip()
        with self._lock:
            provider_bucket = self._provider_bucket(provider)
            provider_bucket["connected"] = False
            provider_bucket["degraded"] = bool(degraded)
            provider_bucket["last_reason"] = reason_text
            if reconnect:
                self._record_event(provider_bucket["reconnect_events"], event_ts)
            if asset:
                asset_bucket = self._asset_bucket(provider, asset)
                asset_bucket["connected"] = False
                asset_bucket["degraded"] = bool(degraded)
                asset_bucket["last_reason"] = reason_text
                if reconnect:
                    self._record_event(asset_bucket["reconnect_events"], event_ts)

    def note_depth(self, provider: str, asset: str, *, ts: Optional[float] = None) -> None:
        event_ts = float(ts or time.time())
        with self._lock:
            provider_bucket = self._provider_bucket(provider)
            provider_bucket["connected"] = True
            provider_bucket["degraded"] = False
            provider_bucket["last_message_ts"] = event_ts
            provider_bucket["last_depth_ts"] = event_ts
            asset_bucket = self._asset_bucket(provider, asset)
            asset_bucket["connected"] = True
            asset_bucket["degraded"] = False
            asset_bucket["last_message_ts"] = event_ts
            asset_bucket["last_depth_ts"] = event_ts

    def note_trade(self, provider: str, asset: str, *, ts: Optional[float] = None) -> None:
        event_ts = float(ts or time.time())
        with self._lock:
            provider_bucket = self._provider_bucket(provider)
            provider_bucket["connected"] = True
            provider_bucket["degraded"] = False
            provider_bucket["last_message_ts"] = event_ts
            provider_bucket["last_trade_ts"] = event_ts
            asset_bucket = self._asset_bucket(provider, asset)
            asset_bucket["connected"] = True
            asset_bucket["degraded"] = False
            asset_bucket["last_message_ts"] = event_ts
            asset_bucket["last_trade_ts"] = event_ts

    def note_sequence_gap(self, provider: str, asset: str = "", *, ts: Optional[float] = None, reason: str = "") -> None:
        event_ts = float(ts or time.time())
        reason_text = str(reason or "sequence_gap").strip()
        with self._lock:
            provider_bucket = self._provider_bucket(provider)
            provider_bucket["degraded"] = True
            provider_bucket["last_reason"] = reason_text
            self._record_event(provider_bucket["sequence_gap_events"], event_ts)
            if asset:
                asset_bucket = self._asset_bucket(provider, asset)
                asset_bucket["degraded"] = True
                asset_bucket["last_reason"] = reason_text
                self._record_event(asset_bucket["sequence_gap_events"], event_ts)

    def snapshot(self, provider: str, asset: str = "") -> Dict[str, Any]:
        provider_key = _normalize_provider(provider)
        asset_key = _normalize_asset(asset)
        now_ts = time.time()
        with self._lock:
            provider_known = provider_key in self._provider_state
            asset_known = (provider_key, asset_key) in self._asset_state
            provider_bucket = dict(self._provider_state.get(provider_key) or self._empty_state())
            asset_bucket = dict(self._asset_state.get((provider_key, asset_key)) or self._empty_state())
        if not provider_known and not asset_known:
            return {
                "dom_stream_health_known": False,
                "dom_stream_connected": False,
                "dom_stream_degraded": False,
                "dom_stream_health_score": 1.0,
                "dom_stream_trust_decay": 0.0,
                "dom_stream_reconnect_count": 0,
                "dom_stream_sequence_gap_count": 0,
                "dom_stream_last_message_age_seconds": None,
                "dom_depth_stream_age_seconds": None,
                "dom_trade_stream_age_seconds": None,
                "dom_depth_stream_missing": False,
                "dom_trade_stream_missing": False,
                "dom_stream_reason": "",
            }
        reconnect_provider = provider_bucket.get("reconnect_events") or deque()
        reconnect_asset = asset_bucket.get("reconnect_events") or deque()
        gap_provider = provider_bucket.get("sequence_gap_events") or deque()
        gap_asset = asset_bucket.get("sequence_gap_events") or deque()
        reconnect_count = max(
            self._recent_count(reconnect_provider, window_seconds=900.0, now_ts=now_ts),
            self._recent_count(reconnect_asset, window_seconds=900.0, now_ts=now_ts),
        )
        sequence_gap_count = max(
            self._recent_count(gap_provider, window_seconds=1800.0, now_ts=now_ts),
            self._recent_count(gap_asset, window_seconds=1800.0, now_ts=now_ts),
        )
        connected = bool(asset_bucket.get("connected")) if asset_key else bool(provider_bucket.get("connected"))
        degraded = bool(provider_bucket.get("degraded")) or bool(asset_bucket.get("degraded"))
        last_message_ts = max(
            float(provider_bucket.get("last_message_ts", 0.0) or 0.0),
            float(asset_bucket.get("last_message_ts", 0.0) or 0.0),
        )
        last_depth_ts = max(
            float(provider_bucket.get("last_depth_ts", 0.0) or 0.0),
            float(asset_bucket.get("last_depth_ts", 0.0) or 0.0),
        )
        last_trade_ts = max(
            float(provider_bucket.get("last_trade_ts", 0.0) or 0.0),
            float(asset_bucket.get("last_trade_ts", 0.0) or 0.0),
        )
        last_message_age = max(0.0, now_ts - last_message_ts) if last_message_ts > 0.0 else 9999.0
        depth_age = max(0.0, now_ts - last_depth_ts) if last_depth_ts > 0.0 else 9999.0
        trade_age = max(0.0, now_ts - last_trade_ts) if last_trade_ts > 0.0 else 9999.0

        depth_missing = bool(last_depth_ts <= 0.0 or depth_age > 30.0)
        trade_missing = bool(last_trade_ts <= 0.0 or trade_age > 45.0)
        score = 1.0
        if not connected:
            score *= 0.55 if last_message_age <= 45.0 else 0.20
        if degraded:
            score -= 0.18
        if last_message_age > 12.0:
            score -= min(0.18, (last_message_age - 12.0) / 60.0)
        if depth_missing:
            score -= 0.24
        if trade_missing:
            score -= 0.12
        score -= min(0.22, reconnect_count * 0.06)
        score -= min(0.20, sequence_gap_count * 0.10)
        health_score = _clip(score)
        trust_decay = _clip(1.0 - health_score, 0.0, 0.80)
        return {
            "dom_stream_health_known": True,
            "dom_stream_connected": connected,
            "dom_stream_degraded": degraded,
            "dom_stream_health_score": round(health_score, 4),
            "dom_stream_trust_decay": round(trust_decay, 4),
            "dom_stream_reconnect_count": int(reconnect_count),
            "dom_stream_sequence_gap_count": int(sequence_gap_count),
            "dom_stream_last_message_age_seconds": round(last_message_age, 3) if last_message_ts > 0.0 else None,
            "dom_depth_stream_age_seconds": round(depth_age, 3) if last_depth_ts > 0.0 else None,
            "dom_trade_stream_age_seconds": round(trade_age, 3) if last_trade_ts > 0.0 else None,
            "dom_depth_stream_missing": depth_missing,
            "dom_trade_stream_missing": trade_missing,
            "dom_stream_reason": str(asset_bucket.get("last_reason") or provider_bucket.get("last_reason") or ""),
        }

    def clear(self) -> None:
        with self._lock:
            self._provider_state.clear()
            self._asset_state.clear()


_service = DomStreamHealthService()


def get_service() -> DomStreamHealthService:
    return _service
