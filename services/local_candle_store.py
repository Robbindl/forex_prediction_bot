from __future__ import annotations

import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import pandas as pd

from config.config import LOCAL_CANDLE_STORE_ENABLED, LOCAL_CANDLE_STORE_PATH
from core.assets import registry
from utils.logger import get_logger

logger = get_logger()

_INTERVAL_SECONDS = {
    "1m": 60,
    "5m": 300,
    "15m": 900,
    "30m": 1800,
    "1h": 3600,
    "4h": 14400,
    "1d": 86400,
}

_RESAMPLE_RULES = {
    "5m": "5min",
    "15m": "15min",
    "30m": "30min",
    "1h": "1h",
    "4h": "4h",
    "1d": "1d",
}


def _utc_now_ts() -> int:
    return int(datetime.now(timezone.utc).timestamp())


def _normalize_interval(value: str) -> str:
    return str(value or "").strip().lower()


def _canonical_asset(asset: str) -> str:
    return registry.canonical(str(asset or "").strip())


def _category_for(asset: str, category: str = "") -> str:
    resolved = str(category or "").strip().lower()
    if resolved:
        return resolved
    return registry.category(asset)


def _provider_family(value: Any) -> str:
    token = str(value or "").strip().upper()
    if token.startswith("IG"):
        return "IG"
    if token.startswith("DERIV"):
        return "DERIV"
    if token.startswith("BINANCE"):
        return "BINANCE"
    if token.startswith("DUKASCOPY"):
        return "DUKASCOPY"
    if token.startswith("FMP"):
        return "FMP"
    return token or "UNKNOWN"


def _coerce_cutoff(value: Any) -> pd.Timestamp:
    ts = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(ts):
        return pd.Timestamp(datetime.now(timezone.utc))
    return pd.Timestamp(ts)


def _bucket_epoch(timestamp: float, interval_seconds: int) -> int:
    return int(timestamp // interval_seconds) * interval_seconds


class LocalCandleStore:
    """Persistent local OHLCV store fed by provider history and live prices."""

    def __init__(self, *, enabled: Optional[bool] = None, path: Optional[Path | str] = None) -> None:
        self._enabled = LOCAL_CANDLE_STORE_ENABLED if enabled is None else bool(enabled)
        self._path = Path(path or LOCAL_CANDLE_STORE_PATH)
        self._lock = threading.Lock()
        self._conn: Optional[sqlite3.Connection] = None
        if self._enabled:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            self._ensure_schema()

    def enabled(self) -> bool:
        return bool(self._enabled)

    def _connection(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(str(self._path), check_same_thread=False, timeout=30)
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=NORMAL")
            self._conn.execute("PRAGMA temp_store=MEMORY")
        return self._conn

    def _ensure_schema(self) -> None:
        with self._lock:
            conn = self._connection()
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS ohlcv_bars (
                    asset TEXT NOT NULL,
                    category TEXT NOT NULL,
                    interval TEXT NOT NULL,
                    timestamp INTEGER NOT NULL,
                    open REAL NOT NULL,
                    high REAL NOT NULL,
                    low REAL NOT NULL,
                    close REAL NOT NULL,
                    volume REAL,
                    source TEXT,
                    provider_family TEXT,
                    source_class TEXT,
                    data_origin TEXT,
                    updated_at INTEGER NOT NULL,
                    PRIMARY KEY (asset, interval, timestamp)
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_ohlcv_lookup ON ohlcv_bars (asset, interval, timestamp DESC)"
            )
            conn.commit()

    @staticmethod
    def _normalize_frame(df: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
        if df is None or df.empty:
            return None
        frame = df.copy()
        frame.index = pd.to_datetime(frame.index, utc=True, errors="coerce")
        frame = frame[~frame.index.isna()]
        if frame.empty:
            return None
        frame = frame.sort_index()
        for column in ("open", "high", "low", "close"):
            if column not in frame.columns:
                return None
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
        if "volume" not in frame.columns:
            frame["volume"] = 0.0
        frame["volume"] = pd.to_numeric(frame["volume"], errors="coerce").fillna(0.0)
        frame = frame.dropna(subset=["open", "high", "low", "close"])
        return frame if not frame.empty else None

    def store_ohlcv(
        self,
        asset: str,
        category: str,
        interval: str,
        df: Optional[pd.DataFrame],
        meta: Optional[Dict[str, Any]] = None,
    ) -> int:
        if not self._enabled:
            return 0
        frame = self._normalize_frame(df)
        interval_key = _normalize_interval(interval)
        if frame is None or interval_key not in _INTERVAL_SECONDS:
            return 0
        canonical = _canonical_asset(asset)
        category_key = _category_for(canonical, category)
        source = str((meta or {}).get("source") or "unknown")
        provider_family = _provider_family((meta or {}).get("provider_family") or source)
        source_class = str((meta or {}).get("source_class") or "history_provider")
        data_origin = str((meta or {}).get("data_origin") or "history_provider")
        updated_at = _utc_now_ts()
        rows = [
            (
                canonical,
                category_key,
                interval_key,
                int(index.timestamp()),
                float(row["open"]),
                float(row["high"]),
                float(row["low"]),
                float(row["close"]),
                float(row.get("volume", 0.0) or 0.0),
                source,
                provider_family,
                source_class,
                data_origin,
                updated_at,
            )
            for index, row in frame.iterrows()
        ]
        if not rows:
            return 0
        with self._lock:
            conn = self._connection()
            conn.executemany(
                """
                INSERT INTO ohlcv_bars (
                    asset, category, interval, timestamp,
                    open, high, low, close, volume,
                    source, provider_family, source_class, data_origin, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(asset, interval, timestamp) DO UPDATE SET
                    category=excluded.category,
                    open=excluded.open,
                    high=excluded.high,
                    low=excluded.low,
                    close=excluded.close,
                    volume=excluded.volume,
                    source=excluded.source,
                    provider_family=excluded.provider_family,
                    source_class=excluded.source_class,
                    data_origin=excluded.data_origin,
                    updated_at=excluded.updated_at
                """,
                rows,
            )
            conn.commit()
        return len(rows)

    def record_live_price(
        self,
        asset: str,
        price: float,
        *,
        source: str = "WebSocket",
        timestamp: Optional[float] = None,
    ) -> None:
        if not self._enabled:
            return
        canonical = _canonical_asset(asset)
        category = _category_for(canonical)
        if category == "unknown":
            return
        try:
            price_value = float(price)
        except Exception:
            return
        ts = float(timestamp if timestamp is not None else datetime.now().timestamp())
        bucket = _bucket_epoch(ts, 60)
        updated_at = _utc_now_ts()
        family = _provider_family(source)
        with self._lock:
            conn = self._connection()
            conn.execute(
                """
                INSERT INTO ohlcv_bars (
                    asset, category, interval, timestamp,
                    open, high, low, close, volume,
                    source, provider_family, source_class, data_origin, updated_at
                )
                VALUES (?, ?, '1m', ?, ?, ?, ?, ?, 0.0, ?, ?, 'stream_cache', 'live_stream', ?)
                ON CONFLICT(asset, interval, timestamp) DO UPDATE SET
                    high=MAX(ohlcv_bars.high, excluded.high),
                    low=MIN(ohlcv_bars.low, excluded.low),
                    close=excluded.close,
                    source=excluded.source,
                    provider_family=excluded.provider_family,
                    source_class='stream_cache',
                    data_origin='live_stream',
                    updated_at=excluded.updated_at
                """,
                (
                    canonical,
                    category,
                    bucket,
                    price_value,
                    price_value,
                    price_value,
                    price_value,
                    str(source or "WebSocket"),
                    family,
                    updated_at,
                ),
            )
            conn.commit()

    def get_ohlcv(
        self,
        asset: str,
        category: str,
        interval: str,
        periods: int,
        *,
        end_time: Any = None,
        closed_only: bool = False,
    ) -> Tuple[Optional[pd.DataFrame], Dict[str, Any]]:
        if not self._enabled:
            return None, {}
        interval_key = _normalize_interval(interval)
        if interval_key not in _INTERVAL_SECONDS:
            return None, {}
        canonical = _canonical_asset(asset)
        category_key = _category_for(canonical, category)
        cutoff = _coerce_cutoff(end_time)
        exact_df, exact_meta = self._load_exact(canonical, interval_key, int(periods), cutoff, closed_only=closed_only)
        resampled_df, resampled_meta = (
            self._load_resampled_from_minute(canonical, interval_key, int(periods), cutoff, closed_only=closed_only)
            if interval_key != "1m"
            else (None, {})
        )
        merged_df, merged_meta = self._merge_frames(
            exact_df,
            exact_meta,
            resampled_df,
            resampled_meta,
            periods=int(periods),
        )
        if merged_df is not None:
            return merged_df, self._metadata(
                category_key,
                interval_key,
                merged_df,
                merged_meta,
                mode="merged_live_tail",
            )
        if exact_df is not None and (resampled_df is None or len(exact_df) >= len(resampled_df)):
            return exact_df, self._metadata(category_key, interval_key, exact_df, exact_meta, mode="exact")
        if resampled_df is not None:
            return resampled_df, self._metadata(
                category_key,
                interval_key,
                resampled_df,
                resampled_meta,
                mode="resampled_1m",
            )
        return None, {}

    def _load_exact(
        self,
        asset: str,
        interval: str,
        periods: int,
        cutoff: pd.Timestamp,
        *,
        closed_only: bool,
    ) -> Tuple[Optional[pd.DataFrame], Dict[str, Any]]:
        interval_seconds = _INTERVAL_SECONDS[interval]
        cutoff_bucket = _bucket_epoch(cutoff.timestamp(), interval_seconds)
        comparator = "<" if closed_only else "<="
        with self._lock:
            conn = self._connection()
            rows = conn.execute(
                f"""
                SELECT timestamp, open, high, low, close, volume, source, provider_family, source_class, data_origin
                FROM ohlcv_bars
                WHERE asset = ? AND interval = ? AND timestamp {comparator} ?
                ORDER BY timestamp DESC
                LIMIT ?
                """,
                (asset, interval, cutoff_bucket, max(1, int(periods))),
            ).fetchall()
        return self._rows_to_frame(rows)

    def _load_resampled_from_minute(
        self,
        asset: str,
        interval: str,
        periods: int,
        cutoff: pd.Timestamp,
        *,
        closed_only: bool,
    ) -> Tuple[Optional[pd.DataFrame], Dict[str, Any]]:
        if interval not in _RESAMPLE_RULES:
            return None, {}
        factor = max(1, int(_INTERVAL_SECONDS[interval] / 60))
        limit = int(max(periods * factor + factor * 12, factor * 4))
        cutoff_bucket = _bucket_epoch(cutoff.timestamp(), 60)
        comparator = "<" if closed_only else "<="
        with self._lock:
            conn = self._connection()
            rows = conn.execute(
                f"""
                SELECT timestamp, open, high, low, close, volume, source, provider_family, source_class, data_origin
                FROM ohlcv_bars
                WHERE asset = ? AND interval = '1m' AND timestamp {comparator} ?
                ORDER BY timestamp DESC
                LIMIT ?
                """,
                (asset, cutoff_bucket, limit),
            ).fetchall()
        frame, meta = self._rows_to_frame(rows)
        if frame is None or frame.empty:
            return None, {}
        resampled = frame.resample(_RESAMPLE_RULES[interval], label="left", closed="left").agg(
            {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}
        )
        resampled = resampled.dropna(subset=["open", "high", "low", "close"])
        if resampled.empty:
            return None, {}
        interval_seconds = _INTERVAL_SECONDS[interval]
        cutoff_bucket = _bucket_epoch(cutoff.timestamp(), interval_seconds)
        cutoff_ts = pd.to_datetime(cutoff_bucket, unit="s", utc=True)
        if closed_only:
            resampled = resampled[resampled.index < cutoff_ts]
        else:
            resampled = resampled[resampled.index <= cutoff_ts]
        if resampled.empty:
            return None, {}
        return resampled.tail(periods).copy(), meta

    @staticmethod
    def _merge_frames(
        exact_df: Optional[pd.DataFrame],
        exact_meta: Dict[str, Any],
        resampled_df: Optional[pd.DataFrame],
        resampled_meta: Dict[str, Any],
        *,
        periods: int,
    ) -> Tuple[Optional[pd.DataFrame], Dict[str, Any]]:
        if exact_df is None or exact_df.empty or resampled_df is None or resampled_df.empty:
            return None, {}
        exact_last = pd.Timestamp(exact_df.index.max())
        resampled_last = pd.Timestamp(resampled_df.index.max())
        if pd.isna(exact_last) or pd.isna(resampled_last) or resampled_last <= exact_last:
            return None, {}

        merged = pd.concat([exact_df, resampled_df]).sort_index()
        merged = merged[~merged.index.duplicated(keep="last")]
        merged = merged.tail(max(1, int(periods))).copy()
        families: list[str] = []
        for family in list((exact_meta or {}).get("provider_families", [])) + list((resampled_meta or {}).get("provider_families", [])):
            token = str(family or "").strip()
            if token and token not in families:
                families.append(token)
        sources: list[str] = []
        for source in list((exact_meta or {}).get("sources", [])) + list((resampled_meta or {}).get("sources", [])):
            token = str(source or "").strip()
            if token and token not in sources:
                sources.append(token)
        return merged, {
            "provider_families": families,
            "sources": sources,
            "latest_provider_family": str((resampled_meta or {}).get("latest_provider_family") or ""),
            "latest_source": str((resampled_meta or {}).get("latest_source") or ""),
            "latest_source_class": str((resampled_meta or {}).get("latest_source_class") or ""),
            "latest_data_origin": str((resampled_meta or {}).get("latest_data_origin") or ""),
        }

    @staticmethod
    def _rows_to_frame(rows: list[tuple]) -> Tuple[Optional[pd.DataFrame], Dict[str, Any]]:
        if not rows:
            return None, {}
        frame = pd.DataFrame(
            list(reversed(rows)),
            columns=[
                "timestamp",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "source",
                "provider_family",
                "source_class",
                "data_origin",
            ],
        )
        meta = {
            "sources": [str(value) for value in frame["source"].dropna().unique().tolist() if str(value)],
            "provider_families": [
                str(value)
                for value in frame["provider_family"].dropna().unique().tolist()
                if str(value)
            ],
            "latest_source": str(frame["source"].iloc[-1] or ""),
            "latest_provider_family": str(frame["provider_family"].iloc[-1] or ""),
            "latest_source_class": str(frame["source_class"].iloc[-1] or ""),
            "latest_data_origin": str(frame["data_origin"].iloc[-1] or ""),
        }
        frame.index = pd.to_datetime(frame["timestamp"], unit="s", utc=True)
        frame = frame.drop(columns=["timestamp", "source", "provider_family", "source_class", "data_origin"])
        for column in ("open", "high", "low", "close", "volume"):
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
        frame = frame.dropna(subset=["open", "high", "low", "close"])
        return (frame if not frame.empty else None), meta

    @staticmethod
    def _metadata(
        category: str,
        interval: str,
        frame: pd.DataFrame,
        row_meta: Dict[str, Any],
        *,
        mode: str,
    ) -> Dict[str, Any]:
        families = [value for value in (row_meta or {}).get("provider_families", []) if value]
        provider_family = families[0] if len(families) == 1 else ("MIXED" if families else "")
        sources = [value for value in (row_meta or {}).get("sources", []) if value]
        source_detail = sources[0] if len(sources) == 1 else ("mixed" if sources else "unknown")
        latest_provider_family = str((row_meta or {}).get("latest_provider_family") or provider_family or "")
        latest_source = str((row_meta or {}).get("latest_source") or source_detail or "")
        latest_source_class = str((row_meta or {}).get("latest_source_class") or "")
        latest_data_origin = str((row_meta or {}).get("latest_data_origin") or "")
        realtime = mode in {"resampled_1m", "merged_live_tail"} or latest_source_class == "stream_cache"
        return {
            "source": "LocalStore",
            "source_class": "local_store",
            "provider_family": provider_family,
            "backing_source": source_detail,
            "backing_sources": sources,
            "backing_provider_families": families,
            "latest_provider_family": latest_provider_family,
            "latest_source": latest_source,
            "latest_source_class": latest_source_class,
            "latest_data_origin": latest_data_origin,
            "category": category,
            "interval": interval,
            "local_mode": mode,
            "local_rows": int(len(frame)),
            "data_origin": "live_stream" if realtime else "history_provider",
            "delayed": False,
            "realtime": realtime,
            "as_of_utc": datetime.now(timezone.utc).isoformat(),
        }


local_candle_store = LocalCandleStore()
