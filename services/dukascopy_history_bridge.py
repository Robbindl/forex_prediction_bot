from __future__ import annotations

import json
import lzma
import struct
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Iterator, Optional, Tuple

import pandas as pd
import requests

from config.config import DUKASCOPY_HISTORY_ENABLED, DUKASCOPY_SYMBOL_MAP
from core.assets import registry
from utils.logger import get_logger

logger = get_logger()

_BASE_URL = "https://datafeed.dukascopy.com/datafeed"
_CACHE_MISS = object()

_SUPPORTED_SYMBOLS: Dict[str, Dict[str, Any]] = {
    "EUR/USD": {"symbol": "EURUSD", "scale": 100000, "category": "forex"},
    "EUR/JPY": {"symbol": "EURJPY", "scale": 1000, "category": "forex"},
    "EUR/GBP": {"symbol": "EURGBP", "scale": 100000, "category": "forex"},
    "GBP/USD": {"symbol": "GBPUSD", "scale": 100000, "category": "forex"},
    "GBP/JPY": {"symbol": "GBPJPY", "scale": 1000, "category": "forex"},
    "AUD/USD": {"symbol": "AUDUSD", "scale": 100000, "category": "forex"},
    "NZD/USD": {"symbol": "NZDUSD", "scale": 100000, "category": "forex"},
    "USD/JPY": {"symbol": "USDJPY", "scale": 1000, "category": "forex"},
    "USD/CAD": {"symbol": "USDCAD", "scale": 100000, "category": "forex"},
    "USD/CHF": {"symbol": "USDCHF", "scale": 100000, "category": "forex"},
    "XAU/USD": {"symbol": "XAUUSD", "scale": 1000, "category": "commodities"},
    "XAG/USD": {"symbol": "XAGUSD", "scale": 1000, "category": "commodities"},
    "WTI": {"symbol": "LIGHTCMDUSD", "scale": 1000, "category": "commodities"},
    "US30": {"symbol": "USA30IDXUSD", "scale": 1000, "category": "indices"},
    "US100": {"symbol": "USATECHIDXUSD", "scale": 1000, "category": "indices"},
    "US500": {"symbol": "USA500IDXUSD", "scale": 1000, "category": "indices"},
    "UK100": {"symbol": "GBRIDXGBP", "scale": 1000, "category": "indices"},
}

_SUPPORTED_INTERVALS = {"1m", "5m", "15m", "30m", "1h", "4h", "1d"}

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
    "1m": "1min",
    "5m": "5min",
    "15m": "15min",
    "30m": "30min",
    "1h": "1h",
    "4h": "4h",
    "1d": "1d",
}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_symbol_map(raw: str) -> Dict[str, str]:
    if not raw:
        return {}
    try:
        payload = json.loads(raw)
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}
    parsed: Dict[str, str] = {}
    for key, value in payload.items():
        canonical = registry.canonical(str(key or ""))
        symbol = str(value or "").strip()
        if canonical and symbol:
            parsed[canonical] = symbol
    return parsed


def _coerce_cutoff(value: Any) -> pd.Timestamp:
    ts = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(ts):
        return pd.Timestamp(datetime.now(timezone.utc))
    return pd.Timestamp(ts)


def _iter_dates(start_date: date, end_date: date) -> Iterator[date]:
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)


def _iter_months(start_ts: pd.Timestamp, end_ts: pd.Timestamp) -> Iterator[Tuple[int, int]]:
    year = int(start_ts.year)
    month = int(start_ts.month)
    end_year = int(end_ts.year)
    end_month = int(end_ts.month)
    while (year, month) <= (end_year, end_month):
        yield year, month
        month += 1
        if month > 12:
            month = 1
            year += 1


class DukascopyHistoryBridge:
    """
    Free historical/backfill market-data bridge backed by Dukascopy raw BI5 files.

    This is intentionally history-only. Live quotes and execution remain on the
    existing broker paths.
    """

    def __init__(self) -> None:
        self._enabled = bool(DUKASCOPY_HISTORY_ENABLED)
        self._symbol_overrides = _parse_symbol_map(DUKASCOPY_SYMBOL_MAP)
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": "Robbie-TradingBot/1.0"})
        self._file_cache: Dict[str, Optional[pd.DataFrame]] = {}

    def list_profiles(self) -> list[str]:
        return ["dukascopy_history"] if self._enabled else []

    def supports(self, asset: str, category: str = "") -> bool:
        return self._resolve_symbol_info(asset, category=category) is not None

    def resolve_symbol_info(self, asset: str, category: str = "") -> Optional[Dict[str, Any]]:
        resolved = self._resolve_symbol_info(asset, category=category)
        if not resolved:
            return None
        return {
            "symbol": str(resolved["symbol"]),
            "display_name": str(resolved["asset"]),
            "market": str(category or resolved["category"]),
            "exchange": "dukascopy",
        }

    def get_ohlcv(
        self,
        asset: str,
        interval: str,
        periods: int,
        category: str = "",
        end_time: Any = None,
        closed_only: bool = False,
    ) -> Tuple[Optional[pd.DataFrame], Dict[str, Any]]:
        resolved = self._resolve_symbol_info(asset, category=category)
        interval_key = str(interval or "").lower()
        if not resolved or interval_key not in _SUPPORTED_INTERVALS:
            return None, {}

        cutoff = _coerce_cutoff(end_time)
        requested = int(max(2, periods or 0))

        try:
            if interval_key in {"1m", "5m", "15m", "30m"}:
                frame = self._load_intraday_history(
                    symbol=str(resolved["symbol"]),
                    scale=int(resolved["scale"]),
                    interval=interval_key,
                    periods=requested,
                    cutoff=cutoff,
                    closed_only=closed_only,
                )
            else:
                frame = self._load_hourly_history(
                    symbol=str(resolved["symbol"]),
                    scale=int(resolved["scale"]),
                    interval=interval_key,
                    periods=requested,
                    cutoff=cutoff,
                    closed_only=closed_only,
                )
            if frame is None or frame.empty:
                return None, {}
            frame = frame.tail(requested).copy()
            return frame, self._metadata(str(resolved["symbol"]))
        except DukascopyRequestError as exc:
            return None, self._error_metadata(str(resolved["symbol"]), code=exc.code, message=exc.message)
        except Exception as exc:
            logger.debug(f"[DukascopyBridge] ohlcv {asset}: {exc}")
            return None, self._error_metadata(str(resolved["symbol"]), code="ohlcv_failed", message=str(exc))

    def _load_intraday_history(
        self,
        *,
        symbol: str,
        scale: int,
        interval: str,
        periods: int,
        cutoff: pd.Timestamp,
        closed_only: bool,
    ) -> Optional[pd.DataFrame]:
        interval_seconds = int(_INTERVAL_SECONDS[interval])
        lookback_seconds = int(periods * interval_seconds + max(interval_seconds * 4, 86400))
        start_ts = cutoff - pd.Timedelta(seconds=lookback_seconds)
        frames = []
        for item in _iter_dates(start_ts.date(), cutoff.date()):
            day_frame = self._fetch_minute_day(symbol, scale, item)
            if day_frame is not None and not day_frame.empty:
                frames.append(day_frame)
        if not frames:
            return None
        frame = pd.concat(frames).sort_index()
        frame = frame[~frame.index.duplicated(keep="last")]
        if interval != "1m":
            frame = self._resample(frame, interval)
        frame = self._apply_cutoff(frame, interval, cutoff, closed_only=closed_only)
        return frame if frame is not None and not frame.empty else None

    def _load_hourly_history(
        self,
        *,
        symbol: str,
        scale: int,
        interval: str,
        periods: int,
        cutoff: pd.Timestamp,
        closed_only: bool,
    ) -> Optional[pd.DataFrame]:
        interval_seconds = int(_INTERVAL_SECONDS[interval])
        lookback_seconds = int(periods * interval_seconds + max(interval_seconds * 4, 31 * 86400))
        start_ts = cutoff - pd.Timedelta(seconds=lookback_seconds)
        frames = []
        for year, month in _iter_months(start_ts, cutoff):
            month_frame = self._fetch_hour_month(symbol, scale, year, month)
            if month_frame is not None and not month_frame.empty:
                frames.append(month_frame)
        if not frames:
            return None
        frame = pd.concat(frames).sort_index()
        frame = frame[~frame.index.duplicated(keep="last")]
        if interval != "1h":
            frame = self._resample(frame, interval)
        frame = self._apply_cutoff(frame, interval, cutoff, closed_only=closed_only)
        return frame if frame is not None and not frame.empty else None

    def _fetch_minute_day(self, symbol: str, scale: int, item: date) -> Optional[pd.DataFrame]:
        month0 = int(item.month) - 1
        url = f"{_BASE_URL}/{symbol}/{item.year}/{month0:02d}/{item.day:02d}/BID_candles_min_1.bi5"
        base_ts = datetime(item.year, item.month, item.day, tzinfo=timezone.utc)
        return self._fetch_bi5_candles(url, base_ts=base_ts, scale=scale)

    def _fetch_hour_month(self, symbol: str, scale: int, year: int, month: int) -> Optional[pd.DataFrame]:
        month0 = int(month) - 1
        url = f"{_BASE_URL}/{symbol}/{year}/{month0:02d}/BID_candles_hour_1.bi5"
        base_ts = datetime(year, month, 1, tzinfo=timezone.utc)
        return self._fetch_bi5_candles(url, base_ts=base_ts, scale=scale)

    def _fetch_bi5_candles(
        self,
        url: str,
        *,
        base_ts: datetime,
        scale: int,
    ) -> Optional[pd.DataFrame]:
        cached = self._file_cache.get(url, _CACHE_MISS)
        if cached is not _CACHE_MISS:
            return None if cached is None else cached.copy()

        response = self._session.get(url, timeout=20)
        if response.status_code == 404:
            self._file_cache[url] = None
            return None
        if response.status_code >= 400:
            raise DukascopyRequestError(
                code=f"http_{response.status_code}",
                message=f"Dukascopy request failed with HTTP {response.status_code}",
            )

        try:
            payload = lzma.decompress(response.content)
        except Exception as exc:
            raise DukascopyRequestError("decode_failed", f"Unable to decode Dukascopy BI5 payload: {exc}") from exc

        if not payload:
            self._file_cache[url] = None
            return None
        if len(payload) % 24 != 0:
            raise DukascopyRequestError(
                "unexpected_record_size",
                f"Unexpected Dukascopy BI5 payload length {len(payload)} for {url}",
            )

        rows = []
        for offset, open_raw, close_raw, low_raw, high_raw, volume_raw in struct.iter_unpack(">IIIII f", payload):
            ts = pd.Timestamp(base_ts + timedelta(seconds=int(offset)))
            rows.append(
                {
                    "timestamp": ts,
                    "open": float(open_raw) / float(scale),
                    "high": float(high_raw) / float(scale),
                    "low": float(low_raw) / float(scale),
                    "close": float(close_raw) / float(scale),
                    "volume": float(volume_raw or 0.0),
                }
            )

        if not rows:
            self._file_cache[url] = None
            return None

        frame = pd.DataFrame(rows).set_index("timestamp").sort_index()
        frame = frame[["open", "high", "low", "close", "volume"]]
        self._file_cache[url] = frame.copy()
        return frame

    @staticmethod
    def _resample(frame: pd.DataFrame, interval: str) -> pd.DataFrame:
        rule = _RESAMPLE_RULES.get(str(interval or "").lower())
        if not rule:
            return frame
        resampled = frame.resample(rule, label="left", closed="left").agg(
            {
                "open": "first",
                "high": "max",
                "low": "min",
                "close": "last",
                "volume": "sum",
            }
        )
        return resampled.dropna(subset=["open", "high", "low", "close"])

    @staticmethod
    def _apply_cutoff(frame: pd.DataFrame, interval: str, cutoff: pd.Timestamp, *, closed_only: bool) -> pd.DataFrame:
        cutoff_ts = pd.Timestamp(cutoff)
        if cutoff_ts.tzinfo is None:
            cutoff_ts = cutoff_ts.tz_localize("UTC")
        else:
            cutoff_ts = cutoff_ts.tz_convert("UTC")
        if frame.empty:
            return frame
        if closed_only:
            interval_seconds = int(_INTERVAL_SECONDS.get(str(interval or "").lower(), 60))
            return frame[(frame.index + pd.to_timedelta(interval_seconds, unit="s")) <= cutoff_ts]
        return frame[frame.index <= cutoff_ts]

    def _resolve_symbol_info(self, asset: str, category: str = "") -> Optional[Dict[str, Any]]:
        if not self._enabled:
            return None
        canonical = registry.canonical(str(asset or ""))
        resolved = dict(_SUPPORTED_SYMBOLS.get(canonical) or {})
        if not resolved:
            return None
        expected_category = str(resolved.get("category") or "")
        category_token = str(category or "").strip().lower()
        if category_token and category_token != expected_category:
            return None
        override = self._symbol_overrides.get(canonical)
        if override:
            resolved["symbol"] = str(override)
        resolved["asset"] = canonical
        return resolved

    @staticmethod
    def _metadata(symbol: str) -> Dict[str, Any]:
        return {
            "source": "Dukascopy",
            "source_class": "secondary_api",
            "delayed": False,
            "realtime": False,
            "from_cache": False,
            "exchange": "dukascopy",
            "dukascopy_symbol": symbol,
            "as_of_utc": _utc_now_iso(),
        }

    def _error_metadata(self, symbol: str, *, code: str, message: str) -> Dict[str, Any]:
        payload = self._metadata(symbol)
        payload["provider_error_code"] = str(code or "dukascopy_error")
        payload["provider_error_message"] = str(message or "unknown Dukascopy error")
        return payload


class DukascopyRequestError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        self.code = str(code or "dukascopy_request_error")
        self.message = str(message or code or "Dukascopy request failed")
        super().__init__(self.message)


dukascopy_history_bridge = DukascopyHistoryBridge()
