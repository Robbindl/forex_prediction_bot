from __future__ import annotations

import json
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Tuple

import pandas as pd
import requests

from config.config import FMP_API_KEY, FMP_HISTORY_ENABLED, FMP_SYMBOL_MAP
from utils.logger import get_logger

logger = get_logger()

_BASE_URL = "https://financialmodelingprep.com/stable"
_INTRADAY_ENDPOINT = "/historical-chart/{interval}"
_DAILY_ENDPOINT = "/historical-price-eod/full"

_SUPPORTED_SYMBOLS = {
    "EUR/USD": "EURUSD",
    "EUR/JPY": "EURJPY",
    "EUR/GBP": "EURGBP",
    "GBP/USD": "GBPUSD",
    "GBP/JPY": "GBPJPY",
    "AUD/USD": "AUDUSD",
    "NZD/USD": "NZDUSD",
    "USD/JPY": "USDJPY",
    "USD/CAD": "USDCAD",
    "USD/CHF": "USDCHF",
    "BTC-USD": "BTCUSD",
    "ETH-USD": "ETHUSD",
    "BNB-USD": "BNBUSD",
    "SOL-USD": "SOLUSD",
    "XRP-USD": "XRPUSD",
    "XAU/USD": "GCUSD",
    "XAG/USD": "SIUSD",
    "WTI": "CLUSD",
    "US30": "^DJI",
    "US100": "^NDX",
    "US500": "^GSPC",
    "UK100": "^FTSE",
}

_ASSET_ALIASES = {
    "EUR/USD": "EUR/USD",
    "EURUSD": "EUR/USD",
    "EUR/JPY": "EUR/JPY",
    "EURJPY": "EUR/JPY",
    "EUR/GBP": "EUR/GBP",
    "EURGBP": "EUR/GBP",
    "GBP/USD": "GBP/USD",
    "GBPUSD": "GBP/USD",
    "GBP/JPY": "GBP/JPY",
    "GBPJPY": "GBP/JPY",
    "AUD/USD": "AUD/USD",
    "AUDUSD": "AUD/USD",
    "NZD/USD": "NZD/USD",
    "NZDUSD": "NZD/USD",
    "USD/JPY": "USD/JPY",
    "USDJPY": "USD/JPY",
    "USD/CAD": "USD/CAD",
    "USDCAD": "USD/CAD",
    "USD/CHF": "USD/CHF",
    "USDCHF": "USD/CHF",
    "BTC-USD": "BTC-USD",
    "BTCUSD": "BTC-USD",
    "ETH-USD": "ETH-USD",
    "ETHUSD": "ETH-USD",
    "BNB-USD": "BNB-USD",
    "BNBUSD": "BNB-USD",
    "SOL-USD": "SOL-USD",
    "SOLUSD": "SOL-USD",
    "XRP-USD": "XRP-USD",
    "XRPUSD": "XRP-USD",
    "XAU/USD": "XAU/USD",
    "XAUUSD": "XAU/USD",
    "GC=F": "XAU/USD",
    "XAG/USD": "XAG/USD",
    "XAGUSD": "XAG/USD",
    "SI=F": "XAG/USD",
    "WTI": "WTI",
    "WTI/USD": "WTI",
    "CL=F": "WTI",
    "US30": "US30",
    "^DJI": "US30",
    "US100": "US100",
    "^NDX": "US100",
    "US500": "US500",
    "^GSPC": "US500",
    "UK100": "UK100",
    "^FTSE": "UK100",
}

_INTERVAL_MAP = {
    "1m": "1min",
    "5m": "5min",
    "15m": "15min",
    "30m": "30min",
    "1h": "1hour",
    "4h": "4hour",
}

_INTERVAL_SECONDS = {
    "1m": 60,
    "5m": 300,
    "15m": 900,
    "30m": 1800,
    "1h": 3600,
    "4h": 14400,
    "1d": 86400,
}

_RESTRICTION_TTL_SEC = 15 * 60.0


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _canonical_asset(asset: str) -> str:
    return _ASSET_ALIASES.get(str(asset or "").strip().upper(), str(asset or "").strip())


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
        canonical = _canonical_asset(str(key or ""))
        symbol = str(value or "").strip()
        if canonical and symbol:
            parsed[canonical] = symbol
    return parsed


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except Exception:
        return None


class FMPHistoryBridge:
    """
    Historical/backfill market-data bridge backed by Financial Modeling Prep.

    This bridge is intentionally OHLCV-only. Live quote routing stays with the
    existing broker/stream providers.
    """

    def __init__(self) -> None:
        self._enabled = bool(FMP_HISTORY_ENABLED and str(FMP_API_KEY or "").strip())
        self._api_key = str(FMP_API_KEY or "").strip()
        self._symbol_overrides = _parse_symbol_map(FMP_SYMBOL_MAP)
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": "Robbie-TradingBot/1.0"})
        self._intraday_restricted_until = 0.0
        self._symbol_restrictions: Dict[str, Tuple[float, str]] = {}

    def list_profiles(self) -> list[str]:
        return ["fmp_history"] if self._enabled else []

    def supports(self, asset: str, category: str = "") -> bool:
        return self._resolve_symbol(asset, category=category) is not None

    def resolve_symbol_info(self, asset: str, category: str = "") -> Optional[Dict[str, Any]]:
        symbol = self._resolve_symbol(asset, category=category)
        if not symbol:
            return None
        return {
            "symbol": symbol,
            "display_name": _canonical_asset(asset),
            "market": str(category or ""),
            "exchange": "fmp",
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
        symbol = self._resolve_symbol(asset, category=category)
        interval_key = str(interval or "").lower()
        if not symbol or interval_key not in _INTERVAL_SECONDS:
            return None, {}

        restricted_meta = self._restriction_metadata(symbol, interval_key)
        if restricted_meta is not None:
            return None, restricted_meta

        try:
            cutoff = pd.to_datetime(end_time, utc=True, errors="coerce") if end_time not in (None, "") else None
            requested = int(max(2, periods or 0))
            frame = self._fetch_history(symbol, interval_key, requested, cutoff=cutoff, closed_only=closed_only)
            if frame is None or frame.empty:
                return None, {}
            frame = frame.tail(requested)
            return frame, self._metadata(symbol)
        except FMPRequestError as exc:
            return None, self._error_metadata(symbol, code=exc.code, message=exc.message)
        except Exception as exc:
            logger.debug(f"[FMPBridge] ohlcv {asset}: {exc}")
            return None, self._error_metadata(symbol, code="ohlcv_failed", message=str(exc))

    def _fetch_history(
        self,
        symbol: str,
        interval: str,
        periods: int,
        *,
        cutoff: Optional[pd.Timestamp],
        closed_only: bool,
    ) -> Optional[pd.DataFrame]:
        if interval == "1d":
            rows = self._request_daily(symbol, periods, cutoff=cutoff)
        else:
            rows = self._request_intraday(symbol, interval, periods, cutoff=cutoff)
        frame = self._normalize_rows(rows)
        if frame is None or frame.empty:
            return None
        if cutoff is not None and not pd.isna(cutoff):
            cutoff_ts = pd.Timestamp(cutoff)
            if cutoff_ts.tzinfo is None:
                cutoff_ts = cutoff_ts.tz_localize("UTC")
            else:
                cutoff_ts = cutoff_ts.tz_convert("UTC")
            if closed_only:
                frame = frame[frame.index < cutoff_ts]
            else:
                frame = frame[frame.index <= cutoff_ts]
        return frame

    def _request_daily(self, symbol: str, periods: int, *, cutoff: Optional[pd.Timestamp]) -> Any:
        params = {"symbol": symbol, "apikey": self._api_key}
        if cutoff is not None and not pd.isna(cutoff):
            end = pd.Timestamp(cutoff).tz_convert("UTC") if pd.Timestamp(cutoff).tzinfo else pd.Timestamp(cutoff).tz_localize("UTC")
            start = end - timedelta(days=max(14, int(periods * 3)))
            params["from"] = start.strftime("%Y-%m-%d")
            params["to"] = end.strftime("%Y-%m-%d")
        response = self._session.get(f"{_BASE_URL}{_DAILY_ENDPOINT}", params=params, timeout=15)
        self._raise_for_status(response, symbol=symbol, interval="1d")
        payload = response.json() or []
        if isinstance(payload, dict):
            return payload.get("historical") or payload.get("data") or payload.get("results") or []
        return payload

    def _request_intraday(self, symbol: str, interval: str, periods: int, *, cutoff: Optional[pd.Timestamp]) -> Any:
        endpoint_interval = _INTERVAL_MAP.get(interval)
        if not endpoint_interval:
            return []
        params = {"symbol": symbol, "apikey": self._api_key}
        if cutoff is not None and not pd.isna(cutoff):
            end = pd.Timestamp(cutoff).tz_convert("UTC") if pd.Timestamp(cutoff).tzinfo else pd.Timestamp(cutoff).tz_localize("UTC")
        else:
            end = pd.Timestamp(datetime.now(timezone.utc))
        lookback_seconds = int(_INTERVAL_SECONDS.get(interval, 60) * max(10, periods + 20))
        start = end - timedelta(seconds=lookback_seconds)
        params["from"] = start.strftime("%Y-%m-%d %H:%M:%S")
        params["to"] = end.strftime("%Y-%m-%d %H:%M:%S")
        response = self._session.get(
            f"{_BASE_URL}{_INTRADAY_ENDPOINT.format(interval=endpoint_interval)}",
            params=params,
            timeout=15,
        )
        self._raise_for_status(response, symbol=symbol, interval=interval)
        payload = response.json() or []
        if isinstance(payload, dict):
            return payload.get("historical") or payload.get("data") or payload.get("results") or []
        return payload

    def _raise_for_status(self, response: requests.Response, *, symbol: str, interval: str) -> None:
        if response.status_code < 400:
            return
        text = str(response.text or "").strip()
        lowered = text.lower()
        if response.status_code == 402:
            if "restricted endpoint" in lowered and interval != "1d":
                self._intraday_restricted_until = max(self._intraday_restricted_until, time.time() + _RESTRICTION_TTL_SEC)
                raise FMPRequestError(
                    "restricted_intraday",
                    "FMP intraday history endpoint is not available under the current subscription.",
                )
            if "premium query parameter" in lowered or "special endpoint" in lowered:
                self._symbol_restrictions[symbol] = (time.time() + _RESTRICTION_TTL_SEC, text)
                raise FMPRequestError(
                    "restricted_symbol",
                    f"FMP history for {symbol} is not available under the current subscription.",
                )
        raise FMPRequestError(f"http_{response.status_code}", text or f"FMP request failed with HTTP {response.status_code}")

    @staticmethod
    def _normalize_rows(rows: Any) -> Optional[pd.DataFrame]:
        if not isinstance(rows, list) or not rows:
            return None
        normalized = []
        for item in rows:
            if not isinstance(item, dict):
                continue
            ts = pd.to_datetime(
                item.get("date") or item.get("datetime") or item.get("timestamp"),
                utc=True,
                errors="coerce",
            )
            if pd.isna(ts):
                continue
            open_price = _safe_float(item.get("open"))
            high_price = _safe_float(item.get("high"))
            low_price = _safe_float(item.get("low"))
            close_price = _safe_float(item.get("close"))
            if None in (open_price, high_price, low_price, close_price):
                continue
            normalized.append(
                {
                    "timestamp": pd.Timestamp(ts),
                    "open": float(open_price),
                    "high": float(high_price),
                    "low": float(low_price),
                    "close": float(close_price),
                    "volume": float(_safe_float(item.get("volume")) or 0.0),
                }
            )
        if not normalized:
            return None
        frame = pd.DataFrame(normalized).set_index("timestamp").sort_index()
        return frame[["open", "high", "low", "close", "volume"]]

    def _resolve_symbol(self, asset: str, category: str = "") -> Optional[str]:
        if not self._enabled:
            return None
        canonical = _canonical_asset(asset)
        override = self._symbol_overrides.get(canonical)
        if override:
            return override
        return _SUPPORTED_SYMBOLS.get(canonical)

    @staticmethod
    def _metadata(symbol: str) -> Dict[str, Any]:
        return {
            "source": "FMP",
            "source_class": "secondary_api",
            "delayed": False,
            "realtime": False,
            "from_cache": False,
            "exchange": "fmp",
            "fmp_symbol": symbol,
            "as_of_utc": _utc_now_iso(),
        }

    def _error_metadata(self, symbol: str, *, code: str, message: str) -> Dict[str, Any]:
        payload = self._metadata(symbol)
        payload["provider_error_code"] = str(code or "fmp_error")
        payload["provider_error_message"] = str(message or "unknown FMP error")
        return payload

    def _restriction_metadata(self, symbol: str, interval: str) -> Optional[Dict[str, Any]]:
        now = time.time()
        if interval != "1d" and self._intraday_restricted_until > now:
            return self._error_metadata(
                symbol,
                code="restricted_intraday",
                message="FMP intraday history endpoint is not available under the current subscription.",
            )
        restricted = self._symbol_restrictions.get(symbol)
        if restricted and restricted[0] > now:
            return self._error_metadata(
                symbol,
                code="restricted_symbol",
                message=str(restricted[1] or f"FMP history for {symbol} is not available under the current subscription."),
            )
        return None


class FMPRequestError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        self.code = str(code or "fmp_request_error")
        self.message = str(message or code or "FMP request failed")
        super().__init__(self.message)


fmp_history_bridge = FMPHistoryBridge()
