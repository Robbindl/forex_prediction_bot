from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests

from config.config import BYBIT_PUBLIC_DATA_ENABLED, BYBIT_SYMBOL_MAP
from core.assets import registry
from utils.logger import get_logger

logger = get_logger()

_BASE_URL = "https://api.bybit.com"
_TICKER_ENDPOINT = "/v5/market/tickers"
_BOOKS_ENDPOINT = "/v5/market/orderbook"
_KLINE_ENDPOINT = "/v5/market/kline"
_BOOK_DEPTH = "500"
_CATEGORY = "linear"
_MAX_KLINE_LIMIT = 1000

_INTERVAL_MAP = {
    "1m": "1",
    "5m": "5",
    "15m": "15",
    "30m": "30",
    "1h": "60",
    "4h": "240",
    "1d": "D",
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

_DEFAULT_SYMBOLS = {
    "XAU/USD": "XAUUSDT",
    "XAG/USD": "XAGUSDT",
    "WTI": "CLUSDT",
}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except Exception:
        return None


def _parse_symbol_map(raw: str) -> Dict[str, str]:
    overrides = dict(_DEFAULT_SYMBOLS)
    if not raw:
        return overrides
    try:
        payload = json.loads(raw)
    except Exception as exc:
        logger.warning(f"[BybitBridge] Invalid BYBIT_SYMBOL_MAP JSON: {exc}")
        return overrides
    if not isinstance(payload, dict):
        return overrides
    for key, value in payload.items():
        canonical = registry.canonical(str(key or "").strip())
        symbol = str(value or "").strip().upper()
        if canonical and symbol:
            overrides[canonical] = symbol
    return overrides


class BybitMarketBridge:
    """
    Public Bybit market-data bridge for supported commodity contracts.

    We use this bridge for richer commodity depth and now for commodity
    candle history too, while legacy broker/provider rules still decide
    whether trading is permitted.
    """

    def __init__(self) -> None:
        self._enabled = bool(BYBIT_PUBLIC_DATA_ENABLED)
        self._symbol_overrides = _parse_symbol_map(BYBIT_SYMBOL_MAP)
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": "Robbie-TradingBot/1.0"})

    def list_profiles(self) -> list[str]:
        return ["bybit_public"] if self._enabled else []

    def resolve_symbol_info(self, asset: str, category: str = "") -> Optional[Dict[str, Any]]:
        symbol = self._resolve_symbol(asset, category=category)
        if not symbol:
            return None
        canonical = registry.canonical(str(asset or "").strip())
        return {
            "symbol": symbol,
            "display_name": canonical,
            "market": "commodities",
            "exchange": "bybit",
            "category": _CATEGORY,
        }

    def supports(self, asset: str, category: str = "") -> bool:
        return self._resolve_symbol(asset, category=category) is not None

    def get_market_status(self, asset: str, category: str = "") -> Optional[Dict[str, Any]]:
        symbol = self._resolve_symbol(asset, category=category)
        if not symbol:
            return None
        return {
            "market_open": True,
            "provider_market_open": True,
            "exchange_is_open": True,
            "is_24h": True,
            "session_name": "continuous",
            "next_open_utc": None,
            "next_close_utc": None,
            "exchange": "bybit",
            "exchange_symbol": symbol,
            "as_of_utc": _utc_now_iso(),
        }

    def get_quote(
        self,
        asset: str,
        category: str = "",
    ) -> Tuple[Optional[float], Optional[float], Dict[str, Any]]:
        symbol = self._resolve_symbol(asset, category=category)
        if not symbol:
            return None, None, {}

        try:
            payload = self._request_json(_TICKER_ENDPOINT, {"category": _CATEGORY, "symbol": symbol})
            rows = (payload.get("result") or {}).get("list") or []
            if not rows:
                return None, None, {}
            row = rows[0] or {}
            bid = _safe_float(row.get("bid1Price"))
            ask = _safe_float(row.get("ask1Price"))
            last = _safe_float(row.get("lastPrice"))
            price = last
            spread = 0.0
            if bid and ask and bid > 0 and ask > 0:
                price = last if last and last > 0 else (bid + ask) / 2.0
                spread = max(0.0, ask - bid)
            elif price is None:
                return None, None, {}

            try:
                from services.live_microstructure_service import get_service as get_live_microstructure_service

                get_live_microstructure_service().record_quote(
                    "bybit",
                    registry.canonical(asset),
                    bid=bid,
                    ask=ask,
                    price=price,
                    timestamp=datetime.now(timezone.utc),
                )
            except Exception:
                pass
            return float(price), float(spread or 0.0), self._metadata(symbol, realtime=True)
        except Exception as exc:
            logger.debug(f"[BybitBridge] quote {asset}: {exc}")
            return None, None, {}

    def get_microstructure(self, asset: str, category: str = "") -> Dict[str, Any]:
        symbol = self._resolve_symbol(asset, category=category)
        if not symbol:
            return {}

        price: Optional[float] = None
        spread: float = 0.0
        levels: List[Dict[str, Any]] = []

        try:
            payload = self._request_json(
                _BOOKS_ENDPOINT,
                {"category": _CATEGORY, "symbol": symbol, "limit": _BOOK_DEPTH},
            )
            book = (payload.get("result") or {})
            bids = list(book.get("b") or [])
            asks = list(book.get("a") or [])
            for idx in range(max(len(bids), len(asks))):
                bid_row = bids[idx] if idx < len(bids) else None
                ask_row = asks[idx] if idx < len(asks) else None
                bid_px = _safe_float((bid_row or [None])[0] if bid_row else None)
                bid_sz = _safe_float((bid_row or [None, None])[1] if bid_row else None)
                ask_px = _safe_float((ask_row or [None])[0] if ask_row else None)
                ask_sz = _safe_float((ask_row or [None, None])[1] if ask_row else None)
                levels.append(
                    {
                        "bid": bid_px,
                        "bid_size": bid_sz,
                        "ask": ask_px,
                        "ask_size": ask_sz,
                    }
                )

            top_bid = _safe_float((bids[0] or [None])[0] if bids else None)
            top_ask = _safe_float((asks[0] or [None])[0] if asks else None)
            if top_bid and top_ask and top_bid > 0 and top_ask > 0:
                price = (top_bid + top_ask) / 2.0
                spread = max(0.0, top_ask - top_bid)
                try:
                    from services.live_microstructure_service import get_service as get_live_microstructure_service

                    get_live_microstructure_service().record_quote(
                        "bybit",
                        registry.canonical(asset),
                        bid=top_bid,
                        ask=top_ask,
                        price=price,
                        levels=levels,
                        timestamp=datetime.now(timezone.utc),
                        flags="depth_snapshot",
                    )
                except Exception:
                    pass
        except Exception as exc:
            logger.debug(f"[BybitBridge] books {asset}: {exc}")

        if price is None:
            price, spread, meta = self.get_quote(asset, category=category)
            if price is None:
                return {}
        else:
            meta = self._metadata(symbol, realtime=True)

        try:
            from services.live_microstructure_service import get_service as get_live_microstructure_service

            snapshot = get_live_microstructure_service().get_snapshot(
                "bybit",
                registry.canonical(asset),
                price=price,
                spread=spread,
                meta=meta,
            )
            if snapshot:
                return {
                    **meta,
                    "quote_price": float(price),
                    "quote_spread": float(spread or 0.0),
                    **snapshot,
                }
        except Exception:
            pass

        spread_bps = round((float(spread or 0.0) / float(price)) * 10000.0, 3) if price else 0.0
        return {
            **meta,
            "quote_price": float(price),
            "quote_spread": float(spread or 0.0),
            "spread_bps": spread_bps,
            "tick_imbalance": 0.0,
            "book_imbalance": 0.0,
            "stop_hunt_risk": 0.0,
            "score": 0.0,
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
        bybit_interval = _INTERVAL_MAP.get(interval_key)
        interval_seconds = _INTERVAL_SECONDS.get(interval_key)
        if not symbol or not bybit_interval or not interval_seconds:
            return None, {}

        cutoff = pd.to_datetime(end_time, utc=True, errors="coerce") if end_time not in (None, "") else None
        if cutoff is not None and pd.isna(cutoff):
            cutoff = None

        target_rows = int(max(2, periods or 0))
        buffered_rows = int(max(2, target_rows + 4))
        rows: List[List[Any]] = []
        seen: set[str] = set()
        next_end_ms: Optional[int] = None
        if cutoff is not None:
            cutoff_ts = pd.Timestamp(cutoff)
            cutoff_ts = cutoff_ts.tz_convert("UTC") if cutoff_ts.tzinfo else cutoff_ts.tz_localize("UTC")
            next_end_ms = int(cutoff_ts.timestamp() * 1000)

        max_pages = max(1, int((buffered_rows + _MAX_KLINE_LIMIT - 1) / _MAX_KLINE_LIMIT) + 1)
        try:
            for _ in range(max_pages):
                limit = int(min(_MAX_KLINE_LIMIT, max(2, buffered_rows - len(rows))))
                params: Dict[str, Any] = {
                    "category": _CATEGORY,
                    "symbol": symbol,
                    "interval": bybit_interval,
                    "limit": limit,
                }
                if next_end_ms is not None:
                    params["end"] = next_end_ms

                payload = self._request_json(_KLINE_ENDPOINT, params)
                page = ((payload.get("result") or {}).get("list")) or []
                if not isinstance(page, list) or not page:
                    break

                added = 0
                for entry in page:
                    if not isinstance(entry, list) or len(entry) < 6:
                        continue
                    stamp = str(entry[0] or "")
                    if not stamp or stamp in seen:
                        continue
                    seen.add(stamp)
                    rows.append(entry)
                    added += 1

                try:
                    oldest_ts = int(str(page[-1][0] or "0"))
                except Exception:
                    oldest_ts = 0

                if oldest_ts <= 0 or added <= 0 or len(page) < limit or len(rows) >= buffered_rows:
                    break
                next_end_ms = oldest_ts - 1

            if not rows:
                return None, {}

            frame = pd.DataFrame(
                rows,
                columns=[
                    "open_time",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "turnover",
                ],
            )
            frame["open_time"] = pd.to_numeric(frame["open_time"], errors="coerce")
            frame["timestamp"] = pd.to_datetime(frame["open_time"], unit="ms", utc=True, errors="coerce")
            frame = frame.drop(columns=["open_time", "turnover"]).dropna(subset=["timestamp"])
            frame = frame.set_index("timestamp").sort_index()
            for column in ("open", "high", "low", "close", "volume"):
                frame[column] = pd.to_numeric(frame[column], errors="coerce")
            frame = frame.dropna(subset=["open", "high", "low", "close"])
            if frame.empty:
                return None, {}

            effective_cutoff = cutoff
            if closed_only and effective_cutoff is None:
                effective_cutoff = pd.Timestamp(datetime.now(timezone.utc))
            if effective_cutoff is not None:
                cutoff_ts = pd.Timestamp(effective_cutoff)
                cutoff_ts = cutoff_ts.tz_convert("UTC") if cutoff_ts.tzinfo else cutoff_ts.tz_localize("UTC")
                if closed_only:
                    close_times = frame.index + pd.to_timedelta(interval_seconds, unit="s")
                    frame = frame[close_times <= cutoff_ts]
                else:
                    frame = frame[frame.index <= cutoff_ts]
            frame = frame.tail(target_rows)
            if frame.empty:
                return None, {}

            return frame[["open", "high", "low", "close", "volume"]], self._metadata(symbol, realtime=False)
        except Exception as exc:
            logger.debug(f"[BybitBridge] ohlcv {asset}: {exc}")
            return None, {}

    def _resolve_symbol(self, asset: str, category: str = "") -> Optional[str]:
        if not self._enabled:
            return None
        canonical = registry.canonical(str(asset or "").strip())
        resolved_category = str(category or registry.category(canonical) or "").strip().lower()
        if resolved_category != "commodities":
            return None
        return self._symbol_overrides.get(canonical)

    def _request_json(self, endpoint: str, params: Dict[str, Any]) -> Dict[str, Any]:
        response = self._session.get(f"{_BASE_URL}{endpoint}", params=params, timeout=8)
        response.raise_for_status()
        payload = response.json() or {}
        if str(payload.get("retCode", "0")) not in {"", "0"}:
            raise RuntimeError(payload.get("retMsg") or f"Bybit request failed: {payload}")
        return payload

    @staticmethod
    def _metadata(symbol: str, realtime: bool) -> Dict[str, Any]:
        return {
            "source": "Bybit",
            "source_class": "exchange_depth",
            "delayed": False,
            "realtime": bool(realtime),
            "from_cache": False,
            "exchange": "bybit",
            "exchange_symbol": symbol,
            "as_of_utc": _utc_now_iso(),
        }


bybit_market_bridge = BybitMarketBridge()
