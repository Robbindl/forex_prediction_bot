from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests

from config.config import OKX_PUBLIC_DATA_ENABLED, OKX_SYMBOL_MAP
from core.assets import registry
from utils.logger import get_logger

logger = get_logger()

_BASE_URL = "https://www.okx.com"
_TICKER_ENDPOINT = "/api/v5/market/ticker"
_BOOKS_ENDPOINT = "/api/v5/market/books"
_CANDLES_ENDPOINT = "/api/v5/market/history-candles"
_BOOK_DEPTH = "400"
_MAX_KLINE_LIMIT = 300

_INTERVAL_MAP = {
    "1m": "1m",
    "5m": "5m",
    "15m": "15m",
    "30m": "30m",
    "1h": "1H",
    "4h": "4H",
    "1d": "1D",
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
    "XAU/USD": "XAU-USDT-SWAP",
    "XAG/USD": "XAG-USDT-SWAP",
    "WTI": "CL-USDT-SWAP",
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
        logger.warning(f"[OKXBridge] Invalid OKX_SYMBOL_MAP JSON: {exc}")
        return overrides
    if not isinstance(payload, dict):
        return overrides
    for key, value in payload.items():
        canonical = registry.canonical(str(key or "").strip())
        symbol = str(value or "").strip()
        if canonical and symbol:
            overrides[canonical] = symbol
    return overrides


class OkxMarketBridge:
    """
    Public OKX market-data bridge focused on commodity perpetuals.

    This is used primarily for commodity book depth and timing confirmation,
    while IG / Deriv remain the primary execution-quote anchors.
    """

    def __init__(self) -> None:
        self._enabled = bool(OKX_PUBLIC_DATA_ENABLED)
        self._symbol_overrides = _parse_symbol_map(OKX_SYMBOL_MAP)
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": "Robbie-TradingBot/1.0"})

    def list_profiles(self) -> list[str]:
        return ["okx_public"] if self._enabled else []

    def resolve_symbol_info(self, asset: str, category: str = "") -> Optional[Dict[str, Any]]:
        symbol = self._resolve_symbol(asset, category=category)
        if not symbol:
            return None
        canonical = registry.canonical(str(asset or "").strip())
        return {
            "symbol": symbol,
            "display_name": canonical,
            "market": "commodities",
            "exchange": "okx",
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
            "exchange": "okx",
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
            payload = self._request_json(_TICKER_ENDPOINT, {"instId": symbol})
            rows = payload.get("data") or []
            if not rows:
                return None, None, {}
            row = rows[0] or {}
            bid = _safe_float(row.get("bidPx"))
            ask = _safe_float(row.get("askPx"))
            last = _safe_float(row.get("last"))
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
                    "okx",
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
            logger.debug(f"[OKXBridge] quote {asset}: {exc}")
            return None, None, {}

    def get_microstructure(self, asset: str, category: str = "") -> Dict[str, Any]:
        symbol = self._resolve_symbol(asset, category=category)
        if not symbol:
            return {}

        price: Optional[float] = None
        spread: float = 0.0
        levels: List[Dict[str, Any]] = []

        try:
            payload = self._request_json(_BOOKS_ENDPOINT, {"instId": symbol, "sz": _BOOK_DEPTH})
            rows = payload.get("data") or []
            if rows:
                book = rows[0] or {}
                bids = list(book.get("bids") or [])
                asks = list(book.get("asks") or [])
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
                            "okx",
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
            logger.debug(f"[OKXBridge] books {asset}: {exc}")

        if price is None:
            price, spread, meta = self.get_quote(asset, category=category)
            if price is None:
                return {}
        else:
            meta = self._metadata(symbol, realtime=True)

        try:
            from services.live_microstructure_service import get_service as get_live_microstructure_service

            snapshot = get_live_microstructure_service().get_snapshot(
                "okx",
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
        okx_interval = _INTERVAL_MAP.get(interval_key)
        interval_seconds = _INTERVAL_SECONDS.get(interval_key)
        if not symbol or not okx_interval or not interval_seconds:
            return None, {}

        cutoff = pd.to_datetime(end_time, utc=True, errors="coerce") if end_time not in (None, "") else None
        if cutoff is not None and pd.isna(cutoff):
            cutoff = None

        target_rows = int(max(2, periods or 0))
        buffered_rows = int(max(2, target_rows + 4))
        rows: List[List[Any]] = []
        seen: set[str] = set()
        next_after_ms: Optional[int] = None
        if cutoff is not None:
            cutoff_ts = pd.Timestamp(cutoff)
            cutoff_ts = cutoff_ts.tz_convert("UTC") if cutoff_ts.tzinfo else cutoff_ts.tz_localize("UTC")
            next_after_ms = int(cutoff_ts.timestamp() * 1000) + 1

        max_pages = max(1, int((buffered_rows + _MAX_KLINE_LIMIT - 1) / _MAX_KLINE_LIMIT) + 1)
        try:
            for _ in range(max_pages):
                limit = int(min(_MAX_KLINE_LIMIT, max(2, buffered_rows - len(rows))))
                params: Dict[str, Any] = {
                    "instId": symbol,
                    "bar": okx_interval,
                    "limit": limit,
                }
                if next_after_ms is not None:
                    params["after"] = str(next_after_ms)

                payload = self._request_json(_CANDLES_ENDPOINT, params)
                page = payload.get("data") or []
                if not isinstance(page, list) or not page:
                    break

                added = 0
                for entry in page:
                    if not isinstance(entry, list) or len(entry) < 8:
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
                next_after_ms = oldest_ts

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
                    "volume_contracts",
                    "volume_base",
                    "turnover_quote",
                    "confirm",
                ],
            )
            frame["open_time"] = pd.to_numeric(frame["open_time"], errors="coerce")
            frame["timestamp"] = pd.to_datetime(frame["open_time"], unit="ms", utc=True, errors="coerce")
            frame = frame.drop(columns=["open_time", "turnover_quote"]).dropna(subset=["timestamp"])
            frame = frame.set_index("timestamp").sort_index()
            for column in ("open", "high", "low", "close", "volume_contracts", "volume_base"):
                frame[column] = pd.to_numeric(frame[column], errors="coerce")
            frame["confirm"] = frame["confirm"].astype(str)
            frame["volume"] = frame["volume_base"].where(frame["volume_base"].notna(), frame["volume_contracts"])
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
                    frame = frame[(close_times <= cutoff_ts) & (frame["confirm"] == "1")]
                else:
                    frame = frame[frame.index <= cutoff_ts]
            elif closed_only:
                frame = frame[frame["confirm"] == "1"]
            frame = frame.tail(target_rows)
            if frame.empty:
                return None, {}

            return frame[["open", "high", "low", "close", "volume"]], self._metadata(symbol, realtime=False)
        except Exception as exc:
            logger.debug(f"[OKXBridge] ohlcv {asset}: {exc}")
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
        if str(payload.get("code", "0")) not in {"", "0"}:
            raise RuntimeError(payload.get("msg") or f"OKX request failed: {payload}")
        return payload

    @staticmethod
    def _metadata(symbol: str, realtime: bool) -> Dict[str, Any]:
        return {
            "source": "OKX",
            "source_class": "exchange_depth",
            "delayed": False,
            "realtime": bool(realtime),
            "from_cache": False,
            "exchange": "okx",
            "exchange_symbol": symbol,
            "as_of_utc": _utc_now_iso(),
        }


okx_market_bridge = OkxMarketBridge()
