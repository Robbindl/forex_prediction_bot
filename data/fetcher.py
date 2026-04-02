"""data/fetcher.py - Deriv-first market data fetcher with bounded crypto fallback."""
from __future__ import annotations

from datetime import datetime
import threading
from typing import Any, Dict, Optional, Tuple

import pandas as pd

from config.config import (
    LOOKBACK_PERIOD,
    MARKET_DATA_OHLCV_CACHE_TTL,
    MARKET_DATA_OHLCV_SLOW_CACHE_TTL,
    MARKET_DATA_QUOTE_CACHE_TTL,
    get_timeframe_periods,
    get_trading_timeframe,
)
from data.cache import Cache
from utils.logger import get_logger

logger = get_logger()

# Local-only market-data cache. Do not upgrade this to Redis.
cache = Cache(default_ttl=MARKET_DATA_OHLCV_CACHE_TTL)
_shared_fetcher: Optional["DataFetcher"] = None
_shared_fetcher_lock = threading.Lock()


def get_shared_fetcher() -> "DataFetcher":
    global _shared_fetcher
    if _shared_fetcher is None:
        with _shared_fetcher_lock:
            if _shared_fetcher is None:
                _shared_fetcher = DataFetcher()
    return _shared_fetcher


def _utc_now_iso() -> str:
    return datetime.utcnow().isoformat()


def _ohlcv_cache_ttl(interval: str) -> int:
    interval_key = str(interval or "").lower()
    if interval_key in {"4h", "1d", "1w"}:
        return MARKET_DATA_OHLCV_SLOW_CACHE_TTL
    return MARKET_DATA_OHLCV_CACHE_TTL


def _normalize_end_time(value: Any) -> Optional[pd.Timestamp]:
    if value in (None, ""):
        return None
    try:
        ts = pd.to_datetime(value, utc=True, errors="coerce")
        if pd.isna(ts):
            return None
        return pd.Timestamp(ts)
    except Exception:
        return None


class DataFetcher:
    """
    Deriv-first market data fetcher.

    Order of preference:
      1. Deriv live stream cache
      2. Deriv direct market data API
      3. Binance public market data for unsupported crypto assets
      4. Short-lived internal cache from recent responses
    """

    _announced_clients: set[str] = set()

    def __init__(self) -> None:
        self._ohlcv_meta: Dict[str, Dict[str, Any]] = {}
        self._rt_meta: Dict[str, Dict[str, Any]] = {}
        self._deriv_bridge = None
        self._binance_bridge = None
        self._init_clients()

    @staticmethod
    def _stamp_metadata(meta: Optional[Dict[str, Any]] = None, **updates: Any) -> Dict[str, Any]:
        payload = {
            "source": "unknown",
            "source_class": "unknown",
            "delayed": False,
            "realtime": False,
            "from_cache": False,
            "as_of_utc": _utc_now_iso(),
        }
        if meta:
            payload.update(meta)
        payload.update(updates)
        payload["as_of_utc"] = _utc_now_iso()
        return payload

    def _attach_provider_symbol(self, asset: str, category: str, meta: Dict[str, Any]) -> Dict[str, Any]:
        payload = dict(meta or {})
        source = str(payload.get("source", "")).lower()
        if source == "deriv" and "deriv_symbol" not in payload and self._deriv_bridge is not None:
            try:
                resolved = self._deriv_bridge.resolve_symbol_info(asset, category=category)
                if resolved:
                    payload["deriv_symbol"] = str(resolved.get("symbol", ""))
                    payload["deriv_display_name"] = str(
                        resolved.get("display_name") or resolved.get("display_name_long") or ""
                    )
            except Exception:
                pass
        if source == "binance" and "exchange_symbol" not in payload and self._binance_bridge is not None:
            try:
                resolved = self._binance_bridge.resolve_symbol_info(asset, category=category)
                if resolved:
                    payload["exchange_symbol"] = str(resolved.get("symbol", ""))
                    payload["exchange"] = str(resolved.get("exchange", "binance"))
            except Exception:
                pass
        return payload

    def _init_clients(self) -> None:
        try:
            from services.deriv_bridge import deriv_bridge as _deriv_bridge

            if _deriv_bridge.list_profiles():
                self._deriv_bridge = _deriv_bridge
                if "deriv" not in DataFetcher._announced_clients:
                    DataFetcher._announced_clients.add("deriv")
                    logger.info("[DataFetcher] Deriv bridge configured as the primary market-data source")
        except Exception as exc:
            logger.debug(f"[DataFetcher] Deriv bridge unavailable: {exc}")

        try:
            from services.binance_market_bridge import binance_market_bridge as _binance_bridge

            if _binance_bridge.list_profiles():
                self._binance_bridge = _binance_bridge
                if "binance" not in DataFetcher._announced_clients:
                    DataFetcher._announced_clients.add("binance")
                    logger.info("[DataFetcher] Binance public bridge configured for unsupported crypto assets")
        except Exception as exc:
            logger.debug(f"[DataFetcher] Binance bridge unavailable: {exc}")

    def get_last_ohlcv_metadata(self, asset: str, interval: str) -> Dict[str, Any]:
        return dict(self._ohlcv_meta.get(f"ohlcv:{asset}:{interval}", {}))

    def get_last_price_metadata(self, asset: str) -> Dict[str, Any]:
        return dict(self._rt_meta.get(f"rt:{asset}", {}))

    def get_market_microstructure(self, asset: str, category: str) -> Dict[str, Any]:
        if self._deriv_bridge is not None:
            try:
                micro = self._deriv_bridge.get_microstructure(asset, category=category)
                if micro:
                    return micro
            except Exception as exc:
                logger.debug(f"[DataFetcher] Deriv microstructure {asset}: {exc}")

        if self._binance_bridge is not None:
            try:
                micro = self._binance_bridge.get_microstructure(asset, category=category)
                if micro:
                    return micro
            except Exception as exc:
                logger.debug(f"[DataFetcher] Binance microstructure {asset}: {exc}")

        price, spread = self.get_real_time_price(asset, category=category)
        if price is None:
            return {}

        meta = self.get_last_price_metadata(asset)
        spread_bps = 0.0
        try:
            if float(price) > 0:
                spread_bps = round(float(spread or 0.0) / float(price) * 10000, 3)
        except Exception:
            spread_bps = 0.0

        return {
            **meta,
            "spread_bps": spread_bps,
            "tick_imbalance": 0.0,
            "book_imbalance": 0.0,
            "stop_hunt_risk": 0.0,
            "score": 0.0,
        }

    def get_ohlcv(
        self,
        asset: str,
        category: str,
        interval: Optional[str] = None,
        periods: Optional[int] = None,
        end_time: Any = None,
        closed_only: bool = False,
    ) -> Optional[pd.DataFrame]:
        interval = (interval or get_trading_timeframe(category)).lower()
        periods = int(periods or get_timeframe_periods(interval) or LOOKBACK_PERIOD)
        meta_key = f"ohlcv:{asset}:{interval}"
        normalized_end = _normalize_end_time(end_time)
        end_key = normalized_end.isoformat() if normalized_end is not None else "latest"
        cache_key = f"fetcher:{meta_key}:{category}:{periods}:{end_key}:{int(bool(closed_only))}"

        cached = cache.get(cache_key)
        if cached:
            cached_df, cached_meta = cached
            meta = self._stamp_metadata(cached_meta, from_cache=True)
            self._ohlcv_meta[meta_key] = meta
            return cached_df.copy()

        if self._deriv_bridge is not None:
            try:
                try:
                    df, deriv_meta = self._deriv_bridge.get_ohlcv(
                        asset,
                        interval,
                        periods,
                        category=category,
                        end_time=normalized_end,
                        closed_only=closed_only,
                    )
                except TypeError:
                    df, deriv_meta = self._deriv_bridge.get_ohlcv(asset, interval, periods, category=category)
                if df is not None and not df.empty:
                    if normalized_end is not None:
                        if closed_only:
                            df = df[df.index < normalized_end]
                        else:
                            df = df[df.index <= normalized_end]
                    df = df.tail(periods).copy()
                    meta = self._stamp_metadata(
                        deriv_meta,
                        source="Deriv",
                        source_class="primary_api",
                        delayed=False,
                        realtime=False,
                    )
                    meta = self._attach_provider_symbol(asset, category, meta)
                    self._ohlcv_meta[meta_key] = meta
                    cache.set(cache_key, (df.copy(), meta), ttl=_ohlcv_cache_ttl(interval))
                    self._ping_health("market_data")
                    return df
            except Exception as exc:
                logger.debug(f"[DataFetcher] Deriv OHLCV {asset}: {exc}")

        if self._binance_bridge is not None:
            try:
                try:
                    df, binance_meta = self._binance_bridge.get_ohlcv(
                        asset,
                        interval,
                        periods,
                        category=category,
                        end_time=normalized_end,
                        closed_only=closed_only,
                    )
                except TypeError:
                    df, binance_meta = self._binance_bridge.get_ohlcv(asset, interval, periods, category=category)
                if df is not None and not df.empty:
                    if normalized_end is not None:
                        if closed_only:
                            df = df[df.index < normalized_end]
                        else:
                            df = df[df.index <= normalized_end]
                    df = df.tail(periods).copy()
                    meta = self._stamp_metadata(
                        binance_meta,
                        source="Binance",
                        source_class="secondary_api",
                        delayed=False,
                        realtime=False,
                    )
                    meta = self._attach_provider_symbol(asset, category, meta)
                    self._ohlcv_meta[meta_key] = meta
                    cache.set(cache_key, (df.copy(), meta), ttl=_ohlcv_cache_ttl(interval))
                    self._ping_health("market_data")
                    return df
            except Exception as exc:
                logger.debug(f"[DataFetcher] Binance OHLCV {asset}: {exc}")

        self._ohlcv_meta[meta_key] = self._stamp_metadata(
            {"source": "unavailable", "source_class": "unavailable", "delayed": False}
        )
        return None

    def get_real_time_price(
        self,
        asset: str,
        category: str,
    ) -> Tuple[Optional[float], Optional[float]]:
        meta_key = f"rt:{asset}"
        cache_key = f"fetcher:{meta_key}:{category}"

        try:
            from websocket_dashboard import get_live_price

            live_price, live_source = get_live_price(asset, max_age_seconds=15.0)
            if live_price is not None:
                meta = self._stream_metadata(asset, category, str(live_source or "DerivStream"))
                self._rt_meta[meta_key] = meta
                return float(live_price), 0.0
        except Exception as exc:
            logger.debug(f"[DataFetcher] live stream cache {asset}: {exc}")

        if self._deriv_bridge is not None:
            try:
                price, spread, deriv_meta = self._deriv_bridge.get_quote(asset, category=category)
                if price is not None:
                    meta = self._stamp_metadata(
                        deriv_meta,
                        source="Deriv",
                        source_class="primary_api",
                    )
                    meta = self._attach_provider_symbol(asset, category, meta)
                    self._rt_meta[meta_key] = meta
                    cache.set(
                        cache_key,
                        (float(price), float(spread or 0.0), meta),
                        ttl=MARKET_DATA_QUOTE_CACHE_TTL,
                    )
                    self._ping_health("market_data")
                    return float(price), float(spread or 0.0)
            except Exception as exc:
                logger.debug(f"[DataFetcher] Deriv quote {asset}: {exc}")

        if self._binance_bridge is not None:
            try:
                price, spread, binance_meta = self._binance_bridge.get_quote(asset, category=category)
                if price is not None:
                    meta = self._stamp_metadata(
                        binance_meta,
                        source="Binance",
                        source_class="secondary_api",
                        delayed=False,
                        realtime=True,
                    )
                    meta = self._attach_provider_symbol(asset, category, meta)
                    self._rt_meta[meta_key] = meta
                    cache.set(
                        cache_key,
                        (float(price), float(spread or 0.0), meta),
                        ttl=MARKET_DATA_QUOTE_CACHE_TTL,
                    )
                    self._ping_health("market_data")
                    return float(price), float(spread or 0.0)
            except Exception as exc:
                logger.debug(f"[DataFetcher] Binance quote {asset}: {exc}")

        cached = cache.get(cache_key)
        if cached:
            price, spread, cached_meta = cached
            meta = self._stamp_metadata(cached_meta, from_cache=True)
            self._rt_meta[meta_key] = meta
            return float(price), float(spread or 0.0)

        self._rt_meta[meta_key] = self._stamp_metadata(
            {"source": "unavailable", "source_class": "unavailable", "delayed": False}
        )
        return None, None

    def _stream_metadata(self, asset: str, category: str, source: str) -> Dict[str, Any]:
        meta = self._stamp_metadata(
            {
                "source": source,
                "source_class": "stream",
                "delayed": False,
                "realtime": True,
            }
        )
        if self._deriv_bridge is not None:
            try:
                resolved = self._deriv_bridge.resolve_symbol_info(asset, category=category)
                if resolved:
                    meta.update({
                        "deriv_symbol": str(resolved.get("symbol", "")),
                        "deriv_display_name": str(
                            resolved.get("display_name") or resolved.get("display_name_long") or ""
                        ),
                    })
                    return meta
            except Exception:
                pass
        if self._binance_bridge is not None:
            try:
                resolved = self._binance_bridge.resolve_symbol_info(asset, category=category)
                if resolved:
                    meta.update({
                        "exchange_symbol": str(resolved.get("symbol", "")),
                        "exchange": str(resolved.get("exchange", "binance")),
                    })
            except Exception:
                pass
        return meta

    def get_prices_batch(self, assets: Dict[str, str]) -> Dict[str, float]:
        prices: Dict[str, float] = {}
        for asset, category in assets.items():
            price, _ = self.get_real_time_price(asset, category)
            if price is not None:
                prices[asset] = float(price)
        return prices

    def invalidate_ohlcv_cache(
        self,
        asset: str,
        *,
        category: Optional[str] = None,
        interval: Optional[str] = None,
    ) -> int:
        intervals = [str(interval).lower()] if interval else []
        removed = 0

        if intervals:
            for tf in intervals:
                prefix = f"fetcher:ohlcv:{asset}:{tf}:"
                if hasattr(cache, "delete_prefix"):
                    removed += int(cache.delete_prefix(prefix))
                self._ohlcv_meta.pop(f"ohlcv:{asset}:{tf}", None)
            return removed

        for key in list(self._ohlcv_meta.keys()):
            if key.startswith(f"ohlcv:{asset}:"):
                tf = key.split(":", 2)[-1]
                prefix = f"fetcher:ohlcv:{asset}:{tf}:"
                if hasattr(cache, "delete_prefix"):
                    removed += int(cache.delete_prefix(prefix))
                self._ohlcv_meta.pop(key, None)
        return removed

    @staticmethod
    def _ping_health(source: str) -> None:
        return None
