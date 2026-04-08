"""data/fetcher.py - Hybrid market data fetcher with IG-routed commodities."""
from __future__ import annotations

from datetime import datetime
import math
import threading
from typing import Any, Dict, Optional, Tuple

import pandas as pd

from config.config import (
    IG_ROUTED_CATEGORIES,
    LOOKBACK_PERIOD,
    LOCAL_CANDLE_STORE_REQUIRED_COVERAGE,
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
    Hybrid market data fetcher.

    Order of preference:
      1. Shared live-price cache (Deriv/Binance streams plus IG commodity poller)
      2. IG direct market data API for routed categories (currently commodities)
      3. Deriv direct market data API
      4. Binance public market data for unsupported crypto assets
      5. Short-lived internal cache from recent responses
    """

    _announced_clients: set[str] = set()

    def __init__(self) -> None:
        self._ohlcv_meta: Dict[str, Dict[str, Any]] = {}
        self._rt_meta: Dict[str, Dict[str, Any]] = {}
        self._local_candle_store = None
        self._dukascopy_bridge = None
        self._fmp_bridge = None
        self._ig_bridge = None
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
        if "deriv" in source and "deriv_symbol" not in payload and self._deriv_bridge is not None:
            try:
                resolved = self._deriv_bridge.resolve_symbol_info(asset, category=category)
                if resolved:
                    payload["deriv_symbol"] = str(resolved.get("symbol", ""))
                    payload["deriv_display_name"] = str(
                        resolved.get("display_name") or resolved.get("display_name_long") or ""
                    )
            except Exception:
                pass
        if "binance" in source and "exchange_symbol" not in payload and self._binance_bridge is not None:
            try:
                resolved = self._binance_bridge.resolve_symbol_info(asset, category=category)
                if resolved:
                    payload["exchange_symbol"] = str(resolved.get("symbol", ""))
                    payload["exchange"] = str(resolved.get("exchange", "binance"))
            except Exception:
                pass
        if "dukascopy" in source and "dukascopy_symbol" not in payload and self._dukascopy_bridge is not None:
            try:
                resolved = self._dukascopy_bridge.resolve_symbol_info(asset, category=category)
                if resolved:
                    payload["dukascopy_symbol"] = str(resolved.get("symbol", ""))
                    payload["exchange"] = str(resolved.get("exchange", "dukascopy"))
            except Exception:
                pass
        if "fmp" in source and "fmp_symbol" not in payload and self._fmp_bridge is not None:
            try:
                resolved = self._fmp_bridge.resolve_symbol_info(asset, category=category)
                if resolved:
                    payload["fmp_symbol"] = str(resolved.get("symbol", ""))
                    payload["exchange"] = str(resolved.get("exchange", "fmp"))
            except Exception:
                pass
        if source.startswith("ig") and "ig_epic" not in payload and self._ig_bridge is not None:
            try:
                resolved = self._ig_bridge.resolve_symbol_info(asset, category=category)
                if resolved:
                    payload["ig_epic"] = str(resolved.get("symbol", ""))
                    payload["exchange"] = str(resolved.get("exchange", "ig"))
                    payload["ig_instrument_name"] = str(resolved.get("instrument_name") or resolved.get("display_name") or "")
            except Exception:
                pass
        return payload

    def _init_clients(self) -> None:
        try:
            from services.local_candle_store import local_candle_store as _local_candle_store

            if _local_candle_store.enabled():
                self._local_candle_store = _local_candle_store
                if "local_store" not in DataFetcher._announced_clients:
                    DataFetcher._announced_clients.add("local_store")
                    logger.info("[DataFetcher] Local candle store configured for restart-safe OHLCV continuity")
        except Exception as exc:
            logger.debug(f"[DataFetcher] Local candle store unavailable: {exc}")

        try:
            from services.dukascopy_history_bridge import dukascopy_history_bridge as _dukascopy_bridge

            if _dukascopy_bridge.list_profiles():
                self._dukascopy_bridge = _dukascopy_bridge
                if "dukascopy" not in DataFetcher._announced_clients:
                    DataFetcher._announced_clients.add("dukascopy")
                    logger.info("[DataFetcher] Dukascopy bridge configured as the free historical/backfill source for forex, commodities, and indices")
        except Exception as exc:
            logger.debug(f"[DataFetcher] Dukascopy bridge unavailable: {exc}")

        try:
            from services.fmp_history_bridge import fmp_history_bridge as _fmp_bridge

            if _fmp_bridge.list_profiles():
                self._fmp_bridge = _fmp_bridge
                if "fmp" not in DataFetcher._announced_clients:
                    DataFetcher._announced_clients.add("fmp")
                    logger.info("[DataFetcher] FMP bridge configured as the secondary historical/backfill OHLCV source")
        except Exception as exc:
            logger.debug(f"[DataFetcher] FMP bridge unavailable: {exc}")

        try:
            from services.ig_market_bridge import ig_market_bridge as _ig_bridge

            if _ig_bridge.list_profiles():
                self._ig_bridge = _ig_bridge
                if "ig" not in DataFetcher._announced_clients:
                    DataFetcher._announced_clients.add("ig")
                    logger.info("[DataFetcher] IG bridge configured for routed commodity market data")
        except Exception as exc:
            logger.debug(f"[DataFetcher] IG bridge unavailable: {exc}")

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

    @staticmethod
    def _ig_primary_category(category: str) -> bool:
        return str(category or "").strip().lower() in set(IG_ROUTED_CATEGORIES or [])

    @staticmethod
    def _orderflow_symbol(asset: str) -> str:
        return str(asset or "").replace("-USD", "USDT").replace("/", "").replace("-", "")

    @staticmethod
    def _normalize_provider(provider: str) -> str:
        token = str(provider or "").strip().lower()
        if token.startswith("ig"):
            return "ig"
        if token.startswith("deriv"):
            return "deriv"
        if token.startswith("binance"):
            return "binance"
        return token

    @staticmethod
    def _local_history_satisfies(df: Optional[pd.DataFrame], periods: int) -> bool:
        if df is None or df.empty:
            return False
        required = max(1, int(math.ceil(float(periods) * float(LOCAL_CANDLE_STORE_REQUIRED_COVERAGE))))
        return len(df) >= required

    def get_provider_quote(
        self,
        asset: str,
        category: str,
        provider: str,
    ) -> Tuple[Optional[float], Optional[float], Dict[str, Any]]:
        target = self._normalize_provider(provider)
        if target == "ig" and self._ig_bridge is not None:
            if not self._ig_bridge.supports(asset, category=category):
                return None, None, {}
            return self._ig_bridge.get_quote(asset, category=category)
        if target == "deriv" and self._deriv_bridge is not None:
            if not self._deriv_bridge.is_available(asset, category=category):
                return None, None, {}
            return self._deriv_bridge.get_quote(asset, category=category)
        if target == "binance" and self._binance_bridge is not None:
            if not self._binance_bridge.supports(asset, category=category):
                return None, None, {}
            return self._binance_bridge.get_quote(asset, category=category)
        return None, None, {}

    def get_provider_market_status(self, asset: str, category: str, provider: str) -> Optional[Dict[str, Any]]:
        target = self._normalize_provider(provider)
        if target == "ig" and self._ig_bridge is not None:
            if not self._ig_bridge.supports(asset, category=category):
                return None
            return self._ig_bridge.get_market_status(asset, category=category)
        if target == "deriv" and self._deriv_bridge is not None:
            if not self._deriv_bridge.is_available(asset, category=category):
                return None
            return self._deriv_bridge.get_market_status(asset, category=category)
        return None

    def get_market_microstructure(self, asset: str, category: str) -> Dict[str, Any]:
        orderflow_snapshot: Dict[str, Any] = {}
        if str(category or "").strip().lower() == "crypto":
            try:
                from order_flow import get_snapshot as get_orderflow_snapshot

                orderflow_snapshot = get_orderflow_snapshot(self._orderflow_symbol(asset)) or {}
            except Exception:
                orderflow_snapshot = {}

        if self._ig_primary_category(category) and self._ig_bridge is not None:
            try:
                micro = self._ig_bridge.get_microstructure(asset, category=category)
                if micro:
                    return self._merge_orderflow_snapshot(micro, orderflow_snapshot)
            except Exception as exc:
                logger.debug(f"[DataFetcher] IG microstructure {asset}: {exc}")

        if self._deriv_bridge is not None:
            try:
                micro = self._deriv_bridge.get_microstructure(asset, category=category)
                if micro:
                    return self._merge_orderflow_snapshot(micro, orderflow_snapshot)
            except Exception as exc:
                logger.debug(f"[DataFetcher] Deriv microstructure {asset}: {exc}")

        if self._binance_bridge is not None:
            try:
                micro = self._binance_bridge.get_microstructure(asset, category=category)
                if micro:
                    return self._merge_orderflow_snapshot(micro, orderflow_snapshot)
            except Exception as exc:
                logger.debug(f"[DataFetcher] Binance microstructure {asset}: {exc}")

        price, spread = self.get_real_time_price(asset, category=category)
        if price is None:
            return {}

        meta = self.get_last_price_metadata(asset)
        try:
            from services.live_microstructure_service import get_service as get_live_microstructure_service

            provider = str((meta or {}).get("source") or "")
            snapshot = get_live_microstructure_service().get_snapshot(
                provider,
                asset,
                price=price,
                spread=spread,
                meta=meta,
            )
            if snapshot:
                return self._merge_orderflow_snapshot({**meta, **snapshot}, orderflow_snapshot)
        except Exception:
            pass
        spread_bps = 0.0
        try:
            if float(price) > 0:
                spread_bps = round(float(spread or 0.0) / float(price) * 10000, 3)
        except Exception:
            spread_bps = 0.0

        base = {
            **meta,
            "spread_bps": spread_bps,
            "tick_imbalance": 0.0,
            "book_imbalance": 0.0,
            "stop_hunt_risk": 0.0,
            "score": 0.0,
        }
        return self._merge_orderflow_snapshot(base, orderflow_snapshot)

    @staticmethod
    def _merge_orderflow_snapshot(micro: Dict[str, Any], orderflow_snapshot: Dict[str, Any]) -> Dict[str, Any]:
        payload = dict(micro or {})
        snapshot = dict(orderflow_snapshot or {})
        if not snapshot:
            return payload

        imbalance = 0.0
        try:
            imbalance = float(snapshot.get("imbalance", 0.0) or 0.0)
        except Exception:
            imbalance = 0.0

        spread_pct = 0.0
        try:
            spread_pct = float(snapshot.get("spread_pct", 0.0) or 0.0)
        except Exception:
            spread_pct = 0.0

        existing_score = 0.0
        try:
            existing_score = float(payload.get("score", 0.0) or 0.0)
        except Exception:
            existing_score = 0.0

        payload["book_imbalance"] = round(imbalance, 4)
        payload["depth_available"] = True
        payload["synthetic_depth_available"] = False
        payload["depth_levels"] = max(len(snapshot.get("top_bids", []) or []), len(snapshot.get("top_asks", []) or []))
        payload["bid_vol"] = float(snapshot.get("bid_vol", 0.0) or 0.0)
        payload["ask_vol"] = float(snapshot.get("ask_vol", 0.0) or 0.0)
        payload["orderbook_top_bids"] = list(snapshot.get("top_bids", []) or [])
        payload["orderbook_top_asks"] = list(snapshot.get("top_asks", []) or [])
        payload["microstructure_source"] = "order_flow_true_depth"
        if spread_pct > 0.0:
            payload["spread_bps"] = round(spread_pct * 100.0, 3)
        payload["score"] = round(
            max(-1.0, min(1.0, existing_score * 0.45 + imbalance * 0.55)),
            4,
        )
        return payload

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
            self._ping_health("technicals")
            return cached_df.copy()

        last_error_meta: Optional[Dict[str, Any]] = None
        local_partial_df: Optional[pd.DataFrame] = None
        local_partial_meta: Optional[Dict[str, Any]] = None

        if self._local_candle_store is not None:
            try:
                local_df, local_meta = self._local_candle_store.get_ohlcv(
                    asset,
                    category,
                    interval,
                    periods,
                    end_time=normalized_end,
                    closed_only=closed_only,
                )
                if local_df is not None and not local_df.empty:
                    local_df = local_df.tail(periods).copy()
                    meta = self._stamp_metadata(
                        local_meta,
                        source="LocalStore",
                        source_class="local_store",
                        delayed=False,
                        realtime=bool((local_meta or {}).get("realtime", False)),
                    )
                    meta = self._attach_provider_symbol(asset, category, meta)
                    if self._local_history_satisfies(local_df, periods):
                        self._ohlcv_meta[meta_key] = meta
                        cache.set(cache_key, (local_df.copy(), meta), ttl=_ohlcv_cache_ttl(interval))
                        self._ping_health("technicals")
                        return local_df
                    local_partial_df = local_df
                    local_partial_meta = meta
            except Exception as exc:
                logger.debug(f"[DataFetcher] LocalStore OHLCV {asset}: {exc}")

        if self._dukascopy_bridge is not None:
            try:
                df, dukascopy_meta = self._dukascopy_bridge.get_ohlcv(
                    asset,
                    interval,
                    periods,
                    category=category,
                    end_time=normalized_end,
                    closed_only=closed_only,
                )
                if df is not None and not df.empty:
                    if normalized_end is not None:
                        if closed_only:
                            df = df[df.index < normalized_end]
                        else:
                            df = df[df.index <= normalized_end]
                    df = df.tail(periods).copy()
                    meta = self._stamp_metadata(
                        dukascopy_meta,
                        source="Dukascopy",
                        source_class="secondary_api",
                        delayed=False,
                        realtime=False,
                    )
                    meta = self._attach_provider_symbol(asset, category, meta)
                    if self._local_candle_store is not None:
                        self._local_candle_store.store_ohlcv(asset, category, interval, df, meta)
                    self._ohlcv_meta[meta_key] = meta
                    cache.set(cache_key, (df.copy(), meta), ttl=_ohlcv_cache_ttl(interval))
                    self._ping_health("technicals")
                    return df
                if dukascopy_meta:
                    last_error_meta = self._stamp_metadata(
                        dukascopy_meta,
                        source="Dukascopy",
                        source_class="secondary_api",
                        delayed=False,
                        realtime=False,
                    )
                    last_error_meta = self._attach_provider_symbol(asset, category, last_error_meta)
            except Exception as exc:
                logger.debug(f"[DataFetcher] Dukascopy OHLCV {asset}: {exc}")

        if self._fmp_bridge is not None:
            try:
                df, fmp_meta = self._fmp_bridge.get_ohlcv(
                    asset,
                    interval,
                    periods,
                    category=category,
                    end_time=normalized_end,
                    closed_only=closed_only,
                )
                if df is not None and not df.empty:
                    if normalized_end is not None:
                        if closed_only:
                            df = df[df.index < normalized_end]
                        else:
                            df = df[df.index <= normalized_end]
                    df = df.tail(periods).copy()
                    meta = self._stamp_metadata(
                        fmp_meta,
                        source="FMP",
                        source_class="secondary_api",
                        delayed=False,
                        realtime=False,
                    )
                    meta = self._attach_provider_symbol(asset, category, meta)
                    if self._local_candle_store is not None:
                        self._local_candle_store.store_ohlcv(asset, category, interval, df, meta)
                    self._ohlcv_meta[meta_key] = meta
                    cache.set(cache_key, (df.copy(), meta), ttl=_ohlcv_cache_ttl(interval))
                    self._ping_health("technicals")
                    return df
                if fmp_meta:
                    last_error_meta = self._stamp_metadata(
                        fmp_meta,
                        source="FMP",
                        source_class="secondary_api",
                        delayed=False,
                        realtime=False,
                    )
                    last_error_meta = self._attach_provider_symbol(asset, category, last_error_meta)
            except Exception as exc:
                logger.debug(f"[DataFetcher] FMP OHLCV {asset}: {exc}")

        if self._ig_primary_category(category) and self._ig_bridge is not None:
            try:
                df, ig_meta = self._ig_bridge.get_ohlcv(
                    asset,
                    interval,
                    periods,
                    category=category,
                    end_time=normalized_end,
                    closed_only=closed_only,
                )
                if df is not None and not df.empty:
                    if normalized_end is not None:
                        if closed_only:
                            df = df[df.index < normalized_end]
                        else:
                            df = df[df.index <= normalized_end]
                    df = df.tail(periods).copy()
                    meta = self._stamp_metadata(
                        ig_meta,
                        source="IG",
                        source_class="primary_api",
                        delayed=bool((ig_meta or {}).get("delayed", False)),
                        realtime=False,
                    )
                    meta = self._attach_provider_symbol(asset, category, meta)
                    if self._local_candle_store is not None:
                        self._local_candle_store.store_ohlcv(asset, category, interval, df, meta)
                    self._ohlcv_meta[meta_key] = meta
                    cache.set(cache_key, (df.copy(), meta), ttl=_ohlcv_cache_ttl(interval))
                    self._ping_health("technicals")
                    return df
                if ig_meta:
                    last_error_meta = self._stamp_metadata(
                        ig_meta,
                        source="IG",
                        source_class="primary_api",
                        delayed=bool((ig_meta or {}).get("delayed", False)),
                        realtime=False,
                    )
                    last_error_meta = self._attach_provider_symbol(asset, category, last_error_meta)
            except Exception as exc:
                logger.debug(f"[DataFetcher] IG OHLCV {asset}: {exc}")

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
                    if self._local_candle_store is not None:
                        self._local_candle_store.store_ohlcv(asset, category, interval, df, meta)
                    self._ohlcv_meta[meta_key] = meta
                    cache.set(cache_key, (df.copy(), meta), ttl=_ohlcv_cache_ttl(interval))
                    self._ping_health("technicals")
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
                    if self._local_candle_store is not None:
                        self._local_candle_store.store_ohlcv(asset, category, interval, df, meta)
                    self._ohlcv_meta[meta_key] = meta
                    cache.set(cache_key, (df.copy(), meta), ttl=_ohlcv_cache_ttl(interval))
                    self._ping_health("technicals")
                    return df
            except Exception as exc:
                logger.debug(f"[DataFetcher] Binance OHLCV {asset}: {exc}")

        if local_partial_df is not None and not local_partial_df.empty:
            partial_meta = self._stamp_metadata(
                local_partial_meta,
                provider_constrained=True,
                local_rows=int(len(local_partial_df)),
                requested_rows=int(periods),
            )
            self._ohlcv_meta[meta_key] = partial_meta
            return local_partial_df.copy()

        self._ohlcv_meta[meta_key] = last_error_meta or self._stamp_metadata(
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
        last_error_meta: Optional[Dict[str, Any]] = None
        ig_primary = self._ig_primary_category(category) and self._ig_bridge is not None

        if ig_primary:
            cached = cache.get(cache_key)
            if cached:
                price, spread, cached_meta = cached
                meta = self._stamp_metadata(cached_meta, from_cache=True)
                self._rt_meta[meta_key] = meta
                return float(price), float(spread or 0.0)

        try:
            from websocket_dashboard import get_live_price_snapshot

            live_snapshot = get_live_price_snapshot(asset, max_age_seconds=15.0)
            if live_snapshot is None:
                try:
                    from websocket_dashboard import get_live_price

                    live_price, live_source = get_live_price(asset, max_age_seconds=15.0)
                    if live_price is not None:
                        live_snapshot = {
                            "price": float(live_price),
                            "source": str(live_source or "LiveCache"),
                            "age_seconds": 0.0,
                        }
                except Exception:
                    live_snapshot = None
            if live_snapshot is not None:
                live_price = float(live_snapshot.get("price", 0.0) or 0.0)
                live_source = str(live_snapshot.get("source") or "LiveCache")
                meta = self._stream_metadata(asset, category, live_source)
                meta["live_age_seconds"] = round(float(live_snapshot.get("age_seconds", 0.0) or 0.0), 3)
                if meta["live_age_seconds"] <= 2.0:
                    meta["quote_freshness"] = "fresh"
                elif meta["live_age_seconds"] <= 8.0:
                    meta["quote_freshness"] = "aging"
                else:
                    meta["quote_freshness"] = "stale"
                self._rt_meta[meta_key] = meta
                self._ping_health("trades")
                return float(live_price), 0.0
        except Exception as exc:
            logger.debug(f"[DataFetcher] live stream cache {asset}: {exc}")

        if ig_primary:
            try:
                price, spread, ig_meta = self._ig_bridge.get_quote(asset, category=category)
                if price is not None:
                    meta = self._stamp_metadata(
                        ig_meta,
                        source="IG",
                        source_class="primary_api",
                        delayed=bool((ig_meta or {}).get("delayed", False)),
                        realtime=bool((ig_meta or {}).get("realtime", True)),
                    )
                    meta = self._attach_provider_symbol(asset, category, meta)
                    self._rt_meta[meta_key] = meta
                    cache.set(
                        cache_key,
                        (float(price), float(spread or 0.0), meta),
                        ttl=MARKET_DATA_QUOTE_CACHE_TTL,
                    )
                    self._ping_health("trades")
                    return float(price), float(spread or 0.0)
                if ig_meta:
                    last_error_meta = self._stamp_metadata(
                        ig_meta,
                        source="IG",
                        source_class="primary_api",
                        delayed=bool((ig_meta or {}).get("delayed", False)),
                        realtime=bool((ig_meta or {}).get("realtime", True)),
                    )
                    last_error_meta = self._attach_provider_symbol(asset, category, last_error_meta)
            except Exception as exc:
                logger.debug(f"[DataFetcher] IG quote {asset}: {exc}")

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
                    self._ping_health("trades")
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
                    self._ping_health("trades")
                    return float(price), float(spread or 0.0)
            except Exception as exc:
                logger.debug(f"[DataFetcher] Binance quote {asset}: {exc}")

        cached = cache.get(cache_key)
        if cached:
            price, spread, cached_meta = cached
            meta = self._stamp_metadata(cached_meta, from_cache=True)
            self._rt_meta[meta_key] = meta
            return float(price), float(spread or 0.0)

        self._rt_meta[meta_key] = last_error_meta or self._stamp_metadata(
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
        return self._attach_provider_symbol(asset, category, meta)

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
        try:
            from monitoring.system_health_service import monitor

            monitor.ping_source(str(source or ""))
        except Exception:
            return None
