"""data/fetcher.py - Hybrid market data fetcher with IG-routed assets."""
from __future__ import annotations

from datetime import datetime
import math
import threading
import time
from typing import Any, Dict, Optional, Tuple

import pandas as pd

from config.config import (
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
      1. Shared live-price cache (Deriv/Binance streams plus IG routed-asset polling)
      2. IG direct market data API for routed assets/categories
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
        self._ctrader_live_bridge = None
        self._dukascopy_live_bridge = None
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
        if "deriv" in source:
            self._attach_deriv_symbol(payload, asset, category)
        if "binance" in source:
            self._attach_binance_symbol(payload, asset, category)
        if "dukascopy" in source:
            self._attach_dukascopy_symbol(payload, asset, category)
        if "fmp" in source:
            self._attach_fmp_symbol(payload, asset, category)
        if source.startswith("ig"):
            self._attach_ig_symbol(payload, asset, category)
        return payload

    @staticmethod
    def _resolve_provider_symbol(bridge, asset: str, category: str) -> Optional[Dict[str, Any]]:
        if bridge is None:
            return None
        try:
            return bridge.resolve_symbol_info(asset, category=category)
        except Exception:
            return None

    def _attach_deriv_symbol(self, payload: Dict[str, Any], asset: str, category: str) -> None:
        if "deriv_symbol" in payload or self._deriv_bridge is None:
            return
        resolved = self._resolve_provider_symbol(self._deriv_bridge, asset, category)
        if resolved:
            payload["deriv_symbol"] = str(resolved.get("symbol", ""))
            payload["deriv_display_name"] = str(resolved.get("display_name") or resolved.get("display_name_long") or "")

    def _attach_binance_symbol(self, payload: Dict[str, Any], asset: str, category: str) -> None:
        if "exchange_symbol" in payload or self._binance_bridge is None:
            return
        resolved = self._resolve_provider_symbol(self._binance_bridge, asset, category)
        if resolved:
            payload["exchange_symbol"] = str(resolved.get("symbol", ""))
            payload["exchange"] = str(resolved.get("exchange", "binance"))

    def _attach_dukascopy_symbol(self, payload: Dict[str, Any], asset: str, category: str) -> None:
        if "dukascopy_symbol" in payload or self._dukascopy_bridge is None:
            return
        resolved = self._resolve_provider_symbol(self._dukascopy_bridge, asset, category)
        if resolved:
            payload["dukascopy_symbol"] = str(resolved.get("symbol", ""))
            payload["exchange"] = str(resolved.get("exchange", "dukascopy"))

    def _attach_fmp_symbol(self, payload: Dict[str, Any], asset: str, category: str) -> None:
        if "fmp_symbol" in payload or self._fmp_bridge is None:
            return
        resolved = self._resolve_provider_symbol(self._fmp_bridge, asset, category)
        if resolved:
            payload["fmp_symbol"] = str(resolved.get("symbol", ""))
            payload["exchange"] = str(resolved.get("exchange", "fmp"))

    def _attach_ig_symbol(self, payload: Dict[str, Any], asset: str, category: str) -> None:
        if "ig_epic" in payload or self._ig_bridge is None:
            return
        resolved = self._resolve_provider_symbol(self._ig_bridge, asset, category)
        if resolved:
            payload["ig_epic"] = str(resolved.get("symbol", ""))
            payload["exchange"] = str(resolved.get("exchange", "ig"))
            payload["ig_instrument_name"] = str(resolved.get("instrument_name") or resolved.get("display_name") or "")

    @classmethod
    def _announce_client(cls, key: str, message: str) -> None:
        if key in cls._announced_clients:
            return
        cls._announced_clients.add(key)
        logger.info(message)

    def _activate_bridge(self, attr_name: str, bridge: Any, announce_key: str, message: str) -> None:
        if bridge.list_profiles():
            setattr(self, attr_name, bridge)
            self._announce_client(announce_key, message)

    def _init_local_candle_store(self) -> None:
        try:
            from services.local_candle_store import local_candle_store as _local_candle_store

            if _local_candle_store.enabled():
                self._local_candle_store = _local_candle_store
                self._announce_client("local_store", "[DataFetcher] Local candle store configured for restart-safe OHLCV continuity")
        except Exception as exc:
            logger.debug(f"[DataFetcher] Local candle store unavailable: {exc}")

    def _init_dukascopy_bridge(self) -> None:
        try:
            from services.dukascopy_history_bridge import dukascopy_history_bridge as _dukascopy_bridge

            self._activate_bridge(
                "_dukascopy_bridge",
                _dukascopy_bridge,
                "dukascopy",
                "[DataFetcher] Dukascopy bridge configured as the free historical/backfill source for forex, commodities, and indices",
            )
        except Exception as exc:
            logger.debug(f"[DataFetcher] Dukascopy bridge unavailable: {exc}")

    def _init_fmp_bridge(self) -> None:
        try:
            from services.fmp_history_bridge import fmp_history_bridge as _fmp_bridge

            self._activate_bridge(
                "_fmp_bridge",
                _fmp_bridge,
                "fmp",
                "[DataFetcher] FMP bridge configured as the secondary historical/backfill OHLCV source",
            )
        except Exception as exc:
            logger.debug(f"[DataFetcher] FMP bridge unavailable: {exc}")

    def _init_dukascopy_live_bridge(self) -> None:
        try:
            from services.dukascopy_live_depth_bridge import dukascopy_live_depth_bridge as _dukascopy_live_bridge

            self._activate_bridge(
                "_dukascopy_live_bridge",
                _dukascopy_live_bridge,
                "dukascopy_live_depth",
                "[DataFetcher] Dukascopy live-depth bridge configured for true non-crypto order-book depth",
            )
        except Exception as exc:
            logger.debug(f"[DataFetcher] Dukascopy live-depth bridge unavailable: {exc}")

    def _init_ctrader_live_bridge(self) -> None:
        try:
            from services.ctrader_live_depth_bridge import ctrader_live_depth_bridge as _ctrader_live_bridge

            self._activate_bridge(
                "_ctrader_live_bridge",
                _ctrader_live_bridge,
                "ctrader_live_depth",
                "[DataFetcher] cTrader live-depth bridge configured for preferred non-crypto order-book depth",
            )
        except Exception as exc:
            logger.debug(f"[DataFetcher] cTrader live-depth bridge unavailable: {exc}")

    def _init_ig_bridge(self) -> None:
        try:
            from services.ig_market_bridge import ig_market_bridge as _ig_bridge

            self._activate_bridge(
                "_ig_bridge",
                _ig_bridge,
                "ig",
                "[DataFetcher] IG bridge configured for routed asset market data",
            )
        except Exception as exc:
            logger.debug(f"[DataFetcher] IG bridge unavailable: {exc}")

    def _init_deriv_bridge(self) -> None:
        try:
            from services.deriv_bridge import deriv_bridge as _deriv_bridge

            self._activate_bridge(
                "_deriv_bridge",
                _deriv_bridge,
                "deriv",
                "[DataFetcher] Deriv bridge configured as the primary market-data source",
            )
        except Exception as exc:
            logger.debug(f"[DataFetcher] Deriv bridge unavailable: {exc}")

    def _init_binance_bridge(self) -> None:
        try:
            from services.binance_market_bridge import binance_market_bridge as _binance_bridge

            self._activate_bridge(
                "_binance_bridge",
                _binance_bridge,
                "binance",
                "[DataFetcher] Binance public bridge configured for unsupported crypto assets",
            )
        except Exception as exc:
            logger.debug(f"[DataFetcher] Binance bridge unavailable: {exc}")

    def _init_clients(self) -> None:
        self._init_local_candle_store()
        self._init_dukascopy_bridge()
        self._init_ctrader_live_bridge()
        self._init_dukascopy_live_bridge()
        self._init_fmp_bridge()
        self._init_ig_bridge()
        self._init_deriv_bridge()
        self._init_binance_bridge()

    def get_last_ohlcv_metadata(self, asset: str, interval: str) -> Dict[str, Any]:
        return dict(self._ohlcv_meta.get(f"ohlcv:{asset}:{interval}", {}))

    def get_last_price_metadata(self, asset: str) -> Dict[str, Any]:
        return dict(self._rt_meta.get(f"rt:{asset}", {}))

    @staticmethod
    def _ig_primary_asset(asset: str, category: str) -> bool:
        try:
            from services.market_data_router import is_ig_primary_asset

            return bool(is_ig_primary_asset(asset, category))
        except Exception:
            return False

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
        if token.startswith("ctrader"):
            return "ctrader"
        if token.startswith("duka"):
            return "dukascopy"
        if token in {"financialmodelingprep", "fmp"}:
            return "fmp"
        return token

    @staticmethod
    def _preferred_quote_provider_order(asset: str, category: str) -> Tuple[str, ...]:
        try:
            from services.market_data_router import preferred_quote_provider_order

            order = tuple(
                str(token or "").strip().lower()
                for token in preferred_quote_provider_order(asset, category)
                if str(token or "").strip()
            )
            if order:
                return order
        except Exception:
            pass
        return ("deriv", "binance") if str(category or "").strip().lower() == "crypto" else ("deriv",)

    @classmethod
    def _live_stream_source_allowed(cls, asset: str, category: str, source: str) -> bool:
        if str(category or "").strip().lower() != "crypto":
            return True
        preferred_order = cls._preferred_quote_provider_order(asset, category)
        if not preferred_order:
            return True
        preferred_provider = str(preferred_order[0] or "").strip().lower()
        if not preferred_provider:
            return True
        return cls._normalize_provider(source) == preferred_provider

    def _ohlcv_bridge_catalog(self, *, ig_primary: bool) -> Dict[str, Tuple[Any, str, str, bool, bool, bool]]:
        return {
            "ig": (self._ig_bridge if ig_primary else None, "IG", "primary_api", False, False, False),
            "dukascopy": (self._dukascopy_bridge, "Dukascopy", "secondary_api", False, False, False),
            "fmp": (self._fmp_bridge, "FMP", "secondary_api", False, False, False),
            "deriv": (self._deriv_bridge, "Deriv", "primary_api", False, False, True),
            "binance": (self._binance_bridge, "Binance", "secondary_api", False, False, True),
        }

    def _preferred_ohlcv_bridge_order(
        self,
        asset: str,
        category: str,
        provider_preference: Optional[Tuple[str, ...]] = None,
    ) -> Tuple[Tuple[Any, str, str, bool, bool, bool], ...]:
        ig_primary = self._ig_primary_asset(asset, category)
        catalog = self._ohlcv_bridge_catalog(ig_primary=ig_primary)
        default_tokens = (
            ("ig", "dukascopy", "fmp", "deriv", "binance")
            if ig_primary
            else ("dukascopy", "fmp", "deriv", "binance")
        )
        ordered_tokens: list[str] = []
        seen: set[str] = set()

        for raw_token in tuple(provider_preference or ()):
            token = self._normalize_provider(raw_token)
            if token in catalog and token not in seen:
                ordered_tokens.append(token)
                seen.add(token)
        for token in default_tokens:
            if token in catalog and token not in seen:
                ordered_tokens.append(token)
                seen.add(token)

        return tuple(catalog[token] for token in ordered_tokens)

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

    def _crypto_orderflow_snapshot(self, asset: str, category: str) -> Dict[str, Any]:
        if str(category or "").strip().lower() != "crypto":
            return {}
        try:
            from order_flow import get_imbalance as get_orderflow_imbalance
            from order_flow import get_snapshot as get_orderflow_snapshot

            symbol = self._orderflow_symbol(asset)
            snapshot = dict(get_orderflow_snapshot(symbol) or {})
            if snapshot:
                return snapshot

            imbalance = float(get_orderflow_imbalance(symbol) or 0.0)
            if abs(imbalance) >= 0.05:
                return {
                    "asset": symbol,
                    "imbalance": round(imbalance, 4),
                    "bid_vol": 0.0,
                    "ask_vol": 0.0,
                    "top_bids": [],
                    "top_asks": [],
                    "synthetic_imbalance_only": True,
                }
            return {}
        except Exception:
            return {}

    def _microstructure_from_bridge(
        self,
        bridge: Any,
        bridge_name: str,
        asset: str,
        category: str,
        orderflow_snapshot: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        if bridge is None:
            return None
        try:
            micro = bridge.get_microstructure(asset, category=category)
            if micro:
                return self._merge_orderflow_snapshot(micro, orderflow_snapshot)
        except Exception as exc:
            logger.debug(f"[DataFetcher] {bridge_name} microstructure {asset}: {exc}")
        return None

    def _ctrader_live_microstructure(self, asset: str, category: str) -> Dict[str, Any]:
        bridge = self._ctrader_live_bridge
        if bridge is None:
            return {}
        try:
            return bridge.get_microstructure(asset, category=category) or {}
        except Exception as exc:
            logger.debug(f"[DataFetcher] cTrader live-depth {asset}: {exc}")
            return {}

    def _dukascopy_live_microstructure(self, asset: str, category: str) -> Dict[str, Any]:
        bridge = self._dukascopy_live_bridge
        if bridge is None:
            return {}
        try:
            return bridge.get_microstructure(asset, category=category) or {}
        except Exception as exc:
            logger.debug(f"[DataFetcher] Dukascopy live-depth {asset}: {exc}")
            return {}

    @staticmethod
    def _coerce_float(value: Any, default: float = 0.0) -> float:
        try:
            return float(value or default)
        except Exception:
            return default

    @staticmethod
    def _top_level_price(levels: Any, *, index: int) -> float:
        try:
            entry = list(levels or [])[index]
            return float(list(entry or [])[0] or 0.0)
        except Exception:
            return 0.0

    @staticmethod
    def _book_mid_price(payload: Dict[str, Any]) -> Optional[float]:
        price = DataFetcher._coerce_float(
            payload.get("depth_mid_price", payload.get("quote_price", payload.get("price"))),
            0.0,
        )
        if price > 0.0:
            return float(price)

        bid = DataFetcher._coerce_float(payload.get("depth_bid", payload.get("bid")), 0.0)
        ask = DataFetcher._coerce_float(payload.get("depth_ask", payload.get("ask")), 0.0)
        if bid > 0.0 and ask >= bid and ask > 0.0:
            return round((bid + ask) / 2.0, 8)

        top_bid = DataFetcher._top_level_price(payload.get("orderbook_top_bids"), index=0)
        top_ask = DataFetcher._top_level_price(payload.get("orderbook_top_asks"), index=0)
        if top_bid > 0.0 and top_ask >= top_bid and top_ask > 0.0:
            return round((top_bid + top_ask) / 2.0, 8)
        return None

    @staticmethod
    def _book_spread_bps(payload: Dict[str, Any]) -> Optional[float]:
        bid = DataFetcher._coerce_float(payload.get("depth_bid", payload.get("bid")), 0.0)
        ask = DataFetcher._coerce_float(payload.get("depth_ask", payload.get("ask")), 0.0)
        if bid <= 0.0 or ask < bid or ask <= 0.0:
            bid = DataFetcher._top_level_price(payload.get("orderbook_top_bids"), index=0)
            ask = DataFetcher._top_level_price(payload.get("orderbook_top_asks"), index=0)
        mid = DataFetcher._book_mid_price(payload)
        if mid is None or mid <= 0.0 or bid <= 0.0 or ask < bid:
            return None
        return round(((ask - bid) / mid) * 10000.0, 4)

    @staticmethod
    def _external_true_depth_provider_trust(extra: Dict[str, Any]) -> float:
        provider = str(extra.get("depth_provider") or extra.get("source") or "").strip().lower()
        source_class = str(extra.get("depth_provider_class") or extra.get("source_class") or "").strip().lower()
        environment = str(extra.get("depth_environment") or extra.get("environment") or "").strip().lower()

        trust = 0.65
        if source_class == "redis_subscriber" or "orderflow" in provider:
            trust = 0.90
        elif "dukascopy" in provider:
            trust = 0.92
        elif "ctrader" in provider:
            trust = 0.78

        if "ctrader" in provider and environment and environment not in {"live", "real", "production"}:
            trust = min(trust, 0.58)

        return round(max(0.0, min(1.0, trust)), 4)

    @staticmethod
    def _external_true_depth_quote_alignment(base: Dict[str, Any], overlay: Dict[str, Any]) -> Dict[str, Any]:
        execution_price = DataFetcher._book_mid_price(base) or 0.0
        depth_price = DataFetcher._book_mid_price(overlay) or 0.0
        if execution_price <= 0.0 or depth_price <= 0.0:
            return {
                "state": "unconfirmed",
                "score": 0.55,
                "divergence_bps": None,
                "tolerance_bps": None,
                "usable": True,
            }

        divergence_bps = abs(depth_price - execution_price) / max(execution_price, 1e-9) * 10000.0
        execution_spread_bps = max(0.0, DataFetcher._coerce_float(base.get("spread_bps"), 0.0))
        depth_spread_bps = max(0.0, DataFetcher._coerce_float(DataFetcher._book_spread_bps(overlay), 0.0))
        tolerance_bps = max(1.5, execution_spread_bps * 2.5, depth_spread_bps * 2.5)
        ratio = divergence_bps / max(tolerance_bps, 1e-9)

        if ratio <= 0.50:
            state = "strong"
            score = 1.00
            usable = True
        elif ratio <= 1.00:
            state = "aligned"
            score = 0.82
            usable = True
        elif ratio <= 1.60:
            state = "divergent"
            score = 0.48
            usable = False
        else:
            state = "severe_divergence"
            score = 0.18
            usable = False

        return {
            "state": state,
            "score": round(score, 4),
            "divergence_bps": round(divergence_bps, 4),
            "tolerance_bps": round(tolerance_bps, 4),
            "usable": usable,
        }

    @staticmethod
    def _external_true_depth_information_key(extra: Dict[str, Any]) -> Tuple[int, float, float]:
        signal_strength = max(
            abs(DataFetcher._coerce_float(extra.get("book_imbalance"))),
            abs(DataFetcher._coerce_float(extra.get("tick_imbalance"))),
            abs(DataFetcher._coerce_float(extra.get("score"))),
        )
        depth_levels = int(
            extra.get("depth_levels")
            or max(
                int(extra.get("visible_bid_levels") or 0),
                int(extra.get("visible_ask_levels") or 0),
            )
            or 0
        )
        total_visible_volume = max(
            0.0,
            DataFetcher._coerce_float(extra.get("bid_vol")) + DataFetcher._coerce_float(extra.get("ask_vol")),
        )
        informative = int(depth_levels >= 2 and signal_strength >= 0.06)
        return informative, round(signal_strength, 6), round(total_visible_volume, 6)

    @staticmethod
    def _overlay_external_true_depth(base: Dict[str, Any], overlay: Dict[str, Any]) -> Dict[str, Any]:
        payload = dict(base or {})
        extra = dict(overlay or {})
        if not extra or not bool(extra.get("depth_available")):
            return payload
        age_seconds = extra.get("depth_live_age_seconds")
        try:
            if age_seconds not in (None, "") and float(age_seconds) > 30.0:
                return payload
        except Exception:
            pass

        provider_trust = DataFetcher._external_true_depth_provider_trust(extra)
        alignment = DataFetcher._external_true_depth_quote_alignment(payload, extra) if payload else {
            "state": "unconfirmed",
            "score": 0.55,
            "divergence_bps": None,
            "tolerance_bps": None,
            "usable": True,
        }

        def _attach_overlay_meta(target: Dict[str, Any]) -> Dict[str, Any]:
            target["depth_provider"] = str(extra.get("depth_provider") or extra.get("source") or "Dukascopy")
            target["depth_provider_class"] = str(extra.get("source_class") or "sidecar")
            target["depth_environment"] = str(extra.get("environment") or target.get("depth_environment") or "")
            target["depth_as_of_utc"] = str(extra.get("as_of_utc") or target.get("depth_as_of_utc") or "")
            target["depth_live_age_seconds"] = extra.get("depth_live_age_seconds")
            target["depth_provider_trust_score"] = provider_trust
            target["depth_quote_agreement_state"] = str(alignment.get("state") or "unconfirmed")
            target["depth_quote_agreement_bps"] = alignment.get("divergence_bps")
            target["depth_quote_tolerance_bps"] = alignment.get("tolerance_bps")
            target["depth_quote_alignment_score"] = alignment.get("score")
            target["external_depth_rejected"] = bool(not alignment.get("usable", True))
            if target["external_depth_rejected"]:
                target["external_depth_rejection_reason"] = "cross_provider_quote_divergence"
            else:
                target.pop("external_depth_rejection_reason", None)
            if extra.get("dukascopy_symbol"):
                target["dukascopy_symbol"] = extra["dukascopy_symbol"]
            return target

        if not payload:
            return _attach_overlay_meta(extra)

        payload = _attach_overlay_meta(payload)
        if not alignment.get("usable", True):
            return payload

        for key in (
            "tick_imbalance",
            "book_imbalance",
            "synthetic_book_imbalance",
            "velocity_bps",
            "latest_delta_bps",
            "spread_stress",
            "stop_hunt_risk",
            "exhaustion_risk",
            "pressure_direction",
            "depth_available",
            "synthetic_depth_available",
            "depth_levels",
            "bid_level_count",
            "ask_level_count",
            "depth_quality",
            "depth_quality_tier",
            "quote_updates",
            "score",
            "bid_vol",
            "ask_vol",
            "orderbook_top_bids",
            "orderbook_top_asks",
            "microstructure_source",
        ):
            if key in extra:
                payload[key] = extra[key]
        return payload

    @staticmethod
    def _select_external_true_depth(*overlays: Dict[str, Any]) -> Dict[str, Any]:
        best_overlay: Dict[str, Any] = {}
        best_key: Optional[Tuple[int, float, float, float, float, int]] = None

        for overlay in overlays:
            extra = dict(overlay or {})
            if not extra or not bool(extra.get("depth_available")):
                continue

            age_seconds = float("inf")
            try:
                if extra.get("depth_live_age_seconds") not in (None, ""):
                    age_seconds = float(extra.get("depth_live_age_seconds"))
            except Exception:
                age_seconds = float("inf")
            if math.isfinite(age_seconds) and age_seconds > 30.0:
                continue

            try:
                depth_quality = float(extra.get("depth_quality", 0.0) or 0.0)
            except Exception:
                depth_quality = 0.0
            try:
                depth_levels = int(
                    extra.get("depth_levels")
                    or max(
                        int(extra.get("visible_bid_levels") or 0),
                        int(extra.get("visible_ask_levels") or 0),
                    )
                    or 0
                )
            except Exception:
                depth_levels = 0

            informative, signal_strength, total_visible_volume = DataFetcher._external_true_depth_information_key(extra)
            provider_trust = DataFetcher._external_true_depth_provider_trust(extra)
            freshness_score = -age_seconds if math.isfinite(age_seconds) else float("-inf")
            candidate_key = (
                informative,
                signal_strength,
                provider_trust,
                depth_quality,
                freshness_score,
                depth_levels if depth_levels > 0 else int(total_visible_volume > 0.0),
            )
            if best_key is None or candidate_key > best_key:
                best_key = candidate_key
                best_overlay = extra

        return best_overlay

    def _fallback_microstructure(
        self,
        asset: str,
        category: str,
        price: float,
        spread: float,
        orderflow_snapshot: Dict[str, Any],
    ) -> Dict[str, Any]:
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
                return self._merge_orderflow_snapshot(
                    {
                        **meta,
                        "quote_price": float(price),
                        "quote_spread": float(spread or 0.0),
                        **snapshot,
                    },
                    orderflow_snapshot,
                )
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
            "quote_price": float(price),
            "quote_spread": float(spread or 0.0),
            "spread_bps": spread_bps,
            "tick_imbalance": 0.0,
            "book_imbalance": 0.0,
            "stop_hunt_risk": 0.0,
            "score": 0.0,
        }
        return self._merge_orderflow_snapshot(base, orderflow_snapshot)

    def get_market_microstructure(self, asset: str, category: str) -> Dict[str, Any]:
        orderflow_snapshot = self._crypto_orderflow_snapshot(asset, category)
        ctrader_overlay = self._ctrader_live_microstructure(asset, category)
        dukascopy_overlay = self._dukascopy_live_microstructure(asset, category)
        selected_external_depth = self._select_external_true_depth(dukascopy_overlay, ctrader_overlay)

        if self._ig_primary_asset(asset, category):
            micro = self._microstructure_from_bridge(self._ig_bridge, "IG", asset, category, orderflow_snapshot)
            if micro:
                return self._overlay_external_true_depth(micro, selected_external_depth)

        micro = self._microstructure_from_bridge(self._deriv_bridge, "Deriv", asset, category, orderflow_snapshot)
        if micro:
            return self._overlay_external_true_depth(micro, selected_external_depth)

        micro = self._microstructure_from_bridge(self._binance_bridge, "Binance", asset, category, orderflow_snapshot)
        if micro:
            return self._overlay_external_true_depth(micro, selected_external_depth)

        price, spread = self.get_real_time_price(asset, category=category)
        if price is None:
            return dict(selected_external_depth or {})
        return self._overlay_external_true_depth(
            self._fallback_microstructure(asset, category, price, spread, orderflow_snapshot),
            selected_external_depth,
        )

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

        top_bids = list(snapshot.get("top_bids", []) or [])
        top_asks = list(snapshot.get("top_asks", []) or [])
        depth_levels = max(len(top_bids), len(top_asks))
        synthetic_only = bool(snapshot.get("synthetic_imbalance_only")) or depth_levels <= 0
        depth_metrics: Dict[str, Any] = {}
        if not synthetic_only and (top_bids or top_asks):
            try:
                from services.live_microstructure_service import estimate_true_depth_metrics

                levels = []
                for idx in range(depth_levels):
                    level: Dict[str, Any] = {}
                    if idx < len(top_bids):
                        bid_level = list(top_bids[idx] or [])
                        if len(bid_level) >= 2:
                            level["bid"] = bid_level[0]
                            level["bid_size"] = bid_level[1]
                    if idx < len(top_asks):
                        ask_level = list(top_asks[idx] or [])
                        if len(ask_level) >= 2:
                            level["ask"] = ask_level[0]
                            level["ask_size"] = ask_level[1]
                    if level:
                        levels.append(level)
                depth_metrics = estimate_true_depth_metrics(
                    levels,
                    bid_size=snapshot.get("bid_vol"),
                    ask_size=snapshot.get("ask_vol"),
                )
            except Exception:
                depth_metrics = {}

        payload["book_imbalance"] = round(imbalance, 4)
        payload["depth_available"] = depth_levels > 0
        payload["synthetic_depth_available"] = synthetic_only
        payload["depth_levels"] = depth_levels
        payload["bid_vol"] = float(snapshot.get("bid_vol", 0.0) or 0.0)
        payload["ask_vol"] = float(snapshot.get("ask_vol", 0.0) or 0.0)
        payload["orderbook_top_bids"] = top_bids
        payload["orderbook_top_asks"] = top_asks
        payload["microstructure_source"] = "order_flow_synthetic_imbalance" if synthetic_only else "order_flow_true_depth"
        if depth_metrics:
            payload["depth_quality"] = round(float(depth_metrics.get("depth_quality", 0.0) or 0.0), 4)
            payload["depth_quality_tier"] = str(depth_metrics.get("depth_quality_tier") or "none")
            payload["bid_level_count"] = int(depth_metrics.get("bid_level_count", 0) or 0)
            payload["ask_level_count"] = int(depth_metrics.get("ask_level_count", 0) or 0)
        elif synthetic_only:
            payload["depth_quality"] = 0.0
            payload["depth_quality_tier"] = "synthetic"
        if depth_levels > 0 or synthetic_only:
            payload["depth_provider"] = "OrderFlow"
            payload["depth_provider_class"] = "redis_subscriber"
            payload["depth_provider_trust_score"] = 0.90 if depth_levels > 0 else 0.52
        top_bid = DataFetcher._top_level_price(top_bids, index=0)
        top_ask = DataFetcher._top_level_price(top_asks, index=0)
        if top_bid > 0.0 and top_ask >= top_bid and top_ask > 0.0:
            mid = round((top_bid + top_ask) / 2.0, 8)
            payload["depth_bid"] = top_bid
            payload["depth_ask"] = top_ask
            payload["depth_mid_price"] = mid
            payload["depth_spread_bps"] = round(((top_ask - top_bid) / mid) * 10000.0, 4) if mid > 0.0 else 0.0
        ts_value = snapshot.get("ts")
        try:
            ts_float = float(ts_value or 0.0)
            if ts_float > 10_000_000_000:
                ts_float /= 1000.0
            if ts_float > 1_000_000:
                payload["depth_live_age_seconds"] = round(max(0.0, time.time() - ts_float), 3)
        except Exception:
            pass
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
        *,
        prefer_local: bool = True,
        provider_preference: Optional[Tuple[str, ...]] = None,
    ) -> Optional[pd.DataFrame]:
        interval = (interval or get_trading_timeframe(category)).lower()
        periods = int(periods or get_timeframe_periods(interval) or LOOKBACK_PERIOD)
        meta_key = f"ohlcv:{asset}:{interval}"
        normalized_end = _normalize_end_time(end_time)
        end_key = normalized_end.isoformat() if normalized_end is not None else "latest"
        preference_token = ",".join(
            token for token in (self._normalize_provider(item) for item in tuple(provider_preference or ())) if token
        ) or "default"
        cache_key = (
            f"fetcher:{meta_key}:{category}:{periods}:{end_key}:{int(bool(closed_only))}:"
            f"{int(bool(prefer_local))}:{preference_token}"
        )

        cached = self._cached_ohlcv_frame(cache_key, meta_key)
        if cached is not None:
            return cached

        local_hit, local_partial_df, local_partial_meta = self._fetch_local_ohlcv(
            asset,
            category,
            interval,
            periods,
            normalized_end,
            closed_only,
            meta_key,
            cache_key,
            allow_short_circuit=prefer_local,
        )
        if local_hit is not None:
            return local_hit

        last_error_meta: Optional[Dict[str, Any]] = None
        bridge_order = self._preferred_ohlcv_bridge_order(asset, category, provider_preference)
        for bridge, source, source_class, delayed, realtime, allow_legacy_signature in bridge_order:
            df, error_meta = self._fetch_ohlcv_from_bridge(
                bridge,
                asset,
                category,
                interval,
                periods,
                normalized_end,
                closed_only,
                meta_key,
                cache_key,
                source=source,
                source_class=source_class,
                delayed=delayed,
                realtime=realtime,
                allow_legacy_signature=allow_legacy_signature,
            )
            if df is not None:
                return df
            if error_meta is not None:
                last_error_meta = error_meta

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
        *,
        prefer_live_stream: bool = True,
        allow_cached_quote: bool = True,
    ) -> Tuple[Optional[float], Optional[float]]:
        meta_key = f"rt:{asset}"
        cache_key = f"fetcher:{meta_key}:{category}"
        last_error_meta: Optional[Dict[str, Any]] = None
        ig_primary = self._ig_primary_asset(asset, category) and self._ig_bridge is not None

        if prefer_live_stream:
            live_price = self._live_stream_real_time_price(asset, category, meta_key)
            if live_price is not None:
                return live_price

        if ig_primary and allow_cached_quote:
            cached = self._cached_real_time_price(cache_key, meta_key)
            if cached is not None:
                return cached

        if ig_primary:
            price, spread, last_error_meta = self._fetch_ig_real_time_price(asset, category, meta_key, cache_key)
            if price is not None:
                return price, spread

        bridge_catalog = {
            "deriv": (
                self._deriv_bridge,
                "Deriv",
                "primary_api",
                "Deriv",
                False,
            ),
            "binance": (
                self._binance_bridge,
                "Binance",
                "secondary_api",
                "Binance",
                True,
            ),
        }
        for provider in self._preferred_quote_provider_order(asset, category):
            bridge_entry = bridge_catalog.get(str(provider or "").strip().lower())
            if bridge_entry is None:
                continue
            bridge, source, source_class, log_label, realtime = bridge_entry
            price, spread = self._fetch_standard_real_time_price(
                bridge,
                asset,
                category,
                meta_key,
                cache_key,
                source=source,
                source_class=source_class,
                log_label=log_label,
                realtime=realtime,
            )
            if price is not None:
                return price, spread

        if not prefer_live_stream:
            live_price = self._live_stream_real_time_price(asset, category, meta_key)
            if live_price is not None:
                return live_price

        if allow_cached_quote:
            cached = self._cached_real_time_price(cache_key, meta_key)
            if cached is not None:
                return cached

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

    def _cached_real_time_price(self, cache_key: str, meta_key: str) -> Optional[Tuple[float, float]]:
        cached = cache.get(cache_key)
        if not cached:
            return None
        price, spread, cached_meta = cached
        meta = self._stamp_metadata(cached_meta, from_cache=True)
        self._rt_meta[meta_key] = meta
        return float(price), float(spread or 0.0)

    def _live_stream_real_time_price(self, asset: str, category: str, meta_key: str) -> Optional[Tuple[float, float]]:
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
                if live_price <= 0:
                    return None
                if not self._live_stream_source_allowed(asset, category, live_source):
                    logger.debug(
                        f"[DataFetcher] ignoring live stream source {live_source} for {asset}; preferred provider mismatch"
                    )
                    return None
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
        return None

    def _fetch_ig_real_time_price(
        self,
        asset: str,
        category: str,
        meta_key: str,
        cache_key: str,
    ) -> Tuple[Optional[float], Optional[float], Optional[Dict[str, Any]]]:
        if self._ig_bridge is None:
            return None, None, None
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
                return float(price), float(spread or 0.0), None
            if ig_meta:
                last_error_meta = self._stamp_metadata(
                    ig_meta,
                    source="IG",
                    source_class="primary_api",
                    delayed=bool((ig_meta or {}).get("delayed", False)),
                    realtime=bool((ig_meta or {}).get("realtime", True)),
                )
                last_error_meta = self._attach_provider_symbol(asset, category, last_error_meta)
                return None, None, last_error_meta
        except Exception as exc:
            logger.debug(f"[DataFetcher] IG quote {asset}: {exc}")
        return None, None, None

    def _fetch_standard_real_time_price(
        self,
        bridge: Any,
        asset: str,
        category: str,
        meta_key: str,
        cache_key: str,
        *,
        source: str,
        source_class: str,
        log_label: str,
        realtime: bool,
    ) -> Tuple[Optional[float], Optional[float]]:
        if bridge is None:
            return None, None
        try:
            price, spread, provider_meta = bridge.get_quote(asset, category=category)
            if price is not None:
                meta = self._stamp_metadata(
                    provider_meta,
                    source=source,
                    source_class=source_class,
                    delayed=False,
                    realtime=realtime,
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
            logger.debug(f"[DataFetcher] {log_label} quote {asset}: {exc}")
        return None, None

    def _cached_ohlcv_frame(self, cache_key: str, meta_key: str) -> Optional[pd.DataFrame]:
        cached = cache.get(cache_key)
        if not cached:
            return None
        cached_df, cached_meta = cached
        meta = self._stamp_metadata(cached_meta, from_cache=True)
        self._ohlcv_meta[meta_key] = meta
        self._ping_health("technicals")
        return cached_df.copy()

    @staticmethod
    def _trim_ohlcv_frame(
        df: pd.DataFrame,
        periods: int,
        normalized_end: Optional[pd.Timestamp],
        closed_only: bool,
    ) -> pd.DataFrame:
        frame = df
        if normalized_end is not None:
            if closed_only:
                frame = frame[frame.index < normalized_end]
            else:
                frame = frame[frame.index <= normalized_end]
        return frame.tail(periods).copy()

    def _prepare_ohlcv_meta(
        self,
        asset: str,
        category: str,
        meta: Optional[Dict[str, Any]],
        *,
        source: str,
        source_class: str,
        delayed: bool,
        realtime: bool,
    ) -> Dict[str, Any]:
        stamped = self._stamp_metadata(
            meta,
            source=source,
            source_class=source_class,
            delayed=delayed,
            realtime=realtime,
        )
        return self._attach_provider_symbol(asset, category, stamped)

    def _store_ohlcv_result(
        self,
        asset: str,
        category: str,
        interval: str,
        meta_key: str,
        cache_key: str,
        df: pd.DataFrame,
        meta: Dict[str, Any],
        *,
        persist_local: bool = True,
    ) -> None:
        if persist_local and self._local_candle_store is not None and hasattr(self._local_candle_store, "store_ohlcv"):
            self._local_candle_store.store_ohlcv(asset, category, interval, df, meta)
        self._ohlcv_meta[meta_key] = meta
        cache.set(cache_key, (df.copy(), meta), ttl=_ohlcv_cache_ttl(interval))
        self._ping_health("technicals")

    def _fetch_local_ohlcv(
        self,
        asset: str,
        category: str,
        interval: str,
        periods: int,
        normalized_end: Optional[pd.Timestamp],
        closed_only: bool,
        meta_key: str,
        cache_key: str,
        *,
        allow_short_circuit: bool = True,
    ) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame], Optional[Dict[str, Any]]]:
        if self._local_candle_store is None:
            return None, None, None
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
                local_df = self._trim_ohlcv_frame(local_df, periods, normalized_end, closed_only)
                meta = self._prepare_ohlcv_meta(
                    asset,
                    category,
                    local_meta,
                    source="LocalStore",
                    source_class="local_store",
                    delayed=False,
                    realtime=bool((local_meta or {}).get("realtime", False)),
                )
                if self._local_history_satisfies(local_df, periods):
                    if allow_short_circuit:
                        self._store_ohlcv_result(
                            asset,
                            category,
                            interval,
                            meta_key,
                            cache_key,
                            local_df,
                            meta,
                            persist_local=False,
                        )
                        return local_df, None, None
                    return None, local_df, meta
                return None, local_df, meta
        except Exception as exc:
            logger.debug(f"[DataFetcher] LocalStore OHLCV {asset}: {exc}")
        return None, None, None

    def _fetch_ohlcv_from_bridge(
        self,
        bridge: Any,
        asset: str,
        category: str,
        interval: str,
        periods: int,
        normalized_end: Optional[pd.Timestamp],
        closed_only: bool,
        meta_key: str,
        cache_key: str,
        *,
        source: str,
        source_class: str,
        delayed: bool,
        realtime: bool,
        allow_legacy_signature: bool,
    ) -> Tuple[Optional[pd.DataFrame], Optional[Dict[str, Any]]]:
        if bridge is None:
            return None, None
        try:
            try:
                df, bridge_meta = bridge.get_ohlcv(
                    asset,
                    interval,
                    periods,
                    category=category,
                    end_time=normalized_end,
                    closed_only=closed_only,
                )
            except TypeError:
                if not allow_legacy_signature:
                    raise
                df, bridge_meta = bridge.get_ohlcv(asset, interval, periods, category=category)
            if df is not None and not df.empty:
                df = self._trim_ohlcv_frame(df, periods, normalized_end, closed_only)
                meta = self._prepare_ohlcv_meta(
                    asset,
                    category,
                    bridge_meta,
                    source=source,
                    source_class=source_class,
                    delayed=delayed,
                    realtime=realtime,
                )
                self._store_ohlcv_result(asset, category, interval, meta_key, cache_key, df, meta)
                return df, None
            if bridge_meta:
                meta = self._prepare_ohlcv_meta(
                    asset,
                    category,
                    bridge_meta,
                    source=source,
                    source_class=source_class,
                    delayed=delayed,
                    realtime=realtime,
                )
                return None, meta
        except Exception as exc:
            logger.debug(f"[DataFetcher] {source} OHLCV {asset}: {exc}")
        return None, None

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
