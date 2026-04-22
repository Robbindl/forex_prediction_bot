from __future__ import annotations

import json
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Tuple
from urllib.parse import quote as _urlquote

import pandas as pd
import requests

from config.config import (
    IG_ACCOUNT_ID,
    IG_API_KEY,
    IG_ENABLED,
    IG_ENVIRONMENT,
    IG_EPIC_MAP,
    IG_IDENTIFIER,
    IG_PASSWORD,
    IG_ROUTED_ASSETS,
    IG_ROUTED_CATEGORIES,
)
from services.market_hours_guard import build_market_status
from utils.logger import get_logger

logger = get_logger()

_BASE_URLS = {
    "demo": "https://demo-api.ig.com/gateway/deal",
    "live": "https://api.ig.com/gateway/deal",
}
_SESSION_ENDPOINT = "/session"
_SESSION_REFRESH_ENDPOINT = "/session/refresh-token"
_MARKETS_ENDPOINT = "/markets"
_PRICES_ENDPOINT = "/prices/{epic}/{resolution}/{num_points}"
_MARKET_DETAILS_ENDPOINT = "/markets/{epic}"
_ACCOUNTS_ENDPOINT = "/accounts"
_WATCHLISTS_ENDPOINT = "/watchlists"
_HISTORY_ACTIVITY_ENDPOINT = "/history/activity"
_CLIENT_SENTIMENT_ENDPOINT = "/clientsentiment/{market_id}"
_DETAIL_TTL_SEC = 5.0
_ACCOUNTS_TTL_SEC = 30.0
_WATCHLISTS_TTL_SEC = 60.0
_CLIENT_SENTIMENT_TTL_SEC = 300.0
_ACTIVITY_TTL_SEC = 30.0
_MIN_TOKEN_TTL_SEC = 5.0
_TOKEN_EXPIRY_SKEW_SEC = 15.0
_STREAMING_SESSION_TTL_SEC = 5 * 60.0

_SUPPORTED_ASSET_ALIASES = {
    "XAU/USD": "XAU/USD",
    "GC=F": "XAU/USD",
    "XAUUSD": "XAU/USD",
    "XAU": "XAU/USD",
    "GOLD": "XAU/USD",
    "XAG/USD": "XAG/USD",
    "SI=F": "XAG/USD",
    "XAGUSD": "XAG/USD",
    "XAG": "XAG/USD",
    "SILVER": "XAG/USD",
    "WTI": "WTI",
    "WTI/USD": "WTI",
    "CL=F": "WTI",
    "USOIL": "WTI",
    "CRUDE": "WTI",
    "OIL": "WTI",
    "EUR/USD": "EUR/USD",
    "EURUSD": "EUR/USD",
    "EUR": "EUR/USD",
    "EURO": "EUR/USD",
    "GBP/USD": "GBP/USD",
    "GBPUSD": "GBP/USD",
    "GBP": "GBP/USD",
    "POUND": "GBP/USD",
    "CABLE": "GBP/USD",
    "AUD/USD": "AUD/USD",
    "AUDUSD": "AUD/USD",
    "AUD": "AUD/USD",
    "AUSSIE": "AUD/USD",
    "USD/JPY": "USD/JPY",
    "USDJPY": "USD/JPY",
    "JPY": "USD/JPY",
    "YEN": "USD/JPY",
    "USD/CAD": "USD/CAD",
    "USDCAD": "USD/CAD",
    "CAD": "USD/CAD",
    "LOONIE": "USD/CAD",
    "EUR/JPY": "EUR/JPY",
    "EURJPY": "EUR/JPY",
    "GBP/JPY": "GBP/JPY",
    "GBPJPY": "GBP/JPY",
    "NZD/USD": "NZD/USD",
    "NZDUSD": "NZD/USD",
    "NZD": "NZD/USD",
    "KIWI": "NZD/USD",
    "EUR/GBP": "EUR/GBP",
    "EURGBP": "EUR/GBP",
    "USD/CHF": "USD/CHF",
    "USDCHF": "USD/CHF",
    "CHF": "USD/CHF",
    "SWISSY": "USD/CHF",
    "US30": "US30",
    "^DJI": "US30",
    "DOW": "US30",
    "DOWJONES": "US30",
    "US100": "US100",
    "^IXIC": "US100",
    "NASDAQ": "US100",
    "NDX": "US100",
    "US500": "US500",
    "^GSPC": "US500",
    "SP500": "US500",
    "SPX": "US500",
    "UK100": "UK100",
    "^FTSE": "UK100",
    "FTSE": "UK100",
    "FTSE100": "UK100",
    "GER40": "GER40",
    "DE40": "GER40",
    "DAX": "GER40",
    "DAX40": "GER40",
    "GERMANY40": "GER40",
    "AUS200": "AUS200",
    "AU200": "AUS200",
    "ASX200": "AUS200",
    "AUSTRALIA200": "AUS200",
    "JPN225": "JPN225",
    "JP225": "JPN225",
    "JAPAN225": "JPN225",
    "NIKKEI": "JPN225",
    "NIKKEI225": "JPN225",
}

_SEARCH_TERMS = {
    "XAU/USD": ("spot gold", "gold", "xau"),
    "XAG/USD": ("spot silver", "silver", "xag"),
    "WTI": ("wti", "us crude", "crude oil"),
    "EUR/USD": ("eur/usd", "eur usd", "euro us dollar"),
    "GBP/USD": ("gbp/usd", "gbp usd", "sterling us dollar"),
    "AUD/USD": ("aud/usd", "aud usd", "australian dollar us dollar"),
    "USD/JPY": ("usd/jpy", "usd jpy", "us dollar japanese yen"),
    "USD/CAD": ("usd/cad", "usd cad", "us dollar canadian dollar"),
    "EUR/JPY": ("eur/jpy", "eur jpy", "euro japanese yen"),
    "GBP/JPY": ("gbp/jpy", "gbp jpy", "sterling japanese yen"),
    "NZD/USD": ("nzd/usd", "nzd usd", "new zealand dollar us dollar"),
    "EUR/GBP": ("eur/gbp", "eur gbp", "euro sterling"),
    "USD/CHF": ("usd/chf", "usd chf", "us dollar swiss franc"),
    "US30": ("us30", "dow jones", "wall street", "dow"),
    "US100": ("us100", "nasdaq 100", "nasdaq", "ndx"),
    "US500": ("us500", "s&p 500", "spx", "s&p"),
    "UK100": ("uk100", "ftse 100", "ftse"),
    "GER40": ("ger40", "germany 40", "dax 40", "dax"),
    "AUS200": ("aus200", "australia 200", "asx 200", "spi 200"),
    "JPN225": ("jpn225", "japan 225", "nikkei 225", "nikkei"),
}

_ASSET_CATEGORIES = {
    "XAU/USD": "commodities",
    "XAG/USD": "commodities",
    "WTI": "commodities",
    "EUR/USD": "forex",
    "GBP/USD": "forex",
    "AUD/USD": "forex",
    "USD/JPY": "forex",
    "USD/CAD": "forex",
    "EUR/JPY": "forex",
    "GBP/JPY": "forex",
    "NZD/USD": "forex",
    "EUR/GBP": "forex",
    "USD/CHF": "forex",
    "US30": "indices",
    "US100": "indices",
    "US500": "indices",
    "UK100": "indices",
    "GER40": "indices",
    "AUS200": "indices",
    "JPN225": "indices",
}

_ASSET_INSTRUMENT_TYPE_TOKENS = {
    "XAU/USD": ("COMMODITIES",),
    "XAG/USD": ("COMMODITIES",),
    "WTI": ("COMMODITIES",),
    "EUR/USD": ("CURRENCIES", "FOREX"),
    "GBP/USD": ("CURRENCIES", "FOREX"),
    "AUD/USD": ("CURRENCIES", "FOREX"),
    "USD/JPY": ("CURRENCIES", "FOREX"),
    "USD/CAD": ("CURRENCIES", "FOREX"),
    "EUR/JPY": ("CURRENCIES", "FOREX"),
    "GBP/JPY": ("CURRENCIES", "FOREX"),
    "NZD/USD": ("CURRENCIES", "FOREX"),
    "EUR/GBP": ("CURRENCIES", "FOREX"),
    "USD/CHF": ("CURRENCIES", "FOREX"),
    "US30": ("INDICES", "INDICE"),
    "US100": ("INDICES", "INDICE"),
    "US500": ("INDICES", "INDICE"),
    "UK100": ("INDICES", "INDICE"),
    "GER40": ("INDICES", "INDICE"),
    "AUS200": ("INDICES", "INDICE"),
    "JPN225": ("INDICES", "INDICE"),
}

_ASSET_MATCH_GROUPS = {
    "XAU/USD": (("gold",), ("xau",)),
    "XAG/USD": (("silver",), ("xag",)),
    "WTI": (("wti",), ("us crude",), ("crude", "oil"), ("west texas",)),
    "EUR/USD": (("eur/usd",), ("eur", "usd"), ("euro", "us", "dollar")),
    "GBP/USD": (("gbp/usd",), ("gbp", "usd"), ("sterling", "us", "dollar")),
    "AUD/USD": (("aud/usd",), ("aud", "usd"), ("australian", "dollar", "us", "dollar")),
    "USD/JPY": (("usd/jpy",), ("usd", "jpy"), ("us", "dollar", "japanese", "yen")),
    "USD/CAD": (("usd/cad",), ("usd", "cad"), ("us", "dollar", "canadian", "dollar")),
    "EUR/JPY": (("eur/jpy",), ("eur", "jpy"), ("euro", "japanese", "yen")),
    "GBP/JPY": (("gbp/jpy",), ("gbp", "jpy"), ("sterling", "japanese", "yen")),
    "NZD/USD": (("nzd/usd",), ("nzd", "usd"), ("new zealand", "usd")),
    "EUR/GBP": (("eur/gbp",), ("eur", "gbp"), ("euro", "sterling")),
    "USD/CHF": (("usd/chf",), ("usd", "chf"), ("us dollar", "swiss franc")),
    "US30": (("us30",), ("dow",), ("dow", "jones"), ("wall", "street")),
    "US100": (("us100",), ("nasdaq",), ("nasdaq", "100"), ("ndx",)),
    "US500": (("us500",), ("s&p", "500"), ("spx",)),
    "UK100": (("uk100",), ("ftse",), ("ftse", "100")),
    "GER40": (("ger40",), ("germany", "40"), ("dax", "40"), ("dax",)),
    "AUS200": (("aus200",), ("australia", "200"), ("asx", "200"), ("spi", "200")),
    "JPN225": (("jpn225",), ("japan", "225"), ("nikkei", "225"), ("nikkei",)),
}

_ASSET_REJECT_TERMS = {
    "XAU/USD": ("silver", "wti", "crude"),
    "XAG/USD": ("gold", "wti", "crude"),
    "WTI": ("brent",),
    "EUR/USD": ("eur/gbp", "eur/jpy", "gbp/usd", "usd/chf", "usd/jpy", "usd/cad"),
    "GBP/USD": ("gbp/jpy", "eur/gbp", "eur/usd", "usd/cad", "usd/chf"),
    "AUD/USD": ("aud/jpy", "nzd/usd", "usd/cad", "eur/usd"),
    "USD/JPY": ("eur/jpy", "gbp/jpy", "usd/cad", "usd/chf"),
    "USD/CAD": ("usd/jpy", "usd/chf", "eur/usd", "aud/usd"),
    "EUR/JPY": ("usd/jpy", "eur/usd", "gbp/jpy", "eur/gbp"),
    "GBP/JPY": ("usd/jpy", "gbp/usd", "eur/jpy", "eur/gbp"),
    "EUR/GBP": ("eur/usd", "gbp/usd", "usd/chf", "nzd/usd", "eur/jpy", "gbp/jpy"),
    "USD/CHF": ("usd/cad", "usd/jpy", "eur/gbp", "nzd/usd", "eur/usd"),
    "US30": ("nasdaq", "s&p", "ftse", "uk 100", "dax", "germany", "nikkei", "japan", "australia", "asx"),
    "US100": ("dow", "dji", "s&p", "ftse", "uk 100", "dax", "germany", "nikkei", "japan", "australia", "asx"),
    "US500": ("dow", "dji", "nasdaq", "ndx", "ftse", "uk 100", "dax", "germany", "nikkei", "japan", "australia", "asx"),
    "UK100": ("dax", "germany", "japan", "nikkei", "australia", "asx", "nasdaq", "dow", "s&p"),
    "GER40": ("ftse", "uk 100", "japan", "nikkei", "australia", "asx", "nasdaq", "dow", "s&p"),
    "AUS200": ("dax", "germany", "japan", "nikkei", "ftse", "uk 100", "nasdaq", "dow", "s&p"),
    "JPN225": ("dax", "germany", "australia", "asx", "ftse", "uk 100", "nasdaq", "dow", "s&p"),
}

_RESOLUTION_MAP = {
    "1m": "MINUTE",
    "5m": "MINUTE_5",
    "15m": "MINUTE_15",
    "30m": "MINUTE_30",
    "1h": "HOUR",
    "4h": "HOUR_4",
    "1d": "DAY",
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


def _safe_int(value: Any) -> Optional[int]:
    try:
        if value in (None, ""):
            return None
        return int(float(value))
    except Exception:
        return None


def _canonical_asset(asset: str) -> str:
    return _SUPPORTED_ASSET_ALIASES.get(str(asset or "").strip().upper(), str(asset or "").strip())


def _asset_category(canonical_asset: str) -> str:
    return str(_ASSET_CATEGORIES.get(canonical_asset, "") or "")


def _default_instrument_type(canonical_asset: str) -> str:
    tokens = _ASSET_INSTRUMENT_TYPE_TOKENS.get(canonical_asset) or ("MARKETS",)
    return str(tokens[0] or "MARKETS")


def _matches_required_group(haystack: str, canonical_asset: str) -> bool:
    groups = _ASSET_MATCH_GROUPS.get(canonical_asset) or ()
    return any(all(token in haystack for token in group) for group in groups)


def _configured_routed_assets() -> list[str]:
    routed = set()
    routed_categories = {str(item or "").strip().lower() for item in (IG_ROUTED_CATEGORIES or []) if str(item or "").strip()}
    for asset, category in _ASSET_CATEGORIES.items():
        if str(category or "").strip().lower() in routed_categories:
            routed.add(asset)
    for asset in IG_ROUTED_ASSETS or []:
        canonical = _canonical_asset(str(asset or ""))
        if canonical in _ASSET_CATEGORIES:
            routed.add(canonical)
    return sorted(routed)


def _configured_routed_categories() -> list[str]:
    categories = {str(item or "").strip().lower() for item in (IG_ROUTED_CATEGORIES or []) if str(item or "").strip()}
    categories.update(_asset_category(asset) for asset in _configured_routed_assets() if _asset_category(asset))
    return sorted(categories)


def _mid_price(payload: Dict[str, Any]) -> Optional[float]:
    bid = _safe_float(payload.get("bid"))
    ask = _safe_float(payload.get("ask"))
    last = _safe_float(payload.get("lastTraded"))
    if bid is not None and ask is not None:
        return (bid + ask) / 2.0
    return last if last is not None else (bid if bid is not None else ask)


def _normalize_ig_commodity_price(asset: str, value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    try:
        numeric = float(value)
    except Exception:
        return None

    canonical = _canonical_asset(asset)
    if canonical in {"XAG/USD", "WTI"} and numeric >= 1000.0:
        return numeric / 100.0
    return numeric


def _parse_epic_map(raw: str) -> Dict[str, str]:
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
        epic = str(value or "").strip()
        if canonical and epic:
            parsed[canonical] = epic
    return parsed


class IGRequestError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        self.code = str(code or "ig_request_error")
        self.message = str(message or code or "IG request failed")
        super().__init__(self.message)


class IGMarketBridge:
    """
    IG market-data bridge used as the primary source for routed assets.

    Routed assets or routed categories use IG first, with other providers
    retained only as fallbacks when IG is unavailable or not yet fully
    configured. REST authentication uses IG's OAuth-style v3 session flow.
    """

    def __init__(self) -> None:
        self._enabled = bool(IG_ENABLED and str(IG_API_KEY or "").strip())
        self._api_key = str(IG_API_KEY or "").strip()
        self._identifier = str(IG_IDENTIFIER or "").strip()
        self._password = str(IG_PASSWORD or "").strip()
        self._account_id = str(IG_ACCOUNT_ID or "").strip()
        env_key = str(IG_ENVIRONMENT or "demo").strip().lower()
        self._environment = env_key if env_key in _BASE_URLS else "demo"
        self._base_url = _BASE_URLS[self._environment]
        self._session = requests.Session()
        self._session.headers.update(
            {
                "User-Agent": "Robbie-TradingBot/1.0",
                "Accept": "application/json; charset=UTF-8",
                "Content-Type": "application/json",
            }
        )
        self._epic_overrides = _parse_epic_map(IG_EPIC_MAP)
        self._resolved_symbols: Dict[str, Optional[Dict[str, Any]]] = {}
        self._detail_cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}
        self._accounts_cache: Tuple[float, list[Dict[str, Any]]] = (0.0, [])
        self._watchlists_cache: Tuple[float, list[Dict[str, Any]]] = (0.0, [])
        self._activity_cache: Dict[Tuple[int, int], Tuple[float, list[Dict[str, Any]]]] = {}
        self._client_sentiment_cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}
        self._lock = threading.RLock()
        self._access_token = ""
        self._refresh_token = ""
        self._token_type = "Bearer"
        self._oauth_scope = ""
        self._cst_token = ""
        self._security_token = ""
        self._lightstreamer_endpoint = ""
        self._session_expires_at = 0.0
        self._streaming_session_expires_at = 0.0
        self._missing_credentials_logged = False

    def list_profiles(self) -> list[str]:
        return ["ig"] if self._enabled and self._credentials_ready(log_warning=False) else []

    def resolve_symbol_info(self, asset: str, category: str = "") -> Optional[Dict[str, Any]]:
        canonical = _canonical_asset(asset)
        if not self._supports_asset(canonical, category=category):
            return None

        cache_key = f"{str(category or '').lower()}:{canonical}"
        with self._lock:
            if cache_key in self._resolved_symbols:
                cached = self._resolved_symbols[cache_key]
                return dict(cached) if cached else None

            override = self._epic_overrides.get(canonical)
            if override:
                override_payload: Dict[str, Any] = {
                    "epic": override,
                    "instrumentName": canonical,
                    "instrumentType": _default_instrument_type(canonical),
                    "marketStatus": "",
                    "delayTime": None,
                    "streamingPricesAvailable": False,
                }
                if self._credentials_ready(log_warning=False):
                    try:
                        details = self._get_market_details(override)
                        instrument = dict(details.get("instrument") or {})
                        snapshot = dict(details.get("snapshot") or {})
                        override_payload.update(
                            {
                                "instrumentName": str(
                                    instrument.get("name")
                                    or instrument.get("instrumentName")
                                    or canonical
                                ),
                                "instrumentType": str(
                                    instrument.get("type")
                                    or instrument.get("instrumentType")
                                    or _default_instrument_type(canonical)
                                ),
                                "marketStatus": str(
                                    snapshot.get("marketStatus")
                                    or instrument.get("marketStatus")
                                    or ""
                                ),
                                "delayTime": snapshot.get("delayTime"),
                                "streamingPricesAvailable": bool(
                                    instrument.get("streamingPricesAvailable")
                                    or snapshot.get("streamingPricesAvailable")
                                ),
                            }
                        )
                    except Exception as exc:
                        logger.debug(f"[IGBridge] override detail lookup failed for {canonical}: {exc}")
                resolved = self._build_resolved(
                    canonical,
                    override_payload,
                )
                self._resolved_symbols[cache_key] = resolved
                return dict(resolved)

            if not self._credentials_ready():
                self._resolved_symbols[cache_key] = None
                return None

            candidates: Dict[str, Tuple[int, Dict[str, Any]]] = {}
            for term in _SEARCH_TERMS.get(canonical, (canonical,)):
                for item in self._search_markets(term):
                    score = self._candidate_score(canonical, item)
                    if score <= 0:
                        continue
                    epic = str(item.get("epic", "")).strip()
                    if not epic:
                        continue
                    prev = candidates.get(epic)
                    if prev is None or score > prev[0]:
                        candidates[epic] = (score, item)

            if not candidates:
                self._resolved_symbols[cache_key] = None
                return None

            best_item = max(candidates.values(), key=lambda pair: pair[0])[1]
            resolved = self._build_resolved(canonical, best_item)
            self._resolved_symbols[cache_key] = resolved
            return dict(resolved)

    def supports(self, asset: str, category: str = "") -> bool:
        return self._supports_asset(_canonical_asset(asset), category=category)

    def get_quote(
        self,
        asset: str,
        category: str = "",
    ) -> Tuple[Optional[float], Optional[float], Dict[str, Any]]:
        canonical = _canonical_asset(asset)
        if not self._supports_asset(canonical, category=category):
            return None, None, {}
        if not self._credentials_ready():
            return None, None, self._error_metadata(
                canonical,
                realtime=True,
                message="IG_IDENTIFIER and IG_PASSWORD are required for IG routed market data.",
                code="missing_credentials",
            )

        resolved = self.resolve_symbol_info(canonical, category=category)
        if not resolved:
            return None, None, self._error_metadata(
                canonical,
                realtime=True,
                message=f"IG could not resolve an epic for {canonical}.",
                code="epic_not_found",
            )

        epic = str(resolved.get("symbol", "") or "")
        try:
            details = self._get_market_details(epic)
            snapshot = details.get("snapshot") or {}
            bid = _safe_float(snapshot.get("bid"))
            offer = _safe_float(snapshot.get("offer"))
            price = None
            if bid is not None and offer is not None:
                price = (bid + offer) / 2.0
            else:
                price = _mid_price(snapshot)
            bid = _normalize_ig_commodity_price(canonical, bid)
            offer = _normalize_ig_commodity_price(canonical, offer)
            price = _normalize_ig_commodity_price(canonical, price)
            if price is None:
                return None, None, self._error_metadata(
                    canonical,
                    realtime=True,
                    message=f"IG market snapshot for {canonical} did not include a usable price.",
                    code="missing_price",
                    epic=epic,
                )

            spread = max(0.0, float(offer or price) - float(bid or price))
            market_status = str(snapshot.get("marketStatus") or resolved.get("market_status") or "")
            delay_time = _safe_int(snapshot.get("delayTime"))
            delayed = bool(delay_time and delay_time > 0)
            try:
                from services.live_microstructure_service import get_service as get_live_microstructure_service

                get_live_microstructure_service().record_quote(
                    "ig",
                    canonical,
                    bid=bid,
                    ask=offer,
                    price=price,
                    timestamp=time.time(),
                    flags=market_status,
                )
            except Exception:
                pass
            return float(price), float(spread), self._metadata(
                epic,
                instrument_name=str((details.get("instrument") or {}).get("name") or resolved.get("instrument_name") or canonical),
                market_status=market_status,
                delayed=delayed,
                realtime=not delayed,
            )
        except IGRequestError as exc:
            return None, None, self._error_metadata(
                canonical,
                realtime=True,
                message=exc.message,
                code=exc.code,
                epic=epic,
            )
        except Exception as exc:
            logger.debug(f"[IGBridge] quote {canonical}: {exc}")
            return None, None, self._error_metadata(
                canonical,
                realtime=True,
                message=str(exc),
                code="quote_failed",
                epic=epic,
            )

    def get_ohlcv(
        self,
        asset: str,
        interval: str,
        periods: int,
        category: str = "",
        end_time: Any = None,
        closed_only: bool = False,
    ) -> Tuple[Optional[pd.DataFrame], Dict[str, Any]]:
        canonical = _canonical_asset(asset)
        resolution = _RESOLUTION_MAP.get(str(interval or "").lower())
        if not self._supports_asset(canonical, category=category) or not resolution:
            return None, {}
        if not self._credentials_ready():
            return None, self._error_metadata(
                canonical,
                realtime=False,
                message="IG_IDENTIFIER and IG_PASSWORD are required for IG routed market data.",
                code="missing_credentials",
            )

        resolved = self.resolve_symbol_info(canonical, category=category)
        if not resolved:
            return None, self._error_metadata(
                canonical,
                realtime=False,
                message=f"IG could not resolve an epic for {canonical}.",
                code="epic_not_found",
            )

        epic = str(resolved.get("symbol", "") or "")
        cutoff = pd.to_datetime(end_time, utc=True, errors="coerce") if end_time not in (None, "") else None
        try:
            payload = self._request(
                "GET",
                _PRICES_ENDPOINT.format(epic=_urlquote(epic, safe=""), resolution=resolution, num_points=int(max(2, periods))),
                version="2",
            )
            prices = payload.get("prices") or []
            if not isinstance(prices, list) or not prices:
                return None, self._error_metadata(
                    canonical,
                    realtime=False,
                    message=f"IG price history for {canonical} did not include candle data.",
                    code="missing_history",
                    epic=epic,
                )

            rows = self._normalize_ohlcv_rows(canonical, prices)
            if not rows:
                return None, self._error_metadata(
                    canonical,
                    realtime=False,
                    message=f"IG price history for {canonical} could not be normalized into OHLCV bars.",
                    code="normalization_failed",
                    epic=epic,
                )

            frame = pd.DataFrame(rows).set_index("timestamp").sort_index()
            frame = self._apply_ohlcv_cutoff(frame, cutoff, closed_only)
            frame = frame.tail(int(max(2, periods)))
            if frame.empty:
                return None, self._error_metadata(
                    canonical,
                    realtime=False,
                    message=f"IG returned price history for {canonical}, but no rows survived the requested cutoff.",
                    code="empty_after_cutoff",
                    epic=epic,
                )

            delayed = bool(_safe_int(resolved.get("delay_time")) or 0)
            return frame[["open", "high", "low", "close", "volume"]], self._metadata(
                epic,
                instrument_name=str(resolved.get("instrument_name") or canonical),
                market_status=str(resolved.get("market_status") or ""),
                delayed=delayed,
                realtime=False,
            )
        except IGRequestError as exc:
            return None, self._error_metadata(
                canonical,
                realtime=False,
                message=exc.message,
                code=exc.code,
                epic=epic,
            )
        except Exception as exc:
            logger.debug(f"[IGBridge] ohlcv {canonical}: {exc}")
            return None, self._error_metadata(
                canonical,
                realtime=False,
                message=str(exc),
                code="ohlcv_failed",
                epic=epic,
            )

    @staticmethod
    def _normalize_ohlcv_rows(canonical: str, prices: list) -> list[dict[str, Any]]:
        rows = []
        for item in prices:
            if not isinstance(item, dict):
                continue
            ts = pd.to_datetime(item.get("snapshotTimeUTC") or item.get("snapshotTime"), utc=True, errors="coerce")
            if pd.isna(ts):
                continue
            open_price = _mid_price(item.get("openPrice") or {})
            high_price = _mid_price(item.get("highPrice") or {})
            low_price = _mid_price(item.get("lowPrice") or {})
            close_price = _mid_price(item.get("closePrice") or {})
            open_price = _normalize_ig_commodity_price(canonical, open_price)
            high_price = _normalize_ig_commodity_price(canonical, high_price)
            low_price = _normalize_ig_commodity_price(canonical, low_price)
            close_price = _normalize_ig_commodity_price(canonical, close_price)
            if None in (open_price, high_price, low_price, close_price):
                continue
            rows.append(
                {
                    "timestamp": pd.Timestamp(ts),
                    "open": float(open_price),
                    "high": float(high_price),
                    "low": float(low_price),
                    "close": float(close_price),
                    "volume": float(_safe_float(item.get("lastTradedVolume")) or 0.0),
                }
            )
        return rows

    @staticmethod
    def _apply_ohlcv_cutoff(frame: pd.DataFrame, cutoff: Any, closed_only: bool) -> pd.DataFrame:
        if cutoff is None or pd.isna(cutoff):
            return frame
        cutoff_ts = pd.Timestamp(cutoff)
        if cutoff_ts.tzinfo is None:
            cutoff_ts = cutoff_ts.tz_localize("UTC")
        else:
            cutoff_ts = cutoff_ts.tz_convert("UTC")
        if closed_only:
            return frame[frame.index < cutoff_ts]
        return frame[frame.index <= cutoff_ts]

    @staticmethod
    def _current_session_close_utc(details: Dict[str, Any], *, now_utc: Optional[datetime] = None) -> Optional[datetime]:
        now = now_utc.astimezone(timezone.utc) if isinstance(now_utc, datetime) else datetime.now(timezone.utc)
        opening_hours = details.get("openingHours") or (details.get("instrument") or {}).get("openingHours") or {}
        market_times = opening_hours.get("marketTimes") or []
        if not isinstance(market_times, list):
            return None

        for day_offset in (-1, 0, 1):
            trading_day = (now + timedelta(days=day_offset)).date()
            for session in market_times:
                if not isinstance(session, dict):
                    continue
                raw_open = str(session.get("openTime") or "").strip()
                raw_close = str(session.get("closeTime") or "").strip()
                if not raw_open or not raw_close:
                    continue
                open_time = None
                close_time = None
                for fmt in ("%H:%M:%S", "%H:%M"):
                    try:
                        if open_time is None:
                            open_time = datetime.strptime(raw_open, fmt).time()
                        if close_time is None:
                            close_time = datetime.strptime(raw_close, fmt).time()
                    except Exception:
                        continue
                if open_time is None or close_time is None:
                    continue
                open_dt = datetime(
                    trading_day.year,
                    trading_day.month,
                    trading_day.day,
                    open_time.hour,
                    open_time.minute,
                    open_time.second,
                    tzinfo=timezone.utc,
                )
                close_dt = datetime(
                    trading_day.year,
                    trading_day.month,
                    trading_day.day,
                    close_time.hour,
                    close_time.minute,
                    close_time.second,
                    tzinfo=timezone.utc,
                )
                if close_dt <= open_dt:
                    close_dt += timedelta(days=1)
                if open_dt <= now < close_dt:
                    return close_dt
        return None

    def get_market_status(self, asset: str, category: str = "") -> Optional[Dict[str, Any]]:
        canonical = _canonical_asset(asset)
        if not self._supports_asset(canonical, category=category) or not self._credentials_ready():
            return None

        resolved = self.resolve_symbol_info(canonical, category=category)
        if not resolved:
            return None

        epic = str(resolved.get("symbol", "") or "")
        try:
            details = self._get_market_details(epic)
            snapshot = details.get("snapshot") or {}
            market_status = str(snapshot.get("marketStatus") or resolved.get("market_status") or "").upper()
            market_open = market_status in {"TRADEABLE", "DEAL_NO_EDIT"}
            instrument_name = str((details.get("instrument") or {}).get("name") or resolved.get("instrument_name") or canonical)
            provider_status = {
                "asset": canonical,
                "market_open": market_open,
                "reason": f"{instrument_name} {market_status.lower() or 'status unavailable'} on IG",
                "source": "IG",
                "ig_epic": epic,
                "utc_now": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
            }
            if market_open:
                session_close_utc = self._current_session_close_utc(details)
                if session_close_utc is not None:
                    provider_status["session_close_utc"] = session_close_utc.isoformat()
            return build_market_status(
                canonical,
                category=category,
                provider_status=provider_status,
            )
        except Exception:
            return None

    def get_microstructure(self, asset: str, category: str = "") -> Dict[str, Any]:
        price, spread, meta = self.get_quote(asset, category=category)
        if price is None:
            return {}
        try:
            from services.live_microstructure_service import get_service as get_live_microstructure_service

            snapshot = get_live_microstructure_service().get_snapshot(
                "ig",
                _canonical_asset(asset),
                price=price,
                spread=spread,
                meta=meta,
            )
            if snapshot:
                return {**meta, **snapshot}
        except Exception:
            pass
        spread_bps = round((float(spread or 0.0) / float(price)) * 10000, 3) if price else 0.0
        return {
            **meta,
            "spread_bps": spread_bps,
            "tick_imbalance": 0.0,
            "book_imbalance": 0.0,
            "stop_hunt_risk": 0.0,
            "score": 0.0,
        }

    def get_account_summary(self) -> Dict[str, Any]:
        if not self._enabled:
            return {
                "enabled": False,
                "authenticated": False,
                "provider": "IG",
                "environment": self._environment,
            }
        if not self._credentials_ready(log_warning=False):
            return {
                "enabled": True,
                "authenticated": False,
                "provider": "IG",
                "environment": self._environment,
                "error_code": "missing_credentials",
                "error_message": "IG_IDENTIFIER and IG_PASSWORD are required for IG routed market data.",
            }
        try:
            accounts = self._get_accounts()
            active = None
            account_id = str(self._account_id or "")
            for item in accounts:
                if str(item.get("accountId") or "") == account_id:
                    active = item
                    break
            if active is None and accounts:
                active = next((item for item in accounts if bool(item.get("preferred"))), accounts[0])

            balance = dict((active or {}).get("balance") or {})
            watchlists = self._get_watchlists()
            activities = self.get_recent_activity(days=14, limit=10)
            return {
                "enabled": True,
                "authenticated": True,
                "provider": "IG",
                "environment": self._environment,
                "account_id": str((active or {}).get("accountId") or self._account_id or ""),
                "account_name": str((active or {}).get("accountName") or ""),
                "account_type": str((active or {}).get("accountType") or ""),
                "status": str((active or {}).get("status") or ""),
                "currency": str((active or {}).get("currency") or ""),
                "preferred": bool((active or {}).get("preferred")),
                "balance": _safe_float(balance.get("balance")),
                "available": _safe_float(balance.get("available")),
                "profit_loss": _safe_float(balance.get("profitLoss")),
                "watchlist_count": len(watchlists),
                "watchlists": watchlists[:5],
                "recent_activity_count": len(activities),
                "recent_activities": activities[:5],
                "routed_categories": _configured_routed_categories(),
                "routed_assets": _configured_routed_assets(),
            }
        except IGRequestError as exc:
            return {
                "enabled": True,
                "authenticated": False,
                "provider": "IG",
                "environment": self._environment,
                "error_code": exc.code,
                "error_message": exc.message,
            }
        except Exception as exc:
            logger.debug(f"[IGBridge] account summary failed: {exc}")
            return {
                "enabled": True,
                "authenticated": False,
                "provider": "IG",
                "environment": self._environment,
                "error_code": "summary_failed",
                "error_message": str(exc),
            }

    def get_watchlists(self) -> list[Dict[str, Any]]:
        if not self._enabled or not self._credentials_ready(log_warning=False):
            return []
        try:
            return self._get_watchlists()
        except Exception:
            return []

    def get_recent_activity(self, *, days: int = 7, limit: int = 10) -> list[Dict[str, Any]]:
        if not self._enabled or not self._credentials_ready(log_warning=False):
            return []

        safe_days = max(1, int(days or 7))
        safe_limit = max(1, min(50, int(limit or 10)))
        cache_key = (safe_days, safe_limit)
        with self._lock:
            now = time.monotonic()
            cached = self._activity_cache.get(cache_key)
            if cached and (now - cached[0]) < _ACTIVITY_TTL_SEC:
                return [dict(item) for item in cached[1]]

        end_utc = datetime.now(timezone.utc)
        start_utc = end_utc - pd.Timedelta(days=safe_days)
        payload = self._request(
            "GET",
            _HISTORY_ACTIVITY_ENDPOINT,
            params={
                "from": start_utc.strftime("%Y-%m-%dT%H:%M:%S"),
                "to": end_utc.strftime("%Y-%m-%dT%H:%M:%S"),
                "detailed": "true",
                "pageSize": str(safe_limit),
            },
            version="3",
        )
        activities = payload.get("activities") or []
        simplified: list[Dict[str, Any]] = []
        for item in activities:
            if not isinstance(item, dict):
                continue
            details = list(item.get("details") or [])
            market_name = ""
            market_epic = ""
            for detail in details:
                if not isinstance(detail, dict):
                    continue
                if not market_name:
                    market_name = str(detail.get("marketName") or "")
                if not market_epic:
                    market_epic = str(detail.get("epic") or "")
            simplified.append(
                {
                    "date": str(item.get("date") or ""),
                    "action": str(item.get("actionType") or item.get("type") or ""),
                    "channel": str(item.get("channel") or ""),
                    "description": str(item.get("description") or ""),
                    "deal_id": str(item.get("dealId") or ""),
                    "market_name": market_name,
                    "market_epic": market_epic,
                    "status": str(item.get("status") or ""),
                }
            )

        with self._lock:
            self._activity_cache[cache_key] = (time.monotonic(), [dict(item) for item in simplified])
        return simplified

    def get_client_sentiment(self, asset: str, category: str = "") -> Optional[Dict[str, Any]]:
        canonical = _canonical_asset(asset)
        if not self._supports_asset(canonical, category=category):
            return None
        if not self._credentials_ready(log_warning=False):
            return None

        resolved = self.resolve_symbol_info(canonical, category=category)
        if not resolved:
            return None

        epic = str(resolved.get("symbol") or "")
        market_id = str(resolved.get("market_id") or "").strip()
        if not market_id:
            details = self._get_market_details(epic)
            instrument = dict(details.get("instrument") or {})
            market_id = str(instrument.get("marketId") or "").strip()
        if not market_id:
            return None

        with self._lock:
            now = time.monotonic()
            cached = self._client_sentiment_cache.get(market_id)
            if cached and (now - cached[0]) < _CLIENT_SENTIMENT_TTL_SEC:
                return dict(cached[1])

        payload = self._request(
            "GET",
            _CLIENT_SENTIMENT_ENDPOINT.format(market_id=_urlquote(market_id, safe="")),
            version="1",
        )
        long_pct = _safe_float(payload.get("longPositionPercentage"))
        short_pct = _safe_float(payload.get("shortPositionPercentage"))
        if long_pct is None or short_pct is None:
            return None
        score = max(-1.0, min(1.0, (float(long_pct) - float(short_pct)) / 100.0))
        bias = "BUY" if long_pct >= short_pct else "SELL"
        result = {
            "asset": canonical,
            "epic": epic,
            "market_id": market_id,
            "long_pct": round(float(long_pct), 1),
            "short_pct": round(float(short_pct), 1),
            "bias": bias,
            "score": round(score, 3),
        }
        with self._lock:
            self._client_sentiment_cache[market_id] = (time.monotonic(), dict(result))
        return result

    def supports_streaming(self, asset: str, category: str = "") -> bool:
        canonical = _canonical_asset(asset)
        if not self._supports_asset(canonical, category=category):
            return False
        try:
            resolved = self.resolve_symbol_info(canonical, category=category)
            return bool(resolved and resolved.get("streaming_prices_available") and resolved.get("symbol"))
        except Exception:
            return False

    def get_streaming_session(self) -> Dict[str, Any]:
        if not self._enabled or not self._credentials_ready():
            raise IGRequestError(
                "missing_credentials",
                "IG_IDENTIFIER and IG_PASSWORD are required for IG routed market-data streaming.",
            )

        with self._lock:
            now = time.monotonic()
            if (
                self._cst_token
                and self._security_token
                and self._lightstreamer_endpoint
                and now < self._streaming_session_expires_at
            ):
                return {
                    "lightstreamer_endpoint": self._lightstreamer_endpoint,
                    "account_id": self._account_id,
                    "cst": self._cst_token,
                    "x_security_token": self._security_token,
                    "password": f"CST-{self._cst_token}|XST-{self._security_token}",
                }

        self._ensure_session()
        headers = {
            "X-IG-API-KEY": self._api_key,
            "Version": "1",
            "Authorization": f"{self._token_type or 'Bearer'} {self._access_token}",
        }
        if self._account_id:
            headers["IG-ACCOUNT-ID"] = self._account_id

        response = self._session.get(
            f"{self._base_url}{_SESSION_ENDPOINT}",
            params={"fetchSessionTokens": "true"},
            headers=headers,
            timeout=15,
        )
        body = self._parse_json(response)
        if not response.ok:
            raise IGRequestError(
                self._extract_error_code(response, body),
                self._extract_error_message(response, body),
            )

        cst = str(response.headers.get("CST") or "").strip()
        x_security_token = str(response.headers.get("X-SECURITY-TOKEN") or "").strip()
        endpoint = str(body.get("lightstreamerEndpoint") or self._lightstreamer_endpoint or "").strip()
        account_id = str(body.get("accountId") or body.get("currentAccountId") or self._account_id or "").strip()

        if not cst or not x_security_token:
            raise IGRequestError(
                "missing_streaming_tokens",
                "IG session did not return Lightstreamer CST/X-SECURITY-TOKEN headers.",
            )
        if not endpoint:
            raise IGRequestError(
                "missing_lightstreamer_endpoint",
                "IG session did not return a Lightstreamer endpoint.",
            )
        if not account_id:
            raise IGRequestError(
                "missing_account_id",
                "IG session did not return an account identifier for streaming.",
            )

        with self._lock:
            self._cst_token = cst
            self._security_token = x_security_token
            self._lightstreamer_endpoint = endpoint
            self._account_id = account_id
            ttl_cap = time.monotonic() + _STREAMING_SESSION_TTL_SEC
            if self._session_expires_at:
                self._streaming_session_expires_at = min(self._session_expires_at, ttl_cap)
            else:
                self._streaming_session_expires_at = ttl_cap

            return {
                "lightstreamer_endpoint": self._lightstreamer_endpoint,
                "account_id": self._account_id,
                "cst": self._cst_token,
                "x_security_token": self._security_token,
                "password": f"CST-{self._cst_token}|XST-{self._security_token}",
            }

    def _supports_asset(self, canonical_asset: str, category: str = "") -> bool:
        if not self._enabled:
            return False
        expected_category = _asset_category(canonical_asset)
        if not expected_category:
            return False
        normalized_category = str(category or "").strip().lower()
        if normalized_category and normalized_category != expected_category:
            return False
        return canonical_asset in _ASSET_CATEGORIES

    def _credentials_ready(self, *, log_warning: bool = True) -> bool:
        ready = bool(self._identifier and self._password)
        if log_warning and not ready and not self._missing_credentials_logged:
            logger.warning(
                "[IGBridge] IG market-data routing is enabled, but IG_IDENTIFIER / IG_PASSWORD are not set. "
                "IG will stay unavailable until both are configured."
            )
            self._missing_credentials_logged = True
        return ready

    def _search_markets(self, term: str) -> list[Dict[str, Any]]:
        payload = self._request("GET", _MARKETS_ENDPOINT, params={"searchTerm": term}, version="1")
        markets = payload.get("markets") or []
        return [item for item in markets if isinstance(item, dict)]

    def _candidate_score(self, canonical_asset: str, item: Dict[str, Any]) -> int:
        instrument_type = str(item.get("instrumentType") or "").upper()
        expected_tokens = _ASSET_INSTRUMENT_TYPE_TOKENS.get(canonical_asset) or ()
        if expected_tokens and not any(token in instrument_type for token in expected_tokens):
            return 0

        haystack = " ".join(
            [
                str(item.get("instrumentName") or ""),
                str(item.get("epic") or ""),
                str(item.get("expiry") or ""),
            ]
        ).lower()

        for reject_term in _ASSET_REJECT_TERMS.get(canonical_asset) or ():
            if reject_term in haystack:
                return 0
        if not _matches_required_group(haystack, canonical_asset):
            return 0

        score = 6
        for term in _SEARCH_TERMS.get(canonical_asset, ()):
            lowered = str(term or "").strip().lower()
            if lowered and lowered in haystack:
                score += 4 if "/" in lowered else 2

        market_status = str(item.get("marketStatus") or "").upper()
        if market_status == "TRADEABLE":
            score += 3
        elif market_status in {"DEAL_NO_EDIT", "EDITS_ONLY"}:
            score += 1
        if bool(item.get("streamingPricesAvailable")):
            score += 1
        delay = _safe_int(item.get("delayTime"))
        if delay == 0:
            score += 1
        return score

    @staticmethod
    def _build_resolved(canonical_asset: str, item: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "symbol": str(item.get("epic") or "").strip(),
            "display_name": canonical_asset,
            "instrument_name": str(item.get("instrumentName") or canonical_asset),
            "market_id": str(item.get("marketId") or "").strip(),
            "instrument_type": str(item.get("instrumentType") or _default_instrument_type(canonical_asset)),
            "market": _asset_category(canonical_asset) or "unknown",
            "exchange": "ig",
            "market_status": str(item.get("marketStatus") or ""),
            "delay_time": _safe_int(item.get("delayTime")),
            "streaming_prices_available": bool(item.get("streamingPricesAvailable")),
        }

    def _get_market_details(self, epic: str) -> Dict[str, Any]:
        with self._lock:
            cached = self._detail_cache.get(epic)
            now = time.monotonic()
            if cached and (now - cached[0]) < _DETAIL_TTL_SEC:
                return dict(cached[1])

            payload = self._request(
                "GET",
                _MARKET_DETAILS_ENDPOINT.format(epic=_urlquote(epic, safe="")),
                version="4",
            )
            self._detail_cache[epic] = (now, dict(payload))
            return dict(payload)

    def _get_accounts(self) -> list[Dict[str, Any]]:
        with self._lock:
            cached_at, cached_payload = self._accounts_cache
            now = time.monotonic()
            if cached_payload and (now - cached_at) < _ACCOUNTS_TTL_SEC:
                return [dict(item) for item in cached_payload]

        payload = self._request("GET", _ACCOUNTS_ENDPOINT, version="1")
        accounts = [dict(item) for item in (payload.get("accounts") or []) if isinstance(item, dict)]
        with self._lock:
            self._accounts_cache = (time.monotonic(), [dict(item) for item in accounts])
        return accounts

    def _get_watchlists(self) -> list[Dict[str, Any]]:
        with self._lock:
            cached_at, cached_payload = self._watchlists_cache
            now = time.monotonic()
            if cached_payload and (now - cached_at) < _WATCHLISTS_TTL_SEC:
                return [dict(item) for item in cached_payload]

        payload = self._request("GET", _WATCHLISTS_ENDPOINT, version="1")
        watchlists: list[Dict[str, Any]] = []
        for item in payload.get("watchlists") or []:
            if not isinstance(item, dict):
                continue
            watchlists.append(
                {
                    "id": str(item.get("id") or ""),
                    "name": str(item.get("name") or ""),
                    "editable": bool(item.get("editable")),
                    "deleteable": bool(item.get("deleteable")),
                    "default_system_watchlist": bool(item.get("defaultSystemWatchlist")),
                }
            )
        with self._lock:
            self._watchlists_cache = (time.monotonic(), [dict(item) for item in watchlists])
        return watchlists

    def _ensure_session(self) -> None:
        if not self._credentials_ready():
            raise IGRequestError("missing_credentials", "IG_IDENTIFIER and IG_PASSWORD are required for IG routed market data.")

        with self._lock:
            now = time.monotonic()
            if self._access_token and now < self._session_expires_at:
                return

            if self._refresh_token:
                try:
                    self._refresh_oauth_locked()
                    if self._access_token and time.monotonic() < self._session_expires_at:
                        return
                except IGRequestError as exc:
                    logger.debug(f"[IGBridge] OAuth refresh failed; falling back to login: {exc}")
                    self._clear_oauth_tokens_locked(clear_refresh=True)

            self._login_oauth_locked()

    def _login_oauth_locked(self) -> None:
        payload = {"identifier": self._identifier, "password": self._password}
        headers = {
            "X-IG-API-KEY": self._api_key,
            "Version": "3",
        }
        response = self._session.post(
            f"{self._base_url}{_SESSION_ENDPOINT}",
            json=payload,
            headers=headers,
            timeout=15,
        )
        body = self._parse_json(response)
        if not response.ok:
            raise IGRequestError(
                self._extract_error_code(response, body),
                self._extract_error_message(response, body),
            )

        self._apply_oauth_payload_locked(body)

    def _refresh_oauth_locked(self) -> None:
        if not self._refresh_token:
            raise IGRequestError("missing_refresh_token", "IG OAuth refresh requested without a refresh token.")

        headers = {
            "X-IG-API-KEY": self._api_key,
            "Version": "1",
        }
        response = self._session.post(
            f"{self._base_url}{_SESSION_REFRESH_ENDPOINT}",
            json={"refresh_token": self._refresh_token},
            headers=headers,
            timeout=15,
        )
        body = self._parse_json(response)
        if not response.ok:
            raise IGRequestError(
                self._extract_error_code(response, body),
                self._extract_error_message(response, body),
            )

        self._apply_oauth_payload_locked(body, keep_account=True)

    def _apply_oauth_payload_locked(self, payload: Dict[str, Any], *, keep_account: bool = False) -> None:
        oauth = payload.get("oauthToken") if isinstance(payload.get("oauthToken"), dict) else payload
        access_token = str((oauth or {}).get("access_token") or "").strip()
        refresh_token = str((oauth or {}).get("refresh_token") or "").strip()
        token_type = str((oauth or {}).get("token_type") or "Bearer").strip() or "Bearer"
        scope = str((oauth or {}).get("scope") or "").strip()
        expires_in = _safe_float((oauth or {}).get("expires_in"))

        if not access_token:
            raise IGRequestError("missing_access_token", "IG OAuth response did not include an access token.")

        self._access_token = access_token
        if refresh_token:
            self._refresh_token = refresh_token
        self._token_type = token_type
        self._oauth_scope = scope
        self._session_expires_at = time.monotonic() + self._oauth_ttl(expires_in)
        self._lightstreamer_endpoint = str(payload.get("lightstreamerEndpoint") or self._lightstreamer_endpoint or "").strip()

        if not keep_account:
            current_account = str(payload.get("accountId") or payload.get("currentAccountId") or "").strip()
            if current_account and not self._account_id:
                self._account_id = current_account

    @staticmethod
    def _oauth_ttl(expires_in: Optional[float]) -> float:
        try:
            numeric = float(expires_in or 0.0)
        except Exception:
            numeric = 0.0
        if numeric <= 0:
            return _MIN_TOKEN_TTL_SEC
        adjusted = numeric - _TOKEN_EXPIRY_SKEW_SEC
        return max(_MIN_TOKEN_TTL_SEC, adjusted)

    def _clear_oauth_tokens_locked(self, *, clear_refresh: bool) -> None:
        self._access_token = ""
        self._cst_token = ""
        self._security_token = ""
        self._streaming_session_expires_at = 0.0
        self._session_expires_at = 0.0
        if clear_refresh:
            self._refresh_token = ""

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        version: str = "1",
        allow_retry: bool = True,
    ) -> Dict[str, Any]:
        self._ensure_session()
        headers = {
            "X-IG-API-KEY": self._api_key,
            "Version": str(version),
            "Authorization": f"{self._token_type or 'Bearer'} {self._access_token}",
        }
        if self._account_id:
            headers["IG-ACCOUNT-ID"] = self._account_id

        response = self._session.request(
            method=method.upper(),
            url=f"{self._base_url}{path}",
            params=params,
            headers=headers,
            timeout=15,
        )
        body = self._parse_json(response)
        if response.status_code == 401 and allow_retry:
            with self._lock:
                self._clear_oauth_tokens_locked(clear_refresh=False)
            self._ensure_session()
            return self._request(method, path, params=params, version=version, allow_retry=False)
        if not response.ok:
            raise IGRequestError(
                self._extract_error_code(response, body),
                self._extract_error_message(response, body),
            )
        return body

    @staticmethod
    def _parse_json(response: requests.Response) -> Dict[str, Any]:
        try:
            payload = response.json()
            return payload if isinstance(payload, dict) else {}
        except Exception:
            return {}

    @staticmethod
    def _extract_error_code(response: requests.Response, payload: Dict[str, Any]) -> str:
        return str(
            payload.get("errorCode")
            or payload.get("code")
            or payload.get("error")
            or response.status_code
        )

    @staticmethod
    def _extract_error_message(response: requests.Response, payload: Dict[str, Any]) -> str:
        if payload.get("errorCode"):
            return str(payload.get("errorCode"))
        if payload.get("message"):
            return str(payload.get("message"))
        text = (response.text or "").strip()
        return text or f"HTTP {response.status_code}"

    @staticmethod
    def _metadata(
        epic: str,
        *,
        instrument_name: str,
        market_status: str,
        delayed: bool,
        realtime: bool,
    ) -> Dict[str, Any]:
        return {
            "source": "IG",
            "source_class": "primary_api",
            "delayed": bool(delayed),
            "realtime": bool(realtime),
            "from_cache": False,
            "exchange": "ig",
            "ig_epic": epic,
            "ig_instrument_name": instrument_name,
            "ig_market_status": market_status,
            "as_of_utc": _utc_now_iso(),
        }

    @classmethod
    def _error_metadata(
        cls,
        asset: str,
        *,
        realtime: bool,
        message: str,
        code: Any,
        epic: str = "",
    ) -> Dict[str, Any]:
        payload = cls._metadata(
            epic or asset,
            instrument_name=str(asset or ""),
            market_status="",
            delayed=False,
            realtime=realtime,
        )
        payload["provider_error_message"] = str(message or "unknown IG error")
        payload["provider_error_code"] = str(code or "ig_error")
        return payload


ig_market_bridge = IGMarketBridge()

