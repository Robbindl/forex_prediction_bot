from __future__ import annotations

import json
import math
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from config.config import DERIV_APP_ID, DERIV_ENABLED, DERIV_SYMBOL_MAP
from services.market_hours_guard import build_market_status
from utils.display_time import format_display_datetime
from utils.logger import get_logger

logger = get_logger()

_PUBLIC_WS_URL = "wss://api.derivws.com/trading/v1/options/ws/public"
_ACTIVE_SYMBOLS_TTL_SEC = 6 * 60 * 60
_TRADING_TIMES_TTL_SEC = 60 * 60
_ECON_CAL_TTL_SEC = 20 * 60
_KEEPALIVE_SEC = 25
_WS_CONNECT_TIMEOUT_SEC = 15
_WS_REQUEST_TIMEOUT_SEC = 20
_PING_TIMEOUT_SEC = 5
_REQUEST_RETRIES = 1

_GRANULARITY_SECONDS = {
    "1m": 60,
    "5m": 300,
    "15m": 900,
    "30m": 1800,
    "1h": 3600,
    "4h": 14400,
    "1d": 86400,
}


class DerivRequestError(RuntimeError):
    """Logical Deriv API error returned by the server."""


class DerivUnsupportedRequestError(DerivRequestError):
    """Raised when the current Deriv endpoint does not support a request."""

_CATEGORY_HINTS = {
    "forex": ("forex", "major pairs", "minor pairs", "smart fx"),
    "crypto": ("crypto", "cryptocurrency"),
    "commodities": ("commodities", "commodity"),
    "indices": ("indices", "index", "stock indices", "basket indices"),
}

_ASSET_HINTS = {
    "EUR/USD": ("eur/usd", "eurusd", "euro/us dollar"),
    "EUR/JPY": ("eur/jpy", "eurjpy", "euro/japanese yen"),
    "EUR/GBP": ("eur/gbp", "eurgbp", "euro/british pound"),
    "GBP/USD": ("gbp/usd", "gbpusd", "british pound/us dollar"),
    "AUD/USD": ("aud/usd", "audusd", "australian dollar/us dollar"),
    "NZD/USD": ("nzd/usd", "nzdusd", "new zealand dollar/us dollar"),
    "USD/JPY": ("usd/jpy", "usdjpy", "us dollar/japanese yen"),
    "USD/CAD": ("usd/cad", "usdcad", "us dollar/canadian dollar"),
    "USD/CHF": ("usd/chf", "usdchf", "us dollar/swiss franc"),
    "GBP/JPY": ("gbp/jpy", "gbpjpy", "british pound/japanese yen"),
    "BTC-USD": ("btc/usd", "btcusd", "bitcoin"),
    "ETH-USD": ("eth/usd", "ethusd", "ethereum"),
    "BNB-USD": ("bnb/usd", "bnbusd", "binance coin"),
    "SOL-USD": ("sol/usd", "solusd", "solana"),
    "XRP-USD": ("xrp/usd", "xrpusd", "ripple"),
    "XAU/USD": ("gold", "xau", "xau/usd", "gold/usd"),
    "XAG/USD": ("silver", "xag", "xag/usd", "silver/usd"),
    "WTI": ("oil", "crude", "wti", "brent"),
    "WTI/USD": ("oil", "crude", "wti", "brent"),
    "US500": ("us 500", "us500", "s&p 500", "sp500", "spx"),
    "US100": ("us tech 100", "nasdaq", "nas100", "ustec", "us 100"),
    "US30": ("wall street", "us 30", "dow jones", "dj30", "us30"),
    "UK100": ("uk 100", "ftse", "ftse 100", "uk100"),
    "GER40": ("germany 40", "ger40", "dax", "dax 40"),
    "AUS200": ("australia 200", "aus200", "asx 200"),
    "JPN225": ("japan 225", "jpn225", "nikkei", "nikkei 225"),
    "GC=F": ("gold", "xau", "xau/usd", "gold/usd"),
    "SI=F": ("silver", "xag", "xag/usd", "silver/usd"),
    "CL=F": ("oil", "crude", "wti", "brent"),
    "^GSPC": ("us 500", "us500", "s&p 500", "sp500", "spx"),
    "^IXIC": ("us tech 100", "nasdaq", "nas100", "ustec", "us 100"),
    "^DJI": ("wall street", "us 30", "dow jones", "dj30", "us30"),
    "^FTSE": ("uk 100", "ftse", "ftse 100", "uk100"),
}

_DERIV_SYMBOL_HINTS = {
    "EUR/USD": ("frxeurusd", "eurusd"),
    "EUR/JPY": ("frxeurjpy", "eurjpy"),
    "EUR/GBP": ("frxeurgbp", "eurgbp"),
    "GBP/USD": ("frxgbpusd", "gbpusd"),
    "AUD/USD": ("frxaudusd", "audusd"),
    "NZD/USD": ("frxnzdusd", "nzdusd"),
    "USD/JPY": ("frxusdjpy", "usdjpy"),
    "USD/CAD": ("frxusdcad", "usdcad"),
    "USD/CHF": ("frxusdchf", "usdchf"),
    "GBP/JPY": ("frxgbpjpy", "gbpjpy"),
    "BTC-USD": ("crybtcusd", "btcusd"),
    "ETH-USD": ("cryethusd", "ethusd"),
    "BNB-USD": ("crybnbusd", "bnbusd"),
    "SOL-USD": ("crysolusd", "solusd"),
    "XRP-USD": ("cryxrpusd", "xrpusd"),
    "XAU/USD": ("frxxauusd", "xauusd", "gold"),
    "XAG/USD": ("frxxagusd", "xagusd", "silver"),
    "WTI": ("frxusoil", "usoil", "oil"),
    "WTI/USD": ("frxusoil", "usoil", "oil"),
    "US500": ("spx500", "us500", "sp500"),
    "US100": ("nas100", "ustech", "us100"),
    "US30": ("wallstreet", "us30", "dji"),
    "UK100": ("uk100", "ftse"),
    "GER40": ("otc_gdaxi", "gdaxi", "ger40", "dax"),
    "AUS200": ("otc_as51", "as51", "aus200", "asx"),
    "JPN225": ("otc_n225", "n225", "jpn225", "nikkei"),
    "GC=F": ("frxxauusd", "xauusd", "gold"),
    "SI=F": ("frxxagusd", "xagusd", "silver"),
    "CL=F": ("frxusoil", "usoil", "oil"),
    "^GSPC": ("spx500", "us500", "sp500"),
    "^IXIC": ("nas100", "ustech", "us100"),
    "^DJI": ("wallstreet", "us30", "dji"),
    "^FTSE": ("uk100", "ftse"),
}

_DEFAULT_SYMBOL_OVERRIDES = {
    "EUR/USD": "frxEURUSD",
    "EUR/JPY": "frxEURJPY",
    "EUR/GBP": "frxEURGBP",
    "GBP/USD": "frxGBPUSD",
    "GBP/JPY": "frxGBPJPY",
    "AUD/USD": "frxAUDUSD",
    "NZD/USD": "frxNZDUSD",
    "USD/JPY": "frxUSDJPY",
    "USD/CAD": "frxUSDCAD",
    "USD/CHF": "frxUSDCHF",
    "BTC-USD": "cryBTCUSD",
    "ETH-USD": "cryETHUSD",
    "BNB-USD": "cryBNBUSD",
    "SOL-USD": "crySOLUSD",
    "XRP-USD": "cryXRPUSD",
    "XAU/USD": "frxXAUUSD",
    "XAG/USD": "frxXAGUSD",
    "US30": "OTC_DJI",
    "US100": "OTC_NDX",
    "US500": "OTC_SPC",
    "UK100": "OTC_FTSE",
    "GER40": "OTC_GDAXI",
    "AUS200": "OTC_AS51",
    "JPN225": "OTC_N225",
}


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


def _as_bool(value: Any) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"1", "true", "yes", "open"}:
            return True
        if lowered in {"0", "false", "no", "closed"}:
            return False
    return None


def _iso_utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _try_parse_datetime(value: Any) -> Optional[datetime]:
    if value in (None, ""):
        return None

    try:
        if isinstance(value, datetime):
            return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)

        if isinstance(value, (int, float)):
            magnitude = abs(float(value))
            if magnitude >= 1_000_000_000_000:
                return datetime.fromtimestamp(float(value) / 1000.0, tz=timezone.utc)
            return datetime.fromtimestamp(float(value), tz=timezone.utc)

        text = str(value).strip()
        if not text:
            return None
        if text.isdigit():
            numeric = int(text)
            if len(text) >= 13:
                return datetime.fromtimestamp(numeric / 1000.0, tz=timezone.utc)
            return datetime.fromtimestamp(numeric, tz=timezone.utc)

        text = text.replace("Z", "+00:00")
        parsed = datetime.fromisoformat(text)
        return parsed.astimezone(timezone.utc) if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _impact_label(value: Any) -> str:
    if value is None:
        return "MEDIUM"
    if isinstance(value, (int, float)):
        numeric = float(value)
        if numeric >= 3:
            return "HIGH"
        if numeric >= 2:
            return "MEDIUM"
        return "LOW"

    text = str(value).strip().upper()
    if not text:
        return "MEDIUM"
    if text in {"HIGH", "MEDIUM", "LOW"}:
        return text
    if "HIGH" in text or text in {"3", "STRONG"}:
        return "HIGH"
    if "LOW" in text or text == "1":
        return "LOW"
    return "MEDIUM"


def _pip_digits_from_value(value: Optional[float]) -> Optional[int]:
    if value is None or value <= 0:
        return None
    if value >= 1:
        return 0
    try:
        return max(0, int(round(-math.log10(value))))
    except Exception:
        text = f"{value:.12f}".rstrip("0")
        if "." not in text:
            return 0
        return len(text.split(".", 1)[1])


class DerivBridge:
    """
    Public-market-data bridge for Deriv.

    Market data is fetched from Deriv and normalized into the shapes the rest
    of the bot already understands.
    """

    def __init__(self) -> None:
        self._enabled = bool(DERIV_ENABLED)
        self._app_id = str(DERIV_APP_ID or "").strip()
        self._url = _PUBLIC_WS_URL
        self._lock = threading.RLock()
        self._ws = None
        self._req_id = 0
        self._last_io = 0.0
        self._active_symbols: List[Dict[str, Any]] = []
        self._active_symbols_loaded_at = 0.0
        self._resolved_symbols: Dict[str, Optional[Dict[str, Any]]] = {}
        self._trading_times_cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}
        self._economic_calendar_cache: Dict[str, Tuple[float, List[Dict[str, Any]]]] = {}
        self._symbol_overrides = self._parse_symbol_map(DERIV_SYMBOL_MAP)
        self._has_connected_once = False
        self._reconnect_count = 0
        self._last_reconnect_log = 0.0
        self._next_connect_attempt_at = 0.0
        self._last_connect_error_log = 0.0
        self._last_connect_error_message = ""
        self._last_request_error_log = 0.0
        self._last_request_error_message = ""
        self._economic_calendar_supported: Optional[bool] = None
        self._econ_disabled_log_at = 0.0

    def _parse_symbol_map(self, raw: str) -> Dict[str, str]:
        overrides = dict(_DEFAULT_SYMBOL_OVERRIDES)
        if not raw:
            return overrides
        try:
            payload = json.loads(raw)
        except Exception as exc:
            logger.warning(f"[DerivBridge] Invalid DERIV_SYMBOL_MAP JSON: {exc}")
            return overrides
        if not isinstance(payload, dict):
            return overrides
        for key, value in payload.items():
            if str(key).strip() and str(value).strip():
                overrides[str(key)] = str(value)
        return overrides

    def list_profiles(self) -> List[str]:
        return ["deriv"] if self._enabled else []

    def is_available(self, asset: str = "", category: str = "") -> bool:
        if not self._enabled:
            return False
        with self._lock:
            if not self._ensure_session_locked():
                return False
            if not asset:
                return True
            return self._resolve_symbol_locked(asset, category=category) is not None

    def resolve_symbol_info(
        self,
        asset: str,
        category: str = "",
    ) -> Optional[Dict[str, Any]]:
        if not self._enabled:
            return None
        with self._lock:
            if not self._ensure_session_locked():
                return None
            resolved = self._resolve_symbol_locked(asset, category=category)
            return dict(resolved) if resolved else None

    def get_quote(
        self,
        asset: str,
        category: str = "",
    ) -> Tuple[Optional[float], Optional[float], Dict[str, Any]]:
        if not self._enabled:
            return None, None, {}

        with self._lock:
            if not self._ensure_session_locked():
                return None, None, {}

            resolved = self._resolve_symbol_locked(asset, category=category)
            if not resolved:
                return None, None, {}

            if _as_bool(resolved.get("exchange_is_open")) is False:
                return self._history_quote_fallback(resolved)

            try:
                # Use a single-tick snapshot rather than a live subscription.
                # Deriv supports subscribe=0 for one-shot tick reads; this keeps
                # the bridge from creating and forgetting a stream for every quote.
                response = self._request_locked({"ticks": resolved["symbol"], "subscribe": 0})
                tick = response.get("tick") or {}
                bid = _safe_float(tick.get("bid"))
                ask = _safe_float(tick.get("ask"))
                quote = _safe_float(tick.get("quote"))
                price = quote
                spread = 0.0

                if bid is not None and ask is not None and ask >= bid:
                    price = quote if quote is not None else (bid + ask) / 2.0
                    spread = max(0.0, ask - bid)
                elif quote is not None:
                    pip_size = max(1, _safe_int(tick.get("pip_size")) or _safe_int(resolved.get("pip_size")) or 4)
                    spread = 10 ** (-pip_size)

                if price is None:
                    return self._history_quote_fallback(resolved)

                try:
                    from services.live_microstructure_service import get_service as get_live_microstructure_service

                    get_live_microstructure_service().record_quote(
                        "deriv",
                        str(asset or ""),
                        bid=bid,
                        ask=ask,
                        price=price,
                        timestamp=time.time(),
                    )
                except Exception:
                    pass

                return float(price), float(spread or 0.0), self._metadata(resolved, source_class="primary_api", realtime=True)
            except Exception as exc:
                logger.debug(f"[DerivBridge] quote {asset}: {exc}")
                return self._history_quote_fallback(resolved)

    def get_ohlcv(
        self,
        asset: str,
        interval: str,
        periods: int,
        category: str = "",
        end_time: Any = None,
        closed_only: bool = False,
    ) -> Tuple[Optional[pd.DataFrame], Dict[str, Any]]:
        if not self._enabled:
            return None, {}

        granularity = _GRANULARITY_SECONDS.get(interval)
        if granularity is None:
            return None, {}

        with self._lock:
            if not self._ensure_session_locked():
                return None, {}

            resolved = self._resolve_symbol_locked(asset, category=category)
            if not resolved:
                return None, {}

            cutoff = _try_parse_datetime(end_time)
            request_count = int(max(2, periods + (2 if cutoff is not None or closed_only else 0)))
            payload = {
                "ticks_history": resolved["symbol"],
                "adjust_start_time": 1,
                "count": request_count,
                "end": "latest",
                "granularity": granularity,
                "style": "candles",
            }
            if cutoff is not None:
                payload["end"] = int(cutoff.timestamp()) - (1 if closed_only else 0)

            try:
                response = self._request_locked(payload)
                candles = response.get("candles") or ((response.get("history") or {}).get("candles") or [])
                rows = self._ohlcv_rows_from_candles(candles)
                if rows:
                    df = self._finalise_ohlcv_rows(rows, periods=periods, cutoff=cutoff, closed_only=closed_only)
                    if df is not None:
                        return df, self._metadata(resolved, source_class="primary_api")

                history = response.get("history") or {}
                rows = self._ohlcv_rows_from_history(history)
                if rows:
                    df = self._finalise_ohlcv_rows(rows, periods=periods, cutoff=cutoff, closed_only=closed_only)
                    if df is not None:
                        return df, self._metadata(resolved, source_class="primary_api")
            except Exception as exc:
                logger.debug(f"[DerivBridge] ohlcv {asset}: {exc}")

            return None, {}

    def get_microstructure(
        self,
        asset: str,
        category: str = "",
    ) -> Dict[str, Any]:
        price, spread, meta = self.get_quote(asset, category=category)
        if price is None:
            return {}
        try:
            from services.live_microstructure_service import get_service as get_live_microstructure_service

            snapshot = get_live_microstructure_service().get_snapshot(
                "deriv",
                str(asset or ""),
                price=price,
                spread=spread,
                meta=meta,
            )
            if snapshot:
                return {**meta, **snapshot}
        except Exception:
            pass
        spread_bps = 0.0
        try:
            spread_bps = round(float(spread or 0.0) / float(price) * 10000, 3) if float(price) > 0 else 0.0
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

    def get_market_status(
        self,
        asset: str,
        category: str = "",
    ) -> Optional[Dict[str, Any]]:
        if not self._enabled:
            return None

        with self._lock:
            if not self._ensure_session_locked():
                return None

            resolved = self._resolve_symbol_locked(asset, category=category)
            if not resolved:
                return None

            suspended = _as_bool(resolved.get("is_trading_suspended"))
            exchange_open = _as_bool(resolved.get("exchange_is_open"))
            market_display = str(
                resolved.get("market_display")
                or resolved.get("market")
                or resolved.get("submarket_display")
                or "Deriv"
            )

            if suspended is True:
                return build_market_status(
                    asset,
                    category=category,
                    provider_status={
                        "asset": asset,
                        "market_open": False,
                        "reason": f"{market_display} suspended on Deriv",
                        "source": "Deriv",
                        "utc_now": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
                    },
                )

            if exchange_open is not None:
                return build_market_status(
                    asset,
                    category=category,
                    provider_status={
                        "asset": asset,
                        "market_open": bool(exchange_open),
                        "reason": f"{market_display} {'open' if exchange_open else 'closed'} on Deriv",
                        "source": "Deriv",
                        "utc_now": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
                    },
                )

            trading_times = self._get_trading_times_locked(datetime.now(timezone.utc).date())
            if not trading_times:
                return None

            hours_status = self._status_from_trading_times(trading_times, resolved["symbol"])
            if hours_status is None:
                return None

            return build_market_status(
                asset,
                category=category,
                provider_status={
                    "asset": asset,
                    "market_open": bool(hours_status[0]),
                    "reason": str(hours_status[1]),
                    "source": "Deriv",
                    "utc_now": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
                },
            )

    def get_economic_events(
        self,
        start_time: Any = None,
        end_time: Any = None,
        currencies: Optional[List[str]] = None,
        impacts: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        if not self._enabled:
            return []

        start = _try_parse_datetime(start_time) or datetime.now(timezone.utc)
        end = _try_parse_datetime(end_time) or (start + timedelta(days=3))
        if end < start:
            start, end = end, start
        currency_key = ",".join(sorted({c.upper() for c in currencies or [] if c}))
        impact_key = ",".join(sorted({_impact_label(i) for i in impacts or ["HIGH", "MEDIUM"]}))
        cache_key = f"{start.isoformat()}:{end.isoformat()}:{currency_key}:{impact_key}"

        with self._lock:
            if self._economic_calendar_supported is False:
                self._economic_calendar_cache.setdefault(cache_key, (time.monotonic(), []))
                return []

            cached = self._economic_calendar_cache.get(cache_key)
            if cached and (time.monotonic() - cached[0]) < _ECON_CAL_TTL_SEC:
                return list(cached[1])

            if not self._ensure_session_locked():
                return []

            events: List[Dict[str, Any]] = []
            request_values = sorted({c.upper() for c in currencies or [] if c}) or ["all"]
            for request_value in request_values:
                payload = {"economic_calendar": request_value}
                try:
                    response = self._request_locked(payload)
                    batch = self._normalise_economic_events(
                        response,
                        start=start,
                        end=end,
                        currencies=currencies,
                        impacts=impacts,
                    )
                    if batch:
                        events.extend(batch)
                    self._economic_calendar_supported = True
                except DerivUnsupportedRequestError as exc:
                    self._economic_calendar_supported = False
                    now = time.monotonic()
                    if (now - self._econ_disabled_log_at) >= 300:
                        logger.debug(
                            "[DerivBridge] Economic calendar unsupported by this Deriv endpoint"
                        )
                        self._econ_disabled_log_at = now
                    logger.debug(f"[DerivBridge] economic calendar unsupported for {payload}: {exc}")
                    events = []
                    break
                except Exception as exc:
                    logger.debug(f"[DerivBridge] economic calendar request failed for {payload}: {exc}")

            if events:
                deduped: Dict[str, Dict[str, Any]] = {}
                for item in events:
                    key = "|".join(
                        [
                            str(item.get("date", "")),
                            str(item.get("currency", "")),
                            str(item.get("event", "")),
                        ]
                    )
                    deduped[key] = item
                events = sorted(deduped.values(), key=lambda item: item.get("date", ""))

            self._economic_calendar_cache[cache_key] = (time.monotonic(), list(events))
            return events

    def get_high_impact_events(
        self,
        days: int = 3,
        currencies: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        start = datetime.now(timezone.utc)
        end = start + timedelta(days=max(1, days))
        return self.get_economic_events(
            start_time=start,
            end_time=end,
            currencies=currencies,
            impacts=["HIGH", "MEDIUM"],
        )

    def _ensure_session_locked(self) -> bool:
        if not self._enabled:
            return False

        now = time.monotonic()
        if not self._ws_is_connected_locked():
            self._close_locked()
            return self._connect_locked()

        if now - self._last_io < _KEEPALIVE_SEC:
            return True

        try:
            self._request_locked({"ping": 1}, request_timeout=_PING_TIMEOUT_SEC, max_retries=0)
            return True
        except Exception:
            self._close_locked()
            return self._connect_locked()

    def _ws_is_connected_locked(self) -> bool:
        ws = self._ws
        if ws is None:
            return False
        try:
            if not bool(getattr(ws, "connected", False)):
                return False
            return getattr(ws, "sock", None) is not None
        except Exception:
            return False

    def _connect_locked(self) -> bool:
        now = time.monotonic()
        if not self._app_id:
            logger.warning("[DerivBridge] connect failed: DERIV_APP_ID is not configured")
            return False
        if now < self._next_connect_attempt_at:
            return False
        try:
            from websocket import create_connection

            headers = [f"Deriv-App-ID: {self._app_id}"]
            self._ws = create_connection(
                self._url,
                timeout=_WS_CONNECT_TIMEOUT_SEC,
                enable_multithread=True,
                header=headers or None,
            )
            self._ws.settimeout(_WS_REQUEST_TIMEOUT_SEC)
            self._last_io = time.monotonic()
            self._next_connect_attempt_at = 0.0
            self._last_connect_error_message = ""
            if not self._has_connected_once:
                logger.info(f"[DerivBridge] Connected to Deriv public market data (app_id={self._app_id})")
                self._has_connected_once = True
                self._last_reconnect_log = self._last_io
            else:
                self._reconnect_count += 1
                if (self._last_io - self._last_reconnect_log) >= 60:
                    logger.info(
                        f"[DerivBridge] Reconnected to Deriv public market data "
                        f"(reconnects={self._reconnect_count})"
                    )
                    self._last_reconnect_log = self._last_io
                else:
                    logger.debug("[DerivBridge] Reconnected to Deriv public market data")
            return True
        except Exception as exc:
            message = str(exc)
            self._next_connect_attempt_at = now + 5.0
            if message != self._last_connect_error_message or (now - self._last_connect_error_log) >= 60:
                logger.warning(f"[DerivBridge] connect failed: {message}")
                self._last_connect_error_log = now
                self._last_connect_error_message = message
            else:
                logger.debug(f"[DerivBridge] connect failed: {message}")
            self._ws = None
            return False

    def _close_locked(self) -> None:
        if self._ws is not None:
            try:
                self._ws.close()
            except Exception:
                pass
        self._ws = None

    def _next_req_id_locked(self) -> int:
        self._req_id += 1
        return self._req_id

    @staticmethod
    def _payload_name(payload: Dict[str, Any]) -> str:
        for key in (
            "ticks_history",
            "ticks",
            "active_symbols",
            "trading_times",
            "economic_calendar",
            "forget",
            "ping",
        ):
            if key in payload:
                return key
        for key in payload:
            if key != "req_id":
                return str(key)
        return "request"

    @staticmethod
    def _response_matches_request(response: Dict[str, Any], message: Dict[str, Any], req_id: int) -> bool:
        response_req_id = response.get("req_id")
        if response_req_id is not None:
            return response_req_id == req_id

        if "ping" in message:
            return "ping" in response or str(response.get("msg_type") or "").lower() == "ping"
        if "ticks" in message:
            return "tick" in response or str(response.get("msg_type") or "").lower() == "tick"
        if "ticks_history" in message:
            msg_type = str(response.get("msg_type") or "").lower()
            return "candles" in response or "history" in response or msg_type in {"candles", "history"}
        if "active_symbols" in message:
            return "active_symbols" in response or str(response.get("msg_type") or "").lower() == "active_symbols"
        if "trading_times" in message:
            return "trading_times" in response or str(response.get("msg_type") or "").lower() == "trading_times"
        if "economic_calendar" in message:
            msg_type = str(response.get("msg_type") or "").lower()
            return "events" in response or "economic_calendar" in response or msg_type == "economic_calendar"
        if "forget" in message:
            msg_type = str(response.get("msg_type") or "").lower()
            return "forget" in response or msg_type == "forget"
        return False

    def _log_request_error(self, message: str) -> None:
        now = time.monotonic()
        if message != self._last_request_error_message or (now - self._last_request_error_log) >= 60:
            logger.warning(message)
            self._last_request_error_log = now
            self._last_request_error_message = message
        else:
            logger.debug(message)

    def _request_locked(
        self,
        payload: Dict[str, Any],
        *,
        request_timeout: Optional[float] = None,
        max_retries: int = _REQUEST_RETRIES,
    ) -> Dict[str, Any]:
        last_exc: Optional[Exception] = None
        total_attempts = max(1, int(max_retries) + 1)
        op_name = self._payload_name(payload)

        for attempt in range(1, total_attempts + 1):
            if not self._ws_is_connected_locked():
                self._close_locked()
            if self._ws is None and not self._connect_locked():
                raise RuntimeError("Deriv WebSocket unavailable")

            req_id = self._next_req_id_locked()
            message = dict(payload)
            message["req_id"] = req_id

            try:
                if request_timeout is not None and hasattr(self._ws, "settimeout"):
                    self._ws.settimeout(request_timeout)
                self._ws.send(json.dumps(message))
                self._last_io = time.monotonic()
                while True:
                    raw = self._ws.recv()
                    self._last_io = time.monotonic()
                    response = json.loads(raw)
                    if not self._response_matches_request(response, message, req_id):
                        continue
                    error = response.get("error")
                    if error:
                        code = error.get("code", "Error")
                        message_text = str(error.get("message", "unknown Deriv error"))
                        detail = f"{code}: {message_text}"
                        if str(code).lower() == "unrecognisedrequest" or "unrecognised request" in message_text.lower():
                            raise DerivUnsupportedRequestError(detail)
                        raise DerivRequestError(detail)
                    return response
            except Exception as exc:
                last_exc = exc
                detail = f"[DerivBridge] {op_name} request failed on attempt {attempt}/{total_attempts}: {exc}"
                if isinstance(exc, DerivRequestError):
                    if attempt < total_attempts:
                        logger.debug(detail)
                    else:
                        self._log_request_error(detail)
                    break
                if attempt < total_attempts:
                    logger.debug(detail)
                else:
                    self._log_request_error(detail)
                self._close_locked()
                if attempt < total_attempts:
                    continue
            finally:
                if self._ws is not None and hasattr(self._ws, "settimeout"):
                    try:
                        self._ws.settimeout(_WS_REQUEST_TIMEOUT_SEC)
                    except Exception:
                        pass

        if last_exc is not None:
            raise last_exc
        raise RuntimeError(f"Deriv {op_name} request failed")

    def _load_active_symbols_locked(self) -> List[Dict[str, Any]]:
        if self._active_symbols and (time.monotonic() - self._active_symbols_loaded_at) < _ACTIVE_SYMBOLS_TTL_SEC:
            return self._active_symbols

        response = self._request_locked({"active_symbols": "full"})
        symbols = response.get("active_symbols") or []
        if not isinstance(symbols, list):
            symbols = []
        self._active_symbols = [self._normalise_active_symbol(item) for item in symbols if isinstance(item, dict)]
        self._active_symbols_loaded_at = time.monotonic()
        self._resolved_symbols.clear()
        return self._active_symbols

    def _resolve_symbol_locked(self, asset: str, category: str = "") -> Optional[Dict[str, Any]]:
        cache_key = f"{category}:{asset}"
        if cache_key in self._resolved_symbols:
            return self._resolved_symbols[cache_key]

        symbols = self._load_active_symbols_locked()
        override = self._symbol_overrides.get(asset)
        if override:
            for item in symbols:
                if str(item.get("symbol", "")).strip().lower() == override.strip().lower():
                    self._resolved_symbols[cache_key] = item
                    return item

        candidates = []
        for item in symbols:
            score = self._candidate_score(asset, category, item)
            if score > 0:
                candidates.append((score, item))

        candidates.sort(key=lambda pair: pair[0], reverse=True)
        resolved = candidates[0][1] if candidates else None
        self._resolved_symbols[cache_key] = resolved
        if resolved is None:
            logger.debug(f"[DerivBridge] No Deriv symbol match for {asset} ({category})")
        return resolved

    def _candidate_score(self, asset: str, category: str, item: Dict[str, Any]) -> int:
        symbol = str(item.get("symbol", "")).strip()
        if not symbol:
            return 0

        haystack = " ".join(
            str(item.get(key, "")).lower()
            for key in (
                "symbol",
                "display_name",
                "display_name_long",
                "market",
                "market_display",
                "submarket",
                "submarket_display",
                "symbol_type",
            )
        )

        score = 0
        matched_asset_hint = False
        matched_symbol_hint = False
        category_hints = _CATEGORY_HINTS.get((category or "").lower(), ())
        if category_hints and any(hint in haystack for hint in category_hints):
            score += 6

        for hint in _ASSET_HINTS.get(asset, ()):
            if hint in haystack:
                score += 8
                matched_asset_hint = True

        lowered_symbol = symbol.lower()
        for hint in _DERIV_SYMBOL_HINTS.get(asset, ()):
            if hint in lowered_symbol:
                score += 10
                matched_symbol_hint = True

        if not matched_asset_hint and not matched_symbol_hint:
            return 0

        if "otc" in haystack:
            score -= 2
        if _as_bool(item.get("is_trading_suspended")) is True:
            score -= 3
        if _as_bool(item.get("exchange_is_open")) is True:
            score += 1

        return score

    def _metadata(
        self,
        resolved: Dict[str, Any],
        source_class: str = "primary_api",
        realtime: bool = False,
        delayed: bool = False,
    ) -> Dict[str, Any]:
        return {
            "source": "Deriv",
            "source_class": source_class,
            "delayed": bool(delayed),
            "realtime": realtime,
            "from_cache": False,
            "as_of_utc": _iso_utc_now(),
            "deriv_symbol": str(resolved.get("symbol", "")),
            "deriv_display_name": str(resolved.get("display_name") or resolved.get("display_name_long") or ""),
            "deriv_market": str(resolved.get("market_display") or resolved.get("market") or ""),
            "deriv_submarket": str(resolved.get("submarket_display") or resolved.get("submarket") or ""),
        }

    def _history_quote_fallback(self, resolved: Dict[str, Any]) -> Tuple[Optional[float], Optional[float], Dict[str, Any]]:
        pip = _safe_float(resolved.get("pip"))
        spread = float(pip) if pip and pip > 0 else 0.0
        market_open = _as_bool(resolved.get("exchange_is_open"))
        delayed = market_open is False
        price = None

        try:
            response = self._request_locked({
                "ticks_history": resolved["symbol"],
                "count": 1,
                "end": "latest",
                "style": "ticks",
            })
            history = response.get("history") or {}
            prices = history.get("prices") or []
            if prices:
                price = _safe_float(prices[-1])
        except Exception:
            price = None

        if price is None:
            try:
                response = self._request_locked({
                    "ticks_history": resolved["symbol"],
                    "count": 1,
                    "end": "latest",
                    "style": "candles",
                    "granularity": _GRANULARITY_SECONDS["15m"],
                })
                candles = response.get("candles") or []
                if candles:
                    price = _safe_float((candles[-1] or {}).get("close"))
                    delayed = True
            except Exception:
                price = None

        if price is None:
            return None, None, {}

        meta = self._metadata(resolved, source_class="primary_api", realtime=not delayed, delayed=delayed)
        meta["market_open"] = bool(market_open) if market_open is not None else False
        try:
            from services.live_microstructure_service import get_service as get_live_microstructure_service

            get_live_microstructure_service().record_quote(
                "deriv",
                str(resolved.get("display_name") or resolved.get("display_name_long") or resolved.get("symbol") or ""),
                price=price,
                timestamp=time.time(),
            )
        except Exception:
            pass
        return float(price), spread, meta

    @staticmethod
    def _ohlcv_row_from_candle(candle: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not isinstance(candle, dict):
            return None

        timestamp = _try_parse_datetime(candle.get("epoch"))
        if timestamp is None:
            return None

        return {
            "timestamp": timestamp,
            "open": _safe_float(candle.get("open")),
            "high": _safe_float(candle.get("high")),
            "low": _safe_float(candle.get("low")),
            "close": _safe_float(candle.get("close")),
            "volume": _safe_float(candle.get("volume")) or 0.0,
        }

    @staticmethod
    def _ohlcv_rows_from_candles(candles: Any) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        if not isinstance(candles, list):
            return rows

        for candle in candles:
            row = DerivBridge._ohlcv_row_from_candle(candle)
            if row is not None:
                rows.append(row)
        return rows

    @staticmethod
    def _ohlcv_rows_from_history(history: Dict[str, Any]) -> List[Dict[str, Any]]:
        prices = history.get("prices") or []
        times = history.get("times") or []
        if not prices or not times or len(prices) != len(times):
            return []

        rows: List[Dict[str, Any]] = []
        for raw_time, raw_price in zip(times, prices):
            timestamp = _try_parse_datetime(raw_time)
            price = _safe_float(raw_price)
            if timestamp is None or price is None:
                continue
            rows.append(
                {
                    "timestamp": timestamp,
                    "open": price,
                    "high": price,
                    "low": price,
                    "close": price,
                    "volume": 0.0,
                }
            )
        return rows

    @staticmethod
    def _finalise_ohlcv_rows(
        rows: List[Dict[str, Any]],
        periods: int,
        cutoff: Optional[datetime],
        closed_only: bool,
    ) -> Optional[pd.DataFrame]:
        if not rows:
            return None

        df = pd.DataFrame(rows).set_index("timestamp")
        df.index = pd.to_datetime(df.index, utc=True)
        if cutoff is not None:
            if closed_only:
                df = df[df.index < cutoff]
            else:
                df = df[df.index <= cutoff]
        df = df.tail(int(max(2, periods)))
        return df[["open", "high", "low", "close", "volume"]].astype(float)

    def _get_trading_times_locked(self, day) -> Dict[str, Any]:
        day_key = day.isoformat()
        cached = self._trading_times_cache.get(day_key)
        if cached and (time.monotonic() - cached[0]) < _TRADING_TIMES_TTL_SEC:
            return dict(cached[1])

        response = self._request_locked({"trading_times": day_key})
        trading_times = response.get("trading_times") or {}
        if isinstance(trading_times, dict):
            trading_times.setdefault("date", day_key)
            self._trading_times_cache[day_key] = (time.monotonic(), trading_times)
            return dict(trading_times)
        return {}

    def _status_from_trading_times(self, trading_times: Dict[str, Any], deriv_symbol: str) -> Optional[Tuple[bool, str]]:
        now = datetime.now(timezone.utc)
        symbol_key = deriv_symbol.lower()
        markets = trading_times.get("markets") or []

        for market in markets:
            for submarket in market.get("submarkets") or []:
                for symbol in submarket.get("symbols") or []:
                    symbol_code = str(symbol.get("underlying_symbol") or symbol.get("symbol", "")).strip().lower()
                    if symbol_code != symbol_key:
                        continue

                    sessions = self._trading_times_sessions(trading_times, symbol)
                    if not sessions:
                        continue
                    return self._status_from_sessions(sessions, now)

        return None

    @staticmethod
    def _trading_times_sessions(trading_times: Dict[str, Any], symbol: Dict[str, Any]) -> List[Tuple[datetime, datetime]]:
        opens = []
        closes = []
        times_payload = symbol.get("times")
        if isinstance(times_payload, dict):
            opens = times_payload.get("open") or times_payload.get("opens") or []
            closes = times_payload.get("close") or times_payload.get("closes") or []
        else:
            opens = symbol.get("open") or symbol.get("opens") or []
            closes = symbol.get("close") or symbol.get("closes") or []

        if not isinstance(opens, list):
            opens = [opens]
        if not isinstance(closes, list):
            closes = [closes]

        sessions: List[Tuple[datetime, datetime]] = []
        trading_day = trading_times.get("date")
        if not trading_day:
            return sessions

        for raw_open, raw_close in zip(opens, closes):
            if not raw_open or not raw_close or raw_open == "--" or raw_close == "--":
                continue
            try:
                open_dt = datetime.fromisoformat(f"{trading_day}T{raw_open}+00:00")
                close_dt = datetime.fromisoformat(f"{trading_day}T{raw_close}+00:00")
            except Exception:
                continue
            sessions.append((open_dt, close_dt))
        return sessions

    @staticmethod
    def _status_from_sessions(sessions: List[Tuple[datetime, datetime]], now: datetime) -> Tuple[bool, str]:
        for open_dt, close_dt in sessions:
            if open_dt <= now <= close_dt:
                return True, f"Open on Deriv until {format_display_datetime(close_dt, '%H:%M')}"

        next_open = min((open_dt for open_dt, _ in sessions if open_dt > now), default=None)
        if next_open is not None:
            return False, f"Closed on Deriv until {format_display_datetime(next_open, '%H:%M')}"
        return False, "Closed on Deriv"

    @staticmethod
    def _normalise_active_symbol(item: Dict[str, Any]) -> Dict[str, Any]:
        normalized = dict(item)
        symbol = str(item.get("underlying_symbol") or item.get("symbol") or "").strip()
        display_name = str(item.get("underlying_symbol_name") or item.get("display_name") or "").strip()
        market = str(item.get("market") or "").strip()
        submarket = str(item.get("submarket") or "").strip()
        pip_value = _safe_float(item.get("pip_size"))
        pip_digits = _pip_digits_from_value(pip_value)

        normalized["symbol"] = symbol
        normalized["display_name"] = display_name
        normalized["display_name_long"] = display_name
        normalized["symbol_type"] = str(item.get("underlying_symbol_type") or item.get("symbol_type") or "").strip()
        normalized["market_display"] = str(item.get("market_display") or market.replace("_", " ").title())
        normalized["submarket_display"] = str(item.get("submarket_display") or submarket.replace("_", " ").title())
        normalized["pip_size"] = pip_digits
        normalized["pip"] = pip_value
        return normalized

    def _normalise_economic_events(
        self,
        response: Dict[str, Any],
        start: datetime,
        end: datetime,
        currencies: Optional[List[str]] = None,
        impacts: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        currency_filter = {c.upper() for c in currencies or [] if c}
        impact_filter = {_impact_label(i) for i in impacts or ["HIGH", "MEDIUM"]}
        raw_events = (
            response.get("economic_calendar")
            or response.get("events")
            or ((response.get("calendar") or {}).get("events"))
            or []
        )
        if not isinstance(raw_events, list):
            return []

        items = []
        for event in raw_events:
            if not isinstance(event, dict):
                continue

            event_dt = (
                _try_parse_datetime(event.get("epoch"))
                or _try_parse_datetime(event.get("event_date"))
                or _try_parse_datetime(event.get("release_date"))
                or _try_parse_datetime(event.get("date"))
                or _try_parse_datetime(event.get("datetime"))
                or _try_parse_datetime(event.get("timestamp"))
            )
            if event_dt is None or event_dt < start or event_dt > end:
                continue

            currency = str(event.get("currency") or event.get("symbol") or "").upper()
            if currency_filter and currency and currency not in currency_filter:
                continue

            impact = _impact_label(event.get("impact") or event.get("importance") or event.get("market_impact"))
            if impact not in impact_filter:
                continue

            items.append({
                "date": format_display_datetime(event_dt, "%Y-%m-%d %H:%M"),
                "event": str(event.get("title") or event.get("description") or event.get("name") or event.get("event") or ""),
                "impact": impact,
                "actual": event.get("actual"),
                "estimate": event.get("forecast") or event.get("estimate"),
                "forecast": event.get("forecast") or event.get("estimate"),
                "previous": event.get("previous"),
                "currency": currency,
                "surprise_direction": str(event.get("surprise_direction") or ""),
                "source": "Deriv",
            })

        items.sort(key=lambda item: item.get("date", ""))
        return items


deriv_bridge = DerivBridge()
