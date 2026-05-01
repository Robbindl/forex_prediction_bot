from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

import pandas as pd
import requests

from config.config import BINANCE_PUBLIC_DATA_ENABLED, BINANCE_TRADFI_CONTEXT_ENABLED
from utils.logger import get_logger

logger = get_logger()

_SPOT_BASE_URL = "https://api.binance.com"
_FUTURES_BASE_URL = "https://fapi.binance.com"
_SPOT_KLINES_ENDPOINT = "/api/v3/klines"
_SPOT_BOOK_TICKER_ENDPOINT = "/api/v3/ticker/bookTicker"
_FUTURES_KLINES_ENDPOINT = "/fapi/v1/klines"
_FUTURES_BOOK_TICKER_ENDPOINT = "/fapi/v1/ticker/bookTicker"

_SUPPORTED_SPOT_SYMBOLS = {
    "BTC-USD": "BTCUSDT",
    "ETH-USD": "ETHUSDT",
    "BNB-USD": "BNBUSDT",
    "SOL-USD": "SOLUSDT",
    "XRP-USD": "XRPUSDT",
}

_TRADFI_CONTEXT_SYMBOLS: Dict[str, Dict[str, str]] = {
    "QQQ": {"symbol": "QQQUSDT", "category": "equities"},
    "SPY": {"symbol": "SPYUSDT", "category": "equities"},
    "NVDA": {"symbol": "NVDAUSDT", "category": "equities"},
    "TSLA": {"symbol": "TSLAUSDT", "category": "equities"},
    "EWJ": {"symbol": "EWJUSDT", "category": "equities"},
    "EWY": {"symbol": "EWYUSDT", "category": "equities"},
    "XCU": {"symbol": "COPPERUSDT", "category": "commodities"},
    "NATGAS": {"symbol": "NATGASUSDT", "category": "commodities"},
}

_INTERVAL_MAP = {
    "1m": "1m",
    "5m": "5m",
    "15m": "15m",
    "30m": "30m",
    "1h": "1h",
    "4h": "4h",
    "1d": "1d",
}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class BinanceMarketBridge:
    """
    Public Binance bridge for:
      - spot crypto assets Deriv does not cover
      - a small TradFi-style futures proxy basket used only for context

    The proxy basket is intentionally small and does not replace the bot's
    primary tradable universe. It exists to sharpen cross-asset context
    without letting exchange proxies dominate decisions.
    """

    def __init__(self) -> None:
        self._enabled = bool(BINANCE_PUBLIC_DATA_ENABLED)
        self._tradfi_context_enabled = bool(BINANCE_TRADFI_CONTEXT_ENABLED)
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": "Robbie-TradingBot/1.0"})

    def list_profiles(self) -> list[str]:
        return ["binance_public"] if self._enabled else []

    def resolve_symbol_info(self, asset: str, category: str = "") -> Optional[Dict[str, Any]]:
        profile = self._resolve_profile(asset, category=category)
        if not profile:
            return None
        return {
            "symbol": profile["symbol"],
            "display_name": asset,
            "market": profile["market"],
            "exchange": "binance",
            "surface": profile["surface"],
        }

    def supports(self, asset: str, category: str = "") -> bool:
        return self._resolve_profile(asset, category=category) is not None

    def get_quote(
        self,
        asset: str,
        category: str = "",
    ) -> Tuple[Optional[float], Optional[float], Dict[str, Any]]:
        profile = self._resolve_profile(asset, category=category)
        if not profile:
            return None, None, {}

        try:
            response = self._session.get(
                f"{profile['base_url']}{profile['book_ticker_endpoint']}",
                params={"symbol": profile["symbol"]},
                timeout=8,
            )
            response.raise_for_status()
            payload = response.json() or {}
            bid = float(payload.get("bidPrice", 0.0) or 0.0)
            ask = float(payload.get("askPrice", 0.0) or 0.0)
            if bid <= 0 and ask <= 0:
                return None, None, {}

            if bid > 0 and ask > 0:
                price = (bid + ask) / 2.0
                spread = max(0.0, ask - bid)
            else:
                price = ask if ask > 0 else bid
                spread = 0.0

            try:
                from services.live_microstructure_service import get_service as get_live_microstructure_service

                get_live_microstructure_service().record_quote(
                    "binance",
                    asset,
                    bid=bid if bid > 0 else None,
                    ask=ask if ask > 0 else None,
                    price=price,
                    timestamp=datetime.now(timezone.utc),
                )
            except Exception:
                pass
            return float(price), float(spread), self._metadata(profile, realtime=True)
        except Exception as exc:
            logger.debug(f"[BinanceBridge] quote {asset}: {exc}")
            return None, None, {}

    def get_ohlcv(
        self,
        asset: str,
        interval: str,
        periods: int,
        category: str = "",
        end_time: Any = None,
        closed_only: bool = False,
    ) -> Tuple[Optional[pd.DataFrame], Dict[str, Any]]:
        profile = self._resolve_profile(asset, category=category)
        binance_interval = _INTERVAL_MAP.get((interval or "").lower())
        if not profile or not binance_interval:
            return None, {}

        try:
            cutoff = pd.to_datetime(end_time, utc=True, errors="coerce") if end_time not in (None, "") else None
            request_limit = int(max(2, periods + (2 if cutoff is not None or closed_only else 0)))
            params = {
                "symbol": profile["symbol"],
                "interval": binance_interval,
                "limit": request_limit,
            }
            if cutoff is not None and not pd.isna(cutoff):
                cutoff_ts = pd.Timestamp(cutoff)
                if cutoff_ts.tzinfo is None:
                    cutoff_ts = cutoff_ts.tz_localize("UTC")
                else:
                    cutoff_ts = cutoff_ts.tz_convert("UTC")
                params["endTime"] = int(cutoff_ts.timestamp() * 1000) - (1 if closed_only else 0)
            response = self._session.get(
                f"{profile['base_url']}{profile['klines_endpoint']}",
                params=params,
                timeout=10,
            )
            response.raise_for_status()
            rows = response.json() or []
            if not isinstance(rows, list) or not rows:
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
                    "close_time",
                    "quote_volume",
                    "trade_count",
                    "taker_base_volume",
                    "taker_quote_volume",
                    "ignore",
                ],
            )
            frame = frame[["open_time", "open", "high", "low", "close", "volume"]].copy()
            frame["timestamp"] = pd.to_datetime(frame["open_time"], unit="ms", utc=True)
            frame = frame.drop(columns=["open_time"]).set_index("timestamp")
            for column in ("open", "high", "low", "close", "volume"):
                frame[column] = pd.to_numeric(frame[column], errors="coerce")
            frame = frame.dropna(subset=["open", "high", "low", "close"])
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
            frame = frame.tail(int(max(2, periods)))
            if frame.empty:
                return None, {}

            return frame, self._metadata(profile, realtime=False)
        except Exception as exc:
            logger.debug(f"[BinanceBridge] ohlcv {asset}: {exc}")
            return None, {}

    def get_microstructure(self, asset: str, category: str = "") -> Dict[str, Any]:
        price, spread, meta = self.get_quote(asset, category=category)
        if price is None:
            return {}
        try:
            from services.live_microstructure_service import get_service as get_live_microstructure_service

            snapshot = get_live_microstructure_service().get_snapshot(
                "binance",
                asset,
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
        spread_bps = round((float(spread or 0.0) / float(price)) * 10000, 3) if price else 0.0
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

    def _resolve_profile(self, asset: str, category: str = "") -> Optional[Dict[str, str]]:
        if not self._enabled:
            return None
        key = str(asset or "").strip().upper()
        normalized_category = str(category or "").strip().lower()

        symbol = _SUPPORTED_SPOT_SYMBOLS.get(key)
        if symbol and normalized_category in {"", "crypto"}:
            return {
                "symbol": symbol,
                "market": "crypto",
                "surface": "spot",
                "base_url": _SPOT_BASE_URL,
                "book_ticker_endpoint": _SPOT_BOOK_TICKER_ENDPOINT,
                "klines_endpoint": _SPOT_KLINES_ENDPOINT,
            }

        if not self._tradfi_context_enabled:
            return None
        context = _TRADFI_CONTEXT_SYMBOLS.get(key)
        if not context:
            return None

        allowed_categories = {"", "context", context["category"]}
        if normalized_category and normalized_category not in allowed_categories:
            return None

        return {
            "symbol": context["symbol"],
            "market": "context",
            "surface": "futures_tradfi",
            "base_url": _FUTURES_BASE_URL,
            "book_ticker_endpoint": _FUTURES_BOOK_TICKER_ENDPOINT,
            "klines_endpoint": _FUTURES_KLINES_ENDPOINT,
        }

    @staticmethod
    def _metadata(profile: Dict[str, str], realtime: bool) -> Dict[str, Any]:
        return {
            "source": "Binance",
            "source_class": "secondary_api",
            "delayed": False,
            "realtime": bool(realtime),
            "from_cache": False,
            "exchange": "binance",
            "exchange_symbol": str(profile.get("symbol") or ""),
            "exchange_surface": str(profile.get("surface") or ""),
            "market": str(profile.get("market") or ""),
            "as_of_utc": _utc_now_iso(),
        }


binance_market_bridge = BinanceMarketBridge()
