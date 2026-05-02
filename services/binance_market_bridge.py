from __future__ import annotations

import threading
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests

from config.config import BINANCE_PUBLIC_DATA_ENABLED, BINANCE_TRADFI_CONTEXT_ENABLED
from utils.logger import get_logger

logger = get_logger()

_SPOT_BASE_URL = "https://api.binance.com"
_FUTURES_BASE_URL = "https://fapi.binance.com"
_SPOT_KLINES_ENDPOINT = "/api/v3/klines"
_SPOT_BOOK_TICKER_ENDPOINT = "/api/v3/ticker/bookTicker"
_SPOT_DEPTH_ENDPOINT = "/api/v3/depth"
_FUTURES_KLINES_ENDPOINT = "/fapi/v1/klines"
_FUTURES_BOOK_TICKER_ENDPOINT = "/fapi/v1/ticker/bookTicker"
_FUTURES_DEPTH_ENDPOINT = "/fapi/v1/depth"
_DEPTH_LIMIT = "1000"

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

_ASSET_BY_SYMBOL: Dict[str, str] = {
    **{symbol: asset for asset, symbol in _SUPPORTED_SPOT_SYMBOLS.items()},
    **{row["symbol"]: asset for asset, row in _TRADFI_CONTEXT_SYMBOLS.items()},
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


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value if value not in (None, "") else default)
    except Exception:
        return float(default)


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
        self._depth_lock = threading.RLock()
        self._depth_books: Dict[str, Dict[str, Any]] = {}
        self._depth_resync_attempted_at: Dict[str, float] = {}

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
        profile = self._resolve_profile(asset, category=category)
        if not profile:
            return {}

        price, spread, meta = self.get_quote(asset, category=category)
        if not meta:
            meta = self._metadata(profile, realtime=True)

        depth_price, depth_spread, depth_meta = self._record_depth_snapshot(
            asset,
            profile,
            quote_price=price,
        )
        if depth_price is not None:
            price = depth_price
            spread = depth_spread

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
                depth_meta_payload = dict(depth_meta)
                if (
                    bool(snapshot.get("dom_ladder_ready"))
                    and str(snapshot.get("depth_update_mode") or "").strip().lower() == "event_stream"
                    and bool(snapshot.get("depth_available"))
                ):
                    depth_meta_payload["microstructure_source"] = "binance_live_depth"
                    depth_meta_payload["depth_provider_trust_score"] = max(
                        0.90,
                        _safe_float(depth_meta_payload.get("depth_provider_trust_score"), 0.0),
                    )
                return {
                    **meta,
                    "quote_price": float(price),
                    "quote_spread": float(spread or 0.0),
                    **snapshot,
                    **depth_meta_payload,
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
            **depth_meta,
        }

    def _record_depth_snapshot(
        self,
        asset: str,
        profile: Dict[str, str],
        *,
        quote_price: Optional[float],
    ) -> Tuple[Optional[float], float, Dict[str, Any]]:
        try:
            response = self._session.get(
                f"{profile['base_url']}{profile['depth_endpoint']}",
                params={"symbol": profile["symbol"], "limit": _DEPTH_LIMIT},
                timeout=8,
            )
            response.raise_for_status()
            payload = response.json() or {}
            bids = list(payload.get("bids") or [])
            asks = list(payload.get("asks") or [])
            levels = self._normalise_depth_levels(bids, asks)
            if not levels:
                return None, 0.0, {}
            self._seed_depth_book(
                profile["symbol"],
                levels,
                last_update_id=payload.get("lastUpdateId"),
            )

            top_bid = _safe_float(levels[0].get("bid"))
            top_ask = _safe_float(levels[0].get("ask"))
            if top_bid <= 0.0 or top_ask <= 0.0 or top_ask < top_bid:
                return None, 0.0, {}

            price = (top_bid + top_ask) / 2.0
            spread = max(0.0, top_ask - top_bid)
            try:
                from services.live_microstructure_service import get_service as get_live_microstructure_service

                get_live_microstructure_service().record_quote(
                    "binance",
                    asset,
                    bid=top_bid,
                    ask=top_ask,
                    price=price,
                    levels=levels,
                    timestamp=datetime.now(timezone.utc),
                    flags="depth_snapshot,binance_rest_depth",
                    event_type="depth_snapshot",
                )
            except Exception:
                pass

            alignment_score, agreement_state = self._depth_quote_alignment(quote_price, price)
            return (
                float(price),
                float(spread),
                {
                    "microstructure_source": "binance_rest_depth",
                    "depth_provider": "Binance",
                    "depth_provider_class": "exchange_depth",
                    "depth_environment": "live",
                    "depth_provider_trust_score": 0.88,
                    "depth_quote_alignment_score": alignment_score,
                    "depth_quote_agreement_state": agreement_state,
                    "orderbook_top_bids": self._top_side_levels(levels, side="bid", limit=20),
                    "orderbook_top_asks": self._top_side_levels(levels, side="ask", limit=20),
                    "binance_depth_limit": int(_DEPTH_LIMIT),
                },
            )
        except Exception as exc:
            logger.debug(f"[BinanceBridge] depth {asset}: {exc}")
            return None, 0.0, {}

    def record_stream_depth_delta(
        self,
        symbol: str,
        bids: List[Any],
        asks: List[Any],
        *,
        timestamp: Any = None,
        first_update_id: Any = None,
        final_update_id: Any = None,
    ) -> bool:
        canonical_asset = _ASSET_BY_SYMBOL.get(str(symbol or "").strip().upper())
        if not canonical_asset:
            return False

        profile = self._resolve_profile(canonical_asset, category="crypto") or self._resolve_profile(
            canonical_asset,
            category="context",
        )
        if not profile:
            return False

        symbol_key = profile["symbol"]
        if not self._ensure_depth_book_seeded(symbol_key, canonical_asset, profile):
            return False

        try:
            final_id = int(final_update_id) if final_update_id not in (None, "") else 0
        except Exception:
            final_id = 0
        try:
            first_id = int(first_update_id) if first_update_id not in (None, "") else 0
        except Exception:
            first_id = 0

        with self._depth_lock:
            state = self._depth_books.get(symbol_key)
            if not state:
                return False

            last_id = int(state.get("last_update_id") or 0)
            if final_id and last_id and final_id <= last_id:
                return False
            if first_id and last_id and first_id > last_id + 1:
                self._depth_books.pop(symbol_key, None)
                self._note_depth_sequence_gap(canonical_asset, reason="binance_depth_sequence_gap")
                return self._ensure_depth_book_seeded(symbol_key, canonical_asset, profile)

            bid_book = state.setdefault("bids", {})
            ask_book = state.setdefault("asks", {})
            self._apply_depth_delta(bid_book, bids)
            self._apply_depth_delta(ask_book, asks)
            if final_id:
                state["last_update_id"] = final_id
            state["last_ts"] = timestamp

            levels = self._levels_from_books(bid_book, ask_book, limit=int(_DEPTH_LIMIT))

        if not levels:
            return False

        top_bid = _safe_float(levels[0].get("bid"))
        top_ask = _safe_float(levels[0].get("ask"))
        if top_bid <= 0.0 or top_ask <= 0.0 or top_ask < top_bid:
            return False

        price = (top_bid + top_ask) / 2.0
        try:
            from services.dom_stream_health_service import get_service as get_dom_stream_health_service
            from services.live_microstructure_service import get_service as get_live_microstructure_service

            get_live_microstructure_service().record_depth_delta(
                "binance",
                canonical_asset,
                bid=top_bid,
                ask=top_ask,
                price=price,
                levels=levels,
                timestamp=timestamp,
                flags="depth_delta,ladder_delta,binance_live_depth",
            )
            get_dom_stream_health_service().note_depth("binance", canonical_asset, ts=self._timestamp_seconds(timestamp))
        except Exception:
            return False
        return True

    def record_stream_trade(
        self,
        symbol: str,
        *,
        price: Any,
        qty: Any = None,
        side: str = "",
        timestamp: Any = None,
    ) -> bool:
        canonical_asset = _ASSET_BY_SYMBOL.get(str(symbol or "").strip().upper())
        if not canonical_asset:
            return False
        try:
            from services.dom_stream_health_service import get_service as get_dom_stream_health_service
            from services.live_microstructure_service import get_service as get_live_microstructure_service

            get_live_microstructure_service().record_trade(
                "binance",
                canonical_asset,
                price=price,
                size=qty,
                side=side,
                timestamp=timestamp,
                flags="trade_print,trade_stream,binance_live_depth",
            )
            get_dom_stream_health_service().note_trade("binance", canonical_asset, ts=self._timestamp_seconds(timestamp))
        except Exception:
            return False
        return True

    def record_stream_quote(
        self,
        symbol: str,
        *,
        price: Any,
        timestamp: Any = None,
    ) -> bool:
        canonical_asset = _ASSET_BY_SYMBOL.get(str(symbol or "").strip().upper())
        if not canonical_asset:
            return False
        price_value = _safe_float(price)
        if price_value <= 0.0:
            return False
        try:
            from services.dom_stream_health_service import get_service as get_dom_stream_health_service
            from services.live_microstructure_service import get_service as get_live_microstructure_service

            get_live_microstructure_service().record_quote(
                "binance",
                canonical_asset,
                price=price_value,
                timestamp=timestamp,
                flags="ticker,binance_stream",
            )
            get_dom_stream_health_service().mark_connected(
                "binance",
                canonical_asset,
                ts=self._timestamp_seconds(timestamp),
            )
        except Exception:
            return False
        return True

    def _seed_depth_book(
        self,
        symbol: str,
        levels: List[Dict[str, Any]],
        *,
        last_update_id: Any = None,
    ) -> None:
        symbol_key = str(symbol or "").strip().upper()
        if not symbol_key:
            return
        bid_book: Dict[float, float] = {}
        ask_book: Dict[float, float] = {}
        for level in levels:
            bid_px = _safe_float(level.get("bid"))
            bid_sz = _safe_float(level.get("bid_size"))
            ask_px = _safe_float(level.get("ask"))
            ask_sz = _safe_float(level.get("ask_size"))
            if bid_px > 0.0 and bid_sz > 0.0:
                bid_book[bid_px] = bid_sz
            if ask_px > 0.0 and ask_sz > 0.0:
                ask_book[ask_px] = ask_sz
        try:
            update_id = int(last_update_id) if last_update_id not in (None, "") else 0
        except Exception:
            update_id = 0
        with self._depth_lock:
            self._depth_books[symbol_key] = {
                "bids": bid_book,
                "asks": ask_book,
                "last_update_id": update_id,
                "last_ts": datetime.now(timezone.utc),
            }

    def _ensure_depth_book_seeded(
        self,
        symbol: str,
        canonical_asset: str,
        profile: Dict[str, str],
    ) -> bool:
        symbol_key = str(symbol or "").strip().upper()
        with self._depth_lock:
            if symbol_key in self._depth_books:
                return True
        now = datetime.now(timezone.utc).timestamp()
        last_attempt = float(self._depth_resync_attempted_at.get(symbol_key, 0.0) or 0.0)
        if now - last_attempt < 5.0:
            return False
        self._depth_resync_attempted_at[symbol_key] = now
        depth_price, _, _ = self._record_depth_snapshot(canonical_asset, profile, quote_price=None)
        return depth_price is not None

    @staticmethod
    def _apply_depth_delta(book: Dict[float, float], rows: List[Any]) -> None:
        for row in list(rows or []):
            try:
                price = float(row[0])
                size = float(row[1])
            except Exception:
                continue
            if price <= 0.0:
                continue
            if size <= 0.0:
                book.pop(price, None)
            else:
                book[price] = size

    @staticmethod
    def _levels_from_books(
        bid_book: Dict[float, float],
        ask_book: Dict[float, float],
        *,
        limit: int,
    ) -> List[Dict[str, Any]]:
        bid_rows = sorted(bid_book.items(), key=lambda item: item[0], reverse=True)[: max(1, int(limit or 1))]
        ask_rows = sorted(ask_book.items(), key=lambda item: item[0])[: max(1, int(limit or 1))]
        levels: List[Dict[str, Any]] = []
        for idx in range(max(len(bid_rows), len(ask_rows))):
            level: Dict[str, Any] = {}
            if idx < len(bid_rows):
                level["bid"] = float(bid_rows[idx][0])
                level["bid_size"] = float(bid_rows[idx][1])
            if idx < len(ask_rows):
                level["ask"] = float(ask_rows[idx][0])
                level["ask_size"] = float(ask_rows[idx][1])
            if level:
                levels.append(level)
        return levels

    @staticmethod
    def _timestamp_seconds(value: Any) -> Optional[float]:
        if value in (None, ""):
            return None
        if isinstance(value, datetime):
            return float(value.timestamp())
        try:
            numeric = float(value)
            if numeric > 10_000_000_000:
                numeric /= 1000.0
            return numeric
        except Exception:
            return None

    @staticmethod
    def _note_depth_sequence_gap(asset: str, *, reason: str) -> None:
        try:
            from services.dom_stream_health_service import get_service as get_dom_stream_health_service

            get_dom_stream_health_service().note_sequence_gap("binance", asset, reason=reason)
        except Exception:
            pass

    @staticmethod
    def _normalise_depth_levels(bids: List[Any], asks: List[Any]) -> List[Dict[str, Any]]:
        levels: List[Dict[str, Any]] = []
        for idx in range(max(len(bids), len(asks))):
            bid_row = bids[idx] if idx < len(bids) else None
            ask_row = asks[idx] if idx < len(asks) else None
            bid_px = _safe_float((bid_row or [None])[0] if bid_row else None)
            bid_sz = _safe_float((bid_row or [None, None])[1] if bid_row else None)
            ask_px = _safe_float((ask_row or [None])[0] if ask_row else None)
            ask_sz = _safe_float((ask_row or [None, None])[1] if ask_row else None)
            level: Dict[str, Any] = {}
            if bid_px > 0.0 and bid_sz > 0.0:
                level["bid"] = bid_px
                level["bid_size"] = bid_sz
            if ask_px > 0.0 and ask_sz > 0.0:
                level["ask"] = ask_px
                level["ask_size"] = ask_sz
            if level:
                levels.append(level)
        return levels

    @staticmethod
    def _top_side_levels(levels: List[Dict[str, Any]], *, side: str, limit: int) -> List[List[float]]:
        price_key = "bid" if side == "bid" else "ask"
        size_key = "bid_size" if side == "bid" else "ask_size"
        rows: List[List[float]] = []
        for level in levels[: max(1, int(limit or 1))]:
            price = _safe_float(level.get(price_key))
            size = _safe_float(level.get(size_key))
            if price > 0.0 and size > 0.0:
                rows.append([round(price, 8), round(size, 8)])
        return rows

    @staticmethod
    def _depth_quote_alignment(quote_price: Optional[float], depth_price: float) -> Tuple[float, str]:
        quote = _safe_float(quote_price)
        depth = _safe_float(depth_price)
        if quote <= 0.0 or depth <= 0.0:
            return 1.0, "aligned"
        diff_bps = abs(depth - quote) / max(quote, 1e-9) * 10000.0
        if diff_bps <= 2.0:
            return 1.0, "aligned"
        if diff_bps <= 6.0:
            return 0.90, "near"
        if diff_bps <= 15.0:
            return 0.72, "divergent"
        return 0.35, "severe_divergence"

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
                "depth_endpoint": _SPOT_DEPTH_ENDPOINT,
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
            "depth_endpoint": _FUTURES_DEPTH_ENDPOINT,
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
