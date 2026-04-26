from __future__ import annotations

import json
import os
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from ctrader_open_api import Client, TcpProtocol
from ctrader_open_api.auth import Auth
from ctrader_open_api.endpoints import EndPoints
from ctrader_open_api.messages.OpenApiMessages_pb2 import (
    ProtoOAAccountAuthReq,
    ProtoOAApplicationAuthReq,
    ProtoOAErrorRes,
    ProtoOAGetAccountListByAccessTokenReq,
    ProtoOASpotEvent,
    ProtoOASubscribeDepthQuotesReq,
    ProtoOASubscribeSpotsReq,
    ProtoOASymbolsListReq,
    ProtoOADepthEvent,
)
from twisted.internet import reactor


def _stderr(message: str) -> None:
    sys.stderr.write(str(message).rstrip() + "\n")
    sys.stderr.flush()


def _stdout(payload: Dict[str, Any]) -> None:
    sys.stdout.write(json.dumps(payload, ensure_ascii=True, separators=(",", ":")) + "\n")
    sys.stdout.flush()


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value if value not in (None, "") else default)
    except Exception:
        return default


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _normalize_name(value: str) -> str:
    return "".join(ch for ch in str(value or "").upper() if ch.isalnum())


_SUPPORTED_ASSETS: Dict[str, Dict[str, Any]] = {
    "EUR/USD": {"category": "forex", "aliases": ("EURUSD",)},
    "EUR/JPY": {"category": "forex", "aliases": ("EURJPY",)},
    "EUR/GBP": {"category": "forex", "aliases": ("EURGBP",)},
    "GBP/JPY": {"category": "forex", "aliases": ("GBPJPY",)},
    "GBP/USD": {"category": "forex", "aliases": ("GBPUSD",)},
    "AUD/USD": {"category": "forex", "aliases": ("AUDUSD",)},
    "NZD/USD": {"category": "forex", "aliases": ("NZDUSD",)},
    "USD/JPY": {"category": "forex", "aliases": ("USDJPY",)},
    "USD/CAD": {"category": "forex", "aliases": ("USDCAD",)},
    "USD/CHF": {"category": "forex", "aliases": ("USDCHF",)},
    "XAU/USD": {"category": "commodities", "aliases": ("XAUUSD", "GOLD")},
    "XAG/USD": {"category": "commodities", "aliases": ("XAGUSD", "SILVER")},
    "WTI": {"category": "commodities", "aliases": ("USOIL", "WTI", "CRUDE", "USCRUDE")},
    "US30": {"category": "indices", "aliases": ("US30", "DJ30", "WALLSTREET30")},
    "US100": {"category": "indices", "aliases": ("US100", "USTEC", "NAS100", "NASDAQ100")},
    "US500": {"category": "indices", "aliases": ("US500", "SPX500", "SP500")},
    "UK100": {"category": "indices", "aliases": ("UK100", "FTSE100")},
    "GER40": {"category": "indices", "aliases": ("GER40", "DE40", "DAX40")},
    "AUS200": {"category": "indices", "aliases": ("AUS200", "AU200")},
    "JPN225": {"category": "indices", "aliases": ("JPN225", "JP225", "JAP225", "NI225")},
}


def _selected_assets(raw: str) -> Tuple[str, ...]:
    values = [item.strip() for item in str(raw or "").split(",") if item.strip()]
    if not values:
        return tuple(_SUPPORTED_ASSETS.keys())
    result: List[str] = []
    for item in values:
        normalized = str(item or "").strip().upper()
        for asset in _SUPPORTED_ASSETS:
            if asset.upper() == normalized and asset not in result:
                result.append(asset)
                break
    return tuple(result or _SUPPORTED_ASSETS.keys())


class CTraderDepthBridge:
    def __init__(self) -> None:
        self.environment = (os.getenv("CTRADER_LIVE_DEPTH_ENVIRONMENT", "demo").strip().lower() or "demo")
        self.client_id = os.getenv("CTRADER_LIVE_DEPTH_CLIENT_ID", "").strip()
        self.client_secret = os.getenv("CTRADER_LIVE_DEPTH_CLIENT_SECRET", "").strip()
        self.access_token = os.getenv("CTRADER_LIVE_DEPTH_ACCESS_TOKEN", "").strip()
        self.refresh_token = os.getenv("CTRADER_LIVE_DEPTH_REFRESH_TOKEN", "").strip()
        self.account_hint = os.getenv("CTRADER_LIVE_DEPTH_ACCOUNT_ID", "").strip()
        self.redirect_uri = os.getenv("CTRADER_LIVE_DEPTH_REDIRECT_URI", "http://localhost").strip() or "http://localhost"
        self.assets = _selected_assets(os.getenv("CTRADER_LIVE_DEPTH_ASSETS", "").strip())
        self.min_emit_ms = max(50, int(os.getenv("CTRADER_LIVE_DEPTH_MIN_EMIT_MS", "150") or "150"))
        self.max_levels = max(1, int(os.getenv("CTRADER_LIVE_DEPTH_MAX_LEVELS", "20") or "20"))
        self.store_path = Path(os.getenv("CTRADER_LIVE_DEPTH_STORE_PATH", "data/ctrader_live_depth.json"))
        self.store_path.parent.mkdir(parents=True, exist_ok=True)
        self.token_cache_path = Path(os.getenv("CTRADER_LIVE_DEPTH_TOKEN_CACHE_PATH", "data/ctrader_tokens.json"))
        self.token_cache_path.parent.mkdir(parents=True, exist_ok=True)

        self.client: Optional[Client] = None
        self.account_id: Optional[int] = None
        self.symbol_to_asset: Dict[int, str] = {}
        self.asset_state: Dict[str, Dict[str, Any]] = {}
        self.last_emit = 0.0

        for asset in self.assets:
            self.asset_state[asset] = {
                "asset": asset,
                "category": _SUPPORTED_ASSETS[asset]["category"],
                "symbol_id": None,
                "symbol_name": "",
                "quotes": {},
                "bid": None,
                "ask": None,
                "price": 0.0,
                "timestamp": 0.0,
            }

    def run(self) -> int:
        if not self.client_id or not self.client_secret:
            _stderr("cTrader client credentials are missing. Set CTRADER_LIVE_DEPTH_CLIENT_ID and CTRADER_LIVE_DEPTH_CLIENT_SECRET.")
            return 2
        self._load_cached_tokens()
        if not self.access_token and self.refresh_token:
            self._refresh_access_token()
        if not self.access_token:
            auth = Auth(self.client_id, self.client_secret, self.redirect_uri)
            _stderr("Missing cTrader access token. Generate one via the Open API OAuth flow or Playground.")
            _stderr(f"Authorization URL: {auth.getAuthUri(scope='trading')}")
            return 2

        host = EndPoints.PROTOBUF_DEMO_HOST if self.environment == "demo" else EndPoints.PROTOBUF_LIVE_HOST
        self.client = Client(host, EndPoints.PROTOBUF_PORT, TcpProtocol)
        self.client.setConnectedCallback(self._on_connected)
        self.client.setDisconnectedCallback(self._on_disconnected)
        self.client.setMessageReceivedCallback(self._on_message)
        self.client.startService()
        reactor.run()
        return 0

    def _load_cached_tokens(self) -> None:
        if not self.token_cache_path.exists():
            return
        try:
            payload = json.loads(self.token_cache_path.read_text(encoding="utf-8"))
        except Exception:
            return
        if not self.access_token:
            self.access_token = str(payload.get("access_token") or "").strip()
        if not self.refresh_token:
            self.refresh_token = str(payload.get("refresh_token") or "").strip()

    def _persist_tokens(self) -> None:
        payload = {
            "updated_at": _now_iso(),
            "access_token": self.access_token,
            "refresh_token": self.refresh_token,
        }
        self.token_cache_path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")

    def _refresh_access_token(self) -> None:
        try:
            auth = Auth(self.client_id, self.client_secret, self.redirect_uri)
            response = auth.refreshToken(self.refresh_token)
        except Exception as exc:
            _stderr(f"Token refresh failed: {exc}")
            return
        token = str((response or {}).get("accessToken") or "").strip()
        refresh = str((response or {}).get("refreshToken") or self.refresh_token).strip()
        if token:
            self.access_token = token
        if refresh:
            self.refresh_token = refresh
        if self.access_token:
            self._persist_tokens()

    def _on_connected(self, client: Client) -> None:
        _stderr(f"Connected to cTrader {self.environment} endpoint.")
        req = ProtoOAApplicationAuthReq(clientId=self.client_id, clientSecret=self.client_secret)
        client.send(req).addCallbacks(self._after_app_auth, self._fatal)

    def _after_app_auth(self, _response: Any) -> None:
        assert self.client is not None
        req = ProtoOAGetAccountListByAccessTokenReq(accessToken=self.access_token)
        self.client.send(req).addCallbacks(self._after_account_list, self._fatal)

    def _after_account_list(self, response: Any) -> None:
        accounts = list(getattr(response, "ctidTraderAccount", []) or [])
        if not accounts:
            self._fatal(RuntimeError("No cTrader accounts were returned for the access token."))
            return

        selected = None
        if self.account_hint:
            for item in accounts:
                if str(getattr(item, "ctidTraderAccountId", "")) == self.account_hint or str(getattr(item, "traderLogin", "")) == self.account_hint:
                    selected = item
                    break
        if selected is None:
            want_live = self.environment == "live"
            for item in accounts:
                if bool(getattr(item, "isLive", False)) == want_live:
                    selected = item
                    break
        if selected is None:
            selected = accounts[0]

        self.account_id = int(getattr(selected, "ctidTraderAccountId"))
        login = str(getattr(selected, "traderLogin", "") or "")
        _stderr(f"Authorizing cTrader account {self.account_id}{f' (login {login})' if login else ''}.")

        assert self.client is not None
        req = ProtoOAAccountAuthReq(ctidTraderAccountId=self.account_id, accessToken=self.access_token)
        self.client.send(req).addCallbacks(self._after_account_auth, self._fatal)

    def _after_account_auth(self, _response: Any) -> None:
        assert self.client is not None and self.account_id is not None
        req = ProtoOASymbolsListReq(ctidTraderAccountId=self.account_id, includeArchivedSymbols=False)
        self.client.send(req).addCallbacks(self._after_symbols, self._fatal)

    def _after_symbols(self, response: Any) -> None:
        symbols = list(getattr(response, "symbol", []) or [])
        matched_symbol_ids: List[int] = []
        for item in symbols:
            raw_name = str(getattr(item, "symbolName", "") or "")
            normalized = _normalize_name(raw_name)
            description = _normalize_name(str(getattr(item, "description", "") or ""))
            for asset in self.assets:
                aliases = {_normalize_name(alias) for alias in _SUPPORTED_ASSETS[asset]["aliases"]}
                if normalized in aliases or description in aliases:
                    symbol_id = int(getattr(item, "symbolId"))
                    self.symbol_to_asset[symbol_id] = asset
                    state = self.asset_state[asset]
                    state["symbol_id"] = symbol_id
                    state["symbol_name"] = raw_name
                    matched_symbol_ids.append(symbol_id)
                    break

        matched_symbol_ids = sorted(set(matched_symbol_ids))
        if not matched_symbol_ids:
            self._fatal(RuntimeError("No configured assets could be mapped to cTrader symbols on this account."))
            return

        assert self.client is not None and self.account_id is not None
        self.client.send(
            ProtoOASubscribeSpotsReq(
                ctidTraderAccountId=self.account_id,
                symbolId=matched_symbol_ids,
                subscribeToSpotTimestamp=True,
            )
        ).addErrback(self._fatal)
        self.client.send(
            ProtoOASubscribeDepthQuotesReq(
                ctidTraderAccountId=self.account_id,
                symbolId=matched_symbol_ids,
            )
        ).addErrback(self._fatal)
        _stderr(f"Depth bridge running for {len(matched_symbol_ids)} matched assets.")

    def _on_disconnected(self, _client: Client, reason: Any) -> None:
        _stderr(f"Disconnected from cTrader: {reason}")

    def _on_message(self, _client: Client, message: Any) -> None:
        if isinstance(message, ProtoOAErrorRes):
            description = str(getattr(message, "description", "") or "")
            code = str(getattr(message, "errorCode", "") or "")
            _stderr(f"API error {code}: {description}")
            return
        if isinstance(message, ProtoOASpotEvent):
            self._handle_spot_event(message)
            return
        if isinstance(message, ProtoOADepthEvent):
            self._handle_depth_event(message)

    def _handle_spot_event(self, message: ProtoOASpotEvent) -> None:
        symbol_id = int(getattr(message, "symbolId", 0) or 0)
        asset = self.symbol_to_asset.get(symbol_id)
        if not asset:
            return
        state = self.asset_state[asset]
        if message.HasField("bid"):
            state["bid"] = float(message.bid) / 100000.0
        if message.HasField("ask"):
            state["ask"] = float(message.ask) / 100000.0
        if state["bid"] is not None and state["ask"] is not None:
            state["price"] = (float(state["bid"]) + float(state["ask"])) / 2.0
        elif state["bid"] is not None:
            state["price"] = float(state["bid"])
        elif state["ask"] is not None:
            state["price"] = float(state["ask"])
        state["timestamp"] = time.time()
        self._maybe_emit(asset)

    def _handle_depth_event(self, message: ProtoOADepthEvent) -> None:
        symbol_id = int(getattr(message, "symbolId", 0) or 0)
        asset = self.symbol_to_asset.get(symbol_id)
        if not asset:
            return
        state = self.asset_state[asset]
        quotes: Dict[int, Dict[str, float]] = state["quotes"]
        for quote in list(getattr(message, "newQuotes", []) or []):
            quote_id = int(getattr(quote, "id", 0) or 0)
            size = float(getattr(quote, "size", 0) or 0) / 100.0
            if quote.HasField("bid"):
                quotes[quote_id] = {"side": "bid", "price": float(quote.bid) / 100000.0, "size": size}
            elif quote.HasField("ask"):
                quotes[quote_id] = {"side": "ask", "price": float(quote.ask) / 100000.0, "size": size}
        for quote_id in list(getattr(message, "deletedQuotes", []) or []):
            quotes.pop(int(quote_id), None)
        state["timestamp"] = time.time()
        self._maybe_emit(asset, force=True)

    def _build_levels(self, asset: str) -> Tuple[List[Dict[str, float]], float, float]:
        state = self.asset_state[asset]
        bid_book: Dict[float, float] = defaultdict(float)
        ask_book: Dict[float, float] = defaultdict(float)
        for item in state["quotes"].values():
            side = str(item.get("side") or "")
            price = _safe_float(item.get("price"), 0.0)
            size = max(0.0, _safe_float(item.get("size"), 0.0))
            if price <= 0.0 or size <= 0.0:
                continue
            if side == "bid":
                bid_book[price] += size
            elif side == "ask":
                ask_book[price] += size

        bid_levels = sorted(bid_book.items(), key=lambda item: item[0], reverse=True)[: self.max_levels]
        ask_levels = sorted(ask_book.items(), key=lambda item: item[0])[: self.max_levels]
        total_bid = sum(size for _price, size in bid_levels)
        total_ask = sum(size for _price, size in ask_levels)
        levels: List[Dict[str, float]] = []
        rows = max(len(bid_levels), len(ask_levels))
        for idx in range(rows):
            bid = bid_levels[idx] if idx < len(bid_levels) else (None, None)
            ask = ask_levels[idx] if idx < len(ask_levels) else (None, None)
            levels.append(
                {
                    "bid": bid[0],
                    "ask": ask[0],
                    "bid_size": bid[1],
                    "ask_size": ask[1],
                }
            )
        return levels, total_bid, total_ask

    def _maybe_emit(self, asset: str, *, force: bool = False) -> None:
        now = time.time()
        if not force and (now - self.last_emit) * 1000.0 < self.min_emit_ms:
            return
        state = self.asset_state[asset]
        symbol_id = state.get("symbol_id")
        if not symbol_id:
            return
        levels, total_bid, total_ask = self._build_levels(asset)
        payload = {
            "asset": asset,
            "category": state["category"],
            "symbol_id": symbol_id,
            "symbol_name": state.get("symbol_name") or "",
            "bid": state.get("bid"),
            "ask": state.get("ask"),
            "price": state.get("price") or 0.0,
            "bid_size": levels[0]["bid_size"] if levels and levels[0].get("bid_size") is not None else None,
            "ask_size": levels[0]["ask_size"] if levels and levels[0].get("ask_size") is not None else None,
            "total_bid_volume": total_bid,
            "total_ask_volume": total_ask,
            "levels": levels,
            "as_of_utc": _now_iso(),
            "timestamp": now,
            "environment": self.environment,
            "broker": "IC Markets",
        }
        self._persist_store(payload)
        _stdout(payload)
        self.last_emit = now

    def _persist_store(self, payload: Dict[str, Any]) -> None:
        aggregate = {"updated_at": _now_iso(), "assets": {}}
        if self.store_path.exists():
            try:
                aggregate = json.loads(self.store_path.read_text(encoding="utf-8"))
            except Exception:
                aggregate = {"updated_at": _now_iso(), "assets": {}}
        assets = aggregate.get("assets") if isinstance(aggregate, dict) else {}
        if not isinstance(assets, dict):
            assets = {}
        assets[str(payload.get("asset") or "")] = payload
        aggregate = {"updated_at": _now_iso(), "assets": assets}
        tmp = self.store_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(aggregate, ensure_ascii=True, separators=(",", ":")), encoding="utf-8")
        tmp.replace(self.store_path)

    def _fatal(self, failure: Any) -> Any:
        _stderr(f"Fatal cTrader bridge error: {failure}")
        if reactor.running:
            reactor.callLater(0.1, reactor.stop)
        return failure


def main() -> int:
    return CTraderDepthBridge().run()


if __name__ == "__main__":
    raise SystemExit(main())
