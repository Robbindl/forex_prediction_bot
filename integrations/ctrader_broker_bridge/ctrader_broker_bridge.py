from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Type

from ctrader_open_api import Client, TcpProtocol
from ctrader_open_api.auth import Auth
from ctrader_open_api.endpoints import EndPoints
from ctrader_open_api.messages.OpenApiMessages_pb2 import (
    ProtoOAAccountAuthReq,
    ProtoOAApplicationAuthReq,
    ProtoOAApplicationAuthRes,
    ProtoOAAssetListReq,
    ProtoOAAssetListRes,
    ProtoOAErrorRes,
    ProtoOAExecutionEvent,
    ProtoOAGetAccountListByAccessTokenReq,
    ProtoOANewOrderReq,
    ProtoOAOrderErrorEvent,
    ProtoOAReconcileReq,
    ProtoOASpotEvent,
    ProtoOASubscribeSpotsReq,
    ProtoOASymbolsListReq,
    ProtoOATraderReq,
    ProtoOATraderRes,
    ProtoOAAmendPositionSLTPReq,
    ProtoOAClosePositionReq,
    ProtoOAGetAccountListByAccessTokenRes,
    ProtoOASymbolsListRes,
    ProtoOAReconcileRes,
    ProtoOASymbolByIdReq,
    ProtoOASymbolByIdRes,
)
from ctrader_open_api.messages.OpenApiModelMessages_pb2 import (
    ProtoOAExecutionType,
    ProtoOAOrderType,
    ProtoOATimeInForce,
    ProtoOATradeSide,
)
from twisted.internet import reactor


SUPPORTED_ASSETS: Dict[str, Dict[str, Any]] = {
    "EUR/USD": {"category": "forex", "aliases": ("EURUSD", "EUR/USD")},
    "EUR/JPY": {"category": "forex", "aliases": ("EURJPY", "EUR/JPY")},
    "EUR/GBP": {"category": "forex", "aliases": ("EURGBP", "EUR/GBP")},
    "GBP/JPY": {"category": "forex", "aliases": ("GBPJPY", "GBP/JPY")},
    "GBP/USD": {"category": "forex", "aliases": ("GBPUSD", "GBP/USD")},
    "AUD/USD": {"category": "forex", "aliases": ("AUDUSD", "AUD/USD")},
    "NZD/USD": {"category": "forex", "aliases": ("NZDUSD", "NZD/USD")},
    "USD/JPY": {"category": "forex", "aliases": ("USDJPY", "USD/JPY")},
    "USD/CAD": {"category": "forex", "aliases": ("USDCAD", "USD/CAD")},
    "USD/CHF": {"category": "forex", "aliases": ("USDCHF", "USD/CHF")},
    "BTC-USD": {"category": "crypto", "aliases": ("BTCUSD", "BTC/USD", "BITCOIN")},
    "ETH-USD": {"category": "crypto", "aliases": ("ETHUSD", "ETH/USD", "ETHEREUM")},
    "BNB-USD": {"category": "crypto", "aliases": ("BNBUSD", "BNB/USD", "BINANCECOIN")},
    "SOL-USD": {"category": "crypto", "aliases": ("SOLUSD", "SOL/USD", "SOLANA")},
    "XRP-USD": {"category": "crypto", "aliases": ("XRPUSD", "XRP/USD", "RIPPLE")},
    "XAU/USD": {"category": "commodities", "aliases": ("XAUUSD", "GOLD", "XAU/USD")},
    "XAG/USD": {"category": "commodities", "aliases": ("XAGUSD", "SILVER", "XAG/USD")},
    "WTI": {"category": "commodities", "aliases": ("USOIL", "WTI", "CRUDE", "USCRUDE", "SPOTCRUDE", "WTI Cash (or Spot) Contract")},
    "US30": {"category": "indices", "aliases": ("US30", "DJ30", "WALLSTREET30")},
    "US100": {"category": "indices", "aliases": ("US100", "USTEC", "NAS100", "NASDAQ100")},
    "US500": {"category": "indices", "aliases": ("US500", "SPX500", "SP500")},
    "UK100": {"category": "indices", "aliases": ("UK100", "FTSE100")},
    "GER40": {"category": "indices", "aliases": ("GER40", "DE40", "DAX40")},
    "AUS200": {"category": "indices", "aliases": ("AUS200", "AU200")},
    "JPN225": {"category": "indices", "aliases": ("JPN225", "JP225", "JAP225", "NI225")},
}

PEPPERSTONE_CRYPTO_ALT_BASES = {
    "BTC-USD": "BTC",
    "ETH-USD": "ETH",
}


def _stdout(payload: Dict[str, Any]) -> None:
    sys.stdout.write(json.dumps(payload, ensure_ascii=True, separators=(",", ":"), default=str) + "\n")
    sys.stdout.flush()


def _stderr(message: str) -> None:
    sys.stderr.write(str(message).rstrip() + "\n")
    sys.stderr.flush()


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value if value not in (None, "") else default)
    except Exception:
        return float(default)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value if value not in (None, "") else default))
    except Exception:
        return int(default)


def _normalize_name(value: Any) -> str:
    return "".join(ch for ch in str(value or "").upper() if ch.isalnum())


def _bool_env(name: str, default: bool = False) -> bool:
    text = str(os.getenv(name, "")).strip().lower()
    if not text:
        return bool(default)
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return bool(default)


def _broker_key(value: Any) -> str:
    return str(value or "").strip().lower().replace(" ", "").replace("_", "")


def _message_to_dict(message: Any) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    if message is None:
        return result
    for field in getattr(message, "DESCRIPTOR", object()).fields:
        name = str(field.name)
        try:
            if field.label == field.LABEL_REPEATED:
                result[name] = [_message_to_dict(item) if hasattr(item, "DESCRIPTOR") else item for item in getattr(message, name)]
            elif hasattr(getattr(message, name), "DESCRIPTOR"):
                result[name] = _message_to_dict(getattr(message, name))
            elif message.HasField(name):
                result[name] = getattr(message, name)
        except Exception:
            try:
                result[name] = getattr(message, name)
            except Exception:
                pass
    return result


class CTraderOneShot:
    def __init__(self, action: str, payload: Dict[str, Any]) -> None:
        self.action = str(action or "").strip()
        self.payload = dict(payload or {})
        self.environment = os.getenv("CTRADER_EXECUTION_ENVIRONMENT", "demo").strip().lower() or "demo"
        self.client_id = os.getenv("CTRADER_EXECUTION_CLIENT_ID", "").strip()
        self.client_secret = os.getenv("CTRADER_EXECUTION_CLIENT_SECRET", "").strip()
        self.access_token = os.getenv("CTRADER_EXECUTION_ACCESS_TOKEN", "").strip()
        self.refresh_token = os.getenv("CTRADER_EXECUTION_REFRESH_TOKEN", "").strip()
        self.account_hint = os.getenv("CTRADER_EXECUTION_ACCOUNT_ID", "").strip()
        self.redirect_uri = os.getenv("CTRADER_EXECUTION_REDIRECT_URI", "http://localhost").strip() or "http://localhost"
        self.token_cache_path = Path(
            os.getenv("CTRADER_EXECUTION_TOKEN_CACHE_PATH", "data/runtime_locks/ctrader_execution_tokens.json")
        )
        self.token_cache_path.parent.mkdir(parents=True, exist_ok=True)
        self.client: Optional[Client] = None
        self.account_id: Optional[int] = None
        self.assets_by_id: Dict[int, str] = {}
        self.symbols: List[Any] = []
        self.light_symbol_by_id: Dict[int, Any] = {}
        self.symbol_lookup: Dict[str, Tuple[int, str]] = {}
        self._pending_order: Dict[str, Any] = {}
        self._pending_specs: Dict[str, Any] = {}
        self._pending_conversions: Dict[str, Dict[str, Any]] = {}
        self._pending_conversion_by_symbol_id: Dict[int, str] = {}
        self._last_order_broker_sizing: Optional[Dict[str, Any]] = None
        self._finished = False
        self._stage = "init"
        self._order_request_sent = False

    def _mark_stage(self, stage: str) -> None:
        self._stage = str(stage or "").strip() or self._stage

    def run(self) -> int:
        if not self.client_id or not self.client_secret:
            self._finish(False, "cTrader execution credentials missing: set CTRADER_EXECUTION_CLIENT_ID and CTRADER_EXECUTION_CLIENT_SECRET")
            return 2
        self._mark_stage("load_tokens")
        self._load_cached_tokens()
        if not self.access_token and self.refresh_token:
            self._mark_stage("refresh_access_token")
            self._refresh_access_token()
        if not self.access_token:
            auth = Auth(self.client_id, self.client_secret, self.redirect_uri)
            self._finish(
                False,
                "cTrader execution access token missing",
                auth_url=auth.getAuthUri(scope="trading"),
            )
            return 2
        self._mark_stage("connect")
        host = EndPoints.PROTOBUF_DEMO_HOST if self.environment == "demo" else EndPoints.PROTOBUF_LIVE_HOST
        self.client = Client(host, EndPoints.PROTOBUF_PORT, TcpProtocol)
        self.client.setConnectedCallback(self._on_connected)
        self.client.setDisconnectedCallback(self._on_disconnected)
        self.client.setMessageReceivedCallback(self._on_message)
        self.client.startService()
        reactor.callLater(float(os.getenv("CTRADER_EXECUTION_BRIDGE_TIMEOUT_SECONDS", "25") or 25), self._timeout)
        reactor.run()
        return 0 if self._finished else 1

    def _load_cached_tokens(self) -> None:
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
            "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "access_token": self.access_token,
            "refresh_token": self.refresh_token,
        }
        self.token_cache_path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")

    def _refresh_access_token(self) -> None:
        try:
            response = Auth(self.client_id, self.client_secret, self.redirect_uri).refreshToken(self.refresh_token)
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

    def _finish(self, success: bool, message: str = "", **extra: Any) -> None:
        if self._finished:
            return
        self._finished = True
        payload = {"success": bool(success), "action": self.action}
        if message:
            payload["message" if success else "error"] = message
        payload.setdefault("stage", self._stage)
        payload.update(extra)
        _stdout(payload)
        try:
            if self.client is not None:
                self.client.stopService()
        except Exception:
            pass
        try:
            if reactor.running:
                reactor.callLater(0, reactor.stop)
        except Exception:
            pass

    def _timeout(self) -> None:
        if not self._finished:
            if self._order_request_sent:
                self._finish(
                    False,
                    f"ctrader_execution_unknown: cTrader execution bridge timed out at stage={self._stage} after an order request was sent; check broker before retrying",
                )
            else:
                self._finish(False, f"cTrader execution bridge timed out at stage={self._stage}")

    def _parse(self, response: Any, cls: Type[Any]) -> Any:
        if isinstance(response, cls):
            return response
        payload_type = int(getattr(response, "payloadType", 0) or 0)
        parsed = cls()
        payload = getattr(response, "payload", None)
        if payload:
            expected_type = int(getattr(parsed, "payloadType", 0) or 0)
            error_type = int(ProtoOAErrorRes().payloadType)
            if payload_type == error_type:
                error = ProtoOAErrorRes()
                error.ParseFromString(payload)
                code = str(getattr(error, "errorCode", "") or "ctrader_error")
                description = str(getattr(error, "description", "") or code)
                raise RuntimeError(f"{code}: {description}")
            if expected_type and payload_type and payload_type != expected_type:
                raise RuntimeError(f"unexpected cTrader payload type {payload_type}; expected {expected_type}")
            parsed.ParseFromString(payload)
            return parsed
        return response

    def _response_meta(self, response: Any) -> Dict[str, Any]:
        payload = getattr(response, "payload", None)
        return {
            "response_type": type(response).__name__,
            "payload_type": int(getattr(response, "payloadType", 0) or 0),
            "payload_bytes": len(payload or b""),
            "stage": self._stage,
        }

    def _finish_execution_unknown(self, message: str, *, response: Any = None, **extra: Any) -> None:
        payload = dict(extra)
        if response is not None:
            payload.setdefault("response_meta", self._response_meta(response))
        self._finish(
            False,
            f"ctrader_execution_unknown: {message}; check broker before retrying",
            **payload,
        )

    def _defer_place_order_finish_for_reconcile(self, *, position_id: int, finish_payload: Dict[str, Any]) -> None:
        if self.client is None or self.account_id is None:
            self._finish(True, "execution accepted", **finish_payload)
            return
        self._mark_stage("execution_reconcile_wait")
        reactor.callLater(1.0, self._request_place_order_reconcile, position_id, finish_payload)

    def _request_place_order_reconcile(self, position_id: int, finish_payload: Dict[str, Any]) -> None:
        if self._finished:
            return
        if self.client is None or self.account_id is None:
            self._finish(True, "execution accepted", **finish_payload)
            return
        self._mark_stage("execution_reconcile_request")
        self.client.send(ProtoOAReconcileReq(ctidTraderAccountId=self.account_id)).addCallbacks(
            lambda response: self._after_place_order_reconcile(response, position_id=position_id, finish_payload=finish_payload),
            lambda failure: self._finish_place_order_without_reconcile(failure, finish_payload),
        )

    def _finish_place_order_without_reconcile(self, failure: Any, finish_payload: Dict[str, Any]) -> None:
        payload = dict(finish_payload)
        payload["reconcile_error"] = str(getattr(failure, "value", failure))
        self._finish(True, "execution accepted; reconcile unavailable", **payload)

    def _after_place_order_reconcile(self, response: Any, *, position_id: int, finish_payload: Dict[str, Any]) -> None:
        self._mark_stage("execution_reconcile_response")
        payload = dict(finish_payload)
        try:
            parsed = self._parse(response, ProtoOAReconcileRes)
            matched = None
            for item in list(getattr(parsed, "position", []) or []):
                if int(getattr(item, "positionId", 0) or 0) == int(position_id):
                    matched = item
                    break
            if matched is not None:
                trade_data = getattr(matched, "tradeData", None)
                price = _safe_float(getattr(matched, "price", 0.0), 0.0)
                volume = _safe_int(getattr(trade_data, "volume", 0), 0)
                symbol_id = int(getattr(trade_data, "symbolId", 0) or 0)
                if price > 0:
                    payload["avg_price"] = price
                if volume > 0:
                    payload["volume"] = volume
                    broker_sizing = dict(payload.get("broker_sizing") or {})
                    if broker_sizing:
                        broker_sizing["filled_volume"] = volume
                        broker_sizing["filled_lots"] = broker_sizing.get("broker_size")
                        payload["broker_sizing"] = broker_sizing
                if symbol_id and not int(payload.get("symbol_id") or 0):
                    payload["symbol_id"] = symbol_id
                payload["raw"] = {
                    "execution_event": dict(finish_payload.get("raw") or {}),
                    "reconciled_position": _message_to_dict(matched),
                }
            else:
                payload["reconcile_warning"] = f"position {position_id} not found after execution accepted"
        except Exception as exc:
            payload["reconcile_error"] = str(exc)
        self._finish(True, "execution accepted", **payload)

    def _on_connected(self, client: Client) -> None:
        self._mark_stage("application_auth")
        req = ProtoOAApplicationAuthReq(clientId=self.client_id, clientSecret=self.client_secret)
        client.send(req).addCallbacks(self._after_app_auth, self._fatal)

    def _after_app_auth(self, _response: Any) -> None:
        self._mark_stage("application_auth_response")
        try:
            self._parse(_response, ProtoOAApplicationAuthRes)
        except Exception as exc:
            self._finish(False, str(exc))
            return
        assert self.client is not None
        self._mark_stage("account_list")
        req = ProtoOAGetAccountListByAccessTokenReq(accessToken=self.access_token)
        self.client.send(req).addCallbacks(self._after_account_list, self._fatal)

    def _after_account_list(self, response: Any) -> None:
        self._mark_stage("account_list_response")
        try:
            parsed = self._parse(response, ProtoOAGetAccountListByAccessTokenRes)
            accounts = list(getattr(parsed, "ctidTraderAccount", []) or [])
        except Exception as exc:
            message = str(exc)
            if ":" in message and "wire type" not in message.lower() and "end-group" not in message.lower():
                self._finish(False, message)
                return
            if self.account_hint:
                try:
                    self.account_id = int(float(self.account_hint))
                except Exception:
                    self._finish(False, f"cTrader account list parse failed and account hint is not numeric: {exc}")
                    return
                _stderr(
                    "cTrader account list parse failed; continuing with configured "
                    f"account hint {self.account_id}: {exc}"
                )
                assert self.client is not None
                self._mark_stage("account_auth")
                req = ProtoOAAccountAuthReq(ctidTraderAccountId=self.account_id, accessToken=self.access_token)
                self.client.send(req).addCallbacks(self._after_account_auth, self._fatal)
                return
            self._finish(False, f"cTrader account list parse failed and no account hint is configured: {exc}")
            return
        if not accounts:
            self._finish(False, "No cTrader accounts were returned for the execution token")
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
        assert self.client is not None
        self._mark_stage("account_auth")
        req = ProtoOAAccountAuthReq(ctidTraderAccountId=self.account_id, accessToken=self.access_token)
        self.client.send(req).addCallbacks(self._after_account_auth, self._fatal)

    def _after_account_auth(self, _response: Any) -> None:
        self._mark_stage("account_auth_response")
        assert self.client is not None and self.account_id is not None
        if self.action == "balance":
            self._send_balance()
            return
        if self.action == "list_positions":
            self._send_reconcile()
            return
        self._mark_stage("asset_list")
        self.client.send(ProtoOAAssetListReq(ctidTraderAccountId=self.account_id)).addCallbacks(self._after_assets, self._fatal)

    def _after_assets(self, response: Any) -> None:
        self._mark_stage("asset_list_response")
        try:
            parsed = self._parse(response, ProtoOAAssetListRes)
            self.assets_by_id = {
                int(getattr(item, "assetId", 0) or 0): str(getattr(item, "name", "") or getattr(item, "displayName", "") or "").upper()
                for item in list(getattr(parsed, "asset", []) or [])
                if int(getattr(item, "assetId", 0) or 0)
            }
        except Exception as exc:
            self.assets_by_id = {}
            _stderr(f"cTrader asset list parse failed; continuing without asset names: {exc}")
        assert self.client is not None and self.account_id is not None
        self._mark_stage("symbols_list")
        req = ProtoOASymbolsListReq(ctidTraderAccountId=self.account_id, includeArchivedSymbols=False)
        self.client.send(req).addCallbacks(self._after_symbols, self._fatal)

    def _after_symbols(self, response: Any) -> None:
        self._mark_stage("symbols_list_response")
        parsed = self._parse(response, ProtoOASymbolsListRes)
        self.symbols = list(getattr(parsed, "symbol", []) or [])
        self._build_symbol_lookup()
        if self.action == "balance":
            self._send_balance()
        elif self.action == "list_positions":
            self._send_reconcile()
        elif self.action == "preflight":
            self._send_preflight()
        elif self.action in {"place_order", "order_preflight"}:
            self._send_place_order()
        elif self.action in {"close_position", "partial_close"}:
            self._send_close_position()
        elif self.action == "update_stop":
            self._send_update_stop()
        else:
            self._finish(False, f"unsupported cTrader bridge action: {self.action}")

    def _build_symbol_lookup(self) -> None:
        lookup: Dict[str, Tuple[int, str]] = {}
        self.light_symbol_by_id = {}
        for item in self.symbols:
            symbol_id = int(getattr(item, "symbolId", 0) or 0)
            if symbol_id:
                self.light_symbol_by_id[symbol_id] = item
            raw_name = str(getattr(item, "symbolName", "") or "")
            description = str(getattr(item, "description", "") or "")
            names = {_normalize_name(raw_name), _normalize_name(description)}
            if raw_name:
                lookup.setdefault(str(raw_name).strip().upper(), (symbol_id, raw_name))
                lookup.setdefault(_normalize_name(raw_name), (symbol_id, raw_name))
            for asset, meta in SUPPORTED_ASSETS.items():
                aliases = {_normalize_name(alias) for alias in meta["aliases"]}
                if names & aliases:
                    lookup[asset] = (symbol_id, raw_name)
                    for alias in aliases:
                        lookup[alias] = (symbol_id, raw_name)
                    break
        self.symbol_lookup = lookup

    def _resolve_symbol(self, asset: Any, symbol: Any = "") -> Tuple[int, str]:
        keys = [
            str(asset or "").strip().upper(),
            _normalize_name(asset),
            _normalize_name(symbol),
            str(symbol or "").strip().upper(),
        ]
        for key in keys:
            if key and key in self.symbol_lookup:
                return self.symbol_lookup[key]
        raise RuntimeError(f"cTrader symbol not found for {asset or symbol}")

    def _canonical_supported_asset(self, asset: Any) -> str:
        direct = str(asset or "").strip().upper()
        if direct in SUPPORTED_ASSETS:
            return direct
        normalized = _normalize_name(asset)
        for key, meta in SUPPORTED_ASSETS.items():
            aliases = {key, *(meta.get("aliases") or ())}
            if normalized in {_normalize_name(item) for item in aliases}:
                return key
        return direct

    def _pepperstone_crypto_alt_quotes(self) -> List[str]:
        raw = (
            os.getenv("PEPPERSTONE_CTRADER_CRYPTO_ALT_QUOTES", "").strip()
            or os.getenv("PEPPERSTONE_CTRADER_LIVE_CRYPTO_ALT_QUOTES", "").strip()
            or "EUR,GBP,AUD"
        )
        return [item.strip().upper() for item in raw.replace(";", ",").split(",") if item.strip()]

    def _resolve_pepperstone_crypto_alt(self, asset: Any) -> Optional[Dict[str, Any]]:
        if _broker_key(os.getenv("CTRADER_EXECUTION_BROKER_NAME", "")) != "pepperstone":
            return None
        canonical = self._canonical_supported_asset(asset)
        base = PEPPERSTONE_CRYPTO_ALT_BASES.get(canonical)
        if not base:
            return None
        for quote in self._pepperstone_crypto_alt_quotes():
            symbol_name = f"{base}{quote}"
            key = _normalize_name(symbol_name)
            if key in self.symbol_lookup:
                symbol_id, resolved_name = self.symbol_lookup[key]
                return {
                    "requested_asset": canonical,
                    "symbol_id": symbol_id,
                    "symbol_name": resolved_name,
                    "broker_base": base,
                    "broker_quote": quote,
                    "signal_quote": "USD",
                    "reason": "pepperstone_crypto_alt_quote",
                }
        return None

    def _send_preflight(self) -> None:
        requested = self.payload.get("assets")
        if isinstance(requested, str):
            assets = [item.strip() for item in requested.replace(";", ",").split(",") if item.strip()]
        elif isinstance(requested, list):
            assets = [str(item).strip() for item in requested if str(item).strip()]
        else:
            assets = list(SUPPORTED_ASSETS)

        resolved: Dict[str, Dict[str, Any]] = {}
        missing: Dict[str, Any] = {}
        for asset in assets:
            try:
                symbol_id, symbol_name = self._resolve_symbol(asset)
                resolved[str(asset)] = {"symbol_id": symbol_id, "symbol_name": symbol_name}
            except Exception as exc:
                missing[str(asset)] = {"error": str(exc), "suggestions": self._suggest_symbols(asset)}

        benchmark: Dict[str, Any] = {}
        if self._pepperstone_gold_parity_enabled():
            try:
                symbol_id, symbol_name = self._resolve_symbol("XAU/USD", "XAUUSD")
                benchmark = {"asset": "XAU/USD", "symbol_id": symbol_id, "symbol_name": symbol_name}
            except Exception as exc:
                missing["XAU/USD benchmark"] = {"error": str(exc), "suggestions": self._suggest_symbols("XAU/USD")}

        self._finish(
            not missing,
            "preflight passed" if not missing else "preflight failed",
            account_id=str(self.account_id or ""),
            environment=self.environment,
            broker_name=os.getenv("CTRADER_EXECUTION_BROKER_NAME", "pepperstone"),
            symbol_count=len(self.symbols),
            resolved=resolved,
            missing=missing,
            pepperstone_gold_parity_enabled=self._pepperstone_gold_parity_enabled(),
            benchmark=benchmark,
        )

    def _suggest_symbols(self, asset: Any) -> List[Dict[str, Any]]:
        canonical = str(asset or "").strip().upper()
        meta = SUPPORTED_ASSETS.get(canonical, {})
        terms = {_normalize_name(canonical)}
        terms.update(_normalize_name(alias) for alias in meta.get("aliases", ()))
        if canonical == "WTI":
            terms.update({"OIL", "CRUDE", "USOIL", "XTI", "XTIUSD", "CL"})
        terms = {term for term in terms if term}
        suggestions: List[Dict[str, Any]] = []
        for item in self.symbols:
            symbol_id = int(getattr(item, "symbolId", 0) or 0)
            raw_name = str(getattr(item, "symbolName", "") or "")
            description = str(getattr(item, "description", "") or "")
            haystack = f"{_normalize_name(raw_name)} {_normalize_name(description)}"
            if any(term in haystack for term in terms):
                suggestions.append({"symbol_id": symbol_id, "symbol_name": raw_name, "description": description})
            if len(suggestions) >= 25:
                break
        return suggestions

    def _pepperstone_gold_parity_enabled(self) -> bool:
        if self.environment not in {"demo", "live"}:
            return False
        if _broker_key(os.getenv("CTRADER_EXECUTION_BROKER_NAME", "")) != "pepperstone":
            return False
        value = os.getenv("PEPPERSTONE_CTRADER_GOLD_PIP_PARITY_SIZING", "").strip()
        if value:
            return _bool_env("PEPPERSTONE_CTRADER_GOLD_PIP_PARITY_SIZING", True)
        return _bool_env("PEPPERSTONE_CTRADER_LIVE_GOLD_PIP_PARITY_SIZING", True)

    def _symbol_asset_codes(self, symbol_id: int) -> Tuple[str, str]:
        light = self.light_symbol_by_id.get(int(symbol_id or 0))
        if light is None:
            return "", ""
        base = self.assets_by_id.get(int(getattr(light, "baseAssetId", 0) or 0), "")
        quote = self.assets_by_id.get(int(getattr(light, "quoteAssetId", 0) or 0), "")
        return str(base or "").upper(), str(quote or "").upper()

    def _find_symbol_by_asset_codes(self, base: str, quote: str) -> Tuple[int, str]:
        base = str(base or "").upper()
        quote = str(quote or "").upper()
        for item in self.symbols:
            symbol_id = int(getattr(item, "symbolId", 0) or 0)
            item_base, item_quote = self._symbol_asset_codes(symbol_id)
            if item_base == base and item_quote == quote:
                return symbol_id, str(getattr(item, "symbolName", "") or "")
        raise RuntimeError(f"cTrader conversion symbol not found for {base}/{quote}")

    @staticmethod
    def _symbol_pip_size(symbol: Any) -> float:
        pip_position = int(getattr(symbol, "pipPosition", 0) or 0)
        if pip_position < 0:
            return 0.0
        return 10.0 ** (-pip_position)

    @staticmethod
    def _symbol_lot_size_cents(symbol: Any) -> int:
        return int(getattr(symbol, "lotSize", 0) or 0)

    @staticmethod
    def _symbol_price_digits(symbol: Any) -> int:
        return max(0, int(getattr(symbol, "digits", 0) or 0))

    @staticmethod
    def _category_for_asset(asset: Any) -> str:
        canonical = str(asset or "").strip().upper()
        direct = SUPPORTED_ASSETS.get(canonical)
        if direct:
            return str(direct.get("category") or "").lower()
        normalized = _normalize_name(canonical)
        for key, meta in SUPPORTED_ASSETS.items():
            aliases = {key, *(meta.get("aliases") or ())}
            if normalized in {_normalize_name(item) for item in aliases}:
                return str(meta.get("category") or "").lower()
        return ""

    def _max_lots_cap_for_asset(self, asset: Any) -> Tuple[float, str]:
        base_text = os.getenv("PEPPERSTONE_CTRADER_MAX_LOTS", "").strip() or os.getenv("PEPPERSTONE_CTRADER_LIVE_MAX_LOTS", "1.00")
        base_cap = max(0.01, _safe_float(base_text, 1.0))
        category = self._category_for_asset(asset)
        category_key = category.upper()
        category_text = ""
        if category_key:
            category_text = (
                os.getenv(f"PEPPERSTONE_CTRADER_MAX_LOTS_{category_key}", "").strip()
                or os.getenv(f"PEPPERSTONE_CTRADER_LIVE_MAX_LOTS_{category_key}", "").strip()
            )
        if category_text:
            return max(0.01, _safe_float(category_text, base_cap)), f"{category}_env_cap"
        if category == "crypto":
            return min(base_cap, 0.01), "crypto_default_cap"
        return base_cap, "global_cap"

    @staticmethod
    def _round_price_to_digits(price: Any, digits: int) -> float:
        value = _safe_float(price, 0.0)
        if value <= 0.0:
            return 0.0
        digits = max(0, int(digits or 0))
        return float(f"{value:.{digits}f}")

    def _order_price_precision(self, symbol: Any, *, price_factor: float = 1.0, price_conversion: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        digits = self._symbol_price_digits(symbol)
        price_factor = float(price_factor or 1.0)
        precision: Dict[str, Any] = {"digits": digits}
        if price_conversion:
            precision["price_conversion"] = dict(price_conversion)
        for key in ("entry_price", "stop_loss", "take_profit"):
            raw = _safe_float(self.payload.get(key), 0.0)
            converted = raw * price_factor if raw > 0.0 else 0.0
            rounded = self._round_price_to_digits(converted, digits)
            precision[f"raw_{key}"] = raw
            if price_factor != 1.0:
                precision[f"converted_{key}"] = converted
            precision[key] = rounded
            precision[f"{key}_changed"] = bool(raw > 0.0 and rounded != raw)
        return precision

    @staticmethod
    def _symbol_volume_to_lots(symbol: Any, volume: int) -> float:
        lot_size = CTraderOneShot._symbol_lot_size_cents(symbol)
        if lot_size <= 0:
            return 0.0
        return round(float(volume or 0) / float(lot_size), 6)

    @staticmethod
    def _price_from_spot(symbol: Any, bid: Any, ask: Any) -> float:
        digits = int(getattr(symbol, "digits", 0) or 0)
        divisor = 10 ** max(0, digits)
        bid_value = _safe_float(bid, 0.0) / divisor
        ask_value = _safe_float(ask, 0.0) / divisor
        if bid_value > 0.0 and ask_value > 0.0:
            return (bid_value + ask_value) / 2.0
        return bid_value or ask_value

    @staticmethod
    def _snap_volume(symbol: Any, desired_lots: float, *, max_lots_cap: float = 1.0) -> Tuple[int, float]:
        lot_size = CTraderOneShot._symbol_lot_size_cents(symbol)
        if lot_size <= 0:
            raise RuntimeError("cTrader symbol lotSize missing")
        min_volume = int(getattr(symbol, "minVolume", 0) or 0)
        max_volume = int(getattr(symbol, "maxVolume", 0) or 0)
        step_volume = int(getattr(symbol, "stepVolume", 0) or 0)
        max_lots_cap = max(0.01, float(max_lots_cap or 1.0))
        raw_min_lots = min_volume / lot_size if min_volume > 0 else 0.0
        raw_max_lots = max_volume / lot_size if max_volume > 0 else 0.0
        if raw_min_lots > max_lots_cap * 1000:
            raise RuntimeError(
                f"cTrader symbol minimum volume {raw_min_lots:.2f} lots exceeds configured max cap {max_lots_cap:.2f}; "
                "symbol is not tradable at the current risk cap"
            )
        if raw_max_lots > max_lots_cap * 1000:
            max_volume = 0
            raw_max_lots = 0.0
        min_lots = max(0.01, raw_min_lots if raw_min_lots > 0 else 0.01)
        max_lots = min(max_lots_cap, raw_max_lots if raw_max_lots > 0 else max_lots_cap)
        if max_lots < min_lots:
            max_lots = min_lots
        lots = max(min_lots, min(max_lots, float(desired_lots or 0.0)))
        lots = round(round(lots / 0.01) * 0.01, 2)
        volume = int(round(lots * lot_size))
        if step_volume > 0:
            volume = int(round(volume / step_volume) * step_volume)
        if min_volume > 0:
            volume = max(volume, min_volume)
        if max_volume > 0:
            volume = min(volume, max_volume)
        volume = max(1, volume)
        return volume, CTraderOneShot._symbol_volume_to_lots(symbol, volume)

    def _pip_value_usd_at_001_lots(self, symbol: Any, symbol_id: int) -> Tuple[float, Dict[str, Any]]:
        lot_size = self._symbol_lot_size_cents(symbol)
        pip_size = self._symbol_pip_size(symbol)
        if lot_size <= 0 or pip_size <= 0:
            raise RuntimeError("cTrader symbol lotSize or pipPosition missing")
        _base, quote = self._symbol_asset_codes(symbol_id)
        quote = quote or "USD"
        conversion = 1.0
        conversion_source = "quote_is_usd"
        if quote != "USD":
            conversion_meta = self._pending_conversions.get(quote) or {}
            conversion = float(conversion_meta.get("rate") or 0.0)
            conversion_source = str(conversion_meta.get("symbol_name") or "")
            if conversion <= 0.0:
                raise RuntimeError(f"live USD conversion missing for {quote}")
        units_at_001_lots = (float(lot_size) * 0.01) / 100.0
        pip_value_quote = units_at_001_lots * pip_size
        return pip_value_quote * conversion, {
            "quote_asset": quote,
            "pip_size": pip_size,
            "lot_size_cents": lot_size,
            "units_at_0_01_lots": units_at_001_lots,
            "quote_to_usd": conversion,
            "conversion_source": conversion_source,
        }

    def _request_symbol_detail(self, label: str, symbol_id: int) -> None:
        assert self.client is not None and self.account_id is not None
        self._mark_stage(f"symbol_detail:{label}")
        self.client.send(ProtoOASymbolByIdReq(ctidTraderAccountId=self.account_id, symbolId=[int(symbol_id)])).addCallbacks(
            lambda response: self._after_symbol_detail(response, label=label, symbol_id=symbol_id),
            self._fatal,
        )

    def _after_symbol_detail(self, response: Any, *, label: str, symbol_id: int) -> None:
        parsed = self._parse(response, ProtoOASymbolByIdRes)
        symbols = list(getattr(parsed, "symbol", []) or [])
        if not symbols:
            self._finish(False, f"cTrader symbol spec missing for symbolId={symbol_id}")
            return
        self._pending_specs[label] = symbols[0]
        if label == "update_stop":
            position_id = _safe_int(self._pending_order.get("position_id"), 0)
            raw_stop_loss = _safe_float(self._pending_order.get("stop_loss"), 0.0)
            digits = self._symbol_price_digits(symbols[0])
            stop_loss = self._round_price_to_digits(raw_stop_loss, digits)
            self._last_order_broker_sizing = {
                "price_precision": {
                    "digits": digits,
                    "raw_stop_loss": raw_stop_loss,
                    "stop_loss": stop_loss,
                    "stop_loss_changed": bool(raw_stop_loss > 0.0 and stop_loss != raw_stop_loss),
                }
            }
            self._submit_stop_update(position_id, stop_loss)
            return
        if label == "direct_order":
            volume = max(1, _safe_int(self._pending_order.get("volume"), 0))
            price_precision = self._order_price_precision(symbols[0])
            broker_sizing = {
                "broker": "ctrader",
                "broker_name": os.getenv("CTRADER_EXECUTION_BROKER_NAME", "pepperstone"),
                "environment": self.environment,
                "broker_volume": volume,
                "local_position_size": round(volume / 100.0, 8),
                "sizing_model": "direct_payload_volume",
                "price_precision": price_precision,
            }
            if self.action == "order_preflight":
                self._finish(
                    True,
                    "order preflight passed",
                    account_id=str(self.account_id or ""),
                    environment=self.environment,
                    broker_name=os.getenv("CTRADER_EXECUTION_BROKER_NAME", "pepperstone"),
                    symbol_id=symbol_id,
                    symbol_name=str(self._pending_order.get("symbol_name") or ""),
                    volume=volume,
                    broker_sizing=broker_sizing,
                    price_precision=price_precision,
                    would_submit_order=False,
                )
                return
            self._submit_market_order(
                symbol_id,
                str(self._pending_order.get("symbol_name") or ""),
                volume,
                broker_sizing=broker_sizing,
                symbol_spec=symbols[0],
            )
            return
        if label == "target":
            self._request_symbol_detail("gold", int(self._pending_order.get("gold_symbol_id") or 0))
            return
        if label == "gold":
            try:
                self._prepare_pepperstone_conversion_rates()
            except Exception as exc:
                self._finish(False, f"Pepperstone cTrader lot sizing failed: {exc}")
            return
        if label.startswith("conversion:"):
            quote = label.split(":", 1)[1]
            if quote in self._pending_conversions:
                self._pending_conversions[quote]["symbol"] = symbols[0]
            self._maybe_subscribe_conversion_spots()

    def _prepare_pepperstone_conversion_rates(self) -> None:
        self._pending_conversions = {}
        for label, symbol_id in (("target", self._pending_order.get("symbol_id")), ("gold", self._pending_order.get("gold_symbol_id"))):
            _base, quote = self._symbol_asset_codes(int(symbol_id or 0))
            quote = str(quote or "USD").upper()
            if quote and quote != "USD" and quote not in self._pending_conversions:
                try:
                    conv_symbol_id, conv_name = self._find_symbol_by_asset_codes(quote, "USD")
                    inverse = False
                except Exception:
                    conv_symbol_id, conv_name = self._find_symbol_by_asset_codes("USD", quote)
                    inverse = True
                self._pending_conversions[quote] = {
                    "symbol_id": conv_symbol_id,
                    "symbol_name": conv_name,
                    "inverse": inverse,
                    "rate": 0.0,
                }
        if not self._pending_conversions:
            try:
                self._submit_pepperstone_gold_parity_order()
            except Exception as exc:
                self._finish(False, f"Pepperstone cTrader lot sizing failed: {exc}")
            return
        for quote, meta in list(self._pending_conversions.items()):
            self._request_symbol_detail(f"conversion:{quote}", int(meta["symbol_id"]))

    def _maybe_subscribe_conversion_spots(self) -> None:
        if not self._pending_conversions:
            return
        if any(not meta.get("symbol") for meta in self._pending_conversions.values()):
            return
        self._pending_conversion_by_symbol_id = {
            int(meta["symbol_id"]): quote for quote, meta in self._pending_conversions.items()
        }
        assert self.client is not None and self.account_id is not None
        self._mark_stage("conversion_spot_subscribe")
        self.client.send(
            ProtoOASubscribeSpotsReq(
                ctidTraderAccountId=self.account_id,
                symbolId=list(self._pending_conversion_by_symbol_id.keys()),
                subscribeToSpotTimestamp=True,
            )
        ).addCallbacks(lambda _response: None, self._fatal)

    def _handle_spot_event(self, event: ProtoOASpotEvent) -> None:
        if not self._pending_conversion_by_symbol_id:
            return
        symbol_id = int(getattr(event, "symbolId", 0) or 0)
        quote = self._pending_conversion_by_symbol_id.get(symbol_id)
        if not quote:
            return
        meta = self._pending_conversions.get(quote) or {}
        symbol = meta.get("symbol")
        price = self._price_from_spot(symbol, getattr(event, "bid", 0), getattr(event, "ask", 0))
        if price <= 0.0:
            return
        meta["rate"] = (1.0 / price) if meta.get("inverse") else price
        self._pending_conversions[quote] = meta
        if all(float(item.get("rate") or 0.0) > 0.0 for item in self._pending_conversions.values()):
            self._pending_conversion_by_symbol_id = {}
            try:
                self._submit_pepperstone_gold_parity_order()
            except Exception as exc:
                self._finish(False, f"Pepperstone cTrader lot sizing failed: {exc}")

    def _order_price_factor(self) -> Tuple[float, Dict[str, Any]]:
        broker_quote = str(self._pending_order.get("broker_quote") or "").upper()
        signal_quote = str(self._pending_order.get("signal_quote") or "").upper()
        if not broker_quote or not signal_quote or broker_quote == signal_quote:
            return 1.0, {}
        if signal_quote != "USD":
            raise RuntimeError(f"unsupported cTrader signal quote conversion {signal_quote}->{broker_quote}")
        meta = self._pending_conversions.get(broker_quote) or {}
        quote_to_usd = float(meta.get("rate") or 0.0)
        if quote_to_usd <= 0.0:
            raise RuntimeError(f"live USD conversion missing for broker quote {broker_quote}")
        signal_to_broker = 1.0 / quote_to_usd
        return signal_to_broker, {
            "signal_quote": signal_quote,
            "broker_quote": broker_quote,
            "signal_to_broker_rate": signal_to_broker,
            "broker_to_signal_rate": quote_to_usd,
            "conversion_symbol": str(meta.get("symbol_name") or ""),
            "conversion_source": "live_ctrader_spot",
        }

    def _submit_pepperstone_gold_parity_order(self) -> None:
        target_symbol = self._pending_specs.get("target")
        gold_symbol = self._pending_specs.get("gold")
        if target_symbol is None or gold_symbol is None:
            self._finish(False, "Pepperstone cTrader lot sizing specs incomplete")
            return
        asset = str(self._pending_order.get("asset") or "")
        target_symbol_id = int(self._pending_order.get("symbol_id") or 0)
        gold_symbol_id = int(self._pending_order.get("gold_symbol_id") or 0)
        gold_pip_usd, gold_profile = self._pip_value_usd_at_001_lots(gold_symbol, gold_symbol_id)
        target_pip_usd, target_profile = self._pip_value_usd_at_001_lots(target_symbol, target_symbol_id)
        if gold_pip_usd <= 0.0 or target_pip_usd <= 0.0:
            self._finish(False, "Pepperstone cTrader pip value calculation failed")
            return
        if _normalize_name(asset) in {"XAUUSD", "GOLD"}:
            desired_lots = 0.01
            reason = "gold benchmark fixed 0.01"
        else:
            desired_lots = 0.01 * (gold_pip_usd / target_pip_usd)
            reason = "gold pip parity"
            if desired_lots <= 0.01:
                desired_lots = 0.01
                reason = "volatility cap 0.01"
        max_lots_cap, max_lots_source = self._max_lots_cap_for_asset(asset)
        desired_lots = min(max_lots_cap, max(0.01, float(desired_lots or 0.01)))
        volume, broker_lots = self._snap_volume(target_symbol, desired_lots, max_lots_cap=max_lots_cap)
        target_lot_size = max(1, self._symbol_lot_size_cents(target_symbol))
        raw_min_lots = int(getattr(target_symbol, "minVolume", 0) or 0) / target_lot_size
        raw_max_lots = int(getattr(target_symbol, "maxVolume", 0) or 0) / target_lot_size
        if raw_max_lots > max_lots_cap * 1000:
            raw_max_lots = 0.0
        display_min_lots = max(0.01, raw_min_lots if raw_min_lots > 0 else 0.01)
        display_max_lots = min(max_lots_cap, raw_max_lots if raw_max_lots > 0 else max_lots_cap)
        if display_max_lots < display_min_lots:
            display_max_lots = display_min_lots
        sizing = {
            "sizing_model": "pepperstone_ctrader_gold_0_01_pip_parity",
            "broker": "ctrader",
            "broker_name": "pepperstone",
            "environment": self.environment,
            "broker_size": broker_lots,
            "broker_volume": volume,
            "local_position_size": round(volume / 100.0, 8),
            "gold_pip_value_usd_at_0_01_lots": round(gold_pip_usd, 8),
            "target_pip_value_usd_at_0_01_lots": round(target_pip_usd, 8),
            "raw_calculated_lots": round(0.01 * (gold_pip_usd / target_pip_usd), 8),
            "rounded_lots": broker_lots,
            "min_lots": round(display_min_lots, 6),
            "max_lots": round(display_max_lots, 6),
            "max_lots_cap": max_lots_cap,
            "max_lots_source": max_lots_source,
            "reason": reason,
            "gold_profile": gold_profile,
            "target_profile": target_profile,
        }
        price_factor, price_conversion = self._order_price_factor()
        if price_conversion:
            sizing["execution_symbol_override"] = {
                "requested_asset": str(self._pending_order.get("requested_asset") or asset),
                "broker_symbol": str(self._pending_order.get("symbol_name") or ""),
                "broker_quote": str(self._pending_order.get("broker_quote") or ""),
                "reason": str(self._pending_order.get("symbol_override_reason") or ""),
            }
            sizing["price_conversion"] = price_conversion
        price_precision = self._order_price_precision(target_symbol, price_factor=price_factor, price_conversion=price_conversion)
        sizing["price_precision"] = price_precision
        if self.action == "order_preflight":
            self._finish(
                True,
                "order preflight passed",
                account_id=str(self.account_id or ""),
                environment=self.environment,
                broker_name=os.getenv("CTRADER_EXECUTION_BROKER_NAME", "pepperstone"),
                symbol_id=target_symbol_id,
                symbol_name=str(self._pending_order.get("symbol_name") or ""),
                volume=volume,
                broker_sizing=sizing,
                price_precision=price_precision,
                would_submit_order=False,
            )
            return
        self._submit_market_order(
            target_symbol_id,
            str(self._pending_order.get("symbol_name") or ""),
            volume,
            broker_sizing=sizing,
            symbol_spec=target_symbol,
            price_factor=price_factor,
            price_conversion=price_conversion,
        )

    def _send_balance(self) -> None:
        assert self.client is not None and self.account_id is not None
        self._mark_stage("balance_request")
        self.client.send(ProtoOATraderReq(ctidTraderAccountId=self.account_id)).addCallbacks(self._after_balance, self._fatal)

    def _after_balance(self, response: Any) -> None:
        self._mark_stage("balance_response")
        parsed = self._parse(response, ProtoOATraderRes)
        trader = getattr(parsed, "trader", None)
        money_digits = int(getattr(trader, "moneyDigits", 2) or 2)
        divisor = 10 ** max(0, money_digits)
        balance = float(getattr(trader, "balance", 0) or 0) / divisor if trader is not None else 0.0
        self._finish(
            True,
            "balance fetched",
            balance=round(balance, 2),
            currency="USD",
            account_id=str(self.account_id or ""),
            broker_name=str(getattr(trader, "brokerName", "") or os.getenv("CTRADER_EXECUTION_BROKER_NAME", "pepperstone")),
            environment=self.environment,
        )

    def _send_reconcile(self) -> None:
        assert self.client is not None and self.account_id is not None
        self._mark_stage("reconcile_request")
        self.client.send(ProtoOAReconcileReq(ctidTraderAccountId=self.account_id)).addCallbacks(self._after_reconcile, self._fatal)

    def _after_reconcile(self, response: Any) -> None:
        self._mark_stage("reconcile_response")
        parsed = self._parse(response, ProtoOAReconcileRes)
        positions = [_message_to_dict(item) for item in list(getattr(parsed, "position", []) or [])]
        self._finish(True, "positions fetched", positions=positions, account_id=str(self.account_id or ""), environment=self.environment)

    def _send_place_order(self) -> None:
        assert self.client is not None and self.account_id is not None
        self._mark_stage("place_order_prepare")
        asset = self.payload.get("asset") or self.payload.get("symbol") or ""
        symbol_id, symbol_name = self._resolve_symbol(asset, self.payload.get("symbol"))
        symbol_override = self._resolve_pepperstone_crypto_alt(asset)
        if symbol_override:
            symbol_id = int(symbol_override["symbol_id"])
            symbol_name = str(symbol_override["symbol_name"])
        if self._pepperstone_gold_parity_enabled():
            try:
                gold_symbol_id, gold_symbol_name = self._resolve_symbol("XAU/USD", "XAUUSD")
            except Exception as exc:
                self._finish(False, f"Pepperstone cTrader lot sizing requires XAU/USD benchmark spec: {exc}")
                return
            self._pending_order = {
                "asset": asset,
                "symbol_id": symbol_id,
                "symbol_name": symbol_name,
                "gold_symbol_id": gold_symbol_id,
                "gold_symbol_name": gold_symbol_name,
            }
            if symbol_override:
                self._pending_order.update(
                    {
                        "requested_asset": symbol_override["requested_asset"],
                        "broker_base": symbol_override["broker_base"],
                        "broker_quote": symbol_override["broker_quote"],
                        "signal_quote": symbol_override["signal_quote"],
                        "symbol_override_reason": symbol_override["reason"],
                    }
                )
            self._pending_specs = {}
            self._request_symbol_detail("target", symbol_id)
            return
        volume = max(1, _safe_int(self.payload.get("volume"), 0))
        self._pending_order = {
            "asset": asset,
            "symbol_id": symbol_id,
            "symbol_name": symbol_name,
            "volume": volume,
        }
        self._pending_specs = {}
        self._request_symbol_detail("direct_order", symbol_id)

    def _submit_market_order(
        self,
        symbol_id: int,
        symbol_name: str,
        volume: int,
        *,
        broker_sizing: Optional[Dict[str, Any]] = None,
        symbol_spec: Any = None,
        price_factor: float = 1.0,
        price_conversion: Optional[Dict[str, Any]] = None,
    ) -> None:
        assert self.client is not None and self.account_id is not None
        side = str(self.payload.get("side") or "BUY").upper()
        volume = max(1, int(volume or 0))
        self._last_order_broker_sizing = dict(broker_sizing or {})
        req_kwargs = {
            "ctidTraderAccountId": self.account_id,
            "symbolId": symbol_id,
            "orderType": ProtoOAOrderType.MARKET,
            "tradeSide": ProtoOATradeSide.SELL if side == "SELL" else ProtoOATradeSide.BUY,
            "volume": volume,
            "timeInForce": ProtoOATimeInForce.IMMEDIATE_OR_CANCEL,
            "clientOrderId": str(self.payload.get("client_order_id") or "")[:64],
            "label": "forex_prediction_bot",
            "comment": str(self.payload.get("reason") or "bot execution")[:120],
        }
        stop_loss = _safe_float(self.payload.get("stop_loss"), 0.0)
        take_profit = _safe_float(self.payload.get("take_profit"), 0.0)
        if symbol_spec is not None:
            price_precision = self._order_price_precision(symbol_spec, price_factor=price_factor, price_conversion=price_conversion)
            stop_loss = float(price_precision.get("stop_loss") or 0.0)
            take_profit = float(price_precision.get("take_profit") or 0.0)
            self._last_order_broker_sizing["price_precision"] = price_precision
            if price_conversion:
                self._last_order_broker_sizing["price_conversion"] = dict(price_conversion)
        if stop_loss > 0:
            req_kwargs["stopLoss"] = stop_loss
        if take_profit > 0:
            req_kwargs["takeProfit"] = take_profit
        self._mark_stage("order_submitted")
        self._order_request_sent = True
        self.client.send(ProtoOANewOrderReq(**req_kwargs)).addCallbacks(
            lambda response: self._after_execution_event(response, symbol_id=symbol_id, symbol_name=symbol_name),
            self._fatal,
        )

    def _send_close_position(self) -> None:
        assert self.client is not None and self.account_id is not None
        self._mark_stage("close_position_submit")
        self._last_order_broker_sizing = None
        position_id = _safe_int(self.payload.get("position_id") or self.payload.get("broker_trade_id") or self.payload.get("trade_id"), 0)
        volume = max(1, _safe_int(self.payload.get("volume"), 0))
        if position_id <= 0:
            self._finish(False, "cTrader position id missing for close")
            return
        self._order_request_sent = True
        self.client.send(
            ProtoOAClosePositionReq(ctidTraderAccountId=self.account_id, positionId=position_id, volume=volume)
        ).addCallbacks(self._after_execution_event, self._fatal)

    def _send_update_stop(self) -> None:
        assert self.client is not None and self.account_id is not None
        self._mark_stage("update_stop_submit")
        self._last_order_broker_sizing = None
        position_id = _safe_int(self.payload.get("position_id") or self.payload.get("broker_trade_id") or self.payload.get("trade_id"), 0)
        stop_loss = _safe_float(self.payload.get("stop_loss"), 0.0)
        if position_id <= 0 or stop_loss <= 0:
            self._finish(False, "cTrader position id or stop level missing for stop update")
            return
        asset = self.payload.get("asset") or ""
        symbol = self.payload.get("symbol") or asset
        if symbol or asset:
            try:
                symbol_id, symbol_name = self._resolve_symbol(symbol or asset, symbol)
                self._pending_order = {
                    "position_id": position_id,
                    "stop_loss": stop_loss,
                    "symbol_id": symbol_id,
                    "symbol_name": symbol_name,
                }
                self._pending_specs = {}
                self._request_symbol_detail("update_stop", symbol_id)
                return
            except Exception as exc:
                _stderr(f"cTrader stop update precision lookup failed for {asset}: {exc}")
        self._submit_stop_update(position_id, stop_loss)

    def _submit_stop_update(self, position_id: int, stop_loss: float) -> None:
        assert self.client is not None and self.account_id is not None
        if position_id <= 0 or stop_loss <= 0:
            self._finish(False, "cTrader position id or stop level missing for stop update")
            return
        self._order_request_sent = True
        self.client.send(
            ProtoOAAmendPositionSLTPReq(ctidTraderAccountId=self.account_id, positionId=position_id, stopLoss=stop_loss)
        ).addCallbacks(self._after_execution_event, self._fatal)

    def _after_execution_event(self, response: Any, *, symbol_id: int = 0, symbol_name: str = "") -> None:
        self._mark_stage("execution_event")
        try:
            order_error_type = int(ProtoOAOrderErrorEvent().payloadType)
            if int(getattr(response, "payloadType", 0) or 0) == order_error_type:
                parsed_error = self._parse(response, ProtoOAOrderErrorEvent)
                error_code = str(getattr(parsed_error, "errorCode", "") or "ctrader_order_error")
                description = str(getattr(parsed_error, "description", "") or error_code)
                self._finish(
                    False,
                    f"ctrader_order_error: {error_code}: {description}",
                    order_id=str(getattr(parsed_error, "orderId", "") or ""),
                    position_id=str(getattr(parsed_error, "positionId", "") or ""),
                    raw=_message_to_dict(parsed_error),
                    response_meta=self._response_meta(response),
                )
                return
            parsed = self._parse(response, ProtoOAExecutionEvent)
            execution_type = int(getattr(parsed, "executionType", 0) or 0)
            position = getattr(parsed, "position", None)
            order = getattr(parsed, "order", None)
            deal = getattr(parsed, "deal", None)
            error_code = str(getattr(parsed, "errorCode", "") or "")
            rejected = execution_type in {ProtoOAExecutionType.ORDER_REJECTED, ProtoOAExecutionType.ORDER_CANCEL_REJECTED}
            raw = _message_to_dict(parsed)
            if rejected or error_code:
                self._finish(
                    False,
                    error_code or f"cTrader execution rejected type={execution_type}",
                    raw=raw,
                    response_meta=self._response_meta(response),
                )
                return
            position_id = int(getattr(position, "positionId", 0) or getattr(order, "positionId", 0) or getattr(deal, "positionId", 0) or 0)
            order_id = int(getattr(order, "orderId", 0) or getattr(deal, "orderId", 0) or 0)
            deal_id = int(getattr(deal, "dealId", 0) or 0)
            if self.action == "place_order" and not (position_id or order_id or deal_id):
                self._finish_execution_unknown(
                    f"cTrader execution event returned no broker ids for type={execution_type}",
                    response=response,
                    execution_type=execution_type,
                    raw=raw,
                )
                return
            price = (
                _safe_float(getattr(deal, "executionPrice", 0.0), 0.0)
                or _safe_float(getattr(order, "executionPrice", 0.0), 0.0)
                or _safe_float(getattr(position, "price", 0.0), 0.0)
            )
            volume = _safe_int(getattr(deal, "filledVolume", 0) or getattr(deal, "volume", 0) or getattr(order, "executedVolume", 0), 0)
            if not symbol_id:
                symbol_id = int(getattr(deal, "symbolId", 0) or 0)
            broker_sizing = dict(self._last_order_broker_sizing or {})
            if broker_sizing:
                broker_sizing["filled_volume"] = volume
                broker_sizing["filled_lots"] = broker_sizing.get("broker_size")
            finish_payload = {
                "execution_type": execution_type,
                "position_id": str(position_id or ""),
                "order_id": str(order_id or ""),
                "deal_id": str(deal_id or ""),
                "avg_price": price,
                "volume": volume,
                "symbol_id": symbol_id,
                "symbol_name": symbol_name,
                "broker_sizing": broker_sizing,
                "account_id": str(self.account_id or ""),
                "environment": self.environment,
                "raw": raw,
                "response_meta": self._response_meta(response),
            }
            if self.action == "place_order" and position_id and (
                execution_type == ProtoOAExecutionType.ORDER_ACCEPTED or price <= 0.0 or volume <= 0
            ):
                self._defer_place_order_finish_for_reconcile(position_id=position_id, finish_payload=finish_payload)
                return
            self._finish(True, "execution accepted", **finish_payload)
        except Exception as exc:
            self._finish_execution_unknown(
                f"cTrader execution event callback failed at stage={self._stage}: {exc}",
                response=response,
            )

    def _on_disconnected(self, _client: Client, reason: Any) -> None:
        if not self._finished:
            _stderr(f"Disconnected from cTrader: {reason}")

    def _on_message(self, _client: Client, message: Any) -> None:
        if isinstance(message, ProtoOASpotEvent):
            self._handle_spot_event(message)
            return
        if int(getattr(message, "payloadType", 0) or 0) == int(ProtoOASpotEvent().payloadType):
            try:
                self._handle_spot_event(self._parse(message, ProtoOASpotEvent))
                return
            except Exception:
                pass
        if isinstance(message, ProtoOAErrorRes) and not self._finished:
            code = str(getattr(message, "errorCode", "") or "ctrader_error")
            description = str(getattr(message, "description", "") or code)
            self._finish(False, f"{code}: {description}")

    def _fatal(self, failure: Any) -> None:
        if self._finished:
            return
        message = ""
        try:
            message = str(failure.getErrorMessage())
        except Exception:
            message = str(failure)
        self._finish(False, message or "cTrader execution bridge failed")


def main() -> int:
    action = sys.argv[1] if len(sys.argv) > 1 else ""
    try:
        payload = json.loads(sys.stdin.read() or "{}")
    except Exception as exc:
        _stdout({"success": False, "action": action, "error": f"invalid stdin JSON: {exc}"})
        return 2
    return CTraderOneShot(action, payload if isinstance(payload, dict) else {}).run()


if __name__ == "__main__":
    raise SystemExit(main())
