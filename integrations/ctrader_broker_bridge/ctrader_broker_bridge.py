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
    ProtoOAErrorRes,
    ProtoOAExecutionEvent,
    ProtoOAGetAccountListByAccessTokenReq,
    ProtoOANewOrderReq,
    ProtoOAReconcileReq,
    ProtoOASymbolsListReq,
    ProtoOATraderReq,
    ProtoOATraderRes,
    ProtoOAAmendPositionSLTPReq,
    ProtoOAClosePositionReq,
    ProtoOAGetAccountListByAccessTokenRes,
    ProtoOASymbolsListRes,
    ProtoOAReconcileRes,
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
    "XAU/USD": {"category": "commodities", "aliases": ("XAUUSD", "GOLD", "XAU/USD")},
    "XAG/USD": {"category": "commodities", "aliases": ("XAGUSD", "SILVER", "XAG/USD")},
    "WTI": {"category": "commodities", "aliases": ("USOIL", "WTI", "CRUDE", "USCRUDE")},
    "US30": {"category": "indices", "aliases": ("US30", "DJ30", "WALLSTREET30")},
    "US100": {"category": "indices", "aliases": ("US100", "USTEC", "NAS100", "NASDAQ100")},
    "US500": {"category": "indices", "aliases": ("US500", "SPX500", "SP500")},
    "UK100": {"category": "indices", "aliases": ("UK100", "FTSE100")},
    "GER40": {"category": "indices", "aliases": ("GER40", "DE40", "DAX40")},
    "AUS200": {"category": "indices", "aliases": ("AUS200", "AU200")},
    "JPN225": {"category": "indices", "aliases": ("JPN225", "JP225", "JAP225", "NI225")},
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
        self.symbols: List[Any] = []
        self.symbol_lookup: Dict[str, Tuple[int, str]] = {}
        self._finished = False

    def run(self) -> int:
        if not self.client_id or not self.client_secret:
            self._finish(False, "cTrader execution credentials missing: set CTRADER_EXECUTION_CLIENT_ID and CTRADER_EXECUTION_CLIENT_SECRET")
            return 2
        self._load_cached_tokens()
        if not self.access_token and self.refresh_token:
            self._refresh_access_token()
        if not self.access_token:
            auth = Auth(self.client_id, self.client_secret, self.redirect_uri)
            self._finish(
                False,
                "cTrader execution access token missing",
                auth_url=auth.getAuthUri(scope="trading"),
            )
            return 2
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
            self._finish(False, "cTrader execution bridge timed out")

    def _parse(self, response: Any, cls: Type[Any]) -> Any:
        if isinstance(response, cls):
            return response
        parsed = cls()
        payload = getattr(response, "payload", None)
        if payload:
            parsed.ParseFromString(payload)
            return parsed
        return response

    def _on_connected(self, client: Client) -> None:
        req = ProtoOAApplicationAuthReq(clientId=self.client_id, clientSecret=self.client_secret)
        client.send(req).addCallbacks(self._after_app_auth, self._fatal)

    def _after_app_auth(self, _response: Any) -> None:
        assert self.client is not None
        req = ProtoOAGetAccountListByAccessTokenReq(accessToken=self.access_token)
        self.client.send(req).addCallbacks(self._after_account_list, self._fatal)

    def _after_account_list(self, response: Any) -> None:
        parsed = self._parse(response, ProtoOAGetAccountListByAccessTokenRes)
        accounts = list(getattr(parsed, "ctidTraderAccount", []) or [])
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
        req = ProtoOAAccountAuthReq(ctidTraderAccountId=self.account_id, accessToken=self.access_token)
        self.client.send(req).addCallbacks(self._after_account_auth, self._fatal)

    def _after_account_auth(self, _response: Any) -> None:
        assert self.client is not None and self.account_id is not None
        req = ProtoOASymbolsListReq(ctidTraderAccountId=self.account_id, includeArchivedSymbols=False)
        self.client.send(req).addCallbacks(self._after_symbols, self._fatal)

    def _after_symbols(self, response: Any) -> None:
        parsed = self._parse(response, ProtoOASymbolsListRes)
        self.symbols = list(getattr(parsed, "symbol", []) or [])
        self._build_symbol_lookup()
        if self.action == "balance":
            self._send_balance()
        elif self.action == "list_positions":
            self._send_reconcile()
        elif self.action == "place_order":
            self._send_place_order()
        elif self.action in {"close_position", "partial_close"}:
            self._send_close_position()
        elif self.action == "update_stop":
            self._send_update_stop()
        else:
            self._finish(False, f"unsupported cTrader bridge action: {self.action}")

    def _build_symbol_lookup(self) -> None:
        lookup: Dict[str, Tuple[int, str]] = {}
        for item in self.symbols:
            symbol_id = int(getattr(item, "symbolId", 0) or 0)
            raw_name = str(getattr(item, "symbolName", "") or "")
            description = str(getattr(item, "description", "") or "")
            names = {_normalize_name(raw_name), _normalize_name(description)}
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

    def _send_balance(self) -> None:
        assert self.client is not None and self.account_id is not None
        self.client.send(ProtoOATraderReq(ctidTraderAccountId=self.account_id)).addCallbacks(self._after_balance, self._fatal)

    def _after_balance(self, response: Any) -> None:
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
        self.client.send(ProtoOAReconcileReq(ctidTraderAccountId=self.account_id)).addCallbacks(self._after_reconcile, self._fatal)

    def _after_reconcile(self, response: Any) -> None:
        parsed = self._parse(response, ProtoOAReconcileRes)
        positions = [_message_to_dict(item) for item in list(getattr(parsed, "position", []) or [])]
        self._finish(True, "positions fetched", positions=positions, account_id=str(self.account_id or ""), environment=self.environment)

    def _send_place_order(self) -> None:
        assert self.client is not None and self.account_id is not None
        asset = self.payload.get("asset") or self.payload.get("symbol") or ""
        symbol_id, symbol_name = self._resolve_symbol(asset, self.payload.get("symbol"))
        side = str(self.payload.get("side") or "BUY").upper()
        volume = max(1, _safe_int(self.payload.get("volume"), 0))
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
        if stop_loss > 0:
            req_kwargs["stopLoss"] = stop_loss
        if take_profit > 0:
            req_kwargs["takeProfit"] = take_profit
        self.client.send(ProtoOANewOrderReq(**req_kwargs)).addCallbacks(
            lambda response: self._after_execution_event(response, symbol_id=symbol_id, symbol_name=symbol_name),
            self._fatal,
        )

    def _send_close_position(self) -> None:
        assert self.client is not None and self.account_id is not None
        position_id = _safe_int(self.payload.get("position_id") or self.payload.get("broker_trade_id") or self.payload.get("trade_id"), 0)
        volume = max(1, _safe_int(self.payload.get("volume"), 0))
        if position_id <= 0:
            self._finish(False, "cTrader position id missing for close")
            return
        self.client.send(
            ProtoOAClosePositionReq(ctidTraderAccountId=self.account_id, positionId=position_id, volume=volume)
        ).addCallbacks(self._after_execution_event, self._fatal)

    def _send_update_stop(self) -> None:
        assert self.client is not None and self.account_id is not None
        position_id = _safe_int(self.payload.get("position_id") or self.payload.get("broker_trade_id") or self.payload.get("trade_id"), 0)
        stop_loss = _safe_float(self.payload.get("stop_loss"), 0.0)
        if position_id <= 0 or stop_loss <= 0:
            self._finish(False, "cTrader position id or stop level missing for stop update")
            return
        self.client.send(
            ProtoOAAmendPositionSLTPReq(ctidTraderAccountId=self.account_id, positionId=position_id, stopLoss=stop_loss)
        ).addCallbacks(self._after_execution_event, self._fatal)

    def _after_execution_event(self, response: Any, *, symbol_id: int = 0, symbol_name: str = "") -> None:
        parsed = self._parse(response, ProtoOAExecutionEvent)
        execution_type = int(getattr(parsed, "executionType", 0) or 0)
        position = getattr(parsed, "position", None)
        order = getattr(parsed, "order", None)
        deal = getattr(parsed, "deal", None)
        error_code = str(getattr(parsed, "errorCode", "") or "")
        rejected = execution_type in {ProtoOAExecutionType.ORDER_REJECTED, ProtoOAExecutionType.ORDER_CANCEL_REJECTED}
        if rejected or error_code:
            self._finish(False, error_code or f"cTrader execution rejected type={execution_type}", raw=_message_to_dict(parsed))
            return
        position_id = int(getattr(position, "positionId", 0) or getattr(order, "positionId", 0) or getattr(deal, "positionId", 0) or 0)
        order_id = int(getattr(order, "orderId", 0) or getattr(deal, "orderId", 0) or 0)
        deal_id = int(getattr(deal, "dealId", 0) or 0)
        price = (
            _safe_float(getattr(deal, "executionPrice", 0.0), 0.0)
            or _safe_float(getattr(order, "executionPrice", 0.0), 0.0)
            or _safe_float(getattr(position, "price", 0.0), 0.0)
        )
        volume = _safe_int(getattr(deal, "filledVolume", 0) or getattr(deal, "volume", 0) or getattr(order, "executedVolume", 0), 0)
        if not symbol_id:
            symbol_id = int(getattr(deal, "symbolId", 0) or 0)
        self._finish(
            True,
            "execution accepted",
            execution_type=execution_type,
            position_id=str(position_id or ""),
            order_id=str(order_id or ""),
            deal_id=str(deal_id or ""),
            avg_price=price,
            volume=volume,
            symbol_id=symbol_id,
            symbol_name=symbol_name,
            account_id=str(self.account_id or ""),
            environment=self.environment,
            raw=_message_to_dict(parsed),
        )

    def _on_disconnected(self, _client: Client, reason: Any) -> None:
        if not self._finished:
            _stderr(f"Disconnected from cTrader: {reason}")

    def _on_message(self, _client: Client, message: Any) -> None:
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
