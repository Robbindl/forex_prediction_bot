from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from config.config import (
    CTRADER_EXECUTION_ACCESS_TOKEN,
    CTRADER_EXECUTION_ACCOUNT_ID,
    CTRADER_EXECUTION_ALLOWED_CATEGORIES,
    CTRADER_EXECUTION_BROKER_NAME,
    CTRADER_EXECUTION_CLIENT_ID,
    CTRADER_EXECUTION_CLIENT_SECRET,
    CTRADER_EXECUTION_DISABLED_ASSETS,
    CTRADER_EXECUTION_DRY_RUN,
    CTRADER_EXECUTION_ENABLED,
    CTRADER_EXECUTION_ENVIRONMENT,
    CTRADER_EXECUTION_REFRESH_TOKEN,
    CTRADER_EXECUTION_REDIRECT_URI,
    CTRADER_EXECUTION_TOKEN_CACHE_PATH,
    EXECUTION_ROLE,
)
from execution.exchange_adapter import ExchangeAdapter, OrderBookSnapshot, OrderRequest, OrderResult
from risk.position_sizer import PositionSizer
from utils.logger import get_logger

logger = get_logger()

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
    "BTC-USD": {"category": "crypto", "aliases": ("BTCUSD", "BTC/USD", "BITCOIN")},
    "ETH-USD": {"category": "crypto", "aliases": ("ETHUSD", "ETH/USD", "ETHEREUM")},
    "BNB-USD": {"category": "crypto", "aliases": ("BNBUSD", "BNB/USD", "BINANCECOIN")},
    "SOL-USD": {"category": "crypto", "aliases": ("SOLUSD", "SOL/USD", "SOLANA")},
    "XRP-USD": {"category": "crypto", "aliases": ("XRPUSD", "XRP/USD", "RIPPLE")},
    "XAU/USD": {"category": "commodities", "aliases": ("XAUUSD", "GOLD")},
    "XAG/USD": {"category": "commodities", "aliases": ("XAGUSD", "SILVER")},
    "WTI": {"category": "commodities", "aliases": ("USOIL", "WTI", "CRUDE", "USCRUDE", "SPOTCRUDE", "WTI Cash (or Spot) Contract")},
    "US30": {"category": "indices", "aliases": ("US30", "DJ30", "WALLSTREET30")},
    "US100": {"category": "indices", "aliases": ("US100", "USTEC", "NAS100", "NASDAQ100")},
    "US500": {"category": "indices", "aliases": ("US500", "SPX500", "SP500")},
    "UK100": {"category": "indices", "aliases": ("UK100", "FTSE100")},
    "GER40": {"category": "indices", "aliases": ("GER40", "DE40", "DAX40")},
    "AUS200": {"category": "indices", "aliases": ("AUS200", "AU200")},
    "JPN225": {"category": "indices", "aliases": ("JPN225", "JP225", "JAP225", "NI225")},
}


def _canon(value: Any) -> str:
    return str(value or "").strip().upper()


def _symbol_key(value: Any) -> str:
    return "".join(ch for ch in str(value or "").upper() if ch.isalnum())


_ALIASES: Dict[str, str] = {}
for _asset, _meta in _SUPPORTED_ASSETS.items():
    _ALIASES[_canon(_asset)] = _asset
    _ALIASES[_symbol_key(_asset)] = _asset
    for _alias in _meta.get("aliases", ()):
        _ALIASES[_canon(_alias)] = _asset
        _ALIASES[_symbol_key(_alias)] = _asset


class CTraderAdapter(ExchangeAdapter):
    """cTrader execution adapter.

    This adapter is intentionally isolated from IG: no IG epics, no IG attached
    order conversion, and no IG crypto permissions are reused here.
    """

    def __init__(self) -> None:
        super().__init__("ctrader", rate_per_second=2.0, circuit_max_failures=3)
        self.broker_name = CTRADER_EXECUTION_BROKER_NAME
        self.environment = CTRADER_EXECUTION_ENVIRONMENT
        self._script = Path(__file__).resolve().parents[1] / "integrations" / "ctrader_broker_bridge" / "ctrader_broker_bridge.py"
        self._bridge_timeout = max(10.0, float(os.getenv("CTRADER_EXECUTION_ADAPTER_TIMEOUT_SECONDS", "45") or 45.0))

    @staticmethod
    def canonical_asset(asset: Any) -> str:
        text = _canon(asset)
        return _ALIASES.get(text) or _ALIASES.get(_symbol_key(text)) or text

    @staticmethod
    def _bool_env(value: Any, default: bool = False) -> bool:
        text = str(value or "").strip().lower()
        if not text:
            return bool(default)
        if text in {"1", "true", "yes", "on"}:
            return True
        if text in {"0", "false", "no", "off"}:
            return False
        return bool(default)

    def _broker_name(self) -> str:
        try:
            from services.execution_broker_state import load_execution_broker_state

            state = load_execution_broker_state()
            if str(state.get("provider") or "").strip().lower() == "ctrader":
                name = str(state.get("broker_name") or "").strip()
                if name.lower().replace(" ", "").replace("_", "") == "pepperstone":
                    return name
        except Exception:
            pass
        return "pepperstone"

    @staticmethod
    def _profile_prefix(broker_name: str) -> str:
        key = str(broker_name or "").strip().lower().replace(" ", "").replace("_", "")
        if key == "pepperstone":
            return "PEPPERSTONE_CTRADER_EXECUTION"
        return "CTRADER_EXECUTION"

    def _profile_env(self, suffix: str, default: Any = "") -> str:
        suffix = str(suffix or "").strip().upper()
        prefix = self._profile_prefix(self._broker_name())
        if prefix != "CTRADER_EXECUTION":
            value = os.getenv(f"{prefix}_{suffix}", "").strip()
            if value:
                return value
            configured_prefix = self._profile_prefix(CTRADER_EXECUTION_BROKER_NAME)
            isolated = {"ENABLED", "DRY_RUN", "CLIENT_ID", "CLIENT_SECRET", "ACCESS_TOKEN", "REFRESH_TOKEN", "ACCOUNT_ID", "TOKEN_CACHE_PATH"}
            if prefix != configured_prefix and suffix in isolated:
                if suffix == "ENABLED":
                    return "false"
                if suffix == "DRY_RUN":
                    return "true"
                if suffix == "TOKEN_CACHE_PATH":
                    return f"data/runtime_locks/{prefix.lower()}_tokens.json"
                return ""
        return os.getenv(f"CTRADER_EXECUTION_{suffix}", str(default or "")).strip() or str(default or "")

    def _profile_config(self) -> Dict[str, str]:
        return {
            "BROKER_NAME": self._broker_name(),
            "ENVIRONMENT": self._profile_env("ENVIRONMENT", CTRADER_EXECUTION_ENVIRONMENT),
            "CLIENT_ID": self._profile_env("CLIENT_ID", CTRADER_EXECUTION_CLIENT_ID),
            "CLIENT_SECRET": self._profile_env("CLIENT_SECRET", CTRADER_EXECUTION_CLIENT_SECRET),
            "ACCESS_TOKEN": self._profile_env("ACCESS_TOKEN", CTRADER_EXECUTION_ACCESS_TOKEN),
            "REFRESH_TOKEN": self._profile_env("REFRESH_TOKEN", CTRADER_EXECUTION_REFRESH_TOKEN),
            "ACCOUNT_ID": self._profile_env("ACCOUNT_ID", CTRADER_EXECUTION_ACCOUNT_ID),
            "REDIRECT_URI": self._profile_env("REDIRECT_URI", CTRADER_EXECUTION_REDIRECT_URI),
            "TOKEN_CACHE_PATH": self._profile_env("TOKEN_CACHE_PATH", str(CTRADER_EXECUTION_TOKEN_CACHE_PATH)),
        }

    def _enabled(self) -> bool:
        return self._bool_env(self._profile_env("ENABLED", "true" if CTRADER_EXECUTION_ENABLED else "false"), CTRADER_EXECUTION_ENABLED)

    def _dry_run(self) -> bool:
        return self._bool_env(self._profile_env("DRY_RUN", "true" if CTRADER_EXECUTION_DRY_RUN else "false"), CTRADER_EXECUTION_DRY_RUN)

    def _environment(self) -> str:
        return self._profile_env("ENVIRONMENT", CTRADER_EXECUTION_ENVIRONMENT).strip().lower() or "demo"

    def _disabled_assets(self) -> set[str]:
        raw = self._profile_env("DISABLED_ASSETS", ",".join(CTRADER_EXECUTION_DISABLED_ASSETS))
        items = [item.strip() for item in raw.replace(";", ",").split(",") if item.strip()]
        disabled = {_canon(item) for item in items}
        disabled.update(_symbol_key(item) for item in items)
        return {item for item in disabled if item}

    def _allowed_categories(self) -> set[str]:
        raw = self._profile_env("ALLOWED_CATEGORIES", ",".join(CTRADER_EXECUTION_ALLOWED_CATEGORIES))
        allowed = {item.strip().lower() for item in raw.replace(";", ",").split(",") if item.strip()}
        return allowed or {"forex", "crypto", "commodities"}

    def _credentials_ready(self) -> bool:
        profile = self._profile_config()
        has_tokens = bool(profile["ACCESS_TOKEN"] or profile["REFRESH_TOKEN"])
        return bool(profile["CLIENT_ID"] and profile["CLIENT_SECRET"] and profile["ACCOUNT_ID"] and has_tokens)

    def supports_asset(self, asset: str, category: str) -> Tuple[bool, str]:
        canonical = self.canonical_asset(asset)
        normalized_disabled = self._disabled_assets()
        if canonical in normalized_disabled or _symbol_key(canonical) in normalized_disabled:
            return False, f"cTrader execution disabled for {canonical}"
        category = str(category or _SUPPORTED_ASSETS.get(canonical, {}).get("category") or "").strip().lower()
        if category not in self._allowed_categories():
            return False, f"cTrader execution category not allowed: {category or 'unknown'}"
        if canonical not in _SUPPORTED_ASSETS:
            return False, f"cTrader symbol not mapped for {asset}"
        if not self._enabled():
            return False, f"cTrader execution disabled for {self._broker_name()}; set CTRADER_EXECUTION_ENABLED=true or broker-specific cTrader execution enabled"
        if not self._dry_run() and not self._credentials_ready():
            return False, "cTrader execution credentials incomplete"
        return True, ""

    def _run_bridge(self, action: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        if not self._script.exists():
            return {"success": False, "error": f"cTrader bridge script missing: {self._script}"}
        env = os.environ.copy()
        for key, value in self._profile_config().items():
            env[f"CTRADER_EXECUTION_{key}"] = str(value or "")
        if not str(env.get("CTRADER_EXECUTION_BRIDGE_TIMEOUT_SECONDS") or "").strip():
            env["CTRADER_EXECUTION_BRIDGE_TIMEOUT_SECONDS"] = str(max(20.0, self._bridge_timeout - 5.0))
        try:
            proc = subprocess.run(
                [sys.executable, str(self._script), action],
                input=json.dumps(payload, ensure_ascii=True, default=str),
                text=True,
                capture_output=True,
                timeout=self._bridge_timeout,
                env=env,
            )
        except subprocess.TimeoutExpired:
            return {
                "success": False,
                "error": f"cTrader execution adapter timed out after {self._bridge_timeout:.0f}s",
                "action": action,
            }
        stdout = str(proc.stdout or "").strip().splitlines()
        stderr = str(proc.stderr or "").strip()
        parsed: Dict[str, Any] = {}
        if stdout:
            try:
                parsed = json.loads(stdout[-1])
            except Exception:
                parsed = {"success": False, "error": stdout[-1]}
        if not parsed:
            parsed = {"success": False, "error": stderr or f"cTrader bridge exited {proc.returncode}"}
        if stderr:
            parsed.setdefault("stderr", stderr[-1000:])
        if proc.returncode and parsed.get("success") is not True:
            parsed.setdefault("error", f"cTrader bridge exited {proc.returncode}")
        return parsed

    @staticmethod
    def _local_size(req: OrderRequest) -> float:
        return float(req.local_quantity or req.quantity or 0.0)

    @staticmethod
    def _local_size_to_volume(local_size: float) -> int:
        return max(1, int(round(float(local_size or 0.0) * 100.0)))

    @staticmethod
    def _local_size_from_volume(volume: Any, fallback: float = 0.0) -> float:
        value = float(volume or 0.0) / 100.0
        return value if value > 0.0 else float(fallback or 0.0)

    def _order_payload(self, req: OrderRequest) -> Dict[str, Any]:
        canonical = self.canonical_asset(req.asset or req.symbol)
        category = str(req.category or _SUPPORTED_ASSETS.get(canonical, {}).get("category") or "forex").lower()
        local_size = self._local_size(req)
        lots = PositionSizer.lots_from_size(canonical, category, local_size)
        client_id = str(req.client_id or req.metadata.get("trade_id") or f"rb-{uuid.uuid4().hex[:20]}")[:64]
        return {
            "asset": canonical,
            "symbol": req.symbol or canonical,
            "category": category,
            "side": str(req.side or "BUY").upper(),
            "local_size": local_size,
            "lot_size": lots,
            "volume": self._local_size_to_volume(local_size),
            "entry_price": float(req.price or 0.0),
            "stop_loss": float(req.stop_loss or 0.0),
            "take_profit": float(req.take_profit or 0.0),
            "client_order_id": client_id,
            "reason": "bot execution",
        }

    def _place_order(self, req: OrderRequest) -> OrderResult:
        canonical = self.canonical_asset(req.asset or req.symbol)
        category = str(req.category or _SUPPORTED_ASSETS.get(canonical, {}).get("category") or "forex").lower()
        supported, reason = self.supports_asset(canonical, category)
        if not supported:
            return OrderResult(order_id="", status="FAILED", error=reason)
        if EXECUTION_ROLE != "trader":
            return OrderResult(order_id="", status="FAILED", error=f"read-only execution role: {EXECUTION_ROLE}")
        payload = self._order_payload(req)
        if self._dry_run():
            return OrderResult(
                order_id="",
                status="FAILED",
                error=f"cTrader execution dry-run is enabled for {self._broker_name()}; set CTRADER_EXECUTION_DRY_RUN=false or the broker-specific dry-run flag after credentials are verified",
                raw={"request": payload, "broker": "ctrader", "dry_run": True},
            )

        result = self._run_bridge("place_order", payload)
        if not result.get("success"):
            return OrderResult(order_id="", status="FAILED", error=str(result.get("error") or result), raw=result)

        order_id = str(result.get("position_id") or result.get("order_id") or result.get("deal_id") or payload["client_order_id"])
        avg_price = float(result.get("avg_price") or payload.get("entry_price") or 0.0)
        broker_sizing = dict(result.get("broker_sizing") or {})
        filled_size = float(broker_sizing.get("local_position_size") or self._local_size_from_volume(result.get("volume"), payload.get("local_size")))
        if not broker_sizing:
            broker_sizing = {
                "broker": "ctrader",
                "broker_name": self._broker_name(),
                "environment": self._environment(),
                "broker_size": round(float(payload["lot_size"]), 6),
                "broker_volume": int(payload["volume"]),
                "local_position_size": round(float(payload["local_size"]), 8),
                "broker_cash_per_price_unit_per_size": 1.0,
            }
        raw = dict(result)
        raw.update(
            {
                "broker": "ctrader",
                "request": payload,
                "broker_sizing": broker_sizing,
            }
        )
        raw["trade"] = self._build_trade_snapshot(req, payload, result, order_id=order_id, avg_price=avg_price, filled_size=filled_size)
        return OrderResult(order_id=order_id, status="FILLED", filled_qty=filled_size, avg_price=avg_price, raw=raw)

    def _build_trade_snapshot(
        self,
        req: OrderRequest,
        payload: Dict[str, Any],
        result: Dict[str, Any],
        *,
        order_id: str,
        avg_price: float,
        filled_size: float,
    ) -> Dict[str, Any]:
        metadata = dict(req.metadata or {})
        profile = self._profile_config()
        broker_name = self._broker_name()
        environment = self._environment()
        broker_execution = {
            "broker": "ctrader",
            "broker_name": broker_name,
            "environment": environment,
            "account_id": str(result.get("account_id") or profile.get("ACCOUNT_ID") or ""),
            "symbol_id": str(result.get("symbol_id") or ""),
            "symbol_name": str(result.get("symbol_name") or req.symbol or payload.get("asset") or ""),
            "position_id": str(result.get("position_id") or order_id),
            "order_id": str(result.get("order_id") or ""),
            "deal_id": str(result.get("deal_id") or ""),
            "broker_sizing": dict(result.get("broker_sizing") or payload.get("broker_sizing") or {}),
        }
        if not broker_execution["broker_sizing"]:
            broker_execution["broker_sizing"] = {
                "broker": "ctrader",
                "broker_name": broker_name,
                "environment": environment,
                "broker_size": round(float(payload.get("lot_size") or 0.0), 6),
                "broker_volume": int(payload.get("volume") or 0),
                "local_position_size": round(float(filled_size or payload.get("local_size") or 0.0), 8),
                "broker_cash_per_price_unit_per_size": 1.0,
            }
        broker_size = float(broker_execution["broker_sizing"].get("broker_size") or payload.get("lot_size") or 0.0)
        broker_volume = int(broker_execution["broker_sizing"].get("broker_volume") or payload.get("volume") or 0)
        local_position_size = float(broker_execution["broker_sizing"].get("local_position_size") or filled_size or payload.get("local_size") or 0.0)
        metadata["broker_execution"] = broker_execution
        metadata.setdefault("take_profit_levels", list(req.metadata.get("take_profit_levels") or []))
        return {
            "trade_id": order_id,
            "broker_trade_id": str(result.get("position_id") or order_id),
            "broker_deal_reference": str(result.get("deal_id") or result.get("order_id") or ""),
            "broker_symbol": str(result.get("symbol_name") or req.symbol or payload.get("asset") or ""),
            "broker": "ctrader",
            "execution_mode": f"ctrader_{environment}",
            "asset": payload.get("asset"),
            "category": payload.get("category"),
            "direction": payload.get("side"),
            "signal": payload.get("side"),
            "entry_price": avg_price,
            "current_price": avg_price,
            "stop_loss": float(payload.get("stop_loss") or 0.0),
            "take_profit": float(payload.get("take_profit") or 0.0),
            "take_profit_levels": list(req.metadata.get("take_profit_levels") or []),
            "position_size": local_position_size,
            "initial_position_size": local_position_size,
            "broker_position_size": local_position_size,
            "broker_volume": broker_volume,
            "lot_size": broker_size,
            "metadata": metadata,
        }

    def partial_close_position(
        self,
        position: Dict[str, Any],
        *,
        local_close_size: float,
        reason: str = "Partial Close",
    ) -> OrderResult:
        position_id = str(position.get("broker_trade_id") or position.get("trade_id") or "").strip()
        local_close_size = max(0.0, float(local_close_size or 0.0))
        volume = self._local_size_to_volume(local_close_size)
        if self._dry_run():
            return OrderResult(order_id=position_id, status="FAILED", error="cTrader execution dry-run", raw={"dry_run": True})
        result = self._run_bridge(
            "partial_close",
            {
                "position_id": position_id,
                "trade_id": position.get("trade_id"),
                "volume": volume,
                "local_close_size": local_close_size,
                "reason": reason,
            },
        )
        if not result.get("success"):
            return OrderResult(order_id=position_id, status="FAILED", error=str(result.get("error") or result), raw=result)
        avg_price = float(result.get("avg_price") or position.get("current_price") or position.get("entry_price") or 0.0)
        return OrderResult(
            order_id=position_id,
            status="FILLED",
            filled_qty=local_close_size,
            avg_price=avg_price,
            raw={
                **result,
                "broker_close_size": local_close_size,
                "broker_close_volume": volume,
            },
        )

    def close_position(self, position: Dict[str, Any], *, reason: str = "Manual Close") -> OrderResult:
        size = float(position.get("position_size") or 0.0)
        return self.partial_close_position(position, local_close_size=size, reason=reason)

    def update_position_stop(
        self,
        position: Dict[str, Any],
        *,
        stop_level: float,
        reason: str = "Managed Stop Update",
    ) -> OrderResult:
        position_id = str(position.get("broker_trade_id") or position.get("trade_id") or "").strip()
        if self._dry_run():
            return OrderResult(order_id=position_id, status="FAILED", error="cTrader execution dry-run", raw={"dry_run": True})
        result = self._run_bridge(
            "update_stop",
            {
                "position_id": position_id,
                "trade_id": position.get("trade_id"),
                "asset": position.get("asset") or position.get("symbol") or position.get("broker_symbol"),
                "symbol": position.get("broker_symbol") or position.get("asset") or position.get("symbol"),
                "category": position.get("category"),
                "stop_loss": float(stop_level or 0.0),
                "reason": reason,
            },
        )
        if not result.get("success"):
            return OrderResult(order_id=position_id, status="FAILED", error=str(result.get("error") or result), raw=result)
        return OrderResult(order_id=position_id, status="FILLED", filled_qty=0.0, avg_price=0.0, raw=result)

    def list_open_positions(self) -> list[dict]:
        if self._dry_run() or not self._enabled():
            return []
        result = self._run_bridge("list_positions", {})
        if not result.get("success"):
            raise RuntimeError(str(result.get("error") or result))
        return list(result.get("positions") or [])

    def _cancel_order(self, order_id: str) -> bool:
        return False

    def _get_order_status(self, order_id: str) -> Optional[OrderResult]:
        return None

    def _get_balance(self, currency: str) -> float:
        if self._dry_run() or not self._enabled():
            return 0.0
        result = self._run_bridge("balance", {})
        if not result.get("success"):
            logger.warning(f"[cTrader] balance fetch failed: {result.get('error') or result}")
            return 0.0
        return float(result.get("balance") or 0.0)

    def _get_orderbook(self, symbol: str, depth: int) -> Optional[OrderBookSnapshot]:
        return None
