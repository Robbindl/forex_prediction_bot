from __future__ import annotations

import time
import math
import os
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from config.config import (
    EXECUTION_ROLE,
    IG_ENVIRONMENT,
    IG_EXECUTION_CURRENCY,
    IG_EXECUTION_DRY_RUN,
)
from execution.exchange_adapter import ExchangeAdapter, OrderBookSnapshot, OrderRequest, OrderResult
from risk.broker_sizer import BrokerContractSpec, BrokerPositionSizer
from risk.position_sizer import PositionSizer
from services.ig_market_bridge import (
    IGMarketBridge,
    IGRequestError,
    denormalize_ig_market_price,
    normalize_ig_market_price,
)
from utils.logger import get_logger

logger = get_logger()

_POSITIONS_OTC = "/positions/otc"
_CONFIRM_ENDPOINT = "/confirms/{deal_reference}"
_POSITIONS_ENDPOINT = "/positions"
_ALT_CRYPTO_ASSETS = {"ETH-USD", "SOL-USD", "XRP-USD", "BNB-USD"}


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except Exception:
        return int(default)


def _env_float(name: str, default: float) -> float:
    try:
        text = str(os.getenv(name, "")).strip()
        return float(text) if text else float(default)
    except Exception:
        return float(default)


def _decimals_from_step(value: float) -> int:
    text = f"{float(value or 0.0):.10f}".rstrip("0").rstrip(".")
    if "." not in text:
        return 0
    return min(10, max(0, len(text.split(".", 1)[1])))


_STOP_DISTANCE_PCT_DEFAULTS = {
    "forex": 0.015,
    "commodities": 0.035,
    "indices": 0.035,
    "crypto": 0.060,
}


def _max_stop_distance_pct(category: str) -> float:
    key = str(category or "").strip().lower() or "forex"
    default = _STOP_DISTANCE_PCT_DEFAULTS.get(key, 0.025)
    category_key = key.upper().replace("-", "_")
    configured = _env_float(f"IG_MAX_STOP_DISTANCE_PCT_{category_key}", -1.0)
    if configured <= 0:
        configured = _env_float("IG_MAX_STOP_DISTANCE_PCT", default)
    return max(0.001, float(configured or default))


class IGAdapter(ExchangeAdapter):
    """
    IG execution adapter.

    It receives the bot's internal JustMarkets/MT-style position size, converts
    it to IG deal size by matching cash P&L per price move, and submits the
    converted size to IG.
    """

    def __init__(
        self,
        *,
        bridge: Optional[IGMarketBridge] = None,
        dry_run: Optional[bool] = None,
        currency: str = "",
    ) -> None:
        super().__init__(name="ig", rate_per_second=1.0)
        self._bridge = bridge or IGMarketBridge()
        self._dry_run = bool(IG_EXECUTION_DRY_RUN if dry_run is None else dry_run)
        self._currency = str(currency or IG_EXECUTION_CURRENCY or "USD").upper()
        self._environment = str(IG_ENVIRONMENT or "demo").lower()
        self._allow_alt_crypto = os.getenv("IG_EXECUTION_ALLOW_ALT_CRYPTO", "false").lower() == "true"
        disabled_assets = {
            item.strip().upper()
            for item in os.getenv("IG_EXECUTION_DISABLED_ASSETS", "").replace(";", ",").split(",")
            if item.strip()
        }
        self._unsupported_assets: Dict[str, str] = {
            asset: "IG execution disabled by IG_EXECUTION_DISABLED_ASSETS"
            for asset in disabled_assets
        }

    def _execution_disabled_reason(self, asset: str, category: str = "") -> str:
        asset_key = str(asset or "").upper()
        if (
            str(category or "").lower() == "crypto"
            and asset_key in _ALT_CRYPTO_ASSETS
            and not self._allow_alt_crypto
        ):
            return (
                "IG alt-crypto execution disabled: account has no access to "
                "CRYP_CFD_ALT; BTC-USD is the only enabled IG crypto route"
            )
        return ""

    def supports_asset(self, asset: str, category: str = "") -> tuple[bool, str]:
        asset_key = str(asset or "").upper()
        disabled_reason = self._execution_disabled_reason(asset_key, category)
        if disabled_reason:
            return False, disabled_reason
        cached_reason = self._unsupported_assets.get(asset_key)
        if cached_reason:
            return False, cached_reason
        if asset_key == "BNB-USD" and asset_key not in getattr(self._bridge, "_epic_overrides", {}):
            return False, "IG epic not configured for BNB-USD"
        try:
            resolved = self._bridge.resolve_symbol_info(asset, category=category)
        except IGRequestError as exc:
            if IGMarketBridge._is_allowance_error(exc.code, exc.message):
                return False, f"broker_temporarily_unavailable:{exc.code}"
            return False, f"{exc.code}: {exc.message}"
        if not resolved:
            return False, f"IG epic not found for {asset}"
        return True, ""

    def _place_order(self, req: OrderRequest) -> OrderResult:
        if str(EXECUTION_ROLE or "trader").lower() != "trader":
            return OrderResult(order_id="", status="FAILED", error="execution role is read-only")

        asset = req.asset or req.symbol
        category = req.category or ""
        disabled_reason = self._execution_disabled_reason(asset, category)
        if disabled_reason:
            return OrderResult(order_id="", status="FAILED", error=disabled_reason)
        resolved = self._bridge.resolve_symbol_info(asset, category=category)
        if not resolved:
            return OrderResult(order_id="", status="FAILED", error=f"IG epic not found for {asset}")

        epic = str(resolved.get("symbol") or req.symbol or "")
        if not epic:
            return OrderResult(order_id="", status="FAILED", error=f"IG epic missing for {asset}")

        try:
            spec_payload = self._bridge.get_contract_spec(asset, category=category)
        except IGRequestError as exc:
            prefix = "broker_temporarily_unavailable" if IGMarketBridge._is_allowance_error(exc.code, exc.message) else "ig_contract_spec_error"
            return OrderResult(
                order_id="",
                status="FAILED",
                error=f"{prefix}: {exc.code}: {exc.message}",
            )
        spec = self._contract_spec_from_payload(asset=asset, epic=epic, payload=spec_payload)
        if spec is None:
            return OrderResult(order_id="", status="FAILED", error=f"IG contract spec missing for {asset}")

        converted = BrokerPositionSizer.convert(
            asset=asset,
            category=category,
            local_size=float(req.local_quantity or req.quantity or 0.0),
            broker_spec=spec,
        )
        if not converted.accepted:
            return OrderResult(
                order_id="",
                status="FAILED",
                error=(
                    f"IG size rejected for {asset}: {converted.reason} "
                    f"raw={converted.broker_raw_size} min={converted.broker_min_size}"
                ),
                raw={"broker_sizing": converted.to_dict()},
            )

        deal_reference = req.client_id or f"rb-{uuid.uuid4().hex[:24]}"
        size = converted.broker_size
        payload: Dict[str, Any] = {
            "dealReference": deal_reference,
            "direction": req.side.upper(),
            "epic": epic,
            "expiry": "-",
            "size": size,
            "orderType": str(req.order_type or "MARKET").upper(),
            "timeInForce": "FILL_OR_KILL",
            "forceOpen": True,
            "guaranteedStop": False,
            "currencyCode": self._order_currency(spec),
        }
        if payload["orderType"] != "MARKET" and req.price:
            payload["level"] = float(req.price)
        attached_orders = self._apply_attached_orders(payload, req, spec_payload or {}, spec)
        fatal_attached_error = str((attached_orders or {}).get("fatal_error") or "").strip()
        if fatal_attached_error:
            return OrderResult(
                order_id="",
                status="FAILED",
                error=f"IG attached order rejected locally: {fatal_attached_error}",
                raw={"request": payload, "broker_sizing": converted.to_dict(), "attached_orders": attached_orders},
            )

        if self._dry_run:
            return OrderResult(
                order_id="",
                status="FAILED",
                error="IG execution dry-run is enabled; order was not sent",
                raw={"request": payload, "broker_sizing": converted.to_dict(), "attached_orders": attached_orders},
            )

        try:
            ack = self._bridge._request("POST", _POSITIONS_OTC, json_body=payload, version="2")
            confirm = self._confirm(str(ack.get("dealReference") or deal_reference))
        except IGRequestError as exc:
            error = f"{exc.code}: {exc.message}"
            if self._is_permission_error(error):
                error = f"broker_permission_denied: {error}"
                self._unsupported_assets[str(asset or "").upper()] = error
                self._cache_alt_crypto_denial(error)
            return OrderResult(
                order_id="",
                status="FAILED",
                error=error,
                raw={"request": payload, "attached_orders": attached_orders},
            )

        deal_status = str(confirm.get("dealStatus") or "").upper()
        if confirm.get("_confirm_pending"):
            return OrderResult(
                order_id="",
                status="FAILED",
                error=(
                    f"ig_confirm_pending: IG did not finalize dealReference={deal_reference} "
                    f"within the polling window; last_status={deal_status or 'UNKNOWN'}"
                ),
                raw={
                    "request": payload,
                    "confirm": confirm,
                    "broker_sizing": converted.to_dict(),
                    "attached_orders": attached_orders,
                },
            )
        if deal_status != "ACCEPTED":
            error = self._confirm_reject_message(confirm)
            if self._is_permission_error(error):
                error = f"broker_permission_denied: {error}"
                self._unsupported_assets[str(asset or "").upper()] = error
                self._cache_alt_crypto_denial(error)
            return OrderResult(
                order_id="",
                status="FAILED",
                error=error,
                raw={
                    "request": payload,
                    "confirm": confirm,
                    "broker_sizing": converted.to_dict(),
                    "attached_orders": attached_orders,
                },
            )

        deal_id = str(confirm.get("dealId") or ack.get("dealReference") or deal_reference)
        raw_fill_price = _safe_float(confirm.get("level"), req.price or 0.0)
        fill_price = _safe_float(normalize_ig_market_price(asset, raw_fill_price), raw_fill_price)
        trade = self._build_trade(
            req,
            asset,
            category,
            epic,
            deal_id,
            deal_reference,
            fill_price,
            converted.to_dict(),
            attached_orders,
        )
        return OrderResult(
            order_id=deal_id,
            status="FILLED",
            filled_qty=float(req.local_quantity or req.quantity or 0.0),
            avg_price=fill_price,
            raw={
                "trade": trade,
                "confirm": confirm,
                "broker_sizing": converted.to_dict(),
                "attached_orders": attached_orders,
                "broker_fill_price": raw_fill_price,
                "display_fill_price": fill_price,
            },
        )

    def _contract_spec(self, *, asset: str, category: str, epic: str) -> Optional[BrokerContractSpec]:
        spec_payload = self._bridge.get_contract_spec(asset, category=category)
        return self._contract_spec_from_payload(asset=asset, epic=epic, payload=spec_payload)

    def _contract_spec_from_payload(
        self,
        *,
        asset: str,
        epic: str,
        payload: Optional[Dict[str, Any]],
    ) -> Optional[BrokerContractSpec]:
        spec_payload = payload
        if not spec_payload:
            return None
        return BrokerContractSpec(
            broker="ig",
            asset=asset,
            symbol=str(spec_payload.get("symbol") or epic),
            point_size=_safe_float(spec_payload.get("point_size"), 0.0),
            cash_per_point_per_size=_safe_float(spec_payload.get("cash_per_point_per_size"), 0.0),
            min_size=_safe_float(spec_payload.get("min_size"), 0.0),
            size_step=_safe_float(spec_payload.get("size_step"), 0.01) or 0.01,
            currency=str(spec_payload.get("currency") or self._currency),
            source=str(spec_payload.get("source") or "ig_market_details"),
        )

    def _order_currency(self, spec: BrokerContractSpec) -> str:
        currency = str(spec.currency or "").strip().upper()
        if len(currency) == 3 and currency.isalpha():
            return currency
        return self._currency

    def _rule_points(
        self,
        spec_payload: Dict[str, Any],
        key: str,
        *,
        reference: float,
        point_size: float,
    ) -> float:
        value = _safe_float(spec_payload.get(f"{key}_value"), 0.0)
        if value <= 0:
            return 0.0
        unit = str(spec_payload.get(f"{key}_unit") or "").upper()
        if unit == "PERCENTAGE":
            if reference <= 0 or point_size <= 0:
                return 0.0
            return abs(reference * value / 100.0) / point_size
        return value

    def _apply_attached_orders(
        self,
        payload: Dict[str, Any],
        req: OrderRequest,
        spec_payload: Dict[str, Any],
        spec: BrokerContractSpec,
    ) -> Dict[str, Any]:
        side = str(req.side or "BUY").upper()
        asset = str(req.asset or req.symbol or "")
        category = str(req.category or "").strip().lower()
        point_size = _safe_float(spec.point_size, 0.0)
        bid = _safe_float(spec_payload.get("bid"), 0.0)
        offer = _safe_float(spec_payload.get("offer"), 0.0)
        reference = _safe_float(req.price, 0.0)
        if reference <= 0:
            reference = offer if side == "BUY" else bid
        if reference <= 0 and bid > 0 and offer > 0:
            reference = (bid + offer) / 2.0

        meta: Dict[str, Any] = {
            "model": "ig_distance",
            "reference_price": round(reference, 10),
            "point_size": round(point_size, 10),
            "max_stop_distance_pct": round(_max_stop_distance_pct(category), 6),
        }
        if reference <= 0 or point_size <= 0:
            meta["status"] = "skipped_missing_reference_or_point_size"
            return meta

        min_points = self._rule_points(
            spec_payload,
            "min_normal_stop_or_limit_distance",
            reference=reference,
            point_size=point_size,
        )
        max_points = self._rule_points(
            spec_payload,
            "max_stop_or_limit_distance",
            reference=reference,
            point_size=point_size,
        )
        step_points = self._rule_points(
            spec_payload,
            "min_step_distance",
            reference=reference,
            point_size=point_size,
        )
        buffer_points = max(step_points, min_points * 0.05, 2.0 if min_points > 0 else 0.0)
        required_points = min_points + buffer_points if min_points > 0 else 0.0
        meta.update(
            {
                "min_points": round(min_points, 8),
                "max_points": round(max_points, 8),
                "step_points": round(step_points, 8),
                "buffer_points": round(buffer_points, 8),
                "required_points": round(required_points, 8),
            }
        )

        decimals = max(_decimals_from_step(point_size), _safe_int(spec_payload.get("decimal_places"), 0))
        if decimals < 0 or decimals > 8:
            decimals = _decimals_from_step(point_size)

        def estimate_level(kind: str, points: float) -> float:
            distance = points * point_size
            if kind == "stop":
                level = reference - distance if side == "BUY" else reference + distance
            else:
                level = reference + distance if side == "BUY" else reference - distance
            return round(level, decimals)

        def align_points(points: float) -> float:
            value = float(points or 0.0)
            if step_points <= 0 or value <= 0:
                return value
            aligned = math.ceil((value - 1e-12) / step_points) * step_points
            if max_points > 0 and aligned > max_points:
                floored = math.floor((max_points + 1e-12) / step_points) * step_points
                aligned = floored if floored > 0 else max_points
            return float(aligned)

        def attach(kind: str, requested_level: Any) -> None:
            level = _safe_float(normalize_ig_market_price(asset, requested_level), 0.0)
            if level <= 0:
                meta[kind] = {
                    "requested_level": requested_level,
                    "status": "missing_level",
                }
                return
            valid_side = (
                (kind == "stop" and ((side == "BUY" and level < reference) or (side == "SELL" and level > reference)))
                or (kind == "limit" and ((side == "BUY" and level > reference) or (side == "SELL" and level < reference)))
            )
            if not valid_side:
                meta[kind] = {
                    "requested_level": level,
                    "reference_price": round(reference, 10),
                    "status": "rejected_invalid_side",
                }
                meta["fatal_error"] = (
                    f"{kind} is on the wrong side for {asset} {side}: "
                    f"level={level:.10f} reference={reference:.10f}"
                )
                return
            raw_points = abs(reference - level) / point_size if valid_side else 0.0
            if kind == "stop" and valid_side:
                max_stop_distance = abs(reference) * _max_stop_distance_pct(category)
                requested_distance = abs(reference - level)
                if max_stop_distance > 0 and requested_distance > max_stop_distance:
                    meta[kind] = {
                        "requested_level": level,
                        "requested_points": round(raw_points, 8),
                        "requested_distance": round(requested_distance, 8),
                        "max_distance": round(max_stop_distance, 8),
                        "status": "rejected_requested_stop_too_wide",
                    }
                    meta["fatal_error"] = (
                        f"requested stop distance too wide for {asset}: "
                        f"{requested_distance:.6f} > {max_stop_distance:.6f}"
                    )
                    return
            points = raw_points
            if required_points > 0 and points < required_points:
                points = required_points
            if max_points > 0 and points > max_points:
                points = max_points
            points = align_points(points)
            if points <= 0:
                meta[kind] = {
                    "requested_level": level,
                    "status": "skipped_invalid_side_or_distance",
                }
                return
            if kind == "stop" and required_points > raw_points and raw_points > 0:
                widened_distance = points * point_size
                max_stop_distance = abs(reference) * _max_stop_distance_pct(category)
                if max_stop_distance > 0 and widened_distance > max_stop_distance:
                    meta[kind] = {
                        "requested_level": level,
                        "requested_points": round(raw_points, 8),
                        "distance_points": round(points, 8),
                        "estimated_level": estimate_level(kind, points),
                        "max_distance": round(max_stop_distance, 8),
                        "status": "rejected_broker_min_stop_too_wide",
                    }
                    meta["fatal_error"] = (
                        f"broker minimum stop distance too wide for {asset}: "
                        f"{widened_distance:.6f} > {max_stop_distance:.6f}"
                    )
                    return
            payload_key = "stopDistance" if kind == "stop" else "limitDistance"
            payload[payload_key] = round(points, 8)
            meta[kind] = {
                "requested_level": level,
                "requested_points": round(raw_points, 8),
                "distance_points": round(points, 8),
                "estimated_level": estimate_level(kind, points),
                "adjusted": bool(abs(points - raw_points) > 1e-9),
            }

        attach("stop", req.stop_loss)
        attach("limit", req.take_profit)
        if "fatal_error" not in meta and "stopDistance" not in payload:
            meta["fatal_error"] = f"attached stop missing for {asset}; broker order was not opened unprotected"
        if "fatal_error" not in meta and "limitDistance" not in payload:
            meta["fatal_error"] = f"attached limit missing for {asset}; broker order was not opened without a broker TP"
        meta["status"] = "attached" if ("stopDistance" in payload and "limitDistance" in payload) else "none"
        return meta

    def _confirm(self, deal_reference: str) -> Dict[str, Any]:
        deadline = time.time() + 15.0
        last: Dict[str, Any] = {}
        while time.time() < deadline:
            try:
                last = self._bridge._request(
                    "GET",
                    _CONFIRM_ENDPOINT.format(deal_reference=deal_reference),
                    version="1",
                )
                status = str(last.get("dealStatus") or "").upper() if isinstance(last, dict) else ""
                if last and status not in {"", "UNKNOWN"}:
                    return last
            except IGRequestError as exc:
                if "not_found" not in exc.code.lower() and "error" not in exc.code.lower():
                    raise
            time.sleep(0.35)
        if isinstance(last, dict):
            last = dict(last)
            last["_confirm_pending"] = True
        return last

    @staticmethod
    def _is_permission_error(error: str) -> bool:
        text = str(error or "").lower()
        return any(
            phrase in text
            for phrase in (
                "unauthorised access",
                "unauthorized access",
                "no access to the relevant exchange",
                "apiuser has no access",
                "permission denied",
                "account not enabled",
            )
        )

    def _cache_alt_crypto_denial(self, error: str) -> None:
        if "cryp_cfd_alt" not in str(error or "").lower():
            return
        reason = (
            "broker_permission_denied: IG account has no access to CRYP_CFD_ALT "
            f"({error})"
        )
        for asset in _ALT_CRYPTO_ASSETS:
            self._unsupported_assets[asset] = reason

    @staticmethod
    def _confirm_reject_message(confirm: Dict[str, Any]) -> str:
        deal_status = str(confirm.get("dealStatus") or "").upper()
        fields = []
        for key in ("reason", "status", "errorCode", "dealId", "dealReference"):
            value = confirm.get(key)
            if value not in (None, ""):
                fields.append(f"{key}={value}")
        details = " ".join(fields).strip()
        if details:
            return f"IG rejected order: dealStatus={deal_status or 'UNKNOWN'} {details}"
        return f"IG rejected order: dealStatus={deal_status or 'UNKNOWN'}"

    def _build_trade(
        self,
        req: OrderRequest,
        asset: str,
        category: str,
        epic: str,
        deal_id: str,
        deal_reference: str,
        fill_price: float,
        broker_sizing: Dict[str, Any],
        attached_orders: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        local_size = float(req.local_quantity or req.quantity or 0.0)
        try:
            local_lots = PositionSizer.lots_from_size(asset, category, local_size)
        except Exception:
            local_lots = 0.0
        metadata = dict(req.metadata or {})
        metadata["broker_execution"] = {
            "broker": "ig",
            "environment": self._environment,
            "epic": epic,
            "deal_id": deal_id,
            "deal_reference": deal_reference,
            "display_entry_price": fill_price,
            "broker_sizing": broker_sizing,
            "attached_orders": dict(attached_orders or {}),
        }
        stop_loss = _safe_float((attached_orders or {}).get("stop", {}).get("estimated_level"), req.stop_loss or 0.0)
        take_profit = _safe_float((attached_orders or {}).get("limit", {}).get("estimated_level"), req.take_profit or 0.0)
        return {
            "trade_id": deal_id,
            "asset": asset,
            "canonical_asset": asset,
            "category": category,
            "signal": req.side.upper(),
            "direction": req.side.upper(),
            "confidence": _safe_float(metadata.get("confidence"), 0.0),
            "entry_price": fill_price,
            "stop_loss": stop_loss,
            "original_sl": stop_loss,
            "take_profit": take_profit,
            "original_take_profit": take_profit,
            "take_profit_levels": metadata.get("take_profit_levels", []),
            "position_size": local_size,
            "initial_position_size": local_size,
            "broker_position_size": broker_sizing.get("broker_size", 0.0),
            "broker": "ig",
            "execution_mode": f"ig_{self._environment}",
            "broker_symbol": epic,
            "broker_trade_id": deal_id,
            "broker_deal_reference": deal_reference,
            "lot_size": round(float(local_lots or 0.0), 6),
            "strategy_id": metadata.get("strategy_id", "UNKNOWN"),
            "open_time": datetime.now(timezone.utc).isoformat(),
            "pnl": 0.0,
            "highest_price": fill_price,
            "lowest_price": fill_price,
            "tp_hit": 0,
            "risk_reward": 0.0,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "requested_entry_price": req.price or fill_price,
            "metadata": metadata,
        }

    def _cancel_order(self, order_id: str) -> bool:
        return False

    def _get_balance(self, currency: str) -> float:
        summary = self._bridge.get_account_balance_summary()
        if not summary.get("authenticated"):
            return 0.0
        requested = str(currency or self._currency or "").upper()
        account_currency = str(summary.get("currency") or "").upper()
        if requested and account_currency and requested != account_currency:
            logger.debug(
                f"[IGAdapter] requested balance currency {requested} differs from IG account {account_currency}"
            )
        return _safe_float(summary.get("balance"), 0.0)

    @staticmethod
    def _broker_sizing_payload(position: Dict[str, Any]) -> Dict[str, Any]:
        metadata = position.get("metadata") if isinstance(position.get("metadata"), dict) else {}
        broker_execution = metadata.get("broker_execution") if isinstance(metadata.get("broker_execution"), dict) else {}
        broker_sizing = broker_execution.get("broker_sizing") if isinstance(broker_execution.get("broker_sizing"), dict) else {}
        if not broker_sizing and isinstance(metadata.get("broker_sizing"), dict):
            broker_sizing = metadata.get("broker_sizing") or {}
        return dict(broker_sizing or {})

    @classmethod
    def _position_broker_size(cls, position: Dict[str, Any]) -> float:
        broker_size = _safe_float(position.get("broker_position_size"), 0.0)
        if broker_size > 0:
            return broker_size
        return _safe_float(cls._broker_sizing_payload(position).get("broker_size"), 0.0)

    @staticmethod
    def _round_broker_size_down(value: float, step: float) -> float:
        step = _safe_float(step, 0.0)
        if step <= 0:
            return round(float(value or 0.0), 8)
        return round(math.floor((float(value or 0.0) + 1e-12) / step) * step, 8)

    def _close_position_with_size(
        self,
        position: Dict[str, Any],
        *,
        broker_size: float,
        local_filled_qty: float,
        reason: str,
    ) -> OrderResult:
        if str(EXECUTION_ROLE or "trader").lower() != "trader":
            return OrderResult(order_id="", status="FAILED", error="execution role is read-only")

        deal_id = str(position.get("broker_trade_id") or position.get("trade_id") or "")
        epic = str(position.get("broker_symbol") or "")
        direction = str(position.get("direction") or position.get("signal") or "BUY").upper()
        close_direction = "SELL" if direction == "BUY" else "BUY"
        broker_size = _safe_float(broker_size, 0.0)
        if not deal_id or broker_size <= 0:
            return OrderResult(order_id="", status="FAILED", error="IG close missing deal id or broker size")

        payload: Dict[str, Any] = {
            "dealId": deal_id,
            "direction": close_direction,
            "orderType": "MARKET",
            "size": broker_size,
        }
        if not deal_id and epic:
            payload["epic"] = epic
            payload["expiry"] = "-"

        if self._dry_run:
            return OrderResult(
                order_id="",
                status="FAILED",
                error="IG execution dry-run is enabled; close was not sent",
                raw={"request": payload, "reason": reason},
            )

        try:
            ack = self._bridge._request("DELETE", _POSITIONS_OTC, json_body=payload, version="1")
        except IGRequestError:
            ack = self._bridge._request(
                "POST",
                _POSITIONS_OTC,
                json_body=payload,
                version="1",
                extra_headers={"_method": "DELETE"},
            )

        deal_reference = str(ack.get("dealReference") or "")
        confirm = self._confirm(deal_reference) if deal_reference else {}
        deal_status = str(confirm.get("dealStatus") or "").upper()
        if deal_status and deal_status != "ACCEPTED":
            return OrderResult(
                order_id=deal_id,
                status="FAILED",
                error=f"IG rejected close: {confirm.get('reason') or deal_status}",
                raw={"request": payload, "confirm": confirm},
            )
        asset = str(position.get("asset") or position.get("canonical_asset") or "")
        raw_close_price = _safe_float(confirm.get("level"), 0.0)
        if raw_close_price <= 0:
            raw_close_price = _safe_float(position.get("current_price"), 0.0) or _safe_float(position.get("entry_price"), 0.0)
        close_price = _safe_float(normalize_ig_market_price(asset, raw_close_price), raw_close_price)
        return OrderResult(
            order_id=deal_id,
            status="FILLED",
            filled_qty=_safe_float(local_filled_qty, 0.0),
            avg_price=close_price,
            raw={
                "request": payload,
                "confirm": confirm,
                "reason": reason,
                "broker_close_price": raw_close_price,
                "display_close_price": close_price,
                "broker_close_size": broker_size,
            },
        )

    def close_position(self, position: Dict[str, Any], *, reason: str = "Manual Close") -> OrderResult:
        broker_size = self._position_broker_size(position)
        return self._close_position_with_size(
            position,
            broker_size=broker_size,
            local_filled_qty=_safe_float(position.get("position_size"), 0.0),
            reason=reason,
        )

    def partial_close_position(
        self,
        position: Dict[str, Any],
        *,
        local_close_size: float,
        reason: str = "Partial Close",
    ) -> OrderResult:
        total_local_size = _safe_float(position.get("position_size"), 0.0)
        requested_local_size = _safe_float(local_close_size, 0.0)
        broker_total_size = self._position_broker_size(position)
        if total_local_size <= 0 or requested_local_size <= 0 or broker_total_size <= 0:
            return OrderResult(order_id="", status="FAILED", error="IG partial close missing local or broker size")

        fraction = min(1.0, max(0.0, requested_local_size / total_local_size))
        broker_sizing = self._broker_sizing_payload(position)
        step = _safe_float(broker_sizing.get("broker_size_step"), 0.01) or 0.01
        min_size = _safe_float(broker_sizing.get("broker_min_size"), 0.0)
        raw_broker_close_size = broker_total_size * fraction
        broker_close_size = self._round_broker_size_down(raw_broker_close_size, step)
        if min_size > 0 and raw_broker_close_size > 0 and broker_close_size < min_size < broker_total_size:
            broker_close_size = min_size
        if broker_close_size <= 0 or (min_size > 0 and broker_close_size < min_size):
            return OrderResult(
                order_id=str(position.get("broker_trade_id") or position.get("trade_id") or ""),
                status="FAILED",
                error=(
                    f"IG partial size rejected for {position.get('asset')}: "
                    f"broker_partial_below_min_size raw={raw_broker_close_size:.8f} min={min_size:.8f}"
                ),
                raw={
                    "broker_total_size": broker_total_size,
                    "requested_local_size": requested_local_size,
                    "total_local_size": total_local_size,
                    "fraction": fraction,
                    "broker_size_step": step,
                    "broker_min_size": min_size,
                },
            )
        actual_local_close_size = min(total_local_size, total_local_size * (broker_close_size / broker_total_size))

        return self._close_position_with_size(
            position,
            broker_size=broker_close_size,
            local_filled_qty=actual_local_close_size,
            reason=reason,
        )

    def update_position_stop(
        self,
        position: Dict[str, Any],
        *,
        stop_level: float,
        reason: str = "Managed Stop Update",
    ) -> OrderResult:
        if str(EXECUTION_ROLE or "trader").lower() != "trader":
            return OrderResult(order_id="", status="FAILED", error="execution role is read-only")

        self._rate_limiter.acquire()
        deal_id = str(position.get("broker_trade_id") or position.get("trade_id") or "").strip()
        asset = str(position.get("asset") or position.get("canonical_asset") or "").strip()
        display_stop = _safe_float(stop_level, 0.0)
        broker_stop = round(_safe_float(denormalize_ig_market_price(asset, display_stop), display_stop), 10)
        if not deal_id or broker_stop <= 0:
            return OrderResult(order_id=deal_id, status="FAILED", error="IG stop update missing deal id or stop level")

        payload: Dict[str, Any] = {
            "guaranteedStop": False,
            "stopLevel": broker_stop,
            "trailingStop": False,
        }

        if self._dry_run:
            return OrderResult(
                order_id=deal_id,
                status="FAILED",
                error="IG execution dry-run is enabled; stop update was not sent",
                raw={
                    "request": payload,
                    "reason": reason,
                    "display_stop_level": display_stop,
                    "broker_stop_level": broker_stop,
                },
            )

        try:
            ack = self._bridge._request("PUT", f"{_POSITIONS_OTC}/{deal_id}", json_body=payload, version="2")
            deal_reference = str(ack.get("dealReference") or "")
            confirm = self._confirm(deal_reference) if deal_reference else {}
        except IGRequestError as exc:
            return OrderResult(
                order_id=deal_id,
                status="FAILED",
                error=f"{exc.code}: {exc.message}",
                raw={
                    "request": payload,
                    "reason": reason,
                    "display_stop_level": display_stop,
                    "broker_stop_level": broker_stop,
                },
            )

        deal_status = str(confirm.get("dealStatus") or "").upper()
        if confirm.get("_confirm_pending"):
            return OrderResult(
                order_id=deal_id,
                status="FAILED",
                error=f"ig_confirm_pending: IG did not finalize stop update for {deal_id}",
                raw={
                    "request": payload,
                    "confirm": confirm,
                    "reason": reason,
                    "display_stop_level": display_stop,
                    "broker_stop_level": broker_stop,
                },
            )
        if deal_status and deal_status != "ACCEPTED":
            return OrderResult(
                order_id=deal_id,
                status="FAILED",
                error=f"IG rejected stop update: {confirm.get('reason') or deal_status}",
                raw={
                    "request": payload,
                    "confirm": confirm,
                    "reason": reason,
                    "display_stop_level": display_stop,
                    "broker_stop_level": broker_stop,
                },
            )

        return OrderResult(
            order_id=deal_id,
            status="FILLED",
            filled_qty=0.0,
            avg_price=display_stop,
            raw={
                "request": payload,
                "confirm": confirm,
                "reason": reason,
                "display_stop_level": display_stop,
                "broker_stop_level": broker_stop,
            },
        )

    def _get_order_status(self, order_id: str) -> Optional[OrderResult]:
        return None

    def _get_balance(self, currency: str) -> float:
        summary = self._bridge.get_account_balance_summary()
        if not summary.get("authenticated"):
            return 0.0
        requested = str(currency or self._currency or "").upper()
        account_currency = str(summary.get("currency") or "").upper()
        if requested and account_currency and requested != account_currency:
            logger.debug(
                f"[IGAdapter] requested balance currency {requested} differs from IG account {account_currency}"
            )
        return _safe_float(summary.get("balance"), 0.0)

    def _get_orderbook(self, symbol: str, depth: int) -> Optional[OrderBookSnapshot]:
        return None

    def list_open_positions(self) -> list[Dict[str, Any]]:
        payload = self._bridge._request("GET", _POSITIONS_ENDPOINT, version="2")
        positions = payload.get("positions") if isinstance(payload, dict) else []
        return [dict(item) for item in positions or [] if isinstance(item, dict)]
