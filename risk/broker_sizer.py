from __future__ import annotations

import math
import os
import re
from dataclasses import asdict, dataclass
from typing import Any, Dict, Mapping, Optional

from risk.position_sizer import PositionSizer


@dataclass(frozen=True)
class BrokerContractSpec:
    broker: str
    asset: str
    point_size: float
    cash_per_point_per_size: float
    min_size: float = 0.0
    size_step: float = 0.01
    currency: str = "USD"
    source: str = "manual"
    symbol: str = ""

    @property
    def cash_per_price_unit_per_size(self) -> float:
        if self.point_size <= 0:
            return 0.0
        return self.cash_per_point_per_size / self.point_size


@dataclass(frozen=True)
class BrokerSizingResult:
    asset: str
    category: str
    broker: str
    accepted: bool
    reason: str
    local_size: float
    local_lots: float
    local_pip_size: float
    local_cash_per_pip: float
    local_cash_per_price_unit: float
    broker_raw_size: float
    broker_size: float
    broker_min_size: float
    broker_size_step: float
    broker_point_size: float
    broker_cash_per_point_per_size: float
    broker_cash_per_price_unit_per_size: float
    broker_min_size_upscale: float = 0.0
    broker_symbol: str = ""
    broker_spec_source: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


_NUMBER_RE = re.compile(r"[-+]?\d+(?:,\d{3})*(?:\.\d+)?")
_MONEY_RE = re.compile(r"\(\s*[$€£]\s*(\d+(?:,\d{3})*(?:\.\d+)?)\s*\)")
_CURRENCY_RE = re.compile(r"['\"]code['\"]\s*:\s*['\"]([A-Z]{3})['\"]")


def _safe_float(value: Any, default: float = 0.0) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        match = _NUMBER_RE.search(value.replace("\u00a0", " "))
        if match:
            try:
                return float(match.group(0).replace(",", ""))
            except Exception:
                return float(default)
    return float(default)


def _nested_value(payload: Mapping[str, Any], *path: str) -> Any:
    cur: Any = payload
    for key in path:
        if not isinstance(cur, Mapping):
            return None
        cur = cur.get(key)
    return cur


def _round_down_to_step(value: float, step: float) -> float:
    step = float(step or 0.0)
    if step <= 0:
        return round(float(value or 0.0), 8)
    return round(math.floor((float(value or 0.0) + 1e-12) / step) * step, 8)


def _round_up_to_step(value: float, step: float) -> float:
    step = float(step or 0.0)
    if step <= 0:
        return round(float(value or 0.0), 8)
    return round(math.ceil((float(value or 0.0) - 1e-12) / step) * step, 8)


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return float(default)


def _below_min_mode() -> str:
    configured = os.getenv("BROKER_BELOW_MIN_SIZE_MODE", "").strip().lower()
    if configured:
        return configured
    execution_mode = os.getenv("EXECUTION_MODE", "").strip().lower()
    return "floor" if execution_mode == "ig_demo" else "reject"


def _below_min_max_upscale() -> float:
    execution_mode = os.getenv("EXECUTION_MODE", "").strip().lower()
    default = 50.0 if execution_mode == "ig_demo" else 3.0
    return max(1.0, _env_float("BROKER_BELOW_MIN_SIZE_MAX_UPSCALE", default))


def _asset_category(asset: str) -> str:
    if asset in {"XAU/USD", "XAG/USD", "WTI", "GC=F", "SI=F", "CL=F"}:
        return "commodities"
    if asset in {"US30", "US100", "US500", "UK100", "GER40", "AUS200", "JPN225"}:
        return "indices"
    if str(asset or "").endswith("-USD"):
        return "crypto"
    return "forex"


def _money_value_from_name(name: str) -> float:
    match = _MONEY_RE.search(str(name or "").replace("\u00a0", " "))
    if not match:
        return 0.0
    try:
        return float(match.group(1).replace(",", ""))
    except Exception:
        return 0.0


def _currency_code(value: Any, default: str = "USD") -> str:
    if isinstance(value, list):
        for item in value:
            if isinstance(item, Mapping) and item.get("isDefault") and item.get("code"):
                return str(item.get("code")).upper()
        for item in value:
            if isinstance(item, Mapping) and item.get("code"):
                return str(item.get("code")).upper()
    if isinstance(value, Mapping) and value.get("code"):
        return str(value.get("code")).upper()
    text = str(value or "").strip()
    if len(text) == 3 and text.isalpha():
        return text.upper()
    match = _CURRENCY_RE.search(text)
    if match:
        return match.group(1).upper()
    return str(default or "USD").upper()


def _fallback_ig_point_size(asset: str, category: str, local_pip: float, name: str) -> float:
    if category == "forex":
        return float(local_pip or 0.0)
    if category in {"indices", "crypto"}:
        return 1.0
    upper_name = str(name or "").upper()
    if asset in {"XAG/USD", "SI=F", "WTI", "WTI/USD", "CL=F"}:
        return 0.01
    if asset in {"XAU/USD", "GC=F"} or "$1" in upper_name:
        return 1.0
    return float(local_pip or 1.0)


class BrokerPositionSizer:
    """
    Converts the bot's internal JustMarkets/MT-style exposure into a broker's
    native deal size by matching cash P&L per price move.

    The internal position size stays account-scaled by PositionSizer.  This
    class only translates that already-sized exposure into the number the
    target broker expects.
    """

    @staticmethod
    def convert(
        *,
        asset: str,
        category: str,
        local_size: float,
        broker_spec: BrokerContractSpec,
    ) -> BrokerSizingResult:
        local_profile = PositionSizer.cash_profile(asset, category, local_size)
        local_cash_per_price_unit = float(local_profile["cash_per_price_unit"])
        local_cash_per_pip = float(local_profile["cash_per_pip"])
        local_lots = float(local_profile["lots"])
        local_pip = float(local_profile["pip_size"])

        broker_cash_per_price_unit = broker_spec.cash_per_price_unit_per_size
        if local_size <= 0:
            return BrokerPositionSizer._result(
                asset=asset,
                category=category,
                spec=broker_spec,
                accepted=False,
                reason="local_size_zero",
                local_size=local_size,
                local_lots=local_lots,
                local_pip=local_pip,
                local_cash_per_pip=local_cash_per_pip,
                local_cash_per_price_unit=local_cash_per_price_unit,
                broker_raw_size=0.0,
                broker_size=0.0,
            )
        if broker_cash_per_price_unit <= 0:
            return BrokerPositionSizer._result(
                asset=asset,
                category=category,
                spec=broker_spec,
                accepted=False,
                reason="broker_cash_per_move_missing",
                local_size=local_size,
                local_lots=local_lots,
                local_pip=local_pip,
                local_cash_per_pip=local_cash_per_pip,
                local_cash_per_price_unit=local_cash_per_price_unit,
                broker_raw_size=0.0,
                broker_size=0.0,
            )

        raw_size = local_cash_per_price_unit / broker_cash_per_price_unit
        rounded_size = _round_down_to_step(raw_size, broker_spec.size_step)
        min_size = max(0.0, float(broker_spec.min_size or 0.0))
        accepted = rounded_size > 0 and (min_size <= 0 or rounded_size >= min_size)
        reason = "ok" if accepted else "below_broker_min_size"
        min_size_upscale = 0.0
        broker_size = rounded_size if accepted else 0.0
        if not accepted and raw_size > 0 and min_size > 0:
            min_floor = max(min_size, _round_up_to_step(min_size, broker_spec.size_step))
            min_size_upscale = min_floor / max(raw_size, 1e-12)
            if _below_min_mode() in {"floor", "min", "broker_min", "broker_minimum"} and min_size_upscale <= _below_min_max_upscale():
                accepted = True
                reason = "broker_min_size_floor"
                broker_size = min_floor
        return BrokerPositionSizer._result(
            asset=asset,
            category=category,
            spec=broker_spec,
            accepted=accepted,
            reason=reason,
            local_size=local_size,
            local_lots=local_lots,
            local_pip=local_pip,
            local_cash_per_pip=local_cash_per_pip,
            local_cash_per_price_unit=local_cash_per_price_unit,
            broker_raw_size=raw_size,
            broker_size=broker_size,
            broker_min_size_upscale=min_size_upscale,
        )

    @staticmethod
    def _result(
        *,
        asset: str,
        category: str,
        spec: BrokerContractSpec,
        accepted: bool,
        reason: str,
        local_size: float,
        local_lots: float,
        local_pip: float,
        local_cash_per_pip: float,
        local_cash_per_price_unit: float,
        broker_raw_size: float,
        broker_size: float,
        broker_min_size_upscale: float = 0.0,
    ) -> BrokerSizingResult:
        return BrokerSizingResult(
            asset=asset,
            category=category,
            broker=spec.broker,
            accepted=accepted,
            reason=reason,
            local_size=round(float(local_size or 0.0), 8),
            local_lots=round(float(local_lots or 0.0), 8),
            local_pip_size=round(float(local_pip or 0.0), 10),
            local_cash_per_pip=round(float(local_cash_per_pip or 0.0), 8),
            local_cash_per_price_unit=round(float(local_cash_per_price_unit or 0.0), 8),
            broker_raw_size=round(float(broker_raw_size or 0.0), 8),
            broker_size=round(float(broker_size or 0.0), 8),
            broker_min_size=round(float(spec.min_size or 0.0), 8),
            broker_size_step=round(float(spec.size_step or 0.0), 8),
            broker_point_size=round(float(spec.point_size or 0.0), 10),
            broker_cash_per_point_per_size=round(float(spec.cash_per_point_per_size or 0.0), 8),
            broker_cash_per_price_unit_per_size=round(
                float(spec.cash_per_price_unit_per_size or 0.0),
                8,
            ),
            broker_min_size_upscale=round(float(broker_min_size_upscale or 0.0), 8),
            broker_symbol=spec.symbol,
            broker_spec_source=spec.source,
        )

    @staticmethod
    def annotate_local(signal: Dict[str, Any], *, account_balance: Optional[float] = None) -> Dict[str, Any]:
        asset = str(signal.get("asset") or "")
        category = str(signal.get("category") or "forex")
        local_size = _safe_float(signal.get("position_size"), 0.0)
        profile = PositionSizer.cash_profile(asset, category, local_size)
        if account_balance is not None:
            profile["account_balance"] = round(float(account_balance or 0.0), 2)
            profile["account_scale"] = round(
                float(account_balance or 0.0) / max(float(profile.get("reference_balance") or 1.0), 1.0),
                8,
            )
        metadata = signal.get("metadata") if isinstance(signal.get("metadata"), dict) else {}
        signal["metadata"] = {
            **metadata,
            "execution_sizing": {
                "model": "cash_per_price_move",
                "reference_broker": "justmarkets_mt_style",
                **profile,
            },
        }
        return signal

    @staticmethod
    def apply_broker_spec(signal: Dict[str, Any], broker_spec: BrokerContractSpec) -> Dict[str, Any]:
        result = BrokerPositionSizer.convert(
            asset=str(signal.get("asset") or ""),
            category=str(signal.get("category") or "forex"),
            local_size=_safe_float(signal.get("position_size"), 0.0),
            broker_spec=broker_spec,
        )
        metadata = signal.get("metadata") if isinstance(signal.get("metadata"), dict) else {}
        signal["metadata"] = {**metadata, "broker_sizing": result.to_dict()}
        if result.accepted:
            signal["broker"] = broker_spec.broker
            signal["broker_symbol"] = broker_spec.symbol or broker_spec.asset
            signal["broker_position_size"] = result.broker_size
        return signal

    @staticmethod
    def spec_from_ig_market_details(
        *,
        asset: str,
        details: Mapping[str, Any],
        default_point_size: float = 0.0,
        default_size_step: float = 0.01,
    ) -> Optional[BrokerContractSpec]:
        instrument = details.get("instrument") if isinstance(details.get("instrument"), Mapping) else {}
        dealing_rules = details.get("dealingRules") if isinstance(details.get("dealingRules"), Mapping) else {}
        snapshot = details.get("snapshot") if isinstance(details.get("snapshot"), Mapping) else {}
        name = str(
            instrument.get("name")
            or instrument.get("instrumentName")
            or details.get("instrumentName")
            or asset
        )
        category = _asset_category(asset)
        local_spec = PositionSizer.get_spec(asset, category)
        local_pip = _safe_float(local_spec.get("pip"), 0.0)
        local_pip_value = _safe_float(local_spec.get("pip_val"), 0.0)

        point_size = _safe_float(instrument.get("onePipMeans"), 0.0)
        if point_size <= 0:
            point_size = _safe_float(instrument.get("pip"), 0.0)
        if point_size <= 0:
            fallback_point_size = _fallback_ig_point_size(asset, category, local_pip, name)
            if category != "forex" and fallback_point_size > 0:
                point_size = fallback_point_size
        if point_size <= 0:
            point_size = float(default_point_size or 0.0)
        if point_size <= 0:
            point_size = _fallback_ig_point_size(asset, category, local_pip, name)

        cash_per_point = _safe_float(instrument.get("valueOfOnePip"), 0.0)
        if cash_per_point <= 0:
            contract_size = _safe_float(instrument.get("contractSize"), 0.0)
            if contract_size > 0 and point_size > 0:
                cash_per_point = contract_size * point_size
        if cash_per_point <= 0:
            named_value = _money_value_from_name(name)
            if named_value > 0:
                cash_per_point = named_value
        if category == "forex" and local_pip_value > 0:
            if cash_per_point <= 0:
                cash_per_point = local_pip_value
            elif cash_per_point > local_pip_value * 20.0 or cash_per_point < local_pip_value / 20.0:
                cash_per_point = local_pip_value
        if cash_per_point <= 0 and category == "crypto":
            cash_per_point = 1.0
        if cash_per_point <= 0 and category in {"indices", "commodities"}:
            cash_per_point = max(1.0, local_pip_value)

        if point_size <= 0 or cash_per_point <= 0:
            return None

        min_size = _safe_float(_nested_value(dealing_rules, "minDealSize", "value"), 0.0)
        if min_size <= 0:
            min_size = _safe_float(dealing_rules.get("minDealSize"), 0.0)

        epic = str(instrument.get("epic") or details.get("epic") or snapshot.get("epic") or "")
        currency = _currency_code(
            instrument.get("currencies")
            or instrument.get("currency")
            or instrument.get("profitCurrency"),
            "USD",
        )
        return BrokerContractSpec(
            broker="ig",
            asset=asset,
            symbol=epic,
            point_size=point_size,
            cash_per_point_per_size=cash_per_point,
            min_size=min_size,
            size_step=float(default_size_step or 0.01),
            currency=currency,
            source="ig_market_details",
        )
