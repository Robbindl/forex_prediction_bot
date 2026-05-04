from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.assets import registry
from risk.broker_sizer import BrokerContractSpec, BrokerPositionSizer
from risk.manager import RiskManager
from risk.position_sizer import PositionSizer
from services.ig_market_bridge import IGMarketBridge, IGRequestError


_REFERENCE_PRICES = {
    "EUR/USD": 1.10,
    "EUR/JPY": 170.0,
    "EUR/GBP": 0.86,
    "GBP/JPY": 195.0,
    "GBP/USD": 1.27,
    "AUD/USD": 0.66,
    "NZD/USD": 0.60,
    "USD/JPY": 155.0,
    "USD/CAD": 1.36,
    "USD/CHF": 0.90,
    "US30": 39000.0,
    "US100": 18000.0,
    "US500": 5200.0,
    "UK100": 8200.0,
    "GER40": 18500.0,
    "AUS200": 7800.0,
    "JPN225": 39000.0,
    "XAU/USD": 2300.0,
    "XAG/USD": 29.0,
    "WTI": 78.0,
    "BTC-USD": 78000.0,
    "ETH-USD": 3900.0,
    "BNB-USD": 620.0,
    "SOL-USD": 150.0,
    "XRP-USD": 0.55,
}


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _asset_list(raw_assets: Iterable[str]) -> List[str]:
    if raw_assets:
        return [registry.canonical(item) for item in raw_assets if item]
    return [asset for asset, _category in registry.all_assets()]


def _account_balance(bridge: IGMarketBridge, explicit: float) -> float:
    if explicit > 0:
        return explicit
    summary = bridge.get_account_summary()
    for key in ("available", "balance"):
        value = _safe_float(summary.get(key), 0.0)
        if value > 0:
            return value
    return 10_000.0


def _entry_price(bridge: IGMarketBridge, asset: str, category: str) -> tuple[float, str, Dict[str, Any]]:
    try:
        price, _spread, metadata = bridge.get_quote(asset, category=category)
    except IGRequestError as exc:
        return float(_REFERENCE_PRICES.get(asset, 1.0)), "reference_after_ig_error", {
            "provider_error_code": exc.code,
            "provider_error_message": exc.message,
        }
    except Exception as exc:
        return float(_REFERENCE_PRICES.get(asset, 1.0)), "reference_after_network_error", {
            "provider_error_code": "ig_network_error",
            "provider_error_message": str(exc),
        }
    price_value = _safe_float(price, 0.0)
    if price_value > 0:
        return price_value, "ig_quote", metadata
    return float(_REFERENCE_PRICES.get(asset, 1.0)), "reference_fallback", metadata or {}


def _broker_spec_from_payload(asset: str, payload: Optional[Dict[str, Any]]) -> Optional[BrokerContractSpec]:
    if not payload:
        return None
    return BrokerContractSpec(
        broker="ig",
        asset=asset,
        symbol=str(payload.get("symbol") or ""),
        point_size=_safe_float(payload.get("point_size"), 0.0),
        cash_per_point_per_size=_safe_float(payload.get("cash_per_point_per_size"), 0.0),
        min_size=_safe_float(payload.get("min_size"), 0.0),
        size_step=_safe_float(payload.get("size_step"), 0.01) or 0.01,
        currency=str(payload.get("currency") or "USD"),
        source=str(payload.get("source") or "ig_market_details"),
    )


def _audit_asset(
    bridge: IGMarketBridge,
    asset: str,
    *,
    balance: float,
    confidence: float,
    risk_multiplier: float,
) -> Dict[str, Any]:
    category = registry.category(asset)
    if category == "unknown":
        return {"asset": asset, "status": "unknown_asset"}

    entry, price_source, quote_meta = _entry_price(bridge, asset, category)
    risk = RiskManager(account_balance=balance)
    stop = risk.get_stop_loss(entry, "BUY", category)
    local_size = risk.calculate_position_size(
        entry_price=entry,
        stop_loss=stop,
        category=category,
        confidence=confidence,
        asset=asset,
        risk_multiplier=risk_multiplier,
    )
    local_profile = PositionSizer.cash_profile(asset, category, local_size)

    try:
        spec_payload = bridge.get_contract_spec(asset, category=category)
    except IGRequestError as exc:
        return {
            "asset": asset,
            "category": category,
            "status": "ig_api_error",
            "entry_price": round(entry, 8),
            "price_source": price_source,
            "local_lots": local_profile["lots"],
            "local_cash_per_price_unit": local_profile["cash_per_price_unit"],
            "ig_error": exc.code,
            "ig_error_message": exc.message,
        }
    except Exception as exc:
        return {
            "asset": asset,
            "category": category,
            "status": "ig_network_error",
            "entry_price": round(entry, 8),
            "price_source": price_source,
            "local_lots": local_profile["lots"],
            "local_cash_per_price_unit": local_profile["cash_per_price_unit"],
            "ig_error": "ig_network_error",
            "ig_error_message": str(exc),
        }
    broker_spec = _broker_spec_from_payload(asset, spec_payload)
    if broker_spec is None:
        return {
            "asset": asset,
            "category": category,
            "status": "missing_ig_contract_spec",
            "entry_price": round(entry, 8),
            "price_source": price_source,
            "local_lots": local_profile["lots"],
            "local_cash_per_price_unit": local_profile["cash_per_price_unit"],
            "ig_error": quote_meta.get("provider_error_code") or quote_meta.get("provider_error_message"),
        }

    converted = BrokerPositionSizer.convert(
        asset=asset,
        category=category,
        local_size=local_size,
        broker_spec=broker_spec,
    )
    return {
        "asset": asset,
        "category": category,
        "status": "tradeable_size" if converted.accepted else converted.reason,
        "entry_price": round(entry, 8),
        "price_source": price_source,
        "ig_epic": broker_spec.symbol,
        "ig_market_status": (spec_payload or {}).get("market_status"),
        "ig_min_size": converted.broker_min_size,
        "ig_size_step": converted.broker_size_step,
        "ig_point_size": converted.broker_point_size,
        "ig_cash_per_point_per_1_size": converted.broker_cash_per_point_per_size,
        "local_lots": converted.local_lots,
        "local_cash_per_pip": converted.local_cash_per_pip,
        "local_cash_per_price_unit": converted.local_cash_per_price_unit,
        "ig_raw_size": converted.broker_raw_size,
        "ig_order_size": converted.broker_size,
    }


def _print_table(rows: List[Dict[str, Any]], *, balance: float) -> None:
    print(f"IG contract audit | balance=${balance:,.2f}")
    print(
        "asset       cat          status                    "
        "local_lots  $/unit      ig_size    ig_min   epic"
    )
    print("-" * 112)
    for row in rows:
        print(
            f"{str(row.get('asset','')):<11} "
            f"{str(row.get('category','')):<12} "
            f"{str(row.get('status','')):<25} "
            f"{_safe_float(row.get('local_lots')):>10.5f} "
            f"{_safe_float(row.get('local_cash_per_price_unit')):>9.4f} "
            f"{_safe_float(row.get('ig_order_size')):>10.5f} "
            f"{_safe_float(row.get('ig_min_size')):>8.4f} "
            f"{str(row.get('ig_epic') or '')}"
        )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Audit IG contract sizing against the bot's JustMarkets-style account-scaled exposure.",
    )
    parser.add_argument("--assets", nargs="*", default=[], help="Canonical assets to audit. Default: all bot assets.")
    parser.add_argument("--balance", type=float, default=0.0, help="Override account balance. Default: IG available balance.")
    parser.add_argument("--confidence", type=float, default=0.70, help="Sizing confidence used for the audit.")
    parser.add_argument("--risk-multiplier", type=float, default=1.0, help="Sizing risk multiplier used for the audit.")
    parser.add_argument("--sleep", type=float, default=2.5, help="Seconds to wait between assets to respect IG API limits.")
    parser.add_argument("--json", action="store_true", help="Print JSON instead of a table.")
    args = parser.parse_args()

    bridge = IGMarketBridge()
    balance = _account_balance(bridge, args.balance)
    rows = []
    assets = _asset_list(args.assets)
    for idx, asset in enumerate(assets):
        rows.append(
            _audit_asset(
            bridge,
            asset,
            balance=balance,
            confidence=float(args.confidence),
            risk_multiplier=float(args.risk_multiplier),
        )
        )
        if args.sleep > 0 and idx < len(assets) - 1:
            time.sleep(float(args.sleep))
    if args.json:
        print(json.dumps({"balance": balance, "rows": rows}, indent=2, sort_keys=True))
    else:
        _print_table(rows, balance=balance)
    failures = [row for row in rows if row.get("status") not in {"tradeable_size"}]
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
