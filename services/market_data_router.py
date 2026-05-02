from __future__ import annotations

from functools import lru_cache
from typing import Any, Dict, Optional

from config.config import (
    IG_MAX_ROUTED_ASSETS,
    IG_ROUTE_TO_DERIV_BY_DEFAULT,
    IG_ROUTED_ASSETS,
    IG_ROUTED_CATEGORIES,
)
from core.assets import registry
from core.asset_profiles import get_profile
from services.market_hours_guard import build_market_status

_DERIV_PRIMARY_CRYPTO_ASSETS = frozenset({"BTC-USD", "ETH-USD"})


@lru_cache(maxsize=1)
def _configured_ig_routed_assets() -> frozenset[str]:
    return frozenset(
        registry.canonical(str(asset or "").strip())
        for asset in (IG_ROUTED_ASSETS or [])
        if str(asset or "").strip()
    )


def _is_configured_ig_asset(asset: str) -> bool:
    canonical = registry.canonical(str(asset or "").strip())
    return canonical in _configured_ig_routed_assets()


def is_ig_primary_category(category: str) -> bool:
    if IG_ROUTE_TO_DERIV_BY_DEFAULT:
        return False
    normalized = str(category or "").strip().lower()
    if normalized not in set(IG_ROUTED_CATEGORIES or []):
        return False
    try:
        from services.ig_market_bridge import ig_market_bridge

        return bool(ig_market_bridge.list_profiles())
    except Exception:
        return False


def is_ig_primary_asset(asset: str, category: str = "") -> bool:
    if IG_ROUTE_TO_DERIV_BY_DEFAULT:
        return False
    canonical = registry.canonical(str(asset or "").strip())
    resolved_category = str(category or get_profile(canonical).category or "").strip().lower()
    if (
        canonical not in _configured_ig_routed_assets()
        and resolved_category not in set(IG_ROUTED_CATEGORIES or [])
    ):
        return False
    return _ig_supported_asset(canonical, resolved_category)


def is_deriv_primary_crypto_asset(asset: str, category: str = "") -> bool:
    canonical = registry.canonical(str(asset or "").strip())
    resolved_category = str(category or get_profile(canonical).category or "").strip().lower()
    return resolved_category == "crypto" and canonical in _DERIV_PRIMARY_CRYPTO_ASSETS


def is_binance_primary_crypto_asset(asset: str, category: str = "") -> bool:
    canonical = registry.canonical(str(asset or "").strip())
    resolved_category = str(category or get_profile(canonical).category or "").strip().lower()
    if resolved_category != "crypto" or canonical in _DERIV_PRIMARY_CRYPTO_ASSETS:
        return False
    return is_binance_supported_crypto_asset(canonical, resolved_category)


def is_binance_supported_crypto_asset(asset: str, category: str = "") -> bool:
    canonical = registry.canonical(str(asset or "").strip())
    resolved_category = str(category or get_profile(canonical).category or "").strip().lower()
    if resolved_category != "crypto":
        return False
    try:
        from services.binance_market_bridge import binance_market_bridge

        return bool(binance_market_bridge.supports(canonical, category=resolved_category))
    except Exception:
        return False


def is_bybit_supported_commodity_asset(asset: str, category: str = "") -> bool:
    canonical = registry.canonical(str(asset or "").strip())
    resolved_category = str(category or get_profile(canonical).category or "").strip().lower()
    if resolved_category != "commodities":
        return False
    try:
        from services.bybit_market_bridge import bybit_market_bridge

        return bool(bybit_market_bridge.supports(canonical, category=resolved_category))
    except Exception:
        return False


def is_okx_supported_commodity_asset(asset: str, category: str = "") -> bool:
    canonical = registry.canonical(str(asset or "").strip())
    resolved_category = str(category or get_profile(canonical).category or "").strip().lower()
    if resolved_category != "commodities":
        return False
    try:
        from services.okx_market_bridge import okx_market_bridge

        return bool(okx_market_bridge.supports(canonical, category=resolved_category))
    except Exception:
        return False


def preferred_quote_provider_order(asset: str, category: str = "") -> tuple[str, ...]:
    canonical = registry.canonical(str(asset or "").strip())
    resolved_category = str(category or get_profile(canonical).category or "").strip().lower()

    if is_ig_primary_asset(canonical, resolved_category):
        if is_bybit_supported_commodity_asset(canonical, resolved_category):
            return ("ig", "deriv", "bybit", "okx")
        if is_okx_supported_commodity_asset(canonical, resolved_category):
            return ("ig", "deriv", "okx")
        return ("ig", "deriv")
    if is_binance_supported_crypto_asset(canonical, resolved_category):
        return ("binance", "deriv")
    if is_binance_primary_crypto_asset(canonical, resolved_category):
        return ("binance",)
    if is_deriv_primary_crypto_asset(canonical, resolved_category):
        return ("deriv", "binance")
    if resolved_category == "crypto":
        return ("deriv", "binance")
    if is_bybit_supported_commodity_asset(canonical, resolved_category):
        return ("deriv", "bybit", "okx")
    if is_okx_supported_commodity_asset(canonical, resolved_category):
        return ("deriv", "okx")
    return ("deriv",)


def _ig_supported_asset(asset: str, category: str = "") -> bool:
    canonical = registry.canonical(str(asset or "").strip())
    resolved_category = str(category or get_profile(canonical).category or "").strip().lower()
    try:
        from services.ig_market_bridge import ig_market_bridge

        return bool(ig_market_bridge.list_profiles()) and bool(
            ig_market_bridge.supports(canonical, category=resolved_category)
        )
    except Exception:
        return False


def _select_ig_primary_assets(asset_map: Dict[str, str]) -> tuple[Dict[str, str], Dict[str, str]]:
    selected: Dict[str, str] = {}
    overflow: Dict[str, str] = {}
    explicit_candidates: Dict[str, str] = {}
    category_candidates: Dict[str, str] = {}

    for asset, category in (asset_map or {}).items():
        asset_text = str(asset or "")
        category_text = str(category or "")
        if not is_ig_primary_asset(asset_text, category_text):
            continue
        target = explicit_candidates if _is_configured_ig_asset(asset_text) else category_candidates
        target[asset_text] = category_text

    candidates = {**explicit_candidates, **category_candidates}
    if IG_MAX_ROUTED_ASSETS is None or IG_MAX_ROUTED_ASSETS <= 0 or len(candidates) <= IG_MAX_ROUTED_ASSETS:
        return candidates, {}

    items = list(candidates.items())
    selected = dict(items[:IG_MAX_ROUTED_ASSETS])
    overflow = dict(items[IG_MAX_ROUTED_ASSETS:])
    return selected, overflow


def filter_deriv_stream_assets(asset_map: Dict[str, str]) -> Dict[str, str]:
    selected_ig, _ = _select_ig_primary_assets(asset_map)
    return {
        str(asset): str(category)
        for asset, category in (asset_map or {}).items()
        if str(asset) not in selected_ig
    }


def filter_ig_primary_assets(asset_map: Dict[str, str]) -> Dict[str, str]:
    selected_ig, _ = _select_ig_primary_assets(asset_map)
    return selected_ig


def split_pending_ig_fallback_assets(asset_map: Dict[str, str]) -> tuple[Dict[str, str], Dict[str, str]]:
    """Split pending IG assets into Deriv-streamable fallbacks vs true IG REST poll assets."""

    deriv_fallback_assets: Dict[str, str] = {}
    ig_poll_assets: Dict[str, str] = {}

    try:
        from services.deriv_bridge import deriv_bridge
    except Exception:
        deriv_bridge = None

    for asset, category in (asset_map or {}).items():
        asset_text = str(asset or "")
        category_text = str(category or "")
        resolved = None
        if deriv_bridge is not None:
            try:
                resolved = deriv_bridge.resolve_symbol_info(asset_text, category=category_text)
            except Exception:
                resolved = None
        if resolved and str((resolved or {}).get("symbol") or "").strip():
            deriv_fallback_assets[asset_text] = category_text
        else:
            ig_poll_assets[asset_text] = category_text

    return deriv_fallback_assets, ig_poll_assets


def get_market_status(asset: str, category: str = ""):
    resolved_category = str(category or get_profile(asset).category or "").strip().lower()

    if is_binance_supported_crypto_asset(asset, resolved_category):
        return build_market_status(
            asset,
            resolved_category,
            provider_status={
                "market_open": True,
                "reason": "crypto exchange open 24x7",
                "source": "binance",
            },
        )

    if is_ig_primary_asset(asset, resolved_category):
        try:
            from services.ig_market_bridge import ig_market_bridge

            status = ig_market_bridge.get_market_status(asset, category=resolved_category)
            if status and "market_open" in status:
                return build_market_status(asset, resolved_category, provider_status=status)
        except Exception:
            pass

    try:
        from services.deriv_bridge import deriv_bridge

        status = deriv_bridge.get_market_status(asset, category=resolved_category)
        if status and "market_open" in status:
            return build_market_status(asset, resolved_category, provider_status=status)
    except Exception:
        pass

    return None


def get_broker_account_summary() -> Dict[str, Any]:
    try:
        from services.ig_market_bridge import ig_market_bridge

        summary = ig_market_bridge.get_account_summary()
        if isinstance(summary, dict):
            return dict(summary)
    except Exception:
        pass
    return {}


def get_client_sentiment(asset: str, category: str = "") -> Optional[Dict[str, Any]]:
    resolved_category = str(category or get_profile(asset).category or "").strip().lower()
    # Only use IG client-positioning for assets that are actually routed to IG.
    # Otherwise Deriv-primary assets still burn IG allowance through sentiment.
    if not is_ig_primary_asset(asset, resolved_category):
        return None
    try:
        from services.ig_market_bridge import ig_market_bridge

        data = ig_market_bridge.get_client_sentiment(asset, category=resolved_category)
        if isinstance(data, dict) and data:
            return dict(data)
    except Exception:
        pass
    return None
