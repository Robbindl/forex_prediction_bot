from __future__ import annotations

from functools import lru_cache
from typing import Any, Dict, Optional

from config.config import IG_ROUTED_ASSETS, IG_ROUTED_CATEGORIES
from core.assets import registry
from core.asset_profiles import get_profile
from services.market_hours_guard import build_market_status


@lru_cache(maxsize=1)
def _configured_ig_routed_assets() -> frozenset[str]:
    return frozenset(
        registry.canonical(str(asset or "").strip())
        for asset in (IG_ROUTED_ASSETS or [])
        if str(asset or "").strip()
    )


def is_ig_primary_category(category: str) -> bool:
    normalized = str(category or "").strip().lower()
    if normalized not in set(IG_ROUTED_CATEGORIES or []):
        return False
    try:
        from services.ig_market_bridge import ig_market_bridge

        return bool(ig_market_bridge.list_profiles())
    except Exception:
        return False


def is_ig_primary_asset(asset: str, category: str = "") -> bool:
    canonical = registry.canonical(str(asset or "").strip())
    resolved_category = str(category or get_profile(canonical).category or "").strip().lower()
    if (
        canonical not in _configured_ig_routed_assets()
        and resolved_category not in set(IG_ROUTED_CATEGORIES or [])
    ):
        return False
    return _ig_supported_asset(canonical, resolved_category)


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


def filter_deriv_stream_assets(asset_map: Dict[str, str]) -> Dict[str, str]:
    return {
        str(asset): str(category)
        for asset, category in (asset_map or {}).items()
        if not is_ig_primary_asset(str(asset or ""), str(category or ""))
    }


def filter_ig_primary_assets(asset_map: Dict[str, str]) -> Dict[str, str]:
    return {
        str(asset): str(category)
        for asset, category in (asset_map or {}).items()
        if is_ig_primary_asset(str(asset or ""), str(category or ""))
    }


def get_market_status(asset: str, category: str = ""):
    resolved_category = str(category or get_profile(asset).category or "").strip().lower()

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
    if not _ig_supported_asset(asset, resolved_category):
        return None
    try:
        from services.ig_market_bridge import ig_market_bridge

        data = ig_market_bridge.get_client_sentiment(asset, category=resolved_category)
        if isinstance(data, dict) and data:
            return dict(data)
    except Exception:
        pass
    return None
