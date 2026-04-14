from __future__ import annotations

from typing import Any, Dict, Optional

from config.config import IG_ROUTED_CATEGORIES
from core.asset_profiles import get_profile
from services.market_hours_guard import build_market_status


def is_ig_primary_category(category: str) -> bool:
    normalized = str(category or "").strip().lower()
    if normalized not in set(IG_ROUTED_CATEGORIES or []):
        return False
    try:
        from services.ig_market_bridge import ig_market_bridge

        return bool(ig_market_bridge.list_profiles())
    except Exception:
        return False


def filter_deriv_stream_assets(asset_map: Dict[str, str]) -> Dict[str, str]:
    return {
        str(asset): str(category)
        for asset, category in (asset_map or {}).items()
        if not is_ig_primary_category(str(category or ""))
    }


def filter_ig_primary_assets(asset_map: Dict[str, str]) -> Dict[str, str]:
    return {
        str(asset): str(category)
        for asset, category in (asset_map or {}).items()
        if is_ig_primary_category(str(category or ""))
    }


def get_market_status(asset: str, category: str = ""):
    resolved_category = str(category or get_profile(asset).category or "").strip().lower()

    if is_ig_primary_category(resolved_category):
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
    if not is_ig_primary_category(resolved_category):
        return None
    try:
        from services.ig_market_bridge import ig_market_bridge

        data = ig_market_bridge.get_client_sentiment(asset, category=resolved_category)
        if isinstance(data, dict) and data:
            return dict(data)
    except Exception:
        pass
    return None
