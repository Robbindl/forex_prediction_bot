from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, FrozenSet


FOREX_ASSETS: FrozenSet[str] = frozenset(
    {
        "EUR/USD",
        "EUR/JPY",
        "EUR/GBP",
        "GBP/JPY",
        "GBP/USD",
        "AUD/USD",
        "NZD/USD",
        "USD/JPY",
        "USD/CAD",
        "USD/CHF",
    }
)

US_INDEX_ASSETS: FrozenSet[str] = frozenset({"US30", "US100", "US500"})
UK_INDEX_ASSETS: FrozenSet[str] = frozenset({"UK100"})
EUROPE_INDEX_ASSETS: FrozenSet[str] = frozenset({"GER40"})
AUSTRALIA_INDEX_ASSETS: FrozenSet[str] = frozenset({"AUS200"})
JAPAN_INDEX_ASSETS: FrozenSet[str] = frozenset({"JPN225"})
INDEX_ASSETS: FrozenSet[str] = (
    US_INDEX_ASSETS
    | UK_INDEX_ASSETS
    | EUROPE_INDEX_ASSETS
    | AUSTRALIA_INDEX_ASSETS
    | JAPAN_INDEX_ASSETS
)

COMMODITY_ASSETS: FrozenSet[str] = frozenset({"XAU/USD", "XAG/USD", "WTI"})
CRYPTO_ASSETS: FrozenSet[str] = frozenset({"BTC-USD", "ETH-USD", "BNB-USD", "SOL-USD", "XRP-USD"})
ALL_ASSETS: FrozenSet[str] = FOREX_ASSETS | INDEX_ASSETS | COMMODITY_ASSETS | CRYPTO_ASSETS

_LEGACY_CANONICAL: Dict[str, str] = {
    "BTCUSD": "BTC-USD",
    "BTCUSDT": "BTC-USD",
    "ETHUSD": "ETH-USD",
    "ETHUSDT": "ETH-USD",
    "BNBUSD": "BNB-USD",
    "BNBUSDT": "BNB-USD",
    "SOLUSD": "SOL-USD",
    "SOLUSDT": "SOL-USD",
    "XRPUSD": "XRP-USD",
    "XRPUSDT": "XRP-USD",
    "XAUUSD": "XAU/USD",
    "XAGUSD": "XAG/USD",
    "USOIL": "WTI",
    "WTIUSD": "WTI",
    "SPX500": "US500",
    "NAS100": "US100",
    "DJ30": "US30",
    "GER40": "GER40",
    "DE40": "GER40",
    "UK100": "UK100",
    "AUS200": "AUS200",
    "JPN225": "JPN225",
}


@dataclass(frozen=True)
class AssetProfile:
    category: str
    use_order_flow: bool = True
    use_liquidations: bool = False
    use_funding_rates: bool = False
    use_whale_data: bool = False
    use_aaii: bool = False
    use_put_call: bool = False
    use_reddit: bool = False
    use_session_gates: bool = True
    use_macro_news: bool = True
    min_valid_layers: int = 2
    source_families: tuple[str, ...] = field(default_factory=tuple)
    news_keywords: tuple[str, ...] = field(default_factory=tuple)
    market_hours: str = "unknown"


_BASE_SOURCE_FAMILIES = (
    "model",
    "regime",
    "structure",
    "sentiment",
    "macro",
    "flow",
    "depth",
    "cross_asset",
)

_PROFILE_REGISTRY: Dict[str, AssetProfile] = {}


def _register(assets: FrozenSet[str], profile: AssetProfile) -> None:
    for asset in assets:
        _PROFILE_REGISTRY[asset] = profile


_register(
    FOREX_ASSETS,
    AssetProfile(
        category="forex",
        source_families=_BASE_SOURCE_FAMILIES + ("positioning",),
        news_keywords=("central bank", "inflation", "cpi", "rates", "currency", "forex"),
        market_hours="forex_24_5",
    ),
)
_register(
    US_INDEX_ASSETS,
    AssetProfile(
        category="indices",
        use_aaii=True,
        use_put_call=True,
        source_families=_BASE_SOURCE_FAMILIES + ("options", "equity_breadth"),
        news_keywords=("fed", "nasdaq", "s&p", "dow", "earnings", "stocks", "risk sentiment"),
        market_hours="us_equity",
    ),
)
_register(
    UK_INDEX_ASSETS,
    AssetProfile(
        category="indices",
        source_families=_BASE_SOURCE_FAMILIES + ("equity_breadth",),
        news_keywords=("ftse", "boe", "uk economy", "gbp", "risk sentiment"),
        market_hours="uk_equity",
    ),
)
_register(
    EUROPE_INDEX_ASSETS,
    AssetProfile(
        category="indices",
        source_families=_BASE_SOURCE_FAMILIES + ("equity_breadth",),
        news_keywords=("dax", "ecb", "eurozone", "bund", "germany", "risk sentiment"),
        market_hours="europe_equity",
    ),
)
_register(
    AUSTRALIA_INDEX_ASSETS,
    AssetProfile(
        category="indices",
        source_families=_BASE_SOURCE_FAMILIES + ("equity_breadth",),
        news_keywords=("asx", "australia", "rba", "risk sentiment"),
        market_hours="australia_equity",
    ),
)
_register(
    JAPAN_INDEX_ASSETS,
    AssetProfile(
        category="indices",
        source_families=_BASE_SOURCE_FAMILIES + ("equity_breadth",),
        news_keywords=("nikkei", "boj", "yen", "japan", "risk sentiment"),
        market_hours="japan_equity",
    ),
)
_register(
    COMMODITY_ASSETS,
    AssetProfile(
        category="commodities",
        source_families=_BASE_SOURCE_FAMILIES + ("inventory", "dollar", "rates"),
        news_keywords=("oil", "gold", "silver", "inventory", "opec", "dollar", "yields"),
        market_hours="futures",
    ),
)
_register(
    CRYPTO_ASSETS,
    AssetProfile(
        category="crypto",
        use_liquidations=True,
        use_funding_rates=True,
        use_whale_data=True,
        use_session_gates=False,
        source_families=_BASE_SOURCE_FAMILIES + ("funding", "liquidations", "onchain"),
        news_keywords=("bitcoin", "ethereum", "crypto", "stablecoin", "risk sentiment"),
        market_hours="crypto_24_7",
    ),
)


_UNIVERSAL_EXECUTION_POLICY: Dict[str, float | int] = {
    "min_confidence": 0.55,
    "min_final_confidence": 0.58,
    "min_rr": 1.45,
    "target_rr": 1.80,
    "max_spread": 0.0035,
    "max_spread_bps": 18.0,
    "structure_min_alignment": 0.24,
    "structure_min_setup": 0.30,
    "entry_confirm_bars": 1,
    "max_extension_score": 1.18,
    "max_impulse_age_bars": 6,
    "min_target_efficiency": 0.18,
    "depth_required_when_available": 1,
    "minimum_usable_true_depth_quality": 0.24,
    "preferred_true_depth_min_quality": 0.36,
    "minimum_usable_true_depth_trust_score": 0.50,
    "preferred_true_depth_min_trust_score": 0.60,
    "snapshot_true_depth_min_levels": 2,
    "event_ladder_min_levels": 2,
    "depth_support_min": 0.18,
    "depth_conflict_block": 0.22,
    "depth_sovereignty_min_directional_flow": 0.28,
    "dom_stream_health_floor": 0.35,
    "dom_stream_hard_floor": 0.30,
    "scorecard_min_score": 0.56,
    "cooldown_minutes": 12,
}

_CATEGORY_MARKET_COSTS: Dict[str, Dict[str, float | int]] = {
    "forex": {"max_spread": 0.0025, "max_spread_bps": 12.0},
    "indices": {"max_spread": 0.0045, "max_spread_bps": 24.0},
    "commodities": {"max_spread": 0.0040, "max_spread_bps": 22.0},
    "crypto": {"max_spread": 0.0035, "max_spread_bps": 18.0},
}

_ASSET_POLICY_OVERRIDES: Dict[str, Dict[str, float | int]] = {
    "XAU/USD": {"max_spread_bps": 20.0},
    "XAG/USD": {"max_spread_bps": 26.0},
    "WTI": {"max_spread_bps": 24.0},
    "US100": {"max_spread_bps": 28.0},
    "BTC-USD": {"max_spread_bps": 16.0},
    "ETH-USD": {"max_spread_bps": 18.0},
}

_EXCHANGE_DEPTH_PROVIDERS = frozenset({"binance", "bybit", "okx"})
_BROKER_L2_PROVIDERS = frozenset({"ctrader", "dukascopy", "ig", "deriv"})

_DEPTH_FEED_POLICIES: Dict[str, Dict[str, float | int | bool | str]] = {
    "exchange_deep": {
        "depth_feed_class": "exchange_deep",
        "min_levels": 5,
        "preferred_levels": 20,
        "min_quality": 0.45,
        "min_trust": 0.64,
        "support_min": 0.18,
        "conflict_block": 0.22,
        "sovereignty_allowed": True,
        "confirmation_override_allowed": True,
    },
    "broker_l2": {
        "depth_feed_class": "broker_l2",
        "min_levels": 5,
        "preferred_levels": 10,
        "min_quality": 0.32,
        "min_trust": 0.50,
        "support_min": 0.14,
        "conflict_block": 0.32,
        "sovereignty_allowed": False,
        "confirmation_override_allowed": False,
    },
    "thin_broker_l2": {
        "depth_feed_class": "thin_broker_l2",
        "min_levels": 2,
        "preferred_levels": 4,
        "min_quality": 0.24,
        "min_trust": 0.48,
        "support_min": 0.18,
        "conflict_block": 0.38,
        "sovereignty_allowed": False,
        "confirmation_override_allowed": False,
    },
    "quote_only": {
        "depth_feed_class": "quote_only",
        "min_levels": 0,
        "preferred_levels": 0,
        "min_quality": 1.0,
        "min_trust": 1.0,
        "support_min": 1.0,
        "conflict_block": 1.0,
        "sovereignty_allowed": False,
        "confirmation_override_allowed": False,
    },
}


def canonical_asset(asset: str) -> str:
    raw = str(asset or "").strip()
    upper = raw.upper()
    if upper in _LEGACY_CANONICAL:
        return _LEGACY_CANONICAL[upper]
    if "/" in raw:
        left, right = raw.split("/", 1)
        return f"{left.upper()}/{right.upper()}"
    if "-" in raw:
        left, right = raw.split("-", 1)
        return f"{left.upper()}-{right.upper()}"
    if len(upper) == 6 and upper[:3].isalpha() and upper[3:].isalpha():
        pair = f"{upper[:3]}/{upper[3:]}"
        if pair in FOREX_ASSETS or pair in COMMODITY_ASSETS:
            return pair
    return upper


def get_profile(asset: str) -> AssetProfile:
    canonical = canonical_asset(asset)
    return _PROFILE_REGISTRY.get(
        canonical,
        AssetProfile(
            category="unknown",
            use_order_flow=True,
            source_families=_BASE_SOURCE_FAMILIES,
            min_valid_layers=2,
            market_hours="unknown",
        ),
    )


def is_crypto(asset: str) -> bool:
    return get_profile(asset).category == "crypto"


def is_forex(asset: str) -> bool:
    return get_profile(asset).category == "forex"


def is_index(asset: str) -> bool:
    return get_profile(asset).category == "indices"


def is_us_index(asset: str) -> bool:
    return canonical_asset(asset) in US_INDEX_ASSETS


def is_commodity(asset: str) -> bool:
    return get_profile(asset).category == "commodities"


def is_uk_index(asset: str) -> bool:
    return canonical_asset(asset) in UK_INDEX_ASSETS


def is_europe_index(asset: str) -> bool:
    return canonical_asset(asset) in EUROPE_INDEX_ASSETS


def is_australia_index(asset: str) -> bool:
    return canonical_asset(asset) in AUSTRALIA_INDEX_ASSETS


def is_japan_index(asset: str) -> bool:
    return canonical_asset(asset) in JAPAN_INDEX_ASSETS


def get_execution_policy(asset: str) -> Dict[str, float | int]:
    canonical = canonical_asset(asset)
    profile = get_profile(canonical)
    policy: Dict[str, float | int] = dict(_UNIVERSAL_EXECUTION_POLICY)
    policy.update(_CATEGORY_MARKET_COSTS.get(profile.category, {}))
    policy.update(_ASSET_POLICY_OVERRIDES.get(canonical, {}))
    policy["asset_universe_size"] = len(ALL_ASSETS)
    return policy


def classify_depth_feed(
    *,
    asset: str = "",
    category: str = "",
    provider: str = "",
    provider_class: str = "",
    source: str = "",
    depth_available: bool = False,
    synthetic_depth: bool = False,
    levels: int = 0,
) -> str:
    """Classify DOM source capability without comparing unrelated venues."""

    if synthetic_depth:
        return "synthetic"
    if not depth_available or int(levels or 0) <= 0:
        return "quote_only"

    provider_key = str(provider or "").strip().lower()
    class_key = str(provider_class or "").strip().lower()
    source_key = str(source or "").strip().lower()
    resolved_category = str(category or get_profile(asset).category or "").strip().lower()

    if (
        class_key in {"exchange", "exchange_depth", "exchange_deep"}
        or provider_key in _EXCHANGE_DEPTH_PROVIDERS
        or any(token in source_key for token in _EXCHANGE_DEPTH_PROVIDERS)
    ):
        return "exchange_deep"

    if class_key == "redis_subscriber" and resolved_category == "crypto":
        return "exchange_deep"

    if (
        class_key in {"broker_l2", "sidecar"}
        or provider_key in _BROKER_L2_PROVIDERS
        or any(token in source_key for token in _BROKER_L2_PROVIDERS)
    ):
        return "broker_l2" if int(levels or 0) >= 5 else "thin_broker_l2"

    return "broker_l2" if resolved_category in {"forex", "indices"} else "exchange_deep"


def get_depth_feed_policy(
    asset: str,
    category: str = "",
    feed_class: str = "",
) -> Dict[str, float | int | bool | str]:
    feed_key = str(feed_class or "").strip().lower()
    if feed_key == "synthetic":
        feed_key = "quote_only"
    policy = dict(_DEPTH_FEED_POLICIES.get(feed_key) or _DEPTH_FEED_POLICIES["quote_only"])
    resolved_category = str(category or get_profile(asset).category or "").strip().lower()
    if resolved_category in {"forex", "indices"} and policy["depth_feed_class"] == "exchange_deep":
        # Do not let proxy or mislabeled broker data be judged as Binance-style depth.
        policy = dict(_DEPTH_FEED_POLICIES["broker_l2"])
    return policy


def all_asset_categories() -> Dict[str, str]:
    return {asset: get_profile(asset).category for asset in sorted(ALL_ASSETS)}


def profile_payload(asset: str) -> Dict[str, Any]:
    profile = get_profile(asset)
    return {
        "asset": canonical_asset(asset),
        "category": profile.category,
        "market_hours": profile.market_hours,
        "source_families": list(profile.source_families),
        "use_session_gates": bool(profile.use_session_gates),
    }
