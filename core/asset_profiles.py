from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, FrozenSet, Set

# ── Asset universe ────────────────────────────────────────────────────────────

FOREX_ASSETS: FrozenSet[str] = frozenset({
    "EUR/USD", "EUR/JPY", "EUR/GBP", "GBP/JPY", "GBP/USD",
    "AUD/USD", "NZD/USD", "USD/JPY", "USD/CAD", "USD/CHF",
})

US_INDEX_ASSETS: FrozenSet[str] = frozenset({
    "US30", "US100", "US500",
})

UK_INDEX_ASSETS: FrozenSet[str] = frozenset({
    "UK100",
})

EUROPE_INDEX_ASSETS: FrozenSet[str] = frozenset({
    "GER40",
})

AUSTRALIA_INDEX_ASSETS: FrozenSet[str] = frozenset({
    "AUS200",
})

JAPAN_INDEX_ASSETS: FrozenSet[str] = frozenset({
    "JPN225",
})

INDEX_ASSETS: FrozenSet[str] = (
    US_INDEX_ASSETS
    | UK_INDEX_ASSETS
    | EUROPE_INDEX_ASSETS
    | AUSTRALIA_INDEX_ASSETS
    | JAPAN_INDEX_ASSETS
)

COMMODITY_ASSETS: FrozenSet[str] = frozenset({
    "XAU/USD",  # Gold
    "XAG/USD",  # Silver
    "WTI",      # WTI Crude Oil
})

CRYPTO_ASSETS: FrozenSet[str] = frozenset({
    "BTC-USD", "ETH-USD", "BNB-USD", "SOL-USD", "XRP-USD",
})

ALL_ASSETS: FrozenSet[str] = (
    FOREX_ASSETS | INDEX_ASSETS | COMMODITY_ASSETS | CRYPTO_ASSETS
)


# ── Profile dataclass ─────────────────────────────────────────────────────────

@dataclass(frozen=True)
class AssetProfile:
    """Immutable profile describing which data sources are valid for an asset."""

    category: str                          # forex | indices | commodities | crypto

    # Decision-engine inputs
    use_order_flow:    bool = False        # Order book + imbalance
    use_liquidations:  bool = False        # Liquidation stream
    use_funding_rates: bool = False        # Funding-rate monitor
    use_whale_data:    bool = False        # Whale and on-chain intelligence
    use_aaii:          bool = False        # AAII bullish/bearish survey
    use_put_call:      bool = False        # Equity put/call ratio
    use_reddit:        bool = False        # Reddit sentiment
    use_session_gates: bool = True         # Market-hours gating
    use_macro_news:    bool = True         # Macro news / economic events

    # Minimum valid inputs required to emit a signal
    min_valid_layers:  int  = 3

    # Governance source-family lanes that can satisfy the real-source minimum.
    # Families are counted once each so one category cannot dominate simply by
    # having more feeds inside the same lane.
    source_families: tuple[str, ...] = field(default_factory=tuple)

    # News keyword filter for this asset type (used by the sentiment service)
    news_keywords: tuple = field(default_factory=tuple)

    # Market hours identifier (used by Layer 4 and dashboard)
    market_hours: str = "unknown"          # forex_24_5 | crypto_24_7 | us_equity | uk_equity | europe_equity | australia_equity | japan_equity | futures


# ── Profiles ─────────────────────────────────────────────────────────────────

_FOREX_PROFILE = AssetProfile(
    category          = "forex",
    use_order_flow    = False,
    use_liquidations  = False,
    use_funding_rates = False,
    use_whale_data    = False,
    use_aaii          = False,
    use_put_call      = False,
    use_reddit        = False,
    use_session_gates = True,
    use_macro_news    = True,
    min_valid_layers  = 2,
    source_families   = ("model", "regime", "sentiment", "macro", "positioning", "flow", "cross_asset"),
    news_keywords     = ("fed", "ecb", "boe", "rba", "inflation", "cpi", "interest rate",
                         "central bank", "monetary policy", "forex", "currency"),
    market_hours      = "forex_24_5",
)

_US_INDEX_PROFILE = AssetProfile(
    category          = "indices",
    use_order_flow    = False,
    use_liquidations  = False,
    use_funding_rates = False,
    use_whale_data    = False,
    use_aaii          = True,    # AAII valid for US indices only
    use_put_call      = True,    # Put/call valid for US indices only
    use_reddit        = False,
    use_session_gates = True,
    use_macro_news    = True,
    min_valid_layers  = 2,
    source_families   = ("model", "regime", "sentiment", "macro", "positioning", "options", "flow", "cross_asset"),
    news_keywords     = ("earnings", "economy", "gdp", "stocks", "s&p", "nasdaq",
                         "dow", "fed", "recession", "market", "equities"),
    market_hours      = "us_equity",
)

_UK_INDEX_PROFILE = AssetProfile(
    category          = "indices",
    use_order_flow    = False,
    use_liquidations  = False,
    use_funding_rates = False,
    use_whale_data    = False,
    use_aaii          = False,   # AAII not applicable to UK
    use_put_call      = False,   # US put/call not applicable to UK
    use_reddit        = False,
    use_session_gates = True,
    use_macro_news    = True,
    min_valid_layers  = 2,
    source_families   = ("model", "regime", "sentiment", "macro", "flow", "cross_asset"),
    news_keywords     = ("ftse", "boe", "uk economy", "british", "gbp", "earnings",
                         "market", "stocks", "interest rate"),
    market_hours      = "uk_equity",
)

_EUROPE_INDEX_PROFILE = AssetProfile(
    category          = "indices",
    use_order_flow    = False,
    use_liquidations  = False,
    use_funding_rates = False,
    use_whale_data    = False,
    use_aaii          = False,
    use_put_call      = False,
    use_reddit        = False,
    use_session_gates = True,
    use_macro_news    = True,
    min_valid_layers  = 2,
    source_families   = ("model", "regime", "sentiment", "macro", "flow", "cross_asset"),
    news_keywords     = ("dax", "ger40", "germany 40", "bund", "ecb", "eurozone", "german economy", "risk sentiment"),
    market_hours      = "europe_equity",
)

_AUSTRALIA_INDEX_PROFILE = AssetProfile(
    category          = "indices",
    use_order_flow    = False,
    use_liquidations  = False,
    use_funding_rates = False,
    use_whale_data    = False,
    use_aaii          = False,
    use_put_call      = False,
    use_reddit        = False,
    use_session_gates = True,
    use_macro_news    = True,
    min_valid_layers  = 2,
    source_families   = ("model", "regime", "sentiment", "macro", "flow", "cross_asset"),
    news_keywords     = ("australia 200", "aus200", "asx", "rba", "australian economy", "commodities", "china growth"),
    market_hours      = "australia_equity",
)

_JAPAN_INDEX_PROFILE = AssetProfile(
    category          = "indices",
    use_order_flow    = False,
    use_liquidations  = False,
    use_funding_rates = False,
    use_whale_data    = False,
    use_aaii          = False,
    use_put_call      = False,
    use_reddit        = False,
    use_session_gates = True,
    use_macro_news    = True,
    min_valid_layers  = 2,
    source_families   = ("model", "regime", "sentiment", "macro", "flow", "cross_asset"),
    news_keywords     = ("nikkei", "jpn225", "japan 225", "boj", "bank of japan", "yen", "japanese economy"),
    market_hours      = "japan_equity",
)

_COMMODITY_PROFILE = AssetProfile(
    category          = "commodities",
    use_order_flow    = False,
    use_liquidations  = False,
    use_funding_rates = False,
    use_whale_data    = False,
    use_aaii          = False,
    use_put_call      = False,
    use_reddit        = False,
    use_session_gates = True,
    use_macro_news    = True,
    min_valid_layers  = 2,
    source_families   = ("model", "regime", "sentiment", "macro", "positioning", "flow", "cross_asset"),
    news_keywords     = ("oil", "gold", "silver", "commodity", "supply", "demand",
                         "inventory", "opec", "fed", "dollar", "inflation"),
    market_hours      = "futures",
)

_CRYPTO_PROFILE = AssetProfile(
    category          = "crypto",
    use_order_flow    = True,
    use_liquidations  = True,
    use_funding_rates = True,
    use_whale_data    = True,
    use_aaii          = False,
    use_put_call      = False,
    use_reddit        = True,
    use_session_gates = False,   # Crypto is 24/7
    use_macro_news    = True,
    min_valid_layers  = 2,       # Fewer required layers (crypto is fast-moving)
    source_families   = ("model", "regime", "sentiment", "flow", "derivatives", "positioning", "cross_asset"),
    news_keywords     = ("bitcoin", "ethereum", "crypto", "blockchain", "defi",
                         "altcoin", "btc", "eth", "binance", "solana", "ripple"),
    market_hours      = "crypto_24_7",
)


# ── Registry ──────────────────────────────────────────────────────────────────

_PROFILE_REGISTRY: Dict[str, AssetProfile] = {}

_LEGACY_CANONICAL = {
    "GC=F": "XAU/USD",
    "SI=F": "XAG/USD",
    "CL=F": "WTI",
    "WTI/USD": "WTI",
    "EURGBP": "EUR/GBP",
    "NZDUSD": "NZD/USD",
    "USDCHF": "USD/CHF",
    "^DJI": "US30",
    "^IXIC": "US100",
    "^GSPC": "US500",
    "^FTSE": "UK100",
    "DAX": "GER40",
    "DAX40": "GER40",
    "DE40": "GER40",
    "ASX200": "AUS200",
    "AU200": "AUS200",
    "JP225": "JPN225",
    "NIKKEI": "JPN225",
    "NIKKEI225": "JPN225",
}

_DEFAULT_EXECUTION_POLICY: Dict[str, float | int] = {
    "risk_kill_threshold": 0.58,
    "weak_candle_extension_limit": 1.25,
    "weak_candle_floor": 0.26,
    "target_efficiency_hard_floor": 0.15,
    "opposing_distance_hard_floor": 0.0035,
    "impulse_age_hard_limit": 6,
    "directional_extension_hard_limit": 0.74,
    "pattern_rank_hard_floor": 0.12,
    "pattern_rank_strong_floor": 0.08,
    "preferred_true_depth_min_quality": 0.50,
    "minimum_usable_true_depth_quality": 0.0,
    "preferred_true_depth_min_trust_score": 0.78,
    "minimum_usable_true_depth_trust_score": 0.60,
    "true_depth_bonus": 0.03,
    "thin_true_depth_penalty": 0.0,
    "low_trust_true_depth_penalty": 0.03,
    "misaligned_true_depth_penalty": 0.05,
    "synthetic_depth_penalty": 0.04,
    "asset_edge_bonus_scale": 0.08,
    "asset_edge_penalty_scale": 0.09,
    "book_edge_bonus_scale": 0.06,
    "book_edge_penalty_scale": 0.07,
    "depth_sovereignty_min_directional_flow": 0.22,
    "depth_sovereignty_min_true_depth_support": 0.12,
    "depth_sovereignty_min_component": 0.18,
    "htf_depth_override_penalty": 0.015,
    "dom_stream_health_min_score_for_sovereignty": 0.58,
    "dom_stream_health_hard_floor": 0.34,
    "dom_stream_trust_penalty_scale": 0.60,
    "dom_stream_degraded_penalty": 0.12,
}

_CATEGORY_EXECUTION_POLICIES: Dict[str, Dict[str, float | int]] = {
    "forex": {
        "risk_kill_threshold": 0.60,
        "weak_candle_extension_limit": 1.28,
        "weak_candle_floor": 0.25,
        "target_efficiency_hard_floor": 0.14,
        "opposing_distance_hard_floor": 0.0032,
        "directional_extension_hard_limit": 0.76,
        "pattern_rank_hard_floor": 0.10,
        "pattern_rank_strong_floor": 0.06,
        "minimum_usable_true_depth_quality": 0.25,
        "preferred_true_depth_min_trust_score": 0.80,
        "minimum_usable_true_depth_trust_score": 0.60,
        "true_depth_bonus": 0.02,
        "thin_true_depth_penalty": 0.03,
        "low_trust_true_depth_penalty": 0.03,
        "misaligned_true_depth_penalty": 0.05,
        "synthetic_depth_penalty": 0.02,
        "asset_edge_bonus_scale": 0.09,
        "dom_stream_health_min_score_for_sovereignty": 0.56,
        "dom_stream_health_hard_floor": 0.32,
        "dom_stream_trust_penalty_scale": 0.58,
    },
    "indices": {
        "risk_kill_threshold": 0.59,
        "weak_candle_extension_limit": 1.27,
        "weak_candle_floor": 0.25,
        "target_efficiency_hard_floor": 0.14,
        "opposing_distance_hard_floor": 0.0033,
        "directional_extension_hard_limit": 0.75,
        "pattern_rank_hard_floor": 0.10,
        "pattern_rank_strong_floor": 0.07,
        "minimum_usable_true_depth_quality": 0.25,
        "preferred_true_depth_min_trust_score": 0.78,
        "minimum_usable_true_depth_trust_score": 0.60,
        "true_depth_bonus": 0.03,
        "thin_true_depth_penalty": 0.03,
        "low_trust_true_depth_penalty": 0.03,
        "misaligned_true_depth_penalty": 0.05,
        "synthetic_depth_penalty": 0.03,
        "dom_stream_health_min_score_for_sovereignty": 0.57,
        "dom_stream_health_hard_floor": 0.33,
        "dom_stream_trust_penalty_scale": 0.60,
    },
    "commodities": {
        "risk_kill_threshold": 0.55,
        "weak_candle_extension_limit": 1.20,
        "weak_candle_floor": 0.28,
        "target_efficiency_hard_floor": 0.18,
        "opposing_distance_hard_floor": 0.0040,
        "impulse_age_hard_limit": 5,
        "directional_extension_hard_limit": 0.70,
        "pattern_rank_hard_floor": 0.14,
        "pattern_rank_strong_floor": 0.10,
        "preferred_true_depth_min_quality": 0.60,
        "minimum_usable_true_depth_quality": 0.25,
        "preferred_true_depth_min_trust_score": 0.82,
        "minimum_usable_true_depth_trust_score": 0.64,
        "true_depth_bonus": 0.02,
        "thin_true_depth_penalty": 0.04,
        "low_trust_true_depth_penalty": 0.04,
        "misaligned_true_depth_penalty": 0.06,
        "synthetic_depth_penalty": 0.06,
        "asset_edge_bonus_scale": 0.07,
        "book_edge_bonus_scale": 0.05,
        "dom_stream_health_min_score_for_sovereignty": 0.62,
        "dom_stream_health_hard_floor": 0.38,
        "dom_stream_trust_penalty_scale": 0.68,
        "dom_stream_degraded_penalty": 0.16,
    },
    "crypto": {
        "risk_kill_threshold": 0.56,
        "weak_candle_extension_limit": 1.30,
        "weak_candle_floor": 0.24,
        "target_efficiency_hard_floor": 0.14,
        "opposing_distance_hard_floor": 0.0032,
        "directional_extension_hard_limit": 0.76,
        "pattern_rank_hard_floor": 0.11,
        "pattern_rank_strong_floor": 0.07,
        "preferred_true_depth_min_quality": 0.45,
        "preferred_true_depth_min_trust_score": 0.72,
        "minimum_usable_true_depth_trust_score": 0.55,
        "true_depth_bonus": 0.05,
        "low_trust_true_depth_penalty": 0.02,
        "misaligned_true_depth_penalty": 0.04,
        "synthetic_depth_penalty": 0.07,
        "asset_edge_bonus_scale": 0.10,
        "book_edge_bonus_scale": 0.07,
        "dom_stream_health_min_score_for_sovereignty": 0.60,
        "dom_stream_health_hard_floor": 0.36,
        "dom_stream_trust_penalty_scale": 0.65,
        "dom_stream_degraded_penalty": 0.14,
    },
    "unknown": {},
}

_ASSET_EXECUTION_POLICY_OVERRIDES: Dict[str, Dict[str, float | int]] = {
    "US100": {
        "risk_kill_threshold": 0.61,
        "weak_candle_extension_limit": 1.31,
        "pattern_rank_hard_floor": 0.09,
    },
    "US500": {
        "risk_kill_threshold": 0.60,
        "weak_candle_extension_limit": 1.29,
        "pattern_rank_hard_floor": 0.09,
    },
    "XAU/USD": {
        "risk_kill_threshold": 0.54,
        "target_efficiency_hard_floor": 0.19,
        "opposing_distance_hard_floor": 0.0042,
        "synthetic_depth_penalty": 0.07,
    },
    "WTI": {
        "risk_kill_threshold": 0.53,
        "target_efficiency_hard_floor": 0.20,
        "opposing_distance_hard_floor": 0.0044,
        "impulse_age_hard_limit": 4,
        "synthetic_depth_penalty": 0.08,
    },
    "BTC-USD": {
        "risk_kill_threshold": 0.57,
        "weak_candle_extension_limit": 1.32,
        "true_depth_bonus": 0.06,
    },
    "ETH-USD": {
        "risk_kill_threshold": 0.57,
        "weak_candle_extension_limit": 1.31,
        "true_depth_bonus": 0.055,
    },
    "BNB-USD": {
        "risk_kill_threshold": 0.54,
        "opposing_distance_hard_floor": 0.0036,
        "synthetic_depth_penalty": 0.09,
    },
    "SOL-USD": {
        "risk_kill_threshold": 0.54,
        "opposing_distance_hard_floor": 0.0037,
        "synthetic_depth_penalty": 0.09,
    },
    "XRP-USD": {
        "risk_kill_threshold": 0.53,
        "opposing_distance_hard_floor": 0.0038,
        "pattern_rank_hard_floor": 0.12,
        "synthetic_depth_penalty": 0.10,
    },
}

for _asset in FOREX_ASSETS:
    _PROFILE_REGISTRY[_asset] = _FOREX_PROFILE

for _asset in US_INDEX_ASSETS:
    _PROFILE_REGISTRY[_asset] = _US_INDEX_PROFILE

for _asset in UK_INDEX_ASSETS:
    _PROFILE_REGISTRY[_asset] = _UK_INDEX_PROFILE

for _asset in EUROPE_INDEX_ASSETS:
    _PROFILE_REGISTRY[_asset] = _EUROPE_INDEX_PROFILE

for _asset in AUSTRALIA_INDEX_ASSETS:
    _PROFILE_REGISTRY[_asset] = _AUSTRALIA_INDEX_PROFILE

for _asset in JAPAN_INDEX_ASSETS:
    _PROFILE_REGISTRY[_asset] = _JAPAN_INDEX_PROFILE

for _asset in COMMODITY_ASSETS:
    _PROFILE_REGISTRY[_asset] = _COMMODITY_PROFILE

for _asset in CRYPTO_ASSETS:
    _PROFILE_REGISTRY[_asset] = _CRYPTO_PROFILE


def get_profile(asset: str) -> AssetProfile:
    """
    Return the AssetProfile for a given canonical asset ID.
    Falls back to a conservative default if the asset is unknown.
    """
    canonical = _LEGACY_CANONICAL.get((asset or "").strip().upper(), asset)
    if canonical == "WTI":
        return _COMMODITY_PROFILE
    return _PROFILE_REGISTRY.get(
        canonical,
        AssetProfile(
            category          = "unknown",
            use_order_flow    = False,
            use_liquidations  = False,
            use_funding_rates = False,
            use_whale_data    = False,
            use_aaii          = False,
            use_put_call      = False,
            use_reddit        = False,
            use_session_gates = True,
            use_macro_news    = True,
            min_valid_layers  = 4,
            source_families   = ("model", "regime", "sentiment", "cross_asset"),
            market_hours      = "unknown",
        )
    )


def is_crypto(asset: str) -> bool:
    return get_profile(asset).category == "crypto"


def is_forex(asset: str) -> bool:
    return get_profile(asset).category == "forex"


def is_index(asset: str) -> bool:
    return get_profile(asset).category == "indices"


def is_us_index(asset: str) -> bool:
    canonical = _LEGACY_CANONICAL.get((asset or "").strip().upper(), asset)
    return canonical in US_INDEX_ASSETS


def is_commodity(asset: str) -> bool:
    return get_profile(asset).category == "commodities"

def is_uk_index(asset: str) -> bool:
    canonical = _LEGACY_CANONICAL.get((asset or "").strip().upper(), asset)
    return canonical in UK_INDEX_ASSETS


def is_europe_index(asset: str) -> bool:
    canonical = _LEGACY_CANONICAL.get((asset or "").strip().upper(), asset)
    return canonical in EUROPE_INDEX_ASSETS


def is_australia_index(asset: str) -> bool:
    canonical = _LEGACY_CANONICAL.get((asset or "").strip().upper(), asset)
    return canonical in AUSTRALIA_INDEX_ASSETS


def is_japan_index(asset: str) -> bool:
    canonical = _LEGACY_CANONICAL.get((asset or "").strip().upper(), asset)
    return canonical in JAPAN_INDEX_ASSETS


def get_execution_policy(asset: str) -> Dict[str, float | int]:
    canonical = _LEGACY_CANONICAL.get((asset or "").strip().upper(), asset)
    profile = get_profile(canonical)
    policy = dict(_DEFAULT_EXECUTION_POLICY)
    policy.update(_CATEGORY_EXECUTION_POLICIES.get(profile.category, {}))
    policy.update(_ASSET_EXECUTION_POLICY_OVERRIDES.get(canonical, {}))
    return policy
