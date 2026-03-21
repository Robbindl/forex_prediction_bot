from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, FrozenSet, Set

# ── Asset universe ────────────────────────────────────────────────────────────

FOREX_ASSETS: FrozenSet[str] = frozenset({
    "EUR/USD", "GBP/JPY", "GBP/USD", "AUD/USD", "USD/JPY", "USD/CAD",
})

US_INDEX_ASSETS: FrozenSet[str] = frozenset({
    "^DJI", "^IXIC", "^GSPC",
})

UK_INDEX_ASSETS: FrozenSet[str] = frozenset({
    "^FTSE",
})

INDEX_ASSETS: FrozenSet[str] = US_INDEX_ASSETS | UK_INDEX_ASSETS

COMMODITY_ASSETS: FrozenSet[str] = frozenset({
    "GC=F",   # Gold
    "SI=F",   # Silver
    "CL=F",   # Crude Oil
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

    # Pipeline layers
    use_order_flow:    bool = False        # Phase 3 order book + imbalance
    use_liquidations:  bool = False        # Phase 1 liquidation stream
    use_funding_rates: bool = False        # Phase 1 funding rate monitor
    use_whale_data:    bool = False        # Phase 2 + Layer 6
    use_aaii:          bool = False        # AAII bullish/bearish survey
    use_put_call:      bool = False        # Equity put/call ratio
    use_reddit:        bool = False        # Reddit sentiment
    use_session_gates: bool = True         # Layer 4 market-hours gating
    use_macro_news:    bool = True         # Macro news / economic events

    # Minimum valid layers required to emit a signal
    min_valid_layers:  int  = 3

    # News keyword filter for this asset type (used by SentimentAnalyzer)
    news_keywords: tuple = field(default_factory=tuple)

    # Market hours identifier (used by Layer 4 and dashboard)
    market_hours: str = "unknown"          # forex_24_5 | crypto_24_7 | us_equity | uk_equity | futures


# ── Profiles ─────────────────────────────────────────────────────────────────

_FOREX_PROFILE = AssetProfile(
    category          = "forex",
    use_order_flow    = False,
    use_liquidations  = False,
    use_funding_rates = False,
    use_whale_data    = False,
    use_aaii          = False,
    use_put_call      = False,
    use_reddit        = True,
    use_session_gates = True,
    use_macro_news    = True,
    min_valid_layers  = 3,
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
    use_reddit        = True,
    use_session_gates = True,
    use_macro_news    = True,
    min_valid_layers  = 3,
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
    use_reddit        = True,
    use_session_gates = True,
    use_macro_news    = True,
    min_valid_layers  = 3,
    news_keywords     = ("ftse", "boe", "uk economy", "british", "gbp", "earnings",
                         "market", "stocks", "interest rate"),
    market_hours      = "uk_equity",
)

_COMMODITY_PROFILE = AssetProfile(
    category          = "commodities",
    use_order_flow    = False,
    use_liquidations  = False,
    use_funding_rates = False,
    use_whale_data    = False,
    use_aaii          = False,
    use_put_call      = False,
    use_reddit        = True,
    use_session_gates = True,
    use_macro_news    = True,
    min_valid_layers  = 3,
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
    news_keywords     = ("bitcoin", "ethereum", "crypto", "blockchain", "defi",
                         "altcoin", "btc", "eth", "binance", "solana", "ripple"),
    market_hours      = "crypto_24_7",
)


# ── Registry ──────────────────────────────────────────────────────────────────

_PROFILE_REGISTRY: Dict[str, AssetProfile] = {}

for _asset in FOREX_ASSETS:
    _PROFILE_REGISTRY[_asset] = _FOREX_PROFILE

for _asset in US_INDEX_ASSETS:
    _PROFILE_REGISTRY[_asset] = _US_INDEX_PROFILE

for _asset in UK_INDEX_ASSETS:
    _PROFILE_REGISTRY[_asset] = _UK_INDEX_PROFILE

for _asset in COMMODITY_ASSETS:
    _PROFILE_REGISTRY[_asset] = _COMMODITY_PROFILE

for _asset in CRYPTO_ASSETS:
    _PROFILE_REGISTRY[_asset] = _CRYPTO_PROFILE


def get_profile(asset: str) -> AssetProfile:
    """
    Return the AssetProfile for a given canonical asset ID.
    Falls back to a conservative default if the asset is unknown.
    """
    return _PROFILE_REGISTRY.get(
        asset,
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
            market_hours      = "unknown",
        )
    )


def is_crypto(asset: str) -> bool:
    return asset in CRYPTO_ASSETS


def is_forex(asset: str) -> bool:
    return asset in FOREX_ASSETS


def is_index(asset: str) -> bool:
    return asset in INDEX_ASSETS


def is_us_index(asset: str) -> bool:
    return asset in US_INDEX_ASSETS


def is_commodity(asset: str) -> bool:
    return asset in COMMODITY_ASSETS

def get_pip_value(asset: str) -> float:
    from risk.position_sizer import PositionSizer
    return PositionSizer.ASSET_PIP_VALUES.get(asset, 0.0001)


def get_pip_value_per_lot(asset: str) -> float:
    from risk.position_sizer import PositionSizer
    return PositionSizer.PIP_VALUE_PER_LOT.get(asset, 10.0)