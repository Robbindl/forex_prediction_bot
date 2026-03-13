"""
core/assets.py — Canonical asset registry and identity layer.

Every asset the bot can trade has exactly ONE canonical ID.
All aliases (spot tickers, futures tickers, human names) resolve to it.

Why this matters:
  • Cooldown: a loss on SI=F must block XAG/USD entry (same silver)
  • Duplicate guard: can't hold XAU/USD and GC=F simultaneously
  • Signal dedup: two signals for the same underlying → take only the stronger
  • Position reporting: dashboard always shows the same ticker regardless of
    which feed generated the signal

Usage:
    from core.assets import registry

    registry.canonical("XAG/USD")     # → "SI=F"
    registry.canonical("Silver")      # → "SI=F"
    registry.category("SI=F")         # → "commodities"
    registry.yahoo_ticker("XAG/USD")  # → "SI=F"
    registry.is_same("XAG/USD", "SI=F")  # → True
    registry.all_assets()             # → list of (canonical_id, category)
"""

from __future__ import annotations

from typing import Dict, List, Optional, Tuple


class AssetRegistry:
    """
    Immutable registry of all tradeable assets and their aliases.

    The canonical ID is the primary Yahoo Finance ticker used for data fetching.
    All other names (spot codes, human names, common abbreviations) are aliases
    that resolve to the canonical ID.
    """

    # ── Canonical → category ──────────────────────────────────────────────────
    _ASSETS: Dict[str, str] = {
        # ── Commodities ─────────────────────────────────────────
        "GC=F":   "commodities",   # Gold futures
        "SI=F":   "commodities",   # Silver futures
        "CL=F":   "commodities",   # Crude oil futures
        "NG=F":   "commodities",   # Natural gas futures
        "HG=F":   "commodities",   # Copper futures

        # ── Crypto ──────────────────────────────────────────────
        "BTC-USD":  "crypto",
        "ETH-USD":  "crypto",
        "BNB-USD":  "crypto",
        "SOL-USD":  "crypto",
        "XRP-USD":  "crypto",
        "ADA-USD":  "crypto",
        "DOGE-USD": "crypto",
        "DOT-USD":  "crypto",
        "LTC-USD":  "crypto",
        "AVAX-USD": "crypto",
        "LINK-USD": "crypto",

        # ── Forex ────────────────────────────────────────────────
        "EUR/USD": "forex",
        "GBP/USD": "forex",
        "USD/JPY": "forex",
        "AUD/USD": "forex",
        "USD/CAD": "forex",
        "NZD/USD": "forex",
        "USD/CHF": "forex",
        "EUR/GBP": "forex",
        "EUR/JPY": "forex",
        "GBP/JPY": "forex",
        "AUD/JPY": "forex",
        "EUR/AUD": "forex",
        "GBP/AUD": "forex",
        "AUD/CAD": "forex",
        "CAD/JPY": "forex",
        "CHF/JPY": "forex",
        "EUR/CAD": "forex",
        "EUR/CHF": "forex",
        "GBP/CAD": "forex",
        "GBP/CHF": "forex",

        # ── Indices ──────────────────────────────────────────────
        "^GSPC":  "indices",   # S&P 500
        "^DJI":   "indices",   # Dow Jones
        "^IXIC":  "indices",   # Nasdaq
        "^FTSE":  "indices",   # FTSE 100
        "^N225":  "indices",   # Nikkei 225
        "^HSI":   "indices",   # Hang Seng
        "^GDAXI": "indices",   # DAX
        "^VIX":   "indices",   # VIX

        # ── Stocks ───────────────────────────────────────────────
        "AAPL":  "stocks",
        "MSFT":  "stocks",
        "GOOGL": "stocks",
        "AMZN":  "stocks",
        "TSLA":  "stocks",
        "NVDA":  "stocks",
        "META":  "stocks",
        "JPM":   "stocks",
        "V":     "stocks",
        "MA":    "stocks",
        "JNJ":   "stocks",
        "PFE":   "stocks",
        "WMT":   "stocks",
        "PG":    "stocks",
        "KO":    "stocks",
        "XOM":   "stocks",
        "CVX":   "stocks",
    }

    # ── Alias → canonical ─────────────────────────────────────────────────────
    # Keys are uppercase for case-insensitive lookup.
    # Every canonical ID also maps to itself.
    _ALIASES: Dict[str, str] = {
        # Gold
        "XAU/USD": "GC=F",  "GOLD": "GC=F",  "GC=F": "GC=F",
        "XAUUSD":  "GC=F",  "XAU":  "GC=F",

        # Silver
        "XAG/USD": "SI=F",  "SILVER": "SI=F", "SI=F": "SI=F",
        "XAGUSD":  "SI=F",  "XAG":   "SI=F",

        # Crude Oil
        "WTI/USD": "CL=F",  "WTI":   "CL=F",  "CL=F": "CL=F",
        "OIL":     "CL=F",  "CRUDE": "CL=F",  "BRENT": "CL=F",

        # Natural Gas
        "NG/USD": "NG=F",   "NG=F": "NG=F",
        "NATURALGAS": "NG=F", "GAS": "NG=F",

        # Copper
        "XCU/USD": "HG=F",  "HG=F": "HG=F",
        "COPPER":  "HG=F",  "CU":   "HG=F",

        # Crypto common names
        "BITCOIN":   "BTC-USD", "BTC":      "BTC-USD", "BTC-USD": "BTC-USD",
        "ETHEREUM":  "ETH-USD", "ETH":      "ETH-USD", "ETH-USD": "ETH-USD",
        "BINANCE":   "BNB-USD", "BNB":      "BNB-USD", "BNB-USD": "BNB-USD",
        "SOLANA":    "SOL-USD", "SOL":      "SOL-USD", "SOL-USD": "SOL-USD",
        "RIPPLE":    "XRP-USD", "XRP":      "XRP-USD", "XRP-USD": "XRP-USD",
        "CARDANO":   "ADA-USD", "ADA":      "ADA-USD", "ADA-USD": "ADA-USD",
        "DOGECOIN":  "DOGE-USD","DOGE":     "DOGE-USD","DOGE-USD":"DOGE-USD",
        "POLKADOT":  "DOT-USD", "DOT":      "DOT-USD", "DOT-USD": "DOT-USD",
        "LITECOIN":  "LTC-USD", "LTC":      "LTC-USD", "LTC-USD": "LTC-USD",
        "AVALANCHE": "AVAX-USD","AVAX":     "AVAX-USD","AVAX-USD":"AVAX-USD",
        "CHAINLINK": "LINK-USD","LINK":     "LINK-USD","LINK-USD":"LINK-USD",

        # Forex human names
        "EURO":   "EUR/USD", "EUR":    "EUR/USD", "EUR/USD": "EUR/USD",
        "EURUSD": "EUR/USD",
        "POUND":  "GBP/USD", "GBP":    "GBP/USD", "GBP/USD": "GBP/USD",
        "GBPUSD": "GBP/USD",
        "YEN":    "USD/JPY", "JPY":    "USD/JPY", "USD/JPY": "USD/JPY",
        "USDJPY": "USD/JPY",
        "AUD":    "AUD/USD", "AUSSIE": "AUD/USD", "AUD/USD": "AUD/USD",
        "AUDUSD": "AUD/USD",
        "CAD":    "USD/CAD", "LOONIE": "USD/CAD", "USD/CAD": "USD/CAD",
        "USDCAD": "USD/CAD",
        "CHF":    "USD/CHF", "SWISS":  "USD/CHF", "USD/CHF": "USD/CHF",
        "USDCHF": "USD/CHF",
        "NZD":    "NZD/USD", "KIWI":   "NZD/USD", "NZD/USD": "NZD/USD",
        "NZDUSD": "NZD/USD",
        "EUR/GBP": "EUR/GBP", "EURGBP": "EUR/GBP",
        "EUR/JPY": "EUR/JPY", "EURJPY": "EUR/JPY",
        "GBP/JPY": "GBP/JPY", "GBPJPY": "GBP/JPY",
        "AUD/JPY": "AUD/JPY", "AUDJPY": "AUD/JPY",
        "EUR/AUD": "EUR/AUD", "EURAUD": "EUR/AUD",
        "GBP/AUD": "GBP/AUD", "GBPAUD": "GBP/AUD",
        "AUD/CAD": "AUD/CAD", "AUDCAD": "AUD/CAD",
        "CAD/JPY": "CAD/JPY", "CADJPY": "CAD/JPY",
        "CHF/JPY": "CHF/JPY", "CHFJPY": "CHF/JPY",
        "EUR/CAD": "EUR/CAD", "EURCAD": "EUR/CAD",
        "EUR/CHF": "EUR/CHF", "EURCHF": "EUR/CHF",
        "GBP/CAD": "GBP/CAD", "GBPCAD": "GBP/CAD",
        "GBP/CHF": "GBP/CHF", "GBPCHF": "GBP/CHF",

        # Indices human names
        "SP500":    "^GSPC", "S&P":   "^GSPC", "SPX": "^GSPC", "^GSPC": "^GSPC",
        "DOW":      "^DJI",  "DJI":   "^DJI",  "^DJI": "^DJI",
        "NASDAQ":   "^IXIC", "IXIC":  "^IXIC", "^IXIC": "^IXIC",
        "FTSE":     "^FTSE", "UK100": "^FTSE", "^FTSE": "^FTSE",
        "NIKKEI":   "^N225", "N225":  "^N225", "^N225": "^N225",
        "HANGSENG": "^HSI",  "HSI":   "^HSI",  "^HSI":  "^HSI",
        "DAX":      "^GDAXI","GDAXI": "^GDAXI","^GDAXI":"^GDAXI",
        "VIX":      "^VIX",  "FEAR":  "^VIX",  "^VIX":  "^VIX",

        # Stocks human names
        "APPLE":        "AAPL", "AAPL":  "AAPL",
        "MICROSOFT":    "MSFT", "MSFT":  "MSFT",
        "GOOGLE":       "GOOGL","GOOGL": "GOOGL","GOOG": "GOOGL",
        "AMAZON":       "AMZN", "AMZN":  "AMZN",
        "TESLA":        "TSLA", "TSLA":  "TSLA",
        "NVIDIA":       "NVDA", "NVDA":  "NVDA",
        "FACEBOOK":     "META", "META":  "META",
        "JPMORGAN":     "JPM",  "JPM":   "JPM",
        "VISA":         "V",    "V":     "V",
        "MASTERCARD":   "MA",   "MA":    "MA",
        "JOHNSON":      "JNJ",  "JNJ":   "JNJ",
        "PFIZER":       "PFE",  "PFE":   "PFE",
        "WALMART":      "WMT",  "WMT":   "WMT",
        "PROCTER":      "PG",   "PG":    "PG",
        "COCACOLA":     "KO",   "KO":    "KO",
        "EXXON":        "XOM",  "XOM":   "XOM",
        "CHEVRON":      "CVX",  "CVX":   "CVX",
    }

    # ── Category → max concurrent positions ──────────────────────────────────
    _CATEGORY_CAPS: Dict[str, int] = {
        "forex":       3,
        "crypto":      3,
        "commodities": 2,
        "indices":     2,
        "stocks":      2,
    }

    # ── Yahoo Finance fetch ticker overrides ──────────────────────────────────
    # If a canonical ID needs a different symbol for Yahoo data fetching.
    # e.g. forex pairs need EURUSD=X format for some endpoints.
    _YAHOO_FETCH_MAP: Dict[str, str] = {
        "EUR/USD": "EURUSD=X",
        "GBP/USD": "GBPUSD=X",
        "USD/JPY": "USDJPY=X",
        "AUD/USD": "AUDUSD=X",
        "USD/CAD": "USDCAD=X",
        "NZD/USD": "NZDUSD=X",
        "USD/CHF": "USDCHF=X",
        "EUR/GBP": "EURGBP=X",
        "EUR/JPY": "EURJPY=X",
        "GBP/JPY": "GBPJPY=X",
        "AUD/JPY": "AUDJPY=X",
        "EUR/AUD": "EURAUD=X",
        "GBP/AUD": "GBPAUD=X",
        "AUD/CAD": "AUDCAD=X",
        "CAD/JPY": "CADJPY=X",
        "CHF/JPY": "CHFJPY=X",
        "EUR/CAD": "EURCAD=X",
        "EUR/CHF": "EURCHF=X",
        "GBP/CAD": "GBPCAD=X",
        "GBP/CHF": "GBPCHF=X",
    }

    def canonical(self, asset: str) -> str:
        """
        Return the canonical ID for any asset ticker or name.
        Case-insensitive.  Returns the input unchanged if unknown.
        """
        key = asset.upper().strip()
        return self._ALIASES.get(key, asset)

    def category(self, asset: str) -> str:
        """Return the asset category for a canonical or alias ticker."""
        can = self.canonical(asset)
        return self._ASSETS.get(can, "unknown")

    def yahoo_ticker(self, asset: str) -> str:
        """Return the Yahoo Finance fetch ticker for a canonical or alias."""
        can = self.canonical(asset)
        return self._YAHOO_FETCH_MAP.get(can, can)

    def is_same(self, a: str, b: str) -> bool:
        """Return True if two tickers refer to the same underlying asset."""
        return self.canonical(a) == self.canonical(b)

    def is_known(self, asset: str) -> bool:
        """Return True if the asset (or its alias) is in the registry."""
        return self.canonical(asset) in self._ASSETS

    def category_cap(self, category: str) -> int:
        """Maximum concurrent open positions for this category."""
        return self._CATEGORY_CAPS.get(category, 2)

    def all_assets(self) -> List[Tuple[str, str]]:
        """Return list of (canonical_id, category) for all tradeable assets."""
        return list(self._ASSETS.items())

    def assets_by_category(self, category: str) -> List[str]:
        """Return canonical IDs for all assets in a category."""
        return [k for k, v in self._ASSETS.items() if v == category]

    def all_aliases_for(self, canonical_id: str) -> List[str]:
        """Return all known aliases for a canonical ID."""
        target = canonical_id.upper()
        return [
            alias for alias, canon in self._ALIASES.items()
            if canon.upper() == target
        ]


# ── Global singleton ──────────────────────────────────────────────────────────
registry: AssetRegistry = AssetRegistry()