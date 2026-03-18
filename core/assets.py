from __future__ import annotations

from typing import Dict, List, Optional, Tuple


class AssetRegistry:

    # ── Canonical → category ──────────────────────────────────────────────────
    _ASSETS: Dict[str, str] = {
        # ── Commodities ──────────────────────────────────────────
        "GC=F":   "commodities",   # Gold
        "SI=F":   "commodities",   # Silver
        "CL=F":   "commodities",   # Crude Oil

        # ── Forex ─────────────────────────────────────────────────
        "EUR/USD": "forex",
        "GBP/JPY": "forex",
        "GBP/USD": "forex",
        "AUD/USD": "forex",
        "USD/JPY": "forex",
        "USD/CAD": "forex",

        # ── Indices ───────────────────────────────────────────────
        "^DJI":   "indices",       # US30  — Dow Jones
        "^IXIC":  "indices",       # US100 — Nasdaq
        "^GSPC":  "indices",       # US500 — S&P 500
        "^FTSE":  "indices",       # FTSE 100

        # ── Crypto ────────────────────────────────────────────────
        "BTC-USD": "crypto",
        "ETH-USD": "crypto",
        "BNB-USD": "crypto",
        "SOL-USD": "crypto",
        "XRP-USD": "crypto",
    }

    # ── Alias → canonical ─────────────────────────────────────────────────────
    _ALIASES: Dict[str, str] = {
        # Gold
        "GC=F": "GC=F", "GOLD": "GC=F", "XAU": "GC=F",
        "XAU/USD": "GC=F", "XAUUSD": "GC=F",

        # Silver
        "SI=F": "SI=F", "SILVER": "SI=F", "XAG": "SI=F",
        "XAG/USD": "SI=F", "XAGUSD": "SI=F",

        # Oil
        "CL=F": "CL=F", "OIL": "CL=F", "CRUDE": "CL=F",
        "WTI": "CL=F", "WTI/USD": "CL=F", "BRENT": "CL=F",

        # EUR/USD
        "EUR/USD": "EUR/USD", "EURUSD": "EUR/USD",
        "EURO": "EUR/USD", "EUR": "EUR/USD",

        # GBP/JPY
        "GBP/JPY": "GBP/JPY", "GBPJPY": "GBP/JPY",

        # GBP/USD
        "GBP/USD": "GBP/USD", "GBPUSD": "GBP/USD",
        "POUND": "GBP/USD", "GBP": "GBP/USD", "CABLE": "GBP/USD",

        # AUD/USD
        "AUD/USD": "AUD/USD", "AUDUSD": "AUD/USD",
        "AUD": "AUD/USD", "AUSSIE": "AUD/USD",

        # USD/JPY
        "USD/JPY": "USD/JPY", "USDJPY": "USD/JPY",
        "YEN": "USD/JPY", "JPY": "USD/JPY",

        # USD/CAD
        "USD/CAD": "USD/CAD", "USDCAD": "USD/CAD",
        "CAD": "USD/CAD", "LOONIE": "USD/CAD",

        # US30 — Dow Jones
        "^DJI": "^DJI", "US30": "^DJI", "DOW": "^DJI",
        "DJI": "^DJI", "DOWJONES": "^DJI",

        # US100 — Nasdaq
        "^IXIC": "^IXIC", "US100": "^IXIC", "NASDAQ": "^IXIC",
        "NAS100": "^IXIC", "NDX": "^IXIC", "IXIC": "^IXIC",

        # US500 — S&P 500
        "^GSPC": "^GSPC", "US500": "^GSPC", "SP500": "^GSPC",
        "S&P": "^GSPC", "SPX": "^GSPC", "GSPC": "^GSPC",

        # FTSE
        "^FTSE": "^FTSE", "FTSE": "^FTSE", "UK100": "^FTSE",
        "FTSE100": "^FTSE",

        # BTC
        "BTC-USD": "BTC-USD", "BTC": "BTC-USD", "BITCOIN": "BTC-USD",
        "BTCUSD": "BTC-USD",

        # ETH
        "ETH-USD": "ETH-USD", "ETH": "ETH-USD", "ETHEREUM": "ETH-USD",
        "ETHUSD": "ETH-USD",

        # BNB
        "BNB-USD": "BNB-USD", "BNB": "BNB-USD", "BINANCE": "BNB-USD",
        "BNBUSD": "BNB-USD",

        # SOL
        "SOL-USD": "SOL-USD", "SOL": "SOL-USD", "SOLANA": "SOL-USD",
        "SOLUSD": "SOL-USD",

        # XRP
        "XRP-USD": "XRP-USD", "XRP": "XRP-USD", "RIPPLE": "XRP-USD",
        "XRPUSD": "XRP-USD",
    }

    # ── Category → max concurrent open positions ──────────────────────────────
    _CATEGORY_CAPS: Dict[str, int] = {
        "forex":       3,
        "crypto":      3,
        "commodities": 2,
        "indices":     2,
    }

    # ── Yahoo Finance fetch ticker overrides ──────────────────────────────────
    _YAHOO_FETCH_MAP: Dict[str, str] = {
        "EUR/USD": "EURUSD=X",
        "GBP/JPY": "GBPJPY=X",
        "GBP/USD": "GBPUSD=X",
        "AUD/USD": "AUDUSD=X",
        "USD/JPY": "USDJPY=X",
        "USD/CAD": "USDCAD=X",
    }

    def canonical(self, asset: str) -> str:
        key = asset.upper().strip()
        return self._ALIASES.get(key, asset)

    def category(self, asset: str) -> str:
        can = self.canonical(asset)
        return self._ASSETS.get(can, "unknown")

    def yahoo_ticker(self, asset: str) -> str:
        can = self.canonical(asset)
        return self._YAHOO_FETCH_MAP.get(can, can)

    def is_same(self, a: str, b: str) -> bool:
        return self.canonical(a) == self.canonical(b)

    def is_known(self, asset: str) -> bool:
        return self.canonical(asset) in self._ASSETS

    def category_cap(self, category: str) -> int:
        return self._CATEGORY_CAPS.get(category, 2)

    def all_assets(self) -> List[Tuple[str, str]]:
        """Return list of (canonical_id, category) for all tradeable assets."""
        return list(self._ASSETS.items())

    def assets_by_category(self, category: str) -> List[str]:
        return [k for k, v in self._ASSETS.items() if v == category]

    def all_aliases_for(self, canonical_id: str) -> List[str]:
        target = canonical_id.upper()
        return [
            alias for alias, canon in self._ALIASES.items()
            if canon.upper() == target
        ]


# ── Global singleton ──────────────────────────────────────────────────────────
registry: AssetRegistry = AssetRegistry()