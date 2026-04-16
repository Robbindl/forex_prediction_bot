from __future__ import annotations

from typing import Dict, List, Optional, Tuple


class AssetRegistry:

    # ── Canonical → category ──────────────────────────────────────────────────
    _ASSETS: Dict[str, str] = {
        # ── Commodities ──────────────────────────────────────────
        "XAU/USD": "commodities",  # Gold
        "XAG/USD": "commodities",  # Silver
        "WTI": "commodities",      # WTI Crude Oil

        # ── Forex ─────────────────────────────────────────────────
        "EUR/USD": "forex",
        "EUR/JPY": "forex",
        "EUR/GBP": "forex",
        "GBP/JPY": "forex",
        "GBP/USD": "forex",
        "AUD/USD": "forex",
        "NZD/USD": "forex",
        "USD/JPY": "forex",
        "USD/CAD": "forex",
        "USD/CHF": "forex",

        # ── Indices ───────────────────────────────────────────────
        "US30":   "indices",       # US30  — Dow Jones
        "US100":  "indices",       # US100 — Nasdaq
        "US500":  "indices",       # US500 — S&P 500
        "UK100":  "indices",       # FTSE 100
        "GER40":  "indices",       # GER40 — Germany 40 / DAX
        "AUS200": "indices",       # AUS200 — Australia 200 / ASX
        "JPN225": "indices",       # JPN225 — Japan 225 / Nikkei

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
        "XAU/USD": "XAU/USD", "GC=F": "XAU/USD", "GOLD": "XAU/USD", "XAU": "XAU/USD",
        "XAUUSD": "XAU/USD",

        # Silver
        "XAG/USD": "XAG/USD", "SI=F": "XAG/USD", "SILVER": "XAG/USD", "XAG": "XAG/USD",
        "XAGUSD": "XAG/USD",

        # Oil
        "WTI": "WTI", "WTI/USD": "WTI", "CL=F": "WTI", "OIL": "WTI", "CRUDE": "WTI",
        "BRENT": "WTI",

        # EUR/USD
        "EUR/USD": "EUR/USD", "EURUSD": "EUR/USD",
        "EURO": "EUR/USD", "EUR": "EUR/USD",

        # EUR/JPY
        "EUR/JPY": "EUR/JPY", "EURJPY": "EUR/JPY",
        "EUROYEN": "EUR/JPY",

        # EUR/GBP
        "EUR/GBP": "EUR/GBP", "EURGBP": "EUR/GBP",
        "EUROSTERLING": "EUR/GBP",

        # GBP/JPY
        "GBP/JPY": "GBP/JPY", "GBPJPY": "GBP/JPY",

        # GBP/USD
        "GBP/USD": "GBP/USD", "GBPUSD": "GBP/USD",
        "POUND": "GBP/USD", "GBP": "GBP/USD", "CABLE": "GBP/USD",

        # AUD/USD
        "AUD/USD": "AUD/USD", "AUDUSD": "AUD/USD",
        "AUD": "AUD/USD", "AUSSIE": "AUD/USD",

        # NZD/USD
        "NZD/USD": "NZD/USD", "NZDUSD": "NZD/USD",
        "NZD": "NZD/USD", "KIWI": "NZD/USD",

        # USD/JPY
        "USD/JPY": "USD/JPY", "USDJPY": "USD/JPY",
        "YEN": "USD/JPY", "JPY": "USD/JPY",

        # USD/CAD
        "USD/CAD": "USD/CAD", "USDCAD": "USD/CAD",
        "CAD": "USD/CAD", "LOONIE": "USD/CAD",

        # USD/CHF
        "USD/CHF": "USD/CHF", "USDCHF": "USD/CHF",
        "CHF": "USD/CHF", "SWISSY": "USD/CHF",

        # US30 — Dow Jones
        "US30": "US30", "^DJI": "US30", "DOW": "US30",
        "DJI": "US30", "DOWJONES": "US30",

        # US100 — Nasdaq
        "US100": "US100", "^IXIC": "US100", "NASDAQ": "US100",
        "NAS100": "US100", "NDX": "US100", "IXIC": "US100",

        # US500 — S&P 500
        "US500": "US500", "^GSPC": "US500", "SP500": "US500",
        "S&P": "US500", "SPX": "US500", "GSPC": "US500",

        # FTSE
        "UK100": "UK100", "^FTSE": "UK100", "FTSE": "UK100",
        "FTSE100": "UK100",

        # GER40 — DAX
        "GER40": "GER40", "DE40": "GER40", "DAX": "GER40",
        "DAX40": "GER40", "GERMANY40": "GER40",

        # AUS200 — Australia 200
        "AUS200": "AUS200", "AU200": "AUS200", "ASX200": "AUS200",
        "AUSTRALIA200": "AUS200",

        # JPN225 — Japan 225 / Nikkei
        "JPN225": "JPN225", "JP225": "JPN225", "JAPAN225": "JPN225",
        "NIKKEI": "JPN225", "NIKKEI225": "JPN225",

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

    # ── Category → soft target concurrent open positions ──────────────────────
    _CATEGORY_CAPS: Dict[str, int] = {
        "forex":       4,
        "crypto":      4,
        "commodities": 4,
        "indices":     4,
    }

    def canonical(self, asset: str) -> str:
        key = asset.upper().strip()
        return self._ALIASES.get(key, asset)

    def category(self, asset: str) -> str:
        can = self.canonical(asset)
        if can == "WTI":
            return "commodities"
        return self._ASSETS.get(can, "unknown")

    def is_same(self, a: str, b: str) -> bool:
        return self.canonical(a) == self.canonical(b)

    def is_known(self, asset: str) -> bool:
        can = self.canonical(asset)
        return can in self._ASSETS or can == "WTI"

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
