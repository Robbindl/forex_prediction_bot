"""data/fetcher.py — Unified market data fetcher. Rewrite of data/fetcher.py."""
from __future__ import annotations
import time
from typing import Dict, Optional, Tuple
import pandas as pd
import yfinance as yf
from data.cache import cache
from utils.logger import get_logger
from config.config import (
    FINNHUB_API_KEY, TWELVE_DATA_API_KEY, ALPHA_VANTAGE_API_KEY,
    CACHE_TTL, LOOKBACK_PERIOD,
)

logger = get_logger()

_YF_INTERVAL_MAP = {"1d": "1d", "1h": "1h", "15m": "15m", "4h": "60m"}
_FOREX_SUFFIX    = {"EUR/USD": "EURUSD=X", "GBP/USD": "GBPUSD=X",
                    "USD/JPY": "JPY=X",    "USD/CHF": "CHF=X",
                    "AUD/USD": "AUDUSD=X", "USD/CAD": "CAD=X",
                    "NZD/USD": "NZDUSD=X", "EUR/GBP": "EURGBP=X",
                    "GBP/JPY": "GBPJPY=X", "AUD/JPY": "AUDJPY=X"}
_COMMODITY_MAP   = {"XAU/USD": "GC=F", "XAG/USD": "SI=F",
                    "WTI/USD": "CL=F",  "NG/USD":  "NG=F", "XCU/USD": "HG=F"}


def _normalize_symbol(asset: str, category: str) -> str:
    if category == "forex":
        return _FOREX_SUFFIX.get(asset, asset.replace("/", "") + "=X")
    if category == "commodities":
        return _COMMODITY_MAP.get(asset, asset)
    return asset


class DataFetcher:
    """
    Fetches OHLCV and real-time price data.
    Priority: Twelve Data → Finnhub → yfinance (always available).
    """

    def __init__(self):
        self._td_client = None
        self._fh_client = None
        self._init_clients()

    def _init_clients(self) -> None:
        if TWELVE_DATA_API_KEY:
            try:
                import twelvedata as td
                self._td_client = td.TDClient(apikey=TWELVE_DATA_API_KEY)
            except Exception:
                pass
        if FINNHUB_API_KEY:
            try:
                import finnhub
                self._fh_client = finnhub.Client(api_key=FINNHUB_API_KEY)
            except Exception:
                pass

    # ── OHLCV ─────────────────────────────────────────────────────────────────

    def get_ohlcv(
        self,
        asset: str,
        category: str,
        interval: str = "1d",
        periods: int = LOOKBACK_PERIOD,
    ) -> Optional[pd.DataFrame]:
        cache_key = f"ohlcv:{asset}:{interval}"
        cached    = cache.get(cache_key)
        if cached is not None:
            return cached

        symbol = _normalize_symbol(asset, category)
        df     = None

        # Try Twelve Data first
        if self._td_client and category in ("forex", "crypto", "commodities"):
            df = self._fetch_td(symbol, interval, periods)

        # Fallback: yfinance
        if df is None:
            df = self._fetch_yf(symbol, interval, periods)

        if df is not None and not df.empty:
            cache.set(cache_key, df, ttl=CACHE_TTL)

        return df

    def _fetch_td(self, symbol: str, interval: str, periods: int) -> Optional[pd.DataFrame]:
        try:
            ts = self._td_client.time_series(
                symbol=symbol, interval=interval, outputsize=periods, timezone="UTC"
            )
            df = ts.as_pandas()
            df = df.rename(columns=str.lower)
            df = df[["open", "high", "low", "close", "volume"]].astype(float)
            return df.iloc[::-1].reset_index(drop=True)
        except Exception as e:
            logger.debug(f"[Fetcher] TwelveData {symbol}: {e}")
            return None

    def _fetch_yf(self, symbol: str, interval: str, periods: int) -> Optional[pd.DataFrame]:
        try:
            yf_interval = _YF_INTERVAL_MAP.get(interval, "1d")
            period_map  = {"1d": "3mo", "1h": "1mo", "15m": "5d", "60m": "1mo"}
            yf_period   = period_map.get(yf_interval, "3mo")

            ticker = yf.Ticker(symbol)
            df     = ticker.history(period=yf_period, interval=yf_interval, auto_adjust=True)
            if df.empty:
                return None
            df = df.rename(columns={"Open": "open", "High": "high", "Low": "low",
                                    "Close": "close", "Volume": "volume"})
            df = df[["open", "high", "low", "close", "volume"]].tail(periods)
            return df.reset_index(drop=True)
        except Exception as e:
            logger.debug(f"[Fetcher] yfinance {symbol}: {e}")
            return None

    # ── Real-time price ───────────────────────────────────────────────────────

    def get_real_time_price(
        self, asset: str, category: str
    ) -> Tuple[Optional[float], Optional[float]]:
        """Returns (price, spread) or (None, None)."""
        cache_key = f"rt:{asset}"
        cached    = cache.get(cache_key)
        if cached:
            return cached

        symbol = _normalize_symbol(asset, category)
        result = None

        # Finnhub
        if self._fh_client:
            try:
                q = self._fh_client.quote(symbol)
                if q and q.get("c"):
                    price  = float(q["c"])
                    spread = abs(float(q.get("h", price)) - float(q.get("l", price))) * 0.1
                    result = (price, spread)
            except Exception:
                pass

        # yfinance fallback
        if result is None:
            try:
                ticker = yf.Ticker(symbol)
                hist   = ticker.history(period="1d", interval="1m")
                if not hist.empty:
                    price  = float(hist["Close"].iloc[-1])
                    spread = float(hist["High"].iloc[-1] - hist["Low"].iloc[-1]) * 0.1
                    result = (price, spread)
            except Exception:
                pass

        if result:
            cache.set(cache_key, result, ttl=15)
        return result or (None, None)

    # ── Multi-asset batch ─────────────────────────────────────────────────────

    def get_prices_batch(self, assets: Dict[str, str]) -> Dict[str, float]:
        """assets = {asset: category}. Returns {asset: price}."""
        prices = {}
        for asset, category in assets.items():
            try:
                price, _ = self.get_real_time_price(asset, category)
                if price:
                    prices[asset] = price
                time.sleep(0.05)
            except Exception:
                pass
        return prices