"""data/fetcher.py — Unified market data fetcher."""
from __future__ import annotations
import time
import warnings
warnings.filterwarnings("ignore", category=FutureWarning, module="yfinance")
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

# ── Interval maps ──────────────────────────────────────────────────────────────
_YF_INTERVAL_MAP = {"1d": "1d", "1h": "1h", "15m": "15m", "4h": "60m"}
_TD_INTERVAL_MAP = {"1d": "1day", "1h": "1h", "15m": "15min", "4h": "4h"}

# ── yfinance symbol maps ───────────────────────────────────────────────────────
_FOREX_SUFFIX = {
    "EUR/USD": "EURUSD=X", "GBP/USD": "GBPUSD=X",
    "USD/JPY": "JPY=X",    "USD/CHF": "CHF=X",
    "AUD/USD": "AUDUSD=X", "USD/CAD": "CAD=X",
    "NZD/USD": "NZDUSD=X", "EUR/GBP": "EURGBP=X",
    "GBP/JPY": "GBPJPY=X", "AUD/JPY": "AUDJPY=X",
}
_COMMODITY_MAP = {
    "XAU/USD": "GC=F", "XAG/USD": "SI=F",
    "WTI/USD": "CL=F", "NG/USD":  "NG=F", "XCU/USD": "HG=F",
}

# ── TwelveData symbol maps (forex only on free plan) ──────────────────────────
_TD_FOREX_MAP = {
    "EUR/USD": "EUR/USD", "GBP/USD": "GBP/USD",
    "USD/JPY": "USD/JPY", "USD/CHF": "USD/CHF",
    "AUD/USD": "AUD/USD", "USD/CAD": "USD/CAD",
    "NZD/USD": "NZD/USD", "EUR/GBP": "EUR/GBP",
    "GBP/JPY": "GBP/JPY", "AUD/JPY": "AUD/JPY",
}
_TD_COMMODITY_MAP = {}   # Commodities need Grow plan — yfinance used instead
_TD_CRYPTO_MAP = {
    "BTC-USD": "BTC/USD", "ETH-USD": "ETH/USD",
    "BNB-USD": "BNB/USD", "SOL-USD": "SOL/USD",
    "XRP-USD": "XRP/USD",
}

# Futures/indices have no 1-min intraday on Yahoo free tier
_NO_INTRADAY = {"commodities", "indices"}

# Finnhub free tier only supports crypto
_FINNHUB_CATEGORIES = {"crypto"}


def _yf_symbol(asset: str, category: str) -> str:
    if category == "forex":
        return _FOREX_SUFFIX.get(asset, asset.replace("/", "") + "=X")
    if category == "commodities":
        return _COMMODITY_MAP.get(asset, asset)
    return asset


def _td_symbol(asset: str, category: str) -> Optional[str]:
    if category == "forex":
        return _TD_FOREX_MAP.get(asset)
    if category == "commodities":
        return _TD_COMMODITY_MAP.get(asset)
    if category == "crypto":
        return _TD_CRYPTO_MAP.get(asset)
    return None


def _flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Flatten MultiIndex columns from newer yfinance versions."""
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    return df


class DataFetcher:
    """
    Fetches OHLCV and real-time price data.
    Priority: TwelveData (forex only, free plan) -> Finnhub (crypto only) -> yfinance.
    Returns None on failure — never fake data.
    """

    def __init__(self):
        self._td_client = None
        self._fh_client = None
        self._init_clients()

    def _init_clients(self) -> None:
        if TWELVE_DATA_API_KEY:
            for attempt in range(2):
                try:
                    import twelvedata as td
                    self._td_client = td.TDClient(apikey=TWELVE_DATA_API_KEY)
                    break
                except Exception as e:
                    if attempt == 0:
                        logger.warning(f"[DataFetcher] TwelveData init failed, retrying in 5s: {e}")
                        import time; time.sleep(5)
                    else:
                        logger.warning(f"[DataFetcher] TwelveData unavailable — falling back to yfinance: {e}")
        if FINNHUB_API_KEY:
            try:
                import finnhub
                self._fh_client = finnhub.Client(api_key=FINNHUB_API_KEY)
            except Exception as e:
                logger.error(f"[DataFetcher] Finnhub client init failed: {e}")

    # ── OHLCV ──────────────────────────────────────────────────────────────────

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

        df = None

        # TwelveData — forex only on free plan
        if self._td_client and category == "forex":
            td_sym = _td_symbol(asset, category)
            if td_sym:
                df = self._fetch_td(td_sym, interval, periods)

        # yfinance fallback
        if df is None:
            yf_sym = _yf_symbol(asset, category)
            df = self._fetch_yf(yf_sym, interval, periods)

        if df is not None and not df.empty:
            cache.set(cache_key, df, ttl=CACHE_TTL)
            self._ping_health("technicals")

        return df

    def _fetch_td(self, symbol: str, interval: str, periods: int) -> Optional[pd.DataFrame]:
        try:
            td_interval = _TD_INTERVAL_MAP.get(interval, interval)
            ts = self._td_client.time_series(
                symbol=symbol, interval=td_interval, outputsize=periods, timezone="UTC"
            )
            df = ts.as_pandas()
            df = df.rename(columns=str.lower)
            # Forex pairs have no volume — add a zero column so downstream code is safe
            cols = [c for c in ["open", "high", "low", "close", "volume"] if c in df.columns]
            df = df[cols].astype(float)
            if "volume" not in df.columns:
                df["volume"] = 0.0
            return df.iloc[::-1].reset_index(drop=True)
        except Exception as e:
            msg = str(e).lower()
            if "invalid api key" in msg or ("api key" in msg and "quota" not in msg):
                logger.error("[DataFetcher] TwelveData disabled — invalid API key. Check TWELVEDATA_KEY.")
                self._td_client = None
            elif "run out of api credits" in msg or "quota" in msg:
                logger.debug(f"[DataFetcher] TwelveData quota reached for {symbol} — falling back to yfinance")
            elif "symbol or figi" in msg:
                logger.warning(f"[DataFetcher] TwelveData symbol unavailable on your plan: {symbol}")
            elif "invalid" in msg and "interval" in msg:
                logger.warning(f"[DataFetcher] TwelveData invalid interval for {symbol}: {e}")
            else:
                logger.error(f"[DataFetcher] TwelveData fetch failed for {symbol}: {e}")
            return None

    def _fetch_yf(self, symbol: str, interval: str, periods: int) -> Optional[pd.DataFrame]:
        try:
            yf_interval = _YF_INTERVAL_MAP.get(interval, "1d")
            period_map  = {
                "1d": "6mo", "1h": "60d", "15m": "60d",
                "60m": "60d", "1m": "7d", "5m": "60d",
            }
            yf_period = period_map.get(yf_interval, "6mo")
            ticker    = yf.Ticker(symbol)

            try:
                df = ticker.history(period=yf_period, interval=yf_interval, auto_adjust=True)
            except TypeError:
                # Yahoo returned empty/null chart data — temporary outage or rate limit
                logger.debug(f"[DataFetcher] yfinance chart unavailable for {symbol} — skipping")
                return None

            # Futures sometimes need a longer window
            if df.empty and yf_interval == "1d":
                try:
                    df = ticker.history(period="1y", interval="1d", auto_adjust=True)
                except TypeError:
                    logger.debug(f"[DataFetcher] yfinance chart unavailable for {symbol} (1y) — skipping")
                    return None

            if df.empty:
                logger.debug(f"[DataFetcher] yfinance returned empty for {symbol}")
                return None

            df = _flatten_columns(df)
            df = df.rename(columns={
                "Open": "open", "High": "high", "Low": "low",
                "Close": "close", "Volume": "volume",
            })

            available = [c for c in ["open", "high", "low", "close", "volume"] if c in df.columns]
            if "close" not in available:
                logger.warning(f"[DataFetcher] yfinance missing close column for {symbol}")
                return None

            return df[available].tail(periods)

        except Exception as e:
            logger.error(f"[DataFetcher] yfinance OHLCV failed for {symbol}: {e}")
            return None

    # ── Real-time price ────────────────────────────────────────────────────────

    def get_real_time_price(
        self, asset: str, category: str
    ) -> Tuple[Optional[float], Optional[float]]:
        """Returns (price, spread) or (None, None). Never fake data."""
        cache_key = f"rt:{asset}"
        cached    = cache.get(cache_key)
        if cached:
            return cached

        yf_sym = _yf_symbol(asset, category)
        result = None

        # Finnhub — free tier supports crypto only
        if self._fh_client and category in _FINNHUB_CATEGORIES:
            try:
                q = self._fh_client.quote(yf_sym)
                if q and q.get("c"):
                    price  = float(q["c"])
                    spread = abs(float(q.get("h", price)) - float(q.get("l", price))) * 0.1
                    result = (price, spread)
            except Exception as e:
                logger.warning(f"[DataFetcher] Finnhub quote failed for {yf_sym}: {e}")

        # yfinance fallback
        if result is None:
            try:
                ticker = yf.Ticker(yf_sym)

                try:
                    if category in _NO_INTRADAY:
                        hist = ticker.history(period="5d", interval="1d", auto_adjust=True)
                    else:
                        hist = ticker.history(period="1d", interval="1m", auto_adjust=True)
                except TypeError:
                    logger.debug(f"[DataFetcher] yfinance real-time unavailable for {yf_sym} — skipping")
                    return (None, None)

                if not hist.empty:
                    hist = _flatten_columns(hist)

                    # Handle both capitalized and lowercase column names
                    close_col = "Close" if "Close" in hist.columns else "close"
                    high_col  = "High"  if "High"  in hist.columns else "high"
                    low_col   = "Low"   if "Low"   in hist.columns else "low"

                    if close_col not in hist.columns:
                        logger.warning(f"[DataFetcher] yfinance missing Close for {yf_sym}")
                    else:
                        price = float(hist[close_col].iloc[-1])
                        if high_col in hist.columns and low_col in hist.columns:
                            spread = float(hist[high_col].iloc[-1] - hist[low_col].iloc[-1]) * 0.1
                        else:
                            spread = price * 0.001
                        result = (price, spread)
                else:
                    logger.debug(f"[DataFetcher] yfinance real-time empty for {yf_sym}")

            except Exception as e:
                logger.error(f"[DataFetcher] yfinance real-time failed for {yf_sym}: {e}")

        if result:
            cache.set(cache_key, result, ttl=15)
            self._ping_health("technicals")
            return result

        return (None, None)

    # ── Multi-asset batch ──────────────────────────────────────────────────────

    def get_prices_batch(self, assets: Dict[str, str]) -> Dict[str, float]:
        """assets = {asset: category}. Returns {asset: price} for successful fetches only."""
        prices = {}
        for asset, category in assets.items():
            try:
                price, _ = self.get_real_time_price(asset, category)
                if price is not None:
                    prices[asset] = price
                time.sleep(0.05)
            except Exception as e:
                logger.error(f"[DataFetcher] Batch price failed for {asset}: {e}")
        return prices

    # ── Health monitor ping ────────────────────────────────────────────────────

    @staticmethod
    def _ping_health(source: str) -> None:
        try:
            from monitoring.system_health_service import monitor
            monitor.ping_source(source)
        except Exception:
            pass
