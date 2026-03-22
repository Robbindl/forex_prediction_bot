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
    ITICK_TOKEN, OILPRICE_API_KEY,
    CACHE_TTL, LOOKBACK_PERIOD,
)

logger = get_logger()

# ── Interval maps ──────────────────────────────────────────────────────────────
_YF_INTERVAL_MAP = {"1d": "1d", "1h": "1h", "15m": "15m", "4h": "60m"}
_TD_INTERVAL_MAP = {"1m": "1min", "5m": "5min", "15m": "15min", "30m": "30min", "45m": "45min", "1h": "1h", "2h": "2h", "4h": "4h", "8h": "8h", "1d": "1day", "1w": "1week"}

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

# ── iTick symbol maps — real-time, free, covers all categories ────────────────
# Base URL: https://api.itick.org
# Headers:  {"accept": "application/json", "token": ITICK_TOKEN}
# kType:    1=1m, 2=5m, 3=15m, 4=30m, 5=1h, 8=1d
_ITICK_KTYPE_MAP = {
    "1m": 1, "5m": 2, "15m": 3, "30m": 4,
    "1h": 5, "2h": 6, "4h": 7, "1d": 8, "1w": 9,
}
_ITICK_FOREX_MAP = {
    "EUR/USD": ("EURUSD", "GB"), "GBP/USD": ("GBPUSD", "GB"),
    "GBP/JPY": ("GBPJPY", "GB"), "AUD/USD": ("AUDUSD", "GB"),
    "USD/JPY": ("USDJPY", "GB"), "USD/CAD": ("USDCAD", "GB"),
    "USD/CHF": ("USDCHF", "GB"), "NZD/USD": ("NZDUSD", "GB"),
    "EUR/GBP": ("EURGBP", "GB"), "AUD/JPY": ("AUDJPY", "GB"),
}
_ITICK_CRYPTO_MAP = {
    "BTC-USD": ("BTCUSDT", "BA"), "ETH-USD": ("ETHUSDT", "BA"),
    "BNB-USD": ("BNBUSDT", "BA"), "SOL-USD": ("SOLUSDT", "BA"),
    "XRP-USD": ("XRPUSDT", "BA"),
}
_ITICK_COMMODITY_MAP = {
    "GC=F": ("XAUUSD", "GB"),  # Gold — traded as forex pair on iTick
    "SI=F": ("XAGUSD", "GB"),  # Silver — same
    # CL=F (Oil) handled by OilPrice API then yfinance
}
_ITICK_INDEX_MAP = {
    "^GSPC": ("SPX",  "GB"),  # S&P 500
    "^DJI":  ("DJI",  "GB"),  # Dow Jones
    "^IXIC": ("IXIC", "GB"),  # Nasdaq
    # ^FTSE → yfinance (dropped from free plan to save slots)
}

def _itick_symbol(asset: str, category: str):
    """Return (code, region) tuple for iTick, or None if not mapped."""
    if category == "forex":       return _ITICK_FOREX_MAP.get(asset)
    if category == "crypto":      return _ITICK_CRYPTO_MAP.get(asset)
    if category == "commodities": return _ITICK_COMMODITY_MAP.get(asset)
    if category == "indices":     return _ITICK_INDEX_MAP.get(asset)
    return None

# ── TwelveData symbol maps (forex only on free plan) ──────────────────────────
_TD_FOREX_MAP = {
    "EUR/USD": "EUR/USD", "GBP/USD": "GBP/USD",
    "USD/JPY": "USD/JPY", "USD/CHF": "USD/CHF",
    "AUD/USD": "AUD/USD", "USD/CAD": "USD/CAD",
    "NZD/USD": "NZD/USD", "EUR/GBP": "EUR/GBP",
    "GBP/JPY": "GBP/JPY", "AUD/JPY": "AUD/JPY",
}
_TD_COMMODITY_MAP = {}   # Commodities need Grow plan — Alpha Vantage used instead

# ── Alpha Vantage commodity symbol map ────────────────────────────────────────
# Free plan: 25 requests/day, real-time commodity prices
_AV_COMMODITY_MAP = {
    "GC=F": "WTI",        # Gold → AV uses XAU but commodity endpoint uses different format
    "SI=F": "NATURAL_GAS", # placeholder — AV commodity endpoint
    "CL=F": "WTI",        # Crude Oil WTI
}
# AV physical commodity symbols via CURRENCY endpoint (more reliable on free plan)
_AV_FOREX_COMMODITY = {
    "GC=F": "XAU",   # Gold vs USD
    "SI=F": "XAG",   # Silver vs USD
}
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
    Waterfall data priority (each falls through to the next on failure):
      ALL categories  : iTick (real-time, 120/min free, covers forex/crypto/indices/commodities)
      Forex/Crypto    : TwelveData (real-time, free plan)
      Commodities     : Alpha Vantage FX (XAU/XAG real-time, free)
      Oil (CL=F)      : OilPrice API (real-time, 1000/month free)
      All             : yfinance (15-min delay universal fallback)
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
        # Alpha Vantage — used for real-time commodity OHLCV (free plan)
        self._av_key = ALPHA_VANTAGE_API_KEY if ALPHA_VANTAGE_API_KEY else None
        if self._av_key:
            logger.info("[DataFetcher] Alpha Vantage ready for commodities")
        # iTick — real-time, covers forex/crypto/indices/commodities, 120/min free
        self._itick_key = ITICK_TOKEN if ITICK_TOKEN else None
        if self._itick_key:
            logger.info("[DataFetcher] iTick ready — real-time for all categories")
        # OilPrice API — crude oil CL=F only, 1000/month free
        self._oilprice_key = OILPRICE_API_KEY if OILPRICE_API_KEY else None
        if self._oilprice_key:
            logger.info("[DataFetcher] OilPrice API ready for CL=F")

    # ── OHLCV ──────────────────────────────────────────────────────────────────

    def get_ohlcv(
        self,
        asset: str,
        category: str,
        interval: str = "",           # empty = use TRADING_TIMEFRAME from config
        periods: int = 0,              # 0 = auto-select based on interval
    ) -> Optional[pd.DataFrame]:
        # Resolve defaults from config so one env-var change affects everything
        if not interval:
            try:
                from config.config import TRADING_TIMEFRAME
                interval = TRADING_TIMEFRAME
            except Exception:
                interval = "15m"
        if periods == 0:
            # More bars needed for intraday — 500 × 15m ≈ 5 trading days
            _auto = {"15m": 500, "1h": 300, "4h": 200, "1d": LOOKBACK_PERIOD}
            periods = _auto.get(interval, LOOKBACK_PERIOD)
        cache_key = f"ohlcv:{asset}:{interval}"
        cached    = cache.get(cache_key)
        if cached is not None:
            return cached

        df = None

        # ── Waterfall: try each source in order, use first that succeeds ────────

        # 1. iTick — real-time, covers ALL categories (forex/crypto/indices/commodities)
        if df is None and self._itick_key:
            itick_info = _itick_symbol(asset, category)
            if itick_info:
                df = self._fetch_itick(itick_info[0], itick_info[1], interval, periods, category)

        # 2. TwelveData — forex + crypto backup
        if df is None and self._td_client and category in ("forex", "crypto"):
            td_sym = _td_symbol(asset, category)
            if td_sym:
                df = self._fetch_td(td_sym, interval, periods)

        # 3. Alpha Vantage — Gold/Silver backup
        if df is None and category == "commodities" and self._av_key:
            df = self._fetch_av_commodity(asset, interval, periods)

        # 4. OilPrice API — CL=F crude oil backup
        # OilPrice API only returns a single price point — not usable for OHLCV history.
        # It is wired into get_real_time_price() only (for current CL=F price checks).
        # yfinance handles CL=F OHLCV history reliably.

        # Universal fallback — 15-min delay but always works
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

    def _fetch_itick(self, code: str, region: str, interval: str, periods: int, category: str) -> Optional[pd.DataFrame]:
        """
        Fetch OHLCV from iTick API.
        Confirmed endpoints (docs.itick.org):
          Forex/Commodities → GET /forex/kline?region=GB&code=EURUSD&kType=3&limit=500
          Crypto            → GET /crypto/kline?region=BA&code=BTCUSDT&kType=3&limit=500
          Indices           → GET /indices/kline?region=GB&code=SPX&kType=3&limit=500
        kType: 1=1m,2=5m,3=15m,4=30m,5=1h,6=2h,7=4h,8=1d,9=1w
        Response: {code:0, data:[{o,h,l,c,v,t,tu}]} — newest first
        """
        try:
            import requests as _req
            ktype = _ITICK_KTYPE_MAP.get(interval, 3)
            if category in ("forex", "commodities"):
                endpoint = "https://api.itick.org/forex/kline"
            elif category == "crypto":
                endpoint = "https://api.itick.org/crypto/kline"
            elif category == "indices":
                endpoint = "https://api.itick.org/indices/kline"
            else:
                return None

            r = _req.get(
                endpoint,
                params={"region": region, "code": code, "kType": ktype, "limit": periods},
                headers={"accept": "application/json", "token": self._itick_key},
                timeout=10,
            )
            if r.status_code != 200:
                logger.debug(f"[DataFetcher] iTick HTTP {r.status_code} for {code}")
                return None

            resp = r.json()
            # iTick returns code=0 on success
            if resp.get("code") != 0:
                logger.debug(f"[DataFetcher] iTick error for {code}: {resp.get('msg')}")
                return None

            candles = resp.get("data", [])
            if not candles:
                return None

            rows = []
            for c in candles:
                # Confirmed fields: o=open, h=high, l=low, c=close, v=volume, t=timestamp
                o = float(c.get("o") or 0)
                h = float(c.get("h") or 0)
                l = float(c.get("l") or 0)
                cl = float(c.get("c") or 0)
                v = float(c.get("v") or 0)
                if cl == 0:  # skip bad bars
                    continue
                rows.append({"open": o, "high": h, "low": l, "close": cl, "volume": v})

            if not rows:
                return None

            # iTick returns newest first — reverse to oldest-first for strategy compatibility
            df = pd.DataFrame(rows[::-1]).reset_index(drop=True)
            logger.debug(f"[DataFetcher] iTick: {code} {len(df)} bars ({interval})")
            return df

        except Exception as e:
            logger.debug(f"[DataFetcher] iTick {code}: {e}")
            return None

    def _fetch_oilprice(self, interval: str, periods: int) -> Optional[pd.DataFrame]:
        """
        Fetch crude oil WTI price from OilPrice API.
        Free plan: 1,000 requests/month. Returns single price — built into OHLCV.
        Only used for CL=F when iTick fails.
        """
        try:
            import requests as _req
            # OilPrice API returns latest price only on free tier
            r = _req.get(
                "https://api.oilpriceapi.com/v1/prices/latest",
                headers={"Authorization": f"Token {self._oilprice_key}",
                         "Content-Type": "application/json"},
                timeout=10,
            )
            if r.status_code != 200:
                logger.debug(f"[DataFetcher] OilPrice API HTTP {r.status_code}")
                return None
            data = r.json()
            price = float(data.get("data", {}).get("price", 0))
            if not price:
                return None
            # Single price point — wrap in minimal OHLCV for compatibility
            # Downstream code will still work; signal quality unaffected for latest bar
            row = {"open": price, "high": price, "low": price, "close": price, "volume": 0.0}
            logger.debug(f"[DataFetcher] OilPrice API: CL=F @ {price}")
            # Return as 1-row df — fetcher will fall through to yfinance for history
            return pd.DataFrame([row])

        except Exception as e:
            logger.debug(f"[DataFetcher] OilPrice API: {e}")
            return None

    def _fetch_av_commodity(self, asset: str, interval: str, periods: int) -> Optional[pd.DataFrame]:
        """
        Fetch commodity OHLCV from Alpha Vantage.
        Uses the FX_INTRADAY endpoint for Gold/Silver (XAU/USD, XAG/USD)
        and TIME_SERIES_INTRADAY for crude oil.
        Free plan: 25 requests/day — only called when cache misses.
        """
        try:
            import requests as _req
            # Map interval to AV format
            _AV_INTERVAL_MAP = {
                "1m": "1min", "5m": "5min", "15m": "15min",
                "30m": "30min", "1h": "60min", "4h": "60min",
                "1d": None,  # use daily endpoint
            }
            av_interval = _AV_INTERVAL_MAP.get(interval, "15min")

            # Gold and Silver — use FX endpoint (XAU/USD, XAG/USD)
            if asset in _AV_FOREX_COMMODITY:
                from_sym = _AV_FOREX_COMMODITY[asset]
                if av_interval is None:
                    url = "https://www.alphavantage.co/query"
                    params = {
                        "function":    "FX_DAILY",
                        "from_symbol": from_sym,
                        "to_symbol":   "USD",
                        "outputsize":  "compact",
                        "apikey":      self._av_key,
                    }
                    r = _req.get(url, params=params, timeout=15)
                    data = r.json().get("Time Series FX (Daily)", {})
                    key_map = {"1. open": "open", "2. high": "high",
                               "3. low": "low", "4. close": "close"}
                else:
                    url = "https://www.alphavantage.co/query"
                    params = {
                        "function":    "FX_INTRADAY",
                        "from_symbol": from_sym,
                        "to_symbol":   "USD",
                        "interval":    av_interval,
                        "outputsize":  "compact",
                        "apikey":      self._av_key,
                    }
                    r = _req.get(url, params=params, timeout=15)
                    data = r.json().get(f"Time Series FX ({av_interval})", {})
                    key_map = {"1. open": "open", "2. high": "high",
                               "3. low": "low", "4. close": "close"}

                if not data:
                    # Check for API limit message
                    msg = str(r.json())
                    if "Thank you for using Alpha Vantage" in msg or "premium" in msg.lower():
                        logger.debug(f"[DataFetcher] Alpha Vantage daily limit reached for {asset}")
                    return None

                rows = []
                for ts, vals in sorted(data.items(), reverse=True)[:periods]:
                    rows.append({
                        "open":   float(vals["1. open"]),
                        "high":   float(vals["2. high"]),
                        "low":    float(vals["3. low"]),
                        "close":  float(vals["4. close"]),
                        "volume": 0.0,
                    })
                if not rows:
                    return None
                df = pd.DataFrame(rows[::-1]).reset_index(drop=True)
                logger.info(f"[DataFetcher] Alpha Vantage real-time: {asset} ({len(df)} bars)")
                return df

            # Crude Oil (CL=F) — use commodity endpoint
            if asset == "CL=F":
                if av_interval is None:
                    url = "https://www.alphavantage.co/query"
                    params = {
                        "function": "WTI",
                        "interval": "daily",
                        "apikey":   self._av_key,
                    }
                    r = _req.get(url, params=params, timeout=15)
                    data_list = r.json().get("data", [])
                    rows = []
                    for entry in data_list[:periods]:
                        val = entry.get("value", ".")
                        if val == ".": continue
                        price = float(val)
                        rows.append({
                            "open": price, "high": price,
                            "low":  price, "close": price, "volume": 0.0,
                        })
                    if not rows:
                        return None
                    return pd.DataFrame(rows[::-1]).reset_index(drop=True)
                else:
                    # Intraday oil — AV doesn't have intraday commodity, fall through to yfinance
                    return None

        except Exception as e:
            logger.debug(f"[DataFetcher] Alpha Vantage commodity {asset}: {e}")
        return None

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

        # OilPrice API — real-time CL=F price (1,000/month free, single price point)
        # Only used here for current price display, NOT for OHLCV history.
        # Rate limit: 1,000/month = ~33/day. We cache for 30 minutes to stay well within.
        if result is None and asset == "CL=F" and self._oilprice_key:
            _op_cache_key = "oilprice:CL=F:rt"
            _op_cached = cache.get(_op_cache_key)
            if _op_cached is not None:
                result = (_op_cached, "OilPriceAPI:cached")
            else:
                try:
                    import requests as _req
                    r = _req.get(
                        "https://api.oilpriceapi.com/v1/prices/latest",
                        headers={"Authorization": f"Token {self._oilprice_key}",
                                 "Content-Type": "application/json"},
                        timeout=5,
                    )
                    if r.status_code == 200:
                        p = float(r.json().get("data", {}).get("price", 0) or 0)
                        if p:
                            cache.set(_op_cache_key, p, ttl=3600)  # 60-min cache = ~24 calls/day, well under 33/day limit
                            result = (p, "OilPriceAPI")
                except Exception as _oe:
                    logger.debug(f"[DataFetcher] OilPrice API: {_oe}")

        # iTick — real-time price for all categories
        # Confirmed endpoint: /forex/quote?region=GB&code=EURUSD → data.ld = last price
        if result is None and self._itick_key:
            itick_info = _itick_symbol(asset, category)
            if itick_info:
                try:
                    import requests as _req
                    code, region = itick_info
                    if category in ("forex", "commodities"):
                        ep = "https://api.itick.org/forex/quote"
                    elif category == "crypto":
                        ep = "https://api.itick.org/crypto/quote"
                    elif category == "indices":
                        ep = "https://api.itick.org/indices/quote"
                    else:
                        ep = None
                    if ep:
                        r = _req.get(
                            ep,
                            params={"region": region, "code": code},
                            headers={"accept": "application/json", "token": self._itick_key},
                            timeout=5,
                        )
                        if r.status_code == 200:
                            resp = r.json()
                            if resp.get("code") == 0:
                                d = resp.get("data", {})
                                # quote response: data = {ld: last_price, o: open, ...}
                                p = float(d.get("ld", 0) or 0)
                                if p:
                                    result = (p, "iTick")
                except Exception as _ie:
                    logger.debug(f"[DataFetcher] iTick quote {asset}: {_ie}")

        # TwelveData — real-time price for crypto (no delay, free plan)
        if result is None and self._td_client and category in ("forex", "crypto"):
            td_sym = _td_symbol(asset, category)
            if td_sym:
                try:
                    price_data = self._td_client.price(symbol=td_sym).as_json()
                    price = float(price_data.get("price", 0))
                    if price > 0:
                        result = (price, price * 0.0002)  # estimated spread
                except Exception as e:
                    logger.debug(f"[DataFetcher] TwelveData real-time {td_sym}: {e}")

        # Finnhub — free tier supports crypto only
        if result is None and self._fh_client and category in _FINNHUB_CATEGORIES:
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