"""
sentiment_analyzer.py — News and social media sentiment analyzer.
Asset-aware, no fake data, no spam logging.
"""
from __future__ import annotations

import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import requests
import yfinance as yf
from bs4 import BeautifulSoup
from textblob import TextBlob

from utils.logger import logger
from config.config import (
    NEWSAPI_KEY, GNEWS_KEY, RAPIDAPI_KEY,
    WHALE_ALERT_KEY, TWITTER_BEARER_TOKEN, ALPHA_VANTAGE_API_KEY,
)
from news_sources import NewsSourceIntegrator
from narrative_ai import ingest as narrative_ingest


def _flatten_df_columns(df):
    """Flatten MultiIndex columns from newer yfinance versions."""
    import pandas as pd
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    return df


class SentimentAnalyzer:

    # ── Singleton ─────────────────────────────────────────────────────────
    _instance: "SentimentAnalyzer | None" = None
    _singleton_lock: threading.Lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if cls._instance is not None:
            return cls._instance
        with cls._singleton_lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if getattr(self, "_initialized", False):
            return
        self._initialized = True

        self.rapidapi_key = RAPIDAPI_KEY
        self.gnews_key    = GNEWS_KEY
        self.newsapi_key  = NEWSAPI_KEY

        # Check FMP key once at init — don't warn every cycle
        self._fmp_key = os.getenv("FMP_API_KEY", "")
        self._fmp_available = bool(self._fmp_key and "your_" not in self._fmp_key.lower())
        if not self._fmp_available:
            logger.info("[SentimentAnalyzer] FMP_API_KEY not set — put/call ratio disabled for US indices")

        self.news_integrator = NewsSourceIntegrator()

        # Reddit — optional, crypto only
        try:
            from reddit_watcher import RedditWatcher
            rw = RedditWatcher()
            self.reddit = rw if rw.enabled else None
            logger.info(f"[SentimentAnalyzer] Reddit: {'ACTIVE' if self.reddit else 'DISABLED'}")
        except Exception as e:
            logger.warning(f"[SentimentAnalyzer] Reddit init failed: {e}")
            self.reddit = None

        # Market calendar — optional
        try:
            from market_calendar import MarketCalendar
            self.market_calendar = MarketCalendar()
            self.market_calendar.fetch_economic_calendar()
            self.market_calendar.fetch_earnings_calendar()
        except Exception as e:
            logger.warning(f"[SentimentAnalyzer] MarketCalendar init failed: {e}")
            self.market_calendar = None

        # Whale manager — optional, get singleton (bot.py starts it)
        try:
            from whale_alert_manager import WhaleAlertManager
            self.whale_manager = WhaleAlertManager()
        except Exception as e:
            logger.warning(f"[SentimentAnalyzer] WhaleAlertManager init failed: {e}")
            self.whale_manager = None

        logger.info(f"[SentimentAnalyzer] Initialised — {len(self.news_integrator.sources)} news sources")

    # ── Reddit helpers ────────────────────────────────────────────────────

    def get_reddit_sentiment(self) -> Optional[Dict]:
        if not self.reddit:
            return None
        try:
            return self.reddit.get_news_sentiment()
        except Exception as e:
            logger.warning(f"[SentimentAnalyzer] Reddit sentiment error: {e}")
            return None

    def get_reddit_sentiment_for_asset(self, asset: str) -> Optional[Dict]:
        if not self.reddit:
            return None
        try:
            return self.reddit.get_market_sentiment_by_asset(asset)
        except Exception as e:
            logger.warning(f"[SentimentAnalyzer] Reddit asset sentiment error for {asset}: {e}")
            return None

    # ── Keyword helpers ───────────────────────────────────────────────────

    def apply_keyword_boost(self, title: str, sentiment: float) -> float:
        tl = title.lower()
        boost = 0.0
        weights = {"very_strong": 0.35, "strong": 0.25, "medium": 0.15, "weak": 0.08}
        very_strong_bull = ["record high", "all-time high", "ath", "breakthrough", "skyrocket", "soar", "milestone"]
        strong_bull      = ["surge", "rally", "bullish", "beat", "outperform", "boom", "breakout"]
        medium_bull      = ["growth", "gain", "jump", "positive", "strong", "rise", "climb", "momentum", "rebound"]
        very_strong_bear = ["crash", "plunge", "bankruptcy", "fraud", "scandal", "liquidation", "insolvency"]
        strong_bear      = ["slump", "downgrade", "warning", "miss", "underperform", "tumble", "selloff"]
        medium_bear      = ["decline", "drop", "fall", "negative", "weak", "loss", "cut", "lower", "correction"]

        for word in very_strong_bull:
            if word in tl: boost += weights["very_strong"]; break
        if boost == 0:
            for word in strong_bull:
                if word in tl: boost += weights["strong"]; break
        if boost == 0:
            for word in medium_bull:
                if word in tl: boost += weights["medium"]; break
        if boost == 0:
            for word in very_strong_bear:
                if word in tl: boost -= weights["very_strong"]; break
        if boost == 0:
            for word in strong_bear:
                if word in tl: boost -= weights["strong"]; break
        if boost == 0:
            for word in medium_bear:
                if word in tl: boost -= weights["medium"]; break

        return max(-1.0, min(1.0, sentiment + boost))

    # ── Whale alerts ──────────────────────────────────────────────────────

    def fetch_whale_alerts(self, min_value_usd: int = 1_000_000) -> List[Dict]:
        """Return real whale alerts. Never returns fake data."""
        if not self.whale_manager:
            # Silent — already logged at init
            return []
        try:
            return self.whale_manager.get_alerts(min_value_usd)
        except Exception as e:
            logger.warning(f"[SentimentAnalyzer] fetch_whale_alerts failed: {e}")
            return []

    # ── AAII ─────────────────────────────────────────────────────────────

    def fetch_aaii_sentiment(self) -> Optional[Dict]:
        """
        Fetch AAII investor sentiment. Cached 6 hours.
        Returns None on failure — never placeholder data.
        """
        _cache_attr      = "_aaii_cache"
        _cache_time_attr = "_aaii_cache_time"
        if hasattr(self, _cache_attr) and hasattr(self, _cache_time_attr):
            if time.time() - getattr(self, _cache_time_attr) < 21600:
                return getattr(self, _cache_attr)

        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,*/*",
        }

        # Source 1: YCharts
        try:
            r = requests.get("https://ycharts.com/indicators/us_investor_sentiment_bullish",
                             headers=headers, timeout=10)
            if r.status_code == 200:
                m = re.search(r'"value"\s*:\s*([\d.]+)', r.text)
                if m:
                    bullish = float(m.group(1))
                    neutral = max(0.0, 100 - bullish - max(20, 60 - bullish))
                    bearish = max(0.0, 100 - bullish - neutral)
                    result  = self._process_aaii_data(bullish, neutral, bearish, "Current Week")
                    if result:
                        setattr(self, _cache_attr, result)
                        setattr(self, _cache_time_attr, time.time())
                        return result
        except Exception as e:
            logger.debug(f"[SentimentAnalyzer] AAII YCharts failed: {e}")

        # Source 2: WSJ
        try:
            r = requests.get("https://www.wsj.com/market-data/stocks/market-sentiment",
                             headers=headers, timeout=10)
            if r.status_code == 200:
                text = BeautifulSoup(r.content, "html.parser").get_text()
                m = re.search(
                    r"Bullish[:\s]+([\d.]+)%.*?Neutral[:\s]+([\d.]+)%.*?Bearish[:\s]+([\d.]+)%",
                    text, re.DOTALL | re.IGNORECASE
                )
                if m:
                    bullish, neutral, bearish = float(m.group(1)), float(m.group(2)), float(m.group(3))
                    result = self._process_aaii_data(bullish, neutral, bearish, "Current Week")
                    if result:
                        setattr(self, _cache_attr, result)
                        setattr(self, _cache_time_attr, time.time())
                        return result
        except Exception as e:
            logger.debug(f"[SentimentAnalyzer] AAII WSJ failed: {e}")

        # Source 3: aaii.com direct
        try:
            session = requests.Session()
            session.headers.update(headers)
            session.get("https://www.aaii.com", timeout=8)
            r = session.get("https://www.aaii.com/sentimentsurvey", timeout=10)
            if r.status_code == 200:
                text = BeautifulSoup(r.content, "html.parser").get_text()
                m = re.search(
                    r"Bullish[:\s]+([\d.]+)%.*?Neutral[:\s]+([\d.]+)%.*?Bearish[:\s]+([\d.]+)%",
                    text, re.DOTALL | re.IGNORECASE
                )
                if m:
                    bullish, neutral, bearish = float(m.group(1)), float(m.group(2)), float(m.group(3))
                    result = self._process_aaii_data(bullish, neutral, bearish, "Current Week")
                    if result:
                        setattr(self, _cache_attr, result)
                        setattr(self, _cache_time_attr, time.time())
                        return result
        except Exception as e:
            logger.debug(f"[SentimentAnalyzer] AAII direct failed: {e}")

        # All sources failed — not an error, just scraping limits
        logger.warning("[SentimentAnalyzer] AAII: all sources unavailable — US index signals proceed without it")
        return None

    def _process_aaii_data(self, bullish: float, neutral: float, bearish: float, date_text: str) -> Optional[Dict]:
        total = bullish + neutral + bearish
        if total < 90 or total > 110:
            logger.warning(
                f"[SentimentAnalyzer] AAII data invalid: total={total:.1f} (expected ~100)"
            )
            return None

        bull_bear_ratio = bullish / bearish if bearish > 0 else 0.0
        sentiment_score = 0.0
        interpretation  = "Neutral"

        if bearish > 50:     sentiment_score, interpretation = 0.7,  "Extremely Bearish (Contrarian Buy)"
        elif bearish > 45:   sentiment_score, interpretation = 0.5,  "Very Bearish (Contrarian Buy)"
        elif bearish > 39:   sentiment_score, interpretation = 0.3,  "Bearish (Contrarian Opportunity)"
        elif bearish > 35:   sentiment_score, interpretation = 0.1,  "Mildly Bearish"
        elif bullish > 50:   sentiment_score, interpretation = -0.7, "Extremely Bullish (Contrarian Caution)"
        elif bullish > 45:   sentiment_score, interpretation = -0.5, "Very Bullish (Contrarian Caution)"
        elif bullish > 39:   sentiment_score, interpretation = -0.3, "Bullish"
        elif bullish > 35:   sentiment_score, interpretation = -0.1, "Mildly Bullish"

        logger.info(
            f"[SentimentAnalyzer] AAII: bull={bullish:.1f}% bear={bearish:.1f}% "
            f"ratio={bull_bear_ratio:.2f} → {interpretation}"
        )
        return {
            "date":            date_text,
            "bullish":         bullish,
            "neutral":         neutral,
            "bearish":         bearish,
            "bull_bear_ratio": round(bull_bear_ratio, 2),
            "sentiment_score": sentiment_score,
            "interpretation":  interpretation,
            "source":          "AAII Sentiment Survey",
        }

    # ── Fear & Greed ──────────────────────────────────────────────────────

    def fetch_fear_greed_index(self) -> Optional[Dict]:
        try:
            r    = requests.get("https://api.alternative.me/fng/", timeout=8)
            data = r.json()
            if "data" in data and data["data"]:
                value = int(data["data"][0]["value"])
                cls   = data["data"][0]["value_classification"]
                if value < 25:   score = -0.9
                elif value < 40: score = -0.6
                elif value < 45: score = -0.3
                elif value < 55: score = 0.0
                elif value < 60: score = 0.3
                elif value < 75: score = 0.6
                else:            score = 0.9
                return {"score": score, "value": value, "classification": cls, "source": "Fear & Greed"}
        except Exception as e:
            logger.warning(f"[SentimentAnalyzer] Fear & Greed unavailable: {e}")
        return None

    def fetch_cnn_fear_greed(self) -> Optional[Dict]:
        # CNN internal API — unreliable, silently skip on failure
        try:
            r   = requests.get(
                "https://production.dataviz.cnn.io/index/fearandgreed/graphdata",
                headers={"User-Agent": "Mozilla/5.0"}, timeout=8
            )
            val = r.json().get("fear_and_greed", {}).get("score", None)
            if val is None:
                return None
            if val < 20:   score, sent = -0.9, "Extreme Fear"
            elif val < 35: score, sent = -0.6, "Fear"
            elif val < 45: score, sent = -0.3, "Mild Fear"
            elif val < 55: score, sent = 0.0,  "Neutral"
            elif val < 65: score, sent = 0.3,  "Mild Greed"
            elif val < 80: score, sent = 0.6,  "Greed"
            else:          score, sent = 0.9,  "Extreme Greed"
            return {"score": score, "value": val, "classification": sent, "source": "CNN Fear & Greed"}
        except Exception:
            pass
        return None

    # ── VIX ───────────────────────────────────────────────────────────────

    def fetch_vix(self) -> Optional[Dict]:
        try:
            data = yf.Ticker("^VIX").history(period="1d")
            if not data.empty:
                data = _flatten_df_columns(data)
                # Handle both capitalised and lowercase column names
                close_col = "Close" if "Close" in data.columns else "close"
                if close_col not in data.columns:
                    return None
                v = float(data[close_col].iloc[-1])
                if v > 28:   score, sent = -0.8, "High Fear"
                elif v > 23: score, sent = -0.5, "Moderate Fear"
                elif v > 18: score, sent = -0.2, "Mild Fear"
                elif v > 14: score, sent = 0.3,  "Complacent"
                else:        score, sent = 0.6,  "Very Complacent"
                return {"score": score, "value": round(v, 2), "classification": sent, "source": "VIX"}
        except Exception as e:
            logger.warning(f"[SentimentAnalyzer] VIX unavailable: {e}")
        return None

    # ── Put/Call Ratio ────────────────────────────────────────────────────

    def fetch_put_call_ratio(self) -> Optional[Dict]:
        """
        Fetch put/call ratio from FMP API.
        Returns None silently if key not set (checked once at init).
        """
        if not self._fmp_available:
            return None
        try:
            r = requests.get(
                f"https://financialmodelingprep.com/api/v4/put_call_ratio?apikey={self._fmp_key}",
                timeout=10
            )
            r.raise_for_status()
            data = r.json()
            if not data or not isinstance(data, list):
                return None
            ratio = float(data[0].get("putCallRatio", 1.0))
            if ratio > 1.0:
                score, interp = -0.6, "Bearish (high put volume)"
            elif ratio > 0.85:
                score, interp = -0.3, "Slightly Bearish"
            elif ratio < 0.65:
                score, interp = 0.6,  "Bullish (high call volume)"
            elif ratio < 0.8:
                score, interp = 0.3,  "Slightly Bullish"
            else:
                score, interp = 0.0,  "Neutral"
            return {"ratio": round(ratio, 2), "score": score,
                    "interpretation": interp, "source": "FMP Put/Call"}
        except Exception:
            pass
        return None

    # ── News sentiment per asset ──────────────────────────────────────────

    def fetch_news_sentiment(self, asset: str, days: int = 1) -> Optional[Dict]:
        search_terms = {
            "BTC-USD": "bitcoin", "ETH-USD": "ethereum", "BNB-USD": "binance",
            "XRP-USD": "xrp ripple", "SOL-USD": "solana",
            "GC=F": "gold price", "SI=F": "silver price", "CL=F": "crude oil",
            "^GSPC": "S&P 500", "^DJI": "Dow Jones", "^IXIC": "Nasdaq", "^FTSE": "FTSE 100",
            "EUR/USD": "euro dollar", "GBP/USD": "pound sterling",
            "USD/JPY": "dollar yen", "AUD/USD": "australian dollar",
            "GBP/JPY": "gbp jpy pound yen", "USD/CAD": "usd cad dollar loonie",
        }
        query = search_terms.get(asset, asset.replace("-", " ").replace("^", ""))
        try:
            r    = requests.get("https://newsapi.org/v2/everything", params={
                "q":        query,
                "from":     (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d"),
                "sortBy":   "relevancy",
                "language": "en",
                "apiKey":   self.newsapi_key,
            }, timeout=10)
            data = r.json()
            if data.get("status") == "ok" and data.get("totalResults", 0) > 0:
                sentiments = []
                for article in data["articles"][:10]:
                    text = f"{article['title']} {article.get('description', '')}"
                    try:
                        narrative_ingest(text, source="newsapi")
                    except Exception:
                        pass
                    s = self.apply_keyword_boost(article["title"], TextBlob(text).sentiment.polarity)
                    sentiments.append(s)
                if sentiments:
                    avg = sum(sentiments) / len(sentiments)
                    return {"score": avg, "articles": len(sentiments),
                            "interpretation": self.interpret_sentiment(avg), "source": "NewsAPI"}
        except Exception as e:
            logger.warning(f"[SentimentAnalyzer] NewsAPI unavailable for {asset}: {e}")
        return None

    def fetch_gnews_sentiment(self, asset: str, days: int = 1) -> Optional[Dict]:
        search_terms = {
            "BTC-USD": "bitcoin", "ETH-USD": "ethereum", "BNB-USD": "binance",
            "XRP-USD": "xrp", "SOL-USD": "solana",
            "GC=F": "gold", "SI=F": "silver", "CL=F": "oil",
            "^GSPC": "S&P 500", "^DJI": "Dow Jones", "^IXIC": "Nasdaq", "^FTSE": "FTSE 100",
            "EUR/USD": "euro dollar", "GBP/USD": "pound sterling",
            "USD/JPY": "dollar yen", "AUD/USD": "australian dollar",
            "GBP/JPY": "pound yen", "USD/CAD": "dollar cad",
        }
        query = search_terms.get(asset, asset.replace("-USD", "").replace("^", ""))
        try:
            r    = requests.get("https://gnews.io/api/v4/search", params={
                "q": query, "lang": "en", "max": 10, "apikey": self.gnews_key,
            }, timeout=10)
            data = r.json()
            if data.get("articles"):
                sentiments = []
                for article in data["articles"][:10]:
                    text = f"{article.get('title', '')} {article.get('description', '')}"
                    try:
                        narrative_ingest(text, source="gnews")
                    except Exception:
                        pass
                    s = self.apply_keyword_boost(article.get("title", ""), TextBlob(text).sentiment.polarity)
                    sentiments.append(s)
                if sentiments:
                    avg = sum(sentiments) / len(sentiments)
                    return {"score": avg, "articles": len(sentiments),
                            "interpretation": self.interpret_sentiment(avg), "source": "GNews"}
        except Exception as e:
            logger.warning(f"[SentimentAnalyzer] GNews unavailable for {asset}: {e}")
        return None

    def fetch_rapidapi_news(self, asset: str, days: int = 1) -> Optional[Dict]:
        symbol_map = {
            "BTC-USD": "BTC:USD", "ETH-USD": "ETH:USD", "BNB-USD": "BNB:USD",
            "XRP-USD": "XRP:USD", "SOL-USD": "SOL:USD",
            "GC=F": "GC:COM", "SI=F": "SI:COM", "CL=F": "CL:COM",
            "^GSPC": "SPX:INDEX", "^DJI": "DJI:INDEX", "^IXIC": "IXIC:NASDAQ", "^FTSE": "FTSE:INDEX",
            "EUR/USD": "EUR:USD", "GBP/USD": "GBP:USD", "USD/JPY": "USD:JPY",
            "AUD/USD": "AUD:USD", "GBP/JPY": "GBP:JPY", "USD/CAD": "USD:CAD",
        }
        api_symbol = symbol_map.get(asset)
        if not api_symbol:
            return None
        try:
            r = requests.get(
                "https://real-time-finance-data.p.rapidapi.com/stock-news",
                headers={"x-rapidapi-key": self.rapidapi_key,
                         "x-rapidapi-host": "real-time-finance-data.p.rapidapi.com"},
                params={"symbol": api_symbol, "language": "en"},
                timeout=10,
            )
            data = r.json()
            news = data.get("data", {}).get("news", [])
            if news:
                sentiments = []
                for item in news[:10]:
                    text = f"{item.get('title', '')} {item.get('summary', '')}"
                    s    = self.apply_keyword_boost(item.get("title", ""), TextBlob(text).sentiment.polarity)
                    sentiments.append(s)
                if sentiments:
                    avg = sum(sentiments) / len(sentiments)
                    return {"score": avg, "articles": len(sentiments),
                            "interpretation": self.interpret_sentiment(avg), "source": "RapidAPI"}
        except Exception as e:
            logger.warning(f"[SentimentAnalyzer] RapidAPI unavailable for {asset}: {e}")
        return None

    def get_best_sentiment(self, asset: str, days: int = 1) -> Optional[Dict]:
        """Try all news sources in parallel; return best result or None."""
        sources = [
            ("rapidapi", lambda: self.fetch_rapidapi_news(asset, days)),
            ("newsapi",  lambda: self.fetch_news_sentiment(asset, days)),
            ("gnews",    lambda: self.fetch_gnews_sentiment(asset, days)),
        ]
        best: Optional[Dict] = None
        best_articles = 0
        with ThreadPoolExecutor(max_workers=3) as pool:
            futures = {pool.submit(fn): name for name, fn in sources}
            for future in as_completed(futures):
                try:
                    result = future.result(timeout=12)
                    if result and result.get("articles", 0) > best_articles:
                        best          = result
                        best_articles = result["articles"]
                except Exception as e:
                    logger.debug(f"[SentimentAnalyzer] {futures[future]} failed: {e}")
        if not best:
            logger.debug(f"[SentimentAnalyzer] No news results for {asset}")
        return best

    # ── On-chain metrics ──────────────────────────────────────────────────

    def fetch_onchain_metrics(self) -> Optional[Dict]:
        try:
            btc  = requests.get("https://api.coinpaprika.com/v1/tickers/btc-bitcoin", timeout=8).json()
            q    = btc.get("quotes", {}).get("USD", {})
            pchg = q.get("percent_change_24h", 0.0)
            vol  = q.get("volume_24h", 0.0)
            if pchg < -1.5: pscore = -0.5
            elif pchg > 1.5: pscore = 0.5
            else:            pscore = 0.0
            vol_mult = min(vol / 1_000_000_000, 2.0)
            combined = pscore * vol_mult
            return {
                "combined_score":   round(combined, 4),
                "price_change_24h": round(pchg, 2),
                "interpretation":   self.interpret_sentiment(combined),
                "source":           "CoinPaprika",
            }
        except Exception as e:
            logger.warning(f"[SentimentAnalyzer] On-chain metrics unavailable: {e}")
        return None

    # ── Interpretation helper ─────────────────────────────────────────────

    def interpret_sentiment(self, score: float) -> str:
        if score > 0.15:  return "Very Bullish"
        if score > 0.03:  return "Bullish"
        if score > -0.03: return "Neutral"
        if score > -0.15: return "Bearish"
        return "Very Bearish"

    _interpret_sentiment = interpret_sentiment

    # ── Main entry point — asset-aware ────────────────────────────────────

    def get_comprehensive_sentiment(self, asset: Optional[str] = None) -> Dict:
        """
        Return sentiment appropriate for the given asset type.
        All sources optional — missing sources are omitted, not faked.
        """
        from core.asset_profiles import (
            is_crypto, is_us_index, is_index, is_forex, is_commodity,
        )

        result: Dict = {
            "score":           0.0,
            "composite_score": 0.0,
            "interpretation":  "Neutral",
            "components":      {},
            "timestamp":       datetime.now().isoformat(),
        }

        scores:     List[float] = []
        weights:    List[float] = []
        components: Dict        = {}

        # ── 1. News (all assets) ──────────────────────────────────────────
        news = self.get_best_sentiment(asset) if asset else self._general_news()
        if news:
            components["news"] = news
            scores.append(news["score"])
            weights.append(0.35)

        # ── 2. Crypto-specific ────────────────────────────────────────────
        if asset and is_crypto(asset):
            fg = self.fetch_fear_greed_index()
            if fg:
                components["fear_greed"] = fg
                scores.append(fg["score"])
                weights.append(0.25)

            oc = self.fetch_onchain_metrics()
            if oc:
                components["onchain"] = oc
                scores.append(oc.get("combined_score", 0.0))
                weights.append(0.20)

            whale_alerts = self.fetch_whale_alerts()
            if whale_alerts:
                ws = sum(a.get("sentiment", 0.0) for a in whale_alerts) / len(whale_alerts)
                components["whale"] = {"score": ws, "count": len(whale_alerts)}
                scores.append(ws)
                weights.append(0.10)

            reddit = self.get_reddit_sentiment()
            if reddit:
                components["reddit"] = reddit
                scores.append(reddit["score"])
                weights.append(0.10)

        # ── 3. US index-specific ──────────────────────────────────────────
        elif asset and is_us_index(asset):
            cnn = self.fetch_cnn_fear_greed()
            if cnn:
                components["cnn_fear_greed"] = cnn
                scores.append(cnn["score"])
                weights.append(0.25)

            vix = self.fetch_vix()
            if vix:
                components["vix"] = vix
                scores.append(vix["score"])
                weights.append(0.20)

            aaii = self.fetch_aaii_sentiment()
            if aaii:
                components["aaii"] = aaii
                scores.append(aaii.get("sentiment_score", 0.0))
                weights.append(0.20)

            pc = self.fetch_put_call_ratio()
            if pc:
                components["put_call"] = pc
                scores.append(pc["score"])
                weights.append(0.20)

        # ── 4. UK index ───────────────────────────────────────────────────
        elif asset and is_index(asset):
            vix = self.fetch_vix()
            if vix:
                components["vix"] = vix
                scores.append(vix["score"])
                weights.append(0.40)

        # ── 5. Forex ─────────────────────────────────────────────────────
        elif asset and is_forex(asset):
            vix = self.fetch_vix()
            if vix:
                components["vix"] = vix
                scores.append(vix["score"])
                weights.append(0.30)

        # ── 6. Commodity / general ────────────────────────────────────────
        else:
            cnn = self.fetch_cnn_fear_greed()
            if cnn:
                components["cnn_fear_greed"] = cnn
                scores.append(cnn["score"])
                weights.append(0.30)

        # ── Weighted composite ────────────────────────────────────────────
        if scores:
            total_w   = sum(weights)
            composite = sum(s * w for s, w in zip(scores, weights)) / total_w
        else:
            composite = 0.0

        result["score"]           = round(composite, 4)
        result["composite_score"] = round(composite, 4)
        result["interpretation"]  = self.interpret_sentiment(composite)
        result["components"]      = components

        return result

    def _general_news(self) -> Optional[Dict]:
        try:
            articles = self.news_integrator.fetch_all_sources()
            for a in (articles or []):
                try:
                    narrative_ingest(a.get("title", ""), source="news_integrator")
                except Exception:
                    pass
            if articles:
                avg = sum(a.get("sentiment", 0.0) for a in articles) / len(articles)
                return {"score": avg, "articles": len(articles),
                        "interpretation": self.interpret_sentiment(avg), "source": "NewsIntegrator"}
        except Exception as e:
            logger.warning(f"[SentimentAnalyzer] General news failed: {e}")
        return None

    def get_market_events(self) -> Dict:
        if not self.market_calendar:
            return {"error": "Market calendar not available"}
        try:
            self.market_calendar.fetch_economic_calendar()
            self.market_calendar.fetch_earnings_calendar()
            events  = self.market_calendar.get_high_impact_events(days=7)
            fmt_evt = []
            for ev in events[:5]:
                days_out = (ev["date"] - datetime.now()).days
                fmt_evt.append({
                    "name":     ev["event"],
                    "days":     days_out,
                    "date":     ev["date"].strftime("%Y-%m-%d"),
                    "impact":   ev["impact"],
                    "forecast": ev["forecast"],
                    "previous": ev["previous"],
                })
            risk = self.market_calendar.should_reduce_risk()
            return {
                "events":       fmt_evt,
                "risk_outlook": {
                    "multiplier":     risk["risk_multiplier"],
                    "reduce_trading": risk["reduce_trading"],
                },
            }
        except Exception as e:
            logger.warning(f"[SentimentAnalyzer] get_market_events failed: {e}")
            return {"error": str(e)}

    def get_trading_signal(self, asset: str) -> Dict:
        sentiment = self.get_best_sentiment(asset)
        if not sentiment:
            return {"signal": "HOLD", "confidence": 0.5, "score": 0.0, "source": "none"}
        score = sentiment["score"]
        if score > 0.3:
            return {"signal": "BUY",  "confidence": round(min(0.5 + score * 0.5, 0.95), 2),
                    "score": score, "source": sentiment["source"]}
        if score < -0.3:
            return {"signal": "SELL", "confidence": round(min(0.5 + abs(score) * 0.5, 0.95), 2),
                    "score": score, "source": sentiment["source"]}
        return {"signal": "HOLD", "confidence": 0.5, "score": score, "source": sentiment["source"]}
