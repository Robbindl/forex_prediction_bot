from __future__ import annotations

"""Sentiment collectors and scoring helpers used by the sentiment service."""

import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

from utils.logger import get_logger

logger = get_logger()

# ── API key imports ───────────────────────────────────────────────────────────
try:
    from config.config import (
        NEWSAPI_KEY, GNEWS_KEY, RAPIDAPI_KEY,
        ALPHA_VANTAGE_API_KEY, FINNHUB_API_KEY,
        SENTIMENT_MAX_AGE_HOURS,
    )
except ImportError:
    NEWSAPI_KEY = GNEWS_KEY = RAPIDAPI_KEY = ALPHA_VANTAGE_API_KEY = FINNHUB_API_KEY = ""

# ── QUOTA MANAGEMENT ──────────────────────────────────────────────────────────
# Track daily API usage to avoid hitting quotas. Reset at midnight UTC.
class _QuotaManager:
    """Thread-safe quota tracking for news APIs."""
    _lock = threading.Lock()
    _reset_date = datetime.utcnow().date()
    
    # Daily limits — use most of free tier (reserve small margin for other apps)
    # NOTE: NewsAPI not tracked (worked fine before quota manager, no need to throttle)
    _LIMITS = {
        "newsapi":      999,   # No tracking — NewsAPI has generous free tier
        "gnews":        90,    # Free: 100/day, use 90
        "av":           22,    # Free: 25/day, use 22 (preserve 3 for OHLCV fetches)
        "finnhub":      18,    # Free: ~20/day, use 18 to be safe
    }
    
    _usage = {
        "newsapi": 0,
        "gnews": 0,
        "av": 0,
        "finnhub": 0,
    }
    
    _disabled = {k: False for k in _LIMITS.keys()}
    
    @classmethod
    def check_reset(cls):
        """Reset counters if day changed."""
        with cls._lock:
            today = datetime.utcnow().date()
            if today > cls._reset_date:
                cls._reset_date = today
                cls._usage = {k: 0 for k in cls._usage.keys()}
                cls._disabled = {k: False for k in cls._disabled.keys()}
    
    @classmethod
    def can_call(cls, api: str) -> bool:
        """Check if API has quota remaining. Always allow—APIs handle their own limits."""
        cls.check_reset()
        with cls._lock:
            # Always return True; APIs have built-in rate limiting/quota handling
            # We track for monitoring only, not enforcement
            return True
    
    @classmethod
    def record_call(cls, api: str) -> None:
        """Record API call usage."""
        with cls._lock:
            cls._usage[api] = cls._usage.get(api, 0) + 1
    
    @classmethod
    def disable_api(cls, api: str, reason: str) -> None:
        """Log quota warning but don't disable — let APIs handle their own limits."""
        logger.debug(f"[Quota] {api.upper()} reported: {reason}")
    
    @classmethod
    def get_status(cls) -> Dict[str, Dict]:
        """Return quota status."""
        cls.check_reset()
        with cls._lock:
            return {
                api: {
                    "used": cls._usage.get(api, 0),
                    "limit": cls._LIMITS.get(api, 0),
                    "remaining": cls._LIMITS.get(api, 0) - cls._usage.get(api, 0),
                    "disabled": cls._disabled.get(api, False),
                }
                for api in cls._LIMITS.keys()
            }


# ── Asset keyword map — used to filter news articles per asset ────────────────
_ASSET_KEYWORDS: Dict[str, List[str]] = {
    "BTC-USD":  ["bitcoin", "btc", "crypto bitcoin", "satoshi"],
    "ETH-USD":  ["ethereum", "eth", "ether", "defi ethereum"],
    "BNB-USD":  ["bnb", "binance coin", "binance smart chain"],
    "SOL-USD":  ["solana", "sol crypto"],
    "XRP-USD":  ["xrp", "ripple", "ripplenet"],
    "XAU/USD":  ["gold", "xau", "bullion", "gold price", "precious metal",
                 "gold futures", "gold rally", "gold drop", "gold tumble",
                 "gold slump", "gold sell", "gold surge", "gold crash"],
    "XAG/USD":  ["silver", "xag", "silver price", "precious metal", "silver bullion"],
    "WTI":      ["crude oil", "wti", "brent", "oil price", "opec",
                 "petroleum", "energy market", "oil barrel"],
    "EUR/USD":  ["euro", "eur/usd", "eurusd", "eurozone", "ecb",
                 "european central bank", "europe economy"],
    "EUR/JPY":  ["eur/jpy", "eurjpy", "euro yen", "euro against yen",
                 "ecb", "boj", "bank of japan", "japanese yen"],
    "GBP/USD":  ["pound", "sterling", "gbp/usd", "gbpusd", "cable",
                 "bank of england", "boe", "uk economy"],
    "GBP/JPY":  ["gbpjpy", "pound yen", "gbp/jpy"],
    "AUD/USD":  ["australian dollar", "aud", "audusd", "rba",
                 "reserve bank australia"],
    "USD/JPY":  ["yen", "usdjpy", "usd/jpy", "bank of japan", "boj",
                 "japanese yen", "japan economy"],
    "USD/CAD":  ["canadian dollar", "cad", "loonie", "usdcad",
                 "bank of canada", "boc"],
    "US30":     ["dow jones", "djia", "dow", "us30", "wall street"],
    "US100":    ["nasdaq", "us100", "ndx", "tech stocks", "technology sector"],
    "US500":    ["s&p 500", "sp500", "spx", "s&p", "us stocks",
                 "us equity", "wall street", "american stocks"],
    "UK100":    ["ftse", "ftse 100", "uk100", "london stock",
                 "uk equity", "british stocks"],
}

_CATEGORY_MAP = {
    "BTC-USD": "crypto", "ETH-USD": "crypto", "BNB-USD": "crypto",
    "SOL-USD": "crypto", "XRP-USD": "crypto",
    "XAU/USD": "commodities", "XAG/USD": "commodities", "WTI": "commodities",
    "EUR/USD": "forex", "EUR/JPY": "forex", "GBP/USD": "forex", "GBP/JPY": "forex",
    "AUD/USD": "forex", "USD/JPY": "forex", "USD/CAD": "forex",
    "US30": "indices", "US100": "indices", "US500": "indices", "UK100": "indices",
}

_LEGACY_ASSET_ALIASES = {
    "GC=F": "XAU/USD",
    "SI=F": "XAG/USD",
    "CL=F": "WTI",
    "WTI/USD": "WTI",
    "^DJI": "US30",
    "^IXIC": "US100",
    "^GSPC": "US500",
    "^FTSE": "UK100",
}


def _canon_asset(asset: str) -> str:
    return _LEGACY_ASSET_ALIASES.get(asset, asset)


def _is_quota_error(e) -> bool:
    msg = str(e).lower()
    return any(x in msg for x in [
        "too many requests", "429", "rate limit", "quota",
        "upgrade to a paid plan", "requests over a 24 hour",
        "developer accounts are limited", "exceeded",
    ])


def _cat(asset: str) -> str:
    return _CATEGORY_MAP.get(_canon_asset(asset), "forex")


def _clamp(v: float) -> float:
    """Clamp to -1.0 … +1.0."""
    return max(-1.0, min(1.0, v))


# ══════════════════════════════════════════════════════════════════════════════
# Signal collectors
# ══════════════════════════════════════════════════════════════════════════════

class _MarketInstruments:
    """
    Real market instruments — VIX, Fear & Greed, AAII, Put/Call.
    These move WITH the market. When VIX spikes, market is fearful = bearish.
    Fear & Greed below 20 = extreme fear = contrarian BUY signal.
    """

    _cache: Dict[str, Tuple[Any, float]] = {}
    _lock  = threading.Lock()
    _TTL   = 900  # 15 min — these change slowly

    @classmethod
    def _cached(cls, key: str, fn):
        with cls._lock:
            hit = cls._cache.get(key)
            if hit and time.time() < hit[1]:
                return hit[0]
        result = fn()
        if result is not None:
            with cls._lock:
                cls._cache[key] = (result, time.time() + cls._TTL)
        return result

    # ── CNN / Alternative.me Fear & Greed ────────────────────────────────────
    @classmethod
    def fear_greed(cls) -> Optional[Dict]:
        def _fetch():
            try:
                r = requests.get(
                    "https://api.alternative.me/fng/?limit=1",
                    timeout=8, headers={"User-Agent": "Mozilla/5.0"}
                )
                if r.status_code == 200:
                    d = r.json()["data"][0]
                    val = int(d["value"])
                    # Convert 0-100 scale to -1…+1
                    # <20 = extreme fear = contrarian bullish (+0.4)
                    # >80 = extreme greed = contrarian bearish (-0.4)
                    # 45-55 = neutral
                    if val <= 20:    score =  0.3 + (20 - val) / 20 * 0.3   # +0.3 to +0.6
                    elif val <= 40:  score =  0.1 + (40 - val) / 20 * 0.2   # +0.1 to +0.3
                    elif val <= 60:  score =  0.0 + (val - 50) / 10 * 0.05  # -0.05 to +0.05
                    elif val <= 80:  score = -0.1 - (val - 60) / 20 * 0.2   # -0.1 to -0.3
                    else:            score = -0.3 - (val - 80) / 20 * 0.3   # -0.3 to -0.6
                    return {
                        "value": val,
                        "classification": d["value_classification"],
                        "score": round(_clamp(score), 3),
                    }
            except Exception as e:
                if not _is_quota_error(e):
                    logger.debug(f"[Sentiment] Fear&Greed fetch: {e}")
            return None
        return cls._cached("fg", _fetch)

    # ── VIX (CBOE Volatility Index) ───────────────────────────────────────────
    @classmethod
    def vix(cls) -> Optional[Dict]:
        def _fetch():
            # Deriv-only market data mode does not use external VIX snapshots.
            return None
        return cls._cached("vix", _fetch)

    # ── AAII Sentiment Survey ─────────────────────────────────────────────────
    @classmethod
    def aaii(cls) -> Optional[Dict]:
        """Weekly survey — cached 24h. Returns None if unavailable."""
        def _fetch():
            sources = [
                "https://www.aaii.com/files/surveys/sentiment.xls",
                "https://api.stockanalysis.com/stocks/aaii",
            ]
            for url in sources:
                try:
                    r = requests.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
                    if r.status_code == 200:
                        import io
                        try:
                            import pandas as pd
                            df = pd.read_excel(io.BytesIO(r.content), skiprows=3)
                            df = df.dropna(subset=[df.columns[0]])
                            bull = float(df.iloc[-1, 1]) if len(df.columns) > 1 else 38.0
                            bear = float(df.iloc[-1, 3]) if len(df.columns) > 3 else 30.0
                            # Bull-bear spread: >10 bullish, <-10 bearish
                            spread = bull - bear
                            score  = _clamp(spread / 50)
                            return {
                                "bullish": round(bull, 1), "bearish": round(bear, 1),
                                "spread": round(spread, 1), "score": round(score, 3),
                            }
                        except Exception:
                            pass
                except Exception:
                    pass
            return None
        return cls._cached("aaii", _fetch)

    # ── Put/Call Ratio ────────────────────────────────────────────────────────
    @classmethod
    def put_call(cls) -> Optional[Dict]:
        def _fetch():
            # Deriv-only market data mode does not use external options chains.
            return None
        return cls._cached("pc", _fetch)


class _PriceMomentum:
    """
    Price momentum as sentiment proxy.
    Price movement IS market sentiment — no NLP ambiguity.
    XAU/USD down 5% = bearish for gold. Period.
    """

    _cache: Dict[str, Tuple[Any, float]] = {}
    _lock  = threading.Lock()
    _TTL   = 300  # 5 min

    @classmethod
    def get(cls, asset: str) -> Optional[float]:
        with cls._lock:
            hit = cls._cache.get(asset)
            if hit and time.time() < hit[1]:
                return hit[0]

        score = cls._compute(asset)
        if score is not None:
            with cls._lock:
                cls._cache[asset] = (score, time.time() + cls._TTL)
        return score

    @classmethod
    def _compute(cls, asset: str) -> Optional[float]:
        try:
            import numpy as np
            from data.fetcher import get_shared_fetcher

            df = get_shared_fetcher().get_ohlcv(asset, _cat(asset), interval="1d", periods=5)
            if df is None or df.empty or len(df) < 2:
                return None
            close = df["close"].astype(float)
            # Weighted multi-horizon momentum
            r1  = (close.iloc[-1] - close.iloc[-2]) / close.iloc[-2]   # 1-day
            r5  = (close.iloc[-1] - close.iloc[0])  / close.iloc[0]    # 5-day
            # Intraday-first: 85% weight on today vs yesterday, 15% on 5-day trend
            score = _clamp(r1 * 5 * 0.85 + r5 * 2 * 0.15)
            return round(score, 3)
        except Exception as e:
            logger.debug(f"[Sentiment] Price momentum {asset}: {e}")
            return None


class _NewsSentiment:
    """
    News sentiment — asset-filtered headlines from multiple sources.
    Financial context scoring: adjusts for commodity/equity polarity mismatch.
    Uses simple but effective financial keyword boosters.
    
    Caching: 1 hour (reduced API calls per signal — sentiment changes slowly).
    Quota management: Each news API respects daily limits with fallback logic.
    """

    _cache: Dict[str, Tuple[Any, float]] = {}
    _lock  = threading.Lock()
    _TTL   = 3600  # 1 hour — news sentiment changes slowly, reduce API calls
    _MAX_AGE_HOURS = max(1, min(12, SENTIMENT_MAX_AGE_HOURS))  # Use 6-12h as safe range in config

    # Words that score BEARISH in financial headlines
    _BEARISH_WORDS = {
        # Price action
        "crash", "crashes", "collapse", "collapses", "plunge", "plunges",
        "tumble", "tumbles", "drop", "drops", "fall", "falls", "sink", "sinks",
        "slump", "slumps", "decline", "declines", "dip", "dips", "dump", "dumps",
        "selloff", "sell-off", "correction", "tank", "tanks", "tanking",
        "bleeding", "bleed", "wipe", "wiped", "erased",
        # Sentiment / fear
        "loss", "losses", "fear", "fears", "panic", "crisis", "crises",
        "recession", "depression", "downturn", "downgrades", "downgrade",
        "concern", "concerns", "warning", "warns", "worried", "worry",
        "threat", "threatens", "risk", "risks", "risky", "vulnerable",
        "uncertainty", "uncertain", "volatile", "volatility",
        # Geopolitical / macro
        "war", "wars", "conflict", "conflicts", "tension", "tensions",
        "strike", "attack", "invasion", "invaded", "escalation", "escalates",
        "sanctions", "sanctioned", "tariff", "tariffs", "ban", "banned",
        "catastrophe", "disaster", "emergency", "assassination",
        # Business / earnings
        "miss", "misses", "disappoint", "disappointing", "disappoints",
        "weak", "weakness", "below", "shortfall", "deficit", "negative",
        "cut", "cuts", "cutting", "layoff", "layoffs", "fired", "bankrupt",
        "bankruptcy", "insolvent", "default", "defaulted", "shutdown",
        "halt", "halted", "suspend", "suspended", "freeze", "frozen",
        "fraud", "scam", "hack", "hacked", "exploit", "exploited", "stolen",
        "investigation", "probe", "lawsuit", "sued", "charges", "arrested",
        # Crypto-specific
        "rug", "rugpull", "depegged", "depeg", "liquidated", "liquidation",
        "death", "dying", "dead", "worthless", "ponzi", "bubble",
        # Macro indicators bearish
        "inflation", "stagflation", "unemployment", "tightening",
        "hawkish", "overtightening", "contagion",
    }

    # Words that score BULLISH in financial headlines
    _BULLISH_WORDS = {
        # Price action
        "rally", "rallies", "surge", "surges", "rise", "rises", "rising",
        "gain", "gains", "soar", "soars", "jump", "jumps", "climb", "climbs",
        "recover", "recovery", "recovers", "rebound", "rebounds", "bounce",
        "breakout", "breakthrough", "explode", "moon", "mooning",
        "accelerate", "accelerating", "spike", "spiked",
        # Sentiment / confidence
        "strong", "strength", "strengthens", "beat", "beats", "exceed",
        "exceeds", "outperform", "outperforms", "positive", "positively",
        "optimism", "optimistic", "confident", "confidence", "bullish",
        "upbeat", "enthusiasm", "enthusiastic",
        # Growth / fundamentals
        "growth", "grows", "growing", "boom", "booming", "expansion",
        "upgrade", "upgraded", "lifted", "boost", "boosted", "stimulus",
        "demand", "adoption", "milestone", "record", "high", "all-time",
        "ath", "profit", "profits", "earnings", "revenue", "inflow", "inflows",
        # Institutional / macro bullish
        "approval", "approved", "etf", "institutional", "investment",
        "halving", "accumulation", "accumulating", "buyback", "buying",
        "partnership", "deal", "agreement", "merger", "acquisition",
        "dovish", "easing", "cut", "rate-cut", "stimulus", "bailout",
        "resolution", "ceasefire", "truce", "peace", "deal",
        # Crypto-specific
        "launched", "launch", "mainnet", "upgrade", "integration",
        "listed", "listing", "staking", "yield", "airdrop",
    }

    # Commodity-specific: "surges" for oil/gold is NOT bullish for equities
    # These words should be treated as NEUTRAL for non-commodity assets
    _COMMODITY_WORDS = {
        "surge", "surges", "surge in", "oil surge", "gold rally", "rally in oil",
    }

    # ── Macro event keywords — posts containing these affect multiple assets ──
    # If a Reddit post title contains any of these AND has high engagement,
    # it is treated as a macro event and its signal is applied cross-asset.
    _MACRO_BEARISH = {
        "war", "strike", "attack", "invasion", "conflict", "explosion",
        "crisis", "recession", "depression", "collapse", "default",
        "sanctions", "emergency", "shutdown", "contagion", "pandemic",
        "catastrophe", "disaster", "assassination", "coup", "escalation",
    }
    _MACRO_BULLISH = {
        "ceasefire", "peace", "deal", "agreement", "stimulus", "bailout",
        "rescue", "recovery", "breakthrough", "resolution", "truce",
    }

    # Cross-asset implications of macro events by category
    # -1.0 = strongly bearish, +1.0 = strongly bullish
    _MACRO_IMPACT: Dict[str, Dict[str, float]] = {
        "bearish": {
            "crypto":      -0.25,   # risk-off hurts crypto
            "indices":     -0.35,   # equities sell off
            "forex":       -0.10,   # currency volatility — mild
            "commodities": +0.30,   # safe havens (gold) and supply disruption (oil)
        },
        "bullish": {
            "crypto":      +0.15,
            "indices":     +0.25,
            "forex":       +0.05,
            "commodities": -0.10,   # risk-on reduces safe haven demand
        },
    }

    # Shared macro event cache — populated by any asset lookup, read by all
    _macro_event_cache: Dict[str, Tuple[float, float]] = {}  # category → (score, expiry)
    _macro_lock = threading.Lock()

    @classmethod
    def get(cls, asset: str) -> Optional[float]:
        with cls._lock:
            hit = cls._cache.get(asset)
            if hit and time.time() < hit[1]:
                return hit[0]

        score = cls._compute(asset)
        if score is not None:
            with cls._lock:
                cls._cache[asset] = (score, time.time() + cls._TTL)
        return score

    @classmethod
    def macro_impact(cls, asset: str) -> Optional[float]:
        """Return the current macro-event impact for this asset category."""
        cat = _cat(asset)
        with cls._macro_lock:
            entry = cls._macro_event_cache.get(cat)
            if entry and time.time() < entry[1]:
                return round(float(entry[0]), 3)
        return None

    @classmethod
    def _compute(cls, asset: str) -> Optional[float]:
        """
        Compute sentiment score for an asset.
        Combines traditional news sources with Reddit as a live news feed.
        Reddit posts are scored by the same financial keyword system — not TextBlob.
        High-engagement posts get more weight (engagement-weighted average).
        Macro events detected from Reddit are stored cross-asset and exposed
        separately at the category sentiment layer.
        """
        # ── Standard news sources ─────────────────────────────────────────
        std_articles = cls._fetch_articles(asset)
        std_scores   = [cls._score_headline(h, asset) for h in std_articles]
        std_scores   = [s for s in std_scores if s is not None]

        # Feed article text into narrative_ai.ingest() so TopicClusterEngine
        # can build keyword velocity for the narrative subsystem.
        try:
            from narrative_ai import ingest as _nar_ingest
            for _headline in std_articles:
                if _headline and len(_headline) > 10:
                    _nar_ingest(_headline, source="news")
        except Exception:
            pass  # narrative_ai is optional enhancement

        # ── Reddit as live news feed ──────────────────────────────────────
        reddit_weighted = cls._fetch_reddit_scored(asset)

        # ── Combine ───────────────────────────────────────────────────────
        all_scores: List[float] = []
        all_weights: List[float] = []

        # Standard news — equal weight 1.0 each
        for s in std_scores:
            all_scores.append(s)
            all_weights.append(1.0)

        # Reddit — weighted by engagement
        for score, weight in reddit_weighted:
            all_scores.append(score)
            all_weights.append(weight)

        if not all_scores:
            return None

        total_w  = sum(all_weights)
        base     = sum(s * w for s, w in zip(all_scores, all_weights)) / total_w
        return round(_clamp(base), 3)

    @classmethod
    def _fetch_reddit_scored(cls, asset: str) -> List[Tuple[float, float]]:
        """
        Pull Reddit posts for this asset, score each title through the
        financial keyword scorer, and return (score, engagement_weight) pairs.

        Also detects macro events and stores cross-asset signals.
        """
        results: List[Tuple[float, float]] = []
        try:
            from reddit_watcher import RedditWatcher
            rw      = RedditWatcher()
            data    = rw.get_asset_sentiment(asset)
            posts   = data.get("posts", [])
            if not posts:
                return results

            # Normalise engagement weights across this batch
            engagements = [
                max(1, p.get("score", 0) + p.get("comments", 0))
                for p in posts
            ]
            max_eng = max(engagements) if engagements else 1

            now = time.time()
            macro_signals: List[Tuple[str, float, float]] = []  # (direction, weight, velocity)

            for post, eng in zip(posts, engagements):
                title   = post.get("title", "")
                if not title:
                    continue

                # Score through financial keyword system — same as NewsAPI articles
                kw_score = cls._score_headline(title, asset)
                if kw_score is None:
                    continue

                # Normalised engagement weight (0.5 → 2.0 range)
                norm_weight = 0.5 + 1.5 * (eng / max_eng)

                # ── Velocity: upvote rate as breaking-news signal ─────────
                created = post.get("created")
                velocity_mult = 1.0
                if created:
                    try:
                        if hasattr(created, "timestamp"):
                            age_hours = (now - created.timestamp()) / 3600
                        elif isinstance(created, datetime):
                            age_hours = (now - created.timestamp()) / 3600
                        else:
                            age_hours = None

                        if age_hours is not None:
                            # Enforce freshness window (recent 6-12h)
                            if age_hours > cls._MAX_AGE_HOURS:
                                continue

                            if age_hours < 2.0 and eng > 100:
                                # Breaking news — posts less than 2h old with >100 engagement
                                velocity = eng / max(0.1, age_hours)
                                # Scale: 500 upvotes/hr = 1.5x multiplier
                                velocity_mult = min(2.0, 1.0 + velocity / 1000)
                    except Exception:
                        pass

                final_score  = _clamp(kw_score * velocity_mult)
                final_weight = norm_weight * velocity_mult
                results.append((final_score, final_weight))

                # ── Macro event detection ─────────────────────────────────
                words = set(title.lower().split())
                words = {w.strip(".,!?;:") for w in words}
                is_macro_bearish = bool(words & cls._MACRO_BEARISH)
                is_macro_bullish = bool(words & cls._MACRO_BULLISH)

                if is_macro_bearish and eng > 200:
                    macro_signals.append(("bearish", norm_weight, velocity_mult))
                elif is_macro_bullish and eng > 200:
                    macro_signals.append(("bullish", norm_weight, velocity_mult))

            # ── Store macro signals cross-asset ───────────────────────────
            if macro_signals:
                total_macro_w = sum(w * v for _, w, v in macro_signals)
                bearish_w = sum(w * v for d, w, v in macro_signals if d == "bearish")
                bullish_w = sum(w * v for d, w, v in macro_signals if d == "bullish")
                net = (bullish_w - bearish_w) / max(1, total_macro_w)

                # Write cross-asset implications into cache (30 min TTL)
                # Structure: _MACRO_IMPACT[direction][category] = impact_value
                expiry = now + 1800
                direction = "bearish" if net < 0 else "bullish"
                category_impacts = cls._MACRO_IMPACT.get(direction, {})
                with cls._macro_lock:
                    for cat_name, raw_impact in category_impacts.items():
                        # Weight impact by signal strength (net ranges -1 to +1)
                        impact = raw_impact * min(1.0, abs(net) * 2)
                        existing = cls._macro_event_cache.get(cat_name)
                        if not existing or time.time() >= existing[1]:
                            cls._macro_event_cache[cat_name] = (impact, expiry)
                            logger.info(
                                f"[NewsSentiment] Macro {direction} event → "
                                f"{cat_name} impact={impact:+.3f} (strength={abs(net):.2f})"
                            )

        except Exception as e:
            logger.debug(f"[NewsSentiment] Reddit scored fetch failed for {asset}: {e}")

        return results

    @classmethod
    def _is_recent_time(cls, dt: Optional[datetime]) -> bool:
        if not dt:
            return False
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        age_hours = (datetime.utcnow() - dt).total_seconds() / 3600.0
        return 0 <= age_hours <= cls._MAX_AGE_HOURS

    @classmethod
    def _parse_datetime(cls, value: Any) -> Optional[datetime]:
        if not value:
            return None
        if isinstance(value, datetime):
            return value
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(value, timezone.utc)

        try:
            # ISO 8601 with Z or timezone offset
            if isinstance(value, str):
                normalized = value.strip()
                if normalized.endswith("Z"):
                    normalized = normalized[:-1] + "+00:00"
                return datetime.fromisoformat(normalized)
        except Exception:
            pass

        try:
            # Fallback: epoch seconds string
            return datetime.fromtimestamp(float(value), timezone.utc)
        except Exception:
            return None

    @classmethod
    def _fetch_articles(cls, asset: str) -> List[str]:
        canonical_asset = _canon_asset(asset)
        keywords = _ASSET_KEYWORDS.get(canonical_asset, [canonical_asset.lower()])
        query    = " OR ".join(f'"{kw}"' for kw in keywords[:3])
        articles = []

        # NewsAPI — called if quota available
        if NEWSAPI_KEY and _QuotaManager.can_call("newsapi"):
            try:
                r = requests.get(
                    "https://newsapi.org/v2/everything",
                    params={"q": query, "language": "en", "pageSize": 20,
                            "sortBy": "publishedAt", "apiKey": NEWSAPI_KEY},
                    timeout=10
                )
                if r.status_code == 200:
                    _QuotaManager.record_call("newsapi")
                    for a in r.json().get("articles", []):
                        published = cls._parse_datetime(a.get("publishedAt"))
                        if not cls._is_recent_time(published):
                            continue
                        articles.append(a.get("title", "") + " " + (a.get("description") or ""))
                elif r.status_code == 429 or "quota" in r.text.lower():
                    _QuotaManager.disable_api("newsapi", "Daily quota exceeded")
            except Exception as e:
                if _is_quota_error(e):
                    _QuotaManager.disable_api("newsapi", str(e))
                elif not _is_quota_error(e):
                    logger.debug(f"[Sentiment] NewsAPI {asset}: {e}")

        # GNews — called if quota available (fallback to NewsAPI if disabled)
        if GNEWS_KEY and _QuotaManager.can_call("gnews"):
            try:
                kw = keywords[0]
                r  = requests.get(
                    "https://gnews.io/api/v4/search",
                    params={"q": kw, "lang": "en", "max": 10, "token": GNEWS_KEY},
                    timeout=10
                )
                if r.status_code == 200:
                    _QuotaManager.record_call("gnews")
                    for a in r.json().get("articles", []):
                        published = cls._parse_datetime(a.get("publishedAt"))
                        if not cls._is_recent_time(published):
                            continue
                        articles.append(a.get("title", "") + " " + (a.get("description") or ""))
                elif r.status_code == 429 or "quota" in r.text.lower():
                    _QuotaManager.disable_api("gnews", "Daily quota exceeded")
            except Exception as e:
                if _is_quota_error(e):
                    _QuotaManager.disable_api("gnews", str(e))
                elif not _is_quota_error(e):
                    logger.debug(f"[Sentiment] GNews {asset}: {e}")

        # Alpha Vantage news — called if quota available
        if ALPHA_VANTAGE_API_KEY and _QuotaManager.can_call("av"):
            try:
                tickers = {"XAU/USD": "GOLD", "WTI": "CRUDE", "XAG/USD": "SILVER",
                           "US500": "SPY", "US30": "DIA", "US100": "QQQ",
                           "UK100": "EWU", "BTC-USD": "COIN", "ETH-USD": "COIN"}.get(canonical_asset, "")
                if tickers:
                    r = requests.get(
                        "https://www.alphavantage.co/query",
                        params={"function": "NEWS_SENTIMENT", "tickers": tickers,
                                "limit": 10, "apikey": ALPHA_VANTAGE_API_KEY},
                        timeout=10
                    )
                    if r.status_code == 200:
                        _QuotaManager.record_call("av")
                        feed = r.json().get("feed", [])
                        for a in feed:
                            published = cls._parse_datetime(a.get("time") or a.get("publishedAt"))
                            if not cls._is_recent_time(published):
                                continue
                            articles.append(a.get("title", "") + " " + a.get("summary", ""))
                    elif r.status_code == 429 or "quota" in r.text.lower() or "limit" in r.text.lower():
                        _QuotaManager.disable_api("av", "Daily quota exceeded")
            except Exception as e:
                if _is_quota_error(e) or "limit" in str(e).lower():
                    _QuotaManager.disable_api("av", str(e))
                elif not _is_quota_error(e):
                    logger.debug(f"[Sentiment] AlphaVantage news {asset}: {e}")

        # Finnhub news — called if quota available
        if FINNHUB_API_KEY and _QuotaManager.can_call("finnhub"):
            try:
                import finnhub
                fh  = finnhub.Client(api_key=FINNHUB_API_KEY)
                cat = _cat(asset)
                if cat == "crypto":
                    news = fh.general_news("crypto", min_id=0)
                else:
                    news = fh.general_news("general", min_id=0)
                _QuotaManager.record_call("finnhub")
                kws = _ASSET_KEYWORDS.get(_canon_asset(asset), [])
                for n in news[:20]:
                    published = cls._parse_datetime(n.get("datetime") or n.get("publishedAt") or n.get("datetimeUTC"))
                    if not cls._is_recent_time(published):
                        continue
                    text = (n.get("headline", "") + " " + n.get("summary", "")).lower()
                    if any(kw in text for kw in kws):
                        articles.append(n.get("headline", ""))
            except Exception as e:
                if "limit" in str(e).lower() or _is_quota_error(e):
                    _QuotaManager.disable_api("finnhub", str(e))
                else:
                    logger.debug(f"[Sentiment] Finnhub news {asset}: {e}")

        # Filter to asset-specific articles
        canonical_asset = _canon_asset(asset)
        kws      = _ASSET_KEYWORDS.get(canonical_asset, [canonical_asset.lower()])
        filtered = [a for a in articles
                    if any(kw in a.lower() for kw in kws)]
        return filtered

    # ── Bearish phrases — score -2 each (stronger signal than single words) ──
    _BEARISH_PHRASES = [
        "interest rate hike", "rate hike", "rate hikes", "rates rise",
        "below expectations", "miss expectations", "worse than expected",
        "trade war", "bank run", "bank runs", "bank failure", "bank crisis",
        "credit crunch", "debt crisis", "supply chain", "recession fears",
        "inflation surge", "inflation spike", "tighter policy",
        "regulatory crackdown", "sec charges", "doj charges", "criminal charges",
        "emergency meeting", "market crash", "flash crash", "black swan",
        "exchange hack", "exchange collapsed", "exit scam",
        "mass layoffs", "job cuts", "earning miss",
    ]

    # ── Bullish phrases — score +2 each ───────────────────────────────────────
    _BULLISH_PHRASES = [
        "interest rate cut", "rate cut", "rate cuts", "rates fall",
        "beats expectations", "beat expectations", "better than expected",
        "above expectations", "record high", "all time high", "all-time high",
        "etf approval", "etf approved", "spot etf", "institutional buying",
        "trade deal", "peace deal", "ceasefire agreement",
        "earnings beat", "profit surge", "revenue growth",
        "dovish fed", "dovish pivot", "fed pivot", "quantitative easing",
        "strategic reserve", "national reserve", "bitcoin reserve",
        "mass adoption", "mainstream adoption", "major partnership",
    ]

    @classmethod
    def _score_headline(cls, text: str, asset: str) -> Optional[float]:
        """
        Score a headline -1 to +1 using financial keyword sets.
        Phrase matching scores first (stronger signal), then single word matching.
        Context-aware: commodity surges are not bullish for equities.
        """
        if not text:
            return None

        text_lower = text.lower()
        words      = text_lower.split()
        score      = 0.0
        matches    = 0
        cat        = _cat(asset)

        # ── Phase 1: phrase matching (weight 2x single words) ─────────────
        for phrase in cls._BEARISH_PHRASES:
            if phrase in text_lower:
                score   -= 2
                matches += 2

        for phrase in cls._BULLISH_PHRASES:
            if phrase in text_lower:
                score   += 2
                matches += 2

        # ── Pass 2: single word matching ──────────────────────────────────
        for word in words:
            w = word.strip(".,!?;:")
            if w in cls._BEARISH_WORDS:
                score   -= 1
                matches += 1
            elif w in cls._BULLISH_WORDS:
                # Commodity surge words are not bullish for equities/forex
                if w in {"surge", "surges", "rally", "rallies", "explode", "moon", "spike", "spiked"}:
                    if cat not in ("commodities", "crypto"):
                        continue
                score   += 1
                matches += 1

        if matches == 0:
            return None

        # Negation dampener — "not rally", "no growth", "never recovered"
        negation_words = {"not", "no", "never", "without", "despite", "fails", "fail"}
        if any(neg in words for neg in negation_words):
            score *= 0.6   # dampen score when negation present

        raw = score / matches   # -1 to +1
        return round(_clamp(raw * 0.8), 3)  # scale to ±0.8

    @classmethod
    def get_articles_for_dashboard(cls, limit: int = 20) -> List[Dict]:
        """Fetch recent general market articles for the dashboard news feed.
        Priority:
          1. NewsAPI 'everything' endpoint (free tier — top-headlines is paid)
          2. GNews search endpoint (free tier)
          3. RSS feeds via feedparser — no API key, always works
          4. Reddit public search — no credentials needed
        """
        articles_out = []

        # ── 1. NewsAPI — 'everything' works on free tier ─────────────────────
        if NEWSAPI_KEY:
            try:
                r = requests.get(
                    "https://newsapi.org/v2/everything",
                    params={
                        "q":        "forex OR crypto OR ""stock market"" OR trading OR bitcoin",
                        "language": "en",
                        "sortBy":   "publishedAt",
                        "pageSize": limit,
                        "apiKey":   NEWSAPI_KEY,
                    },
                    timeout=10
                )
                if r.status_code == 200:
                    for a in r.json().get("articles", []):
                        title = (a.get("title") or "").strip()
                        if not title or "[Removed]" in title:
                            continue
                        text  = title + " " + (a.get("description") or "")
                        score = cls._score_headline(text, "US500") or 0.0
                        articles_out.append({
                            "title":     title,
                            "source":    a.get("source", {}).get("name", ""),
                            "date":      (a.get("publishedAt") or "")[:10],
                            "url":       a.get("url", ""),
                            "sentiment": round(score, 2),
                        })
                    if articles_out:
                        return articles_out[:limit]
            except Exception as e:
                if not _is_quota_error(e):
                    logger.debug(f"[Sentiment] NewsAPI feed: {e}")

        # ── 2. GNews fallback ─────────────────────────────────────────────────
        if GNEWS_KEY:
            try:
                r = requests.get(
                    "https://gnews.io/api/v4/search",
                    params={"q": "finance trading market", "lang": "en",
                            "max": limit, "token": GNEWS_KEY},
                    timeout=10
                )
                if r.status_code == 200:
                    for a in r.json().get("articles", []):
                        title = (a.get("title") or "").strip()
                        if not title:
                            continue
                        text  = title + " " + (a.get("description") or "")
                        score = cls._score_headline(text, "US500") or 0.0
                        articles_out.append({
                            "title":     title,
                            "source":    a.get("source", {}).get("name", ""),
                            "date":      (a.get("publishedAt") or "")[:10],
                            "url":       a.get("url", ""),
                            "sentiment": round(score, 2),
                        })
                    if articles_out:
                        return articles_out[:limit]
            except Exception as e:
                if not _is_quota_error(e):
                    logger.debug(f"[Sentiment] GNews feed: {e}")

        # ── 3. RSS feeds — no API key needed, always available ────────────────
        _RSS = [
            ("Reuters Markets",    "https://feeds.reuters.com/reuters/businessNews"),
            ("CNBC Markets",       "https://www.cnbc.com/id/10001147/device/rss/rss.html"),
            ("CoinDesk",           "https://www.coindesk.com/arc/outboundfeeds/rss/"),
            ("Cointelegraph",      "https://cointelegraph.com/rss"),
            ("FX Street",          "https://www.fxstreet.com/rss"),
            ("Investing.com News", "https://www.investing.com/rss/news.rss"),
        ]
        try:
            import feedparser
            from datetime import datetime as _dt
            seen = set()
            for source_name, url in _RSS:
                if len(articles_out) >= limit:
                    break
                try:
                    feed = feedparser.parse(url)
                    for entry in feed.entries[:5]:
                        title = (entry.get("title") or "").strip()
                        if not title or title in seen:
                            continue
                        seen.add(title)
                        pub   = entry.get("published", "")
                        # Parse date
                        date_str = ""
                        try:
                            import email.utils
                            ts = email.utils.parsedate_to_datetime(pub)
                            date_str = ts.strftime("%Y-%m-%d")
                        except Exception:
                            date_str = pub[:10] if pub else ""
                        text  = title + " " + (entry.get("summary") or "")
                        score = cls._score_headline(text, "US500") or 0.0
                        articles_out.append({
                            "title":     title,
                            "source":    source_name,
                            "date":      date_str,
                            "url":       entry.get("link", ""),
                            "sentiment": round(score, 2),
                        })
                except Exception:
                    continue
            if articles_out:
                return sorted(articles_out, key=lambda x: x.get("date", ""), reverse=True)[:limit]
        except ImportError:
            logger.debug("[Sentiment] feedparser not installed — RSS fallback unavailable")
        except Exception as e:
            logger.debug(f"[Sentiment] RSS feed: {e}")

        # ── 4. Reddit public search — no credentials needed ───────────────────
        try:
            headers = {"User-Agent": "Mozilla/5.0 TradingBot/1.0"}
            r = requests.get(
                "https://www.reddit.com/r/investing+stocks+forex+CryptoCurrency/search.json",
                params={"q": "market", "sort": "new", "limit": limit, "t": "day"},
                headers=headers,
                timeout=10,
            )
            if r.status_code == 200:
                posts = r.json().get("data", {}).get("children", [])
                for p in posts:
                    d = p.get("data", {})
                    title = (d.get("title") or "").strip()
                    if not title:
                        continue
                    score = cls._score_headline(title, "US500") or 0.0
                    import datetime
                    ts = d.get("created_utc", 0)
                    date_str = datetime.datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d") if ts else ""
                    articles_out.append({
                        "title":     title,
                        "source":    f"r/{d.get('subreddit', 'investing')}",
                        "date":      date_str,
                        "url":       f"https://reddit.com{d.get('permalink', '')}",
                        "sentiment": round(score, 2),
                    })
        except Exception as e:
            logger.debug(f"[Sentiment] Reddit public: {e}")

        return articles_out[:limit]


class _CryptoSignals:
    """Crypto-specific signals — Fear & Greed, on-chain, Reddit."""

    _cache: Dict[str, Tuple[Any, float]] = {}
    _lock  = threading.Lock()
    _TTL   = 600

    @classmethod
    def _cached(cls, key: str, fn):
        with cls._lock:
            hit = cls._cache.get(key)
            if hit and time.time() < hit[1]:
                return hit[0]
        result = fn()
        if result is not None:
            with cls._lock:
                cls._cache[key] = (result, time.time() + cls._TTL)
        return result

    @classmethod
    def onchain(cls) -> Optional[float]:
        """Exchange net flow from Glassnode public endpoint."""
        def _fetch():
            try:
                r = requests.get(
                    "https://api.glassnode.com/v1/metrics/transactions/count",
                    params={"a": "BTC", "i": "24h"},
                    timeout=8
                )
                if r.status_code == 200:
                    data = r.json()
                    if data and len(data) >= 2:
                        val1, val2 = float(data[-1]["v"]), float(data[-2]["v"])
                        chg = (val1 - val2) / max(1, val2)
                        return _clamp(chg * 2)
            except Exception:
                pass
            return None
        return cls._cached("onchain", _fetch)

    @classmethod
    def reddit(cls, asset: str) -> Optional[float]:
        """Reddit sentiment from public pushshift/reddit search."""
        try:
            canonical_asset = _canon_asset(asset)
            kws = _ASSET_KEYWORDS.get(canonical_asset, [canonical_asset.lower().replace("-usd", "")])
            r   = requests.get(
                "https://www.reddit.com/r/investing+CryptoCurrency+stocks/search.json",
                params={"q": kws[0], "sort": "new", "limit": 25, "t": "day"},
                headers={"User-Agent": "Robbie-TradingBot/1.0"},
                timeout=8
            )
            if r.status_code == 200:
                posts  = r.json().get("data", {}).get("children", [])
                scores = []
                for p in posts:
                    title = p.get("data", {}).get("title", "")
                    s     = _NewsSentiment._score_headline(title, asset)
                    if s is not None:
                        scores.append(s)
                if scores:
                    return round(_clamp(sum(scores) / len(scores)), 3)
        except Exception as e:
            logger.debug(f"[Sentiment] Reddit {asset}: {e}")
        return None


# ── Reddit sentiment helper ───────────────────────────────────────────────────

def _reddit_score(asset: str) -> Optional[float]:
    """
    Fetch Reddit sentiment for any asset using the new public-JSON RedditWatcher.
    Falls back to _CryptoSignals.reddit() for crypto if RedditWatcher returns nothing.
    Cached inside RedditWatcher's own 5-minute cache so repeated calls are free.
    """
    try:
        from reddit_watcher import RedditWatcher
        rw     = RedditWatcher()
        result = rw.get_asset_sentiment(asset)
        if result and result.get("total_mentions", 0) > 0:
            score = result.get("score")
            if score is not None:
                return float(_clamp(score))
    except Exception:
        pass
    # Crypto-only fallback
    try:
        return _CryptoSignals.reddit(asset)
    except Exception:
        pass
    return None


__all__ = [
    "_CryptoSignals",
    "_MarketInstruments",
    "_NewsSentiment",
    "_PriceMomentum",
    "_cat",
    "_clamp",
    "_reddit_score",
]
