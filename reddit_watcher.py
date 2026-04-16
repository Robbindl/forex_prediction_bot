from __future__ import annotations

import os
import requests
import re
import time
import threading
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set, Any, Tuple
# TextBlob removed — replaced with financial keyword scorer (see _score_headline)
from services.intelligence_event_utils import record_whale_alert_event, score_whale_text
from utils.logger import logger


# ── GLOBAL RATE LIMITER (shared across ALL instances) ─────────────────────────
_REDDIT_MAX_CONCURRENT_REQUESTS = max(1, int(os.getenv("REDDIT_MAX_CONCURRENT_REQUESTS", "1")))
_REDDIT_MIN_REQUEST_INTERVAL_SECONDS = float(os.getenv("REDDIT_MIN_REQUEST_INTERVAL_SECONDS", "12"))
_REDDIT_GLOBAL_429_BACKOFF_SECONDS = float(os.getenv("REDDIT_GLOBAL_429_BACKOFF_SECONDS", "300"))
_REDDIT_SUBREDDIT_429_BACKOFF_SECONDS = float(os.getenv("REDDIT_SUBREDDIT_429_BACKOFF_SECONDS", "900"))
_REDDIT_MAX_BACKOFF_SECONDS = float(os.getenv("REDDIT_MAX_BACKOFF_SECONDS", "3600"))
_REDDIT_USER_AGENT = os.getenv(
    "REDDIT_USER_AGENT",
    "forex_prediction_bot/1.0 (Windows; research bot; contact local)",
).strip() or "forex_prediction_bot/1.0"

_global_request_semaphore = threading.Semaphore(_REDDIT_MAX_CONCURRENT_REQUESTS)
_last_request_time = 0.0
_request_lock = threading.Lock()
_shared_cache: Dict[str, Tuple[Any, float]] = {}        # Shared cache across instances
_cache_lock = threading.Lock()


_rate_limit_until: float = 0.0  # global 429 backoff — block ALL requests until this time
_subreddit_backoff_until: Dict[str, float] = {}
_subreddit_backoff_notified: Set[str] = set()
_global_backoff_notified_until: float = 0.0

_SUBREDDIT_NETWORK_BACKOFF_SECS = 180.0
_SUBREDDIT_FORBIDDEN_BACKOFF_SECS = 600.0

def _rate_limited_request(url: str, headers: Dict, timeout: int = 10) -> requests.Response:
    """
    Global rate limiter for ALL RedditWatcher instances.
    Ensures:
    - No more than 3 concurrent requests
    - Minimum 5 seconds between requests
    - Global 429 backoff — if any request hits 429, ALL requests pause for 60s
    
    FIX M-11: Sleep happens OUTSIDE _request_lock to avoid blocking concurrent sentiment analysis
    """
    with _global_request_semaphore:
        # Acquire lock only to check/update timestamp, not during sleep
        sleep_time = 0
        with _request_lock:
            global _last_request_time
            elapsed = time.time() - _last_request_time
            if elapsed < _REDDIT_MIN_REQUEST_INTERVAL_SECONDS:
                sleep_time = _REDDIT_MIN_REQUEST_INTERVAL_SECONDS - elapsed
            _last_request_time = time.time()
        
        # Sleep OUTSIDE the lock to allow concurrent operations
        if sleep_time > 0:
            logger.debug(f"[RedditWatcher] Rate limit: sleeping {sleep_time:.2f}s")
            time.sleep(sleep_time)
        
        return requests.get(url, headers=headers, timeout=timeout)


def _get_cached_posts(cache_key: str, *, allow_stale: bool = False, ttl_seconds: float = 0.0) -> Optional[List[Dict]]:
    with _cache_lock:
        cached = _shared_cache.get(cache_key)
    if not cached:
        return None
    posts, cached_at = cached
    if allow_stale or (time.time() - cached_at) < ttl_seconds:
        return posts
    return None


def _retry_after_seconds(response: requests.Response) -> float:
    raw = response.headers.get("Retry-After")
    if not raw:
        return 0.0
    try:
        return max(0.0, float(raw))
    except Exception:
        return 0.0


def _apply_global_backoff(seconds: float, reason: str = "rate_limited") -> None:
    global _rate_limit_until, _global_backoff_notified_until
    seconds = max(_REDDIT_GLOBAL_429_BACKOFF_SECONDS, min(float(seconds), _REDDIT_MAX_BACKOFF_SECONDS))
    until = time.time() + seconds
    if until > _rate_limit_until:
        _rate_limit_until = until
    if _rate_limit_until > _global_backoff_notified_until:
        logger.warning(f"[RedditWatcher] {reason} — global backoff {int(_rate_limit_until - time.time())}s")
        _global_backoff_notified_until = _rate_limit_until


def _apply_subreddit_backoff(subreddit: str, seconds: float) -> None:
    seconds = min(max(float(seconds), _REDDIT_SUBREDDIT_429_BACKOFF_SECONDS), _REDDIT_MAX_BACKOFF_SECONDS)
    _subreddit_backoff_until[subreddit] = max(_subreddit_backoff_until.get(subreddit, 0.0), time.time() + seconds)
    _subreddit_backoff_notified.discard(subreddit)


class RedditWatcher:
    """
    Fetches sentiment for ANY asset from Reddit's public JSON endpoints.
    Also detects whale alerts for crypto assets.
    No authentication needed.
    
    SINGLETON: Only one instance exists across the entire application.
    """
    
    # Singleton instance
    _instance: Optional["RedditWatcher"] = None
    _singleton_lock = threading.Lock()
    
    # Asset-to-subreddit mapping
    ASSET_SUBREDDITS: Dict[str, List[str]] = {
        # Crypto assets
        "BTC-USD": ["Bitcoin", "CryptoCurrency", "CryptoMarkets"],
        "ETH-USD": ["ethereum", "CryptoCurrency", "CryptoMarkets"],
        "SOL-USD": ["solana", "CryptoCurrency", "CryptoMarkets"],
        "BNB-USD": ["CryptoCurrency", "CryptoMarkets"],
        "XRP-USD": ["Ripple", "CryptoCurrency", "CryptoMarkets"],
        
        # Forex
        "EUR/USD": ["Forex", "Forexstrategy", "trading"],
        "EUR/JPY": ["Forex", "Forexstrategy", "trading"],
        "EUR/GBP": ["Forex", "Forexstrategy", "trading"],
        "GBP/USD": ["Forex", "Forexstrategy", "trading"],
        "USD/JPY": ["Forex", "Forexstrategy", "trading"],
        "AUD/USD": ["Forex", "Forexstrategy", "trading"],
        "NZD/USD": ["Forex", "Forexstrategy", "trading"],
        "USD/CAD": ["Forex", "Forexstrategy", "trading"],
        "USD/CHF": ["Forex", "Forexstrategy", "trading"],
        "GBP/JPY": ["Forex", "Forexstrategy", "trading"],
        
        # Indices
        "US30": ["stocks", "investing", "wallstreetbets", "trading"],
        "US100": ["stocks", "investing", "wallstreetbets", "trading"],
        "US500": ["stocks", "investing", "wallstreetbets", "trading"],
        "UK100": ["stocks", "investing", "UKPersonalFinance", "trading"],
        "GER40": ["stocks", "investing", "trading"],
        "AUS200": ["stocks", "investing", "trading"],
        "JPN225": ["stocks", "investing", "trading"],
        
        # Commodities
        "XAU/USD": ["Gold", "Silverbugs", "investing", "commodities"],
        "XAG/USD": ["Silverbugs", "investing", "commodities"],
    }
    
    # Asset search terms (common names)
    ASSET_TERMS: Dict[str, List[str]] = {
        "XAU/USD": ["gold", "xau", "gold price"],
        "XAG/USD": ["silver", "xag", "silver price"],
        "EUR/USD": ["eur", "euro", "eurusd", "euro dollar"],
        "EUR/JPY": ["eur/jpy", "eurjpy", "euro yen", "euro jpy"],
        "EUR/GBP": ["eur/gbp", "eurgbp", "euro sterling", "euro pound"],
        "GBP/USD": ["gbp", "pound", "cable", "gbpusd"],
        "USD/JPY": ["usd/jpy", "yen", "usdjpy", "dollar yen"],
        "AUD/USD": ["aud", "aussie", "audusd"],
        "NZD/USD": ["nzd", "nzdusd", "kiwi", "new zealand dollar"],
        "USD/CAD": ["cad", "loonie", "usdcad"],
        "USD/CHF": ["usd/chf", "usdchf", "swiss franc", "swissy"],
        "GBP/JPY": ["gbp/jpy", "gpbjpy"],
        "US30": ["dow", "dow jones", "us30", "dji"],
        "US100": ["nasdaq", "us100", "nas100", "ixic"],
        "US500": ["sp500", "s&p", "spx", "sp 500"],
        "UK100": ["ftse", "ftse100", "uk100"],
        "GER40": ["dax", "germany 40", "ger40", "de40", "german stocks"],
        "AUS200": ["asx", "asx200", "australia 200", "aus200", "spi 200"],
        "JPN225": ["nikkei", "nikkei225", "japan 225", "jpn225", "jp225"],
    }
    
    # Whale-specific subreddits and keywords (reduced for rate limiting)
    WHALE_SUBREDDITS: List[str] = ["CryptoCurrency", "CryptoMarkets", "whalealert"]
    WHALE_KEYWORDS: List[str] = [
        "whale", "million", "billion", "large transfer", "moved", "alert",
        "accumulation", "distribution", "withdrawal", "deposit", "transaction"
    ]
    
    # Live crypto price cache — fetched from Deriv, no hardcoded fallbacks.
    # If price unavailable the whale alert is skipped rather than using a
    # stale number that could be thousands of dollars wrong.
    _price_cache: Dict[str, float] = {}
    _price_cache_ts: Dict[str, float] = {}
    _price_ttl: float = 300.0   # 5 min per symbol

    _PRICE_ASSETS: Dict[str, str] = {
        "BTC": "BTC-USD", "ETH": "ETH-USD", "BNB": "BNB-USD",
        "SOL": "SOL-USD", "XRP": "XRP-USD", "ADA": "ADA-USD",
        "DOGE": "DOGE-USD", "LINK": "LINK-USD", "DOT": "DOT-USD",
        "MATIC": "MATIC-USD",
    }

    def get_crypto_price(self, symbol: str) -> Optional[float]:
        """
        Return current USD price for a crypto symbol.
        Cached 5 minutes. Returns None if unavailable — callers must
        handle None and skip rather than use a stale fallback.
        """
        now = time.time()
        sym = symbol.upper()
        if sym in self._price_cache:
            if now - self._price_cache_ts.get(sym, 0) < self._price_ttl:
                return self._price_cache[sym]
        canonical_asset = self._PRICE_ASSETS.get(sym)
        if not canonical_asset:
            return None
        try:
            from data.fetcher import get_shared_fetcher

            fetcher = get_shared_fetcher()
            price, _ = fetcher.get_real_time_price(canonical_asset, "crypto")
            if price and price > 0:
                self._price_cache[sym] = float(price)
                self._price_cache_ts[sym] = now
                logger.debug(f"[RedditWatcher] {sym} price: ${float(price):,.2f}")
                return float(price)
        except Exception as e:
            logger.debug(f"[RedditWatcher] Price fetch failed for {sym}: {e}")
        return None

    def __new__(cls) -> "RedditWatcher":
        """Singleton pattern: only one instance ever created."""
        if cls._instance is not None:
            return cls._instance
        with cls._singleton_lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
            return cls._instance
    
    def __init__(self):
        """Initialize Reddit watcher with rate limiting and caching."""
        # Prevent re-initialization
        if hasattr(self, '_initialized'):
            return
        self._initialized = True
        
        self.enabled = True
        self.recent_posts: List[Dict] = []
        self.recent_whale_alerts: List[Dict] = []
        self.max_posts = 100
        self.max_alerts = 50
        self.is_running = False
        
        # Rate limiting (increased delay)
        self.request_delay = 8.0  # Enough gap to prevent rate limiting
        
        # Cache TTL (15 minutes — reduces Reddit request frequency)
        self._cache_ttl = 900
        
        # All subreddits to monitor
        self.subreddits: List[str] = []
        for subs in self.ASSET_SUBREDDITS.values():
            self.subreddits.extend(subs)
        self.subreddits = sorted(set(self.subreddits))
        
        logger.info(
            f"[RedditWatcher] Monitoring {len(self.subreddits)} subreddits "
            f"for {len(self.ASSET_SUBREDDITS)} assets (SINGLETON MODE)"
        )
        logger.info(
            f"[RedditWatcher] Whale alert monitoring on {len(self.WHALE_SUBREDDITS)} subreddits"
        )

    @classmethod
    def _default_subreddits_for_asset(cls, asset: str) -> List[str]:
        if "/" in asset:
            return ["Forex", "Forexstrategy", "trading"]
        if asset.endswith("-USD"):
            return ["CryptoCurrency", "CryptoMarkets", "trading"]
        if asset.startswith("US") or asset.startswith("UK"):
            return ["investing", "wallstreetbets", "trading"]
        return ["trading", "investing"]
    
    def _fetch_subreddit(
        self, 
        subreddit: str, 
        sort: str = "hot", 
        limit: int = 20  # Standardised to 20 for cache key consistency
    ) -> Optional[List[Dict]]:
        """
        Fetch posts from a subreddit with SHARED caching.
        
        Args:
            subreddit: Subreddit name
            sort: Sort order (hot, new, top)
            limit: Number of posts to fetch (reduced to 25)
        
        Returns:
            List of post data dicts or None if failed
        """
        cache_key = f"{subreddit}_{sort}_{limit}"
        now = time.time()

        # Check SHARED cache (global, across all instances)
        cached = _get_cached_posts(cache_key, ttl_seconds=self._cache_ttl)
        if cached is not None:
            logger.debug(f"[RedditWatcher] Cache hit for r/{subreddit}")
            return cached

        backoff_until = _subreddit_backoff_until.get(subreddit, 0.0)
        if now < backoff_until:
            if subreddit not in _subreddit_backoff_notified:
                logger.warning(
                    f"[RedditWatcher] Backoff active for r/{subreddit} — "
                    f"skipping requests for {int(backoff_until - now)}s"
                )
                _subreddit_backoff_notified.add(subreddit)
            return _get_cached_posts(cache_key, allow_stale=True)

        global_wait = _rate_limit_until - now
        if global_wait > 0:
            if subreddit not in _subreddit_backoff_notified:
                logger.debug(
                    f"[RedditWatcher] Global backoff active — using cache for r/{subreddit} "
                    f"for {int(global_wait)}s"
                )
                _subreddit_backoff_notified.add(subreddit)
            return _get_cached_posts(cache_key, allow_stale=True)
        
        # Fetch from Reddit with global rate limiting
        url = f"https://www.reddit.com/r/{subreddit}/{sort}.json?limit={limit}&raw_json=1"
        
        try:
            headers = {
                "User-Agent": _REDDIT_USER_AGENT,
                "Accept": "application/json",
            }
            response = _rate_limited_request(url, headers, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                posts = data.get('data', {}).get('children', [])
                result = [post.get('data', {}) for post in posts]
                
                # Store in SHARED cache
                with _cache_lock:
                    _shared_cache[cache_key] = (result, now)
                _subreddit_backoff_until.pop(subreddit, None)
                _subreddit_backoff_notified.discard(subreddit)
                logger.debug(f"[RedditWatcher] Fetched {len(result)} posts from r/{subreddit}")
                return result
            elif response.status_code == 429:
                retry_after = _retry_after_seconds(response)
                _apply_subreddit_backoff(subreddit, max(retry_after, _REDDIT_SUBREDDIT_429_BACKOFF_SECONDS))
                _apply_global_backoff(max(retry_after, _REDDIT_GLOBAL_429_BACKOFF_SECONDS), f"HTTP 429 for r/{subreddit}")
                return _get_cached_posts(cache_key, allow_stale=True)
            else:
                if response.status_code in (403, 404, 451, 500, 502, 503, 504):
                    _apply_subreddit_backoff(subreddit, _SUBREDDIT_NETWORK_BACKOFF_SECS)
                    _subreddit_backoff_notified.discard(subreddit)
                logger.warning(
                    f"[RedditWatcher] HTTP {response.status_code} for r/{subreddit}"
                )
                return _get_cached_posts(cache_key, allow_stale=True)
                
        except requests.RequestException as e:
            msg = str(e).lower()
            backoff_secs = (
                _SUBREDDIT_FORBIDDEN_BACKOFF_SECS
                if "10013" in msg or "forbidden by its access permissions" in msg
                else _SUBREDDIT_NETWORK_BACKOFF_SECS
            )
            _apply_subreddit_backoff(subreddit, backoff_secs)
            _subreddit_backoff_notified.discard(subreddit)
            logger.warning(
                f"[RedditWatcher] Network error fetching r/{subreddit}: {e} "
                f"— backing off {int(backoff_secs)}s"
            )
            return _get_cached_posts(cache_key, allow_stale=True)
        except Exception as e:
            _apply_subreddit_backoff(subreddit, _SUBREDDIT_NETWORK_BACKOFF_SECS)
            _subreddit_backoff_notified.discard(subreddit)
            logger.warning(
                f"[RedditWatcher] Error fetching r/{subreddit}: {e} "
                f"— backing off {int(_SUBREDDIT_NETWORK_BACKOFF_SECS)}s"
            )
            return _get_cached_posts(cache_key, allow_stale=True)
    
    # ── Financial keyword sets (self-contained, no external deps) ────────────
    _BEARISH = {
        "crash", "crashes", "collapse", "collapses", "plunge", "plunges",
        "tumble", "drop", "drops", "fall", "falls", "sink", "sinks",
        "slump", "decline", "declines", "dip", "dump", "dumps", "selloff",
        "sell-off", "correction", "tank", "tanks", "tanking", "bleeding",
        "loss", "losses", "fear", "fears", "panic", "crisis", "crises",
        "recession", "depression", "downturn", "downgrade", "concern",
        "warning", "warns", "worried", "threat", "threatens", "risk",
        "risks", "uncertainty", "uncertain", "volatile", "volatility",
        "war", "wars", "conflict", "tension", "tensions", "strike",
        "attack", "invasion", "sanctions", "tariff", "tariffs", "ban",
        "catastrophe", "disaster", "emergency", "miss", "misses",
        "disappoint", "disappointing", "weak", "weakness", "below",
        "shortfall", "deficit", "cut", "cuts", "layoff", "layoffs",
        "bankrupt", "bankruptcy", "default", "shutdown", "halt",
        "fraud", "scam", "hack", "hacked", "exploit", "stolen",
        "investigation", "lawsuit", "arrested", "rug", "rugpull",
        "depegged", "liquidated", "liquidation", "death", "dying",
        "dead", "worthless", "ponzi", "bubble", "inflation", "hawkish",
        "contagion", "tightening", "overtightening", "wiped", "erased", "rip", "capitulate", "capitulates", "capitulation", "plummeted", "imploded", "evaporated", "worthless",
    }
    _BULLISH = {
        "rally", "rallies", "surge", "surges", "rise", "rises", "rising",
        "gain", "gains", "soar", "soars", "jump", "jumps", "climb", "climbs",
        "recover", "recovery", "rebound", "rebounds", "bounce", "breakout",
        "breakthrough", "strong", "strength", "beat", "beats", "exceed",
        "exceeds", "outperform", "positive", "optimism", "optimistic",
        "confident", "confidence", "bullish", "growth", "grows", "boom",
        "upgrade", "lifted", "boost", "boosted", "stimulus", "demand",
        "adoption", "milestone", "record", "high", "profit", "profits",
        "earnings", "revenue", "inflow", "inflows", "approval", "approved",
        "etf", "institutional", "halving", "accumulation", "accumulating",
        "partnership", "deal", "agreement", "ceasefire", "truce", "peace",
        "launched", "launch", "mainnet", "integration", "listed",
        "dovish", "easing", "rate-cut",
    }
    _BEARISH_PHRASES = [
        "interest rate hike", "rate hike", "below expectations",
        "trade war", "bank run", "bank failure", "debt crisis",
        "regulatory crackdown", "emergency meeting", "market crash",
        "exchange hack", "mass layoffs", "earning miss",
        "sells for less", "sold for less", "sells for just",
        "down from", "fell from", "dropped from",
    ]
    _BULLISH_PHRASES = [
        "interest rate cut", "rate cut", "cut rates", "cuts rates",
        "beats expectations", "record high", "all time high", "all-time high",
        "etf approval", "etf approved", "institutional buying",
        "trade deal", "peace deal", "earnings beat",
        "dovish pivot", "fed pivot", "strategic reserve",
        "national reserve", "mass adoption", "must cut",
    ]

    def analyze_sentiment(self, text: str) -> float:
        """
        Score a headline using financial keyword matching.
        Phrase matching takes priority (2x weight).
        Negation detection dampens score when present.
        Returns -1.0 (bearish) to +1.0 (bullish). Returns 0.0 if no signal.
        """
        if not text:
            return 0.0
        try:
            text_lower = text.lower()
            words      = text_lower.split()
            score      = 0.0
            matches    = 0

            # Phase 1: phrase matching (weight 2)
            for phrase in self._BEARISH_PHRASES:
                if phrase in text_lower:
                    score   -= 2
                    matches += 2
            for phrase in self._BULLISH_PHRASES:
                if phrase in text_lower:
                    score   += 2
                    matches += 2

            # Phase 2: single word matching (weight 1)
            for word in words:
                w = word.strip(".,!?;:")
                if w in self._BEARISH:
                    score   -= 1
                    matches += 1
                elif w in self._BULLISH:
                    score   += 1
                    matches += 1

            if matches == 0:
                return 0.0

            # Negation dampener
            negation = {"not", "no", "never", "without", "despite", "fails", "fail"}
            if any(n in words for n in negation):
                score *= 0.6

            raw = score / matches
            return round(max(-1.0, min(1.0, raw * 0.8)), 4)
        except Exception as e:
            logger.debug(f"[RedditWatcher] Sentiment error: {e}")
            return 0.0
    
    def extract_whale_info(self, text: str) -> Optional[Dict]:
        """Extract whale transaction information from post title/text."""
        if not text:
            return None
        # prices fetched lazily via _get_price() below
        
        clean_text = text.replace(',', '').replace('$', '').lower()
        
        patterns = [
            r'(\d+(?:\.\d+)?)\s*(btc|eth|bnb|sol|xrp|ada|doge|link|dot|matic)\s*\(?\$?(\d+(?:\.\d+)?)\s*(m|million|b|billion)\)?',
            r'(\d+(?:\.\d+)?)\s*(btc|eth|bnb|sol|xrp)\s*(?:worth|valued at)\s*\$?(\d+(?:\.\d+)?)\s*(m|million|b|billion)',
            r'\$?(\d+(?:\.\d+)?)\s*(m|million|b|billion)\s*(?:worth\s*of)?\s*(btc|eth|bnb|sol|xrp)',
            r'(\d+(?:\.\d+)?)\s*(btc|eth|bnb|sol|xrp)\s*(?:moved|transferred|sent|received|deposited|withdrew)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, clean_text, re.IGNORECASE)
            if match:
                try:
                    groups = match.groups()
                    
                    if len(groups) >= 3 and groups[1] in ['m', 'million', 'b', 'billion']:
                        value_num = float(groups[0])
                        unit = groups[1]
                        symbol = groups[2].upper() if len(groups) > 2 else None
                        
                        if unit in ['m', 'million']:
                            value_usd = value_num * 1_000_000
                        else:
                            value_usd = value_num * 1_000_000_000
                        
                        if symbol and value_usd >= 1_000_000:
                            price = self.get_crypto_price(symbol)
                            amount = round(value_usd / price, 2) if price else 0.0
                            return {
                                'amount': amount,
                                'symbol': symbol,
                                'value_usd': round(value_usd)
                            }
                    
                    elif len(groups) >= 2:
                        amount = float(groups[0])
                        symbol = groups[1].upper()
                        price = self.get_crypto_price(symbol)
                        if price is None:
                            continue   # no live price — skip rather than guess
                        value_usd = amount * price
                        
                        if value_usd >= 1_000_000:
                            return {
                                'amount': round(amount, 2),
                                'symbol': symbol,
                                'value_usd': round(value_usd)
                            }
                            
                except (ValueError, IndexError) as e:
                    logger.debug(f"[RedditWatcher] Whale parse error: {e}")
                    continue
        
        return None
    
    def get_whale_alerts(
        self, 
        min_value_usd: float = 1_000_000, 
        limit: int = 50
    ) -> List[Dict]:
        """
        Fetch whale alerts from Reddit.
        Now only checks NEW posts (not both hot and new) to reduce requests.
        """
        alerts = []
        
        for subreddit in self.WHALE_SUBREDDITS[:3]:  # Reduced from 5 to 3
            # Only check NEW posts (hot is less likely to have whale alerts)
            posts = self._fetch_subreddit(subreddit, "new", 20)  # Reduced from 25 to 20
            if not posts:
                continue
            
            for post in posts:
                title = post.get('title', '')
                
                title_lower = title.lower()
                if not any(k in title_lower for k in self.WHALE_KEYWORDS):
                    continue
                
                whale_info = self.extract_whale_info(title)
                if whale_info and whale_info['value_usd'] >= min_value_usd:
                    url = f"https://reddit.com{post.get('permalink', '')}"
                    sentiment = score_whale_text(title)
                    alerts.append({
                        'title': title,
                        'value_usd': whale_info['value_usd'],
                        'symbol': whale_info['symbol'],
                        'amount': whale_info['amount'],
                        'url': url,
                        'score': post.get('score', 0),
                        'comments': post.get('num_comments', 0),
                        'created': datetime.fromtimestamp(post.get('created_utc', 0)),
                        'subreddit': subreddit,
                        'source': f"Reddit r/{subreddit}",
                        'type': 'whale_alert',
                        'raw_text': title,
                        'sentiment': sentiment,
                        'external_id': f"reddit:{url}",
                    })
        
        # Remove duplicates by URL and sort by value
        seen_urls = set()
        unique_alerts = []
        for alert in sorted(alerts, key=lambda x: x['value_usd'], reverse=True):
            if alert['url'] not in seen_urls:
                seen_urls.add(alert['url'])
                unique_alerts.append(alert)
        
        self.recent_whale_alerts = unique_alerts[:self.max_alerts]

        for alert in self.recent_whale_alerts:
            record_whale_alert_event(
                symbol=alert.get('symbol', ''),
                source=alert.get('source', 'Reddit'),
                value_usd=float(alert.get('value_usd', 0.0) or 0.0),
                raw_text=alert.get('raw_text', alert.get('title', '')),
                sentiment=float(alert.get('sentiment', 0.1) or 0.1),
                timestamp=alert.get('created'),
                metadata={
                    'title': alert.get('title', ''),
                    'url': alert.get('url', ''),
                    'subreddit': alert.get('subreddit', ''),
                },
                external_id=str(alert.get('external_id', '')),
            )

        if unique_alerts:
            logger.info(f"[RedditWatcher] Found {len(unique_alerts)} whale alerts")
        
        return unique_alerts[:limit]
    
    def get_whale_alerts_by_asset(
        self, 
        asset_symbol: str, 
        min_value_usd: float = 1_000_000
    ) -> List[Dict]:
        """Get whale alerts filtered by asset symbol."""
        alerts = self.get_whale_alerts(min_value_usd=min_value_usd)
        return [a for a in alerts if a['symbol'].upper() == asset_symbol.upper()]
    
    def get_whale_summary(self) -> Dict:
        """Get summary of whale activity."""
        alerts = self.get_whale_alerts()
        
        if not alerts:
            return {
                'total_alerts': 0,
                'total_value_usd': 0,
                'by_asset': {},
                'largest_alerts': [],
                'timestamp': datetime.utcnow().isoformat(),
            }
        
        by_asset = {}
        for alert in alerts:
            symbol = alert['symbol']
            if symbol not in by_asset:
                by_asset[symbol] = {'count': 0, 'total_value_usd': 0}
            by_asset[symbol]['count'] += 1
            by_asset[symbol]['total_value_usd'] += alert['value_usd']
        
        return {
            'total_alerts': len(alerts),
            'total_value_usd': sum(a['value_usd'] for a in alerts),
            'by_asset': by_asset,
            'largest_alerts': alerts[:10],
            'timestamp': datetime.utcnow().isoformat(),
        }
    
    def get_asset_sentiment(self, asset: str, limit: int = 20) -> Dict:
        """
        Get sentiment for a specific asset.
        Reduced subreddit count per asset to 2 (was 3).
        """
        subreddits = self.ASSET_SUBREDDITS.get(asset, self._default_subreddits_for_asset(asset))
        search_terms = self.ASSET_TERMS.get(
            asset, 
            [asset.replace("-USD", "").replace("=F", "").lower()]
        )
        
        symbol = asset.split("-")[0].split("/")[0].replace("^", "").lower()
        if symbol not in search_terms:
            search_terms.append(symbol)
        
        all_posts = []
        sentiments = []
        
        # 1 subreddit per asset — prevents burst traffic causing 429
        for subreddit in subreddits[:1]:
            posts = self._fetch_subreddit(subreddit, "hot", min(limit, 20))
            if not posts:
                continue
            
            for post in posts:
                title = post.get('title', '').lower()
                selftext = post.get('selftext', '').lower()
                combined = title + " " + selftext
                
                is_relevant = any(term in combined for term in search_terms)
                
                if is_relevant:
                    created_ts = post.get('created_utc', 0)
                    created_dt = datetime.fromtimestamp(created_ts, timezone.utc)
                    # Limit to recent posts (last 6-12h window) to avoid stale sentiment drift
                    max_age_hours = int(os.getenv('SENTIMENT_MAX_AGE_HOURS', '12'))
                    age_hours = (datetime.utcnow().replace(tzinfo=timezone.utc) - created_dt).total_seconds() / 3600
                    if age_hours > max_age_hours:
                        continue

                    try:
                        from narrative_ai import ingest as narrative_ingest
                        narrative_ingest(post.get('title', ''), source="reddit")
                    except Exception:
                        pass
                    sent = self.analyze_sentiment(post.get('title', ''))
                    all_posts.append({
                        "title": post.get('title', ''),
                        "subreddit": subreddit,
                        "sentiment": sent,
                        "score": post.get('score', 0),
                        "comments": post.get('num_comments', 0),
                        "url": f"https://reddit.com{post.get('permalink', '')}",
                        "created": created_dt,
                    })
                    sentiments.append(sent)
        
        if all_posts:
            total_weight = sum(p["score"] + p["comments"] + 1 for p in all_posts)
            weighted_sent = sum(
                p["sentiment"] * (p["score"] + p["comments"] + 1)
                for p in all_posts
            ) / total_weight if total_weight > 0 else sum(sentiments) / len(sentiments)
        else:
            weighted_sent = 0.0
        
        logger.debug(
            f"[RedditWatcher] Asset {asset}: sentiment={weighted_sent:.3f}, "
            f"mentions={len(all_posts)}"
        )
        
        return {
            "asset": asset,
            "score": round(weighted_sent, 4),
            "posts": sorted(all_posts, key=lambda x: x["score"], reverse=True)[:20],
            "total_mentions": len(all_posts),
            "timestamp": datetime.utcnow().isoformat(),
        }
    
    def get_multi_asset_sentiment(self, assets: List[str]) -> Dict[str, Dict]:
        """Get sentiment for multiple assets at once."""
        results = {}
        for asset in assets:
            results[asset] = self.get_asset_sentiment(asset)
        return results
    
    def get_all_sentiment(self) -> Dict[str, Dict]:
        """Get sentiment for ALL configured assets."""
        return self.get_multi_asset_sentiment(list(self.ASSET_SUBREDDITS.keys()))
    
    def get_hot_topics(self, limit: int = 50) -> List[Dict]:
        """Get hot topics across all monitored subreddits."""
        all_posts = []
        
        # Reduced from 10 to 5 subreddits
        for subreddit in self.subreddits[:5]:
            posts = self._fetch_subreddit(subreddit, "hot", 20)
            if posts:
                for post in posts:
                    all_posts.append({
                        "title": post.get('title', ''),
                        "subreddit": subreddit,
                        "score": post.get('score', 0),
                        "comments": post.get('num_comments', 0),
                        "url": f"https://reddit.com{post.get('permalink', '')}",
                        "created": datetime.fromtimestamp(post.get('created_utc', 0)),
                    })
        
        return sorted(all_posts, key=lambda x: x["score"], reverse=True)[:limit]
    
    # ── Compatibility methods ──────────────────────────────────────────────

    def start_monitoring(self, interval_seconds: int = 180) -> None:
        """
        Start background thread polling whale alerts periodically.
        Increased interval from 120 to 180 seconds to reduce load.
        """
        if self.is_running:
            logger.debug("[RedditWatcher] Already running")
            return
        self.is_running = True
        import threading
        def _loop():
            logger.info("[RedditWatcher] Background monitor started")
            while self.is_running:
                try:
                    alerts = self.get_whale_alerts()
                    if alerts:
                        self.recent_whale_alerts = alerts
                        logger.debug(f"[RedditWatcher] Monitor: {len(alerts)} whale alerts")
                except Exception as e:
                    logger.error(f"[RedditWatcher] Monitor loop error: {e}")
                import time as _t
                _t.sleep(interval_seconds)
        threading.Thread(target=_loop, name="RedditWatcher", daemon=True).start()

    def stop_monitoring(self) -> None:
        """Stop background monitoring thread."""
        self.is_running = False
        logger.info("[RedditWatcher] Monitor stopped")

    def get_news_sentiment(self, limit: int = 200) -> Dict:
        """Compatibility method for overall crypto sentiment."""
        crypto_assets = ["BTC-USD", "ETH-USD", "SOL-USD", "BNB-USD", "XRP-USD"]
        results = self.get_multi_asset_sentiment(crypto_assets)
        scores = [r["score"] for r in results.values() if r["total_mentions"] > 0]
        avg = sum(scores) / len(scores) if scores else 0.0
        all_posts = []
        for r in results.values():
            all_posts.extend(r.get("posts", []))
        return {
            "score": round(avg, 4),
            "posts": sorted(all_posts, key=lambda x: x.get("score", 0), reverse=True)[:30],
            "total_posts": len(all_posts),
            "timestamp": __import__("datetime").datetime.utcnow().isoformat(),
        }

    def get_market_sentiment_by_asset(self, asset: str) -> Dict:
        """Compatibility method for asset sentiment."""
        return self.get_asset_sentiment(asset)

    def get_dashboard(self) -> Dict:
        """Get complete dashboard with sentiment and whale alerts."""
        sentiment = self.get_all_sentiment()
        whale_summary = self.get_whale_summary()
        
        return {
            "sentiment": sentiment,
            "whale_alerts": whale_summary,
            "timestamp": datetime.utcnow().isoformat(),
        }
