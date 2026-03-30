from __future__ import annotations

import re
import time
import threading
from datetime import datetime
from typing import Dict, List, Optional

from services.intelligence_event_utils import record_whale_alert_event, score_whale_text
from utils.logger import logger

try:
    import tweepy
    _TWEEPY_OK = True
except ImportError:
    _TWEEPY_OK = False


class TwitterWhaleWatcher:
    """
    Monitors Twitter/X accounts that post whale alerts using the official API.
    If API keys are missing or billing is not enabled, starts in disabled mode
    and logs nothing — no noise on the terminal.
    """

    _WHALE_ACCOUNTS: List[str] = [
        "whale_alert",
        "lookonchain",
        "spotonchain",
        "OnchainDataNerd",
        "nansen_ai",
        "ArkhamIntel",
        "CryptoWhale",
        "WhaleWire",
        "santimentfeed",
        "glassnode",
        "intotheblock",
        "KobeissiLetter",
        "WatcherGuru",
        "tier10k",
    ]

    # Live price cache — no hardcoded fallbacks
    _price_cache: Dict[str, float] = {}
    _price_cache_ts: Dict[str, float] = {}
    _price_ttl: float = 300.0

    _PRICE_ASSETS: Dict[str, str] = {
        "BTC": "BTC-USD", "ETH": "ETH-USD", "BNB": "BNB-USD",
        "SOL": "SOL-USD", "XRP": "XRP-USD", "ADA": "ADA-USD",
        "DOGE": "DOGE-USD",
    }

    def _get_live_price(self, symbol: str) -> Optional[float]:
        """Fetch live price from Deriv. Returns None if unavailable."""
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
                return float(price)
        except Exception:
            pass
        return None

    def __init__(self) -> None:
        self._client:         Optional[tweepy.Client] = None
        self._enabled:        bool = False
        self._running:        bool = False
        self._thread:         Optional[threading.Thread] = None
        self._recent_tweets:  List[Dict] = []
        self._max_tweets:     int = 200
        self._last_request:   float = 0.0
        self._request_delay:  float = 1.0   # seconds between API calls

        self._setup()

    # ── Setup ─────────────────────────────────────────────────────────────────

    def _setup(self) -> None:
        if not _TWEEPY_OK:
            return   # tweepy not installed — silent

        import os
        api_key    = os.getenv("TWITTER_API_KEY", "")
        api_secret = os.getenv("TWITTER_API_SECRET", "")
        acc_token  = os.getenv("TWITTER_ACCESS_TOKEN", "")
        acc_secret = os.getenv("TWITTER_ACCESS_SECRET", "")

        if not all([api_key, api_secret, acc_token, acc_secret]):
            return   # credentials missing — silent, no warning spam

        try:
            self._client = tweepy.Client(
                consumer_key        = api_key,
                consumer_secret     = api_secret,
                access_token        = acc_token,
                access_token_secret = acc_secret,
                wait_on_rate_limit  = True,
            )
            me = self._client.get_me()
            if me and me.data:
                self._enabled = True
                logger.info(f"[Twitter] Connected as @{me.data.username}")
            else:
                logger.debug("[Twitter] Could not verify account — disabled")
        except Exception as e:
            msg = str(e).lower()
            if "453" in msg or "billing" in msg or "payment" in msg:
                logger.debug("[Twitter] API requires billing plan — disabled")
            else:
                logger.debug(f"[Twitter] Setup failed: {e}")

    # ── Whale extraction ───────────────────────────────────────────────────────

    def extract_whale_info(self, text: str) -> Optional[Dict]:
        if not text:
            return None
        clean = text.replace(",", "").replace("$", "")
        patterns = [
            r"(\d+\.?\d*)\s*(BTC|ETH|BNB|SOL|XRP|ADA|DOGE).*?(\d+\.?\d*)\s*(million|M|m)",
            r"(\d+\.?\d*)\s*(BTC|ETH|BNB|SOL|XRP).*?\$?(\d+\.?\d*)\s*(M|Million)",
            r"(\d+\.?\d*)\s*#?(BTC|ETH|BNB|SOL|XRP).*?(\d+\.?\d*)\s*M",
        ]
        for pattern in patterns:
            match = re.search(pattern, clean, re.IGNORECASE)
            if match:
                try:
                    amount = float(match.group(1))
                    symbol = match.group(2).upper()
                    if len(match.groups()) >= 4:
                        value_num = float(match.group(3))
                        unit      = match.group(4).lower()
                        value_usd = value_num * 1_000_000 if "m" in unit else value_num
                    else:
                        price = self._get_live_price(symbol)
                        if price is None:
                            continue   # skip if price unavailable
                        value_usd = amount * price
                    if value_usd >= 1_000_000:
                        return {
                            "amount":    round(amount, 2),
                            "symbol":    symbol,
                            "value_usd": round(value_usd),
                        }
                except Exception:
                    pass
        return None

    # ── Fetch ──────────────────────────────────────────────────────────────────

    def _rate_limit(self) -> None:
        elapsed = time.time() - self._last_request
        if elapsed < self._request_delay:
            time.sleep(self._request_delay - elapsed)
        self._last_request = time.time()

    def _fetch_account(self, username: str, count: int = 10) -> List[Dict]:
        if not self._enabled or not self._client:
            return []
        try:
            self._rate_limit()
            user = self._client.get_user(username=username)
            if not user or not user.data:
                return []
            tweets = self._client.get_users_tweets(
                id           = user.data.id,
                max_results  = count,
                tweet_fields = ["created_at"],
                exclude      = ["retweets", "replies"],
            )
            results = []
            if tweets and tweets.data:
                for t in tweets.data:
                    results.append({
                        "id":         t.id,
                        "text":       t.text,
                        "created_at": t.created_at or datetime.utcnow(),
                        "account":    username,
                        "source":     f"Twitter @{username}",
                    })
            return results
        except Exception as e:
            msg = str(e).lower()
            if "402" in msg or "payment" in msg:
                self._enabled = False
                logger.debug("[Twitter] Billing required — disabling")
            else:
                logger.debug(f"[Twitter] Fetch @{username}: {e}")
            return []

    def fetch_whale_tweets(self) -> List[Dict]:
        if not self._enabled:
            return []

        all_tweets = []
        for account in self._WHALE_ACCOUNTS[:8]:
            tweets = self._fetch_account(account, count=10)
            for tweet in tweets:
                try:
                    from narrative_ai import ingest as narrative_ingest
                    narrative_ingest(tweet.get("text", ""), source="twitter")
                except Exception:
                    pass
                info = self.extract_whale_info(tweet.get("text", ""))
                if info:
                    sentiment = score_whale_text(tweet.get("text", ""))
                    created_at = tweet.get("created_at") or datetime.utcnow()
                    fallback_id = f"{account}:{info['symbol']}:{int(info['value_usd'])}:{int(created_at.timestamp())}"
                    external_id = f"twitter:{tweet.get('id') or fallback_id}"
                    record_whale_alert_event(
                        symbol=info["symbol"],
                        source=tweet.get("source", f"Twitter @{account}"),
                        value_usd=info["value_usd"],
                        raw_text=tweet.get("text", ""),
                        sentiment=sentiment,
                        timestamp=created_at,
                        metadata={"account": account, "tweet_id": tweet.get("id")},
                        external_id=external_id,
                    )
                    all_tweets.append({
                        "id":         tweet.get("id"),
                        "text":       tweet.get("text"),
                        "created_at": created_at,
                        "account":    account,
                        "whale_info": info,
                        "sentiment":  sentiment,
                        "external_id": external_id,
                        "source":     tweet.get("source", f"Twitter @{account}"),
                    })
            time.sleep(0.5)

        return all_tweets

    # ── Monitoring ─────────────────────────────────────────────────────────────

    def start_monitoring(self, interval_seconds: int = 120) -> None:
        if not self._enabled:
            return   # disabled — silent, no warning
        if self._running:
            return

        self._running = True

        def _loop():
            logger.info("[Twitter] Whale monitor started")
            while self._running:
                try:
                    tweets = self.fetch_whale_tweets()
                    if tweets:
                        self._recent_tweets = (
                            tweets + self._recent_tweets
                        )[:self._max_tweets]
                        count = sum(1 for t in tweets if t.get("whale_info"))
                        if count:
                            logger.info(f"[Twitter] {count} whale alert(s) found")
                except Exception as e:
                    logger.error(f"[Twitter] Monitor error: {e}")
                time.sleep(interval_seconds)

        self._thread = threading.Thread(target=_loop, daemon=True, name="TwitterWhale")
        self._thread.start()

    def stop_monitoring(self) -> None:
        self._running = False

    # ── Public API ─────────────────────────────────────────────────────────────

    def get_recent_alerts(self, min_value_usd: float = 1_000_000) -> List[Dict]:
        alerts = []
        for tweet in self._recent_tweets[:50]:
            info = tweet.get("whale_info", {})
            if info.get("value_usd", 0) >= min_value_usd:
                value_m = info["value_usd"] / 1_000_000
                dt = tweet.get("created_at", datetime.utcnow())
                alerts.append({
                    "title":     f"🐋 {info['amount']} {info['symbol']} (${value_m:.1f}M)",
                    "value_usd": info["value_usd"],
                    "symbol":    info["symbol"],
                    "date":      dt.isoformat() if hasattr(dt, "isoformat") else str(dt),
                    "source":    tweet.get("source", "Twitter"),
                    "account":   tweet.get("account", ""),
                    "created_at": dt,
                    "text":      tweet.get("text", ""),
                    "whale_info": info,
                    "sentiment":  tweet.get("sentiment", 0.1),
                    "external_id": tweet.get("external_id", ""),
                })
        return alerts
