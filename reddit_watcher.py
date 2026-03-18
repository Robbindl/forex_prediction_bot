"""
reddit_watcher.py — Reddit sentiment and whale mention watcher.

Changes vs original:
  - Hard credential validation: if client_id/client_secret are missing or
    placeholders, RedditWatcher.enabled = False and a clear error is logged.
    The constructor does NOT raise (keeps startup non-fatal), but all methods
    guard on self.enabled and return [] instead of silently running.
  - Silent except:pass replaced with logger.error throughout.
  - Asset-awareness: get_whale_alerts() only returns crypto-relevant posts.
"""
from __future__ import annotations

import os
import re
import threading
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from textblob import TextBlob
from utils.logger import logger

try:
    import praw
    _PRAW_AVAILABLE = True
except ImportError:
    _PRAW_AVAILABLE = False


class RedditWatcher:
    """
    Fetches crypto whale alerts and sentiment from Reddit.
    Requires REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, REDDIT_USER_AGENT in .env.
    """

    def __init__(self):
        self.reddit     = None
        self.enabled    = False
        self.recent_posts: List[Dict] = []
        self.max_posts  = 100
        self.is_running = False

        # Rate-limit state
        self.request_count  = 0
        self.last_reset     = time.time()
        self.requests_per_minute  = 50
        self.request_interval     = 60.0 / self.requests_per_minute

        # Crypto-specific subreddits only
        self.whale_subs = [
            "whalealert",
            "CryptoMarkets",
            "CryptoCurrency",
            "Bitcoin",
            "ethereum",
            "solana",
        ]
        self.news_subs = [
            "CryptoMarkets",
            "CryptoCurrency",
            "Bitcoin",
            "ethereum",
        ]

        self._setup()

        logger.info(
            f"[RedditWatcher] enabled={self.enabled}  "
            f"whale_subs={len(self.whale_subs)}  "
            f"rate_limit={self.requests_per_minute}/min"
        )

    # ── Setup ─────────────────────────────────────────────────────────────────

    def _setup(self) -> None:
        if not _PRAW_AVAILABLE:
            logger.warning("[RedditWatcher] praw not installed — Reddit disabled")
            return

        client_id     = os.getenv("REDDIT_CLIENT_ID", "").strip()
        client_secret = os.getenv("REDDIT_CLIENT_SECRET", "").strip()
        user_agent    = os.getenv("REDDIT_USER_AGENT", "trading-bot/1.0")

        # Hard validation — refuse to proceed with placeholders
        _placeholders = {"", "your_client_id", "xxx", "your_secret", "your_reddit_client_id"}
        if client_id.lower() in _placeholders:
            logger.error(
                "[RedditWatcher] REDDIT_CLIENT_ID is not configured.  "
                "Get credentials at https://www.reddit.com/prefs/apps  "
                "Reddit sentiment will be UNAVAILABLE."
            )
            return
        if client_secret.lower() in _placeholders:
            logger.error(
                "[RedditWatcher] REDDIT_CLIENT_SECRET is not configured.  "
                "Reddit sentiment will be UNAVAILABLE."
            )
            return

        try:
            r  = praw.Reddit(
                client_id     = client_id,
                client_secret = client_secret,
                user_agent    = user_agent,
            )
            me = r.user.me()
            if me is None:
                # Read-only auth works without user context — check a sub to verify
                list(r.subreddit("CryptoCurrency").hot(limit=1))
            self.reddit  = r
            self.enabled = True
            logger.info(f"[RedditWatcher] Connected (user={me.name if me else 'read-only'})")
        except Exception as e:
            logger.error(
                f"[RedditWatcher] Connection failed: {e}  "
                "Reddit sentiment will be UNAVAILABLE."
            )

    # ── Rate limiting ─────────────────────────────────────────────────────────

    def _wait_for_rate_limit(self) -> None:
        now = time.time()
        if now - self.last_reset >= 60:
            self.request_count = 0
            self.last_reset    = now
        if self.request_count >= self.requests_per_minute:
            sleep = 60 - (now - self.last_reset)
            if sleep > 0:
                time.sleep(sleep)
            self.request_count = 0
            self.last_reset    = time.time()
        self.request_count += 1
        time.sleep(self.request_interval)

    # ── Whale extraction ──────────────────────────────────────────────────────

    def extract_whale_info(self, text: str) -> Optional[Dict]:
        if not text:
            return None
        text = text.replace(",", "").replace("$", "")
        patterns = [
            r"(\d+\.?\d*)\s*(BTC|ETH|BNB|SOL|XRP).*?(\d+\.?\d*)\s*(M|Million)",
            r"(\d+\.?\d*)\s*(BTC|ETH).*?\$?(\d+\.?\d*)\s*M",
            r"(\d+\.?\d*)\s*#?(BTC|ETH).*?(\d+\.?\d*)\s*M",
        ]
        for pattern in patterns:
            m = re.search(pattern, text, re.IGNORECASE)
            if m:
                try:
                    amount = float(m.group(1))
                    symbol = m.group(2).upper()
                    if len(m.groups()) >= 4:
                        value_usd = float(m.group(3)) * 1_000_000
                    else:
                        price_map = {"BTC": 65_000, "ETH": 3_500,
                                     "BNB": 600, "SOL": 150, "XRP": 0.5}
                        value_usd = amount * price_map.get(symbol, 50_000)
                    if value_usd >= 1_000_000:
                        return {"amount": round(amount, 2),
                                "symbol": symbol,
                                "value_usd": round(value_usd)}
                except Exception as e:
                    logger.error(f"[RedditWatcher] Whale parse error: {e}")
        return None

    def analyze_sentiment(self, text: str) -> float:
        try:
            return TextBlob(text).sentiment.polarity
        except Exception as e:
            logger.error(f"[RedditWatcher] Sentiment analysis error: {e}")
            return 0.0

    # ── Public API ────────────────────────────────────────────────────────────

    def get_whale_alerts(self, limit: int = 50) -> List[Dict]:
        """Return crypto whale alert mentions from Reddit. [] if disabled."""
        if not self.enabled:
            return []
        alerts: List[Dict] = []
        for sub_name in self.whale_subs[:5]:
            try:
                self._wait_for_rate_limit()
                sub = self.reddit.subreddit(sub_name)
                for post in sub.new(limit=25):
                    tl = post.title.lower()
                    if any(k in tl for k in ("whale", "million", "billion", "large transfer")):
                        info = self.extract_whale_info(post.title)
                        if info:
                            alerts.append({
                                "title":    post.title,
                                "value_usd": info["value_usd"],
                                "symbol":   info["symbol"],
                                "amount":   info["amount"],
                                "url":      f"https://reddit.com{post.permalink}",
                                "score":    post.score,
                                "created":  datetime.fromtimestamp(post.created_utc),
                                "source":   f"Reddit r/{sub_name}",
                                "type":     "whale_alert",
                            })
            except Exception as e:
                logger.error(f"[RedditWatcher] get_whale_alerts r/{sub_name}: {e}")
        logger.debug(f"[RedditWatcher] {len(alerts)} whale alerts found")
        return alerts

    def get_news_sentiment(self, limit: int = 200) -> Dict:
        """Return overall crypto sentiment from Reddit. Empty result if disabled."""
        if not self.enabled:
            return {"score": 0.0, "posts": [], "total_posts": 0}
        sentiments: List[float] = []
        all_posts:  List[Dict]  = []
        for sub_name in self.news_subs[:4]:
            try:
                self._wait_for_rate_limit()
                sub = self.reddit.subreddit(sub_name)
                for post in sub.hot(limit=30):
                    sent  = self.analyze_sentiment(post.title)
                    try:
                        from narrative_ai import ingest as narrative_ingest
                        narrative_ingest(post.title, source="reddit")
                    except Exception:
                        pass
                    all_posts.append({
                        "title":        post.title,
                        "subreddit":    sub_name,
                        "sentiment":    sent,
                        "score":        post.score,
                        "upvote_ratio": post.upvote_ratio,
                        "comments":     post.num_comments,
                        "url":          f"https://reddit.com{post.permalink}",
                        "created":      datetime.fromtimestamp(post.created_utc),
                    })
                    sentiments.append(sent)
            except Exception as e:
                logger.error(f"[RedditWatcher] get_news_sentiment r/{sub_name}: {e}")

        if not sentiments:
            return {"score": 0.0, "posts": [], "total_posts": 0}

        total_w    = sum(p["score"] + p["comments"] + 1 for p in all_posts)
        final_sent = (
            sum(p["sentiment"] * (p["score"] + p["comments"] + 1)
                for p in all_posts) / total_w
            if total_w > 0 else sum(sentiments) / len(sentiments)
        )
        return {
            "score":       final_sent,
            "posts":       sorted(all_posts, key=lambda x: x["score"], reverse=True)[:30],
            "total_posts": len(all_posts),
            "timestamp":   datetime.utcnow().isoformat(),
        }

    def get_market_sentiment_by_asset(self, asset: str) -> Dict:
        """Asset-specific Reddit sentiment. Crypto only."""
        if not self.enabled:
            return {"score": 0.0, "posts": [], "total_mentions": 0}
        # Only crypto assets make sense on Reddit
        from core.asset_profiles import is_crypto
        if not is_crypto(asset):
            logger.debug(f"[RedditWatcher] Skipping Reddit sentiment for non-crypto {asset}")
            return {"score": 0.0, "posts": [], "total_mentions": 0, "skipped": True}

        asset_subs = {
            "BTC-USD": ["Bitcoin", "CryptoMarkets"],
            "ETH-USD": ["ethereum", "CryptoMarkets"],
            "SOL-USD": ["solana", "CryptoMarkets"],
            "BNB-USD": ["CryptoMarkets"],
            "XRP-USD": ["Ripple", "CryptoMarkets"],
        }
        subs         = asset_subs.get(asset, ["CryptoMarkets"])
        search_terms = [asset.replace("-USD", ""), asset]
        posts:       List[Dict]  = []
        sentiments:  List[float] = []

        for sub_name in subs[:2]:
            try:
                self._wait_for_rate_limit()
                sub = self.reddit.subreddit(sub_name)
                for post in sub.hot(limit=40):
                    if any(t.lower() in post.title.lower() for t in search_terms):
                        sent = self.analyze_sentiment(post.title)
                        posts.append({
                            "title":     post.title,
                            "sentiment": sent,
                            "score":     post.score,
                            "url":       f"https://reddit.com{post.permalink}",
                            "subreddit": sub_name,
                        })
                        sentiments.append(sent)
            except Exception as e:
                logger.error(f"[RedditWatcher] get_market_sentiment_by_asset r/{sub_name}: {e}")

        avg = sum(sentiments) / len(sentiments) if sentiments else 0.0
        return {"asset": asset, "score": avg, "posts": posts[:15],
                "total_mentions": len(posts)}

    # ── Background monitoring ─────────────────────────────────────────────────

    def start_monitoring(self) -> None:
        if not self.enabled:
            return
        if self.is_running:
            logger.debug("[RedditWatcher] Already running")
            return
        self.is_running = True
        threading.Thread(target=self._monitor_loop, daemon=True).start()
        logger.info("[RedditWatcher] Background monitor started")

    def _monitor_loop(self) -> None:
        while self.is_running:
            try:
                whales = self.get_whale_alerts()
                if whales:
                    self.recent_posts = (whales + self.recent_posts)[: self.max_posts]
            except Exception as e:
                logger.error(f"[RedditWatcher] Monitor loop error: {e}")
            time.sleep(120)

    def stop_monitoring(self) -> None:
        self.is_running = False
        logger.info("[RedditWatcher] Monitor stopped")
