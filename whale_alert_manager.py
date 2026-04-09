"""
whale_alert_manager.py — Whale alert aggregator.
No fake data. Thread-safe DB (per-call sessions). Singleton.
"""
from __future__ import annotations

import os
import threading
import time
import requests
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Callable

from config.config import (
    WHALE_REDDIT_WHALE_ENABLED,
    WHALE_TWITTER_WHALE_ENABLED,
)
from utils.logger import logger

try:
    from twitter_watcher import TwitterWhaleWatcher
except Exception:
    TwitterWhaleWatcher = None  # type: ignore

try:
    from telegram_whale_watcher import TelegramWhaleWatcher
except Exception:
    TelegramWhaleWatcher = None  # type: ignore

try:
    from reddit_watcher import RedditWatcher
except Exception:
    RedditWatcher = None  # type: ignore

try:
    from services.db_pool import get_db
    _DB_AVAILABLE = True
except Exception:
    _DB_AVAILABLE = False
    get_db = None  # type: ignore

try:
    from telethon_whale_store import whale_store as _whale_store
except Exception:
    _whale_store = None


# ── Whale text scorer — shared by all sources in this module ─────────────────
_WHALE_BEARISH_WORDS = {
    "dump", "dumped", "dumping", "sell", "selling", "sold", "distribution",
    "distributing", "outflow", "withdrawal", "withdrew", "exit", "exiting",
    "crash", "crashing", "fear", "panic", "warning", "suspect",
    "hack", "hacked", "stolen", "fraud", "scam", "liquidation", "liquidated",
    "bearish", "offload", "offloading", "drops", "falls", "declines",
}
_WHALE_BULLISH_WORDS = {
    "buy", "buying", "bought", "accumulation", "accumulating", "inflow",
    "deposit", "deposited", "holding", "hodl", "cold wallet", "cold storage",
    "bullish", "long", "institutional", "treasury", "reserve",
    "staking", "locked", "accumulate", "rises", "gains",
}

def _score_whale_text(text: str) -> float:
    """
    Score a whale alert text using financial keywords.
    Returns -1.0 (strong sell pressure) to +1.0 (strong buy pressure).
    Unknown transfers default to slightly positive (accumulation bias).
    """
    if not text:
        return 0.1
    lower   = text.lower()
    words   = {w.strip(".,!?;:") for w in lower.split()}
    bearish = len(words & _WHALE_BEARISH_WORDS)
    bullish = len(words & _WHALE_BULLISH_WORDS)
    if "to exchange" in lower or "exchange inflow" in lower:
        bearish += 1
    if "from exchange" in lower or "exchange outflow" in lower:
        bullish += 1
    total = bearish + bullish
    if total == 0:
        return 0.1
    raw = (bullish - bearish) / total
    return round(max(-1.0, min(1.0, raw)), 3)


class WhaleAlertAPI:
    """Authenticated whale-alert.io API. No fake data ever returned."""

    def __init__(self, api_key: str):
        if not api_key or "your_" in api_key.lower():
            raise RuntimeError(
                "WHALE_ALERT_KEY is missing or still a placeholder. "
                "Get a real key at https://whale-alert.io/ and set it in .env"
            )
        self._key       = api_key
        self._base_url  = "https://api.whale-alert.io/v1"
        self._session   = requests.Session()
        self._cache:     List[Dict] = []
        self._last_fetch = 0.0
        self._cache_ttl  = 300

    def fetch_transactions(self, min_value: int = 1_000_000) -> List[Dict]:
        if time.time() - self._last_fetch < self._cache_ttl:
            return list(self._cache)
        try:
            resp = self._session.get(
                f"{self._base_url}/transactions",
                params={"api_key": self._key, "min_value": min_value, "limit": 25},
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json()
        except requests.HTTPError as e:
            logger.warning(f"[WhaleAPI] HTTP error: {e}")
            return list(self._cache)
        except Exception as e:
            logger.warning(f"[WhaleAPI] Request failed: {e}")
            return list(self._cache)

        alerts = []
        for tx in data.get("transactions", []):
            try:
                value_usd = float(tx.get("amount_usd", 0))
                if value_usd < 1_000_000:
                    continue
                symbol = str(tx.get("symbol", "")).upper()
                # Derive direction from transaction type — from/to exchange metadata
                tx_type   = str(tx.get("transaction_type", "")).lower()
                from_owner = str(tx.get("from", {}).get("owner", "")).lower()
                to_owner   = str(tx.get("to", {}).get("owner", "")).lower()

                # Exchange inflow = whale selling, outflow = whale accumulating
                if "exchange" in to_owner:
                    direction = "SELL"   # moving TO exchange = selling intent
                elif "exchange" in from_owner:
                    direction = "BUY"    # moving FROM exchange = accumulation
                elif "unknown" in from_owner and "unknown" in to_owner:
                    direction = "BUY"    # wallet-to-wallet = accumulation bias
                else:
                    direction = "BUY"    # default accumulation bias

                # Sentiment scales with transaction size — larger = stronger signal
                if value_usd >= 100_000_000:    # $100M+
                    sentiment = 0.40 if direction == "BUY" else -0.40
                elif value_usd >= 10_000_000:   # $10M+
                    sentiment = 0.25 if direction == "BUY" else -0.25
                else:                            # $1M+
                    sentiment = 0.10 if direction == "BUY" else -0.10

                alerts.append({
                    "title":      f"🐋 {tx['amount']:.2f} {symbol} (${value_usd/1e6:.1f}M)",
                    "value_usd":  value_usd,
                    "symbol":     symbol,
                    "asset":      symbol,
                    "amount":     float(tx.get("amount", 0)),
                    "direction":  direction,
                    "alert_time": datetime.fromtimestamp(int(tx.get("timestamp", time.time()))),
                    "source":     "whale-alert.io",
                    "url":        tx.get("url", ""),
                    "sentiment":  sentiment,
                })
            except Exception as e:
                logger.warning(f"[WhaleAPI] Transaction parse error: {e}")
                continue

        self._cache      = alerts
        self._last_fetch = time.time()
        logger.info(f"[WhaleAPI] Fetched {len(alerts)} transactions")
        return alerts

    def fetch_by_symbol(self, symbol: str, min_value: int = 1_000_000) -> List[Dict]:
        return [a for a in self.fetch_transactions(min_value)
                if a.get("symbol") == symbol.upper()]


class WhaleAlertDB:
    """
    Database adapter over the shared DatabaseService.
    """

    def __init__(self):
        self.enabled = _DB_AVAILABLE and get_db is not None
        if self.enabled:
            try:
                self._db = get_db()
                self.enabled = bool(self._db and self._db.ping())
                logger.info("[WhaleDB] Connected")
            except Exception as e:
                logger.warning(f"[WhaleDB] Connection test failed: {e}")
                self.enabled = False
                self._db = None
        else:
            logger.warning("[WhaleDB] Not connected — DB persistence disabled")
            self._db = None

    def save_alert(self, alert_data: Dict) -> bool:
        if not self.enabled:
            return False
        try:
            return bool(self._db and self._db.save_whale_alert(alert_data))
        except Exception as e:
            logger.warning(f"[WhaleDB] Save failed: {e}")
        return False

    def save_alerts(self, alerts: List[Dict]) -> int:
        return sum(1 for a in alerts if self.save_alert(a))

    def get_alerts(self, hours: int = 24, min_value: int = 1_000_000) -> List[Dict]:
        if not self.enabled:
            return []
        try:
            alerts = list(self._db.get_recent_whale_alerts(hours=hours)) if self._db else []
            filtered = [a for a in alerts if float(a.get("value_usd", 0) or 0) >= float(min_value)]
            filtered.sort(key=lambda x: float(x.get("value_usd", 0) or 0), reverse=True)
            return filtered[:100]
        except Exception as e:
            logger.warning(f"[WhaleDB] Get alerts failed: {e}")
            return []

    def close(self):
        pass  # no persistent session to close


class WhaleAlertManager:
    """
    Aggregates whale alerts from the paid API and Telegram fallback.
    Social sources can still be used elsewhere for sentiment, but are disabled
    here by default so whale pressure is not mixed with noisy crowd chatter.
    """

    _instance: Optional["WhaleAlertManager"] = None
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

        # ── Safe defaults first — must exist before anything can fail ─────
        self.collecting       = False
        self.on_alert:        Optional[Callable[[Dict], None]] = None
        self.all_alerts:      List[Dict] = []
        self.max_alerts       = 100
        self.whale_api        = None
        self.twitter_watcher  = None
        self.telegram_watcher = None
        self.reddit           = None
        self.db               = WhaleAlertDB()

        logger.info("[WhaleManager] Initialising...")

        self._init_whale_api()
        self._init_twitter_watcher()
        self._init_telegram_watcher()
        self._init_reddit_watcher()
        self._log_source_summary()

    def _init_whale_api(self) -> None:
        api_key = os.getenv("WHALE_ALERT_KEY", "")
        if api_key and "your_" not in api_key.lower():
            try:
                self.whale_api = WhaleAlertAPI(api_key)
                logger.info("[WhaleManager] Whale Alert API: ACTIVE")
            except RuntimeError as e:
                logger.warning(f"[WhaleManager] Whale Alert API init failed: {e}")
        else:
            logger.warning(
                "[WhaleManager] WHALE_ALERT_KEY not set — "
                "authenticated API disabled. Set it in .env for richer whale data."
            )

    def _init_twitter_watcher(self) -> None:
        if WHALE_TWITTER_WHALE_ENABLED and TwitterWhaleWatcher:
            try:
                self.twitter_watcher = TwitterWhaleWatcher()
            except Exception as e:
                logger.warning(f"[WhaleManager] TwitterWhaleWatcher init failed: {e}")
        elif not WHALE_TWITTER_WHALE_ENABLED:
            logger.info("[WhaleManager] Twitter whale source disabled — Twitter can remain sentiment-only")

    def _init_telegram_watcher(self) -> None:
        if TelegramWhaleWatcher:
            try:
                self.telegram_watcher = TelegramWhaleWatcher()
            except Exception as e:
                logger.warning(f"[WhaleManager] TelegramWhaleWatcher init failed: {e}")

    def _init_reddit_watcher(self) -> None:
        if WHALE_REDDIT_WHALE_ENABLED and RedditWatcher:
            try:
                self.reddit = RedditWatcher()
                logger.info("[WhaleManager] Reddit whale source enabled")
            except Exception as e:
                logger.warning(f"[WhaleManager] RedditWatcher init failed: {e}")
        elif not WHALE_REDDIT_WHALE_ENABLED:
            logger.info("[WhaleManager] Reddit whale source disabled — Reddit remains available for sentiment")

    def _log_source_summary(self) -> None:
        logger.info(
            f"[WhaleManager] "
            f"API={'on' if self.whale_api else 'off'}  "
            f"Twitter={'on' if self.twitter_watcher else 'off'}  "
            f"Telegram={'on' if self.telegram_watcher and getattr(self.telegram_watcher, 'bot_token', None) else 'off'}  "
            f"Reddit={'on' if self.reddit else 'off'}  "
            f"DB={'on' if self.db.enabled else 'off'}"
        )

    @staticmethod
    def _unique_alerts(alerts: List[Dict], key_fn: Callable[[Dict], object]) -> List[Dict]:
        seen = set()
        unique = []
        for alert in alerts:
            key = key_fn(alert)
            if key not in seen:
                seen.add(key)
                unique.append(alert)
        return unique

    @staticmethod
    def _sort_alerts_by_value(alerts: List[Dict]) -> List[Dict]:
        return sorted(alerts, key=lambda x: x.get("value_usd", 0), reverse=True)

    def _collect_api_alerts(self) -> List[Dict]:
        alerts: List[Dict] = []
        if self.whale_api:
            try:
                alerts.extend(self.whale_api.fetch_transactions())
            except Exception as e:
                logger.warning(f"[WhaleManager] API collect error: {e}")
        return alerts

    def _collect_twitter_alerts(self) -> List[Dict]:
        alerts: List[Dict] = []
        if not self.twitter_watcher:
            return alerts
        try:
            for a in self.twitter_watcher.get_recent_alerts():
                if "whale_info" not in a:
                    continue
                info = a["whale_info"]
                raw_text = a.get("text", "")
                sentiment = _score_whale_text(raw_text) if raw_text else 0.1
                direction = "BUY" if sentiment >= 0 else "SELL"
                alerts.append(
                    {
                        "title": f"🐋 {info['amount']} {info['symbol']} (${info['value_usd']/1e6:.1f}M)",
                        "value_usd": info["value_usd"],
                        "symbol": info["symbol"],
                        "asset": info["symbol"],
                        "direction": direction,
                        "alert_time": a.get("created_at", datetime.utcnow()),
                        "source": f"Twitter @{a['account']}",
                        "sentiment": sentiment,
                        "raw_text": raw_text,
                        "external_id": a.get("external_id", ""),
                    }
                )
        except Exception as e:
            logger.warning(f"[WhaleManager] Twitter collect error: {e}")
        return alerts

    def _collect_telegram_alerts(self) -> List[Dict]:
        alerts: List[Dict] = []
        if not (self.telegram_watcher and getattr(self.telegram_watcher, "bot_token", None)):
            return alerts
        try:
            for a in self.telegram_watcher.get_recent_alerts():
                alert_time = a["date"]
                if isinstance(alert_time, str):
                    try:
                        alert_time = datetime.fromisoformat(alert_time)
                    except Exception:
                        alert_time = datetime.utcnow()
                if hasattr(alert_time, "tzinfo") and alert_time.tzinfo is not None:
                    alert_time = alert_time.replace(tzinfo=None)
                sentiment = a.get("sentiment")
                if sentiment is None or sentiment == 0.1:
                    sentiment = _score_whale_text(a.get("title", ""))
                direction = "BUY" if sentiment >= 0 else "SELL"
                alerts.append(
                    {
                        "title": a["title"],
                        "value_usd": a["value_usd"],
                        "symbol": a["symbol"],
                        "asset": a["symbol"],
                        "direction": direction,
                        "alert_time": alert_time,
                        "source": a["source"],
                        "sentiment": sentiment,
                        "raw_text": a.get("raw_text", a.get("title", "")),
                        "external_id": a.get("external_id", ""),
                    }
                )
        except Exception as e:
            logger.warning(f"[WhaleManager] Telegram collect error: {e}")
        return alerts

    def _collect_reddit_alerts(self) -> List[Dict]:
        alerts: List[Dict] = []
        if not self.reddit:
            return alerts
        try:
            for a in self.reddit.get_whale_alerts():
                alert_time = a["created"]
                if hasattr(alert_time, "tzinfo") and alert_time.tzinfo is not None:
                    alert_time = alert_time.replace(tzinfo=None)
                sentiment = _score_whale_text(a.get("title", ""))
                direction = "BUY" if sentiment >= 0 else "SELL"
                alerts.append(
                    {
                        "title": a["title"],
                        "value_usd": a["value_usd"],
                        "symbol": a["symbol"],
                        "asset": a["symbol"],
                        "direction": direction,
                        "alert_time": alert_time,
                        "source": a["source"],
                        "sentiment": sentiment,
                        "raw_text": a.get("raw_text", a.get("title", "")),
                        "external_id": a.get("external_id", a.get("url", "")),
                    }
                )
        except Exception as e:
            logger.warning(f"[WhaleManager] Reddit collect error: {e}")
        return alerts

    def _dispatch_alerts(self, alerts: List[Dict]) -> None:
        if self.on_alert:
            for alert in alerts:
                try:
                    self.on_alert(alert)
                except Exception as e:
                    logger.warning(f"[WhaleManager] on_alert callback error: {e}")

    def _store_collected_alerts(self, alerts: List[Dict]) -> None:
        if self.db.enabled:
            saved = self.db.save_alerts(alerts)
            if saved:
                logger.info(f"[WhaleManager] Saved {saved} new alerts to DB")
        self.all_alerts = (alerts + self.all_alerts)[: self.max_alerts]

    def _collect_store_alerts(self, hours: int, min_value_usd: float) -> List[Dict]:
        alerts: List[Dict] = []
        try:
            if _whale_store and len(_whale_store) > 0:
                for a in _whale_store.format_for_dashboard(hours=hours):
                    if a.get("value_usd", 0) >= min_value_usd:
                        alerts.append(a)
        except Exception as e:
            logger.warning(f"[WhaleManager] Telethon store error: {e}")
        return alerts

    def _collect_db_alerts(self, hours: int, min_value_usd: float) -> List[Dict]:
        try:
            if self.db.enabled:
                return self.db.get_alerts(hours=hours, min_value=int(min_value_usd))
        except Exception as e:
            logger.warning(f"[WhaleManager] DB get alerts error: {e}")
        return []

    def _collect_memory_alerts(self, hours: int, min_value_usd: float) -> List[Dict]:
        alerts: List[Dict] = []
        try:
            cutoff = datetime.utcnow().replace(tzinfo=None) - timedelta(hours=hours)
            for a in self.all_alerts:
                if a.get("value_usd", 0) < min_value_usd:
                    continue
                alert_time = a.get("alert_time", datetime.utcnow())
                if hasattr(alert_time, "tzinfo") and alert_time.tzinfo is not None:
                    alert_time = alert_time.replace(tzinfo=None)
                if alert_time > cutoff:
                    alerts.append(a)
        except Exception as e:
            logger.warning(f"[WhaleManager] In-memory get alerts error: {e}")
        return alerts

    # ── Monitoring ────────────────────────────────────────────────────────

    def start_monitoring(self):
        if self.collecting:
            logger.debug("[WhaleManager] Already monitoring — skipping duplicate start")
            return

        if self.twitter_watcher:
            try:
                self.twitter_watcher.start_monitoring()
            except Exception as e:
                logger.warning(f"[WhaleManager] Twitter start failed: {e}")

        if self.telegram_watcher and getattr(self.telegram_watcher, "bot_token", None):
            try:
                self.telegram_watcher.start_monitoring()
            except Exception as e:
                logger.warning(f"[WhaleManager] Telegram start failed: {e}")

        if self.reddit:
            try:
                self.reddit.start_monitoring()
            except Exception as e:
                logger.warning(f"[WhaleManager] Reddit start failed: {e}")

        self.collecting = True
        threading.Thread(target=self._collect_loop, daemon=True).start()
        logger.info("[WhaleManager] Collector thread started")

    def _collect_loop(self):
        logger.info("[WhaleManager] Collector running (every 60s)")
        while self.collecting:
            try:
                self._collect_once()
            except Exception as e:
                logger.warning(f"[WhaleManager] Collector loop error: {e}")
            time.sleep(60)

    def _collect_once(self):
        all_new: List[Dict] = []
        all_new.extend(self._collect_api_alerts())
        all_new.extend(self._collect_twitter_alerts())
        all_new.extend(self._collect_telegram_alerts())
        all_new.extend(self._collect_reddit_alerts())
        if not all_new:
            return

        self._dispatch_alerts(all_new)
        unique = self._sort_alerts_by_value(self._unique_alerts(all_new, lambda a: a.get("title", "")))
        self._store_collected_alerts(unique)

    # ── Public API ────────────────────────────────────────────────────────

    def get_alerts(self, min_value_usd: float = 1_000_000, hours: int = 24) -> List[Dict]:
        results: List[Dict] = []
        results.extend(self._collect_store_alerts(hours, min_value_usd))
        results.extend(self._collect_db_alerts(hours, min_value_usd))
        results.extend(self._collect_memory_alerts(hours, min_value_usd))
        deduped = self._unique_alerts(
            results,
            lambda a: (a.get("title", ""), str(a.get("alert_time", ""))),
        )
        return self._sort_alerts_by_value(deduped)[: self.max_alerts]

    def get_alerts_for_symbol(self, symbol: str,
                               min_value_usd: float = 1_000_000,
                               days: int = 7) -> List[Dict]:
        return [a for a in self.get_alerts(min_value_usd, hours=days * 24)
                if a.get("symbol") == symbol]

    def get_top_alerts(self, limit: int = 10, days: int = 7) -> List[Dict]:
        return self.get_alerts(hours=days * 24)[:limit]

    def get_summary(self) -> Dict:
        alerts = self.get_alerts(hours=24)
        if not alerts:
            return {"total_alerts": 0}
        sources   = {}
        symbols   = {}
        total_val = 0.0
        for a in alerts:
            src         = a.get("source", "Unknown")
            sources[src] = sources.get(src, 0) + 1
            sym         = a.get("symbol", "Unknown")
            symbols[sym] = symbols.get(sym, 0) + 1
            total_val  += a.get("value_usd", 0)
        return {
            "total_alerts":         len(alerts),
            "total_value_millions": round(total_val / 1_000_000, 1),
            "by_source":            sources,
            "by_symbol":            dict(list(symbols.items())[:5]),
            "largest_alert":        alerts[0],
            "database_active":      self.db.enabled,
        }

    def stop(self):
        self.collecting = False
        for attr in ("twitter_watcher", "telegram_watcher", "reddit"):
            w = getattr(self, attr, None)
            if w and hasattr(w, "stop_monitoring"):
                try:
                    w.stop_monitoring()
                except Exception:
                    pass
        logger.info("[WhaleManager] Stopped")
