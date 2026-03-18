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
    from models.trade_models import WhaleAlert
    from config.database import SessionLocal
    _DB_AVAILABLE = True
except Exception:
    _DB_AVAILABLE = False
    SessionLocal = None  # type: ignore

try:
    from telethon_whale_store import whale_store as _whale_store
except Exception:
    _whale_store = None


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
                alerts.append({
                    "title":      f"🐋 {tx['amount']:.2f} {symbol} (${value_usd/1e6:.1f}M)",
                    "value_usd":  value_usd,
                    "symbol":     symbol,
                    "asset":      symbol,
                    "amount":     float(tx.get("amount", 0)),
                    "direction":  "BUY" if float(tx.get("amount", 0)) > 0 else "SELL",
                    "alert_time": datetime.fromtimestamp(int(tx.get("timestamp", time.time()))),
                    "source":     "whale-alert.io",
                    "url":        tx.get("url", ""),
                    "sentiment":  0.15 if value_usd > 10_000_000 else 0.1,
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
    Database handler. Uses a NEW session per call to avoid SQLAlchemy
    'concurrent operations are not permitted' errors across threads.
    """

    def __init__(self):
        self.enabled = _DB_AVAILABLE and SessionLocal is not None
        if self.enabled:
            # Test connectivity once
            try:
                s = SessionLocal()
                s.close()
                logger.info("[WhaleDB] Connected")
            except Exception as e:
                logger.warning(f"[WhaleDB] Connection test failed: {e}")
                self.enabled = False
        else:
            logger.warning("[WhaleDB] Not connected — DB persistence disabled")

    def _get_session(self):
        """Always return a fresh session — never reuse across threads."""
        return SessionLocal()

    def save_alert(self, alert_data: Dict) -> bool:
        if not self.enabled:
            return False
        session = self._get_session()
        try:
            exists = session.query(WhaleAlert).filter(
                WhaleAlert.title      == alert_data["title"],
                WhaleAlert.alert_time == alert_data["alert_time"],
            ).first()
            if not exists:
                alert = WhaleAlert(
                    title      = alert_data["title"],
                    symbol     = alert_data["symbol"],
                    value_usd  = alert_data["value_usd"],
                    source     = alert_data["source"],
                    alert_time = alert_data["alert_time"],
                )
                session.add(alert)
                session.commit()
                return True
        except Exception as e:
            logger.warning(f"[WhaleDB] Save failed: {e}")
            try:
                session.rollback()
            except Exception:
                pass
        finally:
            session.close()
        return False

    def save_alerts(self, alerts: List[Dict]) -> int:
        return sum(1 for a in alerts if self.save_alert(a))

    def get_alerts(self, hours: int = 24, min_value: int = 1_000_000) -> List[Dict]:
        if not self.enabled:
            return []
        session = self._get_session()
        try:
            cutoff = datetime.now() - timedelta(hours=hours)
            rows   = (
                session.query(WhaleAlert)
                .filter(
                    WhaleAlert.alert_time >= cutoff,
                    WhaleAlert.value_usd  >= min_value,
                )
                .order_by(WhaleAlert.value_usd.desc())
                .limit(100)
                .all()
            )
            return [{
                "title":          r.title,
                "value_usd":      float(r.value_usd),
                "symbol":         r.symbol,
                "source":         r.source,
                "alert_time":     r.alert_time.isoformat(),
                "value_millions": float(r.value_usd) / 1_000_000,
            } for r in rows]
        except Exception as e:
            logger.warning(f"[WhaleDB] Get alerts failed: {e}")
            return []
        finally:
            session.close()

    def close(self):
        pass  # no persistent session to close


class WhaleAlertManager:
    """
    Aggregates whale alerts from API, Twitter, Telegram, Reddit.
    Singleton. No fake data. Thread-safe.
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

        # ── Authenticated Whale Alert API ─────────────────────────────────
        _api_key = os.getenv("WHALE_ALERT_KEY", "")
        if _api_key and "your_" not in _api_key.lower():
            try:
                self.whale_api = WhaleAlertAPI(_api_key)
                logger.info("[WhaleManager] Whale Alert API: ACTIVE")
            except RuntimeError as e:
                logger.warning(f"[WhaleManager] Whale Alert API init failed: {e}")
        else:
            logger.warning(
                "[WhaleManager] WHALE_ALERT_KEY not set — "
                "authenticated API disabled. Set it in .env for richer whale data."
            )

        # ── Twitter watcher ───────────────────────────────────────────────
        if TwitterWhaleWatcher:
            try:
                self.twitter_watcher = TwitterWhaleWatcher()
            except Exception as e:
                logger.warning(f"[WhaleManager] TwitterWhaleWatcher init failed: {e}")

        # ── Telegram watcher ──────────────────────────────────────────────
        if TelegramWhaleWatcher:
            try:
                self.telegram_watcher = TelegramWhaleWatcher()
            except Exception as e:
                logger.warning(f"[WhaleManager] TelegramWhaleWatcher init failed: {e}")

        # ── Reddit watcher ────────────────────────────────────────────────
        if RedditWatcher:
            try:
                rw = RedditWatcher()
                self.reddit = rw if rw.enabled else None
                if not rw.enabled:
                    logger.info("[WhaleManager] Reddit: disabled (no credentials)")
            except Exception as e:
                logger.warning(f"[WhaleManager] RedditWatcher init failed: {e}")

        logger.info(
            f"[WhaleManager] "
            f"API={'✓' if self.whale_api else '✗'}  "
            f"Twitter={'✓' if self.twitter_watcher else '✗'}  "
            f"Telegram={'✓' if self.telegram_watcher and getattr(self.telegram_watcher, 'bot_token', None) else '✗'}  "
            f"Reddit={'✓' if self.reddit else '✗'}  "
            f"DB={'✓' if self.db.enabled else '✗'}"
        )

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

        # 1. Authenticated API
        if self.whale_api:
            try:
                all_new.extend(self.whale_api.fetch_transactions())
            except Exception as e:
                logger.warning(f"[WhaleManager] API collect error: {e}")

        # 2. Twitter
        if self.twitter_watcher:
            try:
                for a in self.twitter_watcher.get_recent_alerts():
                    if "whale_info" in a:
                        info = a["whale_info"]
                        all_new.append({
                            "title":      f"🐋 {info['amount']} {info['symbol']} (${info['value_usd']/1e6:.1f}M)",
                            "value_usd":  info["value_usd"],
                            "symbol":     info["symbol"],
                            "asset":      info["symbol"],
                            "direction":  "BUY",
                            "alert_time": a.get("created_at", datetime.utcnow()),
                            "source":     f"Twitter @{a['account']}",
                            "sentiment":  0.1,
                        })
            except Exception as e:
                logger.warning(f"[WhaleManager] Twitter collect error: {e}")

        # 3. Telegram
        if self.telegram_watcher and getattr(self.telegram_watcher, "bot_token", None):
            try:
                for a in self.telegram_watcher.get_recent_alerts():
                    alert_time = a["date"]
                    if isinstance(alert_time, str):
                        try:
                            alert_time = datetime.fromisoformat(alert_time)
                        except Exception:
                            alert_time = datetime.utcnow()
                    # Normalise to naive datetime
                    if hasattr(alert_time, "tzinfo") and alert_time.tzinfo is not None:
                        alert_time = alert_time.replace(tzinfo=None)
                    all_new.append({
                        "title":      a["title"],
                        "value_usd":  a["value_usd"],
                        "symbol":     a["symbol"],
                        "asset":      a["symbol"],
                        "direction":  "BUY" if a.get("sentiment", 0.1) >= 0 else "SELL",
                        "alert_time": alert_time,
                        "source":     a["source"],
                        "sentiment":  a.get("sentiment", 0.1),
                    })
            except Exception as e:
                logger.warning(f"[WhaleManager] Telegram collect error: {e}")

        # 4. Reddit
        if self.reddit:
            try:
                for a in self.reddit.get_whale_alerts():
                    alert_time = a["created"]
                    if hasattr(alert_time, "tzinfo") and alert_time.tzinfo is not None:
                        alert_time = alert_time.replace(tzinfo=None)
                    all_new.append({
                        "title":      a["title"],
                        "value_usd":  a["value_usd"],
                        "symbol":     a["symbol"],
                        "asset":      a["symbol"],
                        "direction":  "BUY",
                        "alert_time": alert_time,
                        "source":     a["source"],
                        "sentiment":  0.1,
                    })
            except Exception as e:
                logger.warning(f"[WhaleManager] Reddit collect error: {e}")

        if not all_new:
            return

        # Fire callback (feeds Layer 6)
        if self.on_alert:
            for alert in all_new:
                try:
                    self.on_alert(alert)
                except Exception as e:
                    logger.warning(f"[WhaleManager] on_alert callback error: {e}")

        # Deduplicate and store
        seen   = set()
        unique = []
        for a in all_new:
            key = a.get("title", "")
            if key not in seen:
                seen.add(key)
                unique.append(a)
        unique.sort(key=lambda x: x.get("value_usd", 0), reverse=True)

        if self.db.enabled:
            saved = self.db.save_alerts(unique)
            if saved:
                logger.info(f"[WhaleManager] Saved {saved} new alerts to DB")

        self.all_alerts = (unique + self.all_alerts)[: self.max_alerts]

    # ── Public API ────────────────────────────────────────────────────────

    def get_alerts(self, min_value_usd: float = 1_000_000, hours: int = 24) -> List[Dict]:
        results: List[Dict] = []

        # Telethon real-time store
        try:
            if _whale_store and len(_whale_store) > 0:
                for a in _whale_store.format_for_dashboard(hours=hours):
                    if a.get("value_usd", 0) >= min_value_usd:
                        results.append(a)
        except Exception as e:
            logger.warning(f"[WhaleManager] Telethon store error: {e}")

        # DB (fresh session per call — thread safe)
        try:
            if self.db.enabled:
                results.extend(self.db.get_alerts(hours=hours, min_value=int(min_value_usd)))
        except Exception as e:
            logger.warning(f"[WhaleManager] DB get alerts error: {e}")

        # In-memory
        try:
            cutoff = datetime.utcnow().replace(tzinfo=None) - timedelta(hours=hours)
            for a in self.all_alerts:
                if a.get("value_usd", 0) < min_value_usd:
                    continue
                alert_time = a.get("alert_time", datetime.utcnow())
                if hasattr(alert_time, "tzinfo") and alert_time.tzinfo is not None:
                    alert_time = alert_time.replace(tzinfo=None)
                if alert_time > cutoff:
                    results.append(a)
        except Exception as e:
            logger.warning(f"[WhaleManager] In-memory get alerts error: {e}")

        # Deduplicate
        seen    = set()
        deduped = []
        for a in results:
            key = (a.get("title", ""), str(a.get("alert_time", "")))
            if key not in seen:
                seen.add(key)
                deduped.append(a)
        deduped.sort(key=lambda x: x.get("value_usd", 0), reverse=True)
        return deduped[: self.max_alerts]

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
