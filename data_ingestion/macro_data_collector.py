from __future__ import annotations

import json
import threading
import time
from datetime import datetime, timezone
from typing import Dict, Optional

import requests

from config.config import FRED_API_KEY
from utils.logger import get_logger

logger = get_logger()

# ── Series to track ────────────────────────────────────────────────────────────
HIGH_IMPACT_SERIES: Dict[str, str] = {
    "FEDFUNDS": "Fed Funds Rate",
    "CPIAUCSL": "US CPI (inflation)",
    "UNRATE":   "US Unemployment Rate",
    "GDP":      "US GDP Growth",
    "DGS10":    "10-Year Treasury Yield",
    "DEXUSEU":  "EUR/USD Exchange Rate (FRED)",
    "DEXJPUS":  "USD/JPY Exchange Rate (FRED)",
    "DCOILWTICO":"WTI Crude Oil Price",
}

# Minimum % change before we publish an event (avoids noise)
CHANGE_THRESHOLD_PCT = 0.10

FRED_BASE_URL = "https://api.stlouisfed.org/fred/series/observations"
POLL_INTERVAL = 3600   # 1 hour — FRED data is released at most daily


class MacroDataCollector:
    """
    Fetches macro data from FRED and publishes change events to Redis.
    Gracefully skips all operations when FRED_API_KEY is absent.
    """

    def __init__(self, poll_interval_secs: int = POLL_INTERVAL) -> None:
        self._interval  = poll_interval_secs
        self._running   = False
        self._thread:   Optional[threading.Thread] = None
        self._cache:    Dict[str, float] = {}      # series_id → last known value
        self._pub                        = None    # Redis publisher (lazy)

    # ── Public API ────────────────────────────────────────────────────────────

    def start(self) -> None:
        if not FRED_API_KEY:
            logger.info("[MacroCollector] FRED_API_KEY not set — macro collection disabled")
            return
        self._running = True
        self._init_redis()
        self._thread = threading.Thread(
            target=self._loop, name="MacroCollector", daemon=True
        )
        self._thread.start()
        logger.info("[MacroCollector] Started — polling FRED every "
                    f"{self._interval // 60} min")

    def stop(self) -> None:
        self._running = False

    def get_latest(self) -> Dict[str, float]:
        """Return the most recently fetched value for each series."""
        return dict(self._cache)

    # ── Internal ──────────────────────────────────────────────────────────────

    def _init_redis(self) -> None:
        try:
            import redis
            from config.config import REDIS_URL
            from services.redis_pool import get_client as _get_redis_client

            self._pub = _get_redis_client()
            self._pub.ping()
        except Exception as e:
            logger.warning(f"[MacroCollector] Redis unavailable: {e}")

    def _loop(self) -> None:
        while self._running:
            for series_id, label in HIGH_IMPACT_SERIES.items():
                try:
                    value = self._fetch_latest(series_id)
                    if value is not None:
                        self._process(series_id, label, value)
                except Exception as e:
                    logger.debug(f"[MacroCollector] {series_id} error: {e}")
                time.sleep(0.5)   # gentle rate-limiting between FRED calls
            time.sleep(self._interval)

    @staticmethod
    def _fetch_latest(series_id: str) -> Optional[float]:
        """Return the most recent observation value from FRED."""
        try:
            resp = requests.get(
                FRED_BASE_URL,
                params={
                    "series_id":       series_id,
                    "api_key":         FRED_API_KEY,
                    "file_type":       "json",
                    "sort_order":      "desc",
                    "limit":           1,
                    "observation_end": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                },
                timeout=15,
            )
            resp.raise_for_status()
            observations = resp.json().get("observations", [])
            if observations:
                raw = observations[0].get("value", ".")
                if raw != ".":
                    return float(raw)
        except requests.RequestException as e:
            logger.debug(f"[MacroCollector] FRED request {series_id}: {e}")
        except (ValueError, KeyError) as e:
            logger.debug(f"[MacroCollector] FRED parse {series_id}: {e}")
        return None

    def _process(self, series_id: str, label: str, value: float) -> None:
        """Compare to cached value; publish event if meaningfully changed."""
        prev = self._cache.get(series_id)
        if prev is None:
            # First fetch — just cache, don't alert
            self._cache[series_id] = value
            return

        change_pct = ((value - prev) / abs(prev) * 100) if prev != 0 else 0.0

        if abs(change_pct) < CHANGE_THRESHOLD_PCT:
            return

        impact = self._classify_impact(series_id, abs(change_pct))
        event  = {
            "type":       "MACRO_NEWS_EVENT",
            "series_id":  series_id,
            "label":      label,
            "prev":       prev,
            "current":    value,
            "change_pct": round(change_pct, 4),
            "impact":     impact,
            "ts":         int(time.time() * 1000),
        }
        self._cache[series_id] = value

        if self._pub:
            try:
                self._pub.publish("MACRO_NEWS_EVENT", json.dumps(event))
            except Exception as e:
                logger.debug(f"[MacroCollector] Redis publish: {e}")

        logger.info(
            f"[MacroCollector] {label}: {prev} → {value} "
            f"({change_pct:+.3f}%) [{impact}]"
        )

    @staticmethod
    def _classify_impact(series_id: str, abs_change_pct: float) -> str:
        high_impact_series = {"FEDFUNDS", "CPIAUCSL", "UNRATE", "GDP"}
        if series_id in high_impact_series and abs_change_pct > 0.5:
            return "HIGH"
        if abs_change_pct > 0.2:
            return "MEDIUM"
        return "LOW"