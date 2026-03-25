"""
monitoring/system_health_service.py — System health + data freshness monitor.

Changes vs original:
  - Added per-source data freshness tracking with configurable max-age thresholds.
  - Added signal_blocked_reason tracking when freshness violations block signals.
  - All exceptions properly logged (no silent pass).
  - Source health is exposed via get_source_health() for dashboard API.
"""
from __future__ import annotations

import os
import sys
import threading
import time
import traceback
from collections import defaultdict, deque
from datetime import datetime
from typing import Any, Deque, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.logger import get_logger

logger = get_logger()

# ── Alert thresholds ──────────────────────────────────────────────────────────

CPU_ALERT_PCT         = 90.0
RAM_ALERT_PCT         = 85.0
PHASE_SILENT_SECS     = 300
PIPELINE_SLOW_MS      = 5000
ERROR_RATE_PER_MIN    = 10
ALERT_COOLDOWN_SECS   = 300

# ── Collection intervals ──────────────────────────────────────────────────────

TELEMETRY_INTERVAL   = 30
ALERT_CHECK_INTERVAL = 60
HISTORY_WINDOW       = 3600

# ── Data freshness thresholds (seconds) ──────────────────────────────────────
# If a source hasn't been updated within this window, it is considered STALE.

FRESHNESS_THRESHOLDS: Dict[str, int] = {
    "order_book":   10,      # order book must update every 10s
    "trades":       30,      # trade feed every 30s
    "liquidations": 60,      # liquidation feed every 60s
    "news":         3600,    # news every hour
    "technicals":   300,     # OHLCV cached for 180s (CACHE_TTL) — allow 300s before stale
    "whale":        300,     # whale alerts every 5 min
    "sentiment":    1800,    # sentiment score every 30 min
    # FIX HIGH: FundingRateMonitor polls every POLL_INTERVAL=300 seconds.
    # The previous threshold of 30s meant the source was flagged as stale
    # for 270 out of every 300 seconds — perpetually stale, potentially
    # blocking all crypto signals in check_signal_data_freshness().
    # Set to 360s (300s poll + 20% buffer for processing/clock drift).
    "funding_rate": 360,
    "open_interest": 360,    # same poll interval as funding rate
    "macro":        3600,
}


class SystemHealthService:
    """
    Singleton telemetry collector, data-freshness checker, and alert dispatcher.
    """

    _instance: Optional["SystemHealthService"] = None
    _lock      = threading.Lock()

    def __new__(cls) -> "SystemHealthService":
        with cls._lock:
            if cls._instance is None:
                inst = super().__new__(cls)
                inst._initialised = False
                cls._instance = inst
        return cls._instance

    def __init__(self) -> None:
        if self._initialised:
            return
        self._initialised    = True
        self._running        = False
        self._telegram       = None
        self._pub            = None

        self._start_time     = time.time()
        self._signal_count   = 0
        self._signal_kills:  Dict[str, int]  = defaultdict(int)
        self._pipeline_lats: Deque[float]    = deque(maxlen=200)
        self._pred_lats:     Deque[float]    = deque(maxlen=200)
        self._error_log:     Deque[Dict]     = deque(maxlen=100)
        self._error_times:   Deque[float]    = deque(maxlen=500)
        self._win_count      = 0
        self._loss_count     = 0

        # ── Data freshness tracking ───────────────────────────────────────
        # source_name → last timestamp the source provided data
        self._source_last_seen: Dict[str, float] = {}
        self._source_lock       = threading.Lock()

        # ── Alert state ───────────────────────────────────────────────────
        self._last_alert:      Dict[str, float] = {}
        self._cpu_high_since:  Optional[float]  = None

        self._collect_thread: Optional[threading.Thread] = None
        self._alert_thread:   Optional[threading.Thread] = None

        self._init_redis()
        logger.info("[Monitor] SystemHealthService initialised")

    # ── Data freshness API ────────────────────────────────────────────────────

    def ping_source(self, source: str) -> None:
        """
        Called by any data provider to record that it produced data right now.
        Examples: funding_rate_monitor, order_flow, news_event_monitor.
        """
        with self._source_lock:
            self._source_last_seen[source] = time.time()

    def is_source_fresh(self, source: str) -> bool:
        """Return True if the source has produced data within its freshness window."""
        threshold = FRESHNESS_THRESHOLDS.get(source)
        if threshold is None:
            return True   # unknown source — don't block
        with self._source_lock:
            last = self._source_last_seen.get(source)
        # FIX S22: last can be 0.0 (epoch start) — treat as "never seen".
        if last is None or last == 0.0:
            return False
        return (time.time() - last) <= threshold

    def get_source_age_seconds(self, source: str) -> Optional[float]:
        with self._source_lock:
            last = self._source_last_seen.get(source)
        if last is None:
            return None
        return time.time() - last

    def get_source_health(self) -> Dict[str, Dict]:
        """Return health dict for all tracked sources (for dashboard)."""
        result = {}
        now = time.time()
        for source, threshold in FRESHNESS_THRESHOLDS.items():
            with self._source_lock:
                last = self._source_last_seen.get(source)
            if last is None:
                result[source] = {
                    "status":    "never_seen",
                    "age_secs":  None,
                    "threshold": threshold,
                    "fresh":     False,
                }
            else:
                age   = now - last
                fresh = age <= threshold
                result[source] = {
                    "status":    "fresh" if fresh else "stale",
                    "age_secs":  round(age, 1),
                    "threshold": threshold,
                    "fresh":     fresh,
                }
        return result

    def check_signal_data_freshness(
        self,
        asset: str,
        category: str,
    ) -> Tuple[bool, str]:
        """
        Check whether required data sources for this asset/category are fresh.
        Returns (ok: bool, reason: str).
        Uses AssetProfile to determine which sources are required.
        """
        from core.asset_profiles import get_profile
        profile = get_profile(asset)

        stale = []

        # Technicals always required
        if not self.is_source_fresh("technicals"):
            stale.append("technicals")

        if profile.use_order_flow and not self.is_source_fresh("order_book"):
            stale.append("order_book")

        if profile.use_liquidations and not self.is_source_fresh("liquidations"):
            stale.append("liquidations")

        if profile.use_funding_rates and not self.is_source_fresh("funding_rate"):
            stale.append("funding_rate")

        if stale:
            reason = f"Stale data sources: {', '.join(stale)}"
            logger.warning(f"[Monitor] Data freshness check failed for {asset}: {reason}")
            return False, reason

        return True, "ok"

    # ── Existing metric recording API ─────────────────────────────────────────

    def record_pipeline_latency(self, ms: float) -> None:
        self._pipeline_lats.append(ms)

    def record_prediction_latency(self, ms: float) -> None:
        self._pred_lats.append(ms)

    def record_signal(self, asset: str, direction: str, survived: bool) -> None:
        self._signal_count += 1

    def record_kill(self, layer_name: str) -> None:
        self._signal_kills[layer_name] += 1

    def record_trade_result(self, pnl: float) -> None:
        if pnl > 0:
            self._win_count += 1
        else:
            self._loss_count += 1

    def record_error(self, module: str, message: str) -> None:
        self._error_log.append({
            "module": module,
            "message": message[:200],
            "ts": datetime.utcnow().isoformat(),
        })
        self._error_times.append(time.time())

    # ── Start / Stop ──────────────────────────────────────────────────────────

    def start(self, telegram_bot=None) -> None:
        # FIX HIGH S7: Previously, if self._running was already True (second
        # call from bot.py after Telegram init), the method returned immediately
        # and the telegram_bot argument was silently discarded — Phase 11 health
        # alerts to Bot 2 were permanently lost.
        # Now we always update _telegram regardless of running state, and only
        # skip thread creation if threads are already alive.
        if telegram_bot is not None:
            self._telegram = telegram_bot
            logger.info("[Monitor] Telegram bot reference updated")

        if self._running:
            return   # threads already running — only needed the telegram update
        self._running = True

        self._collect_thread = threading.Thread(
            target=self._collect_loop, name="Monitor-collect", daemon=True
        )
        self._alert_thread = threading.Thread(
            target=self._alert_loop, name="Monitor-alerts", daemon=True
        )
        self._collect_thread.start()
        self._alert_thread.start()
        logger.info("[Monitor] Started")

    def stop(self) -> None:
        self._running = False

    # ── Telemetry snapshot ────────────────────────────────────────────────────

    def get_snapshot(self) -> Dict:
        total_trades = self._win_count + self._loss_count
        win_rate     = self._win_count / total_trades if total_trades else 0.0
        avg_pipeline = (
            sum(self._pipeline_lats) / len(self._pipeline_lats)
            if self._pipeline_lats else 0.0
        )
        avg_pred = (
            sum(self._pred_lats) / len(self._pred_lats)
            if self._pred_lats else 0.0
        )
        recent_errors = [
            e for e in self._error_log
            if (datetime.utcnow() - datetime.fromisoformat(e["ts"])).total_seconds() < 3600
        ]
        return {
            "uptime_seconds":      round(time.time() - self._start_time),
            "total_signals":       self._signal_count,
            "total_trades":        total_trades,
            "win_rate":            round(win_rate, 3),
            "avg_pipeline_ms":     round(avg_pipeline, 1),
            "avg_prediction_ms":   round(avg_pred, 1),
            "signal_kills":        dict(self._signal_kills),
            "recent_error_count":  len(recent_errors),
            "recent_errors":       recent_errors[-5:],
            "source_health":       self.get_source_health(),
        }

    # ── Internal loops ────────────────────────────────────────────────────────

    def _init_redis(self) -> None:
        try:
            from services.redis_pool import get_client as _get_redis_client
            self._pub = _get_redis_client()
            self._pub.ping()
        except Exception as e:
            logger.warning(f"[Monitor] Redis unavailable: {e}")

    def _collect_loop(self) -> None:
        while self._running:
            try:
                snapshot = self.get_snapshot()
                if self._pub:
                    import json
                    self._pub.set("monitor:snapshot", json.dumps(snapshot), ex=120)
            except Exception as e:
                logger.error(f"[Monitor] Collect loop error: {e}")
            time.sleep(TELEMETRY_INTERVAL)

    def _alert_loop(self) -> None:
        while self._running:
            try:
                self._check_alerts()
            except Exception as e:
                logger.error(f"[Monitor] Alert loop error: {e}")
            time.sleep(ALERT_CHECK_INTERVAL)

    def _check_alerts(self) -> None:
        # CPU / RAM
        try:
            import psutil
            cpu = psutil.cpu_percent(interval=1)
            ram = psutil.virtual_memory().percent
            if cpu > CPU_ALERT_PCT:
                self._send_alert("cpu_high", f"CPU usage {cpu:.0f}% (threshold {CPU_ALERT_PCT}%)")
            if ram > RAM_ALERT_PCT:
                self._send_alert("ram_high", f"RAM usage {ram:.0f}% (threshold {RAM_ALERT_PCT}%)")
        except Exception as e:
            logger.error(f"[Monitor] CPU/RAM check failed: {e}")

        # Pipeline latency
        if self._pipeline_lats:
            avg_ms = sum(self._pipeline_lats) / len(self._pipeline_lats)
            if avg_ms > PIPELINE_SLOW_MS:
                self._send_alert(
                    "pipeline_slow",
                    f"Pipeline avg latency {avg_ms:.0f}ms > {PIPELINE_SLOW_MS}ms",
                )

        # Error rate
        now = time.time()
        recent_errors = sum(1 for t in self._error_times if now - t < 60)
        if recent_errors > ERROR_RATE_PER_MIN:
            self._send_alert("error_rate", f"Error rate {recent_errors}/min")

        # Stale sources
        for source, health in self.get_source_health().items():
            if not health["fresh"] and health["age_secs"] is not None:
                self._send_alert(
                    f"stale_{source}",
                    f"Data source '{source}' is stale (age={health['age_secs']:.0f}s, max={health['threshold']}s)",
                )

    def _send_alert(self, alert_type: str, message: str) -> None:
        now = time.time()
        if now - self._last_alert.get(alert_type, 0) < ALERT_COOLDOWN_SECS:
            return
        self._last_alert[alert_type] = now
        logger.warning(f"[Monitor] ALERT [{alert_type}]: {message}")
        if self._telegram:
            try:
                msg = f"⚠️ System Alert\n`{alert_type}`\n{message}"
                from config.config import INTELLIGENCE_CHAT_ID
                self._telegram.send_message(msg)
            except Exception as e:
                logger.error(f"[Monitor] Telegram alert failed: {e}")


# ── Global singleton ──────────────────────────────────────────────────────────
monitor = SystemHealthService()


def start_monitoring(telegram_bot=None) -> None:
    monitor.start(telegram_bot=telegram_bot)
