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
PHASE_SILENT_SECS     = 300     # 5 minutes
PIPELINE_SLOW_MS      = 5000    # 5 seconds
ERROR_RATE_PER_MIN    = 10
ALERT_COOLDOWN_SECS   = 300     # don't repeat same alert within 5 minutes

# ── Collection intervals ──────────────────────────────────────────────────────
TELEMETRY_INTERVAL    = 30      # publish full snapshot every 30s
ALERT_CHECK_INTERVAL  = 60      # check alert conditions every 60s
HISTORY_WINDOW        = 3600    # keep 1 hour of history


class SystemHealthService:
    """
    Singleton telemetry collector and alert dispatcher.
    Runs two background threads: one for collection, one for alerting.
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

        # ── Metrics stores ────────────────────────────────────────────────
        self._start_time     = time.time()
        self._signal_count   = 0
        self._signal_kills:  Dict[str, int] = defaultdict(int)  # layer → kills
        self._pipeline_lats: Deque[float]   = deque(maxlen=200)
        self._pred_lats:     Deque[float]   = deque(maxlen=200)
        self._error_log:     Deque[Dict]    = deque(maxlen=100)
        self._error_times:   Deque[float]   = deque(maxlen=500)
        self._win_count      = 0
        self._loss_count     = 0

        # ── Alert state ───────────────────────────────────────────────────
        self._last_alert:    Dict[str, float] = {}
        self._cpu_high_since: Optional[float] = None

        # ── Threads ───────────────────────────────────────────────────────
        self._collect_thread: Optional[threading.Thread] = None
        self._alert_thread:   Optional[threading.Thread] = None

        self._init_redis()
        logger.info("[Monitor] SystemHealthService initialised")

    # ── Public API ────────────────────────────────────────────────────────────

    def start(self) -> None:
        """Start background collection and alert threads."""
        if self._running:
            return
        self._running = True
        self._collect_thread = threading.Thread(
            target=self._collect_loop, name="SysHealth-collect", daemon=True
        )
        self._alert_thread = threading.Thread(
            target=self._alert_loop, name="SysHealth-alert", daemon=True
        )
        self._collect_thread.start()
        self._alert_thread.start()
        logger.info("[Monitor] Started — telemetry every 30s, alerts every 60s")

    def stop(self) -> None:
        self._running = False

    def wire_telegram(self, telegram_bot) -> None:
        self._telegram = telegram_bot
        logger.info("[Monitor] Telegram wired for alerts")

    # ── Instrumentation hooks (called by other components) ────────────────────

    def record_signal(self, asset: str, direction: str, survived: bool) -> None:
        """Call when a signal completes the pipeline."""
        self._signal_count += 1
        if not survived:
            pass  # kill reason tracked separately via record_kill

    def record_kill(self, layer_name: str) -> None:
        """Call when a pipeline layer kills a signal."""
        self._signal_kills[layer_name] += 1

    def record_pipeline_latency(self, ms: float) -> None:
        """Call after pipeline.run() completes."""
        self._pipeline_lats.append(ms)

    def record_prediction_latency(self, ms: float) -> None:
        """Call after MLPredictor.predict() returns."""
        self._pred_lats.append(ms)

    def record_error(self, module: str, message: str,
                     exc: Optional[Exception] = None) -> None:
        """Call when any module catches an unexpected error."""
        self._error_times.append(time.time())
        self._error_log.append({
            "module":  module,
            "message": message,
            "tb":      traceback.format_exc() if exc else "",
            "ts":      time.time(),
        })

    def record_trade_result(self, pnl: float) -> None:
        """Call when a trade closes."""
        if pnl > 0:
            self._win_count += 1
        else:
            self._loss_count += 1

    # ── Snapshot ──────────────────────────────────────────────────────────────

    def snapshot(self) -> Dict[str, Any]:
        """Return the current full telemetry snapshot."""
        sys_metrics   = self._collect_system_metrics()
        phase_health  = self._collect_phase_health()
        pipeline_stats = self._collect_pipeline_stats()
        error_stats   = self._collect_error_stats()

        return {
            "uptime_s":     round(time.time() - self._start_time),
            "ts":           datetime.now().isoformat(),
            "system":       sys_metrics,
            "phases":       phase_health,
            "pipeline":     pipeline_stats,
            "errors":       error_stats,
            "signals": {
                "total":    self._signal_count,
                "kills":    dict(self._signal_kills),
                "win_rate": self._live_win_rate(),
            },
        }

    # ── Collection loops ──────────────────────────────────────────────────────

    def _collect_loop(self) -> None:
        while self._running:
            try:
                snap = self.snapshot()
                self._publish(snap)
                self._store_redis(snap)
            except Exception as e:
                logger.debug(f"[Monitor] collect: {e}")
            time.sleep(TELEMETRY_INTERVAL)

    def _alert_loop(self) -> None:
        while self._running:
            try:
                self._check_alerts()
            except Exception as e:
                logger.debug(f"[Monitor] alert check: {e}")
            time.sleep(ALERT_CHECK_INTERVAL)

    # ── Metric collectors ─────────────────────────────────────────────────────

    def _collect_system_metrics(self) -> Dict:
        result = {
            "cpu_pct":    0.0, "ram_pct":   0.0,
            "disk_pct":   0.0, "proc_mb":   0.0,
            "threads":    0,   "uptime_s":  round(time.time() - self._start_time),
        }
        try:
            import psutil
            result["cpu_pct"]  = psutil.cpu_percent(interval=0)
            result["ram_pct"]  = psutil.virtual_memory().percent
            result["disk_pct"] = psutil.disk_usage("/").percent
            result["proc_mb"]  = round(
                psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024, 1
            )
            result["threads"]  = threading.active_count()
        except Exception:
            pass
        return result

    def _collect_phase_health(self) -> Dict[str, bool]:
        health: Dict[str, bool] = {}
        checks = {
            "phase1_data_feeds":   ("data_ingestion",    "is_running", None),
            "phase2_whale_intel":  ("whale_intelligence", "is_running", None),
            "phase3_order_flow":   ("order_flow",         None,          None),
            "phase4_narrative_ai": ("narrative_ai",       None,          None),
            "phase5_strategy_lab": ("strategy_lab",       None,          None),
            "phase6_meta_ai":      ("ml.meta_model",      None,          None),
            "phase7_intel_alerts": ("services.intelligence_alerts", "alert_service", "_running"),
        }
        for phase, (mod, check_attr, flag) in checks.items():
            try:
                m = __import__(mod, fromlist=[check_attr] if check_attr else [])
                if check_attr:
                    check_obj = getattr(m, check_attr, None)
                else:
                    check_obj = None

                if check_obj is None and flag:
                    # legacy behavior: directly check flag on module or submodule object
                    check_obj = getattr(m, flag, None)

                if callable(check_obj):
                    health[phase] = bool(check_obj())
                elif isinstance(check_obj, (bool, int, float, str)):
                    health[phase] = bool(check_obj)
                elif check_obj is not None:
                    health[phase] = bool(getattr(check_obj, flag, False)) if flag else True
                else:
                    health[phase] = check_attr is None
            except Exception:
                health[phase] = False

        # Infrastructure
        health["redis"]      = self._check_redis()
        health["postgres"]   = self._check_postgres()
        health["telegram"]   = self._telegram is not None

        return health

    def _collect_pipeline_stats(self) -> Dict:
        lats = list(self._pipeline_lats)
        pred = list(self._pred_lats)
        return {
            "avg_latency_ms":  round(sum(lats) / len(lats), 1) if lats else 0.0,
            "max_latency_ms":  round(max(lats), 1)             if lats else 0.0,
            "p95_latency_ms":  round(self._p95(lats), 1)       if lats else 0.0,
            "pred_avg_ms":     round(sum(pred) / len(pred), 1) if pred else 0.0,
            "pred_p95_ms":     round(self._p95(pred), 1)       if pred else 0.0,
            "samples":         len(lats),
        }

    def _collect_error_stats(self) -> Dict:
        now      = time.time()
        recent   = [t for t in self._error_times if now - t < 60]
        per_min  = len(recent)
        last_10  = list(self._error_log)[-10:]
        return {
            "rate_per_min": per_min,
            "total":        len(self._error_times),
            "last_10":      [{"module": e["module"], "message": e["message"][:100],
                              "ts": e["ts"]} for e in last_10],
        }

    # ── Alert checks ──────────────────────────────────────────────────────────

    def _check_alerts(self) -> None:
        snap = self.snapshot()
        sys  = snap["system"]

        # CPU high for sustained period
        if sys["cpu_pct"] >= CPU_ALERT_PCT:
            if self._cpu_high_since is None:
                self._cpu_high_since = time.time()
            elif time.time() - self._cpu_high_since > 120:
                self._fire_alert(
                    "high_cpu",
                    f"🔥 CPU {sys['cpu_pct']:.0f}% for 2+ minutes — bot may be struggling",
                    "WARNING",
                )
        else:
            self._cpu_high_since = None

        # RAM
        if sys["ram_pct"] >= RAM_ALERT_PCT:
            self._fire_alert(
                "high_ram",
                f"⚠️ RAM {sys['ram_pct']:.0f}% — risk of OOM crash",
                "WARNING",
            )

        # Pipeline latency
        pl = snap["pipeline"]["p95_latency_ms"]
        if pl > PIPELINE_SLOW_MS:
            self._fire_alert(
                "slow_pipeline",
                f"🐢 Pipeline P95 latency {pl:.0f}ms — signals delayed",
                "WARNING",
            )

        # Error rate
        er = snap["errors"]["rate_per_min"]
        if er >= ERROR_RATE_PER_MIN:
            self._fire_alert(
                "high_errors",
                f"❌ Error rate {er}/min — check logs immediately",
                "CRITICAL",
            )

        # Phase health
        for phase, alive in snap["phases"].items():
            if not alive and phase not in ("telegram",):
                self._fire_alert(
                    f"phase_down_{phase}",
                    f"💀 {phase.replace('_', ' ').title()} is DOWN",
                    "CRITICAL",
                )

    def _fire_alert(self, key: str, message: str, level: str) -> None:
        """Fire a Telegram alert with cooldown to prevent spam."""
        now  = time.time()
        last = self._last_alert.get(key, 0)
        if now - last < ALERT_COOLDOWN_SECS:
            return
        self._last_alert[key] = now

        prefix = "🚨 *CRITICAL*" if level == "CRITICAL" else "⚠️ *WARNING*"
        full   = f"{prefix}\n\n{message}\n\n_Time: {datetime.now().strftime('%H:%M:%S')}_"

        try:
            logger.warning(f"[Monitor] ALERT [{level}] {message}")
        except Exception:
            pass

        if self._telegram:
            try:
                self._telegram.send_message(full)
            except Exception as e:
                logger.debug(f"[Monitor] Telegram alert: {e}")

        self._publish({"type": "SYSTEM_ALERT", "level": level,
                        "message": message, "key": key, "ts": time.time()},
                      channel="SYSTEM_ALERT")

    # ── Redis ─────────────────────────────────────────────────────────────────

    def _init_redis(self) -> None:
        try:
            import redis
            from config.config import REDIS_URL
            from services.redis_pool import get_client as _get_redis_client

            self._pub = _get_redis_client()
            self._pub.ping()
        except Exception as e:
            logger.debug(f"[Monitor] Redis: {e}")

    def _publish(self, data: Dict, channel: str = "SYSTEM_TELEMETRY") -> None:
        if not self._pub:
            return
        try:
            import json
            self._pub.publish(channel, json.dumps(data, default=str))
        except Exception:
            pass

    def _store_redis(self, snap: Dict) -> None:
        if not self._pub:
            return
        try:
            import json
            self._pub.setex("monitoring:latest", 90, json.dumps(snap, default=str))
        except Exception:
            pass

    def _check_redis(self) -> bool:
        try:
            return bool(self._pub and self._pub.ping())
        except Exception:
            return False

    def _check_postgres(self) -> bool:
        try:
            from services.db_pool import get_db
            from sqlalchemy import text
            db = get_db()
            with db.get_session() as s:
                s.execute(text("SELECT 1"))
            return True
        except Exception:
            return False

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _live_win_rate(self) -> float:
        total = self._win_count + self._loss_count
        return round(self._win_count / total, 4) if total else 0.0

    @staticmethod
    def _p95(values: list) -> float:
        if not values:
            return 0.0
        s = sorted(values)
        return s[int(len(s) * 0.95)]


# ── Global singleton ──────────────────────────────────────────────────────────
monitor = SystemHealthService()


def start_monitoring(telegram_bot=None) -> None:
    """Call from bot.py to start the monitoring service."""
    if telegram_bot:
        monitor.wire_telegram(telegram_bot)
    monitor.start()