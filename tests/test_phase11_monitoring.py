"""
tests/test_phase11_monitoring.py — Phase 11 Observability tests.

Run:
    pytest tests/test_phase11_monitoring.py -v -m "not integration"
"""
from __future__ import annotations

import time
import pytest


# ── MetricsCollector tests ────────────────────────────────────────────────────

class TestMetricsCollector:

    def test_record_and_retrieve(self):
        from monitoring.metrics import MetricsCollector
        m = MetricsCollector()
        m.record("test_metric", 120.0)
        m.record("test_metric", 80.0)
        m.record("test_metric", 200.0)
        stats = m.get("test_metric")
        assert stats["count"] == 3
        assert stats["avg_ms"] == pytest.approx(133.33, abs=0.1)
        assert stats["min_ms"] == 80.0
        assert stats["max_ms"] == 200.0

    def test_p95_correct(self):
        from monitoring.metrics import MetricsCollector
        m = MetricsCollector()
        for i in range(100):
            m.record("p95_test", float(i))
        stats = m.get("p95_test")
        assert 93.0 <= stats["p95_ms"] <= 96.0

    def test_error_count_tracked(self):
        from monitoring.metrics import MetricsCollector
        m = MetricsCollector()
        m.record("err_metric", 10.0, success=True)
        m.record("err_metric", 10.0, success=False)
        m.record("err_metric", 10.0, success=False)
        stats = m.get("err_metric")
        assert stats["errors"] == 2

    def test_unknown_metric_returns_zeros(self):
        from monitoring.metrics import MetricsCollector
        m     = MetricsCollector()
        stats = m.get("nonexistent")
        assert stats["count"]  == 0
        assert stats["avg_ms"] == 0.0

    def test_summary_returns_all_metrics(self):
        from monitoring.metrics import MetricsCollector
        m = MetricsCollector()
        m.record("alpha",  10.0)
        m.record("beta",   20.0)
        m.record("gamma",  30.0)
        s = m.summary()
        assert "alpha" in s
        assert "beta"  in s
        assert "gamma" in s

    def test_reset_clears_metric(self):
        from monitoring.metrics import MetricsCollector
        m = MetricsCollector()
        m.record("to_clear", 50.0)
        m.reset("to_clear")
        assert m.get("to_clear")["count"] == 0

    def test_reset_all_clears_everything(self):
        from monitoring.metrics import MetricsCollector
        m = MetricsCollector()
        m.record("a", 10.0)
        m.record("b", 20.0)
        m.reset()
        assert m.get("a")["count"] == 0
        assert m.get("b")["count"] == 0

    def test_window_size_respected(self):
        from monitoring.metrics import MetricsCollector
        m = MetricsCollector(window=10)
        for i in range(50):
            m.record("win", float(i))
        # Window of 10 — only last 10 samples retained
        assert m.get("win")["min_ms"] == pytest.approx(40.0)


# ── MetricsTimer tests ────────────────────────────────────────────────────────

class TestMetricsTimer:

    def test_timer_records_elapsed(self):
        from monitoring.metrics import MetricsTimer, MetricsCollector
        col = MetricsCollector()
        with MetricsTimer("timed_op", collector=col) as t:
            time.sleep(0.05)
        assert t.elapsed_ms >= 40.0
        assert col.get("timed_op")["count"] == 1

    def test_timer_records_success_true_on_normal_exit(self):
        from monitoring.metrics import MetricsTimer, MetricsCollector
        col = MetricsCollector()
        with MetricsTimer("success_op", collector=col):
            pass
        assert col.get("success_op")["errors"] == 0

    def test_timer_records_error_on_exception(self):
        from monitoring.metrics import MetricsTimer, MetricsCollector
        col = MetricsCollector()
        try:
            with MetricsTimer("fail_op", collector=col):
                raise ValueError("simulated error")
        except ValueError:
            pass
        assert col.get("fail_op")["errors"] == 1

    def test_elapsed_ms_available_after_context(self):
        from monitoring.metrics import MetricsTimer, MetricsCollector
        col = MetricsCollector()
        with MetricsTimer("check_elapsed", collector=col) as t:
            time.sleep(0.01)
        assert t.elapsed_ms > 0


# ── track_latency decorator tests ─────────────────────────────────────────────

class TestTrackLatency:

    def test_decorator_wraps_function(self):
        from monitoring.metrics import track_latency, MetricsCollector
        col = MetricsCollector()

        @track_latency("decorated_fn", collector=col)
        def my_func(x):
            return x * 2

        result = my_func(5)
        assert result == 10
        assert col.get("decorated_fn")["count"] == 1

    def test_decorator_preserves_return_value(self):
        from monitoring.metrics import track_latency, MetricsCollector
        col = MetricsCollector()

        @track_latency("ret_test", collector=col)
        def returns_dict():
            return {"key": "value"}

        assert returns_dict() == {"key": "value"}

    def test_decorator_propagates_exceptions(self):
        from monitoring.metrics import track_latency, MetricsCollector
        col = MetricsCollector()

        @track_latency("exc_test", collector=col)
        def raises():
            raise RuntimeError("test error")

        with pytest.raises(RuntimeError):
            raises()
        assert col.get("exc_test")["errors"] == 1

    def test_decorator_preserves_function_name(self):
        from monitoring.metrics import track_latency

        @track_latency("name_test")
        def my_named_function():
            pass

        assert my_named_function.__name__ == "my_named_function"


# ── SystemHealthService tests ─────────────────────────────────────────────────

class TestSystemHealthService:

    def _make_monitor(self):
        """Create a fresh monitor instance for testing (bypasses singleton)."""
        from monitoring.system_health_service import SystemHealthService
        m = object.__new__(SystemHealthService)
        m._initialised  = False
        SystemHealthService.__init__(m)
        return m

    def test_snapshot_has_required_keys(self):
        from monitoring.system_health_service import monitor
        snap = monitor.snapshot()
        for key in ["uptime_s", "ts", "system", "pipeline", "errors", "signals"]:
            assert key in snap, f"Missing key: {key}"

    def test_system_metrics_has_cpu_ram(self):
        from monitoring.system_health_service import monitor
        sys = monitor.snapshot()["system"]
        for key in ["cpu_pct", "ram_pct", "threads", "uptime_s"]:
            assert key in sys

    def test_pipeline_stats_zero_on_fresh_instance(self):
        from monitoring.system_health_service import SystemHealthService
        m = SystemHealthService()
        m._pipeline_lats.clear()
        stats = m._collect_pipeline_stats()
        assert stats["avg_latency_ms"] == 0.0
        assert stats["samples"]        == 0

    def test_record_signal_increments_count(self):
        from monitoring.system_health_service import SystemHealthService
        m = SystemHealthService()
        before = m._signal_count
        m.record_signal("BTC-USD", "BUY", survived=True)
        m.record_signal("ETH-USD", "SELL", survived=False)
        assert m._signal_count == before + 2

    def test_record_kill_tracks_layer(self):
        from monitoring.system_health_service import SystemHealthService
        m = SystemHealthService()
        m._signal_kills.clear()
        m.record_kill("regime")
        m.record_kill("regime")
        m.record_kill("sentiment")
        assert m._signal_kills["regime"]   == 2
        assert m._signal_kills["sentiment"] == 1

    def test_record_pipeline_latency(self):
        from monitoring.system_health_service import SystemHealthService
        m = SystemHealthService()
        m._pipeline_lats.clear()
        m.record_pipeline_latency(320.0)
        m.record_pipeline_latency(450.0)
        stats = m._collect_pipeline_stats()
        assert stats["avg_latency_ms"] == pytest.approx(385.0, abs=0.1)
        assert stats["samples"] == 2

    def test_record_trade_updates_win_rate(self):
        from monitoring.system_health_service import SystemHealthService
        m = SystemHealthService()
        m._win_count  = 0
        m._loss_count = 0
        m.record_trade_result(100.0)
        m.record_trade_result(50.0)
        m.record_trade_result(-30.0)
        assert m._live_win_rate() == pytest.approx(0.6667, abs=0.001)

    def test_error_log_records(self):
        from monitoring.system_health_service import SystemHealthService
        m = SystemHealthService()
        before = len(m._error_log)
        m.record_error("test_module", "something went wrong")
        assert len(m._error_log) == before + 1
        last = m._error_log[-1]
        assert last["module"]  == "test_module"
        assert last["message"] == "something went wrong"

    def test_error_rate_calculation(self):
        from monitoring.system_health_service import SystemHealthService
        m = SystemHealthService()
        m._error_times.clear()
        now = time.time()
        # Add 5 errors within the last 60 seconds
        for i in range(5):
            m._error_times.append(now - i * 5)
        stats = m._collect_error_stats()
        assert stats["rate_per_min"] == 5

    def test_alert_cooldown_prevents_spam(self):
        from monitoring.system_health_service import SystemHealthService
        m = SystemHealthService()
        m._last_alert.clear()
        alerts_sent = []

        def fake_send(self, msg):          # <-- now accepts self
            alerts_sent.append(msg)

        m._telegram = type("FakeTG", (), {"send_message": fake_send})()
        m._fire_alert("test_key", "Test alert", "WARNING")
        m._fire_alert("test_key", "Test alert", "WARNING")  # should be blocked by cooldown
        assert len(alerts_sent) == 1

    def test_p95_calculation(self):
        from monitoring.system_health_service import SystemHealthService
        values = list(range(100))   # 0..99
        p95    = SystemHealthService._p95(values)
        assert p95 == 95   # 95th percentile of 0-99 is 95

    def test_phase_health_returns_dict(self):
        from monitoring.system_health_service import monitor
        phases = monitor._collect_phase_health()
        assert isinstance(phases, dict)
        # Redis and Postgres should always be in there
        assert "redis"    in phases
        assert "postgres" in phases
        assert "telegram" in phases


# ── Integration tests ─────────────────────────────────────────────────────────

@pytest.mark.integration
class TestMonitoringIntegration:

    def test_monitor_starts_and_publishes_to_redis(self):
        """Start monitoring and verify it publishes to Redis."""
        import redis as redis_lib
        try:
            r = redis_lib.Redis(host="localhost", port=6379)
            r.ping()
        except Exception:
            pytest.skip("Redis not reachable")

        import json
        from monitoring.system_health_service import monitor

        if not monitor._running:
            monitor.start()

        time.sleep(35)   # wait for one collection cycle

        raw = r.get("monitoring:latest")
        assert raw is not None, "No telemetry published to Redis"
        data = json.loads(raw)
        assert "system"   in data
        assert "pipeline" in data

    def test_metrics_timer_integrates_with_monitor(self):
        """MetricsTimer pipeline metric should forward to SystemHealthService."""
        from monitoring.metrics import MetricsTimer, PIPELINE
        from monitoring.system_health_service import monitor

        before = len(monitor._pipeline_lats)
        with MetricsTimer(PIPELINE):
            time.sleep(0.01)
        assert len(monitor._pipeline_lats) == before + 1
