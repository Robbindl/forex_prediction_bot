"""
tests/test_intelligence_alerts.py — Intelligence Alert System tests.

  Unit tests        — always run, no external services required.
  Integration tests — skipped when Redis is not reachable.

Run just unit tests:
    pytest tests/test_intelligence_alerts.py -v -m "not integration"

Run everything (requires live Redis):
    pytest tests/test_intelligence_alerts.py -v
"""
from __future__ import annotations

import json
import time
import pytest


# ── Fixtures ──────────────────────────────────────────────────────────────────

def _make_pub():
    """Fake Redis publisher that records published messages."""
    class FakePub:
        def __init__(self):
            self.messages = []
        def publish(self, channel, data):
            self.messages.append({"channel": channel, "data": json.loads(data)})
        def ping(self): pass
    return FakePub()


def _make_telegram():
    """Fake Telegram bot that records sent messages."""
    class FakeTelegram:
        def __init__(self):
            self.messages = []
        def send_message(self, text, **kwargs):
            self.messages.append(text)
            return True
    return FakeTelegram()


# ── AlertFormatter tests ──────────────────────────────────────────────────────

class TestAlertFormatter:

    def _fmt(self):
        from services.intelligence_alerts.alert_formatter import AlertFormatter
        return AlertFormatter()

    def test_liquidation_cascade_formatted(self):
        fmt   = self._fmt()
        event = {
            "type": "LIQUIDATION_CASCADE_ALERT",
            "asset": "BTCUSDT",
            "usd_total": 47_000_000,
            "window_s": 60,
            "severity": "CRITICAL",
        }
        msg = fmt.format("LIQUIDATION_CASCADE_ALERT", event, "CRITICAL")
        assert msg is not None
        assert "47,000,000" in msg
        assert "BTCUSDT" in msg
        assert "CRITICAL" in msg

    def test_whale_cluster_formatted(self):
        fmt   = self._fmt()
        event = {
            "type": "WHALE_CLUSTER_ALERT",
            "direction": "BUY",
            "wallet_count": 4,
            "total_asset": 28.5,
            "asset": "BTC",
            "confidence": 0.87,
            "labels": ["Whale A", "Whale B", "Whale C"],
        }
        msg = fmt.format("WHALE_CLUSTER_ALERT", event, "CRITICAL")
        assert msg is not None
        assert "4" in msg
        assert "BUY" in msg
        assert "28" in msg

    def test_stop_hunt_formatted(self):
        fmt   = self._fmt()
        event = {
            "type": "STOP_HUNT_DETECTED",
            "asset": "BTCUSDT",
            "wall_price": 64850.0,
            "wall_side": "BID",
            "wick_pct": 0.21,
            "revert_ms": 8200,
            "implication": "BUY",
            "confidence": 0.78,
        }
        msg = fmt.format("STOP_HUNT_DETECTED", event, "HIGH")
        assert msg is not None
        assert "64850" in msg
        assert "BUY" in msg
        assert "0.21" in msg

    def test_funding_rate_formatted(self):
        fmt   = self._fmt()
        event = {
            "type": "FUNDING_RATE_ALERT",
            "asset": "BTCUSDT",
            "rate": 0.0105,
            "rate_pct": 1.05,
            "bias": "EXTREME_LONG",
            "implication": "Long squeeze risk HIGH",
        }
        msg = fmt.format("FUNDING_RATE_ALERT", event, "HIGH")
        assert msg is not None
        assert "EXTREME_LONG" in msg
        assert "1.05" in msg

    def test_narrative_trend_formatted(self):
        fmt   = self._fmt()
        event = {
            "type": "NARRATIVE_TREND_DETECTED",
            "narrative": "ETF_NEWS",
            "velocity": 2.4,
            "strength": "STRONG",
            "count": 14,
            "keywords": ["etf", "blackrock", "approval"],
        }
        msg = fmt.format("NARRATIVE_TREND_DETECTED", event, "MEDIUM")
        assert msg is not None
        assert "ETF_NEWS" in msg
        assert "2.4" in msg
        assert "STRONG" in msg

    def test_low_impact_macro_returns_none(self):
        """LOW impact macro events should be silently dropped."""
        fmt   = self._fmt()
        event = {
            "type": "MACRO_NEWS_EVENT",
            "label": "Minor economic data",
            "impact": "LOW",
            "change_pct": 0.1,
        }
        msg = fmt.format("MACRO_NEWS_EVENT", event, "MEDIUM")
        # Should return None or empty after filtering
        assert not msg or "LOW" not in msg

    def test_moderate_liquidity_wall_dropped(self):
        """MODERATE walls should be silently dropped."""
        fmt   = self._fmt()
        event = {
            "type": "LIQUIDITY_WALL_DETECTED",
            "asset": "BTCUSDT",
            "side": "BID",
            "price": 64000.0,
            "size_ratio": 5.2,
            "strength": "MODERATE",
            "implication": "Support level",
        }
        result = fmt.format("LIQUIDITY_WALL_DETECTED", event, "LOW")
        # MODERATE walls are filtered — should return None or empty
        assert result is None or "MODERATE" not in (result or "")

    def test_strong_liquidity_wall_shown(self):
        """STRONG walls should be shown."""
        fmt   = self._fmt()
        event = {
            "type": "LIQUIDITY_WALL_DETECTED",
            "asset": "BTCUSDT",
            "side": "ASK",
            "price": 66000.0,
            "size_ratio": 11.3,
            "strength": "STRONG",
            "implication": "Institutional seller defending level",
        }
        msg = fmt.format("LIQUIDITY_WALL_DETECTED", event, "LOW")
        assert msg is not None
        assert "66000" in msg
        assert "STRONG" in msg

    def test_generic_fallback_handles_unknown_type(self):
        fmt   = self._fmt()
        event = {"type": "UNKNOWN_FUTURE_EVENT", "asset": "ETHUSDT"}
        msg   = fmt.format("UNKNOWN_FUTURE_EVENT", event, "LOW")
        assert msg is not None
        assert "UNKNOWN_FUTURE_EVENT" in msg

    def test_priority_header_included(self):
        fmt   = self._fmt()
        event = {
            "type": "WHALE_CLUSTER_ALERT",
            "direction": "SELL", "wallet_count": 3,
            "total_asset": 10.0, "asset": "BTC",
            "confidence": 0.7, "labels": [],
        }
        msg = fmt.format("WHALE_CLUSTER_ALERT", event, "CRITICAL")
        assert "CRITICAL" in msg

    def test_all_phase1_events_format_without_error(self):
        fmt = self._fmt()
        events = [
            ("LIQUIDATION_CASCADE_ALERT", {
                "asset": "BTCUSDT", "usd_total": 10_000_000,
                "window_s": 60, "severity": "HIGH"
            }),
            ("FUNDING_RATE_ALERT", {
                "asset": "BTCUSDT", "rate": 0.005,
                "rate_pct": 0.5, "bias": "HIGH_LONG", "implication": "Test"
            }),
            ("OI_CHANGE_ALERT", {
                "asset": "BTCUSDT", "change_pct": 6.2,
                "signal": "TREND_CONTINUATION"
            }),
            ("MACRO_NEWS_EVENT", {
                "label": "CPI", "prev": 3.1, "current": 3.4,
                "change_pct": 9.7, "impact": "HIGH"
            }),
        ]
        for channel, event in events:
            msg = fmt.format(channel, event, "HIGH")
            # Should not raise — may return None for filtered events
            assert msg is None or isinstance(msg, str)

    def test_all_phase2_events_format_without_error(self):
        fmt = self._fmt()
        events = [
            ("WHALE_ACCUMULATION", {
                "label": "Test Whale", "asset": "BTC",
                "delta": 15.0, "behavior": "accumulator"
            }),
            ("WHALE_DISTRIBUTION", {
                "label": "Test Whale", "asset": "BTC",
                "delta": -12.0, "behavior": "distributor"
            }),
            ("EXCHANGE_INFLOW_ALERT", {
                "label": "Binance", "asset": "BTC", "delta": 500.0
            }),
            ("EXCHANGE_OUTFLOW_ALERT", {
                "label": "Binance", "asset": "ETH", "delta": 2000.0
            }),
        ]
        for channel, event in events:
            msg = fmt.format(channel, event, "HIGH")
            assert isinstance(msg, str)
            assert len(msg) > 10


# ── AlertRouter tests ─────────────────────────────────────────────────────────

class TestAlertRouter:

    def test_telegram_receives_message(self):
        from services.intelligence_alerts.alert_router import AlertRouter
        router   = AlertRouter()
        telegram = _make_telegram()
        router.set_telegram(telegram)
        router._pub = _make_pub()   # prevent real Redis call

        router.route(
            channel="WHALE_CLUSTER_ALERT",
            message="🐋🐋🐋 Test whale cluster",
            event={"type": "WHALE_CLUSTER_ALERT", "asset": "BTC"},
            priority="CRITICAL",
        )
        assert len(telegram.messages) == 1
        assert "Test whale cluster" in telegram.messages[0]

    def test_redis_receives_intelligence_alert(self):
        from services.intelligence_alerts.alert_router import AlertRouter
        router = AlertRouter()
        pub    = _make_pub()
        router._pub = pub

        router.route(
            channel="STOP_HUNT_DETECTED",
            message="⚡ Stop hunt test",
            event={"type": "STOP_HUNT_DETECTED", "asset": "BTCUSDT"},
            priority="HIGH",
        )
        assert len(pub.messages) == 1
        assert pub.messages[0]["channel"] == "INTELLIGENCE_ALERT"
        data = pub.messages[0]["data"]
        assert data["priority"] == "HIGH"
        assert data["channel"]  == "STOP_HUNT_DETECTED"

    def test_no_telegram_does_not_crash(self):
        """Router without Telegram should route to Redis only — no crash."""
        from services.intelligence_alerts.alert_router import AlertRouter
        router      = AlertRouter()
        router._pub = _make_pub()
        router._telegram = None

        router.route(
            channel="OI_CHANGE_ALERT",
            message="📊 OI test",
            event={"type": "OI_CHANGE_ALERT", "asset": "BTCUSDT"},
            priority="MEDIUM",
        )   # must not raise

    def test_no_redis_does_not_crash(self):
        """Router without Redis should send Telegram only — no crash."""
        from services.intelligence_alerts.alert_router import AlertRouter
        router       = AlertRouter()
        router._pub  = None
        telegram     = _make_telegram()
        router.set_telegram(telegram)

        router.route(
            channel="NARRATIVE_TREND_DETECTED",
            message="📣 Narrative test",
            event={"type": "NARRATIVE_TREND_DETECTED"},
            priority="MEDIUM",
        )
        assert len(telegram.messages) == 1


# ── IntelligenceAlertService tests ───────────────────────────────────────────

class TestIntelligenceAlertService:

    def test_rate_check_blocks_repeated_calls(self):
        from services.intelligence_alerts.intelligence_alert_service import (
            IntelligenceAlertService
        )
        svc = IntelligenceAlertService()
        # First call should pass
        assert svc._rate_check("WHALE_ACCUMULATION", "HIGH") is True
        # Second call immediately after should be blocked
        assert svc._rate_check("WHALE_ACCUMULATION", "HIGH") is False

    def test_rate_check_allows_after_interval(self):
        from services.intelligence_alerts.intelligence_alert_service import (
            IntelligenceAlertService, PRIORITY_RATE_LIMITS
        )
        svc = IntelligenceAlertService()
        svc._rate_check("OI_CHANGE_ALERT", "MEDIUM")
        # Manually backdate the last sent time
        svc._rate_cache["OI_CHANGE_ALERT"] = (
            time.time() - PRIORITY_RATE_LIMITS["MEDIUM"] - 1
        )
        assert svc._rate_check("OI_CHANGE_ALERT", "MEDIUM") is True

    def test_critical_bypasses_rate_limit(self):
        """CRITICAL events always fire — no rate limiting."""
        from services.intelligence_alerts.intelligence_alert_service import (
            IntelligenceAlertService
        )
        svc = IntelligenceAlertService()

        dispatched = []
        def fake_dispatch(channel, event, priority):
            dispatched.append(channel)

        svc._dispatch = fake_dispatch

        # Simulate two CRITICAL events back to back
        svc._handle_event("LIQUIDATION_CASCADE_ALERT", {"type": "LIQUIDATION_CASCADE_ALERT"})
        svc._handle_event("LIQUIDATION_CASCADE_ALERT", {"type": "LIQUIDATION_CASCADE_ALERT"})

        # Both should have been dispatched (no rate limiting for CRITICAL)
        time.sleep(0.1)
        assert len(dispatched) == 2

    def test_channel_priority_mapping_complete(self):
        """All subscribed channels must have a priority."""
        from services.intelligence_alerts.intelligence_alert_service import (
            SUBSCRIBED_CHANNELS, CHANNEL_PRIORITY
        )
        for channel in SUBSCRIBED_CHANNELS:
            assert channel in CHANNEL_PRIORITY, \
                f"Channel '{channel}' missing from CHANNEL_PRIORITY"

    def test_all_priorities_have_rate_limits(self):
        from services.intelligence_alerts.intelligence_alert_service import (
            CHANNEL_PRIORITY, PRIORITY_RATE_LIMITS
        )
        priorities = set(CHANNEL_PRIORITY.values())
        for priority in priorities:
            if priority != "CRITICAL":
                assert priority in PRIORITY_RATE_LIMITS, \
                    f"Priority '{priority}' missing from PRIORITY_RATE_LIMITS"

    def test_add_custom_handler(self):
        from services.intelligence_alerts.intelligence_alert_service import (
            IntelligenceAlertService
        )
        svc      = IntelligenceAlertService()
        received = []
        svc.add_handler(lambda event, priority: received.append((event, priority)))
        assert len(svc._handlers) == 1


# ── Integration tests (require live Redis) ────────────────────────────────────

@pytest.mark.integration
class TestIntelligenceAlertsIntegration:

    def test_service_subscribes_to_redis(self):
        """Start the service and verify it subscribes without error."""
        import redis as redis_lib
        try:
            r = redis_lib.Redis(host="localhost", port=6379)
            r.ping()
        except Exception:
            pytest.skip("Redis not reachable")

        from services.intelligence_alerts import IntelligenceAlertService
        svc = IntelligenceAlertService()
        svc.start()
        time.sleep(1.0)   # give thread time to subscribe
        assert svc._running is True
        svc.stop()

    def test_published_event_reaches_telegram(self):
        """Publish a test event to Redis and verify Telegram receives it."""
        import redis as redis_lib
        try:
            r = redis_lib.Redis(host="localhost", port=6379)
            r.ping()
        except Exception:
            pytest.skip("Redis not reachable")

        from services.intelligence_alerts import IntelligenceAlertService
        telegram = _make_telegram()
        svc      = IntelligenceAlertService()
        svc.set_telegram(telegram)
        svc.start()
        time.sleep(2.5)

        # Publish a test event
        event = {
            "type":         "WHALE_CLUSTER_ALERT",
            "direction":    "BUY",
            "wallet_count": 4,
            "total_asset":  15.0,
            "asset":        "BTC",
            "confidence":   0.85,
            "labels":       ["Test A", "Test B"],
        }
        r.publish("WHALE_CLUSTER_ALERT", json.dumps(event))
        time.sleep(2.0)   # allow async dispatch

        svc.stop()
        assert len(telegram.messages) >= 1
        assert "Whale" in telegram.messages[0]
