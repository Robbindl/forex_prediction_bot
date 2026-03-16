"""
tests/test_narrative_ai.py — Market Narrative AI Engine tests.

  Unit tests        — always run, no external services required.
  Integration tests — skipped automatically when Redis is not reachable.

Run just unit tests:
    pytest tests/test_narrative_ai.py -v -m "not integration"

Run everything (requires live Redis):
    pytest tests/test_narrative_ai.py -v
"""
from __future__ import annotations

import json
import time
import pytest


# ── TopicClusterEngine tests ──────────────────────────────────────────────────

class TestTopicClusterEngine:

    def test_ingest_returns_matched_narratives(self):
        from narrative_ai.topic_cluster_engine import TopicClusterEngine
        engine = TopicClusterEngine()
        engine._pub = None

        matched = engine.ingest("Bitcoin ETF approval by SEC is imminent")
        assert "ETF_NEWS" in matched
        assert "REGULATION" in matched

    def test_ingest_returns_empty_for_unrelated_text(self):
        from narrative_ai.topic_cluster_engine import TopicClusterEngine
        engine = TopicClusterEngine()
        engine._pub = None

        matched = engine.ingest("The stock market closed up slightly today")
        assert matched == []

    def test_counts_accumulate(self):
        from narrative_ai.topic_cluster_engine import TopicClusterEngine
        engine = TopicClusterEngine()
        engine._pub = None

        engine.ingest("Bitcoin halving is coming soon")
        engine.ingest("Halving cycle could drive huge gains")
        engine.ingest("Block reward halving imminent")

        counts = engine.get_counts()
        assert counts.get("HALVING_BUZZ", 0) == 3

    def test_get_dominant_narrative(self):
        from narrative_ai.topic_cluster_engine import TopicClusterEngine
        engine = TopicClusterEngine()
        engine._pub = None

        for _ in range(5):
            engine.ingest("DeFi yield farming TVL protocol")
        engine.ingest("Bitcoin ETF approved")

        dominant = engine.get_dominant_narrative()
        assert dominant == "DEFI_TREND"

    def test_narrative_scores_sum_to_reasonable_value(self):
        from narrative_ai.topic_cluster_engine import TopicClusterEngine
        engine = TopicClusterEngine()
        engine._pub = None

        engine.ingest("Bitcoin price surge rally ETF")
        engine.ingest("SEC regulation crackdown")

        scores = engine.get_narrative_scores()
        assert all(0.0 <= v <= 1.0 for v in scores.values())

    def test_velocity_alert_fires_on_spike(self):
        """
        Ingest SNAPSHOT_INTERVAL texts, half with a narrative match.
        Then ingest another SNAPSHOT_INTERVAL texts, all matching.
        The second snapshot should detect a velocity spike.
        """
        from narrative_ai.topic_cluster_engine import (
            TopicClusterEngine, SNAPSHOT_INTERVAL
        )
        alerts = []

        class FakePub:
            def publish(self, ch, data):
                alerts.append(json.loads(data))
            def ping(self): pass

        engine = TopicClusterEngine()
        engine._pub = FakePub()
        engine._cooldown = {}

        # First window — moderate halving mentions (prev baseline)
        for i in range(SNAPSHOT_INTERVAL):
            if i % 2 == 0:
                engine.ingest("Bitcoin halving block reward")
            else:
                engine.ingest("General market news today")

        # Second window — ALL are halving mentions (velocity spike)
        for _ in range(SNAPSHOT_INTERVAL):
            engine.ingest("Bitcoin halving imminent block reward halving cycle")

        narrative_alerts = [
            a for a in alerts
            if a.get("type") == "NARRATIVE_TREND_DETECTED"
            and a.get("narrative") == "HALVING_BUZZ"
        ]
        assert len(narrative_alerts) >= 1
        assert narrative_alerts[0]["velocity"] >= 0.40

    def test_alert_cooldown_prevents_spam(self):
        """Two velocity spikes in a row should only fire a small number of alerts."""
        from narrative_ai.topic_cluster_engine import (
            TopicClusterEngine, SNAPSHOT_INTERVAL
        )
        alerts = []

        class FakePub:
            def publish(self, ch, data):
                alerts.append(json.loads(data))
            def ping(self): pass

        engine = TopicClusterEngine()
        engine._pub = FakePub()

        for _ in range(SNAPSHOT_INTERVAL * 3):
            engine.ingest("Ethereum layer2 rollup arbitrum scaling")

        narrative_alerts = [
            a for a in alerts
            if a.get("type") == "NARRATIVE_TREND_DETECTED"
        ]
        assert len(narrative_alerts) <= 4

    def test_thread_safety(self):
        """Concurrent ingest() calls from multiple threads must not crash."""
        import threading
        from narrative_ai.topic_cluster_engine import TopicClusterEngine
        engine = TopicClusterEngine()
        engine._pub = None

        errors = []

        def worker():
            try:
                for _ in range(50):
                    engine.ingest("Bitcoin ETF halving defi regulation")
            except Exception as e:
                errors.append(str(e))

        threads = [threading.Thread(target=worker) for _ in range(5)]
        for t in threads: t.start()
        for t in threads: t.join()

        assert errors == [], f"Thread safety errors: {errors}"


# ── Integration test (requires live Redis) ────────────────────────────────────

@pytest.mark.integration
class TestNarrativeAIIntegration:

    def test_narrative_event_published_to_redis(self):
        """Ingesting a velocity spike should publish to Redis channel."""
        import redis as redis_lib
        try:
            r = redis_lib.Redis(host="localhost", port=6379)
            r.ping()
        except Exception:
            pytest.skip("Redis not reachable")

        ps = r.pubsub()
        ps.subscribe("NARRATIVE_TREND_DETECTED")

        from narrative_ai.topic_cluster_engine import (
            TopicClusterEngine, SNAPSHOT_INTERVAL
        )
        engine = TopicClusterEngine()
        engine._cooldown = {}

        for i in range(SNAPSHOT_INTERVAL):
            if i % 3 == 0:
                engine.ingest("Bitcoin ETF SEC approval BlackRock")
            else:
                engine.ingest("general market update")

        for _ in range(SNAPSHOT_INTERVAL):
            engine.ingest("Bitcoin ETF SEC BlackRock approval institutional")

        time.sleep(0.3)
        msg = ps.get_message(timeout=2.0)
        if msg and msg["type"] == "subscribe":
            msg = ps.get_message(timeout=2.0)

        assert msg is not None, "No NARRATIVE_TREND_DETECTED received from Redis"
        data = json.loads(msg["data"])
        assert data["type"]      == "NARRATIVE_TREND_DETECTED"
        assert data["narrative"] == "ETF_NEWS"
        assert data["velocity"]  >= 0.40