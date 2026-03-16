"""
tests/test_whale_intelligence.py — Whale Intelligence Engine tests.

  Unit tests        — always run, no external services required.
  Integration tests — skipped automatically when Redis / DB not reachable.

Run just unit tests:
    pytest tests/test_whale_intelligence.py -v -m "not integration"

Run everything (requires live Redis + DB):
    pytest tests/test_whale_intelligence.py -v
"""
from __future__ import annotations

import time
import pytest

# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_profile(n_buys: int = 8, n_sells: int = 2,
                  wallet_type: str = "unknown",
                  last_active_days_ago: int = 0):
    from whale_intelligence.wallet_behavior_classifier import WalletProfile
    history = (
        [{"delta":  1.0, "ts": int(time.time() * 1000)} for _ in range(n_buys)] +
        [{"delta": -1.0, "ts": int(time.time() * 1000)} for _ in range(n_sells)]
    )
    last_ts = int((time.time() - last_active_days_ago * 86400) * 1000)
    return WalletProfile(
        address="0xTEST1234",
        label="Test Whale",
        wallet_type=wallet_type,
        history=history,
        last_active_ts=last_ts,
    )


# ── Classifier unit tests ─────────────────────────────────────────────────────

class TestClassifier:

    def test_accumulator_label(self):
        from whale_intelligence.wallet_behavior_classifier import WalletBehaviorClassifier
        clf     = WalletBehaviorClassifier()
        profile = _make_profile(n_buys=9, n_sells=1)
        result  = clf.classify(profile)
        assert result.behavior   == "accumulator"
        assert result.confidence >= 0.8

    def test_distributor_label(self):
        from whale_intelligence.wallet_behavior_classifier import WalletBehaviorClassifier
        clf     = WalletBehaviorClassifier()
        profile = _make_profile(n_buys=1, n_sells=9)
        result  = clf.classify(profile)
        assert result.behavior   == "distributor"
        assert result.confidence >= 0.8

    def test_exchange_override(self):
        from whale_intelligence.wallet_behavior_classifier import WalletBehaviorClassifier
        clf     = WalletBehaviorClassifier()
        profile = _make_profile(wallet_type="exchange")
        result  = clf.classify(profile)
        assert result.behavior   == "exchange"
        assert result.confidence == 0.95

    def test_dormant_label(self):
        from whale_intelligence.wallet_behavior_classifier import WalletBehaviorClassifier
        clf     = WalletBehaviorClassifier()
        profile = _make_profile(last_active_days_ago=200)
        result  = clf.classify(profile)
        assert result.behavior == "dormant"

    def test_insufficient_data(self):
        from whale_intelligence.wallet_behavior_classifier import WalletBehaviorClassifier, WalletProfile
        clf     = WalletBehaviorClassifier()
        profile = WalletProfile(address="0xNEW", history=[{"delta": 1.0, "ts": 0}])
        result  = clf.classify(profile)
        assert result.behavior   == "insufficient_data"
        assert result.confidence == 0.0

    def test_signal_weights_sum_to_reasonable_values(self):
        from whale_intelligence.wallet_behavior_classifier import WalletBehaviorClassifier
        for behavior in ["accumulator", "distributor", "exchange", "flipper",
                         "dormant", "mixed", "unknown"]:
            w = WalletBehaviorClassifier.signal_weight(behavior)
            assert 0.0 <= w <= 1.0, f"Weight out of range for {behavior}: {w}"

    def test_unknown_behavior_gets_default_weight(self):
        from whale_intelligence.wallet_behavior_classifier import WalletBehaviorClassifier
        w = WalletBehaviorClassifier.signal_weight("not_a_real_behavior")
        assert w == 0.25


# ── Cluster analyser unit tests ───────────────────────────────────────────────

class TestClusterAnalyzer:

    def _make_accumulation_event(self, address: str, label: str) -> dict:
        return {
            "type":         "WHALE_ACCUMULATION",
            "address":      address[:16] + "...",
            "full_address": address,
            "label":        label,
            "asset":        "BTC",
            "delta":        15.0,
            "behavior":     "accumulator",
            "ts":           int(time.time() * 1000),
        }

    def test_no_alert_below_threshold(self):
        """Two wallets buying should NOT trigger a cluster alert."""
        from whale_intelligence.wallet_cluster_analyzer import WalletClusterAnalyzer
        alerts   = []
        analyzer = WalletClusterAnalyzer()

        # Patch publish to capture alerts instead of sending to Redis
        class FakePub:
            def publish(self, channel, data):
                alerts.append(data)
            def ping(self): pass
        analyzer._pub = FakePub()

        analyzer.ingest(self._make_accumulation_event("0xAAA", "Whale A"))
        analyzer.ingest(self._make_accumulation_event("0xBBB", "Whale B"))
        assert len(alerts) == 0

    def test_cluster_fires_at_threshold(self):
        """Three wallets buying SHOULD trigger a cluster alert."""
        from whale_intelligence.wallet_cluster_analyzer import WalletClusterAnalyzer
        alerts   = []
        analyzer = WalletClusterAnalyzer()

        class FakePub:
            def publish(self, channel, data):
                alerts.append(data)
            def ping(self): pass
        analyzer._pub = FakePub()

        analyzer.ingest(self._make_accumulation_event("0xAAA", "Whale A"))
        analyzer.ingest(self._make_accumulation_event("0xBBB", "Whale B"))
        analyzer.ingest(self._make_accumulation_event("0xCCC", "Whale C"))
        assert len(alerts) == 1

        import json
        event = json.loads(alerts[0])
        assert event["type"]         == "WHALE_CLUSTER_ALERT"
        assert event["direction"]    == "BUY"
        assert event["wallet_count"] == 3

    def test_duplicate_address_does_not_inflate_cluster(self):
        """Same wallet sending 3 events should NOT be a cluster."""
        from whale_intelligence.wallet_cluster_analyzer import WalletClusterAnalyzer
        alerts   = []
        analyzer = WalletClusterAnalyzer()

        class FakePub:
            def publish(self, ch, data): alerts.append(data)
            def ping(self): pass
        analyzer._pub = FakePub()

        for _ in range(5):
            analyzer.ingest(self._make_accumulation_event("0xSAME", "Same Whale"))
        assert len(alerts) == 0

    def test_direction_mapping_sell(self):
        from whale_intelligence.wallet_cluster_analyzer import WalletClusterAnalyzer
        ev = {"type": "WHALE_DISTRIBUTION", "delta": -10.0}
        assert WalletClusterAnalyzer._event_to_direction(ev) == "SELL"

    def test_direction_mapping_buy(self):
        from whale_intelligence.wallet_cluster_analyzer import WalletClusterAnalyzer
        ev = {"type": "WHALE_ACCUMULATION", "delta": 10.0}
        assert WalletClusterAnalyzer._event_to_direction(ev) == "BUY"


# ── Database unit tests ───────────────────────────────────────────────────────

class TestWalletDatabase:

    def test_fallback_storage_without_db(self):
        """DB unavailable — should still store/retrieve in memory."""
        from whale_intelligence.wallet_database import WalletDatabase
        db = WalletDatabase()
        # Do NOT call db.init() — simulates DB being unavailable

        db.update_balance("0xTEST", 5.0)
        assert db.get_balance("0xTEST") == 5.0

    def test_upsert_and_load_wallets_fallback(self):
        from whale_intelligence.wallet_database import WalletDatabase
        db = WalletDatabase()
        db.upsert_wallet({"address": "0xABC", "label": "Test", "chain": "eth", "type": "unknown"})
        wallets = db.load_all_wallets()
        addresses = [w["address"] for w in wallets]
        assert "0xABC" in addresses

    def test_profile_roundtrip_fallback(self):
        from whale_intelligence.wallet_database import WalletDatabase
        from whale_intelligence.wallet_behavior_classifier import WalletProfile
        db      = WalletDatabase()
        profile = WalletProfile(
            address="0xPROFILE", behavior="accumulator",
            confidence=0.9, last_active_ts=int(time.time() * 1000)
        )
        db.update_profile(profile)
        loaded = db.get_profile("0xPROFILE")
        assert loaded is not None
        assert loaded.behavior   == "accumulator"
        assert loaded.confidence == 0.9


# ── Integration tests (require live Redis) ────────────────────────────────────

@pytest.mark.integration
class TestWalletTrackerIntegration:

    def test_redis_publishes_whale_event(self):
        """Verify a synthetic movement publishes to Redis correctly."""
        import redis as redis_lib
        try:
            r = redis_lib.Redis(host="localhost", port=6379)
            r.ping()
        except Exception:
            pytest.skip("Redis not reachable")

        ps = r.pubsub()
        ps.subscribe("WHALE_ACCUMULATION")

        from whale_intelligence.wallet_tracker import WalletTracker
        from whale_intelligence.wallet_database import WalletDatabase
        from whale_intelligence.wallet_behavior_classifier import WalletBehaviorClassifier
        from whale_intelligence.wallet_cluster_analyzer import WalletClusterAnalyzer

        db         = WalletDatabase()
        classifier = WalletBehaviorClassifier()
        cluster    = WalletClusterAnalyzer()
        tracker    = WalletTracker(db=db, classifier=classifier, cluster=cluster)
        tracker._init_redis()

        # Directly invoke the publish path with a synthetic movement
        wallet = {"address": "0xINTEGRATION", "label": "Integration Test",
                  "chain": "eth", "type": "unknown"}
        tracker._balances["0xINTEGRATION"] = 100.0
        tracker._publish_movement(wallet, delta=50.0, asset="ETH", new_balance=150.0)

        # Give Redis a moment to deliver
        time.sleep(0.2)
        msg = ps.get_message(timeout=1.0)
        # Skip the subscribe confirmation
        if msg and msg["type"] == "subscribe":
            msg = ps.get_message(timeout=1.0)

        assert msg is not None, "No message received from Redis"
        assert msg["type"] == "message"

        import json
        data = json.loads(msg["data"])
        assert data["type"]  == "WHALE_ACCUMULATION"
        assert data["delta"] == 50.0
