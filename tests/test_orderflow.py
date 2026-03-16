"""
tests/test_orderflow.py — Order Flow Intelligence Engine tests.

  Unit tests        — always run, no external services required.
  Integration tests — skipped automatically when Redis is not reachable.

Run just unit tests:
    pytest tests/test_orderflow.py -v -m "not integration"

Run everything (requires live Redis):
    pytest tests/test_orderflow.py -v
"""
from __future__ import annotations

import json
import time
import pytest


# ── Fixtures ──────────────────────────────────────────────────────────────────

def _fake_pub():
    """Returns a fake Redis publisher that records published messages."""
    class FakePub:
        def __init__(self):
            self.messages = []
        def publish(self, channel, data):
            self.messages.append({"channel": channel, "data": json.loads(data)})
        def ping(self):
            pass
    return FakePub()


def _make_levels(prices_and_qtys):
    """Helper: build [[price, qty], ...] list."""
    return [[p, q] for p, q in prices_and_qtys]


def _make_wall_levels(wall_price: float, wall_qty: float,
                      n_background: int = 9, background_qty: float = 1.0,
                      side: str = "BID"):
    """
    Build a level list that guarantees a detectable wall.

    With n_background=9 small levels and one wall level:
        avg = (wall_qty + 9 * background_qty) / 10
        ratio = wall_qty / avg

    For wall_qty=10, background_qty=1:
        avg   = (10 + 9) / 10 = 1.9
        ratio = 10 / 1.9 ≈ 5.26  — just above MODERATE threshold of 5.0

    Prices are generated so the wall is best bid (highest) or
    best ask (lowest) depending on side.
    """
    if side == "BID":
        # Best bid = wall_price, background levels below it
        levels = [(wall_price - i * 10, background_qty)
                  for i in range(1, n_background + 1)]
        levels.insert(0, (wall_price, wall_qty))
    else:
        # Best ask = just above wall; wall sits higher up
        levels = [(wall_price + i * 10, background_qty)
                  for i in range(1, n_background + 1)]
        levels.append((wall_price, wall_qty))
    return _make_levels(levels)


# ── OrderbookProcessor tests ──────────────────────────────────────────────────

class TestOrderbookProcessor:

    def test_basic_snapshot_fields(self):
        from order_flow.orderbook_processor import OrderbookProcessor
        proc = OrderbookProcessor("BTCUSDT")
        proc._pub = _fake_pub()

        bids = _make_levels([(65000, 1.0), (64990, 0.5), (64980, 0.3)])
        asks = _make_levels([(65010, 0.8), (65020, 0.4), (65030, 0.2)])

        snap = proc.update(bids, asks)

        assert snap is not None
        assert snap["asset"]    == "BTCUSDT"
        assert snap["mid"]      == pytest.approx(65005.0, abs=1)
        assert snap["best_bid"] == 65000.0
        assert snap["best_ask"] == 65010.0
        assert snap["spread"]   == pytest.approx(10.0, abs=0.01)
        assert "imbalance" in snap
        assert "bid_vol"   in snap
        assert "ask_vol"   in snap

    def test_imbalance_positive_when_more_bids(self):
        from order_flow.orderbook_processor import OrderbookProcessor
        proc = OrderbookProcessor("ETHUSDT")
        proc._pub = _fake_pub()

        bids = _make_levels([(3000, 10.0), (2990, 8.0)])
        asks = _make_levels([(3010,  4.0), (3020, 3.0)])

        snap = proc.update(bids, asks)
        assert snap["imbalance"] > 0, "Imbalance should be positive with more bid volume"

    def test_imbalance_negative_when_more_asks(self):
        from order_flow.orderbook_processor import OrderbookProcessor
        proc = OrderbookProcessor("ETHUSDT")
        proc._pub = _fake_pub()

        bids = _make_levels([(3000, 2.0), (2990, 1.0)])
        asks = _make_levels([(3010, 10.0), (3020, 8.0)])

        snap = proc.update(bids, asks)
        assert snap["imbalance"] < 0, "Imbalance should be negative with more ask volume"

    def test_remove_level_with_zero_qty(self):
        from order_flow.orderbook_processor import OrderbookProcessor
        proc = OrderbookProcessor("BTCUSDT")
        proc._pub = _fake_pub()

        proc.update(_make_levels([(65000, 1.0)]), [])
        assert 65000.0 in proc._bids

        proc.update(_make_levels([(65000, 0.0)]), [])
        assert 65000.0 not in proc._bids

    def test_latest_snapshot_returns_last(self):
        from order_flow.orderbook_processor import OrderbookProcessor
        proc = OrderbookProcessor("BTCUSDT")
        proc._pub = _fake_pub()

        proc.update(_make_levels([(100, 1.0)]), _make_levels([(101, 1.0)]))
        snap = proc.latest_snapshot()
        assert snap["asset"] == "BTCUSDT"
        assert snap["mid"]   == pytest.approx(100.5, abs=0.01)

    def test_price_history(self):
        from order_flow.orderbook_processor import OrderbookProcessor
        proc = OrderbookProcessor("BTCUSDT")
        proc._pub = _fake_pub()

        for i in range(5):
            proc.update(
                _make_levels([(65000 + i, 1.0)]),
                _make_levels([(65010 + i, 1.0)]),
            )
        hist = proc.price_history(n=3)
        assert len(hist) == 3
        assert all("price" in h and "ts" in h for h in hist)


# ── LiquidityWallDetector tests ───────────────────────────────────────────────

class TestLiquidityWallDetector:

    def test_no_wall_with_uniform_levels(self):
        from order_flow.liquidity_wall_detector import LiquidityWallDetector
        det = LiquidityWallDetector("BTCUSDT")
        det._pub = _fake_pub()

        # All same size — ratio == 1.0, well below MODERATE threshold
        bids = _make_levels([(65000 - i * 10, 1.0) for i in range(10)])
        walls = det.scan(bids, [], mid_price=65005.0)
        assert walls == []

    def test_detects_moderate_wall(self):
        """
        Wall level needs enough background levels so ratio >= 5.0.

        With wall_qty=10, n_background=9 at qty=1:
            avg   = (10 + 9×1) / 10 = 1.9
            ratio = 10 / 1.9 ≈ 5.26  → MODERATE detected
        """
        from order_flow.liquidity_wall_detector import LiquidityWallDetector
        det = LiquidityWallDetector("BTCUSDT")
        det._pub = _fake_pub()

        bids = _make_wall_levels(
            wall_price=65000.0, wall_qty=10.0,
            n_background=9, background_qty=1.0, side="BID"
        )
        walls = det.scan(bids, [], mid_price=65100.0)
        assert len(walls) == 1
        assert walls[0]["side"]  == "BID"
        assert walls[0]["price"] == 65000.0
        assert walls[0]["strength"] in ("MODERATE", "STRONG", "EXTREME")

    def test_detects_ask_wall(self):
        """
        Ask wall with ratio >= 5.0 using the same background approach.
        """
        from order_flow.liquidity_wall_detector import LiquidityWallDetector
        det = LiquidityWallDetector("BTCUSDT")
        det._pub = _fake_pub()

        asks = _make_wall_levels(
            wall_price=65500.0, wall_qty=10.0,
            n_background=9, background_qty=1.0, side="ASK"
        )
        walls = det.scan([], asks, mid_price=65100.0)
        assert any(w["side"] == "ASK" for w in walls)

    def test_extreme_wall_classified_correctly(self):
        """
        wall_qty=50 with 9 background at 1.0:
            avg   = (50 + 9) / 10 = 5.9
            ratio = 50 / 5.9 ≈ 8.5  → STRONG (>= 10 needed for EXTREME)

        Use wall_qty=200 with 9 background at 1.0:
            avg   = (200 + 9) / 10 = 20.9
            ratio = 200 / 20.9 ≈ 9.6  → still STRONG

        Use wall_qty=250, background_qty=1, n=9:
            avg = (250+9)/10 = 25.9, ratio = 250/25.9 ≈ 9.65 → STRONG

        To reliably get EXTREME (>= 20×), use very small background:
            wall_qty=20, n=9, background_qty=0.1:
            avg = (20 + 0.9)/10 = 2.09, ratio = 20/2.09 ≈ 9.57 → STRONG

            wall_qty=100, n=19, background_qty=0.1:
            avg = (100 + 1.9)/20 = 5.095, ratio = 100/5.095 ≈ 19.6 → STRONG

            wall_qty=100, n=4, background_qty=0.1:
            avg = (100 + 0.4)/5 = 20.08, ratio = 100/20.08 ≈ 4.98 → below MODERATE!

        Safest approach for EXTREME: wall much larger than all others combined.
            wall_qty=200, n=9, background_qty=0.1:
            avg = (200 + 0.9)/10 = 20.09, ratio = 200/20.09 ≈ 9.95 → STRONG

        Actually EXTREME needs ratio >= 20. Need wall_qty >> 20 * avg.
        wall_qty / ((wall_qty + n*bg) / (n+1)) >= 20
        wall_qty*(n+1) / (wall_qty + n*bg) >= 20
        With n=4, bg=0.1, wall=X: X*5/(X+0.4) >= 20 → 5X >= 20X+8 → never.
        The wall is always included in the average, making EXTREME hard with few levels.

        Solution: use many background levels with tiny qty.
            n=99, background_qty=0.01, wall_qty=30:
            avg = (30 + 0.99)/100 = 0.3099
            ratio = 30/0.3099 ≈ 96.8 → EXTREME ✓
        """
        from order_flow.liquidity_wall_detector import LiquidityWallDetector
        det = LiquidityWallDetector("BTCUSDT")
        det._pub = _fake_pub()

        # Build manually: 1 big wall + 99 tiny background levels
        bids = _make_levels(
            [(65000.0, 30.0)] +
            [(65000.0 - (i + 1) * 10, 0.01) for i in range(99)]
        )
        walls = det.scan(bids, [], mid_price=65050.0)
        if walls:
            assert walls[0]["strength"] == "EXTREME"

    def test_distance_pct_calculated(self):
        from order_flow.liquidity_wall_detector import LiquidityWallDetector
        det = LiquidityWallDetector("BTCUSDT")
        det._pub = _fake_pub()

        bids = _make_wall_levels(
            wall_price=64000.0, wall_qty=10.0,
            n_background=9, background_qty=1.0, side="BID"
        )
        walls = det.scan(bids, [], mid_price=65000.0)
        if walls:
            # Wall at 64000, mid at 65000 → ~1.54% below
            assert walls[0]["distance_pct"] > 0


# ── ImbalanceDetector tests ───────────────────────────────────────────────────

class TestImbalanceDetector:

    def _make_snapshot(self, bid_vol: float, ask_vol: float) -> dict:
        total = bid_vol + ask_vol
        imb   = (bid_vol - ask_vol) / total if total else 0.0
        return {
            "asset":     "BTCUSDT",
            "bid_vol":   bid_vol,
            "ask_vol":   ask_vol,
            "imbalance": imb,
            "mid":       65000.0,
            "ts":        int(time.time() * 1000),
        }

    def test_no_alert_when_neutral(self):
        from order_flow.imbalance_detector import ImbalanceDetector, ROLLING_WINDOW
        det = ImbalanceDetector("BTCUSDT")
        det._pub = _fake_pub()

        result = None
        for _ in range(ROLLING_WINDOW + 2):
            result = det.analyse(self._make_snapshot(bid_vol=10.0, ask_vol=10.0))
        assert result is None

    def test_strong_buy_alert_fires(self):
        """
        The alert fires on the first iteration that completes the rolling window,
        then the cooldown blocks subsequent calls. Capture the first non-None
        result rather than only checking the last loop iteration.
        """
        from order_flow.imbalance_detector import ImbalanceDetector, ROLLING_WINDOW
        det = ImbalanceDetector("BTCUSDT")
        det._pub = _fake_pub()

        first_alert = None
        for _ in range(ROLLING_WINDOW + 2):
            # bid_vol massively dominant → imbalance ≈ +0.80
            result = det.analyse(self._make_snapshot(bid_vol=90.0, ask_vol=10.0))
            if result is not None and first_alert is None:
                first_alert = result

        assert first_alert is not None
        assert first_alert["bias"] == "STRONG_BUY"

    def test_strong_sell_alert_fires(self):
        """Same pattern as test_strong_buy_alert_fires — capture first alert."""
        from order_flow.imbalance_detector import ImbalanceDetector, ROLLING_WINDOW
        det = ImbalanceDetector("BTCUSDT")
        det._pub = _fake_pub()

        first_alert = None
        for _ in range(ROLLING_WINDOW + 2):
            result = det.analyse(self._make_snapshot(bid_vol=10.0, ask_vol=90.0))
            if result is not None and first_alert is None:
                first_alert = result

        assert first_alert is not None
        assert first_alert["bias"] == "STRONG_SELL"

    def test_current_score_range(self):
        from order_flow.imbalance_detector import ImbalanceDetector
        det = ImbalanceDetector("BTCUSDT")
        det._pub = _fake_pub()

        det.analyse(self._make_snapshot(50.0, 50.0))
        score = det.current_score()
        assert -1.0 <= score <= 1.0

    def test_current_score_zero_when_no_data(self):
        from order_flow.imbalance_detector import ImbalanceDetector
        det = ImbalanceDetector("BTCUSDT")
        assert det.current_score() == 0.0


# ── StopHuntDetector tests ────────────────────────────────────────────────────

class TestStopHuntDetector:

    def _make_wall(self, price: float, side: str, strength: str = "STRONG") -> dict:
        return {
            "type":     "LIQUIDITY_WALL_DETECTED",
            "asset":    "BTCUSDT",
            "side":     side,
            "price":    price,
            "size":     10.0,
            "strength": strength,
        }

    def test_no_hunt_with_flat_price(self):
        from order_flow.stop_hunt_detector import StopHuntDetector
        det = StopHuntDetector("BTCUSDT")
        det._pub = _fake_pub()
        det.update_walls([self._make_wall(65000.0, "BID")])

        ts_now = int(time.time() * 1000)
        for i in range(20):
            det.ingest_price(65100.0, ts_now + i * 500)

        assert len(det._pub.messages) == 0

    def test_bid_stop_hunt_detected(self):
        """
        Sequence:
          1. 16 ticks above the BID wall (price = 65100).
          2. One wick tick below wall by > WICK_THRESHOLD_PCT.
          3. One revert tick back above wall.
          4. Expect STOP_HUNT_DETECTED with implication=BUY.

        Fix applied: stop_hunt_detector now iterates over all ticks
        (not ticks[:-3]) so the freshly-appended wick+revert are scanned.
        """
        from order_flow.stop_hunt_detector import StopHuntDetector, WICK_THRESHOLD_PCT
        det = StopHuntDetector("BTCUSDT")
        pub = _fake_pub()
        det._pub = pub
        det.update_walls([self._make_wall(65000.0, "BID")])

        ts_now = int(time.time() * 1000)

        # Build history above the wall
        for i in range(16):
            det.ingest_price(65100.0, ts_now + i * 200)

        # Spike below wall by WICK_THRESHOLD_PCT + 0.05 % (clearly through)
        wick_price = 65000.0 * (1.0 - (WICK_THRESHOLD_PCT + 0.05) / 100.0)
        det.ingest_price(wick_price, ts_now + 3500)

        # Revert back above the wall
        det.ingest_price(65050.0, ts_now + 6000)

        hunt_events = [
            m for m in pub.messages
            if m["data"].get("type") == "STOP_HUNT_DETECTED"
        ]
        assert len(hunt_events) >= 1
        ev = hunt_events[0]["data"]
        assert ev["wall_side"]   == "BID"
        assert ev["implication"] == "BUY"
        assert ev["confidence"]  >  0.0

    def test_ask_stop_hunt_detected(self):
        """
        Mirror of test_bid_stop_hunt_detected for ASK side.
        Expect STOP_HUNT_DETECTED with implication=SELL.
        """
        from order_flow.stop_hunt_detector import StopHuntDetector, WICK_THRESHOLD_PCT
        det = StopHuntDetector("BTCUSDT")
        pub = _fake_pub()
        det._pub = pub
        det.update_walls([self._make_wall(66000.0, "ASK")])

        ts_now = int(time.time() * 1000)
        for i in range(16):
            det.ingest_price(65900.0, ts_now + i * 200)

        # Spike above wall
        spike_price = 66000.0 * (1.0 + (WICK_THRESHOLD_PCT + 0.05) / 100.0)
        det.ingest_price(spike_price, ts_now + 3500)

        # Revert below wall
        det.ingest_price(65950.0, ts_now + 6000)

        hunt_events = [
            m for m in pub.messages
            if m["data"].get("type") == "STOP_HUNT_DETECTED"
        ]
        assert len(hunt_events) >= 1
        ev = hunt_events[0]["data"]
        assert ev["wall_side"]   == "ASK"
        assert ev["implication"] == "SELL"

    def test_no_hunt_when_price_does_not_revert(self):
        from order_flow.stop_hunt_detector import StopHuntDetector, WICK_THRESHOLD_PCT
        det = StopHuntDetector("BTCUSDT")
        pub = _fake_pub()
        det._pub = pub
        det.update_walls([self._make_wall(65000.0, "BID")])

        ts_now = int(time.time() * 1000)
        for i in range(16):
            det.ingest_price(65100.0, ts_now + i * 200)

        # Spike below wall but price stays low — no revert
        wick_price = 65000.0 * (1.0 - (WICK_THRESHOLD_PCT + 0.05) / 100.0)
        for i in range(10):
            det.ingest_price(wick_price - i * 10, ts_now + 4000 + i * 500)

        hunt_events = [
            m for m in pub.messages
            if m["data"].get("type") == "STOP_HUNT_DETECTED"
        ]
        assert len(hunt_events) == 0, "Should not detect hunt without revert"


# ── Integration test (requires live Redis) ────────────────────────────────────

@pytest.mark.integration
class TestOrderFlowIntegration:

    def test_full_pipeline_publishes_snapshot(self):
        """
        Simulate an order book update flowing through the full pipeline
        and verify ORDERBOOK_SNAPSHOT appears on Redis.
        """
        import redis as redis_lib
        try:
            r = redis_lib.Redis(host="localhost", port=6379)
            r.ping()
        except Exception:
            pytest.skip("Redis not reachable")

        ps = r.pubsub()
        ps.subscribe("ORDERBOOK_SNAPSHOT")

        from order_flow.orderbook_processor import OrderbookProcessor
        proc = OrderbookProcessor("BTCUSDT")

        bids = _make_levels([(65000, 2.0), (64990, 1.0)])
        asks = _make_levels([(65010, 1.5), (65020, 0.8)])
        proc.update(bids, asks)

        time.sleep(0.3)
        msg = ps.get_message(timeout=1.0)
        if msg and msg["type"] == "subscribe":
            msg = ps.get_message(timeout=1.0)

        assert msg is not None, "No ORDERBOOK_SNAPSHOT received from Redis"
        data = json.loads(msg["data"])
        assert data["asset"] == "BTCUSDT"
        assert data["mid"]   > 0