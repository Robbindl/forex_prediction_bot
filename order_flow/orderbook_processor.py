"""
order_flow/orderbook_processor.py — Live order book state manager.

Maintains a real-time level-2 order book for a single asset by
applying incremental delta updates received from the exchange stream.
Calculates derived metrics (mid price, spread, imbalance, depth) and
publishes a normalised ORDERBOOK_SNAPSHOT to Redis on every update.

Metrics calculated
------------------
    mid          — (best_bid + best_ask) / 2
    spread       — best_ask - best_bid  (absolute)
    spread_pct   — spread / mid * 100
    bid_vol      — total qty across top-N bid levels
    ask_vol      — total qty across top-N ask levels
    imbalance    — (bid_vol - ask_vol) / (bid_vol + ask_vol)  — range -1 … +1
                   positive = more buying pressure, negative = more selling pressure
    top_bids     — [[price, qty], …] sorted descending (best first)
    top_asks     — [[price, qty], …] sorted ascending  (best first)

Redis events published
----------------------
    ORDERBOOK_SNAPSHOT  {asset, mid, spread, spread_pct, bid_vol, ask_vol,
                         imbalance, top_bids, top_asks, ts}

Run tests
---------
    pytest tests/test_orderflow.py::TestOrderbookProcessor -v
"""
from __future__ import annotations

import json
import threading
import time
from collections import deque
from typing import Deque, Dict, List, Optional, Tuple

from utils.logger import get_logger

logger = get_logger()

# ── Configuration ─────────────────────────────────────────────────────────────
BOOK_DEPTH      = 20    # levels to maintain on each side
SNAPSHOT_DEPTH  = 5     # levels to include in published snapshot
MAX_SNAPSHOTS   = 200   # rolling history for stop-hunt detector


class OrderbookProcessor:
    """
    Incremental order book for one asset.
    Thread-safe — update() may be called from any thread.
    """

    def __init__(self, asset: str) -> None:
        self.asset      = asset
        self._bids:     Dict[float, float] = {}   # price → qty
        self._asks:     Dict[float, float] = {}
        self._lock      = threading.Lock()
        self._history:  Deque[dict] = deque(maxlen=MAX_SNAPSHOTS)
        self._pub                   = None
        self._init_redis()

    # ── Public API ────────────────────────────────────────────────────────────

    def update(
        self,
        bids: List,
        asks: List,
    ) -> Optional[dict]:
        """
        Apply delta update from ORDER_BOOK_UPDATE event.
        bids / asks are lists of [price, qty] pairs.
        qty == 0 means remove that price level.
        Returns the computed snapshot dict.
        """
        with self._lock:
            self._apply_delta(self._bids, bids)
            self._apply_delta(self._asks, asks)
            snapshot = self._build_snapshot()
            self._history.append(snapshot)

        self._publish(snapshot)
        return snapshot

    def latest_snapshot(self) -> dict:
        """Return the most recently computed snapshot (for dashboard polling)."""
        with self._lock:
            return dict(self._history[-1]) if self._history else {}

    def price_history(self, n: int = 50) -> List[dict]:
        """Return last-n mid-price snapshots (used by StopHuntDetector)."""
        with self._lock:
            return [
                {"price": s["mid"], "ts": s["ts"]}
                for s in list(self._history)[-n:]
                if s.get("mid", 0) > 0
            ]

    # ── Internal ──────────────────────────────────────────────────────────────

    def _init_redis(self) -> None:
        try:
            import redis
            from config.config import REDIS_URL
            self._pub = redis.from_url(REDIS_URL)
            self._pub.ping()
        except Exception as e:
            logger.debug(f"[OrderbookProc] Redis unavailable for {self.asset}: {e}")

    @staticmethod
    def _apply_delta(book: Dict[float, float], levels: List) -> None:
        """Mutate book dict in-place with the incoming delta levels."""
        for level in levels:
            try:
                price = float(level[0])
                qty   = float(level[1])
                if qty == 0.0:
                    book.pop(price, None)
                else:
                    book[price] = qty
            except (IndexError, ValueError, TypeError):
                continue

    def _build_snapshot(self) -> dict:
        """Compute all metrics from current book state (called under lock)."""
        top_bids: List[Tuple[float, float]] = sorted(
            self._bids.items(), reverse=True
        )[:BOOK_DEPTH]
        top_asks: List[Tuple[float, float]] = sorted(
            self._asks.items()
        )[:BOOK_DEPTH]

        bid_vol = sum(q for _, q in top_bids)
        ask_vol = sum(q for _, q in top_asks)

        best_bid = top_bids[0][0] if top_bids else 0.0
        best_ask = top_asks[0][0] if top_asks else 0.0
        mid      = (best_bid + best_ask) / 2.0 if best_bid and best_ask else 0.0
        spread   = best_ask - best_bid         if best_bid and best_ask else 0.0

        imbalance = 0.0
        total_vol = bid_vol + ask_vol
        if total_vol > 0:
            imbalance = (bid_vol - ask_vol) / total_vol

        return {
            "asset":      self.asset,
            "mid":        round(mid, 8),
            "best_bid":   round(best_bid, 8),
            "best_ask":   round(best_ask, 8),
            "spread":     round(spread, 8),
            "spread_pct": round(spread / mid * 100, 6) if mid else 0.0,
            "bid_vol":    round(bid_vol, 4),
            "ask_vol":    round(ask_vol, 4),
            "imbalance":  round(imbalance, 4),
            "top_bids":   [[round(p, 8), round(q, 4)] for p, q in top_bids[:SNAPSHOT_DEPTH]],
            "top_asks":   [[round(p, 8), round(q, 4)] for p, q in top_asks[:SNAPSHOT_DEPTH]],
            "ts":         int(time.time() * 1000),
        }

    def _publish(self, snapshot: dict) -> None:
        if not self._pub:
            return
        try:
            self._pub.publish("ORDERBOOK_SNAPSHOT", json.dumps(snapshot))
        except Exception as e:
            logger.debug(f"[OrderbookProc] Redis publish {self.asset}: {e}")
            self._pub = None   # stop hammering dead Redis
