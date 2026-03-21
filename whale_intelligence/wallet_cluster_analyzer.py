from __future__ import annotations

import json
import threading
import time
from typing import Dict, List, Optional, Set

from utils.logger import get_logger

logger = get_logger()

# ── Configuration ─────────────────────────────────────────────────────────────
CLUSTER_WINDOW_SECS  = 300     # 5-minute rolling window
MIN_WALLETS_IN_CLUSTER = 3     # minimum wallets to call it a cluster
ALERT_RATE_LIMIT_SECS  = 60    # one alert per direction per minute


class WalletClusterAnalyzer:
    """
    Maintains a rolling event window and fires WHALE_CLUSTER_ALERT
    when coordinated movement is detected.
    """

    def __init__(self) -> None:
        self._window:  List[dict]      = []    # all events in rolling window
        self._lock     = threading.Lock()
        self._last_alert: Dict[str, float] = {}   # "BUY"|"SELL" → last ts
        self._pub                      = None

    # ── Public API ────────────────────────────────────────────────────────────

    def start(self) -> None:
        """Initialise Redis publisher. Call from WalletTracker.start()."""
        self._init_redis()
        logger.info("[ClusterAnalyzer] Ready")

    def ingest(self, event: dict) -> None:
        """
        Called by WalletTracker for every WHALE_ACCUMULATION /
        WHALE_DISTRIBUTION / EXCHANGE_INFLOW_ALERT event.
        """
        direction = self._event_to_direction(event)
        if not direction:
            return

        entry = {
            "address":   event.get("full_address", event.get("address", "")),
            "label":     event.get("label", "Unknown"),
            "direction": direction,
            "delta":     abs(float(event.get("delta", 0))),
            "asset":     event.get("asset", "BTC"),
            "behavior":  event.get("behavior", "unknown"),
            "ts":        event.get("ts", int(time.time() * 1000)),
        }

        with self._lock:
            self._window.append(entry)
            self._prune()
            self._detect()

    # ── Internal ──────────────────────────────────────────────────────────────

    def _init_redis(self) -> None:
        try:
            import redis
            from config.config import REDIS_URL
            from services.redis_pool import get_client as _get_redis_client

            self._pub = _get_redis_client()
            self._pub.ping()
        except Exception as e:
            logger.warning(f"[ClusterAnalyzer] Redis unavailable: {e}")

    def _prune(self) -> None:
        """Remove entries older than the rolling window (called under lock)."""
        cutoff = (time.time() - CLUSTER_WINDOW_SECS) * 1000
        self._window = [e for e in self._window if e["ts"] >= cutoff]

    def _detect(self) -> None:
        """Check for clusters (called under lock)."""
        for direction in ("BUY", "SELL"):
            group = [e for e in self._window if e["direction"] == direction]
            if len(group) < MIN_WALLETS_IN_CLUSTER:
                continue

            # Deduplicate by address — same wallet twice doesn't make a cluster
            unique: Dict[str, dict] = {}
            for e in group:
                addr = e["address"]
                # Keep the most recent entry per address
                if addr not in unique or e["ts"] > unique[addr]["ts"]:
                    unique[addr] = e

            if len(unique) < MIN_WALLETS_IN_CLUSTER:
                continue

            # Rate limit
            rate_key = direction
            if time.time() - self._last_alert.get(rate_key, 0) < ALERT_RATE_LIMIT_SECS:
                continue
            self._last_alert[rate_key] = time.time()

            self._publish_cluster(direction, list(unique.values()))

    def _publish_cluster(self, direction: str, wallets: List[dict]) -> None:
        total   = sum(w["delta"] for w in wallets)
        labels  = [w["label"] for w in wallets]
        asset   = wallets[0].get("asset", "BTC") if wallets else "BTC"

        # Confidence scales with wallet count and behaviour quality
        behavior_weights = {
            "accumulator": 0.9, "distributor": 0.9,
            "dormant": 0.8, "mixed": 0.4,
            "flipper": 0.2, "exchange": 0.1, "unknown": 0.3,
        }
        avg_weight = sum(
            behavior_weights.get(w.get("behavior", "unknown"), 0.3)
            for w in wallets
        ) / len(wallets)
        confidence = round(
            min(1.0, avg_weight * (len(wallets) / (MIN_WALLETS_IN_CLUSTER + 2))),
            3,
        )

        event = {
            "type":         "WHALE_CLUSTER_ALERT",
            "direction":    direction,
            "wallet_count": len(wallets),
            "total_asset":  round(total, 4),
            "asset":        asset,
            "window_s":     CLUSTER_WINDOW_SECS,
            "labels":       labels[:5],    # cap list length for readability
            "confidence":   confidence,
            "ts":           int(time.time() * 1000),
        }

        if self._pub:
            try:
                self._pub.publish("WHALE_CLUSTER_ALERT", json.dumps(event))
            except Exception as e:
                logger.debug(f"[ClusterAnalyzer] Redis publish: {e}")

        logger.warning(
            f"[ClusterAnalyzer] CLUSTER [{direction}] "
            f"{len(wallets)} wallets — {total:.4f} {asset} "
            f"(conf={confidence:.2f})"
        )

    @staticmethod
    def _event_to_direction(event: dict) -> Optional[str]:
        ev_type = event.get("type", "")
        if ev_type in ("WHALE_ACCUMULATION", "EXCHANGE_OUTFLOW_ALERT"):
            return "BUY"
        if ev_type in ("WHALE_DISTRIBUTION", "EXCHANGE_INFLOW_ALERT"):
            return "SELL"
        delta = float(event.get("delta", 0))
        if delta > 0:
            return "BUY"
        if delta < 0:
            return "SELL"
        return None