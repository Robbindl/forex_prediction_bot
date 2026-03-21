from __future__ import annotations

import threading
from typing import Dict, List

from order_flow.orderbook_processor    import OrderbookProcessor
from order_flow.liquidity_wall_detector import LiquidityWallDetector
from order_flow.imbalance_detector     import ImbalanceDetector
from order_flow.stop_hunt_detector     import StopHuntDetector
from utils.logger import get_logger

logger = get_logger()

# ── Per-asset state ───────────────────────────────────────────────────────────
_processors: Dict[str, OrderbookProcessor]     = {}
_wall_detectors: Dict[str, LiquidityWallDetector] = {}
_imbalance_detectors: Dict[str, ImbalanceDetector] = {}
_stop_hunt_detectors: Dict[str, StopHuntDetector]  = {}

_running    = False
_sub_thread = None

TRACKED_ASSETS: List[str] = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT",
]


def _get_or_create(asset: str):
    """Lazily instantiate all four detectors for a new asset."""
    if asset not in _processors:
        _processors[asset]         = OrderbookProcessor(asset)
        _wall_detectors[asset]     = LiquidityWallDetector(asset)
        _imbalance_detectors[asset] = ImbalanceDetector(asset)
        _stop_hunt_detectors[asset] = StopHuntDetector(asset)
    return (
        _processors[asset],
        _wall_detectors[asset],
        _imbalance_detectors[asset],
        _stop_hunt_detectors[asset],
    )


def _on_orderbook_update(event: dict) -> None:
    """
    Central handler called for every ORDER_BOOK_UPDATE event.
    Runs all four detectors in sequence for the affected asset.
    """
    asset = event.get("asset", "")
    bids  = event.get("bids", [])
    asks  = event.get("asks", [])
    if not asset or (not bids and not asks):
        return

    proc, walls, imbalance, stop_hunt = _get_or_create(asset)

    # 1. Update book state and get normalised snapshot
    snapshot = proc.update(bids, asks)
    if not snapshot:
        return

    # 2. Scan for liquidity walls
    detected_walls = walls.scan(snapshot["top_bids"], snapshot["top_asks"])

    # 3. Check bid/ask imbalance
    imbalance.analyse(snapshot)

    # 4. Feed price into stop-hunt detector; pass current walls
    mid = snapshot.get("mid", 0)
    if mid:
        stop_hunt.update_walls(detected_walls)
        stop_hunt.ingest_price(mid, snapshot["ts"])


def _subscribe_loop() -> None:
    """Background thread — subscribes to Redis ORDER_BOOK_UPDATE channel."""
    try:
        import json, redis
        from config.config import REDIS_URL
        from services.redis_pool import get_pubsub as _get_pubsub
        ps = _get_pubsub()
        ps.subscribe("ORDER_BOOK_UPDATE")
        logger.info("[OrderFlow] Subscribed to ORDER_BOOK_UPDATE")

        for msg in ps.listen():
            if not _running:
                break
            if msg.get("type") == "message":
                try:
                    _on_orderbook_update(json.loads(msg["data"]))
                except Exception as e:
                    logger.debug(f"[OrderFlow] Handler error: {e}")
    except Exception as e:
        logger.error(f"[OrderFlow] Subscribe loop error: {e}")


def start_all() -> None:
    """Start Phase 3. Call once from bot.py main()."""
    global _running, _sub_thread
    _running = True
    # Pre-create detectors for all tracked assets
    for asset in TRACKED_ASSETS:
        _get_or_create(asset)
    _sub_thread = threading.Thread(
        target=_subscribe_loop, name="OrderFlowSub", daemon=True
    )
    _sub_thread.start()
    logger.info(f"[OrderFlow] Started — monitoring {len(TRACKED_ASSETS)} assets")


def stop_all() -> None:
    """Graceful shutdown."""
    global _running
    _running = False


def get_snapshot(asset: str) -> dict:
    """Return the latest order book snapshot for an asset (used by dashboard)."""
    proc = _processors.get(asset)
    return proc.latest_snapshot() if proc else {}


def get_imbalance(asset: str) -> float:
    """Return current bid/ask imbalance score -1.0 … +1.0 (used by meta-model)."""
    det = _imbalance_detectors.get(asset)
    return det.current_score() if det else 0.0


__all__ = [
    "start_all", "stop_all", "get_snapshot", "get_imbalance",
    "OrderbookProcessor", "LiquidityWallDetector",
    "ImbalanceDetector", "StopHuntDetector",
]