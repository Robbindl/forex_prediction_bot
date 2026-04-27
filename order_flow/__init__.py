from __future__ import annotations

import threading
import time
from typing import Dict, List

from order_flow.orderbook_processor     import OrderbookProcessor
from order_flow.liquidity_wall_detector import LiquidityWallDetector
from order_flow.imbalance_detector      import ImbalanceDetector
from order_flow.stop_hunt_detector      import StopHuntDetector
from order_flow.signal_validator        import OrderFlowSignalValidator, get_validator
from utils.logger import get_logger

logger = get_logger()

# ── Per-asset state ───────────────────────────────────────────────────────────
_processors: Dict[str, OrderbookProcessor]     = {}
_wall_detectors: Dict[str, LiquidityWallDetector] = {}
_imbalance_detectors: Dict[str, ImbalanceDetector] = {}
_stop_hunt_detectors: Dict[str, StopHuntDetector]  = {}

_running    = False
_sub_thread = None
_subscribed = False
_last_subscribed_at = 0.0
_last_message_at = 0.0
_last_error = ""

TRACKED_ASSETS: List[str] = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT",
]


def _get_or_create(asset: str):
    """Lazily instantiate all four detectors for a new asset."""
    if asset not in _processors:
        validator = get_validator()
        _processors[asset]         = OrderbookProcessor(asset)
        _wall_detectors[asset]     = LiquidityWallDetector(asset, on_wall_detected=validator.ingest_wall)
        _imbalance_detectors[asset] = ImbalanceDetector(asset)
        _stop_hunt_detectors[asset] = StopHuntDetector(asset, on_hunt_detected=validator.ingest_hunt)
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
    try:
        from monitoring.system_health_service import monitor

        monitor.ping_source("order_book")
    except Exception:
        pass
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
    import json
    global _subscribed, _last_subscribed_at, _last_message_at, _last_error
    ps = None
    redis_unavailable_logged = False
    while _running:
        try:
            from services.redis_pool import get_pubsub as _get_pubsub
            ps = _get_pubsub(old_pubsub=ps)  # close old before new
            if ps is None:
                if not redis_unavailable_logged:
                    logger.warning("[OrderFlow] Redis unavailable — subscriber paused")
                    redis_unavailable_logged = True
                import time
                time.sleep(10)
                continue
            redis_unavailable_logged = False
            ps.subscribe("ORDER_BOOK_UPDATE")
            _subscribed = True
            _last_subscribed_at = time.time()
            _last_error = ""
            logger.info("[OrderFlow] Subscribed to ORDER_BOOK_UPDATE")

            for msg in ps.listen():
                if not _running:
                    break
                if msg.get("type") == "message":
                    _last_message_at = time.time()
                    try:
                        _on_orderbook_update(json.loads(msg["data"]))
                    except Exception as e:
                        logger.debug(f"[OrderFlow] Handler error: {e}")
        except Exception as e:
            _subscribed = False
            _last_error = str(e)
            logger.warning(f"[OrderFlow] Subscriber dropped ({e}) — retrying in 10s")
            if _running:
                import time; time.sleep(10)


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
    # Feed wall/hunt events directly in-process to avoid an extra Redis pub/sub
    # connection for the validator on every bot instance.
    get_validator().start(use_redis_subscriber=False)
    logger.info(f"[OrderFlow] Started — monitoring {len(TRACKED_ASSETS)} assets")


def stop_all() -> None:
    """Graceful shutdown."""
    global _running, _subscribed
    _running = False
    _subscribed = False
    get_validator().stop()


def get_snapshot(asset: str) -> dict:
    """Return the latest order book snapshot for an asset (used by dashboard)."""
    proc = _processors.get(asset)
    return proc.latest_snapshot() if proc else {}


def get_imbalance(asset: str) -> float:
    """Return current bid/ask imbalance score -1.0 … +1.0 (used by meta-model)."""
    det = _imbalance_detectors.get(asset)
    return det.current_score() if det else 0.0


def status() -> dict:
    thread = _sub_thread
    now = time.time()
    last_subscribed_age = max(0.0, now - _last_subscribed_at) if _last_subscribed_at > 0 else None
    last_message_age = max(0.0, now - _last_message_at) if _last_message_at > 0 else None
    return {
        "running": bool(_running),
        "thread_alive": bool(thread is not None and thread.is_alive()),
        "subscribed": bool(_subscribed),
        "tracked_assets": list(TRACKED_ASSETS),
        "last_subscribed_age_seconds": round(last_subscribed_age, 3) if last_subscribed_age is not None else None,
        "last_message_age_seconds": round(last_message_age, 3) if last_message_age is not None else None,
        "last_error": str(_last_error or ""),
    }


__all__ = [
    "start_all", "stop_all", "get_snapshot", "get_imbalance", "status",
    "OrderbookProcessor", "LiquidityWallDetector",
    "ImbalanceDetector", "StopHuntDetector",
    "get_validator", "OrderFlowSignalValidator",
]
