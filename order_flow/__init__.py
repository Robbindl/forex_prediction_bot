from __future__ import annotations

import json
import threading
import time
from pathlib import Path
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
_snapshot_store_path = Path("data/order_flow_snapshots.json")
_snapshot_store_lock = threading.RLock()
_snapshot_store: Dict[str, dict] = {}
_snapshot_store_mtime = 0.0
_snapshot_store_last_write = 0.0
_SNAPSHOT_STORE_WRITE_MIN_INTERVAL = 0.75

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


def _load_snapshot_store(*, force: bool = False) -> None:
    global _snapshot_store_mtime
    path = _snapshot_store_path
    try:
        mtime = path.stat().st_mtime if path.exists() else 0.0
    except Exception:
        mtime = 0.0
    if not force and mtime <= 0.0:
        return
    if not force and mtime == _snapshot_store_mtime:
        return

    loaded: Dict[str, dict] = {}
    if mtime > 0.0:
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                loaded = {str(asset): dict(snapshot or {}) for asset, snapshot in raw.items() if isinstance(snapshot, dict)}
        except Exception as exc:
            logger.debug(f"[OrderFlow] Snapshot store load failed: {exc}")
            loaded = {}

    with _snapshot_store_lock:
        _snapshot_store.clear()
        _snapshot_store.update(loaded)
        _snapshot_store_mtime = mtime


def _persist_snapshot(asset: str, snapshot: dict, *, force: bool = False) -> None:
    global _snapshot_store_last_write, _snapshot_store_mtime
    asset_key = str(asset or "").strip()
    if not asset_key or not isinstance(snapshot, dict) or not snapshot:
        return

    path = _snapshot_store_path
    path.parent.mkdir(parents=True, exist_ok=True)

    with _snapshot_store_lock:
        _snapshot_store[asset_key] = dict(snapshot)
        now = time.time()
        should_write = force or (now - _snapshot_store_last_write >= _SNAPSHOT_STORE_WRITE_MIN_INTERVAL)
        if not should_write:
            return
        try:
            path.write_text(json.dumps(_snapshot_store, ensure_ascii=True), encoding="utf-8")
            _snapshot_store_last_write = now
            try:
                _snapshot_store_mtime = path.stat().st_mtime
            except Exception:
                _snapshot_store_mtime = now
        except Exception as exc:
            logger.debug(f"[OrderFlow] Snapshot store persist failed: {exc}")


def _get_persisted_snapshot(asset: str) -> dict:
    asset_key = str(asset or "").strip()
    if not asset_key:
        return {}
    _load_snapshot_store(force=False)
    with _snapshot_store_lock:
        return dict(_snapshot_store.get(asset_key, {}) or {})


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
    _persist_snapshot(asset, snapshot)

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
                time.sleep(10)


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
    snapshot = proc.latest_snapshot() if proc else {}
    if snapshot:
        return snapshot
    return _get_persisted_snapshot(asset)


def get_imbalance(asset: str) -> float:
    """Return current bid/ask imbalance score -1.0 … +1.0 (used by meta-model)."""
    det = _imbalance_detectors.get(asset)
    if det:
        return det.current_score()
    snapshot = _get_persisted_snapshot(asset)
    try:
        return round(float(snapshot.get("imbalance", 0.0) or 0.0), 4)
    except Exception:
        return 0.0


def status() -> dict:
    thread = _sub_thread
    now = time.time()
    last_subscribed_age = max(0.0, now - _last_subscribed_at) if _last_subscribed_at > 0 else None
    last_message_age = max(0.0, now - _last_message_at) if _last_message_at > 0 else None
    _load_snapshot_store(force=False)
    try:
        store_mtime = _snapshot_store_path.stat().st_mtime if _snapshot_store_path.exists() else 0.0
    except Exception:
        store_mtime = 0.0
    with _snapshot_store_lock:
        persisted_assets = sorted(_snapshot_store.keys())
    return {
        "running": bool(_running),
        "thread_alive": bool(thread is not None and thread.is_alive()),
        "subscribed": bool(_subscribed),
        "tracked_assets": list(TRACKED_ASSETS),
        "persisted_assets": persisted_assets,
        "snapshot_store_path": str(_snapshot_store_path),
        "snapshot_store_age_seconds": round(max(0.0, now - store_mtime), 3) if store_mtime > 0 else None,
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


