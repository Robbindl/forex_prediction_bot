"""
WebSocket Dashboard - Shared transaction store
Single source of truth imported by both web_app_live.py AND the WS manager thread
"""

from collections import deque
from datetime import datetime
from pathlib import Path
import os
import sqlite3
import threading
import time

try:
    from services.local_candle_store import local_candle_store
except Exception:
    local_candle_store = None

# ─── SINGLE shared store ───────────────────────────────────────────────────────
recent_transactions: deque = deque(maxlen=5000)

# ─── LIVE PRICE STORE (used by fetcher.get_real_time_price for P&L calc) ─────
live_prices: dict = {}  # {asset: (price, timestamp, source)}
live_price_history: dict = {}  # {asset: deque[(price, timestamp, source)]}
live_prices_lock = threading.Lock()
_DASHBOARD_STATE_PATH = Path(os.getenv("LIVE_DASHBOARD_STORE_PATH") or "data/live_dashboard_state.sqlite3")
_dashboard_state_lock = threading.Lock()
_dashboard_state_conn = None
_pending_live_snapshot_rows: dict[str, tuple[float, float, str]] = {}
_snapshot_flush_stop = threading.Event()
_snapshot_flush_thread = None

# ─── Per-exchange connection status ───────────────────────────────────────────
connection_status: dict = {
    'deriv': {'connected': False, 'last_tick': None, 'symbol_count': 0, 'assets': 'Forex, Crypto, Indices'},
    'binance': {'connected': False, 'last_tick': None, 'symbol_count': 0, 'assets': 'BNB, SOL, XRP'},
    'bybit': {'connected': False, 'last_tick': None, 'symbol_count': 0, 'assets': 'Gold, Silver, WTI (1000-level depth)'},
    'okx': {'connected': False, 'last_tick': None, 'symbol_count': 0, 'assets': 'Gold, Silver, WTI (exchange depth)'},
    'ig': {'connected': False, 'last_tick': None, 'symbol_count': 0, 'assets': 'Gold, Silver, WTI, GER40, AUS200, JPN225'},
}

_LIVE_PRICE_JUMP_LIMITS = {
    "forex": {"fast": 0.03, "slow": 0.08},
    "crypto": {"fast": 0.18, "slow": 0.35},
    "commodities": {"fast": 0.08, "slow": 0.18},
    "indices": {"fast": 0.06, "slow": 0.15},
    "unknown": {"fast": 0.10, "slow": 0.20},
}


def _live_price_category(asset: str) -> str:
    try:
        from core.assets import registry

        return str(registry.category(asset) or "unknown").strip().lower()
    except Exception:
        return "unknown"


def _live_price_jump_limit(asset: str, age_seconds: float) -> float:
    category = _live_price_category(asset)
    limits = _LIVE_PRICE_JUMP_LIMITS.get(category, _LIVE_PRICE_JUMP_LIMITS["unknown"])
    age = float(age_seconds or 0.0)
    if age > 1800.0:
        return float("inf")
    return float(limits["slow"] if age > 120.0 else limits["fast"])


def _is_implausible_live_price(asset: str, price: float, now_ts: float) -> bool:
    current = live_prices.get(asset)
    if current is None:
        return False
    prev_price, prev_ts, _prev_source = current
    prev_price = float(prev_price or 0.0)
    if prev_price <= 0.0 or float(price or 0.0) <= 0.0:
        return False
    age_seconds = max(0.0, float(now_ts or 0.0) - float(prev_ts or 0.0))
    change_fraction = abs(float(price) - prev_price) / prev_price
    return change_fraction > _live_price_jump_limit(asset, age_seconds)


def _dashboard_state_connection():
    global _dashboard_state_conn
    if _dashboard_state_conn is None:
        _DASHBOARD_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        _dashboard_state_conn = sqlite3.connect(str(_DASHBOARD_STATE_PATH), check_same_thread=False, timeout=30)
        _dashboard_state_conn.execute("PRAGMA journal_mode=WAL")
        _dashboard_state_conn.execute("PRAGMA synchronous=NORMAL")
        _dashboard_state_conn.execute("PRAGMA temp_store=MEMORY")
    return _dashboard_state_conn


def _ensure_dashboard_state_schema() -> None:
    with _dashboard_state_lock:
        conn = _dashboard_state_connection()
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS dashboard_transactions (
                timestamp REAL NOT NULL,
                time_label TEXT,
                source TEXT,
                source_key TEXT,
                symbol TEXT,
                price REAL,
                price_text TEXT,
                volume TEXT,
                side TEXT
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_dashboard_transactions_ts ON dashboard_transactions (timestamp DESC)"
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS live_price_snapshots (
                asset TEXT PRIMARY KEY,
                price REAL NOT NULL,
                timestamp REAL NOT NULL,
                source TEXT
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_live_price_snapshots_ts ON live_price_snapshots (timestamp DESC)"
        )
        conn.commit()


def _persist_transaction(tx: dict) -> None:
    try:
        with _dashboard_state_lock:
            conn = _dashboard_state_connection()
            conn.execute(
                """
                INSERT INTO dashboard_transactions (
                    timestamp, time_label, source, source_key, symbol, price, price_text, volume, side
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    float(tx.get("timestamp", 0.0) or 0.0),
                    str(tx.get("time", "") or ""),
                    str(tx.get("source", "") or ""),
                    str(tx.get("source_key", "") or ""),
                    str(tx.get("symbol", "") or ""),
                    float(tx.get("price_raw", 0.0) or 0.0),
                    str(tx.get("price", "") or ""),
                    str(tx.get("volume", "") or ""),
                    str(tx.get("side", "") or ""),
                ),
            )
            conn.execute(
                """
                DELETE FROM dashboard_transactions
                WHERE rowid NOT IN (
                    SELECT rowid FROM dashboard_transactions
                    ORDER BY timestamp DESC
                    LIMIT 5000
                )
                """
            )
            conn.commit()
    except Exception:
        pass


def _persist_live_snapshot(asset: str, price: float, ts: float, source: str) -> None:
    with _dashboard_state_lock:
        _pending_live_snapshot_rows[str(asset or "")] = (float(price), float(ts), str(source or ""))


def _flush_live_snapshots() -> None:
    try:
        with _dashboard_state_lock:
            if not _pending_live_snapshot_rows:
                return
            rows = [
                (asset, float(price), float(ts), str(source or ""))
                for asset, (price, ts, source) in _pending_live_snapshot_rows.items()
                if asset
            ]
            if not rows:
                _pending_live_snapshot_rows.clear()
                return
            conn = _dashboard_state_connection()
            conn.executemany(
                """
                INSERT INTO live_price_snapshots (asset, price, timestamp, source)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(asset) DO UPDATE SET
                    price=excluded.price,
                    timestamp=excluded.timestamp,
                    source=excluded.source
                """,
                rows,
            )
            conn.commit()
            _pending_live_snapshot_rows.clear()
    except Exception:
        pass


def _start_snapshot_flush_worker() -> None:
    global _snapshot_flush_thread
    if _snapshot_flush_thread is not None:
        return

    def _loop() -> None:
        while not _snapshot_flush_stop.wait(1.0):
            _flush_live_snapshots()

    _snapshot_flush_thread = threading.Thread(
        target=_loop,
        name="DashboardLiveSnapshotFlush",
        daemon=True,
    )
    _snapshot_flush_thread.start()


def _hydrate_live_history_from_store(asset: str, limit: int = 720) -> None:
    if local_candle_store is None:
        return
    try:
        if not local_candle_store.enabled():
            return
        category = "forex"
        try:
            from core.assets import registry

            category = registry.category(asset) or "forex"
        except Exception:
            pass
        frame, _meta = local_candle_store.get_ohlcv(
            asset,
            category,
            "1m",
            max(60, int(limit or 720)),
            closed_only=False,
        )
        if frame is None or frame.empty or "close" not in frame.columns:
            return
        history = deque(maxlen=5000)
        for idx, row in frame.iterrows():
            try:
                ts = float(getattr(idx, "timestamp", lambda: 0.0)())
            except Exception:
                ts = 0.0
            price = float(row.get("close", 0.0) or 0.0)
            if ts > 0 and price > 0:
                history.append((price, ts, "LocalStore"))
        if not history:
            return
        with live_prices_lock:
            existing = live_price_history.get(asset)
            if existing and len(existing) >= len(history):
                return
            live_price_history[asset] = history
            last_price, last_ts, last_source = history[-1]
            current = live_prices.get(asset)
            if current is None or float(current[1] or 0.0) < float(last_ts):
                live_prices[asset] = (float(last_price), float(last_ts), str(last_source))
    except Exception:
        pass


def _hydrate_dashboard_state() -> None:
    try:
        _ensure_dashboard_state_schema()
    except Exception:
        return

    try:
        with _dashboard_state_lock:
            conn = _dashboard_state_connection()
            rows = conn.execute(
                """
                SELECT timestamp, time_label, source, source_key, symbol, price, price_text, volume, side
                FROM dashboard_transactions
                ORDER BY timestamp ASC
                LIMIT 5000
                """
            ).fetchall()
        for ts, time_label, source, source_key, symbol, price, price_text, volume, side in rows:
            recent_transactions.append(
                {
                    "time": str(time_label or ""),
                    "source": str(source or ""),
                    "source_key": str(source_key or ""),
                    "symbol": str(symbol or ""),
                    "price": str(price_text or ""),
                    "price_raw": float(price or 0.0),
                    "volume": str(volume or "-"),
                    "side": str(side or "-"),
                    "timestamp": float(ts or 0.0),
                }
            )
    except Exception:
        pass

    try:
        with _dashboard_state_lock:
            conn = _dashboard_state_connection()
            rows = conn.execute(
                """
                SELECT asset, price, timestamp, source
                FROM live_price_snapshots
                ORDER BY timestamp DESC
                """
            ).fetchall()
        with live_prices_lock:
            for asset, price, ts, source in rows:
                if not asset:
                    continue
                live_prices[str(asset)] = (float(price), float(ts), str(source or "Persisted"))
    except Exception:
        pass


def mark_feed_activity(source: str, symbol_count: int = None) -> None:
    src = source.lower()
    if src in connection_status:
        connection_status[src]['connected'] = True
        if symbol_count is not None:
            connection_status[src]['symbol_count'] = symbol_count
        connection_status[src]['last_tick'] = datetime.now().strftime('%H:%M:%S')


def add_transaction(source: str, symbol: str, price: float,
                    volume: float = None, side: str = None) -> dict:
    src = source.lower()
    tx = {
        'time':       datetime.now().strftime('%H:%M:%S'),
        'source':     source.upper(),
        'source_key': src,
        'symbol':     symbol,
        'price':      f"${price:,.4f}" if price < 100 else f"${price:,.2f}",
        'price_raw':  price,
        'volume':     f"{volume:.4f}" if volume is not None else '-',
        'side':       side if side else '-',
        'timestamp':  datetime.now().timestamp()
    }
    recent_transactions.append(tx)
    _persist_transaction(tx)
    mark_feed_activity(src)
    return tx


def set_connected(source: str, connected: bool, symbol_count: int = 0):
    src = source.lower()
    if src in connection_status:
        connection_status[src]['connected'] = connected
        connection_status[src]['symbol_count'] = symbol_count


def get_feed(source_filter: str = None, limit: int = 200) -> list:
    txs = list(recent_transactions)
    if source_filter and source_filter.lower() != 'all':
        txs = [t for t in txs if t['source_key'] == source_filter.lower()]
    return txs[-limit:]


# ─── LIVE PRICE HELPERS (for P&L real-time updates) ─────────────────────────
def set_live_price(asset: str, price: float, source: str = "WebSocket") -> None:
    """Store latest real-time price from WebSocket. Called by callback."""
    with live_prices_lock:
        ts = datetime.now().timestamp()
        if _is_implausible_live_price(asset, float(price), ts):
            return
        live_prices[asset] = (price, ts, source)
        history = live_price_history.setdefault(asset, deque(maxlen=5000))
        history.append((float(price), float(ts), str(source)))
    _persist_live_snapshot(asset, float(price), float(ts), str(source))
    if local_candle_store is not None:
        try:
            local_candle_store.record_live_price(asset, float(price), source=str(source), timestamp=float(ts))
        except Exception:
            pass


def get_live_price(asset: str, max_age_seconds: float = 10.0) -> tuple:
    """Get latest live price if fresh enough. Returns (price, source) or (None, None)."""
    with live_prices_lock:
        if asset not in live_prices:
            return (None, None)
        price, ts, source = live_prices[asset]
        age = datetime.now().timestamp() - ts
        if age <= max_age_seconds:
            return (price, source)
    return (None, None)


def get_live_price_snapshot(asset: str, max_age_seconds: float | None = None) -> dict | None:
    """Return live-price details including age, optionally requiring freshness."""
    with live_prices_lock:
        if asset not in live_prices:
            return None
        price, ts, source = live_prices[asset]
        age = max(0.0, datetime.now().timestamp() - float(ts or 0.0))
        if max_age_seconds is not None and age > float(max_age_seconds):
            return None
        return {
            "price": float(price),
            "timestamp": float(ts),
            "source": str(source),
            "age_seconds": age,
        }


def get_live_price_history(
    asset: str,
    max_age_seconds: float | None = None,
    limit: int | None = None,
) -> list[dict]:
    with live_prices_lock:
        history = list(live_price_history.get(asset, ()))
    if not history:
        _hydrate_live_history_from_store(asset, limit=max(int(limit or 0), 720) if limit is not None else 720)
        with live_prices_lock:
            history = list(live_price_history.get(asset, ()))
    if not history:
        return []
    now_ts = datetime.now().timestamp()
    items = []
    for price, ts, source in history:
        age = max(0.0, now_ts - float(ts or 0.0))
        if max_age_seconds is not None and age > float(max_age_seconds):
            continue
        items.append({
            "price": float(price),
            "timestamp": float(ts),
            "source": str(source),
            "age_seconds": age,
        })
    if limit is not None and limit > 0:
        items = items[-int(limit):]
    return items


def get_live_price_snapshots(
    assets: list[str] | tuple[str, ...] | set[str] | None = None,
    max_age_seconds: float | None = None,
) -> dict[str, dict]:
    requested = {str(asset or "") for asset in (assets or []) if str(asset or "").strip()} if assets is not None else None
    now_ts = datetime.now().timestamp()
    snapshots: dict[str, dict] = {}
    with live_prices_lock:
        for asset, (price, ts, source) in live_prices.items():
            if requested is not None and asset not in requested:
                continue
            age = max(0.0, now_ts - float(ts or 0.0))
            if max_age_seconds is not None and age > float(max_age_seconds):
                continue
            snapshots[str(asset)] = {
                "price": float(price),
                "timestamp": float(ts),
                "source": str(source),
                "age_seconds": age,
            }
    return snapshots


_hydrate_dashboard_state()
_start_snapshot_flush_worker()
