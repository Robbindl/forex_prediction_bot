"""
WebSocket Dashboard - Shared transaction store
Single source of truth imported by both web_app_live.py AND the WS manager thread
"""

from collections import deque
from datetime import datetime
import threading

# ─── SINGLE shared store ───────────────────────────────────────────────────────
recent_transactions: deque = deque(maxlen=500)

# ─── LIVE PRICE STORE (used by fetcher.get_real_time_price for P&L calc) ─────
live_prices: dict = {}  # {asset: (price, timestamp, source)}
live_prices_lock = threading.Lock()

# ─── Per-exchange connection status ───────────────────────────────────────────
connection_status: dict = {
    'deriv': {'connected': False, 'last_tick': None, 'symbol_count': 0, 'assets': 'Forex, Crypto, Indices'},
    'binance': {'connected': False, 'last_tick': None, 'symbol_count': 0, 'assets': 'BNB, SOL, XRP'},
    'ig': {'connected': False, 'last_tick': None, 'symbol_count': 0, 'assets': 'Gold, Silver, WTI'},
}


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
        live_prices[asset] = (price, datetime.now().timestamp(), source)


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
