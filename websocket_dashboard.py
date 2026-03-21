"""
WebSocket Dashboard - Shared transaction store
Single source of truth imported by both web_app_live.py AND the WS manager thread
"""

from collections import deque
from datetime import datetime

# ─── SINGLE shared store ───────────────────────────────────────────────────────
recent_transactions: deque = deque(maxlen=500)

# ─── Per-exchange connection status ───────────────────────────────────────────
connection_status: dict = {
    'bybit':      {'connected': False, 'last_tick': None, 'symbol_count': 0, 'assets': 'Crypto'},
    'finnhub':    {'connected': False, 'last_tick': None, 'symbol_count': 0, 'assets': 'Stocks'},
    'twelvedata': {'connected': False, 'last_tick': None, 'symbol_count': 0, 'assets': 'Forex & Commodities'},
}


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
    if src in connection_status:
        connection_status[src]['connected'] = True
        connection_status[src]['last_tick'] = datetime.now().strftime('%H:%M:%S')
    return tx


def set_connected(source: str, connected: bool, symbol_count: int = 0):
    src = source.lower()
    if src in connection_status:
        connection_status[src]['connected'] = connected
        if symbol_count:
            connection_status[src]['symbol_count'] = symbol_count


def get_feed(source_filter: str = None, limit: int = 200) -> list:
    txs = list(recent_transactions)
    if source_filter and source_filter.lower() != 'all':
        txs = [t for t in txs if t['source_key'] == source_filter.lower()]
    return txs[-limit:]