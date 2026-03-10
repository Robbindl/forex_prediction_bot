"""
WebSocket Dashboard - Shared transaction store
Single source of truth imported by both web_app_live.py AND the WS manager thread
"""

from collections import deque
from datetime import datetime

# ─── SINGLE shared store ───────────────────────────────────────────────────────
# web_app_live.py and the WebSocket manager BOTH import from here.
# Because they run in the SAME process (manager is a thread, not subprocess),
# they share the same object in memory — so writes are immediately visible.
recent_transactions: deque = deque(maxlen=100)


def add_transaction(source: str, symbol: str, price: float,
                    volume: float = None, side: str = None) -> dict:
    """Append a trade to the shared store. Called by the WebSocket callback."""
    tx = {
        'time':      datetime.now().strftime('%H:%M:%S'),
        'source':    source.upper(),
        'symbol':    symbol,
        'price':     f"${price:,.2f}",
        'volume':    f"{volume:.4f}" if volume is not None else '-',
        'side':      side if side else '-',
        'timestamp': datetime.now().timestamp()
    }
    recent_transactions.append(tx)
    return tx
