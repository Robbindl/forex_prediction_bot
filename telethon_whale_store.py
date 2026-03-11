"""
telethon_whale_store.py
=======================
Singleton store that the Telethon whale listener writes to.
Every part of the system reads from here — sentiment dashboard,
signal_learning, voting, everything.

Usage (read):
    from telethon_whale_store import whale_store
    alerts = whale_store.get_alerts()               # all last 24h
    sentiment = whale_store.get_sentiment('BTC')    # -1.0 to +1.0
    boost = whale_store.get_confidence_boost('BTC') # e.g. +0.06

Usage (write — only engines/whale_monitor.py calls this):
    whale_store.add(alert_dict)
"""
from __future__ import annotations
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from logger import logger


class _WhaleStore:
    """Thread-safe singleton store for Telethon whale alerts."""

    _MAX = 500  # keep last 500 alerts in memory

    def __init__(self):
        self._alerts: List[Dict] = []
        self._lock  = threading.Lock()

    # ── Write ────────────────────────────────────────────────────────────────

    def add(self, alert: Dict) -> None:
        """
        Add one alert. Expected keys:
          symbol, amount, value (USD), channel, bullish (bool), time (datetime)
        """
        alert.setdefault('time',    datetime.now())
        alert.setdefault('bullish', True)
        alert.setdefault('source',  'telethon')
        with self._lock:
            self._alerts.append(alert)
            if len(self._alerts) > self._MAX:
                self._alerts = self._alerts[-self._MAX:]
        logger.debug(
            "WhaleStore: +%s %s ($%.1fM) bullish=%s",
            alert.get('amount', '?'),
            alert.get('symbol', '?'),
            alert.get('value', 0) / 1_000_000,
            alert.get('bullish'),
        )

    # ── Read ─────────────────────────────────────────────────────────────────

    def get_alerts(
        self,
        hours:         int   = 24,
        min_value_usd: float = 1_000_000,
        symbol:        Optional[str] = None,
    ) -> List[Dict]:
        """Return alerts sorted by value descending."""
        cutoff = datetime.now() - timedelta(hours=hours)
        with self._lock:
            alerts = [
                a for a in self._alerts
                if a.get('time', datetime.min) >= cutoff
                and a.get('value', 0) >= min_value_usd
                and (symbol is None or a.get('symbol', '').upper() == symbol.upper())
            ]
        alerts.sort(key=lambda x: x.get('value', 0), reverse=True)
        return alerts

    def get_sentiment(self, symbol: str, hours: int = 24) -> float:
        """
        Sentiment score from -1.0 (all bearish) to +1.0 (all bullish).
        Returns 0.0 if no recent alerts for this symbol.
        """
        alerts = self.get_alerts(hours=hours, symbol=symbol, min_value_usd=0)
        if not alerts:
            return 0.0
        total   = sum(a.get('value', 0) for a in alerts)
        bullish = sum(a.get('value', 0) for a in alerts if a.get('bullish'))
        if total == 0:
            return 0.0
        return round((bullish / total) * 2 - 1, 3)

    def get_confidence_boost(self, symbol: str, hours: int = 24) -> float:
        """
        Returns a confidence delta to add to a signal:
          +0.10 max (strong bullish whale flow)
          -0.10 max (strong bearish whale flow)
          0.0 if no data or weak signal
        Threshold: sentiment must exceed ±0.3 to have any effect.
        """
        s = self.get_sentiment(symbol, hours)
        if abs(s) < 0.3:
            return 0.0
        # Scale: sentiment 0.3→1.0 maps to boost 0.0→0.10
        return round(s * 0.10, 4)

    def format_for_dashboard(self, hours: int = 24, limit: int = 10) -> List[Dict]:
        """
        Returns alerts in the format expected by the sentiment dashboard
        (matches whale_alert_manager output schema).
        """
        alerts = self.get_alerts(hours=hours)[:limit]
        result = []
        for a in alerts:
            val = a.get('value', 0)
            sym = a.get('symbol', '?')
            amt = a.get('amount', 0)
            ch  = a.get('channel', 'telegram')
            bull= a.get('bullish', True)
            t   = a.get('time', datetime.now())
            result.append({
                'title':         f"{amt:.0f} {sym} (${val/1e6:.1f}M) via @{ch}",
                'symbol':        sym,
                'amount':        amt,
                'value_usd':     val,
                'value_millions': val / 1_000_000,
                'source':        'Telethon',
                'channel':       ch,
                'bullish':       bull,
                'sentiment':     'BULLISH' if bull else 'BEARISH',
                'alert_time':    t.isoformat() if hasattr(t, 'isoformat') else str(t),
            })
        return result

    def __len__(self):
        with self._lock:
            return len(self._alerts)


# Module-level singleton — import this everywhere
whale_store = _WhaleStore()
