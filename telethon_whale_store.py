"""
telethon_whale_store.py — Shared in-memory store for Telethon whale alerts.

whale_alert_manager.py imports this as:
    from telethon_whale_store import whale_store as _whale_store

It checks:
    len(_whale_store) > 0
    _whale_store.format_for_dashboard(hours=hours)
"""
from __future__ import annotations

import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional


class TelethonWhaleStore:
    """
    Thread-safe store for whale alerts captured by TelegramWhaleWatcher.
    Implements len() and format_for_dashboard() as expected by WhaleAlertManager.
    """

    def __init__(self, max_alerts: int = 500):
        self._alerts: List[Dict] = []
        self._lock               = threading.Lock()
        self._max_alerts         = max_alerts

    # ── Interface used by whale_alert_manager.py ──────────────────────────

    def __len__(self) -> int:
        with self._lock:
            return len(self._alerts)

    def format_for_dashboard(self, hours: int = 24) -> List[Dict]:
        """
        Return alerts from the last N hours, formatted for the dashboard.
        Schema matches what WhaleAlertManager.get_alerts() expects.
        """
        cutoff = datetime.utcnow() - timedelta(hours=hours)
        with self._lock:
            recent = [
                a for a in self._alerts
                if a.get("_ts", datetime.utcnow()) > cutoff
            ]
        return [self._to_dashboard_fmt(a) for a in recent]

    # ── Write interface (called by TelegramWhaleWatcher) ──────────────────

    def add(self, alert: Dict) -> None:
        """Add an alert from TelegramWhaleWatcher."""
        enriched = dict(alert)
        enriched.setdefault("_ts", datetime.utcnow())
        enriched.setdefault("alert_time", datetime.utcnow())

        with self._lock:
            # Deduplicate by title
            titles = {a.get("title") for a in self._alerts}
            if enriched.get("title") not in titles:
                self._alerts.insert(0, enriched)
                self._alerts = self._alerts[: self._max_alerts]

    def get_recent(self, asset: str = "", minutes: int = 30) -> List[Dict]:
        """Get alerts for a specific asset in last N minutes."""
        cutoff = datetime.utcnow() - timedelta(minutes=minutes)
        with self._lock:
            results = [
                a for a in self._alerts
                if a.get("_ts", datetime.utcnow()) > cutoff
            ]
        if asset:
            results = [
                a for a in results
                if asset.upper() in a.get("symbol", "").upper()
                or asset.upper() in a.get("title", "").upper()
            ]
        return results

    def clear(self) -> None:
        with self._lock:
            self._alerts.clear()

    # ── Internal ──────────────────────────────────────────────────────────

    @staticmethod
    def _to_dashboard_fmt(alert: Dict) -> Dict:
        """Normalise to the schema WhaleAlertManager.get_alerts() uses."""
        value_usd = float(alert.get("value_usd", 0))
        return {
            "title":          alert.get("title", ""),
            "value_usd":      value_usd,
            "value_millions": round(value_usd / 1_000_000, 2),
            "symbol":         alert.get("symbol", "UNKNOWN"),
            "source":         alert.get("source", "Telegram"),
            "alert_time":     alert.get("alert_time", datetime.utcnow()),
            "sentiment":      alert.get("sentiment", 0.1),
        }


# Global singleton — imported by whale_alert_manager and bot.py
whale_store = TelethonWhaleStore()