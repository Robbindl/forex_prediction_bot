"""
market_calendar.py — Market calendar backed by Finnhub economic API.
Replaces hardcoded stub with real data from NewsEventMonitor.
"""
from __future__ import annotations
from datetime import datetime
from typing import Dict, List
from utils.logger import get_logger
logger = get_logger()

def get_high_impact_events(days: int = 3) -> List[Dict]:
    try:
        from data_ingestion.news_event_monitor import news_monitor
        events = news_monitor.upcoming_events(hours=days * 24)
        result = []
        for ev in events:
            if ev.get("impact") not in ("HIGH", "MEDIUM"):
                continue
            ev_time = ev.get("time")
            result.append({
                "date":     ev_time.strftime("%Y-%m-%d %H:%M UTC") if ev_time else "",
                "event":    ev.get("name", ""),
                "impact":   ev.get("impact", ""),
                "actual":   ev.get("actual"),
                "estimate": ev.get("estimate"),
                "surprise_direction": ev.get("surprise_direction", ""),
                "source":   ev.get("source", ""),
            })
        return result
    except Exception as e:
        logger.debug(f"[MarketCalendar] {e}")
        return []

class MarketCalendar:
    def __init__(self):
        self.economic_events = []
        self.earnings        = []
        self.halving_data    = self._init_halving_data()

    def _init_halving_data(self):
        return {
            "bitcoin":  {"next_halving": datetime(2028, 3, 15), "block_reward": 3.125,  "next_reward": 1.5625, "halving_count": 5},
            "litecoin": {"next_halving": datetime(2027, 8, 2),  "block_reward": 6.25,   "next_reward": 3.125,  "halving_count": 4},
        }

    def fetch_economic_calendar(self, days: int = 7):
        self.economic_events = get_high_impact_events(days=days)
        return self.economic_events

    @staticmethod
    def fetch_earnings_calendar(days: int = 7):
        return []

    def get_halving_countdown(self, crypto: str = "bitcoin"):
        data = self.halving_data.get(crypto, {})
        if not data:
            return {"error": f"Unknown: {crypto}"}
        days_until = (data["next_halving"] - datetime.now()).days
        return {
            "crypto": crypto, "days_until": days_until,
            "current_reward": data["block_reward"], "next_reward": data["next_reward"],
            "reduction_percent": (data["block_reward"]-data["next_reward"]) / data["block_reward"] * 100,
            "halving_date": data["next_halving"].strftime("%Y-%m-%d"),
            "is_soon": days_until <= 90,
        }

    def get_high_impact_events(self, days: int = 3):
        return get_high_impact_events(days=days)

    def should_reduce_risk(self):
        hi = self.get_high_impact_events(days=2)
        hv = self.get_halving_countdown()
        m  = 1.0
        if hi: m *= 0.7
        if hv.get("is_soon"): m *= 0.5
        return {"risk_multiplier": m, "reduce_trading": m < 0.8,
                "high_impact_events": len(hi) > 0, "halving_soon": hv.get("is_soon", False)}