"""
services/intelligence_alerts/__init__.py — Intelligence Alert System.

Subscribes to ALL Redis market event channels published by Phases 1-4
and dispatches rich formatted alerts to Telegram, email, and the
dashboard — completely independent of the signal pipeline.

These are MARKET INTELLIGENCE alerts, not trade signals. They fire
when something significant happens in the market regardless of whether
a trade signal was generated.

Alert sources (Redis channels subscribed)
-----------------------------------------
Phase 1 (Data Ingestion)
    LIQUIDATION_CASCADE_ALERT   — $10M+ liquidated in 60 seconds
    FUNDING_RATE_ALERT          — extreme funding rates detected
    OI_CHANGE_ALERT             — open interest spike/drop
    MACRO_NEWS_EVENT            — FRED economic data change

Phase 2 (Whale Intelligence)
    WHALE_ACCUMULATION          — single wallet large buy
    WHALE_DISTRIBUTION          — single wallet large sell
    WHALE_CLUSTER_ALERT         — 3+ wallets coordinated move
    EXCHANGE_INFLOW_ALERT       — large transfer to exchange
    EXCHANGE_OUTFLOW_ALERT      — large transfer from exchange

Phase 3 (Order Flow)
    LIQUIDITY_WALL_DETECTED     — large order cluster found
    BID_ASK_IMBALANCE_ALERT     — severe order book imbalance
    STOP_HUNT_DETECTED          — wick-and-revert pattern

Phase 4 (Narrative AI)
    NARRATIVE_TREND_DETECTED    — topic velocity spike
    REDDIT_TOPIC_SPIKE          — subreddit narrative surge
    TWITTER_TOPIC_SPIKE         — Twitter narrative surge

Priority levels
---------------
    CRITICAL  — immediate attention required (cascade, cluster)
    HIGH      — significant market event (whale, stop hunt)
    MEDIUM    — notable development (imbalance, narrative)
    LOW       — informational (OI change, sentiment shift)

Rate limiting
-------------
    Each event type has a minimum interval between alerts to prevent
    flooding your Telegram. CRITICAL = 60s, HIGH = 300s, MEDIUM = 600s,
    LOW = 900s.

Run tests
---------
    pytest tests/test_intelligence_alerts.py -v -m "not integration"
"""
from __future__ import annotations

from services.intelligence_alerts.intelligence_alert_service import (
    IntelligenceAlertService
)

# ── Module-level singleton ────────────────────────────────────────────────────
alert_service = IntelligenceAlertService()


def start_all(telegram_bot=None) -> None:
    """Start Phase 7. Call once from bot.py after Telegram is started."""
    if telegram_bot:
        alert_service.set_telegram(telegram_bot)
    alert_service.start()


def stop_all() -> None:
    alert_service.stop()


def set_telegram(telegram_bot) -> None:
    """Wire Telegram after the service is already running."""
    alert_service.set_telegram(telegram_bot)


__all__ = ["alert_service", "start_all", "stop_all", "set_telegram"]
