"""
config/api_validation.py — Startup API key validation.

Call validate_apis() from bot.py before starting the trading loop.
Raises RuntimeError for any [REQUIRED] key that is missing/placeholder.
Logs warnings for optional keys that are not set.
"""
from __future__ import annotations

import os
from typing import Dict, List, Tuple
from utils.logger import get_logger

logger = get_logger()

_PLACEHOLDER_PREFIXES = ("your_", "xxx", "placeholder", "changeme", "test_key")


def _is_placeholder(value: str) -> bool:
    v = value.strip().lower()
    return not v or any(v.startswith(p) for p in _PLACEHOLDER_PREFIXES)


def validate_apis() -> None:
    """
    Validate all API keys at startup.

    Required keys: raises RuntimeError if missing.
    Optional keys: logs a warning if missing (never raises).
    """
    errors:   List[str] = []
    warnings: List[str] = []

    # ── Required for news sentiment ───────────────────────────────────────
    required_news = {
        "NEWSAPI_KEY": os.getenv("NEWSAPI_KEY", ""),
        "GNEWS_KEY":   os.getenv("GNEWS_KEY", ""),
    }
    for name, val in required_news.items():
        if _is_placeholder(val):
            errors.append(
                f"{name} is missing or a placeholder.  "
                "News sentiment will not work.  Get a key from the respective provider."
            )

    # ── Required for DB ───────────────────────────────────────────────────
    db_url = os.getenv("DATABASE_URL", "")
    if "user:password" in db_url or _is_placeholder(db_url):
        errors.append(
            "DATABASE_URL is still the template value.  "
            "Set a real PostgreSQL connection string in .env."
        )

    # ── Optional: AlphaVantage ────────────────────────────────────────────
    av_key = os.getenv("ALPHA_VANTAGE_KEY", "")
    if _is_placeholder(av_key):
        warnings.append(
            "ALPHA_VANTAGE_KEY not set.  "
            "AlphaVantage sentiment disabled."
        )

    # ── Optional: Reddit ──────────────────────────────────────────────────
    reddit_id  = os.getenv("REDDIT_CLIENT_ID", "")
    reddit_sec = os.getenv("REDDIT_CLIENT_SECRET", "")
    if _is_placeholder(reddit_id) or _is_placeholder(reddit_sec):
        warnings.append(
            "REDDIT_CLIENT_ID / REDDIT_CLIENT_SECRET not set.  "
            "Reddit crypto sentiment disabled."
        )

    # ── Optional: FMP for put/call ────────────────────────────────────────
    fmp_key = os.getenv("FMP_API_KEY", "")
    if _is_placeholder(fmp_key):
        warnings.append(
            "FMP_API_KEY not set.  "
            "Put/call ratio for US indices disabled (AAII may still work)."
        )

    # ── Optional: Twitter ─────────────────────────────────────────────────
    twitter = os.getenv("TWITTER_BEARER_TOKEN", "")
    if _is_placeholder(twitter):
        warnings.append(
            "TWITTER_BEARER_TOKEN not set.  Twitter/X whale alerts disabled."
        )

    # ── Optional: Telegram ───────────────────────────────────────────────
    tg_token = os.getenv("TELEGRAM_TOKEN", "") or os.getenv("COMMAND_BOT_TOKEN", "")
    if _is_placeholder(tg_token):
        warnings.append(
            "TELEGRAM_TOKEN not set.  Telegram alerts and commands disabled."
        )

    # ── Optional: Finnhub / TwelveData ────────────────────────────────────
    if _is_placeholder(os.getenv("FINNHUB_KEY", "")):
        warnings.append("FINNHUB_KEY not set.  Real-time quotes will use yfinance only.")

    if _is_placeholder(os.getenv("TWELVEDATA_KEY", "")):
        warnings.append("TWELVEDATA_KEY not set.  OHLCV will use yfinance only.")

    # ── Report ────────────────────────────────────────────────────────────
    for w in warnings:
        logger.warning(f"[APIValidation] ⚠  {w}")

    if errors:
        for e in errors:
            logger.error(f"[APIValidation] ✗  {e}")
        raise RuntimeError(
            f"API validation failed with {len(errors)} error(s).  "
            "Fix .env before starting the bot.  "
            "See log output above for details."
        )

    logger.info(
        f"[APIValidation] ✓ Validation complete — "
        f"{len(warnings)} warning(s), 0 errors"
    )
