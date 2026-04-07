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
            "DATABASE_URL is missing or still the template value.  "
            "Set a real PostgreSQL connection string in .env."
        )

    # ── Optional: AlphaVantage ────────────────────────────────────────────
    av_key = os.getenv("ALPHA_VANTAGE_KEY", "")
    if _is_placeholder(av_key):
        warnings.append(
            "ALPHA_VANTAGE_KEY not set.  "
            "AlphaVantage sentiment disabled."
        )

    # Reddit — no credentials needed (uses public JSON endpoints)

    # ── Optional: FMP history/backfill ────────────────────────────────────
    fmp_key = os.getenv("FMP_API_KEY", "")
    if _is_placeholder(fmp_key):
        warnings.append(
            "FMP_API_KEY not set.  "
            "FMP historical backfill is disabled, so charts/research/history will fall back to broker-only OHLCV."
        )

    dukascopy_enabled = os.getenv("DUKASCOPY_HISTORY_ENABLED", "true").strip().lower() == "true"
    if not dukascopy_enabled:
        warnings.append(
            "DUKASCOPY_HISTORY_ENABLED is false.  "
            "Free Dukascopy historical backfill for forex/commodities/indices is disabled."
        )

    # ── Optional: Twitter ─────────────────────────────────────────────────
    twitter = os.getenv("TWITTER_BEARER_TOKEN", "")
    if _is_placeholder(twitter):
        warnings.append(
            "TWITTER_BEARER_TOKEN not set.  Twitter/X whale alerts disabled."
        )

    # ── Optional: Telegram ───────────────────────────────────────────────
    tg_token = os.getenv("COMMAND_BOT_TOKEN", "")
    if _is_placeholder(tg_token):
        warnings.append(
            "COMMAND_BOT_TOKEN not set.  Telegram alerts and commands disabled."
        )

    # ── Primary market-data source: Deriv ─────────────────────────────────
    deriv_enabled = os.getenv("DERIV_ENABLED", "true").strip().lower() == "true"
    deriv_app_id = os.getenv("DERIV_APP_ID", "").strip()
    if not deriv_enabled:
        warnings.append(
            "DERIV_ENABLED is false.  Primary Deriv market data is disabled."
        )
    if _is_placeholder(deriv_app_id):
        warnings.append(
            "DERIV_APP_ID not set.  Primary Deriv market data is unavailable until configured."
        )
    binance_enabled = os.getenv("BINANCE_PUBLIC_DATA_ENABLED", "true").strip().lower() == "true"
    if not binance_enabled:
        warnings.append(
            "BINANCE_PUBLIC_DATA_ENABLED is false.  BNB/SOL/XRP market data will be unavailable when Deriv has no symbol."
        )
    ig_enabled = os.getenv("IG_ENABLED", "false").strip().lower() == "true"
    ig_api_key = os.getenv("IG_API_KEY", "")
    ig_identifier = os.getenv("IG_IDENTIFIER", "")
    ig_password = os.getenv("IG_PASSWORD", "")
    if ig_enabled and _is_placeholder(ig_api_key):
        warnings.append(
            "IG_API_KEY not set.  IG-routed commodities will fall back to Deriv when IG market data is unavailable."
        )
    if ig_enabled and (_is_placeholder(ig_identifier) or _is_placeholder(ig_password)):
        warnings.append(
            "IG_IDENTIFIER / IG_PASSWORD not set.  IG commodity routing is configured, but IG market data cannot authenticate yet."
        )

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
