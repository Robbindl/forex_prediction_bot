from __future__ import annotations

import json
import re
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

from config.config import (
    DEEPSEEK_API_KEY,
    GNEWS_KEY,
    NEWSAPI_KEY,
    ROBBIE_CHAT_BASE_URL,
    ROBBIE_CHAT_HISTORY_LIMIT,
    ROBBIE_CHAT_MAX_TOKENS,
    ROBBIE_CHAT_MODEL,
    ROBBIE_CHAT_NEWS_ENABLED,
    ROBBIE_CHAT_TEMPERATURE,
    ROBBIE_CHAT_TIMEOUT_SECONDS,
)
from core.assets import registry
from core.state import rollup_closed_trade_history
from utils.display_time import display_timezone_label, format_display_datetime, now_in_display_timezone
from utils.logger import get_logger

logger = get_logger()

_SESSION_FILE = Path("data/deepseek_chat_sessions.json")
_SESSION_FILE.parent.mkdir(parents=True, exist_ok=True)
_MAX_HISTORY_MESSAGES = max(2, int(ROBBIE_CHAT_HISTORY_LIMIT or 10)) * 2
_MACRO_SNAPSHOT_TTL_SECONDS = 300.0
_MACRO_SNAPSHOT_CACHE: Dict[str, tuple[Dict[str, Any], float]] = {}
_MACRO_SNAPSHOT_LOCK = threading.Lock()
_CURRENT_NEWS_SNAPSHOT_TTL_SECONDS = 300.0
_CURRENT_NEWS_SNAPSHOT_CACHE: Dict[str, tuple[Dict[str, Any], float]] = {}
_CURRENT_NEWS_SNAPSHOT_LOCK = threading.Lock()
_ASSET_ALIAS_PATTERNS: List[tuple[re.Pattern[str], str]] = []

_MACRO_QUESTION_TERMS = (
    "nfp",
    "non-farm",
    "nonfarm",
    "payroll",
    "jobs",
    "jobless",
    "unemployment",
    "cpi",
    "ppi",
    "pce",
    "fomc",
    "fed",
    "powell",
    "interest rate",
    "rate cut",
    "rate hike",
    "inflation",
    "gdp",
    "retail sales",
    "oil",
    "wti",
    "crude",
    "brent",
    "opec",
    "inventory",
    "inventories",
    "energy",
    "geopolitical",
    "middle east",
    "war",
    "conflict",
)

_MACRO_NEWS_TERMS = (
    "nfp",
    "non-farm",
    "nonfarm",
    "payroll",
    "jobs",
    "jobless",
    "unemployment",
    "cpi",
    "ppi",
    "pce",
    "fomc",
    "fed",
    "powell",
    "interest rate",
    "inflation",
    "gdp",
    "retail sales",
    "oil",
    "wti",
    "crude",
    "brent",
    "opec",
    "inventory",
    "inventories",
    "energy",
    "geopolitical",
    "middle east",
    "war",
    "conflict",
    "dollar",
    "usd",
)

_CURRENT_NEWS_CUE_TERMS = (
    "today",
    "latest",
    "recent",
    "currently",
    "current",
    "news",
    "headline",
    "headlines",
    "update",
    "updates",
    "said",
    "say",
    "statement",
    "statements",
    "announced",
    "announcement",
    "posted",
    "post",
    "speaking",
    "speech",
    "remarks",
)

_CURRENT_NEWS_ENTITY_TERMS = (
    "trump",
    "biden",
    "white house",
    "president",
    "presidential",
    "election",
    "tariff",
    "tariffs",
    "congress",
    "senate",
    "house",
    "treasury",
)

_BOT_STATUS_TERMS = (
    "bot",
    "snapshot",
    "position",
    "positions",
    "trade",
    "trades",
    "balance",
    "p&l",
    "pnl",
    "cooldown",
    "cooldowns",
    "entry",
    "open position",
    "open positions",
)

_LOG_CONTEXT_TERMS = (
    "log",
    "logs",
    "error",
    "errors",
    "traceback",
    "server",
    "kamatera",
    "crash",
    "failed",
    "failure",
)

_ATTACHMENT_TERMS = (
    "image",
    "photo",
    "screenshot",
    "picture",
    "attachment",
    "chart",
    "posted",
    "upload",
)

_TRADE_EXECUTION_TERMS = (
    "stoploss",
    "stop loss",
    "stopped out",
    "hit stop",
    "hit tp",
    "tp1",
    "tp2",
    "take profit",
    "preclose",
    "pre-close",
    "flatten",
    "why did",
    "what happened",
)

_NEWS_QUERY_STOP_WORDS = {
    "a",
    "an",
    "and",
    "anything",
    "about",
    "are",
    "been",
    "can",
    "current",
    "did",
    "for",
    "has",
    "have",
    "headline",
    "headlines",
    "his",
    "how",
    "is",
    "latest",
    "me",
    "news",
    "of",
    "on",
    "or",
    "our",
    "post",
    "posted",
    "recent",
    "said",
    "say",
    "something",
    "statement",
    "statements",
    "tell",
    "that",
    "the",
    "their",
    "them",
    "they",
    "this",
    "today",
    "update",
    "updates",
    "was",
    "what",
    "when",
    "with",
}


def _clip_text(text: Any, limit: int = 600) -> str:
    clean = str(text or "").strip()
    if len(clean) <= limit:
        return clean
    return clean[: max(0, limit - 1)].rstrip() + "…"


def _json_text(value: Any, limit: int = 1600) -> str:
    try:
        text = json.dumps(value, ensure_ascii=True, default=str)
    except Exception:
        text = str(value)
    return _clip_text(text, limit)


def _parse_dt(value: Any) -> Optional[datetime]:
    if value in (None, ""):
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except Exception:
        return None


def _hours_ago(value: Any) -> Optional[float]:
    parsed = _parse_dt(value)
    if parsed is None:
        return None
    try:
        delta = datetime.now(timezone.utc) - parsed
        return max(0.0, delta.total_seconds() / 3600.0)
    except Exception:
        return None


def _summarize_position(position: Dict[str, Any]) -> Dict[str, Any]:
    metadata = position.get("metadata") if isinstance(position.get("metadata"), dict) else {}
    summary = {
        "trade_id": str(position.get("trade_id") or ""),
        "asset": str(position.get("asset") or position.get("canonical_asset") or ""),
        "direction": str(position.get("direction") or position.get("signal") or "HOLD").upper(),
        "entry_price": position.get("entry_price"),
        "current_price": position.get("current_price"),
        "pnl": position.get("pnl"),
        "confidence": position.get("confidence"),
        "entry_time": position.get("entry_time") or position.get("open_time"),
        "hours_open": round(v, 2) if (v := _hours_ago(position.get("entry_time") or position.get("open_time"))) is not None else None,
    }
    strategy_id = metadata.get("strategy_id") or position.get("strategy_id")
    if strategy_id not in (None, ""):
        summary["strategy_id"] = strategy_id
    regime = metadata.get("regime") or position.get("regime")
    if regime not in (None, ""):
        summary["regime"] = regime
    return summary


def _summarize_closed_trade(trade: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "trade_id": str(trade.get("trade_id") or ""),
        "asset": str(trade.get("asset") or trade.get("canonical_asset") or ""),
        "direction": str(trade.get("direction") or trade.get("signal") or "HOLD").upper(),
        "pnl": trade.get("pnl"),
        "exit_reason": trade.get("display_exit_reason") or trade.get("exit_reason"),
        "entry_time": trade.get("entry_time") or trade.get("open_time"),
        "exit_time": trade.get("exit_time"),
    }


def _summarize_cooldowns(cooldowns: Dict[str, int], limit: int = 5) -> Dict[str, Any]:
    items = list(cooldowns.items())
    items.sort(key=lambda item: str(item[0]))
    trimmed = items[: max(0, limit)]
    return {str(k): int(v) for k, v in trimmed}


def _build_asset_alias_patterns() -> List[tuple[re.Pattern[str], str]]:
    patterns: List[tuple[re.Pattern[str], str]] = []
    seen: set[tuple[str, str]] = set()

    for canonical, _category in registry.all_assets():
        aliases = {canonical.upper(), canonical.replace("/", "").replace("-", "").upper()}
        aliases.update(alias.upper() for alias in registry.all_aliases_for(canonical))
        for alias in aliases:
            clean = str(alias or "").strip().upper()
            if not clean:
                continue
            pattern = re.compile(rf"(?<![A-Z0-9]){re.escape(clean)}(?![A-Z0-9])")
            marker = (pattern.pattern, canonical)
            if marker in seen:
                continue
            seen.add(marker)
            patterns.append((pattern, canonical))

    patterns.sort(key=lambda item: len(item[0].pattern), reverse=True)
    return patterns


_ASSET_ALIAS_PATTERNS = _build_asset_alias_patterns()


def _resolve_asset_from_text(question: str, fallback: str = "") -> str:
    upper = str(question or "").upper()
    for pattern, canonical in _ASSET_ALIAS_PATTERNS:
        if pattern.search(upper):
            return canonical
    candidate = registry.canonical(str(fallback or "").strip())
    return candidate if registry.is_known(candidate) else ""


def _question_needs_macro_context(question: str) -> bool:
    text = str(question or "").lower()
    return any(term in text for term in _MACRO_QUESTION_TERMS)


def _question_needs_wti_context(question: str) -> bool:
    text = str(question or "").lower()
    return any(term in text for term in ("oil", "wti", "crude", "brent", "opec", "inventory", "inventories", "energy"))


def _question_needs_current_news_context(question: str) -> bool:
    text = str(question or "").lower()
    if not text:
        return False
    if any(term in text for term in _BOT_STATUS_TERMS) and not any(term in text for term in _CURRENT_NEWS_ENTITY_TERMS):
        return False
    return any(term in text for term in _CURRENT_NEWS_CUE_TERMS) or any(term in text for term in _CURRENT_NEWS_ENTITY_TERMS)


def _question_needs_log_context(question: str) -> bool:
    text = str(question or "").lower()
    return any(term in text for term in _LOG_CONTEXT_TERMS)


def _question_mentions_attachment(question: str) -> bool:
    text = str(question or "").lower()
    return any(term in text for term in _ATTACHMENT_TERMS)


def _question_needs_trade_execution_context(question: str) -> bool:
    text = str(question or "").lower()
    return any(term in text for term in _TRADE_EXECUTION_TERMS)


def _derive_news_query(question: str) -> str:
    tokens = re.findall(r"[a-z0-9][a-z0-9'-]*", str(question or "").lower())
    query_terms: List[str] = []
    for token in tokens:
        clean = token.strip("-'")
        if not clean or clean.isdigit():
            continue
        if clean in _NEWS_QUERY_STOP_WORDS:
            continue
        if len(clean) < 3:
            continue
        if clean not in query_terms:
            query_terms.append(clean)
        if len(query_terms) >= 6:
            break
    return " ".join(query_terms)


def _summarize_macro_event(event: Dict[str, Any]) -> Dict[str, Any]:
    raw_time = event.get("time") or event.get("date") or event.get("datetime")
    return {
        "time_local": format_display_datetime(raw_time, "%Y-%m-%d %H:%M", include_tz=False, default="") if raw_time else "",
        "event": str(event.get("event") or event.get("name") or "").strip(),
        "impact": str(event.get("impact") or "").upper(),
        "actual": event.get("actual"),
        "estimate": event.get("estimate") or event.get("forecast"),
        "surprise_direction": str(event.get("surprise_direction") or "").strip(),
        "source": str(event.get("source") or "").strip(),
    }


def _summarize_macro_article(article: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "title": _clip_text(article.get("title") or "", 140),
        "source": _clip_text(article.get("source") or "", 60),
        "date": str(article.get("date") or ""),
        "sentiment": article.get("sentiment"),
    }


def _summarize_current_news_article(article: Dict[str, Any]) -> Dict[str, Any]:
    published = article.get("publishedAt") or article.get("published_at") or article.get("published")
    return {
        "title": _clip_text(article.get("title") or "", 160),
        "source": _clip_text(article.get("source") or "", 60),
        "published_local": format_display_datetime(published, "%Y-%m-%d %H:%M", include_tz=False, default="") if published else "",
        "summary": _clip_text(article.get("description") or article.get("summary") or "", 220),
        "url": _clip_text(article.get("url") or "", 180),
    }


def _article_matches_macro(article: Dict[str, Any]) -> bool:
    text = f"{article.get('title') or ''} {article.get('source') or ''}".lower()
    return any(term in text for term in _MACRO_NEWS_TERMS)


def _build_macro_snapshot(question: str) -> Dict[str, Any]:
    cache_key = "macro:wti" if _question_needs_wti_context(question) else "macro:general"
    now = time.time()
    with _MACRO_SNAPSHOT_LOCK:
        hit = _MACRO_SNAPSHOT_CACHE.get(cache_key)
        if hit and now < hit[1]:
            return dict(hit[0])

    snapshot: Dict[str, Any] = {
        "available": False,
        "source": "market_calendar_and_news",
        "display_now_local": now_in_display_timezone().strftime(f"%Y-%m-%d %H:%M:%S {display_timezone_label()}"),
        "question_scope": "macro" if _question_needs_macro_context(question) else "general",
        "high_impact_events": [],
        "upcoming_events": [],
        "macro_headlines": [],
        "risk_outlook": {},
        "wti_intelligence": {},
    }

    event_count = 0
    headline_count = 0
    source_count = 0

    try:
        from market_calendar import MarketCalendar

        calendar = MarketCalendar()
        risk_outlook = calendar.should_reduce_risk() or {}
        high_impact_events = calendar.get_high_impact_events(days=3) or []
        snapshot["risk_outlook"] = risk_outlook
        snapshot["high_impact_events"] = [_summarize_macro_event(item) for item in high_impact_events[:6]]
        event_count += len(snapshot["high_impact_events"])
        if high_impact_events:
            source_count += 1
    except Exception as exc:
        snapshot["calendar_error"] = str(exc)

    try:
        from data_ingestion.news_event_monitor import news_monitor

        now_utc = datetime.now(timezone.utc)
        upcoming_events = []
        for event in news_monitor.upcoming_events(hours=72) or []:
            event_time = event.get("time")
            if not isinstance(event_time, datetime):
                continue
            if event_time.tzinfo is None:
                event_time = event_time.replace(tzinfo=timezone.utc)
            if event_time < now_utc:
                continue
            if event_time > now_utc + timedelta(hours=72):
                continue
            impact = str(event.get("impact") or "").upper()
            if impact not in {"HIGH", "MEDIUM"}:
                continue
            upcoming_events.append(_summarize_macro_event(event))
        snapshot["upcoming_events"] = upcoming_events[:6]
        if upcoming_events:
            event_count += len(snapshot["upcoming_events"])
            source_count += 1
    except Exception as exc:
        snapshot["news_event_error"] = str(exc)

    try:
        from services.sentiment_sources import _NewsSentiment

        articles = _NewsSentiment.get_articles_for_dashboard(limit=20) or []
        relevant_articles = [article for article in articles if _article_matches_macro(article)]
        selected_articles = relevant_articles[:6] if relevant_articles else articles[:5]
        snapshot["macro_headlines"] = [_summarize_macro_article(item) for item in selected_articles]
        if selected_articles:
            headline_count = len(snapshot["macro_headlines"])
            source_count += 1
    except Exception as exc:
        snapshot["news_error"] = str(exc)

    if _question_needs_wti_context(question):
        try:
            from services.free_market_intelligence import free_market_intelligence

            wti_context = free_market_intelligence.get_asset_context("WTI", "commodities") or {}
            snapshot["wti_intelligence"] = {
                "score": wti_context.get("score", 0.0),
                "components": wti_context.get("components", {}),
                "sources": wti_context.get("sources", []),
                "details": {
                    "macro": (wti_context.get("details") or {}).get("macro", {}),
                    "eia": (wti_context.get("details") or {}).get("eia", {}),
                    "cftc": (wti_context.get("details") or {}).get("cftc", {}),
                },
            }
            if wti_context:
                source_count += 1
        except Exception as exc:
            snapshot["wti_error"] = str(exc)

    snapshot["available"] = any(
        bool(snapshot.get(key))
        for key in ("high_impact_events", "upcoming_events", "macro_headlines", "wti_intelligence")
    )
    snapshot["summary"] = {
        "event_count": event_count,
        "headline_count": headline_count,
        "source_count": source_count,
        "wti_requested": _question_needs_wti_context(question),
    }

    with _MACRO_SNAPSHOT_LOCK:
        _MACRO_SNAPSHOT_CACHE[cache_key] = (dict(snapshot), now + _MACRO_SNAPSHOT_TTL_SECONDS)
    return snapshot


def _build_current_news_snapshot(question: str) -> Dict[str, Any]:
    query = _derive_news_query(question)
    cache_key = f"news:{query or 'empty'}"
    now = time.time()
    with _CURRENT_NEWS_SNAPSHOT_LOCK:
        hit = _CURRENT_NEWS_SNAPSHOT_CACHE.get(cache_key)
        if hit and now < hit[1]:
            return dict(hit[0])

    snapshot: Dict[str, Any] = {
        "available": False,
        "source": "question_news_search",
        "display_now_local": now_in_display_timezone().strftime(f"%Y-%m-%d %H:%M:%S {display_timezone_label()}"),
        "query": query,
        "articles": [],
        "providers": [],
    }

    if not ROBBIE_CHAT_NEWS_ENABLED:
        snapshot["message"] = "Configured news feeds are disabled."
        return snapshot
    if not query:
        snapshot["message"] = "Could not derive a focused news search query from the question."
        return snapshot

    articles: List[Dict[str, Any]] = []
    seen_titles: set[str] = set()
    query_terms = [term for term in query.split() if term]
    since_utc = datetime.now(timezone.utc) - timedelta(days=3)

    def _keep_article(title: str, description: str) -> bool:
        haystack = f"{title} {description}".lower()
        return not query_terms or any(term in haystack for term in query_terms)

    if NEWSAPI_KEY:
        try:
            resp = requests.get(
                "https://newsapi.org/v2/everything",
                params={
                    "q": query,
                    "language": "en",
                    "sortBy": "publishedAt",
                    "pageSize": 6,
                    "from": since_utc.date().isoformat(),
                    "apiKey": NEWSAPI_KEY,
                },
                timeout=10,
            )
            if resp.status_code == 200:
                snapshot["providers"].append("newsapi")
                for item in resp.json().get("articles", []):
                    title = str(item.get("title") or "").strip()
                    description = str(item.get("description") or "").strip()
                    if not title or not _keep_article(title, description):
                        continue
                    normalized = title.lower()
                    if normalized in seen_titles:
                        continue
                    published = _parse_dt(item.get("publishedAt"))
                    if published and published < since_utc:
                        continue
                    seen_titles.add(normalized)
                    articles.append(
                        _summarize_current_news_article(
                            {
                                "title": title,
                                "source": (item.get("source") or {}).get("name", ""),
                                "publishedAt": item.get("publishedAt"),
                                "description": description,
                                "url": item.get("url"),
                            }
                        )
                    )
                    if len(articles) >= 6:
                        break
        except Exception as exc:
            snapshot["newsapi_error"] = str(exc)

    if len(articles) < 6 and GNEWS_KEY:
        try:
            resp = requests.get(
                "https://gnews.io/api/v4/search",
                params={"q": query, "lang": "en", "max": 6, "token": GNEWS_KEY},
                timeout=10,
            )
            if resp.status_code == 200:
                snapshot["providers"].append("gnews")
                for item in resp.json().get("articles", []):
                    title = str(item.get("title") or "").strip()
                    description = str(item.get("description") or "").strip()
                    if not title or not _keep_article(title, description):
                        continue
                    normalized = title.lower()
                    if normalized in seen_titles:
                        continue
                    published = _parse_dt(item.get("publishedAt"))
                    if published and published < since_utc:
                        continue
                    seen_titles.add(normalized)
                    articles.append(
                        _summarize_current_news_article(
                            {
                                "title": title,
                                "source": (item.get("source") or {}).get("name", ""),
                                "publishedAt": item.get("publishedAt"),
                                "description": description,
                                "url": item.get("url"),
                            }
                        )
                    )
                    if len(articles) >= 6:
                        break
        except Exception as exc:
            snapshot["gnews_error"] = str(exc)

    snapshot["articles"] = articles[:6]
    snapshot["available"] = bool(snapshot["articles"])
    snapshot["summary"] = {
        "article_count": len(snapshot["articles"]),
        "provider_count": len(snapshot["providers"]),
        "query": query,
    }
    if not snapshot["available"]:
        snapshot["message"] = "No recent headlines matched the question in the configured news feeds."

    with _CURRENT_NEWS_SNAPSHOT_LOCK:
        _CURRENT_NEWS_SNAPSHOT_CACHE[cache_key] = (dict(snapshot), now + _CURRENT_NEWS_SNAPSHOT_TTL_SECONDS)
    return snapshot


def _build_bot_snapshot() -> Dict[str, Any]:
    try:
        from core.state import state as shared_state

        try:
            shared_state.init_db()
        except Exception:
            pass

        performance = dict(shared_state.get_performance() or {})
        open_positions = list(shared_state.get_open_positions() or [])
        closed_trades = rollup_closed_trade_history(
            list(shared_state.get_closed_positions(limit=20) or []),
            limit=10,
        )
        cooldowns = dict(shared_state.get_all_cooldowns() or {})
        last_entry_time = shared_state.get_last_entry_time()
        last_entry_local = None
        if last_entry_time is not None:
            try:
                last_entry_local = last_entry_time.astimezone(timezone.utc).isoformat()
            except Exception:
                last_entry_local = str(last_entry_time)

        return {
            "available": True,
            "source": "live_shared_state",
            "display_now_local": now_in_display_timezone().strftime(f"%Y-%m-%d %H:%M:%S {display_timezone_label()}"),
            "balance": round(float(performance.get("balance", shared_state.balance) or 0.0), 2),
            "daily_pnl": round(float(performance.get("daily_pnl", shared_state.daily_pnl) or 0.0), 2),
            "daily_trades": int(performance.get("daily_trades", shared_state.daily_trades) or 0),
            "total_trades": int(performance.get("total_trades", 0) or 0),
            "win_rate": round(float(performance.get("win_rate", 0.0) or 0.0), 2),
            "total_pnl": round(float(performance.get("total_pnl", 0.0) or 0.0), 2),
            "open_positions_count": int(performance.get("open_positions", len(open_positions)) or len(open_positions)),
            "hours_since_last_entry": round(v, 2) if (v := shared_state.hours_since_last_entry(now_in_display_timezone())) is not None else None,
            "last_entry_time_utc": last_entry_local,
            "cooldowns": _summarize_cooldowns(cooldowns),
            "open_positions": [_summarize_position(pos) for pos in open_positions[:5]],
            "recent_closed_trades": [_summarize_closed_trade(trade) for trade in closed_trades[:10]],
        }
    except Exception as exc:
        return {
            "available": False,
            "source": "live_shared_state",
            "error": str(exc),
        }


def _tail_log_lines(path: Path, limit: int = 12) -> List[str]:
    if limit <= 0 or not path.exists():
        return []
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return []
    lines = [line.rstrip() for line in text.splitlines() if str(line).strip()]
    return lines[-limit:]


def _build_log_snapshot(question: str, *, focus_asset: str = "") -> Dict[str, Any]:
    focus = str(focus_asset or "").strip().upper()
    focus_tokens = {focus, focus.replace("/", ""), focus.replace("-", "")} if focus else set()
    logs_dir = Path("logs")
    result: Dict[str, Any] = {
        "available": False,
        "source": "local_log_tail",
        "display_now_local": now_in_display_timezone().strftime(f"%Y-%m-%d %H:%M:%S {display_timezone_label()}"),
        "focus_asset": focus_asset,
        "errors": _tail_log_lines(logs_dir / "errors.log", limit=8),
        "trades": _tail_log_lines(logs_dir / "trades.log", limit=8),
        "engine": _tail_log_lines(logs_dir / "trading_bot.log", limit=12),
        "asset_matches": [],
    }
    if focus_tokens:
        asset_matches: List[str] = []
        for line in reversed(result["trades"] + result["engine"]):
            upper = str(line or "").upper()
            if any(token and token in upper for token in focus_tokens):
                asset_matches.append(str(line))
            if len(asset_matches) >= 8:
                break
        result["asset_matches"] = list(reversed(asset_matches))

    result["available"] = any(bool(result.get(key)) for key in ("errors", "trades", "engine", "asset_matches"))
    if not result["available"]:
        result["message"] = "No local log tail was available."
    return result


def _build_focus_asset_snapshot(asset: str) -> Dict[str, Any]:
    canonical = registry.canonical(str(asset or "").strip())
    if not canonical or not registry.is_known(canonical):
        return {"available": False, "asset": canonical, "message": "No canonical focus asset could be resolved."}

    category = registry.category(canonical)
    snapshot: Dict[str, Any] = {
        "available": True,
        "asset": canonical,
        "category": category,
        "source": "live_asset_snapshot",
        "display_now_local": now_in_display_timezone().strftime(f"%Y-%m-%d %H:%M:%S {display_timezone_label()}"),
        "market_status": {},
        "live_quote": {},
        "recent_5m": {},
        "recent_closed_trades": [],
        "open_positions": [],
        "market_intelligence": {},
    }

    try:
        from services.market_data_router import get_market_status

        status = get_market_status(canonical, category=category)
        if isinstance(status, dict):
            snapshot["market_status"] = dict(status)
    except Exception:
        snapshot["market_status"] = {}

    try:
        from data.fetcher import get_shared_fetcher

        fetcher = get_shared_fetcher()
        price, spread = fetcher.get_real_time_price(
            canonical,
            category,
            prefer_live_stream=True,
            allow_cached_quote=False,
        )
        quote_meta = dict(fetcher.get_last_price_metadata(canonical) or {})
        if price is not None:
            snapshot["live_quote"] = {
                "price": round(float(price), 6),
                "spread": round(float(spread or 0.0), 6),
                "source": quote_meta.get("source"),
                "realtime": quote_meta.get("realtime"),
                "quote_freshness": quote_meta.get("quote_freshness"),
                "live_age_seconds": quote_meta.get("live_age_seconds"),
                "as_of_utc": quote_meta.get("as_of_utc"),
            }

        bars = fetcher.get_ohlcv(
            canonical,
            category,
            interval="5m",
            periods=24,
            closed_only=True,
            prefer_local=True,
        )
        bars_meta = dict(fetcher.get_last_ohlcv_metadata(canonical, "5m") or {})
        if bars is not None and not bars.empty:
            latest_close = float(bars["close"].iloc[-1])
            prev_close = float(bars["close"].iloc[-2]) if len(bars) >= 2 else latest_close
            sixth_close = float(bars["close"].iloc[-6]) if len(bars) >= 6 else prev_close
            change_1 = ((latest_close - prev_close) / prev_close * 100.0) if prev_close else 0.0
            change_6 = ((latest_close - sixth_close) / sixth_close * 100.0) if sixth_close else 0.0
            snapshot["recent_5m"] = {
                "bars": int(len(bars)),
                "latest_close": round(latest_close, 6),
                "prev_close": round(prev_close, 6),
                "change_pct_last_bar": round(change_1, 4),
                "change_pct_last_6_bars": round(change_6, 4),
                "session_high": round(float(bars["high"].max()), 6),
                "session_low": round(float(bars["low"].min()), 6),
                "source": bars_meta.get("source"),
                "as_of_utc": bars_meta.get("as_of_utc"),
            }
    except Exception as exc:
        snapshot["quote_error"] = str(exc)

    try:
        from core.state import state as shared_state

        open_positions = [
            pos for pos in list(shared_state.get_open_positions() or [])
            if registry.canonical(str(pos.get("asset") or pos.get("canonical_asset") or "")) == canonical
        ]
        closed_trades = rollup_closed_trade_history(
            list(shared_state.get_closed_positions(limit=40) or []),
            limit=20,
        )
        asset_trades = [
            trade for trade in closed_trades
            if registry.canonical(str(trade.get("asset") or trade.get("canonical_asset") or "")) == canonical
        ]
        snapshot["open_positions"] = [_summarize_position(pos) for pos in open_positions[:4]]
        snapshot["recent_closed_trades"] = [_summarize_closed_trade(trade) for trade in asset_trades[:6]]
    except Exception as exc:
        snapshot["trade_error"] = str(exc)

    try:
        from services.market_intelligence_service import get_service as get_market_intelligence_service

        intelligence = get_market_intelligence_service().get_asset_snapshot(canonical, category) or {}
        snapshot["market_intelligence"] = {
            "score": intelligence.get("market_intelligence_score"),
            "sources": list(intelligence.get("market_intelligence_sources") or []),
            "timestamp": intelligence.get("market_intelligence_timestamp") or intelligence.get("intelligence_timestamp"),
            "free_market_intelligence": intelligence.get("free_market_intelligence") or {},
        }
    except Exception:
        snapshot["market_intelligence"] = {}

    return snapshot


class ChatSessionStore:
    def __init__(self, path: Path | str = _SESSION_FILE):
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._sessions: Dict[str, Dict[str, Any]] = {}
        self._load()

    def _load(self) -> None:
        with self._lock:
            if not self._path.exists():
                self._sessions = {}
                return
            try:
                payload = json.loads(self._path.read_text(encoding="utf-8"))
                sessions = payload.get("sessions") if isinstance(payload, dict) else {}
                self._sessions = sessions if isinstance(sessions, dict) else {}
            except Exception:
                self._sessions = {}

    def _persist(self) -> None:
        tmp = self._path.with_suffix(".tmp")
        payload = {
            "sessions": self._sessions,
            "updated_at": int(time.time()),
        }
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        tmp.replace(self._path)

    def get(self, chat_id: str) -> Dict[str, Any]:
        with self._lock:
            key = str(chat_id)
            session = dict(self._sessions.get(key, {}) or {})
            session.setdefault("messages", [])
            session.setdefault("updated_at", 0)
            session.setdefault("last_asset", "")
            session.setdefault("last_attachment", {})
            return session

    def append_turn(
        self,
        chat_id: str,
        *,
        user_message: str,
        assistant_message: str,
        last_asset: str = "",
    ) -> None:
        with self._lock:
            key = str(chat_id)
            session = dict(self._sessions.get(key, {}) or {})
            messages = list(session.get("messages") or [])
            messages.append({"role": "user", "content": str(user_message or "")})
            messages.append({"role": "assistant", "content": str(assistant_message or "")})
            session["messages"] = messages[-_MAX_HISTORY_MESSAGES:]
            if last_asset:
                session["last_asset"] = str(last_asset)
            session["updated_at"] = int(time.time())
            self._sessions[key] = session
            self._persist()

    def set_attachment(self, chat_id: str, attachment: Dict[str, Any]) -> None:
        with self._lock:
            key = str(chat_id)
            session = dict(self._sessions.get(key, {}) or {})
            session["messages"] = list(session.get("messages") or [])
            session["last_attachment"] = dict(attachment or {})
            session["updated_at"] = int(time.time())
            self._sessions[key] = session
            self._persist()

    def reset(self, chat_id: str) -> None:
        with self._lock:
            self._sessions.pop(str(chat_id), None)
            self._persist()


class DeepSeekChatService:
    def __init__(self, *, session_store: Optional[ChatSessionStore] = None):
        self._sessions = session_store or ChatSessionStore()

    def reset(self, chat_id: str) -> None:
        self._sessions.reset(chat_id)

    def answer(self, *, question: str, chat_id: str, attachment: Optional[Dict[str, Any]] = None) -> str:
        prompt = str(question or "").strip()
        if not prompt:
            return "Send me a message and I will answer it."

        if isinstance(attachment, dict) and attachment:
            self._sessions.set_attachment(chat_id, attachment)
        session = self._sessions.get(chat_id)
        focus_asset = _resolve_asset_from_text(prompt, session.get("last_asset", ""))
        response = self._answer_via_deepseek(prompt, session, focus_asset=focus_asset)
        self._sessions.append_turn(
            chat_id,
            user_message=prompt,
            assistant_message=response,
            last_asset=focus_asset,
        )
        return response

    def _system_prompt(self) -> str:
        local_now = now_in_display_timezone().strftime(f"%Y-%m-%d %H:%M:%S {display_timezone_label()}")
        return (
            "You are DeepSeek running as a dedicated private Telegram chat bot for the user's trading system. "
            "A current runtime bot snapshot may be provided in a separate system message. "
            "Use that snapshot first for questions about positions, balance, P&L, recent trades, cooldowns, and bot health. "
            "If a focus-asset runtime snapshot is provided, use it for questions about how gold, silver, indices, forex, or crypto are moving right now. "
            "If a second system message provides macro context, use it for questions about NFP, CPI, FOMC, oil, and current market events. "
            "If another system message provides a current news snapshot, use it for latest-headline or public-statement questions instead of claiming you cannot browse. "
            "If another system message provides recent local log tails, use them for operational or trade-explanation questions instead of claiming you cannot access logs. "
            "If another system message provides a recent Telegram attachment summary, use that extracted text or metadata instead of claiming you cannot see images. "
            "Do not claim access to menus or controls. "
            "If OCR text is missing from an attachment, say that only metadata or caption was available instead of pretending the image was unreadable or unseen. "
            "If a detail is missing from the runtime facts, say it is not available instead of inventing it. "
            "Answer naturally and directly. "
            f"Current local time is {local_now}. "
            f"Use {display_timezone_label()} for relative date references."
        )

    @staticmethod
    def _bot_context_prompt(snapshot: Dict[str, Any]) -> str:
        return (
            "Current bot runtime snapshot from shared in-process state. "
            "Treat this as the current known bot state for trading-bot questions, but not as a live control surface. "
            "Do not invent anything beyond it.\n"
            f"{_json_text(snapshot, 3500)}"
        )

    @staticmethod
    def _focus_asset_context_prompt(snapshot: Dict[str, Any]) -> str:
        return (
            "Current focus-asset runtime snapshot. "
            "Use this for questions about how a specific market is behaving right now, recent trade outcomes, and live quote behavior. "
            "Do not invent prices or fills beyond it.\n"
            f"{_json_text(snapshot, 3600)}"
        )

    @staticmethod
    def _macro_context_prompt(snapshot: Dict[str, Any]) -> str:
        return (
            "Read-only macro snapshot for current market context. "
            "Use this for questions about NFP, CPI, Fed policy, oil, and other market-moving events. "
            "Do not invent breaking news beyond this snapshot.\n"
            f"{_json_text(snapshot, 3500)}"
        )

    @staticmethod
    def _current_news_context_prompt(snapshot: Dict[str, Any]) -> str:
        return (
            "Read-only current news snapshot for the user's question. "
            "Use only these fetched headlines for latest-news or what-did-they-say questions. "
            "If no matching articles are present, say that no current headline match was found in the configured feeds.\n"
            f"{_json_text(snapshot, 3500)}"
        )

    @staticmethod
    def _log_context_prompt(snapshot: Dict[str, Any]) -> str:
        return (
            "Recent local log tail from the bot host. "
            "Use this for questions about server issues, failures, fills, or why something closed. "
            "Do not claim live shell access beyond these lines.\n"
            f"{_json_text(snapshot, 3200)}"
        )

    @staticmethod
    def _attachment_context_prompt(snapshot: Dict[str, Any]) -> str:
        return (
            "Recent Telegram attachment summary. "
            "Use extracted text, caption, and metadata from the user's recent image or document when relevant. "
            "If OCR text is empty, say only metadata/caption was available.\n"
            f"{_json_text(snapshot, 2200)}"
        )

    def _answer_via_deepseek(self, question: str, session: Dict[str, Any], *, focus_asset: str = "") -> str:
        if not DEEPSEEK_API_KEY:
            raise RuntimeError("DEEPSEEK_API_KEY is not configured")

        base_url = str(ROBBIE_CHAT_BASE_URL or "https://api.deepseek.com").rstrip("/")
        endpoint = f"{base_url}/chat/completions"
        history = list(session.get("messages") or [])[-_MAX_HISTORY_MESSAGES:]
        bot_snapshot = _build_bot_snapshot()
        messages = [
            {"role": "system", "content": self._system_prompt()},
            {"role": "system", "content": self._bot_context_prompt(bot_snapshot)},
        ]
        if focus_asset:
            focus_snapshot = _build_focus_asset_snapshot(focus_asset)
            messages.append({"role": "system", "content": self._focus_asset_context_prompt(focus_snapshot)})
        if _question_needs_macro_context(question):
            macro_snapshot = _build_macro_snapshot(question)
            messages.append({"role": "system", "content": self._macro_context_prompt(macro_snapshot)})
        if _question_needs_current_news_context(question):
            current_news_snapshot = _build_current_news_snapshot(question)
            messages.append({"role": "system", "content": self._current_news_context_prompt(current_news_snapshot)})
        if _question_needs_log_context(question) or (focus_asset and _question_needs_trade_execution_context(question)):
            log_snapshot = _build_log_snapshot(question, focus_asset=focus_asset)
            messages.append({"role": "system", "content": self._log_context_prompt(log_snapshot)})
        last_attachment = dict(session.get("last_attachment") or {})
        if last_attachment and _question_mentions_attachment(question):
            messages.append({"role": "system", "content": self._attachment_context_prompt(last_attachment)})
        messages.extend(
            [
                *history,
                {"role": "user", "content": question},
            ]
        )
        payload = {
            "model": str(ROBBIE_CHAT_MODEL or "deepseek-chat"),
            "messages": messages,
            "temperature": max(0.0, min(1.1, float(ROBBIE_CHAT_TEMPERATURE or 0.35))),
            "max_tokens": max(300, min(2000, int(ROBBIE_CHAT_MAX_TOKENS or 1100))),
        }
        try:
            logger.debug(
                "[DeepSeekChat] sending prompt",
                extra={
                    "endpoint": endpoint,
                    "history_len": len(history),
                    "payload_preview": _json_text(payload, 1000),
                },
            )
        except Exception:
            pass

        resp = requests.post(
            endpoint,
            headers={
                "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=float(ROBBIE_CHAT_TIMEOUT_SECONDS or 20),
        )
        resp.raise_for_status()
        body = resp.json() if resp.content else {}
        choices = body.get("choices") if isinstance(body, dict) else []
        if not isinstance(choices, list) or not choices:
            return "DeepSeek returned no answer."
        message = choices[0].get("message") if isinstance(choices[0], dict) else {}
        content = str((message or {}).get("content") or "").strip()
        return content or "DeepSeek returned an empty answer."


_deepseek_chat_service: Optional[DeepSeekChatService] = None
_deepseek_chat_service_lock = threading.Lock()


def get_deepseek_chat_service() -> DeepSeekChatService:
    global _deepseek_chat_service
    with _deepseek_chat_service_lock:
        if _deepseek_chat_service is None:
            _deepseek_chat_service = DeepSeekChatService()
        return _deepseek_chat_service


__all__ = ["ChatSessionStore", "DeepSeekChatService", "get_deepseek_chat_service"]
