from __future__ import annotations

import json
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

from config.config import (
    DEEPSEEK_API_KEY,
    ROBBIE_CHAT_BASE_URL,
    ROBBIE_CHAT_HISTORY_LIMIT,
    ROBBIE_CHAT_MAX_TOKENS,
    ROBBIE_CHAT_MODEL,
    ROBBIE_CHAT_TEMPERATURE,
    ROBBIE_CHAT_TIMEOUT_SECONDS,
)
from utils.display_time import display_timezone_label, format_display_datetime, now_in_display_timezone
from utils.logger import get_logger

logger = get_logger()

_SESSION_FILE = Path("data/deepseek_chat_sessions.json")
_SESSION_FILE.parent.mkdir(parents=True, exist_ok=True)
_MAX_HISTORY_MESSAGES = max(2, int(ROBBIE_CHAT_HISTORY_LIMIT or 10)) * 2
_MACRO_SNAPSHOT_TTL_SECONDS = 300.0
_MACRO_SNAPSHOT_CACHE: Dict[str, tuple[Dict[str, Any], float]] = {}
_MACRO_SNAPSHOT_LOCK = threading.Lock()

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


def _question_needs_macro_context(question: str) -> bool:
    text = str(question or "").lower()
    return any(term in text for term in _MACRO_QUESTION_TERMS)


def _question_needs_wti_context(question: str) -> bool:
    text = str(question or "").lower()
    return any(term in text for term in ("oil", "wti", "crude", "brent", "opec", "inventory", "inventories", "energy"))


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


def _build_bot_snapshot() -> Dict[str, Any]:
    try:
        from core.state import state as shared_state

        try:
            shared_state.init_db()
        except Exception:
            pass

        performance = dict(shared_state.get_performance() or {})
        open_positions = list(shared_state.get_open_positions() or [])
        closed_trades = list(shared_state.get_closed_positions(limit=5) or [])
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
            "source": "read_only_persisted_state",
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
            "recent_closed_trades": [_summarize_closed_trade(trade) for trade in closed_trades[:5]],
        }
    except Exception as exc:
        return {
            "available": False,
            "source": "read_only_persisted_state",
            "error": str(exc),
        }


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
            return session

    def append_turn(self, chat_id: str, *, user_message: str, assistant_message: str) -> None:
        with self._lock:
            key = str(chat_id)
            session = dict(self._sessions.get(key, {}) or {})
            messages = list(session.get("messages") or [])
            messages.append({"role": "user", "content": str(user_message or "")})
            messages.append({"role": "assistant", "content": str(assistant_message or "")})
            session["messages"] = messages[-_MAX_HISTORY_MESSAGES:]
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

    def answer(self, *, question: str, chat_id: str) -> str:
        prompt = str(question or "").strip()
        if not prompt:
            return "Send me a message and I will answer it."

        session = self._sessions.get(chat_id)
        response = self._answer_via_deepseek(prompt, session)
        self._sessions.append_turn(
            chat_id,
            user_message=prompt,
            assistant_message=response,
        )
        return response

    def _system_prompt(self) -> str:
        local_now = now_in_display_timezone().strftime(f"%Y-%m-%d %H:%M:%S {display_timezone_label()}")
        return (
            "You are DeepSeek running as a dedicated private Telegram chat bot for the user's trading system. "
            "A read-only snapshot of the latest persisted bot state is provided in a separate system message. "
            "Use that snapshot first for questions about positions, balance, P&L, recent trades, cooldowns, and bot health. "
            "If a second system message provides macro context, use it for questions about NFP, CPI, FOMC, oil, and current market events. "
            "Do not claim access to menus or controls. "
            "If a detail is missing from the snapshot, say it is not available instead of inventing it. "
            "Answer naturally and directly. "
            f"Current local time is {local_now}. "
            f"Use {display_timezone_label()} for relative date references."
        )

    @staticmethod
    def _bot_context_prompt(snapshot: Dict[str, Any]) -> str:
        return (
            "Read-only bot snapshot from the latest persisted trading state. "
            "Treat this as the current known bot state for trading-bot questions, but not as a live control surface. "
            "Do not invent anything beyond it.\n"
            f"{_json_text(snapshot, 3500)}"
        )

    @staticmethod
    def _macro_context_prompt(snapshot: Dict[str, Any]) -> str:
        return (
            "Read-only macro snapshot for current market context. "
            "Use this for questions about NFP, CPI, Fed policy, oil, and other market-moving events. "
            "Do not invent breaking news beyond this snapshot.\n"
            f"{_json_text(snapshot, 3500)}"
        )

    def _answer_via_deepseek(self, question: str, session: Dict[str, Any]) -> str:
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
        if _question_needs_macro_context(question):
            macro_snapshot = _build_macro_snapshot(question)
            messages.append({"role": "system", "content": self._macro_context_prompt(macro_snapshot)})
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
