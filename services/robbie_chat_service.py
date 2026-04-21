from __future__ import annotations

import json
import re
import threading
import time
from datetime import datetime, timedelta, timezone
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

from config.config import (
    DEEPSEEK_API_KEY,
    LEARNING_HISTORY_LIMIT,
    ROBBIE_CHAT_ALLOW_WORLD_KNOWLEDGE,
    ROBBIE_CHAT_BASE_URL,
    ROBBIE_CHAT_CLOSED_TRADES_LIMIT,
    ROBBIE_CHAT_CONTEXT_CHAR_LIMIT,
    ROBBIE_CHAT_HISTORY_LIMIT,
    ROBBIE_CHAT_INCLUDE_LOCAL_DRAFT,
    ROBBIE_CHAT_MAX_TOKENS,
    ROBBIE_CHAT_MARKET_EVENT_LIMIT,
    ROBBIE_CHAT_MARKET_LOOKAHEAD_DAYS,
    ROBBIE_CHAT_MODEL,
    ROBBIE_CHAT_MODE,
    ROBBIE_CHAT_NEWS_ENABLED,
    ROBBIE_CHAT_NEWS_LIMIT,
    ROBBIE_CHAT_OPEN_POSITIONS_LIMIT,
    ROBBIE_CHAT_PROVIDER,
    ROBBIE_CHAT_TEMPERATURE,
    ROBBIE_CHAT_TIMEOUT_SECONDS,
    TOP_OPPORTUNITIES_LIMIT,
)
from core.asset_profiles import get_profile
from core.assets import registry
from core.state import rollup_closed_trade_history
from market_calendar import MarketCalendar
from services.market_hours_guard import build_market_status
from services.personality_service import RobbieExplainer, personality
from utils.logger import get_logger

logger = get_logger()

_SESSION_FILE = Path("data/robbie_chat_sessions.json")
_SESSION_FILE.parent.mkdir(exist_ok=True)
_MAX_HISTORY_MESSAGES = max(2, int(ROBBIE_CHAT_HISTORY_LIMIT or 6)) * 2
_ASSET_ALIAS_PATTERNS: List[Tuple[re.Pattern[str], str]] = []
_RUNTIME_FACT_INTENTS = {"issues", "learning", "stop_loss", "adjustment", "market", "positions", "trade_why"}
_WORLD_KNOWLEDGE_INTENTS = {"general", "macro", "forecast", "market", "trade_why", "stop_loss", "learning", "adjustment"}


def _build_asset_alias_patterns() -> List[Tuple[re.Pattern[str], str]]:
    patterns: List[Tuple[re.Pattern[str], str]] = []
    seen: set[tuple[str, str]] = set()

    for canonical, _category in registry.all_assets():
        aliases = {canonical.upper(), canonical.replace("/", "").replace("-", "").upper()}
        aliases.update(alias.upper() for alias in registry.all_aliases_for(canonical))
        for alias in aliases:
            clean = str(alias or "").strip().upper()
            if not clean:
                continue
            if clean.isalpha():
                pattern = re.compile(rf"(?<![A-Z0-9]){re.escape(clean)}(?![A-Z0-9])")
            else:
                pattern = re.compile(rf"(?<![A-Z0-9]){re.escape(clean)}(?![A-Z0-9])")
            marker = (pattern.pattern, canonical)
            if marker in seen:
                continue
            seen.add(marker)
            patterns.append((pattern, canonical))

    patterns.sort(key=lambda item: len(item[0].pattern), reverse=True)
    return patterns


_ASSET_ALIAS_PATTERNS = _build_asset_alias_patterns()


def _coerce_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


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


def _section_text(title: str, value: Any, limit: int = 1600) -> str:
    return f"{title}:\n{_json_text(value, limit)}"


def _humanize_token(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    return " ".join(text.replace("_", " ").replace("-", " ").split())


def _price_text(value: Any) -> str:
    try:
        num = float(value or 0.0)
    except Exception:
        return "n/a"
    if num == 0.0:
        return "n/a"
    if abs(num) >= 1000:
        return f"{num:,.2f}"
    if abs(num) >= 10:
        return f"{num:.2f}"
    if abs(num) >= 0.1:
        return f"{num:.4f}"
    return f"{num:.5f}"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _format_iso_minutes(value: Any) -> str:
    if value in (None, ""):
        return ""
    try:
        if isinstance(value, datetime):
            dt = value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
            return dt.strftime("%Y-%m-%d %H:%M UTC")
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    except Exception:
        return str(value)


def _next_us_exchange_holiday(now: Optional[datetime] = None) -> Dict[str, Any]:
    now_utc = now or _utc_now()
    year = now_utc.year

    def _thanksgiving(y: int) -> datetime:
        dt = datetime(y, 11, 1, tzinfo=timezone.utc)
        while dt.weekday() != 3:
            dt += timedelta(days=1)
        return dt + timedelta(days=21)

    holidays = [
        ("New Year", datetime(year, 1, 1, tzinfo=timezone.utc)),
        ("May Day", datetime(year, 5, 1, tzinfo=timezone.utc)),
        ("Independence Day", datetime(year, 7, 4, tzinfo=timezone.utc)),
        ("Thanksgiving", _thanksgiving(year)),
        ("Christmas", datetime(year, 12, 25, tzinfo=timezone.utc)),
        ("Boxing Day", datetime(year, 12, 26, tzinfo=timezone.utc)),
        ("New Year", datetime(year + 1, 1, 1, tzinfo=timezone.utc)),
        ("May Day", datetime(year + 1, 5, 1, tzinfo=timezone.utc)),
        ("Independence Day", datetime(year + 1, 7, 4, tzinfo=timezone.utc)),
        ("Thanksgiving", _thanksgiving(year + 1)),
        ("Christmas", datetime(year + 1, 12, 25, tzinfo=timezone.utc)),
        ("Boxing Day", datetime(year + 1, 12, 26, tzinfo=timezone.utc)),
    ]
    future = [(name, dt) for name, dt in holidays if dt >= now_utc.replace(hour=0, minute=0, second=0, microsecond=0)]
    if not future:
        return {"next_holiday": "", "next_holiday_date": "", "days_until": 0, "is_today": False}
    name, dt = min(future, key=lambda item: item[1])
    return {
        "next_holiday": name,
        "next_holiday_date": dt.strftime("%Y-%m-%d"),
        "days_until": max(0, (dt.date() - now_utc.date()).days),
        "is_today": dt.date() == now_utc.date(),
    }


def _asset_category(asset: str, analysis: Optional[Dict[str, Any]] = None) -> str:
    if isinstance(analysis, dict):
        category = str(analysis.get("category") or "").strip().lower()
        if category:
            return category
    try:
        return str(get_profile(asset).category or "").strip().lower()
    except Exception:
        return ""


def _asset_macro_currencies(asset: str, category: str) -> List[str]:
    canonical = registry.canonical(str(asset or "").strip())
    if not canonical:
        return []

    if category == "forex":
        pair = canonical.replace("-", "/").split("/")
        return [token for token in pair if token]

    if category == "indices":
        mapping = {
            "US30": ["USD"],
            "US100": ["USD"],
            "US500": ["USD"],
            "UK100": ["GBP"],
            "GER40": ["EUR"],
            "AUS200": ["AUD"],
            "JPN225": ["JPY"],
        }
        return list(mapping.get(canonical, ["USD"]))

    if category == "commodities":
        return ["USD"]

    if category == "crypto":
        pair = canonical.replace("-", "/").split("/")
        return [token for token in pair if token]

    return []


def _question_macro_driver(question: str) -> str:
    q = str(question or "").lower()
    if any(token in q for token in ("bank holiday", "holiday", "market holiday")):
        return "holiday"
    if any(token in q for token in ("cpi", "inflation", "pce")):
        return "inflation"
    if any(token in q for token in ("fomc", "fed", "rate cut", "rate hike", "central bank", "ecb", "boe", "boj", "rba")):
        return "rates"
    if any(token in q for token in ("nfp", "payroll", "jobs report", "employment")):
        return "labor"
    return "macro"


def _extract_horizon(question: str) -> str:
    q = " ".join(str(question or "").lower().split())
    for label in ("2 years", "two years", "next year", "12 months", "6 months", "5 years", "long term", "long-term"):
        if label in q:
            return label.replace("-", " ")
    return "the horizon you asked about"


def _news_terms_for_asset(asset: str, category: str) -> List[str]:
    canonical = registry.canonical(str(asset or "").strip()).upper()
    terms = {canonical, canonical.replace("/", "").replace("-", "")}
    if category == "crypto":
        if canonical.startswith("BTC"):
            terms.update({"BTC", "BITCOIN"})
        elif canonical.startswith("ETH"):
            terms.update({"ETH", "ETHEREUM"})
        else:
            terms.add("CRYPTO")
    elif category == "forex":
        terms.update({
            canonical[:3],
            canonical[-3:],
            "CPI",
            "FOMC",
            "FED",
            "ECB",
            "BOE",
            "BOJ",
        })
    elif category == "commodities":
        if canonical.startswith("XAU"):
            terms.update({"GOLD", "XAU"})
        elif canonical.startswith("XAG"):
            terms.update({"SILVER", "XAG"})
        elif canonical.startswith("WTI"):
            terms.update({"OIL", "WTI", "CRUDE"})
    elif category == "indices":
        terms.update({"STOCKS", "EQUITIES", "NASDAQ", "SP500", "S&P", "DOW", "FTSE", "DAX", "NIKKEI"})
    return [term for term in sorted(terms) if term]


def _resolve_asset_from_text(question: str, fallback: str = "") -> str:
    upper = str(question or "").upper()
    for pattern, canonical in _ASSET_ALIAS_PATTERNS:
        if pattern.search(upper):
            return canonical

    candidate = registry.canonical(str(fallback or "").strip())
    if registry.is_known(candidate):
        return candidate
    return ""


class ChatSessionStore:
    def __init__(self, path: Optional[Path] = None):
        self._path = Path(path or _SESSION_FILE)
        self._lock = threading.RLock()
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
            except Exception as exc:
                logger.debug(f"[RobbieChat] Session load skipped: {exc}")
                self._sessions = {}

    def _persist(self) -> None:
        with self._lock:
            payload = {
                "version": 1,
                "updated_at": int(time.time()),
                "sessions": self._sessions,
            }
            tmp = self._path.with_suffix(".tmp")
            tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            tmp.replace(self._path)

    def get(self, chat_id: str) -> Dict[str, Any]:
        with self._lock:
            session = dict(self._sessions.get(str(chat_id), {}) or {})
            session.setdefault("messages", [])
            session.setdefault("last_asset", "")
            session.setdefault("last_trade_id", "")
            return session

    def append_turn(
        self,
        chat_id: str,
        *,
        user_message: str,
        assistant_message: str,
        last_asset: str = "",
        last_trade_id: str = "",
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
            if last_trade_id:
                session["last_trade_id"] = str(last_trade_id)
            session["updated_at"] = int(time.time())
            self._sessions[key] = session
            self._persist()

    def reset(self, chat_id: str) -> None:
        with self._lock:
            self._sessions.pop(str(chat_id), None)
            self._persist()


class RobbieChatService:
    def __init__(
        self,
        *,
        session_store: Optional[ChatSessionStore] = None,
        explainer_factory=RobbieExplainer,
        report_provider=None,
    ):
        self._sessions = session_store or ChatSessionStore()
        self._explainer_factory = explainer_factory
        self._report_provider = report_provider or personality.get_report

    def reset(self, chat_id: str) -> None:
        self._sessions.reset(chat_id)

    def answer(
        self,
        *,
        question: str,
        trading_system: Any,
        chat_id: str,
        asset_hint: str = "",
    ) -> str:
        session = self._sessions.get(chat_id)
        focus_asset = _resolve_asset_from_text(question, asset_hint or session.get("last_asset", ""))
        runtime = self._build_runtime_context(trading_system, focus_asset)
        intent = self._classify_intent(question)
        deterministic = self._answer_deterministic(question, runtime, session, intent=intent)
        response = self._answer_with_optional_deepseek(question, runtime, session, deterministic, intent)
        self._sessions.append_turn(
            chat_id,
            user_message=question,
            assistant_message=response,
            last_asset=str(runtime.get("focus_asset") or ""),
            last_trade_id=str(runtime.get("focus_trade_id") or ""),
        )
        return response

    def _build_runtime_context(self, trading_system: Any, focus_asset: str) -> Dict[str, Any]:
        core = trading_system
        health: Dict[str, Any] = {}
        positions: List[Dict[str, Any]] = []
        closed_trades: List[Dict[str, Any]] = []
        daily: Dict[str, Any] = {}
        performance: Dict[str, Any] = {}
        balance = 0.0
        top_setups: List[Dict[str, Any]] = []
        focus_analysis: Dict[str, Any] = {}

        if core is not None:
            try:
                health = dict(core.health_report() or {})
            except Exception:
                health = {}
            try:
                positions = list(core.get_positions() or [])
            except Exception:
                positions = []
            try:
                history_limit = max(20, int(ROBBIE_CHAT_CLOSED_TRADES_LIMIT or 100))
                closed_trades = rollup_closed_trade_history(
                    list(core.get_closed_trades(limit=history_limit) or []),
                    limit=history_limit,
                )
            except Exception:
                closed_trades = []
            try:
                daily = dict(core.get_daily_stats() or {})
            except Exception:
                daily = {}
            try:
                performance = dict(core.get_performance() or {})
            except Exception:
                performance = {}
            try:
                balance = float(core.get_balance() or 0.0)
            except Exception:
                balance = 0.0

        if not focus_asset:
            focus_asset = self._default_focus_asset(positions, closed_trades)

        if core is not None and focus_asset:
            try:
                focus_analysis = dict(core.inspect_asset(focus_asset) or {})
            except Exception:
                focus_analysis = {}

        market_snapshot = self._build_market_snapshot(
            focus_asset=focus_asset,
            trading_system=core,
            focus_analysis=focus_analysis,
        )
        news_snapshot = self._build_news_snapshot(
            focus_asset=focus_asset,
            category=str(market_snapshot.get("focus_category") or ""),
        )

        try:
            report = self._report_provider() or {}
        except Exception:
            report = {}
        learning = self._learning_snapshot(closed_trades)
        current_trade = self._current_trade_for_asset(positions, focus_asset)
        recent_trade = self._recent_trade_for_asset(closed_trades, focus_asset)
        stop_trade = self._recent_stop_trade_for_asset(closed_trades, focus_asset)

        return {
            "core": core,
            "health": health,
            "positions": positions,
            "closed_trades": closed_trades,
            "daily": daily,
            "performance": performance,
            "balance": balance,
            "focus_asset": focus_asset,
            "focus_analysis": focus_analysis,
            "focus_signal": (focus_analysis.get("signal") if isinstance(focus_analysis.get("signal"), dict) else {}),
            "current_trade": current_trade,
            "recent_trade": recent_trade,
            "stop_trade": stop_trade,
            "market_snapshot": market_snapshot,
            "report": report if isinstance(report, dict) else {},
            "learning": learning,
            "top_setups": top_setups,
            "focus_trade_id": str(
                (current_trade or {}).get("trade_id")
                or (stop_trade or {}).get("trade_id")
                or (recent_trade or {}).get("trade_id")
                or ""
            ),
            "news_snapshot": news_snapshot,
        }

    def _build_market_snapshot(
        self,
        *,
        focus_asset: str,
        trading_system: Any,
        focus_analysis: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        calendar = MarketCalendar()
        events = []
        try:
            events = list(calendar.get_high_impact_events(days=max(1, int(ROBBIE_CHAT_MARKET_LOOKAHEAD_DAYS or 5))) or [])
        except Exception:
            events = []

        analysis = focus_analysis if isinstance(focus_analysis, dict) else {}
        focus_category = _asset_category(focus_asset, analysis)
        halving = {}
        if str(focus_asset or "").upper() in {"BTC-USD", "ETH-USD", "SOL-USD", "BNB-USD", "XRP-USD"}:
            try:
                halving = dict(calendar.get_halving_countdown("bitcoin") or {})
            except Exception:
                halving = {}

        risk_outlook = {}
        try:
            risk_outlook = dict(calendar.should_reduce_risk() or {})
        except Exception:
            risk_outlook = {}

        upcoming: List[Dict[str, Any]] = []
        try:
            from data_ingestion.news_event_monitor import news_monitor

            raw_upcoming = list(news_monitor.upcoming_events(hours=48) or [])
            for item in raw_upcoming[: max(1, int(ROBBIE_CHAT_MARKET_EVENT_LIMIT or 10))]:
                upcoming.append(
                    {
                        "event": str(item.get("name") or ""),
                        "impact": str(item.get("impact") or ""),
                        "time": _format_iso_minutes(item.get("time")),
                        "affects": sorted(list(item.get("affects") or [])),
                        "source": str(item.get("source") or ""),
                    }
                )
        except Exception:
            upcoming = []

        exchange_holiday = _next_us_exchange_holiday()
        focused_status = {}
        if focus_asset:
            focused_status = dict(analysis.get("market_status") or {}) if analysis else {}
        if focus_asset and not focused_status:
            try:
                focused_status = dict(build_market_status(focus_asset, focus_category) or {})
            except Exception:
                focused_status = {}

        market_intelligence = dict(analysis.get("market_intelligence") or {}) if analysis else {}
        free_market_intelligence = dict(market_intelligence.get("free_market_intelligence") or {})

        return {
            "high_impact_events": [
                {
                    "event": str(item.get("event") or ""),
                    "impact": str(item.get("impact") or ""),
                    "date": str(item.get("date") or ""),
                    "currency": str(item.get("currency") or ""),
                    "forecast": item.get("forecast", item.get("estimate")),
                    "previous": item.get("previous", item.get("actual")),
                    "source": str(item.get("source") or ""),
                    "surprise_direction": str(item.get("surprise_direction") or ""),
                }
                for item in events[: max(1, int(ROBBIE_CHAT_MARKET_EVENT_LIMIT or 10))]
            ],
            "upcoming_events": upcoming,
            "halving": halving,
            "risk_outlook": risk_outlook,
            "exchange_holiday": exchange_holiday,
            "focus_market_status": focused_status,
            "focus_category": focus_category,
            "focus_macro_currencies": _asset_macro_currencies(focus_asset, focus_category),
            "focus_free_market_intelligence": free_market_intelligence,
        }

    def _build_news_snapshot(self, *, focus_asset: str, category: str) -> Dict[str, Any]:
        if not ROBBIE_CHAT_NEWS_ENABLED:
            return {"enabled": False, "articles": []}

        articles: List[Dict[str, Any]] = []
        try:
            from services.sentiment_dashboard_service import get_dashboard_service

            raw_articles = list(get_dashboard_service().news_integrator.fetch_all_sources() or [])
        except Exception:
            raw_articles = []

        terms = set(_news_terms_for_asset(focus_asset, category))
        for item in raw_articles:
            if not isinstance(item, dict):
                continue
            title = str(item.get("title") or item.get("headline") or "").strip()
            if not title:
                continue
            text = title.upper()
            source = str(item.get("source") or "").strip()
            if terms and not any(term in text for term in terms):
                if focus_asset and category == "crypto" and "BTC" not in text and "BITCOIN" not in text:
                    continue
                if focus_asset and category == "forex" and not any(term in text for term in ("USD", "EUR", "GBP", "JPY", "CPI", "FOMC", "FED", "ECB", "BOE", "BOJ")):
                    continue
            articles.append(
                {
                    "title": title,
                    "source": source,
                    "date": str(item.get("date") or item.get("published_at") or item.get("published") or ""),
                    "sentiment": item.get("sentiment"),
                }
            )
            if len(articles) >= max(1, int(ROBBIE_CHAT_NEWS_LIMIT or 8)):
                break

        return {
            "enabled": True,
            "count": len(articles),
            "articles": articles,
        }

    @staticmethod
    def _default_focus_asset(positions: List[Dict[str, Any]], closed_trades: List[Dict[str, Any]]) -> str:
        if positions:
            latest = sorted(
                positions,
                key=lambda item: str(item.get("entry_time") or item.get("open_time") or ""),
                reverse=True,
            )[0]
            return str(latest.get("asset") or latest.get("canonical_asset") or "")
        if closed_trades:
            return str(closed_trades[0].get("asset") or "")
        return ""

    @staticmethod
    def _current_trade_for_asset(positions: List[Dict[str, Any]], asset: str) -> Dict[str, Any]:
        if not asset:
            return dict(positions[0]) if positions else {}
        for position in positions:
            position_asset = registry.canonical(str(position.get("asset") or position.get("canonical_asset") or ""))
            if position_asset == registry.canonical(asset):
                return dict(position)
        return {}

    @staticmethod
    def _recent_trade_for_asset(closed_trades: List[Dict[str, Any]], asset: str) -> Dict[str, Any]:
        if not closed_trades:
            return {}
        if not asset:
            return dict(closed_trades[0])
        target = registry.canonical(asset)
        for trade in closed_trades:
            if registry.canonical(str(trade.get("asset") or "")) == target:
                return dict(trade)
        return {}

    @staticmethod
    def _recent_stop_trade_for_asset(closed_trades: List[Dict[str, Any]], asset: str) -> Dict[str, Any]:
        target = registry.canonical(asset) if asset else ""
        for trade in closed_trades:
            if target and registry.canonical(str(trade.get("asset") or "")) != target:
                continue
            reason = str(trade.get("exit_reason") or "").lower()
            if "stop" in reason or _coerce_float(trade.get("pnl"), 0.0) < 0:
                return dict(trade)
        return {}

    def _ensure_top_setups(self, runtime: Dict[str, Any]) -> List[Dict[str, Any]]:
        cached = list(runtime.get("top_setups") or [])
        if cached:
            return cached
        core = runtime.get("core")
        if core is None:
            return []
        try:
            setups = list(core.scan_top_ranked_opportunities(limit=max(3, int(TOP_OPPORTUNITIES_LIMIT or 10))) or [])
        except Exception:
            setups = []
        runtime["top_setups"] = setups
        return setups

    @staticmethod
    def _learning_snapshot(closed_trades: List[Dict[str, Any]]) -> Dict[str, Any]:
        sample = list(closed_trades[: max(10, int(LEARNING_HISTORY_LIMIT or 40))])
        total = len(sample)
        pnl_total = round(sum(_coerce_float(trade.get("pnl"), 0.0) for trade in sample), 2)
        wins = sum(1 for trade in sample if _coerce_float(trade.get("pnl"), 0.0) > 0)
        losses = sum(1 for trade in sample if _coerce_float(trade.get("pnl"), 0.0) < 0)
        review_summaries: List[str] = []
        lesson_counter: Counter[str] = Counter()
        avoid_counter: Counter[str] = Counter()
        keep_counter: Counter[str] = Counter()

        for trade in sample:
            metadata = trade.get("metadata") if isinstance(trade.get("metadata"), dict) else {}
            review = metadata.get("post_trade_review") if isinstance(metadata.get("post_trade_review"), dict) else {}
            if not review:
                continue
            summary = str(review.get("summary") or "").strip()
            if summary:
                review_summaries.append(summary)
            lesson = str(review.get("lesson") or "").strip()
            if lesson:
                lesson_counter[lesson] += 1
            for item in list(review.get("avoid") or [])[:3]:
                clean = str(item).strip()
                if clean:
                    avoid_counter[clean] += 1
            for item in list(review.get("keep") or [])[:3]:
                clean = str(item).strip()
                if clean:
                    keep_counter[clean] += 1

        return {
            "sample_size": total,
            "wins": wins,
            "losses": losses,
            "win_rate": round((wins / total) * 100.0, 1) if total else 0.0,
            "pnl_total": pnl_total,
            "top_lesson": lesson_counter.most_common(1)[0][0] if lesson_counter else "",
            "top_avoid": avoid_counter.most_common(1)[0][0] if avoid_counter else "",
            "top_keep": keep_counter.most_common(1)[0][0] if keep_counter else "",
            "review_summaries": review_summaries[:3],
        }

    @staticmethod
    def _format_event_brief(event: Dict[str, Any]) -> str:
        currency = str(event.get("currency") or "").strip().upper()
        name = str(event.get("event") or "").strip()
        impact = str(event.get("impact") or "").strip().upper()
        when = str(event.get("date") or event.get("time") or "").strip()
        label = " ".join(part for part in [currency, name] if part).strip()
        pieces = [label or "Unnamed event"]
        if impact:
            pieces.append(f"({impact})")
        if when:
            pieces.append(f"at {when}")
        return " ".join(pieces).strip()

    def _relevant_market_events(
        self,
        market_snapshot: Dict[str, Any],
        *,
        focus_asset: str,
        category: str,
        limit: int = 3,
    ) -> List[Dict[str, Any]]:
        high_impact = list(market_snapshot.get("high_impact_events") or [])
        upcoming = list(market_snapshot.get("upcoming_events") or [])
        if not focus_asset:
            return (high_impact + upcoming)[:limit]

        currencies = {
            str(item).strip().upper()
            for item in list(market_snapshot.get("focus_macro_currencies") or _asset_macro_currencies(focus_asset, category))
            if str(item).strip()
        }
        asset_markers = {
            focus_asset.upper(),
            focus_asset.replace("/", "").replace("-", "").upper(),
        }

        selected: List[Dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()

        def _append_if_relevant(item: Dict[str, Any]) -> None:
            event_name = str(item.get("event") or "").strip()
            currency = str(item.get("currency") or "").strip().upper()
            affects = {str(entry).strip().upper() for entry in list(item.get("affects") or []) if str(entry).strip()}
            haystack = " ".join([event_name.upper(), " ".join(sorted(affects))])
            relevant = (
                not currencies
                or (currency and currency in currencies)
                or bool(affects & currencies)
                or any(marker in haystack for marker in asset_markers)
                or any(code in haystack for code in currencies)
            )
            if not relevant:
                return
            signature = (str(item.get("date") or item.get("time") or ""), event_name.upper())
            if signature in seen:
                return
            seen.add(signature)
            selected.append(item)

        for item in high_impact:
            if not isinstance(item, dict):
                continue
            _append_if_relevant(item)
            if len(selected) >= limit:
                return selected[:limit]

        for item in upcoming:
            if not isinstance(item, dict):
                continue
            _append_if_relevant(item)
            if len(selected) >= limit:
                break
        return selected[:limit]

    @staticmethod
    def _macro_proxy_summary(market_snapshot: Dict[str, Any]) -> str:
        free_market_intelligence = (
            market_snapshot.get("focus_free_market_intelligence")
            if isinstance(market_snapshot.get("focus_free_market_intelligence"), dict)
            else {}
        )
        details = (
            free_market_intelligence.get("details")
            if isinstance(free_market_intelligence.get("details"), dict)
            else {}
        )
        macro = details.get("macro") if isinstance(details.get("macro"), dict) else {}
        if not macro:
            return ""

        usd_delta = _coerce_float(((macro.get("usd_broad") or {}) if isinstance(macro.get("usd_broad"), dict) else {}).get("delta_pct"), 0.0)
        us2y = _coerce_float(((macro.get("us2y") or {}) if isinstance(macro.get("us2y"), dict) else {}).get("latest"), 0.0)
        real10y = _coerce_float(((macro.get("real10y") or {}) if isinstance(macro.get("real10y"), dict) else {}).get("latest"), 0.0)
        vix = _coerce_float(((macro.get("vix") or {}) if isinstance(macro.get("vix"), dict) else {}).get("latest"), 0.0)

        parts: List[str] = []
        if usd_delta:
            parts.append(f"USD broad {usd_delta:+.2f}% versus the prior print")
        if us2y:
            parts.append(f"US 2Y {us2y:.2f}")
        if real10y:
            parts.append(f"real 10Y {real10y:.2f}")
        if vix:
            parts.append(f"VIX {vix:.2f}")
        if not parts:
            return ""
        return "Current macro proxy: " + "; ".join(parts[:3]) + "."

    @staticmethod
    def _macro_effect_summary(asset: str, category: str, driver: str) -> str:
        asset_upper = str(asset or "").upper()
        if not asset_upper:
            return ""

        tighter_label, easier_label = {
            "inflation": ("hotter inflation prints", "cooler inflation prints"),
            "rates": ("a hawkish central-bank path", "a more dovish central-bank path"),
            "labor": ("a stronger jobs print", "a softer jobs print"),
            "holiday": ("bank-holiday liquidity", "normal liquidity"),
            "macro": ("a hawkish / risk-off macro mix", "a softer / risk-on macro mix"),
        }.get(driver, ("a tighter macro mix", "an easier macro mix"))

        if driver == "holiday":
            return (
                f"For {asset}, bank holidays usually mean thinner liquidity, wider spreads, and weaker follow-through. "
                f"Breakouts become less trustworthy until {easier_label} returns."
            )

        if category == "forex":
            if asset_upper.startswith("USD/"):
                return (
                    f"For {asset}, {tighter_label} usually support USD and can push the pair higher, "
                    f"while {easier_label} can take pressure back off USD."
                )
            if asset_upper.endswith("/USD"):
                return (
                    f"For {asset}, {tighter_label} usually strengthen USD and pressure the pair lower, "
                    f"while {easier_label} can help the pair recover."
                )
            return (
                f"For {asset}, the transmission is more indirect: {tighter_label} matter mainly through rate differentials, "
                f"risk appetite, and how USD pressure spills into the cross."
            )

        if category == "commodities":
            if asset_upper in {"XAU/USD", "XAG/USD"}:
                return (
                    f"For {asset}, {tighter_label} usually lift real-yield and USD pressure, which tends to weigh on metals. "
                    f"{easier_label} usually help."
                )
            if asset_upper in {"WTI", "WTI/USD"}:
                return (
                    f"For {asset}, {tighter_label} can weigh through a stronger USD and softer growth expectations, "
                    f"while {easier_label} are usually more supportive if demand expectations improve."
                )

        if category == "indices":
            return (
                f"For {asset}, {tighter_label} usually pressure equity multiples and risk appetite, "
                f"while {easier_label} are supportive as long as growth is not breaking down."
            )

        if category == "crypto":
            return (
                f"For {asset}, {tighter_label} usually mean tighter liquidity, a firmer USD, and weaker risk appetite, "
                f"which tends to pressure crypto. {easier_label} usually help."
            )

        return (
            f"For {asset}, {tighter_label} usually raise volatility and can overpower clean technical structure for a while, "
            f"while {easier_label} usually reduce that pressure."
        )

    def _macro_response(self, runtime: Dict[str, Any], focus_asset: str, question: str) -> str:
        market_snapshot = runtime.get("market_snapshot") if isinstance(runtime.get("market_snapshot"), dict) else {}
        focus_analysis = runtime.get("focus_analysis") if isinstance(runtime.get("focus_analysis"), dict) else {}
        category = str(market_snapshot.get("focus_category") or _asset_category(focus_asset, focus_analysis))
        driver = _question_macro_driver(question)
        events = self._relevant_market_events(
            market_snapshot,
            focus_asset=focus_asset,
            category=category,
            limit=3,
        )
        risk = market_snapshot.get("risk_outlook") if isinstance(market_snapshot.get("risk_outlook"), dict) else {}
        holiday = market_snapshot.get("exchange_holiday") if isinstance(market_snapshot.get("exchange_holiday"), dict) else {}
        status = market_snapshot.get("focus_market_status") if isinstance(market_snapshot.get("focus_market_status"), dict) else {}

        lines: List[str] = []
        if focus_asset:
            lines.append(f"Macro read for *{focus_asset}*:")
            effect = self._macro_effect_summary(focus_asset, category, driver)
            if effect:
                lines.append(effect)
            if status:
                market_state = "open" if bool(status.get("market_open")) else "closed"
                reason = _humanize_token(status.get("reason")) or "status unavailable"
                lines.append(f"Session state: market is {market_state} ({reason}).")
            proxy = self._macro_proxy_summary(market_snapshot)
            if proxy:
                lines.append(proxy)
        else:
            lines.append("Current macro snapshot:")

        if events:
            lines.append("Relevant scheduled risk:")
            for item in events:
                lines.append(f"• {self._format_event_brief(item)}")
        elif driver != "holiday":
            lines.append("I do not have a matching scheduled calendar event in the current snapshot, so this is a mechanics-based macro read rather than a dated event call.")

        if holiday:
            holiday_name = str(holiday.get("next_holiday") or "").strip()
            holiday_date = str(holiday.get("next_holiday_date") or "").strip()
            holiday_days = int(_coerce_float(holiday.get("days_until"), 0.0))
            if holiday_name and (driver == "holiday" or holiday_days <= 7):
                if bool(holiday.get("is_today")):
                    lines.append(f"Next US holiday is *{holiday_name}* and it is today, so liquidity can be thinner than normal.")
                else:
                    lines.append(f"Next US holiday on the calendar is *{holiday_name}* on {holiday_date} ({holiday_days} day(s) away).")

        risk_multiplier = _coerce_float(risk.get("risk_multiplier"), 1.0)
        if bool(risk.get("reduce_trading")):
            lines.append(f"Calendar risk posture is reduced right now with risk multiplier {risk_multiplier:.2f}.")
        elif events:
            lines.append(f"Calendar risk posture is normal for now with risk multiplier {risk_multiplier:.2f}.")

        if not focus_asset:
            lines.append("Ask with an asset name if you want the effect translated into a specific market.")
        return "\n".join(lines)

    def _forecast_response(self, runtime: Dict[str, Any], focus_asset: str, question: str) -> str:
        if not focus_asset:
            return "I can give a scenario outlook, but you need to anchor it to an asset like BTC-USD, EUR/USD, gold, or US500."

        focus_analysis = runtime.get("focus_analysis") if isinstance(runtime.get("focus_analysis"), dict) else {}
        focus_signal = runtime.get("focus_signal") if isinstance(runtime.get("focus_signal"), dict) else {}
        market_snapshot = runtime.get("market_snapshot") if isinstance(runtime.get("market_snapshot"), dict) else {}
        category = str(market_snapshot.get("focus_category") or _asset_category(focus_asset, focus_analysis))
        horizon = _extract_horizon(question)
        direction = str(focus_signal.get("direction") or "").upper()
        confidence = _coerce_float(focus_signal.get("confidence"), 0.0) * 100.0
        sentiment = _coerce_float(focus_analysis.get("sentiment_score"), 0.0)
        decision_status = str(focus_analysis.get("decision_status") or "").strip().lower()
        decision_reason = _humanize_token(focus_analysis.get("decision_reason"))
        halving = market_snapshot.get("halving") if isinstance(market_snapshot.get("halving"), dict) else {}
        risk = market_snapshot.get("risk_outlook") if isinstance(market_snapshot.get("risk_outlook"), dict) else {}

        lines = [
            f"I cannot know where *{focus_asset}* will be in {horizon} with certainty. The best I can give is a scenario map from the current regime."
        ]
        if direction:
            lines.append(f"Current engine read is {direction} with about {confidence:.0f}% live confidence and sentiment {sentiment:+.2f}.")
        elif decision_status:
            lines.append(f"Current engine posture is `{decision_status}`{f' because {decision_reason}' if decision_reason else ''}.")

        if category == "crypto":
            if halving:
                lines.append(
                    f"Structural context: the next Bitcoin halving is in {int(_coerce_float(halving.get('days_until'), 0.0))} day(s) on {halving.get('halving_date') or 'unknown date'}."
                )
            lines.append("Bull case: liquidity loosens, risk appetite stays firm, and crypto cycle momentum keeps compounding.")
            lines.append("Bear case: inflation re-accelerates, policy stays tighter for longer, or risk appetite breaks.")
            lines.append("Base case: expect a volatile path driven more by liquidity and macro than by a straight-line trend.")
        elif category == "forex":
            lines.append("Bull case: the rate differential and macro growth mix keep shifting in favor of the base currency.")
            lines.append("Bear case: the rate differential flips the other way or risk sentiment turns against the pair.")
            lines.append("Base case: the pair will mostly track relative central-bank policy and growth surprises, not a fixed destination.")
        elif category == "commodities":
            lines.append("Bull case: real yields and USD pressure ease, while physical demand stays firm.")
            lines.append("Bear case: real yields stay high, USD stays firm, or growth expectations weaken.")
            lines.append("Base case: macro regime and inventory / supply headlines will matter more than any static long-range target.")
        else:
            lines.append("Bull case: liquidity improves and the earnings / growth backdrop stays supportive.")
            lines.append("Bear case: rates stay restrictive or growth deteriorates enough to hurt risk assets.")
            lines.append("Base case: the path is regime-dependent, so scenario management matters more than a single target.")

        if bool(risk.get("reduce_trading")):
            risk_multiplier = _coerce_float(risk.get("risk_multiplier"), 1.0)
            lines.append(
                f"Right now the calendar is already in reduced-risk mode at {risk_multiplier:.2f}, so near-term noise is elevated."
            )
        lines.append("Treat that as a scenario outlook, not a promise.")
        return "\n".join(lines)

    def _classify_intent(self, question: str) -> str:
        q = str(question or "").lower()
        if any(token in q for token in ("issue", "problem", "error", "health", "wrong", "degraded", "failing")):
            return "issues"
        if any(token in q for token in ("learned", "learnt", "learning", "lesson", "improve", "improved")):
            return "learning"
        if any(token in q for token in ("stop loss", "stopped out", "hit stop", "why did it lose", "what happened to the loss")):
            return "stop_loss"
        if any(token in q for token in ("where do you see", "2 years", "two years", "next year", "12 months", "6 months", "long term", "long-term", "forecast", "outlook")):
            return "forecast"
        if any(token in q for token in ("cpi", "inflation", "pce", "fomc", "fed", "rate cut", "rate hike", "bank holiday", "holiday", "central bank", "nfp", "payroll", "jobs report", "macro")):
            return "macro"
        if any(token in q for token in ("adjust", "adapt", "self adjust", "self-adjust", "tune yourself", "how can you change")):
            return "adjustment"
        if any(token in q for token in ("what is happening", "what's happening", "currently happening", "right now", "market now", "top setups", "opportunity", "affect trading")):
            return "market"
        if any(token in q for token in ("open position", "running trade", "current trade", "current position", "what are you in")):
            return "positions"
        if any(token in q for token in ("why did you choose", "why choose", "why this trade", "why take", "why did you buy", "why did you sell")):
            return "trade_why"
        return "general"

    def _answer_deterministic(
        self,
        question: str,
        runtime: Dict[str, Any],
        session: Dict[str, Any],
        *,
        intent: Optional[str] = None,
    ) -> str:
        intent = str(intent or self._classify_intent(question) or "general")
        focus_asset = str(runtime.get("focus_asset") or "")
        focus_analysis = runtime.get("focus_analysis") if isinstance(runtime.get("focus_analysis"), dict) else {}
        focus_signal = runtime.get("focus_signal") if isinstance(runtime.get("focus_signal"), dict) else {}

        if intent == "issues":
            return self._issues_response(runtime)
        if intent == "learning":
            return self._learning_response(runtime, focus_asset)
        if intent == "stop_loss":
            return self._stop_loss_response(runtime, focus_asset, session)
        if intent == "forecast":
            return self._forecast_response(runtime, focus_asset, question)
        if intent == "macro":
            return self._macro_response(runtime, focus_asset, question)
        if intent == "adjustment":
            return self._adjustment_response(runtime, focus_asset)
        if intent == "market":
            return self._market_response(runtime, focus_asset)
        if intent == "positions":
            return self._positions_response(runtime)
        if intent == "trade_why":
            return self._trade_why_response(runtime, focus_asset, question)
        if focus_asset:
            return self._asset_response(focus_asset, question, focus_signal, focus_analysis)
        return self._general_response(runtime)

    def _asset_response(
        self,
        asset: str,
        question: str,
        signal: Dict[str, Any],
        analysis: Dict[str, Any],
    ) -> str:
        explainer = self._explainer_factory()
        try:
            return explainer.answer(asset, question, signal=signal, analysis=analysis)
        finally:
            close = getattr(explainer, "close", None)
            if callable(close):
                close()

    @staticmethod
    def _issues_response(runtime: Dict[str, Any]) -> str:
        health = runtime.get("health") if isinstance(runtime.get("health"), dict) else {}
        status = str(health.get("status") or "unknown").lower()
        issues = [str(item).strip() for item in list(health.get("issues") or []) if str(item).strip()]
        stale = list(health.get("stale_sources") or [])
        never_seen = list(health.get("never_seen_sources") or [])
        recent_errors = int(health.get("recent_error_count", 0) or 0)
        diagnostics = health.get("signal_diagnostics") if isinstance(health.get("signal_diagnostics"), dict) else {}
        summary = str(diagnostics.get("summary_label") or "").strip()

        lines = [f"Robbie health is *{status.upper()}*."]
        if issues:
            lines.append("Main issues right now:")
            for issue in issues[:4]:
                lines.append(f"• {issue}")
        else:
            lines.append("I do not see a hard operational problem right now.")

        detail_bits: List[str] = []
        if stale:
            detail_bits.append(f"stale feeds: {', '.join(stale[:4])}")
        if never_seen:
            detail_bits.append(f"never-seen feeds: {', '.join(never_seen[:4])}")
        if recent_errors:
            detail_bits.append(f"recent monitor errors: {recent_errors}")
        if summary:
            detail_bits.append(f"signal diagnostics: {summary}")
        if detail_bits:
            lines.append("")
            lines.append("Operational read: " + " | ".join(detail_bits) + ".")
        if issues:
            lines.append("")
            lines.append("Execution bias: until those problems clear, I would trust ongoing position management more than forcing fresh entries.")
        return "\n".join(lines)

    @staticmethod
    def _review_for_trade(trade: Dict[str, Any]) -> Dict[str, Any]:
        metadata = trade.get("metadata") if isinstance(trade.get("metadata"), dict) else {}
        review = metadata.get("post_trade_review")
        return dict(review) if isinstance(review, dict) else {}

    def _learning_response(self, runtime: Dict[str, Any], focus_asset: str) -> str:
        trades = list(runtime.get("closed_trades") or [])
        if focus_asset:
            target = registry.canonical(focus_asset)
            asset_trades = [trade for trade in trades if registry.canonical(str(trade.get("asset") or "")) == target]
        else:
            asset_trades = trades
        learning = self._learning_snapshot(asset_trades)
        if int(learning.get("sample_size", 0) or 0) == 0:
            return "I do not have enough closed-trade history yet to tell you what I have learned."

        lines = [
            f"From the last {learning['sample_size']} closed trade(s){f' on {focus_asset}' if focus_asset else ''}, "
            f"win rate is {learning['win_rate']:.1f}% and realized P&L is ${learning['pnl_total']:+.2f}."
        ]
        if learning.get("top_lesson"):
            lines.append(f"Most repeated lesson: {learning['top_lesson']}")
        if learning.get("top_avoid"):
            lines.append(f"Most repeated thing to avoid: {learning['top_avoid']}")
        if learning.get("top_keep"):
            lines.append(f"What has kept working: {learning['top_keep']}")
        summaries = list(learning.get("review_summaries") or [])
        if summaries:
            lines.append("")
            lines.append("Recent review read:")
            for summary in summaries[:2]:
                lines.append(f"• {_clip_text(summary, 180)}")
        return "\n".join(lines)

    def _stop_loss_response(self, runtime: Dict[str, Any], focus_asset: str, session: Dict[str, Any]) -> str:
        trade = runtime.get("stop_trade") if isinstance(runtime.get("stop_trade"), dict) else {}
        if not trade:
            last_trade_id = str(session.get("last_trade_id") or "")
            if last_trade_id:
                for item in list(runtime.get("closed_trades") or []):
                    if str(item.get("trade_id") or "") == last_trade_id:
                        trade = dict(item)
                        break
        if not trade:
            return "I could not find a recent stop-like trade to explain yet."

        review = self._review_for_trade(trade)
        asset = str(trade.get("asset") or focus_asset or "that asset")
        pnl = _coerce_float(trade.get("pnl"), 0.0)
        entry = _price_text(trade.get("entry_price"))
        exit_price = _price_text(trade.get("exit_price"))
        reason = str(trade.get("display_exit_reason") or trade.get("exit_reason") or "closed").strip()

        lines = [
            f"The most relevant stop-like trade I found is *{asset}*.",
            f"Entry was {entry}, exit was {exit_price}, realized P&L was ${pnl:+.2f}, and the close reason was `{reason}`.",
        ]
        if review.get("summary"):
            lines.append(f"Review summary: {review['summary']}")
        wrong = [str(item).strip() for item in list(review.get("what_went_wrong") or []) if str(item).strip()]
        if wrong:
            lines.append("What went wrong:")
            for item in wrong[:3]:
                lines.append(f"• {item}")
        if review.get("lesson"):
            lines.append(f"Lesson: {review['lesson']}")
        if review.get("next_focus"):
            lines.append(f"Next focus: {review['next_focus']}")
        return "\n".join(lines)

    def _adaptive_policy_bits(self, runtime: Dict[str, Any]) -> Dict[str, Any]:
        signal = runtime.get("focus_signal") if isinstance(runtime.get("focus_signal"), dict) else {}
        metadata = signal.get("metadata") if isinstance(signal.get("metadata"), dict) else {}
        adaptive = metadata.get("adaptive_policy") if isinstance(metadata.get("adaptive_policy"), dict) else {}
        if adaptive:
            return adaptive
        current_trade = runtime.get("current_trade") if isinstance(runtime.get("current_trade"), dict) else {}
        meta = current_trade.get("metadata") if isinstance(current_trade.get("metadata"), dict) else {}
        return meta.get("adaptive_policy") if isinstance(meta.get("adaptive_policy"), dict) else {}

    def _adjustment_response(self, runtime: Dict[str, Any], focus_asset: str) -> str:
        adaptive = self._adaptive_policy_bits(runtime)
        health = runtime.get("health") if isinstance(runtime.get("health"), dict) else {}
        learning = runtime.get("learning") if isinstance(runtime.get("learning"), dict) else {}
        lines: List[str] = []

        if adaptive:
            min_conf = _coerce_float(adaptive.get("min_final_confidence"), 0.0)
            risk_mult = _coerce_float(adaptive.get("risk_multiplier"), 1.0)
            min_rr = _coerce_float(adaptive.get("min_rr"), 0.0)
            cooldown = int(_coerce_float(adaptive.get("cooldown_minutes"), 0.0))
            lines.append(
                f"Current adaptive posture{f' for {focus_asset}' if focus_asset else ''}: "
                f"min confidence {min_conf:.3f}, risk multiplier {risk_mult:.2f}, min RR {min_rr:.2f}, cooldown {cooldown}m."
            )
            notes = [str(item).strip() for item in list(adaptive.get("notes") or []) if str(item).strip()]
            if bool(adaptive.get("block_new_entries")):
                lines.append(f"Fresh entries are currently blocked: {_humanize_token(adaptive.get('block_reason')) or 'adaptive block active'}.")
            elif notes:
                lines.append("Adaptive notes: " + "; ".join(_humanize_token(item) for item in notes[:3]) + ".")
        else:
            lines.append("I do not have a live adaptive-policy snapshot attached to the current focus yet.")

        issues = [str(item).strip() for item in list(health.get("issues") or []) if str(item).strip()]
        if issues:
            lines.append("Given the current issues, the first adjustment should be to trust fewer fresh entries until the degraded inputs recover.")

        if learning.get("top_avoid"):
            lines.append(f"From recent losses, the strongest adjustment signal is to stop repeating this mistake: {learning['top_avoid']}")
        if learning.get("top_keep"):
            lines.append(f"From recent wins, the thing worth preserving is: {learning['top_keep']}")

        return "\n".join(lines)

    def _market_response(self, runtime: Dict[str, Any], focus_asset: str) -> str:
        market_snapshot = runtime.get("market_snapshot") if isinstance(runtime.get("market_snapshot"), dict) else {}
        news_snapshot = runtime.get("news_snapshot") if isinstance(runtime.get("news_snapshot"), dict) else {}
        focus_analysis = runtime.get("focus_analysis") if isinstance(runtime.get("focus_analysis"), dict) else {}
        category = str(market_snapshot.get("focus_category") or _asset_category(focus_asset, focus_analysis))
        if focus_asset:
            base = self._asset_response(
                focus_asset,
                f"What is happening on {focus_asset} right now and what would affect trading?",
                runtime.get("focus_signal") if isinstance(runtime.get("focus_signal"), dict) else {},
                runtime.get("focus_analysis") if isinstance(runtime.get("focus_analysis"), dict) else {},
            )
            events = self._relevant_market_events(
                market_snapshot,
                focus_asset=focus_asset,
                category=category,
                limit=2,
            )
            risk = market_snapshot.get("risk_outlook") if isinstance(market_snapshot.get("risk_outlook"), dict) else {}
            holiday = market_snapshot.get("exchange_holiday") if isinstance(market_snapshot.get("exchange_holiday"), dict) else {}
            lines = [base]
            if events:
                lines.append("")
                lines.append("Event risk around it:")
                for item in events:
                    lines.append(f"• {self._format_event_brief(item)}")
            if bool(risk.get("reduce_trading")):
                lines.append(
                    f"Calendar risk is reduced right now with multiplier {_coerce_float(risk.get('risk_multiplier'), 1.0):.2f}."
                )
            if holiday and int(_coerce_float(holiday.get("days_until"), 99.0)) <= 7:
                lines.append(
                    f"Next US holiday is {holiday.get('next_holiday') or 'unknown'} on {holiday.get('next_holiday_date') or 'unknown date'}."
                )
            headlines = list(news_snapshot.get("articles") or [])[:2]
            if headlines:
                lines.append("Recent headlines:")
                for item in headlines:
                    lines.append(f"• {str(item.get('title') or '')} [{str(item.get('source') or '')}]")
            return "\n".join(lines)

        health = runtime.get("health") if isinstance(runtime.get("health"), dict) else {}
        balance = _coerce_float(runtime.get("balance"), 0.0)
        daily = runtime.get("daily") if isinstance(runtime.get("daily"), dict) else {}
        positions = list(runtime.get("positions") or [])
        setups = self._ensure_top_setups(runtime)

        lines = [
            f"Engine status is *{str(health.get('status') or 'unknown').upper()}* with balance at `${balance:,.2f}`.",
            f"Today I have {int(daily.get('daily_trades', 0) or 0)} trade(s) and ${_coerce_float(daily.get('daily_pnl'), 0.0):+.2f} daily P&L.",
            f"There are {len(positions)} open position(s) right now.",
        ]
        issues = [str(item).strip() for item in list(health.get("issues") or []) if str(item).strip()]
        if issues:
            lines.append("Main drag on trading right now: " + "; ".join(issues[:2]) + ".")
        events = self._relevant_market_events(market_snapshot, focus_asset="", category="", limit=3)
        if events:
            lines.append("")
            lines.append("Closest macro/event risk:")
            for item in events[:3]:
                lines.append(f"• {self._format_event_brief(item)}")
        risk = market_snapshot.get("risk_outlook") if isinstance(market_snapshot.get("risk_outlook"), dict) else {}
        if bool(risk.get("reduce_trading")):
            lines.append(f"Calendar posture is reduced with risk multiplier {_coerce_float(risk.get('risk_multiplier'), 1.0):.2f}.")
        headlines = list(news_snapshot.get("articles") or [])[:2]
        if headlines:
            lines.append("")
            lines.append("Recent headlines:")
            for item in headlines:
                lines.append(f"• {str(item.get('title') or '')} [{str(item.get('source') or '')}]")
        if setups:
            lines.append("")
            lines.append("Top setups on the board:")
            for item in setups[:3]:
                asset = str(item.get("asset") or "?")
                direction = str(item.get("direction") or item.get("signal") or "HOLD").upper()
                conf = _coerce_float(item.get("confidence"), 0.0) * 100.0
                opp = _coerce_float(item.get("opportunity_score"), 0.0)
                lines.append(f"• {asset}: {direction} at {conf:.0f}% confidence, opportunity {opp:.3f}")
        return "\n".join(lines)

    @staticmethod
    def _positions_response(runtime: Dict[str, Any]) -> str:
        positions = list(runtime.get("positions") or [])
        if not positions:
            return "I am flat right now. There are no open positions on the book."

        lines = [f"I have {len(positions)} open position(s) right now:"]
        ranked = sorted(
            positions,
            key=lambda item: str(item.get("entry_time") or item.get("open_time") or ""),
            reverse=True,
        )
        for position in ranked[:4]:
            asset = str(position.get("asset") or "?")
            direction = str(position.get("direction") or position.get("signal") or "BUY").upper()
            entry = _price_text(position.get("entry_price"))
            pnl = _coerce_float(position.get("pnl"), 0.0)
            lines.append(f"• {asset} {direction} from {entry} | floating P&L ${pnl:+.2f}")
        return "\n".join(lines)

    def _trade_why_response(self, runtime: Dict[str, Any], focus_asset: str, question: str) -> str:
        current_trade = runtime.get("current_trade") if isinstance(runtime.get("current_trade"), dict) else {}
        if current_trade:
            asset = str(current_trade.get("asset") or focus_asset or "")
            analysis = runtime.get("focus_analysis") if isinstance(runtime.get("focus_analysis"), dict) else {}
            signal = runtime.get("focus_signal") if isinstance(runtime.get("focus_signal"), dict) else {}
            if asset:
                return self._asset_response(asset, question or f"Why did you choose {asset}?", signal, analysis)

        recent = runtime.get("recent_trade") if isinstance(runtime.get("recent_trade"), dict) else {}
        if recent:
            review = self._review_for_trade(recent)
            asset = str(recent.get("asset") or focus_asset or "that asset")
            if review:
                return "\n".join(
                    [
                        f"The most recent completed trade I can anchor to is *{asset}*.",
                        _clip_text(review.get("summary") or "", 220),
                        f"Lesson from that trade: {review.get('lesson') or 'no lesson snapshot available yet'}",
                    ]
                ).strip()
        return self._market_response(runtime, focus_asset)

    @staticmethod
    def _general_response(runtime: Dict[str, Any]) -> str:
        report = runtime.get("report") if isinstance(runtime.get("report"), dict) else {}
        learning = runtime.get("learning") if isinstance(runtime.get("learning"), dict) else {}
        health = runtime.get("health") if isinstance(runtime.get("health"), dict) else {}
        market_snapshot = runtime.get("market_snapshot") if isinstance(runtime.get("market_snapshot"), dict) else {}
        news_snapshot = runtime.get("news_snapshot") if isinstance(runtime.get("news_snapshot"), dict) else {}
        mood = str(report.get("current_mood") or "neutral").lower()
        lines = [
            f"Robbie is online and currently feels *{mood.upper()}*.",
            f"Operational status is *{str(health.get('status') or 'unknown').upper()}* with {int(health.get('open_positions', 0) or 0)} open position(s).",
        ]
        if learning.get("sample_size"):
            lines.append(
                f"From the latest {learning['sample_size']} closed trades, win rate is {learning['win_rate']:.1f}% and P&L is ${learning['pnl_total']:+.2f}."
            )
        if learning.get("top_lesson"):
            lines.append(f"Biggest lesson in memory right now: {learning['top_lesson']}")
        events = list(market_snapshot.get("high_impact_events") or [])
        if events:
            first = events[0] if isinstance(events[0], dict) else {}
            lines.append(f"Nearest scheduled macro event: {RobbieChatService._format_event_brief(first)}")
        holiday = market_snapshot.get("exchange_holiday") if isinstance(market_snapshot.get("exchange_holiday"), dict) else {}
        if holiday and str(holiday.get("next_holiday") or "").strip():
            lines.append(
                f"Next US holiday is {holiday.get('next_holiday')} on {holiday.get('next_holiday_date')}."
            )
        headlines = list(news_snapshot.get("articles") or [])[:2]
        if headlines:
            lines.append("Recent headlines: " + " | ".join(f"{str(item.get('title') or '')} [{str(item.get('source') or '')}]" for item in headlines))
        lines.append("Ask me about issues, learning, CPI/FOMC, holidays, current market conditions, a specific asset, or why a stop loss happened.")
        return "\n".join(lines)

    @staticmethod
    def _allow_world_knowledge(intent: str) -> bool:
        if not ROBBIE_CHAT_ALLOW_WORLD_KNOWLEDGE or ROBBIE_CHAT_MODE == "strict":
            return False
        if ROBBIE_CHAT_MODE == "llm":
            return True
        return intent in _WORLD_KNOWLEDGE_INTENTS

    @staticmethod
    def _include_local_draft(intent: str) -> bool:
        policy = str(ROBBIE_CHAT_INCLUDE_LOCAL_DRAFT or "auto").strip().lower() or "auto"
        if policy == "always":
            return True
        if policy == "never":
            return False
        if ROBBIE_CHAT_MODE == "strict":
            return True
        if ROBBIE_CHAT_MODE == "llm":
            return intent in {"issues", "positions", "stop_loss", "trade_why"}
        return intent in _RUNTIME_FACT_INTENTS

    @staticmethod
    def _position_fact(position: Dict[str, Any]) -> Dict[str, Any]:
        metadata = position.get("metadata") if isinstance(position.get("metadata"), dict) else {}
        return {
            "asset": position.get("asset") or position.get("canonical_asset"),
            "direction": position.get("direction") or position.get("signal"),
            "entry_price": position.get("entry_price"),
            "current_price": position.get("current_price"),
            "stop_loss": position.get("stop_loss"),
            "take_profit": position.get("take_profit"),
            "pnl": position.get("pnl"),
            "entry_time": position.get("entry_time") or position.get("open_time"),
            "playbook": metadata.get("playbook"),
            "exit_plan": metadata.get("exit_plan"),
        }

    @classmethod
    def _closed_trade_fact(cls, trade: Dict[str, Any]) -> Dict[str, Any]:
        review = cls._review_for_trade(trade)
        return {
            "trade_id": trade.get("trade_id"),
            "asset": trade.get("asset"),
            "direction": trade.get("direction"),
            "entry_price": trade.get("entry_price"),
            "exit_price": trade.get("exit_price"),
            "pnl": trade.get("pnl"),
            "exit_reason": trade.get("display_exit_reason") or trade.get("exit_reason"),
            "entry_time": trade.get("entry_time") or trade.get("open_time"),
            "exit_time": trade.get("exit_time"),
            "review_summary": review.get("summary"),
            "review_lesson": review.get("lesson"),
            "review_next_focus": review.get("next_focus"),
        }

    def _deepseek_system_prompt(self, *, intent: str, allow_world_knowledge: bool, include_local_draft: bool) -> str:
        instructions = [
            "You are Robbie, the conversational intelligence layer for a live trading bot.",
            "Answer like a strong analytical LLM, not like a menu tree or canned FAQ.",
            "Runtime facts in the provided context are authoritative for bot state, trades, performance, health, and recorded market snapshots.",
            "Do not invent positions, trades, fills, P&L, health issues, headlines, prices, or event timestamps.",
            "If runtime facts are missing, say what is missing instead of fabricating it.",
            "Think through the mechanics before answering, but do not reveal chain-of-thought.",
        ]
        if allow_world_knowledge:
            instructions.extend(
                [
                    "You may use general trading and macro knowledge for explanations, scenario analysis, and conceptual questions.",
                    "When you rely on general knowledge instead of runtime facts, label it clearly as inference, mechanics, or scenario rather than a confirmed live fact.",
                    "If asked about something currently happening in the world that is not in runtime facts, answer at a general-mechanics level and state that you are not confirming a fresh live event.",
                ]
            )
        else:
            instructions.append("Stay grounded to runtime facts and general mechanics only; do not answer as if you have broader live awareness.")
        if include_local_draft:
            instructions.append("A local baseline answer may be provided; use it only if it is consistent with the stronger runtime facts and your own reasoning.")
        if intent in {"stop_loss", "trade_why", "learning", "adjustment", "issues", "positions"}:
            instructions.append("For bot-specific questions, lead with runtime facts first, then add inference only if useful.")
        elif intent in {"macro", "forecast", "general", "market"}:
            instructions.append("For broader market questions, synthesize runtime facts with general knowledge and answer conversationally, but do not overclaim certainty.")
        return " ".join(instructions)

    def _answer_with_optional_deepseek(
        self,
        question: str,
        runtime: Dict[str, Any],
        session: Dict[str, Any],
        deterministic: str,
        intent: str,
    ) -> str:
        provider = str(ROBBIE_CHAT_PROVIDER or "auto").strip().lower()
        if provider not in {"auto", "deepseek"}:
            return deterministic
        if not DEEPSEEK_API_KEY:
            return deterministic

        allow_world_knowledge = self._allow_world_knowledge(intent)
        include_local_draft = self._include_local_draft(intent)

        try:
            response = self._call_deepseek(
                question,
                runtime,
                session,
                deterministic,
                intent=intent,
                allow_world_knowledge=allow_world_knowledge,
                include_local_draft=include_local_draft,
            )
            return response or deterministic
        except Exception as exc:
            logger.debug(f"[RobbieChat] DeepSeek fallback: {exc}")
            return deterministic

    def _call_deepseek(
        self,
        question: str,
        runtime: Dict[str, Any],
        session: Dict[str, Any],
        deterministic: str,
        *,
        intent: str,
        allow_world_knowledge: bool,
        include_local_draft: bool,
    ) -> str:
        base_url = str(ROBBIE_CHAT_BASE_URL or "https://api.deepseek.com").rstrip("/")
        endpoint = f"{base_url}/chat/completions"
        history = list(session.get("messages") or [])[-_MAX_HISTORY_MESSAGES:]
        context = self._llm_context(
            runtime,
            deterministic,
            question=question,
            intent=intent,
            allow_world_knowledge=allow_world_knowledge,
            include_local_draft=include_local_draft,
        )
        payload = {
            "model": str(ROBBIE_CHAT_MODEL or "deepseek-chat"),
            "messages": [
                {
                    "role": "system",
                    "content": self._deepseek_system_prompt(
                        intent=intent,
                        allow_world_knowledge=allow_world_knowledge,
                        include_local_draft=include_local_draft,
                    ),
                },
                {
                    "role": "system",
                    "content": context,
                },
                *history,
                {
                    "role": "user",
                    "content": question,
                },
            ],
            "temperature": max(0.0, min(1.1, float(ROBBIE_CHAT_TEMPERATURE or 0.35))),
            "max_tokens": max(300, min(2000, int(ROBBIE_CHAT_MAX_TOKENS or 1100))),
        }
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
            return ""
        message = choices[0].get("message") if isinstance(choices[0], dict) else {}
        content = str((message or {}).get("content") or "").strip()
        return content

    def _llm_context(
        self,
        runtime: Dict[str, Any],
        deterministic: str,
        *,
        question: str,
        intent: str,
        allow_world_knowledge: bool,
        include_local_draft: bool,
    ) -> str:
        focus_analysis = runtime.get("focus_analysis") if isinstance(runtime.get("focus_analysis"), dict) else {}
        health = runtime.get("health") if isinstance(runtime.get("health"), dict) else {}
        learning = runtime.get("learning") if isinstance(runtime.get("learning"), dict) else {}
        current_trade = runtime.get("current_trade") if isinstance(runtime.get("current_trade"), dict) else {}
        stop_trade = runtime.get("stop_trade") if isinstance(runtime.get("stop_trade"), dict) else {}
        market_snapshot = runtime.get("market_snapshot") if isinstance(runtime.get("market_snapshot"), dict) else {}
        news_snapshot = runtime.get("news_snapshot") if isinstance(runtime.get("news_snapshot"), dict) else {}
        setups = self._ensure_top_setups(runtime)
        focus_signal = runtime.get("focus_signal") if isinstance(runtime.get("focus_signal"), dict) else {}
        position_limit = max(2, int(ROBBIE_CHAT_OPEN_POSITIONS_LIMIT or 8))
        trade_limit = max(4, min(int(ROBBIE_CHAT_CLOSED_TRADES_LIMIT or 100), 12))
        event_limit = max(3, int(ROBBIE_CHAT_MARKET_EVENT_LIMIT or 10))
        setup_limit = max(3, min(int(TOP_OPPORTUNITIES_LIMIT or 10), 12))
        positions = [
            self._position_fact(item)
            for item in list(runtime.get("positions") or [])[:position_limit]
            if isinstance(item, dict)
        ]
        recent_closed = [
            self._closed_trade_fact(item)
            for item in list(runtime.get("closed_trades") or [])[:trade_limit]
            if isinstance(item, dict)
        ]
        daily = runtime.get("daily") if isinstance(runtime.get("daily"), dict) else {}
        performance = runtime.get("performance") if isinstance(runtime.get("performance"), dict) else {}

        sections = [
            _section_text(
                "chat_contract",
                {
                    "utc_now": _utc_now().strftime("%Y-%m-%d %H:%M:%S UTC"),
                    "question": question,
                    "intent": intent,
                    "mode": ROBBIE_CHAT_MODE,
                    "allow_world_knowledge": allow_world_knowledge,
                    "focus_asset": runtime.get("focus_asset"),
                },
                limit=900,
            ),
            _section_text(
                "runtime_facts_engine",
                {
                    "health_status": health.get("status"),
                    "issues": list(health.get("issues") or [])[:8],
                    "open_positions": health.get("open_positions"),
                    "daily": daily,
                    "performance": performance,
                    "balance": runtime.get("balance"),
                    "recent_error_count": health.get("recent_error_count"),
                    "stale_sources": list(health.get("stale_sources") or [])[:8],
                    "never_seen_sources": list(health.get("never_seen_sources") or [])[:8],
                    "signal_diagnostics": health.get("signal_diagnostics"),
                },
                limit=1800,
            ),
            _section_text(
                "runtime_facts_focus_asset",
                {
                    "analysis": {
                        "asset": focus_analysis.get("asset") or runtime.get("focus_asset"),
                        "category": focus_analysis.get("category") or market_snapshot.get("focus_category"),
                        "decision_status": focus_analysis.get("decision_status"),
                        "decision_reason": focus_analysis.get("decision_reason"),
                        "market_status": focus_analysis.get("market_status"),
                        "sentiment_score": focus_analysis.get("sentiment_score"),
                        "signal": {
                            "direction": focus_signal.get("direction"),
                            "confidence": focus_signal.get("confidence"),
                            "alive": focus_signal.get("alive"),
                            "entry_price": focus_signal.get("entry_price"),
                            "stop_loss": focus_signal.get("stop_loss"),
                            "take_profit": focus_signal.get("take_profit"),
                        },
                        "playbook_decision": focus_analysis.get("playbook_decision"),
                        "market_structure": focus_analysis.get("market_structure"),
                    }
                },
                limit=2600,
            ),
            _section_text("runtime_facts_open_positions", positions, limit=1800),
            _section_text(
                "runtime_facts_focus_trade",
                {
                    "current_trade": self._position_fact(current_trade) if current_trade else {},
                    "recent_stop_trade": self._closed_trade_fact(stop_trade) if stop_trade else {},
                    "recent_closed_trades": recent_closed,
                },
                limit=2400,
            ),
            _section_text("runtime_facts_learning", learning, limit=1400),
            _section_text(
                "runtime_facts_market_snapshot",
                {
                    "risk_outlook": market_snapshot.get("risk_outlook"),
                    "focus_market_status": market_snapshot.get("focus_market_status"),
                    "focus_macro_currencies": market_snapshot.get("focus_macro_currencies"),
                    "high_impact_events": list(market_snapshot.get("high_impact_events") or [])[:event_limit],
                    "upcoming_events": list(market_snapshot.get("upcoming_events") or [])[:event_limit],
                    "exchange_holiday": market_snapshot.get("exchange_holiday"),
                    "halving": market_snapshot.get("halving"),
                    "focus_free_market_intelligence": market_snapshot.get("focus_free_market_intelligence"),
                },
                limit=2600,
            ),
            _section_text(
                "runtime_facts_news",
                {
                    "enabled": news_snapshot.get("enabled"),
                    "count": news_snapshot.get("count"),
                    "articles": list(news_snapshot.get("articles") or [])[: max(4, int(ROBBIE_CHAT_NEWS_LIMIT or 8))],
                },
                limit=1800,
            ),
            _section_text(
                "runtime_facts_top_setups",
                [
                    {
                        "asset": item.get("asset"),
                        "direction": item.get("direction") or item.get("signal"),
                        "confidence": item.get("confidence"),
                        "opportunity_score": item.get("opportunity_score"),
                    }
                    for item in setups[:setup_limit]
                    if isinstance(item, dict)
                ],
                limit=1200,
            ),
        ]
        if include_local_draft and deterministic:
            sections.append(_section_text("local_baseline_answer", deterministic, limit=1400))
        text = "\n\n".join(section for section in sections if section.strip())
        return _clip_text(text, int(ROBBIE_CHAT_CONTEXT_CHAR_LIMIT or 12000))


_chat_service: Optional[RobbieChatService] = None
_chat_service_lock = threading.Lock()


def get_chat_service() -> RobbieChatService:
    global _chat_service
    with _chat_service_lock:
        if _chat_service is None:
            _chat_service = RobbieChatService()
        return _chat_service


__all__ = ["ChatSessionStore", "RobbieChatService", "get_chat_service"]
