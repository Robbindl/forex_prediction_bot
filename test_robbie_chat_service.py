from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List
from unittest.mock import MagicMock

import services.robbie_chat_service as robbie_chat_module
from services.robbie_chat_service import ChatSessionStore, RobbieChatService


class _FakeExplainer:
    def answer(self, asset: str, question: str, signal=None, analysis=None, df=None) -> str:
        return f"asset={asset} question={question}"

    def close(self) -> None:
        return None


class _FakeCore:
    def __init__(
        self,
        *,
        health: Dict[str, Any] | None = None,
        positions: List[Dict[str, Any]] | None = None,
        closed_trades: List[Dict[str, Any]] | None = None,
        analyses: Dict[str, Dict[str, Any]] | None = None,
        top_setups: List[Dict[str, Any]] | None = None,
        scan_error: Exception | None = None,
        inspect_error: Exception | None = None,
    ):
        self._health = health or {}
        self._positions = positions or []
        self._closed_trades = closed_trades or []
        self._analyses = analyses or {}
        self._top_setups = top_setups or []
        self._scan_error = scan_error
        self._inspect_error = inspect_error
        self.get_top_setups_calls = 0
        self.scan_calls = 0
        self.inspect_calls = 0

    def health_report(self) -> Dict[str, Any]:
        return dict(self._health)

    def get_positions(self) -> List[Dict[str, Any]]:
        return list(self._positions)

    def get_closed_trades(self, limit: int = 100) -> List[Dict[str, Any]]:
        return list(self._closed_trades[:limit])

    def get_daily_stats(self) -> Dict[str, Any]:
        return {"daily_trades": 3, "daily_pnl": 12.5}

    def get_performance(self) -> Dict[str, Any]:
        return {"total_trades": 12, "total_pnl": 55.0}

    def get_balance(self) -> float:
        return 10_250.0

    def get_runtime_asset_snapshot(self, asset: str) -> Dict[str, Any]:
        return dict(self._analyses.get(asset, self._analyses.get("default", {})))

    def inspect_asset(self, asset: str) -> Dict[str, Any]:
        self.inspect_calls += 1
        if self._inspect_error is not None:
            raise self._inspect_error
        return dict(self._analyses.get(asset, self._analyses.get("default", {})))

    def get_top_ranked_opportunities(self, limit: int = 5, refresh: bool = False, allow_refresh_when_empty: bool = True) -> List[Dict[str, Any]]:
        self.get_top_setups_calls += 1
        return list(self._top_setups[:limit])

    def scan_top_ranked_opportunities(self, limit: int = 5) -> List[Dict[str, Any]]:
        self.scan_calls += 1
        if self._scan_error is not None:
            raise self._scan_error
        return list((self._top_setups or [
            {"asset": "EUR/USD", "direction": "BUY", "confidence": 0.71, "opportunity_score": 0.81},
            {"asset": "XAU/USD", "direction": "SELL", "confidence": 0.68, "opportunity_score": 0.74},
        ])[:limit])


def _service(tmp_path: Path, *, report: Dict[str, Any] | None = None) -> RobbieChatService:
    store = ChatSessionStore(path=tmp_path / "chat_sessions.json")
    return RobbieChatService(
        session_store=store,
        explainer_factory=_FakeExplainer,
        report_provider=lambda: report or {"current_mood": "neutral", "status": "ready", "stats": {}},
    )


class _FakeDeepSeekResponse:
    def __init__(self, content: str = "llm reply") -> None:
        self._content = content
        self.content = b'{"choices":[{"message":{"content":"llm reply"}}]}'

    def raise_for_status(self) -> None:
        return None

    def json(self) -> Dict[str, Any]:
        return {"choices": [{"message": {"content": self._content}}]}


def test_robbie_chat_reports_runtime_issues(tmp_path: Path) -> None:
    service = _service(tmp_path)
    core = _FakeCore(
        health={
            "status": "degraded",
            "issues": ["Stale data sources: sentiment, whale", "Recent monitor errors: 2"],
            "stale_sources": ["sentiment", "whale"],
            "never_seen_sources": [],
            "recent_error_count": 2,
            "open_positions": 1,
            "signal_diagnostics": {"summary_label": "Fragile 1 · Cross conflicts 1"},
        }
    )

    reply = service.answer(
        question="what issues are you experiencing right now?",
        trading_system=core,
        chat_id="chat-1",
    )

    assert "DEGRADED" in reply
    assert "Stale data sources" in reply
    assert "sentiment" in reply


def test_robbie_chat_issues_response_uses_log_and_code_context(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(
        robbie_chat_module,
        "_build_log_snapshot",
        lambda question, focus_asset="": {
            "available": True,
            "signal_scan_summary": ["Signal scan summary: tradable=5 generated=0 no_edge=5"],
            "blocker_matches": ["Decision ETH-USD killed step=3 reason=execution hard block on sell"],
            "asset_matches": [],
            "errors": [],
        },
    )
    monkeypatch.setattr(
        robbie_chat_module,
        "_build_code_snapshot",
        lambda question, focus_asset="": {
            "available": True,
            "search_terms": ["signal scan summary", "execution hard block"],
            "matches": [{"file": "core/decision_engine.py", "line": 123, "snippet": "execution hard block on sell"}],
        },
    )
    service = _service(tmp_path)
    core = _FakeCore(
        health={
            "status": "degraded",
            "issues": ["Signal generation is thin"],
            "recent_error_count": 1,
            "signal_diagnostics": {"summary_label": "tradable=5 generated=0"},
        }
    )

    reply = service.answer(
        question="why am i not seeing any trades right now show me logs and code",
        trading_system=core,
        chat_id="chat-issues-logs",
    )

    assert "Latest signal scan read" in reply
    assert "Code path in play" in reply


def test_robbie_chat_classifies_no_trades_question_as_issues(tmp_path: Path) -> None:
    service = _service(tmp_path)

    assert service._classify_intent("why am i not seeing any trades today") == "issues"


def test_robbie_chat_classifies_latest_log_question_as_logs(tmp_path: Path) -> None:
    service = _service(tmp_path)

    assert service._classify_intent("give me the latest log") == "logs"


def test_robbie_chat_classifies_codebase_access_question_as_codebase(tmp_path: Path) -> None:
    service = _service(tmp_path)

    assert service._classify_intent("do you have access to my code base") == "codebase"


def test_robbie_chat_logs_intent_stays_deterministic(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(
        robbie_chat_module,
        "_build_log_snapshot",
        lambda question, focus_asset="": {
            "available": True,
            "display_now_local": "2026-04-26 18:23:00 EAT",
            "signal_scan_summary": ["Signal scan summary: tradable=5 generated=1 no_edge=4"],
            "blocker_matches": ["Decision ETH-USD killed step=3 reason=execution hard block on sell"],
            "asset_matches": [],
            "runtime_err": [],
            "engine": ["engine line"],
        },
    )
    monkeypatch.setattr(robbie_chat_module, "DEEPSEEK_API_KEY", "test-key")
    service = _service(tmp_path)
    service._call_deepseek = MagicMock(return_value="llm should not be used")

    reply = service.answer(
        question="give me the latest log",
        trading_system=_FakeCore(),
        chat_id="chat-log",
    )

    service._call_deepseek.assert_not_called()
    assert "Yes. I can read the local server log tails" in reply
    assert "Signal scan summary" in reply


def test_robbie_chat_codebase_intent_stays_deterministic(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(
        robbie_chat_module,
        "_build_code_snapshot",
        lambda question, focus_asset="": {
            "available": True,
            "scanned_files": 42,
            "search_terms": ["codebase", "access"],
            "matches": [{"file": "services/robbie_chat_service.py", "line": 123, "snippet": "def answer(..."}],
        },
    )
    monkeypatch.setattr(robbie_chat_module, "DEEPSEEK_API_KEY", "test-key")
    service = _service(tmp_path)
    service._call_deepseek = MagicMock(return_value="llm should not be used")

    reply = service.answer(
        question="do you have access to my code base",
        trading_system=_FakeCore(),
        chat_id="chat-code",
    )

    service._call_deepseek.assert_not_called()
    assert "Yes. I can inspect the local bot codebase" in reply
    assert "services/robbie_chat_service.py:123" in reply


def test_robbie_chat_uses_session_asset_for_stop_loss_follow_up(tmp_path: Path) -> None:
    service = _service(tmp_path)
    review = {
        "summary": "The trade was late and ran into a weak continuation.",
        "lesson": "Avoid chasing the move after it is already mature.",
        "next_focus": "Wait for cleaner pullback confirmation.",
        "what_went_wrong": ["The entry was late.", "The stop was too tight for the noise."],
    }
    core = _FakeCore(
        closed_trades=[
            {
                "trade_id": "t-1",
                "asset": "EUR/USD",
                "entry_price": 1.1010,
                "exit_price": 1.0970,
                "exit_reason": "Stop Loss",
                "pnl": -43.2,
                "metadata": {"post_trade_review": review},
            }
        ],
        analyses={
            "EUR/USD": {
                "asset": "EUR/USD",
                "signal": {"direction": "BUY", "confidence": 0.64, "alive": True, "metadata": {}},
                "decision_status": "accepted",
            }
        },
    )

    first = service.answer(
        question="tell me about EUR/USD",
        trading_system=core,
        chat_id="chat-2",
    )
    second = service.answer(
        question="what happened for it to hit stop loss?",
        trading_system=core,
        chat_id="chat-2",
    )

    assert "asset=EUR/USD" in first
    assert "EUR/USD" in second
    assert "Avoid chasing the move" in second
    assert "Wait for cleaner pullback confirmation" in second


def test_robbie_chat_reports_adaptive_policy_when_asked_to_adjust(tmp_path: Path) -> None:
    service = _service(tmp_path)
    core = _FakeCore(
        analyses={
            "BTC-USD": {
                "asset": "BTC-USD",
                "signal": {
                    "direction": "BUY",
                    "confidence": 0.73,
                    "alive": True,
                    "metadata": {
                        "adaptive_policy": {
                            "min_final_confidence": 0.63,
                            "risk_multiplier": 0.75,
                            "min_rr": 1.8,
                            "cooldown_minutes": 84,
                            "notes": ["late_entry_pressure", "cross_asset_conflict"],
                        }
                    },
                },
            }
        },
    )

    reply = service.answer(
        question="how should you adjust yourself on BTC right now?",
        trading_system=core,
        chat_id="chat-3",
    )

    assert "0.630" in reply
    assert "0.75" in reply
    assert "84m" in reply
    assert "late entry pressure" in reply.lower()


def test_robbie_chat_explains_macro_event_risk_for_asset(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(
        robbie_chat_module.MarketCalendar,
        "get_high_impact_events",
        lambda self, days=3: [
            {
                "date": "2026-04-21 12:30",
                "event": "US CPI",
                "impact": "HIGH",
                "currency": "USD",
                "source": "CalendarTest",
            }
        ],
    )
    monkeypatch.setattr(
        robbie_chat_module.MarketCalendar,
        "should_reduce_risk",
        lambda self: {"risk_multiplier": 0.70, "reduce_trading": True, "high_impact_events": True, "halving_soon": False},
    )
    monkeypatch.setattr(
        robbie_chat_module,
        "_next_us_exchange_holiday",
        lambda now=None: {"next_holiday": "Memorial Day", "next_holiday_date": "2026-05-25", "days_until": 36, "is_today": False},
    )

    service = _service(tmp_path)
    core = _FakeCore(
        analyses={
            "EUR/USD": {
                "asset": "EUR/USD",
                "category": "forex",
                "market_status": {"market_open": True, "reason": "Forex open 24/5"},
                "signal": {"direction": "BUY", "confidence": 0.66, "alive": True, "metadata": {}},
                "sentiment_score": 0.12,
                "market_intelligence": {
                    "free_market_intelligence": {
                        "details": {
                            "macro": {
                                "usd_broad": {"delta_pct": 0.31},
                                "us2y": {"latest": 4.72},
                                "real10y": {"latest": 1.81},
                                "vix": {"latest": 19.4},
                            }
                        }
                    }
                },
            }
        }
    )

    reply = service.answer(
        question="how can cpi affect EUR/USD right now?",
        trading_system=core,
        chat_id="chat-4",
    )

    assert "EUR/USD" in reply
    assert "CPI" in reply
    assert "USD" in reply
    assert "0.70" in reply


def test_robbie_chat_reports_next_holiday(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(robbie_chat_module.MarketCalendar, "get_high_impact_events", lambda self, days=3: [])
    monkeypatch.setattr(
        robbie_chat_module.MarketCalendar,
        "should_reduce_risk",
        lambda self: {"risk_multiplier": 1.0, "reduce_trading": False, "high_impact_events": False, "halving_soon": False},
    )
    monkeypatch.setattr(
        robbie_chat_module,
        "_next_us_exchange_holiday",
        lambda now=None: {"next_holiday": "Memorial Day", "next_holiday_date": "2026-05-25", "days_until": 36, "is_today": False},
    )

    service = _service(tmp_path)
    reply = service.answer(
        question="when is the next bank holiday?",
        trading_system=_FakeCore(),
        chat_id="chat-5",
    )

    assert "Memorial Day" in reply
    assert "2026-05-25" in reply


def test_robbie_chat_calendar_response_uses_internal_calendar_snapshot(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(
        robbie_chat_module,
        "now_in_display_timezone",
        lambda: datetime(2026, 4, 24, 16, 45, tzinfo=timezone(timedelta(hours=3))),
    )
    monkeypatch.setattr(
        robbie_chat_module.MarketCalendar,
        "get_high_impact_events",
        lambda self, days=3: [
            {
                "date": "2026-04-24 15:30",
                "event": "US CPI",
                "impact": "HIGH",
                "currency": "USD",
            }
        ],
    )
    monkeypatch.setattr(
        robbie_chat_module.MarketCalendar,
        "should_reduce_risk",
        lambda self: {"risk_multiplier": 0.7, "reduce_trading": True},
    )
    monkeypatch.setattr(
        robbie_chat_module,
        "_next_us_exchange_holiday",
        lambda now=None: {"next_holiday": "Memorial Day", "next_holiday_date": "2026-05-25", "days_until": 31, "is_today": False},
    )

    service = _service(tmp_path)
    reply = service.answer(
        question="do you have an internal calendar?",
        trading_system=_FakeCore(),
        chat_id="chat-cal",
    )

    assert "internal market-calendar snapshot" in reply
    assert "Friday, April 24, 2026 16:45 EAT" in reply
    assert "US CPI" in reply
    assert "Memorial Day" in reply


def test_robbie_chat_weekly_response_uses_live_weekly_stats(tmp_path: Path, monkeypatch) -> None:
    fixed_now = datetime(2026, 4, 24, 16, 45, tzinfo=timezone(timedelta(hours=3)))
    monkeypatch.setattr(robbie_chat_module, "now_in_display_timezone", lambda: fixed_now)
    monkeypatch.setattr(robbie_chat_module, "_utc_now", lambda: fixed_now.astimezone(timezone.utc))
    service = _service(
        tmp_path,
        report={
            "current_mood": "neutral",
            "status": "ready",
            "stats": {"weekly_trades": 4, "weekly_win_rate": 50.0},
        },
    )
    core = _FakeCore(
        closed_trades=[
            {"asset": "XAU/USD", "pnl": 120.0, "exit_time": "2026-04-23T12:00:00+00:00"},
            {"asset": "WTI", "pnl": -80.0, "exit_time": "2026-04-22T12:00:00+00:00"},
            {"asset": "US500", "pnl": 45.0, "exit_time": "2026-04-21T12:00:00+00:00"},
        ],
    )

    reply = service.answer(
        question="give me my weekly",
        trading_system=core,
        chat_id="chat-weekly",
    )

    assert "Weekly snapshot" in reply
    assert "$+85.00" in reply
    assert "50.0%" in reply
    assert "Best asset this week: XAU/USD" in reply


def test_robbie_chat_schedule_response_creates_weekly_report(tmp_path: Path, monkeypatch) -> None:
    captured: Dict[str, Any] = {}
    schedule_mod = __import__("services.robbie_schedule_service", fromlist=["get_schedule_service"])

    class _FakeScheduleService:
        def schedule_weekly_report(self, **kwargs):
            captured.update(kwargs)
            return {"next_run_at": "2026-04-24T15:00:00+00:00"}

    monkeypatch.setattr(
        schedule_mod,
        "get_schedule_service",
        lambda: _FakeScheduleService(),
        raising=False,
    )

    service = _service(tmp_path)
    reply = service.answer(
        question="give me my weekly every friday at 6pm",
        trading_system=_FakeCore(),
        chat_id="chat-schedule",
    )

    assert captured["chat_id"] == "chat-schedule"
    assert captured["weekday"] == 4
    assert captured["hour"] == 18
    assert "Weekly report scheduled" in reply
    assert "every Friday at 18:00 EAT" in reply


def test_robbie_chat_gives_scenario_forecast_for_bitcoin(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(robbie_chat_module.MarketCalendar, "get_high_impact_events", lambda self, days=3: [])
    monkeypatch.setattr(
        robbie_chat_module.MarketCalendar,
        "should_reduce_risk",
        lambda self: {"risk_multiplier": 0.70, "reduce_trading": True, "high_impact_events": True, "halving_soon": False},
    )
    monkeypatch.setattr(
        robbie_chat_module.MarketCalendar,
        "get_halving_countdown",
        lambda self, crypto="bitcoin": {
            "crypto": crypto,
            "days_until": 690,
            "current_reward": 3.125,
            "next_reward": 1.5625,
            "reduction_percent": 50.0,
            "halving_date": "2028-03-15",
            "is_soon": False,
        },
    )
    monkeypatch.setattr(
        robbie_chat_module,
        "_next_us_exchange_holiday",
        lambda now=None: {"next_holiday": "Memorial Day", "next_holiday_date": "2026-05-25", "days_until": 36, "is_today": False},
    )

    service = _service(tmp_path)
    core = _FakeCore(
        analyses={
            "BTC-USD": {
                "asset": "BTC-USD",
                "category": "crypto",
                "market_status": {"market_open": True, "reason": "crypto_24x7"},
                "decision_status": "accepted",
                "signal": {"direction": "BUY", "confidence": 0.73, "alive": True, "metadata": {}},
                "sentiment_score": 0.28,
            }
        }
    )

    reply = service.answer(
        question="where do you see bitcoin in the next 2 years?",
        trading_system=core,
        chat_id="chat-6",
    )

    assert "BTC-USD" in reply
    assert "scenario" in reply.lower()
    assert "2028-03-15" in reply
    assert "not a promise" in reply.lower()


def test_robbie_chat_market_response_uses_cached_top_setups_without_scan(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(robbie_chat_module.MarketCalendar, "get_high_impact_events", lambda self, days=3: [])
    monkeypatch.setattr(
        robbie_chat_module.MarketCalendar,
        "should_reduce_risk",
        lambda self: {"risk_multiplier": 1.0, "reduce_trading": False, "high_impact_events": False, "halving_soon": False},
    )
    monkeypatch.setattr(
        robbie_chat_module,
        "_next_us_exchange_holiday",
        lambda now=None: {"next_holiday": "Memorial Day", "next_holiday_date": "2026-05-25", "days_until": 36, "is_today": False},
    )

    service = _service(tmp_path)
    core = _FakeCore(
        top_setups=[
            {"asset": "EUR/USD", "direction": "BUY", "confidence": 0.71, "opportunity_score": 0.81},
        ],
        scan_error=AssertionError("chat should not scan opportunities"),
    )

    reply = service.answer(
        question="what is happening right now?",
        trading_system=core,
        chat_id="chat-market-cache",
    )

    assert "Top setups on the board" in reply
    assert "EUR/USD" in reply
    assert core.get_top_setups_calls >= 1
    assert core.scan_calls == 0
    assert core.inspect_calls == 0


def test_robbie_chat_news_snapshot_reads_direct_news_feed(tmp_path: Path, monkeypatch) -> None:
    sentiment_sources_mod = __import__("services.sentiment_sources", fromlist=["_NewsSentiment"])

    monkeypatch.setattr(
        sentiment_sources_mod._NewsSentiment,
        "get_articles_for_dashboard",
        lambda limit=20: [
            {
                "title": "Fed signals slower balance sheet runoff",
                "source": "DirectFeed",
                "date": "2026-04-22T00:10:00Z",
                "sentiment": -0.1,
            }
        ],
        raising=False,
    )

    service = _service(tmp_path)
    snapshot = service._build_news_snapshot(focus_asset="SOL-USD", category="crypto")

    assert snapshot["enabled"] is True
    assert snapshot["raw_count"] == 1
    assert snapshot["count"] == 1
    assert snapshot["articles"][0]["title"] == "Fed signals slower balance sheet runoff"
    assert snapshot["articles"][0]["source"] == "DirectFeed"


def test_robbie_chat_deepseek_context_uses_cached_top_setups_without_scan(tmp_path: Path, monkeypatch) -> None:
    captured: Dict[str, Any] = {}

    def _fake_post(url: str, headers=None, json=None, timeout=None):
        captured["json"] = json
        return _FakeDeepSeekResponse("cached setup llm reply")

    monkeypatch.setattr(robbie_chat_module, "DEEPSEEK_API_KEY", "test-key")
    monkeypatch.setattr(robbie_chat_module, "ROBBIE_CHAT_PROVIDER", "deepseek")
    monkeypatch.setattr(robbie_chat_module, "ROBBIE_CHAT_MODE", "llm")
    monkeypatch.setattr(robbie_chat_module, "ROBBIE_CHAT_ALLOW_WORLD_KNOWLEDGE", True)
    monkeypatch.setattr(robbie_chat_module, "ROBBIE_CHAT_INCLUDE_LOCAL_DRAFT", "auto")
    monkeypatch.setattr(robbie_chat_module.requests, "post", _fake_post)

    service = _service(tmp_path)
    core = _FakeCore(
        top_setups=[
            {"asset": "EUR/USD", "direction": "BUY", "confidence": 0.71, "opportunity_score": 0.81},
        ],
        scan_error=AssertionError("chat llm context should not scan opportunities"),
    )

    reply = service.answer(
        question="what is happening right now?",
        trading_system=core,
        chat_id="chat-market-llm-cache",
    )

    assert reply == "cached setup llm reply"
    assert "runtime_facts_top_setups" in captured["json"]["messages"][1]["content"]
    assert "EUR/USD" in captured["json"]["messages"][1]["content"]
    assert core.get_top_setups_calls >= 1
    assert core.scan_calls == 0
    assert core.inspect_calls == 0


def test_robbie_chat_deepseek_context_includes_display_timezone_now(tmp_path: Path, monkeypatch) -> None:
    captured: Dict[str, Any] = {}

    def _fake_post(url: str, headers=None, json=None, timeout=None):
        captured["json"] = json
        return _FakeDeepSeekResponse("timezone aware reply")

    monkeypatch.setattr(robbie_chat_module, "DEEPSEEK_API_KEY", "test-key")
    monkeypatch.setattr(robbie_chat_module, "ROBBIE_CHAT_PROVIDER", "deepseek")
    monkeypatch.setattr(robbie_chat_module, "ROBBIE_CHAT_MODE", "llm")
    monkeypatch.setattr(robbie_chat_module, "ROBBIE_CHAT_ALLOW_WORLD_KNOWLEDGE", True)
    monkeypatch.setattr(robbie_chat_module, "ROBBIE_CHAT_INCLUDE_LOCAL_DRAFT", "auto")
    monkeypatch.setattr(robbie_chat_module, "display_timezone_label", lambda: "EAT")
    monkeypatch.setattr(
        robbie_chat_module,
        "now_in_display_timezone",
        lambda: robbie_chat_module.datetime(
            2026,
            4,
            22,
            2,
            20,
            0,
            tzinfo=robbie_chat_module.timezone(robbie_chat_module.timedelta(hours=3)),
        ),
    )
    monkeypatch.setattr(robbie_chat_module.requests, "post", _fake_post)

    service = _service(tmp_path)
    reply = service.answer(
        question="what is happening right now?",
        trading_system=_FakeCore(),
        chat_id="chat-display-now",
    )

    assert reply == "timezone aware reply"
    context = captured["json"]["messages"][1]["content"]
    assert "display_now_local" in context
    assert "2026-04-22 02:20:00 EAT" in context
    assert "display_timezone" in context


def test_robbie_chat_macro_response_does_not_call_inspect_asset(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(robbie_chat_module.MarketCalendar, "get_high_impact_events", lambda self, days=3: [])
    monkeypatch.setattr(
        robbie_chat_module.MarketCalendar,
        "should_reduce_risk",
        lambda self: {"risk_multiplier": 1.0, "reduce_trading": False, "high_impact_events": False, "halving_soon": False},
    )
    monkeypatch.setattr(
        robbie_chat_module,
        "_next_us_exchange_holiday",
        lambda now=None: {"next_holiday": "Memorial Day", "next_holiday_date": "2026-05-25", "days_until": 36, "is_today": False},
    )

    service = _service(tmp_path)
    core = _FakeCore(
        positions=[
            {
                "trade_id": "live-1",
                "asset": "SOL-USD",
                "direction": "SELL",
                "entry_price": 85.28,
                "current_price": 85.22,
                "stop_loss": 85.85,
                "take_profit": 84.76,
                "confidence": 0.71,
            }
        ],
        inspect_error=AssertionError("chat should not call inspect_asset"),
    )

    reply = service.answer(
        question="how can cpi affect the current market?",
        trading_system=core,
        chat_id="chat-macro-no-inspect",
    )

    assert "inflation" in reply.lower() or "macro read" in reply.lower()
    assert core.inspect_calls == 0


def test_robbie_chat_deepseek_prompt_allows_hybrid_macro_reasoning(tmp_path: Path, monkeypatch) -> None:
    captured: Dict[str, Any] = {}

    def _fake_post(url: str, headers=None, json=None, timeout=None):
        captured["url"] = url
        captured["headers"] = headers
        captured["json"] = json
        captured["timeout"] = timeout
        return _FakeDeepSeekResponse("macro llm reply")

    monkeypatch.setattr(robbie_chat_module, "DEEPSEEK_API_KEY", "test-key")
    monkeypatch.setattr(robbie_chat_module, "ROBBIE_CHAT_PROVIDER", "deepseek")
    monkeypatch.setattr(robbie_chat_module, "ROBBIE_CHAT_MODE", "hybrid")
    monkeypatch.setattr(robbie_chat_module, "ROBBIE_CHAT_ALLOW_WORLD_KNOWLEDGE", True)
    monkeypatch.setattr(robbie_chat_module, "ROBBIE_CHAT_INCLUDE_LOCAL_DRAFT", "auto")
    monkeypatch.setattr(robbie_chat_module, "ROBBIE_CHAT_MAX_TOKENS", 1234)
    monkeypatch.setattr(robbie_chat_module, "ROBBIE_CHAT_TEMPERATURE", 0.45)
    monkeypatch.setattr(robbie_chat_module.requests, "post", _fake_post)

    service = _service(tmp_path)
    core = _FakeCore(
        analyses={
            "EUR/USD": {
                "asset": "EUR/USD",
                "category": "forex",
                "decision_status": "accepted",
                "decision_reason": "continuation_ready",
                "signal": {"direction": "BUY", "confidence": 0.66, "alive": True, "metadata": {}},
            }
        }
    )

    reply = service.answer(
        question="how can cpi affect EUR/USD right now?",
        trading_system=core,
        chat_id="chat-llm-1",
    )

    assert reply == "macro llm reply"
    payload = captured["json"]
    assert "general trading and macro knowledge" in payload["messages"][0]["content"]
    assert "runtime_facts_market_snapshot" in payload["messages"][1]["content"]
    assert "chat_contract" in payload["messages"][1]["content"]
    assert "local_baseline_answer" not in payload["messages"][1]["content"]
    assert payload["max_tokens"] == 1234
    assert payload["temperature"] == 0.45


def test_robbie_chat_deepseek_prompt_keeps_local_baseline_for_stop_loss(tmp_path: Path, monkeypatch) -> None:
    captured: Dict[str, Any] = {}

    def _fake_post(url: str, headers=None, json=None, timeout=None):
        captured["json"] = json
        return _FakeDeepSeekResponse("stop loss llm reply")

    monkeypatch.setattr(robbie_chat_module, "DEEPSEEK_API_KEY", "test-key")
    monkeypatch.setattr(robbie_chat_module, "ROBBIE_CHAT_PROVIDER", "deepseek")
    monkeypatch.setattr(robbie_chat_module, "ROBBIE_CHAT_MODE", "hybrid")
    monkeypatch.setattr(robbie_chat_module, "ROBBIE_CHAT_ALLOW_WORLD_KNOWLEDGE", True)
    monkeypatch.setattr(robbie_chat_module, "ROBBIE_CHAT_INCLUDE_LOCAL_DRAFT", "auto")
    monkeypatch.setattr(robbie_chat_module.requests, "post", _fake_post)

    service = _service(tmp_path)
    core = _FakeCore(
        closed_trades=[
            {
                "trade_id": "stop-1",
                "asset": "EUR/USD",
                "direction": "BUY",
                "entry_price": 1.1010,
                "exit_price": 1.0970,
                "exit_reason": "Stop Loss",
                "pnl": -43.2,
                "metadata": {
                    "post_trade_review": {
                        "summary": "The trade was late and ran into a weak continuation.",
                        "lesson": "Avoid chasing the move after it is already mature.",
                    }
                },
            }
        ]
    )

    reply = service.answer(
        question="what happened for it to hit stop loss?",
        trading_system=core,
        chat_id="chat-llm-2",
        asset_hint="EUR/USD",
    )

    assert reply == "stop loss llm reply"
    payload = captured["json"]
    assert "runtime facts first" in payload["messages"][0]["content"].lower()
    assert "local_baseline_answer" in payload["messages"][1]["content"]
    assert "review_lesson" in payload["messages"][1]["content"]
