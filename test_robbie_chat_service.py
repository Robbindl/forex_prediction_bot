from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

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
    ):
        self._health = health or {}
        self._positions = positions or []
        self._closed_trades = closed_trades or []
        self._analyses = analyses or {}
        self._top_setups = top_setups or []
        self._scan_error = scan_error
        self.get_top_setups_calls = 0
        self.scan_calls = 0

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

    def inspect_asset(self, asset: str) -> Dict[str, Any]:
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


def _service(tmp_path: Path) -> RobbieChatService:
    store = ChatSessionStore(path=tmp_path / "chat_sessions.json")
    return RobbieChatService(
        session_store=store,
        explainer_factory=_FakeExplainer,
        report_provider=lambda: {"current_mood": "neutral", "status": "ready"},
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
