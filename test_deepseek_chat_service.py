from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import services.deepseek_chat_service as deepseek_module
from services.deepseek_chat_service import ChatSessionStore, DeepSeekChatService


class _FakeDeepSeekResponse:
    def __init__(self, content: str = "deepseek reply") -> None:
        self._content = content
        self.content = b'{"choices":[{"message":{"content":"deepseek reply"}}]}'

    def raise_for_status(self) -> None:
        return None

    def json(self) -> Dict[str, Any]:
        return {"choices": [{"message": {"content": self._content}}]}


def test_deepseek_chat_service_uses_only_chat_context(tmp_path: Path, monkeypatch) -> None:
    captured: Dict[str, Any] = {}

    def fake_post(url, headers=None, json=None, timeout=None):
        captured["url"] = url
        captured["headers"] = headers
        captured["json"] = json
        captured["timeout"] = timeout
        return _FakeDeepSeekResponse("hello there")

    monkeypatch.setattr(deepseek_module.requests, "post", fake_post)
    monkeypatch.setattr(deepseek_module, "DEEPSEEK_API_KEY", "test-key")
    monkeypatch.setattr(
        deepseek_module,
        "_build_bot_snapshot",
        lambda: {
            "available": True,
            "source": "read_only_persisted_state",
            "balance": 1234.5,
            "daily_pnl": 12.34,
            "daily_trades": 3,
            "total_trades": 99,
            "win_rate": 58.0,
            "total_pnl": 444.0,
            "open_positions_count": 2,
            "hours_since_last_entry": 4.5,
            "last_entry_time_utc": "2026-04-22T08:00:00+00:00",
            "cooldowns": {"EUR/USD": 30},
            "open_positions": [{"trade_id": "T1", "asset": "EUR/USD"}],
            "recent_closed_trades": [{"trade_id": "C1", "asset": "GBP/USD"}],
        },
    )

    service = DeepSeekChatService(session_store=ChatSessionStore(path=tmp_path / "sessions.json"))
    reply = service.answer(question="hello", chat_id="chat-1")

    assert reply == "hello there"
    assert captured["url"].endswith("/chat/completions")
    assert len(captured["json"]["messages"]) == 3
    assert "trading system" in captured["json"]["messages"][0]["content"].lower()
    assert "read-only bot snapshot" in captured["json"]["messages"][1]["content"].lower()
    assert "open_positions_count" in captured["json"]["messages"][1]["content"]
    assert "1234.5" in captured["json"]["messages"][1]["content"]


def test_deepseek_chat_service_persists_and_resets_history(tmp_path: Path, monkeypatch) -> None:
    calls: list[Dict[str, Any]] = []

    def fake_post(url, headers=None, json=None, timeout=None):
        calls.append(json or {})
        return _FakeDeepSeekResponse(f"reply-{len(calls)}")

    monkeypatch.setattr(deepseek_module.requests, "post", fake_post)
    monkeypatch.setattr(deepseek_module, "DEEPSEEK_API_KEY", "test-key")

    service = DeepSeekChatService(session_store=ChatSessionStore(path=tmp_path / "sessions.json"))
    first = service.answer(question="first", chat_id="chat-1")
    second = service.answer(question="second", chat_id="chat-1")
    service.reset("chat-1")
    third = service.answer(question="third", chat_id="chat-1")

    assert first == "reply-1"
    assert second == "reply-2"
    assert third == "reply-3"
    assert len(calls[1]["messages"]) == 5
    assert len(calls[2]["messages"]) == 3


def test_deepseek_chat_service_prompts_about_bot_snapshot(tmp_path: Path, monkeypatch) -> None:
    captured: Dict[str, Any] = {}

    def fake_post(url, headers=None, json=None, timeout=None):
        captured["json"] = json
        return _FakeDeepSeekResponse("bot answer")

    monkeypatch.setattr(deepseek_module.requests, "post", fake_post)
    monkeypatch.setattr(deepseek_module, "DEEPSEEK_API_KEY", "test-key")
    monkeypatch.setattr(deepseek_module, "_build_bot_snapshot", lambda: {"available": True, "balance": 2000.0, "open_positions_count": 1})

    service = DeepSeekChatService(session_store=ChatSessionStore(path=tmp_path / "sessions.json"))
    reply = service.answer(question="what is my bot doing?", chat_id="chat-2")

    assert reply == "bot answer"
    assert "read-only bot snapshot" in captured["json"]["messages"][1]["content"].lower()
    assert "balance" in captured["json"]["messages"][1]["content"].lower()
    assert "open_positions_count" in captured["json"]["messages"][1]["content"]


def test_deepseek_chat_service_includes_macro_snapshot_for_nfp_and_oil(tmp_path: Path, monkeypatch) -> None:
    captured: Dict[str, Any] = {}

    def fake_post(url, headers=None, json=None, timeout=None):
        captured["json"] = json
        return _FakeDeepSeekResponse("macro answer")

    monkeypatch.setattr(deepseek_module.requests, "post", fake_post)
    monkeypatch.setattr(deepseek_module, "DEEPSEEK_API_KEY", "test-key")
    monkeypatch.setattr(deepseek_module, "_build_bot_snapshot", lambda: {"available": True, "balance": 2000.0, "open_positions_count": 1})
    monkeypatch.setattr(
        deepseek_module,
        "_build_macro_snapshot",
        lambda question: {
            "available": True,
            "source": "market_calendar_and_news",
            "display_now_local": "2026-04-22 08:00:00 EAT",
            "question_scope": "macro",
            "high_impact_events": [{"time_local": "2026-04-22 09:30", "event": "NFP", "impact": "HIGH"}],
            "upcoming_events": [{"time_local": "2026-04-22 10:00", "event": "EIA Crude Inventories", "impact": "HIGH"}],
            "macro_headlines": [{"title": "Oil edges higher on supply concerns", "source": "Reuters", "date": "2026-04-22", "sentiment": 0.2}],
            "risk_outlook": {"reduce_trading": True},
            "wti_intelligence": {"score": 0.4, "sources": ["fred", "eia"]},
            "summary": {"event_count": 2, "headline_count": 1, "source_count": 3, "wti_requested": True},
        },
    )

    service = DeepSeekChatService(session_store=ChatSessionStore(path=tmp_path / "sessions.json"))
    reply = service.answer(question="How is the bot likely to handle the next NFP and what is affecting oil?", chat_id="chat-3")

    assert reply == "macro answer"
    assert len(captured["json"]["messages"]) == 4
    assert "read-only macro snapshot" in captured["json"]["messages"][2]["content"].lower()
    assert "nfp" in captured["json"]["messages"][2]["content"].lower()
    assert "oil" in captured["json"]["messages"][2]["content"].lower()


def test_deepseek_chat_service_includes_current_news_snapshot_for_latest_statement_questions(tmp_path: Path, monkeypatch) -> None:
    captured: Dict[str, Any] = {}

    def fake_post(url, headers=None, json=None, timeout=None):
        captured["json"] = json
        return _FakeDeepSeekResponse("news answer")

    monkeypatch.setattr(deepseek_module.requests, "post", fake_post)
    monkeypatch.setattr(deepseek_module, "DEEPSEEK_API_KEY", "test-key")
    monkeypatch.setattr(deepseek_module, "_build_bot_snapshot", lambda: {"available": True, "balance": 2000.0, "open_positions_count": 1})
    monkeypatch.setattr(
        deepseek_module,
        "_build_current_news_snapshot",
        lambda question: {
            "available": True,
            "source": "question_news_search",
            "query": "trump",
            "articles": [
                {
                    "title": "Trump says tariffs remain on the table",
                    "source": "Reuters",
                    "published_local": "2026-04-22 15:00",
                    "summary": "Remarks from a campaign stop.",
                }
            ],
            "summary": {"article_count": 1, "provider_count": 1, "query": "trump"},
        },
    )

    service = DeepSeekChatService(session_store=ChatSessionStore(path=tmp_path / "sessions.json"))
    reply = service.answer(question="Has Trump said anything today?", chat_id="chat-4")

    assert reply == "news answer"
    assert len(captured["json"]["messages"]) == 4
    assert "current news snapshot" in captured["json"]["messages"][2]["content"].lower()
    assert "trump" in captured["json"]["messages"][2]["content"].lower()
