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
            "source": "live_shared_state",
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
    assert "current bot runtime snapshot" in captured["json"]["messages"][1]["content"].lower()
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
    assert "current bot runtime snapshot" in captured["json"]["messages"][1]["content"].lower()
    assert "balance" in captured["json"]["messages"][1]["content"].lower()
    assert "open_positions_count" in captured["json"]["messages"][1]["content"]


def test_deepseek_chat_service_includes_focus_asset_snapshot_for_live_market_question(tmp_path: Path, monkeypatch) -> None:
    captured: Dict[str, Any] = {}

    def fake_post(url, headers=None, json=None, timeout=None):
        captured["json"] = json
        return _FakeDeepSeekResponse("focus answer")

    monkeypatch.setattr(deepseek_module.requests, "post", fake_post)
    monkeypatch.setattr(deepseek_module, "DEEPSEEK_API_KEY", "test-key")
    monkeypatch.setattr(deepseek_module, "_build_bot_snapshot", lambda: {"available": True, "balance": 2000.0})
    monkeypatch.setattr(
        deepseek_module,
        "_build_focus_asset_snapshot",
        lambda asset: {
            "available": True,
            "asset": asset,
            "live_quote": {"price": 3345.12},
            "recent_5m": {"bars": 24, "change_pct_last_6_bars": 0.41},
        },
    )

    service = DeepSeekChatService(session_store=ChatSessionStore(path=tmp_path / "sessions.json"))
    reply = service.answer(question="How is gold moving right now?", chat_id="chat-focus")

    assert reply == "focus answer"
    assert len(captured["json"]["messages"]) == 4
    assert "current focus-asset runtime snapshot" in captured["json"]["messages"][2]["content"].lower()
    assert "XAU/USD" in captured["json"]["messages"][2]["content"]
    assert "3345.12" in captured["json"]["messages"][2]["content"]


def test_deepseek_chat_service_includes_log_snapshot_for_trade_execution_questions(tmp_path: Path, monkeypatch) -> None:
    captured: Dict[str, Any] = {}

    def fake_post(url, headers=None, json=None, timeout=None):
        captured["json"] = json
        return _FakeDeepSeekResponse("log answer")

    monkeypatch.setattr(deepseek_module.requests, "post", fake_post)
    monkeypatch.setattr(deepseek_module, "DEEPSEEK_API_KEY", "test-key")
    monkeypatch.setattr(deepseek_module, "_build_bot_snapshot", lambda: {"available": True})
    monkeypatch.setattr(deepseek_module, "_build_focus_asset_snapshot", lambda asset: {"available": True, "asset": asset})
    monkeypatch.setattr(
        deepseek_module,
        "_build_log_snapshot",
        lambda question, focus_asset="": {
            "available": True,
            "source": "local_log_tail",
            "focus_asset": focus_asset,
            "asset_matches": [f"{focus_asset} stoploss hit at 08:12"],
        },
    )

    service = DeepSeekChatService(session_store=ChatSessionStore(path=tmp_path / "sessions.json"))
    reply = service.answer(question="Why did AUS200 hit stoploss?", chat_id="chat-log")

    assert reply == "log answer"
    assert len(captured["json"]["messages"]) == 5
    assert "current focus-asset runtime snapshot" in captured["json"]["messages"][2]["content"].lower()
    assert "recent local log tail" in captured["json"]["messages"][3]["content"].lower()
    assert "AUS200 stoploss hit" in captured["json"]["messages"][3]["content"]


def test_deepseek_chat_service_includes_macro_snapshot_for_nfp_and_oil(tmp_path: Path, monkeypatch) -> None:
    captured: Dict[str, Any] = {}

    def fake_post(url, headers=None, json=None, timeout=None):
        captured["json"] = json
        return _FakeDeepSeekResponse("macro answer")

    monkeypatch.setattr(deepseek_module.requests, "post", fake_post)
    monkeypatch.setattr(deepseek_module, "DEEPSEEK_API_KEY", "test-key")
    monkeypatch.setattr(deepseek_module, "_build_bot_snapshot", lambda: {"available": True, "balance": 2000.0, "open_positions_count": 1})
    monkeypatch.setattr(deepseek_module, "_build_focus_asset_snapshot", lambda asset: {"available": True, "asset": asset})
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
    assert len(captured["json"]["messages"]) == 5
    assert "current focus-asset runtime snapshot" in captured["json"]["messages"][2]["content"].lower()
    assert "read-only macro snapshot" in captured["json"]["messages"][3]["content"].lower()
    assert "nfp" in captured["json"]["messages"][3]["content"].lower()
    assert "oil" in captured["json"]["messages"][3]["content"].lower()


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


def test_deepseek_chat_service_uses_attachment_context_from_session(tmp_path: Path, monkeypatch) -> None:
    calls: list[Dict[str, Any]] = []

    def fake_post(url, headers=None, json=None, timeout=None):
        calls.append(json or {})
        return _FakeDeepSeekResponse(f"attachment-reply-{len(calls)}")

    monkeypatch.setattr(deepseek_module.requests, "post", fake_post)
    monkeypatch.setattr(deepseek_module, "DEEPSEEK_API_KEY", "test-key")
    monkeypatch.setattr(deepseek_module, "_build_bot_snapshot", lambda: {"available": True})

    service = DeepSeekChatService(session_store=ChatSessionStore(path=tmp_path / "sessions.json"))
    first = service.answer(
        question="Please analyze the attached image.",
        chat_id="chat-attach",
        attachment={"kind": "image", "caption": "Silver exit", "ocr_text": "XAG/USD Take Profit 2"},
    )
    second = service.answer(question="Can you see the image I posted?", chat_id="chat-attach")

    assert first == "attachment-reply-1"
    assert second == "attachment-reply-2"
    assert any("recent telegram attachment summary" in msg["content"].lower() for msg in calls[0]["messages"])
    assert any("recent telegram attachment summary" in msg["content"].lower() for msg in calls[1]["messages"])
    assert any("Take Profit 2" in msg["content"] for msg in calls[1]["messages"])
