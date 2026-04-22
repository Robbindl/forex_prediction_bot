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

    service = DeepSeekChatService(session_store=ChatSessionStore(path=tmp_path / "sessions.json"))
    reply = service.answer(question="hello", chat_id="chat-1")

    assert reply == "hello there"
    assert captured["url"].endswith("/chat/completions")
    assert len(captured["json"]["messages"]) == 2
    assert "trading bot" in captured["json"]["messages"][0]["content"].lower()
    assert "runtime_facts" not in captured["json"]["messages"][0]["content"].lower()


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
    assert len(calls[1]["messages"]) == 4
    assert len(calls[2]["messages"]) == 2
