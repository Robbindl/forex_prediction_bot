from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any, List, Tuple
from unittest.mock import patch

from deepseek_bot import DeepSeekTelegramBot


@dataclass
class _FakeChat:
    id: int = 5747207752
    type: str = "private"
    actions: List[str] = field(default_factory=list)

    async def send_action(self, action: str) -> None:
        self.actions.append(action)


@dataclass
class _FakeReply:
    parent: "_FakeMessage"

    async def edit_text(self, text: str, reply_markup: Any = None) -> "_FakeReply":
        self.parent.edits.append((text, reply_markup))
        return self

    async def reply_text(self, text: str, reply_markup: Any = None) -> "_FakeReply":
        self.parent.followups.append((text, reply_markup))
        return self


@dataclass
class _FakeMessage:
    chat: _FakeChat
    text: str = ""
    replies: List[Tuple[str, Any]] = field(default_factory=list)
    edits: List[Tuple[str, Any]] = field(default_factory=list)
    followups: List[Tuple[str, Any]] = field(default_factory=list)

    async def reply_text(self, text: str, reply_markup: Any = None) -> _FakeReply:
        self.replies.append((text, reply_markup))
        return _FakeReply(self)


@dataclass
class _FakeService:
    answers: List[Tuple[str, str]] = field(default_factory=list)
    resets: List[str] = field(default_factory=list)

    def answer(self, *, question: str, chat_id: str) -> str:
        self.answers.append((question, chat_id))
        return "deepseek reply"

    def reset(self, chat_id: str) -> None:
        self.resets.append(chat_id)


def _build_update(text: str = "hello", chat_id: int = 5747207752, chat_type: str = "private"):
    chat = _FakeChat(id=chat_id, type=chat_type)
    message = _FakeMessage(chat=chat, text=text)
    return SimpleNamespace(
        effective_chat=chat,
        message=message,
        callback_query=None,
    )


def test_deepseek_bot_replies_to_plain_text() -> None:
    bot = DeepSeekTelegramBot(token="test-token", allowed_chat_id="5747207752")
    fake_service = _FakeService()
    update = _build_update(text="hello DeepSeek")
    ctx = SimpleNamespace(args=[])

    async def run() -> None:
        with patch("deepseek_bot.get_deepseek_chat_service", return_value=fake_service):
            await bot._on_text(update, ctx)

    asyncio.run(run())

    assert fake_service.answers == [("hello DeepSeek", "5747207752")]
    assert update.message.replies[0][0] == "DeepSeek is thinking..."
    assert update.message.edits[0][0] == "deepseek reply"
    assert update.effective_chat.actions == ["typing"]


def test_deepseek_bot_replies_to_chat_command() -> None:
    bot = DeepSeekTelegramBot(token="test-token", allowed_chat_id="5747207752")
    fake_service = _FakeService()
    update = _build_update(text="/chat how are markets looking?")
    ctx = SimpleNamespace(args=["how", "are", "markets", "looking?"])

    async def run() -> None:
        with patch("deepseek_bot.get_deepseek_chat_service", return_value=fake_service):
            await bot._cmd_chat(update, ctx)

    asyncio.run(run())

    assert fake_service.answers == [("how are markets looking?", "5747207752")]
    assert update.message.replies[0][0] == "DeepSeek is thinking..."
    assert update.message.edits[0][0] == "deepseek reply"


def test_deepseek_bot_requires_allowed_private_chat() -> None:
    bot = DeepSeekTelegramBot(token="test-token", allowed_chat_id="5747207752")

    allowed_update = _build_update(chat_id=5747207752, chat_type="private")
    denied_update = _build_update(chat_id=111111111, chat_type="private")
    group_update = _build_update(chat_id=5747207752, chat_type="group")

    assert bot._is_allowed(allowed_update) is True
    assert bot._is_allowed(denied_update) is False
    assert bot._is_allowed(group_update) is False


def test_deepseek_bot_run_uses_compatible_run_polling_signature() -> None:
    captured: dict[str, Any] = {}

    class _FakeApp:
        def __init__(self) -> None:
            async def _set_my_commands(commands: Any) -> None:
                captured["commands"] = commands

            self.bot = SimpleNamespace(set_my_commands=_set_my_commands)

        def add_handler(self, handler: Any) -> None:
            captured.setdefault("handlers", []).append(type(handler).__name__)

        def add_error_handler(self, handler: Any) -> None:
            captured["error_handler"] = True

        def run_polling(self, *args: Any, **kwargs: Any) -> None:
            captured["run_polling_args"] = args
            captured["run_polling_kwargs"] = kwargs

    class _FakeBuilder:
        def token(self, value: str) -> "_FakeBuilder":
            captured["token"] = value
            return self

        def connect_timeout(self, value: int) -> "_FakeBuilder":
            captured["connect_timeout"] = value
            return self

        def read_timeout(self, value: int) -> "_FakeBuilder":
            captured["read_timeout"] = value
            return self

        def write_timeout(self, value: int) -> "_FakeBuilder":
            captured["write_timeout"] = value
            return self

        def build(self) -> _FakeApp:
            captured["built"] = True
            return _FakeApp()

    bot = DeepSeekTelegramBot(token="test-token", allowed_chat_id="5747207752")

    with patch("deepseek_bot.Application.builder", return_value=_FakeBuilder()):
        bot.run()

    assert captured["token"] == "test-token"
    assert captured["built"] is True
    assert "post_init" not in captured["run_polling_kwargs"]
    assert captured["run_polling_kwargs"]["allowed_updates"] is not None
    assert len(captured["commands"]) == 4
