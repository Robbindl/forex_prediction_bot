from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any, List, Tuple
from unittest.mock import patch

from telegram.constants import ParseMode

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

    async def edit_text(self, text: str, parse_mode: Any = None, reply_markup: Any = None) -> "_FakeReply":
        self.parent.edits.append((text, reply_markup, parse_mode))
        return self

    async def reply_text(self, text: str, parse_mode: Any = None, reply_markup: Any = None) -> "_FakeReply":
        self.parent.followups.append((text, reply_markup, parse_mode))
        return self


@dataclass
class _FakeMessage:
    chat: _FakeChat
    text: str = ""
    caption: str = ""
    photo: List[Any] = field(default_factory=list)
    document: Any = None
    replies: List[Tuple[str, Any]] = field(default_factory=list)
    edits: List[Tuple[str, Any]] = field(default_factory=list)
    followups: List[Tuple[str, Any]] = field(default_factory=list)

    async def reply_text(self, text: str, parse_mode: Any = None, reply_markup: Any = None) -> _FakeReply:
        self.replies.append((text, reply_markup, parse_mode))
        return _FakeReply(self)


@dataclass
class _FakeService:
    answers: List[Tuple[str, str, Any]] = field(default_factory=list)
    resets: List[str] = field(default_factory=list)
    reply_text: str = "deepseek reply"

    def answer(self, *, question: str, chat_id: str, attachment: Any = None) -> str:
        self.answers.append((question, chat_id, attachment))
        return self.reply_text

    def reset(self, chat_id: str) -> None:
        self.resets.append(chat_id)


@dataclass
class _FakeRobbieService:
    answers: List[Tuple[str, Any, str]] = field(default_factory=list)
    resets: List[str] = field(default_factory=list)
    reply_text: str = "robbie reply"

    def answer(self, *, question: str, trading_system: Any, chat_id: str) -> str:
        self.answers.append((question, trading_system, chat_id))
        return self.reply_text

    def reset(self, chat_id: str) -> None:
        self.resets.append(chat_id)


def _build_update(text: str = "hello", chat_id: int = 5747207752, chat_type: str = "private", caption: str = ""):
    chat = _FakeChat(id=chat_id, type=chat_type)
    message = _FakeMessage(chat=chat, text=text, caption=caption)
    return SimpleNamespace(
        effective_chat=chat,
        message=message,
        callback_query=None,
    )


def test_deepseek_bot_replies_to_plain_text() -> None:
    bot = DeepSeekTelegramBot(token="test-token", allowed_chat_id="5747207752")
    fake_service = _FakeService()
    fake_robbie = _FakeRobbieService()
    update = _build_update(text="hello DeepSeek")
    ctx = SimpleNamespace(args=[])

    async def run() -> None:
        with (
            patch("deepseek_bot.get_deepseek_chat_service", return_value=fake_service),
            patch("services.robbie_chat_service.get_chat_service", return_value=fake_robbie),
        ):
            await bot._on_text(update, ctx)

    asyncio.run(run())

    assert fake_service.answers == []
    assert len(fake_robbie.answers) == 1
    assert fake_robbie.answers[0][0] == "hello DeepSeek"
    assert fake_robbie.answers[0][2] == "5747207752"
    assert update.message.replies[0][0] == "The bot is thinking..."
    assert update.message.edits[0][0] == "robbie reply"
    assert update.message.edits[0][2] == ParseMode.HTML
    assert update.effective_chat.actions == ["typing"]


def test_deepseek_bot_replies_to_chat_command() -> None:
    bot = DeepSeekTelegramBot(token="test-token", allowed_chat_id="5747207752")
    fake_service = _FakeService()
    fake_robbie = _FakeRobbieService()
    update = _build_update(text="/chat how are markets looking?")
    ctx = SimpleNamespace(args=["how", "are", "markets", "looking?"])

    async def run() -> None:
        with (
            patch("deepseek_bot.get_deepseek_chat_service", return_value=fake_service),
            patch("services.robbie_chat_service.get_chat_service", return_value=fake_robbie),
        ):
            await bot._cmd_chat(update, ctx)

    asyncio.run(run())

    assert fake_service.answers == []
    assert len(fake_robbie.answers) == 1
    assert fake_robbie.answers[0][0] == "how are markets looking?"
    assert update.message.replies[0][0] == "The bot is thinking..."
    assert update.message.edits[0][0] == "robbie reply"
    assert update.message.edits[0][2] == ParseMode.HTML


def test_deepseek_bot_renders_markdownish_output_for_telegram() -> None:
    bot = DeepSeekTelegramBot(token="test-token", allowed_chat_id="5747207752")
    fake_service = _FakeService(reply_text="deepseek reply")
    fake_robbie = _FakeRobbieService(reply_text="**Entry Time:** `BNB_USD`")
    update = _build_update(text="show entry")
    ctx = SimpleNamespace(args=[])

    async def run() -> None:
        with (
            patch("deepseek_bot.get_deepseek_chat_service", return_value=fake_service),
            patch("services.robbie_chat_service.get_chat_service", return_value=fake_robbie),
        ):
            await bot._on_text(update, ctx)

    asyncio.run(run())

    assert update.message.edits[0][0] == "<b>Entry Time:</b> <code>BNB_USD</code>"
    assert update.message.edits[0][2] == ParseMode.HTML


def test_deepseek_bot_strips_markdown_headings_for_telegram() -> None:
    bot = DeepSeekTelegramBot(token="test-token", allowed_chat_id="5747207752")
    fake_service = _FakeService(reply_text="deepseek reply")
    fake_robbie = _FakeRobbieService(reply_text="## What This Means\n- Gold is firm")
    update = _build_update(text="gold")
    ctx = SimpleNamespace(args=[])

    async def run() -> None:
        with (
            patch("deepseek_bot.get_deepseek_chat_service", return_value=fake_service),
            patch("services.robbie_chat_service.get_chat_service", return_value=fake_robbie),
        ):
            await bot._on_text(update, ctx)

    asyncio.run(run())

    assert "##" not in update.message.edits[0][0]
    assert "What This Means" in update.message.edits[0][0]


def test_deepseek_bot_requires_allowed_private_chat() -> None:
    bot = DeepSeekTelegramBot(token="test-token", allowed_chat_id="5747207752")

    allowed_update = _build_update(chat_id=5747207752, chat_type="private")
    denied_update = _build_update(chat_id=111111111, chat_type="private")
    group_update = _build_update(chat_id=5747207752, chat_type="group")

    assert bot._is_allowed(allowed_update) is True
    assert bot._is_allowed(denied_update) is False
    assert bot._is_allowed(group_update) is False


def test_deepseek_bot_intro_mentions_bot_snapshot() -> None:
    intro = DeepSeekTelegramBot._intro_text().lower()
    assert "bot speaking directly" in intro
    assert "current thinking" in intro
    assert "macro/news" in intro
    assert "logs" in intro
    assert "image and chart questions" in intro


def test_deepseek_bot_handles_visual_messages_with_attachment_context() -> None:
    bot = DeepSeekTelegramBot(token="test-token", allowed_chat_id="5747207752")
    fake_service = _FakeService()
    fake_robbie = _FakeRobbieService()
    update = _build_update(text="", caption="")
    ctx = SimpleNamespace(args=[])
    attachment = {"kind": "image", "ocr_text": "XAG/USD hit TP2", "ocr_available": True}

    async def run() -> None:
        with (
            patch("deepseek_bot.get_deepseek_chat_service", return_value=fake_service),
            patch("services.robbie_chat_service.get_chat_service", return_value=fake_robbie),
            patch.object(bot, "_extract_attachment_context", return_value=attachment),
        ):
            await bot._on_visual_message(update, ctx)

    asyncio.run(run())

    assert fake_service.answers == [("Please analyze the attached image.", "5747207752", attachment)]
    assert fake_robbie.answers == []
    assert update.message.replies[0][0] == "The bot is analyzing the attachment..."
    assert update.message.edits[0][0] == "deepseek reply"
    assert update.message.edits[0][2] == ParseMode.HTML


def test_deepseek_bot_attachment_followup_text_stays_on_attachment_path() -> None:
    bot = DeepSeekTelegramBot(token="test-token", allowed_chat_id="5747207752")
    fake_service = _FakeService()
    fake_robbie = _FakeRobbieService()
    update = _build_update(text="can you see the image I posted")
    ctx = SimpleNamespace(args=[])

    async def run() -> None:
        with (
            patch("deepseek_bot.get_deepseek_chat_service", return_value=fake_service),
            patch("services.robbie_chat_service.get_chat_service", return_value=fake_robbie),
        ):
            await bot._on_text(update, ctx)

    asyncio.run(run())

    assert fake_service.answers == [("can you see the image I posted", "5747207752", None)]
    assert fake_robbie.answers == []


def test_deepseek_bot_reset_clears_both_chat_services() -> None:
    bot = DeepSeekTelegramBot(token="test-token", allowed_chat_id="5747207752")
    fake_service = _FakeService()
    fake_robbie = _FakeRobbieService()
    update = _build_update(text="/reset")
    ctx = SimpleNamespace(args=[])

    async def run() -> None:
        with (
            patch("deepseek_bot.get_deepseek_chat_service", return_value=fake_service),
            patch("services.robbie_chat_service.get_chat_service", return_value=fake_robbie),
        ):
            await bot._cmd_reset(update, ctx)

    asyncio.run(run())

    assert fake_service.resets == ["5747207752"]
    assert fake_robbie.resets == ["5747207752"]


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

        def post_init(self, value: Any) -> "_FakeBuilder":
            captured["post_init"] = value
            return self

        def build(self) -> _FakeApp:
            captured["built"] = True
            return _FakeApp()

    bot = DeepSeekTelegramBot(token="test-token", allowed_chat_id="5747207752")

    with patch("deepseek_bot.Application.builder", return_value=_FakeBuilder()):
        bot.run()

    asyncio.run(captured["post_init"](_FakeApp()))
    assert captured["token"] == "test-token"
    assert captured["built"] is True
    assert captured["post_init"] is not None
    assert "post_init" not in captured["run_polling_kwargs"]
    assert captured["run_polling_kwargs"]["allowed_updates"] is not None
    assert len(captured["commands"]) == 4
