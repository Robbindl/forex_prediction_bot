from __future__ import annotations

import argparse
import asyncio
from typing import Optional

from telegram import BotCommand, InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.error import BadRequest, NetworkError, RetryAfter, TimedOut
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, ContextTypes, MessageHandler, filters

from config.config import DEEPSEEK_TELEGRAM_CHAT_ID, DEEPSEEK_TELEGRAM_TOKEN
from services.deepseek_chat_service import get_deepseek_chat_service
from utils.logger import get_logger

logger = get_logger()

_CHAT_TIMEOUT_SECONDS = 60.0


def _keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[
            InlineKeyboardButton("🗑 Reset chat", callback_data="reset"),
            InlineKeyboardButton("ℹ️ Help", callback_data="help"),
        ]]
    )


class DeepSeekTelegramBot:
    def __init__(self, token: str, allowed_chat_id: str = ""):
        self.token = str(token or "").strip()
        self.allowed_chat_id = str(allowed_chat_id or "").strip()
        self.application: Optional[Application] = None

    def run(self) -> None:
        if not self.token:
            raise RuntimeError("DeepSeek Telegram token is missing. Set DEEPSEEK_TELEGRAM_TOKEN.")

        self.application = (
            Application.builder()
            .token(self.token)
            .connect_timeout(30)
            .read_timeout(30)
            .write_timeout(30)
            .build()
        )
        self._register_handlers()
        self.application.run_polling(
            allowed_updates=Update.ALL_TYPES,
            drop_pending_updates=True,
            post_init=self._post_init,
        )

    def _register_handlers(self) -> None:
        app = self.application
        if app is None:
            return

        app.add_handler(CommandHandler("start", self._cmd_start))
        app.add_handler(CommandHandler("help", self._cmd_start))
        app.add_handler(CommandHandler("chat", self._cmd_chat))
        app.add_handler(CommandHandler("reset", self._cmd_reset))
        app.add_handler(CommandHandler("resetchat", self._cmd_reset))
        app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self._on_text))
        app.add_handler(CallbackQueryHandler(self._on_button))
        app.add_error_handler(self._on_error)

    async def _post_init(self, app: Application) -> None:
        try:
            await app.bot.set_my_commands(
                [
                    BotCommand("start", "Open the DeepSeek chat bot"),
                    BotCommand("chat", "Ask DeepSeek anything"),
                    BotCommand("reset", "Clear chat memory"),
                    BotCommand("help", "Show help"),
                ]
            )
        except Exception as exc:
            logger.debug(f"[DeepSeekBot] command registration skipped: {exc}")

    def _is_allowed(self, update: Update) -> bool:
        chat = update.effective_chat
        if chat is None:
            return False
        if chat.type != "private":
            return False
        if self.allowed_chat_id and str(chat.id) != self.allowed_chat_id:
            return False
        return True

    async def _deny(self, update: Update) -> None:
        message = "Use this bot in a private chat only."
        if self.allowed_chat_id:
            message = "This DeepSeek bot is locked to a specific private chat."
        if update.message:
            await update.message.reply_text(message)
        elif update.callback_query and update.callback_query.message:
            await update.callback_query.message.reply_text(message)

    @staticmethod
    def _intro_text() -> str:
        return (
            "DeepSeek private chat is ready.\n\n"
            "Send me a message and I will answer directly.\n"
            "This bot is separate from the trading control bot and does not expose signals, positions, dashboards, or other menus.\n\n"
            "Use /reset to clear chat memory."
        )

    async def _cmd_start(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if not self._is_allowed(update):
            await self._deny(update)
            return
        if ctx.args:
            question = " ".join(ctx.args).strip()
            if question:
                await self._answer_message(update, question)
                return
        await update.message.reply_text(self._intro_text(), reply_markup=_keyboard())

    async def _cmd_chat(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if not self._is_allowed(update):
            await self._deny(update)
            return
        if ctx.args:
            question = " ".join(ctx.args).strip()
            if question:
                await self._answer_message(update, question)
                return
        await update.message.reply_text(self._intro_text(), reply_markup=_keyboard())

    async def _cmd_reset(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if not self._is_allowed(update):
            await self._deny(update)
            return
        get_deepseek_chat_service().reset(str(update.effective_chat.id))
        if update.message:
            await update.message.reply_text("DeepSeek chat memory cleared for this chat.", reply_markup=_keyboard())

    async def _on_text(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if not self._is_allowed(update):
            await self._deny(update)
            return
        question = str(update.message.text or "").strip()
        if not question:
            return
        await self._answer_message(update, question)

    async def _on_button(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        if query is None:
            return
        await query.answer()
        if not self._is_allowed(update):
            await self._deny(update)
            return
        data = str(query.data or "")
        if data == "reset":
            get_deepseek_chat_service().reset(str(update.effective_chat.id))
            await query.message.reply_text("DeepSeek chat memory cleared for this chat.", reply_markup=_keyboard())
            return
        await query.message.reply_text(self._intro_text(), reply_markup=_keyboard())

    async def _answer_message(self, update: Update, question: str) -> None:
        await update.message.chat.send_action("typing")
        placeholder = await update.message.reply_text("DeepSeek is thinking...")
        answer = await self._run_answer(question, chat_id=str(update.effective_chat.id))
        await self._replace_placeholder_with_chunks(placeholder, answer)

    async def _run_answer(self, question: str, *, chat_id: str) -> str:
        try:
            return await asyncio.wait_for(
                asyncio.to_thread(
                    get_deepseek_chat_service().answer,
                    question=question,
                    chat_id=chat_id,
                ),
                timeout=_CHAT_TIMEOUT_SECONDS,
            )
        except asyncio.TimeoutError:
            logger.warning(f"[DeepSeekBot] chat timed out after {_CHAT_TIMEOUT_SECONDS:.0f}s")
            return "DeepSeek took too long to answer. Try a shorter message."
        except Exception as exc:
            logger.error(f"[DeepSeekBot] chat error: {exc}", exc_info=True)
            return f"DeepSeek hit an error: {exc}"

    async def _replace_placeholder_with_chunks(self, message, text: str) -> None:
        chunks = [str(text or "")[i:i + 4000] for i in range(0, max(len(str(text or "")), 1), 4000)]
        for index, chunk in enumerate(chunks):
            is_last = index == len(chunks) - 1
            reply_markup = _keyboard() if is_last else None
            if index == 0:
                try:
                    await message.edit_text(chunk, reply_markup=reply_markup)
                except BadRequest as exc:
                    lowered = str(exc).lower()
                    if "message is not modified" in lowered:
                        continue
                    if "message to edit not found" in lowered:
                        await message.reply_text(chunk, reply_markup=reply_markup)
                    else:
                        raise
            else:
                await message.reply_text(chunk, reply_markup=reply_markup)

    async def _on_error(self, update: object, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        error = getattr(ctx, "error", None)
        if isinstance(error, (NetworkError, TimedOut, RetryAfter)):
            logger.warning(f"[DeepSeekBot] transient Telegram error: {error}")
            return
        logger.error(f"[DeepSeekBot] handler error: {error}", exc_info=error)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Standalone DeepSeek Telegram bot")
    parser.add_argument("--token", type=str, default=DEEPSEEK_TELEGRAM_TOKEN)
    parser.add_argument("--chat-id", type=str, default=DEEPSEEK_TELEGRAM_CHAT_ID)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    bot = DeepSeekTelegramBot(token=args.token, allowed_chat_id=args.chat_id)
    logger.info("[DeepSeekBot] starting standalone DeepSeek chat bot")
    bot.run()


if __name__ == "__main__":
    main()
