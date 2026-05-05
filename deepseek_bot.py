from __future__ import annotations

import argparse
import asyncio
import hashlib
import html
import json
import os
import re
import shutil
import subprocess
import tempfile
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Dict, Optional

from telegram import BotCommand, InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.error import BadRequest, NetworkError, RetryAfter, TimedOut
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, ContextTypes, MessageHandler, filters

from config.config import DEEPSEEK_TELEGRAM_CHAT_ID, DEEPSEEK_TELEGRAM_TOKEN
from services.deepseek_chat_service import (
    _build_focus_asset_snapshot,
    _build_log_snapshot,
    get_deepseek_chat_service,
)
from utils.logger import get_logger

logger = get_logger()

_CHAT_TIMEOUT_SECONDS = 60.0
_MARKDOWNISH_TOKEN_RE = re.compile(r"(```[\s\S]+?```|`[^`\n]+`|\*\*[\s\S]+?\*\*|\*[^\*\n][^\n]*?\*)")
_ATX_HEADING_RE = re.compile(r"^(\s{0,3})#{1,6}\s+(.+?)\s*$")


class _SingleInstanceLock:
    def __init__(self, token: str):
        digest = hashlib.sha256(str(token or "").encode("utf-8")).hexdigest()[:16]
        self.path = Path("data") / "runtime_locks" / f"deepseek_bot_{digest}.lock"
        self._fd: Optional[int] = None

    def acquire(self) -> bool:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            self._fd = os.open(str(self.path), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            os.write(self._fd, str(os.getpid()).encode("ascii"))
            return True
        except FileExistsError:
            if self._remove_stale_lock():
                return self.acquire()
            return False

    def release(self) -> None:
        if self._fd is not None:
            try:
                os.close(self._fd)
            except OSError:
                pass
            self._fd = None
        try:
            self.path.unlink(missing_ok=True)
        except OSError:
            pass

    def _remove_stale_lock(self) -> bool:
        try:
            pid = int((self.path.read_text(encoding="utf-8").strip() or "0"))
        except Exception:
            pid = 0
        if pid <= 0:
            try:
                self.path.unlink(missing_ok=True)
                return True
            except OSError:
                return False
        try:
            os.kill(pid, 0)
            return False
        except ProcessLookupError:
            try:
                self.path.unlink(missing_ok=True)
                return True
            except OSError:
                return False
        except PermissionError:
            return False


class _SharedRuntimeTradingSystemProxy:
    """Expose enough runtime state for RobbieChatService in the standalone chat process."""

    def __init__(self) -> None:
        from core.state import state as shared_state

        try:
            shared_state.init_db()
        except Exception:
            pass
        self._state = shared_state

    def health_report(self) -> Dict[str, Any]:
        snapshot = _build_log_snapshot("latest bot health")
        runtime_err = [str(item).strip() for item in list(snapshot.get("runtime_err") or []) if str(item).strip()]
        blocker_matches = [str(item).strip() for item in list(snapshot.get("blocker_matches") or []) if str(item).strip()]
        scan_summary = [str(item).strip() for item in list(snapshot.get("signal_scan_summary") or []) if str(item).strip()]
        issues = []
        if runtime_err:
            issues.append("Recent runtime errors were detected in the local log tail.")
        if any("EXCEPTION" in item.upper() or "ERROR" in item.upper() for item in blocker_matches):
            issues.append("Recent signal blockers include exception or error lines.")
        status = "degraded" if issues else "running"
        return {
            "status": status,
            "issues": issues,
            "recent_error_count": len(runtime_err),
            "stale_sources": [],
            "never_seen_sources": [],
            "signal_diagnostics": {
                "summary_label": scan_summary[-1] if scan_summary else "",
            },
        }

    def get_positions(self) -> list[Dict[str, Any]]:
        try:
            return list(self._state.get_open_positions() or [])
        except Exception:
            return []

    def get_closed_trades(self, limit: int = 100) -> list[Dict[str, Any]]:
        try:
            return list(self._state.get_closed_positions(limit=limit) or [])
        except Exception:
            return []

    def get_daily_stats(self) -> Dict[str, Any]:
        perf = self.get_performance()
        return {
            "daily_trades": int(perf.get("daily_trades", 0) or 0),
            "daily_pnl": float(perf.get("daily_pnl", 0.0) or 0.0),
        }

    def get_performance(self) -> Dict[str, Any]:
        try:
            return dict(self._state.get_performance() or {})
        except Exception:
            return {
                "balance": float(getattr(self._state, "balance", 0.0) or 0.0),
                "daily_trades": int(getattr(self._state, "daily_trades", 0) or 0),
                "daily_pnl": float(getattr(self._state, "daily_pnl", 0.0) or 0.0),
                "open_positions": len(self.get_positions()),
            }

    def get_balance(self) -> float:
        performance = self.get_performance()
        return float(performance.get("balance", getattr(self._state, "balance", 0.0)) or 0.0)

    def get_runtime_asset_snapshot(self, asset: str) -> Dict[str, Any]:
        return dict(_build_focus_asset_snapshot(asset) or {})

    def get_top_ranked_opportunities(self, limit: int = 10, refresh: bool = False, allow_refresh_when_empty: bool = False) -> list[Dict[str, Any]]:
        return []

    def scan_top_ranked_opportunities(self, limit: int = 10) -> list[Dict[str, Any]]:
        return []

    @staticmethod
    def _command_response_key(request_id: str) -> str:
        return f"DASHBOARD_COMMAND_RESPONSE:{request_id}"

    def _send_runtime_command(self, action: str, payload: Dict[str, Any], *, timeout_seconds: float = 20.0) -> Dict[str, Any]:
        request_id = uuid.uuid4().hex
        command = {
            "request_id": request_id,
            "action": action,
            "payload": payload,
            "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "source": "deepseek_chat",
        }
        response_key = self._command_response_key(request_id)
        client = None
        dedicated_client = False
        try:
            from services.redis_pool import get_client, get_dedicated_client

            client = get_dedicated_client(socket_timeout=1.0)
            dedicated_client = client is not None
            if client is None:
                client = get_client()
            if client is None:
                return {"success": False, "error": "Redis command bridge unavailable"}
            try:
                client.delete(response_key)
            except Exception:
                pass
            client.rpush("DASHBOARD_COMMAND_QUEUE", json.dumps(command, default=str))
            deadline = time.monotonic() + max(1.0, float(timeout_seconds or 20.0))
            while time.monotonic() < deadline:
                raw = client.get(response_key)
                if raw:
                    try:
                        client.delete(response_key)
                    except Exception:
                        pass
                    response = json.loads(raw) if isinstance(raw, str) else json.loads(raw.decode("utf-8"))
                    return response if isinstance(response, dict) else {"success": False, "error": "Invalid command response"}
                time.sleep(0.15)
            return {"success": False, "error": "TradingCore command bridge timed out"}
        except Exception as exc:
            return {"success": False, "error": str(exc)}
        finally:
            if dedicated_client and client is not None:
                try:
                    client.close()
                except Exception:
                    pass

    def pause_trading(self, *, reason: str = "Robbie chat pause", until: Optional[Any] = None, source: str = "deepseek_chat") -> Dict[str, Any]:
        until_utc = until.isoformat() if hasattr(until, "isoformat") else str(until or "")
        return self._send_runtime_command(
            "pause_trading",
            {"reason": reason, "until_utc": until_utc, "source": source},
        )

    def resume_trading(self, *, source: str = "deepseek_chat") -> Dict[str, Any]:
        return self._send_runtime_command("resume_trading", {"source": source})


def _keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[
            InlineKeyboardButton("🗑 Reset chat", callback_data="reset"),
            InlineKeyboardButton("ℹ️ Help", callback_data="help"),
        ]]
    )


def _render_telegram_html(text: str) -> str:
    raw = _strip_markdown_headings(str(text or "").replace("\r\n", "\n"))
    if not raw:
        return ""

    parts: list[str] = []
    cursor = 0
    for match in _MARKDOWNISH_TOKEN_RE.finditer(raw):
        parts.append(html.escape(raw[cursor:match.start()]))
        token = match.group(0)
        if token.startswith("```") and token.endswith("```"):
            parts.append(f"<pre>{html.escape(token[3:-3].strip())}</pre>")
        elif token.startswith("`") and token.endswith("`"):
            parts.append(f"<code>{html.escape(token[1:-1])}</code>")
        elif token.startswith("**") and token.endswith("**"):
            parts.append(f"<b>{html.escape(token[2:-2])}</b>")
        elif token.startswith("*") and token.endswith("*"):
            parts.append(f"<i>{html.escape(token[1:-1])}</i>")
        else:
            parts.append(html.escape(token))
        cursor = match.end()
    parts.append(html.escape(raw[cursor:]))
    return "".join(parts)


def _strip_markdown_headings(text: str) -> str:
    lines = str(text or "").split("\n")
    rendered: list[str] = []
    in_fence = False
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("```"):
            in_fence = not in_fence
            rendered.append(line)
            continue
        if not in_fence:
            line = _ATX_HEADING_RE.sub(r"\1\2", line)
        rendered.append(line)
    return "\n".join(rendered)


class DeepSeekTelegramBot:
    def __init__(self, token: str, allowed_chat_id: str = ""):
        self.token = str(token or "").strip()
        self.allowed_chat_id = str(allowed_chat_id or "").strip()
        self.application: Optional[Application] = None
        self._thread: Optional[threading.Thread] = None

    def run(self) -> None:
        if not self.token:
            raise RuntimeError("DeepSeek Telegram token is missing. Set DEEPSEEK_TELEGRAM_TOKEN.")

        lock = _SingleInstanceLock(self.token)
        if not lock.acquire():
            logger.error("[DeepSeekBot] another local DeepSeek Telegram poller is already running; exiting")
            return

        try:
            self.application = (
                Application.builder()
                .token(self.token)
                .connect_timeout(30)
                .read_timeout(30)
                .write_timeout(30)
                .post_init(self._post_init)
                .build()
            )
            self._register_handlers()
            self.application.run_polling(
                allowed_updates=Update.ALL_TYPES,
                drop_pending_updates=True,
                stop_signals=None,
            )
        finally:
            lock.release()

    def start_background(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self.run, daemon=True, name="deepseek-bot")
        self._thread.start()

    def _register_handlers(self) -> None:
        app = self.application
        if app is None:
            return

        image_filter = filters.PHOTO
        try:
            image_filter = image_filter | filters.Document.IMAGE
        except Exception:
            pass

        app.add_handler(CommandHandler("start", self._cmd_start))
        app.add_handler(CommandHandler("help", self._cmd_start))
        app.add_handler(CommandHandler("chat", self._cmd_chat))
        app.add_handler(CommandHandler("reset", self._cmd_reset))
        app.add_handler(CommandHandler("resetchat", self._cmd_reset))
        app.add_handler(MessageHandler(image_filter & ~filters.COMMAND, self._on_visual_message))
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
            "The bot voice is ready.\n\n"
            "Use /chat or send me a message and I will answer as the trading bot speaking directly from current runtime context.\n"
            "Normal text questions use the live bot brain: positions, recent trades, current thinking, asset context, macro/news, logs, and code-path context when available.\n"
            "Image and chart questions use the attachment analyzer, including OCR text when available.\n"
            "You can also ask me to pause or resume new trading entries; existing broker position management stays active while paused.\n\n"
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
        try:
            from services.robbie_chat_service import get_chat_service

            get_chat_service().reset(str(update.effective_chat.id))
        except Exception:
            pass
        if update.message:
            await update.message.reply_text("Bot chat memory cleared for this chat.", reply_markup=_keyboard())

    async def _on_text(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if not self._is_allowed(update):
            await self._deny(update)
            return
        question = str(update.message.text or "").strip()
        if not question:
            return
        await self._answer_message(update, question)

    async def _on_visual_message(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if not self._is_allowed(update):
            await self._deny(update)
            return
        attachment = await self._extract_attachment_context(update)
        question = str(update.message.caption or "").strip() or "Please analyze the attached image."
        if "attach" not in question.lower() and "image" not in question.lower():
            question = f"Use the attached image context for this question: {question}"
        await self._answer_message(update, question, attachment=attachment)

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
            try:
                from services.robbie_chat_service import get_chat_service

                get_chat_service().reset(str(update.effective_chat.id))
            except Exception:
                pass
            await query.message.reply_text("Bot chat memory cleared for this chat.", reply_markup=_keyboard())
            return
        await query.message.reply_text(self._intro_text(), reply_markup=_keyboard())

    async def _answer_message(self, update: Update, question: str, *, attachment: Optional[Dict[str, Any]] = None) -> None:
        await update.message.chat.send_action("typing")
        thinking_text = "The bot is analyzing the attachment..." if attachment else "The bot is thinking..."
        placeholder = await update.message.reply_text(thinking_text)
        answer = await self._run_answer(question, chat_id=str(update.effective_chat.id), attachment=attachment)
        await self._replace_placeholder_with_chunks(placeholder, answer)

    async def _run_answer(self, question: str, *, chat_id: str, attachment: Optional[Dict[str, Any]] = None) -> str:
        try:
            from services.robbie_chat_service import get_chat_service

            runtime_proxy = _SharedRuntimeTradingSystemProxy()
            answer_fn = get_chat_service().answer
            kwargs = {
                "question": question,
                "trading_system": runtime_proxy,
                "chat_id": chat_id,
                "attachment": attachment,
            }
            return await asyncio.wait_for(
                asyncio.to_thread(answer_fn, **kwargs),
                timeout=_CHAT_TIMEOUT_SECONDS,
            )
        except asyncio.TimeoutError:
            logger.warning(f"[DeepSeekBot] bot chat timed out after {_CHAT_TIMEOUT_SECONDS:.0f}s")
            return "The bot took too long to answer. Try a shorter message."
        except Exception as exc:
            logger.error(f"[DeepSeekBot] chat error: {exc}", exc_info=True)
            return f"The bot hit a chat error: {exc}"

    @staticmethod
    def _ocr_image_text(path: Path) -> str:
        tesseract = shutil.which("tesseract")
        if not tesseract:
            return ""
        try:
            proc = subprocess.run(
                [tesseract, str(path), "stdout", "--psm", "6"],
                capture_output=True,
                text=True,
                timeout=20,
                check=False,
            )
        except Exception:
            return ""
        text = str(proc.stdout or "").strip()
        text = re.sub(r"\s+\n", "\n", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text[:4000]

    async def _extract_attachment_context(self, update: Update) -> Dict[str, Any]:
        message = update.message
        if message is None:
            return {}

        payload: Dict[str, Any] = {
            "kind": "image",
            "caption": str(message.caption or "").strip(),
        }
        temp_path: Optional[Path] = None
        try:
            if message.photo:
                photo = message.photo[-1]
                payload["width"] = getattr(photo, "width", None)
                payload["height"] = getattr(photo, "height", None)
                tg_file = await photo.get_file()
                suffix = ".jpg"
            elif message.document is not None:
                doc = message.document
                payload["file_name"] = str(getattr(doc, "file_name", "") or "")
                payload["mime_type"] = str(getattr(doc, "mime_type", "") or "")
                tg_file = await doc.get_file()
                suffix = Path(payload["file_name"] or "attachment.bin").suffix or ".bin"
            else:
                return payload

            with tempfile.NamedTemporaryFile(prefix="deepseek_attach_", suffix=suffix, delete=False) as handle:
                temp_path = Path(handle.name)
            await tg_file.download_to_drive(custom_path=str(temp_path))

            ocr_text = self._ocr_image_text(temp_path)
            if ocr_text:
                payload["ocr_text"] = ocr_text
                payload["ocr_source"] = "tesseract"
            payload["ocr_available"] = bool(shutil.which("tesseract"))
        except Exception as exc:
            payload["attachment_error"] = str(exc)
        finally:
            if temp_path is not None:
                try:
                    temp_path.unlink(missing_ok=True)
                except Exception:
                    pass
        return payload

    async def _replace_placeholder_with_chunks(self, message, text: str) -> None:
        chunks = [str(text or "")[i:i + 4000] for i in range(0, max(len(str(text or "")), 1), 4000)]
        for index, chunk in enumerate(chunks):
            is_last = index == len(chunks) - 1
            reply_markup = _keyboard() if is_last else None
            rendered_chunk = _render_telegram_html(chunk)
            if index == 0:
                try:
                    await message.edit_text(rendered_chunk, parse_mode=ParseMode.HTML, reply_markup=reply_markup)
                except BadRequest as exc:
                    lowered = str(exc).lower()
                    if "message is not modified" in lowered:
                        continue
                    if "message to edit not found" in lowered:
                        await message.reply_text(rendered_chunk, parse_mode=ParseMode.HTML, reply_markup=reply_markup)
                    else:
                        raise
            else:
                await message.reply_text(rendered_chunk, parse_mode=ParseMode.HTML, reply_markup=reply_markup)

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
