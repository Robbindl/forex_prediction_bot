from __future__ import annotations

import asyncio
import threading
import time
from concurrent.futures import TimeoutError as FutureTimeoutError
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from telegram import (
    BotCommand,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    MenuButtonCommands,
    Update,
)
from telegram.constants import ParseMode
from telegram.error import BadRequest, NetworkError, RetryAfter, TimedOut
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

from config.config import ROBBIE_CHAT_TIMEOUT_SECONDS
from core.assets import registry
from services.live_position_pricing import resolve_live_position_snapshot
from utils.display_time import (
    display_timezone_label,
    format_display_datetime,
    now_in_display_timezone,
    to_display_datetime,
)
from utils.logger import logger

# ── Conversation state ────────────────────────────────────────────────────────
WAITING_ASK_ASSET    = 1
WAITING_ASK_QUESTION = 2
WAITING_CHAT_MESSAGE = 3
_CHAT_HANDLER_TIMEOUT_SECONDS = max(60.0, float(ROBBIE_CHAT_TIMEOUT_SECONDS or 45) + 15.0)

# ── Asset display names ───────────────────────────────────────────────────────
_DISPLAY = {
    "BTC-USD": "₿ BTC",  "ETH-USD": "Ξ ETH",   "BNB-USD": "BNB",
    "XRP-USD": "XRP",    "SOL-USD": "SOL",
    "EUR/USD": "EUR/USD","EUR/JPY": "EUR/JPY",  "EUR/GBP": "EUR/GBP",  "GBP/USD": "GBP/USD",
    "USD/JPY": "USD/JPY","AUD/USD": "AUD/USD",  "NZD/USD": "NZD/USD",  "USD/CAD": "USD/CAD",
    "USD/CHF": "USD/CHF", "GBP/JPY": "GBP/JPY",
    "XAU/USD": "Gold",   "XAG/USD": "Silver",    "WTI": "WTI Oil",
    "US500":   "S&P 500","US30":  "Dow Jones",  "US100": "Nasdaq",
    "UK100":   "FTSE100","GER40": "Germany 40","AUS200": "Australia 200","JPN225": "Japan 225",
}

_CATEGORY_ORDER = ["crypto", "forex", "commodities", "indices"]
_CATEGORY_ASSETS: Dict[str, List[str]] = {
    category: registry.assets_by_category(category)
    for category in _CATEGORY_ORDER
}
_ASK_CATEGORY_LABELS = {
    "crypto": "🪙 Crypto",
    "forex": "💱 Forex",
    "commodities": "📦 Commodities",
    "indices": "📉 Indices",
}

# ── Keyboard builders ─────────────────────────────────────────────────────────

def _units_to_lots(asset: str, category: str, units: float) -> float:
    """Convert raw position units back to lots for display."""
    try:
        from risk.position_sizer import CONTRACT_SPECS, _DEFAULTS
        spec = CONTRACT_SPECS.get(asset) or _DEFAULTS.get(category, {})
        contract = spec.get("contract", 1)
        return units / contract if contract > 0 else units
    except Exception:
        return units


def _kb(*rows) -> InlineKeyboardMarkup:
    """Helper — pass lists of (label, callback_data) tuples."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(label, callback_data=data) for label, data in row]
        for row in rows
    ])


class _SafeCallbackQuery:
    def __init__(self, query: Any, edit_fn: Any):
        self._query = query
        self._edit_fn = edit_fn

    def __getattr__(self, name: str) -> Any:
        return getattr(self._query, name)

    async def edit_message_text(self, text: str, parse_mode=ParseMode.MARKDOWN, reply_markup=None, **kwargs):
        return await self._edit_fn(
            self._query,
            text,
            parse_mode=parse_mode,
            reply_markup=reply_markup,
            **kwargs,
        )

def _main_menu_keyboard(summary: Optional[Dict[str, Any]] = None) -> InlineKeyboardMarkup:
    summary = summary or {}
    open_positions = max(0, int(summary.get("open_positions", 0) or 0))
    diary_trades = max(0, int(summary.get("diary_trades", 0) or 0))
    is_running = bool(summary.get("is_running", False))

    positions_label = f"📈 Positions ({open_positions})"
    diary_label = f"📔 Diary ({diary_trades})" if diary_trades > 0 else "📔 Diary"
    run_label = "⏸ Pause" if is_running else "▶️ Resume"
    run_action = "pause" if is_running else "resume"

    return _kb(
        [("📊 Status",    "status"),   (positions_label, "positions")],
        [("💰 Balance",   "balance"),  ("🎯 Signals",    "signals")],
        [("🧠 Ask Robbie","chat_menu"), (diary_label,    "diary")],
        [("😶 Mood",      "mood"),      ("📡 Market",    "market")],
        [("🏠 Menu",      "menu"),      (run_label,      run_action)],
    )

def _back_button(dest: str = "menu") -> List:
    return [("◀️ Back", dest)]

def _category_keyboard() -> InlineKeyboardMarkup:
    return _kb(
        [("🪙 Crypto",      "cat:crypto"),  ("💱 Forex",       "cat:forex")],
        [("📦 Commodities", "cat:commodities"), ("📉 Indices", "cat:indices")],
        _back_button(),
    )

def _asset_keyboard(category: str) -> InlineKeyboardMarkup:
    assets = _CATEGORY_ASSETS.get(category, [])
    rows   = []
    # 3 columns
    for i in range(0, len(assets), 3):
        row = []
        for asset in assets[i:i+3]:
            label = _DISPLAY.get(asset, asset)
            row.append((label, f"sig:{asset}"))
        rows.append(row)
    rows.append(_back_button("signals"))
    return _kb(*rows)

# ── Ask Robbie keyboard builders ──────────────────────────────────────────────

_ASK_CATEGORY_ASSETS: Dict[str, List[str]] = {
    _ASK_CATEGORY_LABELS[category]: list(_CATEGORY_ASSETS.get(category, []))
    for category in _CATEGORY_ORDER
}

_ASK_QUESTIONS = [
    ("📊 Should I trade?",       "trade"),
    ("🔍 Explain the signal",    "explain"),
    ("⚠️ Risk analysis",         "risk"),
    ("🧠 What do you remember?", "remember"),
    ("💬 What's the sentiment?",  "sentiment"),
    ("🎯 How confident are you?", "confidence"),
]

_CHAT_QUICK_PROMPTS = {
    "top": "What are the top setups right now?",
    "issues": "What issues are you experiencing right now?",
    "learned": "What have you learned recently?",
    "market": "What is currently happening that affects trading right now?",
    "macro": "How can CPI or FOMC affect the current market?",
    "holiday": "When is the next bank holiday and how could it affect trading?",
    "outlook": "Where do you see Bitcoin in the next 2 years?",
    "adjust": "How should you adjust yourself right now?",
}

def _ask_category_keyboard() -> InlineKeyboardMarkup:
    rows = [(emoji_cat, f"askcat:{emoji_cat}") for emoji_cat in _ASK_CATEGORY_ASSETS]
    # 2 per row
    paired = [rows[i:i+2] for i in range(0, len(rows), 2)]
    return _kb(*paired, _back_button())

def _ask_asset_keyboard(emoji_cat: str) -> InlineKeyboardMarkup:
    assets = _ASK_CATEGORY_ASSETS.get(emoji_cat, [])
    rows   = []
    for i in range(0, len(assets), 3):
        row = []
        for asset in assets[i:i+3]:
            label = _DISPLAY.get(asset, asset)
            row.append((label, f"askasset:{asset}"))
        rows.append(row)
    rows.append([("◀️ Back", "ask_asset_menu")])
    return _kb(*rows)

def _ask_question_keyboard(asset: str) -> InlineKeyboardMarkup:
    rows = []
    for i in range(0, len(_ASK_QUESTIONS), 2):
        row = []
        for label, qkey in _ASK_QUESTIONS[i:i+2]:
            row.append((label, f"askq:{asset}:{qkey}"))
        rows.append(row)
    rows.append([("◀️ Back", "ask_asset_menu")])
    return _kb(*rows)


def _chat_shortcuts_keyboard() -> InlineKeyboardMarkup:
    return _kb(
        [("📍 Top setups", "chatq:top"), ("🩺 Issues", "chatq:issues")],
        [("🧠 What learned", "chatq:learned"), ("📡 Market now", "chatq:market")],
        [("🏛 Macro", "chatq:macro"), ("📅 Holiday", "chatq:holiday")],
        [("🔭 Outlook", "chatq:outlook"), ("⚙️ Adjust", "chatq:adjust")],
        [("🗑 Reset chat", "chat_reset")],
        [("🎯 Asset Q&A", "ask_asset_menu"), ("🏠 Menu", "menu")],
    )


def _bot_menu_commands() -> List[BotCommand]:
    return [
        BotCommand("menu", "Open the control panel"),
        BotCommand("status", "Show system status"),
        BotCommand("positions", "Show open positions"),
        BotCommand("balance", "Show account balance"),
        BotCommand("signal", "Review a signal for an asset"),
        BotCommand("why", "Explain the current signal for an asset"),
        BotCommand("history", "Show recent trade history"),
        BotCommand("chat", "Talk to Robbie freely"),
        BotCommand("resetchat", "Clear Robbie chat memory"),
        BotCommand("ask", "Ask Robbie about an asset"),
        BotCommand("pause", "Pause the bot"),
        BotCommand("resume", "Resume the bot"),
    ]


# ══════════════════════════════════════════════════════════════════════════════
# TelegramCommander
# ══════════════════════════════════════════════════════════════════════════════

class TelegramCommander:
    """
    Full-featured Telegram bot with inline keyboard navigation.
    trading_system must be a TradingCore instance.
    """

    def __init__(self, token: str, chat_id: str, trading_system: Any):
        self.token          = token
        self.chat_id        = str(chat_id)
        self.trading_system = trading_system
        self.application:   Optional[Application] = None
        self.is_running:    bool = False
        self._loop:         Optional[asyncio.AbstractEventLoop] = None
        self._thread:       Optional[threading.Thread] = None
        # Rate limiter: (timestamp list)
        self._rl_times:     List[float] = []
        self._rl_lock       = threading.Lock()

    # ── Startup ───────────────────────────────────────────────────────────────

    def start(self) -> None:
        try:
            self.application = (
                Application.builder()
                .token(self.token)
                .connect_timeout(30)
                .read_timeout(30)
                .write_timeout(30)
                .build()
            )
            self._register_handlers()
            # Mark running before the worker thread starts so the polling
            # loop does not exit immediately on startup.
            self.is_running = True
            self._thread = threading.Thread(
                target=self._run_bot, daemon=True, name="telegram-bot"
            )
            self._thread.start()

            # Wait for loop to be live
            for _ in range(60):
                if self._loop and self._loop.is_running():
                    break
                time.sleep(0.1)

            logger.info("✅ TelegramCommander started")
            self.send_message(
                "🤖 *Robbie is online*\n\nUse Menu or /menu to open the control panel.",
            )
        except Exception as e:
            logger.error(f"[Telegram] start error: {e}", exc_info=True)
            self.is_running = False

    def _run_bot(self) -> None:
        while self.is_running:
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)
            try:
                self._loop.run_until_complete(self._polling())
            except Exception as e:
                logger.warning(
                    f"[Telegram] polling crashed: {e} — retrying in 5s (network issues are recoverable)"
                )
                time.sleep(5)
                continue
            finally:
                try:
                    self._loop.close()
                except Exception:
                    pass
        logger.info("[Telegram] polling thread exiting")

    async def _polling(self) -> None:
        while self.is_running:
            try:
                async with self.application:
                    await self.application.start()
                    await self._configure_bot_menu()
                    await self.application.updater.start_polling(
                        allowed_updates=Update.ALL_TYPES,
                        drop_pending_updates=True,
                        timeout=20,
                        error_callback=self._handle_polling_error,
                    )
                    while self.is_running:
                        await asyncio.sleep(1)
                    await self.application.updater.stop()
                    await self.application.stop()
            except (NetworkError, TimedOut, RetryAfter) as e:
                logger.warning(f"[Telegram] Network issue: {e} — retrying in 5s")
                await asyncio.sleep(5)
                continue
            except Exception as e:
                logger.error(f"[Telegram] polling exception: {e}", exc_info=True)
                await asyncio.sleep(5)
                continue
            else:
                break

    async def _configure_bot_menu(self) -> None:
        if not self.application:
            return
        try:
            await self.application.bot.set_my_commands(_bot_menu_commands())
        except Exception as exc:
            logger.warning(f"[Telegram] failed to register bot commands: {exc}")
        try:
            await self.application.bot.set_chat_menu_button(menu_button=MenuButtonCommands())
        except Exception as exc:
            logger.warning(f"[Telegram] failed to register chat menu button: {exc}")

    def _handle_polling_error(self, error: Exception) -> None:
        msg = str(error or "")
        if isinstance(error, (NetworkError, TimedOut, RetryAfter)):
            logger.warning(f"[Telegram] polling network issue: {msg}")
            return
        logger.error(f"[Telegram] polling callback error: {msg}")

    def stop(self) -> None:
        self.is_running = False
        try:
            self.send_message("🛑 Robbie going offline.")
        except Exception:
            pass

    # ── Handler registration ──────────────────────────────────────────────────

    def _register_handlers(self) -> None:
        app = self.application

        # /start and /menu → main menu
        app.add_handler(CommandHandler("start", self._cmd_menu))
        app.add_handler(CommandHandler("menu",  self._cmd_menu))

        # Convenience text commands still work
        app.add_handler(CommandHandler("status",     self._cmd_status_direct))
        app.add_handler(CommandHandler("positions",  self._cmd_positions_direct))
        app.add_handler(CommandHandler("balance",    self._cmd_balance_direct))
        app.add_handler(CommandHandler("signal",     self._cmd_signal_direct))
        app.add_handler(CommandHandler("why",        self._cmd_why_direct))
        app.add_handler(CommandHandler("close",      self._cmd_close_direct))
        app.add_handler(CommandHandler("history",    self._cmd_history))
        app.add_handler(CommandHandler("resetchat",  self._cmd_reset_chat))
        app.add_handler(CommandHandler("pause",      self._cmd_pause_direct))
        app.add_handler(CommandHandler("resume",     self._cmd_resume_direct))
        app.add_handler(CommandHandler("reprice",    self._cmd_reprice_direct))
        app.add_handler(CommandHandler("reduce_weak", self._cmd_reduce_weak_direct))
        app.add_handler(CommandHandler("top_setups", self._cmd_top_setups_direct))

        # /ask conversation handler
        ask_conv = ConversationHandler(
            entry_points=[CommandHandler("ask", self._ask_entry)],
            states={
                WAITING_ASK_ASSET: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self._ask_got_asset)
                ],
                WAITING_ASK_QUESTION: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self._ask_got_question)
                ],
            },
            fallbacks=[CommandHandler("cancel", self._ask_cancel)],
            conversation_timeout=120,
        )
        app.add_handler(ask_conv)

        chat_conv = ConversationHandler(
            entry_points=[
                CommandHandler("chat", self._chat_entry),
                CallbackQueryHandler(self._chat_entry_from_button, pattern="^chat_menu$"),
            ],
            states={
                WAITING_CHAT_MESSAGE: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self._chat_got_message)
                ],
            },
            fallbacks=[
                CommandHandler("chat", self._chat_entry),
                CallbackQueryHandler(self._chat_entry_from_button, pattern="^chat_menu$"),
                CommandHandler("cancel", self._chat_cancel),
                CommandHandler("resetchat", self._cmd_reset_chat),
            ],
            conversation_timeout=1800,
        )
        app.add_handler(chat_conv)

        # All inline button presses
        app.add_handler(CallbackQueryHandler(self._on_button))
        app.add_error_handler(self._on_application_error)

    async def _on_application_error(self, update: object, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        error = getattr(ctx, "error", None)
        message = str(error or "").lower()
        if isinstance(error, BadRequest) and (
            "message is not modified" in message or "message to edit not found" in message
        ):
            logger.debug(f"[Telegram] benign callback edit error ignored: {error}")
            return
        logger.error(f"[Telegram] application handler error: {error}", exc_info=error)

    # ── send_message (thread-safe, callable from any thread) ─────────────────

    @staticmethod
    def _fmt_price(price: float, asset: str = "") -> str:
        """
        Format price with correct decimal places for the asset type.
        Forex/low-price pairs: 5dp. Crypto major / commodities: 2dp.
        Indices / large values: 2dp with comma separator.
        """
        if price == 0:
            return "0"
        if price >= 1000:
            return f"{price:,.2f}"
        if price >= 10:
            return f"{price:.2f}"
        if price >= 0.1:
            return f"{price:.4f}"
        return f"{price:.5f}"

    @staticmethod
    def _trade_review(trade: Dict[str, Any]) -> Dict[str, Any]:
        meta = trade.get("metadata", trade.get("trade_metadata", {})) or {}
        if not isinstance(meta, dict):
            meta = {}
        review = meta.get("post_trade_review")
        if isinstance(review, dict) and review:
            return review
        try:
            from services.post_trade_review_service import get_service as get_post_trade_review_service

            return get_post_trade_review_service().build_review(
                {
                    **trade,
                    "metadata": meta,
                }
            )
        except Exception:
            return {}

    @staticmethod
    def _review_items(items: Any, limit: int = 2) -> str:
        if not isinstance(items, list):
            return ""
        lines = [str(item).strip() for item in items if str(item).strip()]
        if not lines:
            return ""
        return "\n".join(f"• {line}" for line in lines[: max(1, int(limit or 2))])

    @classmethod
    def _format_trade_review_block(cls, trade: Dict[str, Any]) -> str:
        review = cls._trade_review(trade)
        if not isinstance(review, dict) or not review:
            return ""

        headline = str(review.get("headline") or "").strip()
        outcome = str(review.get("outcome") or "").lower()
        summary = str(review.get("summary") or "").strip()
        lesson = str(review.get("lesson") or "").strip()
        next_focus = str(review.get("next_focus") or "").strip()
        keep = cls._review_items(review.get("keep"))
        avoid = cls._review_items(review.get("avoid"))
        what_right = cls._review_items(review.get("what_went_right"))
        what_wrong = cls._review_items(review.get("what_went_wrong"))

        parts = ["🧠 *Post-Trade Audit*"]
        if headline:
            parts.append(f"*Thesis:* {headline}")
        if summary:
            parts.append(f"*Summary:* {summary}")

        if outcome in {"win", "partial_win"}:
            if what_right:
                parts.append("")
                parts.append("*What held:*")
                parts.append(what_right)
            if lesson:
                parts.append("")
                parts.append(f"*Desk lesson:* {lesson}")
            if keep:
                parts.append("")
                parts.append("*Keep:*")
                parts.append(keep)
        else:
            if what_wrong:
                parts.append("")
                parts.append("*Failure stack:*")
                parts.append(what_wrong)
            if lesson:
                parts.append("")
                parts.append(f"*Desk lesson:* {lesson}")
            if avoid:
                parts.append("")
                parts.append("*Next avoid:*")
                parts.append(avoid)

        if next_focus:
            parts.append("")
            parts.append(f"*Next focus:* {next_focus}")

        return "\n".join(part for part in parts if part is not None)

    @staticmethod
    def _sanitise_markdown(text: str) -> str:
        """
        Fix common Markdown issues that cause Telegram parse errors.
        Approach: balance every special character so Telegram never sees
        an unclosed entity. Handles text from OpenAI, news APIs, price feeds.
        """
        if not text:
            return text

        # 1. Replace smart quotes and dashes that look like markdown
        text = text.replace("‘", "'").replace("’", "'")
        text = text.replace("“", '"').replace("”", '"')
        text = text.replace("—", "--").replace("–", "-")

        # 2. Escape raw URLs that contain underscores (breaks italic parsing)
        import re
        # Temporarily protect intentional *bold* and _italic_ and `code`
        # by checking they are balanced. If not — strip the markers entirely.

        def _balance(s: str, char: str) -> str:
            """If char count is odd, remove all bare occurrences of char."""            # Only strip if unbalanced AND not part of a word boundary pair
            count = s.count(char)
            if count % 2 == 0:
                return s
            # Odd count — strip all standalone markers
            # Keep ones inside words (e.g. snake_case, C++ operators)
            if char == "_":
                # Replace _ that are surrounded by spaces/newlines (markdown italic)
                return re.sub(r'(?<![\w])_(?![\w])|(?<=[\w])_(?=[\s\n])|(?<=[\s\n])_(?=[\w])', '', s)
            if char in ("*", "`"):
                return s.replace(char, "")
            return s

        for marker in ("*", "_", "`"):
            text = _balance(text, marker)

        # 3. Ensure [ always has a matching ] (broken links)
        open_b  = text.count("[")
        close_b = text.count("]")
        if open_b != close_b:
            text = text.replace("[", "").replace("]", "")

        return text

    def _schedule_message_send(self, text: str, parse_mode: str, reply_markup):
        async def _send():
            await self.application.bot.send_message(
                chat_id=self.chat_id,
                text=text,
                parse_mode=parse_mode,
                reply_markup=reply_markup,
            )

        return asyncio.run_coroutine_threadsafe(_send(), self._loop)

    @staticmethod
    def _is_shutdown_send_error(error: Exception) -> bool:
        msg = str(error).lower()
        return "cannot schedule new futures after shutdown" in msg or "event loop is closed" in msg

    @staticmethod
    def _is_parse_send_error(error: Exception) -> bool:
        msg = str(error).lower()
        return "can't parse entities" in msg or "parse entities" in msg

    def _send_plain_message(self, text: str, reply_markup) -> bool:
        try:
            future = self._schedule_message_send(text, None, reply_markup)
            future.result(timeout=15)
            return True
        except Exception as e:
            logger.error(f"[Telegram] send fallback plain text error: {e}")
            return False

    def _handle_send_message_error(
        self,
        error: Exception,
        text: str,
        reply_markup,
        *,
        network_error: bool = False,
    ) -> bool:
        if self._is_shutdown_send_error(error):
            logger.debug(f"[Telegram] send skipped during shutdown: {error}")
            return False
        if self._is_parse_send_error(error):
            logger.warning(f"[Telegram] parse error: {error}. Retrying without markdown")
            return self._send_plain_message(text, reply_markup)
        if network_error:
            logger.warning(f"[Telegram] network error: {error}")
        else:
            msg = str(error).lower()
            if "unauthorized" not in msg and "401" not in msg:
                logger.error(f"[Telegram] send error ({type(error).__name__}): {error}")
        return False

    def send_message(self, text: str, parse_mode: str = ParseMode.MARKDOWN,
                     reply_markup=None) -> bool:
        if not str(text or "").strip():
            logger.debug("[Telegram] skipping empty message")
            return False
        if not self._rate_ok():
            return False
        if not self.application or not self._loop or self._loop.is_closed():
            return False

        # Sanitise before sending to prevent parse entity errors
        if parse_mode == ParseMode.MARKDOWN:
            text = self._sanitise_markdown(text)

        try:
            future = self._schedule_message_send(text, parse_mode, reply_markup)
            future.result(timeout=15)
            return True
        except FutureTimeoutError:
            try:
                future.cancel()
            except Exception:
                pass
            logger.warning("[Telegram] send timed out")
        except RetryAfter as e:
            wait = e.retry_after if isinstance(e.retry_after, (int, float)) else 30
            logger.warning(f"[Telegram] flood control — retry in {wait}s")
        except (TimedOut, NetworkError) as e:
            if self._handle_send_message_error(e, text, reply_markup, network_error=True):
                return True
        except Exception as e:
            if self._handle_send_message_error(e, text, reply_markup):
                return True
        return False

    def _rate_ok(self) -> bool:
        now = time.time()
        with self._rl_lock:
            self._rl_times = [t for t in self._rl_times if now - t < 60]
            if len(self._rl_times) >= 25:
                return False
            self._rl_times.append(now)
            return True

    # ── Trade alert helpers (called from core/engine.py) ─────────────────────

    @classmethod
    def _trade_open_narrative(cls, trade: Dict[str, Any]) -> str:
        asset = str(trade.get("asset", "") or "this asset")
        direction = str(trade.get("direction", trade.get("signal", "BUY")) or "BUY").upper()
        meta = trade.get("metadata", trade.get("trade_metadata", {})) or {}
        if not isinstance(meta, dict):
            meta = {}
        playbook = cls._humanise_diagnostic_label(meta.get("playbook_name") or "").lower()
        entry_style = cls._humanise_diagnostic_label(meta.get("playbook_entry_style") or "").lower()
        session = cls._humanise_diagnostic_label(meta.get("session_label") or meta.get("playbook_session") or "").lower()
        broker = meta.get("broker_quality") if isinstance(meta.get("broker_quality"), dict) else {}
        cross = meta.get("cross_asset_context") if isinstance(meta.get("cross_asset_context"), dict) else {}

        clauses: List[str] = []
        if playbook:
            clauses.append(f"the {playbook} playbook is active")
        if entry_style:
            clauses.append(f"entry style is {entry_style}")
        if session:
            clauses.append(f"timing lines up with {session}")
        quote_quality = cls._humanise_diagnostic_label(broker.get("quote_quality_state") or "").lower()
        spread = cls._humanise_diagnostic_label(broker.get("spread_regime") or "").lower()
        if quote_quality or spread:
            broker_bits = [bit for bit in [quote_quality, spread] if bit]
            clauses.append(f"quote conditions are {' and '.join(broker_bits)}")
        peer = str(cross.get("dominant_peer") or "").strip()
        cross_state = cls._humanise_diagnostic_label(cross.get("state") or "").lower()
        if peer and cross_state:
            clauses.append(f"{peer} is {cross_state}")
        if not clauses:
            clauses.append("the full review stack aligned")
        return f"Why now: {asset} triggered a {direction.lower()} because " + ", ".join(clauses[:3]) + "."

    @classmethod
    def _trade_close_narrative(cls, trade: Dict[str, Any]) -> str:
        review = cls._trade_review(trade)
        if isinstance(review, dict) and review:
            summary = str(review.get("summary") or "").strip()
            headline = str(review.get("headline") or "").strip()
            lesson = str(review.get("lesson") or "").strip()
            if summary and lesson:
                return f"What actually happened: {summary} Main adjustment: {lesson}"
            if summary:
                return f"What actually happened: {summary}"
            if headline and lesson:
                return f"What actually happened: {headline}. Main adjustment: {lesson}"
            if headline:
                return f"What actually happened: {headline}."
        reason = str(trade.get("display_exit_reason", trade.get("exit_reason", "the exit logic closed the trade")) or "").strip()
        return f"What actually happened: the trade was closed because {reason.lower()}."

    def alert_trade_opened(self, trade: Dict) -> None:
        try:
            d     = trade.get("direction", trade.get("signal", "BUY"))
            emoji = "🟢" if d == "BUY" else "🔴"
            entry = float(trade.get("entry_price", 0))
            sl    = float(trade.get("stop_loss",   0))
            target_plan = self._target_snapshot(trade)
            primary_tp = float(target_plan.get("primary_target", 0.0) or 0.0)
            runner_tp = float(target_plan.get("runner_target", 0.0) or 0.0)
            primary_rr = float(target_plan.get("primary_rr", 0.0) or 0.0)
            runner_rr = float(target_plan.get("runner_rr", 0.0) or 0.0)
            _a    = trade.get('asset', '?')
            open_raw = trade.get("open_time") or trade.get("entry_time")
            opened_at = format_display_datetime(open_raw or now_in_display_timezone())
            playbook_block = self._format_playbook_runtime_block(trade)
            playbook_text = f"\n🧭 *Entry Context*\n{playbook_block}" if playbook_block else ""
            diagnostics_block = self._format_runtime_diagnostics_block(trade)
            diagnostics_text = f"\n🧪 *Market Context*\n{diagnostics_block}" if diagnostics_block else ""
            target_lines = [f"Stop:     `{self._fmt_price(sl, _a)}`"]
            if primary_tp > 0:
                target_label = "TP1" if runner_tp > 0 and abs(runner_tp - primary_tp) > 1e-9 else "Target"
                target_lines.append(f"{target_label}:      `{self._fmt_price(primary_tp, _a)}`")
            if runner_tp > 0 and abs(runner_tp - primary_tp) > 1e-9:
                target_lines.append(f"Runner:   `{self._fmt_price(runner_tp, _a)}`")
                rr_line = f"R:R:      `TP1 {primary_rr:.1f}:1 | Runner {runner_rr:.1f}:1`"
            else:
                rr_display = primary_rr if primary_rr > 0 else (abs(primary_tp - entry) / abs(entry - sl) if abs(entry - sl) > 0 and primary_tp > 0 else 0.0)
                rr_line = f"R:R:      `{rr_display:.1f}:1`"
            self.send_message(
                f"{emoji} *TRADE OPENED*\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"📌 Asset:    *{_a}*\n"
                f"📍 Direction: *{d}*\n"
                f"🕐 Opened:   `{opened_at}`\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"{self._trade_open_narrative(trade)}\n"
                f"Entry:    `{self._fmt_price(entry, _a)}`\n"
                f"{chr(10).join(target_lines)}\n"
                f"{rr_line}\n"
                f"Conf:     `{float(trade.get('confidence', 0)):.0%}`\n"
                f"{playbook_text}"
                f"{diagnostics_text}\n"
                f"ID:       `{trade.get('trade_id', '?')}`"
            )
        except Exception as e:
            logger.error(f"[Telegram] alert_trade_opened: {e}")

    def alert_trade_closed(self, trade: Dict) -> None:
        try:
            pnl   = float(trade.get("pnl", 0))
            icon  = "✅" if pnl >= 0 else "❌"
            sign  = "+" if pnl >= 0 else ""
            _a2   = trade.get('asset', '?')
            d     = str(trade.get("direction", trade.get("signal", "BUY")) or "BUY").upper()
            _en   = float(trade.get('entry_price', 0))
            _ex   = float(trade.get('exit_price',  0))
            reason = str(trade.get('display_exit_reason', trade.get('exit_reason', '?')) or '?')
            # Reason emoji
            r_emoji = {"Take Profit":"🎯","Stop Loss":"🛑","Trailing":"📈","Manual":"👆","Break":"⚖️"}
            r_em = next((v for k, v in r_emoji.items() if k in reason), "📌")
            # Times
            now_dt = now_in_display_timezone()
            now_str = format_display_datetime(now_dt)
            open_str = "—"
            close_str = now_str
            dur_str  = "—"
            try:
                open_t = trade.get("open_time") or trade.get("entry_time")
                exit_t = trade.get("exit_time")
                close_dt = None
                if exit_t:
                    close_dt = to_display_datetime(exit_t)
                    close_str = format_display_datetime(close_dt)
                if open_t:
                    ot = to_display_datetime(open_t)
                    if ot is not None:
                        open_str = format_display_datetime(ot)
                        duration_minutes = trade.get("duration_minutes")
                        if duration_minutes is not None:
                            mins = max(0, int(float(duration_minutes)))
                        else:
                            ref_close = close_dt or now_dt
                            mins = max(0, int((ref_close - ot).total_seconds() / 60))
                        if mins < 60:
                            dur_str = f"{mins}m"
                        elif mins < 1440:
                            dur_str = f"{mins//60}h {mins%60}m"
                        else:
                            dur_str = f"{mins//1440}d {(mins%1440)//60}h"
            except Exception:
                pass
            playbook_block = self._format_playbook_runtime_block(trade)
            playbook_text = f"\n🧭 *Entry Context*\n{playbook_block}" if playbook_block else ""
            execution_block = self._format_trade_execution_block(trade)
            execution_text = f"\n🏛️ *Execution Review*\n{execution_block}" if execution_block else ""
            review_block = self._format_trade_review_block(trade)
            review_text = f"\n{review_block}" if review_block else ""
            self.send_message(
                f"{icon} *TRADE CLOSED*\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"📌 Asset:    *{_a2}*\n"
                f"📍 Direction: *{d}*\n"
                f"{r_em} Reason:   *{reason}*\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"🕐 Opened:   `{open_str}`\n"
                f"🕑 Closed:   `{close_str}`\n"
                f"⏱ Duration: `{dur_str}`\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"{self._trade_close_narrative(trade)}\n"
                f"Entry:  `{self._fmt_price(_en, _a2)}`\n"
                f"Exit:   `{self._fmt_price(_ex, _a2)}`\n"
                f"P&L:    `{sign}${pnl:.2f}`"
                f"{playbook_text}"
                f"{execution_text}"
                f"{review_text}"
            )
        except Exception as e:
            logger.error(f"[Telegram] alert_trade_closed: {e}")

    def alert_daily_loss_limit(self, loss_pct: float) -> None:
        self.send_message(
            f"⚠️ *Daily Loss Limit Hit*\n\n"
            f"Loss: {loss_pct:.1f}%\n"
            f"Trading paused automatically.\n"
            f"Use /menu → ▶️ Resume to restart."
        )

    # ══════════════════════════════════════════════════════════════════════════
    # Command handlers (direct text commands, no buttons)
    # ══════════════════════════════════════════════════════════════════════════

    async def _cmd_menu(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        text, kb = self._build_main_menu()
        await update.message.reply_text(
            text,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=kb,
        )

    async def _cmd_status_direct(self, update, ctx):
        text, kb = await self._build_status()
        await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)

    async def _cmd_positions_direct(self, update, ctx):
        text, kb = await self._build_positions()
        await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)

    async def _cmd_balance_direct(self, update, ctx):
        text, kb = await self._build_balance()
        await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)

    async def _cmd_signal_direct(self, update, ctx):
        if ctx.args:
            raw   = " ".join(ctx.args).upper()
            asset = _resolve_alias(raw)
            text, kb = await self._build_signal(asset)
        else:
            text = "🎯 *Pick a category to scan:*"
            kb   = _category_keyboard()
        await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)

    async def _cmd_why_direct(self, update, ctx):
        if not ctx.args:
            await update.message.reply_text(
                "Usage: `/why <asset>`\nExample: `/why BTC`",
                parse_mode=ParseMode.MARKDOWN,
            )
            return
        raw = " ".join(ctx.args).upper()
        asset = _resolve_alias(raw)
        await update.message.reply_text(
            f"🧠 Robbie is explaining {asset}…",
            parse_mode=ParseMode.MARKDOWN,
        )
        text = await self._build_why(asset)
        for chunk in [text[i:i+4000] for i in range(0, len(text), 4000)]:
            await update.message.reply_text(
                chunk,
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=_kb(
                    [(f"🎯 Signal", f"sig:{asset}"), ("🏠 Menu", "menu")],
                ),
            )

    async def _cmd_close_direct(self, update, ctx):
        if not ctx.args:
            await update.message.reply_text(
                "Usage: `/close <trade_id>`\nGet IDs from /positions",
                parse_mode=ParseMode.MARKDOWN,
            )
            return
        trade_id = ctx.args[0]
        text, kb = self._do_close(trade_id)
        await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)

    async def _cmd_pause_direct(self, update, ctx):
        text = self._do_pause()
        await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)

    async def _cmd_resume_direct(self, update, ctx):
        text = self._do_resume()
        await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)

    async def _cmd_reprice_direct(self, update, ctx):
        text, kb = self._do_reprice_weak()
        await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)

    async def _cmd_reduce_weak_direct(self, update, ctx):
        text, kb = self._do_reduce_weak()
        await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)

    async def _cmd_top_setups_direct(self, update, ctx):
        await update.message.reply_text("Scanning current opportunities…", parse_mode=ParseMode.MARKDOWN)
        text, kb = self._build_top_setups(refresh=True)
        await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)

    # ══════════════════════════════════════════════════════════════════════════
    # /ask conversation
    # ══════════════════════════════════════════════════════════════════════════

    async def _ask_entry(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if ctx.args and len(ctx.args) >= 2:
            # /ask BTC should I buy? — all-in-one, skip conversation
            raw      = ctx.args[0].upper()
            asset    = _resolve_alias(raw)
            question = " ".join(ctx.args[1:])
            await update.message.chat.send_action("typing")
            text = await self._run_ask(asset, question)
            await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)
            return ConversationHandler.END

        await update.message.reply_text(
            "🧠 *Ask Robbie*\n\n"
            "Ask about one asset at a time.\n"
            "Good prompts: `should I trade?`, `explain`, `risk`, `sentiment`, `confidence`, `what do you remember?`\n\n"
            "Which asset do you want to ask about?\n"
            "_Type the ticker or name, e.g. BTC, EURUSD, GOLD_",
            parse_mode=ParseMode.MARKDOWN,
        )
        return WAITING_ASK_ASSET

    async def _ask_got_asset(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        raw   = update.message.text.strip().upper()
        asset = _resolve_alias(raw)
        ctx.user_data["ask_asset"] = asset
        await update.message.reply_text(
            f"Got it — *{asset}*.\n\nWhat do you want to know?\n"
            f"_(e.g. 'should I buy?', 'explain the risk', 'what do you remember?')_",
            parse_mode=ParseMode.MARKDOWN,
        )
        return WAITING_ASK_QUESTION

    async def _ask_got_question(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        asset    = ctx.user_data.get("ask_asset", "BTC-USD")
        question = update.message.text.strip()
        await update.message.chat.send_action("typing")
        text = await self._run_ask(asset, question)
        # Split if needed
        for chunk in [text[i:i+4000] for i in range(0, len(text), 4000)]:
            await update.message.reply_text(
                chunk, parse_mode=ParseMode.MARKDOWN,
                reply_markup=_kb(
                    [("🔄 Ask another", "ask"), ("🏠 Menu", "menu")],
                )
            )
        return ConversationHandler.END

    async def _ask_cancel(self, update, ctx):
        await update.message.reply_text("Cancelled.")
        return ConversationHandler.END

    async def _run_ask(self, asset: str, question: str) -> str:
        def _compute() -> str:
            analysis, sig, df = self._inspect_asset_snapshot(asset)
            from services.personality_service import RobbieExplainer

            explainer = RobbieExplainer()
            try:
                return explainer.answer(asset, question, signal=sig, df=df, analysis=analysis)
            finally:
                explainer.close()

        try:
            return await asyncio.wait_for(
                asyncio.to_thread(_compute),
                timeout=_CHAT_HANDLER_TIMEOUT_SECONDS,
            )
        except asyncio.TimeoutError:
            logger.warning(f"[Telegram] /ask timed out after {_CHAT_HANDLER_TIMEOUT_SECONDS:.0f}s")
            return "⏱️ Robbie took too long to answer that asset question. Try again with a shorter question."
        except Exception as e:
            logger.error(f"[Telegram] /ask error: {e}")
            return f"❌ Robbie hit an error: {e}"

    @staticmethod
    def _chat_intro_text() -> str:
        return (
            "🧠 *Robbie Chat*\n\n"
            "Talk to me normally here. You can ask things like:\n"
            "• `why did you choose that trade?`\n"
            "• `what happened for it to hit stop loss?`\n"
            "• `what have you learned recently?`\n"
            "• `what is currently happening that affects trading?`\n"
            "• `how should you adjust yourself right now?`\n"
            "• `what issues are you experiencing?`\n\n"
            "Send a message to start. Use `/resetchat` to clear context or `/cancel` to exit chat mode."
        )

    async def _chat_entry(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if ctx.args:
            question = " ".join(ctx.args).strip()
            await update.message.chat.send_action("typing")
            placeholder = await update.message.reply_text("🧠 *Robbie is thinking...*", parse_mode=ParseMode.MARKDOWN)
            text = await self._run_chat(question, chat_id=str(update.effective_chat.id))
            await self._replace_placeholder_with_chunks(
                placeholder,
                text,
                reply_markup=_kb([("🗑 Reset chat", "chat_reset"), ("🏠 Menu", "menu")]),
            )
            return WAITING_CHAT_MESSAGE

        await update.message.reply_text(
            self._chat_intro_text(),
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=_chat_shortcuts_keyboard(),
        )
        return WAITING_CHAT_MESSAGE

    async def _chat_entry_from_button(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        await self._edit_query_text_or_reply(
            query,
            self._chat_intro_text(),
            reply_markup=_chat_shortcuts_keyboard(),
        )
        return WAITING_CHAT_MESSAGE

    async def _chat_got_message(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        question = update.message.text.strip()
        await update.message.chat.send_action("typing")
        placeholder = await update.message.reply_text("🧠 *Robbie is thinking...*", parse_mode=ParseMode.MARKDOWN)
        text = await self._run_chat(question, chat_id=str(update.effective_chat.id))
        await self._replace_placeholder_with_chunks(
            placeholder,
            text,
            reply_markup=_kb([("🗑 Reset chat", "chat_reset"), ("🏠 Menu", "menu")]),
        )
        return WAITING_CHAT_MESSAGE

    async def _chat_cancel(self, update, ctx):
        await update.message.reply_text("Chat mode closed. Use `/chat` when you want to continue.", parse_mode=ParseMode.MARKDOWN)
        return ConversationHandler.END

    async def _cmd_reset_chat(self, update, ctx):
        try:
            from services.robbie_chat_service import get_chat_service

            get_chat_service().reset(str(update.effective_chat.id))
            await update.message.reply_text(
                "Robbie chat memory cleared for this chat. Start again with `/chat`.",
                parse_mode=ParseMode.MARKDOWN,
            )
        except Exception as e:
            logger.error(f"[Telegram] /resetchat error: {e}")
            await update.message.reply_text(f"❌ Could not reset chat memory: {e}")
        return ConversationHandler.END

    async def _run_chat(self, question: str, chat_id: str) -> str:
        try:
            from services.robbie_chat_service import get_chat_service

            answer = await asyncio.wait_for(
                asyncio.to_thread(
                    get_chat_service().answer,
                    question=question,
                    trading_system=self.trading_system,
                    chat_id=chat_id,
                ),
                timeout=_CHAT_HANDLER_TIMEOUT_SECONDS,
            )
            return answer
        except asyncio.TimeoutError:
            logger.warning(f"[Telegram] /chat timed out after {_CHAT_HANDLER_TIMEOUT_SECONDS:.0f}s")
            return "⏱️ Robbie took too long to answer. Try again with a shorter question or use `/resetchat` if the thread feels stuck."
        except Exception as e:
            logger.error(f"[Telegram] /chat error: {e}", exc_info=True)
            return f"❌ Robbie hit a chat error: {e}"

    async def _reply_in_chunks(self, send_fn, text: str, reply_markup=None) -> None:
        answer = self._sanitise_markdown(text)
        chunks = [answer[i:i+4000] for i in range(0, max(len(answer), 1), 4000)]
        for i, chunk in enumerate(chunks):
            await send_fn(
                chunk,
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=reply_markup if i == len(chunks) - 1 else None,
            )

    async def _replace_placeholder_with_chunks(self, message, text: str, reply_markup=None) -> None:
        answer = self._sanitise_markdown(text)
        chunks = [answer[i:i+4000] for i in range(0, max(len(answer), 1), 4000)]
        for i, chunk in enumerate(chunks):
            kb = reply_markup if i == len(chunks) - 1 else None
            if i == 0:
                await self._edit_message_text_or_reply(message, chunk, reply_markup=kb)
            else:
                await message.reply_text(
                    chunk,
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=kb,
                )

    async def _edit_message_text_or_reply(self, message, text: str, *, parse_mode=ParseMode.MARKDOWN, reply_markup=None, **kwargs) -> bool:
        try:
            await message.edit_text(
                text,
                parse_mode=parse_mode,
                reply_markup=reply_markup,
                **kwargs,
            )
            return True
        except BadRequest as exc:
            lowered = str(exc).lower()
            if "message is not modified" in lowered:
                return False
            if "message to edit not found" not in lowered:
                raise
            await message.reply_text(
                text,
                parse_mode=parse_mode,
                reply_markup=reply_markup,
                **kwargs,
            )
            return True

    async def _edit_query_text_or_reply(self, query, text: str, *, parse_mode=ParseMode.MARKDOWN, reply_markup=None, **kwargs) -> bool:
        try:
            await query.edit_message_text(
                text,
                parse_mode=parse_mode,
                reply_markup=reply_markup,
                **kwargs,
            )
            return True
        except BadRequest as exc:
            lowered = str(exc).lower()
            if "message is not modified" in lowered:
                return False
            if "message to edit not found" not in lowered:
                raise
            await query.message.reply_text(
                text,
                parse_mode=parse_mode,
                reply_markup=reply_markup,
                **kwargs,
            )
            return True

    # ══════════════════════════════════════════════════════════════════════════
    # Inline button router
    # ══════════════════════════════════════════════════════════════════════════

    def _resolve_button_action(self, data: str):
        exact_actions = {
            "menu": (self._btn_menu, ()),
            "status": (self._btn_status, ()),
            "positions": (self._btn_positions, ()),
            "balance": (self._btn_balance, ()),
            "signals": (self._btn_signals, ()),
            "ask_asset_menu": (self._btn_ask_asset_menu, ()),
            "chat_reset": (self._btn_chat_reset, ()),
            "mood": (self._btn_mood, ()),
            "diary": (self._btn_diary, ()),
            "market": (self._btn_market, ()),
            "pause": (self._btn_pause, ()),
            "resume": (self._btn_resume, ()),
            "reprice_weak": (self._btn_reprice_weak, ()),
            "reduce_weak": (self._btn_reduce_weak, ()),
            "top_setups": (self._btn_top_setups, ()),
            "close_menu": (self._btn_close_menu, ()),
            "history": (self._btn_history, ()),
            "close_losing": (self._btn_close_filter, ("losing",)),
            "close_winning": (self._btn_close_filter, ("winning",)),
            "close_all_confirm": (self._btn_close_all_confirm, ()),
            "close_all_execute": (self._btn_close_all_execute, ()),
        }
        if data in exact_actions:
            return exact_actions[data]

        prefix_actions = (
            ("cat:", self._btn_category, 4),
            ("sig:", self._btn_signal, 4),
            ("why:", self._btn_why, 4),
            ("close:", self._btn_close_confirm, 6),
            ("close_ok:", self._btn_close_execute, 9),
            ("askcat:", self._btn_ask_category, 7),
            ("askasset:", self._btn_ask_asset, 9),
            ("askq:", self._btn_ask_question, 5),
            ("chatq:", self._btn_chat_prompt, 6),
            ("history_filter:", self._btn_history, 15),
            ("close_cat:", self._btn_close_category, 10),
        )
        for prefix, handler, offset in prefix_actions:
            if data.startswith(prefix):
                return handler, (data[offset:],)

        return None, ()

    async def _on_button(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        safe_query = _SafeCallbackQuery(query, self._edit_query_text_or_reply)
        await safe_query.answer()               # acknowledge tap immediately
        data = safe_query.data or ""
        handler, args = self._resolve_button_action(data)
        if handler is None:
            await safe_query.edit_message_text("⚠️ Unknown action.")
            return
        await handler(safe_query, *args)

    # ── Button implementations ────────────────────────────────────────────────

    async def _btn_menu(self, query) -> None:
        text, kb = self._build_main_menu()
        await query.edit_message_text(
            text,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=kb,
        )

    async def _btn_status(self, query) -> None:
        text, kb = await self._build_status()
        await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)

    async def _btn_positions(self, query) -> None:
        text, kb = await self._build_positions()
        await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)

    async def _btn_balance(self, query) -> None:
        text, kb = await self._build_balance()
        await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)

    async def _btn_signals(self, query) -> None:
        await query.edit_message_text(
            "🎯 *Signals*\n\nPick a category to scan:",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=_category_keyboard(),
        )

    async def _btn_category(self, query, category: str) -> None:
        label = category.capitalize()
        await query.edit_message_text(
            f"🎯 *{label} Signals*\n\nPick an asset:",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=_asset_keyboard(category),
        )

    async def _btn_signal(self, query, asset: str) -> None:
        await query.edit_message_text(
            f"⏳ Reviewing {asset} through the decision engine…",
            parse_mode=ParseMode.MARKDOWN,
        )
        text, kb = await self._build_signal(asset)
        await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)

    async def _btn_why(self, query, asset: str) -> None:
        await query.edit_message_text(
            f"🧠 Robbie is explaining {asset}…",
            parse_mode=ParseMode.MARKDOWN,
        )
        text = await self._build_why(asset)
        kb   = _kb(
            [(f"🎯 Signal", f"sig:{asset}"), ("◀️ Back", "signals")],
            [("🏠 Menu", "menu")],
        )
        for chunk in [text[i:i+4000] for i in range(0, len(text), 4000)]:
            await query.edit_message_text(
                chunk, parse_mode=ParseMode.MARKDOWN, reply_markup=kb,
            )

    async def _btn_close_confirm(self, query, trade_id: str) -> None:
        core      = self.trading_system
        positions = core.get_positions() if core else []
        pos       = next((p for p in positions if p.get("trade_id") == trade_id), None)
        if not pos:
            await query.edit_message_text(
                f"❌ Trade `{trade_id}` not found.",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=_kb([("◀️ Back", "positions"), ("🏠 Menu", "menu")]),
            )
            return
        asset = pos.get("asset", trade_id)
        d     = (pos.get("direction") or pos.get("signal", "BUY")).upper()
        entry = float(pos.get("entry_price", 0))
        await query.edit_message_text(
            f"⚠️ *Confirm Close*\n\n"
            f"Asset:  {asset}\n"
            f"Side:   {d}\n"
            f"Entry:  `{self._fmt_price(entry, asset)}`\n"
            f"ID:     `{trade_id}`\n\n"
            f"Are you sure?",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=_kb(
                [(f"✅ Yes, close it", f"close_ok:{trade_id}")],
                [("❌ Cancel",          "positions")],
            ),
        )

    async def _btn_close_execute(self, query, trade_id: str) -> None:
        text, kb = self._do_close(trade_id)
        await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)

    async def _btn_ask_asset_menu(self, query) -> None:
        """Open the asset-specific Ask Robbie flow."""
        await query.edit_message_text(
            "🧠 *Ask Robbie By Asset*\n\n"
            "Pick a market category, then choose an asset.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=_ask_category_keyboard(),
        )

    async def _btn_ask_category(self, query, emoji_cat: str) -> None:
        """Show asset picker for the chosen category."""
        await query.edit_message_text(
            f"🧠 *Ask Robbie — {emoji_cat}*\n\n"
            "Pick an asset:",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=_ask_asset_keyboard(emoji_cat),
        )

    async def _btn_ask_asset(self, query, asset: str) -> None:
        """Show question picker for the chosen asset."""
        display = _DISPLAY.get(asset, asset)
        await query.edit_message_text(
            f"🧠 *Ask Robbie — {display}*\n\n"
            "What do you want to know?",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=_ask_question_keyboard(asset),
        )

    async def _btn_ask_question(self, query, payload: str) -> None:
        """Run the question — payload is 'asset:qkey'."""
        try:
            asset, qkey = payload.split(":", 1)
        except ValueError:
            await query.edit_message_text("❌ Invalid question.")
            return

        display = _DISPLAY.get(asset, asset)

        _question_map = {
            "trade":      f"Should I trade {display} right now?",
            "explain":    f"Explain the current signal for {display}.",
            "risk":       f"What is the risk on {display} right now?",
            "remember":   f"What do you remember about {display}?",
            "sentiment":  f"What is the sentiment for {display}?",
            "confidence": f"How confident are you about {display}?",
        }
        question = _question_map.get(qkey, f"Tell me about {display}.")

        await query.edit_message_text(
            f"🧠 *Robbie is thinking about {display}...*\n"            f"_{question}_",
            parse_mode=ParseMode.MARKDOWN,
        )

        try:
            answer = await self._run_ask(asset, question)
        except Exception as e:
            answer = f"❌ Error: {e}"

        # Sanitise OpenAI output before sending — prevents parse entity errors
        answer = self._sanitise_markdown(answer)

        # Split long answers into chunks
        chunks = [answer[i:i+4000] for i in range(0, max(len(answer), 1), 4000)]
        for i, chunk in enumerate(chunks):
            kb = _kb(
                [("🔄 Ask again", f"askasset:{asset}"), ("🏠 Menu", "menu")],
            ) if i == len(chunks) - 1 else None
            if i == 0:
                await query.edit_message_text(
                    chunk,
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=kb,
                )
            else:
                await query.message.reply_text(
                    chunk,
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=kb,
                )

    async def _btn_chat_prompt(self, query, prompt_key: str) -> None:
        prompt = _CHAT_QUICK_PROMPTS.get(prompt_key, "What is happening right now?")
        chat_id = str(query.message.chat_id)
        await self._edit_query_text_or_reply(
            query,
            f"🧠 *Robbie is thinking...*\n_{prompt}_",
        )
        answer = await self._run_chat(prompt, chat_id=chat_id)
        answer = self._sanitise_markdown(answer)
        chunks = [answer[i:i+4000] for i in range(0, max(len(answer), 1), 4000)]
        for i, chunk in enumerate(chunks):
            kb = _kb([("🗑 Reset chat", "chat_reset"), ("🏠 Menu", "menu")]) if i == len(chunks) - 1 else None
            if i == 0:
                await self._edit_query_text_or_reply(query, chunk, reply_markup=kb)
            else:
                await query.message.reply_text(chunk, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)

    async def _btn_chat_reset(self, query) -> None:
        try:
            from services.robbie_chat_service import get_chat_service

            get_chat_service().reset(str(query.message.chat_id))
            await self._edit_query_text_or_reply(
                query,
                "Robbie chat memory cleared for this chat.\n\nSend a new message to start fresh.",
                reply_markup=_kb([("🧠 Chat shortcuts", "chat_menu"), ("🏠 Menu", "menu")]),
            )
        except Exception as e:
            await self._edit_query_text_or_reply(query, f"❌ Could not reset chat memory: {e}")

    async def _btn_mood(self, query) -> None:
        text = self._build_mood()
        await query.edit_message_text(
            text, parse_mode=ParseMode.MARKDOWN,
            reply_markup=_kb(
                [("📔 Diary", "diary"), ("🏠 Menu", "menu")],
            ),
        )

    async def _btn_diary(self, query) -> None:
        text = self._build_diary()
        await query.edit_message_text(
            text, parse_mode=ParseMode.MARKDOWN,
            reply_markup=_kb(
                [("😶 Mood", "mood"), ("🏠 Menu", "menu")],
            ),
        )

    async def _btn_market(self, query) -> None:
        text = _build_market_text()
        await query.edit_message_text(
            text, parse_mode=ParseMode.MARKDOWN,
            reply_markup=_kb([("🔄 Refresh", "market"), ("🏠 Menu", "menu")]),
        )

    async def _btn_pause(self, query) -> None:
        text = self._do_pause()
        await query.edit_message_text(
            text, parse_mode=ParseMode.MARKDOWN,
            reply_markup=_kb([("▶️ Resume", "resume"), ("🏠 Menu", "menu")]),
        )

    async def _btn_resume(self, query) -> None:
        text = self._do_resume()
        await query.edit_message_text(
            text, parse_mode=ParseMode.MARKDOWN,
            reply_markup=_kb([("⏸ Pause", "pause"), ("🏠 Menu", "menu")]),
        )

    # ══════════════════════════════════════════════════════════════════════════
    # Content builders — return (text, keyboard) tuples
    # ══════════════════════════════════════════════════════════════════════════

    async def _build_status(self):
        core = self.trading_system
        if not core:
            return "⏳ Engine not ready.", _kb([("🔄 Refresh", "status"), ("🏠 Menu", "menu")])

        health  = core.health_report()
        perf    = core.get_performance()
        daily   = core.get_daily_stats()
        status  = "🟢 Running" if core.is_running else "🔴 Stopped"
        ready   = "✅" if core.is_ready else "⏳"

        open_pos  = health.get("open_positions", 0)
        daily_pnl = daily.get("daily_pnl", 0)
        pnl_icon  = "📈" if daily_pnl >= 0 else "📉"
        routing_text = self._format_provider_routing_status()
        ig_text = self._format_ig_broker_status(health.get("ig_broker") or {})
        diagnostics_text = self._format_signal_diagnostics_status(health.get("signal_diagnostics") or {})

        text = (
            f"📊 *System Status*\n"
            f"{'─' * 24}\n"
            f"Status:    {status}\n"
            f"Engine:    {ready} {'Ready' if core.is_ready else 'Initialising'}\n"
            f"Mode:      {core.strategy_mode.upper()}\n\n"
            f"💰 *Financials*\n"
            f"Balance:   `${core.get_balance():.2f}`\n"
            f"Daily P&L: {pnl_icon} `${daily_pnl:+.2f}`\n"
            f"Total P&L: `${perf.get('total_pnl', 0):+.2f}`\n\n"
            f"📈 *Trading*\n"
            f"Open:      {open_pos} position{'s' if open_pos != 1 else ''}\n"
            f"Today:     {daily.get('daily_trades', 0)} trades\n"
            f"Win Rate:  {perf.get('win_rate', 0):.1f}%\n"
            f"Total:     {perf.get('total_trades', 0)} trades\n\n"
            f"🖥 *System*\n"
            f"RAM:       {health.get('ram_pct', 0):.0f}%\n"
            f"CPU:       {health.get('cpu_pct', 0):.0f}%\n"
            f"Cooldowns: {health.get('active_cooldowns', 0)}\n"
            f"{routing_text}"
            f"{ig_text}"
            f"{diagnostics_text}"
            f"_Updated: {now_in_display_timezone().strftime('%H:%M:%S')} {display_timezone_label()}_"
        )
        kb = _kb(
            [("🔄 Refresh", "status"),   ("📈 Positions", "positions")],
            [("💰 Balance",  "balance"),  ("🏠 Menu",      "menu")],
        )
        return text, kb

    @staticmethod
    def _format_ig_broker_status(ig_broker: Dict[str, Any]) -> str:
        if not isinstance(ig_broker, dict) or not ig_broker.get("enabled"):
            return ""
        if not ig_broker.get("authenticated", False):
            error_message = str(ig_broker.get("error_message") or ig_broker.get("error_code") or "unavailable")
            return (
                f"\n"
                f"🏦 *IG Broker Data*\n"
                f"IG data is not ready: {error_message}\n"
            )

        environment = str(ig_broker.get("environment") or "").upper()
        account_type = str(ig_broker.get("account_type") or "")
        account_id = str(ig_broker.get("account_id") or "")
        balance = ig_broker.get("balance")
        available = ig_broker.get("available")
        watchlist_count = int(ig_broker.get("watchlist_count", 0) or 0)
        recent_activity_count = int(ig_broker.get("recent_activity_count", 0) or 0)
        balance_text = f"${float(balance):,.2f}" if balance is not None else "—"
        available_text = f"${float(available):,.2f}" if available is not None else "—"

        return (
            f"\n"
            f"🏦 *IG Broker Data*\n"
            f"Account:   {environment} {account_type} {account_id}\n"
            f"IG Bal:    {balance_text}\n"
            f"Available: {available_text}\n"
            f"Lists/Act: {watchlist_count} watchlists, {recent_activity_count} recent activities\n"
        )

    @staticmethod
    def _format_signal_diagnostics_status(signal_diagnostics: Dict[str, Any]) -> str:
        if not isinstance(signal_diagnostics, dict) or int(signal_diagnostics.get("count", 0) or 0) <= 0:
            return ""

        return (
            f"\n"
            f"🧪 *Signal Diagnostics*\n"
            f"Broker:    {int(signal_diagnostics.get('broker_supportive_count', 0) or 0)} supportive"
            f" / {int(signal_diagnostics.get('broker_fragile_count', 0) or 0)} fragile\n"
            f"Depth:     {int(signal_diagnostics.get('true_depth_count', 0) or 0)} true"
            f" / {int(signal_diagnostics.get('synthetic_depth_count', 0) or 0)} synthetic\n"
            f"Cross-Mkt: {int(signal_diagnostics.get('cross_support_count', 0) or 0)} supportive"
            f" / {int(signal_diagnostics.get('cross_conflict_count', 0) or 0)} conflicted\n"
            f"Blocks:    {int(signal_diagnostics.get('recent_pattern_block_count', 0) or 0)} recent-pattern block(s)\n"
        )

    @staticmethod
    def _format_provider_routing_status() -> str:
        try:
            from config.config import IG_ROUTED_CATEGORIES
        except Exception:
            return ""

        routed_categories = {str(cat or "").strip().lower() for cat in (IG_ROUTED_CATEGORIES or [])}
        total_assets = list(registry.all_assets())
        if not total_assets:
            return ""

        ig_count = sum(1 for _, category in total_assets if str(category or "").strip().lower() in routed_categories)
        deriv_count = max(0, len(total_assets) - ig_count)
        if ig_count <= 0:
            return ""

        return (
            f"\n"
            f"🛰 *Provider Routing*\n"
            f"Deriv:    {deriv_count} assets\n"
            f"IG:       {ig_count} assets\n"
        )

    @staticmethod
    def _humanise_diagnostic_label(value: Any, drop_prefix: str = "") -> str:
        text = str(value or "").strip()
        if not text:
            return ""
        if drop_prefix and text.lower().startswith(drop_prefix.lower()):
            text = text[len(drop_prefix):]
        text = text.replace("_", " ").strip()
        replacements = {
            "broad equity confirmation": "broad index confirmation",
            "global equity confirmation": "global index confirmation",
            "risk off equities": "risk-off indices",
            "us equity": "us index",
            "uk equity": "uk index",
            "stock indices": "indices",
            "basket indices": "indices",
        }
        return replacements.get(text.lower(), text)

    @classmethod
    def _playbook_snapshot(cls, payload: Dict[str, Any]) -> Dict[str, str]:
        meta = payload.get("metadata", payload.get("trade_metadata", {})) or {}
        if not isinstance(meta, dict):
            meta = {}
        management = meta.get("trade_management_plan") if isinstance(meta.get("trade_management_plan"), dict) else {}

        raw_playbook = str(
            payload.get("playbook_name")
            or meta.get("playbook_name")
            or payload.get("playbook")
            or meta.get("playbook")
            or ""
        ).strip()
        if not raw_playbook:
            legacy_runtime_id = str(payload.get("strategy_id") or meta.get("strategy_id") or "").strip()
            if legacy_runtime_id.startswith("playbook_"):
                raw_playbook = legacy_runtime_id[len("playbook_"):]
            elif legacy_runtime_id == "playbook_runtime":
                raw_playbook = "playbook runtime"

        entry_style = cls._humanise_diagnostic_label(
            payload.get("playbook_entry_style")
            or meta.get("playbook_entry_style")
            or payload.get("entry_style")
            or meta.get("entry_style")
            or management.get("entry_style")
            or ""
        )
        session_label = cls._humanise_diagnostic_label(
            payload.get("session_label")
            or meta.get("session_label")
            or payload.get("playbook_session")
            or meta.get("playbook_session")
            or payload.get("session")
            or meta.get("session")
            or management.get("session")
            or ""
        )
        timeframe = str(
            payload.get("playbook_timeframe")
            or meta.get("playbook_timeframe")
            or payload.get("preferred_interval")
            or meta.get("preferred_interval")
            or management.get("preferred_interval")
            or ""
        ).strip().lower()

        partials = management.get("partial_take_profit_rr") if isinstance(management.get("partial_take_profit_rr"), list) else []
        partial_label = ""
        if partials:
            try:
                partial_label = f"TP1 {float(partials[0]):.1f}R"
            except Exception:
                partial_label = ""
        runner_target = ""
        try:
            runner = float(management.get("runner_target_rr", 0.0) or 0.0)
            if runner > 0:
                runner_target = f"Runner {runner:.1f}R"
        except Exception:
            runner_target = ""
        trail_label = ""
        try:
            activation = float(management.get("trail_activation_rr", 0.0) or 0.0)
            atr_multiple = float(management.get("trail_atr_multiple", 0.0) or 0.0)
            if activation > 0 and atr_multiple > 0:
                trail_label = f"Trail {activation:.1f}R · ATRx{atr_multiple:.2f}"
        except Exception:
            trail_label = ""

        management_bits = [bit for bit in (partial_label, runner_target, trail_label) if bit]
        return {
            "playbook": cls._humanise_diagnostic_label(raw_playbook),
            "entry_style": entry_style,
            "session_label": session_label,
            "timeframe": timeframe,
            "management_summary": " | ".join(management_bits),
        }

    @staticmethod
    def _target_snapshot(payload: Dict[str, Any]) -> Dict[str, float]:
        entry = 0.0
        stop = 0.0
        try:
            entry = float(payload.get("entry_price", 0) or 0)
            stop = float(payload.get("stop_loss", 0) or 0)
        except Exception:
            entry = 0.0
            stop = 0.0
        levels: List[float] = []
        for raw_level in list(payload.get("take_profit_levels", []) or []):
            try:
                level = float(raw_level)
            except Exception:
                continue
            if level > 0:
                levels.append(level)
        fallback_target = 0.0
        try:
            fallback_target = float(payload.get("take_profit", 0) or 0)
        except Exception:
            fallback_target = 0.0
        tp_hit = 0
        try:
            tp_hit = max(0, int(payload.get("tp_hit", 0) or 0))
        except Exception:
            tp_hit = 0
        primary_target = levels[min(tp_hit, len(levels) - 1)] if levels else fallback_target
        runner_target = levels[-1] if len(levels) > 1 and tp_hit < len(levels) - 1 else 0.0
        risk = abs(entry - stop)
        primary_rr = abs(primary_target - entry) / risk if risk > 0 and primary_target > 0 else 0.0
        runner_rr = abs(runner_target - entry) / risk if risk > 0 and runner_target > 0 else 0.0
        return {
            "primary_target": round(primary_target, 6) if primary_target > 0 else 0.0,
            "runner_target": round(runner_target, 6) if runner_target > 0 else 0.0,
            "primary_rr": round(primary_rr, 4),
            "runner_rr": round(runner_rr, 4),
        }

    @classmethod
    def _format_playbook_runtime_block(cls, payload: Dict[str, Any], prefix: str = "") -> str:
        snap = cls._playbook_snapshot(payload)
        lines: List[str] = []
        if snap["playbook"]:
            label = f"{prefix}Playbook `{snap['playbook']}`"
            if snap["entry_style"]:
                label += f" | `{snap['entry_style']}`"
            lines.append(label)
        session_bits = []
        if snap["session_label"]:
            session_bits.append(f"Session `{snap['session_label']}`")
        if snap["timeframe"]:
            session_bits.append(f"TF `{snap['timeframe']}`")
        if session_bits:
            lines.append(f"{prefix}{' | '.join(session_bits)}")
        if snap["management_summary"]:
            lines.append(f"{prefix}Manage `{snap['management_summary']}`")
        return "\n".join(lines)

    @classmethod
    def _diagnostic_snapshot(cls, payload: Dict[str, Any]) -> Dict[str, Any]:
        meta = payload.get("metadata", payload.get("trade_metadata", {})) or {}
        if not isinstance(meta, dict):
            meta = {}
        broker = meta.get("broker_quality") if isinstance(meta.get("broker_quality"), dict) else {}
        micro = meta.get("market_microstructure") if isinstance(meta.get("market_microstructure"), dict) else {}
        cross = meta.get("cross_asset_context") if isinstance(meta.get("cross_asset_context"), dict) else {}
        adaptive = meta.get("adaptive_policy") if isinstance(meta.get("adaptive_policy"), dict) else {}
        recent_review = adaptive.get("recent_review_profile") if isinstance(adaptive.get("recent_review_profile"), dict) else {}

        notes = payload.get("recent_pattern_notes")
        if not isinstance(notes, list):
            notes = meta.get("recent_pattern_notes")
        if not isinstance(notes, list):
            notes = recent_review.get("notes") if isinstance(recent_review.get("notes"), list) else []

        clean_notes: List[str] = []
        for note in list(notes or [])[:2]:
            clean = cls._humanise_diagnostic_label(note, drop_prefix="recent_pattern_")
            if clean:
                clean_notes.append(clean)

        return {
            "broker_score": float(
                payload.get("broker_quality_score", meta.get("broker_quality_score", broker.get("score", 0.0))) or 0.0
            ),
            "provider": str(
                payload.get("broker_primary_provider", meta.get("broker_primary_provider", broker.get("primary_provider", "")))
                or ""
            ),
            "agreement": cls._humanise_diagnostic_label(
                payload.get("broker_agreement_state", meta.get("broker_agreement_state", broker.get("quote_agreement_state", "")))
            ),
            "quote_quality": cls._humanise_diagnostic_label(
                payload.get(
                    "broker_quote_quality_state",
                    meta.get("broker_quote_quality_state", broker.get("quote_quality_state", "")),
                )
            ),
            "spread_regime": cls._humanise_diagnostic_label(
                payload.get("broker_spread_regime", meta.get("broker_spread_regime", broker.get("spread_regime", "")))
            ),
            "micro_score": float(
                payload.get("microstructure_score", meta.get("microstructure_score", micro.get("score", 0.0))) or 0.0
            ),
            "depth_available": bool(payload.get("depth_available", meta.get("depth_available", micro.get("depth_available")))),
            "synthetic_depth_available": bool(
                payload.get(
                    "synthetic_depth_available",
                    meta.get("synthetic_depth_available", micro.get("synthetic_depth_available")),
                )
            ),
            "cross_state": cls._humanise_diagnostic_label(
                payload.get("cross_asset_state", meta.get("cross_asset_state", cross.get("state", "")))
            ),
            "cross_peer": str(
                payload.get("cross_asset_primary_peer", meta.get("cross_asset_primary_peer", cross.get("dominant_peer", "")))
                or ""
            ),
            "recent_pattern_notes": clean_notes,
            "recent_pattern_block": bool(
                payload.get(
                    "recent_pattern_block_new_entries",
                    meta.get("recent_pattern_block_new_entries", recent_review.get("block_new_entries")),
                )
            ),
        }

    @classmethod
    def _format_runtime_diagnostics_block(cls, payload: Dict[str, Any], prefix: str = "") -> str:
        snapshot = cls._diagnostic_snapshot(payload)
        lines: List[str] = []

        broker_parts = [part for part in [snapshot["agreement"], snapshot["quote_quality"], snapshot["spread_regime"]] if part]
        if snapshot["broker_score"] > 0.0 or snapshot["provider"] or broker_parts:
            broker_line = f"{prefix}Broker `{snapshot['broker_score']:.2f}`"
            if snapshot["provider"]:
                broker_line += f" | `{snapshot['provider']}`"
            if broker_parts:
                broker_line += f" | {' / '.join(broker_parts)}"
            lines.append(broker_line)

        depth_mode = "Top-book"
        if snapshot["depth_available"]:
            depth_mode = "True depth"
        elif snapshot["synthetic_depth_available"]:
            depth_mode = "Synthetic depth"
        if snapshot["micro_score"] > 0.0 or snapshot["depth_available"] or snapshot["synthetic_depth_available"]:
            lines.append(f"{prefix}Micro `{snapshot['micro_score']:.2f}` | Depth `{depth_mode}`")

        context_parts = []
        if snapshot["cross_state"] and snapshot["cross_peer"]:
            context_parts.append(f"Cross-market `{snapshot['cross_state']}` via `{snapshot['cross_peer']}`")
        elif snapshot["cross_peer"]:
            context_parts.append(f"Cross-market via `{snapshot['cross_peer']}`")
        if snapshot["recent_pattern_notes"]:
            context_parts.append(f"Pattern memory `{', '.join(snapshot['recent_pattern_notes'])}`")
        elif snapshot["recent_pattern_block"]:
            context_parts.append("Pattern memory `recent-pattern block`")
        if context_parts:
            lines.append(f"{prefix}{' | '.join(context_parts)}")

        return "\n".join(lines)

    @staticmethod
    def _trade_execution_summary_parts(
        outcome: str,
        rr_realized: float,
        quality_score: float,
        target_capture: float,
        memory_score: float,
    ) -> List[str]:
        parts: List[str] = []
        if outcome:
            parts.append(f"Outcome `{outcome}`")
        parts.append(f"Realized `{rr_realized:+.2f}R`")
        if quality_score > 0.0:
            parts.append(f"Quality `{quality_score:.1f}/100`")
        if target_capture > 0.0:
            parts.append(f"Capture `{target_capture * 100:.0f}%`")
        if memory_score > 0.0:
            parts.append(f"Memory `{memory_score:.0f}`")
        return parts

    @staticmethod
    def _trade_execution_entry_parts(
        provider: str,
        agreement: str,
        spread_regime: str,
        quote_quality: str,
        broker_context: str,
        transition_risk: float,
    ) -> List[str]:
        parts: List[str] = []
        if broker_context:
            parts.append(f"State `{broker_context}`")
        if provider:
            parts.append(f"Provider `{provider}`")
        if agreement:
            parts.append(f"Agreement `{agreement}`")
        if quote_quality:
            parts.append(f"Quote `{quote_quality}`")
        if spread_regime:
            parts.append(f"Spread `{spread_regime}`")
        if transition_risk >= 0.15:
            parts.append(f"Transition `{transition_risk:.2f}`")
        return parts

    @staticmethod
    def _trade_execution_flow_parts(
        depth_mode: str,
        micro_context: str,
        stop_hunt_risk: float,
        exhaustion_risk: float,
    ) -> List[str]:
        parts: List[str] = []
        if depth_mode:
            parts.append(f"Depth `{depth_mode}`")
        if micro_context:
            parts.append(f"Flow `{micro_context}`")
        risk_parts: List[str] = []
        if stop_hunt_risk >= 0.20:
            risk_parts.append(f"stop-hunt {stop_hunt_risk:.2f}")
        if exhaustion_risk >= 0.20:
            risk_parts.append(f"exhaustion {exhaustion_risk:.2f}")
        if risk_parts:
            parts.append("Risk `" + " | ".join(risk_parts) + "`")
        return parts

    @staticmethod
    def _trade_execution_cross_parts(
        cross_context: str,
        cross_peer: str,
        cross_relation: str,
        cross_alignment: float,
        cross_confidence: float,
    ) -> List[str]:
        if cross_context and cross_peer:
            parts = [f"Cross-market `{cross_context}` via `{cross_peer}`"]
            if cross_relation:
                parts.append(f"Relation `{cross_relation}`")
            if cross_alignment > 0.0:
                parts.append(f"Align `{cross_alignment:.2f}`")
            if cross_confidence > 0.0:
                parts.append(f"Conf `{cross_confidence:.2f}`")
            return parts
        if cross_context:
            return [f"Cross-market `{cross_context}`"]
        return []

    @classmethod
    def _format_trade_execution_block(cls, trade: Dict[str, Any], prefix: str = "") -> str:
        review = cls._trade_review(trade)
        if not isinstance(review, dict) or not review:
            continuation_summary = str(trade.get("continuation_summary") or "").strip()
            if continuation_summary:
                return f"{prefix}Continuation `{continuation_summary}`"
            return ""

        entry = review.get("entry_diagnostics") if isinstance(review.get("entry_diagnostics"), dict) else {}
        outcome = cls._humanise_diagnostic_label(review.get("outcome") or "")
        rr_realized = float(review.get("rr_realized", 0.0) or 0.0)
        quality_score = float(review.get("quality_score", 0.0) or 0.0)
        target_capture = float(review.get("target_capture", 0.0) or 0.0)
        memory_score = float(review.get("memory_score", 0.0) or 0.0)

        provider = str(entry.get("primary_provider") or "").strip()
        agreement = cls._humanise_diagnostic_label(entry.get("quote_agreement_state") or "")
        spread_regime = cls._humanise_diagnostic_label(entry.get("spread_regime") or "")
        quote_quality = cls._humanise_diagnostic_label(entry.get("quote_quality_state") or "")
        broker_context = cls._humanise_diagnostic_label(entry.get("broker_context") or "")
        transition_risk = float(entry.get("market_transition_risk", 0.0) or 0.0)

        depth_mode = cls._humanise_diagnostic_label(entry.get("depth_mode") or "")
        micro_context = cls._humanise_diagnostic_label(entry.get("micro_context") or "")
        stop_hunt_risk = float(entry.get("stop_hunt_risk", 0.0) or 0.0)
        exhaustion_risk = float(entry.get("exhaustion_risk", 0.0) or 0.0)

        cross_context = cls._humanise_diagnostic_label(entry.get("cross_asset_context") or "")
        cross_peer = str(entry.get("cross_asset_primary_peer") or "").strip()
        cross_relation = cls._humanise_diagnostic_label(entry.get("cross_asset_primary_relation") or "")
        cross_alignment = float(entry.get("cross_asset_alignment", 0.0) or 0.0)
        cross_confidence = float(entry.get("cross_asset_confidence", 0.0) or 0.0)

        continuation_summary = str(trade.get("continuation_summary") or "").strip()

        lines: List[str] = []

        summary_parts = cls._trade_execution_summary_parts(
            outcome,
            rr_realized,
            quality_score,
            target_capture,
            memory_score,
        )
        if summary_parts:
            lines.append(f"{prefix}{' | '.join(summary_parts)}")

        entry_parts = cls._trade_execution_entry_parts(
            provider,
            agreement,
            spread_regime,
            quote_quality,
            broker_context,
            transition_risk,
        )
        if entry_parts:
            lines.append(f"{prefix}{' | '.join(entry_parts)}")

        flow_parts = cls._trade_execution_flow_parts(
            depth_mode,
            micro_context,
            stop_hunt_risk,
            exhaustion_risk,
        )
        if flow_parts:
            lines.append(f"{prefix}{' | '.join(flow_parts)}")

        cross_parts = cls._trade_execution_cross_parts(
            cross_context,
            cross_peer,
            cross_relation,
            cross_alignment,
            cross_confidence,
        )
        if cross_parts:
            lines.append(f"{prefix}{' | '.join(cross_parts)}")

        if continuation_summary:
            lines.append(f"{prefix}Continuation `{continuation_summary}`")

        return "\n".join(lines)

    @classmethod
    def _format_trade_history_context(cls, trade: Dict[str, Any]) -> str:
        playbook_block = cls._format_playbook_runtime_block(trade, prefix="  ")
        execution_block = cls._format_trade_execution_block(trade, prefix="  ")
        lines: List[str] = []
        if playbook_block:
            lines.append(playbook_block)
        if execution_block:
            lines.append(execution_block)
        return "\n".join(lines)

    def _main_menu_snapshot(self) -> Dict[str, Any]:
        core = self.trading_system
        open_positions = 0
        balance = "—"
        is_running = False

        if core:
            try:
                open_positions = len(core.get_positions() or [])
            except Exception:
                open_positions = 0
            try:
                balance = f"${core.get_balance():.2f}"
            except Exception:
                balance = "—"
            is_running = bool(getattr(core, "is_running", False))

        diary_trades = 0
        try:
            from services.personality_service import personality as _personality

            report = _personality.get_report() or {}
            diary_trades = int(((report.get("stats") or {}).get("total_trades_remembered", 0)) or 0)
        except Exception:
            diary_trades = 0

        return {
            "open_positions": open_positions,
            "balance": balance,
            "is_running": is_running,
            "diary_trades": diary_trades,
        }

    def _build_main_menu(self) -> tuple[str, InlineKeyboardMarkup]:
        summary = self._main_menu_snapshot()
        status = "🟢 Running" if summary["is_running"] else "🔴 Stopped"
        balance = summary["balance"]
        open_positions = int(summary["open_positions"])
        diary_trades = int(summary["diary_trades"])

        if diary_trades > 0:
            history_line = (
                f"History is live: diary has {diary_trades} closed trade"
                f"{'s' if diary_trades != 1 else ''} to learn from."
            )
        else:
            history_line = "The diary fills in after the bot has closed trades to learn from."

        text = (
            f"🤖 *Robbie Control Panel*\n\n"
            f"Status: {status}\n"
            f"Balance: {balance}\n"
            f"Open positions: {open_positions}\n\n"
            f"*Quick guide*\n"
            f"• `Signals` scans the live decision engine for current setups.\n"
            f"• `Ask Robbie` explains one asset in plain English.\n"
            f"• `Positions` manages active trades.\n"
            f"• {history_line}\n\n"
            f"_{now_in_display_timezone().strftime('%H:%M:%S')} {display_timezone_label()}_"
        )
        return text, _main_menu_keyboard(summary)

    def _position_price_context(
        self,
        core: Any,
        position: Dict[str, Any],
        asset: str,
        entry: float,
        size: float,
        direction: str,
    ) -> tuple[float, str]:
        display_current = float(position.get("current_price", entry) or entry)
        pnl_str = ""
        try:
            fetcher = getattr(core, "fetcher", None)
            cat = str(position.get("category", "forex") or "forex")

            def _provider_fallback(fallback_asset: str, fallback_category: str) -> tuple[Optional[float], str]:
                if not fetcher:
                    return None, ""
                price, _meta = fetcher.get_real_time_price(fallback_asset, fallback_category)
                return (float(price), "provider quote") if price not in (None, 0, 0.0) else (None, "")

            quote = resolve_live_position_snapshot(
                {
                    **dict(position or {}),
                    "asset": asset,
                    "category": cat,
                    "direction": direction,
                    "entry_price": entry,
                    "position_size": size,
                },
                live_snapshot_max_age_seconds=3.0,
                provider_fallback=_provider_fallback,
            )
            if float(quote.get("current_price", 0.0) or 0.0):
                display_current = float(quote.get("current_price", display_current) or display_current)
                pnl = float(quote.get("pnl", 0.0) or 0.0)
                pnl_str = f"  P&L: `${pnl:+.2f}`\n"
        except Exception:
            pass
        return display_current, pnl_str

    @staticmethod
    def _position_open_time_text(position: Dict[str, Any]) -> str:
        try:
            ot = position.get("open_time", "")
            if ot:
                opened = to_display_datetime(ot)
                if opened is None:
                    return ""
                elapsed = now_in_display_timezone() - opened
                mins = int(elapsed.total_seconds() / 60)
                if mins < 60:
                    duration = f"{mins}m ago"
                elif mins < 1440:
                    duration = f"{mins//60}h {mins%60}m ago"
                else:
                    duration = f"{mins//1440}d {(mins%1440)//60}h ago"
                return f"  ⏱ Opened: `{opened.strftime('%b %d %H:%M')} {display_timezone_label()}` ({duration})\n"
        except Exception:
            pass
        return ""

    def _position_target_text(self, position: Dict[str, Any], asset: str, sl: float, tp: float) -> str:
        target_plan = self._target_snapshot(position)
        primary_tp = float(target_plan.get("primary_target", 0.0) or 0.0)
        runner_tp = float(target_plan.get("runner_target", 0.0) or 0.0)
        if runner_tp > 0 and abs(runner_tp - primary_tp) > 1e-9:
            return (
                f"  Stop:  `{self._fmt_price(sl, asset)}` | TP1: `{self._fmt_price(primary_tp, asset)}`\n"
                f"  Run:   `{self._fmt_price(runner_tp, asset)}`\n"
            )
        return f"  Stop:  `{self._fmt_price(sl, asset)}` | Target: `{self._fmt_price(primary_tp or tp, asset)}`\n"

    async def _build_positions(self):
        core = self.trading_system
        if not core:
            return "⏳ Engine not ready.", _kb([("🏠 Menu", "menu")])

        positions = core.get_positions()
        if not positions:
            return (
                "📭 *No open positions*\n\nThe bot isn't in any trades right now.",
                _kb([("🎯 Find a signal", "signals"), ("🏠 Menu", "menu")])
            )

        lines = [f"📈 *Open Positions ({len(positions)})*\n"]
        buttons = []

        for i, p in enumerate(positions[:8], 1):
            direction = (p.get("direction") or p.get("signal", "BUY")).upper()
            emoji     = "🟢" if direction == "BUY" else "🔴"
            asset     = p.get("asset", "?")
            entry     = float(p.get("entry_price", 0))
            sl        = float(p.get("stop_loss", 0))
            tp        = float(p.get("take_profit", 0))
            size      = float(p.get("position_size", 0))
            conf      = float(p.get("confidence", 0))
            tid       = p.get("trade_id", "")
            display_current, pnl_str = self._position_price_context(core, p, asset, entry, size, direction)
            open_time_str = self._position_open_time_text(p)

            diagnostics_block = self._format_runtime_diagnostics_block(p, prefix="  ")
            diagnostics_text = f"{diagnostics_block}\n" if diagnostics_block else ""
            playbook_block = self._format_playbook_runtime_block(p, prefix="  ")
            playbook_text = f"{playbook_block}\n" if playbook_block else ""
            target_line = self._position_target_text(p, asset, sl, tp)

            lines.append(
                f"{emoji} *{asset}* ({direction})\n"
                f"  Entry: `{self._fmt_price(entry, asset)}` → Current: `{self._fmt_price(display_current, asset)}`\n"
                f"{target_line}"
                f"  Conf: {conf:.0%} | Size: `{p.get('position_size', 0):.4f}`\n"
                f"{pnl_str}"
                f"{open_time_str}"
                f"{playbook_text}"
                f"{diagnostics_text}"
                f"  ID: `{tid}`\n"
            )
            buttons.append([(f"❌ Close {asset}", f"close:{tid}")])

        if len(positions) > 8:
            lines.append(f"_…and {len(positions) - 8} more_")

        buttons.append([
            ("🧭 Top Setups", "top_setups"),
            ("⚙️ Reprice Weak", "reprice_weak"),
        ])
        buttons.append([
            ("📉 Reduce Weak", "reduce_weak"),
            ("⚡ Manage Positions", "close_menu"),
            ("📋 Trade History",    "history"),
        ])
        buttons.append([("🔄 Refresh", "positions")])
        buttons.append([("🏠 Menu", "menu")])
        return "\n".join(lines), _kb(*buttons)

    # ── Bulk close menu ──────────────────────────────────────────────────────

    async def _cmd_history(self, update, ctx) -> None:
        """Show recent trade history via /history command."""
        await self._show_history(update.message.reply_text)

    async def _btn_history(self, query, filter_cat: str = "all") -> None:
        """Show trade history with optional filter."""
        await self._show_history(query.edit_message_text, filter_cat=filter_cat)

    async def _btn_reprice_weak(self, query) -> None:
        await query.edit_message_text("Adjusting weak exits…", parse_mode=ParseMode.MARKDOWN)
        text, kb = self._do_reprice_weak()
        await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)

    async def _btn_reduce_weak(self, query) -> None:
        await query.edit_message_text("Reducing weakest live positions…", parse_mode=ParseMode.MARKDOWN)
        text, kb = self._do_reduce_weak()
        await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)

    async def _btn_top_setups(self, query) -> None:
        await query.edit_message_text("Scanning current opportunities…", parse_mode=ParseMode.MARKDOWN)
        text, kb = self._build_top_setups(refresh=True)
        await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)

    @staticmethod
    def _history_trade_get(trade: Any, key: str, default=None):
        if isinstance(trade, dict):
            return trade.get(key, default)
        return getattr(trade, key, default)

    @staticmethod
    def _history_normalize_trade(trade: Any) -> Dict[str, Any]:
        if isinstance(trade, dict):
            row = dict(trade)
        elif hasattr(trade, "to_dict"):
            row = dict(trade.to_dict())
        else:
            row = {}
        entry_time = row.get("entry_time") or row.get("open_time")
        if entry_time:
            row["entry_time"] = entry_time
            row.setdefault("open_time", entry_time)
        return row

    @classmethod
    def _history_load_recent_trades(cls, limit: int = 120) -> List[Dict[str, Any]]:
        raw_trades: List[Dict[str, Any]] = []
        try:
            from services.db_pool import get_db

            raw_trades = [cls._history_normalize_trade(t) for t in (get_db().get_recent_trades(limit=limit) or [])]
        except Exception:
            raw_trades = []

        if not raw_trades:
            from core.state import state as runtime_state

            raw_trades = [cls._history_normalize_trade(t) for t in runtime_state.get_closed_positions(limit=limit)]
        return raw_trades

    @classmethod
    def _history_trade_lines(
        cls,
        trade: Dict[str, Any],
        cat_emojis: Dict[str, str],
        reason_emojis: Dict[str, str],
    ) -> List[str]:
        pnl = float(cls._history_trade_get(trade, "pnl", 0) or 0)
        category = str(cls._history_trade_get(trade, "category", "") or "")
        em = cat_emojis.get(category, "📊")
        pnl_em = "🟢" if pnl >= 0 else "🔴"
        dir_ = str(cls._history_trade_get(trade, "direction", "BUY") or "BUY").upper()

        open_t_raw = cls._history_trade_get(trade, "entry_time") or cls._history_trade_get(trade, "open_time")
        close_t_raw = cls._history_trade_get(trade, "exit_time")
        open_t = None
        close_t = None
        try:
            if open_t_raw:
                open_t = datetime.fromisoformat(str(open_t_raw).replace("Z", "+00:00"))
            if close_t_raw:
                close_t = datetime.fromisoformat(str(close_t_raw).replace("Z", "+00:00"))
        except Exception:
            open_t = None
            close_t = None

        duration_minutes = cls._history_trade_get(trade, "duration_minutes")
        if duration_minutes not in (None, ""):
            mins = max(0, int(float(duration_minutes)))
        elif open_t and close_t:
            mins = max(0, int((close_t - open_t).total_seconds() / 60))
        else:
            mins = None

        dur_str = ""
        if mins is not None:
            if mins < 60:
                dur_str = f"{mins}m"
            elif mins < 1440:
                dur_str = f"{mins//60}h {mins%60}m"
            else:
                dur_str = f"{mins//1440}d"

        open_display = to_display_datetime(open_t) if open_t else None
        close_display = to_display_datetime(close_t) if close_t else None
        open_str = open_display.strftime("%d %b %H:%M") if open_display else "—"
        close_str = close_display.strftime("%d %b %H:%M") if close_display else "—"
        reason = str(cls._history_trade_get(trade, "display_exit_reason", cls._history_trade_get(trade, "exit_reason", "")) or "")
        r_em = next((v for k, v in reason_emojis.items() if k in reason), "📌")
        continuation_summary = str(cls._history_trade_get(trade, "continuation_summary", "") or "")
        asset = cls._history_trade_get(trade, "asset", "UNKNOWN")

        lines = [
            f"{em} *{asset}* {dir_}\n"
            f"  🕐 {open_str} → {close_str} ({dur_str})\n"
            f"  {r_em} {reason or '—'} | {pnl_em} ${pnl:+.2f}\n"
        ]
        if continuation_summary:
            lines.append(f"  ↪ {continuation_summary}")
        review_context = TelegramCommander._format_trade_history_context(trade)
        if review_context:
            lines.append(review_context)
        return lines

    async def _show_history(self, send_fn, filter_cat: str = "all") -> None:
        """Render last 10 closed trades with open/close times and P&L."""
        try:
            from core.state import rollup_closed_trade_history

            raw_trades = TelegramCommander._history_load_recent_trades(limit=120)

            category_filter = filter_cat if filter_cat in ("forex", "crypto", "commodities", "indices") else ""
            pnl_filter = filter_cat if filter_cat in ("won", "lost") else "all"
            trades = rollup_closed_trade_history(raw_trades, limit=30)
            if category_filter:
                trades = [
                    t
                    for t in trades
                    if str(TelegramCommander._history_trade_get(t, "category", "") or "") == category_filter
                ]
            if pnl_filter == "won":
                trades = [t for t in trades if float(TelegramCommander._history_trade_get(t, "pnl", 0) or 0) > 0]
            elif pnl_filter == "lost":
                trades = [t for t in trades if float(TelegramCommander._history_trade_get(t, "pnl", 0) or 0) < 0]

            if not trades:
                await send_fn(
                    "📋 *TRADE HISTORY*\n\nNo closed trades found.",
                    parse_mode="Markdown",
                    reply_markup=_kb(
                        [("📋 All", "history_filter:all"), ("💱 Forex", "history_filter:forex")],
                        [("₿ Crypto", "history_filter:crypto"), ("🥇 Comms", "history_filter:commodities")],
                        [("📉 Indices", "history_filter:indices")],
                        [("🟢 Winners", "history_filter:won"), ("🔴 Losers", "history_filter:lost")],
                        [("🏠 Menu", "menu")],
                    ),
                )
                return

            cat_emojis = {"forex":"💱","crypto":"₿","commodities":"🥇","indices":"📈"}
            reason_emojis = {
                "Take Profit": "🎯", "Stop Loss": "🛑",
                "Trailing": "📈", "Manual": "👆", "Break": "⚖️"
            }

            lines = [f"📋 *TRADE HISTORY* ({filter_cat.upper()})\n"]
            total_pnl = sum(float(TelegramCommander._history_trade_get(t, "pnl", 0) or 0) for t in trades[:10])
            won  = sum(1 for t in trades[:10] if float(TelegramCommander._history_trade_get(t, "pnl", 0) or 0) > 0)
            lost = sum(1 for t in trades[:10] if float(TelegramCommander._history_trade_get(t, "pnl", 0) or 0) < 0)
            lines.append(f"Last {min(10,len(trades))} trades | 🟢 {won} won | 🔴 {lost} lost | Net: ${total_pnl:+.2f}\n")

            for t in trades[:10]:
                lines.extend(TelegramCommander._history_trade_lines(t, cat_emojis, reason_emojis))

            await send_fn(
                "\n".join(lines),
                parse_mode="Markdown",
                reply_markup=_kb(
                    [("📋 All", "history_filter:all"), ("💱 Forex", "history_filter:forex")],
                    [("₿ Crypto", "history_filter:crypto"), ("🥇 Comms", "history_filter:commodities")],
                    [("📉 Indices", "history_filter:indices")],
                    [("🟢 Winners", "history_filter:won"), ("🔴 Losers", "history_filter:lost")],
                    [("🏠 Menu", "menu")],
                ),
            )
        except Exception as e:
            logger.error(f"[Telegram] history error: {e}", exc_info=True)
            await send_fn(f"❌ Error loading history: {e}")

    async def _btn_close_menu(self, query) -> None:
        """Show the bulk position management menu."""
        core = self.trading_system
        if not core:
            await query.edit_message_text("⏳ Engine not ready.")
            return
        positions = core.get_positions()
        if not positions:
            await query.edit_message_text("📭 No open positions to manage.")
            return

        # Count by category and P&L
        cats = {}
        losing = winning = 0
        for p in positions:
            cat = p.get("category", "other")
            cats[cat] = cats.get(cat, 0) + 1
            pnl = float(p.get("pnl", 0) or 0)
            if pnl < 0: losing += 1
            elif pnl > 0: winning += 1

        lines = [
            "⚡ *POSITION MANAGER*",
            f"📊 {len(positions)} open position(s)\n",
        ]
        for cat, count in sorted(cats.items()):
            emoji = {"forex":"💱","crypto":"₿","commodities":"🥇","indices":"📈"}.get(cat, "📊")
            lines.append(f"  {emoji} {cat.title()}: {count} position(s)")

        lines.append(f"\n🔴 Losing: {losing}  |  🟢 Winning: {winning}")

        buttons = []
        # Per category buttons
        cat_row = []
        for cat in sorted(cats.keys()):
            emoji = {"forex":"💱","crypto":"₿","commodities":"🥇","indices":"📈"}.get(cat, "📊")
            cat_row.append((f"{emoji} Close {cat.title()}", f"close_cat:{cat}"))
            if len(cat_row) == 2:
                buttons.append(cat_row)
                cat_row = []
        if cat_row:
            buttons.append(cat_row)

        # P&L filter buttons
        buttons.append([
            ("🔴 Close Losing", "close_losing"),
            ("🟢 Close Winning", "close_winning"),
        ])
        buttons.append([("💣 Close ALL", "close_all_confirm")])
        buttons.append([("◀️ Back", "positions"), ("🏠 Menu", "menu")])

        await query.edit_message_text(
            "\n".join(lines),
            parse_mode="Markdown",
            reply_markup=_kb(*buttons),
        )

    async def _btn_close_category(self, query, category: str) -> None:
        """Close all positions in a category."""
        core = self.trading_system
        if not core:
            await query.edit_message_text("⏳ Engine not ready.")
            return
        positions = [p for p in core.get_positions() if p.get("category") == category]
        if not positions:
            await query.edit_message_text(f"📭 No open {category} positions.")
            return

        closed = errors = 0
        results = []
        for p in positions:
            tid = p.get("trade_id", "")
            asset = p.get("asset", "")
            try:
                if self._is_weekend_for_category(category):
                    results.append(f"⏸ {asset} — market closed (weekend)")
                    continue
                result = core.close_position_manually(tid)
                if result and result.get("success"):
                    pnl = result.get("pnl", 0)
                    results.append(f"✅ {asset} closed | P&L: ${pnl:+.2f}")
                    closed += 1
                else:
                    results.append(f"❌ {asset} — {result.get('error','failed')}")
                    errors += 1
            except Exception as e:
                results.append(f"❌ {asset} — {e}")
                errors += 1

        emoji = {"forex":"💱","crypto":"₿","commodities":"🥇","indices":"📈"}.get(category,"📊")
        summary = [
            f"{emoji} *{category.upper()} POSITIONS CLOSED*",
            f"✅ Closed: {closed}  |  ❌ Errors: {errors}\n",
        ] + results

        await query.edit_message_text(
            "\n".join(summary),
            parse_mode="Markdown",
            reply_markup=_kb([("◀️ Back", "close_menu"), ("🏠 Menu", "menu")]),
        )

    async def _btn_close_filter(self, query, mode: str) -> None:
        """Close losing or winning positions."""
        core = self.trading_system
        if not core:
            await query.edit_message_text("⏳ Engine not ready.")
            return

        all_pos = core.get_positions()
        if mode == "losing":
            positions = [p for p in all_pos if float(p.get("pnl", 0) or 0) < 0]
            title = "🔴 LOSING POSITIONS CLOSED"
        else:
            positions = [p for p in all_pos if float(p.get("pnl", 0) or 0) > 0]
            title = "🟢 WINNING POSITIONS CLOSED"

        if not positions:
            label = "losing" if mode == "losing" else "winning"
            await query.edit_message_text(f"📭 No {label} positions found.")
            return

        closed = errors = 0
        results = []
        for p in positions:
            tid = p.get("trade_id", "")
            asset = p.get("asset", "")
            pnl = float(p.get("pnl", 0) or 0)
            cat = p.get("category", "forex")
            try:
                if self._is_weekend_for_category(cat):
                    results.append(f"⏸ {asset} — market closed")
                    continue
                result = core.close_position_manually(tid)
                if result and result.get("success"):
                    actual_pnl = result.get("pnl", pnl)
                    results.append(f"✅ {asset} | P&L: ${actual_pnl:+.2f}")
                    closed += 1
                else:
                    results.append(f"❌ {asset} — {result.get('error','failed')}")
                    errors += 1
            except Exception as e:
                results.append(f"❌ {asset} — {e}")
                errors += 1

        summary = [
            f"*{title}*",
            f"✅ Closed: {closed}  |  ❌ Errors: {errors}\n",
        ] + results

        await query.edit_message_text(
            "\n".join(summary),
            parse_mode="Markdown",
            reply_markup=_kb([("◀️ Back", "close_menu"), ("🏠 Menu", "menu")]),
        )

    async def _btn_close_all_confirm(self, query) -> None:
        """Confirm close all positions."""
        core = self.trading_system
        if not core:
            await query.edit_message_text("⏳ Engine not ready.")
            return
        count = len(core.get_positions())
        await query.edit_message_text(
            f"⚠️ *CONFIRM CLOSE ALL*\n\n"
            f"This will close all *{count} open position(s)*.\n"
            f"This action cannot be undone.\n\n"
            f"Are you sure?",
            parse_mode="Markdown",
            reply_markup=_kb(
                [("💣 YES — Close All", "close_all_execute")],
                [("❌ Cancel", "close_menu")],
            ),
        )

    async def _btn_close_all_execute(self, query) -> None:
        """Execute close all positions."""
        core = self.trading_system
        if not core:
            await query.edit_message_text("⏳ Engine not ready.")
            return

        positions = core.get_positions()
        closed = errors = total_pnl = 0
        results = []

        for p in positions:
            tid = p.get("trade_id", "")
            asset = p.get("asset", "")
            cat = p.get("category", "forex")
            try:
                if self._is_weekend_for_category(cat):
                    results.append(f"⏸ {asset} — market closed")
                    continue
                result = core.close_position_manually(tid)
                if result and result.get("success"):
                    pnl = float(result.get("pnl", 0))
                    total_pnl += pnl
                    emoji = "🟢" if pnl >= 0 else "🔴"
                    results.append(f"{emoji} {asset} | ${pnl:+.2f}")
                    closed += 1
                else:
                    results.append(f"❌ {asset} — {result.get('error','failed')}")
                    errors += 1
            except Exception as e:
                results.append(f"❌ {asset} — {e}")
                errors += 1

        pnl_emoji = "🟢" if total_pnl >= 0 else "🔴"
        summary = [
            "💣 *ALL POSITIONS CLOSED*",
            f"✅ Closed: {closed}  |  ❌ Errors: {errors}",
            f"{pnl_emoji} Total P&L: *${total_pnl:+.2f}*\n",
        ] + results

        await query.edit_message_text(
            "\n".join(summary),
            parse_mode="Markdown",
            reply_markup=_kb([("📊 Positions", "positions"), ("🏠 Menu", "menu")]),
        )

    async def _build_balance(self):
        core = self.trading_system
        if not core:
            return "⏳ Engine not ready.", _kb([("🏠 Menu", "menu")])

        perf    = core.get_performance()
        daily   = core.get_daily_stats()
        bal     = core.get_balance()
        init    = perf.get("initial_balance", bal)
        total   = perf.get("total_pnl", 0)
        d_pnl   = daily.get("daily_pnl", 0)
        d_trades= daily.get("daily_trades", 0)
        wr      = perf.get("win_rate", 0)
        growth  = ((bal - init) / init * 100) if init else 0
        g_icon  = "📈" if growth >= 0 else "📉"
        d_icon  = "📈" if d_pnl >= 0 else "📉"

        text = (
            f"💰 *Account Balance*\n"
            f"{'─' * 24}\n"
            f"Current:  `${bal:,.2f}`\n"
            f"Started:  `${init:,.2f}`\n"
            f"Growth:   {g_icon} `{growth:+.2f}%`\n\n"
            f"📊 *Performance*\n"
            f"Total P&L:    `${total:+.2f}`\n"
            f"Today's P&L:  {d_icon} `${d_pnl:+.2f}`\n"
            f"Today trades: {d_trades}\n"
            f"Win Rate:     {wr:.1f}%\n"
            f"Total trades: {perf.get('total_trades', 0)}\n\n"
            f"📉 *Risk*\n"
            f"Avg Win:  `${perf.get('avg_win', 0):.2f}`\n"
            f"Avg Loss: `${perf.get('avg_loss', 0):.2f}`\n"
            f"P-Factor: {perf.get('profit_factor', 0):.2f}"
        )
        kb = _kb(
            [("🔄 Refresh", "balance"),  ("📊 Status",   "status")],
            [("🏠 Menu", "menu")],
        )
        return text, kb

    def _inspect_asset_snapshot(self, asset: str):
        core = self.trading_system
        analysis = None
        signal = None
        df = None

        if core:
            try:
                inspect_fn = getattr(core, "inspect_asset", None)
                if callable(inspect_fn):
                    analysis = inspect_fn(asset)
            except Exception:
                analysis = None

            if isinstance(analysis, dict):
                maybe_signal = analysis.get("signal")
                if isinstance(maybe_signal, dict):
                    signal = maybe_signal

            if signal is None:
                try:
                    signal = core.get_signal_for_asset(asset)
                except Exception:
                    signal = None

            if analysis is None and isinstance(signal, dict) and signal:
                meta = signal.get("metadata") if isinstance(signal.get("metadata"), dict) else {}
                analysis = {
                    "asset": str(signal.get("asset") or asset),
                    "canonical_asset": str(signal.get("canonical_asset") or signal.get("asset") or asset),
                    "category": str(signal.get("category") or ""),
                    "market_status": {"market_open": True, "reason": "open"},
                    "decision_status": "accepted" if signal.get("alive", True) else "killed",
                    "decision_reason": str(signal.get("kill_reason") or ""),
                    "signal": signal,
                    "playbook_decision": {
                        "playbook": meta.get("playbook_name"),
                        "entry_style": meta.get("playbook_entry_style"),
                        "session_label": meta.get("session_label") or meta.get("playbook_session"),
                        "preferred_interval": meta.get("playbook_timeframe"),
                        "confidence": meta.get("playbook_confidence", signal.get("confidence", 0.0)),
                    },
                    "market_structure": dict(meta.get("market_structure") or {}),
                    "market_intelligence": {},
                    "broker_quality": dict(meta.get("broker_quality") or {}),
                    "market_microstructure": dict(meta.get("market_microstructure") or {}),
                    "cross_asset_context": dict(meta.get("cross_asset_context") or {}),
                    "sentiment_score": float(meta.get("sentiment_score", 0.0) or 0.0),
                    "funding_bias": str(meta.get("funding_bias", "NEUTRAL") or "NEUTRAL"),
                    "oi_signal": str(meta.get("oi_signal", "NEUTRAL") or "NEUTRAL"),
                    "timeframe": str(meta.get("playbook_timeframe") or ""),
                    "current_price": float(signal.get("entry_price", 0.0) or 0.0),
                    "latest_close": float(signal.get("entry_price", 0.0) or 0.0),
                    "open_position": None,
                }

            try:
                from core.assets import registry

                canonical = str((analysis or {}).get("canonical_asset") or "") or registry.canonical(asset)
                category = str((analysis or {}).get("category") or "") or registry.category(canonical)
                fetcher = core.fetcher
                if fetcher:
                    df = fetcher.get_ohlcv(canonical or asset, category, interval="15m", periods=100)
                    if df is not None and not df.empty:
                        from indicators.technical import TechnicalIndicators

                        df = TechnicalIndicators.add_all_indicators(df)
            except Exception:
                df = None

        return analysis, signal, df

    async def _build_signal(self, asset: str):
        core = self.trading_system
        if not core:
            return "⏳ Engine not ready.", _kb([("🏠 Menu", "menu")])

        display = _DISPLAY.get(asset, asset)
        try:
            analysis, sig, df = self._inspect_asset_snapshot(asset)
            from services.personality_service import RobbieExplainer

            explainer = RobbieExplainer()
            text = explainer.describe_market_state(display, analysis or {"asset": asset, "signal": sig}, df=df, topic="signal")
            explainer.close()
        except Exception as e:
            return f"❌ Error: {e}", _kb([("◀️ Back", "signals"), ("🏠 Menu", "menu")])

        kb = _kb(
            [(f"🧠 Why {display}?", f"why:{asset}"), ("🔄 Refresh", f"sig:{asset}")],
            [("◀️ Assets", "signals"), ("🏠 Menu", "menu")],
        )
        return text, kb

    async def _build_why(self, asset: str) -> str:
        display = _DISPLAY.get(asset, asset)

        try:
            analysis, sig, df = self._inspect_asset_snapshot(asset)
            from services.personality_service import RobbieExplainer
            explainer = RobbieExplainer()
            text      = explainer.explain_signal(
                asset=display, df=df, signal=sig or {}, analysis=analysis,
            )
            explainer.close()
            return text
        except Exception as e:
            return (
                f"🧠 *{display}*\n\n"
                f"Couldn't get full explanation: {e}\n"
                f"Try `/ask {asset} explain`."
            )

    def _build_mood(self) -> str:
        try:
            from services.personality_service import PersonalityDatabase
            db     = PersonalityDatabase()
            report = db.get_personality_report()
            db.close()
        except Exception:
            return "❌ Personality service unavailable."

        mood   = report.get("current_mood", "neutral")
        emoji  = report.get("mood_emoji",   "😐")
        stats  = report.get("stats", {})
        traits = report.get("traits", {})

        text = f"😶 *Robbie's Mood: {emoji} {mood.upper()}*\n\n"

        cw = stats.get("consecutive_wins", 0)
        cl = stats.get("consecutive_losses", 0)
        if cw >= 3:
            text += f"🔥 On a {cw}-trade winning streak!\n"
        elif cl >= 3:
            text += f"😰 {cl} losses in a row — being careful.\n"

        if mood in {"grumpy", "cautious", "shaken"}:
            posture = (
                "Operationally this means Robbie is being selective. "
                "He wants cleaner retests, fresher confirmation, and less late-entry risk before acting."
            )
        elif mood in {"confident", "euphoric", "on_fire"}:
            posture = (
                "Operationally this means Robbie is more willing to press clean continuation setups, "
                "as long as structure and execution still agree."
            )
        else:
            posture = (
                "Operationally this means Robbie is balanced: he will take the clean setups, "
                "but he is not forcing trades just because the market is moving."
            )

        text += (
            f"{posture}\n\n"
            f"\n📊 *This Week*\n"
            f"Win Rate:    {stats.get('weekly_win_rate', 0):.0f}%\n"
            f"Trades:      {stats.get('weekly_trades', 0)}\n\n"
            f"📊 *Last 10 Trades*\n"
            f"Wins:  {stats.get('last_10_wins', 0)}/10\n"
            f"P&L:   ${stats.get('last_10_pnl', 0):+.2f}\n\n"
            f"🎭 *Personality*\n"
            f"Confidence:   {traits.get('base_confidence', 0.7)*100:.0f}%\n"
            f"Cautiousness: {traits.get('cautiousness', 0.5)*100:.0f}%\n"
            f"Optimism:     {traits.get('optimism', 0.6)*100:.0f}%"
        )
        return text

    def _build_diary(self) -> str:
        try:
            from services.personality_service import personality as _pers
            report = _pers.get_report()
        except Exception:
            return "❌ Diary unavailable."

        moments = report.get("memorable_moments", [])
        total = report.get("stats", {}).get("total_trades_remembered", 0)
        text    = "📔 *Trading Diary*\n\n"

        if moments:
            text += "*Memorable Moments*\n"
            for m in moments:
                icon  = "✅" if m.get("is_win") else "❌"
                pnl   = m.get("pnl", 0)
                ps    = f"+${pnl:.0f}" if pnl >= 0 else f"-${abs(pnl):.0f}"
                text += f"{icon} {m.get('title', '—')} — {ps} _{m.get('date', '')}_\n"
        elif total > 0:
            text += (
                "You've got closed trades in memory, but none have been promoted into memorable diary moments yet.\n"
                "As more trades close, the diary will start surfacing the standout wins, losses, and lessons.\n"
            )
        else:
            text += (
                "The diary is still empty because it feeds on *closed trades*.\n"
                "Once the bot has a trade history, this page will store the memorable wins, losses, and lessons.\n"
            )

        text += f"\n_Total trades in memory: {total}_"
        return text

    @staticmethod
    def _is_weekend_for_category(category: str) -> bool:
        """Returns True if the market for this category is closed right now."""
        if category == "crypto":
            return False   # crypto never sleeps
        from datetime import datetime as _dt, timezone as _tz
        _now  = _dt.now(tz=_tz.utc)
        _wd   = _now.weekday()   # 5=Sat 6=Sun 4=Fri
        _hour = _now.hour
        return (
            _wd == 5                          # all Saturday
            or (_wd == 6 and _hour < 22)      # Sunday before 22:00 UTC
            or (_wd == 4 and _hour >= 22)     # Friday after 22:00 UTC
        )

    def _do_close(self, trade_id: str):
        core = self.trading_system
        if not core:
            return "⏳ Engine not ready.", _kb([("🏠 Menu", "menu")])
        try:
            # ── Weekend block — non-crypto cannot be closed when market is shut ──
            positions = core.get_positions()
            pos       = next((p for p in positions if p.get("trade_id") == trade_id), None)
            if pos:
                category = pos.get("category", "forex")
                if self._is_weekend_for_category(category):
                    asset = pos.get("asset", trade_id)
                    return (
                        f"🚫 *Market Closed — Cannot Close Position*\n\n"
                        f"*{asset}* ({category}) cannot be closed right now "
                        f"because {category} markets are closed on weekends.\n\n"
                        f"Your position will remain open and SL/TP will resume "
                        f"automatically when the market reopens:\n"
                        f"• *Forex & Commodities* — Monday 01:00 {display_timezone_label()}\n"
                        f"• *Indices* — Monday market open\n\n"
                        f"_Only crypto positions can be closed on weekends._",
                        _kb([("📈 Positions", "positions"), ("🏠 Menu", "menu")])
                    )
            result = core.close_position_manually(trade_id)
            if not result:
                return (
                    f"❌ Trade `{trade_id}` not found.",
                    _kb([("◀️ Positions", "positions"), ("🏠 Menu", "menu")])
                )
            pnl  = float(result.get("pnl", 0))
            icon = "✅" if pnl >= 0 else "❌"
            return (
                f"{icon} *Closed*\n\n"
                f"Asset:   {result.get('asset', trade_id)}\n"
                f"P&L:     `${pnl:+.2f}`\n"
                f"Entry:   `{self._fmt_price(float(result.get('entry_price', 0)), result.get('asset', ''))}`\n"
                f"Exit:    `{self._fmt_price(float(result.get('exit_price', 0)), result.get('asset', ''))}`\n"
                f"Reason:  {result.get('exit_reason', 'Manual')}",
                _kb([("📈 Positions", "positions"), ("🏠 Menu", "menu")])
            )
        except Exception as e:
            logger.error(f"[Telegram] close error: {e}")
            return f"❌ Error: {e}", _kb([("🏠 Menu", "menu")])

    def _do_pause(self) -> str:
        core = self.trading_system
        if not core:
            return "⏳ Engine not ready."
        if not core.is_running:
            return "⚠️ Already paused."
        try:
            core.stop(reason="Paused via Telegram")
            return "⏸️ *Trading Paused*\n\nNo new trades will open.\nUse ▶️ Resume to restart."
        except Exception as e:
            return f"❌ Error: {e}"

    def _do_resume(self) -> str:
        core = self.trading_system
        if not core:
            return "⏳ Engine not ready."
        if core.is_running:
            return "⚠️ Already running."
        try:
            core.start()
            return "▶️ *Trading Resumed*\n\nScanning for opportunities."
        except Exception as e:
            return f"❌ Error: {e}"

    def _do_reprice_weak(self):
        core = self.trading_system
        if not core:
            return "⏳ Engine not ready.", _kb([("🏠 Menu", "menu")])
        try:
            updates = core.reprice_weak_exits(limit=3, score_threshold=0.62, tighten_only=True)
            if not updates:
                return (
                    "No weak exits needed adjustment right now.",
                    _kb([("📈 Positions", "positions"), ("🏠 Menu", "menu")]),
                )

            lines = [
                "*Weak Exit Repricing*",
                f"{len(updates)} position(s) updated.\n",
            ]
            for row in updates[:4]:
                reasons = ", ".join(row.get("weak_reasons", [])[:2]) or "quality drift"
                lines.append(
                    f"*{row.get('asset','?')}*"
                    f"\n  Stop `{self._fmt_price(float(row.get('old_stop_loss', 0) or 0), row.get('asset',''))}`"
                    f" → `{self._fmt_price(float(row.get('new_stop_loss', 0) or 0), row.get('asset',''))}`"
                    f"\n  Target `{self._fmt_price(float(row.get('old_take_profit', 0) or 0), row.get('asset',''))}`"
                    f" → `{self._fmt_price(float(row.get('new_take_profit', 0) or 0), row.get('asset',''))}`"
                    f"\n  Quality `{float(row.get('quality_score', 0.0) or 0.0):.1f}` | {reasons}\n"
                )
            return "\n".join(lines), _kb(
                [("📈 Positions", "positions"), ("🧭 Top Setups", "top_setups")],
                [("🏠 Menu", "menu")],
            )
        except Exception as e:
            logger.error(f"[Telegram] reprice weak error: {e}", exc_info=True)
            return f"❌ Error: {e}", _kb([("🏠 Menu", "menu")])

    def _do_reduce_weak(self):
        core = self.trading_system
        if not core:
            return "⏳ Engine not ready.", _kb([("🏠 Menu", "menu")])
        try:
            actions = core.reduce_weak_positions(limit=3, score_threshold=0.58, reduction_fraction=0.35)
            if not actions:
                return (
                    "No weak positions qualified for reduction right now.",
                    _kb([("📈 Positions", "positions"), ("🏠 Menu", "menu")]),
                )

            lines = ["*Weak Position Reduction*\n"]
            for row in actions[:4]:
                if row.get("success"):
                    lines.append(
                        f"*{row.get('asset','?')}* reduced `{int(float(row.get('reduction_fraction',0) or 0)*100)}%`"
                        f"\n  Realised `${float(row.get('realized_pnl', 0.0) or 0.0):+.2f}`"
                        f" | Remaining `{float(row.get('remaining_size', 0.0) or 0.0):.4f}`"
                        f"\n  Quality `{float(row.get('quality_score', 0.0) or 0.0):.1f}`"
                        f" | {', '.join(row.get('weak_reasons', [])[:2]) or 'quality drift'}\n"
                    )
                else:
                    lines.append(
                        f"*{row.get('asset','?')}* skipped"
                        f"\n  {row.get('reason','not eligible')}\n"
                    )
            return "\n".join(lines), _kb(
                [("📈 Positions", "positions"), ("⚙️ Reprice Weak", "reprice_weak")],
                [("🏠 Menu", "menu")],
            )
        except Exception as e:
            logger.error(f"[Telegram] reduce weak error: {e}", exc_info=True)
            return f"❌ Error: {e}", _kb([("🏠 Menu", "menu")])

    def _build_top_setups(self, refresh: bool = False):
        core = self.trading_system
        if not core:
            return "⏳ Engine not ready.", _kb([("🏠 Menu", "menu")])
        try:
            from config.config import TOP_OPPORTUNITIES_LIMIT

            setups = core.get_top_ranked_opportunities(limit=max(3, int(TOP_OPPORTUNITIES_LIMIT or 10)), refresh=refresh)
            if not setups:
                return (
                    "No ranked opportunities available yet.\n"
                    "That usually means there are no strong current candidates, or the ranking view has not filled yet.",
                    _kb([("🎯 Signals", "signals"), ("🏠 Menu", "menu")]),
                )

            lines = ["*Top Playbook Opportunities*\n"]
            for idx, item in enumerate(setups, start=1):
                opp = float(item.get("opportunity_score", 0.0) or 0.0)
                conf = float(item.get("confidence", 0.0) or 0.0)
                mem = float(item.get("memory_score", 0.0) or 0.0)
                exec_q = float(item.get("execution_quality_score", 0.0) or 0.0)
                broker_q = float(item.get("broker_quality_score", 0.0) or 0.0)
                micro_q = float(item.get("microstructure_score", 0.0) or 0.0)
                broker_state_parts = [
                    str(item.get("broker_agreement_state", "") or "").replace("_", " "),
                    str(item.get("broker_quote_quality_state", "") or "").replace("_", " "),
                    str(item.get("broker_spread_regime", "") or "").replace("_", " "),
                ]
                broker_state = " / ".join([part for part in broker_state_parts if part])
                depth_mode = "Top-book"
                if bool(item.get("depth_available")):
                    depth_mode = "True depth"
                elif bool(item.get("synthetic_depth_available")):
                    depth_mode = "Synthetic depth"
                provider = str(item.get("broker_primary_provider", "") or "")
                cross_asset_peer = str(item.get("cross_asset_primary_peer", "") or "")
                cross_asset_state = str(item.get("cross_asset_state", "") or "").replace("_", " ")
                if cross_asset_peer and cross_asset_state:
                    cross_line = f"\n  Cross-market `{cross_asset_state}` via `{cross_asset_peer}`"
                elif cross_asset_peer:
                    cross_line = f"\n  Cross-market via `{cross_asset_peer}`"
                else:
                    cross_line = ""
                if provider and broker_state:
                    provider_line = f"\n  Provider `{provider}` | {broker_state}"
                elif provider:
                    provider_line = f"\n  Provider `{provider}`"
                elif broker_state:
                    provider_line = f"\n  Broker state `{broker_state}`"
                else:
                    provider_line = ""
                playbook_block = self._format_playbook_runtime_block(item, prefix="  ")
                playbook_line = f"\n{playbook_block}" if playbook_block else ""
                source = "live signal" if item.get("source") == "signal" else "open position"
                lines.append(
                    f"`#{idx}` *{item.get('asset','?')}* {str(item.get('direction','')).upper()}"
                    f"\n  Opportunity `{opp:.3f}` | Confidence `{conf:.0%}`"
                    f"\n  Memory `{mem:.0f}` | Execution `{exec_q:.0f}` | Broker `{broker_q:.2f}`"
                    f"\n  Micro `{micro_q:.2f}` | Depth `{depth_mode}` | {source}"
                    f"{playbook_line}"
                    f"{cross_line}"
                    f"{provider_line}\n"
                )
            return "\n".join(lines), _kb(
                [("⚙️ Reprice Weak", "reprice_weak"), ("📉 Reduce Weak", "reduce_weak")],
                [("📈 Positions", "positions"), ("🏠 Menu", "menu")],
            )
        except Exception as e:
            logger.error(f"[Telegram] top setups error: {e}", exc_info=True)
            return f"❌ Error: {e}", _kb([("🏠 Menu", "menu")])


# ── Helpers ───────────────────────────────────────────────────────────────────

_ALIASES = {
    "BTC": "BTC-USD", "BITCOIN": "BTC-USD",
    "ETH": "ETH-USD", "ETHEREUM": "ETH-USD",
    "BNB": "BNB-USD", "SOL": "SOL-USD",  "SOLANA": "SOL-USD",
    "XRP": "XRP-USD", "ADA": "ADA-USD",  "DOGE": "DOGE-USD",
    "DOT": "DOT-USD", "LTC": "LTC-USD",  "AVAX": "AVAX-USD",
    "LINK":"LINK-USD",
    "GOLD":"XAU/USD", "XAU": "XAU/USD",  "SILVER": "XAG/USD",
    "XAG": "XAG/USD", "EURJPY":"EUR/JPY", "EUROYEN":"EUR/JPY",
    "EURO":"EUR/USD", "EUR": "EUR/USD",   "POUND": "GBP/USD",
    "GBP": "GBP/USD", "YEN": "USD/JPY",  "JPY":   "USD/JPY",
    "SP500":"US500",  "SPX": "US500",    "DOW":   "US30",
    "NASDAQ":"US100", "FTSE":"UK100",    "NIKKEI":"^N225",
    "APPLE":"AAPL",   "GOOGLE":"GOOGL",  "AMAZON":"AMZN",
    "TESLA":"TSLA",   "NVIDIA":"NVDA",   "FACEBOOK":"META",
}

def _resolve_alias(raw: str) -> str:
    return _ALIASES.get(raw.upper().strip(), raw.upper().strip())


def _build_market_text() -> str:
    utc_h = datetime.now(tz=timezone.utc).hour
    dow   = datetime.now(tz=timezone.utc).weekday()
    wd    = dow < 5
    local_now = now_in_display_timezone()

    def _s(open_: bool) -> str:
        return "🟢 Open" if open_ else "🔴 Closed"

    sessions = []
    if wd and (utc_h >= 22 or utc_h < 8):  sessions.append("🌏 Sydney/Tokyo")
    if wd and 7 <= utc_h < 16:             sessions.append("🇬🇧 London")
    if wd and 12 <= utc_h < 21:            sessions.append("🗽 New York")
    if not sessions:                        sessions.append("😴 Off-hours")

    return (
        f"📡 *Market Status* _({display_timezone_label()} {local_now:%H}:xx)_\n"
        f"{'─' * 24}\n"
        f"🪙 Crypto:      {_s(True)}\n"
        f"💱 Forex:       {_s(wd and (utc_h < 21 or utc_h >= 22))}\n"
        f"📈 Stocks:      {_s(wd and 13 <= utc_h < 21)}\n"
        f"📦 Commodities: {_s(wd and 7 <= utc_h < 21)}\n"
        f"📉 Indices:     {_s(wd and 13 <= utc_h < 21)}\n"
        f"\n*Active sessions:*\n" +
        "\n".join(f"  • {s}" for s in sessions)
    )
