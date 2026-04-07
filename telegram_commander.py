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
from telegram.error import NetworkError, RetryAfter, TimedOut
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

from core.assets import registry
from utils.logger import logger

# ── Conversation state ────────────────────────────────────────────────────────
WAITING_ASK_ASSET    = 1
WAITING_ASK_QUESTION = 2

# ── Asset display names ───────────────────────────────────────────────────────
_DISPLAY = {
    "BTC-USD": "₿ BTC",  "ETH-USD": "Ξ ETH",   "BNB-USD": "BNB",
    "XRP-USD": "XRP",    "SOL-USD": "SOL",
    "EUR/USD": "EUR/USD","EUR/JPY": "EUR/JPY",  "GBP/USD": "GBP/USD",  "USD/JPY": "USD/JPY",
    "AUD/USD": "AUD/USD","USD/CAD": "USD/CAD",  "GBP/JPY": "GBP/JPY",
    "XAU/USD": "Gold",   "XAG/USD": "Silver",    "WTI": "WTI Oil",
    "US500":   "S&P 500","US30":  "Dow Jones",  "US100": "Nasdaq",
    "UK100":   "FTSE100",
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
        [("🧠 Ask Robbie","ask"),      (diary_label,     "diary")],
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
    rows.append([("◀️ Back", "ask")])
    return _kb(*rows)

def _ask_question_keyboard(asset: str) -> InlineKeyboardMarkup:
    rows = []
    for i in range(0, len(_ASK_QUESTIONS), 2):
        row = []
        for label, qkey in _ASK_QUESTIONS[i:i+2]:
            row.append((label, f"askq:{asset}:{qkey}"))
        rows.append(row)
    rows.append([("◀️ Back", "ask")])
    return _kb(*rows)


def _bot_menu_commands() -> List[BotCommand]:
    return [
        BotCommand("menu", "Open the control panel"),
        BotCommand("status", "Show system status"),
        BotCommand("positions", "Show open positions"),
        BotCommand("balance", "Show account balance"),
        BotCommand("signal", "Review a signal for an asset"),
        BotCommand("why", "Explain the current signal for an asset"),
        BotCommand("history", "Show recent trade history"),
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

        # All inline button presses
        app.add_handler(CallbackQueryHandler(self._on_button))

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

        outcome = str(review.get("outcome") or "").lower()
        summary = str(review.get("summary") or "").strip()
        lesson = str(review.get("lesson") or "").strip()
        next_focus = str(review.get("next_focus") or "").strip()
        keep = cls._review_items(review.get("keep"))
        avoid = cls._review_items(review.get("avoid"))
        what_right = cls._review_items(review.get("what_went_right"))
        what_wrong = cls._review_items(review.get("what_went_wrong"))

        parts = ["", "🧠 *Trade Review*"]
        if summary:
            parts.append(summary)

        if outcome in {"win", "partial_win"}:
            if what_right:
                parts.append("")
                parts.append("*What was right:*")
                parts.append(what_right)
            if lesson:
                parts.append("")
                parts.append(f"*What I learned:* {lesson}")
            if keep:
                parts.append("")
                parts.append("*What I'll keep:*")
                parts.append(keep)
        else:
            if what_wrong:
                parts.append("")
                parts.append("*What went wrong:*")
                parts.append(what_wrong)
            if lesson:
                parts.append("")
                parts.append(f"*What I learned:* {lesson}")
            if avoid:
                parts.append("")
                parts.append("*What I'll avoid:*")
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

        async def _send():
            await self.application.bot.send_message(
                chat_id=self.chat_id,
                text=text,
                parse_mode=parse_mode,
                reply_markup=reply_markup,
            )

        try:
            future = asyncio.run_coroutine_threadsafe(_send(), self._loop)
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
            msg = str(e).lower()
            if "cannot schedule new futures after shutdown" in msg or "event loop is closed" in msg:
                logger.debug(f"[Telegram] send skipped during shutdown: {e}")
                return False
            if "can't parse entities" in msg or "parse entities" in msg:
                logger.warning(f"[Telegram] parse error: {e}. Retrying without markdown")
                try:
                    async def _send_plain():
                        await self.application.bot.send_message(
                            chat_id=self.chat_id,
                            text=text,
                            parse_mode=None,
                            reply_markup=reply_markup,
                        )
                    future = asyncio.run_coroutine_threadsafe(_send_plain(), self._loop)
                    future.result(timeout=15)
                    return True
                except Exception as e2:
                    logger.error(f"[Telegram] send fallback plain text error: {e2}")
                    return False
            logger.warning(f"[Telegram] network error: {e}")
        except Exception as e:
            msg = str(e).lower()
            if "cannot schedule new futures after shutdown" in msg or "event loop is closed" in msg:
                logger.debug(f"[Telegram] send skipped during shutdown: {e}")
                return False
            if "can't parse entities" in msg or "parse entities" in msg:
                logger.warning(f"[Telegram] parse error: {e}. Retrying without markdown")
                try:
                    async def _send_plain():
                        await self.application.bot.send_message(
                            chat_id=self.chat_id,
                            text=text,
                            parse_mode=None,
                            reply_markup=reply_markup,
                        )
                    future = asyncio.run_coroutine_threadsafe(_send_plain(), self._loop)
                    future.result(timeout=15)
                    return True
                except Exception as e2:
                    logger.error(f"[Telegram] send fallback plain text error: {e2}")
                    return False
            if "unauthorized" not in msg and "401" not in msg:
                logger.error(f"[Telegram] send error ({type(e).__name__}): {e}")
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

    def alert_trade_opened(self, trade: Dict) -> None:
        try:
            from datetime import datetime as _dt, timezone as _tz
            d     = trade.get("direction", trade.get("signal", "BUY"))
            emoji = "🟢" if d == "BUY" else "🔴"
            entry = float(trade.get("entry_price", 0))
            sl    = float(trade.get("stop_loss",   0))
            tp    = float(trade.get("take_profit", 0))
            rr    = abs(tp - entry) / abs(entry - sl) if abs(entry - sl) > 0 else 0
            _a    = trade.get('asset', '?')
            open_raw = trade.get("open_time") or trade.get("entry_time")
            if open_raw:
                try:
                    open_dt = _dt.fromisoformat(str(open_raw).replace("Z", "+00:00"))
                    if open_dt.tzinfo is None:
                        open_dt = open_dt.replace(tzinfo=_tz.utc)
                    else:
                        open_dt = open_dt.astimezone(_tz.utc)
                    opened_at = open_dt.strftime("%d %b %Y %H:%M:%S UTC")
                except Exception:
                    opened_at = _dt.now(_tz.utc).strftime("%d %b %Y %H:%M:%S UTC")
            else:
                opened_at = _dt.now(_tz.utc).strftime("%d %b %Y %H:%M:%S UTC")
            playbook_block = self._format_playbook_runtime_block(trade)
            playbook_text = f"\n🧭 *Playbook*\n{playbook_block}" if playbook_block else ""
            diagnostics_block = self._format_runtime_diagnostics_block(trade)
            diagnostics_text = f"\n🧪 *Diagnostics*\n{diagnostics_block}" if diagnostics_block else ""
            self.send_message(
                f"{emoji} *TRADE OPENED*\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"📌 Asset:    *{_a}*\n"
                f"📍 Direction: *{d}*\n"
                f"🕐 Opened:   `{opened_at}`\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"Entry:    `{self._fmt_price(entry, _a)}`\n"
                f"Stop:     `{self._fmt_price(sl, _a)}`\n"
                f"Target:   `{self._fmt_price(tp, _a)}`\n"
                f"R:R:      `{rr:.1f}:1`\n"
                f"Conf:     `{float(trade.get('confidence', 0)):.0%}`\n"
                f"{playbook_text}"
                f"{diagnostics_text}\n"
                f"ID:       `{trade.get('trade_id', '?')}`"
            )
        except Exception as e:
            logger.error(f"[Telegram] alert_trade_opened: {e}")

    def alert_trade_closed(self, trade: Dict) -> None:
        try:
            from datetime import datetime as _dt, timezone as _tz
            pnl   = float(trade.get("pnl", 0))
            icon  = "✅" if pnl >= 0 else "❌"
            sign  = "+" if pnl >= 0 else ""
            _a2   = trade.get('asset', '?')
            _en   = float(trade.get('entry_price', 0))
            _ex   = float(trade.get('exit_price',  0))
            reason = trade.get('exit_reason', '?')
            # Reason emoji
            r_emoji = {"Take Profit":"🎯","Stop Loss":"🛑","Trailing":"📈","Manual":"👆","Break":"⚖️"}
            r_em = next((v for k, v in r_emoji.items() if k in reason), "📌")
            # Times
            now_str = _dt.now(_tz.utc).strftime("%d %b %Y %H:%M:%S UTC")
            open_str = "—"
            close_str = now_str
            dur_str  = "—"
            try:
                open_t = trade.get("open_time") or trade.get("entry_time")
                exit_t = trade.get("exit_time")
                close_dt = None
                if exit_t:
                    close_dt = _dt.fromisoformat(str(exit_t).replace("Z","+00:00"))
                    if close_dt.tzinfo is None:
                        close_dt = close_dt.replace(tzinfo=_tz.utc)
                    else:
                        close_dt = close_dt.astimezone(_tz.utc)
                    close_str = close_dt.strftime("%d %b %Y %H:%M:%S UTC")
                if open_t:
                    ot = _dt.fromisoformat(str(open_t).replace("Z","+00:00"))
                    if ot.tzinfo is None:
                        ot = ot.replace(tzinfo=_tz.utc)
                    else:
                        ot = ot.astimezone(_tz.utc)
                    open_str = ot.strftime("%d %b %Y %H:%M:%S UTC")
                    duration_minutes = trade.get("duration_minutes")
                    if duration_minutes is not None:
                        mins = max(0, int(float(duration_minutes)))
                    else:
                        ref_close = close_dt or _dt.now(_tz.utc)
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
            playbook_text = f"\n🧭 *Playbook*\n{playbook_block}" if playbook_block else ""
            review_block = self._format_trade_review_block(trade)
            self.send_message(
                f"{icon} *TRADE CLOSED*\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"📌 Asset:    *{_a2}*\n"
                f"{r_em} Reason:   *{reason}*\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"🕐 Opened:   `{open_str}`\n"
                f"🕑 Closed:   `{close_str}`\n"
                f"⏱ Duration: `{dur_str}`\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"Entry:  `{self._fmt_price(_en, _a2)}`\n"
                f"Exit:   `{self._fmt_price(_ex, _a2)}`\n"
                f"P&L:    `{sign}${pnl:.2f}`"
                f"{playbook_text}"
                f"{review_block}"
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
        try:
            core = self.trading_system
            sig  = None
            df   = None
            if core:
                try:
                    sig = core.get_signal_for_asset(asset)
                except Exception:
                    pass
                try:
                    from core.assets import registry
                    cat     = registry.category(asset)
                    fetcher = core.fetcher
                    if fetcher:
                        _TF = "15m"
                        df = fetcher.get_ohlcv(asset, cat, interval=_TF, periods=100)
                        if df is not None and not df.empty:
                            from indicators.technical import TechnicalIndicators
                            df = TechnicalIndicators.add_all_indicators(df)
                except Exception:
                    pass

            from services.personality_service import RobbieExplainer
            explainer = RobbieExplainer()
            answer    = explainer.answer(asset, question, signal=sig, df=df)
            explainer.close()
            return answer
        except Exception as e:
            logger.error(f"[Telegram] /ask error: {e}")
            return f"❌ Robbie hit an error: {e}"

    # ══════════════════════════════════════════════════════════════════════════
    # Inline button router
    # ══════════════════════════════════════════════════════════════════════════

    async def _on_button(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()               # acknowledge tap immediately
        data  = query.data or ""

        # Routing table
        if data == "menu":
            await self._btn_menu(query)
        elif data == "status":
            await self._btn_status(query)
        elif data == "positions":
            await self._btn_positions(query)
        elif data == "balance":
            await self._btn_balance(query)
        elif data == "signals":
            await self._btn_signals(query)
        elif data.startswith("cat:"):
            await self._btn_category(query, data[4:])
        elif data.startswith("sig:"):
            await self._btn_signal(query, data[4:])
        elif data.startswith("why:"):
            await self._btn_why(query, data[4:])
        elif data.startswith("close:"):
            await self._btn_close_confirm(query, data[6:])
        elif data.startswith("close_ok:"):
            await self._btn_close_execute(query, data[9:])
        elif data == "ask":
            await self._btn_ask(query)
        elif data.startswith("askcat:"):
            await self._btn_ask_category(query, data[7:])
        elif data.startswith("askasset:"):
            await self._btn_ask_asset(query, data[9:])
        elif data.startswith("askq:"):
            await self._btn_ask_question(query, data[5:])
        elif data == "mood":
            await self._btn_mood(query)
        elif data == "diary":
            await self._btn_diary(query)
        elif data == "market":
            await self._btn_market(query)
        elif data == "pause":
            await self._btn_pause(query)
        elif data == "resume":
            await self._btn_resume(query)
        elif data == "reprice_weak":
            await self._btn_reprice_weak(query)
        elif data == "reduce_weak":
            await self._btn_reduce_weak(query)
        elif data == "top_setups":
            await self._btn_top_setups(query)
        elif data == "close_menu":
            await self._btn_close_menu(query)
        elif data == "history":
            await self._btn_history(query)
        elif data.startswith("history_filter:"):
            await self._btn_history(query, data[15:])
        elif data.startswith("close_cat:"):
            await self._btn_close_category(query, data[10:])
        elif data == "close_losing":
            await self._btn_close_filter(query, "losing")
        elif data == "close_winning":
            await self._btn_close_filter(query, "winning")
        elif data == "close_all_confirm":
            await self._btn_close_all_confirm(query)
        elif data == "close_all_execute":
            await self._btn_close_all_execute(query)
        else:
            await query.edit_message_text("⚠️ Unknown action.")

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

    async def _btn_ask(self, query) -> None:
        """Entry point — show category picker."""
        await query.edit_message_text(
            "🧠 *Ask Robbie*\n\n"
            "Pick a market category, then an asset, then the kind of answer you want.\n"
            "This explains the existing setup and memory for that asset; it does not run a second trading engine.",
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
            f"_Updated: {datetime.now().strftime('%H:%M:%S')}_"
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
            f"Spillover: {int(signal_diagnostics.get('cross_support_count', 0) or 0)} supportive"
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
        return text.replace("_", " ").strip()

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
            context_parts.append(f"Spillover `{snapshot['cross_state']}` via `{snapshot['cross_peer']}`")
        elif snapshot["cross_peer"]:
            context_parts.append(f"Spillover via `{snapshot['cross_peer']}`")
        if snapshot["recent_pattern_notes"]:
            context_parts.append(f"Pattern `{', '.join(snapshot['recent_pattern_notes'])}`")
        elif snapshot["recent_pattern_block"]:
            context_parts.append("Pattern `recent-pattern block`")
        if context_parts:
            lines.append(f"{prefix}{' | '.join(context_parts)}")

        return "\n".join(lines)

    @classmethod
    def _format_trade_history_context(cls, trade: Dict[str, Any]) -> str:
        review = cls._trade_review(trade)
        playbook_block = cls._format_playbook_runtime_block(trade, prefix="  ")
        lines: List[str] = []
        if playbook_block:
            lines.append(playbook_block)
        if not isinstance(review, dict) or not review:
            return f"{playbook_block}\n" if playbook_block else ""

        entry = review.get("entry_diagnostics") if isinstance(review.get("entry_diagnostics"), dict) else {}
        broker_context = cls._humanise_diagnostic_label(entry.get("broker_context"))
        depth_mode = cls._humanise_diagnostic_label(entry.get("depth_mode"))
        cross_context = cls._humanise_diagnostic_label(entry.get("cross_asset_context"))
        cross_peer = str(entry.get("cross_asset_primary_peer") or "").strip()

        parts = []
        if broker_context:
            parts.append(f"broker {broker_context}")
        if depth_mode:
            parts.append(depth_mode)
        if cross_context and cross_peer:
            parts.append(f"{cross_context} via {cross_peer}")
        elif cross_context:
            parts.append(cross_context)
        if not parts:
            summary = str(review.get("summary") or "").strip()
            if not summary:
                return "\n".join(lines) + ("\n" if lines else "")
            lines.append(f"  🧠 {summary[:90]}{'...' if len(summary) > 90 else ''}")
            return "\n".join(lines) + "\n"
        lines.append(f"  🧠 {' | '.join(parts)}")
        return "\n".join(lines) + "\n"

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
            f"_{datetime.now().strftime('%H:%M:%S')}_"
        )
        return text, _main_menu_keyboard(summary)

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

            # Try live P&L
            pnl_str   = ""
            try:
                fetcher = core.fetcher
                if fetcher:
                    cat = p.get("category", "forex")
                    price, _ = fetcher.get_real_time_price(asset, cat)
                    if price:
                        try:
                            from risk.position_sizer import PositionSizer as _PS
                            pnl = _PS.pnl(asset, cat, entry, price, size, direction)
                        except Exception:
                            pnl = (price - entry) * size if direction == "BUY" else (entry - price) * size
                        pnl_str = f"  P&L: `${pnl:+.2f}`\n"
            except Exception:
                pass

            # Format open time
            open_time_str = ""
            try:
                from datetime import datetime as _dt, timezone as _tz
                ot = p.get("open_time", "")
                if ot:
                    opened = _dt.fromisoformat(ot)
                    if opened.tzinfo is None:
                        opened = opened.replace(tzinfo=_tz.utc)
                    else:
                        opened = opened.astimezone(_tz.utc)
                    elapsed = _dt.now(_tz.utc) - opened
                    mins = int(elapsed.total_seconds() / 60)
                    if mins < 60:
                        duration = f"{mins}m ago"
                    elif mins < 1440:
                        duration = f"{mins//60}h {mins%60}m ago"
                    else:
                        duration = f"{mins//1440}d {(mins%1440)//60}h ago"
                    open_time_str = f"  ⏱ Opened: `{opened.strftime('%b %d %H:%M')} UTC` ({duration})\n"
            except Exception:
                pass

            diagnostics_block = self._format_runtime_diagnostics_block(p, prefix="  ")
            diagnostics_text = f"{diagnostics_block}\n" if diagnostics_block else ""
            playbook_block = self._format_playbook_runtime_block(p, prefix="  ")
            playbook_text = f"{playbook_block}\n" if playbook_block else ""

            lines.append(
                f"{emoji} *{asset}* ({direction})\n"
                f"  Entry: `{self._fmt_price(entry, asset)}` → Current: `{self._fmt_price(float(p.get('current_price', entry)), asset)}`\n"
                f"  Stop:  `{self._fmt_price(sl, asset)}` | Target: `{self._fmt_price(tp, asset)}`\n"
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

    async def _show_history(self, send_fn, filter_cat: str = "all") -> None:
        """Render last 10 closed trades with open/close times and P&L."""
        try:
            from core.state import rollup_closed_trade_history, state as runtime_state

            def _trade_get(trade, key: str, default=None):
                if isinstance(trade, dict):
                    return trade.get(key, default)
                return getattr(trade, key, default)

            def _normalize_trade(trade):
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

            category_filter = filter_cat if filter_cat in ("forex", "crypto", "commodities", "indices") else ""
            pnl_filter = filter_cat if filter_cat in ("won", "lost") else "all"
            raw_trades = [_normalize_trade(t) for t in runtime_state.get_closed_positions(limit=120)]
            trades = rollup_closed_trade_history(raw_trades, limit=30)
            if category_filter:
                trades = [t for t in trades if str(_trade_get(t, "category", "") or "") == category_filter]
            if pnl_filter == "won":
                trades = [t for t in trades if float(_trade_get(t, "pnl", 0) or 0) > 0]
            elif pnl_filter == "lost":
                trades = [t for t in trades if float(_trade_get(t, "pnl", 0) or 0) < 0]

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
            total_pnl = sum(float(_trade_get(t, "pnl", 0) or 0) for t in trades[:10])
            won  = sum(1 for t in trades[:10] if float(_trade_get(t, "pnl", 0) or 0) > 0)
            lost = sum(1 for t in trades[:10] if float(_trade_get(t, "pnl", 0) or 0) < 0)
            lines.append(f"Last {min(10,len(trades))} trades | 🟢 {won} won | 🔴 {lost} lost | Net: ${total_pnl:+.2f}\n")

            for t in trades[:10]:
                pnl  = float(_trade_get(t, "pnl", 0) or 0)
                category = str(_trade_get(t, "category", "") or "")
                em   = cat_emojis.get(category, "📊")
                pnl_em = "🟢" if pnl >= 0 else "🔴"
                dir_  = str(_trade_get(t, "direction", "BUY") or "BUY").upper()

                # Times and duration
                open_t_raw = _trade_get(t, "entry_time") or _trade_get(t, "open_time")
                close_t_raw = _trade_get(t, "exit_time")
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
                dur_str = ""
                duration_minutes = _trade_get(t, "duration_minutes")
                if duration_minutes not in (None, ""):
                    mins = max(0, int(float(duration_minutes)))
                elif open_t and close_t:
                    mins = max(0, int((close_t - open_t).total_seconds() / 60))
                else:
                    mins = None
                if mins is not None:
                    if mins < 60:    dur_str = f"{mins}m"
                    elif mins < 1440: dur_str = f"{mins//60}h {mins%60}m"
                    else:             dur_str = f"{mins//1440}d"

                open_str  = open_t.strftime("%d %b %H:%M")  if open_t  else "—"
                close_str = close_t.strftime("%d %b %H:%M") if close_t else "—"

                # Exit reason emoji
                reason = str(_trade_get(t, "display_exit_reason", _trade_get(t, "exit_reason", "")) or "")
                r_em = next((v for k, v in reason_emojis.items() if k in reason), "📌")
                continuation_summary = str(_trade_get(t, "continuation_summary", "") or "")

                lines.append(
                    f"{em} *{_trade_get(t, 'asset', 'UNKNOWN')}* {dir_}\n"
                    f"  🕐 {open_str} → {close_str} ({dur_str})\n"
                    f"  {r_em} {reason or '—'} | {pnl_em} ${pnl:+.2f}\n"
                )
                if continuation_summary:
                    lines.append(f"  ↪ {continuation_summary}")
                review_context = TelegramCommander._format_trade_history_context(t)
                if review_context:
                    lines.append(review_context)

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

    async def _build_signal(self, asset: str):
        core = self.trading_system
        if not core:
            return "⏳ Engine not ready.", _kb([("🏠 Menu", "menu")])

        try:
            sig = core.get_signal_for_asset(asset)
        except Exception as e:
            return f"❌ Error: {e}", _kb([("◀️ Back", "signals"), ("🏠 Menu", "menu")])

        display = _DISPLAY.get(asset, asset)

        if not sig or sig.get("direction", "HOLD") == "HOLD":
            text = (
                f"⏸ *{display}* — No signal\n\n"
                f"The decision engine doesn't see a clean entry right now.\n"
                f"_Try again in a few minutes._"
            )
            kb = _kb(
                [(f"🧠 Ask Robbie", f"why:{asset}"), ("🔄 Retry", f"sig:{asset}")],
                [("◀️ Back", "signals"), ("🏠 Menu", "menu")],
            )
            return text, kb

        d      = sig.get("direction", "BUY")
        emoji  = "🟢" if d == "BUY" else "🔴"
        entry  = float(sig.get("entry_price", 0))
        sl     = float(sig.get("stop_loss",   0))
        tp     = float(sig.get("take_profit", 0))
        conf   = float(sig.get("confidence",  0))
        rr     = float(sig.get("risk_reward", sig.get("rr_ratio", 0)))
        meta   = sig.get("metadata", {}) or {}
        regime = meta.get("regime", "")
        sess   = meta.get("session_label") or meta.get("playbook_session") or meta.get("session", "")
        playbook_block = self._format_playbook_runtime_block(sig)
        playbook_text = f"\n🧭 *Playbook*\n{playbook_block}\n" if playbook_block else "\n"
        diagnostics_block = self._format_runtime_diagnostics_block(sig)
        diagnostics_text = f"\n🧪 *Diagnostics*\n{diagnostics_block}\n" if diagnostics_block else "\n"

        # TP levels
        tp_levels = sig.get("take_profit_levels", [])
        tp_lines  = ""
        if tp_levels:
            for i, lv in enumerate(tp_levels[:3], 1):
                price = float(lv) if isinstance(lv, (int, float)) else float(lv.get("price", 0))
                tp_lines += f"  TP{i}: `{self._fmt_price(price, asset)}`\n"
        else:
            tp_lines = f"  TP:  `{self._fmt_price(tp, asset)}`\n"

        text = (
            f"{emoji} *{d} {display}*\n"
            f"{'─' * 26}\n"
            f"📍 Entry:  `{self._fmt_price(entry, asset)}`\n"
            f"🛑 Stop:   `{self._fmt_price(sl, asset)}`\n"
            f"{tp_lines}"
            f"\n📊 *Quality*\n"
            f"Confidence: {conf:.0%}\n"
            f"R:R ratio:  {rr:.2f}:1\n"
            f"Regime:     {regime.replace('_', ' ') or '—'}\n"
            f"Session:    {sess or '—'}\n"
            f"{playbook_text}"
            f"{diagnostics_text}"
            f"_Decision engine ✅_"
        )
        kb = _kb(
            [(f"🧠 Why {display}?", f"why:{asset}"), ("🔄 Refresh", f"sig:{asset}")],
            [("◀️ Assets", "signals"), ("🏠 Menu", "menu")],
        )
        return text, kb

    async def _build_why(self, asset: str) -> str:
        core    = self.trading_system
        sig     = None
        df      = None
        display = _DISPLAY.get(asset, asset)

        if core:
            try:
                sig = core.get_signal_for_asset(asset)
            except Exception:
                pass
            try:
                from core.assets import registry
                cat     = registry.category(asset)
                fetcher = core.fetcher
                if fetcher:
                    _TF = "15m"
                    df = fetcher.get_ohlcv(asset, cat, interval=_TF, periods=100)
                    if df is not None and not df.empty:
                        from indicators.technical import TechnicalIndicators
                        df = TechnicalIndicators.add_all_indicators(df)
            except Exception:
                pass

        try:
            from services.personality_service import RobbieExplainer
            explainer = RobbieExplainer()
            text      = explainer.explain_signal(
                asset=asset, df=df, signal=sig or {},
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

        text += (
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
                        f"• *Forex & Commodities* — Sunday 22:00 UTC\n"
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
            setups = core.get_top_ranked_opportunities(limit=5, refresh=refresh)
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
                    cross_line = f"\n  Cross-asset `{cross_asset_state}` via `{cross_asset_peer}`"
                elif cross_asset_peer:
                    cross_line = f"\n  Cross-asset via `{cross_asset_peer}`"
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

    def _s(open_: bool) -> str:
        return "🟢 Open" if open_ else "🔴 Closed"

    sessions = []
    if wd and (utc_h >= 22 or utc_h < 8):  sessions.append("🌏 Sydney/Tokyo")
    if wd and 7 <= utc_h < 16:             sessions.append("🇬🇧 London")
    if wd and 12 <= utc_h < 21:            sessions.append("🗽 New York")
    if not sessions:                        sessions.append("😴 Off-hours")

    return (
        f"📡 *Market Status* _(UTC {utc_h:02d}:xx)_\n"
        f"{'─' * 24}\n"
        f"🪙 Crypto:      {_s(True)}\n"
        f"💱 Forex:       {_s(wd and (utc_h < 21 or utc_h >= 22))}\n"
        f"📈 Stocks:      {_s(wd and 13 <= utc_h < 21)}\n"
        f"📦 Commodities: {_s(wd and 7 <= utc_h < 21)}\n"
        f"📉 Indices:     {_s(wd and 13 <= utc_h < 21)}\n"
        f"\n*Active sessions:*\n" +
        "\n".join(f"  • {s}" for s in sessions)
    )
