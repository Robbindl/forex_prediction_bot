from __future__ import annotations

import asyncio
import threading
import time
from concurrent.futures import TimeoutError as FutureTimeoutError
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
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
    "XAU/USD": "Gold",   "XAG/USD": "Silver",
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

def _main_menu_keyboard() -> InlineKeyboardMarkup:
    return _kb(
        [("📊 Status",    "status"),   ("📈 Positions", "positions")],
        [("💰 Balance",   "balance"),  ("🎯 Signals",   "signals")],
        [("🧠 Ask Robbie","ask"),       ("📔 Diary",     "diary")],
        [("😶 Mood",      "mood"),      ("📡 Market",    "market")],
        [("🧩 Strategies","strategies"),("⏸ Pause",     "pause")],
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
                "🤖 *Robbie is online*\n\nUse /menu to open the control panel.",
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
    def _sanitise_markdown(text: str) -> str:
        """
        Fix common Markdown issues that cause Telegram parse errors.
        Strategy: balance every special character so Telegram never sees
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
            now   = _dt.now(_tz.utc).strftime("%d %b %Y %H:%M:%S UTC")
            self.send_message(
                f"{emoji} *TRADE OPENED*\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"📌 Asset:    *{_a}*\n"
                f"📍 Direction: *{d}*\n"
                f"🕐 Opened:   `{now}`\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"Entry:    `{self._fmt_price(entry, _a)}`\n"
                f"Stop:     `{self._fmt_price(sl, _a)}`\n"
                f"Target:   `{self._fmt_price(tp, _a)}`\n"
                f"R:R:      `{rr:.1f}:1`\n"
                f"Conf:     `{float(trade.get('confidence', 0)):.0%}`\n"
                f"Strategy: `{trade.get('strategy_id', '?')}`\n"
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
            dur_str  = "—"
            try:
                open_t = trade.get("open_time") or trade.get("entry_time")
                if open_t:
                    ot = _dt.fromisoformat(str(open_t).replace("Z","+00:00"))
                    open_str = ot.strftime("%d %b %Y %H:%M:%S UTC")
                    mins = int((_dt.now(_tz.utc) - ot.replace(tzinfo=_tz.utc) if ot.tzinfo is None else _dt.now(_tz.utc) - ot).total_seconds() / 60)
                    if mins < 60:    dur_str = f"{mins}m"
                    elif mins < 1440: dur_str = f"{mins//60}h {mins%60}m"
                    else:             dur_str = f"{mins//1440}d {(mins%1440)//60}h"
            except Exception:
                pass
            self.send_message(
                f"{icon} *TRADE CLOSED*\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"📌 Asset:    *{_a2}*\n"
                f"{r_em} Reason:   *{reason}*\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"🕐 Opened:   `{open_str}`\n"
                f"🕑 Closed:   `{now_str}`\n"
                f"⏱ Duration: `{dur_str}`\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"Entry:  `{self._fmt_price(_en, _a2)}`\n"
                f"Exit:   `{self._fmt_price(_ex, _a2)}`\n"
                f"P&L:    `{sign}${pnl:.2f}`"
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
        core   = self.trading_system
        status = "🟢 Running" if (core and core.is_running) else "🔴 Stopped"
        bal    = f"${core.get_balance():.2f}" if core else "—"
        text   = (
            f"🤖 *Robbie Control Panel*\n\n"
            f"Status: {status}\n"
            f"Balance: {bal}\n"
            f"_{datetime.now().strftime('%H:%M:%S')}_"
        )
        await update.message.reply_text(
            text, parse_mode=ParseMode.MARKDOWN,
            reply_markup=_main_menu_keyboard(),
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
            "🧠 *Ask Robbie*\n\nWhich asset do you want to ask about?\n"
            "_(type the ticker or name, e.g. BTC or GOLD)_",
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
        elif data == "strategies":
            await self._btn_strategies(query)
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
        core   = self.trading_system
        status = "🟢 Running" if (core and core.is_running) else "🔴 Stopped"
        bal    = f"${core.get_balance():.2f}" if core else "—"
        await query.edit_message_text(
            f"🤖 *Robbie Control Panel*\n\n"
            f"Status: {status}\n"
            f"Balance: {bal}\n"
            f"_{datetime.now().strftime('%H:%M:%S')}_",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=_main_menu_keyboard(),
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
            "Pick a market category:",
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

    async def _btn_strategies(self, query) -> None:
        text = self._build_strategies()
        await query.edit_message_text(
            text, parse_mode=ParseMode.MARKDOWN,
            reply_markup=_kb([("🏠 Menu", "menu")]),
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
            f"_Updated: {datetime.now().strftime('%H:%M:%S')}_"
        )
        kb = _kb(
            [("🔄 Refresh", "status"),   ("📈 Positions", "positions")],
            [("💰 Balance",  "balance"),  ("🏠 Menu",      "menu")],
        )
        return text, kb

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
                from datetime import datetime as _dt
                ot = p.get("open_time", "")
                if ot:
                    opened = _dt.fromisoformat(ot)
                    elapsed = _dt.utcnow() - opened
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

            lines.append(
                f"{emoji} *{asset}* ({direction})\n"
                f"  Entry: `{self._fmt_price(entry, asset)}` → Current: `{self._fmt_price(float(p.get('current_price', entry)), asset)}`\n"
                f"  Stop:  `{self._fmt_price(sl, asset)}` | Target: `{self._fmt_price(tp, asset)}`\n"
                f"  Conf: {conf:.0%} | Size: `{p.get('position_size', 0):.4f}`\n"
                f"{pnl_str}"
                f"{open_time_str}"
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
            from services.db_pool import get_db
            category_filter = filter_cat if filter_cat in ("forex", "crypto", "commodities", "indices") else ""
            pnl_filter = filter_cat if filter_cat in ("won", "lost") else "all"
            trades = get_db().get_recent_trades(
                limit=30,
                category=category_filter,
                pnl_filter=pnl_filter,
            )

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
            total_pnl = sum(float(t.pnl or 0) for t in trades[:10])
            won  = sum(1 for t in trades[:10] if (t.pnl or 0) > 0)
            lost = sum(1 for t in trades[:10] if (t.pnl or 0) < 0)
            lines.append(f"Last {min(10,len(trades))} trades | 🟢 {won} won | 🔴 {lost} lost | Net: ${total_pnl:+.2f}\n")

            for t in trades[:10]:
                pnl  = float(t.get("pnl", 0) or 0)
                category = str(t.get("category", "") or "")
                em   = cat_emojis.get(category, "📊")
                pnl_em = "🟢" if pnl >= 0 else "🔴"
                dir_  = str(t.get("direction") or "BUY").upper()

                # Times and duration
                open_t_raw = t.get("entry_time")
                close_t_raw = t.get("exit_time")
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
                if open_t and close_t:
                    mins = int((close_t - open_t).total_seconds() / 60)
                    if mins < 60:    dur_str = f"{mins}m"
                    elif mins < 1440: dur_str = f"{mins//60}h {mins%60}m"
                    else:             dur_str = f"{mins//1440}d"

                open_str  = open_t.strftime("%d %b %H:%M")  if open_t  else "—"
                close_str = close_t.strftime("%d %b %H:%M") if close_t else "—"

                # Exit reason emoji
                reason = str(t.get("exit_reason") or "")
                r_em = next((v for k, v in reason_emojis.items() if k in reason), "📌")

                lines.append(
                    f"{em} *{t.get('asset', 'UNKNOWN')}* {dir_}\n"
                    f"  🕐 {open_str} → {close_str} ({dur_str})\n"
                    f"  {r_em} {reason or '—'} | {pnl_em} ${pnl:+.2f}\n"
                )

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
            [("🧩 Strategies","strategies"),("🏠 Menu",  "menu")],
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
        sess   = meta.get("session", "")

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
            f"Strategy:   {sig.get('strategy_id', '—')}\n\n"
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
        text    = "📔 *Trading Diary*\n\n"

        if moments:
            text += "*Memorable Moments*\n"
            for m in moments:
                icon  = "✅" if m.get("is_win") else "❌"
                pnl   = m.get("pnl", 0)
                ps    = f"+${pnl:.0f}" if pnl >= 0 else f"-${abs(pnl):.0f}"
                text += f"{icon} {m.get('title', '—')} — {ps} _{m.get('date', '')}_\n"
        else:
            text += "_No memorable moments yet — go make some trades! 💪_\n"

        total = report.get("stats", {}).get("total_trades_remembered", 0)
        text += f"\n_Total trades in memory: {total}_"
        return text

    def _build_strategies(self) -> str:
        core = self.trading_system
        if not core:
            return "⏳ Engine not ready."
        try:
            stats = core.get_strategy_stats()
            if not stats:
                return "📊 *Strategy Stats*\n\n_No completed trades yet._"

            lines = ["🧩 *Strategy Performance*\n"]
            for strat, s in sorted(
                stats.items(), key=lambda x: x[1].get("pnl", 0), reverse=True
            )[:10]:
                total  = s.get("wins", 0) + s.get("losses", 0)
                wr     = s.get("wins", 0) / total * 100 if total else 0
                pnl    = s.get("pnl", 0)
                icon   = "🔥" if wr > 60 else "✅" if wr > 50 else "⚠️"
                ps     = f"+${pnl:.2f}" if pnl >= 0 else f"-${abs(pnl):.2f}"
                lines.append(
                    f"{icon} *{strat}*\n"
                    f"   {total} trades | {wr:.0f}% WR | {ps}\n"
                )
            return "\n".join(lines)
        except Exception as e:
            return f"❌ Error: {e}"

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
                    "No ranked opportunities available yet.",
                    _kb([("🎯 Signals", "signals"), ("🏠 Menu", "menu")]),
                )

            lines = ["*Top Ranked Opportunities*\n"]
            for idx, item in enumerate(setups, start=1):
                opp = float(item.get("opportunity_score", 0.0) or 0.0)
                conf = float(item.get("confidence", 0.0) or 0.0)
                mem = float(item.get("memory_score", 0.0) or 0.0)
                exec_q = float(item.get("execution_quality_score", 0.0) or 0.0)
                source = "live signal" if item.get("source") == "signal" else "open position"
                lines.append(
                    f"`#{idx}` *{item.get('asset','?')}* {str(item.get('direction','')).upper()}"
                    f"\n  Opportunity `{opp:.3f}` | Confidence `{conf:.0%}`"
                    f"\n  Memory `{mem:.0f}` | Execution `{exec_q:.0f}` | {source}\n"
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
