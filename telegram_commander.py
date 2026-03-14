"""
telegram_commander.py — Robbie's Telegram interface.

Architecture
────────────
Every interaction flows through inline keyboard buttons.
No wall-of-text command lists — one tap navigates anywhere.

Navigation model
────────────────
/start  → Main Menu (inline keyboard, edits in-place)
Every button press triggers a CallbackQueryHandler that
edits the same message — so the chat stays clean.

Callback data format:  "action"  or  "action:param"
  menu           → main menu
  status         → live status card
  positions      → open positions list
  close:TRADEID  → confirm close
  close_ok:ID    → execute close
  balance        → balance + P&L card
  signals        → category picker
  cat:CATEGORY   → asset picker for category
  sig:ASSET      → run signal through pipeline
  why:ASSET      → Robbie explains signal
  ask            → prompt user to type question
  mood           → Robbie's current mood
  diary          → memorable moments
  pause          → pause trading
  resume         → resume trading
  strategies     → strategy stats
  market         → market hours

Threading
─────────
Bot runs in a daemon thread with its own asyncio loop.
send_message() / alert_*() use run_coroutine_threadsafe()
so any trading thread can send alerts safely.
"""
from __future__ import annotations

import asyncio
import threading
import time
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

from utils.logger import logger

# ── Conversation state ────────────────────────────────────────────────────────
WAITING_ASK_ASSET    = 1
WAITING_ASK_QUESTION = 2

# ── Asset display names ───────────────────────────────────────────────────────
_DISPLAY = {
    "BTC-USD": "₿ BTC",  "ETH-USD": "Ξ ETH",   "BNB-USD": "BNB",
    "XRP-USD": "XRP",    "SOL-USD": "SOL",      "ADA-USD": "ADA",
    "DOGE-USD":"DOGE",   "DOT-USD": "DOT",      "LTC-USD": "LTC",
    "AVAX-USD":"AVAX",   "LINK-USD":"LINK",
    "EUR/USD": "EUR/USD","GBP/USD": "GBP/USD",  "USD/JPY": "USD/JPY",
    "USD/CHF": "USD/CHF","AUD/USD": "AUD/USD",  "USD/CAD": "USD/CAD",
    "NZD/USD": "NZD/USD","EUR/GBP": "EUR/GBP",  "GBP/JPY": "GBP/JPY",
    "AUD/JPY": "AUD/JPY",
    "AAPL":    "Apple",  "MSFT": "Microsoft",   "GOOGL": "Google",
    "AMZN":    "Amazon", "TSLA": "Tesla",        "META":  "Meta",
    "NVDA":    "Nvidia", "JPM":  "JPMorgan",     "V":     "Visa",
    "MA":      "Mastercard",
    "XAU/USD": "Gold",   "XAG/USD": "Silver",   "WTI/USD":"WTI Oil",
    "GC=F":    "Gold F", "SI=F":  "Silver F",   "CL=F":  "Crude F",
    "NG/USD":  "Nat Gas","XCU/USD":"Copper",
    "^GSPC":   "S&P 500","^DJI":  "Dow Jones",  "^IXIC": "Nasdaq",
    "^FTSE":   "FTSE100","^N225": "Nikkei",
}

_CATEGORY_ASSETS: Dict[str, List[str]] = {
    "crypto":      ["BTC-USD","ETH-USD","BNB-USD","XRP-USD","SOL-USD",
                    "ADA-USD","DOGE-USD","DOT-USD","LTC-USD","AVAX-USD","LINK-USD"],
    "forex":       ["EUR/USD","GBP/USD","USD/JPY","USD/CHF","AUD/USD",
                    "USD/CAD","NZD/USD","EUR/GBP","GBP/JPY","AUD/JPY"],
    "stocks":      ["AAPL","MSFT","GOOGL","AMZN","TSLA","META","NVDA","JPM","V","MA"],
    "commodities": ["XAU/USD","XAG/USD","WTI/USD","GC=F","SI=F","CL=F","NG/USD","XCU/USD"],
    "indices":     ["^GSPC","^DJI","^IXIC","^FTSE","^N225"],
}

# ── Keyboard builders ─────────────────────────────────────────────────────────

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
        [("📈 Stocks",      "cat:stocks"),  ("📦 Commodities", "cat:commodities")],
        [("📉 Indices",     "cat:indices")],
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
            self._thread = threading.Thread(
                target=self._run_bot, daemon=True, name="telegram-bot"
            )
            self._thread.start()

            # Wait for loop to be live
            for _ in range(60):
                if self._loop and self._loop.is_running():
                    break
                time.sleep(0.1)

            self.is_running = True
            logger.info("✅ TelegramCommander started")
            self.send_message(
                "🤖 *Robbie is online*\n\nUse /menu to open the control panel.",
            )
        except Exception as e:
            logger.error(f"[Telegram] start error: {e}", exc_info=True)
            self.is_running = False

    def _run_bot(self) -> None:
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_until_complete(self._polling())
        except Exception as e:
            logger.error(f"[Telegram] polling stopped: {e}")
        finally:
            self._loop.close()

    async def _polling(self) -> None:
        async with self.application:
            await self.application.start()
            await self.application.updater.start_polling(
                allowed_updates=Update.ALL_TYPES,
                drop_pending_updates=True,
            )
            while self.is_running:
                await asyncio.sleep(1)
            await self.application.updater.stop()
            await self.application.stop()

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
        app.add_handler(CommandHandler("pause",      self._cmd_pause_direct))
        app.add_handler(CommandHandler("resume",     self._cmd_resume_direct))

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

    def send_message(self, text: str, parse_mode: str = ParseMode.MARKDOWN,
                     reply_markup=None) -> bool:
        if not self._rate_ok():
            return False
        if not self.application or not self._loop or self._loop.is_closed():
            return False

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
        except RetryAfter as e:
            wait = e.retry_after if isinstance(e.retry_after, (int, float)) else 30
            logger.warning(f"[Telegram] flood control — retry in {wait}s")
        except (TimedOut, NetworkError) as e:
            logger.warning(f"[Telegram] network error: {e}")
        except Exception as e:
            if "Unauthorized" not in str(e) and "401" not in str(e):
                logger.error(f"[Telegram] send error: {e}")
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
            d     = trade.get("direction", trade.get("signal", "BUY"))
            emoji = "🟢" if d == "BUY" else "🔴"
            entry = float(trade.get("entry_price", 0))
            sl    = float(trade.get("stop_loss",   0))
            tp    = float(trade.get("take_profit", 0))
            rr    = abs(tp - entry) / abs(entry - sl) if abs(entry - sl) > 0 else 0
            self.send_message(
                f"{emoji} *Trade Opened*\n\n"
                f"Asset:    {trade.get('asset', '?')}\n"
                f"Entry:    `{entry:.5f}`\n"
                f"Stop:     `{sl:.5f}`\n"
                f"Target:   `{tp:.5f}`\n"
                f"R:R:      {rr:.1f}:1\n"
                f"Conf:     {float(trade.get('confidence', 0)):.0%}\n"
                f"Strategy: {trade.get('strategy_id', '?')}\n"
                f"ID:       `{trade.get('trade_id', '?')}`"
            )
        except Exception as e:
            logger.error(f"[Telegram] alert_trade_opened: {e}")

    def alert_trade_closed(self, trade: Dict) -> None:
        try:
            pnl   = float(trade.get("pnl", 0))
            icon  = "✅" if pnl >= 0 else "❌"
            sign  = "+" if pnl >= 0 else ""
            self.send_message(
                f"{icon} *Trade Closed*\n\n"
                f"Asset:  {trade.get('asset', '?')}\n"
                f"P&L:    `{sign}${pnl:.2f}`\n"
                f"Entry:  `{float(trade.get('entry_price', 0)):.5f}`\n"
                f"Exit:   `{float(trade.get('exit_price',  0)):.5f}`\n"
                f"Reason: {trade.get('exit_reason', '?')}"
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
                        df = fetcher.get_ohlcv(asset, cat, interval="1h", periods=50)
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
            f"⏳ Scanning {asset} through the 7-layer pipeline…",
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
            f"Entry:  `{entry:.5f}`\n"
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
        await query.edit_message_text(
            "🧠 *Ask Robbie*\n\n"
            "Type your question in the format:\n"
            "`/ask <asset> <question>`\n\n"
            "Examples:\n"
            "• `/ask BTC should I buy?`\n"
            "• `/ask GOLD what do you remember?`\n"
            "• `/ask EUR/USD explain the risk`\n"
            "• `/ask ETH how confident are you?`",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=_kb([("🏠 Menu", "menu")]),
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
                        pnl = (price - entry) * size if direction == "BUY" else (entry - price) * size
                        pnl_str = f"  P&L: `${pnl:+.2f}`\n"
            except Exception:
                pass

            lines.append(
                f"{emoji} *{asset}* ({direction})\n"
                f"  Entry: `{entry:.5f}` | Stop: `{sl:.5f}`\n"
                f"  TP:    `{tp:.5f}` | Conf: {conf:.0%}\n"
                f"{pnl_str}"
                f"  ID: `{tid}`\n"
            )
            buttons.append([(f"❌ Close {asset}", f"close:{tid}")])

        if len(positions) > 8:
            lines.append(f"_…and {len(positions) - 8} more_")

        buttons.append([("🔄 Refresh", "positions"), ("🏠 Menu", "menu")])
        return "\n".join(lines), _kb(*buttons)

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
                f"The 7-layer pipeline doesn't see a clean entry right now.\n"
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
                tp_lines += f"  TP{i}: `{price:.5f}`\n"
        else:
            tp_lines = f"  TP:  `{tp:.5f}`\n"

        text = (
            f"{emoji} *{d} {display}*\n"
            f"{'─' * 26}\n"
            f"📍 Entry:  `{entry:.5f}`\n"
            f"🛑 Stop:   `{sl:.5f}`\n"
            f"{tp_lines}"
            f"\n📊 *Quality*\n"
            f"Confidence: {conf:.0%}\n"
            f"R:R ratio:  {rr:.2f}:1\n"
            f"Regime:     {regime.replace('_', ' ') or '—'}\n"
            f"Session:    {sess or '—'}\n"
            f"Strategy:   {sig.get('strategy_id', '—')}\n\n"
            f"_7-layer pipeline ✅_"
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
                    df = fetcher.get_ohlcv(asset, cat, interval="1h", periods=50)
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
            from services.personality_service import PersonalityDatabase
            db     = PersonalityDatabase()
            report = db.get_personality_report()
            db.close()
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

    def _do_close(self, trade_id: str):
        core = self.trading_system
        if not core:
            return "⏳ Engine not ready.", _kb([("🏠 Menu", "menu")])
        try:
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
                f"Entry:   `{float(result.get('entry_price', 0)):.5f}`\n"
                f"Exit:    `{float(result.get('exit_price', 0)):.5f}`\n"
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


# ── Helpers ───────────────────────────────────────────────────────────────────

_ALIASES = {
    "BTC": "BTC-USD", "BITCOIN": "BTC-USD",
    "ETH": "ETH-USD", "ETHEREUM": "ETH-USD",
    "BNB": "BNB-USD", "SOL": "SOL-USD",  "SOLANA": "SOL-USD",
    "XRP": "XRP-USD", "ADA": "ADA-USD",  "DOGE": "DOGE-USD",
    "DOT": "DOT-USD", "LTC": "LTC-USD",  "AVAX": "AVAX-USD",
    "LINK":"LINK-USD",
    "GOLD":"XAU/USD", "XAU": "XAU/USD",  "SILVER": "XAG/USD",
    "XAG": "XAG/USD", "OIL": "WTI/USD", "WTI": "WTI/USD",
    "EURO":"EUR/USD", "EUR": "EUR/USD",   "POUND": "GBP/USD",
    "GBP": "GBP/USD", "YEN": "USD/JPY",  "JPY":   "USD/JPY",
    "SP500":"^GSPC",  "SPX": "^GSPC",    "DOW":   "^DJI",
    "NASDAQ":"^IXIC", "FTSE":"^FTSE",    "NIKKEI":"^N225",
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