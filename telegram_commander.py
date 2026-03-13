"""
Telegram Bot Commander — Fixed for python-telegram-bot v20+
All commands wired through the full 7-layer quality pipeline.

Root causes of previous crashes fixed:
  1. asyncio event loop conflict — run_polling() and send_message() were
     fighting over the same loop from different threads. Fixed with
     run_coroutine_threadsafe() + a dedicated per-thread event loop.
  2. Logger overwrite — 'from logger import logger' was immediately
     clobbered by 'logger = logging.getLogger(...)'. Removed the clobber.
  3. /signal bypassed the quality pipeline — now uses get_instant_signal().
  4. cmd_market had no _require_system guard — could crash on startup.
"""

import asyncio
import os
import threading
import time
from datetime import datetime, timedelta
from typing import List, Optional

from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
from telegram.error import NetworkError, TimedOut, RetryAfter

from logger import logger   # ← centralized logger, NOT overwritten below


# ── Asset alias map (shared by /signal and /why) ──────────────────────────────
ALIASES = {
    # Crypto
    'BITCOIN':'BTC-USD','BTC':'BTC-USD',
    'ETHEREUM':'ETH-USD','ETH':'ETH-USD',
    'BINANCE':'BNB-USD','BNB':'BNB-USD',
    'SOLANA':'SOL-USD','SOL':'SOL-USD',
    'XRP':'XRP-USD','RIPPLE':'XRP-USD',
    'CARDANO':'ADA-USD','ADA':'ADA-USD',
    'DOGECOIN':'DOGE-USD','DOGE':'DOGE-USD',
    'POLKADOT':'DOT-USD','DOT':'DOT-USD',
    'LITECOIN':'LTC-USD','LTC':'LTC-USD',
    'AVALANCHE':'AVAX-USD','AVAX':'AVAX-USD',
    'CHAINLINK':'LINK-USD','LINK':'LINK-USD',
    # Commodities
    'GOLD':'XAU/USD','XAU':'XAU/USD',
    'SILVER':'XAG/USD','XAG':'XAG/USD',
    'OIL':'CL=F','WTI':'CL=F','CRUDE':'CL=F',
    'NATURAL GAS':'NG=F','GAS':'NG=F',
    'COPPER':'HG=F','CU':'HG=F',
    # Forex
    'EURO':'EUR/USD','EUR':'EUR/USD',
    'POUND':'GBP/USD','GBP':'GBP/USD',
    'YEN':'USD/JPY','JPY':'USD/JPY',
    'AUD':'AUD/USD','AUSSIE':'AUD/USD',
    'CAD':'USD/CAD','LOONIE':'USD/CAD',
    'CHF':'USD/CHF','SWISS':'USD/CHF',
    'NZD':'NZD/USD','KIWI':'NZD/USD',
    'EURGBP':'EUR/GBP','EURJPY':'EUR/JPY',
    'GBPJPY':'GBP/JPY','AUDJPY':'AUD/JPY',
    'EURAUD':'EUR/AUD','GBPAUD':'GBP/AUD',
    # Indices
    'SP500':'^GSPC','S&P':'^GSPC','SPX':'^GSPC',
    'DOW':'^DJI','DJI':'^DJI',
    'NASDAQ':'^IXIC','IXIC':'^IXIC',
    'FTSE':'^FTSE','UK100':'^FTSE',
    'NIKKEI':'^N225','N225':'^N225',
    'HANG SENG':'^HSI','HSI':'^HSI',
    'DAX':'^GDAXI','GDAXI':'^GDAXI',
    'VIX':'^VIX','FEAR':'^VIX',
    # Stocks
    'APPLE':'AAPL','MICROSOFT':'MSFT',
    'GOOGLE':'GOOGL','GOOG':'GOOGL',
    'AMAZON':'AMZN','TESLA':'TSLA',
    'NVIDIA':'NVDA','FACEBOOK':'META',
    'JPMORGAN':'JPM','VISA':'V',
    'MASTERCARD':'MA','JOHNSON':'JNJ',
    'PFIZER':'PFE','WALMART':'WMT',
    'PROCTER':'PG','COCA COLA':'KO',
    'EXXON':'XOM','CHEVRON':'CVX',
}

# ── Category lookup for signal_learning ───────────────────────────────────────
def _get_category(asset: str) -> str:
    if 'USD' in asset and '-' in asset: return 'crypto'
    if any(x in asset for x in ['=F', 'XAU', 'XAG', 'WTI', 'NG/', 'HG']): return 'commodities'
    if '/' in asset and 'USD' in asset: return 'forex'
    if asset.startswith('^'): return 'indices'
    return 'stocks'


# ── Rate limiter ──────────────────────────────────────────────────────────────
class RateLimiter:
    def __init__(self, max_per_minute: int = 10):
        self.max_per_minute = max_per_minute
        self.requests: List[datetime] = []

    def can_send(self) -> bool:
        now = datetime.now()
        self.requests = [t for t in self.requests if now - t < timedelta(minutes=1)]
        if len(self.requests) < self.max_per_minute:
            self.requests.append(now)
            return True
        return False


# ── TelegramCommander ─────────────────────────────────────────────────────────
class TelegramCommander:
    """
    Commands:
    /start         — Welcome
    /help          — All commands
    /status        — Bot status + performance
    /positions     — Open trades
    /balance       — Account balance
    /performance   — Full P&L metrics
    /strategies    — Strategy weights
    /market        — Market hours
    /signal <asset>— Quality signal through 7-layer pipeline
    /why <asset>   — Human explanation
    /mood          — Bot's current mood
    /diary         — Trading diary
    /pause         — Stop trading
    /resume        — Resume trading
    /close <id>    — Close a trade
    """

    def __init__(self, token: str, chat_id: str, trading_system):
        self.token          = token
        self.chat_id        = str(chat_id)
        self.trading_system = trading_system
        self.application    = None
        self.is_running     = False
        self.rate_limiter   = RateLimiter(max_per_minute=20)
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        logger.info(f"TelegramCommander initialised for chat {chat_id}")

    # ── Startup ───────────────────────────────────────────────────────────────

    def start(self):
        """Start the bot in a background thread with its own event loop."""
        try:
            self.application = (
                Application.builder()
                .token(self.token)
                .connect_timeout(30)
                .read_timeout(30)
                .write_timeout(30)
                .build()
            )
            # Register all command handlers
            for cmd, handler in [
                ("start",       self.cmd_start),
                ("help",        self.cmd_help),
                ("status",      self.cmd_status),
                ("positions",   self.cmd_positions),
                ("balance",     self.cmd_balance),
                ("performance", self.cmd_performance),
                ("strategies",  self.cmd_strategies),
                ("market",      self.cmd_market),
                ("signal",      self.cmd_signal),
                ("why",         self.cmd_why),
                ("mood",        self.cmd_mood),
                ("diary",       self.cmd_diary),
                ("pause",       self.cmd_pause),
                ("resume",      self.cmd_resume),
                ("close",       self.cmd_close),
            ]:
                self.application.add_handler(CommandHandler(cmd, handler))

            self._thread = threading.Thread(
                target=self._run_bot, daemon=True, name="telegram-bot"
            )
            self._thread.start()

            # Wait up to 5s for loop to be ready, then send startup message
            for _ in range(50):
                if self._loop and self._loop.is_running():
                    break
                time.sleep(0.1)

            self.is_running = True
            logger.info("✅ TelegramCommander started")
            self.send_message(
                "🤖 *Commander Active*\n\n"
                "All commands ready. Use /help to see them.\n"
                "Status: 🟢 Running"
            )
        except Exception as e:
            logger.error(f"TelegramCommander start error: {e}", exc_info=True)
            self.is_running = False

    def _run_bot(self):
        """
        Dedicated thread with its own event loop.
        This is the ONLY correct way to run python-telegram-bot v20+
        in a background thread — all async send_message calls use
        run_coroutine_threadsafe() to schedule onto this same loop.
        """
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_until_complete(self._polling())
        except Exception as e:
            logger.error(f"Telegram polling stopped: {e}")
        finally:
            self._loop.close()

    async def _polling(self):
        async with self.application:
            await self.application.start()
            await self.application.updater.start_polling(
                allowed_updates=Update.ALL_TYPES,
                drop_pending_updates=True,   # ignore stale commands on restart
            )
            # Keep running until stop() is called
            while self.is_running:
                await asyncio.sleep(1)
            await self.application.updater.stop()
            await self.application.stop()

    # ── send_message (thread-safe, works from ANY thread) ─────────────────────

    def send_message(self, text: str, parse_mode: str = 'Markdown') -> bool:
        """
        Thread-safe send. Uses run_coroutine_threadsafe() to schedule
        the coroutine onto the bot's dedicated event loop — no loop
        conflicts, no RuntimeError, works from trading threads.
        """
        if not self.rate_limiter.can_send():
            logger.warning("Telegram rate limit — message skipped")
            return False

        if not self.application or not self._loop or self._loop.is_closed():
            logger.warning("Telegram: bot not ready, message skipped")
            return False

        async def _send():
            await self.application.bot.send_message(
                chat_id=self.chat_id,
                text=text,
                parse_mode=parse_mode,
            )

        try:
            future = asyncio.run_coroutine_threadsafe(_send(), self._loop)
            future.result(timeout=15)
            return True
        except RetryAfter as e:
            # retry_after can be None in some lib versions — guard it
            wait = e.retry_after if isinstance(e.retry_after, (int, float)) else 30
            logger.warning(f"Telegram flood control: retry after {wait}s")
        except (TimedOut, NetworkError) as e:
            logger.warning(f"Telegram network error: {e}")
        except TypeError as e:
            # Catches "'>=' not supported between NoneType and int" from lib internals
            logger.warning(f"Telegram internal type error (ignored): {e}")
        except Exception as e:
            err_str = str(e)
            if "Unauthorized" in err_str or "401" in err_str:
                logger.warning("Telegram: bot token invalid or chat_id wrong — alerts disabled until restart")
            else:
                logger.error(f"Telegram send_message error: {e}")
        return False

    def stop(self):
        self.is_running = False
        logger.info("TelegramCommander stopped")
        try:
            self.send_message("🛑 Trading bot offline")
        except Exception:
            pass

    # ── Guard helper ──────────────────────────────────────────────────────────

    async def _require_system(self, update) -> bool:
        if self.trading_system is None:
            await update.message.reply_text(
                "⏳ Trading system still initialising.\n"
                "Wait ~30s after the dashboard loads, then try again."
            )
            return False
        return True

    # ── /start ────────────────────────────────────────────────────────────────

    async def cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        status = "🟢 Running" if (self.trading_system and self.trading_system.is_running) else "⏳ Initialising"
        await update.message.reply_text(
            "🤖 *Trading Bot Commander*\n\n"
            "I control your Ultimate Trading System.\n"
            "Use /help to see all commands.\n\n"
            f"Status: {status}",
            parse_mode='Markdown'
        )

    # ── /help ─────────────────────────────────────────────────────────────────

    async def cmd_help(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        status = "🟢 Running" if (self.trading_system and self.trading_system.is_running) else "🔴 Stopped"
        await update.message.reply_text(
            "🤖 *Commander Help*\n\n"
            "🔍 *Signals*\n"
            "• `/signal BTC` — Quality signal (7-layer)\n"
            "• `/signal GOLD` — Gold signal\n"
            "• `/signal EUR/USD` — Forex signal\n"
            "• `/why BTC` — Human reasoning\n\n"
            "📊 *Trading*\n"
            "• `/status` — Full bot status\n"
            "• `/positions` — Open trades\n"
            "• `/balance` — Account balance\n"
            "• `/performance` — P&L metrics\n"
            "• `/strategies` — Strategy weights\n"
            "• `/market` — Market hours\n"
            "• `/close <id>` — Close a trade\n"
            "• `/pause` — Stop trading\n"
            "• `/resume` — Resume trading\n\n"
            "🧠 *Bot Brain*\n"
            "• `/mood` — My current mood\n"
            "• `/diary` — Trading diary\n\n"
            f"*Status:* {status}",
            parse_mode='Markdown'
        )

    # ── /status ───────────────────────────────────────────────────────────────

    async def cmd_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await self._require_system(update): return
        try:
            perf          = self.trading_system.paper_trader.get_performance()
            open_pos      = len(self.trading_system.paper_trader.get_open_positions())
            market_status = "🟢 RUNNING" if self.trading_system.is_running else "🔴 STOPPED"
            today         = datetime.now().strftime('%Y-%m-%d %H:%M')
            daily_trades  = self._get_daily_trades()

            msg = (
                f"📊 *Bot Status — {today}*\n\n"
                f"Status: {market_status}\n"
                f"Mode: {self.trading_system.strategy_mode.upper()}\n\n"
                f"📈 *Performance*\n"
                f"Open Positions: {open_pos}\n"
                f"Today's Trades: {daily_trades}\n"
                f"Total Trades:   {perf['total_trades']}\n"
                f"Win Rate:       {perf['win_rate']}%\n"
                f"Total P&L:      ${perf['total_pnl']:.2f}\n"
                f"Balance:        ${perf['current_balance']:.2f}\n"
            )
            await update.message.reply_text(msg, parse_mode='Markdown')
        except Exception as e:
            logger.error(f"/status error: {e}")
            await update.message.reply_text(f"❌ Error: {e}")

    # ── /positions ────────────────────────────────────────────────────────────

    async def cmd_positions(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await self._require_system(update): return
        try:
            # PHASE 2: read from TradingCore.state if available
            core = getattr(self.trading_system, '_trading_core', None)
            if core is not None:
                positions = core.state.get_open_positions()
            else:
                positions = self.trading_system.paper_trader.get_open_positions()

            if not positions:
                await update.message.reply_text("📭 No open positions")
                return

            msg = f"📈 *Open Positions ({len(positions)})*\n\n"
            for i, p in enumerate(positions[:5], 1):
                unrealized = 0
                try:
                    price, _ = self.trading_system.fetcher.get_real_time_price(
                        p['asset'], p.get('category', 'unknown')
                    )
                    if price:
                        if p['signal'] == 'BUY':
                            unrealized = (price - p['entry_price']) * p['position_size']
                        else:
                            unrealized = (p['entry_price'] - price) * p['position_size']
                except Exception:
                    pass

                emoji = "🟢" if p['signal'] == 'BUY' else "🔴"
                pnl_emoji = "📈" if unrealized >= 0 else "📉"
                msg += (
                    f"{emoji} *{p['asset']}* ({p['signal']})\n"
                    f"Entry: `{p['entry_price']:.5f}`\n"
                    f"Stop:  `{p['stop_loss']:.5f}`\n"
                    f"Unrealized: {pnl_emoji} ${unrealized:.2f}\n"
                    f"ID: `{p['trade_id']}`\n\n"
                )
            if len(positions) > 5:
                msg += f"_...and {len(positions)-5} more_"
            await update.message.reply_text(msg, parse_mode='Markdown')
        except Exception as e:
            logger.error(f"/positions error: {e}")
            await update.message.reply_text(f"❌ Error: {e}")

    # ── /balance ──────────────────────────────────────────────────────────────

    async def cmd_balance(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await self._require_system(update): return
        try:
            # PHASE 2: read from TradingCore.state if available
            core = getattr(self.trading_system, '_trading_core', None)
            if core is not None:
                perf  = core.state.get_performance()
                pnl   = perf['total_pnl']
                emoji = "📈" if pnl >= 0 else "📉"
                sign  = "+" if pnl >= 0 else ""
                await update.message.reply_text(
                    f"💰 *Account Balance*\n\n"
                    f"Current:       ${perf['balance']:.2f}\n"
                    f"Total P&L:     {emoji} {sign}${pnl:.2f}\n"
                    f"Win Rate:      {perf['win_rate']:.1f}%\n"
                    f"Open Pos:      {perf['open_positions']}\n"
                    f"Daily Trades:  {core.state.daily_trades}",
                    parse_mode='Markdown'
                )
                return
            # Fallback
            perf   = self.trading_system.paper_trader.get_performance()
            pnl    = perf['total_pnl']
            emoji  = "📈" if pnl >= 0 else "📉"
            sign   = "+" if pnl >= 0 else ""
            await update.message.reply_text(
                f"💰 *Account Balance*\n\n"
                f"Current:       ${perf['current_balance']:.2f}\n"
                f"Total P&L:     {emoji} {sign}${pnl:.2f}\n"
                f"Win Rate:      {perf['win_rate']}%\n"
                f"Open Pos:      {perf['open_positions']}",
                parse_mode='Markdown'
            )
        except Exception as e:
            await update.message.reply_text(f"❌ Error: {e}")

    # ── /performance ──────────────────────────────────────────────────────────

    async def cmd_performance(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await self._require_system(update): return
        try:
            # PHASE 2: read from TradingCore.state if available
            core = getattr(self.trading_system, '_trading_core', None)
            if core is not None:
                perf = core.state.get_performance()
                total = perf['total_trades']
                wins  = perf['winning_trades']
                losses= perf['losing_trades']
                wr    = perf['win_rate']
                avg_w = perf['avg_win']
                avg_l = perf['avg_loss']
            else:
                perf   = self.trading_system.paper_trader.get_performance()
                total  = perf['total_trades']
                wins   = perf.get('winning_trades', 0)
                losses = perf.get('losing_trades', 0)
                wr     = (wins / total * 100) if total > 0 else 0
                avg_w  = perf.get('avg_win', 0)
                avg_l  = perf.get('avg_loss', 0)

            rr  = abs(avg_w / avg_l) if avg_l else 0
            exp = (wr/100 * avg_w) - ((1 - wr/100) * avg_l)

            await update.message.reply_text(
                f"💰 *Performance Report*\n\n"
                f"Total Trades:   {total}\n"
                f"Wins:           {wins}\n"
                f"Losses:         {losses}\n"
                f"Win Rate:       {wr:.1f}%\n"
                f"Avg Win:        ${avg_w:.2f}\n"
                f"Avg Loss:       ${avg_l:.2f}\n"
                f"Risk/Reward:    {rr:.2f}\n"
                f"Expectancy:     ${exp:.2f}\n"
                f"Profit Factor:  {perf.get('profit_factor', 0):.2f}\n"
                f"Total P&L:      ${perf.get('total_pnl', perf.get('total_pnl', 0)):.2f}\n"
                f"Balance:        ${perf.get('balance', perf.get('current_balance', 0)):.2f}",
                parse_mode='Markdown'
            )
        except Exception as e:
            await update.message.reply_text(f"❌ Error: {e}")

    # ── /strategies ───────────────────────────────────────────────────────────

    async def cmd_strategies(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await self._require_system(update): return
        try:
            if not hasattr(self.trading_system, 'voting_engine'):
                await update.message.reply_text("❌ Voting engine not available")
                return
            weights = self.trading_system.voting_engine.strategy_weights
            perfs   = self.trading_system.voting_engine.strategy_performance
            top     = sorted(weights.items(), key=lambda x: x[1], reverse=True)[:10]
            msg     = "🧠 *Active Strategies*\n\n"
            for strat, w in top:
                p      = perfs.get(strat, {})
                trades = p.get('trades', 0)
                wr     = p.get('win_rate', 0) * 100 if trades > 0 else 0
                icon   = "🔥" if w > 1.5 else "⚡" if w > 1.0 else "✅" if w > 0.5 else "⚠️"
                msg   += f"{icon} *{strat}* — {w:.2f}x"
                if trades > 0:
                    msg += f" | {trades} trades | {wr:.0f}% WR"
                msg += "\n"
            await update.message.reply_text(msg, parse_mode='Markdown')
        except Exception as e:
            await update.message.reply_text(f"❌ Error: {e}")

    # ── /market ───────────────────────────────────────────────────────────────

    async def cmd_market(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        # NOTE: market status doesn't need trading_system to be fully ready
        try:
            status = {}
            if self.trading_system and hasattr(self.trading_system, 'fetcher'):
                try:
                    status = self.trading_system.fetcher.get_market_status()
                except Exception:
                    pass

            def _o(key): return "🟢 OPEN" if status.get(key, False) else "🔴 CLOSED"
            await update.message.reply_text(
                f"📊 *Market Status*\n\n"
                f"Crypto:      🟢 OPEN (24/7)\n"
                f"Forex:       {_o('forex')}\n"
                f"Stocks:      {_o('stocks')}\n"
                f"Commodities: {_o('commodities')}\n"
                f"Indices:     {_o('indices')}\n\n"
                f"NY Time:     {status.get('ny_time', 'unknown')}\n"
                f"EAT Time:    {datetime.now().strftime('%H:%M')}",
                parse_mode='Markdown'
            )
        except Exception as e:
            await update.message.reply_text(f"❌ Error: {e}")

    # ── /signal ───────────────────────────────────────────────────────────────

    async def cmd_signal(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await self._require_system(update): return
        if not context.args:
            await update.message.reply_text(
                "Usage: `/signal BTC` or `/signal GOLD` or `/signal EUR/USD`",
                parse_mode='Markdown'
            )
            return
        try:
            raw   = " ".join(context.args).upper().replace('"', '')
            asset = ALIASES.get(raw, raw)
            cat   = _get_category(asset)

            await update.message.chat.send_action("typing")
            searching = await update.message.reply_text(f"🔍 Building quality signal for *{asset}*…", parse_mode='Markdown')

            # ── Run through the FULL 7-layer quality pipeline ──────────────
            sig = None
            try:
                from signal_learning import get_instant_signal
                sig = get_instant_signal(asset, cat, self.trading_system)
            except Exception as e:
                logger.warning(f"signal_learning failed for {asset}: {e}")

            await searching.delete()

            # signal_learning returns 'direction' key; check both for compatibility
            _sig_dir = sig.get('direction') or sig.get('signal', 'HOLD')
            if not sig or _sig_dir == 'HOLD':
                await update.message.reply_text(
                    f"⏸ *{asset}* — No quality signal right now\n\n"
                    f"_Passed quality filter but no clear direction.\n"
                    f"Try again in a few minutes or check another asset._",
                    parse_mode='Markdown'
                )
                return

            # Build rich reply
            _dir    = _sig_dir
            _emoji  = '🟢' if _dir == 'BUY' else '🔴'
            _entry  = sig.get('entry_price', 0)
            _sl     = sig.get('stop_loss', 0)
            _tp     = sig.get('take_profit', 0)
            _tp2    = sig.get('take_profit_2')
            _tp3    = sig.get('take_profit_3')
            _conf   = sig.get('confidence', 0)
            _rr     = sig.get('rr_ratio', 0)
            _wr     = sig.get('win_rate', 0)
            _conf_str = sig.get('confluence', '')
            _bias   = sig.get('learning_bias', 0)
            _sess   = sig.get('session', '')
            _reason = sig.get('reasoning', '')

            _conf_badge = {
                'ALL3': '🔥 ALL 3 TF AGREE',
                'BOTH': '✅ 2/3 TF AGREE',
                '2OF3': '✅ 2/3 TF AGREE',
                'DIVERGE': '⚠️ DIVERGING',
            }.get(_conf_str, _conf_str or '—')

            _tp_lines = f"TP1: `{_tp:.5f}`"
            if _tp2: _tp_lines += f"\nTP2: `{_tp2:.5f}`"
            if _tp3: _tp_lines += f"\nTP3: `{_tp3:.5f}`"

            msg = (
                f"{_emoji} *{_dir} {asset}*\n"
                f"{'─'*28}\n"
                f"📍 Entry: `{_entry:.5f}`\n"
                f"🛑 Stop:  `{_sl:.5f}`\n"
                f"{_tp_lines}\n\n"
                f"📊 *Quality*\n"
                f"Confidence:  {_conf:.0%}\n"
                f"R:R:         {_rr:.2f}:1\n"
                f"Confluence:  {_conf_badge}\n"
                f"Win Rate:    {_wr:.0f}% (learned)\n"
                f"Bias:        {_bias:+.3f}\n"
                f"Session:     {_sess}\n"
            )
            if _reason:
                # First 2 reasons only (keep it readable on phone)
                lines = [l for l in _reason.split('\n') if l.strip()][:2]
                if lines:
                    msg += f"\n🧠 *Why*\n" + "\n".join(lines) + "\n"
            msg += f"{'─'*28}\n_7-layer quality filter passed_"

            await update.message.reply_text(msg, parse_mode='Markdown')

        except Exception as e:
            logger.error(f"/signal error: {e}", exc_info=True)
            await update.message.reply_text(f"❌ Error: {e}")

    # ── /why ──────────────────────────────────────────────────────────────────

    async def cmd_why(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await self._require_system(update): return
        if not context.args:
            await update.message.reply_text("Usage: `/why BTC`", parse_mode='Markdown')
            return
        try:
            raw   = " ".join(context.args).upper().replace('"', '')
            asset = ALIASES.get(raw, raw)

            await update.message.chat.send_action("typing")
            thinking = await update.message.reply_text(f"🤔 Thinking about {asset}…")

            df = self.trading_system.fetch_historical_data(asset, days=3, interval='15m')
            if df is None or df.empty:
                await thinking.edit_text(f"❌ No data for {asset}")
                return
            df         = self.trading_system.add_technical_indicators(df)
            prediction = self.trading_system.predictor.predict_next(df)

            sentiment, news = {}, []
            if hasattr(self.trading_system, 'sentiment_analyzer'):
                try:
                    sentiment = self.trading_system.sentiment_analyzer.get_comprehensive_sentiment('crypto')
                except Exception:
                    pass

            from human_explainer_db import DatabaseExplainer
            explainer   = DatabaseExplainer(self.trading_system)
            explanation = explainer.explain_signal(
                asset, df, prediction, sentiment, news,
                chat_id=str(update.effective_chat.id)
            )
            explainer.close()
            await thinking.delete()

            chunks = [explanation[i:i+4000] for i in range(0, len(explanation), 4000)]
            for chunk in chunks:
                await update.message.reply_text(chunk, parse_mode='Markdown')
        except Exception as e:
            logger.error(f"/why error: {e}")
            await update.message.reply_text(f"❌ Error: {e}")

    # ── /mood ─────────────────────────────────────────────────────────────────

    async def cmd_mood(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await self._require_system(update): return
        try:
            from services.personality_service import PersonalityDatabase
            db     = PersonalityDatabase()
            report = db.get_personality_report()
            db.close()

            mood_emojis = {
                'euphoric':'🚀🚀🚀','confident':'😎','on_fire':'🔥',
                'rich':'🤑','cautious':'🤔','shaken':'😰',
                'grumpy':'😤','neutral':'😐',
            }
            mood  = report['current_mood']
            emoji = mood_emojis.get(mood, '🤖')
            stats = report['stats']

            msg = f"🤖 *Mood:* {emoji} {mood.upper()}\n\n"
            msg += f"Win rate this week: {stats['weekly_win_rate']:.0f}%\n"
            if stats['consecutive_wins'] > 2:
                msg += f"🔥 {stats['consecutive_wins']}-trade winning streak!\n"
            elif stats['consecutive_losses'] > 2:
                msg += f"😰 {stats['consecutive_losses']}-trade losing streak\n"
            msg += (
                f"\n*Stats*\n"
                f"Last 10: {stats['last_10_wins']}/10 wins\n"
                f"Last 10 P&L: ${stats['last_10_pnl']:.2f}\n"
            )
            await update.message.reply_text(msg, parse_mode='Markdown')
        except Exception as e:
            await update.message.reply_text(f"❌ Error: {e}")

    # ── /diary ────────────────────────────────────────────────────────────────

    async def cmd_diary(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await self._require_system(update): return
        try:
            from services.personality_service import PersonalityDatabase
            db     = PersonalityDatabase()
            report = db.get_personality_report()
            db.close()

            msg = "📔 *Trading Diary*\n\n"
            if report.get('memorable_moments'):
                msg += "*Memorable Moments*\n"
                for m in report['memorable_moments'][:5]:
                    icon = "✅" if m['is_win'] else "❌"
                    pnl  = f"+${m['pnl']:.0f}" if m['is_win'] else f"-${abs(m['pnl']):.0f}"
                    msg += f"{icon} {m['title']}: {pnl} ({m['date']})\n"
            else:
                msg += "No memorable moments yet — start trading!\n"

            traits = report.get('traits', {})
            if traits:
                msg += (
                    f"\n*Personality*\n"
                    f"Confidence:   {traits.get('confidence',0)*100:.0f}%\n"
                    f"Cautiousness: {traits.get('cautiousness',0)*100:.0f}%\n"
                    f"Optimism:     {traits.get('optimism',0)*100:.0f}%\n"
                )
            await update.message.reply_text(msg, parse_mode='Markdown')
        except Exception as e:
            await update.message.reply_text(f"❌ Error: {e}")

    # ── /pause ────────────────────────────────────────────────────────────────

    async def cmd_pause(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await self._require_system(update): return
        try:
            if not self.trading_system.is_running:
                await update.message.reply_text("⚠️ Already paused")
                return
            self.trading_system.is_running = False
            await update.message.reply_text("⏸️ *Trading Paused*\n\nNo new trades will open.", parse_mode='Markdown')
        except Exception as e:
            await update.message.reply_text(f"❌ Error: {e}")

    # ── /resume ───────────────────────────────────────────────────────────────

    async def cmd_resume(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await self._require_system(update): return
        try:
            if self.trading_system.is_running:
                await update.message.reply_text("⚠️ Already running")
                return
            self.trading_system.is_running = True
            await update.message.reply_text("▶️ *Trading Resumed*\n\nScanning for opportunities.", parse_mode='Markdown')
        except Exception as e:
            await update.message.reply_text(f"❌ Error: {e}")

    # ── /close ────────────────────────────────────────────────────────────────

    async def cmd_close(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await self._require_system(update): return
        try:
            if not context.args:
                await update.message.reply_text("Usage: `/close <trade_id>`\nGet IDs from /positions", parse_mode='Markdown')
                return
            trade_id  = context.args[0]
            positions = self.trading_system.paper_trader.get_open_positions()
            trade     = next((p for p in positions if p['trade_id'] == trade_id), None)
            if not trade:
                await update.message.reply_text(f"❌ Trade `{trade_id}` not found", parse_mode='Markdown')
                return
            price, _ = self.trading_system.fetcher.get_real_time_price(
                trade['asset'], trade.get('category', 'unknown')
            )
            if not price:
                await update.message.reply_text(f"❌ Could not get price for {trade['asset']}")
                return
            result = self.trading_system.paper_trader.force_close(trade_id, price, "Manual close via Telegram")
            if result:
                pnl   = result.get('pnl', 0)
                icon  = "✅" if pnl > 0 else "❌"
                await update.message.reply_text(
                    f"{icon} *Trade Closed*\n\n"
                    f"Asset: {trade['asset']}\n"
                    f"P&L:   ${pnl:.2f}\n"
                    f"Entry: `{trade['entry_price']:.5f}`\n"
                    f"Exit:  `{price:.5f}`",
                    parse_mode='Markdown'
                )
            else:
                await update.message.reply_text(f"❌ Failed to close {trade_id}")
        except Exception as e:
            logger.error(f"/close error: {e}")
            await update.message.reply_text(f"❌ Error: {e}")

    # ── Alert helpers (called from trading_system) ────────────────────────────

    def alert_trade_opened(self, signal: dict):
        try:
            emoji = "🟢" if signal.get('signal') == 'BUY' else "🔴"
            entry  = signal.get('entry_price', 0) or 0
            sl     = signal.get('stop_loss',   0) or 0
            tp     = signal.get('take_profit', 0) or 0
            rr     = abs(tp - entry) / abs(entry - sl) if abs(entry - sl) > 0 else 0
            self.send_message(
                f"{emoji} *Trade Opened*\n\n"
                f"Asset:    {signal.get('asset','?')}\n"
                f"Entry:    `{entry:.5f}`\n"
                f"Stop:     `{sl:.5f}`\n"
                f"TP:       `{tp:.5f}`\n"
                f"RR:       {rr:.1f}:1\n"
                f"Conf:     {signal.get('confidence',0):.0%}\n"
                f"Strategy: {signal.get('strategy_id','?')}"
            )
        except Exception as e:
            logger.error(f"alert_trade_opened: {e}")

    def alert_trade_closed(self, trade: dict):
        try:
            pnl  = trade.get('pnl', 0)
            icon = "✅" if pnl > 0 else "❌"
            self.send_message(
                f"{icon} *Trade Closed*\n\n"
                f"Asset:  {trade.get('asset','?')}\n"
                f"P&L:    ${pnl:.2f} ({trade.get('pnl_percent',0):.2f}%)\n"
                f"Entry:  `{trade.get('entry_price',0):.5f}`\n"
                f"Exit:   `{trade.get('exit_price',0):.5f}`\n"
                f"Reason: {trade.get('exit_reason','?')}"
            )
        except Exception as e:
            logger.error(f"alert_trade_closed: {e}")

    def alert_daily_loss_limit(self, loss_pct: float):
        self.send_message(
            f"⚠️ *DAILY LOSS LIMIT HIT*\n\n"
            f"Loss: {loss_pct:.1f}%\n"
            f"Trading paused. Use /resume to restart."
        )

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _get_daily_trades(self) -> int:
        try:
            today = datetime.now().date()
            return sum(
                1 for t in self.trading_system.paper_trader.closed_positions
                if getattr(t, 'exit_time', None) and t.exit_time.date() == today
            )
        except Exception:
            return 0