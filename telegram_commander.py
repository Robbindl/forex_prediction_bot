"""
Telegram Bot Commander - FIXED VERSION for python-telegram-bot v20+
Control your trading bot from your phone
"""

import telegram
from telegram.ext import Application, CommandHandler, ContextTypes
import threading
import time
from datetime import datetime, timedelta
import json
import os
from typing import Optional, Dict, List
import logging
from logger import logger

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - TELEGRAM - %(message)s',
    handlers=[
        logging.FileHandler('telegram_bot.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class RateLimiter:
    """Prevent spamming Telegram API"""
    
    def __init__(self, max_per_minute: int = 10):
        self.max_per_minute = max_per_minute
        self.requests: List[datetime] = []
    
    def can_send(self) -> bool:
        """Check if we can send another message"""
        now = datetime.now()
        # Remove requests older than 1 minute
        self.requests = [t for t in self.requests if now - t < timedelta(minutes=1)]
        
        if len(self.requests) < self.max_per_minute:
            self.requests.append(now)
            return True
        return False


class TelegramCommander:
    """
    🚀 FULL TELEGRAM COMMANDER FOR TRADING BOT - FIXED VERSION
    
    Commands:
    /start         - Welcome and help
    /status        - Bot status and market condition
    /positions     - View all open trades
    /pause         - Pause trading immediately
    /resume        - Resume trading
    /performance   - Show P&L and win rate
    /balance       - Current account balance
    /strategies    - Show strategy weights and performance
    /close <id>    - Close a specific trade by ID
    /market        - Show market status (open/closed)
    /help          - Show all commands
    /signal <asset> - Get clean signal for any asset
    /why <asset>   - Get human explanation for any asset
    /mood          - Check my current mood
    /diary         - See my trading diary
    """
    
    def __init__(self, token: str, chat_id: str, trading_system):
        """
        Initialize Telegram Commander
        
        Args:
            token: Telegram bot token from @BotFather
            chat_id: Your Telegram chat ID
            trading_system: Reference to your UltimateTradingSystem instance
        """
        self.token = token
        self.chat_id = chat_id
        self.trading_system = trading_system
        self.application = None
        self.is_running = False
        self.rate_limiter = RateLimiter(max_per_minute=10)
        
        logger.info(f"Telegram Commander initialized for chat {chat_id}")
    
    def start(self):
        """Start the Telegram bot - FIXED for v20+"""
        try:
            # Create application (new way for v20+)
            self.application = Application.builder().token(self.token).build()
            
            # Register command handlers
            self.application.add_handler(CommandHandler("start", self.cmd_start))
            self.application.add_handler(CommandHandler("status", self.cmd_status))
            self.application.add_handler(CommandHandler("positions", self.cmd_positions))
            self.application.add_handler(CommandHandler("pause", self.cmd_pause))
            self.application.add_handler(CommandHandler("resume", self.cmd_resume))
            self.application.add_handler(CommandHandler("performance", self.cmd_performance))
            self.application.add_handler(CommandHandler("balance", self.cmd_balance))
            self.application.add_handler(CommandHandler("strategies", self.cmd_strategies))
            self.application.add_handler(CommandHandler("market", self.cmd_market))
            self.application.add_handler(CommandHandler("close", self.cmd_close))
            self.application.add_handler(CommandHandler("help", self.cmd_help))
            self.application.add_handler(CommandHandler("why", self.cmd_why))
            self.application.add_handler(CommandHandler("mood", self.cmd_mood))
            self.application.add_handler(CommandHandler("diary", self.cmd_diary))
            self.application.add_handler(CommandHandler("signal", self.cmd_signal))
            
            # Start bot in a separate thread
            import threading
            self.bot_thread = threading.Thread(target=self._run_bot, daemon=True)
            self.bot_thread.start()
            
            self.is_running = True
            
            logger.info("✅ Telegram Commander started successfully")
            
            # Send startup message
            self.send_message(
                "🤖 *Commander Bot Active*\n\n"
                "This bot handles your trading commands.\n"
                "Use /help to see all commands.\n\n"
                f"Status: 🟢 Running"
            )
            
        except Exception as e:
            logger.error(f"❌ Telegram bot start error: {e}")
            self.is_running = False
    
    def _run_bot(self):
        """Run the bot in a separate thread"""
        try:
            if self.application:
                self.application.run_polling()
        except Exception as e:
            logger.error(f"Bot polling error: {e}")
    
    def send_message(self, text: str, parse_mode: str = 'Markdown') -> bool:
        """
        Send message to your Telegram with rate limiting
        
        Args:
            text: Message text
            parse_mode: 'Markdown' or 'HTML'
        
        Returns:
            True if sent, False if rate limited
        """
        if not self.rate_limiter.can_send():
            logger.warning("Rate limit exceeded, skipping message")
            return False
        
        try:
            if self.application and self.application.bot:
                # For v20+, we need to run async function
                import asyncio
                
                # Create new event loop if needed
                try:
                    loop = asyncio.get_event_loop()
                except RuntimeError:
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                
                # Run the async send
                loop.run_until_complete(
                    self.application.bot.send_message(
                        chat_id=self.chat_id,
                        text=text,
                        parse_mode=parse_mode
                    )
                )
                return True
            else:
                logger.error("Bot not initialized")
                return False
        except Exception as e:
            logger.error(f"Send message error: {e}")
            return False
    
    # ===== COMMAND HANDLERS =====
    
    async def _require_system(self, update) -> bool:
        """Returns True if trading_system is available, else sends error and returns False."""
        if self.trading_system is None:
            await update.message.reply_text(
                "⏳ Trading system is still initialising.\n"
                "Wait ~30 seconds after the dashboard loads, then try again."
            )
            return False
        return True

    async def cmd_start(self, update, context):
        """Welcome message"""
        status = "🟢 Running" if (self.trading_system and self.trading_system.is_running) else "⏳ Initialising"
        await update.message.reply_text(
            "🤖 *Trading Bot Commander*\n\n"
            "I control your Ultimate Trading System.\n"
            "Use /help to see all commands.\n\n"
            f"Status: {status}",
            parse_mode='Markdown'
        )
    
    async def cmd_status(self, update, context):
        """Show complete bot status"""
        if not await self._require_system(update): return
        try:
            # Get performance
            perf = self.trading_system.paper_trader.get_performance()
            
            # Get market status
            market_status = "🟢 RUNNING" if self.trading_system.is_running else "🔴 STOPPED"
            
            # Get market hours
            try:
                if hasattr(self.trading_system, 'fetcher'):
                    market_hours = self.trading_system.fetcher.get_market_status()
                    market_info = "Open" if not market_hours.get('is_weekend', False) else "Weekend"
                else:
                    market_info = "Unknown"
            except:
                market_info = "Unknown"
            
            # Get open positions
            open_positions = len(self.trading_system.paper_trader.get_open_positions())
            
            # Get today's date
            today = datetime.now().strftime('%Y-%m-%d')
            
            # Get daily trades count
            daily_trades = self.get_daily_trades()
            
            # Build message
            msg = f"📊 *Bot Status - {today}*\n\n"
            msg += f"Status: {market_status}\n"
            msg += f"Mode: {self.trading_system.strategy_mode.upper()}\n"
            msg += f"Market: {market_info}\n\n"
            
            msg += f"📈 *Performance*\n"
            msg += f"Open Positions: {open_positions}\n"
            msg += f"Today's Trades: {daily_trades}\n"
            msg += f"Total Trades: {perf['total_trades']}\n"
            msg += f"Win Rate: {perf['win_rate']}%\n"
            msg += f"Total P&L: ${perf['total_pnl']:.2f}\n"
            msg += f"Balance: ${perf['current_balance']:.2f}\n"
            
            await update.message.reply_text(msg, parse_mode='Markdown')
            
        except Exception as e:
            logger.error(f"Status command error: {e}")
            await update.message.reply_text(f"❌ Error: {e}")
    
    async def cmd_positions(self, update, context):
        """Show open positions with detailed info"""
        if not await self._require_system(update): return
        try:
            positions = self.trading_system.paper_trader.get_open_positions()
            
            if not positions:
                await update.message.reply_text("📭 No open positions")
                return
            
            # Get current prices for unrealized P&L
            current_prices = {}
            for p in positions:
                price, _ = self.trading_system.fetcher.get_real_time_price(
                    p['asset'], p.get('category', 'unknown')
                )
                if price:
                    current_prices[p['asset']] = price
            
            # Build message
            msg = f"📈 *Open Positions ({len(positions)})*\n\n"
            
            for i, p in enumerate(positions[:5], 1):
                # Calculate unrealized P&L
                unrealized = 0
                if p['asset'] in current_prices:
                    current = current_prices[p['asset']]
                    if p['signal'] == 'BUY':
                        unrealized = (current - p['entry_price']) * p['position_size']
                    else:
                        unrealized = (p['entry_price'] - current) * p['position_size']
                
                # Emoji for direction
                direction_emoji = "🟢" if p['signal'] == 'BUY' else "🔴"
                
                # Entry time (simplified)
                entry_time = p.get('entry_time', '')[:10] if p.get('entry_time') else 'Unknown'
                
                msg += f"{direction_emoji} *{p['asset']}* ({p['signal']})\n"
                msg += f"Entry: ${p['entry_price']:.2f}\n"
                msg += f"Stop: ${p['stop_loss']:.2f}\n"
                msg += f"Size: {p['position_size']:.4f}\n"
                msg += f"Unrealized: ${unrealized:.2f}\n"
                msg += f"ID: `{p['trade_id']}`\n\n"
            
            if len(positions) > 5:
                msg += f"... and {len(positions) - 5} more"
            
            await update.message.reply_text(msg, parse_mode='Markdown')
            
        except Exception as e:
            logger.error(f"Positions command error: {e}")
            await update.message.reply_text(f"❌ Error: {e}")
    
    async def cmd_pause(self, update, context):
        if not await self._require_system(update): return
        """Pause trading"""
        try:
            if not self.trading_system.is_running:
                await update.message.reply_text("⚠️ Trading already paused")
                return
            
            self.trading_system.is_running = False
            msg = "⏸️ *Trading Paused*\n\nNo new trades will be opened."
            
            await update.message.reply_text(msg, parse_mode='Markdown')
            self.send_message("⏸️ Trading paused by user")
            
        except Exception as e:
            logger.error(f"Pause command error: {e}")
            await update.message.reply_text(f"❌ Error: {e}")
    
    async def cmd_resume(self, update, context):
        if not await self._require_system(update): return
        """Resume trading"""
        try:
            if self.trading_system.is_running:
                await update.message.reply_text("⚠️ Trading already running")
                return
            
            self.trading_system.is_running = True
            msg = "▶️ *Trading Resumed*\n\nBot is now scanning for opportunities."
            
            await update.message.reply_text(msg, parse_mode='Markdown')
            self.send_message("▶️ Trading resumed by user")
            
        except Exception as e:
            logger.error(f"Resume command error: {e}")
            await update.message.reply_text(f"❌ Error: {e}")
    
    async def cmd_performance(self, update, context):
        if not await self._require_system(update): return
        """Show detailed performance metrics"""
        try:
            perf = self.trading_system.paper_trader.get_performance()
            
            # Calculate additional metrics
            total_trades = perf['total_trades']
            wins = perf['winning_trades']
            losses = perf['losing_trades']
            
            if total_trades > 0:
                win_rate = (wins / total_trades) * 100
                avg_win = perf['avg_win']
                avg_loss = perf['avg_loss']
                
                # Risk/Reward
                if avg_loss > 0:
                    rr_ratio = abs(avg_win / avg_loss)
                else:
                    rr_ratio = 0
                
                # Expectancy
                expectancy = (win_rate/100 * avg_win) - ((1 - win_rate/100) * avg_loss)
            else:
                win_rate = 0
                avg_win = 0
                avg_loss = 0
                rr_ratio = 0
                expectancy = 0
            
            # Build message
            msg = f"💰 *Performance Report*\n\n"
            msg += f"Total Trades: {total_trades}\n"
            msg += f"Wins: {wins}\n"
            msg += f"Losses: {losses}\n"
            msg += f"Win Rate: {win_rate:.1f}%\n"
            msg += f"Avg Win: ${avg_win:.2f}\n"
            msg += f"Avg Loss: ${avg_loss:.2f}\n"
            msg += f"Risk/Reward: {rr_ratio:.2f}\n"
            msg += f"Expectancy: ${expectancy:.2f}\n"
            msg += f"Profit Factor: {perf['profit_factor']:.2f}\n"
            msg += f"Total P&L: ${perf['total_pnl']:.2f}\n"
            msg += f"Current Balance: ${perf['current_balance']:.2f}\n"
            
            await update.message.reply_text(msg, parse_mode='Markdown')
            
        except Exception as e:
            logger.error(f"Performance command error: {e}")
            await update.message.reply_text(f"❌ Error: {e}")
    
    async def cmd_balance(self, update, context):
        if not await self._require_system(update): return
        """Show account balance"""
        try:
            perf = self.trading_system.paper_trader.get_performance()
            balance = perf['current_balance']
            total_pnl = perf['total_pnl']
            
            # Calculate change
            change_emoji = "📈" if total_pnl >= 0 else "📉"
            change_sign = "+" if total_pnl >= 0 else ""
            
            msg = f"💰 *Account Balance*\n\n"
            msg += f"Current: ${balance:.2f}\n"
            msg += f"Total P&L: {change_emoji} {change_sign}${total_pnl:.2f}\n"
            msg += f"Win Rate: {perf['win_rate']}%\n"
            msg += f"Open Positions: {perf['open_positions']}"
            
            await update.message.reply_text(msg, parse_mode='Markdown')
            
        except Exception as e:
            logger.error(f"Balance command error: {e}")
            await update.message.reply_text(f"❌ Error: {e}")

    async def cmd_signal(self, update, context):
        if not await self._require_system(update): return
        """
        🔍 Get CLEAN SIGNAL for ANY asset (exactly as requested)
        Usage: /signal BTC
            /signal ETH
            /signal GOLD
            /signal AAPL
            /signal "EUR/USD"
        """
        try:
            # Check if asset was provided
            if not context.args:
                await update.message.reply_text(
                    "🔍 *Please specify an asset!*\n\n"
                    "Examples:\n"
                    "• `/signal BTC` - Bitcoin\n"
                    "• `/signal ETH` - Ethereum\n"
                    "• `/signal GOLD` - Gold\n"
                    "• `/signal AAPL` - Apple stock\n"
                    "• `/signal EUR/USD` - Euro\n"
                    "• `/signal MSFT` - Microsoft\n"
                    "• `/signal SOL` - Solana\n"
                    "• `/signal OIL` - Crude Oil"
                )
                return
            
            # Get asset name
            asset = " ".join(context.args).upper().replace('"', '')
            
            # ===== COMPLETE ASSET ALIASES =====
            aliases = {
                # ===== CRYPTO (11) =====
                'BITCOIN': 'BTC-USD',
                'BTC': 'BTC-USD',
                'ETHEREUM': 'ETH-USD',
                'ETH': 'ETH-USD',
                'BINANCE': 'BNB-USD',
                'BNB': 'BNB-USD',
                'SOLANA': 'SOL-USD',
                'SOL': 'SOL-USD',
                'XRP': 'XRP-USD',
                'RIPPLE': 'XRP-USD',
                'CARDANO': 'ADA-USD',
                'ADA': 'ADA-USD',
                'DOGECOIN': 'DOGE-USD',
                'DOGE': 'DOGE-USD',
                'POLKADOT': 'DOT-USD',
                'DOT': 'DOT-USD',
                'LITECOIN': 'LTC-USD',
                'LTC': 'LTC-USD',
                'AVALANCHE': 'AVAX-USD',
                'AVAX': 'AVAX-USD',
                'CHAINLINK': 'LINK-USD',
                'LINK': 'LINK-USD',
                
                # ===== COMMODITIES (8) =====
                'GOLD': 'XAU/USD',
                'XAU': 'XAU/USD',
                'SILVER': 'XAG/USD',
                'XAG': 'XAG/USD',
                'PLATINUM': 'XPT/USD',
                'XPT': 'XPT/USD',
                'PALLADIUM': 'XPD/USD',
                'XPD': 'XPD/USD',
                'OIL': 'CL=F',
                'WTI': 'CL=F',
                'CRUDE': 'CL=F',
                'NATURAL GAS': 'NG=F',
                'GAS': 'NG=F',
                'COPPER': 'HG=F',
                'CU': 'HG=F',
                
                # ===== FOREX (20) =====
                'EURO': 'EUR/USD',
                'EUR': 'EUR/USD',
                'POUND': 'GBP/USD',
                'GBP': 'GBP/USD',
                'YEN': 'USD/JPY',
                'JPY': 'USD/JPY',
                'AUD': 'AUD/USD',
                'AUSSIE': 'AUD/USD',
                'CAD': 'USD/CAD',
                'LOONIE': 'USD/CAD',
                'CHF': 'USD/CHF',
                'SWISS': 'USD/CHF',
                'NZD': 'NZD/USD',
                'KIWI': 'NZD/USD',
                'EURGBP': 'EUR/GBP',
                'EURJPY': 'EUR/JPY',
                'GBPJPY': 'GBP/JPY',
                'AUDJPY': 'AUD/JPY',
                'EURAUD': 'EUR/AUD',
                'GBPAUD': 'GBP/AUD',
                
                # ===== INDICES (8) =====
                'SP500': '^GSPC',
                'S&P': '^GSPC',
                'SPX': '^GSPC',
                'DOW': '^DJI',
                'DJI': '^DJI',
                'NASDAQ': '^IXIC',
                'IXIC': '^IXIC',
                'FTSE': '^FTSE',
                'UK100': '^FTSE',
                'NIKKEI': '^N225',
                'N225': '^N225',
                'HANG SENG': '^HSI',
                'HSI': '^HSI',
                'DAX': '^GDAXI',
                'GDAXI': '^GDAXI',
                'VIX': '^VIX',
                'FEAR': '^VIX',
                
                # ===== STOCKS (17) =====
                'APPLE': 'AAPL',
                'AAPL': 'AAPL',
                'MICROSOFT': 'MSFT',
                'MSFT': 'MSFT',
                'GOOGLE': 'GOOGL',
                'GOOGL': 'GOOGL',
                'GOOG': 'GOOGL',
                'AMAZON': 'AMZN',
                'AMZN': 'AMZN',
                'TESLA': 'TSLA',
                'TSLA': 'TSLA',
                'NVIDIA': 'NVDA',
                'NVDA': 'NVDA',
                'META': 'META',
                'FACEBOOK': 'META',
                'JPMORGAN': 'JPM',
                'JPM': 'JPM',
                'VISA': 'V',
                'V': 'V',
                'MASTERCARD': 'MA',
                'MA': 'MA',
                'JOHNSON': 'JNJ',
                'JNJ': 'JNJ',
                'PFIZER': 'PFE',
                'PFE': 'PFE',
                'WALMART': 'WMT',
                'WMT': 'WMT',
                'PROCTER': 'PG',
                'PG': 'PG',
                'COCA COLA': 'KO',
                'KO': 'KO',
                'EXXON': 'XOM',
                'XOM': 'XOM',
                'CHEVRON': 'CVX',
                'CVX': 'CVX',
            }
            
            # Apply alias if exists
            if asset in aliases:
                asset = aliases[asset]
                logger.info(f"Alias converted: {asset}")
            
            # Send typing indicator
            await update.message.chat.send_action(action="typing")
            
            # Show searching message
            searching_msg = await update.message.reply_text(
                f"🔍 Fetching signal for *{asset}*..."
            )
            
            # Fetch data
            df = self.trading_system.fetch_historical_data(asset, days=3, interval='15m')
            
            if df is None or df.empty:
                await searching_msg.edit_text(
                    f"❌ *Asset not found:* {asset}\n\n"
                    f"Try: BTC, ETH, GOLD, AAPL, EUR/USD"
                )
                return
            
            # Add indicators
            df = self.trading_system.add_technical_indicators(df)
            
            # Get ML prediction
            prediction = self.trading_system.predictor.predict_next(df)
            
            # Get current price
            current_price = df['close'].iloc[-1]
            direction = prediction.get('direction', 'HOLD')
            signal_direction = 'BUY' if direction == 'UP' else 'SELL' if direction == 'DOWN' else 'HOLD'
            confidence = prediction.get('confidence', 0.5) * 100
            
            # Calculate stop loss and take profit
            if signal_direction == 'BUY':
                stop_loss = current_price * 0.995
                take_profit = current_price * 1.01
            elif signal_direction == 'SELL':
                stop_loss = current_price * 1.005
                take_profit = current_price * 0.99
            else:
                stop_loss = current_price
                take_profit = current_price
            
            # Get current time
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # Build CLEAN SIGNAL message
            msg = (
                f"*Signal:* {signal_direction}\n"
                f"*Entry:* ${current_price:.2f}\n"
                f"*Stop Loss:* ${stop_loss:.2f}\n"
                f"*Take Profit:* ${take_profit:.2f}\n"
                f"*Confidence:* {confidence:.1f}%\n"
                f"*Strategy:* VOTING\n\n"
                f"*Time:* {current_time}"
            )
            
            await searching_msg.delete()
            await update.message.reply_text(msg, parse_mode='Markdown')
            
        except Exception as e:
            logger.error(f"Signal command error: {e}")
            await update.message.reply_text(f"❌ Error: {e}")

    async def cmd_why(self, update, context):
        if not await self._require_system(update): return
        """Get HUMAN explanation for any asset"""
        try:
            if not context.args:
                asset = "BTC-USD"
                await update.message.reply_text("No asset specified. Using BTC-USD as example.")
            else:
                asset = context.args[0].upper()
            
            # Aliases
            aliases = {
                # ===== CRYPTO (11) =====
                'BITCOIN': 'BTC-USD',
                'BTC': 'BTC-USD',
                'ETHEREUM': 'ETH-USD',
                'ETH': 'ETH-USD',
                'BINANCE': 'BNB-USD',
                'BNB': 'BNB-USD',
                'SOLANA': 'SOL-USD',
                'SOL': 'SOL-USD',
                'XRP': 'XRP-USD',
                'RIPPLE': 'XRP-USD',
                'CARDANO': 'ADA-USD',
                'ADA': 'ADA-USD',
                'DOGECOIN': 'DOGE-USD',
                'DOGE': 'DOGE-USD',
                'POLKADOT': 'DOT-USD',
                'DOT': 'DOT-USD',
                'LITECOIN': 'LTC-USD',
                'LTC': 'LTC-USD',
                'AVALANCHE': 'AVAX-USD',
                'AVAX': 'AVAX-USD',
                'CHAINLINK': 'LINK-USD',
                'LINK': 'LINK-USD',
                
                # ===== COMMODITIES (8) =====
                'GOLD': 'XAU/USD',
                'XAU': 'XAU/USD',
                'SILVER': 'XAG/USD',
                'XAG': 'XAG/USD',
                'PLATINUM': 'XPT/USD',
                'XPT': 'XPT/USD',
                'PALLADIUM': 'XPD/USD',
                'XPD': 'XPD/USD',
                'OIL': 'CL=F',
                'WTI': 'CL=F',
                'CRUDE': 'CL=F',
                'NATURAL GAS': 'NG=F',
                'GAS': 'NG=F',
                'COPPER': 'HG=F',
                'CU': 'HG=F',
                
                # ===== FOREX (20) =====
                'EURO': 'EUR/USD',
                'EUR': 'EUR/USD',
                'POUND': 'GBP/USD',
                'GBP': 'GBP/USD',
                'YEN': 'USD/JPY',
                'JPY': 'USD/JPY',
                'AUD': 'AUD/USD',
                'AUSSIE': 'AUD/USD',
                'CAD': 'USD/CAD',
                'LOONIE': 'USD/CAD',
                'CHF': 'USD/CHF',
                'SWISS': 'USD/CHF',
                'NZD': 'NZD/USD',
                'KIWI': 'NZD/USD',
                'EURGBP': 'EUR/GBP',
                'EURJPY': 'EUR/JPY',
                'GBPJPY': 'GBP/JPY',
                'AUDJPY': 'AUD/JPY',
                'EURAUD': 'EUR/AUD',
                'GBPAUD': 'GBP/AUD',
                
                # ===== INDICES (8) =====
                'SP500': '^GSPC',
                'S&P': '^GSPC',
                'SPX': '^GSPC',
                'DOW': '^DJI',
                'DJI': '^DJI',
                'NASDAQ': '^IXIC',
                'IXIC': '^IXIC',
                'FTSE': '^FTSE',
                'UK100': '^FTSE',
                'NIKKEI': '^N225',
                'N225': '^N225',
                'HANG SENG': '^HSI',
                'HSI': '^HSI',
                'DAX': '^GDAXI',
                'GDAXI': '^GDAXI',
                'VIX': '^VIX',
                'FEAR': '^VIX',
                
                # ===== STOCKS (17) =====
                'APPLE': 'AAPL',
                'AAPL': 'AAPL',
                'MICROSOFT': 'MSFT',
                'MSFT': 'MSFT',
                'GOOGLE': 'GOOGL',
                'GOOGL': 'GOOGL',
                'GOOG': 'GOOGL',
                'AMAZON': 'AMZN',
                'AMZN': 'AMZN',
                'TESLA': 'TSLA',
                'TSLA': 'TSLA',
                'NVIDIA': 'NVDA',
                'NVDA': 'NVDA',
                'META': 'META',
                'FACEBOOK': 'META',
                'JPMORGAN': 'JPM',
                'JPM': 'JPM',
                'VISA': 'V',
                'V': 'V',
                'MASTERCARD': 'MA',
                'MA': 'MA',
                'JOHNSON': 'JNJ',
                'JNJ': 'JNJ',
                'PFIZER': 'PFE',
                'PFE': 'PFE',
                'WALMART': 'WMT',
                'WMT': 'WMT',
                'PROCTER': 'PG',
                'PG': 'PG',
                'COCA COLA': 'KO',
                'KO': 'KO',
                'EXXON': 'XOM',
                'XOM': 'XOM',
                'CHEVRON': 'CVX',
                'CVX': 'CVX',
            }
            if asset in aliases:
                asset = aliases[asset]
            
            await update.message.chat.send_action(action="typing")
            
            # Create explainer
            from human_explainer_db import DatabaseExplainer
            explainer = DatabaseExplainer(self.trading_system)
            
            searching_msg = await update.message.reply_text(f"🤔 Let me think about {asset}...")
            
            # Fetch data
            df = self.trading_system.fetch_historical_data(asset, days=3, interval='15m')
            if df.empty:
                await searching_msg.edit_text(f"❌ Couldn't fetch data for {asset}")
                return
            
            df = self.trading_system.add_technical_indicators(df)
            prediction = self.trading_system.predictor.predict_next(df)
            
            # Get sentiment and news
            sentiment = {}
            news = []
            if hasattr(self.trading_system, 'sentiment_analyzer'):
                sentiment = self.trading_system.sentiment_analyzer.get_comprehensive_sentiment('crypto')
                if hasattr(self.trading_system.sentiment_analyzer, 'news_integrator'):
                    news = self.trading_system.sentiment_analyzer.news_integrator.fetch_by_symbol(asset, limit=3)
            
            # Generate human explanation
            explanation = explainer.explain_signal(
                asset, df, prediction, sentiment, news,
                chat_id=str(update.effective_chat.id)
            )
            
            await searching_msg.delete()
            
            # Send explanation
            if len(explanation) > 4000:
                chunks = [explanation[i:i+4000] for i in range(0, len(explanation), 4000)]
                for chunk in chunks:
                    await update.message.reply_text(chunk, parse_mode='Markdown')
            else:
                await update.message.reply_text(explanation, parse_mode='Markdown')
            
            explainer.close()
            
        except Exception as e:
            logger.error(f"Why command error: {e}")
            await update.message.reply_text(f"❌ Error: {e}")

    async def cmd_mood(self, update, context):
        if not await self._require_system(update): return
        """Check the bot's current mood"""
        try:
            from services.personality_service import PersonalityDatabase
            db = PersonalityDatabase()
            report = db.get_personality_report()
            
            mood_emojis = {
                'euphoric': '🚀🚀🚀',
                'confident': '😎',
                'on_fire': '🔥',
                'rich': '🤑',
                'cautious': '🤔',
                'shaken': '😰',
                'grumpy': '😤',
                'neutral': '😐'
            }
            
            mood = report['current_mood']
            emoji = mood_emojis.get(mood, '🤖')
            
            msg = f"🤖 *Current Mood:* {emoji} {mood.upper()}\n\n"
            msg += f"*Why:* {report['stats']['weekly_win_rate']:.0f}% win rate this week\n"
            
            if report['stats']['consecutive_wins'] > 2:
                msg += f"🔥 On a {report['stats']['consecutive_wins']}-trade winning streak!\n"
            elif report['stats']['consecutive_losses'] > 2:
                msg += f"😰 On a {report['stats']['consecutive_losses']}-trade losing streak\n"
            
            msg += f"\n*Stats:*\n"
            msg += f"• Last 10 trades: {report['stats']['last_10_wins']}/10 wins\n"
            msg += f"• Last 10 P&L: ${report['stats']['last_10_pnl']:.2f}\n"
            msg += f"• Total trades remembered: {report['stats']['total_trades_remembered']}\n"
            
            await update.message.reply_text(msg, parse_mode='Markdown')
            db.close()
            
        except Exception as e:
            await update.message.reply_text(f"Error checking mood: {e}")

    async def cmd_diary(self, update, context):
        if not await self._require_system(update): return
        """Show recent trading diary entries"""
        try:
            from services.personality_service import PersonalityDatabase
            db = PersonalityDatabase()
            report = db.get_personality_report()
            
            msg = f"📔 *Trading Diary*\n\n"
            
            if report['memorable_moments']:
                msg += "*Memorable Moments:*\n"
                for moment in report['memorable_moments'][:5]:
                    emoji = "✅" if moment['is_win'] else "❌"
                    pnl_str = f"+${moment['pnl']:.0f}" if moment['is_win'] else f"-${abs(moment['pnl']):.0f}"
                    msg += f"{emoji} {moment['title']}: {pnl_str} ({moment['date']})\n"
            else:
                msg += "No memorable moments yet. Start trading!\n"
            
            msg += f"\n*Personality Traits:*\n"
            msg += f"• Confidence: {report['traits']['confidence']*100:.0f}%\n"
            msg += f"• Cautiousness: {report['traits']['cautiousness']*100:.0f}%\n"
            msg += f"• Optimism: {report['traits']['optimism']*100:.0f}%\n"
            msg += f"• Talkativeness: {report['traits']['talkativeness']*100:.0f}%\n"
            
            await update.message.reply_text(msg, parse_mode='Markdown')
            db.close()
            
        except Exception as e:
            await update.message.reply_text(f"Error reading diary: {e}")
    
    async def cmd_strategies(self, update, context):
        if not await self._require_system(update): return
        """Show strategy weights and performance"""
        try:
            if not hasattr(self.trading_system, 'voting_engine'):
                await update.message.reply_text("❌ Voting engine not available")
                return
            
            # Get strategy data
            weights = self.trading_system.voting_engine.strategy_weights
            performance = self.trading_system.voting_engine.strategy_performance
            
            # Sort by weight
            sorted_strategies = sorted(weights.items(), key=lambda x: x[1], reverse=True)
            
            msg = "🧠 *Active Strategies*\n\n"
            
            for strategy, weight in sorted_strategies[:10]:
                # Get performance
                perf = performance.get(strategy, {})
                trades = perf.get('trades', 0)
                win_rate = perf.get('win_rate', 0) * 100 if trades > 0 else 0
                
                # Emoji based on weight
                if weight > 1.5:
                    emoji = "🔥"
                elif weight > 1.0:
                    emoji = "⚡"
                elif weight > 0.5:
                    emoji = "✅"
                else:
                    emoji = "⚠️"
                
                msg += f"{emoji} *{strategy}*\n"
                msg += f"Weight: {weight:.2f}x\n"
                if trades > 0:
                    msg += f"Trades: {trades} | WR: {win_rate:.1f}%\n"
                msg += "\n"
            
            await update.message.reply_text(msg, parse_mode='Markdown')
            
        except Exception as e:
            logger.error(f"Strategies command error: {e}")
            await update.message.reply_text(f"❌ Error: {e}")
    
    async def cmd_market(self, update, context):
        """Show market status"""
        try:
            # Get market hours
            status = self.trading_system.fetcher.get_market_status()
            
            msg = "📊 *Market Status*\n\n"
            
            # Crypto (24/7)
            msg += f"Crypto: {'🟢 OPEN' if status.get('crypto', True) else '🔴 CLOSED'}\n"
            
            # Forex
            msg += f"Forex: {'🟢 OPEN' if status.get('forex', False) else '🔴 CLOSED'}\n"
            
            # Stocks
            msg += f"Stocks: {'🟢 OPEN' if status.get('stocks', False) else '🔴 CLOSED'}\n"
            
            # Commodities
            msg += f"Commodities: {'🟢 OPEN' if status.get('commodities', False) else '🔴 CLOSED'}\n"
            
            # Indices
            msg += f"Indices: {'🟢 OPEN' if status.get('indices', False) else '🔴 CLOSED'}\n\n"
            
            msg += f"NY Time: {status.get('ny_time', 'Unknown')}\n"
            msg += f"EAT Time: {status.get('current_time', 'Unknown')}"
            
            await update.message.reply_text(msg, parse_mode='Markdown')
            
        except Exception as e:
            logger.error(f"Market command error: {e}")
            await update.message.reply_text(f"❌ Error: {e}")
    
    async def cmd_close(self, update, context):
        if not await self._require_system(update): return
        """Close a specific trade by ID"""
        try:
            # Get trade ID from command
            trade_id = context.args[0] if context.args else None
            if not trade_id:
                await update.message.reply_text(
                    "Usage: /close <trade_id>\n"
                    "Get trade IDs from /positions"
                )
                return
            
            # Find the trade
            positions = self.trading_system.paper_trader.get_open_positions()
            trade = None
            for p in positions:
                if p['trade_id'] == trade_id:
                    trade = p
                    break
            
            if not trade:
                await update.message.reply_text(f"❌ Trade {trade_id} not found")
                return
            
            # Get current price
            price, source = self.trading_system.fetcher.get_real_time_price(
                trade['asset'], trade.get('category', 'unknown')
            )
            
            if not price:
                await update.message.reply_text(f"❌ Could not get current price for {trade['asset']}")
                return
            
            # Close the trade
            result = self.trading_system.paper_trader.force_close(
                trade_id, price, "Manual close via Telegram"
            )
            
            if result:
                pnl = result.get('pnl', 0)
                emoji = "✅" if pnl > 0 else "❌"
                msg = (
                    f"{emoji} *Trade Closed*\n\n"
                    f"Asset: {trade['asset']}\n"
                    f"P&L: ${pnl:.2f}\n"
                    f"Entry: ${trade['entry_price']:.2f}\n"
                    f"Exit: ${price:.2f}\n"
                    f"Trade ID: `{trade_id}`"
                )
                await update.message.reply_text(msg, parse_mode='Markdown')
            else:
                await update.message.reply_text(f"❌ Failed to close trade {trade_id}")
            
        except IndexError:
            await update.message.reply_text("Usage: /close <trade_id>")
        except Exception as e:
            logger.error(f"Close command error: {e}")
            await update.message.reply_text(f"❌ Error: {e}")
    
    async def cmd_help(self, update, context):
        """Show all commands"""
        msg = (
            "🤖 *Telegram Commander Help*\n\n"
            "🔍 *SIGNAL COMMANDS*\n"
            "• `/signal BTC` - Clean signal for Bitcoin\n"
            "• `/signal ETH` - Clean signal for Ethereum\n"
            "• `/signal SOL` - Clean signal for Solana\n"
            "• `/signal GOLD` - Clean signal for Gold\n"
            "• `/signal OIL` - Clean signal for Crude Oil\n"
            "• `/signal AAPL` - Clean signal for Apple\n"
            "• `/signal MSFT` - Clean signal for Microsoft\n"
            "• `/signal EUR/USD` - Clean signal for Euro\n\n"
            "🧠 *HUMAN COMMANDS*\n"
            "• `/why BTC` - Human explanation\n"
            "• `/mood` - Check my current mood\n"
            "• `/diary` - See my trading diary\n\n"
            "📊 *TRADING COMMANDS*\n"
            "• `/status` - Complete bot status\n"
            "• `/positions` - View open trades\n"
            "• `/performance` - P&L and metrics\n"
            "• `/balance` - Account balance\n"
            "• `/market` - Market hours status\n"
            "• `/strategies` - Strategy weights\n"
            "• `/close <id>` - Close specific trade\n"
            "• `/pause` - Stop trading\n"
            "• `/resume` - Resume trading\n\n"
            f"*Status:* {'🟢 Running' if self.trading_system.is_running else '🔴 Stopped'}"
        )
        await update.message.reply_text(msg, parse_mode='Markdown')
    
    def get_daily_trades(self) -> int:
        """Get number of trades today"""
        try:
            today = datetime.now().date()
            count = 0
            for trade in self.trading_system.paper_trader.closed_positions:
                if trade.exit_time and trade.exit_time.date() == today:
                    count += 1
            return count
        except Exception as e:
            logger.error(f"Daily trades count error: {e}")
            return 0
    
    # ===== AUTO-ALERTS =====
    
    def alert_trade_opened(self, signal: dict):
        """Send alert when new trade opens"""
        try:
            emoji = "🟢" if signal['signal'] == 'BUY' else "🔴"
            strategy = signal.get('strategy_id', 'UNKNOWN')
            confidence = signal.get('confidence', 0.5)
            
            msg = (
                f"{emoji} *New Trade Opened*\n\n"
                f"Asset: {signal['asset']}\n"
                f"Direction: {signal['signal']}\n"
                f"Entry: ${signal['entry_price']:.2f}\n"
                f"Stop: ${signal['stop_loss']:.2f}\n"
                f"Confidence: {confidence:.0%}\n"
                f"Strategy: {strategy}\n"
                f"Reason: {signal.get('reason', 'N/A')[:50]}"
            )
            self.send_message(msg)
        except Exception as e:
            logger.error(f"Trade opened alert error: {e}")
    
    def alert_trade_closed(self, trade: dict):
        """Send alert when trade closes"""
        try:
            pnl = trade.get('pnl', 0)
            emoji = "✅" if pnl > 0 else "❌"
            
            msg = (
                f"{emoji} *Trade Closed*\n\n"
                f"Asset: {trade['asset']}\n"
                f"P&L: ${pnl:.2f} ({trade.get('pnl_percent', 0):.2f}%)\n"
                f"Entry: ${trade['entry_price']:.2f}\n"
                f"Exit: ${trade['exit_price']:.2f}\n"
                f"Reason: {trade.get('exit_reason', 'Unknown')}\n"
                f"Duration: {trade.get('duration_minutes', 0)} min"
            )
            self.send_message(msg)
        except Exception as e:
            logger.error(f"Trade closed alert error: {e}")
    
    def alert_daily_loss_limit(self, loss_pct: float):
        """Send alert when daily loss limit hit"""
        msg = (
            "⚠️ *DAILY LOSS LIMIT HIT*\n\n"
            f"Loss: {loss_pct:.1f}%\n"
            "Trading paused for 1 hour.\n"
            "Use /resume to restart earlier."
        )
        self.send_message(msg)
    
    def alert_profit_target(self, profit_pct: float):
        """Send alert when profit target reached"""
        msg = (
            "🎯 *PROFIT TARGET REACHED*\n\n"
            f"Profit: +{profit_pct:.1f}%\n"
            "Consider taking profits or trailing stops."
        )
        self.send_message(msg)
    
    def stop(self):
        """Stop the Telegram bot"""
        if self.application:
            try:
                import asyncio
                try:
                    loop = asyncio.get_event_loop()
                except RuntimeError:
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                
                if loop.is_running():
                    asyncio.create_task(self.application.stop())
                    time.sleep(1)
                else:
                    loop.run_until_complete(self.application.stop())
                    loop.close()
            except Exception as e:
                logger.error(f"Error stopping bot: {e}")
            
            self.is_running = False
            logger.info("🛑 Telegram Commander stopped")
            try:
                self.send_message("🛑 Trading bot offline")
            except:
                pass