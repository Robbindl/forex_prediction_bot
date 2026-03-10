"""
TRADING MONITOR - Real-time monitoring and alerts with Telegram & Email
"""

import threading
import time
from datetime import datetime
from typing import Dict, Optional
import json
import requests
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from logger import logger


class TradingMonitor:
    """
    - Real-time Trading Monitor
    - Performance tracking
    - Health checks
    - Alert system with Telegram & Email
    """
    
    def __init__(self, risk_manager=None, paper_trader=None, 
                 email_config: Optional[Dict] = None,
                 telegram_config: Optional[Dict] = None):
        self.risk_manager = risk_manager
        self.paper_trader = paper_trader
        self.email_config = email_config
        self.telegram_config = telegram_config
        
        # Alert thresholds
        self.alert_thresholds = {
            'drawdown_warning': 10.0,      # Alert at 10% drawdown
            'drawdown_critical': 15.0,      # Alert at 15% drawdown
            'daily_loss_warning': 3.0,      # Alert at 3% daily loss
            'daily_loss_critical': 5.0,     # Alert at 5% daily loss
            'consecutive_losses': 3,        # Alert after 3 consecutive losses
            'profit_taking': 10.0            # Alert at 10% profit
        }
        
        # State tracking
        self.last_alert_time = {}
        self.alert_cooldown = 300  # 5 minutes cooldown between same alerts
        self.consecutive_losses = 0
        self.last_trade_result = None
        
        # Monitoring thread
        self.monitoring = True
        self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.monitor_thread.start()
        
        logger.info("Trading Monitor Initialized")
        channels = []
        if telegram_config and telegram_config.get('enabled'):
            channels.append("Telegram")
        if email_config and email_config.get('enabled'):
            channels.append("Email")
        
        if channels:
            logger.info(f"Alert System: Enabled ({', '.join(channels)})")
        else:
            logger.info(f"Alert System: Basic (console only)")
    
    def _monitor_loop(self):
        """Background monitoring loop"""
        while self.monitoring:
            try:
                time.sleep(60)  # Check every minute
                self._check_alerts()
            except Exception as e:
                logger.error(f"Monitor error: {e}")
    
    def _check_alerts(self):
        """Check all alert conditions"""
        # Try to get risk manager status safely
        status = {}
        if self.risk_manager:
            try:
                if hasattr(self.risk_manager, 'get_status'):
                    status = self.risk_manager.get_status()
                elif hasattr(self.risk_manager, 'get_risk_status'):
                    status = self.risk_manager.get_risk_status()
            except Exception as e:
                logger.error(f"Could not get risk status: {e}")
        
        # Check drawdown
        drawdown = status.get('current_drawdown', 0)
        if drawdown >= self.alert_thresholds['drawdown_critical']:
            self._send_alert(
                'CRITICAL',
                f"Critical Drawdown: {drawdown:.1f}%",
                f"Max drawdown limit: {self.alert_thresholds['drawdown_critical']}%\n"
                f"Current balance: ${status.get('account_balance', 0):.2f}"
            )
        elif drawdown >= self.alert_thresholds['drawdown_warning']:
            self._send_alert(
                'WARNING',
                f"Drawdown Warning: {drawdown:.1f}%",
                f"Consider reducing risk or reviewing strategy."
            )
        
        # Check daily loss
        daily_loss = abs(status.get('daily_loss_percent', 0))
        if daily_loss >= self.alert_thresholds['daily_loss_critical']:
            self._send_alert(
                'CRITICAL',
                f"Critical Daily Loss: {daily_loss:.1f}%",
                f"Daily loss limit: {self.alert_thresholds['daily_loss_critical']}%\n"
                f"Daily P&L: ${status.get('daily_pnl', 0):.2f}"
            )
        elif daily_loss >= self.alert_thresholds['daily_loss_warning']:
            self._send_alert(
                'WARNING',
                f"Daily Loss Warning: {daily_loss:.1f}%",
                f"Consider stopping trading for the day."
            )
        
        # Check consecutive losses
        if self.consecutive_losses >= self.alert_thresholds['consecutive_losses']:
            self._send_alert(
                'WARNING',
                f"{self.consecutive_losses} Consecutive Losses",
                f"Strategy may need review."
            )
        
        # Check profit taking from paper trader
        if self.paper_trader:
            try:
                perf = self.paper_trader.get_performance()
                total_pnl = perf.get('total_pnl', 0)
                current_balance = perf.get('current_balance', 10000)
                total_pnl_percent = (total_pnl / current_balance) * 100
                
                if total_pnl_percent >= self.alert_thresholds['profit_taking']:
                    self._send_alert(
                        'SUCCESS',
                        f"Profit Target: {total_pnl_percent:.1f}%",
                        f"Total P&L: ${total_pnl:.2f}\n"
                        f"Win rate: {perf.get('win_rate', 0)}%"
                    )
            except Exception as e:
                logger.error(f"Could not get paper trader performance: {e}")
    
    def on_trade_closed(self, trade_result: Dict):
        """Called when a trade is closed"""
        if trade_result.get('pnl', 0) > 0:
            self.consecutive_losses = 0
            self.last_trade_result = f"WIN: ${trade_result['pnl']:.2f}"
        else:
            self.consecutive_losses += 1
            self.last_trade_result = f"LOSS: ${trade_result.get('pnl', 0):.2f}"
        
        # Get strategy info
        status = "[PROFIT]" if trade_result.get('pnl', 0) > 0 else "[LOSS]"
        strategy_emoji = trade_result.get('strategy_emoji', '🤖')
        strategy_id = trade_result.get('strategy_id', 'UNKNOWN')
        
        logger.info(f"Trade closed: {trade_result.get('asset')} - P&L: ${trade_result.get('pnl', 0):.2f}")
        
        # Send trade closed alert with strategy info
        self._send_alert(
            'SUCCESS' if trade_result.get('pnl', 0) > 0 else 'WARNING',
            f"{strategy_emoji} Trade Closed: {trade_result.get('asset', 'Unknown')} [{strategy_id}]",
            f"Exit Reason: {trade_result.get('exit_reason', 'Unknown')}\n"
            f"P&L: ${trade_result.get('pnl', 0):.2f} ({trade_result.get('pnl_percent', 0):.2f}%)\n"
            f"Entry: ${trade_result.get('entry_price', 0):.2f} → Exit: ${trade_result.get('exit_price', 0):.2f}\n"
            f"Strategy: {strategy_id}",
            strategy_info=f"{strategy_emoji} {status} - {strategy_id}"
        )
    
    def on_new_trade(self, trade_result: Dict):
        """Called when a new trade is opened"""
        strategy_emoji = trade_result.get('strategy_emoji', '🤖')
        strategy_id = trade_result.get('strategy_id', 'UNKNOWN')
        
        logger.info(f"New trade opened: {trade_result.get('asset')} - {trade_result.get('signal')}")
        
        self._send_alert(
            'INFO',
            f"{strategy_emoji} New Trade: {trade_result.get('asset', 'Unknown')} [{strategy_id}]",
            f"Signal: {trade_result.get('signal', 'Unknown')}\n"
            f"Entry: ${trade_result.get('entry_price', 0):.2f}\n"
            f"Stop Loss: ${trade_result.get('stop_loss', 0):.2f}\n"
            f"Take Profit: ${trade_result.get('take_profit', 0):.2f}\n"
            f"Confidence: {trade_result.get('confidence', 0):.1%}\n"
            f"Strategy: {strategy_id}",
            strategy_info=f"{strategy_emoji} Strategy: {strategy_id}"
        )
    
    def _send_alert(self, level: str, title: str, message: str, strategy_info: str = ""):
        """Send alert through all configured channels"""
        alert_key = f"{level}:{title}"
        now = time.time()
        
        # Check cooldown
        if alert_key in self.last_alert_time:
            if now - self.last_alert_time[alert_key] < self.alert_cooldown:
                logger.debug(f"Alert skipped due to cooldown: {title}")
                return
        
        self.last_alert_time[alert_key] = now
        logger.info(f"ALERT: {level} - {title}")
        
        # Always print to console
        self._console_alert(level, title, message)
        
        # Send to Telegram if configured
        if self.telegram_config and self.telegram_config.get('enabled'):
            self._telegram_alert(level, title, message, strategy_info)
        
        # Send to Email if configured
        if self.email_config and self.email_config.get('enabled'):
            self._email_alert(level, title, message)
    
    def _console_alert(self, level: str, title: str, message: str):
        """Print alert to console"""
        # Keep as print for visibility (but also log)
        print(f"\n{'='*60}")
        print(f"{level} ALERT: {title}")
        print(f"{'='*60}")
        print(message)
        print(f"{'='*60}\n")
    
    def _telegram_alert(self, level: str, title: str, message: str, strategy_info: str = ""):
        """📱 TELEGRAM ALERTS with strategy identification"""
        try:
            bot_token = self.telegram_config.get('bot_token')
            chat_id = self.telegram_config.get('chat_id')
            
            if not bot_token or not chat_id:
                logger.warning("Telegram: Missing bot_token or chat_id")
                return
            
            # Map level to text prefix
            level_prefix = {
                'CRITICAL': '[CRITICAL]',
                'WARNING': '[WARNING]',
                'SUCCESS': '[SUCCESS]',
                'INFO': '[INFO]'
            }.get(level, '[INFO]')
            
            # Format message with text prefix instead of emojis
            text = f"{level_prefix} {title}\n\n"
            text += f"{message}\n\n"
            text += f"Time: {datetime.now().strftime('%H:%M:%S')}"
            
            # Add strategy identifier to the message if provided
            if strategy_info:
                text = f"{strategy_info}\n\n{text}"
            
            url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
            data = {
                'chat_id': chat_id,
                'text': text,
                'parse_mode': 'Markdown'
            }
            
            response = requests.post(url, data=data, timeout=15)
            if response.status_code != 200:
                logger.error(f"Telegram error: {response.text}")
            else:
                logger.debug(f"Telegram alert sent: {title}")
                
        except Exception as e:
            logger.error(f"Telegram error: {e}")
    
    def _email_alert(self, level: str, title: str, message: str):
        """Send alert via Email"""
        try:
            smtp_server = self.email_config.get('smtp_server', 'smtp.gmail.com')
            smtp_port = self.email_config.get('smtp_port', 587)
            username = self.email_config.get('username')
            password = self.email_config.get('password')
            from_addr = self.email_config.get('from', username)
            to_addr = self.email_config.get('to', username)
            use_tls = self.email_config.get('use_tls', True)
            
            if not username or not password:
                logger.warning("Email: Missing username or password")
                return
            
            # Create message
            msg = MIMEMultipart()
            msg['From'] = from_addr
            msg['To'] = to_addr
            msg['Subject'] = f"[{level}] Trading Bot Alert: {title}"
            
            # HTML body
            html = f"""
            <html>
            <head>
                <style>
                    body {{ font-family: Arial, sans-serif; }}
                    .header {{ background-color: #f0f0f0; padding: 10px; }}
                    .content {{ padding: 20px; }}
                    .footer {{ color: #888; font-size: 12px; margin-top: 20px; }}
                </style>
            </head>
            <body>
                <div class="header">
                    <h2>{title}</h2>
                </div>
                <div class="content">
                    <pre>{message}</pre>
                    <p><strong>Time:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                    <p><strong>Level:</strong> {level}</p>
                </div>
                <div class="footer">
                    <p>This is an automated message from your trading bot.</p>
                </div>
            </body>
            </html>
            """
            
            msg.attach(MIMEText(html, 'html'))
            
            # Send email
            server = smtplib.SMTP(smtp_server, smtp_port)
            if use_tls:
                server.starttls()
            server.login(username, password)
            server.send_message(msg)
            server.quit()
            
            logger.debug(f"Email alert sent: {title}")
            
        except Exception as e:
            logger.error(f"Email error: {e}")
    
    def get_status(self) -> Dict:
        """Get monitoring status"""
        # Get paper trader performance for additional info
        paper_stats = {}
        if self.paper_trader:
            try:
                paper_stats = self.paper_trader.get_performance()
            except Exception as e:
                logger.error(f"Could not get paper trader performance: {e}")
        
        # Get alert channels status
        channels = []
        if self.telegram_config and self.telegram_config.get('enabled'):
            channels.append("Telegram")
        if self.email_config and self.email_config.get('enabled'):
            channels.append("Email")
        
        return {
            'alerts_enabled': bool(channels),
            'alert_channels': channels,
            'thresholds': self.alert_thresholds,
            'consecutive_losses': self.consecutive_losses,
            'last_alert': max(self.last_alert_time.values()) if self.last_alert_time else None,
            'paper_trading': paper_stats
        }


# Also create a simple console monitor for backtesting
class ConsoleMonitor:
    """Simple console monitor for backtesting"""
    
    def __init__(self):
        self.start_time = datetime.now()
        logger.info("Console Monitor initialized for backtesting")
    
    def print_backtest_results(self, results):
        """Print formatted backtest results"""
        # Keep as print for backtesting output
        print("\n" + "="*60)
        print("BACKTEST RESULTS")
        print("="*60)
        print(f"Total Trades: {results.total_trades}")
        print(f"Winning Trades: {results.winning_trades}")
        print(f"Losing Trades: {results.losing_trades}")
        print(f"Win Rate: {results.win_rate:.1%}")
        print(f"Total P&L: ${results.total_pnl:.2f}")
        print(f"Total Return: {results.total_return_pct:.2f}%")
        print(f"Profit Factor: {results.profit_factor:.2f}")
        print(f"Sharpe Ratio: {results.sharpe_ratio:.2f}")
        print(f"Max Drawdown: {results.max_drawdown:.2%}")
        print("="*60)
        
        # Also log summary
        logger.info(f"Backtest complete: {results.total_trades} trades, {results.win_rate:.1%} win rate, P&L: ${results.total_pnl:.2f}")
    
    def print_trade(self, trade):
        """Print individual trade"""
        # Keep as print for backtesting output
        print(f"\nTrade: {trade.asset} {trade.direction}")
        print(f"   Entry: ${trade.entry_price:.2f} → Exit: ${trade.exit_price:.2f}")
        print(f"   P&L: ${trade.pnl:.2f} ({trade.return_pct:.2f}%)")
        print(f"   Duration: {trade.duration_days} days")
        print(f"   Exit Reason: {trade.exit_reason}")
        
        # Also log
        logger.info(f"Trade: {trade.asset} {trade.direction} - P&L: ${trade.pnl:.2f}")


if __name__ == "__main__":
    # Test the monitor
    monitor = TradingMonitor()
    logger.info("Monitor initialized successfully")
    
    # Test console monitor
    console = ConsoleMonitor()
    logger.info("Console monitor ready")