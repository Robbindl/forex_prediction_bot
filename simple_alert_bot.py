"""
Simple Alert Bot - Only sends notifications, no commands
No conflicts with the main command bot!
Use this in trading_system.py for alerts only
"""

import requests
import logging
from typing import Dict, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

class SimpleAlertBot:
    """
    Simple bot that ONLY sends alerts - no command handling
    Perfect for running alongside the main command bot in web_app_live.py
    """
    
    def __init__(self, token: str, chat_id: str):
        self.token = token
        self.chat_id = chat_id
        self.base_url = f"https://api.telegram.org/bot{token}"
        logger.info(f"✅ Simple Alert Bot initialized with token: {token[:10]}...")
    
    def send_message(self, text: str, parse_mode: str = 'Markdown') -> bool:
        """Send a message - no commands, no conflicts!"""
        try:
            url = f"{self.base_url}/sendMessage"
            data = {
                'chat_id': self.chat_id,
                'text': text,
                'parse_mode': parse_mode
            }
            response = requests.post(url, data=data, timeout=10)
            
            if response.status_code == 200:
                logger.debug(f"Alert sent: {text[:50]}...")
                return True
            else:
                logger.error(f"Telegram error: {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"Failed to send alert: {e}")
            return False
    
    # ===== ALERT METHODS =====
    
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
                f"Strategy: {strategy}"
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
                f"Reason: {trade.get('exit_reason', 'Unknown')}"
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
            "Use /resume in the web dashboard to restart earlier."
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
    
    def send_whale_alert(self, amount: float, symbol: str, value_millions: float, channel: str):
        """Send whale alert notification"""
        msg = (
            f"🐋 *Whale Alert*\n"
            f"{amount:.2f} {symbol} (${value_millions:.1f}M)\n"
            f"Channel: @{channel}"
        )
        self.send_message(msg)