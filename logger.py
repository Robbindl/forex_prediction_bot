"""
Professional logging system for trading bot
"""

import logging
import logging.handlers
import sys
from pathlib import Path
from typing import Optional
from datetime import datetime
import io
import traceback
import json
import re

# FIX: Handle Windows console encoding properly
if sys.platform == 'win32':
    # Set console to UTF-8 mode
    try:
        import subprocess
        subprocess.run('chcp 65001', shell=True, capture_output=True)
    except:
        pass
    
    # Wrap stdout/stderr with UTF-8 encoders that replace unsupported characters
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')
else:
    # For non-Windows systems
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')


class SafeFormatter(logging.Formatter):
    """Formatter that safely handles Unicode characters"""
    
    def format(self, record):
        try:
            # Try normal formatting first
            return super().format(record)
        except UnicodeEncodeError:
            # If that fails, remove or replace problematic characters
            msg = record.getMessage()
            # Replace common emojis with text equivalents
            emoji_map = {
                '🐋': '[WHALE]',
                '🚀': '[ROCKET]',
                '✅': '[OK]',
                '❌': '[ERROR]',
                '⚠️': '[WARN]',
                '🔴': '[RED]',
                '🟢': '[GREEN]',
                '📊': '[CHART]',
                '💰': '[MONEY]',
                '🎯': '[TARGET]',
                '🤖': '[BOT]',
                '😎': '[COOL]',
                '🔥': '[FIRE]',
                '📈': '[UP]',
                '📉': '[DOWN]',
                '⚡': '[BOLT]',
                '💬': '[TALK]',
                '📝': '[NOTE]',
                '🏆': '[TROPHY]',
                '🪙': '[COIN]',
                '👊': '[FIST]',
                '☕': '[COFFEE]',
                '🤔': '[THINK]',
                '😰': '[SWEAT]',
                '😤': ['ANGRY'],
                '😐': '[NEUTRAL]',
            }
            for emoji, text in emoji_map.items():
                msg = msg.replace(emoji, text)
            
            # Remove any other non-ASCII characters
            msg = msg.encode('ascii', errors='replace').decode('ascii')
            
            # Reconstruct the log line
            asctime = self.formatTime(record, self.datefmt)
            return f"{asctime} | {record.levelname:<8} | {msg}"
        except:
            # Ultimate fallback
            return f"{record.levelname}: {record.getMessage().encode('ascii', errors='replace').decode('ascii')}"


class TradingLogger:
    """Centralized logging with rotation and formatting"""
    
    _instance = None
    _initialized = False
    
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self, log_dir: str = "logs", log_level: str = "INFO"):
        if self._initialized:
            return
        
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(exist_ok=True)
        
        # Create logger
        self.logger = logging.getLogger('trading_bot')
        self.logger.setLevel(getattr(logging, log_level.upper()))

        # CRITICAL: stop messages bubbling up to the root (Flask) logger.
        # Without this every line prints TWICE — once by our handlers,
        # once by the root logger that Flask installs at startup.
        self.logger.propagate = False

        # Remove existing handlers
        self.logger.handlers.clear()
        
        # Console handler (with safe encoding and formatter)
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_format = SafeFormatter(
            '%(asctime)s | %(levelname)-8s | %(message)s',
            datefmt='%H:%M:%S'
        )
        console_handler.setFormatter(console_format)
        self.logger.addHandler(console_handler)
        
        # File handler with rotation (UTF-8 encoding)
        log_file = self.log_dir / 'trading_bot.log'
        file_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=10 * 1024 * 1024,  # 10MB
            backupCount=5,
            encoding='utf-8'  # Explicit UTF-8 for files
        )
        file_handler.setLevel(logging.DEBUG)
        file_format = logging.Formatter(
            '%(asctime)s | %(levelname)-8s | %(filename)s:%(lineno)d | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(file_format)
        self.logger.addHandler(file_handler)
        
        # Error file handler (separate file for errors)
        error_handler = logging.handlers.RotatingFileHandler(
            self.log_dir / 'errors.log',
            maxBytes=5 * 1024 * 1024,  # 5MB
            backupCount=3,
            encoding='utf-8'
        )
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(file_format)
        self.logger.addHandler(error_handler)
        
        # Trade log (special file for trades only)
        trade_handler = logging.handlers.RotatingFileHandler(
            self.log_dir / 'trades.log',
            maxBytes=5 * 1024 * 1024,  # 5MB
            backupCount=3,
            encoding='utf-8'
        )
        trade_handler.setLevel(logging.INFO)
        trade_format = logging.Formatter(
            '%(asctime)s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        trade_handler.setFormatter(trade_format)
        
        # Add a filter to only log trade messages
        class TradeFilter(logging.Filter):
            def filter(self, record):
                return getattr(record, 'trade', False)
        
        trade_handler.addFilter(TradeFilter())
        self.logger.addHandler(trade_handler)
        
        self._initialized = True
        
        # Log initialization (using safe message)
        self.info("="*60)
        self.info(" TRADING BOT LOGGER INITIALIZED")
        self.info("="*60)
        self.info(f"Log directory: {self.log_dir.absolute()}")
        self.info(f"Log level: {log_level}")
    
    def _safe_msg(self, msg: str) -> str:
        """Convert message to safe ASCII for console"""
        if sys.platform == 'win32':
            # Replace emojis with text for Windows console
            emoji_map = {
                '🐋': '[WHALE]',
                '🚀': '[ROCKET]',
                '✅': '[OK]',
                '❌': '[ERROR]',
                '⚠️': '[WARN]',
                '🔴': '[RED]',
                '🟢': '[GREEN]',
                '📊': '[CHART]',
                '💰': '[MONEY]',
                '🎯': '[TARGET]',
                '🤖': '[BOT]',
            }
            for emoji, text in emoji_map.items():
                msg = msg.replace(emoji, text)
        return msg
    
    def debug(self, msg: str, *args, **kwargs):
        """Log debug message"""
        self.logger.debug(self._safe_msg(msg), *args, **kwargs)
    
    def info(self, msg: str, *args, **kwargs):
        """Log info message"""
        self.logger.info(self._safe_msg(msg), *args, **kwargs)
    
    def warning(self, msg: str, *args, **kwargs):
        """Log warning message"""
        self.logger.warning(self._safe_msg(msg), *args, **kwargs)
    
    def error(self, msg: str, *args, **kwargs):
        """Log error message"""
        self.logger.error(self._safe_msg(msg), *args, **kwargs)
    
    def critical(self, msg: str, *args, **kwargs):
        """Log critical message"""
        self.logger.critical(self._safe_msg(msg), *args, **kwargs)
    
    def exception(self, msg: str, *args, **kwargs):
        """Log exception with traceback"""
        self.logger.exception(self._safe_msg(msg), *args, **kwargs)
    
    def trade(self, msg: str, **kwargs):
        """
        Log trade-specific information
        These go to trades.log
        """
        extra = {'trade': True}
        trade_info = ' | '.join(f"{k}={v}" for k, v in kwargs.items())
        self.logger.info(f"TRADE: {msg} | {trade_info}", extra=extra)
    
    def api_call(self, api_name: str, endpoint: str, status: str, duration: float):
        """Log API call metrics"""
        self.debug(f"API {api_name} | {endpoint} | {status} | {duration:.2f}s")
    
    def signal(self, asset: str, direction: str, confidence: float, strategy: str):
        """Log trading signal"""
        self.info(f"SIGNAL | {asset} | {direction} | conf={confidence:.2f} | {strategy}")
    
    def position_open(self, trade_id: str, asset: str, direction: str, entry: float, size: float):
        """Log position opened"""
        self.trade(f"OPEN | {trade_id} | {asset} | {direction} | entry={entry:.4f} | size={size:.4f}")
    
    def position_close(self, trade_id: str, asset: str, pnl: float, pnl_pct: float, reason: str):
        """Log position closed"""
        self.trade(f"CLOSE | {trade_id} | {asset} | pnl=${pnl:.2f} | {pnl_pct:.2f}% | {reason}")
    
    def market_status(self, category: str, status: bool):
        """Log market status changes"""
        status_str = "OPEN" if status else "CLOSED"
        self.info(f"MARKET | {category} | {status_str}")
    
    def export_trades_json(self, trades: list, filename: Optional[str] = None):
        """Export trades to JSON file"""
        if filename is None:
            filename = self.log_dir / f"trades_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        try:
            with open(filename, 'w') as f:
                json.dump(trades, f, indent=2, default=str)
            self.info(f"Exported {len(trades)} trades to {filename}")
        except Exception as e:
            self.error(f"Failed to export trades: {e}")


# Create global logger instance
logger = TradingLogger()


# Convenience functions
def get_logger():
    """Get the global logger instance"""
    return logger


# Example usage in other files:
if __name__ == "__main__":
    # Test the logger
    log = get_logger()
    
    log.info("Testing logger")
    log.debug("Debug message")
    log.warning("Warning message")
    log.error("Error message")
    
    # Trade logging
    log.trade("Test trade", asset="BTC-USD", direction="BUY", pnl=100.50)
    
    # API call logging
    log.api_call("Finnhub", "quote", "success", 0.235)
    
    # Signal logging
    log.signal("EUR/USD", "BUY", 0.85, "RSI")
    
    # Position logging
    log.position_open("abc123", "BTC-USD", "BUY", 50000, 0.1)
    log.position_close("abc123", "BTC-USD", 150, 3.0, "take_profit")