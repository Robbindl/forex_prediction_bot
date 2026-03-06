"""
Professional logging system for trading bot
"""
import logging
import logging.handlers
import sys
from pathlib import Path
from typing import Optional
from datetime import datetime
import traceback

class TradingLogger:
    """Centralized logging with rotation and formatting"""
    
    _instance = None
    _initialized = False
    
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self, config=None):
        if self._initialized:
            return
        
        self.config = config or {}
        self.logger = logging.getLogger('trading_bot')
        self.logger.setLevel(logging.DEBUG)
        
        # Remove existing handlers
        self.logger.handlers.clear()
        
        # Create logs directory
        log_dir = Path('logs')
        log_dir.mkdir(exist_ok=True)
        
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_format = logging.Formatter(
            '%(asctime)s | %(levelname)-8s | %(message)s',
            datefmt='%H:%M:%S'
        )
        console_handler.setFormatter(console_format)
        self.logger.addHandler(console_handler)
        
        # File handler with rotation
        log_file = log_dir / self.config.get('logging.file', 'trading_bot.log')
        file_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=self.config.get('logging.max_size_mb', 10) * 1024 * 1024,
            backupCount=self.config.get('logging.backup_count', 5)
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
            log_dir / 'errors.log',
            maxBytes=5 * 1024 * 1024,
            backupCount=3
        )
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(file_format)
        self.logger.addHandler(error_handler)
        
        self._initialized = True
    
    def debug(self, msg: str, *args, **kwargs):
        self.logger.debug(msg, *args, **kwargs)
    
    def info(self, msg: str, *args, **kwargs):
        self.logger.info(msg, *args, **kwargs)
    
    def warning(self, msg: str, *args, **kwargs):
        self.logger.warning(msg, *args, **kwargs)
    
    def error(self, msg: str, *args, **kwargs):
        self.logger.error(msg, *args, **kwargs)
    
    def critical(self, msg: str, *args, **kwargs):
        self.logger.critical(msg, *args, **kwargs)
    
    def exception(self, msg: str, *args, **kwargs):
        self.logger.exception(msg, *args, **kwargs)
    
    def trade(self, msg: str, **kwargs):
        """Log trade-specific information"""
        trade_info = ' | '.join(f"{k}={v}" for k, v in kwargs.items())
        self.logger.info(f"TRADE: {msg} | {trade_info}")


# Global logger instance
logger = TradingLogger()