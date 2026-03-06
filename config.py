"""
Configuration management for trading bot
Loads settings from JSON and environment variables
"""
import json
import os
from typing import Dict, Any, Optional
from pathlib import Path

class Config:
    """Centralized configuration management"""
    
    def __init__(self, config_file: str = 'config.json'):
        self.config_file = config_file
        self.config = self._load_config()
        self._load_env_overrides()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from JSON file"""
        default_config = {
            "trading": {
                "default_balance": 30,
                "max_positions": 5,
                "update_interval": 60,
                "strategy_modes": ["fast", "balanced", "strict"],
                "timeframes": ["15m", "1h"]
            },
            "risk": {
                "risk_per_trade": 0.01,
                "max_daily_loss": 5.0,
                "max_drawdown": 15.0,
                "max_correlation": 0.7,
                "consecutive_losses_limit": 3
            },
            "telegram": {
                "enabled": False,
                "bot_token": "",
                "chat_id": "",
                "alert_cooldown": 300
            },
            "email": {
                "enabled": False,
                "smtp_server": "smtp.gmail.com",
                "smtp_port": 587,
                "username": "",
                "password": "",
                "use_tls": True
            },
            "monitoring": {
                "drawdown_warning": 10.0,
                "drawdown_critical": 15.0,
                "daily_loss_warning": 3.0,
                "daily_loss_critical": 5.0,
                "profit_taking": 10.0
            },
            "data": {
                "cache_duration": 300,
                "max_cached_items": 100,
                "default_bars": 100
            },
            "logging": {
                "level": "INFO",
                "file": "trading_bot.log",
                "max_size_mb": 10,
                "backup_count": 5
            },
            "assets": {
                "major": ["EUR/USD", "GBP/USD", "USD/JPY", "USD/CHF"],
                "minor": ["EUR/GBP", "EUR/JPY", "GBP/JPY"],
                "exotic": []
            }
        }
        
        if Path(self.config_file).exists():
            with open(self.config_file, 'r') as f:
                user_config = json.load(f)
                self._deep_merge(default_config, user_config)
        
        return default_config
    
    def _deep_merge(self, base: Dict, update: Dict) -> None:
        """Recursively merge update into base"""
        for key, value in update.items():
            if key in base and isinstance(base[key], dict) and isinstance(value, dict):
                self._deep_merge(base[key], value)
            else:
                base[key] = value
    
    def _load_env_overrides(self) -> None:
        """Override config with environment variables"""
        # Telegram
        if os.getenv('TELEGRAM_BOT_TOKEN'):
            self.config['telegram']['bot_token'] = os.getenv('TELEGRAM_BOT_TOKEN')
            self.config['telegram']['enabled'] = True
        if os.getenv('TELEGRAM_CHAT_ID'):
            self.config['telegram']['chat_id'] = os.getenv('TELEGRAM_CHAT_ID')
        
        # Email
        if os.getenv('EMAIL_USERNAME'):
            self.config['email']['username'] = os.getenv('EMAIL_USERNAME')
        if os.getenv('EMAIL_PASSWORD'):
            self.config['email']['password'] = os.getenv('EMAIL_PASSWORD')
            self.config['email']['enabled'] = True
    
    @property
    def trading(self) -> Dict[str, Any]:
        return self.config['trading']
    
    @property
    def risk(self) -> Dict[str, Any]:
        return self.config['risk']
    
    @property
    def telegram(self) -> Dict[str, Any]:
        return self.config['telegram']
    
    @property
    def email(self) -> Dict[str, Any]:
        return self.config['email']
    
    @property
    def monitoring(self) -> Dict[str, Any]:
        return self.config['monitoring']
    
    @property
    def data(self) -> Dict[str, Any]:
        return self.config['data']
    
    @property
    def logging(self) -> Dict[str, Any]:
        return self.config['logging']
    
    @property
    def assets(self) -> Dict[str, Any]:
        return self.config['assets']
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get config value by dot notation key (e.g., 'trading.max_positions')"""
        keys = key.split('.')
        value = self.config
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
                if value is None:
                    return default
            else:
                return default
        return value
    
    def save(self) -> None:
        """Save current config to file"""
        with open(self.config_file, 'w') as f:
            json.dump(self.config, f, indent=4)


# Global config instance
config = Config()