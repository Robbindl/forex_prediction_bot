"""
Telegram Bot Manager - Simple singleton to prevent conflicts
Add this as a new file
"""

import os
import atexit
from pathlib import Path

class TelegramManager:
    """Simple manager to prevent multiple bot instances"""
    
    _instance = None
    _pid_file = Path("telegram_bot.pid")
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.is_running = False
            cls._instance.bot = None
        return cls._instance
    
    def start(self, token, chat_id, trading_system):
        """Start bot only if not already running"""
        if self.is_running:
            print("⚠️ Telegram bot already running in this instance")
            return False
        
        # Check if another process is running
        if self._pid_file.exists():
            print("⚠️ Telegram bot PID file exists - another instance may be running")
            return False
        
        try:
            from telegram_commander import TelegramCommander
            
            # Create PID file
            with open(self._pid_file, 'w') as f:
                f.write(str(os.getpid()))
            
            self.bot = TelegramCommander(token, chat_id, trading_system)
            self.bot.start()
            self.is_running = True
            
            # Register cleanup
            atexit.register(self.cleanup)
            
            print("✅ Telegram bot started successfully")
            return True
            
        except Exception as e:
            print(f"❌ Telegram bot error: {e}")
            self.cleanup()
            return False
    
    def cleanup(self):
        """Remove PID file"""
        if self._pid_file.exists():
            self._pid_file.unlink()
        self.is_running = False

# Global instance
telegram_manager = TelegramManager()