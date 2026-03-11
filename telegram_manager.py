"""
Telegram Bot Manager - Singleton to prevent conflicts
Handles stale PID files from crashed/force-closed previous runs
"""

import os
import atexit
from pathlib import Path
from logger import logger


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

    def _is_pid_alive(self, pid: int) -> bool:
        """Check if a PID is actually running on this OS."""
        try:
            if os.name == 'nt':  # Windows
                import subprocess
                result = subprocess.run(
                    ['tasklist', '/FI', f'PID eq {pid}'],
                    capture_output=True, text=True
                )
                return str(pid) in result.stdout
            else:  # Linux/Mac
                os.kill(pid, 0)
                return True
        except (OSError, ProcessLookupError):
            return False

    def _check_pid_file(self) -> bool:
        """
        Returns True if another live instance is genuinely running.
        Cleans up stale PID files automatically.
        """
        if not self._pid_file.exists():
            return False

        try:
            pid = int(self._pid_file.read_text().strip())
            if self._is_pid_alive(pid) and pid != os.getpid():
                logger.warning(f"Telegram bot already running (PID {pid})")
                return True
            else:
                # Stale file — previous run crashed or was force-closed
                logger.info(f"Removing stale Telegram PID file (PID {pid} not running)")
                self._pid_file.unlink()
                return False
        except (ValueError, OSError):
            # Corrupted or unreadable PID file — remove it
            logger.info("Removing invalid Telegram PID file")
            try:
                self._pid_file.unlink()
            except OSError:
                pass
            return False

    def start(self, token, chat_id, trading_system):
        """Start bot only if not already running"""
        if self.is_running:
            logger.warning("Telegram bot already running in this instance")
            return False

        if self._check_pid_file():
            logger.warning("Telegram bot not started — another live instance is running")
            return False

        try:
            from telegram_commander import TelegramCommander

            # Write our PID
            self._pid_file.write_text(str(os.getpid()))

            self.bot = TelegramCommander(token, chat_id, trading_system)
            self.bot.start()
            self.is_running = True

            atexit.register(self.cleanup)
            logger.info("✅ Telegram bot started successfully")
            return True

        except Exception as e:
            logger.error(f"Telegram bot error: {e}")
            self.cleanup()
            return False

    def is_other_instance_running(self) -> bool:
        """Public check used by trading_system.py before starting"""
        return self._check_pid_file()

    def cleanup(self):
        """Remove PID file on clean shutdown"""
        try:
            if self._pid_file.exists():
                self._pid_file.unlink()
        except OSError:
            pass
        self.is_running = False


# Global instance
telegram_manager = TelegramManager()