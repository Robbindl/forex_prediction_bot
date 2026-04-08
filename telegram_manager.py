"""
Telegram Bot Manager - Singleton to prevent conflicts.

Uses a PID file with process metadata so Windows PID reuse does not falsely
block Telegram startup after a restart.
"""

import atexit
import json
import os
from pathlib import Path
from config.config import DEBUG_FORCE_TELEGRAM, TELEGRAM_PID_FILE
from utils.logger import logger


class TelegramManager:
    """Simple manager to prevent multiple bot instances"""

    _instance = None
    _pid_file = Path(TELEGRAM_PID_FILE)

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.is_running = False
            cls._instance.bot = None
        return cls._instance

    def _get_process_snapshot(self, pid: int) -> dict | None:
        """Return identifying metadata for a live process, or None."""
        try:
            import psutil

            proc = psutil.Process(pid)
            return {
                "pid": int(pid),
                "create_time": float(proc.create_time()),
                "name": str(proc.name() or ""),
                "exe": str(proc.exe() or ""),
                "cmdline": [str(part) for part in (proc.cmdline() or []) if part],
            }
        except Exception:
            return None

    def _looks_like_bot_process(self, snapshot: dict) -> bool:
        """Best-effort check for legacy plain-PID files."""
        name = str(snapshot.get("name", "")).lower()
        exe = str(snapshot.get("exe", "")).lower()
        cmdline = " ".join(snapshot.get("cmdline") or []).lower()
        repo_root = str(Path.cwd()).lower()
        is_python = "python" in name or "python" in exe
        return is_python and ("bot.py" in cmdline or repo_root in cmdline or "telegram" in cmdline)

    def _read_pid_record(self) -> dict:
        """Support both new JSON metadata and legacy plain PID files."""
        raw = self._pid_file.read_text(encoding="utf-8").strip()
        if not raw:
            raise ValueError("empty pid file")
        if raw.startswith("{"):
            record = json.loads(raw)
            if "pid" not in record:
                raise ValueError("pid missing from record")
            return record
        return {"pid": int(raw), "legacy": True}

    def _matches_live_owner(self, record: dict) -> bool:
        """True only when the PID file still points at the same bot process."""
        pid = int(record["pid"])
        if pid == os.getpid():
            return False

        snapshot = self._get_process_snapshot(pid)
        if snapshot is None:
            return False

        if record.get("legacy"):
            return self._looks_like_bot_process(snapshot)

        try:
            expected_create_time = float(record.get("create_time"))
        except (TypeError, ValueError):
            expected_create_time = None

        if expected_create_time is None:
            return self._looks_like_bot_process(snapshot)

        return abs(float(snapshot["create_time"]) - expected_create_time) < 1.0

    def _current_pid_record(self) -> dict:
        """Metadata written to the PID file for the current process."""
        pid = os.getpid()
        snapshot = self._get_process_snapshot(pid) or {}
        return {
            "pid": pid,
            "create_time": snapshot.get("create_time"),
            "name": snapshot.get("name", ""),
            "cmdline": snapshot.get("cmdline", []),
        }

    def _check_pid_file(self) -> bool:
        """
        Returns True if another live instance is genuinely running.
        Cleans up stale PID files automatically.
        """
        if not self._pid_file.exists():
            return False

        try:
            record = self._read_pid_record()
            pid = int(record["pid"])
            if self._matches_live_owner(record):
                logger.warning(f"Telegram bot already running (PID {pid})")
                return True
            else:
                # Stale file — previous run crashed, or PID was reused by another process.
                logger.info(f"Removing stale Telegram PID file (PID {pid} not active for this bot)")
                self._pid_file.unlink()
                return False
        except (ValueError, OSError, json.JSONDecodeError):
            # Corrupted or unreadable PID file — remove it
            logger.info("Removing invalid Telegram PID file")
            try:
                self._pid_file.unlink()
            except OSError:
                pass
            return False

    @staticmethod
    def _clear_telegram_session(token: str):
        """
        Clear any stale Telegram session before starting a new one.
        Prevents 'Conflict: terminated by other getUpdates request' on fast restarts.
        """
        try:
            import requests as _req
            base = f"https://api.telegram.org/bot{token}"
            _req.post(f"{base}/deleteWebhook", json={"drop_pending_updates": True}, timeout=5)
            _req.post(f"{base}/close", timeout=5)
        except Exception:
            pass

    def start(self, token, chat_id, trading_system):
        """Start bot only if not already running"""
        if self.is_running:
            logger.warning("Telegram bot already running in this instance")
            return False

        if self._check_pid_file():
            if DEBUG_FORCE_TELEGRAM:
                logger.warning("Telegram bot existing instance detected, but DEBUG_FORCE_TELEGRAM=true: forcing start")
            else:
                logger.warning("Telegram bot not started — another live instance is running")
                return False

        # Always clear any stale session before starting polling
        self._clear_telegram_session(token)

        try:
            from telegram_commander import TelegramCommander

            # Write our process metadata so PID reuse does not cause false conflicts.
            self._pid_file.write_text(
                json.dumps(self._current_pid_record()),
                encoding="utf-8",
            )

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
        """Public check used by bot.py before starting"""
        return self._check_pid_file()

    def cleanup(self):
        """Remove PID file on clean shutdown"""
        try:
            if self.bot is not None:
                self.bot.stop()
        except Exception as e:
            logger.debug(f"Telegram bot stop during cleanup failed: {e}")

        try:
            if self._pid_file.exists():
                owner_record = self._read_pid_record()
                if str(owner_record.get("pid")) == str(os.getpid()):
                    self._pid_file.unlink()
        except (OSError, ValueError, json.JSONDecodeError):
            pass
        self.bot = None
        self.is_running = False


# Global instance
telegram_manager = TelegramManager()
