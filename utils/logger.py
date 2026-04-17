"""
utils/logger.py — Centralised logging for the trading platform.
"""
from __future__ import annotations
import json, logging, logging.handlers, sys, threading
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from config.config import (
    ERROR_LOG_MAX_BYTES,
    LOG_BACKUP_COUNT,
    LOG_MAX_BYTES,
    LOG_RETENTION_DAYS,
    ML_SERVICE_LOG_MAX_BYTES,
    TRADES_LOG_MAX_BYTES,
)


def _fix_console_encoding() -> None:
    """Wrap stdout/stderr for UTF-8 on Windows — skip when pytest is running."""
    import io
    # Never touch streams when pytest is active — it manages them itself
    if "pytest" in sys.modules or "pluggy" in sys.modules:
        return
    for attr in ("stdout", "stderr"):
        stream = getattr(sys, attr)
        if hasattr(stream, "buffer"):
            try:
                setattr(sys, attr, io.TextIOWrapper(
                    stream.buffer, encoding="utf-8", errors="replace"
                ))
            except Exception:
                pass


_fix_console_encoding()

_EMOJI_MAP = {
    "🐋": "[WHALE]", "🚀": "[ROCKET]", "✅": "[OK]",  "❌": "[ERROR]",
    "⚠️": "[WARN]",  "🔴": "[RED]",    "🟢": "[GREEN]","📊": "[CHART]",
    "💰": "[MONEY]", "🎯": "[TARGET]", "🤖": "[BOT]",  "🔥": "[FIRE]",
    "📈": "[UP]",    "📉": "[DOWN]",   "⚡": "[BOLT]", "📝": "[NOTE]",
    "✓": "[OK]",     "✗": "[FAIL]",    "→": "->",      "←": "<-",
    "—": "-",        "ù": "u",         "ø": "o",
}


def _sanitize_console_text(text: str) -> str:
    value = str(text or "")
    for raw, replacement in _EMOJI_MAP.items():
        value = value.replace(raw, replacement)
    return value.encode("ascii", errors="replace").decode("ascii")


class _SafeFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        try:
            return super().format(record)
        except UnicodeEncodeError:
            msg = _sanitize_console_text(record.getMessage())
            return f"{self.formatTime(record)} | {record.levelname:<8} | {msg}"


class _SafeRotatingFileHandler(logging.handlers.RotatingFileHandler):
    """RotatingFileHandler that silently skips rotation errors on Windows."""
    def doRollover(self) -> None:
        try:
            super().doRollover()
        except (PermissionError, OSError):
            # On Windows, another process instance may hold the file lock
            pass


class _SafeStreamHandler(logging.StreamHandler):
    """Console handler that degrades to sanitized ASCII on narrow Windows consoles."""

    def emit(self, record: logging.LogRecord) -> None:
        try:
            super().emit(record)
        except UnicodeEncodeError:
            try:
                msg = _sanitize_console_text(self.format(record))
                stream = self.stream
                stream.write(msg + self.terminator)
                self.flush()
            except Exception:
                self.handleError(record)


class _TradeFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        return bool(getattr(record, "trade", False))


def _silence_external_logger(name: str) -> None:
    """
    Prevent dependency loggers from falling through to logging.lastResort,
    which systemd renders as raw one-line stderr noise such as
    `socket.send() raised exception.`.
    """
    ext = logging.getLogger(name)
    ext.handlers.clear()
    ext.addHandler(logging.NullHandler())
    ext.propagate = False


class TradingLogger:
    """Singleton logger with rotating files, trade log, and error log."""
    _instance: Optional["TradingLogger"] = None
    _lock = threading.Lock()

    def __new__(cls, log_dir: str = "logs", level: str = "INFO") -> "TradingLogger":
        with cls._lock:
            if cls._instance is None:
                inst = super().__new__(cls)
                inst._setup(log_dir, level)
                cls._instance = inst
            else:
                # Allow reinitialising logger level when requested (e.g. bot startup)
                if getattr(cls._instance, "_level", "INFO").upper() != level.upper():
                    cls._instance._setup(log_dir, level)
        return cls._instance

    def _setup(self, log_dir: str, level: str) -> None:
        self._level = level.upper()
        self._log_dir = Path(log_dir)
        self._log_dir.mkdir(parents=True, exist_ok=True)

        # Suppress noisy dependency stderr while keeping our own structured
        # stream diagnostics from exchange/websocket managers.
        _silence_external_logger("websocket")
        _silence_external_logger("websockets")

        self._logger = logging.getLogger("trading_bot")
        self._logger.setLevel(getattr(logging, self._level, logging.INFO))
        self._logger.propagate = False
        self._logger.handlers.clear()

        fmt_short = _SafeFormatter(
            "%(asctime)s | %(levelname)-8s | %(message)s", datefmt="%H:%M:%S"
        )
        fmt_full = logging.Formatter(
            "%(asctime)s | %(levelname)-8s | %(filename)s:%(lineno)d | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        fmt_trade = logging.Formatter(
            "%(asctime)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
        )

        # Console — prefer the wrapped stdout so Windows terminals use UTF-8 when available.
        ch = _SafeStreamHandler(sys.stdout)
        ch.setLevel(getattr(logging, level.upper(), logging.INFO))
        ch.setFormatter(fmt_short)
        self._logger.addHandler(ch)

        # Main rotating file
        fh = _SafeRotatingFileHandler(
            self._log_dir / "trading_bot.log",
            maxBytes=LOG_MAX_BYTES, backupCount=LOG_BACKUP_COUNT, encoding="utf-8"
        )
        fh.setLevel(getattr(logging, level.upper(), logging.INFO))
        fh.setFormatter(fmt_full)
        self._logger.addHandler(fh)

        # Errors only
        eh = _SafeRotatingFileHandler(
            self._log_dir / "errors.log",
            maxBytes=ERROR_LOG_MAX_BYTES, backupCount=LOG_BACKUP_COUNT, encoding="utf-8"
        )
        eh.setLevel(logging.ERROR)
        eh.setFormatter(fmt_full)
        self._logger.addHandler(eh)

        # Trades only
        th = _SafeRotatingFileHandler(
            self._log_dir / "trades.log",
            maxBytes=TRADES_LOG_MAX_BYTES, backupCount=LOG_BACKUP_COUNT, encoding="utf-8"
        )
        th.setLevel(logging.INFO)
        th.setFormatter(fmt_trade)
        th.addFilter(_TradeFilter())
        self._logger.addHandler(th)

    # ── Standard levels ────────────────────────────────────────────────────
    def debug(self, msg: str, *a, **kw) -> None:     self._logger.debug(msg, *a, **kw)
    def info(self, msg: str, *a, **kw) -> None:      self._logger.info(msg, *a, **kw)
    def warning(self, msg: str, *a, **kw) -> None:   self._logger.warning(msg, *a, **kw)
    def error(self, msg: str, *a, **kw) -> None:     self._logger.error(msg, *a, **kw)
    def critical(self, msg: str, *a, **kw) -> None:  self._logger.critical(msg, *a, **kw)
    def exception(self, msg: str, *a, **kw) -> None: self._logger.exception(msg, *a, **kw)

    # ── Structured helpers ─────────────────────────────────────────────────
    def log_signal(self, asset: str, direction: str, confidence: float,
                   strategy: str, layer: int = 0) -> None:
        self._logger.info(
            f"SIGNAL | {asset} | {direction} | score={confidence:.3f} | "
            f"strategy={strategy} | layer={layer}"
        )

    def log_trade(self, action: str, **fields) -> None:
        parts = " | ".join(f"{k}={v}" for k, v in fields.items())
        self._logger.info(f"TRADE:{action} | {parts}", extra={"trade": True})

    def log_decision(self, asset: str, step: int, decision: str, reason: str = "") -> None:
        self._logger.debug(f"DECISION | {asset} | S{step} | {decision} | {reason}")

    def log_ml(self, model: str, asset: str, prediction: float, confidence: float) -> None:
        self._logger.info(
            f"ML | {model} | {asset} | pred={prediction:.4f} | score={confidence:.3f}"
        )

    def log_api(self, api: str, endpoint: str, status: str, duration_ms: float) -> None:
        self._logger.debug(f"API | {api} | {endpoint} | {status} | {duration_ms:.0f}ms")

    def export_trades_json(self, trades: list, path: Optional[str] = None) -> None:
        out = (
            Path(path) if path
            else self._log_dir / f"trades_{datetime.now():%Y%m%d_%H%M%S}.json"
        )
        try:
            with open(out, "w", encoding="utf-8") as f:
                json.dump(trades, f, indent=2, default=str)
            self.info(f"Exported {len(trades)} trades → {out}")
        except Exception as e:
            self.error(f"Trade export failed: {e}")


# ── Singletons ─────────────────────────────────────────────────────────────────
_default_logger = TradingLogger()


def get_logger(name: Optional[str] = None) -> TradingLogger:
    return _default_logger


logger = _default_logger


_STALE_LOG_PATTERNS = (
    "*.out.log",
    "*.err.log",
    "*.pid",
    "*.deepcheck.log",
    "*.preclean.log",
    "startup_smoke_*.log",
    "full_startup_*.log",
    "noise_check*.log",
    "dashboard.log",
    "health.log",
    "telegram.log",
)


def get_rotating_file_logger(
    name: str,
    file_path: str | Path,
    *,
    level: int = logging.INFO,
    max_bytes: int = ML_SERVICE_LOG_MAX_BYTES,
    backup_count: int = LOG_BACKUP_COUNT,
) -> logging.Logger:
    path = Path(file_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    logger_name = f"trading_bot.file.{name}"
    named_logger = logging.getLogger(logger_name)
    named_logger.setLevel(level)
    named_logger.propagate = False

    desired_path = str(path.resolve())
    existing_path = getattr(named_logger, "_file_path", "")
    existing_max = getattr(named_logger, "_max_bytes", None)
    existing_backups = getattr(named_logger, "_backup_count", None)
    if (
        getattr(named_logger, "_configured", False)
        and existing_path == desired_path
        and existing_max == int(max_bytes)
        and existing_backups == int(backup_count)
    ):
        return named_logger

    named_logger.handlers.clear()
    handler = _SafeRotatingFileHandler(
        path,
        maxBytes=int(max_bytes),
        backupCount=int(backup_count),
        encoding="utf-8",
    )
    handler.setLevel(level)
    handler.setFormatter(
        logging.Formatter(
            "%(asctime)s | %(levelname)-8s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    named_logger.addHandler(handler)
    named_logger._configured = True  # type: ignore[attr-defined]
    named_logger._file_path = desired_path  # type: ignore[attr-defined]
    named_logger._max_bytes = int(max_bytes)  # type: ignore[attr-defined]
    named_logger._backup_count = int(backup_count)  # type: ignore[attr-defined]
    return named_logger


def prune_stale_log_artifacts(
    log_dir: str | Path = "logs",
    *,
    retention_days: int = LOG_RETENTION_DAYS,
) -> int:
    if int(retention_days) <= 0:
        return 0
    base = Path(log_dir)
    if not base.exists():
        return 0
    cutoff = datetime.now() - timedelta(days=int(retention_days))
    removed = 0
    for pattern in _STALE_LOG_PATTERNS:
        for path in base.glob(pattern):
            if not path.is_file():
                continue
            try:
                modified = datetime.fromtimestamp(path.stat().st_mtime)
            except OSError:
                continue
            if modified >= cutoff:
                continue
            try:
                path.unlink()
                removed += 1
            except OSError:
                continue
    return removed
