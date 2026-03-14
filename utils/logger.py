"""
utils/logger.py — Centralised logging. Replaces logger.py, monitor.py, error_handling.py
"""
from __future__ import annotations
import io, json, logging, logging.handlers, sys, threading
from datetime import datetime
from pathlib import Path
from typing import Optional

def _fix_console_encoding() -> None:
    for attr in ("stdout", "stderr"):
        stream = getattr(sys, attr)
        if hasattr(stream, "buffer"):
            setattr(sys, attr, io.TextIOWrapper(stream.buffer, encoding="utf-8", errors="replace"))

_fix_console_encoding()

_EMOJI_MAP = {
    "🐋":"[WHALE]","🚀":"[ROCKET]","✅":"[OK]","❌":"[ERROR]",
    "⚠️":"[WARN]","🔴":"[RED]","🟢":"[GREEN]","📊":"[CHART]",
    "💰":"[MONEY]","🎯":"[TARGET]","🤖":"[BOT]","🔥":"[FIRE]",
    "📈":"[UP]","📉":"[DOWN]","⚡":"[BOLT]","📝":"[NOTE]",
}

class _SafeFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        try:
            return super().format(record)
        except UnicodeEncodeError:
            msg = record.getMessage()
            for emoji, text in _EMOJI_MAP.items():
                msg = msg.replace(emoji, text)
            msg = msg.encode("ascii", errors="replace").decode("ascii")
            return f"{self.formatTime(record)} | {record.levelname:<8} | {msg}"

class _TradeFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        return bool(getattr(record, "trade", False))

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
        return cls._instance

    def _setup(self, log_dir: str, level: str) -> None:
        self._log_dir = Path(log_dir)
        self._log_dir.mkdir(parents=True, exist_ok=True)

        self._logger = logging.getLogger("trading_bot")
        self._logger.setLevel(getattr(logging, level.upper(), logging.INFO))
        self._logger.propagate = False
        self._logger.handlers.clear()

        fmt_short = _SafeFormatter("%(asctime)s | %(levelname)-8s | %(message)s", datefmt="%H:%M:%S")
        fmt_full  = logging.Formatter(
            "%(asctime)s | %(levelname)-8s | %(filename)s:%(lineno)d | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        fmt_trade = logging.Formatter("%(asctime)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

        # Console
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(logging.INFO)
        ch.setFormatter(fmt_short)
        self._logger.addHandler(ch)

        # Main rotating file
        fh = logging.handlers.RotatingFileHandler(
            self._log_dir / "trading_bot.log", maxBytes=5*1024*1024, backupCount=2, encoding="utf-8"
        )
        fh.setLevel(logging.INFO)
        fh.setFormatter(fmt_full)
        self._logger.addHandler(fh)

        # Errors only
        eh = logging.handlers.RotatingFileHandler(
            self._log_dir / "errors.log", maxBytes=2*1024*1024, backupCount=2, encoding="utf-8"
        )
        eh.setLevel(logging.ERROR)
        eh.setFormatter(fmt_full)
        self._logger.addHandler(eh)

        # Trades only
        th = logging.handlers.RotatingFileHandler(
            self._log_dir / "trades.log", maxBytes=2*1024*1024, backupCount=2, encoding="utf-8"
        )
        th.setLevel(logging.INFO)
        th.setFormatter(fmt_trade)
        th.addFilter(_TradeFilter())
        self._logger.addHandler(th)

    # ── Standard log levels ────────────────────────────────────────────────
    def debug(self, msg: str, *a, **kw) -> None:    self._logger.debug(msg, *a, **kw)
    def info(self, msg: str, *a, **kw) -> None:     self._logger.info(msg, *a, **kw)
    def warning(self, msg: str, *a, **kw) -> None:  self._logger.warning(msg, *a, **kw)
    def error(self, msg: str, *a, **kw) -> None:    self._logger.error(msg, *a, **kw)
    def critical(self, msg: str, *a, **kw) -> None: self._logger.critical(msg, *a, **kw)
    def exception(self, msg: str, *a, **kw) -> None:self._logger.exception(msg, *a, **kw)

    # ── Structured helpers ─────────────────────────────────────────────────
    def log_signal(self, asset: str, direction: str, confidence: float,
                   strategy: str, layer: int = 0) -> None:
        self._logger.info(
            f"SIGNAL | {asset} | {direction} | conf={confidence:.3f} | "
            f"strategy={strategy} | layer={layer}"
        )

    def log_trade(self, action: str, **fields) -> None:
        parts = " | ".join(f"{k}={v}" for k, v in fields.items())
        self._logger.info(f"TRADE:{action} | {parts}", extra={"trade": True})

    def log_pipeline(self, asset: str, layer: int, decision: str, reason: str = "") -> None:
        self._logger.debug(f"PIPELINE | {asset} | L{layer} | {decision} | {reason}")

    def log_ml(self, model: str, asset: str, prediction: float, confidence: float) -> None:
        self._logger.info(f"ML | {model} | {asset} | pred={prediction:.4f} | conf={confidence:.3f}")

    def log_api(self, api: str, endpoint: str, status: str, duration_ms: float) -> None:
        self._logger.debug(f"API | {api} | {endpoint} | {status} | {duration_ms:.0f}ms")

    def export_trades_json(self, trades: list, path: Optional[str] = None) -> None:
        out = Path(path) if path else self._log_dir / f"trades_{datetime.now():%Y%m%d_%H%M%S}.json"
        try:
            with open(out, "w", encoding="utf-8") as f:
                json.dump(trades, f, indent=2, default=str)
            self.info(f"Exported {len(trades)} trades → {out}")
        except Exception as e:
            self.error(f"Trade export failed: {e}")

# ── Singletons ─────────────────────────────────────────────────────────────

_default_logger = TradingLogger()

def get_logger(name: Optional[str] = None) -> TradingLogger:
    """Return the global logger. name param kept for API compatibility."""
    return _default_logger

logger = _default_logger