"""
bot.py — Single entry point for the trading platform.

Startup sequence:
  1. Load config
  2. Init logger
  3. Connect to database (required — exits if unavailable)
  4. Init TradingCore
  5. Start trading loop (daemon thread)
  6. Start auto-trainer (daemon thread)
  7. Start Telegram commander (optional)
  8. Start Flask dashboard (blocking — main thread)
"""
from __future__ import annotations
import argparse
import sys
import signal
import threading
from pathlib import Path

# ── Bootstrap ─────────────────────────────────────────────────────────────────
from config.config import (
    LOG_LEVEL, LOG_DIR, DEFAULT_BALANCE,
    TELEGRAM_TOKEN, TELEGRAM_CHAT_ID,
)
from utils.logger import TradingLogger, get_logger

_trading_logger = TradingLogger(log_dir=str(LOG_DIR), level=LOG_LEVEL)
logger = get_logger()

logger.info("=" * 60)
logger.info(" FOREX PREDICTION BOT — STARTING")
logger.info("=" * 60)

# ── Database (required) ───────────────────────────────────────────────────────
logger.info("[bot] Connecting to database...")
try:
    from config.database import init_db
    init_db()
    logger.info("[bot] Database ready")
except RuntimeError as e:
    logger.critical(str(e))
    sys.exit(1)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Forex/Crypto Prediction Trading Bot")
    p.add_argument("--balance",      type=float, default=DEFAULT_BALANCE,
                   help=f"Starting balance (default: {DEFAULT_BALANCE})")
    p.add_argument("--strategy",     type=str,   default="voting",
                   choices=["voting", "rsi", "macd", "bollinger"],
                   help="Strategy mode")
    p.add_argument("--no-telegram",  action="store_true", help="Disable Telegram")
    p.add_argument("--no-dashboard", action="store_true", help="Disable web dashboard")
    p.add_argument("--port",         type=int,   default=5000,   help="Dashboard port")
    p.add_argument("--host",         type=str,   default="0.0.0.0", help="Dashboard host")
    p.add_argument("--backtest",     type=str,   default=None,
                   help="Run backtest on asset (e.g. BTC-USD) and exit")
    p.add_argument("--backtest-cat", type=str,   default="crypto",
                   help="Category for backtest asset")
    return p.parse_args()


def run_backtest(asset: str, category: str) -> None:
    logger.info(f"[bot] Running backtest: {asset} ({category})")
    try:
        from data.fetcher    import DataFetcher
        from backtest.engine import BacktestEngine
        fetcher = DataFetcher()
        df      = fetcher.get_ohlcv(asset, category, "1d", 500)
        if df is None or df.empty:
            logger.error(f"[bot] No data for {asset}")
            return
        engine = BacktestEngine(initial_balance=10000.0)
        result = engine.run(asset, category, df)
        import json
        print(json.dumps(result.to_dict(), indent=2))
    except Exception as e:
        logger.error(f"[bot] Backtest failed: {e}", exc_info=True)


def main() -> None:
    args = parse_args()

    # ── Backtest mode (no live trading) ──────────────────────────────────
    if args.backtest:
        run_backtest(args.backtest, args.backtest_cat)
        return

    # ── Initialise TradingCore ────────────────────────────────────────────
    from core.engine import TradingCore
    engine = TradingCore(
        balance       = args.balance,
        strategy_mode = args.strategy,
        no_telegram   = args.no_telegram,
    )

    # ── Graceful shutdown handler ─────────────────────────────────────────
    def _shutdown(signum, frame):
        logger.info("[bot] Shutdown signal received")
        engine.stop("signal")
        sys.exit(0)

    signal.signal(signal.SIGINT,  _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    # ── Start trading loop ────────────────────────────────────────────────
    engine.start()

    # ── Wire data fetcher to engine ───────────────────────────────────────
    try:
        from data.fetcher import DataFetcher
        engine.fetcher = DataFetcher()
        logger.info("[bot] DataFetcher wired to engine")
    except Exception as e:
        logger.warning(f"[bot] DataFetcher init failed: {e}")

    # ── Start auto-trainer ────────────────────────────────────────────────
    try:
        from ml.trainer import AutoTrainer
        trainer = AutoTrainer(fetcher=engine.fetcher)
        trainer.start()
        logger.info("[bot] AutoTrainer started")
    except Exception as e:
        logger.warning(f"[bot] AutoTrainer failed to start: {e}")

    # ── Telegram (optional) ───────────────────────────────────────────────
    if not args.no_telegram and TELEGRAM_TOKEN and TELEGRAM_CHAT_ID:
        try:
            from telegram_manager import telegram_manager
            engine.telegram = telegram_manager
            logger.info("[bot] Telegram wired")
        except Exception as e:
            logger.warning(f"[bot] Telegram init failed: {e}")

    # ── Wait for engine to be ready ───────────────────────────────────────
    logger.info("[bot] Waiting for engine to be ready...")
    ready = engine.wait_until_ready(timeout=60.0)
    if ready:
        logger.info(f"[bot] Engine ready — balance=${engine.get_balance():.2f}")
    else:
        logger.warning("[bot] Engine did not become ready in 60s — continuing anyway")

    # ── Dashboard ─────────────────────────────────────────────────────────
    if not args.no_dashboard:
        try:
            from dashboard.web_app_live import start_dashboard
            logger.info(f"[bot] Dashboard → http://{args.host}:{args.port}")
            start_dashboard(engine, host=args.host, port=args.port)  # blocking
        except Exception as e:
            logger.error(f"[bot] Dashboard failed: {e}", exc_info=True)
    else:
        logger.info("[bot] Running without dashboard. Ctrl+C to stop.")
        try:
            import time
            while engine.is_running:
                time.sleep(10)
        except KeyboardInterrupt:
            _shutdown(None, None)


if __name__ == "__main__":
    main()