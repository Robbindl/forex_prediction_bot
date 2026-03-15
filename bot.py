"""
bot.py — Single entry point for the trading platform.

Startup sequence:
  1. Load config
  2. Init logger
  3. Connect to database (required — exits if unavailable)
  4. Init TradingCore
  5. Start trading loop (daemon thread)
  6. Start auto-trainer (daemon thread)
  7. Start Node.js WebSocket gateway (optional — requires node)
  8. Start Telegram commander (optional)
  9. Start Flask dashboard (blocking — main thread)
"""
from __future__ import annotations
import argparse
import sys
import signal
import subprocess
import shutil
import socket
import threading
import time
import atexit
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


# ── Gateway management ────────────────────────────────────────────────────────

_gateway_proc: subprocess.Popen | None = None
_GATEWAY_DIR  = Path(__file__).parent / "gateway"
_GATEWAY_PORT = 8081


def _port_open(port: int, host: str = "127.0.0.1", timeout: float = 0.3) -> bool:
    """Return True if something is already listening on the port."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def start_gateway(force: bool = False) -> subprocess.Popen | None:
    """
    Start the Node.js WebSocket gateway as a background subprocess.

    Steps:
      1. Check node is installed.
      2. Install npm dependencies if node_modules is missing.
      3. Spawn 'node server.js' and return the process handle.

    Returns None (with a warning) if node is not found, deps fail,
    or the port is already in use.
    """
    global _gateway_proc

    if not _GATEWAY_DIR.exists():
        logger.warning("[Gateway] gateway/ directory not found — skipping")
        return None

    server_js = _GATEWAY_DIR / "server.js"
    if not server_js.exists():
        logger.warning("[Gateway] gateway/server.js not found — skipping")
        return None

    # Already running?
    if _port_open(_GATEWAY_PORT) and not force:
        logger.info(f"[Gateway] Port {_GATEWAY_PORT} already in use — assuming gateway is running")
        return None

    # Find node executable (Windows uses 'node.exe', Linux/Mac 'node')
    node = shutil.which("node") or shutil.which("node.exe")
    if not node:
        logger.warning(
            "[Gateway] Node.js not found — WebSocket gateway disabled.\n"
            "          Install Node.js from https://nodejs.org to enable it."
        )
        return None

    # Install npm dependencies if missing
    node_modules = _GATEWAY_DIR / "node_modules"
    if not node_modules.exists():
        npm = shutil.which("npm") or shutil.which("npm.cmd")
        if not npm:
            logger.warning("[Gateway] npm not found — cannot install dependencies")
            return None
        logger.info("[Gateway] node_modules missing — running npm install...")
        try:
            result = subprocess.run(
                [npm, "install"],
                cwd=str(_GATEWAY_DIR),
                capture_output=True,
                text=True,
                timeout=120,
            )
            if result.returncode != 0:
                logger.warning(f"[Gateway] npm install failed:\n{result.stderr[:300]}")
                return None
            logger.info("[Gateway] npm install complete")
        except subprocess.TimeoutExpired:
            logger.warning("[Gateway] npm install timed out after 120s")
            return None
        except Exception as e:
            logger.warning(f"[Gateway] npm install error: {e}")
            return None

    # Launch the gateway process
    try:
        proc = subprocess.Popen(
            [node, "server.js"],
            cwd=str(_GATEWAY_DIR),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            # On Windows, prevent the process from showing a console window
            creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0,
        )
        _gateway_proc = proc

        # Give it up to 3 seconds to become available
        for _ in range(30):
            time.sleep(0.1)
            if _port_open(_GATEWAY_PORT):
                logger.info(
                    f"[Gateway] Started — ws://localhost:{_GATEWAY_PORT}  (PID {proc.pid})"
                )
                return proc

        # Port didn't open but process is running — still return it
        if proc.poll() is None:
            logger.info(f"[Gateway] Launched (PID {proc.pid}) — port not yet open")
            return proc

        logger.warning("[Gateway] Process exited immediately — check Redis/Node setup")
        return None

    except Exception as e:
        logger.warning(f"[Gateway] Failed to start: {e}")
        return None


def stop_gateway() -> None:
    """Terminate the gateway subprocess on shutdown."""
    global _gateway_proc
    if _gateway_proc and _gateway_proc.poll() is None:
        logger.info(f"[Gateway] Stopping (PID {_gateway_proc.pid})...")
        try:
            _gateway_proc.terminate()
            _gateway_proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            _gateway_proc.kill()
        except Exception:
            pass
        _gateway_proc = None
        logger.info("[Gateway] Stopped")


# Register gateway cleanup on normal exit
atexit.register(stop_gateway)


def gateway_is_running() -> bool:
    """Used by the dashboard /api/gateway/status endpoint."""
    return _port_open(_GATEWAY_PORT)


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Forex/Crypto Prediction Trading Bot")
    p.add_argument("--balance",      type=float, default=DEFAULT_BALANCE,
                   help=f"Starting balance (default: {DEFAULT_BALANCE})")
    p.add_argument("--strategy",     type=str,   default="voting",
                   choices=["voting", "rsi", "macd", "bollinger"],
                   help="Strategy mode")
    p.add_argument("--no-telegram",  action="store_true", help="Disable Telegram")
    p.add_argument("--no-dashboard", action="store_true", help="Disable web dashboard")
    p.add_argument("--no-gateway",   action="store_true", help="Disable Node.js WebSocket gateway")
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
        stop_gateway()
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

    # ── Portfolio risk engine ─────────────────────────────────────────────────────
    try:
        from risk.portfolio_risk import PortfolioRiskEngine
        portfolio_risk = PortfolioRiskEngine()
        engine.portfolio_risk = portfolio_risk
        logger.info("[bot] PortfolioRiskEngine attached")
    except Exception as e:
        logger.warning(f"[bot] PortfolioRiskEngine failed: {e}")

    # ── Exchange router + paper adapter ──────────────────────────────────────────
    try:
        from execution.exchange_router import ExchangeRouter
        from execution.paper_adapter   import PaperAdapter
        router = ExchangeRouter()
        if hasattr(engine, '_paper_trader') and engine._paper_trader:
            router.register("paper", PaperAdapter(engine._paper_trader))
        engine.exchange_router = router
        logger.info("[bot] ExchangeRouter ready — paper adapter registered")
    except Exception as e:
        logger.warning(f"[bot] ExchangeRouter failed: {e}")

    # ── ML prediction service (optional separate process) ────────────────────────
    if not args.no_gateway:
        try:
            import subprocess, sys
            ml_proc = subprocess.Popen(
                [sys.executable, "-m", "ml.prediction_service"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0,
            )
            atexit.register(lambda: ml_proc.terminate())
            # Give it 2s to start then switch the engine predictor to client
            time.sleep(2)
            from ml.prediction_service import PredictionClient
            if hasattr(engine, '_paper_trader') and engine._paper_trader and hasattr(engine, 'predictor'):
                engine.predictor = PredictionClient()
            logger.info("[bot] ML prediction service started")
        except Exception as e:
            logger.warning(f"[bot] ML service failed to start ({e}) — using in-process predictor")

    # ── Redis cache upgrade (if Redis available) ──────────────────────────────────
    try:
        from services.redis_cache import get_cache
        upgraded_cache = get_cache(default_ttl=30)
        import data.cache as _cache_mod
        _cache_mod.cache = upgraded_cache
        logger.info("[bot] Redis cache active")
    except Exception as e:
        logger.debug(f"[bot] Redis cache not available ({e}) — using in-process cache")

    # ── Node.js WebSocket gateway (optional) ─────────────────────────────
    if not args.no_gateway:
        start_gateway()
    else:
        logger.info("[bot] Gateway disabled via --no-gateway")

    # ── Telegram (optional) ───────────────────────────────────────────────
    if not args.no_telegram and TELEGRAM_TOKEN and TELEGRAM_CHAT_ID:
        try:
            from telegram_manager import telegram_manager
            started = telegram_manager.start(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, engine)
            if started:
                # Wire the inner TelegramCommander so engine can call alert methods
                engine.telegram = telegram_manager.bot
                logger.info("[bot] Telegram started and wired to engine")
            else:
                logger.warning("[bot] Telegram not started (duplicate instance or missing creds)")
        except Exception as e:
            logger.warning(f"[bot] Telegram init failed: {e}")

    # ── Wait for engine to be ready ───────────────────────────────────────
    logger.info("[bot] Waiting for engine to be ready...")
    ready = engine.wait_until_ready(timeout=60.0)
    if ready:
        logger.info(f"[bot] Engine ready — balance=${engine.get_balance():.2f}")
    else:
        logger.warning("[bot] Engine did not become ready in 60s — continuing anyway")

    # ── Whale monitoring ──────────────────────────────────────────────────
    try:
        from whale_alert_manager import WhaleAlertManager
        from layers.layer6_whale import ingest_whale_alert

        _whale_mgr = WhaleAlertManager()

        def _symbol_to_asset(symbol: str) -> str:
            """Map raw whale symbol (e.g. 'BTC') to canonical asset ID (e.g. 'BTC-USD')."""
            _MAP = {
                "BTC":    "BTC-USD",  "BITCOIN":   "BTC-USD",
                "ETH":    "ETH-USD",  "ETHEREUM":  "ETH-USD",
                "BNB":    "BNB-USD",  "SOL":       "SOL-USD",
                "XRP":    "XRP-USD",  "ADA":       "ADA-USD",
                "DOGE":   "DOGE-USD", "DOT":       "DOT-USD",
                "LTC":    "LTC-USD",  "AVAX":      "AVAX-USD",
                "LINK":   "LINK-USD",
                "GOLD":   "XAU/USD",  "XAU":       "XAU/USD",
                "SILVER": "XAG/USD",  "XAG":       "XAG/USD",
                "OIL":    "WTI/USD",  "WTI":       "WTI/USD",
                "EUR":    "EUR/USD",  "GBP":       "GBP/USD",
                "JPY":    "USD/JPY",  "USDT":      "BTC-USD",
            }
            return _MAP.get(symbol.upper(), "")

        def _on_whale_alert(alert: dict) -> None:
            """Bridge: WhaleAlertManager collector → Layer 6 pipeline cache."""
            try:
                symbol   = str(alert.get("symbol", alert.get("asset", ""))).upper().strip()
                asset    = _symbol_to_asset(symbol)
                if not asset:
                    return

                sentiment = float(alert.get("sentiment", 0.1))
                direction = "BUY" if sentiment >= 0.0 else "SELL"

                size_usd = float(alert.get("value_usd", alert.get("usd_amount", 0)))
                if size_usd < 500_000:
                    return

                ingest_whale_alert(
                    asset=asset,
                    direction=direction,
                    size_usd=size_usd,
                    source=alert.get("source", "whale_alert"),
                )
            except Exception:
                pass

        _whale_mgr.on_alert = _on_whale_alert
        _whale_mgr.start_monitoring()
        logger.info("[bot] WhaleAlertManager started — Layer 6 whale cache active")
    except Exception as e:
        logger.warning(f"[bot] WhaleAlertManager failed to start: {e}")

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
            while engine.is_running:
                time.sleep(10)
        except KeyboardInterrupt:
            _shutdown(None, None)


if __name__ == "__main__":
    main()     