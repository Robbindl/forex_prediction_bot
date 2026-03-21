"""
bot.py — Single entry point for the trading platform.

Startup sequence:
  1. Load config
  2. Init logger
  3. Validate API keys (raises if required keys missing)
  4. Connect to database (required — exits if unavailable)
  5. Init TradingCore
  6. Start trading loop (daemon thread)
  7. Start auto-trainer (daemon thread)
  8. Start Node.js WebSocket gateway (optional)
  9. Start Telegram commander 
 10. Start Flask dashboard (blocking — main thread)
"""
from __future__ import annotations
import argparse
import sys
import signal
import subprocess
import shutil
import socket
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

# ── API key validation (before anything else) ─────────────────────────────────
logger.info("[bot] Validating API keys...")
try:
    from config.api_validation import validate_apis
    validate_apis()
    logger.info("[bot] API validation passed")
except RuntimeError as e:
    
    logger.critical(f"[bot] API validation failed: {e}")
    sys.exit(1)

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
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def start_gateway(force: bool = False) -> subprocess.Popen | None:
    global _gateway_proc

    if not _GATEWAY_DIR.exists():
        logger.warning("[Gateway] gateway/ directory not found — skipping")
        return None

    server_js = _GATEWAY_DIR / "server.js"
    if not server_js.exists():
        logger.warning("[Gateway] gateway/server.js not found — skipping")
        return None

    if _port_open(_GATEWAY_PORT) and not force:
        logger.info(f"[Gateway] Port {_GATEWAY_PORT} already in use — assuming gateway is running")
        return None

    node = shutil.which("node") or shutil.which("node.exe")
    if not node:
        logger.warning(
            "[Gateway] Node.js not found — WebSocket gateway disabled.\n"
            "          Install Node.js from https://nodejs.org to enable it."
        )
        return None

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

    try:
        proc = subprocess.Popen(
            [node, "server.js"],
            cwd=str(_GATEWAY_DIR),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0,
        )
        _gateway_proc = proc

        for _ in range(30):
            time.sleep(0.1)
            if _port_open(_GATEWAY_PORT):
                logger.info(f"[Gateway] Started — ws://localhost:{_GATEWAY_PORT}  (PID {proc.pid})")
                return proc

        if proc.poll() is None:
            logger.info(f"[Gateway] Launched (PID {proc.pid}) — port not yet open")
            return proc

        logger.warning("[Gateway] Process exited immediately — check Redis/Node setup")
        return None

    except Exception as e:
        logger.warning(f"[Gateway] Failed to start: {e}")
        return None


def stop_gateway() -> None:
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


atexit.register(stop_gateway)


def gateway_is_running() -> bool:
    return _port_open(_GATEWAY_PORT)


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Forex/Crypto Prediction Trading Bot")
    p.add_argument("--balance",      type=float, default=DEFAULT_BALANCE)
    p.add_argument("--strategy",     type=str,   default="voting",
                   choices=["voting", "rsi", "macd", "bollinger"])
    p.add_argument("--no-telegram",  action="store_true")
    p.add_argument("--no-dashboard", action="store_true")
    p.add_argument("--no-gateway",   action="store_true")
    p.add_argument("--port",         type=int,   default=5000)
    p.add_argument("--host",         type=str,   default="0.0.0.0")
    p.add_argument("--backtest",     type=str,   default=None)
    p.add_argument("--backtest-cat", type=str,   default="crypto")
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

    if args.backtest:
        run_backtest(args.backtest, args.backtest_cat)
        return

    # ── TradingCore ───────────────────────────────────────────────────────
    from core.engine import TradingCore
    engine = TradingCore(
        balance       = args.balance,
        strategy_mode = args.strategy,
        no_telegram   = args.no_telegram,
    )

    # ── Graceful shutdown ─────────────────────────────────────────────────
    def _shutdown(signum, frame):
        logger.info("[bot] Shutdown signal received")
        stop_gateway()
        engine.stop("signal")
        sys.exit(0)

    signal.signal(signal.SIGINT,  _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    engine.start()

    # ── DataFetcher ───────────────────────────────────────────────────────
    try:
        from data.fetcher import DataFetcher
        engine.fetcher = DataFetcher()
        logger.info("[bot] DataFetcher wired to engine")
    except Exception as e:
        logger.warning(f"[bot] DataFetcher init failed: {e}")

    # ── AutoTrainer ───────────────────────────────────────────────────────
    try:
        from ml.trainer import AutoTrainer
        trainer = AutoTrainer(fetcher=engine.fetcher)
        trainer.start()
        logger.info("[bot] AutoTrainer started")
    except Exception as e:
        logger.warning(f"[bot] AutoTrainer failed to start: {e}")

    # ── Phase 1 — Institutional data feeds ───────────────────────────────
    try:
        from data_ingestion import start_all as start_data_feeds
        start_data_feeds(exchanges=["binance", "bybit"])
        logger.info("[bot] Phase 1 data feeds started")
    except Exception as e:
        logger.warning(f"[bot] Phase 1 data feeds failed to start: {e}")

    # ── Phase 2 — Whale wallet intelligence ──────────────────────────────
    try:
        from whale_intelligence import start_all as start_whale_intelligence
        start_whale_intelligence()
        logger.info("[bot] Phase 2 whale intelligence started")
    except Exception as e:
        logger.warning(f"[bot] Phase 2 whale intelligence failed to start: {e}")

    # ── Phase 3 — Order flow intelligence ────────────────────────────────
    try:
        from order_flow import start_all as start_order_flow
        start_order_flow()
        logger.info("[bot] Phase 3 order flow intelligence started")
    except Exception as e:
        logger.warning(f"[bot] Phase 3 order flow failed to start: {e}")

    # ── Phase 4 — Narrative AI ────────────────────────────────────────────
    try:
        from narrative_ai import get_narrative_scores, get_dominant_narrative
        logger.info("[bot] Phase 4 narrative AI engine ready")
    except Exception as e:
        logger.warning(f"[bot] Phase 4 narrative AI failed to load: {e}")

    # ── Phase 5 — Live strategy bridge ────────────────────────────────────
    try:
        from strategy_lab.live_bridge import list_live_strategies
        active = list_live_strategies()
        if active:
            logger.info(f"[bot] Phase 5 live strategies active: {active}")
        else:
            logger.info("[bot] Phase 5 ready — no lab strategies configured yet")
    except Exception as e:
        logger.warning(f"[bot] Phase 5 live bridge failed to load: {e}")

    # ── Phase 6 — Meta AI ─────────────────────────────────────────────────
    try:
        from ml.meta_model import predictor as meta_predictor  # noqa: F401
        logger.info("[bot] Phase 6 Meta AI engine ready")
    except Exception as e:
        logger.warning(f"[bot] Phase 6 Meta AI failed to load: {e}")

    # ── News event monitor ────────────────────────────────────────────────
    try:
        from data_ingestion.news_event_monitor import start_news_monitor
        start_news_monitor()
        logger.info("[bot] News event monitor started — Finnhub calendar active")
    except Exception as e:
        logger.warning(f"[bot] News event monitor failed to start: {e}")

    # ── Phase 11 — System health monitoring ──────────────────────────────
    try:
        from monitoring import start_monitoring
        logger.info("[bot] Phase 11 system health monitoring ready")
    except Exception as e:
        logger.warning(f"[bot] Phase 11 monitoring failed to load: {e}")

    # ── Portfolio risk engine ─────────────────────────────────────────────
    try:
        from risk.portfolio_risk import PortfolioRiskEngine
        portfolio_risk = PortfolioRiskEngine()
        engine.portfolio_risk = portfolio_risk
        logger.info("[bot] PortfolioRiskEngine attached")
    except Exception as e:
        logger.warning(f"[bot] PortfolioRiskEngine failed: {e}")

    # ── Exchange router + paper adapter ──────────────────────────────────
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

    # ── ML prediction service ─────────────────────────────────────────────
    if not args.no_gateway:
        try:
            ml_proc = subprocess.Popen(
                [sys.executable, "-m", "ml.prediction_service"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0,
            )
            atexit.register(lambda: ml_proc.terminate())
            time.sleep(2)
            from ml.prediction_service import PredictionClient
            if hasattr(engine, '_paper_trader') and engine._paper_trader and hasattr(engine, 'predictor'):
                engine.predictor = PredictionClient()
            logger.info("[bot] ML prediction service started")
        except Exception as e:
            logger.warning(f"[bot] ML service failed to start ({e}) — using in-process predictor")

    # ── Redis cache upgrade ───────────────────────────────────────────────
    try:
        from services.redis_cache import get_cache
        upgraded_cache = get_cache(default_ttl=30)
        import data.cache as _cache_mod
        _cache_mod.cache = upgraded_cache
        logger.info("[bot] Redis cache active")
    except Exception as e:
        logger.debug(f"[bot] Redis cache not available ({e}) — using in-process cache")

    # ── Node.js WebSocket gateway ─────────────────────────────────────────
    if not args.no_gateway:
        start_gateway()
    else:
        logger.info("[bot] Gateway disabled via --no-gateway")

    # ── Telegram ──────────────────────────────────────────────────────────
    #
    # TWO-BOT ARCHITECTURE:
    #
    #   Bot 1 — Command Bot (TelegramCommander, polling)
    #     Receives: trade open/close alerts, pipeline signal journals,
    #               daily loss limit alerts.
    #     Handles:  /menu /signal /ask /close /pause /resume commands.
    #     Why Bot 1: these messages are immediately actionable — you tap
    #               a button directly after seeing a trade alert.
    #
    #   Bot 2 — Intelligence Bot (IntelligenceBot, send-only via requests)
    #     Receives: Phase 7 market intelligence alerts (whale accumulation,
    #               liquidation cascades, order flow, narrative trends).
    #               Phase 11 system health alerts (CPU, RAM, pipeline
    #               latency, stale data sources).
    #     Why Bot 2: passive information — no commands needed, no buttons,
    #               no polling. Raw requests.post, zero conflict risk.
    #
    # Bot 2 is always started regardless of whether Bot 1 starts, because
    # it has its own token and does not depend on Bot 1 in any way.
    # ──────────────────────────────────────────────────────────────────────

    # ── Bot 2 — Intelligence Bot (always started first, no polling) ───────
    _intel_bot = None
    try:
        from intelligence_bot import intelligence_bot as _intel_bot
        if _intel_bot.is_ready:
            logger.info("[bot] Intelligence Bot (Bot 2) ready — send-only")
        else:
            logger.warning("[bot] Intelligence Bot (Bot 2) not ready — check WHALE_TELEGRAM_TOKEN in .env")
    except Exception as e:
        logger.warning(f"[bot] Intelligence Bot init failed: {e}")

    # Phase 7 and Phase 11 always go to Bot 2
    try:
        from services.intelligence_alerts import start_all as start_intel_alerts
        start_intel_alerts(telegram_bot=_intel_bot)
        logger.info("[bot] Phase 7 intelligence alerts → Bot 2")
    except Exception as e:
        logger.warning(f"[bot] Phase 7 intelligence alerts failed: {e}")

    try:
        from monitoring import start_monitoring
        start_monitoring(telegram_bot=_intel_bot)
        logger.info("[bot] Phase 11 monitoring → Bot 2")
    except Exception as e:
        logger.warning(f"[bot] Phase 11 monitoring failed: {e}")

    # ── Bot 1 — Command Bot (polling, interactive) ────────────────────────
    if not args.no_telegram and TELEGRAM_TOKEN and TELEGRAM_CHAT_ID:
        try:
            from telegram_manager import telegram_manager
            started = telegram_manager.start(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, engine)
            if started:
                # Trade alerts and pipeline journals → Bot 1 only
                engine.telegram = telegram_manager.bot
                try:
                    from core.pipeline_reporter import reporter
                    reporter.wire_telegram(telegram_manager.bot)
                    logger.info("[bot] PipelineReporter → Bot 1")
                except Exception as e:
                    logger.warning(f"[bot] PipelineReporter Telegram wire failed: {e}")
                logger.info("[bot] Command Bot (Bot 1) started and wired to engine")
            else:
                logger.warning("[bot] Command Bot (Bot 1) not started — duplicate instance or missing creds")
        except Exception as e:
            logger.warning(f"[bot] Command Bot init failed: {e}")

    # ── Wait for engine ───────────────────────────────────────────────────
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
        from core.asset_profiles import is_crypto

        _whale_mgr = WhaleAlertManager()

        _SYMBOL_MAP = {
            "BTC":    "BTC-USD", "BITCOIN":   "BTC-USD",
            "ETH":    "ETH-USD", "ETHEREUM":  "ETH-USD",
            "BNB":    "BNB-USD", "SOL":       "SOL-USD",
            "XRP":    "XRP-USD", "RIPPLE":    "XRP-USD",
        }

        def _symbol_to_asset(symbol: str) -> str:
            return _SYMBOL_MAP.get(symbol.upper(), "")

        def _on_whale_alert(alert: dict) -> None:
            try:
                symbol = str(alert.get("symbol", alert.get("asset", ""))).upper().strip()
                asset  = _symbol_to_asset(symbol)
                if not asset or not is_crypto(asset):
                    return   # only crypto whale data is valid

                sentiment = float(alert.get("sentiment", 0.1))
                direction = "BUY" if sentiment >= 0.0 else "SELL"
                size_usd  = float(alert.get("value_usd", alert.get("usd_amount", 0)))
                if size_usd < 500_000:
                    return

                ingest_whale_alert(
                    asset=asset,
                    direction=direction,
                    size_usd=size_usd,
                    source=alert.get("source", "whale_alert"),
                    sentiment=sentiment,
                )
            except Exception as e:
                logger.error(f"[bot] on_whale_alert callback error: {e}")

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
