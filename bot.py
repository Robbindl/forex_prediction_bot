from __future__ import annotations
import argparse
import os
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
    LOG_RETENTION_DAYS, LOG_BACKUP_COUNT, ML_SERVICE_LOG_MAX_BYTES,
    AUTO_RESEARCH_ALLOW_IN_BOT_RUNTIME,
    AUTO_RESEARCH_ALLOW_SEPARATE_WORKER,
    LIVE_APPROVED_REGISTRY_ONLY,
)
from utils.logger import TradingLogger, get_logger, get_rotating_file_logger, prune_stale_log_artifacts

_trading_logger = TradingLogger(log_dir=str(LOG_DIR), level=LOG_LEVEL)
logger = get_logger()

try:
    _removed_logs = prune_stale_log_artifacts(LOG_DIR, retention_days=LOG_RETENTION_DAYS)
    if _removed_logs:
        logger.info(f"[bot] Pruned {_removed_logs} stale log artifacts from {LOG_DIR}")
except Exception as e:
    logger.debug(f"[bot] Log cleanup skipped: {e}")

_DEFAULT_HTTP2_CERT = Path("cert.pem")
_DEFAULT_HTTP2_KEY = Path("key.pem")

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

# ── Check optional API keys ─────────────────────────────────────────────────
from config.config import WHALE_ALERT_KEY, WHALE_TELEGRAM_TOKEN, FRED_API_KEY
if not WHALE_ALERT_KEY:
    logger.warning("[bot] WHALE_ALERT_KEY not set — authenticated whale API enrichment disabled")
if not WHALE_TELEGRAM_TOKEN:
    logger.warning("[bot] WHALE_TELEGRAM_TOKEN not set — intelligence alerts disabled")
if not FRED_API_KEY:
    logger.warning("[bot] FRED_API_KEY not set — macro data collection disabled")

# ── Database (required) ───────────────────────────────────────────────────────
logger.info("[bot] Connecting to database...")
try:
    from config.database import init_db
    init_db()
    logger.info("[bot] Database ready")
except RuntimeError as e:
    logger.critical(str(e))
    sys.exit(1)

# ── System state (after DB ready) ─────────────────────────────────────────────
logger.info("[bot] Loading system state...")
try:
    from core.state import state
    state.init_db()
    logger.info("[bot] System state loaded from DB")
except Exception as e:
    logger.critical(f"[bot] Failed to load system state: {e}")
    sys.exit(1)


# ── Gateway management ────────────────────────────────────────────────────────

_gateway_proc: subprocess.Popen | None = None
_auto_research_scheduler = None
_auto_research_worker_proc: subprocess.Popen | None = None
_shutdown_started = threading.Event()
_shutdown_lock = threading.Lock()
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


def stop_telegram() -> None:
    try:
        from telegram_manager import telegram_manager

        bot = getattr(telegram_manager, "bot", None)
        if bot is not None:
            try:
                bot.stop()
            except Exception as e:
                logger.debug(f"[bot] TelegramCommander stop failed: {e}")

        telegram_manager.cleanup()
    except Exception as e:
        logger.debug(f"[bot] Telegram shutdown skipped: {e}")


def stop_auto_research() -> None:
    global _auto_research_scheduler, _auto_research_worker_proc
    try:
        if _auto_research_scheduler is not None:
            _auto_research_scheduler.stop()
            _auto_research_scheduler = None
        if _auto_research_worker_proc and _auto_research_worker_proc.poll() is None:
            logger.info(f"[bot] Stopping auto research worker (PID {_auto_research_worker_proc.pid})...")
            try:
                _auto_research_worker_proc.terminate()
                _auto_research_worker_proc.wait(timeout=8)
            except subprocess.TimeoutExpired:
                _auto_research_worker_proc.kill()
            except Exception:
                pass
            _auto_research_worker_proc = None
    except Exception as e:
        logger.debug(f"[bot] Auto research shutdown skipped: {e}")


def _perform_shutdown(engine, *, reason: str = "signal", exit_code: int = 0) -> None:
    with _shutdown_lock:
        logger.info(f"[bot] Shutdown sequence started — reason={reason}")
        try:
            stop_auto_research()
        except Exception as e:
            logger.debug(f"[bot] Auto research stop during shutdown failed: {e}")
        try:
            stop_telegram()
        except Exception as e:
            logger.debug(f"[bot] Telegram stop during shutdown failed: {e}")
        try:
            stop_gateway()
        except Exception as e:
            logger.debug(f"[bot] Gateway stop during shutdown failed: {e}")
        try:
            engine.stop(reason)
        except Exception as e:
            logger.debug(f"[bot] Engine stop during shutdown failed: {e}")
        logger.info("[bot] Shutdown complete")
    os._exit(exit_code)


def start_auto_research_worker() -> subprocess.Popen | None:
    global _auto_research_worker_proc

    if _auto_research_worker_proc and _auto_research_worker_proc.poll() is None:
        return _auto_research_worker_proc

    try:
        from strategy_lab.auto_research import load_auto_research_settings
        from strategy_lab.auto_research_runtime import (
            build_auto_research_worker_command,
            should_start_separate_auto_research_worker,
        )

        settings = load_auto_research_settings()
        if not should_start_separate_auto_research_worker(settings):
            return None

        command = build_auto_research_worker_command(sys.executable)
        proc = subprocess.Popen(
            command,
            cwd=str(Path(__file__).parent),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0,
        )
        _auto_research_worker_proc = proc
        logger.info(
            "[bot] Auto strategy research worker started "
            f"(PID {proc.pid}, startup_delay={int(settings.get('startup_delay_seconds', 0) or 0)}s, "
            f"interval_hours={float(settings.get('interval_hours', 24.0) or 24.0):.1f})"
        )
        return proc
    except Exception as e:
        logger.warning(f"[bot] Auto research worker failed to start: {e}")
        return None


atexit.register(stop_gateway)
atexit.register(stop_telegram)
atexit.register(stop_auto_research)


def gateway_is_running() -> bool:
    return _port_open(_GATEWAY_PORT)


# ── CLI ───────────────────────────────────────────────────────────────────────

def _resolve_tls_certificates(cert: str | None, key: str | None) -> tuple[str | None, str | None]:
    if cert is None and key is None:
        default_cert = _DEFAULT_HTTP2_CERT
        default_key = _DEFAULT_HTTP2_KEY
        if default_cert.exists() and default_key.exists():
            return str(default_cert), str(default_key)

        if default_cert.exists() != default_key.exists():
            try:
                default_cert.unlink(missing_ok=True)
                default_key.unlink(missing_ok=True)
            except Exception:
                pass

        try:
            from generate_local_cert import generate_certificate
            generate_certificate(default_cert, default_key, common_name="localhost", san=["localhost", "127.0.0.1"], days=365)
            return str(default_cert), str(default_key)
        except Exception as exc:
            logger.warning(f"[bot] TLS certificate generation failed: {exc}")
            return None, None

    if cert is None or key is None:
        raise RuntimeError("Both --ssl-cert and --ssl-key must be provided together, or neither.")

    cert_path = Path(cert)
    key_path = Path(key)
    if not cert_path.exists() or not key_path.exists():
        raise FileNotFoundError(f"TLS files not found: cert={cert_path}, key={key_path}")
    return str(cert_path), str(key_path)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Forex/Crypto Prediction Trading Bot")
    p.add_argument("--balance",      type=float, default=DEFAULT_BALANCE)
    p.add_argument("--no-telegram",  action="store_true")
    p.add_argument("--no-dashboard", action="store_true")
    p.add_argument("--no-gateway",   action="store_true")
    p.add_argument("--no-ml-service", action="store_true")
    p.add_argument("--port",         type=int,   default=5000)
    p.add_argument("--host",         type=str,   default="0.0.0.0")
    p.add_argument("--http2", action="store_true", help="Enable HTTP/2 server if available")
    p.add_argument("--ssl-cert", type=str,   default=None, help="Path to TLS certificate for HTTPS / HTTP/2 (default: generated cert.pem)")
    p.add_argument("--ssl-key",  type=str,   default=None, help="Path to TLS private key for HTTPS / HTTP/2 (default: generated key.pem)")
    p.add_argument("--backtest",     type=str,   default=None)
    p.add_argument("--backtest-cat", type=str,   default="crypto")
    p.add_argument("--backtest-strategy", type=str, default="ema_rsi_crossover")
    return p.parse_args()


def run_backtest(asset: str, category: str, strategy_name: str = "ema_rsi_crossover") -> None:
    logger.info(f"[bot] Running lab backtest: {asset} ({category}) via {strategy_name}")
    try:
        from strategy_lab import (
            StrategyBuilder,
            resolve_backtest_end_time,
            resolve_backtest_periods,
            run_backtest as run_lab_backtest,
        )

        configs = StrategyBuilder.all_configs()
        if strategy_name not in configs:
            logger.error(
                f"[bot] Unknown backtest strategy '{strategy_name}'. "
                f"Available active presets: {sorted(configs.keys())}"
            )
            return

        periods = resolve_backtest_periods(category)
        snapshot_end = resolve_backtest_end_time(category)
        result = run_lab_backtest(
            configs[strategy_name],
            asset,
            category,
            initial_balance=10000.0,
            periods=periods,
            end_time=snapshot_end,
        )
        import json
        payload = result.to_dict()
        payload["strategy"] = strategy_name
        payload["snapshot_end_utc"] = snapshot_end.isoformat()
        print(json.dumps(payload, indent=2))
    except Exception as e:
        logger.error(f"[bot] Backtest failed: {e}", exc_info=True)


def main() -> None:
    args = parse_args()

    if args.backtest:
        run_backtest(args.backtest, args.backtest_cat, args.backtest_strategy)
        return

    os.environ["BOT_LIVE_RUNTIME"] = "1"

    # ── TradingCore ───────────────────────────────────────────────────────
    from core.engine import TradingCore
    engine = TradingCore(
        balance      = args.balance,
        no_telegram  = args.no_telegram,
    )

    # Register engine singleton for cross-module access as early as possible.
    try:
        import core.engine as _eng_mod
        _eng_mod._CORE_INSTANCE = engine
    except Exception:
        pass

    # ── Graceful shutdown ─────────────────────────────────────────────────
    def _shutdown(signum, frame):
        if _shutdown_started.is_set():
            logger.warning("[bot] Forced shutdown requested — exiting immediately")
            os._exit(130)
        _shutdown_started.set()
        signal_name = "manual"
        try:
            if signum is not None:
                signal_name = signal.Signals(signum).name
        except Exception:
            signal_name = str(signum) if signum is not None else "manual"
        logger.info(
            f"[bot] Shutdown signal received ({signal_name}) — stopping services. "
            "Press Ctrl+C again to force exit."
        )
        threading.Thread(
            target=_perform_shutdown,
            args=(engine,),
            kwargs={"reason": signal_name.lower(), "exit_code": 0 if signum is None else 128 + int(signum)},
            name="BotShutdown",
            daemon=True,
        ).start()

    signal.signal(signal.SIGINT,  _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    engine.start()

    # ── API key expiry notifications ──────────────────────────────────────
    try:
        import threading, datetime as _dt
        def _check_api_expiry():
            # Load from config (not hardcoded)
            expiry_alerts = []
            try:
                from config.config import API_KEY_EXPIRY_DATES
                if API_KEY_EXPIRY_DATES:
                    expiry_alerts = list(API_KEY_EXPIRY_DATES.items())
            except (ImportError, AttributeError):
                logger.debug("[bot] API_KEY_EXPIRY_DATES not configured")
                pass
            
            while True:
                try:
                    tc = engine
                    today = _dt.date.today()
                    if tc and hasattr(tc, "telegram") and tc.telegram and expiry_alerts and _check_api_expiry.last_checked != today:
                        today = _dt.date.today()
                        for name, exp_date in expiry_alerts:
                            days_left = (exp_date - today).days
                            if days_left in (7, 3, 1, 0, -1):
                                if days_left < 0:
                                    msg = (
                                        f"🔴 *API KEY EXPIRED*\n\n"
                                        f"*{name}* expired {abs(days_left)} day{'s' if abs(days_left) != 1 else ''} ago.\n"
                                        f"Renew immediately to avoid data gaps."
                                    )
                                else:
                                    msg = (
                                        f"⚠️ *API Key Expiry Alert*\n\n"
                                        f"*{name}* expires in *{days_left} day{'s' if days_left != 1 else ''}* "
                                        f"({exp_date.strftime('%B %d, %Y')}).\n\n"
                                        f"{'🚨 Renew immediately!' if days_left == 0 else 'Please renew soon.'}"
                                    )
                                tc.telegram.send_message(msg)
                        _check_api_expiry.last_checked = today
                except Exception as e:
                    logger.debug(f"[APIExpiryChecker] error: {e}")
                # Retry until the first successful daily check, then back off.
                import time
                sleep_seconds = 86400 if _check_api_expiry.last_checked == _dt.date.today() else 300
                time.sleep(sleep_seconds)
        _check_api_expiry.last_checked = None
        threading.Thread(target=_check_api_expiry, name="APIExpiryChecker", daemon=True).start()
        logger.info("[bot] API expiry checker started")
    except Exception as e:
        logger.warning(f"[bot] API expiry checker failed: {e}")

    # DataFetcher check moved to after wait_until_ready — see below

    # AutoTrainer moved to after wait_until_ready — see below
    # (engine.fetcher is None at this point so trainer would get no data)

    # ── Data feeds ────────────────────────────────────────────────────────
    try:
        from data_ingestion import start_all as start_data_feeds
        start_data_feeds(exchanges=["binance", "bybit"])
        logger.info("[bot] Data feeds started")
    except Exception as e:
        logger.warning(f"[bot] Data feeds failed to start: {e}")

    # ── Whale wallet intelligence ────────────────────────────────────────
    try:
        from whale_intelligence import start_all as start_whale_intelligence
        start_whale_intelligence()
        logger.info("[bot] Whale wallet intelligence started")
    except Exception as e:
        logger.warning(f"[bot] Whale wallet intelligence failed to start: {e}")

    # ── Order flow intelligence ───────────────────────────────────────────
    try:
        from order_flow import start_all as start_order_flow
        start_order_flow()
        logger.info("[bot] Order flow intelligence started")
    except Exception as e:
        logger.warning(f"[bot] Order flow intelligence failed to start: {e}")

    # ── Narrative AI ──────────────────────────────────────────────────────
    try:
        from narrative_ai import get_narrative_scores, get_dominant_narrative
        logger.info("[bot] Narrative AI engine ready")
    except Exception as e:
        logger.warning(f"[bot] Narrative AI failed to load: {e}")

    # ── Live strategy bridge ──────────────────────────────────────────────
    try:
        from strategy_lab.live_bridge import list_live_strategies
        active = list_live_strategies()
        if active:
            logger.info(f"[bot] Live strategies active: {active}")
        else:
            logger.info("[bot] Live strategy bridge ready — no lab strategies configured yet")
            if LIVE_APPROVED_REGISTRY_ONLY:
                logger.warning(
                    "[bot] No approved live strategies in registry — governance will run in bootstrap mode "
                    "until at least one strategy is promoted"
                )
    except Exception as e:
        logger.warning(f"[bot] Live strategy bridge failed to load: {e}")

    # ── Meta AI ───────────────────────────────────────────────────────────
    try:
        from ml.meta_model import predictor as meta_predictor  # noqa: F401
        logger.info("[bot] Meta AI engine ready")
    except Exception as e:
        logger.warning(f"[bot] Meta AI failed to load: {e}")

    # ── News event monitor ────────────────────────────────────────────────
    try:
        from data_ingestion.news_event_monitor import start_news_monitor
        start_news_monitor()
        logger.info("[bot] News event monitor started")
    except Exception as e:
        logger.warning(f"[bot] News event monitor failed to start: {e}")

    # ── System health monitoring ──────────────────────────────────────────
    try:
        from monitoring import start_monitoring
        logger.info("[bot] System health monitoring module loaded")
    except Exception as e:
        logger.warning(f"[bot] System health monitoring failed to load: {e}")

    # ── Portfolio risk engine ─────────────────────────────────────────────
    try:
        from risk.portfolio_risk import PortfolioRiskEngine
        portfolio_risk = PortfolioRiskEngine()
        engine._portfolio_risk = portfolio_risk
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
    if not args.no_ml_service:
        try:
            def _relay_ml_output(pipe) -> None:
                if pipe is None:
                    return
                try:
                    for raw in pipe:
                        line = str(raw or "").rstrip()
                        if line:
                            _ml_service_logger.info(line)
                except Exception as relay_error:
                    logger.debug(f"[bot] ML log relay stopped: {relay_error}")
                finally:
                    try:
                        pipe.close()
                    except Exception:
                        pass

            _ml_service_logger = get_rotating_file_logger(
                "ml_prediction_service",
                LOG_DIR / "ml_prediction_service.log",
                max_bytes=ML_SERVICE_LOG_MAX_BYTES,
                backup_count=LOG_BACKUP_COUNT,
            )
            from ml.prediction_service import PredictionClient, is_service_healthy, wait_for_service

            if is_service_healthy():
                ml_proc = None
                logger.info("[bot] ML prediction service already running — reusing existing local daemon")
            else:
                ml_proc = subprocess.Popen(
                    [sys.executable, "-m", "ml.prediction_service"],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    bufsize=1,
                    creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0,
                )
                threading.Thread(
                    target=_relay_ml_output,
                    args=(ml_proc.stdout,),
                    daemon=True,
                ).start()
                atexit.register(lambda: ml_proc.terminate())
                if not wait_for_service(timeout_sec=15.0):
                    raise RuntimeError("prediction service did not become healthy")

            # FIX S23: engine._predictor is the attribute TradingCore reads in
            # _generate_signals(). Previously bot.py wrote to engine.predictor
            # (no underscore) which TradingCore never reads — the subprocess
            # started successfully but was never queried.
            if hasattr(engine, '_predictor'):
                engine._predictor = PredictionClient()
            if ml_proc is None:
                logger.info("[bot] ML prediction client attached to existing local daemon")
            else:
                logger.info("[bot] ML prediction service started")
        except Exception as e:
            logger.warning(f"[bot] ML service failed to start ({e}) — using in-process predictor")
    else:
        logger.info("[bot] ML prediction service disabled via --no-ml-service")

    # ── Redis cache upgrade ───────────────────────────────────────────────
    try:
        from config.config import CACHE_TTL
        from services.redis_cache import get_cache
        upgraded_cache = get_cache(default_ttl=CACHE_TTL)
        import data.cache as _cache_mod
        _cache_mod.cache = upgraded_cache
        logger.info("[bot] Redis shared cache active (market-data cache remains local)")
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
    #     Receives: trade open/close alerts, signal journals,
    #               daily loss limit alerts.
    #     Handles:  /menu /signal /ask /close /pause /resume commands.
    #     Why Bot 1: these messages are immediately actionable — you tap
    #               a button directly after seeing a trade alert.
    #
    #   Bot 2 — Intelligence Bot (IntelligenceBot, send-only via requests)
    #     Receives: market intelligence alerts (whale accumulation,
    #               liquidation cascades, order flow, narrative trends).
    #               system health alerts (CPU, RAM, decision
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

    # Market intelligence and system health always go to Bot 2
    try:
        from services.intelligence_alerts import start_all as start_intel_alerts
        start_intel_alerts(telegram_bot=_intel_bot)
        logger.info("[bot] Market intelligence alerts → Bot 2")
    except Exception as e:
        logger.warning(f"[bot] Market intelligence alerts failed: {e}")

    try:
        from monitoring import start_monitoring
        start_monitoring(telegram_bot=_intel_bot)
        logger.info("[bot] System health monitoring → Bot 2")
    except Exception as e:
        logger.warning(f"[bot] System health monitoring failed: {e}")

    # ── Bot 1 — Command Bot (polling, interactive) ────────────────────────
    if not args.no_telegram and TELEGRAM_TOKEN and TELEGRAM_CHAT_ID:
        try:
            from telegram_manager import telegram_manager
            started = telegram_manager.start(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, engine)
            if started:
                # Trade alerts and signal journals → Bot 1 only
                engine.telegram = telegram_manager.bot
                try:
                    from core.signal_reporter import reporter
                    reporter.wire_telegram(telegram_manager.bot)
                    logger.info("[bot] SignalReporter → Bot 1")
                except Exception as e:
                    logger.warning(f"[bot] SignalReporter Telegram wire failed: {e}")
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

    # ── DataFetcher ───────────────────────────────────────────────────────
    # Checked AFTER wait_until_ready so engine._init_subsystems() has
    # completed and engine.fetcher is guaranteed to exist if init succeeded.
    if engine.fetcher:
        logger.info("[bot] DataFetcher ready (reusing engine singleton)")
    else:
        try:
            from data.fetcher import DataFetcher
            engine.fetcher = DataFetcher()
            logger.info("[bot] DataFetcher created (engine singleton was None)")
        except Exception as e:
            logger.warning(f"[bot] DataFetcher init failed: {e}")

    # ── AutoTrainer ───────────────────────────────────────────────────────
    # Started AFTER wait_until_ready and DataFetcher confirmed — engine.fetcher
    # is guaranteed non-None here so training data fetch will succeed.
    try:
        from ml.trainer import AutoTrainer
        trainer = AutoTrainer(fetcher=engine.fetcher)
        engine._trainer = trainer
        engine.trainer = trainer
        trainer.start()
        logger.info("[bot] AutoTrainer started")
    except Exception as e:
        logger.warning(f"[bot] AutoTrainer failed to start: {e}")

    # ── Automatic strategy research / promotion ──────────────────────────
    global _auto_research_scheduler
    try:
        from strategy_lab.auto_research import load_auto_research_settings, start_auto_research_scheduler
        from strategy_lab.auto_research_runtime import should_start_separate_auto_research_worker

        auto_research_settings = load_auto_research_settings()
        if auto_research_settings.get("enabled") and AUTO_RESEARCH_ALLOW_IN_BOT_RUNTIME:
            _auto_research_scheduler = start_auto_research_scheduler()
            if _auto_research_scheduler is not None:
                logger.info(
                    "[bot] Auto strategy research scheduler started "
                    f"(startup_delay={int(auto_research_settings.get('startup_delay_seconds', 0) or 0)}s, "
                    f"interval_hours={float(auto_research_settings.get('interval_hours', 24.0) or 24.0):.1f})"
                )
            else:
                logger.info("[bot] Auto strategy research scheduler is disabled")
        elif should_start_separate_auto_research_worker(auto_research_settings):
            if AUTO_RESEARCH_ALLOW_SEPARATE_WORKER and start_auto_research_worker() is None:
                logger.warning("[bot] Auto strategy research worker was requested but did not stay running")
        elif auto_research_settings.get("enabled"):
            logger.info("[bot] Auto strategy research enabled in config but no runtime mode is allowed")
        else:
            logger.info("[bot] Auto strategy research disabled in bot runtime config")
    except Exception as e:
        logger.warning(f"[bot] Auto strategy research failed to start: {e}")

    # ── Whale monitoring ──────────────────────────────────────────────────
    try:
        from whale_alert_manager import WhaleAlertManager
        from services.intelligence_event_utils import canonical_crypto_asset, record_whale_alert_event
        from core.asset_profiles import is_crypto

        _whale_mgr = WhaleAlertManager()

        def _on_whale_alert(alert: dict) -> None:
            try:
                source_name = str(alert.get("source", "") or "").lower()
                if (
                    source_name.startswith("telegram/")
                    or source_name.startswith("twitter")
                    or source_name.startswith("reddit")
                ):
                    return  # social watcher already persisted this event upstream

                symbol = str(alert.get("symbol", alert.get("asset", ""))).upper().strip()
                asset  = canonical_crypto_asset(symbol)
                if not asset or not is_crypto(asset):
                    return   # only crypto whale data is valid

                size_usd  = float(alert.get("value_usd", alert.get("usd_amount", 0)))
                if size_usd < 500_000:
                    return

                record_whale_alert_event(
                    asset=asset,
                    source=alert.get("source", "whale_alert"),
                    value_usd=size_usd,
                    raw_text=alert.get("raw_text", alert.get("title", "")),
                    sentiment=float(alert.get("sentiment", 0.1)),
                    timestamp=alert.get("alert_time") or alert.get("created_at") or alert.get("date"),
                    metadata={
                        "title": alert.get("title", ""),
                        "url": alert.get("url", ""),
                    },
                    external_id=str(alert.get("external_id") or alert.get("url") or ""),
                )
            except Exception as e:
                logger.error(f"[bot] on_whale_alert callback error: {e}")

        _whale_mgr.on_alert = _on_whale_alert
        _whale_mgr.start_monitoring()
        logger.info("[bot] WhaleAlertManager started — market intelligence feed active")
    except Exception as e:
        logger.warning(f"[bot] WhaleAlertManager failed to start: {e}")

    # ── Dashboard ─────────────────────────────────────────────────────────
    dashboard_cert = None
    dashboard_key = None
    if args.http2:
        try:
            dashboard_cert, dashboard_key = _resolve_tls_certificates(args.ssl_cert, args.ssl_key)
        except Exception as e:
            logger.warning(f"[bot] HTTP/2 TLS setup failed: {e}; running without HTTPS.")
            dashboard_cert = None
            dashboard_key = None

    if not args.no_dashboard:
        try:
            from dashboard.web_app_live import start_dashboard
            display_host = "localhost" if args.host in ("0.0.0.0", "127.0.0.1") else args.host
            scheme = "https" if args.http2 and dashboard_cert and dashboard_key else "http"
            logger.info(f"[bot] Dashboard → {scheme}://{display_host}:{args.port}")
            start_dashboard(
                engine,
                host=args.host,
                port=args.port,
                http2=args.http2,
                ssl_cert=dashboard_cert,
                ssl_key=dashboard_key,
            )  # blocking
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
