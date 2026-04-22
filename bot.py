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
    LOG_RETENTION_DAYS,
)
from utils.logger import TradingLogger, get_logger, prune_stale_log_artifacts

_BOT_ROLE = os.getenv("BOT_ROLE", "").strip().lower()
_DEEPSEEK_ONLY_MODE = _BOT_ROLE == "deepseek"
_deepseek_bot = None

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

if _DEEPSEEK_ONLY_MODE:
    logger.info("=" * 60)
    logger.info(" DEEPSEEK STANDALONE BOT — STARTING")
    logger.info("=" * 60)
else:
    logger.info("=" * 60)
    logger.info(" FOREX PREDICTION BOT — STARTING")
    logger.info("=" * 60)

    # ── API key validation (before anything else) ─────────────────────────────
    logger.info("[bot] Validating API keys...")
    try:
        from config.api_validation import validate_apis
        validate_apis()
        logger.info("[bot] API validation passed")
    except RuntimeError as e:
        logger.critical(f"[bot] API validation failed: {e}")
        sys.exit(1)

    # ── Check optional API keys ─────────────────────────────────────────────
    from config.config import WHALE_ALERT_KEY, WHALE_TELEGRAM_TOKEN, FRED_API_KEY
    if not WHALE_ALERT_KEY:
        logger.warning("[bot] WHALE_ALERT_KEY not set — authenticated whale API enrichment disabled")
    if not WHALE_TELEGRAM_TOKEN:
        logger.warning("[bot] WHALE_TELEGRAM_TOKEN not set — intelligence alerts disabled")
    if not FRED_API_KEY:
        logger.warning("[bot] FRED_API_KEY not set — macro data collection disabled")

    # ── Database (required) ───────────────────────────────────────────────────
    logger.info("[bot] Connecting to database...")
    try:
        from config.database import init_db
        init_db()
        logger.info("[bot] Database ready")
    except RuntimeError as e:
        logger.critical(str(e))
        sys.exit(1)

    # ── System state (after DB ready) ─────────────────────────────────────────
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


def _gateway_node_binary() -> str | None:
    node = shutil.which("node") or shutil.which("node.exe")
    if not node:
        logger.warning(
            "[Gateway] Node.js not found — WebSocket gateway disabled.\n"
            "          Install Node.js from https://nodejs.org to enable it."
        )
    return node


def _gateway_ensure_dependencies(gateway_dir: Path) -> bool:
    node_modules = gateway_dir / "node_modules"
    if node_modules.exists():
        return True

    npm = shutil.which("npm") or shutil.which("npm.cmd")
    if not npm:
        logger.warning("[Gateway] npm not found — cannot install dependencies")
        return False

    logger.info("[Gateway] node_modules missing — running npm install...")
    try:
        result = subprocess.run(
            [npm, "install"],
            cwd=str(gateway_dir),
            capture_output=True,
            text=True,
            timeout=120,
        )
        if result.returncode != 0:
            logger.warning(f"[Gateway] npm install failed:\n{result.stderr[:300]}")
            return False
        logger.info("[Gateway] npm install complete")
        return True
    except subprocess.TimeoutExpired:
        logger.warning("[Gateway] npm install timed out after 120s")
    except Exception as e:
        logger.warning(f"[Gateway] npm install error: {e}")
    return False


def _launch_gateway_process(node: str) -> subprocess.Popen | None:
    try:
        proc = subprocess.Popen(
            [node, "server.js"],
            cwd=str(_GATEWAY_DIR),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0,
        )
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

    node = _gateway_node_binary()
    if not node:
        return None

    if not _gateway_ensure_dependencies(_GATEWAY_DIR):
        return None

    proc = _launch_gateway_process(node)
    _gateway_proc = proc
    return proc


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


def _perform_shutdown(engine, *, reason: str = "signal", exit_code: int = 0) -> None:
    with _shutdown_lock:
        logger.info(f"[bot] Shutdown sequence started — reason={reason}")
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


atexit.register(stop_gateway)
atexit.register(stop_telegram)


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
    logger.warning(
        "[bot] Backtest/Strategy Lab path removed from playbook-only runtime "
        f"(requested {asset} {category} via {strategy_name})"
    )


def _run_optional_step(
    action,
    success_message: str | None = None,
    failure_message: str | None = None,
    *,
    success_level: str = "info",
    failure_level: str = "warning",
) -> bool:
    try:
        action()
        if success_message:
            getattr(logger, success_level)(success_message)
        return True
    except Exception as error:
        if failure_message:
            getattr(logger, failure_level)(failure_message.format(error=error))
        return False


def _start_deepseek_bot() -> None:
    from config.config import DEEPSEEK_TELEGRAM_CHAT_ID, DEEPSEEK_TELEGRAM_TOKEN
    from deepseek_bot import DeepSeekTelegramBot

    if not DEEPSEEK_TELEGRAM_TOKEN:
        logger.critical("[bot] DeepSeek Telegram token missing — set DEEPSEEK_TELEGRAM_TOKEN")
        sys.exit(1)

    logger.info("[bot] DeepSeek standalone mode enabled")
    bot = DeepSeekTelegramBot(token=DEEPSEEK_TELEGRAM_TOKEN, allowed_chat_id=DEEPSEEK_TELEGRAM_CHAT_ID)
    bot.run()


def _start_deepseek_background_bot() -> None:
    global _deepseek_bot

    from config.config import DEEPSEEK_TELEGRAM_CHAT_ID, DEEPSEEK_TELEGRAM_TOKEN
    from deepseek_bot import DeepSeekTelegramBot

    if not DEEPSEEK_TELEGRAM_TOKEN:
        logger.info("[bot] DeepSeek Telegram bot not started — DEEPSEEK_TELEGRAM_TOKEN missing")
        return

    _deepseek_bot = DeepSeekTelegramBot(token=DEEPSEEK_TELEGRAM_TOKEN, allowed_chat_id=DEEPSEEK_TELEGRAM_CHAT_ID)
    _deepseek_bot.start_background()
    logger.info("[bot] DeepSeek Telegram bot started in background")


def _register_engine_singleton(engine) -> None:
    try:
        import core.engine as _eng_mod
        _eng_mod._CORE_INSTANCE = engine
    except Exception:
        pass


def _create_shutdown_handler(engine):
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

    return _shutdown


def _load_api_expiry_alerts():
    expiry_alerts = []
    try:
        from config.config import API_KEY_EXPIRY_DATES
        if API_KEY_EXPIRY_DATES:
            expiry_alerts = list(API_KEY_EXPIRY_DATES.items())
    except (ImportError, AttributeError):
        logger.debug("[bot] API_KEY_EXPIRY_DATES not configured")
    return expiry_alerts


def _api_expiry_worker(engine) -> None:
    import datetime as _dt

    expiry_alerts = _load_api_expiry_alerts()
    while True:
        try:
            tc = engine
            today = _dt.date.today()
            if tc and hasattr(tc, "telegram") and tc.telegram and expiry_alerts and _api_expiry_worker.last_checked != today:
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
                _api_expiry_worker.last_checked = today
        except Exception as error:
            logger.debug(f"[APIExpiryChecker] error: {error}")
        import time as _time
        sleep_seconds = 86400 if _api_expiry_worker.last_checked == _dt.date.today() else 300
        _time.sleep(sleep_seconds)


def _start_api_expiry_checker(engine) -> None:
    _api_expiry_worker.last_checked = None
    threading.Thread(target=_api_expiry_worker, args=(engine,), name="APIExpiryChecker", daemon=True).start()
    logger.info("[bot] API expiry checker started")


def _start_pre_bot_services(engine, args) -> None:
    def _start_data_feeds():
        from data_ingestion import start_all as start_data_feeds
        start_data_feeds(exchanges=["binance", "bybit"])

    def _start_whale_intelligence():
        from whale_intelligence import start_all as start_whale_intelligence
        start_whale_intelligence()

    def _start_order_flow():
        from order_flow import start_all as start_order_flow
        start_order_flow()

    def _load_narrative_ai():
        from narrative_ai import get_narrative_scores, get_dominant_narrative
        return None

    def _start_news_monitor():
        from data_ingestion.news_event_monitor import start_news_monitor
        start_news_monitor()

    def _load_system_monitoring():
        from monitoring import start_monitoring
        return start_monitoring

    def _start_portfolio_risk():
        from risk.portfolio_risk import PortfolioRiskEngine
        portfolio_risk = PortfolioRiskEngine()
        engine._portfolio_risk = portfolio_risk
        engine.portfolio_risk = portfolio_risk

    def _start_exchange_router():
        from execution.exchange_router import ExchangeRouter
        from execution.paper_adapter import PaperAdapter
        router = ExchangeRouter()
        if hasattr(engine, "_paper_trader") and engine._paper_trader:
            router.register("paper", PaperAdapter(engine._paper_trader))
        engine.exchange_router = router

    def _upgrade_redis_cache():
        from config.config import CACHE_TTL
        from services.redis_cache import get_cache
        upgraded_cache = get_cache(default_ttl=CACHE_TTL)
        import data.cache as _cache_mod
        _cache_mod.cache = upgraded_cache

    _run_optional_step(_start_data_feeds, "[bot] Data feeds started", "[bot] Data feeds failed to start: {error}")
    _run_optional_step(
        _start_whale_intelligence,
        "[bot] Whale wallet intelligence started",
        "[bot] Whale wallet intelligence failed to start: {error}",
    )
    _run_optional_step(_start_order_flow, "[bot] Order flow intelligence started", "[bot] Order flow intelligence failed to start: {error}")
    _run_optional_step(_load_narrative_ai, "[bot] Narrative AI engine ready", "[bot] Narrative AI failed to load: {error}")

    logger.info("[bot] Playbook-only runtime active — live strategy bridge removed")
    logger.info("[bot] Playbook-only runtime active — Meta AI overlay removed")

    _run_optional_step(_start_news_monitor, "[bot] News event monitor started", "[bot] News event monitor failed to start: {error}")
    _run_optional_step(_load_system_monitoring, "[bot] System health monitoring module loaded", "[bot] System health monitoring failed to load: {error}")
    _run_optional_step(_start_portfolio_risk, "[bot] PortfolioRiskEngine attached", "[bot] PortfolioRiskEngine failed: {error}")
    _run_optional_step(
        _start_exchange_router,
        "[bot] ExchangeRouter ready — paper adapter registered",
        "[bot] ExchangeRouter failed: {error}",
    )

    logger.info("[bot] Playbook-only runtime active — ML prediction service removed")

    _run_optional_step(
        _upgrade_redis_cache,
        "[bot] Redis shared cache active (market-data cache remains local)",
        "[bot] Redis cache not available ({error}) — using in-process cache",
        failure_level="debug",
    )

    if not args.no_gateway:
        start_gateway()
    else:
        logger.info("[bot] Gateway disabled via --no-gateway")


def _start_intelligence_bot():
    try:
        from intelligence_bot import intelligence_bot as _intel_bot
        if _intel_bot.is_ready:
            logger.info("[bot] Intelligence Bot (Bot 2) ready — send-only")
        else:
            logger.warning("[bot] Intelligence Bot (Bot 2) not ready — check WHALE_TELEGRAM_TOKEN in .env")
        return _intel_bot
    except Exception as error:
        logger.warning(f"[bot] Intelligence Bot init failed: {error}")
        return None


def _start_bot_services(engine, args) -> None:
    intel_bot = _start_intelligence_bot()

    _run_optional_step(
        lambda: __import__("services.intelligence_alerts", fromlist=["start_all"]).start_all(telegram_bot=intel_bot),
        "[bot] Market intelligence alerts → Bot 2",
        "[bot] Market intelligence alerts failed: {error}",
    )
    _run_optional_step(
        lambda: __import__("monitoring", fromlist=["start_monitoring"]).start_monitoring(telegram_bot=intel_bot),
        "[bot] System health monitoring → Bot 2",
        "[bot] System health monitoring failed: {error}",
    )

    if not args.no_telegram and TELEGRAM_TOKEN and TELEGRAM_CHAT_ID:
        _start_command_bot(engine)


def _start_command_bot(engine) -> None:
    try:
        from telegram_manager import telegram_manager
        started = telegram_manager.start(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, engine)
        if started:
            engine.telegram = telegram_manager.bot
            try:
                from core.signal_reporter import reporter
                reporter.wire_telegram(telegram_manager.bot)
                logger.info("[bot] SignalReporter → Bot 1")
            except Exception as error:
                logger.warning(f"[bot] SignalReporter Telegram wire failed: {error}")
            logger.info("[bot] Command Bot (Bot 1) started and wired to engine")
        else:
            logger.warning("[bot] Command Bot (Bot 1) not started — duplicate instance or missing creds")
    except Exception as error:
        logger.warning(f"[bot] Command Bot init failed: {error}")


def _wait_for_engine_ready(engine) -> None:
    logger.info("[bot] Waiting for engine to be ready...")
    ready = engine.wait_until_ready(timeout=60.0)
    if ready:
        logger.info(f"[bot] Engine ready — balance=${engine.get_balance():.2f}")
    else:
        logger.warning("[bot] Engine did not become ready in 60s — continuing anyway")


def _ensure_data_fetcher(engine) -> None:
    if engine.fetcher:
        logger.info("[bot] DataFetcher ready (reusing engine singleton)")
        return
    try:
        from data.fetcher import DataFetcher
        engine.fetcher = DataFetcher()
        logger.info("[bot] DataFetcher created (engine singleton was None)")
    except Exception as error:
        logger.warning(f"[bot] DataFetcher init failed: {error}")


def _handle_whale_alert(alert: dict) -> None:
    try:
        from services.intelligence_event_utils import canonical_crypto_asset, record_whale_alert_event
        from core.asset_profiles import is_crypto

        source_name = str(alert.get("source", "") or "").lower()
        if (
            source_name.startswith("telegram/")
            or source_name.startswith("twitter")
            or source_name.startswith("reddit")
        ):
            return

        symbol = str(alert.get("symbol", alert.get("asset", ""))).upper().strip()
        asset = canonical_crypto_asset(symbol)
        if not asset or not is_crypto(asset):
            return

        size_usd = float(alert.get("value_usd", alert.get("usd_amount", 0)))
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
    except Exception as error:
        logger.error(f"[bot] on_whale_alert callback error: {error}")


def _start_whale_monitoring() -> None:
    try:
        from whale_alert_manager import WhaleAlertManager

        whale_mgr = WhaleAlertManager()
        whale_mgr.on_alert = _handle_whale_alert
        whale_mgr.start_monitoring()
        logger.info("[bot] WhaleAlertManager started — market intelligence feed active")
    except Exception as error:
        logger.warning(f"[bot] WhaleAlertManager failed to start: {error}")


def _start_dashboard(engine, args) -> None:
    dashboard_cert = None
    dashboard_key = None
    if args.http2:
        try:
            dashboard_cert, dashboard_key = _resolve_tls_certificates(args.ssl_cert, args.ssl_key)
        except Exception as error:
            logger.warning(f"[bot] HTTP/2 TLS setup failed: {error}; running without HTTPS.")
            dashboard_cert = None
            dashboard_key = None

    if args.no_dashboard:
        logger.info("[bot] Running without dashboard. Ctrl+C to stop.")
        try:
            while engine.is_running:
                time.sleep(10)
        except KeyboardInterrupt:
            _create_shutdown_handler(engine)(None, None)
        return

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
    except Exception as error:
        logger.error(f"[bot] Dashboard failed: {error}", exc_info=True)


def main() -> None:
    args = parse_args()

    if _DEEPSEEK_ONLY_MODE:
        _start_deepseek_bot()
        return

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

    _register_engine_singleton(engine)

    # ── Graceful shutdown ─────────────────────────────────────────────────
    _shutdown = _create_shutdown_handler(engine)
    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    # DeepSeek chat does not depend on the trading engine, so start it early.
    _start_deepseek_background_bot()

    engine.start()

    # ── API key expiry notifications ──────────────────────────────────────
    _start_api_expiry_checker(engine)

    # DataFetcher check moved to after wait_until_ready — see below

    _start_pre_bot_services(engine, args)

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

    _start_bot_services(engine, args)

    # ── Wait for engine ───────────────────────────────────────────────────
    _wait_for_engine_ready(engine)

    # ── DataFetcher ───────────────────────────────────────────────────────
    # Checked AFTER wait_until_ready so engine._init_subsystems() has
    # completed and engine.fetcher is guaranteed to exist if init succeeded.
    _ensure_data_fetcher(engine)

    logger.info("[bot] Playbook-only runtime active — AutoTrainer removed")

    logger.info("[bot] Playbook-only runtime active — auto strategy research removed")

    # ── Whale monitoring ──────────────────────────────────────────────────
    _start_whale_monitoring()

    # ── Dashboard ─────────────────────────────────────────────────────────
    _start_dashboard(engine, args)


if __name__ == "__main__":
    main()
