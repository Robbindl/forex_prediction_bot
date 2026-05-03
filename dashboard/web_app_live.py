from __future__ import annotations

import argparse
import copy
import gzip
import io
import inspect
import json
import os
import sys
import threading
import time
import traceback
from bisect import bisect_right
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from flask import Flask, Response, jsonify, redirect, render_template, request, send_from_directory, stream_with_context
from flask_cors import CORS
from functools import wraps
import hashlib
from werkzeug.middleware.proxy_fix import ProxyFix
from urllib.parse import splitport

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.assets  import registry
from data.fetcher import DataFetcher, get_shared_fetcher
from services.live_position_pricing import resolve_live_position_snapshot
from utils.logger import get_logger
from utils.api_errors import (
    APIError, BadRequest, Unauthorized, Forbidden, NotFound, InternalError,
    log_api_call, handle_api_error, validate_request_json
)

logger = get_logger()

# ── Market hours helper ───────────────────────────────────────────────────────
try:
    from dashboard.market_hours import is_market_open_for_asset
except Exception:
    def is_market_open_for_asset(asset): return (True, "unknown")

# ── Optional services ─────────────────────────────────────────────────────────
try:
    from prediction_tracker import prediction_tracker as _pred_tracker
    _pred_tracker.start()
except Exception as _e:
    _pred_tracker = None
    logger.warning(f"[dashboard] PredictionTracker unavailable: {_e}")

try:
    from redis_broker import broker as _redis_broker
except Exception:
    _redis_broker = None

try:
    from websocket_dashboard import add_transaction, get_feed, set_live_price
    _ws_ok = True
except Exception:
    _ws_ok = False
    def add_transaction(*a, **kw): pass
    def get_feed(**kw): return []
    def set_live_price(*a, **kw): pass

try:
    from telegram_manager import telegram_manager
except Exception:
    telegram_manager = None


def _record_live_quote(
    source: str,
    symbol: str,
    price: float,
    volume: Optional[float] = None,
    side: Optional[str] = None,
    *,
    emit_transaction: bool = True,
    store_live_price: bool = True,
) -> None:
    if emit_transaction:
        add_transaction(source, symbol, price, volume, side)
    if store_live_price:
        # Store price for live P&L updates and shared dashboard cache freshness.
        set_live_price(symbol, price, source)

# ── Flask app ─────────────────────────────────────────────────────────────────
app = Flask(
    __name__,
    template_folder=os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "templates"),
    static_folder  =os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "static"),
)
_PROJECT_ROOT = Path(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))).resolve()
_SOURCE_PEEK_SUFFIXES = {".py", ".html", ".js", ".json", ".md", ".css"}
app.config["TEMPLATES_AUTO_RELOAD"] = True
app.jinja_env.auto_reload = True
try:
    from config.config import (
        DASHBOARD_CORS_ORIGINS,
        DASHBOARD_BG_REFRESH_WORKERS,
        DASHBOARD_COMMAND_CENTER_WORKERS,
        DASHBOARD_CORRELATION_WORKERS,
        DEEPSEEK_API_KEY,
        DEEPSEEK_TELEGRAM_TOKEN,
        DASHBOARD_HEATMAP_WORKERS,
        DASHBOARD_REDIS_CACHE_ENABLED,
        DASHBOARD_REDIS_CACHE_MAX_VALUE_BYTES,
        DASHBOARD_REDIS_CACHE_TTL_CAP_SECONDS,
        DASHBOARD_SENTIMENT_ASSET_WORKERS,
        DASHBOARD_SENTIMENT_FETCH_WORKERS,
        DASHBOARD_WEAK_POSITIONS_LIMIT,
        CTRADER_LIVE_DEPTH_ENABLED,
        DUKASCOPY_HISTORY_ENABLED,
        DUKASCOPY_LIVE_DEPTH_ENABLED,
        DASHBOARD_ALLOWED_HOSTS,
        DEVELOPMENT_MODE,
        LOCAL_CANDLE_STORE_ENABLED,
        NEWS_REDDIT_ENABLED,
        NEWS_RSS_ENABLED,
        NEWS_SENTIMENT_EXECUTION_ENABLED,
        NEWS_SENTIMENT_ENABLED,
        PLAYBOOK_ONLY_RUNTIME,
        ROBBIE_CHAT_PROVIDER,
        TOP_OPPORTUNITIES_LIMIT,
        TRUST_PROXY_COUNT,
    )
except Exception:
    DASHBOARD_CORS_ORIGINS = ["http://localhost:5000"]
    DASHBOARD_ALLOWED_HOSTS = []
    TRUST_PROXY_COUNT = 1
    DEVELOPMENT_MODE = False
    PLAYBOOK_ONLY_RUNTIME = False
    NEWS_SENTIMENT_ENABLED = True
    NEWS_SENTIMENT_EXECUTION_ENABLED = False
    NEWS_REDDIT_ENABLED = False
    NEWS_RSS_ENABLED = False
    TOP_OPPORTUNITIES_LIMIT = 10
    DASHBOARD_WEAK_POSITIONS_LIMIT = 8
    DASHBOARD_BG_REFRESH_WORKERS = 6
    DASHBOARD_COMMAND_CENTER_WORKERS = 4
    DASHBOARD_CORRELATION_WORKERS = 8
    DASHBOARD_HEATMAP_WORKERS = 10
    DASHBOARD_REDIS_CACHE_ENABLED = False
    DASHBOARD_REDIS_CACHE_MAX_VALUE_BYTES = 65536
    DASHBOARD_REDIS_CACHE_TTL_CAP_SECONDS = 300
    DASHBOARD_SENTIMENT_ASSET_WORKERS = 10
    DASHBOARD_SENTIMENT_FETCH_WORKERS = 6
    DEEPSEEK_API_KEY = ""
    DEEPSEEK_TELEGRAM_TOKEN = ""
    CTRADER_LIVE_DEPTH_ENABLED = False
    DUKASCOPY_HISTORY_ENABLED = True
    DUKASCOPY_LIVE_DEPTH_ENABLED = False
    LOCAL_CANDLE_STORE_ENABLED = True
    ROBBIE_CHAT_PROVIDER = "auto"

app.wsgi_app = ProxyFix(app.wsgi_app, x_for=TRUST_PROXY_COUNT, x_proto=TRUST_PROXY_COUNT, x_host=TRUST_PROXY_COUNT)
CORS(app, resources={r"/api/*": {"origins": DASHBOARD_CORS_ORIGINS}})

_COMMAND_CENTER_CACHE_TTL = 15
_COMMAND_CENTER_LAST_GOOD_TTL = 6 * 60 * 60
_COMMAND_CENTER_DEGRADED_CACHE_TTL = 5
_COMMAND_CENTER_BUILD_TIMEOUT_SECONDS = 7.5
_COMMAND_CENTER_PAYLOAD_CACHE_KEY = "command_center_payload:v3"
_COMMAND_CENTER_PAYLOAD_LAST_GOOD_KEY = "command_center_payload:v3:last_good"
_COMMAND_CENTER_PAYLOAD_REFRESH_KEY = "command_center_payload:v3"
_COMMAND_CENTER_RUNTIME_CONTEXT_LOG_KEY = "COMMAND_CENTER_CONTEXT_LOG"
_PAGE_OVERVIEW_CACHE_TTL = 15
_PAGE_OVERVIEW_LAST_GOOD_TTL = 6 * 60 * 60
_PAGE_OVERVIEW_DEGRADED_CACHE_TTL = 5
_PAGE_OVERVIEW_BUILD_TIMEOUT_SECONDS = 4.0
_PAGE_OVERVIEW_CACHE_VERSION = "v6"
_CLOSED_TRADES_CACHE_TTL = 10
_TRADE_HISTORY_CACHE_TTL = 15
_PERFORMANCE_GUARD_CACHE_TTL = 30
_SENTIMENT_BY_ASSET_CACHE_KEY = "sentiment_by_asset:v2"


def _normalize_host_name(value: str) -> str:
    host = (value or "").strip().lower()
    if not host:
        return ""
    if host.startswith("[") and "]" in host:
        host = host[1:].split("]", 1)[0]
        return host
    maybe_host, _ = splitport(host)
    return maybe_host or host


_DASHBOARD_ALLOWED_HOSTS = {
    normalized
    for normalized in (
        _normalize_host_name(host)
        for host in DASHBOARD_ALLOWED_HOSTS
    )
    if normalized
}
_DASHBOARD_ALLOWED_HOSTS.update({"127.0.0.1", "localhost", "::1"})


@app.before_request
def _enforce_allowed_hosts():
    if DEVELOPMENT_MODE or not _DASHBOARD_ALLOWED_HOSTS:
        return None
    request_host = _normalize_host_name(request.host)
    if request_host in _DASHBOARD_ALLOWED_HOSTS:
        return None
    if request.path.startswith("/api/"):
        return jsonify({"success": False, "error": "Host not allowed"}), 421
    return Response("Host not allowed", status=421, mimetype="text/plain")

# ── Flask Error Handlers ──────────────────────────────────────────────────────

@app.errorhandler(400)
def handle_bad_request(e):
    """Handle 400 Bad Request."""
    return jsonify({"success": False, "error": "Bad Request", "message": str(e)}), 400

@app.errorhandler(401)
def handle_unauthorized(e):
    """Handle 401 Unauthorized."""
    return jsonify({"success": False, "error": "Unauthorized"}), 401

@app.errorhandler(403)
def handle_forbidden(e):
    """Handle 403 Forbidden."""
    return jsonify({"success": False, "error": "Forbidden"}), 403

@app.errorhandler(404)
def handle_not_found(e):
    """Handle 404 Not Found."""
    return jsonify({"success": False, "error": "Not Found"}), 404

@app.errorhandler(500)
def handle_internal_error(e):
    """Handle 500 Internal Server Error."""
    error_id = datetime.utcnow().strftime("%Y%m%d%H%M%S%f")[:14]
    logger.error(f"[{error_id}] Internal server error: {str(e)}\n{traceback.format_exc()}")
    return jsonify({
        "success": False,
        "error": "Internal Server Error",
        "error_id": error_id
    }), 500

@app.errorhandler(APIError)
def handle_api_error_exc(e):
    """Handle custom API errors."""
    response = {
        "success": False,
        "error": e.message,
        "status": e.status_code,
    }
    if e.details:
        response["details"] = e.details
    return jsonify(response), e.status_code

# ── FIX SEC-05: API Key Authentication & Rate Limiting ───────────────────────
_DEVELOPMENT_MODE = False  # Will be set from env variable (bypass auth when true)
_API_KEY_HASH = None  # Will be set from env variable
_AUTH_CONFIG_ERROR = ""
_SESSION_TOKENS: Dict[str, float] = {}  # {token: expiry_timestamp}
_SESSION_TOKEN_LOCK = threading.Lock()
_RATE_LIMIT_STORE: Dict[str, List[float]] = {}  # {ip: [req_times...]}
_RATE_LIMIT_LOCK = threading.Lock()
_RATE_LIMIT_REQUESTS_PER_MINUTE = 240  # Dashboard pages fan out across many panels.
_LOCAL_DASHBOARD_HOSTS = {"127.0.0.1", "localhost", "::1"}
_SESSION_TOKEN_TTL = 3600  # 1 hour default

def _init_api_key():
    """Initialize API key and session TTL from environment."""
    global _API_KEY_HASH, _SESSION_TOKEN_TTL, _DEVELOPMENT_MODE, _AUTH_CONFIG_ERROR
    try:
        from config.config import DASHBOARD_API_KEY, SESSION_TOKEN_TTL, DEVELOPMENT_MODE
        _DEVELOPMENT_MODE = DEVELOPMENT_MODE
        _AUTH_CONFIG_ERROR = ""
        if _DEVELOPMENT_MODE:
            logger.warning("[dashboard] ⚠️ DEVELOPMENT MODE ENABLED — All API auth bypassed")
        elif DASHBOARD_API_KEY:
            _API_KEY_HASH = hashlib.sha256(DASHBOARD_API_KEY.encode()).hexdigest()
            logger.info("[dashboard] API key authentication enabled")
        else:
            _AUTH_CONFIG_ERROR = "DASHBOARD_API_KEY is required when DEVELOPMENT_MODE=false"
            _API_KEY_HASH = None
            logger.critical(f"[dashboard] {_AUTH_CONFIG_ERROR}")
        _SESSION_TOKEN_TTL = SESSION_TOKEN_TTL
    except Exception as e:
        logger.warning(f"[dashboard] Failed to load API config: {e}")

def _generate_session_token() -> str:
    """Generate a cryptographically secure session token."""
    return hashlib.sha256(os.urandom(32)).hexdigest()

def _check_api_auth(fn):
    """Decorator to verify session token or API key.
    If DEVELOPMENT_MODE is enabled or API key is not configured, allow all access."""
    @wraps(fn)
    def wrapper(*args, **kwargs):
        # Explicit development mode: bypass auth entirely
        if _DEVELOPMENT_MODE:
            return fn(*args, **kwargs)

        if _AUTH_CONFIG_ERROR:
            return jsonify({"success": False, "error": _AUTH_CONFIG_ERROR}), 503

        if not _API_KEY_HASH:
            return jsonify({"success": False, "error": "Dashboard authentication unavailable"}), 503
        
        # Production mode: enforce authentication
        auth_header = request.headers.get("Authorization", "")
        token = ""
        if auth_header.startswith("Bearer "):
            token = auth_header[7:]
        else:
            token = str(request.args.get("token", "") or "").strip()
            if not token:
                return jsonify({"success": False, "error": "Missing Authorization header"}), 401
        
        # Check if valid session token (issued by /api/login)
        with _SESSION_TOKEN_LOCK:
            if token in _SESSION_TOKENS:
                if _SESSION_TOKENS[token] > time.time():
                    # Token valid and not expired
                    return fn(*args, **kwargs)
                else:
                    # Token expired
                    del _SESSION_TOKENS[token]
                    return jsonify({"success": False, "error": "Session expired"}), 401
        
        # Token not in session store — reject
        logger.warning(f"[dashboard] Invalid token attempt from {request.remote_addr}")
        return jsonify({"success": False, "error": "Invalid or expired token"}), 403
    return wrapper

def _check_rate_limit(fn):
    """Decorator to enforce rate limiting per IP."""
    @wraps(fn)
    def wrapper(*args, **kwargs):
        ip = request.remote_addr
        host = (request.host or "").rsplit(":", 1)[0].strip("[]")
        if ip in _LOCAL_DASHBOARD_HOSTS and host in _LOCAL_DASHBOARD_HOSTS:
            return fn(*args, **kwargs)

        now = time.time()
        
        with _RATE_LIMIT_LOCK:
            if ip not in _RATE_LIMIT_STORE:
                _RATE_LIMIT_STORE[ip] = []
            
            # Remove requests older than 1 minute
            _RATE_LIMIT_STORE[ip] = [ts for ts in _RATE_LIMIT_STORE[ip] if now - ts < 60]
            
            # Check if limit exceeded
            if len(_RATE_LIMIT_STORE[ip]) >= _RATE_LIMIT_REQUESTS_PER_MINUTE:
                logger.warning(f"[dashboard] Rate limit exceeded for {ip}")
                return jsonify({
                    "success": False,
                    "error": f"Rate limit exceeded (max {_RATE_LIMIT_REQUESTS_PER_MINUTE}/min)"
                }), 429
            
            # Record this request
            _RATE_LIMIT_STORE[ip].append(now)
        
        return fn(*args, **kwargs)
    return wrapper

_parser = argparse.ArgumentParser(add_help=False)
_parser.add_argument("--balance", type=float, default=10000.0)
_args, _ = _parser.parse_known_args()

# ── Engine singleton ──────────────────────────────────────────────────────────
_CORE: Any = None

def inject_core(core) -> None:
    global _CORE
    _CORE = core
    if core is None:
        logger.info("[dashboard] Standalone dashboard mode — TradingCore not injected")
        return
    try:
        if getattr(core, "state", None) is not None:
            setattr(_args, "state", core.state)
    except Exception:
        pass
    logger.info("[dashboard] TradingCore injected")

def _core() -> Optional[Any]:
    return _CORE


def _dashboard_state() -> Optional[Any]:
    core = _core()
    if core is not None and getattr(core, "state", None) is not None:
        return core.state
    return getattr(_args, "state", None)

# ── Asset registry ────────────────────────────────────────────────────────────
ALL_ASSETS: List[Tuple[str, str]] = registry.all_assets()
_CAT: Dict[str, str] = {a: c for a, c in ALL_ASSETS}

def _cat(asset: str) -> str:
    return _CAT.get(asset, "crypto")

# ── DataFetcher — use engine singleton when available ────────────────────────
# The engine already holds a DataFetcher (self.fetcher). We reuse it so the
# dashboard live prices are now streamed from Deriv.
# Falls back to a local instance only if the engine isn't ready yet.
_fetcher_local: Optional[Any] = None

def _get_fetcher():
    """Return engine's DataFetcher if available, else local fallback."""
    core = _core()
    if core and getattr(core, "fetcher", None):
        return core.fetcher
    global _fetcher_local
    if _fetcher_local is None:
        _fetcher_local = get_shared_fetcher()
    return _fetcher_local

# Module-level alias so existing code using _fetcher still works
class _FetcherProxy:
    """Proxy that always delegates to the current best fetcher."""
    def __getattr__(self, name):
        return getattr(_get_fetcher(), name)

_fetcher = _FetcherProxy()

def _ohlcv(asset: str, interval: str = "1d", periods: int = 60):
    try:
        return _fetcher.get_ohlcv(asset, _cat(asset), interval=interval, periods=periods)
    except Exception as e:
        logger.debug(f"[dashboard] ohlcv {asset}: {e}")
        return None

# ── Lazy sentiment singleton ──────────────────────────────────────────────────
_sent_svc  = None
_sent_lock = threading.Lock()

def _get_sent():
    global _sent_svc
    if _sent_svc is not None:
        return _sent_svc
    with _sent_lock:
        if _sent_svc is None:
            try:
                from services.sentiment_dashboard_service import get_dashboard_service
                _sent_svc = get_dashboard_service()
            except Exception as e:
                logger.warning(f"[dashboard] SentimentDashboardService: {e}")
    return _sent_svc

# ── Lazy market-intelligence singleton ───────────────────────────────────────
_market_intel_svc  = None
_market_intel_lock = threading.Lock()

def _get_market_intelligence():
    global _market_intel_svc
    if _market_intel_svc is not None:
        return _market_intel_svc
    with _market_intel_lock:
        if _market_intel_svc is None:
            try:
                from services.market_intelligence_service import get_service as get_market_intelligence_service
                _market_intel_svc = get_market_intelligence_service()
            except Exception as e:
                logger.warning(f"[dashboard] MarketIntelligenceService: {e}")
    return _market_intel_svc

# ── Response cache (in-process TTL cache; Redis opt-in with byte guardrails) ───────────────────
_cache_store: Dict[str, Tuple[Any, float]] = {}
_cache_lock  = threading.Lock()
_cache_prefix = "dashboard:cache:"
_LOCAL_ONLY_CACHE_PREFIXES = ("page_overview:", "page_component:", "html_template:")
_dashboard_refresh_lock = threading.Lock()
_dashboard_refresh_state: Dict[str, bool] = {}
_redis_cache_skip_log: Dict[str, float] = {}


def _cache_is_local_only(key: str) -> bool:
    return any(str(key or "").startswith(prefix) for prefix in _LOCAL_ONLY_CACHE_PREFIXES)


def _log_redis_cache_skip(reason: str, key: str, detail: str = "") -> None:
    marker = f"{reason}:{key}"
    now = time.monotonic()
    last = _redis_cache_skip_log.get(marker, 0.0)
    if now - last < 60.0:
        return
    _redis_cache_skip_log[marker] = now
    suffix = f" ({detail})" if detail else ""
    logger.debug(f"[dashboard] Redis dashboard cache skipped for {key}: {reason}{suffix}")

def _redis_cache_get(key: str) -> Optional[Any]:
    if not DASHBOARD_REDIS_CACHE_ENABLED:
        return None

    try:
        from services.redis_pool import get_client as _get_redis_client
        client = _get_redis_client()
        if client is None:
            return None
        raw = client.get(_cache_prefix + key)
        if not raw:
            return None
        return json.loads(raw)
    except Exception:
        return None


def _redis_cache_set(key: str, value: Any, ttl: int = 30) -> None:
    if not DASHBOARD_REDIS_CACHE_ENABLED:
        return
    try:
        from services.redis_pool import get_client as _get_redis_client
        client = _get_redis_client()
        if client is None:
            return
        raw = json.dumps(value, default=str, separators=(",", ":"))
        raw_size = len(raw.encode("utf-8"))
        max_bytes = max(1024, int(DASHBOARD_REDIS_CACHE_MAX_VALUE_BYTES))
        if raw_size > max_bytes:
            _log_redis_cache_skip("payload_too_large", key, f"{raw_size}>{max_bytes} bytes")
            return
        bounded_ttl = max(1, min(int(ttl or 30), int(DASHBOARD_REDIS_CACHE_TTL_CAP_SECONDS)))
        client.set(_cache_prefix + key, raw, ex=bounded_ttl)
    except Exception:
        pass


def _cache_get(key: str) -> Optional[Any]:
    with _cache_lock:
        entry = _cache_store.get(key)
        if entry and time.time() < entry[1]:
            return entry[0]
        if entry:
            _cache_store.pop(key, None)

    if _cache_is_local_only(key):
        return None

    redis_val = _redis_cache_get(key)
    if redis_val is not None:
        return redis_val
    return None


def _cache_set(key: str, value: Any, ttl: int = 30) -> None:
    with _cache_lock:
        _cache_store[key] = (value, time.time() + ttl)
    if _cache_is_local_only(key):
        return
    _redis_cache_set(key, value, ttl)


def _invalidate_cache_prefixes(*prefixes: str) -> None:
    active_prefixes = [str(prefix or "") for prefix in prefixes if str(prefix or "").strip()]
    if not active_prefixes:
        return

    with _cache_lock:
        for key in list(_cache_store.keys()):
            if any(key.startswith(prefix) for prefix in active_prefixes):
                _cache_store.pop(key, None)

    if not DASHBOARD_REDIS_CACHE_ENABLED:
        return

    try:
        from services.redis_pool import get_client as _get_redis_client

        client = _get_redis_client()
        if client is None:
            return
        for prefix in active_prefixes:
            for cache_key in client.scan_iter(match=_cache_prefix + prefix + "*"):
                client.delete(cache_key)
    except Exception:
        pass


def _render_cached_template(template_name: str, ttl: int = 30) -> str:
    template_stamp = "na"
    try:
        template_path = os.path.join(app.template_folder or "", template_name)
        template_stamp = str(os.stat(template_path).st_mtime_ns)
    except OSError:
        template_stamp = "na"
    cache_key = f"html_template:{template_name}:{template_stamp}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    html = render_template(template_name)
    _cache_set(cache_key, html, ttl=ttl)
    return html


def _trigger_dashboard_payload_refresh(
    refresh_key: str,
    *,
    builder,
    cache_key: str,
    ttl: int,
    last_good_key: str = "",
    last_good_ttl: int = 300,
) -> bool:
    with _dashboard_refresh_lock:
        if _dashboard_refresh_state.get(refresh_key):
            return False
        _dashboard_refresh_state[refresh_key] = True

    def _worker() -> None:
        try:
            payload = builder()
            degraded = _is_degraded_dashboard_payload(payload)
            _cache_set(cache_key, payload, ttl=min(ttl, _PAGE_OVERVIEW_DEGRADED_CACHE_TTL) if degraded else ttl)
            if last_good_key and not degraded:
                _cache_set(last_good_key, payload, ttl=last_good_ttl)
        except Exception as exc:
            logger.warning(f"[dashboard] async refresh failed for {refresh_key}: {exc}")
        finally:
            with _dashboard_refresh_lock:
                _dashboard_refresh_state.pop(refresh_key, None)

    thread = threading.Thread(
        target=_worker,
        name=f"DashboardRefresh:{refresh_key}",
        daemon=True,
    )
    thread.start()
    return True


def _is_degraded_dashboard_payload(payload: Any) -> bool:
    if not isinstance(payload, dict):
        return False
    if payload.get("success") is False or payload.get("degraded") or payload.get("partial"):
        return True
    for value in payload.values():
        if isinstance(value, dict) and (value.get("degraded") or value.get("success") is False):
            return True
    return False


_STALE_ONLY_DEGRADED_REASONS = {"stale_fallback", "stale_refresh", "component_stale"}


def _mark_stale_dashboard_payload(payload: Any, reason: str) -> Dict[str, Any]:
    normalized = _response_to_dict(payload)
    if not isinstance(normalized, dict):
        normalized = {}
    normalized.setdefault("success", True)
    normalized["stale"] = True
    normalized["stale_reason"] = str(reason or "stale")
    degraded_reason = str(normalized.get("degraded_reason") or "")
    if degraded_reason in _STALE_ONLY_DEGRADED_REASONS:
        normalized.pop("degraded", None)
        normalized.pop("degraded_reason", None)
    return normalized


def _get_cached_dashboard_payload(
    cache_key: str,
    *,
    builder,
    ttl: int,
    force_refresh: bool = False,
    last_good_key: str = "",
    last_good_ttl: int = 300,
    prefer_stale: bool = True,
    refresh_key: str = "",
):
    if not force_refresh:
        cached = _cache_get(cache_key)
        if cached is not None:
            return _response_to_dict(cached)

        if prefer_stale and last_good_key:
            fallback = _cache_get(last_good_key)
            if fallback is not None:
                _trigger_dashboard_payload_refresh(
                    refresh_key or cache_key,
                    builder=builder,
                    cache_key=cache_key,
                    ttl=ttl,
                    last_good_key=last_good_key,
                    last_good_ttl=last_good_ttl,
                )
                return _response_to_dict(fallback)

    payload = builder()
    degraded = _is_degraded_dashboard_payload(payload)
    _cache_set(cache_key, payload, ttl=min(ttl, _PAGE_OVERVIEW_DEGRADED_CACHE_TTL) if degraded else ttl)
    if last_good_key and not degraded:
        _cache_set(last_good_key, payload, ttl=last_good_ttl)
    return payload


_PLAYBOOK_RUNTIME_BLUEPRINTS = [
    "breakout_continuation",
    "breakout_retest",
    "trend_pullback",
    "early_inflection",
    "reversal_exhaustion",
    "failed_break_reclaim",
    "aggressive_expansion",
    "opening_drive",
    "news_impulse",
    "crypto_orderflow_continuation",
]


def _playbook_only_disabled_response(path: str, feature: str) -> tuple[Response, int]:
    return (
        jsonify(
            {
                "success": False,
                "disabled": True,
                "mode": "playbook_only",
                "feature": feature,
                "error": f"{feature} disabled in playbook-only runtime",
            }
        ),
        409,
    )


def _interpret_sentiment_score(score: float) -> str:
    if score > 0.4:
        return "Strongly Bullish"
    if score > 0.1:
        return "Bullish"
    if score > -0.1:
        return "Neutral"
    if score > -0.4:
        return "Bearish"
    return "Strongly Bearish"


def _sentiment_bucket(score: float) -> str:
    if score > 0.1:
        return "bullish"
    if score < -0.1:
        return "bearish"
    return "neutral"


def _sentiment_component_label(name: str) -> str:
    labels = {
        "fear_greed": "Fear & Greed",
        "vix": "VIX",
        "news": "News",
        "reddit": "Reddit",
        "price_momentum": "Price Momentum",
        "macro_event": "Macro Event",
        "aaii": "AAII",
        "put_call": "Put/Call",
    }
    key = str(name or "").strip().lower()
    return labels.get(key, str(name or "").replace("_", " ").title())


def _build_sentiment_context(
    market_score: float,
    news_score: float,
    fear_greed_value: float,
    market_interpretation: str,
    article_count: int,
) -> Dict[str, str]:
    market_bucket = _sentiment_bucket(market_score)
    news_bucket = _sentiment_bucket(news_score)

    if fear_greed_value <= 25 and market_bucket == "bullish" and news_bucket == "bearish":
        return {
            "mode": "contrarian_rebound",
            "display_label": "Bullish Rebound Bias",
            "summary": (
                "Macro sentiment is contrarian-bullish: extreme fear is being treated as a rebound signal "
                "while recent headlines are still bearish."
            ),
        }
    if fear_greed_value >= 75 and market_bucket == "bearish" and news_bucket == "bullish":
        return {
            "mode": "contrarian_fade",
            "display_label": "Bearish Fade Risk",
            "summary": (
                "Macro sentiment is contrarian-bearish: extreme greed is being treated as an exhaustion signal "
                "while recent headlines are still bullish."
            ),
        }
    if market_bucket == news_bucket and market_bucket != "neutral":
        return {
            "mode": "aligned",
            "display_label": market_interpretation,
            "summary": (
                f"Macro composite and headline tone are aligned on a {market_bucket} read "
                f"across {article_count} recent articles."
            ),
        }
    if market_bucket == "neutral" and news_bucket == "neutral":
        return {
            "mode": "neutral",
            "display_label": "Neutral",
            "summary": "Macro inputs and recent headlines are both broadly neutral right now.",
        }
    if article_count <= 0:
        return {
            "mode": "macro_only",
            "display_label": market_interpretation,
            "summary": "Macro composite is available, but there are not enough recent headlines to compare against it yet.",
        }
    return {
        "mode": "mixed",
        "display_label": f"{market_interpretation} / Mixed",
        "summary": "Macro composite and headline tone are mixed, so treat the top-line score as context, not consensus.",
    }


def _unwrap_view(fn):
    try:
        return inspect.unwrap(fn)
    except Exception:
        return fn


def _call_view(fn, *args, **kwargs):
    return _unwrap_view(fn)(*args, **kwargs)


def _response_to_dict(resp: Any) -> Any:
    if resp is None:
        return {}
    if isinstance(resp, tuple):
        if not resp:
            return {}
        return _response_to_dict(resp[0])
    if isinstance(resp, (dict, list)):
        if isinstance(resp, dict):
            return {key: _response_to_dict(value) for key, value in resp.items()}
        return [_response_to_dict(item) for item in resp]
    try:
        if hasattr(resp, "get_json"):
            payload = resp.get_json()
            if payload is not None:
                return _response_to_dict(payload)
        if hasattr(resp, "get_data"):
            return _response_to_dict(json.loads(resp.get_data(as_text=True)))
    except Exception:
        pass
    return resp


def _language_from_suffix(path: Path) -> str:
    return {
        ".py": "python",
        ".html": "html",
        ".js": "javascript",
        ".json": "json",
        ".md": "markdown",
        ".css": "css",
    }.get(path.suffix.lower(), "text")


def _build_source_peek_payload(raw_path: str, focus_line: Optional[int] = None) -> Dict[str, Any]:
    rel_path = str(raw_path or "").strip().replace("\\", "/")
    if not rel_path:
        raise BadRequest("path query required")
    if rel_path.startswith("/") or ":" in rel_path.split("/")[0]:
        raise BadRequest("path must be repo-relative")

    target = (_PROJECT_ROOT / rel_path).resolve()
    try:
        target.relative_to(_PROJECT_ROOT)
    except ValueError as exc:
        raise Forbidden("path escapes project root") from exc

    if target.suffix.lower() not in _SOURCE_PEEK_SUFFIXES:
        raise BadRequest("unsupported source preview type")
    if not target.exists() or not target.is_file():
        raise NotFound("source file not found")

    try:
        lines = target.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError as exc:
        raise InternalError(f"failed to read source file: {exc}") from exc

    total_lines = len(lines)
    anchor = max(1, int(focus_line or 1))
    anchor = min(anchor, total_lines or 1)
    if total_lines <= 32:
        start_line = 1
        end_line = total_lines
    else:
        window = 16
        start_line = max(1, anchor - window)
        end_line = min(total_lines, start_line + 31)
        start_line = max(1, end_line - 31)

    snippet_lines = lines[start_line - 1:end_line]
    numbered = "\n".join(
        f"{lineno:>4} | {text}" for lineno, text in enumerate(snippet_lines, start=start_line)
    )
    return {
        "success": True,
        "path": rel_path,
        "resolved_path": str(target),
        "language": _language_from_suffix(target),
        "total_lines": total_lines,
        "start_line": start_line,
        "end_line": end_line,
        "anchor_line": anchor,
        "snippet": numbered,
    }


def _get_command_center_top_opportunities(core: Any, limit: int = 5) -> List[Dict[str, Any]]:
    getter = getattr(core, "get_top_ranked_opportunities", None)
    if not callable(getter):
        return []
    try:
        return list(
            getter(
                limit=limit,
                refresh=False,
                allow_refresh_when_empty=False,
            )
            or []
        )
    except TypeError:
        try:
            return list(getter(limit=limit, refresh=False) or [])
        except TypeError:
            return list(getter(limit=limit) or [])


def _get_command_center_weak_positions(core: Any, limit: int = 5) -> List[Dict[str, Any]]:
    getter = getattr(core, "get_weak_positions", None)
    if not callable(getter):
        return []
    try:
        return list(
            getter(
                limit=limit,
                include_market_status=False,
            )
            or []
        )
    except TypeError:
        try:
            return list(getter(limit=limit) or [])
        except TypeError:
            return list(getter() or [])


def _run_with_timeout(
    func,
    *args,
    timeout: float = 5.0,
    default: Any = None,
    label: str = "dashboard task",
    **kwargs,
):
    """Run a non-request-bound callable with a hard wall-clock budget."""
    pool = None
    try:
        from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout

        pool = ThreadPoolExecutor(max_workers=1)
        future = pool.submit(func, *args, **kwargs)
        try:
            result = future.result(timeout=timeout)
            pool.shutdown(wait=False, cancel_futures=True)
            return result
        except FuturesTimeout:
            future.cancel()
            logger.warning(f"[dashboard] {label} timed out after {timeout:.1f}s")
            pool.shutdown(wait=False, cancel_futures=True)
        except Exception as exc:
            logger.debug(f"[dashboard] {label} failed: {exc}")
            pool.shutdown(wait=False, cancel_futures=True)
    except Exception as exc:
        logger.debug(f"[dashboard] {label} dispatch failed: {exc}")
        if pool is not None:
            try:
                pool.shutdown(wait=False, cancel_futures=True)
            except Exception:
                pass

    return copy.deepcopy(default)


def _request_wants_cache_bypass() -> bool:
    if not _DEVELOPMENT_MODE:
        return False
    return request.args.get('no_cache', '').strip().lower() in {'1', 'true', 'yes'}


_GLOBAL_API_CACHE_SKIP_PREFIXES = (
    "/api/status",
    "/api/page-overview",
    "/api/command-center",
    "/api/live-book",
    "/api/chart/candles",
    "/api/chart/quote",
    "/api/chart/stream",
    "/api/live-prices/stream",
    "/api/system/health",
    "/api/system-monitor/overview",
    "/api/monitoring/snapshot",
    "/api/monitoring/metrics",
    "/api/monitoring/errors",
    "/api/market/heatmap",
)


def _should_skip_global_api_cache(path: Optional[str] = None) -> bool:
    current_path = str(path or request.path or "").strip()
    return any(current_path.startswith(prefix) for prefix in _GLOBAL_API_CACHE_SKIP_PREFIXES)


def _normalized_query_string() -> str:
    params = []
    for key in sorted(request.args.keys()):
        if key in {'_', 'no_cache'}:
            continue
        values = request.args.getlist(key)
        for value in sorted(values):
            params.append(f"{key}={value}")
    return '&'.join(params)


def _get_api_cache_key() -> str:
    qs = _normalized_query_string()
    if qs:
        return f"api_cache:{request.path}?{qs}"
    return f"api_cache:{request.path}"


@app.before_request
def _serve_cached_api_response() -> Optional[Response]:
    if request.method != 'GET' or not request.path.startswith('/api/'):
        return None
    if request.path == "/api/trade-history" or request.path.startswith("/api/trade-history/"):
        return None
    if _should_skip_global_api_cache():
        return None
    if _request_wants_cache_bypass():
        return None

    cache_key = _get_api_cache_key()
    cached = _cache_get(cache_key)
    if cached is None:
        return None

    response = jsonify(cached)
    response.headers['Cache-Control'] = 'public, max-age=10, stale-while-revalidate=30'
    response.headers['X-Cache'] = 'HIT'
    return response


def _compress_response(response: Response) -> Response:
    try:
        if response.is_streamed:
            return response
        if response.direct_passthrough:
            return response
        if response.status_code != 200:
            return response
        if response.headers.get("Content-Encoding"):
            return response
        accept_encoding = request.headers.get("Accept-Encoding", "")
        if "gzip" not in accept_encoding.lower():
            return response
        content_type = response.headers.get("Content-Type", "")
        if content_type.startswith("text/event-stream"):
            return response
        if not any(content_type.startswith(prefix) for prefix in ("text/", "application/json", "application/javascript")):
            return response
        data = response.get_data()
        if not data or len(data) < 500:
            return response
        gzip_buffer = io.BytesIO()
        with gzip.GzipFile(mode="wb", fileobj=gzip_buffer) as gz:
            gz.write(data)
        response.set_data(gzip_buffer.getvalue())
        response.headers["Content-Encoding"] = "gzip"
        response.headers["Vary"] = "Accept-Encoding"
        response.headers["Content-Length"] = str(len(response.get_data()))
    except Exception:
        logger.exception("Failed to compress response")
    return response


@app.after_request
def _set_api_cache_headers(response: Response) -> Response:
    if request.path.startswith("/api/") and request.method == "GET":
        if request.path == "/api/trade-history" or request.path.startswith("/api/trade-history/"):
            return _compress_response(response)
        if _should_skip_global_api_cache():
            response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
            response.headers["Pragma"] = "no-cache"
            response.headers["Expires"] = "0"
            return _compress_response(response)
        response.headers.setdefault("Cache-Control", "public, max-age=5, stale-while-revalidate=30")
        if response.status_code == 200 and response.is_json:
            try:
                cache_key = _get_api_cache_key()
                _cache_set(cache_key, response.get_json(), ttl=10)
                response.headers['X-Cache-Write'] = 'MISS'
            except Exception:
                logger.exception("Failed to write API response to cache")
    elif request.method == "GET" and response.mimetype == "text/html":
        response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
    return _compress_response(response)

# ── Signal store (background refresh) ────────────────────────────────────────
_sig_store: Dict[str, Dict] = {}
_sig_lock   = threading.Lock()
_last_ref:  Dict[str, float] = {}
_REFRESH_TTL = {"crypto": 30, "forex": 60, "commodities": 60, "indices": 120}

def _store(asset: str, sig: Dict) -> None:
    with _sig_lock:
        _sig_store[asset] = sig

def _is_market_weekend(category: str) -> bool:
    """True when non-crypto markets are closed (weekend window)."""
    if category == "crypto":
        return False
    from datetime import datetime as _dt, timezone as _tz
    _now  = _dt.now(tz=_tz.utc)
    _wd   = _now.weekday()
    _hour = _now.hour
    return (
        _wd == 5
        or (_wd == 6 and _hour < 22)
        or (_wd == 4 and _hour >= 22)
    )

def _due(asset: str) -> bool:
    cat = _cat(asset)
    if _is_market_weekend(cat):
        return False   # non-crypto never refreshes when market is closed
    return (time.time() - _last_ref.get(asset, 0)) >= _REFRESH_TTL.get(cat, 60)

# ── Phase 3 pub/sub buffers ───────────────────────────────────────────────────
_p3_walls: list = []
_p3_hunts: list = []
_p3_wall_lock = threading.Lock()
_p3_hunt_lock = threading.Lock()
_p3_started   = False

# ── Intelligence alerts pub/sub buffer ───────────────────────────────────────
_p7_alerts: list = []
_p7_lock    = threading.Lock()
_p7_started = False
_dashboard_pubsub_started = False
_command_center_cache_invalidated_at = 0.0
_dashboard_pubsub_channels = (
    "signals",
    "SIGNAL_JOURNAL_UPDATE",
    "COMMAND_CENTER_CONTEXT_UPDATE",
    "LIQUIDITY_WALL_DETECTED",
    "STOP_HUNT_DETECTED",
    "INTELLIGENCE_ALERT",
)


def _invalidate_command_center_related_caches(*, min_interval_seconds: float = 3.0) -> None:
    global _command_center_cache_invalidated_at
    now = time.monotonic()
    if now - _command_center_cache_invalidated_at < min_interval_seconds:
        return
    _command_center_cache_invalidated_at = now
    _invalidate_cache_prefixes(
        _COMMAND_CENTER_PAYLOAD_CACHE_KEY,
        "cc_journals",
        "cc_runtime_context",
        "p7_journal",
        "page_overview:command_center:",
        "page_overview:order_flow:",
        "page_overview:playbook_intel:",
        "page_overview:risk_dashboard:",
        "page_overview:system_monitor:",
        "page_overview:market_intelligence:",
        "page_overview:whale_intelligence:",
        "page_overview:intelligence_alerts:",
        "page_overview:architecture_lab:",
    )


def _handle_dashboard_pubsub_message(channel: str, payload: Dict[str, Any]) -> None:
    if channel == "signals":
        asset = str(payload.get("asset") or "")
        if asset:
            _store(asset, payload)
            _last_ref[asset] = time.time()
        _invalidate_command_center_related_caches()
        return
    if channel in {"SIGNAL_JOURNAL_UPDATE", "COMMAND_CENTER_CONTEXT_UPDATE"}:
        _invalidate_command_center_related_caches()
        return
    if channel == "LIQUIDITY_WALL_DETECTED":
        with _p3_wall_lock:
            _p3_walls.append(payload)
            if len(_p3_walls) > 50:
                _p3_walls.pop(0)
        return
    if channel == "STOP_HUNT_DETECTED":
        with _p3_hunt_lock:
            _p3_hunts.append(payload)
            if len(_p3_hunts) > 30:
                _p3_hunts.pop(0)
        return
    if channel == "INTELLIGENCE_ALERT":
        with _p7_lock:
            _p7_alerts.append(payload)
            if len(_p7_alerts) > 100:
                _p7_alerts.pop(0)


def _start_dashboard_pubsub_listener() -> None:
    global _dashboard_pubsub_started, _p3_started, _p7_started
    if _dashboard_pubsub_started:
        _p3_started = True
        _p7_started = True
        return
    _dashboard_pubsub_started = True
    _p3_started = True
    _p7_started = True

    def _listen():
        ps = None
        while True:
            try:
                from services.redis_pool import get_pubsub as _get_pubsub

                ps = _get_pubsub(old_pubsub=ps)
                if ps is None:
                    raise RuntimeError("Redis unavailable")
                ps.subscribe(*_dashboard_pubsub_channels)
                logger.info(
                    "[dashboard] Shared Redis listener active for "
                    + ", ".join(_dashboard_pubsub_channels)
                )
                for msg in ps.listen():
                    if msg.get("type") != "message":
                        continue
                    try:
                        channel = msg.get("channel", "")
                        if isinstance(channel, bytes):
                            channel = channel.decode()
                        data = msg.get("data", "{}")
                        payload = json.loads(data) if isinstance(data, (str, bytes)) else data
                        if isinstance(payload, dict):
                            _handle_dashboard_pubsub_message(str(channel or ""), payload)
                    except Exception as exc:
                        logger.debug(f"[dashboard] shared redis parse error: {exc}")
            except Exception as exc:
                logger.warning(f"[dashboard] Shared Redis listener dropped ({exc}) — reconnecting in 10s")
                time.sleep(10)

    threading.Thread(target=_listen, name="dashboard-redis-listener", daemon=True).start()


def _start_p3_listener():
    _start_dashboard_pubsub_listener()


def _start_p7_listener():
    _start_dashboard_pubsub_listener()

# ── Background signal listener — Redis subscriber ────────────────────────────
# The trading loop publishes every accepted signal to the Redis 'signals'
# channel immediately after the decision engine approves it. The dashboard
# subscribes here and updates _sig_store directly — no duplicate decision runs,
# no wasted CPU. One decision pass per signal, period.
#
# Fallback: if Redis is unavailable, _bg_refresh_fallback() polls the engine
# using get_signal_for_asset() — this is the old behaviour but only activates
# when Redis is genuinely down.
# ─────────────────────────────────────────────────────────────────────────────

def _bg_refresh() -> None:
    """Entry point — prefer Redis subscriber, fall back to polling."""
    _get_sent()
    _get_market_intelligence()
    try:
        from services.redis_pool import is_available as _redis_available
        if _redis_available():
            logger.info("[dashboard] Signal source: shared Redis listener (zero decision re-runs)")
            _start_dashboard_pubsub_listener()
            while True:
                time.sleep(60)
    except Exception:
        pass
    logger.info("[dashboard] Signal source: engine polling fallback (Redis unavailable)")
    _bg_refresh_fallback()


def _bg_refresh_fallback() -> None:
    """Fallback: poll engine when Redis is unavailable. Runs the decision engine once
    per asset per TTL — only used when Redis is genuinely down."""
    from concurrent.futures import ThreadPoolExecutor
    while True:
        try:
            core = _core()
            due  = [(a, c) for a, c in ALL_ASSETS if _due(a)]
            if not due:
                time.sleep(15)
                continue

            def _refresh_one(ac):
                asset, _ = ac
                try:
                    sig = None
                    if core:
                        try:
                            sig = core.get_signal_for_asset(asset)
                        except Exception:
                            pass
                    if not sig:
                        sig = _fallback_signal(asset)
                    if sig:
                        _store(asset, sig)
                except Exception as e:
                    logger.debug(f"[dashboard] refresh {asset}: {e}")
                finally:
                    _last_ref[asset] = time.time()

            with ThreadPoolExecutor(max_workers=max(1, int(DASHBOARD_BG_REFRESH_WORKERS or 6))) as pool:
                list(pool.map(_refresh_one, due))
        except Exception as e:
            logger.error(f"[dashboard] bg_refresh_fallback: {e}")
        time.sleep(15)

def _fallback_signal(asset: str) -> Optional[Dict]:
    """Generate a simple RSI-based fallback signal when engine unavailable."""
    df = _ohlcv(asset, "15m", 50)
    if df is None or df.empty:
        return None
    try:
        from indicators.technical import TechnicalIndicators
        df = TechnicalIndicators.add_all_indicators(df)
    except Exception:
        pass
    price = float(df["close"].iloc[-1])
    rsi   = float(df["rsi"].iloc[-1]) if "rsi" in df.columns else 50.0
    atr   = float(df["atr"].iloc[-1]) if "atr" in df.columns else price * 0.01
    if rsi < 35:    d = "BUY"
    elif rsi > 65:  d = "SELL"
    else:           return None
    sl = price - atr * 1.5 if d == "BUY" else price + atr * 1.5
    tp = price + atr * 2.0 if d == "BUY" else price - atr * 2.0
    return {
        "asset": asset, "category": _cat(asset),
        "signal": d, "direction": d,
        "confidence": round(0.60 + abs(rsi - 50) / 100, 3),
        "entry_price": round(price, 6),
        "stop_loss":   round(sl, 6),
        "take_profit": round(tp, 6),
        "strategy_id": "Indicators",
        "market_open": is_market_open_for_asset(asset)[0],
        "timestamp":   datetime.now().isoformat(),
        "generated_at": datetime.now().strftime("%H:%M:%S"),
    }

# ── Sentiment prewarm ─────────────────────────────────────────────────────────
def _prewarm_sentiment() -> None:
    time.sleep(12)
    try:
        news_enabled = bool(NEWS_SENTIMENT_ENABLED or NEWS_REDDIT_ENABLED or NEWS_RSS_ENABLED)
        logger.info(
            "[dashboard] Pre-warming sentiment cache..."
            if news_enabled
            else "[dashboard] Pre-warming market-derived sentiment cache (news feeds disabled)"
        )
        _get_sent()
        with app.test_request_context("/api/sentiment/dashboard"):
            try:
                payload = _response_to_dict(_call_view(api_sentiment_dashboard))
                if payload.get("success"):
                    logger.info("[dashboard] sentiment/dashboard warmed")
                else:
                    logger.debug(f"[dashboard] sentiment/dashboard prewarm returned: {payload.get('error') or payload}")
            except Exception as e:
                logger.debug(f"[dashboard] sentiment/dashboard prewarm: {e}")
        with app.test_request_context("/api/sentiment/by-asset"):
            try:
                payload = _response_to_dict(_call_view(api_sentiment_by_asset))
                if payload.get("success"):
                    logger.info("[dashboard] sentiment/by-asset warmed")
                else:
                    logger.debug(f"[dashboard] sentiment/by-asset prewarm returned: {payload.get('error') or payload}")
            except Exception as e:
                logger.debug(f"[dashboard] sentiment/by-asset prewarm: {e}")
    except Exception as e:
        logger.debug(f"[dashboard] sentiment prewarm failed: {e}")


def _prewarm_command_center_payload() -> None:
    try:
        scheduled = _trigger_dashboard_payload_refresh(
            _COMMAND_CENTER_PAYLOAD_REFRESH_KEY,
            builder=_build_command_center_payload_with_budget,
            cache_key=_COMMAND_CENTER_PAYLOAD_CACHE_KEY,
            ttl=_COMMAND_CENTER_CACHE_TTL,
            last_good_key=_COMMAND_CENTER_PAYLOAD_LAST_GOOD_KEY,
            last_good_ttl=_COMMAND_CENTER_LAST_GOOD_TTL,
        )
        if scheduled:
            logger.info("[dashboard] command-center prewarm scheduled")
    except Exception as exc:
        logger.debug(f"[dashboard] command-center prewarm failed: {exc}")


def _prewarm_page_overviews() -> None:
    def _worker() -> None:
        time.sleep(2)
        try:
            pages = sorted(_PAGE_OVERVIEW_VALID_PAGES)
        except Exception:
            pages = []

        for page in pages:
            try:
                _get_cached_page_overview_payload(page, 30, force_refresh=True)
                logger.debug(f"[dashboard] page overview prewarmed: {page}")
            except Exception as exc:
                logger.debug(f"[dashboard] page overview prewarm failed for {page}: {exc}")
            time.sleep(0.2)

    threading.Thread(target=_worker, name="PageOverviewPrewarm", daemon=True).start()

# ── Win rate normaliser (DB returns decimal 0.XX, we display as %) ────────────
def _wr(raw) -> float:
    v = float(raw or 0)
    return round(v * 100, 2) if v <= 1.0 else round(v, 2)


def _clean_text_list(values: Any, *, limit: int = 0) -> List[str]:
    items: List[str] = []
    if isinstance(values, (list, tuple, set)):
        source = list(values)
    elif isinstance(values, str):
        source = [values]
    else:
        source = []
    for value in source:
        text = str(value or "").strip()
        if text:
            items.append(text)
    return items[:limit] if limit > 0 else items


def _join_text_list(values: Any, *, limit: int = 0) -> str:
    parts = _clean_text_list(values, limit=limit)
    return " · ".join(parts)


def _reason_bucket_counts(reasons: Any) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for reason in _clean_text_list(reasons):
        label = reason.split(":", 1)[0].strip() or "unknown"
        counts[label] = counts.get(label, 0) + 1
    return counts


def _extract_execution_feedback_fields(metadata: Any) -> Dict[str, Any]:
    meta = metadata if isinstance(metadata, dict) else {}
    feedback = meta.get("execution_feedback") if isinstance(meta.get("execution_feedback"), dict) else {}
    policy = meta.get("execution_feedback_policy") if isinstance(meta.get("execution_feedback_policy"), dict) else {}
    relief_flags = meta.get("execution_relief_flags") if isinstance(meta.get("execution_relief_flags"), dict) else {}
    notes = feedback.get("notes") if isinstance(feedback.get("notes"), list) else []
    policy_notes = policy.get("notes") if isinstance(policy.get("notes"), list) else []
    return {
        "execution_feedback": feedback,
        "execution_feedback_policy": policy,
        "execution_quality_score": float(
            feedback.get("quality_score", meta.get("execution_quality_score", policy.get("avg_quality_score", 0.0))) or 0.0
        ),
        "execution_feedback_sample_count": int(
            meta.get("execution_feedback_sample_count", policy.get("sample_count", feedback.get("sample_count", 0))) or 0
        ),
        "target_rr_multiplier": float(meta.get("target_rr_multiplier", policy.get("target_rr_multiplier", 1.0)) or 1.0),
        "stop_buffer_multiplier": float(meta.get("stop_buffer_multiplier", policy.get("stop_buffer_multiplier", 1.0)) or 1.0),
        "execution_notes": [str(n) for n in (notes or policy_notes)[:4]],
        "exit_family": str(feedback.get("exit_family") or ""),
        "rr_realized": float(feedback.get("rr_realized", 0.0) or 0.0),
        "target_capture": float(feedback.get("target_capture", 0.0) or 0.0),
        "premature_stop": bool(feedback.get("premature_stop")),
        "late_entry": bool(feedback.get("late_entry")),
        "target_miss": bool(feedback.get("target_miss")),
        "late_entry_risk_score": float(meta.get("late_entry_risk_score", 0.0) or 0.0),
        "late_entry_risk_reasons": _clean_text_list(meta.get("late_entry_risk_reasons"), limit=4),
        "execution_hard_blocks": _clean_text_list(meta.get("execution_hard_blocks"), limit=4),
        "execution_relief_flags": relief_flags,
        "strong_fx_crypto_candidate": bool(relief_flags.get("strong_fx_crypto_candidate")),
        "elite_supported_candidate": bool(relief_flags.get("elite_supported_candidate")),
    }


def _extract_memory_fields(metadata: Any) -> Dict[str, Any]:
    meta = metadata if isinstance(metadata, dict) else {}
    memory = meta.get("setup_memory") if isinstance(meta.get("setup_memory"), dict) else {}
    fingerprint = meta.get("setup_memory_fingerprint") if isinstance(meta.get("setup_memory_fingerprint"), dict) else {}
    notes = memory.get("notes") if isinstance(memory.get("notes"), list) else []
    return {
        "setup_memory": memory,
        "setup_memory_fingerprint": fingerprint,
        "memory_score": float(meta.get("memory_score", memory.get("memory_score", 0.0)) or 0.0),
        "memory_edge": float(meta.get("memory_edge", memory.get("memory_edge", 0.0)) or 0.0),
        "memory_sample_count": int(meta.get("memory_sample_count", memory.get("sample_count", 0)) or 0),
        "memory_win_rate": float(meta.get("memory_win_rate", memory.get("win_rate", 0.0)) or 0.0),
        "memory_similarity": float(meta.get("memory_similarity", memory.get("avg_similarity", 0.0)) or 0.0),
        "memory_notes": [str(n) for n in notes[:4]],
        "memory_regime": str(fingerprint.get("regime") or ""),
        "memory_setup_style": str(fingerprint.get("setup_style") or ""),
    }


def _extract_opportunity_fields(metadata: Any) -> Dict[str, Any]:
    meta = metadata if isinstance(metadata, dict) else {}
    breakdown = meta.get("opportunity_breakdown") if isinstance(meta.get("opportunity_breakdown"), dict) else {}
    return {
        "opportunity_score": float(meta.get("opportunity_score", 0.0) or 0.0),
        "opportunity_rank": int(meta.get("opportunity_rank", 0) or 0),
        "opportunity_breakdown": {str(k): float(v or 0.0) for k, v in breakdown.items()},
    }


def _depth_mode_from_meta(micro: Dict[str, Any]) -> str:
    if bool(micro.get("depth_available")):
        return "true_depth"
    if bool(micro.get("synthetic_depth_available")):
        return "synthetic_depth"
    return "top_of_book"


def _broker_context_state(broker: Dict[str, Any]) -> str:
    agreement_state = str(broker.get("quote_agreement_state") or "").lower()
    spread_regime = str(broker.get("spread_regime") or "").lower()
    quote_quality_state = str(broker.get("quote_quality_state") or "").lower()
    score = float(broker.get("score", 0.0) or 0.0)
    transition_risk = float(broker.get("market_transition_risk", 0.0) or 0.0)
    if (
        agreement_state in {"divergent", "severe_divergence"}
        or spread_regime in {"stressed", "extreme", "wide"}
        or quote_quality_state in {"stale", "delayed"}
        or transition_risk >= 0.65
        or bool(broker.get("market_state_changed"))
    ):
        return "fragile"
    if (
        score >= 0.65
        and agreement_state in {"strong", "aligned"}
        and spread_regime in {"tight", "normal"}
        and quote_quality_state in {"fresh", "aging"}
    ):
        return "supportive"
    return "mixed"


def _cross_asset_context_state(cross: Dict[str, Any]) -> str:
    alignment = float(cross.get("alignment", cross.get("score", 0.0)) or 0.0)
    confidence = float(cross.get("confidence", 0.0) or 0.0)
    if confidence < 0.20:
        return "mixed"
    if alignment >= 0.20:
        return "supportive"
    if alignment <= -0.20:
        return "conflicted"
    return "mixed"


def _extract_signal_intelligence_fields(metadata: Any) -> Dict[str, Any]:
    meta = metadata if isinstance(metadata, dict) else {}
    broker = meta.get("broker_quality") if isinstance(meta.get("broker_quality"), dict) else {}
    micro = meta.get("market_microstructure") if isinstance(meta.get("market_microstructure"), dict) else {}
    cross = meta.get("cross_asset_context") if isinstance(meta.get("cross_asset_context"), dict) else {}
    adaptive = meta.get("adaptive_policy") if isinstance(meta.get("adaptive_policy"), dict) else {}
    recent = adaptive.get("recent_review_profile") if isinstance(adaptive.get("recent_review_profile"), dict) else {}
    recent_notes = recent.get("notes") if isinstance(recent.get("notes"), list) else []
    if not recent_notes:
        adaptive_notes = adaptive.get("notes") if isinstance(adaptive.get("notes"), list) else []
        recent_notes = adaptive_notes

    broker_score = float(meta.get("broker_quality_score", broker.get("score", 0.0)) or 0.0)
    micro_score = float(meta.get("microstructure_score", micro.get("score", 0.0)) or 0.0)
    cross_score = float(meta.get("cross_asset_score", cross.get("score", 0.0)) or 0.0)
    cross_alignment = float(meta.get("cross_asset_alignment", cross.get("alignment", cross.get("score", 0.0))) or 0.0)
    cross_confidence = float(meta.get("cross_asset_confidence", cross.get("confidence", 0.0)) or 0.0)
    depth_mode = _depth_mode_from_meta(micro)
    broker_context = _broker_context_state(broker)
    cross_context = _cross_asset_context_state(
        {
            "alignment": cross_alignment,
            "score": cross_score,
            "confidence": cross_confidence,
        }
    )

    return {
        "broker_quality": broker,
        "broker_quality_score": broker_score,
        "broker_primary_provider": str(broker.get("primary_provider") or ""),
        "broker_comparison_provider": str(broker.get("comparison_provider") or ""),
        "broker_agreement_state": str(broker.get("quote_agreement_state") or ""),
        "broker_quote_quality_state": str(broker.get("quote_quality_state") or ""),
        "broker_spread_regime": str(broker.get("spread_regime") or ""),
        "broker_market_transition_risk": float(broker.get("market_transition_risk", 0.0) or 0.0),
        "broker_context": broker_context,
        "market_microstructure": micro,
        "microstructure_score": micro_score,
        "micro_pressure_direction": str(micro.get("pressure_direction") or ""),
        "stop_hunt_risk": float(micro.get("stop_hunt_risk", 0.0) or 0.0),
        "exhaustion_risk": float(micro.get("exhaustion_risk", 0.0) or 0.0),
        "depth_available": bool(micro.get("depth_available")),
        "synthetic_depth_available": bool(micro.get("synthetic_depth_available")),
        "depth_mode": depth_mode,
        "depth_provider": str(micro.get("depth_provider") or micro.get("source") or ""),
        "microstructure_source": str(micro.get("microstructure_source") or ""),
        "cross_asset_context": cross,
        "cross_asset_score": cross_score,
        "cross_asset_alignment": cross_alignment,
        "cross_asset_confidence": cross_confidence,
        "cross_asset_state": str(meta.get("cross_asset_state", cross.get("state", "")) or ""),
        "cross_asset_primary_peer": str(meta.get("cross_asset_primary_peer", cross.get("dominant_peer", "")) or ""),
        "cross_asset_primary_relation": str(meta.get("cross_asset_primary_relation", cross.get("dominant_relation", "")) or ""),
        "cross_asset_context_state": cross_context,
        "recent_review_profile": recent,
        "recent_pattern_sample_count": int(recent.get("sample_count", 0) or 0),
        "recent_pattern_notes": [str(n) for n in recent_notes[:4]],
        "recent_pattern_block_new_entries": bool(recent.get("block_new_entries")),
        "recent_pattern_target_rr_multiplier": float(recent.get("target_rr_multiplier", 1.0) or 1.0),
    }


def _extract_entry_structure_fields(metadata: Any) -> Dict[str, Any]:
    meta = metadata if isinstance(metadata, dict) else {}
    structure = meta.get("market_structure") if isinstance(meta.get("market_structure"), dict) else {}
    rejected_reasons = _clean_text_list(meta.get("rejected_reasons"), limit=6)
    blocked_reason = str(meta.get("blocked_reason") or "").strip()
    exact_kill_reason = str(
        meta.get("exact_kill_reason")
        or meta.get("execution_kill_reason")
        or meta.get("kill_reason")
        or blocked_reason
        or (rejected_reasons[0] if rejected_reasons else "")
    ).strip()
    return {
        "breakout_retest_ready": bool(meta.get("breakout_retest_ready")),
        "first_pullback_ready": bool(meta.get("first_pullback_ready")),
        "entry_confirmation_ready": bool(meta.get("entry_confirmation_ready")),
        "entry_confirmation_count": int(meta.get("entry_confirmation_count", 0) or 0),
        "entry_confirmation_bars_required": int(meta.get("entry_confirmation_bars_required", 0) or 0),
        "failed_opposite_move_confirmed": bool(meta.get("failed_opposite_move_confirmed")),
        "liquidity_sweep_reclaim": bool(meta.get("liquidity_sweep_reclaim")),
        "liquidity_sweep_buy": bool(meta.get("liquidity_sweep_buy", structure.get("liquidity_sweep_buy"))),
        "liquidity_sweep_sell": bool(meta.get("liquidity_sweep_sell", structure.get("liquidity_sweep_sell"))),
        "pattern_family": str(meta.get("pattern_family") or ""),
        "structure_bias": str(meta.get("structure_bias") or structure.get("structure_bias") or ""),
        "elite_pattern_rank": float(meta.get("elite_pattern_rank", 0.0) or 0.0),
        "cluster_penalty": float(meta.get("cluster_penalty", 0.0) or 0.0),
        "impulse_age_bars": int(meta.get("impulse_age_bars", 0) or 0),
        "extension_score": float(meta.get("extension_score", 0.0) or 0.0),
        "candle_quality_score": float(meta.get("candle_quality_score", 0.0) or 0.0),
        "session_quality_score": float(meta.get("session_quality_score", 0.0) or 0.0),
        "target_efficiency_score": float(meta.get("target_efficiency_score", 0.0) or 0.0),
        "support_proximity": float(meta.get("support_proximity", 0.0) or 0.0),
        "resistance_proximity": float(meta.get("resistance_proximity", 0.0) or 0.0),
        "distance_to_support": float(structure.get("distance_to_support", 0.0) or 0.0),
        "distance_to_resistance": float(structure.get("distance_to_resistance", 0.0) or 0.0),
        "support_levels": list(structure.get("support_levels") or []),
        "resistance_levels": list(structure.get("resistance_levels") or []),
        "session_label": str(meta.get("session_label") or meta.get("session") or ""),
        "regime_policy_summary": str(meta.get("regime_policy_summary") or ""),
        "regime_policy": str(meta.get("regime_policy") or ""),
        "regime_label": str(meta.get("regime_label") or ""),
        "market_review_notes": _join_text_list(meta.get("market_review_notes"), limit=4),
        "execution_review_notes": _join_text_list(meta.get("execution_review_notes"), limit=4),
        "blocked_reason": blocked_reason,
        "rejected_reasons": rejected_reasons,
        "rejected_reason_buckets": _reason_bucket_counts(rejected_reasons),
        "execution_kill_reason": str(meta.get("execution_kill_reason") or meta.get("kill_reason") or "").strip(),
        "exact_kill_reason": exact_kill_reason,
    }


def _enrich_signal_like_row(row: Any) -> Dict[str, Any]:
    if not isinstance(row, dict):
        return {}
    item = dict(row)
    metadata = item.get("metadata") if isinstance(item.get("metadata"), dict) else {}
    asset = str(item.get("asset") or item.get("canonical_asset") or item.get("symbol") or "")
    category = str(item.get("category") or "")
    source = metadata or item
    if metadata:
        item["metadata"] = metadata
    item.update(_extract_entry_structure_fields(source))
    item.update(_extract_execution_feedback_fields(source))
    item.update(_extract_memory_fields(source))
    item.update(_extract_opportunity_fields(source))
    item.update(_extract_signal_intelligence_fields(source))
    item.update(_extract_market_data_provenance_fields(source, asset=asset, category=category))

    if item.get("confidence") in (None, "", 0, 0.0):
        for key in ("final_confidence", "final_conf", "confidence_score", "final_policy_score", "opportunity_score"):
            try:
                value = item.get(key)
                if value not in (None, ""):
                    item["confidence"] = float(value)
                    break
            except Exception:
                continue

    if not item.get("direction"):
        item["direction"] = str(item.get("signal") or item.get("side") or "").upper()
    if not item.get("signal") and item.get("direction"):
        item["signal"] = str(item.get("direction") or "").upper()

    playbook_name = _playbook_name_from_payload(item)
    if not playbook_name:
        raw_family = str(item.get("pattern_family") or "").strip()
        playbook_name = raw_family.replace("_", " ") if raw_family else ""
    if playbook_name and not item.get("playbook_name"):
        item["playbook_name"] = playbook_name

    display_meta = dict(metadata)
    for key in (
        "playbook_name",
        "strategy_id",
        "session_label",
        "session",
        "playbook_session",
        "pattern_family",
        "playbook_entry_style",
        "entry_style",
        "entry_confirmation_ready",
        "entry_confirmation_count",
        "entry_confirmation_bars_required",
        "breakout_retest_ready",
        "first_pullback_ready",
        "failed_opposite_move_confirmed",
        "liquidity_sweep_reclaim",
        "elite_pattern_rank",
        "cluster_penalty",
        "impulse_age_bars",
        "extension_score",
        "candle_quality_score",
        "session_quality_score",
        "target_efficiency_score",
        "regime_policy_summary",
        "regime_policy",
        "regime_label",
        "execution_kill_reason",
        "kill_reason",
        "blocked_reason",
        "market_review_notes",
        "execution_review_notes",
    ):
        value = item.get(key)
        if key not in display_meta and value not in (None, "", [], {}):
            display_meta[key] = value
    if "playbook_name" not in display_meta and playbook_name:
        display_meta["playbook_name"] = playbook_name
    item["metadata"] = display_meta
    return item


def _extract_market_data_provenance_fields(
    metadata: Any,
    *,
    asset: str = "",
    category: str = "",
) -> Dict[str, Any]:
    meta = metadata if isinstance(metadata, dict) else {}
    market_data = meta.get("market_data") if isinstance(meta.get("market_data"), dict) else {}
    price_meta = market_data.get("price") if isinstance(market_data.get("price"), dict) else {}
    ohlcv_meta = market_data.get("ohlcv") if isinstance(market_data.get("ohlcv"), dict) else {}
    descriptor = _chart_asset_descriptor(asset, category) if asset else {}
    primary_provider = str(descriptor.get("primary_provider") or "")
    secondary_provider = str(descriptor.get("secondary_provider") or "")
    quote_mode = str(descriptor.get("quote_mode") or "")
    return {
        "history_source": str(ohlcv_meta.get("source") or ""),
        "history_source_class": str(ohlcv_meta.get("source_class") or ""),
        "history_provider_family": str(ohlcv_meta.get("provider_family") or ohlcv_meta.get("source") or ""),
        "live_source": str(price_meta.get("source") or primary_provider or ""),
        "live_source_class": str(price_meta.get("source_class") or ""),
        "live_realtime": bool(price_meta.get("realtime")) if price_meta else False,
        "runtime_primary_provider": primary_provider,
        "runtime_secondary_provider": secondary_provider,
        "quote_mode": quote_mode,
    }


def _coerce_utc_datetime(value: Any) -> Optional[datetime]:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        raw = str(value).strip()
        if not raw:
            return None
        raw = raw.replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(raw)
        except Exception:
            return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _is_partial_close_trade_row(trade: Dict[str, Any]) -> bool:
    metadata = dict(trade.get("metadata") or {})
    if bool(metadata.get("is_partial_close")):
        return True
    if metadata.get("parent_trade_id") not in (None, ""):
        return True
    trade_id = str(trade.get("trade_id") or "")
    exit_reason = str(trade.get("exit_reason") or "").lower()
    return ("-PT" in trade_id) or exit_reason.startswith("partial tp")


def _playbook_name_from_trade(trade: Dict[str, Any]) -> str:
    metadata = dict(trade.get("metadata") or {})
    direct = str(metadata.get("playbook_name") or "").strip()
    if direct:
        return direct
    strategy_id = str(trade.get("strategy_id") or "").strip()
    if strategy_id.startswith("playbook_"):
        return strategy_id[len("playbook_") :]
    if strategy_id == "playbook_runtime":
        return "playbook_runtime"
    return ""


def _playbook_name_from_payload(payload: Dict[str, Any]) -> str:
    metadata = dict(payload.get("metadata") or {})
    direct = str(payload.get("playbook_name") or metadata.get("playbook_name") or "").strip()
    if direct:
        return direct
    strategy_id = str(payload.get("strategy_id") or "").strip()
    if strategy_id.startswith("playbook_"):
        return strategy_id[len("playbook_") :]
    if strategy_id == "playbook_runtime":
        return "playbook_runtime"
    return ""


def _playbook_performance_bucket(container: Dict[str, Dict[str, Any]], label: str) -> Dict[str, Any]:
    return container.setdefault(
        label,
        {
            "label": label,
            "trade_count": 0,
            "decisive_count": 0,
            "win_count": 0,
            "gross_win": 0.0,
            "gross_loss": 0.0,
            "total_pnl": 0.0,
            "rr_values": [],
            "hold_minutes": [],
            "stop_exit_count": 0,
        },
    )


def _playbook_performance_trade_snapshot(trade: Any, cutoff: datetime) -> Dict[str, Any] | None:
    if not isinstance(trade, dict) or _is_partial_close_trade_row(trade):
        return None
    playbook_name = _playbook_name_from_trade(trade)
    if not playbook_name:
        return None
    event_time = _coerce_utc_datetime(trade.get("exit_time") or trade.get("entry_time"))
    if event_time is not None and event_time < cutoff:
        return None

    metadata = dict(trade.get("metadata") or {})
    execution = _extract_execution_feedback_fields(metadata)
    pnl = float(trade.get("pnl") or 0.0)
    entry_time = _coerce_utc_datetime(trade.get("entry_time") or trade.get("open_time"))
    exit_time = _coerce_utc_datetime(trade.get("exit_time"))
    exit_reason = str(trade.get("exit_reason") or metadata.get("exit_reason") or "").strip().lower()
    hold_minutes_value = None
    if entry_time is not None and exit_time is not None and exit_time >= entry_time:
        hold_minutes_value = (exit_time - entry_time).total_seconds() / 60.0

    return {
        "playbook_name": playbook_name,
        "asset_label": str(trade.get("canonical_asset") or trade.get("asset") or "").strip() or "UNKNOWN",
        "pnl": pnl,
        "rr_realized": float(execution.get("rr_realized", 0.0) or 0.0),
        "hold_minutes_value": hold_minutes_value,
        "is_win": pnl > 0.0,
        "is_loss": pnl < 0.0,
        "is_stop_exit": "stop loss" in exit_reason,
    }


def _finalize_playbook_performance_rows(rows: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    output: List[Dict[str, Any]] = []
    for item in rows.values():
        decisive = int(item.pop("decisive_count", 0) or 0)
        wins = int(item.pop("win_count", 0) or 0)
        gross_win_local = float(item.pop("gross_win", 0.0) or 0.0)
        gross_loss_local = float(item.pop("gross_loss", 0.0) or 0.0)
        rr_local = list(item.pop("rr_values", []) or [])
        hold_local = list(item.pop("hold_minutes", []) or [])
        stop_local = int(item.pop("stop_exit_count", 0) or 0)
        trade_local = int(item.get("trade_count", 0) or 0)
        item["win_rate"] = round((wins / decisive) * 100.0, 1) if decisive else 0.0
        item["profit_factor"] = round(gross_win_local / gross_loss_local, 2) if gross_loss_local else (round(gross_win_local, 2) if gross_win_local else 0.0)
        item["avg_rr_realized"] = round(sum(rr_local) / len(rr_local), 3) if rr_local else 0.0
        item["avg_hold_minutes"] = round(sum(hold_local) / len(hold_local), 1) if hold_local else 0.0
        item["stop_exit_rate"] = round((stop_local / trade_local) * 100.0, 1) if trade_local else 0.0
        item["total_pnl"] = round(float(item.get("total_pnl", 0.0) or 0.0), 2)
        output.append(item)
    output.sort(
        key=lambda row: (
            float(row.get("total_pnl", 0.0) or 0.0),
            float(row.get("win_rate", 0.0) or 0.0),
            int(row.get("trade_count", 0) or 0),
        ),
        reverse=True,
    )
    return output[:5]


def _summarize_playbook_performance(trades: List[Dict[str, Any]], *, days_back: int = 30) -> Dict[str, Any]:
    cutoff = datetime.now(timezone.utc) - timedelta(days=max(1, int(days_back or 30)))
    playbook_rows: Dict[str, Dict[str, Any]] = {}
    asset_rows: Dict[str, Dict[str, Any]] = {}
    trade_count = 0
    decisive_count = 0
    win_count = 0
    gross_win = 0.0
    gross_loss = 0.0
    rr_values: List[float] = []
    hold_minutes: List[float] = []
    stop_exit_count = 0

    for trade in list(trades or []):
        summary = _playbook_performance_trade_snapshot(trade, cutoff)
        if summary is None:
            continue
        pnl = float(summary["pnl"])
        trade_count += 1
        if summary["is_win"] or summary["is_loss"]:
            decisive_count += 1
            if summary["is_win"]:
                win_count += 1
                gross_win += pnl
            else:
                gross_loss += abs(pnl)
        rr_realized = float(summary["rr_realized"] or 0.0)
        if abs(rr_realized) > 1e-9:
            rr_values.append(rr_realized)
        hold_minutes_value = summary["hold_minutes_value"]
        if hold_minutes_value is not None:
            hold_minutes.append(hold_minutes_value)
        if summary["is_stop_exit"]:
            stop_exit_count += 1

        for container, label in ((playbook_rows, summary["playbook_name"]), (asset_rows, summary["asset_label"])):
            row = _playbook_performance_bucket(container, label)
            row["trade_count"] += 1
            row["total_pnl"] += pnl
            if abs(rr_realized) > 1e-9:
                row["rr_values"].append(rr_realized)
            if hold_minutes_value is not None:
                row["hold_minutes"].append(hold_minutes_value)
            if summary["is_stop_exit"]:
                row["stop_exit_count"] += 1
            if summary["is_win"] or summary["is_loss"]:
                row["decisive_count"] += 1
                if summary["is_win"]:
                    row["win_count"] += 1
                    row["gross_win"] += pnl
                else:
                    row["gross_loss"] += abs(pnl)
    playbooks = _finalize_playbook_performance_rows(playbook_rows)
    assets = _finalize_playbook_performance_rows(asset_rows)
    return {
        "summary": {
            "trade_count": trade_count,
            "decisive_trade_count": decisive_count,
            "win_rate": round((win_count / decisive_count) * 100.0, 1) if decisive_count else 0.0,
            "profit_factor": round(gross_win / gross_loss, 2) if gross_loss else (round(gross_win, 2) if gross_win else 0.0),
            "avg_rr_realized": round(sum(rr_values) / len(rr_values), 3) if rr_values else 0.0,
            "avg_hold_minutes": round(sum(hold_minutes) / len(hold_minutes), 1) if hold_minutes else 0.0,
            "stop_exit_rate": round((stop_exit_count / trade_count) * 100.0, 1) if trade_count else 0.0,
            "top_playbook": str(playbooks[0]["label"]) if playbooks else "",
        },
        "playbooks": playbooks,
        "assets": assets,
    }


def _summarize_signal_diagnostics(rows: Any) -> Dict[str, Any]:
    items = list(rows or [])
    total = 0
    broker_supportive = 0
    broker_fragile = 0
    true_depth = 0
    synthetic_depth = 0
    cross_support = 0
    cross_conflict = 0
    recent_pattern_blocks = 0

    for row in items:
        if not isinstance(row, dict):
            continue
        enriched = _enrich_signal_like_row(row)
        total += 1
        broker_context = str(enriched.get("broker_context") or "").lower()
        if broker_context == "supportive":
            broker_supportive += 1
        elif broker_context == "fragile":
            broker_fragile += 1

        depth_mode = str(enriched.get("depth_mode") or "").lower()
        if depth_mode == "true_depth":
            true_depth += 1
        elif depth_mode == "synthetic_depth":
            synthetic_depth += 1

        cross_context = str(enriched.get("cross_asset_context_state") or enriched.get("cross_asset_context") or "").lower()
        if cross_context == "supportive":
            cross_support += 1
        elif cross_context == "conflicted":
            cross_conflict += 1

        if bool(enriched.get("recent_pattern_block_new_entries")):
            recent_pattern_blocks += 1

    summary_parts = [
        f"Fragile {broker_fragile}" if broker_fragile else "",
        f"True depth {true_depth}" if true_depth else "",
        f"Synthetic {synthetic_depth}" if synthetic_depth else "",
        f"Cross conflicts {cross_conflict}" if cross_conflict else "",
        f"Pattern blocks {recent_pattern_blocks}" if recent_pattern_blocks else "",
    ]
    summary_label = " · ".join([part for part in summary_parts if part]) or "No active diagnostics"

    return {
        "count": total,
        "broker_supportive_count": broker_supportive,
        "broker_fragile_count": broker_fragile,
        "true_depth_count": true_depth,
        "synthetic_depth_count": synthetic_depth,
        "cross_support_count": cross_support,
        "cross_conflict_count": cross_conflict,
        "recent_pattern_block_count": recent_pattern_blocks,
        "summary_label": summary_label,
    }


def _summarize_near_misses(journals: Any, *, limit: int = 6) -> List[Dict[str, Any]]:
    items_by_asset: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for row in list(journals or []):
        if not isinstance(row, dict):
            continue
        if str(row.get("decision") or "").upper() == "SURVIVED":
            continue
        asset = str(row.get("asset") or "").strip()
        if not asset:
            continue
        opportunity = float(row.get("opportunity_score") or 0.0)
        setup_quality = float(row.get("setup_quality") or 0.0)
        alignment = float(row.get("alignment_score") or 0.0)
        final_score = float(row.get("final_policy_score") or 0.0)
        stop_hunt_risk = float(row.get("stop_hunt_risk") or 0.0)
        exhaustion_risk = float(row.get("exhaustion_risk") or 0.0)
        rank_score = (
            max(0.0, opportunity) * 0.55
            + max(0.0, setup_quality) * 0.2
            + max(0.0, alignment) * 0.15
            + max(0.0, final_score) * 0.1
        )
        confidence = row.get("confidence")
        for confidence_key in ("final_confidence", "final_conf", "final_policy_score", "opportunity_score"):
            if confidence not in (None, "", 0, 0.0):
                break
            confidence = row.get(confidence_key)

        base_row = dict(row)
        metadata = dict(base_row.get("metadata") or {})
        for key in (
            "playbook_name",
            "strategy_id",
            "session_label",
            "session",
            "playbook_session",
            "pattern_family",
            "playbook_entry_style",
            "entry_style",
            "entry_confirmation_ready",
            "entry_confirmation_count",
            "entry_confirmation_bars_required",
            "breakout_retest_ready",
            "first_pullback_ready",
            "failed_opposite_move_confirmed",
            "liquidity_sweep_reclaim",
            "elite_pattern_rank",
            "cluster_penalty",
            "impulse_age_bars",
            "extension_score",
            "candle_quality_score",
            "session_quality_score",
            "target_efficiency_score",
            "regime_policy_summary",
            "regime_policy",
            "regime_label",
            "market_review_notes",
            "execution_review_notes",
            "blocked_reason",
            "kill_reason",
        ):
            value = base_row.get(key)
            if key not in metadata and value not in (None, "", [], {}):
                metadata[key] = value
        if metadata:
            base_row["metadata"] = metadata

        base_row.update(
            {
                "asset": asset,
                "direction": str(row.get("direction") or row.get("signal") or "").upper(),
                "signal": str(row.get("direction") or row.get("signal") or "").upper(),
                "confidence": confidence,
                "killed_by": str(row.get("killed_by") or row.get("last_layer") or ""),
                "reason": str(row.get("kill_reason") or row.get("final_policy_reason") or ""),
                "opportunity_score": round(opportunity, 3),
                "setup_quality": round(setup_quality, 3),
                "alignment_score": round(alignment, 3),
                "structure_bias": str(row.get("structure_bias") or ""),
                "depth_mode": str(row.get("depth_mode") or ""),
                "broker_agreement_state": str(row.get("broker_agreement_state") or ""),
                "microstructure_source": str(row.get("microstructure_source") or ""),
                "top_positive_factor": str(row.get("top_positive_factor") or ""),
                "top_negative_factor": str(row.get("top_negative_factor") or ""),
                "stop_hunt_risk": round(stop_hunt_risk, 3),
                "exhaustion_risk": round(exhaustion_risk, 3),
                "rank_score": round(rank_score, 4),
                "event_time": _journal_event_iso(row),
                "event_ts": _journal_event_sort_value(row),
                "decision_kind": "blocked",
                "decision_state": "blocked",
            }
        )
        item = _enrich_signal_like_row(
            base_row
        )
        item_key = (
            asset,
            str(item.get("direction") or "").upper(),
        )
        existing = items_by_asset.get(item_key)
        if existing is None:
            items_by_asset[item_key] = item
            continue
        item_ts = float(item.get("event_ts", 0.0) or 0.0)
        existing_ts = float(existing.get("event_ts", 0.0) or 0.0)
        if item_ts > existing_ts or (
            abs(item_ts - existing_ts) < 1e-6
            and float(item.get("rank_score", 0.0) or 0.0) > float(existing.get("rank_score", 0.0) or 0.0)
        ):
            items_by_asset[item_key] = item
    items = list(items_by_asset.values())
    items.sort(
        key=lambda item: (
            float(item.get("event_ts", 0.0) or 0.0),
            float(item.get("rank_score", 0.0) or 0.0),
            float(item.get("opportunity_score", 0.0) or 0.0),
            float(item.get("setup_quality", 0.0) or 0.0),
        ),
        reverse=True,
    )
    return items[: max(1, int(limit or 6))]


def _journal_event_iso(row: Dict[str, Any]) -> str:
    if not isinstance(row, dict):
        return ""
    raw_ts = row.get("ts")
    try:
        if raw_ts not in (None, ""):
            return datetime.fromtimestamp(float(raw_ts), tz=timezone.utc).isoformat()
    except Exception:
        pass
    for key in ("event_time", "timestamp", "time", "created_at"):
        raw = str(row.get(key) or "").strip()
        if raw:
            return raw
    return ""


def _journal_event_sort_value(row: Dict[str, Any]) -> float:
    if not isinstance(row, dict):
        return 0.0
    raw_ts = row.get("ts")
    try:
        if raw_ts not in (None, ""):
            return float(raw_ts)
    except Exception:
        pass
    raw_iso = _journal_event_iso(row)
    if raw_iso:
        try:
            normalized = raw_iso if raw_iso.endswith("Z") or "+" in raw_iso[10:] else raw_iso + "Z"
            return datetime.fromisoformat(normalized.replace("Z", "+00:00")).timestamp()
        except Exception:
            pass
    return 0.0


def _summarize_crypto_rejection_audit(journals: Any, *, limit: int = 8) -> Dict[str, Any]:
    items: List[Dict[str, Any]] = []
    blocker_counts: Dict[str, int] = {}
    asset_counts: Dict[str, int] = {}
    for row in list(journals or []):
        if not isinstance(row, dict):
            continue
        if str(row.get("decision") or "").upper() == "SURVIVED":
            continue
        asset = str(row.get("asset") or "").strip()
        if not asset:
            continue
        category = str(row.get("category") or _CAT.get(asset, "") or "").strip().lower()
        if category != "crypto":
            continue
        enriched = _enrich_signal_like_row(row)
        rejected_reasons = _clean_text_list(
            enriched.get("rejected_reasons") or row.get("rejected_reasons"),
            limit=6,
        )
        rejected_details = _clean_text_list(
            enriched.get("rejected_details") or row.get("rejected_details"),
            limit=4,
        )
        blocker = str(
            enriched.get("exact_kill_reason")
            or enriched.get("blocked_reason")
            or row.get("kill_reason")
            or row.get("final_policy_reason")
            or row.get("reason")
            or row.get("killed_by")
            or "no_candidate"
        ).strip() or "no_candidate"
        blocker_label = blocker.split(":", 1)[0].strip().lower() or "no_candidate"
        blocker_counts[blocker_label] = blocker_counts.get(blocker_label, 0) + 1
        asset_counts[asset] = asset_counts.get(asset, 0) + 1
        enriched["category"] = "crypto"
        enriched["blocked_reason"] = str(enriched.get("blocked_reason") or row.get("blocked_reason") or row.get("kill_reason") or "").strip()
        enriched["rejected_reasons"] = rejected_reasons
        enriched["rejected_details"] = rejected_details
        enriched["audit_time"] = _journal_event_iso(row)
        enriched["blocker_label"] = blocker_label
        enriched["reject_count_for_asset"] = asset_counts[asset]
        items.append(enriched)

    items.sort(
        key=lambda item: (
            str(item.get("audit_time") or ""),
            float(item.get("opportunity_score", 0.0) or 0.0),
            float(item.get("setup_quality", 0.0) or 0.0),
        ),
        reverse=True,
    )
    limited: List[Dict[str, Any]] = []
    per_asset_seen: Dict[str, int] = {}
    for item in items:
        asset = str(item.get("asset") or "").strip()
        if per_asset_seen.get(asset, 0) >= 2:
            continue
        per_asset_seen[asset] = per_asset_seen.get(asset, 0) + 1
        limited.append(item)
        if len(limited) >= max(3, int(limit or 8)):
            break

    top_blockers = [
        {"label": label, "count": count}
        for label, count in sorted(blocker_counts.items(), key=lambda item: (-item[1], item[0]))
    ][:3]
    asset_summary = [
        {"asset": asset, "count": count}
        for asset, count in sorted(asset_counts.items(), key=lambda item: (-item[1], item[0]))
    ][:5]
    return {
        "count": len(items),
        "asset_count": len(asset_counts),
        "lead_blocker": top_blockers[0]["label"] if top_blockers else "",
        "top_blockers": top_blockers,
        "assets": asset_summary,
        "rows": limited,
    }


def _current_playbook_session(category: str = "") -> str:
    try:
        from services.playbook_service import _active_session

        return str(_active_session(category=category) or "off")
    except Exception:
        return "off"


def _build_session_radar(limit: int = 12) -> Dict[str, Any]:
    rows: List[Dict[str, Any]] = []
    current_by_category: Dict[str, str] = {}
    try:
        from services.playbook_service import PlaybookService

        playbooks = PlaybookService()
        for asset, category in ALL_ASSETS:
            cat = str(category or "").strip().lower()
            current_by_category.setdefault(cat, _current_playbook_session(cat))
            try:
                session_open, current, allowed_sessions = playbooks._session_allowed(asset, cat)
            except Exception:
                current = current_by_category.get(cat, "off")
                allowed_sessions = ()
                session_open = False
            descriptor = _chart_asset_descriptor(asset, cat)
            rows.append(
                {
                    "asset": asset,
                    "category": cat,
                    "current_session": current,
                    "allowed_sessions": list(allowed_sessions or ()),
                    "session_open": bool(session_open),
                    "preferred_interval": playbooks.preferred_interval(cat, asset),
                    "primary_provider": descriptor.get("primary_provider", ""),
                    "secondary_provider": descriptor.get("secondary_provider", ""),
                }
            )
    except Exception:
        return {
            "current_by_category": {},
            "open_count": 0,
            "blocked_count": 0,
            "rows": [],
        }

    rows.sort(
        key=lambda row: (
            0 if row.get("session_open") else 1,
            str(row.get("category") or ""),
            str(row.get("asset") or ""),
        )
    )
    return {
        "current_by_category": current_by_category,
        "open_count": sum(1 for row in rows if row.get("session_open")),
        "blocked_count": sum(1 for row in rows if not row.get("session_open")),
        "rows": rows[: max(4, int(limit or 12))],
        "all_rows": rows,
    }


def _summarize_why_not_traded(journals: Any, near_misses: Any, *, limit: int = 6) -> Dict[str, Any]:
    blocker_counts: Dict[str, int] = {}
    asset_counts: Dict[str, Dict[str, Any]] = {}
    confirmation_pending_count = 0
    extension_block_count = 0
    cluster_blocked_count = 0
    regime_blocked_count = 0
    for row in list(journals or []):
        if not isinstance(row, dict):
            continue
        if str(row.get("decision") or "").upper() == "SURVIVED":
            continue
        enriched = _enrich_signal_like_row(row)
        asset = str(row.get("asset") or "").strip()
        if not asset:
            continue
        blocker = str(
            row.get("killed_by")
            or enriched.get("blocked_reason")
            or row.get("kill_reason")
            or row.get("final_policy_reason")
            or row.get("last_layer")
            or "no_candidate"
        ).strip() or "no_candidate"
        blocker_label = blocker.split(":", 1)[0]
        blocker_counts[blocker_label] = blocker_counts.get(blocker_label, 0) + 1
        rejected_buckets = dict(enriched.get("rejected_reason_buckets") or {})
        confirmation_pending_count += int(rejected_buckets.get("confirmation_missing", 0) or 0) + int(rejected_buckets.get("reclaim_unconfirmed", 0) or 0)
        extension_block_count += int(rejected_buckets.get("extension_too_large", 0) or 0)
        cluster_blocked_count += int(rejected_buckets.get("clustered_signal", 0) or 0)
        regime_blocked_count += int(rejected_buckets.get("alignment_too_weak", 0) or 0) + int(rejected_buckets.get("trend_misaligned", 0) or 0)
        bucket = asset_counts.setdefault(
            asset,
            {"asset": asset, "count": 0, "top_blocker": blocker_label, "reason": blocker, "setup_quality": 0.0},
        )
        bucket["count"] += 1
        bucket["setup_quality"] = max(float(bucket.get("setup_quality", 0.0) or 0.0), float(row.get("setup_quality", 0.0) or 0.0))
        for key, value in enriched.items():
            if key in {"asset", "count"}:
                continue
            if key not in bucket or bucket.get(key) in (None, "", [], {}, 0, 0.0, False):
                bucket[key] = value

    top_blockers = [
        {"label": label, "count": count}
        for label, count in sorted(blocker_counts.items(), key=lambda item: (-item[1], item[0]))
    ][: max(3, int(limit or 6))]

    top_assets = sorted(
        asset_counts.values(),
        key=lambda item: (
            int(item.get("count", 0) or 0),
            float(item.get("setup_quality", 0.0) or 0.0),
        ),
        reverse=True,
    )[: max(3, int(limit or 6))]

    return {
        "top_blockers": top_blockers,
        "top_assets": top_assets,
        "lead_blocker": top_blockers[0]["label"] if top_blockers else "",
        "lead_count": top_blockers[0]["count"] if top_blockers else 0,
        "confirmation_pending_count": confirmation_pending_count,
        "blocked_by_confirmation_count": confirmation_pending_count,
        "overextended_count": extension_block_count,
        "cluster_blocked_count": cluster_blocked_count,
        "regime_blocked_count": regime_blocked_count,
    }


def _build_watchlist_ladder(
    top_opportunities: Any,
    near_misses: Any,
    session_radar: Dict[str, Any],
    positions: Any,
) -> Dict[str, Any]:
    hot = []
    for item in list(top_opportunities or [])[:4]:
        if not isinstance(item, dict):
            continue
        hot.append(
            _enrich_signal_like_row(
                {
                "asset": str(item.get("asset") or ""),
                "direction": str(item.get("direction") or item.get("signal") or "").upper(),
                "opportunity_score": round(float(item.get("opportunity_score", 0.0) or 0.0), 3),
                "confidence": round(float(item.get("confidence", 0.0) or 0.0) * 100.0, 1),
                "metadata": dict(item.get("metadata") or {}),
                **item,
                }
            )
        )

    almost_ready = []
    for item in list(near_misses or [])[:4]:
        if not isinstance(item, dict):
            continue
        almost_ready.append(
            _enrich_signal_like_row(
                {
                "asset": str(item.get("asset") or ""),
                "direction": str(item.get("direction") or "").upper(),
                "reason": str(item.get("reason") or item.get("killed_by") or ""),
                "opportunity_score": round(float(item.get("opportunity_score", 0.0) or 0.0), 3),
                "setup_quality": round(float(item.get("setup_quality", 0.0) or 0.0), 3),
                "metadata": dict(item.get("metadata") or {}),
                **item,
                }
            )
        )

    blocked = []
    active_assets = {str(item.get("asset") or "") for item in list(positions or []) if isinstance(item, dict)}
    for row in list(session_radar.get("all_rows") or []):
        if row.get("session_open"):
            continue
        asset = str(row.get("asset") or "")
        if asset in active_assets:
            continue
        blocked.append(
            _enrich_signal_like_row(
                {
                "asset": asset,
                "current_session": str(row.get("current_session") or ""),
                "allowed_sessions": list(row.get("allowed_sessions") or []),
                "category": str(row.get("category") or ""),
                "metadata": dict(row.get("metadata") or {}),
                **row,
                }
            )
        )
        if len(blocked) >= 4:
            break

    occupied = {item.get("asset") for item in hot + almost_ready + blocked if isinstance(item, dict)}
    inactive = []
    for row in list(session_radar.get("all_rows") or []):
        asset = str(row.get("asset") or "")
        if asset in occupied or asset in active_assets or not row.get("session_open"):
            continue
        inactive.append(
            _enrich_signal_like_row(
                {
                "asset": asset,
                "category": str(row.get("category") or ""),
                "preferred_interval": str(row.get("preferred_interval") or ""),
                "metadata": dict(row.get("metadata") or {}),
                **row,
                }
            )
        )
        if len(inactive) >= 4:
            break

    return {
        "hot": hot,
        "almost_ready": almost_ready,
        "blocked": blocked,
        "inactive": inactive,
    }


def _empty_why_not_traded_payload() -> Dict[str, Any]:
    return {
        "top_blockers": [],
        "top_assets": [],
        "lead_blocker": "",
        "lead_count": 0,
        "confirmation_pending_count": 0,
        "blocked_by_confirmation_count": 0,
        "overextended_count": 0,
        "cluster_blocked_count": 0,
        "regime_blocked_count": 0,
    }


def _empty_crypto_rejection_audit_payload() -> Dict[str, Any]:
    return {
        "count": 0,
        "asset_count": 0,
        "lead_blocker": "",
        "top_blockers": [],
        "assets": [],
        "rows": [],
    }


def _empty_session_radar_payload() -> Dict[str, Any]:
    return {
        "current_by_category": {},
        "open_count": 0,
        "blocked_count": 0,
        "rows": [],
        "all_rows": [],
    }


def _empty_watchlist_ladder_payload() -> Dict[str, Any]:
    return {
        "hot": [],
        "almost_ready": [],
        "blocked": [],
        "inactive": [],
    }


def _empty_trade_lifecycle_payload() -> Dict[str, Any]:
    return {
        "seeded": 0,
        "approved": 0,
        "opened": 0,
        "partial": 0,
        "runner_closed": 0,
        "closed": 0,
    }


def _safe_session_radar(limit: int = 12) -> Dict[str, Any]:
    try:
        radar = _build_session_radar(limit=limit)
        if isinstance(radar, dict):
            radar.setdefault("rows", [])
            radar.setdefault("all_rows", list(radar.get("rows") or []))
            radar.setdefault("open_count", sum(1 for row in list(radar.get("all_rows") or []) if row.get("session_open")))
            radar.setdefault("blocked_count", sum(1 for row in list(radar.get("all_rows") or []) if not row.get("session_open")))
            return radar
    except Exception as exc:
        logger.debug(f"[dashboard] session radar fallback failed: {exc}")
    return _empty_session_radar_payload()


def _decision_context_reason(item: Dict[str, Any], fallback: str = "") -> str:
    if not isinstance(item, dict):
        return fallback
    for key in (
        "exact_kill_reason",
        "execution_kill_reason",
        "blocked_reason",
        "kill_reason",
        "reason",
        "killed_by",
        "top_negative_factor",
    ):
        raw = str(item.get(key) or "").strip()
        if raw:
            return raw
    hard_blocks = _clean_text_list(item.get("execution_hard_blocks"), limit=1)
    if hard_blocks:
        return hard_blocks[0]
    rejected = _clean_text_list(item.get("rejected_reasons"), limit=1)
    if rejected:
        return rejected[0]
    return fallback


def _build_decision_context_payload(
    *,
    latest_signals: Any = None,
    runtime_rows: Any = None,
    top_opportunities: Any,
    near_misses: Any,
    session_radar: Dict[str, Any],
    positions: Any,
    why_not_traded: Dict[str, Any],
    signal_diagnostics: Dict[str, Any],
    degraded_reason: str = "",
) -> Dict[str, Any]:
    signal_rows = [row for row in list(latest_signals or []) if isinstance(row, dict)]
    runtime_rows = [row for row in list(runtime_rows or []) if isinstance(row, dict)]
    runtime_blocked_rows = [row for row in runtime_rows if _command_center_row_is_blocked(row)]
    runtime_watch_rows = [row for row in runtime_rows if not _command_center_row_is_blocked(row)]
    top_rows = [row for row in list(top_opportunities or []) if isinstance(row, dict)]
    near_rows = [row for row in list(near_misses or []) if isinstance(row, dict)]
    position_rows = [row for row in list(positions or []) if isinstance(row, dict)]
    all_session_rows = [row for row in list((session_radar or {}).get("all_rows") or (session_radar or {}).get("rows") or []) if isinstance(row, dict)]
    open_session_rows = [row for row in all_session_rows if row.get("session_open")]
    blocked_session_rows = [row for row in all_session_rows if not row.get("session_open")]

    lead_blocker = str((why_not_traded or {}).get("lead_blocker") or "").strip()
    lead_count = int((why_not_traded or {}).get("lead_count") or 0)
    diagnostics_count = int((signal_diagnostics or {}).get("count") or 0)

    if signal_rows:
        state = "active_decisions"
        label = "Decision queue"
        summary = f"{len(signal_rows)} active decision(s) are being tracked by the execution gate."
    elif position_rows:
        state = "active_book"
        label = "Active book"
        summary = f"{len(position_rows)} open position(s) are being monitored."
    elif top_rows:
        state = "candidate_queue"
        label = "Candidate queue"
        summary = f"{len(top_rows)} setup(s) are staged before final execution gates."
    elif runtime_blocked_rows or near_rows or lead_count:
        state = "execution_blocked"
        label = "Execution gate blocked"
        lead_row = runtime_blocked_rows[0] if runtime_blocked_rows else near_rows[0] if near_rows else {}
        blocker_text = lead_blocker or _decision_context_reason(lead_row, "latest gate")
        summary = f"No entry passed because the execution gate is blocking: {blocker_text}."
    elif runtime_watch_rows:
        state = "scanning"
        label = "Scanning, no entry"
        summary = f"{len(runtime_watch_rows)} asset(s) are being watched before a playbook seed clears the execution gate."
    elif open_session_rows:
        state = "scanning"
        label = "Scanning, no entry"
        summary = "Markets are open, but no playbook seed has cleared the execution gate yet."
    elif blocked_session_rows:
        state = "session_blocked"
        label = "Session blocked"
        summary = "Most assets are outside their trading session or market window."
    elif degraded_reason:
        state = "data_warming"
        label = "Context warming"
        summary = f"Dashboard is serving a safe shell while {degraded_reason}."
    else:
        state = "idle"
        label = "No entry context"
        summary = "No decision rows have been published yet."

    rows: List[Dict[str, Any]] = []
    seen_rows: set = set()

    def _append_context_row(
        item: Dict[str, Any],
        *,
        decision_kind: str,
        decision_state: str,
        fallback_reason: str,
    ) -> None:
        if len(rows) >= 8 or not isinstance(item, dict):
            return
        enriched = _enrich_signal_like_row(
            {
                **item,
                "decision_kind": str(item.get("decision_kind") or decision_kind),
                "decision_state": str(item.get("decision_state") or decision_state),
                "decision_reason": _decision_context_reason(item, fallback_reason),
            }
        )
        key = _command_center_context_row_key(enriched)
        if key in seen_rows:
            return
        seen_rows.add(key)
        rows.append(enriched)

    for item in signal_rows[:8]:
        _append_context_row(
            item,
            decision_kind="signal",
            decision_state="Active",
            fallback_reason="live execution gate decision",
        )
    for item in runtime_blocked_rows[:8]:
        _append_context_row(
            item,
            decision_kind="preseed_blocked",
            decision_state="Blocked",
            fallback_reason="pre-seed execution gate blocked",
        )
    for item in top_rows[:4]:
        _append_context_row(
            item,
            decision_kind="candidate",
            decision_state="Candidate",
            fallback_reason="waiting for final execution checks",
        )
    for item in near_rows[:8]:
        _append_context_row(
            item,
            decision_kind="blocked",
            decision_state="Blocked",
            fallback_reason="execution gate blocked",
        )
    for item in runtime_watch_rows[:8]:
        _append_context_row(
            item,
            decision_kind="watching_seed",
            decision_state="Watching",
            fallback_reason="waiting for playbook seed",
        )
    if len(rows) < 8:
        for item in list((why_not_traded or {}).get("top_assets") or [])[: max(0, 8 - len(rows))]:
            if not isinstance(item, dict):
                continue
            _append_context_row(
                item,
                decision_kind="blocked_asset",
                decision_state="Blocked",
                fallback_reason=str(item.get("top_blocker") or "execution gate blocked"),
            )
    if len(rows) < 8:
        session_source = open_session_rows + blocked_session_rows
        for item in session_source[: max(0, 8 - len(rows))]:
            if not isinstance(item, dict):
                continue
            session_open = bool(item.get("session_open"))
            _append_context_row(
                item,
                decision_kind="session_watch",
                decision_state="Watching" if session_open else "Session closed",
                fallback_reason="market open; waiting for playbook seed" if session_open else "outside allowed trading session",
            )

    return {
        "state": state,
        "label": label,
        "summary": summary,
        "lead_blocker": lead_blocker,
        "lead_count": lead_count,
        "signal_count": len(signal_rows),
        "candidate_count": len(top_rows),
        "near_miss_count": len(near_rows),
        "open_position_count": len(position_rows),
        "session_open_count": int((session_radar or {}).get("open_count") or len(open_session_rows)),
        "session_blocked_count": int((session_radar or {}).get("blocked_count") or len(blocked_session_rows)),
        "diagnostics_count": diagnostics_count,
        "degraded_reason": degraded_reason,
        "rows": rows,
    }


def _normalize_command_center_payload_contract(payload: Dict[str, Any]) -> Dict[str, Any]:
    payload = dict(payload or {})

    whale = payload.get("whale")
    if not isinstance(whale, dict):
        whale = {}
    whale = _normalize_command_center_whale_payload(
        {
            **whale,
            "recent": whale.get("recent") or payload.get("recent") or [],
            "alerts": whale.get("alerts") or payload.get("alerts") or [],
            "alert_count_24h": whale.get("alert_count_24h") or payload.get("alert_count_24h") or 0,
            "whale_alerts_24h": whale.get("whale_alerts_24h") or payload.get("whale_alerts_24h") or 0,
        }
    )
    payload["whale"] = whale
    payload["recent"] = list(payload.get("recent") or whale.get("recent") or [])
    payload["alert_count_24h"] = int(payload.get("alert_count_24h") or whale.get("alert_count_24h") or 0)
    payload["whale_alerts_24h"] = int(payload.get("whale_alerts_24h") or whale.get("whale_alerts_24h") or 0)

    sentiment_context = payload.get("sentiment_context")
    if not isinstance(sentiment_context, dict):
        sentiment_context = {}
    sentiment_context = _command_center_sentiment_context(
        {"sentiment_score": payload.get("sentiment_score", 0.0), "sentiment": sentiment_context},
        whale,
    )
    payload["sentiment_context"] = sentiment_context
    payload["sentiment_score"] = float(sentiment_context.get("sentiment_score", payload.get("sentiment_score", 0.0)) or 0.0)

    why_not = payload.get("why_not_traded")
    if not isinstance(why_not, dict):
        why_not = _empty_why_not_traded_payload()
    else:
        normalized_why = _empty_why_not_traded_payload()
        normalized_why.update(why_not)
        why_not = normalized_why

    audit = payload.get("crypto_rejection_audit")
    if not isinstance(audit, dict):
        audit = _empty_crypto_rejection_audit_payload()
    else:
        normalized_audit = _empty_crypto_rejection_audit_payload()
        normalized_audit.update(audit)
        audit = normalized_audit

    session_radar = payload.get("session_radar")
    if not isinstance(session_radar, dict) or not (session_radar.get("rows") or session_radar.get("all_rows")):
        session_radar = _safe_session_radar(limit=12)
    else:
        normalized_radar = _empty_session_radar_payload()
        normalized_radar.update(session_radar)
        normalized_radar["rows"] = list(normalized_radar.get("rows") or [])
        normalized_radar["all_rows"] = list(normalized_radar.get("all_rows") or normalized_radar.get("rows") or [])
        session_radar = normalized_radar

    ladder = payload.get("watchlist_ladder")
    if not isinstance(ladder, dict):
        ladder = _empty_watchlist_ladder_payload()
    else:
        normalized_ladder = _empty_watchlist_ladder_payload()
        normalized_ladder.update(ladder)
        ladder = normalized_ladder

    lifecycle = payload.get("trade_lifecycle")
    if not isinstance(lifecycle, dict):
        lifecycle = _empty_trade_lifecycle_payload()
    else:
        normalized_lifecycle = _empty_trade_lifecycle_payload()
        normalized_lifecycle.update(lifecycle)
        lifecycle = normalized_lifecycle

    signal_diagnostics = payload.get("signal_diagnostics")
    if not isinstance(signal_diagnostics, dict):
        signal_diagnostics = {}

    top_opportunities = [row for row in list(payload.get("top_opportunities") or []) if isinstance(row, dict)]
    near_misses = [row for row in list(payload.get("near_misses") or []) if isinstance(row, dict)]
    positions = [row for row in list(payload.get("positions") or []) if isinstance(row, dict)]

    if not any(ladder.get(key) for key in ("hot", "almost_ready", "blocked", "inactive")):
        ladder = _build_watchlist_ladder(top_opportunities, near_misses, session_radar, positions)

    decision_context = payload.get("decision_context")
    if not isinstance(decision_context, dict) or not decision_context.get("rows"):
        decision_context = _build_decision_context_payload(
            latest_signals=payload.get("latest_signals"),
            top_opportunities=top_opportunities,
            near_misses=near_misses,
            session_radar=session_radar,
            positions=positions,
            why_not_traded=why_not,
            signal_diagnostics=signal_diagnostics,
            degraded_reason=str(payload.get("degraded_reason") or ""),
        )

    if not why_not.get("top_blockers"):
        if decision_context.get("lead_blocker"):
            why_not["top_blockers"] = [{"label": decision_context["lead_blocker"], "count": int(decision_context.get("lead_count") or 1)}]
            why_not["lead_blocker"] = decision_context["lead_blocker"]
            why_not["lead_count"] = int(decision_context.get("lead_count") or 1)
        elif decision_context.get("state") == "session_blocked":
            count = int(decision_context.get("session_blocked_count") or 0)
            why_not["top_blockers"] = [{"label": "session_or_market_closed", "count": count}]
            why_not["lead_blocker"] = "session_or_market_closed"
            why_not["lead_count"] = count
        elif decision_context.get("state") == "scanning":
            count = int(decision_context.get("session_open_count") or 0)
            why_not["top_blockers"] = [{"label": "waiting_for_playbook_seed", "count": count}]
            why_not["lead_blocker"] = "waiting_for_playbook_seed"
            why_not["lead_count"] = count

    if not why_not.get("top_assets"):
        why_not["top_assets"] = [row for row in list(decision_context.get("rows") or []) if isinstance(row, dict)][:6]

    payload["why_not_traded"] = why_not
    payload["crypto_rejection_audit"] = audit
    payload["session_radar"] = session_radar
    payload["watchlist_ladder"] = ladder
    payload["trade_lifecycle"] = lifecycle
    payload["signal_diagnostics"] = signal_diagnostics
    payload["top_opportunities"] = top_opportunities
    payload["near_misses"] = near_misses
    payload["positions"] = positions
    payload["decision_context"] = decision_context
    return payload


def _compact_command_center_for_page_overview(payload: Dict[str, Any]) -> Dict[str, Any]:
    normalized = _normalize_command_center_payload_contract(payload)
    degraded_reason = str(normalized.get("degraded_reason") or "")
    stale = bool(normalized.get("stale", False))
    stale_reason = str(normalized.get("stale_reason") or "")
    degraded = bool(normalized.get("degraded", False))
    if degraded_reason in _STALE_ONLY_DEGRADED_REASONS:
        stale = True
        stale_reason = stale_reason or degraded_reason
        degraded = False
        degraded_reason = ""

    def _rows(name: str, limit: int) -> List[Dict[str, Any]]:
        return [
            dict(row)
            for row in list(normalized.get(name) or [])
            if isinstance(row, dict)
        ][:limit]

    ladder = dict(normalized.get("watchlist_ladder") or {})
    compact_ladder = {}
    for bucket, limit in (("hot", 6), ("almost_ready", 6), ("blocked", 6), ("inactive", 4)):
        compact_ladder[bucket] = [
            dict(row)
            for row in list(ladder.get(bucket) or [])
            if isinstance(row, dict)
        ][:limit]

    why_not = dict(normalized.get("why_not_traded") or {})
    compact_why_not = {
        "lead_blocker": why_not.get("lead_blocker"),
        "lead_count": int(why_not.get("lead_count") or 0),
        "top_blockers": [
            dict(row)
            for row in list(why_not.get("top_blockers") or [])
            if isinstance(row, dict)
        ][:5],
        "top_assets": [
            dict(row)
            for row in list(why_not.get("top_assets") or [])
            if isinstance(row, dict)
        ][:6],
    }

    return {
        "success": bool(normalized.get("success", True)),
        "degraded": degraded,
        "degraded_reason": degraded_reason,
        "stale": stale,
        "stale_reason": stale_reason,
        "balance": normalized.get("balance"),
        "total_pnl": normalized.get("total_pnl"),
        "daily_pnl": normalized.get("daily_pnl"),
        "daily_trades": normalized.get("daily_trades"),
        "win_rate": normalized.get("win_rate"),
        "open_positions": normalized.get("open_positions"),
        "total_trades": normalized.get("total_trades"),
        "engine_running": bool(normalized.get("engine_running", False)),
        "engine_ready": bool(normalized.get("engine_ready", False)),
        "sentiment_score": normalized.get("sentiment_score"),
        "sentiment_context": dict(normalized.get("sentiment_context") or {}),
        "whale": dict(normalized.get("whale") or {}),
        "whale_alerts_24h": int(normalized.get("whale_alerts_24h", 0) or 0),
        "alert_count_24h": int(normalized.get("alert_count_24h", 0) or 0),
        "recent": _rows("recent", 8),
        "latest_signals": _rows("latest_signals", 8),
        "top_opportunities": _rows("top_opportunities", 8),
        "near_misses": _rows("near_misses", 8),
        "weak_positions": _rows("weak_positions", 8),
        "positions": _rows("positions", 12),
        "watchlist_ladder": compact_ladder,
        "trade_lifecycle": dict(normalized.get("trade_lifecycle") or {}),
        "trade_tape": _rows("trade_tape", 16),
        "why_not_traded": compact_why_not,
        "session_radar": dict(normalized.get("session_radar") or {}),
        "crypto_rejection_audit": dict(normalized.get("crypto_rejection_audit") or {}),
        "decision_context": dict(normalized.get("decision_context") or {}),
        "signal_quality": dict(normalized.get("signal_quality") or {}),
        "live_summary": dict(normalized.get("live_summary") or {}),
        "pnl_curve": [
            dict(row)
            for row in list(normalized.get("pnl_curve") or [])
            if isinstance(row, dict)
        ][-96:],
        "pnl_curve_stats": dict(normalized.get("pnl_curve_stats") or {}),
        "provider_routing": dict(normalized.get("provider_routing") or {}),
        "signal_diagnostics": dict(normalized.get("signal_diagnostics") or {}),
        "timestamp": normalized.get("timestamp") or datetime.now().isoformat(),
    }


def _build_trade_tape(positions: Any, closed_trades: Any, *, limit: int = 12) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []
    for pos in list(positions or []):
        if not isinstance(pos, dict):
            continue
        opened_at = _coerce_utc_datetime(pos.get("open_time") or pos.get("entry_time"))
        events.append(
            _enrich_signal_like_row(
                {
                "asset": str(pos.get("asset") or ""),
                "direction": str(pos.get("direction") or pos.get("signal") or "").upper(),
                "stage": "open",
                "event_time": opened_at.isoformat() if opened_at else "",
                "time_sort": opened_at.timestamp() if opened_at else 0.0,
                "note": str(pos.get("strategy_id") or ""),
                "pnl": round(float(pos.get("pnl", 0.0) or 0.0), 2),
                "metadata": dict(pos.get("metadata") or {}),
                }
            )
        )

    for trade in list(closed_trades or []):
        if not isinstance(trade, dict):
            continue
        exit_time = _coerce_utc_datetime(trade.get("exit_time") or trade.get("entry_time"))
        stage = "partial" if _is_partial_close_trade_row(trade) else "closed"
        events.append(
            _enrich_signal_like_row(
                {
                "asset": str(trade.get("asset") or ""),
                "direction": str(trade.get("direction") or trade.get("signal") or "").upper(),
                "stage": stage,
                "event_time": exit_time.isoformat() if exit_time else "",
                "time_sort": exit_time.timestamp() if exit_time else 0.0,
                "note": str(trade.get("exit_reason") or trade.get("continuation_summary") or ""),
                "pnl": round(float(trade.get("pnl", 0.0) or 0.0), 2),
                "metadata": dict(trade.get("metadata") or trade.get("trade_metadata") or {}),
                }
            )
        )

    events.sort(key=lambda item: float(item.get("time_sort", 0.0) or 0.0), reverse=True)
    return events[: max(4, int(limit or 12))]


def _load_authoritative_closed_trades(limit: int = 50) -> List[Dict[str, Any]]:
    target = max(1, int(limit or 50))
    cache_key = f"closed_trades:v2:{target}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return [dict(row) for row in list(cached or []) if isinstance(row, dict)]

    rows: List[Dict[str, Any]] = []
    try:
        from services.db_pool import get_db

        rows = list(get_db().get_recent_trades(limit=target) or [])
    except Exception as exc:
        logger.debug(f"[dashboard] closed-trade DB load fallback: {exc}")

    if not rows:
        try:
            from core.state import state as runtime_state

            rows = list(runtime_state.get_closed_positions(limit=target) or [])
        except Exception as exc:
            logger.debug(f"[dashboard] closed-trade runtime fallback failed: {exc}")
            rows = []

    normalized: List[Dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        item = dict(row)
        metadata = dict(item.get("metadata") or item.get("trade_metadata") or {})
        item["metadata"] = metadata
        paper_execution = metadata.get("paper_execution")
        if isinstance(paper_execution, dict):
            net_pnl = paper_execution.get("net_pnl")
            if net_pnl not in (None, ""):
                try:
                    item["pnl"] = float(net_pnl)
                except Exception:
                    pass
        normalized.append(item)
    _cache_set(cache_key, copy.deepcopy(normalized), ttl=_CLOSED_TRADES_CACHE_TTL)
    return normalized


def _rollup_closed_trades_for_dashboard(closed_trades: Any, *, limit: int = 1000) -> List[Dict[str, Any]]:
    rows = [dict(row) for row in list(closed_trades or []) if isinstance(row, dict)]
    if not rows:
        return []
    try:
        from core.state import rollup_closed_trade_history

        return list(rollup_closed_trade_history(rows, limit=max(1, int(limit or 1000))) or [])
    except Exception as exc:
        logger.debug(f"[dashboard] closed-trade rollup fallback failed: {exc}")
        return [row for row in rows if not _is_partial_close_trade_row(row)][: max(1, int(limit or 1000))]


def _closed_trade_exit_datetime(trade: Dict[str, Any]) -> Optional[datetime]:
    return _coerce_utc_datetime(
        trade.get("exit_time")
        or trade.get("close_time")
        or trade.get("closed_at")
        or trade.get("entry_time")
        or trade.get("open_time")
    )


def _summarize_closed_trade_history(closed_trades: Any) -> Dict[str, Any]:
    rolled = _rollup_closed_trades_for_dashboard(closed_trades, limit=1000)
    total = len(rolled)
    total_pnl = round(sum(float(row.get("pnl", 0.0) or 0.0) for row in rolled), 4)
    wins = sum(1 for row in rolled if float(row.get("pnl", 0.0) or 0.0) > 0.0)
    losses = sum(1 for row in rolled if float(row.get("pnl", 0.0) or 0.0) < 0.0)

    local_tz = _dashboard_local_timezone()
    today = _dashboard_now_local().date()
    daily_rows: List[Dict[str, Any]] = []
    for row in rolled:
        exit_dt = _closed_trade_exit_datetime(row)
        if exit_dt and exit_dt.astimezone(local_tz).date() == today:
            daily_rows.append(row)

    daily_pnl = round(sum(float(row.get("pnl", 0.0) or 0.0) for row in daily_rows), 4)
    return {
        "rolled_trades": rolled,
        "total_trades": total,
        "winning_trades": wins,
        "losing_trades": losses,
        "win_rate": round(wins / total, 4) if total else 0.0,
        "total_pnl": total_pnl,
        "daily_pnl": daily_pnl,
        "daily_trades": len(daily_rows),
    }


def _build_authoritative_trade_history_summary(closed_trades: Any) -> Dict[str, Any]:
    summary = _summarize_closed_trade_history(closed_trades)
    initial_balance = float(getattr(_args, "balance", 10000.0) or 10000.0)
    total_pnl = float(summary.get("total_pnl", 0.0) or 0.0)
    return {
        "initial_balance": initial_balance,
        "balance": round(initial_balance + total_pnl, 2),
        "realized_balance": round(initial_balance + total_pnl, 2),
        "balance_delta": round(total_pnl, 2),
        "account_state": "loss" if total_pnl < -0.005 else "gain" if total_pnl > 0.005 else "flat",
        "total_pnl": round(total_pnl, 2),
        "realized_total_pnl": round(total_pnl, 2),
        "daily_pnl": round(float(summary.get("daily_pnl", 0.0) or 0.0), 2),
        "realized_daily_pnl": round(float(summary.get("daily_pnl", 0.0) or 0.0), 2),
        "daily_trades": int(summary.get("daily_trades", 0) or 0),
        "total_trades": int(summary.get("total_trades", 0) or 0),
        "closed_trades": int(summary.get("total_trades", 0) or 0),
        "winning_trades": int(summary.get("winning_trades", 0) or 0),
        "losing_trades": int(summary.get("losing_trades", 0) or 0),
        "win_rate": round(float(summary.get("win_rate", 0.0) or 0.0) * 100.0, 1),
    }


def _build_trade_lifecycle(
    positions: Any,
    closed_trades: Any,
    journals: Any,
    active_items: Any = None,
) -> Dict[str, Any]:
    def _field(row: Dict[str, Any], key: str, default: Any = None) -> Any:
        meta = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
        value = row.get(key, meta.get(key, default))
        return default if value in (None, "") else value

    def _truthy(row: Dict[str, Any], key: str) -> bool:
        value = _field(row, key, False)
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}

    def _number(row: Dict[str, Any], key: str, default: float = 0.0) -> float:
        try:
            return float(_field(row, key, default) or default)
        except Exception:
            return default

    def _kill_reason(row: Dict[str, Any]) -> str:
        for key in ("exact_kill_reason", "execution_kill_reason", "blocked_reason", "kill_reason", "reason"):
            value = str(_field(row, key, "") or "").strip()
            if value:
                return value
        return ""

    journal_seed_count = 0
    journal_approved_count = 0
    for row in list(journals or []):
        if not isinstance(row, dict):
            continue
        journal_seed_count += 1
        if str(row.get("decision") or "").upper() == "SURVIVED":
            journal_approved_count += 1

    active_rows: List[Dict[str, Any]] = []
    seen: set = set()
    for row in list(active_items or []):
        if not isinstance(row, dict):
            continue
        enriched = _enrich_signal_like_row(row)
        kind = str(enriched.get("decision_kind") or enriched.get("kind") or "").lower()
        state = str(enriched.get("decision_state") or enriched.get("state") or "").lower()
        if kind in {"session_watch", "watching_seed", "preseed_blocked"} or state in {"watching", "session closed"}:
            continue
        identity = (
            str(enriched.get("asset") or ""),
            str(enriched.get("direction") or enriched.get("signal") or ""),
            str(enriched.get("decision_kind") or ""),
            str(enriched.get("exact_kill_reason") or enriched.get("execution_kill_reason") or enriched.get("blocked_reason") or ""),
        )
        if identity in seen:
            continue
        seen.add(identity)
        active_rows.append(enriched)

    active_count = len(active_rows)
    structure_valid = sum(
        1
        for row in active_rows
        if _truthy(row, "breakout_retest_ready")
        or _truthy(row, "first_pullback_ready")
        or _truthy(row, "failed_opposite_move_confirmed")
        or _truthy(row, "entry_confirmation_ready")
    )
    confirmation_ready = sum(1 for row in active_rows if _truthy(row, "entry_confirmation_ready"))
    confirmation_pending = sum(
        1
        for row in active_rows
        if not _truthy(row, "entry_confirmation_ready") and _number(row, "entry_confirmation_bars_required", 0.0) > 0
    )
    blocked_count = sum(
        1
        for row in active_rows
        if _kill_reason(row)
        or str(_field(row, "decision_kind", "") or "").lower() in {"blocked", "killed", "blocked_asset"}
        or _number(row, "extension_score", 0.0) >= 0.80
        or _number(row, "cluster_penalty", 0.0) >= 0.70
    )
    active_approved = sum(
        1
        for row in active_rows
        if not _kill_reason(row)
        and _truthy(row, "entry_confirmation_ready")
        and (
            _truthy(row, "breakout_retest_ready")
            or _truthy(row, "first_pullback_ready")
            or _number(row, "entry_confirmation_bars_required", 0.0) <= 0
        )
    )

    raw_closed = [row for row in list(closed_trades or []) if isinstance(row, dict)]
    partial_count = sum(1 for row in raw_closed if _is_partial_close_trade_row(row))
    closed_count = sum(1 for row in raw_closed if not _is_partial_close_trade_row(row))
    runner_count = sum(1 for row in raw_closed if bool(row.get("has_partial_closes")))
    opened_count = len(list(positions or []))
    seeded_count = max(journal_seed_count, active_count)
    approved_count = max(journal_approved_count, active_approved)

    return {
        "seeded": seeded_count,
        "active": active_count,
        "structure_valid": structure_valid,
        "confirmation_ready": confirmation_ready,
        "confirmation_pending": confirmation_pending,
        "blocked": blocked_count,
        "waiting": max(0, seeded_count - approved_count - blocked_count),
        "approved": approved_count,
        "opened": opened_count,
        "executed": opened_count,
        "partial": partial_count,
        "runner_closed": runner_count,
        "closed": closed_count,
    }


def _summarize_asset_playbook_matrix(signals: Any, trades: Any, *, limit: int = 8) -> List[Dict[str, Any]]:
    matrix: Dict[Tuple[str, str], Dict[str, Any]] = {}

    def _bucket(asset: str, playbook: str) -> Dict[str, Any]:
        return matrix.setdefault(
            (asset, playbook),
            {
                "asset": asset,
                "playbook": playbook,
                "live_count": 0,
                "avg_confidence": 0.0,
                "closed_count": 0,
                "win_count": 0,
                "total_pnl": 0.0,
            },
        )

    for signal in list(signals or []):
        if not isinstance(signal, dict):
            continue
        asset = str(signal.get("asset") or "").strip()
        playbook = _playbook_name_from_payload(signal)
        if not asset or not playbook:
            continue
        row = _bucket(asset, playbook)
        row["live_count"] += 1
        row["avg_confidence"] += float(signal.get("confidence", 0.0) or 0.0) * 100.0

    for trade in list(trades or []):
        if not isinstance(trade, dict) or _is_partial_close_trade_row(trade):
            continue
        asset = str(trade.get("canonical_asset") or trade.get("asset") or "").strip()
        playbook = _playbook_name_from_trade(trade)
        if not asset or not playbook:
            continue
        row = _bucket(asset, playbook)
        pnl = float(trade.get("pnl", 0.0) or 0.0)
        row["closed_count"] += 1
        row["total_pnl"] += pnl
        if pnl > 0.0:
            row["win_count"] += 1

    rows = []
    for row in matrix.values():
        if row["live_count"]:
            row["avg_confidence"] = round(row["avg_confidence"] / row["live_count"], 1)
        else:
            row["avg_confidence"] = 0.0
        row["win_rate"] = round((row["win_count"] / row["closed_count"]) * 100.0, 1) if row["closed_count"] else 0.0
        row["total_pnl"] = round(float(row["total_pnl"] or 0.0), 2)
        rows.append(row)

    rows.sort(
        key=lambda item: (
            int(item.get("live_count", 0) or 0) + int(item.get("closed_count", 0) or 0),
            float(item.get("total_pnl", 0.0) or 0.0),
            float(item.get("avg_confidence", 0.0) or 0.0),
        ),
        reverse=True,
    )
    return rows[: max(4, int(limit or 8))]


def _failure_archetype_label(row: Dict[str, Any]) -> str:
    reason = str(row.get("kill_reason") or row.get("final_policy_reason") or row.get("blocked_reason") or row.get("killed_by") or "").lower()
    if "session" in reason:
        return "Session window"
    if "depth" in reason or str(row.get("depth_mode") or "").lower() in {"synthetic_depth", "top_of_book"}:
        return "Hostile depth"
    if float(row.get("stop_hunt_risk", 0.0) or 0.0) >= 0.55:
        return "Stop-hunt risk"
    if float(row.get("exhaustion_risk", 0.0) or 0.0) >= 0.55:
        return "Exhaustion"
    if "cross" in reason:
        return "Cross-market conflict"
    return "Pattern filter"


def _failure_archetype_record(archetypes: Dict[str, Dict[str, Any]], label: str, asset: str) -> None:
    row = archetypes.setdefault(label, {"label": label, "count": 0, "assets": {}})
    row["count"] += 1
    if asset:
        row["assets"][asset] = row["assets"].get(asset, 0) + 1


def _summarize_failure_archetypes(journals: Any, trades: Any, *, limit: int = 6) -> List[Dict[str, Any]]:
    archetypes: Dict[str, Dict[str, Any]] = {}

    for row in list(journals or []):
        if not isinstance(row, dict):
            continue
        if str(row.get("decision") or "").upper() == "SURVIVED":
            continue
        asset = str(row.get("asset") or "")
        _failure_archetype_record(archetypes, _failure_archetype_label(row), asset)

    for trade in list(trades or []):
        if not isinstance(trade, dict) or _is_partial_close_trade_row(trade):
            continue
        meta = dict(trade.get("metadata") or {})
        execution = _extract_execution_feedback_fields(meta)
        asset = str(trade.get("canonical_asset") or trade.get("asset") or "")
        if bool(execution.get("late_entry")):
            _failure_archetype_record(archetypes, "Late entry", asset)
        if bool(execution.get("premature_stop")):
            _failure_archetype_record(archetypes, "Premature stop", asset)

    rows = []
    for row in archetypes.values():
        assets = sorted(row.pop("assets").items(), key=lambda item: (-item[1], item[0]))
        row["top_assets"] = [asset for asset, _ in assets[:3]]
        rows.append(row)
    rows.sort(key=lambda item: (int(item.get("count", 0) or 0), str(item.get("label") or "")), reverse=True)
    return rows[: max(3, int(limit or 6))]


def _summarize_confidence_decomposition(signals: Any) -> Dict[str, Any]:
    buckets = {
        "structure": [],
        "flow": [],
        "broker": [],
        "memory": [],
        "cross_market": [],
    }
    for signal in list(signals or []):
        if not isinstance(signal, dict):
            continue
        buckets["structure"].append(float(signal.get("opportunity_score", 0.0) or 0.0) * 100.0)
        buckets["flow"].append(abs(float(signal.get("microstructure_score", 0.0) or 0.0)) * 100.0)
        buckets["broker"].append(float(signal.get("broker_quality_score", 0.0) or 0.0) * 100.0)
        buckets["memory"].append(float(signal.get("memory_score", 0.0) or 0.0))
        cross_alignment = float(signal.get("cross_asset_alignment", 0.0) or 0.0)
        cross_conf = float(signal.get("cross_asset_confidence", 0.0) or 0.0)
        buckets["cross_market"].append(((abs(cross_alignment) + max(0.0, cross_conf)) / 2.0) * 100.0)

    components = []
    for label, values in buckets.items():
        avg_score = round(sum(values) / len(values), 1) if values else 0.0
        components.append({"label": label, "avg_score": avg_score, "sample_count": len(values)})
    components.sort(key=lambda item: float(item.get("avg_score", 0.0) or 0.0), reverse=True)
    return {
        "components": components,
        "top_component": components[0]["label"] if components else "",
    }


def _summarize_stop_concentration(positions: Any, *, limit: int = 5) -> List[Dict[str, Any]]:
    groups: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for pos in list(positions or []):
        if not isinstance(pos, dict):
            continue
        cluster = _risk_cluster_group(pos.get("asset"), pos.get("category"))
        direction = str(pos.get("direction") or pos.get("signal") or "BUY").upper()
        key = (cluster, direction)
        row = groups.setdefault(
            key,
            {"label": cluster, "direction": direction, "count": 0, "avg_stop_distance_pct": 0.0, "assets": []},
        )
        entry = float(pos.get("entry_price", 0.0) or 0.0)
        stop = float(pos.get("stop_loss", 0.0) or 0.0)
        dist_pct = abs(entry - stop) / entry * 100.0 if entry and stop else 0.0
        row["count"] += 1
        row["avg_stop_distance_pct"] += dist_pct
        row["assets"].append(str(pos.get("asset") or ""))

    rows: List[Dict[str, Any]] = []
    for row in groups.values():
        count = int(row.get("count", 0) or 0)
        row["avg_stop_distance_pct"] = round(float(row.get("avg_stop_distance_pct", 0.0) or 0.0) / count, 3) if count else 0.0
        row["assets"] = list(dict.fromkeys(row.get("assets") or []))[:4]
        rows.append(row)
    rows.sort(
        key=lambda item: (
            int(item.get("count", 0) or 0),
            -float(item.get("avg_stop_distance_pct", 0.0) or 0.0),
        ),
        reverse=True,
    )
    return rows[: max(3, int(limit or 5))]


def _summarize_scenario_risk(positions: Any) -> List[Dict[str, Any]]:
    scenarios = {
        "USD spike": {"assets": {"EUR/USD", "GBP/USD", "AUD/USD", "NZD/USD", "USD/CAD", "USD/CHF", "USD/JPY", "XAU/USD", "XAG/USD"}},
        "Risk-off": {"categories": {"indices", "crypto"}, "assets": {"XAU/USD"}},
        "Oil shock": {"assets": {"WTI", "USD/CAD", "US30", "US100", "US500"}},
        "Rates shock": {"assets": {"USD/JPY", "XAU/USD", "US100", "US500", "US30", "BTC-USD"}},
    }
    rows: List[Dict[str, Any]] = []
    for label, rule in scenarios.items():
        impacted = []
        exposure = 0.0
        for pos in list(positions or []):
            if not isinstance(pos, dict):
                continue
            asset = str(pos.get("asset") or "")
            category = str(pos.get("category") or "").lower()
            if asset in set(rule.get("assets") or set()) or category in set(rule.get("categories") or set()):
                impacted.append(asset)
                exposure += float(pos.get("position_size", 0.0) or 0.0) * float(pos.get("entry_price", 0.0) or 0.0)
        rows.append(
            {
                "label": label,
                "count": len(impacted),
                "exposure": round(exposure, 2),
                "assets": list(dict.fromkeys(impacted))[:5],
            }
        )
    rows.sort(key=lambda item: (float(item.get("exposure", 0.0) or 0.0), int(item.get("count", 0) or 0)), reverse=True)
    return rows


def _risk_cluster_group(asset: Any, category: Any) -> str:
    symbol = str(asset or "").upper().replace("/", "").replace("-", "").replace("=F", "")
    cat = str(category or "").lower()
    if cat == "crypto":
        if symbol in {"BTCUSD", "ETHUSD"}:
            return "Crypto Majors"
        return "Crypto Alts"
    if cat == "indices":
        if symbol == "UK100":
            return "Europe Indices"
        return "US Indices"
    if cat == "commodities":
        if symbol in {"XAUUSD", "XAGUSD"}:
            return "Precious Metals"
        if "WTI" in symbol or "LIGHTCMDUSD" in symbol:
            return "Energy"
        return "Commodities"
    if cat == "forex":
        if "JPY" in symbol:
            return "JPY FX"
        if symbol in {"EURUSD", "GBPUSD", "AUDUSD", "USDCAD"}:
            return "USD FX"
        return "FX Crosses"
    return cat.title() or "Unclassified"


def _risk_portfolio_category_row() -> Dict[str, Any]:
    return {
        "count": 0,
        "pnl": 0.0,
        "exposure": 0.0,
        "memory_scores": [],
        "execution_scores": [],
        "opportunity_scores": [],
        "broker_scores": [],
        "micro_scores": [],
        "cross_alignments": [],
        "true_depth_count": 0,
        "synthetic_depth_count": 0,
        "cross_conflict_count": 0,
        "recent_block_count": 0,
    }


def _risk_portfolio_cluster_row(cluster: str) -> Dict[str, Any]:
    return {
        "label": cluster or "Unclassified",
        "count": 0,
        "pnl": 0.0,
        "exposure": 0.0,
        "buy_count": 0,
        "sell_count": 0,
        "execution_scores": [],
        "memory_scores": [],
        "opportunity_scores": [],
    }


def _risk_portfolio_average(values: List[float], digits: int) -> float:
    return round(sum(values) / len(values), digits) if values else 0.0


def _risk_portfolio_direction_skew(buy_count: int, sell_count: int) -> str:
    return "SELL" if sell_count > buy_count else "BUY" if buy_count > sell_count else "MIXED"


def _risk_portfolio_skew_ratio(count: int, buy_count: int, sell_count: int) -> float:
    return round((max(buy_count, sell_count) / count) * 100.0, 1) if count else 0.0


def _risk_portfolio_record_position(
    by_cat: Dict[str, Dict[str, Any]],
    by_cluster: Dict[str, Dict[str, Any]],
    position: Any,
) -> None:
    if not isinstance(position, dict):
        return

    cat = str(position.get("category", "unknown") or "unknown")
    cluster = _risk_cluster_group(position.get("asset", ""), cat)
    cluster_key = cluster or "Unclassified"
    direction = str(position.get("direction") or position.get("signal") or "BUY").upper()
    meta = dict(position.get("metadata") or {})
    memory = _extract_memory_fields(meta)
    execution = _extract_execution_feedback_fields(meta)
    opportunity = _extract_opportunity_fields(meta)
    intelligence = _extract_signal_intelligence_fields(meta)
    by_cat.setdefault(cat, _risk_portfolio_category_row())
    by_cluster.setdefault(cluster_key, _risk_portfolio_cluster_row(cluster))
    row_cat = by_cat[cat]
    row_cluster = by_cluster[cluster_key]
    row_cat["count"] += 1
    row_cat["pnl"] += float(position.get("pnl") or 0)
    row_cat["exposure"] += float(position.get("position_size", 0)) * float(position.get("entry_price", 0))
    row_cluster["count"] += 1
    row_cluster["pnl"] += float(position.get("pnl") or 0)
    row_cluster["exposure"] += float(position.get("position_size", 0)) * float(position.get("entry_price", 0))
    if direction == "SELL":
        row_cluster["sell_count"] += 1
    else:
        row_cluster["buy_count"] += 1

    memory_score = float(memory.get("memory_score", 0.0) or 0.0)
    if memory_score > 0:
        row_cat["memory_scores"].append(memory_score)
        row_cluster["memory_scores"].append(memory_score)
    execution_score = float(execution.get("execution_quality_score", 0.0) or 0.0)
    if execution_score > 0:
        row_cat["execution_scores"].append(execution_score)
        row_cluster["execution_scores"].append(execution_score)
    opportunity_score = float(opportunity.get("opportunity_score", 0.0) or 0.0)
    if opportunity_score > 0:
        row_cat["opportunity_scores"].append(opportunity_score)
        row_cluster["opportunity_scores"].append(opportunity_score)
    broker_score = float(intelligence.get("broker_quality_score", 0.0) or 0.0)
    if broker_score > 0:
        row_cat["broker_scores"].append(broker_score)
    micro_score = float(intelligence.get("microstructure_score", 0.0) or 0.0)
    if abs(micro_score) > 1e-9:
        row_cat["micro_scores"].append(micro_score)
    cross_alignment = float(intelligence.get("cross_asset_alignment", 0.0) or 0.0)
    if abs(cross_alignment) > 1e-9:
        row_cat["cross_alignments"].append(cross_alignment)
    if str(intelligence.get("depth_mode") or "") == "true_depth":
        row_cat["true_depth_count"] += 1
    elif str(intelligence.get("depth_mode") or "") == "synthetic_depth":
        row_cat["synthetic_depth_count"] += 1
    if str(intelligence.get("cross_asset_context_state") or "") == "conflicted":
        row_cat["cross_conflict_count"] += 1
    if bool(intelligence.get("recent_pattern_block_new_entries")):
        row_cat["recent_block_count"] += 1


def _risk_portfolio_finalize_category_rows(by_cat: Dict[str, Dict[str, Any]]) -> None:
    for info in by_cat.values():
        info["avg_memory_score"] = _risk_portfolio_average(list(info.pop("memory_scores", []) or []), 1)
        info["avg_execution_quality"] = _risk_portfolio_average(list(info.pop("execution_scores", []) or []), 1)
        info["avg_opportunity_score"] = _risk_portfolio_average(list(info.pop("opportunity_scores", []) or []), 3)
        info["avg_broker_quality"] = _risk_portfolio_average(list(info.pop("broker_scores", []) or []), 3)
        info["avg_microstructure_score"] = _risk_portfolio_average(list(info.pop("micro_scores", []) or []), 3)
        info["avg_cross_asset_alignment"] = _risk_portfolio_average(list(info.pop("cross_alignments", []) or []), 3)


def _risk_portfolio_finalize_cluster_rows(by_cluster: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    cluster_groups: List[Dict[str, Any]] = []
    for info in by_cluster.values():
        execution_scores = list(info.pop("execution_scores", []) or [])
        memory_scores = list(info.pop("memory_scores", []) or [])
        opportunity_scores = list(info.pop("opportunity_scores", []) or [])
        count = int(info.get("count", 0) or 0)
        buy_count = int(info.get("buy_count", 0) or 0)
        sell_count = int(info.get("sell_count", 0) or 0)
        info["avg_execution_quality"] = _risk_portfolio_average(execution_scores, 1)
        info["avg_memory_score"] = _risk_portfolio_average(memory_scores, 1)
        info["avg_opportunity_score"] = _risk_portfolio_average(opportunity_scores, 3)
        info["direction_skew"] = _risk_portfolio_direction_skew(buy_count, sell_count)
        info["skew_ratio"] = _risk_portfolio_skew_ratio(count, buy_count, sell_count)
        info["pnl"] = round(float(info.get("pnl", 0.0) or 0.0), 2)
        info["exposure"] = round(float(info.get("exposure", 0.0) or 0.0), 2)
        cluster_groups.append(info)
    cluster_groups.sort(
        key=lambda item: (
            float(item.get("exposure", 0.0) or 0.0),
            int(item.get("count", 0) or 0),
        ),
        reverse=True,
    )
    return cluster_groups


def _summarize_risk_portfolio_positions(positions: Any) -> tuple[Dict[str, Dict[str, Any]], List[Dict[str, Any]]]:
    by_cat: Dict[str, Dict[str, Any]] = {}
    by_cluster: Dict[str, Dict[str, Any]] = {}
    for position in list(positions or []):
        _risk_portfolio_record_position(by_cat, by_cluster, position)
    _risk_portfolio_finalize_category_rows(by_cat)
    cluster_groups = _risk_portfolio_finalize_cluster_rows(by_cluster)
    return by_cat, cluster_groups


def _risk_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return default
        return float(value)
    except Exception:
        return default


def _basic_risk_portfolio_stats(
    positions: Any,
    balance: float,
    perf: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    total_exposure = 0.0
    open_pnl = 0.0
    for pos in list(positions or []):
        if not isinstance(pos, dict):
            continue
        size = abs(_risk_float(pos.get("position_size", pos.get("size", pos.get("quantity", 0.0)))))
        entry = abs(_risk_float(pos.get("entry_price", pos.get("price", 0.0))))
        total_exposure += size * entry
        open_pnl += _risk_float(pos.get("pnl", pos.get("floating_pnl", 0.0)))

    safe_balance = max(_risk_float(balance), 0.0)
    perf = dict(perf or {})
    peak_balance = _risk_float(perf.get("peak_balance"), safe_balance)
    if peak_balance <= 0:
        peak_balance = safe_balance
    drawdown_pct = _risk_float(perf.get("drawdown_pct"), 0.0)
    if drawdown_pct <= 0 and peak_balance > 0:
        drawdown_pct = max(0.0, (peak_balance - safe_balance) / peak_balance * 100.0)
    return {
        "total_exposure": round(total_exposure, 2),
        "exposure_pct": round((total_exposure / safe_balance * 100.0), 2) if safe_balance else 0.0,
        "drawdown_pct": round(drawdown_pct, 2),
        "peak_balance": round(peak_balance, 2),
        "open_pnl": round(open_pnl, 2),
    }


def _risk_portfolio_quality_snapshot(by_cat: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    if not by_cat:
        return {
            "avg_memory_score": 0.0,
            "avg_execution_quality": 0.0,
            "avg_opportunity_score": 0.0,
            "avg_broker_quality": 0.0,
            "avg_microstructure_score": 0.0,
            "avg_cross_asset_alignment": 0.0,
            "top_category": "",
        }
    return {
        "avg_memory_score": round(sum(float(item.get("avg_memory_score", 0.0) or 0.0) for item in by_cat.values()) / len(by_cat), 1),
        "avg_execution_quality": round(sum(float(item.get("avg_execution_quality", 0.0) or 0.0) for item in by_cat.values()) / len(by_cat), 1),
        "avg_opportunity_score": round(sum(float(item.get("avg_opportunity_score", 0.0) or 0.0) for item in by_cat.values()) / len(by_cat), 3),
        "avg_broker_quality": round(sum(float(item.get("avg_broker_quality", 0.0) or 0.0) for item in by_cat.values()) / len(by_cat), 3),
        "avg_microstructure_score": round(sum(float(item.get("avg_microstructure_score", 0.0) or 0.0) for item in by_cat.values()) / len(by_cat), 3),
        "avg_cross_asset_alignment": round(sum(float(item.get("avg_cross_asset_alignment", 0.0) or 0.0) for item in by_cat.values()) / len(by_cat), 3),
        "top_category": max(
            by_cat.items(),
            key=lambda kv: float(kv[1].get("avg_opportunity_score", 0.0) or 0.0),
        )[0],
    }


# ══════════════════════════════════════════════════════════════════════════════
# PAGE ROUTES
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/")
def pg_root():
    return redirect("/command-center")

@app.route("/command-center")
def pg_command_center():
    return _render_cached_template("command_center.html", ttl=15)

@app.route("/market-intelligence")
def pg_market_intelligence():
    return _render_cached_template("market_intelligence.html", ttl=15)

@app.route("/playbook-intel")
def pg_playbook_intel():
    return _render_cached_template("playbook_intel.html", ttl=15)

@app.route("/ai-predictions")
def pg_ai_predictions():
    return redirect("/playbook-intel")

@app.route("/whale-intelligence")
def pg_whale_intelligence():
    return _render_cached_template("whale_intelligence.html", ttl=15)

@app.route("/sentiment-intelligence")
def pg_sentiment_intelligence():
    return _render_cached_template("sentiment_intelligence.html", ttl=15)

@app.route("/risk-dashboard")
def pg_risk_dashboard():
    return _render_cached_template("risk_dashboard.html", ttl=15)

@app.route("/architecture-lab")
def pg_architecture_lab():
    return _render_cached_template("architecture_lab.html", ttl=15)

@app.route("/system-monitor")
def pg_system_monitor():
    return _render_cached_template("system_monitor.html", ttl=15)

@app.route("/order-flow")
def pg_order_flow():
    return _render_cached_template("order_flow.html", ttl=15)

@app.route("/intelligence-alerts")
def pg_intelligence_alerts():
    return _render_cached_template("intelligence_alerts.html", ttl=15)

# Deprecated compatibility redirects
@app.route("/chart")
def _r_chart():    return redirect("/market-intelligence")
@app.route("/accuracy")
def _r_accuracy(): return redirect("/playbook-intel")
@app.route("/sentiment")
def _r_sentiment():return redirect("/sentiment-intelligence")
@app.route("/backtest")
def _r_backtest(): return redirect("/command-center")
@app.route("/status")
def _r_status():   return redirect("/system-monitor")
@app.route("/websocket-feed")
def _r_ws():       return redirect("/market-intelligence")

@app.route("/service-worker.js")
def service_worker():
    response = send_from_directory(app.static_folder, "service-worker.js")
    response.headers.setdefault("Cache-Control", "no-cache, no-store, must-revalidate")
    return response

# ══════════════════════════════════════════════════════════════════════════════
# API — STATUS (used by all templates for the live dot)
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/api/login", methods=["POST"])
def api_login():
    """Get a session token by providing the API key.
    In dev mode (no API key configured), returns token without validation.
    In prod mode (API key configured), requires api_key in request body."""
    try:
        # Development mode only: no security needed, just issue a token
        if _DEVELOPMENT_MODE and not _API_KEY_HASH:
            token = _generate_session_token()
            with _SESSION_TOKEN_LOCK:
                _SESSION_TOKENS[token] = time.time() + _SESSION_TOKEN_TTL
            logger.info("[dashboard] Dev mode token issued")
            return jsonify({
                "success": True,
                "token": token,
                "expires_in": _SESSION_TOKEN_TTL,
                "mode": "dev"
            })

        if _AUTH_CONFIG_ERROR:
            raise Forbidden(_AUTH_CONFIG_ERROR)

        if not _API_KEY_HASH:
            raise Forbidden("Dashboard authentication is not configured")
        
        # Production mode: validate API key from request body
        body = request.get_json(silent=True) or {}
        provided_key = str(body.get("api_key", "") or "").strip()
        if not provided_key:
            raise BadRequest("Dashboard API key is required", {"required_fields": ["api_key"]})
        
        # Validate key
        provided_hash = hashlib.sha256(provided_key.encode()).hexdigest()
        if provided_hash != _API_KEY_HASH:
            logger.warning(f"[dashboard] Failed login from {request.remote_addr}")
            raise Forbidden("Invalid API key")
        
        # Valid — issue session token
        token = _generate_session_token()
        expiry = time.time() + _SESSION_TOKEN_TTL
        with _SESSION_TOKEN_LOCK:
            _SESSION_TOKENS[token] = expiry
        
        logger.info(f"[dashboard] Session token issued to {request.remote_addr}")
        return jsonify({
            "success": True,
            "token": token,
            "expires_in": _SESSION_TOKEN_TTL,
            "expires_at": int(expiry),
            "mode": "prod"
        })
    
    except APIError as e:
        return handle_api_error(e, "/api/login", e.status_code)[0], e.status_code
    except Exception as e:
        return handle_api_error(e, "/api/login", 500)[0], 500

@app.route("/api/logout", methods=["POST"])
@_check_api_auth
def api_logout():
    """Revoke the current session token."""
    try:
        auth_header = request.headers.get("Authorization", "")
        if auth_header.startswith("Bearer "):
            token = auth_header[7:]
            with _SESSION_TOKEN_LOCK:
                if token in _SESSION_TOKENS:
                    del _SESSION_TOKENS[token]
        
        return jsonify({"success": True, "message": "Logged out"})
    except Exception as e:
        return handle_api_error(e, "/api/logout", 500)

@app.route("/api/status")
@_check_api_auth
@_check_rate_limit
def api_status():
    """Get current bot and trading status."""
    cached = _cache_get("status:v4")
    if cached is not None:
        return jsonify(cached)
    try:
        core = _core()
        diagnostic_summary = _summarize_signal_diagnostics([])
        positions: List[Dict[str, Any]] = []
        if core:
            positions = list(core.get_positions() or [])
            diagnostic_rows = []
            for position in positions:
                meta = dict(position.get("metadata") or {})
                diagnostic_rows.append(_extract_signal_intelligence_fields(meta))
            diagnostic_summary = _summarize_signal_diagnostics(diagnostic_rows)
            payload = {
                "success": True,
                "bot_ready": core.is_ready,
                "engine_running": core.is_running,
                "architecture": "TradingCore",
                "assets_cached": len(_sig_store),
                "provider_routing": _provider_routing_summary(),
                "signal_diagnostics": diagnostic_summary,
            }
        else:
            _perf, _daily, positions, _health, _closed_trades = _load_dashboard_db_runtime_snapshot()
            external_bot_processes = _external_trading_bot_process_count()
            external_bot_running = external_bot_processes > 0
            payload = {
                "success": True,
                "bot_ready": external_bot_running,
                "engine_running": external_bot_running,
                "architecture": "TradingCore",
                "dashboard_standalone": True,
                "process_model": "split" if external_bot_running else "standalone",
                "external_bot_processes": external_bot_processes,
                "assets_cached": len(_sig_store),
                "provider_routing": _provider_routing_summary(),
                "signal_diagnostics": diagnostic_summary,
            }
        payload = _attach_authoritative_account_fields(payload, _authoritative_account_summary(core, positions))
        _cache_set("status:v4", payload, ttl=5)
        return jsonify(payload)
    except Exception as e:
        return handle_api_error(e, "/api/status", 500)

@app.route("/api/system-status")
@_check_api_auth
@_check_rate_limit
def api_system_status():
    """Get system and trading performance statistics."""
    try:
        core = _core()
        if core:
            try:
                positions = list(core.get_positions() or [])
                account = _authoritative_account_summary(core, positions)
                return jsonify({
                    "success": True,
                    "balance": round(float(account.get("balance", 0.0) or 0.0), 2),
                    "realized_balance": round(float(account.get("realized_balance", 0.0) or 0.0), 2),
                    "initial_balance": round(float(account.get("initial_balance", _args.balance) or _args.balance), 2),
                    "balance_delta": round(float(account.get("balance_delta", 0.0) or 0.0), 2),
                    "pnl": round(float(account.get("daily_pnl", 0.0) or 0.0), 2),
                    "total_pnl": round(float(account.get("total_pnl", 0.0) or 0.0), 2),
                    "open_pnl": round(float(account.get("open_pnl", 0.0) or 0.0), 2),
                    "open_positions": int(account.get("open_positions", 0) or 0),
                    "closed_positions": int(account.get("closed_trades", 0) or 0),
                    "daily_trades": int(account.get("daily_trades", 0) or 0),
                    "win_rate": _wr(account.get("win_rate", 0)),
                    "live_summary": account,
                    "engine_ready": core.is_ready,
                    "engine_running": core.is_running,
                    "timestamp": datetime.now().isoformat(),
                })
            except Exception as e:
                logger.error(f"[api_system_status] Failed to get core stats: {e}")
                # Fallback response on error
                account = _authoritative_account_summary(core)
                return jsonify({
                    "success": True, "balance": account.get("balance", _args.balance), "pnl": account.get("daily_pnl", 0),
                    "total_pnl": account.get("total_pnl", 0), "open_positions": account.get("open_positions", 0), "closed_positions": account.get("closed_trades", 0),
                    "daily_trades": account.get("daily_trades", 0), "win_rate": _wr(account.get("win_rate", 0)), "engine_ready": core.is_ready,
                    "engine_running": core.is_running, "live_summary": account,
                    "timestamp": datetime.now().isoformat(),
                })
        
        # Standalone dashboard process: read the live book from PostgreSQL
        # instead of showing cosmetic defaults.
        perf, daily, positions, health, _closed_trades = _load_dashboard_db_runtime_snapshot()
        account = _authoritative_account_summary(None, positions)
        payload = {
            "success": True,
            "balance": round(float(account.get("balance", _args.balance) or _args.balance), 2),
            "realized_balance": round(float(account.get("realized_balance", _args.balance) or _args.balance), 2),
            "initial_balance": round(float(account.get("initial_balance", _args.balance) or _args.balance), 2),
            "balance_delta": round(float(account.get("balance_delta", 0.0) or 0.0), 2),
            "pnl": round(float(account.get("daily_pnl", 0.0) or 0.0), 2),
            "total_pnl": round(float(account.get("total_pnl", 0.0) or 0.0), 2),
            "open_pnl": round(float(account.get("open_pnl", 0.0) or 0.0), 2),
            "open_positions": int(account.get("open_positions", len(positions)) or 0),
            "closed_positions": int(account.get("closed_trades", 0) or 0),
            "daily_trades": int(daily.get("daily_trades", 0) or 0),
            "win_rate": _wr(account.get("win_rate", 0)),
            "live_summary": account,
            "engine_ready": bool(health.get("engine_ready") or health.get("is_running")),
            "engine_running": bool(health.get("is_running") or health.get("engine_ready")),
            "dashboard_standalone": True,
            "process_model": "split" if health.get("is_running") else "standalone",
            "external_bot_processes": int(health.get("external_bot_processes", 0) or 0),
            "timestamp": datetime.now().isoformat(),
        }
        _cache_set("system_status:v4", payload, ttl=5)
        return jsonify(payload)
    except Exception as e:
        return handle_api_error(e, "/api/system-status", 500)

# ══════════════════════════════════════════════════════════════════════════════
# API — COMMAND CENTER
# ══════════════════════════════════════════════════════════════════════════════

def _load_dashboard_db_runtime_snapshot() -> Tuple[Dict[str, Any], Dict[str, Any], List[Dict[str, Any]], Dict[str, Any], List[Dict[str, Any]]]:
    cache_key = "db_runtime_snapshot:v2"
    cached = _cache_get(cache_key)
    if isinstance(cached, dict):
        return (
            dict(cached.get("perf") or {}),
            dict(cached.get("daily") or {}),
            [dict(row) for row in list(cached.get("positions") or []) if isinstance(row, dict)],
            dict(cached.get("health") or {}),
            [dict(row) for row in list(cached.get("closed_trades") or []) if isinstance(row, dict)],
        )

    perf: Dict[str, Any] = {}
    daily: Dict[str, Any] = {}
    positions: List[Dict[str, Any]] = []
    external_bot_processes = _external_trading_bot_process_count()
    external_bot_running = external_bot_processes > 0
    health: Dict[str, Any] = {
        "dashboard_standalone": True,
        "engine_ready": external_bot_running,
        "is_running": external_bot_running,
        "external_bot_processes": external_bot_processes,
        "strategy_mode": "split-dashboard" if external_bot_running else "standalone-dashboard",
    }
    closed_trades: List[Dict[str, Any]] = []
    try:
        from services.db_pool import get_db

        db = get_db()
        positions = list(db.load_open_positions() or [])
        closed_trades = _load_authoritative_closed_trades(limit=1000)
        closed_summary = _summarize_closed_trade_history(closed_trades)
        summary = dict(db.get_performance_summary(days=30) or {})
        daily_rows = list(db.get_daily_stats(days=1) or [])
        latest_daily = dict(daily_rows[0] or {}) if daily_rows else {}
        initial_balance = float(getattr(_args, "balance", 10000.0) or 10000.0)
        trade_total = int(closed_summary.get("total_trades", 0) or 0)
        total_trades = trade_total or int(summary.get("total_trades", 0) or 0)
        trade_history_pnl = float(closed_summary.get("total_pnl", 0.0) or 0.0)
        total_pnl = trade_history_pnl if trade_total else float(summary.get("total_pnl", 0.0) or 0.0)
        stored_balance = db.get_current_balance()
        expected_trade_balance = initial_balance + total_pnl
        if stored_balance is None:
            balance = expected_trade_balance
        else:
            balance = float(stored_balance or 0.0)
            if abs(total_pnl) >= 0.01 and abs(balance - initial_balance) < 0.01:
                balance = expected_trade_balance
        win_rate = float(closed_summary.get("win_rate", 0.0) or 0.0) if trade_total else float(summary.get("win_rate", 0.0) or 0.0)
        winning_trades = int(closed_summary.get("winning_trades", 0) or 0) if trade_total else int(summary.get("winning_trades", 0) or 0)
        losing_trades = int(closed_summary.get("losing_trades", 0) or 0) if trade_total else int(summary.get("losing_trades", 0) or 0)
        daily = {
            "daily_pnl": float(closed_summary.get("daily_pnl", 0.0) or 0.0) if trade_total else float(latest_daily.get("pnl", 0.0) or 0.0),
            "daily_trades": int(closed_summary.get("daily_trades", 0) or 0) if trade_total else int(latest_daily.get("trade_count", 0) or 0),
        }
        perf = {
            "balance": float(balance or 0.0),
            "initial_balance": initial_balance,
            "total_pnl": total_pnl,
            "open_positions": len(positions),
            "total_trades": total_trades,
            "win_rate": win_rate,
            "winning_trades": winning_trades,
            "losing_trades": losing_trades,
        }
    except Exception as exc:
        logger.debug(f"[dashboard] DB runtime snapshot fallback failed: {exc}")

    _cache_set(
        cache_key,
        {
            "perf": perf,
            "daily": daily,
            "positions": positions,
            "health": health,
            "closed_trades": closed_trades,
        },
        ttl=3,
    )
    return perf, daily, positions, health, closed_trades


def _command_center_core_snapshot(core: Any) -> Tuple[Dict[str, Any], Dict[str, Any], List[Dict[str, Any]], Dict[str, Any], List[Dict[str, Any]]]:
    perf: Dict[str, Any] = {}
    daily: Dict[str, Any] = {}
    positions: List[Dict[str, Any]] = []
    health: Dict[str, Any] = {}
    closed_trades: List[Dict[str, Any]] = []
    if core:
        try:
            perf = core.get_performance() if hasattr(core, "get_performance") else {}
        except Exception:
            perf = {}
        try:
            daily = core.get_daily_stats() if hasattr(core, "get_daily_stats") else {}
        except Exception:
            daily = {}
        try:
            positions = core.get_positions() if hasattr(core, "get_positions") else []
        except Exception:
            positions = []
        try:
            health = core.health_report() if hasattr(core, "health_report") else {}
        except Exception:
            health = {}
        health.setdefault("is_running", bool(getattr(core, "is_running", False)))
        health.setdefault("engine_ready", bool(getattr(core, "is_ready", False)))
        closed_trades = _load_authoritative_closed_trades(limit=240)
        closed_summary = _summarize_closed_trade_history(closed_trades)
        if int(closed_summary.get("total_trades", 0) or 0) > 0:
            initial_balance = float(perf.get("initial_balance", getattr(_args, "balance", 10000.0)) or getattr(_args, "balance", 10000.0))
            total_pnl = float(closed_summary.get("total_pnl", 0.0) or 0.0)
            current_balance = float(perf.get("balance", initial_balance) or initial_balance)
            if abs(current_balance - initial_balance) < 0.01 or int(perf.get("total_trades", 0) or 0) <= 0:
                current_balance = initial_balance + total_pnl
            perf.update({
                "balance": current_balance,
                "initial_balance": initial_balance,
                "total_pnl": total_pnl,
                "total_trades": int(closed_summary.get("total_trades", 0) or 0),
                "win_rate": float(closed_summary.get("win_rate", 0.0) or 0.0),
                "winning_trades": int(closed_summary.get("winning_trades", 0) or 0),
                "losing_trades": int(closed_summary.get("losing_trades", 0) or 0),
            })
            daily.update({
                "daily_pnl": float(closed_summary.get("daily_pnl", 0.0) or 0.0),
                "daily_trades": int(closed_summary.get("daily_trades", 0) or 0),
            })
    else:
        perf, daily, positions, health, closed_trades = _load_dashboard_db_runtime_snapshot()
    return perf, daily, positions, health, closed_trades


def _command_center_journals() -> List[Dict[str, Any]]:
    cached = _cache_get("cc_journals")
    if cached is not None:
        return list(cached or [])
    journals: List[Dict[str, Any]] = []
    try:
        journal_payload = _response_to_dict(_call_view(api_phase7_signal_journal))
        if bool(journal_payload.get("success")):
            journals = list(journal_payload.get("journals") or [])
    except Exception:
        journals = []
    _cache_set("cc_journals", journals, ttl=30 if journals else 8)
    return journals


def _build_command_center_payload_with_budget() -> Dict[str, Any]:
    timeout_sentinel = {"__dashboard_timeout__": True}
    payload = _run_with_timeout(
        _build_command_center_payload,
        timeout=_COMMAND_CENTER_BUILD_TIMEOUT_SECONDS,
        default=timeout_sentinel,
        label="command-center payload build",
    )
    if isinstance(payload, dict) and payload.get("__dashboard_timeout__"):
        raise TimeoutError("command-center payload build timed out")
    return dict(payload or {})


def _build_command_center_unavailable_payload(*, reason: str = "unavailable") -> Dict[str, Any]:
    cached_slow = dict(_cache_get("cc_slow") or {})
    whale_context = _command_center_whale_context()
    sentiment_context = _command_center_sentiment_context(cached_slow, whale_context)
    sent_score = float(sentiment_context.get("sentiment_score", cached_slow.get("sentiment_score", 0.0)) or 0.0)
    whale_alerts = int(
        whale_context.get("whale_alerts_24h")
        or whale_context.get("alert_count_24h")
        or cached_slow.get("whale_alerts_24h", cached_slow.get("alert_count_24h", 0))
        or 0
    )
    recent = list(whale_context.get("recent") or cached_slow.get("recent") or [])
    core = _core()
    perf, daily, positions, health, closed_trades = _command_center_core_snapshot(core)
    live_summary = _build_command_center_live_summary(perf, daily, positions)
    payload = {
        "success": True,
        "degraded": True,
        "degraded_reason": reason,
        "balance": live_summary.get("balance", float(getattr(_args, "balance", 0.0) or 0.0)),
        "total_pnl": live_summary.get("total_pnl", 0.0),
        "daily_pnl": live_summary.get("daily_pnl", 0.0),
        "daily_trades": int(daily.get("daily_trades", 0) or 0),
        "win_rate": live_summary.get("win_rate", 0.0),
        "open_positions": live_summary.get("open_positions", 0),
        "total_trades": live_summary.get("total_trades", len(closed_trades or [])),
        "engine_running": bool(health.get("is_running", getattr(core, "is_running", False) if core else False)),
        "engine_ready": bool(health.get("engine_ready", getattr(core, "is_ready", False) if core else False)),
        "sentiment_score": sent_score,
        "sentiment_context": sentiment_context,
        "whale": whale_context,
        "whale_alerts_24h": whale_alerts,
        "alert_count_24h": whale_alerts,
        "recent": recent,
        "latest_signals": [],
        "signal_quality": {"count": 0, "avg_confidence": 0.0, "avg_pnl": 0.0, "wins": 0},
        "top_opportunities": [],
        "weak_positions": [],
        "near_misses": [],
        "crypto_rejection_audit": {},
        "why_not_traded": {},
        "session_radar": {},
        "watchlist_ladder": {},
        "trade_tape": [],
        "trade_lifecycle": {},
        "positions": positions,
        "live_summary": live_summary,
        "pnl_curve": [],
        "pnl_curve_stats": {
            "interval_minutes": 30,
            "timezone": "Africa/Nairobi",
            "current_pnl": 0.0,
            "peak": 0.0,
            "drawdown": 0.0,
            "start_time": datetime.now().isoformat(),
            "end_time": datetime.now().isoformat(),
        },
        "provider_routing": {},
        "signal_diagnostics": {},
        "timestamp": datetime.now().isoformat(),
    }
    return _normalize_command_center_payload_contract(payload)


def _get_cached_command_center_payload(*, force_refresh: bool = False) -> Dict[str, Any]:
    cache_key = _COMMAND_CENTER_PAYLOAD_CACHE_KEY
    last_good_key = _COMMAND_CENTER_PAYLOAD_LAST_GOOD_KEY
    if not force_refresh:
        cached = _cache_get(cache_key)
        if cached is not None:
            payload = _response_to_dict(cached)
            if str(payload.get("degraded_reason") or "") in _STALE_ONLY_DEGRADED_REASONS:
                payload = _mark_stale_dashboard_payload(payload, "cached_stale")
            return _normalize_command_center_payload_contract(payload)

        fallback = _cache_get(last_good_key)
        if fallback is not None:
            _trigger_dashboard_payload_refresh(
                _COMMAND_CENTER_PAYLOAD_REFRESH_KEY,
                builder=_build_command_center_payload_with_budget,
                cache_key=cache_key,
                ttl=_COMMAND_CENTER_CACHE_TTL,
                last_good_key=last_good_key,
                last_good_ttl=_COMMAND_CENTER_LAST_GOOD_TTL,
            )
            return _normalize_command_center_payload_contract(_mark_stale_dashboard_payload(fallback, "stale_fallback"))

    fallback = _cache_get(last_good_key)
    _trigger_dashboard_payload_refresh(
        _COMMAND_CENTER_PAYLOAD_REFRESH_KEY,
        builder=_build_command_center_payload_with_budget,
        cache_key=cache_key,
        ttl=_COMMAND_CENTER_CACHE_TTL,
        last_good_key=last_good_key,
        last_good_ttl=_COMMAND_CENTER_LAST_GOOD_TTL,
    )
    if fallback is not None:
        return _normalize_command_center_payload_contract(_mark_stale_dashboard_payload(fallback, "stale_refresh"))

    payload = _build_command_center_unavailable_payload(reason="refreshing")
    _cache_set(cache_key, payload, ttl=_COMMAND_CENTER_DEGRADED_CACHE_TTL)
    return payload


def _get_cached_command_center_payload_blocking(*, force_refresh: bool = False) -> Dict[str, Any]:
    cache_key = _COMMAND_CENTER_PAYLOAD_CACHE_KEY
    last_good_key = _COMMAND_CENTER_PAYLOAD_LAST_GOOD_KEY
    try:
        return _normalize_command_center_payload_contract(_get_cached_dashboard_payload(
            cache_key,
            builder=_build_command_center_payload_with_budget,
            ttl=_COMMAND_CENTER_CACHE_TTL,
            force_refresh=force_refresh,
            last_good_key=last_good_key,
            last_good_ttl=_COMMAND_CENTER_LAST_GOOD_TTL,
            prefer_stale=True,
            refresh_key=_COMMAND_CENTER_PAYLOAD_REFRESH_KEY,
        ))
    except Exception as exc:
        logger.warning(f"[dashboard] command-center payload build failed: {exc}")
        fallback = _cache_get(last_good_key)
        if fallback is not None:
            return _normalize_command_center_payload_contract(_mark_stale_dashboard_payload(fallback, "stale_fallback"))
        payload = _build_command_center_unavailable_payload(reason=str(exc) or "build_failed")
        _cache_set(cache_key, payload, ttl=_COMMAND_CENTER_DEGRADED_CACHE_TTL)
        return payload


def _fetch_command_center_slow_data() -> Dict[str, Any]:
    sent_score = 0.0
    whale_count = 0
    whale_recent: List[Dict[str, Any]] = []
    whale_payload: Dict[str, Any] = {}
    sentiment_payload: Dict[str, Any] = {}
    pool = None
    try:
        from concurrent.futures import ThreadPoolExecutor, wait

        pool = ThreadPoolExecutor(max_workers=max(1, int(DASHBOARD_COMMAND_CENTER_WORKERS or 4)))
        futures = {}
        sa = _get_sent()
        if sa:
            futures[pool.submit(sa.get_comprehensive_sentiment)] = "sentiment"
        mi = _get_market_intelligence()
        if mi:
            futures[pool.submit(
                mi.get_whale_dashboard_summary,
                min_value_usd=500_000,
                hours=24,
                recent_limit=5,
                alert_limit=5,
            )] = "whales"

        if futures:
            done, not_done = wait(tuple(futures.keys()), timeout=4.5)
            for future in done:
                kind = futures[future]
                try:
                    payload = future.result()
                    if kind == "sentiment":
                        sent_score = float((payload or {}).get("score", 0) or 0)
                        sentiment_payload = dict(payload or {})
                    elif kind == "whales":
                        whale_payload = _normalize_command_center_whale_payload(dict(payload or {}))
                        whale_recent = list(whale_payload.get("recent", []) or [])
                        whale_count = int(whale_payload.get("alert_count_24h", 0) or 0)
                except Exception as exc:
                    logger.debug(f"[dashboard] command-center {kind} error: {exc}")
            for future in not_done:
                future.cancel()
            if not_done:
                logger.warning(f"[dashboard] command-center slow data timed out for {len(not_done)} task(s)")
    except Exception as exc:
        logger.debug(f"[dashboard] command-center slow data setup failed: {exc}")
    finally:
        if pool is not None:
            try:
                pool.shutdown(wait=False, cancel_futures=True)
            except Exception:
                pass
    if not whale_count and not whale_recent:
        whale_payload = _command_center_whale_context()
        whale_recent = list(whale_payload.get("recent", []) or [])
        whale_count = int(whale_payload.get("alert_count_24h", whale_payload.get("whale_alerts_24h", 0)) or 0)
    if not sentiment_payload:
        sentiment_payload = _command_center_sentiment_context({}, whale_payload)
        sent_score = float(sentiment_payload.get("score", sentiment_payload.get("sentiment_score", sent_score)) or 0.0)
    return {
        "sentiment_score": round(sent_score, 3),
        "whale_alerts_24h": whale_count,
        "alert_count_24h": whale_count,
        "recent": whale_recent,
        "whale": whale_payload,
        "sentiment": sentiment_payload,
    }


def _normalize_command_center_whale_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    payload = dict(payload or {})
    alerts = [dict(row) for row in list(payload.get("alerts") or []) if isinstance(row, dict)]
    recent = [dict(row) for row in list(payload.get("recent") or []) if isinstance(row, dict)]
    if not recent and alerts:
        recent = alerts[:10]
    count = int(
        payload.get("alert_count_24h")
        or payload.get("whale_alerts_24h")
        or len(alerts)
        or len(recent)
        or 0
    )
    total_volume = 0.0
    try:
        total_volume = float(payload.get("total_volume_usd", 0.0) or 0.0)
    except Exception:
        total_volume = 0.0
    if not total_volume:
        for row in alerts or recent:
            try:
                total_volume += float(row.get("value_usd", 0.0) or 0.0)
            except Exception:
                continue
    return {
        **payload,
        "success": bool(payload.get("success", True)),
        "alerts": alerts,
        "recent": recent,
        "alert_count_24h": count,
        "whale_alerts_24h": count,
        "total_volume_usd": round(total_volume, 2),
        "top_assets": list(payload.get("top_assets") or []),
    }


def _command_center_whale_context() -> Dict[str, Any]:
    candidates: List[Dict[str, Any]] = []
    cached_whale = _cache_get("whale_summary:v2")
    if isinstance(cached_whale, dict):
        candidates.append(_response_to_dict(cached_whale))
    cached_slow = _cache_get("cc_slow")
    if isinstance(cached_slow, dict):
        if isinstance(cached_slow.get("whale"), dict):
            candidates.append(dict(cached_slow.get("whale") or {}))
        candidates.append(dict(cached_slow or {}))
    for candidate in candidates:
        normalized = _normalize_command_center_whale_payload(candidate)
        if normalized.get("alert_count_24h") or normalized.get("recent") or normalized.get("alerts"):
            return normalized
    try:
        payload = _db_whale_dashboard_summary(
            min_value_usd=500_000,
            hours=24,
            recent_limit=10,
            alert_limit=20,
        )
        normalized = _normalize_command_center_whale_payload(payload)
        if normalized.get("alert_count_24h") or normalized.get("recent") or normalized.get("alerts"):
            _cache_set("whale_summary:v2", normalized, ttl=300)
            return normalized
        _cache_set("whale_summary:v2", normalized, ttl=45)
        return normalized
    except Exception as exc:
        logger.debug(f"[dashboard] command-center DB whale fallback failed: {exc}")
    return _normalize_command_center_whale_payload({})


def _command_center_sentiment_context(
    slow_context: Dict[str, Any],
    whale_context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    slow_context = dict(slow_context or {})
    whale_context = dict(whale_context or {})
    cached = _cache_get("sentiment_dashboard")
    payload = _response_to_dict(cached) if isinstance(cached, dict) else {}
    if not isinstance(payload, dict):
        payload = {}
    nested_slow = slow_context.get("sentiment")
    if isinstance(nested_slow, dict):
        merged = dict(nested_slow)
        merged.update(payload)
        payload = merged
    score = payload.get("score", payload.get("sentiment_score", slow_context.get("sentiment_score", 0.0)))
    try:
        score = float(score or 0.0)
    except Exception:
        score = 0.0
    whale_count = int(
        whale_context.get("alert_count_24h")
        or whale_context.get("whale_alerts_24h")
        or len(whale_context.get("recent") or [])
        or len(whale_context.get("alerts") or [])
        or 0
    )
    return {
        **payload,
        "success": bool(payload.get("success", True)),
        "score": round(score, 3),
        "sentiment_score": round(score, 3),
        "whale_alert_count": whale_count,
        "has_activity": bool(abs(score) > 0.001 or whale_count),
        "source": payload.get("source") or ("market_derived" if payload else "command_center_context"),
    }


def _fetch_command_center_live_snapshots(
    positions: Any,
    *,
    limit: Optional[int] = None,
    max_age_seconds: Optional[float] = None,
) -> Dict[str, Dict[str, Any]]:
    assets: List[Tuple[str, str]] = []
    snapshots: Dict[str, Dict[str, Any]] = {}
    position_list = list(positions or [])
    if limit is not None:
        position_list = position_list[: max(1, int(limit or 0))]
    for position in position_list:
        asset = str(position.get("asset", "") or "")
        category = str(position.get("category", "forex") or "forex")
        if asset and asset not in snapshots:
            assets.append((asset, category))
    if not assets:
        return snapshots
    try:
        from websocket_dashboard import get_live_price_snapshots

        raw = get_live_price_snapshots(
            assets=[asset for asset, _category in assets],
            max_age_seconds=max_age_seconds,
        ) or {}
        for asset, _category in assets:
            snapshots[asset] = dict(raw.get(asset) or {})
    except Exception:
        pass
    return snapshots


def _dashboard_live_quote_fallback(asset: str, category: str) -> tuple[Optional[float], str]:
    try:
        return _chart_stream_live_quote(asset, category, allow_live_cache=False)
    except Exception:
        return None, ""


def _build_command_center_enriched_positions(
    positions: Any,
    live_snapshots: Optional[Dict[str, Dict[str, Any]]] = None,
    *,
    allow_provider_fallback: bool = False,
) -> List[Dict[str, Any]]:
    enriched_positions: List[Dict[str, Any]] = []
    for position in list(positions or []):
        asset = str(position.get("asset", "") or "")
        quote = resolve_live_position_snapshot(
            position,
            live_snapshot=(live_snapshots or {}).get(asset),
            live_snapshot_max_age_seconds=3.0,
            provider_fallback=_dashboard_live_quote_fallback if allow_provider_fallback else None,
        )
        current_price = float(quote.get("current_price", 0.0) or 0.0)
        entry_price = float(quote.get("entry_price", 0.0) or 0.0)
        position_size = float(quote.get("position_size", 0.0) or 0.0)
        direction = str(quote.get("direction") or "BUY")
        category = str(quote.get("category", "forex") or "forex")
        try:
            lot_size = float(position.get("lot_size", 0) or 0)
        except Exception:
            lot_size = 0.0
        if not lot_size and position_size:
            try:
                from risk.position_sizer import PositionSizer as _PS

                lot_size = _PS.lots_from_size(asset, category, position_size)
            except Exception:
                lot_size = 0.0
        live_pnl = float(quote.get("pnl", position.get("pnl", 0.0)) or 0.0)
        metadata = dict(position.get("metadata") or {})
        enriched_positions.append({
            "trade_id": position.get("trade_id", ""),
            "asset": asset,
            "category": category,
            "direction": direction,
            "confidence": float(position.get("confidence", 0) or 0),
            "entry_price": entry_price,
            "current_price": current_price,
            "stop_loss": float(position.get("stop_loss", 0) or 0),
            "take_profit": float(position.get("take_profit", 0) or 0),
            "take_profit_levels": list(position.get("take_profit_levels", []) or []),
            "tp_hit": int(position.get("tp_hit", 0) or 0),
            "pnl": round(live_pnl, 2),
            "position_size": position_size,
            "lot_size": round(lot_size, 4),
            "strategy_id": position.get("strategy_id", ""),
            "open_time": str(position.get("open_time", "") or ""),
            "risk_reward": float(position.get("risk_reward", 0) or 0),
            "metadata": metadata,
            "price_source": str(quote.get("price_source", "") or ""),
            "price_age_seconds": quote.get("price_age_seconds"),
            "price_live": bool(quote.get("price_live")),
            **_extract_entry_structure_fields(metadata),
            **_extract_execution_feedback_fields(metadata),
            **_extract_memory_fields(metadata),
            **_extract_opportunity_fields(metadata),
            **_extract_signal_intelligence_fields(metadata),
            **_extract_market_data_provenance_fields(metadata, asset=asset, category=category),
        })
    return enriched_positions


def _build_command_center_signals(enriched_positions: Any) -> List[Dict[str, Any]]:
    signals: List[Dict[str, Any]] = []
    for position in list(enriched_positions or [])[:6]:
        signals.append({
            "asset": position.get("asset", ""),
            "signal": position.get("direction", "BUY"),
            "direction": position.get("direction", "BUY"),
            "confidence": float(position.get("confidence", 0) or 0),
            "entry_price": float(position.get("entry_price", 0) or 0),
            "current_price": float(position.get("current_price", 0) or 0),
            "stop_loss": float(position.get("stop_loss", 0) or 0),
            "take_profit": float(position.get("take_profit", 0) or 0),
            "category": position.get("category", ""),
            "strategy_id": position.get("strategy_id", ""),
            "pnl": float(position.get("pnl", 0) or 0),
            "metadata": dict(position.get("metadata") or {}),
            **_extract_entry_structure_fields(position.get("metadata") or {}),
            **_extract_execution_feedback_fields(position.get("metadata") or {}),
            **_extract_memory_fields(position.get("metadata") or {}),
            **_extract_opportunity_fields(position.get("metadata") or {}),
            **_extract_signal_intelligence_fields(position.get("metadata") or {}),
            **_extract_market_data_provenance_fields(
                position.get("metadata") or {},
                asset=str(position.get("asset", "") or ""),
                category=str(position.get("category", "") or ""),
            ),
        })
    return signals


def _command_center_context_row_key(row: Dict[str, Any]) -> Tuple[str, str, str, str, str, str, str]:
    return (
        str(row.get("asset") or ""),
        str(row.get("direction") or row.get("signal") or ""),
        str(row.get("decision_kind") or row.get("kind") or ""),
        str(row.get("decision_state") or row.get("state") or ""),
        str(
            row.get("exact_kill_reason")
            or row.get("execution_kill_reason")
            or row.get("blocked_reason")
            or row.get("decision_reason")
            or row.get("reason")
            or ""
        ),
        str(row.get("session_label") or row.get("current_session") or ""),
        str(row.get("timeframe") or row.get("playbook_timeframe") or ""),
    )


def _merge_command_center_rows(*sources: Any) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    seen: set = set()
    for source in sources:
        for row in list(source or []):
            if not isinstance(row, dict):
                continue
            enriched = _enrich_signal_like_row(row)
            key = _command_center_context_row_key(enriched)
            if key in seen:
                continue
            seen.add(key)
            rows.append(enriched)
    return rows


def _command_center_row_is_blocked(row: Any) -> bool:
    if not isinstance(row, dict):
        return False
    state = str(row.get("decision_state") or row.get("state") or "").strip().lower()
    kind = str(row.get("decision_kind") or row.get("kind") or "").strip().lower()
    if state == "watching" or kind == "watching_seed":
        return False
    if "blocked" in {state, kind} or kind in {"killed", "blocked_asset"}:
        return True
    reason = str(
        row.get("exact_kill_reason")
        or row.get("execution_kill_reason")
        or row.get("blocked_reason")
        or row.get("decision_reason")
        or row.get("reason")
        or ""
    ).strip().lower()
    if reason in {"", "n/a", "no_playbook_seed", "waiting_for_playbook_seed"}:
        return False
    return bool(reason)


def _command_center_runtime_context_rows(*, limit: int = 24) -> List[Dict[str, Any]]:
    target = max(1, int(limit or 24))
    cache_key = f"cc_runtime_context:{target}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return [dict(row) for row in list(cached or []) if isinstance(row, dict)]

    rows: List[Dict[str, Any]] = []
    try:
        from services.redis_pool import get_client as _get_redis_client

        redis_client = _get_redis_client()
        if redis_client is None:
            raise RuntimeError("Redis unavailable")
        raw = redis_client.lrange(_COMMAND_CENTER_RUNTIME_CONTEXT_LOG_KEY, 0, max(target * 4, 40))
        seen: set = set()
        for item in raw:
            if not item:
                continue
            try:
                payload = json.loads(item)
            except Exception:
                continue
            if not isinstance(payload, dict):
                continue
            enriched = _enrich_signal_like_row(payload)
            key = _command_center_context_row_key(enriched)
            if key in seen:
                continue
            seen.add(key)
            rows.append(enriched)
            if len(rows) >= target:
                break
    except Exception as exc:
        logger.debug(f"[dashboard] command-center runtime context unavailable: {exc}")
        rows = []

    _cache_set(cache_key, rows, ttl=4 if rows else 2)
    return rows


def _command_center_live_decisions(core: Any) -> List[Dict[str, Any]]:
    try:
        rows = _collect_live_signals_from_core(core, "all") if core else _collect_live_signals_from_store("all")
    except Exception as exc:
        logger.debug(f"[dashboard] command-center live signal store unavailable: {exc}")
        rows = []
    return [_enrich_signal_like_row(row) for row in list(rows or []) if isinstance(row, dict)]


def _command_center_signal_quality(signals: Any) -> Dict[str, Any]:
    signal_list = list(signals or [])
    return {
        "count": len(signal_list),
        "avg_memory_score": round(
            sum(float(signal.get("memory_score", 0.0) or 0.0) for signal in signal_list) / len(signal_list),
            1,
        ) if signal_list else 0.0,
        "avg_execution_quality": round(
            sum(float(signal.get("execution_quality_score", 0.0) or 0.0) for signal in signal_list) / len(signal_list),
            1,
        ) if signal_list else 0.0,
        "avg_opportunity_score": round(
            sum(float(signal.get("opportunity_score", 0.0) or 0.0) for signal in signal_list) / len(signal_list),
            3,
        ) if signal_list else 0.0,
        "memory_ready_count": sum(1 for signal in signal_list if int(signal.get("memory_sample_count", 0) or 0) > 0),
        "execution_ready_count": sum(1 for signal in signal_list if int(signal.get("execution_feedback_sample_count", 0) or 0) > 0),
        "top_signal_asset": (
            max(
                signal_list,
                key=lambda item: (
                    float(item.get("opportunity_score", 0.0) or 0.0),
                    float(item.get("confidence", 0.0) or 0.0),
        ),
        ).get("asset", "")
            if signal_list else ""
        ),
    }


def _dashboard_local_timezone():
    try:
        from zoneinfo import ZoneInfo

        return ZoneInfo("Africa/Nairobi")
    except Exception:
        return timezone(timedelta(hours=3))


def _dashboard_now_local() -> datetime:
    return datetime.now(_dashboard_local_timezone())


def _build_command_center_live_summary(perf: Dict[str, Any], daily: Dict[str, Any], positions: Any) -> Dict[str, Any]:
    position_list = [pos for pos in list(positions or []) if isinstance(pos, dict)]
    balance = float(perf.get("balance", _args.balance) or _args.balance)
    initial_balance = float(perf.get("initial_balance", balance) or balance)
    total_pnl = float(perf.get("total_pnl", 0) or 0)
    realized_daily_pnl = float(daily.get("daily_pnl", 0) or 0)
    closed_total = int(perf.get("total_trades", 0) or 0)

    raw_win_rate = float(perf.get("win_rate", 0) or 0)
    closed_win_rate = raw_win_rate * 100.0 if raw_win_rate <= 1.0 else raw_win_rate
    closed_wins = perf.get("winning_trades")
    if closed_wins in (None, ""):
        closed_wins = int(round(closed_total * (closed_win_rate / 100.0))) if closed_total else 0
    else:
        closed_wins = int(closed_wins or 0)
    closed_losses = perf.get("losing_trades")
    if closed_losses in (None, ""):
        closed_losses = max(0, closed_total - closed_wins)
    else:
        closed_losses = int(closed_losses or 0)

    open_pnl = round(sum(float(pos.get("pnl", 0.0) or 0.0) for pos in position_list), 2)
    open_count = len(position_list)
    buy_count = sum(1 for pos in position_list if str(pos.get("direction") or pos.get("signal") or "").upper() == "BUY")
    sell_count = sum(1 for pos in position_list if str(pos.get("direction") or pos.get("signal") or "").upper() == "SELL")
    profitable_open = sum(1 for pos in position_list if float(pos.get("pnl", 0.0) or 0.0) > 0)
    losing_open = sum(1 for pos in position_list if float(pos.get("pnl", 0.0) or 0.0) < 0)
    live_total_trades = closed_total + open_count
    live_wins = closed_wins + profitable_open
    live_win_rate = round((live_wins / live_total_trades) * 100.0, 1) if live_total_trades else round(closed_win_rate, 1)
    live_balance = round(balance + open_pnl, 2)
    live_total_pnl = round(total_pnl + open_pnl, 2)
    live_daily_pnl = round(realized_daily_pnl + open_pnl, 2)
    equity_delta = round(live_balance - initial_balance, 2)
    book_bias = "BUY-heavy" if buy_count > sell_count else "SELL-heavy" if sell_count > buy_count else "Balanced"
    open_state = "Winning" if open_pnl > 0 else "Losing" if open_pnl < 0 else "Flat"

    return {
        "balance": live_balance,
        "realized_balance": round(balance, 2),
        "initial_balance": round(initial_balance, 2),
        "balance_delta": equity_delta,
        "total_pnl": live_total_pnl,
        "realized_total_pnl": round(total_pnl, 2),
        "daily_pnl": live_daily_pnl,
        "realized_daily_pnl": round(realized_daily_pnl, 2),
        "daily_trades": int(daily.get("daily_trades", 0) or 0),
        "open_pnl": open_pnl,
        "win_rate": live_win_rate,
        "realized_win_rate": round(closed_win_rate, 1),
        "closed_trades": closed_total,
        "open_positions": open_count,
        "total_trades": live_total_trades,
        "winning_trades": live_wins,
        "losing_trades": closed_losses + losing_open,
        "open_winners": profitable_open,
        "open_losers": losing_open,
        "buy_count": buy_count,
        "sell_count": sell_count,
        "book_bias": book_bias,
        "open_state": open_state,
    }


def _authoritative_account_summary(core: Any = None, positions: Any = None) -> Dict[str, Any]:
    perf, daily, snapshot_positions, _health, closed_trades = _command_center_core_snapshot(core)
    selected_positions = [dict(pos) for pos in list(positions if positions is not None else snapshot_positions or []) if isinstance(pos, dict)]
    if selected_positions:
        try:
            snapshots = _fetch_command_center_live_snapshots(selected_positions, max_age_seconds=None)
            selected_positions = _build_command_center_enriched_positions(selected_positions, snapshots)
        except Exception:
            pass
    summary = _build_command_center_live_summary(perf, daily, selected_positions)

    initial_balance = float(summary.get("initial_balance", getattr(_args, "balance", 10000.0)) or getattr(_args, "balance", 10000.0))
    live_balance = float(summary.get("balance", initial_balance) or initial_balance)
    summary["balance_delta"] = round(live_balance - initial_balance, 2)
    summary["closed_trades"] = int(summary.get("closed_trades", len(closed_trades or [])) or 0)
    summary["total_trades"] = int(summary.get("total_trades", summary["closed_trades"]) or 0)
    summary["daily_trades"] = int(summary.get("daily_trades", 0) or 0)
    summary["account_state"] = "loss" if summary["balance_delta"] < -0.005 else "gain" if summary["balance_delta"] > 0.005 else "flat"
    return summary


def _attach_authoritative_account_fields(payload: Dict[str, Any], account: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    payload = dict(payload or {})
    account = dict(account or _authoritative_account_summary(_core()))
    payload["live_summary"] = account

    field_map = {
        "balance": "balance",
        "realized_balance": "realized_balance",
        "initial_balance": "initial_balance",
        "balance_delta": "balance_delta",
        "total_pnl": "total_pnl",
        "realized_total_pnl": "realized_total_pnl",
        "daily_pnl": "daily_pnl",
        "realized_daily_pnl": "realized_daily_pnl",
        "open_pnl": "open_pnl",
        "win_rate": "win_rate",
        "open_positions": "open_positions",
        "total_trades": "total_trades",
        "closed_trades": "closed_trades",
    }
    for target, source in field_map.items():
        if source in account:
            payload[target] = account.get(source)
    payload["closed_positions"] = account.get("closed_trades", payload.get("closed_positions", 0))
    payload["daily_trades"] = account.get("daily_trades", payload.get("daily_trades", 0))
    payload["account_state"] = account.get("account_state", "flat")
    return payload


def _build_live_book_payload() -> Dict[str, Any]:
    core = _core()
    perf, daily, positions, _health, _closed_trades = _command_center_core_snapshot(core)
    position_rows = [dict(pos) for pos in list(positions or []) if isinstance(pos, dict)]
    snapshots = _fetch_command_center_live_snapshots(position_rows, max_age_seconds=None)

    enriched_positions: List[Dict[str, Any]] = []
    stale_assets: List[str] = []
    for position in position_rows:
        asset = str(position.get("asset", "") or "")
        quote = resolve_live_position_snapshot(
            position,
            live_snapshot=snapshots.get(asset),
            live_snapshot_max_age_seconds=3.0,
            provider_fallback=_dashboard_live_quote_fallback,
        )
        if asset and not bool(quote.get("price_live")):
            stale_assets.append(asset)
        enriched_positions.append(
            {
                "trade_id": position.get("trade_id", ""),
                "asset": asset,
                "category": str(quote.get("category", "forex") or "forex"),
                "direction": str(quote.get("direction") or "BUY"),
                "entry_price": float(quote.get("entry_price", 0.0) or 0.0),
                "current_price": round(float(quote.get("current_price", 0.0) or 0.0), 8),
                "pnl": round(float(quote.get("pnl", 0.0) or 0.0), 2),
                "position_size": float(quote.get("position_size", 0.0) or 0.0),
                "lot_size": float(position.get("lot_size", 0) or 0),
                "confidence": float(position.get("confidence", 0) or 0),
                "open_time": str(position.get("open_time", "") or ""),
                "price_source": str(quote.get("price_source", "") or ""),
                "price_age_seconds": quote.get("price_age_seconds"),
                "price_live": bool(quote.get("price_live")),
            }
        )

    live_summary = _build_command_center_live_summary(perf, daily, enriched_positions)
    return {
        "success": True,
        "timestamp": datetime.utcnow().isoformat(),
        "positions": enriched_positions,
        "live_summary": live_summary,
        "open_positions": len(enriched_positions),
        "updated_assets": [item["asset"] for item in enriched_positions if item.get("price_live")],
        "stale_assets": stale_assets,
    }


def _build_command_center_pnl_curve(
    positions: Any,
    closed_trades: Any,
    *,
    interval_minutes: int = 30,
    current_daily_pnl: Optional[float] = None,
    current_open_pnl: Optional[float] = None,
) -> Dict[str, Any]:
    tz = _dashboard_local_timezone()
    now_local = _dashboard_now_local()
    today_start = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
    bucket_minutes = max(5, int(interval_minutes or 30))
    current_bucket = now_local.replace(
        minute=(now_local.minute // bucket_minutes) * bucket_minutes,
        second=0,
        microsecond=0,
    )
    bucket_times: List[datetime] = []
    cursor = today_start
    while cursor <= current_bucket:
        bucket_times.append(cursor)
        cursor += timedelta(minutes=bucket_minutes)
    if not bucket_times:
        bucket_times.append(today_start)

    try:
        from websocket_dashboard import get_live_price_history
    except Exception:
        get_live_price_history = None

    try:
        from services.local_candle_store import local_candle_store
    except Exception:
        local_candle_store = None

    history_map: Dict[str, Dict[str, List[float]]] = {}
    position_rows = [pos for pos in list(positions or []) if isinstance(pos, dict)]
    for pos in position_rows:
        asset = str(pos.get("asset") or "")
        category = str(pos.get("category") or "forex")
        if not asset or asset in history_map:
            continue

        history_points: List[Tuple[float, float]] = []
        if local_candle_store is not None and getattr(local_candle_store, "enabled", None):
            try:
                if local_candle_store.enabled():
                    periods = max(60, int(((now_local - today_start).total_seconds() / 60.0)) + bucket_minutes + 5)
                    frame, _meta = local_candle_store.get_ohlcv(
                        asset,
                        category,
                        "1m",
                        periods,
                        end_time=now_local.astimezone(timezone.utc),
                        closed_only=False,
                    )
                    if frame is not None and not frame.empty and "close" in frame.columns:
                        try:
                            import pandas as pd
                        except Exception:
                            pd = None
                        for idx, row in frame.iterrows():
                            try:
                                if pd is not None:
                                    ts = float(pd.Timestamp(idx).timestamp())
                                else:
                                    ts = float(idx.timestamp())
                            except Exception:
                                ts = 0.0
                            price = float(row.get("close", 0.0) or 0.0)
                            if ts > 0 and price > 0:
                                history_points.append((ts, price))
            except Exception:
                history_points = []

        if not history_points and get_live_price_history is not None:
            try:
                for item in get_live_price_history(asset, limit=2000) or []:
                    ts = float(item.get("timestamp", 0.0) or 0.0)
                    price = float(item.get("price", 0.0) or 0.0)
                    if ts > 0 and price > 0:
                        history_points.append((ts, price))
            except Exception:
                history_points = []

        history_points.sort(key=lambda item: item[0])
        history_map[asset] = {
            "timestamps": [point[0] for point in history_points],
            "prices": [point[1] for point in history_points],
        }

    def _price_at(asset: str, bucket_ts: float) -> Optional[float]:
        series = history_map.get(asset) or {}
        timestamps = series.get("timestamps") or []
        prices = series.get("prices") or []
        if timestamps and prices:
            idx = bisect_right(timestamps, bucket_ts) - 1
            if idx >= 0:
                return float(prices[idx])
        return None

    closed_events: List[Tuple[datetime, float]] = []
    for trade in list(closed_trades or []):
        if not isinstance(trade, dict):
            continue
        exit_dt = _coerce_utc_datetime(trade.get("exit_time") or trade.get("entry_time"))
        if not exit_dt:
            continue
        local_exit = exit_dt.astimezone(tz)
        if local_exit < today_start:
            continue
        closed_events.append((local_exit, float(trade.get("pnl", 0.0) or 0.0)))

    closed_events.sort(key=lambda item: item[0])

    open_positions: List[Dict[str, Any]] = []
    for pos in position_rows:
        entry_dt = _coerce_utc_datetime(pos.get("open_time") or pos.get("entry_time"))
        open_positions.append(
            {
                "asset": str(pos.get("asset") or ""),
                "category": str(pos.get("category") or "forex"),
                "direction": str(pos.get("direction") or pos.get("signal") or "BUY").upper(),
                "entry_price": float(pos.get("entry_price", 0) or 0),
                "position_size": float(pos.get("position_size", 0) or 0),
                "open_time": entry_dt.astimezone(tz) if entry_dt else None,
            }
        )

    def _mark_to_market(pos: Dict[str, Any], bucket: datetime) -> float:
        if pos["open_time"] and pos["open_time"] > bucket:
            return 0.0
        price = _price_at(pos["asset"], bucket.timestamp())
        if price is None or not pos["entry_price"] or not pos["position_size"]:
            return 0.0
        try:
            from risk.position_sizer import PositionSizer as _PS

            return float(
                _PS.pnl(
                    pos["asset"],
                    pos["category"],
                    pos["entry_price"],
                    price,
                    pos["position_size"],
                    pos["direction"],
                )
            )
        except Exception:
            if pos["direction"] == "BUY":
                return (price - pos["entry_price"]) * pos["position_size"]
            return (pos["entry_price"] - price) * pos["position_size"]

    curve: List[Dict[str, Any]] = []
    realized_cum = 0.0
    realized_idx = 0
    for bucket in bucket_times:
        while realized_idx < len(closed_events) and closed_events[realized_idx][0] <= bucket:
            realized_cum += closed_events[realized_idx][1]
            realized_idx += 1
        open_pnl = sum(_mark_to_market(pos, bucket) for pos in open_positions)
        cumulative_pnl = round(realized_cum + open_pnl, 2)
        curve.append(
            {
                "time": bucket.isoformat(),
                "label": bucket.strftime("%H:%M"),
                "realized_pnl": round(realized_cum, 2),
                "open_pnl": round(open_pnl, 2),
                "cumulative_pnl": cumulative_pnl,
            }
        )

    if curve and current_daily_pnl is not None:
        live_open_pnl = round(float(current_open_pnl or 0.0), 2)
        live_daily_total = round(float(current_daily_pnl or 0.0), 2)
        curve[-1] = {
            "time": now_local.isoformat(),
            "label": current_bucket.strftime("%H:%M"),
            "realized_pnl": round(live_daily_total - live_open_pnl, 2),
            "open_pnl": live_open_pnl,
            "cumulative_pnl": live_daily_total,
        }

    running_peak = 0.0
    max_drawdown = 0.0
    for point in curve:
        cumulative_pnl = float(point.get("cumulative_pnl", 0.0) or 0.0)
        running_peak = max(running_peak, cumulative_pnl)
        max_drawdown = min(max_drawdown, cumulative_pnl - running_peak)

    return {
        "points": curve,
        "interval_minutes": bucket_minutes,
        "timezone": "Africa/Nairobi",
        "current_pnl": curve[-1]["cumulative_pnl"] if curve else 0.0,
        "peak": round(running_peak, 2),
        "drawdown": round(abs(max_drawdown), 2),
        "start_time": bucket_times[0].isoformat() if bucket_times else now_local.isoformat(),
        "end_time": bucket_times[-1].isoformat() if bucket_times else now_local.isoformat(),
    }


def _build_command_center_payload() -> Dict[str, Any]:
    core = _core()
    perf, daily, positions, health, closed_trades = _command_center_core_snapshot(core)
    journals = _command_center_journals()
    runtime_context_rows = _command_center_runtime_context_rows(limit=24)

    # Slow external calls are refreshed off the request path. The command
    # center must render from cached/stale data instead of letting whale or
    # sentiment collectors own the whole page load.
    _cc_slow = _cache_get("cc_slow")
    if _cc_slow is None:
        _trigger_dashboard_payload_refresh(
            "cc_slow",
            builder=_fetch_command_center_slow_data,
            cache_key="cc_slow",
            ttl=600,
        )
        _cc_slow = {
            "sentiment_score": 0.0,
            "whale_alerts_24h": 0,
            "alert_count_24h": 0,
            "recent": [],
        }
    whale_recent = list((_cc_slow or {}).get("recent") or [])
    whale_count = int((_cc_slow or {}).get("alert_count_24h", 0) or 0)
    sent_score = float((_cc_slow or {}).get("sentiment_score", 0.0) or 0.0)
    if whale_recent or whale_count or sent_score:
        _cache_set("cc_slow", _cc_slow, ttl=600)
    whale_context = _command_center_whale_context()
    if whale_context.get("alert_count_24h") or whale_context.get("recent") or whale_context.get("alerts"):
        whale_recent = list(whale_context.get("recent", []) or whale_recent)
        whale_count = int(
            whale_context.get("alert_count_24h", whale_context.get("whale_alerts_24h", whale_count))
            or whale_count
        )
    sentiment_context = _command_center_sentiment_context(_cc_slow, whale_context)
    sent_score = float(sentiment_context.get("sentiment_score", sent_score) or sent_score)

    live_snapshots = _fetch_command_center_live_snapshots(positions, max_age_seconds=None)
    enriched_positions = _build_command_center_enriched_positions(positions, live_snapshots)
    live_summary = _build_command_center_live_summary(perf, daily, enriched_positions)
    live_decisions = _command_center_live_decisions(core)
    signals = live_decisions or _build_command_center_signals(enriched_positions)
    signal_context_rows = _merge_command_center_rows(signals, runtime_context_rows)
    signal_quality = _command_center_signal_quality(signal_context_rows)
    signal_diagnostics = _summarize_signal_diagnostics(signal_context_rows or enriched_positions)
    pnl_curve = _build_command_center_pnl_curve(
        enriched_positions,
        closed_trades,
        interval_minutes=30,
        current_daily_pnl=float(live_summary.get("daily_pnl", 0.0) or 0.0),
        current_open_pnl=float(live_summary.get("open_pnl", 0.0) or 0.0),
    )
    top_opportunities = _cache_get("cc_top_opportunities")
    weak_positions = _cache_get("cc_weak_positions")
    if core and (top_opportunities is None or weak_positions is None):
        if top_opportunities is None:
            try:
                top_opportunities = _get_command_center_top_opportunities(
                    core,
                    limit=max(3, int(TOP_OPPORTUNITIES_LIMIT or 10)),
                )
            except Exception as exc:
                logger.debug(f"[dashboard] command-center top_opportunities error: {exc}")
                top_opportunities = []
        if weak_positions is None:
            try:
                weak_positions = _get_command_center_weak_positions(
                    core,
                    limit=max(3, int(DASHBOARD_WEAK_POSITIONS_LIMIT or 8)),
                )
            except Exception as exc:
                logger.debug(f"[dashboard] command-center weak_positions error: {exc}")
                weak_positions = []

    if top_opportunities is None:
        top_opportunities = []
    if weak_positions is None:
        weak_positions = []
    top_opportunities = [_enrich_signal_like_row(item) for item in list(top_opportunities or []) if isinstance(item, dict)]
    weak_positions = [_enrich_signal_like_row(item) for item in list(weak_positions or []) if isinstance(item, dict)]

    _cache_set("cc_top_opportunities", top_opportunities, ttl=20 if top_opportunities else 8)
    _cache_set("cc_weak_positions", weak_positions, ttl=20 if weak_positions else 8)
    runtime_blocked_rows = [row for row in runtime_context_rows if _command_center_row_is_blocked(row)]
    near_miss_source = _merge_command_center_rows(journals, runtime_blocked_rows)
    near_misses = _summarize_near_misses(near_miss_source, limit=8)
    crypto_rejection_audit = _summarize_crypto_rejection_audit(journals, limit=8)
    session_radar = _build_session_radar(limit=12)
    why_not_source = _merge_command_center_rows(journals, runtime_context_rows)
    why_not_traded = _summarize_why_not_traded(why_not_source, near_misses, limit=6)
    watchlist_ladder = _build_watchlist_ladder(top_opportunities, near_misses, session_radar, enriched_positions)
    trade_tape = _build_trade_tape(enriched_positions, closed_trades, limit=12)
    trade_lifecycle = _build_trade_lifecycle(
        enriched_positions,
        closed_trades,
        journals,
        active_items=[*signals, *top_opportunities, *near_misses],
    )
    decision_context = _build_decision_context_payload(
        latest_signals=signals,
        runtime_rows=runtime_context_rows,
        top_opportunities=top_opportunities,
        near_misses=near_misses,
        session_radar=session_radar,
        positions=enriched_positions,
        why_not_traded=why_not_traded,
        signal_diagnostics=signal_diagnostics,
    )

    payload = {
        "success":           True,
        "balance":           float(perf.get("balance", _args.balance) or _args.balance),
        "total_pnl":         float(perf.get("total_pnl", 0) or 0),
        "daily_pnl":         float(daily.get("daily_pnl", 0) or 0),
        "daily_trades":      int(daily.get("daily_trades", 0) or 0),
        "win_rate":          _wr(perf.get("win_rate", 0)),
        "open_positions":    len(enriched_positions),
        "total_trades":      int(perf.get("total_trades", 0) or 0),
        "engine_running":    health.get("is_running", core.is_running if core else False),
        "engine_ready":      health.get("engine_ready", core.is_ready if core else False),
        "sentiment_score":   round(sent_score, 3),
        "sentiment_context": sentiment_context,
        "whale":             whale_context,
        "whale_alerts_24h":  whale_count,
        "alert_count_24h":   whale_count,
        "recent":            whale_recent,
        "latest_signals":    signals,
        "signal_quality":    signal_quality,
        "top_opportunities": top_opportunities,
        "weak_positions":    weak_positions,
        "near_misses":       near_misses,
        "crypto_rejection_audit": crypto_rejection_audit,
        "why_not_traded":    why_not_traded,
        "session_radar":     session_radar,
        "watchlist_ladder":  watchlist_ladder,
        "trade_tape":        trade_tape,
        "trade_lifecycle":   trade_lifecycle,
        "decision_context":  decision_context,
        "positions":         enriched_positions,
        "live_summary":      live_summary,
        "pnl_curve":         pnl_curve.get("points", []),
        "pnl_curve_stats":   {k: v for k, v in pnl_curve.items() if k != "points"},
        "provider_routing":  _provider_routing_summary(),
        "signal_diagnostics": signal_diagnostics,
        "timestamp":         datetime.now().isoformat(),
    }
    payload = _attach_authoritative_account_fields(payload, live_summary)
    return _normalize_command_center_payload_contract(payload)


def _command_center_payload_signature(payload: Dict[str, Any]) -> str:
    signature_payload = dict(payload or {})
    signature_payload.pop("timestamp", None)
    raw = json.dumps(signature_payload, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


@app.route("/api/command-center")
@_check_api_auth
@_check_rate_limit
def api_command_center():
    try:
        return jsonify(_get_cached_command_center_payload(force_refresh=_request_wants_cache_bypass()))
    except APIError as e:
        return handle_api_error(e, "/api/command-center", e.status_code)
    except Exception as e:
        return handle_api_error(e, "/api/command-center", 500)


@app.route("/api/live-book")
@_check_api_auth
@_check_rate_limit
def api_live_book():
    cache_key = "live_book:v1"
    force_refresh = _request_wants_cache_bypass()
    if not force_refresh:
        cached = _cache_get(cache_key)
        if cached is not None:
            return jsonify(cached)
    try:
        payload = _build_live_book_payload()
        if not force_refresh:
            _cache_set(cache_key, payload, ttl=1)
        response = jsonify(payload)
        response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        return response
    except APIError as e:
        return handle_api_error(e, "/api/live-book", e.status_code)
    except Exception as e:
        return handle_api_error(e, "/api/live-book", 500)


@app.route("/api/live-book/stream")
@_check_api_auth
@_check_rate_limit
def api_live_book_stream():
    def _signature(payload: Dict[str, Any]) -> str:
        slim = {
            "positions": [
                {
                    "trade_id": str(item.get("trade_id", "") or ""),
                    "asset": str(item.get("asset", "") or ""),
                    "current_price": round(float(item.get("current_price", 0.0) or 0.0), 8),
                    "pnl": round(float(item.get("pnl", 0.0) or 0.0), 2),
                    "price_live": bool(item.get("price_live")),
                    "price_source": str(item.get("price_source", "") or ""),
                }
                for item in list((payload or {}).get("positions") or [])
            ],
            "live_summary": dict((payload or {}).get("live_summary") or {}),
        }
        return hashlib.sha256(
            json.dumps(slim, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
        ).hexdigest()

    def _gen():
        last_signature = ""
        last_emit = 0.0
        yield f"data: {json.dumps({'type': 'connected', 'ts': int(time.time())})}\n\n"
        while True:
            try:
                payload = _build_live_book_payload()
                signature = _signature(payload)
                now = time.time()
                if signature != last_signature:
                    last_signature = signature
                    last_emit = now
                    yield f"data: {json.dumps({'type': 'snapshot', 'payload': payload, 'ts': int(now)})}\n\n"
                elif now - last_emit >= 5.0:
                    last_emit = now
                    yield f"data: {json.dumps({'type': 'heartbeat', 'ts': int(now)})}\n\n"
                time.sleep(0.5)
            except GeneratorExit:
                raise
            except Exception as exc:
                logger.debug(f"[dashboard] live-book stream error: {exc}")
                yield f"data: {json.dumps({'type': 'heartbeat', 'ts': int(time.time())})}\n\n"
                time.sleep(1.0)

    return Response(
        stream_with_context(_gen()),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


@app.route("/api/command-center/stream")
@_check_api_auth
@_check_rate_limit
def api_command_center_stream():
    def _gen():
        last_signature = ""
        last_emit = 0.0
        yield f"data: {json.dumps({'type': 'connected', 'ts': int(time.time())})}\n\n"
        while True:
            try:
                payload = _get_cached_command_center_payload(force_refresh=False)
                signature = _command_center_payload_signature(payload)
                now = time.time()
                if signature != last_signature:
                    last_signature = signature
                    last_emit = now
                    yield f"data: {json.dumps({'type': 'refresh', 'ts': int(now), 'signature': signature})}\n\n"
                elif now - last_emit >= 5.0:
                    last_emit = now
                    yield f"data: {json.dumps({'type': 'heartbeat', 'ts': int(now)})}\n\n"
                time.sleep(1.0)
            except GeneratorExit:
                raise
            except Exception as exc:
                logger.debug(f"[dashboard] command-center stream error: {exc}")
                yield f"data: {json.dumps({'type': 'heartbeat', 'ts': int(time.time())})}\n\n"
                time.sleep(1.0)

    return Response(
        stream_with_context(_gen()),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )

# ══════════════════════════════════════════════════════════════════════════════
# API — SIGNALS
# ══════════════════════════════════════════════════════════════════════════════

def _live_signal_matches_filter(direction: str, confidence: float, filt: str) -> bool:
    if filt == "buy":
        return direction == "BUY"
    if filt == "sell":
        return direction == "SELL"
    if filt == "high":
        return confidence >= 0.70
    return True


def _live_signal_current_price(asset: str, category: str) -> float:
    try:
        current_price, _ = _get_fetcher().get_real_time_price(asset, category)
        if current_price:
            return float(current_price)
    except Exception:
        pass
    return 0.0


def _live_signal_payload(
    payload: Dict[str, Any],
    *,
    filt: str,
    asset: str,
    category: str,
    direction: str,
    confidence: float,
    metadata: Dict[str, Any],
    current_price: float,
) -> Optional[Dict[str, Any]]:
    if not _live_signal_matches_filter(direction, confidence, filt):
        return None
    return {
        **payload,
        "asset": asset,
        "signal": direction,
        "direction": direction,
        "category": category,
        "confidence": confidence,
        "current_price": current_price,
        "market_open": is_market_open_for_asset(asset)[0],
        "metadata": metadata,
        **_extract_execution_feedback_fields(metadata),
        **_extract_memory_fields(metadata),
        **_extract_opportunity_fields(metadata),
        **_extract_signal_intelligence_fields(metadata),
        **_extract_market_data_provenance_fields(metadata, asset=asset, category=category),
    }


def _live_signal_payload_from_position(pos: Dict[str, Any], filt: str) -> Optional[Dict[str, Any]]:
    asset = str(pos.get("asset", "") or "")
    category = str(pos.get("category", "forex") or "forex")
    direction = str(pos.get("direction") or pos.get("signal", "BUY")).upper()
    confidence = float(pos.get("confidence", 0) or 0)
    metadata = dict(pos.get("metadata") or {})
    entry_price = float(pos.get("entry_price", 0) or 0)
    stop_loss = float(pos.get("stop_loss", 0) or 0)
    take_profit = float(pos.get("take_profit", 0) or 0)
    risk_reward = float(pos.get("risk_reward", 0) or 0)
    if not risk_reward and entry_price and stop_loss and take_profit:
        risk_reward = round(abs(take_profit - entry_price) / max(0.0001, abs(entry_price - stop_loss)), 2)
    payload = {
        "entry_price": entry_price,
        "entry": entry_price,
        "stop_loss": stop_loss,
        "sl": stop_loss,
        "take_profit": take_profit,
        "tp": take_profit,
        "risk_reward": risk_reward,
        "rr": risk_reward,
        "position_size": float(pos.get("position_size", 0)),
        "strategy_id": pos.get("strategy_id", ""),
        "pnl": float(pos.get("pnl", 0)),
        "generated_at": str(pos.get("open_time", "") or ""),
        "step_reached": pos.get("step_reached", 0),
    }
    current_price = _live_signal_current_price(asset, category)
    return _live_signal_payload(
        payload,
        filt=filt,
        asset=asset,
        category=category,
        direction=direction,
        confidence=confidence,
        metadata=metadata,
        current_price=current_price,
    )


def _live_signal_payload_from_store_item(item: Dict[str, Any], filt: str) -> Optional[Dict[str, Any]]:
    asset = str(item.get("asset", "") or "")
    category = str(item.get("category", "forex") or "forex")
    direction = str(item.get("signal", "BUY")).upper()
    confidence = float(item.get("confidence", 0) or 0)
    metadata = dict(item.get("metadata") or {})
    entry_price = float(item.get("entry_price", item.get("entry", 0)) or 0)
    stop_loss = float(item.get("stop_loss", item.get("sl", 0)) or 0)
    take_profit = float(item.get("take_profit", item.get("tp", 0)) or 0)
    risk_reward = float(item.get("risk_reward", item.get("rr", 0)) or 0)
    if not risk_reward and entry_price and stop_loss and take_profit:
        risk_reward = round(abs(take_profit - entry_price) / max(0.0001, abs(entry_price - stop_loss)), 2)
    payload = dict(item)
    payload.update({
        "entry_price": entry_price,
        "entry": entry_price,
        "stop_loss": stop_loss,
        "sl": stop_loss,
        "take_profit": take_profit,
        "tp": take_profit,
        "risk_reward": risk_reward,
        "rr": risk_reward,
    })
    current_price = _live_signal_current_price(asset, category)
    return _live_signal_payload(
        payload,
        filt=filt,
        asset=asset,
        category=category,
        direction=direction,
        confidence=confidence,
        metadata=metadata,
        current_price=current_price,
    )


def _collect_live_signals_from_core(core: Any, filt: str) -> List[Dict[str, Any]]:
    signals: List[Dict[str, Any]] = []
    for pos in core.get_positions():
        payload = _live_signal_payload_from_position(pos, filt)
        if payload is not None:
            signals.append(payload)
    return signals


def _collect_live_signals_from_store(filt: str) -> List[Dict[str, Any]]:
    with _sig_lock:
        sigs = list(_sig_store.values())
    active = [s for s in sigs if s.get("signal", "HOLD") not in ("HOLD", "CLOSED")]
    signals: List[Dict[str, Any]] = []
    for item in active:
        payload = _live_signal_payload_from_store_item(item, filt)
        if payload is not None:
            signals.append(payload)
    return signals


def _live_signal_stats(signals: List[Dict[str, Any]]) -> Dict[str, Any]:
    buys = sum(1 for s in signals if s.get("signal") == "BUY")
    sells = sum(1 for s in signals if s.get("signal") == "SELL")
    avg_conf = sum(float(s.get("confidence", 0) or 0) for s in signals) / max(1, len(signals))
    return {
        "total_signals": len(signals),
        "buy_signals": buys,
        "sell_signals": sells,
        "avg_confidence": round(avg_conf * 100, 1),
    }

@app.route("/api/signals/live")
@_check_api_auth
@_check_rate_limit
def api_signals_live():
    try:
        core   = _core()
        filt   = request.args.get("filter", "all")
        signals = _collect_live_signals_from_core(core, filt) if core else _collect_live_signals_from_store(filt)
        stats = _live_signal_stats(signals)
        return jsonify({
            "success": True,
            "signals": signals,
            **stats,
        })
    except APIError as e:
        return handle_api_error(e, "/api/signals/live", e.status_code)
    except Exception as e:
        return handle_api_error(e, "/api/signals/live", 500)

# ══════════════════════════════════════════════════════════════════════════════
# API — MARKET INTELLIGENCE (Chart + Heatmap + Correlation)
# ══════════════════════════════════════════════════════════════════════════════

def _chart_asset_descriptor(asset: str, category: str) -> Dict[str, Any]:
    normalized_asset = str(asset or "").strip()
    normalized_category = str(category or "").strip().lower()
    primary_provider = "Deriv"
    secondary_provider = ""
    quote_mode = "stream"
    routing_label = "deriv_primary"

    try:
        from services.market_data_router import is_ig_primary_asset

        ig_primary = bool(is_ig_primary_asset(normalized_asset, normalized_category))
    except Exception:
        ig_primary = False

    if ig_primary:
        primary_provider = "IG"
        secondary_provider = "Deriv"
        quote_mode = "stream"
        routing_label = "ig_primary_stream_deriv_fallback"
    else:
        try:
            from services.market_data_router import preferred_quote_provider_order

            provider_order = tuple(
                str(token or "").strip().lower()
                for token in preferred_quote_provider_order(normalized_asset, normalized_category)
                if str(token or "").strip()
            )
            label_map = {
                "ig": "IG",
                "deriv": "Deriv",
                "binance": "Binance",
            }
            if provider_order:
                primary_provider = label_map.get(provider_order[0], provider_order[0].title())
                secondary_provider = (
                    label_map.get(provider_order[1], provider_order[1].title())
                    if len(provider_order) > 1
                    else ""
                )
                if primary_provider == "Binance":
                    routing_label = "binance_primary"
                elif secondary_provider == "Binance":
                    routing_label = "deriv_primary_binance_fallback"
        except Exception:
            pass

    price_precision = _chart_price_precision(normalized_asset, normalized_category)

    return {
        "symbol": normalized_asset,
        "category": normalized_category,
        "primary_provider": primary_provider,
        "secondary_provider": secondary_provider,
        "quote_mode": quote_mode,
        "routing_label": routing_label,
        "price_precision": price_precision,
    }


def _chart_stream_live_quote(asset: str, category: str, *, allow_live_cache: bool = True) -> tuple[Optional[float], str]:
    if allow_live_cache:
        try:
            from websocket_dashboard import get_live_price_snapshot

            live_snapshot = get_live_price_snapshot(asset, max_age_seconds=15.0)
            if live_snapshot is not None:
                live_source = str(live_snapshot.get("source") or "LiveCache")
                if _chart_live_source_allowed(asset, category, live_source):
                    return (
                        float(live_snapshot.get("price", 0.0) or 0.0),
                        live_source,
                    )
        except Exception:
            pass
        try:
            from websocket_dashboard import get_live_price

            live_price, live_source = get_live_price(asset, max_age_seconds=15.0)
            if live_price is not None and _chart_live_source_allowed(asset, category, live_source):
                return float(live_price), str(live_source or "LiveCache")
        except Exception:
            pass

    cache_key = f"chart_quote:{asset}"
    last_good_key = f"chart_quote_last_good:{asset}"
    cached_quote = _cache_get(cache_key)
    if isinstance(cached_quote, dict) and cached_quote.get("price") is not None:
        return float(cached_quote.get("price") or 0.0), str(cached_quote.get("source") or "")

    descriptor = _chart_asset_descriptor(asset, category)
    primary_provider = str(descriptor.get("primary_provider") or "").strip()
    secondary_provider = str(descriptor.get("secondary_provider") or "").strip()
    quote_mode = str(descriptor.get("quote_mode") or "").strip().lower()

    provider_order: list[str] = []
    if primary_provider.upper() == "IG" and secondary_provider:
        provider_order = [secondary_provider, primary_provider]
    else:
        provider_order = [primary_provider, secondary_provider]

    seen: set[str] = set()
    for provider in provider_order:
        token = str(provider or "").strip()
        if not token:
            continue
        normalized = token.lower()
        if normalized in seen:
            continue
        seen.add(normalized)
        try:
            price, _spread, provider_meta = _fetcher.get_provider_quote(asset, category, token)
        except Exception:
            price, provider_meta = None, {}
        if price is not None:
            source = str((provider_meta or {}).get("source") or token)
            payload = {"price": float(price), "source": source}
            ttl = 5 if token.upper() == "IG" else 2 if quote_mode == "stream" else 3
            _cache_set(cache_key, payload, ttl=ttl)
            _cache_set(last_good_key, payload, ttl=30)
            return float(price), source

    price, _ = _fetcher.get_real_time_price(asset, category)
    if price is None:
        last_good = _cache_get(last_good_key)
        if isinstance(last_good, dict) and last_good.get("price") is not None:
            return float(last_good.get("price") or 0.0), str(last_good.get("source") or "")
        return None, ""
    try:
        source = str((_fetcher.get_last_price_metadata(asset) or {}).get("source") or "")
    except Exception:
        source = ""
    payload = {"price": float(price), "source": source}
    _cache_set(cache_key, payload, ttl=3)
    _cache_set(last_good_key, payload, ttl=30)
    return float(price), source


def _chart_price_precision(asset: str, category: str) -> int:
    symbol = str(asset or "").strip().upper()
    category_key = str(category or "").strip().lower()

    if category_key == "crypto" or any(token in symbol for token in ("BTC", "ETH", "SOL", "XRP", "ADA", "DOGE", "LTC", "BNB")):
        return 8

    if category_key == "forex" or "/" in symbol:
        return 3 if symbol.endswith("JPY") or "/JPY" in symbol else 5

    if category_key == "commodities" or any(token in symbol for token in ("XAU", "XAG", "WTI", "BRENT", "OIL", "GAS")):
        return 3 if any(token in symbol for token in ("XAU", "XAG")) else 2

    if category_key == "indices" or symbol.endswith("=F") or symbol.startswith("^"):
        return 2

    return 5 if "/" in symbol else 2


def _provider_routing_summary() -> Dict[str, Any]:
    assets = [_chart_asset_descriptor(asset, category) for asset, category in ALL_ASSETS]
    primary_counts: Dict[str, int] = {}
    secondary_counts: Dict[str, int] = {}
    quote_mode_counts: Dict[str, int] = {}
    category_primary: Dict[str, str] = {}

    for item in assets:
        primary = str(item.get("primary_provider") or "")
        secondary = str(item.get("secondary_provider") or "")
        quote_mode = str(item.get("quote_mode") or "")
        category = str(item.get("category") or "")
        if primary:
            primary_counts[primary] = primary_counts.get(primary, 0) + 1
            category_primary.setdefault(category, primary)
        if secondary:
            secondary_counts[secondary] = secondary_counts.get(secondary, 0) + 1
        if quote_mode:
            quote_mode_counts[quote_mode] = quote_mode_counts.get(quote_mode, 0) + 1

    primary_summary = ", ".join(
        f"{provider} {count}"
        for provider, count in sorted(primary_counts.items(), key=lambda item: (-item[1], item[0]))
    )
    fallback_summary = ", ".join(
        f"{provider} {count}"
        for provider, count in sorted(secondary_counts.items(), key=lambda item: (-item[1], item[0]))
    )

    return {
        "asset_count": len(assets),
        "primary_counts": primary_counts,
        "secondary_counts": secondary_counts,
        "quote_mode_counts": quote_mode_counts,
        "category_primary": category_primary,
        "summary_label": primary_summary or "Unavailable",
        "fallback_label": fallback_summary or "None",
    }


def _provider_family(value: Any) -> str:
    token = str(value or "").strip().upper()
    if token.startswith("IG"):
        return "IG"
    if token.startswith("DERIV"):
        return "DERIV"
    if token.startswith("BINANCE"):
        return "BINANCE"
    if token.startswith("DUKASCOPY"):
        return "DUKASCOPY"
    if token.startswith("FMP"):
        return "FMP"
    return token


def _chart_live_source_allowed(asset: str, category: str, source: str) -> bool:
    if str(category or "").strip().lower() != "crypto":
        return True
    source_family = _provider_family(source).strip().lower()
    if not source_family:
        return False
    try:
        from services.market_data_router import preferred_quote_provider_order

        provider_order = tuple(
            str(token or "").strip().lower()
            for token in preferred_quote_provider_order(asset, category)
            if str(token or "").strip()
        )
    except Exception:
        provider_order = ()
    if not provider_order:
        return True
    return source_family == provider_order[0]


def _history_allows_live_overlay(descriptor: Dict[str, Any], meta: Dict[str, Any]) -> bool:
    source_class = str((meta or {}).get("source_class") or "").strip().lower()
    if source_class == "stream_cache":
        return True
    primary = _provider_family((descriptor or {}).get("primary_provider") or "")
    if source_class == "local_store":
        backing_families = {
            _provider_family(value)
            for value in list((meta or {}).get("backing_provider_families") or [])
            if str(value or "").strip()
        }
        latest_family = _provider_family((meta or {}).get("latest_provider_family") or "")
        latest_source_class = str((meta or {}).get("latest_source_class") or "").strip().lower()
        provider_family = _provider_family((meta or {}).get("provider_family") or "")
        if primary and latest_family == primary and latest_source_class == "stream_cache":
            return True
        if primary and (provider_family == primary or latest_family == primary or primary in backing_families):
            return True
    history = _provider_family((meta or {}).get("provider_family") or (meta or {}).get("source") or "")
    return bool(primary and history and primary == history)


def _chart_history_provider_preference(asset: str, category: str, descriptor: Dict[str, Any]) -> Tuple[str, ...]:
    primary = str((descriptor or {}).get("primary_provider") or "").strip()
    secondary = str((descriptor or {}).get("secondary_provider") or "").strip()
    category_key = str(category or "").strip().lower()

    ordered: list[str] = []

    def _add(token: str) -> None:
        raw = str(token or "").strip()
        if raw and raw not in ordered:
            ordered.append(raw)

    _add(primary)
    _add(secondary)

    if category_key == "crypto":
        _add("Binance")
        _add("Deriv")
    else:
        _add("Deriv")

    _add("Dukascopy")
    _add("FMP")
    return tuple(ordered)


def _chart_period_limit(asset: str, category: str, interval: str, requested: int) -> int:
    descriptor = _chart_asset_descriptor(asset, category)
    if str(descriptor.get("primary_provider") or "").upper() != "IG":
        return int(requested)

    capped = {
        "1m": 180,
        "5m": 240,
        "15m": 240,
        "30m": 240,
        "1h": 300,
        "4h": 300,
        "1d": 365,
    }.get(str(interval or "").lower(), min(int(requested), 240))
    return max(60, min(int(requested), int(capped)))


def _chart_history_cache_ttl(asset: str, category: str, interval: str) -> int:
    interval_key = str(interval or "").lower()
    if interval_key == "1m":
        return 10
    if interval_key in {"5m", "15m", "30m"}:
        return 15
    if interval_key == "1h":
        return 60
    if interval_key in {"4h", "1d"}:
        return 300
    return 60


def _chart_live_period_target(interval: str) -> int:
    return {
        "1m": 300,
        "5m": 300,
        "15m": 300,
        "30m": 240,
        "1h": 240,
        "4h": 200,
        "1d": 365,
    }.get(str(interval or "").lower(), 300)


def _chart_allowance_retry_limit(asset: str, category: str, interval: str, requested: int) -> int:
    descriptor = _chart_asset_descriptor(asset, category)
    if str(descriptor.get("primary_provider") or "").upper() != "IG":
        return int(requested)

    capped = {
        "1m": 60,
        "5m": 60,
        "15m": 60,
        "30m": 60,
        "1h": 90,
        "4h": 120,
        "1d": 180,
    }.get(str(interval or "").lower(), min(int(requested), 90))
    return max(30, min(int(requested), int(capped)))


def _is_historical_allowance_error(meta: Dict[str, Any]) -> bool:
    code = str(meta.get("provider_error_code") or "").lower()
    message = str(meta.get("provider_error_message") or meta.get("message") or "").lower()
    return "historical-data-allowance" in code or "historical data allowance" in message


def _chart_fetcher_cache_token() -> str:
    return str(id(_fetcher))


def _fetch_ohlcv_with_allowance_retry(
    asset: str,
    category: str,
    interval: str,
    periods: int,
    *,
    closed_only: bool = False,
    prefer_local: bool = True,
    provider_preference: Optional[Tuple[str, ...]] = None,
) -> Tuple[Optional["pd.DataFrame"], Dict[str, Any], int]:
    import pandas as pd

    fetcher = _fetcher
    requested = max(2, int(periods or 0))
    base_kwargs = {
        "interval": interval,
        "periods": requested,
        "closed_only": closed_only,
        "prefer_local": prefer_local,
    }
    if provider_preference:
        base_kwargs["provider_preference"] = provider_preference
    try:
        df = fetcher.get_ohlcv(asset, category, **base_kwargs)
    except TypeError:
        fallback_kwargs = {"interval": interval, "periods": requested}
        if closed_only:
            try:
                df = fetcher.get_ohlcv(asset, category, interval=interval, periods=requested, closed_only=closed_only)
            except TypeError:
                df = fetcher.get_ohlcv(asset, category, **fallback_kwargs)
        else:
            df = fetcher.get_ohlcv(asset, category, **fallback_kwargs)
    meta_fetch = getattr(fetcher, "get_last_ohlcv_metadata", None)
    meta = meta_fetch(asset, interval) if callable(meta_fetch) else {}
    used = requested

    if (df is None or df.empty) and _is_historical_allowance_error(meta):
        retry_periods = _chart_allowance_retry_limit(asset, category, interval, requested)
        if retry_periods < requested:
            try:
                retry_df = fetcher.get_ohlcv(
                    asset,
                    category,
                    interval=interval,
                    periods=retry_periods,
                    closed_only=closed_only,
                    prefer_local=prefer_local,
                    provider_preference=provider_preference,
                )
            except TypeError:
                retry_df = fetcher.get_ohlcv(
                    asset,
                    category,
                    interval=interval,
                    periods=retry_periods,
                )
            retry_meta = meta_fetch(asset, interval) if callable(meta_fetch) else {}
            if retry_df is not None and not retry_df.empty:
                return retry_df, retry_meta, retry_periods
            meta = retry_meta
            used = retry_periods

    if isinstance(df, pd.DataFrame):
        df = df.copy()
    return df, meta, used


def _heatmap_reference_from_frame(
    frame: Any,
    current_price: Optional[float],
    reference_price: Optional[float],
    source: str,
    *,
    lookback_hours: Optional[int] = None,
    allow_open_fallback: bool = False,
) -> Tuple[Optional[float], Optional[float], str]:
    import pandas as pd

    if frame is None or frame.empty or "close" not in frame.columns:
        return current_price, reference_price, source

    closes = frame["close"].astype(float).dropna()
    if closes.empty:
        return current_price, reference_price, source

    if current_price is None:
        current_price = float(closes.iloc[-1])

    if len(closes) >= 2:
        if lookback_hours is None:
            reference_price = float(closes.iloc[-2])
        else:
            target_ts = closes.index.max() - pd.Timedelta(hours=lookback_hours)
            history = closes[closes.index <= target_ts]
            reference_price = float(history.iloc[-1] if not history.empty else closes.iloc[0])
        return current_price, reference_price, source

    if allow_open_fallback and "open" in frame.columns:
        opens = frame["open"].astype(float).dropna()
        if not opens.empty:
            reference_price = float(opens.iloc[-1])
    return current_price, reference_price, source


def _heatmap_item(asset: str, category: str) -> Optional[Dict[str, Any]]:
    if _is_market_weekend(category):
        return None

    fetcher = _get_fetcher()
    descriptor = _chart_asset_descriptor(asset, category)
    ig_primary = str(descriptor.get("primary_provider") or "").upper() == "IG"
    live_price, _ = fetcher.get_real_time_price(asset, category)
    price_meta = fetcher.get_last_price_metadata(asset)

    reference_price: Optional[float] = None
    current_price: Optional[float] = float(live_price) if live_price is not None else None
    source = str((price_meta or {}).get("source") or "")

    if reference_price is None or current_price is None:
        df_daily, daily_meta, _ = _fetch_ohlcv_with_allowance_retry(asset, category, "1d", 2)
        current_price, reference_price, source = _heatmap_reference_from_frame(
            df_daily,
            current_price,
            reference_price,
            source,
            allow_open_fallback=True,
        )
        source = str((daily_meta or {}).get("source") or source or "")

    if reference_price is None or current_price is None:
        df_intraday, intraday_meta, _ = _fetch_ohlcv_with_allowance_retry(asset, category, "1h", 30)
        current_price, reference_price, source = _heatmap_reference_from_frame(
            df_intraday,
            current_price,
            reference_price,
            source,
            lookback_hours=24,
        )
        source = str((intraday_meta or {}).get("source") or source or "")

    if ig_primary and (reference_price is None or current_price is None):
        stream_df = _stream_candles_from_live_feed(asset, "5m", 288, source_hint="IG")
        current_price, reference_price, source = _heatmap_reference_from_frame(
            stream_df,
            current_price,
            reference_price,
            source or "IG Stream",
            lookback_hours=24,
        )

    if current_price is None:
        return None

    change_pct = None
    if reference_price is not None and float(reference_price) > 0:
        change_pct = round((float(current_price) - float(reference_price)) / float(reference_price) * 100.0, 3)
    return {
        "asset": asset,
        "category": category,
        "change_pct": change_pct,
        "price": round(float(current_price), 5),
        "source": source or "unknown",
    }


def _normalize_correlation_series(series: "pd.Series", interval: str) -> "pd.Series":
    import pandas as pd

    ts = pd.to_datetime(series.index, utc=True, errors="coerce")
    normalized = pd.Series(series.astype(float).values, index=ts)
    normalized = normalized[~normalized.index.isna()].sort_index()
    if normalized.empty:
        return normalized

    freq = {"1d": "1D", "4h": "4h", "1h": "1h", "30m": "30min"}.get(str(interval or "").lower())
    if freq:
        normalized = normalized.groupby(normalized.index.floor(freq)).last()
    else:
        normalized = normalized.groupby(normalized.index).last()
    return normalized.dropna()


def _stream_candles_from_live_feed(
    asset: str,
    interval: str,
    periods: int,
    *,
    source_hint: str = "IG",
) -> Optional["pd.DataFrame"]:
    import pandas as pd

    bucket_seconds = _stream_bucket_seconds(interval)
    if not bucket_seconds:
        return None

    samples = _stream_live_feed_samples(asset, periods, source_hint)
    if not samples:
        samples = _stream_fallback_feed_samples(asset, periods, source_hint)

    if len(samples) < 2:
        return None

    rows = _stream_candle_rows(samples, bucket_seconds)
    if not rows:
        return None

    frame = pd.DataFrame(rows).set_index("timestamp").sort_index()
    return frame.tail(int(max(2, periods)))


def _stream_bucket_seconds(interval: str) -> Optional[int]:
    return {
        "1m": 60,
        "5m": 300,
        "15m": 900,
        "30m": 1800,
        "1h": 3600,
        "4h": 14400,
        "1d": 86400,
    }.get(str(interval or "").lower())


def _stream_live_feed_samples(asset: str, periods: int, source_hint: str) -> List[Tuple[float, float]]:
    samples: List[Tuple[float, float]] = []
    try:
        from websocket_dashboard import get_live_price_history

        for item in get_live_price_history(asset, limit=max(200, int(periods) * 20)):
            if source_hint and str(item.get("source") or "").upper() != str(source_hint).upper():
                continue
            price = float(item.get("price"))
            ts = float(item.get("timestamp"))
            samples.append((ts, price))
    except Exception:
        pass
    return samples

def _stream_fallback_feed_samples(asset: str, periods: int, source_hint: str) -> List[Tuple[float, float]]:
    samples: List[Tuple[float, float]] = []
    try:
        feed_rows = get_feed(source_filter=str(source_hint or "").lower() or None, limit=max(500, int(periods) * 30))
    except Exception:
        feed_rows = []
    for row in feed_rows:
        if str(row.get("symbol") or "") != asset:
            continue
        try:
            samples.append((float(row.get("timestamp")), float(row.get("price_raw"))))
        except Exception:
            continue
    return samples

def _stream_candle_rows(samples: List[Tuple[float, float]], bucket_seconds: int) -> List[Dict[str, float]]:
    import pandas as pd

    samples.sort(key=lambda item: item[0])
    rows: List[Dict[str, float]] = []
    current_bucket: Optional[int] = None
    bucket_prices: List[float] = []
    bucket_ts: Optional[pd.Timestamp] = None

    for ts, price in samples:
        bucket = int(ts // bucket_seconds) * bucket_seconds
        if current_bucket is None:
            current_bucket = bucket
            bucket_ts = pd.to_datetime(bucket, unit="s", utc=True)
        if bucket != current_bucket:
            if bucket_prices and bucket_ts is not None:
                rows.append(
                    {
                        "timestamp": bucket_ts,
                        "open": float(bucket_prices[0]),
                        "high": float(max(bucket_prices)),
                        "low": float(min(bucket_prices)),
                        "close": float(bucket_prices[-1]),
                        "volume": float(len(bucket_prices)),
                    }
                )
            current_bucket = bucket
            bucket_ts = pd.to_datetime(bucket, unit="s", utc=True)
            bucket_prices = []
        bucket_prices.append(float(price))

    if bucket_prices and bucket_ts is not None:
        rows.append(
            {
                "timestamp": bucket_ts,
                "open": float(bucket_prices[0]),
                "high": float(max(bucket_prices)),
                "low": float(min(bucket_prices)),
                "close": float(bucket_prices[-1]),
                "volume": float(len(bucket_prices)),
            }
        )

    return rows


def _validated_correlation_cache(cached: Any) -> Optional[Dict[str, Any]]:
    if cached is None:
        return None
    try:
        import math

        labels = cached.get("labels") or []
        matrix = cached.get("matrix") or []
        invalid = any(
            isinstance(value, (int, float)) and not math.isfinite(value)
            for row in matrix
            for value in row
        )
        if labels and matrix and not invalid:
            return cached
    except Exception:
        return None
    return None


def _fetch_correlation_close_series(asset: str, interval: str, periods: int) -> Tuple[str, Any]:
    cat = _cat(asset)
    try:
        df, _, _ = _fetch_ohlcv_with_allowance_retry(asset, cat, interval, periods)
        descriptor = _chart_asset_descriptor(asset, cat)
        ig_primary = str(descriptor.get("primary_provider") or "").upper() == "IG"
        if ig_primary and (df is None or df.empty):
            stream_df = _stream_candles_from_live_feed(asset, interval, periods, source_hint="IG")
            if stream_df is not None and not stream_df.empty:
                df = stream_df
        if df is None or df.empty or "close" not in df.columns:
            return asset, None
        normalized = _normalize_correlation_series(df["close"].astype(float), interval)
        if normalized.empty:
            return asset, None
        return asset, normalized.tail(periods)
    except Exception:
        return asset, None


def _fetch_correlation_closes(assets: List[str], interval: str, periods: int) -> Dict[str, Any]:
    from concurrent.futures import ThreadPoolExecutor, wait

    closes: Dict[str, Any] = {}
    pool = ThreadPoolExecutor(max_workers=max(1, int(DASHBOARD_CORRELATION_WORKERS or 8)))
    try:
        futures = {
            pool.submit(_fetch_correlation_close_series, asset, interval, periods): asset
            for asset in assets
        }
        done, not_done = wait(futures, timeout=30)
        for future in done:
            try:
                asset, series = future.result()
                if series is not None:
                    closes[asset] = series
            except Exception:
                pass
        if not_done:
            logger.warning(
                f"[Correlation] timeout fetching {len(not_done)} assets for {interval}: "
                f"{[futures[f] for f in not_done]}"
            )
            for fut in not_done:
                fut.cancel()
    finally:
        try:
            pool.shutdown(wait=False, cancel_futures=True)
        except Exception:
            pass
    return closes


def _build_correlation_candidate(
    assets: List[str],
    interval: str,
    min_points: int,
    closes: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    import pandas as pd

    if len(closes) < 2:
        return None

    frame = pd.DataFrame(closes).sort_index()
    returns = frame.pct_change(fill_method=None)
    returns = returns.dropna(axis=1, thresh=min_points)
    if returns.shape[1] < 2:
        return None

    corr = returns.corr(min_periods=min_points).reindex(index=assets, columns=assets)
    if returns.shape[1] > 0:
        for label in returns.columns:
            corr.loc[label, label] = 1.0

    corr = corr.round(3)
    matrix: List[List[Optional[float]]] = []
    for _, row in corr.iterrows():
        matrix.append([
            None if pd.isna(value) else float(value)
            for value in row.tolist()
        ])
    return {
        "success": True,
        "labels": assets,
        "matrix": matrix,
        "interval": interval,
        "expected_assets": len(assets),
        "available_assets": int(returns.shape[1]),
        "partial": int(returns.shape[1]) < len(assets),
    }


def _select_best_correlation_payload(assets: List[str]) -> Optional[Dict[str, Any]]:
    plans = [
        ("1d", 45, 8),
        ("1h", 240, 24),
        ("5m", 288, 24),
    ]
    best_payload: Optional[Dict[str, Any]] = None
    best_available_assets = -1

    for interval, periods, min_points in plans:
        closes = _fetch_correlation_closes(assets, interval, periods)
        candidate = _build_correlation_candidate(assets, interval, min_points, closes)
        if not candidate:
            continue
        available_assets = int(candidate.get("available_assets", 0) or 0)
        if available_assets > best_available_assets:
            best_payload = candidate
            best_available_assets = available_assets
        if available_assets >= len(assets):
            break

    return best_payload


def _parse_chart_history_end_time(value: Any) -> Optional["pd.Timestamp"]:
    import pandas as pd

    if value in (None, ""):
        return None
    raw = str(value).strip()
    if not raw:
        return None
    try:
        if raw.isdigit():
            ts = pd.to_datetime(int(raw), unit="s", utc=True, errors="coerce")
        else:
            ts = pd.to_datetime(raw, utc=True, errors="coerce")
        if pd.isna(ts):
            return None
        return pd.Timestamp(ts)
    except Exception:
        return None


def _deep_history_bar_limit(interval: str, requested: int) -> int:
    capped = {
        "1m": 1000,
        "5m": 1000,
        "15m": 1000,
        "30m": 1000,
        "1h": 1000,
        "4h": 1000,
        "1d": 1500,
    }.get(str(interval or "").lower(), 1000)
    return max(50, min(int(requested), int(capped)))


def _serialize_chart_candles(df: "pd.DataFrame") -> List[Dict[str, Any]]:
    import pandas as pd

    frame = df.copy()
    frame.columns = [str(c).lower() for c in frame.columns]
    timestamps = []
    for idx_val in frame.index:
        try:
            ts = pd.Timestamp(idx_val)
            if ts.tzinfo is not None:
                ts = ts.tz_convert("UTC").tz_localize(None)
            timestamps.append(int(ts.timestamp()))
        except Exception:
            timestamps.append(0)

    seen: set[int] = set()
    candles: List[Dict[str, Any]] = []
    for t, (_, row) in zip(timestamps, frame.iterrows()):
        if t == 0 or t in seen:
            continue
        seen.add(t)
        candles.append(
            {
                "time": t,
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "volume": float(row.get("volume", 0) or 0),
            }
        )
    candles.sort(key=lambda item: item["time"])
    return candles


def _chart_candle_apply_stream_overlay(
    asset: str,
    interval: str,
    periods: int,
    df: Any,
    meta: Dict[str, Any],
    *,
    primary_provider: str,
) -> Tuple[Any, str, Dict[str, Any]]:
    if df is not None and not df.empty:
        return df, interval, meta

    for source_hint in (str(primary_provider or "").strip(), ""):
        stream_df = _stream_candles_from_live_feed(asset, interval, periods, source_hint=source_hint)
        if stream_df is None or stream_df.empty:
            continue
        source_label = f"{source_hint} Stream" if source_hint else "Live Stream"
        return stream_df, interval, {
            "source": source_label,
            "source_class": "stream_cache",
            "provider_family": str(primary_provider or source_hint or ""),
            "provider_error_message": str(meta.get("provider_error_message") or ""),
            "provider_error_code": str(meta.get("provider_error_code") or ""),
        }

    return df, interval, meta


def _chart_candle_apply_local_store_fallback(
    asset: str,
    category: str,
    interval: str,
    periods: int,
    df: Any,
    meta: Dict[str, Any],
) -> Tuple[Any, Dict[str, Any]]:
    if df is not None and not df.empty:
        return df, meta

    try:
        from services.local_candle_store import local_candle_store
    except Exception:
        local_candle_store = None

    if local_candle_store is None or not callable(getattr(local_candle_store, "get_ohlcv", None)):
        return df, meta

    try:
        local_df, local_meta = local_candle_store.get_ohlcv(
            asset,
            category,
            interval,
            periods,
            closed_only=False,
        )
    except Exception:
        return df, meta

    if local_df is None or local_df.empty:
        return df, meta

    merged_meta = dict(meta or {})
    merged_meta.update(local_meta or {})
    return local_df, merged_meta


def _chart_candle_apply_category_fallbacks(
    asset: str,
    category: str,
    interval: str,
    periods: int,
    df: Any,
    used: str,
    *,
    provider_preference: Optional[Tuple[str, ...]] = None,
) -> Tuple[Any, str, int]:
    if df is not None and not df.empty:
        return df, used, periods

    if category not in {"forex", "indices"}:
        return df, used, periods

    fallbacks = {
        "1m": ["5m", "15m", "30m", "1h", "4h", "1d"],
        "5m": ["15m", "30m", "1h", "4h", "1d"],
        "15m": ["30m", "1h", "4h", "1d"],
        "30m": ["1h", "4h", "1d"],
        "1h": ["4h", "1d"],
        "4h": ["1d"],
    }
    for fb in fallbacks.get(interval, []):
        fb_requested = min(int(get_chart_timeframe_periods(fb)), _chart_live_period_target(fb))
        fb_periods = _chart_period_limit(asset, category, fb, fb_requested)
        try:
            fallback_df = _fetcher.get_ohlcv(
                asset,
                category,
                interval=fb,
                periods=fb_periods,
                prefer_local=True,
                provider_preference=provider_preference,
            )
        except TypeError:
            fallback_df = _fetcher.get_ohlcv(asset, category, interval=fb, periods=fb_periods)
        if fallback_df is not None and not getattr(fallback_df, "empty", True):
            return fallback_df, fb, fb_periods
    return df, used, periods


def _chart_candle_missing_payload(
    asset: str,
    meta: Dict[str, Any],
    interval: str,
    periods: int,
    last_good_key: str,
) -> Dict[str, Any]:
    last_good_payload = _cache_get(last_good_key)
    if _is_historical_allowance_error(meta) and last_good_payload:
        fallback_payload = dict(last_good_payload)
        fallback_payload["cached"] = True
        fallback_payload["provider_warning_code"] = str(meta.get("provider_error_code") or "")
        fallback_payload["provider_warning_message"] = str(meta.get("provider_error_message") or meta.get("message") or "")
        fallback_payload["message"] = "Using cached candles due to provider historical allowance."
        return fallback_payload

    message = str(meta.get("provider_error_message") or f"No data for {asset}")
    if _is_historical_allowance_error(meta):
        message = "IG historical allowance exceeded for this chart. Try a higher timeframe or wait for allowance reset."
    return {
        "success": True,
        "candles": [],
        "message": message,
        "interval_used": interval,
        "bars_requested": periods,
        "data_source": meta.get("source"),
        "provider_error_code": meta.get("provider_error_code"),
        "provider_error_message": meta.get("provider_error_message"),
    }


def _chart_candle_success_payload(
    *,
    asset: str,
    category: str,
    used: str,
    periods: int,
    candles: List[Dict[str, Any]],
    descriptor: Dict[str, Any],
    meta: Dict[str, Any],
) -> Dict[str, Any]:
    return {
        "success": True,
        "candles": candles,
        "interval_used": used,
        "bars_requested": periods,
        "data_source": meta.get("source"),
        "data_source_class": meta.get("source_class"),
        "provider_family": meta.get("provider_family") or meta.get("source"),
        "live_overlay_allowed": _history_allows_live_overlay(descriptor, meta),
        "live_price_source": descriptor.get("primary_provider"),
        "provider_warning_code": str(meta.get("provider_error_code") or ""),
        "provider_warning_message": str(meta.get("provider_error_message") or ""),
        "cached": False,
    }


@app.route("/api/chart/assets")
@_check_api_auth
@_check_rate_limit
def api_chart_assets():
    try:
        return jsonify(
            {
                "success": True,
                "assets": [_chart_asset_descriptor(a, c) for a, c in ALL_ASSETS],
            }
        )
    except Exception as e:
        return handle_api_error(e, "/api/chart/assets", 500)

@app.route("/api/chart/candles")
@_check_api_auth
@_check_rate_limit
def api_chart_candles():
    try:
        from config.config import get_chart_timeframe_periods
        asset    = request.args.get("asset", "EUR/USD")
        interval = request.args.get("interval", "15m")
        cat      = _cat(asset)
        descriptor = _chart_asset_descriptor(asset, cat)
        provider_preference = _chart_history_provider_preference(asset, cat, descriptor)
        requested_periods = min(int(get_chart_timeframe_periods(interval)), _chart_live_period_target(interval))
        periods = _chart_period_limit(asset, cat, interval, requested_periods)
        fetcher_token = _chart_fetcher_cache_token()
        cache_key = f"chart_candles:{asset}:{cat}:{interval}:{periods}:{fetcher_token}"
        last_good_key = f"chart_candles_last:{asset}:{cat}:{interval}:{fetcher_token}"
        cached_payload = _cache_get(cache_key)
        if cached_payload:
            return jsonify(cached_payload)

        df, meta, periods = _fetch_ohlcv_with_allowance_retry(
            asset,
            cat,
            interval,
            periods,
            prefer_local=True,
            provider_preference=provider_preference,
        )
        used = interval
        df, meta = _chart_candle_apply_local_store_fallback(asset, cat, interval, periods, df, meta)
        df, used, overlay_meta = _chart_candle_apply_stream_overlay(
            asset,
            interval,
            periods,
            df,
            meta,
            primary_provider=str(descriptor.get("primary_provider") or ""),
        )
        if overlay_meta is not meta:
            meta = overlay_meta
        df, used, periods = _chart_candle_apply_category_fallbacks(
            asset,
            cat,
            interval,
            periods,
            df,
            used,
            provider_preference=provider_preference,
        )
        if df is None or df.empty:
            return jsonify(_chart_candle_missing_payload(asset, meta, interval, periods, last_good_key))

        candles = _serialize_chart_candles(df)
        if str(meta.get("source_class") or "") not in {"stream_cache", "local_store"}:
            meta = {}
            try:
                meta = _fetcher.get_last_ohlcv_metadata(asset, used)
            except Exception:
                meta = {}
        payload = _chart_candle_success_payload(
            asset=asset,
            category=cat,
            used=used,
            periods=periods,
            candles=candles,
            descriptor=descriptor,
            meta=meta,
        )
        _cache_set(cache_key, payload, ttl=_chart_history_cache_ttl(asset, cat, used))
        _cache_set(last_good_key, payload, ttl=max(_chart_history_cache_ttl(asset, cat, used), 3600))
        return jsonify(payload)
    except APIError as e:
        return handle_api_error(e, "/api/chart/candles", e.status_code)
    except Exception as e:
        logger.error(f"[candles] {e}")
        return handle_api_error(e, "/api/chart/candles", 500)


@app.route("/api/chart/history")
@_check_api_auth
@_check_rate_limit
def api_chart_history():
    try:
        asset = request.args.get("asset", "EUR/USD")
        interval = request.args.get("interval", "1h")
        cat = _cat(asset)
        descriptor = _chart_asset_descriptor(asset, cat)
        provider_preference = _chart_history_provider_preference(asset, cat, descriptor)
        requested = _deep_history_bar_limit(interval, int(request.args.get("bars", "500")))
        end_time = _parse_chart_history_end_time(request.args.get("end_time"))
        closed_only = end_time is not None
        end_key = end_time.isoformat() if end_time is not None else "latest"
        cache_key = f"chart_history:{asset}:{cat}:{interval}:{requested}:{end_key}"
        cached_payload = _cache_get(cache_key)
        if cached_payload:
            return jsonify(cached_payload)

        try:
            df = _fetcher.get_ohlcv(
                asset,
                cat,
                interval=interval,
                periods=requested,
                end_time=end_time,
                closed_only=closed_only,
                prefer_local=True,
                provider_preference=provider_preference,
            )
        except TypeError:
            df = _fetcher.get_ohlcv(
                asset,
                cat,
                interval=interval,
                periods=requested,
                end_time=end_time,
                closed_only=closed_only,
            )
        meta = {}
        try:
            meta = _fetcher.get_last_ohlcv_metadata(asset, interval)
        except Exception:
            meta = {}

        if df is None or df.empty:
            return jsonify(
                {
                    "success": True,
                    "candles": [],
                    "message": str(meta.get("provider_error_message") or f"No history for {asset}"),
                    "interval_used": interval,
                    "bars_requested": requested,
                    "bars_returned": 0,
                    "data_source": meta.get("source"),
                    "data_source_class": meta.get("source_class"),
                    "requested_end_time": end_time.isoformat() if end_time is not None else "",
                    "history_mode": "deep",
                    "has_more": False,
                }
            )

        candles = _serialize_chart_candles(df)
        oldest_time = candles[0]["time"] if candles else None
        newest_time = candles[-1]["time"] if candles else None
        payload = {
            "success": True,
            "candles": candles,
            "interval_used": interval,
            "bars_requested": requested,
            "bars_returned": len(candles),
            "data_source": meta.get("source"),
            "data_source_class": meta.get("source_class"),
            "provider_family": meta.get("provider_family") or meta.get("source"),
            "requested_end_time": end_time.isoformat() if end_time is not None else "",
            "history_mode": "deep",
            "oldest_time": oldest_time,
            "newest_time": newest_time,
            "next_end_time": (int(oldest_time) - 1) if oldest_time is not None else None,
            "has_more": bool(oldest_time is not None and len(candles) >= requested),
        }
        _cache_set(cache_key, payload, ttl=900)
        return jsonify(payload)
    except APIError as e:
        return handle_api_error(e, "/api/chart/history", e.status_code)
    except Exception as e:
        logger.error(f"[chart-history] {e}")
        return handle_api_error(e, "/api/chart/history", 500)


@app.route("/api/chart/stream")
@_check_api_auth
@_check_rate_limit
def api_chart_stream():
    asset = request.args.get("asset", "EUR/USD")
    cat   = _cat(asset)
    def _gen():
        # Emit an immediate event so the SSE connection establishes cleanly even
        # if the next live quote takes a few seconds or the market is closed.
        yield f"data: {json.dumps({'type': 'connected', 'asset': asset, 'ts': int(time.time())})}\n\n"
        try:
            price, source = _chart_stream_live_quote(asset, cat)
            if price:
                yield f"data: {json.dumps({'type': 'tick', 'price': price, 'asset': asset, 'source': source, 'ts': int(time.time())})}\n\n"
            else:
                yield f"data: {json.dumps({'type': 'heartbeat', 'asset': asset, 'ts': int(time.time())})}\n\n"
        except Exception:
            yield f"data: {json.dumps({'type': 'heartbeat', 'asset': asset, 'ts': int(time.time())})}\n\n"
    return Response(stream_with_context(_gen()), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.route("/api/chart/quote")
@_check_api_auth
@_check_rate_limit
def api_chart_quote():
    asset = request.args.get("asset", "EUR/USD")
    cat = _cat(asset)
    try:
        price, source = _chart_stream_live_quote(asset, cat)
        ts = int(time.time())
        payload = {
            "success": True,
            "asset": asset,
            "price": float(price) if price is not None else None,
            "source": str(source or ""),
            "ts": ts,
        }
        response = jsonify(payload)
        response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        return response
    except APIError as e:
        return handle_api_error(e, "/api/chart/quote", e.status_code)
    except Exception as e:
        return handle_api_error(e, "/api/chart/quote", 500)


@app.route("/api/live-prices/stream")
@_check_api_auth
@_check_rate_limit
def api_live_prices_stream():
    raw_assets = str(request.args.get("assets", "") or "").strip()
    assets = [str(item or "").strip() for item in raw_assets.split(",") if str(item or "").strip()]
    if not assets:
        return handle_api_error(BadRequest("assets query required"), "/api/live-prices/stream", 400)

    def _snapshot() -> Dict[str, Any]:
        try:
            from websocket_dashboard import get_live_price_snapshots

            return get_live_price_snapshots(assets, max_age_seconds=30.0) or {}
        except Exception:
            return {}

    def _signature(snapshot: Dict[str, Any]) -> str:
        normalized = {
            asset: {
                "price": round(float((snapshot.get(asset) or {}).get("price", 0.0) or 0.0), 8),
                "timestamp": round(float((snapshot.get(asset) or {}).get("timestamp", 0.0) or 0.0), 3),
                "source": str((snapshot.get(asset) or {}).get("source", "") or ""),
            }
            for asset in assets
        }
        return hashlib.sha256(
            json.dumps(normalized, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
        ).hexdigest()

    def _gen():
        last_signature = ""
        last_emit = 0.0
        yield f"data: {json.dumps({'type': 'connected', 'assets': assets, 'ts': int(time.time())})}\n\n"
        while True:
            try:
                snapshot = _snapshot()
                signature = _signature(snapshot)
                now = time.time()
                if signature != last_signature:
                    last_signature = signature
                    last_emit = now
                    yield f"data: {json.dumps({'type': 'prices', 'prices': snapshot, 'ts': int(now)})}\n\n"
                elif now - last_emit >= 5.0:
                    last_emit = now
                    yield f"data: {json.dumps({'type': 'heartbeat', 'ts': int(now)})}\n\n"
                time.sleep(0.25)
            except GeneratorExit:
                raise
            except Exception as exc:
                logger.debug(f"[dashboard] live-prices stream error: {exc}")
                yield f"data: {json.dumps({'type': 'heartbeat', 'ts': int(time.time())})}\n\n"
                time.sleep(1.0)

    return Response(
        stream_with_context(_gen()),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )

@app.route("/api/market/heatmap")
@_check_api_auth
@_check_rate_limit
def api_market_heatmap():
    cache_key = "heatmap:v3"
    cached = _cache_get(cache_key)
    if cached is not None:
        return jsonify(cached)
    try:
        from concurrent.futures import ThreadPoolExecutor, wait

        def _fetch_one(ac):
            asset, cat = ac
            try:
                return _heatmap_item(asset, cat)
            except Exception as _he:
                logger.debug(f"[Heatmap] {asset}: {_he}")
                return None

        results = []
        expected_assets = sum(1 for _, cat in ALL_ASSETS if not _is_market_weekend(cat))
        max_workers = min(max(1, int(DASHBOARD_HEATMAP_WORKERS or 10)), len(ALL_ASSETS))
        pool = ThreadPoolExecutor(max_workers=max_workers)
        try:
            futures = {pool.submit(_fetch_one, ac): ac for ac in ALL_ASSETS}
            done, not_done = wait(futures, timeout=25)
            for r in done:
                try:
                    v = r.result()
                    if v: results.append(v)
                except Exception:
                    pass
            if not_done:
                logger.warning(f"[Heatmap] timeout fetching {len(not_done)} assets: {[futures[f] for f in not_done]}")
                for fut in not_done:
                    fut.cancel()
        finally:
            try:
                pool.shutdown(wait=False, cancel_futures=True)
            except Exception:
                pass

        results.sort(key=lambda x: float(x.get("change_pct")) if x.get("change_pct") is not None else float("-inf"), reverse=True)
        payload = {
            "success": True,
            "items": results,
            "expected_assets": expected_assets,
            "partial": len(results) < expected_assets,
        }
        _cache_set(cache_key, payload, ttl=15)
        return jsonify(payload)
    except APIError as e:
        return handle_api_error(e, "/api/market/heatmap", e.status_code)
    except Exception as e:
        return handle_api_error(e, "/api/market/heatmap", 500)

@app.route("/api/correlation-matrix")
def api_correlation_matrix():
    cached = _validated_correlation_cache(_cache_get("correlation"))
    if cached is not None:
        return jsonify(cached)
    try:
        assets = [a for a, _ in ALL_ASSETS]
        payload = _select_best_correlation_payload(assets)
        if not payload:
            return jsonify({"success": False, "error": "Not enough price data — try again in 30s"})
        _cache_set("correlation", payload, ttl=600)
        return jsonify(payload)
    except APIError as e:
        return handle_api_error(e, "/api/correlation-matrix", e.status_code)
    except Exception as e:
        return handle_api_error(e, "/api/correlation-matrix", 500)

# ══════════════════════════════════════════════════════════════════════════════
# API — AI PREDICTIONS
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/api/accuracy")
@_check_api_auth
@_check_rate_limit
def api_accuracy():
    try:
        days = min(int(request.args.get("days", 30)), 90)
        if _pred_tracker:
            return jsonify({"success": True, "data": _pred_tracker.get_accuracy_stats(days_back=days)})
        return jsonify({"success": False, "data": {
            "by_horizon": {
                "1H":  {"total": 0, "correct": 0, "accuracy_pct": 0, "avg_move_pct": 0},
                "4H":  {"total": 0, "correct": 0, "accuracy_pct": 0, "avg_move_pct": 0},
                "24H": {"total": 0, "correct": 0, "accuracy_pct": 0, "avg_move_pct": 0},
            },
            "by_asset": {}, "recent": [], "days_back": days,
        }})
    except APIError as e:
        return handle_api_error(e, "/api/accuracy", e.status_code)
    except Exception as e:
        return handle_api_error(e, "/api/accuracy", 500)

@app.route("/api/predictions/summary")
@_check_api_auth
@_check_rate_limit
def api_predictions_summary():
    try:
        stats = _pred_tracker.get_accuracy_stats(days_back=30) if _pred_tracker else {}
        core  = _core()
        preds = []

        # Use real positions as predictions — they are active decision-engine signals
        if core:
            for p in core.get_positions():
                d  = (p.get("direction") or p.get("signal", "BUY")).upper()
                e  = float(p.get("entry_price", 0) or 0)
                sl = float(p.get("stop_loss", 0) or 0)
                tp = float(p.get("take_profit", 0) or 0)
                rr = round(abs(tp - e) / max(0.0001, abs(e - sl)), 2) if sl and tp and e else 0
                preds.append({
                    "asset":      p.get("asset", ""),
                    "direction":  d,
                    "confidence": round(float(p.get("confidence", 0)) * 100, 1),
                    "entry": e, "tp": tp, "sl": sl, "rr": rr,
                    "category":  p.get("category", ""),
                    "strategy":  p.get("strategy_id", ""),
                    "timestamp": str(p.get("open_time", ""))[:16],
                })
        else:
            with _sig_lock:
                sigs = list(_sig_store.values())
            for s in sigs:
                d = s.get("signal", s.get("direction", "HOLD"))
                if d in ("HOLD", "CLOSED"): continue
                e  = float(s.get("entry_price", 0) or 0)
                sl = float(s.get("stop_loss", 0) or 0)
                tp = float(s.get("take_profit", 0) or 0)
                rr = round(abs(tp - e) / max(0.0001, abs(e - sl)), 2) if sl and tp and e else 0
                preds.append({
                    "asset":      s.get("asset", ""),
                    "direction":  d,
                    "confidence": round(float(s.get("confidence", 0)) * 100, 1),
                    "entry": e, "tp": tp, "sl": sl, "rr": rr,
                    "category":  s.get("category", ""),
                    "strategy":  s.get("strategy_id", ""),
                    "timestamp": s.get("timestamp", ""),
                })
        return jsonify({"success": True, "predictions": preds, "accuracy": stats})
    except APIError as e:
        return handle_api_error(e, "/api/predictions/summary", e.status_code)
    except Exception as e:
        return handle_api_error(e, "/api/predictions/summary", 500)

@app.route("/api/playbook-intel/overview")
@app.route("/api/ai-predictions/overview")
@_check_api_auth
@_check_rate_limit
def api_ai_predictions_overview():
    days = min(int(request.args.get("days", 30)), 90)
    cache_key = f"ai_predictions_overview:{days}"
    cached = _cache_get(cache_key)
    if cached:
        return jsonify(cached)

    default_accuracy = {
        "by_horizon": {
            "1H":  {"total": 0, "correct": 0, "accuracy_pct": 0, "avg_move_pct": 0},
            "4H":  {"total": 0, "correct": 0, "accuracy_pct": 0, "avg_move_pct": 0},
            "24H": {"total": 0, "correct": 0, "accuracy_pct": 0, "avg_move_pct": 0},
        },
        "by_asset": {}, "recent": [], "days_back": days,
    }

    accuracy = (
        _run_with_timeout(
            _pred_tracker.get_accuracy_stats,
            days_back=days,
            timeout=4.0,
            default=default_accuracy,
            label="ai accuracy overview",
        )
        if _pred_tracker
        else default_accuracy
    )

    sig_resp = _call_view(api_signals_live)
    sig_data = sig_resp.get_json() if hasattr(sig_resp, 'get_json') else json.loads(sig_resp.get_data(as_text=True))
    journal_resp = _call_view(api_phase7_signal_journal)
    journal_data = journal_resp.get_json() if hasattr(journal_resp, "get_json") else json.loads(journal_resp.get_data(as_text=True))

    signal_list = sig_data.get("signals") if sig_data.get("success") else []
    journals = journal_data.get("journals") if journal_data.get("success") else []
    core = _core()
    closed_trades = []
    if core and hasattr(core, "get_closed_trades"):
        try:
            closed_trades = list(core.get_closed_trades(limit=300) or [])
        except Exception:
            closed_trades = []
    live_quality = {
        "signal_count": len(signal_list),
        "avg_confidence": round(
            sum(float(s.get("confidence", 0.0) or 0.0) for s in signal_list) / len(signal_list) * 100.0,
            1,
        ) if signal_list else 0.0,
        "avg_memory_score": round(
            sum(float(s.get("memory_score", 0.0) or 0.0) for s in signal_list) / len(signal_list),
            1,
        ) if signal_list else 0.0,
        "avg_execution_quality": round(
            sum(float(s.get("execution_quality_score", 0.0) or 0.0) for s in signal_list) / len(signal_list),
            1,
        ) if signal_list else 0.0,
        "avg_opportunity_score": round(
            sum(float(s.get("opportunity_score", 0.0) or 0.0) for s in signal_list) / len(signal_list),
            3,
        ) if signal_list else 0.0,
        "memory_ready_count": sum(1 for s in signal_list if int(s.get("memory_sample_count", 0) or 0) > 0),
        "execution_ready_count": sum(1 for s in signal_list if int(s.get("execution_feedback_sample_count", 0) or 0) > 0),
    }
    live_leaders = {
        "memory": [
            {
                "asset": s.get("asset", ""),
                "direction": s.get("direction", ""),
                "score": round(float(s.get("memory_score", 0.0) or 0.0), 1),
                "samples": int(s.get("memory_sample_count", 0) or 0),
                "subtitle": str(s.get("memory_setup_style") or s.get("memory_regime") or s.get("category", "")),
            }
            for s in sorted(
                signal_list,
                key=lambda item: (
                    float(item.get("memory_score", 0.0) or 0.0),
                    int(item.get("memory_sample_count", 0) or 0),
                ),
                reverse=True,
            )[:5]
        ],
        "execution": [
            {
                "asset": s.get("asset", ""),
                "direction": s.get("direction", ""),
                "score": round(float(s.get("execution_quality_score", 0.0) or 0.0), 1),
                "samples": int(s.get("execution_feedback_sample_count", 0) or 0),
                "subtitle": str(_playbook_name_from_payload(s) or s.get("category", "")),
            }
            for s in sorted(
                signal_list,
                key=lambda item: (
                    float(item.get("execution_quality_score", 0.0) or 0.0),
                    int(item.get("execution_feedback_sample_count", 0) or 0),
                ),
                reverse=True,
            )[:5]
        ],
    }
    playbook_performance = _summarize_playbook_performance(closed_trades, days_back=days)
    near_misses = _summarize_near_misses(journals, limit=6)
    active_decisions = signal_list if signal_list else near_misses
    if not signal_list and active_decisions:
        live_quality.update(
            {
                "signal_count": len(active_decisions),
                "avg_confidence": round(
                    sum(float(s.get("confidence", 0.0) or 0.0) for s in active_decisions)
                    / len(active_decisions)
                    * 100.0,
                    1,
                ),
                "avg_memory_score": round(
                    sum(float(s.get("memory_score", 0.0) or 0.0) for s in active_decisions) / len(active_decisions),
                    1,
                ),
                "avg_execution_quality": round(
                    sum(float(s.get("execution_quality_score", 0.0) or 0.0) for s in active_decisions) / len(active_decisions),
                    1,
                ),
                "avg_opportunity_score": round(
                    sum(float(s.get("opportunity_score", 0.0) or 0.0) for s in active_decisions) / len(active_decisions),
                    3,
                ),
                "memory_ready_count": sum(1 for s in active_decisions if int(s.get("memory_sample_count", 0) or 0) > 0),
                "execution_ready_count": sum(1 for s in active_decisions if int(s.get("execution_feedback_sample_count", 0) or 0) > 0),
            }
        )
        live_leaders = {
            "memory": [
                {
                    "asset": s.get("asset", ""),
                    "direction": s.get("direction", ""),
                    "score": round(float(s.get("memory_score", 0.0) or 0.0), 1),
                    "samples": int(s.get("memory_sample_count", 0) or 0),
                    "subtitle": str(s.get("memory_setup_style") or s.get("memory_regime") or s.get("category", "")),
                }
                for s in sorted(
                    active_decisions,
                    key=lambda item: (
                        float(item.get("memory_score", 0.0) or 0.0),
                        int(item.get("memory_sample_count", 0) or 0),
                    ),
                    reverse=True,
                )[:5]
            ],
            "execution": [
                {
                    "asset": s.get("asset", ""),
                    "direction": s.get("direction", ""),
                    "score": round(float(s.get("execution_quality_score", 0.0) or 0.0), 1),
                    "samples": int(s.get("execution_feedback_sample_count", 0) or 0),
                    "subtitle": str(_playbook_name_from_payload(s) or s.get("category", "")),
                }
                for s in sorted(
                    active_decisions,
                    key=lambda item: (
                        float(item.get("execution_quality_score", 0.0) or 0.0),
                        int(item.get("execution_feedback_sample_count", 0) or 0),
                    ),
                    reverse=True,
                )[:5]
            ],
        }
    asset_playbook_matrix = _summarize_asset_playbook_matrix(active_decisions, closed_trades, limit=10)
    failure_archetypes = _summarize_failure_archetypes(active_decisions or journals, closed_trades, limit=6)
    confidence_decomposition = _summarize_confidence_decomposition(active_decisions)
    payload = {
        "success": True,
        "accuracy": accuracy,
        "signals": active_decisions,
        "near_misses": near_misses,
        "asset_playbook_matrix": asset_playbook_matrix,
        "failure_archetypes": failure_archetypes,
        "confidence_decomposition": confidence_decomposition,
        "live_quality": live_quality,
        "live_leaders": live_leaders,
        "playbook_performance": playbook_performance,
        "timestamp": datetime.utcnow().isoformat(),
    }
    _cache_set(cache_key, payload, ttl=20)
    return jsonify(payload)

# ══════════════════════════════════════════════════════════════════════════════
# API — WHALE INTELLIGENCE
# ══════════════════════════════════════════════════════════════════════════════

def _db_whale_dashboard_summary(
    *,
    min_value_usd: float = 500_000,
    hours: int = 24,
    recent_limit: int = 10,
    alert_limit: int = 20,
) -> Dict[str, Any]:
    try:
        from services.db_pool import get_db

        rows = list(get_db().get_recent_whale_alerts(hours=hours) or [])
    except Exception as exc:
        logger.debug(f"[dashboard] whale DB fallback failed: {exc}")
        rows = []

    alerts: List[Dict[str, Any]] = []
    threshold = float(min_value_usd or 0.0)
    for row in rows:
        if not isinstance(row, dict):
            continue
        value = _risk_float(row.get("value_usd", row.get("size_usd", 0.0)))
        if value < threshold:
            continue
        symbol = str(row.get("symbol") or row.get("asset") or "").strip()
        alert_time = row.get("alert_time") or row.get("date") or row.get("timestamp") or ""
        alerts.append(
            {
                **row,
                "asset": symbol,
                "symbol": symbol,
                "value_usd": value,
                "size_usd": value,
                "direction": str(row.get("direction") or "").upper(),
                "source": row.get("source") or "database",
                "date": str(alert_time),
                "alert_time": str(alert_time),
                "timestamp": str(alert_time),
            }
        )

    total_vol = sum(_risk_float(item.get("value_usd")) for item in alerts)
    by_asset: Dict[str, float] = {}
    for item in alerts:
        asset = str(item.get("asset") or item.get("symbol") or "")
        if asset:
            by_asset[asset] = by_asset.get(asset, 0.0) + _risk_float(item.get("value_usd"))
    top_assets = sorted(by_asset.items(), key=lambda entry: entry[1], reverse=True)[:8]
    return {
        "success": True,
        "source": "postgres",
        "alerts": alerts[:alert_limit],
        "total_volume_usd": round(total_vol, 0),
        "alert_count_24h": len(alerts),
        "top_assets": [{"asset": asset, "volume": round(volume)} for asset, volume in top_assets],
        "recent": alerts[:recent_limit],
    }


@app.route("/api/whale/summary")
@_check_api_auth
@_check_rate_limit
def api_whale_summary():
    cache_key = "whale_summary:v2"
    cached = _cache_get(cache_key)
    if cached is not None:
        return jsonify(cached)
    try:
        mi = _get_market_intelligence()
        if not mi:
            payload = _db_whale_dashboard_summary(
                min_value_usd=500_000,
                hours=24,
                recent_limit=10,
                alert_limit=20,
            )
            ttl = 300 if payload.get("alerts") or payload.get("recent") else 45
            _cache_set(cache_key, payload, ttl=ttl)
            return jsonify(payload)
        payload = _run_with_timeout(
            mi.get_whale_dashboard_summary,
            min_value_usd=500_000,
            hours=24,
            recent_limit=10,
            alert_limit=20,
            timeout=4.5,
            default={
                "success": True,
                "alerts": [],
                "total_volume_usd": 0,
                "top_assets": [],
                "recent": [],
                "alert_count_24h": 0,
            },
            label="whale summary",
        )
        if not payload.get("alerts") and not payload.get("recent"):
            db_payload = _db_whale_dashboard_summary(
                min_value_usd=500_000,
                hours=24,
                recent_limit=10,
                alert_limit=20,
            )
            if db_payload.get("alerts") or db_payload.get("recent"):
                payload = db_payload
        ttl = 300 if payload.get("alerts") or payload.get("recent") else 45
        _cache_set(cache_key, payload, ttl=ttl)
        return jsonify(payload)
    except APIError as e:
        return handle_api_error(e, "/api/whale/summary", e.status_code)
    except Exception as e:
        return handle_api_error(e, "/api/whale/summary", 500)

# ══════════════════════════════════════════════════════════════════════════════
# API — SENTIMENT INTELLIGENCE
# ══════════════════════════════════════════════════════════════════════════════

def _default_sentiment_dashboard_result() -> Dict[str, Any]:
    return {
        "success": True,
        "overall_sentiment": "Neutral",
        "score": 0.0,
        "fear_greed": {"value": 50, "classification": "Neutral"},
        "vix": {"value": 20, "classification": "Normal"},
        "article_count": 0,
        "sentiment_distribution": {"bullish": 0, "neutral": 0, "bearish": 0},
        "articles": [],
        "whale_alerts": [],
        "news_sources_enabled": bool(NEWS_SENTIMENT_ENABLED or NEWS_RSS_ENABLED or NEWS_REDDIT_ENABLED),
        "execution_sentiment_mode": "full_sentiment" if NEWS_SENTIMENT_EXECUTION_ENABLED else "reliable_components_only",
        "market_composite": {
            "score": 0.0,
            "interpretation": "Neutral",
            "components": {},
            "drivers": [],
        },
        "news_sentiment": {
            "score": 0.0,
            "interpretation": "Neutral",
            "article_count": 0,
            "distribution": {"bullish": 0, "neutral": 0, "bearish": 0},
        },
        "sentiment_context": {
            "mode": "neutral",
            "display_label": "Neutral",
            "summary": "Awaiting sentiment inputs.",
        },
    }


def _apply_sentiment_dashboard_task_result(result: Dict[str, Any], key: str, payload: Any) -> None:
    if key == "market_sentiment" and payload:
        market_score = float(payload.get("score", 0) or 0)
        market_interpretation = payload.get("interpretation", "Neutral")
        components = {
            str(name): round(float(value or 0.0), 3)
            for name, value in dict(payload.get("components", {}) or {}).items()
        }
        drivers = [
            {
                "key": name,
                "label": _sentiment_component_label(name),
                "score": value,
            }
            for name, value in sorted(
                components.items(),
                key=lambda item: abs(float(item[1] or 0.0)),
                reverse=True,
            )
            if abs(float(value or 0.0)) > 0.001
        ]
        result["score"] = market_score
        result["overall_sentiment"] = market_interpretation
        result["market_composite"] = {
            "score": market_score,
            "interpretation": market_interpretation,
            "components": components,
            "drivers": drivers[:4],
        }
        return

    if key == "fear_greed" and payload:
        result["fear_greed"] = {
            "value": payload.get("value", 50),
            "classification": payload.get("classification", "Neutral"),
        }
        return

    if key == "vix" and payload:
        result["vix"] = {
            "value": payload.get("value", 20),
            "classification": payload.get("classification", "Normal"),
        }
        return

    if key == "articles" and payload:
        articles = list(payload or [])
        scores = [
            float(article.get("sentiment", 0) or 0)
            for article in articles
            if article.get("sentiment") is not None
        ]
        news_score = (sum(scores) / len(scores)) if scores else 0.0
        bullish = sum(1 for article in articles if float(article.get("sentiment", 0) or 0) > 0.1)
        bearish = sum(1 for article in articles if float(article.get("sentiment", 0) or 0) < -0.1)
        distribution = {
            "bullish": bullish,
            "neutral": len(articles) - bullish - bearish,
            "bearish": bearish,
        }
        result["sentiment_distribution"] = distribution
        result["articles"] = sorted(articles, key=lambda item: item.get("date", ""), reverse=True)[:20]
        result["article_count"] = len(articles)
        result["news_sentiment"] = {
            "score": round(news_score, 3),
            "interpretation": _interpret_sentiment_score(news_score),
            "article_count": len(articles),
            "distribution": distribution,
        }
        return

    if key == "whales" and payload:
        result["whale_alerts"] = list(payload or [])[:10]


def _finalize_sentiment_dashboard_result(result: Dict[str, Any]) -> None:
    if result["sentiment_distribution"] == {"bullish": 0, "neutral": 0, "bearish": 0}:
        article_count = int(result.get("article_count", 0) or 0)
        if article_count <= 0:
            result["sentiment_distribution"] = {"bullish": 0, "neutral": 1, "bearish": 0}
        else:
            score = float(result.get("score", 0) or 0)
            if score > 0.05:
                result["sentiment_distribution"] = {"bullish": 1, "neutral": 0, "bearish": 0}
            elif score < -0.05:
                result["sentiment_distribution"] = {"bullish": 0, "neutral": 0, "bearish": 1}
            else:
                result["sentiment_distribution"] = {"bullish": 0, "neutral": 1, "bearish": 0}

    if result["news_sentiment"]["article_count"] == 0:
        result["news_sentiment"] = {
            "score": 0.0,
            "interpretation": _interpret_sentiment_score(0.0),
            "article_count": int(result.get("article_count", 0) or 0),
            "distribution": dict(result.get("sentiment_distribution", {}) or {}),
        }

    fear_greed_value = float((result.get("fear_greed") or {}).get("value", 50) or 50)
    result["sentiment_context"] = _build_sentiment_context(
        market_score=float(result.get("score", 0) or 0),
        news_score=float((result.get("news_sentiment") or {}).get("score", 0) or 0),
        fear_greed_value=fear_greed_value,
        market_interpretation=str(result.get("overall_sentiment") or "Neutral"),
        article_count=int(result.get("article_count", 0) or 0),
    )


def _sentiment_by_asset_row(mi: Any, asset: str) -> Dict[str, Any]:
    cat = _cat(asset)
    if _is_market_weekend(cat):
        return {
            "asset": asset,
            "category": cat,
            "score": 0.0,
            "label": "Market Closed",
        }
    try:
        snapshot = mi.get_asset_snapshot(asset, cat)
        score = float(snapshot.get("sentiment_score", 0.0) or 0.0)
    except Exception:
        score = 0.0
    return {
        "asset": asset,
        "category": cat,
        "score": round(score, 3),
        "label": "Bullish" if score > 0.1 else "Bearish" if score < -0.1 else "Neutral",
    }


def _sentiment_by_asset_neutral_row(asset: str) -> Dict[str, Any]:
    return {
        "asset": asset,
        "category": _cat(asset),
        "score": 0.0,
        "label": "Neutral",
    }


def _neutral_sentiment_by_asset_payload(reason: str) -> Dict[str, Any]:
    assets = [_sentiment_by_asset_neutral_row(asset) for asset, _ in ALL_ASSETS]
    assets.sort(key=lambda row: (str(row.get("category") or ""), str(row.get("asset") or "")))
    return {
        "success": True,
        "assets": assets,
        "degraded": True,
        "degraded_reason": reason,
    }


def _collect_sentiment_by_asset_rows(mi: Any, watch: List[str], *, timeout: float = 12.0) -> List[Dict[str, Any]]:
    from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FuturesTimeout

    results: List[Dict[str, Any]] = []
    seen: set[str] = set()
    max_workers = max(1, min(int(DASHBOARD_SENTIMENT_ASSET_WORKERS or 10), len(watch)))
    pool = ThreadPoolExecutor(max_workers=max_workers)
    try:
        futures = {pool.submit(_sentiment_by_asset_row, mi, asset): asset for asset in watch}
        try:
            for future in as_completed(futures, timeout=timeout):
                try:
                    row = future.result()
                    asset_key = str(row.get("asset", "") or "")
                    if asset_key and asset_key not in seen:
                        seen.add(asset_key)
                        results.append(row)
                except Exception:
                    pass
        except FuturesTimeout:
            for future, asset in futures.items():
                if asset in seen:
                    continue
                if future.done():
                    try:
                        row = future.result()
                        seen.add(asset)
                        results.append(row)
                    except Exception:
                        pass
                if asset not in seen:
                    seen.add(asset)
                    results.append(_sentiment_by_asset_neutral_row(asset))
            message = f"[Sentiment] by-asset timeout — returning {len(results)}/{len(watch)} assets"
            if len(results) < len(watch):
                logger.warning(message)
            else:
                logger.info(message)
    finally:
        try:
            pool.shutdown(wait=False, cancel_futures=True)
        except Exception:
            pass
    return results


@app.route("/api/sentiment/dashboard")
@_check_api_auth
@_check_rate_limit
def api_sentiment_dashboard():
    cached = _cache_get("sentiment_dashboard")
    if cached is not None:
        return jsonify(cached)
    try:
        sa = _get_sent()
        if sa is None:
            return jsonify({"success": False, "error": "Sentiment service unavailable"}), 503

        result = _default_sentiment_dashboard_result()
        news_enabled = bool(NEWS_SENTIMENT_ENABLED or NEWS_REDDIT_ENABLED or NEWS_RSS_ENABLED)
        result["news_sources_enabled"] = news_enabled
        timed_out_tasks = 0

        pool = None
        try:
            from concurrent.futures import ThreadPoolExecutor, wait

            pool = ThreadPoolExecutor(max_workers=max(1, int(DASHBOARD_SENTIMENT_FETCH_WORKERS or 6)))
            futures = {
                pool.submit(sa.get_comprehensive_sentiment): "market_sentiment",
                pool.submit(sa.fetch_fear_greed_index): "fear_greed",
                pool.submit(sa.fetch_vix): "vix",
                pool.submit(sa.fetch_whale_alerts, min_value_usd=1_000_000): "whales",
            }
            if news_enabled:
                futures[pool.submit(sa.news_integrator.fetch_all_sources)] = "articles"
            done, not_done = wait(tuple(futures.keys()), timeout=9.5)
            for future in done:
                key = futures[future]
                try:
                    payload = future.result()
                except Exception as exc:
                    logger.debug(f"[dashboard] sentiment {key} error: {exc}")
                    continue
                _apply_sentiment_dashboard_task_result(result, key, payload)

            for future in not_done:
                future.cancel()
            if not_done:
                timed_out_tasks = len(not_done)
                logger.warning(f"[dashboard] sentiment dashboard timed out for {len(not_done)} task(s)")
        except Exception as exc:
            logger.debug(f"[dashboard] sentiment dashboard setup failed: {exc}")
        finally:
            if pool is not None:
                try:
                    pool.shutdown(wait=False, cancel_futures=True)
                except Exception:
                    pass

        _finalize_sentiment_dashboard_result(result)

        ttl = 600 if result["article_count"] or result["whale_alerts"] else (5 if timed_out_tasks else 45)
        _cache_set("sentiment_dashboard", result, ttl=ttl)
        return jsonify(result)
    except APIError as e:
        return handle_api_error(e, "/api/sentiment/dashboard", e.status_code)
    except Exception as e:
        return handle_api_error(e, "/api/sentiment/dashboard", 500)

@app.route("/api/sentiment/by-asset")
@_check_api_auth
@_check_rate_limit
def api_sentiment_by_asset():
    cached = _cache_get(_SENTIMENT_BY_ASSET_CACHE_KEY)
    if cached is not None:
        return jsonify(cached)
    try:
        mi = _get_market_intelligence()
        if not mi:
            payload = _neutral_sentiment_by_asset_payload("market_intelligence_unavailable")
            _cache_set(_SENTIMENT_BY_ASSET_CACHE_KEY, payload, ttl=60)
            return jsonify(payload)

        watch = [a for a, _ in ALL_ASSETS]
        results = _collect_sentiment_by_asset_rows(mi, watch, timeout=12)

        results.sort(key=lambda x: x["score"], reverse=True)
        payload = {"success": True, "assets": results}
        # Cache 10 minutes — Reddit data doesn't change that fast
        _cache_set(_SENTIMENT_BY_ASSET_CACHE_KEY, payload, ttl=600)
        return jsonify(payload)
    except APIError as e:
        return handle_api_error(e, "/api/sentiment/by-asset", e.status_code)
    except Exception as e:
        return handle_api_error(e, "/api/sentiment/by-asset", 500)

@app.route("/api/market/events")
@_check_api_auth
@_check_rate_limit
def api_market_events():
    cache_key = "market_events:v1"
    cached = _cache_get(cache_key)
    if cached is not None:
        return jsonify(cached)
    try:
        mi = _get_market_intelligence()
        payload: Dict[str, Any] = {
            "success": True,
            "events": [],
            "earnings": [],
            "halving": {},
            "risk_outlook": {},
        }
        if mi:
            raw = mi.get_market_events(days=7, limit=20)
            if isinstance(raw, dict):
                payload["events"] = raw.get("events", raw.get("calendar", []))[:20]
                payload["earnings"] = raw.get("earnings", [])
                payload["halving"] = raw.get("halving", {}) or {}
                payload["risk_outlook"] = raw.get("risk_outlook", {}) or {}
            elif isinstance(raw, list):
                payload["events"] = raw[:20]
        _cache_set(cache_key, payload, ttl=60)
        return jsonify(payload)
    except APIError as e:
        return handle_api_error(e, "/api/market/events", e.status_code)
    except Exception as e:
        return handle_api_error(e, "/api/market/events", 500)

# ══════════════════════════════════════════════════════════════════════════════
# API — RISK DASHBOARD
# ══════════════════════════════════════════════════════════════════════════════

def _build_risk_portfolio_payload() -> Dict[str, Any]:
    core = _core()
    dashboard_standalone = core is None
    risk_stats: Dict[str, Any] = {}
    weak_queue: List[Dict[str, Any]] = []

    if core:
        positions = core.get_positions()
        perf = core.get_performance()
        account = _authoritative_account_summary(core, positions)
        balance = float(account.get("balance", _args.balance) or _args.balance)
        perf = {
            **dict(perf or {}),
            "balance": balance,
            "total_pnl": account.get("total_pnl", 0.0),
            "total_trades": account.get("closed_trades", account.get("total_trades", 0)),
            "win_rate": account.get("win_rate", 0.0),
        }
        closed = _load_authoritative_closed_trades(limit=100)
        try:
            if hasattr(core, "portfolio_risk") and core.portfolio_risk:
                risk_stats = core.portfolio_risk.get_portfolio_stats(positions, balance)
        except Exception:
            risk_stats = {}
        try:
            weak_queue = _get_command_center_weak_positions(core, limit=5)
        except Exception:
            weak_queue = []
    else:
        perf, _daily, positions, _health, closed = _load_dashboard_db_runtime_snapshot()
        account = _authoritative_account_summary(None, positions)
        balance = _risk_float(account.get("balance"), _risk_float(getattr(_args, "balance", 0.0)))
        perf = {
            **dict(perf or {}),
            "balance": balance,
            "total_pnl": account.get("total_pnl", perf.get("total_pnl", 0.0)),
            "total_trades": account.get("closed_trades", account.get("total_trades", perf.get("total_trades", 0))),
            "win_rate": account.get("win_rate", perf.get("win_rate", 0.0)),
        }
        risk_stats = _basic_risk_portfolio_stats(positions, balance, perf)

    by_cat, cluster_groups = _summarize_risk_portfolio_positions(positions)

    wins = [t for t in closed if float(t.get("pnl") or 0) > 0]
    losses = [t for t in closed if float(t.get("pnl") or 0) <= 0 and float(t.get("pnl") or 0) != 0]
    avg_win = sum(float(t.get("pnl") or 0) for t in wins) / len(wins) if wins else 0.0
    avg_los = sum(float(t.get("pnl") or 0) for t in losses) / len(losses) if losses else 0.0
    pf = abs(avg_win / avg_los) if avg_los else 0.0

    execution_summary: Dict[str, Any] = {}
    execution_by_category: Dict[str, Any] = {}
    weak_queue: List[Dict[str, Any]] = []
    try:
        from services.execution_feedback_service import get_service as get_execution_feedback_service

        feedback_service = get_execution_feedback_service()
        execution_summary = feedback_service.summarize_history(days_back=120, limit=500)
        for cat in ("forex", "crypto", "commodities", "indices"):
            execution_by_category[cat] = feedback_service.summarize_history(
                category=cat,
                days_back=120,
                limit=250,
            )
    except Exception:
        execution_summary = {}
        execution_by_category = {}
    weak_queue = [_enrich_signal_like_row(item) for item in list(weak_queue or []) if isinstance(item, dict)]

    stop_concentration = _summarize_stop_concentration(positions, limit=5)
    scenario_risk = _summarize_scenario_risk(positions)

    payload = {
        "success": True,
        "dashboard_standalone": dashboard_standalone,
        "balance": balance,
        "open_positions": len(positions),
        "total_exposure": risk_stats.get("total_exposure", 0),
        "exposure_pct": risk_stats.get("exposure_pct", 0),
        "drawdown_pct": risk_stats.get("drawdown_pct", 0),
        "peak_balance": risk_stats.get("peak_balance", balance),
        "by_category": by_cat,
        "cluster_groups": cluster_groups,
        "win_rate": _wr(perf.get("win_rate", 0)),
        "profit_factor": round(pf, 2),
        "avg_win": round(avg_win, 2),
        "avg_loss": round(avg_los, 2),
        "total_trades": perf.get("total_trades", 0),
        "total_pnl": perf.get("total_pnl", 0),
        "quality_snapshot": _risk_portfolio_quality_snapshot(by_cat),
        "execution_feedback": execution_summary,
        "execution_by_category": execution_by_category,
        "stop_concentration": stop_concentration,
        "scenario_risk": scenario_risk,
        "weak_queue": weak_queue,
        "signal_diagnostics": _summarize_signal_diagnostics(
            [_extract_signal_intelligence_fields(dict(p.get("metadata") or {})) for p in positions]
        ),
    }
    return _attach_authoritative_account_fields(payload, account)


def _get_cached_risk_portfolio_payload(*, force_refresh: bool = False) -> Dict[str, Any]:
    cache_key = "risk_portfolio:v3"
    last_good_key = "risk_portfolio:v3:last_good"
    try:
        return _get_cached_dashboard_payload(
            cache_key,
            builder=_build_risk_portfolio_payload,
            ttl=15,
            force_refresh=force_refresh,
            last_good_key=last_good_key,
            last_good_ttl=300,
            prefer_stale=True,
            refresh_key="risk_portfolio",
        )
    except Exception as exc:
        logger.warning(f"[dashboard] risk-portfolio payload build failed: {exc}")
        fallback = _cache_get(last_good_key)
        if fallback is not None:
            return _response_to_dict(fallback)
        raise


@app.route("/api/risk/portfolio")
@_check_api_auth
@_check_rate_limit
def api_risk_portfolio():
    try:
        return jsonify(_get_cached_risk_portfolio_payload(force_refresh=_request_wants_cache_bypass()))
    except APIError as e:
        return handle_api_error(e, "/api/risk/portfolio", e.status_code)
    except Exception as e:
        return handle_api_error(e, "/api/risk/portfolio", 500)

# ══════════════════════════════════════════════════════════════════════════════
# API — STRATEGY LAB
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/api/strategy/performance")
@_check_api_auth
@_check_rate_limit
def api_strategy_performance():
    try:
        cache_key = "strategy_performance:v3"
        cached = _cache_get(cache_key)
        if cached is not None:
            return jsonify(cached)
        core = _core()
        dashboard_standalone = core is None
        if core:
            stats  = core.get_strategy_stats()
            trades = core.get_closed_trades(limit=200)
        else:
            trades = _load_authoritative_closed_trades(limit=200)
            stats = {}
            for trade in trades:
                if not isinstance(trade, dict):
                    continue
                strat = str(trade.get("strategy_id") or trade.get("strategy") or trade.get("playbook") or "Unspecified")
                pnl = _risk_float(trade.get("pnl"))
                bucket = stats.setdefault(strat, {"wins": 0, "losses": 0, "pnl": 0.0})
                if pnl > 0:
                    bucket["wins"] += 1
                elif pnl < 0:
                    bucket["losses"] += 1
                bucket["pnl"] = _risk_float(bucket.get("pnl")) + pnl
        enriched: Dict = {}
        summary_memory_scores: List[float] = []
        summary_exec_scores: List[float] = []
        summary_rr: List[float] = []
        summary_target_hits = 0
        summary_premature_stops = 0
        summary_timeline_count = 0
        for strat, s in stats.items():
            total   = s.get("wins", 0) + s.get("losses", 0)
            pnl     = s.get("pnl", 0)
            wr      = s.get("wins", 0) / total * 100 if total else 0
            strat_trades = []
            for t in trades:
                trade_strategy = str(t.get("strategy_id") or t.get("strategy") or t.get("playbook") or "Unspecified")
                if trade_strategy != strat:
                    continue
                row = dict(t)
                meta = dict(row.get("metadata") or {})
                row.update(_extract_execution_feedback_fields(meta))
                row.update(_extract_memory_fields(meta))
                row.update(_extract_signal_intelligence_fields(meta))
                strat_trades.append(row)

            durs    = [int(t.get("duration_minutes", 0)) for t in strat_trades if t.get("duration_minutes")]
            avg_dur = sum(durs) / len(durs) if durs else 0
            memory_scores = [float(t.get("memory_score", 0.0) or 0.0) for t in strat_trades if float(t.get("memory_score", 0.0) or 0.0) > 0]
            exec_scores = [float(t.get("execution_quality_score", 0.0) or 0.0) for t in strat_trades if float(t.get("execution_quality_score", 0.0) or 0.0) > 0]
            rr_vals = [float(t.get("rr_realized", 0.0) or 0.0) for t in strat_trades if abs(float(t.get("rr_realized", 0.0) or 0.0)) > 1e-9]
            target_hits = [1.0 if float(t.get("target_capture", 0.0) or 0.0) >= 0.95 else 0.0 for t in strat_trades if abs(float(t.get("target_capture", 0.0) or 0.0)) > 1e-9]
            premature_flags = [1.0 if bool(t.get("premature_stop")) else 0.0 for t in strat_trades]
            summary_memory_scores.extend(memory_scores)
            summary_exec_scores.extend(exec_scores)
            summary_rr.extend(rr_vals)
            summary_target_hits += int(sum(target_hits))
            summary_premature_stops += int(sum(premature_flags))
            summary_timeline_count += len(strat_trades)
            enriched[strat] = {**s, "total": total, "win_rate": round(wr, 1),
                               "avg_duration_min": round(avg_dur),
                               "avg_trade_pnl": round(pnl / total, 4) if total else 0,
                               "avg_memory_score": round(sum(memory_scores) / len(memory_scores), 1) if memory_scores else 0.0,
                               "avg_execution_quality": round(sum(exec_scores) / len(exec_scores), 1) if exec_scores else 0.0,
                               "avg_rr_realized": round(sum(rr_vals) / len(rr_vals), 3) if rr_vals else 0.0,
                               "target_hit_rate": round(sum(target_hits) / len(target_hits), 4) if target_hits else 0.0,
                               "premature_stop_rate": round(sum(premature_flags) / len(premature_flags), 4) if premature_flags else 0.0}
        timeline = []
        for t in trades[:50]:
            row = {"asset": t.get("asset", ""), "direction": t.get("direction", ""),
                   "pnl": float(t.get("pnl") or 0), "strategy": t.get("strategy_id", ""),
                   "exit_time": str(t.get("exit_time", ""))[:16],
                   "conf": float(t.get("confidence") or 0)}
            row["strategy"] = str(t.get("strategy_id") or t.get("strategy") or t.get("playbook") or "Unspecified")
            meta = dict(t.get("metadata") or {})
            row.update(_extract_execution_feedback_fields(meta))
            row.update(_extract_memory_fields(meta))
            row.update(_extract_signal_intelligence_fields(meta))
            timeline.append(row)
        summary = {
            "avg_memory_score": round(sum(summary_memory_scores) / len(summary_memory_scores), 1) if summary_memory_scores else 0.0,
            "avg_execution_quality": round(sum(summary_exec_scores) / len(summary_exec_scores), 1) if summary_exec_scores else 0.0,
            "avg_rr_realized": round(sum(summary_rr) / len(summary_rr), 3) if summary_rr else 0.0,
            "target_hit_rate": round(summary_target_hits / max(summary_timeline_count, 1), 4) if summary_timeline_count else 0.0,
            "premature_stop_rate": round(summary_premature_stops / max(summary_timeline_count, 1), 4) if summary_timeline_count else 0.0,
            "trade_count": summary_timeline_count,
        }
        payload = {
            "success": True,
            "dashboard_standalone": dashboard_standalone,
            "strategies": enriched,
            "timeline": timeline,
            "summary": summary,
        }
        _cache_set(cache_key, payload, ttl=15)
        return jsonify(payload)
    except APIError as e:
        return handle_api_error(e, "/api/strategy/performance", e.status_code)
    except Exception as e:
        return handle_api_error(e, "/api/strategy/performance", 500)

@app.route("/api/backtest/strategies")
@_check_api_auth
@_check_rate_limit
def api_backtest_strategies():
    try:
        return jsonify(
            {
                "success": True,
                "enabled": False,
                "presets": [],
                "archived_presets": [],
                "playbooks": list(_PLAYBOOK_RUNTIME_BLUEPRINTS),
                "live_runtime": ["playbook_only"],
            }
        )
    except APIError as e:
        return handle_api_error(e, "/api/backtest/strategies", e.status_code)
    except Exception as e:
        return handle_api_error(e, "/api/backtest/strategies", 500)

@app.route("/api/backtest/run")
@_check_api_auth
@_check_rate_limit
def api_backtest_run():
    try:
        return _playbook_only_disabled_response("/api/backtest/run", "Backtest API")
    except APIError as e:
        return handle_api_error(e, "/api/backtest/run", e.status_code)
    except Exception as e:
        return handle_api_error(e, "/api/backtest/run", 500)

@app.route("/api/backtest/robustness")
@_check_api_auth
@_check_rate_limit
def api_backtest_robustness():
    try:
        return _playbook_only_disabled_response("/api/backtest/robustness", "Backtest API")
    except APIError as e:
        return handle_api_error(e, "/api/backtest/robustness", e.status_code)
    except Exception as e:
        return handle_api_error(e, "/api/backtest/robustness", 500)

@app.route("/api/backtest/compare")
@_check_api_auth
@_check_rate_limit
def api_backtest_compare():
    try:
        return _playbook_only_disabled_response("/api/backtest/compare", "Backtest API")
    except APIError as e:
        return handle_api_error(e, "/api/backtest/compare", e.status_code)
    except Exception as e:
        return handle_api_error(e, "/api/backtest/compare", 500)

@app.route("/api/backtest/optimize")
@_check_api_auth
@_check_rate_limit
def api_backtest_optimize():
    try:
        return _playbook_only_disabled_response("/api/backtest/optimize", "Backtest API")
    except APIError as e:
        return handle_api_error(e, "/api/backtest/optimize", e.status_code)
    except Exception as e:
        return handle_api_error(e, "/api/backtest/optimize", 500)

@app.route("/api/backtest/multi-asset")
@_check_api_auth
@_check_rate_limit
def api_backtest_multi_asset():
    try:
        return _playbook_only_disabled_response("/api/backtest/multi-asset", "Backtest API")
    except APIError as e:
        return handle_api_error(e, "/api/backtest/multi-asset", e.status_code)
    except Exception as e:
        return handle_api_error(e, "/api/backtest/multi-asset", 500)
    except APIError as e:
        return handle_api_error(e, "/api/backtest/multi-asset", e.status_code)
    except Exception as e:
        return handle_api_error(e, "/api/backtest/multi-asset", 500)

# ══════════════════════════════════════════════════════════════════════════════
# API — PHASE 1: DATA INGESTION
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/api/phase1/funding-rates")
def api_phase1_funding():
    cached = _cache_get("p1_funding")
    if cached: return jsonify(cached)
    try:
        from data_ingestion import funding_monitor
        data = funding_monitor.get_all_rates() if hasattr(funding_monitor, "get_all_rates") else {}
        payload = {"success": True, "rates": data, "timestamp": datetime.now().isoformat()}
        _cache_set("p1_funding", payload, ttl=60)
        return jsonify(payload)
    except Exception as e:
        return jsonify({"success": False, "rates": {}, "error": str(e)})

@app.route("/api/phase1/open-interest")
def api_phase1_oi():
    cached = _cache_get("p1_oi")
    if cached: return jsonify(cached)
    try:
        from data_ingestion import oi_monitor
        data = oi_monitor.get_all_signals() if hasattr(oi_monitor, "get_all_signals") else {}
        payload = {"success": True, "data": data, "timestamp": datetime.now().isoformat()}
        _cache_set("p1_oi", payload, ttl=120)
        return jsonify(payload)
    except Exception as e:
        return jsonify({"success": False, "data": {}, "error": str(e)})

@app.route("/api/phase1/liquidations")
def api_phase1_liquidations():
    cached = _cache_get("p1_liq")
    if cached: return jsonify(cached)
    try:
        from services.redis_pool import get_client as _get_redis_client
        _rc = _get_redis_client()
        if not _rc:
            raise RuntimeError("Redis unavailable")
        raw = _rc.lrange("LIQUIDATION_EVENTS", 0, 49)
        events = [json.loads(i) for i in raw if i]
        payload = {"success": True, "events": events, "count": len(events),
                   "timestamp": datetime.now().isoformat()}
        _cache_set("p1_liq", payload, ttl=30)
        return jsonify(payload)
    except Exception as e:
        return jsonify({"success": False, "events": [], "error": str(e)})

@app.route("/api/phase1/macro")
def api_phase1_macro():
    cached = _cache_get("p1_macro")
    if cached: return jsonify(cached)
    try:
        from data_ingestion import macro_collector
        data = macro_collector.get_latest() if hasattr(macro_collector, "get_latest") else {}
        payload = {"success": True, "data": data, "timestamp": datetime.now().isoformat()}
        _cache_set("p1_macro", payload, ttl=300)
        return jsonify(payload)
    except Exception as e:
        return jsonify({"success": False, "data": {}, "error": str(e)})

# ══════════════════════════════════════════════════════════════════════════════
# API — PHASE 2: WHALE INTELLIGENCE
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/api/phase2/wallets")
def api_phase2_wallets():
    cached = _cache_get("p2_wallets")
    if cached: return jsonify(cached)
    try:
        from whale_intelligence import tracker
        wallets = tracker.get_wallet_states() if hasattr(tracker, "get_wallet_states") else []
        payload = {"success": True, "wallets": wallets, "count": len(wallets),
                   "timestamp": datetime.now().isoformat()}
        _cache_set("p2_wallets", payload, ttl=120)
        return jsonify(payload)
    except Exception as e:
        return jsonify({"success": False, "wallets": [], "error": str(e)})

@app.route("/api/phase2/clusters")
def api_phase2_clusters():
    cached = _cache_get("p2_clusters")
    if cached: return jsonify(cached)
    try:
        from services.redis_pool import get_client as _get_redis_client
        _rc = _get_redis_client()
        if not _rc:
            raise RuntimeError("Redis unavailable")
        raw = _rc.lrange("WHALE_CLUSTER_EVENTS", 0, 19)
        events = [json.loads(i) for i in raw if i]
        payload = {"success": True, "clusters": events, "timestamp": datetime.now().isoformat()}
        _cache_set("p2_clusters", payload, ttl=120)
        return jsonify(payload)
    except Exception as e:
        return jsonify({"success": False, "clusters": [], "error": str(e)})

# ══════════════════════════════════════════════════════════════════════════════
# API — PHASE 3: ORDER FLOW
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/api/phase3/imbalance")
def api_phase3_imbalance():
    cached = _cache_get("p3_imbalance")
    if cached: return jsonify(cached)
    try:
        from order_flow import get_imbalance, TRACKED_ASSETS
        data     = {a: round(get_imbalance(a), 4) for a in TRACKED_ASSETS}
        scores   = list(data.values())
        bullish  = [s for s in scores if s > 0.05]
        bearish  = [s for s in scores if s < -0.05]
        payload  = {
            "success":       True,
            "imbalances":    data,
            "avg_buy":       round(sum(bullish) / len(bullish), 4) if bullish else 0.0,
            "avg_sell":      round(sum(bearish) / len(bearish), 4) if bearish else 0.0,
            "bullish_count": len(bullish),
            "bearish_count": len(bearish),
            "timestamp":     datetime.now().isoformat(),
        }
        _cache_set("p3_imbalance", payload, ttl=30)
        return jsonify(payload)
    except Exception as e:
        return jsonify({"success": False, "imbalances": {}, "error": str(e)})

@app.route("/api/phase3/walls")
def api_phase3_walls():
    _start_p3_listener()
    with _p3_wall_lock:
        walls = list(_p3_walls[-30:])
    return jsonify({"success": True, "walls": walls, "count": len(walls),
                    "timestamp": datetime.now().isoformat()})

@app.route("/api/phase3/stop-hunts")
def api_phase3_stop_hunts():
    _start_p3_listener()
    with _p3_hunt_lock:
        hunts = list(_p3_hunts[-20:])
    return jsonify({"success": True, "hunts": hunts,
                    "timestamp": datetime.now().isoformat()})


@app.route("/api/phase3/live-depth")
def api_phase3_live_depth():
    cached = _cache_get("p3_live_depth")
    if cached:
        return jsonify(cached)
    try:
        from services.dukascopy_live_depth_bridge import dukascopy_live_depth_bridge

        bridge = dukascopy_live_depth_bridge
        status = bridge.status() if bridge is not None else {}
        supported_assets: List[str] = []
        rows: List[Dict[str, Any]] = []

        if bridge is not None:
            for asset, category in ALL_ASSETS:
                if category == "crypto" or not bridge.supports(asset, category=category):
                    continue
                supported_assets.append(asset)
                snapshot = bridge.get_latest_snapshot(asset) or {}
                micro = bridge.get_microstructure(asset, category=category) or {}
                if not snapshot and not micro:
                    continue

                try:
                    age_seconds = float(micro.get("depth_live_age_seconds", 0.0) or 0.0)
                except Exception:
                    age_seconds = 0.0

                rows.append(
                    {
                        "asset": asset,
                        "category": category,
                        "price": float(snapshot.get("price", 0.0) or 0.0),
                        "bid": snapshot.get("bid"),
                        "ask": snapshot.get("ask"),
                        "spread_bps": float(micro.get("spread_bps", 0.0) or 0.0),
                        "score": float(micro.get("score", 0.0) or 0.0),
                        "book_imbalance": float(micro.get("book_imbalance", 0.0) or 0.0),
                        "pressure_direction": str(micro.get("pressure_direction") or ""),
                        "depth_levels": int(micro.get("depth_levels", 0) or 0),
                        "bid_vol": float(micro.get("bid_vol", 0.0) or 0.0),
                        "ask_vol": float(micro.get("ask_vol", 0.0) or 0.0),
                        "orderbook_top_bids": list(micro.get("orderbook_top_bids") or [])[:3],
                        "orderbook_top_asks": list(micro.get("orderbook_top_asks") or [])[:3],
                        "age_seconds": round(max(0.0, age_seconds), 3),
                        "environment": str(snapshot.get("environment") or micro.get("environment") or "demo"),
                        "dukascopy_symbol": str(snapshot.get("dukascopy_symbol") or micro.get("dukascopy_symbol") or ""),
                        "as_of_utc": str(snapshot.get("as_of_utc") or micro.get("as_of_utc") or ""),
                    }
                )

        rows.sort(
            key=lambda row: (
                1 if float(row.get("age_seconds", 999.0) or 999.0) > 30.0 else 0,
                float(row.get("age_seconds", 999.0) or 999.0),
                -int(row.get("depth_levels", 0) or 0),
                -abs(float(row.get("book_imbalance", 0.0) or 0.0)),
                str(row.get("asset") or ""),
            )
        )

        payload = {
            "success": True,
            "enabled": bool(status.get("enabled")),
            "running": bool(status.get("running")),
            "profiles": list(status.get("profiles") or []),
            "configured_assets": list(status.get("assets") or []),
            "supported_assets": supported_assets,
            "rows": rows,
            "count": len(rows),
            "timestamp": datetime.now().isoformat(),
        }
        _cache_set("p3_live_depth", payload, ttl=5)
        return jsonify(payload)
    except Exception as e:
        return jsonify({"success": False, "rows": [], "error": str(e)})

# ══════════════════════════════════════════════════════════════════════════════
# API — PHASE 7: INTELLIGENCE ALERTS
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/api/phase7/alerts")
def api_phase7_alerts():
    cached = _cache_get("p7_alerts")
    if cached is not None:
        return jsonify(cached)
    _start_p7_listener()
    with _p7_lock:
        alerts = list(_p7_alerts[-50:])
    by_priority: Dict[str, int] = {"CRITICAL": 0, "HIGH": 0, "MEDIUM": 0, "LOW": 0}
    for a in alerts:
        p = a.get("priority", "LOW")
        by_priority[p] = by_priority.get(p, 0) + 1
    payload = {"success": True, "alerts": alerts, "count": len(alerts),
                    "by_priority": by_priority, "timestamp": datetime.now().isoformat()}
    _cache_set("p7_alerts", payload, ttl=10)
    return jsonify(payload)

@app.route("/api/phase7/signal-journal")
def api_phase7_signal_journal():
    cached = _cache_get("p7_journal")
    if cached: return jsonify(cached)
    try:
        from services.redis_pool import get_client as _get_redis_client
        _rc = _get_redis_client()
        if not _rc:
            raise RuntimeError("Redis unavailable")
        raw = _rc.lrange("SIGNAL_JOURNAL_LOG", 0, 19)
        journals = [json.loads(i) for i in raw if i]
        payload  = {"success": True, "journals": journals, "timestamp": datetime.now().isoformat()}
        _cache_set("p7_journal", payload, ttl=30)
        return jsonify(payload)
    except Exception as e:
        return jsonify({"success": False, "journals": [], "error": str(e)})

@app.route("/api/intelligence-alerts/overview")
def api_intelligence_alerts_overview():
    cache_key = "intelligence_alerts_overview:v2"
    cached = _cache_get(cache_key)
    if cached:
        return jsonify(cached)

    alerts_resp = _call_view(api_phase7_alerts)
    journals_resp = _call_view(api_phase7_signal_journal)
    alerts_data = alerts_resp.get_json() if hasattr(alerts_resp, 'get_json') else json.loads(alerts_resp.get_data(as_text=True))
    journals_data = journals_resp.get_json() if hasattr(journals_resp, 'get_json') else json.loads(journals_resp.get_data(as_text=True))

    payload = {
        "success": True,
        "alerts": alerts_data.get("alerts", []),
        "by_priority": alerts_data.get("by_priority", {}),
        "journals": journals_data.get("journals", []),
        "alert_count": alerts_data.get("count", 0),
        "timestamp": datetime.utcnow().isoformat(),
    }
    _cache_set(cache_key, payload, ttl=20)
    return jsonify(payload)


# ══════════════════════════════════════════════════════════════════════════════
# API — TRADE HISTORY + POSITION CLOSE
# ══════════════════════════════════════════════════════════════════════════════

def _infer_partial_trade_shape(trade_id: str, metadata: Dict[str, Any], exit_reason: str) -> tuple[str | None, bool]:
    parent_trade_id = metadata.get("parent_trade_id")
    if parent_trade_id in ("", None):
        raw_id = str(trade_id or "")
        if "-PT" in raw_id:
            candidate, suffix = raw_id.rsplit("-PT", 1)
            if candidate and suffix.isdigit():
                parent_trade_id = candidate
    partial_flag = metadata.get("is_partial_close")
    if partial_flag is None:
        partial_flag = bool(parent_trade_id) or str(exit_reason or "").lower().startswith("partial tp")
    return (str(parent_trade_id) if parent_trade_id not in ("", None) else None, bool(partial_flag))


def _format_trade_duration_label(duration_minutes: Any) -> str:
    if duration_minutes in (None, ""):
        return "—"
    mins = max(0, int(float(duration_minutes)))
    if mins < 60:
        return f"{mins}m"
    if mins < 1440:
        return f"{mins // 60}h {mins % 60}m"
    return f"{mins // 1440}d {(mins % 1440) // 60}h"


def _enrich_trade_history_row(trade: Dict[str, Any]) -> Dict[str, Any]:
    from config.config import TZ_NAME
    from risk.position_sizer import PositionSizer as _PS

    d = dict(trade)
    metadata = dict(d.get("metadata") or {})
    parent_trade_id, is_partial_close = _infer_partial_trade_shape(
        str(d.get("trade_id") or ""),
        metadata,
        str(d.get("exit_reason") or ""),
    )
    d["parent_trade_id"] = parent_trade_id
    d["is_partial_close"] = is_partial_close
    d.update(_extract_execution_feedback_fields(metadata))
    d.update(_extract_memory_fields(metadata))
    d.update(_extract_opportunity_fields(metadata))
    d.update(_extract_signal_intelligence_fields(metadata))
    d.update(
        _extract_market_data_provenance_fields(
            metadata,
            asset=str(d.get("asset") or ""),
            category=str(d.get("category") or ""),
        )
    )
    try:
        lot_size = d.get("lot_size")
        if lot_size in (None, ""):
            lot_size = metadata.get("lot_size")
        if lot_size in (None, ""):
            lot_size = _PS.lots_from_size(
                str(d.get("asset") or ""),
                str(d.get("category") or "forex"),
                float(d.get("position_size", 0.0) or 0.0),
            )
        d["lot_size"] = round(float(lot_size or 0.0), 4)
    except Exception:
        d["lot_size"] = 0.0
    try:
        entry_raw = d.get("entry_time") or d.get("open_time")
        exit_raw = d.get("exit_time")
        if entry_raw and not d.get("entry_time"):
            d["entry_time"] = entry_raw
        if entry_raw and not d.get("open_time"):
            d["open_time"] = entry_raw
        if entry_raw and exit_raw:
            et = datetime.fromisoformat(str(entry_raw).replace("Z", "+00:00"))
            xt = datetime.fromisoformat(str(exit_raw).replace("Z", "+00:00"))
            d["display_timezone"] = TZ_NAME
            secs = abs((xt - et).total_seconds())
            d["duration_str"] = _format_trade_duration_label(int(secs / 60))
        else:
            d["duration_str"] = _format_trade_duration_label(d.get("duration_minutes"))
    except Exception:
        d["duration_str"] = "—"
    return d


@app.route("/api/trade-history")
def api_trade_history():
    """Return last N closed trades with full details for the history panel."""
    try:
        limit = max(1, min(500, int(request.args.get("limit", 50) or 50)))
        cache_key = f"trade_history:v3:{limit}"
        payload = _cache_get(cache_key)
        if payload is None:
            raw_limit = max(limit * 3, limit + 10)
            from core.state import rollup_closed_trade_history
            trades = rollup_closed_trade_history(_load_authoritative_closed_trades(limit=raw_limit), limit=limit)
            payload = {
                "success": True,
                "trades": [_enrich_trade_history_row(t) for t in trades],
                "count": len(trades),
                "summary": _build_authoritative_trade_history_summary(trades),
            }
            _cache_set(cache_key, payload, ttl=_TRADE_HISTORY_CACHE_TTL)
        response = jsonify(payload)
        response.headers["Cache-Control"] = f"private, max-age={_TRADE_HISTORY_CACHE_TTL}, stale-while-revalidate=30"
        return response
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/trade-history/clear", methods=["POST"])
@_check_api_auth
@_check_rate_limit
def api_clear_trade_history():
    """Clear closed trade history from the database and reset in-memory stats."""
    try:
        from services.db_pool import get_db
        db = get_db()
        db.clear_trade_history(clear_daily_stats=True)
        _invalidate_cache_prefixes("closed_trades:", "trade_history:", "strategy_performance:")
    except Exception as e:
        logger.error(f"[dashboard] Failed clearing DB trade history: {e}")
        return jsonify({"success": False, "error": str(e)}), 500

    try:
        core = _core()
        if core:
            core.state.clear_trade_history()
        else:
            from core.state import SystemState
            SystemState().clear_trade_history()
    except Exception as e:
        logger.error(f"[dashboard] Failed clearing in-memory history: {e}")

    return jsonify({"success": True, "message": "Trade history cleared"})


@app.route("/api/position/close", methods=["POST"])
@_check_api_auth  # FIX SEC-05: Require API key authentication
@_check_rate_limit  # FIX SEC-05: Rate limit critical endpoints
def api_close_position():
    """Close a single position by trade_id."""
    try:
        data     = request.get_json() or {}
        trade_id = data.get("trade_id", "")
        if not trade_id:
            return jsonify({"success": False, "error": "trade_id required"}), 400
        core = _core()
        if not core:
            return jsonify({"success": False, "error": "Engine unavailable"}), 503
        result = core.close_position_manually(trade_id)
        _invalidate_cache_prefixes("risk_portfolio", "closed_trades:", "trade_history:", "strategy_performance:", "page_overview:command_center:", "page_overview:risk_dashboard:")
        return jsonify({"success": bool(result), "trade_id": trade_id,
                        "message": "Position closed"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/position/close-bulk", methods=["POST"])
@_check_api_auth  # FIX SEC-05: Require API key authentication
@_check_rate_limit  # FIX SEC-05: Rate limit critical endpoints
def api_close_bulk():
    """Close multiple positions by filter: all | category | losing | winning."""
    try:
        data     = request.get_json() or {}
        mode     = data.get("mode", "all")
        category = data.get("category", "")
        core = _core()
        if not core:
            return jsonify({"success": False, "error": "Engine unavailable"}), 503
        positions = core.state.get_open_positions()
        closed, skipped = [], []
        for pos in positions:
            cat  = pos.get("category", "")
            pnl  = float(pos.get("pnl", 0) or 0)
            tid  = pos.get("trade_id", "")
            if not tid:
                continue
            if mode == "category" and cat != category:
                continue
            if mode == "losing"   and pnl >= 0:
                continue
            if mode == "winning"  and pnl <= 0:
                continue
            try:
                core.close_position_manually(tid)
                closed.append(tid)
            except Exception:
                skipped.append(tid)
        _invalidate_cache_prefixes("risk_portfolio", "closed_trades:", "trade_history:", "strategy_performance:", "page_overview:command_center:", "page_overview:risk_dashboard:")
        return jsonify({
            "success": True,
            "closed":  len(closed),
            "skipped": len(skipped),
            "mode":    mode,
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/positions/reprice-weak", methods=["POST"])
@_check_api_auth
@_check_rate_limit
def api_reprice_weak_positions():
    try:
        data = request.get_json(silent=True) or {}
        core = _core()
        if not core:
            return jsonify({"success": False, "error": "Engine unavailable"}), 503

        limit = max(1, min(10, int(data.get("limit", 3) or 3)))
        score_threshold = float(data.get("score_threshold", 0.62) or 0.62)
        tighten_only = bool(data.get("tighten_only", True))
        updates = core.reprice_weak_exits(
            tighten_only=tighten_only,
            limit=limit,
            score_threshold=score_threshold,
        )
        _invalidate_cache_prefixes("risk_portfolio", "closed_trades:", "trade_history:", "strategy_performance:", "page_overview:command_center:", "page_overview:risk_dashboard:")
        return jsonify({
            "success": True,
            "repriced": len(updates),
            "updates": updates,
            "tighten_only": tighten_only,
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/positions/reduce-weak", methods=["POST"])
@_check_api_auth
@_check_rate_limit
def api_reduce_weak_positions():
    try:
        data = request.get_json(silent=True) or {}
        core = _core()
        if not core:
            return jsonify({"success": False, "error": "Engine unavailable"}), 503

        limit = max(1, min(10, int(data.get("limit", 3) or 3)))
        score_threshold = float(data.get("score_threshold", 0.58) or 0.58)
        reduction_fraction = float(data.get("reduction_fraction", 0.35) or 0.35)
        actions = core.reduce_weak_positions(
            reduction_fraction=reduction_fraction,
            limit=limit,
            score_threshold=score_threshold,
        )
        _invalidate_cache_prefixes("risk_portfolio", "closed_trades:", "trade_history:", "strategy_performance:", "page_overview:command_center:", "page_overview:risk_dashboard:")
        return jsonify({
            "success": True,
            "reduced": sum(1 for item in actions if item.get("success")),
            "actions": actions,
            "reduction_fraction": reduction_fraction,
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/opportunities/top")
@_check_api_auth
@_check_rate_limit
def api_top_opportunities():
    try:
        core = _core()
        if not core:
            return jsonify({"success": False, "error": "Engine unavailable"}), 503

        limit = max(1, min(10, int(request.args.get("limit", 5) or 5)))
        refresh_flag = str(request.args.get("refresh", "0") or "0").lower() in {"1", "true", "yes"}
        opportunities = core.get_top_ranked_opportunities(limit=limit, refresh=refresh_flag)
        return jsonify({
            "success": True,
            "count": len(opportunities),
            "opportunities": opportunities,
            "refreshed": refresh_flag,
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

# ══════════════════════════════════════════════════════════════════════════════
# API — SYSTEM MONITOR + MONITORING
# ══════════════════════════════════════════════════════════════════════════════

_RESOURCE_CPU_LOCK = threading.Lock()
_RESOURCE_CPU_PRIMED = False
_RESOURCE_CPU_LAST_TS = 0.0
_RESOURCE_CPU_SAMPLES: List[float] = []
_RESOURCE_CPU_SAMPLE_WINDOW_SEC = 0.5
_RESOURCE_CPU_CACHE_SECONDS = 2.0
_RESOURCE_CPU_SMOOTH_SAMPLES = 3


def _collect_stable_cpu_pct(psutil_module: Any) -> float:
    global _RESOURCE_CPU_PRIMED, _RESOURCE_CPU_LAST_TS
    with _RESOURCE_CPU_LOCK:
        now = time.monotonic()
        if _RESOURCE_CPU_SAMPLES and (now - _RESOURCE_CPU_LAST_TS) < _RESOURCE_CPU_CACHE_SECONDS:
            return sum(_RESOURCE_CPU_SAMPLES) / max(1, len(_RESOURCE_CPU_SAMPLES))
        if not _RESOURCE_CPU_PRIMED:
            psutil_module.cpu_percent(interval=None)
            _RESOURCE_CPU_PRIMED = True
        sample = float(psutil_module.cpu_percent(interval=_RESOURCE_CPU_SAMPLE_WINDOW_SEC))
        sample = max(0.0, min(100.0, sample))
        _RESOURCE_CPU_SAMPLES.append(sample)
        del _RESOURCE_CPU_SAMPLES[:-_RESOURCE_CPU_SMOOTH_SAMPLES]
        _RESOURCE_CPU_LAST_TS = time.monotonic()
        return sum(_RESOURCE_CPU_SAMPLES) / max(1, len(_RESOURCE_CPU_SAMPLES))


def _collect_system_resource_stats() -> tuple[float, float, float, float]:
    ram_pct = cpu_pct = disk_pct = proc_mb = 0.0
    try:
        import psutil

        ram_pct = psutil.virtual_memory().percent
        cpu_pct = _collect_stable_cpu_pct(psutil)
        disk_pct = psutil.disk_usage("/").percent
        proc_mb = round(psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024, 1)
    except Exception:
        pass
    return ram_pct, cpu_pct, disk_pct, proc_mb


def _collect_redis_health() -> bool:
    redis_ok = False
    try:
        if _redis_broker:
            redis_ok = bool(_redis_broker.is_connected())
    except Exception:
        pass
    return redis_ok


def _collect_database_health() -> bool:
    db_ok = False
    try:
        from services.db_pool import get_db

        db_ok = bool(get_db().ping())
    except Exception:
        pass
    return db_ok


def _collect_system_phase_health(*, external_bot_running: bool = False, dashboard_standalone: bool = False) -> Dict[str, Any]:
    phase_health: Dict[str, Any] = {}
    try:
        from data_ingestion.exchange_stream_manager import stream_manager as _esm

        phase_health["phase1_data_feeds"] = _esm._running.is_set()
    except Exception:
        phase_health["phase1_data_feeds"] = False
    try:
        from whale_intelligence import is_running as _wi_running

        phase_health["phase2_whale_intel"] = _wi_running()
    except Exception:
        phase_health["phase2_whale_intel"] = False
    try:
        import order_flow as _of

        phase_health["phase3_order_flow"] = bool(_of._running)
    except Exception:
        phase_health["phase3_order_flow"] = False
    try:
        from narrative_ai import get_dominant_narrative

        get_dominant_narrative()
        phase_health["phase4_narrative_ai"] = True
    except Exception:
        phase_health["phase4_narrative_ai"] = False
    phase_health["phase6_meta_ai"] = False
    try:
        from services.intelligence_alerts import alert_service as _as

        phase_health["phase7_intel_alerts"] = getattr(_as, "_running", False)
    except Exception:
        phase_health["phase7_intel_alerts"] = False
    if dashboard_standalone and external_bot_running:
        for key in (
            "phase1_data_feeds",
            "phase2_whale_intel",
            "phase3_order_flow",
            "phase7_intel_alerts",
        ):
            phase_health[key] = True
    return phase_health


def _process_match_count(*needles: str) -> int:
    try:
        import psutil
    except Exception:
        return 0
    wanted = [str(item or "").strip().lower() for item in needles if str(item or "").strip()]
    if not wanted:
        return 0
    count = 0
    for proc in psutil.process_iter(attrs=["name", "cmdline"]):
        try:
            cmdline = " ".join(proc.info.get("cmdline") or [])
            name = str(proc.info.get("name") or "")
            haystack = f"{name} {cmdline}".lower()
        except Exception:
            continue
        if all(token in haystack for token in wanted):
            count += 1
    return count


def _external_trading_bot_process_count() -> int:
    count = 0
    try:
        import psutil
    except Exception:
        return 0
    for proc in psutil.process_iter(attrs=["name", "cmdline"]):
        try:
            cmdline = " ".join(proc.info.get("cmdline") or [])
            haystack = f"{proc.info.get('name') or ''} {cmdline}".lower()
        except Exception:
            continue
        if "bot.py" not in haystack:
            continue
        if "dashboard.web_app_live" in haystack:
            continue
        count += 1
    return count


def _collect_runtime_service_details() -> Dict[str, Any]:
    services: Dict[str, Any] = {}
    try:
        from services.deriv_bridge import deriv_bridge

        deriv_profiles = deriv_bridge.list_profiles()
        services["deriv_public_data"] = {
            "ok": bool(deriv_profiles),
            "state": "connected" if deriv_profiles else "disabled",
            "meta": f"{len(deriv_profiles)} profile(s)" if deriv_profiles else "no profile",
        }
    except Exception:
        services["deriv_public_data"] = {"ok": False, "state": "error", "meta": "bridge unavailable"}

    try:
        from services.binance_market_bridge import binance_market_bridge

        binance_profiles = binance_market_bridge.list_profiles()
        services["binance_public_data"] = {
            "ok": bool(binance_profiles),
            "state": "ready" if binance_profiles else "disabled",
            "meta": f"{len(binance_profiles)} profile(s)" if binance_profiles else "no profile",
        }
    except Exception:
        services["binance_public_data"] = {"ok": False, "state": "error", "meta": "bridge unavailable"}

    try:
        from services.ig_market_bridge import ig_market_bridge

        ig_profiles = ig_market_bridge.list_profiles()
        account = ig_market_bridge.get_account_summary() if ig_profiles else {}
        authenticated = bool(account.get("authenticated", False))
        services["ig_routed_data"] = {
            "ok": bool(ig_profiles) and authenticated,
            "state": "authenticated" if authenticated else ("configured" if ig_profiles else "disabled"),
            "meta": str(account.get("environment") or "").upper() or ("profile ready" if ig_profiles else "no profile"),
        }
    except Exception:
        services["ig_routed_data"] = {"ok": False, "state": "error", "meta": "bridge unavailable"}

    try:
        from services.ctrader_live_depth_bridge import ctrader_live_depth_bridge

        status = ctrader_live_depth_bridge.status() if CTRADER_LIVE_DEPTH_ENABLED else {}
        assets = list(status.get("assets") or [])
        healthy = bool(status.get("healthy", False))
        state = str(status.get("state") or ("configured" if status.get("enabled") else "disabled"))
        snapshot_age = status.get("last_snapshot_age_seconds")
        meta_parts = [f"{len(assets)} assets" if assets else str(status.get("store_path") or "no assets")]
        if snapshot_age not in (None, ""):
            meta_parts.append(f"age {float(snapshot_age):.0f}s")
        if status.get("environment"):
            meta_parts.append(str(status.get("environment")).upper())
        services["ctrader_live_depth"] = {
            "ok": healthy,
            "state": state,
            "meta": " | ".join(part for part in meta_parts if part),
            "market_quiet": bool(status.get("market_quiet")),
            "quiet_stale": bool(status.get("quiet_stale")),
            "market_open_assets": list(status.get("market_open_assets") or []),
            "market_closed_assets": list(status.get("market_closed_assets") or []),
        }
    except Exception:
        services["ctrader_live_depth"] = {"ok": False, "state": "error", "meta": "bridge unavailable"}

    try:
        from services.dukascopy_live_depth_bridge import dukascopy_live_depth_bridge

        status = dukascopy_live_depth_bridge.status() if DUKASCOPY_LIVE_DEPTH_ENABLED else {}
        assets = list(status.get("assets") or [])
        healthy = bool(status.get("healthy", False))
        state = str(status.get("state") or ("configured" if status.get("enabled") else "disabled"))
        snapshot_age = status.get("last_snapshot_age_seconds")
        meta_parts = [f"{len(assets)} assets" if assets else str(status.get("store_path") or "no assets")]
        if snapshot_age not in (None, ""):
            meta_parts.append(f"age {float(snapshot_age):.0f}s")
        services["dukascopy_live_depth"] = {
            "ok": healthy,
            "state": state,
            "meta": " | ".join(part for part in meta_parts if part),
            "market_quiet": bool(status.get("market_quiet")),
            "quiet_stale": bool(status.get("quiet_stale")),
            "market_open_assets": list(status.get("market_open_assets") or []),
            "market_closed_assets": list(status.get("market_closed_assets") or []),
        }
    except Exception:
        services["dukascopy_live_depth"] = {"ok": False, "state": "error", "meta": "bridge unavailable"}

    services["dukascopy_history"] = {
        "ok": bool(DUKASCOPY_HISTORY_ENABLED),
        "state": "enabled" if DUKASCOPY_HISTORY_ENABLED else "disabled",
        "meta": "backfill source",
    }
    services["local_candle_store"] = {
        "ok": bool(LOCAL_CANDLE_STORE_ENABLED),
        "state": "enabled" if LOCAL_CANDLE_STORE_ENABLED else "disabled",
        "meta": "live/history spine",
    }
    deepseek_direct = _process_match_count("deepseek_bot.py")
    deepseek_bot_count = max(deepseek_direct, 1 if _process_match_count("bot.py") > 1 else 0)
    services["deepseek_bot"] = {
        "ok": bool(deepseek_bot_count),
        "state": "running" if deepseek_bot_count else ("configured" if DEEPSEEK_TELEGRAM_TOKEN else "disabled"),
        "meta": f"{deepseek_bot_count} proc" if deepseek_bot_count else "telegram token missing",
    }
    services["deepseek_api"] = {
        "ok": bool(DEEPSEEK_API_KEY),
        "state": str(ROBBIE_CHAT_PROVIDER or "auto"),
        "meta": "api key loaded" if DEEPSEEK_API_KEY else "api key missing",
    }
    services["command_telegram"] = {
        "ok": bool(getattr(telegram_manager, "is_running", False)),
        "state": "running" if bool(getattr(telegram_manager, "is_running", False)) else "stopped",
        "meta": "command bot",
    }
    try:
        from config.config import ROBBIE_SCHEDULER_ENABLED
        from services.robbie_schedule_service import get_schedule_service

        scheduler_status = get_schedule_service().status() if ROBBIE_SCHEDULER_ENABLED else {}
        next_run = str(scheduler_status.get("next_run_display") or "").strip()
        schedule_count = int(scheduler_status.get("count", 0) or 0)
        meta = f"{schedule_count} schedule(s)"
        if next_run:
            meta += f" · next {next_run}"
        services["robbie_scheduler"] = {
            "ok": bool(ROBBIE_SCHEDULER_ENABLED),
            "state": "active" if ROBBIE_SCHEDULER_ENABLED else "disabled",
            "meta": meta,
        }
    except Exception:
        services["robbie_scheduler"] = {"ok": False, "state": "error", "meta": "scheduler unavailable"}
    services["monitoring_service"] = {
        "ok": True,
        "state": "active",
        "meta": "health telemetry",
    }
    return services


def _collect_ig_broker_snapshot() -> Dict[str, Any]:
    cached = _cache_get("ig_broker_snapshot:v1")
    if cached is not None:
        payload = _response_to_dict(cached)
        return dict(payload or {}) if isinstance(payload, dict) else {}
    try:
        from services.ig_market_bridge import ig_market_bridge

        payload = dict(ig_market_bridge.get_account_summary() or {})
    except Exception as exc:
        payload = {
            "enabled": True,
            "authenticated": False,
            "provider": "IG",
            "error_code": "summary_failed",
            "error_message": str(exc),
        }
    _cache_set("ig_broker_snapshot:v1", payload, ttl=30)
    return payload


def _collect_performance_guard_snapshot() -> Dict[str, Any]:
    cache_key = "performance_guard_snapshot:v1"
    cached = _cache_get(cache_key)
    if cached is not None:
        return _response_to_dict(cached)

    try:
        from services.execution_feedback_service import get_service as get_execution_feedback_service
        from services.db_pool import get_db
    except Exception:
        return {"portfolio_7d": {}, "portfolio_14d": {}, "worst_assets": [], "worst_playbooks": [], "worst_sessions": []}

    service = get_execution_feedback_service()
    portfolio_7d = service.summarize_context(days_back=7, limit=400)
    portfolio_14d = service.summarize_context(days_back=14, limit=700)
    try:
        rows = get_db().get_execution_feedback_trades(
            since=datetime.utcnow() - timedelta(days=14),
            asset="",
            category="",
            limit=700,
        )
    except Exception:
        rows = []

    assets: Dict[str, Dict[str, Any]] = {}
    playbooks: Dict[str, Dict[str, Any]] = {}
    sessions: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        raw_meta = row.get("metadata") or row.get("trade_metadata") or {}
        if isinstance(raw_meta, dict):
            meta = dict(raw_meta)
        else:
            try:
                meta = json.loads(raw_meta or "{}")
                if not isinstance(meta, dict):
                    meta = {}
            except Exception:
                meta = {}
        pnl = float(row.get("pnl", 0.0) or 0.0)
        keys = {
            "asset": str(row.get("canonical_asset") or row.get("asset") or "").strip(),
            "playbook": str(meta.get("playbook_name") or meta.get("seed_model") or "").strip().lower(),
            "session": str(meta.get("session_label") or meta.get("playbook_session") or meta.get("session") or "").strip().lower(),
        }
        for bucket_name, key in keys.items():
            if not key:
                continue
            bucket = {"asset": assets, "playbook": playbooks, "session": sessions}[bucket_name]
            entry = bucket.setdefault(key, {"key": key, "sample_count": 0, "pnl_total": 0.0, "wins": 0, "losses": 0})
            entry["sample_count"] += 1
            entry["pnl_total"] += pnl
            if pnl > 0:
                entry["wins"] += 1
            elif pnl < 0:
                entry["losses"] += 1

    def _top_losers(bucket: Dict[str, Dict[str, Any]], top: int = 4) -> List[Dict[str, Any]]:
        rows = sorted(bucket.values(), key=lambda item: (float(item["pnl_total"]), -int(item["sample_count"])))
        return [
            {
                "key": str(item["key"]),
                "sample_count": int(item["sample_count"]),
                "pnl_total": round(float(item["pnl_total"]), 2),
                "win_rate": round(
                    (float(item["wins"]) / max(1, int(item["sample_count"]))),
                    4,
                ),
            }
            for item in rows[:top]
        ]

    payload = {
        "portfolio_7d": portfolio_7d,
        "portfolio_14d": portfolio_14d,
        "worst_assets": _top_losers(assets),
        "worst_playbooks": _top_losers(playbooks),
        "worst_sessions": _top_losers(sessions),
    }
    _cache_set(cache_key, payload, ttl=_PERFORMANCE_GUARD_CACHE_TTL)
    return payload


def _collect_system_feed_connections() -> Dict[str, Any]:
    try:
        from websocket_dashboard import connection_status as _connection_status

        feed_connections = copy.deepcopy(dict(_connection_status or {}))
    except Exception:
        feed_connections = {}
    return feed_connections


def _collect_system_health_snapshot(core: Any, health: Dict[str, Any]) -> Dict[str, Any]:
    ram_pct, cpu_pct, disk_pct, proc_mb = _collect_system_resource_stats()
    redis_ok = _collect_redis_health()
    db_ok = _collect_database_health()
    runtime_services = _collect_runtime_service_details()
    dashboard_standalone = core is None
    external_bot_processes = _external_trading_bot_process_count()
    external_bot_running = external_bot_processes > 0
    telegram_local_ok = bool(getattr(telegram_manager, "is_running", False))
    tg_ok = telegram_local_ok or (dashboard_standalone and external_bot_running)
    if dashboard_standalone and external_bot_running and not bool((runtime_services.get("command_telegram") or {}).get("ok")):
        runtime_services["command_telegram"] = {
            "ok": True,
            "state": "bot-owned",
            "meta": "forex-bot service",
        }
    core_running = bool(health.get("is_running", getattr(core, "is_running", False) if core else False))
    core_ready = bool(health.get("engine_ready", getattr(core, "is_ready", False) if core else False))
    trading_engine_ok = core_running or (dashboard_standalone and external_bot_running)
    engine_ready_ok = core_ready or (dashboard_standalone and external_bot_running)
    processes = {
        "TradingCore": trading_engine_ok,
        "Engine ready": engine_ready_ok,
        "Web dashboard": True,
        "Redis": redis_ok,
        "PostgreSQL": db_ok,
        "Telegram": tg_ok,
        "PredTracker": _pred_tracker is not None,
        "WebSocket streams": _ws_ok,
        "DeepSeek Bot": bool((runtime_services.get("deepseek_bot") or {}).get("ok")),
        "cTrader Sidecar": bool((runtime_services.get("ctrader_live_depth") or {}).get("ok")),
        "Dukascopy Sidecar": bool((runtime_services.get("dukascopy_live_depth") or {}).get("ok")),
    }
    process_details = {
        "TradingCore": {
            "ok": trading_engine_ok,
            "state": "Running" if core_running else ("External" if external_bot_running else "Stopped"),
            "meta": "in dashboard process" if core_running else (f"forex-bot process x{external_bot_processes}" if external_bot_running else ""),
        },
        "Engine ready": {
            "ok": engine_ready_ok,
            "state": "Ready" if core_ready else ("Bot-owned" if external_bot_running else "Stopped"),
            "meta": "read through separate forex-bot service" if dashboard_standalone and external_bot_running else "",
        },
        "Telegram": {
            "ok": tg_ok,
            "state": "Running" if telegram_local_ok else ("Bot-owned" if dashboard_standalone and external_bot_running else "Stopped"),
            "meta": "read through separate forex-bot service" if dashboard_standalone and external_bot_running and not telegram_local_ok else "",
        },
    }
    return {
        "dashboard_standalone": dashboard_standalone,
        "process_model": "split" if dashboard_standalone and external_bot_running else ("embedded" if core else "standalone"),
        "external_bot_processes": external_bot_processes,
        "ram_pct": round(ram_pct, 1),
        "cpu_pct": round(cpu_pct, 1),
        "disk_pct": round(disk_pct, 1),
        "process_mem_mb": proc_mb,
        "processes": processes,
        "process_details": process_details,
        "phase_health": _collect_system_phase_health(
            external_bot_running=external_bot_running,
            dashboard_standalone=dashboard_standalone,
        ),
        "runtime_services": runtime_services,
        "feed_connections": _collect_system_feed_connections(),
        "performance_guard": _collect_performance_guard_snapshot(),
    }


def _source_health_with_market_quiet(
    source_health: Any,
    stale_sources: Any,
    runtime_services: Dict[str, Any],
) -> Tuple[Dict[str, Any], List[str]]:
    health_map = {str(k): dict(v or {}) for k, v in dict(source_health or {}).items()}
    stale = [str(item) for item in list(stale_sources or []) if str(item)]
    for source in ("ctrader_live_depth", "dukascopy_live_depth"):
        service = dict((runtime_services or {}).get(source) or {})
        if not bool(service.get("market_quiet")):
            continue
        row = dict(health_map.get(source) or {})
        row.update(
            {
                "status": "market_quiet",
                "fresh": True,
                "suppressed": True,
                "market_quiet": True,
                "quiet_stale": bool(service.get("quiet_stale")),
                "threshold": row.get("threshold", 30),
            }
        )
        health_map[source] = row
        stale = [item for item in stale if item != source]
    return health_map, stale


def _source_health_with_split_ownership(
    source_health: Any,
    stale_sources: Any,
    *,
    external_bot_running: bool,
    telegram_ok: bool,
) -> Tuple[Dict[str, Any], List[str]]:
    health_map = {str(k): dict(v or {}) for k, v in dict(source_health or {}).items()}
    stale = [str(item) for item in list(stale_sources or []) if str(item)]

    def mark_bot_owned(source: str, label: str = "Owned by forex-bot") -> None:
        row = dict(health_map.get(source) or {})
        row.update(
            {
                "status": "bot-owned",
                "fresh": True,
                "suppressed": True,
                "bot_owned": True,
                "age_secs": row.get("age_secs"),
                "display_age": label,
                "display_threshold": "Split service",
            }
        )
        health_map[source] = row

    if external_bot_running:
        for source in ("order_book", "liquidations", "funding_rate", "open_interest"):
            row = dict(health_map.get(source) or {})
            if not row or str(row.get("status") or "") not in {"fresh", "market_quiet"}:
                mark_bot_owned(source)
        if telegram_ok:
            mark_bot_owned("telegram_alerts", "Command bot owned by forex-bot")

    stale = [item for item in stale if not bool((health_map.get(item) or {}).get("bot_owned"))]
    return health_map, stale


@app.route("/api/system/health")
def api_system_health():
    cached = _cache_get("system_health:v5")
    if cached is not None:
        return jsonify(cached)
    try:
        core   = _core()
        if core:
            health = core.health_report()
        else:
            perf, _daily, positions, health, _closed_trades = _load_dashboard_db_runtime_snapshot()
            health = dict(health or {})
            health.update(
                {
                    "open_positions": len(positions),
                    "active_cooldowns": int(health.get("active_cooldowns", 0) or 0),
                    "strategy_mode": "split-dashboard",
                    "balance": float(perf.get("balance", _args.balance) or _args.balance),
                    "issues": list(health.get("issues") or []),
                    "recent_error_count": int(health.get("recent_error_count", 0) or 0),
                    "recent_errors": list(health.get("recent_errors") or []),
                }
            )
        snapshot = _collect_system_health_snapshot(core, health)
        source_health, stale_sources = _source_health_with_market_quiet(
            health.get("source_health") or {},
            health.get("stale_sources") or [],
            snapshot.get("runtime_services") or {},
        )
        source_health, stale_sources = _source_health_with_split_ownership(
            source_health,
            stale_sources,
            external_bot_running=bool(snapshot.get("external_bot_processes")),
            telegram_ok=bool((snapshot.get("processes") or {}).get("Telegram")),
        )

        payload = {
            "success":          True,
            **snapshot,
            "open_positions":   health.get("open_positions", 0),
            "active_cooldowns": health.get("active_cooldowns", 0),
            "source_health":    source_health,
            "stale_sources":    stale_sources,
            "stale_source_count": len(stale_sources),
            "never_seen_sources": list(health.get("never_seen_sources") or []),
            "never_seen_source_count": int(health.get("never_seen_source_count", 0) or 0),
            "ig_broker":       dict(health.get("ig_broker") or _collect_ig_broker_snapshot()),
            "recent_error_count": int(health.get("recent_error_count", 0) or 0),
            "recent_errors":    list(health.get("recent_errors") or []),
            "issues":           health.get("issues", []),
            "strategy_mode":    health.get("strategy_mode", "—"),
            "balance":          health.get("balance", _args.balance),
            "timestamp":        datetime.now().isoformat(),
        }
        payload = _attach_authoritative_account_fields(payload, _authoritative_account_summary(core))
        _cache_set("system_health:v5", payload, ttl=5)
        return jsonify(payload)
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route("/api/monitoring/snapshot")
def api_monitoring_snapshot():
    cached = _cache_get("monitoring_snapshot")
    if cached: return jsonify(cached)
    try:
        from monitoring.system_health_service import monitor
        snap    = monitor.get_snapshot()
        payload = {"success": True, **snap}
        _cache_set("monitoring_snapshot", payload, ttl=60)
        return jsonify(payload)
    except Exception as e:
        try:
            raw = _redis_broker.get("monitoring:latest") if _redis_broker else None
            if raw:
                data = json.loads(raw) if isinstance(raw, (str, bytes)) else raw
                return jsonify({"success": True, **data})
        except Exception:
            pass
        return jsonify({"success": False, "error": str(e)})

@app.route("/api/monitoring/metrics")
def api_monitoring_metrics():
    cached = _cache_get("monitoring_metrics")
    if cached: return jsonify(cached)
    try:
        from monitoring.metrics import metrics
        payload = {"success": True, "metrics": metrics.summary(),
                   "timestamp": datetime.now().isoformat()}
        _cache_set("monitoring_metrics", payload, ttl=30)
        return jsonify(payload)
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

@app.route("/api/monitoring/errors")
def api_monitoring_errors():
    cached = _cache_get("monitoring_errors")
    if cached is not None:
        return jsonify(cached)
    try:
        from monitoring.system_health_service import monitor
        snap = monitor.get_snapshot()
        payload = {"success": True, "errors": snap.get("errors", {}),
                        "timestamp": datetime.now().isoformat()}
        _cache_set("monitoring_errors", payload, ttl=10)
        return jsonify(payload)
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

@app.route("/api/system-monitor/overview")
def api_system_monitor_overview():
    cache_key = "system_monitor_overview:v5"
    cached = _cache_get(cache_key)
    if cached:
        return jsonify(cached)

    health_resp = _call_view(api_system_health)
    metrics_resp = _call_view(api_monitoring_metrics)
    errors_resp = _call_view(api_monitoring_errors)
    snapshot_resp = _call_view(api_monitoring_snapshot)

    health_data = health_resp.get_json() if hasattr(health_resp, 'get_json') else json.loads(health_resp.get_data(as_text=True))
    metrics_data = metrics_resp.get_json() if hasattr(metrics_resp, 'get_json') else json.loads(metrics_resp.get_data(as_text=True))
    errors_data = errors_resp.get_json() if hasattr(errors_resp, 'get_json') else json.loads(errors_resp.get_data(as_text=True))
    snapshot_data = snapshot_resp.get_json() if hasattr(snapshot_resp, 'get_json') else json.loads(snapshot_resp.get_data(as_text=True))

    payload = {
        "success": True,
        "health": health_data,
        "metrics": metrics_data.get("metrics", {}),
        "errors": errors_data.get("errors", {}),
        "snapshot": {
            key: value for key, value in (snapshot_data or {}).items()
            if key != "success"
        },
        "timestamp": datetime.utcnow().isoformat(),
    }
    payload = _attach_authoritative_account_fields(payload)
    _cache_set(cache_key, payload, ttl=5)
    return jsonify(payload)


_PAGE_OVERVIEW_VALID_PAGES = {
    "architecture_lab",
    "command_center",
    "intelligence_alerts",
    "market_intelligence",
    "order_flow",
    "playbook_intel",
    "risk_dashboard",
    "sentiment_intelligence",
    "system_monitor",
    "whale_intelligence",
}


def _normalize_page_overview_name(page: str) -> str:
    normalized = str(page or "").strip().lower().replace("-", "_")
    if normalized == "ai_predictions":
        return "playbook_intel"
    return normalized


def _page_overview_status_shell() -> Dict[str, Any]:
    cached = _cache_get("status:v4") or _cache_get("status")
    if cached:
        return _attach_authoritative_account_fields(_response_to_dict(cached))
    core = _core()
    external_bot_processes = 0 if core else _external_trading_bot_process_count()
    external_bot_running = external_bot_processes > 0
    return _attach_authoritative_account_fields({
        "success": True,
        "engine_running": bool(getattr(core, "is_running", False)) if core else external_bot_running,
        "engine_ready": bool(getattr(core, "is_ready", False)) if core else external_bot_running,
        "dashboard_standalone": core is None,
        "process_model": "split" if external_bot_running else ("embedded" if core else "standalone"),
        "external_bot_processes": external_bot_processes,
        "provider_routing": {},
        "signal_diagnostics": {},
    }, _authoritative_account_summary(core))


def _page_overview_command_center_shell(reason: str = "page_overview_cache_miss") -> Dict[str, Any]:
    cached = _cache_get(_COMMAND_CENTER_PAYLOAD_CACHE_KEY)
    used_last_good = False
    if cached is None:
        cached = _cache_get(_COMMAND_CENTER_PAYLOAD_LAST_GOOD_KEY)
        used_last_good = cached is not None
    if cached:
        payload = _mark_stale_dashboard_payload(cached, "command_center_stale") if used_last_good else _response_to_dict(cached)
        return _compact_command_center_for_page_overview(payload)
    return _compact_command_center_for_page_overview(_build_command_center_unavailable_payload(reason=reason))


def _page_overview_base(page: str, days: int, *, reason: str) -> Dict[str, Any]:
    return {
        "success": True,
        "page": page,
        "days": days,
        "degraded": True,
        "degraded_reason": reason,
        "timestamp": datetime.utcnow().isoformat(),
    }


def _build_page_overview_unavailable_payload(page: str, days: int, reason: str = "overview_unavailable") -> Dict[str, Any]:
    status = _page_overview_status_shell()
    command_center = _page_overview_command_center_shell(reason)
    payload = _page_overview_base(page, days, reason=reason)
    payload["trade_history_summary"] = _page_overview_trade_history_summary(limit=50)

    if page == "risk_dashboard":
        payload.update({"status": status, "risk": {"success": True}, "command_center": command_center})
    elif page == "playbook_intel":
        payload.update(
            {
                "status": status,
                "command_center": command_center,
                "signals": [],
                "accuracy": {"by_horizon": {}, "by_asset": {}, "recent": [], "days_back": days},
                "live_quality": {},
                "live_leaders": {},
                "playbook_performance": {"summary": {}, "playbooks": [], "assets": []},
                "asset_playbook_matrix": [],
                "near_misses": list(command_center.get("near_misses") or []),
            }
        )
    elif page == "intelligence_alerts":
        payload.update({"status": status, "alerts": [], "journals": [], "command_center": command_center})
    elif page == "system_monitor":
        payload.update({"health": {}, "metrics": {}, "errors": {}, "snapshot": {}, "command_center": command_center})
    elif page == "whale_intelligence":
        payload.update(
            {
                "status": status,
                "command_center": command_center,
                "alerts": [],
                "total_volume_usd": 0,
                "top_assets": [],
                "recent": [],
                "alert_count_24h": 0,
                "whale_alerts_24h": 0,
            }
        )
    elif page == "sentiment_intelligence":
        payload.update(
            {
                "status": status,
                "sentiment": {"success": True, "components": {}, "score": 0.0},
                "by_asset": _neutral_sentiment_by_asset_payload(reason),
                "events": {"success": True, "events": []},
                "heatmap": {"success": True, "items": []},
                "command_center": command_center,
            }
        )
    elif page == "order_flow":
        payload.update(
            {
                "status": status,
                "imbalance": {"success": True, "imbalances": {}, "timestamp": datetime.utcnow().isoformat()},
                "walls": {"success": True, "walls": [], "count": 0},
                "hunts": {"success": True, "hunts": []},
                "depth": {"success": True, "rows": [], "count": 0},
                "command_center": command_center,
            }
        )
    elif page == "command_center":
        payload.update(
            {
                "command_center": command_center,
                "whale": dict(command_center.get("whale") or {}),
                "sentiment_context": dict(command_center.get("sentiment_context") or {}),
            }
        )
    elif page == "market_intelligence":
        payload.update(
            {
                "status": status,
                "assets": {"success": True, "assets": [_chart_asset_descriptor(a, c) for a, c in ALL_ASSETS]},
                "events": {"success": True, "events": []},
                "command_center": command_center,
            }
        )
    elif page == "architecture_lab":
        payload.update({"status": status, "health": {}, "system_monitor": {}, "command_center": command_center})
    return payload


def _page_overview_cached_component(
    cache_key: str,
    fallback: Dict[str, Any],
    *,
    builder=None,
    ttl: int = 60,
    last_good_ttl: int = _PAGE_OVERVIEW_LAST_GOOD_TTL,
) -> Dict[str, Any]:
    cached = _cache_get(cache_key)
    if cached is not None:
        return _response_to_dict(cached)
    last_good_key = f"{cache_key}:last_good"
    last_good = _cache_get(last_good_key)
    if last_good is not None:
        if builder is not None:
            try:
                payload = _response_to_dict(builder())
                degraded = _is_degraded_dashboard_payload(payload)
                _cache_set(cache_key, payload, ttl=min(ttl, _PAGE_OVERVIEW_DEGRADED_CACHE_TTL) if degraded else ttl)
                if not degraded:
                    _cache_set(last_good_key, payload, ttl=last_good_ttl)
                    return payload
            except Exception as exc:
                logger.debug(f"[dashboard] page component {cache_key} stale refresh failed: {exc}")
            _trigger_dashboard_payload_refresh(
                cache_key,
                builder=builder,
                cache_key=cache_key,
                ttl=ttl,
                last_good_key=last_good_key,
                last_good_ttl=last_good_ttl,
            )
        return _mark_stale_dashboard_payload(last_good, "component_stale")
    if builder is not None:
        try:
            payload = _response_to_dict(builder())
            degraded = _is_degraded_dashboard_payload(payload)
            _cache_set(cache_key, payload, ttl=min(ttl, _PAGE_OVERVIEW_DEGRADED_CACHE_TTL) if degraded else ttl)
            if not degraded:
                _cache_set(last_good_key, payload, ttl=last_good_ttl)
            return payload
        except Exception as exc:
            logger.debug(f"[dashboard] page component {cache_key} first build failed: {exc}")
        _trigger_dashboard_payload_refresh(
            cache_key,
            builder=builder,
            cache_key=cache_key,
            ttl=ttl,
            last_good_key=last_good_key,
            last_good_ttl=last_good_ttl,
        )
    payload = copy.deepcopy(fallback)
    if isinstance(payload, dict):
        payload.setdefault("success", True)
        payload["degraded"] = True
        payload.setdefault("degraded_reason", "component_warming")
    return payload


def _page_overview_view_builder(path: str, view_fn):
    def _builder() -> Dict[str, Any]:
        with app.test_request_context(path):
            payload = _response_to_dict(_call_view(view_fn))
        if not isinstance(payload, dict):
            raise RuntimeError(f"{path} did not return a JSON object")
        if payload.get("success") is False:
            raise RuntimeError(str(payload.get("error") or f"{path} returned success=false"))
        return payload

    return _builder


def _page_overview_view_component(
    cache_key: str,
    path: str,
    view_fn,
    fallback: Dict[str, Any],
    *,
    ttl: int = 60,
) -> Dict[str, Any]:
    return _page_overview_cached_component(
        cache_key,
        fallback,
        builder=_page_overview_view_builder(path, view_fn),
        ttl=ttl,
    )


def _page_overview_trade_history_summary(*, limit: int = 50) -> Dict[str, Any]:
    target = max(1, int(limit or 50))
    cache_key = f"page_component:v4:trade_history_summary:{target}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return dict(cached) if isinstance(cached, dict) else {}
    raw_limit = max(target * 3, target + 10)
    trades = _rollup_closed_trades_for_dashboard(_load_authoritative_closed_trades(limit=raw_limit), limit=target)
    summary = _build_authoritative_trade_history_summary(trades)
    _cache_set(cache_key, summary, ttl=min(_TRADE_HISTORY_CACHE_TTL, 60))
    return dict(summary)


def _build_page_overview_payload(page: str, days: int, *, force_refresh: bool = False) -> Dict[str, Any]:
    page = _normalize_page_overview_name(page)

    def _command_center_snapshot() -> Dict[str, Any]:
        return _page_overview_command_center_shell("page_overview_cache_miss")

    fallback_payload = _build_page_overview_unavailable_payload(page, days, reason="component_warming")

    if page == "risk_dashboard":
        payload = {
            "success": True,
            "page": page,
            "status": _page_overview_view_component("page_component:v6:status", "/api/status", api_status, fallback_payload.get("status", {}), ttl=10),
            "risk": _page_overview_view_component("page_component:v4:risk_portfolio", "/api/risk/portfolio", api_risk_portfolio, fallback_payload.get("risk", {"success": True}), ttl=15),
            "command_center": _command_center_snapshot(),
        }
    elif page == "playbook_intel":
        payload = _page_overview_view_component(
            f"page_component:v4:playbook_intel:{days}",
            f"/api/playbook-intel/overview?days={days}",
            api_ai_predictions_overview,
            {
                "success": True,
                "signals": [],
                "accuracy": {"by_horizon": {}, "by_asset": {}, "recent": [], "days_back": days},
                "live_quality": {},
                "live_leaders": {},
                "playbook_performance": {"summary": {}, "playbooks": [], "assets": []},
                "asset_playbook_matrix": [],
                "near_misses": [],
            },
            ttl=30,
        )
        payload["page"] = page
        payload["status"] = _page_overview_view_component("page_component:v6:status", "/api/status", api_status, fallback_payload.get("status", {}), ttl=10)
        payload["command_center"] = _command_center_snapshot()
        if not payload.get("near_misses"):
            payload["near_misses"] = list(payload["command_center"].get("near_misses") or [])
    elif page == "intelligence_alerts":
        payload = _page_overview_view_component(
            "page_component:v5:intelligence_alerts",
            "/api/intelligence-alerts/overview",
            api_intelligence_alerts_overview,
            {"success": True, "alerts": [], "journals": []},
            ttl=15,
        )
        payload["page"] = page
        payload["status"] = _page_overview_view_component("page_component:v6:status", "/api/status", api_status, fallback_payload.get("status", {}), ttl=10)
        payload["command_center"] = _command_center_snapshot()
    elif page == "system_monitor":
        payload = _page_overview_view_component(
            "page_component:v5:system_monitor",
            "/api/system-monitor/overview",
            api_system_monitor_overview,
            {"success": True, "health": {}, "metrics": {}, "errors": {}, "snapshot": {}},
            ttl=10,
        )
        payload["page"] = page
        payload["status"] = _page_overview_view_component("page_component:v6:status", "/api/status", api_status, fallback_payload.get("status", {}), ttl=10)
        payload["command_center"] = _command_center_snapshot()
    elif page == "whale_intelligence":
        payload = _page_overview_view_component(
            "page_component:v4:whale_summary",
            "/api/whale/summary",
            api_whale_summary,
            fallback_payload,
            ttl=15,
        )
        payload["page"] = page
        payload["status"] = _page_overview_view_component("page_component:v6:status", "/api/status", api_status, fallback_payload.get("status", {}), ttl=10)
        payload["command_center"] = _command_center_snapshot()
    elif page == "sentiment_intelligence":
        sentiment_news_disabled = not (NEWS_SENTIMENT_ENABLED or NEWS_REDDIT_ENABLED or NEWS_RSS_ENABLED)
        sentiment_fallback_reason = "sentiment_news_disabled" if sentiment_news_disabled else "sentiment_cache_warming"
        payload = {
            "success": True,
            "page": page,
            "status": _response_to_dict(_call_view(api_status)),
            "sentiment": _page_overview_cached_component(
                "sentiment_dashboard",
                {
                    **_default_sentiment_dashboard_result(),
                    "degraded": True,
                    "degraded_reason": sentiment_fallback_reason,
                },
                builder=_page_overview_view_builder("/api/sentiment/dashboard", api_sentiment_dashboard),
                ttl=600,
            ),
            "by_asset": _page_overview_cached_component(
                _SENTIMENT_BY_ASSET_CACHE_KEY,
                _neutral_sentiment_by_asset_payload(sentiment_fallback_reason),
                builder=_page_overview_view_builder("/api/sentiment/by-asset", api_sentiment_by_asset),
                ttl=600,
            ),
            "events": _page_overview_cached_component(
                "market_events:v1",
                {"success": True, "events": [], "earnings": [], "halving": {}, "risk_outlook": {}},
                builder=_page_overview_view_builder("/api/market/events", api_market_events),
                ttl=60,
            ),
            "heatmap": _page_overview_cached_component(
                "heatmap:v3",
                {"success": True, "items": [], "expected_assets": len(ALL_ASSETS), "partial": True},
                builder=_page_overview_view_builder("/api/market/heatmap", api_market_heatmap),
                ttl=15,
            ),
            "command_center": _command_center_snapshot(),
        }
    elif page == "order_flow":
        payload = {
            "success": True,
            "page": page,
            "status": _page_overview_view_component("page_component:v6:status", "/api/status", api_status, fallback_payload.get("status", {}), ttl=10),
            "imbalance": _page_overview_view_component("page_component:v4:phase3_imbalance", "/api/phase3/imbalance", api_phase3_imbalance, fallback_payload.get("imbalance", {"success": True, "imbalances": {}}), ttl=15),
            "walls": _page_overview_view_component("page_component:v4:phase3_walls", "/api/phase3/walls", api_phase3_walls, fallback_payload.get("walls", {"success": True, "walls": [], "count": 0}), ttl=5),
            "hunts": _page_overview_view_component("page_component:v4:phase3_stop_hunts", "/api/phase3/stop-hunts", api_phase3_stop_hunts, fallback_payload.get("hunts", {"success": True, "hunts": []}), ttl=5),
            "depth": _page_overview_view_component("page_component:v4:phase3_live_depth", "/api/phase3/live-depth", api_phase3_live_depth, fallback_payload.get("depth", {"success": True, "rows": [], "count": 0}), ttl=5),
            "command_center": _command_center_snapshot(),
        }
    elif page == "command_center":
        command_center = _command_center_snapshot()
        payload = {
            "success": True,
            "page": page,
            "command_center": command_center,
            "whale": dict(command_center.get("whale") or {}),
            "sentiment_context": dict(command_center.get("sentiment_context") or {}),
        }
    elif page == "market_intelligence":
        payload = {
            "success": True,
            "page": page,
            "status": _page_overview_view_component("page_component:v6:status", "/api/status", api_status, fallback_payload.get("status", {}), ttl=10),
            "assets": _page_overview_view_component("page_component:v4:chart_assets", "/api/chart/assets", api_chart_assets, fallback_payload.get("assets", {"success": True, "assets": [_chart_asset_descriptor(a, c) for a, c in ALL_ASSETS]}), ttl=300),
            "events": _page_overview_view_component("page_component:v4:market_events", "/api/market/events", api_market_events, fallback_payload.get("events", {"success": True, "events": []}), ttl=60),
            "command_center": _command_center_snapshot(),
        }
    elif page == "architecture_lab":
        payload = {
            "success": True,
            "page": page,
            "status": _page_overview_view_component("page_component:v6:status", "/api/status", api_status, fallback_payload.get("status", {}), ttl=10),
            "health": _page_overview_view_component("page_component:v5:system_health", "/api/system/health", api_system_health, fallback_payload.get("health", {}), ttl=10),
            "system_monitor": _page_overview_view_component("page_component:v5:system_monitor", "/api/system-monitor/overview", api_system_monitor_overview, fallback_payload.get("system_monitor", {}), ttl=10),
            "command_center": _command_center_snapshot(),
        }
    else:
        raise BadRequest(f"Unknown overview page '{page}'")

    payload = _response_to_dict(payload)
    payload.setdefault("success", True)
    payload.setdefault("page", page)
    payload.setdefault("days", days)
    payload.setdefault("trade_history_summary", _page_overview_trade_history_summary(limit=50))
    payload.setdefault("timestamp", datetime.utcnow().isoformat())
    if _is_degraded_dashboard_payload(payload):
        payload["degraded"] = True
        payload.setdefault("degraded_reason", "component_warming")
    return payload


def _build_page_overview_payload_in_context(page: str, days: int) -> Dict[str, Any]:
    with app.test_request_context(f"/api/page-overview?page={page}&days={days}"):
        return _build_page_overview_payload(page, days, force_refresh=False)


def _build_page_overview_payload_with_budget(page: str, days: int) -> Dict[str, Any]:
    timeout_marker = {"__dashboard_timeout__": True}
    payload = _run_with_timeout(
        _build_page_overview_payload_in_context,
        page,
        days,
        timeout=_PAGE_OVERVIEW_BUILD_TIMEOUT_SECONDS,
        default=timeout_marker,
        label=f"page overview {page}",
    )
    if isinstance(payload, dict) and payload.get("__dashboard_timeout__"):
        raise TimeoutError(f"page overview {page} timed out")
    return _response_to_dict(payload)


def _normalize_page_overview_payload_contract(payload: Dict[str, Any], page: str, days: int) -> Dict[str, Any]:
    payload = _response_to_dict(payload)
    if str(payload.get("degraded_reason") or "") in _STALE_ONLY_DEGRADED_REASONS:
        payload = _mark_stale_dashboard_payload(payload, str(payload.get("degraded_reason") or "stale_fallback"))
    page = _normalize_page_overview_name(page or payload.get("page") or "")
    account_summary = _authoritative_account_summary(_core())
    payload = _attach_authoritative_account_fields(payload, account_summary)
    if isinstance(payload.get("status"), dict):
        payload["status"] = _attach_authoritative_account_fields(payload["status"], account_summary)
    if isinstance(payload.get("command_center"), dict):
        command_center = payload["command_center"]
        if str(command_center.get("degraded_reason") or "") in _STALE_ONLY_DEGRADED_REASONS:
            command_center = _mark_stale_dashboard_payload(command_center, str(command_center.get("degraded_reason") or "command_center_stale"))
        payload["command_center"] = _attach_authoritative_account_fields(
            _normalize_command_center_payload_contract(command_center),
            account_summary,
        )
    if page == "sentiment_intelligence":
        by_asset = payload.get("by_asset")
        if not isinstance(by_asset, dict) or not list(by_asset.get("assets") or []):
            reason = str(payload.get("degraded_reason") or "sentiment_context_warming")
            payload["by_asset"] = _neutral_sentiment_by_asset_payload(reason)
    if page == "playbook_intel" and isinstance(payload.get("command_center"), dict):
        if not list(payload.get("near_misses") or []):
            command_center = payload["command_center"]
            payload["near_misses"] = list(command_center.get("near_misses") or [])
    payload.setdefault("success", True)
    payload.setdefault("page", page)
    payload.setdefault("days", days)
    return payload


def _get_cached_page_overview_payload(page: str, days: int, *, force_refresh: bool = False) -> Dict[str, Any]:
    page = _normalize_page_overview_name(page)
    cache_key = f"page_overview:{_PAGE_OVERVIEW_CACHE_VERSION}:{page}:{days}"
    last_good_key = f"{cache_key}:last_good"

    def _builder() -> Dict[str, Any]:
        return _build_page_overview_payload_with_budget(page, days)

    if not force_refresh:
        cached = _cache_get(cache_key)
        if cached is not None:
            return _normalize_page_overview_payload_contract(cached, page, days)

        fallback = _cache_get(last_good_key)
        if fallback is not None:
            try:
                payload = _normalize_page_overview_payload_contract(_builder(), page, days)
                ttl = _PAGE_OVERVIEW_DEGRADED_CACHE_TTL if _is_degraded_dashboard_payload(payload) else _PAGE_OVERVIEW_CACHE_TTL
                _cache_set(cache_key, payload, ttl=ttl)
                if not _is_degraded_dashboard_payload(payload):
                    _cache_set(last_good_key, payload, ttl=_PAGE_OVERVIEW_LAST_GOOD_TTL)
                    return payload
            except Exception as exc:
                logger.debug(f"[dashboard] page overview stale refresh failed for {page}: {exc}")
            _trigger_dashboard_payload_refresh(
                cache_key,
                builder=_builder,
                cache_key=cache_key,
                ttl=_PAGE_OVERVIEW_CACHE_TTL,
                last_good_key=last_good_key,
                last_good_ttl=_PAGE_OVERVIEW_LAST_GOOD_TTL,
            )
            return _normalize_page_overview_payload_contract(
                _mark_stale_dashboard_payload(fallback, "stale_fallback"),
                page,
                days,
            )

        try:
            payload = _normalize_page_overview_payload_contract(_builder(), page, days)
            ttl = _PAGE_OVERVIEW_DEGRADED_CACHE_TTL if _is_degraded_dashboard_payload(payload) else _PAGE_OVERVIEW_CACHE_TTL
            _cache_set(cache_key, payload, ttl=ttl)
            if not _is_degraded_dashboard_payload(payload):
                _cache_set(last_good_key, payload, ttl=_PAGE_OVERVIEW_LAST_GOOD_TTL)
            return payload
        except Exception as exc:
            logger.debug(f"[dashboard] page overview first build failed for {page}: {exc}")
            _trigger_dashboard_payload_refresh(
                cache_key,
                builder=_builder,
                cache_key=cache_key,
                ttl=_PAGE_OVERVIEW_CACHE_TTL,
                last_good_key=last_good_key,
                last_good_ttl=_PAGE_OVERVIEW_LAST_GOOD_TTL,
            )
            payload = _build_page_overview_unavailable_payload(page, days, reason="page_overview_warming")
            payload["degraded"] = True
            payload["degraded_reason"] = "page_overview_warming"
            _cache_set(cache_key, payload, ttl=_PAGE_OVERVIEW_DEGRADED_CACHE_TTL)
            return _normalize_page_overview_payload_contract(payload, page, days)

    try:
        payload = _normalize_page_overview_payload_contract(_builder(), page, days)
        ttl = _PAGE_OVERVIEW_DEGRADED_CACHE_TTL if _is_degraded_dashboard_payload(payload) else _PAGE_OVERVIEW_CACHE_TTL
        _cache_set(cache_key, payload, ttl=ttl)
        if not _is_degraded_dashboard_payload(payload):
            _cache_set(last_good_key, payload, ttl=_PAGE_OVERVIEW_LAST_GOOD_TTL)
        return payload
    except Exception as exc:
        logger.warning(f"[dashboard] page overview build failed for {page}: {exc}")
        fallback = _cache_get(last_good_key)
        if fallback is not None:
            _trigger_dashboard_payload_refresh(
                cache_key,
                builder=_builder,
                cache_key=cache_key,
                ttl=_PAGE_OVERVIEW_CACHE_TTL,
                last_good_key=last_good_key,
                last_good_ttl=_PAGE_OVERVIEW_LAST_GOOD_TTL,
            )
            return _normalize_page_overview_payload_contract(
                _mark_stale_dashboard_payload(fallback, "stale_fallback"),
                page,
                days,
            )

        payload = _build_page_overview_unavailable_payload(page, days, reason=str(exc) or "build_failed")
        _cache_set(cache_key, payload, ttl=_PAGE_OVERVIEW_DEGRADED_CACHE_TTL)
        _trigger_dashboard_payload_refresh(
            cache_key,
            builder=_builder,
            cache_key=cache_key,
            ttl=_PAGE_OVERVIEW_CACHE_TTL,
            last_good_key=last_good_key,
            last_good_ttl=_PAGE_OVERVIEW_LAST_GOOD_TTL,
        )
        return payload


@app.route("/api/page-overview")
@_check_api_auth
@_check_rate_limit
def api_page_overview():
    raw_page = request.args.get("page", "")
    page = _normalize_page_overview_name(raw_page)
    if not page:
        return handle_api_error(BadRequest("page query required"), "/api/page-overview", 400)
    if page not in _PAGE_OVERVIEW_VALID_PAGES:
        return handle_api_error(BadRequest(f"Unknown overview page '{page}'"), "/api/page-overview", 400)
    try:
        days = max(1, min(int(request.args.get("days", 30)), 90))
    except ValueError:
        return handle_api_error(BadRequest("days must be an integer"), "/api/page-overview", 400)

    payload = _get_cached_page_overview_payload(
        page,
        days,
        force_refresh=_request_wants_cache_bypass(),
    )
    return jsonify(payload)


@app.route("/api/source-peek")
@_check_api_auth
@_check_rate_limit
def api_source_peek():
    raw_path = request.args.get("path", "")
    focus_line_raw = request.args.get("line", "").strip()
    try:
        focus_line = int(focus_line_raw) if focus_line_raw else None
    except ValueError:
        return handle_api_error(BadRequest("line must be an integer"), "/api/source-peek", 400)
    try:
        payload = _build_source_peek_payload(raw_path, focus_line=focus_line)
        return jsonify(payload)
    except APIError as exc:
        status_code = 404 if isinstance(exc, NotFound) else 403 if isinstance(exc, Forbidden) else 400 if isinstance(exc, BadRequest) else 500
        return handle_api_error(exc, "/api/source-peek", status_code)


# ══════════════════════════════════════════════════════════════════════════════
# STARTUP
# ══════════════════════════════════════════════════════════════════════════════

def _run_hypercorn_server(host: str, port: int, http2: bool = False, ssl_cert: str | None = None, ssl_key: str | None = None) -> bool:
    try:
        import asyncio
        from hypercorn.config import Config
        from hypercorn.asyncio import serve
        from hypercorn.middleware.wsgi import AsyncioWSGIMiddleware
        from hypercorn import app_wrappers as _hypercorn_app_wrappers

        if not getattr(_hypercorn_app_wrappers.WSGIWrapper.run_app, "_robbie_empty_response_fix", False):
            def _safe_run_app(self, environ: dict, send) -> None:
                headers: list[tuple[bytes, bytes]] = []
                response_started = False
                status_code: int | None = None

                def start_response(
                    status: str,
                    response_headers: list[tuple[str, str]],
                    exc_info: Exception | None = None,
                ) -> None:
                    nonlocal headers, response_started, status_code

                    raw, _ = status.split(" ", 1)
                    status_code = int(raw)
                    headers = [
                        (name.lower().encode("latin-1"), value.encode("latin-1"))
                        for name, value in response_headers
                    ]
                    response_started = True

                response_body = self.app(environ, start_response)

                try:
                    first_chunk = True
                    for output in response_body:
                        if not response_started:
                            raise RuntimeError("WSGI app did not call start_response")

                        if first_chunk:
                            send({"type": "http.response.start", "status": status_code, "headers": headers})
                            first_chunk = False

                        send({"type": "http.response.body", "body": output, "more_body": True})

                    # Hypercorn's handle_http() sends the terminating
                    # `http.response.body` frame itself. We only patch the
                    # empty-body case where upstream Hypercorn versions can
                    # miss the initial response start entirely.
                    if response_started and first_chunk:
                        send({"type": "http.response.start", "status": status_code, "headers": headers})
                finally:
                    if hasattr(response_body, "close"):
                        response_body.close()

            _safe_run_app._robbie_empty_response_fix = True
            _hypercorn_app_wrappers.WSGIWrapper.run_app = _safe_run_app

        config = Config()
        config.bind = [f"{host}:{port}"]
        config.worker_class = "asyncio"
        config.loglevel = "info"
        config.keep_alive_timeout = 5
        if http2:
            config.alpn_protocols = ["h2", "http/1.1"]

        if ssl_cert and ssl_key:
            config.certfile = ssl_cert
            config.keyfile = ssl_key
            logger.info(f"[dashboard] Starting Hypercorn server with TLS cert={ssl_cert}")
        elif http2:
            logger.info("[dashboard] Starting Hypercorn HTTP/2 server (cleartext, browser support may be limited)")
        else:
            logger.info("[dashboard] Starting Hypercorn production server")

        asyncio.run(serve(AsyncioWSGIMiddleware(app), config))
        return True
    except Exception as e:
        logger.warning(f"[dashboard] Hypercorn server unavailable or failed: {e}")
        return False


def _run_threaded_wsgi_server(host: str, port: int) -> bool:
    try:
        from waitress import serve

        threads = max(4, int(os.getenv("DASHBOARD_WSGI_THREADS", "12")))
        connection_limit = max(32, int(os.getenv("DASHBOARD_WSGI_CONNECTION_LIMIT", "128")))
        channel_timeout = max(10, int(os.getenv("DASHBOARD_WSGI_CHANNEL_TIMEOUT", "30")))
        logger.info(
            "[dashboard] Starting Waitress threaded WSGI server "
            f"threads={threads} connection_limit={connection_limit}"
        )
        serve(
            app,
            host=host,
            port=port,
            threads=threads,
            connection_limit=connection_limit,
            channel_timeout=channel_timeout,
            ident="forex-dashboard",
        )
        return True
    except ModuleNotFoundError:
        logger.info("[dashboard] Waitress is not installed; using Flask threaded server")
        return False
    except Exception as e:
        logger.warning(f"[dashboard] Waitress server unavailable or failed: {e}")
        return False


def _dashboard_live_quote_callback(source, symbol, price, volume, side, ts=None) -> None:
    _record_live_quote(source, symbol, price, volume, side, emit_transaction=False)


def _dashboard_asset_map_from_positions(open_positions: Any, *, fallback_to_universe: bool = False) -> Dict[str, str]:
    assets_by_category: Dict[str, str] = {}
    for pos in list(open_positions or []):
        if not isinstance(pos, dict):
            continue
        category = pos.get("category", "forex")
        asset = pos.get("asset", "")
        if asset:
            assets_by_category[str(asset)] = str(category)

    if fallback_to_universe:
        from core.asset_profiles import ALL_ASSETS

        for asset in sorted(ALL_ASSETS):
            assets_by_category.setdefault(asset, registry.category(asset))
    return assets_by_category


def _setup_dashboard_live_streams(cb) -> tuple[Any, Dict[str, Dict[str, str]], Any]:
    from websocket_dashboard import set_connected
    from websocket_manager import WebSocketManager
    from services.market_data_router import (
        filter_deriv_stream_assets,
        filter_ig_primary_assets,
        split_pending_ig_fallback_assets,
    )

    try:
        from services.ig_streaming_manager import ig_streaming_manager as _ig_stream_manager
    except Exception:
        _ig_stream_manager = None

    ws = WebSocketManager()
    ws.start()
    state = _dashboard_state()
    open_positions = state.get_open_positions() if hasattr(state, "get_open_positions") else []
    assets_by_category = _dashboard_asset_map_from_positions(open_positions, fallback_to_universe=True)
    ig_primary_assets = filter_ig_primary_assets(assets_by_category)
    ig_stream_assets: Dict[str, str] = {}
    ig_poll_assets: Dict[str, str] = {}
    ig_fallback_assets: Dict[str, str] = {}

    if assets_by_category:
        deriv_stream_assets = filter_deriv_stream_assets(assets_by_category)
        if deriv_stream_assets:
            ws.subscribe_deriv(deriv_stream_assets, cb)
            logger.info(f"[dashboard] Live Deriv stream assets: {sorted(deriv_stream_assets.keys())}")
        else:
            logger.info("[dashboard] No Deriv stream assets after market-data routing filter")
        if _ig_stream_manager is not None:
            try:
                ig_stream_assets = _ig_stream_manager.subscribe_prices(ig_primary_assets, cb)
            except Exception as stream_exc:
                logger.warning(
                    f"[dashboard] IG streaming unavailable; falling back to Deriv stream for IG assets: {stream_exc}"
                )
                ig_stream_assets = {}
        pending_ig_assets = {
            asset: category
            for asset, category in ig_primary_assets.items()
            if asset not in ig_stream_assets
        }
        ig_fallback_assets, ig_poll_assets = split_pending_ig_fallback_assets(pending_ig_assets)
        if pending_ig_assets:
            ws.subscribe_deriv(pending_ig_assets, cb, include_ig_assets=True)
            logger.info(f"[dashboard] Supplemental depth/fallback assets for IG primary markets: {sorted(pending_ig_assets.keys())}")

        if ig_stream_assets:
            logger.info(f"[dashboard] Live IG stream assets: {sorted(ig_stream_assets.keys())}")
        if ig_poll_assets:
            set_connected("ig", True, len(ig_poll_assets))
            logger.info(f"[dashboard] Live IG poll assets: {sorted(ig_poll_assets.keys())}")
        elif ig_stream_assets:
            set_connected("ig", True, len(ig_stream_assets))
        else:
            set_connected("ig", False, 0)

    logger.info("[dashboard] Live streams started")
    return ws, {
        "ig_poll_assets": ig_poll_assets,
        "ig_stream_assets": ig_stream_assets,
        "ig_fallback_assets": ig_fallback_assets,
        "subscription_signature": (
            tuple(sorted((str(asset), str(category or "")) for asset, category in filter_deriv_stream_assets(assets_by_category).items())),
            tuple(sorted((str(asset), str(category or "")) for asset, category in ig_primary_assets.items())),
        ),
        "last_subscription_update_ts": time.time() if assets_by_category else 0.0,
    }, _ig_stream_manager


def _dashboard_subscription_signature(
    deriv_stream_assets: Dict[str, str],
    ig_primary_assets: Dict[str, str],
) -> Tuple[Tuple[Tuple[str, str], ...], Tuple[Tuple[str, str], ...]]:
    return (
        tuple(sorted((str(asset), str(category or "")) for asset, category in deriv_stream_assets.items())),
        tuple(sorted((str(asset), str(category or "")) for asset, category in ig_primary_assets.items())),
    )


def _dashboard_apply_subscription_update(
    ws_global: Any,
    stream_state: Dict[str, Dict[str, str]],
    cb,
    ig_stream_global: Any,
    asset_map: Dict[str, str],
) -> None:
    from websocket_dashboard import set_connected
    from services.market_data_router import (
        filter_deriv_stream_assets,
        filter_ig_primary_assets,
        split_pending_ig_fallback_assets,
    )

    try:
        deriv_stream_assets = filter_deriv_stream_assets(asset_map)
        ig_primary_assets = filter_ig_primary_assets(asset_map)
        desired_signature = _dashboard_subscription_signature(deriv_stream_assets, ig_primary_assets)
        last_signature = stream_state.get("subscription_signature")
        last_update_ts = float(stream_state.get("last_subscription_update_ts") or 0.0)
        ig_degraded = bool(ig_primary_assets) and (
            stream_state.get("ig_poll_assets")
            or stream_state.get("ig_fallback_assets")
            or not stream_state.get("ig_stream_assets")
        )
        if last_signature == desired_signature:
            if not ig_degraded:
                return
            if last_update_ts and (time.time() - last_update_ts) < 300.0:
                return
        if deriv_stream_assets:
            ws_global.subscribe_deriv(deriv_stream_assets, cb)
            logger.debug(f"[dashboard] Updated live Deriv subscriptions: {deriv_stream_assets}")
        if ig_stream_global is not None:
            try:
                stream_state["ig_stream_assets"] = ig_stream_global.subscribe_prices(ig_primary_assets, cb)
            except Exception as stream_exc:
                logger.debug(f"[dashboard] Updated live IG subscriptions failed: {stream_exc}")
                stream_state["ig_stream_assets"] = {}
        else:
            stream_state["ig_stream_assets"] = {}
        pending_ig_assets = {
            asset: category
            for asset, category in ig_primary_assets.items()
            if asset not in stream_state["ig_stream_assets"]
        }
        stream_state["ig_fallback_assets"], stream_state["ig_poll_assets"] = split_pending_ig_fallback_assets(
            pending_ig_assets
        )
        if pending_ig_assets:
            ws_global.subscribe_deriv(pending_ig_assets, cb, include_ig_assets=True)
            logger.debug(
                f"[dashboard] Supplemental depth/fallback subscribed for IG primary assets: {sorted(pending_ig_assets.keys())}"
            )
        try:
            if stream_state["ig_poll_assets"]:
                set_connected("ig", True, len(stream_state["ig_poll_assets"]))
            elif stream_state["ig_stream_assets"]:
                set_connected("ig", True, len(stream_state["ig_stream_assets"]))
            else:
                set_connected("ig", False, 0)
        except Exception:
            pass
        stream_state["subscription_signature"] = desired_signature
        stream_state["last_subscription_update_ts"] = time.time()
    except Exception as _ue:
        logger.debug(f"[dashboard] Update live subs failed: {_ue}")


def _dashboard_update_ws_subscriptions_loop(ws_global: Any, stream_state: Dict[str, Dict[str, str]], cb, ig_stream_global: Any) -> None:
    while True:
        try:
            state = _dashboard_state()
            if ws_global is None or state is None:
                time.sleep(30)
                continue

            time.sleep(30)
            open_positions = state.get_open_positions() if hasattr(state, "get_open_positions") else []
            asset_map = _dashboard_asset_map_from_positions(open_positions, fallback_to_universe=True)
            if asset_map:
                _dashboard_apply_subscription_update(ws_global, stream_state, cb, ig_stream_global, asset_map)
        except Exception as e:
            logger.debug(f"[dashboard] bg_update_ws_subscriptions: {e}")


def _dashboard_refresh_ig_quotes_loop(stream_state: Dict[str, Dict[str, str]]) -> None:
    """Poll non-streaming IG assets into the shared live-price cache."""

    from websocket_dashboard import mark_feed_activity, set_connected

    last_published_prices: Dict[str, float] = {}

    while True:
        try:
            poll_assets = dict(stream_state.get("ig_poll_assets") or {})
            success_count = 0
            for asset, category in list(poll_assets.items()):
                try:
                    price, _ = _fetcher.get_real_time_price(
                        asset,
                        category,
                        prefer_live_stream=False,
                        allow_cached_quote=False,
                    )
                    if price is None:
                        continue
                    meta = _fetcher.get_last_price_metadata(asset)
                    source = str((meta or {}).get("source") or "IG")
                    price_value = float(price)
                    previous_price = last_published_prices.get(asset)
                    emit_transaction = previous_price is None or abs(previous_price - price_value) > 1e-12
                    _record_live_quote(source, asset, price_value, emit_transaction=emit_transaction)
                    mark_feed_activity("ig", len(poll_assets))
                    last_published_prices[asset] = price_value
                    success_count += 1
                except Exception as quote_exc:
                    logger.debug(f"[dashboard] IG quote poll {asset}: {quote_exc}")
            set_connected("ig", True, len(poll_assets))
            if success_count == 0:
                logger.debug("[dashboard] IG quote poll completed with no fresh prices")
        except Exception as e:
            set_connected("ig", False, len(stream_state.get("ig_poll_assets") or {}))
            logger.debug(f"[dashboard] bg_refresh_ig_quotes: {e}")
        time.sleep(5)


def _normalise_dashboard_server(server: str | None) -> str:
    value = str(server or os.getenv("DASHBOARD_SERVER") or "auto").strip().lower()
    aliases = {
        "": "auto",
        "dev": "threaded",
        "flask": "threaded",
        "werkzeug": "threaded",
        "threads": "threaded",
        "production": "hypercorn",
        "h11": "hypercorn",
    }
    return aliases.get(value, value)


def start_dashboard(
    core,
    host: str = "127.0.0.1",
    port: int = 5000,
    http2: bool = False,
    ssl_cert: str | None = None,
    ssl_key: str | None = None,
    server: str | None = None,
) -> None:
    """Called by bot.py after engine.start(). Blocking — never returns."""
    inject_core(core)
    _init_api_key()  # FIX SEC-05: Initialize API key authentication

    # Start background threads
    threading.Thread(target=_bg_refresh,       name="DashBgRefresh",     daemon=True).start()
    threading.Thread(target=_prewarm_sentiment, name="SentimentPrewarm",  daemon=True).start()
    _prewarm_command_center_payload()
    _prewarm_page_overviews()

    # Start pub/sub listeners
    _start_p3_listener()
    _start_p7_listener()

    # Optional live-price cache — Deriv/Binance streams plus IG streaming/poll fallback
    _ws_global = None  # Reference to WebSocket manager for periodic updates
    _ig_stream_global = None
    _stream_state = {"ig_poll_assets": {}, "ig_stream_assets": {}, "ig_fallback_assets": {}}
    try:
        _ws_global, _stream_state, _ig_stream_global = _setup_dashboard_live_streams(_dashboard_live_quote_callback)
    except Exception as e:
        logger.warning(f"[dashboard] WebSocket streams failed (non-fatal): {e}")
        _stream_state = {"ig_poll_assets": {}, "ig_stream_assets": {}, "ig_fallback_assets": {}}

    threading.Thread(
        target=_dashboard_update_ws_subscriptions_loop,
        args=(_ws_global, _stream_state, _dashboard_live_quote_callback, _ig_stream_global),
        name="WSSubsUpdate",
        daemon=True,
    ).start()
    threading.Thread(
        target=_dashboard_refresh_ig_quotes_loop,
        args=(_stream_state,),
        name="IGQuotePoll",
        daemon=True,
    ).start()

    scheme = "https" if http2 and ssl_cert and ssl_key else "http"
    logger.info(f"[dashboard] {scheme}://{host}:{port}/command-center")
    dashboard_server = _normalise_dashboard_server(server)
    if dashboard_server not in {"auto", "hypercorn", "threaded"}:
        logger.warning(f"[dashboard] Unknown DASHBOARD_SERVER={dashboard_server!r}; using auto")
        dashboard_server = "auto"

    prefer_hypercorn = dashboard_server == "hypercorn" or (
        dashboard_server == "auto" and (http2 or not _DEVELOPMENT_MODE)
    )
    if prefer_hypercorn and _run_hypercorn_server(host, port, http2=http2, ssl_cert=ssl_cert, ssl_key=ssl_key):
        return
    if prefer_hypercorn and ssl_cert and ssl_key:
        logger.info("[dashboard] Falling back to Flask HTTPS server")
        app.run(
            debug=False,
            host=host,
            port=port,
            ssl_context=(ssl_cert, ssl_key),
            threaded=True,
            use_reloader=False,
        )
        return
    if not prefer_hypercorn and not (ssl_cert and ssl_key) and _run_threaded_wsgi_server(host, port):
        return
    if not prefer_hypercorn and ssl_cert and ssl_key:
        logger.info("[dashboard] Starting Flask HTTPS threaded server")
        app.run(
            debug=False,
            host=host,
            port=port,
            ssl_context=(ssl_cert, ssl_key),
            threaded=True,
            use_reloader=False,
        )
        return
    if prefer_hypercorn:
        logger.info("[dashboard] Falling back to threaded Flask server")
    else:
        logger.info("[dashboard] Starting threaded Flask server")

    app.run(debug=False, host=host, port=port, threaded=True, use_reloader=False)


# Standalone mode (python -m dashboard.web_app_live)
if __name__ == "__main__":
    standalone_parser = argparse.ArgumentParser(description="Robbindl dashboard server")
    standalone_parser.add_argument("--host", type=str, default=os.getenv("DASHBOARD_HOST", "127.0.0.1"))
    standalone_parser.add_argument("--port", type=int, default=int(os.getenv("DASHBOARD_PORT", "5000")))
    standalone_parser.add_argument("--http2", action="store_true", default=os.getenv("DASHBOARD_HTTP2", "").lower() in {"1", "true", "yes", "on"})
    standalone_parser.add_argument("--ssl-cert", type=str, default=os.getenv("DASHBOARD_SSL_CERT") or None)
    standalone_parser.add_argument("--ssl-key", type=str, default=os.getenv("DASHBOARD_SSL_KEY") or None)
    standalone_parser.add_argument(
        "--server",
        type=str,
        choices=["auto", "hypercorn", "threaded", "flask", "werkzeug"],
        default=os.getenv("DASHBOARD_SERVER", "auto"),
        help="Dashboard HTTP server mode. Use threaded for the split service behind nginx.",
    )
    standalone_args, _ = standalone_parser.parse_known_args()
    logger.info("[dashboard] Standalone mode — engine not connected")
    start_dashboard(
        None,
        host=standalone_args.host,
        port=standalone_args.port,
        http2=standalone_args.http2,
        ssl_cert=standalone_args.ssl_cert,
        ssl_key=standalone_args.ssl_key,
        server=standalone_args.server,
    )
