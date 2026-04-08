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
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from flask import Flask, Response, jsonify, redirect, render_template, request, send_from_directory, stream_with_context
from flask_cors import CORS
from functools import wraps
import hashlib
from werkzeug.middleware.proxy_fix import ProxyFix

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.assets  import registry
from data.fetcher import DataFetcher, get_shared_fetcher
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
) -> None:
    if emit_transaction:
        add_transaction(source, symbol, price, volume, side)
    # Store price for live P&L updates and shared dashboard cache freshness.
    set_live_price(symbol, price, source)

# ── Flask app ─────────────────────────────────────────────────────────────────
app = Flask(
    __name__,
    template_folder=os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "templates"),
    static_folder  =os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "static"),
)
try:
    from config.config import DASHBOARD_CORS_ORIGINS, TRUST_PROXY_COUNT, PLAYBOOK_ONLY_RUNTIME
except Exception:
    DASHBOARD_CORS_ORIGINS = ["http://localhost:5000"]
    TRUST_PROXY_COUNT = 1
    PLAYBOOK_ONLY_RUNTIME = False

app.wsgi_app = ProxyFix(app.wsgi_app, x_for=TRUST_PROXY_COUNT, x_proto=TRUST_PROXY_COUNT, x_host=TRUST_PROXY_COUNT)
CORS(app, resources={r"/api/*": {"origins": DASHBOARD_CORS_ORIGINS}})

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
_RATE_LIMIT_REQUESTS_PER_MINUTE = 60  # Max 60 requests per minute per IP
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
        if not auth_header.startswith("Bearer "):
            return jsonify({"success": False, "error": "Missing Authorization header"}), 401
        
        token = auth_header[7:]  # Remove "Bearer " prefix
        
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
    """Decorator to enforce rate limiting (60 req/min per IP)."""
    @wraps(fn)
    def wrapper(*args, **kwargs):
        ip = request.remote_addr
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
    logger.info("[dashboard] TradingCore injected")

def _core() -> Optional[Any]:
    return _CORE

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

# ── Response cache (in-process TTL cache + Redis fallback) ─────────────────────────────────────
_cache_store: Dict[str, Tuple[Any, float]] = {}
_cache_lock  = threading.Lock()
_cache_prefix = "dashboard:cache:"

def _redis_cache_get(key: str) -> Optional[Any]:
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
    try:
        from services.redis_pool import get_client as _get_redis_client
        client = _get_redis_client()
        if client is None:
            return
        client.set(_cache_prefix + key, json.dumps(value, default=str), ex=ttl)
    except Exception:
        pass


def _cache_get(key: str) -> Optional[Any]:
    with _cache_lock:
        entry = _cache_store.get(key)
        if entry and time.time() < entry[1]:
            return entry[0]
        if entry:
            _cache_store.pop(key, None)

    redis_val = _redis_cache_get(key)
    if redis_val is not None:
        return redis_val
    return None


def _cache_set(key: str, value: Any, ttl: int = 30) -> None:
    with _cache_lock:
        _cache_store[key] = (value, time.time() + ttl)
    _redis_cache_set(key, value, ttl)


def _invalidate_cache_prefixes(*prefixes: str) -> None:
    active_prefixes = [str(prefix or "") for prefix in prefixes if str(prefix or "").strip()]
    if not active_prefixes:
        return

    with _cache_lock:
        for key in list(_cache_store.keys()):
            if any(key.startswith(prefix) for prefix in active_prefixes):
                _cache_store.pop(key, None)

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
    cache_key = f"html_template:{template_name}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    html = render_template(template_name)
    _cache_set(cache_key, html, ttl=ttl)
    return html


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


def _normalized_query_string() -> str:
    params = []
    for key in sorted(request.args.keys()):
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
    if request.args.get('no_cache'):
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
        response.headers.setdefault("Cache-Control", "public, max-age=5, stale-while-revalidate=30")
        if response.status_code == 200 and response.is_json:
            try:
                cache_key = _get_api_cache_key()
                _cache_set(cache_key, response.get_json(), ttl=10)
                response.headers['X-Cache-Write'] = 'MISS'
            except Exception:
                logger.exception("Failed to write API response to cache")
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

def _start_p3_listener():
    global _p3_started
    if _p3_started:
        return
    _p3_started = True
    def _listen():
        try:
            from services.redis_pool import get_pubsub as _get_pubsub
            ps = _get_pubsub()
            if ps is None:
                raise RuntimeError("Redis unavailable")
            ps.subscribe("LIQUIDITY_WALL_DETECTED", "STOP_HUNT_DETECTED")
            for msg in ps.listen():
                if msg["type"] != "message":
                    continue
                try:
                    data = json.loads(msg["data"])
                    ch   = msg["channel"]
                    if isinstance(ch, bytes): ch = ch.decode()
                    if ch == "LIQUIDITY_WALL_DETECTED":
                        with _p3_wall_lock:
                            _p3_walls.append(data)
                            if len(_p3_walls) > 50: _p3_walls.pop(0)
                    elif ch == "STOP_HUNT_DETECTED":
                        with _p3_hunt_lock:
                            _p3_hunts.append(data)
                            if len(_p3_hunts) > 30: _p3_hunts.pop(0)
                except Exception:
                    pass
        except Exception as e:
            logger.warning(f"[Phase3Listener] {e}")
    threading.Thread(target=_listen, name="p3-listener", daemon=True).start()

# ── Intelligence alerts pub/sub buffer ───────────────────────────────────────
_p7_alerts: list = []
_p7_lock    = threading.Lock()
_p7_started = False

def _start_p7_listener():
    global _p7_started
    if _p7_started:
        return
    _p7_started = True
    def _listen():
        try:
            from services.redis_pool import get_pubsub as _get_pubsub
            ps = _get_pubsub()
            if ps is None:
                raise RuntimeError("Redis unavailable")
            ps.subscribe("INTELLIGENCE_ALERT")
            for msg in ps.listen():
                if msg["type"] != "message":
                    continue
                try:
                    data = json.loads(msg["data"])
                    with _p7_lock:
                        _p7_alerts.append(data)
                        if len(_p7_alerts) > 100: _p7_alerts.pop(0)
                except Exception:
                    pass
        except Exception as e:
            logger.warning(f"[IntelListener] {e}")
    threading.Thread(target=_listen, name="p7-listener", daemon=True).start()

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
            logger.info("[dashboard] Signal source: Redis subscriber (zero decision re-runs)")
            _bg_refresh_redis()
            return   # _bg_refresh_redis() blocks forever while Redis is up
    except Exception:
        pass
    logger.info("[dashboard] Signal source: engine polling fallback (Redis unavailable)")
    _bg_refresh_fallback()


def _bg_refresh_redis() -> None:
    """Subscribe to the 'signals' Redis channel published by the trading loop."""
    import json
    ps = None
    while True:
        try:
            from services.redis_pool import get_pubsub as _get_pubsub
            ps = _get_pubsub(old_pubsub=ps)  # closes old connection before new one
            if ps is None:
                raise RuntimeError("pubsub unavailable")
            ps.subscribe("signals")
            logger.info("[dashboard] Subscribed to Redis 'signals' channel")
            for msg in ps.listen():
                if msg.get("type") != "message":
                    continue
                try:
                    data = msg.get("data", "{}")
                    sig  = json.loads(data) if isinstance(data, (str, bytes)) else data
                    asset = sig.get("asset", "")
                    if asset:
                        _store(asset, sig)
                        _last_ref[asset] = time.time()
                except Exception as _pe:
                    logger.debug(f"[dashboard] signal parse: {_pe}")
        except Exception as e:
            logger.warning(f"[dashboard] Redis subscriber dropped ({e}) — reconnecting in 10s")
            time.sleep(10)


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

            with ThreadPoolExecutor(max_workers=4) as pool:
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
        logger.info("[dashboard] Pre-warming sentiment cache...")
        _get_sent()
        with app.test_request_context():
            try:
                api_sentiment_dashboard()
                logger.info("[dashboard] sentiment/dashboard warmed")
            except Exception as e:
                logger.debug(f"[dashboard] sentiment/dashboard prewarm: {e}")
            try:
                api_sentiment_by_asset()
                logger.info("[dashboard] sentiment/by-asset warmed")
            except Exception as e:
                logger.debug(f"[dashboard] sentiment/by-asset prewarm: {e}")
    except Exception as e:
        logger.debug(f"[dashboard] sentiment prewarm failed: {e}")

# ── Win rate normaliser (DB returns decimal 0.XX, we display as %) ────────────
def _wr(raw) -> float:
    v = float(raw or 0)
    return round(v * 100, 2) if v <= 1.0 else round(v, 2)


def _extract_execution_feedback_fields(metadata: Any) -> Dict[str, Any]:
    meta = metadata if isinstance(metadata, dict) else {}
    feedback = meta.get("execution_feedback") if isinstance(meta.get("execution_feedback"), dict) else {}
    policy = meta.get("execution_feedback_policy") if isinstance(meta.get("execution_feedback_policy"), dict) else {}
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

    def _bucket(container: Dict[str, Dict[str, Any]], label: str) -> Dict[str, Any]:
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

    for trade in list(trades or []):
        if not isinstance(trade, dict) or _is_partial_close_trade_row(trade):
            continue
        playbook_name = _playbook_name_from_trade(trade)
        if not playbook_name:
            continue
        event_time = _coerce_utc_datetime(trade.get("exit_time") or trade.get("entry_time"))
        if event_time is not None and event_time < cutoff:
            continue

        pnl = float(trade.get("pnl") or 0.0)
        metadata = dict(trade.get("metadata") or {})
        execution = _extract_execution_feedback_fields(metadata)
        rr_realized = float(execution.get("rr_realized", 0.0) or 0.0)
        asset_label = str(trade.get("canonical_asset") or trade.get("asset") or "").strip() or "UNKNOWN"
        entry_time = _coerce_utc_datetime(trade.get("entry_time") or trade.get("open_time"))
        exit_time = _coerce_utc_datetime(trade.get("exit_time"))
        exit_reason = str(trade.get("exit_reason") or metadata.get("exit_reason") or "").strip().lower()
        is_win = pnl > 0.0
        is_loss = pnl < 0.0
        is_stop_exit = "stop loss" in exit_reason

        trade_count += 1
        if is_win or is_loss:
            decisive_count += 1
            if is_win:
                win_count += 1
                gross_win += pnl
            else:
                gross_loss += abs(pnl)
        if abs(rr_realized) > 1e-9:
            rr_values.append(rr_realized)
        if entry_time is not None and exit_time is not None and exit_time >= entry_time:
            hold_minutes_value = (exit_time - entry_time).total_seconds() / 60.0
            hold_minutes.append(hold_minutes_value)
        else:
            hold_minutes_value = None
        if is_stop_exit:
            stop_exit_count += 1

        for container, label in ((playbook_rows, playbook_name), (asset_rows, asset_label)):
            row = _bucket(container, label)
            row["trade_count"] += 1
            row["total_pnl"] += pnl
            if abs(rr_realized) > 1e-9:
                row["rr_values"].append(rr_realized)
            if hold_minutes_value is not None:
                row["hold_minutes"].append(hold_minutes_value)
            if is_stop_exit:
                row["stop_exit_count"] += 1
            if is_win or is_loss:
                row["decisive_count"] += 1
                if is_win:
                    row["win_count"] += 1
                    row["gross_win"] += pnl
                else:
                    row["gross_loss"] += abs(pnl)

    def _finalize(rows: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
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

    playbooks = _finalize(playbook_rows)
    assets = _finalize(asset_rows)
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
        total += 1
        broker_context = str(row.get("broker_context") or "").lower()
        if broker_context == "supportive":
            broker_supportive += 1
        elif broker_context == "fragile":
            broker_fragile += 1

        depth_mode = str(row.get("depth_mode") or "").lower()
        if depth_mode == "true_depth":
            true_depth += 1
        elif depth_mode == "synthetic_depth":
            synthetic_depth += 1

        cross_context = str(row.get("cross_asset_context_state") or row.get("cross_asset_context") or "").lower()
        if cross_context == "supportive":
            cross_support += 1
        elif cross_context == "conflicted":
            cross_conflict += 1

        if bool(row.get("recent_pattern_block_new_entries")):
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
    items: List[Dict[str, Any]] = []
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
        items.append(
            {
                "asset": asset,
                "direction": str(row.get("direction") or "").upper(),
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
            }
        )
    items.sort(
        key=lambda item: (
            float(item.get("rank_score", 0.0) or 0.0),
            float(item.get("opportunity_score", 0.0) or 0.0),
            float(item.get("setup_quality", 0.0) or 0.0),
        ),
        reverse=True,
    )
    return items[: max(1, int(limit or 6))]


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
    for row in list(journals or []):
        if not isinstance(row, dict):
            continue
        if str(row.get("decision") or "").upper() == "SURVIVED":
            continue
        asset = str(row.get("asset") or "").strip()
        if not asset:
            continue
        blocker = str(
            row.get("killed_by")
            or row.get("blocked_reason")
            or row.get("kill_reason")
            or row.get("final_policy_reason")
            or row.get("last_layer")
            or "no_candidate"
        ).strip() or "no_candidate"
        blocker_label = blocker.split(":", 1)[0]
        blocker_counts[blocker_label] = blocker_counts.get(blocker_label, 0) + 1
        bucket = asset_counts.setdefault(
            asset,
            {"asset": asset, "count": 0, "top_blocker": blocker_label, "reason": blocker, "setup_quality": 0.0},
        )
        bucket["count"] += 1
        bucket["setup_quality"] = max(float(bucket.get("setup_quality", 0.0) or 0.0), float(row.get("setup_quality", 0.0) or 0.0))

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
            {
                "asset": str(item.get("asset") or ""),
                "direction": str(item.get("direction") or item.get("signal") or "").upper(),
                "opportunity_score": round(float(item.get("opportunity_score", 0.0) or 0.0), 3),
                "confidence": round(float(item.get("confidence", 0.0) or 0.0) * 100.0, 1),
            }
        )

    almost_ready = []
    for item in list(near_misses or [])[:4]:
        if not isinstance(item, dict):
            continue
        almost_ready.append(
            {
                "asset": str(item.get("asset") or ""),
                "direction": str(item.get("direction") or "").upper(),
                "reason": str(item.get("reason") or item.get("killed_by") or ""),
                "opportunity_score": round(float(item.get("opportunity_score", 0.0) or 0.0), 3),
                "setup_quality": round(float(item.get("setup_quality", 0.0) or 0.0), 3),
            }
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
            {
                "asset": asset,
                "current_session": str(row.get("current_session") or ""),
                "allowed_sessions": list(row.get("allowed_sessions") or []),
                "category": str(row.get("category") or ""),
            }
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
            {
                "asset": asset,
                "category": str(row.get("category") or ""),
                "preferred_interval": str(row.get("preferred_interval") or ""),
            }
        )
        if len(inactive) >= 4:
            break

    return {
        "hot": hot,
        "almost_ready": almost_ready,
        "blocked": blocked,
        "inactive": inactive,
    }


def _build_trade_tape(positions: Any, closed_trades: Any, *, limit: int = 12) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []
    for pos in list(positions or []):
        if not isinstance(pos, dict):
            continue
        opened_at = _coerce_utc_datetime(pos.get("open_time") or pos.get("entry_time"))
        events.append(
            {
                "asset": str(pos.get("asset") or ""),
                "direction": str(pos.get("direction") or pos.get("signal") or "").upper(),
                "stage": "open",
                "event_time": opened_at.isoformat() if opened_at else "",
                "time_sort": opened_at.timestamp() if opened_at else 0.0,
                "note": str(pos.get("strategy_id") or ""),
                "pnl": round(float(pos.get("pnl", 0.0) or 0.0), 2),
            }
        )

    for trade in list(closed_trades or []):
        if not isinstance(trade, dict):
            continue
        exit_time = _coerce_utc_datetime(trade.get("exit_time") or trade.get("entry_time"))
        stage = "partial" if _is_partial_close_trade_row(trade) else "closed"
        events.append(
            {
                "asset": str(trade.get("asset") or ""),
                "direction": str(trade.get("direction") or trade.get("signal") or "").upper(),
                "stage": stage,
                "event_time": exit_time.isoformat() if exit_time else "",
                "time_sort": exit_time.timestamp() if exit_time else 0.0,
                "note": str(trade.get("exit_reason") or trade.get("continuation_summary") or ""),
                "pnl": round(float(trade.get("pnl", 0.0) or 0.0), 2),
            }
        )

    events.sort(key=lambda item: float(item.get("time_sort", 0.0) or 0.0), reverse=True)
    return events[: max(4, int(limit or 12))]


def _load_authoritative_closed_trades(limit: int = 50) -> List[Dict[str, Any]]:
    target = max(1, int(limit or 50))
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
    return normalized


def _build_trade_lifecycle(positions: Any, closed_trades: Any, journals: Any) -> Dict[str, Any]:
    seed_count = 0
    approved_count = 0
    for row in list(journals or []):
        if not isinstance(row, dict):
            continue
        seed_count += 1
        if str(row.get("decision") or "").upper() == "SURVIVED":
            approved_count += 1

    raw_closed = [row for row in list(closed_trades or []) if isinstance(row, dict)]
    partial_count = sum(1 for row in raw_closed if _is_partial_close_trade_row(row))
    closed_count = sum(1 for row in raw_closed if not _is_partial_close_trade_row(row))
    runner_count = sum(1 for row in raw_closed if bool(row.get("has_partial_closes")))

    return {
        "seeded": seed_count,
        "approved": approved_count,
        "opened": len(list(positions or [])),
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


def _summarize_failure_archetypes(journals: Any, trades: Any, *, limit: int = 6) -> List[Dict[str, Any]]:
    archetypes: Dict[str, Dict[str, Any]] = {}

    def _touch(label: str, asset: str) -> None:
        row = archetypes.setdefault(label, {"label": label, "count": 0, "assets": {}})
        row["count"] += 1
        if asset:
            row["assets"][asset] = row["assets"].get(asset, 0) + 1

    for row in list(journals or []):
        if not isinstance(row, dict):
            continue
        if str(row.get("decision") or "").upper() == "SURVIVED":
            continue
        asset = str(row.get("asset") or "")
        reason = str(row.get("kill_reason") or row.get("final_policy_reason") or row.get("blocked_reason") or row.get("killed_by") or "").lower()
        if "session" in reason:
            label = "Session window"
        elif "depth" in reason or str(row.get("depth_mode") or "").lower() in {"synthetic_depth", "top_of_book"}:
            label = "Hostile depth"
        elif float(row.get("stop_hunt_risk", 0.0) or 0.0) >= 0.55:
            label = "Stop-hunt risk"
        elif float(row.get("exhaustion_risk", 0.0) or 0.0) >= 0.55:
            label = "Exhaustion"
        elif "cross" in reason:
            label = "Cross-market conflict"
        else:
            label = "Pattern filter"
        _touch(label, asset)

    for trade in list(trades or []):
        if not isinstance(trade, dict) or _is_partial_close_trade_row(trade):
            continue
        meta = dict(trade.get("metadata") or {})
        execution = _extract_execution_feedback_fields(meta)
        asset = str(trade.get("canonical_asset") or trade.get("asset") or "")
        if bool(execution.get("late_entry")):
            _touch("Late entry", asset)
        if bool(execution.get("premature_stop")):
            _touch("Premature stop", asset)

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
        "USD spike": {"assets": {"EUR/USD", "GBP/USD", "AUD/USD", "USD/CAD", "USD/JPY", "XAU/USD", "XAG/USD"}},
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

@app.route("/ai-predictions")
def pg_ai_predictions():
    return _render_cached_template("ai_predictions.html", ttl=15)

@app.route("/whale-intelligence")
def pg_whale_intelligence():
    return _render_cached_template("whale_intelligence.html", ttl=15)

@app.route("/sentiment-intelligence")
def pg_sentiment_intelligence():
    return _render_cached_template("sentiment_intelligence.html", ttl=15)

@app.route("/risk-dashboard")
def pg_risk_dashboard():
    return _render_cached_template("risk_dashboard.html", ttl=15)

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
def _r_accuracy(): return redirect("/ai-predictions")
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
    cached = _cache_get("status")
    if cached is not None:
        return jsonify(cached)
    try:
        core = _core()
        diagnostic_summary = _summarize_signal_diagnostics([])
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
                "balance": core.get_balance(),
                "assets_cached": len(_sig_store),
                "provider_routing": _provider_routing_summary(),
                "signal_diagnostics": diagnostic_summary,
            }
        else:
            payload = {
                "success": True,
                "bot_ready": False,
                "engine_running": False,
                "architecture": "TradingCore",
                "balance": _args.balance,
                "assets_cached": len(_sig_store),
                "provider_routing": _provider_routing_summary(),
                "signal_diagnostics": diagnostic_summary,
            }
        _cache_set("status", payload, ttl=5)
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
                perf  = core.get_performance()
                daily = core.get_daily_stats()
                return jsonify({
                    "success": True,
                    "balance": round(core.get_balance(), 2),
                    "pnl": round(daily.get("daily_pnl", 0), 2),
                    "total_pnl": round(perf.get("total_pnl", 0), 2),
                    "open_positions": perf.get("open_positions", 0),
                    "closed_positions": perf.get("total_trades", 0),
                    "daily_trades": daily.get("daily_trades", 0),
                    "win_rate": _wr(perf.get("win_rate", 0)),
                    "engine_ready": core.is_ready,
                    "timestamp": datetime.now().isoformat(),
                })
            except Exception as e:
                logger.error(f"[api_system_status] Failed to get core stats: {e}")
                # Fallback response on error
                return jsonify({
                    "success": True, "balance": core.get_balance(), "pnl": 0,
                    "total_pnl": 0, "open_positions": 0, "closed_positions": 0,
                    "daily_trades": 0, "win_rate": 0, "engine_ready": core.is_ready,
                    "timestamp": datetime.now().isoformat(),
                })
        
        # No core initialized - return defaults
        payload = {"success": True, "balance": _args.balance, "pnl": 0,
            "total_pnl": 0, "open_positions": 0, "closed_positions": 0,
            "daily_trades": 0, "win_rate": 0, "engine_ready": False,
            "timestamp": datetime.now().isoformat(),
        }
        _cache_set("status", payload, ttl=5)
        return jsonify(payload)
    except Exception as e:
        return handle_api_error(e, "/api/system-status", 500)

# ══════════════════════════════════════════════════════════════════════════════
# API — COMMAND CENTER
# ══════════════════════════════════════════════════════════════════════════════

def _command_center_core_snapshot(core: Any) -> Tuple[Dict[str, Any], Dict[str, Any], List[Dict[str, Any]], Dict[str, Any], List[Dict[str, Any]]]:
    perf: Dict[str, Any] = {}
    daily: Dict[str, Any] = {}
    positions: List[Dict[str, Any]] = []
    health: Dict[str, Any] = {}
    closed_trades: List[Dict[str, Any]] = []
    if core:
        perf = core.get_performance()
        daily = core.get_daily_stats()
        positions = core.get_positions()
        health = core.health_report()
        closed_trades = _load_authoritative_closed_trades(limit=40)
    return perf, daily, positions, health, closed_trades


def _command_center_journals() -> List[Dict[str, Any]]:
    journals: List[Dict[str, Any]] = []
    try:
        journal_payload = _response_to_dict(_call_view(api_phase7_signal_journal))
        if bool(journal_payload.get("success")):
            journals = list(journal_payload.get("journals") or [])
    except Exception:
        journals = []
    return journals


def _fetch_command_center_slow_data() -> Dict[str, Any]:
    sent_score = 0.0
    whale_count = 0
    whale_recent: List[Dict[str, Any]] = []
    pool = None
    try:
        from concurrent.futures import ThreadPoolExecutor, wait

        pool = ThreadPoolExecutor(max_workers=2)
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
                    elif kind == "whales":
                        whale_recent = list((payload or {}).get("recent", []) or [])
                        whale_count = int((payload or {}).get("alert_count_24h", 0) or 0)
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
    return {
        "sentiment_score": round(sent_score, 3),
        "whale_alerts_24h": whale_count,
        "alert_count_24h": whale_count,
        "recent": whale_recent,
    }


def _fetch_command_center_live_prices(positions: Any, *, limit: int = 8) -> Dict[str, float]:
    assets: List[Tuple[str, str]] = []
    live_prices: Dict[str, float] = {}
    for position in list(positions or [])[: max(1, int(limit or 8))]:
        asset = str(position.get("asset", "") or "")
        category = str(position.get("category", "forex") or "forex")
        if asset and asset not in live_prices:
            assets.append((asset, category))
    if not assets:
        return live_prices

    from concurrent.futures import ThreadPoolExecutor, wait

    max_workers = min(4, len(assets))
    pool = ThreadPoolExecutor(max_workers=max_workers)
    try:
        futures = {
            pool.submit(_fetcher.get_real_time_price, asset, category): asset
            for asset, category in assets
        }
        done, not_done = wait(tuple(futures.keys()), timeout=4)
        for future in done:
            asset = futures[future]
            try:
                price, _ = future.result()
                if price:
                    live_prices[asset] = float(price)
            except Exception:
                pass
        if not_done:
            for future in not_done:
                future.cancel()
            logger.debug(f"[dashboard] command-center live price fetch timed out for {len(not_done)} asset(s)")
    finally:
        try:
            pool.shutdown(wait=False, cancel_futures=True)
        except Exception:
            pass
    return live_prices


def _build_command_center_enriched_positions(positions: Any, live_prices: Dict[str, float]) -> List[Dict[str, Any]]:
    enriched_positions: List[Dict[str, Any]] = []
    for position in list(positions or [])[:8]:
        current_price = live_prices.get(position.get("asset", ""), 0.0)
        entry_price = float(position.get("entry_price", 0) or 0)
        position_size = float(position.get("position_size", 0) or 0)
        direction = position.get("direction", position.get("signal", "BUY"))
        asset = str(position.get("asset", "") or "")
        category = str(position.get("category", "forex") or "forex")
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
        live_pnl = float(position.get("pnl", 0) or 0)
        if current_price and entry_price and position_size:
            try:
                from risk.position_sizer import PositionSizer as _PS

                live_pnl = _PS.pnl(asset, category, entry_price, current_price, position_size, direction)
            except Exception:
                live_pnl = (current_price - entry_price) * position_size if direction == "BUY" else (entry_price - current_price) * position_size
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


def _command_center_signal_quality(signals: Any) -> Dict[str, Any]:
    signal_list = list(signals or [])
    return {
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


@app.route("/api/command-center")
@_check_api_auth
@_check_rate_limit
def api_command_center():
    try:
        core = _core()
        perf, daily, positions, health, closed_trades = _command_center_core_snapshot(core)
        journals = _command_center_journals()

        # Slow external calls cached 5 minutes
        _cc_slow = _cache_get("cc_slow")
        if _cc_slow is None:
            _cc_slow = _fetch_command_center_slow_data()
        whale_recent = list((_cc_slow or {}).get("recent") or [])
        whale_count = int((_cc_slow or {}).get("alert_count_24h", 0) or 0)
        sent_score = float((_cc_slow or {}).get("sentiment_score", 0.0) or 0.0)
        _cache_set("cc_slow", _cc_slow, ttl=600 if whale_recent or whale_count or sent_score else 45)

        live_prices = _fetch_command_center_live_prices(positions, limit=8)
        enriched_positions = _build_command_center_enriched_positions(positions, live_prices)
        signals = _build_command_center_signals(enriched_positions)
        signal_quality = _command_center_signal_quality(signals)
        signal_diagnostics = _summarize_signal_diagnostics(enriched_positions)
        top_opportunities = _cache_get("cc_top_opportunities")
        weak_positions = _cache_get("cc_weak_positions")
        if core and (top_opportunities is None or weak_positions is None):
            if top_opportunities is None:
                try:
                    top_opportunities = _get_command_center_top_opportunities(core, limit=5)
                except Exception as exc:
                    logger.debug(f"[dashboard] command-center top_opportunities error: {exc}")
                    top_opportunities = []
            if weak_positions is None:
                try:
                    weak_positions = _get_command_center_weak_positions(core, limit=5)
                except Exception as exc:
                    logger.debug(f"[dashboard] command-center weak_positions error: {exc}")
                    weak_positions = []

        if top_opportunities is None:
            top_opportunities = []
        if weak_positions is None:
            weak_positions = []

        _cache_set("cc_top_opportunities", top_opportunities, ttl=20 if top_opportunities else 8)
        _cache_set("cc_weak_positions", weak_positions, ttl=20 if weak_positions else 8)
        near_misses = _summarize_near_misses(journals, limit=8)
        session_radar = _build_session_radar(limit=12)
        why_not_traded = _summarize_why_not_traded(journals, near_misses, limit=6)
        watchlist_ladder = _build_watchlist_ladder(top_opportunities, near_misses, session_radar, enriched_positions)
        trade_tape = _build_trade_tape(enriched_positions, closed_trades, limit=12)
        trade_lifecycle = _build_trade_lifecycle(enriched_positions, closed_trades, journals)

        return jsonify({
            "success":          True,
            "balance":          float(perf.get("balance", _args.balance) or _args.balance),
            "total_pnl":        float(perf.get("total_pnl", 0) or 0),
            "daily_pnl":        float(daily.get("daily_pnl", 0) or 0),
            "daily_trades":     int(daily.get("daily_trades", 0) or 0),
            "win_rate":         _wr(perf.get("win_rate", 0)),
            "open_positions":   len(enriched_positions),
            "total_trades":     int(perf.get("total_trades", 0) or 0),
            "engine_running":   health.get("is_running", core.is_running if core else False),
            "engine_ready":     health.get("engine_ready", core.is_ready if core else False),
            "sentiment_score":  _cc_slow["sentiment_score"],
            "whale_alerts_24h": _cc_slow["whale_alerts_24h"],
            "alert_count_24h":  _cc_slow["alert_count_24h"],
            "recent":           _cc_slow["recent"],
            "latest_signals":   signals,
            "signal_quality":   signal_quality,
            "top_opportunities": top_opportunities,
            "weak_positions":   weak_positions,
            "near_misses":      near_misses,
            "why_not_traded":   why_not_traded,
            "session_radar":    session_radar,
            "watchlist_ladder": watchlist_ladder,
            "trade_tape":       trade_tape,
            "trade_lifecycle":  trade_lifecycle,
            "positions":        enriched_positions,
            "provider_routing": _provider_routing_summary(),
            "signal_diagnostics": signal_diagnostics,
            "timestamp":        datetime.now().isoformat(),
        })
    except APIError as e:
        return handle_api_error(e, "/api/command-center", e.status_code)
    except Exception as e:
        return handle_api_error(e, "/api/command-center", 500)

# ══════════════════════════════════════════════════════════════════════════════
# API — SIGNALS
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/api/signals/live")
@_check_api_auth
@_check_rate_limit
def api_signals_live():
    try:
        core   = _core()
        filt   = request.args.get("filter", "all")
        signals = []

        if core:
            for p in core.get_positions():
                _asset = p.get("asset", "")
                _cat = p.get("category", "forex")
                _meta = dict(p.get("metadata") or {})
                _exec = _extract_execution_feedback_fields(_meta)
                _memory = _extract_memory_fields(_meta)
                _opportunity = _extract_opportunity_fields(_meta)
                _provenance = _extract_market_data_provenance_fields(_meta, asset=_asset, category=_cat)
                d = (p.get("direction") or p.get("signal", "BUY")).upper()
                c = float(p.get("confidence", 0))
                if filt == "buy"  and d != "BUY":  continue
                if filt == "sell" and d != "SELL": continue
                if filt == "high" and c < 0.70:    continue
                # Fetch live price for current_price display
                _cur_price = 0.0
                try:
                    _cp, _ = _get_fetcher().get_real_time_price(
                        _asset, _cat
                    )
                    if _cp:
                        _cur_price = float(_cp)
                except Exception:
                    pass

                signals.append({
                    "asset":         _asset,
                    "signal":        d, "direction": d,
                    "category":      _cat,
                    "confidence":    c,
                    "entry_price":   float(p.get("entry_price", 0)),
                    "current_price": _cur_price,
                    "stop_loss":     float(p.get("stop_loss", 0)),
                    "take_profit":   float(p.get("take_profit", 0)),
                    "position_size": float(p.get("position_size", 0)),
                    "strategy_id":   p.get("strategy_id", ""),
                    "pnl":           float(p.get("pnl", 0)),
                    "market_open":   is_market_open_for_asset(p.get("asset", ""))[0],
                    "generated_at":  str(p.get("open_time", "") or ""),
                    "metadata":      _meta,
                    "step_reached": p.get("step_reached", 0),
                    **_exec,
                    **_memory,
                    **_opportunity,
                    **_extract_signal_intelligence_fields(_meta),
                    **_provenance,
                })
        else:
            with _sig_lock:
                sigs = list(_sig_store.values())
            active = [s for s in sigs if s.get("signal", "HOLD") not in ("HOLD", "CLOSED")]
            if filt == "buy":    active = [s for s in active if s.get("signal") == "BUY"]
            elif filt == "sell": active = [s for s in active if s.get("signal") == "SELL"]
            elif filt == "high": active = [s for s in active if s.get("confidence", 0) >= 0.70]
            signals = []
            for s in active:
                _meta = dict(s.get("metadata") or {})
                signals.append({
                    **s,
                    "metadata": _meta,
                    **_extract_execution_feedback_fields(_meta),
                    **_extract_memory_fields(_meta),
                    **_extract_opportunity_fields(_meta),
                    **_extract_signal_intelligence_fields(_meta),
                    **_extract_market_data_provenance_fields(
                        _meta,
                        asset=str(s.get("asset", "") or ""),
                        category=str(s.get("category", "") or ""),
                    ),
                })

        buys     = sum(1 for s in signals if s.get("signal") == "BUY")
        sells    = sum(1 for s in signals if s.get("signal") == "SELL")
        avg_conf = sum(s.get("confidence", 0) for s in signals) / max(1, len(signals))
        return jsonify({
            "success": True, "signals": signals,
            "total_signals": len(signals), "buy_signals": buys,
            "sell_signals": sells, "avg_confidence": round(avg_conf * 100, 1),
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
        from config.config import IG_ENABLED, IG_ROUTED_CATEGORIES

        ig_primary = bool(IG_ENABLED) and normalized_category in set(IG_ROUTED_CATEGORIES or [])
    except Exception:
        ig_primary = False

    if ig_primary:
        primary_provider = "IG"
        secondary_provider = "Deriv"
        quote_mode = "stream"
        routing_label = "ig_primary_stream_deriv_fallback"
    else:
        try:
            from services.binance_market_bridge import binance_market_bridge

            if normalized_category == "crypto" and binance_market_bridge.supports(
                normalized_asset,
                category=normalized_category,
            ):
                secondary_provider = "Binance"
                routing_label = "deriv_primary_binance_fallback"
        except Exception:
            pass

    return {
        "symbol": normalized_asset,
        "category": normalized_category,
        "primary_provider": primary_provider,
        "secondary_provider": secondary_provider,
        "quote_mode": quote_mode,
        "routing_label": routing_label,
    }


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


def _history_allows_live_overlay(descriptor: Dict[str, Any], meta: Dict[str, Any]) -> bool:
    source_class = str((meta or {}).get("source_class") or "").strip().lower()
    if source_class == "stream_cache":
        return True
    primary = _provider_family((descriptor or {}).get("primary_provider") or "")
    if source_class == "local_store":
        latest_family = _provider_family((meta or {}).get("latest_provider_family") or "")
        latest_source_class = str((meta or {}).get("latest_source_class") or "").strip().lower()
        if primary and latest_family == primary and latest_source_class == "stream_cache":
            return True
    history = _provider_family((meta or {}).get("provider_family") or (meta or {}).get("source") or "")
    return bool(primary and history and primary == history)


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


def _fetch_ohlcv_with_allowance_retry(
    asset: str,
    category: str,
    interval: str,
    periods: int,
    *,
    closed_only: bool = False,
) -> Tuple[Optional["pd.DataFrame"], Dict[str, Any], int]:
    import pandas as pd

    fetcher = _get_fetcher()
    requested = max(2, int(periods or 0))
    try:
        df = fetcher.get_ohlcv(asset, category, interval=interval, periods=requested, closed_only=closed_only)
    except TypeError:
        df = fetcher.get_ohlcv(asset, category, interval=interval, periods=requested)
    meta = fetcher.get_last_ohlcv_metadata(asset, interval)
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
                )
            except TypeError:
                retry_df = fetcher.get_ohlcv(
                    asset,
                    category,
                    interval=interval,
                    periods=retry_periods,
                )
            retry_meta = fetcher.get_last_ohlcv_metadata(asset, interval)
            if retry_df is not None and not retry_df.empty:
                return retry_df, retry_meta, retry_periods
            meta = retry_meta
            used = retry_periods

    if isinstance(df, pd.DataFrame):
        df = df.copy()
    return df, meta, used


def _heatmap_item(asset: str, category: str) -> Optional[Dict[str, Any]]:
    import pandas as pd

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
        if df_daily is not None and not df_daily.empty and "close" in df_daily.columns:
            closes = df_daily["close"].astype(float).dropna()
            opens = df_daily["open"].astype(float).dropna() if "open" in df_daily.columns else pd.Series(dtype=float)
            if not closes.empty:
                if current_price is None:
                    current_price = float(closes.iloc[-1])
                if len(closes) >= 2:
                    reference_price = float(closes.iloc[-2])
                elif not opens.empty:
                    reference_price = float(opens.iloc[-1])
                source = str((daily_meta or {}).get("source") or source or "")

    if reference_price is None or current_price is None:
        df_intraday, intraday_meta, _ = _fetch_ohlcv_with_allowance_retry(asset, category, "1h", 30)
        if df_intraday is not None and not df_intraday.empty and "close" in df_intraday.columns:
            closes = df_intraday["close"].astype(float).dropna()
            if not closes.empty:
                if current_price is None:
                    current_price = float(closes.iloc[-1])
                if len(closes) >= 2:
                    target_ts = closes.index.max() - pd.Timedelta(hours=24)
                    history = closes[closes.index <= target_ts]
                    reference_price = float(history.iloc[-1] if not history.empty else closes.iloc[0])
                source = str((intraday_meta or {}).get("source") or source or "")

    if ig_primary and (reference_price is None or current_price is None):
        stream_df = _stream_candles_from_live_feed(asset, "5m", 288, source_hint="IG")
        if stream_df is not None and not stream_df.empty and "close" in stream_df.columns:
            closes = stream_df["close"].astype(float).dropna()
            if not closes.empty:
                if current_price is None:
                    current_price = float(closes.iloc[-1])
                if reference_price is None and len(closes) >= 2:
                    target_ts = closes.index.max() - pd.Timedelta(hours=24)
                    history = closes[closes.index <= target_ts]
                    reference_price = float(history.iloc[-1] if not history.empty else closes.iloc[0])
                source = source or "IG Stream"

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

    bucket_seconds = {
        "1m": 60,
        "5m": 300,
        "15m": 900,
        "30m": 1800,
        "1h": 3600,
        "4h": 14400,
        "1d": 86400,
    }.get(str(interval or "").lower())
    if not bucket_seconds:
        return None

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

    if not samples:
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

    if len(samples) < 2:
        return None

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

    if not rows:
        return None

    frame = pd.DataFrame(rows).set_index("timestamp").sort_index()
    return frame.tail(int(max(2, periods)))


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
        import pandas as pd
        from config.config import get_chart_timeframe_periods
        asset    = request.args.get("asset", "EUR/USD")
        interval = request.args.get("interval", "15m")
        cat      = _cat(asset)
        descriptor = _chart_asset_descriptor(asset, cat)
        ig_primary = str(descriptor.get("primary_provider") or "").upper() == "IG"
        requested_periods = int(get_chart_timeframe_periods(interval))
        periods = _chart_period_limit(asset, cat, interval, requested_periods)
        cache_key = f"chart_candles:{asset}:{cat}:{interval}:{periods}"
        last_good_key = f"chart_candles_last:{asset}:{cat}:{interval}"
        cached_payload = _cache_get(cache_key)
        if cached_payload:
            return jsonify(cached_payload)

        # Try requested interval then fall back for forex/indices intraday gaps
        fallbacks = {
            "1m": ["5m", "15m", "30m", "1h", "4h", "1d"],
            "5m": ["15m", "30m", "1h", "4h", "1d"],
            "15m": ["30m", "1h", "4h", "1d"],
            "30m": ["1h", "4h", "1d"],
            "1h": ["4h", "1d"],
            "4h": ["1d"],
        }
        df = _fetcher.get_ohlcv(asset, cat, interval=interval, periods=periods)
        used = interval
        meta = {}
        try:
            meta = _fetcher.get_last_ohlcv_metadata(asset, interval)
        except Exception:
            meta = {}
        if (df is None or df.empty) and _is_historical_allowance_error(meta):
            retry_periods = _chart_allowance_retry_limit(asset, cat, interval, periods)
            if retry_periods < periods:
                retry_df = _fetcher.get_ohlcv(asset, cat, interval=interval, periods=retry_periods)
                if retry_df is not None and not retry_df.empty:
                    df = retry_df
                    periods = retry_periods
                    try:
                        meta = _fetcher.get_last_ohlcv_metadata(asset, interval)
                    except Exception:
                        meta = {}

        if ig_primary:
            if df is None or df.empty:
                stream_df = _stream_candles_from_live_feed(asset, interval, periods, source_hint="IG")
                if stream_df is not None and not stream_df.empty:
                    df = stream_df
                    used = interval
                    meta = {
                        "source": "IG Stream",
                        "source_class": "stream_cache",
                        "provider_warning_message": str(meta.get("provider_error_message") or ""),
                        "provider_warning_code": str(meta.get("provider_error_code") or ""),
                    }

        allow_fallback = cat in ("forex", "indices")
        if (df is None or df.empty) and allow_fallback and interval in fallbacks:
            for fb in fallbacks[interval]:
                fb_periods = _chart_period_limit(asset, cat, fb, int(get_chart_timeframe_periods(fb)))
                df = _fetcher.get_ohlcv(asset, cat, interval=fb, periods=fb_periods)
                if df is not None and not df.empty:
                    used = fb
                    periods = fb_periods
                    break

        if df is None or df.empty:
            last_good_payload = _cache_get(last_good_key)
            if _is_historical_allowance_error(meta) and last_good_payload:
                fallback_payload = dict(last_good_payload)
                fallback_payload["cached"] = True
                fallback_payload["provider_warning_code"] = str(meta.get("provider_error_code") or "")
                fallback_payload["provider_warning_message"] = str(meta.get("provider_error_message") or meta.get("message") or "")
                fallback_payload["message"] = "Using cached candles due to provider historical allowance."
                return jsonify(fallback_payload)
            message = str(meta.get("provider_error_message") or f"No data for {asset}")
            if _is_historical_allowance_error(meta):
                message = "IG historical allowance exceeded for this chart. Try a higher timeframe or wait for allowance reset."
            return jsonify({"success": True, "candles": [],
                            "message": message,
                            "interval_used": interval,
                            "bars_requested": periods,
                            "data_source": meta.get("source"),
                            "provider_error_code": meta.get("provider_error_code"),
                            "provider_error_message": meta.get("provider_error_message")})

        candles = _serialize_chart_candles(df)
        if str(meta.get("source_class") or "") != "stream_cache":
            meta = {}
            try:
                meta = _fetcher.get_last_ohlcv_metadata(asset, used)
            except Exception:
                meta = {}
        payload = {
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
        requested = _deep_history_bar_limit(interval, int(request.args.get("bars", "500")))
        end_time = _parse_chart_history_end_time(request.args.get("end_time"))
        closed_only = end_time is not None
        end_key = end_time.isoformat() if end_time is not None else "latest"
        cache_key = f"chart_history:{asset}:{cat}:{interval}:{requested}:{end_key}"
        cached_payload = _cache_get(cache_key)
        if cached_payload:
            return jsonify(cached_payload)

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
        while True:
            try:
                price = None
                source = None
                try:
                    from websocket_dashboard import get_live_price

                    price, source = get_live_price(asset, max_age_seconds=15.0)
                except Exception:
                    price, source = None, None

                if price is None:
                    price, _ = _fetcher.get_real_time_price(asset, cat)
                    try:
                        source = str((_fetcher.get_last_price_metadata(asset) or {}).get("source") or source or "")
                    except Exception:
                        source = source or ""
                if price:
                    yield f"data: {json.dumps({'type': 'tick', 'price': price, 'asset': asset, 'source': source, 'ts': int(time.time())})}\n\n"
                else:
                    yield f"data: {json.dumps({'type': 'heartbeat', 'asset': asset, 'ts': int(time.time())})}\n\n"
            except Exception:
                yield f"data: {json.dumps({'type': 'heartbeat', 'asset': asset, 'ts': int(time.time())})}\n\n"
            time.sleep(3)
    return Response(stream_with_context(_gen()), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

@app.route("/api/market/heatmap")
@_check_api_auth
@_check_rate_limit
def api_market_heatmap():
    cache_key = "heatmap:v2"
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
        max_workers = min(8, len(ALL_ASSETS))
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
        _cache_set(cache_key, payload, ttl=120)   # refresh every 120s for live price changes
        return jsonify(payload)
    except APIError as e:
        return handle_api_error(e, "/api/market/heatmap", e.status_code)
    except Exception as e:
        return handle_api_error(e, "/api/market/heatmap", 500)

@app.route("/api/correlation-matrix")
def api_correlation_matrix():
    cached = _cache_get("correlation")
    if cached is not None:
        try:
            import math

            labels = cached.get("labels") or []
            matrix = cached.get("matrix") or []
            invalid = any(
                isinstance(value, (int, float)) and not math.isfinite(value)
                for row in matrix for value in row
            )
            if labels and matrix and not invalid:
                return jsonify(cached)
        except Exception:
            pass
    try:
        import pandas as pd
        from concurrent.futures import ThreadPoolExecutor, wait

        assets = [a for a, _ in ALL_ASSETS]
        plans = [
            ("1d", 45, 8),
            ("1h", 240, 24),
            ("5m", 288, 24),
        ]
        best_payload: Optional[Dict[str, Any]] = None
        best_available_assets = -1

        for interval, periods, min_points in plans:
            def _fetch_close(a):
                cat = _cat(a)
                try:
                    df, meta, _ = _fetch_ohlcv_with_allowance_retry(a, cat, interval, periods)
                    descriptor = _chart_asset_descriptor(a, cat)
                    ig_primary = str(descriptor.get("primary_provider") or "").upper() == "IG"
                    if ig_primary and (df is None or df.empty):
                        stream_df = _stream_candles_from_live_feed(a, interval, periods, source_hint="IG")
                        if stream_df is not None and not stream_df.empty:
                            df = stream_df
                    if df is not None and not df.empty and "close" in df.columns:
                        normalized = _normalize_correlation_series(df["close"].astype(float), interval)
                        if not normalized.empty:
                            return a, normalized.tail(periods)
                except Exception:
                    pass
                return a, None

            closes: Dict[str, Any] = {}
            pool = ThreadPoolExecutor(max_workers=6)
            try:
                futures = {pool.submit(_fetch_close, a): a for a in assets}
                done, not_done = wait(futures, timeout=30)
                for future in done:
                    try:
                        a, series = future.result()
                        if series is not None:
                            closes[a] = series
                    except Exception:
                        pass
                if not_done:
                    logger.warning(f"[Correlation] timeout fetching {len(not_done)} assets for {interval}: {[futures[f] for f in not_done]}")
                    for fut in not_done:
                        fut.cancel()
            finally:
                try:
                    pool.shutdown(wait=False, cancel_futures=True)
                except Exception:
                    pass

            if len(closes) < 2:
                continue

            frame = pd.DataFrame(closes).sort_index()
            returns = frame.pct_change(fill_method=None)
            returns = returns.dropna(axis=1, thresh=min_points)
            if returns.shape[1] < 2:
                continue

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

            candidate = {
                "success": True,
                "labels": assets,
                "matrix": matrix,
                "interval": interval,
                "expected_assets": len(assets),
                "available_assets": int(returns.shape[1]),
                "partial": int(returns.shape[1]) < len(assets),
            }
            if int(returns.shape[1]) > best_available_assets:
                best_payload = candidate
                best_available_assets = int(returns.shape[1])
            if int(returns.shape[1]) >= len(assets):
                break

        if not best_payload:
            return jsonify({"success": False, "error": "Not enough price data — try again in 30s"})

        payload = best_payload
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
    asset_playbook_matrix = _summarize_asset_playbook_matrix(signal_list, closed_trades, limit=10)
    failure_archetypes = _summarize_failure_archetypes(journals, closed_trades, limit=6)
    confidence_decomposition = _summarize_confidence_decomposition(signal_list)
    payload = {
        "success": True,
        "accuracy": accuracy,
        "signals": signal_list,
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

@app.route("/api/whale/summary")
@_check_api_auth
@_check_rate_limit
def api_whale_summary():
    cached = _cache_get("whale_summary")
    if cached is not None:
        return jsonify(cached)
    try:
        mi = _get_market_intelligence()
        if not mi:
            return jsonify({"success": True, "alerts": [], "total_volume_usd": 0,
                            "top_assets": [], "recent": [], "alert_count_24h": 0})
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
        ttl = 300 if payload.get("alerts") or payload.get("recent") else 45
        _cache_set("whale_summary", payload, ttl=ttl)
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
        timed_out_tasks = 0

        pool = None
        try:
            from concurrent.futures import ThreadPoolExecutor, wait

            pool = ThreadPoolExecutor(max_workers=5)
            futures = {
                pool.submit(sa.get_comprehensive_sentiment): "market_sentiment",
                pool.submit(sa.fetch_fear_greed_index): "fear_greed",
                pool.submit(sa.fetch_vix): "vix",
                pool.submit(sa.news_integrator.fetch_all_sources): "articles",
                pool.submit(sa.fetch_whale_alerts, min_value_usd=1_000_000): "whales",
            }
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
    cached = _cache_get("sentiment_by_asset")
    if cached is not None:
        return jsonify(cached)
    try:
        mi = _get_market_intelligence()
        if not mi:
            return jsonify({"success": False, "error": "Market intelligence unavailable"})

        watch = [a for a, _ in ALL_ASSETS]
        from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FuturesTimeout

        def _sent_one(asset):
            cat = _cat(asset)
            if _is_market_weekend(cat):
                return {
                    "asset":    asset,
                    "category": cat,
                    "score":    0.0,
                    "label":    "Market Closed",
                }
            try:
                snapshot = mi.get_asset_snapshot(asset, cat)
                score = float(snapshot.get("sentiment_score", 0.0) or 0.0)
            except Exception:
                score = 0.0
            return {
                "asset":    asset,
                "category": cat,
                "score":    round(score, 3),
                "label":    "Bullish" if score > 0.1 else "Bearish" if score < -0.1 else "Neutral",
            }

        results = []
        seen = set()  # prevent duplicates
        max_workers = min(8, len(watch))

        pool = ThreadPoolExecutor(max_workers=max_workers)
        try:
            futures = {pool.submit(_sent_one, a): a for a in watch}
            try:
                for future in as_completed(futures, timeout=12):
                    try:
                        r = future.result()
                        asset_key = r.get("asset", "")
                        if asset_key and asset_key not in seen:
                            seen.add(asset_key)
                            results.append(r)
                    except Exception:
                        pass
            except FuturesTimeout:
                # On timeout — collect completed futures, fill rest with neutral
                for future, asset in futures.items():
                    if asset in seen:
                        continue
                    if future.done():
                        try:
                            r = future.result()
                            seen.add(asset)
                            results.append(r)
                        except Exception:
                            pass
                    if asset not in seen:
                        # Never completed — neutral placeholder
                        seen.add(asset)
                        results.append({
                            "asset": asset, "category": _cat(asset),
                            "score": 0.0, "label": "Neutral",
                        })
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

        results.sort(key=lambda x: x["score"], reverse=True)
        payload = {"success": True, "assets": results}
        # Cache 10 minutes — Reddit data doesn't change that fast
        _cache_set("sentiment_by_asset", payload, ttl=600)
        return jsonify(payload)
    except APIError as e:
        return handle_api_error(e, "/api/sentiment/by-asset", e.status_code)
    except Exception as e:
        return handle_api_error(e, "/api/sentiment/by-asset", 500)

@app.route("/api/market/events")
@_check_api_auth
@_check_rate_limit
def api_market_events():
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
        return jsonify(payload)
    except APIError as e:
        return handle_api_error(e, "/api/market/events", e.status_code)
    except Exception as e:
        return handle_api_error(e, "/api/market/events", 500)

# ══════════════════════════════════════════════════════════════════════════════
# API — RISK DASHBOARD
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/api/risk/portfolio")
@_check_api_auth
@_check_rate_limit
def api_risk_portfolio():
    cached = _cache_get("risk_portfolio")
    if cached is not None:
        return jsonify(cached)
    try:
        core = _core()
        if not core:
            return jsonify({"success": False, "error": "Engine not ready"})

        positions = core.get_positions()
        balance   = core.get_balance()
        perf      = core.get_performance()

        risk_stats: Dict = {}
        try:
            if hasattr(core, "portfolio_risk") and core.portfolio_risk:
                risk_stats = core.portfolio_risk.get_portfolio_stats(positions, balance)
        except Exception:
            pass

        by_cat: Dict = {}
        by_cluster: Dict[str, Dict[str, Any]] = {}
        for p in positions:
            cat = p.get("category", "unknown")
            cluster = _risk_cluster_group(p.get("asset", ""), cat)
            direction = str(p.get("direction") or p.get("signal") or "BUY").upper()
            meta = dict(p.get("metadata") or {})
            memory = _extract_memory_fields(meta)
            execution = _extract_execution_feedback_fields(meta)
            opportunity = _extract_opportunity_fields(meta)
            intelligence = _extract_signal_intelligence_fields(meta)
            by_cat.setdefault(cat, {
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
            })
            by_cluster.setdefault(cat and cluster or "Unclassified", {
                "label": cluster or "Unclassified",
                "count": 0,
                "pnl": 0.0,
                "exposure": 0.0,
                "buy_count": 0,
                "sell_count": 0,
                "execution_scores": [],
                "memory_scores": [],
                "opportunity_scores": [],
            })
            by_cat[cat]["count"]    += 1
            by_cat[cat]["pnl"]      += float(p.get("pnl") or 0)
            by_cat[cat]["exposure"] += float(p.get("position_size", 0)) * float(p.get("entry_price", 0))
            by_cluster[cluster]["count"] += 1
            by_cluster[cluster]["pnl"] += float(p.get("pnl") or 0)
            by_cluster[cluster]["exposure"] += float(p.get("position_size", 0)) * float(p.get("entry_price", 0))
            if direction == "SELL":
                by_cluster[cluster]["sell_count"] += 1
            else:
                by_cluster[cluster]["buy_count"] += 1
            if float(memory.get("memory_score", 0.0) or 0.0) > 0:
                by_cat[cat]["memory_scores"].append(float(memory.get("memory_score", 0.0) or 0.0))
                by_cluster[cluster]["memory_scores"].append(float(memory.get("memory_score", 0.0) or 0.0))
            if float(execution.get("execution_quality_score", 0.0) or 0.0) > 0:
                by_cat[cat]["execution_scores"].append(float(execution.get("execution_quality_score", 0.0) or 0.0))
                by_cluster[cluster]["execution_scores"].append(float(execution.get("execution_quality_score", 0.0) or 0.0))
            if float(opportunity.get("opportunity_score", 0.0) or 0.0) > 0:
                by_cat[cat]["opportunity_scores"].append(float(opportunity.get("opportunity_score", 0.0) or 0.0))
                by_cluster[cluster]["opportunity_scores"].append(float(opportunity.get("opportunity_score", 0.0) or 0.0))
            if float(intelligence.get("broker_quality_score", 0.0) or 0.0) > 0:
                by_cat[cat]["broker_scores"].append(float(intelligence.get("broker_quality_score", 0.0) or 0.0))
            if abs(float(intelligence.get("microstructure_score", 0.0) or 0.0)) > 1e-9:
                by_cat[cat]["micro_scores"].append(float(intelligence.get("microstructure_score", 0.0) or 0.0))
            if abs(float(intelligence.get("cross_asset_alignment", 0.0) or 0.0)) > 1e-9:
                by_cat[cat]["cross_alignments"].append(float(intelligence.get("cross_asset_alignment", 0.0) or 0.0))
            if str(intelligence.get("depth_mode") or "") == "true_depth":
                by_cat[cat]["true_depth_count"] += 1
            elif str(intelligence.get("depth_mode") or "") == "synthetic_depth":
                by_cat[cat]["synthetic_depth_count"] += 1
            if str(intelligence.get("cross_asset_context_state") or "") == "conflicted":
                by_cat[cat]["cross_conflict_count"] += 1
            if bool(intelligence.get("recent_pattern_block_new_entries")):
                by_cat[cat]["recent_block_count"] += 1

        for cat, info in by_cat.items():
            memory_scores = info.pop("memory_scores", [])
            execution_scores = info.pop("execution_scores", [])
            opportunity_scores = info.pop("opportunity_scores", [])
            broker_scores = info.pop("broker_scores", [])
            micro_scores = info.pop("micro_scores", [])
            cross_alignments = info.pop("cross_alignments", [])
            info["avg_memory_score"] = round(sum(memory_scores) / len(memory_scores), 1) if memory_scores else 0.0
            info["avg_execution_quality"] = round(sum(execution_scores) / len(execution_scores), 1) if execution_scores else 0.0
            info["avg_opportunity_score"] = round(sum(opportunity_scores) / len(opportunity_scores), 3) if opportunity_scores else 0.0
            info["avg_broker_quality"] = round(sum(broker_scores) / len(broker_scores), 3) if broker_scores else 0.0
            info["avg_microstructure_score"] = round(sum(micro_scores) / len(micro_scores), 3) if micro_scores else 0.0
            info["avg_cross_asset_alignment"] = round(sum(cross_alignments) / len(cross_alignments), 3) if cross_alignments else 0.0

        cluster_groups: List[Dict[str, Any]] = []
        for info in by_cluster.values():
            execution_scores = list(info.pop("execution_scores", []) or [])
            memory_scores = list(info.pop("memory_scores", []) or [])
            opportunity_scores = list(info.pop("opportunity_scores", []) or [])
            count = int(info.get("count", 0) or 0)
            buy_count = int(info.get("buy_count", 0) or 0)
            sell_count = int(info.get("sell_count", 0) or 0)
            info["avg_execution_quality"] = round(sum(execution_scores) / len(execution_scores), 1) if execution_scores else 0.0
            info["avg_memory_score"] = round(sum(memory_scores) / len(memory_scores), 1) if memory_scores else 0.0
            info["avg_opportunity_score"] = round(sum(opportunity_scores) / len(opportunity_scores), 3) if opportunity_scores else 0.0
            info["direction_skew"] = "SELL" if sell_count > buy_count else "BUY" if buy_count > sell_count else "MIXED"
            info["skew_ratio"] = round((max(buy_count, sell_count) / count) * 100.0, 1) if count else 0.0
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

        closed  = core.get_closed_trades(limit=100)
        wins    = [t for t in closed if float(t.get("pnl") or 0) > 0]
        losses  = [t for t in closed if float(t.get("pnl") or 0) <= 0 and float(t.get("pnl") or 0) != 0]
        avg_win = sum(float(t.get("pnl") or 0) for t in wins)   / len(wins)   if wins   else 0.0
        avg_los = sum(float(t.get("pnl") or 0) for t in losses) / len(losses) if losses else 0.0
        pf      = abs(avg_win / avg_los) if avg_los else 0.0

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
        try:
            weak_queue = _get_command_center_weak_positions(core, limit=5)
        except Exception:
            weak_queue = []

        stop_concentration = _summarize_stop_concentration(positions, limit=5)
        scenario_risk = _summarize_scenario_risk(positions)

        payload = {
            "success":        True,
            "balance":        balance,
            "open_positions": len(positions),
            "total_exposure": risk_stats.get("total_exposure", 0),
            "exposure_pct":   risk_stats.get("exposure_pct", 0),
            "drawdown_pct":   risk_stats.get("drawdown_pct", 0),
            "peak_balance":   risk_stats.get("peak_balance", balance),
            "by_category":    by_cat,
            "cluster_groups": cluster_groups,
            "win_rate":       _wr(perf.get("win_rate", 0)),
            "profit_factor":  round(pf, 2),
            "avg_win":        round(avg_win, 2),
            "avg_loss":       round(avg_los, 2),
            "total_trades":   perf.get("total_trades", 0),
            "total_pnl":      perf.get("total_pnl", 0),
            "quality_snapshot": {
                "avg_memory_score": round(
                    sum(float(item.get("avg_memory_score", 0.0) or 0.0) for item in by_cat.values()) / len(by_cat),
                    1,
                ) if by_cat else 0.0,
                "avg_execution_quality": round(
                    sum(float(item.get("avg_execution_quality", 0.0) or 0.0) for item in by_cat.values()) / len(by_cat),
                    1,
                ) if by_cat else 0.0,
                "avg_opportunity_score": round(
                    sum(float(item.get("avg_opportunity_score", 0.0) or 0.0) for item in by_cat.values()) / len(by_cat),
                    3,
                ) if by_cat else 0.0,
                "avg_broker_quality": round(
                    sum(float(item.get("avg_broker_quality", 0.0) or 0.0) for item in by_cat.values()) / len(by_cat),
                    3,
                ) if by_cat else 0.0,
                "avg_microstructure_score": round(
                    sum(float(item.get("avg_microstructure_score", 0.0) or 0.0) for item in by_cat.values()) / len(by_cat),
                    3,
                ) if by_cat else 0.0,
                "avg_cross_asset_alignment": round(
                    sum(float(item.get("avg_cross_asset_alignment", 0.0) or 0.0) for item in by_cat.values()) / len(by_cat),
                    3,
                ) if by_cat else 0.0,
                "top_category": (
                    max(
                        by_cat.items(),
                        key=lambda kv: float(kv[1].get("avg_opportunity_score", 0.0) or 0.0),
                    )[0]
                    if by_cat else ""
                ),
            },
            "execution_feedback": execution_summary,
            "execution_by_category": execution_by_category,
            "stop_concentration": stop_concentration,
            "scenario_risk": scenario_risk,
            "weak_queue": weak_queue,
            "signal_diagnostics": _summarize_signal_diagnostics(
                [_extract_signal_intelligence_fields(dict(p.get("metadata") or {})) for p in positions]
            ),
        }
        _cache_set("risk_portfolio", payload, ttl=10)
        return jsonify(payload)
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
        core = _core()
        if not core:
            return jsonify({"success": False, "error": "Engine not ready"})
        stats  = core.get_strategy_stats()
        trades = core.get_closed_trades(limit=200)
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
                if t.get("strategy_id") != strat:
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
        return jsonify({"success": True, "strategies": enriched, "timeline": timeline, "summary": summary})
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
    cache_key = "intelligence_alerts_overview"
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

@app.route("/api/trade-history")
def api_trade_history():
    """Return last N closed trades with full details for the history panel."""
    try:
        limit = int(request.args.get("limit", 50))
        raw_limit = max(limit * 3, limit + 10)
        from core.state import rollup_closed_trade_history
        from risk.position_sizer import PositionSizer as _PS
        trades = rollup_closed_trade_history(_load_authoritative_closed_trades(limit=raw_limit), limit=limit)
        from config.config import TZ_NAME
        def _infer_partial_trade_shape(trade_id: str, metadata: dict, exit_reason: str) -> tuple[str | None, bool]:
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
        def _enrich(trade):
            d = dict(trade)
            _meta = dict(d.get("metadata") or {})
            parent_trade_id, is_partial_close = _infer_partial_trade_shape(
                str(d.get("trade_id") or ""),
                _meta,
                str(d.get("exit_reason") or ""),
            )
            d["parent_trade_id"] = parent_trade_id
            d["is_partial_close"] = is_partial_close
            d.update(_extract_execution_feedback_fields(_meta))
            d.update(_extract_memory_fields(_meta))
            d.update(_extract_opportunity_fields(_meta))
            d.update(_extract_signal_intelligence_fields(_meta))
            d.update(
                _extract_market_data_provenance_fields(
                    _meta,
                    asset=str(d.get("asset") or ""),
                    category=str(d.get("category") or ""),
                )
            )
            try:
                lot_size = d.get("lot_size")
                if lot_size in (None, ""):
                    lot_size = _meta.get("lot_size")
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
                    mins = int(secs / 60)
                    if mins < 60:
                        d["duration_str"] = f"{mins}m"
                    elif mins < 1440:
                        d["duration_str"] = f"{mins//60}h {mins%60}m"
                    else:
                        d["duration_str"] = f"{mins//1440}d {(mins%1440)//60}h"
                else:
                    duration_minutes = d.get("duration_minutes")
                    if duration_minutes not in (None, ""):
                        mins = max(0, int(float(duration_minutes)))
                        if mins < 60:
                            d["duration_str"] = f"{mins}m"
                        elif mins < 1440:
                            d["duration_str"] = f"{mins//60}h {mins%60}m"
                        else:
                            d["duration_str"] = f"{mins//1440}d {(mins%1440)//60}h"
                    else:
                        d["duration_str"] = "—"
            except Exception:
                d["duration_str"] = "—"
            return d
        response = jsonify({
            "success": True,
            "trades": [_enrich(t) for t in trades],
            "count": len(trades),
        })
        response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
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
        _invalidate_cache_prefixes("risk_portfolio", "page_overview:command_center:", "page_overview:risk_dashboard:")
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
        _invalidate_cache_prefixes("risk_portfolio", "page_overview:command_center:", "page_overview:risk_dashboard:")
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
        _invalidate_cache_prefixes("risk_portfolio", "page_overview:command_center:", "page_overview:risk_dashboard:")
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
        _invalidate_cache_prefixes("risk_portfolio", "page_overview:command_center:", "page_overview:risk_dashboard:")
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

@app.route("/api/system/health")
def api_system_health():
    cached = _cache_get("system_health")
    if cached is not None:
        return jsonify(cached)
    try:
        core   = _core()
        health = core.health_report() if core else {}

        ram_pct = cpu_pct = disk_pct = proc_mb = 0.0
        try:
            import psutil
            ram_pct  = psutil.virtual_memory().percent
            cpu_pct  = psutil.cpu_percent(interval=0)
            disk_pct = psutil.disk_usage("/").percent
            proc_mb  = round(psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024, 1)
        except Exception:
            pass

        redis_ok = False
        try:
            if _redis_broker: redis_ok = bool(_redis_broker.is_connected())
        except Exception:
            pass

        db_ok = False
        try:
            from services.db_pool import get_db
            db_ok = bool(get_db().ping())
        except Exception:
            pass

        tg_ok = bool(getattr(telegram_manager, "is_running", False))

        processes = {
            "TradingCore":       health.get("is_running", core.is_running if core else False),
            "Engine ready":      health.get("engine_ready", core.is_ready if core else False),
            "Web dashboard":     True,
            "Redis":             redis_ok,
            "PostgreSQL":        db_ok,
            "Telegram":          tg_ok,
            "PredTracker":       _pred_tracker is not None,
            "WebSocket streams": _ws_ok,
        }

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

        feed_connections: Dict[str, Any] = {}
        try:
            from websocket_dashboard import connection_status as _connection_status

            feed_connections = copy.deepcopy(dict(_connection_status or {}))
        except Exception:
            feed_connections = {}

        payload = {
            "success":          True,
            "ram_pct":          round(ram_pct, 1),
            "cpu_pct":          round(cpu_pct, 1),
            "disk_pct":         round(disk_pct, 1),
            "process_mem_mb":   proc_mb,
            "processes":        processes,
            "phase_health":     phase_health,
            "feed_connections": feed_connections,
            "open_positions":   health.get("open_positions", 0),
            "active_cooldowns": health.get("active_cooldowns", 0),
            "source_health":    dict(health.get("source_health") or {}),
            "stale_sources":    list(health.get("stale_sources") or []),
            "stale_source_count": int(health.get("stale_source_count", 0) or 0),
            "never_seen_sources": list(health.get("never_seen_sources") or []),
            "never_seen_source_count": int(health.get("never_seen_source_count", 0) or 0),
            "ig_broker":       dict(health.get("ig_broker") or {}),
            "recent_error_count": int(health.get("recent_error_count", 0) or 0),
            "recent_errors":    list(health.get("recent_errors") or []),
            "issues":           health.get("issues", []),
            "strategy_mode":    health.get("strategy_mode", "—"),
            "balance":          health.get("balance", _args.balance),
            "timestamp":        datetime.now().isoformat(),
        }
        _cache_set("system_health", payload, ttl=10)
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
    cache_key = "system_monitor_overview"
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
    _cache_set(cache_key, payload, ttl=10)
    return jsonify(payload)


@app.route("/api/page-overview")
@_check_api_auth
@_check_rate_limit
def api_page_overview():
    page = request.args.get("page", "").strip().lower()
    days = min(int(request.args.get("days", 30)), 90)
    if not page:
        return handle_api_error(BadRequest("page query required"), "/api/page-overview", 400)

    cache_key = f"page_overview:{page}:{days}"
    cached = _cache_get(cache_key)
    if cached:
        return jsonify(_response_to_dict(cached))

    if page == "risk_dashboard":
        status = _response_to_dict(_call_view(api_status))
        risk = _response_to_dict(_call_view(api_risk_portfolio))
        payload = {"success": True, "page": page, "status": status, "risk": risk}
        ttl = 10
    elif page == "ai_predictions":
        payload = _response_to_dict(_call_view(api_ai_predictions_overview))
        payload["status"] = _response_to_dict(_call_view(api_status))
        ttl = 10
    elif page == "intelligence_alerts":
        payload = _response_to_dict(_call_view(api_intelligence_alerts_overview))
        payload["status"] = _response_to_dict(_call_view(api_status))
        ttl = 10
    elif page == "system_monitor":
        payload = _response_to_dict(_call_view(api_system_monitor_overview))
        ttl = 10
    elif page == "whale_intelligence":
        payload = _response_to_dict(_call_view(api_whale_summary))
        payload["status"] = _response_to_dict(_call_view(api_status))
        ttl = 30
    elif page == "sentiment_intelligence":
        payload = {
            "success": True,
            "status": _response_to_dict(_call_view(api_status)),
            "sentiment": _response_to_dict(_call_view(api_sentiment_dashboard)),
            "by_asset": _response_to_dict(_call_view(api_sentiment_by_asset)),
            "events": _response_to_dict(_call_view(api_market_events)),
            "heatmap": _response_to_dict(_call_view(api_market_heatmap)),
        }
        ttl = 30
    elif page == "order_flow":
        payload = {
            "success": True,
            "status": _response_to_dict(_call_view(api_status)),
            "imbalance": _response_to_dict(_call_view(api_phase3_imbalance)),
            "walls": _response_to_dict(_call_view(api_phase3_walls)),
            "hunts": _response_to_dict(_call_view(api_phase3_stop_hunts)),
        }
        ttl = 15
    elif page == "command_center":
        command_center = _response_to_dict(_call_view(api_command_center))
        payload = {
            "success": True,
            "command_center": command_center,
            "whale": {
                "success": bool(command_center.get("success", False)),
                "recent": list(command_center.get("recent", []) or []),
                "alert_count_24h": int(command_center.get("alert_count_24h", 0) or 0),
                "whale_alerts_24h": int(command_center.get("whale_alerts_24h", 0) or 0),
            },
        }
        ttl = 15
    elif page == "market_intelligence":
        payload = {
            "success": True,
            "status": _response_to_dict(_call_view(api_status)),
            "assets": _response_to_dict(_call_view(api_chart_assets)),
            "events": _response_to_dict(_call_view(api_market_events)),
        }
        ttl = 30
    else:
        return handle_api_error(BadRequest(f"Unknown overview page '{page}'"), "/api/page-overview", 400)

    payload = _response_to_dict(payload)
    _cache_set(cache_key, payload, ttl=ttl)
    return jsonify(payload)


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
                    sent_start = False
                    for output in response_body:
                        if not response_started:
                            raise RuntimeError("WSGI app did not call start_response")

                        if not sent_start:
                            send({"type": "http.response.start", "status": status_code, "headers": headers})
                            sent_start = True

                        send({"type": "http.response.body", "body": output, "more_body": True})

                    if response_started and not sent_start:
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


def start_dashboard(core, host: str = "0.0.0.0", port: int = 5000, http2: bool = False, ssl_cert: str | None = None, ssl_key: str | None = None) -> None:
    """Called by bot.py after engine.start(). Blocking — never returns."""
    inject_core(core)
    _init_api_key()  # FIX SEC-05: Initialize API key authentication

    # Start background threads
    threading.Thread(target=_bg_refresh,       name="DashBgRefresh",     daemon=True).start()
    threading.Thread(target=_prewarm_sentiment, name="SentimentPrewarm",  daemon=True).start()

    # Start pub/sub listeners
    _start_p3_listener()
    _start_p7_listener()

    # Optional live-price cache — Deriv/Binance streams plus IG streaming/poll fallback
    _ws_global = None  # Reference to WebSocket manager for periodic updates
    _ig_stream_global = None
    try:
        from websocket_manager import WebSocketManager
        from websocket_dashboard import set_connected
        from core.asset_profiles import ALL_ASSETS
        from core.assets import registry as _asset_registry
        from services.market_data_router import filter_deriv_stream_assets, filter_ig_primary_assets
        try:
            from services.ig_streaming_manager import ig_streaming_manager as _ig_stream_manager
        except Exception:
            _ig_stream_manager = None

        def _cb(source, symbol, price, volume, side, ts=None):
            _record_live_quote(source, symbol, price, volume, side)
        
        ws = WebSocketManager()
        ws.start()
        _ws_global = ws  # Save for periodic updates
        _ig_stream_global = _ig_stream_manager
        
        # Build dynamic asset list from open positions
        _open_pos = _args.state.get_open_positions() if hasattr(_args, 'state') else []
        _assets_by_category = {}
        for pos in _open_pos:
            _cat = pos.get("category", "forex")
            _asset = pos.get("asset", "")
            if _asset:
                _assets_by_category[_asset] = _cat

        # If no positions exist yet, stream the configured asset universe so the
        # dashboards still show live market movement.
        if not _assets_by_category:
            _assets_by_category = {
                asset: _asset_registry.category(asset)
                for asset in sorted(ALL_ASSETS)
            }

        _ig_primary_assets = filter_ig_primary_assets(_assets_by_category)
        _ig_stream_assets = {}
        _ig_poll_assets = dict(_ig_primary_assets)
        if _assets_by_category:
            _deriv_stream_assets = filter_deriv_stream_assets(_assets_by_category)
            if _deriv_stream_assets:
                ws.subscribe_deriv(_deriv_stream_assets, _cb)
                logger.info(f"[dashboard] Live Deriv stream assets: {sorted(_deriv_stream_assets.keys())}")
            else:
                logger.info("[dashboard] No Deriv stream assets after market-data routing filter")
            if _ig_stream_manager is not None:
                try:
                    _ig_stream_assets = _ig_stream_manager.subscribe_prices(_ig_primary_assets, _cb)
                except Exception as stream_exc:
                    logger.warning(f"[dashboard] IG streaming unavailable; falling back to quote polling: {stream_exc}")
                    _ig_stream_assets = {}
            _ig_poll_assets = {
                asset: category
                for asset, category in _ig_primary_assets.items()
                if asset not in _ig_stream_assets
            }
            if _ig_stream_assets:
                logger.info(f"[dashboard] Live IG stream assets: {sorted(_ig_stream_assets.keys())}")
            if _ig_poll_assets:
                set_connected("ig", True, len(_ig_poll_assets))
                logger.info(f"[dashboard] Live IG poll assets: {sorted(_ig_poll_assets.keys())}")
            elif not _ig_stream_assets:
                set_connected("ig", False, 0)
        
        logger.info("[dashboard] Live streams started")
    except Exception as e:
        logger.warning(f"[dashboard] WebSocket streams failed (non-fatal): {e}")
        _ig_poll_assets = {}
        _ig_stream_assets = {}

    # Background thread to periodically update WebSocket subscriptions for new positions
    def _bg_update_ws_subscriptions():
        """Periodically check for new positions and update WebSocket subscriptions."""
        while True:
            try:
                if _ws_global is None or not hasattr(_args, 'state'):
                    time.sleep(30)
                    continue
                
                # Check open positions every 30 seconds
                time.sleep(30)
                _open = _args.state.get_open_positions() if hasattr(_args.state, 'get_open_positions') else []
                
                # Extract unique assets currently being subscribed
                _asset_map = {}
                for pos in _open:
                    _cat = pos.get("category", "forex")
                    _asset = pos.get("asset", "")
                    if not _asset:
                        continue
                    _asset_map[_asset] = _cat

                if _asset_map:
                    try:
                        nonlocal _ig_poll_assets, _ig_stream_assets

                        _deriv_stream_assets = filter_deriv_stream_assets(_asset_map)
                        _ig_primary_assets = filter_ig_primary_assets(_asset_map)
                        if _deriv_stream_assets:
                            _ws_global.subscribe_deriv(
                                _deriv_stream_assets,
                                _cb if '_cb' in locals() else lambda *a, **k: None,
                            )
                            logger.debug(f"[dashboard] Updated live Deriv subscriptions: {_deriv_stream_assets}")
                        if _ig_stream_global is not None:
                            try:
                                _ig_stream_assets = _ig_stream_global.subscribe_prices(
                                    _ig_primary_assets,
                                    _cb if '_cb' in locals() else lambda *a, **k: None,
                                )
                            except Exception as stream_exc:
                                logger.debug(f"[dashboard] Updated live IG subscriptions failed: {stream_exc}")
                                _ig_stream_assets = {}
                        else:
                            _ig_stream_assets = {}
                        _ig_poll_assets = {
                            asset: category
                            for asset, category in _ig_primary_assets.items()
                            if asset not in _ig_stream_assets
                        }
                        try:
                            from websocket_dashboard import set_connected

                            if _ig_poll_assets:
                                set_connected("ig", True, len(_ig_poll_assets))
                            elif not _ig_stream_assets:
                                set_connected("ig", False, 0)
                        except Exception:
                            pass
                    except Exception as _ue:
                        logger.debug(f"[dashboard] Update live subs failed: {_ue}")
            except Exception as e:
                logger.debug(f"[dashboard] bg_update_ws_subscriptions: {e}")

    def _bg_refresh_ig_quotes():
        """Poll non-streaming IG assets into the shared live-price cache."""

        from websocket_dashboard import mark_feed_activity, set_connected

        last_published_prices: Dict[str, float] = {}

        while True:
            try:
                success_count = 0
                for asset, category in list((_ig_poll_assets or {}).items()):
                    try:
                        price, _ = _fetcher.get_real_time_price(asset, category)
                        if price is None:
                            continue
                        meta = _fetcher.get_last_price_metadata(asset)
                        source = str((meta or {}).get("source") or "IG")
                        price_value = float(price)
                        previous_price = last_published_prices.get(asset)
                        emit_transaction = previous_price is None or abs(previous_price - price_value) > 1e-12
                        _record_live_quote(source, asset, price_value, emit_transaction=emit_transaction)
                        mark_feed_activity("ig", len(_ig_poll_assets))
                        last_published_prices[asset] = price_value
                        success_count += 1
                    except Exception as quote_exc:
                        logger.debug(f"[dashboard] IG quote poll {asset}: {quote_exc}")
                set_connected("ig", True, len(_ig_poll_assets))
                if success_count == 0:
                    logger.debug("[dashboard] IG quote poll completed with no fresh prices")
            except Exception as e:
                set_connected("ig", False, len(_ig_poll_assets))
                logger.debug(f"[dashboard] bg_refresh_ig_quotes: {e}")
            time.sleep(5)
    
    threading.Thread(target=_bg_update_ws_subscriptions, name="WSSubsUpdate", daemon=True).start()
    threading.Thread(target=_bg_refresh_ig_quotes, name="IGQuotePoll", daemon=True).start()

    scheme = "https" if http2 and ssl_cert and ssl_key else "http"
    logger.info(f"[dashboard] {scheme}://{host}:{port}/command-center")
    prefer_hypercorn = http2 or not _DEVELOPMENT_MODE
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
    if prefer_hypercorn:
        logger.info("[dashboard] Falling back to Flask development server")

    app.run(debug=False, host=host, port=port, threaded=True, use_reloader=False)


# Standalone mode (python -m dashboard.web_app_live)
if __name__ == "__main__":
    logger.info("[dashboard] Standalone mode — engine not connected")
    app.run(debug=False, host="0.0.0.0", port=5000, threaded=True)
