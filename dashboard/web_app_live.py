from __future__ import annotations

import argparse
import gzip
import io
import inspect
import json
import os
import sys
import threading
import time
import traceback
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from flask import Flask, Response, jsonify, redirect, render_template, request, send_from_directory, stream_with_context
from flask_cors import CORS
from functools import wraps
import hashlib

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
    from websocket_dashboard import add_transaction, get_feed
    _ws_ok = True
except Exception:
    _ws_ok = False
    def add_transaction(*a, **kw): pass
    def get_feed(**kw): return []

try:
    from telegram_manager import telegram_manager
except Exception:
    telegram_manager = None

# ── Flask app ─────────────────────────────────────────────────────────────────
app = Flask(
    __name__,
    template_folder=os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "templates"),
    static_folder  =os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "static"),
)
CORS(app, resources={r"/api/*": {"origins": ["localhost:5000"]}})  # FIX SEC-05: CORS now restricted

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
_SESSION_TOKENS: Dict[str, float] = {}  # {token: expiry_timestamp}
_SESSION_TOKEN_LOCK = threading.Lock()
_RATE_LIMIT_STORE: Dict[str, List[float]] = {}  # {ip: [req_times...]}
_RATE_LIMIT_LOCK = threading.Lock()
_RATE_LIMIT_REQUESTS_PER_MINUTE = 60  # Max 60 requests per minute per IP
_SESSION_TOKEN_TTL = 3600  # 1 hour default

def _init_api_key():
    """Initialize API key and session TTL from environment."""
    global _API_KEY_HASH, _SESSION_TOKEN_TTL, _DEVELOPMENT_MODE
    try:
        from config.config import DASHBOARD_API_KEY, SESSION_TOKEN_TTL, DEVELOPMENT_MODE
        _DEVELOPMENT_MODE = DEVELOPMENT_MODE
        if _DEVELOPMENT_MODE:
            logger.warning("[dashboard] ⚠️ DEVELOPMENT MODE ENABLED — All API auth bypassed")
        elif DASHBOARD_API_KEY:
            _API_KEY_HASH = hashlib.sha256(DASHBOARD_API_KEY.encode()).hexdigest()
            logger.info("[dashboard] API key authentication enabled")
        else:
            logger.warning("[dashboard] DASHBOARD_API_KEY not set — API in dev mode")
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
        
        # No API key configured: fallback to dev mode
        if not _API_KEY_HASH:
            # Allow all requests without auth in dev mode
            return fn(*args, **kwargs)
        
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


def _render_cached_template(template_name: str, ttl: int = 30) -> str:
    cache_key = f"html_template:{template_name}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    html = render_template(template_name)
    _cache_set(cache_key, html, ttl=ttl)
    return html


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

@app.route("/strategy-lab")
def pg_strategy_lab():
    return _render_cached_template("strategy_lab.html", ttl=15)

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
def _r_backtest(): return redirect("/strategy-lab")
@app.route("/status")
def _r_status():   return redirect("/system-monitor")
@app.route("/websocket-feed")
def _r_ws():       return redirect("/market-intelligence")
@app.route("/strategy-lab-v2")
def _r_lab2():     return redirect("/strategy-lab")

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
        # Development mode: no security needed, just issue a token
        if not _API_KEY_HASH:
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
        
        # Production mode: validate API key from request body
        body = validate_request_json(required_fields=["api_key"])
        provided_key = body.get("api_key", "")
        
        if not provided_key:
            raise BadRequest("api_key cannot be empty")
        
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
        if core:
            payload = {
                "success": True,
                "bot_ready": core.is_ready,
                "engine_running": core.is_running,
                "architecture": "TradingCore",
                "balance": core.get_balance(),
                "assets_cached": len(_sig_store),
            }
        else:
            payload = {
                "success": True,
                "bot_ready": False,
                "engine_running": False,
                "architecture": "TradingCore",
                "balance": _args.balance,
                "assets_cached": len(_sig_store),
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

@app.route("/api/command-center")
@_check_api_auth
@_check_rate_limit
def api_command_center():
    try:
        core = _core()
        perf = {}; daily = {}; positions = []; health = {}
        if core:
            perf      = core.get_performance()
            daily     = core.get_daily_stats()
            positions = core.get_positions()
            health    = core.health_report()

        # Slow external calls cached 5 minutes
        _cc_slow = _cache_get("cc_slow")
        if _cc_slow is None:
            sent_score = 0.0; whale_count = 0; whale_recent = []
            try:
                sa = _get_sent()
                if sa:
                    ms = sa.get_comprehensive_sentiment()
                    sent_score = float(ms.get("score", 0)) if ms else 0.0
            except Exception:
                pass
            try:
                mi = _get_market_intelligence()
                if mi:
                    whale_summary = mi.get_whale_dashboard_summary(
                        min_value_usd=500_000,
                        hours=24,
                        recent_limit=5,
                        alert_limit=5,
                    )
                    whale_recent = whale_summary.get("recent", [])
                    whale_count  = int(whale_summary.get("alert_count_24h", 0) or 0)
            except Exception:
                pass
            _cc_slow = {
                "sentiment_score":  round(sent_score, 3),
                "whale_alerts_24h": whale_count,
                "alert_count_24h":  whale_count,
                "recent":           whale_recent,
            }
            _cache_set("cc_slow", _cc_slow, ttl=600)

        # Fetch live prices for all open positions in one pass,
        # then use the same prices for both latest_signals and positions table.
        _live_prices: Dict[str, float] = {}
        assets = []
        for p in positions[:8]:
            _asset = p.get("asset", "")
            _cat   = p.get("category", "forex")
            if _asset and _asset not in _live_prices:
                assets.append((_asset, _cat))

        if assets:
            from concurrent.futures import ThreadPoolExecutor, wait
            max_workers = min(4, len(assets))
            with ThreadPoolExecutor(max_workers=max_workers) as pool:
                futures = {pool.submit(_fetcher.get_real_time_price, asset, cat): asset
                           for asset, cat in assets}
                done, not_done = wait(tuple(futures.keys()), timeout=8)
                for future in done:
                    asset = futures[future]
                    try:
                        _r2, _ = future.result()
                        if _r2:
                            _live_prices[asset] = float(_r2)
                    except Exception:
                        pass
                if not_done:
                    for future in not_done:
                        future.cancel()
                    logger.debug(f"[dashboard] command-center live price fetch timed out for {len(not_done)} asset(s)")

        # Build signals list (Active Signals panel)
        signals = []
        for p in positions[:6]:
            _cp2 = _live_prices.get(p.get("asset", ""), 0.0)
            signals.append({
                "asset":         p.get("asset", ""),
                "signal":        p.get("direction", p.get("signal", "BUY")),
                "direction":     p.get("direction", p.get("signal", "BUY")),
                "confidence":    float(p.get("confidence", 0) or 0),
                "entry_price":   float(p.get("entry_price", 0) or 0),
                "current_price": _cp2,
                "stop_loss":     float(p.get("stop_loss", 0) or 0),
                "take_profit":   float(p.get("take_profit", 0) or 0),
                "category":      p.get("category", ""),
                "strategy_id":   p.get("strategy_id", ""),
                "pnl":           float(p.get("pnl", 0) or 0),
            })

        # Build enriched positions list (Open Positions table)
        # Each entry is guaranteed to have all numeric fields as Python floats
        # and includes current_price so the table can show live movement colour.
        enriched_positions = []
        for p in positions[:8]:
            _cp3   = _live_prices.get(p.get("asset", ""), 0.0)
            _entry = float(p.get("entry_price", 0) or 0)
            _size  = float(p.get("position_size", 0) or 0)
            _dir   = p.get("direction", p.get("signal", "BUY"))
            _asset = p.get("asset", "")
            _cat   = p.get("category", "forex")
            # Recalculate live P&L using pip-based formula
            _live_pnl = float(p.get("pnl", 0) or 0)
            if _cp3 and _entry and _size:
                try:
                    from risk.position_sizer import PositionSizer as _PS
                    _live_pnl = _PS.pnl(_asset, _cat, _entry, _cp3, _size, _dir)
                except Exception:
                    _live_pnl = (_cp3 - _entry) * _size if _dir == "BUY" else (_entry - _cp3) * _size
            enriched_positions.append({
                "trade_id":      p.get("trade_id", ""),
                "asset":         _asset,
                "category":      _cat,
                "direction":     _dir,
                "confidence":    float(p.get("confidence", 0) or 0),
                "entry_price":   _entry,
                "current_price": _cp3,
                "stop_loss":     float(p.get("stop_loss", 0) or 0),
                "take_profit":   float(p.get("take_profit", 0) or 0),
                "pnl":           round(_live_pnl, 2),
                "position_size": _size,
                "strategy_id":   p.get("strategy_id", ""),
                "open_time":     str(p.get("open_time", ""))[:16],
            })

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
            "positions":        enriched_positions,
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
                d = (p.get("direction") or p.get("signal", "BUY")).upper()
                c = float(p.get("confidence", 0))
                if filt == "buy"  and d != "BUY":  continue
                if filt == "sell" and d != "SELL": continue
                if filt == "high" and c < 0.70:    continue
                # Fetch live price for current_price display
                _cur_price = 0.0
                try:
                    _cp, _ = _get_fetcher().get_real_time_price(
                        p.get("asset", ""), p.get("category", "forex")
                    )
                    if _cp:
                        _cur_price = float(_cp)
                except Exception:
                    pass

                signals.append({
                    "asset":         p.get("asset", ""),
                    "signal":        d, "direction": d,
                    "category":      p.get("category", ""),
                    "confidence":    c,
                    "entry_price":   float(p.get("entry_price", 0)),
                    "current_price": _cur_price,
                    "stop_loss":     float(p.get("stop_loss", 0)),
                    "take_profit":   float(p.get("take_profit", 0)),
                    "position_size": float(p.get("position_size", 0)),
                    "strategy_id":   p.get("strategy_id", ""),
                    "pnl":           float(p.get("pnl", 0)),
                    "market_open":   is_market_open_for_asset(p.get("asset", ""))[0],
                    "generated_at":  str(p.get("open_time", ""))[:16],
                    "metadata":      p.get("metadata", {}),
                    "step_reached": p.get("step_reached", 0),
                })
        else:
            with _sig_lock:
                sigs = list(_sig_store.values())
            active = [s for s in sigs if s.get("signal", "HOLD") not in ("HOLD", "CLOSED")]
            if filt == "buy":    active = [s for s in active if s.get("signal") == "BUY"]
            elif filt == "sell": active = [s for s in active if s.get("signal") == "SELL"]
            elif filt == "high": active = [s for s in active if s.get("confidence", 0) >= 0.70]
            signals = active

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

@app.route("/api/chart/assets")
@_check_api_auth
@_check_rate_limit
def api_chart_assets():
    try:
        return jsonify({"success": True,
                        "assets": [{"symbol": a, "category": c} for a, c in ALL_ASSETS]})
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
        periods  = int(get_chart_timeframe_periods(interval))
        cat      = _cat(asset)

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
        allow_fallback = cat in ("forex", "indices")
        if (df is None or df.empty) and allow_fallback and interval in fallbacks:
            for fb in fallbacks[interval]:
                df = _fetcher.get_ohlcv(asset, cat, interval=fb,
                                        periods=int(get_chart_timeframe_periods(fb)))
                if df is not None and not df.empty:
                    used = fb
                    break

        if df is None or df.empty:
            return jsonify({"success": True, "candles": [],
                            "message": f"No data for {asset}", "interval_used": interval, "bars_requested": periods})

        df.columns = [c.lower() for c in df.columns]
        timestamps = []
        for idx_val in df.index:
            try:
                ts = pd.Timestamp(idx_val)
                if ts.tzinfo is not None:
                    ts = ts.tz_convert("UTC").tz_localize(None)
                timestamps.append(int(ts.timestamp()))
            except Exception:
                timestamps.append(0)

        seen: set = set()
        candles   = []
        for t, (_, row) in zip(timestamps, df.iterrows()):
            if t == 0 or t in seen:
                continue
            seen.add(t)
            candles.append({
                "time":   t,
                "open":   float(row["open"]),
                "high":   float(row["high"]),
                "low":    float(row["low"]),
                "close":  float(row["close"]),
                "volume": float(row.get("volume", 0)),
            })
        candles.sort(key=lambda x: x["time"])
        return jsonify({
            "success": True,
            "candles": candles,
            "interval_used": used,
            "bars_requested": periods,
        })
    except APIError as e:
        return handle_api_error(e, "/api/chart/candles", e.status_code)
    except Exception as e:
        logger.error(f"[candles] {e}")
        return handle_api_error(e, "/api/chart/candles", 500)

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
                price, _ = _fetcher.get_real_time_price(asset, cat)
                if price:
                    yield f"data: {json.dumps({'type': 'tick', 'price': price, 'asset': asset, 'ts': int(time.time())})}\n\n"
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
            if _is_market_weekend(cat):
                return None
            try:
                # Fetch 1-day data to get TODAY's open vs current price (true 24h view)
                df_daily = _fetcher.get_ohlcv(asset, cat, interval="1d", periods=5)
                if df_daily is None or df_daily.empty or "close" not in df_daily.columns:
                    return None
                
                closes = df_daily["close"].astype(float)
                opens  = df_daily["open"].astype(float)
                current_price = float(closes.iloc[-1])
                today_open = float(opens.iloc[-1])  # Today's opening price
                
                # Calculate % change from today's open to current price
                chg = (current_price - today_open) / today_open * 100 if today_open > 0 else 0.0
                
                return {"asset": asset, "category": cat,
                        "change_pct": round(float(chg), 3),
                        "price": round(current_price, 5)}
            except Exception as _he:
                logger.debug(f"[Heatmap] {asset}: {_he}")
                return None

        results = []
        expected_assets = sum(1 for _, cat in ALL_ASSETS if not _is_market_weekend(cat))
        max_workers = min(8, len(ALL_ASSETS))
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
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

        results.sort(key=lambda x: x["change_pct"], reverse=True)
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
            labels = cached.get("labels") or []
            matrix = cached.get("matrix") or []
            invalid = any(
                value is None or (isinstance(value, (int, float)) and not np.isfinite(value))
                for row in matrix for value in row
            )
            if labels and matrix and not invalid:
                return jsonify(cached)
        except Exception:
            pass
    try:
        import pandas as pd
        import numpy as np
        from concurrent.futures import ThreadPoolExecutor, wait
        from config.config import get_trading_timeframe
        assets = [a for a, _ in ALL_ASSETS]  # all 18 tradeable assets

        def _fetch_close(a):
            cat = _cat(a)
            interval = get_trading_timeframe(cat)
            try:
                # Correlation uses historical closes only; fetcher handles local OHLCV caching.
                df = _fetcher.get_ohlcv(a, cat, interval=interval, periods=50)
                if df is not None and not df.empty and "close" in df.columns:
                    return a, df["close"].astype(float)
            except Exception:
                pass
            return a, None

        closes: Dict[str, Any] = {}
        # Use 6 workers — fewer threads avoids throttling while still fetching cached data in parallel
        with ThreadPoolExecutor(max_workers=6) as pool:
            futures = {pool.submit(_fetch_close, a): a for a in assets}
            done, not_done = wait(futures, timeout=30)
            for future in done:
                try:
                    a, series = future.result()
                    if series is not None: closes[a] = series
                except Exception:
                    pass
            if not_done:
                logger.warning(f"[Correlation] timeout fetching {len(not_done)} assets: {[futures[f] for f in not_done]}")
                for fut in not_done:
                    fut.cancel()

        if len(closes) < 2:
            return jsonify({"success": False, "error": "Not enough price data — try again in 30s"})

        # Keep pairwise overlap instead of dropping every row that has any NaN.
        # Different asset classes often have slightly different candle timestamps.
        frame = pd.DataFrame(closes)
        returns = frame.pct_change(fill_method=None)
        returns = returns.dropna(axis=1, thresh=10)
        if returns.shape[1] < 2:
            return jsonify({"success": False, "error": "Not enough aligned data"})

        corr = returns.corr(min_periods=10)
        corr = corr.dropna(axis=0, how="all").dropna(axis=1, how="all")
        if corr.shape[1] < 2:
            return jsonify({"success": False, "error": "Not enough correlated data"})

        for label in corr.columns:
            corr.loc[label, label] = 1.0
        corr = corr.fillna(0.0).round(3)

        payload = {"success": True, "labels": list(corr.columns), "matrix": corr.values.tolist()}
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

    acc_resp = _call_view(api_accuracy)
    sig_resp = _call_view(api_signals_live)

    acc_data = acc_resp.get_json() if hasattr(acc_resp, 'get_json') else json.loads(acc_resp.get_data(as_text=True))
    sig_data = sig_resp.get_json() if hasattr(sig_resp, 'get_json') else json.loads(sig_resp.get_data(as_text=True))

    accuracy = acc_data.get("data") if acc_data.get("success") else {
        "by_horizon": {
            "1H":  {"total": 0, "correct": 0, "accuracy_pct": 0, "avg_move_pct": 0},
            "4H":  {"total": 0, "correct": 0, "accuracy_pct": 0, "avg_move_pct": 0},
            "24H": {"total": 0, "correct": 0, "accuracy_pct": 0, "avg_move_pct": 0},
        },
        "by_asset": {}, "recent": [], "days_back": days,
    }

    signal_list = sig_data.get("signals") if sig_data.get("success") else []
    payload = {
        "success": True,
        "accuracy": accuracy,
        "signals": signal_list,
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
        payload = mi.get_whale_dashboard_summary(
            min_value_usd=500_000,
            hours=24,
            recent_limit=10,
            alert_limit=20,
        )
        _cache_set("whale_summary", payload, ttl=300)
        return jsonify(payload)
    except APIError as e:
        return handle_api_error(e, "/api/whale/summary", e.status_code)
    except Exception as e:
        return handle_api_error(e, "/api/whale/summary", 500)

# ══════════════════════════════════════════════════════════════════════════════
# API — SENTIMENT INTELLIGENCE
# ══════════════════════════════════════════════════════════════════════════════

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

        result: Dict = {
            "success": True, "overall_sentiment": "Neutral", "score": 0.0,
            "fear_greed": {"value": 50, "classification": "Neutral"},
            "vix": {"value": 20, "classification": "Normal"},
            "article_count": 0,
            "sentiment_distribution": {"bullish": 0, "neutral": 0, "bearish": 0},
            "articles": [], "whale_alerts": [],
        }

        ms = sa.get_comprehensive_sentiment()
        if ms:
            result["score"]             = float(ms.get("score", 0))
            result["overall_sentiment"] = ms.get("interpretation", "Neutral")

        fg = sa.fetch_fear_greed_index()
        if fg:
            result["fear_greed"] = {"value": fg.get("value", 50),
                                    "classification": fg.get("classification", "Neutral")}

        vix = sa.fetch_vix()
        if vix:
            result["vix"] = {"value": vix.get("value", 20),
                             "classification": vix.get("classification", "Normal")}

        # News articles via news_integrator shim
        try:
            arts = sa.news_integrator.fetch_all_sources()
            if arts:
                b  = sum(1 for a in arts if float(a.get("sentiment", 0)) > 0.1)
                be = sum(1 for a in arts if float(a.get("sentiment", 0)) < -0.1)
                result["sentiment_distribution"] = {
                    "bullish": b, "neutral": len(arts) - b - be, "bearish": be,
                }
                result["articles"]      = sorted(arts, key=lambda x: x.get("date", ""), reverse=True)[:20]
                result["article_count"] = len(arts)
        except Exception as _ae:
            logger.debug(f"[dashboard] articles error: {_ae}")

        # Distribution fallback — if no articles, derive from per-asset scores
        if result["sentiment_distribution"] == {"bullish": 0, "neutral": 0, "bearish": 0}:
            try:
                from core.assets import registry as _reg
                b = be = n = 0
                for asset, _ in _reg.all_assets():
                    try:
                        score = float(sa.get_comprehensive_sentiment(asset).get("score", 0) or 0)
                        if score > 0.05:   b  += 1
                        elif score < -0.05: be += 1
                        else:               n  += 1
                    except Exception:
                        n += 1
                if b + be + n > 0:
                    result["sentiment_distribution"] = {
                        "bullish": b, "neutral": n, "bearish": be
                    }
            except Exception as _de:
                logger.debug(f"[dashboard] distribution fallback error: {_de}")

        result["whale_alerts"] = sa.fetch_whale_alerts(min_value_usd=1_000_000)[:10]

        _cache_set("sentiment_dashboard", result, ttl=600)
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
        max_workers = min(18, len(watch))

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {pool.submit(_sent_one, a): a for a in watch}
            try:
                for future in as_completed(futures, timeout=60):
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
                logger.warning(f"[Sentiment] by-asset timeout — returning {len(results)}/{len(watch)} assets")

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
        for p in positions:
            cat = p.get("category", "unknown")
            by_cat.setdefault(cat, {"count": 0, "pnl": 0.0, "exposure": 0.0})
            by_cat[cat]["count"]    += 1
            by_cat[cat]["pnl"]      += float(p.get("pnl") or 0)
            by_cat[cat]["exposure"] += float(p.get("position_size", 0)) * float(p.get("entry_price", 0))

        closed  = core.get_closed_trades(limit=100)
        wins    = [t for t in closed if float(t.get("pnl") or 0) > 0]
        losses  = [t for t in closed if float(t.get("pnl") or 0) <= 0 and float(t.get("pnl") or 0) != 0]
        avg_win = sum(float(t.get("pnl") or 0) for t in wins)   / len(wins)   if wins   else 0.0
        avg_los = sum(float(t.get("pnl") or 0) for t in losses) / len(losses) if losses else 0.0
        pf      = abs(avg_win / avg_los) if avg_los else 0.0

        payload = {
            "success":        True,
            "balance":        balance,
            "open_positions": len(positions),
            "total_exposure": risk_stats.get("total_exposure", 0),
            "exposure_pct":   risk_stats.get("exposure_pct", 0),
            "drawdown_pct":   risk_stats.get("drawdown_pct", 0),
            "peak_balance":   risk_stats.get("peak_balance", balance),
            "by_category":    by_cat,
            "win_rate":       _wr(perf.get("win_rate", 0)),
            "profit_factor":  round(pf, 2),
            "avg_win":        round(avg_win, 2),
            "avg_loss":       round(avg_los, 2),
            "total_trades":   perf.get("total_trades", 0),
            "total_pnl":      perf.get("total_pnl", 0),
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
        for strat, s in stats.items():
            total   = s.get("wins", 0) + s.get("losses", 0)
            pnl     = s.get("pnl", 0)
            wr      = s.get("wins", 0) / total * 100 if total else 0
            durs    = [int(t.get("duration_minutes", 0)) for t in trades
                       if t.get("strategy_id") == strat and t.get("duration_minutes")]
            avg_dur = sum(durs) / len(durs) if durs else 0
            enriched[strat] = {**s, "total": total, "win_rate": round(wr, 1),
                               "avg_duration_min": round(avg_dur),
                               "avg_trade_pnl": round(pnl / total, 4) if total else 0}
        timeline = [{"asset": t.get("asset", ""), "direction": t.get("direction", ""),
                     "pnl": float(t.get("pnl") or 0), "strategy": t.get("strategy_id", ""),
                     "exit_time": str(t.get("exit_time", ""))[:16],
                     "conf": float(t.get("confidence") or 0)} for t in trades[:50]]
        return jsonify({"success": True, "strategies": enriched, "timeline": timeline})
    except APIError as e:
        return handle_api_error(e, "/api/strategy/performance", e.status_code)
    except Exception as e:
        return handle_api_error(e, "/api/strategy/performance", 500)

@app.route("/api/backtest/strategies")
@_check_api_auth
@_check_rate_limit
def api_backtest_strategies():
    try:
        from strategy_lab import StrategyBuilder
        presets  = list(StrategyBuilder.all_configs().keys())
        existing = ["voting", "rsi", "macd", "bollinger"]
        return jsonify({"success": True, "presets": presets, "existing": existing})
    except APIError as e:
        return handle_api_error(e, "/api/backtest/strategies", e.status_code)
    except Exception as e:
        return handle_api_error(e, "/api/backtest/strategies", 500)

@app.route("/api/backtest/run")
@_check_api_auth
@_check_rate_limit
def api_backtest_run():
    try:
        asset    = request.args.get("asset", "").strip()
        strategy = request.args.get("strategy", "").strip()
        periods  = int(request.args.get("periods", 500))

        if not asset or not strategy:
            raise BadRequest("asset and strategy are required")

        canonical = registry.canonical(asset)
        category  = registry.category(canonical)
        if category == "unknown":
            raise BadRequest(f"Unknown asset: {asset}")

        from strategy_lab import StrategyBuilder, run_backtest

        configs = StrategyBuilder.all_configs()
        if strategy not in configs:
            raise BadRequest(f"Unknown strategy: {strategy}")

        result = run_backtest(configs[strategy], canonical, category, periods=periods)
        return jsonify({
            "success": True,
            "strategy": strategy,
            "asset": canonical,
            "metrics": result.to_dict(),
            "trades": result.trades,
            "equity_curve": result.equity_curve,
        })
    except APIError as e:
        return handle_api_error(e, "/api/backtest/run", e.status_code)
    except Exception as e:
        return handle_api_error(e, "/api/backtest/run", 500)

@app.route("/api/backtest/compare")
@_check_api_auth
@_check_rate_limit
def api_backtest_compare():
    try:
        asset   = request.args.get("asset", "").strip()
        periods = int(request.args.get("periods", 500))

        if not asset:
            raise BadRequest("asset is required")

        canonical = registry.canonical(asset)
        category  = registry.category(canonical)
        if category == "unknown":
            raise BadRequest(f"Unknown asset: {asset}")

        from strategy_lab import StrategyBuilder, run_backtest
        from strategy_lab.performance_analyzer import PerformanceAnalyzer

        configs = StrategyBuilder.all_configs()
        results = []
        labels = []
        for name, config in configs.items():
            try:
                result = run_backtest(config, canonical, category, periods=periods)
                results.append(result)
                labels.append(name)
            except Exception as e:
                logger.warning(f"[StrategyLab] compare skip {name}: {e}")

        if not results:
            raise BadRequest("No strategies could be backtested")

        analyzer = PerformanceAnalyzer()
        ranked = analyzer.compare(results, labels=labels)
        output = [
            {
                "name": row["label"],
                "strategy": row["label"],
                "type": "lab",
                "sharpe": row["sharpe"],
                "win_rate": row["win_rate"],
                "total_pnl": row["total_pnl"],
                "max_dd": row.get("max_drawdown", 0),
                "trades": row["trades"],
                "profit_factor": row.get("profit_factor", 0),
            }
            for row in ranked
        ]
        best = output[0]["name"] if output else ""
        return jsonify({"success": True, "results": output, "best": best})
    except APIError as e:
        return handle_api_error(e, "/api/backtest/compare", e.status_code)
    except Exception as e:
        return handle_api_error(e, "/api/backtest/compare", 500)

@app.route("/api/backtest/optimize")
@_check_api_auth
@_check_rate_limit
def api_backtest_optimize():
    try:
        asset    = request.args.get("asset", "").strip()
        strategy = request.args.get("strategy", "").strip()
        periods  = int(request.args.get("periods", 500))

        if not asset or not strategy:
            raise BadRequest("asset and strategy are required")

        canonical = registry.canonical(asset)
        category  = registry.category(canonical)
        if category == "unknown":
            raise BadRequest(f"Unknown asset: {asset}")

        from strategy_lab import StrategyBuilder, optimize_strategy

        configs = StrategyBuilder.all_configs()
        if strategy not in configs:
            raise BadRequest(f"Unknown strategy: {strategy}")

        results = optimize_strategy(
            base_config=configs[strategy],
            param_grid={
                "rsi_period": [10, 14, 21],
                "stop_mult":  [1.0, 1.5, 2.0],
                "tp_mult":    [2.0, 3.0, 4.0],
            },
            asset=canonical,
            category=category,
            periods=periods,
        )

        return jsonify({
            "success": True,
            "strategy": strategy,
            "asset": canonical,
            "total": len(results),
            "top5": results[:5],
        })
    except APIError as e:
        return handle_api_error(e, "/api/backtest/optimize", e.status_code)
    except Exception as e:
        return handle_api_error(e, "/api/backtest/optimize", 500)

@app.route("/api/backtest/multi-asset")
@_check_api_auth
@_check_rate_limit
def api_backtest_multi_asset():
    try:
        strategy = request.args.get("strategy", "").strip()
        periods  = int(request.args.get("periods", 500))

        if not strategy:
            raise BadRequest("strategy is required")

        from strategy_lab import StrategyBuilder, run_backtest

        configs = StrategyBuilder.all_configs()
        if strategy not in configs:
            raise BadRequest(f"Unknown strategy: {strategy}")

        chosen_config = configs[strategy]
        test_assets = [
            ("BTC-USD",  "crypto"),
            ("ETH-USD",  "crypto"),
            ("SOL-USD",  "crypto"),
            ("EUR/USD",  "forex"),
            ("GBP/USD",  "forex"),
            ("USD/JPY",  "forex"),
            ("XAU/USD",  "commodities"),
            ("US30",     "indices"),
        ]

        results = []
        for asset, category in test_assets:
            try:
                result = run_backtest(chosen_config, asset, category, periods=periods)
                results.append({
                    "asset": asset,
                    "category": category,
                    "sharpe": result.sharpe_ratio,
                    "win_rate": result.win_rate,
                    "total_pnl": result.total_pnl,
                    "max_dd": result.max_drawdown,
                    "trades": result.total_trades,
                })
            except Exception as e:
                logger.warning(f"[StrategyLab] multi-asset skip {asset}: {e}")

        if not results:
            raise BadRequest("No assets could be backtested")

        best = max(results, key=lambda r: r.get("sharpe", 0))["asset"]
        return jsonify({"success": True, "results": results, "best": best})
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
        from services.db_pool import get_db
        trades = get_db().get_recent_trades(limit=limit)
        from datetime import timedelta as _td
        from config.config import TZ_NAME, TZ_OFFSET_HOURS
        def _enrich(trade):
            d = dict(trade)
            # Convert stored UTC timestamps into the configured dashboard timezone.
            display_offset = _td(hours=TZ_OFFSET_HOURS)
            try:
                entry_raw = d.get("entry_time")
                exit_raw = d.get("exit_time")
                if entry_raw and exit_raw:
                    et = datetime.fromisoformat(str(entry_raw).replace("Z", "+00:00")).replace(tzinfo=None)
                    xt = datetime.fromisoformat(str(exit_raw).replace("Z", "+00:00")).replace(tzinfo=None)
                    et_local = et + display_offset
                    xt_local = xt + display_offset
                    d["entry_time"] = et_local.isoformat()
                    d["exit_time"] = xt_local.isoformat()
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
        return jsonify({
            "success": True,
            "closed":  len(closed),
            "skipped": len(skipped),
            "mode":    mode,
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
        try:
            from strategy_lab import StrategyBuilder
            StrategyBuilder.all_configs()
            phase_health["phase5_strategy_lab"] = True
        except Exception:
            phase_health["phase5_strategy_lab"] = False
        try:
            from ml.meta_model import predictor as _mp
            phase_health["phase6_meta_ai"] = _mp is not None
        except Exception:
            phase_health["phase6_meta_ai"] = False
        try:
            from services.intelligence_alerts import alert_service as _as
            phase_health["phase7_intel_alerts"] = getattr(_as, "_running", False)
        except Exception:
            phase_health["phase7_intel_alerts"] = False

        payload = {
            "success":          True,
            "ram_pct":          round(ram_pct, 1),
            "cpu_pct":          round(cpu_pct, 1),
            "disk_pct":         round(disk_pct, 1),
            "process_mem_mb":   proc_mb,
            "processes":        processes,
            "phase_health":     phase_health,
            "open_positions":   health.get("open_positions", 0),
            "active_cooldowns": health.get("active_cooldowns", 0),
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
        ttl = 30
    elif page == "sentiment_intelligence":
        payload = {
            "success": True,
            "sentiment": _response_to_dict(_call_view(api_sentiment_dashboard)),
            "by_asset": _response_to_dict(_call_view(api_sentiment_by_asset)),
            "events": _response_to_dict(_call_view(api_market_events)),
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
    elif page == "strategy_lab":
        payload = {
            "success": True,
            "status": _response_to_dict(_call_view(api_status)),
            "strategies": _response_to_dict(_call_view(api_backtest_strategies)),
            "performance": _response_to_dict(_call_view(api_strategy_performance)),
        }
        ttl = 20
    elif page == "command_center":
        payload = {
            "success": True,
            "command_center": _response_to_dict(_call_view(api_command_center)),
            "whale": _response_to_dict(_call_view(api_whale_summary)),
        }
        ttl = 15
    elif page == "market_intelligence":
        payload = {
            "success": True,
            "assets": _response_to_dict(_call_view(api_chart_assets)),
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

    # Optional WebSocket manager — Deriv-first live prices with Binance support
    _ws_global = None  # Reference to WebSocket manager for periodic updates
    try:
        from websocket_manager import WebSocketManager
        from websocket_dashboard import set_live_price
        from core.asset_profiles import ALL_ASSETS
        from core.assets import registry as _asset_registry

        def _cb(source, symbol, price, volume, side, ts=None):
            add_transaction(source, symbol, price, volume, side)
            # Store price for live P&L updates (real-time, no API calls)
            set_live_price(symbol, price, source)
        
        ws = WebSocketManager()
        ws.start()
        _ws_global = ws  # Save for periodic updates
        
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

        if _assets_by_category:
            ws.subscribe_deriv(_assets_by_category, _cb)
            logger.info(f"[dashboard] Live stream assets: {sorted(_assets_by_category.keys())}")
        
        logger.info("[dashboard] Live streams started")
    except Exception as e:
        logger.warning(f"[dashboard] WebSocket streams failed (non-fatal): {e}")

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
                        _ws_global.subscribe_deriv(_asset_map, _cb if '_cb' in locals() else lambda *a, **k: None)
                        logger.debug(f"[dashboard] Updated live subscriptions: {_asset_map}")
                    except Exception as _ue:
                        logger.debug(f"[dashboard] Update live subs failed: {_ue}")
            except Exception as e:
                logger.debug(f"[dashboard] bg_update_ws_subscriptions: {e}")
    
    threading.Thread(target=_bg_update_ws_subscriptions, name="WSSubsUpdate", daemon=True).start()

    scheme = "https" if http2 and ssl_cert and ssl_key else "http"
    logger.info(f"[dashboard] {scheme}://{host}:{port}/command-center")
    if http2:
        try:
            import asyncio
            from hypercorn.config import Config
            from hypercorn.asyncio import serve
            try:
                from hypercorn.wsgi import Wsgi
            except ImportError:
                from hypercorn.app_wrappers import WSGIWrapper as Wsgi

            config = Config()
            config.bind = [f"{host}:{port}"]
            config.alpn_protocols = ["h2", "http/1.1"]
            config.worker_class = "asyncio"
            config.loglevel = "info"
            config.keep_alive_timeout = 5

            if ssl_cert and ssl_key:
                config.certfile = ssl_cert
                config.keyfile = ssl_key
                logger.info(f"[dashboard] Starting Hypercorn with TLS cert={ssl_cert}")
            else:
                logger.info("[dashboard] Starting Hypercorn HTTP/2 server (cleartext, browser support may be limited)")

            asyncio.run(serve(Wsgi(app, max_body_size=16 * 1024 * 1024), config))
            return
        except Exception as e:
            logger.warning(f"[dashboard] HTTP/2 server unavailable or failed: {e}")
            if ssl_cert and ssl_key:
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
            logger.info("[dashboard] Falling back to Flask development server")

    app.run(debug=False, host=host, port=port, threaded=True, use_reloader=False)


# Standalone mode (python -m dashboard.web_app_live)
if __name__ == "__main__":
    logger.info("[dashboard] Standalone mode — engine not connected")
    app.run(debug=True, host="0.0.0.0", port=5000, threaded=True)
