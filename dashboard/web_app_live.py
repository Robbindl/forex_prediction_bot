# type: ignore
"""
dashboard/web_app_live.py
Professional Trading Intelligence Platform — clean rewrite.

Imports ONLY from files confirmed to exist in this project.
Zero references to non-existent modules.

bot.py contract:
  from dashboard.web_app_live import start_dashboard
  start_dashboard(engine, host=..., port=...)   # blocking
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import threading
import time
import traceback
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from flask import Flask, Response, jsonify, redirect, render_template, request, stream_with_context
from flask_cors import CORS

# ── project path ──────────────────────────────────────────────────────────────
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.assets import registry        # AssetRegistry singleton — all_assets(), category()
from data.fetcher import DataFetcher
from utils.logger import logger

# ── optional services (each wrapped so failure never crashes the dashboard) ───
try:
    from prediction_tracker import prediction_tracker as _pred_tracker
    _pred_tracker.start()
except Exception as _e:
    _pred_tracker = None
    logger.warning(f"[dashboard] PredictionTracker: {_e}")

try:
    from redis_broker import broker as _redis_broker
except Exception as _e:
    _redis_broker = None

try:
    from websocket_dashboard import add_transaction, connection_status, get_feed
    _ws_ok = True
except Exception:
    _ws_ok = False
    connection_status: dict = {}
    def add_transaction(*a, **kw): pass
    def get_feed(**kw): return []

try:
    from telegram_manager import telegram_manager
except Exception:
    telegram_manager = None

# ── Flask ─────────────────────────────────────────────────────────────────────
app = Flask(
    __name__,
    template_folder=os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "templates"
    ),
    static_folder=os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "static"
    ),
)
CORS(app)

_parser = argparse.ArgumentParser(add_help=False)
_parser.add_argument("--balance", type=float, default=10000.0)
_args, _ = _parser.parse_known_args()

# ── singletons ────────────────────────────────────────────────────────────────
_fetcher = DataFetcher()
_CORE: Any = None


def inject_core(core) -> None:
    global _CORE
    _CORE = core
    logger.info("[dashboard] TradingCore injected")


def _core() -> Optional[Any]:
    return _CORE


# ── lazy whale / sentiment ────────────────────────────────────────────────────
_whale_svc = None
_whale_lock = threading.Lock()


def _get_whale() -> Optional[Any]:
    global _whale_svc
    if _whale_svc is not None:
        return _whale_svc
    with _whale_lock:
        if _whale_svc is not None:
            return _whale_svc
        try:
            from whale_alert_manager import WhaleAlertManager
            _whale_svc = WhaleAlertManager()
            _whale_svc.start_monitoring()
        except Exception as _e:
            logger.warning(f"[dashboard] WhaleAlertManager: {_e}")
    return _whale_svc


_sent_svc = None
_sent_lock = threading.Lock()


def _get_sent() -> Optional[Any]:
    global _sent_svc
    if _sent_svc is not None:
        return _sent_svc
    with _sent_lock:
        if _sent_svc is not None:
            return _sent_svc
        try:
            from sentiment_analyzer import SentimentAnalyzer
            _sent_svc = SentimentAnalyzer()
        except Exception as _e:
            logger.warning(f"[dashboard] SentimentAnalyzer: {_e}")
    return _sent_svc


# ── asset registry ────────────────────────────────────────────────────────────
ALL_ASSETS: List[Tuple[str, str]] = registry.all_assets()   # [(id, category)]
_CAT: Dict[str, str] = {a: c for a, c in ALL_ASSETS}


def _cat(asset: str) -> str:
    return _CAT.get(asset, "crypto")


# ── OHLCV helper ──────────────────────────────────────────────────────────────
def _ohlcv(asset: str, interval: str = "1d", periods: int = 30):
    try:
        return _fetcher.get_ohlcv(asset, _cat(asset), interval=interval, periods=periods)
    except Exception as _e:
        logger.debug(f"[dashboard] ohlcv {asset}: {_e}")
        return None


# ── background signal cache ───────────────────────────────────────────────────
_sig_store: Dict[str, Dict] = {}
_sig_lock   = threading.Lock()
_last_ref:  Dict[str, float] = {}
_TTL = {"crypto": 30, "forex": 60, "commodities": 60, "indices": 120, "stocks": 120}


def _store(asset: str, sig: Dict) -> None:
    with _sig_lock:
        _sig_store[asset] = sig


def _due(asset: str) -> bool:
    return (time.time() - _last_ref.get(asset, 0)) >= _TTL.get(_cat(asset), 60)


def _fallback_signal(asset: str) -> Optional[Dict]:
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
    if rsi < 35:       d = "BUY"
    elif rsi > 65:     d = "SELL"
    else:              return None
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
        "market_open": True,
        "timestamp":   datetime.now().isoformat(),
        "generated_at": datetime.now().strftime("%H:%M:%S"),
    }


def _bg_refresh() -> None:
    from concurrent.futures import ThreadPoolExecutor, as_completed
    try:
        _get_sent()
        _get_whale()
    except Exception:
        pass
    while True:
        try:
            core = _core()
            due  = [(a, c) for a, c in ALL_ASSETS if _due(a)]
            if not due:
                time.sleep(15)
                continue

            def _refresh_one(asset_cat):
                asset, _ = asset_cat
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
                except Exception as _e:
                    logger.debug(f"[dashboard] refresh {asset}: {_e}")
                finally:
                    _last_ref[asset] = time.time()

            with ThreadPoolExecutor(max_workers=6) as pool:
                list(pool.map(_refresh_one, due))

        except Exception as _e:
            logger.error(f"[dashboard] bg_refresh: {_e}")
        time.sleep(15)


# ══════════════════════════════════════════════════════════════════════════════
# RESPONSE CACHE
# Stores the last computed response for slow API routes so repeated page loads
# hit memory instead of making external API calls every time.
#
# TTLs chosen per data volatility:
#   sentiment_dashboard  — 300s (5 min): fear/greed and news change slowly
#   sentiment_by_asset   — 300s (5 min): 18 concurrent sentiment calls, expensive
#   market_heatmap       — 60s  (1 min): daily price change, updates once per day
#   correlation_matrix   — 600s (10 min): 30-day correlation barely moves
#   whale_summary        — 120s (2 min): whale alerts are collected in background
#   command_center_slow  — 300s (5 min): sentiment + whale portion only
#
# Two-tier strategy:
#   Primary  — Redis via _redis_broker.set/get (already imported above).
#              Survives dashboard restarts. Shared across processes.
#              A Redis GET on localhost is ~0.2ms — negligible vs the 3-8s
#              external API calls being replaced.
#   Fallback — _RCache (in-process memory). Used when Redis is unavailable.
#              Data lost on restart but still prevents repeated API calls
#              within a single run session.
# ══════════════════════════════════════════════════════════════════════════════

class _RCache:
    """In-process TTL cache — fallback when Redis is unavailable."""

    def __init__(self):
        self._store: Dict[str, Tuple[Any, float]] = {}
        self._lock  = threading.Lock()

    def get(self, key: str) -> Optional[Any]:
        with self._lock:
            entry = self._store.get(key)
            if entry is None:
                return None
            value, expires_at = entry
            if time.time() > expires_at:
                del self._store[key]
                return None
            return value

    def set(self, key: str, value: Any, ttl: int) -> None:
        with self._lock:
            self._store[key] = (value, time.time() + ttl)

    def invalidate(self, key: str) -> None:
        with self._lock:
            self._store.pop(key, None)


_mem_cache = _RCache()   # always available — in-process fallback


def _cache_get(key: str) -> Optional[Any]:
    """Read from Redis if available, otherwise read from in-process cache."""
    if _redis_broker:
        try:
            val = _redis_broker.get(f"dash:{key}")
            if val is not None:
                return val
        except Exception:
            pass
    return _mem_cache.get(key)


def _cache_set(key: str, value: Any, ttl: int) -> None:
    """Write to Redis if available. Always write to in-process cache as backup."""
    if _redis_broker:
        try:
            _redis_broker.set(f"dash:{key}", value, ttl_seconds=ttl)
        except Exception:
            pass
    _mem_cache.set(key, value, ttl)


# ── JSON encoder ──────────────────────────────────────────────────────────────
class _Enc(json.JSONEncoder):
    def default(self, obj):
        if hasattr(obj, "isoformat"):
            return obj.isoformat()
        try:
            import numpy as np
            if isinstance(obj, np.integer):  return int(obj)
            if isinstance(obj, np.floating): return float(obj)
            if isinstance(obj, np.ndarray):  return obj.tolist()
        except ImportError:
            pass
        return super().default(obj)


app.json_encoder = _Enc


# ── error handlers ────────────────────────────────────────────────────────────
@app.errorhandler(404)
def _e404(e):
    return jsonify({"success": False, "error": f"Not found: {request.path}"}), 404


@app.errorhandler(Exception)
def _e500(e):
    logger.error(f"[dashboard] unhandled: {e}\n{traceback.format_exc()}")
    code = getattr(e, "code", 500)
    if not isinstance(code, int):
        code = 500
    return jsonify({"success": False, "error": str(e)}), code


# ══════════════════════════════════════════════════════════════════════════════
# PAGE ROUTES
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/")
def index():
    return redirect("/command-center")


@app.route("/command-center")
def pg_command_center():
    return render_template("command_center.html")


@app.route("/market-intelligence")
def pg_market_intelligence():
    return render_template("market_intelligence.html")


@app.route("/ai-predictions")
def pg_ai_predictions():
    return render_template("ai_predictions.html")


@app.route("/whale-intelligence")
def pg_whale_intelligence():
    return render_template("whale_intelligence.html")


@app.route("/sentiment-intelligence")
def pg_sentiment_intelligence():
    return render_template("sentiment_intelligence.html")


@app.route("/risk-dashboard")
def pg_risk_dashboard():
    return render_template("risk_dashboard.html")


@app.route("/strategy-lab")
def pg_strategy_lab():
    return render_template("strategy_lab.html")


@app.route("/system-monitor")
def pg_system_monitor():
    return render_template("system_monitor.html")


# ── legacy redirects ──────────────────────────────────────────────────────────
@app.route("/chart")
def _r_chart():        return redirect("/market-intelligence")

@app.route("/accuracy")
def _r_accuracy():     return redirect("/ai-predictions")

@app.route("/sentiment")
def _r_sentiment():    return redirect("/sentiment-intelligence")

@app.route("/backtest")
def _r_backtest():     return redirect("/strategy-lab")

@app.route("/status")
def _r_status():       return redirect("/system-monitor")

@app.route("/websocket-feed")
def _r_ws():           return redirect("/market-intelligence")


# ══════════════════════════════════════════════════════════════════════════════
# API — STATUS (lightweight, used by all templates for nav dot)
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/api/status")
def api_status():
    core = _core()
    return jsonify({
        "bot_ready":     core.is_ready if core else False,
        "architecture":  "TradingCore" if core else "standalone",
        "assets_cached": len(_sig_store),
    })


@app.route("/api/system-status")
def api_system_status():
    core = _core()
    if core:
        perf  = core.get_performance()
        daily = core.get_daily_stats()
        return jsonify({
            "success": True,
            "balance":          round(core.get_balance(), 2),
            "pnl":              round(daily.get("daily_pnl", 0), 2),
            "total_pnl":        round(perf.get("total_pnl", 0), 2),
            "open_positions":   perf.get("open_positions", 0),
            "closed_positions": perf.get("total_trades", 0),
            "daily_trades":     daily.get("daily_trades", 0),
            "win_rate":         perf.get("win_rate", 0),
            "engine_ready":     core.is_ready,
            "timestamp":        datetime.now().isoformat(),
        })
    return jsonify({
        "success": True, "balance": _args.balance, "pnl": 0, "total_pnl": 0,
        "open_positions": 0, "closed_positions": 0, "daily_trades": 0,
        "win_rate": 0, "engine_ready": False,
        "timestamp": datetime.now().isoformat(),
    })


# ══════════════════════════════════════════════════════════════════════════════
# API — COMMAND CENTER
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/api/command-center")
def api_command_center():
    try:
        core = _core()
        perf = {}; daily = {}; positions = []; health = {}
        if core:
            perf      = core.get_performance()
            daily     = core.get_daily_stats()
            positions = core.get_positions()
            health    = core.health_report()

        # Sentiment and whale data are slow external calls — cache for 5 minutes.
        # Balance, positions, and signals are fast in-memory reads — always live.
        _cc_slow = _cache_get("cc_slow")
        if _cc_slow is None:
            sent_score   = 0.0
            whale_count  = 0
            whale_recent = []
            try:
                sa = _get_sent()
                if sa:
                    ms = sa.get_comprehensive_sentiment()
                    sent_score = float(ms.get("score", 0)) if ms else 0.0
            except Exception:
                pass
            try:
                wm = _get_whale()
                if wm:
                    whale_recent = wm.get_top_alerts(limit=5, days=1)
                    whale_count  = len(whale_recent)
            except Exception:
                pass
            _cc_slow = {
                "sentiment_score":  round(sent_score, 3),
                "whale_alerts_24h": whale_count,
                "alert_count_24h":  whale_count,
                "recent":           whale_recent,
            }
            _cache_set("cc_slow", _cc_slow, ttl=300)

        with _sig_lock:
            signals = [s for s in list(_sig_store.values())
                       if s.get("signal", "HOLD") not in ("HOLD", "CLOSED")][:6]

        return jsonify({
            "success":          True,
            "balance":          perf.get("balance", _args.balance),
            "total_pnl":        perf.get("total_pnl", 0),
            "daily_pnl":        daily.get("daily_pnl", 0),
            "daily_trades":     daily.get("daily_trades", 0),
            "win_rate":         perf.get("win_rate", 0),
            "open_positions":   len(positions),
            "total_trades":     perf.get("total_trades", 0),
            "engine_running":   health.get("is_running", core.is_running if core else False),
            "engine_ready":     health.get("engine_ready", core.is_ready if core else False),
            "sentiment_score":  _cc_slow["sentiment_score"],
            "whale_alerts_24h": _cc_slow["whale_alerts_24h"],
            "alert_count_24h":  _cc_slow["alert_count_24h"],
            "recent":           _cc_slow["recent"],
            "latest_signals":   signals,
            "positions":        positions[:8],
            "timestamp":        datetime.now().isoformat(),
        })
    except Exception as _e:
        return jsonify({"success": False, "error": str(_e)}), 500


# ══════════════════════════════════════════════════════════════════════════════
# API — SIGNALS (risk_dashboard reads /api/signals/live)
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/api/signals/live")
def api_signals_live():
    try:
        core = _core()
        filt = request.args.get("filter", "all")

        if core:
            positions = core.get_positions()
            signals   = []
            for p in positions:
                d = (p.get("direction") or p.get("signal", "BUY")).upper()
                if filt == "buy"  and d != "BUY":  continue
                if filt == "sell" and d != "SELL": continue
                c = float(p.get("confidence", 0))
                if filt == "high" and c < 0.70:    continue
                signals.append({
                    "asset":         p.get("asset", ""),
                    "signal":        d,
                    "direction":     d,
                    "category":      p.get("category", ""),
                    "confidence":    c,
                    "entry_price":   float(p.get("entry_price", 0)),
                    "stop_loss":     float(p.get("stop_loss", 0)),
                    "take_profit":   float(p.get("take_profit", 0)),
                    "position_size": float(p.get("position_size", 0)),
                    "strategy_id":   p.get("strategy_id", ""),
                    "pnl":           float(p.get("pnl", 0)),
                    "market_open":   True,
                    "generated_at":  str(p.get("open_time", ""))[:16],
                    "metadata":      p.get("metadata", {}),
                    "layer_reached": p.get("layer_reached", 0),
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
            "total_signals": len(signals),
            "buy_signals": buys, "sell_signals": sells,
            "avg_confidence": round(avg_conf * 100, 1),
        })
    except Exception as _e:
        return jsonify({"success": False, "error": str(_e)}), 500


# ══════════════════════════════════════════════════════════════════════════════
# API — MARKET INTELLIGENCE
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/api/chart/assets")
def api_chart_assets():
    return jsonify({"success": True,
                    "assets": [{"symbol": a, "category": c} for a, c in ALL_ASSETS]})


@app.route("/api/chart/candles")
def api_chart_candles():
    try:
        import pandas as pd
        asset    = request.args.get("asset", "EUR/USD")
        interval = request.args.get("interval", "1h")
        periods_map = {"1m": 60, "5m": 200, "15m": 200, "1h": 168, "4h": 200, "1d": 365}
        periods  = periods_map.get(interval, 100)
        df = _fetcher.get_ohlcv(asset, _cat(asset), interval=interval, periods=periods)
        if df is None or df.empty:
            return jsonify({"success": True, "candles": [],
                            "message": f"No data yet for {asset}"})

        df.columns = [c.lower() for c in df.columns]

        # Extract timestamps from DatetimeIndex
        timestamps = []
        for idx_val in df.index:
            try:
                ts = pd.Timestamp(idx_val)
                if ts.tzinfo is not None:
                    ts = ts.tz_convert("UTC").tz_localize(None)
                timestamps.append(int(ts.timestamp()))
            except Exception:
                timestamps.append(0)

        candles = []
        for i, (t, (_, row)) in enumerate(zip(timestamps, df.iterrows())):
            if t == 0:
                continue
            candles.append({
                "time":   t,
                "open":   float(row["open"]),
                "high":   float(row["high"]),
                "low":    float(row["low"]),
                "close":  float(row["close"]),
                "volume": float(row["volume"]),
            })

        # Deduplicate and sort
        seen: set = set()
        clean = []
        for c in sorted(candles, key=lambda x: x["time"]):
            if c["time"] not in seen:
                seen.add(c["time"])
                clean.append(c)

        return jsonify({"success": True, "candles": clean})
    except Exception as _e:
        logger.error(f"[candles] {asset} {interval}: {_e}")
        return jsonify({"success": False, "error": str(_e)}), 500


@app.route("/api/chart/stream")
def api_chart_stream():
    asset = request.args.get("asset", "EUR/USD")
    cat   = _cat(asset)

    def _gen():
        while True:
            try:
                price, _ = _fetcher.get_real_time_price(asset, cat)
                if price:
                    yield f"data: {json.dumps({'type': 'tick', 'asset': asset, 'price': price})}\n\n"
                core = _core()
                if core:
                    open_pos = core.get_positions()
                    history  = core.get_closed_trades(limit=20)
                    balance  = core.get_balance()
                    yield f"data: {json.dumps({'type': 'positions', 'open': open_pos, 'history': history, 'balance': balance}, default=str)}\n\n"
            except GeneratorExit:
                return
            except Exception:
                pass
            time.sleep(3)

    return Response(stream_with_context(_gen()), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.route("/api/market/heatmap")
def api_market_heatmap():
    # Cache for 60 seconds — daily % change only updates once per trading day
    cached = _cache_get("heatmap")
    if cached is not None:
        return jsonify(cached)
    try:
        from concurrent.futures import ThreadPoolExecutor, as_completed
        # All 18 assets — was missing GBP/JPY, AUD/USD, USD/CAD from the original
        sample = [
            ("BTC-USD", "crypto"),   ("ETH-USD", "crypto"),   ("SOL-USD", "crypto"),
            ("XRP-USD", "crypto"),   ("BNB-USD", "crypto"),
            ("EUR/USD", "forex"),    ("GBP/USD", "forex"),    ("GBP/JPY", "forex"),
            ("AUD/USD", "forex"),    ("USD/JPY", "forex"),    ("USD/CAD", "forex"),
            ("GC=F", "commodities"), ("SI=F", "commodities"), ("CL=F", "commodities"),
            ("^DJI", "indices"),     ("^IXIC", "indices"),
            ("^GSPC", "indices"),    ("^FTSE", "indices"),
        ]

        def _fetch_one(asset_cat):
            asset, cat = asset_cat
            try:
                df = _fetcher.get_ohlcv(asset, cat, interval="1d", periods=3)
                if df is not None and len(df) >= 2 and "close" in df.columns:
                    closes = df["close"].astype(float)
                    chg = (closes.iloc[-1] - closes.iloc[-2]) / closes.iloc[-2] * 100
                    return {
                        "asset": asset, "category": cat,
                        "change_pct": round(float(chg), 2),
                        "price": round(float(closes.iloc[-1]), 5),
                    }
            except Exception:
                pass
            return None

        results = []
        with ThreadPoolExecutor(max_workers=6) as pool:
            futures = {pool.submit(_fetch_one, ac): ac for ac in sample}
            for future in as_completed(futures):
                r = future.result()
                if r:
                    results.append(r)

        results.sort(key=lambda x: x["change_pct"], reverse=True)
        payload = {"success": True, "items": results}
        _cache_set("heatmap", payload, ttl=60)
        return jsonify(payload)
    except Exception as _e:
        return jsonify({"success": False, "error": str(_e)}), 500


@app.route("/api/correlation-matrix")
def api_correlation_matrix():
    # Cache for 10 minutes — 30-day correlation barely changes minute-to-minute
    cached = _cache_get("correlation")
    if cached is not None:
        return jsonify(cached)
    try:
        import pandas as pd
        import numpy as np
        from concurrent.futures import ThreadPoolExecutor
        # Representative cross-asset selection from your 18
        assets = [
            "BTC-USD", "ETH-USD", "GC=F", "SI=F",
            "EUR/USD", "GBP/USD", "^GSPC", "^FTSE",
        ]

        def _fetch_close(a):
            try:
                df = _fetcher.get_ohlcv(a, _cat(a), interval="1d", periods=35)
                if df is not None and not df.empty and "close" in df.columns:
                    return a, df["close"].astype(float)
            except Exception:
                pass
            return a, None

        closes: Dict[str, Any] = {}
        with ThreadPoolExecutor(max_workers=6) as pool:
            for asset, series in pool.map(_fetch_close, assets):
                if series is not None:
                    closes[asset] = series

        if len(closes) < 2:
            return jsonify({"success": False, "error": "Not enough data"})
        frame = pd.DataFrame(closes).pct_change().dropna()
        corr  = frame.corr().round(3)
        payload = {"success": True, "labels": list(corr.columns),
                   "matrix": corr.values.tolist()}
        _cache_set("correlation", payload, ttl=600)
        return jsonify(payload)
    except Exception as _e:
        return jsonify({"success": False, "error": str(_e)}), 500


# ══════════════════════════════════════════════════════════════════════════════
# API — AI PREDICTIONS
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/api/accuracy")
def api_accuracy():
    days = min(int(request.args.get("days", 30)), 90)
    if _pred_tracker:
        return jsonify({"success": True,
                        "data": _pred_tracker.get_accuracy_stats(days_back=days)})
    empty = {
        "by_horizon": {
            "1H":  {"total": 0, "correct": 0, "accuracy_pct": 0, "avg_move_pct": 0},
            "4H":  {"total": 0, "correct": 0, "accuracy_pct": 0, "avg_move_pct": 0},
            "24H": {"total": 0, "correct": 0, "accuracy_pct": 0, "avg_move_pct": 0},
        },
        "by_asset": {}, "recent": [], "days_back": days,
    }
    return jsonify({"success": False, "data": empty})


@app.route("/api/predictions/summary")
def api_predictions_summary():
    try:
        stats = _pred_tracker.get_accuracy_stats(days_back=30) if _pred_tracker else {}
        with _sig_lock:
            sigs = list(_sig_store.values())
        preds = []
        for s in sigs:
            d = s.get("signal", s.get("direction", "HOLD"))
            if d in ("HOLD", "CLOSED"):
                continue
            e  = float(s.get("entry_price", 0) or 0)
            sl = float(s.get("stop_loss",   0) or 0)
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
    except Exception as _e:
        return jsonify({"success": False, "error": str(_e)}), 500


# ══════════════════════════════════════════════════════════════════════════════
# API — WHALE INTELLIGENCE
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/api/whale/summary")
def api_whale_summary():
    # Cache for 2 minutes — whale alerts are collected in background, no need to
    # call get_alerts() on every page load
    cached = _cache_get("whale_summary")
    if cached is not None:
        return jsonify(cached)
    try:
        wm = _get_whale()
        if not wm:
            return jsonify({"success": True, "alerts": [], "total_volume_usd": 0,
                            "top_assets": [], "recent": [], "alert_count_24h": 0})
        # WhaleAlertManager.get_alerts(min_value_usd, hours)
        alerts    = wm.get_alerts(min_value_usd=500_000, hours=24)
        # WhaleAlertManager.get_top_alerts(limit, days)
        top       = wm.get_top_alerts(limit=10, days=7)
        total_vol = sum(float(a.get("value_usd", 0)) for a in alerts)
        by_asset: Dict[str, float] = {}
        for a in alerts:
            sym = a.get("symbol", a.get("asset", ""))
            by_asset[sym] = by_asset.get(sym, 0.0) + float(a.get("value_usd", 0))
        top_assets = sorted(by_asset.items(), key=lambda x: x[1], reverse=True)[:8]
        payload = {
            "success":          True,
            "alerts":           alerts[:20],
            "total_volume_usd": round(total_vol, 0),
            "alert_count_24h":  len(alerts),
            "top_assets":       [{"asset": k, "volume": round(v)} for k, v in top_assets],
            "recent":           top[:10],
        }
        _cache_set("whale_summary", payload, ttl=120)
        return jsonify(payload)
    except Exception as _e:
        return jsonify({"success": False, "error": str(_e)}), 500


# ══════════════════════════════════════════════════════════════════════════════
# API — SENTIMENT INTELLIGENCE
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/api/sentiment/dashboard")
def api_sentiment_dashboard():
    # Cache for 5 minutes — fear/greed, VIX, and news change slowly.
    # This route makes 4 chained external calls: get_comprehensive_sentiment,
    # fetch_fear_greed_index, fetch_vix, fetch_all_sources.
    # Without caching every page open waits 3-6 seconds.
    cached = _cache_get("sentiment_dashboard")
    if cached is not None:
        return jsonify(cached)
    try:
        sa = _get_sent()
        if sa is None:
            return jsonify({"success": False, "error": "SentimentAnalyzer unavailable"}), 503

        result: Dict = {
            "success": True, "overall_sentiment": "Neutral", "score": 0.0,
            "fear_greed": {"value": 50, "classification": "Neutral"},
            "vix": {"value": 20, "classification": "Normal"},
            "article_count": 0,
            "sentiment_distribution": {"bullish": 0, "neutral": 0, "bearish": 0},
            "articles": [], "whale_alerts": [],
        }

        # SentimentAnalyzer.get_comprehensive_sentiment(self, asset=None)
        ms = sa.get_comprehensive_sentiment()
        if ms:
            result["score"]             = float(ms.get("score", 0))
            result["overall_sentiment"] = ms.get("interpretation", "Neutral")

        # SentimentAnalyzer.fetch_fear_greed_index(self)
        fg = sa.fetch_fear_greed_index()
        if fg:
            result["fear_greed"] = {"value": fg.get("value", 50),
                                    "classification": fg.get("classification", "Neutral")}

        # SentimentAnalyzer.fetch_vix(self)
        vix = sa.fetch_vix()
        if vix:
            result["vix"] = {"value": vix.get("value", 20),
                             "classification": vix.get("classification", "Normal")}

        if hasattr(sa, "news_integrator"):
            try:
                arts = sa.news_integrator.fetch_all_sources()
                # Compute distribution on full article set before slicing for display
                if arts:
                    b  = sum(1 for a in arts if float(a.get("sentiment", 0)) > 0.1)
                    be = sum(1 for a in arts if float(a.get("sentiment", 0)) < -0.1)
                    result["sentiment_distribution"] = {
                        "bullish": b, "neutral": len(arts) - b - be, "bearish": be,
                    }
                result["articles"]      = sorted(arts, key=lambda x: x.get("date", ""), reverse=True)[:20]
                result["article_count"] = len(arts)
            except Exception:
                pass

        # SentimentAnalyzer.fetch_whale_alerts(self, min_value_usd=1000000)
        result["whale_alerts"] = sa.fetch_whale_alerts(min_value_usd=1_000_000)[:10]

        _cache_set("sentiment_dashboard", result, ttl=300)
        return jsonify(result)
    except Exception as _e:
        return jsonify({"success": False, "error": str(_e)}), 500


@app.route("/api/sentiment/by-asset")
def api_sentiment_by_asset():
    # Cache for 5 minutes — calls get_comprehensive_sentiment for all 18 assets
    # via ThreadPoolExecutor. Most expensive route: 18 concurrent external calls.
    cached = _cache_get("sentiment_by_asset")
    if cached is not None:
        return jsonify(cached)
    try:
        sa = _get_sent()
        if not sa:
            return jsonify({"success": False, "error": "SentimentAnalyzer unavailable"})
        # Exactly your 18 assets in canonical form
        watch = [
            "BTC-USD", "ETH-USD", "BNB-USD", "SOL-USD", "XRP-USD",
            "GC=F", "SI=F", "CL=F",
            "EUR/USD", "GBP/JPY", "GBP/USD", "AUD/USD", "USD/JPY", "USD/CAD",
            "^DJI", "^IXIC", "^GSPC", "^FTSE",
        ]
        from concurrent.futures import ThreadPoolExecutor

        def _sent_one(asset):
            score = 0.0
            try:
                r     = sa.get_comprehensive_sentiment(asset)
                score = float(r.get("composite_score", r.get("score", 0))) if r else 0.0
            except Exception:
                pass
            return {
                "asset":    asset,
                "category": _cat(asset),
                "score":    round(score, 3),
                "label":    "Bullish" if score > 0.1 else "Bearish" if score < -0.1 else "Neutral",
            }

        with ThreadPoolExecutor(max_workers=6) as pool:
            results = list(pool.map(_sent_one, watch))

        results.sort(key=lambda x: x["score"], reverse=True)
        payload = {"success": True, "assets": results}
        _cache_set("sentiment_by_asset", payload, ttl=300)
        return jsonify(payload)
    except Exception as _e:
        return jsonify({"success": False, "error": str(_e)}), 500


@app.route("/api/market/events")
def api_market_events():
    try:
        events: List = []
        sa = _get_sent()
        if sa:
            raw = sa.get_market_events()
            if isinstance(raw, dict):
                events = raw.get("events", raw.get("calendar", []))
            elif isinstance(raw, list):
                events = raw
        try:
            from market_calendar import get_high_impact_events
            cal = get_high_impact_events()
            if cal:
                events = (events + list(cal))[:20]
        except Exception:
            pass
        return jsonify({"success": True, "events": events[:20]})
    except Exception as _e:
        return jsonify({"success": False, "error": str(_e)}), 500


# ══════════════════════════════════════════════════════════════════════════════
# API — RISK DASHBOARD
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/api/risk/portfolio")
def api_risk_portfolio():
    try:
        core = _core()
        if not core:
            return jsonify({"success": False, "error": "Engine not ready"})

        positions = core.get_positions()
        balance   = core.get_balance()
        perf      = core.get_performance()

        # PortfolioRiskEngine.get_portfolio_stats(open_positions, balance)
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
            by_cat[cat]["pnl"]      += float(p.get("pnl", 0))
            by_cat[cat]["exposure"] += (float(p.get("position_size", 0))
                                        * float(p.get("entry_price", 0)))

        # TradingCore.get_closed_trades(limit=100)
        closed   = core.get_closed_trades(limit=100)
        wins     = [t for t in closed if float(t.get("pnl", 0)) > 0]
        losses   = [t for t in closed if float(t.get("pnl", 0)) <= 0]
        avg_win  = sum(float(t["pnl"]) for t in wins)   / len(wins)   if wins   else 0.0
        avg_loss = sum(float(t["pnl"]) for t in losses) / len(losses) if losses else 0.0
        pf       = abs(avg_win / avg_loss) if avg_loss else 0.0

        return jsonify({
            "success":        True,
            "balance":        balance,
            "open_positions": len(positions),
            "total_exposure": risk_stats.get("total_exposure", 0),
            "exposure_pct":   risk_stats.get("exposure_pct", 0),
            "drawdown_pct":   risk_stats.get("drawdown_pct", 0),
            "peak_balance":   risk_stats.get("peak_balance", balance),
            "by_category":    by_cat,
            "win_rate":       perf.get("win_rate", 0),
            "profit_factor":  round(pf, 2),
            "avg_win":        round(avg_win, 2),
            "avg_loss":       round(avg_loss, 2),
            "total_trades":   perf.get("total_trades", 0),
            "total_pnl":      perf.get("total_pnl", 0),
        })
    except Exception as _e:
        return jsonify({"success": False, "error": str(_e)}), 500


# ══════════════════════════════════════════════════════════════════════════════
# API — STRATEGY LAB
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/api/strategy/performance")
def api_strategy_performance():
    try:
        core = _core()
        if not core:
            return jsonify({"success": False, "error": "Engine not ready"})

        # TradingCore.get_strategy_stats() → Dict[str, Dict]
        stats  = core.get_strategy_stats()
        trades = core.get_closed_trades(limit=200)

        enriched: Dict = {}
        for strat, s in stats.items():
            total = s.get("wins", 0) + s.get("losses", 0)
            pnl   = s.get("pnl", 0)
            wr    = s.get("wins", 0) / total * 100 if total else 0
            durs  = [int(t.get("duration_minutes", 0))
                     for t in trades
                     if t.get("strategy_id") == strat and t.get("duration_minutes")]
            avg_dur = sum(durs) / len(durs) if durs else 0
            enriched[strat] = {
                **s,
                "total":            total,
                "win_rate":         round(wr, 1),
                "avg_duration_min": round(avg_dur),
                "avg_trade_pnl":    round(pnl / total, 4) if total else 0,
            }

        timeline = [{
            "asset":     t.get("asset", ""),
            "direction": t.get("direction", t.get("signal", "")),
            "pnl":       float(t.get("pnl", 0)),
            "strategy":  t.get("strategy_id", ""),
            "exit_time": str(t.get("exit_time", ""))[:16],
            "conf":      float(t.get("confidence", 0)),
        } for t in trades[:50]]

        return jsonify({"success": True, "strategies": enriched, "timeline": timeline})
    except Exception as _e:
        return jsonify({"success": False, "error": str(_e)}), 500


@app.route("/api/backtest/run")
def api_backtest_run():
    try:
        asset  = request.args.get("asset", "BTC-USD")
        period = request.args.get("period", "90d")
        days   = {"30d": 30, "90d": 90, "180d": 180, "365d": 365, "730d": 730}.get(period, 90)
        cat    = _cat(asset)

        df = _fetcher.get_ohlcv(asset, cat, interval="1d", periods=days)
        if df is None or df.empty:
            return jsonify({"success": False, "error": f"No data for {asset}"}), 404

        try:
            from indicators.technical import TechnicalIndicators
            df = TechnicalIndicators.add_all_indicators(df)
        except Exception:
            pass

        # BacktestEngine.run(self, asset, category, df, warmup=50) → BacktestResult
        from backtest.engine import BacktestEngine
        result = BacktestEngine(initial_balance=_args.balance).run(asset, cat, df)
        rd     = result.to_dict()

        bal = _args.balance
        equity_curve = []
        for i, trade in enumerate(result.trades):
            bal += float(trade.get("pnl", 0))
            equity_curve.append({
                "date":      str(trade.get("open_time", i))[:10],
                "value":     round(bal, 2),
                "benchmark": round(_args.balance * (1 + i * 0.0005), 2),
            })

        monthly: Dict = defaultdict(float)
        for trade in result.trades:
            key = str(trade.get("open_time", ""))[:7] or "Unknown"
            monthly[key] += float(trade.get("pnl", 0))

        def _clean(obj):
            if isinstance(obj, dict):  return {k: _clean(v) for k, v in obj.items()}
            if isinstance(obj, list):  return [_clean(v) for v in obj]
            try:
                import numpy as np
                if isinstance(obj, np.integer):  return int(obj)
                if isinstance(obj, np.floating): return float(obj)
            except ImportError:
                pass
            if hasattr(obj, "isoformat"): return obj.isoformat()
            return obj

        return jsonify(_clean({
            "success": True,
            "results": {
                **rd,
                "equity_curve": equity_curve,
                "monthly_returns": [
                    {"month": k, "return_pct": round(v / _args.balance * 100, 2)}
                    for k, v in sorted(monthly.items())
                ],
                "trades": result.trades,
            },
        }))
    except Exception as _e:
        return jsonify({"success": False, "error": str(_e)}), 500


# ══════════════════════════════════════════════════════════════════════════════
# API — SYSTEM MONITOR
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/api/system/health")
def api_system_health():
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

        # RedisBroker.is_connected() method
        redis_ok = False
        try:
            if _redis_broker:
                redis_ok = bool(_redis_broker.is_connected())
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

        return jsonify({
            "success":          True,
            "ram_pct":          round(ram_pct, 1),
            "cpu_pct":          round(cpu_pct, 1),
            "disk_pct":         round(disk_pct, 1),
            "process_mem_mb":   proc_mb,
            "processes":        processes,
            "open_positions":   health.get("open_positions", 0),
            "active_cooldowns": health.get("active_cooldowns", 0),
            "issues":           health.get("issues", []),
            "strategy_mode":    health.get("strategy_mode", "—"),
            "balance":          core.get_balance() if core else _args.balance,
            "timestamp":        datetime.now().isoformat(),
        })
    except Exception as _e:
        return jsonify({"success": False, "error": str(_e)}), 500


# ══════════════════════════════════════════════════════════════════════════════
# start_dashboard — bot.py entry point  (blocking)
# ══════════════════════════════════════════════════════════════════════════════

def start_dashboard(core, host: str = "0.0.0.0", port: int = 5000) -> None:
    """
    Called by bot.py after engine.start().
    Wires TradingCore, starts background threads, then blocks on Flask.
    """
    inject_core(core)

    threading.Thread(target=_bg_refresh, name="DashBgRefresh", daemon=True).start()

    try:
        from websocket_manager import WebSocketManager
        def _cb(source, symbol, price, volume, side, ts=None):
            add_transaction(source, symbol, price, volume, side)
        ws = WebSocketManager()
        ws.start()
        ws.subscribe_bybit(["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"], _cb)
        ws.subscribe_finnhub(["AAPL", "MSFT", "GOOGL", "TSLA", "NVDA", "AMZN"], _cb)
        ws.subscribe_twelvedata(["EUR/USD", "XAU/USD"], _cb)
        logger.info("[dashboard] WebSocket streams started")
    except Exception as _e:
        logger.warning(f"[dashboard] WebSocket streams failed (non-fatal): {_e}")

    logger.info(f"[dashboard] http://{host}:{port}/command-center")
    app.run(debug=False, host=host, port=port, threaded=True, use_reloader=False)


# ══════════════════════════════════════════════════════════════════════════════
# standalone mode
# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    logger.info("Robbie dashboard — standalone (no TradingCore)")
    logger.info("http://localhost:5000/command-center")
    threading.Thread(target=_bg_refresh, name="DashBgRefresh", daemon=True).start()
    app.run(debug=False, host="0.0.0.0", port=5000, threaded=True, use_reloader=False)