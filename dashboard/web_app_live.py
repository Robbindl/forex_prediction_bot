"""
dashboard/web_app_live.py — Flask + SocketIO live dashboard.
Fixed: template_folder points to root templates/ directory.
Added: all missing API endpoints required by the 7 HTML templates.
"""
from __future__ import annotations
import os
import threading
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Optional, TYPE_CHECKING

from flask import Flask, jsonify, render_template, request, Response
from flask_cors import CORS
from flask_socketio import SocketIO, emit

from utils.logger import get_logger
from config.config import ASSET_CATEGORIES

if TYPE_CHECKING:
    from core.engine import TradingCore

logger = get_logger()

_TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), "..", "templates")

app      = Flask(__name__, template_folder=_TEMPLATE_DIR)
CORS(app)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading")

_engine: Optional["TradingCore"] = None

_settings: dict = {
    "interval": "15m",
    "balance":  10.0,
    "risk":     1.0,
    "filter":   "all",
}


def init_app(engine: "TradingCore") -> None:
    global _engine
    _engine = engine
    logger.info("[Dashboard] TradingCore wired")


def _e() -> Optional["TradingCore"]:
    return _engine


def _asset_category(asset: str) -> str:
    if "-USD" in asset and "/" not in asset:
        return "crypto"
    if "/" in asset and "-" not in asset:
        return "forex"
    if "=F" in asset:
        return "commodities"
    if asset.startswith("^"):
        return "indices"
    return "stocks"


def _active_sessions() -> list:
    hour = datetime.utcnow().hour
    sessions = []
    if 22 <= hour or hour < 7:  sessions.append("Sydney")
    if 0  <= hour < 9:          sessions.append("Tokyo")
    if 7  <= hour < 16:         sessions.append("London")
    if 12 <= hour < 21:         sessions.append("New York")
    return sessions or ["Off-hours"]


# ── Page routes ───────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index_live.html")

@app.route("/dashboard")
@app.route("/status")
def dashboard():
    return render_template("status_dashboard.html")

@app.route("/chart")
def chart():
    return render_template("chart_live.html")

@app.route("/backtest")
def backtest_view():
    return render_template("backtest_visualizer.html")

@app.route("/sentiment")
def sentiment_view():
    return render_template("sentiment_dashboard.html")

@app.route("/accuracy")
def accuracy_view():
    return render_template("accuracy_dashboard.html")

@app.route("/websocket")
@app.route("/websocket-feed")
def websocket_view():
    return render_template("websocket_feed.html")


# ── Core API ──────────────────────────────────────────────────────────────────

@app.route("/api/status")
def api_status():
    e = _e()
    if not e:
        return jsonify({"status": "not_ready"})
    return jsonify({
        "status":        "running" if e.is_running else "stopped",
        "ready":         e.is_ready,
        "strategy_mode": e.strategy_mode,
        "balance":       e.get_balance(),
        "last_refresh":  datetime.now(tz=timezone.utc).isoformat(),
        "timestamp":     datetime.utcnow().isoformat(),
    })

@app.route("/api/positions")
def api_positions():
    e = _e()
    positions = e.get_positions() if e else []
    return jsonify({"positions": positions, "count": len(positions)})

@app.route("/api/performance")
def api_performance():
    e = _e()
    return jsonify(e.get_performance() if e else {})

@app.route("/api/trades")
def api_trades():
    e     = _e()
    limit = int(request.args.get("limit", 50))
    trades = e.get_closed_trades(limit) if e else []
    return jsonify({"trades": trades, "count": len(trades)})

@app.route("/api/health")
def api_health():
    e = _e()
    return jsonify(e.health_report() if e else {"status": "not_ready"})

@app.route("/api/cooldowns")
def api_cooldowns():
    e = _e()
    return jsonify(e.get_cooldowns() if e else {})

@app.route("/api/assets")
def api_assets():
    return jsonify(ASSET_CATEGORIES)

@app.route("/api/strategy_stats")
def api_strategy_stats():
    e = _e()
    return jsonify(e.get_strategy_stats() if e else {})

@app.route("/api/signal/<path:asset>")
def api_signal(asset: str):
    e   = _e()
    sig = e.get_signal_for_asset(asset) if e else None
    if sig:
        return jsonify({"success": True, "signal": sig, "asset": asset,
                        "human_response": _build_human_response(sig, asset)})
    return jsonify({
        "success": False,
        "signal":  {"direction": "HOLD", "confidence": 0, "current_price": 0},
        "human_response": {"direction": "HOLD", "confidence": 0,
                           "reasons": ["No signal at this time"], "current_price": 0},
        "asset": asset, "error": "No signal available",
    })

def _build_human_response(sig: dict, asset: str) -> dict:
    direction = sig.get("direction", "HOLD")
    conf      = float(sig.get("confidence", 0))
    meta      = sig.get("metadata", {})
    inds      = sig.get("indicators", {})
    reasons   = []
    if inds.get("rsi"):
        reasons.append(f"RSI at {inds['rsi']:.1f} — "
                       + ("oversold bounce" if direction == "BUY" else "overbought reversal"))
    if inds.get("votes"):
        reasons.append(f"{inds['votes']} of {inds.get('total_signals', 3)} strategies agree")
    regime = meta.get("regime", "")
    if regime and regime != "unknown":
        reasons.append(f"Market regime: {regime.replace('_', ' ')}")
    if meta.get("session"):
        reasons.append(f"Active session: {meta['session'].title()}")
    if not reasons:
        reasons = [f"Voting pipeline: {direction} signal at {conf:.0%} confidence"]
    return {
        "direction": direction, "confidence": conf,
        "current_price": sig.get("entry_price", 0),
        "predicted_price": sig.get("take_profit", 0),
        "stop_loss": sig.get("stop_loss", 0),
        "reasons": reasons[:4], "whale_alerts": "",
    }

@app.route("/api/close_position/<trade_id>", methods=["POST"])
def api_close_position(trade_id: str):
    e      = _e()
    result = e.close_position_manually(trade_id) if e and hasattr(e, "close_position_manually") else None
    return jsonify({"success": bool(result), "trade": result})


# ── index_live.html ───────────────────────────────────────────────────────────

@app.route("/api/signals/live")
def api_signals_live():
    e = _e()
    if not e:
        return jsonify({"success": True, "signals": [],
                        "last_refresh": datetime.utcnow().isoformat()})

    filter_val = request.args.get("filter", "all")
    positions  = e.get_positions()
    perf       = e.get_performance()
    signals    = []

    for pos in positions:
        direction = (pos.get("direction") or pos.get("signal", "BUY")).upper()
        if filter_val == "buy"             and direction != "BUY":  continue
        if filter_val == "sell"            and direction != "SELL": continue
        conf = float(pos.get("confidence", 0))
        if filter_val == "high-confidence" and conf < 0.70:         continue

        entry    = float(pos.get("entry_price", 0))
        sl       = float(pos.get("stop_loss",   0))
        tp       = float(pos.get("take_profit", 0))
        risk_pct = abs(entry - sl) / entry * 100 if entry else 0

        try:
            open_dt   = datetime.fromisoformat(
                pos.get("open_time", datetime.utcnow().isoformat()).replace("Z", "")
            )
            elapsed   = (datetime.utcnow() - open_dt.replace(tzinfo=None)).total_seconds() / 60
            remaining = max(0.0, 5.0 - elapsed)
        except Exception:
            remaining = 5.0

        tp_levels = pos.get("take_profit_levels", [])
        signals.append({
            "asset":              pos.get("asset", ""),
            "signal":             direction,
            "category":           pos.get("category", ""),
            "confidence":         conf,
            "entry_price":        entry,
            "stop_loss":          sl,
            "take_profit":        tp,
            "take_profit_levels": [{"price": lv} for lv in tp_levels],
            "position_size":      float(pos.get("position_size", 0)),
            "risk_pct":           round(risk_pct, 3),
            "strategy_id":        pos.get("strategy_id", ""),
            "reason":             pos.get("strategy_id", "Voting pipeline"),
            "time_remaining":     round(remaining, 1),
            "generated_at":       str(pos.get("open_time", ""))[:16],
        })

    return jsonify({
        "success": True, "signals": signals,
        "last_refresh": datetime.utcnow().isoformat(),
        "balance": e.get_balance(),
        "total_trades": perf.get("total_trades", 0),
        "win_rate": perf.get("win_rate", 0),
    })

@app.route("/api/settings/update", methods=["POST"])
def api_settings_update():
    global _settings
    data = request.get_json(force=True) or {}
    _settings.update({k: v for k, v in data.items() if k in _settings})
    return jsonify({"success": True, "settings": _settings})

@app.route("/api/refresh/manual", methods=["POST"])
def api_refresh_manual():
    return jsonify({"success": True, "timestamp": datetime.utcnow().isoformat()})


# ── status_dashboard.html ─────────────────────────────────────────────────────

@app.route("/api/system-status")
def api_system_status():
    e = _e()
    if not e:
        return jsonify({"success": False})

    perf      = e.get_performance()
    positions = e.get_positions()
    trades    = e.get_closed_trades(15)
    daily     = e.get_daily_stats()

    total_exposure = sum(
        float(p.get("entry_price", 0)) * float(p.get("position_size", 0))
        for p in positions
    )
    cat_counts: dict = {}
    for p in positions:
        cat = p.get("category", "unknown")
        cat_counts[cat] = cat_counts.get(cat, 0) + 1

    return jsonify({
        "success":        True,
        "balance":        round(e.get_balance(), 2),
        "pnl":            round(daily.get("daily_pnl", 0), 2),
        "open_positions": len(positions),
        "win_rate":       perf.get("win_rate", 0),
        "processes": {
            "Trading Engine":  e.is_running,
            "ML Predictor":    e.is_ready,
            "Risk Manager":    e.is_ready,
            "Paper Trader":    e.is_ready,
            "Database":        True,
            "Signal Pipeline": e.is_ready,
        },
        "portfolio": {
            "total_exposure": round(total_exposure, 2),
            "today_trades":   daily.get("daily_trades", 0),
            "mode":           e.strategy_mode,
            "ai_status":      "Active" if e.is_ready else "Loading",
            "risk_breakdown": {
                cat: round(count / max(1, len(positions)) * 100, 1)
                for cat, count in cat_counts.items()
            },
        },
        "recent_trades": trades,
    })


# ── backtest_visualizer.html ──────────────────────────────────────────────────

@app.route("/api/backtest/run", methods=["POST"])
def api_backtest_run():
    data    = request.get_json(force=True) or {}
    asset   = data.get("asset", "BTC-USD")
    period  = data.get("period", "3mo")
    capital = float(data.get("initial_capital", 10000))
    bars    = {"1mo": 30, "3mo": 90, "6mo": 180, "1y": 365, "2y": 730}.get(period, 90)
    cat     = _asset_category(asset)

    try:
        from data.fetcher    import DataFetcher
        from backtest.engine import BacktestEngine
        df = DataFetcher().get_ohlcv(asset, cat, "1d", bars)
        if df is None or df.empty:
            return jsonify({"success": False, "error": f"No data for {asset}"})

        result = BacktestEngine(initial_balance=capital).run(asset, cat, df)
        d      = result.to_dict()

        balance      = capital
        equity_curve = []
        for i, trade in enumerate(result.trades):
            balance += float(trade.get("pnl", 0))
            equity_curve.append({
                "date":      str(trade.get("open_time", i))[:10],
                "value":     round(balance, 2),
                "benchmark": round(capital * (1 + i * 0.0005), 2),
            })

        monthly: dict = defaultdict(float)
        for trade in result.trades:
            monthly[str(trade.get("open_time", ""))[:7] or "Unknown"] += float(trade.get("pnl", 0))

        return jsonify({
            "success": True,
            "results": {
                **d,
                "total_return":    d.get("return_pct", 0),
                "equity_curve":    equity_curve,
                "monthly_returns": [
                    {"month": k, "return_pct": round(v / capital * 100, 2)}
                    for k, v in sorted(monthly.items())
                ],
                "trades": [
                    {**t, "date": str(t.get("open_time", ""))[:10],
                     "return_pct": round(float(t.get("pnl", 0)) / capital * 100, 4),
                     "bars": t.get("entry_bar", "—")}
                    for t in result.trades
                ],
            },
        })
    except Exception as ex:
        logger.error(f"[Dashboard] Backtest run error: {ex}")
        return jsonify({"success": False, "error": str(ex)})


# ── chart_live.html ───────────────────────────────────────────────────────────

@app.route("/api/chart/assets")
def api_chart_assets():
    assets = [
        {"symbol": sym, "category": cat}
        for cat, syms in ASSET_CATEGORIES.items()
        for sym in syms
    ]
    return jsonify({"success": True, "assets": assets})

@app.route("/api/chart/candles")
def api_chart_candles():
    asset    = request.args.get("asset", "BTC-USD")
    interval = request.args.get("interval", "1h")
    cat      = _asset_category(asset)
    try:
        from data.fetcher import DataFetcher
        df = DataFetcher().get_ohlcv(asset, cat, interval, 200)
        if df is None or df.empty:
            return jsonify({"success": False, "candles": []})
        candles = [
            {"time": int(i), "open": round(float(r["open"]), 6),
             "high": round(float(r["high"]), 6), "low": round(float(r["low"]), 6),
             "close": round(float(r["close"]), 6), "volume": round(float(r.get("volume", 0)), 2)}
            for i, r in df.iterrows()
        ]
        return jsonify({"success": True, "candles": candles, "asset": asset})
    except Exception as ex:
        logger.error(f"[Dashboard] Chart candles error: {ex}")
        return jsonify({"success": False, "candles": [], "error": str(ex)})

@app.route("/api/chart/stream")
def api_chart_stream():
    asset = request.args.get("asset", "BTC-USD")
    cat   = _asset_category(asset)

    def generate():
        import json
        while True:
            try:
                from data.fetcher import DataFetcher
                price, spread = DataFetcher().get_real_time_price(asset, cat)
                if price:
                    yield f"data: {json.dumps({'type':'price','asset':asset,'price':price,'spread':spread or 0,'time':datetime.utcnow().isoformat()})}\n\n"
            except Exception:
                pass
            time.sleep(5)

    return Response(generate(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

@app.route("/api/prediction-overlay/<path:asset>")
def api_prediction_overlay(asset: str):
    e = _e()
    if not e:
        return jsonify({"success": False, "overlay": None})
    try:
        sig = e.get_signal_for_asset(asset)
        if sig:
            return jsonify({"success": True, "overlay": {
                "direction": sig.get("direction", "HOLD"),
                "confidence": sig.get("confidence", 0),
                "entry": sig.get("entry_price", 0),
                "stop_loss": sig.get("stop_loss", 0),
                "take_profit": sig.get("take_profit", 0),
            }})
    except Exception as ex:
        logger.debug(f"[Dashboard] Prediction overlay error: {ex}")
    return jsonify({"success": False, "overlay": None})


# ── sentiment_dashboard.html ──────────────────────────────────────────────────

@app.route("/api/sentiment/dashboard")
def api_sentiment_dashboard():
    score = 0.0
    try:
        from sentiment_analyzer import SentimentAnalyzer
        result = SentimentAnalyzer().get_comprehensive_sentiment("BTC-USD", "crypto")
        score  = float(result.get("composite_score", 0)) if isinstance(result, dict) else 0.0
    except Exception:
        pass
    label = ("Extreme Fear" if score < -0.6 else "Fear" if score < -0.2
             else "Neutral" if score < 0.2 else "Greed" if score < 0.6
             else "Extreme Greed")
    return jsonify({
        "success": True, "articles": [], "whale_alerts": [],
        "overall_score": round(score, 3), "overall_label": label,
        "fear_greed_index": round((score + 1) / 2 * 100),
        "categories": {}, "timestamp": datetime.utcnow().isoformat(),
    })

@app.route("/api/market/events")
def api_market_events():
    e         = _e()
    positions = e.get_positions() if e else []
    daily     = e.get_daily_stats() if e else {}
    daily_pnl = float(daily.get("daily_pnl", 0))
    reduce    = daily_pnl < -50
    return jsonify({
        "success": True,
        "events": {
            "risk_outlook": {
                "reduce_trading": reduce,
                "reason": f"Daily loss ${abs(daily_pnl):.2f}" if reduce else "Normal trading conditions",
            },
            "open_positions": len(positions),
            "market_sessions": _active_sessions(),
        },
    })


# ── accuracy_dashboard.html ───────────────────────────────────────────────────

@app.route("/api/accuracy")
def api_accuracy():
    days  = int(request.args.get("days", 30))
    e     = _e()
    perf  = e.get_performance() if e else {}
    total = perf.get("total_trades", 0)
    wr    = float(perf.get("win_rate", 0))
    acc   = round(wr, 1)
    h     = {"accuracy_pct": acc, "total": total, "correct": int(total * wr / 100)}
    return jsonify({
        "success": True,
        "data": {"by_horizon": {"1H": h, "4H": h, "24H": h},
                 "overall": acc, "total_trades": total, "period_days": days},
    })

@app.route("/api/alpha")
def api_alpha():
    n         = int(request.args.get("n", 20))
    e         = _e()
    positions = e.get_positions() if e else []
    signals   = [
        {"asset": p.get("asset", ""),
         "direction": (p.get("direction") or p.get("signal", "BUY")).upper(),
         "confidence": float(p.get("confidence", 0)),
         "entry_price": float(p.get("entry_price", 0)),
         "category": p.get("category", ""),
         "strategy": p.get("strategy_id", ""),
         "pnl": float(p.get("pnl", 0))}
        for p in positions[:n]
    ]
    return jsonify({"success": True, "signals": signals, "count": len(signals)})


# ── websocket_feed.html ───────────────────────────────────────────────────────

@app.route("/api/websocket/feed")
def api_websocket_feed():
    source    = request.args.get("source", "prices")
    e         = _e()
    positions = e.get_positions() if e else []
    daily     = e.get_daily_stats() if e else {}
    ts        = datetime.utcnow().isoformat()
    events    = []

    if source in ("prices", "all"):
        for pos in positions:
            events.append({
                "type":      "position",
                "asset":     pos.get("asset", ""),
                "direction": (pos.get("direction") or pos.get("signal", "")).upper(),
                "pnl":       round(float(pos.get("pnl", 0)), 4),
                "entry":     float(pos.get("entry_price", 0)),
                "time":      ts,
            })
    if source in ("trades", "all"):
        for t in (e.get_closed_trades(10) if e else []):
            events.append({
                "type": "trade_closed", "asset": t.get("asset", ""),
                "pnl": round(float(t.get("pnl", 0)), 4),
                "reason": t.get("exit_reason", ""), "time": t.get("exit_time", ts),
            })

    return jsonify({
        "success": True, "source": source, "events": events,
        "balance": e.get_balance() if e else 0,
        "daily_pnl": float(daily.get("daily_pnl", 0)), "timestamp": ts,
    })


# ── Gateway status ────────────────────────────────────────────────────────────

@app.route("/api/gateway/status")
def api_gateway_status():
    """
    Called by accuracy_dashboard.html before attempting ws://localhost:8081.
    Checks if the gateway port is open so the browser avoids a noisy
    console error when the gateway isn't running.
    """
    try:
        import socket as _socket
        with _socket.create_connection(("127.0.0.1", 8081), timeout=0.3):
            running = True
    except OSError:
        running = False

    return jsonify({"running": running, "port": 8081, "url": "ws://localhost:8081"})


# ── Legacy alias ──────────────────────────────────────────────────────────────

@app.route("/api/backtest", methods=["POST"])
def api_backtest():
    return api_backtest_run()


# ── WebSocket (SocketIO) ──────────────────────────────────────────────────────

@socketio.on("connect")
def ws_connect():
    logger.debug("[Dashboard] WebSocket client connected")
    e = _e()
    if e:
        emit("status", {"connected": True, "balance": e.get_balance()})

@socketio.on("disconnect")
def ws_disconnect():
    logger.debug("[Dashboard] WebSocket client disconnected")

@socketio.on("request_positions")
def ws_positions():
    e = _e()
    emit("positions", {"positions": e.get_positions() if e else []})

@socketio.on("request_performance")
def ws_performance():
    e = _e()
    emit("performance", e.get_performance() if e else {})


def _broadcast_loop() -> None:
    while True:
        try:
            e = _e()
            if e and e.is_ready:
                socketio.emit("positions",   {"positions": e.get_positions()})
                socketio.emit("balance",     {"balance":   e.get_balance()})
                socketio.emit("performance", e.get_performance())
        except Exception:
            pass
        time.sleep(5)


def start_dashboard(engine: "TradingCore", host: str = "0.0.0.0", port: int = 5000) -> None:
    init_app(engine)
    t = threading.Thread(target=_broadcast_loop, daemon=True, name="ws-broadcast")
    t.start()
    logger.info(f"[Dashboard] Starting on http://{host}:{port}")
    socketio.run(app, host=host, port=port, debug=False, use_reloader=False)