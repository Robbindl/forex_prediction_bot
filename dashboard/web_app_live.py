"""
dashboard/web_app_live.py — Flask + SocketIO live dashboard.
Fixed: template_folder points to root templates/ directory.
"""
from __future__ import annotations
import os
import threading
from datetime import datetime
from typing import Any, Optional, TYPE_CHECKING

from flask import Flask, jsonify, render_template, request
from flask_cors import CORS
from flask_socketio import SocketIO, emit

from utils.logger import get_logger
from config.config import ASSET_CATEGORIES

if TYPE_CHECKING:
    from core.engine import TradingCore

logger = get_logger()

# FIX: templates/ is in project root, dashboard/ is a subdirectory
_TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), "..", "templates")

app      = Flask(__name__, template_folder=_TEMPLATE_DIR)
CORS(app)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading")

_engine: Optional["TradingCore"] = None


def init_app(engine: "TradingCore") -> None:
    global _engine
    _engine = engine
    logger.info("[Dashboard] TradingCore wired")


def _e() -> Optional["TradingCore"]:
    return _engine


# ── Page routes ───────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index_live.html")

@app.route("/dashboard")
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
def websocket_view():
    return render_template("websocket_feed.html")


# ── REST API ──────────────────────────────────────────────────────────────────

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
    return jsonify({"signal": sig, "asset": asset})

@app.route("/api/backtest", methods=["POST"])
def api_backtest():
    data     = request.get_json(force=True)
    asset    = data.get("asset", "BTC-USD")
    category = data.get("category", "crypto")
    balance  = float(data.get("balance", 10000))
    try:
        from data.fetcher    import DataFetcher
        from backtest.engine import BacktestEngine
        fetcher = DataFetcher()
        df      = fetcher.get_ohlcv(asset, category, "1d", 500)
        if df is None or df.empty:
            return jsonify({"error": "No data"}), 400
        engine = BacktestEngine(initial_balance=balance)
        result = engine.run(asset, category, df)
        return jsonify(result.to_dict())
    except Exception as ex:
        logger.error(f"[Dashboard] Backtest error: {ex}")
        return jsonify({"error": str(ex)}), 500

@app.route("/api/close_position/<trade_id>", methods=["POST"])
def api_close_position(trade_id: str):
    e      = _e()
    result = e.close_position_manually(trade_id) if e else None
    return jsonify({"success": bool(result), "trade": result})


# ── WebSocket ─────────────────────────────────────────────────────────────────

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
    import time
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