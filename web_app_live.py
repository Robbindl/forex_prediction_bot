# type: ignore
"""
ULTIMATE TRADING DASHBOARD  —  Wall-Street-Grade Signal Engine
==============================================================
All features in one file:
  • Instant signals (< 200ms) via background cache
  • Multi-timeframe confluence  (15m / 1h / 4h)
  • ATR-based stops with min 2:1 RR enforced
  • Signal learning — confidence bias from win/loss history
  • Real-time position monitoring via Server-Sent Events
  • Walk-forward ML optimisation endpoint
  • Portfolio stress test (2008 / 2020 / 2022 crash scenarios)
  • Rate limiting for every external API
  • Zero print() — all errors go through structured logger
  • Unit test endpoint  GET /api/tests
  • Install helper     GET /api/install
"""

from flask import Flask, render_template, jsonify, request, Response, stream_with_context
from flask_cors import CORS
from datetime import datetime, timedelta
import threading
import queue
import time
import sys
import json
import os
import argparse
import traceback
from typing import Dict, List, Optional, Any
from collections import deque
from pandas import Period, Timestamp

from utils.logger import logger
from telegram_manager import telegram_manager
from websocket_dashboard import recent_transactions

# ── Platform upgrades ──────────────────────────────────────────────────────────
try:
    from redis_broker import broker as _redis_broker
except Exception:
    _redis_broker = None

try:
    from orderflow_engine import orderflow_engine as _orderflow_engine
    _orderflow_engine.start()
except Exception:
    _orderflow_engine = None

try:
    from alpha_discovery import alpha_engine as _alpha_engine
    _alpha_engine.start()
except Exception:
    _alpha_engine = None

try:
    from prediction_tracker import prediction_tracker as _pred_tracker
    _pred_tracker.start()
except Exception:
    _pred_tracker = None


# ── JSON encoder (handles pandas/numpy types) ─────────────────────────────────
class _Encoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (Period, Timestamp)):  return str(obj)
        if hasattr(obj, 'isoformat'):             return obj.isoformat()
        try:
            import numpy as np
            if isinstance(obj, (np.integer,)):  return int(obj)
            if isinstance(obj, (np.floating,)): return float(obj)
            if isinstance(obj, np.ndarray):     return obj.tolist()
        except ImportError:
            pass
        return super().default(obj)


# ── CLI args ──────────────────────────────────────────────────────────────────
_parser = argparse.ArgumentParser(description='Trading Dashboard')
_parser.add_argument('--balance',     type=float, default=30)
_parser.add_argument('--no-telegram', action='store_true')
args, _ = _parser.parse_known_args()

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from data.fetcher import NASALevelFetcher, MarketHours

app = Flask(__name__)
app.json_encoder = _Encoder
CORS(app)

# ── Global error handler — always return JSON, never HTML ─────────────────────
# Prevents "Unexpected token '<'" errors in the browser when Flask throws an
# unhandled exception and would otherwise return its default HTML error page.
@app.errorhandler(Exception)
def _handle_any_exception(e):
    import traceback as _tb
    logger.error(f"Unhandled Flask exception: {e}\n{_tb.format_exc()}")
    from flask import jsonify as _jsonify
    code = getattr(e, 'code', 500)
    if not isinstance(code, int):
        code = 500
    return _jsonify({'success': False, 'error': str(e)}), code

@app.errorhandler(404)
def _handle_404(e):
    from flask import jsonify as _jsonify
    return _jsonify({'success': False, 'error': f'Endpoint not found: {request.path}'}), 404
fetcher = NASALevelFetcher()

# ── Telegram ──────────────────────────────────────────────────────────────────
if not args.no_telegram:
    try:
        tok  = os.getenv('TELEGRAM_TOKEN')
        chat = os.getenv('TELEGRAM_CHAT_ID')
        if not tok and os.path.exists('config/telegram_config.json'):
            with open('config/telegram_config.json', encoding='utf-8') as _f:
                _cfg = json.load(_f)
                tok, chat = _cfg.get('bot_token'), _cfg.get('chat_id')
        if tok and chat:
            if telegram_manager.start(tok, chat, None):
                logger.info("Telegram manager active")
            else:
                logger.warning("Telegram: another instance may be running")
        else:
            logger.warning("Telegram not configured")
    except Exception as _te:
        logger.warning(f"Telegram skipped: {_te}")
else:
    logger.info("Telegram disabled via --no-telegram")


# ══════════════════════════════════════════════════════════════════════════════
# ASSET UNIVERSE
# ══════════════════════════════════════════════════════════════════════════════
ALL_ASSETS = [
    # Commodities
    ('GC=F','commodities',5.0), ('SI=F','commodities',0.2), ('CL=F','commodities',0.5),
    ('NG=F','commodities',0.05),('HG=F','commodities',0.05),
    # Crypto
    ('BTC-USD','crypto',0.02),('ETH-USD','crypto',0.03),('BNB-USD','crypto',0.02),
    ('SOL-USD','crypto',0.04),('XRP-USD','crypto',0.015),('ADA-USD','crypto',0.02),
    ('DOGE-USD','crypto',0.03),('DOT-USD','crypto',0.02),('LTC-USD','crypto',0.015),
    ('AVAX-USD','crypto',0.03),('LINK-USD','crypto',0.02),
    # Forex
    ('EUR/USD','forex',0.001),('GBP/USD','forex',0.001),('USD/JPY','forex',0.1),
    ('AUD/USD','forex',0.001),('USD/CAD','forex',0.001),('NZD/USD','forex',0.001),
    ('USD/CHF','forex',0.001),('EUR/GBP','forex',0.001),('EUR/JPY','forex',0.1),
    ('GBP/JPY','forex',0.1),  ('AUD/JPY','forex',0.05), ('EUR/AUD','forex',0.001),
    ('GBP/AUD','forex',0.001),('AUD/CAD','forex',0.001),('CAD/JPY','forex',0.05),
    ('CHF/JPY','forex',0.05), ('EUR/CAD','forex',0.001),('EUR/CHF','forex',0.001),
    ('GBP/CAD','forex',0.001),('GBP/CHF','forex',0.001),
    # Indices
    ('^GSPC','indices',10),('^DJI','indices',50),('^IXIC','indices',30),
    ('^FTSE','indices',20),('^N225','indices',100),('^HSI','indices',50),
    ('^GDAXI','indices',30),('^VIX','indices',1),
    # Stocks
    ('AAPL','stocks',0.5),('MSFT','stocks',0.5),('GOOGL','stocks',0.5),
    ('AMZN','stocks',0.5),('TSLA','stocks',0.5),('NVDA','stocks',1.0),
    ('META','stocks',0.5),('JPM','stocks',0.5),('V','stocks',0.5),
    ('MA','stocks',0.5), ('JNJ','stocks',0.5),('PFE','stocks',0.5),
    ('WMT','stocks',0.5),('PG','stocks',0.5), ('KO','stocks',0.5),
    ('XOM','stocks',0.5),('CVX','stocks',0.5),
]

_ASSET_MAP = {a: (cat, pip) for a, cat, pip in ALL_ASSETS}  # quick lookup

# ── SSE shared price cache (5-second TTL, avoids hammering APIs per-tab) ────
_sse_price_cache: dict  = {}   # asset -> (price, timestamp)
_sse_price_lock         = threading.Lock()
_SSE_CACHE_TTL          = 5    # seconds — fresher than the 30s main cache

def _get_sse_price(asset: str, category: str) -> float | None:
    """Fetch the latest price for SSE, using a 5s shared cache."""
    import time as _t
    now = _t.time()

    with _sse_price_lock:
        cached = _sse_price_cache.get(asset)
        if cached and (now - cached[1]) < _SSE_CACHE_TTL:
            return cached[0]

    price = None
    try:
        if category in ('forex', 'stocks', 'crypto'):
            price = fetcher.fetch_itick_price(asset, category)
        if not price and category == 'crypto':
            price = fetcher.fetch_coingecko_price(asset)
        if not price:
            # Yahoo fallback — direct call bypasses 30s main cache
            sym_map = {
                'EUR/USD':'EURUSD=X','GBP/USD':'GBPUSD=X','USD/JPY':'JPY=X',
                'AUD/USD':'AUDUSD=X','USD/CAD':'CAD=X','NZD/USD':'NZDUSD=X',
                'USD/CHF':'CHF=X','EUR/GBP':'EURGBP=X','EUR/JPY':'EURJPY=X',
                'GBP/JPY':'GBPJPY=X','AUD/JPY':'AUDJPY=X','EUR/AUD':'EURAUD=X',
                'GBP/AUD':'GBPAUD=X','AUD/CAD':'AUDCAD=X','CAD/JPY':'CADJPY=X',
                'CHF/JPY':'CHFJPY=X','EUR/CAD':'EURCAD=X','EUR/CHF':'EURCHF=X',
                'GBP/CAD':'GBPCAD=X','GBP/CHF':'GBPCHF=X',
            }
            yahoo_sym = sym_map.get(asset, asset)
            price = fetcher.fetch_yahoo_price(yahoo_sym, '1m')
    except Exception as _e:
        logger.debug(f"_get_sse_price {asset}: {_e}")

    if price:
        with _sse_price_lock:
            _sse_price_cache[asset] = (price, _t.time())
    return price



ASSET_ALIASES = {
    'BITCOIN':'BTC-USD','BTC':'BTC-USD','ETHEREUM':'ETH-USD','ETH':'ETH-USD',
    'BINANCE':'BNB-USD','BNB':'BNB-USD','SOLANA':'SOL-USD','SOL':'SOL-USD',
    'XRP':'XRP-USD','RIPPLE':'XRP-USD','GOLD':'GC=F','SILVER':'SI=F',
    'OIL':'CL=F','WTI':'CL=F','SP500':'^GSPC','S&P':'^GSPC','DOW':'^DJI',
    'NASDAQ':'^IXIC','APPLE':'AAPL','MICROSOFT':'MSFT','GOOGLE':'GOOGL',
    'AMAZON':'AMZN','TESLA':'TSLA','NVIDIA':'NVDA','META':'META',
    'EURO':'EUR/USD','POUND':'GBP/USD','YEN':'USD/JPY',
    # Allow /api/signal/XAU-USD style too
    'XAU-USD':'GC=F','XAG-USD':'SI=F',
}

# Per-category background refresh interval (seconds)
_REFRESH = {'crypto':30,'forex':60,'commodities':60,'indices':120,'stocks':120}
# Price-gate: skip re-compute if price moved < this fraction
_PRICE_GATE = 0.001

# ══════════════════════════════════════════════════════════════════════════════
# PHASE 2 — TradingCore injection point
# ══════════════════════════════════════════════════════════════════════════════
# bot.py calls inject_core(engine) before starting Flask.
# After injection:
#   • get_bot() returns engine._engine (the UltimateTradingSystem)
#   • positions_stream reads from engine.state (SystemState) — zero lag
#   • system_status reads balance/perf from engine.state
#   • get_bot() NEVER creates a second UltimateTradingSystem
#
# If running without injection (standalone dev mode), get_bot() falls back
# to lazy-init as before.
# ══════════════════════════════════════════════════════════════════════════════
_CORE     = None   # TradingCore instance — set by inject_core()
_bot      = None
_bot_lock = threading.Lock()


def inject_core(core) -> None:
    """
    Called by bot.py after TradingCore is created.
    Wires the central engine into all dashboard routes.
    After this call get_bot() returns core._engine directly
    and positions/balance are read from core.state (zero-copy, zero-lag).
    """
    global _bot, _CORE
    _CORE = core
    # Pre-populate _bot so get_bot() never creates a duplicate instance
    if core._engine is not None:
        _bot = core._engine
    else:
        # Engine still initialising — wire it once ready
        def _wire_when_ready():
            if core.wait_until_ready(timeout=120):
                global _bot
                _bot = core._engine
                logger.info("[web_app] TradingCore engine wired to dashboard")
            else:
                logger.warning("[web_app] TradingCore did not become ready in 120s")
        threading.Thread(target=_wire_when_ready, name="core-wire", daemon=True).start()
    logger.info("[web_app] inject_core() called — dashboard connected to TradingCore")

# ── Module-level singletons — created ONCE, reused everywhere ────────────────
# WhaleAlertManager: spawns Twitter/Telegram/Reddit threads — never recreate
_whale_mgr      = None
_whale_mgr_lock = threading.Lock()

def get_whale_mgr():
    global _whale_mgr
    if _whale_mgr is not None:
        return _whale_mgr
    with _whale_mgr_lock:
        if _whale_mgr is not None:
            return _whale_mgr
        # Reuse the instance already created inside the trading bot
        # to avoid spawning duplicate Twitter/Telegram/Reddit threads
        try:
            bot = _bot  # read without calling get_bot() to avoid recursion
            if bot is not None and hasattr(bot, 'sentiment_analyzer') and                bot.sentiment_analyzer is not None and                hasattr(bot.sentiment_analyzer, 'whale_manager') and                bot.sentiment_analyzer.whale_manager is not None:
                _whale_mgr = bot.sentiment_analyzer.whale_manager
                logger.info("WhaleAlertManager: reusing bot instance (no duplicate threads)")
                return _whale_mgr
        except Exception:
            pass
        # Fallback: create fresh only if bot not ready yet
        try:
            from whale_alert_manager import WhaleAlertManager
            _whale_mgr = WhaleAlertManager()
        except Exception as _e:
            logger.warning(f"WhaleAlertManager init failed: {_e}")
    return _whale_mgr

# SentimentAnalyzer: initialises 42 news sources — never recreate per-request
_sentiment      = None
_sentiment_lock = threading.Lock()

def get_sentiment():
    global _sentiment
    if _sentiment is not None:
        return _sentiment
    with _sentiment_lock:
        if _sentiment is not None:
            return _sentiment
        # Reuse the instance already created inside the trading bot
        try:
            bot = _bot
            if bot is not None and hasattr(bot, 'sentiment_analyzer') and                bot.sentiment_analyzer is not None:
                _sentiment = bot.sentiment_analyzer
                logger.info("SentimentAnalyzer: reusing bot instance (no duplicate init)")
                return _sentiment
        except Exception:
            pass
        # Fallback: create fresh only if bot not ready yet
        try:
            from sentiment_analyzer import SentimentAnalyzer
            _sentiment = SentimentAnalyzer()
        except Exception as _e:
            logger.warning(f"SentimentAnalyzer init failed: {_e}")
    return _sentiment

def get_bot():
    """
    Thread-safe singleton.
    PHASE 2: If TradingCore was injected via inject_core(), returns its
    engine directly — NEVER creates a second UltimateTradingSystem.
    Falls back to lazy-init only in standalone/dev mode.
    Returns None on failure — never raises.
    """
    global _bot
    if _bot is not None:
        return _bot

    # If TradingCore is injected and engine is ready, use it
    if _CORE is not None:
        if _CORE._engine is not None:
            _bot = _CORE._engine
            return _bot
        # Engine still initialising
        return None

    # Standalone dev mode — lazy init (no TradingCore)
    with _bot_lock:
        if _bot is not None:
            return _bot
        try:
            from trading_system import UltimateTradingSystem
            _bot = UltimateTradingSystem(account_balance=args.balance, no_telegram=True)
            logger.info("[web_app] Trading bot loaded (standalone mode)")
            try:
                from telegram_manager import telegram_manager
                if telegram_manager.is_running and telegram_manager.bot is not None:
                    telegram_manager.bot.trading_system = _bot
            except Exception as _te:
                logger.warning(f"Telegram wiring skipped: {_te}")
            try:
                from signal_learning import signal_cache
                signal_cache.start(_bot)
            except Exception as _ce:
                logger.warning(f"SignalCache start failed: {_ce}")
        except Exception as e:
            logger.error(f"Trading bot init failed: {e}\n{traceback.format_exc()}")
            _bot = None
    return _bot


# ══════════════════════════════════════════════════════════════════════════════
# BACKGROUND SIGNAL REFRESH (tiered per category)
# ══════════════════════════════════════════════════════════════════════════════
_sig_store: Dict[str, Dict] = {}   # asset → signal dict
_sig_lock   = threading.Lock()
_last_ref:  Dict[str, float] = {}  # asset → unix timestamp of last refresh
_price_prev:Dict[str, float] = {}  # asset → last seen price

def _store_signal(asset: str, sig: Dict):
    with _sig_lock:
        _sig_store[asset] = sig

def _get_cached_signal(asset: str) -> Optional[Dict]:
    with _sig_lock:
        return _sig_store.get(asset)

def _should_refresh(asset: str, category: str) -> bool:
    interval = _REFRESH.get(category, 60)
    return (time.time() - _last_ref.get(asset, 0)) >= interval

def _bg_refresh_worker():
    """Background thread: refreshes signals for all assets on their own schedule."""
    # Eagerly init bot FIRST — then singletons reuse its internal instances
    # This prevents duplicate WhaleAlertManager/SentimentAnalyzer/Reddit threads
    try:
        get_bot()           # creates UltimateTradingSystem (includes SentimentAnalyzer + WhaleAlertManager)
        get_sentiment()     # picks up bot.sentiment_analyzer — no new instance
        get_whale_mgr()     # picks up bot.sentiment_analyzer.whale_manager — no new instance
    except Exception as _init_e:
        logger.warning(f"Eager init warning: {_init_e}")
    while True:
        try:
            bot = get_bot()
            if bot is None:
                time.sleep(30); continue

            status = MarketHours.get_status()
            assets = [(a,c,p) for a,c,p in ALL_ASSETS if c=='crypto' or not status.get('is_weekend',False)]
            refreshed = 0

            for asset, category, _ in assets:
                if not _should_refresh(asset, category):
                    continue
                if not MarketHours.get_status().get(category, True):
                    _store_signal(asset, _closed_sig(asset, category))
                    _last_ref[asset] = time.time()
                    continue
                try:
                    sig = _fetch_signal(asset, category, bot)
                    if sig:
                        _store_signal(asset, sig)
                        refreshed += 1
                except Exception as e:
                    logger.debug(f"BG refresh {asset}: {e}")
                finally:
                    _last_ref[asset] = time.time()

            if refreshed:
                logger.info(f"BG refresh: {refreshed} signals updated")
        except Exception as e:
            logger.error(f"BG refresh worker error: {e}")
        time.sleep(10)   # check every 10s; actual refresh governed by _REFRESH


def _closed_sig(asset: str, category: str) -> Dict:
    return {'asset':asset,'category':category,'signal':'CLOSED','confidence':0,
            'entry_price':0,'stop_loss':0,'take_profit_levels':[],'risk_pct':0,
            'timestamp':datetime.now().isoformat(),'generated_at':datetime.now().strftime('%H:%M:%S'),
            'reason':'Market Closed','market_open':False,'data_source':'N/A',
            'time_remaining':5.0,'expires_at':(datetime.now()+timedelta(minutes=5)).isoformat()}


def _fetch_signal(asset: str, category: str, bot) -> Optional[Dict]:
    """Build one signal via the quality engine (with price-gate check)."""
    try:
        price, source = fetcher.get_real_time_price(asset, category)
        if not price or price <= 0:
            return None
        prev = _price_prev.get(asset, 0)
        if prev and abs(price - prev) / prev < _PRICE_GATE:
            cached = _get_cached_signal(asset)
            if cached:
                return cached  # price barely moved — reuse existing signal
        _price_prev[asset] = price

        # Use the quality signal engine (multi-TF, ATR stops, confluence)
        try:
            from signal_learning import get_instant_signal
            sig = get_instant_signal(asset, category, bot)
            if sig and sig.get('direction') not in (None,):
                _dir = sig.get('direction', 'HOLD')
                sig.update({'category':category,'market_open':True,'data_source':source,
                            'timestamp':datetime.now().isoformat(),
                            'generated_at':datetime.now().strftime('%H:%M:%S'),
                            'expires_at':(datetime.now()+timedelta(hours=4)).isoformat(),
                            'time_remaining':240.0,
                            'take_profit_levels': _tp_levels(sig),
                            'signal': _dir})  # dashboard filters on 'signal' key
                return sig
        except Exception as _qe:
            logger.debug(f"Quality signal engine: {_qe} — falling back to voting engine")

        # Fallback: original voting-engine path
        from indicators.technical import TechnicalIndicators
        df = bot.fetch_historical_data(asset, days=5, interval='15m')
        if df is None or df.empty:
            return None
        df  = TechnicalIndicators.add_all_indicators(df)
        if not hasattr(bot, 'voting_engine'):
            return None
        combined = bot.voting_engine.weighted_vote(bot.voting_engine.get_all_signals(df))
        if not combined or combined.get('signal') == 'HOLD':
            return None
        atr = float(df['atr'].iloc[-1]) if 'atr' in df.columns else price * 0.01
        d   = combined['signal']
        sl  = price-(atr*1.5) if d=='BUY' else price+(atr*1.5)
        tp1 = price+(atr*2)   if d=='BUY' else price-(atr*2)
        tp2 = price+(atr*3)   if d=='BUY' else price-(atr*3)
        tp3 = price+(atr*4)   if d=='BUY' else price-(atr*4)
        return {'asset':asset,'category':category,'signal':d,'direction':d,
                'confidence':round(combined.get('confidence',0.7),2),
                'entry_price':round(price,5),'stop_loss':round(sl,5),
                'take_profit':round(tp1,5),
                'take_profit_levels':[{'level':1,'price':round(tp1,5)},
                                       {'level':2,'price':round(tp2,5)},
                                       {'level':3,'price':round(tp3,5)}],
                'risk_pct':round(abs(price-sl)/price*100,2),
                'timestamp':datetime.now().isoformat(),
                'generated_at':datetime.now().strftime('%H:%M:%S'),
                'expires_at':(datetime.now()+timedelta(hours=4)).isoformat(),
                'time_remaining':240.0,'reason':combined.get('reason','Voting engine signal'),
                'market_open':True,'data_source':source,'strategy':'VOTING'}
    except Exception as e:
        logger.error(f"_fetch_signal {asset}: {e}")
        return None


def _tp_levels(sig: Dict) -> List[Dict]:
    """Convert quality signal tp fields into dashboard-compatible take_profit_levels."""
    levels = []
    for i, key in enumerate(['take_profit','take_profit_2','take_profit_3'], 1):
        v = sig.get(key)
        if v:
            levels.append({'level': i, 'price': round(float(v), 6)})
    return levels


# ══════════════════════════════════════════════════════════════════════════════
# HUMAN RESPONSE GENERATOR
# ══════════════════════════════════════════════════════════════════════════════
def generate_human_response(asset: str, df, prediction: Dict, news: List, whale: str = None) -> Dict:
    """Always returns a dict — never raises, never returns None."""
    direction     = prediction.get('direction', 'HOLD')
    confidence    = prediction.get('confidence', 0.5)
    current_price = float(df['close'].iloc[-1]) if df is not None and not df.empty else 0

    _COMM  = any(k in asset for k in ('XAU','XAG','GC=','SI=','CL=','WTI'))
    _CRYPT = any(k in asset for k in ('-USD','BTC','ETH','SOL','BNB','XRP'))
    sl_pct, tp_pct = (0.015,0.025) if _COMM else (0.005,0.015) if _CRYPT else (0.003,0.008)

    sl = current_price*(1-sl_pct) if direction=='UP' else current_price*(1+sl_pct)
    tp = current_price*(1+tp_pct) if direction=='UP' else current_price*(1-tp_pct)

    reasons, context, mood, emoji = [], '', 'neutral', '😐'
    try:
        from human_explainer_db import DatabaseExplainer
        bot = get_bot()
        if bot:
            exp = DatabaseExplainer(bot)
            reasons = [r.replace('**','') for r in exp._get_technical_reasons(df, prediction)][:5]
            setup   = 'breakout' if any('breakout' in r.lower() for r in reasons) else 'pullback'
            context = exp.personality.get_historical_context(asset, setup)
            m       = exp.personality.current_mood
            mood, emoji = m.get('name','neutral'), m.get('emoji','😐')
    except Exception:
        try:
            if df is not None:
                if 'rsi' in df.columns:
                    r = float(df['rsi'].iloc[-1])
                    reasons.append(f'RSI {"oversold" if r<30 else "overbought" if r>70 else "neutral"} at {r:.1f}')
                if 'macd' in df.columns and 'macd_signal' in df.columns:
                    reasons.append('MACD bullish cross' if float(df['macd'].iloc[-1])>float(df['macd_signal'].iloc[-1]) else 'MACD bearish')
                if 'sma_20' in df.columns and 'sma_50' in df.columns:
                    reasons.append('Above 20/50 SMA (uptrend)' if float(df['sma_20'].iloc[-1])>float(df['sma_50'].iloc[-1]) else 'Below 20/50 SMA (downtrend)')
        except Exception as _ie:
            logger.debug(f"Indicator fallback: {_ie}")
        if not reasons:
            reasons = ['Technical analysis in progress']

    return {'direction':direction,'confidence':confidence,'current_price':current_price,
            'predicted_price':prediction.get('predicted_price'),'stop_loss':round(sl,5),
            'take_profit':round(tp,5),'reasons':reasons,
            'news':[{'title':a.get('title','')} for a in (news or [])[:3]],
            'whale_alerts':whale,'historical_context':context,'mood':mood,
            'mood_emoji':emoji,'timestamp':datetime.now().isoformat()}


# ══════════════════════════════════════════════════════════════════════════════
# ROUTES
# ══════════════════════════════════════════════════════════════════════════════

@app.route('/')
def index():
    return render_template('index_live.html')

@app.route('/status')
def status_page():
    return render_template('status_dashboard.html')

@app.route('/sentiment')
def sentiment_dashboard():
    return render_template('sentiment_dashboard.html')

@app.route('/backtest')
def backtest_page():
    return render_template('backtest_visualizer.html')

@app.route('/websocket-feed')
def websocket_feed_page():
    return render_template('websocket_feed.html')


# ── /api/signals/live ─────────────────────────────────────────────────────────
@app.route('/api/signals/live')
def get_live_signals():
    try:
        now     = datetime.now()
        filt    = request.args.get('filter','all')
        signals = list(_sig_store.values())

        # update time_remaining
        for s in signals:
            try:
                age = (now - datetime.fromisoformat(s.get('timestamp', now.isoformat()))).total_seconds() / 60
                s['time_remaining'] = max(0.0, 240.0 - age)
            except Exception:
                s['time_remaining'] = 240.0

        # apply filter
        if filt == 'buy':
            signals = [s for s in signals if s.get('signal') == 'BUY']
        elif filt == 'sell':
            signals = [s for s in signals if s.get('signal') == 'SELL']
        elif filt == 'high-confidence':
            signals = [s for s in signals if s.get('confidence', 0) >= 0.7]

        open_sigs  = [s for s in signals if s.get('market_open') and s.get('signal') not in ('HOLD','CLOSED')]
        buys       = sum(1 for s in open_sigs if s.get('signal') == 'BUY')
        sells      = sum(1 for s in open_sigs if s.get('signal') == 'SELL')
        avg_conf   = sum(s.get('confidence',0) for s in open_sigs) / max(1, len(open_sigs))
        signals.sort(key=lambda x: (-(x.get('confidence',0)) if x.get('market_open') else -999))

        return jsonify({'success':True,'signals':signals,'total_signals':len(open_sigs),
                        'buy_signals':buys,'sell_signals':sells,
                        'avg_confidence':round(avg_conf*100,1),
                        'market_status':MarketHours.get_status(),
                        'last_update':now.strftime('%H:%M:%S'),
                        'is_updating':False})
    except Exception as e:
        logger.error(f"get_live_signals: {e}")
        return jsonify({'success':False,'error':str(e)}), 500


# ── /api/signal/<asset>  — INSTANT via cache ──────────────────────────────────
@app.route('/api/signal/<path:asset>')
def get_signal(asset: str):
    try:
        asset = ASSET_ALIASES.get(asset.upper().strip(), asset.upper().strip())
        bot   = get_bot()
        if bot is None:
            return jsonify({'success':False,'error':'Trading system not ready — check logs'}), 503

        category, _ = _ASSET_MAP.get(asset, ('stocks', 0.5))

        # Quality signal engine (uses pre-warmed cache → instant)
        try:
            from signal_learning import get_instant_signal, signal_engine
            sig = get_instant_signal(asset, category, bot)
            if sig:
                sig.update({'category':category,'market_open':True,
                            'take_profit_levels':_tp_levels(sig),
                            'signal': sig.get('direction', 'HOLD')})
                return jsonify({'success':True,'signal':sig,'human_response':sig,
                                'win_rate':signal_engine.get_win_rate(asset)})
        except Exception as _qe:
            logger.warning(f"Quality engine for {asset}: {_qe}")

        # Fallback to original path if quality engine errors
        from indicators.technical import TechnicalIndicators
        df = None
        for iv in ('15m','1h','1d'):
            df = bot.fetch_historical_data(asset, days=5, interval=iv)
            if df is not None and not df.empty:
                break
        if df is None or df.empty:
            return jsonify({'success':False,'error':f'No data for {asset}'})

        df         = TechnicalIndicators.add_all_indicators(df)
        prediction = bot.predictor.predict_next(df)

        news = []
        try:
            if hasattr(bot,'sentiment_analyzer') and hasattr(bot.sentiment_analyzer,'news_integrator'):
                news = bot.sentiment_analyzer.news_integrator.fetch_by_symbol(asset, limit=3)
        except Exception as _ne:
            logger.debug(f"News fetch {asset}: {_ne}")

        whale = None
        if any(k in asset for k in ('-USD','XAU','XAG','GC=','SI=','CL=','BTC','ETH','BNB','SOL','XRP')):
            try:
                alerts = (get_whale_mgr() or {}) and get_whale_mgr().get_alerts(min_value_usd=1_000_000) if get_whale_mgr() else []
                base   = asset.replace('-USD','').replace('/USD','').replace('=F','').replace('=X','')
                for a in alerts[:5]:
                    sym = a.get('symbol','')
                    if sym and (sym in base or base in sym):
                        whale = f"{a.get('amount',0)} {sym} (${a['value_usd']/1e6:.1f}M) moved"
                        break
            except Exception as _we:
                logger.debug(f"Whale alert {asset}: {_we}")

        price  = float(df['close'].iloc[-1])
        sr     = {'direction':prediction.get('direction','HOLD'),'confidence':prediction.get('confidence',0.5),
                  'current_price':price,'predicted_price':prediction.get('predicted_price'),
                  'stop_loss':price*0.995,'take_profit':price*1.01}
        hr     = generate_human_response(asset, df, prediction, news, whale)

        # Record for learning
        try:
            from signal_learning import signal_engine
            snap = {c: round(float(df[c].iloc[-1]),6) for c in
                    ['rsi','macd','macd_signal','sma_20','sma_50','bb_upper','bb_lower','atr']
                    if c in df.columns}
            sid = signal_engine.record({
                'asset':asset,'direction':sr['direction'],'confidence':sr['confidence'],
                'entry_price':price,'stop_loss':sr['stop_loss'],'take_profit':sr['take_profit'],
                'reasons':hr.get('reasons',[]),'indicators':snap,
                'news_titles':[n.get('title','') for n in news[:3]],'whale_alert':whale,
            })
            wr = signal_engine.get_win_rate(asset)
            sr['signal_id'], sr['win_rate'] = sid, wr
            hr['signal_id'], hr['win_rate'] = sid, wr
        except Exception as _le:
            logger.debug(f"Signal record {asset}: {_le}")

        return jsonify({'success':True,'signal':sr,'human_response':hr})

    except Exception as e:
        logger.error(f"get_signal {asset}: {e}\n{traceback.format_exc()}")
        return jsonify({'success':False,'error':str(e)}), 500


# ── /api/signal/history ───────────────────────────────────────────────────────
@app.route('/api/signal/history')
def signal_history():
    try:
        from signal_learning import signal_engine
        asset = request.args.get('asset')
        limit = int(request.args.get('limit', 20))
        return jsonify({'success':True,'signals':signal_engine.get_history(asset, limit)})
    except Exception as e:
        logger.error(f"signal_history: {e}")
        return jsonify({'success':False,'error':str(e)}), 500


# ── /api/position-audit ───────────────────────────────────────────────────────
@app.route('/api/position-audit')
def position_audit():
    try:
        bot = get_bot()
        if not bot:
            return jsonify({'error':'Trading system not ready','healthy':False})
        if not hasattr(bot,'paper_trader'):
            return jsonify({'error':'Paper trader not initialised','healthy':False})
        return jsonify(bot.paper_trader.audit_position_health())
    except Exception as e:
        logger.error(f"position_audit: {e}")
        return jsonify({'error':str(e),'healthy':False})


# ── /api/positions/stream  — Server-Sent Events for real-time dashboard ───────
@app.route('/api/positions/stream')
def positions_stream():
    """
    Server-Sent Events endpoint.
    Dashboard connects once; receives live position updates every 5s.

    PHASE 2: When TradingCore is injected, reads directly from core.state
    (SystemState) — zero lag, zero file polling, always authoritative.
    Falls back to state_bridge file, then in-process bot for dev mode.
    """
    def _enrich_position(p_dict):
        """Add current price and live P&L to a position dict."""
        try:
            asset    = p_dict.get('asset', '')
            category = p_dict.get('category', _ASSET_MAP.get(asset, ('stocks', 0))[0])
            cur_price, _ = fetcher.get_real_time_price(asset, category)
            entry     = float(p_dict.get('entry_price', 0))
            size      = float(p_dict.get('position_size', p_dict.get('size', 1)))
            direction = p_dict.get('signal', p_dict.get('direction', 'BUY'))
            if cur_price and entry:
                pnl_pct = ((cur_price - entry) / entry * 100) if direction == 'BUY' else ((entry - cur_price) / entry * 100)
                pnl_usd = pnl_pct / 100 * size
            else:
                pnl_pct = pnl_usd = 0
            return {
                **p_dict,
                'current_price': cur_price,
                'pnl_pct':       round(pnl_pct, 3),
                'pnl_usd':       round(pnl_usd, 2),
                'updated_at':    datetime.now().isoformat(),
            }
        except Exception:
            return p_dict

    def _event_gen():
        while True:
            try:
                positions = []

                # ── Primary: TradingCore.state — zero lag, always current ────
                if _CORE is not None:
                    try:
                        raw = _CORE.state.get_open_positions()
                        positions = [_enrich_position(p) for p in raw]
                    except Exception as _ce:
                        logger.debug(f"TradingCore positions read error: {_ce}")

                # ── Secondary: state_bridge file (legacy cross-process) ──────
                if not positions:
                    try:
                        from state_bridge import read_trading_state
                        state = read_trading_state()
                        if state and state.get('open_positions'):
                            positions = [_enrich_position(p) for p in state['open_positions']]
                    except Exception:
                        pass

                # ── Fallback: in-process bot singleton (dev mode) ─────────────
                if not positions:
                    bot = get_bot()
                    if bot and hasattr(bot, 'paper_trader') and bot.paper_trader:
                        positions = [_enrich_position(p) for p in bot.paper_trader.get_open_positions()]

                payload = json.dumps({'positions': positions, 'count': len(positions),
                                       'ts': datetime.now().isoformat()}, cls=_Encoder)
                yield f"data: {payload}\n\n"
            except Exception as e:
                logger.error(f"positions_stream: {e}")
                yield f"data: {json.dumps({'error': str(e)})}\n\n"
            time.sleep(5)

    return Response(stream_with_context(_event_gen()),
                    mimetype='text/event-stream',
                    headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'})


# ── /api/stress-test ──────────────────────────────────────────────────────────
@app.route('/api/stress-test', methods=['GET','POST'])
def stress_test():
    """
    Simulate historical crash scenarios against current or supplied positions.
    GET  — uses open paper-trade positions
    POST — accepts JSON body: {"positions":[{"asset":"AAPL","category":"stocks",
                                              "direction":"BUY","size_usd":1000}],
                                "balance":10000}
    """
    try:
        from signal_learning import run_stress_test

        if request.method == 'POST':
            body      = request.get_json(force=True) or {}
            positions = body.get('positions', [])
            balance   = float(body.get('balance', args.balance))
        else:
            # Pull from TradingCore.state (Phase 2) or paper_trader (fallback)
            positions = []
            balance   = args.balance
            if _CORE is not None:
                for p in _CORE.state.get_open_positions():
                    asset  = p.get('asset', '')
                    cat, _ = _ASSET_MAP.get(asset, ('stocks', 0))
                    positions.append({
                        'asset':     asset,
                        'category':  p.get('category', cat),
                        'direction': p.get('signal', p.get('direction', 'BUY')),
                        'size_usd':  float(p.get('position_size', p.get('size', 0))),
                    })
                balance = _CORE.state.balance
            else:
                bot = get_bot()
                # FIX BUG 9: use get_open_positions() — .positions attribute doesn't exist
                if bot and hasattr(bot, 'paper_trader') and bot.paper_trader:
                    for p in bot.paper_trader.get_open_positions():
                        asset  = p.get('asset', '')
                        cat, _ = _ASSET_MAP.get(asset, ('stocks', 0))
                        positions.append({
                            'asset':     asset,
                            'category':  cat,
                            'direction': p.get('signal', p.get('direction', 'BUY')),
                            'size_usd':  float(p.get('position_size', p.get('size', 0))),
                        })
                    balance = float(getattr(bot.paper_trader, 'balance', args.balance))

        result = run_stress_test(positions, balance)
        return jsonify({'success':True, **result})
    except Exception as e:
        logger.error(f"stress_test: {e}")
        return jsonify({'success':False,'error':str(e)}), 500


# ── /api/walk-forward/<asset> ─────────────────────────────────────────────────
@app.route('/api/walk-forward/<path:asset>')
def walk_forward(asset: str):
    """
    Walk-forward ML optimisation for one asset.
    Splits 90 days into 3 × 30-day windows, trains on first 20 days,
    tests on last 10 days of each window, returns per-window performance.
    Heavy — runs in ~30s. Do not call from the main signal loop.
    """
    try:
        import numpy as np, pandas as pd
        from indicators.technical import TechnicalIndicators

        asset = ASSET_ALIASES.get(asset.upper().strip(), asset.upper().strip())
        bot   = get_bot()
        if not bot:
            return jsonify({'success':False,'error':'Bot not ready'}), 503

        df = bot.fetch_historical_data(asset, days=90, interval='1d')
        if df is None or len(df) < 60:
            return jsonify({'success':False,'error':'Not enough history (need 60+ days)'}), 422
        df = TechnicalIndicators.add_all_indicators(df)

        WINDOWS  = 3
        WIN_SIZE = len(df) // WINDOWS
        results  = []
        best_model = None
        best_sharpe = -999

        for w in range(WINDOWS):
            start = w * WIN_SIZE
            end   = start + WIN_SIZE
            train = df.iloc[start : end - WIN_SIZE//3]
            test  = df.iloc[end - WIN_SIZE//3 : end]
            if len(train) < 20 or len(test) < 5:
                continue
            try:
                bot.predictor.train(train, target_periods=5)
                preds = []
                for i in range(len(test)):
                    p = bot.predictor.predict_next(test.iloc[:i+1] if i > 0 else train.iloc[-5:])
                    preds.append({'direction':p.get('direction','HOLD'),'confidence':p.get('confidence',0.5)})

                # Simulate signals on test window
                rets = []
                for i, pred in enumerate(preds[:-1]):
                    if pred['direction'] in ('UP','DOWN') and pred['confidence'] > 0.55:
                        ret = float(test['close'].iloc[i+1] - test['close'].iloc[i]) / float(test['close'].iloc[i])
                        if pred['direction'] == 'DOWN': ret = -ret
                        rets.append(ret)

                if rets:
                    arr    = np.array(rets)
                    sharpe = float(arr.mean() / (arr.std() + 1e-8) * np.sqrt(252))
                    win_r  = float((arr > 0).mean())
                    total  = float(arr.sum() * 100)
                else:
                    sharpe = win_r = total = 0.0

                window_result = {
                    'window':w+1,
                    'train_start':str(train.index[0]),
                    'train_end':str(train.index[-1]),
                    'test_start':str(test.index[0]),
                    'test_end':str(test.index[-1]),
                    'signals':len(rets),
                    'win_rate':round(win_r,3),
                    'total_return_pct':round(total,2),
                    'sharpe':round(sharpe,3),
                }
                results.append(window_result)

                if sharpe > best_sharpe:
                    best_sharpe = sharpe
                    best_model  = w + 1

            except Exception as _we:
                logger.warning(f"Walk-forward window {w}: {_we}")
                results.append({'window':w+1,'error':str(_we)})

        if not results:
            return jsonify({'success':False,'error':'All windows failed'})

        valid   = [r for r in results if 'sharpe' in r]
        avg_sh  = round(sum(r['sharpe'] for r in valid) / max(1, len(valid)), 3)
        avg_wr  = round(sum(r['win_rate'] for r in valid) / max(1, len(valid)), 3)

        return jsonify({'success':True,'asset':asset,'windows':results,
                        'summary':{'avg_sharpe':avg_sh,'avg_win_rate':avg_wr,'best_window':best_model,
                                   'verdict':'ROBUST' if avg_sh>0.5 else 'MARGINAL' if avg_sh>0 else 'POOR'}})
    except Exception as e:
        logger.error(f"walk_forward {asset}: {e}")
        return jsonify({'success':False,'error':str(e)}), 500


# ── /api/system-status ────────────────────────────────────────────────────────
@app.route('/api/system-status')
def system_status():
    try:
        open_p = closed_p = total_pnl = today_pnl = 0

        # ── PHASE 2: read from TradingCore.state — always authoritative ──────
        if _CORE is not None:
            perf    = _CORE.state.get_performance()
            balance = perf.get('balance', args.balance)
            open_p  = perf.get('open_positions', 0)
            closed_p= perf.get('total_trades', 0)
            today_pnl = _CORE.state.daily_pnl
            total_pnl = perf.get('total_pnl', 0)
            return jsonify({
                'success':          True,
                'balance':          round(balance, 2),
                'pnl':              round(today_pnl, 2),
                'total_pnl':        round(total_pnl, 2),
                'open_positions':   open_p,
                'closed_positions': closed_p,
                'daily_trades':     _CORE.state.daily_trades,
                'win_rate':         perf.get('win_rate', 0),
                'processes':        {'Trading Bot': _CORE.is_running, 'Web Dashboard': True},
                'engine_ready':     _CORE.is_ready,
                'timestamp':        datetime.now().isoformat(),
            })

        # ── Fallback: legacy path (standalone / dev mode) ─────────────────────
        balance = args.balance
        bot = get_bot()
        if bot and hasattr(bot, 'risk_manager') and bot.risk_manager:
            balance = bot.risk_manager.account_balance
        elif bot and hasattr(bot, 'paper_trader') and bot.paper_trader:
            perf = bot.paper_trader.get_performance()
            balance = perf.get('current_balance', args.balance)

        try:
            from services.database_service import DatabaseService
            db     = DatabaseService()
            trades = db.get_recent_trades(100) if db.use_db else []
            for t in trades:
                if not t.get('exit_time'):
                    open_p += 1
                else:
                    closed_p += 1
                    total_pnl += t.get('pnl', 0)
                    try:
                        if datetime.fromisoformat(t['exit_time']).date() == datetime.now().date():
                            today_pnl += t.get('pnl', 0)
                    except Exception:
                        pass
        except Exception as _dbe:
            logger.debug(f"DB status query: {_dbe}")
            if bot and hasattr(bot, 'paper_trader') and bot.paper_trader:
                perf = bot.paper_trader.get_performance()
                open_p    = perf.get('open_positions', 0)
                closed_p  = perf.get('total_trades', 0)
                today_pnl = perf.get('total_pnl', 0)

        return jsonify({'success': True, 'balance': round(balance, 2), 'pnl': round(today_pnl, 2),
                        'open_positions': open_p, 'closed_positions': closed_p,
                        'processes': {'Trading Bot': _bot is not None, 'Web Dashboard': True},
                        'timestamp': datetime.now().isoformat()})
    except Exception as e:
        logger.error(f"system_status: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/settings/update', methods=['POST'])
def update_settings():
    try:
        data = request.get_json(force=True) or {}
        if 'balance' in data: args.balance = float(data['balance'])
        return jsonify({'success':True})
    except Exception as e:
        return jsonify({'success':False,'error':str(e)}), 400

@app.route('/api/status')
def api_status():
    engine_ready = _CORE.is_ready if _CORE else (_bot is not None)
    return jsonify({'market_status': MarketHours.get_status(),
                    'assets_cached': len(_sig_store),
                    'bot_ready':     engine_ready,
                    'architecture':  'single-process' if _CORE else 'standalone'})


# ── /api/backtest/run ─────────────────────────────────────────────────────────
@app.route('/api/backtest/run')
def api_backtest_run():
    import numpy as np, pandas as pd
    from dataclasses import asdict

    asset    = request.args.get('asset','BTC-USD')
    strategy = request.args.get('strategy','rsi')
    period   = request.args.get('period','90d')
    days     = {'30d':30,'90d':90,'180d':180,'365d':365,'730d':730}.get(period,90)

    def _clean(obj):
        if isinstance(obj,dict):  return {k:_clean(v) for k,v in obj.items()}
        if isinstance(obj,list):  return [_clean(v) for v in obj]
        try:
            import numpy as _np
            if isinstance(obj,_np.integer): return int(obj)
            if isinstance(obj,_np.floating): return float(obj)
            if isinstance(obj,_np.ndarray): return obj.tolist()
        except Exception: pass
        if hasattr(obj,'isoformat'): return obj.isoformat()
        return obj

    try:
        bot = get_bot()
        if not bot:
            return jsonify({'success':False,'error':'Bot not ready'}), 503
        df = bot.fetch_historical_data(asset, days=days, interval='1d')
        if df is None or df.empty:
            return jsonify({'success':False,'error':f'No data for {asset}'}), 404
        from indicators.technical import TechnicalIndicators
        df = TechnicalIndicators.add_all_indicators(df)

        _ALIAS = {'bb':'bollinger','bollinger':'bollinger','rsi':'rsi','macd':'macd',
                  'ma_cross':'ma_cross','ma':'ma_cross','ml':'ml_ensemble',
                  'ultimate':'ultimate_indicator','breakout':'breakout',
                  'mean_reversion':'mean_reversion','trend':'trend_following',
                  'trend_following':'trend_following','scalping':'scalping',
                  'day_trading':'day_trading','news':'news_sentiment'}
        res = _ALIAS.get(strategy, strategy)
        fn  = (getattr(bot.strategy_engine, f'{res}_strategy', None)
               if hasattr(bot,'strategy_engine') and bot.strategy_engine else None)
        fn  = fn or getattr(bot, f'{res}_strategy', None)
        if fn is None:
            return jsonify({'success':False,'error':f'Unknown strategy: {strategy}'}), 400

        all_sigs = []
        for i in range(60, len(df)):
            try:
                for s in (fn(df.iloc[:i+1]) or []):
                    if 'date' not in s: s['date'] = df.index[i]
                    if 'entry_price' in s and 'entry' not in s: s['entry'] = s['entry_price']
                    if 'take_profit_levels' in s and 'take_profit' not in s:
                        tl = s['take_profit_levels']
                        s['take_profit'] = tl[0]['price'] if tl else s.get('entry',0)*1.02
                    all_sigs.append(s)
            except Exception: pass

        sdf = pd.DataFrame(all_sigs) if all_sigs else pd.DataFrame()
        for c in ['date','signal','entry','stop_loss','take_profit','confidence']:
            if c not in sdf.columns: sdf[c] = None
        if not sdf.empty: sdf = sdf.dropna(subset=['date','signal','entry'])

        bt = bot.backtester; bt.trades = []; bt.equity_curve = [bt.initial_capital]
        res_obj = bt.run_backtest(df, sdf)
        if res_obj is None:
            return jsonify({'success':False,'error':'Backtest returned nothing'}), 500
        rd = asdict(res_obj) if hasattr(res_obj,'__dataclass_fields__') else dict(res_obj)
        rd['trades'] = rd.pop('total_trades',0); rd['total_return'] = rd.pop('total_return_pct',0)
        rd['max_dd']  = rd.pop('max_drawdown',0)

        eq   = bt.equity_curve
        dts  = ['Start'] + [str(t.entry_date) for t in bt.trades]
        eq_a = np.array(eq); rm = np.maximum.accumulate(eq_a)
        dd   = ((eq_a-rm)/(rm+1e-10)*100).tolist()

        mr   = {'months':[],'returns':[]}
        if bt.trades:
            tdf = pd.DataFrame([asdict(t) for t in bt.trades])
            tdf['exit_date'] = pd.to_datetime(tdf['exit_date'],errors='coerce')
            tdf = tdf.dropna(subset=['exit_date'])
            if not tdf.empty:
                tdf['month'] = tdf['exit_date'].dt.to_period('M')
                mon = tdf.groupby('month')['pnl'].sum()
                mr  = {'months':[str(m) for m in mon.index],'returns':mon.values.tolist()}

        trades_out = [{'entry_date':str(asdict(t).get('entry_date','')),'exit_date':str(asdict(t).get('exit_date','')),
                        'direction':asdict(t).get('direction',''),'entry_price':asdict(t).get('entry_price',0),
                        'exit_price':asdict(t).get('exit_price',0),'pnl':asdict(t).get('pnl',0),
                        'return_pct':asdict(t).get('return_pct',0),'exit_reason':asdict(t).get('exit_reason','')}
                       for t in bt.trades[-50:]]

        return jsonify(_clean({'success':True,'results':rd,
                                'equity_curve':{'dates':dts[:len(eq)],'values':eq},
                                'drawdown':{'dates':dts[:len(dd)],'values':dd},
                                'monthly_returns':mr,'trades':trades_out}))
    except Exception as e:
        logger.error(f"backtest {asset}: {e}")
        return jsonify({'success':False,'error':str(e),'trace':traceback.format_exc()}), 500


# ── /api/sentiment/dashboard ──────────────────────────────────────────────────
@app.route('/api/sentiment/dashboard')
def api_sentiment_dashboard():
    try:
        analyzer = get_sentiment()
        if analyzer is None:
            return jsonify({'success': False, 'error': 'SentimentAnalyzer unavailable'}), 503
        result   = {'success':True,'overall_sentiment':'Neutral','score':0,
                    'fear_greed':{'value':50,'classification':'Neutral','score':0},
                    'vix':{'value':20,'classification':'Normal','score':0},
                    'article_count':0,'sentiment_distribution':{'bullish':0,'neutral':0,'bearish':0},
                    'sources':{},'articles':[],'whale_alerts':[]}
        ms = analyzer.get_comprehensive_sentiment('general')
        if ms: result.update({'overall_sentiment':ms.get('interpretation','Neutral'),'score':ms.get('score',0)})
        fg = analyzer.fetch_fear_greed_index()
        if fg: result['fear_greed'] = {'value':fg.get('value',50),'classification':fg.get('classification','Neutral'),'score':fg.get('score',0)}
        vix = analyzer.fetch_vix()
        if vix: result['vix'] = {'value':vix.get('value',20),'classification':vix.get('classification','Normal'),'score':vix.get('score',0)}
        if hasattr(analyzer,'news_integrator'):
            arts = analyzer.news_integrator.fetch_all_sources()
            result['articles'] = sorted(arts, key=lambda x:x.get('date',''), reverse=True)[:20]
            result['article_count'] = len(result['articles'])
        result['whale_alerts'] = analyzer.fetch_whale_alerts(min_value_usd=1_000_000)[:10]
        if result['articles']:
            b = sum(1 for a in result['articles'] if a.get('sentiment',0)>0.1)
            be= sum(1 for a in result['articles'] if a.get('sentiment',0)<-0.1)
            result['sentiment_distribution'] = {'bullish':b,'neutral':len(result['articles'])-b-be,'bearish':be}
        return jsonify(result)
    except Exception as e:
        logger.error(f"sentiment_dashboard: {e}")
        return jsonify({'success':False,'error':str(e)}), 500


@app.route('/api/market/events')
def api_market_events():
    try:
        from sentiment_analyzer import SentimentAnalyzer
        analyzer = get_sentiment()
        return jsonify({'success':True,'events':analyzer.get_market_events() if analyzer else []})
    except Exception as e:
        logger.error(f"market_events: {e}")
        return jsonify({'success':False,'error':str(e)}), 500

@app.route('/api/websocket/feed')
def get_websocket_feed():
    try:
        from websocket_dashboard import get_feed, connection_status
        src = request.args.get('source','all')
        txs = get_feed(source_filter=src, limit=200)
        return jsonify({'success':True,'transactions':txs,'count':len(txs),'connection_status':connection_status})
    except Exception as e:
        logger.error(f"websocket_feed: {e}")
        return jsonify({'success':False,'error':str(e)}), 500

# ══════════════════════════════════════════════════════════════════════════════
# LIVE CHART API  — feeds chart_live.html
# ══════════════════════════════════════════════════════════════════════════════

@app.route('/api/chart/stream')
def chart_stream():
    """
    Server-Sent Events stream for the live chart.
    Emits:
      • 'tick'      every ~2s  — latest price for the requested asset
      • 'positions' every ~5s  — open positions + recent history + balance
    Client keeps the connection open; chart updates the current candle live.
    """
    import time as _t

    asset    = request.args.get('asset', 'EUR/USD')
    category = _ASSET_MAP.get(asset, ('forex', 0.001))[0]

    def generate():
        last_pos_push = 0.0
        try:
            while True:
                now   = _t.time()
                price = _get_sse_price(asset, category)

                if price:
                    tick_payload = json.dumps({
                        'type':  'tick',
                        'asset': asset,
                        'price': price,
                        'time':  int(now),
                    })
                    yield f"data: {tick_payload}\n\n"

                # Push position update every 5 seconds
                if now - last_pos_push >= 5:
                    try:
                        bot = _bot   # read global directly — avoids recursive lock
                        open_pos = []
                        history  = []
                        balance  = None

                        if bot and hasattr(bot, 'paper_trader') and bot.paper_trader:
                            raw_open = bot.paper_trader.get_open_positions()
                            for p in raw_open:
                                unreal = None
                                try:
                                    if price and p.get('asset') == asset and p.get('entry_price'):
                                        diff = price - p['entry_price']
                                        if p.get('signal') == 'SELL':
                                            diff = -diff
                                        unreal = round(diff * float(p.get('position_size', 0)), 4)
                                except Exception:
                                    pass
                                p['unrealized_pnl'] = unreal
                                open_pos.append(p)
                            history = bot.paper_trader.get_trade_history(limit=50)

                        if bot and hasattr(bot, 'risk_manager') and bot.risk_manager:
                            balance = bot.risk_manager.account_balance

                        pos_payload = json.dumps({
                            'type':    'positions',
                            'open':    open_pos,
                            'history': history,
                            'balance': balance,
                        }, default=str)
                        yield f"data: {pos_payload}\n\n"
                        last_pos_push = now
                    except Exception as _pe:
                        logger.debug(f"SSE positions error: {_pe}")

                _t.sleep(2)

        except GeneratorExit:
            pass   # client disconnected — clean exit
        except Exception as _se:
            logger.debug(f"SSE stream error for {asset}: {_se}")

    return Response(
        stream_with_context(generate()),
        mimetype='text/event-stream',
        headers={
            'Cache-Control':    'no-cache',
            'X-Accel-Buffering':'no',       # disable nginx buffering if behind proxy
            'Connection':       'keep-alive',
        },
    )


@app.route('/chart')
def chart_page():
    """Serve the MT5-style live chart page."""
    return render_template('chart_live.html')


@app.route('/api/chart/assets')
def chart_assets():
    """Return full asset list grouped by category for the chart dropdown."""
    try:
        assets = [{'symbol': a, 'category': cat} for a, cat, _ in ALL_ASSETS]
        return jsonify({'success': True, 'assets': assets})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/chart/candles')
def chart_candles():
    """
    Return OHLCV candles for a given asset and interval.
    Used by chart_live.html to draw the candlestick chart.
    """
    try:
        asset    = request.args.get('asset', 'EUR/USD')
        interval = request.args.get('interval', '1h')
        bot      = get_bot()
        if not bot:
            return jsonify({'success': False, 'error': 'Bot not ready'}), 503

        days_map = {'1m': 1, '5m': 5, '15m': 7, '1h': 30, '4h': 90, '1d': 365}
        days     = days_map.get(interval, 30)

        df = bot.fetch_historical_data(asset, days=days, interval=interval)
        if df is None or df.empty:
            return jsonify({'success': False, 'error': f'No data for {asset}'}), 404

        # Normalise column names
        df.columns = [c.lower() for c in df.columns]

        candles = []
        for ts, row in df.iterrows():
            try:
                # Convert index to unix timestamp
                if hasattr(ts, 'timestamp'):
                    t = int(ts.timestamp())
                else:
                    import pandas as pd
                    t = int(pd.Timestamp(ts).timestamp())

                candles.append({
                    'time':   t,
                    'open':   float(row.get('open',  row.get('close', 0))),
                    'high':   float(row.get('high',  row.get('close', 0))),
                    'low':    float(row.get('low',   row.get('close', 0))),
                    'close':  float(row.get('close', 0)),
                    'volume': float(row.get('volume', 0)),
                })
            except Exception:
                continue

        # Lightweight-charts requires strictly ascending time with no duplicates
        seen  = set()
        clean = []
        for c in sorted(candles, key=lambda x: x['time']):
            if c['time'] not in seen:
                seen.add(c['time'])
                clean.append(c)

        return jsonify({'success': True, 'candles': clean, 'count': len(clean)})

    except Exception as e:
        logger.error(f"chart_candles: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/chart/positions')
def chart_positions():
    """
    Return open positions + recent trade history + current balance.
    Also computes unrealized P&L for each open position using the latest price.
    """
    try:
        bot = get_bot()
        if not bot:
            return jsonify({'success': False, 'error': 'Bot not ready'}), 503

        # ── Open positions ──────────────────────────────────────────────────
        open_pos = []
        if hasattr(bot, 'paper_trader') and bot.paper_trader:
            raw_open = bot.paper_trader.get_open_positions()
            for p in raw_open:
                # Try to get current price for unrealized P&L
                unrealized = None
                try:
                    cur = fetcher.fetch_current_price(p['asset'])
                    if cur and p.get('entry_price'):
                        diff = cur - p['entry_price']
                        if p.get('signal') == 'SELL':
                            diff = -diff
                        unrealized = round(diff * float(p.get('position_size', 0)), 4)
                except Exception:
                    pass
                p['unrealized_pnl'] = unrealized
                open_pos.append(p)

        # ── Trade history ───────────────────────────────────────────────────
        history = []
        if hasattr(bot, 'paper_trader') and bot.paper_trader:
            history = bot.paper_trader.get_trade_history(limit=50)

        # ── Balance ─────────────────────────────────────────────────────────
        balance = None
        if hasattr(bot, 'risk_manager') and bot.risk_manager:
            balance = bot.risk_manager.account_balance

        return jsonify({
            'success': True,
            'open':    open_pos,
            'history': history,
            'balance': balance,
        })

    except Exception as e:
        logger.error(f"chart_positions: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/refresh/manual', methods=['POST'])
def manual_refresh():
    _last_ref.clear()  # force all assets to refresh next cycle
    return jsonify({'success':True,'message':'Refresh queued'})


# ── /api/tests  — run unit tests, return results as JSON ─────────────────────
@app.route('/api/tests')
def run_tests():
    """
    Runs all unit tests and returns pass/fail JSON.
    GET /api/tests
    """
    import io, contextlib
    buf = io.StringIO()
    try:
        from signal_learning import _run_tests
        with contextlib.redirect_stdout(buf):
            success = _run_tests()
        return jsonify({'success':success,'output':buf.getvalue()})
    except Exception as e:
        logger.error(f"run_tests: {e}")
        return jsonify({'success':False,'error':str(e),'output':buf.getvalue()}), 500


# ── /api/install  — returns ready-to-run install script ──────────────────────
@app.route('/api/install')
def install_script():
    """Returns the install.bat (Windows) or install.sh (Linux) setup script."""
    script = r"""@echo off
REM ── Forex Bot Auto-Installer ──────────────────────────────────────────────
REM  Run from the root of the forex_prediction_bot folder
REM  Requirements: Python 3.11+, PostgreSQL 14+ (optional but recommended)
REM ──────────────────────────────────────────────────────────────────────────

echo [1/6] Creating virtual environment...
python -m venv venv_tf
call venv_tf\Scripts\activate.bat

echo [2/6] Upgrading pip...
python -m pip install --upgrade pip

echo [3/6] Installing core dependencies...
pip install flask flask-cors pandas numpy scikit-learn yfinance sqlalchemy psycopg2-binary python-dotenv requests

echo [4/6] Installing trading-specific packages...
pip install ta-lib-binary pandas-ta websockets python-telegram-bot praw finnhub-python twelvedata

echo [5/6] Installing ML packages...
pip install xgboost lightgbm optuna

echo [6/6] Creating .env template if missing...
if not exist .env (
    echo TELEGRAM_TOKEN=your_token_here > .env
    echo TELEGRAM_CHAT_ID=your_chat_id_here >> .env
    echo TWELVEDATA_KEY=your_key_here >> .env
    echo FINNHUB_KEY=your_key_here >> .env
    echo DATABASE_URL=postgresql://postgres:password@localhost:5432/trading_bot >> .env
    echo REDDIT_CLIENT_ID=your_client_id >> .env
    echo REDDIT_CLIENT_SECRET=your_secret >> .env
    echo Created .env — fill in your API keys before starting
)

echo.
echo  Install complete!
echo  Start with:  python web_app_live.py --balance 1000
echo  Dashboard:   http://localhost:5000
echo  Unit tests:  http://localhost:5000/api/tests
echo  Stress test: http://localhost:5000/api/stress-test
"""
    return Response(script, mimetype='text/plain',
                    headers={'Content-Disposition':'attachment; filename=install.bat'})


# ══════════════════════════════════════════════════════════════════════════════
# PLATFORM UPGRADE ROUTES — OrderFlow, Alpha, Accuracy, Prediction Overlay
# ══════════════════════════════════════════════════════════════════════════════

@app.route('/accuracy')
def accuracy_page():
    return render_template('accuracy_dashboard.html')


@app.route('/api/orderflow/<path:asset>')
def api_orderflow(asset: str):
    """
    GET /api/orderflow/BTC-USD
    Returns latest order flow snapshot for the asset.
    {bid_vol, ask_vol, delta, imbalance, pressure, bid_walls, ask_walls}
    """
    asset = ASSET_ALIASES.get(asset.upper(), asset)
    if _orderflow_engine:
        snap = _orderflow_engine.get_snapshot(asset)
        if snap:
            return jsonify({'success': True, 'data': snap})
    return jsonify({'success': False, 'error': 'No orderflow data yet', 'data': {
        'asset': asset, 'pressure': 'NEUTRAL', 'imbalance': 0,
        'bid_vol': 0, 'ask_vol': 0, 'delta': 0,
    }})


@app.route('/api/orderflow')
def api_orderflow_all():
    """GET /api/orderflow — returns all available orderflow snapshots."""
    if _orderflow_engine:
        snaps = _orderflow_engine.get_all_snapshots()
        return jsonify({'success': True, 'data': snaps, 'count': len(snaps)})
    return jsonify({'success': False, 'data': {}, 'count': 0})


@app.route('/api/alpha')
def api_alpha_signals():
    """
    GET /api/alpha?n=50
    Returns recent alpha discovery signals.
    """
    n = min(int(request.args.get('n', 50)), 200)
    if _alpha_engine:
        sigs = _alpha_engine.get_recent_signals(n)
        return jsonify({'success': True, 'signals': sigs, 'count': len(sigs)})
    return jsonify({'success': False, 'signals': [], 'count': 0})


@app.route('/api/alpha/<path:asset>')
def api_alpha_for_asset(asset: str):
    """GET /api/alpha/EUR/USD — alpha signals for a specific asset."""
    asset = ASSET_ALIASES.get(asset.upper(), asset)
    if _alpha_engine:
        sigs = _alpha_engine.get_signals_for_asset(asset, 20)
        return jsonify({'success': True, 'asset': asset, 'signals': sigs})
    return jsonify({'success': False, 'asset': asset, 'signals': []})


@app.route('/api/accuracy')
def api_accuracy():
    """
    GET /api/accuracy?days=30
    Returns AI prediction accuracy stats by horizon (1H, 4H, 24H).
    """
    days = min(int(request.args.get('days', 30)), 90)
    if _pred_tracker:
        stats = _pred_tracker.get_accuracy_stats(days)
        return jsonify({'success': True, 'data': stats})
    return jsonify({'success': False, 'data': {
        'by_horizon': {
            '1H':  {'total':0,'correct':0,'accuracy_pct':0},
            '4H':  {'total':0,'correct':0,'accuracy_pct':0},
            '24H': {'total':0,'correct':0,'accuracy_pct':0},
        },
        'by_asset': {}, 'recent': [], 'days_back': days,
    }})


@app.route('/api/prediction-overlay/<path:asset>')
def api_prediction_overlay(asset: str):
    """
    GET /api/prediction-overlay/EUR/USD
    Returns the AI prediction overlay data for the live chart.
    {direction, target_price, confidence, horizon_minutes, entry_price}
    Combines the latest cached signal with prediction tracker data.
    """
    asset = ASSET_ALIASES.get(asset.upper(), asset)
    cat   = _ASSET_MAP.get(asset, ('forex', 0.001))[0]

    # Pull from signal cache
    cached = _signal_cache.get(asset, {})
    signal = cached.get('signal') if isinstance(cached, dict) else None

    overlay = None
    if signal and isinstance(signal, dict):
        direction  = signal.get('signal', 'HOLD')
        entry      = signal.get('entry_price', signal.get('entry', 0))
        tp1        = signal.get('tp1', signal.get('take_profit', 0))
        confidence = signal.get('confidence', 0.5)

        if direction != 'HOLD' and entry:
            overlay = {
                'direction':       direction,
                'entry_price':     entry,
                'target_price':    tp1,
                'stop_loss':       signal.get('stop_loss', 0),
                'confidence':      confidence,
                'horizon_minutes': 60,
                'asset':           asset,
                'strategy':        signal.get('strategy', ''),
                'regime':          signal.get('regime', ''),
            }

    # Augment with alpha signals if available
    alpha_sigs = []
    if _alpha_engine:
        alpha_sigs = _alpha_engine.get_signals_for_asset(asset, 3)

    # Augment with orderflow
    of_snap = None
    if _orderflow_engine:
        of_snap = _orderflow_engine.get_snapshot(asset)

    return jsonify({
        'success': True,
        'asset':       asset,
        'overlay':     overlay,
        'alpha':       alpha_sigs,
        'orderflow':   of_snap,
    })


@app.route('/api/redis/status')
def api_redis_status():
    """GET /api/redis/status — check Redis and gateway connectivity."""
    if _redis_broker:
        return jsonify({
            'success':   True,
            'connected': _redis_broker.is_connected,
            'channels':  _redis_broker.CHANNELS,
            'gateway':   'ws://localhost:8080',
        })
    return jsonify({'success': False, 'connected': False,
                    'message': 'redis-py not installed or Redis not running'})


# ══════════════════════════════════════════════════════════════════════════════
# STARTUP
# ══════════════════════════════════════════════════════════════════════════════
if __name__ == '__main__':
    logger.info("="*60)
    logger.info("  ULTIMATE TRADING DASHBOARD  —  starting")
    logger.info(f"  Balance: ${args.balance}  |  Assets: {len(ALL_ASSETS)}")
    logger.info("  Dashboard  : http://localhost:5000")
    logger.info("  Signals    : http://localhost:5000/api/signal/GOLD")
    logger.info("  Stress Test: http://localhost:5000/api/stress-test")
    logger.info("  Walk-Fwd   : http://localhost:5000/api/walk-forward/BTC-USD")
    logger.info("  Unit Tests : http://localhost:5000/api/tests")
    logger.info("  Install    : http://localhost:5000/api/install")
    logger.info("="*60)

    # Background signal refresh
    threading.Thread(target=_bg_refresh_worker, name='BgRefresh', daemon=True).start()

    # WebSocket price feeds (Bybit + Finnhub + TwelveData)
    def _start_ws():
        try:
            from websocket_manager import WebSocketManager
            from websocket_dashboard import add_transaction
            def _cb(source, symbol, price, volume, side, ts):
                add_transaction(source, symbol, price, volume, side)
            ws = WebSocketManager()
            ws.start()
            ws.subscribe_bybit(['BTCUSDT','ETHUSDT','BNBUSDT','SOLUSDT','XRPUSDT'], _cb)
            ws.subscribe_finnhub(['AAPL','MSFT','GOOGL','TSLA','NVDA','AMZN'], _cb)
            ws.subscribe_twelvedata(['EUR/USD','XAU/USD'], _cb)   # free tier: 2 symbols max
            logger.info("WebSocket manager running")
        except Exception as e:
            logger.warning(f"WebSocket start failed: {e}")

    threading.Thread(target=_start_ws, name='WsManager', daemon=True).start()

    app.run(debug=False, host='0.0.0.0', port=5000, threaded=True, use_reloader=False)