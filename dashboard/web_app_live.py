# type: ignore
"""
dashboard/web_app_live.py — Professional Trading Intelligence Platform
All routes for both legacy pages and the new 8-dashboard professional system.
Wired exclusively to TradingCore — no old system references.
"""

from flask import Flask, render_template, jsonify, request, Response, stream_with_context
from flask_cors import CORS
from datetime import datetime, timedelta
import threading
import time
import sys
import json
import os
import argparse
import traceback
from typing import Dict, List, Optional, Any
from collections import deque
from pandas import Period, Timestamp
from flask import redirect

from utils.logger import logger
from telegram_manager import telegram_manager

try:
    from websocket_dashboard import recent_transactions
except Exception:
    recent_transactions = []

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


# ── JSON encoder ──────────────────────────────────────────────────────────────
class _Encoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (Period, Timestamp)): return str(obj)
        if hasattr(obj, 'isoformat'):            return obj.isoformat()
        try:
            import numpy as np
            if isinstance(obj, np.integer):  return int(obj)
            if isinstance(obj, np.floating): return float(obj)
            if isinstance(obj, np.ndarray):  return obj.tolist()
        except ImportError:
            pass
        return super().default(obj)


# ── CLI args ──────────────────────────────────────────────────────────────────
_parser = argparse.ArgumentParser(description='Trading Dashboard')
_parser.add_argument('--balance',     type=float, default=30)
_parser.add_argument('--no-telegram', action='store_true')
args, _ = _parser.parse_known_args()

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from data.fetcher import DataFetcher

app = Flask(__name__)
app.json_encoder = _Encoder
CORS(app)


@app.errorhandler(Exception)
def _handle_any_exception(e):
    logger.error(f"Unhandled Flask exception: {e}\n{traceback.format_exc()}")
    from flask import jsonify as _jsonify
    code = getattr(e, 'code', 500)
    if not isinstance(code, int): code = 500
    return _jsonify({'success': False, 'error': str(e)}), code


@app.errorhandler(404)
def _handle_404(e):
    from flask import jsonify as _jsonify
    return _jsonify({'success': False, 'error': f'Endpoint not found: {request.path}'}), 404


fetcher = DataFetcher()


class MarketHours:
    @staticmethod
    def get_status() -> Dict[str, Any]:
        utc_h = datetime.utcnow().hour
        dow   = datetime.utcnow().weekday()
        is_weekend = dow >= 5
        return {
            'crypto':      True,
            'forex':       not is_weekend and (utc_h < 21 or utc_h >= 22),
            'stocks':      not is_weekend and 13 <= utc_h < 21,
            'indices':     not is_weekend and 13 <= utc_h < 21,
            'commodities': not is_weekend and 7 <= utc_h < 21,
            'is_weekend':  is_weekend,
            'ny_time':     datetime.utcnow().strftime('%H:%M UTC'),
        }


if not args.no_telegram:
    try:
        tok  = os.getenv('COMMAND_BOT_TOKEN') or os.getenv('TELEGRAM_TOKEN')
        chat = os.getenv('COMMAND_BOT_CHAT_ID') or os.getenv('TELEGRAM_CHAT_ID')
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
    ('GC=F','commodities',5.0), ('SI=F','commodities',0.2), ('CL=F','commodities',0.5),
    ('NG=F','commodities',0.05),('HG=F','commodities',0.05),
    ('BTC-USD','crypto',0.02),('ETH-USD','crypto',0.03),('BNB-USD','crypto',0.02),
    ('SOL-USD','crypto',0.04),('XRP-USD','crypto',0.015),('ADA-USD','crypto',0.02),
    ('DOGE-USD','crypto',0.03),('DOT-USD','crypto',0.02),('LTC-USD','crypto',0.015),
    ('AVAX-USD','crypto',0.03),('LINK-USD','crypto',0.02),
    ('EUR/USD','forex',0.001),('GBP/USD','forex',0.001),('USD/JPY','forex',0.1),
    ('AUD/USD','forex',0.001),('USD/CAD','forex',0.001),('NZD/USD','forex',0.001),
    ('USD/CHF','forex',0.001),('EUR/GBP','forex',0.001),('EUR/JPY','forex',0.1),
    ('GBP/JPY','forex',0.1),  ('AUD/JPY','forex',0.05), ('EUR/AUD','forex',0.001),
    ('GBP/AUD','forex',0.001),('AUD/CAD','forex',0.001),('CAD/JPY','forex',0.05),
    ('CHF/JPY','forex',0.05), ('EUR/CAD','forex',0.001),('EUR/CHF','forex',0.001),
    ('GBP/CAD','forex',0.001),('GBP/CHF','forex',0.001),
    ('^GSPC','indices',10),('^DJI','indices',50),('^IXIC','indices',30),
    ('^FTSE','indices',20),('^N225','indices',100),('^HSI','indices',50),
    ('^GDAXI','indices',30),('^VIX','indices',1),
    ('AAPL','stocks',0.5),('MSFT','stocks',0.5),('GOOGL','stocks',0.5),
    ('AMZN','stocks',0.5),('TSLA','stocks',0.5),('NVDA','stocks',1.0),
    ('META','stocks',0.5),('JPM','stocks',0.5),('V','stocks',0.5),
    ('MA','stocks',0.5), ('JNJ','stocks',0.5),('PFE','stocks',0.5),
    ('WMT','stocks',0.5),('PG','stocks',0.5), ('KO','stocks',0.5),
    ('XOM','stocks',0.5),('CVX','stocks',0.5),
]

_ASSET_MAP = {a: (cat, pip) for a, cat, pip in ALL_ASSETS}

_sse_price_cache: dict = {}
_sse_price_lock        = threading.Lock()
_SSE_CACHE_TTL         = 5


def _get_sse_price(asset: str, category: str) -> Optional[float]:
    import time as _t
    now = _t.time()
    with _sse_price_lock:
        cached = _sse_price_cache.get(asset)
        if cached and (now - cached[1]) < _SSE_CACHE_TTL:
            return cached[0]
    price = None
    try:
        price, _ = fetcher.get_real_time_price(asset, category)
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
    'XAU-USD':'GC=F','XAG-USD':'SI=F',
}

_REFRESH    = {'crypto':30,'forex':60,'commodities':60,'indices':120,'stocks':120}
_PRICE_GATE = 0.001

# ══════════════════════════════════════════════════════════════════════════════
# TRADINGCORE INJECTION
# ══════════════════════════════════════════════════════════════════════════════
_CORE     = None
_bot      = None
_bot_lock = threading.Lock()


def inject_core(core) -> None:
    global _bot, _CORE
    _CORE = core
    _bot  = core
    logger.info("[web_app] inject_core() called — TradingCore wired to dashboard")


_whale_mgr      = None
_whale_mgr_lock = threading.Lock()

def get_whale_mgr():
    global _whale_mgr
    if _whale_mgr is not None:
        return _whale_mgr
    with _whale_mgr_lock:
        if _whale_mgr is not None:
            return _whale_mgr
        try:
            from whale_alert_manager import WhaleAlertManager
            _whale_mgr = WhaleAlertManager()
        except Exception as _e:
            logger.warning(f"WhaleAlertManager unavailable: {_e}")
    return _whale_mgr


_sentiment      = None
_sentiment_lock = threading.Lock()

def get_sentiment():
    global _sentiment
    if _sentiment is not None:
        return _sentiment
    with _sentiment_lock:
        if _sentiment is not None:
            return _sentiment
        try:
            from sentiment_analyzer import SentimentAnalyzer
            _sentiment = SentimentAnalyzer()
        except Exception as _e:
            logger.warning(f"SentimentAnalyzer unavailable: {_e}")
    return _sentiment


def get_bot():
    global _bot
    if _bot is not None:
        return _bot
    if _CORE is not None:
        _bot = _CORE
        return _bot
    return None


def _fetch_ohlcv(asset: str, category: str, days: int = 5,
                 interval: str = '1d') -> Optional[Any]:
    period_map = {'1m':1,'5m':5,'15m':7,'1h':30,'4h':90,'1d':days,'60m':30}
    periods    = period_map.get(interval, days)
    try:
        return fetcher.get_ohlcv(asset, category, interval=interval, periods=periods)
    except Exception as _e:
        logger.debug(f"_fetch_ohlcv {asset}: {_e}")
        return None


# ══════════════════════════════════════════════════════════════════════════════
# BACKGROUND SIGNAL REFRESH
# ══════════════════════════════════════════════════════════════════════════════
_sig_store: Dict[str, Dict] = {}
_sig_lock   = threading.Lock()
_last_ref:  Dict[str, float] = {}
_price_prev:Dict[str, float] = {}
_signal_cache: Dict[str, Dict] = {}


def _store_signal(asset: str, sig: Dict):
    with _sig_lock:
        _sig_store[asset] = sig
        _signal_cache[asset] = {'signal': sig}


def _get_cached_signal(asset: str) -> Optional[Dict]:
    with _sig_lock:
        return _sig_store.get(asset)


def _should_refresh(asset: str, category: str) -> bool:
    interval = _REFRESH.get(category, 60)
    return (time.time() - _last_ref.get(asset, 0)) >= interval


def _bg_refresh_worker():
    try:
        get_sentiment()
        get_whale_mgr()
    except Exception as _init_e:
        logger.warning(f"Eager init warning: {_init_e}")

    while True:
        try:
            status = MarketHours.get_status()
            assets = [(a, c, p) for a, c, p in ALL_ASSETS
                      if c == 'crypto' or not status.get('is_weekend', False)]
            refreshed = 0
            for asset, category, _ in assets:
                if not _should_refresh(asset, category):
                    continue
                if not MarketHours.get_status().get(category, True):
                    _store_signal(asset, _closed_sig(asset, category))
                    _last_ref[asset] = time.time()
                    continue
                try:
                    sig = _fetch_signal(asset, category)
                    if sig:
                        _store_signal(asset, sig)
                        refreshed += 1
                except Exception as _e:
                    logger.debug(f"BG refresh {asset}: {_e}")
                finally:
                    _last_ref[asset] = time.time()
            if refreshed:
                logger.info(f"BG refresh: {refreshed} signals updated")
        except Exception as _e:
            logger.error(f"BG refresh worker error: {_e}")
        time.sleep(10)


def _closed_sig(asset: str, category: str) -> Dict:
    return {
        'asset': asset, 'category': category, 'signal': 'CLOSED',
        'confidence': 0, 'entry_price': 0, 'stop_loss': 0,
        'take_profit_levels': [], 'risk_pct': 0,
        'timestamp': datetime.now().isoformat(),
        'generated_at': datetime.now().strftime('%H:%M:%S'),
        'reason': 'Market Closed', 'market_open': False, 'data_source': 'N/A',
        'time_remaining': 5.0,
        'expires_at': (datetime.now() + timedelta(minutes=5)).isoformat(),
    }


def _fetch_signal(asset: str, category: str) -> Optional[Dict]:
    try:
        price, _ = fetcher.get_real_time_price(asset, category)
        if not price or price <= 0:
            return None
        prev = _price_prev.get(asset, 0)
        if prev and abs(price - prev) / prev < _PRICE_GATE:
            cached = _get_cached_signal(asset)
            if cached:
                return cached
        _price_prev[asset] = price

        core = get_bot()
        if core is not None and hasattr(core, 'get_signal_for_asset'):
            try:
                sig = core.get_signal_for_asset(asset)
                if sig and sig.get('direction', 'HOLD') != 'HOLD':
                    d = sig.get('direction', 'HOLD')
                    tp_levels = [{'level': i + 1, 'price': round(float(lv), 6)}
                                 for i, lv in enumerate(sig.get('take_profit_levels', [])[:3])]
                    if not tp_levels and sig.get('take_profit'):
                        tp_levels = [{'level': 1, 'price': round(float(sig['take_profit']), 6)}]
                    return {
                        'asset': asset, 'category': category,
                        'signal': d, 'direction': d,
                        'confidence': round(float(sig.get('confidence', 0.5)), 3),
                        'entry_price': round(float(sig.get('entry_price', price)), 6),
                        'stop_loss':   round(float(sig.get('stop_loss', 0)), 6),
                        'take_profit': round(float(sig.get('take_profit', 0)), 6),
                        'take_profit_levels': tp_levels,
                        'risk_reward': sig.get('risk_reward', 0),
                        'strategy_id': sig.get('strategy_id', ''),
                        'risk_pct': round(abs(price - float(sig.get('stop_loss', price))) / price * 100, 2),
                        'market_open': True, 'data_source': 'TradingCore',
                        'timestamp': datetime.now().isoformat(),
                        'generated_at': datetime.now().strftime('%H:%M:%S'),
                        'expires_at': (datetime.now() + timedelta(hours=4)).isoformat(),
                        'time_remaining': 240.0,
                    }
            except Exception as _ce:
                logger.debug(f"TradingCore signal for {asset}: {_ce}")

        df = _fetch_ohlcv(asset, category, days=5, interval='15m')
        if df is None or df.empty:
            return None
        try:
            from indicators.technical import TechnicalIndicators
            df = TechnicalIndicators.add_all_indicators(df)
        except Exception:
            pass
        atr = float(df['atr'].iloc[-1]) if 'atr' in df.columns else price * 0.01
        rsi = float(df['rsi'].iloc[-1]) if 'rsi' in df.columns else 50.0
        if rsi < 35:   d = 'BUY'
        elif rsi > 65: d = 'SELL'
        else:          return None
        sl  = price - (atr * 1.5) if d == 'BUY' else price + (atr * 1.5)
        tp1 = price + (atr * 2)   if d == 'BUY' else price - (atr * 2)
        tp2 = price + (atr * 3)   if d == 'BUY' else price - (atr * 3)
        tp3 = price + (atr * 4)   if d == 'BUY' else price - (atr * 4)
        return {
            'asset': asset, 'category': category, 'signal': d, 'direction': d,
            'confidence': round(0.60 + abs(rsi - 50) / 100, 3),
            'entry_price': round(price, 6), 'stop_loss': round(sl, 6),
            'take_profit': round(tp1, 6),
            'take_profit_levels': [
                {'level': 1, 'price': round(tp1, 6)},
                {'level': 2, 'price': round(tp2, 6)},
                {'level': 3, 'price': round(tp3, 6)},
            ],
            'risk_pct': round(abs(price - sl) / price * 100, 2),
            'market_open': True, 'data_source': 'Indicators',
            'timestamp': datetime.now().isoformat(),
            'generated_at': datetime.now().strftime('%H:%M:%S'),
            'expires_at': (datetime.now() + timedelta(hours=4)).isoformat(),
            'time_remaining': 240.0,
        }
    except Exception as _e:
        logger.error(f"_fetch_signal {asset}: {_e}")
        return None


def _tp_levels(sig: Dict) -> List[Dict]:
    levels = []
    for i, key in enumerate(['take_profit', 'take_profit_2', 'take_profit_3'], 1):
        v = sig.get(key)
        if v:
            levels.append({'level': i, 'price': round(float(v), 6)})
    return levels


def generate_human_response(asset: str, df, prediction: Dict,
                             news: List, whale: str = None) -> Dict:
    direction     = prediction.get('direction', 'HOLD')
    confidence    = prediction.get('confidence', 0.5)
    current_price = float(df['close'].iloc[-1]) if df is not None and not df.empty else 0
    _COMM  = any(k in asset for k in ('XAU','XAG','GC=','SI=','CL=','WTI'))
    _CRYPT = any(k in asset for k in ('-USD','BTC','ETH','SOL','BNB','XRP'))
    sl_pct, tp_pct = (0.015, 0.025) if _COMM else (0.005, 0.015) if _CRYPT else (0.003, 0.008)
    sl = current_price * (1 - sl_pct) if direction == 'UP' else current_price * (1 + sl_pct)
    tp = current_price * (1 + tp_pct) if direction == 'UP' else current_price * (1 - tp_pct)
    reasons = []
    try:
        if df is not None:
            if 'rsi' in df.columns:
                r = float(df['rsi'].iloc[-1])
                reasons.append(f'RSI {"oversold" if r < 30 else "overbought" if r > 70 else "neutral"} at {r:.1f}')
            if 'macd' in df.columns and 'macd_signal' in df.columns:
                reasons.append('MACD bullish cross' if float(df['macd'].iloc[-1]) > float(df['macd_signal'].iloc[-1]) else 'MACD bearish')
            if 'sma_20' in df.columns and 'sma_50' in df.columns:
                reasons.append('Above 20/50 SMA (uptrend)' if float(df['sma_20'].iloc[-1]) > float(df['sma_50'].iloc[-1]) else 'Below 20/50 SMA (downtrend)')
    except Exception: pass
    if not reasons:
        reasons = ['Technical analysis in progress']
    return {
        'direction': direction, 'confidence': confidence,
        'current_price': current_price,
        'predicted_price': prediction.get('predicted_price'),
        'stop_loss': round(sl, 5), 'take_profit': round(tp, 5),
        'reasons': reasons,
        'news': [{'title': a.get('title', '')} for a in (news or [])[:3]],
        'whale_alerts': whale,
        'timestamp': datetime.now().isoformat(),
    }

# ══════════════════════════════════════════════════════════════════════════════
# PROFESSIONAL DASHBOARD PAGE ROUTES
# ══════════════════════════════════════════════════════════════════════════════
@app.route('/')
def index():
    return redirect('/command-center')

@app.route('/command-center')
def command_center():
    return render_template('command_center.html')

@app.route('/market-intelligence')
def market_intelligence():
    return render_template('market_intelligence.html')

@app.route('/ai-predictions')
def ai_predictions():
    return render_template('ai_predictions.html')

@app.route('/whale-intelligence')
def whale_intelligence():
    return render_template('whale_intelligence.html')

@app.route('/sentiment-intelligence')
def sentiment_intelligence():
    return render_template('sentiment_intelligence.html')

@app.route('/risk-dashboard')
def risk_dashboard():
    return render_template('risk_dashboard.html')

@app.route('/strategy-lab')
def strategy_lab():
    return render_template('strategy_lab.html')

@app.route('/system-monitor')
def system_monitor():
    return render_template('system_monitor.html')


# ══════════════════════════════════════════════════════════════════════════════
# EXISTING API ROUTES
# ══════════════════════════════════════════════════════════════════════════════

@app.route('/api/signals/live')
def get_live_signals():
    try:
        now    = datetime.now()
        filt   = request.args.get('filter', 'all')
        status = MarketHours.get_status()
        core   = get_bot()
        if core is not None:
            positions = core.get_positions()
            signals   = []
            for p in positions:
                direction = (p.get('direction') or p.get('signal', 'BUY')).upper()
                if filt == 'buy'             and direction != 'BUY':  continue
                if filt == 'sell'            and direction != 'SELL': continue
                conf = float(p.get('confidence', 0))
                if filt == 'high-confidence' and conf < 0.70:         continue
                signals.append({
                    'asset':              p.get('asset', ''),
                    'signal':             direction,
                    'category':           p.get('category', ''),
                    'confidence':         conf,
                    'entry_price':        float(p.get('entry_price', 0)),
                    'stop_loss':          float(p.get('stop_loss', 0)),
                    'take_profit':        float(p.get('take_profit', 0)),
                    'take_profit_levels': p.get('take_profit_levels', []),
                    'position_size':      float(p.get('position_size', 0)),
                    'strategy_id':        p.get('strategy_id', ''),
                    'pnl':                float(p.get('pnl', 0)),
                    'market_open':        True,
                    'time_remaining':     240.0,
                    'generated_at':       str(p.get('open_time', ''))[:16],
                    'timestamp':          p.get('open_time', now.isoformat()),
                    'metadata':           p.get('metadata', {}),
                    'layer_reached':      p.get('layer_reached', 0),
                })
            buys     = sum(1 for s in signals if s['signal'] == 'BUY')
            sells    = sum(1 for s in signals if s['signal'] == 'SELL')
            avg_conf = sum(s['confidence'] for s in signals) / max(1, len(signals))
            return jsonify({
                'success': True, 'signals': signals,
                'total_signals': len(signals),
                'buy_signals': buys, 'sell_signals': sells,
                'avg_confidence': round(avg_conf * 100, 1),
                'market_status': status,
                'last_update': now.strftime('%H:%M:%S'), 'is_updating': False,
            })

        signals = list(_sig_store.values())
        for s in signals:
            try:
                age = (now - datetime.fromisoformat(s.get('timestamp', now.isoformat()))).total_seconds() / 60
                s['time_remaining'] = max(0.0, 240.0 - age)
            except Exception:
                s['time_remaining'] = 240.0
        if filt == 'buy':             signals = [s for s in signals if s.get('signal') == 'BUY']
        elif filt == 'sell':          signals = [s for s in signals if s.get('signal') == 'SELL']
        elif filt == 'high-confidence': signals = [s for s in signals if s.get('confidence', 0) >= 0.7]
        open_sigs = [s for s in signals if s.get('market_open') and s.get('signal') not in ('HOLD', 'CLOSED')]
        buys      = sum(1 for s in open_sigs if s.get('signal') == 'BUY')
        sells     = sum(1 for s in open_sigs if s.get('signal') == 'SELL')
        avg_conf  = sum(s.get('confidence', 0) for s in open_sigs) / max(1, len(open_sigs))
        signals.sort(key=lambda x: (-(x.get('confidence', 0)) if x.get('market_open') else -999))
        return jsonify({
            'success': True, 'signals': signals, 'total_signals': len(open_sigs),
            'buy_signals': buys, 'sell_signals': sells,
            'avg_confidence': round(avg_conf * 100, 1),
            'market_status': status,
            'last_update': now.strftime('%H:%M:%S'), 'is_updating': False,
        })
    except Exception as _e:
        logger.error(f"get_live_signals: {_e}")
        return jsonify({'success': False, 'error': str(_e)}), 500


@app.route('/api/signal/<path:asset>')
def get_signal(asset: str):
    try:
        asset    = ASSET_ALIASES.get(asset.upper().strip(), asset.upper().strip())
        category, _ = _ASSET_MAP.get(asset, ('stocks', 0.5))
        core     = get_bot()
        if core is not None and hasattr(core, 'get_signal_for_asset'):
            try:
                sig = core.get_signal_for_asset(asset)
                if sig and sig.get('direction', 'HOLD') != 'HOLD':
                    d  = sig['direction']
                    tp = sig.get('take_profit_levels', [])
                    if not tp and sig.get('take_profit'):
                        tp = [{'level': 1, 'price': sig['take_profit']}]
                    sig['signal']             = d
                    sig['take_profit_levels'] = tp
                    sig['category']           = category
                    sig['market_open']        = True
                    return jsonify({'success': True, 'signal': sig, 'human_response': sig})
            except Exception as _pe:
                logger.warning(f"TradingCore signal {asset}: {_pe}")
        df = _fetch_ohlcv(asset, category, days=5, interval='15m')
        if df is None or df.empty:
            for iv in ('1h', '1d'):
                df = _fetch_ohlcv(asset, category, days=30, interval=iv)
                if df is not None and not df.empty:
                    break
        if df is None or df.empty:
            return jsonify({'success': False, 'error': f'No data for {asset}'}), 404
        try:
            from indicators.technical import TechnicalIndicators
            df = TechnicalIndicators.add_all_indicators(df)
        except Exception:
            pass
        price = float(df['close'].iloc[-1])
        atr   = float(df['atr'].iloc[-1]) if 'atr' in df.columns else price * 0.01
        rsi   = float(df['rsi'].iloc[-1]) if 'rsi' in df.columns else 50.0
        d     = 'BUY' if rsi < 40 else 'SELL' if rsi > 60 else 'HOLD'
        sl    = price - atr * 1.5 if d == 'BUY' else price + atr * 1.5
        tp    = price + atr * 2   if d == 'BUY' else price - atr * 2
        whale = None
        try:
            wm = get_whale_mgr()
            if wm:
                alerts = wm.get_alerts(min_value_usd=1_000_000)
                base   = asset.replace('-USD', '').replace('/USD', '').replace('=F', '')
                for a in alerts[:5]:
                    sym = a.get('symbol', '')
                    if sym and (sym in base or base in sym):
                        whale = f"{a.get('amount', 0)} {sym} (${a['value_usd']/1e6:.1f}M) moved"
                        break
        except Exception:
            pass
        sr = {
            'direction': d, 'signal': d, 'confidence': 0.6,
            'current_price': price, 'entry_price': price,
            'stop_loss': round(sl, 5), 'take_profit': round(tp, 5),
            'take_profit_levels': [{'level': 1, 'price': round(tp, 5)}],
            'category': category, 'market_open': True,
        }
        hr = generate_human_response(asset, df, sr, [], whale)
        return jsonify({'success': True, 'signal': sr, 'human_response': hr})
    except Exception as _e:
        logger.error(f"get_signal {asset}: {_e}\n{traceback.format_exc()}")
        return jsonify({'success': False, 'error': str(_e)}), 500


@app.route('/api/signal/history')
def signal_history():
    try:
        from signal_learning import signal_engine
        asset = request.args.get('asset')
        limit = int(request.args.get('limit', 20))
        return jsonify({'success': True, 'signals': signal_engine.get_history(asset, limit)})
    except Exception as _e:
        return jsonify({'success': False, 'error': str(_e)}), 500


@app.route('/api/position-audit')
def position_audit():
    try:
        core = get_bot()
        if not core:
            return jsonify({'error': 'Trading system not ready', 'healthy': False})
        positions = core.get_positions()
        return jsonify({'healthy': True, 'positions': positions,
                        'count': len(positions), 'timestamp': datetime.now().isoformat()})
    except Exception as _e:
        return jsonify({'error': str(_e), 'healthy': False})


@app.route('/api/positions/stream')
def positions_stream():
    def _enrich(p: dict) -> dict:
        try:
            asset    = p.get('asset', '')
            category = p.get('category', _ASSET_MAP.get(asset, ('stocks', 0))[0])
            cur, _   = fetcher.get_real_time_price(asset, category)
            entry    = float(p.get('entry_price', 0))
            size     = float(p.get('position_size', p.get('size', 1)))
            d        = p.get('direction', p.get('signal', 'BUY'))
            if cur and entry:
                pnl_pct = ((cur - entry) / entry * 100) if d == 'BUY' else ((entry - cur) / entry * 100)
                pnl_usd = pnl_pct / 100 * size
            else:
                pnl_pct = pnl_usd = 0
            return {**p, 'current_price': cur, 'pnl_pct': round(pnl_pct, 3),
                    'pnl_usd': round(pnl_usd, 2), 'updated_at': datetime.now().isoformat()}
        except Exception:
            return p

    def _event_gen():
        while True:
            try:
                positions = []
                if _CORE is not None:
                    try:
                        positions = [_enrich(p) for p in _CORE.state.get_open_positions()]
                    except Exception as _ce:
                        logger.debug(f"TradingCore positions: {_ce}")
                payload = json.dumps({'positions': positions, 'count': len(positions),
                                      'ts': datetime.now().isoformat()}, cls=_Encoder)
                yield f"data: {payload}\n\n"
            except Exception as _e:
                yield f"data: {json.dumps({'error': str(_e)})}\n\n"
            time.sleep(5)

    return Response(stream_with_context(_event_gen()), mimetype='text/event-stream',
                    headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'})


@app.route('/api/stress-test', methods=['GET', 'POST'])
def stress_test():
    try:
        from signal_learning import run_stress_test
        if request.method == 'POST':
            body      = request.get_json(force=True) or {}
            positions = body.get('positions', [])
            balance   = float(body.get('balance', args.balance))
        else:
            positions = []
            balance   = args.balance
            if _CORE is not None:
                for p in _CORE.state.get_open_positions():
                    asset = p.get('asset', '')
                    cat, _ = _ASSET_MAP.get(asset, ('stocks', 0))
                    positions.append({'asset': asset, 'category': p.get('category', cat),
                                      'direction': p.get('direction', p.get('signal', 'BUY')),
                                      'size_usd': float(p.get('position_size', p.get('size', 0)))})
                balance = _CORE.state.balance
        result = run_stress_test(positions, balance)
        return jsonify({'success': True, **result})
    except Exception as _e:
        return jsonify({'success': False, 'error': str(_e)}), 500


@app.route('/api/walk-forward/<path:asset>')
def walk_forward(asset: str):
    try:
        import numpy as np
        from indicators.technical import TechnicalIndicators
        from ml.predictor import MLPredictor
        asset    = ASSET_ALIASES.get(asset.upper().strip(), asset.upper().strip())
        category, _ = _ASSET_MAP.get(asset, ('stocks', 0.5))
        df = _fetch_ohlcv(asset, category, days=90, interval='1d')
        if df is None or len(df) < 60:
            return jsonify({'success': False, 'error': 'Not enough history (need 60+ days)'}), 422
        df = TechnicalIndicators.add_all_indicators(df)
        predictor  = MLPredictor()
        WINDOWS    = 3
        WIN_SIZE   = len(df) // WINDOWS
        results    = []
        best_model = None
        best_sharpe = -999
        for w in range(WINDOWS):
            start = w * WIN_SIZE
            end   = start + WIN_SIZE
            train = df.iloc[start: end - WIN_SIZE // 3]
            test  = df.iloc[end - WIN_SIZE // 3: end]
            if len(train) < 20 or len(test) < 5:
                continue
            try:
                preds = []
                for i in range(len(test)):
                    prob, conf = predictor.predict(asset, category,
                                                   test.iloc[:i + 1] if i > 0 else train.iloc[-5:])
                    direction = 'UP' if prob > 0.55 else 'DOWN' if prob < 0.45 else 'HOLD'
                    preds.append({'direction': direction, 'confidence': conf})
                rets = []
                for i, pred in enumerate(preds[:-1]):
                    if pred['direction'] in ('UP', 'DOWN') and pred['confidence'] > 0.3:
                        ret = float(test['close'].iloc[i + 1] - test['close'].iloc[i]) / float(test['close'].iloc[i])
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
                    'window': w + 1,
                    'train_start': str(train.index[0]), 'train_end': str(train.index[-1]),
                    'test_start':  str(test.index[0]),  'test_end':  str(test.index[-1]),
                    'signals': len(rets), 'win_rate': round(win_r, 3),
                    'total_return_pct': round(total, 2), 'sharpe': round(sharpe, 3),
                }
                results.append(window_result)
                if sharpe > best_sharpe:
                    best_sharpe = sharpe
                    best_model  = w + 1
            except Exception as _we:
                results.append({'window': w + 1, 'error': str(_we)})
        if not results:
            return jsonify({'success': False, 'error': 'All windows failed'})
        valid   = [r for r in results if 'sharpe' in r]
        avg_sh  = round(sum(r['sharpe'] for r in valid) / max(1, len(valid)), 3)
        avg_wr  = round(sum(r['win_rate'] for r in valid) / max(1, len(valid)), 3)
        return jsonify({
            'success': True, 'asset': asset, 'windows': results,
            'summary': {'avg_sharpe': avg_sh, 'avg_win_rate': avg_wr,
                        'best_window': best_model,
                        'verdict': 'ROBUST' if avg_sh > 0.5 else 'MARGINAL' if avg_sh > 0 else 'POOR'},
        })
    except Exception as _e:
        return jsonify({'success': False, 'error': str(_e)}), 500


@app.route('/api/system-status')
def system_status():
    try:
        if _CORE is not None:
            perf      = _CORE.state.get_performance()
            balance   = perf.get('balance', args.balance)
            daily     = _CORE.get_daily_stats()
            return jsonify({
                'success':          True,
                'balance':          round(balance, 2),
                'pnl':              round(daily.get('daily_pnl', 0), 2),
                'total_pnl':        round(perf.get('total_pnl', 0), 2),
                'open_positions':   perf.get('open_positions', 0),
                'closed_positions': perf.get('total_trades', 0),
                'daily_trades':     daily.get('daily_trades', 0),
                'win_rate':         perf.get('win_rate', 0),
                'processes':        {'Trading Bot': _CORE.is_running, 'Web Dashboard': True},
                'engine_ready':     _CORE.is_ready,
                'timestamp':        datetime.now().isoformat(),
            })
        balance   = args.balance
        open_p    = closed_p = today_pnl = total_pnl = 0
        try:
            from services.database_service import DatabaseService
            db     = DatabaseService()
            trades = db.get_recent_trades(100) if getattr(db, 'use_db', False) else []
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
        return jsonify({'success': True, 'balance': round(balance, 2),
                        'pnl': round(today_pnl, 2), 'open_positions': open_p,
                        'closed_positions': closed_p,
                        'processes': {'Trading Bot': _bot is not None, 'Web Dashboard': True},
                        'timestamp': datetime.now().isoformat()})
    except Exception as _e:
        return jsonify({'success': False, 'error': str(_e)}), 500


@app.route('/api/settings/update', methods=['POST'])
def update_settings():
    try:
        data = request.get_json(force=True) or {}
        if 'balance' in data: args.balance = float(data['balance'])
        return jsonify({'success': True})
    except Exception as _e:
        return jsonify({'success': False, 'error': str(_e)}), 400


@app.route('/api/status')
def api_status():
    engine_ready = _CORE.is_ready if _CORE else (_bot is not None)
    return jsonify({
        'market_status': MarketHours.get_status(),
        'assets_cached': len(_sig_store),
        'bot_ready':     engine_ready,
        'architecture':  'TradingCore' if _CORE else 'standalone',
    })


@app.route('/api/backtest/run')
def api_backtest_run():
    import numpy as np
    asset    = request.args.get('asset', 'BTC-USD')
    period   = request.args.get('period', '90d')
    days     = {'30d': 30, '90d': 90, '180d': 180, '365d': 365, '730d': 730}.get(period, 90)
    category, _ = _ASSET_MAP.get(asset, ('crypto', 0.02))

    def _clean(obj):
        if isinstance(obj, dict):  return {k: _clean(v) for k, v in obj.items()}
        if isinstance(obj, list):  return [_clean(v) for v in obj]
        try:
            import numpy as _np
            if isinstance(obj, _np.integer):  return int(obj)
            if isinstance(obj, _np.floating): return float(obj)
            if isinstance(obj, _np.ndarray):  return obj.tolist()
        except Exception: pass
        if hasattr(obj, 'isoformat'): return obj.isoformat()
        return obj

    try:
        df = _fetch_ohlcv(asset, category, days=days, interval='1d')
        if df is None or df.empty:
            return jsonify({'success': False, 'error': f'No data for {asset}'}), 404
        try:
            from indicators.technical import TechnicalIndicators
            df = TechnicalIndicators.add_all_indicators(df)
        except Exception: pass
        try:
            from backtest.engine import BacktestEngine
            result  = BacktestEngine(initial_balance=args.balance).run(asset, category, df)
            rd      = result.to_dict()
            balance = args.balance
            equity_curve = []
            for i, trade in enumerate(result.trades):
                balance += float(trade.get('pnl', 0))
                equity_curve.append({'date': str(trade.get('open_time', i))[:10],
                                     'value': round(balance, 2),
                                     'benchmark': round(args.balance * (1 + i * 0.0005), 2)})
            from collections import defaultdict as _dd
            monthly: dict = _dd(float)
            for trade in result.trades:
                monthly[str(trade.get('open_time', ''))[:7] or 'Unknown'] += float(trade.get('pnl', 0))
            return jsonify(_clean({'success': True, 'results': {
                **rd, 'total_return': rd.get('return_pct', 0),
                'equity_curve':    equity_curve,
                'monthly_returns': [{'month': k, 'return_pct': round(v / args.balance * 100, 2)}
                                    for k, v in sorted(monthly.items())],
                'trades': result.trades,
            }}))
        except ImportError:
            pass
        return jsonify({'success': True, 'results': {'total_trades': 0, 'win_rate': 0, 'total_pnl': 0,
                        'return_pct': 0, 'equity_curve': [], 'monthly_returns': [], 'trades': []}})
    except Exception as _e:
        return jsonify({'success': False, 'error': str(_e)}), 500


@app.route('/api/sentiment/dashboard')
def api_sentiment_dashboard():
    try:
        analyzer = get_sentiment()
        if analyzer is None:
            return jsonify({'success': False, 'error': 'SentimentAnalyzer unavailable'}), 503
        result = {
            'success': True, 'overall_sentiment': 'Neutral', 'score': 0,
            'fear_greed': {'value': 50, 'classification': 'Neutral', 'score': 0},
            'vix': {'value': 20, 'classification': 'Normal', 'score': 0},
            'article_count': 0,
            'sentiment_distribution': {'bullish': 0, 'neutral': 0, 'bearish': 0},
            'sources': {}, 'articles': [], 'whale_alerts': [],
        }
        ms = analyzer.get_comprehensive_sentiment('general')
        if ms:
            result.update({'overall_sentiment': ms.get('interpretation', 'Neutral'), 'score': ms.get('score', 0)})
        fg = analyzer.fetch_fear_greed_index()
        if fg:
            result['fear_greed'] = {'value': fg.get('value', 50), 'classification': fg.get('classification', 'Neutral'), 'score': fg.get('score', 0)}
        vix = analyzer.fetch_vix()
        if vix:
            result['vix'] = {'value': vix.get('value', 20), 'classification': vix.get('classification', 'Normal'), 'score': vix.get('score', 0)}
        if hasattr(analyzer, 'news_integrator'):
            arts = analyzer.news_integrator.fetch_all_sources()
            result['articles'] = sorted(arts, key=lambda x: x.get('date', ''), reverse=True)[:20]
            result['article_count'] = len(result['articles'])
        result['whale_alerts'] = analyzer.fetch_whale_alerts(min_value_usd=1_000_000)[:10]
        if result['articles']:
            b  = sum(1 for a in result['articles'] if a.get('sentiment', 0) > 0.1)
            be = sum(1 for a in result['articles'] if a.get('sentiment', 0) < -0.1)
            result['sentiment_distribution'] = {
                'bullish': b, 'neutral': len(result['articles']) - b - be, 'bearish': be,
            }
        return jsonify(result)
    except Exception as _e:
        return jsonify({'success': False, 'error': str(_e)}), 500


@app.route('/api/market/events')
def api_market_events():
    try:
        analyzer = get_sentiment()
        return jsonify({'success': True, 'events': analyzer.get_market_events() if analyzer else []})
    except Exception as _e:
        return jsonify({'success': False, 'error': str(_e)}), 500


@app.route('/api/websocket/feed')
def get_websocket_feed():
    try:
        from websocket_dashboard import get_feed, connection_status
        src = request.args.get('source', 'all')
        txs = get_feed(source_filter=src, limit=200)
        return jsonify({'success': True, 'transactions': txs, 'count': len(txs),
                        'connection_status': connection_status})
    except Exception as _e:
        return jsonify({'success': False, 'error': str(_e)}), 500


@app.route('/api/chart/stream')
def chart_stream():
    asset    = request.args.get('asset', 'EUR/USD')
    category = _ASSET_MAP.get(asset, ('forex', 0.001))[0]

    def generate():
        last_pos_push = 0.0
        try:
            while True:
                import time as _t
                now   = _t.time()
                price = _get_sse_price(asset, category)
                if price:
                    yield f"data: {json.dumps({'type': 'tick', 'asset': asset, 'price': price, 'time': int(now)})}\n\n"
                if now - last_pos_push >= 5:
                    try:
                        open_pos = []
                        history  = []
                        balance  = None
                        if _CORE is not None:
                            positions = _CORE.state.get_open_positions()
                            for p in positions:
                                unreal = None
                                try:
                                    if price and p.get('asset') == asset and p.get('entry_price'):
                                        diff = price - float(p['entry_price'])
                                        if p.get('direction', p.get('signal', 'BUY')) == 'SELL': diff = -diff
                                        unreal = round(diff * float(p.get('position_size', 0)), 4)
                                except Exception: pass
                                p['unrealized_pnl'] = unreal
                                open_pos.append(p)
                            history = _CORE.get_closed_trades(limit=50)
                            balance = _CORE.get_balance()
                        yield f"data: {json.dumps({'type': 'positions', 'open': open_pos, 'history': history, 'balance': balance}, default=str)}\n\n"
                        last_pos_push = now
                    except Exception as _pe:
                        logger.debug(f"SSE positions error: {_pe}")
                _t.sleep(2)
        except GeneratorExit: pass
        except Exception as _se:
            logger.debug(f"SSE stream error for {asset}: {_se}")

    return Response(stream_with_context(generate()), mimetype='text/event-stream',
                    headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no', 'Connection': 'keep-alive'})


@app.route('/api/chart/assets')
def chart_assets():
    try:
        assets = [{'symbol': a, 'category': cat} for a, cat, _ in ALL_ASSETS]
        return jsonify({'success': True, 'assets': assets})
    except Exception as _e:
        return jsonify({'success': False, 'error': str(_e)}), 500


@app.route('/api/chart/candles')
def chart_candles():
    try:
        asset    = request.args.get('asset', 'EUR/USD')
        interval = request.args.get('interval', '1h')
        category, _ = _ASSET_MAP.get(asset, ('forex', 0.001))
        days_map = {'1m': 1, '5m': 5, '15m': 7, '1h': 30, '4h': 90, '1d': 365}
        days     = days_map.get(interval, 30)
        df = _fetch_ohlcv(asset, category, days=days, interval=interval)
        if df is None or df.empty:
            return jsonify({'success': False, 'error': f'No data for {asset}'}), 404
        df.columns = [c.lower() for c in df.columns]
        candles = []
        for ts, row in df.iterrows():
            try:
                t = int(ts.timestamp()) if hasattr(ts, 'timestamp') else int(
                    __import__('pandas').Timestamp(ts).timestamp())
                candles.append({
                    'time':   t,
                    'open':   float(row.get('open',  row.get('close', 0))),
                    'high':   float(row.get('high',  row.get('close', 0))),
                    'low':    float(row.get('low',   row.get('close', 0))),
                    'close':  float(row.get('close', 0)),
                    'volume': float(row.get('volume', 0)),
                })
            except Exception: continue
        seen, clean = set(), []
        for c in sorted(candles, key=lambda x: x['time']):
            if c['time'] not in seen:
                seen.add(c['time'])
                clean.append(c)
        return jsonify({'success': True, 'candles': clean, 'count': len(clean)})
    except Exception as _e:
        return jsonify({'success': False, 'error': str(_e)}), 500


@app.route('/api/chart/positions')
def chart_positions():
    try:
        open_pos = []
        history  = []
        balance  = None
        if _CORE is not None:
            raw = _CORE.state.get_open_positions()
            for p in raw:
                unreal = None
                try:
                    cat = p.get('category', 'forex')
                    cur, _ = fetcher.get_real_time_price(p['asset'], cat)
                    if cur and p.get('entry_price'):
                        diff = cur - float(p['entry_price'])
                        if p.get('direction', p.get('signal', 'BUY')) == 'SELL': diff = -diff
                        unreal = round(diff * float(p.get('position_size', 0)), 4)
                except Exception: pass
                p['unrealized_pnl'] = unreal
                open_pos.append(p)
            history = _CORE.get_closed_trades(limit=50)
            balance = _CORE.get_balance()
        return jsonify({'success': True, 'open': open_pos, 'history': history, 'balance': balance})
    except Exception as _e:
        return jsonify({'success': False, 'error': str(_e)}), 500


@app.route('/api/refresh/manual', methods=['POST'])
def manual_refresh():
    _last_ref.clear()
    return jsonify({'success': True, 'message': 'Refresh queued'})


@app.route('/api/tests')
def run_tests():
    import io, contextlib
    buf = io.StringIO()
    try:
        from signal_learning import _run_tests
        with contextlib.redirect_stdout(buf):
            success = _run_tests()
        return jsonify({'success': success, 'output': buf.getvalue()})
    except Exception as _e:
        return jsonify({'success': False, 'error': str(_e), 'output': buf.getvalue()}), 500


@app.route('/api/orderflow/<path:asset>')
def api_orderflow(asset: str):
    asset = ASSET_ALIASES.get(asset.upper(), asset)
    if _orderflow_engine:
        snap = _orderflow_engine.get_snapshot(asset)
        if snap:
            return jsonify({'success': True, 'data': snap})
    return jsonify({'success': False, 'error': 'No orderflow data yet',
                    'data': {'asset': asset, 'pressure': 'NEUTRAL', 'imbalance': 0,
                             'bid_vol': 0, 'ask_vol': 0, 'delta': 0}})


@app.route('/api/orderflow')
def api_orderflow_all():
    if _orderflow_engine:
        snaps = _orderflow_engine.get_all_snapshots()
        return jsonify({'success': True, 'data': snaps, 'count': len(snaps)})
    return jsonify({'success': False, 'data': {}, 'count': 0})


@app.route('/api/alpha')
def api_alpha_signals():
    n = min(int(request.args.get('n', 50)), 200)
    if _alpha_engine:
        sigs = _alpha_engine.get_recent_signals(n)
        return jsonify({'success': True, 'signals': sigs, 'count': len(sigs)})
    return jsonify({'success': False, 'signals': [], 'count': 0})


@app.route('/api/alpha/<path:asset>')
def api_alpha_for_asset(asset: str):
    asset = ASSET_ALIASES.get(asset.upper(), asset)
    if _alpha_engine:
        sigs = _alpha_engine.get_signals_for_asset(asset, 20)
        return jsonify({'success': True, 'asset': asset, 'signals': sigs})
    return jsonify({'success': False, 'asset': asset, 'signals': []})


@app.route('/api/accuracy')
def api_accuracy():
    days = min(int(request.args.get('days', 30)), 90)
    if _pred_tracker:
        stats = _pred_tracker.get_accuracy_stats(days)
        return jsonify({'success': True, 'data': stats})
    return jsonify({'success': False, 'data': {
        'by_horizon': {'1H': {'total': 0, 'correct': 0, 'accuracy_pct': 0},
                       '4H': {'total': 0, 'correct': 0, 'accuracy_pct': 0},
                       '24H': {'total': 0, 'correct': 0, 'accuracy_pct': 0}},
        'by_asset': {}, 'recent': [], 'days_back': days,
    }})


@app.route('/api/prediction-overlay/<path:asset>')
def api_prediction_overlay(asset: str):
    asset    = ASSET_ALIASES.get(asset.upper(), asset)
    cat, _   = _ASSET_MAP.get(asset, ('forex', 0.001))
    overlay  = None
    core = get_bot()
    if core is not None and hasattr(core, 'get_signal_for_asset'):
        try:
            sig = core.get_signal_for_asset(asset)
            if sig and sig.get('direction', 'HOLD') != 'HOLD':
                overlay = {
                    'direction':       sig['direction'],
                    'entry_price':     sig.get('entry_price', 0),
                    'target_price':    sig.get('take_profit', 0),
                    'stop_loss':       sig.get('stop_loss', 0),
                    'confidence':      sig.get('confidence', 0.5),
                    'risk_reward':     sig.get('risk_reward', 0),
                    'horizon_minutes': 60, 'asset': asset,
                    'strategy':        sig.get('strategy_id', ''),
                    'regime':          sig.get('metadata', {}).get('regime', ''),
                }
        except Exception: pass
    if overlay is None:
        cached = _signal_cache.get(asset, {})
        signal = cached.get('signal') if isinstance(cached, dict) else None
        if signal and isinstance(signal, dict) and signal.get('signal', 'HOLD') != 'HOLD':
            entry = signal.get('entry_price', signal.get('entry', 0))
            if entry:
                overlay = {
                    'direction':       signal.get('signal', 'HOLD'),
                    'entry_price':     entry,
                    'target_price':    signal.get('take_profit', 0),
                    'stop_loss':       signal.get('stop_loss', 0),
                    'confidence':      signal.get('confidence', 0.5),
                    'risk_reward':     signal.get('risk_reward', 0),
                    'horizon_minutes': 60, 'asset': asset,
                    'strategy':        signal.get('strategy', ''),
                    'regime':          signal.get('regime', ''),
                }
    alpha_sigs = _alpha_engine.get_signals_for_asset(asset, 3) if _alpha_engine else []
    of_snap    = _orderflow_engine.get_snapshot(asset) if _orderflow_engine else None
    # Flatten for chart_live.html — it reads top-level fields
    if overlay:
        return jsonify({'success': True, 'asset': asset, **overlay,
                        'alpha': alpha_sigs, 'orderflow': of_snap})
    return jsonify({'success': False, 'asset': asset, 'direction': 'HOLD',
                    'alpha': alpha_sigs, 'orderflow': of_snap})


@app.route('/api/redis/status')
def api_redis_status():
    if _redis_broker:
        return jsonify({'success': True, 'connected': _redis_broker.is_connected,
                        'channels': _redis_broker.CHANNELS, 'gateway': 'ws://localhost:8081'})
    return jsonify({'success': False, 'connected': False,
                    'message': 'redis-py not installed or Redis not running'})


# ══════════════════════════════════════════════════════════════════════════════
# NEW PROFESSIONAL DASHBOARD API ROUTES
# ══════════════════════════════════════════════════════════════════════════════

@app.route('/api/command-center')
def api_command_center():
    try:
        core = get_bot()
        perf = {}; daily = {}; positions = []; health = {}
        if core:
            perf      = core.get_performance()
            daily     = core.get_daily_stats()
            positions = core.get_positions()
            health    = core.health_report()

        sent_score = 0.0
        try:
            sa = get_sentiment()
            if sa:
                ms = sa.get_comprehensive_sentiment('general')
                sent_score = float(ms.get('score', 0)) if ms else 0.0
        except Exception: pass

        whale_count = 0
        try:
            wm = get_whale_mgr()
            if wm:
                alerts = wm.get_top_alerts(limit=5, days=1)
                whale_count = len(alerts)
        except Exception: pass

        signals = [s for s in list(_sig_store.values())
                   if s.get('signal', 'HOLD') not in ('HOLD', 'CLOSED')][:5]

        return jsonify({
            'success':          True,
            'balance':          perf.get('balance', args.balance),
            'total_pnl':        perf.get('total_pnl', 0),
            'daily_pnl':        daily.get('daily_pnl', 0),
            'daily_trades':     daily.get('daily_trades', 0),
            'win_rate':         perf.get('win_rate', 0),
            'open_positions':   len(positions),
            'total_trades':     perf.get('total_trades', 0),
            'engine_running':   health.get('is_running', False),
            'engine_ready':     health.get('engine_ready', False),
            'sentiment_score':  round(sent_score, 3),
            'whale_alerts_24h': whale_count,
            'latest_signals':   signals,
            'positions':        positions[:8],
            'timestamp':        datetime.now().isoformat(),
        })
    except Exception as _e:
        return jsonify({'success': False, 'error': str(_e)}), 500


@app.route('/api/correlation-matrix')
def api_correlation_matrix():
    try:
        import pandas as pd
        import numpy as np
        assets = ['BTC-USD','ETH-USD','GC=F','EUR/USD','^GSPC','AAPL','NVDA','TSLA']
        closes = {}
        for asset in assets:
            cat = _ASSET_MAP.get(asset, ('crypto', 0.02))[0]
            df  = _fetch_ohlcv(asset, cat, days=30, interval='1d')
            if df is not None and not df.empty and 'close' in df.columns:
                closes[asset] = df['close'].astype(float)
        if len(closes) < 2:
            return jsonify({'success': False, 'error': 'Not enough data'})
        frame  = pd.DataFrame(closes).pct_change().dropna()
        corr   = frame.corr().round(3)
        return jsonify({'success': True, 'labels': list(corr.columns),
                        'matrix': corr.values.tolist()})
    except Exception as _e:
        return jsonify({'success': False, 'error': str(_e)}), 500


@app.route('/api/market/heatmap')
def api_market_heatmap():
    try:
        sample = [
            ('BTC-USD','crypto'), ('ETH-USD','crypto'), ('SOL-USD','crypto'),
            ('XRP-USD','crypto'),  ('BNB-USD','crypto'),
            ('EUR/USD','forex'),   ('GBP/USD','forex'),  ('USD/JPY','forex'),
            ('GC=F','commodities'),('CL=F','commodities'),
            ('^GSPC','indices'),   ('^IXIC','indices'),
            ('AAPL','stocks'),     ('NVDA','stocks'),     ('TSLA','stocks'),
        ]
        results = []
        for asset, cat in sample:
            try:
                df = _fetch_ohlcv(asset, cat, days=2, interval='1d')
                if df is not None and len(df) >= 2 and 'close' in df.columns:
                    closes = df['close'].astype(float)
                    chg    = (closes.iloc[-1] - closes.iloc[-2]) / closes.iloc[-2] * 100
                    results.append({'asset': asset, 'category': cat,
                                    'change_pct': round(float(chg), 2),
                                    'price': round(float(closes.iloc[-1]), 5)})
            except Exception: pass
        results.sort(key=lambda x: x['change_pct'], reverse=True)
        return jsonify({'success': True, 'items': results})
    except Exception as _e:
        return jsonify({'success': False, 'error': str(_e)}), 500


@app.route('/api/predictions/summary')
def api_predictions_summary():
    try:
        stats   = {}
        if _pred_tracker:
            stats = _pred_tracker.get_accuracy_stats(days_back=30)
        signals = list(_sig_store.values())
        preds   = []
        for s in signals:
            d = s.get('signal', s.get('direction', 'HOLD'))
            if d in ('HOLD', 'CLOSED'): continue
            e  = s.get('entry_price', 0)
            sl = s.get('stop_loss', 0)
            tp = s.get('take_profit', 0)
            rr = round(abs(tp - e) / max(0.0001, abs(e - sl)), 2) if sl and tp and e else 0
            preds.append({
                'asset':      s.get('asset', ''),
                'direction':  d,
                'confidence': round((s.get('confidence', 0)) * 100, 1),
                'entry':      e, 'tp': tp, 'sl': sl, 'rr': rr,
                'category':   s.get('category', ''),
                'strategy':   s.get('strategy_id', ''),
                'timestamp':  s.get('timestamp', ''),
            })
        return jsonify({'success': True, 'predictions': preds, 'accuracy': stats})
    except Exception as _e:
        return jsonify({'success': False, 'error': str(_e)}), 500


@app.route('/api/whale/summary')
def api_whale_summary():
    try:
        wm = get_whale_mgr()
        if not wm:
            return jsonify({'success': True, 'alerts': [], 'total_volume_usd': 0,
                            'top_assets': [], 'recent': [], 'alert_count_24h': 0})
        alerts    = wm.get_alerts(min_value_usd=500_000, hours=24)
        top       = wm.get_top_alerts(limit=10, days=7)
        total_vol = sum(float(a.get('value_usd', 0)) for a in alerts)
        by_asset  = {}
        for a in alerts:
            sym = a.get('symbol', a.get('asset', ''))
            by_asset[sym] = by_asset.get(sym, 0) + float(a.get('value_usd', 0))
        top_assets = sorted(by_asset.items(), key=lambda x: x[1], reverse=True)[:8]
        return jsonify({
            'success':          True,
            'alerts':           alerts[:20],
            'total_volume_usd': round(total_vol, 0),
            'alert_count_24h':  len(alerts),
            'top_assets':       [{'asset': k, 'volume': round(v)} for k, v in top_assets],
            'recent':           top[:10],
        })
    except Exception as _e:
        return jsonify({'success': False, 'error': str(_e)}), 500


@app.route('/api/risk/portfolio')
def api_risk_portfolio():
    try:
        core = get_bot()
        if not core:
            return jsonify({'success': False, 'error': 'Engine not ready'})
        positions = core.get_positions()
        balance   = core.get_balance()
        perf      = core.get_performance()

        risk_stats = {}
        try:
            if hasattr(core, 'portfolio_risk') and core.portfolio_risk:
                risk_stats = core.portfolio_risk.get_portfolio_stats(positions, balance)
        except Exception: pass

        by_cat = {}
        for p in positions:
            cat  = p.get('category', 'unknown')
            pnl  = float(p.get('pnl', 0))
            by_cat.setdefault(cat, {'count': 0, 'pnl': 0.0, 'exposure': 0.0})
            by_cat[cat]['count']    += 1
            by_cat[cat]['pnl']      += pnl
            by_cat[cat]['exposure'] += float(p.get('position_size', 0)) * float(p.get('entry_price', 0))

        closed   = core.get_closed_trades(limit=100)
        wins     = [t for t in closed if float(t.get('pnl', 0)) > 0]
        losses   = [t for t in closed if float(t.get('pnl', 0)) <= 0]
        avg_win  = sum(float(t['pnl']) for t in wins)   / len(wins)   if wins   else 0
        avg_loss = sum(float(t['pnl']) for t in losses) / len(losses) if losses else 0
        profit_factor = abs(avg_win / avg_loss) if avg_loss else 0

        return jsonify({
            'success':        True,
            'balance':        balance,
            'open_positions': len(positions),
            'total_exposure': risk_stats.get('total_exposure', 0),
            'exposure_pct':   risk_stats.get('exposure_pct', 0),
            'drawdown_pct':   risk_stats.get('drawdown_pct', 0),
            'peak_balance':   risk_stats.get('peak_balance', balance),
            'by_category':    by_cat,
            'win_rate':       perf.get('win_rate', 0),
            'profit_factor':  round(profit_factor, 2),
            'avg_win':        round(avg_win, 2),
            'avg_loss':       round(avg_loss, 2),
            'total_trades':   perf.get('total_trades', 0),
            'total_pnl':      perf.get('total_pnl', 0),
        })
    except Exception as _e:
        return jsonify({'success': False, 'error': str(_e)}), 500


@app.route('/api/strategy/performance')
def api_strategy_performance():
    try:
        core = get_bot()
        if not core:
            return jsonify({'success': False, 'error': 'Engine not ready'})
        stats  = core.get_strategy_stats()
        trades = core.get_closed_trades(limit=200)
        enriched = {}
        for strat, s in stats.items():
            total = s.get('wins', 0) + s.get('losses', 0)
            pnl   = s.get('pnl', 0)
            wr    = s.get('wins', 0) / total * 100 if total else 0
            strat_trades = [t for t in trades if t.get('strategy_id') == strat]
            durs = [int(t.get('duration_minutes', 0)) for t in strat_trades if t.get('duration_minutes')]
            avg_dur = sum(durs) / len(durs) if durs else 0
            enriched[strat] = {
                **s, 'total': total, 'win_rate': round(wr, 1),
                'avg_duration_min': round(avg_dur),
                'avg_trade_pnl': round(pnl / total, 4) if total else 0,
            }
        timeline = [{
            'trade_id':  t.get('trade_id', ''),
            'asset':     t.get('asset', ''),
            'direction': t.get('direction', t.get('signal', '')),
            'pnl':       float(t.get('pnl', 0)),
            'strategy':  t.get('strategy_id', ''),
            'exit_time': str(t.get('exit_time', ''))[:16],
            'conf':      float(t.get('confidence', 0)),
        } for t in trades[:50]]
        return jsonify({'success': True, 'strategies': enriched, 'timeline': timeline})
    except Exception as _e:
        return jsonify({'success': False, 'error': str(_e)}), 500


@app.route('/api/system/health')
def api_system_health():
    try:
        core   = get_bot()
        health = core.health_report() if core else {}

        ram_pct = cpu_pct = disk_pct = process_mem_mb = 0.0
        try:
            import psutil, os as _os
            ram_pct        = psutil.virtual_memory().percent
            cpu_pct        = psutil.cpu_percent(interval=0)
            disk_pct       = psutil.disk_usage('/').percent
            proc           = psutil.Process(_os.getpid())
            process_mem_mb = round(proc.memory_info().rss / 1024 / 1024, 1)
        except Exception: pass

        redis_ok = False
        try:
            if _redis_broker:
                redis_ok = bool(_redis_broker.is_connected())
        except Exception: pass

        db_ok = False
        try:
            from services.db_pool import get_db
            db_ok = get_db().ping()
        except Exception: pass

        tg_ok = bool(getattr(telegram_manager, 'is_running', False))

        processes = {
            'TradingCore':       health.get('is_running', False),
            'Engine ready':      health.get('engine_ready', False),
            'Web dashboard':     True,
            'Redis':             redis_ok,
            'PostgreSQL':        db_ok,
            'Telegram':          tg_ok,
            'PredTracker':       _pred_tracker is not None,
            'WebSocket manager': _redis_broker is not None,
        }

        return jsonify({
            'success':          True,
            'ram_pct':          round(ram_pct, 1),
            'cpu_pct':          round(cpu_pct, 1),
            'disk_pct':         round(disk_pct, 1),
            'process_mem_mb':   process_mem_mb,
            'processes':        processes,
            'open_positions':   health.get('open_positions', 0),
            'active_cooldowns': health.get('active_cooldowns', 0),
            'issues':           health.get('issues', []),
            'strategy_mode':    health.get('strategy_mode', '—'),
            'balance':          health.get('balance', 0),
            'timestamp':        datetime.now().isoformat(),
        })
    except Exception as _e:
        return jsonify({'success': False, 'error': str(_e)}), 500


@app.route('/api/sentiment/by-asset')
def api_sentiment_by_asset():
    try:
        sa = get_sentiment()
        if not sa:
            return jsonify({'success': False, 'error': 'SentimentAnalyzer unavailable'})
        assets  = ['BTC-USD','ETH-USD','SOL-USD','XRP-USD','GC=F','EUR/USD','GBP/USD','AAPL','NVDA','TSLA']
        results = []
        for asset in assets:
            try:
                cat = _ASSET_MAP.get(asset, ('crypto', 0))[0]
                r   = sa.get_comprehensive_sentiment(asset, cat)
                score = float(r.get('composite_score', r.get('score', 0))) if r else 0.0
                results.append({'asset': asset, 'category': cat, 'score': round(score, 3),
                                 'label': 'Bullish' if score > 0.1 else 'Bearish' if score < -0.1 else 'Neutral'})
            except Exception:
                results.append({'asset': asset, 'category': _ASSET_MAP.get(asset, ('crypto',))[0],
                                 'score': 0.0, 'label': 'Neutral'})
        results.sort(key=lambda x: x['score'], reverse=True)
        return jsonify({'success': True, 'assets': results})
    except Exception as _e:
        return jsonify({'success': False, 'error': str(_e)}), 500


# ══════════════════════════════════════════════════════════════════════════════
# start_dashboard — called by bot.py (was missing — caused ImportError)
# ══════════════════════════════════════════════════════════════════════════════

def start_dashboard(core, host: str = '0.0.0.0', port: int = 5000) -> None:
    """
    Wire TradingCore into the Flask app and start the blocking server.
    Called by bot.py after engine.start().
    """
    inject_core(core)

    threading.Thread(target=_bg_refresh_worker, name='BgRefresh', daemon=True).start()

    try:
        from websocket_manager import WebSocketManager
        from websocket_dashboard import add_transaction
        def _cb(source, symbol, price, volume, side, ts=None):
            add_transaction(source, symbol, price, volume, side)
        ws = WebSocketManager()
        ws.start()
        ws.subscribe_bybit(['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'SOLUSDT', 'XRPUSDT'], _cb)
        ws.subscribe_finnhub(['AAPL', 'MSFT', 'GOOGL', 'TSLA', 'NVDA', 'AMZN'], _cb)
        ws.subscribe_twelvedata(['EUR/USD', 'XAU/USD'], _cb)
        logger.info("[Dashboard] WebSocket streams started")
    except Exception as _e:
        logger.warning(f"[Dashboard] WebSocket start failed: {_e}")

    logger.info(f"[Dashboard] Starting on http://{host}:{port}")
    logger.info(f"[Dashboard] Professional system: http://{host}:{port}/command-center")
    app.run(debug=False, host=host, port=port, threaded=True, use_reloader=False)


# ══════════════════════════════════════════════════════════════════════════════
# STANDALONE STARTUP
# ══════════════════════════════════════════════════════════════════════════════
if __name__ == '__main__':
    logger.info("=" * 60)
    logger.info("  ROBBIE TRADING PLATFORM — starting standalone")
    logger.info(f"  Balance: ${args.balance}  |  Assets: {len(ALL_ASSETS)}")
    logger.info("  Dashboard      : http://localhost:5000")
    logger.info("  Command Center : http://localhost:5000/command-center")
    logger.info("=" * 60)

    threading.Thread(target=_bg_refresh_worker, name='BgRefresh', daemon=True).start()

    def _start_ws():
        try:
            from websocket_manager import WebSocketManager
            from websocket_dashboard import add_transaction
            def _cb(source, symbol, price, volume, side, ts=None):
                add_transaction(source, symbol, price, volume, side)
            ws = WebSocketManager()
            ws.start()
            ws.subscribe_bybit(['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'SOLUSDT', 'XRPUSDT'], _cb)
            ws.subscribe_finnhub(['AAPL', 'MSFT', 'GOOGL', 'TSLA', 'NVDA', 'AMZN'], _cb)
            ws.subscribe_twelvedata(['EUR/USD', 'XAU/USD'], _cb)
            logger.info("WebSocket manager running")
        except Exception as _e:
            logger.warning(f"WebSocket start failed: {_e}")

    threading.Thread(target=_start_ws, name='WsManager', daemon=True).start()
    app.run(debug=False, host='0.0.0.0', port=5000, threaded=True, use_reloader=False)