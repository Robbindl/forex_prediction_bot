# type: ignore
"""
⚡ ULTIMATE MULTI-API DASHBOARD - REAL Trading Signals from Your Bot
"""

from flask import Flask, render_template, jsonify, request
from flask_cors import CORS
from datetime import datetime, timedelta
import threading
import time
import sys
import psutil
import json
from typing import Dict, List, Any
import argparse
import os
from pandas import Period, Timestamp
from telegram_manager import telegram_manager
from websocket_dashboard import recent_transactions
from collections import deque

class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Period):
            return str(obj)
        if isinstance(obj, Timestamp):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        if hasattr(obj, 'isoformat'):
            return obj.isoformat()
        return super().default(obj)

# Parse command line arguments
parser = argparse.ArgumentParser(description='Web Dashboard')
parser.add_argument('--balance', type=float, default=30, help='Initial account balance')
parser.add_argument('--no-telegram', action='store_true', help='Disable Telegram commander')
args = parser.parse_args()

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from data.fetcher import NASALevelFetcher, MarketHours

app = Flask(__name__)
CORS(app)

# Initialize the ULTIMATE fetcher
fetcher = NASALevelFetcher()

# ===== TELEGRAM MANAGER =====
if not args.no_telegram:
    try:
        telegram_token = os.getenv('TELEGRAM_TOKEN')
        telegram_chat_id = os.getenv('TELEGRAM_CHAT_ID')
        
        if not telegram_token and os.path.exists('config/telegram_config.json'):
            import json
            with open('config/telegram_config.json') as f:
                config = json.load(f)
                telegram_token = config.get('bot_token')
                telegram_chat_id = config.get('chat_id')
        
        if telegram_token and telegram_chat_id:
            if telegram_manager.start(telegram_token, telegram_chat_id, None):
                print("✅ Telegram manager active")
            else:
                print("⚠️ Telegram bot not started (another instance may be running)")
        else:
            print("⚠️ Telegram not configured")
    except Exception as e:
        print(f"⚠️ Telegram initialization skipped: {e}")
else:
    print("ℹ️ Telegram disabled by --no-telegram flag")

# Global state
signals_cache = {
    'signals': [],
    'settings': {
        'interval': '5m',
        'balance': args.balance,
        'risk': 1.0,
        'filter': 'all'
    },
    'last_refresh': None,
    'is_updating': False
}

# Complete asset universe
ALL_ASSETS = [
    # ===== COMMODITIES =====
    ('GC=F', 'commodities', 5.0),      # Gold Futures (works)
    ('SI=F', 'commodities', 0.2),      # Silver Futures (works)
    ('CL=F', 'commodities', 0.5),      # Crude Futures (works)
    ('NG=F', 'commodities', 0.05),     # Natural Gas Futures (works)
    ('HG=F', 'commodities', 0.05),
    
    # ===== CRYPTO =====
    ('BTC-USD', 'crypto', 0.02),
    ('ETH-USD', 'crypto', 0.03),
    ('BNB-USD', 'crypto', 0.02),
    ('SOL-USD', 'crypto', 0.04),
    ('XRP-USD', 'crypto', 0.015),
    ('ADA-USD', 'crypto', 0.02),
    ('DOGE-USD', 'crypto', 0.03),
    ('DOT-USD', 'crypto', 0.02),
    ('LTC-USD', 'crypto', 0.015),
    ('AVAX-USD', 'crypto', 0.03),
    ('LINK-USD', 'crypto', 0.02),
    
    # ===== FOREX =====
    ('EUR/USD', 'forex', 0.001),
    ('GBP/USD', 'forex', 0.001),
    ('USD/JPY', 'forex', 0.1),
    ('AUD/USD', 'forex', 0.001),
    ('USD/CAD', 'forex', 0.001),
    ('NZD/USD', 'forex', 0.001),
    ('USD/CHF', 'forex', 0.001),
    ('EUR/GBP', 'forex', 0.001),
    ('EUR/JPY', 'forex', 0.1),
    ('GBP/JPY', 'forex', 0.1),
    ('AUD/JPY', 'forex', 0.05),
    ('EUR/AUD', 'forex', 0.001),
    ('GBP/AUD', 'forex', 0.001),
    ('AUD/CAD', 'forex', 0.001),
    ('CAD/JPY', 'forex', 0.05),
    ('CHF/JPY', 'forex', 0.05),
    ('EUR/CAD', 'forex', 0.001),
    ('EUR/CHF', 'forex', 0.001),
    ('GBP/CAD', 'forex', 0.001),
    ('GBP/CHF', 'forex', 0.001),

    # ===== INDICES =====
    ('^GSPC', 'indices', 10),
    ('^DJI', 'indices', 50),
    ('^IXIC', 'indices', 30),
    ('^FTSE', 'indices', 20),
    ('^N225', 'indices', 100),
    ('^HSI', 'indices', 50),
    ('^GDAXI', 'indices', 30),
    ('^VIX', 'indices', 1),
    
    # ===== STOCKS =====
    ('AAPL', 'stocks', 0.5),
    ('MSFT', 'stocks', 0.5),
    ('GOOGL', 'stocks', 0.5),
    ('AMZN', 'stocks', 0.5),
    ('TSLA', 'stocks', 0.5),
    ('NVDA', 'stocks', 1.0),
    ('META', 'stocks', 0.5),
    ('JPM', 'stocks', 0.5),
    ('V', 'stocks', 0.5),
    ('MA', 'stocks', 0.5),
    ('JNJ', 'stocks', 0.5),
    ('PFE', 'stocks', 0.5),
    ('WMT', 'stocks', 0.5),
    ('PG', 'stocks', 0.5),
    ('KO', 'stocks', 0.5),
    ('XOM', 'stocks', 0.5),
    ('CVX', 'stocks', 0.5),
]

# ===== TRADING BOT INTEGRATION =====
trading_bot = None

def get_trading_bot():
    """Get or create trading bot instance"""
    global trading_bot
    if trading_bot is None:
        try:
            from trading_system import UltimateTradingSystem
            trading_bot = UltimateTradingSystem(account_balance=args.balance, no_telegram=True)
            print("✅ Trading bot loaded for signals")
        except Exception as e:
            print(f"⚠️ Could not load trading bot: {e}")
            trading_bot = None
    return trading_bot

def get_real_signal(asset: str, category: str):
    """Get REAL signal from trading bot"""
    try:
        bot = get_trading_bot()
        if bot is None:
            return None
        
        if not MarketHours.get_status().get(category, False):
            return {
                'asset': asset,
                'category': category,
                'signal': 'CLOSED',
                'confidence': 0,
                'entry_price': 0,
                'stop_loss': 0,
                'take_profit_levels': [],
                'risk_pct': 0,
                'reason': f"Market Closed",
                'market_open': False
            }
        
        price, source = fetcher.get_real_time_price(asset, category)
        if not price or price <= 0:
            return None
        
        df = bot.fetch_historical_data(asset, 100, '15m')
        if df.empty:
            return None
        
        from indicators.technical import TechnicalIndicators
        df = TechnicalIndicators.add_all_indicators(df)
        
        if hasattr(bot, 'voting_engine'):
            signals = bot.voting_engine.get_all_signals(df)
            combined = bot.voting_engine.weighted_vote(signals)
            
            if combined and combined.get('signal') != 'HOLD':
                atr = df['atr'].iloc[-1] if 'atr' in df.columns else price * 0.01
                
                if combined['signal'] == 'BUY':
                    stop_loss = price - (atr * 1.5)
                    tp1 = price + (atr * 2)
                    tp2 = price + (atr * 3)
                    tp3 = price + (atr * 4)
                else:
                    stop_loss = price + (atr * 1.5)
                    tp1 = price - (atr * 2)
                    tp2 = price - (atr * 3)
                    tp3 = price - (atr * 4)
                
                return {
                    'asset': asset,
                    'category': category,
                    'signal': combined['signal'],
                    'confidence': round(combined.get('confidence', 0.7), 2),
                    'entry_price': round(price, 5),
                    'stop_loss': round(stop_loss, 5),
                    'take_profit_levels': [
                        {'level': 1, 'price': round(tp1, 5)},
                        {'level': 2, 'price': round(tp2, 5)},
                        {'level': 3, 'price': round(tp3, 5)}
                    ],
                    'risk_pct': round(abs(price - stop_loss) / price * 100, 2),
                    'timestamp': datetime.now().isoformat(),
                    'generated_at': datetime.now().strftime('%H:%M:%S'),
                    'expires_at': (datetime.now() + timedelta(minutes=5)).isoformat(),
                    'time_remaining': 5.0,
                    'reason': combined.get('reason', 'Signal from voting engine'),
                    'market_open': True,
                    'data_source': source,
                    'strategy': 'VOTING',
                    'contributing_strategies': combined.get('contributing_strategies', [])
                }
        
        return None
        
    except Exception as e:
        print(f"❌ Error getting real signal for {asset}: {e}")
        return None

def refresh_signals():
    """Refresh all signals with REAL data from trading bot"""
    print(f"\n🔄 Fetching REAL trading signals...")
    
    status = MarketHours.get_status()
    if status['is_weekend']:
        print(f"   WEEKEND MODE: Only crypto markets")
        assets_to_process = [a for a in ALL_ASSETS if a[1] == 'crypto']
    else:
        assets_to_process = ALL_ASSETS
    
    print(f"   Processing {len(assets_to_process)} assets")
    
    signals = []
    
    for asset, category, _ in assets_to_process:
        try:
            if not MarketHours.get_status().get(category, False):
                signals.append({
                    'asset': asset,
                    'category': category,
                    'signal': 'CLOSED',
                    'confidence': 0,
                    'entry_price': 0,
                    'stop_loss': 0,
                    'take_profit_levels': [],
                    'risk_pct': 0,
                    'timestamp': datetime.now().isoformat(),
                    'generated_at': datetime.now().strftime('%H:%M:%S'),
                    'expires_at': (datetime.now() + timedelta(minutes=5)).isoformat(),
                    'time_remaining': 5.0,
                    'reason': f"Market Closed",
                    'market_open': False,
                    'data_source': 'N/A'
                })
                continue
            
            signal = get_real_signal(asset, category)
            if signal:
                signals.append(signal)
                if signal['signal'] != 'HOLD':
                    print(f"  ✅ {asset}: {signal['signal']} @ ${signal['entry_price']:.2f} (conf: {signal['confidence']:.0%})")
            
        except Exception as e:
            print(f"  ❌ Error processing {asset}: {e}")
    
    signals.sort(key=lambda x: (-x.get('confidence', 0) if x.get('market_open') else -1))
    
    active = len([s for s in signals if s.get('market_open') and s.get('signal') != 'HOLD'])
    closed = len([s for s in signals if not s.get('market_open')])
    
    print(f"\n✅ Generated {active} active signals, {closed} markets closed")
    
    return signals

def auto_refresh_worker():
    """Background worker"""
    global signals_cache
    
    while True:
        try:
            if not signals_cache['is_updating']:
                signals_cache['is_updating'] = True
                fresh_signals = refresh_signals()
                signals_cache['signals'] = fresh_signals
                signals_cache['last_refresh'] = datetime.now()
                signals_cache['is_updating'] = False
                
                status = MarketHours.get_status()
                if status['is_weekend']:
                    print("🏦 WEEKEND MODE: Forex/Stocks/Indices Closed")
            
        except Exception as e:
            print(f"❌ Error in auto_refresh: {e}")
            signals_cache['is_updating'] = False
        
        time.sleep(30)

# ===== HUMAN RESPONSE GENERATOR =====
def generate_human_response(asset: str, df, prediction: Dict, news: List, whale_info: str = None) -> Dict:
    """Generate a human-like response"""
    try:
        from human_explainer_db import DatabaseExplainer
        
        bot = get_trading_bot()
        if bot is None:
            return None
        
        explainer = DatabaseExplainer(bot)
        reasons = explainer._get_technical_reasons(df, prediction)
        
        clean_reasons = []
        for r in reasons:
            r = r.replace('**', '').replace('**', '')
            clean_reasons.append(r)
        
        setup_type = "breakout" if any('breakout' in r.lower() for r in clean_reasons) else "pullback"
        historical_context = explainer.personality.get_historical_context(asset, setup_type)
        mood = explainer.personality.current_mood
        
        current_price = float(df['close'].iloc[-1])
        
        return {
            'direction': prediction.get('direction', 'HOLD'),
            'confidence': prediction.get('confidence', 0.5),
            'current_price': current_price,
            'predicted_price': prediction.get('predicted_price'),
            'stop_loss': current_price * 0.995 if prediction.get('direction') == 'UP' else current_price * 1.005,
            'take_profit': current_price * 1.01 if prediction.get('direction') == 'UP' else current_price * 0.99,
            'reasons': clean_reasons[:5],
            'news': [{'title': a.get('title', '')} for a in news[:3]],
            'whale_alerts': whale_info,
            'historical_context': historical_context,
            'mood': mood.get('name', 'neutral'),
            'mood_emoji': mood.get('emoji', '😐'),
            'timestamp': datetime.now().isoformat()
        }
        
    except Exception as e:
        print(f"Error generating human response: {e}")
        return None

# ===== FLASK ROUTES =====
@app.route('/')
def index():
    return render_template('index_live.html')

@app.route('/api/signals/live', methods=['GET'])
def get_live_signals():
    try:
        current_time = datetime.now()
        
        for signal in signals_cache['signals']:
            if 'timestamp' in signal:
                try:
                    signal_time = datetime.fromisoformat(signal['timestamp'])
                    age_minutes = (current_time - signal_time).seconds / 60
                    signal['time_remaining'] = max(0, 5 - age_minutes)
                except:
                    signal['time_remaining'] = 5.0
        
        valid_signals = [s for s in signals_cache['signals'] if s.get('time_remaining', 0) > 0]
        
        filter_type = request.args.get('filter', 'all')
        if filter_type == 'buy':
            valid_signals = [s for s in valid_signals if s.get('signal') == 'BUY']
        elif filter_type == 'sell':
            valid_signals = [s for s in valid_signals if s.get('signal') == 'SELL']
        elif filter_type == 'high-confidence':
            valid_signals = [s for s in valid_signals if s.get('confidence', 0) >= 0.7]
        
        open_signals = [s for s in valid_signals if s.get('market_open', False) and s.get('signal') != 'HOLD']
        buy_signals = len([s for s in open_signals if s.get('signal') == 'BUY'])
        sell_signals = len([s for s in open_signals if s.get('signal') == 'SELL'])
        
        avg_confidence = 0
        if open_signals:
            confidences = [s.get('confidence', 0) for s in open_signals]
            avg_confidence = sum(confidences) / len(confidences)
        
        next_refresh = 30
        if signals_cache['last_refresh']:
            elapsed = (current_time - signals_cache['last_refresh']).seconds
            next_refresh = max(0, 30 - elapsed)
        
        return jsonify({
            'success': True,
            'signals': valid_signals,
            'total_signals': len(open_signals),
            'buy_signals': buy_signals,
            'sell_signals': sell_signals,
            'avg_confidence': round(avg_confidence * 100, 1),
            'market_status': MarketHours.get_status(),
            'last_refresh': signals_cache['last_refresh'].isoformat() if signals_cache['last_refresh'] else None,
            'last_update': signals_cache['last_refresh'].strftime('%H:%M:%S') if signals_cache['last_refresh'] else '--:--:--',
            'next_refresh_in': next_refresh,
            'is_updating': signals_cache['is_updating']
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/settings/update', methods=['POST'])
def update_settings():
    try:
        data = request.json
        if 'interval' in data:
            signals_cache['settings']['interval'] = data['interval']
        if 'balance' in data:
            signals_cache['settings']['balance'] = float(data['balance'])
        if 'risk' in data:
            signals_cache['settings']['risk'] = float(data['risk'])
        
        signals_cache['is_updating'] = False
        return jsonify({'success': True})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/refresh/manual', methods=['POST'])
def manual_refresh():
    signals_cache['is_updating'] = False
    return jsonify({'success': True})

@app.route('/api/status', methods=['GET'])
def get_status():
    return jsonify({
        'market_status': MarketHours.get_status(),
        'last_refresh': signals_cache['last_refresh'].isoformat() if signals_cache['last_refresh'] else None,
        'current_settings': signals_cache['settings']
    })

# ===== ASSET SIGNAL API (RETURNS BOTH FORMATS) =====
@app.route('/api/signal/<path:asset>')
def get_signal(asset):
    """Get signal for any asset (returns both signal and human formats)"""
    try:
        from indicators.technical import TechnicalIndicators
        from whale_alert_manager import WhaleAlertManager
        
        asset = asset.upper().strip()
        
        ASSET_ALIASES = {
            'BITCOIN': 'BTC-USD',
            'BTC': 'BTC-USD',
            'ETHEREUM': 'ETH-USD',
            'ETH': 'ETH-USD',
            'BINANCE': 'BNB-USD',
            'BNB': 'BNB-USD',
            'SOLANA': 'SOL-USD',
            'SOL': 'SOL-USD',
            'XRP': 'XRP-USD',
            'RIPPLE': 'XRP-USD',
            'GOLD': 'XAU/USD',
            'SILVER': 'XAG/USD',
            'OIL': 'CL=F',
            'WTI': 'CL=F',
            'SP500': '^GSPC',
            'S&P': '^GSPC',
            'DOW': '^DJI',
            'NASDAQ': '^IXIC',
            'APPLE': 'AAPL',
            'MICROSOFT': 'MSFT',
            'GOOGLE': 'GOOGL',
            'AMAZON': 'AMZN',
            'TESLA': 'TSLA',
            'NVIDIA': 'NVDA',
            'META': 'META',
            'EURO': 'EUR/USD',
            'POUND': 'GBP/USD',
            'YEN': 'USD/JPY',
        }
        
        if asset in ASSET_ALIASES:
            asset = ASSET_ALIASES[asset]
        
        bot = get_trading_bot()
        if bot is None:
            return jsonify({
                'success': False,
                'error': 'Trading bot not available'
            })
        
        df = None
        for interval in ['15m', '1h', '1d']:
            df = bot.fetch_historical_data(asset, days=3, interval=interval)
            if df is not None and not df.empty:
                print(f"Got {len(df)} rows of {interval} data for {asset}")
                break
        
        if df is None or df.empty:
            return jsonify({
                'success': False,
                'error': f'No data found for {asset}'
            })
        
        df = TechnicalIndicators.add_all_indicators(df)
        prediction = bot.predictor.predict_next(df)
        
        news = []
        if hasattr(bot, 'sentiment_analyzer') and hasattr(bot.sentiment_analyzer, 'news_integrator'):
            try:
                news = bot.sentiment_analyzer.news_integrator.fetch_by_symbol(asset, limit=3)
            except:
                pass
        
        whale_alerts = None
        if '-USD' in asset or asset in ['BTC-USD', 'ETH-USD', 'BNB-USD', 'SOL-USD', 'XRP-USD']:
            try:
                whales = WhaleAlertManager()
                alerts = whales.get_alerts(min_value_usd=1000000)
                for alert in alerts[:2]:
                    if alert.get('symbol') in asset:
                        amount = alert.get('amount', 0)
                        symbol = alert.get('symbol', '')
                        value_m = alert['value_usd'] / 1_000_000
                        whale_alerts = f"{amount} {symbol} (${value_m:.1f}M) moved"
                        break
            except Exception as e:
                print(f"Whale error: {e}")
        
        current_price = float(df['close'].iloc[-1])
        
        # Signal format (clean)
        signal_response = {
            'direction': prediction.get('direction', 'HOLD'),
            'confidence': prediction.get('confidence', 0.5),
            'current_price': current_price,
            'predicted_price': prediction.get('predicted_price'),
            'stop_loss': current_price * 0.995 if prediction.get('direction') == 'UP' else current_price * 1.005,
            'take_profit': current_price * 1.01 if prediction.get('direction') == 'UP' else current_price * 0.99
        }
        
        # Human format (with personality)
        human_response = generate_human_response(asset, df, prediction, news, whale_alerts)
        
        return jsonify({
            'success': True,
            'signal': signal_response,
            'human_response': human_response
        })
        
    except Exception as e:
        print(f"Signal API error: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'error': str(e)
        })

# ===== STATUS MONITOR ROUTES =====
@app.route('/api/system-status')
def get_system_status():
    """Get complete system status"""
    try:
        open_positions = 0
        closed_positions = 0
        total_pnl = 0
        today_pnl = 0
        current_balance = signals_cache['settings'].get('balance', 30)
        
        try:
            from services.database_service import DatabaseService
            db = DatabaseService()
            
            if db.use_db:
                trades = db.get_recent_trades(100)
                
                for trade in trades:
                    if not trade.get('exit_time'):
                        open_positions += 1
                    else:
                        closed_positions += 1
                        total_pnl += trade.get('pnl', 0)
                        
                        if trade.get('exit_time'):
                            try:
                                exit_date = datetime.fromisoformat(trade['exit_time']).date()
                                if exit_date == datetime.now().date():
                                    today_pnl += trade.get('pnl', 0)
                            except:
                                pass
                
                current_balance = signals_cache['settings'].get('balance', 30) + total_pnl
        except Exception as e:
            print(f"⚠️ Database error: {e}")
        
        processes = {
            'Trading Bot': False,
            'Web Dashboard': True,
            'Database': 'Connected' if 'db' in locals() and db.use_db else 'Disconnected'
        }
        
        try:
            import subprocess
            result = subprocess.run(
                ['docker', 'ps', '--format', '{{.Names}}'],
                capture_output=True, text=True, timeout=2
            )
            if result.returncode == 0 and 'trading-bot' in result.stdout.lower():
                processes['Trading Bot'] = True
        except:
            processes['Trading Bot'] = True
        
        return jsonify({
            'success': True,
            'balance': round(current_balance, 2),
            'pnl': round(today_pnl, 2),
            'open_positions': open_positions,
            'closed_positions': closed_positions,
            'processes': processes,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/status')
def status_page():
    return render_template('status_dashboard.html')

@app.route('/sentiment')
def sentiment_dashboard():
    return render_template('sentiment_dashboard.html')

@app.route('/backtest')
def backtest_page():
    return render_template('backtest_visualizer.html')

@app.route('/api/sentiment/dashboard')
def api_sentiment_dashboard():
    try:
        from sentiment_analyzer import SentimentAnalyzer
        analyzer = SentimentAnalyzer()
        
        result = {
            'success': True,
            'overall_sentiment': 'Neutral',
            'score': 0,
            'fear_greed': {'value': 50, 'classification': 'Neutral', 'score': 0},
            'vix': {'value': 20, 'classification': 'Normal', 'score': 0},
            'article_count': 0,
            'sentiment_distribution': {'bullish': 0, 'neutral': 0, 'bearish': 0},
            'sources': {},
            'articles': [],
            'whale_alerts': []
        }
        
        market_sent = analyzer.get_comprehensive_sentiment('general')
        if market_sent:
            result['overall_sentiment'] = market_sent.get('interpretation', 'Neutral')
            result['score'] = market_sent.get('score', 0)
        
        fg = analyzer.fetch_fear_greed_index()
        if fg:
            result['fear_greed'] = {
                'value': fg.get('value', 50),
                'classification': fg.get('classification', 'Neutral'),
                'score': fg.get('score', 0)
            }
        
        vix = analyzer.fetch_vix()
        if vix:
            result['vix'] = {
                'value': vix.get('value', 20),
                'classification': vix.get('classification', 'Normal'),
                'score': vix.get('score', 0)
            }
        
        if hasattr(analyzer, 'news_integrator'):
            all_articles = analyzer.news_integrator.fetch_all_sources()
            result['articles'] = sorted(all_articles, key=lambda x: x.get('date', ''), reverse=True)[:20]
            result['article_count'] = len(result['articles'])
        
        whale = analyzer.fetch_whale_alerts(min_value_usd=1000000)
        result['whale_alerts'] = whale[:10]
        
        if result['articles']:
            bullish = sum(1 for a in result['articles'] if a.get('sentiment', 0) > 0.1)
            bearish = sum(1 for a in result['articles'] if a.get('sentiment', 0) < -0.1)
            neutral = len(result['articles']) - bullish - bearish
            result['sentiment_distribution'] = {
                'bullish': bullish,
                'neutral': neutral,
                'bearish': bearish
            }
        
        return jsonify(result)
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/market/events')
def api_market_events():
    try:
        from sentiment_analyzer import SentimentAnalyzer
        analyzer = SentimentAnalyzer()
        events = analyzer.get_market_events()
        return jsonify({'success': True, 'events': events})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
    
@app.route('/api/websocket/feed')
def get_websocket_feed():
    from websocket_dashboard import recent_transactions as ws_transactions
    return jsonify({
        'success': True,
        'transactions': list(ws_transactions),
        'count': len(ws_transactions)
    })


@app.route('/websocket-feed')
def websocket_feed_page():
    """WebSocket feed page"""
    return render_template('websocket_feed.html')

if __name__ == '__main__':
    print("\n" + "🚀"*60)
    print("🚀 REAL TRADING SIGNALS DASHBOARD")
    print("🚀"*60)
    print("\n✅ Features:")
    print("   • REAL signals from your trading bot")
    print("   • Actual VOTING engine decisions")
    print("   • Clean signal format and human responses")
    print("   • Asset Search Commander")
    print("   • Real stop losses and take profits")
    print(f"   • Tracking {len(ALL_ASSETS)} assets")
    print("   • Status at http://localhost:5000/status")
    
    refresh_thread = threading.Thread(target=auto_refresh_worker, daemon=True)
    refresh_thread.start()
    
    print("\n🚀 Dashboard starting!")
    print("📊 http://localhost:5000")
    print("🚀"*60 + "\n")

    # ===== START WEBSOCKET MANAGER IN-PROCESS =====
    def start_websocket_in_process():
        from websocket_manager import WebSocketManager
        from websocket_dashboard import add_transaction

        def ws_callback(source, symbol, price, volume, side, timestamp):
            add_transaction(source, symbol, price, volume, side)

        ws = WebSocketManager()
        ws.start()
        ws.subscribe_bybit(
            ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'SOLUSDT', 'XRPUSDT'],
            ws_callback
        )
        print("🚀 WebSocket manager running in-process")

    ws_thread = threading.Thread(target=start_websocket_in_process, daemon=True)
    ws_thread.start()
    # ================================================
    
    app.run(debug=True, host='0.0.0.0', port=5000, threaded=True)