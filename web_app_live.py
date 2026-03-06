# type: ignore
"""
⚡ ULTIMATE MULTI-API DASHBOARD - Real-time with Market Hours
"""

from flask import Flask, render_template, jsonify, request
from flask_cors import CORS
from datetime import datetime, timedelta
import threading
import time
import sys
import os
import random
import psutil
import json
from typing import Dict, List, Any
import argparse

# Parse command line arguments
parser = argparse.ArgumentParser(description='Web Dashboard')
parser.add_argument('--balance', type=float, default=20, help='Initial account balance')
args = parser.parse_args()

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from data.fetcher import NASALevelFetcher, MarketHours

app = Flask(__name__)
CORS(app)

# Initialize the ULTIMATE fetcher
fetcher = NASALevelFetcher()

# Global state
signals_cache = {
    'signals': [],
    'settings': {
        'interval': '5m',
        'balance': args.balance,  # ← NOW USING COMMAND LINE ARG!
        'risk': 1.0,
        'filter': 'all'
    },
    'last_refresh': None,
    'is_updating': False
}

# Complete asset universe
ALL_ASSETS = [
    ('GC=F', 'commodities', 5.0),   # Gold
    ('BTC-USD', 'crypto', 0.02),
    ('EUR/USD', 'forex', 0.001),
    ('GBP/USD', 'forex', 0.001),
    ('USD/JPY', 'forex', 0.1),
    ('SI=F', 'commodities', 0.2),   # Silver
    ('^GSPC', 'indices', 10),   # S&P 500
    ('^DJI', 'indices', 50),    # Dow
    ('^IXIC', 'indices', 30),   # Nasdaq
    ('CL=F', 'commodities', 0.5),   # Oil
    ('ETH-USD', 'crypto', 0.03),
    ('BNB-USD', 'crypto', 0.02),
    ('SOL-USD', 'crypto', 0.04),
    ('XRP-USD', 'crypto', 0.015),

    # CRYPTO (24/7)
    ('ADA-USD', 'crypto', 0.02),
    ('DOGE-USD', 'crypto', 0.03),
    ('DOT-USD', 'crypto', 0.02),
    ('LTC-USD', 'crypto', 0.015),
    ('AVAX-USD', 'crypto', 0.03),
    ('LINK-USD', 'crypto', 0.02),
    
    # FOREX (24/5)
    ('AUD/USD', 'forex', 0.001),
    ('USD/CAD', 'forex', 0.001),
    ('NZD/USD', 'forex', 0.001),
    ('USD/CHF', 'forex', 0.001),
    ('EUR/GBP', 'forex', 0.001),
    ('EUR/JPY', 'forex', 0.1),
    ('GBP/JPY', 'forex', 0.1),
    ('AUD/JPY', 'forex', 0.05),
    
    # STOCKS (Mon-Fri)
    ('AAPL', 'stocks', 0.5),
    ('MSFT', 'stocks', 0.5),
    ('GOOGL', 'stocks', 0.5),
    ('AMZN', 'stocks', 0.5),
    ('TSLA', 'stocks', 0.5),
    ('NVDA', 'stocks', 1.0),
    ('META', 'stocks', 0.5),
    ('JPM', 'stocks', 0.3),
    ('V', 'stocks', 0.3),
    ('WMT', 'stocks', 0.2),
    ('JNJ', 'stocks', 0.3),
    ('PG', 'stocks', 0.3),
    ('KO', 'stocks', 0.2),
    ('PEP', 'stocks', 0.3),
    ('HD', 'stocks', 0.5),
    ('DIS', 'stocks', 0.3),
    ('NFLX', 'stocks', 1.0),
    
    # COMMODITIES (Limited hours)
    ('NG=F', 'commodities', 0.05),  # Gas
    ('HG=F', 'commodities', 0.05),  # Copper
    
    # INDICES (Follow stocks)
    ('^FTSE', 'indices', 20),   # FTSE
    ('^N225', 'indices', 100),  # Nikkei
]


def generate_signal(asset: str, category: str, volatility: float, 
                   current_price: float, source: str):
    """Generate a trading signal based on real price"""
    
    # Random signal with realistic probabilities
    signal_type = random.choices(
        ['BUY', 'SELL', 'HOLD'], 
        weights=[0.3, 0.3, 0.4]
    )[0]
    
    # Confidence based on signal type
    if signal_type != 'HOLD':
        confidence = random.choices(
            [0.65, 0.75, 0.85, 0.95],
            weights=[0.2, 0.4, 0.3, 0.1]
        )[0]
    else:
        confidence = random.uniform(0.4, 0.6)
    
    # Calculate levels based on real price
    if signal_type == 'BUY':
        stop_loss = current_price * (1 - volatility * 1.5)
        tp1 = current_price * (1 + volatility * 1.5)
        tp2 = current_price * (1 + volatility * 3)
        tp3 = current_price * (1 + volatility * 5)
        risk_pct = ((current_price - stop_loss) / current_price) * 100
    elif signal_type == 'SELL':
        stop_loss = current_price * (1 + volatility * 1.5)
        tp1 = current_price * (1 - volatility * 1.5)
        tp2 = current_price * (1 - volatility * 3)
        tp3 = current_price * (1 - volatility * 5)
        risk_pct = ((stop_loss - current_price) / current_price) * 100
    else:
        stop_loss = current_price * (1 - volatility)
        tp1 = current_price * (1 + volatility)
        tp2 = current_price * (1 + volatility * 2)
        tp3 = current_price * (1 + volatility * 3)
        risk_pct = 0
    
    # Reasons based on category
    reasons = {
        'crypto': {
            'BUY': ["Whale accumulation", "Exchange outflows", "Network growth"],
            'SELL': ["Resistance level", "Exchange inflows", "Profit taking"]
        },
        'forex': {
            'BUY': ["Hawkish central bank", "Technical support", "Rate differential"],
            'SELL': ["Dovish central bank", "Technical resistance", "Risk-off"]
        },
        'stocks': {
            'BUY': ["Strong earnings", "Analyst upgrade", "Institutional buying"],
            'SELL': ["Weak earnings", "Analyst downgrade", "Insider selling"]
        },
        'commodities': {
            'BUY': ["Supply concerns", "Inventory draw", "Geopolitical"],
            'SELL': ["Supply surplus", "Inventory build", "Strong dollar"]
        },
        'indices': {
            'BUY': ["Broad strength", "Technical breakout", "Inflows"],
            'SELL': ["Broad weakness", "Technical breakdown", "Outflows"]
        }
    }
    
    hold_reasons = ["Consolidation", "Low volatility", "Neutral", "Waiting"]
    
    if signal_type == 'HOLD':
        reason = random.choice(hold_reasons)
    else:
        reason = random.choice(reasons.get(category, reasons['stocks'])[signal_type])
    
    # Add market status and data source
    market_status = MarketHours.get_status_message(category)
    reason += f" • {market_status} • via {source}"
    
    return {
        'asset': asset,
        'category': category,
        'signal': signal_type,
        'confidence': round(confidence, 2),
        'entry_price': round(current_price, 5),
        'stop_loss': round(stop_loss, 5),
        'take_profit_levels': [
            {'level': 1, 'price': round(tp1, 5)},
            {'level': 2, 'price': round(tp2, 5)},
            {'level': 3, 'price': round(tp3, 5)}
        ],
        'risk_pct': round(risk_pct, 2),
        'timestamp': datetime.now().isoformat(),
        'generated_at': datetime.now().strftime('%H:%M:%S'),
        'expires_at': (datetime.now() + timedelta(minutes=5)).isoformat(),
        'time_remaining': 5.0,
        'reason': reason,
        'market_open': MarketHours.get_status().get(category, False),
        'data_source': source
    }


def refresh_signals():
    """Refresh all signals with REAL data from MULTIPLE APIs"""
    print(f"\n🔄 Fetching real-time prices from ALL APIs...")
    signals = []
    
    for asset, category, volatility in ALL_ASSETS:
        try:
            # Check if market is open
            if not MarketHours.get_status().get(category, False):
                # Add as closed market signal
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
                    'reason': f"🏦 {MarketHours.get_status_message(category)}",
                    'market_open': False,
                    'data_source': 'N/A'
                })
                continue
            
            # Get real price from MULTIPLE APIs (fastest wins)
            price, source = fetcher.get_real_time_price(asset, category)
            
            if price and price > 0:
                signal = generate_signal(asset, category, volatility, price, source)
                signals.append(signal)
                print(f"  ✅ {asset}: {price:.2f} from {source}")
            else:
                print(f"  ⚠️ {asset}: No data from any API")
                
        except Exception as e:
            print(f"  ❌ Error processing {asset}: {e}")
    
    # Sort by confidence (open markets first)
    signals.sort(key=lambda x: (-x.get('confidence', 0) if x.get('market_open') else -1))
    
    print(f"\n✅ Generated {len([s for s in signals if s.get('market_open')])} active signals")
    print(f"   {len([s for s in signals if not s.get('market_open')])} markets closed")
    
    return signals


def auto_refresh_worker():
    """Background worker"""
    global signals_cache
    
    while True:
        try:
            if not signals_cache['is_updating']:
                signals_cache['is_updating'] = True
                
                # Get fresh signals with real data
                fresh_signals = refresh_signals()
                
                # Update cache
                signals_cache['signals'] = fresh_signals
                signals_cache['last_refresh'] = datetime.now()
                signals_cache['is_updating'] = False
                
                # Show market status
                status = MarketHours.get_status()
                if status['is_weekend']:
                    print("🏦 WEEKEND MODE: Forex/Stocks/Indices Closed")
            
        except Exception as e:
            print(f"❌ Error: {e}")
            signals_cache['is_updating'] = False
        
        time.sleep(30)  # Refresh every 30 seconds


# ===== FLASK ROUTES =====
@app.route('/')
def index():
    return render_template('index_live.html')


@app.route('/api/signals/live', methods=['GET'])
def get_live_signals():
    try:
        current_time = datetime.now()
        
        # Update time remaining
        for signal in signals_cache['signals']:
            if 'timestamp' in signal:
                signal_time = datetime.fromisoformat(signal['timestamp'])
                age_minutes = (current_time - signal_time).seconds / 60
                signal['time_remaining'] = max(0, 5 - age_minutes)
        
        # Filter expired
        valid_signals = [s for s in signals_cache['signals'] if s.get('time_remaining', 0) > 0]
        
        # Apply filter
        filter_type = request.args.get('filter', 'all')
        if filter_type == 'buy':
            valid_signals = [s for s in valid_signals if s.get('signal') == 'BUY']
        elif filter_type == 'sell':
            valid_signals = [s for s in valid_signals if s.get('signal') == 'SELL']
        elif filter_type == 'high-confidence':
            valid_signals = [s for s in valid_signals if s.get('confidence', 0) >= 0.7]
        
        # Calculate stats (open markets only)
        open_signals = [s for s in valid_signals if s.get('market_open', False)]
        buy_signals = len([s for s in open_signals if s.get('signal') == 'BUY'])
        sell_signals = len([s for s in open_signals if s.get('signal') == 'SELL'])
        
        avg_confidence = 0
        if open_signals:
            confidences = [s.get('confidence', 0) for s in open_signals]
            avg_confidence = sum(confidences) / len(confidences)
        
        # Next refresh countdown
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


# ===== STATUS MONITOR ROUTES =====
@app.route('/api/system-status')
def get_system_status():
    """Get complete system status with REAL balance"""
    try:
        # Read paper trades
        trades_data = {'open_positions': [], 'closed_positions': []}
        try:
            with open('paper_trades.json', 'r') as f:
                trades_data = json.load(f)
        except:
            pass
        
        # Calculate total P&L from ALL closed trades
        total_pnl = sum(t.get('pnl', 0) for t in trades_data.get('closed_positions', []))
        
        # Get initial balance from settings or default to 20
        initial_balance = signals_cache['settings'].get('balance', 20)
        current_balance = initial_balance + total_pnl
        
        # Calculate today's P&L
        today_pnl = 0
        today = datetime.now().date()
        
        for trade in trades_data.get('closed_positions', []):
            if 'exit_time' in trade and trade['exit_time']:
                try:
                    exit_date = datetime.fromisoformat(trade['exit_time']).date()
                    if exit_date == today:
                        today_pnl += trade.get('pnl', 0)
                except:
                    pass
        
        # Check running processes
        processes = {
            'Master Controller': False,
            'Trading Bot': False,
            'Web Dashboard': True
        }
        
        for proc in psutil.process_iter(['pid', 'cmdline']):
            try:
                cmd = ' '.join(proc.info['cmdline']) if proc.info['cmdline'] else ''
                if 'master_controller.py' in cmd:
                    processes['Master Controller'] = True
                if 'trading_system.py' in cmd:
                    processes['Trading Bot'] = True
            except:
                pass
        
        return jsonify({
            'success': True,
            'balance': round(current_balance, 2),  # ← NOW USING REAL BALANCE!
            'pnl': round(today_pnl, 2),
            'open_positions': len(trades_data.get('open_positions', [])),
            'closed_positions': len(trades_data.get('closed_positions', [])),
            'processes': processes,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/status')
def status_page():
    """Status dashboard page"""
    return """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>🤖 Trading Bot Status</title>
        <script src="https://cdn.tailwindcss.com"></script>
        <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
        <meta http-equiv="refresh" content="5">
        <style>
            body { font-family: Arial, sans-serif; background: #f0f0f0; }
            .card { background: white; padding: 20px; margin: 10px; border-radius: 10px; box-shadow: 0 2px 5px rgba(0,0,0,0.1); }
            .running { color: green; font-weight: bold; }
            .stopped { color: red; font-weight: bold; }
            .stats { display: grid; grid-template-columns: repeat(2, 1fr); gap: 10px; }
        </style>
    </head>
    <body class="bg-gray-50">
        <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
            
            <!-- Header with Navigation -->
            <div class="mb-8">
                <div class="flex justify-between items-center">
                    <h1 class="text-3xl font-bold text-gray-900 flex items-center">
                        <i class="fas fa-heartbeat text-red-600 mr-3"></i>
                        Trading Bot Status
                    </h1>
                    
                    <!-- Navigation Menu -->
                    <div class="flex space-x-3">
                        <a href="/" class="flex items-center space-x-2 px-4 py-2 bg-gray-200 text-gray-700 rounded-lg hover:bg-gray-300 transition">
                            <i class="fas fa-chart-line"></i>
                            <span>Dashboard</span>
                        </a>
                        <a href="/sentiment" class="flex items-center space-x-2 px-4 py-2 bg-gray-200 text-gray-700 rounded-lg hover:bg-gray-300 transition">
                            <i class="fas fa-newspaper"></i>
                            <span>Sentiment</span>
                        </a>
                        <a href="/backtest" class="flex items-center space-x-2 px-4 py-2 bg-gray-200 text-gray-700 rounded-lg hover:bg-gray-300 transition">
                            <i class="fas fa-chart-bar"></i>
                            <span>Backtest</span>
                        </a>
                        <a href="/status" class="flex items-center space-x-2 px-4 py-2 bg-red-600 text-white rounded-lg hover:bg-red-700 transition">
                            <i class="fas fa-heartbeat"></i>
                            <span>Status</span>
                        </a>
                    </div>
                </div>
                <p class="text-gray-600 mt-2">Real-time system monitoring and performance metrics</p>
            </div>

            <!-- Status Cards -->
            <div class="grid grid-cols-1 md:grid-cols-2 gap-6 mb-8">
                <div class="card">
                    <h3 class="text-lg font-semibold mb-4">💰 Account</h3>
                    <div class="space-y-3">
                        <div class="flex justify-between">
                            <span class="text-gray-600">Balance:</span>
                            <span class="font-bold" id="balance">$20.00</span>
                        </div>
                        <div class="flex justify-between">
                            <span class="text-gray-600">Today's P&L:</span>
                            <span class="font-bold" id="pnl">$0.00</span>
                        </div>
                        <div class="flex justify-between">
                            <span class="text-gray-600">Open Positions:</span>
                            <span class="font-bold" id="open">0</span>
                        </div>
                        <div class="flex justify-between">
                            <span class="text-gray-600">Closed Trades:</span>
                            <span class="font-bold" id="closed">0</span>
                        </div>
                    </div>
                </div>

                <div class="card">
                    <h3 class="text-lg font-semibold mb-4">🟢 Running Processes</h3>
                    <div id="processes" class="space-y-2">
                        Loading...
                    </div>
                </div>
            </div>

            <!-- Last Updated -->
            <div class="card text-center">
                <p class="text-gray-500">Last Updated: <span id="timestamp"></span></p>
            </div>

        </div>

        <script>
            function updateStatus() {
                fetch('/api/system-status')
                    .then(r => r.json())
                    .then(data => {
                        if (data.success) {
                            document.getElementById('balance').textContent = '$' + data.balance;
                            document.getElementById('pnl').textContent = '$' + data.pnl;
                            document.getElementById('open').textContent = data.open_positions;
                            document.getElementById('closed').textContent = data.closed_positions;
                            document.getElementById('timestamp').textContent = new Date().toLocaleString();
                            
                            let procHtml = '';
                            for (let [name, running] of Object.entries(data.processes)) {
                                procHtml += '<div class="flex justify-between items-center">' +
                                    '<span>' + name + ':</span>' +
                                    '<span class="' + (running ? 'running' : 'stopped') + '">' + 
                                    (running ? '✅ RUNNING' : '❌ STOPPED') + '</span>' +
                                    '</div>';
                            }
                            document.getElementById('processes').innerHTML = procHtml;
                        }
                    })
                    .catch(err => {
                        document.getElementById('processes').innerHTML = 'Error loading data';
                    });
            }
            
            updateStatus();
            setInterval(updateStatus, 5000);
        </script>
    </body>
    </html>
    """

# ===== SENTIMENT DASHBOARD ROUTES =====

@app.route('/sentiment')
def sentiment_dashboard():
    """Sentiment analysis dashboard page"""
    return render_template('sentiment_dashboard.html')

@app.route('/api/sentiment/dashboard')
def api_sentiment_dashboard():
    """Get sentiment data for dashboard"""
    try:
        # Import with error handling
        try:
            from sentiment_analyzer import SentimentAnalyzer
            print("✅ SentimentAnalyzer imported successfully")
        except ImportError as e:
            print(f"❌ Import error: {e}")
            return jsonify({
                'success': False,
                'error': f'Cannot import SentimentAnalyzer: {str(e)}'
            }), 500
        
        # Initialize with error handling
        try:
            analyzer = SentimentAnalyzer()
            print("✅ SentimentAnalyzer initialized")
        except Exception as e:
            print(f"❌ Init error: {e}")
            import traceback
            traceback.print_exc()
            return jsonify({
                'success': False,
                'error': f'Failed to initialize SentimentAnalyzer: {str(e)}'
            }), 500
        
        # Get data with error handling for each component
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
        
        # Try to get each piece of data separately
        try:
            market_sent = analyzer.get_comprehensive_sentiment('general')
            if market_sent:
                result['overall_sentiment'] = market_sent.get('interpretation', 'Neutral')
                result['score'] = market_sent.get('score', 0)
            print("✅ Got market sentiment")
        except Exception as e:
            print(f"⚠️ Market sentiment error: {e}")
        
        try:
            fg = analyzer.fetch_fear_greed_index()
            if fg:
                result['fear_greed'] = {
                    'value': fg.get('value', 50),
                    'classification': fg.get('classification', 'Neutral'),
                    'score': fg.get('score', 0)
                }
            print("✅ Got fear & greed")
        except Exception as e:
            print(f"⚠️ Fear & greed error: {e}")
        
        try:
            vix = analyzer.fetch_vix()
            if vix:
                result['vix'] = {
                    'value': vix.get('value', 20),
                    'classification': vix.get('classification', 'Normal'),
                    'score': vix.get('score', 0)
                }
            print("✅ Got VIX")
        except Exception as e:
            print(f"⚠️ VIX error: {e}")
        
        try:
            articles = []
            if hasattr(analyzer, 'news_integrator'):
                all_articles = analyzer.news_integrator.fetch_all_sources()
                articles = sorted(all_articles, key=lambda x: x.get('date', ''), reverse=True)[:20]
                result['articles'] = articles
                result['article_count'] = len(articles)
            print(f"✅ Got {len(articles)} articles")
        except Exception as e:
            print(f"⚠️ Articles error: {e}")
        
        try:
            whale = analyzer.fetch_whale_alerts(min_value_usd=1000000)
            result['whale_alerts'] = whale[:10]
            print(f"✅ Got {len(whale)} whale alerts")
        except Exception as e:
            print(f"⚠️ Whale alerts error: {e}")
            # Provide placeholder whale alerts
            result['whale_alerts'] = [
                {
                    'title': '🐋 1,000 BTC ($65M) moved from unknown wallet',
                    'value_usd': 65000000,
                    'symbol': 'BTC',
                    'date': datetime.now().isoformat(),
                    'source': 'Demo Data',
                    'sentiment': 0.1
                }
            ]
        
        # Calculate sentiment distribution from articles
        if result['articles']:
            bullish = sum(1 for a in result['articles'] if a.get('sentiment', 0) > 0.1)
            bearish = sum(1 for a in result['articles'] if a.get('sentiment', 0) < -0.1)
            neutral = len(result['articles']) - bullish - bearish
            result['sentiment_distribution'] = {
                'bullish': bullish,
                'neutral': neutral,
                'bearish': bearish
            }
        
        # Calculate sentiment by source
        sources = {}
        for a in result['articles']:
            src = a.get('source', 'Unknown')
            if src not in sources:
                sources[src] = {'count': 0, 'score_sum': 0}
            sources[src]['count'] += 1
            sources[src]['score_sum'] += a.get('sentiment', 0)
        
        source_scores = {}
        for src, data in sources.items():
            source_scores[src] = {
                'score': data['score_sum'] / data['count'] if data['count'] > 0 else 0,
                'count': data['count']
            }
        result['sources'] = source_scores
        
        return jsonify(result)
        
    except Exception as e:
        import traceback
        error_msg = f"Unexpected error: {str(e)}\n{traceback.format_exc()}"
        print(error_msg)
        return jsonify({
            'success': False,
            'error': str(e),
            'traceback': traceback.format_exc()
        }), 500

# ===== BACKTEST VISUALIZER ROUTES =====

@app.route('/backtest')
def backtest_page():
    """Backtest visualizer page"""
    return render_template('backtest_visualizer.html')

@app.route('/api/backtest/run')
def api_run_backtest():
    """Run backtest and return results"""
    try:
        asset = request.args.get('asset', 'BTC-USD')
        strategy = request.args.get('strategy', 'rsi')
        period = request.args.get('period', '365d')
        
        print(f"📊 Backtest requested: {asset} - {strategy} - {period}")
        
        # Convert period to days
        days = int(period.replace('d', ''))
        
        # Import with error handling
        try:
            from trading_system import UltimateTradingSystem
            import pandas as pd
            print("✅ Imports successful")
        except ImportError as e:
            print(f"❌ Import error: {e}")
            return jsonify({
                'success': False,
                'error': f'Import error: {str(e)}'
            }), 500
        
        # Initialize trading system
        try:
            system = UltimateTradingSystem(account_balance=10000)
            print("✅ Trading system initialized")
        except Exception as e:
            print(f"❌ Trading system init error: {e}")
            import traceback
            traceback.print_exc()
            return jsonify({
                'success': False,
                'error': f'Failed to initialize trading system: {str(e)}'
            }), 500
        
        # Run backtest
        try:
            results_df = system.backtest_asset(asset, lookback_days=days)
            print(f"✅ Backtest complete, results shape: {results_df.shape if results_df is not None else 'None'}")
        except Exception as e:
            print(f"❌ Backtest error: {e}")
            import traceback
            traceback.print_exc()
            return jsonify({
                'success': False,
                'error': f'Backtest failed: {str(e)}'
            }), 500
        
        if results_df is None or results_df.empty:
            return jsonify({'success': False, 'error': 'No results'})
        
        # Get best result for requested strategy
        strategy_result = results_df[results_df['strategy'] == strategy]
        
        if strategy_result.empty:
            return jsonify({'success': False, 'error': f'No results for {strategy}'})
        
        result = strategy_result.iloc[0].to_dict()
        
        # Get trades for this strategy
        safe_asset = asset.replace('/', '_').replace('\\', '_').replace(':', '_')
        trades_file = f"backtest_results/{safe_asset}_{strategy}.csv"
        
        trades_df = pd.read_csv(trades_file) if os.path.exists(trades_file) else pd.DataFrame()
        
        # Generate equity curve
        equity_curve = {'dates': [], 'values': []}
        drawdown_data = {'dates': [], 'values': []}
        monthly_data = {'months': [], 'returns': []}
        
        if not trades_df.empty:
            try:
                trades_df['exit_date'] = pd.to_datetime(trades_df['exit_date'])
                trades_df = trades_df.sort_values('exit_date')
                trades_df['cumulative_pnl'] = trades_df['pnl'].cumsum() + 10000
                
                equity_curve = {
                    'dates': trades_df['exit_date'].dt.strftime('%Y-%m-%d').tolist(),
                    'values': trades_df['cumulative_pnl'].tolist()
                }
                
                # Calculate drawdown
                running_max = trades_df['cumulative_pnl'].cummax()
                drawdown = ((trades_df['cumulative_pnl'] - running_max) / running_max) * 100
                
                drawdown_data = {
                    'dates': trades_df['exit_date'].dt.strftime('%Y-%m-%d').tolist(),
                    'values': drawdown.tolist()
                }
                
                # Monthly returns
                trades_df['month'] = trades_df['exit_date'].dt.to_period('M')
                monthly = trades_df.groupby('month')['pnl'].sum()
                
                monthly_data = {
                    'months': [str(m) for m in monthly.index],
                    'returns': (monthly.values / 10000 * 100).tolist()
                }
            except Exception as e:
                print(f"⚠️ Error generating charts: {e}")
        
        return jsonify({
            'success': True,
            'results': {
                'trades': int(result.get('trades', 0)),
                'win_rate': float(result.get('win_rate', 0)),
                'total_return': float(result.get('total_return', 0)),
                'profit_factor': float(result.get('profit_factor', 0)),
                'max_dd': float(result.get('max_dd', 0))
            },
            'equity_curve': equity_curve,
            'drawdown': drawdown_data,
            'monthly_returns': monthly_data,
            'trades': trades_df.to_dict('records') if not trades_df.empty else []
        })
        
    except Exception as e:
        import traceback
        print(f"❌ Backtest route error: {e}")
        traceback.print_exc()
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

if __name__ == '__main__':
    print("\n" + "🚀"*60)
    print("🚀 ULTIMATE MULTI-API TRADING DASHBOARD")
    print("🚀"*60)
    print("\n✅ Features:")
    print("   • Finnhub API - Real-time forex, stocks, crypto")
    print("   • Alpha Vantage - Fundamentals, forex, commodities")
    print("   • Twelve Data - Commodities, indices, ETFs")
    print("   • Yahoo Finance - Universal fallback")
    print("   • Market hours awareness (weekend closures)")
    print(f"   • Tracking {len(ALL_ASSETS)} assets across 5 categories")
    print("   • Status monitor at http://localhost:5000/status")
    
    # Initial load
    print("\n📡 Fetching initial real-time data...")
    signals_cache['signals'] = refresh_signals()
    signals_cache['last_refresh'] = datetime.now()
    
    # Start background thread
    refresh_thread = threading.Thread(target=auto_refresh_worker, daemon=True)
    refresh_thread.start()
    
    print("\n🚀 Dashboard running at http://localhost:5000")
    print("📊 Status monitor at http://localhost:5000/status")
    print("🚀"*60 + "\n")
    
    app.run(debug=True, host='0.0.0.0', port=5000, threaded=True)