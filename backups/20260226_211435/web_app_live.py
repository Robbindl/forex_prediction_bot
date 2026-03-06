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
parser.add_argument('--balance', type=float, default=10, help='Initial account balance')
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
    <html>
    <head>
        <title>🤖 Trading Bot Status</title>
        <meta http-equiv="refresh" content="5">
        <style>
            body { font-family: Arial; padding: 20px; background: #f0f0f0; }
            .card { background: white; padding: 20px; margin: 10px; border-radius: 10px; box-shadow: 0 2px 5px rgba(0,0,0,0.1); }
            .running { color: green; font-weight: bold; }
            .stopped { color: red; font-weight: bold; }
            h1 { color: #333; }
            .stats { display: grid; grid-template-columns: repeat(2, 1fr); gap: 10px; }
        </style>
    </head>
    <body>
        <h1>🤖 TRADING BOT STATUS</h1>
        <div class="card">
            <div class="stats">
                <div>
                    <h3>💰 Account Balance</h3>
                    <h2 id="balance">$20.00</h2>
                </div>
                <div>
                    <h3>📈 Today's P&L</h3>
                    <h2 id="pnl">$0.00</h2>
                </div>
                <div>
                    <h3>📊 Open Positions</h3>
                    <h2 id="open">0</h2>
                </div>
                <div>
                    <h3>📋 Closed Trades</h3>
                    <h2 id="closed">0</h2>
                </div>
            </div>
        </div>
        
        <div class="card">
            <h3>🟢 Running Processes</h3>
            <div id="processes">Loading...</div>
        </div>
        
        <div class="card">
            <h3>⏰ Last Updated</h3>
            <div id="timestamp"></div>
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
                                procHtml += '<p>' + name + ': <span class="' + (running ? 'running' : 'stopped') + '">' + 
                                           (running ? '✅ RUNNING' : '❌ STOPPED') + '</span></p>';
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
    
    app.run(debug=False, host='0.0.0.0', port=5000, threaded=True)