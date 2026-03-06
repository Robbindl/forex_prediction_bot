"""
Flask Web Application for Forex Prediction Bot
Modern web interface for trading signals and analysis
"""

from flask import Flask, render_template, jsonify, request
from flask_cors import CORS
import pandas as pd
import json
from datetime import datetime
import threading
import time

# Import bot components
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config.config import *
from data.fetcher import DataFetcher
from indicators.technical import TechnicalIndicators
from models.predictor import PredictionEngine
from utils.analysis import MarketAnalyzer, AlertSystem, ReportGenerator
from utils.trading_signals import TradingSignalGenerator

app = Flask(__name__)
CORS(app)

# Global cache for data
data_cache = {
    'signals': {},
    'market_data': {},
    'last_update': None,
    'is_updating': False
}

# Bot instance
fetcher = DataFetcher()
models = {}


@app.route('/')
def index():
    """Main dashboard page"""
    return render_template('index.html')


@app.route('/api/signals', methods=['GET'])
def get_signals():
    """Get current trading signals"""
    interval = request.args.get('interval', '1d')
    
    # Use cache if recent (< 5 minutes old)
    if data_cache['last_update']:
        time_diff = (datetime.now() - data_cache['last_update']).seconds
        if time_diff < 300 and data_cache['signals']:  # 5 minutes
            return jsonify({
                'success': True,
                'signals': data_cache['signals'],
                'last_update': data_cache['last_update'].isoformat(),
                'cached': True
            })
    
    # Fetch fresh data
    try:
        signals = fetch_trading_signals(interval)
        data_cache['signals'] = signals
        data_cache['last_update'] = datetime.now()
        
        return jsonify({
            'success': True,
            'signals': signals,
            'last_update': data_cache['last_update'].isoformat(),
            'cached': False
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/asset/<asset_name>', methods=['GET'])
def get_asset_details(asset_name):
    """Get detailed analysis for specific asset"""
    try:
        interval = request.args.get('interval', '1d')
        
        # Determine asset type
        asset_type = determine_asset_type(asset_name)
        
        # Fetch data
        if asset_type == 'forex':
            df = fetcher.fetch_forex_data(asset_name, interval, lookback=100)
        elif asset_type == 'stock':
            df = fetcher.fetch_stock_data(asset_name, interval, lookback=100)
        elif asset_type == 'commodity':
            df = fetcher.fetch_commodity_data(asset_name, interval, lookback=100)
        else:
            df = fetcher.fetch_index_data(asset_name, interval, lookback=100)
        
        if df.empty:
            return jsonify({'success': False, 'error': 'No data available'}), 404
        
        # Add indicators
        df = TechnicalIndicators.add_all_indicators(df)
        
        # Generate signal
        prediction = None
        if asset_name in models:
            prediction = models[asset_name].predict_next(df)
        
        signal = TradingSignalGenerator.generate_entry_signal(df, prediction)
        
        # Get price history for chart
        price_history = df[['close', 'sma_20', 'sma_50']].tail(50).reset_index()
        price_history['date'] = price_history['date'].astype(str)
        
        # Get recent indicators
        latest = df.iloc[-1]
        indicators = {
            'rsi': float(latest['rsi']) if 'rsi' in latest else None,
            'macd': float(latest['macd']) if 'macd' in latest else None,
            'macd_signal': float(latest['macd_signal']) if 'macd_signal' in latest else None,
            'adx': float(latest['adx']) if 'adx' in latest else None,
            'atr': float(latest['atr']) if 'atr' in latest else None,
            'volume': float(latest['volume']) if 'volume' in latest else None
        }
        
        # Get alerts
        alerts = AlertSystem.generate_all_alerts(df)
        
        return jsonify({
            'success': True,
            'asset': asset_name,
            'signal': signal,
            'indicators': indicators,
            'alerts': alerts,
            'price_history': price_history.to_dict('records'),
            'ml_prediction': prediction
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/position-size', methods=['POST'])
def calculate_position():
    """Calculate position size based on risk parameters"""
    try:
        data = request.json
        
        position = TradingSignalGenerator.calculate_position_size(
            account_balance=float(data['balance']),
            risk_percentage=float(data['risk']),
            entry_price=float(data['entry']),
            stop_loss=float(data['stop_loss'])
        )
        
        return jsonify({
            'success': True,
            'position': position
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/market-overview', methods=['GET'])
def market_overview():
    """Get overview of all markets"""
    try:
        interval = request.args.get('interval', '1d')
        
        # Quick overview without full analysis
        assets = {
            'forex': FOREX_PAIRS[:5],  # Top 5
            'stocks': STOCKS[:5],
            'commodities': COMMODITIES[:3],
            'indices': INDICES[:3]
        }
        
        overview = {
            'forex': [],
            'stocks': [],
            'commodities': [],
            'indices': []
        }
        
        for category, asset_list in assets.items():
            for asset in asset_list:
                try:
                    # Fetch basic data
                    if category == 'forex':
                        df = fetcher.fetch_forex_data(asset, interval, lookback=20)
                    elif category == 'stocks':
                        df = fetcher.fetch_stock_data(asset, interval, lookback=20)
                    elif category == 'commodities':
                        df = fetcher.fetch_commodity_data(asset, interval, lookback=20)
                    else:
                        df = fetcher.fetch_index_data(asset, interval, lookback=20)
                    
                    if not df.empty:
                        latest = df.iloc[-1]
                        prev = df.iloc[-2]
                        
                        change = ((latest['close'] - prev['close']) / prev['close']) * 100
                        
                        overview[category].append({
                            'name': asset,
                            'price': float(latest['close']),
                            'change': float(change),
                            'volume': float(latest['volume']) if 'volume' in latest else 0
                        })
                except:
                    continue
        
        return jsonify({
            'success': True,
            'overview': overview,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/train-model', methods=['POST'])
def train_model():
    """Train ML model for specific asset"""
    try:
        data = request.json
        asset_name = data['asset']
        model_type = data.get('model_type', 'ensemble')
        
        # Fetch data
        df = fetch_asset_data(asset_name, '1d', 200)
        
        if df.empty:
            return jsonify({'success': False, 'error': 'No data available'}), 404
        
        # Add indicators
        df = TechnicalIndicators.add_all_indicators(df)
        
        # Train model
        engine = PredictionEngine(model_type=model_type)
        engine.train(df, target_periods=5)
        
        # Store model
        models[asset_name] = engine
        
        return jsonify({
            'success': True,
            'message': f'Model trained for {asset_name}',
            'model_type': model_type
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# Helper functions

def fetch_trading_signals(interval='1d'):
    """Fetch trading signals for all assets"""
    signals = []
    
    assets = {
        'forex': FOREX_PAIRS[:10],
        'stocks': STOCKS[:10],
        'commodities': COMMODITIES[:5],
        'indices': INDICES[:5]
    }
    
    for category, asset_list in assets.items():
        for asset in asset_list:
            try:
                df = fetch_asset_data(asset, interval, 100)
                
                if df.empty:
                    continue
                
                df = TechnicalIndicators.add_all_indicators(df)
                
                # Get prediction if model exists
                prediction = None
                if asset in models:
                    try:
                        prediction = models[asset].predict_next(df)
                    except:
                        pass
                
                # Generate signal
                signal = TradingSignalGenerator.generate_entry_signal(df, prediction)
                
                # Add metadata
                signal['asset'] = asset
                signal['category'] = category
                signal['timestamp'] = datetime.now().isoformat()
                
                # Only include actionable signals or high confidence holds
                if signal['signal'] != 'HOLD' or signal['confidence'] > 0.7:
                    signals.append(signal)
                    
            except Exception as e:
                print(f"Error processing {asset}: {e}")
                continue
    
    # Sort by confidence
    signals.sort(key=lambda x: x['confidence'], reverse=True)
    
    return signals


def fetch_asset_data(asset_name, interval, lookback):
    """Fetch data for any asset"""
    asset_type = determine_asset_type(asset_name)
    
    if asset_type == 'forex':
        return fetcher.fetch_forex_data(asset_name, interval, lookback)
    elif asset_type == 'stock':
        return fetcher.fetch_stock_data(asset_name, interval, lookback)
    elif asset_type == 'commodity':
        return fetcher.fetch_commodity_data(asset_name, interval, lookback)
    else:
        return fetcher.fetch_index_data(asset_name, interval, lookback)


def determine_asset_type(asset_name):
    """Determine asset type from name"""
    if asset_name in FOREX_PAIRS or '/' in asset_name:
        return 'forex'
    elif asset_name in STOCKS:
        return 'stock'
    elif asset_name in COMMODITIES or '=' in asset_name:
        return 'commodity'
    else:
        return 'index'


if __name__ == '__main__':
    print("\n" + "="*70)
    print("🌐 FOREX PREDICTION BOT - WEB INTERFACE")
    print("="*70)
    print("\n✅ Starting web server...")
    print("📊 Dashboard will be available at: http://localhost:5000")
    print("\n⚠️  DISCLAIMER: For educational purposes only.")
    print("This is NOT financial advice. Trade at your own risk.\n")
    print("="*70 + "\n")
    
    app.run(debug=True, host='0.0.0.0', port=5000)
