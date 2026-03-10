import threading
from web_app_live import app, refresh_signals, signals_cache
from datetime import datetime

def load_data():
    """Load initial data in background"""
    print("📡 Loading initial data in background...")
    try:
        signals_cache['signals'] = refresh_signals()
        signals_cache['last_refresh'] = datetime.now()
        print("✅ Initial data loaded successfully!")
    except Exception as e:
        print(f"❌ Error loading initial data: {e}")

# Start background thread for data loading
thread = threading.Thread(target=load_data, daemon=True)
thread.start()

print("🚀 Starting Flask server immediately...")
print("📊 Dashboard will be available at http://localhost:5000")
print("   (Data will load in the background)")

# Start Flask server
app.run(host='0.0.0.0', port=5000, debug=False, threaded=True)