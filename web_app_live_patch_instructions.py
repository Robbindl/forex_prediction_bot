"""
TWO CHANGES NEEDED IN web_app_live.py
======================================

─────────────────────────────────────────────────────────────────────────────
CHANGE 1 — near the bottom, FIND THIS (the duplicate deque + broken route):
─────────────────────────────────────────────────────────────────────────────

    recent_transactions = deque(maxlen=50)
    
@app.route('/api/websocket/feed')
def get_websocket_feed():
    return jsonify({
        'success': True,
        'transactions': list(recent_transactions),
        'count': len(recent_transactions)
    })

─── REPLACE WITH ────────────────────────────────────────────────────────────

@app.route('/api/websocket/feed')
def get_websocket_feed():
    from websocket_dashboard import recent_transactions as ws_transactions
    return jsonify({
        'success': True,
        'transactions': list(ws_transactions),
        'count': len(ws_transactions)
    })

─────────────────────────────────────────────────────────────────────────────
CHANGE 2 — in the if __name__ == '__main__': block, BEFORE app.run(...),
           ADD this block to start the WebSocket manager as a thread
           (same process = shared memory = dashboard sees trades instantly):
─────────────────────────────────────────────────────────────────────────────

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
        logger.info("🚀 WebSocket manager running in-process")

    ws_thread = threading.Thread(target=start_websocket_in_process, daemon=True)
    ws_thread.start()
    # ================================================

    app.run(debug=True, host='0.0.0.0', port=5000, threaded=True)

─────────────────────────────────────────────────────────────────────────────
WHY THIS WORKS
─────────────────────────────────────────────────────────────────────────────

Before (broken):
  subprocess → websocket_manager.py (own memory)  ──writes──▶ its own recent_transactions
  web_app_live.py (own memory)                     ──reads──▶  its own recent_transactions (always empty)

After (fixed):
  web_app_live.py
    └── ws_thread (same process, same memory)
          └── WebSocketManager callback
                └── add_transaction()  ──writes──▶  websocket_dashboard.recent_transactions
    └── /api/websocket/feed            ──reads──▶   websocket_dashboard.recent_transactions (same object!)

No Redis, no files, no two terminals. One process, shared memory.
─────────────────────────────────────────────────────────────────────────────
"""
