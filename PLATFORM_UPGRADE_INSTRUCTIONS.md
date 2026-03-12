# Platform Upgrade — Full Setup Instructions
## 7 Upgrades: WebSocket Gateway · OrderFlow · Redis · Prediction Overlay · Accuracy Tracker · TimescaleDB · Alpha Discovery

---

## Step 1 — Deploy All Files

Copy everything from the `forex_fixes` folder to your bot:

```
Bot root:
  redis_broker.py          → C:\Users\ROBBIE\Downloads\forex_prediction_bot\
  orderflow_engine.py      → C:\Users\ROBBIE\Downloads\forex_prediction_bot\
  alpha_discovery.py       → C:\Users\ROBBIE\Downloads\forex_prediction_bot\
  prediction_tracker.py    → C:\Users\ROBBIE\Downloads\forex_prediction_bot\
  migrate_timescale.py     → C:\Users\ROBBIE\Downloads\forex_prediction_bot\
  trading_system.py        → C:\Users\ROBBIE\Downloads\forex_prediction_bot\
  web_app_live.py          → C:\Users\ROBBIE\Downloads\forex_prediction_bot\

Templates:
  templates\accuracy_dashboard.html   → templates\
  templates\chart_live.html           → templates\

Gateway (new folder):
  gateway\package.json     → C:\Users\ROBBIE\Downloads\forex_prediction_bot\gateway\
  gateway\server.js        → C:\Users\ROBBIE\Downloads\forex_prediction_bot\gateway\
```

---

## Step 2 — Install Redis (Windows)

Redis is the message bus between Python and Node.js.
The bot works WITHOUT Redis — it just won't have live WebSocket broadcast.

**Option A — Docker (recommended, easiest):**
```
docker run -d --name redis -p 6379:6379 redis:alpine
```

**Option B — Windows native:**
1. Download from: https://github.com/tporadowski/redis/releases
2. Download `Redis-x64-xxx.msi`, run installer
3. Redis starts automatically as a Windows service on port 6379

**Verify Redis is running:**
```
redis-cli ping
```
Should reply: `PONG`

---

## Step 3 — Install Python redis-py

```
cd C:\Users\ROBBIE\Downloads\forex_prediction_bot
venv_tf\Scripts\activate
pip install redis websocket-client
```

---

## Step 4 — Install Node.js WebSocket Gateway

```
cd C:\Users\ROBBIE\Downloads\forex_prediction_bot\gateway
npm install
```

This installs: `ws`, `ioredis`, `express`, `cors`, `http-proxy-middleware`

---

## Step 5 — Install TimescaleDB (Optional but Recommended)

TimescaleDB is a free extension to your existing PostgreSQL.
It turns your time-series tables into hypertables that query 10-100x faster.

1. Go to: https://docs.timescale.com/self-hosted/latest/install/installation-windows/
2. Download the installer for your PostgreSQL version (check in pgAdmin → About)
3. Run installer — it adds the extension to your existing PostgreSQL
4. **Your DATABASE_URL does NOT change**

Then run the migration script ONCE:
```
cd C:\Users\ROBBIE\Downloads\forex_prediction_bot
venv_tf\Scripts\activate
python migrate_timescale.py
```

This creates all new tables, converts to hypertables, sets up retention policies.
Safe to run multiple times.

---

## Step 6 — Start Everything

You now have 3 terminals:

**Terminal 1 — Main bot (unchanged):**
```
cd C:\Users\ROBBIE\Downloads\forex_prediction_bot
venv_tf\Scripts\activate
python bot.py --no-perf --balance 50
```

**Terminal 2 — Node.js WebSocket Gateway:**
```
cd C:\Users\ROBBIE\Downloads\forex_prediction_bot\gateway
node server.js
```

**Terminal 3 — Redis (if not running as service):**
```
redis-server
```
(Skip if you used the Docker or Windows service option)

---

## What You Get After Setup

### Live Chart  → http://localhost:5000/chart
- AI prediction overlay renders as a dashed purple line showing price target
- Confidence band shown as shaded area around prediction
- Order flow pressure displayed in top-right legend panel
- Updates every 2 minutes with fresh predictions

### Accuracy Dashboard  → http://localhost:5000/accuracy
- Rolling accuracy at 1H, 4H, 24H horizons
- Per-asset accuracy breakdown table
- Calibration chart: predicted confidence vs actual win rate
- Alpha discovery signal feed (updates every 5 minutes)
- Recent 20 prediction outcomes with direction/target result

### WebSocket Gateway  → ws://localhost:8080
- Every signal, price tick, whale alert, and alpha signal broadcasts instantly
- Connect from browser, mobile app, or any WebSocket client
- Subscribe to specific channels: signals, prices, whale_alerts, orderflow, alpha, predictions

### New API Endpoints
```
GET  /api/orderflow/EUR/USD      → latest bid/ask imbalance + pressure
GET  /api/orderflow              → all assets order flow
GET  /api/alpha                  → recent alpha signals (correlation break, volume anomaly, divergence)
GET  /api/alpha/BTC-USD          → alpha signals for one asset
GET  /api/accuracy               → AI accuracy stats by horizon
GET  /api/accuracy?days=7        → last 7 days only
GET  /api/prediction-overlay/EUR/USD → overlay data for live chart
GET  /api/redis/status           → Redis + gateway connectivity check
```

---

## Architecture After Upgrade

```
Python Bot (trading_system.py)
     │
     │ redis_broker.publish('signals', signal)
     │ redis_broker.publish('prices', price)
     │ redis_broker.publish('whale_alerts', alert)
     ▼
Redis pub/sub (localhost:6379)
     │
     │ subscribes
     ▼
Node.js Gateway (gateway/server.js :8080)
     │
     │ WebSocket broadcast
     ▼
Browser (chart_live.html)
Mobile app
External tools
```

---

## How Each Upgrade Integrates

### OrderFlow Engine
- Crypto: connects to Binance WebSocket depth stream (free, no API key)
- Forex/stocks: builds synthetic order flow from tick velocity analysis
- Feeds a confidence modifier (+/-4%) into the 7-layer quality gate
- Stored in PostgreSQL `orderflow_snapshots` table

### Alpha Discovery Engine
- Runs every 5 minutes scanning all major assets
- Finds: correlation breakdowns, volume z-score spikes (>2.5σ), RSI divergence, cross-asset flow rotation
- Published to Redis `alpha` channel → WebSocket → browser
- Visible in accuracy dashboard alpha feed

### Prediction Tracker
- Records every signal that passes the 7-layer gate
- Background thread checks prices at 1H, 4H, 24H after each signal
- Stores outcome in `prediction_outcomes` table
- Powers the accuracy dashboard with rolling stats

### TimescaleDB
- Zero change to your code or DATABASE_URL
- `orderflow_snapshots`, `alpha_signals`, `prediction_outcomes`, `price_candles` are hypertables
- Automatic data retention: orderflow 90 days, alpha/candles 1 year
- Continuous aggregates for instant hourly/daily rollups

---

## Troubleshooting

**Redis not connecting:**
- Check Redis is running: `redis-cli ping` → should return PONG
- Check port 6379 is not blocked by firewall
- Bot still works without Redis — all signals still go to Telegram

**Gateway not starting:**
- Run `npm install` inside the `gateway/` folder first
- Ensure Node.js is installed: `node --version`
- Check Flask is running on :5000 first (gateway proxies to it)

**No prediction overlay on chart:**
- Overlay only appears after a signal has been generated for that asset
- Switch to an asset that has had recent signals
- Check `/api/prediction-overlay/EUR/USD` in browser — if `overlay: null` no signal yet

**No accuracy data:**
- Accuracy data appears 1 hour after first signal is generated
- Check `/api/accuracy` — `total: 0` means no signals evaluated yet

**TimescaleDB migration failed:**
- Ensure TimescaleDB extension is installed for your PostgreSQL version
- The bot works fine without it — tables are created as regular PostgreSQL tables
- Re-run `python migrate_timescale.py` after installing the extension

---

## No Changes Needed to .env

Your existing `.env` is fully compatible. Optionally add:

```
REDIS_HOST=127.0.0.1
REDIS_PORT=6379
REDIS_PASSWORD=
```

These default to `127.0.0.1:6379` with no password if not set.
