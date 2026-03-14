# Installation Guide

## Requirements
- Python 3.11+
- pip
- (Optional) PostgreSQL for persistence
- (Optional) Redis for WebSocket pub/sub

## Quick Start

grep -r "api_key\s*=\s*['\"][A-Za-z0-9]" --include="*.py" .

### 1. Clone and install
```bash
git clone <repo>
cd forex_prediction_bot
pip install -r requirements.txt
```

### 2. Configure environment
```bash
cp .env.example .env
# Edit .env — add your API keys
```

### 3. Run the bot
```bash
# Default (voting strategy, $30 balance, dashboard on :5000)
python bot.py

# Custom balance and strategy
python bot.py --balance 1000 --strategy voting

# Without Telegram
python bot.py --no-telegram

# Without dashboard (headless)
python bot.py --no-dashboard

# Backtest only
python bot.py --backtest BTC-USD --backtest-cat crypto
```

### 4. Access dashboard
Open `http://localhost:5000` in your browser.

## Project structure
```
forex_prediction_bot/
├── bot.py                    ← Single entry point
├── config/config.py          ← All configuration
├── core/
│   ├── engine.py             ← TradingCore
│   ├── pipeline.py           ← 7-layer pipeline
│   ├── signal.py             ← Signal dataclass
│   ├── state.py              ← SystemState (persisted)
│   ├── events.py             ← EventBus
│   └── assets.py             ← AssetRegistry
├── strategies/               ← RSI, MACD, Bollinger, Voting
├── layers/                   ← L1–L7 pipeline filters
├── data/                     ← Fetcher + Cache
├── ml/                       ← Predictor + Trainer + Registry
├── risk/                     ← Manager + PositionSizer
├── execution/                ← PaperTrader
├── backtest/                 ← BacktestEngine
├── dashboard/                ← Flask app + templates
├── utils/                    ← Logger
└── indicators/               ← Technical indicators
```

## Files to delete from old repo
```bash
rm config.py              # duplicate
rm trading_system.py      # god class — split into core/
rm back_up.py ai_refactor.py clean_models.py init.py
rm profitability_upgrade.py migrate_timescale.py
rm error_handling.py monitor.py
rm engines/backtest_engine.py engines/ml_engine.py
rm engines/strategy_engine.py engines/whale_monitor.py
rm risk_manager.py
rm training_monitor.py
rm human_explainer_db.py
rm telegram_whale_watcher.py telethon_whale_store.py
rm alpha_discovery.py orderflow_engine.py signal_learning.py
```

## Environment variables
See `.env.example` for the full list.
Required for live trading:
- `FINNHUB_KEY` or `TWELVEDATA_KEY` (market data)
- `TELEGRAM_TOKEN` + `TELEGRAM_CHAT_ID` (alerts)

Optional:
- `DATABASE_URL` (PostgreSQL — system works without it)
- `REDIS_URL` (WebSocket pub/sub — system works without it)

## Running with Docker
```bash
docker-compose up --build
```