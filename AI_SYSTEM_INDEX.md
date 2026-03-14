# AI SYSTEM INDEX

Forex Prediction Bot Platform

This document is designed for AI systems that analyze or modify this repository.

It provides a structural map of the system architecture and module relationships.

AI agents should read this document **before analyzing the codebase**.

---

# SYSTEM TYPE

Algorithmic trading intelligence platform.

The system performs:

• market data ingestion
• technical signal generation
• multi-layer signal filtering
• machine learning prediction
• risk management
• paper trade execution
• real-time dashboard broadcasting

---

# TECHNOLOGY STACK

Python — trading engine
Node.js — WebSocket gateway
Redis — message broker
PostgreSQL — persistent storage
HTML / JavaScript — monitoring dashboards

---

# SYSTEM ARCHITECTURE

Signal processing pipeline:

Market Data
→ Indicators
→ Strategy Engines
→ Voting System
→ 7-Layer Signal Filtering
→ Machine Learning Prediction
→ Risk Management
→ Execution Engine
→ Database Persistence
→ Redis Messaging
→ Node.js WebSocket Gateway
→ HTML Dashboards

---

# CORE MODULES

## core/

Central orchestration engine.

Key modules:

engine.py
pipeline.py
events.py
state.py
signal.py
assets.py

Responsibilities:

• system orchestration
• event coordination
• signal pipeline routing
• trading state management

---

# MARKET DATA LAYER

Directory:

data/

Modules:

fetcher.py
websocket_fetcher.py
cache.py

Responsibilities:

• retrieve market data
• maintain price cache
• distribute price updates

---

# STRATEGY ENGINE

Directory:

strategies/

Strategies implemented:

rsi.py
macd.py
bollinger.py
voting.py

All strategies inherit from:

strategies/base.py

Strategies produce directional signals with confidence values.

---

# INDICATOR LIBRARY

Directory:

indicators/

File:

technical.py

Contains implementations for:

RSI
MACD
Bollinger Bands
Moving averages
Volatility calculations

Strategies depend on these functions.

---

# SIGNAL FILTERING PIPELINE

Directory:

layers/

Signal passes through **seven filtering layers**:

layer1_voting.py
layer2_quality.py
layer3_regime.py
layer4_session.py
layer5_sentiment.py
layer6_whale.py
layer7_calibration.py

Each layer evaluates the signal and either modifies or rejects it.

---

# MACHINE LEARNING SYSTEM

Directory:

ml/

Modules:

predictor.py
trainer.py
registry.py

Model storage:

models/

Training automation:

auto_train_daily.py
auto_train_intelligent.py

Purpose:

• prediction scoring
• model training
• model version management

---

# RISK MANAGEMENT

Directory:

risk/

Modules:

manager.py
position_sizer.py

Configuration:

config/risk_config.json

Purpose:

• position sizing
• exposure checks
• trade validation

---

# EXECUTION ENGINE

Directory:

execution/

Module:

paper_trader.py

Implements simulated order execution.

Updates portfolio state and trade history.

---

# BACKTESTING ENGINE

Directory:

backtest/

Module:

engine.py

Provides historical simulation of strategies.

Used for performance evaluation.

---

# DATABASE SERVICES

Directory:

services/

Modules:

database_service.py
db_pool.py

Configuration:

config/database.py

Database:

PostgreSQL

Stores:

• trades
• predictions
• strategy metrics
• system state

---

# MESSAGE BUS

Module:

redis_broker.py

Message broker:

Redis

Used for:

• system events
• dashboard updates
• signal broadcasting

---

# WEBSOCKET GATEWAY

Directory:

gateway/

Server:

server.js

Technology:

Node.js

Function:

Bridge between Redis and real-time dashboards.

Broadcasts signals and system data to connected clients.

---

# DASHBOARD FRONTEND

Directory:

templates/

Dashboards:

index_live.html
chart_live.html
accuracy_dashboard.html
sentiment_dashboard.html
status_dashboard.html
websocket_feed.html

Dashboards receive data via WebSocket connections.

---

# TESTING SYSTEM

Directory:

tests/

Testing framework:

pytest

Test coverage includes:

strategies
signal pipeline
ML subsystem
risk engine
execution engine
database services
state management

All tests currently pass.

---

# AUTOMATION SCRIPTS

Scripts located in project root.

Examples:

install_deps.bat
start_trading.bat
start_dashboard.bat
setup_daily_training.ps1

These automate system setup and operations.

---

# AI ANALYSIS RULES

When analyzing this repository:

1. Never assume files exist if they are not present.
2. Preserve subsystem boundaries.
3. Understand the signal pipeline before modifying logic.
4. Do not reorder signal layers unless explicitly instructed.
5. Maintain compatibility with Redis messaging and WebSocket dashboards.

---

# SIGNAL PIPELINE SUMMARY

Market Data
→ Indicators
→ Strategy Signals
→ Voting System
→ Layer Filters (1–7)
→ ML Prediction
→ Risk Manager
→ Execution Engine
→ Database Storage
→ Redis Broadcast
→ Dashboard WebSocket

---

End of AI system index.
