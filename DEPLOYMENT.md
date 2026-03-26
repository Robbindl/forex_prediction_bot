# All-Asset Trading Mode — Deployment Guide
## For Profitable Trading: Crypto, Forex, Commodities, Indices

**Status**: ✅ ALL OPTIMIZATIONS APPLIED  
**Target Timeframe**: 15 minutes  
**Expected Deployment Time**: 30 minutes  

---

## ⚡ QUICK START (3 steps)

```powershell
# Step 1: Activate environment
& C:\Users\ROBBIE\Downloads\forex_prediction_bot\venv_tf\Scripts\Activate.ps1

# Step 2: Copy .env template and update API keys
Copy-Item .env.template .env -Force  # WARNING: Overwrites existing .env

# Step 3: Run bot on paper account
python bot.py --no-telegram  # Disable Telegram for testing
```

---

## 📋 DEPLOYMENT CHECKLIST

### Phase 1: Configuration (5 minutes)

- [ ] **Copy .env template**
  ```powershell
  Copy-Item C:\Users\ROBBIE\Downloads\forex_prediction_bot\.env.template `
            C:\Users\ROBBIE\Downloads\forex_prediction_bot\.env
  ```

- [ ] **Verify key settings in .env**:
  ```
  TRADING_TIMEFRAME=15m            ← CRITICAL
  DEFAULT_BALANCE=10000
  MAX_POSITIONS=8
  DAILY_LOSS_LIMIT_PERCENT=4.0     ← Tighter than default 5%
  ```

- [ ] **Update API keys** (edit .env file):
  - [ ] ITICK_TOKEN (required for real-time data)
  - [ ] FINNHUB_KEY (optional, fallback)
  - [ ] OILPRICE_API_KEY (for CL=F)
  - [ ] TELEGRAM_TOKEN (optional, for alerts)

- [ ] **Optional: Set API expiry alerts** (edit config/config.py):
  ```python
  API_KEY_EXPIRY_DATES = {
      "ITICK": date(2026, 12, 31),
      "Finnhub": date(2027, 3, 1),
  }
  ```

### Phase 2: Code Validation (5 minutes)

- [ ] **Verify strategy optimizations applied**:
  ```bash
  # RSI: period should be 8 (not 14)
  grep -n "def __init__.*period.*=.*8" strategies/rsi.py
  
  # MACD: signal should be 6 (not 9)
  grep -n "signal_line.*ewm.*span.*6" strategies/macd.py
  
  # News events: PRE_EVENT_MINS should be 10 (not 60)
  grep -n "PRE_EVENT_MINS.*=.*10" data_ingestion/news_event_monitor.py
  ```

- [ ] **Verify confidence thresholds**:
  ```bash
  # Voting min_confidence should be 0.58
  grep -n "min_confidence.*0.58" strategies/voting.py
  ```

### Phase 3: Backtest on 15m Data (10 minutes)

- [ ] **Run backtest on single asset first** (test one pair):
  ```powershell
  # Backtest EUR/USD on 15m
  python bot.py --backtest EUR/USD --backtest-cat forex
  
  # Expected: Backtest should complete in <30 seconds
  # Expected: See chart + summary CSV in backtest_results/
  ```

- [ ] **Run performance comparison** (check all assets):
  ```powershell
  python -c "
  import pandas as pd
  results = pd.read_csv('backtest_results/all_strategies_comparison.csv')
  # Group by asset, show avg win rate
  print(results.groupby('asset')['win_rate'].mean().sort_values(ascending=False))
  "
  ```

- [ ] **Verify Bollinger Bands is working best** (Crypto should dominate top 5):
  ```powershell
  python -c "
  import pandas as pd
  results = pd.read_csv('backtest_results/all_strategies_comparison.csv')
  top = results.nlargest(5, 'total_return')[['asset', 'strategy', 'total_return', 'win_rate']]
  print(top)
  # Expect: BTC/ETH/SOL/XRP BB to dominate
  "
  ```

### Phase 4: Paper Trading Validation (3 days minimum)

- [ ] **Start bot on paper account** (no real capital):
  ```powershell
  python bot.py --no-gateway --no-telegram
  # Monitor: Check logs every 30 minutes
  ```

- [ ] **Monitor first 24 hours**:
  - [ ] Check that signals are generated every 45 seconds (SCAN_INTERVAL_SECONDS=45)
  - [ ] Verify position entry prices are reasonable
  - [ ] Check SL/TP distances match ATR multipliers
  - [ ] Confirm daily loss limit blocks trading after 4% loss

- [ ] **Monitor P&L by asset class** (see dashboard):
  ```
  Navigate to: http://localhost:5000
  Check: "Open Positions" tab
  Look for: Mixed assets (not just forex or crypto)
  Expected: Crypto trades should be winning 55%+, Forex 45%+
  ```

- [ ] **Review trade signals in Telegram** (optional):
  - [ ] Check that signals have confidence scores 0.58+ ✅
  - [ ] Verify signal reasons mention strategy votes (RSI/MACD/BB)
  - [ ] Confirm no forex signals during PRE_EVENT (news window)

- [ ] **Verify daily loss limit works**:
  ```
  After bot loses 4% of balance:
  [ ] New positions should NOT be opened
  [ ] Log should show: "Daily loss limit reached"
  [ ] Reset at UTC midnight
  ```

- [ ] **Run for 3 days minimum**:
  - [ ] Day 1: Check stability, signal generation
  - [ ] Day 2-3: Verify P&L trends, profit/loss by asset
  - [ ] Expected: 50%+ win rate on crypto, 40%+ on others

### Phase 5: Go Live (if P&L is positive)

- [ ] **Reduce balance to test size** (e.g., $1,000):
  ```
  Edit .env: DEFAULT_BALANCE=1000
  Bot position sizes will scale down 10×
  ```

- [ ] **Enable Telegram alerts** (optional):
  ```
  Remove: --no-telegram flag from bot startup
  ```

- [ ] **Monitor first week closely**:
  - [ ] Check daily P&L every morning
  - [ ] Review biggest losing trades
  - [ ] Confirm stop losses are protecting capital

- [ ] **Scale up gradually** (only if consistent profit):
  ```
  Week 1:  $1,000 balance
  Week 2:  $2,000 balance (if +10% return)
  Week 3:  $5,000 balance (if running 2 weeks profitable)
  Month 2: $10,000 balance (if 1 month consistent profit)
  ```

---

## 🔧 KEY CHANGES FROM DEFAULT

| Setting | Before | After | Reason |
|---------|--------|-------|--------|
| **TRADING_TIMEFRAME** | 1d | 15m | Faster signals, more opportunities |
| **RSI Period** | 14 candles | 8 candles | Faster response on 15m |
| **MACD Signal** | 9 bars | 6 bars | Quicker histogram crosses |
| **BB Squeeze** | 0.5% | 0.35% | More sensitive bounces |
| **PRE_EVENT_MINS** | 60 | 10 | Allows trading 50 min before news |
| **ACTIVE_MINS** | 15 | 10 | Markets settle faster than expected |
| **MIN_FINAL_CONFIDENCE** | 0.62 | 0.62 | Keep stricter, more quality |
| **DAILY_LOSS_LIMIT** | 5% | 4% | More trades = more risk |
| **Risk/Trade (Forex)** | 1.5% | 1.2% | Wider SL needed on 15m |
| **Voting Min Votes** | 1 | 1 | Same (accept single strong signal) |

---

## 🚨 CRITICAL VALIDATION STEPS

### 1️⃣ Verify News Event Blocking Works (CRITICAL)
```python
# Test: Check that PRE_EVENT_MINS=10 is active
python -c "
from data_ingestion.news_event_monitor import PRE_EVENT_MINS, ACTIVE_MINS
print(f'PRE_EVENT_MINS={PRE_EVENT_MINS} (should be 10)')
print(f'ACTIVE_MINS={ACTIVE_MINS} (should be 10)')
assert PRE_EVENT_MINS == 10, 'PRE_EVENT_MINS not updated!'
assert ACTIVE_MINS == 10, 'ACTIVE_MINS not updated!'
print('✅ News event timing is correct')
"
```

### 2️⃣ Verify Strategy Parameters
```python
# Test: Check RSI, MACD, BB parameters
python -c "
from strategies.rsi import RSIStrategy
from strategies.macd import MACDStrategy
from strategies.bollinger import BollingerStrategy

rsi = RSIStrategy()
macd = MACDStrategy()
bb = BollingerStrategy()

assert rsi.period == 8, f'RSI period {rsi.period} != 8'
assert macd.signal == 6, f'MACD signal {macd.signal} != 6'
assert bb.period == 20, f'BB period {bb.period} != 20'
print('✅ All strategy parameters are correct')
"
```

### 3️⃣ Verify Confidence Thresholds
```python
# Test: Check min_confidence in voting
python -c "
from strategies.voting import VotingStrategy

voting = VotingStrategy()
assert voting.min_confidence == 0.58, f'Voting confidence {voting.min_confidence} != 0.58'
print(f'✅ Voting min_confidence={voting.min_confidence}')
"
```

### 4️⃣ Verify No Stale API Keys
```python
# Test: Check API expiry handling
python -c "
from config.config import API_KEY_EXPIRY_DATES
from datetime import date

today = date.today()
print(f'Today: {today}')
print(f'API Key Expiry Dates configured:')
for name, exp_date in (API_KEY_EXPIRY_DATES or {}).items():
    days = (exp_date - today).days
    print(f'  {name}: {exp_date} ({days} days remaining)')

if not API_KEY_EXPIRY_DATES:
    print('⚠️  No API key expiry dates configured (optional)')
"
```

---

## 📊 EXPECTED RESULTS AFTER DEPLOYMENT

### Win Rates by Asset Class (from backtest)
- **Crypto (BTC/ETH/SOL/XRP)**: 55–68% with Bollinger Bands
- **Forex (EUR/USD, GBP/USD)**: 33–50% (harder market)
- **Commodities (Gold, Oil)**: 40–67% (trend-dependent)
- **Indices (SPX, DJI)**: 45–60% (correlated moves)

### Expected P&L (paper trading 3 days, $10k)
- **Target**: +2% to +5% (3-day return)
- **Warning**: -3% or more = revisit configuration
- **Failure**: Negative over first week = rollback and backtest more

### Daily Trade Frequency (15m timeframe)
- **Crypto pairs**: 3-5 trades per day
- **Forex pairs**: 1-3 trades per day
- **Commodities**: 1-2 trades per day
- **Indices**: 1-2 trades per day
- **TOTAL**: Expect 8-12 positions open at peak

---

## 🛑 TROUBLESHOOTING

### "No signals generated"
→ Check ITICK_TOKEN is valid (primary data source)  
→ Verify SCAN_INTERVAL_SECONDS allows enough time (45 sec default)  
→ Check logs for "data integrity gate killed signal"

### "All forex trades are losing"
→ Expected for this market (forex is harder)  
→ Forex backtest shows 33-50% win rate—this is normal  
→ Consider disabling EUR/USD, GBP/USD if capital is limited

### "Crypto trades winning but forex losing"
→ This is EXPECTED and OPTIMAL  
→ Allocate more to crypto (3 positions): BTC, ETH, SOL, XRP
→ Allocate less to forex (2 positions): main pairs only (EUR/USD, GBP/USD)

### "Daily loss limit triggered instantly"
→ Check DEFAULT_BALANCE is set correctly ($10,000)  
→ Check DAILY_LOSS_LIMIT_PERCENT is 4% (not hardcoded 5%)  
→ Check RISK_PER_TRADE is 1.2% (not 1.5%)

### "API key expiry alert not working"
→ Check API_KEY_EXPIRY_DATES in config.py  
→ Verify Telegram token is set  
→ Manual check: `python -c "from config.config import API_KEY_EXPIRY_DATES; print(API_KEY_EXPIRY_DATES)"`

---

## 📞 NEXT STEPS

1. **Backup your current .env**: `cp .env .env.backup`
2. **Copy .env.template to .env**: Follow Quick Start step 2
3. **Run validation tests**: Follow Deployment Checklist step 2
4. **Start paper trading**: Follow Deployment Checklist step 4
5. **Monitor for 3 days**: Check logs, P&L, win rates
6. **Only go live** if P&L is positive and consistent

---

**Questions?** Check the bot logs in `logs/` directory for detailed error messages.
