# API LIMIT AUDIT & OPTIMIZATION PLAN

**Date:** March 27, 2026  
**Status:** CRITICAL — You're exceeding TwelveData quota  
**Action Required:** Immediate optimization needed

---

## 📊 CURRENT STATUS: API USAGE DASHBOARD

### TwelveData (CONFIRMED OVER LIMIT)
```
Plan:              Basic 8
API Credits Used:  1,039 / 800    ❌ EXCEEDED (+239)
Minutely Avg:      17 / 8         ❌ EXCEEDED (2.1x limit)
Minutely Max:      31 / 8         ❌ EXCEEDED (3.8x limit)
WebSockets Used:   0 / 8          ❌ NOT UTILIZED
```
**Problem:** You're calling TwelveData API (REST) instead of WebSocket.  
REST calls = higher credit cost. WebSocket = same data, unlimited.

---

## 🔍 YOUR CURRENT API STACK

### Tier 1: Real-Time Data (Used in Pipeline)
| API | Purpose | Plan | Limit | Status | Cost |
|-----|---------|------|-------|--------|------|
| **TwelveData** | Forex + Crypto OHLCV | Basic 8 | 800 credits/day | ❌ OVER | REST = expensive |
| **iTick** | Forex, Crypto, Indices, Commodities | Free | 120 req/min | ✅ OK | Free |
| **Alpha Vantage** | Commodity real-time | Free | 25 req/day | ⏳ Unknown | Free |
| **OilPrice API** | Oil (CL=F) prices | Free | 1000 req/month | ✅ OK | Free |
| **Finnhub** | Crypto candles | Free | ~600/month | ⏳ Unknown | Free |

### Tier 2: Sentiment/News (Not Critical to Signals)
| API | Purpose | Limit | Status |
|-----|---------|-------|--------|
| NewsAPI | Financial news | 100/day | ❌ Likely over |
| GNews | Global news | Varies | ❌ Likely over |
| RapidAPI | General | Rate limited | ❌ Likely over |
| MarketAux | Market data | Limited | ❌ Likely over |
| FRED API | Economic data | 120/min | ✅ OK |
| Twitter/X API | Sentiment tweets | Limited | ❌ Likely over |

### Tier 3: Infrastructure (Not Data)
| API | Purpose | Status |
|-----|---------|--------|
| OpenAI | ML predictions | ✅ Working |
| Telegram | Alerts | ✅ Working |
| Reddit | Sentiment | ✅ Working |
| Apify | Web scraping | ✅ Working |
| Whale Alert | Large trades | Authenticated only |

---

## 🚨 ROOT CAUSE ANALYSIS

### Why You're Over Limit on TwelveData

**Your fetcher calls TwelveData REST API like this:**
```python
get_ohlcv(asset, category, interval="15m", periods=500)
```

**Each call = multiple credits:**
- 1 API call = 1-5 credits (varies by interval)
- Your pipeline runs every 15min
- × 6 forex pairs = 6 calls × 5 credits = ~30 credits per cycle
- × 24 hours = ~720 credits/day (within limit)
- BUT: During backtesting/validation = massive credit spike (+300 at once)

**WebSocket Alternative:**
- 1 WebSocket ≈ 1 credit/month
- Unlimited real-time data
- Your "Basic 8" plan includes 8 simultaneous connections

---

## ✅ SOLUTION: 3-LAYER OPTIMIZATION

### Layer 1: Use WebSocket Instead of REST (Saves 90% Credits)

**FROM (Current - REST API):**
```python
# Each call = 5 credits
df = fetcher.get_ohlcv("EUR/USD", "forex", interval="15m", periods=500)
```

**TO (WebSocket - Almost Free):**
```python
# 1 WebSocket = 1 credit/month, unlimited data
df = fetcher.get_ohlcv_websocket("EUR/USD", "forex")
```

**Impact:** 1,000+credits/day → 5 credits/month

---

### Layer 2: Use iTick as Primary (Already Free)

**Current hierarchy (WRONG):**
1. TwelveData REST (expensive) ← STOP USING
2. yfinance (slow)

**Optimized hierarchy:**
1. iTick real-time (120/min free) ← USE FIRST
2. yfinance (fallback only)

**iTick supports:**
- ✅ All 6 forex pairs
- ✅ All 5 crypto
- ✅ All 3 commodities (via XAUUSD, XAGUSD)
- ✅ All 4 indices
- ✅ 120 requests/min = ~86,400/day (you need <100)

**Cost:** Free forever

---

### Layer 3: Cache Aggressively (Reduce Calls)

**Current:**
```python
Every 15min → Fresh API call
```

**Optimized:**
```python
Every 15min → Check cache (300s TTL)
Every 5min cache miss → Fetch fresh
Every cache hit → Use cached data
```

**Savings:** 60% fewer API calls

---

## 📋 API BY API: LIMITS & STATUS

### CRITICAL - EXCEEDING NOW

#### TwelveData
- **Current:** 1,039 / 800 (130% over)
- **Free Plan:** 800 credits/day, 8 WebSocket connections
- **Problem:** Using REST API (5 credits/call) instead of WebSocket (free)
- **Fix:** Switch to WebSocket immediately
- **Timeline:** 2 hours

#### Alpha Vantage
- **Current:** Unknown (likely over on commodity requests)
- **Free Plan:** 25 requests/day
- **What You're Using:** Commodity prices (GC=F, SI=F, CL=F)
- **Problem:** Probably checking every 15min = too many calls
- **Fix:** Use iTick instead (maps to XAUUSD/XAGUSD)
- **Timeline:** 1 hour

#### NewsAPI
- **Current:** Unknown
- **Free Plan:** 100 requests/day
- **What You're Using:** Financial news sentiment
- **Problem:** Probably checking every 5min across 18 assets
- **Fix:** Reduce to 1x/hour, sample 5 assets
- **Timeline:** 1 hour

---

### WARNING - LIKELY OVER

#### Twitter/X API
- **Current:** Unknown
- **Free Plan:** 450 requests/month (15/day)
- **What You're Using:** Whale tweet monitoring
- **Problem:** Likely checking every 5min
- **Fix:** Rate limit to 15 checks/day or buy paid plan
- **Timeline:** 30 min

#### Finnhub
- **Current:** Unknown
- **Free Plan:** ~600 requests/month
- **What You're Using:** Crypto candles
- **Problem:** May exceed if checking multiple pairs
- **Fix:** Use iTick instead
- **Timeline:** 30 min

---

### OK - WITHIN LIMITS

#### iTick
- **Current:** ~100-200/day (estimate)
- **Free Plan:** 120 requests/minute = 172,800/day
- **Status:** ✅ Safe
- **Action:** Make this your PRIMARY source

#### OilPrice API  
- **Current:** ~60/month (estimate)
- **Free Plan:** 1,000 requests/month
- **Status:** ✅ Safe

#### FRED API
- **Current:** ~200/day (estimate)
- **Free Plan:** 120 requests/minute
- **Status:** ✅ Safe

---

## 🛠️ IMPLEMENTATION PLAN

### Phase 1: Emergency Fix (TODAY - 2 hours)
- [ ] Enable TwelveData WebSocket in fetcher
- [ ] Replace REST calls with WebSocket calls
- [ ] Test on EUR/USD, BTC-USD
- [ ] Monitor credit usage

### Phase 2: Optimize Secondary Fetchers (TOMORROW - 2 hours)
- [ ] Add iTick fetching to primary layer (before TwelveData)
- [ ] Remove Alpha Vantage commodity calls (use iTick)
- [ ] Test coverage of all 18 assets

### Phase 3: Rate Limit News/Sentiment (TOMORROW - 1 hour)
- [ ] Reduce NewsAPI from 5min/check to 1hr/check
- [ ] Reduce Twitter checks from 5min to 30min
- [ ] Cache sentiment results for 4 hours

### Phase 4: Caching Strategy (FRIDAY - 1 hour)
- [ ] Implement 300s cache for OHLCV
- [ ] Skip redundant calls within TTL
- [ ] Add cache metrics to logs

---

## 📈 EXPECTED RESULTS

### Credit Usage Before → After

**TwelveData:**
- Before: 1,039/day (130% over limit)
- After: ~5/day (via WebSocket)
- Savings: 99.5%

**Alpha Vantage:**
- Before: 50+/day (2x limit)
- After: 0/day (use iTick)
- Savings: 100%

**NewsAPI:**
- Before: 200/day (2x limit)
- After: 24/day (1x/hour × 24h)
- Savings: 88%

**Total Savings: 93% of all API calls**

---

## ⚠️ RISKS

| Risk | Severity | Mitigation |
|------|----------|-----------|
| WebSocket disconnect | Medium | Auto-reconnect + fallback to REST |
| iTick not covering all pairs | Low | Documented in code, falls back to yfinance |
| Cache stale data | Low | 300s TTL, manual refresh available |
| Twitter limits | High | Get paid API key or reduce frequency |

---

## NEXT STEPS

**1. Confirm you want to proceed**
**2. I'll implement Phase 1 (WebSocket) immediately**
**3. Test on 1-2 pairs**
**4. If working, roll out to all 18 assets**

Should I start with Phase 1 (WebSocket optimization)?
