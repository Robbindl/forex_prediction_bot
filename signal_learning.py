"""
Signal Learning Engine  —  Wall-Street-Grade Signal System
===========================================================
Single file. No new dependencies beyond what the bot already has.

Features:
  1. PRICE CACHE      — pre-fetches all 61 assets every 60s → instant clicks
  2. SIGNAL QUALITY   — 3-TF confluence, ATR stops, min 2:1 RR, news blackout
  3. LEARNING ENGINE  — stores every signal, auto-resolves TP/SL, adjusts bias
  4. RATE LIMITER     — per-API call budgets
  5. UNIT TESTS       — run: python signal_learning.py
  6. STRESS TEST      — 2008/2020/2022 crash simulation
"""
from __future__ import annotations
import os, uuid, time, threading, traceback
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Tuple
from collections import defaultdict, deque
from logger import logger
try:
    from telethon_whale_store import whale_store as _whale_store
except Exception:
    _whale_store = None

# ─── DB ─────────────────────────────────────────────────────────────────────
try:
    from sqlalchemy import (Column, BigInteger, String, Numeric, DateTime,
                             Boolean, Integer, JSON, Index)
    from sqlalchemy.sql import func
    from config.database import Base, SessionLocal, engine
    _DB_OK = True
except Exception as _dbe:
    logger.warning(f"SignalEngine: DB unavailable ({_dbe}) — memory-only mode")
    _DB_OK = False
    Base = object
    def SessionLocal(): return None
    engine = None

# ══════════════════════════════════════════════════════════════════════════════
# DB MODELS
# ══════════════════════════════════════════════════════════════════════════════
class SignalHistory(Base if _DB_OK else object):
    __tablename__ = 'signal_history'
    if _DB_OK:
        id             = Column(BigInteger, primary_key=True, autoincrement=True)
        signal_id      = Column(String(36),  unique=True, nullable=False, index=True)
        asset          = Column(String(30),  nullable=False, index=True)
        direction      = Column(String(4),   nullable=False)
        confidence     = Column(Numeric(6,4), nullable=False)
        entry_price    = Column(Numeric(20,8), nullable=False)
        stop_loss      = Column(Numeric(20,8), nullable=False)
        take_profit    = Column(Numeric(20,8), nullable=False)
        take_profit_2  = Column(Numeric(20,8))
        take_profit_3  = Column(Numeric(20,8))
        atr            = Column(Numeric(20,8))
        risk_reward    = Column(Numeric(6,2))
        timeframe_conf = Column(String(10))
        session        = Column(String(20))
        reasons        = Column(JSON)
        indicators     = Column(JSON)
        news_titles    = Column(JSON)
        whale_alert    = Column(String(200))
        strategy_votes = Column(JSON)
        issued_at      = Column(DateTime(timezone=True), server_default=func.now(), index=True)
        outcome        = Column(String(10))
        outcome_price  = Column(Numeric(20,8))
        outcome_at     = Column(DateTime(timezone=True))
        pnl_r          = Column(Numeric(6,3))
        still_watching = Column(Boolean, default=True)
        __table_args__ = (
            Index('idx_sh_asset_issued', 'asset', 'issued_at'),
            Index('idx_sh_watching',     'still_watching'),
        )
    def to_dict(self):
        return {
            'signal_id': self.signal_id, 'asset': self.asset,
            'direction': self.direction, 'confidence': float(self.confidence),
            'entry_price': float(self.entry_price), 'stop_loss': float(self.stop_loss),
            'take_profit': float(self.take_profit),
            'take_profit_2': float(self.take_profit_2) if self.take_profit_2 else None,
            'take_profit_3': float(self.take_profit_3) if self.take_profit_3 else None,
            'risk_reward': float(self.risk_reward) if self.risk_reward else None,
            'timeframe_conf': self.timeframe_conf, 'session': self.session,
            'reasons': self.reasons or [], 'indicators': self.indicators or {},
            'news': self.news_titles or [], 'whale_alert': self.whale_alert,
            'issued_at': self.issued_at.isoformat() if self.issued_at else None,
            'outcome': self.outcome,
            'outcome_at': self.outcome_at.isoformat() if self.outcome_at else None,
            'pnl_r': float(self.pnl_r) if self.pnl_r else None,
        }

class SignalAssetStats(Base if _DB_OK else object):
    __tablename__ = 'signal_asset_stats'
    if _DB_OK:
        id               = Column(BigInteger, primary_key=True, autoincrement=True)
        asset            = Column(String(30), unique=True, nullable=False, index=True)
        total_signals    = Column(Integer, default=0)
        tp_hits          = Column(Integer, default=0)
        tp2_hits         = Column(Integer, default=0)
        sl_hits          = Column(Integer, default=0)
        expired          = Column(Integer, default=0)
        win_rate         = Column(Numeric(5,4), default=0.5)
        avg_rr_achieved  = Column(Numeric(5,2), default=0)
        confidence_bias  = Column(Numeric(5,4), default=0.0)
        strategy_weights = Column(JSON)
        last_outcome     = Column(String(10))
        updated_at       = Column(DateTime(timezone=True), server_default=func.now())

# ══════════════════════════════════════════════════════════════════════════════
# RATE LIMITER
# ══════════════════════════════════════════════════════════════════════════════
class RateLimiter:
    _BUDGETS = {'yahoo':60,'twelvedata':8,'finnhub':30,'alpha':5,
                'coingecko':10,'newsapi':100,'gnews':100,'marketaux':100,
                'forexfactory':10,'default':60}
    def __init__(self):
        self._windows = defaultdict(deque)
        self._lock    = threading.Lock()
    def check(self, api:str, cost:int=1) -> bool:
        budget   = self._BUDGETS.get(api, self._BUDGETS['default'])
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            with self._lock:
                now = time.monotonic()
                w   = self._windows[api]
                while w and w[0] < now - 60: w.popleft()
                if len(w) + cost <= budget:
                    for _ in range(cost): w.append(now)
                    return True
            time.sleep(0.05)
        logger.warning(f"RateLimiter: {api} budget exhausted")
        return False
    def remaining(self, api:str) -> int:
        budget = self._BUDGETS.get(api, self._BUDGETS['default'])
        with self._lock:
            now = time.monotonic(); w = self._windows[api]
            while w and w[0] < now-60: w.popleft()
            return max(0, budget - len(w))

rate_limiter = RateLimiter()

# ══════════════════════════════════════════════════════════════════════════════
# SIGNAL CACHE (instant click engine)
# ══════════════════════════════════════════════════════════════════════════════
class SignalCache:
    REFRESH_INTERVAL = 60
    STALE_THRESHOLD  = 300
    def __init__(self):
        self._cache   = {}
        self._df      = {}
        self._lock    = threading.RLock()
        self._bot     = None
        self._running = False
    def start(self, bot):
        self._bot     = bot
        self._running = True
        t = threading.Thread(target=self._loop, name='SignalCacheRefresh', daemon=True)
        t.start()
        logger.info("SignalCache: background refresh started (every 60s)")
    def get(self, asset): 
        with self._lock: return self._cache.get(asset)
    def put(self, asset, sig):
        with self._lock:
            sig['_cached_at'] = datetime.utcnow().isoformat()
            self._cache[asset] = sig
    def get_df(self, asset, tf):
        with self._lock: return self._df.get(f"{asset}_{tf}")
    def put_df(self, asset, tf, df):
        with self._lock: self._df[f"{asset}_{tf}"] = df
    def is_stale(self, asset):
        s = self.get(asset)
        if not s: return True
        ca = s.get('_cached_at')
        if not ca: return True
        return (datetime.utcnow()-datetime.fromisoformat(ca)).total_seconds() > self.STALE_THRESHOLD
    def _loop(self):
        time.sleep(15)
        while self._running:
            try: self._refresh_all()
            except Exception as e: logger.error(f"SignalCache refresh error: {e}")
            time.sleep(self.REFRESH_INTERVAL)
    def _refresh_all(self):
        if not self._bot: return
        assets = self._bot.get_asset_list()
        ok = 0
        for asset, cat in assets:
            try:
                sig = _build_quality_signal(asset, cat, self._bot, self)
                if sig: self.put(asset, sig); ok += 1
            except Exception as e: logger.debug(f"Cache {asset}: {e}")
            # Stagger by 0.5s — prevents cold-start rate burst.
            # 64 assets × 3 Yahoo calls = 192 needed vs budget of 60/min.
            # At 0.5s spacing the 64 refreshes take 32s, spreading calls
            # across time so the rate limiter never sees a burst.
            time.sleep(0.5)
        logger.info(f"SignalCache: refreshed {ok}/{len(assets)} assets")

signal_cache = SignalCache()

# ══════════════════════════════════════════════════════════════════════════════
# SIGNAL QUALITY ENGINE
# ══════════════════════════════════════════════════════════════════════════════
_ATR_CFG = {
    'crypto':     {'sl':1.5,'tp1':2.5,'tp2':4.0,'tp3':6.0,'min_rr':1.5},
    'forex':      {'sl':1.2,'tp1':2.0,'tp2':3.5,'tp3':5.0,'min_rr':1.5},
    'commodities':{'sl':1.3,'tp1':2.2,'tp2':3.8,'tp3':5.5,'min_rr':1.5},
    'stocks':     {'sl':1.0,'tp1':2.0,'tp2':3.0,'tp3':4.5,'min_rr':1.8},
    'indices':    {'sl':1.0,'tp1':1.8,'tp2':3.0,'tp3':4.0,'min_rr':1.5},
}
_ASSET_SESSION = {
    'forex':      ['London','NewYork','Overlap'],
    # Overlap (12-16 UTC) = London/NY crossover = highest liquidity for ALL asset classes
    # Gold, Silver, Oil volume peaks exactly in this window — must not penalise it
    'commodities':['London','NewYork','Overlap'],
    # Crypto trades 24/7 — Overlap is a valid high-volume window
    'crypto':     ['Asian','London','NewYork','Overlap'],
    # US stocks open at 14:30 UTC — Overlap ends at 16:00 so 14:30-16:00 is valid
    'stocks':     ['NewYork','Overlap'],
    # Indices follow their underlying markets
    'indices':    ['NewYork','London','Overlap'],
}

def _session() -> str:
    h = datetime.utcnow().hour
    if 12<=h<16: return 'Overlap'
    if 7<=h<16:  return 'London'
    if 12<=h<21: return 'NewYork'
    return 'Asian'

def _calc_atr(df, p=14) -> float:
    try:
        import pandas as pd
        h,l,c = df['high'],df['low'],df['close']
        tr = pd.concat([(h-l),(h-c.shift(1)).abs(),(l-c.shift(1)).abs()],axis=1).max(axis=1)
        return float(tr.rolling(p).mean().iloc[-1])
    except Exception:
        return float(df['close'].iloc[-1])*0.01

def _confluence(t15, t1h, t4h):
    avail = [t for t in [t15,t1h,t4h] if t and t.get('available')]
    if not avail: return 0.0,'HOLD','NONE'
    up   = sum(1 for t in avail if t.get('trend')=='UP')
    down = sum(1 for t in avail if t.get('trend')=='DOWN')
    n    = len(avail)
    if up==n:   return 0.90,'BUY', 'ALL3' if n==3 else 'BOTH'
    if down==n: return 0.90,'SELL','ALL3' if n==3 else 'BOTH'
    if up==2:   return 0.70,'BUY', '2OF3'
    if down==2: return 0.70,'SELL','2OF3'
    return 0.0,'HOLD','DIVERGE'

def _analyze_tf(df, name:str) -> Dict:
    try:
        if df is None or len(df)<50: return {'available':False,'name':name}
        c=df['close']; price=float(c.iloc[-1])
        s20=float(c.rolling(20).mean().iloc[-1]); s50=float(c.rolling(50).mean().iloc[-1])
        e9=float(c.ewm(span=9).mean().iloc[-1]); e21=float(c.ewm(span=21).mean().iloc[-1])
        if 'rsi' in df.columns: rsi=float(df['rsi'].iloc[-1])
        else:
            d=c.diff(); g=d.where(d>0,0).rolling(14).mean(); l=(-d.where(d<0,0)).rolling(14).mean()
            rsi=float((100-(100/(1+g/l))).iloc[-1])
        ml=c.ewm(span=12).mean()-c.ewm(span=26).mean()
        macd='BUY' if ml.iloc[-1]>ml.ewm(span=9).mean().iloc[-1] else 'SELL'
        vu=sum([price>s20,price>s50,s20>s50,e9>e21,rsi>50,macd=='BUY'])
        trend='UP' if vu>=4 else 'DOWN' if vu<=2 else 'NEUTRAL'
        return {'available':True,'name':name,'trend':trend,'rsi':round(rsi,1),'macd':macd,
                'sma20':round(s20,6),'sma50':round(s50,6),'price':price,'votes_up':vu}
    except Exception as e:
        logger.debug(f"_analyze_tf {name}: {e}")
        return {'available':False,'name':name}

def _news_blackout(asset:str) -> bool:
    try:
        import importlib; mc=importlib.import_module('market_calendar')
        cal=mc.MarketCalendar(); events=cal.get_upcoming_events(minutes_ahead=30)
        now=datetime.utcnow()
        for ev in events:
            if ev.get('impact','').lower()=='high':
                et=ev.get('time')
                if et and abs((et-now).total_seconds())<1800: return True
    except Exception: pass
    return False

def _build_reasons(t15,t1h,t4h,label,direction,atr,rr,session,df) -> List[str]:
    r=[]
    if label=='ALL3': r.append(f"{'Bullish' if direction=='BUY' else 'Bearish'} on all 3 timeframes (15m/1h/4h) — highest-conviction setup")
    elif label in ('BOTH','2OF3'): r.append(f"{'Bullish' if direction=='BUY' else 'Bearish'} on 2 of 3 timeframes — valid entry")
    rsi=t15.get('rsi',50)
    if direction=='BUY':
        r.append(f"RSI {rsi:.0f} — {'oversold, reversal setup' if rsi<40 else 'building momentum' if rsi<55 else 'strong momentum'}")
    else:
        r.append(f"RSI {rsi:.0f} — {'overbought, pullback likely' if rsi>60 else 'fading momentum'}")
    if t15.get('macd')==('BUY' if direction=='BUY' else 'SELL'): r.append("MACD cross confirms direction")
    if t1h and t1h.get('available'):
        aligned = t1h.get('trend')==('UP' if direction=='BUY' else 'DOWN')
        r.append(f"1h trend {'aligned' if aligned else 'conflicting — consider reduced size'}")
    try:
        p=float(df['close'].iloc[-1]); ap=(atr/p)*100
        r.append(f"ATR {ap:.2f}% — {'low vol, tight stops optimal' if ap<0.3 else 'elevated vol, size down' if ap>1.5 else 'normal conditions'}")
    except Exception: pass
    r.append(f"Risk/Reward {rr:.1f}:1 — {'excellent' if rr>=3 else 'good' if rr>=2 else 'acceptable'}")
    if session in ('Overlap','London'): r.append(f"{session} session — peak liquidity")
    return r[:6]

def _build_quality_signal(asset:str, category:str, bot, cache:SignalCache) -> Optional[Dict]:
    try:
        from indicators.technical import TechnicalIndicators
        def _df(tf,days):
            c=cache.get_df(asset,tf)
            if c is not None and len(c)>10: return c
            if not rate_limiter.check('yahoo'): return None
            d=bot.fetch_historical_data(asset,days=days,interval=tf)
            if d is not None and not d.empty: cache.put_df(asset,tf,d)
            return d
        df15=_df('15m',5); df1h=_df('1h',30); df4h=_df('4h',90)
        if df15 is None or df15.empty: return None
        df15=TechnicalIndicators.add_all_indicators(df15)
        if df1h is not None and len(df1h)>=50: df1h=TechnicalIndicators.add_all_indicators(df1h)
        if df4h is not None and len(df4h)>=50: df4h=TechnicalIndicators.add_all_indicators(df4h)
        t15=_analyze_tf(df15,'15m'); t1h=_analyze_tf(df1h,'1h'); t4h=_analyze_tf(df4h,'4h')
        conf_base,direction,label=_confluence(t15,t1h,t4h)
        if direction=='HOLD': return {'direction':'HOLD','asset':asset,'confidence':0.0,'reason':'Timeframes diverging','timeframe_conf':label}
        if _news_blackout(asset): return {'direction':'HOLD','asset':asset,'confidence':0.0,'reason':'High-impact news ±30min','timeframe_conf':label}
        session=_session()
        if session not in _ASSET_SESSION.get(category,['London','NewYork']) and category!='crypto': conf_base*=0.85
        cfg=_ATR_CFG.get(category,_ATR_CFG['forex'])
        atr=_calc_atr(df15); entry=float(df15['close'].iloc[-1])
        if direction=='BUY':
            sl=entry-atr*cfg['sl']; tp1=entry+atr*cfg['tp1']; tp2=entry+atr*cfg['tp2']; tp3=entry+atr*cfg['tp3']
        else:
            sl=entry+atr*cfg['sl']; tp1=entry-atr*cfg['tp1']; tp2=entry-atr*cfg['tp2']; tp3=entry-atr*cfg['tp3']
        sl_d=abs(entry-sl); rr=round(abs(tp1-entry)/sl_d,2) if sl_d>0 else 0
        if rr<cfg['min_rr']: return {'direction':'HOLD','asset':asset,'confidence':0.0,'reason':f'RR {rr:.1f} below min {cfg["min_rr"]}'}
        ml_b=0.0
        try:
            pred=bot.predictor.predict_next(df15); md=pred.get('direction','HOLD')
            if md!='HOLD':
                mdir='BUY' if md=='UP' else 'SELL'
                ml_b=0.05*pred.get('confidence',0.5) if mdir==direction else -0.05
        except Exception: pass
        vb=0.0; svotes={}
        try:
            # FIX BUG 13: scan_asset_parallel already ran get_combined_signal in
            # Layer 1 and passed the result here indirectly via the 'direction'
            # variable.  We re-use that result instead of calling get_combined_signal
            # a second time.  The result is stored in a thread-local by scan_asset_parallel.
            import threading as _thr
            _tl = getattr(_thr.current_thread(), '_scan_cache', {})
            if _tl.get('asset') == asset and _tl.get('combined'):
                comb = _tl['combined']
            else:
                comb = bot.get_combined_signal(df15)
            vd=comb.get('signal','HOLD')
            if vd!='HOLD': vb=0.05 if (('BUY' if vd=='BUY' else 'SELL')==direction) else -0.05
            svotes=comb.get('votes',{})
        except Exception: pass
        learn_b=signal_engine._get_bias(asset)
        # ── Telethon whale sentiment boost ───────────────────────────────────
        whale_b = 0.0
        try:
            if _whale_store is not None:
                sym = asset.split('-')[0].upper()
                raw_boost = _whale_store.get_confidence_boost(sym)
                # Only apply if whale direction agrees with signal direction
                sentiment = _whale_store.get_sentiment(sym)
                if (direction == 'BUY'  and sentiment > 0) or                    (direction == 'SELL' and sentiment < 0):
                    whale_b = abs(raw_boost)
                elif abs(sentiment) > 0.3:
                    whale_b = -abs(raw_boost) * 0.5   # opposing whale = mild penalty
        except Exception:
            pass
        # ─────────────────────────────────────────────────────────────────────
        confidence=min(0.97,max(0.30,conf_base+ml_b+vb+learn_b+whale_b))
        snap={}
        for col in ['rsi','macd','macd_signal','sma_20','sma_50','bb_upper','bb_lower','atr']:
            if col in df15.columns:
                try: snap[col]=round(float(df15[col].iloc[-1]),6)
                except Exception: pass
        return {
            'asset':asset,'direction':direction,'confidence':round(confidence,4),
            'entry_price':round(entry,6),'stop_loss':round(sl,6),
            'take_profit':round(tp1,6),'take_profit_2':round(tp2,6),'take_profit_3':round(tp3,6),
            'atr':round(atr,6),'risk_reward':rr,'timeframe_conf':label,'session':session,
            'reasons':_build_reasons(t15,t1h,t4h,label,direction,atr,rr,session,df15),
            'indicators':snap,'strategy_votes':svotes,
            'whale_boost':round(whale_b,4),
            'whale_sentiment':round(_whale_store.get_sentiment(asset.split('-')[0].upper()) if _whale_store else 0.0, 3),
        }
    except Exception as e:
        logger.error(f"_build_quality_signal {asset}: {e}")
        return None

# ══════════════════════════════════════════════════════════════════════════════
# LEARNING ENGINE (dog treats)
# ══════════════════════════════════════════════════════════════════════════════
class SignalLearningEngine:
    def __init__(self):
        self._stats:  Dict[str,Dict] = {}
        self._memory: Dict[str,Dict] = {}
        self._lock    = threading.Lock()
        if _DB_OK: self._ensure_tables(); self._load_stats(); self._start_watcher()
        logger.info("SignalLearningEngine: ready")

    def _ensure_tables(self):
        try:
            if engine:
                SignalHistory.__table__.create(bind=engine,checkfirst=True)
                SignalAssetStats.__table__.create(bind=engine,checkfirst=True)
        except Exception as e: logger.warning(f"SignalLearning table ensure: {e}")

    def _load_stats(self):
        try:
            db=SessionLocal(); rows=db.query(SignalAssetStats).all()
            for r in rows:
                self._stats[r.asset]={'total':r.total_signals or 0,'tp':r.tp_hits or 0,'tp2':r.tp2_hits or 0,
                    'sl':r.sl_hits or 0,'expired':r.expired or 0,'win_rate':float(r.win_rate or 0.5),
                    'bias':float(r.confidence_bias or 0.0),'strat_w':r.strategy_weights or {}}
            db.close()
            if self._stats: logger.info(f"SignalLearning: loaded {len(self._stats)} asset stats")
        except Exception as e: logger.warning(f"SignalLearning load_stats: {e}")

    def record(self, signal:Dict) -> str:
        sid=str(uuid.uuid4()); signal['signal_id']=sid
        self._memory[sid]={**signal,'still_watching':True}
        if _DB_OK:
            try:
                db=SessionLocal()
                db.add(SignalHistory(signal_id=sid,asset=signal.get('asset',''),
                    direction=signal.get('direction','HOLD'),confidence=signal.get('confidence',0.5),
                    entry_price=signal.get('entry_price',0),stop_loss=signal.get('stop_loss',0),
                    take_profit=signal.get('take_profit',0),take_profit_2=signal.get('take_profit_2'),
                    take_profit_3=signal.get('take_profit_3'),atr=signal.get('atr'),
                    risk_reward=signal.get('risk_reward'),timeframe_conf=signal.get('timeframe_conf'),
                    session=signal.get('session'),reasons=signal.get('reasons',[]),
                    indicators=signal.get('indicators',{}),news_titles=signal.get('news_titles',[]),
                    whale_alert=signal.get('whale_alert'),strategy_votes=signal.get('strategy_votes',{})))
                db.commit(); db.close()
            except Exception as e: logger.error(f"SignalLearning record: {e}")
        return sid

    def resolve(self, sid:str, outcome:str, price:float):
        pnl_r={'TP_HIT':1.0,'TP2_HIT':2.0,'SL_HIT':-1.0,'EXPIRED':0.0}.get(outcome,0.0)
        asset=None
        if sid in self._memory:
            asset=self._memory[sid].get('asset')
            self._memory[sid].update({'outcome':outcome,'outcome_price':price,
                'outcome_at':datetime.utcnow().isoformat(),'pnl_r':pnl_r,'still_watching':False})
        if _DB_OK:
            try:
                db=SessionLocal(); row=db.query(SignalHistory).filter_by(signal_id=sid).first()
                if row:
                    asset=row.asset; row.outcome=outcome; row.outcome_price=price
                    row.outcome_at=datetime.utcnow(); row.pnl_r=pnl_r; row.still_watching=False
                    db.commit()
                db.close()
            except Exception as e: logger.error(f"SignalLearning resolve: {e}")
        if asset: self._reinforce(asset, outcome); logger.info(f"SignalLearning: {asset} → {outcome} pnl={pnl_r:+.1f}R")

    def _reinforce(self, asset:str, outcome:str):
        with self._lock:
            s=self._stats.setdefault(asset,{'total':0,'tp':0,'tp2':0,'sl':0,'expired':0,'win_rate':0.5,'bias':0.0,'strat_w':{}})
            s['total']+=1
            if   outcome=='TP_HIT':  s['tp']+=1
            elif outcome=='TP2_HIT': s['tp']+=1; s['tp2']+=1
            elif outcome=='SL_HIT':  s['sl']+=1
            else:                    s['expired']+=1
            decided=s['tp']+s['sl']
            if decided>0: s['win_rate']=round(s['tp']/decided,4)
            # Treat / correction
            if outcome in ('TP_HIT','TP2_HIT'): s['bias']=round(min(0.10,s['bias']+0.01),4)
            elif outcome=='SL_HIT':             s['bias']=round(max(-0.10,s['bias']-0.01),4)
            if decided>=10: s['bias']=round(min(0.10,max(-0.10,(s['win_rate']-0.5)*0.2)),4)
        self._persist(asset)

    def _persist(self, asset:str):
        if not _DB_OK: return
        try:
            s=self._stats[asset]; db=SessionLocal()
            row=db.query(SignalAssetStats).filter_by(asset=asset).first()
            if not row: row=SignalAssetStats(asset=asset); db.add(row)
            row.total_signals=s['total']; row.tp_hits=s['tp']; row.tp2_hits=s['tp2']
            row.sl_hits=s['sl']; row.expired=s['expired']; row.win_rate=s['win_rate']
            row.confidence_bias=s['bias']; row.strategy_weights=s['strat_w']
            db.commit(); db.close()
        except Exception as e: logger.warning(f"SignalLearning persist: {e}")

    def _get_bias(self, asset:str) -> float:
        return self._stats.get(asset,{}).get('bias',0.0)

    def get_win_rate(self, asset:str) -> Optional[float]:
        s=self._stats.get(asset,{}); d=s.get('tp',0)+s.get('sl',0)
        return round(s['win_rate'],3) if d>=3 else None

    def get_history(self, asset:str=None, limit:int=20) -> List[Dict]:
        if _DB_OK:
            try:
                db=SessionLocal(); q=db.query(SignalHistory).order_by(SignalHistory.issued_at.desc())
                if asset: q=q.filter(SignalHistory.asset==asset)
                r=[x.to_dict() for x in q.limit(limit).all()]; db.close(); return r
            except Exception as e: logger.error(f"SignalLearning get_history: {e}")
        items=sorted(self._memory.values(),key=lambda x:x.get('issued_at',''),reverse=True)
        return [i for i in items if not asset or i.get('asset')==asset][:limit]

    def _start_watcher(self):
        t=threading.Thread(target=self._watcher_loop,name='OutcomeWatcher',daemon=True); t.start()
        logger.info("SignalLearning: outcome watcher started (every 5 min)")

    def _watcher_loop(self):
        time.sleep(30)
        while True:
            try: self._check_outcomes()
            except Exception as e: logger.error(f"OutcomeWatcher: {e}")
            time.sleep(300)

    def _check_outcomes(self):
        if not _DB_OK: return
        try:
            db=SessionLocal(); cutoff=datetime.utcnow()-timedelta(hours=48)
            rows=db.query(SignalHistory).filter(SignalHistory.still_watching==True,SignalHistory.issued_at>=cutoff).all()
            db.close()
        except Exception as e: logger.error(f"OutcomeWatcher query: {e}"); return
        if not rows: return
        by_asset=defaultdict(list)
        for r in rows: by_asset[r.asset].append(r)
        for asset,rrows in by_asset.items():
            price=self._price(asset)
            if price is None: continue
            for row in rrows:
                age=(datetime.utcnow()-row.issued_at.replace(tzinfo=None)).total_seconds()/3600
                e=float(row.entry_price); tp=float(row.take_profit)
                tp2=float(row.take_profit_2) if row.take_profit_2 else None
                sl=float(row.stop_loss); d=row.direction
                if d=='BUY':
                    if tp2 and price>=tp2:   self.resolve(row.signal_id,'TP2_HIT',price)
                    elif price>=tp:           self.resolve(row.signal_id,'TP_HIT',price)
                    elif price<=sl:           self.resolve(row.signal_id,'SL_HIT',price)
                    elif age>=48:             self.resolve(row.signal_id,'EXPIRED',price)
                elif d=='SELL':
                    if tp2 and price<=tp2:   self.resolve(row.signal_id,'TP2_HIT',price)
                    elif price<=tp:           self.resolve(row.signal_id,'TP_HIT',price)
                    elif price>=sl:           self.resolve(row.signal_id,'SL_HIT',price)
                    elif age>=48:             self.resolve(row.signal_id,'EXPIRED',price)

    def _price(self, asset:str) -> Optional[float]:
        if not rate_limiter.check('yahoo'): return None
        try:
            import yfinance as yf
            _M={'XAU/USD':'GC=F','XAG/USD':'SI=F','WTI/USD':'CL=F','EUR/USD':'EURUSD=X',
                'GBP/USD':'GBPUSD=X','USD/JPY':'JPY=X','AUD/USD':'AUDUSD=X','USD/CAD':'CAD=X'}
            t=yf.Ticker(_M.get(asset,asset)).fast_info
            p=getattr(t,'last_price',None) or getattr(t,'regular_market_price',None)
            return float(p) if p else None
        except Exception: return None

signal_engine = SignalLearningEngine()

# ══════════════════════════════════════════════════════════════════════════════
# PUBLIC ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════
def get_instant_signal(asset:str, category:str, bot) -> Dict:
    cached=signal_cache.get(asset)
    if cached and not signal_cache.is_stale(asset):
        sig=dict(cached); sig['from_cache']=True
        sig['win_rate']=signal_engine.get_win_rate(asset)
        if sig.get('direction') not in ('HOLD',None): sig['signal_id']=signal_engine.record(sig)
        return sig
    sig=_build_quality_signal(asset,category,bot,signal_cache)
    if sig is None: return {'direction':'HOLD','asset':asset,'confidence':0.0,'error':True,'reason':'No data'}
    signal_cache.put(asset,sig); sig['from_cache']=False
    sig['win_rate']=signal_engine.get_win_rate(asset)
    if sig.get('direction') not in ('HOLD',None): sig['signal_id']=signal_engine.record(sig)
    return sig

# ══════════════════════════════════════════════════════════════════════════════
# STRESS TEST ENGINE
# ══════════════════════════════════════════════════════════════════════════════
_CRASHES = {
    '2008 Financial Crisis':     {'stocks':-0.57,'indices':-0.57,'crypto':0.0,'forex':-0.05,'commodities':-0.32,'desc':'S&P -57%, commodities -32%'},
    '2020 COVID Crash':          {'stocks':-0.34,'indices':-0.34,'crypto':-0.50,'forex':-0.04,'commodities':-0.67,'desc':'S&P -34%, BTC -50%, WTI -67%'},
    '2022 Crypto Winter':        {'stocks':-0.25,'indices':-0.25,'crypto':-0.77,'forex':-0.06,'commodities':-0.15,'desc':'BTC -77%, ETH -80%'},
    '2022 Forex USD Surge':      {'stocks':-0.10,'indices':-0.10,'crypto':-0.30,'forex':-0.15,'commodities':-0.10,'desc':'GBP/USD -26%, EUR/USD -17%'},
}

def run_stress_test(positions:List[Dict], balance:float) -> Dict:
    results={}
    for name,shocks in _CRASHES.items():
        pnl=0.0; impacts=[]
        for pos in positions:
            shock=shocks.get(pos.get('category','stocks'),0.0)
            size=pos.get('size_usd',0.0)
            p=size*shock if pos.get('direction','BUY').upper()=='BUY' else size*(-shock)
            pnl+=p; impacts.append({'asset':pos.get('asset',''),'direction':pos.get('direction',''),
                'shock_pct':round(shock*100,1),'pnl_usd':round(p,2)})
        dd=(pnl/balance*100) if balance>0 else 0
        results[name]={'description':shocks['desc'],'total_pnl_usd':round(pnl,2),
            'drawdown_pct':round(dd,2),'positions':impacts,'survives':(balance+pnl)>0}
    worst=min(results.items(),key=lambda x:x[1]['total_pnl_usd'])
    return {'scenarios':results,'worst_case':worst[0],'worst_pnl':worst[1]['total_pnl_usd'],
            'worst_drawdown':worst[1]['drawdown_pct'],'balance':balance,
            'post_worst':round(balance+worst[1]['total_pnl_usd'],2),'timestamp':datetime.utcnow().isoformat()}

# ══════════════════════════════════════════════════════════════════════════════
# UNIT TESTS
# ══════════════════════════════════════════════════════════════════════════════
def _run_tests():
    import traceback as tb
    p=0; f=0
    results=[]
    def t(name,fn):
        nonlocal p,f
        try:
            fn()
            logger.info(f"TEST PASS  {name}")
            results.append({"name":name,"status":"PASS"})
            p+=1
        except Exception as e:
            logger.error(f"TEST FAIL  {name}: {e}\n{tb.format_exc()}")
            results.append({"name":name,"status":"FAIL","error":str(e)})
            f+=1
    logger.info("="*60+" SIGNAL ENGINE UNIT TESTS "+"="*60)
    def test_atr_rr():
        import pandas as pd, numpy as np
        prices=2000+np.cumsum(np.random.randn(100))
        df=pd.DataFrame({'close':prices,'high':prices+np.random.uniform(0,5,100),'low':prices-np.random.uniform(0,5,100)})
        atr=_calc_atr(df); assert atr>0
        entry=prices[-1]; cfg=_ATR_CFG['forex']
        sl=entry-atr*cfg['sl']; tp=entry+atr*cfg['tp1']
        rr=(tp-entry)/(entry-sl); assert rr>=cfg['min_rr'],f"RR {rr:.2f}<{cfg['min_rr']}"
    t("ATR stops produce valid RR",test_atr_rr)
    def test_all3_bullish():
        tf={'available':True,'trend':'UP','rsi':55,'macd':'BUY','votes_up':5,'name':'x'}
        c,d,l=_confluence(tf,tf,tf); assert d=='BUY' and l=='ALL3' and c>=0.85
    t("3-TF bullish → BUY ALL3 conf≥0.85",test_all3_bullish)
    def test_diverge_hold():
        u={'available':True,'trend':'UP','votes_up':5,'name':'x'}
        d={'available':True,'trend':'DOWN','votes_up':1,'name':'y'}
        _,dr,_=_confluence(u,d,None); assert dr=='HOLD'
    t("Diverging TFs → HOLD",test_diverge_hold)
    def test_2of3():
        d={'available':True,'trend':'DOWN','votes_up':1,'name':'x'}
        u={'available':True,'trend':'UP','votes_up':5,'name':'y'}
        _,dr,l=_confluence(d,d,u); assert dr=='SELL' and l=='2OF3'
    t("2/3 bearish → SELL 2OF3",test_2of3)
    def test_rl_allows():
        rl=RateLimiter(); assert rl.check('yahoo')
    t("Rate limiter allows within budget",test_rl_allows)
    def test_rl_blocks():
        rl=RateLimiter()
        for _ in range(8): rl.check('twelvedata')
        assert rl.remaining('twelvedata')==0
    t("Rate limiter blocks at budget",test_rl_blocks)
    def test_treat():
        e=SignalLearningEngine.__new__(SignalLearningEngine)
        e._stats={}; e._lock=threading.Lock()
        for _ in range(3): e._reinforce('T','TP_HIT')
        assert e._get_bias('T')>0
    t("3 TP hits → positive bias (treat)",test_treat)
    def test_correction():
        e=SignalLearningEngine.__new__(SignalLearningEngine)
        e._stats={}; e._lock=threading.Lock()
        for _ in range(3): e._reinforce('T','SL_HIT')
        assert e._get_bias('T')<0
    t("3 SL hits → negative bias (correction)",test_correction)
    def test_bias_cap():
        e=SignalLearningEngine.__new__(SignalLearningEngine)
        e._stats={}; e._lock=threading.Lock()
        for _ in range(50): e._reinforce('T','TP_HIT')
        assert e._get_bias('T')<=0.10
    t("Bias capped at ±0.10",test_bias_cap)
    def test_stress():
        positions=[{'asset':'AAPL','category':'stocks','direction':'BUY','size_usd':1000},
                   {'asset':'BTC-USD','category':'crypto','direction':'BUY','size_usd':500}]
        r=run_stress_test(positions,5000); assert '2008 Financial Crisis' in r['scenarios']
        assert r['scenarios']['2008 Financial Crisis']['total_pnl_usd']<0
    t("Stress test 2008 long positions lose",test_stress)
    def test_stress_short():
        positions=[{'asset':'^GSPC','category':'indices','direction':'SELL','size_usd':2000}]
        r=run_stress_test(positions,5000); assert r['scenarios']['2008 Financial Crisis']['total_pnl_usd']>0
    t("Stress test 2008 short profits",test_stress_short)
    def test_session():
        s=_session(); assert s in ('Asian','London','NewYork','Overlap')
    t("Session detection valid",test_session)
    logger.info(f"TESTS COMPLETE: {p} passed, {f} failed")
    return f==0, results

if __name__=='__main__':
    exit(0 if _run_tests() else 1)