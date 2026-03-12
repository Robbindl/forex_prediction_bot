"""
prediction_tracker.py — AI Prediction Accuracy Tracker
=======================================================
Records every signal generated, then checks the outcome at 1H, 4H, and 24H.
Builds rolling accuracy stats that power the accuracy dashboard.

A prediction is "correct" if:
  - Direction was right (price moved the correct way)
  - Bonus: target price was actually hit within the horizon

Stores all data in PostgreSQL.  Publishes live accuracy to Redis.
"""

import os
import sys
import time
import json
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from collections import defaultdict

from logger import logger

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from redis_broker import broker as _broker
except Exception:
    _broker = None

try:
    from data.fetcher import NASALevelFetcher
    _fetcher = NASALevelFetcher()
except Exception:
    _fetcher = None

try:
    from services.database_service import DatabaseService
    _db = DatabaseService()
    _DB_AVAILABLE = True
except Exception:
    _db = None
    _DB_AVAILABLE = False


HORIZONS = [60, 240, 1440]    # 1H, 4H, 24H in minutes
HORIZON_LABELS = {60: '1H', 240: '4H', 1440: '24H'}


class PredictionTracker:
    """
    Records signals and evaluates their accuracy after each horizon.
    """

    def __init__(self):
        self._pending: List[Dict] = []       # signals awaiting evaluation
        self._lock    = threading.Lock()
        self._running = False
        self._stats_cache: Dict = {}         # cached accuracy stats
        self._cache_ts: float = 0
        self._ensure_table()

    # ── DB setup ───────────────────────────────────────────────────────────

    def _ensure_table(self):
        if not _DB_AVAILABLE:
            return
        try:
            with _db.get_session() as session:
                from sqlalchemy import text
                session.execute(text("""
                    CREATE TABLE IF NOT EXISTS prediction_outcomes (
                        id              SERIAL PRIMARY KEY,
                        asset           TEXT NOT NULL,
                        category        TEXT,
                        direction       TEXT NOT NULL,
                        entry_price     FLOAT,
                        target_price    FLOAT,
                        confidence      FLOAT,
                        signal_time     TIMESTAMP NOT NULL,
                        horizon_minutes INT NOT NULL,
                        eval_time       TIMESTAMP,
                        actual_price    FLOAT,
                        direction_correct BOOLEAN,
                        target_hit      BOOLEAN,
                        pct_move        FLOAT,
                        evaluated       BOOLEAN DEFAULT FALSE,
                        strategy        TEXT,
                        session         TEXT,
                        regime          TEXT
                    )
                """))
                session.commit()
                logger.info("[PredTracker] Table ready")
        except Exception as e:
            logger.warning(f"[PredTracker] Table creation failed: {e}")

    # ── Record a new signal ────────────────────────────────────────────────

    def record_signal(self, signal: Dict):
        """
        Call this whenever a signal passes the 7-layer quality gate.
        signal must have: asset, signal (direction), entry_price, confidence
        Optional: tp1, stop_loss, category, strategy, session, regime
        """
        now        = datetime.utcnow()
        asset      = signal.get('asset', '')
        direction  = signal.get('signal', signal.get('direction', 'HOLD'))
        entry      = signal.get('entry_price', signal.get('entry', 0))
        confidence = signal.get('confidence', 0.5)
        target     = signal.get('tp1', signal.get('take_profit', 0))
        category   = signal.get('category', '')
        strategy   = signal.get('strategy', '')
        sess       = signal.get('session', '')
        regime     = signal.get('regime', '')

        if direction == 'HOLD' or not asset or not entry:
            return

        records = []
        for horizon in HORIZONS:
            rec = {
                'asset':           asset,
                'category':        category,
                'direction':       direction,
                'entry_price':     float(entry),
                'target_price':    float(target) if target else None,
                'confidence':      float(confidence),
                'signal_time':     now.isoformat(),
                'horizon_minutes': horizon,
                'eval_time':       (now + timedelta(minutes=horizon)).isoformat(),
                'strategy':        strategy,
                'session':         sess,
                'regime':          regime,
                'evaluated':       False,
            }
            records.append(rec)

        with self._lock:
            self._pending.extend(records)

        self._store_pending(records)
        logger.debug(f"[PredTracker] Recorded {asset} {direction} @ {entry:.5f} — {len(records)} horizons queued")

    # ── Evaluate pending predictions ───────────────────────────────────────

    def _evaluate_due(self):
        """Check if any pending predictions have reached their evaluation time."""
        if not _fetcher:
            return

        now   = datetime.utcnow()
        to_eval = []

        with self._lock:
            still_pending = []
            for rec in self._pending:
                eval_time = datetime.fromisoformat(rec['eval_time'])
                if now >= eval_time:
                    to_eval.append(rec)
                else:
                    still_pending.append(rec)
            self._pending = still_pending

        for rec in to_eval:
            try:
                price = self._get_current_price(rec['asset'], rec.get('category', ''))
                if price is None:
                    continue

                entry     = rec['entry_price']
                direction = rec['direction']
                target    = rec.get('target_price')

                pct_move = (price - entry) / entry * 100
                direction_correct = (
                    (direction == 'BUY'  and price > entry) or
                    (direction == 'SELL' and price < entry)
                )
                target_hit = False
                if target and target > 0:
                    if direction == 'BUY':
                        target_hit = price >= target
                    else:
                        target_hit = price <= target

                rec.update({
                    'actual_price':      price,
                    'direction_correct': direction_correct,
                    'target_hit':        target_hit,
                    'pct_move':          pct_move,
                    'evaluated':         True,
                })

                self._store_outcome(rec)
                self._invalidate_cache()

                logger.debug(
                    f"[PredTracker] Evaluated {rec['asset']} {direction} "
                    f"{HORIZON_LABELS[rec['horizon_minutes']]} | "
                    f"{'✓' if direction_correct else '✗'} | move={pct_move:+.2f}%"
                )
            except Exception as e:
                logger.debug(f"[PredTracker] Eval error {rec.get('asset')}: {e}")

    def _get_current_price(self, asset: str, category: str) -> Optional[float]:
        try:
            price, _ = _fetcher.get_real_time_price(asset, category)
            return float(price) if price else None
        except Exception:
            return None

    # ── Storage ────────────────────────────────────────────────────────────

    def _store_pending(self, records: List[Dict]):
        if not _DB_AVAILABLE:
            return
        try:
            with _db.get_session() as session:
                from sqlalchemy import text
                for r in records:
                    session.execute(text("""
                        INSERT INTO prediction_outcomes
                            (asset, category, direction, entry_price, target_price,
                             confidence, signal_time, horizon_minutes, eval_time,
                             strategy, session, regime, evaluated)
                        VALUES
                            (:asset, :category, :direction, :entry_price, :target_price,
                             :confidence, :signal_time, :horizon_minutes, :eval_time,
                             :strategy, :session, :regime, false)
                    """), {
                        'asset':          r['asset'],
                        'category':       r.get('category', ''),
                        'direction':      r['direction'],
                        'entry_price':    r['entry_price'],
                        'target_price':   r.get('target_price'),
                        'confidence':     r['confidence'],
                        'signal_time':    r['signal_time'],
                        'horizon_minutes':r['horizon_minutes'],
                        'eval_time':      r['eval_time'],
                        'strategy':       r.get('strategy', ''),
                        'session':        r.get('session', ''),
                        'regime':         r.get('regime', ''),
                    })
                session.commit()
        except Exception as e:
            logger.debug(f"[PredTracker] Store pending failed: {e}")

    def _store_outcome(self, rec: Dict):
        if not _DB_AVAILABLE:
            return
        try:
            with _db.get_session() as session:
                from sqlalchemy import text
                session.execute(text("""
                    UPDATE prediction_outcomes SET
                        actual_price      = :actual,
                        direction_correct = :correct,
                        target_hit        = :hit,
                        pct_move          = :move,
                        evaluated         = true
                    WHERE asset = :asset
                      AND signal_time = :signal_time
                      AND horizon_minutes = :horizon
                """), {
                    'actual':      rec['actual_price'],
                    'correct':     rec['direction_correct'],
                    'hit':         rec['target_hit'],
                    'move':        rec['pct_move'],
                    'asset':       rec['asset'],
                    'signal_time': rec['signal_time'],
                    'horizon':     rec['horizon_minutes'],
                })
                session.commit()
        except Exception as e:
            logger.debug(f"[PredTracker] Store outcome failed: {e}")

    # ── Accuracy stats ─────────────────────────────────────────────────────

    def _invalidate_cache(self):
        self._cache_ts = 0

    def get_accuracy_stats(self, days_back: int = 30) -> Dict:
        """
        Returns accuracy statistics.
        Cached for 5 minutes to avoid hammering DB.
        """
        now = time.time()
        if now - self._cache_ts < 300:
            return self._stats_cache

        stats = self._compute_stats(days_back)
        self._stats_cache = stats
        self._cache_ts = now

        if _broker:
            _broker.publish('predictions', {'type': 'accuracy_update', 'stats': stats})

        return stats

    def _compute_stats(self, days_back: int) -> Dict:
        if not _DB_AVAILABLE:
            return self._empty_stats()

        since = (datetime.utcnow() - timedelta(days=days_back)).isoformat()

        try:
            with _db.get_session() as session:
                from sqlalchemy import text
                rows = session.execute(text("""
                    SELECT
                        horizon_minutes,
                        COUNT(*) AS total,
                        SUM(CASE WHEN direction_correct THEN 1 ELSE 0 END) AS correct,
                        SUM(CASE WHEN target_hit THEN 1 ELSE 0 END) AS targets_hit,
                        AVG(pct_move) AS avg_move,
                        AVG(confidence) AS avg_confidence
                    FROM prediction_outcomes
                    WHERE evaluated = true AND signal_time >= :since
                    GROUP BY horizon_minutes
                """), {'since': since}).fetchall()

                by_horizon = {}
                for row in rows:
                    h    = row[0]
                    tot  = row[1] or 0
                    corr = row[2] or 0
                    acc  = round(corr / tot * 100, 1) if tot > 0 else 0
                    by_horizon[HORIZON_LABELS.get(h, f'{h}m')] = {
                        'total':          tot,
                        'correct':        corr,
                        'accuracy_pct':   acc,
                        'targets_hit':    row[3] or 0,
                        'avg_move_pct':   round(row[4] or 0, 3),
                        'avg_confidence': round(row[5] or 0, 3),
                    }

                # Per-asset accuracy
                asset_rows = session.execute(text("""
                    SELECT
                        asset,
                        horizon_minutes,
                        COUNT(*) AS total,
                        SUM(CASE WHEN direction_correct THEN 1 ELSE 0 END) AS correct
                    FROM prediction_outcomes
                    WHERE evaluated = true AND signal_time >= :since
                    GROUP BY asset, horizon_minutes
                    ORDER BY total DESC
                    LIMIT 50
                """), {'since': since}).fetchall()

                by_asset = defaultdict(dict)
                for row in asset_rows:
                    asset  = row[0]
                    label  = HORIZON_LABELS.get(row[1], f'{row[1]}m')
                    tot    = row[2] or 0
                    corr   = row[3] or 0
                    acc    = round(corr / tot * 100, 1) if tot > 0 else 0
                    by_asset[asset][label] = {'total': tot, 'accuracy_pct': acc}

                # Recent 20 outcomes
                recent_rows = session.execute(text("""
                    SELECT asset, direction, entry_price, actual_price,
                           direction_correct, pct_move, confidence, horizon_minutes, signal_time
                    FROM prediction_outcomes
                    WHERE evaluated = true
                    ORDER BY signal_time DESC
                    LIMIT 20
                """)).fetchall()

                recent = []
                for row in recent_rows:
                    recent.append({
                        'asset':    row[0],
                        'direction':row[1],
                        'entry':    row[2],
                        'actual':   row[3],
                        'correct':  row[4],
                        'move_pct': round(row[5] or 0, 3),
                        'confidence':row[6],
                        'horizon':  HORIZON_LABELS.get(row[7], f'{row[7]}m'),
                        'time':     str(row[8]),
                    })

                return {
                    'by_horizon':  by_horizon,
                    'by_asset':    dict(by_asset),
                    'recent':      recent,
                    'days_back':   days_back,
                    'updated_at':  datetime.utcnow().isoformat(),
                }
        except Exception as e:
            logger.warning(f"[PredTracker] Stats query failed: {e}")
            return self._empty_stats()

    def _empty_stats(self) -> Dict:
        return {
            'by_horizon': {
                '1H':  {'total':0,'correct':0,'accuracy_pct':0,'targets_hit':0,'avg_move_pct':0,'avg_confidence':0},
                '4H':  {'total':0,'correct':0,'accuracy_pct':0,'targets_hit':0,'avg_move_pct':0,'avg_confidence':0},
                '24H': {'total':0,'correct':0,'accuracy_pct':0,'targets_hit':0,'avg_move_pct':0,'avg_confidence':0},
            },
            'by_asset':  {},
            'recent':    [],
            'days_back': 30,
            'updated_at':datetime.utcnow().isoformat(),
        }

    # ── Background evaluation loop ─────────────────────────────────────────

    def start(self):
        if self._running:
            return
        self._running = True
        self._reload_pending_from_db()
        t = threading.Thread(target=self._eval_loop, name='PredTracker', daemon=True)
        t.start()
        logger.info("[PredTracker] Accuracy tracker started")

    def stop(self):
        self._running = False

    def _eval_loop(self):
        while self._running:
            try:
                self._evaluate_due()
            except Exception as e:
                logger.debug(f"[PredTracker] eval loop error: {e}")
            time.sleep(60)   # Check every minute

    def _reload_pending_from_db(self):
        """On startup, reload any unevaluated predictions from DB."""
        if not _DB_AVAILABLE:
            return
        try:
            with _db.get_session() as session:
                from sqlalchemy import text
                rows = session.execute(text("""
                    SELECT asset, category, direction, entry_price, target_price,
                           confidence, signal_time, horizon_minutes, eval_time,
                           strategy, session, regime
                    FROM prediction_outcomes
                    WHERE evaluated = false
                      AND eval_time > NOW() - INTERVAL '2 days'
                """)).fetchall()

                cols = ['asset','category','direction','entry_price','target_price',
                        'confidence','signal_time','horizon_minutes','eval_time',
                        'strategy','session','regime']
                with self._lock:
                    for row in rows:
                        rec = dict(zip(cols, row))
                        rec['signal_time'] = str(rec['signal_time'])
                        rec['eval_time']   = str(rec['eval_time'])
                        rec['evaluated']   = False
                        self._pending.append(rec)

                logger.info(f"[PredTracker] Reloaded {len(rows)} pending predictions from DB")
        except Exception as e:
            logger.debug(f"[PredTracker] Reload failed: {e}")


# ── Global singleton ──────────────────────────────────────────────────────────
prediction_tracker = PredictionTracker()
