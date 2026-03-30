import os
import sys
import time
import json
import threading
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from collections import defaultdict

import pandas as pd

from utils.logger import logger

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from redis_broker import broker as _broker
except Exception:
    _broker = None

try:
    import core.engine as _eng_mod
    _fetcher = getattr(getattr(_eng_mod, "_CORE_INSTANCE", None), "fetcher", None)
    if _fetcher is None:
        from data.fetcher import get_shared_fetcher
        _fetcher = get_shared_fetcher()
except Exception:
    _fetcher = None

try:
    from services.db_pool import get_db
    _db = get_db()
    _DB_AVAILABLE = True
except Exception:
    _db = None
    _DB_AVAILABLE = False

try:
    from data.cache import cache as _data_cache
except Exception:
    _data_cache = None


HORIZONS = [60, 240, 1440]    # 1H, 4H, 24H in minutes
HORIZON_LABELS = {60: '1H', 240: '4H', 1440: '24H'}
_EVAL_LOOKBACK_DAYS = 45
_INTERVAL_MINUTES = {"15m": 15, "1h": 60, "1d": 1440}
_INTERVAL_TOLERANCE_MINUTES = {"15m": 45, "1h": 180, "1d": 2880}


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
        self._live_outcomes_since_training: Dict[str, int] = defaultdict(int)
        self._live_training_lock = threading.Lock()
        self._live_training_running = False
        self._ensure_table()
        self.start()

    # ── DB setup ───────────────────────────────────────────────────────────

    def _ensure_table(self):
        if not _DB_AVAILABLE:
            return
        try:
            _db.ensure_prediction_outcomes_table()
            logger.info("[PredTracker] Table ready")
        except Exception as e:
            logger.warning(f"[PredTracker] Table creation failed: {e}")

    # ── Record a new signal ────────────────────────────────────────────────

    def record_signal(self, signal: Dict):
        """
        Call this whenever a signal passes the decision engine.
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

        features = signal.get('features')
        signal_features = None
        if features is not None:
            try:
                if hasattr(features, 'tolist'):
                    features = features.tolist()
                signal_features = json.dumps(features, default=str)
            except Exception:
                signal_features = None

        signal_metadata = signal.get('signal_metadata') or signal.get('metadata') or {}
        try:
            signal_metadata = json.dumps(signal_metadata, default=str)
        except Exception:
            signal_metadata = None

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
                'signal_features': signal_features,
                'signal_metadata': signal_metadata,
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

        now = datetime.utcnow()
        to_eval = []

        with self._lock:
            still_pending = []
            for rec in self._pending:
                eval_time = self._to_utc_naive(rec.get('eval_time'))
                if now >= eval_time:
                    to_eval.append(rec)
                else:
                    still_pending.append(rec)
            self._pending = still_pending

        history_cache: Dict[Tuple[str, str, str, int], Optional[pd.DataFrame]] = {}
        retry_pending = []

        for rec in to_eval:
            try:
                price = self._get_price_at_eval_time(
                    rec['asset'],
                    rec.get('category', ''),
                    rec.get('eval_time'),
                    int(rec.get('horizon_minutes') or 0),
                    history_cache,
                )
                if price is None:
                    rec['eval_attempts'] = int(rec.get('eval_attempts', 0)) + 1
                    retry_pending.append(rec)
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
                self._record_live_outcome(rec.get('category', ''))

                logger.debug(
                    f"[PredTracker] Evaluated {rec['asset']} {direction} "
                    f"{HORIZON_LABELS[rec['horizon_minutes']]} | "
                    f"{'✓' if direction_correct else '✗'} | move={pct_move:+.2f}%"
                )
            except Exception as e:
                rec['eval_attempts'] = int(rec.get('eval_attempts', 0)) + 1
                retry_pending.append(rec)
                logger.debug(f"[PredTracker] Eval error {rec.get('asset')}: {e}")

        if retry_pending:
            with self._lock:
                self._pending.extend(retry_pending)

    def _get_current_price(self, asset: str, category: str) -> Optional[float]:
        try:
            price, _ = _fetcher.get_real_time_price(asset, category)
            return float(price) if price else None
        except Exception:
            return None

    @staticmethod
    def _to_utc_naive(value: Any) -> datetime:
        ts = pd.Timestamp(value)
        if ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        else:
            ts = ts.tz_convert("UTC")
        return ts.tz_localize(None).to_pydatetime()

    @staticmethod
    def _pending_key(rec: Dict[str, Any]) -> Tuple[str, str, int]:
        return (
            str(rec.get('asset', '')),
            str(rec.get('signal_time', '')),
            int(rec.get('horizon_minutes') or 0),
        )

    @staticmethod
    def _history_intervals(horizon_minutes: int) -> List[str]:
        if horizon_minutes <= 240:
            return ["15m", "1h", "1d"]
        return ["1h", "1d"]

    def _history_periods(self, target_time: datetime, interval: str) -> int:
        interval_minutes = _INTERVAL_MINUTES.get(interval, 60)
        age_minutes = max(
            int((datetime.utcnow() - target_time).total_seconds() / 60),
            interval_minutes,
        )
        buffer_minutes = max(interval_minutes * 24, 180)
        periods = int((age_minutes + buffer_minutes) / interval_minutes) + 2
        max_periods = 6000 if interval == "15m" else 3000
        return max(100, min(periods, max_periods))

    def _get_price_at_eval_time(
        self,
        asset: str,
        category: str,
        eval_time: Any,
        horizon_minutes: int,
        history_cache: Dict[Tuple[str, str, str, int], Optional[pd.DataFrame]],
    ) -> Optional[float]:
        target_time = self._to_utc_naive(eval_time)
        for interval in self._history_intervals(horizon_minutes):
            periods = self._history_periods(target_time, interval)
            cache_key = (asset, category, interval, periods)
            if cache_key not in history_cache:
                try:
                    if hasattr(_fetcher, "invalidate_ohlcv_cache"):
                        _fetcher.invalidate_ohlcv_cache(asset, category=category, interval=interval)
                    elif _data_cache is not None:
                        _data_cache.delete(f"ohlcv:{asset}:{interval}")
                except Exception:
                    pass
                try:
                    history_cache[cache_key] = _fetcher.get_ohlcv(asset, category, interval, periods)
                except Exception as e:
                    logger.debug(f"[PredTracker] History fetch failed for {asset} {interval}: {e}")
                    history_cache[cache_key] = None

            price = self._extract_price_from_history(history_cache.get(cache_key), target_time, interval)
            if price is not None:
                return price

        if datetime.utcnow() - target_time <= timedelta(minutes=20):
            return self._get_current_price(asset, category)
        return None

    @staticmethod
    def _extract_price_from_history(
        df: Optional[pd.DataFrame],
        target_time: datetime,
        interval: str,
    ) -> Optional[float]:
        if df is None or df.empty or 'close' not in df.columns:
            return None

        closes = df['close'].dropna()
        if closes.empty:
            return None

        try:
            ts_index = pd.to_datetime(closes.index, utc=True).tz_convert(None)
            deltas = (ts_index - pd.Timestamp(target_time)).to_series().abs()
            if deltas.empty:
                return None

            nearest_label = deltas.idxmin()
            tolerance = pd.Timedelta(minutes=_INTERVAL_TOLERANCE_MINUTES.get(interval, 180))
            if deltas.loc[nearest_label] > tolerance:
                return None

            nearest_pos = ts_index.get_loc(nearest_label)
            if isinstance(nearest_pos, slice):
                nearest_pos = nearest_pos.start
            elif isinstance(nearest_pos, (list, tuple)):
                nearest_pos = nearest_pos[0]
            return float(closes.iloc[int(nearest_pos)])
        except Exception:
            return None

    # ── Storage ────────────────────────────────────────────────────────────

    def _store_pending(self, records: List[Dict]):
        if not _DB_AVAILABLE:
            return
        try:
            _db.save_prediction_outcomes(records)
        except Exception as e:
            logger.debug(f"[PredTracker] Store pending failed: {e}")

    def _record_live_outcome(self, category: str) -> None:
        if not category:
            return
        self._live_outcomes_since_training[category] += 1
        if self._live_outcomes_since_training[category] >= 20:
            self._live_outcomes_since_training[category] = 0
            self._schedule_live_training(category)

    def _schedule_live_training(self, category: str) -> None:
        if not _DB_AVAILABLE:
            return
        with self._live_training_lock:
            if self._live_training_running:
                return
            self._live_training_running = True
        threading.Thread(
            target=self._run_live_training,
            args=(category,),
            name=f"PredTrackerTrainer-{category}",
            daemon=True,
        ).start()

    def _run_live_training(self, category: str) -> None:
        try:
            from ml import trainer as _trainer
            _trainer.train_live_from_outcomes(category)
        except Exception as e:
            logger.debug(f"[PredTracker] Live training failed: {e}")
        finally:
            with self._live_training_lock:
                self._live_training_running = False

    def _store_outcome(self, rec: Dict):
        if not _DB_AVAILABLE:
            return
        try:
            _db.mark_prediction_outcome_evaluated(rec)
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

        since = datetime.utcnow() - timedelta(days=days_back)

        try:
            rollups = _db.get_prediction_accuracy_rollups(since=since)
            rows = rollups["by_horizon"]

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

            asset_rows = rollups["by_asset"]
            by_asset = defaultdict(dict)
            for row in asset_rows:
                asset  = row[0]
                label  = HORIZON_LABELS.get(row[1], f'{row[1]}m')
                tot    = row[2] or 0
                corr   = row[3] or 0
                acc    = round(corr / tot * 100, 1) if tot > 0 else 0
                by_asset[asset][label] = {'total': tot, 'accuracy_pct': acc}

            recent_rows = rollups["recent"]
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

    def evaluate_pending_once(self) -> None:
        self._reload_pending_from_db(merge=True)
        self._evaluate_due()

    def _eval_loop(self):
        while self._running:
            try:
                self._evaluate_due()
            except Exception as e:
                logger.debug(f"[PredTracker] eval loop error: {e}")
            time.sleep(60)   # Check every minute

    def _reload_pending_from_db(self, merge: bool = False):
        """On startup, reload any unevaluated predictions from DB."""
        if not _DB_AVAILABLE:
            return
        try:
            rows = _db.get_pending_prediction_outcomes(_EVAL_LOOKBACK_DAYS)
            added = 0
            with self._lock:
                if merge:
                    existing_keys = {self._pending_key(rec) for rec in self._pending}
                else:
                    existing_keys = set()
                    self._pending.clear()
                for rec in rows:
                    rec['signal_time'] = str(rec['signal_time'])
                    rec['eval_time']   = str(rec['eval_time'])
                    rec['evaluated']   = False
                    key = self._pending_key(rec)
                    if key in existing_keys:
                        continue
                    self._pending.append(rec)
                    existing_keys.add(key)
                    added += 1

            logger.info(f"[PredTracker] Reloaded {added} pending predictions from DB")
        except Exception as e:
            logger.debug(f"[PredTracker] Reload failed: {e}")


# ── Global singleton ──────────────────────────────────────────────────────────
prediction_tracker = PredictionTracker()
