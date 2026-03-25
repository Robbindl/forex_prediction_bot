"""
ml/trainer.py — Background model trainer.
Merges: auto_train_daily.py, auto_train_intelligent.py, training_monitor.py, signal_learning.py
"""
from __future__ import annotations
import threading
import time
from datetime import datetime
from typing import Dict, List, Optional
import numpy as np
import pandas as pd
from utils.logger import get_logger
from ml.registry import ModelRegistry, registry
from config.config import (
    ASSET_CATEGORIES, LOOKBACK_PERIOD, TRAIN_TEST_SPLIT, MODEL_MAX_AGE_HOURS
)

logger = get_logger()


def _build_training_data(df: pd.DataFrame, horizon: int = 5):
    """Build X (features) and y (label: 1=up, 0=down) for classification.
    
    FIX: Feature set now matches ml/predictor.py _build_features() exactly —
    10 features: ret1, ret5, ret10, vol5, vol20, sma5, sma20, sma50, hl_pct, oc_pct
    Previously 6 features were produced here but 10 were consumed in predictor,
    causing every model.predict_proba() call to raise a feature-count mismatch error.
    """
    if df is None or len(df) < horizon + 50:  # raised from 30 to accommodate sma50
        return None, None
    try:
        close  = df["close"].astype(float)
        open_  = df["open"].astype(float)
        high   = df["high"].astype(float)
        low    = df["low"].astype(float)
        future = close.shift(-horizon)
        y      = (future > close).astype(int).values[:-horizon]

        ret1   = close.pct_change(1)
        ret5   = close.pct_change(5)
        ret10  = close.pct_change(10)
        vol5   = ret1.rolling(5).std()
        vol20  = ret1.rolling(20).std()
        sma5   = close.rolling(5).mean()  / close
        sma20  = close.rolling(20).mean() / close
        sma50  = close.rolling(50).mean() / close if len(close) >= 50 else pd.Series(1.0, index=close.index)
        hl_pct = (high - low) / close
        oc_pct = (close - open_) / open_.replace(0, np.nan)

        X = pd.DataFrame({
            "ret1":   ret1,
            "ret5":   ret5,
            "ret10":  ret10,
            "vol5":   vol5,
            "vol20":  vol20,
            "sma5":   sma5,
            "sma20":  sma20,
            "sma50":  sma50,
            "hl_pct": hl_pct,
            "oc_pct": oc_pct,
        }).replace([np.inf, -np.inf], np.nan).fillna(0).values[:-horizon]

        return X, y
    except Exception as e:
        logger.debug(f"[Trainer] Feature build error: {e}")
        return None, None


def _train_model(name: str, df: pd.DataFrame) -> bool:
    """Train an XGBoost classifier and register it."""
    try:
        from xgboost import XGBClassifier
    except ImportError:
        try:
            from sklearn.ensemble import GradientBoostingClassifier as XGBClassifier
        except ImportError:
            logger.warning("[Trainer] No XGBoost or sklearn available")
            return False

    X, y = _build_training_data(df)
    if X is None or len(X) < 50:
        logger.warning(f"[Trainer] Insufficient training data for {name}")
        return False

    split  = int(len(X) * TRAIN_TEST_SPLIT)
    X_tr, X_te = X[:split], X[split:]
    y_tr, y_te = y[:split], y[split:]

    try:
        from xgboost import XGBClassifier as XGB
        model = XGB(n_estimators=100, max_depth=4, learning_rate=0.1,
                    eval_metric="logloss", random_state=42)
    except ImportError:
        from sklearn.ensemble import GradientBoostingClassifier
        model = GradientBoostingClassifier(n_estimators=100, max_depth=4)

    try:
        model.fit(X_tr, y_tr)
    except Exception as fit_err:
        logger.error(f"[Trainer] Model fit failed for {name}: {fit_err}")
        return False
    acc = model.score(X_te, y_te) if len(X_te) > 0 else 0.0

    # FIX: Quality gate — refuse to deploy a model worse than random chance.
    # Previously any model (even 48% accuracy) was saved without warning.
    MIN_ACCEPTABLE_ACCURACY = 0.52
    if acc < MIN_ACCEPTABLE_ACCURACY:
        logger.warning(
            f"[Trainer] ⚠️  {name} accuracy={acc:.3f} below minimum {MIN_ACCEPTABLE_ACCURACY} "
            f"— model NOT saved to prevent degraded predictions"
        )
        return False

    try:
        registry.save(name, model)
    except Exception as save_err:
        logger.error(f"[Trainer] Model save failed for {name}: {save_err}")
        return False
    logger.info(f"[Trainer] ✅ Trained {name} — acc={acc:.3f} samples={len(X)}")
    return True


class AutoTrainer:
    """
    Background trainer that checks model staleness every hour
    and retrains any model older than MODEL_MAX_AGE_HOURS.
    """

    def __init__(self, fetcher=None):
        self._fetcher  = fetcher
        self._thread:  Optional[threading.Thread] = None
        self._stop     = threading.Event()
        self._status:  Dict = {}
        self._lock     = threading.Lock()

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._loop, name="AutoTrainer", daemon=True
        )
        self._thread.start()
        logger.info("[AutoTrainer] Background training started")

    def stop(self) -> None:
        self._stop.set()

    def get_status(self) -> Dict:
        with self._lock:
            return dict(self._status)

    def train_now(self, category: str = "all") -> None:
        """Force immediate training (blocking). Used for on-demand retraining."""
        categories = list(ASSET_CATEGORIES.keys()) if category == "all" else [category]
        for cat in categories:
            self._train_category(cat)

    def _loop(self) -> None:
        # Wait for engine to warm up OHLCV cache before first training attempt.
        # Without this, training fires before iTick/TwelveData clients are ready
        # and the fetcher returns None for all assets.
        logger.info("[AutoTrainer] Waiting 120s for OHLCV cache to warm up...")
        self._stop.wait(timeout=120)
        if self._stop.is_set():
            return
        logger.info("[AutoTrainer] Starting initial model check")

        while not self._stop.is_set():
            trained_any = False
            for cat in ASSET_CATEGORIES:
                model_key = f"{cat}_classifier"
                stale = registry.is_stale(model_key)
                logger.info(f"[AutoTrainer] {model_key} stale={stale}")
                if stale:
                    logger.info(f"[AutoTrainer] Training {model_key}...")
                    self._train_category(cat)
                    trained_any = True
            if not trained_any:
                logger.info("[AutoTrainer] All models up to date — next check in 1h")
            self._stop.wait(timeout=3600)   # check every hour

    def _train_category(self, category: str) -> None:
        assets = ASSET_CATEGORIES.get(category, [])
        if not assets:
            return

        # Try engine singleton fetcher first, fall back to self._fetcher
        try:
            import core.engine as _eng_mod
            _fetcher = getattr(getattr(_eng_mod, "_CORE_INSTANCE", None), "fetcher", None)
        except Exception:
            _fetcher = None
        if _fetcher is None:
            _fetcher = self._fetcher
        if _fetcher is None:
            logger.warning(f"[AutoTrainer] No fetcher available for {category} — skipping")
            return

        all_dfs: List[pd.DataFrame] = []
        for asset in assets[:5]:    # limit to 5 assets per category for speed
            try:
                from config.config import TRADING_TIMEFRAME
                tf = TRADING_TIMEFRAME
                periods_map = {"15m": 500, "1h": 300, "4h": 200, "1d": LOOKBACK_PERIOD}
                periods = periods_map.get(tf, LOOKBACK_PERIOD)
                df = _fetcher.get_ohlcv(asset, category, tf, periods)
                if df is not None and not df.empty:
                    all_dfs.append(df)
                    logger.info(f"[AutoTrainer] Got {len(df)} bars for {asset}")
                else:
                    logger.warning(f"[AutoTrainer] No OHLCV data for {asset} ({category}) tf={tf}")
            except Exception as _te:
                logger.warning(f"[AutoTrainer] Fetch error {asset}: {_te}")

        if not all_dfs:
            logger.warning(f"[AutoTrainer] No data available for {category}")
            return

        # FIX: Train on each asset separately then pick the best model, rather
        # than concatenating all assets into one DataFrame.  Mixing BTC + ETH
        # rows produces a contaminated dataset where the model cannot learn
        # asset-specific patterns — a row from BTC is indistinguishable from
        # a row from XRP.  We now train one candidate per asset and keep the
        # highest-accuracy model for the category key.
        model_key = f"{category}_classifier"
        if len(all_dfs) == 1:
            success = _train_model(model_key, all_dfs[0])
        else:
            # Train per asset, register the best one
            best_acc = -1.0
            success  = False
            for asset_df in all_dfs:
                tmp_key = f"_tmp_{model_key}"
                if _train_model(tmp_key, asset_df):
                    # Check if this candidate outperforms what we already have
                    from ml.registry import registry as _reg
                    candidate = _reg.get(tmp_key)
                    if candidate is not None:
                        X_val, y_val = _build_training_data(asset_df)
                        if X_val is not None and len(X_val) > 10:
                            split  = int(len(X_val) * TRAIN_TEST_SPLIT)
                            try:
                                acc = float(candidate.score(X_val[split:], y_val[split:]))
                            except Exception:
                                acc = 0.0
                            if acc > best_acc:
                                best_acc = acc
                                registry.save(model_key, candidate)
                                success = True
            logger.info(
                f"[AutoTrainer] {model_key} best_acc={best_acc:.3f} "
                f"(trained on {len(all_dfs)} assets individually)"
            )

        with self._lock:
            self._status[category] = {
                "last_trained": datetime.utcnow().isoformat(),
                "success":      success,
                "samples":      len(combined),
            }