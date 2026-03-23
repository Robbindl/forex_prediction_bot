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
    """Build X (features) and y (label: 1=up, 0=down) for classification."""
    if df is None or len(df) < horizon + 30:
        return None, None
    try:
        close  = df["close"].astype(float)
        future = close.shift(-horizon)
        y      = (future > close).astype(int).values[:-horizon]

        ret1   = close.pct_change(1)
        vol5   = ret1.rolling(5).std()
        sma5   = close.rolling(5).mean()  / close
        sma20  = close.rolling(20).mean() / close
        hl_pct = (df["high"].astype(float) - df["low"].astype(float)) / close

        X = pd.DataFrame({
            "ret1":   ret1,
            "ret5":   close.pct_change(5),
            "vol5":   vol5,
            "sma5":   sma5,
            "sma20":  sma20,
            "hl_pct": hl_pct,
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

        combined = pd.concat(all_dfs, ignore_index=True)
        model_key = f"{category}_classifier"
        success   = _train_model(model_key, combined)

        with self._lock:
            self._status[category] = {
                "last_trained": datetime.utcnow().isoformat(),
                "success":      success,
                "samples":      len(combined),
            }