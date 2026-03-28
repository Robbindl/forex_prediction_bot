"""
ml/trainer.py — Background model trainer.
Merges: auto_train_daily.py, auto_train_intelligent.py, training_monitor.py, signal_learning.py
"""
from __future__ import annotations
import threading
import time
import json
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
import numpy as np
import pandas as pd
from utils.logger import get_logger
from ml.registry import ModelRegistry, registry
from ml.features import build_features
from config.config import (
    ASSET_CATEGORIES, LOOKBACK_PERIOD, TRAIN_TEST_SPLIT, MODEL_MAX_AGE_HOURS
)

logger = get_logger()


def _sync_prediction_outcomes() -> None:
    """Ensure the live outcome tracker is running and catch up due rows."""
    try:
        from prediction_tracker import prediction_tracker as _prediction_tracker
        _prediction_tracker.start()
        _prediction_tracker.evaluate_pending_once()
    except Exception as e:
        logger.debug(f"[Trainer] Prediction outcome sync skipped: {e}")


def _build_training_data(df: pd.DataFrame, horizon: int = 5):
    """Build X (features) and y (label: 1=up, 0=down) for classification.
    
    FIX: Now uses unified 6-feature set from ml.features.build_features()
    to ensure consistency with predictor. Features are built for each historical
    point to create training samples with future price direction labels.
    """
    if df is None or len(df) < horizon + 20:  # Need enough data for features + horizon
        return None, None
    
    try:
        # Build features for each point in the dataset (rolling window)
        features_list = []
        labels = []
        
        # Start from index 20 to have enough history for features
        for i in range(20, len(df) - horizon):
            window_df = df.iloc[:i+1]  # Data up to current point
            features = build_features(window_df)
            if features is not None:
                features_list.append(features)
                # Label: 1 if price goes up in next 'horizon' periods, 0 otherwise
                future_price = df.iloc[i + horizon]['close']
                current_price = df.iloc[i]['close']
                label = 1 if future_price > current_price else 0
                labels.append(label)
        
        if not features_list:
            return None, None
            
        X = np.array(features_list)
        y = np.array(labels)
        
        return X, y
    except Exception as e:
        logger.debug(f"[Trainer] Feature build error: {e}")
        return None, None


def _build_historical_policy_data(df: pd.DataFrame):
    """Build historical policy training vectors from OHLCV and default metadata."""
    X, y = _build_training_data(df)
    if X is None or y is None:
        return None, None
    metadata_padding = np.zeros((X.shape[0], 16), dtype=np.float32)
    return np.hstack([X, metadata_padding]), y


def _float_val(metadata: dict, key: str, default: float = 0.0) -> float:
    try:
        return float(metadata.get(key, default) or default)
    except Exception:
        return default


def _bool_val(metadata: dict, key: str) -> float:
    val = metadata.get(key)
    if isinstance(val, str):
        return 1.0 if val.lower() in ("true", "1", "yes", "real") else 0.0
    return 1.0 if val else 0.0


def _regime_to_numeric(value: Any) -> float:
    if isinstance(value, str):
        value = value.lower()
        if value in ("bull", "up", "long"):
            return 1.0
        if value in ("bear", "down", "short"):
            return -1.0
    return 0.0


def _dominant_to_numeric(value: Any) -> float:
    if isinstance(value, str):
        value = value.lower()
        if value in ("bull", "buy", "long"):
            return 1.0
        if value in ("bear", "sell", "short"):
            return -1.0
    return 0.0


def _build_metadata_features(signal_metadata: Any) -> np.ndarray | None:
    if not signal_metadata:
        return np.zeros(16, dtype=np.float32)

    if isinstance(signal_metadata, str):
        try:
            signal_metadata = json.loads(signal_metadata)
        except Exception:
            return np.zeros(16, dtype=np.float32)

    if not isinstance(signal_metadata, dict):
        return np.zeros(16, dtype=np.float32)

    return np.array([
        _float_val(signal_metadata, "ml_confidence"),
        _bool_val(signal_metadata, "ml_prediction_real"),
        _regime_to_numeric(signal_metadata.get("regime")),
        _float_val(signal_metadata, "sentiment_score"),
        _float_val(signal_metadata, "reddit_score"),
        _float_val(signal_metadata, "put_call_score"),
        float(len(signal_metadata.get("sentiment_sources") or [])),
        _float_val(signal_metadata, "whale_buy_vol"),
        _float_val(signal_metadata, "whale_sell_vol"),
        _dominant_to_numeric(signal_metadata.get("whale_dominant") or signal_metadata.get("whale_data")),
        _bool_val(signal_metadata, "whale_data"),
        _bool_val(signal_metadata, "orderflow_applicable"),
        _float_val(signal_metadata, "orderflow_imbalance"),
        _float_val(signal_metadata, "liquidity_proxy"),
        _float_val(signal_metadata, "spread_penalty"),
        _float_val(signal_metadata, "confidence"),
    ], dtype=np.float32)


def _train_model_from_arrays(name: str, X: np.ndarray, y: np.ndarray) -> bool:
    if X is None or y is None or len(X) < 50:
        logger.warning(f"[Trainer] Insufficient training samples for {name}")
        return False

    split = int(len(X) * TRAIN_TEST_SPLIT)
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


def _train_policy_model(name: str, df: pd.DataFrame) -> bool:
    X, y = _build_historical_policy_data(df)
    return _train_model_from_arrays(name, X, y)


def _parse_signal_metadata(raw_metadata: Any) -> dict:
    if raw_metadata is None:
        return {}
    if isinstance(raw_metadata, str):
        try:
            return json.loads(raw_metadata)
        except Exception:
            return {}
    if isinstance(raw_metadata, dict):
        return raw_metadata
    return {}


def _build_live_feature_vector(signal_features: Any, signal_metadata: Any) -> np.ndarray | None:
    try:
        if isinstance(signal_features, str):
            signal_features = json.loads(signal_features)
        if not isinstance(signal_features, (list, tuple)) or len(signal_features) != 6:
            return None
        price_features = np.array(signal_features, dtype=np.float32)
    except Exception:
        return None

    return price_features


def _build_live_policy_feature_vector(signal_features: Any, signal_metadata: Any) -> np.ndarray | None:
    base_features = _build_live_feature_vector(signal_features, signal_metadata)
    if base_features is None:
        return None
    metadata_features = _build_metadata_features(signal_metadata)
    if metadata_features is None:
        return None
    return np.concatenate([base_features, metadata_features])


def _build_live_policy_training_data(category: str):
    """
    Build live policy training data using realised market direction.

    The runtime interprets the policy model output as P(price goes up), then
    maps that to BUY/SELL thresholds. The live training target must therefore
    be directional too. Training on "was the previous signal correct?" would
    mix BUY/SELL outcomes into a label that does not match inference.
    """
    try:
        from services.database_service import DatabaseService
        from sqlalchemy import text
    except Exception as e:
        logger.debug(f"[Trainer] Live policy training disabled: {e}")
        return None, None

    try:
        db = DatabaseService()
        since = (datetime.utcnow() - timedelta(days=30)).isoformat()
        with db.get_session() as session:
            rows = session.execute(text("""
                SELECT entry_price, actual_price,
                       signal_features, signal_metadata
                FROM prediction_outcomes
                WHERE evaluated = true
                  AND category = :category
                  AND signal_features IS NOT NULL
                  AND signal_time >= :since
                ORDER BY signal_time DESC
                LIMIT 2000
            """), {'category': category, 'since': since}).fetchall()

        if not rows:
            return None, None

        X = []
        y = []
        for entry_price, actual_price, signal_features, signal_metadata in rows:
            if entry_price is None or actual_price is None or not signal_features:
                continue
            features = _build_live_policy_feature_vector(signal_features, signal_metadata)
            if features is None:
                continue
            label = 1 if actual_price > entry_price else 0
            X.append(features)
            y.append(label)

        if len(X) < 50:
            return None, None

        return np.vstack(X), np.array(y)
    except Exception as e:
        logger.debug(f"[Trainer] Live policy data build failed: {e}")
        return None, None


def _build_live_training_data(category: str):
    try:
        from services.database_service import DatabaseService
        from sqlalchemy import text
    except Exception as e:
        logger.debug(f"[Trainer] Live training disabled: {e}")
        return None, None

    try:
        db = DatabaseService()
        since = (datetime.utcnow() - timedelta(days=30)).isoformat()
        with db.get_session() as session:
            rows = session.execute(text("""
                SELECT entry_price, actual_price, signal_features,
                       signal_metadata
                FROM prediction_outcomes
                WHERE evaluated = true
                  AND category = :category
                  AND signal_features IS NOT NULL
                  AND signal_time >= :since
                ORDER BY signal_time DESC
                LIMIT 2000
            """), {'category': category, 'since': since}).fetchall()

        if not rows:
            return None, None

        X = []
        y = []
        for entry_price, actual_price, signal_features, signal_metadata in rows:
            if entry_price is None or actual_price is None or not signal_features:
                continue
            features = _build_live_feature_vector(signal_features, signal_metadata)
            if features is None:
                continue
            label = 1 if actual_price > entry_price else 0
            X.append(features)
            y.append(label)

        if len(X) < 50:
            return None, None

        return np.vstack(X), np.array(y)
    except Exception as e:
        logger.debug(f"[Trainer] Live data build failed: {e}")
        return None, None


def train_live_from_outcomes(category: str, min_samples: int = 20) -> bool:
    """Train a live policy model from evaluated prediction outcomes."""
    X, y = _build_live_policy_training_data(category)
    if X is None or y is None or len(X) < min_samples:
        logger.info(f"[Trainer] Not enough live outcomes to train {category}_policy ({0 if X is None else len(X)})")
        return False
    model_key = f"{category}_policy"
    success = _train_model_from_arrays(model_key, X, y)
    if success:
        logger.info(f"[Trainer] ✅ Live policy model trained for {model_key} using {len(X)} samples")
    return success


def _train_model(name: str, df: pd.DataFrame) -> bool:
    """Train an XGBoost classifier and register it."""
    X, y = _build_training_data(df)
    return _train_model_from_arrays(name, X, y)


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
        _sync_prediction_outcomes()
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
        _sync_prediction_outcomes()
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
            _sync_prediction_outcomes()
            trained_any = False
            for cat in ASSET_CATEGORIES:
                policy_key = f"{cat}_policy"
                classifier_key = f"{cat}_classifier"
                policy_stale = registry.is_stale(policy_key)
                classifier_stale = registry.is_stale(classifier_key)

                logger.info(f"[AutoTrainer] {policy_key} stale={policy_stale}")
                if policy_stale:
                    logger.info(f"[AutoTrainer] Training {policy_key}...")
                    self._train_policy_category(cat)
                    trained_any = True

                logger.info(f"[AutoTrainer] {classifier_key} stale={classifier_stale}")
                if classifier_stale:
                    logger.info(f"[AutoTrainer] Training {classifier_key}...")
                    self._train_category(cat)
                    trained_any = True

            if not trained_any:
                logger.info("[AutoTrainer] All models up to date — next check in 1h")
            self._stop.wait(timeout=3600)   # check every hour

    def _train_policy_category(self, category: str) -> None:
        assets = ASSET_CATEGORIES.get(category, [])
        if not assets:
            return

        try:
            import core.engine as _eng_mod
            _fetcher = getattr(getattr(_eng_mod, "_CORE_INSTANCE", None), "fetcher", None)
        except Exception:
            _fetcher = None
        if _fetcher is None:
            _fetcher = self._fetcher
        if _fetcher is None:
            logger.warning(f"[AutoTrainer] No fetcher available for {category} — skipping policy training")
            return

        policy_key = f"{category}_policy"
        policy_X, policy_y = _build_live_policy_training_data(category)
        if policy_X is not None:
            logger.info(f"[AutoTrainer] Training {policy_key} from live prediction outcomes")
            if _train_model_from_arrays(policy_key, policy_X, policy_y):
                return
            logger.info(f"[AutoTrainer] Live policy training failed for {category}, falling back to historical data")

        all_dfs: List[pd.DataFrame] = []
        for asset in assets[:5]:
            df = self._get_ohlcv_with_fallback(_fetcher, asset, category)
            if df is not None and not df.empty:
                all_dfs.append(df)
                logger.info(f"[AutoTrainer] Got {len(df)} bars for {asset}")
            else:
                logger.warning(f"[AutoTrainer] No OHLCV data for {asset} ({category}) — all timeframes failed")

        if not all_dfs:
            logger.warning(f"[AutoTrainer] No data available for policy training in {category}")
            return

        if len(all_dfs) == 1:
            _train_policy_model(policy_key, all_dfs[0])
            return

        best_acc = -1.0
        for asset_df in all_dfs:
            tmp_key = f"_tmp_{policy_key}"
            if _train_policy_model(tmp_key, asset_df):
                from ml.registry import registry as _reg
                candidate = _reg.get(tmp_key)
                if candidate is not None:
                    X_val, y_val = _build_historical_policy_data(asset_df)
                    if X_val is not None and len(X_val) > 10:
                        split = int(len(X_val) * TRAIN_TEST_SPLIT)
                        try:
                            acc = float(candidate.score(X_val[split:], y_val[split:]))
                        except Exception:
                            acc = 0.0
                        if acc > best_acc:
                            best_acc = acc
                            registry.save(policy_key, candidate)
        logger.info(
            f"[AutoTrainer] {policy_key} best_acc={best_acc:.3f} "
            f"(trained on {len(all_dfs)} assets individually)"
        )

    def _get_ohlcv_with_fallback(self, fetcher, asset: str, category: str) -> Optional[pd.DataFrame]:
        """Get OHLCV data with timeframe fallbacks for assets that don't have intraday data."""
        from config.config import TRADING_TIMEFRAME

        # Define fallback timeframes: primary -> fallbacks
        timeframe_fallbacks = {
            "forex": [TRADING_TIMEFRAME],  # Forex usually has all timeframes
            "crypto": [TRADING_TIMEFRAME, "1h", "1d"],  # Some cryptos may not have 15m
            "commodities": [TRADING_TIMEFRAME, "1h", "1d"],  # Commodities may not have 15m
            "indices": [TRADING_TIMEFRAME, "1h", "1d"],      # Indices may not have 15m
        }

        periods_map = {"15m": 500, "1h": 300, "4h": 200, "1d": LOOKBACK_PERIOD}

        for tf in timeframe_fallbacks.get(category, [TRADING_TIMEFRAME]):
            try:
                periods = periods_map.get(tf, LOOKBACK_PERIOD)
                df = fetcher.get_ohlcv(asset, category, tf, periods)
                if df is not None and not df.empty:
                    if tf != TRADING_TIMEFRAME:
                        logger.info(f"[AutoTrainer] {asset} using fallback timeframe {tf} (primary {TRADING_TIMEFRAME} unavailable)")
                    return df
            except Exception as e:
                logger.debug(f"[AutoTrainer] Fetch failed for {asset} tf={tf}: {e}")
                continue

        return None

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

        # Train a live policy model if we have enough evaluated outcome data.
        policy_key = f"{category}_policy"
        policy_X, policy_y = _build_live_policy_training_data(category)
        if policy_X is not None:
            logger.info(f"[AutoTrainer] Training {policy_key} from live prediction outcomes")
            _train_model_from_arrays(policy_key, policy_X, policy_y)

        model_key = f"{category}_classifier"
        live_X, live_y = _build_live_training_data(category)
        if live_X is not None:
            logger.info(f"[AutoTrainer] Training {model_key} from live prediction outcomes")
            if _train_model_from_arrays(model_key, live_X, live_y):
                with self._lock:
                    self._status[category] = {
                        "last_trained": datetime.utcnow().isoformat(),
                        "success":      True,
                        "samples":      len(live_X),
                    }
                return
            logger.info(
                f"[AutoTrainer] Live outcome training failed for {category}, falling back to historical OHLCV"
            )

        all_dfs: List[pd.DataFrame] = []
        for asset in assets[:5]:    # limit to 5 assets per category for speed
            df = self._get_ohlcv_with_fallback(_fetcher, asset, category)
            if df is not None and not df.empty:
                all_dfs.append(df)
                logger.info(f"[AutoTrainer] Got {len(df)} bars for {asset}")
            else:
                logger.warning(f"[AutoTrainer] No OHLCV data for {asset} ({category}) — all timeframes failed")

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
            total_samples = sum(len(df) for df in all_dfs) if all_dfs else 0
            self._status[category] = {
                "last_trained": datetime.utcnow().isoformat(),
                "success":      success,
                "samples":      total_samples,
            }
