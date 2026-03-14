"""Tests for ML predictor, registry, and trainer data building."""
import pytest
import numpy as np
import pandas as pd
from unittest.mock import MagicMock, patch


def _make_ohlcv(n=100, trend="flat"):
    """Synthetic OHLCV dataframe."""
    np.random.seed(0)
    if trend == "up":
        prices = np.cumsum(np.random.normal(0.2, 1.0, n)) + 100
    elif trend == "down":
        prices = np.cumsum(np.random.normal(-0.2, 1.0, n)) + 100
    else:
        prices = np.cumsum(np.random.normal(0.0, 0.5, n)) + 100
    prices = np.maximum(prices, 1.0)
    return pd.DataFrame({
        "open":   prices,
        "high":   prices + 0.5,
        "low":    prices - 0.5,
        "close":  prices,
        "volume": np.ones(n) * 1_000_000,
    })


# ── Cache ─────────────────────────────────────────────────────────────────────

def test_cache_set_and_get():
    from data.cache import Cache
    c = Cache(default_ttl=60)
    c.set("key1", "value1")
    assert c.get("key1") == "value1"


def test_cache_miss_returns_none():
    from data.cache import Cache
    c = Cache(default_ttl=60)
    assert c.get("nonexistent") is None


def test_cache_expiry():
    from data.cache import Cache
    import time
    c = Cache(default_ttl=60)
    c.set("key", "val", ttl=0)
    time.sleep(0.01)
    assert c.get("key") is None


def test_cache_delete():
    from data.cache import Cache
    c = Cache(default_ttl=60)
    c.set("key", "val")
    c.delete("key")
    assert c.get("key") is None


def test_cache_clear():
    from data.cache import Cache
    c = Cache(default_ttl=60)
    c.set("a", 1)
    c.set("b", 2)
    c.clear()
    assert len(c) == 0


def test_cache_contains():
    from data.cache import Cache
    c = Cache(default_ttl=60)
    c.set("x", 42)
    assert "x" in c
    assert "y" not in c


def test_cache_purge_expired():
    from data.cache import Cache
    import time
    c = Cache(default_ttl=60)
    c.set("fresh", "yes", ttl=60)
    c.set("stale", "no",  ttl=0)
    time.sleep(0.01)
    removed = c.purge_expired()
    assert removed == 1
    assert c.get("fresh") == "yes"


# ── ModelRegistry ─────────────────────────────────────────────────────────────

def test_registry_register_and_get():
    from ml.registry import ModelRegistry
    reg   = ModelRegistry()
    model = MagicMock()
    reg.register("test_model", model)
    assert reg.get("test_model") is model


def test_registry_get_unknown_returns_none():
    from ml.registry import ModelRegistry
    reg = ModelRegistry()
    assert reg.get("does_not_exist") is None


def test_registry_freshly_registered_not_stale():
    from ml.registry import ModelRegistry
    reg   = ModelRegistry()
    model = MagicMock()
    reg.register("fresh_model", model)
    assert reg.is_stale("fresh_model") is False


def test_registry_unknown_model_is_stale():
    from ml.registry import ModelRegistry
    reg = ModelRegistry()
    assert reg.is_stale("never_trained") is True


def test_registry_list_models_includes_registered():
    from ml.registry import ModelRegistry
    reg = ModelRegistry()
    reg.register("listed_model", MagicMock(), {"accuracy": 0.75})
    models = reg.list_models()
    assert "listed_model" in models
    assert models["listed_model"]["loaded"] is True


# ── MLPredictor ───────────────────────────────────────────────────────────────

def test_predictor_returns_tuple():
    from ml.predictor import MLPredictor
    pred  = MLPredictor()
    df    = _make_ohlcv(100)
    result = pred.predict("BTC-USD", "crypto", df)
    assert isinstance(result, tuple)
    assert len(result) == 2


def test_predictor_probability_in_range():
    from ml.predictor import MLPredictor
    pred   = MLPredictor()
    df     = _make_ohlcv(100)
    prob, conf = pred.predict("BTC-USD", "crypto", df)
    assert 0.0 <= prob <= 1.0
    assert 0.0 <= conf <= 1.0


def test_predictor_short_data_returns_neutral():
    from ml.predictor import MLPredictor
    pred   = MLPredictor()
    df     = _make_ohlcv(5)
    prob, conf = pred.predict("BTC-USD", "crypto", df)
    assert prob == 0.5
    assert conf == 0.0


def test_predictor_with_mock_model():
    from ml.predictor import MLPredictor
    import numpy as np

    mock_model = MagicMock()
    mock_model.predict_proba.return_value = np.array([[0.3, 0.7]])

    pred = MLPredictor()
    with patch("ml.predictor.registry") as mock_reg:
        mock_reg.get.return_value = mock_model
        df = _make_ohlcv(100)
        prob, conf = pred.predict("BTC-USD", "crypto", df)

    assert prob == pytest.approx(0.7, rel=0.01)
    assert conf > 0.0


def test_predictor_momentum_fallback_up():
    from ml.predictor import MLPredictor
    pred   = MLPredictor()
    # Strongly rising prices
    prices = list(range(80, 180))
    df = pd.DataFrame({
        "open": prices, "high": [p+1 for p in prices],
        "low":  [p-1 for p in prices], "close": prices,
        "volume": [1_000_000]*100,
    })
    prob, conf = pred._momentum_fallback(df)
    assert prob > 0.5   # rising → bullish


def test_predictor_momentum_fallback_down():
    from ml.predictor import MLPredictor
    pred   = MLPredictor()
    prices = list(range(180, 80, -1))
    df = pd.DataFrame({
        "open": prices, "high": [p+1 for p in prices],
        "low":  [p-1 for p in prices], "close": prices,
        "volume": [1_000_000]*100,
    })
    prob, conf = pred._momentum_fallback(df)
    assert prob < 0.5   # falling → bearish


# ── Trainer data building ─────────────────────────────────────────────────────

def test_build_training_data_returns_arrays():
    from ml.trainer import _build_training_data
    df = _make_ohlcv(100)
    X, y = _build_training_data(df, horizon=5)
    assert X is not None
    assert y is not None
    assert len(X) == len(y)


def test_build_training_data_short_returns_none():
    from ml.trainer import _build_training_data
    df = _make_ohlcv(20)
    X, y = _build_training_data(df, horizon=5)
    assert X is None
    assert y is None


def test_build_training_data_labels_are_binary():
    from ml.trainer import _build_training_data
    df = _make_ohlcv(100)
    X, y = _build_training_data(df)
    assert set(y).issubset({0, 1})


def test_build_training_data_no_nan_in_features():
    from ml.trainer import _build_training_data
    df = _make_ohlcv(100)
    X, y = _build_training_data(df)
    assert not np.isnan(X).any()


def test_autotrainer_start_stop():
    from ml.trainer import AutoTrainer
    trainer = AutoTrainer(fetcher=None)
    trainer.start()
    assert trainer._thread is not None
    assert trainer._thread.is_alive()
    trainer.stop()


def test_autotrainer_get_status_empty_initially():
    from ml.trainer import AutoTrainer
    trainer = AutoTrainer(fetcher=None)
    assert trainer.get_status() == {}