"""Tests for each pipeline layer in isolation."""
import pytest
from core.signal import Signal


def _make_signal(confidence=0.75, direction="BUY", category="crypto",
                 entry=50000.0, stop_loss=49000.0, take_profit=52000.0):
    s = Signal(
        asset="BTC-USD", canonical_asset="BTC-USD",
        direction=direction, category=category,
        confidence=confidence,
        entry_price=entry, stop_loss=stop_loss, take_profit=take_profit,
        strategy_id="TEST",
    )
    return s


# ── Layer 1 — Voting ──────────────────────────────────────────────────────────

def test_layer1_kills_low_confidence():
    from layers.layer1_voting import VotingLayer
    layer = VotingLayer()
    sig   = _make_signal(confidence=0.3)
    result = layer.process(sig, {})
    assert result is None
    assert sig.alive is False


def test_layer1_passes_high_confidence():
    from layers.layer1_voting import VotingLayer
    layer  = VotingLayer()
    sig    = _make_signal(confidence=0.9)
    result = layer.process(sig, {})
    assert result is not None
    assert result.alive is True


def test_layer1_boosts_when_ml_agrees():
    from layers.layer1_voting import VotingLayer
    layer = VotingLayer()
    sig   = _make_signal(confidence=0.75, direction="BUY")
    before = sig.confidence
    layer.process(sig, {"ml_prediction": 0.8})   # > 0.5 means BUY
    assert sig.confidence >= before


def test_layer1_reduces_when_ml_disagrees():
    from layers.layer1_voting import VotingLayer
    layer = VotingLayer()
    sig   = _make_signal(confidence=0.75, direction="BUY")
    before = sig.confidence
    layer.process(sig, {"ml_prediction": 0.2})   # < 0.5 means SELL
    assert sig.confidence <= before


# ── Layer 2 — Quality ─────────────────────────────────────────────────────────

def test_layer2_kills_bad_rr():
    from layers.layer2_quality import QualityLayer
    layer = QualityLayer()
    # RR = 0.5 (reward=500, risk=1000) — below MIN_RR of 1.5
    sig = _make_signal(entry=50000, stop_loss=49000, take_profit=50500)
    result = layer.process(sig, {})
    assert result is None


def test_layer2_passes_good_rr():
    from layers.layer2_quality import QualityLayer
    layer = QualityLayer()
    # RR = 2.5 (reward=2500, risk=1000)
    sig = _make_signal(entry=50000, stop_loss=49000, take_profit=52500)
    result = layer.process(sig, {})
    assert result is not None


def test_layer2_penalises_wide_spread():
    from layers.layer2_quality import QualityLayer
    layer  = QualityLayer()
    sig    = _make_signal(entry=50000, stop_loss=49000, take_profit=52500)
    before = sig.confidence
    # spread = 300, price = 50000 → 0.6% > MAX_SPREAD_PCT
    layer.process(sig, {"spread": 300, "price": 50000})
    assert sig.confidence <= before


# ── Layer 3 — Regime ──────────────────────────────────────────────────────────

def test_layer3_passes_unknown_regime():
    from layers.layer3_regime import RegimeLayer
    layer  = RegimeLayer()
    sig    = _make_signal(direction="BUY")
    result = layer.process(sig, {"regime": "unknown"})
    assert result is not None


def test_layer3_kills_volatile_regime():
    from layers.layer3_regime import RegimeLayer
    layer  = RegimeLayer()
    sig    = _make_signal(direction="BUY")
    result = layer.process(sig, {"regime": "volatile"})
    assert result is None


def test_layer3_kills_conflicting_regime():
    from layers.layer3_regime import RegimeLayer
    layer  = RegimeLayer()
    sig    = _make_signal(direction="BUY")
    result = layer.process(sig, {"regime": "trending_down"})
    assert result is None


def test_layer3_passes_aligned_regime():
    from layers.layer3_regime import RegimeLayer
    layer  = RegimeLayer()
    sig    = _make_signal(direction="BUY")
    result = layer.process(sig, {"regime": "trending_up"})
    assert result is not None


# ── Layer 4 — Session ─────────────────────────────────────────────────────────

def test_layer4_crypto_always_open():
    from layers.layer4_session import SessionLayer
    layer  = SessionLayer()
    sig    = _make_signal(category="crypto")
    result = layer.process(sig, {})
    assert result is not None


def test_layer4_sets_session_in_metadata():
    from layers.layer4_session import SessionLayer
    layer  = SessionLayer()
    sig    = _make_signal(category="crypto")
    layer.process(sig, {})
    assert "session" in sig.metadata


# ── Layer 5 — Sentiment ───────────────────────────────────────────────────────

def test_layer5_passes_neutral_sentiment():
    from layers.layer5_sentiment import SentimentLayer
    layer  = SentimentLayer()
    sig    = _make_signal(direction="BUY")
    result = layer.process(sig, {"sentiment_score": 0.0})
    assert result is not None


def test_layer5_kills_strongly_negative_sentiment_for_buy():
    from layers.layer5_sentiment import SentimentLayer
    layer  = SentimentLayer()
    sig    = _make_signal(direction="BUY")
    # aligned_score = -0.8 * 1 = -0.8 < KILL_THRESHOLD (-0.6)
    result = layer.process(sig, {"sentiment_score": -0.8})
    assert result is None


def test_layer5_boosts_aligned_sentiment():
    from layers.layer5_sentiment import SentimentLayer
    layer  = SentimentLayer()
    sig    = _make_signal(direction="BUY", confidence=0.75)
    before = sig.confidence
    layer.process(sig, {"sentiment_score": 0.5})
    assert sig.confidence >= before


# ── Layer 6 — Whale ───────────────────────────────────────────────────────────

def test_layer6_passes_with_no_whale_data():
    from layers.layer6_whale import WhaleLayer
    layer  = WhaleLayer()
    sig    = _make_signal(direction="BUY")
    result = layer.process(sig, {})
    assert result is not None


def test_layer6_ingest_and_kill():
    from layers.layer6_whale import WhaleLayer, ingest_whale_alert, _WHALE_CACHE
    _WHALE_CACHE.clear()
    ingest_whale_alert("BTC-USD", "SELL", 5_000_000)
    ingest_whale_alert("BTC-USD", "SELL", 5_000_000)
    layer  = WhaleLayer()
    sig    = _make_signal(direction="BUY")
    result = layer.process(sig, {})
    assert result is None
    _WHALE_CACHE.clear()


# ── Layer 7 — Calibration ─────────────────────────────────────────────────────

def test_layer7_kills_spread_too_wide():
    from layers.layer7_calibration import CalibrationLayer
    layer  = CalibrationLayer()
    sig    = _make_signal(confidence=0.9, entry=50000)
    # spread 200 / price 50000 = 0.004 > MAX_SPREAD_PCT (0.003)
    result = layer.process(sig, {"spread": 200, "price": 50000})
    assert result is None


def test_layer7_kills_final_confidence_too_low():
    from layers.layer7_calibration import CalibrationLayer
    layer  = CalibrationLayer()
    sig    = _make_signal(confidence=0.55, entry=50000,
                          stop_loss=49000, take_profit=52000)
    result = layer.process(sig, {})
    assert result is None


def test_layer7_adds_tp_levels():
    from layers.layer7_calibration import CalibrationLayer
    layer = CalibrationLayer()
    sig   = _make_signal(confidence=0.8, direction="BUY",
                         entry=50000, stop_loss=49000, take_profit=52000)
    result = layer.process(sig, {})
    assert result is not None
    assert len(result.take_profit_levels) == 3


def test_layer7_tp_levels_ascending_for_buy():
    from layers.layer7_calibration import CalibrationLayer
    layer = CalibrationLayer()
    sig   = _make_signal(confidence=0.8, direction="BUY",
                         entry=50000, stop_loss=49000, take_profit=52000)
    layer.process(sig, {})
    levels = sig.take_profit_levels
    assert levels[0] < levels[1] < levels[2]


def test_layer7_tp_levels_descending_for_sell():
    from layers.layer7_calibration import CalibrationLayer
    layer = CalibrationLayer()
    sig   = _make_signal(confidence=0.8, direction="SELL",
                         entry=50000, stop_loss=51000, take_profit=48000)
    layer.process(sig, {})
    levels = sig.take_profit_levels
    assert levels[0] > levels[1] > levels[2]