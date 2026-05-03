from core.asset_profiles import classify_depth_feed, get_depth_feed_policy
from core.decision_engine import SignalDecisionEngine
from core.signal import Signal
from services.playbook_service import PlaybookService


def test_depth_feed_policy_keeps_forex_indices_out_of_exchange_dom_path():
    assert (
        classify_depth_feed(
            asset="BTC-USD",
            category="crypto",
            provider="Binance",
            provider_class="exchange_depth",
            source="binance_live_depth",
            depth_available=True,
            levels=1000,
        )
        == "exchange_deep"
    )
    assert (
        classify_depth_feed(
            asset="US100",
            category="indices",
            provider="cTrader",
            provider_class="broker_l2",
            source="ctrader_live_depth",
            depth_available=True,
            levels=5,
        )
        == "broker_l2"
    )
    assert (
        classify_depth_feed(
            asset="EUR/USD",
            category="forex",
            provider="Dukascopy",
            provider_class="broker_l2",
            source="dukascopy_live_depth",
            depth_available=False,
            levels=0,
        )
        == "quote_only"
    )

    downgraded = get_depth_feed_policy("US100", "indices", "exchange_deep")
    assert downgraded["depth_feed_class"] == "broker_l2"
    assert downgraded["confirmation_override_allowed"] is False
    assert downgraded["sovereignty_allowed"] is False


def test_playbook_depth_readiness_uses_source_specific_permissions():
    service = PlaybookService()

    us100 = service._depth_readiness(
        {
            "asset": "US100",
            "category": "indices",
            "depth_available": True,
            "depth_provider": "IC Markets cTrader",
            "depth_provider_class": "broker_l2",
            "microstructure_source": "ctrader_live_depth",
            "depth_update_mode": "stream_snapshot",
            "depth_levels": 5,
            "depth_quality": 0.35,
            "depth_provider_trust_score": 0.58,
        },
        asset="US100",
        category="indices",
    )
    assert us100["true_depth_ready"] is True
    assert us100["depth_feed_class"] == "broker_l2"
    assert us100["depth_confirmation_override_allowed"] is False
    assert us100["depth_sovereignty_allowed"] is False

    btc = service._depth_readiness(
        {
            "asset": "BTC-USD",
            "category": "crypto",
            "depth_available": True,
            "depth_provider": "Binance",
            "depth_provider_class": "exchange_depth",
            "microstructure_source": "binance_live_depth",
            "depth_update_mode": "stream_snapshot",
            "depth_levels": 25,
            "depth_quality": 0.55,
            "depth_provider_trust_score": 0.88,
        },
        asset="BTC-USD",
        category="crypto",
    )
    assert btc["true_depth_ready"] is True
    assert btc["depth_feed_class"] == "exchange_deep"
    assert btc["depth_confirmation_override_allowed"] is True
    assert btc["depth_sovereignty_allowed"] is True


def test_context_confluence_accepts_asset_category_from_context():
    service = PlaybookService()
    profile = service._context_directional_confluence(
        {
            "asset": "US100",
            "category": "indices",
            "market_microstructure": {
                "asset": "US100",
                "category": "indices",
                "depth_available": True,
                "depth_provider": "IC Markets cTrader",
                "depth_provider_class": "broker_l2",
                "microstructure_source": "ctrader_live_depth",
                "depth_update_mode": "stream_snapshot",
                "depth_levels": 5,
                "depth_quality": 0.35,
                "depth_provider_trust_score": 0.58,
                "book_imbalance": 0.25,
                "score": 0.24,
            },
        },
        "BUY",
    )
    assert profile["depth_feed_class"] == "broker_l2"
    assert profile["true_depth_ready"] is True
    assert profile["depth_confirmation_override_allowed"] is False


def test_market_review_blocks_exchange_conflict_but_not_broker_l2_like_exchange_dom():
    engine = SignalDecisionEngine()
    structure = {
        "structure_bias": "buy",
        "alignment_score": 0.55,
        "setup_quality": 0.55,
        "trend_1h": "up",
        "trend_4h": "up",
    }

    broker_signal = Signal(asset="US100", direction="BUY", category="indices", confidence=0.70)
    broker_ok = engine._apply_market_review(
        broker_signal,
        {
            "market_status": {"market_open": True},
            "market_structure": structure,
            "market_microstructure": {
                "asset": "US100",
                "category": "indices",
                "depth_available": True,
                "depth_provider": "IC Markets cTrader",
                "depth_provider_class": "broker_l2",
                "microstructure_source": "ctrader_live_depth",
                "depth_feed_class": "broker_l2",
                "depth_levels": 5,
                "depth_quality": 0.35,
                "depth_provider_trust_score": 0.58,
                "book_imbalance": -0.24,
                "score": -0.25,
            },
        },
    )
    assert broker_ok is True
    assert broker_signal.alive is True
    assert broker_signal.metadata["depth_feed_class"] == "broker_l2"
    assert broker_signal.metadata["depth_policy_conflict_block"] == 0.32

    exchange_signal = Signal(asset="BTC-USD", direction="BUY", category="crypto", confidence=0.70)
    exchange_ok = engine._apply_market_review(
        exchange_signal,
        {
            "market_status": {"market_open": True},
            "market_structure": structure,
            "market_microstructure": {
                "asset": "BTC-USD",
                "category": "crypto",
                "depth_available": True,
                "depth_provider": "Binance",
                "depth_provider_class": "exchange_depth",
                "microstructure_source": "binance_live_depth",
                "depth_feed_class": "exchange_deep",
                "depth_levels": 100,
                "depth_quality": 0.55,
                "depth_provider_trust_score": 0.88,
                "book_imbalance": -0.24,
                "score": -0.25,
            },
        },
    )
    assert exchange_ok is False
    assert exchange_signal.alive is False
    assert "exchange_deep depth context conflicts" in exchange_signal.kill_reason
