from __future__ import annotations

import time
from types import SimpleNamespace

from data.fetcher import DataFetcher
from services.dukascopy_live_depth_bridge import DukascopyLiveDepthBridge
from services.live_microstructure_service import get_service as get_live_microstructure_service
from services.opportunity_ranker import OpportunityRanker


def test_dukascopy_live_depth_bridge_ingest_snapshot_exposes_true_depth(tmp_path) -> None:
    service = get_live_microstructure_service()
    service.clear()

    bridge = DukascopyLiveDepthBridge(
        enabled=True,
        store_path=tmp_path / "dukascopy_depth.json",
        assets=("EUR/USD",),
        jnlp_url="http://platform.dukascopy.com/demo_3/jforex_3.jnlp",
        username="demo_user",
        password="demo_pass",
        auto_build=False,
    )

    bridge.ingest_snapshot(
        {
            "asset": "EUR/USD",
            "bid": 1.1000,
            "ask": 1.1002,
            "bid_size": 3.0,
            "ask_size": 4.0,
            "total_bid_volume": 13.0,
            "total_ask_volume": 11.0,
            "levels": [
                {"bid": 1.1000, "ask": 1.1002, "bid_size": 3.0, "ask_size": 4.0},
                {"bid": 1.0999, "ask": 1.1003, "bid_size": 10.0, "ask_size": 7.0},
            ],
            "timestamp": time.time(),
            "as_of_utc": "2026-04-24T12:00:00Z",
            "environment": "demo",
        }
    )

    micro = bridge.get_microstructure("EUR/USD", category="forex")

    assert micro["depth_available"] is True
    assert micro["microstructure_source"] == "dukascopy_live_depth"
    assert micro["depth_provider"] == "Dukascopy"
    assert micro["orderbook_top_bids"][:2] == [[1.1, 3.0], [1.0999, 10.0]]
    assert micro["orderbook_top_asks"][:2] == [[1.1002, 4.0], [1.1003, 7.0]]
    assert micro["bid_vol"] == 13.0
    assert micro["ask_vol"] == 11.0


def test_overlay_external_true_depth_prefers_fresh_dukascopy_levels() -> None:
    base = {
        "source": "IG",
        "score": 0.15,
        "depth_available": False,
        "microstructure_source": "live_store",
    }
    overlay = {
        "source": "Dukascopy",
        "source_class": "sidecar",
        "score": 0.92,
        "depth_available": True,
        "depth_levels": 4,
        "bid_vol": 20.0,
        "ask_vol": 18.0,
        "orderbook_top_bids": [[92.4, 6.0]],
        "orderbook_top_asks": [[92.5, 5.0]],
        "depth_provider": "Dukascopy",
        "depth_live_age_seconds": 2.5,
        "microstructure_source": "dukascopy_live_depth",
        "as_of_utc": "2026-04-24T12:00:00Z",
        "dukascopy_symbol": "USA500.IDX/USD",
    }

    payload = DataFetcher._overlay_external_true_depth(base, overlay)

    assert payload["source"] == "IG"
    assert payload["score"] == 0.92
    assert payload["depth_available"] is True
    assert payload["depth_provider"] == "Dukascopy"
    assert payload["microstructure_source"] == "dukascopy_live_depth"
    assert payload["depth_provider_class"] == "sidecar"
    assert payload["dukascopy_symbol"] == "USA500.IDX/USD"


def test_overlay_external_true_depth_ignores_stale_dukascopy_levels() -> None:
    base = {
        "source": "Deriv",
        "score": 0.21,
        "depth_available": False,
        "microstructure_source": "live_store",
    }
    overlay = {
        "score": 0.95,
        "depth_available": True,
        "microstructure_source": "dukascopy_live_depth",
        "depth_live_age_seconds": 45.0,
    }

    payload = DataFetcher._overlay_external_true_depth(base, overlay)

    assert payload == base


def test_opportunity_ranker_treats_dukascopy_depth_as_true_depth() -> None:
    signal = SimpleNamespace(
        direction="BUY",
        metadata={
            "microstructure_score": 0.4,
            "tick_imbalance": 0.2,
            "book_imbalance": 0.3,
            "depth_available": True,
            "microstructure_source": "dukascopy_live_depth",
            "stop_hunt_risk": 0.0,
            "exhaustion_risk": 0.0,
        },
    )
    fallback_signal = SimpleNamespace(
        direction="BUY",
        metadata={**signal.metadata, "microstructure_source": "live_store_depth"},
    )

    assert OpportunityRanker._microstructure_score(signal) > OpportunityRanker._microstructure_score(fallback_signal)
