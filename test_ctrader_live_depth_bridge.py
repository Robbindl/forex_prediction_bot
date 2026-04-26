from __future__ import annotations

import time

from data.fetcher import DataFetcher
from services.ctrader_live_depth_bridge import CTraderLiveDepthBridge
from services.live_microstructure_service import get_service as get_live_microstructure_service


def test_ctrader_live_depth_bridge_ingest_snapshot_exposes_true_depth(tmp_path) -> None:
    service = get_live_microstructure_service()
    service.clear()

    bridge = CTraderLiveDepthBridge(
        enabled=True,
        store_path=tmp_path / "ctrader_depth.json",
        token_cache_path=tmp_path / "ctrader_tokens.json",
        assets=("EUR/USD",),
        environment="demo",
        client_id="demo_client",
        client_secret="demo_secret",
        access_token="demo_access",
        account_id="9998492",
    )

    bridge.ingest_snapshot(
        {
            "asset": "EUR/USD",
            "symbol_id": 101,
            "symbol_name": "EURUSD",
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
            "as_of_utc": "2026-04-26T12:00:00Z",
            "environment": "demo",
            "broker": "IC Markets",
        }
    )

    micro = bridge.get_microstructure("EUR/USD", category="forex")

    assert micro["depth_available"] is True
    assert micro["microstructure_source"] == "ctrader_live_depth"
    assert micro["depth_provider"] == "IC Markets cTrader"
    assert micro["orderbook_top_bids"][:2] == [[1.1, 3.0], [1.0999, 10.0]]
    assert micro["orderbook_top_asks"][:2] == [[1.1002, 4.0], [1.1003, 7.0]]
    assert micro["bid_vol"] == 13.0
    assert micro["ask_vol"] == 11.0


def test_ctrader_live_depth_bridge_status_reports_token_and_assets(tmp_path) -> None:
    bridge = CTraderLiveDepthBridge(
        enabled=True,
        store_path=tmp_path / "ctrader_depth.json",
        token_cache_path=tmp_path / "ctrader_tokens.json",
        assets=("EUR/USD", "XAU/USD"),
        environment="demo",
        client_id="demo_client",
        client_secret="demo_secret",
        refresh_token="demo_refresh",
        account_id="9998492",
    )

    status = bridge.status()

    assert status["enabled"] is True
    assert status["has_client"] is True
    assert status["has_token"] is True
    assert status["account_id"] == "9998492"


def test_ctrader_true_depth_overlay_can_override_dukascopy_when_fresher() -> None:
    base = {"source": "IG", "score": 0.12, "depth_available": False, "microstructure_source": "live_store"}
    dukascopy = {
        "source": "Dukascopy",
        "source_class": "sidecar",
        "score": 0.72,
        "depth_available": True,
        "depth_levels": 3,
        "depth_live_age_seconds": 2.0,
        "depth_provider": "Dukascopy",
        "microstructure_source": "dukascopy_live_depth",
    }
    ctrader = {
        "source": "cTrader",
        "source_class": "sidecar",
        "score": 0.91,
        "depth_available": True,
        "depth_levels": 8,
        "depth_live_age_seconds": 1.0,
        "depth_provider": "IC Markets cTrader",
        "microstructure_source": "ctrader_live_depth",
    }

    payload = DataFetcher._overlay_external_true_depth(
        DataFetcher._overlay_external_true_depth(base, dukascopy),
        ctrader,
    )

    assert payload["depth_provider"] == "IC Markets cTrader"
    assert payload["microstructure_source"] == "ctrader_live_depth"
    assert payload["score"] == 0.91
