from __future__ import annotations

import importlib
import sys
from datetime import datetime, timezone
from types import ModuleType, SimpleNamespace

from core.asset_profiles import get_profile
from core.assets import registry


def test_registry_and_profiles_include_new_ig_assets() -> None:
    expected = {
        "GER40": "indices",
        "AUS200": "indices",
        "JPN225": "indices",
        "NZD/USD": "forex",
        "EUR/GBP": "forex",
        "USD/CHF": "forex",
    }

    aliases = {
        "DAX": "GER40",
        "ASX200": "AUS200",
        "NIKKEI225": "JPN225",
        "NZDUSD": "NZD/USD",
        "EURGBP": "EUR/GBP",
        "USDCHF": "USD/CHF",
    }

    for canonical, category in expected.items():
        assert registry.canonical(canonical) == canonical
        assert registry.category(canonical) == category
        assert get_profile(canonical).category == category

    for alias, canonical in aliases.items():
        assert registry.canonical(alias) == canonical
        assert get_profile(alias).category == expected[canonical]


def test_market_hours_guard_recognises_new_regional_indices() -> None:
    guard = importlib.import_module("services.market_hours_guard")

    ger_open, ger_reason = guard.session_market_status(
        "GER40",
        "indices",
        now_utc=datetime(2026, 4, 7, 11, 0, tzinfo=timezone.utc),
    )
    aus_open, aus_reason = guard.session_market_status(
        "AUS200",
        "indices",
        now_utc=datetime(2026, 4, 7, 1, 0, tzinfo=timezone.utc),
    )
    jpn_open, jpn_reason = guard.session_market_status(
        "JPN225",
        "indices",
        now_utc=datetime(2026, 4, 7, 2, 0, tzinfo=timezone.utc),
    )

    assert ger_open is True
    assert aus_open is True
    assert jpn_open is True
    assert ger_reason != "Unknown index asset"
    assert aus_reason != "Unknown index asset"
    assert jpn_reason != "Unknown index asset"


def test_market_data_router_uses_explicit_ig_assets(monkeypatch) -> None:
    router = importlib.import_module("services.market_data_router")
    monkeypatch.setattr(router, "IG_ROUTED_CATEGORIES", ["commodities"], raising=False)
    monkeypatch.setattr(
        router,
        "IG_ROUTED_ASSETS",
        ["GER40", "AUS200", "JP225", "NZDUSD", "EURGBP", "USDCHF"],
        raising=False,
    )
    router._configured_ig_routed_assets.cache_clear()

    fake_module = ModuleType("services.ig_market_bridge")
    fake_module.ig_market_bridge = SimpleNamespace(
        list_profiles=lambda: ["ig"],
        supports=lambda asset, category="": True,
    )
    monkeypatch.setitem(sys.modules, "services.ig_market_bridge", fake_module)

    assert router.is_ig_primary_asset("GER40", "indices") is True
    assert router.is_ig_primary_asset("NZD/USD", "forex") is True
    assert router.is_ig_primary_asset("XAU/USD", "commodities") is True
    assert router.is_ig_primary_asset("US100", "indices") is False

    deriv_assets = router.filter_deriv_stream_assets(
        {
            "GER40": "indices",
            "US100": "indices",
            "NZD/USD": "forex",
            "XAU/USD": "commodities",
        }
    )
    ig_assets = router.filter_ig_primary_assets(
        {
            "GER40": "indices",
            "US100": "indices",
            "NZD/USD": "forex",
            "XAU/USD": "commodities",
        }
    )

    assert deriv_assets == {"US100": "indices"}
    assert ig_assets == {
        "GER40": "indices",
        "NZD/USD": "forex",
        "XAU/USD": "commodities",
    }


def test_ig_bridge_scores_new_assets_without_network(monkeypatch) -> None:
    ig_mod = importlib.import_module("services.ig_market_bridge")
    monkeypatch.setattr(ig_mod, "IG_ENABLED", True, raising=False)
    monkeypatch.setattr(ig_mod, "IG_API_KEY", "demo-key", raising=False)

    bridge = ig_mod.IGMarketBridge()

    assert bridge.supports("GER40", "indices") is True
    assert bridge.supports("NZD/USD", "forex") is True
    assert bridge.supports("USD/CHF", "forex") is True

    ger_score = bridge._candidate_score(
        "GER40",
        {
            "instrumentType": "INDICES",
            "instrumentName": "Germany 40 Daily Funded Bet",
            "epic": "IX.D.DAX.IFD.IP",
            "expiry": "DFB",
            "marketStatus": "TRADEABLE",
            "streamingPricesAvailable": True,
            "delayTime": 0,
        },
    )
    eurgbp_score = bridge._candidate_score(
        "EUR/GBP",
        {
            "instrumentType": "CURRENCIES",
            "instrumentName": "EUR/GBP Daily Funded Bet",
            "epic": "CS.D.EURGBP.CFD.IP",
            "expiry": "DFB",
            "marketStatus": "TRADEABLE",
            "streamingPricesAvailable": True,
            "delayTime": 0,
        },
    )
    wrong_index_score = bridge._candidate_score(
        "GER40",
        {
            "instrumentType": "INDICES",
            "instrumentName": "UK 100 Daily Funded Bet",
            "epic": "IX.D.FTSE.IFD.IP",
            "expiry": "DFB",
            "marketStatus": "TRADEABLE",
            "streamingPricesAvailable": True,
            "delayTime": 0,
        },
    )

    assert ger_score > 0
    assert eurgbp_score > 0
    assert wrong_index_score == 0


def test_new_index_specs_and_stale_maps_are_updated() -> None:
    from risk.position_sizer import CONTRACT_SPECS
    from reddit_watcher import RedditWatcher
    from websocket_dashboard import connection_status

    assert CONTRACT_SPECS["GER40"] == {
        "contract": 25,
        "pip": 1.0,
        "pip_val": 28.95,
        "base_lots": 3.25,
        "min_lot": 0.01,
        "lot_step": 0.01,
    }
    assert CONTRACT_SPECS["AUS200"] == {
        "contract": 25,
        "pip": 1.0,
        "pip_val": 17.91,
        "base_lots": 5.35,
        "min_lot": 0.01,
        "lot_step": 0.01,
    }
    assert CONTRACT_SPECS["JPN225"] == {
        "contract": 5,
        "pip": 1.0,
        "pip_val": 5.0,
        "base_lots": 10.0,
        "min_lot": 0.01,
        "lot_step": 0.01,
    }

    for asset in ("GER40", "AUS200", "JPN225", "NZD/USD", "EUR/GBP", "USD/CHF"):
        assert asset in RedditWatcher.ASSET_SUBREDDITS
        assert asset in RedditWatcher.ASSET_TERMS

    assert "GER40" in connection_status["ig"]["assets"]
    assert "AUS200" in connection_status["ig"]["assets"]
    assert "JPN225" in connection_status["ig"]["assets"]
