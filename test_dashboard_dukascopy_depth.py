from __future__ import annotations

import importlib


class _Resp:
    def __init__(self, payload):
        self._payload = payload

    def get_json(self):
        return self._payload


def test_api_phase3_live_depth_returns_dukascopy_rows(monkeypatch) -> None:
    dashboard_mod = importlib.import_module("dashboard.web_app_live")
    bridge_mod = importlib.import_module("services.dukascopy_live_depth_bridge")

    monkeypatch.setattr(dashboard_mod, "_cache_get", lambda key: None, raising=False)
    monkeypatch.setattr(dashboard_mod, "_cache_set", lambda key, value, ttl=0: None, raising=False)

    class _FakeBridge:
        def status(self):
            return {
                "enabled": True,
                "running": True,
                "profiles": ["dukascopy_live_depth"],
                "assets": ["EUR/USD", "XAU/USD"],
            }

        def supports(self, asset: str, category: str = "") -> bool:
            return asset in {"EUR/USD", "XAU/USD"} and category != "crypto"

        def get_latest_snapshot(self, asset: str):
            if asset == "EUR/USD":
                return {
                    "price": 1.10234,
                    "bid": 1.1023,
                    "ask": 1.10238,
                    "environment": "demo",
                    "dukascopy_symbol": "EUR/USD",
                    "as_of_utc": "2026-04-24T12:00:00Z",
                }
            if asset == "XAU/USD":
                return {
                    "price": 3332.4,
                    "bid": 3332.2,
                    "ask": 3332.6,
                    "environment": "demo",
                    "dukascopy_symbol": "XAU/USD",
                    "as_of_utc": "2026-04-24T12:00:00Z",
                }
            return {}

        def get_microstructure(self, asset: str, category: str = ""):
            if asset == "EUR/USD":
                return {
                    "spread_bps": 0.7,
                    "score": 0.42,
                    "book_imbalance": 0.21,
                    "pressure_direction": "buy",
                    "depth_levels": 6,
                    "bid_vol": 18.0,
                    "ask_vol": 12.0,
                    "orderbook_top_bids": [[1.1023, 5.0]],
                    "orderbook_top_asks": [[1.10238, 4.0]],
                    "depth_live_age_seconds": 0.8,
                }
            if asset == "XAU/USD":
                return {
                    "spread_bps": 1.2,
                    "score": -0.18,
                    "book_imbalance": -0.12,
                    "pressure_direction": "sell",
                    "depth_levels": 5,
                    "bid_vol": 9.0,
                    "ask_vol": 15.0,
                    "orderbook_top_bids": [[3332.2, 2.0]],
                    "orderbook_top_asks": [[3332.6, 3.0]],
                    "depth_live_age_seconds": 1.4,
                }
            return {}

    monkeypatch.setattr(bridge_mod, "dukascopy_live_depth_bridge", _FakeBridge(), raising=False)

    with dashboard_mod.app.test_request_context("/api/phase3/live-depth"):
        response = dashboard_mod.api_phase3_live_depth()

    payload = response.get_json()
    assert response.status_code == 200
    assert payload["success"] is True
    assert payload["running"] is True
    assert payload["count"] == 2
    assert payload["rows"][0]["asset"] == "EUR/USD"
    assert payload["rows"][0]["orderbook_top_bids"][0] == [1.1023, 5.0]


def test_page_overview_order_flow_includes_depth_payload(monkeypatch) -> None:
    dashboard_mod = importlib.import_module("dashboard.web_app_live")

    monkeypatch.setattr(dashboard_mod, "_DEVELOPMENT_MODE", True, raising=False)
    monkeypatch.setattr(dashboard_mod, "_AUTH_CONFIG_ERROR", "", raising=False)
    monkeypatch.setattr(dashboard_mod, "_cache_get", lambda key: None, raising=False)
    monkeypatch.setattr(dashboard_mod, "_cache_set", lambda key, value, ttl=0: None, raising=False)
    monkeypatch.setattr(dashboard_mod, "_request_wants_cache_bypass", lambda: True, raising=False)
    monkeypatch.setattr(dashboard_mod, "_command_center_snapshot", lambda: {}, raising=False)

    def _fake_call_view(view):
        mapping = {
            "api_status": {"engine_running": True},
            "api_phase3_imbalance": {"success": True, "imbalances": {}},
            "api_phase3_walls": {"success": True, "walls": []},
            "api_phase3_stop_hunts": {"success": True, "hunts": []},
            "api_phase3_live_depth": {"success": True, "rows": [{"asset": "EUR/USD"}], "count": 1},
        }
        return _Resp(mapping.get(view.__name__, {"success": True}))

    monkeypatch.setattr(dashboard_mod, "_call_view", _fake_call_view, raising=False)

    with dashboard_mod.app.test_request_context("/api/page-overview?page=order_flow&no_cache=1"):
        response = dashboard_mod.api_page_overview()

    payload = response.get_json()
    assert response.status_code == 200
    assert payload["success"] is True
    assert payload["depth"]["success"] is True
    assert payload["depth"]["count"] == 1


def test_extract_signal_intelligence_fields_exposes_depth_provider() -> None:
    dashboard_mod = importlib.import_module("dashboard.web_app_live")

    payload = dashboard_mod._extract_signal_intelligence_fields(
        {
            "market_microstructure": {
                "depth_available": True,
                "depth_provider": "Dukascopy",
                "microstructure_source": "dukascopy_live_depth",
            }
        }
    )

    assert payload["depth_provider"] == "Dukascopy"
