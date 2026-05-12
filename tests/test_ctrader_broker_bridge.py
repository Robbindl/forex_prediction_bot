from __future__ import annotations

from types import SimpleNamespace

import pytest
from ctrader_open_api.messages.OpenApiModelMessages_pb2 import ProtoOATradingMode

from integrations.ctrader_broker_bridge.ctrader_broker_bridge import CTraderOneShot


def test_order_price_precision_uses_ctrader_symbol_digits() -> None:
    bridge = CTraderOneShot.__new__(CTraderOneShot)
    bridge.payload = {
        "entry_price": 86.43929999999997,
        "stop_loss": 85.87654321,
        "take_profit": 87.123456789,
    }

    precision = bridge._order_price_precision(SimpleNamespace(digits=3))

    assert precision["entry_price"] == 86.439
    assert precision["stop_loss"] == 85.877
    assert precision["take_profit"] == 87.123
    assert precision["entry_price_changed"] is True
    assert precision["stop_loss_changed"] is True
    assert precision["take_profit_changed"] is True


def test_market_order_sltp_uses_relative_distances_not_absolute_prices() -> None:
    fields, meta = CTraderOneShot._market_order_relative_sltp_kwargs(
        entry_price=2.50000,
        stop_loss=2.45000,
        take_profit=2.65000,
    )

    assert fields == {
        "relativeStopLoss": 5000,
        "relativeTakeProfit": 15000,
    }
    assert "stopLoss" not in fields
    assert "takeProfit" not in fields
    assert meta["unit"] == "1/100000_price"


def test_pepperstone_crypto_alt_quote_keeps_fallbacks_in_order(monkeypatch) -> None:
    monkeypatch.setenv("CTRADER_EXECUTION_BROKER_NAME", "pepperstone")
    monkeypatch.setenv("PEPPERSTONE_CTRADER_CRYPTO_ALT_QUOTES", "EUR,GBP,AUD")
    bridge = CTraderOneShot.__new__(CTraderOneShot)
    bridge.symbol_lookup = {
        "ETHEUR": (11, "ETHEUR"),
        "ETHGBP": (12, "ETHGBP"),
    }

    selected = bridge._resolve_pepperstone_crypto_alt("ETH-USD")

    assert selected is not None
    assert selected["symbol_name"] == "ETHEUR"
    assert selected["broker_quote"] == "EUR"
    assert selected["fallbacks"] == [
        {
            "requested_asset": "ETH-USD",
            "symbol_id": 12,
            "symbol_name": "ETHGBP",
            "broker_base": "ETH",
            "broker_quote": "GBP",
            "signal_quote": "USD",
            "reason": "pepperstone_crypto_alt_quote",
        }
    ]


def test_symbol_trading_enabled_rejects_disabled_contract() -> None:
    disabled = SimpleNamespace(tradingMode=ProtoOATradingMode.CLOSE_ONLY_MODE)
    enabled = SimpleNamespace(tradingMode=ProtoOATradingMode.ENABLED)

    assert CTraderOneShot._symbol_trading_enabled(enabled) is True
    assert CTraderOneShot._symbol_trading_enabled(disabled) is False
    assert CTraderOneShot._symbol_trading_mode_label(disabled) == "CLOSE_ONLY_MODE"


def test_crypto_max_lots_defaults_to_tiny_cap(monkeypatch) -> None:
    monkeypatch.delenv("PEPPERSTONE_CTRADER_MAX_LOTS_CRYPTO", raising=False)
    monkeypatch.delenv("PEPPERSTONE_CTRADER_LIVE_MAX_LOTS_CRYPTO", raising=False)
    monkeypatch.setenv("PEPPERSTONE_CTRADER_MAX_LOTS", "1.00")
    bridge = CTraderOneShot.__new__(CTraderOneShot)

    cap, source = bridge._max_lots_cap_for_asset("ETH-USD")

    assert cap == 0.01
    assert source == "crypto_default_cap"


def test_crypto_max_lots_allows_explicit_category_override(monkeypatch) -> None:
    monkeypatch.setenv("PEPPERSTONE_CTRADER_MAX_LOTS", "1.00")
    monkeypatch.setenv("PEPPERSTONE_CTRADER_MAX_LOTS_CRYPTO", "0.03")
    bridge = CTraderOneShot.__new__(CTraderOneShot)

    cap, source = bridge._max_lots_cap_for_asset("BTC-USD")

    assert cap == 0.03
    assert source == "crypto_env_cap"


def test_snap_volume_rejects_broker_minimum_above_risk_cap() -> None:
    symbol = SimpleNamespace(
        lotSize=100,
        minVolume=99999999999900,
        maxVolume=99999999999900,
        stepVolume=0,
    )

    with pytest.raises(RuntimeError, match="minimum volume .* exceeds configured max cap"):
        CTraderOneShot._snap_volume(symbol, 1.0, max_lots_cap=0.01)


def test_index_zero_pip_position_is_valid_one_point_pip() -> None:
    symbol = SimpleNamespace(pipPosition=0)

    assert CTraderOneShot._symbol_pip_size(symbol) == 1.0


def test_pepperstone_unroutable_crypto_is_blocked_before_submission(monkeypatch) -> None:
    from execution.ctrader_adapter import CTraderAdapter

    monkeypatch.delenv("PEPPERSTONE_CTRADER_EXECUTION_ALLOW_HIGH_MIN_CRYPTO", raising=False)
    adapter = CTraderAdapter()

    supported, reason = adapter.supports_asset("BNB-USD", "crypto")

    assert supported is False
    assert "no normal-size tradable contract" in reason


def test_pepperstone_btc_eth_are_allowed_for_alt_quote_execution(monkeypatch) -> None:
    from execution.ctrader_adapter import CTraderAdapter

    monkeypatch.delenv("PEPPERSTONE_CTRADER_EXECUTION_DISABLED_ASSETS", raising=False)
    adapter = CTraderAdapter()

    assert adapter.supports_asset("BTC-USD", "crypto")[0] is True
    assert adapter.supports_asset("ETH-USD", "crypto")[0] is True


def test_ctrader_close_uses_exact_broker_volume(monkeypatch) -> None:
    from execution.ctrader_adapter import CTraderAdapter

    adapter = CTraderAdapter()
    captured = {}

    def fake_run_bridge(action: str, payload: dict) -> dict:
        captured["action"] = action
        captured["payload"] = dict(payload)
        return {"success": True, "avg_price": 86.12}

    monkeypatch.setattr(adapter, "_dry_run", lambda: False)
    monkeypatch.setattr(adapter, "_run_bridge", fake_run_bridge)
    position = {
        "trade_id": "216941623",
        "broker_trade_id": "216941623",
        "position_size": 50.0,
        "broker_volume": 5000,
        "current_price": 86.12,
        "metadata": {},
    }

    result = adapter.close_position(position, reason="test")

    assert result.status == "FILLED"
    assert captured["action"] == "partial_close"
    assert captured["payload"]["position_id"] == "216941623"
    assert captured["payload"]["volume"] == 5000


def test_ctrader_partial_close_snaps_to_saved_volume_step() -> None:
    from execution.ctrader_adapter import CTraderAdapter

    position = {
        "position_size": 71.43,
        "broker_volume": 7143,
        "metadata": {
            "broker_execution": {
                "broker_sizing": {
                    "broker_volume": 7143,
                    "volume_step": 50,
                }
            }
        },
    }

    volume = CTraderAdapter._close_volume_for_local_size(position, 21.43)

    assert volume == 2100
    assert volume % 50 == 0


def test_ctrader_bridge_close_volume_snaps_to_symbol_step() -> None:
    symbol = SimpleNamespace(stepVolume=50)

    close_volume, meta = CTraderOneShot._snap_close_volume_to_step(
        symbol,
        21.43,
        live_position_volume=7143,
    )

    assert close_volume == 50
    assert close_volume % 50 == 0
    assert meta["requested_volume"] == 21
    assert meta["adjusted"] is True


def test_ctrader_ambiguous_close_reconciles_when_position_missing(monkeypatch) -> None:
    from execution.ctrader_adapter import CTraderAdapter

    adapter = CTraderAdapter()

    def fake_run_bridge(_action: str, _payload: dict) -> dict:
        return {
            "success": False,
            "error": "ctrader_execution_unknown: cTrader execution bridge timed out at stage=execution_event after an order request was sent; check broker before retrying",
        }

    monkeypatch.setattr(adapter, "_dry_run", lambda: False)
    monkeypatch.setattr(adapter, "_run_bridge", fake_run_bridge)
    monkeypatch.setattr(adapter, "list_open_positions", lambda: [])
    position = {
        "trade_id": "216941623",
        "broker_trade_id": "216941623",
        "position_size": 50.0,
        "broker_volume": 5000,
        "current_price": 83.90,
        "metadata": {},
    }

    result = adapter.close_position(position, reason="Managed Stop")

    assert result.status == "FILLED"
    assert result.raw["reconciled_after_error"] is True
    assert result.raw["reconcile_result"] == "position_missing_after_close_attempt"


def test_ctrader_trade_snapshot_preserves_trade_metadata() -> None:
    from execution.ctrader_adapter import CTraderAdapter
    from execution.exchange_adapter import OrderRequest

    adapter = CTraderAdapter()
    req = OrderRequest(
        symbol="XAUUSD",
        side="BUY",
        quantity=1.0,
        asset="XAU/USD",
        category="commodities",
        local_quantity=1.0,
        price=4711.53,
        stop_loss=4700.48,
        take_profit=4731.17,
        metadata={
            "confidence": 0.74,
            "strategy_id": "breakout_continuation",
            "take_profit_levels": [4731.17, 4742.21],
            "timestamp": "2026-05-12T12:52:22+00:00",
        },
    )

    snapshot = adapter._build_trade_snapshot(
        req,
        {
            "asset": "XAU/USD",
            "category": "commodities",
            "side": "BUY",
            "entry_price": 4711.53,
            "stop_loss": 4700.48,
            "take_profit": 4731.17,
            "local_size": 1.0,
            "lot_size": 1.0,
            "volume": 100,
        },
        {
            "account_id": "47228282",
            "symbol_id": "41",
            "symbol_name": "XAUUSD",
        },
        order_id="217015017",
        avg_price=4711.53,
        filled_size=1.0,
    )

    assert snapshot["confidence"] == pytest.approx(0.74)
    assert snapshot["strategy_id"] == "breakout_continuation"
    assert snapshot["requested_entry_price"] == pytest.approx(4711.53)
    assert snapshot["highest_price"] == pytest.approx(4711.53)
    assert snapshot["lowest_price"] == pytest.approx(4711.53)
    assert snapshot["tp_hit"] == 0
    assert snapshot["risk_reward"] > 0.0
    assert snapshot["entry_time"] == snapshot["open_time"]
    assert snapshot["management_checkpoint_at"] == snapshot["open_time"]
    assert snapshot["take_profit_levels"] == [4731.17, 4742.21]
