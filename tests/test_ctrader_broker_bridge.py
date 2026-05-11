from __future__ import annotations

from types import SimpleNamespace

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


def test_snap_volume_ignores_absurd_ctrader_volume_sentinels() -> None:
    symbol = SimpleNamespace(
        lotSize=100,
        minVolume=99999999999900,
        maxVolume=99999999999900,
        stepVolume=0,
    )

    volume, lots = CTraderOneShot._snap_volume(symbol, 1.0, max_lots_cap=0.01)

    assert volume == 1
    assert lots == 0.01


def test_index_zero_pip_position_is_valid_one_point_pip() -> None:
    symbol = SimpleNamespace(pipPosition=0)

    assert CTraderOneShot._symbol_pip_size(symbol) == 1.0
