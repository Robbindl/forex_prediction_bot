from integrations.ctrader_broker_bridge.ctrader_broker_bridge import CTraderOneShot


class _Obj:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


def test_pepperstone_live_gold_parity_uses_ctrader_symbol_specs():
    bridge = CTraderOneShot("place_order", {})
    bridge.assets_by_id = {1: "XAU", 2: "USD", 3: "EUR"}
    bridge.light_symbol_by_id = {
        10: _Obj(baseAssetId=1, quoteAssetId=2, symbolName="XAUUSD"),
        20: _Obj(baseAssetId=3, quoteAssetId=2, symbolName="EURUSD"),
    }
    gold = _Obj(pipPosition=2, lotSize=10_000, minVolume=100, maxVolume=1_000_000, stepVolume=100, digits=2)
    eurusd = _Obj(pipPosition=4, lotSize=10_000_000, minVolume=100_000, maxVolume=100_000_000, stepVolume=100_000, digits=5)

    gold_pip_usd, _gold_profile = bridge._pip_value_usd_at_001_lots(gold, 10)
    eur_pip_usd, _eur_profile = bridge._pip_value_usd_at_001_lots(eurusd, 20)
    raw_lots = 0.01 * (gold_pip_usd / eur_pip_usd)
    volume, broker_lots = bridge._snap_volume(eurusd, raw_lots, max_lots_cap=1.0)

    assert gold_pip_usd == 0.01
    assert eur_pip_usd == 0.1
    assert raw_lots == 0.001
    assert broker_lots == 0.01
    assert volume == 100_000


def test_pepperstone_live_gold_parity_converts_non_usd_quote_to_usd():
    bridge = CTraderOneShot("place_order", {})
    bridge.assets_by_id = {1: "USD", 2: "JPY"}
    bridge.light_symbol_by_id = {
        30: _Obj(baseAssetId=1, quoteAssetId=2, symbolName="USDJPY"),
    }
    bridge._pending_conversions = {
        "JPY": {"rate": 0.0064, "symbol_name": "USDJPY", "inverse": True}
    }
    usdjpy = _Obj(pipPosition=2, lotSize=10_000_000, minVolume=100_000, maxVolume=100_000_000, stepVolume=100_000, digits=3)

    pip_usd, profile = bridge._pip_value_usd_at_001_lots(usdjpy, 30)

    assert pip_usd == 0.064
    assert profile["quote_asset"] == "JPY"
    assert profile["quote_to_usd"] == 0.0064
