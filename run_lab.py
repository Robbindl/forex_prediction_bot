from __future__ import annotations

import sys


# ── Helpers ───────────────────────────────────────────────────────────────────

def _header(title: str) -> None:
    print()
    print("=" * 70)
    print(f"  {title}")
    print("=" * 70)


def _pick_asset() -> tuple:
    assets = [
        ("BTC-USD",  "crypto"),
        ("ETH-USD",  "crypto"),
        ("SOL-USD",  "crypto"),
        ("XRP-USD",  "crypto"),
        ("BNB-USD",  "crypto"),
        ("EUR/USD",  "forex"),
        ("GBP/USD",  "forex"),
        ("GBP/JPY",  "forex"),
        ("USD/JPY",  "forex"),
        ("AUD/USD",  "forex"),
        ("USD/CAD",  "forex"),
        ("GC=F",     "commodities"),
        ("SI=F",     "commodities"),
        ("CL=F",     "commodities"),
        ("^DJI",     "indices"),
        ("^IXIC",    "indices"),
        ("^GSPC",    "indices"),
        ("^FTSE",    "indices"),
    ]
    print()
    for i, (asset, cat) in enumerate(assets, 1):
        print(f"  {i:2}. {asset:12}  ({cat})")
    print()
    while True:
        try:
            choice = int(input("Pick asset number: ").strip())
            if 1 <= choice <= len(assets):
                return assets[choice - 1]
        except (ValueError, KeyboardInterrupt):
            pass
        print("  Invalid — enter a number from the list")


def _print_result(name: str, result) -> None:
    print(
        f"  {name:35}  "
        f"Sharpe={result.sharpe_ratio:+6.2f}  "
        f"WinRate={result.win_rate:5.1%}  "
        f"PnL={result.total_pnl:+8.2f}  "
        f"MaxDD={result.max_drawdown:5.1%}  "
        f"Trades={result.total_trades:3}"
    )


# ── Option 1 — All presets on one asset ───────────────────────────────────────

def run_all_presets() -> None:
    from strategy_lab import run_backtest, StrategyBuilder

    _header("ALL 15 PRESET STRATEGIES")
    asset, category = _pick_asset()
    print(f"\n  Running 15 backtests on {asset} ({category}) — please wait...\n")

    results = []
    for name, config in StrategyBuilder.all_configs().items():
        try:
            result = run_backtest(config, asset, category)
            results.append((name, result))
            _print_result(name, result)
        except Exception as e:
            print(f"  {name:35}  ERROR: {e}")

    if results:
        best = max(results, key=lambda x: x[1].sharpe_ratio)
        print()
        print(f"  Best Sharpe: {best[0]}  ({best[1].sharpe_ratio:+.2f})")


# ── Option 2 — Lab presets vs existing strategies ─────────────────────────────

def compare_all() -> None:
    from strategy_lab import run_backtest, StrategyBuilder
    from strategy_lab.strategy_adapter import compare_all_strategies_from_asset

    _header("LAB PRESETS vs YOUR EXISTING STRATEGIES")
    asset, category = _pick_asset()
    print(f"\n  Running on {asset} ({category}) — please wait...\n")

    print("  LAB PRESETS")
    print("  " + "-" * 66)
    lab_results = []
    for name, config in StrategyBuilder.all_configs().items():
        try:
            result = run_backtest(config, asset, category)
            lab_results.append((name, result))
            _print_result(name, result)
        except Exception as e:
            print(f"  {name:35}  ERROR: {e}")

    print()
    print("  YOUR EXISTING STRATEGIES")
    print("  " + "-" * 66)
    try:
        existing = compare_all_strategies_from_asset(asset, category)
        for r in existing:
            print(
                f"  {r['label']:35}  "
                f"Sharpe={r['sharpe']:+6.2f}  "
                f"WinRate={r['win_rate']:5.1%}  "
                f"PnL={r['total_pnl']:+8.2f}  "
                f"MaxDD={r['max_drawdown']:5.1%}  "
                f"Trades={r['trades']:3}"
            )
    except Exception as e:
        print(f"  Could not run existing strategies: {e}")

    if lab_results:
        best = max(lab_results, key=lambda x: x[1].sharpe_ratio)
        print()
        print(f"  Best lab strategy: {best[0]}  (Sharpe={best[1].sharpe_ratio:+.2f})")
        print(f"  To activate: uncomment {best[0]}_config() in strategy_lab/live_bridge.py")


# ── Option 3 — Parameter optimiser ───────────────────────────────────────────

def optimise_strategy() -> None:
    from strategy_lab import optimize_strategy, StrategyBuilder

    _header("PARAMETER OPTIMISER")

    strategies = list(StrategyBuilder.all_configs().keys())
    print()
    for i, name in enumerate(strategies, 1):
        print(f"  {i:2}. {name}")
    print()

    while True:
        try:
            choice = int(input("Pick strategy number: ").strip())
            if 1 <= choice <= len(strategies):
                break
        except (ValueError, KeyboardInterrupt):
            pass
        print("  Invalid choice")

    chosen_name   = strategies[choice - 1]
    chosen_config = StrategyBuilder.all_configs()[chosen_name]
    asset, category = _pick_asset()

    print(f"\n  Optimising {chosen_name} on {asset} ({category})...")
    print("  Grid: rsi_period=[10,14,21]  stop_mult=[1.0,1.5,2.0]  tp_mult=[2.0,3.0,4.0]\n")

    try:
        results = optimize_strategy(
            base_config=chosen_config,
            param_grid={
                "rsi_period": [10, 14, 21],
                "stop_mult":  [1.0, 1.5, 2.0],
                "tp_mult":    [2.0, 3.0, 4.0],
            },
            asset=asset,
            category=category,
        )
        print("  TOP 5 PARAMETER COMBINATIONS")
        print("  " + "-" * 66)
        for i, r in enumerate(results[:5], 1):
            print(
                f"  {i}. "
                f"rsi={r.get('rsi_period', '-'):2}  "
                f"stop={r.get('stop_mult', '-'):.1f}  "
                f"tp={r.get('tp_mult', '-'):.1f}  "
                f"Sharpe={r.get('sharpe', 0):+.2f}  "
                f"WinRate={r.get('win_rate', 0):.1%}  "
                f"PnL={r.get('total_pnl', 0):+.2f}  "
                f"Trades={r.get('trades', 0)}"
            )
    except Exception as e:
        print(f"  Optimiser error: {e}")


# ── Option 4 — One strategy across multiple assets ───────────────────────────

def multi_asset_test() -> None:
    from strategy_lab import run_backtest, StrategyBuilder

    _header("MULTI-ASSET TEST")

    strategies = list(StrategyBuilder.all_configs().keys())
    print()
    for i, name in enumerate(strategies, 1):
        print(f"  {i:2}. {name}")
    print()

    while True:
        try:
            choice = int(input("Pick strategy number: ").strip())
            if 1 <= choice <= len(strategies):
                break
        except (ValueError, KeyboardInterrupt):
            pass
        print("  Invalid choice")

    chosen_name   = strategies[choice - 1]
    chosen_config = StrategyBuilder.all_configs()[chosen_name]

    test_assets = [
        ("BTC-USD",  "crypto"),
        ("ETH-USD",  "crypto"),
        ("SOL-USD",  "crypto"),
        ("EUR/USD",  "forex"),
        ("GBP/USD",  "forex"),
        ("USD/JPY",  "forex"),
        ("GC=F",     "commodities"),
        ("^DJI",     "indices"),
    ]

    print(f"\n  Testing {chosen_name} across {len(test_assets)} assets...\n")

    for asset, category in test_assets:
        try:
            result = run_backtest(chosen_config, asset, category)
            _print_result(asset, result)
        except Exception as e:
            print(f"  {asset:35}  ERROR: {e}")


# ── Option 5 — Full report ────────────────────────────────────────────────────

def full_report() -> None:
    from strategy_lab import run_backtest, StrategyBuilder

    _header("FULL REPORT — ALL PRESETS ON ALL YOUR ASSETS")

    assets = [
        ("BTC-USD",  "crypto"),
        ("ETH-USD",  "crypto"),
        ("EUR/USD",  "forex"),
        ("GBP/USD",  "forex"),
        ("GBP/JPY",  "forex"),
        ("GC=F",     "commodities"),
        ("^DJI",     "indices"),
        ("^GSPC",    "indices"),
    ]

    configs = StrategyBuilder.all_configs()
    best_overall = []

    for asset, category in assets:
        print(f"\n  {asset} ({category})")
        print("  " + "-" * 66)
        asset_results = []
        for name, config in configs.items():
            try:
                result = run_backtest(config, asset, category)
                asset_results.append((name, result))
                _print_result(name, result)
            except Exception as e:
                print(f"  {name:35}  ERROR: {e}")
        if asset_results:
            best = max(asset_results, key=lambda x: x[1].sharpe_ratio)
            best_overall.append((asset, best[0], best[1].sharpe_ratio))
            print(f"\n  → Best for {asset}: {best[0]} (Sharpe={best[1].sharpe_ratio:+.2f})")

    if best_overall:
        print()
        _header("SUMMARY — BEST STRATEGY PER ASSET")
        for asset, name, sharpe in best_overall:
            print(f"  {asset:12}  →  {name:35}  Sharpe={sharpe:+.2f}")


# ── Option 6 — Custom strategy ────────────────────────────────────────────────

def custom_strategy() -> None:
    from strategy_lab import run_backtest

    _header("CUSTOM STRATEGY BACKTEST")

    print("""
  Edit the config below in run_lab.py then re-run.

  Supported indicators: rsi, ema, macd, bollinger, atr, volume_ma, stoch, adx
  Supported operators:  >  <  >=  <=  cross_above  cross_below
    """)

    # ── Edit your custom config here ──────────────────────────────────────────
    custom_config = {
        "name":    "my_custom_strategy",
        "version": "1.0",
        "indicators": [
            {"name": "stoch", "params": {"k_period": 14, "d_period": 3}},
            {"name": "ema",   "params": {"period": 50}},
            {"name": "atr",   "params": {"period": 14}},
        ],
        "entry_rules": [
            {"col": "stoch_k", "op": "cross_above",
             "col2": "stoch_d", "direction": "BUY"},
            {"col": "stoch_k", "op": "<",  "val": 50},
            {"col": "close",   "op": ">",  "col2": "ema_50"},
        ],
        "confidence_boosts": [
            {"col": "stoch_k", "below": 30, "boost": 0.07},
        ],
        "stop_mult": 1.5,
        "tp_mult":   2.5,
    }
    # ─────────────────────────────────────────────────────────────────────────

    asset, category = _pick_asset()
    print(f"\n  Running {custom_config['name']} on {asset}...\n")

    try:
        result = run_backtest(custom_config, asset, category)
        _print_result(custom_config["name"], result)
        print()
        print(f"  Detailed stats:")
        print(f"    Profit factor : {result.profit_factor:.2f}")
        print(f"    Expectancy    : ${result.expectancy:.2f} per trade")
        print(f"    Largest win   : ${result.largest_win:.2f}")
        print(f"    Largest loss  : ${result.largest_loss:.2f}")
        print(f"    Avg win       : ${result.avg_win:.2f}")
        print(f"    Avg loss      : ${result.avg_loss:.2f}")
        if result.total_trades > 0:
            print()
            print(f"  To activate live: add this config to")
            print(f"  LIVE_STRATEGY_CONFIGS in strategy_lab/live_bridge.py")
    except Exception as e:
        print(f"  Error: {e}")


# ── Main menu ─────────────────────────────────────────────────────────────────

def main() -> None:
    print()
    print("  ╔══════════════════════════════════════════╗")
    print("  ║       STRATEGY LABORATORY                ║")
    print("  ║       Trading Intelligence Platform      ║")
    print("  ╚══════════════════════════════════════════╝")

    menu = {
        "1": ("Backtest all 15 presets on one asset",           run_all_presets),
        "2": ("Compare presets vs your existing strategies",    compare_all),
        "3": ("Optimise parameters for a strategy",             optimise_strategy),
        "4": ("Test one strategy across multiple assets",       multi_asset_test),
        "5": ("Full report — all presets on all your assets",   full_report),
        "6": ("Backtest a custom strategy config",              custom_strategy),
        "0": ("Exit",                                           None),
    }

    while True:
        print()
        for key, (label, _) in menu.items():
            print(f"  {key}.  {label}")
        print()

        choice = input("  Choose an option: ").strip()

        if choice == "0":
            print("\n  Goodbye.\n")
            sys.exit(0)

        if choice in menu:
            _, fn = menu[choice]
            try:
                fn()
            except KeyboardInterrupt:
                print("\n  Cancelled.")
        else:
            print("  Invalid choice — enter 0–6")


if __name__ == "__main__":
    main()