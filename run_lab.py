from __future__ import annotations

import copy
import sys

SCREENING_RESEARCH_PROFILE = "standard"
FINAL_RESEARCH_PROFILE = "deep"
RESEARCH_SHORTLIST = 3
AUTO_PROMOTE_DEPLOYABLE = True


# ── Helpers ───────────────────────────────────────────────────────────────────

def _header(title: str) -> None:
    print()
    print("=" * 70)
    print(f"  {title}")
    print("=" * 70)


def _pick_asset() -> tuple:
    assets = [
        ("BTC-USD", "crypto"),
        ("ETH-USD", "crypto"),
        ("SOL-USD", "crypto"),
        ("XRP-USD", "crypto"),
        ("BNB-USD", "crypto"),
        ("EUR/USD", "forex"),
        ("EUR/JPY", "forex"),
        ("GBP/USD", "forex"),
        ("GBP/JPY", "forex"),
        ("USD/JPY", "forex"),
        ("AUD/USD", "forex"),
        ("USD/CAD", "forex"),
        ("XAU/USD", "commodities"),
        ("XAG/USD", "commodities"),
        ("US30", "indices"),
        ("US100", "indices"),
        ("US500", "indices"),
        ("UK100", "indices"),
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


def _lab_window(category: str, periods: int | None = None):
    from config.config import get_trading_timeframe
    from strategy_lab import resolve_backtest_end_time, resolve_backtest_periods

    timeframe = get_trading_timeframe(category)
    resolved_periods = resolve_backtest_periods(category, periods)
    snapshot_end = resolve_backtest_end_time(category)
    return timeframe, resolved_periods, snapshot_end


def _research_settings(profile: str = SCREENING_RESEARCH_PROFILE) -> dict:
    from strategy_lab import resolve_research_profile

    return resolve_research_profile(profile)


def _sort_key(item) -> tuple:
    result = item[2]
    trades = int(getattr(result, "total_trades", 0) or 0)
    return (
        int(trades > 0),
        float(getattr(result, "sharpe_ratio", 0.0) or 0.0),
        float(getattr(result, "total_pnl", 0.0) or 0.0),
        -float(getattr(result, "max_drawdown", 0.0) or 0.0),
        float(getattr(result, "win_rate", 0.0) or 0.0),
        trades,
    )


def _research_label(report: dict | None) -> str:
    if not report:
        return "Research=—"
    verdict = str(report.get("verdict", "unknown")).upper()
    return f"Research={float(report.get('overall_score', 0.0)):4.0f} {verdict}"


def _print_result(name: str, result, report: dict | None = None) -> None:
    print(
        f"  {name:35}  "
        f"Sharpe={result.sharpe_ratio:+6.2f}  "
        f"WinRate={result.win_rate:5.1%}  "
        f"PnL={result.total_pnl:+8.2f}  "
        f"MaxDD={result.max_drawdown:5.1%}  "
        f"Trades={result.total_trades:3}  "
        f"{_research_label(report)}"
    )


def _print_research_report(report: dict) -> None:
    base = report.get("base_metrics", {})
    monte = report.get("bootstrap_monte_carlo", {})
    walk = report.get("walk_forward_validation", {})
    stress = report.get("stress_testing", {})
    sens = report.get("sensitivity_analysis", {})
    psr = report.get("probabilistic_sharpe", {})
    cost = report.get("transaction_cost_impact", {})
    regime = report.get("regime_analysis", {})
    cross = report.get("cross_asset_validation", {})

    print(f"    Overall robustness : {float(report.get('overall_score', 0.0)):5.1f} / 100  ({report.get('verdict', 'unknown')})")
    print(f"    Research depth     : {str(report.get('research_profile', SCREENING_RESEARCH_PROFILE)).upper()}")
    print(
        f"    Trade sufficiency  : {int(base.get('total_trades', 0))} trades  "
        f"(score={float(report.get('trade_sufficiency_score', 0.0)):5.1f}, "
        f"minimum={int(report.get('minimum_trades_required', 0) or 0)})"
    )
    print(f"    Base Sharpe        : {float(base.get('sharpe_ratio', 0.0)):+6.2f}")
    print(f"    Base PnL           : {float(base.get('total_pnl', 0.0)):+8.2f}")
    print(f"    Base MaxDD         : {float(base.get('max_drawdown', 0.0)):6.1%}")
    if monte.get("insufficient_data"):
        print("    Bootstrap Monte    : insufficient closed-trade count")
    else:
        print(
            f"    Bootstrap Monte    : profit={float(monte.get('profit_probability', 0.0)):6.1%}  "
            f"DD P95={float(monte.get('max_drawdown_p95', 0.0)):6.1%}"
        )
    print(
        f"    Walk-Forward       : folds={int(walk.get('fold_count', 0))}  "
        f"pass={float(walk.get('pass_rate', 0.0)):6.1%}  "
        f"testSharpe={float(walk.get('avg_test_sharpe', 0.0)):+6.2f}"
    )
    print(
        f"    Stress             : score={float(stress.get('resilience_score', 0.0)):5.1f}  "
        f"worst={stress.get('worst_case_scenario', 'n/a')}"
    )
    critical = sens.get("critical_parameters", []) or []
    print(
        f"    Sensitivity        : score={float(sens.get('sensitivity_score', 0.0)):5.1f}  "
        f"critical={', '.join(critical) if critical else 'n/a'}"
    )
    print(
        f"    Probabilistic SR   : P(SR>0)={float(psr.get('probability_sharpe_positive', 0.0)):6.1%}  "
        f"confidence={psr.get('confidence_label', 'n/a')}"
    )
    print(
        f"    Cost Impact        : score={float(cost.get('cost_resilience_score', 0.0)):5.1f}  "
        f"breakEven≈x{float(cost.get('break_even_cost_multiplier', 0.0) or 0.0):.1f}"
    )
    print(
        f"    Regime Analysis    : score={float(regime.get('regime_balance_score', 0.0)):5.1f}  "
        f"best={regime.get('best_regime', 'n/a')}  worst={regime.get('worst_regime', 'n/a')}"
    )
    if cross and not bool(cross.get("insufficient_data")):
        print(
            f"    Cross-Asset        : score={float(cross.get('consistency_score', 0.0)):5.1f}  "
            f"positive={float(cross.get('positive_asset_rate', 0.0)):6.1%}  "
            f"best={cross.get('best_asset', 'n/a')}"
        )


def _run_research(
    config: dict,
    asset: str,
    category: str,
    periods: int | None = None,
    end_time=None,
    profile: str = SCREENING_RESEARCH_PROFILE,
) -> dict:
    from strategy_lab import run_robustness_analysis

    settings = _research_settings(profile)
    return run_robustness_analysis(
        strategy_config=config,
        asset=asset,
        category=category,
        periods=periods,
        end_time=end_time,
        research_profile=str(settings.get("profile", profile)),
        monte_carlo_iterations=int(settings.get("monte_carlo_iterations", 80) or 80),
        max_walk_forward_folds=int(settings.get("max_walk_forward_folds", 3) or 3),
        max_sensitivity_params=int(settings.get("max_sensitivity_params", 3) or 3),
        include_cross_asset_validation=bool(settings.get("include_cross_asset_validation", False)),
        max_cross_asset_peers=int(settings.get("max_cross_asset_peers", 0) or 0),
    )


def _is_research_acceptable(report: dict | None) -> bool:
    if not report:
        return False
    if bool(report.get("insufficient_data")):
        return False
    return str(report.get("verdict", "")).lower() in {"mixed", "robust"} and float(report.get("overall_score", 0.0) or 0.0) >= 55.0


def _auto_promote_live_candidate(config: dict, report: dict | None, asset: str, category: str) -> bool:
    if not AUTO_PROMOTE_DEPLOYABLE or not _is_research_acceptable(report):
        return False
    try:
        from strategy_lab.live_bridge import LIVE_STRATEGY_REGISTRY_PATH, promote_strategy_config

        entry = promote_strategy_config(
            config,
            report=report,
            asset=asset,
            category=category,
            source="run_lab",
        )
        print()
        print(
            f"  Auto-promoted to live registry: {entry.get('name', config.get('name', 'unknown'))}  "
            f"({LIVE_STRATEGY_REGISTRY_PATH})"
        )
        print("  Live bot instances will load this strategy from the registry automatically.")
        return True
    except Exception as e:
        print()
        print(f"  Live auto-promotion skipped: {e}")
        return False


def _upgrade_to_final_research(
    label: str,
    config: dict,
    report: dict | None,
    asset: str,
    category: str,
    periods: int | None = None,
    end_time=None,
    result=None,
) -> dict | None:
    if not report:
        return report
    current_profile = str(report.get("research_profile", SCREENING_RESEARCH_PROFILE)).strip().lower()
    if current_profile == FINAL_RESEARCH_PROFILE:
        return report
    if not _is_research_acceptable(report):
        return report

    print()
    print(f"  Deep validation on final candidate: {label} ...")
    deep_report = _run_research(
        config,
        asset,
        category,
        periods=periods,
        end_time=end_time,
        profile=FINAL_RESEARCH_PROFILE,
    )
    if result is not None:
        _print_result(label, result, deep_report)
    return deep_report


def _research_shortlist(
    candidates: list[tuple[str, dict, object]],
    asset: str,
    category: str,
    periods: int | None = None,
    end_time=None,
    profile: str = SCREENING_RESEARCH_PROFILE,
) -> list[dict]:
    ranked = sorted(candidates, key=_sort_key, reverse=True)
    tradable = [item for item in ranked if int(getattr(item[2], "total_trades", 0) or 0) > 0]
    shortlist = (tradable if tradable else ranked)[:RESEARCH_SHORTLIST]
    if not shortlist:
        return []

    print()
    print(f"  Research shortlist on {asset} ({category})  [{profile.upper()}]")
    print("  " + "-" * 66)
    research_rows = []
    for idx, (name, config, result) in enumerate(shortlist, 1):
        print(f"  [{idx}/{len(shortlist)}] Researching {name} ...")
        report = _run_research(config, asset, category, periods=periods, end_time=end_time, profile=profile)
        research_rows.append({
            "name": name,
            "config": config,
            "result": result,
            "report": report,
        })
        _print_result(name, result, report)
    return research_rows


def _best_by_research(rows: list[dict]) -> dict | None:
    if not rows:
        return None
    eligible = [row for row in rows if not bool(row["report"].get("insufficient_data"))]
    pool = eligible or []
    if not pool:
        return None
    return max(
        pool,
        key=lambda row: (
            float(row["report"].get("overall_score", 0.0) or 0.0),
            float(row["result"].sharpe_ratio or 0.0),
            float(row["result"].total_pnl or 0.0),
        ),
    )


# ── Option 1 — All presets on one asset ───────────────────────────────────────

def run_all_presets() -> None:
    from strategy_lab import run_backtest, StrategyBuilder

    _header("ALL 15 PRESET STRATEGIES")
    asset, category = _pick_asset()
    timeframe, periods, snapshot_end = _lab_window(category)
    print(
        f"\n  Running 15 backtests on {asset} ({category})"
        f" using {periods} closed {timeframe} bars through {snapshot_end.strftime('%Y-%m-%d %H:%M UTC')} — please wait...\n"
    )

    candidates: list[tuple[str, dict, object]] = []
    for name, config in StrategyBuilder.all_configs().items():
        try:
            result = run_backtest(config, asset, category, periods=periods, end_time=snapshot_end)
            candidates.append((name, config, result))
            _print_result(name, result)
        except Exception as e:
            print(f"  {name:35}  ERROR: {e}")

    research_rows = _research_shortlist(
        candidates,
        asset,
        category,
        periods=periods,
        end_time=snapshot_end,
        profile=SCREENING_RESEARCH_PROFILE,
    )
    best = _best_by_research(research_rows)
    if best:
        best["report"] = _upgrade_to_final_research(
            best["name"],
            best["config"],
            best["report"],
            asset,
            category,
            periods=periods,
            end_time=snapshot_end,
            result=best["result"],
        )
    if best and _is_research_acceptable(best["report"]):
        print()
        print(
            f"  Research winner: {best['name']}  "
            f"(score={float(best['report'].get('overall_score', 0.0)):4.1f}, "
            f"verdict={best['report'].get('verdict', 'unknown')})"
        )
        _auto_promote_live_candidate(best["config"], best["report"], asset, category)
    elif best:
        print()
        print(
            f"  Top research candidate is still not deployable: {best['name']}  "
            f"(score={float(best['report'].get('overall_score', 0.0)):4.1f}, "
            f"verdict={best['report'].get('verdict', 'unknown')})"
        )
    elif research_rows:
        print()
        print("  No research-valid winner: shortlisted strategies did not meet the minimum trade evidence requirement.")


# ── Option 2 — Lab presets vs existing strategies ─────────────────────────────

def compare_all() -> None:
    from strategy_lab import run_backtest, StrategyBuilder
    from strategy_lab.strategy_adapter import compare_all_strategies_from_asset

    _header("LAB PRESETS vs YOUR EXISTING STRATEGIES")
    asset, category = _pick_asset()
    timeframe, periods, snapshot_end = _lab_window(category)
    print(
        f"\n  Running on {asset} ({category})"
        f" using {periods} closed {timeframe} bars through {snapshot_end.strftime('%Y-%m-%d %H:%M UTC')} — please wait...\n"
    )

    print("  LAB PRESETS")
    print("  " + "-" * 66)
    candidates: list[tuple[str, dict, object]] = []
    for name, config in StrategyBuilder.all_configs().items():
        try:
            result = run_backtest(config, asset, category, periods=periods, end_time=snapshot_end)
            candidates.append((name, config, result))
            _print_result(name, result)
        except Exception as e:
            print(f"  {name:35}  ERROR: {e}")

    research_rows = _research_shortlist(
        candidates,
        asset,
        category,
        periods=periods,
        end_time=snapshot_end,
        profile=SCREENING_RESEARCH_PROFILE,
    )

    print()
    print("  YOUR EXISTING STRATEGIES")
    print("  " + "-" * 66)
    try:
        existing = compare_all_strategies_from_asset(asset, category, periods=periods, end_time=snapshot_end)
        for r in existing:
            print(
                f"  {r['label']:35}  "
                f"Sharpe={r['sharpe']:+6.2f}  "
                f"WinRate={r['win_rate']:5.1%}  "
                f"PnL={r['total_pnl']:+8.2f}  "
                f"MaxDD={r['max_drawdown']:5.1%}  "
                f"Trades={r['trades']:3}  "
                f"Research=legacy"
            )
    except Exception as e:
        print(f"  Could not run existing strategies: {e}")

    best = _best_by_research(research_rows)
    if best:
        best["report"] = _upgrade_to_final_research(
            best["name"],
            best["config"],
            best["report"],
            asset,
            category,
            periods=periods,
            end_time=snapshot_end,
            result=best["result"],
        )
    if best and _is_research_acceptable(best["report"]):
        print()
        print(
            f"  Best research-validated lab strategy: {best['name']}  "
            f"(score={float(best['report'].get('overall_score', 0.0)):4.1f})"
        )
        _auto_promote_live_candidate(best["config"], best["report"], asset, category)
    elif best:
        print()
        print(
            f"  Top researched lab strategy remains non-deployable: {best['name']}  "
            f"(score={float(best['report'].get('overall_score', 0.0)):4.1f}, "
            f"verdict={best['report'].get('verdict', 'unknown')})"
        )
    elif research_rows:
        print()
        print("  No research-validated lab strategy: shortlisted candidates lacked enough trade evidence.")


# ── Option 3 — Parameter optimiser ───────────────────────────────────────────

def optimise_strategy() -> None:
    from strategy_lab import optimize_strategy, StrategyBuilder
    from strategy_lab.parameter_optimizer import ParameterOptimizer

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

    chosen_name = strategies[choice - 1]
    chosen_config = StrategyBuilder.all_configs()[chosen_name]
    asset, category = _pick_asset()
    timeframe, periods, snapshot_end = _lab_window(category)

    print(
        f"\n  Optimising {chosen_name} on {asset} ({category})"
        f" using {periods} closed {timeframe} bars through {snapshot_end.strftime('%Y-%m-%d %H:%M UTC')}..."
    )
    print("  Grid: rsi_period=[10,14,21]  stop_mult=[1.0,1.5,2.0]  tp_mult=[2.0,3.0,4.0]\n")

    try:
        results = optimize_strategy(
            base_config=chosen_config,
            param_grid={
                "rsi_period": [10, 14, 21],
                "stop_mult": [1.0, 1.5, 2.0],
                "tp_mult": [2.0, 3.0, 4.0],
            },
            asset=asset,
            category=category,
            periods=periods,
            end_time=snapshot_end,
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

        print()
        print(f"  Research pass on top parameter sets  [{SCREENING_RESEARCH_PROFILE.upper()}]")
        print("  " + "-" * 66)
        research_rows = []
        for rank, row in enumerate(results[:RESEARCH_SHORTLIST], 1):
            params = {k: row[k] for k in ("rsi_period", "stop_mult", "tp_mult") if k in row}
            config = ParameterOptimizer._apply_params(copy.deepcopy(chosen_config), params)
            report = _run_research(
                config,
                asset,
                category,
                periods=periods,
                end_time=snapshot_end,
                profile=SCREENING_RESEARCH_PROFILE,
            )
            research_rows.append({"params": params, "row": row, "report": report, "config": config})
            print(
                f"  #{rank} params={params}  "
                f"Sharpe={float(row.get('sharpe', 0.0)):+6.2f}  "
                f"{_research_label(report)}"
            )

        if research_rows:
            best = max(
                [row for row in research_rows if not bool(row["report"].get("insufficient_data"))] or research_rows,
                key=lambda item: (
                    float(item["report"].get("overall_score", 0.0) or 0.0),
                    float(item["row"].get("sharpe", 0.0) or 0.0),
                    float(item["row"].get("total_pnl", 0.0) or 0.0),
                ),
            )
            best["report"] = _upgrade_to_final_research(
                str(best["params"]),
                best["config"],
                best["report"],
                asset,
                category,
                periods=periods,
                end_time=snapshot_end,
            )
            if best["report"].get("insufficient_data"):
                print()
                print("  No research-valid parameter set yet: the tested leaders still lack enough trade evidence.")
            elif not _is_research_acceptable(best["report"]):
                print()
                print(
                    f"  Top researched parameter set remains non-deployable: {best['params']}  "
                    f"(score={float(best['report'].get('overall_score', 0.0)):4.1f}, "
                    f"verdict={best['report'].get('verdict', 'unknown')})"
                )
            else:
                print()
                print(
                    f"  Recommended params: {best['params']}  "
                    f"(score={float(best['report'].get('overall_score', 0.0)):4.1f}, "
                    f"verdict={best['report'].get('verdict', 'unknown')})"
                )
                _auto_promote_live_candidate(best["config"], best["report"], asset, category)
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

    chosen_name = strategies[choice - 1]
    chosen_config = StrategyBuilder.all_configs()[chosen_name]

    test_assets = [
        ("BTC-USD", "crypto"),
        ("ETH-USD", "crypto"),
        ("SOL-USD", "crypto"),
        ("EUR/USD", "forex"),
        ("GBP/USD", "forex"),
        ("USD/JPY", "forex"),
        ("XAU/USD", "commodities"),
        ("US30", "indices"),
    ]

    print(f"\n  Testing {chosen_name} across {len(test_assets)} assets with category-aware research windows...\n")

    candidates: list[tuple[str, dict, object]] = []
    asset_map: dict[str, str] = {}
    asset_windows: dict[str, tuple[int, object]] = {}
    for asset, category in test_assets:
        try:
            timeframe, periods, snapshot_end = _lab_window(category)
            result = run_backtest(chosen_config, asset, category, periods=periods, end_time=snapshot_end)
            candidates.append((asset, chosen_config, result))
            asset_map[asset] = category
            asset_windows[asset] = (periods, snapshot_end)
            _print_result(asset, result)
        except Exception as e:
            print(f"  {asset:35}  ERROR: {e}")

    print()
    print(f"  Research pass on top assets  [{SCREENING_RESEARCH_PROFILE.upper()}]")
    print("  " + "-" * 66)
    research_rows = []
    for idx, (asset, config, result) in enumerate(sorted(candidates, key=_sort_key, reverse=True)[:RESEARCH_SHORTLIST], 1):
        category = asset_map[asset]
        periods, snapshot_end = asset_windows[asset]
        report = _run_research(
            config,
            asset,
            category,
            periods=periods,
            end_time=snapshot_end,
            profile=SCREENING_RESEARCH_PROFILE,
        )
        research_rows.append({"asset": asset, "result": result, "report": report, "config": config})
        print(
            f"  [{idx}/{min(len(candidates), RESEARCH_SHORTLIST)}] {asset:12}  "
            f"Sharpe={result.sharpe_ratio:+6.2f}  {_research_label(report)}"
        )

    if research_rows:
        best = max(
            [row for row in research_rows if not bool(row["report"].get("insufficient_data"))] or research_rows,
            key=lambda row: (
                float(row["report"].get("overall_score", 0.0) or 0.0),
                float(row["result"].sharpe_ratio or 0.0),
            ),
        )
        best_periods, best_end_time = asset_windows[best["asset"]]
        best["report"] = _upgrade_to_final_research(
            best["asset"],
            best["config"],
            best["report"],
            best["asset"],
            asset_map.get(best["asset"], ""),
            periods=best_periods,
            end_time=best_end_time,
            result=best["result"],
        )
        if best["report"].get("insufficient_data"):
            print()
            print("  No research-valid asset winner yet: top assets still lack enough trade evidence.")
        elif not _is_research_acceptable(best["report"]):
            print()
            print(
                f"  Top researched asset remains non-deployable: {best['asset']}  "
                f"(score={float(best['report'].get('overall_score', 0.0)):4.1f}, "
                f"verdict={best['report'].get('verdict', 'unknown')})"
            )
        else:
            print()
            print(
                f"  Best research-validated asset: {best['asset']}  "
                f"(score={float(best['report'].get('overall_score', 0.0)):4.1f})"
            )
            _auto_promote_live_candidate(best["config"], best["report"], best["asset"], asset_map.get(best["asset"], ""))


# ── Option 5 — Full report ────────────────────────────────────────────────────

def full_report() -> None:
    from strategy_lab import run_backtest, StrategyBuilder

    _header("FULL REPORT — ALL PRESETS ON ALL YOUR ASSETS")

    assets = [
        ("BTC-USD", "crypto"),
        ("ETH-USD", "crypto"),
        ("EUR/JPY", "forex"),
        ("EUR/USD", "forex"),
        ("GBP/USD", "forex"),
        ("GBP/JPY", "forex"),
        ("XAU/USD", "commodities"),
        ("US30", "indices"),
        ("US500", "indices"),
    ]

    configs = StrategyBuilder.all_configs()
    best_overall = []

    for asset, category in assets:
        timeframe, periods, snapshot_end = _lab_window(category)
        print(f"\n  {asset} ({category})")
        print("  " + "-" * 66)
        print(f"  Window: {periods} closed {timeframe} bars through {snapshot_end.strftime('%Y-%m-%d %H:%M UTC')}")
        asset_results: list[tuple[str, dict, object]] = []
        for name, config in configs.items():
            try:
                result = run_backtest(config, asset, category, periods=periods, end_time=snapshot_end)
                asset_results.append((name, config, result))
                _print_result(name, result)
            except Exception as e:
                print(f"  {name:35}  ERROR: {e}")
        if asset_results:
            research_rows = _research_shortlist(
                asset_results,
                asset,
                category,
                periods=periods,
                end_time=snapshot_end,
                profile=SCREENING_RESEARCH_PROFILE,
            )
            best = _best_by_research(research_rows)
            if best:
                best["report"] = _upgrade_to_final_research(
                    best["name"],
                    best["config"],
                    best["report"],
                    asset,
                    category,
                    periods=periods,
                    end_time=snapshot_end,
                    result=best["result"],
                )
                best_overall.append({
                    "asset": asset,
                    "name": best["name"],
                    "report": best["report"],
                    "deployable": _is_research_acceptable(best["report"]),
                })
                status = "Best deployable" if _is_research_acceptable(best["report"]) else "Top non-deployable"
                print(
                    f"\n  → {status} for {asset}: {best['name']}  "
                    f"(score={float(best['report'].get('overall_score', 0.0)):4.1f}, "
                    f"verdict={best['report'].get('verdict', 'unknown')})"
                )
            else:
                print(f"\n  → No research-valid candidate for {asset}")

    if best_overall:
        print()
        _header("SUMMARY — BEST STRATEGY PER ASSET")
        for row in best_overall:
            asset = row["asset"]
            name = row["name"]
            report = row["report"]
            prefix = "Deployable" if row["deployable"] else "Watchlist"
            print(
                f"  {asset:12}  →  {name:35}  "
                f"Research={float(report.get('overall_score', 0.0)):4.1f}  "
                f"{report.get('verdict', 'unknown')}  {prefix}"
            )


# ── Option 6 — Custom strategy ────────────────────────────────────────────────

def custom_strategy() -> None:
    from strategy_lab import run_backtest

    _header("CUSTOM STRATEGY BACKTEST")

    print("""
  Edit the config below in run_lab.py then re-run.

  Supported indicators: rsi, ema, macd, bollinger, atr, volume_ma, stoch, adx
  Supported operators:  >  <  >=  <=  cross_above  cross_below
    """)

    custom_config = {
        "name": "my_custom_strategy",
        "version": "1.0",
        "indicators": [
            {"name": "stoch", "params": {"k_period": 14, "d_period": 3}},
            {"name": "ema", "params": {"period": 50}},
            {"name": "atr", "params": {"period": 14}},
        ],
        "entry_rules": [
            {"col": "stoch_k", "op": "cross_above", "col2": "stoch_d", "direction": "BUY"},
            {"col": "stoch_k", "op": "<", "val": 50},
            {"col": "close", "op": ">", "col2": "ema_50"},
        ],
        "confidence_boosts": [
            {"col": "stoch_k", "below": 30, "boost": 0.07},
        ],
        "stop_mult": 1.5,
        "tp_mult": 2.5,
    }

    asset, category = _pick_asset()
    timeframe, periods, snapshot_end = _lab_window(category)
    print(
        f"\n  Running {custom_config['name']} on {asset}"
        f" using {periods} closed {timeframe} bars through {snapshot_end.strftime('%Y-%m-%d %H:%M UTC')}...\n"
    )

    try:
        result = run_backtest(custom_config, asset, category, periods=periods, end_time=snapshot_end)
        report = _run_research(
            custom_config,
            asset,
            category,
            periods=periods,
            end_time=snapshot_end,
            profile=SCREENING_RESEARCH_PROFILE,
        )
        report = _upgrade_to_final_research(
            custom_config["name"],
            custom_config,
            report,
            asset,
            category,
            periods=periods,
            end_time=snapshot_end,
        )
        _print_result(custom_config["name"], result, report)
        print()
        print("  Detailed stats:")
        print(f"    Profit factor : {result.profit_factor:.2f}")
        print(f"    Expectancy    : ${result.expectancy:.2f} per trade")
        print(f"    Largest win   : ${result.largest_win:.2f}")
        print(f"    Largest loss  : ${result.largest_loss:.2f}")
        print(f"    Avg win       : ${result.avg_win:.2f}")
        print(f"    Avg loss      : ${result.avg_loss:.2f}")
        print()
        _print_research_report(report)
        _auto_promote_live_candidate(custom_config, report, asset, category)
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
        "1": ("Backtest all 15 presets on one asset", run_all_presets),
        "2": ("Compare presets vs your existing strategies", compare_all),
        "3": ("Optimise parameters for a strategy", optimise_strategy),
        "4": ("Test one strategy across multiple assets", multi_asset_test),
        "5": ("Full report — all presets on all your assets", full_report),
        "6": ("Backtest a custom strategy config", custom_strategy),
        "0": ("Exit", None),
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
