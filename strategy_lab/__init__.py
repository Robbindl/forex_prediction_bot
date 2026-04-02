from __future__ import annotations

import sys
from datetime import datetime, timezone
import numpy as np

from strategy_lab.strategy_builder    import StrategyBuilder, DynamicStrategy
from strategy_lab.backtest_engine_v2  import BacktestEngineV2, BacktestResult
from strategy_lab.parameter_optimizer import ParameterOptimizer
from strategy_lab.performance_analyzer import PerformanceAnalyzer
from strategy_lab.robustness_analyzer import RobustnessAnalyzer

from utils.logger import get_logger

logger = get_logger()

RESEARCH_DEPTH_PRESETS = {
    "fast": {
        "monte_carlo_iterations": 30,
        "max_walk_forward_folds": 2,
        "max_sensitivity_params": 2,
        "include_cross_asset_validation": False,
        "max_cross_asset_peers": 0,
    },
    "standard": {
        "monte_carlo_iterations": 80,
        "max_walk_forward_folds": 3,
        "max_sensitivity_params": 3,
        "include_cross_asset_validation": False,
        "max_cross_asset_peers": 0,
    },
    "deep": {
        "monte_carlo_iterations": 160,
        "max_walk_forward_folds": 4,
        "max_sensitivity_params": 4,
        "include_cross_asset_validation": True,
        "max_cross_asset_peers": 4,
    },
}

_CATEGORY_ASSETS = {
    "crypto": ["BTC-USD", "ETH-USD", "SOL-USD", "XRP-USD", "BNB-USD"],
    "forex": ["EUR/USD", "EUR/JPY", "GBP/USD", "GBP/JPY", "USD/JPY", "AUD/USD", "USD/CAD"],
    "commodities": ["XAU/USD", "XAG/USD"],
    "indices": ["US30", "US100", "US500", "UK100"],
}

_INTERVAL_SECONDS = {
    "1m": 60,
    "5m": 300,
    "15m": 900,
    "30m": 1800,
    "1h": 3600,
    "4h": 14400,
    "1d": 86400,
}


def _parse_utc_datetime(value) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        if isinstance(value, datetime):
            return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
        text = str(value).strip()
        if not text:
            return None
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return parsed.astimezone(timezone.utc) if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def resolve_backtest_periods(category: str, periods: int | None = None) -> int:
    from config.config import get_research_timeframe_periods, get_trading_timeframe

    if periods not in (None, "", 0):
        return max(2, int(periods))
    timeframe = get_trading_timeframe(category)
    return max(2, int(get_research_timeframe_periods(timeframe, category)))


def resolve_backtest_end_time(category: str, end_time=None) -> datetime:
    from config.config import get_trading_timeframe

    timeframe = get_trading_timeframe(category)
    granularity = _INTERVAL_SECONDS.get(str(timeframe).lower(), 900)
    anchor = _parse_utc_datetime(end_time) or datetime.now(timezone.utc)
    epoch = int(anchor.timestamp())
    aligned = epoch - (epoch % granularity)
    return datetime.fromtimestamp(aligned, tz=timezone.utc)


def resolve_research_profile(
    profile: str | None = None,
    *,
    monte_carlo_iterations: int | None = None,
    max_walk_forward_folds: int | None = None,
    max_sensitivity_params: int | None = None,
    include_cross_asset_validation: bool | None = None,
    max_cross_asset_peers: int | None = None,
) -> dict:
    name = str(profile or "standard").strip().lower()
    if name not in RESEARCH_DEPTH_PRESETS:
        name = "standard"
    resolved = dict(RESEARCH_DEPTH_PRESETS[name])
    if monte_carlo_iterations is not None:
        resolved["monte_carlo_iterations"] = max(10, int(monte_carlo_iterations))
    if max_walk_forward_folds is not None:
        resolved["max_walk_forward_folds"] = max(1, int(max_walk_forward_folds))
    if max_sensitivity_params is not None:
        resolved["max_sensitivity_params"] = max(1, int(max_sensitivity_params))
    if include_cross_asset_validation is not None:
        resolved["include_cross_asset_validation"] = bool(include_cross_asset_validation)
    if max_cross_asset_peers is not None:
        resolved["max_cross_asset_peers"] = max(0, int(max_cross_asset_peers))
    resolved["profile"] = name
    return resolved


def _resolve_fetcher():
    eng_mod = sys.modules.get("core.engine")
    if eng_mod is not None:
        fetcher = getattr(getattr(eng_mod, "_CORE_INSTANCE", None), "fetcher", None)
        if fetcher is not None:
            return fetcher
    from data.fetcher import get_shared_fetcher

    return get_shared_fetcher()


def _cross_asset_candidates(asset: str, category: str, max_peers: int = 4) -> list[str]:
    universe = list(_CATEGORY_ASSETS.get(str(category or "").lower(), []))
    ordered = [asset] + [item for item in universe if item != asset]
    return ordered[: max(1, max_peers + 1)]


def _cross_asset_validation(
    strategy_config: dict,
    asset: str,
    category: str,
    *,
    initial_balance: float,
    periods: int,
    end_time,
    base_result: BacktestResult,
    max_peers: int = 4,
) -> dict:
    assets = _cross_asset_candidates(asset, category, max_peers=max_peers)
    rows = [
        {
            "asset": asset,
            "category": category,
            "sharpe_ratio": round(base_result.sharpe_ratio, 4),
            "total_pnl": round(base_result.total_pnl, 2),
            "win_rate": round(base_result.win_rate, 4),
            "max_drawdown": round(base_result.max_drawdown, 4),
            "trades": int(base_result.total_trades or 0),
        }
    ]
    for peer in assets[1:]:
        try:
            result = run_backtest(
                strategy_config=strategy_config,
                asset=peer,
                category=category,
                initial_balance=initial_balance,
                periods=periods,
                end_time=end_time,
            )
            rows.append(
                {
                    "asset": peer,
                    "category": category,
                    "sharpe_ratio": round(result.sharpe_ratio, 4),
                    "total_pnl": round(result.total_pnl, 2),
                    "win_rate": round(result.win_rate, 4),
                    "max_drawdown": round(result.max_drawdown, 4),
                    "trades": int(result.total_trades or 0),
                }
            )
        except Exception as exc:
            logger.debug(f"[StrategyLab] cross-asset skip {peer}: {exc}")

    if not rows:
        return {
            "evaluated_assets": 0,
            "best_asset": "",
            "worst_asset": "",
            "positive_asset_rate": 0.0,
            "tradable_asset_rate": 0.0,
            "consistency_score": 0.0,
            "assets": [],
            "insufficient_data": True,
        }

    positive_asset_rate = sum(1 for row in rows if row["total_pnl"] > 0 and row["sharpe_ratio"] > 0) / max(len(rows), 1)
    tradable_asset_rate = sum(1 for row in rows if row["trades"] > 0) / max(len(rows), 1)
    sharpes = [float(row["sharpe_ratio"]) for row in rows]
    median_sharpe = float(np.median(np.array(sharpes, dtype=float))) if sharpes else 0.0
    sharpe_dispersion = (
        float(np.mean(np.abs(np.array(sharpes, dtype=float) - median_sharpe)))
        if len(sharpes) > 1
        else 0.0
    )
    dispersion_score = max(0.0, 1.0 - min(1.0, sharpe_dispersion / 2.0))
    consistency_score = round((0.45 * positive_asset_rate + 0.30 * tradable_asset_rate + 0.25 * dispersion_score) * 100.0, 1)
    ranked = sorted(rows, key=lambda row: (row["sharpe_ratio"], row["total_pnl"], -row["max_drawdown"]), reverse=True)

    return {
        "evaluated_assets": len(rows),
        "best_asset": ranked[0]["asset"],
        "worst_asset": ranked[-1]["asset"],
        "positive_asset_rate": round(positive_asset_rate, 4),
        "tradable_asset_rate": round(tradable_asset_rate, 4),
        "median_sharpe": round(median_sharpe, 4),
        "sharpe_dispersion": round(sharpe_dispersion, 4),
        "consistency_score": consistency_score,
        "assets": rows,
        "insufficient_data": False,
    }


def _apply_extended_validation(report: dict) -> dict:
    extended_scores = [
        float((report.get("transaction_cost_impact") or {}).get("cost_resilience_score", 0.0) or 0.0),
        float((report.get("regime_analysis") or {}).get("regime_balance_score", 0.0) or 0.0),
    ]
    cross_asset = report.get("cross_asset_validation") or {}
    if cross_asset and not bool(cross_asset.get("insufficient_data")):
        extended_scores.append(float(cross_asset.get("consistency_score", 0.0) or 0.0))

    if not extended_scores:
        return report

    core_raw_score = float(report.get("raw_score", 0.0) or 0.0)
    trade_ratio = max(0.0, min(1.0, float(report.get("trade_sufficiency_score", 0.0) or 0.0) / 100.0))
    combined_raw = ((core_raw_score * 5.0) + sum(extended_scores)) / float(5 + len(extended_scores))
    report["core_raw_score"] = round(core_raw_score, 1)
    report["extended_validation_score"] = round(sum(extended_scores) / len(extended_scores), 1)
    report["raw_score"] = round(combined_raw, 1)
    report["overall_score"] = round(combined_raw * trade_ratio, 1)
    if not bool(report.get("insufficient_data")):
        report["verdict"] = "robust" if report["overall_score"] >= 72 else "mixed" if report["overall_score"] >= 55 else "fragile"
    return report


def run_backtest(
    strategy_config: dict,
    asset: str,
    category: str,
    initial_balance: float = 10_000.0,
    periods: int | None = None,
    end_time=None,
) -> "BacktestResult":
    """
    Convenience wrapper — fetch data and run a full backtest in one call.
    Uses the existing DataFetcher so all caching and API fallbacks apply.
    """
    try:
        fetcher = _resolve_fetcher()
        from config.config import get_trading_timeframe
        _TF = get_trading_timeframe(category)
        resolved_periods = resolve_backtest_periods(category, periods)
        resolved_end_time = resolve_backtest_end_time(category, end_time)
        df = fetcher.get_ohlcv(
            asset,
            category,
            _TF,
            resolved_periods,
            end_time=resolved_end_time,
            closed_only=True,
        )
        if df is None or df.empty:
            raise ValueError(f"No OHLCV data available for {asset}")
        strategy = StrategyBuilder.from_dict(strategy_config, asset=asset, category=category)
        engine   = BacktestEngineV2(
            strategy=strategy,
            initial_balance=initial_balance,
            asset=asset,
            category=category,
        )
        return engine.run(df)
    except Exception as e:
        logger.error(f"[StrategyLab] run_backtest failed: {e}", exc_info=True)
        raise


def optimize_strategy(
    base_config: dict,
    param_grid: dict,
    asset: str,
    category: str,
    initial_balance: float = 10_000.0,
    periods: int | None = None,
    end_time=None,
) -> list:
    """
    Convenience wrapper — fetch data and run a full parameter grid search.
    Returns results sorted by Sharpe ratio (best first).
    """
    try:
        fetcher = _resolve_fetcher()
        from config.config import get_trading_timeframe
        _TF = get_trading_timeframe(category)
        resolved_periods = resolve_backtest_periods(category, periods)
        resolved_end_time = resolve_backtest_end_time(category, end_time)
        df = fetcher.get_ohlcv(
            asset,
            category,
            _TF,
            resolved_periods,
            end_time=resolved_end_time,
            closed_only=True,
        )
        if df is None or df.empty:
            raise ValueError(f"No OHLCV data available for {asset}")
        optimizer = ParameterOptimizer(
            base_config=base_config,
            df=df,
            initial_balance=initial_balance,
            asset=asset,
            category=category,
        )
        return optimizer.grid_search(param_grid)
    except Exception as e:
        logger.error(f"[StrategyLab] optimize_strategy failed: {e}", exc_info=True)
        raise


def run_robustness_analysis(
    strategy_config: dict,
    asset: str,
    category: str,
    initial_balance: float = 10_000.0,
    periods: int | None = None,
    research_profile: str | None = None,
    monte_carlo_iterations: int | None = None,
    max_walk_forward_folds: int | None = None,
    max_sensitivity_params: int | None = None,
    include_cross_asset_validation: bool | None = None,
    max_cross_asset_peers: int | None = None,
    end_time=None,
) -> dict:
    """
    Run the full research/robustness suite on one strategy and asset.
    Reuses the same OHLCV source path as the rest of Strategy Lab.
    """
    try:
        settings = resolve_research_profile(
            research_profile,
            monte_carlo_iterations=monte_carlo_iterations,
            max_walk_forward_folds=max_walk_forward_folds,
            max_sensitivity_params=max_sensitivity_params,
            include_cross_asset_validation=include_cross_asset_validation,
            max_cross_asset_peers=max_cross_asset_peers,
        )
        fetcher = _resolve_fetcher()
        from config.config import get_trading_timeframe

        _TF = get_trading_timeframe(category)
        resolved_periods = resolve_backtest_periods(category, periods)
        resolved_end_time = resolve_backtest_end_time(category, end_time)
        df = fetcher.get_ohlcv(
            asset,
            category,
            _TF,
            resolved_periods,
            end_time=resolved_end_time,
            closed_only=True,
        )
        if df is None or df.empty:
            raise ValueError(f"No OHLCV data available for {asset}")

        strategy = StrategyBuilder.from_dict(strategy_config, asset=asset, category=category)
        engine = BacktestEngineV2(
            strategy=strategy,
            initial_balance=initial_balance,
            asset=asset,
            category=category,
        )
        base_result = engine.run(df)
        analyzer = RobustnessAnalyzer(
            strategy_config=strategy_config,
            df=df,
            initial_balance=initial_balance,
            base_result=base_result,
            asset=asset,
            category=category,
        )
        report = analyzer.analyze(
            monte_carlo_iterations=int(settings["monte_carlo_iterations"]),
            max_walk_forward_folds=int(settings["max_walk_forward_folds"]),
            max_sensitivity_params=int(settings["max_sensitivity_params"]),
        )
        report["research_profile"] = str(settings["profile"])
        if bool(settings.get("include_cross_asset_validation")) and int(settings.get("max_cross_asset_peers", 0) or 0) > 0:
            report["cross_asset_validation"] = _cross_asset_validation(
                strategy_config=strategy_config,
                asset=asset,
                category=category,
                initial_balance=initial_balance,
                periods=resolved_periods,
                end_time=resolved_end_time,
                base_result=base_result,
                max_peers=int(settings.get("max_cross_asset_peers", 0) or 0),
            )
        else:
            report["cross_asset_validation"] = {
                "evaluated_assets": 0,
                "best_asset": "",
                "worst_asset": "",
                "positive_asset_rate": 0.0,
                "tradable_asset_rate": 0.0,
                "consistency_score": 0.0,
                "assets": [],
                "insufficient_data": True,
            }
        return _apply_extended_validation(report)
    except Exception as e:
        logger.error(f"[StrategyLab] run_robustness_analysis failed: {e}", exc_info=True)
        raise


__all__ = [
    "StrategyBuilder", "DynamicStrategy",
    "BacktestEngineV2", "BacktestResult",
    "ParameterOptimizer", "PerformanceAnalyzer", "RobustnessAnalyzer",
    "resolve_backtest_periods", "resolve_backtest_end_time", "resolve_research_profile",
    "run_backtest", "optimize_strategy", "run_robustness_analysis",
]
