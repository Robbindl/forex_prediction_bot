from __future__ import annotations

import copy
import itertools
import math
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from strategy_lab.backtest_engine_v2 import (
    DEFAULT_COMMISSION,
    DEFAULT_RISK_PER_TRADE,
    DEFAULT_SLIPPAGE,
    BacktestEngineV2,
    BacktestResult,
    resolve_execution_profile,
)
from strategy_lab.parameter_optimizer import ParameterOptimizer
from strategy_lab.performance_analyzer import PerformanceAnalyzer, _annualisation_factor
from strategy_lab.strategy_builder import StrategyBuilder
from utils.logger import get_logger

logger = get_logger()
MIN_TRADES_REQUIRED = 20
FULL_TRADE_CONFIDENCE = 75
WALK_FORWARD_OPTIM_PARAMS = 2
WALK_FORWARD_MAX_COMBOS = 5
SENSITIVITY_INTERACTION_PARAMS = 3


def _safe_round(value: Any, digits: int = 4) -> float:
    try:
        if value is None or isinstance(value, str):
            return 0.0
        if isinstance(value, (float, np.floating)) and not np.isfinite(value):
            return 0.0
        return round(float(value), digits)
    except Exception:
        return 0.0


def _clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, float(value)))


def _lag1_autocorr(values: np.ndarray) -> float:
    arr = np.asarray(values, dtype=float)
    if len(arr) < 3:
        return 0.0
    x = arr[:-1]
    y = arr[1:]
    x_std = float(np.std(x))
    y_std = float(np.std(y))
    if x_std == 0.0 or y_std == 0.0:
        return 0.0
    corr = float(np.corrcoef(x, y)[0, 1])
    if not np.isfinite(corr):
        return 0.0
    return float(np.clip(corr, -0.99, 0.99))


class RobustnessAnalyzer:
    """
    Research suite for Strategy Lab.

    Produces:
    - Bootstrap Monte Carlo
    - Rolling walk-forward validation
    - Stress testing
    - Sensitivity analysis
    - Probabilistic Sharpe
    """

    def __init__(
        self,
        strategy_config: Dict[str, Any],
        df: pd.DataFrame,
        initial_balance: float = 10_000.0,
        base_result: Optional[BacktestResult] = None,
        asset: str = "",
        category: str = "",
    ) -> None:
        self.strategy_config = copy.deepcopy(strategy_config)
        self.df = BacktestEngineV2._prepare(df)
        self.initial_balance = float(initial_balance)
        self.base_result = base_result
        self.asset = asset
        self.category = category
        self.execution_profile = resolve_execution_profile(asset=asset, category=category)
        self._analyzer = PerformanceAnalyzer()

    def analyze(
        self,
        monte_carlo_iterations: int = 250,
        max_walk_forward_folds: int = 4,
        max_sensitivity_params: int = 5,
    ) -> Dict[str, Any]:
        if self.df is None or self.df.empty:
            raise ValueError("No OHLCV data available for robustness analysis")

        base = self.base_result or self._run_config(self.strategy_config, self.df)
        monte = self._bootstrap_monte_carlo(base, iterations=monte_carlo_iterations)
        walk = self._walk_forward_validation(max_folds=max_walk_forward_folds)
        stress = self._stress_test(base)
        sensitivity = self._sensitivity_analysis(base, max_params=max_sensitivity_params)
        psr = self._probabilistic_sharpe(base)
        cost_impact = self._transaction_cost_impact(base)
        regime_analysis = self._regime_analysis(base)
        trade_count = int(base.total_trades or 0)
        insufficient_data = trade_count < MIN_TRADES_REQUIRED
        trade_sufficiency_ratio = _clamp(trade_count / float(FULL_TRADE_CONFIDENCE), 0.0, 1.0)
        trade_sufficiency_score = round(trade_sufficiency_ratio * 100.0, 1)

        component_scores = [
            float(monte.get("stability_score", 0.0) or 0.0),
            float(walk.get("stability_score", 0.0) or 0.0),
            float(stress.get("resilience_score", 0.0) or 0.0),
            float(sensitivity.get("sensitivity_score", 0.0) or 0.0),
            float(psr.get("confidence_score", 0.0) or 0.0),
        ]
        raw_score = sum(component_scores) / max(len(component_scores), 1)
        overall_score = round(raw_score * trade_sufficiency_ratio, 1)
        if insufficient_data:
            verdict = "insufficient_data"
        else:
            verdict = "robust" if overall_score >= 72 else "mixed" if overall_score >= 55 else "fragile"

        return {
            "overall_score": overall_score,
            "raw_score": round(raw_score, 1),
            "verdict": verdict,
            "insufficient_data": insufficient_data,
            "minimum_trades_required": MIN_TRADES_REQUIRED,
            "trade_sufficiency_score": trade_sufficiency_score,
            "base_metrics": {
                "sharpe_ratio": _safe_round(base.sharpe_ratio, 4),
                "total_pnl": _safe_round(base.total_pnl, 2),
                "win_rate": _safe_round(base.win_rate, 4),
                "max_drawdown": _safe_round(base.max_drawdown, 4),
                "total_trades": trade_count,
            },
            "bootstrap_monte_carlo": monte,
            "walk_forward_validation": walk,
            "stress_testing": stress,
            "sensitivity_analysis": sensitivity,
            "probabilistic_sharpe": psr,
            "transaction_cost_impact": cost_impact,
            "regime_analysis": regime_analysis,
        }

    def _run_config(
        self,
        strategy_config: Dict[str, Any],
        df: pd.DataFrame,
        *,
        commission: Optional[float] = None,
        slippage: Optional[float] = None,
        risk_per_trade: Optional[float] = None,
    ) -> BacktestResult:
        strategy = StrategyBuilder.from_dict(copy.deepcopy(strategy_config), asset=self.asset, category=self.category)
        engine = BacktestEngineV2(
            strategy=strategy,
            initial_balance=self.initial_balance,
            asset=self.asset,
            category=self.category,
            commission=float(commission if commission is not None else self.execution_profile.get("commission", DEFAULT_COMMISSION)),
            slippage=float(slippage if slippage is not None else self.execution_profile.get("slippage", DEFAULT_SLIPPAGE)),
            risk_per_trade=float(risk_per_trade if risk_per_trade is not None else self.execution_profile.get("risk_per_trade", DEFAULT_RISK_PER_TRADE)),
        )
        return engine.run(df)

    def _bootstrap_monte_carlo(self, base: BacktestResult, iterations: int = 250) -> Dict[str, Any]:
        trades = list(base.trades or [])
        pnls = np.array([float(t.get("pnl", 0.0) or 0.0) for t in trades], dtype=float)
        if len(pnls) < 5:
            return {
                "iterations": int(iterations),
                "trade_count": int(len(pnls)),
                "block_size": 0,
                "serial_correlation_lag1": 0.0,
                "profit_probability": 0.0,
                "iid_profit_probability": 0.0,
                "block_profit_probability": 0.0,
                "median_pnl": 0.0,
                "pnl_p05": 0.0,
                "pnl_p50": 0.0,
                "pnl_p95": 0.0,
                "max_drawdown_p50": 0.0,
                "max_drawdown_p95": 0.0,
                "iid_max_drawdown_p95": 0.0,
                "block_max_drawdown_p95": 0.0,
                "ruin_probability": 0.0,
                "iid_ruin_probability": 0.0,
                "block_ruin_probability": 0.0,
                "path_dependency_score": 0.0,
                "stability_score": 0.0,
                "insufficient_data": True,
            }

        rng = np.random.default_rng(42)
        iid_pnl_samples: List[float] = []
        block_pnl_samples: List[float] = []
        iid_drawdown_samples: List[float] = []
        block_drawdown_samples: List[float] = []
        iid_final_balances: List[float] = []
        block_final_balances: List[float] = []
        block_size = max(2, min(5, int(round(math.sqrt(len(pnls))))))
        serial_correlation = _lag1_autocorr(pnls)

        for _ in range(iterations):
            iid_sample = rng.choice(pnls, size=len(pnls), replace=True)
            block_sample = self._block_bootstrap_sample(pnls, block_size=block_size, rng=rng)
            for sample, pnl_store, dd_store, balance_store in (
                (iid_sample, iid_pnl_samples, iid_drawdown_samples, iid_final_balances),
                (block_sample, block_pnl_samples, block_drawdown_samples, block_final_balances),
            ):
                balance = self.initial_balance
                equity = [balance]
                for pnl in sample:
                    balance += float(pnl)
                    equity.append(balance)
                balance_store.append(balance)
                pnl_store.append(balance - self.initial_balance)
                dd_store.append(self._analyzer._max_drawdown(equity))

        iid_profit_probability = float(np.mean(np.array(iid_final_balances) > self.initial_balance))
        block_profit_probability = float(np.mean(np.array(block_final_balances) > self.initial_balance))
        profit_probability = (iid_profit_probability + block_profit_probability) / 2.0
        iid_ruin_probability = float(np.mean(np.array(iid_final_balances) < self.initial_balance * 0.85))
        block_ruin_probability = float(np.mean(np.array(block_final_balances) < self.initial_balance * 0.85))
        ruin_probability = (iid_ruin_probability + block_ruin_probability) / 2.0
        iid_drawdown_p95 = float(np.percentile(iid_drawdown_samples, 95))
        block_drawdown_p95 = float(np.percentile(block_drawdown_samples, 95))
        drawdown_p95 = max(iid_drawdown_p95, block_drawdown_p95)
        path_dependency_gap = 0.5 * abs(iid_profit_probability - block_profit_probability) + 0.5 * abs(iid_drawdown_p95 - block_drawdown_p95)
        path_dependency_score = 1.0 - _clamp(path_dependency_gap, 0.0, 1.0)
        autocorr_score = 1.0 - _clamp(abs(serial_correlation), 0.0, 1.0)
        combined_pnl_samples = iid_pnl_samples + block_pnl_samples
        stability_score = round(
            (
                0.35 * profit_probability
                + 0.25 * (1.0 - _clamp(drawdown_p95, 0.0, 1.0))
                + 0.15 * (1.0 - ruin_probability)
                + 0.15 * path_dependency_score
                + 0.10 * autocorr_score
            )
            * 100.0,
            1,
        )

        return {
            "iterations": int(iterations),
            "trade_count": int(len(pnls)),
            "block_size": int(block_size),
            "serial_correlation_lag1": _safe_round(serial_correlation, 4),
            "profit_probability": _safe_round(profit_probability, 4),
            "iid_profit_probability": _safe_round(iid_profit_probability, 4),
            "block_profit_probability": _safe_round(block_profit_probability, 4),
            "median_pnl": _safe_round(np.median(combined_pnl_samples), 2),
            "pnl_p05": _safe_round(np.percentile(combined_pnl_samples, 5), 2),
            "pnl_p50": _safe_round(np.percentile(combined_pnl_samples, 50), 2),
            "pnl_p95": _safe_round(np.percentile(combined_pnl_samples, 95), 2),
            "max_drawdown_p50": _safe_round(np.percentile(block_drawdown_samples, 50), 4),
            "max_drawdown_p95": _safe_round(drawdown_p95, 4),
            "iid_max_drawdown_p95": _safe_round(iid_drawdown_p95, 4),
            "block_max_drawdown_p95": _safe_round(block_drawdown_p95, 4),
            "ruin_probability": _safe_round(ruin_probability, 4),
            "iid_ruin_probability": _safe_round(iid_ruin_probability, 4),
            "block_ruin_probability": _safe_round(block_ruin_probability, 4),
            "path_dependency_score": _safe_round(path_dependency_score * 100.0, 1),
            "stability_score": stability_score,
            "insufficient_data": False,
        }

    def _walk_forward_validation(self, max_folds: int = 4) -> Dict[str, Any]:
        n = len(self.df)
        if n < 160:
            return {
                "fold_count": 0,
                "avg_train_sharpe": 0.0,
                "avg_test_sharpe": 0.0,
                "avg_test_pnl": 0.0,
                "positive_test_rate": 0.0,
                "pass_rate": 0.0,
                "degradation_pct": 0.0,
                "parameter_consistency": 0.0,
                "baseline_outperformance_rate": 0.0,
                "stability_score": 0.0,
                "folds": [],
                "insufficient_data": True,
            }

        train_size = max(100, int(n * 0.5))
        test_size = max(40, int(n * 0.15))
        step = max(20, int(test_size * 0.75))
        start = 0
        folds: List[Dict[str, Any]] = []
        signature_counts: Dict[str, int] = {}
        baseline_beats: List[float] = []
        param_grid = self._walk_forward_param_grid(max_params=WALK_FORWARD_OPTIM_PARAMS)

        while start + train_size + test_size <= n and len(folds) < max_folds:
            train_df = self.df.iloc[start : start + train_size].reset_index(drop=True)
            test_df = self.df.iloc[start + train_size : start + train_size + test_size].reset_index(drop=True)
            train_base_result = self._run_config(self.strategy_config, train_df)
            best_params: Dict[str, Any] = {}
            optimized_config = copy.deepcopy(self.strategy_config)
            if param_grid:
                optimizer = ParameterOptimizer(
                    copy.deepcopy(self.strategy_config),
                    train_df,
                    initial_balance=self.initial_balance,
                    asset=self.asset,
                    category=self.category,
                )
                combo_count = 1
                for values in param_grid.values():
                    combo_count *= max(1, len(values))
                candidates = (
                    optimizer.grid_search(param_grid)
                    if combo_count <= WALK_FORWARD_MAX_COMBOS
                    else optimizer.random_search(param_grid, n_samples=WALK_FORWARD_MAX_COMBOS, seed=42 + len(folds))
                )
                best_row = self._select_best_optimization_row(candidates)
                if best_row:
                    best_params = {key: best_row[key] for key in param_grid.keys() if key in best_row}
                    optimized_config = ParameterOptimizer._apply_params(copy.deepcopy(self.strategy_config), best_params)
            train_result = self._run_config(optimized_config, train_df)
            test_result = self._run_config(optimized_config, test_df)
            baseline_test_result = self._run_config(self.strategy_config, test_df)
            fold_pass = (
                test_result.total_trades > 0
                and test_result.total_pnl > 0
                and test_result.sharpe_ratio > 0
                and test_result.max_drawdown <= max(0.35, train_result.max_drawdown * 1.5 + 0.05)
            )
            signature = "|".join(f"{key}={best_params.get(key)}" for key in sorted(best_params)) if best_params else "base"
            signature_counts[signature] = signature_counts.get(signature, 0) + 1
            baseline_beats.append(
                1.0
                if (
                    float(test_result.total_pnl or 0.0) >= float(baseline_test_result.total_pnl or 0.0)
                    and float(test_result.sharpe_ratio or 0.0) >= float(baseline_test_result.sharpe_ratio or 0.0)
                )
                else 0.0
            )
            folds.append(
                {
                    "window": len(folds) + 1,
                    "train_start": int(start),
                    "train_end": int(start + train_size),
                    "test_start": int(start + train_size),
                    "test_end": int(start + train_size + test_size),
                    "optimized_parameters": best_params,
                    "parameter_signature": signature,
                    "train_sharpe_base": _safe_round(train_base_result.sharpe_ratio, 4),
                    "train_sharpe": _safe_round(train_result.sharpe_ratio, 4),
                    "baseline_test_sharpe": _safe_round(baseline_test_result.sharpe_ratio, 4),
                    "test_sharpe": _safe_round(test_result.sharpe_ratio, 4),
                    "train_pnl": _safe_round(train_result.total_pnl, 2),
                    "baseline_test_pnl": _safe_round(baseline_test_result.total_pnl, 2),
                    "test_pnl": _safe_round(test_result.total_pnl, 2),
                    "test_win_rate": _safe_round(test_result.win_rate, 4),
                    "test_drawdown": _safe_round(test_result.max_drawdown, 4),
                    "test_trades": int(test_result.total_trades or 0),
                    "passed": bool(fold_pass),
                }
            )
            start += step

        if not folds:
            return {
                "fold_count": 0,
                "avg_train_sharpe": 0.0,
                "avg_test_sharpe": 0.0,
                "avg_test_pnl": 0.0,
                "positive_test_rate": 0.0,
                "pass_rate": 0.0,
                "degradation_pct": 0.0,
                "parameter_consistency": 0.0,
                "baseline_outperformance_rate": 0.0,
                "stability_score": 0.0,
                "folds": [],
                "insufficient_data": True,
            }

        train_sharpes = np.array([f["train_sharpe"] for f in folds], dtype=float)
        test_sharpes = np.array([f["test_sharpe"] for f in folds], dtype=float)
        test_pnls = np.array([f["test_pnl"] for f in folds], dtype=float)
        positive_test_rate = float(np.mean(test_pnls > 0))
        pass_rate = float(np.mean([1.0 if f["passed"] else 0.0 for f in folds]))
        avg_train = float(train_sharpes.mean()) if len(train_sharpes) else 0.0
        avg_test = float(test_sharpes.mean()) if len(test_sharpes) else 0.0
        degradation_pct = (avg_test - avg_train) / max(abs(avg_train), 1.0)
        parameter_consistency = max(signature_counts.values()) / len(folds) if signature_counts and folds else 0.0
        baseline_outperformance_rate = float(np.mean(baseline_beats)) if baseline_beats else 0.0
        stability_score = round(
            (
                0.35 * positive_test_rate
                + 0.25 * pass_rate
                + 0.20 * (1.0 - _clamp(abs(degradation_pct), 0.0, 1.0))
                + 0.10 * parameter_consistency
                + 0.10 * baseline_outperformance_rate
            )
            * 100.0,
            1,
        )

        return {
            "fold_count": len(folds),
            "avg_train_sharpe": _safe_round(avg_train, 4),
            "avg_test_sharpe": _safe_round(avg_test, 4),
            "avg_test_pnl": _safe_round(float(test_pnls.mean()) if len(test_pnls) else 0.0, 2),
            "positive_test_rate": _safe_round(positive_test_rate, 4),
            "pass_rate": _safe_round(pass_rate, 4),
            "degradation_pct": _safe_round(degradation_pct, 4),
            "parameter_consistency": _safe_round(parameter_consistency, 4),
            "baseline_outperformance_rate": _safe_round(baseline_outperformance_rate, 4),
            "stability_score": stability_score,
            "folds": folds,
            "insufficient_data": False,
        }

    def _stress_test(self, base: BacktestResult) -> Dict[str, Any]:
        scenarios: List[Dict[str, Any]] = []
        base_pnl = float(base.total_pnl or 0.0)
        base_dd = float(base.max_drawdown or 0.0)
        base_commission = float(self.execution_profile.get("commission", DEFAULT_COMMISSION))
        base_slippage = float(self.execution_profile.get("slippage", DEFAULT_SLIPPAGE))

        scenario_specs = [
            (
                "high_friction",
                self.df,
                {
                    "commission": base_commission * 2.5,
                    "slippage": base_slippage * 4.0,
                },
            ),
            (
                "volatility_spike",
                self._apply_volatility_shock(self.df),
                {
                    "commission": base_commission * 1.2,
                    "slippage": base_slippage * 1.8,
                },
            ),
            (
                "whipsaw_regime",
                self._apply_whipsaw_regime(self.df),
                {
                    "commission": base_commission * 1.35,
                    "slippage": base_slippage * 2.2,
                },
            ),
            (
                "gap_shock",
                self._apply_gap_shock(self.df),
                {
                    "commission": base_commission * 1.5,
                    "slippage": base_slippage * 2.5,
                },
            ),
            (
                "flash_crash",
                self._apply_flash_crash(self.df),
                {
                    "commission": base_commission * 2.0,
                    "slippage": base_slippage * 3.5,
                },
            ),
            (
                "combined_crisis",
                self._apply_flash_crash(self._apply_gap_shock(self._apply_volatility_shock(self.df), gap_size=0.022)),
                {
                    "commission": base_commission * 3.0,
                    "slippage": base_slippage * 5.0,
                },
            ),
        ]

        for name, scenario_df, overrides in scenario_specs:
            result = self._run_config(
                self.strategy_config,
                scenario_df,
                commission=overrides["commission"],
                slippage=overrides["slippage"],
            )
            pnl_delta = float(result.total_pnl or 0.0) - base_pnl
            dd_increase = max(0.0, float(result.max_drawdown or 0.0) - base_dd)
            pnl_baseline = max(abs(base_pnl), self.initial_balance * 0.01, 1.0)
            degradation = min(
                1.0,
                0.55 * (abs(pnl_delta) / pnl_baseline)
                + 0.40 * (dd_increase / max(base_dd, 0.05))
                + 0.05 * (
                    abs(int(result.total_trades or 0) - int(base.total_trades or 0))
                    / max(int(base.total_trades or 0), 5)
                ),
            )
            scenario_score = round((1.0 - degradation) * 100.0, 1)
            scenarios.append(
                {
                    "name": name,
                    "sharpe_ratio": _safe_round(result.sharpe_ratio, 4),
                    "total_pnl": _safe_round(result.total_pnl, 2),
                    "win_rate": _safe_round(result.win_rate, 4),
                    "max_drawdown": _safe_round(result.max_drawdown, 4),
                    "pnl_delta": _safe_round(pnl_delta, 2),
                    "drawdown_delta": _safe_round(dd_increase, 4),
                    "trades": int(result.total_trades or 0),
                    "scenario_score": scenario_score,
                }
            )

        scenarios.sort(key=lambda row: row["scenario_score"])
        resilience_score = round(
            float(np.mean([row["scenario_score"] for row in scenarios])) if scenarios else 0.0,
            1,
        )
        worst = scenarios[0]["name"] if scenarios else ""
        return {
            "scenario_count": len(scenarios),
            "base_pnl": _safe_round(base_pnl, 2),
            "base_drawdown": _safe_round(base_dd, 4),
            "worst_case_scenario": worst,
            "resilience_score": resilience_score,
            "scenarios": scenarios,
        }

    def _sensitivity_analysis(self, base: BacktestResult, max_params: int = 5) -> Dict[str, Any]:
        params = self._extract_tunable_params(self.strategy_config)
        params = params[:max_params]
        if not params:
            return {
                "parameter_count": 0,
                "interaction_count": 0,
                "critical_parameters": [],
                "parameters": [],
                "interactions": [],
                "sensitivity_score": 0.0,
                "insufficient_data": True,
            }

        rows: List[Dict[str, Any]] = []
        base_sharpe = float(base.sharpe_ratio or 0.0)

        for item in params:
            key = item["name"]
            base_value = item["value"]
            values = self._variation_values(base_value)
            variants: List[Dict[str, Any]] = []
            for value in values:
                mutated = ParameterOptimizer._apply_params(copy.deepcopy(self.strategy_config), {key: value})
                result = self._run_config(mutated, self.df)
                variants.append(
                    {
                        "value": value,
                        "sharpe_ratio": _safe_round(result.sharpe_ratio, 4),
                        "total_pnl": _safe_round(result.total_pnl, 2),
                        "max_drawdown": _safe_round(result.max_drawdown, 4),
                        "trades": int(result.total_trades or 0),
                    }
                )
            sharpes = [v["sharpe_ratio"] for v in variants]
            swing_sharpe = max(sharpes) - min(sharpes) if sharpes else 0.0
            stability = max(0.0, 1.0 - min(1.0, abs(swing_sharpe) / max(abs(base_sharpe), 1.0)))
            best_variant = max(variants, key=lambda v: (v["sharpe_ratio"], v["total_pnl"]))
            worst_variant = min(variants, key=lambda v: (v["sharpe_ratio"], v["total_pnl"]))
            rows.append(
                {
                    "parameter": key,
                    "base_value": base_value,
                    "best_value": best_variant["value"],
                    "worst_value": worst_variant["value"],
                    "swing_sharpe": _safe_round(swing_sharpe, 4),
                    "stability": _safe_round(stability, 4),
                    "variants": variants,
                }
            )

        rows.sort(key=lambda row: row["swing_sharpe"], reverse=True)
        interactions = self._sensitivity_interactions(rows, base_sharpe)
        single_score = float(np.mean([float(r["stability"]) for r in rows])) if rows else 0.0
        interaction_score = float(np.mean([float(r["stability"]) for r in interactions])) if interactions else 0.0
        sensitivity_score = round(
            (
                (0.65 * single_score)
                + (0.35 * interaction_score if interactions else 0.35 * single_score)
            )
            * 100.0
            if rows
            else 0.0,
            1,
        )
        return {
            "parameter_count": len(rows),
            "interaction_count": len(interactions),
            "critical_parameters": [row["parameter"] for row in rows[:3]],
            "parameters": rows,
            "interactions": interactions,
            "interaction_score": round(interaction_score * 100.0, 1) if interactions else 0.0,
            "sensitivity_score": sensitivity_score,
            "insufficient_data": False,
        }

    def _probabilistic_sharpe(self, base: BacktestResult) -> Dict[str, Any]:
        curve = np.array(list(base.equity_curve or []), dtype=float)
        if len(curve) < 3:
            return {
                "sample_size": 0,
                "observed_sharpe": 0.0,
                "probability_sharpe_positive": 0.0,
                "probability_sharpe_above_one": 0.0,
                "skewness": 0.0,
                "kurtosis": 0.0,
                "autocorrelation_lag1": 0.0,
                "effective_sample_size": 0.0,
                "minimum_track_record_positive_95": 0.0,
                "track_record_ratio": 0.0,
                "confidence_score": 0.0,
                "confidence_label": "insufficient",
            }

        curve[curve == 0] = 1e-10
        returns = np.diff(curve) / curve[:-1]
        returns = returns[np.isfinite(returns)]
        active_returns = returns[np.abs(returns) > 1e-12]
        if len(active_returns) < 3 or np.std(active_returns) == 0:
            return {
                "sample_size": int(len(active_returns)),
                "non_zero_return_count": int(len(active_returns)),
                "observed_sharpe": _safe_round(base.sharpe_ratio, 4),
                "probability_sharpe_positive": 0.0,
                "probability_sharpe_above_one": 0.0,
                "skewness": 0.0,
                "kurtosis": 0.0,
                "autocorrelation_lag1": 0.0,
                "effective_sample_size": float(len(active_returns)),
                "minimum_track_record_positive_95": 0.0,
                "track_record_ratio": 0.0,
                "confidence_score": 0.0,
                "confidence_label": "insufficient",
            }

        mean_ret = float(np.mean(active_returns))
        std_ret = float(np.std(active_returns))
        period_sharpe = mean_ret / std_ret if std_ret > 0 else 0.0
        observed_sharpe = period_sharpe * _annualisation_factor()
        centered = (active_returns - mean_ret) / (std_ret or 1e-10)
        skewness = float(np.mean(centered ** 3))
        kurtosis = float(np.mean(centered ** 4))
        autocorr = _lag1_autocorr(active_returns)
        effective_sample_size = max(1.0, float(len(active_returns)) * (1.0 - autocorr) / max(1e-6, 1.0 + autocorr))

        def _psr_prob(benchmark_annualized: float) -> float:
            benchmark_period = benchmark_annualized / max(_annualisation_factor(), 1e-10)
            denom = math.sqrt(
                max(
                    1e-10,
                    1.0
                    - skewness * period_sharpe
                    + ((kurtosis - 1.0) / 4.0) * (period_sharpe ** 2),
                )
            )
            z_score = ((period_sharpe - benchmark_period) * math.sqrt(max(effective_sample_size - 1.0, 1.0))) / denom
            return 0.5 * (1.0 + math.erf(z_score / math.sqrt(2.0)))

        prob_positive = float(_psr_prob(0.0))
        prob_above_one = float(_psr_prob(1.0))
        min_track_record = self._minimum_track_record_length(
            observed_period_sharpe=period_sharpe,
            benchmark_period_sharpe=0.0,
            skewness=skewness,
            kurtosis=kurtosis,
        )
        track_record_ratio = (
            _clamp(effective_sample_size / max(min_track_record, 1.0), 0.0, 1.0)
            if np.isfinite(min_track_record)
            else 0.0
        )
        confidence_score = round(prob_positive * track_record_ratio * 100.0, 1)
        label = (
            "high" if prob_positive >= 0.85 and track_record_ratio >= 0.8
            else "moderate" if prob_positive >= 0.65 and track_record_ratio >= 0.5
            else "low"
        )

        return {
            "sample_size": int(len(active_returns)),
            "non_zero_return_count": int(len(active_returns)),
            "observed_sharpe": _safe_round(observed_sharpe, 4),
            "probability_sharpe_positive": _safe_round(prob_positive, 4),
            "probability_sharpe_above_one": _safe_round(prob_above_one, 4),
            "skewness": _safe_round(skewness, 4),
            "kurtosis": _safe_round(kurtosis, 4),
            "autocorrelation_lag1": _safe_round(autocorr, 4),
            "effective_sample_size": _safe_round(effective_sample_size, 2),
            "minimum_track_record_positive_95": _safe_round(min_track_record if np.isfinite(min_track_record) else 0.0, 2),
            "track_record_ratio": _safe_round(track_record_ratio, 4),
            "confidence_score": confidence_score,
            "confidence_label": label,
        }

    def _transaction_cost_impact(self, base: BacktestResult) -> Dict[str, Any]:
        base_commission = float(self.execution_profile.get("commission", DEFAULT_COMMISSION))
        base_slippage = float(self.execution_profile.get("slippage", DEFAULT_SLIPPAGE))
        scenarios: List[Dict[str, Any]] = []
        base_pnl = float(base.total_pnl or 0.0)
        base_sharpe = float(base.sharpe_ratio or 0.0)
        break_even_multiplier = 0.0

        for multiplier in (0.5, 1.0, 1.5, 2.0, 3.0):
            result = self._run_config(
                self.strategy_config,
                self.df,
                commission=base_commission * multiplier,
                slippage=base_slippage * multiplier,
            )
            if float(result.total_pnl or 0.0) > 0.0 and float(result.sharpe_ratio or 0.0) >= 0.0:
                break_even_multiplier = multiplier
            scenarios.append(
                {
                    "cost_multiplier": multiplier,
                    "commission": _safe_round(base_commission * multiplier, 6),
                    "slippage": _safe_round(base_slippage * multiplier, 6),
                    "sharpe_ratio": _safe_round(result.sharpe_ratio, 4),
                    "total_pnl": _safe_round(result.total_pnl, 2),
                    "max_drawdown": _safe_round(result.max_drawdown, 4),
                    "trades": int(result.total_trades or 0),
                }
            )

        baseline = next((row for row in scenarios if row["cost_multiplier"] == 1.0), scenarios[0])
        worst = min(scenarios, key=lambda row: (row["total_pnl"], row["sharpe_ratio"]))
        double_cost = next((row for row in scenarios if row["cost_multiplier"] == 2.0), baseline)
        positive_rate = float(np.mean([1.0 if row["total_pnl"] > 0 else 0.0 for row in scenarios]))
        pnl_retention = _clamp(float(double_cost["total_pnl"]) / max(abs(base_pnl), 1.0), -1.0, 1.0)
        sharpe_retention = _clamp((float(double_cost["sharpe_ratio"]) + 5.0) / max(abs(base_sharpe) + 5.0, 1.0), 0.0, 1.0)
        break_even_score = _clamp(break_even_multiplier / 3.0, 0.0, 1.0)
        cost_resilience_score = round(
            (
                0.4 * positive_rate
                + 0.35 * max(0.0, pnl_retention)
                + 0.15 * sharpe_retention
                + 0.10 * break_even_score
            ) * 100.0,
            1,
        )

        return {
            "scenario_count": len(scenarios),
            "base_commission": _safe_round(base_commission, 6),
            "base_slippage": _safe_round(base_slippage, 6),
            "break_even_cost_multiplier": _safe_round(break_even_multiplier, 2),
            "positive_scenario_rate": _safe_round(positive_rate, 4),
            "cost_resilience_score": cost_resilience_score,
            "baseline": baseline,
            "worst_case": worst,
            "scenarios": scenarios,
        }

    def _regime_analysis(self, base: BacktestResult) -> Dict[str, Any]:
        if self.df is None or self.df.empty:
            return {
                "regime_count": 0,
                "best_regime": "",
                "worst_regime": "",
                "regime_balance_score": 0.0,
                "regimes": [],
                "insufficient_data": True,
            }

        labeled = self._label_market_regimes(self.df)
        trade_rows = list(base.trades or [])
        if not trade_rows:
            return {
                "regime_count": 0,
                "best_regime": "",
                "worst_regime": "",
                "regime_balance_score": 0.0,
                "regimes": [],
                "insufficient_data": True,
            }

        buckets: Dict[str, List[float]] = {}
        for trade in trade_rows:
            entry_bar = int(trade.get("entry_bar", 0) or 0)
            if entry_bar < 0 or entry_bar >= len(labeled):
                continue
            regime = str(labeled.iloc[entry_bar] or "unknown")
            buckets.setdefault(regime, []).append(float(trade.get("pnl", 0.0) or 0.0))

        rows: List[Dict[str, Any]] = []
        for regime, pnls in buckets.items():
            arr = np.array(pnls, dtype=float)
            wins = arr[arr > 0]
            losses = arr[arr <= 0]
            trade_count = int(len(arr))
            win_rate = float(len(wins) / trade_count) if trade_count else 0.0
            avg_pnl = float(np.mean(arr)) if trade_count else 0.0
            sharpe = float(np.mean(arr) / np.std(arr)) if trade_count >= 3 and float(np.std(arr)) > 0 else 0.0
            stability = (
                1.0
                if trade_count >= 4 and avg_pnl > 0 and sharpe > 0
                else 0.65 if avg_pnl >= 0
                else 0.25
            )
            rows.append(
                {
                    "regime": regime,
                    "trades": trade_count,
                    "win_rate": _safe_round(win_rate, 4),
                    "total_pnl": _safe_round(arr.sum(), 2),
                    "avg_pnl": _safe_round(avg_pnl, 2),
                    "sharpe_ratio": _safe_round(sharpe, 4),
                    "largest_win": _safe_round(max(wins) if len(wins) else 0.0, 2),
                    "largest_loss": _safe_round(min(losses) if len(losses) else 0.0, 2),
                    "stability": _safe_round(stability, 4),
                }
            )

        rows.sort(key=lambda row: (row["avg_pnl"], row["sharpe_ratio"], row["win_rate"]), reverse=True)
        best_regime = rows[0]["regime"] if rows else ""
        worst_regime = rows[-1]["regime"] if rows else ""
        profitable_regimes = float(np.mean([1.0 if row["total_pnl"] > 0 else 0.0 for row in rows])) if rows else 0.0
        coverage = float(np.mean([min(1.0, row["trades"] / 5.0) for row in rows])) if rows else 0.0
        regime_balance_score = round((0.6 * profitable_regimes + 0.4 * coverage) * 100.0, 1)
        return {
            "regime_count": len(rows),
            "best_regime": best_regime,
            "worst_regime": worst_regime,
            "regime_balance_score": regime_balance_score,
            "regimes": rows,
            "insufficient_data": False,
        }

    @staticmethod
    def _apply_volatility_shock(df: pd.DataFrame, factor: float = 1.35) -> pd.DataFrame:
        out = df.copy()
        mid = (out["high"] + out["low"]) / 2.0
        high_dist = (out["high"] - mid).abs() * factor
        low_dist = (mid - out["low"]).abs() * factor
        close_dev = (out["close"] - out["open"]) * (1.0 + (factor - 1.0) * 0.75)
        out["close"] = out["open"] + close_dev
        upper = pd.concat([out["open"], out["close"]], axis=1).max(axis=1)
        lower = pd.concat([out["open"], out["close"]], axis=1).min(axis=1)
        out["high"] = np.maximum(mid + high_dist, upper)
        out["low"] = np.minimum(mid - low_dist, lower)
        return BacktestEngineV2._prepare(out)

    @staticmethod
    def _apply_gap_shock(df: pd.DataFrame, gap_size: float = 0.018, every: int = 45) -> pd.DataFrame:
        out = df.copy()
        for idx in range(max(10, every), len(out), every):
            sign = -1.0 if ((idx // every) % 2 == 0) else 1.0
            mult = 1.0 + sign * gap_size
            o = float(out.at[idx, "open"]) * mult
            h = float(out.at[idx, "high"]) * mult
            l = float(out.at[idx, "low"]) * mult
            c = float(out.at[idx, "close"]) * mult
            out.at[idx, "open"] = o
            out.at[idx, "close"] = c
            out.at[idx, "high"] = max(h, o, c)
            out.at[idx, "low"] = min(l, o, c)
        return BacktestEngineV2._prepare(out)

    @staticmethod
    def _apply_whipsaw_regime(df: pd.DataFrame, magnitude: float = 0.012, every: int = 16) -> pd.DataFrame:
        out = df.copy()
        for start in range(max(8, every), len(out), every):
            end = min(start + every, len(out))
            for idx in range(start, end):
                sign = -1.0 if ((idx - start) % 2 == 0) else 1.0
                open_price = float(out.at[idx, "open"])
                close_price = open_price * (1.0 + sign * magnitude)
                high = max(float(out.at[idx, "high"]), open_price, close_price) * (1.0 + magnitude * 0.35)
                low = min(float(out.at[idx, "low"]), open_price, close_price) * (1.0 - magnitude * 0.35)
                out.at[idx, "close"] = close_price
                out.at[idx, "high"] = max(high, open_price, close_price)
                out.at[idx, "low"] = min(low, open_price, close_price)
        return BacktestEngineV2._prepare(out)

    @staticmethod
    def _apply_flash_crash(df: pd.DataFrame, crash_size: float = 0.045, recovery_bars: int = 6) -> pd.DataFrame:
        out = df.copy()
        if len(out) < 30:
            return BacktestEngineV2._prepare(out)
        crash_idx = len(out) // 2
        open_price = float(out.at[crash_idx, "open"])
        close_price = open_price * (1.0 - crash_size)
        low_price = close_price * (1.0 - crash_size * 0.35)
        out.at[crash_idx, "close"] = close_price
        out.at[crash_idx, "low"] = min(float(out.at[crash_idx, "low"]), low_price, open_price, close_price)
        out.at[crash_idx, "high"] = max(float(out.at[crash_idx, "high"]), open_price, close_price)

        for offset in range(1, min(recovery_bars + 1, len(out) - crash_idx)):
            idx = crash_idx + offset
            recovery_mult = 1.0 - crash_size * max(0.1, (recovery_bars - offset) / max(recovery_bars, 1))
            out.at[idx, "open"] = float(out.at[idx, "open"]) * recovery_mult
            out.at[idx, "close"] = float(out.at[idx, "close"]) * recovery_mult
            out.at[idx, "high"] = max(float(out.at[idx, "high"]) * recovery_mult, float(out.at[idx, "open"]), float(out.at[idx, "close"]))
            out.at[idx, "low"] = min(float(out.at[idx, "low"]) * recovery_mult, float(out.at[idx, "open"]), float(out.at[idx, "close"]))
        return BacktestEngineV2._prepare(out)

    @staticmethod
    def _label_market_regimes(df: pd.DataFrame) -> pd.Series:
        prepared = BacktestEngineV2._prepare(df)
        if prepared is None or prepared.empty:
            return pd.Series(dtype="object")
        close = prepared["close"].astype(float)
        high = prepared["high"].astype(float)
        low = prepared["low"].astype(float)
        ema_fast = close.ewm(span=20, adjust=False).mean()
        ema_slow = close.ewm(span=50, adjust=False).mean()
        prev_close = close.shift(1)
        true_range = pd.concat(
            [
                (high - low).abs(),
                (high - prev_close).abs(),
                (low - prev_close).abs(),
            ],
            axis=1,
        ).max(axis=1)
        atr = true_range.rolling(14, min_periods=5).mean()
        atr_pct = (atr / close.replace(0, np.nan)).fillna(0.0)
        trend_gap = ((ema_fast - ema_slow).abs() / close.replace(0, np.nan)).fillna(0.0)
        vol_threshold = float(atr_pct.quantile(0.7)) if len(atr_pct.dropna()) else 0.0
        trend_threshold = max(0.0025, float(trend_gap.quantile(0.6)) if len(trend_gap.dropna()) else 0.0)

        labels = []
        for idx in range(len(prepared)):
            if float(atr_pct.iloc[idx]) >= max(vol_threshold, 0.01):
                labels.append("volatile")
            elif float(trend_gap.iloc[idx]) >= trend_threshold:
                labels.append("trending")
            else:
                labels.append("ranging")
        return pd.Series(labels, index=prepared.index, dtype="object")

    @staticmethod
    def _variation_values(value: Any) -> List[Any]:
        if isinstance(value, bool):
            return [value]
        if isinstance(value, int):
            delta = max(1, int(round(abs(value) * 0.2)))
            values = {
                max(2, value - delta),
                int(value),
                max(2, value + delta),
            }
            return sorted(values)
        numeric = float(value)
        delta = max(0.1, abs(numeric) * 0.2)
        values = {
            round(max(0.05, numeric - delta), 4),
            round(numeric, 4),
            round(max(0.05, numeric + delta), 4),
        }
        return sorted(values)

    @staticmethod
    def _extract_tunable_params(config: Dict[str, Any]) -> List[Dict[str, Any]]:
        tunables: List[Dict[str, Any]] = []

        for key in ("stop_mult", "tp_mult"):
            if isinstance(config.get(key), (int, float)):
                tunables.append({"name": key, "value": config[key]})

        indicators = list(config.get("indicators", []))
        ema_periods = []
        for ind in indicators:
            name = ind.get("name")
            params = ind.get("params", {})
            if name == "rsi" and isinstance(params.get("period"), (int, float)):
                tunables.append({"name": "rsi_period", "value": params["period"]})
            elif name == "atr" and isinstance(params.get("period"), (int, float)):
                tunables.append({"name": "atr_period", "value": params["period"]})
            elif name == "bollinger":
                if isinstance(params.get("period"), (int, float)):
                    tunables.append({"name": "bb_period", "value": params["period"]})
                if isinstance(params.get("std"), (int, float)):
                    tunables.append({"name": "bb_std", "value": params["std"]})
            elif name == "macd":
                for key in ("fast", "slow", "signal"):
                    if isinstance(params.get(key), (int, float)):
                        tunables.append({"name": f"macd_{key}", "value": params[key]})
            elif name == "ema" and isinstance(params.get("period"), (int, float)):
                ema_periods.append(int(params["period"]))

        ema_periods = sorted(ema_periods)
        if len(ema_periods) >= 2:
            tunables.append({"name": "ema_fast", "value": ema_periods[0]})
            tunables.append({"name": "ema_slow", "value": ema_periods[-1]})

        seen: set[str] = set()
        ordered: List[Dict[str, Any]] = []
        for item in tunables:
            name = item["name"]
            if name not in seen:
                ordered.append(item)
                seen.add(name)
        return ordered

    def _sensitivity_interactions(self, rows: List[Dict[str, Any]], base_sharpe: float) -> List[Dict[str, Any]]:
        if len(rows) < 2:
            return []
        base_lookup = {row["parameter"]: row["base_value"] for row in rows}
        selected = [row["parameter"] for row in rows[: min(SENSITIVITY_INTERACTION_PARAMS, len(rows))]]
        interactions: List[Dict[str, Any]] = []
        for left, right in itertools.combinations(selected, 2):
            left_values = self._variation_values(base_lookup[left])
            right_values = self._variation_values(base_lookup[right])
            variants: List[Dict[str, Any]] = []
            for left_value in left_values:
                for right_value in right_values:
                    mutated = ParameterOptimizer._apply_params(copy.deepcopy(self.strategy_config), {left: left_value, right: right_value})
                    result = self._run_config(mutated, self.df)
                    variants.append(
                        {
                            left: left_value,
                            right: right_value,
                            "sharpe_ratio": _safe_round(result.sharpe_ratio, 4),
                            "total_pnl": _safe_round(result.total_pnl, 2),
                            "max_drawdown": _safe_round(result.max_drawdown, 4),
                            "trades": int(result.total_trades or 0),
                        }
                    )
            sharpes = [v["sharpe_ratio"] for v in variants]
            swing_sharpe = max(sharpes) - min(sharpes) if sharpes else 0.0
            stability = max(0.0, 1.0 - min(1.0, abs(swing_sharpe) / max(abs(base_sharpe), 1.0)))
            best_variant = max(variants, key=lambda v: (v["sharpe_ratio"], v["total_pnl"]))
            worst_variant = min(variants, key=lambda v: (v["sharpe_ratio"], v["total_pnl"]))
            interactions.append(
                {
                    "parameters": [left, right],
                    "swing_sharpe": _safe_round(swing_sharpe, 4),
                    "stability": _safe_round(stability, 4),
                    "best_values": {left: best_variant[left], right: best_variant[right]},
                    "worst_values": {left: worst_variant[left], right: worst_variant[right]},
                    "variants": variants,
                }
            )
        interactions.sort(key=lambda row: row["swing_sharpe"], reverse=True)
        return interactions

    @staticmethod
    def _block_bootstrap_sample(values: np.ndarray, block_size: int, rng: np.random.Generator) -> np.ndarray:
        arr = np.asarray(values, dtype=float)
        if len(arr) <= block_size:
            return arr.copy()
        sample: List[float] = []
        while len(sample) < len(arr):
            start = int(rng.integers(0, len(arr) - block_size + 1))
            sample.extend(arr[start : start + block_size].tolist())
        return np.array(sample[: len(arr)], dtype=float)

    def _walk_forward_param_grid(self, max_params: int = WALK_FORWARD_OPTIM_PARAMS) -> Dict[str, List[Any]]:
        params = self._extract_tunable_params(self.strategy_config)
        priority = {
            "stop_mult": 0,
            "tp_mult": 1,
            "atr_period": 2,
            "rsi_period": 3,
            "bb_period": 4,
            "bb_std": 5,
            "macd_fast": 6,
            "macd_signal": 7,
            "macd_slow": 8,
            "ema_fast": 9,
            "ema_slow": 10,
        }
        params = sorted(params, key=lambda item: (priority.get(item["name"], 99), item["name"]))[:max_params]
        return {item["name"]: self._variation_values(item["value"]) for item in params}

    @staticmethod
    def _select_best_optimization_row(rows: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if not rows:
            return None
        return max(
            rows,
            key=lambda row: (
                float(row.get("sharpe", 0.0) or 0.0),
                float(row.get("total_pnl", 0.0) or 0.0),
                -float(row.get("max_dd", 0.0) or 0.0),
                float(row.get("trades", 0) or 0),
            ),
        )

    @staticmethod
    def _minimum_track_record_length(
        observed_period_sharpe: float,
        benchmark_period_sharpe: float,
        skewness: float,
        kurtosis: float,
        z_score: float = 1.6448536269514722,
    ) -> float:
        excess = observed_period_sharpe - benchmark_period_sharpe
        if excess <= 0:
            return float("inf")
        numerator = max(
            1e-10,
            1.0
            - skewness * observed_period_sharpe
            + ((kurtosis - 1.0) / 4.0) * (observed_period_sharpe ** 2),
        )
        return 1.0 + numerator * ((z_score / excess) ** 2)
