from __future__ import annotations

import copy
import itertools
import random
from typing import Any, Dict, List, Optional

import pandas as pd

from utils.logger import get_logger

logger = get_logger()

# Maps param_grid key → (indicator_name, param_key) or top-level config key
_PARAM_MAP: Dict[str, tuple] = {
    "rsi_period":     ("rsi",        "period"),
    "ema_fast":       ("ema_fast",   "period"),   # handled specially
    "ema_slow":       ("ema_slow",   "period"),   # handled specially
    "macd_fast":      ("macd",       "fast"),
    "macd_slow":      ("macd",       "slow"),
    "macd_signal":    ("macd",       "signal"),
    "atr_period":     ("atr",        "period"),
    "bb_period":      ("bollinger",  "period"),
    "bb_std":         ("bollinger",  "std"),
    "stop_mult":      ("_top",       "stop_mult"),
    "tp_mult":        ("_top",       "tp_mult"),
}


class ParameterOptimizer:
    """
    Grid and random search over strategy parameters.
    Each combination is evaluated by running a full backtest.
    """

    def __init__(
        self,
        base_config: Dict,
        df: pd.DataFrame,
        initial_balance: float = 10_000.0,
        asset: str = "",
        category: str = "",
    ) -> None:
        self._base_config      = base_config
        self._df               = df
        self._initial_balance  = initial_balance
        self._asset            = asset
        self._category         = category

    # ── Public API ────────────────────────────────────────────────────────────

    def grid_search(self, param_grid: Dict[str, List[Any]]) -> List[Dict]:
        """
        Exhaustive search over all combinations in param_grid.

        Example
        -------
            optimizer.grid_search({
                "rsi_period": [10, 14, 21],
                "stop_mult":  [1.0, 1.5, 2.0],
                "tp_mult":    [2.0, 3.0],
            })
        Returns list of result dicts sorted by Sharpe ratio (best first).
        Each dict contains the param values + performance metrics.
        """
        keys   = list(param_grid.keys())
        combos = list(itertools.product(*[param_grid[k] for k in keys]))
        logger.info(
            f"[Optimizer] Grid search: {len(combos)} combinations "
            f"for strategy '{self._base_config.get('name', '?')}'"
        )
        return self._run_all(keys, combos)

    def random_search(self, param_grid: Dict[str, List[Any]],
                      n_samples: int = 20,
                      seed: int = 42) -> List[Dict]:
        """
        Random sample of n_samples combinations from param_grid.
        Faster than grid_search for large parameter spaces.
        """
        random.seed(seed)
        keys   = list(param_grid.keys())
        combos = [
            tuple(random.choice(param_grid[k]) for k in keys)
            for _ in range(n_samples)
        ]
        # Deduplicate
        combos = list(set(combos))
        logger.info(
            f"[Optimizer] Random search: {len(combos)} samples "
            f"for strategy '{self._base_config.get('name', '?')}'"
        )
        return self._run_all(keys, combos)

    def top_n(self, results: List[Dict], n: int = 5) -> List[Dict]:
        """Return top-N results by Sharpe ratio."""
        return sorted(results, key=lambda x: x.get("sharpe", -999),
                      reverse=True)[:n]

    # ── Internal ──────────────────────────────────────────────────────────────

    def _run_all(self, keys: List[str], combos: list) -> List[Dict]:
        results = []
        for combo in combos:
            params = dict(zip(keys, combo))
            result = self._evaluate(params)
            if result:
                results.append({**params, **result})
        results.sort(key=lambda x: x.get("sharpe", -999), reverse=True)
        if results:
            best = results[0]
            logger.info(
                f"[Optimizer] Best → Sharpe={best.get('sharpe', 0):.2f}  "
                f"WinRate={best.get('win_rate', 0):.1%}  "
                f"PnL={best.get('total_pnl', 0):+.2f}  "
                f"params={{{', '.join(f'{k}={best[k]}' for k in keys)}}}"
            )
        return results

    def _evaluate(self, params: Dict[str, Any]) -> Optional[Dict]:
        """Apply params to a copy of base config and run a backtest."""
        try:
            from strategy_lab.strategy_builder   import StrategyBuilder
            from strategy_lab.backtest_engine_v2 import BacktestEngineV2

            config   = self._apply_params(copy.deepcopy(self._base_config), params)
            strategy = StrategyBuilder.from_dict(config, asset=self._asset, category=self._category)
            engine   = BacktestEngineV2(
                strategy=strategy,
                initial_balance=self._initial_balance,
                asset=self._asset,
                category=self._category,
            )
            result = engine.run(self._df)
            return {
                "sharpe":     round(result.sharpe_ratio,  4),
                "total_pnl":  round(result.total_pnl,     2),
                "win_rate":   round(result.win_rate,       4),
                "max_dd":     round(result.max_drawdown,   4),
                "pf":         round(result.profit_factor,  4),
                "trades":     result.total_trades,
            }
        except Exception as e:
            logger.debug(f"[Optimizer] Eval error params={params}: {e}")
            return None

    @staticmethod
    def _apply_params(config: Dict, params: Dict[str, Any]) -> Dict:
        """Inject param values into the config dict."""
        for key, value in params.items():
            mapping = _PARAM_MAP.get(key)
            if not mapping:
                continue
            ind_name, param_key = mapping

            # Top-level config key (stop_mult, tp_mult)
            if ind_name == "_top":
                config[param_key] = value
                continue

            # Special handling for ema_fast / ema_slow
            # We track which EMA is "fast" (smaller period) vs "slow" (larger)
            if key in ("ema_fast", "ema_slow"):
                ema_inds = [
                    ind for ind in config.get("indicators", [])
                    if ind.get("name") == "ema"
                ]
                ema_inds.sort(key=lambda x: x.get("params", {}).get("period", 0))
                target_idx = 0 if key == "ema_fast" else -1
                if ema_inds:
                    ema_inds[target_idx]["params"]["period"] = value
                continue

            # Regular indicator param
            for ind in config.get("indicators", []):
                if ind.get("name") == ind_name:
                    ind.setdefault("params", {})[param_key] = value
                    break

        return config
