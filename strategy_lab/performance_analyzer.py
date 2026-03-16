"""
strategy_lab/performance_analyzer.py — Backtest performance metrics engine.

Computes a complete suite of professional performance metrics from
a list of trades and an equity curve produced by BacktestEngineV2.

Metrics computed
----------------
    win_rate        — % of trades that were profitable
    total_pnl       — net profit/loss in currency
    total_pnl_pct   — total PnL as % of initial balance
    max_drawdown    — largest peak-to-trough % decline in equity curve
    sharpe_ratio    — annualised Sharpe ratio (daily bars, rf=0)
    sortino_ratio   — Sharpe using only downside deviation
    profit_factor   — gross profit / gross loss
    expectancy      — average PnL per trade (positive = edge exists)
    avg_win         — average winning trade size
    avg_loss        — average losing trade size (negative)
    largest_win     — single best trade
    largest_loss    — single worst trade (negative)
    avg_duration    — average bars held per trade
    consecutive_wins  — longest winning streak
    consecutive_losses — longest losing streak

Run tests
---------
    pytest tests/test_strategy_lab.py::TestPerformanceAnalyzer -v
"""
from __future__ import annotations

import math
from typing import Dict, List, Optional

import numpy as np

from strategy_lab.backtest_engine_v2 import BacktestResult
from utils.logger import get_logger

logger = get_logger()

TRADING_DAYS_PER_YEAR = 252


class PerformanceAnalyzer:
    """
    Stateless calculator — takes trades + equity curve, returns BacktestResult.
    Also provides a compare() method for ranking multiple BacktestResults.
    """

    # ── Public API ────────────────────────────────────────────────────────────

    def compute(
        self,
        trades:          List[Dict],
        equity_curve:    List[float],
        initial_balance: float,
        final_balance:   float,
    ) -> BacktestResult:
        """Build a complete BacktestResult from raw trade data."""
        if not trades:
            return BacktestResult(
                initial_balance=initial_balance,
                final_balance=final_balance,
                total_trades=0,
                win_rate=0.0,
                total_pnl=round(final_balance - initial_balance, 2),
                total_pnl_pct=0.0,
                max_drawdown=0.0,
                sharpe_ratio=0.0,
                profit_factor=0.0,
                expectancy=0.0,
                avg_win=0.0,
                avg_loss=0.0,
                largest_win=0.0,
                largest_loss=0.0,
                trades=trades,
                equity_curve=equity_curve,
            )

        pnls        = [t["pnl"] for t in trades]
        wins        = [p for p in pnls if p > 0]
        losses      = [p for p in pnls if p <= 0]
        total_pnl   = sum(pnls)

        win_rate     = len(wins) / len(pnls) if pnls else 0.0
        avg_win      = sum(wins)   / len(wins)   if wins   else 0.0
        avg_loss     = sum(losses) / len(losses) if losses else 0.0
        largest_win  = max(pnls) if pnls else 0.0
        largest_loss = min(pnls) if pnls else 0.0

        gross_profit = sum(wins)
        gross_loss   = abs(sum(losses))
        profit_factor = (gross_profit / gross_loss
                         if gross_loss > 0 else float("inf"))

        expectancy = total_pnl / len(pnls) if pnls else 0.0

        return BacktestResult(
            initial_balance = initial_balance,
            final_balance   = round(final_balance, 2),
            total_trades    = len(trades),
            win_rate        = round(win_rate, 4),
            total_pnl       = round(total_pnl, 2),
            total_pnl_pct   = round(total_pnl / initial_balance, 4),
            max_drawdown    = round(self._max_drawdown(equity_curve), 4),
            sharpe_ratio    = round(self._sharpe(equity_curve), 4),
            profit_factor   = round(profit_factor, 4),
            expectancy      = round(expectancy, 2),
            avg_win         = round(avg_win, 2),
            avg_loss        = round(avg_loss, 2),
            largest_win     = round(largest_win, 2),
            largest_loss    = round(largest_loss, 2),
            trades          = trades,
            equity_curve    = equity_curve,
        )

    def compare(self, results: List[BacktestResult],
                labels: Optional[List[str]] = None) -> List[Dict]:
        """
        Rank multiple BacktestResults by Sharpe ratio.
        Returns a list of dicts sorted best-to-worst.
        """
        labels = labels or [f"Strategy_{i}" for i in range(len(results))]
        ranked = []
        for label, r in zip(labels, results):
            ranked.append({
                "label":        label,
                "sharpe":       r.sharpe_ratio,
                "total_pnl":    r.total_pnl,
                "win_rate":     r.win_rate,
                "max_drawdown": r.max_drawdown,
                "trades":       r.total_trades,
                "profit_factor": r.profit_factor,
            })
        ranked.sort(key=lambda x: x["sharpe"], reverse=True)
        return ranked

    def extended_stats(self, result: BacktestResult) -> Dict:
        """
        Compute additional stats not in the core BacktestResult.
        Returns a dict with sortino_ratio, avg_duration,
        consecutive_wins, consecutive_losses.
        """
        trades = result.trades
        if not trades:
            return {
                "sortino_ratio":       0.0,
                "avg_duration":        0.0,
                "consecutive_wins":    0,
                "consecutive_losses":  0,
            }
        pnls         = [t["pnl"] for t in trades]
        durations    = [t.get("duration", 0) for t in trades]
        avg_duration = sum(durations) / len(durations)
        max_consec_w = self._max_consecutive(pnls, positive=True)
        max_consec_l = self._max_consecutive(pnls, positive=False)
        sortino      = self._sortino(result.equity_curve)

        return {
            "sortino_ratio":      round(sortino, 4),
            "avg_duration":       round(avg_duration, 1),
            "consecutive_wins":   max_consec_w,
            "consecutive_losses": max_consec_l,
        }

    # ── Internal ──────────────────────────────────────────────────────────────

    @staticmethod
    def _max_drawdown(equity_curve: List[float]) -> float:
        """Maximum peak-to-trough decline as a fraction of peak value."""
        if len(equity_curve) < 2:
            return 0.0
        arr = np.array(equity_curve, dtype=float)
        peak = np.maximum.accumulate(arr)
        peak[peak == 0] = 1e-10
        dd   = (arr - peak) / peak
        return float(abs(dd.min()))

    @staticmethod
    def _sharpe(equity_curve: List[float]) -> float:
        """Annualised Sharpe ratio assuming daily bars and risk-free rate = 0."""
        if len(equity_curve) < 3:
            return 0.0
        arr     = np.array(equity_curve, dtype=float)
        # Replace zeros to avoid division errors
        arr[arr == 0] = 1e-10
        returns = np.diff(arr) / arr[:-1]
        if returns.std() == 0:
            return 0.0
        return float(
            returns.mean() / returns.std() * math.sqrt(TRADING_DAYS_PER_YEAR)
        )

    @staticmethod
    def _sortino(equity_curve: List[float]) -> float:
        """Sortino ratio — like Sharpe but only penalises downside volatility."""
        if len(equity_curve) < 3:
            return 0.0
        arr     = np.array(equity_curve, dtype=float)
        arr[arr == 0] = 1e-10
        returns = np.diff(arr) / arr[:-1]
        down    = returns[returns < 0]
        if len(down) == 0 or down.std() == 0:
            return float("inf") if returns.mean() > 0 else 0.0
        return float(
            returns.mean() / down.std() * math.sqrt(TRADING_DAYS_PER_YEAR)
        )

    @staticmethod
    def _max_consecutive(pnls: List[float], positive: bool) -> int:
        """Count longest streak of winning (positive=True) or losing trades."""
        max_streak = current = 0
        for p in pnls:
            if (p > 0) == positive:
                current += 1
                max_streak = max(max_streak, current)
            else:
                current = 0
        return max_streak
