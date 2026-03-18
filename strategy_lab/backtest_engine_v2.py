from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from utils.logger import get_logger

logger = get_logger()

# ── Default parameters ────────────────────────────────────────────────────────
DEFAULT_COMMISSION  = 0.001   # 0.10% per trade (entry + exit)
DEFAULT_SLIPPAGE    = 0.0005  # 0.05% slippage on fill price
DEFAULT_RISK_PER_TRADE = 0.01 # 1% of balance risked per trade


@dataclass
class BacktestResult:
    """Complete results from a single backtest run."""
    initial_balance: float
    final_balance:   float
    total_trades:    int
    win_rate:        float
    total_pnl:       float
    total_pnl_pct:   float
    max_drawdown:    float
    sharpe_ratio:    float
    profit_factor:   float
    expectancy:      float
    avg_win:         float
    avg_loss:        float
    largest_win:     float
    largest_loss:    float
    trades:          List[Dict] = field(default_factory=list)
    equity_curve:    List[float] = field(default_factory=list)

    def summary(self) -> str:
        return (
            f"Trades={self.total_trades}  "
            f"WinRate={self.win_rate:.1%}  "
            f"PnL={self.total_pnl:+.2f} ({self.total_pnl_pct:+.1f}%)  "
            f"MaxDD={self.max_drawdown:.1%}  "
            f"Sharpe={self.sharpe_ratio:.2f}  "
            f"PF={self.profit_factor:.2f}"
        )

    def to_dict(self) -> Dict:
        return {
            "initial_balance": self.initial_balance,
            "final_balance":   round(self.final_balance, 2),
            "total_trades":    self.total_trades,
            "win_rate":        round(self.win_rate, 4),
            "total_pnl":       round(self.total_pnl, 2),
            "total_pnl_pct":   round(self.total_pnl_pct, 4),
            "max_drawdown":    round(self.max_drawdown, 4),
            "sharpe_ratio":    round(self.sharpe_ratio, 4),
            "profit_factor":   round(self.profit_factor, 4),
            "expectancy":      round(self.expectancy, 2),
            "avg_win":         round(self.avg_win, 2),
            "avg_loss":        round(self.avg_loss, 2),
            "largest_win":     round(self.largest_win, 2),
            "largest_loss":    round(self.largest_loss, 2),
            "trade_count":     self.total_trades,
        }


class BacktestEngineV2:
    """
    Event-driven backtester. Iterates bar-by-bar, applies the strategy,
    and manages open positions with stop-loss / take-profit checks.
    """

    def __init__(
        self,
        strategy,
        initial_balance:  float = 10_000.0,
        commission:       float = DEFAULT_COMMISSION,
        slippage:         float = DEFAULT_SLIPPAGE,
        risk_per_trade:   float = DEFAULT_RISK_PER_TRADE,
        min_bars_warmup:  int   = 50,
    ) -> None:
        self.strategy         = strategy
        self.initial_balance  = initial_balance
        self.commission       = commission
        self.slippage         = slippage
        self.risk_per_trade   = risk_per_trade
        self.min_bars_warmup  = min_bars_warmup

    # ── Public API ────────────────────────────────────────────────────────────

    def run(self, df: pd.DataFrame) -> BacktestResult:
        """
        Run backtest against a full OHLCV DataFrame.
        df must have columns: open, high, low, close, volume (lowercase).
        """
        df = self._prepare(df)
        if df is None or len(df) < self.min_bars_warmup + 10:
            return self._empty_result()

        balance      = self.initial_balance
        position     = None       # open position dict or None
        trades:      List[Dict]  = []
        equity_curve: List[float] = [balance]

        for i in range(self.min_bars_warmup, len(df)):
            bar    = df.iloc[i]
            window = df.iloc[:i]

            # ── Check if open position hits SL or TP on this bar ──────────
            if position:
                exit_price, outcome = self._check_exit(position, bar)
                if exit_price:
                    pnl     = self._calc_pnl(position, exit_price)
                    balance = balance + pnl
                    trades.append(self._close_trade(position, exit_price,
                                                     pnl, outcome, i))
                    position = None

            # ── Generate signal on close of this bar ─────────────────────
            if position is None:
                signal = self.strategy.generate(window)
                if signal and signal.get("entry_price"):
                    # Fill on next bar open (realistic execution)
                    if i + 1 < len(df):
                        fill_price = float(df.iloc[i + 1]["open"])
                        fill_price = self._apply_slippage(
                            fill_price, signal["direction"]
                        )
                        size = self._position_size(
                            balance, fill_price,
                            float(signal.get("stop_loss", fill_price * 0.985))
                        )
                        if size > 0:
                            position = {
                                "direction":  signal["direction"],
                                "entry_price": fill_price,
                                "stop_loss":   float(signal.get("stop_loss",  fill_price * 0.985)),
                                "take_profit": float(signal.get("take_profit", fill_price * 1.03)),
                                "size":        size,
                                "entry_bar":   i + 1,
                                "commission":  fill_price * size * self.commission,
                            }
                            balance -= position["commission"]

            equity_curve.append(balance)

        # Force-close any remaining position at last close
        if position:
            last_price = float(df.iloc[-1]["close"])
            pnl        = self._calc_pnl(position, last_price)
            balance    = balance + pnl
            trades.append(self._close_trade(position, last_price,
                                             pnl, "forced_close", len(df) - 1))
            equity_curve.append(balance)

        analyzer = PerformanceAnalyzer()
        return analyzer.compute(
            trades=trades,
            equity_curve=equity_curve,
            initial_balance=self.initial_balance,
            final_balance=balance,
        )

    # ── Internal ──────────────────────────────────────────────────────────────

    @staticmethod
    def _prepare(df: pd.DataFrame) -> Optional[pd.DataFrame]:
        df = df.copy()
        df.columns = [c.lower() for c in df.columns]
        required = {"open", "high", "low", "close", "volume"}
        if not required.issubset(df.columns):
            logger.warning(f"[BacktestV2] Missing columns: {required - set(df.columns)}")
            return None
        for col in required:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df = df.dropna(subset=list(required))
        df = df.reset_index(drop=True)
        return df

    def _check_exit(self, position: Dict, bar) -> tuple:
        """Check if SL or TP was hit on this bar. Returns (exit_price, outcome)."""
        direction   = position["direction"]
        sl          = position["stop_loss"]
        tp          = position["take_profit"]
        bar_low     = float(bar["low"])
        bar_high    = float(bar["high"])

        if direction == "BUY":
            if bar_low  <= sl: return sl, "stop_loss"
            if bar_high >= tp: return tp, "take_profit"
        else:
            if bar_high >= sl: return sl, "stop_loss"
            if bar_low  <= tp: return tp, "take_profit"
        return None, None

    def _calc_pnl(self, position: Dict, exit_price: float) -> float:
        direction  = position["direction"]
        entry      = position["entry_price"]
        size       = position["size"]
        commission = position.get("commission", 0)
        exit_comm  = exit_price * size * self.commission

        if direction == "BUY":
            gross = (exit_price - entry) * size
        else:
            gross = (entry - exit_price) * size

        return gross - commission - exit_comm

    def _apply_slippage(self, price: float, direction: str) -> float:
        if direction == "BUY":
            return price * (1 + self.slippage)
        return price * (1 - self.slippage)

    def _position_size(self, balance: float,
                       entry: float, stop: float) -> float:
        """Kelly-lite: risk a fixed % of balance per trade."""
        risk_amount = balance * self.risk_per_trade
        stop_dist   = abs(entry - stop)
        if stop_dist == 0:
            return 0.0
        size = risk_amount / stop_dist
        # Cap: never risk more than 20% of balance in a single position
        max_size = balance * 0.20 / entry
        return min(size, max_size)

    @staticmethod
    def _close_trade(position: Dict, exit_price: float,
                     pnl: float, outcome: str, bar_idx: int) -> Dict:
        return {
            "direction":   position["direction"],
            "entry_price": position["entry_price"],
            "exit_price":  exit_price,
            "stop_loss":   position["stop_loss"],
            "take_profit": position["take_profit"],
            "size":        position["size"],
            "pnl":         round(pnl, 4),
            "outcome":     outcome,
            "entry_bar":   position["entry_bar"],
            "exit_bar":    bar_idx,
            "duration":    bar_idx - position["entry_bar"],
        }

    def _empty_result(self) -> BacktestResult:
        return BacktestResult(
            initial_balance=self.initial_balance,
            final_balance=self.initial_balance,
            total_trades=0, win_rate=0.0,
            total_pnl=0.0, total_pnl_pct=0.0,
            max_drawdown=0.0, sharpe_ratio=0.0,
            profit_factor=0.0, expectancy=0.0,
            avg_win=0.0, avg_loss=0.0,
            largest_win=0.0, largest_loss=0.0,
        )


# Import here to avoid circular dependency
from strategy_lab.performance_analyzer import PerformanceAnalyzer  # noqa: E402
