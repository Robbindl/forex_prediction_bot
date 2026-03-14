"""
backtest/engine.py — Full pipeline backtester.
Merges: engines/backtest_engine.py + backtest logic from trading_system.py
"""
from __future__ import annotations
import time
from datetime import datetime
from typing import Any, Dict, List, Optional
import pandas as pd
import numpy as np
from core.signal import Signal
from core.pipeline import Pipeline
from strategies.voting import VotingStrategy
from risk.manager import RiskManager
from utils.logger import get_logger

logger = get_logger()


class BacktestResult:
    def __init__(self, trades: List[Dict], equity_curve: List[float], initial_balance: float):
        self.trades         = trades
        self.equity_curve   = equity_curve
        self.initial_balance = initial_balance

    @property
    def total_trades(self) -> int: return len(self.trades)

    @property
    def wins(self) -> int: return sum(1 for t in self.trades if t.get("pnl", 0) > 0)

    @property
    def losses(self) -> int: return self.total_trades - self.wins

    @property
    def win_rate(self) -> float: return self.wins / self.total_trades if self.total_trades else 0.0

    @property
    def total_pnl(self) -> float: return sum(t.get("pnl", 0) for t in self.trades)

    @property
    def profit_factor(self) -> float:
        gross_profit = sum(t["pnl"] for t in self.trades if t.get("pnl", 0) > 0)
        gross_loss   = abs(sum(t["pnl"] for t in self.trades if t.get("pnl", 0) < 0))
        return gross_profit / gross_loss if gross_loss > 0 else float("inf")

    @property
    def max_drawdown(self) -> float:
        if not self.equity_curve:
            return 0.0
        peak = self.equity_curve[0]
        dd   = 0.0
        for v in self.equity_curve:
            peak = max(peak, v)
            dd   = min(dd, (v - peak) / peak)
        return abs(dd)

    @property
    def sharpe_ratio(self) -> float:
        if len(self.equity_curve) < 2:
            return 0.0
        rets = pd.Series(self.equity_curve).pct_change().dropna()
        return float(rets.mean() / rets.std() * np.sqrt(252)) if rets.std() > 0 else 0.0

    def to_dict(self) -> Dict:
        return {
            "total_trades":   self.total_trades,
            "wins":           self.wins,
            "losses":         self.losses,
            "win_rate":       round(self.win_rate * 100, 2),
            "total_pnl":      round(self.total_pnl, 4),
            "profit_factor":  round(self.profit_factor, 3),
            "max_drawdown":   round(self.max_drawdown * 100, 2),
            "sharpe_ratio":   round(self.sharpe_ratio, 3),
            "initial_balance": self.initial_balance,
            "final_balance":  round(self.initial_balance + self.total_pnl, 4),
            "return_pct":     round(self.total_pnl / self.initial_balance * 100, 2),
        }

    def __repr__(self) -> str:
        d = self.to_dict()
        return (
            f"BacktestResult(trades={d['total_trades']} "
            f"wr={d['win_rate']}% pf={d['profit_factor']} "
            f"pnl={d['total_pnl']} dd={d['max_drawdown']}%)"
        )


class BacktestEngine:
    """
    Runs a full walk-forward backtest using the 7-layer pipeline.
    Every bar generates a signal attempt; pipeline decides entry.
    """

    def __init__(
        self,
        initial_balance: float = 10000.0,
        use_pipeline: bool = True,
        strategy=None,
    ):
        self.initial_balance = initial_balance
        self._pipeline       = Pipeline() if use_pipeline else None
        self._strategy       = strategy or VotingStrategy()
        self._risk_manager   = RiskManager(initial_balance)

    def run(
        self,
        asset: str,
        category: str,
        df: pd.DataFrame,
        warmup: int = 50,
    ) -> BacktestResult:
        """
        Walk-forward backtest on df.
        For each bar i > warmup: generate signal, run pipeline, track trade outcome.
        """
        if df is None or len(df) < warmup + 10:
            logger.warning(f"[Backtest] Insufficient data for {asset}")
            return BacktestResult([], [self.initial_balance], self.initial_balance)

        trades: List[Dict]       = []
        equity: List[float]      = [self.initial_balance]
        balance                  = self.initial_balance
        open_trade: Optional[Dict] = None

        df = df.reset_index(drop=True)

        for i in range(warmup, len(df)):
            bar   = df.iloc[i]
            price = float(bar["close"])
            ctx   = {"price_data": df.iloc[:i], "balance": balance}

            # ── Check open trade exit ──────────────────────────────────────
            if open_trade:
                direction = open_trade["direction"]
                sl        = open_trade["stop_loss"]
                tp        = open_trade["take_profit"]
                entry     = open_trade["entry_price"]
                size      = open_trade["position_size"]

                pnl     = (price - entry) * size if direction == "BUY" else (entry - price) * size
                hit_sl  = (direction == "BUY" and price <= sl) or (direction == "SELL" and price >= sl)
                hit_tp  = (direction == "BUY" and price >= tp) or (direction == "SELL" and price <= tp)

                if hit_sl or hit_tp:
                    reason = "Stop Loss" if hit_sl else "Take Profit"
                    open_trade.update({"exit_price": price, "pnl": pnl, "exit_reason": reason, "exit_bar": i})
                    balance += pnl
                    trades.append(open_trade)
                    open_trade = None

                equity.append(balance)
                continue

            # ── Generate new signal ────────────────────────────────────────
            try:
                sig = self._strategy.generate(asset, asset, category, df.iloc[:i])
            except Exception:
                equity.append(balance)
                continue

            if sig is None:
                equity.append(balance)
                continue

            # ── Run through pipeline ───────────────────────────────────────
            if self._pipeline:
                ctx["price_data"] = df.iloc[:i]
                sig = self._pipeline.run(sig, ctx)

            if sig is None:
                equity.append(balance)
                continue

            # ── Open trade at next bar open ────────────────────────────────
            entry = float(df.iloc[i]["open"]) if i + 1 < len(df) else price
            size  = self._risk_manager.calculate_position_size(
                entry, sig.stop_loss, category, sig.confidence
            )
            if size <= 0:
                equity.append(balance)
                continue

            # Recalculate SL/TP relative to actual entry
            sl_dist = abs(sig.entry_price - sig.stop_loss)
            tp_dist = abs(sig.take_profit - sig.entry_price)
            sl = entry - sl_dist if sig.direction == "BUY" else entry + sl_dist
            tp = entry + tp_dist if sig.direction == "BUY" else entry - tp_dist

            open_trade = {
                "trade_id":      f"bt_{i}",
                "asset":          asset,
                "category":       category,
                "direction":      sig.direction,
                "entry_price":    entry,
                "stop_loss":      sl,
                "take_profit":    tp,
                "position_size":  size,
                "confidence":     sig.confidence,
                "strategy_id":    sig.strategy_id,
                "entry_bar":      i,
                "open_time":      str(df.index[i]) if hasattr(df.index[i], '__str__') else str(i),
            }
            equity.append(balance)

        # Close any remaining open trade at last price
        if open_trade:
            last_price = float(df["close"].iloc[-1])
            entry      = open_trade["entry_price"]
            size       = open_trade["position_size"]
            direction  = open_trade["direction"]
            pnl = (last_price - entry) * size if direction == "BUY" else (entry - last_price) * size
            open_trade.update({"exit_price": last_price, "pnl": pnl, "exit_reason": "End of data"})
            trades.append(open_trade)
            balance += pnl

        logger.info(
            f"[Backtest] {asset} complete — "
            f"trades={len(trades)} balance={balance:.2f}"
        )
        return BacktestResult(trades, equity, self.initial_balance)

    def run_portfolio(
        self,
        asset_data: Dict[str, Dict],   # {asset: {"category": cat, "df": df}}
        warmup: int = 50,
    ) -> Dict[str, BacktestResult]:
        """Run backtest across multiple assets."""
        results = {}
        for asset, info in asset_data.items():
            try:
                result = self.run(
                    asset    = asset,
                    category = info.get("category", "forex"),
                    df       = info.get("df"),
                    warmup   = warmup,
                )
                results[asset] = result
                logger.info(f"[Backtest] {asset}: {result}")
            except Exception as e:
                logger.error(f"[Backtest] {asset} failed: {e}")
        return results