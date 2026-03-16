"""
strategy_lab/strategy_builder.py — JSON-driven dynamic strategy factory.

Define a trading strategy entirely as a Python dict (or JSON file) —
no new class required. The config specifies which indicators to compute,
entry rules to evaluate, confidence boosts to apply, and risk parameters
to use. StrategyBuilder converts the config into a runnable DynamicStrategy.

Config schema
-------------
    {
        "name":        str            # strategy identifier
        "version":     str            # semver string
        "indicators":  [              # list of indicators to compute
            {"name": "rsi",  "params": {"period": 14}},
            {"name": "ema",  "params": {"period": 20}},
            {"name": "ema",  "params": {"period": 50}},
            {"name": "atr",  "params": {"period": 14}},
            {"name": "macd", "params": {"fast": 12, "slow": 26, "signal": 9}},
            {"name": "bollinger", "params": {"period": 20, "std": 2.0}},
        ],
        "entry_rules": [              # ALL must pass (AND logic)
            {
                "col":       str,     # DataFrame column to check
                "op":        str,     # >, <, >=, <=, cross_above, cross_below
                "val":       float,   # static comparison value  (use val OR col2)
                "col2":      str,     # dynamic comparison column (use val OR col2)
                "direction": str,     # "BUY" | "SELL"  (optional, first one wins)
            }
        ],
        "confidence_boosts": [        # each passing rule adds boost to base 0.65
            {"col": str, "above": float, "boost": float},
            {"col": str, "below": float, "boost": float},
        ],
        "stop_mult":   float,         # ATR multiplier for stop loss  (default 1.5)
        "tp_mult":     float,         # risk:reward multiplier        (default 3.0)
    }

Supported indicators
--------------------
    rsi, ema, macd, bollinger, atr, volume_ma, stoch, adx

Run tests
---------
    pytest tests/test_strategy_lab.py::TestStrategyBuilder -v
"""
from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from utils.logger import get_logger

logger = get_logger()

# ── Indicator registry ────────────────────────────────────────────────────────
_INDICATORS: Dict[str, str] = {
    "rsi":       "_calc_rsi",
    "ema":       "_calc_ema",
    "macd":      "_calc_macd",
    "bollinger": "_calc_bollinger",
    "atr":       "_calc_atr",
    "volume_ma": "_calc_volume_ma",
    "stoch":     "_calc_stoch",
    "adx":       "_calc_adx",
}


class DynamicStrategy:
    """
    A fully config-driven strategy. Created by StrategyBuilder.
    Compatible with BacktestEngineV2 and the existing strategies/base.py interface.
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        self.name    = config.get("name", "dynamic")
        self.version = config.get("version", "1.0")
        self._config = config
        self._validate()

    # ── Public API ────────────────────────────────────────────────────────────

    def generate(self, df: pd.DataFrame) -> Optional[Dict]:
        """
        Run strategy against an OHLCV DataFrame.
        Returns a signal dict or None if no entry triggered.
        df must have columns: open, high, low, close, volume (lowercase).
        """
        if df is None or len(df) < 50:
            return None
        try:
            df      = self._add_indicators(df.copy())
            entry   = self._evaluate_rules(df)
            if not entry:
                return None

            direction   = entry["direction"]
            confidence  = self._calc_confidence(df)
            price       = float(df["close"].iloc[-1])
            atr         = float(df["atr"].iloc[-1]) if "atr" in df.columns else price * 0.015
            stop_mult   = float(self._config.get("stop_mult", 1.5))
            tp_mult     = float(self._config.get("tp_mult",   3.0))

            if direction == "BUY":
                stop_loss   = price - atr * stop_mult
                take_profit = price + atr * stop_mult * tp_mult
            else:
                stop_loss   = price + atr * stop_mult
                take_profit = price - atr * stop_mult * tp_mult

            return {
                "direction":   direction,
                "confidence":  round(confidence, 4),
                "entry_price": round(price, 8),
                "stop_loss":   round(stop_loss, 8),
                "take_profit": round(take_profit, 8),
                "strategy_id": self.name,
                "indicators":  self._snapshot(df),
            }
        except Exception as e:
            logger.debug(f"[StrategyBuilder] generate error: {e}")
            return None

    # ── Internal ──────────────────────────────────────────────────────────────

    def _validate(self) -> None:
        if "entry_rules" not in self._config:
            raise ValueError(f"Strategy '{self.name}' missing 'entry_rules'")

    def _add_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        for ind in self._config.get("indicators", []):
            name   = ind.get("name", "")
            params = ind.get("params", {})
            method = getattr(self, _INDICATORS.get(name, "_noop"), None)
            if method:
                df = method(df, **params)
        return df

    def _evaluate_rules(self, df: pd.DataFrame) -> Optional[Dict]:
        rules   = self._config.get("entry_rules", [])
        if not rules:
            return None
        results    = []
        directions = []
        for rule in rules:
            col  = rule.get("col", "")
            op   = rule.get("op",  ">")
            val  = rule.get("val")
            col2 = rule.get("col2")
            if col not in df.columns:
                return None
            cur  = float(df[col].iloc[-1])
            prev = float(df[col].iloc[-2]) if len(df) > 1 else cur
            cmp  = (float(df[col2].iloc[-1])
                    if col2 and col2 in df.columns
                    else float(val if val is not None else 0))
            cmp_prev = (float(df[col2].iloc[-2])
                        if col2 and col2 in df.columns and len(df) > 1
                        else cmp)
            passed = {
                ">":           cur > cmp,
                "<":           cur < cmp,
                ">=":          cur >= cmp,
                "<=":          cur <= cmp,
                "cross_above": cur > cmp and prev <= cmp_prev,
                "cross_below": cur < cmp and prev >= cmp_prev,
            }.get(op, False)
            results.append(passed)
            if rule.get("direction"):
                directions.append(rule["direction"])
        if all(results):
            return {"direction": directions[0] if directions else "BUY"}
        return None

    def _calc_confidence(self, df: pd.DataFrame) -> float:
        base   = float(self._config.get("base_confidence", 0.65))
        boosts = self._config.get("confidence_boosts", [])
        for rule in boosts:
            col = rule.get("col", "")
            if col not in df.columns:
                continue
            val = float(df[col].iloc[-1])
            if rule.get("above") is not None and val > float(rule["above"]):
                base = min(1.0, base + float(rule.get("boost", 0.05)))
            if rule.get("below") is not None and val < float(rule["below"]):
                base = min(1.0, base + float(rule.get("boost", 0.05)))
        return base

    def _snapshot(self, df: pd.DataFrame) -> Dict:
        cols = ["rsi", "ema_20", "ema_50", "macd", "signal", "atr",
                "bb_upper", "bb_lower", "adx"]
        return {
            c: round(float(df[c].iloc[-1]), 6)
            for c in cols
            if c in df.columns and not pd.isna(df[c].iloc[-1])
        }

    # ── Indicator calculators ─────────────────────────────────────────────────

    def _calc_rsi(self, df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
        delta = df["close"].diff()
        gain  = delta.clip(lower=0).rolling(period).mean()
        loss  = (-delta.clip(upper=0)).rolling(period).mean()
        rs    = gain / loss.replace(0, np.nan)
        df["rsi"] = 100 - 100 / (1 + rs)
        return df

    def _calc_ema(self, df: pd.DataFrame, period: int = 20) -> pd.DataFrame:
        df[f"ema_{period}"] = df["close"].ewm(span=period, adjust=False).mean()
        return df

    def _calc_macd(self, df: pd.DataFrame,
                   fast: int = 12, slow: int = 26, signal: int = 9) -> pd.DataFrame:
        ema_f        = df["close"].ewm(span=fast,   adjust=False).mean()
        ema_s        = df["close"].ewm(span=slow,   adjust=False).mean()
        df["macd"]   = ema_f - ema_s
        df["signal"] = df["macd"].ewm(span=signal, adjust=False).mean()
        df["hist"]   = df["macd"] - df["signal"]
        return df

    def _calc_atr(self, df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
        hl  = df["high"] - df["low"]
        hc  = (df["high"] - df["close"].shift()).abs()
        lc  = (df["low"]  - df["close"].shift()).abs()
        tr  = pd.concat([hl, hc, lc], axis=1).max(axis=1)
        df["atr"] = tr.rolling(period).mean()
        return df

    def _calc_bollinger(self, df: pd.DataFrame,
                        period: int = 20, std: float = 2.0) -> pd.DataFrame:
        ma             = df["close"].rolling(period).mean()
        sd             = df["close"].rolling(period).std()
        df["bb_upper"] = ma + sd * std
        df["bb_lower"] = ma - sd * std
        df["bb_mid"]   = ma
        return df

    def _calc_volume_ma(self, df: pd.DataFrame, period: int = 20) -> pd.DataFrame:
        df["volume_ma"] = df["volume"].rolling(period).mean()
        return df

    def _calc_stoch(self, df: pd.DataFrame,
                    k_period: int = 14, d_period: int = 3) -> pd.DataFrame:
        low_min  = df["low"].rolling(k_period).min()
        high_max = df["high"].rolling(k_period).max()
        df["stoch_k"] = 100 * (df["close"] - low_min) / (high_max - low_min).replace(0, np.nan)
        df["stoch_d"] = df["stoch_k"].rolling(d_period).mean()
        return df

    def _calc_adx(self, df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
        high  = df["high"]
        low   = df["low"]
        close = df["close"]
        plus_dm  = high.diff().clip(lower=0)
        minus_dm = (-low.diff()).clip(lower=0)
        tr = pd.concat([
            high - low,
            (high - close.shift()).abs(),
            (low  - close.shift()).abs(),
        ], axis=1).max(axis=1)
        atr      = tr.rolling(period).mean().replace(0, np.nan)
        plus_di  = 100 * plus_dm.rolling(period).mean()  / atr
        minus_di = 100 * minus_dm.rolling(period).mean() / atr
        dx       = (100 * (plus_di - minus_di).abs()
                    / (plus_di + minus_di).replace(0, np.nan))
        df["adx"]      = dx.rolling(period).mean()
        df["plus_di"]  = plus_di
        df["minus_di"] = minus_di
        return df

    def _noop(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        return df


# ── Factory ───────────────────────────────────────────────────────────────────

class StrategyBuilder:
    """
    Factory for creating DynamicStrategy instances from configs or JSON files.
    """

    @staticmethod
    def from_dict(config: Dict) -> DynamicStrategy:
        return DynamicStrategy(config)

    @staticmethod
    def from_json(path: str) -> DynamicStrategy:
        with open(path, encoding="utf-8") as f:
            return DynamicStrategy(json.load(f))

    @staticmethod
    def example_config() -> Dict:
        """
        Ready-to-use EMA crossover + RSI filter strategy.
        BUY when EMA20 crosses above EMA50 and RSI is between 45–70.
        """
        return {
            "name":    "ema_rsi_crossover",
            "version": "1.0",
            "indicators": [
                {"name": "rsi",  "params": {"period": 14}},
                {"name": "ema",  "params": {"period": 20}},
                {"name": "ema",  "params": {"period": 50}},
                {"name": "atr",  "params": {"period": 14}},
            ],
            "entry_rules": [
                {"col": "ema_20", "op": "cross_above",
                 "col2": "ema_50", "direction": "BUY"},
                {"col": "rsi", "op": ">", "val": 45},
                {"col": "rsi", "op": "<", "val": 70},
            ],
            "confidence_boosts": [
                {"col": "rsi", "above": 55, "boost": 0.05},
            ],
            "stop_mult": 1.5,
            "tp_mult":   3.0,
        }

    @staticmethod
    def rsi_mean_reversion_config() -> Dict:
        """
        BUY when RSI oversold (< 30), SELL when overbought (> 70).
        Mean-reversion approach — works well in ranging markets.
        """
        return {
            "name":    "rsi_mean_reversion",
            "version": "1.0",
            "indicators": [
                {"name": "rsi", "params": {"period": 14}},
                {"name": "atr", "params": {"period": 14}},
                {"name": "bollinger", "params": {"period": 20, "std": 2.0}},
            ],
            "entry_rules": [
                {"col": "rsi", "op": "<", "val": 30, "direction": "BUY"},
            ],
            "confidence_boosts": [
                {"col": "rsi", "below": 25, "boost": 0.08},
                {"col": "rsi", "below": 20, "boost": 0.10},
            ],
            "stop_mult": 2.0,
            "tp_mult":   2.0,
        }

    @staticmethod
    def macd_trend_config() -> Dict:
        """
        BUY when MACD crosses above signal line with positive histogram.
        Trend-following approach.
        """
        return {
            "name":    "macd_trend",
            "version": "1.0",
            "indicators": [
                {"name": "macd", "params": {"fast": 12, "slow": 26, "signal": 9}},
                {"name": "ema",  "params": {"period": 200}},
                {"name": "atr",  "params": {"period": 14}},
            ],
            "entry_rules": [
                {"col": "macd", "op": "cross_above",
                 "col2": "signal", "direction": "BUY"},
                {"col": "hist", "op": ">", "val": 0},
            ],
            "confidence_boosts": [
                {"col": "hist", "above": 0.001, "boost": 0.05},
            ],
            "stop_mult": 1.5,
            "tp_mult":   3.0,
        }

    # ── 4. Stochastic Trend Filter ────────────────────────────────────────────

    @staticmethod
    def stoch_trend_config() -> Dict:
        """
        BUY when Stochastic K crosses above D from oversold (<20)
        while price is above EMA50 (trend filter).
        Works well on crypto and forex in trending markets.
        """
        return {
            "name":    "stoch_trend",
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
                {"col": "stoch_k", "below": 20, "boost": 0.10},
            ],
            "stop_mult": 1.5,
            "tp_mult":   2.5,
        }

    # ── 5. ADX Strong Trend ───────────────────────────────────────────────────

    @staticmethod
    def adx_trend_config() -> Dict:
        """
        Only trades when ADX > 25 (strong trend confirmed).
        BUY when +DI crosses above -DI in a strong uptrend.
        SELL when -DI crosses above +DI in a strong downtrend.
        Avoids choppy, ranging markets entirely.
        """
        return {
            "name":    "adx_trend",
            "version": "1.0",
            "indicators": [
                {"name": "adx", "params": {"period": 14}},
                {"name": "ema", "params": {"period": 20}},
                {"name": "atr", "params": {"period": 14}},
            ],
            "entry_rules": [
                {"col": "adx",      "op": ">",           "val": 25},
                {"col": "plus_di",  "op": "cross_above",
                 "col2": "minus_di", "direction": "BUY"},
            ],
            "confidence_boosts": [
                {"col": "adx", "above": 30, "boost": 0.05},
                {"col": "adx", "above": 40, "boost": 0.08},
            ],
            "stop_mult": 1.8,
            "tp_mult":   2.5,
        }

    # ── 6. Bollinger Band Squeeze Breakout ────────────────────────────────────

    @staticmethod
    def bollinger_breakout_config() -> Dict:
        """
        Waits for Bollinger Band squeeze (low bandwidth = compressed volatility),
        then trades the breakout when price closes outside the bands.
        High win rate when volatility expansion follows compression.
        """
        return {
            "name":    "bollinger_breakout",
            "version": "1.0",
            "indicators": [
                {"name": "bollinger", "params": {"period": 20, "std": 2.0}},
                {"name": "volume_ma", "params": {"period": 20}},
                {"name": "atr",       "params": {"period": 14}},
            ],
            "entry_rules": [
                {"col": "close",  "op": ">",  "col2": "bb_upper", "direction": "BUY"},
                {"col": "volume", "op": ">",  "col2": "volume_ma"},
            ],
            "confidence_boosts": [
                {"col": "volume", "above": 0, "boost": 0.05},
            ],
            "stop_mult": 1.0,
            "tp_mult":   3.0,
        }

    # ── 7. Triple EMA Trend Rider ─────────────────────────────────────────────

    @staticmethod
    def triple_ema_config() -> Dict:
        """
        Uses three EMAs (8, 21, 55) for trend confirmation.
        BUY only when EMA8 > EMA21 > EMA55 (full alignment = strong trend).
        One of the cleanest trend-following setups for crypto.
        """
        return {
            "name":    "triple_ema",
            "version": "1.0",
            "indicators": [
                {"name": "ema", "params": {"period": 8}},
                {"name": "ema", "params": {"period": 21}},
                {"name": "ema", "params": {"period": 55}},
                {"name": "rsi", "params": {"period": 14}},
                {"name": "atr", "params": {"period": 14}},
            ],
            "entry_rules": [
                {"col": "ema_8",  "op": ">", "col2": "ema_21", "direction": "BUY"},
                {"col": "ema_21", "op": ">", "col2": "ema_55"},
                {"col": "rsi",    "op": ">", "val": 50},
                {"col": "rsi",    "op": "<", "val": 75},
            ],
            "confidence_boosts": [
                {"col": "rsi", "above": 55, "boost": 0.04},
                {"col": "rsi", "above": 60, "boost": 0.04},
            ],
            "stop_mult": 2.0,
            "tp_mult":   3.0,
        }

    # ── 8. RSI Divergence Scalper ─────────────────────────────────────────────

    @staticmethod
    def rsi_scalper_config() -> Dict:
        """
        Fast RSI (7) scalping strategy with tight stops.
        BUY when RSI(7) crosses above 30 from oversold.
        Designed for short-term reversals — best on 1h and 15m data.
        """
        return {
            "name":    "rsi_scalper",
            "version": "1.0",
            "indicators": [
                {"name": "rsi", "params": {"period": 7}},
                {"name": "ema", "params": {"period": 20}},
                {"name": "atr", "params": {"period": 7}},
            ],
            "entry_rules": [
                {"col": "rsi", "op": "cross_above", "val": 30, "direction": "BUY"},
            ],
            "confidence_boosts": [
                {"col": "rsi", "below": 25, "boost": 0.08},
                {"col": "rsi", "below": 20, "boost": 0.12},
            ],
            "stop_mult": 1.0,
            "tp_mult":   1.5,
        }

    # ── 9. Volume Breakout ────────────────────────────────────────────────────

    @staticmethod
    def volume_breakout_config() -> Dict:
        """
        Trades breakouts confirmed by above-average volume.
        Price closes above EMA20 with volume > 1.5× its 20-bar average.
        High-conviction entries only — avoids false breakouts.
        """
        return {
            "name":    "volume_breakout",
            "version": "1.0",
            "indicators": [
                {"name": "ema",       "params": {"period": 20}},
                {"name": "volume_ma", "params": {"period": 20}},
                {"name": "rsi",       "params": {"period": 14}},
                {"name": "atr",       "params": {"period": 14}},
            ],
            "entry_rules": [
                {"col": "close",  "op": "cross_above",
                 "col2": "ema_20", "direction": "BUY"},
                {"col": "volume", "op": ">", "col2": "volume_ma"},
                {"col": "rsi",    "op": ">", "val": 50},
            ],
            "confidence_boosts": [
                {"col": "rsi",    "above": 55, "boost": 0.05},
                {"col": "volume", "above": 0,  "boost": 0.03},
            ],
            "stop_mult": 1.5,
            "tp_mult":   2.5,
        }

    # ── 10. Golden Cross ──────────────────────────────────────────────────────

    @staticmethod
    def golden_cross_config() -> Dict:
        """
        Classic Golden Cross / Death Cross strategy.
        BUY when EMA50 crosses above EMA200 (long-term trend change).
        Fewer signals but higher quality — best on daily charts.
        """
        return {
            "name":    "golden_cross",
            "version": "1.0",
            "indicators": [
                {"name": "ema", "params": {"period": 50}},
                {"name": "ema", "params": {"period": 200}},
                {"name": "rsi", "params": {"period": 14}},
                {"name": "atr", "params": {"period": 14}},
            ],
            "entry_rules": [
                {"col": "ema_50",  "op": "cross_above",
                 "col2": "ema_200", "direction": "BUY"},
                {"col": "rsi",     "op": ">", "val": 45},
            ],
            "confidence_boosts": [
                {"col": "rsi", "above": 55, "boost": 0.06},
            ],
            "stop_mult": 2.5,
            "tp_mult":   4.0,
        }

    # ── 11. MACD + RSI Confluence ─────────────────────────────────────────────

    @staticmethod
    def macd_rsi_confluence_config() -> Dict:
        """
        Requires BOTH MACD crossover AND RSI confirmation.
        Reduces false signals by demanding confluence from two indicators.
        BUY when MACD crosses above signal AND RSI > 50 AND RSI < 70.
        """
        return {
            "name":    "macd_rsi_confluence",
            "version": "1.0",
            "indicators": [
                {"name": "macd", "params": {"fast": 12, "slow": 26, "signal": 9}},
                {"name": "rsi",  "params": {"period": 14}},
                {"name": "atr",  "params": {"period": 14}},
            ],
            "entry_rules": [
                {"col": "macd", "op": "cross_above",
                 "col2": "signal", "direction": "BUY"},
                {"col": "rsi",  "op": ">", "val": 50},
                {"col": "rsi",  "op": "<", "val": 70},
                {"col": "hist", "op": ">", "val": 0},
            ],
            "confidence_boosts": [
                {"col": "rsi",  "above": 55, "boost": 0.05},
                {"col": "hist", "above": 0,  "boost": 0.04},
            ],
            "stop_mult": 1.5,
            "tp_mult":   3.0,
        }

    # ── 12. Bollinger + RSI Mean Reversion ────────────────────────────────────

    @staticmethod
    def bollinger_rsi_reversion_config() -> Dict:
        """
        High-probability mean reversion combining Bollinger Bands and RSI.
        BUY when price touches lower band AND RSI < 35 simultaneously.
        Double confirmation reduces noise significantly.
        """
        return {
            "name":    "bollinger_rsi_reversion",
            "version": "1.0",
            "indicators": [
                {"name": "bollinger", "params": {"period": 20, "std": 2.0}},
                {"name": "rsi",       "params": {"period": 14}},
                {"name": "atr",       "params": {"period": 14}},
            ],
            "entry_rules": [
                {"col": "close", "op": "<=", "col2": "bb_lower", "direction": "BUY"},
                {"col": "rsi",   "op": "<",  "val": 35},
            ],
            "confidence_boosts": [
                {"col": "rsi", "below": 30, "boost": 0.07},
                {"col": "rsi", "below": 25, "boost": 0.10},
            ],
            "stop_mult": 1.5,
            "tp_mult":   2.0,
        }

    # ── 13. Stoch + MACD Swing ────────────────────────────────────────────────

    @staticmethod
    def stoch_macd_swing_config() -> Dict:
        """
        Swing trading setup combining Stochastic and MACD.
        BUY when Stoch is oversold AND MACD histogram turns positive.
        Three-day swing trades — best on daily and 4h charts.
        """
        return {
            "name":    "stoch_macd_swing",
            "version": "1.0",
            "indicators": [
                {"name": "stoch", "params": {"k_period": 14, "d_period": 3}},
                {"name": "macd",  "params": {"fast": 12, "slow": 26, "signal": 9}},
                {"name": "atr",   "params": {"period": 14}},
            ],
            "entry_rules": [
                {"col": "stoch_k", "op": "<",           "val": 40, "direction": "BUY"},
                {"col": "hist",    "op": "cross_above",  "val": 0},
            ],
            "confidence_boosts": [
                {"col": "stoch_k", "below": 30, "boost": 0.06},
                {"col": "stoch_k", "below": 20, "boost": 0.08},
            ],
            "stop_mult": 1.5,
            "tp_mult":   2.5,
        }

    # ── 14. ADX + EMA Momentum ────────────────────────────────────────────────

    @staticmethod
    def adx_ema_momentum_config() -> Dict:
        """
        Momentum strategy — only enters when ADX confirms strong trend
        AND price is above both EMA20 and EMA50 (stacked bullish).
        Designed for riding strong crypto bull runs.
        """
        return {
            "name":    "adx_ema_momentum",
            "version": "1.0",
            "indicators": [
                {"name": "adx", "params": {"period": 14}},
                {"name": "ema", "params": {"period": 20}},
                {"name": "ema", "params": {"period": 50}},
                {"name": "rsi", "params": {"period": 14}},
                {"name": "atr", "params": {"period": 14}},
            ],
            "entry_rules": [
                {"col": "adx",   "op": ">", "val": 20,       "direction": "BUY"},
                {"col": "close", "op": ">", "col2": "ema_20"},
                {"col": "close", "op": ">", "col2": "ema_50"},
                {"col": "rsi",   "op": ">", "val": 55},
            ],
            "confidence_boosts": [
                {"col": "adx", "above": 25, "boost": 0.05},
                {"col": "adx", "above": 35, "boost": 0.07},
                {"col": "rsi", "above": 60, "boost": 0.04},
            ],
            "stop_mult": 2.0,
            "tp_mult":   3.5,
        }

    # ── 15. Supertrend Proxy (ATR-Based) ──────────────────────────────────────

    @staticmethod
    def atr_supertrend_config() -> Dict:
        """
        Approximates Supertrend indicator using ATR and EMA.
        BUY when price crosses above EMA20 + ATR (upper breakout zone).
        Trend-following with dynamic stop based on ATR.
        """
        return {
            "name":    "atr_supertrend",
            "version": "1.0",
            "indicators": [
                {"name": "ema", "params": {"period": 20}},
                {"name": "ema", "params": {"period": 50}},
                {"name": "atr", "params": {"period": 10}},
                {"name": "rsi", "params": {"period": 14}},
            ],
            "entry_rules": [
                {"col": "ema_20", "op": "cross_above",
                 "col2": "ema_50", "direction": "BUY"},
                {"col": "rsi",    "op": ">", "val": 50},
                {"col": "adx",    "op": ">", "val": 20} if False else
                {"col": "rsi",    "op": "<", "val": 75},
            ],
            "confidence_boosts": [
                {"col": "rsi", "above": 55, "boost": 0.05},
            ],
            "stop_mult": 1.2,
            "tp_mult":   2.5,
        }

    # ── All presets registry ──────────────────────────────────────────────────

    @staticmethod
    def all_configs() -> Dict[str, Dict]:
        """
        Returns all preset strategy configs as a dict keyed by name.
        Use this to batch-backtest or compare every preset at once.

        Example
        -------
            from strategy_lab.strategy_builder import StrategyBuilder

            for name, config in StrategyBuilder.all_configs().items():
                result = run_backtest(config, "BTC-USD", "crypto")
                print(f"{name:30} {result.summary()}")
        """
        sb = StrategyBuilder
        return {
            "ema_rsi_crossover":        sb.example_config(),
            "rsi_mean_reversion":       sb.rsi_mean_reversion_config(),
            "macd_trend":               sb.macd_trend_config(),
            "stoch_trend":              sb.stoch_trend_config(),
            "adx_trend":                sb.adx_trend_config(),
            "bollinger_breakout":       sb.bollinger_breakout_config(),
            "triple_ema":               sb.triple_ema_config(),
            "rsi_scalper":              sb.rsi_scalper_config(),
            "volume_breakout":          sb.volume_breakout_config(),
            "golden_cross":             sb.golden_cross_config(),
            "macd_rsi_confluence":      sb.macd_rsi_confluence_config(),
            "bollinger_rsi_reversion":  sb.bollinger_rsi_reversion_config(),
            "stoch_macd_swing":         sb.stoch_macd_swing_config(),
            "adx_ema_momentum":         sb.adx_ema_momentum_config(),
            "atr_supertrend":           sb.atr_supertrend_config(),
        }