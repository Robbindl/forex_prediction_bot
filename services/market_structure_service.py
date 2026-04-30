from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping, Optional

import numpy as np

import pandas as pd

from core.asset_profiles import (
    is_australia_index,
    is_europe_index,
    is_japan_index,
    is_uk_index,
    is_us_index,
)
from utils.logger import get_logger

logger = get_logger()

_FRAME_WEIGHTS = {
    "1m": 0.15,
    "5m": 0.25,
    "15m": 0.45,
    "30m": 0.40,
    "1h": 0.35,
    "4h": 0.20,
    "1d": 0.10,
}

_VOLATILITY_FIT = {
    "calm": 0.65,
    "normal": 1.00,
    "expansion": 0.85,
    "extreme": 0.35,
}

_ANCHOR_INTERVAL_PRIORITY = ("5m", "15m", "1m", "30m", "1h")

_SESSION_FIT_BY_CATEGORY: Dict[str, Dict[str, float]] = {
    "forex": {
        "asia_core": 0.72,
        "europe_open": 0.92,
        "europe_core": 1.00,
        "us_overlap": 0.98,
        "us_open": 0.92,
        "us_core": 0.88,
        "off": 0.25,
    },
    "commodities": {
        "asia_core": 0.45,
        "europe_open": 0.88,
        "europe_core": 0.96,
        "us_overlap": 1.00,
        "us_open": 1.00,
        "us_core": 0.90,
        "off": 0.25,
    },
    "indices": {
        "asia_core": 0.35,
        "europe_open": 0.55,
        "europe_core": 0.62,
        "us_overlap": 0.98,
        "us_open": 1.00,
        "us_core": 0.94,
        "off": 0.25,
    },
    "crypto": {
        "asia_core": 0.86,
        "europe_open": 0.92,
        "europe_core": 0.96,
        "us_overlap": 1.00,
        "us_open": 1.00,
        "us_core": 0.96,
        "off": 0.55,
    },
}

_SESSION_FIT_BY_ASSET: Dict[str, Dict[str, float]] = {
    "US30": {"asia_core": 0.30, "europe_open": 0.42, "europe_core": 0.48, "us_overlap": 0.98, "us_open": 1.00, "us_core": 0.96, "off": 0.20},
    "US100": {"asia_core": 0.30, "europe_open": 0.42, "europe_core": 0.48, "us_overlap": 0.98, "us_open": 1.00, "us_core": 0.96, "off": 0.20},
    "US500": {"asia_core": 0.30, "europe_open": 0.42, "europe_core": 0.48, "us_overlap": 0.98, "us_open": 1.00, "us_core": 0.96, "off": 0.20},
    "UK100": {"asia_core": 0.40, "europe_open": 1.00, "europe_core": 1.00, "us_overlap": 0.90, "us_open": 0.60, "us_core": 0.50, "off": 0.20},
    "GER40": {"asia_core": 0.40, "europe_open": 1.00, "europe_core": 1.00, "us_overlap": 0.90, "us_open": 0.60, "us_core": 0.50, "off": 0.20},
    "AUS200": {"asia_core": 1.00, "europe_open": 0.82, "europe_core": 0.55, "us_overlap": 0.40, "us_open": 0.34, "us_core": 0.30, "off": 0.20},
    "JPN225": {"asia_core": 1.00, "europe_open": 0.84, "europe_core": 0.58, "us_overlap": 0.42, "us_open": 0.36, "us_core": 0.32, "off": 0.20},
    "WTI": {"asia_core": 0.25, "europe_open": 0.35, "europe_core": 0.45, "us_overlap": 1.00, "us_open": 1.00, "us_core": 0.92, "off": 0.20},
    "XAU/USD": {"asia_core": 0.52, "europe_open": 0.96, "europe_core": 1.00, "us_overlap": 1.00, "us_open": 0.98, "us_core": 0.86, "off": 0.20},
    "XAG/USD": {"asia_core": 0.50, "europe_open": 0.94, "europe_core": 1.00, "us_overlap": 1.00, "us_open": 0.98, "us_core": 0.86, "off": 0.20},
    "EUR/USD": {"asia_core": 0.62, "europe_open": 0.95, "europe_core": 1.00, "us_overlap": 0.98, "us_open": 0.92, "us_core": 0.88, "off": 0.20},
    "GBP/USD": {"asia_core": 0.62, "europe_open": 0.95, "europe_core": 1.00, "us_overlap": 0.98, "us_open": 0.92, "us_core": 0.88, "off": 0.20},
    "EUR/GBP": {"asia_core": 0.60, "europe_open": 0.98, "europe_core": 1.00, "us_overlap": 0.90, "us_open": 0.82, "us_core": 0.78, "off": 0.20},
    "USD/CAD": {"asia_core": 0.60, "europe_open": 0.88, "europe_core": 0.94, "us_overlap": 1.00, "us_open": 0.98, "us_core": 0.94, "off": 0.20},
    "USD/CHF": {"asia_core": 0.60, "europe_open": 0.92, "europe_core": 0.98, "us_overlap": 0.98, "us_open": 0.92, "us_core": 0.88, "off": 0.20},
    "USD/JPY": {"asia_core": 0.88, "europe_open": 0.95, "europe_core": 0.94, "us_overlap": 0.96, "us_open": 0.90, "us_core": 0.84, "off": 0.20},
    "EUR/JPY": {"asia_core": 0.84, "europe_open": 0.94, "europe_core": 0.96, "us_overlap": 0.96, "us_open": 0.90, "us_core": 0.84, "off": 0.20},
    "GBP/JPY": {"asia_core": 0.84, "europe_open": 0.96, "europe_core": 0.98, "us_overlap": 0.96, "us_open": 0.90, "us_core": 0.84, "off": 0.20},
    "AUD/USD": {"asia_core": 0.94, "europe_open": 0.86, "europe_core": 0.80, "us_overlap": 0.78, "us_open": 0.74, "us_core": 0.70, "off": 0.20},
    "NZD/USD": {"asia_core": 0.94, "europe_open": 0.86, "europe_core": 0.80, "us_overlap": 0.78, "us_open": 0.74, "us_core": 0.70, "off": 0.20},
}

_SESSION_STRUCTURE_PROFILES: Dict[str, Dict[str, Dict[str, Any]]] = {
    "forex": {
        "asia_core": {
            "mode": "range_balance",
            "breakout_multiplier": -0.18,
            "pullback_multiplier": 0.10,
            "confirmation_delta": 1,
            "extension_limit_delta": -0.10,
            "target_efficiency_delta": -0.03,
            "candle_quality_delta": 0.02,
            "max_impulse_age_delta": -1,
            "anchor_weight": 1.10,
        },
        "europe_open": {
            "mode": "asia_range_break",
            "breakout_multiplier": 0.16,
            "pullback_multiplier": -0.02,
            "confirmation_delta": 0,
            "extension_limit_delta": 0.06,
            "target_efficiency_delta": 0.03,
            "candle_quality_delta": 0.00,
            "max_impulse_age_delta": 0,
            "anchor_weight": 1.22,
        },
        "europe_core": {
            "mode": "london_trend",
            "breakout_multiplier": 0.10,
            "pullback_multiplier": 0.08,
            "confirmation_delta": 0,
            "extension_limit_delta": 0.04,
            "target_efficiency_delta": 0.02,
            "candle_quality_delta": 0.00,
            "max_impulse_age_delta": 0,
            "anchor_weight": 1.18,
        },
        "us_overlap": {
            "mode": "overlap_expansion",
            "breakout_multiplier": 0.14,
            "pullback_multiplier": 0.04,
            "confirmation_delta": 0,
            "extension_limit_delta": 0.05,
            "target_efficiency_delta": 0.03,
            "candle_quality_delta": 0.00,
            "max_impulse_age_delta": 0,
            "anchor_weight": 1.16,
        },
        "us_open": {
            "mode": "london_reversal_or_follow",
            "breakout_multiplier": 0.06,
            "pullback_multiplier": 0.10,
            "confirmation_delta": 0,
            "extension_limit_delta": 0.02,
            "target_efficiency_delta": 0.02,
            "candle_quality_delta": 0.01,
            "max_impulse_age_delta": 0,
            "anchor_weight": 1.14,
        },
        "us_core": {
            "mode": "continuation",
            "breakout_multiplier": 0.03,
            "pullback_multiplier": 0.05,
            "confirmation_delta": 0,
            "extension_limit_delta": 0.00,
            "target_efficiency_delta": 0.01,
            "candle_quality_delta": 0.00,
            "max_impulse_age_delta": 0,
            "anchor_weight": 1.05,
        },
        "off": {
            "mode": "dead_hours",
            "breakout_multiplier": -0.22,
            "pullback_multiplier": -0.10,
            "confirmation_delta": 1,
            "extension_limit_delta": -0.14,
            "target_efficiency_delta": 0.05,
            "candle_quality_delta": 0.03,
            "max_impulse_age_delta": -1,
            "anchor_weight": 0.96,
        },
    },
    "indices": {
        "asia_core": {
            "mode": "closed_or_thin",
            "breakout_multiplier": -0.24,
            "pullback_multiplier": -0.10,
            "confirmation_delta": 1,
            "extension_limit_delta": -0.16,
            "target_efficiency_delta": 0.05,
            "candle_quality_delta": 0.03,
            "max_impulse_age_delta": -1,
            "anchor_weight": 0.95,
        },
        "europe_open": {
            "mode": "opening_range_breakout",
            "breakout_multiplier": 0.18,
            "pullback_multiplier": -0.04,
            "confirmation_delta": 0,
            "extension_limit_delta": 0.08,
            "target_efficiency_delta": 0.04,
            "candle_quality_delta": 0.01,
            "max_impulse_age_delta": 0,
            "anchor_weight": 1.28,
        },
        "europe_core": {
            "mode": "cash_session_trend",
            "breakout_multiplier": 0.10,
            "pullback_multiplier": 0.04,
            "confirmation_delta": 0,
            "extension_limit_delta": 0.04,
            "target_efficiency_delta": 0.02,
            "candle_quality_delta": 0.00,
            "max_impulse_age_delta": 0,
            "anchor_weight": 1.18,
        },
        "us_overlap": {
            "mode": "overlap_trend",
            "breakout_multiplier": 0.12,
            "pullback_multiplier": 0.02,
            "confirmation_delta": 0,
            "extension_limit_delta": 0.04,
            "target_efficiency_delta": 0.03,
            "candle_quality_delta": 0.00,
            "max_impulse_age_delta": 0,
            "anchor_weight": 1.20,
        },
        "us_open": {
            "mode": "opening_range_breakout",
            "breakout_multiplier": 0.20,
            "pullback_multiplier": -0.02,
            "confirmation_delta": 0,
            "extension_limit_delta": 0.10,
            "target_efficiency_delta": 0.04,
            "candle_quality_delta": 0.01,
            "max_impulse_age_delta": 0,
            "anchor_weight": 1.30,
        },
        "us_core": {
            "mode": "cash_session_continuation",
            "breakout_multiplier": 0.08,
            "pullback_multiplier": 0.04,
            "confirmation_delta": 0,
            "extension_limit_delta": 0.03,
            "target_efficiency_delta": 0.02,
            "candle_quality_delta": 0.00,
            "max_impulse_age_delta": 0,
            "anchor_weight": 1.14,
        },
        "off": {
            "mode": "dead_hours",
            "breakout_multiplier": -0.26,
            "pullback_multiplier": -0.12,
            "confirmation_delta": 1,
            "extension_limit_delta": -0.18,
            "target_efficiency_delta": 0.06,
            "candle_quality_delta": 0.04,
            "max_impulse_age_delta": -1,
            "anchor_weight": 0.94,
        },
    },
    "commodities": {
        "asia_core": {
            "mode": "thin_metals_or_oil",
            "breakout_multiplier": -0.16,
            "pullback_multiplier": -0.02,
            "confirmation_delta": 1,
            "extension_limit_delta": -0.10,
            "target_efficiency_delta": 0.03,
            "candle_quality_delta": 0.02,
            "max_impulse_age_delta": -1,
            "anchor_weight": 1.00,
        },
        "europe_open": {
            "mode": "metals_fix_build",
            "breakout_multiplier": 0.12,
            "pullback_multiplier": 0.02,
            "confirmation_delta": 0,
            "extension_limit_delta": 0.05,
            "target_efficiency_delta": 0.03,
            "candle_quality_delta": 0.00,
            "max_impulse_age_delta": 0,
            "anchor_weight": 1.22,
        },
        "europe_core": {
            "mode": "metals_london_trend",
            "breakout_multiplier": 0.10,
            "pullback_multiplier": 0.04,
            "confirmation_delta": 0,
            "extension_limit_delta": 0.04,
            "target_efficiency_delta": 0.02,
            "candle_quality_delta": 0.00,
            "max_impulse_age_delta": 0,
            "anchor_weight": 1.18,
        },
        "us_overlap": {
            "mode": "oil_and_metals_expansion",
            "breakout_multiplier": 0.14,
            "pullback_multiplier": 0.02,
            "confirmation_delta": 0,
            "extension_limit_delta": 0.06,
            "target_efficiency_delta": 0.03,
            "candle_quality_delta": 0.00,
            "max_impulse_age_delta": 0,
            "anchor_weight": 1.24,
        },
        "us_open": {
            "mode": "oil_cash_open",
            "breakout_multiplier": 0.16,
            "pullback_multiplier": 0.00,
            "confirmation_delta": 0,
            "extension_limit_delta": 0.06,
            "target_efficiency_delta": 0.03,
            "candle_quality_delta": 0.00,
            "max_impulse_age_delta": 0,
            "anchor_weight": 1.24,
        },
        "us_core": {
            "mode": "commodity_continuation",
            "breakout_multiplier": 0.06,
            "pullback_multiplier": 0.04,
            "confirmation_delta": 0,
            "extension_limit_delta": 0.02,
            "target_efficiency_delta": 0.01,
            "candle_quality_delta": 0.00,
            "max_impulse_age_delta": 0,
            "anchor_weight": 1.10,
        },
        "off": {
            "mode": "dead_hours",
            "breakout_multiplier": -0.22,
            "pullback_multiplier": -0.08,
            "confirmation_delta": 1,
            "extension_limit_delta": -0.16,
            "target_efficiency_delta": 0.05,
            "candle_quality_delta": 0.03,
            "max_impulse_age_delta": -1,
            "anchor_weight": 0.96,
        },
    },
    "crypto": {
        "asia_core": {
            "mode": "accumulation_reclaim",
            "breakout_multiplier": -0.08,
            "pullback_multiplier": 0.10,
            "confirmation_delta": 1,
            "extension_limit_delta": -0.06,
            "target_efficiency_delta": 0.01,
            "candle_quality_delta": 0.01,
            "max_impulse_age_delta": 0,
            "anchor_weight": 1.08,
        },
        "europe_open": {
            "mode": "europe_reacceleration",
            "breakout_multiplier": 0.04,
            "pullback_multiplier": 0.04,
            "confirmation_delta": 0,
            "extension_limit_delta": 0.02,
            "target_efficiency_delta": 0.01,
            "candle_quality_delta": 0.00,
            "max_impulse_age_delta": 0,
            "anchor_weight": 1.06,
        },
        "europe_core": {
            "mode": "balanced",
            "breakout_multiplier": 0.02,
            "pullback_multiplier": 0.05,
            "confirmation_delta": 0,
            "extension_limit_delta": 0.01,
            "target_efficiency_delta": 0.00,
            "candle_quality_delta": 0.00,
            "max_impulse_age_delta": 0,
            "anchor_weight": 1.02,
        },
        "us_overlap": {
            "mode": "trend_confirmation",
            "breakout_multiplier": 0.10,
            "pullback_multiplier": 0.02,
            "confirmation_delta": 0,
            "extension_limit_delta": 0.04,
            "target_efficiency_delta": 0.02,
            "candle_quality_delta": 0.00,
            "max_impulse_age_delta": 0,
            "anchor_weight": 1.12,
        },
        "us_open": {
            "mode": "trend_confirmation",
            "breakout_multiplier": 0.12,
            "pullback_multiplier": 0.00,
            "confirmation_delta": 0,
            "extension_limit_delta": 0.05,
            "target_efficiency_delta": 0.02,
            "candle_quality_delta": 0.00,
            "max_impulse_age_delta": 0,
            "anchor_weight": 1.14,
        },
        "us_core": {
            "mode": "continuation",
            "breakout_multiplier": 0.08,
            "pullback_multiplier": 0.02,
            "confirmation_delta": 0,
            "extension_limit_delta": 0.03,
            "target_efficiency_delta": 0.01,
            "candle_quality_delta": 0.00,
            "max_impulse_age_delta": 0,
            "anchor_weight": 1.10,
        },
        "off": {
            "mode": "weekend_or_thin",
            "breakout_multiplier": -0.12,
            "pullback_multiplier": 0.00,
            "confirmation_delta": 1,
            "extension_limit_delta": -0.08,
            "target_efficiency_delta": 0.02,
            "candle_quality_delta": 0.02,
            "max_impulse_age_delta": 0,
            "anchor_weight": 0.98,
        },
    },
}


def _clip(value: float, lo: float = -1.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, float(value)))


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value if value is not None else default)
    except Exception:
        return default


def _trend_state_sign(state: Any) -> int:
    label = str(state or "").strip().lower()
    if label in {"trending_up", "buy", "bullish", "up"}:
        return 1
    if label in {"trending_down", "sell", "bearish", "down"}:
        return -1
    return 0


def _to_float_series(df: pd.DataFrame, column: str) -> Optional[pd.Series]:
    try:
        return df[column].astype(float)
    except Exception:
        return None


def _estimate_atr(df: pd.DataFrame, period: int = 14) -> float:
    if len(df) < period + 1:
        return 0.0
    high = _to_float_series(df, "high")
    low = _to_float_series(df, "low")
    close = _to_float_series(df, "close")
    if high is None or low is None or close is None:
        return 0.0
    prev_close = close.shift(1)
    tr_components = pd.concat(
        [
            high - low,
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    )
    tr = pd.Series(tr_components.max(axis=1), index=tr_components.index, dtype=float)
    try:
        return float(tr.tail(period).mean())
    except Exception:
        return 0.0


def _estimate_vwap(df: pd.DataFrame, window: int = 30) -> float:
    if len(df) < 5:
        return 0.0
    high = _to_float_series(df, "high")
    low = _to_float_series(df, "low")
    close = _to_float_series(df, "close")
    if high is None or low is None or close is None:
        return 0.0
    volume = _to_float_series(df, "volume")
    if volume is None:
        volume = pd.Series(np.ones(len(df), dtype=float), index=df.index)
    tail = df.tail(window)
    typical = ((high.loc[tail.index] + low.loc[tail.index] + close.loc[tail.index]) / 3.0).astype(float)
    vol = pd.Series(volume.loc[tail.index].abs().astype(float), index=tail.index, dtype=float)
    vol = pd.Series(np.where(vol.to_numpy() == 0.0, np.nan, vol.to_numpy()), index=tail.index, dtype=float)
    try:
        weighted = float((typical * vol).sum())
        vol_sum = float(vol.sum())
        if np.isnan(weighted) or np.isnan(vol_sum) or vol_sum <= 0:
            return float(typical.iloc[-1])
        return float(weighted / vol_sum)
    except Exception:
        return 0.0


def _timeframe_minutes(interval: str) -> int:
    label = str(interval or "").strip().lower()
    mapping = {
        "1m": 1,
        "5m": 5,
        "15m": 15,
        "30m": 30,
        "1h": 60,
        "4h": 240,
        "1d": 1440,
    }
    return int(mapping.get(label, 15))


def _adx_metrics(df: pd.DataFrame, period: int = 14) -> Dict[str, float]:
    if len(df) < period + 2:
        return {"adx": 0.0, "plus_di": 0.0, "minus_di": 0.0, "adx_slope": 0.0}

    high = _to_float_series(df, "high")
    low = _to_float_series(df, "low")
    close = _to_float_series(df, "close")
    if high is None or low is None or close is None:
        return {"adx": 0.0, "plus_di": 0.0, "minus_di": 0.0, "adx_slope": 0.0}

    up_move = high.diff()
    down_move = -low.diff()
    plus_dm = pd.Series(
        np.where((up_move > down_move) & (up_move > 0), up_move, 0.0),
        index=high.index,
        dtype=float,
    )
    minus_dm = pd.Series(
        np.where((down_move > up_move) & (down_move > 0), down_move, 0.0),
        index=high.index,
        dtype=float,
    )

    prev_close = close.shift(1)
    tr_components = pd.concat(
        [
            high - low,
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    )
    tr = pd.Series(tr_components.max(axis=1), index=tr_components.index, dtype=float)
    atr = tr.ewm(alpha=1.0 / float(period), adjust=False, min_periods=period).mean()
    plus = plus_dm.ewm(alpha=1.0 / float(period), adjust=False, min_periods=period).mean()
    minus = minus_dm.ewm(alpha=1.0 / float(period), adjust=False, min_periods=period).mean()

    atr_nonzero = atr.replace(0.0, np.nan)
    plus_di = (100.0 * plus / atr_nonzero).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    minus_di = (100.0 * minus / atr_nonzero).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    di_sum = (plus_di + minus_di).replace(0.0, np.nan)
    dx = (100.0 * (plus_di - minus_di).abs() / di_sum).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    adx = dx.ewm(alpha=1.0 / float(period), adjust=False, min_periods=period).mean().fillna(0.0)

    try:
        current_adx = float(adx.iloc[-1])
        current_plus = float(plus_di.iloc[-1])
        current_minus = float(minus_di.iloc[-1])
        slope_lookback = min(4, len(adx) - 1)
        slope = 0.0
        if slope_lookback > 0:
            slope = current_adx - float(adx.iloc[-1 - slope_lookback])
        return {
            "adx": max(0.0, current_adx),
            "plus_di": max(0.0, current_plus),
            "minus_di": max(0.0, current_minus),
            "adx_slope": slope,
        }
    except Exception:
        return {"adx": 0.0, "plus_di": 0.0, "minus_di": 0.0, "adx_slope": 0.0}


def _utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)


def _frame_timestamp_series(df: pd.DataFrame) -> Optional[pd.Series]:
    try:
        if isinstance(df.index, pd.DatetimeIndex):
            idx = pd.DatetimeIndex(df.index)
        elif "timestamp" in df.columns:
            idx = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
        else:
            return None
        if idx.tz is None:
            idx = idx.tz_localize(timezone.utc)
        else:
            idx = idx.tz_convert(timezone.utc)
        return pd.Series(idx, index=df.index)
    except Exception:
        return None


def _anchor_frame(intervals: Mapping[str, pd.DataFrame]) -> tuple[str, Optional[pd.DataFrame]]:
    for interval in _ANCHOR_INTERVAL_PRIORITY:
        frame = intervals.get(interval)
        if isinstance(frame, pd.DataFrame) and not frame.empty:
            return interval, frame
    for interval, frame in intervals.items():
        if isinstance(frame, pd.DataFrame) and not frame.empty:
            return str(interval), frame
    return "", None


def _empty_anchor_context(anchor_type: str = "", label: str = "", interval: str = "") -> Dict[str, Any]:
    return {
        "type": anchor_type,
        "label": label,
        "interval": interval,
        "ready": False,
        "state": "unavailable",
        "bias": "neutral",
        "direction_score": 0.0,
        "high": 0.0,
        "low": 0.0,
        "mid": 0.0,
        "range_pct": 0.0,
    }


def _summarize_anchor_window(
    frame: pd.DataFrame,
    *,
    anchor_mask: pd.Series,
    post_mask: pd.Series,
    anchor_type: str,
    label: str,
    interval: str,
    ready: bool,
) -> Dict[str, Any]:
    anchor = frame.loc[anchor_mask]
    if anchor.empty:
        return _empty_anchor_context(anchor_type, label, interval)

    anchor_high = float(anchor["high"].max())
    anchor_low = float(anchor["low"].min())
    current = float(frame["close"].iloc[-1])
    anchor_mid = (anchor_high + anchor_low) / 2.0
    anchor_range = max(anchor_high - anchor_low, current * 0.0001, 1e-9)
    tolerance = max(anchor_range * 0.08, current * 0.00015)

    state = "forming" if not ready else "inside_range"
    bias = "neutral"
    direction_score = 0.0

    if ready:
        post = frame.loc[post_mask]
        prior_post = post.iloc[:-1] if len(post) > 1 else post.iloc[0:0]
        prior_above = bool(not prior_post.empty and float(prior_post["close"].max()) > anchor_high + tolerance * 0.20)
        prior_below = bool(not prior_post.empty and float(prior_post["close"].min()) < anchor_low - tolerance * 0.20)
        post_high = float(post["high"].max()) if not post.empty else current
        post_low = float(post["low"].min()) if not post.empty else current

        if current > anchor_high + tolerance * 0.20:
            state = "holding_above" if prior_above else "breakout_above"
            bias = "buy"
            direction_score = 0.68 if prior_above else 0.48
        elif current < anchor_low - tolerance * 0.20:
            state = "holding_below" if prior_below else "breakdown_below"
            bias = "sell"
            direction_score = -0.68 if prior_below else -0.48
        elif post_high > anchor_high + tolerance and current <= anchor_high:
            state = "failed_breakout"
            bias = "sell"
            direction_score = -0.74
        elif post_low < anchor_low - tolerance and current >= anchor_low:
            state = "failed_breakdown"
            bias = "buy"
            direction_score = 0.74

    return {
        "type": anchor_type,
        "label": label,
        "interval": interval,
        "ready": bool(ready),
        "state": state,
        "bias": bias,
        "direction_score": round(direction_score, 4),
        "high": round(anchor_high, 6),
        "low": round(anchor_low, 6),
        "mid": round(anchor_mid, 6),
        "range_pct": round(anchor_range / max(current, 1e-9), 6),
    }


def _asian_range_context(df: pd.DataFrame, *, interval: str) -> Dict[str, Any]:
    ts = _frame_timestamp_series(df)
    if ts is None:
        return _empty_anchor_context("asia_range", "Asian range", interval)

    high = _to_float_series(df, "high")
    low = _to_float_series(df, "low")
    close = _to_float_series(df, "close")
    if high is None or low is None or close is None:
        return _empty_anchor_context("asia_range", "Asian range", interval)

    frame = (
        pd.DataFrame({"timestamp": ts, "high": high, "low": low, "close": close})
        .dropna()
        .reset_index(drop=True)
        .sort_values("timestamp")
        .reset_index(drop=True)
    )
    if frame.empty:
        return _empty_anchor_context("asia_range", "Asian range", interval)

    current_ts = pd.Timestamp(frame["timestamp"].iloc[-1]).tz_convert("UTC")
    day_start = current_ts.normalize()
    asia_end = day_start + pd.Timedelta(hours=6)
    anchor_cutoff = min(current_ts, asia_end)
    anchor_mask = (frame["timestamp"] >= day_start) & (frame["timestamp"] < anchor_cutoff)
    ready = bool(current_ts >= asia_end)
    post_mask = frame["timestamp"] >= asia_end
    return _summarize_anchor_window(
        frame,
        anchor_mask=anchor_mask,
        post_mask=post_mask,
        anchor_type="asia_range",
        label="Asian range",
        interval=interval,
        ready=ready,
    )


def _index_open_profile(asset: str) -> tuple[str, int, int, str]:
    if is_us_index(asset):
        return "America/New_York", 9, 30, "opening range"
    if is_uk_index(asset):
        return "Europe/London", 8, 0, "opening range"
    if is_europe_index(asset):
        return "Europe/Berlin", 9, 0, "opening range"
    if is_australia_index(asset):
        return "Australia/Sydney", 10, 0, "opening range"
    if is_japan_index(asset):
        return "Asia/Tokyo", 9, 0, "opening range"
    return "", 0, 0, "opening range"


def _opening_range_context(asset: str, df: pd.DataFrame, *, interval: str) -> Dict[str, Any]:
    timezone_name, open_hour, open_minute, label = _index_open_profile(asset)
    if not timezone_name:
        return _empty_anchor_context("opening_range", label, interval)

    ts = _frame_timestamp_series(df)
    if ts is None:
        return _empty_anchor_context("opening_range", label, interval)

    high = _to_float_series(df, "high")
    low = _to_float_series(df, "low")
    close = _to_float_series(df, "close")
    if high is None or low is None or close is None:
        return _empty_anchor_context("opening_range", label, interval)

    frame = (
        pd.DataFrame({"timestamp": ts, "high": high, "low": low, "close": close})
        .dropna()
        .reset_index(drop=True)
        .sort_values("timestamp")
        .reset_index(drop=True)
    )
    if frame.empty:
        return _empty_anchor_context("opening_range", label, interval)

    local_ts = frame["timestamp"].dt.tz_convert(timezone_name)
    current_local = pd.Timestamp(local_ts.iloc[-1])
    open_start = current_local.normalize() + pd.Timedelta(hours=open_hour, minutes=open_minute)
    open_end = open_start + pd.Timedelta(minutes=15)
    if current_local < open_start:
        return _empty_anchor_context("opening_range", label, interval)

    anchor_cutoff = min(current_local, open_end)
    anchor_mask = (local_ts >= open_start) & (local_ts < anchor_cutoff)
    ready = bool(current_local >= open_end)
    post_mask = local_ts >= open_end
    return _summarize_anchor_window(
        frame,
        anchor_mask=anchor_mask,
        post_mask=post_mask,
        anchor_type="opening_range",
        label=label,
        interval=interval,
        ready=ready,
    )


def _commodity_anchor_profile(asset: str) -> tuple[str, int, int, int, str, str]:
    canonical = str(asset or "").strip().upper()
    if canonical in {"XAU/USD", "XAG/USD"}:
        return "Europe/London", 8, 0, 60, "metals_london_range", "London metals range"
    if canonical == "WTI":
        return "America/New_York", 9, 0, 30, "oil_opening_range", "US oil opening range"
    return "", 0, 0, 0, "", ""


def _commodity_anchor_context(asset: str, df: pd.DataFrame, *, interval: str) -> Dict[str, Any]:
    timezone_name, open_hour, open_minute, window_minutes, anchor_type, label = _commodity_anchor_profile(asset)
    if not timezone_name:
        return _empty_anchor_context("commodity_anchor", "commodity anchor", interval)

    ts = _frame_timestamp_series(df)
    if ts is None:
        return _empty_anchor_context(anchor_type, label, interval)

    high = _to_float_series(df, "high")
    low = _to_float_series(df, "low")
    close = _to_float_series(df, "close")
    if high is None or low is None or close is None:
        return _empty_anchor_context(anchor_type, label, interval)

    frame = (
        pd.DataFrame({"timestamp": ts, "high": high, "low": low, "close": close})
        .dropna()
        .reset_index(drop=True)
        .sort_values("timestamp")
        .reset_index(drop=True)
    )
    if frame.empty:
        return _empty_anchor_context(anchor_type, label, interval)

    local_ts = frame["timestamp"].dt.tz_convert(timezone_name)
    current_local = pd.Timestamp(local_ts.iloc[-1])
    open_start = current_local.normalize() + pd.Timedelta(hours=open_hour, minutes=open_minute)
    open_end = open_start + pd.Timedelta(minutes=window_minutes)
    if current_local < open_start:
        return _empty_anchor_context(anchor_type, label, interval)

    anchor_cutoff = min(current_local, open_end)
    anchor_mask = (local_ts >= open_start) & (local_ts < anchor_cutoff)
    ready = bool(current_local >= open_end)
    post_mask = local_ts >= open_end
    return _summarize_anchor_window(
        frame,
        anchor_mask=anchor_mask,
        post_mask=post_mask,
        anchor_type=anchor_type,
        label=label,
        interval=interval,
        ready=ready,
    )


def _commodity_event_profiles(asset: str) -> List[Dict[str, Any]]:
    canonical = str(asset or "").strip().upper()
    if canonical == "XAU/USD":
        return [
            {
                "timezone": "Europe/London",
                "hour": 10,
                "minute": 30,
                "pre_minutes": 60,
                "post_minutes": 180,
                "anchor_type": "lbma_gold_am_fix",
                "label": "LBMA Gold AM fix",
            },
            {
                "timezone": "Europe/London",
                "hour": 15,
                "minute": 0,
                "pre_minutes": 60,
                "post_minutes": 180,
                "anchor_type": "lbma_gold_pm_fix",
                "label": "LBMA Gold PM fix",
            },
        ]
    if canonical == "XAG/USD":
        return [
            {
                "timezone": "Europe/London",
                "hour": 12,
                "minute": 0,
                "pre_minutes": 60,
                "post_minutes": 180,
                "anchor_type": "lbma_silver_fix",
                "label": "LBMA Silver fix",
            },
        ]
    if canonical == "WTI":
        return [
            {
                "timezone": "America/New_York",
                "weekday": 2,
                "hour": 10,
                "minute": 30,
                "pre_minutes": 60,
                "post_minutes": 240,
                "anchor_type": "eia_crude_release",
                "label": "EIA crude release",
            },
        ]
    return []


def _event_reset_context(asset: str, df: pd.DataFrame, *, interval: str) -> Dict[str, Any]:
    profiles = _commodity_event_profiles(asset)
    if not profiles:
        return _empty_anchor_context("event_reset", "event reset", interval)

    ts = _frame_timestamp_series(df)
    if ts is None:
        return _empty_anchor_context("event_reset", "event reset", interval)

    high = _to_float_series(df, "high")
    low = _to_float_series(df, "low")
    close = _to_float_series(df, "close")
    if high is None or low is None or close is None:
        return _empty_anchor_context("event_reset", "event reset", interval)

    frame = (
        pd.DataFrame({"timestamp": ts, "high": high, "low": low, "close": close})
        .dropna()
        .reset_index(drop=True)
        .sort_values("timestamp")
        .reset_index(drop=True)
    )
    if frame.empty:
        return _empty_anchor_context("event_reset", "event reset", interval)

    selected: Dict[str, Any] = {}
    for profile in profiles:
        timezone_name = str(profile.get("timezone") or "")
        if not timezone_name:
            continue
        local_ts = frame["timestamp"].dt.tz_convert(timezone_name)
        current_local = pd.Timestamp(local_ts.iloc[-1])
        event_time = current_local.normalize() + pd.Timedelta(
            hours=int(profile.get("hour", 0) or 0),
            minutes=int(profile.get("minute", 0) or 0),
        )
        weekday = profile.get("weekday")
        if weekday is not None and int(current_local.weekday()) != int(weekday):
            continue
        post_minutes = int(profile.get("post_minutes", 0) or 0)
        pre_minutes = int(profile.get("pre_minutes", 0) or 0)
        if current_local < event_time or current_local > event_time + pd.Timedelta(minutes=post_minutes):
            continue

        anchor_start = event_time - pd.Timedelta(minutes=pre_minutes)
        anchor_cutoff = min(current_local, event_time)
        anchor_mask = (local_ts >= anchor_start) & (local_ts < anchor_cutoff)
        post_mask = (local_ts >= event_time) & (local_ts <= current_local)
        ready = bool(current_local >= event_time)
        context = _summarize_anchor_window(
            frame,
            anchor_mask=anchor_mask,
            post_mask=post_mask,
            anchor_type=str(profile.get("anchor_type") or "event_reset"),
            label=str(profile.get("label") or "event reset"),
            interval=interval,
            ready=ready,
        )
        if not context.get("ready"):
            continue
        context["event_timestamp"] = event_time.tz_convert("UTC").isoformat()
        context["event_window_minutes"] = post_minutes
        selected = context
    return selected or _empty_anchor_context("event_reset", "event reset", interval)


def _crypto_daily_open_context(df: pd.DataFrame, *, interval: str) -> Dict[str, Any]:
    ts = _frame_timestamp_series(df)
    if ts is None:
        return _empty_anchor_context("crypto_daily_open_range", "Crypto daily open range", interval)

    high = _to_float_series(df, "high")
    low = _to_float_series(df, "low")
    close = _to_float_series(df, "close")
    if high is None or low is None or close is None:
        return _empty_anchor_context("crypto_daily_open_range", "Crypto daily open range", interval)

    frame = (
        pd.DataFrame({"timestamp": ts, "high": high, "low": low, "close": close})
        .dropna()
        .reset_index(drop=True)
        .sort_values("timestamp")
        .reset_index(drop=True)
    )
    if frame.empty:
        return _empty_anchor_context("crypto_daily_open_range", "Crypto daily open range", interval)

    current_ts = pd.Timestamp(frame["timestamp"].iloc[-1]).tz_convert("UTC")
    day_start = current_ts.normalize()
    open_end = day_start + pd.Timedelta(minutes=60)
    anchor_cutoff = min(current_ts, open_end)
    anchor_mask = (frame["timestamp"] >= day_start) & (frame["timestamp"] < anchor_cutoff)
    ready = bool(current_ts >= open_end)
    post_mask = frame["timestamp"] >= open_end
    return _summarize_anchor_window(
        frame,
        anchor_mask=anchor_mask,
        post_mask=post_mask,
        anchor_type="crypto_daily_open_range",
        label="Crypto daily open range",
        interval=interval,
        ready=ready,
    )


def _crypto_weekly_open_context(df: pd.DataFrame, *, interval: str) -> Dict[str, Any]:
    ts = _frame_timestamp_series(df)
    if ts is None:
        return _empty_anchor_context("crypto_weekly_open", "Crypto weekly open", interval)

    open_series = _to_float_series(df, "open")
    high = _to_float_series(df, "high")
    low = _to_float_series(df, "low")
    close = _to_float_series(df, "close")
    if open_series is None or high is None or low is None or close is None:
        return _empty_anchor_context("crypto_weekly_open", "Crypto weekly open", interval)

    frame = (
        pd.DataFrame(
            {
                "timestamp": ts,
                "open": open_series,
                "high": high,
                "low": low,
                "close": close,
            }
        )
        .dropna()
        .reset_index(drop=True)
        .sort_values("timestamp")
        .reset_index(drop=True)
    )
    if frame.empty:
        return _empty_anchor_context("crypto_weekly_open", "Crypto weekly open", interval)

    current_ts = pd.Timestamp(frame["timestamp"].iloc[-1]).tz_convert("UTC")
    week_start = current_ts.normalize() - pd.Timedelta(days=int(current_ts.weekday()))
    week_frame = frame.loc[frame["timestamp"] >= week_start].reset_index(drop=True)
    if week_frame.empty:
        return _empty_anchor_context("crypto_weekly_open", "Crypto weekly open", interval)

    anchor_price = float(week_frame["open"].iloc[0])
    current = float(week_frame["close"].iloc[-1])
    tolerance = max(current * 0.0012, abs(anchor_price) * 0.0010, 1e-9)
    state = "above_open" if current > anchor_price + tolerance * 0.15 else "below_open" if current < anchor_price - tolerance * 0.15 else "at_open"
    bias = "buy" if state == "above_open" else "sell" if state == "below_open" else "neutral"
    direction_score = 0.34 if state == "above_open" else -0.34 if state == "below_open" else 0.0

    post = week_frame.iloc[1:]
    if not post.empty:
        post_high = float(post["high"].max())
        post_low = float(post["low"].min())
        if post_high > anchor_price + tolerance and current <= anchor_price + tolerance * 0.05:
            state = "failed_breakout"
            bias = "sell"
            direction_score = -0.56
        elif post_low < anchor_price - tolerance and current >= anchor_price - tolerance * 0.05:
            state = "failed_breakdown"
            bias = "buy"
            direction_score = 0.56

    return {
        "type": "crypto_weekly_open",
        "label": "Crypto weekly open",
        "interval": interval,
        "ready": True,
        "state": state,
        "bias": bias,
        "direction_score": round(direction_score, 4),
        "high": round(anchor_price, 6),
        "low": round(anchor_price, 6),
        "mid": round(anchor_price, 6),
        "range_pct": 0.0,
    }


def _crypto_anchor_context(asset: str, df: pd.DataFrame, *, interval: str) -> Dict[str, Any]:
    daily = _crypto_daily_open_context(df, interval=interval)
    weekly = _crypto_weekly_open_context(df, interval=interval)
    daily_score = _safe_float(daily.get("direction_score", 0.0), 0.0)
    weekly_score = _safe_float(weekly.get("direction_score", 0.0), 0.0)
    combined_score = daily_score * 0.65 + weekly_score * 0.35
    if daily_score and weekly_score and (daily_score > 0) == (weekly_score > 0):
        combined_score += 0.08 if combined_score > 0 else -0.08
    combined_score = _clip(combined_score, -0.92, 0.92)

    anchor_ready = bool(daily.get("ready") or weekly.get("ready"))
    anchor_bias = "buy" if combined_score > 0.08 else "sell" if combined_score < -0.08 else "neutral"
    primary = daily if abs(daily_score) >= abs(weekly_score) else weekly
    anchor_state = str(primary.get("state") or "unavailable")
    if anchor_bias == "buy" and daily_score > 0 and weekly_score > 0:
        anchor_state = "stacked_buy_support"
    elif anchor_bias == "sell" and daily_score < 0 and weekly_score < 0:
        anchor_state = "stacked_sell_support"

    return {
        "type": "crypto_open_stack",
        "label": "Crypto daily/weekly open stack",
        "interval": interval,
        "ready": anchor_ready,
        "state": anchor_state,
        "bias": anchor_bias,
        "direction_score": round(combined_score, 4),
        "high": daily.get("high", weekly.get("high", 0.0)),
        "low": daily.get("low", weekly.get("low", 0.0)),
        "mid": daily.get("mid", weekly.get("mid", 0.0)),
        "range_pct": max(
            float(daily.get("range_pct", 0.0) or 0.0),
            float(weekly.get("range_pct", 0.0) or 0.0),
        ),
        "daily_anchor_type": str(daily.get("type") or ""),
        "daily_anchor_state": str(daily.get("state") or "unavailable"),
        "daily_anchor_bias": str(daily.get("bias") or "neutral"),
        "daily_anchor_direction_score": round(daily_score, 4),
        "weekly_anchor_type": str(weekly.get("type") or ""),
        "weekly_anchor_state": str(weekly.get("state") or "unavailable"),
        "weekly_anchor_bias": str(weekly.get("bias") or "neutral"),
        "weekly_anchor_direction_score": round(weekly_score, 4),
    }


def _session_anchor_context(asset: str, category: str, frames: Mapping[str, pd.DataFrame]) -> Dict[str, Any]:
    interval, frame = _anchor_frame(frames)
    if frame is None:
        return _empty_anchor_context()

    category_key = str(category or "").strip().lower()
    if category_key == "forex":
        return _asian_range_context(frame, interval=interval)
    if category_key == "indices":
        return _opening_range_context(asset, frame, interval=interval)
    if category_key == "commodities":
        return _commodity_anchor_context(asset, frame, interval=interval)
    if category_key == "crypto":
        return _crypto_anchor_context(asset, frame, interval=interval)
    return _empty_anchor_context()


def _dedupe_sorted_levels(levels: List[float], *, reverse: bool = False) -> List[float]:
    ordered = sorted(
        [float(level) for level in levels if isinstance(level, (int, float)) and float(level) > 0.0],
        reverse=reverse,
    )
    deduped: List[float] = []
    for level in ordered:
        if deduped and abs(level - deduped[-1]) <= max(abs(level), abs(deduped[-1]), 1.0) * 0.00005:
            continue
        deduped.append(round(level, 6))
    return deduped


def _structure_levels(
    *,
    current_price: float,
    primary: Dict[str, Any],
    session_anchor: Dict[str, Any],
    event_anchor: Dict[str, Any],
    atr: float,
) -> Dict[str, Any]:
    support_levels: List[float] = []
    resistance_levels: List[float] = []

    for raw in [primary.get("support"), primary.get("session_anchor_low"), session_anchor.get("low"), event_anchor.get("low")]:
        level = _safe_float(raw, 0.0)
        if 0.0 < level < current_price * 1.5:
            support_levels.append(level)
    for raw in [primary.get("resistance"), primary.get("session_anchor_high"), session_anchor.get("high"), event_anchor.get("high")]:
        level = _safe_float(raw, 0.0)
        if 0.0 < level < current_price * 1.5:
            resistance_levels.append(level)

    session_range = max(_safe_float(session_anchor.get("high"), 0.0) - _safe_float(session_anchor.get("low"), 0.0), 0.0)
    event_range = max(_safe_float(event_anchor.get("high"), 0.0) - _safe_float(event_anchor.get("low"), 0.0), 0.0)
    projection_range = max(session_range, event_range, atr, current_price * 0.001)

    if current_price > 0:
        anchor_high = _safe_float(session_anchor.get("high"), 0.0)
        anchor_low = _safe_float(session_anchor.get("low"), 0.0)
        event_high = _safe_float(event_anchor.get("high"), 0.0)
        event_low = _safe_float(event_anchor.get("low"), 0.0)

        if anchor_high > 0 and anchor_high >= current_price * 0.995:
            resistance_levels.append(anchor_high + projection_range)
        if event_high > 0 and event_high >= current_price * 0.995:
            resistance_levels.append(event_high + projection_range)
        if anchor_low > 0 and anchor_low <= current_price * 1.005:
            support_levels.append(max(0.0, anchor_low - projection_range))
        if event_low > 0 and event_low <= current_price * 1.005:
            support_levels.append(max(0.0, event_low - projection_range))

    support_levels = _dedupe_sorted_levels(support_levels, reverse=True)
    resistance_levels = _dedupe_sorted_levels(resistance_levels, reverse=False)
    invalid_below = next((level for level in support_levels if level < current_price), 0.0)
    invalid_above = next((level for level in resistance_levels if level > current_price), 0.0)
    bullish_targets = [level for level in resistance_levels if level > current_price]
    bearish_targets = [level for level in support_levels if level < current_price]

    return {
        "support_levels": support_levels,
        "resistance_levels": resistance_levels,
        "invalid_below": round(invalid_below, 6) if invalid_below > 0 else 0.0,
        "invalid_above": round(invalid_above, 6) if invalid_above > 0 else 0.0,
        "bullish_target_levels": [round(level, 6) for level in bullish_targets[:4]],
        "bearish_target_levels": [round(level, 6) for level in bearish_targets[:4]],
    }


def _active_session(*, category: str = "") -> str:
    return _session_phase_at_timestamp(_utc_now(), category=category)


def _session_phase_at_timestamp(timestamp: Any, *, category: str = "") -> str:
    try:
        now = pd.Timestamp(timestamp)
        if now.tzinfo is None:
            now = now.tz_localize("UTC")
        else:
            now = now.tz_convert("UTC")
    except Exception:
        now = pd.Timestamp(_utc_now())
    hour = now.hour
    weekday = now.weekday()
    category_key = str(category or "").strip().lower()
    if category_key == "crypto":
        if 0 <= hour < 6:
            return "asia_core"
        if 6 <= hour < 14:
            return "europe_open" if hour < 8 else "europe_core"
        if 14 <= hour < 16:
            return "us_overlap"
        if 16 <= hour < 19:
            return "us_open"
        return "us_core"
    if weekday == 5 or weekday == 6:
        if weekday == 6 and hour >= 22:
            return "asia_core"
        return "off"
    if weekday == 4 and hour >= 22:
        return "off"
    if 0 <= hour < 6:
        return "asia_core"
    if 6 <= hour < 8:
        return "europe_open"
    if 8 <= hour < 13:
        return "europe_core"
    if 13 <= hour < 15:
        return "us_overlap"
    if 15 <= hour < 17:
        return "us_open"
    if 17 <= hour < 22:
        return "us_core"
    return "off"


def _session_context_fit(asset: str, category: str, session: str) -> float:
    canonical = str(asset or "").strip().upper()
    session_key = str(session or "").strip().lower() or "off"
    asset_fit = _SESSION_FIT_BY_ASSET.get(canonical)
    if isinstance(asset_fit, dict) and session_key in asset_fit:
        return float(asset_fit[session_key])
    category_fit = _SESSION_FIT_BY_CATEGORY.get(str(category or "").strip().lower(), {})
    return float(category_fit.get(session_key, 0.60))


def _session_quality(
    interval: str,
    atr_pct: float,
    *,
    asset: str = "",
    category: str = "",
    session_phase: str = "",
) -> tuple[str, float]:
    london_intervals = {"5m", "15m", "30m", "1h"}
    scalp_intervals = {"1m", "5m"}
    if atr_pct >= 0.020:
        base_label, base_score = "chaotic", 0.26
    elif atr_pct <= 0.0018 and interval in scalp_intervals:
        base_label, base_score = "dead", 0.34
    elif atr_pct <= 0.0025 and interval in london_intervals:
        base_label, base_score = "quiet", 0.52
    elif 0.0025 < atr_pct <= 0.012:
        base_label, base_score = "active", 0.82
    elif 0.012 < atr_pct <= 0.020:
        base_label, base_score = "fast", 0.66
    else:
        base_label, base_score = "mixed", 0.58

    session = str(session_phase or "").strip().lower() or _active_session(category=category)
    fit = _clip(_session_context_fit(asset, category, session), 0.0, 1.0)
    adjusted_score = round(float(base_score) * fit, 4)

    if session == "off" or fit <= 0.40:
        adjusted_label = "off_session"
    elif fit <= 0.60 and base_label in {"active", "fast", "mixed"}:
        adjusted_label = "thin_session"
    else:
        adjusted_label = base_label
    return adjusted_label, adjusted_score


def _bar_metrics(
    current: float,
    bar_open: float,
    bar_high: float,
    bar_low: float,
    prev_high: float,
    prev_low: float,
    atr_ref: float,
) -> Dict[str, float]:
    candle_range = max(bar_high - bar_low, 1e-9)
    body = abs(current - bar_open)
    upper_wick = max(0.0, bar_high - max(current, bar_open))
    lower_wick = max(0.0, min(current, bar_open) - bar_low)
    close_location = (current - bar_low) / candle_range
    return {
        "candle_range_atr": candle_range / max(atr_ref, 1e-9),
        "body_ratio": body / candle_range,
        "upper_wick_ratio": upper_wick / candle_range,
        "lower_wick_ratio": lower_wick / candle_range,
        "close_location": close_location,
        "broke_prev_high": float(bar_high > prev_high),
        "broke_prev_low": float(bar_low < prev_low),
    }


def _pattern_family(
    trend_state: str,
    breakout_retest_ready: bool,
    first_pullback_ready: bool,
    liquidity_sweep_buy: bool,
    liquidity_sweep_sell: bool,
    failed_opposite_move_confirmed: bool,
) -> str:
    if failed_opposite_move_confirmed:
        return f"{trend_state}_failed_opposite_reclaim"
    if breakout_retest_ready:
        return f"{trend_state}_breakout_retest"
    if first_pullback_ready:
        return f"{trend_state}_first_pullback"
    if liquidity_sweep_buy or liquidity_sweep_sell:
        return f"{trend_state}_liquidity_sweep"
    return f"{trend_state}_generic"


def _regime_entry_policy(regime: str) -> Dict[str, float]:
    if regime in {"trending_up", "trending_down"}:
        return {
            "min_setup_quality": 0.32,
            "min_candle_quality": 0.34,
            "max_extension_score": 1.28,
            "min_target_efficiency": 0.26,
            "max_impulse_age_bars": 5.0,
            "confirmation_bars": 1.0,
        }
    if regime == "volatile":
        return {
            "min_setup_quality": 0.42,
            "min_candle_quality": 0.42,
            "max_extension_score": 1.05,
            "min_target_efficiency": 0.36,
            "max_impulse_age_bars": 4.0,
            "confirmation_bars": 2.0,
        }
    return {
        "min_setup_quality": 0.40,
        "min_candle_quality": 0.38,
        "max_extension_score": 1.00,
        "min_target_efficiency": 0.34,
        "max_impulse_age_bars": 4.0,
        "confirmation_bars": 2.0,
    }


def _session_structure_profile(category: str, session_phase: str) -> Dict[str, Any]:
    category_key = str(category or "").strip().lower()
    session_key = str(session_phase or "off").strip().lower()
    profile = dict((_SESSION_STRUCTURE_PROFILES.get(category_key) or {}).get(session_key) or {})
    if profile:
        return profile
    return {
        "mode": "balanced",
        "breakout_multiplier": 0.0,
        "pullback_multiplier": 0.0,
        "confirmation_delta": 0,
        "extension_limit_delta": 0.0,
        "target_efficiency_delta": 0.0,
        "candle_quality_delta": 0.0,
        "max_impulse_age_delta": 0,
        "anchor_weight": 1.0,
    }


def _session_adjusted_entry_policy(regime: str, session_profile: Dict[str, Any], trend_phase: str) -> Dict[str, float]:
    base = dict(_regime_entry_policy(regime))
    confirmation_delta = int(session_profile.get("confirmation_delta", 0) or 0)
    max_impulse_age_delta = int(session_profile.get("max_impulse_age_delta", 0) or 0)

    if trend_phase in {"trend_exhausted", "transition"}:
        base["confirmation_bars"] += 1.0
        base["max_extension_score"] -= 0.08
        base["min_target_efficiency"] += 0.03
        base["min_candle_quality"] += 0.02
        base["max_impulse_age_bars"] -= 1.0
    elif trend_phase == "trend_building":
        base["max_extension_score"] += 0.04
        base["min_target_efficiency"] -= 0.02

    base["confirmation_bars"] = float(max(1.0, base["confirmation_bars"] + confirmation_delta))
    base["max_extension_score"] = float(max(0.70, base["max_extension_score"] + float(session_profile.get("extension_limit_delta", 0.0) or 0.0)))
    base["min_target_efficiency"] = float(_clip(base["min_target_efficiency"] + float(session_profile.get("target_efficiency_delta", 0.0) or 0.0), 0.10, 0.90))
    base["min_candle_quality"] = float(_clip(base["min_candle_quality"] + float(session_profile.get("candle_quality_delta", 0.0) or 0.0), 0.18, 0.90))
    base["max_impulse_age_bars"] = float(max(2.0, base["max_impulse_age_bars"] + max_impulse_age_delta))
    return base


def _frame_state_from_score(score: float) -> str:
    if score >= 0.18:
        return "trending_up"
    if score <= -0.18:
        return "trending_down"
    return "ranging"


def _volatility_state(atr_pct: float) -> str:
    if atr_pct <= 0.003:
        return "calm"
    if atr_pct <= 0.010:
        return "normal"
    if atr_pct <= 0.022:
        return "expansion"
    return "extreme"


def _analyze_frame(interval: str, df: pd.DataFrame, *, asset: str = "", category: str = "") -> Optional[Dict[str, Any]]:
    if len(df) < 30:
        return None

    close = _to_float_series(df, "close")
    high = _to_float_series(df, "high")
    low = _to_float_series(df, "low")
    open_ = _to_float_series(df, "open")
    if close is None or high is None or low is None or open_ is None:
        return None

    current = float(close.iloc[-1])
    if current <= 0:
        return None

    fast_span = 12 if interval in {"1m", "5m"} else 20
    slow_span = 30 if interval in {"1m", "5m"} else 50
    fast = close.ewm(span=fast_span, adjust=False).mean()
    slow = close.ewm(span=slow_span, adjust=False).mean()
    adx_data = _adx_metrics(df)

    lookback = min(8, len(close) - 1)
    slope = 0.0
    if lookback > 0:
        try:
            slope = (float(fast.iloc[-1]) - float(fast.iloc[-1 - lookback])) / current
        except Exception:
            slope = 0.0
    ema_gap = (float(fast.iloc[-1]) - float(slow.iloc[-1])) / current
    directional_pressure = _clip(
        (float(adx_data.get("plus_di", 0.0) or 0.0) - float(adx_data.get("minus_di", 0.0) or 0.0)) / 35.0
    )
    trend_score = _clip(((ema_gap * 42.0) + (slope * 135.0)) * 0.72 + directional_pressure * 0.28)
    trend_state = _frame_state_from_score(trend_score)

    atr = _estimate_atr(df)
    atr_ref = max(atr, current * 0.001, 1e-9)
    atr_pct = atr / current if current else 0.0
    vol_state = _volatility_state(atr_pct)
    timestamp_series = _frame_timestamp_series(df)
    frame_timestamp = timestamp_series.iloc[-1] if timestamp_series is not None and not timestamp_series.empty else _utc_now()
    session_phase = _session_phase_at_timestamp(frame_timestamp, category=category)
    session_label, session_quality = _session_quality(
        interval,
        atr_pct,
        asset=asset,
        category=category,
        session_phase=session_phase,
    )
    session_profile = _session_structure_profile(category, session_phase)
    session_mode = str(session_profile.get("mode") or "balanced")

    recent_window = min(20, len(df))
    recent_high = float(high.tail(recent_window).max())
    recent_low = float(low.tail(recent_window).min())
    prev_window_high = float(high.iloc[-recent_window:-1].max()) if recent_window > 1 else recent_high
    prev_window_low = float(low.iloc[-recent_window:-1].min()) if recent_window > 1 else recent_low
    support = recent_low
    resistance = recent_high
    distance_to_support = max(0.0, (current - support) / current)
    distance_to_resistance = max(0.0, (resistance - current) / current)

    near_high = 1.0 - min(1.0, max(0.0, (recent_high - current) / atr_ref))
    near_low = 1.0 - min(1.0, max(0.0, (current - recent_low) / atr_ref))
    breakout_score = 0.0
    if trend_state == "trending_up":
        breakout_score = near_high
    elif trend_state == "trending_down":
        breakout_score = -near_low
    else:
        if near_high >= 0.80:
            breakout_score = min(0.55, near_high * 0.55)
        elif near_low >= 0.80:
            breakout_score = -min(0.55, near_low * 0.55)
    breakout_score *= max(0.40, 1.0 + float(session_profile.get("breakout_multiplier", 0.0) or 0.0))

    pullback_score = 0.0
    fast_level = float(fast.iloc[-1])
    slow_level = float(slow.iloc[-1])
    pullback_proximity = 1.0 - min(1.0, abs(current - fast_level) / (atr_ref * 1.6))
    if trend_state == "trending_up" and current >= slow_level:
        pullback_score = pullback_proximity
    elif trend_state == "trending_down" and current <= slow_level:
        pullback_score = -pullback_proximity
    pullback_score *= max(0.40, 1.0 + float(session_profile.get("pullback_multiplier", 0.0) or 0.0))

    upside_extension_fast = max(0.0, (current - fast_level) / atr_ref)
    downside_extension_fast = max(0.0, (fast_level - current) / atr_ref)
    upside_extension_slow = max(0.0, (current - slow_level) / atr_ref)
    downside_extension_slow = max(0.0, (slow_level - current) / atr_ref)

    upside_exhaustion = 0.0
    downside_exhaustion = 0.0
    if trend_state == "trending_up":
        upside_exhaustion = _clip(
            _clip((upside_extension_fast - 0.85) / 1.10) * 0.42
            + _clip((upside_extension_slow - 1.35) / 1.35) * 0.34
            + _clip((near_high - 0.82) / 0.18) * 0.16
            + (0.08 if vol_state in {"expansion", "extreme"} else 0.0)
        )
    elif trend_state == "trending_down":
        downside_exhaustion = _clip(
            _clip((downside_extension_fast - 0.85) / 1.10) * 0.42
            + _clip((downside_extension_slow - 1.35) / 1.35) * 0.34
            + _clip((near_low - 0.82) / 0.18) * 0.16
            + (0.08 if vol_state in {"expansion", "extreme"} else 0.0)
        )

    vwap = _estimate_vwap(df, window=min(36, len(df)))
    vwap_distance = ((current - vwap) / atr_ref) if vwap > 0 else 0.0

    bar_high = float(high.iloc[-1])
    bar_low = float(low.iloc[-1])
    bar_open = float(open_.iloc[-1])
    candle = _bar_metrics(
        current=current,
        bar_open=bar_open,
        bar_high=bar_high,
        bar_low=bar_low,
        prev_high=prev_window_high,
        prev_low=prev_window_low,
        atr_ref=atr_ref,
    )
    close_location = float(candle["close_location"])
    prev_close = float(close.iloc[-2]) if len(close) >= 2 else current
    prev_bar_open = float(open_.iloc[-2]) if len(open_) >= 2 else prev_close
    prev_bar_high = float(high.iloc[-2]) if len(high) >= 2 else bar_high
    prev_bar_low = float(low.iloc[-2]) if len(low) >= 2 else bar_low
    prev_close_location = (
        (prev_close - prev_bar_low) / max(prev_bar_high - prev_bar_low, 1e-9)
        if len(close) >= 2
        else close_location
    )

    liquidity_sweep_buy = candle["broke_prev_low"] >= 1.0 and close_location >= 0.58
    liquidity_sweep_sell = candle["broke_prev_high"] >= 1.0 and close_location <= 0.42
    breakout_retest_ready = False
    if trend_state == "trending_up":
        breakout_retest_ready = bool(
            bar_low <= prev_window_high + atr_ref * 0.18
            and current >= prev_window_high
            and close_location >= 0.55
        )
    elif trend_state == "trending_down":
        breakout_retest_ready = bool(
            bar_high >= prev_window_low - atr_ref * 0.18
            and current <= prev_window_low
            and close_location <= 0.45
        )

    impulse_window = min(9, len(close) - 1)
    impulse_age_bars = 0
    if impulse_window > 1:
        ref_fast = fast.tail(impulse_window + 1).astype(float)
        ref_close = close.tail(impulse_window + 1).astype(float)
        for idx in range(len(ref_close) - 1, 0, -1):
            aligned = (
                ref_close.iloc[idx] >= ref_fast.iloc[idx]
                if trend_state == "trending_up"
                else ref_close.iloc[idx] <= ref_fast.iloc[idx]
                if trend_state == "trending_down"
                else False
            )
            if aligned:
                impulse_age_bars += 1
            else:
                break

    first_pullback_ready = False
    if trend_state == "trending_up":
        first_pullback_ready = bool(
            pullback_score >= 0.38
            and close_location >= 0.52
            and upside_extension_fast <= 0.95
            and impulse_age_bars <= 4
        )
    elif trend_state == "trending_down":
        first_pullback_ready = bool(
            abs(pullback_score) >= 0.38
            and close_location <= 0.48
            and downside_extension_fast <= 0.95
            and impulse_age_bars <= 4
        )

    failed_opposite_move_confirmed = False
    if trend_state == "trending_up":
        failed_opposite_move_confirmed = bool(
            prev_bar_low < prev_window_low
            and prev_close_location <= 0.42
            and bar_low >= prev_bar_low
            and current > prev_close
            and close_location >= 0.60
        )
    elif trend_state == "trending_down":
        failed_opposite_move_confirmed = bool(
            prev_bar_high > prev_window_high
            and prev_close_location >= 0.58
            and bar_high <= prev_bar_high
            and current < prev_close
            and close_location <= 0.40
        )

    candle_quality_score = 0.0
    if trend_state == "trending_up":
        candle_quality_score = _clip(
            candle["body_ratio"] * 0.45
            + close_location * 0.35
            - candle["upper_wick_ratio"] * 0.30
            - _clip(candle["candle_range_atr"] - 1.55, 0.0, 1.0) * 0.22,
            0.0,
            1.0,
        )
    elif trend_state == "trending_down":
        candle_quality_score = _clip(
            candle["body_ratio"] * 0.45
            + (1.0 - close_location) * 0.35
            - candle["lower_wick_ratio"] * 0.30
            - _clip(candle["candle_range_atr"] - 1.55, 0.0, 1.0) * 0.22,
            0.0,
            1.0,
        )
    else:
        candle_quality_score = _clip(
            candle["body_ratio"] * 0.35
            + (1.0 - abs(close_location - 0.5) * 2.0) * 0.20
            - abs(vwap_distance) * 0.08,
            0.0,
            1.0,
        )
    candle_quality_score = _clip(
        candle_quality_score + float(session_profile.get("candle_quality_delta", 0.0) or 0.0),
        0.0,
        1.0,
    )

    extension_score = max(abs(vwap_distance) / 2.0, upside_extension_fast, downside_extension_fast)
    target_efficiency = 0.0
    if trend_state == "trending_up":
        target_efficiency = _clip((distance_to_resistance * current) / max(atr_ref * 1.2, 1e-9), 0.0, 1.0)
    elif trend_state == "trending_down":
        target_efficiency = _clip((distance_to_support * current) / max(atr_ref * 1.2, 1e-9), 0.0, 1.0)

    dominant_exhaustion = upside_exhaustion if trend_state == "trending_up" else downside_exhaustion if trend_state == "trending_down" else max(upside_exhaustion, downside_exhaustion)
    adx_value = float(adx_data.get("adx", 0.0) or 0.0)
    adx_slope = float(adx_data.get("adx_slope", 0.0) or 0.0)
    di_spread = abs(float(adx_data.get("plus_di", 0.0) or 0.0) - float(adx_data.get("minus_di", 0.0) or 0.0))
    adx_strength = _clip((adx_value - 18.0) / 18.0, 0.0, 1.0)
    trend_strength_score = _clip(
        abs(trend_score) * 0.42
        + adx_strength * 0.36
        + min(1.0, di_spread / 22.0) * 0.22,
        0.0,
        1.0,
    )

    if trend_state == "ranging" and adx_value < 18.0:
        trend_phase = "range_balance"
    elif trend_state == "ranging":
        trend_phase = "transition"
    elif dominant_exhaustion >= 0.72 or extension_score >= 1.55 or impulse_age_bars >= 7:
        trend_phase = "trend_exhausted"
    elif dominant_exhaustion >= 0.50 or extension_score >= 1.18 or impulse_age_bars >= 5:
        trend_phase = "trend_mature"
    elif trend_strength_score >= 0.58 and adx_slope >= 0.0:
        trend_phase = "trend_building"
    elif trend_strength_score >= 0.42:
        trend_phase = "trend_confirmed"
    else:
        trend_phase = "transition"

    if vol_state == "extreme" and trend_phase in {"transition", "trend_exhausted"}:
        regime = "volatile"
    elif trend_state != "ranging" and trend_strength_score >= 0.40:
        regime = trend_state
    elif adx_value >= 28.0 and trend_state == "ranging":
        regime = "volatile"
    else:
        regime = "ranging"

    entry_policy = _session_adjusted_entry_policy(regime, session_profile, trend_phase)
    confirmation_bars = int(entry_policy["confirmation_bars"])
    setup_valid_now = bool(
        candle_quality_score >= entry_policy["min_candle_quality"]
        and extension_score <= entry_policy["max_extension_score"]
        and target_efficiency >= entry_policy["min_target_efficiency"]
        and impulse_age_bars <= int(entry_policy["max_impulse_age_bars"])
        and (
            breakout_retest_ready
            or first_pullback_ready
            or failed_opposite_move_confirmed
            or liquidity_sweep_buy
            or liquidity_sweep_sell
        )
    )
    confirmation_window = min(max(confirmation_bars, 1), len(df))
    confirmation_count = 0
    if setup_valid_now:
        confirmation_count = 1
        for back in range(2, confirmation_window + 1):
            idx = -back
            hist_close = float(close.iloc[idx])
            hist_fast = float(fast.iloc[idx])
            hist_slow = float(slow.iloc[idx])
            hist_atr_ref = max(atr_ref, hist_close * 0.001, 1e-9)
            if trend_state == "trending_up":
                hist_valid = bool(
                    hist_close >= hist_slow
                    and hist_close >= hist_fast - hist_atr_ref * 0.35
                )
            elif trend_state == "trending_down":
                hist_valid = bool(
                    hist_close <= hist_slow
                    and hist_close <= hist_fast + hist_atr_ref * 0.35
                )
            else:
                hist_valid = bool(abs(hist_close - hist_fast) <= hist_atr_ref * 0.75)
            if hist_valid:
                confirmation_count += 1
            else:
                break
    confirmation_ready = bool(setup_valid_now and confirmation_count >= confirmation_bars)
    elite_pattern_rank = 0.0
    if setup_valid_now:
        session_anchor_weight = max(0.85, min(1.35, float(session_profile.get("anchor_weight", 1.0) or 1.0)))
        setup_quality = (
            abs(trend_score) * 0.18
            + trend_strength_score * 0.14
            + abs(pullback_score) * 0.16
            + abs(breakout_score) * 0.18
            + candle_quality_score * 0.14
            + target_efficiency * 0.12
            + session_quality * 0.08
            + (0.10 if failed_opposite_move_confirmed else 0.0)
            + (0.08 if breakout_retest_ready else 0.0)
            + (0.06 if first_pullback_ready else 0.0)
            + (0.05 if liquidity_sweep_buy or liquidity_sweep_sell else 0.0)
            - min(0.18, extension_score * 0.10)
        )
        if session_mode in {"opening_range_breakout", "asia_range_break", "overlap_expansion", "trend_confirmation"}:
            setup_quality += max(0.0, abs(breakout_score)) * 0.06
        elif session_mode in {"range_balance", "dead_hours", "weekend_or_thin"}:
            setup_quality += max(0.0, abs(pullback_score)) * 0.04
        elite_pattern_rank = _clip(setup_quality * session_anchor_weight, 0.0, 1.0)
    cluster_penalty = 0.0
    if impulse_age_bars >= 4:
        cluster_penalty += 0.10
    if breakout_retest_ready and first_pullback_ready:
        cluster_penalty += 0.08
    if liquidity_sweep_buy or liquidity_sweep_sell:
        cluster_penalty += 0.04
    pattern_family = _pattern_family(
        trend_state=trend_state,
        breakout_retest_ready=breakout_retest_ready,
        first_pullback_ready=first_pullback_ready,
        liquidity_sweep_buy=liquidity_sweep_buy,
        liquidity_sweep_sell=liquidity_sweep_sell,
        failed_opposite_move_confirmed=failed_opposite_move_confirmed,
    )

    return {
        "interval": interval,
        "current_price": round(current, 6),
        "trend_state": trend_state,
        "trend_score": round(trend_score, 4),
        "trend_strength_score": round(trend_strength_score, 4),
        "trend_phase": trend_phase,
        "regime_confidence": round(trend_strength_score, 4),
        "adx": round(adx_value, 4),
        "plus_di": round(float(adx_data.get("plus_di", 0.0) or 0.0), 4),
        "minus_di": round(float(adx_data.get("minus_di", 0.0) or 0.0), 4),
        "adx_slope": round(adx_slope, 4),
        "atr": round(atr, 6),
        "atr_pct": round(atr_pct, 6),
        "volatility_state": vol_state,
        "support": round(support, 6),
        "resistance": round(resistance, 6),
        "distance_to_support": round(distance_to_support, 6),
        "distance_to_resistance": round(distance_to_resistance, 6),
        "vwap": round(vwap, 6) if vwap > 0 else 0.0,
        "vwap_distance_atr": round(vwap_distance, 4),
        "session_phase": session_phase,
        "session_quality_label": session_label,
        "session_quality_score": round(session_quality, 4),
        "session_structure_mode": session_mode,
        "session_structure_profile": {
            "mode": session_mode,
            "breakout_multiplier": round(float(session_profile.get("breakout_multiplier", 0.0) or 0.0), 4),
            "pullback_multiplier": round(float(session_profile.get("pullback_multiplier", 0.0) or 0.0), 4),
            "confirmation_delta": int(session_profile.get("confirmation_delta", 0) or 0),
            "anchor_weight": round(float(session_profile.get("anchor_weight", 1.0) or 1.0), 4),
        },
        "pullback_score": round(_clip(pullback_score, -1.0, 1.0), 4),
        "breakout_score": round(_clip(breakout_score, -1.0, 1.0), 4),
        "upside_exhaustion_score": round(_clip(upside_exhaustion), 4),
        "downside_exhaustion_score": round(_clip(downside_exhaustion), 4),
        "candle_range_atr": round(float(candle["candle_range_atr"]), 4),
        "candle_body_ratio": round(float(candle["body_ratio"]), 4),
        "upper_wick_ratio": round(float(candle["upper_wick_ratio"]), 4),
        "lower_wick_ratio": round(float(candle["lower_wick_ratio"]), 4),
        "close_location": round(close_location, 4),
        "candle_quality_score": round(candle_quality_score, 4),
        "liquidity_sweep_buy": bool(liquidity_sweep_buy),
        "liquidity_sweep_sell": bool(liquidity_sweep_sell),
        "breakout_retest_ready": bool(breakout_retest_ready),
        "first_pullback_ready": bool(first_pullback_ready),
        "impulse_age_bars": int(impulse_age_bars),
        "extension_score": round(_clip(extension_score, 0.0, 2.0), 4),
        "target_efficiency_score": round(target_efficiency, 4),
        "failed_opposite_move_confirmed": bool(failed_opposite_move_confirmed),
        "entry_confirmation_bars_required": int(confirmation_bars),
        "entry_confirmation_count": int(confirmation_count),
        "entry_confirmation_ready": bool(confirmation_ready),
        "pattern_family": pattern_family,
        "elite_pattern_rank": round(elite_pattern_rank, 4),
        "cluster_penalty": round(_clip(cluster_penalty, 0.0, 1.0), 4),
        "regime_entry_policy": {
            "min_setup_quality": round(entry_policy["min_setup_quality"], 4),
            "min_candle_quality": round(entry_policy["min_candle_quality"], 4),
            "max_extension_score": round(entry_policy["max_extension_score"], 4),
            "min_target_efficiency": round(entry_policy["min_target_efficiency"], 4),
            "max_impulse_age_bars": int(entry_policy["max_impulse_age_bars"]),
            "confirmation_bars": int(entry_policy["confirmation_bars"]),
        },
    }


class MarketStructureService:
    def analyze(
        self,
        asset: str,
        category: str,
        frames: Mapping[str, pd.DataFrame],
        context: Optional[Mapping[str, Any]] = None,
    ) -> Dict[str, Any]:
        details, ordered_intervals = self._collect_frame_details(frames, asset=asset, category=category)
        if not details:
            return self._empty_analysis(asset, category)

        primary_interval = self._primary_interval(ordered_intervals, details)
        primary = details[primary_interval]
        trend = self._trend_summary(details)
        range_scores = self._range_summary(details, trend["weight_total"])
        volatility_state = str(primary.get("volatility_state", "unknown"))
        regime = self._classify_regime(
            volatility_state,
            trend["structure_bias"],
            trend["alignment_score"],
            float(trend.get("avg_trend_strength", 0.0) or 0.0),
            float(trend.get("transition_weight", 0.0) or 0.0),
            float(trend.get("exhaustion_weight", 0.0) or 0.0),
        )
        opportunity_score = max(
            abs(trend["weighted_score"]),
            abs(range_scores["pullback_score"]),
            abs(range_scores["breakout_score"]),
        )
        dominant_exhaustion = (
            range_scores["upside_exhaustion_score"]
            if trend["structure_bias"] == "buy"
            else range_scores["downside_exhaustion_score"]
            if trend["structure_bias"] == "sell"
            else max(range_scores["upside_exhaustion_score"], range_scores["downside_exhaustion_score"])
        )
        structure_bias = str(trend["structure_bias"] or "neutral").lower()
        direction_sign = 1 if structure_bias == "buy" else -1 if structure_bias == "sell" else 0
        trend_5m = str(details.get("5m", {}).get("trend_state", "unknown") or "unknown").lower()
        primary_trend_state = str(primary.get("trend_state", "unknown") or "unknown").lower()
        session_quality_score = float(primary.get("session_quality_score", 0.0) or 0.0)
        candle_quality_score = float(primary.get("candle_quality_score", 0.0) or 0.0)
        extension_score = float(primary.get("extension_score", 0.0) or 0.0)
        target_efficiency_score = float(primary.get("target_efficiency_score", 0.0) or 0.0)
        impulse_age_bars = int(primary.get("impulse_age_bars", 0) or 0)
        breakout_retest_ready = bool(primary.get("breakout_retest_ready"))
        first_pullback_ready = bool(primary.get("first_pullback_ready"))
        failed_opposite_move_confirmed = bool(primary.get("failed_opposite_move_confirmed"))
        entry_confirmation_bars_required = int(primary.get("entry_confirmation_bars_required", 0) or 0)
        entry_confirmation_count = int(primary.get("entry_confirmation_count", 0) or 0)
        entry_confirmation_ready = bool(primary.get("entry_confirmation_ready"))
        directional_breakout = range_scores["breakout_score"] * direction_sign if direction_sign else 0.0
        directional_pullback = range_scores["pullback_score"] * direction_sign if direction_sign else 0.0

        cross = dict((context or {}).get("cross_asset_context") or {})
        cross_alignment_raw = _safe_float(cross.get("alignment", cross.get("score", 0.0)), 0.0)
        if abs(cross_alignment_raw) < 1e-9:
            cross_alignment_raw = _safe_float(cross.get("score", 0.0), 0.0)
        cross_confidence = _clip(_safe_float(cross.get("confidence", 0.0), 0.0), 0.0, 1.0)
        cross_support_score = _clip(cross_alignment_raw * direction_sign) if direction_sign else 0.0

        micro = dict((context or {}).get("market_microstructure") or {})
        micro_score = _safe_float(micro.get("score", 0.0), 0.0) * direction_sign if direction_sign else 0.0
        book_support = _safe_float(micro.get("book_imbalance", 0.0), 0.0) * direction_sign if direction_sign else 0.0
        tick_support = _safe_float(micro.get("tick_imbalance", 0.0), 0.0) * direction_sign if direction_sign else 0.0
        velocity_support = (_safe_float(micro.get("velocity_bps", 0.0), 0.0) * direction_sign / 4.0) if direction_sign else 0.0
        microstructure_support_score = _clip(max(micro_score, book_support * 0.90, tick_support * 0.75, velocity_support))
        anchor_context = _session_anchor_context(asset, category, frames)
        event_anchor_frame = frames.get(primary_interval)
        if not isinstance(event_anchor_frame, pd.DataFrame) or event_anchor_frame.empty:
            _, event_anchor_frame = _anchor_frame(frames)
        event_anchor_context = (
            _event_reset_context(asset, event_anchor_frame, interval=primary_interval)
            if isinstance(event_anchor_frame, pd.DataFrame) and not event_anchor_frame.empty
            else _empty_anchor_context("event_reset", "event reset", primary_interval)
        )
        session_anchor_direction_score = _safe_float(anchor_context.get("direction_score", 0.0), 0.0)
        session_anchor_support_score = _clip(session_anchor_direction_score * direction_sign) if direction_sign else 0.0
        event_anchor_direction_score = _safe_float(event_anchor_context.get("direction_score", 0.0), 0.0)
        event_anchor_support_score = _clip(event_anchor_direction_score * direction_sign) if direction_sign else 0.0
        external_confirmation = _clip(
            max(
                cross_support_score * (0.72 + cross_confidence * 0.28),
                microstructure_support_score,
                max(0.0, session_anchor_support_score) * 0.92,
                max(0.0, event_anchor_support_score) * 0.88,
            ),
            0.0,
            1.0,
        )
        structure_levels = _structure_levels(
            current_price=float(primary.get("current_price", 0.0) or 0.0),
            primary=primary,
            session_anchor=anchor_context,
            event_anchor=event_anchor_context,
            atr=float(primary.get("atr", 0.0) or 0.0),
        )
        trigger_trend_aligned = bool(
            direction_sign != 0 and _trend_state_sign(trend_5m) == direction_sign
        )

        resolved_trend_state = primary_trend_state
        structure_promoted = False
        if primary_trend_state == "ranging" and direction_sign != 0:
            promoted_by_structure = (
                trend["alignment_score"] >= 0.55
                and abs(trend["weighted_score"]) >= 0.12
                and max(directional_breakout, directional_pullback) >= 0.14
                and dominant_exhaustion <= 0.58
            )
            promoted_by_context = (
                external_confirmation >= 0.18
                and session_quality_score >= 0.46
                and candle_quality_score >= 0.28
            )
            if promoted_by_structure or promoted_by_context:
                resolved_trend_state = "trending_up" if direction_sign > 0 else "trending_down"
                structure_promoted = True

        if direction_sign != 0 and not first_pullback_ready:
            if (
                resolved_trend_state in {"trending_up", "trending_down"}
                and directional_pullback >= 0.16
                and candle_quality_score >= 0.30
                and session_quality_score >= 0.46
                and extension_score <= 0.96
                and impulse_age_bars <= 5
                and external_confirmation >= 0.14
                and session_anchor_support_score >= -0.12
            ):
                first_pullback_ready = True
                structure_promoted = True

        if direction_sign != 0 and not entry_confirmation_ready:
            if (
                resolved_trend_state in {"trending_up", "trending_down"}
                and directional_breakout >= 0.18
                and candle_quality_score >= 0.30
                and session_quality_score >= 0.46
                and target_efficiency_score >= 0.24
                and extension_score <= 1.18
                and impulse_age_bars <= 5
                and external_confirmation >= 0.16
                and session_anchor_support_score >= -0.12
            ):
                entry_confirmation_ready = True
                entry_confirmation_count = max(entry_confirmation_count, max(entry_confirmation_bars_required, 1))
                structure_promoted = True

        fast_entry_confirmation_bars_required = max(1, min(int(entry_confirmation_bars_required or 1), 1))
        fast_entry_confirmation_count = int(entry_confirmation_count)
        fast_entry_confirmation_ready = bool(entry_confirmation_ready)
        if direction_sign != 0 and not fast_entry_confirmation_ready:
            if (
                resolved_trend_state in {"trending_up", "trending_down"}
                and directional_breakout >= 0.12
                and candle_quality_score >= 0.28
                and session_quality_score >= 0.40
                and target_efficiency_score >= 0.20
                and extension_score <= 1.24
                and impulse_age_bars <= 5
                and external_confirmation >= 0.12
                and session_anchor_support_score >= -0.16
                and (
                    trigger_trend_aligned
                    or structure_promoted
                    or bool(primary.get("liquidity_sweep_buy"))
                    or bool(primary.get("liquidity_sweep_sell"))
                )
            ):
                fast_entry_confirmation_ready = True
                fast_entry_confirmation_count = max(
                    fast_entry_confirmation_count,
                    fast_entry_confirmation_bars_required,
                )

        setup_quality = self._setup_quality(
            trend["weighted_score"],
            trend["alignment_score"],
            opportunity_score,
            volatility_state,
            dominant_exhaustion,
            session_quality_score=session_quality_score,
            candle_quality_score=candle_quality_score,
            directional_opportunity=max(directional_breakout, directional_pullback, 0.0),
            external_confirmation=external_confirmation,
            structure_promoted=structure_promoted,
        )
        setup_quality = _clip(setup_quality + session_anchor_support_score * 0.08 + event_anchor_support_score * 0.05, 0.0, 1.0)
        pattern_family = _pattern_family(
            trend_state=resolved_trend_state,
            breakout_retest_ready=breakout_retest_ready,
            first_pullback_ready=first_pullback_ready,
            liquidity_sweep_buy=bool(primary.get("liquidity_sweep_buy")),
            liquidity_sweep_sell=bool(primary.get("liquidity_sweep_sell")),
            failed_opposite_move_confirmed=failed_opposite_move_confirmed,
        )
        family_directional_match = bool(
            (direction_sign > 0 and pattern_family.startswith("trending_up_"))
            or (direction_sign < 0 and pattern_family.startswith("trending_down_"))
        )
        premium_generic_trend_ready = bool(
            direction_sign != 0
            and family_directional_match
            and pattern_family.endswith("generic")
            and resolved_trend_state in {"trending_up", "trending_down"}
            and trend["alignment_score"] >= 0.86
            and setup_quality >= 0.78
            and target_efficiency_score >= 0.55
            and extension_score <= 1.18
            and impulse_age_bars <= 5
            and dominant_exhaustion <= 0.50
            and session_anchor_support_score >= -0.12
        )
        generic_trend_ready = bool(
            direction_sign != 0
            and family_directional_match
            and pattern_family.endswith("generic")
            and resolved_trend_state in {"trending_up", "trending_down"}
            and trend["alignment_score"] >= 0.68
            and setup_quality >= 0.62
            and target_efficiency_score >= 0.40
            and extension_score <= 1.45
            and impulse_age_bars <= 5
            and dominant_exhaustion <= 0.58
            and session_anchor_support_score >= -0.16
            and (
                (candle_quality_score >= 0.36 and session_quality_score >= 0.40)
                or premium_generic_trend_ready
            )
        )
        elite_pattern_rank = float(primary.get("elite_pattern_rank", 0.0) or 0.0)
        if generic_trend_ready:
            derived_generic_rank = _clip(
                setup_quality * 0.38
                + max(directional_breakout, directional_pullback, 0.0) * 0.18
                + target_efficiency_score * 0.16
                + session_quality_score * 0.10
                + candle_quality_score * 0.10
                + external_confirmation * 0.08
                + (0.06 if premium_generic_trend_ready else 0.0)
                - min(0.10, extension_score * 0.05)
                - dominant_exhaustion * 0.08,
                0.0,
                0.58,
            )
            elite_pattern_rank = max(elite_pattern_rank, derived_generic_rank)

        return {
            "asset": asset,
            "category": category,
            "regime": regime,
            "primary_interval": primary_interval,
            "volatility_state": volatility_state,
            "structure_bias": structure_bias,
            "trend_strength_score": round(float(trend.get("avg_trend_strength", primary.get("trend_strength_score", 0.0)) or 0.0), 4),
            "regime_phase": str(primary.get("trend_phase") or "unknown"),
            "regime_confidence": round(float(primary.get("regime_confidence", 0.0) or 0.0), 4),
            "trend_5m": trend_5m,
            "trend_15m": details.get("15m", {}).get("trend_state", "unknown"),
            "trend_1h": details.get("1h", {}).get("trend_state", "unknown"),
            "trend_4h": details.get("4h", {}).get("trend_state", "unknown"),
            "alignment_score": round(trend["alignment_score"], 4),
            "pullback_score": round(_clip(range_scores["pullback_score"], -1.0, 1.0), 4),
            "breakout_score": round(_clip(range_scores["breakout_score"], -1.0, 1.0), 4),
            "setup_quality": round(setup_quality, 4),
            "upside_exhaustion_score": round(_clip(range_scores["upside_exhaustion_score"]), 4),
            "downside_exhaustion_score": round(_clip(range_scores["downside_exhaustion_score"]), 4),
            "dominant_exhaustion_score": round(_clip(dominant_exhaustion), 4),
            "bias_exhausted": bool(dominant_exhaustion >= 0.60),
            "support_levels": list(structure_levels.get("support_levels") or []),
            "resistance_levels": list(structure_levels.get("resistance_levels") or []),
            "invalid_below": structure_levels.get("invalid_below", 0.0),
            "invalid_above": structure_levels.get("invalid_above", 0.0),
            "bullish_target_levels": list(structure_levels.get("bullish_target_levels") or []),
            "bearish_target_levels": list(structure_levels.get("bearish_target_levels") or []),
            "distance_to_support": primary.get("distance_to_support"),
            "distance_to_resistance": primary.get("distance_to_resistance"),
            "vwap": primary.get("vwap"),
            "vwap_distance_atr": primary.get("vwap_distance_atr"),
            "session_phase": str(primary.get("session_phase") or ""),
            "session_structure_mode": str(primary.get("session_structure_mode") or "balanced"),
            "session_structure_profile": dict(primary.get("session_structure_profile") or {}),
            "session_quality_label": primary.get("session_quality_label", "unknown"),
            "session_quality_score": session_quality_score,
            "candle_quality_score": candle_quality_score,
            "candle_range_atr": primary.get("candle_range_atr", 0.0),
            "candle_body_ratio": primary.get("candle_body_ratio", 0.0),
            "upper_wick_ratio": primary.get("upper_wick_ratio", 0.0),
            "lower_wick_ratio": primary.get("lower_wick_ratio", 0.0),
            "close_location": primary.get("close_location", 0.0),
            "liquidity_sweep_buy": bool(primary.get("liquidity_sweep_buy")),
            "liquidity_sweep_sell": bool(primary.get("liquidity_sweep_sell")),
            "breakout_retest_ready": breakout_retest_ready,
            "first_pullback_ready": first_pullback_ready,
            "impulse_age_bars": impulse_age_bars,
            "extension_score": extension_score,
            "target_efficiency_score": target_efficiency_score,
            "failed_opposite_move_confirmed": failed_opposite_move_confirmed,
            "entry_confirmation_bars_required": entry_confirmation_bars_required,
            "entry_confirmation_count": entry_confirmation_count,
            "entry_confirmation_ready": entry_confirmation_ready,
            "fast_entry_confirmation_bars_required": int(fast_entry_confirmation_bars_required),
            "fast_entry_confirmation_count": int(fast_entry_confirmation_count),
            "fast_entry_confirmation_ready": bool(fast_entry_confirmation_ready),
            "pattern_family": pattern_family,
            "resolved_trend_state": resolved_trend_state,
            "structure_promoted": bool(structure_promoted),
            "trigger_trend_aligned": bool(trigger_trend_aligned),
            "cross_asset_support_score": round(cross_support_score, 4),
            "cross_asset_confidence": round(cross_confidence, 4),
            "microstructure_support_score": round(microstructure_support_score, 4),
            "session_anchor_type": str(anchor_context.get("type") or ""),
            "session_anchor_label": str(anchor_context.get("label") or ""),
            "session_anchor_interval": str(anchor_context.get("interval") or ""),
            "session_anchor_ready": bool(anchor_context.get("ready")),
            "session_anchor_state": str(anchor_context.get("state") or "unavailable"),
            "session_anchor_bias": str(anchor_context.get("bias") or "neutral"),
            "session_anchor_high": anchor_context.get("high", 0.0),
            "session_anchor_low": anchor_context.get("low", 0.0),
            "session_anchor_mid": anchor_context.get("mid", 0.0),
            "session_anchor_range_pct": anchor_context.get("range_pct", 0.0),
            "session_anchor_direction_score": round(session_anchor_direction_score, 4),
            "session_anchor_support_score": round(session_anchor_support_score, 4),
            "session_anchor_daily_state": str(anchor_context.get("daily_anchor_state") or ""),
            "session_anchor_daily_bias": str(anchor_context.get("daily_anchor_bias") or ""),
            "session_anchor_daily_direction_score": round(float(anchor_context.get("daily_anchor_direction_score", 0.0) or 0.0), 4),
            "session_anchor_weekly_state": str(anchor_context.get("weekly_anchor_state") or ""),
            "session_anchor_weekly_bias": str(anchor_context.get("weekly_anchor_bias") or ""),
            "session_anchor_weekly_direction_score": round(float(anchor_context.get("weekly_anchor_direction_score", 0.0) or 0.0), 4),
            "event_anchor_type": str(event_anchor_context.get("type") or ""),
            "event_anchor_label": str(event_anchor_context.get("label") or ""),
            "event_anchor_interval": str(event_anchor_context.get("interval") or ""),
            "event_anchor_ready": bool(event_anchor_context.get("ready")),
            "event_anchor_state": str(event_anchor_context.get("state") or "unavailable"),
            "event_anchor_bias": str(event_anchor_context.get("bias") or "neutral"),
            "event_anchor_high": event_anchor_context.get("high", 0.0),
            "event_anchor_low": event_anchor_context.get("low", 0.0),
            "event_anchor_mid": event_anchor_context.get("mid", 0.0),
            "event_anchor_range_pct": event_anchor_context.get("range_pct", 0.0),
            "event_anchor_direction_score": round(event_anchor_direction_score, 4),
            "event_anchor_support_score": round(event_anchor_support_score, 4),
            "event_anchor_timestamp": str(event_anchor_context.get("event_timestamp") or ""),
            "external_confirmation_score": round(external_confirmation, 4),
            "elite_pattern_rank": round(elite_pattern_rank, 4),
            "cluster_penalty": primary.get("cluster_penalty", 0.0),
            "regime_entry_policy": primary.get("regime_entry_policy", {}),
            "frame_details": details,
        }

    def _collect_frame_details(
        self,
        frames: Mapping[str, pd.DataFrame],
        *,
        asset: str = "",
        category: str = "",
    ) -> tuple[Dict[str, Dict[str, Any]], List[str]]:
        details: Dict[str, Dict[str, Any]] = {}
        ordered_intervals = [str(interval).lower() for interval in frames.keys()]
        for interval, df in frames.items():
            analyzed = _analyze_frame(str(interval).lower(), df, asset=asset, category=category)
            if analyzed:
                details[str(interval).lower()] = analyzed
        return details, ordered_intervals

    @staticmethod
    def _empty_analysis(asset: str, category: str) -> Dict[str, Any]:
        return {
            "asset": asset,
            "category": category,
            "regime": "unknown",
            "regime_phase": "unknown",
            "regime_confidence": 0.0,
            "structure_bias": "neutral",
            "trend_5m": "unknown",
            "trend_strength_score": 0.0,
            "alignment_score": 0.0,
            "setup_quality": 0.0,
            "pullback_score": 0.0,
            "breakout_score": 0.0,
            "volatility_state": "unknown",
            "frame_details": {},
            "support_levels": [],
            "resistance_levels": [],
            "invalid_below": 0.0,
            "invalid_above": 0.0,
            "bullish_target_levels": [],
            "bearish_target_levels": [],
            "distance_to_support": None,
            "distance_to_resistance": None,
            "vwap": 0.0,
            "vwap_distance_atr": 0.0,
            "session_phase": "",
            "session_structure_mode": "balanced",
            "session_structure_profile": {},
            "session_quality_label": "unknown",
            "session_quality_score": 0.0,
            "candle_quality_score": 0.0,
            "candle_range_atr": 0.0,
            "candle_body_ratio": 0.0,
            "upper_wick_ratio": 0.0,
            "lower_wick_ratio": 0.0,
            "close_location": 0.0,
            "liquidity_sweep_buy": False,
            "liquidity_sweep_sell": False,
            "breakout_retest_ready": False,
            "first_pullback_ready": False,
            "impulse_age_bars": 0,
            "extension_score": 0.0,
            "target_efficiency_score": 0.0,
            "failed_opposite_move_confirmed": False,
            "entry_confirmation_bars_required": 0,
            "entry_confirmation_count": 0,
            "entry_confirmation_ready": False,
            "fast_entry_confirmation_bars_required": 0,
            "fast_entry_confirmation_count": 0,
            "fast_entry_confirmation_ready": False,
            "pattern_family": "unknown",
            "resolved_trend_state": "unknown",
            "structure_promoted": False,
            "trigger_trend_aligned": False,
            "cross_asset_support_score": 0.0,
            "cross_asset_confidence": 0.0,
            "microstructure_support_score": 0.0,
            "external_confirmation_score": 0.0,
            "session_anchor_type": "",
            "session_anchor_state": "unavailable",
            "session_anchor_support_score": 0.0,
            "event_anchor_type": "",
            "event_anchor_state": "unavailable",
            "event_anchor_support_score": 0.0,
            "elite_pattern_rank": 0.0,
            "cluster_penalty": 0.0,
            "regime_entry_policy": {},
        }

    @staticmethod
    def _primary_interval(ordered_intervals: List[str], details: Dict[str, Dict[str, Any]]) -> str:
        return next((i for i in ordered_intervals if i in details), next(iter(details)))

    def _trend_summary(self, details: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        weighted_score = 0.0
        weight_total = 0.0
        avg_trend_strength = 0.0
        transition_weight = 0.0
        exhaustion_weight = 0.0
        for interval, info in details.items():
            weight = _FRAME_WEIGHTS.get(interval, 0.15)
            strength = max(0.20, float(info.get("trend_strength_score", abs(float(info["trend_score"]))) or abs(float(info["trend_score"]))))
            weighted_score += float(info["trend_score"]) * weight * (0.70 + strength * 0.30)
            weight_total += weight
            avg_trend_strength += strength * weight
            phase = str(info.get("trend_phase") or "")
            if phase == "transition":
                transition_weight += weight
            elif phase == "trend_exhausted":
                exhaustion_weight += weight
        if weight_total > 0:
            weighted_score /= weight_total
            avg_trend_strength /= weight_total
            transition_weight /= weight_total
            exhaustion_weight /= weight_total

        if weighted_score >= 0.12:
            structure_bias = "buy"
        elif weighted_score <= -0.12:
            structure_bias = "sell"
        else:
            structure_bias = "neutral"

        dominant_sign = 1 if structure_bias == "buy" else -1 if structure_bias == "sell" else 0
        if dominant_sign != 0 and weight_total > 0:
            aligned_weight = 0.0
            for interval, info in details.items():
                interval_sign = 1 if float(info["trend_score"]) > 0.10 else -1 if float(info["trend_score"]) < -0.10 else 0
                strength = float(info.get("trend_strength_score", abs(float(info["trend_score"]))) or abs(float(info["trend_score"])))
                if interval_sign == dominant_sign:
                    aligned_weight += _FRAME_WEIGHTS.get(interval, 0.15) * max(0.35, min(1.0, strength))
            alignment_score = aligned_weight / weight_total
        else:
            alignment_score = 0.0

        return {
            "weighted_score": weighted_score,
            "weight_total": weight_total,
            "structure_bias": structure_bias,
            "alignment_score": alignment_score,
            "avg_trend_strength": round(avg_trend_strength, 4),
            "transition_weight": round(transition_weight, 4),
            "exhaustion_weight": round(exhaustion_weight, 4),
        }

    def _range_summary(self, details: Dict[str, Dict[str, Any]], weight_total: float) -> Dict[str, Any]:
        pullback_score = 0.0
        breakout_score = 0.0
        upside_exhaustion_score = 0.0
        downside_exhaustion_score = 0.0
        for interval, info in details.items():
            weight = _FRAME_WEIGHTS.get(interval, 0.15)
            pullback_score += float(info["pullback_score"]) * weight
            breakout_score += float(info["breakout_score"]) * weight
            upside_exhaustion_score += float(info.get("upside_exhaustion_score", 0.0) or 0.0) * weight
            downside_exhaustion_score += float(info.get("downside_exhaustion_score", 0.0) or 0.0) * weight
        if weight_total > 0:
            pullback_score /= weight_total
            breakout_score /= weight_total
            upside_exhaustion_score /= weight_total
            downside_exhaustion_score /= weight_total

        return {
            "pullback_score": pullback_score,
            "breakout_score": breakout_score,
            "upside_exhaustion_score": upside_exhaustion_score,
            "downside_exhaustion_score": downside_exhaustion_score,
        }

    @staticmethod
    def _classify_regime(
        volatility_state: str,
        structure_bias: str,
        alignment_score: float,
        avg_trend_strength: float,
        transition_weight: float,
        exhaustion_weight: float,
    ) -> str:
        if volatility_state == "extreme" and (transition_weight >= 0.35 or exhaustion_weight >= 0.35):
            return "volatile"
        if structure_bias == "buy" and alignment_score >= 0.55 and avg_trend_strength >= 0.42:
            return "trending_up"
        if structure_bias == "sell" and alignment_score >= 0.55 and avg_trend_strength >= 0.42:
            return "trending_down"
        if avg_trend_strength >= 0.55 and transition_weight >= 0.32:
            return "volatile"
        return "ranging"

    @staticmethod
    def _setup_quality(
        weighted_score: float,
        alignment_score: float,
        opportunity_score: float,
        volatility_state: str,
        dominant_exhaustion: float,
        *,
        session_quality_score: float = 0.0,
        candle_quality_score: float = 0.0,
        directional_opportunity: float = 0.0,
        external_confirmation: float = 0.0,
        structure_promoted: bool = False,
    ) -> float:
        setup_quality = (
            abs(weighted_score) * 0.35
            + alignment_score * 0.25
            + opportunity_score * 0.25
            + _VOLATILITY_FIT.get(volatility_state, 0.5) * 0.15
        )
        if alignment_score >= 0.55 and abs(weighted_score) >= 0.12:
            setup_quality += 0.05
        if directional_opportunity >= 0.16:
            setup_quality += min(0.05, directional_opportunity * 0.16)
        if session_quality_score >= 0.52 and candle_quality_score >= 0.30:
            setup_quality += 0.04
        if external_confirmation >= 0.16:
            setup_quality += min(0.06, external_confirmation * 0.18)
        if structure_promoted:
            setup_quality += 0.04
        if dominant_exhaustion > 0.0:
            setup_quality -= min(0.22, dominant_exhaustion * 0.22)
        return max(0.0, min(1.0, setup_quality))
_service = MarketStructureService()


def get_service() -> MarketStructureService:
    return _service
