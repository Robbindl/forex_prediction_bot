from __future__ import annotations

from typing import Any, Dict, Mapping, Optional

import pandas as pd

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


def _clip(value: float, lo: float = -1.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, float(value)))


def _to_float_series(df: pd.DataFrame, column: str) -> Optional[pd.Series]:
    try:
        return df[column].astype(float)
    except Exception:
        return None


def _estimate_atr(df: pd.DataFrame, period: int = 14) -> float:
    if df is None or len(df) < period + 1:
        return 0.0
    high = _to_float_series(df, "high")
    low = _to_float_series(df, "low")
    close = _to_float_series(df, "close")
    if high is None or low is None or close is None:
        return 0.0
    prev_close = close.shift(1)
    tr = pd.concat(
        [
            high - low,
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    try:
        return float(tr.tail(period).mean())
    except Exception:
        return 0.0


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


def _analyze_frame(interval: str, df: pd.DataFrame) -> Optional[Dict[str, Any]]:
    if df is None or len(df) < 30:
        return None

    close = _to_float_series(df, "close")
    high = _to_float_series(df, "high")
    low = _to_float_series(df, "low")
    if close is None or high is None or low is None:
        return None

    current = float(close.iloc[-1])
    if current <= 0:
        return None

    fast_span = 12 if interval in {"1m", "5m"} else 20
    slow_span = 30 if interval in {"1m", "5m"} else 50
    fast = close.ewm(span=fast_span, adjust=False).mean()
    slow = close.ewm(span=slow_span, adjust=False).mean()

    lookback = min(8, len(close) - 1)
    slope = 0.0
    if lookback > 0:
        try:
            slope = (float(fast.iloc[-1]) - float(fast.iloc[-1 - lookback])) / current
        except Exception:
            slope = 0.0
    ema_gap = (float(fast.iloc[-1]) - float(slow.iloc[-1])) / current
    trend_score = _clip((ema_gap * 42.0) + (slope * 135.0))
    trend_state = _frame_state_from_score(trend_score)

    atr = _estimate_atr(df)
    atr_ref = max(atr, current * 0.001, 1e-9)
    atr_pct = atr / current if current else 0.0
    vol_state = _volatility_state(atr_pct)

    recent_window = min(20, len(df))
    recent_high = float(high.tail(recent_window).max())
    recent_low = float(low.tail(recent_window).min())
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

    pullback_score = 0.0
    fast_level = float(fast.iloc[-1])
    slow_level = float(slow.iloc[-1])
    pullback_proximity = 1.0 - min(1.0, abs(current - fast_level) / (atr_ref * 1.6))
    if trend_state == "trending_up" and current >= slow_level:
        pullback_score = pullback_proximity
    elif trend_state == "trending_down" and current <= slow_level:
        pullback_score = -pullback_proximity

    return {
        "interval": interval,
        "current_price": round(current, 6),
        "trend_state": trend_state,
        "trend_score": round(trend_score, 4),
        "atr": round(atr, 6),
        "atr_pct": round(atr_pct, 6),
        "volatility_state": vol_state,
        "support": round(support, 6),
        "resistance": round(resistance, 6),
        "distance_to_support": round(distance_to_support, 6),
        "distance_to_resistance": round(distance_to_resistance, 6),
        "pullback_score": round(_clip(pullback_score), 4),
        "breakout_score": round(_clip(breakout_score), 4),
    }


class MarketStructureService:
    def analyze(
        self,
        asset: str,
        category: str,
        frames: Mapping[str, pd.DataFrame],
    ) -> Dict[str, Any]:
        details: Dict[str, Dict[str, Any]] = {}
        ordered_intervals = [str(interval).lower() for interval in frames.keys()]

        for interval, df in frames.items():
            analyzed = _analyze_frame(str(interval).lower(), df)
            if analyzed:
                details[str(interval).lower()] = analyzed

        if not details:
            return {
                "asset": asset,
                "category": category,
                "regime": "unknown",
                "structure_bias": "neutral",
                "alignment_score": 0.0,
                "setup_quality": 0.0,
                "pullback_score": 0.0,
                "breakout_score": 0.0,
                "volatility_state": "unknown",
                "frame_details": {},
                "support_levels": [],
                "resistance_levels": [],
                "distance_to_support": None,
                "distance_to_resistance": None,
            }

        primary_interval = next((i for i in ordered_intervals if i in details), next(iter(details)))
        primary = details[primary_interval]

        weighted_score = 0.0
        weight_total = 0.0
        for interval, info in details.items():
            weight = _FRAME_WEIGHTS.get(interval, 0.15)
            weighted_score += float(info["trend_score"]) * weight
            weight_total += weight
        if weight_total > 0:
            weighted_score /= weight_total

        if weighted_score >= 0.12:
            structure_bias = "buy"
        elif weighted_score <= -0.12:
            structure_bias = "sell"
        else:
            structure_bias = "neutral"

        dominant_sign = 1 if structure_bias == "buy" else -1 if structure_bias == "sell" else 0
        aligned_weight = 0.0
        if dominant_sign != 0:
            for interval, info in details.items():
                interval_sign = 1 if float(info["trend_score"]) > 0.10 else -1 if float(info["trend_score"]) < -0.10 else 0
                if interval_sign == dominant_sign:
                    aligned_weight += _FRAME_WEIGHTS.get(interval, 0.15)
            alignment_score = aligned_weight / weight_total if weight_total > 0 else 0.0
        else:
            alignment_score = 0.0

        pullback_score = 0.0
        breakout_score = 0.0
        for interval, info in details.items():
            weight = _FRAME_WEIGHTS.get(interval, 0.15)
            pullback_score += float(info["pullback_score"]) * weight
            breakout_score += float(info["breakout_score"]) * weight
        if weight_total > 0:
            pullback_score /= weight_total
            breakout_score /= weight_total

        volatility_state = str(primary.get("volatility_state", "unknown"))
        if volatility_state == "extreme":
            regime = "volatile"
        elif structure_bias == "buy" and alignment_score >= 0.55:
            regime = "trending_up"
        elif structure_bias == "sell" and alignment_score >= 0.55:
            regime = "trending_down"
        else:
            regime = "ranging"

        opportunity_score = max(abs(weighted_score), abs(pullback_score), abs(breakout_score))
        setup_quality = (
            abs(weighted_score) * 0.35
            + alignment_score * 0.25
            + opportunity_score * 0.25
            + _VOLATILITY_FIT.get(volatility_state, 0.5) * 0.15
        )
        setup_quality = max(0.0, min(1.0, setup_quality))

        return {
            "asset": asset,
            "category": category,
            "regime": regime,
            "primary_interval": primary_interval,
            "volatility_state": volatility_state,
            "structure_bias": structure_bias,
            "trend_15m": details.get("15m", {}).get("trend_state", "unknown"),
            "trend_1h": details.get("1h", {}).get("trend_state", "unknown"),
            "trend_4h": details.get("4h", {}).get("trend_state", "unknown"),
            "alignment_score": round(alignment_score, 4),
            "pullback_score": round(_clip(pullback_score), 4),
            "breakout_score": round(_clip(breakout_score), 4),
            "setup_quality": round(setup_quality, 4),
            "support_levels": [primary.get("support")] if primary.get("support") is not None else [],
            "resistance_levels": [primary.get("resistance")] if primary.get("resistance") is not None else [],
            "distance_to_support": primary.get("distance_to_support"),
            "distance_to_resistance": primary.get("distance_to_resistance"),
            "frame_details": details,
        }


_service = MarketStructureService()


def get_service() -> MarketStructureService:
    return _service
