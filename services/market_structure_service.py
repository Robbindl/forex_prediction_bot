from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional

import numpy as np

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


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value if value is not None else default)
    except Exception:
        return default


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


def _session_quality(interval: str, atr_pct: float) -> tuple[str, float]:
    london_intervals = {"5m", "15m", "30m", "1h"}
    scalp_intervals = {"1m", "5m"}
    if atr_pct >= 0.020:
        return "chaotic", 0.26
    if atr_pct <= 0.0018 and interval in scalp_intervals:
        return "dead", 0.34
    if atr_pct <= 0.0025 and interval in london_intervals:
        return "quiet", 0.52
    if 0.0025 < atr_pct <= 0.012:
        return "active", 0.82
    if 0.012 < atr_pct <= 0.020:
        return "fast", 0.66
    return "mixed", 0.58


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

    pullback_score = 0.0
    fast_level = float(fast.iloc[-1])
    slow_level = float(slow.iloc[-1])
    pullback_proximity = 1.0 - min(1.0, abs(current - fast_level) / (atr_ref * 1.6))
    if trend_state == "trending_up" and current >= slow_level:
        pullback_score = pullback_proximity
    elif trend_state == "trending_down" and current <= slow_level:
        pullback_score = -pullback_proximity

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
    session_label, session_quality = _session_quality(interval, atr_pct)

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

    extension_score = max(abs(vwap_distance) / 2.0, upside_extension_fast, downside_extension_fast)
    target_efficiency = 0.0
    if trend_state == "trending_up":
        target_efficiency = _clip((distance_to_resistance * current) / max(atr_ref * 1.2, 1e-9), 0.0, 1.0)
    elif trend_state == "trending_down":
        target_efficiency = _clip((distance_to_support * current) / max(atr_ref * 1.2, 1e-9), 0.0, 1.0)

    regime = _frame_state_from_score(trend_score)
    entry_policy = _regime_entry_policy(regime)
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
        elite_pattern_rank = _clip(
            setup_quality := (
                abs(trend_score) * 0.22
                + abs(pullback_score) * 0.18
                + abs(breakout_score) * 0.18
                + candle_quality_score * 0.16
                + target_efficiency * 0.14
                + session_quality * 0.08
                + (0.12 if failed_opposite_move_confirmed else 0.0)
                + (0.08 if breakout_retest_ready else 0.0)
                + (0.06 if first_pullback_ready else 0.0)
                + (0.05 if liquidity_sweep_buy or liquidity_sweep_sell else 0.0)
                - min(0.18, extension_score * 0.10)
            ),
            0.0,
            1.0,
        )
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
        "atr": round(atr, 6),
        "atr_pct": round(atr_pct, 6),
        "volatility_state": vol_state,
        "support": round(support, 6),
        "resistance": round(resistance, 6),
        "distance_to_support": round(distance_to_support, 6),
        "distance_to_resistance": round(distance_to_resistance, 6),
        "vwap": round(vwap, 6) if vwap > 0 else 0.0,
        "vwap_distance_atr": round(vwap_distance, 4),
        "session_quality_label": session_label,
        "session_quality_score": round(session_quality, 4),
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
        details, ordered_intervals = self._collect_frame_details(frames)
        if not details:
            return self._empty_analysis(asset, category)

        primary_interval = self._primary_interval(ordered_intervals, details)
        primary = details[primary_interval]
        trend = self._trend_summary(details)
        range_scores = self._range_summary(details, trend["weight_total"])
        volatility_state = str(primary.get("volatility_state", "unknown"))
        regime = self._classify_regime(volatility_state, trend["structure_bias"], trend["alignment_score"])
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
        external_confirmation = _clip(
            max(
                cross_support_score * (0.72 + cross_confidence * 0.28),
                microstructure_support_score,
            ),
            0.0,
            1.0,
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
            ):
                entry_confirmation_ready = True
                entry_confirmation_count = max(entry_confirmation_count, max(entry_confirmation_bars_required, 1))
                structure_promoted = True

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
            "support_levels": [primary.get("support")] if primary.get("support") is not None else [],
            "resistance_levels": [primary.get("resistance")] if primary.get("resistance") is not None else [],
            "distance_to_support": primary.get("distance_to_support"),
            "distance_to_resistance": primary.get("distance_to_resistance"),
            "vwap": primary.get("vwap"),
            "vwap_distance_atr": primary.get("vwap_distance_atr"),
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
            "pattern_family": pattern_family,
            "resolved_trend_state": resolved_trend_state,
            "structure_promoted": bool(structure_promoted),
            "cross_asset_support_score": round(cross_support_score, 4),
            "cross_asset_confidence": round(cross_confidence, 4),
            "microstructure_support_score": round(microstructure_support_score, 4),
            "elite_pattern_rank": round(elite_pattern_rank, 4),
            "cluster_penalty": primary.get("cluster_penalty", 0.0),
            "regime_entry_policy": primary.get("regime_entry_policy", {}),
            "frame_details": details,
        }

    def _collect_frame_details(self, frames: Mapping[str, pd.DataFrame]) -> tuple[Dict[str, Dict[str, Any]], List[str]]:
        details: Dict[str, Dict[str, Any]] = {}
        ordered_intervals = [str(interval).lower() for interval in frames.keys()]
        for interval, df in frames.items():
            analyzed = _analyze_frame(str(interval).lower(), df)
            if analyzed:
                details[str(interval).lower()] = analyzed
        return details, ordered_intervals

    @staticmethod
    def _empty_analysis(asset: str, category: str) -> Dict[str, Any]:
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
            "vwap": 0.0,
            "vwap_distance_atr": 0.0,
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
            "pattern_family": "unknown",
            "resolved_trend_state": "unknown",
            "structure_promoted": False,
            "cross_asset_support_score": 0.0,
            "cross_asset_confidence": 0.0,
            "microstructure_support_score": 0.0,
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
        if dominant_sign != 0 and weight_total > 0:
            aligned_weight = 0.0
            for interval, info in details.items():
                interval_sign = 1 if float(info["trend_score"]) > 0.10 else -1 if float(info["trend_score"]) < -0.10 else 0
                if interval_sign == dominant_sign:
                    aligned_weight += _FRAME_WEIGHTS.get(interval, 0.15)
            alignment_score = aligned_weight / weight_total
        else:
            alignment_score = 0.0

        return {
            "weighted_score": weighted_score,
            "weight_total": weight_total,
            "structure_bias": structure_bias,
            "alignment_score": alignment_score,
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
    def _classify_regime(volatility_state: str, structure_bias: str, alignment_score: float) -> str:
        if volatility_state == "extreme":
            return "volatile"
        if structure_bias == "buy" and alignment_score >= 0.55:
            return "trending_up"
        if structure_bias == "sell" and alignment_score >= 0.55:
            return "trending_down"
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
