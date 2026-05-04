from __future__ import annotations

from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

try:
    import pandas as pd
except Exception:  # pragma: no cover
    pd = None  # type: ignore


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return float(default)
        return float(value)
    except Exception:
        return float(default)


def _clip(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, float(value)))


def _direction_sign(direction: str) -> int:
    token = str(direction or "").strip().lower()
    if token in {"buy", "bull", "bullish", "up", "trending_up"}:
        return 1
    if token in {"sell", "bear", "bearish", "down", "trending_down"}:
        return -1
    return 0


def _trend_label(score: float) -> str:
    if score >= 0.18:
        return "trending_up"
    if score <= -0.18:
        return "trending_down"
    return "ranging"


def _trusted_micro_pressure(context: Optional[Mapping[str, Any]]) -> Tuple[int, float, bool]:
    micro = dict((context or {}).get("market_microstructure") or {})
    depth_available = bool(micro.get("depth_available"))
    synthetic = bool(micro.get("synthetic_depth_available") or micro.get("synthetic_depth"))
    levels = int(
        _safe_float(
            micro.get("depth_levels")
            or max(
                _safe_float(micro.get("bid_level_count") or micro.get("visible_bid_levels"), 0.0),
                _safe_float(micro.get("ask_level_count") or micro.get("visible_ask_levels"), 0.0),
            ),
            0.0,
        )
    )
    mode = str(micro.get("depth_update_mode") or "").strip().lower()
    fidelity = str(micro.get("dom_source_fidelity") or "").strip().lower()
    true_depth = bool(
        depth_available
        and not synthetic
        and (
            levels >= 2
            or mode in {"event_stream", "stream_snapshot", "snapshot_poll", "fragmented_event_ladder"}
            or fidelity in {"event_ladder", "snapshot_depth", "stream_snapshot"}
        )
    )
    if not true_depth:
        return 0, 0.0, False

    quality = _safe_float(micro.get("depth_quality"), 0.0)
    trust = _safe_float(micro.get("depth_provider_trust_score"), 0.0)
    if quality < 0.24 or trust < 0.50:
        return 0, 0.0, True

    weighted = [
        (_safe_float(micro.get("score"), 0.0), 0.16),
        (_safe_float(micro.get("microstructure_alignment"), 0.0), 0.14),
        (_safe_float(micro.get("book_imbalance"), 0.0), 0.20),
        (_safe_float(micro.get("orderflow_book_imbalance"), 0.0), 0.20),
        (_safe_float(micro.get("orderflow_score"), 0.0), 0.14),
        (_safe_float(micro.get("trade_flow_score"), 0.0), 0.16),
        (_safe_float(micro.get("trade_delta_ratio"), 0.0), 0.08),
        (_safe_float(micro.get("tick_imbalance"), 0.0) * 0.75, 0.06),
    ]
    present = [(value, weight) for value, weight in weighted if abs(value) > 1e-9]
    if not present:
        return 0, 0.0, True
    weight_total = sum(weight for _, weight in present)
    pressure = sum(value * weight for value, weight in present) / max(weight_total, 1e-9)
    strongest = max((value for value, _ in present), key=lambda item: abs(item))
    if abs(pressure) < 0.18 and abs(strongest) >= 0.45:
        same_side = sum(1 for value, _ in present if value * strongest > 0.0 and abs(value) >= 0.14)
        opposite_side = sum(1 for value, _ in present if value * strongest < 0.0 and abs(value) >= 0.24)
        if same_side >= max(1, opposite_side):
            pressure = strongest * 0.65
    strength = abs(pressure)
    if strength < 0.24:
        return 0, strength, True
    return (1 if pressure > 0.0 else -1), strength, True


_COLUMN_SYNONYMS = {
    "open": ("open", "o", "openPrice", "open_price"),
    "high": ("high", "h", "highPrice", "high_price"),
    "low": ("low", "l", "lowPrice", "low_price"),
    "close": (
        "close",
        "c",
        "last",
        "price",
        "mid",
        "mid_price",
        "midPrice",
        "mark",
        "closePrice",
        "close_price",
    ),
}


def _column_lookup(frame: Any) -> Dict[str, Any]:
    try:
        raw_columns = getattr(frame, "columns", None)
        columns = list(raw_columns) if raw_columns is not None else []
    except Exception:
        columns = []
    if not columns and isinstance(frame, Mapping):
        try:
            columns = list(frame.keys())
        except Exception:
            columns = []
    lookup: Dict[str, Any] = {}
    for column in columns:
        token = str(column or "").strip().lower()
        if token and token not in lookup:
            lookup[token] = column
    return lookup


def _find_column(frame: Any, *names: str) -> Optional[Any]:
    lookup = _column_lookup(frame)
    for name in names:
        token = str(name or "").strip().lower()
        if token in lookup:
            return lookup[token]
    return None


def _values_for_column(frame: Any, column: Any) -> List[float]:
    try:
        series = frame[column]
    except Exception:
        return []
    try:
        raw_values = series.tail(160).tolist()
    except Exception:
        try:
            raw_values = list(series)[-160:]
        except Exception:
            return []
    values: List[float] = []
    for value in raw_values:
        numeric = _safe_float(value, 0.0)
        if numeric > 0.0:
            values.append(numeric)
    return values


def _bid_ask_mid_values(frame: Any) -> List[float]:
    bid_col = _find_column(frame, "bid", "Bid", "bid_price", "bidPrice")
    ask_col = _find_column(frame, "ask", "Ask", "ask_price", "askPrice")
    if bid_col is None or ask_col is None:
        return []
    bids = _values_for_column(frame, bid_col)
    asks = _values_for_column(frame, ask_col)
    if not bids or not asks:
        return []
    sample = min(len(bids), len(asks), 160)
    mids: List[float] = []
    for bid, ask in zip(bids[-sample:], asks[-sample:]):
        if bid > 0.0 and ask > 0.0:
            mids.append((bid + ask) / 2.0)
    return mids


def _column_values(frame: Any, name: str) -> List[float]:
    if frame is None:
        return []
    normalized = str(name or "").strip().lower()
    candidates = _COLUMN_SYNONYMS.get(normalized, (name,))
    selected = _find_column(frame, *candidates)
    if selected is not None:
        values = _values_for_column(frame, selected)
        if values:
            return values
    if normalized == "close":
        return _bid_ask_mid_values(frame)
    return []


def _frame_len(frame: Any) -> int:
    try:
        return int(len(frame))
    except Exception:
        return 0


def _interval_weight(interval: str) -> float:
    table = {
        "1m": 0.35,
        "3m": 0.45,
        "5m": 0.70,
        "15m": 1.00,
        "30m": 0.86,
        "1h": 0.76,
        "4h": 0.52,
        "1d": 0.36,
    }
    return table.get(str(interval).lower(), 0.55)


def _atr(highs: List[float], lows: List[float], closes: List[float], period: int = 14) -> float:
    if len(closes) < 2:
        return 0.0
    true_ranges: List[float] = []
    for idx in range(1, min(len(closes), len(highs), len(lows))):
        true_ranges.append(max(highs[idx] - lows[idx], abs(highs[idx] - closes[idx - 1]), abs(lows[idx] - closes[idx - 1])))
    if not true_ranges:
        return 0.0
    sample = true_ranges[-period:]
    return sum(sample) / len(sample)


def _detail(interval: str, frame: Any) -> Dict[str, Any]:
    closes = _column_values(frame, "close")
    highs = _column_values(frame, "high") or closes
    lows = _column_values(frame, "low") or closes
    opens = _column_values(frame, "open") or closes
    if len(closes) < 6:
        return {}
    current = closes[-1]
    atr = _atr(highs, lows, closes)
    atr_pct = atr / current if current > 0.0 else 0.0
    lookback = min(36, len(closes) - 1)
    recent = closes[-lookback:]
    short = sum(closes[-5:]) / min(5, len(closes))
    medium = sum(closes[-20:]) / min(20, len(closes))
    long = sum(closes[-50:]) / min(50, len(closes))
    momentum = (current - closes[-min(lookback, len(closes) - 1)]) / max(current, 1e-9)
    ma_spread = (short - medium) / max(current, 1e-9)
    long_spread = (medium - long) / max(current, 1e-9)
    raw_score = (momentum * 18.0) + (ma_spread * 42.0) + (long_spread * 28.0)
    score = max(-1.0, min(1.0, raw_score))
    high_window = max(highs[-lookback:])
    low_window = min(lows[-lookback:])
    range_span = max(high_window - low_window, atr, current * 0.00001)
    close_location = _clip((current - low_window) / range_span, 0.0, 1.0)
    direction = 1 if score > 0.08 else -1 if score < -0.08 else 0
    extension = abs(current - medium) / max(atr, current * 0.0005)
    body = abs(closes[-1] - opens[-1]) if opens else 0.0
    candle_range = max(highs[-1] - lows[-1], body, current * 0.00001)
    candle_quality = _clip(body / candle_range, 0.0, 1.0)
    if direction > 0:
        breakout = _clip((current - high_window + atr * 0.35) / max(atr * 1.35, 1e-9), 0.0, 1.0)
        pullback = _clip((high_window - current) / max(range_span, 1e-9), 0.0, 1.0)
        target_eff = _clip((high_window + atr * 1.8 - current) / max(atr * 2.5, 1e-9), 0.0, 1.0)
    elif direction < 0:
        breakout = _clip((low_window - current + atr * 0.35) / max(atr * 1.35, 1e-9), 0.0, 1.0)
        pullback = _clip((current - low_window) / max(range_span, 1e-9), 0.0, 1.0)
        target_eff = _clip((current - (low_window - atr * 1.8)) / max(atr * 2.5, 1e-9), 0.0, 1.0)
    else:
        breakout = 0.0
        pullback = _clip(1.0 - abs(close_location - 0.5) * 2.0, 0.0, 1.0)
        target_eff = 0.35
    impulse_age = 0
    for previous in reversed(closes[:-1]):
        if direction > 0 and previous <= medium:
            break
        if direction < 0 and previous >= medium:
            break
        if direction == 0:
            break
        impulse_age += 1
        if impulse_age >= 12:
            break
    return {
        "interval": interval,
        "bars": _frame_len(frame),
        "current_price": current,
        "atr": atr,
        "atr_pct": atr_pct,
        "trend_score": score,
        "trend_state": _trend_label(score),
        "direction_sign": direction,
        "high_window": high_window,
        "low_window": low_window,
        "range_span": range_span,
        "close_location": close_location,
        "breakout_score": breakout,
        "pullback_score": pullback,
        "extension_score": round(min(2.0, extension), 4),
        "target_efficiency_score": target_eff,
        "candle_quality_score": candle_quality,
        "session_quality_score": _clip(0.42 + min(abs(score), 0.8) * 0.35 + target_eff * 0.23),
        "impulse_age_bars": int(impulse_age),
        "volatility_state": "expanded" if atr_pct >= 0.008 else "compressed" if atr_pct <= 0.0012 else "normal",
    }


class MarketStructureService:
    def _empty_analysis(self, asset: str, category: str, context: Optional[Mapping[str, Any]] = None) -> Dict[str, Any]:
        micro_sign, micro_strength, micro_trusted = _trusted_micro_pressure(context)
        if micro_sign:
            pattern_base = "trending_up" if micro_sign > 0 else "trending_down"
            return {
                "asset": asset,
                "category": category,
                "structure_bias": "buy" if micro_sign > 0 else "sell",
                "alignment_score": round(_clip(0.36 + micro_strength * 0.34, 0.0, 0.62), 4),
                "setup_quality": round(_clip(0.32 + micro_strength * 0.32, 0.0, 0.58), 4),
                "opportunity_score": round(_clip(0.36 + micro_strength * 0.34, 0.0, 0.62), 4),
                "weighted_trend_score": 0.0,
                "trend_5m": pattern_base,
                "trend_15m": pattern_base,
                "trend_1h": "unknown",
                "primary_trend_state": pattern_base,
                "pattern_family": f"{pattern_base}_depth_pressure",
                "regime": "depth_pressure",
                "volatility_state": "unknown",
                "pullback_score": 0.0,
                "breakout_score": round(_clip(micro_strength, 0.0, 1.0), 4),
                "extension_score": 0.0,
                "target_efficiency_score": 0.18,
                "session_quality_score": 0.50,
                "candle_quality_score": 0.34,
                "impulse_age_bars": 0,
                "entry_confirmation_bars_required": 1,
                "entry_confirmation_count": 1,
                "entry_confirmation_ready": True,
                "fast_entry_confirmation_bars_required": 1,
                "fast_entry_confirmation_count": 1,
                "fast_entry_confirmation_ready": True,
                "trigger_trend_aligned": True,
                "breakout_retest_ready": False,
                "first_pullback_ready": False,
                "failed_opposite_move_confirmed": False,
                "liquidity_sweep_buy": False,
                "liquidity_sweep_sell": False,
                "close_location": 0.5,
                "elite_pattern_rank": round(_clip(micro_strength * 0.35, 0.0, 0.30), 4),
                "cluster_penalty": 0.0,
                "distance_to_resistance": 0.004,
                "distance_to_support": 0.004,
                "structure_source": "trusted_true_depth_pressure",
                "micro_depth_directional_strength": round(micro_strength, 4),
                "micro_depth_trusted": bool(micro_trusted),
            }
        return {
            "asset": asset,
            "category": category,
            "structure_bias": "neutral",
            "alignment_score": 0.0,
            "setup_quality": 0.0,
            "trend_5m": "unknown",
            "trend_15m": "unknown",
            "trend_1h": "unknown",
            "pattern_family": "ranging_generic",
            "regime": "unknown",
            "volatility_state": "unknown",
        }

    def analyze(
        self,
        asset: str,
        category: str,
        frames: Mapping[str, Any],
        context: Optional[Mapping[str, Any]] = None,
    ) -> Dict[str, Any]:
        details: Dict[str, Dict[str, Any]] = {}
        for interval, frame in dict(frames or {}).items():
            item = _detail(str(interval), frame)
            if item:
                details[str(interval)] = item
        if not details:
            return self._empty_analysis(asset, category, context=context)

        weighted = 0.0
        weight_total = 0.0
        for interval, item in details.items():
            weight = _interval_weight(interval)
            weighted += _safe_float(item.get("trend_score"), 0.0) * weight
            weight_total += weight
        trend_score = weighted / weight_total if weight_total > 0.0 else 0.0
        direction = 1 if trend_score >= 0.10 else -1 if trend_score <= -0.10 else 0
        micro_sign, micro_strength, micro_trusted = _trusted_micro_pressure(context)
        micro_provisional = False
        if direction == 0 and micro_sign:
            direction = micro_sign
            micro_provisional = True
        bias = "buy" if direction > 0 else "sell" if direction < 0 else "neutral"
        primary = details.get("15m") or details.get("5m") or next(iter(details.values()))

        cross = dict((context or {}).get("cross_asset_context") or {})
        micro = dict((context or {}).get("market_microstructure") or {})
        cross_alignment = _safe_float(cross.get("alignment", cross.get("score")), 0.0)
        micro_score = _safe_float(micro.get("score", micro.get("microstructure_alignment")), 0.0)
        context_support = 0.0
        if direction:
            context_support = _clip(max(cross_alignment * direction, micro_score * direction), 0.0, 1.0)
        if micro_provisional:
            context_support = max(context_support, _clip(micro_strength, 0.0, 1.0))

        alignment_score = _clip(abs(trend_score) * 0.70 + context_support * 0.30)
        breakout_score = _safe_float(primary.get("breakout_score"), 0.0) * direction if direction else 0.0
        pullback_score = _safe_float(primary.get("pullback_score"), 0.0) * direction if direction else 0.0
        dominant_setup = max(abs(breakout_score), abs(pullback_score))
        setup_quality = _clip(dominant_setup * 0.45 + alignment_score * 0.35 + _safe_float(primary.get("candle_quality_score"), 0.0) * 0.20)
        if micro_provisional:
            alignment_score = max(alignment_score, _clip(0.36 + micro_strength * 0.34, 0.0, 0.62))
            setup_quality = max(setup_quality, _clip(0.32 + micro_strength * 0.32, 0.0, 0.58))
        extension = _safe_float(primary.get("extension_score"), 0.0)
        target_eff = _safe_float(primary.get("target_efficiency_score"), 0.0)
        impulse_age = int(_safe_float(primary.get("impulse_age_bars"), 0.0))
        confirmation_required = 2 if alignment_score < 0.58 else 1
        confirmation_count = 1 if setup_quality >= 0.38 and extension <= 1.18 else 0
        confirmation_ready = confirmation_count >= confirmation_required
        fast_ready = bool(confirmation_count >= 1 and alignment_score >= 0.32 and target_eff >= 0.16)
        trend_5m = str(details.get("5m", primary).get("trend_state", "unknown"))
        trigger_item = details.get("5m", primary)
        trigger_direction = int(_safe_float(trigger_item.get("direction_sign"), 0.0))
        pattern_base = "trending_up" if direction > 0 else "trending_down" if direction < 0 else "ranging"
        if micro_provisional:
            trend_5m = pattern_base
        regime = "trend" if abs(trend_score) >= 0.22 else "transition" if abs(trend_score) >= 0.10 else "range"
        if micro_provisional:
            regime = "depth_pressure"
        current_price = _safe_float(primary.get("current_price"), 0.0)
        atr = _safe_float(primary.get("atr"), 0.0)
        high_window = _safe_float(primary.get("high_window"), current_price)
        low_window = _safe_float(primary.get("low_window"), current_price)
        distance_to_resistance = (high_window - current_price) / max(current_price, 1e-9) if current_price > 0 else 0.0
        distance_to_support = (current_price - low_window) / max(current_price, 1e-9) if current_price > 0 else 0.0

        return {
            "asset": asset,
            "category": category,
            "structure_bias": bias,
            "alignment_score": round(alignment_score, 4),
            "setup_quality": round(setup_quality, 4),
            "opportunity_score": round(max(alignment_score, setup_quality), 4),
            "weighted_trend_score": round(trend_score, 4),
            "trend_5m": trend_5m,
            "trend_15m": pattern_base if micro_provisional else str(details.get("15m", primary).get("trend_state", "unknown")),
            "trend_1h": str(details.get("1h", primary).get("trend_state", "unknown")),
            "primary_trend_state": str(primary.get("trend_state", "unknown")),
            "pattern_family": f"{pattern_base}_{'depth_pressure' if micro_provisional else 'generic'}",
            "regime": regime,
            "volatility_state": str(primary.get("volatility_state", "unknown")),
            "pullback_score": round(pullback_score, 4),
            "breakout_score": round(breakout_score, 4),
            "extension_score": round(extension, 4),
            "target_efficiency_score": round(target_eff, 4),
            "session_quality_score": round(_safe_float(primary.get("session_quality_score"), 0.0), 4),
            "candle_quality_score": round(_safe_float(primary.get("candle_quality_score"), 0.0), 4),
            "impulse_age_bars": impulse_age,
            "entry_confirmation_bars_required": confirmation_required,
            "entry_confirmation_count": confirmation_count,
            "entry_confirmation_ready": bool(confirmation_ready),
            "fast_entry_confirmation_bars_required": 1,
            "fast_entry_confirmation_count": 1 if fast_ready else 0,
            "fast_entry_confirmation_ready": bool(fast_ready),
            "trigger_trend_aligned": bool(micro_provisional or (direction and trigger_direction == direction)),
            "breakout_retest_ready": bool(abs(breakout_score) >= 0.34 and target_eff >= 0.20),
            "first_pullback_ready": bool(abs(pullback_score) >= 0.36 and extension <= 1.05),
            "failed_opposite_move_confirmed": False,
            "liquidity_sweep_buy": bool(direction > 0 and _safe_float(primary.get("close_location"), 0.5) >= 0.70),
            "liquidity_sweep_sell": bool(direction < 0 and _safe_float(primary.get("close_location"), 0.5) <= 0.30),
            "close_location": round(_safe_float(primary.get("close_location"), 0.5), 4),
            "elite_pattern_rank": round(setup_quality * alignment_score, 4),
            "cluster_penalty": round(_clip(extension - 1.0, 0.0, 1.0), 4),
            "dominant_exhaustion_score": round(_clip(extension / 1.7, 0.0, 1.0), 4),
            "bias_exhausted": bool(extension >= 1.32 or impulse_age > 8),
            "distance_to_resistance": round(max(0.0, distance_to_resistance), 6),
            "distance_to_support": round(max(0.0, distance_to_support), 6),
            "atr": round(atr, 8),
            "current_price": round(current_price, 8),
            "structure_frames": {interval: {"bars": item.get("bars"), "trend_state": item.get("trend_state")} for interval, item in details.items()},
            "regime_entry_policy": {
                "min_setup_quality": 0.30,
                "min_candle_quality": 0.26,
                "max_extension_score": 1.18,
                "min_target_efficiency": 0.16,
                "max_impulse_age_bars": 6,
            },
            "structure_source": "trusted_true_depth_pressure" if micro_provisional else "price_structure",
            "micro_depth_directional_strength": round(micro_strength, 4),
            "micro_depth_trusted": bool(micro_trusted),
        }


_service = MarketStructureService()


def get_service() -> MarketStructureService:
    return _service
