from __future__ import annotations

import statistics
import threading
import time
from collections import deque
from datetime import datetime
from typing import Any, Deque, Dict, List, Optional, Tuple

from services.dom_evidence import attach_dom_evidence


def _clip(value: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, float(value or 0.0)))


def _clip11(value: float) -> float:
    return max(-1.0, min(1.0, float(value or 0.0)))


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value or default)
    except Exception:
        return default


def _safe_ts(value: Any) -> float:
    if isinstance(value, datetime):
        return float(value.timestamp())
    try:
        numeric = float(value)
        if numeric > 10_000_000_000:
            numeric /= 1000.0
        if numeric > 1_000_000:
            return numeric
    except Exception:
        pass
    return time.time()


def _flag_tokens(value: Any) -> set[str]:
    raw = str(value or "").strip().lower()
    if not raw:
        return set()
    normalized = raw.replace(";", ",").replace("|", ",")
    return {token.strip() for token in normalized.split(",") if token.strip()}


def _normalize_provider(provider: str) -> str:
    token = str(provider or "").strip().lower()
    if token.startswith("ig"):
        return "ig"
    if token.startswith("deriv"):
        return "deriv"
    if token.startswith("binance"):
        return "binance"
    return token or "unknown"


def estimate_true_depth_metrics(
    levels: Optional[List[Dict[str, Any]]] = None,
    *,
    bid_size: Any = None,
    ask_size: Any = None,
) -> Dict[str, Any]:
    bid_depth = 0.0
    ask_depth = 0.0
    depth_levels = 0
    bid_level_count = 0
    ask_level_count = 0

    normalized_levels = list(levels or [])
    if normalized_levels:
        for level in normalized_levels:
            if not isinstance(level, dict):
                continue
            bid_size_level = _safe_float(level.get("bid_size"), 0.0)
            ask_size_level = _safe_float(level.get("ask_size"), 0.0)
            if level.get("bid") not in (None, "") and bid_size_level > 0:
                bid_level_count += 1
            if level.get("ask") not in (None, "") and ask_size_level > 0:
                ask_level_count += 1
            if bid_size_level > 0 or ask_size_level > 0:
                depth_levels += 1
            bid_depth += max(0.0, bid_size_level)
            ask_depth += max(0.0, ask_size_level)
    else:
        bid_depth = max(0.0, _safe_float(bid_size, 0.0))
        ask_depth = max(0.0, _safe_float(ask_size, 0.0))
        depth_levels = 1 if (bid_depth > 0 or ask_depth > 0) else 0
        bid_level_count = 1 if bid_depth > 0 else 0
        ask_level_count = 1 if ask_depth > 0 else 0

    total_depth = bid_depth + ask_depth
    book_imbalance = (bid_depth - ask_depth) / total_depth if total_depth > 0 else 0.0
    synthetic_depth_available = False
    synthetic_book_imbalance = 0.0

    visible_levels = max(depth_levels, bid_level_count, ask_level_count)
    level_balance = 1.0
    if max(bid_level_count, ask_level_count) > 0:
        level_balance = min(bid_level_count, ask_level_count) / max(bid_level_count, ask_level_count)
    if visible_levels >= 10:
        depth_quality = 1.0
        depth_quality_tier = "full"
    elif visible_levels >= 8:
        depth_quality = 0.88
        depth_quality_tier = "strong"
    elif visible_levels >= 6:
        depth_quality = 0.74
        depth_quality_tier = "solid"
    elif visible_levels >= 4:
        depth_quality = 0.58
        depth_quality_tier = "partial"
    elif visible_levels >= 2:
        depth_quality = 0.36
        depth_quality_tier = "thin"
    elif visible_levels >= 1:
        depth_quality = 0.18
        depth_quality_tier = "top_only"
    else:
        depth_quality = 0.0
        depth_quality_tier = "none"
    if visible_levels > 0:
        depth_quality = _clip(depth_quality * (0.85 + level_balance * 0.15))

    return {
        "bid_depth": bid_depth,
        "ask_depth": ask_depth,
        "depth_levels": depth_levels,
        "bid_level_count": bid_level_count,
        "ask_level_count": ask_level_count,
        "total_depth": total_depth,
        "book_imbalance": book_imbalance,
        "synthetic_depth_available": synthetic_depth_available,
        "synthetic_book_imbalance": synthetic_book_imbalance,
        "depth_quality": depth_quality,
        "depth_quality_tier": depth_quality_tier,
    }


class LiveMicrostructureService:
    def __init__(self, maxlen: int = 64) -> None:
        self._lock = threading.RLock()
        self._maxlen = int(max(8, maxlen))
        self._quotes: Dict[Tuple[str, str], Deque[Dict[str, Any]]] = {}

    def record_quote(
        self,
        provider: str,
        asset: str,
        *,
        bid: Any = None,
        ask: Any = None,
        price: Any = None,
        bid_size: Any = None,
        ask_size: Any = None,
        levels: Optional[List[Dict[str, Any]]] = None,
        timestamp: Any = None,
        flags: str = "",
        trade_size: Any = None,
        trade_side: str = "",
        event_type: str = "",
    ) -> None:
        provider_key = _normalize_provider(provider)
        asset_key = str(asset or "").strip()
        if not asset_key:
            return

        bid_value = _safe_float(bid, 0.0) if bid not in (None, "") else None
        ask_value = _safe_float(ask, 0.0) if ask not in (None, "") else None
        price_value = _safe_float(price, 0.0) if price not in (None, "") else None
        if price_value is None or price_value <= 0.0:
            if bid_value and ask_value:
                price_value = (bid_value + ask_value) / 2.0
            else:
                price_value = ask_value if ask_value and ask_value > 0 else bid_value
        if price_value is None or price_value <= 0.0:
            return

        normalized_levels: List[Dict[str, Any]] = []
        for level in levels or []:
            if not isinstance(level, dict):
                continue
            bid_level = _safe_float(level.get("bid"), 0.0) if level.get("bid") not in (None, "") else None
            ask_level = _safe_float(level.get("ask"), 0.0) if level.get("ask") not in (None, "") else None
            bid_size_level = _safe_float(level.get("bid_size"), 0.0) if level.get("bid_size") not in (None, "") else None
            ask_size_level = _safe_float(level.get("ask_size"), 0.0) if level.get("ask_size") not in (None, "") else None
            normalized_levels.append(
                {
                    "bid": bid_level,
                    "ask": ask_level,
                    "bid_size": bid_size_level,
                    "ask_size": ask_size_level,
                }
            )

        normalized_event_type = str(event_type or "").strip().lower()
        normalized_flags = str(flags or "").strip()
        if not normalized_event_type and (
            normalized_levels
            or bid_size not in (None, "")
            or ask_size not in (None, "")
        ):
            normalized_event_type = "depth_snapshot"
            if not normalized_flags:
                normalized_flags = "depth_snapshot"

        event = {
            "timestamp": _safe_ts(timestamp),
            "price": float(price_value),
            "bid": bid_value,
            "ask": ask_value,
            "bid_size": _safe_float(bid_size, 0.0) if bid_size not in (None, "") else None,
            "ask_size": _safe_float(ask_size, 0.0) if ask_size not in (None, "") else None,
            "levels": normalized_levels,
            "flags": normalized_flags,
            "trade_size": _safe_float(trade_size, 0.0) if trade_size not in (None, "") else None,
            "trade_side": str(trade_side or "").strip().lower(),
            "event_type": normalized_event_type,
        }
        with self._lock:
            bucket = self._quotes.setdefault((provider_key, asset_key), deque(maxlen=self._maxlen))
            bucket.append(event)

    def record_depth_delta(
        self,
        provider: str,
        asset: str,
        *,
        bid: Any = None,
        ask: Any = None,
        price: Any = None,
        levels: Optional[List[Dict[str, Any]]] = None,
        timestamp: Any = None,
        flags: str = "depth_delta",
    ) -> None:
        self.record_quote(
            provider,
            asset,
            bid=bid,
            ask=ask,
            price=price,
            levels=levels,
            timestamp=timestamp,
            flags=flags,
            event_type="depth_delta",
        )

    def record_trade(
        self,
        provider: str,
        asset: str,
        *,
        price: Any,
        size: Any = None,
        side: str = "",
        bid: Any = None,
        ask: Any = None,
        timestamp: Any = None,
        flags: str = "trade_print",
    ) -> None:
        self.record_quote(
            provider,
            asset,
            bid=bid,
            ask=ask,
            price=price,
            timestamp=timestamp,
            flags=flags,
            trade_size=size,
            trade_side=side,
            event_type="trade_print",
        )

    @staticmethod
    def _series_metrics(events: List[Dict[str, Any]], current_price: float, spread: Any) -> Dict[str, Any]:
        mids = [float(evt.get("price")) for evt in events if _safe_float(evt.get("price"), 0.0) > 0.0]
        deltas = [curr - prev for prev, curr in zip(mids, mids[1:]) if abs(curr - prev) > 1e-12]
        up_ticks = sum(1 for delta in deltas if delta > 0)
        down_ticks = sum(1 for delta in deltas if delta < 0)
        total_ticks = up_ticks + down_ticks
        tick_imbalance = ((up_ticks - down_ticks) / total_ticks) if total_ticks else 0.0

        velocity_bps = 0.0
        if len(mids) >= 2 and mids[-1] > 0:
            velocity_bps = ((mids[-1] - mids[0]) / mids[-1]) * 10000.0

        latest_delta_bps = 0.0
        if deltas and mids[-1] > 0:
            latest_delta_bps = (deltas[-1] / mids[-1]) * 10000.0

        spread_history = []
        bid_series = []
        ask_series = []
        for evt in events:
            bid_value = evt.get("bid")
            ask_value = evt.get("ask")
            price_value = _safe_float(evt.get("price"), 0.0)
            if bid_value not in (None, ""):
                bid_series.append(_safe_float(bid_value, 0.0))
            if ask_value not in (None, ""):
                ask_series.append(_safe_float(ask_value, 0.0))
            if price_value <= 0.0 or bid_value in (None, "") or ask_value in (None, ""):
                continue
            spread_history.append(max(0.0, (_safe_float(ask_value) - _safe_float(bid_value)) / price_value * 10000.0))

        baseline_spread_bps = _safe_float(spread, 0.0)
        if spread_history:
            try:
                baseline_spread_bps = statistics.median(spread_history[:-1] or spread_history)
            except Exception:
                baseline_spread_bps = spread_history[-1]
        spread_stress = (_safe_float(spread, 0.0) / max(baseline_spread_bps, 0.01)) if baseline_spread_bps > 0 else 1.0

        return {
            "mids": mids,
            "deltas": deltas,
            "tick_imbalance": tick_imbalance,
            "velocity_bps": velocity_bps,
            "latest_delta_bps": latest_delta_bps,
            "spread_history": spread_history,
            "bid_series": bid_series,
            "ask_series": ask_series,
            "baseline_spread_bps": baseline_spread_bps,
            "spread_stress": spread_stress,
        }

    @staticmethod
    def _depth_metrics(
        latest: Dict[str, Any],
        bid_series: List[float],
        ask_series: List[float],
    ) -> Dict[str, Any]:
        if latest.get("levels"):
            return estimate_true_depth_metrics(latest.get("levels"))

        bid_depth = 0.0
        ask_depth = 0.0
        depth_levels = 0
        bid_level_count = 0
        ask_level_count = 0
        if not latest.get("levels"):
            bid_depth = max(0.0, _safe_float(latest.get("bid_size"), 0.0))
            ask_depth = max(0.0, _safe_float(latest.get("ask_size"), 0.0))
            depth_levels = 1 if (bid_depth > 0 or ask_depth > 0) else 0
            bid_level_count = 1 if bid_depth > 0 else 0
            ask_level_count = 1 if ask_depth > 0 else 0

        total_depth = bid_depth + ask_depth
        synthetic_depth_available = False
        synthetic_book_imbalance = 0.0
        if total_depth > 0:
            book_imbalance = (bid_depth - ask_depth) / total_depth
        else:
            book_imbalance = 0.0
            if len(bid_series) >= 3 and len(ask_series) >= 3:
                synthetic_depth_available = True
                first_bid = bid_series[0]
                last_bid = bid_series[-1]
                first_ask = ask_series[0]
                last_ask = ask_series[-1]
                quote_move_norm = max(abs(last_bid - first_bid) + abs(last_ask - first_ask), 1e-9)
                quote_skew = (last_bid - first_bid - (last_ask - first_ask)) / quote_move_norm
                spread_pressure = 0.0
                synthetic_book_imbalance = _clip11(
                    quote_skew * 0.30 + spread_pressure * 0.25
                )
                book_imbalance = synthetic_book_imbalance

        visible_levels = max(depth_levels, bid_level_count, ask_level_count)
        level_balance = 1.0
        if max(bid_level_count, ask_level_count) > 0:
            level_balance = min(bid_level_count, ask_level_count) / max(bid_level_count, ask_level_count)
        if visible_levels >= 10:
            depth_quality = 1.0
            depth_quality_tier = "full"
        elif visible_levels >= 8:
            depth_quality = 0.88
            depth_quality_tier = "strong"
        elif visible_levels >= 6:
            depth_quality = 0.74
            depth_quality_tier = "solid"
        elif visible_levels >= 4:
            depth_quality = 0.58
            depth_quality_tier = "partial"
        elif visible_levels >= 2:
            depth_quality = 0.36
            depth_quality_tier = "thin"
        elif visible_levels >= 1:
            depth_quality = 0.18
            depth_quality_tier = "top_only"
        else:
            depth_quality = 0.0
            depth_quality_tier = "synthetic" if synthetic_depth_available else "none"
        if visible_levels > 0:
            depth_quality = _clip(depth_quality * (0.85 + level_balance * 0.15))

        return {
            "bid_depth": bid_depth,
            "ask_depth": ask_depth,
            "depth_levels": depth_levels,
            "bid_level_count": bid_level_count,
            "ask_level_count": ask_level_count,
            "total_depth": total_depth,
            "book_imbalance": book_imbalance,
            "synthetic_depth_available": synthetic_depth_available,
            "synthetic_book_imbalance": synthetic_book_imbalance,
            "depth_quality": depth_quality,
            "depth_quality_tier": depth_quality_tier,
        }

    @staticmethod
    def _risk_metrics(
        tick_imbalance: float,
        velocity_bps: float,
        latest_delta_bps: float,
        spread_stress: float,
        book_imbalance: float,
        total_depth: float,
        flags: str,
    ) -> Dict[str, Any]:
        trend_sign = 1.0 if velocity_bps > 0 else (-1.0 if velocity_bps < 0 else (1.0 if latest_delta_bps > 0 else (-1.0 if latest_delta_bps < 0 else 0.0)))
        latest_sign = 1.0 if latest_delta_bps > 0 else (-1.0 if latest_delta_bps < 0 else 0.0)
        reversal = bool(trend_sign and latest_sign and trend_sign != latest_sign)
        weak_depth = total_depth <= 0 or abs(book_imbalance) < 0.08
        opposing_depth = bool(trend_sign and book_imbalance and (trend_sign * book_imbalance) < -0.10)

        stop_hunt_risk = 0.0
        if abs(velocity_bps) >= 1.2:
            stop_hunt_risk += min(0.28, abs(velocity_bps) / 10.0)
        if spread_stress >= 1.35:
            stop_hunt_risk += min(0.22, (spread_stress - 1.0) * 0.18)
        if reversal:
            stop_hunt_risk += 0.22
        if opposing_depth:
            stop_hunt_risk += 0.18
        elif weak_depth and abs(velocity_bps) >= 2.0:
            stop_hunt_risk += 0.10
        if "EDIT" in str(flags or "").upper():
            stop_hunt_risk += 0.05
        stop_hunt_risk = _clip(stop_hunt_risk)

        exhaustion_risk = 0.0
        if abs(tick_imbalance) >= 0.65 and abs(latest_delta_bps) <= max(0.08, abs(velocity_bps) * 0.12):
            exhaustion_risk += 0.18
        if reversal:
            exhaustion_risk += 0.28
        if spread_stress >= 1.45 and abs(tick_imbalance) >= 0.45:
            exhaustion_risk += 0.14
        exhaustion_risk = _clip(exhaustion_risk)

        pressure_score = _clip11(tick_imbalance * 0.55 + book_imbalance * 0.45)
        velocity_score = _clip11(velocity_bps / 6.0)
        spread_penalty = _clip((spread_stress - 1.0) / 2.0, 0.0, 0.35)
        score = _clip11(
            pressure_score * 0.65
            + velocity_score * 0.35
            - stop_hunt_risk * 0.55
            - exhaustion_risk * 0.25
            - spread_penalty
        )

        pressure_direction = "NEUTRAL"
        if score >= 0.12:
            pressure_direction = "BUY"
        elif score <= -0.12:
            pressure_direction = "SELL"

        return {
            "stop_hunt_risk": stop_hunt_risk,
            "exhaustion_risk": exhaustion_risk,
            "score": score,
            "pressure_direction": pressure_direction,
        }

    @staticmethod
    def _depth_state(event: Dict[str, Any]) -> Dict[str, float]:
        levels = list(event.get("levels") or [])
        best_bid = event.get("bid")
        best_ask = event.get("ask")
        best_bid_size = event.get("bid_size")
        best_ask_size = event.get("ask_size")
        bid_depth = 0.0
        ask_depth = 0.0

        for idx, level in enumerate(levels):
            if not isinstance(level, dict):
                continue
            bid_level = level.get("bid")
            ask_level = level.get("ask")
            bid_size_level = _safe_float(level.get("bid_size"), 0.0) if level.get("bid_size") not in (None, "") else 0.0
            ask_size_level = _safe_float(level.get("ask_size"), 0.0) if level.get("ask_size") not in (None, "") else 0.0
            bid_depth += max(0.0, bid_size_level)
            ask_depth += max(0.0, ask_size_level)
            if idx == 0:
                if best_bid in (None, "") and bid_level not in (None, ""):
                    best_bid = _safe_float(bid_level, 0.0)
                if best_ask in (None, "") and ask_level not in (None, ""):
                    best_ask = _safe_float(ask_level, 0.0)
                if best_bid_size in (None, "") and bid_size_level > 0.0:
                    best_bid_size = bid_size_level
                if best_ask_size in (None, "") and ask_size_level > 0.0:
                    best_ask_size = ask_size_level

        best_bid_value = _safe_float(best_bid, 0.0)
        best_ask_value = _safe_float(best_ask, 0.0)
        best_bid_size_value = _safe_float(best_bid_size, 0.0)
        best_ask_size_value = _safe_float(best_ask_size, 0.0)
        if bid_depth <= 0.0:
            bid_depth = max(0.0, best_bid_size_value)
        if ask_depth <= 0.0:
            ask_depth = max(0.0, best_ask_size_value)

        mid_price = 0.0
        if best_bid_value > 0.0 and best_ask_value > 0.0:
            mid_price = (best_bid_value + best_ask_value) / 2.0
        elif _safe_float(event.get("price"), 0.0) > 0.0:
            mid_price = _safe_float(event.get("price"), 0.0)
        total_depth = bid_depth + ask_depth
        imbalance = ((bid_depth - ask_depth) / total_depth) if total_depth > 0.0 else 0.0
        return {
            "mid_price": float(mid_price),
            "best_bid": float(best_bid_value),
            "best_ask": float(best_ask_value),
            "best_bid_size": float(best_bid_size_value),
            "best_ask_size": float(best_ask_size_value),
            "bid_depth": float(bid_depth),
            "ask_depth": float(ask_depth),
            "total_depth": float(total_depth),
            "imbalance": float(imbalance),
            "timestamp": _safe_ts(event.get("timestamp")),
        }

    @staticmethod
    def _ladder_proxy_metrics(events: List[Dict[str, Any]]) -> Dict[str, Any]:
        depth_events = []
        for evt in events:
            flags = _flag_tokens(evt.get("flags"))
            event_type = str(evt.get("event_type") or "").strip().lower()
            has_depth_payload = bool(
                evt.get("levels")
                or evt.get("bid_size") not in (None, "")
                or evt.get("ask_size") not in (None, "")
                or "depth_snapshot" in flags
                or event_type == "depth_snapshot"
            )
            if has_depth_payload:
                depth_events.append(evt)

        if len(depth_events) < 2:
            return {
                "dom_depth_window": int(len(depth_events)),
                "dom_liquidity_shift_proxy": 0.0,
                "dom_sweep_pressure_proxy": 0.0,
                "dom_refill_resilience_proxy": 0.0,
                "dom_absorption_proxy": 0.0,
                "dom_iceberg_proxy": 0.0,
                "dom_queue_persistence": 0.0,
                "dom_supportive_reload_count": 0,
            }

        states = [LiveMicrostructureService._depth_state(evt) for evt in depth_events]
        pair_count = 0
        stable_top_pairs = 0.0
        liquidity_shift_sum = 0.0
        sweep_pressure_sum = 0.0
        refill_sum = 0.0
        absorption_sum = 0.0
        absorption_hits = 0

        for prev, curr in zip(states, states[1:]):
            pair_count += 1
            prev_total = max(prev["total_depth"], 1e-9)
            curr_total = max(curr["total_depth"], 1e-9)
            depth_norm = max(prev_total, curr_total, 1.0)
            ref_price = max(prev["mid_price"], curr["mid_price"], 1e-9)
            price_delta = curr["mid_price"] - prev["mid_price"]
            price_delta_bps = (price_delta / ref_price) * 10000.0
            price_sign = 1.0 if price_delta_bps > 0.02 else -1.0 if price_delta_bps < -0.02 else 0.0

            same_bid = (
                prev["best_bid"] > 0.0
                and curr["best_bid"] > 0.0
                and abs(prev["best_bid"] - curr["best_bid"]) <= max(ref_price * 1e-6, 1e-8)
            )
            same_ask = (
                prev["best_ask"] > 0.0
                and curr["best_ask"] > 0.0
                and abs(prev["best_ask"] - curr["best_ask"]) <= max(ref_price * 1e-6, 1e-8)
            )
            if same_bid and same_ask:
                stable_top_pairs += 1.0
            elif same_bid or same_ask:
                stable_top_pairs += 0.5

            net_shift = (
                (curr["bid_depth"] - prev["bid_depth"])
                - (curr["ask_depth"] - prev["ask_depth"])
            ) / depth_norm
            imbalance_shift = curr["imbalance"] - prev["imbalance"]
            liquidity_shift_sum += _clip11(net_shift * 0.7 + imbalance_shift * 0.3)

            if price_sign > 0:
                opposing_depth_depletion = max(0.0, prev["ask_depth"] - curr["ask_depth"]) / max(prev["ask_depth"], 1e-9)
                supportive_refill = max(0.0, curr["bid_depth"] - prev["bid_depth"]) / max(prev["bid_depth"], 1e-9)
                opposing_refill = max(0.0, curr["ask_depth"] - prev["ask_depth"]) / max(prev["ask_depth"], 1e-9)
                sweep_pressure_sum += opposing_depth_depletion - opposing_refill * 0.45
                refill_sum += supportive_refill - opposing_refill * 0.35
                if abs(price_delta_bps) <= 0.18 and curr["imbalance"] >= 0.08 and supportive_refill >= 0.06:
                    absorption_hits += 1
                    absorption_sum += min(1.0, supportive_refill + curr["imbalance"] * 0.55)
            elif price_sign < 0:
                opposing_depth_depletion = max(0.0, prev["bid_depth"] - curr["bid_depth"]) / max(prev["bid_depth"], 1e-9)
                supportive_refill = max(0.0, curr["ask_depth"] - prev["ask_depth"]) / max(prev["ask_depth"], 1e-9)
                opposing_refill = max(0.0, curr["bid_depth"] - prev["bid_depth"]) / max(prev["bid_depth"], 1e-9)
                sweep_pressure_sum -= opposing_depth_depletion - opposing_refill * 0.45
                refill_sum -= supportive_refill - opposing_refill * 0.35
                if abs(price_delta_bps) <= 0.18 and curr["imbalance"] <= -0.08 and supportive_refill >= 0.06:
                    absorption_hits += 1
                    absorption_sum -= min(1.0, supportive_refill + abs(curr["imbalance"]) * 0.55)

        reload_count = 0
        iceberg_sum = 0.0
        triplet_count = 0
        for first, second, third in zip(states, states[1:], states[2:]):
            triplet_count += 1
            ref_price = max(first["mid_price"], second["mid_price"], third["mid_price"], 1e-9)
            stable_mid = max(
                abs(second["mid_price"] - first["mid_price"]),
                abs(third["mid_price"] - second["mid_price"]),
            ) <= ref_price * 0.00004

            same_bid_price = (
                first["best_bid"] > 0.0
                and abs(first["best_bid"] - second["best_bid"]) <= max(ref_price * 1e-6, 1e-8)
                and abs(second["best_bid"] - third["best_bid"]) <= max(ref_price * 1e-6, 1e-8)
            )
            same_ask_price = (
                first["best_ask"] > 0.0
                and abs(first["best_ask"] - second["best_ask"]) <= max(ref_price * 1e-6, 1e-8)
                and abs(second["best_ask"] - third["best_ask"]) <= max(ref_price * 1e-6, 1e-8)
            )

            bid_reload = (
                stable_mid
                and same_bid_price
                and second["best_bid_size"] > 0.0
                and third["best_bid_size"] > second["best_bid_size"] * 1.18
                and second["best_bid_size"] < first["best_bid_size"] * 0.92
            )
            ask_reload = (
                stable_mid
                and same_ask_price
                and second["best_ask_size"] > 0.0
                and third["best_ask_size"] > second["best_ask_size"] * 1.18
                and second["best_ask_size"] < first["best_ask_size"] * 0.92
            )
            if bid_reload:
                reload_count += 1
                iceberg_sum += min(
                    1.0,
                    (third["best_bid_size"] - second["best_bid_size"]) / max(second["best_bid_size"], 1e-9),
                )
            if ask_reload:
                reload_count += 1
                iceberg_sum -= min(
                    1.0,
                    (third["best_ask_size"] - second["best_ask_size"]) / max(second["best_ask_size"], 1e-9),
                )

        depth_window = len(depth_events)
        queue_persistence = stable_top_pairs / max(1, pair_count)
        liquidity_shift_proxy = _clip11(liquidity_shift_sum / max(1, pair_count))
        sweep_pressure_proxy = _clip11(sweep_pressure_sum / max(1, pair_count))
        refill_resilience_proxy = _clip11(refill_sum / max(1, pair_count))
        absorption_proxy = _clip11(absorption_sum / max(1, absorption_hits or pair_count))
        iceberg_proxy = _clip11(iceberg_sum / max(1, triplet_count))
        return {
            "dom_depth_window": int(depth_window),
            "dom_liquidity_shift_proxy": round(liquidity_shift_proxy, 4),
            "dom_sweep_pressure_proxy": round(sweep_pressure_proxy, 4),
            "dom_refill_resilience_proxy": round(refill_resilience_proxy, 4),
            "dom_absorption_proxy": round(absorption_proxy, 4),
            "dom_iceberg_proxy": round(iceberg_proxy, 4),
            "dom_queue_persistence": round(_clip(queue_persistence, 0.0, 1.0), 4),
            "dom_supportive_reload_count": int(reload_count),
        }

    @staticmethod
    def _event_metrics(events: List[Dict[str, Any]], depth_available: bool, synthetic_depth_available: bool) -> Dict[str, Any]:
        snapshot_count = 0
        delta_count = 0
        trade_count = 0
        latest_flags = ""
        latest_ts = 0.0
        earliest_snapshot_ts = 0.0
        for evt in events:
            event_type = str(evt.get("event_type") or "").strip().lower()
            flags = _flag_tokens(evt.get("flags"))
            latest_ts = max(latest_ts, _safe_ts(evt.get("timestamp")))
            latest_flags = ",".join(sorted(flags)) or latest_flags
            has_depth_payload = bool(
                evt.get("levels")
                or evt.get("bid_size") not in (None, "")
                or evt.get("ask_size") not in (None, "")
            )
            if "depth_snapshot" in flags or "stream_snapshot" in flags or event_type == "depth_snapshot" or has_depth_payload:
                snapshot_count += 1
                ts = _safe_ts(evt.get("timestamp"))
                if earliest_snapshot_ts <= 0.0:
                    earliest_snapshot_ts = ts
                else:
                    earliest_snapshot_ts = min(earliest_snapshot_ts, ts)
            if any(token in flags for token in ("depth_delta", "book_delta", "ladder_delta")) or event_type == "depth_delta":
                delta_count += 1
            if any(token in flags for token in ("trade_print", "trade_stream", "tape_print")) or event_type == "trade_print":
                trade_count += 1

        event_age_seconds = round(max(0.0, time.time() - latest_ts), 3) if latest_ts > 0.0 else 0.0
        snapshot_span_seconds = round(max(0.0, latest_ts - earliest_snapshot_ts), 3) if earliest_snapshot_ts > 0.0 else 0.0
        stream_snapshot_ready = bool(
            depth_available
            and snapshot_count >= 3
            and delta_count == 0
            and event_age_seconds <= 25.0
            and snapshot_span_seconds <= 180.0
        )

        if depth_available and (delta_count > 0 or trade_count > 0):
            depth_update_mode = "event_stream"
        elif depth_available and stream_snapshot_ready:
            depth_update_mode = "stream_snapshot"
        elif depth_available and snapshot_count > 0:
            depth_update_mode = "snapshot_poll"
        elif depth_available:
            depth_update_mode = "top_of_book"
        elif synthetic_depth_available:
            depth_update_mode = "synthetic"
        elif events:
            depth_update_mode = "top_quote"
        else:
            depth_update_mode = "none"

        return {
            "dom_snapshot_count": int(snapshot_count),
            "dom_delta_count": int(delta_count),
            "dom_trade_count": int(trade_count),
            "depth_update_mode": depth_update_mode,
            "dom_stream_snapshot_ready": stream_snapshot_ready,
            "dom_depth_event_age_seconds": event_age_seconds,
            "dom_snapshot_span_seconds": snapshot_span_seconds,
            "flags": latest_flags,
        }

    def get_snapshot(
        self,
        provider: str,
        asset: str,
        *,
        price: Any = None,
        spread: Any = None,
        meta: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        provider_key = _normalize_provider(provider)
        asset_key = str(asset or "").strip()
        with self._lock:
            events = list(self._quotes.get((provider_key, asset_key), ()))
        if not events and price in (None, "", 0):
            return {}

        latest = events[-1] if events else {}
        latest_depth = latest
        for evt in reversed(events):
            flags = _flag_tokens(evt.get("flags"))
            event_type = str(evt.get("event_type") or "").strip().lower()
            has_depth_payload = bool(
                evt.get("levels")
                or evt.get("bid_size") not in (None, "")
                or evt.get("ask_size") not in (None, "")
                or "depth_snapshot" in flags
                or "stream_snapshot" in flags
                or "depth_delta" in flags
                or "book_delta" in flags
                or "ladder_delta" in flags
                or event_type in {"depth_snapshot", "depth_delta"}
            )
            if has_depth_payload:
                latest_depth = evt
                break

        current_price = _safe_float(price, latest.get("price", 0.0))
        if current_price <= 0.0:
            return {}

        current_bid = latest.get("bid")
        if current_bid in (None, ""):
            current_bid = latest_depth.get("bid")
        current_ask = latest.get("ask")
        if current_ask in (None, ""):
            current_ask = latest_depth.get("ask")
        spread_bps = round((_safe_float(spread, 0.0) / current_price) * 10000.0, 3) if current_price > 0 else 0.0
        series = self._series_metrics(events, current_price, spread)
        depth = self._depth_metrics(latest_depth, series["bid_series"], series["ask_series"])
        risk = self._risk_metrics(
            series["tick_imbalance"],
            series["velocity_bps"],
            series["latest_delta_bps"],
            series["spread_stress"],
            depth["book_imbalance"],
            depth["total_depth"],
            str(latest_depth.get("flags", "") or latest.get("flags", "")),
        )
        event_metrics = self._event_metrics(
            events,
            bool(depth["total_depth"] > 0),
            bool(depth["synthetic_depth_available"]),
        )
        ladder_proxy = self._ladder_proxy_metrics(events)

        payload = {
            "provider": provider_key,
            "spread_bps": round(spread_bps, 3),
            "tick_imbalance": round(_clip11(series["tick_imbalance"]), 4),
            "book_imbalance": round(_clip11(depth["book_imbalance"]), 4),
            "synthetic_book_imbalance": round(_clip11(depth["synthetic_book_imbalance"]), 4),
            "velocity_bps": round(series["velocity_bps"], 4),
            "latest_delta_bps": round(series["latest_delta_bps"], 4),
            "spread_stress": round(max(0.0, series["spread_stress"]), 4),
            "stop_hunt_risk": round(risk["stop_hunt_risk"], 4),
            "exhaustion_risk": round(risk["exhaustion_risk"], 4),
            "pressure_direction": risk["pressure_direction"],
            "depth_available": bool(depth["total_depth"] > 0),
            "synthetic_depth_available": bool(depth["synthetic_depth_available"]),
            "depth_levels": int(depth["depth_levels"]),
            "bid_level_count": int(depth["bid_level_count"]),
            "ask_level_count": int(depth["ask_level_count"]),
            "depth_quality": round(float(depth["depth_quality"]), 4),
            "depth_quality_tier": str(depth["depth_quality_tier"]),
            "quote_updates": int(len(events)),
            "score": round(risk["score"], 4),
            "microstructure_source": "live_store_depth" if depth["total_depth"] > 0 else ("live_store_synthetic_depth" if depth["synthetic_depth_available"] else "live_store"),
            "flags": str(latest_depth.get("flags", "") or latest.get("flags", "")),
            "depth_update_mode": event_metrics["depth_update_mode"],
            "dom_snapshot_count": event_metrics["dom_snapshot_count"],
            "dom_delta_count": event_metrics["dom_delta_count"],
            "dom_trade_count": event_metrics["dom_trade_count"],
            "dom_stream_snapshot_ready": bool(event_metrics["dom_stream_snapshot_ready"]),
            "dom_depth_event_age_seconds": float(event_metrics["dom_depth_event_age_seconds"]),
            "dom_snapshot_span_seconds": float(event_metrics["dom_snapshot_span_seconds"]),
        }
        payload.update(ladder_proxy)
        return attach_dom_evidence(payload)

    def clear(self) -> None:
        with self._lock:
            self._quotes.clear()


_service = LiveMicrostructureService()


def get_service() -> LiveMicrostructureService:
    return _service
