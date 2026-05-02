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
        depth_event_positions: List[int] = []
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
        for idx, evt in enumerate(events):
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
                depth_event_positions.append(idx)

        if len(depth_events) < 2:
            return {
                "dom_depth_window": int(len(depth_events)),
                "dom_liquidity_shift_proxy": 0.0,
                "dom_sweep_pressure_proxy": 0.0,
                "dom_refill_resilience_proxy": 0.0,
                "dom_absorption_proxy": 0.0,
                "dom_iceberg_proxy": 0.0,
                "dom_queue_persistence": 0.0,
                "dom_add_intent_bias": 0.0,
                "dom_cancel_pressure_bias": 0.0,
                "dom_queue_erosion_bias": 0.0,
                "dom_trade_absorption_proxy": 0.0,
                "dom_refill_after_sweep_bias": 0.0,
                "dom_trade_aggression_bias": 0.0,
                "dom_trade_backed_iceberg_proxy": 0.0,
                "dom_trade_backed_iceberg_hits": 0,
                "dom_refill_after_sweep_hits": 0,
                "dom_sweep_up_count": 0,
                "dom_sweep_down_count": 0,
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
        add_intent_sum = 0.0
        cancel_pressure_sum = 0.0
        queue_erosion_sum = 0.0
        trade_absorption_sum = 0.0
        refill_after_sweep_sum = 0.0
        trade_absorption_hits = 0
        trade_buy_volume = 0.0
        trade_sell_volume = 0.0
        refill_after_sweep_hits = 0
        sweep_up_count = 0
        sweep_down_count = 0

        def _trade_window(start_idx: int, end_idx: int) -> Tuple[float, float]:
            buy_volume = 0.0
            sell_volume = 0.0
            for evt in events[start_idx:end_idx]:
                flags = _flag_tokens(evt.get("flags"))
                event_type = str(evt.get("event_type") or "").strip().lower()
                is_trade = bool(
                    any(token in flags for token in ("trade_print", "trade_stream", "tape_print"))
                    or event_type == "trade_print"
                )
                if not is_trade:
                    continue
                size = max(0.0, _safe_float(evt.get("trade_size"), 0.0))
                side = str(evt.get("trade_side") or "").strip().lower()
                if side == "buy":
                    buy_volume += size
                elif side == "sell":
                    sell_volume += size
            return buy_volume, sell_volume

        for pair_index, (prev, curr) in enumerate(zip(states, states[1:])):
            pair_count += 1
            prev_total = max(prev["total_depth"], 1e-9)
            curr_total = max(curr["total_depth"], 1e-9)
            depth_norm = max(prev_total, curr_total, 1.0)
            ref_price = max(prev["mid_price"], curr["mid_price"], 1e-9)
            price_delta = curr["mid_price"] - prev["mid_price"]
            price_delta_bps = (price_delta / ref_price) * 10000.0
            price_sign = 1.0 if price_delta_bps > 0.02 else -1.0 if price_delta_bps < -0.02 else 0.0
            prev_event_pos = depth_event_positions[pair_index]
            curr_event_pos = depth_event_positions[pair_index + 1]

            buy_volume, sell_volume = _trade_window(prev_event_pos + 1, curr_event_pos + 1)
            trade_buy_volume += buy_volume
            trade_sell_volume += sell_volume
            total_trade_volume = buy_volume + sell_volume
            trade_bias = ((buy_volume - sell_volume) / total_trade_volume) if total_trade_volume > 0.0 else 0.0

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

            bid_add = max(0.0, curr["bid_depth"] - prev["bid_depth"]) / depth_norm
            ask_add = max(0.0, curr["ask_depth"] - prev["ask_depth"]) / depth_norm
            bid_cancel = max(0.0, prev["bid_depth"] - curr["bid_depth"]) / depth_norm
            ask_cancel = max(0.0, prev["ask_depth"] - curr["ask_depth"]) / depth_norm
            add_intent_sum += _clip11((bid_add - ask_add) * 1.15 + (ask_cancel - bid_cancel) * 0.35)
            cancel_pressure_sum += _clip11((ask_cancel - bid_cancel) * 1.10 + (bid_add - ask_add) * 0.25)

            queue_norm = max(
                prev["best_bid_size"],
                curr["best_bid_size"],
                prev["best_ask_size"],
                curr["best_ask_size"],
                1.0,
            )
            queue_erosion_sum += _clip11(
                (
                    (curr["best_bid_size"] - prev["best_bid_size"])
                    - (curr["best_ask_size"] - prev["best_ask_size"])
                )
                / queue_norm
            )

            if price_sign > 0:
                opposing_depth_depletion = max(0.0, prev["ask_depth"] - curr["ask_depth"]) / max(prev["ask_depth"], 1e-9)
                supportive_refill = max(0.0, curr["bid_depth"] - prev["bid_depth"]) / max(prev["bid_depth"], 1e-9)
                opposing_refill = max(0.0, curr["ask_depth"] - prev["ask_depth"]) / max(prev["ask_depth"], 1e-9)
                sweep_pressure_sum += opposing_depth_depletion - opposing_refill * 0.45
                refill_sum += supportive_refill - opposing_refill * 0.35
                if (
                    (opposing_depth_depletion >= 0.08 or (not same_ask and curr["best_ask"] > prev["best_ask"] > 0.0))
                    and supportive_refill >= 0.06
                ):
                    sweep_up_count += 1
                    refill_after_sweep_hits += 1
                    refill_after_sweep_sum += min(1.0, supportive_refill + opposing_depth_depletion * 0.60)
                if abs(price_delta_bps) <= 0.18 and curr["imbalance"] >= 0.08 and supportive_refill >= 0.06:
                    absorption_hits += 1
                    absorption_sum += min(1.0, supportive_refill + curr["imbalance"] * 0.55)
                if (
                    total_trade_volume > 0.0
                    and buy_volume >= sell_volume * 1.15
                    and same_ask
                    and curr["best_ask_size"] >= prev["best_ask_size"] * 0.95
                    and abs(price_delta_bps) <= 0.16
                ):
                    trade_absorption_hits += 1
                    trade_absorption_sum -= min(
                        1.0,
                        abs(trade_bias) * 0.75
                        + max(0.0, curr["best_ask_size"] - prev["best_ask_size"]) / max(prev["best_ask_size"], 1e-9),
                    )
            elif price_sign < 0:
                opposing_depth_depletion = max(0.0, prev["bid_depth"] - curr["bid_depth"]) / max(prev["bid_depth"], 1e-9)
                supportive_refill = max(0.0, curr["ask_depth"] - prev["ask_depth"]) / max(prev["ask_depth"], 1e-9)
                opposing_refill = max(0.0, curr["bid_depth"] - prev["bid_depth"]) / max(prev["bid_depth"], 1e-9)
                sweep_pressure_sum -= opposing_depth_depletion - opposing_refill * 0.45
                refill_sum -= supportive_refill - opposing_refill * 0.35
                if (
                    (opposing_depth_depletion >= 0.08 or (not same_bid and curr["best_bid"] < prev["best_bid"] and curr["best_bid"] > 0.0))
                    and supportive_refill >= 0.06
                ):
                    sweep_down_count += 1
                    refill_after_sweep_hits += 1
                    refill_after_sweep_sum -= min(1.0, supportive_refill + opposing_depth_depletion * 0.60)
                if abs(price_delta_bps) <= 0.18 and curr["imbalance"] <= -0.08 and supportive_refill >= 0.06:
                    absorption_hits += 1
                    absorption_sum -= min(1.0, supportive_refill + abs(curr["imbalance"]) * 0.55)
                if (
                    total_trade_volume > 0.0
                    and sell_volume >= buy_volume * 1.15
                    and same_bid
                    and curr["best_bid_size"] >= prev["best_bid_size"] * 0.95
                    and abs(price_delta_bps) <= 0.16
                ):
                    trade_absorption_hits += 1
                    trade_absorption_sum += min(
                        1.0,
                        abs(trade_bias) * 0.75
                        + max(0.0, curr["best_bid_size"] - prev["best_bid_size"]) / max(prev["best_bid_size"], 1e-9),
                    )
            else:
                if total_trade_volume > 0.0 and abs(trade_bias) >= 0.18:
                    if (
                        trade_bias > 0.0
                        and same_ask
                        and curr["best_ask_size"] >= prev["best_ask_size"] * 0.98
                    ):
                        trade_absorption_hits += 1
                        trade_absorption_sum -= min(1.0, abs(trade_bias) * 0.65)
                    elif (
                        trade_bias < 0.0
                        and same_bid
                        and curr["best_bid_size"] >= prev["best_bid_size"] * 0.98
                    ):
                        trade_absorption_hits += 1
                        trade_absorption_sum += min(1.0, abs(trade_bias) * 0.65)

        reload_count = 0
        iceberg_sum = 0.0
        trade_backed_iceberg_sum = 0.0
        trade_backed_iceberg_hits = 0
        triplet_count = 0
        for triplet_index, (first, second, third) in enumerate(zip(states, states[1:], states[2:])):
            triplet_count += 1
            ref_price = max(first["mid_price"], second["mid_price"], third["mid_price"], 1e-9)
            stable_mid = max(
                abs(second["mid_price"] - first["mid_price"]),
                abs(third["mid_price"] - second["mid_price"]),
            ) <= ref_price * 0.00004
            first_pos = depth_event_positions[triplet_index]
            third_pos = depth_event_positions[triplet_index + 2]
            triplet_buy_volume, triplet_sell_volume = _trade_window(first_pos + 1, third_pos + 1)

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
                reload_strength = min(
                    1.0,
                    (third["best_bid_size"] - second["best_bid_size"]) / max(second["best_bid_size"], 1e-9),
                )
                iceberg_sum += reload_strength
                if triplet_sell_volume >= max(1.0, triplet_buy_volume * 1.10):
                    trade_backed_iceberg_hits += 1
                    trade_backed_iceberg_sum += min(
                        1.0,
                        reload_strength * 0.70
                        + (triplet_sell_volume - triplet_buy_volume)
                        / max(triplet_sell_volume + triplet_buy_volume, 1.0)
                        * 0.45,
                    )
            if ask_reload:
                reload_count += 1
                reload_strength = min(
                    1.0,
                    (third["best_ask_size"] - second["best_ask_size"]) / max(second["best_ask_size"], 1e-9),
                )
                iceberg_sum -= reload_strength
                if triplet_buy_volume >= max(1.0, triplet_sell_volume * 1.10):
                    trade_backed_iceberg_hits += 1
                    trade_backed_iceberg_sum -= min(
                        1.0,
                        reload_strength * 0.70
                        + (triplet_buy_volume - triplet_sell_volume)
                        / max(triplet_sell_volume + triplet_buy_volume, 1.0)
                        * 0.45,
                    )

        depth_window = len(depth_events)
        queue_persistence = stable_top_pairs / max(1, pair_count)
        liquidity_shift_proxy = _clip11(liquidity_shift_sum / max(1, pair_count))
        sweep_pressure_proxy = _clip11(sweep_pressure_sum / max(1, pair_count))
        refill_resilience_proxy = _clip11(refill_sum / max(1, pair_count))
        absorption_proxy = _clip11(absorption_sum / max(1, absorption_hits or pair_count))
        iceberg_proxy = _clip11(iceberg_sum / max(1, triplet_count))
        add_intent_bias = _clip11(add_intent_sum / max(1, pair_count))
        cancel_pressure_bias = _clip11(cancel_pressure_sum / max(1, pair_count))
        queue_erosion_bias = _clip11(queue_erosion_sum / max(1, pair_count))
        trade_absorption_proxy = _clip11(trade_absorption_sum / max(1, trade_absorption_hits or pair_count))
        refill_after_sweep_bias = _clip11(refill_after_sweep_sum / max(1, pair_count))
        trade_backed_iceberg_proxy = _clip11(
            trade_backed_iceberg_sum / max(1, trade_backed_iceberg_hits or triplet_count)
        )
        total_trade_volume = trade_buy_volume + trade_sell_volume
        trade_aggression_bias = _clip11(
            ((trade_buy_volume - trade_sell_volume) / total_trade_volume) if total_trade_volume > 0.0 else 0.0
        )
        return {
            "dom_depth_window": int(depth_window),
            "dom_liquidity_shift_proxy": round(liquidity_shift_proxy, 4),
            "dom_sweep_pressure_proxy": round(sweep_pressure_proxy, 4),
            "dom_refill_resilience_proxy": round(refill_resilience_proxy, 4),
            "dom_absorption_proxy": round(absorption_proxy, 4),
            "dom_iceberg_proxy": round(iceberg_proxy, 4),
            "dom_queue_persistence": round(_clip(queue_persistence, 0.0, 1.0), 4),
            "dom_add_intent_bias": round(add_intent_bias, 4),
            "dom_cancel_pressure_bias": round(cancel_pressure_bias, 4),
            "dom_queue_erosion_bias": round(queue_erosion_bias, 4),
            "dom_trade_absorption_proxy": round(trade_absorption_proxy, 4),
            "dom_refill_after_sweep_bias": round(refill_after_sweep_bias, 4),
            "dom_trade_aggression_bias": round(trade_aggression_bias, 4),
            "dom_trade_backed_iceberg_proxy": round(trade_backed_iceberg_proxy, 4),
            "dom_trade_backed_iceberg_hits": int(trade_backed_iceberg_hits),
            "dom_refill_after_sweep_hits": int(refill_after_sweep_hits),
            "dom_sweep_up_count": int(sweep_up_count),
            "dom_sweep_down_count": int(sweep_down_count),
            "dom_supportive_reload_count": int(reload_count),
        }

    @staticmethod
    def _fragmentation_metrics(
        provider_key: str,
        asset_key: str,
        provider_events: Dict[str, List[Dict[str, Any]]],
    ) -> Dict[str, Any]:
        now_ts = time.time()
        venue_states: List[Dict[str, Any]] = []
        for venue, venue_events in provider_events.items():
            if not venue_events:
                continue
            latest = venue_events[-1]
            latest_depth = latest
            for evt in reversed(venue_events):
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
            ts = _safe_ts(latest_depth.get("timestamp", latest.get("timestamp")))
            if now_ts - ts > 45.0:
                continue
            state = LiveMicrostructureService._depth_state(latest_depth)
            price_value = state["mid_price"] if state["mid_price"] > 0.0 else _safe_float(latest.get("price"), 0.0)
            if price_value <= 0.0:
                continue
            bid_value = state["best_bid"] if state["best_bid"] > 0.0 else _safe_float(latest.get("bid"), 0.0)
            ask_value = state["best_ask"] if state["best_ask"] > 0.0 else _safe_float(latest.get("ask"), 0.0)
            spread_bps = 0.0
            if bid_value > 0.0 and ask_value >= bid_value and price_value > 0.0:
                spread_bps = max(0.0, (ask_value - bid_value) / price_value * 10000.0)
            venue_states.append(
                {
                    "provider": venue,
                    "mid": float(price_value),
                    "imbalance": float(state["imbalance"]),
                    "spread_bps": float(spread_bps),
                    "timestamp": float(ts),
                }
            )

        provider_count = len(venue_states)
        if provider_count <= 1:
            return {
                "dom_fragmentation_provider_count": int(provider_count),
                "dom_cross_venue_mid_dislocation_bps": 0.0,
                "dom_cross_venue_imbalance_dispersion": 0.0,
                "dom_cross_venue_spread_dispersion_bps": 0.0,
                "dom_cross_venue_agreement": 1.0 if provider_count == 1 else 0.0,
                "dom_cross_venue_consensus_bias": 0.0,
                "dom_primary_vs_consensus_gap": 0.0,
                "dom_fragmentation_score": 0.0,
                "dom_fragmented_market": False,
            }

        mids = [row["mid"] for row in venue_states if row["mid"] > 0.0]
        imbalances = [row["imbalance"] for row in venue_states]
        spreads = [row["spread_bps"] for row in venue_states]
        avg_mid = sum(mids) / max(1, len(mids))
        mid_dislocation_bps = ((max(mids) - min(mids)) / avg_mid * 10000.0) if avg_mid > 0.0 and len(mids) >= 2 else 0.0
        imbalance_dispersion = statistics.pstdev(imbalances) if len(imbalances) >= 2 else 0.0
        spread_dispersion_bps = statistics.pstdev(spreads) if len(spreads) >= 2 else 0.0
        signed_votes = [1 if value >= 0.06 else -1 if value <= -0.06 else 0 for value in imbalances]
        non_neutral_votes = [vote for vote in signed_votes if vote != 0]
        agreement = (
            abs(sum(non_neutral_votes)) / len(non_neutral_votes)
            if non_neutral_votes
            else 0.0
        )
        consensus_bias = sum(imbalances) / max(1, len(imbalances))
        primary_gap = 0.0
        for row in venue_states:
            if row["provider"] == provider_key:
                primary_gap = abs(row["imbalance"] - consensus_bias)
                break
        fragmentation_score = _clip(
            (mid_dislocation_bps / 8.0) * 0.45
            + (imbalance_dispersion / 0.35) * 0.35
            + (spread_dispersion_bps / 3.0) * 0.20
        )
        fragmented_market = bool(
            fragmentation_score >= 0.42
            or (len(non_neutral_votes) >= 2 and agreement <= 0.34)
            or mid_dislocation_bps >= 4.0
        )
        return {
            "dom_fragmentation_provider_count": int(provider_count),
            "dom_cross_venue_mid_dislocation_bps": round(mid_dislocation_bps, 4),
            "dom_cross_venue_imbalance_dispersion": round(imbalance_dispersion, 4),
            "dom_cross_venue_spread_dispersion_bps": round(spread_dispersion_bps, 4),
            "dom_cross_venue_agreement": round(_clip(agreement), 4),
            "dom_cross_venue_consensus_bias": round(_clip11(consensus_bias), 4),
            "dom_primary_vs_consensus_gap": round(_clip(primary_gap, 0.0, 1.0), 4),
            "dom_fragmentation_score": round(fragmentation_score, 4),
            "dom_fragmented_market": fragmented_market,
        }

    @staticmethod
    def _event_metrics(events: List[Dict[str, Any]], depth_available: bool, synthetic_depth_available: bool) -> Dict[str, Any]:
        now_ts = time.time()
        latest_event_ts = 0.0
        for evt in events:
            latest_event_ts = max(latest_event_ts, _safe_ts(evt.get("timestamp")))

        snapshot_count = 0
        delta_count = 0
        trade_count = 0
        latest_flags = ""
        latest_flag_ts = 0.0
        latest_depth_ts = 0.0
        earliest_snapshot_ts = 0.0
        for evt in events:
            event_type = str(evt.get("event_type") or "").strip().lower()
            flags = _flag_tokens(evt.get("flags"))
            ts = _safe_ts(evt.get("timestamp"))
            if flags and ts >= latest_flag_ts:
                latest_flag_ts = ts
                latest_flags = ",".join(sorted(flags))
            has_depth_payload = bool(
                evt.get("levels")
                or evt.get("bid_size") not in (None, "")
                or evt.get("ask_size") not in (None, "")
            )
            snapshot_like = bool(
                "depth_snapshot" in flags
                or "stream_snapshot" in flags
                or event_type == "depth_snapshot"
                or has_depth_payload
            )
            delta_like = bool(
                any(token in flags for token in ("depth_delta", "book_delta", "ladder_delta"))
                or event_type == "depth_delta"
            )
            trade_like = bool(
                any(token in flags for token in ("trade_print", "trade_stream", "tape_print"))
                or event_type == "trade_print"
            )
            if snapshot_like:
                latest_depth_ts = max(latest_depth_ts, ts)

            age_from_latest = max(0.0, latest_event_ts - ts) if latest_event_ts > 0.0 and ts > 0.0 else 0.0
            snapshot_fresh = bool(snapshot_like and age_from_latest <= 180.0)
            delta_fresh = bool(delta_like and age_from_latest <= 35.0)
            trade_fresh = bool(trade_like and age_from_latest <= 45.0)

            if snapshot_fresh:
                snapshot_count += 1
                if earliest_snapshot_ts <= 0.0:
                    earliest_snapshot_ts = ts
                else:
                    earliest_snapshot_ts = min(earliest_snapshot_ts, ts)
            if delta_fresh:
                delta_count += 1
            if trade_fresh:
                trade_count += 1

        event_age_seconds = round(max(0.0, now_ts - latest_depth_ts), 3) if latest_depth_ts > 0.0 else 0.0
        snapshot_span_seconds = round(max(0.0, latest_depth_ts - earliest_snapshot_ts), 3) if earliest_snapshot_ts > 0.0 else 0.0
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
            asset_provider_events = {
                venue: list(bucket)
                for (venue, venue_asset), bucket in self._quotes.items()
                if venue_asset == asset_key and bucket
            }
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
        fragmentation = self._fragmentation_metrics(provider_key, asset_key, asset_provider_events)
        try:
            from services.dom_stream_health_service import get_service as get_dom_stream_health_service

            stream_health = dict(get_dom_stream_health_service().snapshot(provider_key, asset_key) or {})
        except Exception:
            stream_health = {
                "dom_stream_health_known": False,
                "dom_stream_connected": False,
                "dom_stream_degraded": False,
                "dom_stream_health_score": 1.0,
                "dom_stream_trust_decay": 0.0,
                "dom_stream_reconnect_count": 0,
                "dom_stream_sequence_gap_count": 0,
                "dom_stream_last_message_age_seconds": None,
                "dom_depth_stream_age_seconds": None,
                "dom_trade_stream_age_seconds": None,
                "dom_depth_stream_missing": False,
                "dom_trade_stream_missing": False,
                "dom_stream_reason": "",
            }

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
            "dom_stream_health_known": bool(stream_health.get("dom_stream_health_known")),
            "dom_stream_connected": bool(stream_health.get("dom_stream_connected")),
            "dom_stream_degraded": bool(stream_health.get("dom_stream_degraded")),
            "dom_stream_health_score": round(_safe_float(stream_health.get("dom_stream_health_score"), 1.0), 4),
            "dom_stream_trust_decay": round(_safe_float(stream_health.get("dom_stream_trust_decay"), 0.0), 4),
            "dom_stream_reconnect_count": int(stream_health.get("dom_stream_reconnect_count", 0) or 0),
            "dom_stream_sequence_gap_count": int(stream_health.get("dom_stream_sequence_gap_count", 0) or 0),
            "dom_stream_last_message_age_seconds": (
                round(_safe_float(stream_health.get("dom_stream_last_message_age_seconds"), 0.0), 3)
                if stream_health.get("dom_stream_last_message_age_seconds") is not None
                else None
            ),
            "dom_depth_stream_age_seconds": (
                round(_safe_float(stream_health.get("dom_depth_stream_age_seconds"), 0.0), 3)
                if stream_health.get("dom_depth_stream_age_seconds") is not None
                else None
            ),
            "dom_trade_stream_age_seconds": (
                round(_safe_float(stream_health.get("dom_trade_stream_age_seconds"), 0.0), 3)
                if stream_health.get("dom_trade_stream_age_seconds") is not None
                else None
            ),
            "dom_depth_stream_missing": bool(stream_health.get("dom_depth_stream_missing")),
            "dom_trade_stream_missing": bool(stream_health.get("dom_trade_stream_missing")),
            "dom_stream_reason": str(stream_health.get("dom_stream_reason") or ""),
        }
        payload.update(ladder_proxy)
        payload.update(fragmentation)
        return attach_dom_evidence(payload)

    def clear(self) -> None:
        with self._lock:
            self._quotes.clear()


_service = LiveMicrostructureService()


def get_service() -> LiveMicrostructureService:
    return _service
